//! A shard: a group of markets carried by `redundancy` connections, plus the
//! logic that decides which copy of each book is published.
//!
//! Every connection keeps its own copy ("view") of every book in the shard.
//! Per market, one view is the *source*: its changes go to the output. The
//! shard switches source, and writes a full snapshot of the new source's book
//! so readers can't inherit the old copy's errors, when:
//!
//! - the source socket drops or loses frames (failover), or
//! - another copy has seen a change (matched by the venue's per-market
//!   `clock`) that the source still hasn't seen after `lag_threshold`.
//!
//! The rest is checking. Every few seconds a few books per connection are
//! re-requested from the venue and compared level by level with the copy we
//! built from deltas (the audit probe). A copy that goes quiet while another
//! copy of the same market keeps changing is resubscribed.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::book::Book;
use crate::conn::{self, ConnCmd, ConnEvent, ConnEventKind, ConnHandle, ConnSpec, Mode};
use crate::model::{BookBody, BookMsg, Micros, now_micros, to_datetime};
use crate::stats::Stats;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::venue::Venue;
use crate::writer::OutMsg;

/// How many recent changes per market to remember for the arrival race.
const ARRIVALS_KEPT: usize = 16;
/// How long a copy may stay unusable on a live socket before it is
/// re-requested, and how long between further attempts.
const REPAIR_AFTER: Duration = Duration::from_secs(15);
const REPAIR_RETRY: Duration = Duration::from_secs(30);
/// A market counts as active if it changed at least this often in the last
/// activity window.
const ACTIVE_UPDATES: u32 = 10;

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub redundancy: usize,
    pub lag_threshold: Duration,
    pub probe_every: Duration,
    pub probe_batch: usize,
    pub quiet_after: Duration,
    pub recycle_every: Option<Duration>,
}

#[derive(Debug)]
pub enum ShardCmd {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Write a full snapshot of every book.
    SnapshotAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReason {
    /// The venue sent a snapshot for the source copy (first, or a resync).
    Exchange,
    /// The source copy became unusable; another copy took over.
    Failover,
    /// The source fell behind another copy.
    Lag,
    /// The minute snapshot.
    Periodic,
}

struct View {
    book: Book,
    /// False until this copy has a snapshot, and again after it loses data.
    valid: bool,
    clock: u64,
    exchange_ts: Option<Micros>,
    /// When this copy last applied a delta. Snapshots don't count: they are
    /// not evidence that the market moved.
    last_delta: Option<Instant>,
    /// Since when another usable copy has held a newer change (higher
    /// clock) than this one.
    behind_since: Option<Instant>,
    probe_pending: bool,
    /// Since when the repair sweep has seen this copy unusable.
    invalid_since: Option<Instant>,
    /// 0: not asked yet; 1: snapshot requested; 2: resubscribed.
    repair_stage: u8,
    repair_at: Option<Instant>,
    /// The venue refused this market on this socket; the next connection
    /// retries it, not the repair sweep.
    refused: bool,
}

impl View {
    fn new() -> Self {
        Self {
            book: Book::default(),
            valid: false,
            clock: 0,
            exchange_ts: None,
            last_delta: None,
            behind_since: None,
            probe_pending: false,
            invalid_since: None,
            repair_stage: 0,
            repair_at: None,
            refused: false,
        }
    }

    fn repaired(&mut self) {
        self.invalid_since = None;
        self.repair_stage = 0;
        self.repair_at = None;
        self.refused = false;
    }
}

/// A change seen on at least one route: when it first arrived, and which
/// routes (bit per slot) have delivered it so far.
struct Arrival {
    clock: u64,
    at: Micros,
    seen: u64,
}

#[derive(Clone, Copy)]
struct Lag {
    slot: usize,
    clock: u64,
    since: Instant,
}

struct Market {
    views: Vec<View>,
    source: Option<usize>,
    lag: Option<Lag>,
    /// Recent changes seen on one route but not yet on the others.
    arrivals: VecDeque<Arrival>,
    updates_this_window: u32,
    updates_last_window: u32,
    last_quiet_probe: Option<Instant>,
    /// The latest timestamp written for this market. Nothing later in the
    /// file is stamped earlier (see [`stamp`]).
    last_out: Micros,
}

impl Market {
    fn new(redundancy: usize) -> Self {
        Self {
            views: (0..redundancy).map(|_| View::new()).collect(),
            source: None,
            lag: None,
            arrivals: VecDeque::new(),
            updates_this_window: 0,
            updates_last_window: 0,
            last_quiet_probe: None,
            last_out: 0,
        }
    }
}

/// The shard's decisions, kept free of I/O so they can be tested directly.
/// Output goes to `out`, commands for connections to `cmds`.
pub struct ShardCore {
    redundancy: usize,
    lag_threshold: Duration,
    quiet_after: Duration,
    /// Whether the venue can re-send a snapshot in place (`get_snapshot`).
    can_probe: bool,
    markets: IndexMap<String, Market>,
    lagging: HashSet<String>,
    up: Vec<bool>,
    pub out: Vec<OutMsg>,
    pub cmds: Vec<(usize, ConnCmd)>,
    stats: Arc<Stats>,
}

impl ShardCore {
    pub fn new(
        redundancy: usize,
        lag_threshold: Duration,
        quiet_after: Duration,
        can_probe: bool,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            redundancy,
            lag_threshold,
            quiet_after,
            can_probe,
            markets: IndexMap::new(),
            lagging: HashSet::new(),
            up: vec![false; redundancy],
            out: Vec::new(),
            cmds: Vec::new(),
            stats,
        }
    }

    pub fn len(&self) -> usize {
        self.markets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    pub fn is_up(&self, slot: usize) -> bool {
        self.up[slot]
    }

    /// Starts tracking markets; returns the ones that were new.
    pub fn add(&mut self, markets: Vec<String>) -> Vec<String> {
        let redundancy = self.redundancy;
        markets
            .into_iter()
            .filter(|m| {
                if self.markets.contains_key(m) {
                    return false;
                }
                self.markets.insert(m.clone(), Market::new(redundancy));
                true
            })
            .collect()
    }

    /// Stops tracking markets; returns the ones that were tracked.
    pub fn remove(&mut self, markets: Vec<String>) -> Vec<String> {
        markets
            .into_iter()
            .filter(|m| {
                self.lagging.remove(m);
                self.markets.swap_remove(m).is_some()
            })
            .collect()
    }

    /// A fresh socket resubscribes everything, so repairs start over.
    pub fn conn_up(&mut self, slot: usize) {
        self.up[slot] = true;
        for market in self.markets.values_mut() {
            market.views[slot].repaired();
        }
    }

    /// The socket in `slot` went down (`down`) or lost frames: every copy it
    /// carried is stale until the venue snapshots it again.
    pub fn conn_lost(&mut self, slot: usize, down: bool, now: Micros) {
        if down {
            self.up[slot] = false;
        }
        let Self {
            markets,
            lagging,
            out,
            stats,
            ..
        } = self;
        for (symbol, market) in markets.iter_mut() {
            let view = &mut market.views[slot];
            view.valid = false;
            view.probe_pending = false;
            if market.lag.is_some_and(|lag| lag.slot == slot) {
                market.lag = None;
                lagging.remove(symbol);
            }
            if market.source == Some(slot) {
                reselect(symbol, market, now, out, stats);
            }
        }
    }

    /// Book data from the socket in `slot`: `received` is when the frame came
    /// off the socket, `sent` when the venue says it sent it.
    pub fn on_book(
        &mut self,
        slot: usize,
        received: Micros,
        sent: Option<Micros>,
        now: Instant,
        msg: BookMsg,
    ) {
        let Self {
            markets,
            lagging,
            out,
            cmds,
            stats,
            ..
        } = self;
        let Some((_, symbol, market)) = markets.get_full_mut(&msg.market) else {
            return; // removed while frames were in flight
        };
        match msg.body {
            BookBody::Snapshot { bids, asks } => {
                stats.snapshots_in.inc();
                let book = Book::from_levels(bids, asks);
                let view = &mut market.views[slot];
                if std::mem::take(&mut view.probe_pending) && view.valid {
                    stats.probes.inc();
                    if view.book != book {
                        // This socket's subscription missed changes. The
                        // venue's book replaces ours now, and the market is
                        // resubscribed in case the subscription is broken.
                        stats.probe_mismatches.inc();
                        tracing::warn!(market = %symbol, slot, clock = ?msg.clock,
                            ours = view.book.level_count(), venue = book.level_count(),
                            "audit probe: our copy differed from the venue's book; resubscribing it");
                        cmds.push((slot, ConnCmd::Resubscribe(vec![symbol.clone()])));
                    }
                }
                let changed = !view.valid || view.book != book;
                view.book = book;
                view.valid = true;
                view.repaired();
                view.clock = msg.clock.unwrap_or(view.clock);
                // A snapshot has no change time of its own; the venue's send
                // stamp is the time of the state it shows.
                view.exchange_ts = msg.exchange_ts.or(sent).or(view.exchange_ts);
                refresh_behind(market, now);
                match market.source {
                    Some(source) if source == slot => {
                        if changed {
                            let view = &market.views[slot];
                            emit_snapshot(
                                out,
                                stats,
                                symbol,
                                view,
                                &mut market.last_out,
                                received,
                                SnapshotReason::Exchange,
                            );
                        }
                    }
                    Some(source) if market.views[source].valid => {}
                    _ => {
                        market.source = Some(slot);
                        let view = &market.views[slot];
                        emit_snapshot(
                            out,
                            stats,
                            symbol,
                            view,
                            &mut market.last_out,
                            received,
                            SnapshotReason::Exchange,
                        );
                    }
                }
            }
            BookBody::Level {
                side,
                price,
                change,
            } => {
                stats.deltas_in.inc();
                let view = &mut market.views[slot];
                if !view.valid {
                    stats.stale_deltas.inc();
                    return;
                }
                let quantity = match view.book.apply(side, price, change) {
                    Ok(quantity) => quantity,
                    Err(error) => {
                        stats.negative_levels.inc();
                        tracing::warn!(market = %symbol, slot, %error, "copy lost data; re-snapshotting it");
                        view.valid = false;
                        cmds.push((slot, ConnCmd::Snapshots(vec![symbol.clone()])));
                        if market.source == Some(slot) {
                            reselect(symbol, market, received, out, stats);
                        }
                        return;
                    }
                };
                if let Some(clock) = msg.clock {
                    view.clock = clock;
                }
                view.exchange_ts = msg.exchange_ts.or(view.exchange_ts);
                view.last_delta = Some(now);
                // Every copy carries the same change under the same clock, so
                // the change is stamped with the earliest time any socket
                // received it: the published copy decides the content, the
                // fastest copy the time.
                let mut first_seen = received;
                if let Some(clock) = msg.clock {
                    first_seen = race(market, slot, clock, received, stats);
                    refresh_behind(market, now);
                }
                if market.source == Some(slot) {
                    market.updates_this_window += 1;
                    stats.updates_out.inc();
                    if first_seen < received {
                        stats.stamped_from_faster_copy.inc();
                    }
                    let at = stamp(&mut market.last_out, first_seen, stats);
                    out.push(OutMsg::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: to_datetime(at),
                        exchange_timestamp: to_datetime(
                            market.views[slot].exchange_ts.unwrap_or(received),
                        ),
                        price_level: price,
                        quantity,
                        side,
                        symbol: symbol.clone(),
                    }));
                    if let (Some(lag), Some(clock)) = (market.lag, msg.clock)
                        && clock >= lag.clock
                    {
                        market.lag = None;
                        lagging.remove(symbol);
                    }
                } else if let (Some(source), Some(clock)) = (market.source, msg.clock) {
                    let source_view = &market.views[source];
                    if market.lag.is_none() && source_view.valid && clock > source_view.clock {
                        market.lag = Some(Lag {
                            slot,
                            clock,
                            since: now,
                        });
                        lagging.insert(symbol.clone());
                    }
                }
            }
        }
    }

    /// Switches markets whose source has trailed another copy for longer
    /// than the threshold.
    pub fn check_lags(&mut self, now: Instant, now_us: Micros) {
        let due: Vec<String> = self.lagging.iter().cloned().collect();
        for symbol in due {
            let Some(market) = self.markets.get_mut(&symbol) else {
                self.lagging.remove(&symbol);
                continue;
            };
            let Some(lag) = market.lag else {
                self.lagging.remove(&symbol);
                continue;
            };
            if now.duration_since(lag.since) < self.lag_threshold {
                continue;
            }
            market.lag = None;
            self.lagging.remove(&symbol);
            let behind = market
                .source
                .is_some_and(|s| market.views[s].clock < lag.clock);
            if behind && market.views[lag.slot].valid {
                tracing::info!(market = %symbol, from = ?market.source, to = lag.slot,
                    "source fell behind another copy; switching");
                market.source = Some(lag.slot);
                let view = &market.views[lag.slot];
                emit_snapshot(
                    &mut self.out,
                    &self.stats,
                    &symbol,
                    view,
                    &mut market.last_out,
                    now_us,
                    SnapshotReason::Lag,
                );
            }
        }
    }

    /// The minute snapshot: every book with a usable source copy.
    pub fn snapshot_all(&mut self, now: Micros) {
        let Self {
            markets,
            out,
            stats,
            ..
        } = self;
        for (symbol, market) in markets.iter_mut() {
            match market.source {
                Some(source) if market.views[source].valid => {
                    let view = &market.views[source];
                    emit_snapshot(
                        out,
                        stats,
                        symbol,
                        view,
                        &mut market.last_out,
                        now,
                        SnapshotReason::Periodic,
                    );
                }
                _ => stats.uncovered.inc(),
            }
        }
    }

    /// Picks up to `n` random markets whose copy on `slot` to audit against
    /// a fresh venue snapshot.
    pub fn pick_probes(&mut self, slot: usize, n: usize) -> Vec<String> {
        let mut picked = Vec::new();
        if self.markets.is_empty() || !self.can_probe {
            return picked;
        }
        for _ in 0..n * 4 {
            if picked.len() >= n {
                break;
            }
            let index = fastrand::usize(..self.markets.len());
            let (symbol, market) = self.markets.get_index_mut(index).expect("index in range");
            let view = &mut market.views[slot];
            if view.valid && !view.probe_pending {
                view.probe_pending = true;
                picked.push(symbol.clone());
            }
        }
        picked
    }

    /// Looks for copies that stopped receiving a market's changes.
    ///
    /// - A copy that has trailed another copy's clock for `quiet_after` is
    ///   missing changes the other copy got: it is resubscribed on its
    ///   connection. (A briefly trailing source is handled sooner by the lag
    ///   switch; this also repairs backups.)
    /// - An active market whose copies have all gone silent is probed; if it
    ///   is just quiet the probe matches and nothing changes.
    pub fn check_quiet(&mut self, now: Instant, now_us: Micros) {
        let Self {
            markets,
            up,
            out,
            cmds,
            stats,
            quiet_after,
            lagging,
            redundancy,
            can_probe,
            ..
        } = self;
        let quiet_after = *quiet_after;
        let mut quiet: Vec<Vec<String>> = vec![Vec::new(); *redundancy];
        let mut probes: Vec<Vec<String>> = vec![Vec::new(); *redundancy];
        for (symbol, market) in markets.iter_mut() {
            let mut any_quiet = false;
            for slot in 0..market.views.len() {
                let view = &market.views[slot];
                if up[slot]
                    && view.valid
                    && view
                        .behind_since
                        .is_some_and(|t| now.duration_since(t) > quiet_after)
                {
                    quiet[slot].push(symbol.clone());
                    any_quiet = true;
                }
            }
            let newest = market
                .views
                .iter()
                .filter(|v| v.valid)
                .filter_map(|v| v.last_delta)
                .max();
            let Some(newest) = newest else { continue };
            let silent = now.duration_since(newest) > quiet_after;
            let was_active = market.updates_last_window >= ACTIVE_UPDATES;
            let probed_recently = market
                .last_quiet_probe
                .is_some_and(|t| now.duration_since(t) < quiet_after);
            if *can_probe && !any_quiet && silent && was_active && !probed_recently {
                market.last_quiet_probe = Some(now);
                for slot in 0..market.views.len() {
                    let view = &mut market.views[slot];
                    if up[slot] && view.valid && !view.probe_pending {
                        view.probe_pending = true;
                        probes[slot].push(symbol.clone());
                    }
                }
            }
        }

        // Many quiet copies on one socket means the socket, not the markets,
        // is the problem: reconnect it once rather than resubscribing each.
        let socket_limit = (markets.len() / 10).max(50);
        for (slot, symbols) in quiet.into_iter().enumerate() {
            if symbols.is_empty() {
                continue;
            }
            if symbols.len() > socket_limit {
                stats.quiet_reconnects.inc();
                tracing::warn!(
                    slot,
                    quiet = symbols.len(),
                    "most of this socket's copies went quiet; reconnecting it"
                );
                cmds.push((slot, ConnCmd::Reconnect));
                continue;
            }
            stats.quiet_resubscribes.add(symbols.len() as u64);
            tracing::warn!(slot, count = symbols.len(), first = %symbols[0],
                "copies trailed the other copy's clock for too long; resubscribing them");
            for symbol in &symbols {
                let Some(market) = markets.get_mut(symbol) else {
                    continue;
                };
                market.views[slot].valid = false;
                if market.lag.is_some_and(|lag| lag.slot == slot) {
                    market.lag = None;
                    lagging.remove(symbol);
                }
                if market.source == Some(slot) {
                    reselect(symbol, market, now_us, out, stats);
                }
            }
            cmds.push((slot, ConnCmd::Resubscribe(symbols)));
        }
        for (slot, symbols) in probes.into_iter().enumerate() {
            if !symbols.is_empty() {
                cmds.push((slot, ConnCmd::Snapshots(symbols)));
            }
        }
    }

    /// Starts a new activity window (see [`ACTIVE_UPDATES`]).
    pub fn roll_activity_window(&mut self) {
        for market in self.markets.values_mut() {
            market.updates_last_window = std::mem::take(&mut market.updates_this_window);
        }
    }

    /// Share of markets whose copy on `slot` is usable.
    pub fn valid_share(&self, slot: usize) -> f64 {
        if self.markets.is_empty() {
            return 1.0;
        }
        let valid = self
            .markets
            .values()
            .filter(|m| m.views[slot].valid)
            .count();
        valid as f64 / self.markets.len() as f64
    }

    /// The venue refused these markets on `slot`. That copy is unusable until
    /// the socket reconnects (which retries them); the repair sweep leaves it.
    pub fn refused(&mut self, slot: usize, markets: &[String], now: Micros) {
        let Self {
            markets: tracked,
            out,
            stats,
            ..
        } = self;
        for symbol in markets {
            if let Some(market) = tracked.get_mut(symbol) {
                market.views[slot].valid = false;
                market.views[slot].refused = true;
                if market.source == Some(slot) {
                    reselect(symbol, market, now, out, stats);
                }
            }
        }
    }

    /// Re-requests copies that stayed unusable on a live socket, whatever the
    /// reason: a snapshot request that was refused or lost, a market added
    /// but never confirmed. First a snapshot request (where the venue has
    /// one), then a resubscribe, repeated every [`REPAIR_RETRY`]. If most of
    /// a socket needs repair, the socket is reconnected instead.
    pub fn check_repairs(&mut self, now: Instant) {
        let Self {
            markets,
            up,
            cmds,
            stats,
            redundancy,
            can_probe,
            ..
        } = self;
        let mut snapshots: Vec<Vec<String>> = vec![Vec::new(); *redundancy];
        let mut resubscribe: Vec<Vec<String>> = vec![Vec::new(); *redundancy];
        for (symbol, market) in markets.iter_mut() {
            for slot in 0..market.views.len() {
                let view = &mut market.views[slot];
                if !up[slot] || view.valid || view.refused {
                    continue;
                }
                let since = *view.invalid_since.get_or_insert(now);
                let last = view.repair_at.unwrap_or(since);
                let wait = if view.repair_stage == 0 {
                    REPAIR_AFTER
                } else {
                    REPAIR_RETRY
                };
                if now.duration_since(last) < wait {
                    continue;
                }
                if view.repair_stage == 0 && *can_probe {
                    snapshots[slot].push(symbol.clone());
                    view.repair_stage = 1;
                } else {
                    resubscribe[slot].push(symbol.clone());
                    view.repair_stage = 2;
                }
                view.repair_at = Some(now);
            }
        }
        let socket_limit = (markets.len() / 10).max(50);
        for slot in 0..*redundancy {
            let count = snapshots[slot].len() + resubscribe[slot].len();
            if count == 0 {
                continue;
            }
            if count > socket_limit {
                stats.repair_reconnects.inc();
                tracing::warn!(
                    slot,
                    count,
                    "most of this socket's copies stayed unusable; reconnecting it"
                );
                cmds.push((slot, ConnCmd::Reconnect));
                continue;
            }
            stats.repairs.add(count as u64);
            tracing::info!(slot, count, "re-requesting copies that stayed unusable");
            if !snapshots[slot].is_empty() {
                cmds.push((
                    slot,
                    ConnCmd::Snapshots(std::mem::take(&mut snapshots[slot])),
                ));
            }
            if !resubscribe[slot].is_empty() {
                cmds.push((
                    slot,
                    ConnCmd::Resubscribe(std::mem::take(&mut resubscribe[slot])),
                ));
            }
        }
    }
}

/// The source copy is unusable: hand over to the freshest usable copy, or
/// stop publishing the market until one exists.
fn reselect(symbol: &str, market: &mut Market, now: Micros, out: &mut Vec<OutMsg>, stats: &Stats) {
    let best = (0..market.views.len())
        .filter(|&slot| Some(slot) != market.source && market.views[slot].valid)
        .max_by_key(|&slot| (market.views[slot].clock, std::cmp::Reverse(slot)));
    market.source = best;
    market.lag = None;
    match best {
        Some(slot) => {
            let view = &market.views[slot];
            emit_snapshot(
                out,
                stats,
                symbol,
                view,
                &mut market.last_out,
                now,
                SnapshotReason::Failover,
            );
        }
        None => stats.uncovered.inc(),
    }
}

/// The timestamp to write for a market: `at`, unless something later was
/// already written for it. A frame received before a snapshot we generated
/// can be processed just after it; stamping it earlier than the snapshot
/// would put the file's per-market timestamps out of order.
fn stamp(last_out: &mut Micros, at: Micros, stats: &Stats) -> Micros {
    if at < *last_out {
        stats.stamp_clamps.inc();
        return *last_out;
    }
    *last_out = at;
    at
}

/// Marks which usable copies trail the newest clock any copy has seen.
fn refresh_behind(market: &mut Market, now: Instant) {
    let newest = market
        .views
        .iter()
        .filter(|v| v.valid)
        .map(|v| v.clock)
        .max()
        .unwrap_or(0);
    for view in &mut market.views {
        if !view.valid || view.clock >= newest {
            view.behind_since = None;
        } else if view.behind_since.is_none() {
            view.behind_since = Some(now);
        }
    }
}

/// Records which route delivered each change first, and how far behind it
/// every other route was. Returns when the change first reached any route.
fn race(market: &mut Market, slot: usize, clock: u64, at: Micros, stats: &Stats) -> Micros {
    let bit = 1u64 << slot.min(63);
    let all = (1u64 << market.views.len().min(63)) - 1;
    // Several deltas can share a clock; each copy delivers them in the same
    // order, so match against the oldest arrival this route hasn't joined.
    if let Some(i) = market
        .arrivals
        .iter()
        .position(|a| a.clock == clock && a.seen & bit == 0)
    {
        let arrival = &mut market.arrivals[i];
        let first = arrival.at;
        arrival.seen |= bit;
        stats.route(slot).behind_first.record(at - first);
        if arrival.seen == all {
            market.arrivals.remove(i);
        }
        first.min(at)
    } else {
        stats.route(slot).first_arrivals.inc();
        market.arrivals.push_back(Arrival {
            clock,
            at,
            seen: bit,
        });
        if market.arrivals.len() > ARRIVALS_KEPT {
            market.arrivals.pop_front();
        }
        at
    }
}

fn emit_snapshot(
    out: &mut Vec<OutMsg>,
    stats: &Stats,
    symbol: &str,
    view: &View,
    last_out: &mut Micros,
    at: Micros,
    reason: SnapshotReason,
) {
    match reason {
        SnapshotReason::Exchange => stats.snapshots_out_exchange.inc(),
        SnapshotReason::Failover => stats.snapshots_out_failover.inc(),
        SnapshotReason::Lag => stats.snapshots_out_lag.inc(),
        SnapshotReason::Periodic => stats.snapshots_out_periodic.inc(),
    }
    let at = stamp(last_out, at, stats);
    out.push(OutMsg::Snapshot(SnapshotDomain {
        received_at: to_datetime(at),
        exchange_timestamp: to_datetime(view.exchange_ts.unwrap_or(at)),
        bids: view
            .book
            .bids()
            .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: view
            .book
            .asks()
            .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_owned(),
    }));
}

pub struct ShardHandle {
    pub cmds: mpsc::UnboundedSender<ShardCmd>,
    pub task: JoinHandle<()>,
}

pub fn spawn<V: Venue>(
    id: usize,
    venue: Arc<V>,
    cfg: ShardConfig,
    out: crossbeam::channel::Sender<OutMsg>,
    stats: Arc<Stats>,
    shutdown: watch::Receiver<bool>,
) -> ShardHandle {
    let (cmds, cmds_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(id, venue, cfg, cmds_rx, out, stats, shutdown));
    ShardHandle { cmds, task }
}

fn jittered(period: Duration) -> Duration {
    // ±20% so a shard's sockets, and different shards, recycle at different times.
    period.mul_f64(0.8 + 0.4 * fastrand::f64())
}

async fn run<V: Venue>(
    id: usize,
    venue: Arc<V>,
    cfg: ShardConfig,
    mut cmds: mpsc::UnboundedReceiver<ShardCmd>,
    out: crossbeam::channel::Sender<OutMsg>,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let urls = venue.ws_urls().to_vec();
    let conns: Vec<ConnHandle> = (0..cfg.redundancy)
        .map(|slot| {
            let spec = ConnSpec {
                label: format!("s{id}/{slot}"),
                slot,
                url: urls[slot % urls.len()].clone(),
                mode: Mode::Books,
            };
            conn::spawn(
                venue.clone(),
                spec,
                Vec::new(),
                events_tx.clone(),
                stats.clone(),
                shutdown.clone(),
            )
        })
        .collect();
    drop(events_tx);
    stats.sockets.add(cfg.redundancy as i64);

    let mut core = ShardCore::new(
        cfg.redundancy,
        cfg.lag_threshold,
        cfg.quiet_after,
        venue.snapshots_in_place(),
        stats.clone(),
    );
    let mut next_recycle: Vec<Option<Instant>> = vec![None; cfg.redundancy];
    let handle = |core: &mut ShardCore,
                  next_recycle: &mut Vec<Option<Instant>>,
                  event: ConnEvent| match event.kind {
        ConnEventKind::Up => {
            core.conn_up(event.slot);
            next_recycle[event.slot] = cfg.recycle_every.map(|p| Instant::now() + jittered(p));
        }
        ConnEventKind::Down { .. } => core.conn_lost(event.slot, true, now_micros()),
        ConnEventKind::Gap => core.conn_lost(event.slot, false, now_micros()),
        ConnEventKind::Book {
            received,
            sent,
            msg,
        } => core.on_book(event.slot, received, sent, Instant::now(), msg),
        ConnEventKind::Refused { markets, .. } => core.refused(event.slot, &markets, now_micros()),
        ConnEventKind::Lifecycle(_) => {}
    };
    // Frames already received are applied before anything time-driven
    // (minute snapshot, lag switch), so what that writes includes them.
    let drain = |core: &mut ShardCore,
                 next_recycle: &mut Vec<Option<Instant>>,
                 events: &mut mpsc::UnboundedReceiver<ConnEvent>| {
        while let Ok(event) = events.try_recv() {
            handle(core, next_recycle, event);
        }
    };
    let mut lag_timer = tokio::time::interval(Duration::from_millis(100));
    let mut probe_timer = tokio::time::interval(cfg.probe_every);
    let mut quiet_timer = tokio::time::interval(
        (cfg.quiet_after / 4).clamp(Duration::from_millis(100), Duration::from_secs(5)),
    );
    let mut window_timer = tokio::time::interval(Duration::from_secs(300));
    let mut recycle_timer = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            cmd = cmds.recv() => match cmd {
                Some(ShardCmd::Add(markets)) => {
                    let new = core.add(markets);
                    if !new.is_empty() {
                        for conn in &conns {
                            let _ = conn.cmds.send(ConnCmd::Add(new.clone()));
                        }
                    }
                }
                Some(ShardCmd::Remove(markets)) => {
                    let gone = core.remove(markets);
                    if !gone.is_empty() {
                        for conn in &conns {
                            let _ = conn.cmds.send(ConnCmd::Remove(gone.clone()));
                        }
                    }
                }
                Some(ShardCmd::SnapshotAll) => {
                    drain(&mut core, &mut next_recycle, &mut events);
                    core.snapshot_all(now_micros());
                }
                None => break,
            },
            Some(event) = events.recv() => handle(&mut core, &mut next_recycle, event),
            _ = lag_timer.tick() => {
                drain(&mut core, &mut next_recycle, &mut events);
                core.check_lags(Instant::now(), now_micros());
            }
            _ = probe_timer.tick() => {
                for (slot, conn) in conns.iter().enumerate() {
                    if core.is_up(slot) {
                        let picked = core.pick_probes(slot, cfg.probe_batch);
                        if !picked.is_empty() {
                            let _ = conn.cmds.send(ConnCmd::Snapshots(picked));
                        }
                    }
                }
            }
            _ = quiet_timer.tick() => {
                drain(&mut core, &mut next_recycle, &mut events);
                core.check_quiet(Instant::now(), now_micros());
                core.check_repairs(Instant::now());
            }
            _ = window_timer.tick() => core.roll_activity_window(),
            _ = recycle_timer.tick() => {
                // Recycle one socket at a time, and only while every other
                // copy in the shard is up and current.
                let now = Instant::now();
                let due = (0..cfg.redundancy).find(|&slot| next_recycle[slot].is_some_and(|t| now >= t));
                if let Some(slot) = due {
                    let others_ready = (0..cfg.redundancy)
                        .filter(|&other| other != slot)
                        .all(|other| core.is_up(other) && core.valid_share(other) > 0.99);
                    if others_ready && cfg.redundancy > 1 {
                        tracing::info!(shard = id, slot, "planned socket recycle");
                        next_recycle[slot] = None;
                        let _ = conns[slot].cmds.send(ConnCmd::Reconnect);
                    }
                }
            }
        }
        for msg in core.out.drain(..) {
            let _ = out.send(msg);
        }
        for (slot, cmd) in core.cmds.drain(..) {
            let _ = conns[slot].cmds.send(cmd);
        }
    }
    // Shutting down: once the sockets have closed, apply and write whatever
    // they had already received.
    for conn in conns {
        let _ = conn.task.await;
    }
    drain(&mut core, &mut next_recycle, &mut events);
    for msg in core.out.drain(..) {
        let _ = out.send(msg);
    }
    stats.sockets.add(-(cfg.redundancy as i64));
}

#[cfg(test)]
mod tests;
