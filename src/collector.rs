//! Decides which markets are collected and on which shard, and drives the
//! minute snapshots.
//!
//! Market discovery has two sources:
//! - the venue's lifecycle feed, a socket that announces new and finished
//!   markets as they happen, and
//! - a full REST listing at start-up and every `listing_every` after that,
//!   which catches anything the feed missed. A listing only removes markets
//!   if it completed without errors.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};

use crate::conn::{self, ConnEventKind, ConnSpec, Mode};
use crate::model::Lifecycle;
use crate::shard::{self, ShardCmd, ShardConfig, ShardHandle};
use crate::stats::Stats;
use crate::venue::Venue;
use crate::writer::OutMsg;

#[derive(Debug, Clone)]
pub struct Config {
    pub markets_per_socket: usize,
    pub max_markets: Option<usize>,
    pub snapshot_every: Duration,
    pub listing_every: Duration,
    pub shard: ShardConfig,
}

struct Shard {
    handle: ShardHandle,
    size: usize,
}

struct Assigned {
    shard: usize,
    added: Instant,
}

enum Listing {
    Page(u64, Vec<String>),
    Done(u64, anyhow::Result<usize>),
}

struct Manager<V> {
    venue: Arc<V>,
    cfg: Config,
    out: crossbeam::channel::Sender<OutMsg>,
    stats: Arc<Stats>,
    shutdown: watch::Receiver<bool>,
    shards: Vec<Shard>,
    assigned: HashMap<String, Assigned>,
}

impl<V: Venue> Manager<V> {
    fn add(&mut self, markets: Vec<String>) {
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for market in markets {
            if self.assigned.contains_key(&market) {
                continue;
            }
            if self.cfg.max_markets.is_some_and(|max| self.assigned.len() >= max) {
                break;
            }
            let shard = self.shard_with_room();
            self.shards[shard].size += 1;
            self.assigned.insert(market.clone(), Assigned { shard, added: Instant::now() });
            batches.entry(shard).or_default().push(market);
        }
        for (shard, batch) in batches {
            self.stats.markets_added.add(batch.len() as u64);
            self.stats.markets.add(batch.len() as i64);
            let _ = self.shards[shard].handle.cmds.send(ShardCmd::Add(batch));
        }
    }

    fn remove(&mut self, markets: Vec<String>) {
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for market in markets {
            if let Some(assigned) = self.assigned.remove(&market) {
                self.shards[assigned.shard].size -= 1;
                batches.entry(assigned.shard).or_default().push(market);
            }
        }
        for (shard, batch) in batches {
            tracing::info!(shard, count = batch.len(), first = %batch[0], "markets finished trading; unsubscribing");
            self.stats.markets_removed.add(batch.len() as u64);
            self.stats.markets.add(-(batch.len() as i64));
            let _ = self.shards[shard].handle.cmds.send(ShardCmd::Remove(batch));
        }
    }

    fn shard_with_room(&mut self) -> usize {
        if let Some(index) = self.shards.iter().position(|s| s.size < self.cfg.markets_per_socket) {
            return index;
        }
        let id = self.shards.len();
        let handle = shard::spawn(
            id,
            self.venue.clone(),
            self.cfg.shard.clone(),
            self.out.clone(),
            self.stats.clone(),
            self.shutdown.clone(),
        );
        self.shards.push(Shard { handle, size: 0 });
        id
    }
}

/// Time until the next multiple of `period` since the epoch, so minute
/// snapshots land on the wall-clock minute.
fn until_aligned(period: Duration) -> Duration {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let period_nanos = period.as_nanos().max(1);
    let into = since_epoch.as_nanos() % period_nanos;
    Duration::from_nanos((period_nanos - into) as u64)
}

pub async fn run<V: Venue>(
    venue: Arc<V>,
    cfg: Config,
    out: crossbeam::channel::Sender<OutMsg>,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut manager = Manager {
        venue: venue.clone(),
        cfg: cfg.clone(),
        out,
        stats: stats.clone(),
        shutdown: shutdown.clone(),
        shards: Vec::new(),
        assigned: HashMap::new(),
    };

    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let lifecycle = venue.subscribe_lifecycle(0).map(|_| {
        let spec = ConnSpec {
            label: "lifecycle".into(),
            slot: 0,
            url: venue.ws_urls()[0].clone(),
            mode: Mode::Lifecycle,
        };
        stats.sockets.add(1);
        conn::spawn(venue.clone(), spec, Vec::new(), lifecycle_tx, stats.clone(), shutdown.clone())
    });

    let (listing_tx, mut listing_rx) = mpsc::unbounded_channel::<Listing>();
    let start_listing = |id: u64| {
        let venue = venue.clone();
        let tx = listing_tx.clone();
        tokio::spawn(async move {
            let (pages_tx, mut pages_rx) = mpsc::unbounded_channel();
            let forward = {
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(page) = pages_rx.recv().await {
                        let _ = tx.send(Listing::Page(id, page));
                    }
                })
            };
            let result = venue.list_markets(pages_tx).await;
            let _ = forward.await;
            let _ = tx.send(Listing::Done(id, result));
        });
    };
    let mut listing_id = 1u64;
    // (id, started, markets seen so far) of the listing in progress.
    let mut listing: Option<(u64, Instant, HashSet<String>)> = Some((listing_id, Instant::now(), HashSet::new()));
    start_listing(listing_id);
    stats.listings.inc();

    let (confirm_tx, mut confirm_rx) = mpsc::unbounded_channel::<String>();
    let mut listing_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + cfg.listing_every,
        cfg.listing_every,
    );
    let mut snapshot_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + until_aligned(cfg.snapshot_every),
        cfg.snapshot_every,
    );
    snapshot_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let mut list_now = false;
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(msg) = listing_rx.recv() => match msg {
                Listing::Page(id, markets) => {
                    if let Some((_, _, seen)) = listing.as_mut().filter(|l| l.0 == id) {
                        seen.extend(markets.iter().cloned());
                    }
                    manager.add(markets);
                }
                Listing::Done(id, result) => {
                    let Some((_, started, seen)) = listing.take_if(|l| l.0 == id) else { continue };
                    match result {
                        Ok(total) => {
                            // Markets added after the listing began may be missing
                            // from pages fetched earlier, so they are kept.
                            let closed: Vec<String> = manager
                                .assigned
                                .iter()
                                .filter(|(m, a)| a.added < started && !seen.contains(*m))
                                .map(|(m, _)| m.clone())
                                .collect();
                            tracing::info!(listed = total, tracked = manager.assigned.len(), closed = closed.len(),
                                shards = manager.shards.len(), secs = started.elapsed().as_secs(), "market listing done");
                            manager.remove(closed);
                        }
                        Err(error) => {
                            stats.listing_failures.inc();
                            tracing::warn!(error = %format!("{error:#}"), "market listing failed; removing nothing");
                        }
                    }
                }
            },
            Some(event) = lifecycle_rx.recv() => match event.kind {
                ConnEventKind::Lifecycle(Lifecycle::Opened { market, open_at }) => {
                    stats.lifecycle_opened.inc();
                    if !manager.assigned.contains_key(&market) {
                        let venue = venue.clone();
                        let tx = confirm_tx.clone();
                        tokio::spawn(async move {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                            if let Some(wait) = open_at.filter(|&t| t > now).map(|t| (t - now) as u64) {
                                tokio::time::sleep(Duration::from_secs(wait)).await;
                            }
                            match venue.confirm_market(market.clone()).await {
                                Ok(true) => { let _ = tx.send(market); }
                                Ok(false) => tracing::debug!(%market, "announced market is not collectable"),
                                Err(error) => tracing::warn!(%market, error = %format!("{error:#}"), "couldn't confirm new market"),
                            }
                        });
                    }
                }
                ConnEventKind::Lifecycle(Lifecycle::Closed { market }) => {
                    stats.lifecycle_closed.inc();
                    manager.remove(vec![market]);
                }
                // Lifecycle events may have been missed: list everything now.
                ConnEventKind::Gap | ConnEventKind::Down { .. } => list_now = true,
                _ => {}
            },
            Some(market) = confirm_rx.recv() => {
                tracing::info!(%market, "new market from the lifecycle feed");
                manager.add(vec![market]);
            }
            _ = listing_timer.tick() => list_now = true,
            _ = snapshot_timer.tick() => {
                for shard in &manager.shards {
                    let _ = shard.handle.cmds.send(ShardCmd::SnapshotAll);
                }
            }
        }
        if list_now && listing.is_none() {
            listing_id += 1;
            listing = Some((listing_id, Instant::now(), HashSet::new()));
            start_listing(listing_id);
            stats.listings.inc();
        }
    }

    for shard in manager.shards.drain(..) {
        let _ = shard.handle.task.await;
    }
    if let Some(lifecycle) = lifecycle {
        let _ = lifecycle.task.await;
        stats.sockets.add(-1);
    }
}
