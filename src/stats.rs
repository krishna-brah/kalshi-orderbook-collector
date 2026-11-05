//! Counters and latency histograms shared by every task, plus the periodic
//! status line and the final run summary.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use hdrhistogram::Histogram;
use serde_json::{Value, json};

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Relaxed);
    }

    pub fn set(&self, n: i64) {
        self.0.store(n, Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Relaxed)
    }
}

/// Microsecond latency histogram (1 µs to 1 hour, 3 significant digits).
pub struct Latency {
    hist: Mutex<Histogram<u64>>,
    /// Samples below zero: the exchange's clock is ahead of ours.
    negative: Counter,
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            hist: Mutex::new(Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("valid bounds")),
            negative: Counter::default(),
        }
    }
}

impl Latency {
    pub fn record(&self, micros: i64) {
        if micros < 0 {
            self.negative.inc();
        }
        let mut hist = self.hist.lock().unwrap_or_else(|e| e.into_inner());
        hist.saturating_record(micros.max(1) as u64);
    }

    pub fn count(&self) -> u64 {
        self.hist.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Value at quantile `q` in milliseconds.
    pub fn quantile_ms(&self, q: f64) -> Option<f64> {
        let hist = self.hist.lock().unwrap_or_else(|e| e.into_inner());
        (!hist.is_empty()).then(|| hist.value_at_quantile(q) as f64 / 1_000.0)
    }

    fn summary(&self) -> Value {
        let ms = |q| self.quantile_ms(q).map(|v| (v * 1_000.0).round() / 1_000.0);
        json!({
            "samples": self.count(),
            "p50_ms": ms(0.5),
            "p90_ms": ms(0.9),
            "p99_ms": ms(0.99),
            "p999_ms": ms(0.999),
            "max_ms": ms(1.0),
            "negative_samples": self.negative.get(),
        })
    }
}

/// Per route: connection slot `i` of every shard uses websocket URL `i`.
#[derive(Default)]
pub struct RouteStats {
    pub url: String,
    pub frames: Counter,
    pub bytes: Counter,
    /// Receive time minus the exchange's send stamp.
    pub delay: Latency,
    /// Book changes that arrived here before any other copy.
    pub first_arrivals: Counter,
    /// How much earlier, when they did.
    pub lead: Latency,
}

pub struct Stats {
    started: Instant,
    pub routes: Vec<RouteStats>,

    pub markets: Gauge,
    pub sockets_up: Gauge,
    pub sockets: Gauge,

    pub snapshots_in: Counter,
    pub deltas_in: Counter,
    pub updates_out: Counter,
    pub snapshots_out_exchange: Counter,
    pub snapshots_out_failover: Counter,
    pub snapshots_out_lag: Counter,
    pub snapshots_out_periodic: Counter,

    pub stale_deltas: Counter,
    pub gaps: Counter,
    pub decode_errors: Counter,
    pub negative_levels: Counter,
    pub reconnects: Counter,
    pub recycles: Counter,
    pub refused: Counter,
    pub uncovered: Counter,

    pub probes: Counter,
    pub probe_mismatches: Counter,
    pub quiet_resubscribes: Counter,

    pub listings: Counter,
    pub listing_failures: Counter,
    pub lifecycle_opened: Counter,
    pub lifecycle_closed: Counter,
    pub markets_added: Counter,
    pub markets_removed: Counter,

    pub written: Counter,
    pub writer_queue: Gauge,
}

impl Stats {
    pub fn new(urls: &[String]) -> Self {
        Self {
            started: Instant::now(),
            routes: urls.iter().map(|url| RouteStats { url: url.clone(), ..Default::default() }).collect(),
            markets: Gauge::default(),
            sockets_up: Gauge::default(),
            sockets: Gauge::default(),
            snapshots_in: Counter::default(),
            deltas_in: Counter::default(),
            updates_out: Counter::default(),
            snapshots_out_exchange: Counter::default(),
            snapshots_out_failover: Counter::default(),
            snapshots_out_lag: Counter::default(),
            snapshots_out_periodic: Counter::default(),
            stale_deltas: Counter::default(),
            gaps: Counter::default(),
            decode_errors: Counter::default(),
            negative_levels: Counter::default(),
            reconnects: Counter::default(),
            recycles: Counter::default(),
            refused: Counter::default(),
            uncovered: Counter::default(),
            probes: Counter::default(),
            probe_mismatches: Counter::default(),
            quiet_resubscribes: Counter::default(),
            listings: Counter::default(),
            listing_failures: Counter::default(),
            lifecycle_opened: Counter::default(),
            lifecycle_closed: Counter::default(),
            markets_added: Counter::default(),
            markets_removed: Counter::default(),
            written: Counter::default(),
            writer_queue: Gauge::default(),
        }
    }

    pub fn route(&self, slot: usize) -> &RouteStats {
        &self.routes[slot % self.routes.len()]
    }

    fn snapshots_out(&self) -> u64 {
        self.snapshots_out_exchange.get()
            + self.snapshots_out_failover.get()
            + self.snapshots_out_lag.get()
            + self.snapshots_out_periodic.get()
    }

    /// One status line; rates are per second since `prev`.
    pub fn line(&self, prev: &mut Rates) -> String {
        let now = Instant::now();
        let secs = now.duration_since(prev.at).as_secs_f64().max(1e-9);
        let frames: u64 = self.routes.iter().map(|r| r.frames.get()).sum();
        let rate = |now: u64, then: u64| (now - then) as f64 / secs;
        let delays: Vec<String> = self
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                format!(
                    "r{i} {}/{}ms",
                    r.delay.quantile_ms(0.5).map_or("-".into(), |v| format!("{v:.0}")),
                    r.delay.quantile_ms(0.99).map_or("-".into(), |v| format!("{v:.0}"))
                )
            })
            .collect();
        let line = format!(
            "markets {} | sockets {}/{} | in {:.0}/s | out {:.0} upd/s | snaps {} | gaps {} | switches {}+{} | \
             probes {} bad {} | quiet {} | stale {} | queue {} | delay p50/p99 {}",
            self.markets.get(),
            self.sockets_up.get(),
            self.sockets.get(),
            rate(frames, prev.frames),
            rate(self.updates_out.get(), prev.updates),
            self.snapshots_out(),
            self.gaps.get(),
            self.snapshots_out_failover.get(),
            self.snapshots_out_lag.get(),
            self.probes.get(),
            self.probe_mismatches.get(),
            self.quiet_resubscribes.get(),
            self.stale_deltas.get(),
            self.writer_queue.get(),
            delays.join(" "),
        );
        *prev = Rates { at: now, frames, updates: self.updates_out.get() };
        line
    }

    pub fn summary(&self) -> Value {
        json!({
            "runtime_secs": self.started.elapsed().as_secs(),
            "markets_tracked": self.markets.get(),
            "markets_added": self.markets_added.get(),
            "markets_removed": self.markets_removed.get(),
            "sockets": self.sockets.get(),
            "book_snapshots_in": self.snapshots_in.get(),
            "book_deltas_in": self.deltas_in.get(),
            "price_level_updates_out": self.updates_out.get(),
            "snapshots_out": {
                "exchange_or_resync": self.snapshots_out_exchange.get(),
                "failover": self.snapshots_out_failover.get(),
                "lagging_source": self.snapshots_out_lag.get(),
                "periodic": self.snapshots_out_periodic.get(),
            },
            "messages_written": self.written.get(),
            "sequence_gaps": self.gaps.get(),
            "decode_errors": self.decode_errors.get(),
            "negative_levels": self.negative_levels.get(),
            "stale_deltas_ignored": self.stale_deltas.get(),
            "reconnects": self.reconnects.get(),
            "planned_recycles": self.recycles.get(),
            "refused_markets": self.refused.get(),
            "uncovered_markets": self.uncovered.get(),
            "audit_probes": self.probes.get(),
            "audit_mismatches": self.probe_mismatches.get(),
            "quiet_resubscribes": self.quiet_resubscribes.get(),
            "listings": self.listings.get(),
            "listing_failures": self.listing_failures.get(),
            "lifecycle_opened": self.lifecycle_opened.get(),
            "lifecycle_closed": self.lifecycle_closed.get(),
            "routes": self.routes.iter().map(|r| json!({
                "url": r.url,
                "frames": r.frames.get(),
                "bytes": r.bytes.get(),
                "delay_vs_exchange_send_stamp": r.delay.summary(),
                "first_arrivals": r.first_arrivals.get(),
                "lead_when_first": r.lead.summary(),
            })).collect::<Vec<_>>(),
        })
    }
}

pub struct Rates {
    at: Instant,
    frames: u64,
    updates: u64,
}

impl Default for Rates {
    fn default() -> Self {
        Self { at: Instant::now(), frames: 0, updates: 0 }
    }
}
