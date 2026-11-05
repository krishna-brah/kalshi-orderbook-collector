//! End-to-end runs against a fake Kalshi exchange on localhost that breaks
//! one of the two sockets in the ways the challenge describes. Whatever goes
//! wrong, the output file must replay to exactly the exchange's final books.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engineering_challenge::Side;
use engineering_challenge::book::{Book, LevelChange};
use engineering_challenge::collector::{self, Config};
use engineering_challenge::model::Frame;
use engineering_challenge::shard::ShardConfig;
use engineering_challenge::stats::Stats;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use engineering_challenge::venue::Venue;
use engineering_challenge::venue::kalshi::{Env, Kalshi, Signer};
use engineering_challenge::writer::Writer;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::handshake::server::{Request as ServerRequest, Response};

const MARKETS: [&str; 4] = ["MKT-A", "MKT-B", "MKT-C", "MKT-D"];

// ---------------------------------------------------------------- exchange

#[derive(Clone, Debug)]
struct Change {
    market: String,
    clock: u64,
    yes: bool,
    cents: i64,
    delta: i64,
    ts_ms: i64,
}

#[derive(Default)]
struct Exchange {
    clock: u64,
    /// Per market: YES bids and NO bids, price in cents to contracts.
    books: BTreeMap<String, (BTreeMap<i64, i64>, BTreeMap<i64, i64>)>,
    last_clock: HashMap<String, u64>,
}

impl Exchange {
    /// Kalshi with `use_yes_price`: NO bids are reported at the YES price.
    fn snapshot_frame(&self, market: &str, seq: u64) -> String {
        let empty = (BTreeMap::new(), BTreeMap::new());
        let (yes, no) = self.books.get(market).unwrap_or(&empty);
        let levels = |side: &BTreeMap<i64, i64>, yes_leg: bool| -> Vec<Value> {
            side.iter()
                .map(|(&c, q)| {
                    let c = if yes_leg { c } else { 100 - c };
                    json!([format!("0.{c:02}00"), format!("{q}.00")])
                })
                .collect()
        };
        json!({
            "type": "orderbook_snapshot", "sid": 1, "seq": seq, "sending_ts_ms": now_ms(),
            "msg": {
                "market_ticker": market, "market_id": "x",
                "clock": self.last_clock.get(market).copied().unwrap_or(0),
                "yes_dollars_fp": levels(yes, true), "no_dollars_fp": levels(no, false),
            }
        })
        .to_string()
    }

    /// The final books in the collector's units: YES bids, and NO bids as
    /// YES asks at 1 - p.
    fn truth(&self) -> HashMap<String, Book> {
        self.books
            .iter()
            .map(|(market, (yes, no))| {
                let bids = yes.iter().map(|(&c, &q)| (c * 10_000, q * 1_000_000));
                let asks = no
                    .iter()
                    .map(|(&c, &q)| (1_000_000 - c * 10_000, q * 1_000_000));
                (market.clone(), Book::from_levels(bids, asks))
            })
            .collect()
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn delta_frame(change: &Change, seq: u64) -> String {
    let cents = if change.yes {
        change.cents
    } else {
        100 - change.cents
    };
    json!({
        "type": "orderbook_delta", "sid": 1, "seq": seq, "sending_ts_ms": now_ms(),
        "msg": {
            "market_ticker": change.market, "market_id": "x", "clock": change.clock,
            "price_dollars": format!("0.{cents:02}00"), "delta_fp": format!("{}.00", change.delta),
            "side": if change.yes { "yes" } else { "no" }, "ts_ms": change.ts_ms,
        }
    })
    .to_string()
}

/// Generates random book changes and broadcasts them to every connection.
async fn drive(
    exchange: Arc<Mutex<Exchange>>,
    events: broadcast::Sender<Change>,
    count: usize,
    seed: u64,
) {
    let mut rng = fastrand::Rng::with_seed(seed);
    for _ in 0..count {
        {
            let mut ex = exchange.lock().unwrap();
            ex.clock += rng.u64(1..4); // clocks skip values, like Kalshi's
            let clock = ex.clock;
            let market = MARKETS[rng.usize(..MARKETS.len())].to_owned();
            let yes = rng.bool();
            let cents = rng.i64(1..100);
            let entry = ex.books.entry(market.clone()).or_default();
            let side = if yes { &mut entry.0 } else { &mut entry.1 };
            let current = side.get(&cents).copied().unwrap_or(0);
            let delta = if current > 0 && rng.bool() {
                -rng.i64(1..=current)
            } else {
                rng.i64(1..50)
            };
            if current + delta == 0 {
                side.remove(&cents);
            } else {
                side.insert(cents, current + delta);
            }
            ex.last_clock.insert(market.clone(), clock);
            let _ = events.send(Change {
                market,
                clock,
                yes,
                cents,
                delta,
                ts_ms: now_ms(),
            });
        }
        tokio::time::sleep(Duration::from_millis(4)).await;
    }
}

/// What the first connection on a route does wrong.
#[derive(Clone, Default)]
struct Faults {
    /// Hold back the subscription for this long, so the other route's copy
    /// becomes every market's source.
    slow_start: Option<Duration>,
    /// Close the socket after forwarding this many changes.
    drop_after: Option<usize>,
    /// Lose the change at this index (the sequence number jumps).
    lose_at: Option<usize>,
    /// Stop forwarding this market after this many changes, with no gap in
    /// the sequence: a subscription that silently went quiet.
    mute: Option<(&'static str, usize)>,
    /// Deliver every frame this much later than the other route.
    delay: Option<Duration>,
    /// Answer the first subscribe with Kalshi's transient "command timeout".
    refuse_first_subscribe: bool,
}

async fn serve(
    stream: TcpStream,
    exchange: Arc<Mutex<Exchange>>,
    events: broadcast::Sender<Change>,
    faults: [Faults; 2],
    connections: Arc<[AtomicUsize; 2]>,
) {
    let route = Arc::new(Mutex::new(0usize));
    let route_cb = route.clone();
    let callback = move |req: &ServerRequest, resp: Response| {
        *route_cb.lock().unwrap() = usize::from(req.uri().path().ends_with("/b"));
        Ok(resp)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let route = *route.lock().unwrap();
    let first = connections[route].fetch_add(1, Ordering::SeqCst) == 0;
    let faults = if first {
        faults[route].clone()
    } else {
        Faults::default()
    };
    let (mut sink, mut commands) = ws.split();
    let mut changes = events.subscribe();

    // Frames go out through a queue so a delayed route stays ordered.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<(Instant, String)>();
    tokio::spawn(async move {
        while let Some((due, text)) = out_rx.recv().await {
            tokio::time::sleep_until(due.into()).await;
            if sink.send(Message::text(text)).await.is_err() {
                return;
            }
        }
    });
    if let Some(wait) = faults.slow_start {
        tokio::time::sleep(wait).await;
    }
    let mut feed = Feed {
        out: out_tx,
        faults,
        subscribed: HashMap::new(),
        seq: 0,
        forwarded: 0,
    };
    let mut refused_once = false;
    loop {
        tokio::select! {
            command = commands.next() => {
                let Some(Ok(Message::Text(text))) = command else { return };
                let cmd: Value = serde_json::from_str(&text).unwrap();
                let id = cmd["id"].as_u64().unwrap_or(0);
                let markets: Vec<String> = cmd["params"]["market_tickers"].as_array().map(|a| {
                    a.iter().map(|m| m.as_str().unwrap().to_owned()).collect()
                }).unwrap_or_default();
                let action = cmd["params"]["action"].as_str().unwrap_or("");
                match (cmd["cmd"].as_str().unwrap_or(""), action) {
                    ("subscribe", _) => {
                        assert_eq!(cmd["params"]["use_yes_price"], true, "frames below assume yes-leg pricing");
                        if feed.faults.refuse_first_subscribe && !refused_once {
                            refused_once = true;
                            feed.send(json!({"id": id, "type": "error", "msg": {"code": 18, "msg": "Command timeout"}}).to_string());
                            continue;
                        }
                        feed.send(json!({"id": id, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}}).to_string());
                    }
                    ("update_subscription", "add_markets") => feed.send(json!({"id": id, "type": "ok", "sid": 1}).to_string()),
                    ("update_subscription", "delete_markets") => {
                        for m in &markets {
                            feed.subscribed.remove(m);
                        }
                        continue;
                    }
                    ("update_subscription", "get_snapshot") => {}
                    other => panic!("unexpected command {other:?}"),
                }
                // Like Kalshi: every change broadcast before this snapshot
                // goes out on this socket before it. Holding the lock stops
                // new changes while the backlog is flushed.
                let ex = exchange.lock().unwrap();
                loop {
                    match changes.try_recv() {
                        Ok(change) => {
                            if !feed.forward(change) {
                                return;
                            }
                        }
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(error) => panic!("test broadcast failed: {error}"),
                    }
                }
                for market in markets {
                    feed.seq += 1;
                    feed.send(ex.snapshot_frame(&market, feed.seq));
                    feed.subscribed.insert(market.clone(), ex.last_clock.get(&market).copied().unwrap_or(0));
                }
            }
            change = changes.recv() => {
                if !feed.forward(change.expect("test broadcast should not lag")) {
                    return;
                }
            }
        }
    }
}

/// One socket's outgoing stream: which markets it carries, its sequence
/// numbers, and the faults it injects.
struct Feed {
    out: mpsc::UnboundedSender<(Instant, String)>,
    faults: Faults,
    /// Market to the clock of the snapshot this socket sent for it.
    subscribed: HashMap<String, u64>,
    seq: u64,
    forwarded: usize,
}

impl Feed {
    fn send(&self, text: String) {
        let _ = self.out.send((Instant::now(), text));
    }

    /// Sends a change if this socket carries its market. Only book changes
    /// are delayed, so a delayed route still wins the first snapshots and
    /// starts as the source. Returns false when the socket should drop.
    fn forward(&mut self, change: Change) -> bool {
        let Some(&from) = self.subscribed.get(&change.market) else {
            return true;
        };
        if change.clock <= from {
            return true; // already in the snapshot this socket sent
        }
        let index = self.forwarded;
        self.forwarded += 1;
        if self.faults.drop_after == Some(index) {
            return false;
        }
        if self.faults.lose_at == Some(index) {
            self.seq += 1;
            return true;
        }
        if self
            .faults
            .mute
            .is_some_and(|(m, after)| m == change.market && index >= after)
        {
            return true;
        }
        self.seq += 1;
        let due = Instant::now() + self.faults.delay.unwrap_or_default();
        let _ = self.out.send((due, delta_frame(&change, self.seq)));
        true
    }
}

// ----------------------------------------------------------------- venue

/// Kalshi's encoding and decoding, with a fixed market list instead of REST.
struct TestVenue(Kalshi);

impl Venue for TestVenue {
    fn name(&self) -> &'static str {
        "kalshi-fake"
    }
    fn ws_urls(&self) -> &[String] {
        self.0.ws_urls()
    }
    fn connect_request(&self, url: &str) -> anyhow::Result<Request> {
        self.0.connect_request(url)
    }
    fn command_batch(&self) -> usize {
        self.0.command_batch()
    }
    fn subscribe_books(&self, req_id: u64, markets: &[String]) -> String {
        self.0.subscribe_books(req_id, markets)
    }
    fn add_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String {
        self.0.add_markets(req_id, sid, markets)
    }
    fn remove_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String {
        self.0.remove_markets(req_id, sid, markets)
    }
    fn request_snapshots(
        &self,
        req_id: u64,
        sid: Option<u64>,
        markets: &[String],
    ) -> Option<String> {
        self.0.request_snapshots(req_id, sid, markets)
    }
    fn subscribe_lifecycle(&self, _req_id: u64) -> Option<String> {
        None
    }
    fn acks_commands(&self) -> bool {
        true
    }
    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        None
    }
    fn decode(&self, text: &str, out: &mut Vec<Frame>) -> anyhow::Result<()> {
        self.0.decode(text, out)
    }
    async fn list_markets(
        &self,
        pages: mpsc::UnboundedSender<Vec<String>>,
    ) -> anyhow::Result<usize> {
        let _ = pages.send(MARKETS.iter().map(|m| (*m).to_owned()).collect());
        Ok(MARKETS.len())
    }
    async fn confirm_market(&self, _market: String) -> anyhow::Result<bool> {
        Ok(true)
    }
}

// --------------------------------------------------------------- harness

struct Outcome {
    stats: Arc<Stats>,
    truth: HashMap<String, Book>,
    replayed: HashMap<String, Book>,
    snapshots: u64,
    updates: u64,
}

async fn run(faults: [Faults; 2], lag: Duration, quiet: Duration, changes: usize) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let exchange = Arc::new(Mutex::new(Exchange::default()));
    let (events, _) = broadcast::channel(100_000);
    let connections = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    {
        let (exchange, events, connections) =
            (exchange.clone(), events.clone(), connections.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(
                    stream,
                    exchange.clone(),
                    events.clone(),
                    faults.clone(),
                    connections.clone(),
                ));
            }
        });
    }

    let key = ed25519_dalek::SigningKey::from_bytes(&[1; 32]);
    let urls = vec![
        format!("ws://127.0.0.1:{port}/a"),
        format!("ws://127.0.0.1:{port}/b"),
    ];
    let venue = Arc::new(TestVenue(
        Kalshi::new(Env::Demo, "test-key".into(), Signer::Ed25519(key))
            .unwrap()
            .with_ws_urls(urls),
    ));
    let stats = Arc::new(Stats::new(venue.ws_urls()));
    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = Writer::start(file.path().to_str().unwrap(), stats.clone()).unwrap();
    let config = Config {
        markets_per_socket: 100,
        max_markets: None,
        fixed_markets: None,
        snapshot_every: Duration::from_secs(1),
        listing_every: Duration::from_secs(3_600),
        shard: ShardConfig {
            redundancy: 2,
            lag_threshold: lag,
            probe_every: Duration::from_millis(500),
            probe_batch: 2,
            quiet_after: quiet,
            recycle_every: None,
        },
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let collector = tokio::spawn(collector::run(
        venue,
        config,
        writer.tx.clone(),
        stats.clone(),
        shutdown_rx,
    ));

    tokio::time::sleep(Duration::from_millis(700)).await;
    drive(exchange.clone(), events.clone(), changes, 7).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), collector)
        .await
        .unwrap()
        .unwrap();
    writer.finish().unwrap();

    let (replayed, snapshots, updates) = replay(file.path());
    let truth = exchange.lock().unwrap().truth();
    Outcome {
        stats,
        truth,
        replayed,
        snapshots,
        updates,
    }
}

/// Rebuilds every book from the output file, failing on anything malformed.
fn replay(path: &std::path::Path) -> (HashMap<String, Book>, u64, u64) {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut books: HashMap<String, Book> = HashMap::new();
    let (mut snapshots, mut updates) = (0, 0);
    while let Some(msg) = reader.read_msg().unwrap() {
        match msg {
            AnyMessage::Snapshot(decoded) => {
                let s = SnapshotDomain::try_from_decoder(decoded).unwrap();
                snapshots += 1;
                let bids = s.bids.iter().map(|l| (l.price_level, l.quantity));
                let asks = s.asks.iter().map(|l| (l.price_level, l.quantity));
                books.insert(s.symbol, Book::from_levels(bids, asks));
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                updates += 1;
                let book = books
                    .get_mut(&u.symbol)
                    .expect("update before any snapshot of its book");
                let side: Side = u.side;
                book.apply(side, u.price_level, LevelChange::Absolute(u.quantity))
                    .unwrap();
            }
            _ => panic!("unexpected message type"),
        }
    }
    (books, snapshots, updates)
}

fn assert_exact(outcome: &Outcome) {
    assert!(
        outcome.updates > 100,
        "only {} updates written",
        outcome.updates
    );
    for market in MARKETS {
        assert_eq!(
            outcome.replayed.get(market),
            outcome.truth.get(market),
            "replayed book for {market} differs from the exchange's"
        );
    }
}

fn faults(a: Faults) -> [Faults; 2] {
    // Route b starts late, so route a's copy is every market's source.
    [
        a,
        Faults {
            slow_start: Some(Duration::from_millis(300)),
            ..Faults::default()
        },
    ]
}

// ----------------------------------------------------------------- tests

#[tokio::test(flavor = "multi_thread")]
async fn clean_run_replays_exactly() {
    let outcome = run(
        faults(Faults::default()),
        Duration::from_millis(200),
        Duration::from_secs(30),
        400,
    )
    .await;
    assert_exact(&outcome);
    assert_eq!(outcome.stats.snapshots_out_failover.get(), 0);
    assert_eq!(outcome.stats.gaps.get(), 0);
    assert!(outcome.stats.probes.get() > 0, "audit probes ran");
    assert_eq!(outcome.stats.probe_mismatches.get(), 0);
    assert!(
        outcome.snapshots >= MARKETS.len() as u64 * 2,
        "first snapshots plus minute snapshots"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_socket_fails_over_and_loses_nothing() {
    let a = Faults {
        drop_after: Some(60),
        ..Faults::default()
    };
    let outcome = run(
        faults(a),
        Duration::from_millis(200),
        Duration::from_secs(30),
        400,
    )
    .await;
    assert_exact(&outcome);
    assert!(outcome.stats.reconnects.get() >= 1);
    assert!(
        outcome.stats.snapshots_out_failover.get() >= MARKETS.len() as u64,
        "every market moved to the other copy with a snapshot"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_frame_is_detected_and_resnapshotted() {
    let a = Faults {
        lose_at: Some(40),
        ..Faults::default()
    };
    let outcome = run(
        faults(a),
        Duration::from_millis(200),
        Duration::from_secs(30),
        400,
    )
    .await;
    assert_exact(&outcome);
    assert_eq!(outcome.stats.gaps.get(), 1);
    assert!(outcome.stats.snapshots_out_failover.get() >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_market_that_goes_quiet_on_one_socket_moves_and_is_resubscribed() {
    let a = Faults {
        mute: Some(("MKT-B", 30)),
        ..Faults::default()
    };
    let outcome = run(
        faults(a),
        Duration::from_millis(200),
        Duration::from_millis(800),
        500,
    )
    .await;
    assert_exact(&outcome);
    assert!(
        outcome.stats.snapshots_out_lag.get() >= 1,
        "MKT-B switched to the copy that kept changing"
    );
    assert!(
        outcome.stats.quiet_resubscribes.get() >= 1,
        "the quiet copy was resubscribed"
    );
    assert_eq!(
        outcome.stats.gaps.get(),
        0,
        "no sequence gap: only the cross-copy check could catch this"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transient_refusal_is_retried_not_forgotten() {
    let a = Faults {
        refuse_first_subscribe: true,
        ..Faults::default()
    };
    let outcome = run(
        faults(a),
        Duration::from_millis(200),
        Duration::from_secs(30),
        400,
    )
    .await;
    assert_exact(&outcome);
    assert_eq!(
        outcome.stats.refused.get(),
        0,
        "a timeout is retried, not treated as a bad ticker"
    );
    let (probes, mismatches) = (
        outcome.stats.probes.get(),
        outcome.stats.probe_mismatches.get(),
    );
    assert!(
        probes > 0 && mismatches == 0,
        "probes {probes}, mismatches {mismatches}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_socket_loses_the_source_role() {
    let a = Faults {
        delay: Some(Duration::from_millis(400)),
        ..Faults::default()
    };
    let outcome = run(
        faults(a),
        Duration::from_millis(150),
        Duration::from_secs(30),
        400,
    )
    .await;
    assert_exact(&outcome);
    assert!(
        outcome.stats.snapshots_out_lag.get() >= MARKETS.len() as u64,
        "every market moved to the faster copy"
    );
    assert!(
        outcome.stats.route(1).first_arrivals.get() > outcome.stats.route(0).first_arrivals.get()
    );
}
