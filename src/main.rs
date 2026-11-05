//! Records every live order book on a venue (Kalshi by default): price-level
//! deltas plus a full snapshot every minute, written with the challenge's
//! `AlignedSbeWriter`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use engineering_challenge::collector::{self, Config};
use engineering_challenge::shard::ShardConfig;
use engineering_challenge::stats::{Rates, Stats};
use engineering_challenge::venue::Venue;
use engineering_challenge::venue::kalshi::{Env, Kalshi, Signer};
use engineering_challenge::venue::polymarket::Polymarket;
use engineering_challenge::writer::Writer;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum VenueName {
    Kalshi,
    Polymarket,
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long, value_enum, default_value_t = VenueName::Kalshi)]
    venue: VenueName,
    /// Kalshi environment.
    #[arg(long, value_enum, default_value_t = Env::Prod)]
    env: Env,
    /// Kalshi API key id.
    #[arg(long, env = "KALSHI_KEY_ID", hide_env_values = true)]
    key_id: Option<String>,
    /// Kalshi private key file (Ed25519 or RSA, PEM).
    #[arg(long, env = "KALSHI_KEY_FILE")]
    key_file: Option<PathBuf>,
    /// Output file, or `-` for stdout. Logs always go to stderr.
    #[arg(long, short, default_value = "-")]
    out: String,
    /// Copies of every book, each on its own socket (spread across the
    /// venue's websocket hosts).
    #[arg(long, default_value_t = 2)]
    redundancy: usize,
    /// Markets per socket [default: 3000 on Kalshi, 500 on Polymarket].
    #[arg(long)]
    markets_per_socket: Option<usize>,
    /// Collect at most this many markets (for testing).
    #[arg(long)]
    max_markets: Option<usize>,
    /// Collect exactly the markets listed in this file, one per line,
    /// instead of discovering them (for experiments).
    #[arg(long)]
    markets_file: Option<PathBuf>,
    /// Seconds between full snapshots of every book (aligned to the clock).
    #[arg(long, default_value_t = 60)]
    snapshot_secs: u64,
    /// Seconds between full market listings (catches new or closed markets
    /// the lifecycle feed missed).
    #[arg(long, default_value_t = 600)]
    listing_secs: u64,
    /// Switch a market to another copy once its source trails by this long.
    #[arg(long, default_value_t = 500)]
    lag_ms: u64,
    /// Seconds between audit probes (fresh venue snapshots compared with our
    /// books); 0 turns them off.
    #[arg(long, default_value_t = 5)]
    probe_secs: u64,
    /// Markets audited per socket per probe.
    #[arg(long, default_value_t = 4)]
    probe_batch: usize,
    /// A copy silent this long while another copy changes is resubscribed.
    #[arg(long, default_value_t = 60)]
    quiet_secs: u64,
    /// Minutes between planned reconnects of each socket; 0 turns them off.
    #[arg(long, default_value_t = 30)]
    recycle_mins: u64,
    /// Seconds between status lines on stderr.
    #[arg(long, default_value_t = 10)]
    stats_secs: u64,
    /// Also write the run summary (JSON) to this file on exit.
    #[arg(long)]
    summary: Option<PathBuf>,
    /// Override the websocket URLs (repeatable), e.g. a local test server.
    #[arg(long = "ws-url")]
    ws_urls: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run(args))
}

async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(args.redundancy >= 1, "--redundancy must be at least 1");
    match args.venue {
        VenueName::Kalshi => {
            let key_id = args.key_id.clone().context("Kalshi needs --key-id (or KALSHI_KEY_ID)")?;
            let key_file = args.key_file.clone().context("Kalshi needs --key-file (or KALSHI_KEY_FILE)")?;
            let pem = std::fs::read_to_string(&key_file).with_context(|| format!("reading {}", key_file.display()))?;
            let mut venue = Kalshi::new(args.env, key_id, Signer::from_pem(&pem)?)?;
            if !args.ws_urls.is_empty() {
                venue = venue.with_ws_urls(args.ws_urls.clone());
            }
            collect(Arc::new(venue), args, 3_000).await
        }
        VenueName::Polymarket => {
            let mut venue = Polymarket::new()?;
            if !args.ws_urls.is_empty() {
                venue = venue.with_ws_urls(args.ws_urls.clone());
            }
            collect(Arc::new(venue), args, 500).await
        }
    }
}

async fn collect<V: Venue>(venue: Arc<V>, args: Args, default_per_socket: usize) -> anyhow::Result<()> {
    let stats = Arc::new(Stats::new(venue.ws_urls()));
    let writer = Writer::start(&args.out, stats.clone())?;
    let markets_per_socket = args.markets_per_socket.unwrap_or(default_per_socket);

    let probing = args.probe_secs > 0 && venue.snapshots_in_place();
    let fixed_markets = match &args.markets_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect(),
        ),
        None => None,
    };
    let config = Config {
        markets_per_socket,
        max_markets: args.max_markets,
        fixed_markets,
        snapshot_every: Duration::from_secs(args.snapshot_secs.max(1)),
        listing_every: Duration::from_secs(args.listing_secs.max(30)),
        shard: ShardConfig {
            redundancy: args.redundancy,
            lag_threshold: Duration::from_millis(args.lag_ms),
            probe_every: Duration::from_secs(if probing { args.probe_secs } else { 3_600 }),
            probe_batch: if probing { args.probe_batch } else { 0 },
            quiet_after: Duration::from_secs(args.quiet_secs.max(1)),
            recycle_every: (args.recycle_mins > 0).then(|| Duration::from_secs(args.recycle_mins * 60)),
        },
    };
    tracing::info!(venue = venue.name(), env = ?args.env, out = %args.out, redundancy = args.redundancy,
        markets_per_socket, urls = ?venue.ws_urls(), "starting");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut collector = tokio::spawn(collector::run(venue, config, writer.tx.clone(), stats.clone(), shutdown_rx));
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut status = tokio::time::interval(Duration::from_secs(args.stats_secs.max(1)));
    status.tick().await;
    let mut rates = Rates::default();
    let mut failed = false;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT: closing sockets and the output file");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM: closing sockets and the output file");
                break;
            }
            result = &mut collector => {
                tracing::error!(?result, "collector stopped unexpectedly");
                failed = true;
                break;
            }
            _ = status.tick() => {
                stats.writer_queue.set(writer.tx.len() as i64);
                tracing::info!("{}", stats.line(&mut rates));
                if writer.is_finished() {
                    tracing::error!("output writer stopped");
                    failed = true;
                    break;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    if !collector.is_finished() && tokio::time::timeout(Duration::from_secs(10), &mut collector).await.is_err() {
        tracing::warn!("sockets did not close within 10 s; finishing the file anyway");
    }
    let written = writer.finish()?;
    let summary = stats.summary();
    tracing::info!(written, "output closed");
    eprintln!("{}", serde_json::to_string_pretty(&summary)?);
    if let Some(path) = &args.summary {
        std::fs::write(path, serde_json::to_string_pretty(&summary)?)?;
    }
    anyhow::ensure!(!failed, "collector failed");
    Ok(())
}
