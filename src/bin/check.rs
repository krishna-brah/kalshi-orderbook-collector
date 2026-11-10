//! Replays a collector output file and checks that it is internally
//! consistent: every update applies to a known book, sizes and prices are
//! valid, snapshots are well formed, and per-market timestamps never go
//! backwards. It also reports how many snapshots matched the book rebuilt
//! from the preceding updates.
//!
//! Usage: check <file> [--json] [--top N] [--corrections]
//!
//! `--top N` also lists the N markets with the most updates. `--corrections`
//! lists every snapshot that changed the replayed book, with its market and
//! time, so each can be matched with the collector's log: a source switch
//! writes one by design.

use std::collections::HashMap;

use anyhow::Context;
use engineering_challenge::Side;
use engineering_challenge::book::{Book, LevelChange};
use engineering_challenge::fixed::SCALE;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use hdrhistogram::Histogram;
use serde_json::json;

#[derive(Default)]
struct Market {
    book: Book,
    last_ts: i64,
    updates: u64,
}

#[derive(Default)]
struct Report {
    messages: u64,
    snapshots: u64,
    updates: u64,
    unknown: u64,
    markets: usize,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    // Hard failures: the file can't be trusted where these occur.
    updates_without_snapshot: u64,
    negative_sizes: u64,
    prices_out_of_range: u64,
    malformed_snapshots: u64,
    timestamp_regressions: u64,
    // Informational.
    snapshots_matching_replay: u64,
    snapshots_correcting_replay: u64,
    crossed_after_update: u64,
    exchange_after_receive: u64,
}

fn valid_price(price: i64) -> bool {
    price > 0 && price < SCALE
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args
        .first()
        .context("usage: check <file> [--json] [--top N] [--corrections]")?
        .clone();
    let json_out = args.iter().any(|a| a == "--json");
    let list_corrections = args.iter().any(|a| a == "--corrections");
    let top: usize = match args.iter().position(|a| a == "--top") {
        Some(i) => args.get(i + 1).context("--top needs a number")?.parse()?,
        None => 0,
    };

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)
        .with_context(|| format!("opening {path}"))?;
    let mut markets: HashMap<String, Market> = HashMap::new();
    let mut report = Report::default();
    let mut delay = Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3)?;

    while let Some(message) = reader.read_msg()? {
        report.messages += 1;
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                report.snapshots += 1;
                let ts = snapshot.received_at.timestamp_micros();
                track_time(&mut report, ts);
                let bids: Vec<(i64, i64)> = snapshot
                    .bids
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let asks: Vec<(i64, i64)> = snapshot
                    .asks
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let ordered = bids.windows(2).all(|w| w[0].0 > w[1].0)
                    && asks.windows(2).all(|w| w[0].0 < w[1].0);
                let sane = bids
                    .iter()
                    .chain(&asks)
                    .all(|&(p, q)| valid_price(p) && q > 0);
                if !ordered || !sane {
                    report.malformed_snapshots += 1;
                }
                let book = Book::from_levels(bids, asks);
                let market = markets.entry(snapshot.symbol.clone()).or_default();
                if market.last_ts != 0 {
                    if market.book == book {
                        report.snapshots_matching_replay += 1;
                    } else {
                        report.snapshots_correcting_replay += 1;
                        if list_corrections {
                            println!("corrected {} at {}", snapshot.symbol, snapshot.received_at);
                        }
                    }
                }
                if ts < market.last_ts {
                    report.timestamp_regressions += 1;
                }
                market.last_ts = market.last_ts.max(ts);
                market.book = book;
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                report.updates += 1;
                let ts = update.backtest_timestamp.timestamp_micros();
                track_time(&mut report, ts);
                let exchange = update.exchange_timestamp.timestamp_micros();
                if exchange > ts {
                    report.exchange_after_receive += 1;
                }
                delay.saturating_record((ts - exchange).max(1) as u64);
                if !valid_price(update.price_level) {
                    report.prices_out_of_range += 1;
                }
                if update.quantity < 0 {
                    report.negative_sizes += 1;
                    continue;
                }
                let Some(market) = markets.get_mut(&update.symbol) else {
                    report.updates_without_snapshot += 1;
                    continue;
                };
                if ts < market.last_ts {
                    report.timestamp_regressions += 1;
                }
                market.last_ts = market.last_ts.max(ts);
                market.updates += 1;
                let side: Side = update.side;
                let _ = market.book.apply(
                    side,
                    update.price_level,
                    LevelChange::Absolute(update.quantity),
                );
                if market.book.is_crossed() {
                    report.crossed_after_update += 1;
                }
            }
            _ => report.unknown += 1,
        }
    }
    report.markets = markets.len();

    let hard_failures = report.updates_without_snapshot
        + report.negative_sizes
        + report.prices_out_of_range
        + report.malformed_snapshots
        + report.timestamp_regressions
        + report.unknown;
    let span_secs = match (report.first_ts, report.last_ts) {
        (Some(first), Some(last)) => (last - first) as f64 / 1e6,
        _ => 0.0,
    };
    let ms = |q: f64| (!delay.is_empty()).then(|| delay.value_at_quantile(q) as f64 / 1_000.0);
    let summary = json!({
        "file": path,
        "messages": report.messages,
        "snapshots": report.snapshots,
        "price_level_updates": report.updates,
        "markets": report.markets,
        "span_secs": span_secs,
        "hard_failures": {
            "total": hard_failures,
            "updates_without_snapshot": report.updates_without_snapshot,
            "negative_sizes": report.negative_sizes,
            "prices_out_of_range": report.prices_out_of_range,
            "malformed_snapshots": report.malformed_snapshots,
            "per_market_timestamp_regressions": report.timestamp_regressions,
            "unknown_messages": report.unknown,
        },
        "snapshots_vs_replayed_book": {
            "matching": report.snapshots_matching_replay,
            "correcting": report.snapshots_correcting_replay,
        },
        "crossed_after_update": report.crossed_after_update,
        "receive_minus_exchange_ms": {"p50": ms(0.5), "p99": ms(0.99), "max": ms(1.0)},
        "exchange_stamp_after_receive": report.exchange_after_receive,
    });
    if json_out {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!(
            "{path}: {} messages ({} snapshots, {} updates) for {} markets over {:.0} s",
            report.messages, report.snapshots, report.updates, report.markets, span_secs
        );
        println!(
            "snapshots vs replayed book: {} matched, {} corrected it",
            report.snapshots_matching_replay, report.snapshots_correcting_replay
        );
        println!(
            "receive minus exchange stamp: p50 {:?} ms, p99 {:?} ms",
            ms(0.5),
            ms(0.99)
        );
        println!(
            "hard failures: {hard_failures} {}",
            summary["hard_failures"]
        );
    }
    if top > 0 {
        let mut busiest: Vec<(&String, u64)> =
            markets.iter().map(|(s, m)| (s, m.updates)).collect();
        busiest.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        println!("most updated markets:");
        for (symbol, updates) in busiest.into_iter().take(top) {
            println!("{updates}\t{symbol}");
        }
    }
    anyhow::ensure!(hard_failures == 0, "{hard_failures} hard failures");
    Ok(())
}

fn track_time(report: &mut Report, ts: i64) {
    report.first_ts = Some(report.first_ts.map_or(ts, |t| t.min(ts)));
    report.last_ts = Some(report.last_ts.map_or(ts, |t| t.max(ts)));
}
