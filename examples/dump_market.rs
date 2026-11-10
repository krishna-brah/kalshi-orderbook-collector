//! Prints every message for one market in an output file, in file order:
//! useful next to `check --corrections` to see why a snapshot changed a book.
//!
//! Usage: cargo run --release --example dump_market -- <file> <market>

use anyhow::Context;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (path, market) = match args.as_slice() {
        [path, market] => (path, market),
        _ => anyhow::bail!("usage: dump_market <file> <market>"),
    };
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)
        .with_context(|| format!("opening {path}"))?;
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let s = SnapshotDomain::try_from_decoder(decoded)?;
                if &s.symbol == market {
                    let bids: Vec<_> = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                    let asks: Vec<_> = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
                    println!(
                        "snapshot {} (exchange {}) bids {bids:?} asks {asks:?}",
                        s.received_at, s.exchange_timestamp
                    );
                }
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                if &u.symbol == market {
                    println!(
                        "update   {} (exchange {}) {:?} {} -> {}",
                        u.backtest_timestamp,
                        u.exchange_timestamp,
                        u.side,
                        u.price_level,
                        u.quantity
                    );
                }
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }
    Ok(())
}
