//! Throwaway check: does `use_yes_price: true` report each NO bid at
//! `1 - p` with the same size? Subscribes the same demo markets twice, once
//! with the flag and once without, and compares the first snapshots.
//!
//! Usage: cargo run --release --example yes_price_check -- <markets>

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use engineering_challenge::fixed;
use engineering_challenge::venue::kalshi::Signer;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
const WS: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";

type Levels = BTreeMap<i64, i64>;

async fn snapshots(
    signer: &Signer,
    key_id: &str,
    markets: &[String],
    yes_price: bool,
) -> Result<BTreeMap<String, Levels>> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let mut req = WS.into_client_request()?;
    let sig = signer.sign(&format!("{ts}GET/trade-api/ws/v2"));
    req.headers_mut()
        .insert("KALSHI-ACCESS-KEY", key_id.parse()?);
    req.headers_mut()
        .insert("KALSHI-ACCESS-TIMESTAMP", ts.parse()?);
    req.headers_mut()
        .insert("KALSHI-ACCESS-SIGNATURE", sig.parse()?);
    let (ws, _) = tokio_tungstenite::connect_async(req).await?;
    let (mut tx, mut rx) = ws.split();
    let mut params = json!({"channels": ["orderbook_delta"], "market_tickers": markets});
    if yes_price {
        params["use_yes_price"] = json!(true);
    }
    tx.send(Message::text(
        json!({"id": 1, "cmd": "subscribe", "params": params}).to_string(),
    ))
    .await?;
    let mut books = BTreeMap::new();
    while books.len() < markets.len() {
        let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(20), rx.next()).await else {
            break;
        };
        let Message::Text(text) = msg? else { continue };
        let v: Value = serde_json::from_str(&text)?;
        match v["type"].as_str() {
            Some("orderbook_snapshot") => {
                let mut no = Levels::new();
                for level in v["msg"]["no_dollars_fp"].as_array().into_iter().flatten() {
                    no.insert(
                        fixed::parse(level[0].as_str().unwrap())?,
                        fixed::parse(level[1].as_str().unwrap())?,
                    );
                }
                books.insert(v["msg"]["market_ticker"].as_str().unwrap().to_owned(), no);
            }
            Some("error") => anyhow::bail!("error: {text}"),
            _ => {}
        }
    }
    let _ = tx.send(Message::Close(None)).await;
    Ok(books)
}

#[tokio::main]
async fn main() -> Result<()> {
    let key_id = std::env::var("KALSHI_KEY_ID")?;
    let signer = Signer::from_pem(&std::fs::read_to_string(std::env::var("KALSHI_KEY_FILE")?)?)?;
    let markets: Vec<String> = match std::env::args().nth(1) {
        // A file of tickers (markets known to have NO bids).
        Some(path) if std::path::Path::new(&path).exists() => std::fs::read_to_string(path)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect(),
        count => {
            let count: usize = count.map(|s| s.parse()).transpose()?.unwrap_or(300);
            let page: Value = reqwest::Client::new()
                .get(format!("{REST}/markets"))
                .query(&[
                    ("status", "open"),
                    ("mve_filter", "exclude"),
                    ("limit", "1000"),
                ])
                .send()
                .await?
                .json()
                .await?;
            page["markets"]
                .as_array()
                .context("markets")?
                .iter()
                .filter_map(|m| m["ticker"].as_str().map(str::to_owned))
                .take(count)
                .collect()
        }
    };
    let legacy = snapshots(&signer, &key_id, &markets, false).await?;
    let yes = snapshots(&signer, &key_id, &markets, true).await?;
    let (mut same, mut mirrored, mut differ, mut no_side) = (0, 0, 0, 0);
    for (market, legacy_no) in &legacy {
        let Some(yes_no) = yes.get(market) else {
            continue;
        };
        if legacy_no.is_empty() && yes_no.is_empty() {
            continue;
        }
        no_side += 1;
        let flipped: Levels = legacy_no
            .iter()
            .map(|(&p, &q)| (fixed::SCALE - p, q))
            .collect();
        if yes_no == &flipped {
            mirrored += 1;
        } else if yes_no == legacy_no {
            same += 1;
        } else {
            differ += 1;
        }
    }
    println!(
        "{} markets compared, {no_side} with NO bids: {mirrored} reported at 1 - p with the flag, {same} unchanged, {differ} different (moved between snapshots)",
        legacy.len().min(yes.len())
    );
    Ok(())
}
