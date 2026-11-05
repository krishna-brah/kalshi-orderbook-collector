//! Throwaway spike: how many markets will one Kalshi `orderbook_delta`
//! subscription hold, how fast do snapshots arrive, and what do real frames
//! look like? Not part of the collector.
//!
//! Usage: cargo run --release --example spike -- <ws-host> <max-markets> <batch> [fixture-out]

use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, pkcs8::DecodePrivateKey};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
const WS_PATH: &str = "/trade-api/ws/v2";

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

async fn open_tickers(max: usize) -> Result<Vec<String>> {
    let client = reqwest::Client::new();
    let mut out = Vec::new();
    let mut cursor = String::new();
    while out.len() < max {
        let mut url = format!("{REST}/markets?status=open&mve_filter=exclude&limit=1000");
        if !cursor.is_empty() {
            url.push_str(&format!("&cursor={cursor}"));
        }
        let body: Value = client.get(&url).send().await?.error_for_status()?.json().await?;
        let markets = body["markets"].as_array().context("markets")?;
        out.extend(markets.iter().filter_map(|m| m["ticker"].as_str().map(str::to_owned)));
        cursor = body["cursor"].as_str().unwrap_or_default().to_owned();
        if cursor.is_empty() || markets.is_empty() {
            break;
        }
    }
    out.truncate(max);
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let host = args.get(1).cloned().unwrap_or("external-api-ws.demo.kalshi.co".into());
    let max: usize = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(20_000);
    let batch: usize = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(1_000);
    let fixture = args.get(4).cloned();

    let key_id = std::env::var("KALSHI_KEY_ID")?;
    let pem = std::fs::read_to_string(std::env::var("KALSHI_KEY_FILE")?)?;
    let key = SigningKey::from_pkcs8_pem(&pem).context("Ed25519 PKCS#8 key")?;

    let t0 = Instant::now();
    let tickers = open_tickers(max).await?;
    eprintln!("fetched {} tickers in {:?}", tickers.len(), t0.elapsed());

    let ts = now_ms().to_string();
    let sig = key.sign(format!("{ts}GET{WS_PATH}").as_bytes());
    let mut req = format!("wss://{host}{WS_PATH}").into_client_request()?;
    let h = req.headers_mut();
    h.insert("KALSHI-ACCESS-KEY", key_id.parse()?);
    h.insert("KALSHI-ACCESS-TIMESTAMP", ts.parse()?);
    h.insert(
        "KALSHI-ACCESS-SIGNATURE",
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes()).parse()?,
    );
    let t1 = Instant::now();
    let (ws, resp) = tokio_tungstenite::connect_async(req).await.context("connect")?;
    eprintln!("connected to {host} in {:?} (HTTP {})", t1.elapsed(), resp.status());
    let (mut tx, mut rx) = ws.split();

    let mut fixture = fixture.map(std::fs::File::create).transpose()?;
    let mut kept_snapshots = 0usize;
    let mut kept_other = 0usize;

    let mut next = 0usize;
    let mut id = 1u64;
    let first: Vec<&String> = tickers[..batch.min(tickers.len())].iter().collect();
    next += first.len();
    tx.send(Message::text(
        json!({"id": id, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": first}})
            .to_string(),
    ))
    .await?;
    let mut sent_at = Instant::now();
    let mut sid: Option<u64> = None;
    let mut snapshots = 0usize;
    let mut deltas = 0usize;
    let mut last_seq: Option<u64> = None;
    let mut gaps = 0usize;
    let mut snap_bytes = 0usize;
    let mut delays: Vec<i64> = Vec::new();
    let mut awaiting = first.len();
    let deadline = Instant::now() + Duration::from_secs(600);

    loop {
        let msg = match tokio::time::timeout(Duration::from_secs(30), rx.next()).await {
            Ok(Some(m)) => m?,
            Ok(None) => bail!("socket closed"),
            Err(_) => {
                eprintln!("30s with no frames; stopping");
                break;
            }
        };
        let recv_ms = now_ms() as i64;
        let Message::Text(text) = msg else { continue };
        let v: Value = serde_json::from_str(&text)?;
        if let Some(sending) = v["sending_ts_ms"].as_i64() {
            delays.push(recv_ms - sending);
        }
        if let Some(seq) = v["seq"].as_u64() {
            if let Some(prev) = last_seq {
                if seq != prev + 1 {
                    gaps += 1;
                }
            }
            last_seq = Some(seq);
        }
        let kind = v["type"].as_str().unwrap_or("?");
        if let Some(f) = fixture.as_mut() {
            let keep = match kind {
                "orderbook_snapshot" if kept_snapshots < 300 => {
                    kept_snapshots += 1;
                    true
                }
                "orderbook_snapshot" => false,
                _ if kept_other < 3_000 => {
                    kept_other += 1;
                    true
                }
                _ => false,
            };
            if keep {
                writeln!(f, "{}", json!({"recv_ms": recv_ms, "frame": v}))?;
            }
        }
        match kind {
            "subscribed" => {
                sid = v["msg"]["sid"].as_u64();
                eprintln!("subscribed sid={sid:?} after {:?}", sent_at.elapsed());
            }
            "ok" => eprintln!("ok id={} after {:?}", v["id"], sent_at.elapsed()),
            "error" => {
                eprintln!("ERROR {} at {} markets requested: {text}", v["msg"]["code"], next);
                break;
            }
            "orderbook_snapshot" => {
                snapshots += 1;
                snap_bytes += text.len();
                awaiting = awaiting.saturating_sub(1);
            }
            "orderbook_delta" => deltas += 1,
            other => eprintln!("other frame type {other}: {}", &text[..text.len().min(300)]),
        }
        if awaiting == 0 {
            eprintln!(
                "{} markets live: {} snapshots ({} KB), {} deltas, last batch took {:?}",
                next,
                snapshots,
                snap_bytes / 1024,
                deltas,
                sent_at.elapsed()
            );
            if next >= tickers.len() || Instant::now() > deadline {
                break;
            }
            let Some(sid) = sid else { bail!("no sid") };
            let end = (next + batch).min(tickers.len());
            let add: Vec<&String> = tickers[next..end].iter().collect();
            awaiting = add.len();
            next = end;
            id += 1;
            tx.send(Message::text(
                json!({"id": id, "cmd": "update_subscription", "params": {"sid": sid, "market_tickers": add, "action": "add_markets"}})
                    .to_string(),
            ))
            .await?;
            sent_at = Instant::now();
        }
    }
    delays.sort_unstable();
    let pct = |p: f64| delays.get(((delays.len() as f64 - 1.0) * p) as usize).copied().unwrap_or(-1);
    eprintln!(
        "done: requested {next}, snapshots {snapshots}, deltas {deltas}, seq gaps {gaps}, delay ms p50 {} p99 {} max {}",
        pct(0.5),
        pct(0.99),
        delays.last().copied().unwrap_or(-1)
    );
    let _ = tx.send(Message::Close(None)).await;
    Ok(())
}
