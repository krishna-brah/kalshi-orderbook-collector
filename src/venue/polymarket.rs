//! Polymarket's CLOB market channel: the second venue, to show the core is
//! exchange-agnostic (Extension 3).
//!
//! Differences the core has to absorb, all handled here:
//! - Every outcome token has its own book, so the symbol is the token id.
//! - `price_change` carries the level's new size, not a delta.
//! - No sequence numbers and no replies to commands. The event `timestamp`
//!   (ms) is the same on every connection, so it serves as the clock for
//!   cross-copy comparison. There is no in-place re-snapshot, so lost data
//!   means reconnecting.
//! - The server wants a text `PING` every 10 s.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::Venue;
use crate::Side;
use crate::book::LevelChange;
use crate::fixed::{self, SCALE};
use crate::model::{BookBody, BookMsg, Decoded, Frame};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const MARKETS_URL: &str = "https://gamma-api.polymarket.com/markets/keyset";

pub struct Polymarket {
    ws_urls: Vec<String>,
    http: reqwest::Client,
}

impl Polymarket {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            ws_urls: vec![WS_URL.to_owned()],
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
        })
    }

    pub fn with_ws_urls(mut self, urls: Vec<String>) -> Self {
        self.ws_urls = urls;
        self
    }
}

/// Polymarket sends numbers as strings, but older messages used numbers.
fn decimal(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::String(s) => Ok(fixed::parse(s)?),
        Value::Number(n) => Ok(fixed::parse(&n.to_string())?),
        other => bail!("expected a decimal, got {other}"),
    }
}

fn price(value: &Value) -> anyhow::Result<i64> {
    let price = decimal(value)?;
    if price <= 0 || price >= SCALE {
        bail!("price {price} outside (0, 1)");
    }
    Ok(price)
}

fn millis(value: &Value) -> Option<i64> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn levels(value: &Value) -> anyhow::Result<Vec<(i64, i64)>> {
    let Some(levels) = value.as_array() else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|l| Ok((price(&l["price"])?, decimal(&l["size"])?)))
        .collect()
}

fn side(value: &Value) -> anyhow::Result<Side> {
    match value.as_str().map(str::to_ascii_uppercase).as_deref() {
        Some("BUY") => Ok(Side::Buy),
        Some("SELL") => Ok(Side::Sell),
        _ => bail!("unknown side {value}"),
    }
}

fn decode_event(event: &Value, out: &mut Vec<Frame>) -> anyhow::Result<()> {
    let ts = millis(&event["timestamp"]);
    let book = |market: &Value, body: BookBody| -> anyhow::Result<Frame> {
        Ok(Frame {
            seq: None,
            sent_at: None,
            msg: Decoded::Book(BookMsg {
                market: market.as_str().context("asset_id")?.to_owned(),
                clock: ts.map(|t| t as u64),
                exchange_ts: ts.map(|t| t * 1_000),
                body,
            }),
        })
    };
    match event["event_type"].as_str() {
        Some("book") => {
            let body = BookBody::Snapshot {
                bids: levels(&event["bids"])?,
                asks: levels(&event["asks"])?,
            };
            out.push(book(&event["asset_id"], body)?);
        }
        Some("price_change") => {
            // Current format: one entry per changed level, each with its token.
            if let Some(changes) = event["price_changes"].as_array() {
                for change in changes {
                    let body = BookBody::Level {
                        side: side(&change["side"])?,
                        price: price(&change["price"])?,
                        change: LevelChange::Absolute(decimal(&change["size"])?),
                    };
                    out.push(book(&change["asset_id"], body)?);
                }
            } else if let Some(changes) = event["changes"].as_array() {
                // Older format: one token per message.
                for change in changes {
                    let body = BookBody::Level {
                        side: side(&change["side"])?,
                        price: price(&change["price"])?,
                        change: LevelChange::Absolute(decimal(&change["size"])?),
                    };
                    out.push(book(&event["asset_id"], body)?);
                }
            }
        }
        _ => out.push(Frame {
            seq: None,
            sent_at: None,
            msg: Decoded::Ignored,
        }),
    }
    Ok(())
}

#[derive(Deserialize)]
struct KeysetPage {
    markets: Vec<GammaMarket>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    clob_token_ids: Option<String>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
}

impl Venue for Polymarket {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_urls(&self) -> &[String] {
        &self.ws_urls
    }

    fn connect_request(&self, url: &str) -> anyhow::Result<Request> {
        Ok(url.into_client_request()?)
    }

    fn command_batch(&self) -> usize {
        500
    }

    fn subscribe_books(&self, _req_id: u64, markets: &[String]) -> String {
        json!({"type": "market", "assets_ids": markets}).to_string()
    }

    fn add_markets(&self, _req_id: u64, _sid: Option<u64>, markets: &[String]) -> String {
        json!({"assets_ids": markets, "operation": "subscribe"}).to_string()
    }

    fn remove_markets(&self, _req_id: u64, _sid: Option<u64>, markets: &[String]) -> String {
        json!({"assets_ids": markets, "operation": "unsubscribe"}).to_string()
    }

    fn request_snapshots(
        &self,
        _req_id: u64,
        _sid: Option<u64>,
        _markets: &[String],
    ) -> Option<String> {
        None
    }

    fn subscribe_lifecycle(&self, _req_id: u64) -> Option<String> {
        None
    }

    fn acks_commands(&self) -> bool {
        false
    }

    fn snapshots_in_place(&self) -> bool {
        false
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        Some((Duration::from_secs(10), "PING"))
    }

    fn decode(&self, text: &str, out: &mut Vec<Frame>) -> anyhow::Result<()> {
        if text.eq_ignore_ascii_case("pong") {
            out.push(Frame {
                seq: None,
                sent_at: None,
                msg: Decoded::Ignored,
            });
            return Ok(());
        }
        match serde_json::from_str::<Value>(text)? {
            Value::Array(events) => events.iter().try_for_each(|e| decode_event(e, out)),
            event => decode_event(&event, out),
        }
    }

    fn list_markets(
        &self,
        pages: mpsc::UnboundedSender<Vec<String>>,
    ) -> impl Future<Output = anyhow::Result<usize>> + Send {
        let http = self.http.clone();
        async move {
            let mut cursor: Option<String> = None;
            let mut seen = HashSet::new();
            let mut total = 0;
            loop {
                let mut query = vec![
                    ("active", "true".to_owned()),
                    ("closed", "false".to_owned()),
                    ("limit", "500".to_owned()),
                ];
                if let Some(cursor) = &cursor {
                    query.push(("after_cursor", cursor.clone()));
                }
                let page: KeysetPage = http
                    .get(MARKETS_URL)
                    .query(&query)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                let mut tokens = Vec::new();
                for market in &page.markets {
                    if market.enable_order_book != Some(true)
                        || market.accepting_orders == Some(false)
                    {
                        continue;
                    }
                    let Some(ids) = &market.clob_token_ids else {
                        continue;
                    };
                    let ids: Vec<String> = serde_json::from_str(ids).unwrap_or_default();
                    tokens.extend(ids.into_iter().filter(|id| seen.insert(id.clone())));
                }
                total += tokens.len();
                if !tokens.is_empty() && pages.send(tokens).is_err() {
                    bail!("market listing receiver closed");
                }
                match page.next_cursor {
                    Some(next) if !next.is_empty() && !page.markets.is_empty() => {
                        if cursor.as_deref() == Some(next.as_str()) {
                            bail!("Polymarket repeated a page cursor");
                        }
                        cursor = Some(next);
                    }
                    _ => return Ok(total),
                }
            }
        }
    }

    fn confirm_market(&self, _market: String) -> impl Future<Output = anyhow::Result<bool>> + Send {
        async { Ok(true) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(text: &str) -> Vec<Frame> {
        let mut out = Vec::new();
        Polymarket::new().unwrap().decode(text, &mut out).unwrap();
        out
    }

    #[test]
    fn book_events_become_snapshots_keyed_by_token() {
        let frames = decode(
            r#"[{"event_type":"book","asset_id":"123","market":"0xabc","timestamp":"1757908892351",
                 "bids":[{"price":"0.48","size":"30"},{"price":"0.47","size":"12.5"}],
                 "asks":[{"price":"0.52","size":"25"}],"hash":"h"}]"#,
        );
        let Decoded::Book(book) = &frames[0].msg else {
            panic!()
        };
        assert_eq!(book.market, "123");
        assert_eq!(book.clock, Some(1_757_908_892_351));
        assert_eq!(
            book.body,
            BookBody::Snapshot {
                bids: vec![(480_000, 30_000_000), (470_000, 12_500_000)],
                asks: vec![(520_000, 25_000_000)]
            }
        );
    }

    #[test]
    fn price_changes_carry_absolute_sizes_per_token() {
        let frames = decode(
            r#"{"event_type":"price_change","market":"0xabc","timestamp":"1757908892351","price_changes":[
                {"asset_id":"123","price":"0.5","size":"200","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"0.52"},
                {"asset_id":"456","price":"0.5","size":"0","side":"SELL","hash":"h","best_bid":"0.48","best_ask":"0.5"}]}"#,
        );
        let bodies: Vec<_> = frames
            .iter()
            .map(|f| match &f.msg {
                Decoded::Book(b) => (b.market.clone(), b.body.clone()),
                _ => panic!(),
            })
            .collect();
        assert_eq!(
            bodies,
            [
                (
                    "123".into(),
                    BookBody::Level {
                        side: Side::Buy,
                        price: 500_000,
                        change: LevelChange::Absolute(200_000_000)
                    }
                ),
                (
                    "456".into(),
                    BookBody::Level {
                        side: Side::Sell,
                        price: 500_000,
                        change: LevelChange::Absolute(0)
                    }
                ),
            ]
        );
    }

    #[test]
    fn pong_and_other_events_are_ignored() {
        assert_eq!(decode("PONG")[0].msg, Decoded::Ignored);
        assert_eq!(
            decode(r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.5"}"#)[0].msg,
            Decoded::Ignored
        );
    }
}
