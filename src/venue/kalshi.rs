//! Kalshi: request signing, command encoding, frame decoding and market listing.
//!
//! Kalshi's book lists YES bids and NO bids. A NO bid at `p` is an offer to
//! sell YES at `1 - p`, so we publish the YES contract's book: bids are YES
//! bids and asks are the NO bids at `1 - p`.
//!
//! We subscribe with `use_yes_price: true`, which makes Kalshi report NO bids
//! already at `1 - p` (checked live: identical books either way). Kalshi has
//! announced that this becomes the only behavior, so relying on it now means
//! the default changing can't silently mirror our asks.

use std::borrow::Cow;
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::Venue;
use crate::Side;
use crate::book::LevelChange;
use crate::fixed::{self, SCALE};
use crate::model::{BookBody, BookMsg, Decoded, Frame, Lifecycle};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Env {
    Prod,
    Demo,
}

impl Env {
    fn rest_base(self) -> &'static str {
        match self {
            Self::Prod => "https://external-api.kalshi.com/trade-api/v2",
            Self::Demo => "https://external-api.demo.kalshi.co/trade-api/v2",
        }
    }

    /// Kalshi's two websocket hosts. In production the first is a load
    /// balancer in AWS us-east-2 and the second sits behind CloudFront, so
    /// they take different network paths.
    fn ws_urls(self) -> [&'static str; 2] {
        match self {
            Self::Prod => [
                "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
                "wss://api.elections.kalshi.com/trade-api/ws/v2",
            ],
            Self::Demo => [
                "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
                "wss://demo-api.kalshi.co/trade-api/ws/v2",
            ],
        }
    }
}

/// Signs Kalshi's pre-sign text (`timestamp + METHOD + path`) with either key
/// type Kalshi issues.
pub enum Signer {
    Ed25519(ed25519_dalek::SigningKey),
    Rsa(rsa::pss::BlindedSigningKey<rsa::sha2::Sha256>),
}

impl Signer {
    pub fn from_pem(pem: &str) -> anyhow::Result<Self> {
        use ed25519_dalek::pkcs8::DecodePrivateKey as _;
        if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self::Ed25519(key));
        }
        let rsa_key = {
            use rsa::pkcs1::DecodeRsaPrivateKey as _;
            use rsa::pkcs8::DecodePrivateKey as _;
            rsa::RsaPrivateKey::from_pkcs1_pem(pem).or_else(|_| rsa::RsaPrivateKey::from_pkcs8_pem(pem))
        }
        .context("key file is neither an Ed25519 nor an RSA private key")?;
        Ok(Self::Rsa(rsa::pss::BlindedSigningKey::new(rsa_key)))
    }

    /// Base64 signature of `text`.
    pub fn sign(&self, text: &str) -> String {
        let bytes: Vec<u8> = match self {
            Self::Ed25519(key) => {
                use ed25519_dalek::Signer as _;
                key.sign(text.as_bytes()).to_bytes().to_vec()
            }
            Self::Rsa(key) => {
                use rsa::signature::{RandomizedSigner as _, SignatureEncoding as _};
                key.sign_with_rng(&mut rand_core::OsRng, text.as_bytes()).to_vec()
            }
        };
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }
}

pub struct Kalshi {
    key_id: String,
    signer: Signer,
    ws_urls: Vec<String>,
    rest_base: String,
    http: reqwest::Client,
}

impl Kalshi {
    pub fn new(env: Env, key_id: String, signer: Signer) -> anyhow::Result<Self> {
        Ok(Self {
            key_id,
            signer,
            ws_urls: env.ws_urls().iter().map(|u| (*u).to_owned()).collect(),
            rest_base: env.rest_base().to_owned(),
            http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?,
        })
    }

    /// Points the collector at other websocket URLs, e.g. a local fake
    /// exchange in tests.
    pub fn with_ws_urls(mut self, urls: Vec<String>) -> Self {
        self.ws_urls = urls;
        self
    }

    fn update(req_id: u64, sid: Option<u64>, markets: &[String], action: &str) -> String {
        json!({
            "id": req_id,
            "cmd": "update_subscription",
            "params": {"sid": sid, "market_tickers": markets, "action": action},
        })
        .to_string()
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct SnapshotMsg<'a> {
    #[serde(borrow)]
    market_ticker: Cow<'a, str>,
    clock: Option<u64>,
    #[serde(default, borrow)]
    yes_dollars_fp: Option<Vec<(Cow<'a, str>, Cow<'a, str>)>>,
    #[serde(default, borrow)]
    no_dollars_fp: Option<Vec<(Cow<'a, str>, Cow<'a, str>)>>,
}

#[derive(Deserialize)]
struct DeltaMsg<'a> {
    #[serde(borrow)]
    market_ticker: Cow<'a, str>,
    clock: Option<u64>,
    #[serde(borrow)]
    price_dollars: Cow<'a, str>,
    #[serde(borrow)]
    delta_fp: Cow<'a, str>,
    #[serde(borrow)]
    side: Cow<'a, str>,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    sid: u64,
}

#[derive(Deserialize)]
struct ErrorMsg {
    code: i64,
    #[serde(default)]
    msg: String,
}

#[derive(Deserialize)]
struct LifecycleMsg<'a> {
    #[serde(borrow)]
    market_ticker: Cow<'a, str>,
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    open_ts: Option<i64>,
}

/// A level's side and YES-terms price, checked to be strictly inside (0, 1).
/// With `use_yes_price`, a NO bid's price is already the YES ask's price.
fn yes_level(side: &str, price: &str) -> anyhow::Result<(Side, i64)> {
    let price = fixed::parse(price)?;
    if price <= 0 || price >= SCALE {
        bail!("price {price} outside (0, 1)");
    }
    match side {
        "yes" => Ok((Side::Buy, price)),
        "no" => Ok((Side::Sell, price)),
        other => bail!("unknown side {other:?}"),
    }
}

fn snapshot_levels(levels: Option<Vec<(Cow<'_, str>, Cow<'_, str>)>>, side: &str) -> anyhow::Result<Vec<(i64, i64)>> {
    levels
        .unwrap_or_default()
        .iter()
        .map(|(price, qty)| Ok((yes_level(side, price)?.1, fixed::parse(qty)?)))
        .collect()
}

fn msg<'a, T: Deserialize<'a>>(env: &Envelope<'a>) -> anyhow::Result<T> {
    let raw = env.msg.with_context(|| format!("{} frame without msg", env.kind))?;
    Ok(serde_json::from_str(raw.get())?)
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<MarketRow>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct MarketRow {
    ticker: String,
}

#[derive(Deserialize)]
struct MarketReply {
    market: MarketDetail,
}

#[derive(Deserialize)]
struct MarketDetail {
    status: String,
    mve_collection_ticker: Option<String>,
}

async fn get_json<T: for<'de> Deserialize<'de>>(
    http: &reqwest::Client,
    url: &str,
    query: &[(&str, &str)],
) -> anyhow::Result<T> {
    let mut wait = Duration::from_millis(500);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = async {
            let reply = http.get(url).query(query).send().await?.error_for_status()?;
            anyhow::Ok(reply.json::<T>().await?)
        }
        .await;
        match result {
            Ok(value) => return Ok(value),
            Err(error) if attempt < 5 => {
                tracing::warn!(%url, attempt, %error, "REST request failed; retrying");
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
            Err(error) => return Err(error.context(format!("GET {url}"))),
        }
    }
}

impl Venue for Kalshi {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    fn ws_urls(&self) -> &[String] {
        &self.ws_urls
    }

    fn connect_request(&self, url: &str) -> anyhow::Result<Request> {
        let mut request = url.into_client_request()?;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
        let signature = self.signer.sign(&format!("{timestamp}GET{}", request.uri().path()));
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", self.key_id.parse()?);
        headers.insert("KALSHI-ACCESS-TIMESTAMP", timestamp.parse()?);
        headers.insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
        Ok(request)
    }

    fn command_batch(&self) -> usize {
        1_000
    }

    fn subscribe_books(&self, req_id: u64, markets: &[String]) -> String {
        json!({
            "id": req_id,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": markets, "use_yes_price": true},
        })
        .to_string()
    }

    fn add_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String {
        Self::update(req_id, sid, markets, "add_markets")
    }

    fn remove_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String {
        Self::update(req_id, sid, markets, "delete_markets")
    }

    fn request_snapshots(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> Option<String> {
        Some(Self::update(req_id, sid, markets, "get_snapshot"))
    }

    fn subscribe_lifecycle(&self, req_id: u64) -> Option<String> {
        Some(json!({"id": req_id, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}).to_string())
    }

    fn acks_commands(&self) -> bool {
        true
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        // Kalshi pings every 10 s; tungstenite answers those automatically.
        None
    }

    fn decode(&self, text: &str, out: &mut Vec<Frame>) -> anyhow::Result<()> {
        let env: Envelope = serde_json::from_str(text)?;
        let decoded = match env.kind {
            "orderbook_snapshot" => {
                let m: SnapshotMsg = msg(&env)?;
                Decoded::Book(BookMsg {
                    market: m.market_ticker.into_owned(),
                    clock: m.clock,
                    exchange_ts: None,
                    body: BookBody::Snapshot {
                        bids: snapshot_levels(m.yes_dollars_fp, "yes")?,
                        asks: snapshot_levels(m.no_dollars_fp, "no")?,
                    },
                })
            }
            "orderbook_delta" => {
                let m: DeltaMsg = msg(&env)?;
                let (side, price) = yes_level(&m.side, &m.price_dollars)?;
                Decoded::Book(BookMsg {
                    market: m.market_ticker.into_owned(),
                    clock: m.clock,
                    exchange_ts: m.ts_ms.map(|ms| ms * 1_000),
                    body: BookBody::Level { side, price, change: LevelChange::Delta(fixed::parse(&m.delta_fp)?) },
                })
            }
            "subscribed" => {
                let m: SubscribedMsg = msg(&env)?;
                Decoded::Subscribed { req_id: env.id.unwrap_or(0), sid: m.sid }
            }
            "ok" | "unsubscribed" => Decoded::Ack { req_id: env.id.unwrap_or(0) },
            "error" => {
                let m: ErrorMsg = msg(&env)?;
                Decoded::Error { req_id: env.id, code: m.code, message: m.msg }
            }
            "market_lifecycle_v2" => {
                let m: LifecycleMsg = msg(&env)?;
                let market = m.market_ticker.into_owned();
                Decoded::Lifecycle(match &*m.event_type {
                    "created" | "activated" => Lifecycle::Opened { market, open_at: m.open_ts },
                    "determined" | "settled" => Lifecycle::Closed { market },
                    _ => Lifecycle::Other,
                })
            }
            _ => Decoded::Ignored,
        };
        out.push(Frame {
            seq: env.sid.zip(env.seq),
            sent_at: env.sending_ts_ms.map(|ms| ms * 1_000),
            msg: decoded,
        });
        Ok(())
    }

    fn list_markets(
        &self,
        pages: mpsc::UnboundedSender<Vec<String>>,
    ) -> impl Future<Output = anyhow::Result<usize>> + Send {
        let http = self.http.clone();
        let url = format!("{}/markets", self.rest_base);
        async move {
            let mut cursor = String::new();
            let mut cursors_seen = HashSet::new();
            let mut total = 0;
            loop {
                let mut query = vec![("status", "open"), ("mve_filter", "exclude"), ("limit", "1000")];
                if !cursor.is_empty() {
                    query.push(("cursor", cursor.as_str()));
                }
                let page: MarketsPage = get_json(&http, &url, &query).await?;
                total += page.markets.len();
                let tickers: Vec<String> = page.markets.into_iter().map(|m| m.ticker).collect();
                if !tickers.is_empty() && pages.send(tickers).is_err() {
                    bail!("market listing receiver closed");
                }
                match page.cursor {
                    Some(next) if !next.is_empty() => {
                        if !cursors_seen.insert(next.clone()) {
                            bail!("Kalshi repeated a page cursor; listing is not trustworthy");
                        }
                        cursor = next;
                    }
                    _ => return Ok(total),
                }
            }
        }
    }

    fn confirm_market(&self, market: String) -> impl Future<Output = anyhow::Result<bool>> + Send {
        let http = self.http.clone();
        let url = format!("{}/markets/{market}", self.rest_base);
        async move {
            let reply: MarketReply = get_json(&http, &url, &[]).await?;
            Ok(matches!(reply.market.status.as_str(), "active" | "open") && reply.market.mve_collection_ticker.is_none())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn venue() -> Kalshi {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        Kalshi::new(Env::Demo, "key-id".into(), Signer::Ed25519(key)).unwrap()
    }

    fn decode_one(text: &str) -> Frame {
        let mut out = Vec::new();
        venue().decode(text, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }

    #[test]
    fn subscribes_with_yes_leg_pricing() {
        let text = venue().subscribe_books(1, &["A".into()]);
        let cmd: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(cmd["params"]["use_yes_price"], true);
        assert_eq!(cmd["params"]["channels"][0], "orderbook_delta");
    }

    #[test]
    fn snapshot_maps_no_bids_to_yes_asks() {
        // With use_yes_price, a NO bid at 0.59 arrives as 0.41: the YES ask.
        let frame = decode_one(
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"sending_ts_ms":1793899605589,
                "msg":{"market_ticker":"KXTEST-1","market_id":"x","clock":2604399952,
                       "yes_dollars_fp":[["0.3900","100.00"],["0.0100","2.50"]],
                       "no_dollars_fp":[["0.4100","100.00"]]}}"#,
        );
        assert_eq!(frame.seq, Some((1, 1)));
        assert_eq!(frame.sent_at, Some(1_793_899_605_589_000));
        let Decoded::Book(book) = frame.msg else { panic!("{frame:?}") };
        assert_eq!(book.market, "KXTEST-1");
        assert_eq!(book.clock, Some(2_604_399_952));
        assert_eq!(
            book.body,
            BookBody::Snapshot {
                bids: vec![(390_000, 100_000_000), (10_000, 2_500_000)],
                asks: vec![(410_000, 100_000_000)],
            }
        );
    }

    #[test]
    fn an_empty_side_may_be_missing() {
        let frame = decode_one(
            r#"{"type":"orderbook_snapshot","sid":3,"seq":9,"msg":{"market_ticker":"KXTEST-2","clock":0}}"#,
        );
        let Decoded::Book(book) = frame.msg else { panic!() };
        assert_eq!(book.body, BookBody::Snapshot { bids: vec![], asks: vec![] });
    }

    #[test]
    fn deltas_keep_their_sign_and_side() {
        let frame = decode_one(
            r#"{"type":"orderbook_delta","sid":1,"seq":2002,"sending_ts_ms":1793899605984,
                "msg":{"market_ticker":"KXPLAT","market_id":"x","clock":55106393253,"price_dollars":"0.9600",
                       "delta_fp":"-54.00","side":"no","ts":"2026-11-05T17:26:45.983Z","ts_ms":1793899605983}}"#,
        );
        let Decoded::Book(book) = frame.msg else { panic!() };
        assert_eq!(book.exchange_ts, Some(1_793_899_605_983_000));
        assert_eq!(
            book.body,
            BookBody::Level { side: Side::Sell, price: 960_000, change: LevelChange::Delta(-54_000_000) }
        );
    }

    #[test]
    fn rejects_prices_outside_the_unit_interval() {
        for price in ["0.0000", "1.0000", "1.2500"] {
            let text = format!(
                r#"{{"type":"orderbook_delta","sid":1,"seq":2,"msg":{{"market_ticker":"K","clock":1,
                    "price_dollars":"{price}","delta_fp":"1.00","side":"yes"}}}}"#
            );
            assert!(venue().decode(&text, &mut Vec::new()).is_err(), "{price}");
        }
    }

    #[test]
    fn control_frames() {
        let subscribed = decode_one(r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#);
        assert_eq!(subscribed.msg, Decoded::Subscribed { req_id: 1, sid: 7 });
        let ok = decode_one(r#"{"id":4,"type":"ok","sid":7,"seq":12,"msg":{"market_tickers":["A"]}}"#);
        assert_eq!(ok.msg, Decoded::Ack { req_id: 4 });
        let error = decode_one(r#"{"id":5,"type":"error","msg":{"code":26,"msg":"limit"}}"#);
        assert_eq!(error.msg, Decoded::Error { req_id: Some(5), code: 26, message: "limit".into() });
        let lifecycle = decode_one(
            r#"{"type":"market_lifecycle_v2","sid":13,"seq":3,"msg":{"market_ticker":"INXD-B4487",
                "event_type":"created","open_ts":1694635200,"close_ts":1694721600}}"#,
        );
        assert_eq!(lifecycle.seq, Some((13, 3)));
        assert_eq!(
            lifecycle.msg,
            Decoded::Lifecycle(Lifecycle::Opened { market: "INXD-B4487".into(), open_at: Some(1_694_635_200) })
        );
    }

    #[test]
    fn decodes_every_recorded_demo_frame() {
        let fixture = include_str!("../../tests/fixtures/kalshi-demo-frames.jsonl");
        let mut out = Vec::new();
        let (mut snapshots, mut deltas) = (0, 0);
        for line in fixture.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            out.clear();
            venue().decode(&row["frame"].to_string(), &mut out).unwrap();
            for frame in &out {
                if let Decoded::Book(book) = &frame.msg {
                    match book.body {
                        BookBody::Snapshot { .. } => snapshots += 1,
                        BookBody::Level { .. } => deltas += 1,
                    }
                    assert!(frame.seq.is_some() && book.clock.is_some());
                }
            }
        }
        assert!(snapshots > 0 && deltas > 0, "{snapshots} snapshots, {deltas} deltas");
    }

    #[test]
    fn ed25519_signature_verifies_against_the_public_key() {
        use ed25519_dalek::Verifier as _;
        let key = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let public = key.verifying_key();
        let text = "1700000000000GET/trade-api/ws/v2";
        let encoded = Signer::Ed25519(key).sign(text);
        let raw = base64::engine::general_purpose::STANDARD.decode(encoded).unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&raw).unwrap();
        assert!(public.verify(text.as_bytes(), &signature).is_ok());
    }

    #[test]
    fn rsa_pss_signature_verifies_against_the_public_key() {
        use rsa::signature::Verifier as _;
        let private = rsa::RsaPrivateKey::new(&mut rand_core::OsRng, 2048).unwrap();
        let public = rsa::pss::VerifyingKey::<rsa::sha2::Sha256>::new(private.to_public_key());
        let text = "1700000000000GET/trade-api/ws/v2";
        let encoded = Signer::Rsa(rsa::pss::BlindedSigningKey::new(private)).sign(text);
        let raw = base64::engine::general_purpose::STANDARD.decode(encoded).unwrap();
        let signature = rsa::pss::Signature::try_from(raw.as_slice()).unwrap();
        assert!(public.verify(text.as_bytes(), &signature).is_ok());
    }
}
