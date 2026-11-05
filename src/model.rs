//! Exchange-neutral messages passed between the venue decoders, the socket
//! tasks and the shard logic.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::LevelChange;

/// Microseconds since the Unix epoch, the unit of every output timestamp.
pub type Micros = i64;

/// Wall-clock time now, in microseconds.
pub fn now_micros() -> Micros {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as Micros)
        .unwrap_or(0)
}

pub fn to_datetime(micros: Micros) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(micros).unwrap_or(DateTime::UNIX_EPOCH)
}

/// One decoded websocket frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    /// `(subscription id, sequence number)` when the venue numbers its frames.
    pub seq: Option<(u64, u64)>,
    /// When the venue says it sent the frame.
    pub sent_at: Option<Micros>,
    pub msg: Decoded,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decoded {
    Book(BookMsg),
    /// A `subscribe` command succeeded; `sid` names the new subscription.
    Subscribed { req_id: u64, sid: u64 },
    /// Any other command succeeded.
    Ack { req_id: u64 },
    /// A command failed.
    Error { req_id: Option<u64>, code: i64, message: String },
    Lifecycle(Lifecycle),
    /// Heartbeats and frame types the collector does not use.
    Ignored,
}

/// Book data for one market, already in the output's units: YES-contract
/// prices and sizes in fixed point.
#[derive(Debug, Clone, PartialEq)]
pub struct BookMsg {
    pub market: String,
    /// The venue's per-market ordering key, identical on every connection
    /// (Kalshi `clock`). `None` when the venue has no such key.
    pub clock: Option<u64>,
    /// When the venue says the change happened (Kalshi `ts_ms`).
    pub exchange_ts: Option<Micros>,
    pub body: BookBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BookBody {
    Snapshot { bids: Vec<(i64, i64)>, asks: Vec<(i64, i64)> },
    Level { side: Side, price: i64, change: LevelChange },
}

/// Market listing changes pushed by the venue.
#[derive(Debug, Clone, PartialEq)]
pub enum Lifecycle {
    /// The market is (or will be at `open_at`, seconds since the epoch) open
    /// for trading.
    Opened { market: String, open_at: Option<i64> },
    /// The market has finished trading.
    Closed { market: String },
    Other,
}
