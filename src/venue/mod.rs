//! The exchange-specific layer: everything that changes per venue.
//!
//! The rest of the collector (sockets, redundancy, monitoring, books, output)
//! only sees this trait and the neutral types in [`crate::model`].

pub mod kalshi;
pub mod polymarket;

use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::model::Frame;

pub trait Venue: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// WebSocket URLs. A shard's redundant connections are spread across them
    /// (connection `i` uses `ws_urls()[i % len]`) so one bad route can't take
    /// out every copy.
    fn ws_urls(&self) -> &[String];

    /// The HTTP upgrade request for `url`, including any authentication.
    fn connect_request(&self, url: &str) -> anyhow::Result<Request>;

    /// Most markets to put in one subscribe/add command.
    fn command_batch(&self) -> usize;

    /// First book subscription on a fresh connection.
    fn subscribe_books(&self, req_id: u64, markets: &[String]) -> String;

    /// Add markets to the connection's book subscription. `sid` is the id the
    /// venue returned for it (venues without ids ignore it).
    fn add_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String;

    fn remove_markets(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> String;

    /// Ask for fresh snapshots of markets already subscribed, if the venue can.
    fn request_snapshots(&self, req_id: u64, sid: Option<u64>, markets: &[String]) -> Option<String>;

    /// Whether `request_snapshots` works. Without it there are no audit
    /// probes, and a socket that lost data reconnects instead.
    fn snapshots_in_place(&self) -> bool {
        true
    }

    /// Subscribe to the venue's market-lifecycle feed, if it has one.
    fn subscribe_lifecycle(&self, req_id: u64) -> Option<String>;

    /// Whether `subscribe_books` gets an explicit reply carrying a
    /// subscription id. Without one the connection treats the command as
    /// accepted once sent.
    fn acks_commands(&self) -> bool;

    /// An application-level keepalive frame and how often to send it, for
    /// venues that need one on top of websocket pings.
    fn keepalive(&self) -> Option<(Duration, &'static str)>;

    /// Decodes one text frame, appending zero or more frames to `out`.
    fn decode(&self, text: &str, out: &mut Vec<Frame>) -> anyhow::Result<()>;

    /// Lists every market that should be collected right now, sending pages
    /// as they arrive. Returns the total listed. An error means the listing is
    /// incomplete, so callers must not treat missing markets as closed.
    fn list_markets(
        &self,
        pages: mpsc::UnboundedSender<Vec<String>>,
    ) -> impl Future<Output = anyhow::Result<usize>> + Send;

    /// Confirms a market announced on the lifecycle feed is one we collect
    /// (open, and not a combination market).
    fn confirm_market(&self, market: String) -> impl Future<Output = anyhow::Result<bool>> + Send;
}
