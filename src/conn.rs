//! One websocket connection: connect, subscribe, read, detect loss, reconnect.
//!
//! A connection knows which markets it should carry and keeps carrying them
//! across reconnects. It decodes frames, checks sequence numbers and passes
//! book data up to its shard. Which copy of a book is published is the
//! shard's decision, not the connection's.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::model::{BookMsg, Decoded, Frame, Lifecycle, Micros, now_micros};
use crate::stats::Stats;
use crate::venue::Venue;

/// Kalshi pings every 10 s, so 30 s of nothing means the socket is dead even
/// if TCP hasn't noticed.
const SILENCE_LIMIT: Duration = Duration::from_secs(30);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Wait before retrying a command, multiplied by the attempt number.
const RETRY_PAUSE: Duration = Duration::from_secs(1);
/// Retryable errors in a row before the socket is reset.
const MAX_RETRIES: u32 = 5;
/// Spacing between commands the venue doesn't reply to.
const UNACKED_COMMAND_GAP: Duration = Duration::from_millis(25);
/// How many un-acknowledged commands to remember, to recognise their errors.
const UNACKED_KEPT: usize = 64;

// Kalshi error codes the connection acts on.
const ERROR_GENERAL: i64 = 1;
const ERROR_UNKNOWN_SID: i64 = 7;
const ERROR_CHANNEL: i64 = 10;
const ERROR_COMMAND_TIMEOUT: i64 = 18;
const ERROR_BUFFER_OVERFLOW: i64 = 25;
const ERROR_MARKET_LIMIT: i64 = 26;
const ERROR_RATE_LIMITED: i64 = 27;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Order books for the markets this connection is given.
    Books,
    /// The venue's market lifecycle feed (new and finished markets).
    Lifecycle,
}

#[derive(Debug)]
pub enum ConnCmd {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Ask the venue for fresh snapshots of these markets.
    Snapshots(Vec<String>),
    /// Drop and re-add these markets.
    Resubscribe(Vec<String>),
    /// Close and reopen the socket (planned recycle).
    Reconnect,
}

#[derive(Debug)]
pub struct ConnEvent {
    /// The connection's slot within its shard.
    pub slot: usize,
    pub kind: ConnEventKind,
}

#[derive(Debug)]
pub enum ConnEventKind {
    Up,
    Down {
        reason: String,
    },
    /// Frames were lost; every book this socket carries is suspect until it
    /// is snapshotted again. Fresh snapshots have already been requested.
    Gap,
    /// Book data, with when it came off the socket and when the venue says
    /// it sent it.
    Book {
        received: Micros,
        sent: Option<Micros>,
        msg: BookMsg,
    },
    Lifecycle(Lifecycle),
    /// The venue would not subscribe these markets.
    Refused {
        markets: Vec<String>,
        code: i64,
        message: String,
    },
}

pub struct ConnHandle {
    pub cmds: mpsc::UnboundedSender<ConnCmd>,
    pub task: JoinHandle<()>,
}

pub struct ConnSpec {
    /// Log label, e.g. `s3/1`.
    pub label: String,
    pub slot: usize,
    pub url: String,
    pub mode: Mode,
}

pub fn spawn<V: Venue>(
    venue: Arc<V>,
    spec: ConnSpec,
    markets: Vec<String>,
    events: mpsc::UnboundedSender<ConnEvent>,
    stats: Arc<Stats>,
    shutdown: watch::Receiver<bool>,
) -> ConnHandle {
    let (cmds, cmds_rx) = mpsc::unbounded_channel();
    let conn = Conn {
        venue,
        spec,
        desired: markets.into_iter().collect(),
        cmds: cmds_rx,
        events,
        stats,
        shutdown,
    };
    ConnHandle {
        cmds,
        task: tokio::spawn(conn.run()),
    }
}

#[derive(Debug, Clone)]
enum Command {
    Add(Vec<String>),
    Remove(Vec<String>),
    Snapshots(Vec<String>),
    Lifecycle,
}

struct InFlight {
    id: u64,
    command: Command,
    /// Whether this was the first `subscribe` (its reply carries the sid).
    subscribe: bool,
    sent: Instant,
}

enum End {
    Shutdown,
    Recycle,
}

/// Per-socket state, reset on every reconnect.
struct Session {
    queue: VecDeque<Command>,
    /// Whether the first book `subscribe` has gone out; later markets are
    /// added to that subscription.
    subscribe_sent: bool,
    sid: Option<u64>,
    in_flight: Option<InFlight>,
    seqs: HashMap<u64, u64>,
    next_id: u64,
    not_before: Instant,
    /// Retryable errors in a row; reset by any success.
    retries: u32,
    /// Ids of recent commands the venue doesn't acknowledge.
    unacked: VecDeque<u64>,
}

struct Conn<V> {
    venue: Arc<V>,
    spec: ConnSpec,
    desired: HashSet<String>,
    cmds: mpsc::UnboundedReceiver<ConnCmd>,
    events: mpsc::UnboundedSender<ConnEvent>,
    stats: Arc<Stats>,
    shutdown: watch::Receiver<bool>,
}

impl<V: Venue> Conn<V> {
    async fn run(mut self) {
        let mut backoff = Duration::from_millis(250);
        loop {
            if *self.shutdown.borrow() {
                return;
            }
            let opened = Instant::now();
            let mut connected = false;
            let result = self.session(&mut connected).await;
            let reason = match result {
                Ok(End::Shutdown) => return,
                Ok(End::Recycle) => {
                    self.stats.recycles.inc();
                    "planned recycle".to_owned()
                }
                Err(error) => {
                    self.stats.reconnects.inc();
                    format!("{error:#}")
                }
            };
            if connected {
                self.stats.sockets_up.add(-1);
                tracing::warn!(conn = %self.spec.label, url = %self.spec.url, %reason, "socket down");
                self.emit(ConnEventKind::Down { reason });
            } else {
                tracing::warn!(conn = %self.spec.label, url = %self.spec.url, %reason, "connect failed");
            }
            if opened.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_millis(250);
            }
            let wait = backoff + Duration::from_millis(fastrand::u64(0..250));
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.shutdown.changed() => return,
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    fn emit(&self, kind: ConnEventKind) {
        let _ = self.events.send(ConnEvent {
            slot: self.spec.slot,
            kind,
        });
    }

    fn batches(&self, markets: &[String]) -> Vec<Vec<String>> {
        markets
            .chunks(self.venue.command_batch().max(1))
            .map(<[String]>::to_vec)
            .collect()
    }

    /// Applies a command to the desired market set and queues what to send.
    fn accept(&mut self, cmd: ConnCmd, session: Option<&mut Session>) {
        let mut queue = VecDeque::new();
        match cmd {
            ConnCmd::Add(markets) => {
                let new: Vec<String> = markets
                    .into_iter()
                    .filter(|m| self.desired.insert(m.clone()))
                    .collect();
                queue.extend(self.batches(&new).into_iter().map(Command::Add));
            }
            ConnCmd::Remove(markets) => {
                let gone: Vec<String> = markets
                    .into_iter()
                    .filter(|m| self.desired.remove(m))
                    .collect();
                queue.extend(self.batches(&gone).into_iter().map(Command::Remove));
            }
            ConnCmd::Snapshots(markets) => {
                let live: Vec<String> = markets
                    .into_iter()
                    .filter(|m| self.desired.contains(m))
                    .collect();
                queue.extend(self.batches(&live).into_iter().map(Command::Snapshots));
            }
            ConnCmd::Resubscribe(markets) => {
                let live: Vec<String> = markets
                    .into_iter()
                    .filter(|m| self.desired.contains(m))
                    .collect();
                for batch in self.batches(&live) {
                    queue.push_back(Command::Remove(batch.clone()));
                    queue.push_back(Command::Add(batch));
                }
            }
            ConnCmd::Reconnect => {}
        }
        // While disconnected only the desired set matters; the next session
        // subscribes everything in it.
        if let Some(session) = session {
            session.queue.extend(queue);
        }
    }

    async fn session(&mut self, connected: &mut bool) -> anyhow::Result<End> {
        while let Ok(cmd) = self.cmds.try_recv() {
            self.accept(cmd, None);
        }
        let request = self.venue.connect_request(&self.spec.url)?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(64 << 20))
            .max_frame_size(Some(16 << 20));
        let (ws, _) = tokio::time::timeout(
            CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(request, Some(config), true),
        )
        .await
        .context("connect timed out")?
        .context("connect")?;
        *connected = true;
        self.stats.sockets_up.add(1);
        tracing::info!(conn = %self.spec.label, url = %self.spec.url, markets = self.desired.len(), "socket up");
        self.emit(ConnEventKind::Up);

        let (mut sink, mut stream) = ws.split();
        let mut session = Session {
            queue: VecDeque::new(),
            subscribe_sent: false,
            sid: None,
            in_flight: None,
            seqs: HashMap::new(),
            next_id: 1,
            not_before: Instant::now(),
            retries: 0,
            unacked: VecDeque::new(),
        };
        match self.spec.mode {
            Mode::Books => {
                let all: Vec<String> = self.desired.iter().cloned().collect();
                session
                    .queue
                    .extend(self.batches(&all).into_iter().map(Command::Add));
            }
            Mode::Lifecycle => session.queue.push_back(Command::Lifecycle),
        }

        let mut last_frame = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        let keepalive = self.venue.keepalive();
        let mut keepalive_timer =
            tokio::time::interval(keepalive.map_or(Duration::from_secs(3600), |k| k.0));
        let mut frames = Vec::new();

        loop {
            if session.in_flight.is_none()
                && Instant::now() >= session.not_before
                && let Some(command) = session.queue.pop_front()
            {
                self.send(&mut sink, &mut session, command).await?;
                continue;
            }
            tokio::select! {
                biased;
                _ = self.shutdown.changed() => {
                    let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(Message::Close(None))).await;
                    return Ok(End::Shutdown);
                }
                frame = stream.next() => {
                    let received = now_micros();
                    last_frame = Instant::now();
                    let message = match frame {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => return Err(error.into()),
                        None => bail!("server closed the connection"),
                    };
                    let text = match message {
                        Message::Text(text) => text,
                        Message::Close(frame) => {
                            self.stats.server_closes.inc();
                            bail!("server sent close: {frame:?}");
                        }
                        _ => continue, // pings are answered by tungstenite
                    };
                    let route = self.stats.route(self.spec.slot);
                    route.frames.inc();
                    route.bytes.add(text.len() as u64);
                    frames.clear();
                    if let Err(error) = self.venue.decode(&text, &mut frames) {
                        self.stats.decode_errors.inc();
                        tracing::warn!(conn = %self.spec.label, %error, frame = %truncate(&text), "undecodable frame");
                        // We can't tell which book it belonged to.
                        self.lost_data(&mut session)?;
                        continue;
                    }
                    for frame in frames.drain(..) {
                        self.handle(frame, received, &mut session)?;
                    }
                }
                cmd = self.cmds.recv() => match cmd {
                    Some(ConnCmd::Reconnect) => {
                        let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(Message::Close(None))).await;
                        return Ok(End::Recycle);
                    }
                    Some(cmd) => self.accept(cmd, Some(&mut session)),
                    None => return Ok(End::Shutdown),
                },
                _ = ticker.tick() => {
                    if last_frame.elapsed() > SILENCE_LIMIT {
                        bail!("no frames for {:?}", last_frame.elapsed());
                    }
                    if let Some(in_flight) = &session.in_flight
                        && in_flight.sent.elapsed() > COMMAND_TIMEOUT {
                            bail!("command {} unanswered for {:?}", in_flight.id, in_flight.sent.elapsed());
                        }
                }
                _ = keepalive_timer.tick(), if keepalive.is_some() => {
                    sink.send(Message::text(keepalive.map_or("", |k| k.1))).await?;
                }
                // Wake when a paced command may go out.
                _ = tokio::time::sleep_until(session.not_before.into()),
                    if session.in_flight.is_none() && !session.queue.is_empty() => {}
            }
        }
    }

    async fn send<S>(
        &mut self,
        sink: &mut S,
        session: &mut Session,
        command: Command,
    ) -> anyhow::Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let id = session.next_id;
        session.next_id += 1;
        let mut subscribe = false;
        let subscribed = session.subscribe_sent;
        let text = match &command {
            Command::Add(markets) if !subscribed => {
                subscribe = true;
                session.subscribe_sent = true;
                Some(self.venue.subscribe_books(id, markets))
            }
            Command::Add(markets) => Some(self.venue.add_markets(id, session.sid, markets)),
            Command::Remove(markets) if subscribed => {
                Some(self.venue.remove_markets(id, session.sid, markets))
            }
            Command::Snapshots(markets) if subscribed => {
                match self.venue.request_snapshots(id, session.sid, markets) {
                    Some(text) => Some(text),
                    None => bail!("the venue can't re-snapshot in place; reconnecting"),
                }
            }
            Command::Remove(_) | Command::Snapshots(_) => None,
            Command::Lifecycle => self.venue.subscribe_lifecycle(id),
        };
        let Some(text) = text else { return Ok(()) };
        sink.send(Message::text(text)).await?;
        // Kalshi confirms subscribes and added markets, but not snapshot
        // requests or removals, so only the former wait for a reply. The rest
        // are paced instead.
        let replied = matches!(command, Command::Add(_) | Command::Lifecycle);
        if self.venue.acks_commands() && replied {
            session.in_flight = Some(InFlight {
                id,
                command,
                subscribe,
                sent: Instant::now(),
            });
        } else {
            session.not_before = Instant::now() + UNACKED_COMMAND_GAP;
            session.unacked.push_back(id);
            if session.unacked.len() > UNACKED_KEPT {
                session.unacked.pop_front();
            }
        }
        Ok(())
    }

    fn handle(
        &mut self,
        frame: Frame,
        received: Micros,
        session: &mut Session,
    ) -> anyhow::Result<()> {
        if let Some((sid, seq)) = frame.seq
            && let Some(previous) = session.seqs.insert(sid, seq)
            && seq != previous + 1
        {
            tracing::warn!(conn = %self.spec.label, sid, previous, seq, "sequence gap");
            self.lost_data(session)?;
        }
        match frame.msg {
            Decoded::Book(msg) => {
                if let Some(sent) = frame.sent_at {
                    self.stats
                        .route(self.spec.slot)
                        .delay
                        .record(received - sent);
                }
                self.emit(ConnEventKind::Book {
                    received,
                    sent: frame.sent_at,
                    msg,
                });
            }
            Decoded::Subscribed { req_id, sid } => {
                if session.in_flight.as_ref().is_some_and(|f| f.id == req_id) {
                    let in_flight = session.in_flight.take().expect("checked");
                    if in_flight.subscribe {
                        session.sid = Some(sid);
                    }
                    session.retries = 0;
                }
            }
            Decoded::Ack { req_id } => {
                if session.in_flight.as_ref().is_some_and(|f| f.id == req_id) {
                    session.in_flight = None;
                    session.retries = 0;
                }
            }
            Decoded::Error {
                req_id,
                code,
                message,
            } => {
                // The subscription itself is gone; only a new connection
                // (which resubscribes everything) can fix that.
                if matches!(code, ERROR_UNKNOWN_SID | ERROR_BUFFER_OVERFLOW) {
                    bail!("subscription broken (code {code}: {message})");
                }
                if let Some(in_flight) = session.in_flight.take_if(|f| Some(f.id) == req_id) {
                    return self.command_failed(in_flight, code, message, session);
                }
                if req_id.is_some_and(|id| session.unacked.contains(&id)) {
                    // A refused snapshot request or removal. Copies left
                    // without a snapshot are re-requested by the shard.
                    tracing::debug!(conn = %self.spec.label, code, %message, "command refused");
                    return Ok(());
                }
                if code == ERROR_CHANNEL {
                    bail!("channel error on the subscription (code {code}: {message})");
                }
                tracing::warn!(conn = %self.spec.label, code, %message, "venue error");
            }
            Decoded::Lifecycle(event) => self.emit(ConnEventKind::Lifecycle(event)),
            Decoded::Ignored => {}
        }
        Ok(())
    }

    fn command_failed(
        &mut self,
        failed: InFlight,
        code: i64,
        message: String,
        session: &mut Session,
    ) -> anyhow::Result<()> {
        if failed.subscribe {
            // Nothing is subscribed yet, so the retry must subscribe again.
            session.subscribe_sent = false;
        }
        if matches!(
            code,
            ERROR_RATE_LIMITED | ERROR_COMMAND_TIMEOUT | ERROR_GENERAL
        ) {
            session.retries += 1;
            if session.retries > MAX_RETRIES {
                bail!(
                    "{} retryable errors in a row (last: code {code}: {message})",
                    session.retries
                );
            }
            if code == ERROR_RATE_LIMITED {
                self.stats.rate_limited.inc();
            }
            session.queue.push_front(failed.command);
            session.not_before = Instant::now() + RETRY_PAUSE * session.retries;
            return Ok(());
        }
        match failed.command {
            // One bad ticker fails a whole batch, so split until the bad ones
            // are isolated; a full subscription can't be fixed by splitting.
            Command::Add(markets) if markets.len() > 1 && code != ERROR_MARKET_LIMIT => {
                let (left, right) = markets.split_at(markets.len() / 2);
                session.queue.push_front(Command::Add(right.to_vec()));
                session.queue.push_front(Command::Add(left.to_vec()));
            }
            Command::Add(markets) => {
                // The markets stay wanted: the next connection tries them
                // again. Forgetting them would drop them for good.
                tracing::warn!(conn = %self.spec.label, code, %message, count = markets.len(), first = %markets[0], "markets refused");
                self.stats.refused.add(markets.len() as u64);
                self.emit(ConnEventKind::Refused {
                    markets,
                    code,
                    message,
                });
            }
            Command::Lifecycle => bail!("lifecycle subscription refused (code {code}: {message})"),
            Command::Remove(_) | Command::Snapshots(_) => {
                tracing::debug!(conn = %self.spec.label, code, %message, "command refused");
            }
        }
        Ok(())
    }

    /// Something was lost on this socket: tell the shard, then re-snapshot
    /// every market (or reconnect if the venue can't re-snapshot in place).
    fn lost_data(&mut self, session: &mut Session) -> anyhow::Result<()> {
        self.stats.gaps.inc();
        self.emit(ConnEventKind::Gap);
        if self.spec.mode == Mode::Lifecycle {
            return Ok(());
        }
        if !session.subscribe_sent {
            return Ok(());
        }
        if !self.venue.snapshots_in_place() {
            bail!("data lost and the venue can't re-snapshot in place; reconnecting");
        }
        let all: Vec<String> = self.desired.iter().cloned().collect();
        session
            .queue
            .retain(|c| !matches!(c, Command::Snapshots(_)));
        session
            .queue
            .extend(self.batches(&all).into_iter().map(Command::Snapshots));
        Ok(())
    }
}

fn truncate(text: &str) -> &str {
    let end = text.char_indices().nth(300).map_or(text.len(), |(i, _)| i);
    &text[..end]
}
