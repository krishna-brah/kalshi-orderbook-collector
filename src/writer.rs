//! The single writer thread. It owns the output file; every other task sends
//! it messages over a channel, so the file sees one ordered stream.

use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Context;
use crossbeam::channel::{Receiver, Sender, bounded, select, unbounded};

use crate::stats::Stats;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// The template's reader only reads 64 MiB chunks, so the writer must use them.
pub const CHUNK_SIZE: usize = 64 << 20;

#[derive(Debug, Clone, PartialEq)]
pub enum OutMsg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

pub struct Writer {
    pub tx: Sender<OutMsg>,
    stop: Sender<()>,
    thread: JoinHandle<anyhow::Result<u64>>,
}

impl Writer {
    /// Opens (and truncates) `path`; `-` means stdout.
    pub fn start(path: &str, stats: Arc<Stats>) -> anyhow::Result<Self> {
        let path = if path == "-" { "/dev/stdout" } else { path };
        let file = AlignedSbeWriter::create(Path::new(path), CHUNK_SIZE)
            .with_context(|| format!("opening output {path}"))?;
        let (tx, rx) = unbounded();
        let (stop, stop_rx) = bounded(1);
        let thread = std::thread::Builder::new()
            .name("writer".into())
            .spawn(move || write_loop(file, &rx, &stop_rx, &stats))?;
        Ok(Self { tx, stop, thread })
    }

    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }

    /// Writes whatever is already queued, flushes the last chunk and closes
    /// the file. Returns how many messages were written in total.
    pub fn finish(self) -> anyhow::Result<u64> {
        let Self { tx, stop, thread } = self;
        drop(tx);
        let _ = stop.send(());
        thread.join().map_err(|_| anyhow::anyhow!("writer thread panicked"))?
    }
}

fn write_loop(
    mut file: AlignedSbeWriter,
    rx: &Receiver<OutMsg>,
    stop: &Receiver<()>,
    stats: &Stats,
) -> anyhow::Result<u64> {
    let mut written = 0u64;
    let mut write = |file: &mut AlignedSbeWriter, msg: OutMsg| -> anyhow::Result<()> {
        match &msg {
            OutMsg::Snapshot(snapshot) => file.write_msg(snapshot)?,
            OutMsg::Update(update) => file.write_msg(update)?,
        }
        written += 1;
        stats.written.inc();
        Ok(())
    };
    loop {
        select! {
            recv(rx) -> msg => match msg {
                Ok(msg) => write(&mut file, msg)?,
                Err(_) => break,
            },
            recv(stop) -> _ => {
                while let Ok(msg) = rx.try_recv() {
                    write(&mut file, msg)?;
                }
                break;
            }
        }
    }
    file.flush()?;
    drop(file);
    Ok(stats.written.get())
}
