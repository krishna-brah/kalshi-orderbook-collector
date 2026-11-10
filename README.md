# Kalshi order-book collector

Collects every live Kalshi order book (every open market that is not a
multivariate combination) and writes price-level updates, plus a full snapshot
of every book each minute, through the template's `AlignedSbeWriter`.

The bar is "complete and time-accurate", so most of the design is about
knowing the books are right:

- **Two copies of every book**, on two sockets, over Kalshi's two websocket
  hosts. In production they take different network paths: one is a load
  balancer in AWS us-east-2, the other sits behind CloudFront. Kalshi stamps
  every change with a per-market `clock` that is identical on every
  connection, so the two copies can be compared change by change.
- **One copy is published.** If it drops, loses a frame, or falls behind the
  other copy, the other copy takes over and a full snapshot is written first
  (Extension 1).
- **Checked while running.** Every 5 seconds a few books per socket are
  re-fetched from Kalshi and compared level by level with the book built from
  deltas. A copy that misses changes the other copy received is resubscribed;
  a socket where most copies fall behind is reconnected; every socket is also
  reconnected on a schedule, one at a time (Extension 2).
- **Checked afterwards.** `check` replays an output file and verifies that
  every update lands on a known book, sizes and prices are valid, snapshots are
  well formed, and per-market timestamps never go backwards.

The core doesn't know which exchange it is talking to: `--venue polymarket`
runs the same code on Polymarket (Extension 3). [LATENCY.md](LATENCY.md) has
the latency measurements and experiments (Extension 4).

## The brief, point by point

| The brief asks for | Where |
| --- | --- |
| Every live market except MVEs, with a book for each | `venue/kalshi.rs` lists open markets with `mve_filter=exclude`; `book.rs` keeps each book |
| New markets subscribed, finished ones dropped | Kalshi's lifecycle feed, plus a full listing every 10 minutes (`collector.rs`) |
| Deltas to stdout plus a full snapshot every minute, through the template's helpers | stdout by default; snapshots on every wall-clock minute; `writer.rs` → `AlignedSbeWriter` (`src/util/` is unchanged) |
| SIGINT closes the websockets and the file | `main.rs`, `writer.rs` |
| Markets spread over many websockets | 3,000 markets per socket |
| Extension 1: redundant websockets, a snapshot on every source switch | `shard.rs` (`ShardCore`) |
| Extension 2: resubscribe a market that goes too quiet; reconnect sockets periodically | `shard.rs`: quiet checks, audit probes, repair sweep; every socket reconnects every 30 minutes |
| Extension 3: only the exchange's encoding and decoding change | `venue/`; `--venue polymarket` |
| Extension 4: latency experiments, written up in markdown | [LATENCY.md](LATENCY.md) |

## Run it

Needs Rust 1.88 or newer (the template's SBE crate requires it). Kalshi
requires an API key even for public data.

```bash
export KALSHI_KEY_ID=<key id>
export KALSHI_KEY_FILE=<private key file>    # Ed25519 or RSA, PEM
cargo build --release

./target/release/collector > books.sbe                  # production, written to stdout
./target/release/collector --env demo --out books.sbe   # demo, written to a file
./target/release/check books.sbe
```

The output goes to stdout unless `--out` names a file. Logs and a status line
every 10 s go to stderr, and it refuses to write binary to a terminal.
Ctrl-C (SIGINT) or SIGTERM closes every socket, writes whatever is queued,
flushes the last chunk and closes the file, then prints a JSON run summary
(`--summary run.json` keeps a copy). Use a plain redirect rather than a pipe:
Ctrl-C also stops the next program in a pipe, which can lose the last chunk.

`./target/release/collector --venue polymarket --out pm.sbe` needs no key.
`collector --help` lists the settings: copies per book, markets per socket,
lag threshold, probe rate, quiet threshold and recycle interval.

## What the file contains

- **One book per market, in YES terms.** Kalshi lists YES bids and NO bids. A
  NO bid at `p` is an offer to sell YES at `1 − p`, so it is written as an ask
  at `1 − p`. The collector subscribes with `use_yes_price`, so Kalshi sends
  NO bids already at `1 − p` (checked live against the default mode:
  identical books). Kalshi has announced that this becomes the only behavior.
  Decimal strings are converted to fixed point digit by digit, never through
  a float.
- **`PriceLevelUpdate.quantity` is the level's new total size**, 0 when the
  level is gone, even though Kalshi sends changes.
- **`backtest_timestamp`** is when the change first came off any of our
  sockets. Both copies carry it under the same `clock`, so the published copy
  decides the content and the faster copy the time (see
  [LATENCY.md](LATENCY.md)). **`exchange_timestamp`** is Kalshi's `ts_ms` for
  the change.
- **A `Snapshot`** is written when a book is first received, when Kalshi
  re-sends the published copy, whenever the published copy changes
  (Extension 1), and for every book on each wall-clock minute. Its
  `received_at` is when it was taken; its `exchange_timestamp` is the time of
  the newest change it includes.
- Within a market, messages are in Kalshi's order and timestamps never go
  backwards. A frame received just before a snapshot the collector generated,
  but processed just after it, is stamped no earlier than that snapshot
  (counted as `timestamps_held_back`). Across markets, messages are
  interleaved in arrival order.

## How it works

```text
 REST listing (start-up, then every 10 min) ──┐
 lifecycle socket (new and settled markets) ──┤
                                              ▼
                                 collector: market → shard (3,000 per shard)
                                              │
            ┌─────────────────────────────────┼──────────────────┐
            ▼                                 ▼                  ▼
  shard 0                                  shard 1      …     shard N
  ├─ socket A → external-api-ws.kalshi.com ─┐
  └─ socket B → api.elections.kalshi.com ───┴─► ShardCore: a book copy per socket,
                                                one published copy per market
                                                              │ updates, snapshots
                                                              ▼
                                       writer thread → AlignedSbeWriter → file or stdout
```

- **`conn.rs`**: one task per socket. Signs the handshake, keeps one
  `orderbook_delta` subscription and adds markets in batches of 1,000 with one
  command awaiting its reply at a time. Every frame's `seq` is checked; a gap
  means some market on that socket lost a frame, so every book on it is marked
  unusable and re-requested with `get_snapshot`, without dropping the socket.
  Thirty seconds without any frame (Kalshi pings every 10 s) means the socket
  is dead and it reconnects with backoff, resubscribing everything. Kalshi's
  errors are sorted by what they mean: timeouts and rate limits are retried
  with backoff; a dead subscription (unknown subscription id, buffer
  overflow, channel error) reconnects; a refused batch is split until the bad
  ticker is isolated, and a refused market is retried on the next connection,
  never forgotten.
- **`shard.rs`**: `ShardCore` makes every decision and does no I/O, so it is
  unit tested directly.
  - Only the published (source) copy's changes are written.
  - If the source socket drops, loses a frame, or a delta would take a level
    below zero, the freshest other copy takes over with a snapshot.
  - If the other copy has applied a change (by `clock`) that the source still
    hasn't after 500 ms, the source has stalled or is buffering: it is
    replaced, with a snapshot.
  - Audit probes: every 5 s, 4 random books per socket are re-fetched with
    `get_snapshot`. Kalshi guarantees a snapshot includes every change up to
    its clock and nothing after, and frames on one socket arrive in order, so
    our copy must match it exactly. A mismatch means that socket's
    subscription missed changes: it is counted, the venue's book replaces
    ours, and the market is resubscribed on that socket.
  - Quiet markets (Extension 2). Activity is counted per market in 5-minute
    windows, but a plain "too quiet, resubscribe" rule can't tell a quiet
    market from a broken subscription, and most markets never move (in a
    5-minute run on demo, all the changes came from about 400 of ~256,000
    markets), so it would resubscribe almost everything for nothing. Two
    checks replace it. A copy that trails the other copy's clock for 60 s is
    resubscribed; if most of a socket's copies do, the socket is reconnected
    instead. A market that was active (10 or more changes in the last window)
    and has gone silent on every copy for 60 s is probed: if the venue's book
    matches ours, the market is just quiet; if not, it is resubscribed.
  - Repair sweep: a copy that stays unusable on a live socket for 15 s,
    whatever the reason (a lost or refused snapshot request, an add that
    never confirmed), is re-requested, then resubscribed; a socket where most
    copies need repair is reconnected.
  - Every socket reconnects every 30 min (±20%), one per shard at a time, and
    only while the other copy is up and current.
- **`collector.rs`**: discovery and the minute snapshot. New markets come from
  Kalshi's lifecycle feed and a full REST listing every 10 minutes. A listing
  removes markets only if it completed without errors, and never removes a
  market added after it started.
- **`writer.rs`**: the only owner of the file, fed by a channel, so the file
  sees one ordered stream. On shutdown the shards first apply every frame
  their sockets had already received, then the writer drains the channel and
  flushes. If a write fails (a full disk, say), the collector stops with an
  error instead of collecting what it can't record.
- **`venue/`**: everything exchange-specific (`kalshi.rs`, `polymarket.rs`)
  behind one trait: URLs and auth, command encoding, frame decoding and market
  listing.

Why 3,000 markets per socket: one demo socket accepted 100,000 markets, but
while digesting them its delay rose from about 30 ms to 0.8–2.6 s. Smaller
sockets keep start-up bursts and reconnects cheap, and ~260,000 markets × 2
copies still fit in 175 of the 200 sockets Kalshi allows per account.

## Evidence

Tests (`cargo test`, about 6 s):

- 41 unit tests: exact decimal parsing (every 4-decimal price), YES/NO
  mapping, recorded demo frames, Ed25519 and RSA-PSS signing, book
  arithmetic, and the switching and repair rules (failover, gaps, lag,
  probes, quiet copies, repairs, refusals, timestamps), including
  regression tests for each bug below.
- 6 end-to-end tests ([tests/fake_exchange.rs](tests/fake_exchange.rs)) run
  the whole collector against a fake Kalshi on localhost that drops a socket,
  loses a frame, silently stops one market on one socket, delays a socket,
  or answers the first subscribe with a timeout error. Each replays the
  output file and requires it to equal the exchange's final books exactly.
  With the lag switch disabled, the two tests that depend on it fail.

Live runs on Kalshi demo, from a Mac mini in Austin:

| Run | Markets | Sockets | Length | Messages written | `check` failures | Audit probes (mismatches) | Sequence gaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Every market, final code, written to stdout (Nov 10)³ | 243,005 | 165 | 4 min | 2.06 M | 0 | 25,387 (1) | 0 |
| Every market, default settings | 254,862 | 171 | 24 min¹ | 5.00 M | 0 | 181,329 (0) | 0 |
| Every market, after the review fixes² | 256,353 | 171 | 5 min | 1.44 M | 0 | 32,160 (0) | 0 |
| Every market, recycling every 2 min (stress) | 259,508 | 175 | 7 min | 2.70 M | 0 | 46,278 (0) | 0 |
| Polymarket, 2,000 books, after the review fixes | 2,000 | 8 | 2 min | 47,004 | 0 | n/a | n/a |

¹ It ended when the machine's disk filled, and stopped itself rather than
keep collecting what it couldn't write; the file holds the first 19
minutes. All 4.58 million snapshots in it matched the book rebuilt from the
updates before them. It ran at about 365 MB of memory and under 3% of one
CPU core.

² With `use_yes_price` on, no book was crossed after any of its 117,961
updates, a sanity check on the NO-side mapping (the direct check,
`examples/yes_price_check.rs`, found identical books in both modes for every
market with NO bids). Every snapshot again matched the replayed book. All of
that run's updates came from about 400 markets; the other ~256,000 demo
books never moved.

³ Demo was about 11 times busier than in the Nov 5 five-minute run (about
4,400 published updates a second, against 395), and the exchange misbehaved
in the ways the brief describes. 491 markets switched to their other copy
because the published copy fell more than 0.5 s behind, and 108 copies that
went a minute without a change the other copy had received were
resubscribed. One audit probe found a published copy one
contract off on one price level, with no gap in its socket's sequence
numbers: one change never reached that copy. The probe's snapshot, arriving
right behind it, corrected the book, and the market was resubscribed.
`check --corrections` lists the 490 snapshots that changed the replayed
book: 487 came within a second of a source switch on the same market, 1 is
that audit catch, and 2 are Kalshi sending empty books for two markets as
they closed, which were then unsubscribed. The file compressed 17× with
zstd.

The live runs caught three bugs, each now fixed and covered by a test:

1. Kalshi doesn't reply to `get_snapshot`. Waiting for a reply reconnected
   every socket at once, 30 s in.
2. The first quiet-copy rule counted a fresh snapshot as activity, so
   resubscribing one copy made the other look silent: about 280,000 needless
   resubscribes in 7 minutes at full scale. Copies are now compared by
   Kalshi's clock, which a snapshot cannot advance.
3. Recycling all 175 sockets every 2 minutes made Kalshi start closing
   sockets itself: 162 normal closes in 4 minutes, rising as recycling piled
   up. The books stayed exact. The default is now 30 minutes, and a socket
   whose copies mostly fall behind is reconnected once instead of
   resubscribing its markets one by one.

An independent review of the code then found five more, also fixed with
tests: a refused market was dropped from its socket for good; a lost
`get_snapshot` after a gap was never retried (now the repair sweep); a
quiet-market probe on Polymarket reconnected both sockets at once; a frame
processed just after a snapshot the collector generated could carry an
earlier timestamp; and the NO-side mapping depended on a Kalshi default that
is scheduled to change.

## Limits and next steps

- **Every live number here is from Kalshi's demo environment.** Production
  (`--env prod`, the default) runs the same code with the same message
  formats, auth and two-host layout, but it has not been run against
  production yet.
- Timestamps come from the wall clock. On the machine used here it ran 70–90 ms
  slow (`sntp`). A real capture belongs on a host with a disciplined clock
  (chrony, or AWS Time Sync on EC2) near Kalshi; production's load balancer is
  in us-east-2.
- Every source switch writes a full snapshot, as the brief asks, even when
  both copies agree, so a planned recycle writes a snapshot for every market
  whose published copy was on the recycled socket.
- The template's reader only reads 64 MiB chunks, so the writer buffers up to
  64 MiB. SIGINT and SIGTERM write everything already received; a hard kill
  or power loss loses at most the unflushed chunk.
- Snapshots of every book every minute dominate the file: about 17 MB a
  minute (1 GB an hour) for the ~255,000 demo markets, most of them empty
  books. The file compresses well (zstd, once it is closed).
- Polymarket has no sequence numbers and no in-place re-snapshot, so there
  lost frames show up only as one copy trailing the other, and there are no
  audit probes.
- Kalshi's per-socket market cap is unpublished; one demo socket took 100,000.

## Layout

```text
src/main.rs            CLI, signals, status line
src/collector.rs       discovery, market → shard assignment, minute snapshots
src/shard.rs           redundancy and checking logic (ShardCore) + its task
src/conn.rs            one websocket: subscribe, sequence checks, reconnects
src/writer.rs          the single writer thread
src/book.rs, fixed.rs  order book and exact fixed-point parsing
src/model.rs           exchange-neutral messages
src/venue/             Kalshi and Polymarket encode/decode/listing
src/bin/check.rs       output file checker
src/util/              the template's SBE helpers (unchanged)
tests/fake_exchange.rs end-to-end failure tests
examples/spike.rs      the throwaway probe that measured per-socket limits
examples/yes_price_check.rs  live check that use_yes_price changes no book
examples/dump_market.rs      every message for one market in an output file
```
