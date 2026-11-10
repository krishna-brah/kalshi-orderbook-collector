# Latency notes (Extension 4)

Completeness comes first, so nothing here trades a missed update for speed.
These are measurements from the collector itself. Except where noted, they
were taken on Kalshi's demo environment from a Mac mini in Austin, which is far
from where a real collector should run.

## Two ways to measure, and why one is unreliable here

1. **Receive time minus Kalshi's send stamp.** Every frame carries
   `sending_ts_ms`, when Kalshi queued it. Subtracting it from our receive
   time gives a per-socket delay, but it mixes in the difference between our
   clock and Kalshi's. On this machine `sntp` put the clock 70–90 ms slow, and
   many samples came out negative. Treat absolute values from this machine as
   wrong by about that much.
2. **The arrival race.** This one doesn't depend on clocks. Kalshi gives the
   same change the same `clock` on every connection, so when two sockets carry
   the same market the collector knows exactly which socket delivered each
   change first, and how far behind the others were, all on one local clock.
   The status line and run summary report this per route.

## What the hosts are

| Host | Resolves to |
| --- | --- |
| `external-api-ws.kalshi.com` (production, recommended) | CNAME to `external-api-alb.elections.kalshi.com`: 8 addresses, all AWS us-east-2 (checked against AWS's published IP ranges) |
| `api.elections.kalshi.com` (production, also supported) | CloudFront |
| `external-api-ws.demo.kalshi.co` | an AWS load balancer in us-west-2 (2 addresses) |
| `demo-api.kalshi.co` | CloudFront |

The collector puts a market's two copies on the two hosts, so they take
different paths: the direct load balancer and CloudFront's edge.

## Findings

**The route matters more than the connection.** Two runs, both on demo from
Austin:

- *Every demo market, two sockets per market (one per host), 5 minutes.* Of
  119,053 changes that reached both copies, CloudFront (`demo-api.kalshi.co`)
  delivered 89,441 first (75%). When CloudFront lost, it trailed by a median
  12 ms (p99 64 ms). When the direct load balancer lost, it trailed by a median
  58 ms (p99 263 ms).
- *The 400 busiest markets, four sockets per market (two per host), 6
  minutes, 148,858 changes.* Every update in the full run came from these 400
  markets, so this is where latency matters.

  | Socket | Host | First to deliver | Behind the first, when not (p50 / p90 / p99) |
  | --- | --- | ---: | --- |
  | 1 | CloudFront | 114,930 (77.2%) | 0.02 / 0.3 / 49 ms |
  | 3 | CloudFront | 33,270 (22.4%) | 0.1 / 4.2 / 12 ms |
  | 2 | load balancer | 409 (0.3%) | 9.9 / 84 / 150 ms |
  | 0 | load balancer | 249 (0.2%) | 11.6 / 88 / 162 ms |

  The two CloudFront sockets between them won 99.6% of changes, and the gap
  between them was usually microseconds. From here, picking the route is what
  counts; opening more sockets on the same route buys a little at the tail
  (socket 3's p99 of 12 ms against socket 1's 49 ms), not at the median.

**Which route wins changes from day to day.** A second run on every demo
market, on Nov 10, with demo about 11 times busier (about 4,400 published
updates a second, against 395), reversed the Nov 5 result. The direct load
balancer delivered 980,543 changes first (67%) and CloudFront 478,739.
Against Kalshi's send stamp, the direct route's median delay was 97 ms (p99
657 ms) and CloudFront's 155 ms (p99 1.9 s); this machine's clock was within
about 25 ms of Apple's time servers that day, so those are close to real
delays. 447 of the run's 491 lag switches moved a market onto the direct
route. A route can't be picked once and trusted, which is why the collector
races its copies all the time and switches per market.

**What the collector does with this.** Both copies of a market carry the same
change under the same `clock`, so each published update is stamped with the
earliest time *any* socket received it, not just the published copy's time.
The published copy still decides the content; the fastest copy decides the
timestamp. Losing the race on one route no longer shows up in the file.
In a 2.5-minute run on the 400 busiest markets, 10,838 of 51,607 published
updates (21%) took the other copy's earlier time, with no timestamp going
backwards (`updates_stamped_from_faster_copy` in the run summary).

**A socket's load sets its delay.** While one demo socket subscribed 30,000
markets in batches of 1,000, delay against the send stamp stayed at p50 28 ms
and p99 258 ms. Subscribing 100,000 in batches of 5,000 pushed it to p50 776 ms
and p99 2.56 s: frames queue behind the snapshot burst. The collector
therefore keeps 3,000 markets per socket and adds them 1,000 at a time.

**Snapshot bursts dominate start-up.** In full-universe runs, the windowed
p99 delay is in seconds while the ~255,000 initial snapshots stream in, then
settles to tens of milliseconds. Recycling a socket replays that burst for its
3,000 markets, which is one reason recycling is staggered and slow (30 min
±20%, one socket per shard at a time).

**Kalshi pushes back on reconnect storms.** Recycling all 175 sockets every 2
minutes made Kalshi close sockets on its own (normal close frames), at a rate
that rose as recycling continued. Connection churn is not free on their side
either.

## Experiments to run next

These need a production key and a host in us-east-2 with a disciplined clock
(AWS Time Sync), which this machine isn't:

- **Route versus distance.** On demo from Austin, CloudFront wins. Its edge
  is nearby, while demo's load balancer is in Oregon. From inside us-east-2,
  production's direct load balancer may win instead. The race metric answers
  this directly, and the per-route counters show it within minutes.
- **Pick the fastest load-balancer node.** `external-api-ws.kalshi.com` has 8
  addresses. Connect to each explicitly (keeping the hostname for TLS),
  measure the race for a few minutes, and keep the fastest few. This is the
  "spam connections until you find a fast one" idea, with a measurement
  instead of a guess. The four-socket run above suggests that on one route
  the gain is mostly at the tail.
- **Source ports.** Different local ports give different 5-tuples, which can
  hash to different paths and targets inside AWS. Binding sockets to chosen
  local ports and racing them tests whether that matters here.
- **Being first after maintenance.** Kalshi schedules maintenance windows. A
  collector that reconnects the moment trading resumes may land on fresh,
  idle backends, before everyone else's reconnect storm.
- **Stamp earlier.** `backtest_timestamp` is taken after TLS decryption and
  frame parsing in user space. Kernel receive timestamps (`SO_TIMESTAMPING`)
  plus a pinned reader thread per hot socket would remove our own scheduling
  delay from the stamp.
