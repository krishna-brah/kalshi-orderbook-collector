use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::Side;
use crate::book::LevelChange;

const LAG: Duration = Duration::from_millis(500);
const QUIET: Duration = Duration::from_secs(60);

fn core() -> ShardCore {
    let stats = Arc::new(Stats::new(&["wss://a".into(), "wss://b".into()]));
    let mut core = ShardCore::new(2, LAG, QUIET, true, stats);
    core.add(vec!["M".into()]);
    core.conn_up(0);
    core.conn_up(1);
    core
}

fn snapshot(clock: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> BookMsg {
    BookMsg {
        market: "M".into(),
        clock: Some(clock),
        exchange_ts: None,
        body: BookBody::Snapshot { bids: bids.to_vec(), asks: asks.to_vec() },
    }
}

fn delta(clock: u64, side: Side, price: i64, change: i64) -> BookMsg {
    BookMsg {
        market: "M".into(),
        clock: Some(clock),
        exchange_ts: Some(1_000 + clock as i64),
        body: BookBody::Level { side, price, change: LevelChange::Delta(change) },
    }
}

/// Both copies hold the same starting book; slot 0 is the source.
fn synced(now: Instant) -> ShardCore {
    let mut core = core();
    core.on_book(0, 10, None, now, snapshot(5, &[(400_000, 10)], &[(600_000, 20)]));
    core.on_book(1, 11, None, now, snapshot(5, &[(400_000, 10)], &[(600_000, 20)]));
    core.out.clear();
    core
}

fn snapshot_of(msg: &OutMsg) -> &SnapshotDomain {
    match msg {
        OutMsg::Snapshot(s) => s,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

fn update_of(msg: &OutMsg) -> &PriceLevelUpdateDomain {
    match msg {
        OutMsg::Update(u) => u,
        other => panic!("expected an update, got {other:?}"),
    }
}

#[test]
fn the_first_snapshot_is_published_and_only_the_source_copy_streams() {
    let now = Instant::now();
    let mut core = core();
    core.on_book(0, 10, None, now, snapshot(5, &[(400_000, 10)], &[]));
    assert_eq!(core.out.len(), 1);
    assert_eq!(snapshot_of(&core.out[0]).bids[0].price_level, 400_000);
    core.on_book(1, 11, None, now, snapshot(5, &[(400_000, 10)], &[]));
    assert_eq!(core.out.len(), 1, "the backup copy's snapshot is not published");

    core.out.clear();
    core.on_book(0, 20, None, now, delta(6, Side::Buy, 400_000, 5));
    core.on_book(1, 21, None, now, delta(6, Side::Buy, 400_000, 5));
    assert_eq!(core.out.len(), 1, "one copy of each change");
    let update = update_of(&core.out[0]);
    assert_eq!((update.price_level, update.quantity), (400_000, 15), "absolute size, not the delta");
    assert_eq!(update.backtest_timestamp.timestamp_micros(), 20);
    assert_eq!(update.exchange_timestamp.timestamp_micros(), 1_006);
}

#[test]
fn a_deleted_level_is_published_as_zero() {
    let now = Instant::now();
    let mut core = synced(now);
    core.on_book(0, 20, None, now, delta(6, Side::Sell, 600_000, -20));
    let update = update_of(&core.out[0]);
    assert_eq!((update.side, update.price_level, update.quantity), (Side::Sell, 600_000, 0));
}

#[test]
fn when_the_source_drops_the_other_copy_takes_over_with_a_snapshot() {
    let now = Instant::now();
    let mut core = synced(now);
    // Slot 1 sees a change, then slot 0's socket dies before delivering it.
    core.on_book(1, 30, None, now, delta(7, Side::Buy, 400_000, -4));
    core.conn_lost(0, true, 31);
    assert_eq!(core.out.len(), 1);
    let snap = snapshot_of(&core.out[0]);
    assert_eq!(snap.received_at.timestamp_micros(), 31);
    assert_eq!(snap.bids[0].quantity, 6, "the snapshot carries the new source's book");

    core.out.clear();
    core.on_book(1, 40, None, now, delta(8, Side::Buy, 400_000, 1));
    assert_eq!(update_of(&core.out[0]).quantity, 7);
    assert_eq!(core.stats.snapshots_out_failover.get(), 1);
}

#[test]
fn a_gap_makes_every_copy_on_that_socket_unusable_until_resnapshotted() {
    let now = Instant::now();
    let mut core = synced(now);
    core.conn_lost(0, false, 50);
    assert!(core.is_up(0), "a gap is not a disconnect");
    assert_eq!(core.out.len(), 1, "failover to slot 1");
    core.out.clear();
    core.on_book(0, 51, None, now, delta(6, Side::Buy, 400_000, 1));
    assert!(core.out.is_empty());
    assert_eq!(core.stats.stale_deltas.get(), 1, "deltas on an invalid copy are dropped");
    core.on_book(0, 52, None, now, snapshot(6, &[(400_000, 11)], &[]));
    assert!(core.out.is_empty(), "slot 0 is a backup again, not the source");
}

#[test]
fn a_source_that_trails_the_other_copy_is_replaced_after_the_threshold() {
    let start = Instant::now();
    let mut core = synced(start);
    core.on_book(1, 60, None, start, delta(9, Side::Buy, 400_000, 2));
    core.check_lags(start + LAG / 2, 61);
    assert!(core.out.is_empty(), "a short lead is normal jitter");
    core.check_lags(start + LAG + Duration::from_millis(1), 62);
    assert_eq!(core.out.len(), 1);
    assert_eq!(snapshot_of(&core.out[0]).bids[0].quantity, 12);
    assert_eq!(core.stats.snapshots_out_lag.get(), 1);

    // The old source catching up later publishes nothing.
    core.out.clear();
    core.on_book(0, 63, None, start, delta(9, Side::Buy, 400_000, 2));
    assert!(core.out.is_empty());
}

#[test]
fn a_source_that_catches_up_in_time_is_kept() {
    let start = Instant::now();
    let mut core = synced(start);
    core.on_book(1, 60, None, start, delta(9, Side::Buy, 400_000, 2));
    core.on_book(0, 61, None, start, delta(9, Side::Buy, 400_000, 2));
    core.out.clear();
    core.check_lags(start + LAG * 2, 62);
    assert!(core.out.is_empty());
}

#[test]
fn a_delta_that_breaks_the_book_triggers_a_resnapshot_and_failover() {
    let now = Instant::now();
    let mut core = synced(now);
    core.on_book(0, 70, None, now, delta(6, Side::Buy, 400_000, -11));
    assert_eq!(core.stats.negative_levels.get(), 1);
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Snapshots(m))] if m == &["M"]));
    assert_eq!(core.out.len(), 1, "slot 1 takes over");
    assert_eq!(snapshot_of(&core.out[0]).bids[0].quantity, 10);
}

#[test]
fn audit_probes_compare_the_venue_book_with_ours() {
    let now = Instant::now();
    let mut core = synced(now);
    assert_eq!(core.pick_probes(0, 1), ["M"]);
    core.on_book(0, 80, None, now, snapshot(5, &[(400_000, 10)], &[(600_000, 20)]));
    assert_eq!((core.stats.probes.get(), core.stats.probe_mismatches.get()), (1, 0));
    assert!(core.out.is_empty(), "a matching probe changes nothing");

    assert_eq!(core.pick_probes(0, 1), ["M"]);
    core.on_book(0, 81, None, now, snapshot(6, &[(400_000, 9)], &[(600_000, 20)]));
    assert_eq!((core.stats.probes.get(), core.stats.probe_mismatches.get()), (2, 1));
    assert_eq!(snapshot_of(&core.out[0]).bids[0].quantity, 9, "the venue's book replaces ours");
}

#[test]
fn a_copy_that_never_gets_a_change_the_other_copy_got_is_resubscribed() {
    let start = Instant::now();
    let mut core = synced(start);
    core.on_book(1, 90, None, start, delta(9, Side::Buy, 400_000, 1));
    core.check_quiet(start + QUIET / 2, 91);
    assert!(core.cmds.is_empty(), "trailing briefly is normal");
    core.check_quiet(start + QUIET + Duration::from_secs(1), 92);
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Resubscribe(m))] if m == &["M"]));
    assert_eq!(core.stats.quiet_resubscribes.get(), 1);
    assert_eq!(core.out.len(), 1, "slot 0 was the source, so slot 1 takes over");
}

#[test]
fn a_fresh_snapshot_on_one_copy_does_not_make_the_other_look_quiet() {
    // Regression: counting a resubscribe's snapshot as activity made the
    // other copy look silent, which resubscribed it, and so on forever.
    let start = Instant::now();
    let mut core = synced(start);
    core.conn_lost(0, false, 1);
    core.on_book(0, 2, None, start + QUIET * 2, snapshot(5, &[(400_000, 10)], &[(600_000, 20)]));
    core.check_quiet(start + QUIET * 2 + Duration::from_secs(1), 3);
    core.check_quiet(start + QUIET * 4, 4);
    assert!(core.cmds.is_empty(), "both copies are at the same clock: {:?}", core.cmds);
    assert_eq!(core.stats.quiet_resubscribes.get(), 0);
}

#[test]
fn quiet_markets_are_left_alone_unless_they_used_to_be_active() {
    let start = Instant::now();
    let mut core = synced(start);
    core.check_quiet(start + QUIET * 3, 1);
    assert!(core.cmds.is_empty(), "both copies quiet and the market was never active");

    for clock in 6..20 {
        core.on_book(0, 2, None, start, delta(clock, Side::Buy, 400_000, 1));
        core.on_book(1, 2, None, start, delta(clock, Side::Buy, 400_000, 1));
    }
    core.roll_activity_window();
    core.check_quiet(start + QUIET * 3, 3);
    assert_eq!(core.cmds.len(), 2, "an active market gone silent is probed on both copies");
    assert!(core.cmds.iter().all(|(_, cmd)| matches!(cmd, ConnCmd::Snapshots(_))));
}

#[test]
fn when_most_of_a_socket_goes_quiet_the_socket_is_reconnected_instead() {
    let start = Instant::now();
    let mut core = synced(start);
    let markets: Vec<String> = (0..60).map(|i| format!("Q{i}")).collect();
    core.add(markets.clone());
    for market in &markets {
        let snap = |clock| BookMsg { market: market.clone(), ..snapshot(clock, &[(400_000, 1)], &[]) };
        core.on_book(0, 1, None, start, snap(5));
        core.on_book(1, 1, None, start, snap(5));
        // Only slot 1 hears about the next change.
        let change = BookMsg { market: market.clone(), ..delta(6, Side::Buy, 400_000, 1) };
        core.on_book(1, 2, None, start, change);
    }
    core.cmds.clear();
    core.check_quiet(start + QUIET + Duration::from_secs(1), 3);
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Reconnect)]), "{:?}", core.cmds);
    assert_eq!(core.stats.quiet_reconnects.get(), 1);
    assert_eq!(core.stats.quiet_resubscribes.get(), 0);
}

#[test]
fn minute_snapshots_cover_every_book_with_a_usable_copy() {
    let now = Instant::now();
    let mut core = synced(now);
    core.add(vec!["UNSYNCED".into()]);
    core.snapshot_all(100);
    assert_eq!(core.out.len(), 1);
    assert_eq!(snapshot_of(&core.out[0]).received_at.timestamp_micros(), 100);
    assert_eq!(core.stats.uncovered.get(), 1);
}

#[test]
fn frames_for_removed_markets_are_ignored() {
    let now = Instant::now();
    let mut core = synced(now);
    assert_eq!(core.remove(vec!["M".into(), "NEVER".into()]), ["M"]);
    core.on_book(0, 5, None, now, delta(6, Side::Buy, 400_000, 1));
    assert!(core.out.is_empty());
}

#[test]
fn the_arrival_race_credits_the_earlier_route() {
    let now = Instant::now();
    let mut core = synced(now);
    core.on_book(1, 1_000, None, now, delta(6, Side::Buy, 400_000, 1));
    core.on_book(0, 1_250, None, now, delta(6, Side::Buy, 400_000, 1));
    // Two deltas sharing a clock are matched in order, not to each other.
    core.on_book(0, 2_000, None, now, delta(7, Side::Buy, 400_000, 1));
    core.on_book(0, 2_010, None, now, delta(7, Side::Sell, 600_000, 1));
    core.on_book(1, 2_500, None, now, delta(7, Side::Buy, 400_000, 1));
    core.on_book(1, 2_600, None, now, delta(7, Side::Sell, 600_000, 1));
    assert_eq!(core.stats.route(1).first_arrivals.get(), 1);
    assert_eq!(core.stats.route(0).first_arrivals.get(), 2);
    assert_eq!(core.stats.route(0).behind_first.quantile_ms(0.5), Some(0.25));
    assert_eq!(core.stats.route(1).behind_first.count(), 2);
    assert_eq!(core.stats.route(1).behind_first.quantile_ms(1.0), Some(0.59));
}

#[test]
fn a_market_never_gets_a_timestamp_earlier_than_one_already_written() {
    // A failover snapshot is stamped when it is made (t=500). A delta the
    // new source had received earlier (t=400) but that was still queued
    // must not be written with the earlier stamp after it.
    let now = Instant::now();
    let mut core = synced(now);
    core.conn_lost(0, true, 500);
    core.on_book(1, 400, None, now, delta(6, Side::Buy, 400_000, 1));
    let stamps: Vec<i64> = core
        .out
        .iter()
        .map(|m| match m {
            OutMsg::Snapshot(s) => s.received_at.timestamp_micros(),
            OutMsg::Update(u) => u.backtest_timestamp.timestamp_micros(),
        })
        .collect();
    assert_eq!(stamps, [500, 500]);
    assert_eq!(core.stats.stamp_clamps.get(), 1);
}

#[test]
fn an_exchange_snapshot_carries_the_venue_send_time() {
    let now = Instant::now();
    let mut core = core();
    core.on_book(0, 10, Some(7), now, snapshot(5, &[(400_000, 1)], &[]));
    assert_eq!(snapshot_of(&core.out[0]).exchange_timestamp.timestamp_micros(), 7);
}

#[test]
fn a_copy_left_unusable_is_re_requested_then_resubscribed() {
    let start = Instant::now();
    let mut core = synced(start);
    // A gap invalidates slot 0, and the re-snapshot never arrives.
    core.conn_lost(0, false, 1);
    core.cmds.clear();
    core.check_repairs(start);
    assert!(core.cmds.is_empty(), "noticed, but too soon to act");
    core.check_repairs(start + REPAIR_AFTER + Duration::from_secs(1));
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Snapshots(m))] if m == &["M"]));
    core.cmds.clear();
    core.check_repairs(start + REPAIR_AFTER + Duration::from_secs(2));
    assert!(core.cmds.is_empty(), "one request at a time");
    core.check_repairs(start + REPAIR_AFTER + REPAIR_RETRY + Duration::from_secs(2));
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Resubscribe(m))] if m == &["M"]));
    assert_eq!(core.stats.repairs.get(), 2);

    // Once the copy is usable again, nothing more is requested.
    core.cmds.clear();
    core.on_book(0, 2, None, start, snapshot(5, &[(400_000, 10)], &[(600_000, 20)]));
    core.check_repairs(start + REPAIR_AFTER * 10);
    assert!(core.cmds.is_empty());
}

#[test]
fn refused_copies_wait_for_the_next_connection_instead_of_the_repair_sweep() {
    let start = Instant::now();
    let mut core = synced(start);
    core.refused(0, &["M".into()], 1);
    core.check_repairs(start);
    core.check_repairs(start + REPAIR_AFTER * 4);
    assert!(core.cmds.is_empty());
    core.conn_up(0);
    core.check_repairs(start + REPAIR_AFTER * 5);
    core.check_repairs(start + REPAIR_AFTER * 7);
    assert!(!core.cmds.is_empty(), "after a reconnect the copy is repaired again if still missing");
}

#[test]
fn venues_without_in_place_snapshots_get_no_probes_and_resubscribe_repairs() {
    let start = Instant::now();
    let stats = Arc::new(Stats::new(&["wss://a".into()]));
    let mut core = ShardCore::new(2, LAG, QUIET, false, stats);
    core.add(vec!["M".into()]);
    core.conn_up(0);
    core.conn_up(1);
    core.on_book(0, 1, None, start, snapshot(5, &[(400_000, 1)], &[]));
    core.on_book(1, 1, None, start, snapshot(5, &[(400_000, 1)], &[]));
    for clock in 6..20 {
        core.on_book(0, 2, None, start, delta(clock, Side::Buy, 400_000, 1));
        core.on_book(1, 2, None, start, delta(clock, Side::Buy, 400_000, 1));
    }
    core.roll_activity_window();
    assert!(core.pick_probes(0, 4).is_empty());
    core.check_quiet(start + QUIET * 3, 3);
    assert!(core.cmds.is_empty(), "a quiet active market isn't probed when probing means reconnecting");

    core.conn_lost(0, false, 4);
    core.check_repairs(start);
    core.check_repairs(start + REPAIR_AFTER + Duration::from_secs(1));
    assert!(matches!(core.cmds.as_slice(), [(0, ConnCmd::Resubscribe(_))]), "{:?}", core.cmds);
}

#[test]
fn an_update_is_stamped_when_the_fastest_copy_received_it() {
    let now = Instant::now();
    let mut core = synced(now);
    // Slot 0 publishes, but slot 1 heard about the change 250 µs sooner.
    core.on_book(1, 1_000, None, now, delta(6, Side::Buy, 400_000, 1));
    core.on_book(0, 1_250, None, now, delta(6, Side::Buy, 400_000, 1));
    assert_eq!(update_of(&core.out[0]).backtest_timestamp.timestamp_micros(), 1_000);
    assert_eq!(core.stats.stamped_from_faster_copy.get(), 1);

    // When the published copy is first, its own time stands.
    core.out.clear();
    core.on_book(0, 2_000, None, now, delta(7, Side::Buy, 400_000, 1));
    core.on_book(1, 2_400, None, now, delta(7, Side::Buy, 400_000, 1));
    assert_eq!(update_of(&core.out[0]).backtest_timestamp.timestamp_micros(), 2_000);
}
