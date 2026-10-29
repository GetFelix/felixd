use super::*;

fn at(input_offset: u64) -> SnapshotHeader {
    SnapshotHeader {
        input_offset,
        output_tail: input_offset + 1,
    }
}

#[test]
fn the_latest_snapshot_below_the_end_is_restored() {
    let snapshots = [at(4), at(9), at(14)];
    assert_eq!(nearest_snapshot(snapshots, 15), Some(2));
    assert_eq!(nearest_snapshot(snapshots, 14), Some(1));
    assert_eq!(nearest_snapshot(snapshots, 10), Some(1));
    assert_eq!(nearest_snapshot(snapshots, 5), Some(0));
}

#[test]
fn no_snapshot_below_the_end_starts_from_the_initial_state() {
    assert_eq!(nearest_snapshot([at(4)], 4), None);
    assert_eq!(nearest_snapshot([], 100), None);
    assert_eq!(first_input(None), 0);
    assert_eq!(first_input(Some(at(4))), 5);
}

#[test]
fn snapshots_out_of_order_still_pick_the_highest() {
    // A stale owner's snapshot can land after a later one.
    assert_eq!(nearest_snapshot([at(9), at(4)], 20), Some(0));
    assert_eq!(nearest_snapshot([at(9), at(9)], 20), Some(1));
}

#[test]
fn a_time_past_every_input_replays_them_all() {
    assert_eq!(end_for_time(Some(3), 10), 3);
    assert_eq!(end_for_time(Some(0), 10), 0);
    assert_eq!(end_for_time(None, 10), 10);
    assert_eq!(end_for_time(Some(12), 10), 10);
}
