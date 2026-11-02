use super::*;

fn trimmed(oldest: u64) -> anyhow::Error {
    Trimmed {
        stream: "wl.x.out".into(),
        requested: 0,
        oldest,
    }
    .into()
}

fn snapshot(output_tail: u64) -> SnapshotHeader {
    SnapshotHeader {
        input_offset: 9,
        output_tail,
    }
}

/// The snapshot covers outputs below its tail. If Felix keeps nothing below
/// a later offset, the outputs between are not findings, they are history
/// Felix no longer holds.
#[test]
fn a_head_cut_above_the_snapshots_tail_is_the_trimmed_error() {
    let error = covered(&trimmed(30), &snapshot(20)).unwrap_err();
    let found = Trimmed::of(&error).expect("the error says the history is trimmed");
    assert_eq!((found.stream.as_str(), found.oldest), ("wl.x.out", 30));
}

#[test]
fn a_head_cut_at_or_below_the_snapshots_tail_is_fine() {
    assert!(covered(&trimmed(20), &snapshot(20)).is_ok());
    assert!(covered(&trimmed(5), &snapshot(20)).is_ok());
    assert!(covered(&anyhow::anyhow!("other"), &snapshot(20)).is_ok());
}

/// A snapshot is taken as given, so a check uses one only when asked or when
/// the first input can no longer be replayed from.
#[test]
fn a_check_replays_from_the_first_input_unless_the_head_is_gone() {
    assert!(!starts_at_snapshot(Origin::Auto, false));
    assert!(starts_at_snapshot(Origin::Auto, true));
    assert!(starts_at_snapshot(Origin::Snapshot, false));
    assert!(!starts_at_snapshot(Origin::Start, true));
}
