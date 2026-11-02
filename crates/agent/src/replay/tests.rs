use super::*;

fn trimmed(oldest: u64) -> anyhow::Error {
    Trimmed {
        stream: "wl.a.in".into(),
        requested: 0,
        oldest,
    }
    .into()
}

#[test]
fn a_target_older_than_the_oldest_kept_snapshot_names_where_replay_can_start() {
    let message = older_than_snapshots(
        trimmed(90),
        &Start::Initial(Initial::NoSnapshot),
        Some(120),
        40,
        0,
    )
    .to_string();
    assert_eq!(
        message,
        "felixd replay: input 40 is older than the oldest kept snapshot, which is of input 120, \
         and wl.a.in no longer holds the inputs before offset 90; the oldest offset replay can reach is 120"
    );
}

#[test]
fn trimmed_inputs_without_a_kept_snapshot_are_explained_as_before() {
    let message = older_than_snapshots(
        trimmed(90),
        &Start::Initial(Initial::NoSnapshot),
        None,
        40,
        0,
    )
    .to_string();
    assert!(message.starts_with("felixd replay needs wl.a.in from offset 0"));
}

#[test]
fn a_replay_from_the_start_keeps_the_plain_explanation() {
    let message = older_than_snapshots(
        trimmed(90),
        &Start::Initial(Initial::Requested),
        Some(120),
        40,
        0,
    )
    .to_string();
    assert!(message.starts_with("felixd replay needs wl.a.in from offset 0"));
}

#[test]
fn any_other_error_is_left_as_it_was() {
    let message = older_than_snapshots(
        anyhow!("connection reset"),
        &Start::Initial(Initial::NoSnapshot),
        Some(120),
        40,
        0,
    )
    .to_string();
    assert_eq!(message, "connection reset");
}

#[test]
fn a_diff_can_start_one_input_after_the_oldest_kept_snapshot() {
    let message = older_than_snapshots(
        trimmed(90),
        &Start::Initial(Initial::NoSnapshot),
        Some(120),
        40,
        1,
    )
    .to_string();
    assert!(
        message.ends_with("the oldest offset replay can reach is 121"),
        "{message}"
    );
}
