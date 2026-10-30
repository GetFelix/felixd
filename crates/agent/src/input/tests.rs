use bytes::Bytes;
use felixd_core::class::{ErrorClass, OperatorReason};
use felixd_core::output::{self, Addressed, OutputHeader, Rejected};

use super::StepInput;
use crate::runner::{NeedsOperator, Poison};

fn header(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn named(epoch: u64, at: OutputHeader, name: &str, key: Option<&[u8]>, payload: &[u8]) -> Vec<u8> {
    let addressed = Addressed { name, key, payload };
    output::encode_addressed(at, epoch, addressed).unwrap()
}

/// What a downstream step is given for `record` at `offset`: its key and
/// payload, or `None` when the record is passed over.
fn read(input: &str, offset: u64, record: Vec<u8>) -> Option<(Option<Vec<u8>>, Vec<u8>)> {
    let record = Bytes::from(record);
    StepInput::read(input, offset, 7, &record)
        .unwrap()
        .map(|step| {
            let given = step.as_input();
            assert_eq!(given.stream, input);
            assert_eq!((given.offset, given.timestamp_micros), (offset, 7));
            (given.key.map(<[u8]>::to_vec), given.payload.to_vec())
        })
}

#[test]
fn a_downstream_steps_only_outputs_addressed_to_out() {
    let up = "wl.up.out";
    let got = read(up, 3, named(1, header(0, 0), "out", Some(b"k"), b"v"));
    assert_eq!(got, Some((Some(b"k".to_vec()), b"v".to_vec())));
    assert_eq!(
        read(up, 4, output::encode(header(1, 0), 1, b"plain")),
        Some((None, b"plain".to_vec()))
    );
    let rejected = Rejected {
        code: 7,
        detail: b"bad",
    };
    let passed_over = [
        output::encode_claim(2, "node-a"),
        output::encode_claim_under(2, 5, 5, "node-a"),
        output::encode_failure(2, 9, ErrorClass::Poison, "trap"),
        output::encode_failure(2, 9, ErrorClass::Transient, "lost"),
        output::encode_needs_operator(2, 5, OperatorReason::SpecConflict, "input"),
        output::encode_rejection(2, 2, rejected),
        named(2, header(3, 0), "alerts", None, b"side"),
    ];
    for record in passed_over {
        assert_eq!(read(up, 5, record.clone()), None, "{record:?}");
    }
}

/// An upstream owner dies after part of a batch lands, and the next owner
/// claims and writes the rest. The downstream steps the four outputs in
/// order, with the claim between them passed over.
#[test]
fn an_upstream_takeover_in_the_middle_of_a_step_reads_as_its_outputs() {
    let up = "wl.up.out";
    let stream = [
        named(1, header(9, 0), "out", None, b"a"),
        named(1, header(9, 1), "out", None, b"b"),
        output::encode_claim_under(2, 0, 0, "node-b"),
        named(2, header(9, 2), "out", None, b"c"),
        named(2, header(9, 3), "out", None, b"d"),
    ];
    let stepped: Vec<(u64, Vec<u8>)> = (10..)
        .zip(stream)
        .filter_map(|(offset, record)| read(up, offset, record).map(|(_, p)| (offset, p)))
        .collect();
    let want: Vec<(u64, Vec<u8>)> = vec![
        (10, b"a".to_vec()),
        (11, b"b".to_vec()),
        (13, b"c".to_vec()),
        (14, b"d".to_vec()),
    ];
    assert_eq!(stepped, want);
}

#[test]
fn an_older_output_record_reads_as_out() {
    // Version 2: version, input offset, index, epoch, payload.
    let mut v2 = vec![2];
    v2.extend(4u64.to_be_bytes());
    v2.extend(0u32.to_be_bytes());
    v2.extend(1u64.to_be_bytes());
    v2.extend(b"old");
    assert_eq!(read("wl.up.out", 0, v2), Some((None, b"old".to_vec())));
}

/// A newer build can add a failure class or a needs-operator reason. A
/// downstream on an older build passes over such a record by its kind
/// instead of failing its own workload.
#[test]
fn a_marker_with_a_code_this_build_does_not_know_is_passed_over() {
    let mut failure = output::encode_failure(2, 9, ErrorClass::Poison, "trap");
    failure[18] = 99;
    let mut needs = output::encode_needs_operator(2, 5, OperatorReason::SpecConflict, "input");
    needs[18..20].copy_from_slice(&99u16.to_be_bytes());
    for record in [failure, needs] {
        assert!(output::decode(&record).is_err());
        assert_eq!(read("wl.up.out", 5, record.clone()), None, "{record:?}");
    }
}

/// A version or kind this build does not know is the build's fault, not the
/// workload's, so it needs an operator to upgrade the node.
#[test]
fn a_record_of_an_unknown_version_or_kind_needs_an_operator() {
    let mut kind = output::encode_claim(2, "node-a");
    kind[1] = 9;
    for record in [b"\x09junk".to_vec(), kind] {
        let error = StepInput::read("wl.up.out", 4, 0, &Bytes::from(record))
            .err()
            .expect("an unknown record stops the reader");
        let problem = error
            .downcast_ref::<NeedsOperator>()
            .unwrap_or_else(|| panic!("not needs-operator: {error:#}"));
        assert_eq!(problem.reason, OperatorReason::InputUnreadable);
        assert!(problem.detail.contains("input 4 on wl.up.out"), "{problem}");
    }
}

#[test]
fn an_output_that_does_not_decode_is_poison() {
    let mut record = output::encode(header(1, 0), 1, b"p");
    record.truncate(15);
    let error = StepInput::read("wl.up.out", 4, 0, &Bytes::from(record))
        .err()
        .expect("a truncated output fails the reader");
    assert!(error.is::<Poison>(), "{error:#}");
}

#[test]
fn a_stream_that_is_no_workloads_output_is_read_whole() {
    let claim = output::encode_claim(2, "node-a");
    for input in ["in", "wl.up.snap", "wl.Up.out"] {
        assert_eq!(
            read(input, 0, claim.clone()),
            Some((None, claim.clone())),
            "{input}"
        );
    }
}
