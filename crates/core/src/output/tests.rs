use sha2::Digest;

use super::*;
use crate::class::{ErrorClass, OperatorReason};

fn h(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn out(header: OutputHeader, epoch: u64, payload: &[u8]) -> Record<'_> {
    Record::Output(Written { header, epoch }, Addressed::unnamed(payload))
}

#[test]
fn a_record_round_trips() {
    let header = h(0x0102_0304_0506_0708, 9);
    let record = encode(header, 42, b"payload");
    assert_eq!(decode(&record), Ok(out(header, 42, b"payload")));
}

#[test]
fn the_layout_is_fixed() {
    let record = encode(h(7, 2), 5, b"x");
    assert_eq!(
        record,
        [
            4, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, 3, b'o', b'u', b't',
            0, b'x'
        ]
    );
    let keyed = Addressed {
        name: "a",
        key: Some(b"k"),
        payload: b"x",
    };
    assert_eq!(
        encode_addressed(h(7, 2), 5, keyed).unwrap()[22..],
        [1, b'a', 1, 0, 0, 0, 1, b'k', b'x']
    );
    assert_eq!(encode_claim(5, ""), [4, 1, 0, 0, 0, 0, 0, 0, 0, 5]);
    assert_eq!(
        encode_rejection(
            7,
            5,
            Rejected {
                code: 9,
                detail: b"d"
            }
        ),
        [
            4, 3, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, b'd'
        ]
    );
}

#[test]
fn a_claim_under_a_spec_names_it() {
    let record = encode_claim_under(5, 3, 7, "n");
    assert_eq!(
        record,
        [
            4, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 7, b'n'
        ]
    );
    assert_eq!(
        decode(&record),
        Ok(Record::Claim {
            epoch: 5,
            node: b"n",
            spec: Some(3),
            floor: Some(7),
        })
    );
    // The floor never falls below the claim's own spec.
    assert!(matches!(
        decode(&encode_claim_under(5, 9, 2, "n")),
        Ok(Record::Claim { floor: Some(9), .. })
    ));
    assert!(is_marker(&record));
    // Version 3 never had it.
    let mut old = record.clone();
    old[0] = 3;
    assert_eq!(decode(&old), Err(DecodeError::UnknownKind(4)));
}

#[test]
fn a_claim_round_trips_and_holds_no_output() {
    let record = encode_claim(9, "");
    assert_eq!(
        decode(&record),
        Ok(Record::Claim {
            epoch: 9,
            node: b"",
            spec: None,
            floor: None,
        })
    );
    assert_eq!(
        decode(&encode_claim(9, "node-a")),
        Ok(Record::Claim {
            epoch: 9,
            node: b"node-a",
            spec: None,
            floor: None,
        })
    );
    assert_eq!(decode(&record).unwrap().epoch(), 9);
    assert_eq!(decode(&record).unwrap().output(), None);
    assert!(is_marker(&record));
    assert!(!is_marker(&encode(h(1, 0), 9, b"")));
    assert!(!same_output(&record, &record));
}

#[test]
fn a_version_3_record_reads_as_out_with_no_key() {
    let output = [
        3, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, b'x',
    ];
    assert_eq!(decode(&output), Ok(out(h(7, 2), 5, b"x")));
    let claim = [3, 1, 0, 0, 0, 0, 0, 0, 0, 5, b'n'];
    assert_eq!(
        decode(&claim),
        Ok(Record::Claim {
            epoch: 5,
            node: b"n",
            spec: None,
            floor: None,
        })
    );
    let failure = [3, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, b'r'];
    assert_eq!(
        decode(&failure),
        Ok(Record::Failure {
            epoch: 5,
            spec_version: 1,
            class: ErrorClass::Poison,
            reason: b"r"
        })
    );
    // Rejections are new in version 4.
    let mut rejection = encode_rejection(
        7,
        5,
        Rejected {
            code: 1,
            detail: b"",
        },
    );
    rejection[0] = 3;
    assert_eq!(decode(&rejection), Err(DecodeError::UnknownKind(3)));
}

#[test]
fn a_version_2_record_still_reads() {
    let record = [
        2, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, b'x',
    ];
    assert_eq!(decode(&record), Ok(out(h(7, 2), 5, b"x")));
}

#[test]
fn an_empty_payload_is_allowed() {
    let header = h(1, 0);
    assert_eq!(decode(&encode(header, 1, b"")), Ok(out(header, 1, b"")));
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(decode(&[]), Err(DecodeError::Truncated { need: 1, got: 0 }));
    assert_eq!(
        decode(&[4, 0, 0]),
        Err(DecodeError::Truncated { need: 10, got: 3 })
    );
    assert_eq!(
        decode(&[4, 1, 0]),
        Err(DecodeError::Truncated { need: 10, got: 3 })
    );
    assert_eq!(
        decode(&[2, 0, 0]),
        Err(DecodeError::Truncated { need: 9, got: 3 })
    );
    let keyed = Addressed {
        name: "out",
        key: Some(b"key"),
        payload: b"",
    };
    let record = encode_addressed(h(1, 0), 1, keyed).unwrap();
    assert_eq!(
        decode(&record[..record.len() - 1]),
        Err(DecodeError::Truncated {
            need: record.len(),
            got: record.len() - 1
        })
    );
}

#[test]
fn every_cut_of_a_record_fails_without_panicking() {
    let keyed = Addressed {
        name: "n",
        key: Some(b"key"),
        payload: b"",
    };
    let records = [
        encode_addressed(h(1, 0), 1, keyed).unwrap(),
        encode_rejection(
            1,
            1,
            Rejected {
                code: 2,
                detail: b"",
            },
        ),
        encode_failure(1, 2, ErrorClass::Poison, ""),
        encode_degraded(1, 2, ErrorClass::Transient, "node-a", ""),
        encode_divergence(1, &no_detail(diverged(Some(&[1; 32]), Some(&[2; 32])))),
        encode_divergence(1, &no_detail(diverged(None, None))),
    ];
    for record in records {
        for cut in 0..record.len() {
            assert!(
                matches!(decode(&record[..cut]), Err(DecodeError::Truncated { .. })),
                "{record:?} cut at {cut}"
            );
        }
    }
    // A key length past the end of the record is truncated, not a panic.
    let mut record = encode_addressed(h(1, 0), 1, keyed).unwrap();
    record[25..29].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        decode(&record),
        Err(DecodeError::Truncated { .. })
    ));
}

#[test]
fn a_named_keyed_output_round_trips() {
    for key in [None, Some(&b""[..]), Some(&b"user-7"[..])] {
        let addressed = Addressed {
            name: "alerts",
            key,
            payload: b"payload",
        };
        let record = encode_addressed(h(3, 1), 2, addressed).unwrap();
        assert_eq!(
            decode(&record),
            Ok(Record::Output(
                Written {
                    header: h(3, 1),
                    epoch: 2
                },
                addressed
            ))
        );
    }
}

#[test]
fn output_names_are_checked() {
    for bad in ["", "Out", "a.b", &"a".repeat(65)] {
        let addressed = Addressed {
            name: bad,
            key: None,
            payload: b"",
        };
        assert_eq!(encode_addressed(h(0, 0), 1, addressed), Err(InvalidName));
    }
    let mut record = encode(h(0, 0), 1, b"");
    record[23] = b'O';
    assert_eq!(decode(&record), Err(DecodeError::InvalidName));
    record[22] = 0;
    assert_eq!(decode(&record), Err(DecodeError::InvalidName));
    let mut record = encode(h(0, 0), 1, b"");
    record[26] = 2;
    assert_eq!(decode(&record), Err(DecodeError::InvalidKeyFlag(2)));
}

#[test]
fn a_rejection_round_trips_and_holds_no_output() {
    let rejected = Rejected {
        code: 400,
        detail: b"bad input",
    };
    let record = encode_rejection(12, 3, rejected);
    let decoded = decode(&record).unwrap();
    let written = Written {
        header: h(12, 0),
        epoch: 3,
    };
    assert_eq!(decoded, Record::Rejection(written, rejected));
    assert_eq!(decoded.epoch(), 3);
    assert_eq!(decoded.written(), Some(written));
    assert_eq!(decoded.output(), None);
    assert!(!is_marker(&record));
    assert!(same_output(&record, &encode_rejection(12, 9, rejected)));
    assert!(!same_output(
        &record,
        &encode_rejection(
            12,
            3,
            Rejected {
                code: 401,
                ..rejected
            }
        )
    ));
    assert!(!same_output(&record, &encode(h(12, 0), 3, b"bad input")));
    let mut record = record;
    record[21] = 1;
    assert_eq!(decode(&record), Err(DecodeError::RejectionIndex(1)));
}

#[test]
fn a_reader_of_another_stream_gets_only_outputs_of_its_name() {
    let named = |name| Addressed {
        name,
        key: Some(b"k"),
        payload: b"p",
    };
    let ours = encode_addressed(h(1, 0), 1, named("out")).unwrap();
    assert_eq!(addressed_to(&ours, "out"), Ok(Some(named("out"))));
    assert!(!passed_over(&ours, "out"));
    let others = [
        encode_addressed(h(1, 1), 1, named("alerts")).unwrap(),
        encode_rejection(
            2,
            1,
            Rejected {
                code: 1,
                detail: b"",
            },
        ),
        encode_claim(1, "node"),
        encode_failure(1, 1, ErrorClass::Poison, "trap"),
        encode_degraded(1, 4, ErrorClass::Transient, "node-a", "lost the broker"),
    ];
    for record in &others {
        assert_eq!(addressed_to(record, "out"), Ok(None), "{record:?}");
        assert!(passed_over(record, "out"), "{record:?}");
    }
    assert!(!passed_over(&others[0], "alerts"));
    // A record of an unknown format reaches the reader rather than vanish.
    assert!(addressed_to(b"\x09", "out").is_err());
    assert!(!passed_over(b"\x09", "out"));
}

/// A rolling upgrade can add a class or reason. A reader of the stream
/// passes over such a marker by its kind, without reading the code.
#[test]
fn a_reader_of_another_stream_passes_over_a_marker_with_an_unknown_code() {
    let mut failure = encode_failure(1, 1, ErrorClass::Poison, "trap");
    failure[18] = 99;
    let mut needs = encode_needs_operator(1, 5, OperatorReason::SpecConflict, "input");
    needs[18..20].copy_from_slice(&99u16.to_be_bytes());
    let mut degraded = encode_degraded(1, 5, ErrorClass::Transient, "node-a", "lost the broker");
    degraded[18] = 99;
    for record in [failure, needs, degraded] {
        assert!(decode(&record).is_err(), "{record:?}");
        assert_eq!(addressed_to(&record, "out"), Ok(None), "{record:?}");
        assert!(passed_over(&record, "out"), "{record:?}");
        assert!(is_marker(&record), "{record:?}");
    }
}

#[test]
fn a_reader_of_another_stream_refuses_an_unknown_version_or_kind() {
    let mut kind = encode_claim(1, "node");
    kind[1] = 10;
    for record in [vec![9, 0], kind] {
        assert!(addressed_to(&record, "out").is_err(), "{record:?}");
        assert!(!passed_over(&record, "out"), "{record:?}");
        assert!(!is_marker(&record), "{record:?}");
    }
}

#[test]
fn an_unknown_version_or_kind_is_refused() {
    let mut record = encode(h(1, 0), 1, b"");
    record[0] = 5;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(5)));
    record[0] = 4;
    record[1] = 10;
    assert_eq!(decode(&record), Err(DecodeError::UnknownKind(10)));
    // A trim, degraded or divergence record is version 4 only.
    for kind in [7, 8, 9] {
        record[0] = 3;
        record[1] = kind;
        assert_eq!(decode(&record), Err(DecodeError::UnknownKind(kind)));
    }
}

#[test]
fn headers_order_by_input_then_index() {
    assert!(h(4, 9) < h(5, 0));
    assert!(h(5, 0) < h(5, 1));
}

#[test]
fn the_same_output_from_two_epochs_is_the_same() {
    assert!(same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 0), 9, b"a")
    ));
    assert!(!same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 0), 1, b"b")
    ));
    assert!(!same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 1), 1, b"a")
    ));
    assert!(!same_output(b"junk", &encode(h(3, 0), 1, b"a")));
}

fn batch(epoch: u64, inputs: &[u64]) -> Vec<Vec<u8>> {
    inputs
        .iter()
        .map(|&i| encode(h(i, 0), epoch, &i.to_be_bytes()))
        .collect()
}

fn refs(records: &[Vec<u8>]) -> Vec<&[u8]> {
    records.iter().map(Vec::as_slice).collect()
}

#[test]
fn an_earlier_owners_matching_records_are_skipped() {
    let ours = batch(7, &[10, 11]);
    let theirs = batch(3, &[10]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = batch(3, &[10, 11]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(2));
}

#[test]
fn our_own_records_from_a_lost_answer_are_skipped() {
    let ours = batch(7, &[10, 11]);
    assert_eq!(reconcile(7, &ours, &refs(&ours)), Reconciled::Skip(2));
}

#[test]
fn a_later_owners_records_stop_this_owner() {
    // Identical records, but from a later epoch: skipping them and writing on
    // would interleave two owners on the stream.
    let ours = batch(3, &[10, 11]);
    let theirs = batch(7, &[10]);
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
    let theirs = batch(7, &[10, 11, 12]);
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
}

#[test]
fn an_earlier_owner_past_our_batch_means_resume() {
    let ours = batch(7, &[10]);
    let theirs = batch(3, &[10, 11, 12]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Behind);
}

#[test]
fn different_records_are_a_divergence() {
    let ours = batch(7, &[10]);
    let theirs = batch(3, &[11]);
    let at = |found, ours| Reconciled::Diverged { found, ours };
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), at(0, Some(0)));
    // A record that does not decode takes the place of the next output.
    assert_eq!(reconcile(7, &ours, &[b"junk"]), at(0, Some(0)));
}

#[test]
fn a_divergence_names_the_records_that_disagree() {
    let ours = batch(7, &[10, 11, 12]);
    // A claim, then 10 as expected, then something else where 11 goes.
    let mut theirs = vec![encode_claim(3, ""), batch(3, &[10]).remove(0)];
    theirs.push(encode_degraded(3, 1, ErrorClass::Transient, "n", "x"));
    theirs.push(batch(3, &[99]).remove(0));
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Diverged {
            found: 3,
            ours: Some(1)
        }
    );
    let theirs = [batch(3, &[10]).remove(0), b"junk".to_vec()];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Diverged {
            found: 1,
            ours: Some(1)
        }
    );
    // The batch has nothing for a place past its end.
    let theirs = [b"junk".to_vec()];
    assert_eq!(
        reconcile(7, &batch(7, &[]), &refs(&theirs)),
        Reconciled::Diverged {
            found: 0,
            ours: None
        }
    );
    assert_eq!(
        reconcile(7, &ours, &[b"junk"]),
        Reconciled::Diverged {
            found: 0,
            ours: Some(0)
        }
    );
}

#[test]
fn a_later_claim_stops_this_owner() {
    let ours = batch(3, &[10]);
    let theirs = [encode_claim(7, "")];
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
}

#[test]
fn earlier_claims_are_passed_over() {
    let ours = batch(7, &[10, 11]);
    let mut theirs = vec![encode_claim(2, "")];
    theirs.extend(batch(3, &[10]));
    theirs.push(encode_claim(3, ""));
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = [encode_claim(7, "")];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(0));
}

#[test]
fn a_failure_round_trips_and_holds_no_output() {
    let record = encode_failure(4, 12, ErrorClass::NeedsOperator, "epochs went backwards");
    assert_eq!(&record[..2], [4, 5]);
    assert_eq!(
        decode(&record),
        Ok(Record::Failure {
            epoch: 4,
            spec_version: 12,
            class: ErrorClass::NeedsOperator,
            reason: b"epochs went backwards",
        })
    );
    assert_eq!(decode(&record).unwrap().epoch(), 4);
    assert_eq!(decode(&record).unwrap().output(), None);
    assert!(is_marker(&record));
    assert!(!same_output(&record, &record));
    assert!(decode(&record[..18]).is_err());
}

#[test]
fn a_failure_of_an_unknown_class_is_refused() {
    let mut record = encode_failure(4, 12, ErrorClass::Poison, "trap");
    record[18] = 9;
    assert_eq!(decode(&record), Err(DecodeError::UnknownClass(9)));
}

#[test]
fn a_needs_operator_record_round_trips_and_holds_no_output() {
    let record = encode_needs_operator(3, 40, OperatorReason::ModuleMissing, "module ab");
    assert_eq!(&record[..2], [4, 6]);
    let decoded = decode(&record).unwrap();
    assert_eq!(
        decoded,
        Record::NeedsOperator {
            epoch: 3,
            spec: 40,
            reason: OperatorReason::ModuleMissing,
            detail: b"module ab",
        }
    );
    assert_eq!(decoded.epoch(), 3);
    assert_eq!(decoded.written(), None);
    assert_eq!(
        decoded.needs_operator_under(40),
        Some(OperatorReason::ModuleMissing)
    );
    assert_eq!(decoded.needs_operator_under(41), None);
    assert!(is_marker(&record));
    assert!(passed_over(&record, DEFAULT_NAME));
    assert!(decode(&record[..19]).is_err());
    // A spec change is a new spec record, so it no longer matches.
    let mut unknown = record.clone();
    unknown[18..20].copy_from_slice(&99u16.to_be_bytes());
    assert_eq!(decode(&unknown), Err(DecodeError::UnknownReason(99)));
}

#[test]
fn a_build_before_version_4_refuses_the_new_kinds() {
    // Version 3 knew only outputs, claims and failures.
    for kind in [5, 6] {
        let mut record = encode_needs_operator(1, 1, OperatorReason::ModuleRefused, "");
        record[0] = 3;
        record[1] = kind;
        assert_eq!(decode(&record), Err(DecodeError::UnknownKind(kind)));
    }
}

#[test]
fn a_failure_ends_the_run_unless_a_later_owner_wrote() {
    let ours = batch(7, &[10]);
    let theirs = [encode_failure(5, 3, ErrorClass::Poison, "trap")];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Failed {
            spec_version: 3,
            class: ErrorClass::Poison,
            reason: "trap".to_owned()
        }
    );
    let theirs = [
        encode_failure(5, 3, ErrorClass::Poison, "trap"),
        encode_claim(8, ""),
    ];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 8 }
    );
}

#[test]
fn a_rejection_is_reconciled_like_an_output() {
    let rejected = Rejected {
        code: 7,
        detail: b"no",
    };
    let ours = vec![encode_rejection(10, 7, rejected)];
    let theirs = [encode_claim(3, ""), encode_rejection(10, 3, rejected)];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    // An output where we reject, or a rejection where we output, diverges.
    let theirs = batch(3, &[10]);
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Diverged {
            found: 0,
            ours: Some(0)
        }
    );
    let theirs = vec![encode_rejection(10, 3, rejected)];
    assert_eq!(
        reconcile(7, &batch(7, &[10]), &refs(&theirs)),
        Reconciled::Diverged {
            found: 0,
            ours: Some(0)
        }
    );
    // A rejection past our batch is a later input's, so resume.
    let theirs = [batch(3, &[10]).remove(0), encode_rejection(11, 3, rejected)];
    assert_eq!(
        reconcile(7, &batch(7, &[10]), &refs(&theirs)),
        Reconciled::Behind
    );
}

#[test]
fn a_needs_operator_record_names_its_spec_even_with_an_unknown_reason() {
    let mut record = encode_needs_operator(3, 42, OperatorReason::InputBehind, "x");
    assert_eq!(needs_operator_spec_of(&record), Some(42));
    record[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(decode(&record).is_err());
    assert_eq!(needs_operator_spec_of(&record), Some(42));
    assert_eq!(needs_operator_spec_of(&record[..17]), None);
    let failure = encode_failure(3, 42, ErrorClass::Poison, "x");
    assert_eq!(needs_operator_spec_of(&failure), None);
}

#[test]
fn a_degraded_record_round_trips_and_holds_no_output() {
    let record = encode_degraded(
        3,
        40,
        ErrorClass::Transient,
        "node-a",
        "the input stream closed",
    );
    let mut want = vec![4, 8];
    want.extend(3u64.to_be_bytes());
    want.extend(40u64.to_be_bytes());
    want.extend([0, 6]);
    want.extend(b"node-a");
    want.extend(b"the input stream closed");
    assert_eq!(record, want);
    let decoded = decode(&record).unwrap();
    assert_eq!(
        decoded,
        Record::Degraded {
            epoch: 3,
            spec: 40,
            class: ErrorClass::Transient,
            node: b"node-a",
            detail: b"the input stream closed",
        }
    );
    assert_eq!(decoded.epoch(), 3);
    assert_eq!(decoded.written(), None);
    assert_eq!(decoded.output(), None);
    assert_eq!(decoded.degraded_under(40), Some(ErrorClass::Transient));
    assert_eq!(decoded.degraded_under(41), None);
    assert_eq!(decoded.needs_operator_under(40), None);
    assert!(decoded.precedes_claims());
    assert!(!decode(&encode_claim(1, "n")).unwrap().precedes_claims());
    assert!(is_marker(&record));
    assert!(passed_over(&record, DEFAULT_NAME));
    assert!(!same_output(&record, &record));
    assert_eq!(degraded_spec_of(&record), Some(40));
    assert_eq!(
        degraded_spec_of(&encode_needs_operator(
            3,
            40,
            OperatorReason::ModuleMissing,
            ""
        )),
        None
    );
    assert!(decode(&record[..18]).is_err());
}

#[test]
fn a_degraded_record_reads_every_known_class_and_refuses_an_unknown_one() {
    for class in [
        ErrorClass::Transient,
        ErrorClass::NeedsOperator,
        ErrorClass::Poison,
    ] {
        let record = encode_degraded(1, 2, class, "node-a", "x");
        assert!(matches!(
            decode(&record),
            Ok(Record::Degraded { class: found, .. }) if found == class
        ));
    }
    let mut record = encode_degraded(1, 2, ErrorClass::Transient, "node-a", "x");
    record[18] = 9;
    assert_eq!(decode(&record), Err(DecodeError::UnknownClass(9)));
    // Its spec record is still read from the header, so it can be shown.
    assert_eq!(degraded_spec_of(&record), Some(2));
}

#[test]
fn a_degraded_record_is_passed_over_by_reconcile_unless_a_later_owner_wrote_it() {
    let ours = batch(7, &[10]);
    let degraded = |epoch| encode_degraded(epoch, 3, ErrorClass::Transient, "node-a", "x");
    let mut theirs = vec![encode_claim(3, ""), degraded(3)];
    theirs.extend(batch(3, &[10]));
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = [degraded(3)];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(0));
    let theirs = [degraded(8)];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 8 }
    );
}

#[test]
fn a_long_node_id_in_a_degraded_record_is_cut_at_a_character_boundary() {
    let node = "é".repeat(200);
    let record = encode_degraded(1, 2, ErrorClass::Transient, &node, "x");
    let Ok(Record::Degraded {
        node: found,
        detail,
        ..
    }) = decode(&record)
    else {
        panic!("a degraded record");
    };
    assert_eq!(found.len(), 254);
    assert!(std::str::from_utf8(found).is_ok());
    assert_eq!(detail, b"x");
    // A record cut inside the node id is truncated, not misread.
    assert!(matches!(
        decode(&record[..30]),
        Err(DecodeError::Truncated { .. })
    ));
}

const NO_ABI: Abi<'static> = Abi {
    contract: b"0.2.0",
    wasmtime: b"50.0.0",
    felixd: b"1.2.3",
};

fn diverged<'a>(expected: Option<&'a [u8; 32]>, actual: Option<&'a [u8; 32]>) -> Diverged<'a> {
    Diverged {
        spec: 40,
        header: h(7, 2),
        snapshot: Some(5),
        node: b"node-a",
        abi: NO_ABI,
        expected,
        actual,
        detail: b"replayed output differs",
    }
}

fn no_detail(found: Diverged<'_>) -> Diverged<'_> {
    Diverged {
        detail: b"",
        ..found
    }
}

#[test]
fn a_divergence_record_round_trips_and_holds_no_output() {
    let found = diverged(Some(&[1; 32]), Some(&[2; 32]));
    let record = encode_divergence(3, &found);
    let mut want = vec![4, 9];
    want.extend(3u64.to_be_bytes());
    want.extend(40u64.to_be_bytes());
    want.extend(7u64.to_be_bytes());
    want.extend(2u32.to_be_bytes());
    want.push(1);
    want.extend(5u64.to_be_bytes());
    want.push(32);
    want.extend([1; 32]);
    want.push(32);
    want.extend([2; 32]);
    for text in ["node-a", "0.2.0", "50.0.0", "1.2.3"] {
        want.push(text.len() as u8);
        want.extend(text.as_bytes());
    }
    want.extend(b"replayed output differs");
    assert_eq!(record, want);
    let decoded = decode(&record).unwrap();
    assert_eq!(decoded, Record::Divergence { epoch: 3, found });
    assert_eq!(decoded.epoch(), 3);
    assert_eq!(decoded.written(), None);
    assert_eq!(decoded.output(), None);
    assert_eq!(decoded.divergence_under(40), Some(&found));
    assert_eq!(decoded.divergence_under(41), None);
    assert_eq!(decoded.degraded_under(40), None);
    assert!(decoded.precedes_claims());
    assert!(is_marker(&record));
    assert!(passed_over(&record, DEFAULT_NAME));
    assert!(!same_output(&record, &record));
    assert_eq!(divergence_spec_of(&record), Some(40));
    assert_eq!(degraded_spec_of(&record), None);
    assert_eq!(needs_operator_spec_of(&record), None);
}

#[test]
fn a_divergence_record_may_hold_no_snapshot_and_no_digest() {
    let found = Diverged {
        snapshot: None,
        ..diverged(None, None)
    };
    let record = encode_divergence(3, &found);
    assert_eq!(decode(&record), Ok(Record::Divergence { epoch: 3, found }));
}

#[test]
fn a_divergence_record_with_a_field_this_build_cannot_read_is_still_passed_over() {
    let good = encode_divergence(3, &diverged(Some(&[1; 32]), None));
    // The snapshot flag, then a digest length, as a newer build might write.
    let mut flag = good.clone();
    flag[30] = 2;
    assert_eq!(decode(&flag), Err(DecodeError::InvalidSnapshotFlag(2)));
    let mut digest = good.clone();
    digest[39] = 16;
    assert_eq!(decode(&digest), Err(DecodeError::InvalidDigest(16)));
    // Cut after its spec record, nothing else of it can be read.
    let cut = good[..18].to_vec();
    assert!(decode(&cut).is_err());
    for record in [flag, digest, cut] {
        assert!(is_marker(&record), "{record:?}");
        assert!(passed_over(&record, DEFAULT_NAME), "{record:?}");
        assert_eq!(addressed_to(&record, DEFAULT_NAME), Ok(None));
        assert_eq!(divergence_spec_of(&record), Some(40), "{record:?}");
    }
    // Only the first 18 bytes name a spec record.
    assert_eq!(divergence_spec_of(&good[..17]), None);
}

#[test]
fn a_divergence_record_is_passed_over_by_reconcile_unless_a_later_owner_wrote_it() {
    let ours = batch(7, &[10]);
    let divergence = |epoch| encode_divergence(epoch, &diverged(None, None));
    let mut theirs = vec![encode_claim(3, ""), divergence(3)];
    theirs.extend(batch(3, &[10]));
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = [divergence(3)];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(0));
    let theirs = [divergence(8)];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 8 }
    );
}

#[test]
fn long_fields_in_a_divergence_record_are_cut_at_a_character_boundary() {
    let node = "é".repeat(200);
    let found = Diverged {
        node: node.as_bytes(),
        ..diverged(None, None)
    };
    let record = encode_divergence(1, &found);
    let Ok(Record::Divergence { found, .. }) = decode(&record) else {
        panic!("a divergence record");
    };
    assert_eq!(found.node.len(), 254);
    assert!(std::str::from_utf8(found.node).is_ok());
    assert_eq!(found.abi, NO_ABI);
    assert_eq!(found.detail, b"replayed output differs");
}

#[test]
fn the_digest_covers_what_is_compared_and_not_the_epoch() {
    let header = h(4, 1);
    let one = encode(header, 1, b"x");
    let other_owner = encode(header, 9, b"x");
    assert_eq!(compared_digest(&one), compared_digest(&other_owner));
    // The sha256 of the record with its epoch set to 0.
    let digest: [u8; 32] = sha2::Sha256::digest(encode(header, 0, b"x")).into();
    assert_eq!(compared_digest(&one), Some(digest));
    // What same_output compares, a digest tells apart.
    for different in [
        encode(header, 1, b"y"),
        encode(h(5, 1), 1, b"x"),
        encode(h(4, 2), 1, b"x"),
        encode_addressed(
            header,
            1,
            Addressed {
                name: "alerts",
                key: None,
                payload: b"x",
            },
        )
        .unwrap(),
        encode_addressed(
            header,
            1,
            Addressed {
                name: DEFAULT_NAME,
                key: Some(b"k"),
                payload: b"x",
            },
        )
        .unwrap(),
    ] {
        assert!(!same_output(&one, &different));
        assert_ne!(compared_digest(&one), compared_digest(&different));
    }
    let rejected = Rejected {
        code: 7,
        detail: b"no",
    };
    let rejection = encode_rejection(4, 1, rejected);
    assert_eq!(
        compared_digest(&rejection),
        compared_digest(&encode_rejection(4, 8, rejected))
    );
    assert_ne!(compared_digest(&rejection), compared_digest(&one));
    // Records that hold no output have none.
    assert_eq!(compared_digest(&encode_claim(1, "n")), None);
    assert_eq!(compared_digest(b"junk"), None);
}
