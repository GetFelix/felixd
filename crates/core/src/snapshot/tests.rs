use super::*;

const HEADER: SnapshotHeader = SnapshotHeader {
    input_offset: 1,
    output_tail: 2,
};

#[test]
fn a_record_round_trips() {
    let header = SnapshotHeader {
        input_offset: 41,
        output_tail: 1_000_000_000_000,
    };
    let record = encode(header, 7, &[1, 2, 3]);
    assert_eq!(
        decode(&record),
        Ok((Taken { header, epoch: 7 }, &[1u8, 2, 3][..]))
    );
}

#[test]
fn the_layout_is_fixed() {
    assert_eq!(
        encode(HEADER, 3, b"s"),
        [
            2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, b's'
        ]
    );
}

#[test]
fn a_version_1_record_reads_as_epoch_0() {
    let record = [1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, b's'];
    assert_eq!(
        decode(&record),
        Ok((
            Taken {
                header: HEADER,
                epoch: 0
            },
            &b"s"[..]
        ))
    );
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[2; 10]),
        Err(DecodeError::Truncated { need: 25, got: 10 })
    );
    assert_eq!(
        decode(&[1; 10]),
        Err(DecodeError::Truncated { need: 17, got: 10 })
    );
}

#[test]
fn an_unknown_version_is_refused() {
    let mut record = encode(HEADER, 1, b"");
    record[0] = 0;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(0)));
}

#[test]
fn an_input_in_a_new_interval_starts_one() {
    // 100 ms intervals: 0..100_000 us, 100_000..200_000 us, ...
    assert!(!starts_interval(100, Some(10_000), 99_999));
    assert!(starts_interval(100, Some(99_999), 100_000));
    assert!(starts_interval(100, Some(50_000), 750_000));
    assert!(!starts_interval(100, Some(150_000), 199_999));
    assert!(!starts_interval(100, None, 500_000));
}

#[test]
fn a_clock_that_stepped_back_starts_nothing() {
    assert!(!starts_interval(100, Some(350_000), 120_000));
}
