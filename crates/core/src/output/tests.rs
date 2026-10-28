use super::*;

#[test]
fn a_record_round_trips() {
    let header = OutputHeader {
        input_offset: 0x0102_0304_0506_0708,
        index: 9,
    };
    let record = encode(header, b"payload");
    assert_eq!(decode(&record), Ok((header, &b"payload"[..])));
}

#[test]
fn the_layout_is_fixed() {
    let record = encode(
        OutputHeader {
            input_offset: 7,
            index: 2,
        },
        b"x",
    );
    assert_eq!(record, [1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, b'x']);
}

#[test]
fn an_empty_payload_is_allowed() {
    let header = OutputHeader {
        input_offset: 1,
        index: 0,
    };
    assert_eq!(decode(&encode(header, b"")), Ok((header, &b""[..])));
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[]),
        Err(DecodeError::Truncated { need: 13, got: 0 })
    );
    assert_eq!(
        decode(&[1, 0, 0]),
        Err(DecodeError::Truncated { need: 13, got: 3 })
    );
}

#[test]
fn an_unknown_version_is_refused() {
    let mut record = encode(
        OutputHeader {
            input_offset: 1,
            index: 0,
        },
        b"",
    );
    record[0] = 2;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(2)));
}

#[test]
fn headers_order_by_input_then_index() {
    let h = |input_offset, index| OutputHeader {
        input_offset,
        index,
    };
    assert!(h(4, 9) < h(5, 0));
    assert!(h(5, 0) < h(5, 1));
}
