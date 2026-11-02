use super::*;

fn reads(inputs: &[&str], floors: &[u64]) -> Known {
    Known {
        inputs: Some(inputs.iter().map(|s| (*s).to_owned()).collect()),
        floors: Some(floors.to_vec()),
    }
}

/// Without a trim record there is no evidence of how far a reader has got.
fn untrimmed(inputs: &[&str]) -> Known {
    Known {
        inputs: Some(inputs.iter().map(|s| (*s).to_owned()).collect()),
        floors: None,
    }
}

#[test]
fn a_shared_stream_is_deleted_below_the_lowest_floor() {
    let workloads = [
        reads(&["in"], &[40]),
        reads(&["in"], &[12]),
        reads(&["in"], &[90]),
    ];
    assert_eq!(lowest_floor("in", &workloads), Some(12));
}

#[test]
fn a_reader_of_another_stream_does_not_hold_this_one_back() {
    let workloads = [reads(&["in"], &[40]), reads(&["other"], &[3])];
    assert_eq!(lowest_floor("in", &workloads), Some(40));
    assert_eq!(lowest_floor("nobody", &workloads), None);
}

#[test]
fn a_reader_with_no_trim_record_blocks_deletion() {
    let workloads = [reads(&["in"], &[40]), untrimmed(&["in"])];
    assert_eq!(lowest_floor("in", &workloads), Some(0));
}

/// A spec that does not decode may read anything, so it holds every stream.
#[test]
fn a_spec_that_does_not_decode_blocks_every_stream() {
    let unknown = Known {
        inputs: None,
        floors: None,
    };
    let workloads = [reads(&["in"], &[40]), unknown];
    assert_eq!(lowest_floor("in", &workloads), Some(0));
    assert_eq!(lowest_floor("other", &workloads), Some(0));
}

/// A record that names fewer floors than the spec has inputs does not match
/// the spec, so it shows nothing.
#[test]
fn floors_that_do_not_match_the_inputs_count_as_zero() {
    let workloads = [reads(&["a", "b"], &[7])];
    assert_eq!(lowest_floor("a", &workloads), Some(0));
    assert_eq!(lowest_floor("b", &workloads), Some(0));
}

#[test]
fn the_floor_is_found_by_the_inputs_position() {
    let workloads = [reads(&["a", "b"], &[7, 30])];
    assert_eq!(lowest_floor("a", &workloads), Some(7));
    assert_eq!(lowest_floor("b", &workloads), Some(30));
}

#[test]
fn a_stream_listed_twice_takes_the_lower_floor() {
    let workloads = [reads(&["a", "a"], &[9, 4])];
    assert_eq!(lowest_floor("a", &workloads), Some(4));
}

#[test]
fn a_workload_output_with_no_readers_goes_as_far_as_its_own_need() {
    assert_eq!(output_floor(50, "wl.a.out", &[]), 50);
    assert_eq!(output_floor(50, "wl.a.out", &[reads(&["in"], &[3])]), 50);
}

#[test]
fn a_workload_output_goes_no_further_than_a_reader_needs() {
    let workloads = [reads(&["wl.a.out"], &[20]), reads(&["wl.a.out"], &[35])];
    assert_eq!(output_floor(50, "wl.a.out", &workloads), 20);
}

#[test]
fn a_workload_output_goes_no_further_than_its_own_need() {
    let workloads = [reads(&["wl.a.out"], &[80])];
    assert_eq!(output_floor(50, "wl.a.out", &workloads), 50);
}

#[test]
fn a_downstream_with_no_trim_record_keeps_the_output() {
    let workloads = [untrimmed(&["wl.a.out"])];
    assert_eq!(output_floor(50, "wl.a.out", &workloads), 0);
}

#[test]
fn only_plain_streams_are_deleted_on_the_readers_word() {
    assert!(deletable_input("orders"));
    assert!(!deletable_input(crate::names::FACTS));
    assert!(!deletable_input(crate::names::SPECS));
    assert!(!deletable_input("wl.a.out"));
    assert!(!deletable_input("wl.a.merge"));
}
