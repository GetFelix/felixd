use super::*;

#[tokio::test]
async fn an_error_without_a_not_found_code_is_returned_at_once() {
    // The text matches what a not-ready resource says, but nothing typed it so.
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        until_ready(|| async { Err::<(), _>(anyhow::anyhow!("module not found")) }),
    )
    .await
    .expect("until_ready kept retrying an unrelated error");
    assert!(result.is_err());
}

#[tokio::test]
async fn success_is_returned_at_once() {
    assert_eq!(until_ready(|| async { Ok(7) }).await.unwrap(), 7);
}

#[tokio::test(start_paused = true)]
async fn a_page_short_of_the_commit_point_is_read_again() {
    let mut calls = 0;
    let (records, after) = advancing("s", 4, || {
        calls += 1;
        let page = if calls < 3 {
            (vec![], 4)
        } else {
            (vec![4u64], 5)
        };
        async move { Ok(page) }
    })
    .await
    .unwrap();
    assert_eq!((records, after, calls), (vec![4], 5, 3));
}

#[tokio::test(start_paused = true)]
async fn a_page_that_never_advances_is_an_error() {
    let result = advancing("s", 4, || async { Ok((Vec::<u64>::new(), 4)) }).await;
    assert!(result.is_err());
}

#[test]
fn a_command_that_reads_trimmed_history_names_the_stream_and_where_it_starts() {
    let trimmed = Trimmed {
        stream: "wl.a.merge".into(),
        requested: 0,
        oldest: 37,
    };
    let message = Trimmed::explained(trimmed.into(), "felixd check").to_string();
    assert_eq!(
        message,
        "felixd check needs wl.a.merge from offset 0, but Felix holds it only from offset 37: \
         the history before was trimmed; `felixd check --from-snapshot` starts after a snapshot instead"
    );
    let replay = Trimmed::explained(
        Trimmed {
            stream: "wl.a.merge".into(),
            requested: 0,
            oldest: 37,
        }
        .into(),
        "felixd replay",
    );
    assert!(!replay.to_string().contains("--from-snapshot"));
}

#[test]
fn any_other_error_is_left_as_it_was() {
    let message = Trimmed::explained(anyhow::anyhow!("connection reset"), "felixd check");
    assert_eq!(message.to_string(), "connection reset");
}

fn trimmed_at(oldest: u64) -> anyhow::Error {
    Trimmed {
        stream: "s".into(),
        requested: 0,
        oldest,
    }
    .into()
}

/// A trim can land between a read that is refused and the read from the
/// offset it named, so the walk follows the floor up.
#[tokio::test]
async fn a_floor_that_rises_between_reads_is_followed() {
    let mut floors = vec![10, 14].into_iter();
    let mut asked = Vec::new();
    let (records, ended) = read_held(0, 20, |from| {
        asked.push(from);
        let floor = floors.next().unwrap_or(14);
        async move {
            if from < floor {
                Err(trimmed_at(floor))
            } else {
                Ok(vec![from])
            }
        }
    })
    .await
    .unwrap();
    assert_eq!((records, ended, asked), (vec![14], true, vec![0, 10, 14]));
}

#[tokio::test]
async fn a_floor_past_the_end_leaves_nothing_to_read() {
    let (records, ended) = read_held(0, 8, |_| async { Err::<Vec<u64>, _>(trimmed_at(12)) })
        .await
        .unwrap();
    assert_eq!((records, ended), (vec![], true));
}

#[tokio::test]
async fn a_read_above_the_floor_ends_only_at_the_start_of_the_stream() {
    let (_, ended) = read_held(5, 9, |_| async { Ok(vec![1u64]) }).await.unwrap();
    assert!(!ended);
    let (_, ended) = read_held(0, 9, |_| async { Ok(vec![1u64]) }).await.unwrap();
    assert!(ended);
}

/// A floor that keeps rising is given up on after a few rounds, with the
/// trim reported.
#[tokio::test]
async fn a_floor_that_never_settles_is_an_error() {
    let mut floor = 0;
    let result = read_held(0, 1000, |_| {
        floor += 10;
        let at = floor;
        async move { Err::<Vec<u64>, _>(trimmed_at(at)) }
    })
    .await;
    assert!(Trimmed::of(&result.unwrap_err()).is_some());
}
