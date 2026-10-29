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
