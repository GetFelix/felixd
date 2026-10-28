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
