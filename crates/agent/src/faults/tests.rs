use std::time::{Duration, Instant};

use super::*;

/// A stop that reaches the process but not this thread yet, as when the
/// kernel hands `kill -STOP` to another thread, must still hold this thread
/// until the process is continued.
#[tokio::test]
async fn a_freeze_holds_the_caller_until_the_process_is_continued() {
    let started = Instant::now();
    let pause = Duration::from_millis(300);
    let cont = std::thread::spawn(move || {
        std::thread::sleep(pause);
        std::process::Command::new("kill")
            .args(["-CONT", &std::process::id().to_string()])
            .status()
            .unwrap();
    });
    // The stop never lands on this thread.
    until_continued(|| {}).await;
    assert!(started.elapsed() >= pause, "returned before SIGCONT");
    cont.join().unwrap();
}
