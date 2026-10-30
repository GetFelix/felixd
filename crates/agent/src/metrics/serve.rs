//! The metrics endpoint: plain HTTP, `GET /metrics` and `GET /live`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long a client has to send its request line.
const READ_WITHIN: Duration = Duration::from_secs(5);

/// Turns metrics on for node `node` and serves them on `bind` from a task of
/// the current runtime. Returns the address bound, so a port of 0 can be
/// used.
pub async fn start(bind: SocketAddr, node: &str, max_workloads: usize) -> Result<SocketAddr> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind the metrics endpoint to {bind}"))?;
    let addr = listener.local_addr()?;
    super::enable(node, max_workloads);
    tracing::info!(%addr, "serving metrics");
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tokio::spawn(respond(stream));
                }
                Err(error) => {
                    tracing::warn!(%error, "metrics accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(addr)
}

async fn respond(mut stream: TcpStream) {
    let mut head = [0u8; 1024];
    let read = tokio::time::timeout(READ_WITHIN, stream.read(&mut head)).await;
    let Ok(Ok(read)) = read else { return };
    let request = String::from_utf8_lossy(&head[..read]);
    let mut line = request.lines().next().unwrap_or_default().split(' ');
    let (status, kind, body) = match (line.next(), line.next()) {
        (Some("GET"), Some("/metrics")) => match super::get() {
            Some(m) => (
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                m.render(),
            ),
            None => ("503 Service Unavailable", "text/plain", String::new()),
        },
        (Some("GET"), Some("/live")) => ("200 OK", "text/plain", "ok\n".to_owned()),
        (Some("GET"), _) => ("404 Not Found", "text/plain", "not found\n".to_owned()),
        _ => ("405 Method Not Allowed", "text/plain", String::new()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}
