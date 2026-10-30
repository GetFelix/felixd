//! The metrics endpoint: plain HTTP, `GET /metrics` and `GET /live`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// How long a client has to send its request, and then to take the reply.
const CLIENT_WITHIN: Duration = Duration::from_secs(5);

/// The most connections served at once. More are closed, so a client that
/// holds its connection cannot pin tasks and descriptors without bound.
pub(super) const MAX_CONNECTIONS: usize = 64;

/// The most of a request head that is read.
const MAX_HEAD: usize = 8 * 1024;

/// Turns metrics on for node `node` and serves them on `bind` from a task of
/// the current runtime. Returns the address bound, so a port of 0 can be
/// used. A bind failure is returned, so the agent does not start.
pub async fn start(bind: SocketAddr, node: &str, max_workloads: usize) -> Result<SocketAddr> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind the metrics endpoint to {bind}"))?;
    let addr = listener.local_addr()?;
    super::enable(node, max_workloads);
    tracing::info!(%addr, "serving metrics");
    let room = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    // Dropping the stream closes it.
                    if let Ok(permit) = room.clone().try_acquire_owned() {
                        tokio::spawn(async move {
                            let _ = tokio::time::timeout(CLIENT_WITHIN * 2, respond(stream)).await;
                            drop(permit);
                        });
                    }
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

/// Reads up to the end of the request head. `None` if it is too long, ends
/// early or is too slow.
async fn read_head(stream: &mut TcpStream) -> Option<String> {
    let read = async {
        let mut head = Vec::new();
        let mut chunk = [0u8; 1024];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            if head.len() >= MAX_HEAD {
                return None;
            }
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => head.extend_from_slice(&chunk[..n]),
            }
        }
        Some(String::from_utf8_lossy(&head).into_owned())
    };
    tokio::time::timeout(CLIENT_WITHIN, read).await.ok()?
}

async fn respond(mut stream: TcpStream) {
    let Some(request) = read_head(&mut stream).await else {
        return;
    };
    let mut line = request.lines().next().unwrap_or_default().split(' ');
    let (method, target) = (line.next(), line.next().unwrap_or_default());
    let path = target.split('?').next().unwrap_or_default();
    let (status, kind, body) = match (method, path) {
        (Some("GET"), "/metrics") => match super::get() {
            Some(m) => (
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                m.render(),
            ),
            None => ("503 Service Unavailable", "text/plain", String::new()),
        },
        (Some("GET"), "/live") => ("200 OK", "text/plain", "ok\n".to_owned()),
        (Some("GET"), _) => ("404 Not Found", "text/plain", "not found\n".to_owned()),
        _ => ("405 Method Not Allowed", "text/plain", String::new()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let write = async {
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    };
    let _ = tokio::time::timeout(CLIENT_WITHIN, write).await;
}
