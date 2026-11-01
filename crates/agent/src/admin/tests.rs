use base64::Engine as _;

use super::*;

fn token(perms: &[&str]) -> String {
    let enc = |v: &Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string());
    format!(
        "{}.{}.sig",
        enc(&json!({"alg": "none"})),
        enc(&json!({"tid": "dev", "perms": perms}))
    )
}

#[test]
fn only_a_tenant_wide_view_is_enough_to_audit() {
    assert!(view_is_tenant_wide(
        &token(&["rbac.view:tenant:dev", "rbac.policy.manage:tenant:dev"]),
        "dev"
    ));
    for perms in [
        &["rbac.view:namespace:dev/prod"][..],
        &["rbac.view:tenant:other"],
        &["rbac.policy.manage:tenant:dev"],
        &[],
    ] {
        assert!(!view_is_tenant_wide(&token(perms), "dev"), "{perms:?}");
    }
    assert!(!view_is_tenant_wide("opaque", "dev"));
    assert!(!view_is_tenant_wide("a.!!!.c", "dev"));
}

/// Reads one request, headers and body, so closing the socket afterwards does
/// not reset a client that is still sending.
fn read_request(socket: &mut std::net::TcpStream) -> String {
    use std::io::Read;
    let mut request = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        let n = socket.read(&mut buf).unwrap();
        request.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&request);
        if let Some(end) = text.find("\r\n\r\n") {
            let length = text[..end]
                .to_ascii_lowercase()
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap_or(0usize);
            if request.len() >= end + 4 + length {
                return text.into_owned();
            }
        }
        assert!(n > 0, "the client closed before finishing its request");
    }
}

/// A control plane that answers each request with the next status in
/// `statuses`, and the request paths it saw.
fn control_plane(statuses: &'static [u16]) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let replies: Vec<(u16, &'static str)> = statuses.iter().map(|&s| (s, "{}")).collect();
    control_plane_replying(replies)
}

/// Like [`control_plane`], with a body for each answer.
fn control_plane_replying(
    replies: Vec<(u16, &'static str)>,
) -> (String, std::thread::JoinHandle<Vec<String>>) {
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        let mut paths = Vec::new();
        for (status, body) in replies {
            let (mut socket, _) = listener.accept().unwrap();
            let request = read_request(&mut socket);
            paths.push(request.split_whitespace().nth(1).unwrap().to_owned());
            write!(
                socket,
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
        paths
    });
    (url, served)
}

fn settings_at(url: String) -> Settings {
    Settings {
        brokers: vec![],
        server_name: "localhost".into(),
        tenant: "dev".into(),
        namespace: "ns".into(),
        token: crate::settings::Secret::new("t").into(),
        ca_file: None,
        controlplane_url: url,
        controlplane_token: Some(crate::settings::Secret::new("c").into()),
    }
}

#[tokio::test]
async fn a_namespace_without_old_system_streams_is_not_refused() {
    let (url, served) = control_plane(&[404, 404, 404]);
    refuse_legacy_streams(&settings_at(url)).await.unwrap();
    assert_eq!(
        served.join().unwrap(),
        [
            "/v1/tenants/dev/namespaces/ns/streams/sys.specs",
            "/v1/tenants/dev/namespaces/ns/streams/sys.intake",
            "/v1/tenants/dev/namespaces/ns/streams/sys.facts",
        ]
    );
}

#[tokio::test]
async fn an_old_system_stream_is_refused_by_name() {
    let (url, _served) = control_plane(&[404, 200, 404]);
    let error = refuse_legacy_streams(&settings_at(url)).await.unwrap_err();
    assert!(error.to_string().contains("ns/sys.intake"), "{error}");
}

#[tokio::test]
async fn a_reply_that_is_not_found_or_not_missing_is_an_error_not_absence() {
    let (url, _served) = control_plane(&[500]);
    let error = refuse_legacy_streams(&settings_at(url)).await.unwrap_err();
    assert!(error.to_string().contains("HTTP 500"), "{error}");
}

const STREAMS: &str = "/v1/tenants/dev/namespaces/ns/streams";

const REUSED: &str = r#"{"code":"conflict","message":"a stream named s was deleted on this cluster; finalize stream_incarnations before reusing the name"}"#;

fn kind() -> StreamKind {
    StreamKind {
        replication_factor: 1,
    }
}

#[tokio::test]
async fn a_409_for_a_stream_that_exists_is_success() {
    let (url, served) = control_plane(&[409, 200]);
    ensure_stream(&settings_at(url), "s", kind()).await.unwrap();
    assert_eq!(
        served.join().unwrap(),
        [STREAMS.to_owned(), format!("{STREAMS}/s")]
    );
}

#[tokio::test]
async fn a_409_that_left_no_stream_is_an_error_with_the_reason() {
    let (url, served) = control_plane_replying(vec![(409, REUSED), (404, "{}")]);
    let error = ensure_stream(&settings_at(url), "s", kind())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("refused with HTTP 409"), "{error}");
    assert!(error.contains("stream_incarnations"), "{error}");
    served.join().unwrap();
}

#[tokio::test]
async fn a_409_for_a_cache_is_read_back_too() {
    let (url, served) = control_plane_replying(vec![(409, REUSED), (404, "{}")]);
    let error = ensure_cache(&settings_at(url), "c", kind())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("namespaces/ns/caches/c"), "{error}");
    assert_eq!(
        served.join().unwrap(),
        [
            "/v1/tenants/dev/namespaces/ns/caches".to_owned(),
            "/v1/tenants/dev/namespaces/ns/caches/c".to_owned()
        ]
    );
}

#[tokio::test]
async fn a_409_whose_read_back_fails_is_an_error() {
    let (url, served) = control_plane(&[409, 503]);
    let error = ensure_stream(&settings_at(url), "s", kind())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("reading it back failed with HTTP 503"),
        "{error}"
    );
    served.join().unwrap();
}

#[tokio::test]
async fn a_namespace_that_exists_is_success() {
    let (url, served) = control_plane(&[409, 201]);
    ensure_namespace(&settings_at(url)).await.unwrap();
    assert_eq!(
        served.join().unwrap(),
        ["/v1/tenants/dev/namespaces", "/v1/tenants/dev/namespaces"]
    );
}
