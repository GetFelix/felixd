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

/// A control plane that answers each request with the next status in
/// `statuses`, and the request paths it saw.
fn control_plane(statuses: &'static [u16]) -> (String, std::thread::JoinHandle<Vec<String>>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        let mut paths = Vec::new();
        for status in statuses {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let n = socket.read(&mut buf).unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            paths.push(request.split_whitespace().nth(1).unwrap().to_owned());
            write!(
                socket,
                "HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
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
