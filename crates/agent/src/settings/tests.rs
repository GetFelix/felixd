use super::*;

const TOKEN: &str = "eyJhbGciOiJFZERTQSJ9.c2VjcmV0LWNsYWltcw.c2lnbmF0dXJlLWJ5dGVz";
const CP_TOKEN: &str = "controlplane-secret-value";

fn settings() -> Settings {
    Settings {
        brokers: vec!["127.0.0.1:5000".parse().unwrap()],
        server_name: "localhost".into(),
        tenant: "t".into(),
        namespace: "default".into(),
        token: Secret::new(TOKEN),
        ca_file: None,
        controlplane_url: "http://127.0.0.1:8443".into(),
        controlplane_token: Some(Secret::new(CP_TOKEN)),
    }
}

#[test]
fn formatting_settings_never_shows_a_token() {
    let s = settings();
    for text in [
        format!("{s:?}"),
        format!("{s:#?}"),
        format!("{}", s.token),
        format!("{:?}", s.controlplane_token),
        anyhow::anyhow!("connect with {}", s.token).to_string(),
    ] {
        assert!(!text.contains(TOKEN), "{text}");
        assert!(!text.contains(CP_TOKEN), "{text}");
        for part in TOKEN.split('.') {
            assert!(!text.contains(part), "{text}");
        }
    }
}

#[test]
fn expose_gives_the_value() {
    assert_eq!(settings().token.expose(), TOKEN);
}
