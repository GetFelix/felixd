use felix_client::TokenProvider as _;

use super::*;

const TOKEN: &str = "eyJhbGciOiJFZERTQSJ9.c2VjcmV0LWNsYWltcw.c2lnbmF0dXJlLWJ5dGVz";
const CP_TOKEN: &str = "controlplane-secret-value";

fn settings() -> Settings {
    Settings {
        brokers: vec!["127.0.0.1:5000".parse().unwrap()],
        server_name: "localhost".into(),
        tenant: "t".into(),
        namespace: "default".into(),
        token: Secret::new(TOKEN).into(),
        ca_file: None,
        controlplane_url: "http://127.0.0.1:8443".into(),
        controlplane_token: Some(Secret::new(CP_TOKEN).into()),
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
    assert_eq!(settings().token.current().expose(), TOKEN);
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("felixd-settings-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("token")
}

#[test]
fn a_token_file_is_read_again_on_each_use() {
    let path = scratch("rotate");
    std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
    let source = TokenSource::from_file(&path).unwrap();
    assert_eq!(source.current().expose(), TOKEN);
    std::fs::write(&path, CP_TOKEN).unwrap();
    assert_eq!(source.current().expose(), CP_TOKEN);
}

#[test]
fn a_file_missing_or_half_written_keeps_the_last_token() {
    let path = scratch("gap");
    std::fs::write(&path, TOKEN).unwrap();
    let source = TokenSource::from_file(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(source.current().expose(), TOKEN);
    std::fs::write(&path, "").unwrap();
    assert_eq!(source.current().expose(), TOKEN);
    std::fs::write(&path, "two words").unwrap();
    assert_eq!(source.current().expose(), TOKEN);
    std::fs::write(&path, CP_TOKEN).unwrap();
    assert_eq!(source.current().expose(), CP_TOKEN);
}

#[test]
fn a_file_unreadable_at_startup_is_an_error() {
    let path = scratch("absent");
    assert!(TokenSource::from_file(&path).is_err());
}

#[test]
fn a_token_source_never_shows_its_token() {
    let path = scratch("redact");
    std::fs::write(&path, TOKEN).unwrap();
    let source = TokenSource::from_file(&path).unwrap();
    assert!(!format!("{source:?}{source}").contains("eyJ"));
}

#[tokio::test]
async fn the_client_is_given_the_current_token() {
    let path = scratch("provider");
    std::fs::write(&path, TOKEN).unwrap();
    let source = TokenSource::from_file(&path).unwrap();
    assert_eq!(source.token().await.unwrap(), TOKEN);
    std::fs::write(&path, CP_TOKEN).unwrap();
    assert_eq!(source.token().await.unwrap(), CP_TOKEN);
}

// Every token file felixd reads, the admission one included, is a
// `TokenSource`: `from_file` is the only way the binary builds one.
#[test]
fn an_empty_file_at_startup_is_refused() {
    let path = scratch("empty");
    std::fs::write(&path, "").unwrap();
    assert!(TokenSource::from_file(&path).is_err());
}
