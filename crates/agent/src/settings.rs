use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use felix_client::{Seed, TokenFuture, TokenProvider};

/// Where Felix is and who felixd is talking to it as.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Broker addresses, each an IP or a `host:port` name. Names are resolved
    /// when the client connects and again when no known address answers.
    pub brokers: Vec<Seed>,
    /// The name on the brokers' certificate.
    pub server_name: String,
    pub tenant: String,
    pub namespace: String,
    /// Token for the brokers.
    pub token: TokenSource,
    /// Extra trust roots for the brokers' certificate. The platform roots are
    /// used when this is `None`.
    pub ca_file: Option<PathBuf>,
    pub controlplane_url: String,
    /// Token for the control plane. Needed only to create streams and caches.
    pub controlplane_token: Option<TokenSource>,
}

/// A credential. Its `Debug` and `Display` print a placeholder, so a secret
/// cannot reach a log line, an error or a panic message by formatting. The
/// value is read only with [`Secret::expose`], where it is handed to Felix.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(redacted)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(redacted)")
    }
}

/// Where a credential comes from. A file-backed source is read again on every
/// use, so a token rotated in place is picked up before the old one expires.
/// A file that is missing, empty or unreadable at that moment keeps the last
/// good token and is tried again next time.
#[derive(Clone)]
pub struct TokenSource(Arc<Source>);

struct Source {
    path: Option<PathBuf>,
    last: Mutex<Secret>,
}

impl TokenSource {
    /// A token that never changes.
    pub fn fixed(token: Secret) -> TokenSource {
        TokenSource(Arc::new(Source {
            path: None,
            last: Mutex::new(token),
        }))
    }

    /// The token in `path`. The file must be readable now; later reads that
    /// fail keep the token read here.
    pub fn from_file(path: impl Into<PathBuf>) -> anyhow::Result<TokenSource> {
        use anyhow::Context as _;
        let path = path.into();
        let token = read_token(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(TokenSource(Arc::new(Source {
            path: Some(path),
            last: Mutex::new(token),
        })))
    }

    /// The token to present now.
    pub fn current(&self) -> Secret {
        let mut last = self.0.last.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(path) = &self.0.path {
            match read_token(path) {
                Ok(token) => *last = token,
                // The error names the path and the failure, never the content.
                Err(e) => tracing::warn!(
                    path = %path.display(),
                    "token file not read, keeping the last token: {e}"
                ),
            }
        }
        last.clone()
    }
}

/// A token file's content. A file caught empty or mid-write (whitespace
/// inside) is an error, so the caller keeps the previous token.
fn read_token(path: &std::path::Path) -> std::io::Result<Secret> {
    let text = std::fs::read_to_string(path)?;
    let token = text.trim();
    if token.is_empty() || token.contains(char::is_whitespace) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the file is empty or not a single token",
        ));
    }
    Ok(Secret::new(token))
}

impl From<Secret> for TokenSource {
    fn from(token: Secret) -> TokenSource {
        TokenSource::fixed(token)
    }
}

impl fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenSource(redacted)")
    }
}

impl fmt::Display for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(redacted)")
    }
}

impl TokenProvider for TokenSource {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.current().expose().to_owned();
        Box::pin(async move { Ok(token) })
    }
}

#[cfg(test)]
mod tests;
