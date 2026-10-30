use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Where Felix is and who felixd is talking to it as.
#[derive(Clone, Debug)]
pub struct Settings {
    pub brokers: Vec<SocketAddr>,
    /// The name on the brokers' certificate.
    pub server_name: String,
    pub tenant: String,
    pub namespace: String,
    /// Token for the brokers.
    pub token: Secret,
    /// Extra trust roots for the brokers' certificate. The platform roots are
    /// used when this is `None`.
    pub ca_file: Option<PathBuf>,
    pub controlplane_url: String,
    /// Token for the control plane. Needed only to create streams and caches.
    pub controlplane_token: Option<Secret>,
}

/// A credential. Its `Debug` and `Display` print a placeholder, so a secret
/// cannot reach a log line, an error or a panic message by formatting. The
/// value is read only with [`Secret::expose`], where it is handed to Felix.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
