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
    pub token: String,
    /// Extra trust roots for the brokers' certificate. The platform roots are
    /// used when this is `None`.
    pub ca_file: Option<PathBuf>,
    pub controlplane_url: String,
    /// Token for the control plane. Needed only to create streams and caches.
    pub controlplane_token: Option<String>,
}
