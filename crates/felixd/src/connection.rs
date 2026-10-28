use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use felixd_agent::settings::Settings;

/// How to reach Felix. The environment variables are the ones `felixctl` reads.
#[derive(Args)]
pub(crate) struct Connection {
    /// Broker addresses, `host:port`. One is enough; the rest are discovered.
    #[arg(long, env = "FELIX_BROKERS", value_delimiter = ',', required = true)]
    brokers: Vec<SocketAddr>,
    /// The name on the brokers' certificate.
    #[arg(long, env = "FELIX_SERVER_NAME", default_value = "localhost")]
    server_name: String,
    #[arg(long, env = "FELIX_AUTH_TENANT")]
    tenant: String,
    #[arg(long, env = "FELIX_NAMESPACE", default_value = "default")]
    namespace: String,
    /// File holding the token for the brokers.
    #[arg(long, env = "FELIX_AUTH_TOKEN_FILE")]
    token_file: PathBuf,
    /// Trust the brokers' certificate from this PEM file.
    #[arg(long, env = "FELIX_CA_FILE")]
    ca_file: Option<PathBuf>,
    #[arg(long, env = "FELIX_CONTROLPLANE_URL")]
    controlplane_url: String,
    /// File holding the token for the control plane. Needed by `run`.
    #[arg(long, env = "FELIX_CONTROLPLANE_TOKEN_FILE")]
    controlplane_token_file: Option<PathBuf>,
}

impl Connection {
    pub(crate) fn settings(&self) -> Result<Settings> {
        let read = |path: &PathBuf| {
            std::fs::read_to_string(path)
                .map(|t| t.trim().to_owned())
                .with_context(|| format!("read {}", path.display()))
        };
        Ok(Settings {
            brokers: self.brokers.clone(),
            server_name: self.server_name.clone(),
            tenant: self.tenant.clone(),
            namespace: self.namespace.clone(),
            token: read(&self.token_file)?,
            ca_file: self.ca_file.clone(),
            controlplane_url: self.controlplane_url.clone(),
            controlplane_token: self
                .controlplane_token_file
                .as_ref()
                .map(read)
                .transpose()?,
        })
    }
}
