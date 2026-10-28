use clap::Parser;

use super::Cli;

#[test]
fn a_lease_ttl_below_two_seconds_is_refused() {
    let parse = |ttl: &str| {
        Cli::try_parse_from([
            "felixd",
            "agent",
            "--brokers=127.0.0.1:5000",
            "--tenant=t",
            "--token-file=t.jwt",
            "--controlplane-url=http://127.0.0.1",
            "--lease-ttl-ms",
            ttl,
        ])
    };
    assert!(parse("1999").is_err());
    assert!(parse("2000").is_ok());
}
