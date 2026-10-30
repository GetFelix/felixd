use clap::Parser;

use super::{Cli, Target, parse_target};

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

#[test]
fn replay_takes_an_offset_or_a_time() {
    assert_eq!(parse_target("42"), Ok(Target::Offset(42)));
    assert_eq!(
        parse_target("1970-01-01T00:00:01.5Z"),
        Ok(Target::Time(1_500_000))
    );
    assert_eq!(
        parse_target("2026-10-29T14:02:00-07:00"),
        Ok(Target::Time(1_793_307_720_000_000))
    );
    assert!(parse_target("14:02").is_err());
    assert!(parse_target("1969-12-31T23:59:59Z").is_err());
}

#[test]
fn metrics_are_off_unless_an_address_is_given() {
    let parse = |extra: &[&str]| {
        let mut args = vec![
            "felixd",
            "agent",
            "--brokers=127.0.0.1:5000",
            "--tenant=t",
            "--token-file=t.jwt",
            "--controlplane-url=http://127.0.0.1",
        ];
        args.extend(extra);
        Cli::try_parse_from(args)
    };
    assert!(parse(&[]).is_ok());
    assert!(parse(&["--metrics-bind", "127.0.0.1:9464"]).is_ok());
    assert!(parse(&["--metrics-bind", "not an address"]).is_err());
    // A limit with no endpoint is a mistake worth saying so.
    assert!(parse(&["--metrics-max-workloads", "5"]).is_err());
}
