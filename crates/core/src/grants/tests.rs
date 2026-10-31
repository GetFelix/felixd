use super::*;

fn holds(role: Role, action: &str, object: &str) -> bool {
    role_policies(role, "t", "prod")
        .iter()
        .any(|p| p.action == action && p.object == object)
}

#[test]
fn only_admission_writes_specs_and_creates_streams() {
    for role in Role::ALL {
        let admission = role == Role::Admission;
        assert_eq!(
            holds(role, "stream.publish", "stream:t/prod.sys/sys.specs"),
            admission,
            "{role:?}"
        );
        // A wildcard in the system namespace would reach sys.specs.
        assert!(
            !holds(role, "stream.publish", "stream:t/prod.sys/*"),
            "{role:?}"
        );
        assert_eq!(
            role_policies(role, "t", "prod")
                .iter()
                .any(|p| p.action.ends_with(".manage")),
            admission,
            "{role:?}"
        );
    }
    assert!(holds(Role::Admission, "stream.manage", "stream:t/prod/*"));
}

#[test]
fn a_submitter_publishes_only_requests() {
    let publishes: Vec<_> = role_policies(Role::Submitter, "t", "prod")
        .into_iter()
        .filter(|p| p.action == "stream.publish")
        .map(|p| p.object)
        .collect();
    assert_eq!(publishes, ["stream:t/prod.sys/sys.intake"]);
    assert!(holds(
        Role::Submitter,
        "cache.write",
        "cache:t/prod/modules"
    ));
}

#[test]
fn roles_are_per_namespace() {
    assert_eq!(Role::Node.subject("prod"), "role:felixd.prod.node");
    for policy in policies("t", "prod") {
        assert!(policy.subject.starts_with("role:felixd.prod."));
        assert!(
            policy.object.contains(":t/prod/") || policy.object.contains(":t/prod.sys/"),
            "{policy:?}"
        );
    }
}
