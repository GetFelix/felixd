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

fn held(role: Role) -> Vec<Policy> {
    role_policies(role, "t", "prod")
}

fn all() -> Vec<Policy> {
    policies("t", "prod")
}

fn group(user: &str, role: Role) -> Grouping {
    Grouping {
        user: user.to_owned(),
        role: role.subject("prod"),
    }
}

#[test]
fn an_audit_returns_only_the_missing_rules() {
    assert_eq!(audit("t", "prod", &[], &[], false).unwrap(), all());
    assert!(audit("t", "prod", &all(), &[], false).unwrap().is_empty());
    let missing = audit("t", "prod", &held(Role::Node), &[], false).unwrap();
    assert_eq!(missing.len(), all().len() - held(Role::Node).len());
}

#[test]
fn a_wider_rule_under_a_felixd_role_is_refused() {
    let mut rules = all();
    rules.push(Policy {
        subject: Role::Node.subject("prod"),
        object: "stream:t/prod.sys/*".to_owned(),
        action: "stream.publish".to_owned(),
    });
    assert!(audit("t", "prod", &rules, &[], false).is_err());
    // Rules of other subjects and of another namespace's roles are not ours.
    let mut other = all();
    other.push(Policy {
        subject: "role:ops".to_owned(),
        object: "stream:t/*".to_owned(),
        action: "stream.publish".to_owned(),
    });
    other.push(Policy {
        subject: Role::Node.subject("prod.x"),
        object: "stream:t/prod.x/*".to_owned(),
        action: "stream.publish".to_owned(),
    });
    assert!(audit("t", "prod", &other, &[], false).unwrap().is_empty());
    let mut unknown = all();
    unknown.push(Policy {
        subject: "role:felixd.prod.extra".to_owned(),
        object: "stream:t/prod/*".to_owned(),
        action: "stream.manage".to_owned(),
    });
    assert!(audit("t", "prod", &unknown, &[], false).is_err());
}

#[test]
fn a_principal_holding_two_roles_or_a_nested_role_is_refused() {
    let one = [group("alice", Role::Node), group("bob", Role::Admission)];
    assert!(audit("t", "prod", &all(), &one, false).is_ok());
    let both = [group("alice", Role::Node), group("alice", Role::Admission)];
    assert!(audit("t", "prod", &all(), &both, false).is_err());
    let nested = [Grouping {
        user: Role::Node.subject("prod"),
        role: "role:admin".to_owned(),
    }];
    assert!(audit("t", "prod", &all(), &nested, false).is_err());
    let unknown = [Grouping {
        user: "alice".to_owned(),
        role: "role:felixd.prod.extra".to_owned(),
    }];
    assert!(audit("t", "prod", &all(), &unknown, false).is_err());
}

/// Deleting history is off until the operator asks for it: no role holds
/// `stream.trim` in what `init` always writes, and the one opt-in rule is the
/// node's, over the workload namespace only. (Admission's `stream.manage`
/// includes it at the broker.)
#[test]
fn no_role_is_granted_trim_unless_asked() {
    for role in Role::ALL {
        assert!(
            role_policies(role, "t", "prod")
                .iter()
                .all(|p| p.action != "stream.trim"),
            "{role:?}"
        );
    }
    assert_eq!(
        trim_policy("t", "prod"),
        Policy {
            subject: Role::Node.subject("prod"),
            object: "stream:t/prod/*".to_owned(),
            action: "stream.trim".to_owned(),
        }
    );
}

/// `felixd init` writes what `audit` returns as missing: the trim rule only
/// with `--grant-trim`, and only once.
#[test]
fn init_writes_the_trim_grant_only_when_asked() {
    assert_eq!(audit("t", "prod", &[], &[], false).unwrap(), all());
    let mut with_trim = all();
    with_trim.push(trim_policy("t", "prod"));
    assert_eq!(audit("t", "prod", &[], &[], true).unwrap(), with_trim);
    assert_eq!(
        audit("t", "prod", &all(), &[], true).unwrap(),
        [trim_policy("t", "prod")]
    );
    let mut held = all();
    held.push(trim_policy("t", "prod"));
    assert!(audit("t", "prod", &held, &[], true).unwrap().is_empty());
}

/// Running `init` without the flag after it was run with it neither fails nor
/// takes the grant back.
#[test]
fn a_held_trim_grant_is_not_an_error_without_the_flag() {
    let mut held = all();
    held.push(trim_policy("t", "prod"));
    assert!(audit("t", "prod", &held, &[], false).unwrap().is_empty());
    // Only that rule: the same action on the system namespace is too wide.
    let mut wider = all();
    wider.push(Policy {
        object: "stream:t/prod.sys/*".to_owned(),
        ..trim_policy("t", "prod")
    });
    assert!(audit("t", "prod", &wider, &[], true).is_err());
    // And another role does not get it.
    let mut other = all();
    other.push(Policy {
        subject: Role::Submitter.subject("prod"),
        ..trim_policy("t", "prod")
    });
    assert!(audit("t", "prod", &other, &[], true).is_err());
}
