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
    assert_eq!(audit("t", "prod", &[], &[]).unwrap(), all());
    assert!(audit("t", "prod", &all(), &[]).unwrap().is_empty());
    let missing = audit("t", "prod", &held(Role::Node), &[]).unwrap();
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
    assert!(audit("t", "prod", &rules, &[]).is_err());
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
    assert!(audit("t", "prod", &other, &[]).unwrap().is_empty());
    let mut unknown = all();
    unknown.push(Policy {
        subject: "role:felixd.prod.extra".to_owned(),
        object: "stream:t/prod/*".to_owned(),
        action: "stream.manage".to_owned(),
    });
    assert!(audit("t", "prod", &unknown, &[]).is_err());
}

#[test]
fn a_principal_holding_two_roles_or_a_nested_role_is_refused() {
    let one = [group("alice", Role::Node), group("bob", Role::Admission)];
    assert!(audit("t", "prod", &all(), &one).is_ok());
    let both = [group("alice", Role::Node), group("alice", Role::Admission)];
    assert!(audit("t", "prod", &all(), &both).is_err());
    let nested = [Grouping {
        user: Role::Node.subject("prod"),
        role: "role:admin".to_owned(),
    }];
    assert!(audit("t", "prod", &all(), &nested).is_err());
    let unknown = [Grouping {
        user: "alice".to_owned(),
        role: "role:felixd.prod.extra".to_owned(),
    }];
    assert!(audit("t", "prod", &all(), &unknown).is_err());
}

/// `keep_snapshots` trims `.snap` and the merge log from the node that holds
/// the workload, so a node is granted `stream.trim` on the workload streams
/// and no role is granted it on the system ones. Admission's `stream.manage`
/// includes it at the broker.
#[test]
fn a_node_may_trim_workload_streams_only() {
    assert!(holds(Role::Node, "stream.trim", "stream:t/prod/*"));
    for role in [Role::Admission, Role::Submitter] {
        assert!(!holds(role, "stream.trim", "stream:t/prod/*"), "{role:?}");
    }
    assert!(!holds(Role::Node, "stream.trim", "stream:t/prod.sys/*"));
}

/// The scheduler's holder deletes the facts it no longer reads, so nodes may
/// trim `sys.facts` and no other system stream, by exact name: Felix has no
/// narrower grant than the name, and a wildcard would reach `sys.specs`.
#[test]
fn a_node_may_trim_sys_facts_and_no_other_system_stream() {
    assert!(holds(
        Role::Node,
        "stream.trim",
        "stream:t/prod.sys/sys.facts"
    ));
    for role in [Role::Admission, Role::Submitter] {
        assert!(
            !holds(role, "stream.trim", "stream:t/prod.sys/sys.facts"),
            "{role:?}"
        );
    }
    let trims: Vec<_> = role_policies(Role::Node, "t", "prod")
        .into_iter()
        .filter(|p| p.action == "stream.trim" && p.object.contains("prod.sys"))
        .map(|p| p.object)
        .collect();
    assert_eq!(trims, ["stream:t/prod.sys/sys.facts"]);
}

/// A namespace set up before nodes could trim `sys.facts` gets the grant when
/// `init` runs again, and nothing else changes.
#[test]
fn init_adds_the_facts_trim_grant_to_a_namespace_set_up_without_it() {
    let facts = "stream:t/prod.sys/sys.facts";
    let held: Vec<Policy> = policies("t", "prod")
        .into_iter()
        .filter(|p| !(p.action == "stream.trim" && p.object == facts))
        .collect();
    let missing = audit("t", "prod", &held, &[]).unwrap();
    assert_eq!(
        missing,
        vec![Policy {
            subject: Role::Node.subject("prod"),
            object: facts.to_owned(),
            action: "stream.trim".to_owned(),
        }]
    );
}

/// `felixd init` writes what `audit` returns as missing, so a namespace set up
/// before `stream.trim` existed gets it for the node role when `init` runs
/// again, and nothing else changes.
#[test]
fn init_adds_the_trim_grant_to_a_namespace_set_up_without_it() {
    let held: Vec<Policy> = policies("t", "prod")
        .into_iter()
        .filter(|p| p.action != "stream.trim")
        .collect();
    let missing = audit("t", "prod", &held, &[]).unwrap();
    assert!(missing.iter().all(|p| p.action == "stream.trim"));
    assert!(missing.contains(&Policy {
        subject: Role::Node.subject("prod"),
        object: "stream:t/prod/*".to_owned(),
        action: "stream.trim".to_owned(),
    }));
}
