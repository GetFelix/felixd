use base64::Engine as _;

use super::*;

fn token(perms: &[&str]) -> String {
    let enc = |v: &Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string());
    format!(
        "{}.{}.sig",
        enc(&json!({"alg": "none"})),
        enc(&json!({"tid": "dev", "perms": perms}))
    )
}

#[test]
fn only_a_tenant_wide_view_is_enough_to_audit() {
    assert!(view_is_tenant_wide(
        &token(&["rbac.view:tenant:dev", "rbac.policy.manage:tenant:dev"]),
        "dev"
    ));
    for perms in [
        &["rbac.view:namespace:dev/prod"][..],
        &["rbac.view:tenant:other"],
        &["rbac.policy.manage:tenant:dev"],
        &[],
    ] {
        assert!(!view_is_tenant_wide(&token(perms), "dev"), "{perms:?}");
    }
    assert!(!view_is_tenant_wide("opaque", "dev"));
    assert!(!view_is_tenant_wide("a.!!!.c", "dev"));
}
