//! Unit tests for the pure decision logic of `user.c`: the revoke planner, the
//! grant-option defaults, the GUC check/assign hooks, and `parse_bool`.

use super::*;

/// Build an `AuthMemForm` for a `pg_auth_members` grant in the test fixtures.
fn mem(oid: Oid, roleid: Oid, member: Oid, grantor: Oid, admin: bool) -> AuthMemForm {
    AuthMemForm {
        oid,
        roleid,
        member,
        grantor,
        admin_option: admin,
        inherit_option: true,
        set_option: true,
    }
}

/// `InitGrantRoleOptions` matches the C defaults.
#[test]
fn init_grant_role_options_defaults() {
    let popt = InitGrantRoleOptions();
    assert_eq!(popt.specified, 0);
    assert!(!popt.admin);
    assert!(!popt.inherit);
    assert!(popt.set);
}

/// `initialize_revoke_actions` returns one `RRG_NOOP` per member.
#[test]
fn initialize_revoke_actions_all_noop() {
    let members = vec![mem(1, 100, 200, 10, false), mem(2, 100, 300, 10, false)];
    let actions = initialize_revoke_actions(&members);
    assert_eq!(actions.len(), 2);
    assert!(actions.iter().all(|a| *a == RRG_NOOP));
    assert!(initialize_revoke_actions(&[]).is_empty());
}

/// `assign_createrole_self_grant` mirrors the C globals it sets.
#[test]
fn assign_createrole_self_grant_maps_bits() {
    let (enabled, opts) = assign_createrole_self_grant(0);
    assert!(!enabled);
    assert!(!opts.inherit);
    assert!(!opts.set);

    let (enabled, opts) = assign_createrole_self_grant(GRANT_ROLE_SPECIFIED_INHERIT);
    assert!(enabled);
    assert!(opts.inherit);
    assert!(!opts.set);
    assert!(!opts.admin);
    assert_eq!(
        opts.specified,
        GRANT_ROLE_SPECIFIED_ADMIN | GRANT_ROLE_SPECIFIED_INHERIT | GRANT_ROLE_SPECIFIED_SET
    );

    let (enabled, opts) =
        assign_createrole_self_grant(GRANT_ROLE_SPECIFIED_INHERIT | GRANT_ROLE_SPECIFIED_SET);
    assert!(enabled);
    assert!(opts.inherit);
    assert!(opts.set);
}

/// `plan_single_revoke` returns false when no matching grant exists, and true
/// (recording `RRG_DELETE_GRANT`) when it does.
#[test]
fn plan_single_revoke_finds_and_misses() {
    let members = vec![mem(1, 100, 200, 10, false)];
    let popt = InitGrantRoleOptions(); // specified == 0 => full revoke
    let mut actions = initialize_revoke_actions(&members);

    assert!(!plan_single_revoke(&members, &mut actions, 999, 10, &popt, DROP_RESTRICT).unwrap());
    assert_eq!(actions[0], RRG_NOOP);

    assert!(plan_single_revoke(&members, &mut actions, 200, 10, &popt, DROP_RESTRICT).unwrap());
    assert_eq!(actions[0], RRG_DELETE_GRANT);
}

/// Revoking just the INHERIT option records `RRG_REMOVE_INHERIT_OPTION` without
/// recursing.
#[test]
fn plan_single_revoke_inherit_only() {
    let members = vec![mem(1, 100, 200, 10, true)];
    let mut popt = InitGrantRoleOptions();
    popt.specified = GRANT_ROLE_SPECIFIED_INHERIT;
    let mut actions = initialize_revoke_actions(&members);

    assert!(plan_single_revoke(&members, &mut actions, 200, 10, &popt, DROP_RESTRICT).unwrap());
    assert_eq!(actions[0], RRG_REMOVE_INHERIT_OPTION);
}

/// `plan_recursive_revoke` under DROP_RESTRICT raises when a dependent grant
/// exists.
#[test]
fn plan_recursive_revoke_restrict_errors_on_dependents() {
    let members = vec![mem(1, 100, 200, 10, true), mem(2, 100, 300, 200, false)];
    let mut actions = initialize_revoke_actions(&members);

    let res = plan_recursive_revoke(&members, &mut actions, 0, false, DROP_RESTRICT);
    assert!(res.is_err(), "expected dependent-objects error under RESTRICT");
}

/// `plan_recursive_revoke` under DROP_CASCADE schedules both the grant and its
/// dependent grant for deletion.
#[test]
fn plan_recursive_revoke_cascade_deletes_dependents() {
    let members = vec![mem(1, 100, 200, 10, true), mem(2, 100, 300, 200, false)];
    let mut actions = initialize_revoke_actions(&members);

    plan_recursive_revoke(&members, &mut actions, 0, false, DROP_CASCADE).unwrap();
    assert_eq!(actions[0], RRG_DELETE_GRANT);
    assert_eq!(actions[1], RRG_DELETE_GRANT);
}

/// The `GRANT_ROLE_SPECIFIED_*` flag values match user.c's `#define`s.
#[test]
fn grant_role_specified_bits_are_distinct() {
    assert_eq!(GRANT_ROLE_SPECIFIED_ADMIN, 0x0001);
    assert_eq!(GRANT_ROLE_SPECIFIED_INHERIT, 0x0002);
    assert_eq!(GRANT_ROLE_SPECIFIED_SET, 0x0004);
}

/// `parse_bool` recognizes the PostgreSQL boolean spellings.
#[test]
fn parse_bool_recognizes_spellings() {
    for t in ["t", "true", "TRUE", "y", "Yes", "on", "1"] {
        assert_eq!(parse_bool(t), Some(true), "{t}");
    }
    for f in ["f", "false", "FALSE", "n", "No", "off", "0"] {
        assert_eq!(parse_bool(f), Some(false), "{f}");
    }
    assert_eq!(parse_bool("maybe"), None);
    assert_eq!(parse_bool(""), None);
}

/// `check_createrole_self_grant` recognizes SET / INHERIT and rejects unknown
/// keywords, exercised through a trivial `SplitIdentifierString` seam.
#[test]
fn check_createrole_self_grant_keywords() {
    seam::split_identifier_string::set(|raw| {
        if raw.trim().is_empty() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(raw.split(',').map(|s| s.trim().to_string()).collect()))
    });

    use std::sync::Mutex;
    static LAST_DETAIL: Mutex<Option<String>> = Mutex::new(None);
    seam::guc_check_errdetail::set(|detail| {
        *LAST_DETAIL.lock().unwrap() = Some(detail);
    });

    assert_eq!(
        check_createrole_self_grant("set").unwrap(),
        Some(GRANT_ROLE_SPECIFIED_SET)
    );
    assert_eq!(
        check_createrole_self_grant("inherit").unwrap(),
        Some(GRANT_ROLE_SPECIFIED_INHERIT)
    );
    assert_eq!(
        check_createrole_self_grant("set, inherit").unwrap(),
        Some(GRANT_ROLE_SPECIFIED_SET | GRANT_ROLE_SPECIFIED_INHERIT)
    );
    assert_eq!(check_createrole_self_grant("").unwrap(), Some(0));
    assert_eq!(check_createrole_self_grant("bogus").unwrap(), None);
    /* The detail string for the unrecognized keyword must reach the GUC seam. */
    assert_eq!(
        LAST_DETAIL.lock().unwrap().as_deref(),
        Some("Unrecognized key word: \"bogus\".")
    );
}
