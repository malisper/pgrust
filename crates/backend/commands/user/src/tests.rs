use types_nodes::parsenodes::DropBehavior;
use types_tuple::ItemPointerData;

use super::*;

fn row(oid: u32, roleid: u32, member: u32, grantor: u32, admin: bool) -> AuthMemRow {
    AuthMemRow {
        tid: ItemPointerData::default(),
        oid,
        roleid,
        member,
        grantor,
        admin_option: admin,
        inherit_option: true,
        set_option: true,
    }
}

fn noop_actions(n: usize) -> Vec<RevokeRoleGrantAction> {
    vec![RevokeRoleGrantAction::Noop; n]
}

#[test]
fn attnums_match_pg_authid_header() {
    assert_eq!(Natts_pg_authid, 12);
    assert_eq!(Anum_pg_authid_oid, 1);
    assert_eq!(Anum_pg_authid_rolname, 2);
    assert_eq!(Anum_pg_authid_rolsuper, 3);
    assert_eq!(Anum_pg_authid_rolinherit, 4);
    assert_eq!(Anum_pg_authid_rolcreaterole, 5);
    assert_eq!(Anum_pg_authid_rolcreatedb, 6);
    assert_eq!(Anum_pg_authid_rolcanlogin, 7);
    assert_eq!(Anum_pg_authid_rolreplication, 8);
    assert_eq!(Anum_pg_authid_rolbypassrls, 9);
    assert_eq!(Anum_pg_authid_rolconnlimit, 10);
    assert_eq!(Anum_pg_authid_rolpassword, 11);
    assert_eq!(Anum_pg_authid_rolvaliduntil, 12);
}

#[test]
fn attnums_match_pg_auth_members_header() {
    assert_eq!(Natts_pg_auth_members, 7);
    assert_eq!(Anum_pg_auth_members_oid, 1);
    assert_eq!(Anum_pg_auth_members_roleid, 2);
    assert_eq!(Anum_pg_auth_members_member, 3);
    assert_eq!(Anum_pg_auth_members_grantor, 4);
    assert_eq!(Anum_pg_auth_members_admin_option, 5);
    assert_eq!(Anum_pg_auth_members_inherit_option, 6);
    assert_eq!(Anum_pg_auth_members_set_option, 7);
}

#[test]
fn init_grant_role_options_defaults() {
    let popt = InitGrantRoleOptions();
    assert_eq!(popt.specified, 0);
    assert!(!popt.admin);
    assert!(!popt.inherit);
    assert!(popt.set);
}

#[test]
fn plan_single_revoke_missing_grant_returns_false() {
    let members = [row(1, 100, 200, 10, false)];
    let mut actions = noop_actions(1);
    let popt = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true };
    let found = plan_single_revoke(
        &members,
        &mut actions,
        999,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT,
    )
    .unwrap();
    assert!(!found);
    assert_eq!(actions[0], RevokeRoleGrantAction::Noop);
}

#[test]
fn plan_single_revoke_deletes_plain_grant() {
    let members = [row(1, 100, 200, 10, false)];
    let mut actions = noop_actions(1);
    let popt = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true };
    let found = plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT,
    )
    .unwrap();
    assert!(found);
    assert_eq!(actions[0], RevokeRoleGrantAction::DeleteGrant);
}

#[test]
fn plan_single_revoke_option_only_arms() {
    let members = [row(1, 100, 200, 10, true)];

    let mut actions = noop_actions(1);
    let popt = GrantRoleOptions {
        specified: GRANT_ROLE_SPECIFIED_INHERIT,
        admin: false,
        inherit: false,
        set: true,
    };
    assert!(plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT
    )
    .unwrap());
    assert_eq!(actions[0], RevokeRoleGrantAction::RemoveInheritOption);

    let mut actions = noop_actions(1);
    let popt = GrantRoleOptions {
        specified: GRANT_ROLE_SPECIFIED_SET,
        admin: false,
        inherit: false,
        set: false,
    };
    assert!(plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT
    )
    .unwrap());
    assert_eq!(actions[0], RevokeRoleGrantAction::RemoveSetOption);
}

#[test]
fn plan_recursive_revoke_restrict_errors_on_dependent_grant() {
    // 10 grants ADMIN to 200; 200 grants to 300.
    let members = [row(1, 100, 200, 10, true), row(2, 100, 300, 200, false)];
    let mut actions = noop_actions(2);
    let popt = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true };
    let e = plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT,
    )
    .unwrap_err();
    assert_eq!(e.message(), "dependent privileges exist");
    assert_eq!(e.hint(), Some("Use CASCADE to revoke them too."));
}

#[test]
fn plan_recursive_revoke_cascade_deletes_dependents() {
    let members = [row(1, 100, 200, 10, true), row(2, 100, 300, 200, false)];
    let mut actions = noop_actions(2);
    let popt = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true };
    assert!(plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_CASCADE
    )
    .unwrap());
    assert_eq!(actions[0], RevokeRoleGrantAction::DeleteGrant);
    assert_eq!(actions[1], RevokeRoleGrantAction::DeleteGrant);
}

#[test]
fn plan_recursive_revoke_admin_only_keeps_grant() {
    let members = [row(1, 100, 200, 10, true), row(2, 100, 300, 200, false)];
    let mut actions = noop_actions(2);
    let popt = GrantRoleOptions {
        specified: GRANT_ROLE_SPECIFIED_ADMIN,
        admin: false,
        inherit: false,
        set: true,
    };
    assert!(plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_CASCADE
    )
    .unwrap());
    assert_eq!(actions[0], RevokeRoleGrantAction::RemoveAdminOption);
    assert_eq!(actions[1], RevokeRoleGrantAction::DeleteGrant);
}

#[test]
fn plan_recursive_revoke_stops_when_other_admin_grant_survives() {
    // 200 holds ADMIN from two grantors; revoking one leaves the other, so
    // 200's downstream grant must survive.
    let members = [
        row(1, 100, 200, 10, true),
        row(2, 100, 200, 11, true),
        row(3, 100, 300, 200, false),
    ];
    let mut actions = noop_actions(3);
    let popt = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true };
    assert!(plan_single_revoke(
        &members,
        &mut actions,
        200,
        10,
        &popt,
        DropBehavior::DROP_RESTRICT
    )
    .unwrap());
    assert_eq!(actions[0], RevokeRoleGrantAction::DeleteGrant);
    assert_eq!(actions[1], RevokeRoleGrantAction::Noop);
    assert_eq!(actions[2], RevokeRoleGrantAction::Noop);
}

#[test]
fn plan_member_revoke_removes_all_grants_to_member() {
    let members = [
        row(1, 100, 200, 10, true),
        row(2, 100, 200, 11, false),
        row(3, 100, 300, 10, false),
    ];
    let mut actions = noop_actions(3);
    plan_member_revoke(&members, &mut actions, 200).unwrap();
    assert_eq!(actions[0], RevokeRoleGrantAction::DeleteGrant);
    assert_eq!(actions[1], RevokeRoleGrantAction::DeleteGrant);
    assert_eq!(actions[2], RevokeRoleGrantAction::Noop);
}

#[test]
fn assign_createrole_self_grant_sets_options() {
    assign_createrole_self_grant(None, None);
    assert!(!createrole_self_grant_enabled());

    let extra: guc_tables::GucHookExtra =
        Box::new(GRANT_ROLE_SPECIFIED_SET | GRANT_ROLE_SPECIFIED_INHERIT);
    assign_createrole_self_grant(Some("set, inherit"), Some(&extra));
    assert!(createrole_self_grant_enabled());
    let popt = CREATEROLE_SELF_GRANT_OPTIONS.get();
    assert_eq!(
        popt.specified,
        GRANT_ROLE_SPECIFIED_ADMIN | GRANT_ROLE_SPECIFIED_INHERIT | GRANT_ROLE_SPECIFIED_SET
    );
    assert!(!popt.admin);
    assert!(popt.inherit);
    assert!(popt.set);

    let extra: guc_tables::GucHookExtra = Box::new(0u32);
    assign_createrole_self_grant(Some(""), Some(&extra));
    assert!(!createrole_self_grant_enabled());
}
