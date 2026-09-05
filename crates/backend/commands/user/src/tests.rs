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

// def_get_string T_TypeName/T_List/T_A_Star arms (previously panicked): C
// routes through TypeNameToString / NameListToString / "*".
#[test]
fn def_get_string_remaining_arms_match_c() {
    use types_nodes::rawnodes::TypeName;
    use types_nodes::Node;
    let ctx = mcx::MemoryContext::new("user-test");
    let mcx = ctx.mcx();
    let string_node = |s: &'static str| Node::mk(mcx, types_nodes::String { sval: s }).unwrap();

    let tn = TypeName {
        names: types_nodes::NodeList::from_slice(mcx, &[string_node("admin")]).unwrap(),
        ..TypeName::default()
    };
    let def = DefElem {
        defname: Some("rolename"),
        arg: Some(Node::mk(mcx, tn).unwrap()),
        ..DefElem::default()
    };
    assert_eq!(def_get_string(&def).unwrap(), "admin");

    let names = [string_node("a"), string_node("b")];
    let def = DefElem {
        defname: Some("opt"),
        arg: Some(
            Node::mk(mcx, types_nodes::NodeList::from_slice(mcx, &names).unwrap()).unwrap(),
        ),
        ..DefElem::default()
    };
    assert_eq!(def_get_string(&def).unwrap(), "a.b");
}

// check_password_hook chain: modules install newest-first (C: each _PG_init
// saves the previous pointer and calls it from its own hook), the arguments
// reach every hook unchanged, and the first error stops the chain.
mod check_password_hook_chain {
    use super::*;
    use std::sync::Mutex;

    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

    fn first(
        _mcx: Mcx<'_>,
        username: &str,
        password: &str,
        password_type: crypt::PasswordType,
        validuntil_time: Datum,
        validuntil_null: bool,
    ) -> PgResult<()> {
        SEEN.lock().unwrap().push(format!(
            "first:{username}:{password}:{password_type:?}:{}:{validuntil_null}",
            validuntil_time.as_i64()
        ));
        Ok(())
    }

    fn second(
        _mcx: Mcx<'_>,
        _username: &str,
        password: &str,
        _password_type: crypt::PasswordType,
        _validuntil_time: Datum,
        _validuntil_null: bool,
    ) -> PgResult<()> {
        SEEN.lock().unwrap().push(format!("second:{password}"));
        if password == "refuse" {
            return Err(err("second refuses".into(), ERRCODE_INVALID_PARAMETER_VALUE));
        }
        Ok(())
    }

    #[test]
    fn runs_newest_first_and_stops_at_the_first_error() {
        let root = mcx::session_root("user-check-password-hook-test");
        let mcx = root.mcx();
        assert!(!check_password_hook_installed());
        // No hook: nothing runs, nothing fails.
        run_check_password_hook(mcx, "u", "refuse", Datum::from_i64(7), false).unwrap();
        assert!(SEEN.lock().unwrap().is_empty());

        install_check_password_hook(first);
        install_check_password_hook(second);
        assert!(check_password_hook_installed());

        run_check_password_hook(mcx, "alice", "abcdefg1", Datum::from_i64(7), false).unwrap();
        assert_eq!(
            std::mem::take(&mut *SEEN.lock().unwrap()),
            vec!["second:abcdefg1".to_string(), "first:alice:abcdefg1:Plaintext:7:false".to_string()]
        );

        // get_password_type classifies the supplied string (user.c:400).
        run_check_password_hook(mcx, "alice", "md5e5f1b3fb6e6c8f8a2a4b4f9a7b3c1d2e", Datum::null(), true)
            .unwrap();
        assert_eq!(
            std::mem::take(&mut *SEEN.lock().unwrap())[1],
            "first:alice:md5e5f1b3fb6e6c8f8a2a4b4f9a7b3c1d2e:Md5:0:true"
        );

        let e = run_check_password_hook(mcx, "alice", "refuse", Datum::null(), true)
            .expect_err("the newest hook refuses");
        assert_eq!(e.message(), "second refuses");
        assert_eq!(std::mem::take(&mut *SEEN.lock().unwrap()), vec!["second:refuse".to_string()]);
    }
}
