use super::*;
use adt_acl::ACL_CONNECT;

#[test]
fn superuser_bypasses_object_aclcheck() {
    // Bootstrap superuser + !IsUnderPostmaster: superuser.c's escape hatch.
    assert!(!init_small::globals::IsUnderPostmaster());
    let r = object_aclcheck(DATABASE_RELATION_ID, 1, BOOTSTRAP_SUPERUSERID, ACL_CONNECT).unwrap();
    assert_eq!(r, ACLCHECK_OK);
    let r = object_aclcheck(PROCEDURE_RELATION_ID, 1255, BOOTSTRAP_SUPERUSERID, 1 << 7).unwrap();
    assert_eq!(r, ACLCHECK_OK);
}

#[test]
fn superuser_bypasses_parameter_aclcheck() {
    assert!(pg_parameter_aclcheck("work_mem", BOOTSTRAP_SUPERUSERID, ACL_SET).unwrap() == 0);
    assert_eq!(
        pg_parameter_aclcheck("SEARCH_PATH", BOOTSTRAP_SUPERUSERID, ACL_SET).unwrap(),
        ACLCHECK_OK
    );
}

#[test]
fn aclcheck_result_codes_match_acl_h() {
    assert_eq!(ACLCHECK_OK, 0);
    assert_eq!(ACLCHECK_NO_PRIV, 1);
    assert_eq!(ACLCHECK_NOT_OWNER, 2);
}

#[test]
fn install_seams() {
    init_seams();
    assert!(aclchk_seams::object_aclcheck::is_installed());
    assert!(aclchk_seams::pg_parameter_aclcheck_set::is_installed());
    assert!(
        aclchk_seams::pg_parameter_aclcheck_set::call("work_mem", BOOTSTRAP_SUPERUSERID).unwrap()
    );
    assert_eq!(
        aclchk_seams::object_aclcheck::call(
            DATABASE_RELATION_ID,
            1,
            BOOTSTRAP_SUPERUSERID,
            ACL_CONNECT
        )
        .unwrap(),
        ACLCHECK_OK
    );
}

#[test]
fn record_extension_init_priv_noop_outside_extension_script() {
    // The creating_extension gate must short-circuit before any catalog
    // access: this test runs with no database, so reaching the worker would
    // fail loudly.
    assert!(!pg_depend::creating_extension());
    let ctx = mcx::MemoryContext::new("t");
    grant::record_extension_init_priv(ctx.mcx(), 50001, RELATION_RELATION_ID, 0, &[]).unwrap();
}

#[test]
fn init_priv_privtype_matches_pg_init_privs_h() {
    assert_eq!(grant::INITPRIVS_EXTENSION, b'e' as i8);
}

#[test]
fn init_priv_owner_route_covers_grantable_syscache_classes() {
    // (cacheid, owner attnum) per objectaddress.c's ObjectProperty rows.
    assert_eq!(
        grant::init_priv_owner_route(types_core::FOREIGN_DATA_WRAPPER_RELATION_ID),
        (cache_syscache::cacheinfo::FOREIGNDATAWRAPPEROID, 3, "foreign-data wrapper"),
    );
    assert_eq!(
        grant::init_priv_owner_route(types_core::FOREIGN_SERVER_RELATION_ID),
        (cache_syscache::cacheinfo::FOREIGNSERVEROID, 3, "foreign server"),
    );
    // pg_class.relowner.
    assert_eq!(grant::init_priv_owner_route(RELATION_RELATION_ID).1, 6);
}

#[test]
fn pg_aclmask_defensive_arms_error_catchably() {
    use types_nodes::parsenodes::ObjectType;
    // C's elog(ERROR) arms: no grantable rights on these types; a catchable
    // error, never a process panic.
    let e = pg_aclmask_for_grant(ObjectType::OBJECT_STATISTIC_EXT, 1, 0, 10, 0).unwrap_err();
    assert_eq!(e.message, "grantable rights not supported for statistics objects");
    let e = pg_aclmask_for_grant(ObjectType::OBJECT_EVENT_TRIGGER, 1, 0, 10, 0).unwrap_err();
    assert_eq!(e.message, "grantable rights not supported for event triggers");
}
