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
