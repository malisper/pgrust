use super::*;
use membership::{
    cached_role, roles_is_member_of_contains, seed_db_hash, seed_membership_cache,
    RoleRecurseType,
};
use types_core::catalog::BOOTSTRAP_SUPERUSERID;

const AUTHOID: i32 = 11;
const AUTHMEMROLEMEM: i32 = 9;
const DATABASEOID: i32 = 21;

#[test]
fn constants_match_c_headers() {
    assert_eq!(ACL_EXECUTE, 1 << 7);
    assert_eq!(ACL_CONNECT, 1 << 11);
    assert_eq!(ACL_SET, 1 << 12);
    assert_eq!(ACL_MAINTAIN, 1 << 14);
    assert_eq!(N_ACL_RIGHTS, 15);
    assert_eq!(ACL_ALL_RIGHTS_DATABASE, ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT);
    assert_eq!(ACL_ALL_RIGHTS_PARAMETER_ACL, ACL_SET | ACL_ALTER_SYSTEM);
    assert_eq!(ACLITEM_ALL_GOPTION_BITS, 0xFFFF_FFFF_0000_0000);
    assert_eq!(types_core::catalog::ROLE_PG_DATABASE_OWNER, 6171);
}

#[test]
fn acldefault_database_grants_connect_and_temp_to_public() {
    let acl = acldefault(AclObjectType::Database, 42);
    let items = acl.as_slice();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].ai_grantee, ACL_ID_PUBLIC);
    assert_eq!(items[0].ai_grantor, 42);
    assert_eq!(items[0].ai_privs, ACL_CREATE_TEMP | ACL_CONNECT);
    assert_eq!(items[1].ai_grantee, 42);
    assert_eq!(items[1].ai_privs, ACL_ALL_RIGHTS_DATABASE);
}

#[test]
fn acldefault_arm_shapes() {
    assert_eq!(acldefault(AclObjectType::Column, 1).as_slice().len(), 0);
    assert_eq!(acldefault(AclObjectType::Table, 1).as_slice().len(), 1);
    assert_eq!(acldefault(AclObjectType::Function, 1).as_slice()[0].ai_privs, ACL_EXECUTE);
    let pacl = acldefault(AclObjectType::ParameterAcl, BOOTSTRAP_SUPERUSERID);
    assert_eq!(pacl.as_slice().len(), 1);
    assert_eq!(pacl.as_slice()[0].ai_grantee, BOOTSTRAP_SUPERUSERID);
}

#[test]
fn aclmask_public_and_owner_arms() {
    let acl = acldefault(AclObjectType::Database, 42);
    let items = acl.as_slice();
    // Any role reaches ACL_CONNECT through the PUBLIC entry.
    assert_eq!(
        aclmask(items, 12345, 42, ACL_CONNECT, AclMaskHow::AclmaskAny).unwrap(),
        ACL_CONNECT
    );
    // The owner reaches ACL_CREATE through its own entry (first pass).
    assert_eq!(
        aclmask(items, 42, 42, ACL_CREATE, AclMaskHow::AclmaskAny).unwrap(),
        ACL_CREATE
    );
    assert_eq!(aclmask(items, 42, 42, 0, AclMaskHow::AclmaskAny).unwrap(), 0);
    // ACLMASK_ALL keeps accumulating until the full mask is covered.
    assert_eq!(
        aclmask(items, 42, 42, ACL_CONNECT | ACL_CREATE, AclMaskHow::AclmaskAll).unwrap(),
        ACL_CONNECT | ACL_CREATE
    );
}

#[test]
fn membership_fast_paths_no_catalog() {
    assert!(has_privs_of_role(7, 7).unwrap());
    assert!(member_can_set_role(7, 7).unwrap());
    assert!(is_member_of_role(7, 7).unwrap());
    assert!(is_member_of_role_nosuper(7, 7).unwrap());
    // Bootstrap-superuser escape hatch inside superuser_arg.
    assert!(has_privs_of_role(BOOTSTRAP_SUPERUSERID, 7).unwrap());
}

#[test]
fn membership_memo_and_invalidation() {
    seed_membership_cache(1, 55, vec![55, 66]);
    assert!(roles_is_member_of_contains(55, RoleRecurseType::Privs, 66).unwrap());
    assert!(!roles_is_member_of_contains(55, RoleRecurseType::Privs, 77).unwrap());

    // AUTHOID inval clears every recurse-type slot.
    RoleMembershipCacheCallback(datum::Datum::null(), AUTHOID, 999);
    assert_eq!(cached_role(1), types_core::InvalidOid);

    // pg_database inval for a different database is ignored.
    seed_membership_cache(0, 55, vec![55]);
    seed_db_hash(0xABCD);
    RoleMembershipCacheCallback(datum::Datum::null(), DATABASEOID, 0x1234);
    assert_eq!(cached_role(0), 55);
    RoleMembershipCacheCallback(datum::Datum::null(), DATABASEOID, 0xABCD);
    assert_eq!(cached_role(0), types_core::InvalidOid);
    seed_membership_cache(2, 55, vec![55]);
    RoleMembershipCacheCallback(datum::Datum::null(), AUTHMEMROLEMEM, 0);
    assert_eq!(cached_role(2), types_core::InvalidOid);
}

#[test]
fn install_seams() {
    init_seams();
    assert!(acl_seams::initialize_acl::is_installed());
    assert!(acl_seams::has_privs_of_role::is_installed());
    assert!(acl_seams::member_can_set_role::is_installed());
    assert!(acl_seams::is_member_of_role_nosuper::is_installed());
    assert!(acl_seams::get_role_oid::is_installed());
    assert!(acl_seams::has_privs_of_role::call(9, 9).unwrap());
}

#[test]
fn cache_ids_match_cacheinfo() {
    assert_eq!(cache_syscache::cacheinfo::AUTHOID, AUTHOID);
    assert_eq!(cache_syscache::cacheinfo::AUTHMEMROLEMEM, AUTHMEMROLEMEM);
    assert_eq!(cache_syscache::cacheinfo::DATABASEOID, DATABASEOID);
}
