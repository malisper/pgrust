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

fn item(grantee: Oid, grantor: Oid, privs: u64, gopts: u64) -> AclItem {
    let mut it = AclItem { ai_grantee: grantee, ai_grantor: grantor, ai_privs: 0 };
    aclitem_set_privs_goptions(&mut it, privs, gopts);
    it
}

#[test]
fn acl_image_roundtrips_and_matches_allocacl_layout() {
    let ctx = mcx::MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let items = [item(0, 10, ACL_SELECT, 0), item(11, 10, ACL_ALL_RIGHTS_RELATION, ACL_SELECT)];
    let img = varlena::acl_image(mcx, &items).unwrap();
    assert_eq!(img.len(), 4 + 20 + 2 * 16);
    assert_eq!(&img[0..4], &(((img.len() as u32) << 2).to_le_bytes()));
    assert_eq!(&img[4..8], &1i32.to_le_bytes());
    assert_eq!(&img[8..12], &0i32.to_le_bytes());
    assert_eq!(&img[12..16], &ACLITEMOID.to_le_bytes());
    assert_eq!(&img[16..20], &2i32.to_le_bytes());
    assert_eq!(&img[20..24], &1i32.to_le_bytes());
    let decoded = varlena::decode_acl_payload(mcx, &img[4..]).unwrap();
    assert_eq!(decoded.as_slice(), &items);
}

#[test]
fn decode_rejects_wrong_elemtype_and_nulls() {
    let ctx = mcx::MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let img = varlena::acl_image(mcx, &[item(0, 10, ACL_SELECT, 0)]).unwrap();
    let mut bad = img.as_slice()[4..].to_vec();
    bad[8] = 0x17;
    assert!(varlena::decode_acl_payload(mcx, &bad).is_err());
    let mut withnulls = img.as_slice()[4..].to_vec();
    withnulls[4] = 24;
    assert!(varlena::decode_acl_payload(mcx, &withnulls).is_err());
}

#[test]
fn aclupdate_add_del_and_prune() {
    let ctx = mcx::MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let base = [item(11, 10, ACL_ALL_RIGHTS_RELATION, 0)];
    let grant = item(0, 10, ACL_SELECT, 0);
    let acl = aclupdate(mcx, &base, &grant, ACL_MODECHG_ADD, 10, DROP_RESTRICT).unwrap();
    assert_eq!(acl.len(), 2);
    assert_eq!(acl[1], grant);
    let more = aclupdate(mcx, &acl, &item(0, 10, ACL_INSERT, 0), ACL_MODECHG_ADD, 10, DROP_RESTRICT)
        .unwrap();
    assert_eq!(aclitem_get_privs(&more[1]), ACL_SELECT | ACL_INSERT);
    let gone = aclupdate(
        mcx,
        &more,
        &item(0, 10, ACL_SELECT | ACL_INSERT, ACL_SELECT | ACL_INSERT),
        ACL_MODECHG_DEL,
        10,
        DROP_RESTRICT,
    )
    .unwrap();
    assert_eq!(gone.len(), 1, "empty entry pruned");
}

#[test]
fn aclnewowner_substitutes_and_merges_duplicates() {
    let ctx = mcx::MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let acl = [
        item(11, 10, ACL_ALL_RIGHTS_RELATION, 0),
        item(12, 10, ACL_SELECT, 0),
        item(12, 11, ACL_INSERT, 0),
    ];
    let out = aclnewowner(mcx, &acl, 10, 11).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], item(11, 11, ACL_ALL_RIGHTS_RELATION, 0));
    assert_eq!(out[1], item(12, 11, ACL_SELECT | ACL_INSERT, 0));
}

#[test]
fn aclcontains_requires_rights_subset() {
    let acl = [item(11, 10, ACL_SELECT | ACL_INSERT, ACL_SELECT)];
    assert!(aclcontains(&acl, &item(11, 10, ACL_SELECT, 0)));
    assert!(aclcontains(&acl, &item(11, 10, ACL_SELECT, ACL_SELECT)));
    assert!(!aclcontains(&acl, &item(11, 10, ACL_UPDATE, 0)));
    assert!(!aclcontains(&acl, &item(11, 11, ACL_SELECT, 0)));
}

#[test]
fn aclmembers_dedups_and_skips_public() {
    let ctx = mcx::MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let acl = [item(0, 10, ACL_SELECT, 0), item(11, 10, ACL_SELECT, 0), item(10, 11, ACL_INSERT, 0)];
    let m = aclmembers(mcx, &acl).unwrap();
    assert_eq!(m.as_slice(), &[10, 11]);
}

#[test]
fn convert_priv_string_case_and_spaces() {
    let map = [
        PrivMapEntry { name: "SELECT", value: ACL_SELECT },
        PrivMapEntry { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMapEntry { name: "INSERT", value: ACL_INSERT },
    ];
    assert_eq!(convert_any_priv_string("select", &map).unwrap(), ACL_SELECT);
    assert_eq!(
        convert_any_priv_string(" Select ,insert", &map).unwrap(),
        ACL_SELECT | ACL_INSERT
    );
    assert!(convert_any_priv_string("bogus", &map).is_err());
}

#[test]
fn aclmask_direct_owner_goptions_only_on_exact_match() {
    let acl = [item(11, 10, ACL_SELECT, 0)];
    let g = crate::ACLITEM_ALL_GOPTION_BITS;
    assert_eq!(aclmask_direct(&acl, 10, 10, g, AclMaskHow::AclmaskAll), g);
    assert_eq!(aclmask_direct(&acl, 11, 10, ACL_SELECT, AclMaskHow::AclmaskAll), ACL_SELECT);
    assert_eq!(aclmask_direct(&acl, 12, 10, ACL_SELECT, AclMaskHow::AclmaskAll), 0);
}

#[test]
fn priv_string_maps_match_c() {
    use crate::builtins::{PARAMETER_PRIV_MAP, ROLE_PRIV_MAP, SEQUENCE_PRIV_MAP, TABLESPACE_PRIV_MAP};
    let gof = |m: u64| (m & 0xFFFF_FFFF) << 32;
    let c = |p: &str, map| convert_any_priv_string(p, map).unwrap();
    assert_eq!(c("CREATE", TABLESPACE_PRIV_MAP), ACL_CREATE);
    assert_eq!(c("create with grant option", TABLESPACE_PRIV_MAP), gof(ACL_CREATE));
    assert!(convert_any_priv_string("USAGE", TABLESPACE_PRIV_MAP).is_err());
    assert_eq!(c("USAGE", SEQUENCE_PRIV_MAP), ACL_USAGE);
    assert_eq!(c("SELECT", SEQUENCE_PRIV_MAP), ACL_SELECT);
    assert_eq!(c("UPDATE WITH GRANT OPTION", SEQUENCE_PRIV_MAP), gof(ACL_UPDATE));
    assert_eq!(c("SET", PARAMETER_PRIV_MAP), ACL_SET);
    assert_eq!(c("ALTER SYSTEM", PARAMETER_PRIV_MAP), ACL_ALTER_SYSTEM);
    assert_eq!(c("alter system with grant option", PARAMETER_PRIV_MAP), gof(ACL_ALTER_SYSTEM));
    assert_eq!(c("USAGE", ROLE_PRIV_MAP), ACL_USAGE);
    assert_eq!(c("MEMBER", ROLE_PRIV_MAP), ACL_CREATE);
    assert_eq!(c("SET", ROLE_PRIV_MAP), ACL_SET);
    for spelled in [
        "USAGE WITH GRANT OPTION",
        "USAGE WITH ADMIN OPTION",
        "MEMBER WITH GRANT OPTION",
        "MEMBER WITH ADMIN OPTION",
        "SET WITH GRANT OPTION",
        "SET WITH ADMIN OPTION",
    ] {
        assert_eq!(c(spelled, ROLE_PRIV_MAP), gof(ACL_CREATE));
    }
}

#[test]
fn aclright_strings_match_c() {
    let expected = [
        "INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "EXECUTE",
        "USAGE", "CREATE", "TEMPORARY", "CONNECT", "SET", "ALTER SYSTEM", "MAINTAIN",
    ];
    assert_eq!(expected.len() as u32, N_ACL_RIGHTS);
    for (i, want) in expected.iter().enumerate() {
        assert_eq!(crate::builtins::convert_aclright_to_string(1u64 << i), *want);
    }
}
