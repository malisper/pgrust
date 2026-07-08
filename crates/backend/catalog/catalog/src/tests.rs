use super::*;
use mcx::{Mcx, MemoryContext, PgVec};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;
use types_core::{INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_rel::{LockInfoData, LockRelId, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT};
use types_tuple::{NameData, TupleDescData};

const MY_TEMP_TOAST_NS: Oid = 16999;

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        namespace_seams::is_temp_toast_namespace::set(|ns| ns == MY_TEMP_TOAST_NS);
        init_seams();
    });
}

fn rel_with_ns(mcx: Mcx<'_>, relid: Oid, relnamespace: Oid) -> RelationData<'_> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: relid, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: relid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        }),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

#[test]
fn catalog_relation_oid_cutoff() {
    install_seams();
    assert!(IsCatalogRelationOid(RELATION_RELATION_ID));
    assert!(IsCatalogRelationOid(11999));
    assert!(!IsCatalogRelationOid(12000));
    assert!(!IsCatalogRelationOid(16384));
    assert!(catalog_seams::is_catalog_relation_oid::call(1259));
}

#[test]
fn system_and_catalog_relation_predicates() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pg_class = rel_with_ns(mcx, RELATION_RELATION_ID, PG_CATALOG_NAMESPACE);
    assert!(IsSystemRelation(&pg_class));
    assert!(IsCatalogRelation(&pg_class));
    assert!(catalog_seams::is_catalog_relation::call(&pg_class));

    let user_rel = rel_with_ns(mcx, 16400, 2200);
    assert!(!IsSystemRelation(&user_rel));
    assert!(!IsCatalogRelation(&user_rel));

    // A user table's toast table is a system relation but not a catalog.
    let user_toast = rel_with_ns(mcx, 16401, PG_TOAST_NAMESPACE);
    assert!(IsSystemRelation(&user_toast));
    assert!(!IsCatalogRelation(&user_toast));
    assert!(IsSystemClass(user_toast.rd_id, &user_toast.rd_rel));
}

#[test]
fn toast_predicates() {
    install_seams();
    assert!(IsToastNamespace(PG_TOAST_NAMESPACE));
    assert!(IsToastNamespace(MY_TEMP_TOAST_NS));
    assert!(!IsToastNamespace(PG_CATALOG_NAMESPACE));

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let toast_rel = rel_with_ns(mcx, 16401, PG_TOAST_NAMESPACE);
    assert!(IsToastRelation(&toast_rel));
    assert!(catalog_seams::is_toast_relation::call(&toast_rel));
    assert!(IsToastClass(&toast_rel.rd_rel));

    let temp_toast_rel = rel_with_ns(mcx, 16402, MY_TEMP_TOAST_NS);
    assert!(IsToastRelation(&temp_toast_rel));
    assert!(!IsToastRelation(&rel_with_ns(mcx, 16403, 2200)));
}

#[test]
fn shared_relation_list() {
    install_seams();
    for oid in [
        AuthIdRelationId,
        AuthMemRelationId,
        DATABASE_RELATION_ID,
        DbRoleSettingRelationId,
        ParameterAclRelationId,
        ReplicationOriginRelationId,
        SharedDependRelationId,
        SharedDescriptionRelationId,
        SharedSecLabelRelationId,
        SubscriptionRelationId,
        TABLE_SPACE_RELATION_ID,
        AuthIdOidIndexId,
        AuthIdRolnameIndexId,
        AuthMemMemRoleIndexId,
        AuthMemRoleMemIndexId,
        AuthMemOidIndexId,
        AuthMemGrantorIndexId,
        DatabaseNameIndexId,
        DatabaseOidIndexId,
        DbRoleSettingDatidRolidIndexId,
        ParameterAclOidIndexId,
        ParameterAclParnameIndexId,
        ReplicationOriginIdentIndex,
        ReplicationOriginNameIndex,
        SharedDependDependerIndexId,
        SharedDependReferenceIndexId,
        SharedDescriptionObjIndexId,
        SharedSecLabelObjectIndexId,
        SubscriptionNameIndexId,
        SubscriptionObjectIndexId,
        TablespaceNameIndexId,
        TablespaceOidIndexId,
        PgDatabaseToastTable,
        PgDatabaseToastIndex,
        PgDbRoleSettingToastTable,
        PgDbRoleSettingToastIndex,
        PgParameterAclToastTable,
        PgParameterAclToastIndex,
        PgShdescriptionToastTable,
        PgShdescriptionToastIndex,
        PgShseclabelToastTable,
        PgShseclabelToastIndex,
        PgSubscriptionToastTable,
        PgSubscriptionToastIndex,
        PgTablespaceToastTable,
        PgTablespaceToastIndex,
    ] {
        assert!(IsSharedRelation(oid), "oid {oid} should be shared");
        assert!(catalog_seams::is_shared_relation::call(oid));
    }
    for oid in [RELATION_RELATION_ID, 1249, 16384, 0] {
        assert!(!IsSharedRelation(oid), "oid {oid} should not be shared");
    }
}

#[test]
fn pinned_object_rules() {
    assert!(IsPinnedObject(RELATION_RELATION_ID, 1259));
    assert!(!IsPinnedObject(RELATION_RELATION_ID, 12000));
    assert!(!IsPinnedObject(RELATION_RELATION_ID, 16384));
    assert!(!IsPinnedObject(LargeObjectRelationId, 100));
    assert!(!IsPinnedObject(NamespaceRelationId, PG_PUBLIC_NAMESPACE));
    assert!(IsPinnedObject(NamespaceRelationId, PG_CATALOG_NAMESPACE));
    assert!(!IsPinnedObject(DATABASE_RELATION_ID, 1));
}

#[test]
fn misc_predicates() {
    assert!(IsCatalogNamespace(PG_CATALOG_NAMESPACE));
    assert!(!IsCatalogNamespace(PG_TOAST_NAMESPACE));

    assert!(IsReservedName("pg_toast"));
    assert!(IsReservedName("pg_"));
    assert!(!IsReservedName("pg"));
    assert!(!IsReservedName("Pg_foo"));
    assert!(!IsReservedName(""));

    assert!(IsInplaceUpdateOid(RELATION_RELATION_ID));
    assert!(IsInplaceUpdateOid(DATABASE_RELATION_ID));
    assert!(!IsInplaceUpdateOid(1249));
    let ctx = MemoryContext::new("t");
    assert!(IsInplaceUpdateRelation(&rel_with_ns(
        ctx.mcx(),
        RELATION_RELATION_ID,
        PG_CATALOG_NAMESPACE
    )));

    for oid in [
        ParameterAclParnameIndexId,
        ReplicationOriginNameIndex,
        SecLabelObjectIndexId,
        SharedSecLabelObjectIndexId,
    ] {
        assert!(IsCatalogTextUniqueIndexOid(oid));
    }
    assert!(!IsCatalogTextUniqueIndexOid(AuthIdRolnameIndexId));
}
