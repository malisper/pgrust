use super::*;
use mcx::{Mcx, MemoryContext, PgVec};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Mutex, Once};
use types_core::{INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::{ErrorLevel, PgError, PgResult, ERRCODE_INTERNAL_ERROR, LOG};
use types_rel::{LockInfoData, LockRelId, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT};
use types_tuple::{NameData, TupleDescData};

const MY_TEMP_TOAST_NS: Oid = 16999;

/// Server-log capture: (level, message, detail) of every ereport_msg report
/// on this test binary.
static LOGS: Mutex<Vec<(ErrorLevel, String, Option<String>)>> = Mutex::new(Vec::new());

fn capture_log(elevel: ErrorLevel, msg: String, detail: Option<String>) -> PgResult<()> {
    LOGS.lock().unwrap_or_else(|e| e.into_inner()).push((elevel, msg, detail));
    Ok(())
}

fn take_logs() -> Vec<(ErrorLevel, String, Option<String>)> {
    std::mem::take(&mut *LOGS.lock().unwrap_or_else(|e| e.into_inner()))
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        namespace_seams::is_temp_toast_namespace::set(|ns| ns == MY_TEMP_TOAST_NS);
        elog_seams::ereport_msg::set(capture_log);
        // fmgr.c:132 fmgr_info_cxt_security: a lookup miss is elog(ERROR).
        fmgr_seams::fmgr_info::set(|fid| {
            Err(Box::new(PgError::error(format!("cache lookup failed for function {fid}"))))
        });
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

// ---- audit-18.6 b176: catalog.c GetNewOidWithIndex / GetNewRelFileNumber ----

// catalog.c:581 elog(ERROR, "invalid relpersistence: %c"): a catchable XX000
// with the byte printed as a character, never a panic.
#[test]
fn get_new_relfilenumber_invalid_relpersistence_is_catchable_xx000() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let err = GetNewRelFileNumber(mcx, 0, None, b'x')
        .expect_err("invalid relpersistence must be a catchable ERROR");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid relpersistence: x");
}

// catalog.c:574 ProcNumberForTempRelations(): a parallel worker probes temp
// relfilenumber collisions under its LEADER's proc number, not its own.
#[test]
fn temp_relfilenumber_probes_under_leader_proc_number() {
    install_seams();
    init_small::globals::SetMyProcNumber(7);
    init_small::globals::SetParallelLeaderProcNumber(3);
    assert_eq!(crate::oid::relfilenumber_proc_number(b't').unwrap(), 3);
    init_small::globals::SetParallelLeaderProcNumber(INVALID_PROC_NUMBER);
    assert_eq!(crate::oid::relfilenumber_proc_number(b't').unwrap(), 7);
    assert_eq!(crate::oid::relfilenumber_proc_number(b'p').unwrap(), INVALID_PROC_NUMBER);
    assert_eq!(crate::oid::relfilenumber_proc_number(b'u').unwrap(), INVALID_PROC_NUMBER);
    init_small::globals::SetMyProcNumber(INVALID_PROC_NUMBER);
}

// catalog.c:479 ScanKeyInit -> fmgr_info(F_OIDEQ): an fmgr failure is a
// catchable elog(ERROR), never a panic.
#[test]
fn oid_eq_key_propagates_fmgr_info_error() {
    install_seams();
    let err = crate::oid::oid_eq_key(1, 5).err().expect("fmgr_info failure must propagate");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "cache lookup failed for function 184");
}

// catalog.c:493-535: no LOG traffic below GETNEWOID_LOG_THRESHOLD collisions.
#[test]
fn get_new_oid_few_collisions_log_nothing() {
    install_seams();
    let _ = take_logs();
    let mut next = 100u32;
    let oid = crate::oid::get_new_oid_loop(
        "pg_class",
        || {
            next += 1;
            Ok(next)
        },
        |oid| Ok(oid < 106),
    )
    .unwrap();
    assert_eq!(oid, 106);
    assert!(take_logs().is_empty());
}

// catalog.c:493-535: past 1,000,000 collisions, LOG "still searching" with the
// plural DETAIL at 1M and 2M retries (exponential interval), then the
// completion LOG once an OID is found.
#[test]
fn get_new_oid_million_collisions_log_progress_and_completion() {
    install_seams();
    let _ = take_logs();
    let mut next = 0u32;
    let oid = crate::oid::get_new_oid_loop(
        "pg_class",
        || {
            next += 1;
            Ok(next)
        },
        |oid| Ok(oid <= 2_000_000),
    )
    .unwrap();
    assert_eq!(oid, 2_000_001);
    let logs = take_logs();
    let still = "still searching for an unused OID in relation \"pg_class\"".to_string();
    assert_eq!(
        logs,
        vec![
            (
                LOG,
                still.clone(),
                Some("OID candidates have been checked 1000000 times, but no unused OID has been found yet.".to_string()),
            ),
            (
                LOG,
                still,
                Some("OID candidates have been checked 2000000 times, but no unused OID has been found yet.".to_string()),
            ),
            (
                LOG,
                "new OID has been assigned in relation \"pg_class\" after 2000001 retries".to_string(),
                None,
            ),
        ]
    );
}

// catalog.c:511-519: the log interval doubles up to GETNEWOID_LOG_MAX_INTERVAL,
// then grows by GETNEWOID_LOG_MAX_INTERVAL per report.
#[test]
fn get_new_oid_log_interval_schedule() {
    let mut before_log = 1_000_000u64;
    let mut schedule = Vec::new();
    for _ in 0..10 {
        before_log = crate::oid::next_retries_before_log(before_log);
        schedule.push(before_log);
    }
    assert_eq!(
        schedule,
        [
            2_000_000, 4_000_000, 8_000_000, 16_000_000, 32_000_000, 64_000_000, 128_000_000,
            256_000_000, 384_000_000, 512_000_000
        ]
    );
}
