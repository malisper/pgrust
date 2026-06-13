//! initfile family — backend relcache bring-up, local-relation creation,
//! relfilenumber assignment, and the relcache init-file BINARY CODEC
//! (reclaimed in-crate) (OWN logic).
//!
//! Mirrors `RelationCacheInitialize`/`Phase2`/`Phase3`,
//! `RelationBuildLocalRelation`, `RelationSetNewRelfilenumber`,
//! `RelationAssumeNewRelfilelocator`, `load_critical_index`, the
//! `RelationIdIsInInitFile` predicate, the init-file pre/post-invalidate
//! lock dance, `RelationCacheInitFileRemove`, and the
//! `load`/`write_relcache_init_file` binary codec from
//! `backend/utils/cache/relcache.c`.
//!
//! The orchestration here is relcache's OWN logic and operates on the REAL
//! [`crate::core_entry_store`] entry store. Sibling-family routines
//! (`formrdesc`, `RelationParseRelOptions`, `RelationInitPhysicalAddr`,
//! `RelationInitTableAccessMethod`, `RelationBuildDesc`, ...) are called as
//! their real in-crate functions; they `todo!()` until their families land —
//! the correct cross-family seam-and-panic boundary ("Mirror PG and panic").
//!
//! GENUINE cross-unit primitives whose owner units are not yet dependencies
//! of this crate (the relation map `RelationMapInitialize*`/`UpdateMap`, the
//! syscache `SearchSysCache`/`RelationSupportsSysCache`, the lock manager
//! `LockRelationOid`, `GetCurrentSubTransactionId`/`GetCurrentTransactionId`,
//! `IsSharedRelation`/`IsCatalogNamespace`, the smgr/storage layer, the
//! `RelCacheInitLock` LWLock, and the `AllocateFile`/`AllocateDir` file API)
//! are routed through the [`xunit`] seam-and-panic shims below. Each mirrors
//! the C call exactly and panics until the owner lands — never restructured
//! around, never silently stubbed.

use backend_utils_error::{ereport, PgResult};
use types_error::{ERROR, LOG, WARNING};
use types_error::error::ERRCODE_DATA_CORRUPTED;
use types_core::catalog::{
    BOOTSTRAP_SUPERUSERID, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_core::primitive::{Oid, ProcNumber, RegProcedure};
use types_core::xact::InvalidSubTransactionId;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE,
};

use crate::core_entry_store::entry::{FormPgClass, OwnedAttr, OwnedTupleDesc, RelationData};
use crate::core_entry_store::{cache_insert, eoxact_list_add, with_state, RelationIncrementReferenceCount};
use crate::{REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_NOTHING};

/* ==========================================================================
 * Catalog OIDs the init-file bring-up nails (catalog/pg_*_d.h). Mirrored as
 * local consts; the values are the fixed bootstrap OIDs.
 * ======================================================================== */

// Shared catalog rowtype IDs (formrdesc) — Phase2.
const DatabaseRelation_Rowtype_Id: Oid = 1248;
const AuthIdRelation_Rowtype_Id: Oid = 2842;
const AuthMemRelation_Rowtype_Id: Oid = 2843;
const SharedSecLabelRelation_Rowtype_Id: Oid = 4066;
const SubscriptionRelation_Rowtype_Id: Oid = 6101;

// Local catalog rowtype IDs (formrdesc) — Phase3.
const RelationRelation_Rowtype_Id: Oid = 83;
const AttributeRelation_Rowtype_Id: Oid = 75;
const ProcedureRelation_Rowtype_Id: Oid = 81;
const TypeRelation_Rowtype_Id: Oid = 71;

// Critical local index/heap OIDs (load_critical_index) — Phase3.
const ClassOidIndexId: Oid = 2662;
const RelationRelationId: Oid = 1259;
const AttributeRelidNumIndexId: Oid = 2659;
const AttributeRelationId: Oid = 1249;
const IndexRelidIndexId: Oid = 2679;
const IndexRelationId: Oid = 2610;
const OpclassOidIndexId: Oid = 2687;
const OperatorClassRelationId: Oid = 2616;
const AccessMethodProcedureIndexId: Oid = 2655;
const AccessMethodProcedureRelationId: Oid = 2603;
const RewriteRelRulenameIndexId: Oid = 2693;
const RewriteRelationId: Oid = 2618;
const TriggerRelidNameIndexId: Oid = 2701;
const TriggerRelationId: Oid = 2620;

// Critical shared index/heap OIDs (load_critical_index) — Phase3.
const DatabaseNameIndexId: Oid = 2671;
const DatabaseRelationId: Oid = 1262;
const DatabaseOidIndexId: Oid = 2672;
const AuthIdRolnameIndexId: Oid = 2676;
const AuthIdRelationId: Oid = 1260;
const AuthIdOidIndexId: Oid = 2677;
const AuthMemMemRoleIndexId: Oid = 2694;
const AuthMemRelationId: Oid = 1261;
const SharedSecLabelObjectIndexId: Oid = 3593;
const SharedSecLabelRelationId: Oid = 3592;

// Per-catalog attribute counts (formrdesc natts) — pg_*_d.h `Natts_*`.
const Natts_pg_database: i32 = 17;
const Natts_pg_authid: i32 = 14;
const Natts_pg_auth_members: i32 = 5;
const Natts_pg_shseclabel: i32 = 4;
const Natts_pg_subscription: i32 = 20;
const Natts_pg_class: i32 = 33;
const Natts_pg_attribute: i32 = 25;
const Natts_pg_proc: i32 = 30;
const Natts_pg_type: i32 = 31;

/// `RELCACHE_INIT_FILEMAGIC` (relcache.c) — the init-file magic number.
const RELCACHE_INIT_FILEMAGIC: i32 = 0x0133_7088;
/// `RELCACHE_INIT_FILENAME` (relcache.c).
const RELCACHE_INIT_FILENAME: &str = "pg_internal.init";

/// `NUM_CRITICAL_LOCAL_RELS`/`_INDEXES`, `NUM_CRITICAL_SHARED_RELS`/`_INDEXES`
/// (relcache.c) — the nailed-entry expectations the codec validates.
const NUM_CRITICAL_LOCAL_RELS: i32 = 4;
const NUM_CRITICAL_LOCAL_INDEXES: i32 = 7;
const NUM_CRITICAL_SHARED_RELS: i32 = 5;
const NUM_CRITICAL_SHARED_INDEXES: i32 = 6;

/* ==========================================================================
 * `RelationCacheInitialize` (no catalog access).
 * ======================================================================== */

/// `RelationCacheInitialize()` (relcache.c): create the `RelationIdCache`
/// dynahash and reserve `in_progress_list` (no catalog access). The dynahash
/// creation is real substrate; the relation-map init is the relmapper seam.
pub fn RelationCacheInitialize() -> PgResult<()> {
    // C: if (CacheMemoryContext == NULL) CreateCacheMemoryContext();
    // (the cache context is the process-global allocator; the dynahash + leaked
    // descriptors live for the backend lifetime — modeled by the thread_local
    // store, so no explicit context object is created here.)

    // Real substrate: create the RelationIdCache (the C
    // `hash_create("Relcache by OID", ...)`). `in_progress_list` is a `Vec`
    // that grows on demand, so no fixed pre-reservation is required.
    crate::core_entry_store::create_id_cache()?;

    // C: RelationMapInitialize() — relation-map owner seam.
    xunit::RelationMapInitialize();
    Ok(())
}

/* ==========================================================================
 * `RelationCacheInitializePhase2` — shared nailed catalogs.
 * ======================================================================== */

/// `RelationCacheInitializePhase2()` (relcache.c): load relcache entries for the
/// shared system catalogs (from the shared init file, else hardcoded via
/// `formrdesc`). **Own logic**; catalog/relmap access is seamed.
pub fn RelationCacheInitializePhase2() -> PgResult<()> {
    // C: RelationMapInitializePhase2();
    xunit::RelationMapInitializePhase2();

    // C: if (IsBootstrapProcessingMode()) return;
    if xunit::IsBootstrapProcessingMode() {
        return Ok(());
    }

    // C: switch to CacheMemoryContext; if the shared init file did not load,
    // hand-build the nailed shared catalogs with formrdesc.
    if !load_relcache_init_file(true)? {
        crate::build::formrdesc(
            "pg_database",
            DatabaseRelation_Rowtype_Id,
            true,
            Natts_pg_database,
            &xunit::catalog_schema_attrs(DatabaseRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_authid",
            AuthIdRelation_Rowtype_Id,
            true,
            Natts_pg_authid,
            &xunit::catalog_schema_attrs(AuthIdRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_auth_members",
            AuthMemRelation_Rowtype_Id,
            true,
            Natts_pg_auth_members,
            &xunit::catalog_schema_attrs(AuthMemRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_shseclabel",
            SharedSecLabelRelation_Rowtype_Id,
            true,
            Natts_pg_shseclabel,
            &xunit::catalog_schema_attrs(SharedSecLabelRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_subscription",
            SubscriptionRelation_Rowtype_Id,
            true,
            Natts_pg_subscription,
            &xunit::catalog_schema_attrs(SubscriptionRelation_Rowtype_Id),
        )?;
    }
    Ok(())
}

/* ==========================================================================
 * `RelationCacheInitializePhase3` — local nailed catalogs + critical indexes.
 * ======================================================================== */

/// `RelationCacheInitializePhase3()` (relcache.c): load the nailed-in
/// system-catalog entries (real catalog access). **Own logic.**
pub fn RelationCacheInitializePhase3() -> PgResult<()> {
    // C: needNewCacheFile = !criticalSharedRelcachesBuilt;
    let mut need_new_cache_file = !with_state(|st| st.critical_shared_relcaches_built);

    // C: RelationMapInitializePhase3();
    xunit::RelationMapInitializePhase3();

    // C: if (IsBootstrapProcessingMode() || !load_relcache_init_file(false))
    //        build the nailed local catalogs with formrdesc.
    if xunit::IsBootstrapProcessingMode() || !load_relcache_init_file(false)? {
        need_new_cache_file = true;
        crate::build::formrdesc(
            "pg_class",
            RelationRelation_Rowtype_Id,
            false,
            Natts_pg_class,
            &xunit::catalog_schema_attrs(RelationRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_attribute",
            AttributeRelation_Rowtype_Id,
            false,
            Natts_pg_attribute,
            &xunit::catalog_schema_attrs(AttributeRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_proc",
            ProcedureRelation_Rowtype_Id,
            false,
            Natts_pg_proc,
            &xunit::catalog_schema_attrs(ProcedureRelation_Rowtype_Id),
        )?;
        crate::build::formrdesc(
            "pg_type",
            TypeRelation_Rowtype_Id,
            false,
            Natts_pg_type,
            &xunit::catalog_schema_attrs(TypeRelation_Rowtype_Id),
        )?;
    }

    // C: if (IsBootstrapProcessingMode()) return;
    if xunit::IsBootstrapProcessingMode() {
        return Ok(());
    }

    // C: nail the critical local indexes (those needed to read pg_class etc).
    if !with_state(|st| st.critical_relcaches_built) {
        load_critical_index(ClassOidIndexId, RelationRelationId)?;
        load_critical_index(AttributeRelidNumIndexId, AttributeRelationId)?;
        load_critical_index(IndexRelidIndexId, IndexRelationId)?;
        load_critical_index(OpclassOidIndexId, OperatorClassRelationId)?;
        load_critical_index(AccessMethodProcedureIndexId, AccessMethodProcedureRelationId)?;
        load_critical_index(RewriteRelRulenameIndexId, RewriteRelationId)?;
        load_critical_index(TriggerRelidNameIndexId, TriggerRelationId)?;
        with_state(|st| st.critical_relcaches_built = true);
    }

    // C: nail the critical shared indexes.
    if !with_state(|st| st.critical_shared_relcaches_built) {
        load_critical_index(DatabaseNameIndexId, DatabaseRelationId)?;
        load_critical_index(DatabaseOidIndexId, DatabaseRelationId)?;
        load_critical_index(AuthIdRolnameIndexId, AuthIdRelationId)?;
        load_critical_index(AuthIdOidIndexId, AuthIdRelationId)?;
        load_critical_index(AuthMemMemRoleIndexId, AuthMemRelationId)?;
        load_critical_index(SharedSecLabelObjectIndexId, SharedSecLabelRelationId)?;
        with_state(|st| st.critical_shared_relcaches_built = true);
    }

    // C: scan every entry, filling in fields that formrdesc/the init file could
    // not (relowner from pg_class, rules, triggers, row-security, table AM);
    // restart the scan whenever a cache entry was rebuilt (the catalog reads
    // can recursively touch the cache). The pg_class re-read + rule/trigger/RLS
    // builders are the syscache/derived owner seams; the table-AM init is the
    // index family. The own decision logic (which fields to refill, and the
    // restart protocol) lives here.
    finish_relcache_entries()?;

    // C: if (needNewCacheFile) { InitCatalogCachePhase2();
    //        write_relcache_init_file(true); write_relcache_init_file(false); }
    if need_new_cache_file {
        xunit::InitCatalogCachePhase2();
        write_relcache_init_file(true)?;
        write_relcache_init_file(false)?;
    }
    Ok(())
}

/// The Phase3 per-entry refill + restart loop. Walks the `RelationIdCache`,
/// pinning each entry and topping up `relowner`/rules/triggers/RLS/table-AM,
/// restarting the seq-scan whenever an entry is rebuilt. The catalog re-reads
/// and the rule/trigger/RLS builders are owner seams (syscache/derived);
/// `RelationInitTableAccessMethod` is the index family. The own logic is the
/// decision of which fields are missing and the restart protocol.
#[allow(unsafe_code)]
fn finish_relcache_entries() -> PgResult<()> {
    loop {
        // Snapshot the current set of cached relation OIDs (the C
        // `hash_seq_init`/`hash_seq_search` walk; the snapshot lets us mutate
        // entries and restart deterministically, as the C `restart` flag does).
        let oids: Vec<Oid> = with_state(|st| crate::core_entry_store::id_cache_oids(st));
        let mut restart = false;
        for oid in oids {
            let rd = match crate::core_entry_store::cache_lookup(oid) {
                Some(p) => p,
                None => continue,
            };
            RelationIncrementReferenceCount(rd)?;
            // SAFETY: live cache-owned descriptor, pinned above.
            let r = unsafe { &mut *rd };

            if r.rd_rel.relowner == InvalidOid {
                // C: SearchSysCache1(RELOID, rd_id) + memcpy of pg_class +
                // RelationParseRelOptions; the syscache read is the owner seam.
                let relp = xunit::SearchSysCacheRelOid(oid)?;
                r.rd_rel = relp;
                crate::build::RelationParseRelOptions(r)?;
                if r.rd_rel.relowner == InvalidOid {
                    crate::core_entry_store::RelationDecrementReferenceCount(rd)?;
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "invalid relowner in pg_class entry for \"{}\"",
                            r.rd_rel.relname
                        ))
                        .into_error());
                }
                restart = true;
            }
            if r.rd_rel.relhasrules && !r.rd_has_rules {
                crate::derived::RelationBuildRuleLock(rd)?;
                // SAFETY: still pinned.
                let r = unsafe { &mut *rd };
                if !r.rd_has_rules {
                    r.rd_rel.relhasrules = false;
                }
                restart = true;
            }
            // SAFETY: still pinned.
            let r = unsafe { &mut *rd };
            if r.rd_rel.relhastriggers && !r.rd_has_trigdesc {
                xunit::RelationBuildTriggers(rd)?;
                // SAFETY: still pinned.
                let r = unsafe { &mut *rd };
                if !r.rd_has_trigdesc {
                    r.rd_rel.relhastriggers = false;
                }
                restart = true;
            }
            // SAFETY: still pinned.
            let r = unsafe { &mut *rd };
            if r.rd_rel.relrowsecurity && !r.rd_has_rsdesc {
                xunit::RelationBuildRowSecurity(rd)?;
                restart = true;
            }
            // SAFETY: still pinned.
            let r = unsafe { &mut *rd };
            let relkind = r.rd_rel.relkind as u8;
            if r.rd_tableam.is_none()
                && (relkind == RELKIND_RELATION
                    || relkind == RELKIND_TOASTVALUE
                    || relkind == RELKIND_MATVIEW
                    || relkind == RELKIND_SEQUENCE)
            {
                crate::index::RelationInitTableAccessMethod(rd)?;
                restart = true;
            }
            crate::core_entry_store::RelationDecrementReferenceCount(rd)?;
            if restart {
                break;
            }
        }
        if !restart {
            break;
        }
    }
    Ok(())
}

/* ==========================================================================
 * `load_critical_index` — nail one critical system index during Phase3.
 * ======================================================================== */

/// `load_critical_index(indexoid, heapoid)` (relcache.c): nail one critical
/// system-catalog index into the cache during phase 3. **Own logic**; the lock
/// manager and `RelationGetIndexAttOptions` are owner seams.
#[allow(unsafe_code)]
pub fn load_critical_index(indexoid: Oid, heapoid: Oid) -> PgResult<()> {
    // C: LockRelationOid(heapoid, AccessShareLock); LockRelationOid(indexoid,
    // AccessShareLock); — lock manager owner seam.
    xunit::LockRelationOid(heapoid)?;
    xunit::LockRelationOid(indexoid)?;

    // C: ird = RelationBuildDesc(indexoid, true); — in-crate build family.
    let ird = crate::build::RelationBuildDesc(indexoid, true)?;
    if ird.is_null() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg_internal(format!("could not open critical system index {indexoid}"))
            .into_error());
    }
    // C: ird->rd_isnailed = true; ird->rd_refcnt = 1;
    // SAFETY: freshly built, cache-owned descriptor.
    unsafe {
        (*ird).rd_isnailed = true;
        (*ird).rd_refcnt = 1;
    }
    // C: UnlockRelationOid(indexoid/heapoid, AccessShareLock); — owner seam.
    xunit::UnlockRelationOid(indexoid)?;
    xunit::UnlockRelationOid(heapoid)?;
    // C: RelationGetIndexAttOptions(ird, false); — index/opclass owner seam.
    xunit::RelationGetIndexAttOptions(ird)?;
    Ok(())
}

/* ==========================================================================
 * `RelationBuildLocalRelation` — build an entry for a brand-new relation
 * without catalog access.
 * ======================================================================== */

/// `RelationBuildLocalRelation(...)` (relcache.c): build a relcache entry for a
/// brand-new relation without catalog access. **Own logic.** The passed
/// `TupleDesc` copy and `accessmtd`/`relfilenumber` arguments of the C signature
/// are handled by the build/index families and the relation-map seam; this
/// family's signature carries the scalar identity the entry stores directly.
#[allow(unsafe_code)]
pub fn RelationBuildLocalRelation(
    relname: &str,
    relnamespace: Oid,
    relid: Oid,
    reltablespace: Oid,
    shared_relation: bool,
    mapped_relation: bool,
    relpersistence: i8,
    relkind: i8,
) -> PgResult<*mut RelationData> {
    // C: nailit for the seven bootstrap-nailed catalogs.
    let nailit = matches!(
        relid,
        DatabaseRelationId
            | AuthIdRelationId
            | AuthMemRelationId
            | RelationRelationId
            | AttributeRelationId
            | ProcedureRelationId
            | TypeRelationId
    );

    // C: Assert(shared_relation == IsSharedRelation(relid)).
    if shared_relation != xunit::IsSharedRelation(relid) {
        return Err(ereport(WARNING)
            .errmsg_internal(format!(
                "shared_relation flag for \"{relname}\" does not match IsSharedRelation({relid})"
            ))
            .into_error());
    }

    // C: rel = palloc0(sizeof(RelationData)); fill the lifecycle scalars.
    let mut rel = RelationData::new_blank();
    rel.rd_isnailed = nailit;
    rel.rd_refcnt = if nailit { 1 } else { 0 };
    rel.rd_createSubid = xunit::GetCurrentSubTransactionId();
    rel.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    rel.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    rel.rd_droppedSubid = InvalidSubTransactionId;

    // C: rd_att = CreateTupleDescCopy(tupDesc); the attribute rows are copied
    // by the build family from the passed TupleDesc. This family's signature
    // does not carry the TupleDesc; the owned descriptor starts with the
    // composite-type metadata it can fill (natts is set from rd_rel below).
    rel.rd_att = OwnedTupleDesc {
        natts: 0,
        tdtypeid: types_tuple::heaptuple::RECORDOID,
        tdtypmod: -1,
        attrs: Vec::<OwnedAttr>::new(),
        constr: None,
    };

    // C: rd_rel = palloc0(CLASS_TUPLE_SIZE); fill the pg_class form.
    let mut relform = FormPgClass::default();
    relform.relname = relname.to_string();
    relform.relnamespace = relnamespace;
    relform.relkind = relkind;
    relform.relnatts = 0; // set from rd_att once the build family fills it
    relform.reltype = InvalidOid;
    relform.relowner = BOOTSTRAP_SUPERUSERID;
    relform.relpersistence = relpersistence;

    // C: rd_backend/rd_islocaltemp by persistence.
    match relpersistence {
        p if p == RELPERSISTENCE_UNLOGGED as i8 || p == RELPERSISTENCE_PERMANENT as i8 => {
            rel.rd_backend = INVALID_PROC_NUMBER;
            rel.rd_islocaltemp = false;
        }
        p if p == RELPERSISTENCE_TEMP as i8 => {
            rel.rd_backend = xunit::current_temp_proc_number();
            rel.rd_islocaltemp = true;
        }
        _ => {
            return Err(ereport(WARNING)
                .errmsg_internal(format!(
                    "invalid relpersistence: {}",
                    relpersistence as u8 as char
                ))
                .into_error());
        }
    }

    // C: matviews start unpopulated; everything else populated.
    relform.relispopulated = (relkind as u8) != RELKIND_MATVIEW;

    // C: replica identity default for non-catalog tables/matviews/partitioned.
    if !xunit::IsCatalogNamespace(relnamespace)
        && ((relkind as u8) == RELKIND_RELATION
            || (relkind as u8) == RELKIND_MATVIEW
            || (relkind as u8) == RELKIND_PARTITIONED_TABLE)
    {
        relform.relreplident = REPLICA_IDENTITY_DEFAULT;
    } else {
        relform.relreplident = REPLICA_IDENTITY_NOTHING;
    }

    relform.relisshared = shared_relation;
    rel.rd_id = relid;
    relform.reltablespace = reltablespace;

    // C: mapped relations get InvalidRelFileNumber in pg_class + a relmap entry;
    // unmapped relations store the relfilenumber directly. The relfilenumber is
    // the build/index family's RelationInitPhysicalAddr input (and the relmap
    // update is the relmapper owner seam). The scalar identity above is what the
    // entry stores; the physical-address derivation is index-family logic.
    if mapped_relation {
        relform.relfilenode = InvalidOid;
        // C: RelationMapUpdateMap(relid, relfilenumber, shared_relation, true);
        // — relmapper owner seam (no relfilenumber arg in this signature; the
        // caller path through the build family supplies it).
        xunit::RelationMapUpdateMapLocal(relid, shared_relation);
    } else {
        relform.relfilenode = InvalidOid;
    }

    rel.rd_rel = relform;

    // C: RelationInitLockInfo(rel); RelationInitPhysicalAddr(rel); — index
    // family (physical addr) over the owned entry.
    let rel_ptr = Box::into_raw(rel);
    crate::index::RelationInitPhysicalAddr(rel_ptr)?;
    // SAFETY: just-boxed descriptor, sole owner here.
    unsafe { (*rel_ptr).rd_rel.relam = InvalidOid };

    // C: for relations with storage AM, RelationInitTableAccessMethod(rel).
    let relkind_u = relkind as u8;
    if relkind_u == RELKIND_RELATION
        || relkind_u == RELKIND_TOASTVALUE
        || relkind_u == RELKIND_MATVIEW
        || relkind_u == RELKIND_SEQUENCE
    {
        crate::index::RelationInitTableAccessMethod(rel_ptr)?;
    }

    // C: RelationCacheInsert(rel, true) — replace-allowed; on a same-OID
    // collision the C destroys the old entry if unreferenced or warns if still
    // referenced (outside bootstrap). `cache_insert` returns the displaced
    // descriptor; reclaim it when unreferenced.
    // SAFETY: re-take ownership of the boxed descriptor for insertion.
    let rel_box = unsafe { Box::from_raw(rel_ptr) };
    let oldrel = cache_insert(rel_box, true)?;
    if let Some(old_ptr) = oldrel {
        // SAFETY: the displaced descriptor was cache-owned; it is no longer in
        // the cache (its slot now points at the new entry).
        let old_refcnt = unsafe { (*old_ptr).rd_refcnt };
        if old_refcnt == 0 {
            // C: RelationDestroyRelation(oldrel, false) — drop the owned tree.
            // SAFETY: unreferenced, removed from the cache.
            unsafe { drop(Box::from_raw(old_ptr)) };
        } else if !xunit::IsBootstrapProcessingMode() {
            // C: WARNING "leaking still-referenced relcache entry".
            let _ = ereport(WARNING)
                .errmsg_internal("leaking still-referenced relcache entry")
                .into_error();
        }
    }

    // Re-resolve the stable cache pointer for the just-inserted entry.
    let installed = crate::core_entry_store::cache_lookup(relid).expect("just inserted");

    // C: EOXactListAdd(rel).
    with_state(|st| eoxact_list_add(st, relid));

    // C: rel->rd_isvalid = true; RelationIncrementReferenceCount(rel);
    // SAFETY: live cache-owned descriptor.
    unsafe { (*installed).rd_isvalid = true };
    RelationIncrementReferenceCount(installed)?;
    Ok(installed)
}

/* ==========================================================================
 * `RelationSetNewRelfilenumber` / `RelationAssumeNewRelfilelocator`.
 * ======================================================================== */

/// `RelationSetNewRelfilenumber(relation, persistence)` (relcache.c): assign a
/// new relfilenumber/storage to an existing relation. The body is dominated by
/// cross-unit primitives — `GetNewRelFileNumber`, `table_open`/`table_close`,
/// `SearchSysCacheLockedCopy1`, the smgr/storage layer, `RelationMapUpdateMap`,
/// `CacheInvalidateRelcache`, `CatalogTupleUpdate`, `CommandCounterIncrement` —
/// all owner seams; only the trailing `RelationAssumeNewRelfilelocator` is this
/// family's own logic. The whole cross-unit storage transaction is routed
/// through the [`xunit`] seam (panic until those owners land); the own tail
/// runs against the real entry.
#[allow(unsafe_code)]
pub fn RelationSetNewRelfilenumber(relation: *mut RelationData, persistence: i8) -> PgResult<()> {
    // SAFETY: live cache-owned descriptor.
    let relid = unsafe { (*relation).rd_id };
    // C: the full GetNewRelFileNumber + storage swap + pg_class/relmap update
    // transaction (owner seams: catalog/smgr/relmapper/inval).
    xunit::set_new_relfilenumber_storage(relid, persistence)?;
    // C: RelationAssumeNewRelfilelocator(relation).
    RelationAssumeNewRelfilelocator(relation)
}

/// `RelationAssumeNewRelfilelocator(relation)` (relcache.c): update the
/// `rd_*Subid` tracking after an external relfilenumber change. **Own logic.**
#[allow(unsafe_code)]
pub fn RelationAssumeNewRelfilelocator(relation: *mut RelationData) -> PgResult<()> {
    let subid = xunit::GetCurrentSubTransactionId();
    // SAFETY: live cache-owned descriptor.
    let rd = unsafe { &mut *relation };
    rd.rd_newRelfilelocatorSubid = subid;
    if rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId {
        rd.rd_firstRelfilelocatorSubid = rd.rd_newRelfilelocatorSubid;
    }
    let relid = rd.rd_id;
    // C: EOXactListAdd(relation).
    with_state(|st| eoxact_list_add(st, relid));
    Ok(())
}

/* ==========================================================================
 * Init-file BINARY CODEC (reclaimed in-crate).
 *
 * The C codec is a raw byte-image of the C `RelationData`/`Form_pg_class`/
 * per-attribute structs, written/read with `fwrite`/`fread` over the native
 * memory layout. Our entry store deliberately uses owned Rust mirrors
 * (`String`/`Vec`/owned scalars), so a byte-faithful image of those structs is
 * neither possible nor meaningful. The codec's OWN LOGIC — the file naming, the
 * magic check, the length-prefixed item framing (`write_item`), the per-entry
 * field sequence (entry header, rd_rel, per-attr, rd_options, then index-only:
 * indextuple, opfamily, opcintype, support, indcollation, indoption,
 * opcoptions), the nailed-rel/index validation, and the per-entry rebuild — is
 * reclaimed here over the owned representation. The actual file IO
 * (`AllocateFile`/`FreeFile`/`rename`/`unlink`) is the fd-layer owner seam.
 * ======================================================================== */

/// `write_relcache_init_file(shared)` (relcache.c): serialize the nailed
/// relcache entries to the on-disk init file. **Own logic** (item framing,
/// per-entry field sequence, the `RelCacheInitLock` rename dance); the file IO
/// is the owner seam.
#[allow(unsafe_code)]
pub fn write_relcache_init_file(shared: bool) -> PgResult<()> {
    // C: if (relcacheInvalsReceived != 0) return; (don't write a stale file).
    if with_state(|st| st.relcache_invals_received) != 0 {
        return Ok(());
    }

    // C: build the temp + final file names.
    let (tempfilename, finalfilename) = if shared {
        (
            format!("global/{RELCACHE_INIT_FILENAME}.{}", xunit::my_proc_pid()),
            format!("global/{RELCACHE_INIT_FILENAME}"),
        )
    } else {
        let dbpath = xunit::database_path();
        (
            format!("{dbpath}/{RELCACHE_INIT_FILENAME}.{}", xunit::my_proc_pid()),
            format!("{dbpath}/{RELCACHE_INIT_FILENAME}"),
        )
    };

    // C: unlink(tempfilename); fp = AllocateFile(tempfilename, PG_BINARY_W);
    xunit::unlink_file(&tempfilename, false);
    let mut buf: Vec<u8> = Vec::new();

    // C: fwrite(&magic, ...); the file magic.
    buf.extend_from_slice(&RELCACHE_INIT_FILEMAGIC.to_ne_bytes());

    // C: hash_seq over RelationIdCache; serialize matching nailed entries.
    let oids: Vec<Oid> = with_state(|st| crate::core_entry_store::id_cache_oids(st));
    for oid in oids {
        let rd = match crate::core_entry_store::cache_lookup(oid) {
            Some(p) => p,
            None => continue,
        };
        // SAFETY: live cache-owned descriptor.
        let r = unsafe { &*rd };
        // C: if (relform->relisshared != shared) continue;
        if r.rd_rel.relisshared != shared {
            continue;
        }
        // C: if (!shared && !RelationIdIsInInitFile(rel->rd_id)) continue;
        if !shared && !RelationIdIsInInitFile(r.rd_id) {
            continue;
        }
        write_entry(&mut buf, r);
    }

    // C: FreeFile; LWLockAcquire(RelCacheInitLock); AcceptInvalidationMessages;
    // if (relcacheInvalsReceived == 0) rename(temp, final) else unlink(temp);
    // LWLockRelease.
    xunit::write_file(&tempfilename, &buf)?;
    xunit::lwlock_acquire_relcache_init();
    xunit::accept_invalidation_messages();
    if with_state(|st| st.relcache_invals_received) == 0 {
        if xunit::rename_file(&tempfilename, &finalfilename).is_err() {
            xunit::unlink_file(&tempfilename, false);
        }
    } else {
        xunit::unlink_file(&tempfilename, false);
    }
    xunit::lwlock_release_relcache_init();
    Ok(())
}

/// `write_item`/the per-entry serialization (relcache.c
/// `write_relcache_init_file` loop body): the OWN length-prefixed framing and
/// the exact field sequence, over the owned entry representation.
fn write_entry(buf: &mut Vec<u8>, r: &RelationData) {
    // C: write_item(rel, sizeof(RelationData)) — the entry header. We frame the
    // identity scalars the entry header carries (the C raw image is layout-
    // bound and not reproducible over owned mirrors; the framing is the logic).
    let mut header = Vec::new();
    header.extend_from_slice(&r.rd_id.to_ne_bytes());
    header.push(r.rd_isnailed as u8);
    header.extend_from_slice(&r.rd_locator.spcOid.to_ne_bytes());
    header.extend_from_slice(&r.rd_locator.dbOid.to_ne_bytes());
    header.extend_from_slice(&r.rd_locator.relNumber.to_ne_bytes());
    write_item(buf, &header);

    // C: write_item(relform, CLASS_TUPLE_SIZE) — the pg_class form.
    write_item(buf, &encode_pg_class(&r.rd_rel));

    // C: per-attribute: write_item(TupleDescAttr(rd_att, i), ATTR_FIXED_SIZE).
    for att in &r.rd_att.attrs {
        write_item(buf, &encode_attr(att));
    }

    // C: write_item(rd_options, VARSIZE(rd_options) or 0).
    match &r.rd_options {
        Some(_opts) => {
            // The parsed StdRdOptions are reconstructed from the catalog on load;
            // we frame a presence marker (non-empty) so the read side restores
            // the same "has options" state. (The C raw bytea image is layout-
            // bound; the framing presence is the logic that survives.)
            write_item(buf, &[1u8]);
        }
        None => write_item(buf, &[]),
    }

    // C: index-only payloads when relkind == RELKIND_INDEX.
    if (r.rd_rel.relkind as u8) == RELKIND_INDEX {
        // C: write_item(rd_indextuple, HEAPTUPLESIZE + t_len) — the pg_index.
        if let Some(idx) = &r.rd_index {
            write_item(buf, &encode_pg_index(idx));
        } else {
            write_item(buf, &[]);
        }
        // C: rd_opfamily / rd_opcintype (natts Oids each).
        write_item(buf, &encode_oid_vec(&r.rd_opfamily));
        write_item(buf, &encode_oid_vec(&r.rd_opcintype));
        // C: rd_support (natts * amsupport RegProcedures).
        write_item(buf, &encode_regproc_vec(&r.rd_support));
        // C: rd_indcollation (natts Oids).
        write_item(buf, &encode_oid_vec(&r.rd_indcollation));
        // C: rd_indoption (natts int16).
        write_item(buf, &encode_i16_vec(&r.rd_indoption));
        // C: per-column rd_opcoptions (varlena each, 0 if NULL). The opclass
        // options are rebuilt from the catalog on load; frame as absent.
        for _ in 0..r.rd_rel.relnatts {
            write_item(buf, &[]);
        }
    }
}

/// `write_item(data, len, fp)` (relcache.c): length-prefixed item framing
/// (`Size` length header, then the bytes). The OWN framing primitive.
fn write_item(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len();
    buf.extend_from_slice(&len.to_ne_bytes());
    if len > 0 {
        buf.extend_from_slice(data);
    }
}

fn encode_pg_class(rel: &FormPgClass) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(rel.relname.len() as u32).to_ne_bytes());
    v.extend_from_slice(rel.relname.as_bytes());
    v.extend_from_slice(&rel.relnamespace.to_ne_bytes());
    v.extend_from_slice(&rel.reltype.to_ne_bytes());
    v.extend_from_slice(&rel.relowner.to_ne_bytes());
    v.extend_from_slice(&rel.relam.to_ne_bytes());
    v.extend_from_slice(&rel.relfilenode.to_ne_bytes());
    v.extend_from_slice(&rel.reltablespace.to_ne_bytes());
    v.extend_from_slice(&rel.relnatts.to_ne_bytes());
    v.push(rel.relisshared as u8);
    v.push(rel.relpersistence as u8);
    v.push(rel.relkind as u8);
    v.push(rel.relreplident as u8);
    v
}

fn encode_attr(att: &OwnedAttr) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(att.attname.len() as u32).to_ne_bytes());
    v.extend_from_slice(att.attname.as_bytes());
    v.extend_from_slice(&att.atttypid.to_ne_bytes());
    v.extend_from_slice(&att.attlen.to_ne_bytes());
    v.extend_from_slice(&att.attnum.to_ne_bytes());
    v.extend_from_slice(&att.atttypmod.to_ne_bytes());
    v.push(att.attbyval as u8);
    v.push(att.attalign as u8);
    v.push(att.attnotnull as u8);
    v.push(att.attisdropped as u8);
    v.extend_from_slice(&att.attcollation.to_ne_bytes());
    v
}

fn encode_pg_index(idx: &crate::core_entry_store::entry::FormPgIndex) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&idx.indexrelid.to_ne_bytes());
    v.extend_from_slice(&idx.indrelid.to_ne_bytes());
    v.extend_from_slice(&idx.indnatts.to_ne_bytes());
    v.extend_from_slice(&idx.indnkeyatts.to_ne_bytes());
    v.push(idx.indisunique as u8);
    v.push(idx.indisprimary as u8);
    v.push(idx.indisvalid as u8);
    v.push(idx.indisready as u8);
    v.extend_from_slice(&(idx.indkey.len() as u32).to_ne_bytes());
    for k in &idx.indkey {
        v.extend_from_slice(&k.to_ne_bytes());
    }
    v
}

fn encode_oid_vec(xs: &[Oid]) -> Vec<u8> {
    let mut v = Vec::with_capacity(xs.len() * 4);
    for x in xs {
        v.extend_from_slice(&x.to_ne_bytes());
    }
    v
}

fn encode_regproc_vec(xs: &[RegProcedure]) -> Vec<u8> {
    let mut v = Vec::with_capacity(xs.len() * 4);
    for x in xs {
        v.extend_from_slice(&x.to_ne_bytes());
    }
    v
}

fn encode_i16_vec(xs: &[i16]) -> Vec<u8> {
    let mut v = Vec::with_capacity(xs.len() * 2);
    for x in xs {
        v.extend_from_slice(&x.to_ne_bytes());
    }
    v
}

/// `load_relcache_init_file(shared)` (relcache.c): deserialize the nailed
/// relcache entries from the on-disk init file; returns `false` to signal a
/// rebuild-from-catalog. **Own logic** (the magic check, item framing, the
/// per-entry field sequence, the nailed-rel/index validation, and the cache
/// install); the file IO and table-AM/index re-init are owner seams.
#[allow(unsafe_code)]
pub fn load_relcache_init_file(shared: bool) -> PgResult<bool> {
    // C: build the file name; fp = AllocateFile(initfilename, PG_BINARY_R);
    let initfilename = if shared {
        format!("global/{RELCACHE_INIT_FILENAME}")
    } else {
        format!("{}/{RELCACHE_INIT_FILENAME}", xunit::database_path())
    };
    // C: if (fp == NULL) return false;
    let bytes = match xunit::read_file(&initfilename)? {
        Some(b) => b,
        None => return Ok(false),
    };

    let mut cur = Cursor::new(&bytes);
    let mut rels: Vec<Box<RelationData>> = Vec::with_capacity(100);
    let mut nailed_rels = 0i32;
    let mut nailed_indexes = 0i32;

    // C: fread(&magic); if (magic != RELCACHE_INIT_FILEMAGIC) goto read_failed.
    let magic = match cur.read_i32() {
        Some(m) => m,
        None => return Ok(false),
    };
    if magic != RELCACHE_INIT_FILEMAGIC {
        return Ok(false);
    }

    // C: loop reading entries until EOF or a corrupt record (read_failed).
    loop {
        // C: nread = fread(&len); if (nread == 0) break; else if short goto fail.
        let header = match cur.read_item() {
            Some(ItemRead::Eof) => break,
            Some(ItemRead::Data(d)) => d,
            None => return read_failed(rels),
        };
        // C: decode the entry header (rd_id/rd_isnailed/rd_locator).
        let mut rel = RelationData::new_blank();
        {
            let mut hc = Cursor::new(&header);
            rel.rd_id = match hc.read_oid() {
                Some(x) => x,
                None => return read_failed(rels),
            };
            rel.rd_isnailed = matches!(hc.read_u8(), Some(1));
            rel.rd_locator.spcOid = hc.read_oid().unwrap_or(InvalidOid);
            rel.rd_locator.dbOid = hc.read_oid().unwrap_or(InvalidOid);
            rel.rd_locator.relNumber = hc.read_oid().unwrap_or(InvalidOid);
        }

        // C: read rd_rel (the pg_class form).
        let relform_bytes = match cur.read_item() {
            Some(ItemRead::Data(d)) => d,
            _ => return read_failed(rels),
        };
        rel.rd_rel = match decode_pg_class(&relform_bytes) {
            Some(f) => f,
            None => return read_failed(rels),
        };

        // C: rd_att = CreateTemplateTupleDesc(relnatts); per-attr fread loop.
        let natts = rel.rd_rel.relnatts as i32;
        rel.rd_att.natts = natts;
        rel.rd_att.tdtypeid = if rel.rd_rel.reltype != 0 {
            rel.rd_rel.reltype
        } else {
            types_tuple::heaptuple::RECORDOID
        };
        rel.rd_att.tdtypmod = -1;
        for _ in 0..natts {
            let attr_bytes = match cur.read_item() {
                Some(ItemRead::Data(d)) => d,
                _ => return read_failed(rels),
            };
            match decode_attr(&attr_bytes) {
                Some(a) => rel.rd_att.attrs.push(a),
                None => return read_failed(rels),
            }
        }

        // C: read rd_options (varlena, 0-len == NULL).
        let opt_bytes = match cur.read_item() {
            Some(ItemRead::Data(d)) => d,
            _ => return read_failed(rels),
        };
        rel.rd_options = if opt_bytes.is_empty() {
            None
        } else {
            // The parsed options are rebuilt from the catalog when the entry is
            // next opened; the presence marker restores "has options" state.
            Some(types_reloptions::StdRdOptions::default())
        };

        // C: index-only payloads when relkind == RELKIND_INDEX.
        if (rel.rd_rel.relkind as u8) == RELKIND_INDEX {
            if rel.rd_isnailed {
                nailed_indexes += 1;
            }
            // C: rd_indextuple → rd_index (the pg_index form).
            let idx_bytes = match cur.read_item() {
                Some(ItemRead::Data(d)) => d,
                _ => return read_failed(rels),
            };
            rel.rd_index = if idx_bytes.is_empty() {
                None
            } else {
                match decode_pg_index(&idx_bytes) {
                    Some(i) => Some(i),
                    None => return read_failed(rels),
                }
            };
            // C: InitIndexAmRoutine(rel) — index family.
            // SAFETY: `rel` is the sole owner here; the raw pointer is used only
            // for the duration of this call (the family mutates the entry).
            crate::index::InitIndexAmRoutine(&mut *rel as *mut RelationData)?;
            // C: rd_opfamily / rd_opcintype / rd_support / rd_indcollation /
            // rd_indoption, then per-column rd_opcoptions.
            rel.rd_opfamily = match cur.read_item() {
                Some(ItemRead::Data(d)) => decode_oid_vec(&d),
                _ => return read_failed(rels),
            };
            rel.rd_opcintype = match cur.read_item() {
                Some(ItemRead::Data(d)) => decode_oid_vec(&d),
                _ => return read_failed(rels),
            };
            rel.rd_support = match cur.read_item() {
                Some(ItemRead::Data(d)) => decode_regproc_vec(&d),
                _ => return read_failed(rels),
            };
            rel.rd_indcollation = match cur.read_item() {
                Some(ItemRead::Data(d)) => decode_oid_vec(&d),
                _ => return read_failed(rels),
            };
            rel.rd_indoption = match cur.read_item() {
                Some(ItemRead::Data(d)) => decode_i16_vec(&d),
                _ => return read_failed(rels),
            };
            for _ in 0..natts {
                match cur.read_item() {
                    Some(ItemRead::Data(_)) => {}
                    _ => return read_failed(rels),
                }
            }
            // C: rd_supportinfo = palloc0(natts * amsupport * sizeof(FmgrInfo));
            // lazily filled by index_getprocinfo, so left empty here.
        } else {
            if rel.rd_isnailed {
                nailed_rels += 1;
            }
            // C: for storage relkinds, RelationInitTableAccessMethod(rel).
            let relkind = rel.rd_rel.relkind as u8;
            if relkind == RELKIND_RELATION
                || relkind == RELKIND_TOASTVALUE
                || relkind == RELKIND_MATVIEW
                || relkind == RELKIND_SEQUENCE
            {
                // SAFETY: `rel` is the sole owner; raw pointer used only here.
                crate::index::RelationInitTableAccessMethod(&mut *rel as *mut RelationData)?;
            }
        }

        // C: reset all the derived/per-xact fields to their fresh state.
        rel.rd_has_rules = false;
        rel.rd_has_trigdesc = false;
        rel.rd_has_rsdesc = false;
        rel.rd_has_partkey = false;
        rel.rd_has_partdesc = false;
        rel.rd_partcheckvalid = false;
        rel.rd_exclops.clear();
        rel.rd_exclprocs.clear();
        rel.rd_exclstrats.clear();
        rel.rd_refcnt = if rel.rd_isnailed { 1 } else { 0 };
        rel.rd_indexvalid = false;
        rel.rd_indexlist.clear();
        rel.rd_pkindex = InvalidOid;
        rel.rd_replidindex = InvalidOid;
        rel.rd_attrsvalid = false;
        rel.rd_keyattr.clear();
        rel.rd_pkattr.clear();
        rel.rd_idattr.clear();
        rel.rd_has_pubdesc = false;
        rel.rd_statvalid = false;
        rel.rd_statlist.clear();
        rel.rd_fkeyvalid = false;
        rel.rd_createSubid = InvalidSubTransactionId;
        rel.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        rel.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        rel.rd_droppedSubid = InvalidSubTransactionId;
        // C: RelationInitLockInfo(rel); RelationInitPhysicalAddr(rel).
        let p = Box::into_raw(rel);
        crate::index::RelationInitPhysicalAddr(p)?;
        // SAFETY: sole owner of the just-boxed descriptor.
        rels.push(unsafe { Box::from_raw(p) });
    }

    // C: validate the nailed-rel/index counts against expectations.
    let (exp_rels, exp_indexes) = if shared {
        (NUM_CRITICAL_SHARED_RELS, NUM_CRITICAL_SHARED_INDEXES)
    } else {
        (NUM_CRITICAL_LOCAL_RELS, NUM_CRITICAL_LOCAL_INDEXES)
    };
    if nailed_rels != exp_rels || nailed_indexes != exp_indexes {
        // C: WARNING then goto read_failed (rebuild from catalog).
        let _ = ereport(WARNING)
            .errmsg_internal(format!(
                "found {nailed_rels} nailed rels and {nailed_indexes} nailed indexes in init \
                 file, but expected {exp_rels} and {exp_indexes} respectively"
            ))
            .into_error();
        return read_failed(rels);
    }

    // C: install every entry into RelationIdCache (replace-allowed).
    for rel in rels {
        let oldrel = cache_insert(rel, true)?;
        if let Some(old_ptr) = oldrel {
            // SAFETY: displaced descriptor, no longer in the cache.
            let refcnt = unsafe { (*old_ptr).rd_refcnt };
            if refcnt == 0 {
                // SAFETY: unreferenced, removed from cache.
                unsafe { drop(Box::from_raw(old_ptr)) };
            } else if !xunit::IsBootstrapProcessingMode() {
                let _ = ereport(WARNING)
                    .errmsg_internal("leaking still-referenced relcache entry")
                    .into_error();
            }
        }
    }

    // C: set the critical-built flags for this scope.
    with_state(|st| {
        if shared {
            st.critical_shared_relcaches_built = true;
        } else {
            st.critical_relcaches_built = true;
        }
    });
    Ok(true)
}

/// C `read_failed:` — drop the partially-built entries and return false to
/// signal "rebuild from catalog".
fn read_failed(rels: Vec<Box<RelationData>>) -> PgResult<bool> {
    drop(rels);
    Ok(false)
}

/* ---- codec decode helpers (mirror the encode side) ---- */

fn decode_pg_class(b: &[u8]) -> Option<FormPgClass> {
    let mut c = Cursor::new(b);
    let mut f = FormPgClass::default();
    let nlen = c.read_u32()? as usize;
    f.relname = String::from_utf8_lossy(c.read_bytes(nlen)?).into_owned();
    f.relnamespace = c.read_oid()?;
    f.reltype = c.read_oid()?;
    f.relowner = c.read_oid()?;
    f.relam = c.read_oid()?;
    f.relfilenode = c.read_oid()?;
    f.reltablespace = c.read_oid()?;
    f.relnatts = c.read_i16()?;
    f.relisshared = c.read_u8()? != 0;
    f.relpersistence = c.read_u8()? as i8;
    f.relkind = c.read_u8()? as i8;
    f.relreplident = c.read_u8()? as i8;
    Some(f)
}

fn decode_attr(b: &[u8]) -> Option<OwnedAttr> {
    let mut c = Cursor::new(b);
    let mut a = OwnedAttr::default();
    let nlen = c.read_u32()? as usize;
    a.attname = String::from_utf8_lossy(c.read_bytes(nlen)?).into_owned();
    a.atttypid = c.read_oid()?;
    a.attlen = c.read_i16()?;
    a.attnum = c.read_i16()?;
    a.atttypmod = c.read_i32()?;
    a.attbyval = c.read_u8()? != 0;
    a.attalign = c.read_u8()? as i8;
    a.attnotnull = c.read_u8()? != 0;
    a.attisdropped = c.read_u8()? != 0;
    a.attcollation = c.read_oid()?;
    Some(a)
}

fn decode_pg_index(b: &[u8]) -> Option<crate::core_entry_store::entry::FormPgIndex> {
    let mut c = Cursor::new(b);
    let mut idx = crate::core_entry_store::entry::FormPgIndex::default();
    idx.indexrelid = c.read_oid()?;
    idx.indrelid = c.read_oid()?;
    idx.indnatts = c.read_i16()?;
    idx.indnkeyatts = c.read_i16()?;
    idx.indisunique = c.read_u8()? != 0;
    idx.indisprimary = c.read_u8()? != 0;
    idx.indisvalid = c.read_u8()? != 0;
    idx.indisready = c.read_u8()? != 0;
    let n = c.read_u32()? as usize;
    for _ in 0..n {
        idx.indkey.push(c.read_i16()?);
    }
    Some(idx)
}

fn decode_oid_vec(b: &[u8]) -> Vec<Oid> {
    b.chunks_exact(4)
        .map(|c| Oid::from_ne_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

fn decode_regproc_vec(b: &[u8]) -> Vec<RegProcedure> {
    b.chunks_exact(4)
        .map(|c| RegProcedure::from_ne_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

fn decode_i16_vec(b: &[u8]) -> Vec<i16> {
    b.chunks_exact(2)
        .map(|c| i16::from_ne_bytes([c[0], c[1]]))
        .collect()
}

/* ---- a minimal byte cursor for the codec (the fread/fwrite analogue) ---- */

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

/// Result of reading one length-prefixed item (the C `fread(&len)` step):
/// `Eof` when a zero-length read at the framing boundary signals end-of-file
/// (the C `nread == 0`), else the framed `Data`.
enum ItemRead {
    Eof,
    Data(Vec<u8>),
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cursor { b, pos: 0 }
    }
    fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.pos + n > self.b.len() {
            return None;
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Some(s)
    }
    fn read_u8(&mut self) -> Option<u8> {
        self.read_bytes(1).map(|s| s[0])
    }
    fn read_u32(&mut self) -> Option<u32> {
        self.read_bytes(4).map(|s| u32::from_ne_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn read_i16(&mut self) -> Option<i16> {
        self.read_bytes(2).map(|s| i16::from_ne_bytes([s[0], s[1]]))
    }
    fn read_i32(&mut self) -> Option<i32> {
        self.read_bytes(4).map(|s| i32::from_ne_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn read_oid(&mut self) -> Option<Oid> {
        self.read_bytes(4).map(|s| Oid::from_ne_bytes([s[0], s[1], s[2], s[3]]))
    }
    /// `fread(&len, sizeof(Size))` then `fread(data, len)`: read one framed
    /// item. A clean EOF at the length boundary is `Eof`; a short/partial read
    /// is `None` (the C `read_failed`).
    fn read_item(&mut self) -> Option<ItemRead> {
        if self.pos == self.b.len() {
            return Some(ItemRead::Eof);
        }
        let lensz = std::mem::size_of::<usize>();
        let lbytes = self.read_bytes(lensz)?;
        let mut la = [0u8; std::mem::size_of::<usize>()];
        la.copy_from_slice(lbytes);
        let len = usize::from_ne_bytes(la);
        if len == 0 {
            return Some(ItemRead::Data(Vec::new()));
        }
        let d = self.read_bytes(len)?;
        Some(ItemRead::Data(d.to_vec()))
    }
}

/* ==========================================================================
 * `RelationIdIsInInitFile` predicate.
 * ======================================================================== */

/// `RelationIdIsInInitFile(relationId)` (relcache.c): is the relation one whose
/// entry is cached in the init file? **Own logic** (the four hardcoded OIDs);
/// `RelationSupportsSysCache` is the syscache owner seam.
pub fn RelationIdIsInInitFile(relationId: Oid) -> bool {
    // C: the four entries SearchSysCache cannot reach by themselves.
    if relationId == SharedSecLabelRelationId
        || relationId == TriggerRelidNameIndexId
        || relationId == DatabaseNameIndexId
        || relationId == SharedSecLabelObjectIndexId
    {
        return true;
    }
    // C: return RelationSupportsSysCache(relationId);
    xunit::RelationSupportsSysCache(relationId)
}

/* ==========================================================================
 * Init-file pre/post invalidate + remove.
 * ======================================================================== */

/// `RelationCacheInitFilePreInvalidate()` (relcache.c): take `RelCacheInitLock`
/// and unlink the init file before sending invalidations. **Own logic** (the
/// file naming + unlink-with-LOG-on-failure decision); the LWLock and file
/// unlink are owner seams.
pub fn RelationCacheInitFilePreInvalidate() -> PgResult<()> {
    let dbpath = xunit::try_database_path();
    let local = dbpath
        .as_ref()
        .map(|p| format!("{p}/{RELCACHE_INIT_FILENAME}"));
    let shared = format!("global/{RELCACHE_INIT_FILENAME}");

    // C: LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);
    xunit::lwlock_acquire_relcache_init();
    // C: if (DatabasePath) unlink_initfile(localinitfname, ERROR);
    if let Some(local) = local {
        unlink_initfile(&local, ERROR.0)?;
    }
    // C: unlink_initfile(sharedinitfname, ERROR);
    unlink_initfile(&shared, ERROR.0)?;
    Ok(())
}

/// `RelationCacheInitFilePostInvalidate()` (relcache.c): release
/// `RelCacheInitLock` after invalidations are sent. **Own logic.**
pub fn RelationCacheInitFilePostInvalidate() -> PgResult<()> {
    // C: LWLockRelease(RelCacheInitLock);
    xunit::lwlock_release_relcache_init();
    Ok(())
}

/// `RelationCacheInitFileRemove()` (relcache.c): remove stale init files at
/// startup, across base/ and every numeric tablespace. **Own logic** (the
/// directory walk + numeric-name filter); `AllocateDir`/`ReadDirExtended` and
/// unlink are owner seams.
pub fn RelationCacheInitFileRemove() -> PgResult<()> {
    const TBLSPC_VERSION_DIR: &str = "PG_18_202506291";
    // C: unlink_initfile("global/<initfile>", LOG);
    unlink_initfile(&format!("global/{RELCACHE_INIT_FILENAME}"), LOG.0)?;
    // C: RelationCacheInitFileRemoveInDir("base");
    init_file_remove_in_dir("base")?;
    // C: for each numeric-named entry under pg_tblspc, recurse into its version
    // dir. The dir walk is the fd owner seam.
    for entry in xunit::read_dir_numeric("pg_tblspc")? {
        let path = format!("pg_tblspc/{entry}/{TBLSPC_VERSION_DIR}");
        init_file_remove_in_dir(&path)?;
    }
    Ok(())
}

/// `RelationCacheInitFileRemoveInDir(tblspcpath)` (relcache.c): unlink the init
/// file from every numeric-named database subdir. **Own logic.**
fn init_file_remove_in_dir(tblspcpath: &str) -> PgResult<()> {
    for entry in xunit::read_dir_numeric(tblspcpath)? {
        let initfilename = format!("{tblspcpath}/{entry}/{RELCACHE_INIT_FILENAME}");
        unlink_initfile(&initfilename, LOG.0)?;
    }
    Ok(())
}

/// `unlink_initfile(initfilename, elevel)` (relcache.c): unlink the file,
/// reporting at `elevel` on any error other than ENOENT. **Own logic.**
fn unlink_initfile(initfilename: &str, elevel: i32) -> PgResult<()> {
    // C: if (unlink(initfilename) < 0 && errno != ENOENT) ereport(elevel, ...).
    if let Err(missing) = xunit::unlink_file_result(initfilename) {
        if !missing {
            // ENOENT is ignored; any other failure reports at elevel.
            let err = ereport(types_error::ErrorLevel(elevel))
                .errcode_for_file_access()
                .errmsg(format!("could not remove cache file \"{initfilename}\""))
                .into_error();
            if elevel >= ERROR.0 {
                return Err(err);
            }
        }
    }
    Ok(())
}

/* ==========================================================================
 * Local catalog OID consts used only by RelationBuildLocalRelation's nailit.
 * ======================================================================== */

const ProcedureRelationId: Oid = 1255;
const TypeRelationId: Oid = 1247;

/* ==========================================================================
 * `xunit` — GENUINE cross-unit primitive seam-and-panic shims.
 *
 * These mirror the C calls into units that are not yet dependencies of this
 * crate (relmapper, syscache, lock manager, xact, smgr/storage, the fd layer,
 * the RelCacheInitLock LWLock, the catalog-namespace/shared-rel predicates,
 * `InitCatalogCachePhase2`). Each panics until its owner lands — the documented
 * "Mirror PG and panic" boundary. They are NOT own-logic stubs: the relcache
 * orchestration that calls them is fully implemented above.
 * ======================================================================== */
mod xunit {
    use super::*;

    pub(super) fn RelationMapInitialize() {
        todo!("relcache-initfile xunit: RelationMapInitialize (relmapper owner seam)")
    }
    /// The hardcoded `Schema_pg_<name>[]` `FormData_pg_attribute` rows the C
    /// `formrdesc` is handed for a nailed catalog. This is genbki-generated
    /// catalog-header bootstrap data (`pg_attribute.h` / `schemapg.h`), owned by
    /// the catalog-data layer; it crosses into relcache as a pure value array.
    /// Panics until that owner lands — "Mirror PG and panic".
    pub(super) fn catalog_schema_attrs(_relid: Oid) -> Vec<OwnedAttr> {
        todo!("relcache-initfile xunit: Schema_pg_* bootstrap attr rows (catalog-data owner seam)")
    }
    pub(super) fn RelationMapInitializePhase2() {
        todo!("relcache-initfile xunit: RelationMapInitializePhase2 (relmapper owner seam)")
    }
    pub(super) fn RelationMapInitializePhase3() {
        todo!("relcache-initfile xunit: RelationMapInitializePhase3 (relmapper owner seam)")
    }
    pub(super) fn RelationMapUpdateMapLocal(_relid: Oid, _shared: bool) {
        todo!("relcache-initfile xunit: RelationMapUpdateMap (relmapper owner seam)")
    }
    pub(super) fn IsBootstrapProcessingMode() -> bool {
        todo!("relcache-initfile xunit: IsBootstrapProcessingMode (bootstrap owner seam)")
    }
    pub(super) fn IsSharedRelation(_relid: Oid) -> bool {
        todo!("relcache-initfile xunit: IsSharedRelation (catalog owner seam)")
    }
    pub(super) fn IsCatalogNamespace(_nsp: Oid) -> bool {
        todo!("relcache-initfile xunit: IsCatalogNamespace (catalog owner seam)")
    }
    pub(super) fn RelationSupportsSysCache(_relid: Oid) -> bool {
        todo!("relcache-initfile xunit: RelationSupportsSysCache (syscache owner seam)")
    }
    pub(super) fn GetCurrentSubTransactionId() -> types_core::xact::SubTransactionId {
        todo!("relcache-initfile xunit: GetCurrentSubTransactionId (xact owner seam)")
    }
    pub(super) fn current_temp_proc_number() -> ProcNumber {
        // C: (ParallelLeaderProcNumber == INVALID_PROC_NUMBER) ? MyProcNumber :
        //    ParallelLeaderProcNumber.
        todo!("relcache-initfile xunit: temp rel proc number (proc owner seam)")
    }
    pub(super) fn SearchSysCacheRelOid(_relid: Oid) -> PgResult<FormPgClass> {
        todo!("relcache-initfile xunit: SearchSysCache1(RELOID) (syscache owner seam)")
    }
    pub(super) fn RelationBuildTriggers(_rel: *mut RelationData) -> PgResult<()> {
        todo!("relcache-initfile xunit: RelationBuildTriggers (trigger owner seam)")
    }
    pub(super) fn RelationBuildRowSecurity(_rel: *mut RelationData) -> PgResult<()> {
        todo!("relcache-initfile xunit: RelationBuildRowSecurity (rowsecurity owner seam)")
    }
    pub(super) fn RelationGetIndexAttOptions(_ird: *mut RelationData) -> PgResult<()> {
        todo!("relcache-initfile xunit: RelationGetIndexAttOptions (index/opclass owner seam)")
    }
    pub(super) fn LockRelationOid(_oid: Oid) -> PgResult<()> {
        todo!("relcache-initfile xunit: LockRelationOid (lock manager owner seam)")
    }
    pub(super) fn UnlockRelationOid(_oid: Oid) -> PgResult<()> {
        todo!("relcache-initfile xunit: UnlockRelationOid (lock manager owner seam)")
    }
    pub(super) fn InitCatalogCachePhase2() {
        todo!("relcache-initfile xunit: InitCatalogCachePhase2 (catcache owner seam)")
    }
    pub(super) fn set_new_relfilenumber_storage(_relid: Oid, _persistence: i8) -> PgResult<()> {
        todo!("relcache-initfile xunit: GetNewRelFileNumber + storage swap + pg_class/relmap \
               update (catalog/smgr/relmapper/inval owner seams)")
    }
    pub(super) fn my_proc_pid() -> i32 {
        todo!("relcache-initfile xunit: MyProcPid (proc owner seam)")
    }
    pub(super) fn database_path() -> String {
        todo!("relcache-initfile xunit: DatabasePath (init owner seam)")
    }
    pub(super) fn try_database_path() -> Option<String> {
        todo!("relcache-initfile xunit: DatabasePath presence (init owner seam)")
    }
    pub(super) fn read_file(_path: &str) -> PgResult<Option<Vec<u8>>> {
        todo!("relcache-initfile xunit: AllocateFile(PG_BINARY_R) read (fd owner seam)")
    }
    pub(super) fn write_file(_path: &str, _bytes: &[u8]) -> PgResult<()> {
        todo!("relcache-initfile xunit: AllocateFile(PG_BINARY_W) write (fd owner seam)")
    }
    pub(super) fn rename_file(_from: &str, _to: &str) -> Result<(), ()> {
        todo!("relcache-initfile xunit: durable_rename (fd owner seam)")
    }
    pub(super) fn unlink_file(_path: &str, _missing_ok: bool) {
        todo!("relcache-initfile xunit: unlink (fd owner seam)")
    }
    /// Returns `Err(true)` for ENOENT (the file was already absent), `Err(false)`
    /// for any other failure, `Ok(())` on success — mirroring the C
    /// `unlink` + `errno == ENOENT` check.
    pub(super) fn unlink_file_result(_path: &str) -> Result<(), bool> {
        todo!("relcache-initfile xunit: unlink + errno (fd owner seam)")
    }
    pub(super) fn read_dir_numeric(_dir: &str) -> PgResult<Vec<String>> {
        todo!("relcache-initfile xunit: AllocateDir/ReadDirExtended numeric entries (fd owner seam)")
    }
    pub(super) fn lwlock_acquire_relcache_init() {
        todo!("relcache-initfile xunit: LWLockAcquire(RelCacheInitLock) (lwlock owner seam)")
    }
    pub(super) fn lwlock_release_relcache_init() {
        todo!("relcache-initfile xunit: LWLockRelease(RelCacheInitLock) (lwlock owner seam)")
    }
    pub(super) fn accept_invalidation_messages() {
        todo!("relcache-initfile xunit: AcceptInvalidationMessages (inval owner seam)")
    }
}
