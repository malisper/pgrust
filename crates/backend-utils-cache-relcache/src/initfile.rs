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
//! their real in-crate functions.
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
use types_error::error::{ERRCODE_DATA_CORRUPTED, ERRCODE_INVALID_PARAMETER_VALUE};
use types_core::catalog::{
    BOOTSTRAP_SUPERUSERID, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_core::primitive::{InvalidRelFileNumber, Oid, ProcNumber, RegProcedure};
use types_core::xact::InvalidSubTransactionId;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE,
};

use crate::core_entry_store::entry::{
    FormPgClass, OwnedAttr, OwnedTupleConstr, OwnedTupleDesc, RelationData,
};
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
const AuthMemMemRoleIndexId: Oid = 2695;
const AuthMemRelationId: Oid = 1261;
const SharedSecLabelObjectIndexId: Oid = 3593;
const SharedSecLabelRelationId: Oid = 3592;

// Per-catalog attribute counts (formrdesc natts) — pg_*_d.h `Natts_*`.
const Natts_pg_database: i32 = 18;
const Natts_pg_authid: i32 = 12;
const Natts_pg_auth_members: i32 = 7;
const Natts_pg_shseclabel: i32 = 4;
const Natts_pg_subscription: i32 = 18;
const Natts_pg_class: i32 = 34;
const Natts_pg_attribute: i32 = 25;
const Natts_pg_proc: i32 = 30;
const Natts_pg_type: i32 = 32;

/// `RELCACHE_INIT_FILEMAGIC` (relcache.c) — the init-file magic number.
const RELCACHE_INIT_FILEMAGIC: i32 = 0x573266;
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
    xunit::RelationMapInitializePhase2()?;

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
    xunit::RelationMapInitializePhase3()?;

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
        xunit::InitCatalogCachePhase2()?;
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
fn finish_relcache_entries() -> PgResult<()> {
    use crate::core_entry_store::{with_rel, with_rel_mut};
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

            if with_rel(rd, |r| r.rd_rel.relowner == InvalidOid) {
                // C: SearchSysCache1(RELOID, rd_id) + memcpy of pg_class +
                // RelationParseRelOptions; the syscache read is the owner seam.
                let relp = xunit::SearchSysCacheRelOid(oid)?;
                let bad = with_rel_mut(rd, |r| {
                    r.rd_rel = relp;
                    // C re-parses reloptions from the syscache htup; the
                    // owned-model syscache form seam (search_pg_class_full_form)
                    // does not surface the variable-length reloptions tail
                    // column, so this phase-3 fixup reload re-parses with no
                    // reloptions bytes (None). Widening that syscache seam to
                    // carry reloptions is the syscache owner's lane; until then
                    // this corrupt-initfile-entry reload path leaves rd_options
                    // NULL rather than re-deriving it.
                    crate::build::RelationParseRelOptions(r, None)?;
                    Ok::<bool, backend_utils_error::PgError>(r.rd_rel.relowner == InvalidOid)
                })?;
                if bad {
                    let relname = with_rel(rd, |r| r.rd_rel.relname.clone());
                    crate::core_entry_store::RelationDecrementReferenceCount(rd)?;
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "invalid relowner in pg_class entry for \"{relname}\""
                        ))
                        .into_error());
                }
                restart = true;
            }
            if with_rel(rd, |r| r.rd_rel.relhasrules && r.rd_rules.is_none()) {
                with_rel_mut(rd, crate::derived::RelationBuildRuleLock)?;
                with_rel_mut(rd, |r| {
                    if r.rd_rules.is_none() {
                        r.rd_rel.relhasrules = false;
                    }
                });
                restart = true;
            }
            if with_rel(rd, |r| r.rd_rel.relhastriggers && !r.rd_has_trigdesc) {
                with_rel_mut(rd, crate::derived::RelationBuildTriggers)?;
                with_rel_mut(rd, |r| {
                    // RelationBuildTriggers sets r.rd_trigdesc; keep the presence
                    // flag in sync and flip relhastriggers off if the scan was
                    // empty (C: relhastriggers gets corrected when no triggers
                    // actually exist).
                    r.rd_has_trigdesc = r.rd_trigdesc.is_some();
                    if !r.rd_has_trigdesc {
                        r.rd_rel.relhastriggers = false;
                    }
                });
                restart = true;
            }
            if with_rel(rd, |r| r.rd_rel.relrowsecurity && !r.rd_has_rsdesc) {
                with_rel_mut(rd, crate::derived::RelationBuildRowSecurity)?;
                with_rel_mut(rd, |r| {
                    // RelationBuildRowSecurity sets r.rd_rsdesc; keep the
                    // presence flag in sync (C: relrowsecurity stays set, the
                    // descriptor is always installed when relrowsecurity is on).
                    r.rd_has_rsdesc = r.rd_rsdesc.is_some();
                });
                restart = true;
            }
            let needs_tableam = with_rel(rd, |r| {
                let relkind = r.rd_rel.relkind as u8;
                r.rd_tableam.is_none()
                    && (relkind == RELKIND_RELATION
                        || relkind == RELKIND_TOASTVALUE
                        || relkind == RELKIND_MATVIEW
                        || relkind == RELKIND_SEQUENCE)
            });
            if needs_tableam {
                with_rel_mut(rd, crate::index::RelationInitTableAccessMethod)?;
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
/// manager is an owner seam and `RelationGetIndexAttOptions` is the real
/// in-crate derived-family routine.
pub fn load_critical_index(indexoid: Oid, heapoid: Oid) -> PgResult<()> {
    // C: LockRelationOid(heapoid, AccessShareLock); LockRelationOid(indexoid,
    // AccessShareLock); — lock manager owner seam.
    xunit::LockRelationOid(heapoid)?;
    xunit::LockRelationOid(indexoid)?;

    // C: ird = RelationBuildDesc(indexoid, true); — in-crate build family.
    let ird = crate::build::RelationBuildDesc(indexoid, true)?;
    if ird == InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg_internal(format!("could not open critical system index {indexoid}"))
            .into_error());
    }
    // C: ird->rd_isnailed = true; ird->rd_refcnt = 1;
    crate::core_entry_store::with_rel_mut(ird, |r| {
        r.rd_isnailed = true;
        r.rd_refcnt = 1;
    });
    // C: UnlockRelationOid(indexoid/heapoid, AccessShareLock); — owner seam.
    xunit::UnlockRelationOid(indexoid)?;
    xunit::UnlockRelationOid(heapoid)?;
    // C: RelationGetIndexAttOptions(ird, false); — relcache-OWN fetch/parse of
    // per-column opclass index options (the leaf `get_attoptions` /
    // `index_opclass_options` primitives are the only cross-unit calls, reached
    // via real seams inside the derived-family implementation).
    crate::core_entry_store::with_rel_mut(ird, |r| {
        crate::derived::RelationGetIndexAttOptions(r, false)
    })?;
    Ok(())
}

/* ==========================================================================
 * `RelationBuildLocalRelation` — build an entry for a brand-new relation
 * without catalog access.
 * ======================================================================== */

/// `RelationBuildLocalRelation(...)` (relcache.c): build a relcache entry for a
/// brand-new relation without catalog access. **Own logic.**
///
/// `tup_desc` is the C `TupleDesc tupDesc` the caller passes
/// (`heap_create`/`index_create`'s column descriptor); the entry deep-copies it
/// into the owned [`OwnedTupleDesc`] (the cache-context `CreateTupleDescCopy`),
/// carrying `natts` and the per-column `attnotnull` / `attidentity` /
/// `attgenerated` (the only constraint-ish fields a brand-new relation may
/// already have) and the `attnullability` of not-null columns, and stamps
/// `constr.has_not_null` when any column is NOT NULL. `accessmtd` is the C
/// `Oid accessmtd` argument, stored as `rd_rel->relam`. `relfilenumber` sets
/// `relfilenode` (unmapped relations) or seeds the relation map (mapped).
pub fn RelationBuildLocalRelation<'mcx>(
    relname: &str,
    relnamespace: Oid,
    tup_desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
    relid: Oid,
    accessmtd: Oid,
    reltablespace: Oid,
    shared_relation: bool,
    mapped_relation: bool,
    relpersistence: i8,
    relkind: i8,
    relfilenumber: types_core::RelFileNumber,
) -> PgResult<Oid> {
    // int natts = tupDesc->natts; Assert(natts >= 0);
    let natts = tup_desc.natts;
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

    // C: rel->rd_att = CreateTupleDescCopy(tupDesc); rd_att->tdrefcount = 1.
    // A new relation can't have any defaults or constraints yet (they're added
    // in later steps), but attnotnull constraints + the attidentity/attgenerated
    // markers + the attnullability of not-null columns ARE copied here, and
    // has_not_null is computed for the constr stamp below. The owned entry
    // mirror carries those per-column fields on OwnedAttr.
    let mut attrs = Vec::<OwnedAttr>::with_capacity(natts.max(0) as usize);
    let mut has_not_null = false;
    for i in 0..(natts as usize) {
        // Form_pg_attribute satt = TupleDescAttr(tupDesc, i);
        let satt = tup_desc.attr(i);
        // The compact attribute carries attnullability (copied for not-null
        // columns; the C `dcatt->attnullability = scatt->attnullability`).
        let attnullability = if satt.attnotnull {
            tup_desc.compact_attrs[i].attnullability
        } else {
            // CreateTupleDescCopy leaves a nullable column's attnullability at
            // its populate_compact_attribute default (recomputed on materialize).
            tup_desc.compact_attrs[i].attnullability
        };
        has_not_null |= satt.attnotnull;
        attrs.push(OwnedAttr {
            // NameStr(satt->attname) -> owned String for the entry mirror.
            attname: String::from_utf8_lossy(satt.attname.name_str()).into_owned(),
            atttypid: satt.atttypid,
            attlen: satt.attlen,
            attnum: satt.attnum,
            atttypmod: satt.atttypmod,
            attbyval: satt.attbyval,
            attalign: satt.attalign,
            attstorage: satt.attstorage,
            attcompression: satt.attcompression,
            attnotnull: satt.attnotnull,
            atthasdef: satt.atthasdef,
            // CreateTupleDescCopy clears atthasmissing on the copy (it does not
            // carry constraints/defaults/missing); a newly created relation has
            // no missing values yet.
            atthasmissing: false,
            attndims: satt.attndims,
            attidentity: satt.attidentity,
            attgenerated: satt.attgenerated,
            attisdropped: satt.attisdropped,
            attislocal: satt.attislocal,
            attinhcount: satt.attinhcount,
            attcollation: satt.attcollation,
            attnullability,
        });
    }
    rel.rd_att = OwnedTupleDesc {
        natts,
        tdtypeid: tup_desc.tdtypeid,
        tdtypmod: tup_desc.tdtypmod,
        attrs,
        // if (has_not_null) { constr = palloc0(...); constr->has_not_null = true;
        //                     rd_att->constr = constr; }
        constr: if has_not_null {
            Some(OwnedTupleConstr {
                has_not_null: true,
                ..Default::default()
            })
        } else {
            None
        },
    };

    // C: rd_rel = palloc0(CLASS_TUPLE_SIZE); fill the pg_class form.
    let mut relform = FormPgClass::default();
    relform.relname = relname.to_string();
    relform.relnamespace = relnamespace;
    relform.relkind = relkind;
    relform.relnatts = natts as i16;
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
        // — relmapper owner seam.
        xunit::RelationMapUpdateMapLocal(relid, relfilenumber, shared_relation)?;
    } else {
        // C: rel->rd_rel->relfilenode = relfilenumber;
        relform.relfilenode = relfilenumber as Oid;
    }

    rel.rd_rel = relform;

    // C: RelationInitLockInfo(rel); RelationInitPhysicalAddr(rel); — index
    // family (physical addr) over the owned (not-yet-inserted) entry.
    crate::index::RelationInitPhysicalAddr(&mut rel)?;
    // C: rel->rd_rel->relam = accessmtd;
    rel.rd_rel.relam = accessmtd;

    // C: for relations with storage AM, RelationInitTableAccessMethod(rel).
    let relkind_u = relkind as u8;
    if relkind_u == RELKIND_RELATION
        || relkind_u == RELKIND_TOASTVALUE
        || relkind_u == RELKIND_MATVIEW
        || relkind_u == RELKIND_SEQUENCE
    {
        crate::index::RelationInitTableAccessMethod(&mut rel)?;
    }

    // C: rel->rd_isvalid = true (set before insert; the entry is valid).
    rel.rd_isvalid = true;

    // C: RelationCacheInsert(rel, true) — replace-allowed; on a same-OID
    // collision the C destroys the old entry if unreferenced or warns if still
    // referenced (outside bootstrap). The entry store handles that displacement.
    cache_insert(rel, true)?;

    // C: EOXactListAdd(rel).
    with_state(|st| eoxact_list_add(st, relid));

    // C: RelationIncrementReferenceCount(rel);
    RelationIncrementReferenceCount(relid)?;
    Ok(relid)
}

/* ==========================================================================
 * `RelationSetNewRelfilenumber` / `RelationAssumeNewRelfilelocator`.
 * ======================================================================== */

/// `RELKIND_HAS_STORAGE(relkind)` (`pg_class.h`) — relation kinds that have a
/// physical relfilenode.
fn relkind_has_storage(relkind: i8) -> bool {
    relkind == RELKIND_RELATION as i8
        || relkind == RELKIND_INDEX as i8
        || relkind == RELKIND_SEQUENCE as i8
        || relkind == RELKIND_TOASTVALUE as i8
        || relkind == RELKIND_MATVIEW as i8
}

/// `RELKIND_HAS_TABLE_AM(relkind)` (`pg_class.h`) — relation kinds whose storage
/// is managed through a table access method (`rd_tableam`); excludes sequences.
fn relkind_has_table_am(relkind: i8) -> bool {
    relkind == RELKIND_RELATION as i8
        || relkind == RELKIND_TOASTVALUE as i8
        || relkind == RELKIND_MATVIEW as i8
}

/// `RelationSetNewRelfilenumber(relation, persistence)` (relcache.c): assign a
/// new relfilenumber (and possibly persistence) to an existing relation, doing
/// the full rewrite-with-transactional-safety dance. **The relfilenumber
/// selection (incl. the binary-upgrade variants), the `RELKIND` storage
/// dispatch, and the `RelationIsMapped` branch are this function's OWN control
/// flow.** The genuinely cross-unit leaf operations — `GetNewRelFileNumber`
/// (catalog.c), the binary-upgrade global consume (binary_upgrade.h), the
/// smgr/storage drop+create (storage.c), `table_relation_set_new_filelocator`
/// (tableam), `RelationMapUpdateMap` (relmapper), the pg_class tuple update
/// (catalog), `CacheInvalidateRelcache` (inval), `GetCurrentTransactionId` /
/// `CommandCounterIncrement` (xact) — are owner seams (panic until each owner
/// lands). The trailing `RelationAssumeNewRelfilelocator` is own logic.
pub fn RelationSetNewRelfilenumber(relation: Oid, persistence: i8) -> PgResult<()> {
    let (relid, relkind, reltablespace, relisshared, rd_locator, rd_backend, relfilenode) =
        crate::core_entry_store::with_rel(relation, |rd| {
            (
                rd.rd_id,
                rd.rd_rel.relkind,
                rd.rd_rel.reltablespace,
                rd.rd_rel.relisshared,
                rd.rd_locator,
                rd.rd_backend,
                rd.rd_rel.relfilenode,
            )
        });

    let is_binary_upgrade = backend_catalog_binary_upgrade_seams::is_binary_upgrade::call();

    // --- Relfilenumber selection (OWN: the IsBinaryUpgrade / RELKIND chain) ---
    let newrelfilenumber: Oid = if !is_binary_upgrade {
        // C: GetNewRelFileNumber(relation->rd_rel->reltablespace, NULL, persistence)
        backend_catalog_catalog_seams::get_new_relfilenumber::call(reltablespace, persistence)?
    } else if relkind == RELKIND_INDEX as i8 {
        // C: binary_upgrade_next_index_pg_class_relfilenumber (consume).
        let n = backend_catalog_binary_upgrade_seams::consume_next_relfilenumber::call(true);
        if n == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("index relfilenumber value not set when in binary upgrade mode")
                .into_error());
        }
        n
    } else if relkind == RELKIND_RELATION as i8 {
        // C: binary_upgrade_next_heap_pg_class_relfilenumber (consume).
        let n = backend_catalog_binary_upgrade_seams::consume_next_relfilenumber::call(false);
        if n == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("heap relfilenumber value not set when in binary upgrade mode")
                .into_error());
        }
        n
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("unexpected request for new relfilenumber in binary upgrade mode")
            .into_error());
    };

    // --- Schedule unlinking of the old storage (OWN: the IsBinaryUpgrade
    // branch); the leaf smgr/storage ops are owner seams. ---
    if is_binary_upgrade {
        // C: srel = smgropen(rd_locator, rd_backend);
        //    smgrdounlinkall(&srel, 1, false); smgrclose(srel);
        backend_catalog_storage_seams::smgr_unlink_relation_now::call(rd_locator, rd_backend)?;
    } else {
        // C: RelationDropStorage(relation);
        backend_catalog_storage_seams::relation_drop_storage::call(rd_locator, rd_backend)?;
    }

    // C: newrlocator = relation->rd_locator; newrlocator.relNumber = newrelfilenumber;
    let mut newrlocator = rd_locator;
    newrlocator.relNumber = newrelfilenumber;

    // --- RELKIND storage dispatch (OWN). The AM/storage create are owner
    // seams; the table-AM leg also hands back the new freeze/minmxid. ---
    let mut freeze_xid: u32 = types_core::xact::InvalidTransactionId;
    let mut minmulti: u32 = 0; // InvalidMultiXactId
    if relkind_has_table_am(relkind) {
        // C: table_relation_set_new_filelocator(relation, &newrlocator,
        //    persistence, &freezeXid, &minmulti);
        // The dispatch needs the open Relation (it carries the AM vtable); the
        // entry is registry-owned, so project a transient read handle from the
        // store (no release authority — same pattern as plancat_ext).
        let relcx = mcx::MemoryContext::new("RelationSetNewRelfilenumber");
        let data = crate::core_entry_store::with_relation(relid, |rd| {
            crate::build::project_relation_data(relcx.mcx(), rd)
        })??;
        let rel = types_rel::Relation::open(data, None);
        let (fx, mm) = backend_access_table_tableam_seams::table_relation_set_new_filelocator::call(
            &rel,
            newrlocator,
            persistence,
        )?;
        freeze_xid = fx;
        minmulti = mm;
    } else if relkind_has_storage(relkind) {
        // C: srel = RelationCreateStorage(newrlocator, persistence, true);
        //    smgrclose(srel);
        backend_catalog_storage_seams::relation_create_storage_main_fork::call(
            newrlocator,
            persistence,
        )?;
    } else {
        // C: elog(ERROR, "relation \"%s\" does not have storage", ...).
        return Err(ereport(ERROR)
            .errmsg_internal(format!("relation {relid} does not have storage"))
            .into_error());
    }

    // --- Mapped vs. pg_class-update branch (OWN). ---
    // C: RelationIsMapped(relation) == RELKIND_HAS_STORAGE && relfilenode ==
    //    InvalidRelFileNumber.
    let is_mapped =
        relkind_has_storage(relkind) && relfilenode == InvalidRelFileNumber;
    if is_mapped {
        // C: in some paths the would-be tuple update is the only thing that
        // assigns an XID, but we must have one to delete files — force one.
        backend_access_transam_xact_seams::get_current_transaction_id::call()?;

        // C: RelationMapUpdateMap(RelationGetRelid(relation), newrelfilenumber,
        //    relation->rd_rel->relisshared, false);
        backend_utils_cache_relmapper_seams::relation_map_update_map::call(
            relid,
            newrelfilenumber,
            relisshared,
            false,
        )?;

        // C: not updating pg_class, so trigger relcache inval manually —
        // CacheInvalidateRelcache(relation).
        backend_utils_cache_inval_seams::cache_invalidate_relcache::call(relid)?;
    } else {
        // C: normal case — update the pg_class entry (relfilenode, reset
        // relpages/etc. for non-sequence relkinds, freeze/minmxid/persistence)
        // and CatalogTupleUpdate. The whole pg_class tuple lifecycle is the
        // catalog owner's.
        backend_catalog_storage_seams::update_pg_class_relfilenumber::call(
            relid,
            newrelfilenumber,
            persistence,
            relkind,
            freeze_xid,
            minmulti,
        )?;
    }

    // C: CommandCounterIncrement() — make the pg_class/relmap change visible.
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    // C: RelationAssumeNewRelfilelocator(relation).
    RelationAssumeNewRelfilelocator(relation)
}

/// `RelationAssumeNewRelfilelocator(relation)` (relcache.c): update the
/// `rd_*Subid` tracking after an external relfilenumber change. **Own logic.**
pub fn RelationAssumeNewRelfilelocator(relation: Oid) -> PgResult<()> {
    let subid = xunit::GetCurrentSubTransactionId();
    let relid = crate::core_entry_store::with_rel_mut(relation, |rd| {
        rd.rd_newRelfilelocatorSubid = subid;
        if rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId {
            rd.rd_firstRelfilelocatorSubid = rd.rd_newRelfilelocatorSubid;
        }
        rd.rd_id
    });
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
        if crate::core_entry_store::cache_lookup(oid).is_none() {
            continue;
        }
        // C: if (relform->relisshared != shared) continue; if (!shared &&
        // !RelationIdIsInInitFile(rel->rd_id)) continue; else write_item(...).
        // RelationIdIsInInitFile re-enters the store (read-only), so decide it
        // before opening the borrow.
        let (skip, is_in_init_file) = crate::core_entry_store::with_rel(oid, |r| {
            (r.rd_rel.relisshared != shared, r.rd_id)
        });
        if skip {
            continue;
        }
        if !shared && !RelationIdIsInInitFile(is_in_init_file) {
            continue;
        }
        crate::core_entry_store::with_rel(oid, |r| write_entry(&mut buf, r));
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
    // rd_amhandler is a top-level RelationData scalar that C's whole-struct
    // fread restores for free; this field-wise framing must persist it
    // explicitly so the load side does NOT re-derive it via a syscache lookup
    // (which, during the SHARED Phase2 load, would recurse into building
    // pg_class before pg_class exists — infinite recursion / stack overflow).
    header.extend_from_slice(&r.rd_amhandler.to_ne_bytes());
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
    v.push(att.attstorage as u8);
    v.push(att.attcompression as u8);
    v.push(att.attnotnull as u8);
    v.push(att.atthasdef as u8);
    v.extend_from_slice(&att.attndims.to_ne_bytes());
    v.push(att.attidentity as u8);
    v.push(att.attgenerated as u8);
    v.push(att.attisdropped as u8);
    v.push(att.attislocal as u8);
    v.extend_from_slice(&att.attinhcount.to_ne_bytes());
    v.extend_from_slice(&att.attcollation.to_ne_bytes());
    v.push(att.attnullability as u8);
    v
}

fn encode_pg_index(idx: &crate::core_entry_store::entry::FormPgIndex) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&idx.indexrelid.to_ne_bytes());
    v.extend_from_slice(&idx.indrelid.to_ne_bytes());
    v.extend_from_slice(&idx.indnatts.to_ne_bytes());
    v.extend_from_slice(&idx.indnkeyatts.to_ne_bytes());
    v.push(idx.indisunique as u8);
    v.push(idx.indnullsnotdistinct as u8);
    v.push(idx.indisprimary as u8);
    v.push(idx.indisexclusion as u8);
    v.push(idx.indimmediate as u8);
    v.push(idx.indisclustered as u8);
    v.push(idx.indisvalid as u8);
    v.push(idx.indcheckxmin as u8);
    v.push(idx.indisready as u8);
    v.push(idx.indislive as u8);
    v.push(idx.indisreplident as u8);
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
            // rd_amhandler — persisted by write_entry (mirrors C's whole-struct
            // fread). Read it back here so the index branch below does not need a
            // syscache lookup (which would recurse during the Phase2 shared load).
            rel.rd_amhandler = hc.read_oid().unwrap_or(InvalidOid);
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
            Some(types_reloptions::RdOptions::Std(
                types_reloptions::StdRdOptions::default(),
            ))
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
            // C: InitIndexAmRoutine(rel) reads rel->rd_amhandler, which in C is
            // restored for free by the whole-RelationData struct fread. The
            // field-wise reconstruction persists rd_amhandler in the entry header
            // (above) and restores it there, mirroring C. We must NOT re-derive it
            // here via a syscache lookup: the SHARED Phase2 load runs BEFORE
            // pg_class is built, so SearchSysCache1(AMOID) recurses into building
            // pg_class -> ScanPgRelation(pg_class) -> open pg_class (not yet
            // cached) -> infinite recursion / stack overflow (WALL 1ab).
            debug_assert!(rel.rd_rel.relam != InvalidOid);
            if rel.rd_amhandler == InvalidOid {
                // A pre-existing init file from before rd_amhandler was framed:
                // treat as corrupt and rebuild from catalog (C read_failed path).
                return read_failed(rels);
            }
            // C: InitIndexAmRoutine(rel) — index family (on the local entry).
            crate::index::InitIndexAmRoutine(&mut rel)?;
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
            // C: rel->rd_supportinfo = (FmgrInfo *)
            //        MemoryContextAllocZero(indexcxt, relsupport * sizeof(FmgrInfo));
            // where relsupport = relform->relnatts * am->amsupport. The slots are
            // lazily filled by index_getprocinfo, but the array MUST be sized
            // up-front (zero-filled) so index_getprocinfo can index into it — the
            // from-catalog RelationInitIndexAccessInfo sizes it identically. (The
            // prior port left it empty, so index_getprocinfo on an init-file-loaded
            // index panicked out-of-bounds.)
            let amsupport = rel
                .rd_indam
                .as_ref()
                .map(|am| am.amsupport)
                .unwrap_or(0);
            if amsupport > 0 {
                let nsupport = natts * amsupport as i32;
                rel.rd_supportinfo =
                    (0..nsupport).map(|_| types_core::fmgr::FmgrInfo::default()).collect();
            } else {
                rel.rd_supportinfo = Vec::new();
            }
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
                crate::index::RelationInitTableAccessMethod(&mut rel)?;
            }
        }

        // C: reset all the derived/per-xact fields to their fresh state.
        rel.rd_rules = None;
        rel.rd_trigdesc = None;
        rel.rd_has_trigdesc = false;
        rel.rd_rsdesc = None;
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
        crate::index::RelationInitPhysicalAddr(&mut rel)?;
        rels.push(rel);
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

    // C: install every entry into RelationIdCache (replace-allowed). The entry
    // store handles a same-OID displacement (destroy-if-unreferenced /
    // leak-warning), the C `RelationCacheInsert` macro's collision arm.
    for rel in rels {
        cache_insert(rel, true)?;
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
    a.attstorage = c.read_u8()? as i8;
    a.attcompression = c.read_u8()? as i8;
    a.attnotnull = c.read_u8()? != 0;
    a.atthasdef = c.read_u8()? != 0;
    a.attndims = c.read_i16()?;
    a.attidentity = c.read_u8()? as i8;
    a.attgenerated = c.read_u8()? as i8;
    a.attisdropped = c.read_u8()? != 0;
    a.attislocal = c.read_u8()? != 0;
    a.attinhcount = c.read_i16()?;
    a.attcollation = c.read_oid()?;
    a.attnullability = c.read_u8()? as i8;
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
    idx.indnullsnotdistinct = c.read_u8()? != 0;
    idx.indisprimary = c.read_u8()? != 0;
    idx.indisexclusion = c.read_u8()? != 0;
    idx.indimmediate = c.read_u8()? != 0;
    idx.indisclustered = c.read_u8()? != 0;
    idx.indisvalid = c.read_u8()? != 0;
    idx.indcheckxmin = c.read_u8()? != 0;
    idx.indisready = c.read_u8()? != 0;
    idx.indislive = c.read_u8()? != 0;
    idx.indisreplident = c.read_u8()? != 0;
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
 * `xunit` — GENUINE cross-unit primitive shims (thin marshal + delegate).
 *
 * Each mirrors a C call into a unit that is not a structural part of this
 * family. Where the owner is already ported these delegate to its real
 * `<owner>-seams::fn::call(...)` (relmapper map init/update, miscinit
 * bootstrap/MyProcPid/DatabasePath, catalog IsSharedRelation, namespace
 * IsCatalogNamespace, syscache RelationSupportsSysCache/SearchSysCache1(RELOID)/
 * InitCatalogCachePhase2, xact GetCurrentSubTransactionId, the proc-number
 * pair, lmgr LockRelationOid, the RelCacheInitLock LWLock, the fd file API, and
 * AcceptInvalidationMessages) — panicking with "seam not installed" only until
 * that owner's `init_seams()` runs. The remaining three
 * (`catalog_schema_attrs`, `RelationBuildTriggers`, `RelationBuildRowSecurity`)
 * cross as this crate's OWN entry types into units that are not yet ported
 * (genbki catalog-data, trigger.c, policy.c), so they stay seam-and-panic
 * ("Mirror PG and panic"). None are own-logic stubs: the relcache
 * orchestration that calls them is fully implemented above.
 * ======================================================================== */
mod xunit {
    use super::*;

    use types_storage::lock::AccessShareLock;
    use types_storage::storage::{LWLockMode, REL_CACHE_INIT_LOCK};

    /// `ENOENT` (POSIX): the `errno` value the C `unlink` path treats as
    /// "already gone" (a non-error). The fd `unlink_file` seam reports failures
    /// as `-errno`, so ENOENT surfaces as `-2`.
    const ENOENT: i32 = 2;

    // ---- relmapper (relation map for mapped relations) ----

    pub(super) fn RelationMapInitialize() {
        // C: RelationMapInitialize();
        backend_utils_cache_relmapper_seams::relation_map_initialize::call();
    }
    pub(super) fn RelationMapInitializePhase2() -> PgResult<()> {
        // C: RelationMapInitializePhase2();
        backend_utils_cache_relmapper_seams::relation_map_initialize_phase2::call()
    }
    pub(super) fn RelationMapInitializePhase3() -> PgResult<()> {
        // C: RelationMapInitializePhase3();
        backend_utils_cache_relmapper_seams::relation_map_initialize_phase3::call()
    }
    pub(super) fn RelationMapUpdateMapLocal(
        relid: Oid,
        relfilenumber: types_core::RelFileNumber,
        shared: bool,
    ) -> PgResult<()> {
        // C: RelationMapUpdateMap(relid, relfilenumber, shared_relation, true)
        // — the immediate (transaction-private active-map) update
        // RelationBuildLocalRelation issues for a mapped relation.
        backend_utils_cache_relmapper_seams::relation_map_update_map::call(
            relid,
            relfilenumber,
            shared,
            true,
        )
    }

    // ---- bootstrap / miscinit (proc-global reads) ----

    pub(super) fn IsBootstrapProcessingMode() -> bool {
        // C: IsBootstrapProcessingMode() — Mode == BootstrapProcessing.
        backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call()
    }
    pub(super) fn my_proc_pid() -> i32 {
        // C: MyProcPid.
        backend_utils_init_miscinit_seams::my_proc_pid::call()
    }
    pub(super) fn database_path() -> String {
        // C: DatabasePath — guaranteed non-NULL on the paths that call this
        // (a database has been selected). The presence test goes through
        // `try_database_path`.
        backend_utils_init_miscinit_seams::get_database_path::call()
            .expect("DatabasePath is NULL but a database path was required")
    }
    pub(super) fn try_database_path() -> Option<String> {
        // C: (DatabasePath != NULL) ? DatabasePath : NULL.
        backend_utils_init_miscinit_seams::get_database_path::call()
    }

    // ---- catalog / namespace predicates ----

    pub(super) fn IsSharedRelation(relid: Oid) -> bool {
        // C: IsSharedRelation(relid).
        backend_catalog_catalog_seams::is_shared_relation::call(relid)
    }
    pub(super) fn IsCatalogNamespace(nsp: Oid) -> bool {
        // C: IsCatalogNamespace(relnamespace) — catalog.c.
        backend_catalog_catalog_seams::is_catalog_namespace::call(nsp)
    }

    // ---- syscache ----

    pub(super) fn RelationSupportsSysCache(relid: Oid) -> bool {
        // C: RelationSupportsSysCache(relationId).
        backend_utils_cache_syscache_seams::relation_supports_syscache::call(relid)
    }
    pub(super) fn SearchSysCacheRelOid(relid: Oid) -> PgResult<FormPgClass> {
        // C: htup = SearchSysCache1(RELOID, relid); if (!HeapTupleIsValid(htup))
        //    ereport(FATAL, "cache lookup failed for relation %u");
        //    relp = (Form_pg_class) GETSTRUCT(htup);
        // The owner returns the full Form_pg_class projection by value; marshal
        // it into the entry-owned FormPgClass (the C memcpy into rd_rel).
        let scratch = mcx::MemoryContext::new("pg_class form");
        let form = backend_utils_cache_syscache_seams::search_pg_class_full_form::call(
            scratch.mcx(),
            relid,
        )?;
        match form {
            Some(f) => Ok(FormPgClass {
                relname: f.relname.to_string(),
                relnamespace: f.relnamespace,
                reltype: f.reltype,
                reloftype: f.reloftype,
                relowner: f.relowner,
                relam: f.relam,
                relfilenode: f.relfilenode,
                reltablespace: f.reltablespace,
                relpages: f.relpages,
                reltuples: f.reltuples,
                relallvisible: f.relallvisible,
                reltoastrelid: f.reltoastrelid,
                relhasindex: f.relhasindex,
                relisshared: f.relisshared,
                relpersistence: f.relpersistence,
                relkind: f.relkind,
                relnatts: f.relnatts,
                relchecks: f.relchecks,
                relhasrules: f.relhasrules,
                relhastriggers: f.relhastriggers,
                relhassubclass: f.relhassubclass,
                relrowsecurity: f.relrowsecurity,
                relforcerowsecurity: f.relforcerowsecurity,
                relispopulated: f.relispopulated,
                relreplident: f.relreplident,
                relispartition: f.relispartition,
                relrewrite: f.relrewrite,
                relfrozenxid: f.relfrozenxid,
                relminmxid: f.relminmxid,
            }),
            None => Err(ereport(types_error::FATAL)
                .errcode(types_error::error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg_internal(format!("cache lookup failed for relation {relid}"))
                .into_error()),
        }
    }

    // ---- xact ----

    pub(super) fn GetCurrentSubTransactionId() -> types_core::xact::SubTransactionId {
        // C: GetCurrentSubTransactionId().
        backend_access_transam_xact_seams::get_current_sub_transaction_id::call()
    }

    // ---- proc number (temp relation backend) ----

    pub(super) fn current_temp_proc_number() -> ProcNumber {
        // C: (ParallelLeaderProcNumber == INVALID_PROC_NUMBER) ? MyProcNumber :
        //    ParallelLeaderProcNumber.
        let leader = backend_access_transam_parallel_rt_seams::parallel_leader_proc_number::call();
        if leader == INVALID_PROC_NUMBER {
            backend_utils_init_small_seams::my_proc_number::call()
        } else {
            leader
        }
    }

    // ---- catcache (second-phase syscache init) ----

    pub(super) fn InitCatalogCachePhase2() -> PgResult<()> {
        // C: InitCatalogCachePhase2() — loops InitCatCachePhase2 over every
        // SysCache id; the syscache owner holds the cache-info table.
        backend_utils_cache_syscache_seams::init_catalog_cache_phase2::call()
    }

    // ---- lock manager ----

    pub(super) fn LockRelationOid(oid: Oid) -> PgResult<()> {
        // C: LockRelationOid(oid, AccessShareLock); — held until transaction
        // end (the C default), released by the matching UnlockRelationOid call
        // in load_critical_index, so the guard is kept rather than dropped.
        let guard =
            backend_storage_lmgr_lmgr_seams::lock_relation_oid::call(oid, AccessShareLock)?;
        guard.keep();
        Ok(())
    }
    pub(super) fn UnlockRelationOid(oid: Oid) -> PgResult<()> {
        // C: UnlockRelationOid(oid, AccessShareLock).
        backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(oid, AccessShareLock)
    }

    // ---- RelCacheInitLock LWLock (init-file rename/unlink dance) ----

    pub(super) fn lwlock_acquire_relcache_init() {
        // C: LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE); — held across the
        // rename/unlink dance and released by lwlock_release_relcache_init, so
        // the guard is leaked (kept) rather than dropped at end of scope.
        let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
            REL_CACHE_INIT_LOCK,
            LWLockMode::LW_EXCLUSIVE,
        )
        .expect("LWLockAcquire(RelCacheInitLock) failed");
        core::mem::forget(guard);
    }
    pub(super) fn lwlock_release_relcache_init() {
        // C: LWLockRelease(RelCacheInitLock).
        backend_storage_lmgr_lwlock_seams::lwlock_release_main::call(REL_CACHE_INIT_LOCK)
            .expect("LWLockRelease(RelCacheInitLock) failed");
    }

    // ---- shared-cache invalidation ----

    pub(super) fn accept_invalidation_messages() {
        // C: AcceptInvalidationMessages().
        backend_utils_cache_inval_seams::accept_invalidation_messages::call()
            .expect("AcceptInvalidationMessages failed");
    }

    // ---- file API (AllocateFile / AllocateDir, fd.c) ----

    pub(super) fn read_file(path: &str) -> PgResult<Option<Vec<u8>>> {
        // C: AllocateFile(initfilename, PG_BINARY_R); fread() the whole file.
        // `Ok(None)` is the C miss (file absent), which sends the caller down
        // the rebuild path.
        backend_storage_file_fd_seams::allocate_file_read::call(path)
    }
    pub(super) fn write_file(path: &str, bytes: &[u8]) -> PgResult<()> {
        // C: AllocateFile(tempfilename, PG_BINARY_W); fwrite() then FreeFile().
        backend_storage_file_fd_seams::allocate_file_write::call(path, bytes)
    }
    pub(super) fn rename_file(from: &str, to: &str) -> Result<(), ()> {
        // C: rename(tempfilename, finalfilename) < 0 — the init-file final
        // rename (a plain rename, not durable_rename). 0 on success.
        if backend_storage_file_fd_seams::rename_file::call(from, to) == 0 {
            Ok(())
        } else {
            Err(())
        }
    }
    pub(super) fn unlink_file(path: &str, _missing_ok: bool) {
        // C: unlink(tempfilename) — best-effort cleanup, errors ignored.
        let _ = backend_storage_file_fd_seams::unlink_file::call(path);
    }
    /// Returns `Err(true)` for ENOENT (the file was already absent), `Err(false)`
    /// for any other failure, `Ok(())` on success — mirroring the C
    /// `unlink` + `errno == ENOENT` check.
    pub(super) fn unlink_file_result(path: &str) -> Result<(), bool> {
        // C: if (unlink(initfilename) < 0) { if (errno == ENOENT) ok; else err }
        let rc = backend_storage_file_fd_seams::unlink_file::call(path);
        if rc == 0 {
            Ok(())
        } else {
            // fd's unlink_file returns -errno on failure.
            Err(rc == -ENOENT)
        }
    }
    pub(super) fn read_dir_numeric(dir: &str) -> PgResult<Vec<String>> {
        // C: AllocateDir(dir); while (ReadDirExtended(dir, .., LOG)) keep only
        // entries whose name is all digits (strspn == strlen). The numeric
        // filter is relcache own logic; the dir walk is the fd owner seam (LOG
        // severity, so infallible).
        let names = backend_storage_file_fd_seams::read_dir_names_logged::call(dir);
        Ok(names
            .into_iter()
            .filter(|name| !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()))
            .collect())
    }

    /* ----------------------------------------------------------------------
     * GENUINELY-unported owners — seam-and-panic ("Mirror PG and panic").
     *
     * These mirror C calls into units that are not yet ported AND whose
     * arguments/results cross as relcache's OWN entry types (`*mut
     * RelationData`, the owned `FormPgClass`/`OwnedAttr` payloads) — which
     * cannot cross a cross-crate seam boundary. They panic on an unported
     * callee, which is the sanctioned boundary (audit-crate: "panicking on an
     * unported callee is fine"). The relcache orchestration that calls them is
     * fully implemented above; only the callee body is absent.
     * -------------------------------------------------------------------- */

    /// The hardcoded `Schema_pg_<name>[]` `FormData_pg_attribute` rows the C
    /// `formrdesc` is handed for a nailed catalog, keyed by the catalog's
    /// row-type OID (`*Relation_Rowtype_Id`). This is genbki-generated
    /// catalog-header bootstrap data (`catalog/schemapg.h`), owned by the
    /// `backend-bootstrap-catalog-data` crate and crossing in via the
    /// `catalog_schema_attrs` seam as a [`BootstrapCatalogSchema`] (the rows plus
    /// the catalog relation OID `formrdesc` reads for `rd_id`, which the
    /// `OwnedAttr` rows cannot carry).
    pub(super) fn catalog_schema_attrs(
        reltype: Oid,
    ) -> types_relcache_entry::BootstrapCatalogSchema {
        // C: Desc_pg_* (the genbki Schema_pg_* arrays). Outward seam to the
        // bootstrap-catalog-data owner; installed from its `init_seams()`.
        backend_utils_cache_relcache_seams::catalog_schema_attrs::call(reltype)
    }
}
