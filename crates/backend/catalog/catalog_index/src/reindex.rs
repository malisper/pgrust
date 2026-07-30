use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, INDEX_RELATION_ID, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, WARNING};
use types_rel::{
    AccessExclusiveLock, InplaceUpdateTupleLock, NoLock, Relation, RowExclusiveLock, ShareLock,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
};

use crate::{oid_scankey, IndexGetRelation, IndexRelidIndexId};

const Natts_pg_class: usize = 34;
const Anum_pg_class_relfilenode: usize = 8;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_relpersistence: usize = 17;
const Anum_pg_class_relfrozenxid: usize = 30;
const Anum_pg_class_relminmxid: usize = 31;

// RelationSetNewRelfilenumber (relcache.c), hosted here: relcache cannot dep
// catalog_storage/tableam/catalog_indexing without cycling. The pg_class write
// takes LOCKTAG_TUPLE at InplaceUpdateTupleLock, as C's
// SearchSysCacheLockedCopy1 does (relcache.c:3820/:3949) -- see
// scripts/inplace-lockcheck.sh for the standing enumeration of pg_class's
// transactional updaters and which of them C locks. The subid Cells are set
// before CommandCounterIncrement so the inval rebuild's copy_preserved
// carries them onto the rebuilt entry.
pub fn RelationSetNewRelfilenumber<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    persistence: u8,
) -> PgResult<()> {
    let newrelfilenumber =
        catalog::GetNewRelFileNumber(mcx, rel.rd_rel.reltablespace, None, persistence)?;

    catalog_storage::RelationDropStorage(rel)?;

    let mut newrlocator = rel.rd_locator.get();
    newrlocator.relNumber = newrelfilenumber;
    let (freeze_xid, minmulti) = match rel.rd_rel.relkind {
        types_rel::RELKIND_INDEX | types_rel::RELKIND_SEQUENCE => {
            let srel = catalog_storage::RelationCreateStorage(newrlocator, persistence, true)?;
            smgr::smgrclose(srel)?;
            (
                types_core::InvalidTransactionId,
                types_core::InvalidTransactionId,
            )
        }
        types_rel::RELKIND_RELATION
        | types_rel::RELKIND_TOASTVALUE
        | types_rel::RELKIND_MATVIEW => {
            tableam::table_relation_set_new_filelocator(rel, &newrlocator, persistence as i8)?
        }
        k => panic!("relation \"{}\" does not have storage (relkind {k})", rel.name()),
    };

    if rel.is_mapped() {
        // Mapped index: pg_class stays untouched (essential when reindexing
        // pg_class itself); the relation mapper carries the new number.
        debug_assert!(rel.rd_rel.relkind == types_rel::RELKIND_INDEX);
        xact::GetCurrentTransactionId()?;
        relmapper::RelationMapUpdateMap(
            rel.rd_id,
            newrelfilenumber,
            rel.rd_rel.relisshared,
            false,
        )?;
        inval::invalidate::CacheInvalidateRelcache(rel)?;
    } else {
        let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
        let key = [oid_scankey(1, rel.rd_id)];
        let mut scan =
            genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
        let reltup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("could not find tuple for relation {}", rel.rd_id));
        // C: SearchSysCacheLockedCopy1 (relcache.c:3820) / UnlockTuple
        // (relcache.c:3949). Before the content read that feeds the
        // replacement image, so a concurrent inplace writer is either
        // serialized behind us or visible in what we copy -- losing
        // relfrozenxid/relminmxid here is a durable wraparound-safety
        // regression, and this function writes both.
        let otid = reltup.t_self;
        lmgr::LockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
        let mut values = [Datum::null(); Natts_pg_class];
        let isnull = [false; Natts_pg_class];
        let mut replace = [false; Natts_pg_class];
        let mut set = |anum: usize, d: Datum| {
            values[anum - 1] = d;
            replace[anum - 1] = true;
        };
        set(Anum_pg_class_relfilenode, Datum::from_oid(newrelfilenumber));
        if rel.rd_rel.relkind != types_rel::RELKIND_SEQUENCE {
            set(Anum_pg_class_relpages, Datum::from_i32(0));
            set(Anum_pg_class_reltuples, Datum::from_f32(-1.0));
            set(Anum_pg_class_relallvisible, Datum::from_i32(0));
            set(Anum_pg_class_relallfrozen, Datum::from_i32(0));
        }
        set(Anum_pg_class_relfrozenxid, Datum::from_transaction_id(freeze_xid));
        set(Anum_pg_class_relminmxid, Datum::from_transaction_id(minmulti));
        set(Anum_pg_class_relpersistence, Datum::from_char(persistence as i8));
        let mut newtup = heaptuple::heap_modify_tuple(
            mcx,
            reltup,
            pg_class.descr(),
            &values,
            &isnull,
            &replace,
        )?;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
        lmgr::UnlockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
        pg_class.close(RowExclusiveLock)?;
    }

    // RelationAssumeNewRelfilelocator + the physical-addr refresh the C
    // in-place rebuild would perform on this same entry.
    rel.rd_locator.set(newrlocator);
    relcache::invalidate::RelationAssumeNewRelfilelocator(rel);

    xact::CommandCounterIncrement()
}

pub const REINDEXOPT_VERBOSE: u32 = 0x01;
pub const REINDEXOPT_REPORT_PROGRESS: u32 = 0x02;
pub const REINDEXOPT_MISSING_OK: u32 = 0x04;
pub const REINDEXOPT_CONCURRENTLY: u32 = 0x08;

#[derive(Clone, Copy, Default)]
pub struct ReindexParams {
    pub options: u32,
    pub tablespace_oid: Oid,
}

pub const REINDEX_REL_PROCESS_TOAST: i32 = 0x01;
pub const REINDEX_REL_SUPPRESS_INDEX_USE: i32 = 0x02;
pub const REINDEX_REL_CHECK_CONSTRAINTS: i32 = 0x04;
pub const REINDEX_REL_FORCE_INDEXES_UNLOGGED: i32 = 0x08;
pub const REINDEX_REL_FORCE_INDEXES_PERMANENT: i32 = 0x10;

const Anum_pg_index_indisvalid: i32 = 11;
const Anum_pg_index_indcheckxmin: i32 = 12;
const Anum_pg_index_indisready: i32 = 13;
const Anum_pg_index_indislive: i32 = 14;
const Natts_pg_index: usize = 21;

pub fn reindex_index<'mcx>(
    mcx: Mcx<'mcx>,
    indexId: Oid,
    skip_constraint_checks: bool,
    persistence: u8,
    params: &ReindexParams,
    on_collect: Option<&mut dyn FnMut(Oid)>,
) -> PgResult<()> {
    let ru0 = pg_rusage::pg_rusage_init();
    let missing_ok = params.options & REINDEXOPT_MISSING_OK != 0;
    let heapId = IndexGetRelation(mcx, indexId, missing_ok)?;
    if heapId == InvalidOid {
        return Ok(());
    }
    let heapRelation = if missing_ok {
        match table::try_table_open(mcx, heapId, ShareLock)? {
            Some(rel) => rel,
            None => return Ok(()),
        }
    } else {
        table::table_open(mcx, heapId, ShareLock)?
    };

    let guard = miscinit::SecContextGuard::security_restricted(heapRelation.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let iRel = if missing_ok {
        match indexam::try_index_open(mcx, indexId, AccessExclusiveLock)? {
            Some(rel) => rel,
            None => {
                guc::AtEOXact_GUC(false, save_nestlevel);
                guard.restore();
                return heapRelation.close(NoLock);
            }
        }
    } else {
        indexam::index_open(mcx, indexId, AccessExclusiveLock)?
    };

    // C: EventTriggerCollectSimpleCommand(RelationRelationId, indexId, stmt) —
    // fired only when a REINDEX statement (not an internal caller such as
    // CLUSTER/VACUUM FULL/TRUNCATE) drives this reindex.
    if let Some(cb) = on_collect {
        cb(indexId);
    }

    if iRel.rd_rel.relkind == RELKIND_PARTITIONED_INDEX {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "cannot reindex partitioned index \"{}.{}\"",
                lsyscache::get_namespace_name(mcx, iRel.namespace())?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                iRel.name()
            ),
        )));
    }
    // index.c:3726 — their local buffer manager cannot cope. Without this the
    // rebuild reaches storage and fails by trying to open the owning backend's
    // per-backend temp file.
    if iRel.is_other_temp() {
        return Err(Box::new(
            PgError::error("cannot reindex temporary tables of other sessions")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if catalog::IsToastNamespace(iRel.namespace()) && !lsyscache::get_index_isvalid(indexId)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot reindex invalid index on TOAST table".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if params.tablespace_oid != InvalidOid && catalog::IsSystemRelation(&iRel) {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot move system relation \"{}\"", iRel.name()),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let set_tablespace = params.tablespace_oid != InvalidOid
        && CheckRelationTableSpaceMove(&iRel, params.tablespace_oid)?;

    catalog_heap::CheckTableNotInUse(&iRel, "REINDEX INDEX")?;

    let iRel = if set_tablespace {
        SetRelationTableSpacePgClass(mcx, &iRel, params.tablespace_oid)?;
        catalog_storage::RelationDropStorage(&iRel)?;
        relcache::invalidate::RelationAssumeNewRelfilelocator(&iRel);
        xact::CommandCounterIncrement()?;
        // C's CCI refreshes the entry in place; reopen so rd_rel.reltablespace
        // steers the new relfilenumber below (lock held).
        indexam::index_close(iRel, NoLock)?;
        indexam::index_open(mcx, indexId, NoLock)?
    } else {
        iRel
    };

    predicate_seams::transfer_predicate_locks_to_heap_relation::call(&iRel)?;

    let mut indexInfo = execindexing::BuildIndexInfo(mcx, &iRel)?;
    let mut skipped_constraint = false;
    if skip_constraint_checks {
        if indexInfo.ii_Unique || indexInfo.ii_HasExclusion {
            skipped_constraint = true;
        }
        indexInfo.ii_Unique = false;
        indexInfo.ii_HasExclusion = false;
        indexInfo.ii_ExclusionOps = [InvalidOid; types_core::INDEX_MAX_KEYS as usize];
        indexInfo.ii_ExclusionProcs = [InvalidOid; types_core::INDEX_MAX_KEYS as usize];
        indexInfo.ii_ExclusionStrats = [0; types_core::INDEX_MAX_KEYS as usize];
    }

    types_rel::reindex::set_reindex_processing(
        heapId,
        indexId,
        xact::GetCurrentTransactionNestLevel(),
    );
    let build = (|| -> PgResult<types_rel::Relation<'mcx>> {
        RelationSetNewRelfilenumber(mcx, &iRel, persistence)?;

        // C's CCI inval refreshes the same Relation struct in place; our
        // handles are snapshots, so reopen to see the reset rd_rel
        // (index_update_stats reads reltuples for its -1 hack). Lock held.
        indexam::index_close(iRel, NoLock)?;
        let iRel = indexam::index_open(mcx, indexId, NoLock)?;

        crate::index_build(mcx, &heapRelation, &iRel, &mut indexInfo, true)?;
        Ok(iRel)
    })();
    types_rel::reindex::reset_reindex_processing();
    let iRel = build?;

    if !skipped_constraint {
        reindex_index_flags_fixup(mcx, &heapRelation, indexId, indexInfo.ii_BrokenHotChain)?;
    }

    if params.options & REINDEXOPT_VERBOSE != 0 {
        elog::ereport(types_error::INFO)
            .errmsg(format!(
                "index \"{}\" was reindexed",
                lsyscache::get_rel_name(mcx, indexId)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default()
            ))
            .errdetail_internal(pg_rusage::pg_rusage_show(&ru0).as_str().to_string())
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "reindex_index"))?;
    }

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();

    indexam::index_close(iRel, NoLock)?;
    heapRelation.close(NoLock)
}

const GLOBALTABLESPACE_OID: Oid = 1664;

// CheckRelationTableSpaceMove (tablecmds.c), index arm: storage exists and is
// unmapped by reindex_index's earlier checks.
fn CheckRelationTableSpaceMove(rel: &Relation<'_>, new_tablespace: Oid) -> PgResult<bool> {
    let old_tablespace = rel.rd_rel.reltablespace;
    if new_tablespace == old_tablespace
        || (new_tablespace == init_small::globals::MyDatabaseTableSpace()
            && old_tablespace == InvalidOid)
    {
        return Ok(false);
    }
    if new_tablespace == GLOBALTABLESPACE_OID {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "only shared relations can be placed in pg_global tablespace".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(true)
}

// SetRelationTableSpace (tablecmds.c:3750), storage-bearing arm (no
// tablespace-dependency rewrite; indexes always have storage here).
fn SetRelationTableSpacePgClass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_tablespace: Oid,
) -> PgResult<()> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = [oid_scankey(1, rel.rd_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {}", rel.rd_id));
    // C: SearchSysCacheLockedCopy1 (tablecmds.c:3765) / UnlockTuple (:3777) --
    // reindex_index reaches SetRelationTableSpace through index.c:3774, so this
    // second copy of the function needs the same tuple lock as the first.
    let otid = tup.t_self;
    lmgr::LockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
    let desc = pg_class.descr();
    let natts = desc.natts as usize;
    let store = if new_tablespace == init_small::globals::MyDatabaseTableSpace() {
        InvalidOid
    } else {
        new_tablespace
    };
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    nulls.resize(natts, false);
    replace.resize(natts, false);
    values[ANUM_PG_CLASS_RELTABLESPACE - 1] = Datum::from_oid(store);
    replace[ANUM_PG_CLASS_RELTABLESPACE - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    lmgr::UnlockTuple(&pg_class, &otid, InplaceUpdateTupleLock)?;
    pg_class.close(RowExclusiveLock)
}

const ANUM_PG_CLASS_RELTABLESPACE: usize = 9;

// index.c reindex_index tail: clear indcheckxmin / repair invalid flags on the
// pg_index row. index_bad is reachable only via CONCURRENTLY leftovers (loud
// elsewhere); the indcheckxmin clear is the live arm.
fn reindex_index_flags_fixup<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexId: Oid,
    broken_hot_chain: bool,
) -> PgResult<()> {
    let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
    let key = [oid_scankey(1, indexId)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexId}"));
    let desc = pg_index.descr();
    let get_bool = |attnum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL boolean pg_index columns under pg_index's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d.as_bool()
    };
    let indisvalid = get_bool(Anum_pg_index_indisvalid);
    let indcheckxmin = get_bool(Anum_pg_index_indcheckxmin);
    let indisready = get_bool(Anum_pg_index_indisready);
    let indislive = get_bool(Anum_pg_index_indislive);
    let index_bad = !indisvalid || !indisready || !indislive;

    if index_bad || (indcheckxmin && !broken_hot_chain) {
        let mut values = [Datum::null(); Natts_pg_index];
        let isnull = [false; Natts_pg_index];
        let mut replace = [false; Natts_pg_index];
        let mut set = |anum: i32, d: Datum| {
            values[anum as usize - 1] = d;
            replace[anum as usize - 1] = true;
        };
        if !broken_hot_chain {
            set(Anum_pg_index_indcheckxmin, Datum::from_bool(false));
        } else if index_bad {
            set(Anum_pg_index_indcheckxmin, Datum::from_bool(true));
        }
        set(Anum_pg_index_indisvalid, Datum::from_bool(true));
        set(Anum_pg_index_indisready, Datum::from_bool(true));
        set(Anum_pg_index_indislive, Datum::from_bool(true));
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
        inval::invalidate::CacheInvalidateRelcache(heapRelation)?;
    } else {
        genam::systable_endscan(mcx, scan)?;
    }
    pg_index.close(RowExclusiveLock)
}

pub fn reindex_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    flags: i32,
    params: &ReindexParams,
    on_collect: &mut dyn FnMut(Oid),
) -> PgResult<bool> {
    let rel = if params.options & REINDEXOPT_MISSING_OK != 0 {
        match table::try_table_open(mcx, relid, ShareLock)? {
            Some(rel) => rel,
            None => return Ok(false),
        }
    } else {
        table::table_open(mcx, relid, ShareLock)?
    };
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "cannot reindex partitioned table \"{}.{}\"",
                lsyscache::get_namespace_name(mcx, rel.namespace())?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                rel.name()
            ),
        )));
    }
    let toast_relid = rel.rd_rel.reltoastrelid;
    let indexIds = relcache::indexlist::RelationGetIndexList(mcx, relid)?;

    if flags & REINDEX_REL_SUPPRESS_INDEX_USE != 0 {
        types_rel::reindex::set_reindex_pending(
            &indexIds,
            xact::GetCurrentTransactionNestLevel(),
        );
        xact::CommandCounterIncrement()?;
    }

    let mut result = false;
    if flags & REINDEX_REL_PROCESS_TOAST != 0 && toast_relid != InvalidOid {
        let mut newparams = *params;
        newparams.options &= !REINDEXOPT_MISSING_OK;
        newparams.tablespace_oid = InvalidOid;
        result |= reindex_relation(mcx, toast_relid, flags, &newparams, on_collect)?;
    }

    let persistence = if flags & REINDEX_REL_FORCE_INDEXES_UNLOGGED != 0 {
        types_core::RELPERSISTENCE_UNLOGGED
    } else if flags & REINDEX_REL_FORCE_INDEXES_PERMANENT != 0 {
        types_core::RELPERSISTENCE_PERMANENT
    } else {
        rel.rd_rel.relpersistence
    };

    for &indexOid in indexIds.iter() {
        let indexNamespaceId = lsyscache::get_rel_namespace(indexOid)?;
        if catalog::IsToastNamespace(indexNamespaceId)
            && !lsyscache::get_index_isvalid(indexOid)?
        {
            elog_seams::ereport::call(
                PgError::new(
                    WARNING,
                    format!(
                        "cannot reindex invalid index \"{}.{}\" on TOAST table, skipping",
                        lsyscache::get_namespace_name(mcx, indexNamespaceId)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default(),
                        lsyscache::get_rel_name(mcx, indexOid)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default()
                    ),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            )?;
            if flags & REINDEX_REL_SUPPRESS_INDEX_USE != 0 {
                types_rel::reindex::remove_reindex_pending(indexOid);
            }
            continue;
        }
        reindex_index(
            mcx,
            indexOid,
            flags & REINDEX_REL_CHECK_CONSTRAINTS == 0,
            persistence,
            params,
            Some(&mut *on_collect),
        )?;
        xact::CommandCounterIncrement()?;
        debug_assert!(!types_rel::reindex::ReindexIsProcessingIndex(indexOid));
    }

    rel.close(NoLock)?;
    result |= !indexIds.is_empty();
    Ok(result)
}
