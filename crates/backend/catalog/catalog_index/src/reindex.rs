use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, INDEX_RELATION_ID, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, WARNING};
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, ShareLock,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
};

use crate::{oid_scankey, unported, IndexGetRelation, IndexRelidIndexId};

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
// catalog_storage/tableam/catalog_indexing without cycling. The catalog write
// is the unlocked-tuple shape every catalog updater here uses (no
// InplaceUpdateTupleLock; that divergence rides repo-wide). The subid Cells
// are set before CommandCounterIncrement so the inval rebuild's
// copy_preserved carries them onto the rebuilt entry.
pub fn RelationSetNewRelfilenumber<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    persistence: u8,
) -> PgResult<()> {
    if rel.is_mapped() {
        unported("RelationSetNewRelfilenumber: mapped relations");
    }
    let newrelfilenumber =
        catalog::GetNewRelFileNumber(mcx, rel.rd_rel.reltablespace, None, persistence)?;

    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = [oid_scankey(1, rel.rd_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for relation {}", rel.rd_id));

    catalog_storage::RelationDropStorage(rel)?;

    let mut newrlocator = rel.rd_locator.get();
    newrlocator.relNumber = newrelfilenumber;
    let (freeze_xid, minmulti) =
        tableam::table_relation_set_new_filelocator(rel, &newrlocator, persistence as i8)?;

    let mut values = [Datum::null(); Natts_pg_class];
    let isnull = [false; Natts_pg_class];
    let mut replace = [false; Natts_pg_class];
    let mut set = |anum: usize, d: Datum| {
        values[anum - 1] = d;
        replace[anum - 1] = true;
    };
    set(Anum_pg_class_relfilenode, Datum::from_oid(newrelfilenumber));
    set(Anum_pg_class_relpages, Datum::from_i32(0));
    set(Anum_pg_class_reltuples, Datum::from_f32(-1.0));
    set(Anum_pg_class_relallvisible, Datum::from_i32(0));
    set(Anum_pg_class_relallfrozen, Datum::from_i32(0));
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
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    pg_class.close(RowExclusiveLock)?;

    // RelationAssumeNewRelfilelocator + the physical-addr refresh the C
    // in-place rebuild would perform on this same entry.
    rel.rd_locator.set(newrlocator);
    let subid = xact::GetCurrentSubTransactionId();
    rel.rd_newRelfilelocatorSubid.set(subid);
    if rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId {
        rel.rd_firstRelfilelocatorSubid.set(subid);
    }

    xact::CommandCounterIncrement()
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
) -> PgResult<()> {
    let heapId = IndexGetRelation(mcx, indexId, false)?;
    let heapRelation = table::table_open(mcx, heapId, ShareLock)?;

    let guard = miscinit::SecContextGuard::security_restricted(heapRelation.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let iRel = indexam::index_open(mcx, indexId, AccessExclusiveLock)?;

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
    if catalog::IsToastNamespace(iRel.namespace()) && !lsyscache::get_index_isvalid(indexId)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot reindex invalid index on TOAST table".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    catalog_heap::CheckTableNotInUse(&iRel, "REINDEX INDEX")?;

    if xact::IsolationIsSerializable() {
        unported("reindex_index: TransferPredicateLocksToHeapRelation (predicate.c)");
    }

    let mut indexInfo = execindexing::BuildIndexInfo(&iRel);
    let mut skipped_constraint = false;
    if skip_constraint_checks {
        // exclusion constraints are loud inside BuildIndexInfo
        if indexInfo.ii_Unique {
            skipped_constraint = true;
        }
        indexInfo.ii_Unique = false;
    }

    // SetReindexProcessing elided: genam's reindex_is_processing_index is
    // const-false in this tree; systable scans never touch user indexes.
    RelationSetNewRelfilenumber(mcx, &iRel, persistence)?;

    crate::index_build(mcx, &heapRelation, &iRel, &mut indexInfo, true)?;

    if !skipped_constraint {
        reindex_index_flags_fixup(mcx, &heapRelation, indexId, indexInfo.ii_BrokenHotChain)?;
    }

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();

    indexam::index_close(iRel, NoLock)?;
    heapRelation.close(NoLock)
}

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

pub fn reindex_relation<'mcx>(mcx: Mcx<'mcx>, relid: Oid, flags: i32) -> PgResult<bool> {
    if flags & REINDEX_REL_SUPPRESS_INDEX_USE != 0 {
        unported("reindex_relation: REINDEX_REL_SUPPRESS_INDEX_USE (SetReindexPending)");
    }

    let rel = table::table_open(mcx, relid, ShareLock)?;
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

    let mut result = false;
    if flags & REINDEX_REL_PROCESS_TOAST != 0 && toast_relid != InvalidOid {
        result |= reindex_relation(mcx, toast_relid, flags)?;
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
            continue;
        }
        reindex_index(
            mcx,
            indexOid,
            flags & REINDEX_REL_CHECK_CONSTRAINTS == 0,
            persistence,
        )?;
        xact::CommandCounterIncrement()?;
    }

    rel.close(NoLock)?;
    result |= !indexIds.is_empty();
    Ok(result)
}
