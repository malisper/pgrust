// index_drop / IndexGetRelation (index.c), non-concurrent lane; concurrent
// drop, expression statistics and pg_inherits rows are loud or unreachable.
use mcx::Mcx;
use types_core::{InvalidOid, Oid, INDEX_RELATION_ID};
use types_error::PgResult;
use types_rel::{AccessExclusiveLock, NoLock, RowExclusiveLock};

use crate::{getattr, oid_scankey, unported, IndexRelidIndexId};

const IndexIndrelidIndexId: Oid = 2678;
const Anum_pg_index_indexrelid: usize = 1;
const Anum_pg_index_indrelid: usize = 2;
const Anum_pg_index_indexprs: i32 = 20;
const InheritsRelidSeqnoIndexId: Oid = 2680;
const InheritsRelationId: Oid = 2611;

// IndexGetRelation: pg_index.indrelid for the index.
pub fn IndexGetRelation<'mcx>(mcx: Mcx<'mcx>, indexId: Oid, missing_ok: bool) -> PgResult<Oid> {
    let rel = table::table_open(mcx, INDEX_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_scankey(Anum_pg_index_indexrelid, indexId);
    let mut scan = genam::systable_beginscan(mcx, &rel, IndexRelidIndexId, true, None, &[key])?;
    let result = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => getattr(tup, Anum_pg_index_indrelid, rel.descr()).as_oid(),
        None if missing_ok => InvalidOid,
        None => panic!("cache lookup failed for index {indexId}"),
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn index_drop<'mcx>(mcx: Mcx<'mcx>, indexId: Oid, concurrent: bool) -> PgResult<()> {
    if concurrent {
        unported("index_drop: concurrent lane");
    }

    let heapId = IndexGetRelation(mcx, indexId, false)?;
    let userHeapRelation = table::table_open(mcx, heapId, AccessExclusiveLock)?;
    let userIndexRelation = indexam::index_open(mcx, indexId, AccessExclusiveLock)?;

    catalog_heap::CheckTableNotInUse(&userIndexRelation, "DROP INDEX")?;

    if xact::IsolationIsSerializable() {
        unported("index_drop: TransferPredicateLocksToHeapRelation (predicate.c)");
    }

    if types_rel::RELKIND_HAS_STORAGE(userIndexRelation.rd_rel.relkind) {
        catalog_storage::RelationDropStorage(&userIndexRelation)?;
    }
    pgstat::relation::pgstat_drop_relation(indexId, userIndexRelation.rd_rel.relisshared);

    indexam::index_close(userIndexRelation, NoLock)?;
    relcache::invalidate::RelationForgetRelation(indexId)?;

    let snapshot = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snapshot)?;

    let hasexprs;
    {
        let indexRelation = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
        let key = oid_scankey(Anum_pg_index_indexrelid, indexId);
        let mut scan =
            genam::systable_beginscan(mcx, &indexRelation, IndexRelidIndexId, true, None, &[key])?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {indexId}"));
        let mut isnull = false;
        // SAFETY: indexprs under pg_index's descriptor; null test only.
        unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_index_indexprs,
                indexRelation.descr(),
                &mut isnull,
            )
        };
        hasexprs = !isnull;
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&indexRelation, &tid)?;
        genam::systable_endscan(mcx, scan)?;
        indexRelation.close(RowExclusiveLock)?;
    }

    snapmgr::PopActiveSnapshot()?;

    if hasexprs {
        catalog_heap::RemoveStatistics(mcx, indexId, 0)?;
    }

    catalog_heap::DeleteAttributeTuples(mcx, indexId)?;
    catalog_heap::DeleteRelationTuple(mcx, indexId)?;

    DeleteInheritsTuple(mcx, indexId)?;

    inval::invalidate::CacheInvalidateRelcache(&userHeapRelation)?;

    userHeapRelation.close(NoLock)
}

// DeleteInheritsTuple (pg_inherits.c): a plain index has no pg_inherits rows;
// finding one means the partitioned-index lane leaked in.
fn DeleteInheritsTuple<'mcx>(mcx: Mcx<'mcx>, inhrelid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let key = oid_scankey(1, inhrelid);
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &[key])?;
    if genam::systable_getnext(mcx, &mut scan)?.is_some() {
        unported("DeleteInheritsTuple: pg_inherits rows on a dropped index");
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

const _: () = assert!(IndexIndrelidIndexId == 2678);
