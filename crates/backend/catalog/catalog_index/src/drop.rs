// index_drop / IndexGetRelation (index.c).
use mcx::Mcx;
use types_core::{InvalidOid, InvalidTransactionId, Oid, INDEX_RELATION_ID};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_rel::{AccessExclusiveLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_storage::lock::LOCKTAG;

use crate::{err, getattr, oid_scankey, IndexRelidIndexId, IndexStateFlagsAction};

const IndexIndrelidIndexId: Oid = 2678;
const Anum_pg_index_indexrelid: usize = 1;
const Anum_pg_index_indrelid: usize = 2;
const Anum_pg_index_indexprs: i32 = 20;

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

pub fn index_drop<'mcx>(
    mcx: Mcx<'mcx>,
    indexId: Oid,
    concurrent: bool,
    concurrent_lock_mode: bool,
) -> PgResult<()> {
    debug_assert!(
        lsyscache::get_rel_persistence(indexId)? != types_core::RELPERSISTENCE_TEMP as i8
            || (!concurrent && !concurrent_lock_mode)
    );
    let lockmode = if concurrent || concurrent_lock_mode {
        ShareUpdateExclusiveLock
    } else {
        AccessExclusiveLock
    };

    let heapId = IndexGetRelation(mcx, indexId, false)?;
    let mut userHeapRelation = table::table_open(mcx, heapId, lockmode)?;
    let mut userIndexRelation = indexam::index_open(mcx, indexId, lockmode)?;

    catalog_heap::CheckTableNotInUse(&userIndexRelation, "DROP INDEX")?;

    let mut session_relids = None;
    if concurrent {
        if xact::GetTopTransactionIdIfAny() != InvalidTransactionId {
            return Err(err(
                "DROP INDEX CONCURRENTLY must be first action in transaction".into(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }

        crate::index_set_state_flags(mcx, indexId, IndexStateFlagsAction::DropClearValid)?;

        inval::invalidate::CacheInvalidateRelcache(&userHeapRelation)?;

        let heaprelid = userHeapRelation.rd_lockInfo.lockRelId;
        let heaplocktag = [LOCKTAG::relation(heaprelid.dbId, heaprelid.relId)];
        let indexrelid = userIndexRelation.rd_lockInfo.lockRelId;

        userHeapRelation.close(NoLock)?;
        indexam::index_close(userIndexRelation, NoLock)?;

        lmgr::LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock)?;
        lmgr::LockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock)?;
        session_relids = Some((heaprelid, indexrelid));

        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        xact::StartTransactionCommand()?;

        lmgr::WaitForLockersMultiple(mcx, &heaplocktag, AccessExclusiveLock)?;

        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;
        crate::index_concurrently_set_dead(mcx, heapId, indexId)?;
        snapmgr::PopActiveSnapshot()?;

        xact::CommitTransactionCommand()?;
        xact::StartTransactionCommand()?;

        lmgr::WaitForLockersMultiple(mcx, &heaplocktag, AccessExclusiveLock)?;

        userHeapRelation = table::table_open(mcx, heapId, ShareUpdateExclusiveLock)?;
        userIndexRelation = indexam::index_open(mcx, indexId, AccessExclusiveLock)?;
    } else {
        predicate_seams::transfer_predicate_locks_to_heap_relation::call(&userIndexRelation)?;
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

    pg_inherits::DeleteInheritsTuple(mcx, indexId, InvalidOid, false, None)?;

    inval::invalidate::CacheInvalidateRelcache(&userHeapRelation)?;

    userHeapRelation.close(NoLock)?;

    if let Some((heaprelid, indexrelid)) = session_relids {
        lmgr::UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock)?;
        lmgr::UnlockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock)?;
    }
    Ok(())
}

const _: () = assert!(IndexIndrelidIndexId == 2678);
