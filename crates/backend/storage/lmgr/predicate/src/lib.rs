#![allow(non_snake_case)]

pub mod engine;
mod ilist;
pub mod internals;
pub mod serial;

use types_core::{BlockNumber, Oid, TransactionId};
use types_error::PgResult;
use types_rel::RelationData;
use types_snapshot::SnapshotData;
use types_tuple::ItemPointerData;

pub use engine::{
    predicatelock_twophase_recover, GetPredicateLockStatusData, GetSafeSnapshotBlockingPids,
    PredicateLockShmemInit, PredicateLockShmemResetAfterCrash, PredicateLockShmemSize,
    PredicateLockStatusEntry, PredicateLockTwoPhaseFinish, ReleasePredicateLocks,
    SerializableXactActive,
};
pub use serial::CheckPointPredicate;

struct RelFields {
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    is_index: bool,
}

#[inline]
fn rel_fields(rel: &RelationData<'_>) -> RelFields {
    RelFields {
        db_oid: rel.rd_locator.get().dbOid,
        rd_id: rel.rd_id,
        uses_local_buffers: rel.uses_local_buffers(),
        is_index: rel.rd_index.is_some(),
    }
}

pub fn PredicateLockRelation(rel: &RelationData<'_>, snapshot: &SnapshotData<'_>) -> PgResult<()> {
    if !engine::SerializableXactActive() {
        return Ok(());
    }
    let f = rel_fields(rel);
    engine::PredicateLockRelation(f.db_oid, f.rd_id, f.uses_local_buffers, snapshot)
}

pub fn PredicateLockPage(
    rel: &RelationData<'_>,
    blkno: BlockNumber,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    if !engine::SerializableXactActive() {
        return Ok(());
    }
    let f = rel_fields(rel);
    engine::PredicateLockPage(f.db_oid, f.rd_id, f.uses_local_buffers, blkno, snapshot)
}

pub fn PredicateLockTID(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    snapshot: &SnapshotData<'_>,
    tuple_xid: TransactionId,
) -> PgResult<()> {
    if !engine::SerializableXactActive() {
        return Ok(());
    }
    let f = rel_fields(rel);
    engine::PredicateLockTID(
        f.db_oid,
        f.rd_id,
        f.uses_local_buffers,
        f.is_index,
        types_tuple::itemptr::ItemPointerGetBlockNumber(&tid),
        tid.ip_posid,
        snapshot,
        tuple_xid,
    )
}

pub fn CheckForSerializableConflictOutNeeded(
    rel: &RelationData<'_>,
    snapshot: &SnapshotData<'_>,
) -> PgResult<bool> {
    if !engine::SerializableXactActive() {
        return Ok(false);
    }
    let f = rel_fields(rel);
    engine::CheckForSerializableConflictOutNeeded(f.rd_id, f.uses_local_buffers, snapshot)
}

pub fn CheckForSerializableConflictOut(
    rel: &RelationData<'_>,
    xid: TransactionId,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    if !engine::SerializableXactActive() {
        return Ok(());
    }
    let f = rel_fields(rel);
    engine::CheckForSerializableConflictOut(f.rd_id, f.uses_local_buffers, xid, snapshot)
}

// C gates on PredXact->SxactGlobalXmin (any serializable xact system-wide);
// SerializableXactActive is only a pre-filter for the backend-local fast path,
// so the engine is entered whenever shmem is up.
pub fn PredicateLockPageSplit(
    rel: &RelationData<'_>,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) -> PgResult<()> {
    let f = rel_fields(rel);
    engine::PredicateLockPageSplit(f.db_oid, f.rd_id, f.uses_local_buffers, oldblkno, newblkno)
}

pub fn PredicateLockPageCombine(
    rel: &RelationData<'_>,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) -> PgResult<()> {
    let f = rel_fields(rel);
    engine::PredicateLockPageCombine(f.db_oid, f.rd_id, f.uses_local_buffers, oldblkno, newblkno)
}

pub fn CheckTableForSerializableConflictIn(rel: &RelationData<'_>) -> PgResult<()> {
    let f = rel_fields(rel);
    engine::CheckTableForSerializableConflictIn(f.db_oid, f.rd_id, f.rd_id, f.uses_local_buffers)
}

// Gate lives in the engine: PredXact->SxactGlobalXmin, not this backend's
// isolation level.
pub fn TransferPredicateLocksToHeapRelation(rel: &RelationData<'_>) -> PgResult<()> {
    let f = rel_fields(rel);
    let heap_id = rel.rd_index.as_ref().map_or(f.rd_id, |ix| ix.indrelid);
    engine::TransferPredicateLocksToHeapRelation(
        f.db_oid,
        f.rd_id,
        heap_id,
        f.uses_local_buffers,
        f.is_index,
    )
}

pub fn CheckForSerializableConflictIn(
    rel: &RelationData<'_>,
    tid: Option<&ItemPointerData>,
    blkno: BlockNumber,
) -> PgResult<()> {
    if !engine::SerializableXactActive() {
        return Ok(());
    }
    let f = rel_fields(rel);
    engine::CheckForSerializableConflictIn(
        f.db_oid,
        f.rd_id,
        f.uses_local_buffers,
        tid.map(|t| (types_tuple::itemptr::ItemPointerGetBlockNumber(t), t.ip_posid)),
        blkno,
    )
}

pub fn init_seams() {
    predicate_seams::predicate_lock_relation::set(PredicateLockRelation);
    predicate_seams::predicate_lock_page::set(PredicateLockPage);
    predicate_seams::predicate_lock_tid::set(PredicateLockTID);
    predicate_seams::check_for_serializable_conflict_out_needed::set(
        CheckForSerializableConflictOutNeeded,
    );
    predicate_seams::check_for_serializable_conflict_out::set(CheckForSerializableConflictOut);
    predicate_seams::check_for_serializable_conflict_in::set(CheckForSerializableConflictIn);
    predicate_seams::check_table_for_serializable_conflict_in::set(
        CheckTableForSerializableConflictIn,
    );
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(
        TransferPredicateLocksToHeapRelation,
    );
    predicate_seams::predicate_lock_page_split::set(PredicateLockPageSplit);
    predicate_seams::predicate_lock_page_combine::set(PredicateLockPageCombine);
    predicate_seams::pre_commit_check_for_serialization_failure::set(
        engine::PreCommit_CheckForSerializationFailure,
    );
    predicate_seams::register_predicate_locking_xid::set(engine::RegisterPredicateLockingXid);
    predicate_seams::at_prepare_predicate_locks::set(engine::AtPrepare_PredicateLocks);
    predicate_seams::post_prepare_predicate_locks::set(engine::PostPrepare_PredicateLocks);
    predicate_seams::check_point_predicate::set(serial::CheckPointPredicate);
    predicate_seams::get_serializable_transaction_snapshot::set(
        engine::GetSerializableTransactionSnapshot,
    );
    predicate_seams::release_predicate_locks::set(engine::ReleasePredicateLocks);
    predicate_seams::share_serializable_xact::set(engine::ShareSerializableXact);
    predicate_seams::attach_serializable_xact::set(engine::AttachSerializableXact);
    predicate_seams::set_serializable_transaction_snapshot::set(
        engine::SetSerializableTransactionSnapshot,
    );

    {
        use guc_tables::GucVarAccessors;
        guc_tables::vars::max_predicate_locks_per_xact.install(GucVarAccessors {
            get: engine::max_predicate_locks_per_xact,
            set: engine::set_max_predicate_locks_per_xact,
        });
        guc_tables::vars::max_predicate_locks_per_relation.install(GucVarAccessors {
            get: engine::max_predicate_locks_per_relation,
            set: engine::set_max_predicate_locks_per_relation,
        });
        guc_tables::vars::max_predicate_locks_per_page.install(GucVarAccessors {
            get: engine::max_predicate_locks_per_page,
            set: engine::set_max_predicate_locks_per_page,
        });
        fn check_serial_buffers_hook(
            newval: &mut i32,
            _extra: &mut Option<guc_tables::GucHookExtra>,
            _source: types_guc::GucSource,
        ) -> PgResult<bool> {
            let (ok, detail) = slru::check_slru_buffers("serializable_buffers", *newval);
            if !ok {
                if let Some(d) = detail {
                    guc_seams::guc_check_errdetail::call(d);
                }
            }
            Ok(ok)
        }
        guc_tables::hooks::check_serial_buffers.install(check_serial_buffers_hook);
    }
}

#[cfg(test)]
mod tests;
