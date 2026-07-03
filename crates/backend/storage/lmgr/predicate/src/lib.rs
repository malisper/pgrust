#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::{BlockNumber, TransactionId};
use types_error::PgResult;
use types_rel::RelationData;
use types_snapshot::SnapshotData;
use types_tuple::ItemPointerData;

thread_local! {
    // predicate.c's MySerializableXact != InvalidSerializableXact bit; the
    // only writer (GetSerializableTransactionSnapshot) is unported, so it is
    // provably unset and every SerializationNeededFor{Read,Write} guard takes
    // C's fast exit.
    static MY_SERIALIZABLE_XACT_SET: Cell<bool> = const { Cell::new(false) };
}

#[cold]
fn unported_sxact() -> ! {
    panic!("MySerializableXact is set but storage/lmgr/predicate.c is not ported");
}

fn SerializationNeededForRead(_rel: &RelationData<'_>, _snapshot: &SnapshotData<'_>) -> bool {
    if !MY_SERIALIZABLE_XACT_SET.with(|c| c.get()) {
        return false;
    }
    unported_sxact();
}

fn SerializationNeededForWrite(_rel: &RelationData<'_>) -> bool {
    if !MY_SERIALIZABLE_XACT_SET.with(|c| c.get()) {
        return false;
    }
    unported_sxact();
}

pub fn PredicateLockRelation(rel: &RelationData<'_>, snapshot: &SnapshotData<'_>) -> PgResult<()> {
    if !SerializationNeededForRead(rel, snapshot) {
        return Ok(());
    }
    unported_sxact();
}

pub fn PredicateLockPage(
    rel: &RelationData<'_>,
    _blkno: BlockNumber,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    if !SerializationNeededForRead(rel, snapshot) {
        return Ok(());
    }
    unported_sxact();
}

pub fn PredicateLockTID(
    rel: &RelationData<'_>,
    _tid: ItemPointerData,
    snapshot: &SnapshotData<'_>,
    _tuple_xid: TransactionId,
) -> PgResult<()> {
    if !SerializationNeededForRead(rel, snapshot) {
        return Ok(());
    }
    unported_sxact();
}

pub fn CheckForSerializableConflictOutNeeded(
    rel: &RelationData<'_>,
    snapshot: &SnapshotData<'_>,
) -> bool {
    if !SerializationNeededForRead(rel, snapshot) {
        return false;
    }
    unported_sxact();
}

pub fn CheckForSerializableConflictOut(
    rel: &RelationData<'_>,
    _xid: TransactionId,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    if !SerializationNeededForRead(rel, snapshot) {
        return Ok(());
    }
    unported_sxact();
}

// C gates on PredXact->SxactGlobalXmin (any serializable xact system-wide);
// single-backend approximation matches the rest of this crate.
pub fn PredicateLockPageSplit(
    _rel: &RelationData<'_>,
    _oldblkno: BlockNumber,
    _newblkno: BlockNumber,
) -> PgResult<()> {
    if !MY_SERIALIZABLE_XACT_SET.with(|c| c.get()) {
        return Ok(());
    }
    unported_sxact();
}

// PredicateLockPageCombine: same single-backend gate as PageSplit.
pub fn PredicateLockPageCombine(
    _rel: &RelationData<'_>,
    _oldblkno: BlockNumber,
    _newblkno: BlockNumber,
) -> PgResult<()> {
    if !MY_SERIALIZABLE_XACT_SET.with(|c| c.get()) {
        return Ok(());
    }
    unported_sxact();
}

pub fn CheckForSerializableConflictIn(
    rel: &RelationData<'_>,
    _tid: Option<&ItemPointerData>,
    _blkno: BlockNumber,
) -> PgResult<()> {
    if !SerializationNeededForWrite(rel) {
        return Ok(());
    }
    unported_sxact();
}

fn my_sxact_is_invalid() -> bool {
    if !MY_SERIALIZABLE_XACT_SET.with(|c| c.get()) {
        return true;
    }
    unported_sxact();
}

pub fn PreCommit_CheckForSerializationFailure() -> PgResult<()> {
    my_sxact_is_invalid();
    Ok(())
}

pub fn RegisterPredicateLockingXid(_xid: TransactionId) -> PgResult<()> {
    my_sxact_is_invalid();
    Ok(())
}

pub fn AtPrepare_PredicateLocks() -> PgResult<()> {
    my_sxact_is_invalid();
    Ok(())
}

pub fn PostPrepare_PredicateLocks(_xid: TransactionId) -> PgResult<()> {
    my_sxact_is_invalid();
    Ok(())
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
    predicate_seams::predicate_lock_page_split::set(PredicateLockPageSplit);
    predicate_seams::predicate_lock_page_combine::set(PredicateLockPageCombine);
    predicate_seams::pre_commit_check_for_serialization_failure::set(
        PreCommit_CheckForSerializationFailure,
    );
    predicate_seams::register_predicate_locking_xid::set(RegisterPredicateLockingXid);
    predicate_seams::at_prepare_predicate_locks::set(AtPrepare_PredicateLocks);
    predicate_seams::post_prepare_predicate_locks::set(PostPrepare_PredicateLocks);
}

#[cfg(test)]
mod tests;
