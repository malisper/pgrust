// index.c reindexing-support state (currentlyReindexedHeap/Index,
// pendingReindexedIndexes, reindexingNestLevel), homed below genam/indexam to
// break the genam -> catalog_index dependency cycle; the write side lives in
// catalog_index (accounted on backend-catalog-index). The pending list is
// unbounded, as C's List is.

use core::cell::{Cell, RefCell};

use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERROR};

thread_local! {
    static CURRENTLY_REINDEXED_HEAP: Cell<Oid> = const { Cell::new(InvalidOid) };
    static CURRENTLY_REINDEXED_INDEX: Cell<Oid> = const { Cell::new(InvalidOid) };
    static PENDING: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
    static REINDEXING_NEST_LEVEL: Cell<i32> = const { Cell::new(0) };
}

#[inline]
pub fn ReindexIsProcessingHeap(heapOid: Oid) -> bool {
    CURRENTLY_REINDEXED_HEAP.with(|c| c.get()) == heapOid
}

#[inline]
pub fn ReindexIsCurrentlyProcessingIndex(indexOid: Oid) -> bool {
    CURRENTLY_REINDEXED_INDEX.with(|c| c.get()) == indexOid
}

#[inline]
pub fn ReindexIsProcessingIndex(indexOid: Oid) -> bool {
    if CURRENTLY_REINDEXED_INDEX.with(|c| c.get()) == indexOid {
        return true;
    }
    PENDING.with(|p| p.borrow().contains(&indexOid))
}

// index.c:4185 SetReindexProcessing; the caller supplies C's
// GetCurrentTransactionNestLevel() and IsInParallelMode() (the latter is read
// by the RemoveReindexPending step, index.c:4232).
pub fn set_reindex_processing(
    heapOid: Oid,
    indexOid: Oid,
    nest_level: i32,
    in_parallel_mode: bool,
) -> PgResult<()> {
    assert!(heapOid != InvalidOid && indexOid != InvalidOid);
    // Reindexing is not re-entrant.
    if CURRENTLY_REINDEXED_HEAP.with(|c| c.get()) != InvalidOid {
        return Err(cannot_reindex_while_reindexing());
    }
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(heapOid));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(indexOid));
    // Index is no longer "pending" reindex.
    remove_reindex_pending(indexOid, in_parallel_mode)?;
    // This may have been set already, but in case it isn't, do so now.
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
    Ok(())
}

pub fn reset_reindex_processing() {
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(InvalidOid));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(InvalidOid));
}

// index.c:4217 SetReindexPending; the caller supplies C's
// GetCurrentTransactionNestLevel() and IsInParallelMode().
pub fn set_reindex_pending(
    indexes: &[Oid],
    nest_level: i32,
    in_parallel_mode: bool,
) -> PgResult<()> {
    // Reindexing is not re-entrant.
    if PENDING.with(|p| !p.borrow().is_empty()) {
        return Err(cannot_reindex_while_reindexing());
    }
    if in_parallel_mode {
        return Err(cannot_modify_reindex_state_in_parallel());
    }
    // C list_copy of the caller's list.
    PENDING.with(|p| *p.borrow_mut() = indexes.to_vec());
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
    Ok(())
}

// index.c:4232 RemoveReindexPending; the caller supplies C's IsInParallelMode().
pub fn remove_reindex_pending(indexOid: Oid, in_parallel_mode: bool) -> PgResult<()> {
    if in_parallel_mode {
        return Err(cannot_modify_reindex_state_in_parallel());
    }
    PENDING.with(|p| p.borrow_mut().retain(|&oid| oid != indexOid));
    Ok(())
}

// elog(ERROR, ...) at index.c:4186 / 4220: internal error, no SQLSTATE beyond
// XX000, message bytes verbatim.
fn cannot_reindex_while_reindexing() -> Box<PgError> {
    Box::new(PgError::new(ERROR, "cannot reindex while reindexing"))
}

// elog(ERROR, ...) at index.c:4222 / 4234.
fn cannot_modify_reindex_state_in_parallel() -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        "cannot modify reindex state during a parallel operation",
    ))
}

// index.c Estimate/Serialize/RestoreReindexState; the caller supplies C's
// GetCurrentTransactionNestLevel() at restore.
#[derive(Clone)]
pub struct SerializedReindexState {
    heap: Oid,
    index: Oid,
    pending: Vec<Oid>,
}

pub fn serialize_reindex_state() -> SerializedReindexState {
    SerializedReindexState {
        heap: CURRENTLY_REINDEXED_HEAP.with(|c| c.get()),
        index: CURRENTLY_REINDEXED_INDEX.with(|c| c.get()),
        pending: PENDING.with(|p| p.borrow().clone()),
    }
}

pub fn restore_reindex_state(state: &SerializedReindexState, nest_level: i32) {
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(state.heap));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(state.index));
    PENDING.with(|p| *p.borrow_mut() = state.pending.clone());
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
}

pub fn reset_reindex_state(nest_level: i32) {
    if REINDEXING_NEST_LEVEL.with(|c| c.get()) >= nest_level {
        CURRENTLY_REINDEXED_HEAP.with(|c| c.set(InvalidOid));
        CURRENTLY_REINDEXED_INDEX.with(|c| c.set(InvalidOid));
        PENDING.with(|p| p.borrow_mut().clear());
        REINDEXING_NEST_LEVEL.with(|c| c.set(0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Witness for the retired PENDING_CAP=64 fence: C's pendingReindexedIndexes
    // is an unbounded List, so a REINDEX TABLE covering >64 indexes must track
    // every one (index.c SetReindexPending / ReindexIsProcessingIndex).
    #[test]
    fn pending_list_is_unbounded() {
        let indexes: Vec<Oid> = (1..=100).collect();
        set_reindex_pending(&indexes, 1, false).unwrap();
        assert!(ReindexIsProcessingIndex(65));
        assert!(ReindexIsProcessingIndex(100));
        assert!(!ReindexIsProcessingIndex(101));

        // RemoveReindexPending drops exactly the named index.
        remove_reindex_pending(65, false).unwrap();
        assert!(!ReindexIsProcessingIndex(65));
        assert!(ReindexIsProcessingIndex(66));

        // Serialize/restore round-trips the full list (parallel workers).
        let state = serialize_reindex_state();
        reset_reindex_state(0);
        assert!(!ReindexIsProcessingIndex(100));
        restore_reindex_state(&state, 1);
        assert!(ReindexIsProcessingIndex(100));
        assert!(!ReindexIsProcessingIndex(65));

        reset_reindex_state(0);
    }

    // index.c:4217 SetReindexPending: a second call while the pending list is
    // populated is elog(ERROR, "cannot reindex while reindexing") — a
    // catchable PgError (XX000), never a panic — and leaves the list as it was.
    #[test]
    fn set_pending_is_not_reentrant() {
        set_reindex_pending(&[7, 8], 1, false).unwrap();
        let err = set_reindex_pending(&[9], 1, false)
            .expect_err("second SetReindexPending must fail");
        assert_eq!(err.level(), ERROR);
        assert_eq!(err.message(), "cannot reindex while reindexing");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert!(ReindexIsProcessingIndex(7));
        assert!(!ReindexIsProcessingIndex(9));
        reset_reindex_state(0);
    }

    // index.c:4185 SetReindexProcessing: a nested call while an index is being
    // rebuilt is the same elog(ERROR); the outer state is untouched.
    #[test]
    fn set_processing_is_not_reentrant() {
        set_reindex_processing(11, 12, 1, false).unwrap();
        let err = set_reindex_processing(21, 22, 1, false)
            .expect_err("nested SetReindexProcessing must fail");
        assert_eq!(err.level(), ERROR);
        assert_eq!(err.message(), "cannot reindex while reindexing");
        assert!(ReindexIsProcessingHeap(11));
        assert!(ReindexIsCurrentlyProcessingIndex(12));
        assert!(!ReindexIsProcessingHeap(21));
        reset_reindex_processing();
        assert!(!ReindexIsProcessingHeap(11));
        reset_reindex_state(0);
    }

    // index.c:4219 SetReindexPending / index.c:4232 RemoveReindexPending refuse
    // to touch the pending list while IsInParallelMode(); SetReindexProcessing
    // reaches the same check through RemoveReindexPending after it has already
    // recorded the heap/index (C order), so the abort path clears them.
    #[test]
    fn pending_state_refuses_parallel_mode() {
        let msg = "cannot modify reindex state during a parallel operation";
        let err = set_reindex_pending(&[7], 1, true)
            .expect_err("SetReindexPending in parallel mode");
        assert_eq!(err.level(), ERROR);
        assert_eq!(err.message(), msg);
        assert!(!ReindexIsProcessingIndex(7));

        set_reindex_pending(&[7, 8], 1, false).unwrap();
        let err = remove_reindex_pending(7, true)
            .expect_err("RemoveReindexPending in parallel mode");
        assert_eq!(err.message(), msg);
        assert!(ReindexIsProcessingIndex(7));

        let err = set_reindex_processing(11, 8, 1, true)
            .expect_err("SetReindexProcessing in parallel mode");
        assert_eq!(err.message(), msg);
        assert!(ReindexIsProcessingHeap(11));
        assert!(ReindexIsProcessingIndex(8));
        reset_reindex_state(0);
        assert!(!ReindexIsProcessingHeap(11));
        assert!(!ReindexIsProcessingIndex(8));

        // The non-re-entrancy check precedes the parallel-mode check (C order).
        set_reindex_pending(&[7], 1, false).unwrap();
        let err = set_reindex_pending(&[9], 1, true).expect_err("re-entrant SetReindexPending");
        assert_eq!(err.message(), "cannot reindex while reindexing");
        reset_reindex_state(0);
    }
}
