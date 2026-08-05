// index.c reindexing-support state (currentlyReindexedHeap/Index,
// pendingReindexedIndexes, reindexingNestLevel), homed below genam/indexam to
// break the genam -> catalog_index dependency cycle; the write side lives in
// catalog_index (accounted on backend-catalog-index). The pending list is
// unbounded, as C's List is.

use core::cell::{Cell, RefCell};

use types_core::{InvalidOid, Oid};

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

pub fn set_reindex_processing(heapOid: Oid, indexOid: Oid, nest_level: i32) {
    assert!(heapOid != InvalidOid && indexOid != InvalidOid);
    if CURRENTLY_REINDEXED_HEAP.with(|c| c.get()) != InvalidOid {
        panic!("cannot reindex while reindexing");
    }
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(heapOid));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(indexOid));
    remove_reindex_pending(indexOid);
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
}

pub fn reset_reindex_processing() {
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(InvalidOid));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(InvalidOid));
}

pub fn set_reindex_pending(indexes: &[Oid], nest_level: i32) {
    if PENDING.with(|p| !p.borrow().is_empty()) {
        panic!("cannot reindex while reindexing");
    }
    // C list_copy of the caller's list.
    PENDING.with(|p| *p.borrow_mut() = indexes.to_vec());
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
}

pub fn remove_reindex_pending(indexOid: Oid) {
    PENDING.with(|p| p.borrow_mut().retain(|&oid| oid != indexOid));
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
        set_reindex_pending(&indexes, 1);
        assert!(ReindexIsProcessingIndex(65));
        assert!(ReindexIsProcessingIndex(100));
        assert!(!ReindexIsProcessingIndex(101));

        // RemoveReindexPending drops exactly the named index.
        remove_reindex_pending(65);
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

    #[test]
    fn set_pending_is_not_reentrant() {
        set_reindex_pending(&[7, 8], 1);
        let err = std::panic::catch_unwind(|| set_reindex_pending(&[9], 1))
            .expect_err("second SetReindexPending must fail");
        let msg = err.downcast_ref::<&str>().copied().unwrap_or_default();
        assert_eq!(msg, "cannot reindex while reindexing");
        reset_reindex_state(0);
    }
}
