// index.c reindexing-support state (currentlyReindexedHeap/Index,
// pendingReindexedIndexes, reindexingNestLevel), homed below genam/indexam to
// break the genam -> catalog_index dependency cycle; the write side lives in
// catalog_index (accounted on backend-catalog-index). Pending list is a fixed
// array: C's List is unbounded, but a table carrying more indexes than this
// panics loudly rather than silently mistracking.

use core::cell::Cell;

use types_core::{InvalidOid, Oid};

const PENDING_CAP: usize = 64;

thread_local! {
    static CURRENTLY_REINDEXED_HEAP: Cell<Oid> = const { Cell::new(InvalidOid) };
    static CURRENTLY_REINDEXED_INDEX: Cell<Oid> = const { Cell::new(InvalidOid) };
    static PENDING: Cell<[Oid; PENDING_CAP]> = const { Cell::new([InvalidOid; PENDING_CAP]) };
    static PENDING_LEN: Cell<usize> = const { Cell::new(0) };
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
    let len = PENDING_LEN.with(|c| c.get());
    len != 0 && PENDING.with(|p| p.get()[..len].contains(&indexOid))
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
    if PENDING_LEN.with(|c| c.get()) != 0 {
        panic!("cannot reindex while reindexing");
    }
    if indexes.len() > PENDING_CAP {
        panic!("unported: pendingReindexedIndexes overflow ({} indexes)", indexes.len());
    }
    PENDING.with(|p| {
        let mut arr = [InvalidOid; PENDING_CAP];
        arr[..indexes.len()].copy_from_slice(indexes);
        p.set(arr);
    });
    PENDING_LEN.with(|c| c.set(indexes.len()));
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
}

pub fn remove_reindex_pending(indexOid: Oid) {
    let len = PENDING_LEN.with(|c| c.get());
    if len == 0 {
        return;
    }
    PENDING.with(|p| {
        let mut arr = p.get();
        let mut w = 0;
        for r in 0..len {
            if arr[r] != indexOid {
                arr[w] = arr[r];
                w += 1;
            }
        }
        for slot in arr[w..len].iter_mut() {
            *slot = InvalidOid;
        }
        p.set(arr);
        PENDING_LEN.with(|c| c.set(w));
    });
}

// index.c Estimate/Serialize/RestoreReindexState; the caller supplies C's
// GetCurrentTransactionNestLevel() at restore.
#[derive(Clone)]
pub struct SerializedReindexState {
    heap: Oid,
    index: Oid,
    pending: [Oid; PENDING_CAP],
    pending_len: usize,
}

pub fn serialize_reindex_state() -> SerializedReindexState {
    SerializedReindexState {
        heap: CURRENTLY_REINDEXED_HEAP.with(|c| c.get()),
        index: CURRENTLY_REINDEXED_INDEX.with(|c| c.get()),
        pending: PENDING.with(|p| p.get()),
        pending_len: PENDING_LEN.with(|c| c.get()),
    }
}

pub fn restore_reindex_state(state: &SerializedReindexState, nest_level: i32) {
    CURRENTLY_REINDEXED_HEAP.with(|c| c.set(state.heap));
    CURRENTLY_REINDEXED_INDEX.with(|c| c.set(state.index));
    PENDING.with(|p| p.set(state.pending));
    PENDING_LEN.with(|c| c.set(state.pending_len));
    REINDEXING_NEST_LEVEL.with(|c| c.set(nest_level));
}

pub fn reset_reindex_state(nest_level: i32) {
    if REINDEXING_NEST_LEVEL.with(|c| c.get()) >= nest_level {
        CURRENTLY_REINDEXED_HEAP.with(|c| c.set(InvalidOid));
        CURRENTLY_REINDEXED_INDEX.with(|c| c.set(InvalidOid));
        PENDING_LEN.with(|c| c.set(0));
        REINDEXING_NEST_LEVEL.with(|c| c.set(0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_restore_roundtrip() {
        set_reindex_pending(&[11, 12], 1);
        set_reindex_processing(1, 11, 1);
        let s = serialize_reindex_state();
        reset_reindex_state(0);
        assert!(!ReindexIsProcessingIndex(12));
        restore_reindex_state(&s, 2);
        assert!(ReindexIsProcessingHeap(1));
        assert!(ReindexIsCurrentlyProcessingIndex(11));
        assert!(ReindexIsProcessingIndex(12));
        assert!(!ReindexIsProcessingIndex(11) || ReindexIsCurrentlyProcessingIndex(11));
        reset_reindex_state(0);
    }
}
