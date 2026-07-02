//! heapam_handler.c read lane: the heap AM's table-AM callbacks that are not
//! 1:1 re-exports of heapam.c functions (those bind directly in tableam's
//! dispatch arms). DML and utility callbacks stay loud in tableam until
//! heapam phase 2.

#![allow(non_snake_case)]

use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use ::heapam::{heap_fetch, heap_get_latest_tid, heap_hot_search_buffer, HeapScanDescData};
use ::mcx::Mcx;
use ::tableam_vocab::Snapshot;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_slot::SlotData;
use ::types_snapshot::{IsMVCCSnapshot, SnapshotData};
use ::types_tuple::{
    HeapTupleData, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerIsValid,
};

#[cfg(test)]
mod tests;

#[cold]
#[inline(never)]
fn wrong_slot() -> ! {
    panic!("heap AM callback requires a BufferHeapTuple slot (C Assert(TTS_IS_BUFFERTUPLE))")
}

#[cold]
#[inline(never)]
fn snapshot_any_unported(what: &'static str) -> ! {
    panic!(
        "backend-access-heap-heapam-handler: SnapshotAny {what} unported \
         (visibility seam requires a real snapshot)"
    )
}

// relscan.h IndexFetchHeapData; xs_base.rel folded in, xs_cbuf as the pin guard.
pub struct IndexFetchHeapData<'mcx> {
    pub xs_rel: Relation<'mcx>,
    pub xs_cbuf: Option<BufferPin>,
}

pub fn heapam_index_fetch_begin<'mcx>(rel: &Relation<'mcx>) -> IndexFetchHeapData<'mcx> {
    IndexFetchHeapData {
        xs_rel: rel.alias(),
        xs_cbuf: None,
    }
}

pub fn heapam_index_fetch_reset(hscan: &mut IndexFetchHeapData<'_>) {
    if let Some(pin) = hscan.xs_cbuf.take() {
        pin.release();
    }
}

pub fn heapam_index_fetch_end(mut hscan: IndexFetchHeapData<'_>) {
    heapam_index_fetch_reset(&mut hscan);
}

pub fn heapam_index_fetch_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    hscan: &mut IndexFetchHeapData<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
    call_again: &mut bool,
    mut all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    if !matches!(slot, SlotData::BufferHeap(_)) {
        wrong_slot();
    }
    let snap: &SnapshotData<'_> = match snapshot.as_deref() {
        Some(s) => s,
        None => snapshot_any_unported("index fetch"),
    };

    // Skip the buffer-switching logic when mid-HOT chain.
    if !*call_again {
        let blkno = ItemPointerGetBlockNumber(tid);
        // ReleaseAndReadBuffer: keep the pin when it already covers the block.
        let same = hscan
            .xs_cbuf
            .as_ref()
            .is_some_and(|pin| pin.block_number() == blkno);
        if !same {
            if let Some(prev) = hscan.xs_cbuf.take() {
                prev.release();
            }
            let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(&hscan.xs_rel, blkno)?)
                .expect("ReadBuffer returned InvalidBuffer");
            // Prune page, but only if we weren't already on this page.
            pruneheap_seams::heap_page_prune_opt::call(&hscan.xs_rel, pin.buffer())?;
            hscan.xs_cbuf = Some(pin);
        }
    }

    let pin = hscan
        .xs_cbuf
        .as_ref()
        .expect("index fetch continuation without a pinned buffer");

    // Share-lock the buffer so we can examine visibility.
    let found: Option<(*const u8, u32, ItemPointerData)>;
    {
        let lock = pin.lock_share()?;
        let res = heap_hot_search_buffer(
            *tid,
            &hscan.xs_rel,
            pin,
            snap,
            all_dead.is_some(),
            !*call_again,
        )?;
        if let (Some(dst), Some(v)) = (all_dead.as_deref_mut(), res.all_dead) {
            *dst = v;
        }
        found = if res.found {
            // C mutates *tid to the resolved chain member only on success; the
            // continuation call restarts the HOT walk from this offset.
            *tid = res.tid;
            let t = res.tuple.as_ref().expect("found without tuple");
            Some((t.header_ptr(), t.t_len, res.tid))
        } else {
            None
        };
        drop(lock);
    }

    match found {
        Some((ptr, len, self_tid)) => {
            // Only in a non-MVCC snapshot can more than one member of the HOT
            // chain be visible.
            *call_again = !IsMVCCSnapshot(snap);
            slot.base_mut().tts_tableOid = hscan.xs_rel.rd_id;
            // SAFETY: image on the page pinned by xs_cbuf; the slot takes its
            // own pin (ExecStoreBufferHeapTuple contract).
            let tuple =
                unsafe { HeapTupleData::from_raw_parts(ptr, len, self_tid, hscan.xs_rel.rd_id) };
            exectuples::exec_store_buffer_heap_tuple(slot, mcx, tuple, pin.buffer());
            Ok(true)
        }
        None => {
            // End of the HOT chain.
            *call_again = false;
            Ok(false)
        }
    }
}

pub fn heapam_fetch_row_version<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if !matches!(slot, SlotData::BufferHeap(_)) {
        wrong_slot();
    }
    let snap = match snapshot.as_deref() {
        Some(s) => s,
        None => snapshot_any_unported("row-version fetch"),
    };

    let mut res = heap_fetch(relation, snap, *tid, false)?;
    if !res.found {
        // keep_buf=false: a failed time qual holds no pin.
        debug_assert!(res.pin.is_none());
        return Ok(false);
    }

    let (ptr, len, self_tid) = {
        let t = res.tuple().expect("heap_fetch found without tuple");
        (t.header_ptr(), t.t_len, t.t_self)
    };
    let pin = res.pin.take().expect("heap_fetch found without pin");
    // SAFETY: image on the page pinned by `pin`, whose pin transfers to the
    // slot (ExecStorePinnedBufferHeapTuple contract).
    let tuple = unsafe { HeapTupleData::from_raw_parts(ptr, len, self_tid, relation.rd_id) };
    exectuples::exec_store_pinned_buffer_heap_tuple(slot, mcx, tuple, pin.into_buffer());
    slot.base_mut().tts_tableOid = relation.rd_id;
    Ok(true)
}

pub fn heapam_tuple_tid_valid(hscan: &HeapScanDescData<'_>, tid: &ItemPointerData) -> bool {
    ItemPointerIsValid(tid) && ItemPointerGetBlockNumber(tid) < hscan.rs_nblocks
}

// C binds heap_get_latest_tid(sscan, tid); the scan unwrap lives here.
pub fn heapam_tuple_get_latest_tid(
    scan: &mut HeapScanDescData<'_>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    let snap = match scan.rs_base.rs_snapshot.as_deref() {
        Some(s) => s,
        None => snapshot_any_unported("get_latest_tid"),
    };
    *tid = heap_get_latest_tid(&scan.rs_base.rs_rd, snap, *tid)?;
    Ok(())
}

pub fn heapam_tuple_satisfies_snapshot<'mcx>(
    _rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<bool> {
    let SlotData::BufferHeap(bslot) = slot else {
        wrong_slot()
    };
    let snap = match snapshot.as_deref() {
        Some(s) => s,
        // None is SnapshotAny: everything qualifies (heapgettup convention).
        None => return Ok(true),
    };
    debug_assert!(::types_core::BufferIsValid(bslot.buffer));
    let tuple = bslot
        .base
        .tuple
        .as_mut()
        .expect("satisfies_snapshot on an empty buffer slot");

    // Pin is held by the slot; take the share lock C requires for visibility.
    bufmgr_seams::lock_buffer::call(bslot.buffer, BUFFER_LOCK_SHARE)?;
    let res =
        heapam_visibility_seams::heap_tuple_satisfies_visibility::call(tuple, snap, bslot.buffer);
    bufmgr_seams::lock_buffer::call(bslot.buffer, BUFFER_LOCK_UNLOCK)?;
    res
}
