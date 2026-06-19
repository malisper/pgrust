//! `access/heap/heapam_handler.c` (the table-AM provider) + `access/table/
//! tableamapi.c::GetTableAmRoutine` — the heap access method's `TableAmRoutine`
//! vtable and the resolver that hands it to the relcache.
//!
//! This is the **core** stage: it assembles the complete `TableAmRoutine` and
//! wires the SCAN / FETCH / TOAST / set-new-filelocator / parallel-scan
//! callbacks LIVE to the (already-ported) heap scan + fetch core
//! (`backend-access-heap-heapam`, a direct dep — this crate is the owner one
//! layer above heapam, so the edge is acyclic). The four physical-modification
//! callbacks (`tuple_insert` / `tuple_delete` / `tuple_update` / `tuple_lock`)
//! and the storage-creation leg of `relation_set_new_filelocator` cross into
//! `backend-access-heap-heapam-handler-dml(-seams)` — the marshalling layer
//! that wraps `heap_insert`/`heap_update`/`heap_lock_tuple` plus
//! `ExecFetchSlotHeapTuple` and the `FIND_LAST_VERSION` follow loop, ported in
//! the heapam-handler-dml stage. Until that lands the four DML fields panic
//! loudly through the dml seams (mirror-PG-and-panic).
//!
//! `init_seams()` installs the four provider-facing table-AM dispatch seams
//! (`get_table_am_routine`, `table_relation_toast_am`,
//! `table_relation_needs_toast_table`, `table_parallelscan_reinitialize`) that
//! the relcache / catalog / nodeSeqscan consumers call — retiring their
//! CONTRACT_RECONCILE_PENDING entries.

use mcx::{Mcx, PgVec};
use std::boxed::Box;
use std::sync::Arc;

use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_scan::sdir::{ForwardScanDirection, ScanDirection};
use types_slot::{SlotData, TupleSlotKind};
use types_tableam::amopaque::{tags, AmOpaque, AmOpaqueTag, AmOpaqueType};
use types_tableam::relscan::{
    ParallelTableScanDescData, TableScanDesc, TableScanDescData,
};
use types_tableam::scankey::ScanKeyData;
use types_tableam::tableam::{
    BulkInsertStateData, IndexFetchTableData, LockTupleMode, LockWaitPolicy, Snapshot,
    TM_FailureData, TM_Result, TU_UpdateIndexes, TableAmRoutine,
};
use types_tuple::heaptuple::ItemPointerData;

pub mod analyze_scan;
pub mod build_scan;

use backend_access_heap_heapam as heapam;
use backend_access_heap_heapam_handler_dml_seams as dml_seam;
use backend_access_heap_pruneheap_seams as prune_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_executor_execTuples_seams as slot_seam;

/// `ItemPointerGetBlockNumber(tid)` (itemptr.h).
fn item_pointer_get_block_number(
    tid: &ItemPointerData,
) -> types_core::primitive::BlockNumber {
    backend_storage_page::ItemPointerGetBlockNumber(tid)
}

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;
/// `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_SHARE: i32 = 1;

// ===========================================================================
// IndexFetchHeapData — heap's IndexFetchTableData.am_private payload
// ===========================================================================

/// `IndexFetchHeapData` (`access/heapam.h`) minus its embedded `xs_base`
/// `IndexFetchTableData` (which lives in the generic descriptor). The C struct
/// is `{ IndexFetchTableData xs_base; Buffer xs_cbuf; }`; `xs_base` is the
/// generic [`IndexFetchTableData`], so the only heap-private tail is the
/// current-block buffer pin. Rides opaquely in `IndexFetchTableData.am_private`
/// (the `void *` made `'mcx`-safe via [`AmOpaque`]).
pub struct IndexFetchHeapData {
    /// `xs_cbuf` — current heap buffer in scan, `InvalidBuffer` if none.
    pub xs_cbuf: types_storage::buf::Buffer,
}

impl<'mcx> AmOpaqueType<'mcx> for IndexFetchHeapData {
    const TAG: AmOpaqueTag = tags::HEAP_INDEX_FETCH;
}

/// Box an `IndexFetchHeapData` as the erased `dyn AmOpaque` payload in the
/// scan's `mcx` arena (the same unsize-through-raw-pointer pattern the heap-scan
/// owner uses for `HeapScanDescData`; no `CoerceUnsized` on stable).
fn erase_index_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    hscan: IndexFetchHeapData,
) -> PgResult<mcx::PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: mcx::PgBox<'mcx, IndexFetchHeapData> = mcx::alloc_in(mcx, hscan)?;
    let (ptr, alloc) = mcx::PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable.
    Ok(unsafe { mcx::PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

/// Downcast `IndexFetchTableData.am_private` to `&mut IndexFetchHeapData`
/// (C's `(IndexFetchHeapData *) scan`). A missing/mistyped payload is a wiring
/// error (the heap AM always installs an `IndexFetchHeapData`).
fn heap_index_fetch<'a, 'mcx>(
    scan: &'a mut IndexFetchTableData<'mcx>,
) -> &'a mut IndexFetchHeapData {
    let am = scan
        .am_private
        .as_deref_mut()
        .expect("heap index fetch: IndexFetchTableData.am_private is empty");
    am.downcast_mut::<IndexFetchHeapData>()
        .expect("heap index fetch: am_private is not an IndexFetchHeapData")
}

// ===========================================================================
// Slot callbacks
// ===========================================================================

/// `heapam_slot_callbacks(relation)` — `&TTSOpsBufferHeapTuple`. The owned slot
/// model identifies the ops vtable by [`TupleSlotKind`] (the C code only ever
/// compares the returned pointer for identity).
fn heapam_slot_callbacks(_relation: &Relation<'_>) -> TupleSlotKind {
    TupleSlotKind::BufferHeapTuple
}

// ===========================================================================
// Scan callbacks (LIVE to heapam scan core)
// ===========================================================================

/// `.scan_begin = heap_beginscan`. The vtable borrows `rel`; `heap_beginscan`
/// takes the open relation by value (it bumps the refcount and stores the
/// handle in the descriptor), so we hand it a clone of the handle.
fn heapam_scan_begin<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData<'mcx>>,
    pscan: Option<Arc<ParallelTableScanDescData>>,
    flags: u32,
) -> PgResult<TableScanDesc<'mcx>> {
    heapam::scan::heap_beginscan(mcx, rel.alias(), snapshot, nkeys, key, pscan, flags)
}

/// `.scan_getnextslot = heap_getnextslot`.
fn heapam_scan_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    heapam::scan::heap_getnextslot(mcx, scan, direction, slot)
}

/// `.scan_bitmap_next_tuple = heapam_scan_bitmap_next_tuple`.
fn heapam_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Option<(bool, u64, u64)>> {
    heapam::scan::heapam_scan_bitmap_next_tuple(mcx, scan, slot)
}

/// `.scan_end = heap_endscan`.
fn heapam_scan_end(scan: TableScanDesc<'_>) -> PgResult<()> {
    heapam::scan::heap_endscan(scan)
}

/// `.scan_rescan = heap_rescan`. The vtable carries the (unused-by-heap) `key`
/// argument that the generic `scan_rescan` dispatch passes; `heap_rescan`
/// re-runs `initscan` (which re-copies the descriptor's existing keys), so the
/// supplied `key` is ignored exactly as in C (`heap_rescan` takes no `key`).
fn heapam_scan_rescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    _key: Option<&[ScanKeyData]>,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    heapam::scan::heap_rescan(
        mcx,
        scan,
        set_params,
        allow_strat,
        allow_sync,
        allow_pagemode,
    )
}

// ===========================================================================
// Parallel-scan callbacks (table_block_parallelscan_* in tableam.c)
// ===========================================================================

/// `.parallelscan_estimate = table_block_parallelscan_estimate`.
fn heapam_parallelscan_estimate(rel: &Relation<'_>) -> usize {
    backend_access_table_tableam::table_block_parallelscan_estimate(rel)
}

/// `.parallelscan_initialize = table_block_parallelscan_initialize`.
fn heapam_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelTableScanDescData,
) -> PgResult<usize> {
    backend_access_table_tableam::table_block_parallelscan_initialize(rel, pscan)
}

/// `.parallelscan_reinitialize = table_block_parallelscan_reinitialize`.
fn heapam_parallelscan_reinitialize(
    rel: &Relation<'_>,
    pscan: &ParallelTableScanDescData,
) -> PgResult<()> {
    backend_access_table_tableam::table_block_parallelscan_reinitialize(rel, pscan);
    Ok(())
}

// ===========================================================================
// Index-fetch callbacks
// ===========================================================================

/// `heapam_index_fetch_begin(rel)` — `palloc0(IndexFetchHeapData)`,
/// `xs_base.rel = rel`, `xs_cbuf = InvalidBuffer`.
fn heapam_index_fetch_begin<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<Box<IndexFetchTableData<'mcx>>> {
    let hscan = IndexFetchHeapData {
        xs_cbuf: types_storage::buf::InvalidBuffer,
    };
    Ok(Box::new(IndexFetchTableData {
        rel: rel.alias(),
        am_private: Some(erase_index_fetch(mcx, hscan)?),
    }))
}

/// `heapam_index_fetch_reset(scan)` — release the held buffer pin, if any.
fn heapam_index_fetch_reset(scan: &mut IndexFetchTableData<'_>) -> PgResult<()> {
    let hscan = heap_index_fetch(scan);
    if types_storage::buf::BufferIsValid(hscan.xs_cbuf) {
        bufmgr_seam::release_buffer::call(hscan.xs_cbuf);
        hscan.xs_cbuf = types_storage::buf::InvalidBuffer;
    }
    Ok(())
}

/// `heapam_index_fetch_end(scan)` — `heapam_index_fetch_reset` then `pfree`.
fn heapam_index_fetch_end(mut scan: Box<IndexFetchTableData<'_>>) -> PgResult<()> {
    heapam_index_fetch_reset(&mut scan)?;
    drop(scan); // pfree(hscan)
    Ok(())
}

/// `heapam_index_fetch_tuple(scan, tid, snapshot, slot, &call_again,
/// &all_dead)` — HOT-chain fetch via `heap_hot_search_buffer`, switching to the
/// right page (and pruning it) when not mid-chain.
fn heapam_index_fetch_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    tid: &ItemPointerData,
    snapshot: &mut Snapshot,
    slot: &mut SlotData<'mcx>,
    call_again: &mut bool,
    mut all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    // Assert(TTS_IS_BUFFERTUPLE(slot)).
    debug_assert!(matches!(slot, SlotData::BufferHeap(_)));

    let rel = scan.rel.alias();
    // C passes `snapshot` (a pointer) straight through; the dirty-snapshot
    // visibility check writes xmin/xmax/speculativeToken back into it, and the
    // index scan's owner reads those out of `scan->xs_snapshot` to decide
    // whether to wait on a concurrent inserter. Thread it by `&mut`.
    let snap = snapshot
        .as_mut()
        .expect("heapam_index_fetch_tuple: index scans require a real snapshot");

    // We can skip the buffer-switching logic if we're in mid-HOT chain.
    if !*call_again {
        // Switch to correct buffer if we don't have it already.
        let prev_buf = heap_index_fetch(scan).xs_cbuf;
        let new_buf =
            release_and_read_buffer(prev_buf, &rel, item_pointer_get_block_number(tid))?;
        heap_index_fetch(scan).xs_cbuf = new_buf;

        // Prune page, but only if we weren't already on this page.
        if prev_buf != new_buf {
            prune_seam::heap_page_prune_opt::call(mcx, &rel, new_buf)?;
        }
    }

    let cbuf = heap_index_fetch(scan).xs_cbuf;

    // Obtain share-lock on the buffer so we can examine visibility.
    bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
    let want_all_dead = all_dead.is_some();
    let res = heapam::fetch::heap_hot_search_buffer(
        mcx,
        *tid,
        &rel,
        cbuf,
        snap,
        want_all_dead,
        !*call_again,
    )?;
    bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;

    if let Some(slot_dead) = all_dead.as_deref_mut() {
        if let Some(d) = res.all_dead {
            *slot_dead = d;
        }
    }

    if res.found {
        // Only in a non-MVCC snapshot can more than one member of the HOT
        // chain be visible.
        *call_again = !types_snapshot::snapshot::IsMVCCSnapshot(&*snap);

        let mut tuple = res
            .heap_tuple
            .expect("heap_hot_search_buffer: found==true with no tuple");
        // C does `bslot->base.tupdata.t_self = *tid`, resetting t_self to the
        // index TID (the HOT-chain root). C can do that because catalog catcache
        // entries are normally warmed with the resolved (live) TID by an earlier
        // seqscan-based lookup, so a later CatalogTupleUpdate through the syscache
        // still targets the live tuple rather than a persisting LP_REDIRECT root.
        // We lack that warming, so keep the resolved TID found by
        // heap_hot_search_buffer: t_self must name where the live tuple is, or an
        // UPDATE through the syscache would hit an LP_REDIRECT root and fail with
        // "tuple concurrently deleted". For a non-HOT row res.tid == *tid, so this
        // only differs when the index entry points at a HOT-chain root.
        tuple.tuple.t_self = res.tid;
        tuple.tuple.t_tableOid = rel.rd_id;
        slot.base_mut().tts_tableOid = rel.rd_id;
        slot_seam::exec_store_buffer_heap_tuple::call(tuple, slot, cbuf)?;
    } else {
        // We've reached the end of the HOT chain.
        *call_again = false;
    }

    Ok(res.found)
}

// ===========================================================================
// Non-modifying single-tuple callbacks
// ===========================================================================

/// `heapam_fetch_row_version(relation, tid, snapshot, slot)` — `heap_fetch`
/// into the slot's tupdata; on a visible hit, store the pinned buffer into the
/// slot (transferring the pin) and set `tts_tableOid`.
fn heapam_fetch_row_version<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Assert(TTS_IS_BUFFERTUPLE(slot)).
    debug_assert!(matches!(slot, SlotData::BufferHeap(_)));

    let snap = snapshot
        .as_ref()
        .expect("heapam_fetch_row_version: requires a real snapshot");

    let res = heapam::fetch::heap_fetch(mcx, relation, snap, *tid, false)?;
    if res.found {
        // store in slot, transferring existing pin.
        let mut tuple = res
            .tuple
            .expect("heap_fetch: found==true with no tuple");
        tuple.tuple.t_self = *tid;
        tuple.tuple.t_tableOid = relation.rd_id;
        slot_seam::exec_store_pinned_buffer_heap_tuple::call(tuple, slot, res.userbuf)?;
        slot.base_mut().tts_tableOid = relation.rd_id;
        return Ok(true);
    }
    Ok(false)
}

/// `heapam_tuple_tid_valid(scan, tid)` — within the relation's current size?
fn heapam_tuple_tid_valid(
    scan: &mut TableScanDescData<'_>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    Ok(heapam::scan::heapam_tuple_tid_valid(scan, tid))
}

/// `.tuple_get_latest_tid = heap_get_latest_tid`. The C signature reads the
/// scan's snapshot (`scan->rs_snapshot`) and updates `*tid` in place.
fn heapam_tuple_get_latest_tid<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    let rel = scan.rs_rd.alias();
    let snapshot = scan
        .rs_snapshot
        .clone()
        .expect("heap_get_latest_tid: scan has no snapshot");
    *tid = heapam::fetch::heap_get_latest_tid(mcx, &rel, &snapshot, *tid)?;
    Ok(())
}

// ===========================================================================
// Physical-tuple-modification callbacks (delegate to the heapam-handler-dml
// marshalling layer — mirror-PG-and-panic until that stage lands)
// ===========================================================================

fn heapam_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    dml_seam::heapam_tuple_insert::call(mcx, rel, slot, cid, options, bistate)
}

fn heapam_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    dml_seam::heapam_multi_insert::call(mcx, rel, slots, cid, options, bistate)
}

#[allow(clippy::too_many_arguments)]
fn heapam_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    dml_seam::heapam_tuple_delete::call(
        mcx,
        rel,
        tid,
        cid,
        snapshot,
        crosscheck,
        wait,
        tmfd,
        changing_part,
    )
}

#[allow(clippy::too_many_arguments)]
fn heapam_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    dml_seam::heapam_tuple_update::call(
        mcx,
        rel,
        otid,
        slot,
        cid,
        snapshot,
        crosscheck,
        wait,
        tmfd,
        lockmode,
        update_indexes,
    )
}

#[allow(clippy::too_many_arguments)]
fn heapam_tuple_lock<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut SlotData<'mcx>,
    cid: types_core::xact::CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    flags: u8,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    dml_seam::heapam_tuple_lock::call(
        mcx,
        rel,
        tid,
        snapshot,
        slot,
        cid,
        mode,
        wait_policy,
        flags,
        tmfd,
    )
}

/// `heapam_relation_set_new_filelocator` — the storage-creation leg lives in
/// the heapam-handler-dml stage (RelationCreateStorage + init-fork smgrcreate +
/// log_smgrcreate).
fn heapam_relation_set_new_filelocator(
    rel: &Relation<'_>,
    newrlocator: &types_storage::RelFileLocator,
    persistence: i8,
) -> PgResult<(u32, u32)> {
    dml_seam::heapam_relation_set_new_filelocator::call(rel, newrlocator, persistence)
}

/// `heapam_relation_nontransactional_truncate(rel)` (heapam_handler.c:626) —
/// `RelationTruncate(rel, 0)`. The buffer/smgr/WAL truncation engine lives in
/// storage.c (crossed via the storage seam).
fn heapam_relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
    backend_catalog_storage_seams::relation_truncate::call(rel, 0)
}

// ===========================================================================
// TOAST callbacks
// ===========================================================================

/// `heapam_relation_toast_am(rel)` — `rel->rd_rel->relam` (TOAST tables for
/// heap relations are just heap relations).
fn heapam_relation_toast_am(rel: &Relation<'_>) -> Oid {
    rel.rd_rel.relam
}

// `MAXALIGN` / `BITMAPLEN` (c.h / htup_details.h) — inline in C's
// `heapam_relation_needs_toast_table`.
const fn maxalign(len: i32) -> i32 {
    const ALIGNOF_LONG: i32 = 8;
    (len + (ALIGNOF_LONG - 1)) & !(ALIGNOF_LONG - 1)
}

const fn bitmaplen(natts: i32) -> i32 {
    (natts + 7) / 8
}

/// `att_align_nominal(cur_offset, attalign)` (tupmacs.h) over an `i32` offset.
fn att_align_nominal(cur_offset: i32, attalign: i8) -> i32 {
    use types_tuple::heaptuple::{TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT};
    let a = |to: i32| ((cur_offset + (to - 1)) / to) * to;
    if attalign == TYPALIGN_INT {
        a(4)
    } else if attalign == TYPALIGN_CHAR {
        cur_offset
    } else if attalign == TYPALIGN_DOUBLE {
        a(8)
    } else {
        debug_assert_eq!(attalign, TYPALIGN_SHORT);
        a(2)
    }
}

/// `heapam_relation_needs_toast_table(rel)` (heapam_handler.c): does the
/// relation need a separate TOAST table? Sums the nominal fixed widths +
/// type-maximum-sizes of the variable-length attributes and compares the
/// resulting worst-case tuple length against `TOAST_TUPLE_THRESHOLD`.
fn heapam_relation_needs_toast_table(rel: &Relation<'_>) -> PgResult<bool> {
    let mut data_length: i32 = 0;
    let mut maxlength_unknown = false;
    let mut has_toastable_attrs = false;
    let tupdesc = &rel.rd_att;

    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        if att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL {
            continue;
        }
        data_length = att_align_nominal(data_length, att.attalign);
        if att.attlen > 0 {
            // Fixed-length types are never toastable.
            data_length += att.attlen as i32;
        } else {
            let maxlen = backend_utils_adt_format_type_seams::type_maximum_size::call(
                att.atttypid,
                att.atttypmod,
            )?;
            if maxlen < 0 {
                maxlength_unknown = true;
            } else {
                data_length += maxlen;
            }
            if att.attstorage != types_typcache::TYPSTORAGE_PLAIN {
                has_toastable_attrs = true;
            }
        }
    }
    if !has_toastable_attrs {
        return Ok(false); // nothing to toast?
    }
    if maxlength_unknown {
        return Ok(true); // any unlimited-length attrs?
    }
    let tuple_length = maxalign(
        types_tuple::heap::SizeofHeapTupleHeader as i32 + bitmaplen(tupdesc.natts),
    ) + maxalign(data_length);
    Ok(tuple_length > backend_access_heap_heaptoast::TOAST_TUPLE_THRESHOLD as i32)
}

// ===========================================================================
// The heap AM's TableAmRoutine vtable + GetTableAmRoutine
// ===========================================================================

/// `GetHeapamTableAmRoutine()` (heapam_handler.c) — the const `heapam_methods`
/// vtable. Assembled as an owned value the relcache caches in `rd_tableam`.
pub fn get_heapam_table_am_routine() -> TableAmRoutine {
    TableAmRoutine {
        slot_callbacks: heapam_slot_callbacks,

        scan_begin: heapam_scan_begin,
        scan_getnextslot: heapam_scan_getnextslot,
        scan_end: heapam_scan_end,
        scan_rescan: heapam_scan_rescan,

        parallelscan_estimate: heapam_parallelscan_estimate,
        parallelscan_initialize: heapam_parallelscan_initialize,
        parallelscan_reinitialize: heapam_parallelscan_reinitialize,

        index_fetch_begin: heapam_index_fetch_begin,
        index_fetch_reset: heapam_index_fetch_reset,
        index_fetch_end: heapam_index_fetch_end,
        index_fetch_tuple: heapam_index_fetch_tuple,

        tuple_fetch_row_version: heapam_fetch_row_version,
        tuple_tid_valid: heapam_tuple_tid_valid,
        tuple_get_latest_tid: heapam_tuple_get_latest_tid,

        tuple_insert: heapam_tuple_insert,
        multi_insert: heapam_multi_insert,
        tuple_delete: heapam_tuple_delete,
        tuple_update: heapam_tuple_update,
        tuple_lock: heapam_tuple_lock,

        relation_set_new_filelocator: heapam_relation_set_new_filelocator,
        relation_nontransactional_truncate: heapam_relation_nontransactional_truncate,

        scan_analyze_next_block: analyze_scan::heapam_scan_analyze_next_block,
        scan_analyze_next_tuple: analyze_scan::heapam_scan_analyze_next_tuple,

        scan_bitmap_next_tuple: heapam_scan_bitmap_next_tuple,
    }
}

/// `HEAP_TABLE_AM_OID` / `F_HEAP_TABLEAM_HANDLER` — the heap AM's handler OID
/// (the `pg_proc` entry the relcache stamps into `rd_amhandler`). Matches
/// `backend-utils-cache-relcache::index::F_HEAP_TABLEAM_HANDLER`.
const F_HEAP_TABLEAM_HANDLER: Oid = 3;

/// `GetTableAmRoutine(amhandler)` (tableamapi.c): call the table AM's handler to
/// fetch its `TableAmRoutine`. In C this is `OidFunctionCall0(amhandler)`; the
/// only table AM is heap, so we map its handler OID directly to the heap
/// vtable. An unknown handler is the C "did not return a TableAmRoutine struct"
/// `elog(ERROR)`.
fn get_table_am_routine(amhandler: Oid) -> PgResult<TableAmRoutine> {
    if amhandler == F_HEAP_TABLEAM_HANDLER {
        Ok(get_heapam_table_am_routine())
    } else {
        Err(PgError::error(&format!(
            "table access method handler {amhandler} did not return a TableAmRoutine struct"
        )))
    }
}

/// Wrapper seam adapters for the table-AM TOAST / parallel-scan dispatch
/// helpers. These dispatch through `rel->rd_tableam` in C; the relcache caches
/// the heap vtable in `rd_tableam`, so the provider crate resolves it the same
/// way the dispatcher would (the vtable is identical for every heap relation).
fn table_relation_toast_am(rel: &Relation<'_>) -> Oid {
    heapam_relation_toast_am(rel)
}

fn table_relation_needs_toast_table(rel: &Relation<'_>) -> bool {
    heapam_relation_needs_toast_table(rel)
        .expect("table_relation_needs_toast_table: type_maximum_size errored")
}

fn table_parallelscan_reinitialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelTableScanDescData,
) -> PgResult<()> {
    heapam_parallelscan_reinitialize(rel, pscan)
}

/// `ReleaseAndReadBuffer(buffer, relation, blockNum)` — release the prior pin
/// (unless it already holds `blockNum`) and read+pin the requested block. Mapped
/// onto the bufmgr seam: the same-block fast path is preserved by comparing the
/// pinned buffer's block number.
fn release_and_read_buffer<'mcx>(
    buffer: types_storage::buf::Buffer,
    relation: &Relation<'mcx>,
    block_num: types_core::primitive::BlockNumber,
) -> PgResult<types_storage::buf::Buffer> {
    bufmgr_seam::release_and_read_buffer::call(buffer, relation, block_num)
}

/* ----------------------------------------------------------------------------
 * heapam_estimate_rel_size / table_block_relation_estimate_size
 *  (heapam_handler.c + tableam.c) — the planner-facing table-AM size estimator.
 * ------------------------------------------------------------------------- */

/// `HEAP_OVERHEAD_BYTES_PER_TUPLE` (heapam_handler.c):
/// `MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData)`.
/// `SizeofHeapTupleHeader == offsetof(HeapTupleHeaderData, t_bits) == 23`,
/// `MAXALIGN(23) == 24`, `sizeof(ItemIdData) == 4` ⇒ 28.
const HEAP_OVERHEAD_BYTES_PER_TUPLE: usize = 24 + 4;
/// `HEAP_USABLE_BYTES_PER_PAGE` (heapam_handler.c):
/// `BLCKSZ - SizeOfPageHeaderData` == `8192 - 24` == 8168.
const HEAP_USABLE_BYTES_PER_PAGE: usize = 8192 - 24;
/// `HEAP_DEFAULT_FILLFACTOR` (utils/rel.h).
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

/// `rint(x)` — round half to even (C `rint` under the default rounding mode).
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

/// `table_relation_estimate_size(rel, attr_widths, &pages, &tuples,
/// &allvisfrac)` (tableam.h) for the heap AM = `heapam_estimate_rel_size`
/// (heapam_handler.c), which delegates to `table_block_relation_estimate_size`
/// (tableam.c) with the heap per-tuple overhead / per-page usable constants.
///
/// `relid` is opened via the relcache (the seam carries the OID, mirroring the
/// C callback that receives the open `Relation`). Returns `(pages, tuples,
/// allvisfrac)`.
fn table_relation_estimate_size(
    relid: Oid,
    attr_widths: Option<&mut [i32]>,
) -> PgResult<(types_core::primitive::BlockNumber, f64, f64)> {
    // The estimator reads pg_class stats + RelationGetNumberOfBlocks; the
    // relation handle is opened from a scratch context (the callback's borrowed
    // `Relation` lifetime).
    let scratch = mcx::MemoryContext::new("table_relation_estimate_size");
    let data = backend_utils_cache_relcache_seams::relation_id_get_relation::call(
        scratch.mcx(),
        relid,
    )?
    .expect("table_relation_estimate_size: relation must exist in relcache");
    // Wrap the projected RelationData in a cache-less handle (no close authority
    // — the caller in plancat owns the lock/open lifecycle; this is a read-only
    // stats/size projection mirroring the borrowed `Relation` the C callback gets).
    let rel = Relation::open(data, None);

    table_block_relation_estimate_size(
        &rel,
        relid,
        attr_widths,
        HEAP_OVERHEAD_BYTES_PER_TUPLE,
        HEAP_USABLE_BYTES_PER_PAGE,
    )
}

/// `table_block_relation_estimate_size(rel, attr_widths, &pages, &tuples,
/// &allvisfrac, overhead_bytes_per_tuple, usable_bytes_per_page)` (tableam.c).
fn table_block_relation_estimate_size(
    rel: &Relation<'_>,
    relid: Oid,
    attr_widths: Option<&mut [i32]>,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
) -> PgResult<(types_core::primitive::BlockNumber, f64, f64)> {
    // it should have storage, so we can call the smgr
    // curpages = RelationGetNumberOfBlocks(rel);
    let mut curpages =
        backend_utils_cache_relcache_seams::relation_get_number_of_blocks::call(rel)?;

    // coerce values in pg_class to more desirable types
    let relpages = rel.rd_rel.relpages as u32;
    let reltuples = rel.rd_rel.reltuples as f64;
    let relallvisible = rel.rd_rel.relallvisible as u32;

    // HACK: if the relation has never yet been vacuumed (reltuples < 0), use a
    // minimum size estimate of 10 pages — unless it has inheritance children.
    if curpages < 10 && reltuples < 0.0 && !rel.rd_rel.relhassubclass {
        curpages = 10;
    }

    // report estimated # pages
    let pages = curpages;

    // quick exit if rel is clearly empty
    if curpages == 0 {
        return Ok((pages, 0.0, 0.0));
    }

    // estimate number of tuples from previous tuple density
    let density: f64 = if reltuples >= 0.0 && relpages > 0 {
        reltuples / relpages as f64
    } else {
        // No data (never vacuumed): estimate tuple width from attribute
        // datatypes, assuming pages are completely full, accounting for
        // fillfactor. Integer division is intentional, matching C.
        let fillfactor = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);

        let mut tuple_width =
            backend_optimizer_util_plancat_seams::get_rel_data_width::call(relid, attr_widths)?
                as usize;
        tuple_width += overhead_bytes_per_tuple;
        // note: integer division is intentional here
        let raw = (usable_bytes_per_page * fillfactor as usize / 100) / tuple_width;
        // There's at least one row on the page, even with low fillfactor.
        backend_optimizer_path_costsize_seams::clamp_row_est::call(raw as f64)
    };

    let tuples = rint(density * curpages as f64);

    // relallvisible is used as-is (converted to a fraction by costsize.c).
    let allvisfrac = if relallvisible == 0 || curpages == 0 {
        0.0
    } else if relallvisible as f64 >= curpages as f64 {
        1.0
    } else {
        relallvisible as f64 / curpages as f64
    };

    Ok((pages, tuples, allvisfrac))
}

/// Install the provider-facing table-AM dispatch seams.
pub fn init_seams() {
    use backend_access_table_tableam_seams as sx;
    sx::get_table_am_routine::set(get_table_am_routine);
    sx::table_relation_toast_am::set(table_relation_toast_am);
    sx::table_relation_needs_toast_table::set(table_relation_needs_toast_table);
    sx::table_parallelscan_reinitialize::set(table_parallelscan_reinitialize);
    // The canonical, fully-typed serial index-build heap scan
    // (heapam_index_build_range_scan). brinsummarize reaches it with the real
    // execnodes::IndexInfo + an explicit arena; the whole-relation
    // `table_index_build_scan` forwards to the same provider over the entire
    // relation (anyvisible = false, start_blockno = 0, numblocks =
    // InvalidBlockNumber), and the AM build drivers now carry the real
    // execnodes::IndexInfo + mcx through their ambuild signatures.
    sx::table_index_build_range_scan::set(build_scan::provider_index_build_range_scan);
    sx::table_index_build_scan::set(build_scan::provider_index_build_scan);

    // heapam_estimate_rel_size — the planner-facing table-AM size estimator
    // (plancat's estimate_rel_size dispatches RELKIND_HAS_TABLE_AM here).
    backend_optimizer_util_plancat_ext_seams::table_relation_estimate_size::set(
        table_relation_estimate_size,
    );

    // The table-AM capability probes get_relation_info reads off the heap AM's
    // vtable. The heap table AM (heapam_handler.c) DEFINES
    // `scan_bitmap_next_tuple` (`heapam_scan_bitmap_next_tuple`) and both
    // TID-range callbacks (`heapam_scan_set_tidrange` /
    // `heapam_scan_getnextslot_tidrange`), so for a heap relation both probes
    // are true. (Heap is the only table AM ported; the trimmed `TableAmRoutine`
    // vtable does not carry these callbacks, so the presence is the heap AM's
    // own constant fact rather than a vtable NULL test.)
    backend_optimizer_util_plancat_ext_seams::table_has_scan_bitmap::set(table_has_scan_bitmap);
    backend_optimizer_util_plancat_ext_seams::table_has_tid_range::set(table_has_tid_range);
}

/// `relation->rd_tableam->scan_bitmap_next_tuple != NULL` — the heap table AM
/// supplies `heapam_scan_bitmap_next_tuple`, so this is true for a heap relation.
fn table_has_scan_bitmap(relid: Oid) -> PgResult<bool> {
    let _ = relid;
    Ok(true)
}

/// `relation->rd_tableam->scan_set_tidrange != NULL &&
/// scan_getnextslot_tidrange != NULL` — the heap table AM supplies both, so this
/// is true for a heap relation.
fn table_has_tid_range(relid: Oid) -> PgResult<bool> {
    let _ = relid;
    Ok(true)
}
