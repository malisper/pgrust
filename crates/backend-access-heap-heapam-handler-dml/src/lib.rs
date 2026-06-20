//! `access/heap/heapam_handler.c` — the physical-tuple-modification half of the
//! heap AM's `TableAmRoutine` vtable: `heapam_tuple_insert` /
//! `heapam_tuple_delete` / `heapam_tuple_update` / `heapam_tuple_lock` and the
//! storage-creation callback `heapam_relation_set_new_filelocator`.
//!
//! These are the DML marshalling wrappers around the already-ported heap modify
//! core (`heap_insert` / `heap_delete` / `heap_update` / `heap_lock_tuple`,
//! `backend-access-heap-heapam`, a direct dep — this crate sits one layer above
//! heapam, so the edge is acyclic) plus the slot→tuple bridge
//! (`ExecFetchSlotHeapTuple`). The heapam-handler **core** crate populates the
//! DML vtable fields by `::call`ing through
//! `backend-access-heap-heapam-handler-dml-seams`; this crate installs those
//! seams from `init_seams()`, retiring their latent panics.
//!
//! `heapam_tuple_lock` runs `heap_lock_tuple` plus the
//! `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` update-chain follow loop (`heap_fetch`
//! under a fresh DIRTY snapshot, `XactLockTableWait` /
//! `ConditionalXactLockTableWait` on a conflicting locker), then stores the
//! locked tuple into the `BufferHeapTupleTableSlot` via
//! `ExecStorePinnedBufferHeapTuple`. Rust's `heap_lock_tuple` returns the locked
//! tuple's header + the pinned buffer (`HeapLockResult`), so the success path
//! re-materializes the full on-page `FormedTuple` (header + user-data) from the
//! still-pinned buffer at `tuple.t_self` before the store — behaviorally the
//! same as C, which stores the page-aliasing `t_data` under the transferred pin.
//!
//! The storage-creation leg of `heapam_relation_set_new_filelocator`
//! (`RelationCreateStorage` returning a transient `SMgrRelation`, the unlogged
//! INIT-fork `smgrcreate` / `log_smgrcreate`, and `smgrclose`) lives entirely in
//! the unported `storage.c` owner; this crate calls it through one storage seam
//! (`relation_set_new_filelocator_storage`) that panics until that owner lands.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::TransactionId;
use types_core::xact::{CommandId, InvalidTransactionId};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_T_R_SERIALIZATION_FAILURE};
use types_rel::Relation;
use types_slot::SlotData;
use types_storage::lock::XLTW_Oper;
use types_storage::{Buffer, RelFileLocator};
use types_tableam::tableam::{
    BulkInsertStateData, LockTupleMode, LockWaitPolicy, Snapshot, TM_FailureData, TM_Result,
    TU_UpdateIndexes, TUPLE_LOCK_FLAG_FIND_LAST_VERSION, TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::{HeapTupleHeaderData, ItemPointerData};

use backend_access_heap_heapam as heapam;
use backend_access_heap_heapam_seams::HeapUpdateResult;
use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetXmin, HeapTupleHeaderIsSpeculative, ItemPointerEquals, SpecTokenOffsetNumber,
};
use backend_access_heap_heapam_visibility::HeapTupleHeaderGetUpdateXid;
use backend_executor_execTuples_seams as slot_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_page::{
    ItemIdGetLength, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerIndicatesMovedPartitions, PageGetItem, PageGetItemId, PageRef,
};
use backend_utils_time_combocid_seams as combocid_seam;

/// `td.t_data` as `&HeapTupleHeaderData` (the header is always present on a
/// formed heap tuple).
fn header<'a, 'mcx>(td: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    td.tuple.t_data.as_ref().expect("heap tuple has no header")
}

/// `HEAP_ONLY_TUPLE` (htup_details.h) — `t_infomask2` bit marking a heap-only
/// tuple.
const HEAP_ONLY_TUPLE: u16 = types_tuple::heaptuple::HEAP_ONLY_TUPLE;

// ===========================================================================
// tuple_insert
// ===========================================================================

/// `heapam_tuple_insert(relation, slot, cid, options, bistate)`
/// (heapam_handler.c): `ExecFetchSlotHeapTuple(slot, true, &shouldFree)`, stamp
/// the table OID, `heap_insert`, copy the resulting `t_self` into `tts_tid`.
fn heapam_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    let (mut tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    slot.base_mut().tts_tableOid = relation.rd_id;
    tuple.tuple.t_tableOid = relation.rd_id;

    heapam::insert::heap_insert(mcx, relation, &mut tuple, cid, options, bistate)?;

    slot.base_mut().tts_tid = tuple.tuple.t_self;
    Ok(())
}

/// `heapam_tuple_insert_speculative(relation, slot, cid, options, bistate,
/// specToken)` (heapam_handler.c): like `heapam_tuple_insert`, but sets
/// `options |= HEAP_INSERT_SPECULATIVE` and stamps the tuple header's
/// speculative token (`HeapTupleHeaderSetSpeculativeToken`) before
/// `heap_insert`, so the inserted tuple is marked speculative for ON CONFLICT
/// arbiter-index resolution.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_insert_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
    spec_token: u32,
) -> PgResult<()> {
    let (mut tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    // options |= HEAP_INSERT_SPECULATIVE;
    let options = options | heapam::insert::HEAP_INSERT_SPECULATIVE;

    slot.base_mut().tts_tableOid = relation.rd_id;
    tuple.tuple.t_tableOid = relation.rd_id;

    // HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken):
    //   ItemPointerSet(&(tup)->t_ctid, token, SpecTokenOffsetNumber)
    let hdr = tuple
        .tuple
        .t_data
        .as_mut()
        .expect("heapam_tuple_insert_speculative: tuple has no header");
    hdr.t_ctid = ItemPointerData::new(spec_token, SpecTokenOffsetNumber);

    heapam::insert::heap_insert(mcx, relation, &mut tuple, cid, options, bistate)?;

    slot.base_mut().tts_tid = tuple.tuple.t_self;
    Ok(())
}

/// `heapam_tuple_complete_speculative(relation, slot, specToken, succeeded)`
/// (heapam_handler.c): `ExecFetchSlotHeapTuple`, then
/// `heap_finish_speculative(relation, &slot->tts_tid)` when `succeeded` else
/// `heap_abort_speculative(relation, &slot->tts_tid)`. The `specToken` argument
/// is unused by the heap implementation (it's carried on the tuple header).
fn heapam_tuple_complete_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    _spec_token: u32,
    succeeded: bool,
) -> PgResult<()> {
    // (void) ExecFetchSlotHeapTuple(slot, true, &shouldFree);
    let (_tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    let tid = slot.base().tts_tid;
    if succeeded {
        // heap_finish_speculative(relation, &slot->tts_tid);
        heapam::inplace::heap_finish_speculative(mcx, relation, tid)?;
    } else {
        // heap_abort_speculative(relation, &slot->tts_tid);
        heapam::inplace::heap_abort_speculative(mcx, relation, tid)?;
    }
    Ok(())
}

/// `heapam_multi_insert(relation, slots, ntuples, cid, options, bistate)`
/// (heapam_handler.c): `ExecFetchSlotHeapTuple` each slot, stamp the table OID,
/// `heap_multi_insert`, then copy each stored `t_self` back into the matching
/// slot's `tts_tid`.
fn heapam_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    // heaptuples = palloc(ntuples * sizeof(HeapTuple));
    // for (i = 0; i < ntuples; i++) {
    //     tuple = ExecFetchSlotHeapTuple(slots[i], true, NULL);
    //     slots[i]->tts_tableOid = RelationGetRelid(relation);
    //     tuple->t_tableOid = slots[i]->tts_tableOid;
    // }
    let mut tuples: mcx::PgVec<'mcx, FormedTuple<'mcx>> =
        mcx::vec_with_capacity_in(mcx, slots.len())?;
    for slot in slots.iter_mut() {
        let (mut tuple, _should_free) =
            slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;
        slot.base_mut().tts_tableOid = relation.rd_id;
        tuple.tuple.t_tableOid = relation.rd_id;
        tuples.push(tuple);
    }

    let stored =
        heapam::insert::heap_multi_insert(mcx, relation, tuples, cid, options, bistate)?;

    // for (i = 0; i < ntuples; i++) slots[i]->tts_tid = heaptuples[i]->t_self;
    for (slot, tuple) in slots.iter_mut().zip(stored.iter()) {
        slot.base_mut().tts_tid = tuple.tuple.t_self;
    }
    Ok(())
}

// ===========================================================================
// tuple_delete
// ===========================================================================

/// `heapam_tuple_delete(relation, tid, cid, snapshot, crosscheck, wait, tmfd,
/// changingPart)` (heapam_handler.c): forwards to `heap_delete`. `snapshot` is
/// unused by the heap implementation.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: CommandId,
    _snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    heapam::delete::heap_delete(
        mcx,
        relation,
        *tid,
        cid,
        crosscheck.as_ref(),
        wait,
        tmfd,
        changing_part,
    )
}

// ===========================================================================
// tuple_update
// ===========================================================================

/// `heapam_tuple_update(relation, otid, slot, cid, snapshot, crosscheck, wait,
/// tmfd, lockmode, update_indexes)` (heapam_handler.c): `ExecFetchSlotHeapTuple`,
/// stamp the table OID, `heap_update`, copy `t_self` back, then the HOT /
/// index-update bookkeeping asserts. `snapshot` is unused by the heap
/// implementation.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    _snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    let (mut tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    slot.base_mut().tts_tableOid = relation.rd_id;
    tuple.tuple.t_tableOid = relation.rd_id;

    let HeapUpdateResult {
        result,
        lockmode: hu_lockmode,
        update_indexes: hu_update_indexes,
    } = heapam::update::heap_update(
        mcx,
        relation,
        *otid,
        &mut tuple,
        cid,
        crosscheck.as_ref(),
        wait,
        tmfd,
    )?;
    *lockmode = hu_lockmode;
    *update_indexes = hu_update_indexes;

    slot.base_mut().tts_tid = tuple.tuple.t_self;

    // Mirror C's output asserts on heap_update's *update_indexes.
    if result != TM_Result::TM_Ok {
        debug_assert_eq!(*update_indexes, TU_UpdateIndexes::TU_None);
        *update_indexes = TU_UpdateIndexes::TU_None;
    } else if (header(&tuple).t_infomask2 & HEAP_ONLY_TUPLE) == 0 {
        debug_assert_eq!(*update_indexes, TU_UpdateIndexes::TU_All);
    } else {
        debug_assert!(
            *update_indexes == TU_UpdateIndexes::TU_Summarizing
                || *update_indexes == TU_UpdateIndexes::TU_None
        );
    }

    Ok(result)
}

// ===========================================================================
// tuple_lock
// ===========================================================================

/// `TransactionIdIsValid(xid)` — `xid != InvalidTransactionId`.
#[inline]
fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdEquals(x, y)`.
#[inline]
fn transaction_id_equals(x: TransactionId, y: TransactionId) -> bool {
    x == y
}

/// `TransactionIdIsCurrentTransactionId(xid)` via the xact owner seam.
#[inline]
fn transaction_id_is_current_transaction_id(xid: TransactionId) -> bool {
    backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xid)
}

/// Materialize the full on-page tuple (header incl. its null bitmap + user-data
/// area) at `(buffer, tid)` into an owned [`FormedTuple`], reading the page under
/// the pin the caller already holds. This is the owned rendering of C storing
/// the page-aliasing `t_data`/`t_len` (left set by `heap_lock_tuple`) into the
/// slot under the transferred buffer pin: C reads the on-page bytes lock-free
/// (pin only) at store time, and so do we.
fn materialize_on_page_formed<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: types_core::primitive::Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<FormedTuple<'mcx>> {
    let block = ItemPointerGetBlockNumber(&tid);
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<FormedTuple<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        let item = PageGetItem(&page, &item_id)?;
        let len = ItemIdGetLength(&item_id) as usize;
        out = Some(FormedTuple::read_on_page_full(
            mcx,
            &item[..len],
            block,
            offnum,
            rel_id,
        )?);
        Ok(())
    })?;
    Ok(out.expect("with_buffer_page closure must have run"))
}

/// `heapam_tuple_lock(relation, tid, snapshot, slot, cid, mode, wait_policy,
/// flags, tmfd)` (heapam_handler.c): lock the tuple at `tid` with
/// `heap_lock_tuple`, optionally chasing the update chain forward to the latest
/// row version (`TUPLE_LOCK_FLAG_FIND_LAST_VERSION`), and store the locked tuple
/// into the buffer slot. `snapshot` is unused by the heap implementation (the
/// chase uses its own DIRTY snapshot). Mirrors the C control flow: a
/// `tuple_lock_retry` outer loop around `heap_lock_tuple`, and, on a
/// `TM_Updated` result under FIND_LAST_VERSION, an inner DIRTY-snapshot fetch
/// loop that walks `t_ctid` forward and waits on conflicting lockers.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_lock<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: &ItemPointerData,
    _snapshot: &Snapshot,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    flags: u8,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    // follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
    let follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
    tmfd.traversed = false;

    // C mutates `*tid` across the chase; we track the working tid locally.
    let mut cur_tid = *tid;

    // The locked tuple's header + pinned buffer that we ultimately store. C's
    // `tuple` aliases `bslot->base.tupdata`, left pointing at the on-page tuple
    // by heap_lock_tuple; `buffer` is the pin heap_lock_tuple returned.
    let result: TM_Result;
    let final_buffer: Buffer;

    // tuple_lock_retry: ... goto tuple_lock_retry;
    'tuple_lock_retry: loop {
        // tuple->t_self = *tid;
        let lr = heapam::lock::heap_lock_tuple(
            mcx,
            relation,
            cur_tid,
            cid,
            mode,
            wait_policy,
            follow_updates,
        )?;
        let res = lr.result;
        let buffer = lr.buffer;
        // heap_lock_tuple fills *tmfd on the failure paths.
        *tmfd = lr.tmfd;
        // The header heap_lock_tuple left (t_self == cur_tid).
        let mut locked_hdr = lr.tuple;
        locked_hdr.t_self = cur_tid;

        if res == TM_Result::TM_Updated && (flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 {
            // Should not encounter speculative tuple on recheck.
            debug_assert!(!HeapTupleHeaderIsSpeculative(
                locked_hdr
                    .t_data
                    .as_ref()
                    .expect("locked tuple has no header")
            ));

            bufmgr_seam::release_buffer::call(buffer);

            if ItemPointerEquals(&tmfd.ctid, &locked_hdr.t_self) {
                // tuple was deleted, so give up
                return Ok(TM_Result::TM_Deleted);
            }

            // it was updated, so look at the updated version
            cur_tid = tmfd.ctid;
            // updated row should have xmin matching this xmax
            let mut prior_xmax = tmfd.xmax;
            // signal that a tuple later in the chain is getting locked
            tmfd.traversed = true;

            // fetch target tuple — loop to deal with updated or busy tuples.
            // (InitDirtySnapshot lives inside heap_fetch_dirty, which mints a
            // fresh SNAPSHOT_DIRTY per fetch and returns the stamped xmin/xmax.)
            'chase: loop {
                if ItemPointerIndicatesMovedPartitions(&cur_tid) {
                    return Err(PgError::error(
                        "tuple to be locked was already moved to another partition due to concurrent update",
                    )
                    .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE)
                    .into());
                }

                let fetched =
                    heapam::fetch::heap_fetch_dirty(mcx, relation, cur_tid)?;
                let fetch_buffer = fetched.userbuf;

                if fetched.found {
                    let ftup = fetched
                        .tuple
                        .as_ref()
                        .expect("heap_fetch_dirty found==true must carry the tuple");
                    let fhdr = ftup
                        .tuple
                        .t_data
                        .as_ref()
                        .expect("fetched tuple has no header");

                    // If xmin isn't what we're expecting, the slot was recycled
                    // for an unrelated tuple: the latest version was deleted,
                    // so do nothing.
                    if !transaction_id_equals(HeapTupleHeaderGetXmin(fhdr), prior_xmax) {
                        bufmgr_seam::release_buffer::call(fetch_buffer);
                        return Ok(TM_Result::TM_Deleted);
                    }

                    // otherwise xmin should not be dirty...
                    if transaction_id_is_valid(fetched.snapshot_xmin) {
                        let self_tid = ftup.tuple.t_self;
                        bufmgr_seam::release_buffer::call(fetch_buffer);
                        return Err(PgError::error(format!(
                            "t_xmin {} is uncommitted in tuple ({},{}) to be updated in table \"{}\"",
                            fetched.snapshot_xmin,
                            ItemPointerGetBlockNumber(&self_tid),
                            ItemPointerGetOffsetNumber(&self_tid),
                            relation.rd_rel.relname.as_str(),
                        ))
                        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
                        .into());
                    }

                    // If the tuple is being updated by another transaction we
                    // must wait for its commit/abort, or die trying.
                    if transaction_id_is_valid(fetched.snapshot_xmax) {
                        let snap_xmax = fetched.snapshot_xmax;
                        let self_tid = ftup.tuple.t_self;
                        bufmgr_seam::release_buffer::call(fetch_buffer);
                        match wait_policy {
                            LockWaitPolicy::LockWaitBlock => {
                                heapam::lock::xact_lock_table_wait(
                                    snap_xmax,
                                    relation,
                                    self_tid,
                                    XLTW_Oper::FetchUpdated,
                                )?;
                            }
                            LockWaitPolicy::LockWaitSkip => {
                                if !backend_storage_lmgr_lmgr_seams::conditional_xact_lock_table_wait::call(
                                    snap_xmax, false,
                                )? {
                                    // skip instead of waiting
                                    return Ok(TM_Result::TM_WouldBlock);
                                }
                            }
                            LockWaitPolicy::LockWaitError => {
                                if !backend_storage_lmgr_lmgr_seams::conditional_xact_lock_table_wait::call(
                                    snap_xmax,
                                    log_lock_failures(),
                                )? {
                                    return Err(PgError::error(format!(
                                        "could not obtain lock on row in relation \"{}\"",
                                        relation.rd_rel.relname.as_str(),
                                    ))
                                    .with_sqlstate(types_error::ERRCODE_LOCK_NOT_AVAILABLE)
                                    .into());
                                }
                            }
                        }
                        // loop back to repeat heap_fetch
                        continue 'chase;
                    }

                    // If the tuple was inserted by our own transaction, check
                    // cmin against cid: cmin >= current CID means our command
                    // cannot see the tuple, so ignore it. We just checked
                    // priorXmax == xmin, so test priorXmax instead of re-reading
                    // xmin.
                    if transaction_id_is_current_transaction_id(prior_xmax)
                        && combocid_seam::heap_tuple_header_get_cmin::call(fhdr) >= cid
                    {
                        tmfd.xmax = prior_xmax;
                        // Cmin is the problematic value, so store that.
                        tmfd.cmax = combocid_seam::heap_tuple_header_get_cmin::call(fhdr);
                        bufmgr_seam::release_buffer::call(fetch_buffer);
                        return Ok(TM_Result::TM_SelfModified);
                    }

                    // This is a live tuple, so try to lock it again.
                    bufmgr_seam::release_buffer::call(fetch_buffer);
                    cur_tid = ftup.tuple.t_self;
                    continue 'tuple_lock_retry;
                }

                // Not found (DIRTY-invisible) but kept the pin (keep_buf=true),
                // unless the line pointer was empty (tuple == None).
                let ftup = match fetched.tuple.as_ref() {
                    // If the referenced slot was actually empty, the latest
                    // version was deleted, so do nothing. (C: tuple->t_data ==
                    // NULL, buffer invalid.)
                    None => {
                        debug_assert!(!types_storage::BufferIsValid(fetch_buffer));
                        return Ok(TM_Result::TM_Deleted);
                    }
                    Some(t) => t,
                };
                let fhdr = ftup
                    .tuple
                    .t_data
                    .as_ref()
                    .expect("fetched tuple has no header");

                // As above, if xmin isn't what we're expecting, do nothing.
                if !transaction_id_equals(HeapTupleHeaderGetXmin(fhdr), prior_xmax) {
                    bufmgr_seam::release_buffer::call(fetch_buffer);
                    return Ok(TM_Result::TM_Deleted);
                }

                // The tuple was found but failed SnapshotDirty: it was updated
                // or deleted by a committed xact or our own xact. If deleted,
                // ignore; if updated, chain to the next version and repeat.
                // Examining xmax / t_ctid without the content lock is safe under
                // the buffer pin (they can't be changing).
                if ItemPointerEquals(&ftup.tuple.t_self, &fhdr.t_ctid) {
                    // deleted, so forget about it
                    bufmgr_seam::release_buffer::call(fetch_buffer);
                    return Ok(TM_Result::TM_Deleted);
                }

                // updated, so look at the updated row
                cur_tid = fhdr.t_ctid;
                // updated row should have xmin matching this xmax
                prior_xmax = HeapTupleHeaderGetUpdateXid(fhdr)?;
                bufmgr_seam::release_buffer::call(fetch_buffer);
                // loop back to fetch next in chain
            }
        }

        // Success (or a non-chased failure result): store the locked tuple.
        result = res;
        final_buffer = buffer;
        break 'tuple_lock_retry;
    }

    // slot->tts_tableOid = RelationGetRelid(relation);
    slot.base_mut().tts_tableOid = relation.rd_id;

    // Materialize the locked tuple (heap_lock_tuple left it on the still-pinned
    // page at cur_tid) and store it, transferring the existing pin. t_tableOid
    // is stamped by ExecStorePinnedBufferHeapTuple from the tuple's header, so
    // set it on the materialized FormedTuple to match C's `tuple->t_tableOid =
    // slot->tts_tableOid`.
    let mut formed = materialize_on_page_formed(mcx, relation.rd_id, final_buffer, cur_tid)?;
    formed.tuple.t_tableOid = relation.rd_id;

    slot_seam::exec_store_pinned_buffer_heap_tuple::call(formed, slot, final_buffer)?;

    Ok(result)
}

/// `log_lock_failures` GUC (heapam.c reads it for the LockWaitError wait path).
fn log_lock_failures() -> bool {
    backend_utils_misc_guc_tables::vars::log_lock_failures.read()
}

// ===========================================================================
// relation_set_new_filelocator
// ===========================================================================

/// `heapam_relation_set_new_filelocator(rel, newrlocator, persistence,
/// &freezeXid, &minmulti)` (heapam_handler.c): `*freezeXid = RecentXmin`,
/// `*minmulti = GetOldestMultiXactId()`, then the storage-creation leg
/// (`RelationCreateStorage` + unlogged INIT fork + `smgrclose`), which lives in
/// the `storage.c` owner. Returns the AM-chosen `(relfrozenxid, relminmxid)`.
fn heapam_relation_set_new_filelocator(
    _rel: &Relation<'_>,
    newrlocator: &RelFileLocator,
    persistence: i8,
) -> PgResult<(u32, u32)> {
    let freeze_xid = backend_utils_time_snapmgr::RecentXmin();
    let minmulti = backend_access_transam_multixact_seams::get_oldest_multi_xact_id::call()?;

    backend_catalog_storage_seams::relation_set_new_filelocator_storage::call(
        *newrlocator,
        persistence,
    )?;

    Ok((freeze_xid, minmulti))
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install the DML tuple-modification seams the heapam-handler core `::call`s,
/// including `heapam_tuple_lock` (the heap row-lock + FIND_LAST_VERSION
/// update-chase path).
pub fn init_seams() {
    use backend_access_heap_heapam_handler_dml_seams as sx;
    sx::heapam_tuple_insert::set(heapam_tuple_insert);
    sx::heapam_tuple_insert_speculative::set(heapam_tuple_insert_speculative);
    sx::heapam_tuple_complete_speculative::set(heapam_tuple_complete_speculative);
    sx::heapam_multi_insert::set(heapam_multi_insert);
    sx::heapam_tuple_delete::set(heapam_tuple_delete);
    sx::heapam_tuple_update::set(heapam_tuple_update);
    sx::heapam_tuple_lock::set(heapam_tuple_lock);
    sx::heapam_relation_set_new_filelocator::set(heapam_relation_set_new_filelocator);
}
