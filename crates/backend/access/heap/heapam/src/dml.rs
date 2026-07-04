// heapam.c DML phase 2: heap_insert / heap_delete / heap_update cores.
// Deferred (named panics): toast, visibilitymap clears (pages can't be
// all-visible until vacuum lands), MultiXact create/expand/wait lanes,
// speculative-insert driver, index-attr bitmaps (updates on relhasindex
// rels), bulk insert + heap_multi_insert, heap_lock_tuple (phase 3).
// C divergences: logical-decoding gates (RelationIsLogicallyLogged,
// log_heap_new_cid, ExtractReplicaIdentity payloads) are const-false like
// the read lane's CheckXidAlive; crit sections pend miscadmin; WAL
// prefix/suffix compression off (XLogCheckBufferNeedsBackup pends
// xloginsert), records stay redo-compatible.
use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use ::tableam_vocab::{
    BulkInsertStateData, LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result,
    TU_UpdateIndexes,
};
use ::types_core::xact::{InvalidTransactionId, TransactionIdIsValid};
use ::types_core::{CommandId, InvalidBlockNumber, TransactionId};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_rel::{RelationData, RELKIND_MATVIEW, RELKIND_RELATION};
use ::types_snapshot::SnapshotData;
use ::types_storage::bufpage::{ItemIdData, PageMut, PageRef, SizeofHeapTupleHeader};
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, RowShareLock, LOCKMODE,
};
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, ItemPointerData, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, HEAP2_XACT_MASK, HEAP_KEYS_UPDATED, HEAP_LOCK_MASK, HEAP_MOVED,
    HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_BITS, HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK,
    HEAP_XMAX_INVALID, HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_SHR_LOCK, HEAP_LOCKED_UPGRADED,
};
use ::xloginsert_seams::{REGBUF_KEEP_DATA, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::hio::{RelationGetBufferForTuple, RelationPutHeapTuple, HEAP_INSERT_SPECULATIVE};
use crate::{unported, HeapTupleHeaderGetUpdateXid, MultiXactIdGetUpdateXid};
use heapam_visibility_seams as hv_seam;

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_INSERT_IS_SPECULATIVE: u8 = 1 << 2;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = 1 << 1;
pub const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 1 << 0;

pub const XLHL_XMAX_IS_MULTI: u8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: u8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: u8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: u8 = 0x08;
pub const XLHL_KEYS_UPDATED: u8 = 0x10;

const XLOG_INCLUDE_ORIGIN: u8 = 0x01;

// MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE = 4) (heaptoast.h).
pub const TOAST_TUPLE_THRESHOLD: usize = 2032;

const RM_HEAP_ID: u8 = rmgr::RmgrIds::RM_HEAP_ID as u8;

// tupleLockExtraInfo[mode].hwlock (heapam.c).
const fn tuple_lock_hwlock(mode: LockTupleMode) -> LOCKMODE {
    match mode {
        LockTupleMode::LockTupleKeyShare => AccessShareLock,
        LockTupleMode::LockTupleShare => RowShareLock,
        LockTupleMode::LockTupleNoKeyExclusive => ExclusiveLock,
        LockTupleMode::LockTupleExclusive => AccessExclusiveLock,
    }
}

pub(crate) fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
}

// XLogLogicalInfoActive() && ...: wal_level=logical unported, const-false.
fn relation_is_logically_logged(_rel: &RelationData<'_>) -> bool {
    false
}

fn relation_is_accessible_in_logical_decoding(_rel: &RelationData<'_>) -> bool {
    false
}

// IsParallelWorker() (miscadmin.h): parallel workers unported.
fn is_parallel_worker() -> bool {
    false
}

fn xl_heap_header(hdr: &::types_tuple::HeapTupleHeaderData) -> [u8; 5] {
    let mut b = [0u8; 5];
    b[0..2].copy_from_slice(&hdr.t_infomask2.to_ne_bytes());
    b[2..4].copy_from_slice(&hdr.t_infomask.to_ne_bytes());
    b[4] = hdr.t_hoff;
    b
}

#[inline]
fn compute_infobits(infomask: u16, infomask2: u16) -> u8 {
    (if (infomask & HEAP_XMAX_IS_MULTI) != 0 { XLHL_XMAX_IS_MULTI } else { 0 })
        | (if (infomask & HEAP_XMAX_LOCK_ONLY) != 0 { XLHL_XMAX_LOCK_ONLY } else { 0 })
        | (if (infomask & HEAP_XMAX_EXCL_LOCK) != 0 { XLHL_XMAX_EXCL_LOCK } else { 0 })
        | (if (infomask & HEAP_XMAX_KEYSHR_LOCK) != 0 { XLHL_XMAX_KEYSHR_LOCK } else { 0 })
        | (if (infomask2 & HEAP_KEYS_UPDATED) != 0 { XLHL_KEYS_UPDATED } else { 0 })
}

#[inline]
fn xmax_infomask_changed(new_infomask: u16, old_infomask: u16) -> bool {
    const INTERESTING: u16 = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;
    (new_infomask & INTERESTING) != (old_infomask & INTERESTING)
}

// PageSetPrunable(page, xid).
fn page_set_prunable(page: &mut PageMut<'_>, xid: TransactionId) {
    debug_assert!(TransactionIdIsValid(xid));
    let old = page.as_ref().prune_xid();
    if !TransactionIdIsValid(old) || ::types_core::xact::TransactionIdPrecedes(xid, old) {
        page.set_prune_xid(xid);
    }
}

#[cold]
#[inline(never)]
fn invisible_tuple(op: &str) -> Box<PgError> {
    Box::new(
        PgError::error(std::format!("attempted to {op} invisible tuple"))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

// SAFETY-bearing helper: page-backed tuple view under the caller's pin+lock.
/// # Safety
/// The image is valid only while the pin behind `page` is held; the erased
/// `'any` view must not outlive it (release the pin after the last use).
unsafe fn page_tuple<'any>(
    page: PageRef<'_>,
    lp: ItemIdData,
    tid: ItemPointerData,
    rel: &RelationData<'_>,
) -> HeapTupleData<'any> {
    let (ptr, len) = page.item_raw(lp);
    // SAFETY: item_raw bounds-checks against the pinned page image.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, rel.rd_id) }
}

fn heap_prepare_insert(
    relation: &RelationData<'_>,
    tup: &mut HeapTupleData<'_>,
    xid: TransactionId,
    cid: CommandId,
    options: i32,
) -> PgResult<()> {
    if is_parallel_worker() {
        return Err(Box::new(
            PgError::error("cannot insert tuples in a parallel worker")
                .with_sqlstate(::types_error::ERRCODE_INVALID_TRANSACTION_STATE),
        ));
    }

    let hdr = tup.t_data_mut();
    hdr.t_infomask &= !HEAP_XACT_MASK;
    hdr.t_infomask2 &= !HEAP2_XACT_MASK;
    hdr.t_infomask |= HEAP_XMAX_INVALID;
    hdr.set_xmin(xid);
    if (options & crate::hio::HEAP_INSERT_FROZEN) != 0 {
        hdr.set_xmin_frozen();
    }
    hdr.set_cmin(cid);
    hdr.set_xmax(0);
    tup.t_tableOid = relation.rd_id;

    if relation.rd_rel.relkind != RELKIND_RELATION && relation.rd_rel.relkind != RELKIND_MATVIEW {
        debug_assert!(!tup.has_external());
    }
    Ok(())
}

fn needs_toast(relation: &RelationData<'_>, tup: &HeapTupleData<'_>) -> bool {
    (relation.rd_rel.relkind == RELKIND_RELATION || relation.rd_rel.relkind == RELKIND_MATVIEW)
        && (tup.has_external() || tup.t_len as usize > TOAST_TUPLE_THRESHOLD)
}

/// `heap_insert`: stamps `tup` and stores it; `tup.t_self` receives the TID.
pub fn heap_insert(
    relation: &RelationData<'_>,
    tup: &mut HeapTupleData<'_>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    let xid = xact_seams::get_current_transaction_id::call()?;

    heap_prepare_insert(relation, tup, xid, cid, options)?;

    // Cold: scratch context per oversized-value insert (C's palloc'd toasted
    // copy dies at heap_freetuple; here it dies with the context).
    let toast_ctx;
    let mut toasted = None;
    let mut erased;
    let heaptup: &mut HeapTupleData<'_> = if needs_toast(relation, tup) {
        toast_ctx = ::mcx::MemoryContext::new("heap_toast_insert_or_update");
        toasted = heaptoast_seams::heap_toast_insert_or_update::call(
            toast_ctx.mcx(),
            relation,
            tup,
            None,
            options,
        )?;
        match toasted.as_mut() {
            Some(t) => {
                let ht = t.as_tuple_mut();
                // SAFETY: image owned by toast_ctx, which outlives every use
                // in this function (lifetime-erased view, page_tuple model).
                erased = unsafe {
                    HeapTupleData::from_raw_parts(
                        ht.header_ptr().cast_mut(),
                        ht.t_len,
                        ht.t_self,
                        ht.t_tableOid,
                    )
                };
                &mut erased
            }
            None => tup,
        }
    } else {
        tup
    };

    let pin =
        RelationGetBufferForTuple(relation, heaptup.t_len as usize, None, options, bistate, 0)?;

    predicate_seams::check_for_serializable_conflict_in::call(
        relation,
        None,
        InvalidBlockNumber,
    )?;

    RelationPutHeapTuple(relation, &pin, heaptup, (options & HEAP_INSERT_SPECULATIVE) != 0)?;

    // C pins the VM page inside RelationGetBufferForTuple, before the content
    // lock, so the clear here never does IO under the lock; this pin-at-clear
    // shape is a recorded divergence (single-backend lane).
    let mut all_visible_cleared = false;
    if pin.page().is_all_visible() {
        all_visible_cleared = true;
        let mut vmb = visibilitymap::VmBuffer::new();
        visibilitymap::visibilitymap_pin(relation, pin.block_number(), &mut vmb)?;
        // SAFETY: pinned + exclusive content lock since RelationGetBufferForTuple.
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.clear_all_visible();
        visibilitymap::visibilitymap_clear(
            relation,
            pin.block_number(),
            &vmb,
            visibilitymap::VISIBILITYMAP_VALID_BITS,
        )?;
        vmb.release();
    }

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        let page = pin.page();
        let mut info = XLOG_HEAP_INSERT;
        let mut bufflags = REGBUF_STANDARD;
        let offnum = ItemPointerGetOffsetNumber(&heaptup.t_self);

        if offnum == FirstOffsetNumber && page.max_offset_number() == FirstOffsetNumber {
            info |= XLOG_HEAP_INIT_PAGE;
            bufflags |= REGBUF_WILL_INIT;
        }

        let mut flags = 0u8;
        if all_visible_cleared {
            flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
        }
        if (options & HEAP_INSERT_SPECULATIVE) != 0 {
            flags |= XLH_INSERT_IS_SPECULATIVE;
        }
        let mut xlrec = [0u8; 3];
        xlrec[0..2].copy_from_slice(&offnum.to_ne_bytes());
        xlrec[2] = flags;

        let xlhdr = xl_heap_header(heaptup.t_data());
        // SAFETY: tuple image is t_len readable bytes.
        let body = unsafe {
            core::slice::from_raw_parts(
                heaptup.header_ptr().add(SizeofHeapTupleHeader),
                heaptup.t_len as usize - SizeofHeapTupleHeader,
            )
        };

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(&heaptup.t_self),
                pin.buffer(),
                bufflags,
                &[&xlhdr, body],
            )],
        )?;
        // SAFETY: pinned + exclusively locked since RelationGetBufferForTuple.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    pin.release();

    inval::invalidate::CacheInvalidateHeapTuple(relation, heaptup, None)?;

    if relation.pgstat_enabled.get() {
        pgstat::relation::pgstat_count_heap_insert(relation.rd_id, relation.rd_rel.relisshared, 1);
    }

    let heaptup_self = heaptup.t_self;
    if toasted.is_some() {
        tup.t_self = heaptup_self;
    }
    Ok(())
}

/// `simple_heap_insert`.
pub fn simple_heap_insert(relation: &RelationData<'_>, tup: &mut HeapTupleData<'_>) -> PgResult<()> {
    heap_insert(relation, tup, xact_seams::get_current_command_id::call(true)?, 0, None)
}

const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
const XLH_INSERT_LAST_IN_MULTI: u8 = 1 << 1;
const SizeOfHeapMultiInsert: usize = 4;
const SizeOfMultiInsertTuple: usize = 7;
const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;

fn heap_multi_insert_pages(
    heaptuples: &[HeapTupleData<'_>],
    done: usize,
    save_free_space: usize,
) -> i32 {
    let fresh =
        ::types_core::BLCKSZ - ::types_storage::bufpage::SizeOfPageHeaderData - save_free_space;
    let mut page_avail = fresh;
    let mut npages = 1;
    for t in &heaptuples[done..] {
        let tup_sz = core::mem::size_of::<ItemIdData>() + ((t.t_len as usize + 7) & !7);
        if page_avail < tup_sz {
            npages += 1;
            page_avail = fresh;
        }
        page_avail -= tup_sz;
    }
    npages
}

/// `heap_multi_insert`: slots are materialized in place; `tts_tid` and the
/// slot tuples' `t_self` receive the TIDs.
pub fn heap_multi_insert<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relation: &RelationData<'mcx>,
    slots: &mut [&mut ::types_slot::SlotData<'mcx>],
    cid: CommandId,
    options: i32,
    mut bistate: Option<&mut ::tableam_vocab::BulkInsertStateData>,
) -> PgResult<()> {
    use ::types_slot::SlotData;

    debug_assert!(options & crate::hio::HEAP_INSERT_NO_LOGICAL == 0);
    let xid = xact_seams::get_current_transaction_id::call()?;
    let needwal = relation_needs_wal(relation);
    let save_free_space =
        relation.get_target_page_free_space(crate::hio::HEAP_DEFAULT_FILLFACTOR) as usize;
    let ntuples = slots.len();

    // std Vecs: droppy owners (contexts) and per-call scratch views — neither
    // may live in an mcx arena (no-drop rule); C pallocs the pointer array.
    let mut toast_ctxs: Vec<::mcx::MemoryContext> = Vec::new();
    let mut heaptuples: Vec<HeapTupleData<'_>> = Vec::with_capacity(ntuples);
    for slot in slots.iter_mut() {
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = relation.rd_id;
        let tuple = match &mut **slot {
            SlotData::Heap(h) => h.tuple.as_mut(),
            SlotData::BufferHeap(b) => b.base.tuple.as_mut(),
            _ => panic!("heap_multi_insert: non-heap slot copy arm not ported"),
        }
        .expect("materialized heap slot holds a tuple");
        tuple.t_tableOid = relation.rd_id;
        heap_prepare_insert(relation, tuple, xid, cid, options)?;
        if needs_toast(relation, tuple) {
            let toast_ctx = ::mcx::MemoryContext::new("heap_multi_insert_toast");
            let erased = {
                let toasted = heaptoast_seams::heap_toast_insert_or_update::call(
                    toast_ctx.mcx(),
                    relation,
                    tuple,
                    None,
                    options,
                )?;
                toasted.map(|mut t| {
                    let ht = t.as_tuple_mut();
                    // SAFETY: image owned by toast_ctx, kept alive in
                    // toast_ctxs past the last use (page_tuple model).
                    unsafe {
                        HeapTupleData::from_raw_parts(
                            ht.header_ptr().cast_mut(),
                            ht.t_len,
                            ht.t_self,
                            ht.t_tableOid,
                        )
                    }
                })
            };
            match erased {
                Some(erased) => {
                    toast_ctxs.push(toast_ctx);
                    heaptuples.push(erased);
                }
                None => {
                    // SAFETY: image owned by the materialized slot, which
                    // outlives every use in this function.
                    heaptuples.push(unsafe {
                        HeapTupleData::from_raw_parts(
                            tuple.header_ptr().cast_mut(),
                            tuple.t_len,
                            tuple.t_self,
                            tuple.t_tableOid,
                        )
                    });
                }
            }
        } else {
            // SAFETY: as above; the materialized slot image outlives this call.
            heaptuples.push(unsafe {
                HeapTupleData::from_raw_parts(
                    tuple.header_ptr().cast_mut(),
                    tuple.t_len,
                    tuple.t_self,
                    tuple.t_tableOid,
                )
            });
        }
    }

    predicate_seams::check_for_serializable_conflict_in::call(relation, None, InvalidBlockNumber)?;

    // C's PGAlignedBlock WAL scratch.
    let mut scratch = std::vec![0u8; ::types_core::BLCKSZ];
    let mut ndone = 0usize;
    let mut npages = 0i32;
    let mut npages_used = 0i32;
    let mut starting_with_empty_page = false;
    while ndone < ntuples {
        if ndone == 0 || !starting_with_empty_page {
            npages = heap_multi_insert_pages(&heaptuples, ndone, save_free_space);
            npages_used = 0;
        } else {
            npages_used += 1;
        }

        let pin = RelationGetBufferForTuple(
            relation,
            heaptuples[ndone].t_len as usize,
            None,
            options,
            bistate.as_deref_mut(),
            npages - npages_used,
        )?;
        starting_with_empty_page = pin.page().max_offset_number() == 0;

        std::eprintln!(
            "TOASTPROBE mi place[{}]: len={} xmin={} infomask={:#x} rel={} tid_after={:?}",
            ndone,
            heaptuples[ndone].t_len,
            heaptuples[ndone].t_data().xmin_raw(),
            heaptuples[ndone].t_data().t_infomask,
            relation.rd_id,
            heaptuples[ndone].t_self
        );
        RelationPutHeapTuple(relation, &pin, &mut heaptuples[ndone], false)?;
        let mut nthispage = 1usize;
        while ndone + nthispage < ntuples {
            let need =
                ((heaptuples[ndone + nthispage].t_len as usize + 7) & !7) + save_free_space;
            if pin.page().heap_free_space() < need {
                break;
            }
            let i = ndone + nthispage;
            std::eprintln!(
                "TOASTPROBE mi place[{}]: len={} xmin={} infomask={:#x} rel={} tid_after={:?}",
                i,
                heaptuples[i].t_len,
                heaptuples[i].t_data().xmin_raw(),
                heaptuples[i].t_data().t_infomask,
                relation.rd_id,
                heaptuples[i].t_self
            );
            RelationPutHeapTuple(relation, &pin, &mut heaptuples[ndone + nthispage], false)?;
            nthispage += 1;
        }

        // Pin-at-clear divergence (heap_insert shape): C pins the vm page in
        // RelationGetBufferForTuple, before the content lock.
        let mut all_visible_cleared = false;
        if pin.page().is_all_visible() {
            all_visible_cleared = true;
            let mut vmb = visibilitymap::VmBuffer::new();
            visibilitymap::visibilitymap_pin(relation, pin.block_number(), &mut vmb)?;
            // SAFETY: pinned + exclusive content lock since RelationGetBufferForTuple.
            let mut pm =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            pm.clear_all_visible();
            visibilitymap::visibilitymap_clear(
                relation,
                pin.block_number(),
                &vmb,
                visibilitymap::VISIBILITYMAP_VALID_BITS,
            )?;
            vmb.release();
        }

        bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

        if needwal {
            let init = starting_with_empty_page;
            let mut xl_flags = 0u8;
            if all_visible_cleared {
                xl_flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
            }
            if ndone + nthispage == ntuples {
                xl_flags |= XLH_INSERT_LAST_IN_MULTI;
            }
            scratch[0] = xl_flags;
            scratch[1] = 0;
            scratch[2..4].copy_from_slice(&(nthispage as u16).to_ne_bytes());
            let mut off = SizeOfHeapMultiInsert;
            if !init {
                for i in 0..nthispage {
                    let offnum = ItemPointerGetOffsetNumber(&heaptuples[ndone + i].t_self);
                    scratch[off..off + 2].copy_from_slice(&offnum.to_ne_bytes());
                    off += 2;
                }
            }
            let tupledata_off = off;
            for i in 0..nthispage {
                let heaptup = &heaptuples[ndone + i];
                // xl_multi_insert_tuple needs two-byte alignment; offsets are
                // relative to the scratch base like C's SHORTALIGN(scratchptr).
                off = (off + 1) & !1;
                let hdr = heaptup.t_data();
                let datalen = heaptup.t_len as usize - SizeofHeapTupleHeader;
                scratch[off..off + 2].copy_from_slice(&(datalen as u16).to_ne_bytes());
                scratch[off + 2..off + 4].copy_from_slice(&hdr.t_infomask2.to_ne_bytes());
                scratch[off + 4..off + 6].copy_from_slice(&hdr.t_infomask.to_ne_bytes());
                scratch[off + 6] = hdr.t_hoff;
                off += SizeOfMultiInsertTuple;
                // SAFETY: tuple image is t_len readable bytes.
                let body = unsafe {
                    core::slice::from_raw_parts(
                        heaptup.header_ptr().add(SizeofHeapTupleHeader),
                        datalen,
                    )
                };
                scratch[off..off + datalen].copy_from_slice(body);
                off += datalen;
            }
            debug_assert!(off < ::types_core::BLCKSZ);

            let mut info = XLOG_HEAP2_MULTI_INSERT;
            let mut bufflags = REGBUF_STANDARD;
            if init {
                info |= XLOG_HEAP_INIT_PAGE;
                bufflags |= REGBUF_WILL_INIT;
            }

            let recptr = crate::wal::insert_record(
                RM_HEAP2_ID,
                info,
                XLOG_INCLUDE_ORIGIN,
                &[&scratch[..tupledata_off]],
                &[crate::wal::reg_block(
                    0,
                    relation.rd_locator.get(),
                    ItemPointerGetBlockNumber(&heaptuples[ndone].t_self),
                    pin.buffer(),
                    bufflags,
                    &[&scratch[tupledata_off..off]],
                )],
            )?;
            // SAFETY: pinned + exclusively locked since RelationGetBufferForTuple.
            let mut pm =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            pm.set_lsn(recptr);
        }

        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        pin.release();
        ndone += nthispage;
    }

    predicate_seams::check_for_serializable_conflict_in::call(relation, None, InvalidBlockNumber)?;

    if catalog_seams::is_catalog_relation::call(relation) {
        for t in &heaptuples {
            inval::invalidate::CacheInvalidateHeapTuple(relation, t, None)?;
        }
    }

    for (slot, t) in slots.iter_mut().zip(&heaptuples) {
        slot.base_mut().tts_tid = t.t_self;
        if let Some(tuple) = match &mut **slot {
            SlotData::Heap(h) => h.tuple.as_mut(),
            SlotData::BufferHeap(b) => b.base.tuple.as_mut(),
            _ => None,
        } {
            tuple.t_self = t.t_self;
        }
    }

    if relation.pgstat_enabled.get() {
        pgstat::relation::pgstat_count_heap_insert(
            relation.rd_id,
            relation.rd_rel.relisshared,
            ntuples as i64,
        );
    }

    drop(toast_ctxs);
    Ok(())
}

/// `compute_new_xmax_infomask`: (new_xmax, new_infomask, new_infomask2).
/// MultiXact create/expand arms are unported (named panics).
fn compute_new_xmax_infomask(
    xmax: TransactionId,
    old_infomask: u16,
    old_infomask2: u16,
    add_to_xmax: TransactionId,
    mode: LockTupleMode,
    is_update: bool,
) -> PgResult<(TransactionId, u16, u16)> {
    let mut old_infomask = old_infomask;
    let mut mode = mode;
    loop {
        let mut new_infomask = 0u16;
        let mut new_infomask2 = 0u16;
        if (old_infomask & HEAP_XMAX_INVALID) != 0 {
            let new_xmax;
            if is_update {
                new_xmax = add_to_xmax;
                if mode == LockTupleMode::LockTupleExclusive {
                    new_infomask2 |= HEAP_KEYS_UPDATED;
                }
            } else {
                new_infomask |= HEAP_XMAX_LOCK_ONLY;
                new_xmax = add_to_xmax;
                match mode {
                    LockTupleMode::LockTupleKeyShare => new_infomask |= HEAP_XMAX_KEYSHR_LOCK,
                    LockTupleMode::LockTupleShare => new_infomask |= HEAP_XMAX_SHR_LOCK,
                    LockTupleMode::LockTupleNoKeyExclusive => new_infomask |= HEAP_XMAX_EXCL_LOCK,
                    LockTupleMode::LockTupleExclusive => {
                        new_infomask |= HEAP_XMAX_EXCL_LOCK;
                        new_infomask2 |= HEAP_KEYS_UPDATED;
                    }
                }
            }
            return Ok((new_xmax, new_infomask, new_infomask2));
        } else if (old_infomask & HEAP_XMAX_IS_MULTI) != 0 {
            debug_assert!((old_infomask & HEAP_XMAX_COMMITTED) == 0);
            if HEAP_LOCKED_UPGRADED(old_infomask) {
                old_infomask &= !HEAP_XMAX_IS_MULTI;
                old_infomask |= HEAP_XMAX_INVALID;
                continue;
            }
            let running = multixact_seams::multi_xact_id_is_running::call(
                xmax,
                HEAP_XMAX_IS_LOCKED_ONLY(old_infomask),
            )?;
            if !running {
                let update_committed = if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                    false
                } else {
                    transam_seams::transaction_id_did_commit::call(MultiXactIdGetUpdateXid(
                        xmax,
                        old_infomask,
                    )?)?
                };
                if !update_committed {
                    old_infomask &= !HEAP_XMAX_IS_MULTI;
                    old_infomask |= HEAP_XMAX_INVALID;
                    continue;
                }
            }
            unported("MultiXactIdExpand (multixact.c)");
        } else if (old_infomask & HEAP_XMAX_COMMITTED) != 0 {
            unported("MultiXactIdCreate (multixact.c)");
        } else if procarray_seams::transaction_id_is_in_progress::call(xmax)? {
            if xmax == add_to_xmax {
                debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));
                // acquire the strongest of both; single-xid restart trick
                let old_mode = if ::types_tuple::HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                    LockTupleMode::LockTupleKeyShare
                } else if ::types_tuple::HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                    LockTupleMode::LockTupleShare
                } else if ::types_tuple::HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                    if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                        LockTupleMode::LockTupleExclusive
                    } else {
                        LockTupleMode::LockTupleNoKeyExclusive
                    }
                } else {
                    old_infomask |= HEAP_XMAX_INVALID;
                    old_infomask &= !HEAP_XMAX_LOCK_ONLY;
                    continue;
                };
                if (mode as u32) < (old_mode as u32) {
                    mode = old_mode;
                }
                old_infomask |= HEAP_XMAX_INVALID;
                continue;
            }
            unported("MultiXactIdCreate (multixact.c)");
        } else if !HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)
            && transam_seams::transaction_id_did_commit::call(xmax)?
        {
            unported("MultiXactIdCreate (multixact.c)");
        } else {
            // locker finished between infomask read and in-progress check
            old_infomask |= HEAP_XMAX_INVALID;
            continue;
        }
    }
}

/// `UpdateXmaxHintBits`.
fn update_xmax_hint_bits(
    tuple: &mut HeapTupleData<'_>,
    buffer: ::types_core::Buffer,
    xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(tuple.t_data().xmax_raw() == xid);
    debug_assert!((tuple.t_data().t_infomask & HEAP_XMAX_IS_MULTI) == 0);

    if (tuple.t_data().t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)) == 0 {
        if !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_data().t_infomask)
            && transam_seams::transaction_id_did_commit::call(xid)?
        {
            hv_seam::heap_tuple_set_hint_bits::call(
                tuple.t_data_mut(),
                buffer,
                HEAP_XMAX_COMMITTED,
                xid,
            )?;
        } else {
            hv_seam::heap_tuple_set_hint_bits::call(
                tuple.t_data_mut(),
                buffer,
                HEAP_XMAX_INVALID,
                InvalidTransactionId,
            )?;
        }
    }
    Ok(())
}

/// `heap_acquire_tuplock`.
fn heap_acquire_tuplock(
    relation: &RelationData<'_>,
    tid: &ItemPointerData,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    have_tuple_lock: &mut bool,
) -> PgResult<bool> {
    if *have_tuple_lock {
        return Ok(true);
    }
    match wait_policy {
        LockWaitPolicy::LockWaitBlock => {
            lmgr::LockTuple(relation, tid, tuple_lock_hwlock(mode))?;
        }
        LockWaitPolicy::LockWaitSkip => {
            if !lmgr::ConditionalLockTuple(relation, tid, tuple_lock_hwlock(mode), false)? {
                return Ok(false);
            }
        }
        LockWaitPolicy::LockWaitError => {
            if !lmgr::ConditionalLockTuple(relation, tid, tuple_lock_hwlock(mode), false)? {
                return Err(Box::new(
                    PgError::error(std::format!(
                        "could not obtain lock on row in relation \"{}\"",
                        relation.name()
                    ))
                    .with_sqlstate(::types_error::ERRCODE_LOCK_NOT_AVAILABLE),
                ));
            }
        }
    }
    *have_tuple_lock = true;
    Ok(true)
}

// ExtractReplicaIdentity: gated on RelationIsLogicallyLogged (const-false).
fn extract_replica_identity<'a>(
    relation: &RelationData<'_>,
    _tp: &HeapTupleData<'a>,
    _key_required: bool,
) -> Option<HeapTupleData<'a>> {
    if !relation_is_logically_logged(relation) {
        return None;
    }
    unported("ExtractReplicaIdentity beyond the wal_level gate (heapam.c)")
}

/// `heap_delete` core. Concurrent-updater wait lanes past the self-update
/// case reach lmgr/XactLockTableWait; MultiXact conflicts panic unported.
pub fn heap_delete(
    relation: &RelationData<'_>,
    tid: &ItemPointerData,
    cid: CommandId,
    crosscheck: Option<&SnapshotData<'_>>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    let xid = xact_seams::get_current_transaction_id::call()?;

    if xact_seams::is_in_parallel_mode::call() {
        return Err(Box::new(
            PgError::error("cannot delete tuples during a parallel operation")
                .with_sqlstate(::types_error::ERRCODE_INVALID_TRANSACTION_STATE),
        ));
    }

    let block = ItemPointerGetBlockNumber(tid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");

    if pin.page().is_all_visible() {
        unported("visibilitymap_clear write side (heap_delete all-visible page, visibilitymap.c)");
    }
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let lp = pin.page().item_id(ItemPointerGetOffsetNumber(tid));
    debug_assert!(lp.is_normal());

    let mut have_tuple_lock = false;
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };

    let mut result = 'l1: loop {
        if pin.page().is_all_visible() {
            unported("visibilitymap_clear write side (heap_delete all-visible page, visibilitymap.c)");
        }
        let mut result = hv_seam::heap_tuple_satisfies_update::call(&mut tp, cid, pin.buffer())?;

        if result == TM_Result::TM_Invisible {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            pin.release();
            return Err(invisible_tuple("delete"));
        } else if result == TM_Result::TM_BeingModified && wait {
            let xwait = tp.t_data().xmax_raw();
            let infomask = tp.t_data().t_infomask;

            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                unported("DoesMultiXactIdConflict/MultiXactIdWait (heap_delete, multixact.c)");
            } else if !xact_seams::transaction_id_is_current_transaction_id::call(xwait) {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                heap_acquire_tuplock(
                    relation,
                    &tp.t_self,
                    LockTupleMode::LockTupleExclusive,
                    LockWaitPolicy::LockWaitBlock,
                    &mut have_tuple_lock,
                )?;
                lmgr::XactLockTableWait(
                    xwait,
                    Some(relation),
                    Some(&tp.t_self),
                    ::types_storage::lock::XLTW_Oper::Delete,
                )?;
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

                if xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                    || tp.t_data().xmax_raw() != xwait
                {
                    continue 'l1;
                }
                update_xmax_hint_bits(&mut tp, pin.buffer(), xwait)?;
            }

            if (tp.t_data().t_infomask & HEAP_XMAX_INVALID) != 0
                || HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data().t_infomask)
                || hv_seam::heap_tuple_header_is_only_locked::call(tp.t_data())?
            {
                result = TM_Result::TM_Ok;
            } else if tp.t_self != tp.t_data().t_ctid {
                result = TM_Result::TM_Updated;
            } else {
                result = TM_Result::TM_Deleted;
            }
        }

        if let (Some(snap), TM_Result::TM_Ok) = (crosscheck, result) {
            if !hv_seam::heap_tuple_satisfies_visibility::call(&mut tp, snap, pin.buffer())? {
                result = TM_Result::TM_Updated;
            }
        }
        break result;
    };

    if result != TM_Result::TM_Ok {
        tmfd.ctid = tp.t_data().t_ctid;
        tmfd.xmax = HeapTupleHeaderGetUpdateXid(tp.t_data())?;
        tmfd.cmax = if result == TM_Result::TM_SelfModified {
            combocid_seams::heap_tuple_header_get_cmax::call(tp.t_data())
        } else {
            ::types_core::xact::InvalidCommandId
        };
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        pin.release();
        if have_tuple_lock {
            lmgr::UnlockTuple(
                relation,
                tid,
                tuple_lock_hwlock(LockTupleMode::LockTupleExclusive),
            )?;
        }
        return Ok(result);
    }
    let _ = &mut result;

    predicate_seams::check_for_serializable_conflict_in::call(
        relation,
        Some(tid),
        pin.block_number(),
    )?;

    let (cid, iscombo) = combocid_seams::heap_tuple_header_adjust_cmax::call(tp.t_data(), cid)?;

    let old_key_tuple = extract_replica_identity(relation, &tp, true);

    multixact_seams::multi_xact_id_set_oldest_member::call()?;

    let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        tp.t_data().xmax_raw(),
        tp.t_data().t_infomask,
        tp.t_data().t_infomask2,
        xid,
        LockTupleMode::LockTupleExclusive,
        true,
    )?;

    {
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        page_set_prunable(&mut pm, xid);
        if pm.as_ref().is_all_visible() {
            unported("visibilitymap_clear (heap_delete, visibilitymap.c)");
        }
    }

    let self_tid = tp.t_self;
    let hdr = tp.t_data_mut();
    hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
    hdr.t_infomask |= new_infomask;
    hdr.t_infomask2 |= new_infomask2;
    hdr.clear_hot_updated();
    hdr.set_xmax(new_xmax);
    hdr.set_cmax(cid, iscombo);
    hdr.t_ctid = self_tid;
    if changing_part {
        hdr.set_moved_partitions();
    }

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        debug_assert!(old_key_tuple.is_none());
        let mut flags = 0u8;
        if changing_part {
            flags |= XLH_DELETE_IS_PARTITION_MOVE;
        }
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&new_xmax.to_ne_bytes());
        xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&tp.t_self).to_ne_bytes());
        xlrec[6] = compute_infobits(tp.t_data().t_infomask, tp.t_data().t_infomask2);
        xlrec[7] = flags;

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_DELETE,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(&tp.t_self),
                pin.buffer(),
                REGBUF_STANDARD,
                &[],
            )],
        )?;
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

    if relation.rd_rel.relkind == RELKIND_RELATION || relation.rd_rel.relkind == RELKIND_MATVIEW {
        if tp.has_external() {
            // cold: per-toasted-delete scratch (deform arrays die here)
            let toast_ctx = ::mcx::MemoryContext::new("heap_toast_delete");
            heaptoast_seams::heap_toast_delete::call(toast_ctx.mcx(), relation, &tp, false)?;
        }
    } else {
        debug_assert!(!tp.has_external());
    }

    inval::invalidate::CacheInvalidateHeapTuple(relation, &tp, None)?;

    pin.release();

    if have_tuple_lock {
        lmgr::UnlockTuple(
            relation,
            tid,
            tuple_lock_hwlock(LockTupleMode::LockTupleExclusive),
        )?;
    }

    if relation.pgstat_enabled.get() {
        pgstat::relation::pgstat_count_heap_delete(relation.rd_id, relation.rd_rel.relisshared);
    }
    Ok(TM_Result::TM_Ok)
}

/// `heap_lock_tuple` core (heapam.c). Live: single-locker stamp + the
/// LockWaitBlock wait-then-reread path. LOUD: MultiXact xmax arms, update-chain
/// following (`follow_updates` / EPQ), all-visible VM pin/clear, and the
/// NOWAIT/SKIP-LOCKED wait branches. Returns the pinned (content-unlocked)
/// buffer; the caller stores the locked on-page tuple from it.
#[allow(clippy::too_many_arguments)]
pub fn heap_lock_tuple(
    relation: &RelationData<'_>,
    tid: &ItemPointerData,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    follow_updates: bool,
    tmfd: &mut TM_FailureData,
) -> PgResult<(TM_Result, BufferPin)> {
    use ::types_tuple::{
        HEAP_XMAX_IS_EXCL_LOCKED, HEAP_XMAX_IS_KEYSHR_LOCKED, HEAP_XMAX_IS_SHR_LOCKED,
    };

    let block = ItemPointerGetBlockNumber(tid);
    let offnum = ItemPointerGetOffsetNumber(tid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");

    if pin.page().is_all_visible() {
        unported("visibilitymap_pin write side (heap_lock_tuple all-visible page, visibilitymap.c)");
    }
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let lp = pin.page().item_id(offnum);
    debug_assert!(lp.is_normal());

    let mut have_tuple_lock = false;
    let mut first_time = true;
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };
    macro_rules! relock_tp {
        () => {{
            // SAFETY: pin + exclusive lock held.
            tp = unsafe { page_tuple(pin.page(), pin.page().item_id(offnum), *tid, relation) };
        }};
    }

    let result = 'l3: loop {
        let mut result = hv_seam::heap_tuple_satisfies_update::call(&mut tp, cid, pin.buffer())?;

        if result == TM_Result::TM_Invisible {
            break 'l3 TM_Result::TM_Invisible;
        }

        if matches!(
            result,
            TM_Result::TM_BeingModified | TM_Result::TM_Updated | TM_Result::TM_Deleted
        ) {
            let xwait = tp.t_data().xmax_raw();
            let infomask = tp.t_data().t_infomask;
            let infomask2 = tp.t_data().t_infomask2;
            let t_ctid = tp.t_data().t_ctid;

            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                unported("MultiXact lock arms (heap_lock_tuple, multixact.c)");
            }

            if first_time {
                first_time = false;
                if xact_seams::transaction_id_is_current_transaction_id::call(xwait) {
                    let already = match mode {
                        LockTupleMode::LockTupleKeyShare => true,
                        LockTupleMode::LockTupleShare => {
                            HEAP_XMAX_IS_SHR_LOCKED(infomask) || HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                        }
                        LockTupleMode::LockTupleNoKeyExclusive => HEAP_XMAX_IS_EXCL_LOCKED(infomask),
                        LockTupleMode::LockTupleExclusive => {
                            HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                                && (infomask2 & HEAP_KEYS_UPDATED) != 0
                        }
                    };
                    if already {
                        if have_tuple_lock {
                            lmgr::UnlockTuple(relation, tid, tuple_lock_hwlock(mode))?;
                        }
                        return Ok((TM_Result::TM_Ok, pin));
                    }
                }
            }

            let mut require_sleep = true;
            match mode {
                LockTupleMode::LockTupleKeyShare => {
                    if (infomask2 & HEAP_KEYS_UPDATED) == 0 {
                        let updated = !HEAP_XMAX_IS_LOCKED_ONLY(infomask);
                        if follow_updates && updated && tp.t_self != t_ctid {
                            let res = heap_lock_updated_tuple(
                                relation,
                                infomask,
                                xwait,
                                &t_ctid,
                                xact_seams::get_current_transaction_id::call()?,
                                mode,
                            )?;
                            if res != TM_Result::TM_Ok {
                                // C's goto failed expects the buffer lock held.
                                bufmgr_seams::lock_buffer::call(
                                    pin.buffer(),
                                    BUFFER_LOCK_EXCLUSIVE,
                                )?;
                                relock_tp!();
                                break 'l3 res;
                            }
                        }
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        relock_tp!();
                        if !hv_seam::heap_tuple_header_is_only_locked::call(tp.t_data())?
                            && (((tp.t_data().t_infomask2 & HEAP_KEYS_UPDATED) != 0) || !updated)
                        {
                            continue 'l3;
                        }
                        require_sleep = false;
                    }
                }
                LockTupleMode::LockTupleShare => {
                    if HEAP_XMAX_IS_LOCKED_ONLY(infomask) && !HEAP_XMAX_IS_EXCL_LOCKED(infomask) {
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        relock_tp!();
                        if !HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data().t_infomask)
                            || HEAP_XMAX_IS_EXCL_LOCKED(tp.t_data().t_infomask)
                        {
                            continue 'l3;
                        }
                        require_sleep = false;
                    }
                }
                LockTupleMode::LockTupleNoKeyExclusive => {
                    if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) {
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        relock_tp!();
                        if xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                            || tp.t_data().xmax_raw() != xwait
                        {
                            continue 'l3;
                        }
                        require_sleep = false;
                    }
                }
                LockTupleMode::LockTupleExclusive => {}
            }

            if require_sleep
                && (infomask & HEAP_XMAX_IS_MULTI) == 0
                && xact_seams::transaction_id_is_current_transaction_id::call(xwait)
            {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                relock_tp!();
                if xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                    || tp.t_data().xmax_raw() != xwait
                {
                    continue 'l3;
                }
                debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data().t_infomask));
                require_sleep = false;
            }

            if require_sleep
                && (result == TM_Result::TM_Updated || result == TM_Result::TM_Deleted)
            {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                relock_tp!();
                break 'l3 result;
            } else if require_sleep {
                if !heap_acquire_tuplock(relation, tid, mode, wait_policy, &mut have_tuple_lock)? {
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                    relock_tp!();
                    break 'l3 TM_Result::TM_WouldBlock;
                }
                match wait_policy {
                    LockWaitPolicy::LockWaitBlock => {
                        lmgr::XactLockTableWait(
                            xwait,
                            Some(relation),
                            Some(&tp.t_self),
                            ::types_storage::lock::XLTW_Oper::Lock,
                        )?;
                    }
                    LockWaitPolicy::LockWaitSkip | LockWaitPolicy::LockWaitError => {
                        unported("heap_lock_tuple NOWAIT/SKIP LOCKED wait branch")
                    }
                }
                if follow_updates && !HEAP_XMAX_IS_LOCKED_ONLY(infomask) && tp.t_self != t_ctid {
                    let res = heap_lock_updated_tuple(
                        relation,
                        infomask,
                        xwait,
                        &t_ctid,
                        xact_seams::get_current_transaction_id::call()?,
                        mode,
                    )?;
                    if res != TM_Result::TM_Ok {
                        // C's goto failed expects the buffer lock held.
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        relock_tp!();
                        break 'l3 res;
                    }
                }
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                relock_tp!();
                if xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                    || tp.t_data().xmax_raw() != xwait
                {
                    continue 'l3;
                }
                update_xmax_hint_bits(&mut tp, pin.buffer(), xwait)?;
            }

            result = if !require_sleep
                || (tp.t_data().t_infomask & HEAP_XMAX_INVALID) != 0
                || HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data().t_infomask)
                || hv_seam::heap_tuple_header_is_only_locked::call(tp.t_data())?
            {
                TM_Result::TM_Ok
            } else if tp.t_self != tp.t_data().t_ctid {
                TM_Result::TM_Updated
            } else {
                TM_Result::TM_Deleted
            };
        }
        break 'l3 result;
    };

    if result != TM_Result::TM_Ok {
        tmfd.ctid = tp.t_data().t_ctid;
        tmfd.xmax = HeapTupleHeaderGetUpdateXid(tp.t_data())?;
        tmfd.cmax = if result == TM_Result::TM_SelfModified {
            combocid_seams::heap_tuple_header_get_cmax::call(tp.t_data())
        } else {
            ::types_core::xact::InvalidCommandId
        };
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        if have_tuple_lock {
            lmgr::UnlockTuple(relation, tid, tuple_lock_hwlock(mode))?;
        }
        return Ok((result, pin));
    }

    if pin.page().is_all_visible() {
        unported("visibilitymap_pin re-lock window (heap_lock_tuple, visibilitymap.c)");
    }

    let xmax = tp.t_data().xmax_raw();
    let old_infomask = tp.t_data().t_infomask;
    let old_infomask2 = tp.t_data().t_infomask2;

    multixact_seams::multi_xact_id_set_oldest_member::call()?;

    let (xid, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        xmax,
        old_infomask,
        old_infomask2,
        xact_seams::get_current_transaction_id::call()?,
        mode,
        false,
    )?;

    {
        let hdr = tp.t_data_mut();
        hdr.t_infomask &= !HEAP_XMAX_BITS;
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        hdr.t_infomask |= new_infomask;
        hdr.t_infomask2 |= new_infomask2;
        if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
            hdr.clear_hot_updated();
        }
        hdr.set_xmax(xid);
        if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
            hdr.t_ctid = *tid;
        }
    }

    if pin.page().is_all_visible() {
        unported("visibilitymap_clear ALL_FROZEN (heap_lock_tuple, visibilitymap.c)");
    }

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&xid.to_ne_bytes());
        xlrec[4..6].copy_from_slice(&offnum.to_ne_bytes());
        xlrec[6] = compute_infobits(new_infomask, tp.t_data().t_infomask2);
        xlrec[7] = 0;

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_LOCK,
            0,
            &[&xlrec],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(&tp.t_self),
                pin.buffer(),
                REGBUF_STANDARD,
                &[],
            )],
        )?;
        // SAFETY: pin + exclusive lock held.
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    if have_tuple_lock {
        lmgr::UnlockTuple(relation, tid, tuple_lock_hwlock(mode))?;
    }
    Ok((TM_Result::TM_Ok, pin))
}

/// `simple_heap_delete`.
pub fn simple_heap_delete(relation: &RelationData<'_>, tid: &ItemPointerData) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let result = heap_delete(
        relation,
        tid,
        xact_seams::get_current_command_id::call(true)?,
        None,
        true,
        &mut tmfd,
        false,
    )?;
    match result {
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_SelfModified => Err(Box::new(PgError::error("tuple already updated by self"))),
        TM_Result::TM_Updated => Err(Box::new(PgError::error("tuple concurrently updated"))),
        TM_Result::TM_Deleted => Err(Box::new(PgError::error("tuple concurrently deleted"))),
        _ => Err(Box::new(PgError::error(std::format!(
            "unexpected heap_delete status: {result:?}"
        )))),
    }
}

/// `heap_finish_speculative`: replace the speculative token in t_ctid with a
/// real self-pointing ctid.
pub fn heap_finish_speculative(
    relation: &RelationData<'_>,
    tid: &ItemPointerData,
) -> PgResult<()> {
    let block = ItemPointerGetBlockNumber(tid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let offnum = ItemPointerGetOffsetNumber(tid);
    let page = pin.page();
    if page.max_offset_number() < offnum || !page.item_id(offnum).is_normal() {
        return Err(Box::new(PgError::error("invalid lp")));
    }
    let lp = page.item_id(offnum);
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };
    debug_assert!(tp.t_data().is_speculative());

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
    tp.t_data_mut().t_ctid = *tid;

    if relation_needs_wal(relation) {
        let xlrec = offnum.to_ne_bytes();
        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_CONFIRM,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(tid),
                pin.buffer(),
                REGBUF_STANDARD,
                &[],
            )],
        )?;
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    pin.release();
    Ok(())
}

/// `heap_abort_speculative`: super-delete — xmin goes invalid so the tuple is
/// immediately dead to everyone, including our own transaction.
pub fn heap_abort_speculative(
    relation: &RelationData<'_>,
    tid: &ItemPointerData,
) -> PgResult<()> {
    let xid = xact_seams::get_current_transaction_id::call()?;

    let block = ItemPointerGetBlockNumber(tid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
    debug_assert!(!pin.page().is_all_visible());

    let lp = pin.page().item_id(ItemPointerGetOffsetNumber(tid));
    debug_assert!(lp.is_normal());
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };

    if tp.t_data().xmin_raw() != xid {
        return Err(Box::new(PgError::error(
            "attempted to kill a tuple inserted by another transaction",
        )));
    }
    if !tp.t_data().is_speculative() {
        return Err(Box::new(PgError::error("attempted to kill a non-speculative tuple")));
    }
    debug_assert!(!tp.t_data().is_heap_only());

    {
        // The tuple is DEAD immediately; the oldest cheap wraparound-safe
        // prune hint is TransactionXmin, clamped to relfrozenxid.
        let txmin = snapmgr_seams::transaction_xmin::call();
        debug_assert!(TransactionIdIsValid(txmin));
        let prune_xid = if ::types_core::xact::TransactionIdPrecedes(
            txmin,
            relation.rd_rel.relfrozenxid,
        ) {
            relation.rd_rel.relfrozenxid
        } else {
            txmin
        };
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        page_set_prunable(&mut pm, prune_xid);
    }

    let self_tid = tp.t_self;
    let hdr = tp.t_data_mut();
    hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
    hdr.set_xmin(InvalidTransactionId);
    hdr.t_ctid = self_tid;

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&xid.to_ne_bytes());
        xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&tp.t_self).to_ne_bytes());
        xlrec[6] = compute_infobits(tp.t_data().t_infomask, tp.t_data().t_infomask2);
        xlrec[7] = XLH_DELETE_IS_SUPER;

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_DELETE,
            0,
            &[&xlrec],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(&tp.t_self),
                pin.buffer(),
                REGBUF_STANDARD,
                &[],
            )],
        )?;
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

    if tp.has_external() {
        // cold: per-toasted-delete scratch (deform arrays die here)
        let toast_ctx = ::mcx::MemoryContext::new("heap_toast_delete");
        heaptoast_seams::heap_toast_delete::call(toast_ctx.mcx(), relation, &tp, true)?;
    }

    inval::invalidate::CacheInvalidateHeapTuple(relation, &tp, None)?;

    if relation.pgstat_enabled.get() {
        pgstat::relation::pgstat_count_heap_delete(relation.rd_id, relation.rd_rel.relisshared);
    }

    pin.release();
    Ok(())
}

// PageClearAllVisible + visibilitymap_clear, pin-at-clear (heap_insert shape).
fn clear_page_all_visible(relation: &RelationData<'_>, pin: &BufferPin) -> PgResult<()> {
    let mut vmb = visibilitymap::VmBuffer::new();
    visibilitymap::visibilitymap_pin(relation, pin.block_number(), &mut vmb)?;
    // SAFETY: pinned + exclusive content lock held by the caller.
    let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    pm.clear_all_visible();
    visibilitymap::visibilitymap_clear(
        relation,
        pin.block_number(),
        &vmb,
        visibilitymap::VISIBILITYMAP_VALID_BITS,
    )?;
    vmb.release();
    Ok(())
}

fn log_heap_update(
    relation: &RelationData<'_>,
    oldbuf: &BufferPin,
    newbuf: &BufferPin,
    oldtup: &HeapTupleData<'_>,
    newtup: &HeapTupleData<'_>,
    all_visible_cleared: bool,
    new_all_visible_cleared: bool,
) -> PgResult<::types_core::XLogRecPtr> {
    debug_assert!(relation_needs_wal(relation));

    let mut info = if newtup.t_data().is_heap_only() {
        XLOG_HEAP_HOT_UPDATE
    } else {
        XLOG_HEAP_UPDATE
    };

    // Prefix/suffix WAL compression needs XLogCheckBufferNeedsBackup; off
    // until xloginsert lands (records stay redo-compatible, just larger).
    let mut flags = 0u8;
    if all_visible_cleared {
        flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    }
    if new_all_visible_cleared {
        flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    }

    let new_page = newbuf.page();
    let init = ItemPointerGetOffsetNumber(&newtup.t_self) == FirstOffsetNumber
        && new_page.max_offset_number() == FirstOffsetNumber;

    let mut xlrec = [0u8; 14];
    xlrec[0..4].copy_from_slice(&oldtup.t_data().xmax_raw().to_ne_bytes());
    xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&oldtup.t_self).to_ne_bytes());
    xlrec[6] = compute_infobits(oldtup.t_data().t_infomask, oldtup.t_data().t_infomask2);
    xlrec[7] = flags;
    xlrec[8..12].copy_from_slice(&newtup.t_data().xmax_raw().to_ne_bytes());
    xlrec[12..14].copy_from_slice(&ItemPointerGetOffsetNumber(&newtup.t_self).to_ne_bytes());

    let mut bufflags = REGBUF_STANDARD;
    if init {
        info |= XLOG_HEAP_INIT_PAGE;
        bufflags |= REGBUF_WILL_INIT;
    }
    if relation_is_logically_logged(relation) {
        bufflags |= REGBUF_KEEP_DATA;
    }

    let xlhdr = xl_heap_header(newtup.t_data());
    // SAFETY: tuple image is t_len readable bytes.
    let body = unsafe {
        core::slice::from_raw_parts(
            newtup.header_ptr().add(SizeofHeapTupleHeader),
            newtup.t_len as usize - SizeofHeapTupleHeader,
        )
    };

    let rloc = relation.rd_locator.get();
    let same_buf = oldbuf.buffer() == newbuf.buffer();
    let new_bufdata: [&[u8]; 2] = [&xlhdr, body];
    let new_reg = crate::wal::reg_block(
        0,
        rloc,
        ItemPointerGetBlockNumber(&newtup.t_self),
        newbuf.buffer(),
        bufflags,
        &new_bufdata,
    );
    if same_buf {
        crate::wal::insert_record(RM_HEAP_ID, info, XLOG_INCLUDE_ORIGIN, &[&xlrec], &[new_reg])
    } else {
        crate::wal::insert_record(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[
                new_reg,
                crate::wal::reg_block(
                    1,
                    rloc,
                    ItemPointerGetBlockNumber(&oldtup.t_self),
                    oldbuf.buffer(),
                    REGBUF_STANDARD,
                    &[],
                ),
            ],
        )
    }
}

/// `heap_update` core. Index-attr bitmaps pend relcache
/// (`RelationGetIndexAttrBitmap`): updates on `relhasindex` relations panic;
/// indexless relations take the C empty-bitmap path (HOT when same-page).
pub fn heap_update(
    relation: &RelationData<'_>,
    otid: &ItemPointerData,
    newtup: &mut HeapTupleData<'_>,
    cid: CommandId,
    crosscheck: Option<&SnapshotData<'_>>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    let xid = xact_seams::get_current_transaction_id::call()?;

    if xact_seams::is_in_parallel_mode::call() {
        return Err(Box::new(
            PgError::error("cannot update tuples during a parallel operation")
                .with_sqlstate(::types_error::ERRCODE_INVALID_TRANSACTION_STATE),
        ));
    }

    // Indexless relations take the C empty-bitmap path (HOT when same-page).
    let attr_bitmaps = if relation.rd_rel.relhasindex {
        Some(relcache_seams::relation_get_index_attr_bitmap::call(relation.rd_id)?)
    } else {
        None
    };

    let block = ItemPointerGetBlockNumber(otid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");
    // C pins the VM page here; pin-at-clear is the heap_insert divergence.
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let lp = pin.page().item_id(ItemPointerGetOffsetNumber(otid));
    if !lp.is_normal() {
        // concurrent pruning is only reachable for syscache-origin otids
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        pin.release();
        tmfd.ctid = *otid;
        tmfd.xmax = InvalidTransactionId;
        tmfd.cmax = ::types_core::xact::InvalidCommandId;
        *update_indexes = TU_UpdateIndexes::TU_None;
        return Ok(TM_Result::TM_Deleted);
    }

    // SAFETY: pin + exclusive lock held.
    let mut oldtup = unsafe { page_tuple(pin.page(), lp, *otid, relation) };
    newtup.t_tableOid = relation.rd_id;

    // HeapDetermineColumnsInfo over the four attr sets (empty when indexless).
    let (hot_modified, sum_modified, key_modified, id_modified) = match &attr_bitmaps {
        Some(bm) => (
            any_attr_modified(relation, &oldtup, newtup, &bm.hot_blocking),
            any_attr_modified(relation, &oldtup, newtup, &bm.summarized),
            any_attr_modified(relation, &oldtup, newtup, &bm.key),
            any_attr_modified(relation, &oldtup, newtup, &bm.identity),
        ),
        None => (false, false, false, false),
    };
    let key_intact = !key_modified;
    *lockmode = if key_intact {
        LockTupleMode::LockTupleNoKeyExclusive
    } else {
        LockTupleMode::LockTupleExclusive
    };
    multixact_seams::multi_xact_id_set_oldest_member::call()?;

    let mut have_tuple_lock = false;
    let mut checked_lockers;
    let mut locker_remains;

    let mut result = 'l2: loop {
        checked_lockers = false;
        locker_remains = false;
        let mut result = hv_seam::heap_tuple_satisfies_update::call(&mut oldtup, cid, pin.buffer())?;
        debug_assert!(result != TM_Result::TM_BeingModified || wait);

        if result == TM_Result::TM_Invisible {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            pin.release();
            return Err(invisible_tuple("update"));
        } else if result == TM_Result::TM_BeingModified && wait {
            let xwait = oldtup.t_data().xmax_raw();
            let infomask = oldtup.t_data().t_infomask;
            let mut can_continue = false;

            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                unported("DoesMultiXactIdConflict/MultiXactIdWait (heap_update, multixact.c)");
            } else if xact_seams::transaction_id_is_current_transaction_id::call(xwait) {
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else if ::types_tuple::HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && key_intact {
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                heap_acquire_tuplock(
                    relation,
                    &oldtup.t_self,
                    *lockmode,
                    LockWaitPolicy::LockWaitBlock,
                    &mut have_tuple_lock,
                )?;
                lmgr::XactLockTableWait(
                    xwait,
                    Some(relation),
                    Some(&oldtup.t_self),
                    ::types_storage::lock::XLTW_Oper::Update,
                )?;
                checked_lockers = true;
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

                if xmax_infomask_changed(oldtup.t_data().t_infomask, infomask)
                    || xwait != oldtup.t_data().xmax_raw()
                {
                    continue 'l2;
                }
                update_xmax_hint_bits(&mut oldtup, pin.buffer(), xwait)?;
                if (oldtup.t_data().t_infomask & HEAP_XMAX_INVALID) != 0 {
                    can_continue = true;
                }
            }

            result = if can_continue {
                TM_Result::TM_Ok
            } else if oldtup.t_self != oldtup.t_data().t_ctid {
                TM_Result::TM_Updated
            } else {
                TM_Result::TM_Deleted
            };
        }

        if let (Some(snap), TM_Result::TM_Ok) = (crosscheck, result) {
            if !hv_seam::heap_tuple_satisfies_visibility::call(&mut oldtup, snap, pin.buffer())? {
                result = TM_Result::TM_Updated;
            }
        }

        break 'l2 result;
    };

    if result != TM_Result::TM_Ok {
        tmfd.ctid = oldtup.t_data().t_ctid;
        tmfd.xmax = HeapTupleHeaderGetUpdateXid(oldtup.t_data())?;
        tmfd.cmax = if result == TM_Result::TM_SelfModified {
            combocid_seams::heap_tuple_header_get_cmax::call(oldtup.t_data())
        } else {
            ::types_core::xact::InvalidCommandId
        };
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        pin.release();
        if have_tuple_lock {
            lmgr::UnlockTuple(relation, &oldtup.t_self, tuple_lock_hwlock(*lockmode))?;
        }
        *update_indexes = TU_UpdateIndexes::TU_None;
        return Ok(result);
    }
    let _ = &mut result;

    let (xmax_old_tuple, infomask_old_tuple, infomask2_old_tuple) = compute_new_xmax_infomask(
        oldtup.t_data().xmax_raw(),
        oldtup.t_data().t_infomask,
        oldtup.t_data().t_infomask2,
        xid,
        *lockmode,
        true,
    )?;

    let xmax_new_tuple = if (oldtup.t_data().t_infomask & HEAP_XMAX_INVALID) != 0
        || HEAP_LOCKED_UPGRADED(oldtup.t_data().t_infomask)
        || (checked_lockers && !locker_remains)
    {
        InvalidTransactionId
    } else {
        oldtup.t_data().xmax_raw()
    };

    let (infomask_new_tuple, infomask2_new_tuple) = if !TransactionIdIsValid(xmax_new_tuple) {
        (HEAP_XMAX_INVALID, 0u16)
    } else if (oldtup.t_data().t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        unported("GetMultiXactIdHintBits (heap_update, multixact.c)");
    } else {
        (HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY, 0u16)
    };

    {
        let hdr = newtup.t_data_mut();
        hdr.t_infomask &= !HEAP_XACT_MASK;
        hdr.t_infomask2 &= !HEAP2_XACT_MASK;
        hdr.set_xmin(xid);
        hdr.set_cmin(cid);
        hdr.t_infomask |= HEAP_UPDATED | infomask_new_tuple;
        hdr.t_infomask2 |= infomask2_new_tuple;
        hdr.set_xmax(xmax_new_tuple);
    }

    let (cid, iscombo) =
        combocid_seams::heap_tuple_header_adjust_cmax::call(oldtup.t_data(), cid)?;

    let need_toast = if relation.rd_rel.relkind != RELKIND_RELATION
        && relation.rd_rel.relkind != RELKIND_MATVIEW
    {
        debug_assert!(!oldtup.has_external());
        debug_assert!(!newtup.has_external());
        false
    } else {
        oldtup.has_external()
            || newtup.has_external()
            || newtup.t_len as usize > TOAST_TUPLE_THRESHOLD
    };

    let pagefree = pin.page().heap_free_space();
    let mut newtupsize = (newtup.t_len as usize + 7) & !7;

    let toast_ctx;
    let mut toasted = None;
    let newpin: Option<BufferPin>;
    if need_toast || newtupsize > pagefree {
        // xl_heap_lock the old tuple while off the page lock (C contract)
        let (xmax_lock_old_tuple, infomask_lock_old_tuple, infomask2_lock_old_tuple) =
            compute_new_xmax_infomask(
                oldtup.t_data().xmax_raw(),
                oldtup.t_data().t_infomask,
                oldtup.t_data().t_infomask2,
                xid,
                *lockmode,
                false,
            )?;
        debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(infomask_lock_old_tuple));

        {
            let self_tid = oldtup.t_self;
            let hdr = oldtup.t_data_mut();
            hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
            hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
            hdr.clear_hot_updated();
            debug_assert!(TransactionIdIsValid(xmax_lock_old_tuple));
            hdr.set_xmax(xmax_lock_old_tuple);
            hdr.t_infomask |= infomask_lock_old_tuple;
            hdr.t_infomask2 |= infomask2_lock_old_tuple;
            hdr.set_cmax(cid, iscombo);
            hdr.t_ctid = self_tid;
        }

        // ALL_VISIBLE stays (WAL cost identical either way, per C); only the
        // frozen bit lies once the locker's xmax lands. Pin-at-clear
        // (clear_page_all_visible shape).
        let mut cleared_all_frozen = false;
        if pin.page().is_all_visible() {
            let mut vmb = visibilitymap::VmBuffer::new();
            visibilitymap::visibilitymap_pin(relation, pin.block_number(), &mut vmb)?;
            cleared_all_frozen = visibilitymap::visibilitymap_clear(
                relation,
                pin.block_number(),
                &vmb,
                visibilitymap::VISIBILITYMAP_ALL_FROZEN,
            )?;
            vmb.release();
        }

        bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

        if relation_needs_wal(relation) {
            let mut xlrec = [0u8; 8];
            xlrec[0..4].copy_from_slice(&xmax_lock_old_tuple.to_ne_bytes());
            xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&oldtup.t_self).to_ne_bytes());
            xlrec[6] = compute_infobits(oldtup.t_data().t_infomask, oldtup.t_data().t_infomask2);
            xlrec[7] = if cleared_all_frozen {
                XLH_LOCK_ALL_FROZEN_CLEARED
            } else {
                0
            };
            let recptr = crate::wal::insert_record(
                RM_HEAP_ID,
                XLOG_HEAP_LOCK,
                0,
                &[&xlrec],
                &[crate::wal::reg_block(
                    0,
                    relation.rd_locator.get(),
                    ItemPointerGetBlockNumber(&oldtup.t_self),
                    pin.buffer(),
                    REGBUF_STANDARD,
                    &[],
                )],
            )?;
            // SAFETY: pin + exclusive lock held.
            let mut pm =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            pm.set_lsn(recptr);
        }

        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

        let ht_len = if need_toast {
            // cold: scratch context per oversized-value update (C's palloc'd
            // toasted copy dies at heap_freetuple)
            toast_ctx = ::mcx::MemoryContext::new("heap_toast_insert_or_update");
            toasted = heaptoast_seams::heap_toast_insert_or_update::call(
                toast_ctx.mcx(),
                relation,
                newtup,
                Some(&oldtup),
                0,
            )?;
            toasted.as_ref().map_or(newtup.t_len, |t| t.as_tuple().t_len)
        } else {
            newtup.t_len
        };
        newtupsize = (ht_len as usize + 7) & !7;

        // C re-checks free space in a loop; single-backend recheck reduces to
        // one pass (no concurrent inserters can consume the page meanwhile).
        if newtupsize > pagefree {
            newpin = Some(RelationGetBufferForTuple(
                relation,
                ht_len as usize,
                Some(&pin),
                0,
                None,
                0,
            )?);
        } else {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            newpin = None;
        }
    } else {
        newpin = None;
    }
    let _ = newtupsize;
    let mut erased;
    let heaptup: &mut HeapTupleData<'_> = match toasted.as_mut() {
        Some(t) => {
            let ht = t.as_tuple_mut();
            // SAFETY: image owned by toast_ctx, which outlives every use in
            // this function (lifetime-erased view, page_tuple model).
            erased = unsafe {
                HeapTupleData::from_raw_parts(
                    ht.header_ptr().cast_mut(),
                    ht.t_len,
                    ht.t_self,
                    ht.t_tableOid,
                )
            };
            &mut erased
        }
        None => newtup,
    };

    predicate_seams::check_for_serializable_conflict_in::call(
        relation,
        Some(&oldtup.t_self),
        pin.block_number(),
    )?;

    let same_page = newpin.is_none();
    let mut use_hot_update = false;
    if same_page {
        use_hot_update = !hot_modified;
    }
    if !same_page {
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_full();
    }

    let old_key_tuple = extract_replica_identity(relation, &oldtup, id_modified);
    debug_assert!(old_key_tuple.is_none());

    {
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        page_set_prunable(&mut pm, xid);
    }

    if use_hot_update {
        oldtup.t_data_mut().set_hot_updated();
        heaptup.t_data_mut().set_heap_only();
    } else {
        oldtup.t_data_mut().clear_hot_updated();
        heaptup.t_data_mut().clear_heap_only();
    }

    let put_pin = newpin.as_ref().unwrap_or(&pin);
    RelationPutHeapTuple(relation, put_pin, heaptup, false)?;

    {
        let new_tid = heaptup.t_self;
        let hdr = oldtup.t_data_mut();
        hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        debug_assert!(TransactionIdIsValid(xmax_old_tuple));
        hdr.set_xmax(xmax_old_tuple);
        hdr.t_infomask |= infomask_old_tuple;
        hdr.t_infomask2 |= infomask2_old_tuple;
        hdr.set_cmax(cid, iscombo);
        hdr.t_ctid = new_tid;
    }

    let mut all_visible_cleared = false;
    let mut all_visible_cleared_new = false;
    if pin.page().is_all_visible() {
        all_visible_cleared = true;
        clear_page_all_visible(relation, &pin)?;
    }
    if let Some(np) = &newpin {
        if np.page().is_all_visible() {
            all_visible_cleared_new = true;
            clear_page_all_visible(relation, np)?;
        }
        bufmgr_seams::mark_buffer_dirty::call(np.buffer())?;
    }
    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        let recptr = log_heap_update(
            relation,
            &pin,
            put_pin,
            &oldtup,
            heaptup,
            all_visible_cleared,
            all_visible_cleared_new,
        )?;
        if let Some(np) = &newpin {
            // SAFETY: pin + exclusive lock held.
            let mut pm =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(np.buffer())) };
            pm.set_lsn(recptr);
        }
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    if let Some(np) = &newpin {
        bufmgr_seams::lock_buffer::call(np.buffer(), BUFFER_LOCK_UNLOCK)?;
    }
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

    inval::invalidate::CacheInvalidateHeapTuple(relation, &oldtup, Some(heaptup))?;

    let new_page_stat = newpin.is_some();
    if let Some(np) = newpin {
        np.release();
    }
    pin.release();

    if have_tuple_lock {
        lmgr::UnlockTuple(relation, &oldtup.t_self, tuple_lock_hwlock(*lockmode))?;
    }

    if relation.pgstat_enabled.get() {
        pgstat::relation::pgstat_count_heap_update(
            relation.rd_id,
            relation.rd_rel.relisshared,
            use_hot_update,
            new_page_stat,
        );
    }

    *update_indexes = if use_hot_update {
        if sum_modified {
            TU_UpdateIndexes::TU_Summarizing
        } else {
            TU_UpdateIndexes::TU_None
        }
    } else {
        TU_UpdateIndexes::TU_All
    };

    let heaptup_self = heaptup.t_self;
    if toasted.is_some() {
        newtup.t_self = heaptup_self;
        if use_hot_update {
            newtup.t_data_mut().set_heap_only();
        } else {
            newtup.t_data_mut().clear_heap_only();
        }
    }
    Ok(TM_Result::TM_Ok)
}

/// `simple_heap_update`.
pub fn simple_heap_update(
    relation: &RelationData<'_>,
    otid: &ItemPointerData,
    tup: &mut HeapTupleData<'_>,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let result = heap_update(
        relation,
        otid,
        tup,
        xact_seams::get_current_command_id::call(true)?,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    )?;
    match result {
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_SelfModified => Err(Box::new(PgError::error("tuple already updated by self"))),
        TM_Result::TM_Updated => Err(Box::new(PgError::error("tuple concurrently updated"))),
        TM_Result::TM_Deleted => Err(Box::new(PgError::error("tuple concurrently deleted"))),
        _ => Err(Box::new(PgError::error(std::format!(
            "unexpected heap_update status: {result:?}"
        )))),
    }
}

// HeapDetermineColumnsInfo + heap_attr_equals (heapam.c), reduced to a
// per-set any-modified probe; datumIsEqual's toasted false-negatives only
// cost HOT, as in C.
fn any_attr_modified(
    relation: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    newtup: &HeapTupleData<'_>,
    attnums: &[i16],
) -> bool {
    let td = &relation.rd_att;
    for &attnum in attnums {
        debug_assert!(attnum > 0);
        let mut isnull1 = false;
        let mut isnull2 = false;
        // SAFETY: both tuples were formed/read under this relation's
        // descriptor; attnum comes off its own index definitions.
        let (v1, v2) = unsafe {
            (
                ::types_tuple::heap_getattr(oldtup, attnum as i32, td, &mut isnull1),
                ::types_tuple::heap_getattr(newtup, attnum as i32, td, &mut isnull2),
            )
        };
        if isnull1 != isnull2 {
            return true;
        }
        if isnull1 {
            continue;
        }
        let att = td.attr(attnum as usize - 1);
        if !datum_is_equal(v1, v2, att.attbyval, att.attlen as i32) {
            return true;
        }
    }
    false
}

// datumIsEqual (datum.c).
fn datum_is_equal(v1: ::datum::Datum, v2: ::datum::Datum, typbyval: bool, typlen: i32) -> bool {
    if typbyval {
        return v1 == v2;
    }
    let (p1, p2) = (v1.as_usize() as *const u8, v2.as_usize() as *const u8);
    let size = match typlen {
        l if l > 0 => l as usize,
        -1 => {
            // SAFETY: byref varlena datums off live tuples.
            let (s1, s2) = unsafe {
                (::types_tuple::varatt::varsize_any(p1), ::types_tuple::varatt::varsize_any(p2))
            };
            if s1 != s2 {
                return false;
            }
            s1
        }
        other => unported_ret(other),
    };
    // SAFETY: both images readable for `size` per their headers/typlen.
    unsafe { core::slice::from_raw_parts(p1, size) == core::slice::from_raw_parts(p2, size) }
}

#[cold]
#[inline(never)]
fn unported_ret(typlen: i32) -> ! {
    panic!("backend-access-heap-heapam reached unported unit: datumIsEqual cstring typlen {typlen} (datum.c)")
}

/// `heap_lock_updated_tuple` (heapam.c): lock all descendant versions of an
/// updated tuple so the acquired mode survives the chain. LOUD in the rec:
/// MultiXact member scans and all-visible VM maintenance.
fn heap_lock_updated_tuple(
    relation: &RelationData<'_>,
    prior_infomask: u16,
    prior_raw_xmax: TransactionId,
    prior_ctid: &ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> PgResult<TM_Result> {
    if ::types_tuple::ItemPointerIndicatesMovedPartitions(prior_ctid) {
        return Ok(TM_Result::TM_Ok);
    }
    multixact_seams::multi_xact_id_set_oldest_member::call()?;
    let prior_xmax = if (prior_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        MultiXactIdGetUpdateXid(prior_raw_xmax, prior_infomask)?
    } else {
        prior_raw_xmax
    };
    heap_lock_updated_tuple_rec(relation, prior_xmax, *prior_ctid, xid, mode)
}

fn heap_lock_updated_tuple_rec(
    relation: &RelationData<'_>,
    prior_xmax: TransactionId,
    tid: ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> PgResult<TM_Result> {
    use ::types_tuple::{
        HEAP_XMAX_IS_EXCL_LOCKED, HEAP_XMAX_IS_KEYSHR_LOCKED, HEAP_XMAX_IS_SHR_LOCKED,
    };

    let mut prior_xmax = prior_xmax;
    let mut tupid = tid;
    'chain: loop {
        let block = ItemPointerGetBlockNumber(&tupid);
        let offnum = ItemPointerGetOffsetNumber(&tupid);
        let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
            .expect("ReadBuffer returned InvalidBuffer");

        'l4: loop {
            if pin.page().is_all_visible() {
                unported("visibilitymap_pin (heap_lock_updated_tuple_rec, visibilitymap.c)");
            }
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            macro_rules! done {
                ($res:expr) => {{
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                    pin.release();
                    return Ok($res);
                }};
            }

            // heap_fetch(SnapshotAny) miss: the chain member was pruned after
            // its creator aborted — chain end.
            let page = pin.page();
            if offnum < FirstOffsetNumber || offnum > page.max_offset_number() {
                done!(TM_Result::TM_Ok);
            }
            let lp = page.item_id(offnum);
            if !lp.is_normal() {
                done!(TM_Result::TM_Ok);
            }
            // SAFETY: pin + exclusive lock held.
            let mut mytup = unsafe { page_tuple(page, lp, tupid, relation) };

            if TransactionIdIsValid(prior_xmax) && mytup.t_data().xmin() != prior_xmax {
                done!(TM_Result::TM_Ok);
            }
            if transam_seams::transaction_id_did_abort::call(mytup.t_data().xmin())? {
                done!(TM_Result::TM_Ok);
            }

            let old_infomask = mytup.t_data().t_infomask;
            let old_infomask2 = mytup.t_data().t_infomask2;
            let xmax = mytup.t_data().xmax_raw();
            let mut stamp = true;

            if (old_infomask & HEAP_XMAX_INVALID) == 0 {
                if (old_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    unported("GetMultiXactIdMembers (heap_lock_updated_tuple_rec, multixact.c)");
                }
                let held_locked_only = HEAP_XMAX_IS_LOCKED_ONLY(old_infomask);
                let held_mode = if held_locked_only {
                    if HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                        LockTupleMode::LockTupleKeyShare
                    } else if HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                        LockTupleMode::LockTupleShare
                    } else if HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                        if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                            LockTupleMode::LockTupleExclusive
                        } else {
                            LockTupleMode::LockTupleNoKeyExclusive
                        }
                    } else {
                        return Err(Box::new(PgError::error("invalid lock status in tuple")));
                    }
                } else if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                    LockTupleMode::LockTupleExclusive
                } else {
                    LockTupleMode::LockTupleNoKeyExclusive
                };
                // test_lockmode_for_conflict (heapam.c), single-xact form.
                if xact_seams::transaction_id_is_current_transaction_id::call(xmax) {
                    stamp = false;
                } else if procarray_seams::transaction_id_is_in_progress::call(xmax)? {
                    if ::lock_seams::do_lock_modes_conflict::call(
                        tuple_lock_hwlock(held_mode),
                        tuple_lock_hwlock(mode),
                    ) {
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                        lmgr::XactLockTableWait(
                            xmax,
                            Some(relation),
                            Some(&mytup.t_self),
                            ::types_storage::lock::XLTW_Oper::LockUpdated,
                        )?;
                        continue 'l4;
                    }
                } else if transam_seams::transaction_id_did_abort::call(xmax)? {
                    // lock gone with the aborted xact
                } else if transam_seams::transaction_id_did_commit::call(xmax)? {
                    if !held_locked_only
                        && ::lock_seams::do_lock_modes_conflict::call(
                            tuple_lock_hwlock(held_mode),
                            tuple_lock_hwlock(mode),
                        )
                    {
                        if mytup.t_self != mytup.t_data().t_ctid {
                            done!(TM_Result::TM_Updated);
                        }
                        done!(TM_Result::TM_Deleted);
                    }
                }
                // else: crashed — locks are gone
            }

            if stamp {
                let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
                    xmax,
                    old_infomask,
                    mytup.t_data().t_infomask2,
                    xid,
                    mode,
                    false,
                )?;
                if pin.page().is_all_visible() {
                    unported(
                        "visibilitymap_clear ALL_FROZEN (heap_lock_updated_tuple_rec, \
                         visibilitymap.c)",
                    );
                }
                {
                    let hdr = mytup.t_data_mut();
                    hdr.set_xmax(new_xmax);
                    hdr.t_infomask &= !HEAP_XMAX_BITS;
                    hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
                    hdr.t_infomask |= new_infomask;
                    hdr.t_infomask2 |= new_infomask2;
                }
                bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
                if relation_needs_wal(relation) {
                    let mut xlrec = [0u8; 8];
                    xlrec[0..4].copy_from_slice(&new_xmax.to_ne_bytes());
                    xlrec[4..6].copy_from_slice(&offnum.to_ne_bytes());
                    xlrec[6] = compute_infobits(new_infomask, new_infomask2);
                    xlrec[7] = 0;
                    let recptr = crate::wal::insert_record(
                        RM_HEAP2_ID,
                        XLOG_HEAP2_LOCK_UPDATED,
                        0,
                        &[&xlrec],
                        &[crate::wal::reg_block(
                            0,
                            relation.rd_locator.get(),
                            ItemPointerGetBlockNumber(&mytup.t_self),
                            pin.buffer(),
                            REGBUF_STANDARD,
                            &[],
                        )],
                    )?;
                    // SAFETY: pin + exclusive lock held.
                    let mut pm = unsafe {
                        PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer()))
                    };
                    pm.set_lsn(recptr);
                }
            }

            let hdr = mytup.t_data();
            if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0
                || ::types_tuple::ItemPointerIndicatesMovedPartitions(&hdr.t_ctid)
                || mytup.t_self == hdr.t_ctid
                || hv_seam::heap_tuple_header_is_only_locked::call(hdr)?
            {
                done!(TM_Result::TM_Ok);
            }
            prior_xmax = HeapTupleHeaderGetUpdateXid(hdr)?;
            tupid = hdr.t_ctid;
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            pin.release();
            continue 'chain;
        }
    }
}
