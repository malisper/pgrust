// heapam.c DML phase 2: heap_insert / heap_delete / heap_update cores.
// Deferred (named panics): toast, MultiXact wait lanes,
// speculative-insert driver, index-attr bitmaps (updates on relhasindex
// rels), bulk insert + heap_multi_insert, heap_lock_tuple (phase 3).
// C divergences: crit sections pend miscadmin; WAL prefix/suffix
// compression off (XLogCheckBufferNeedsBackup pends xloginsert; C also
// disables it under wal_level=logical), records stay redo-compatible.
use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use ::tableam_vocab::{
    BulkInsertStateData, LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
};
use ::types_core::xact::{InvalidTransactionId, InvalidXLogRecPtr, TransactionIdIsValid};
use ::types_core::{CommandId, InvalidBlockNumber, MultiXactId, TransactionId};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_rel::{
    RelationData, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_RELATION, REPLICA_IDENTITY_FULL,
    REPLICA_IDENTITY_NOTHING,
};
use ::types_snapshot::SnapshotData;
use ::types_storage::bufpage::{ItemIdData, PageMut, PageRef, SizeofHeapTupleHeader};
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, RowShareLock, LOCKMODE,
};
use ::types_storage::multixact::{
    ISUPDATE_from_mxstatus, MultiXactConflict, MultiXactMember, MultiXactStatus,
};
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, ItemPointerData, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, HEAP2_XACT_MASK, HEAP_COMBOCID, HEAP_KEYS_UPDATED,
    HEAP_LOCKED_UPGRADED, HEAP_LOCK_MASK, HEAP_MOVED, HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_BITS,
    HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_INVALID, HEAP_XMAX_IS_LOCKED_ONLY,
    HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_SHR_LOCK,
};
use ::xloginsert_seams::{REGBUF_KEEP_DATA, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::hio::{RelationGetBufferForTuple, RelationPutHeapTuple, HEAP_INSERT_SPECULATIVE};
use crate::{HeapTupleHeaderGetUpdateXid, MultiXactIdGetUpdateXid};
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
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;
pub const XLH_INSERT_ON_TOAST_RELATION: u8 = 1 << 4;
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = 1 << 5;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = 1 << 1;
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
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

// RelationNeedsWAL (rel.h): under wal_level=minimal, relations created (or
// given a new relfilenumber) in the current transaction skip WAL; commit
// durability comes from smgrDoPendingSyncs.
pub fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

// RelationIsLogicallyLogged (utils/rel.h).
pub fn relation_is_logically_logged(rel: &RelationData<'_>) -> bool {
    transam_xlog_seams::xlog_logical_info_active::call()
        && relation_needs_wal(rel)
        && rel.rd_rel.relkind != RELKIND_FOREIGN_TABLE
        && !catalog_seams::is_catalog_relation::call(rel)
}

// RelationIsAccessibleInLogicalDecoding (utils/rel.h).
pub fn relation_is_accessible_in_logical_decoding(rel: &RelationData<'_>) -> bool {
    transam_xlog_seams::xlog_logical_info_active::call()
        && relation_needs_wal(rel)
        && (catalog_seams::is_catalog_relation::call(rel) || rel.is_used_as_catalog_table())
}

// IsParallelWorker() (miscadmin.h), via the seam (installed by the parallel
// crate at boot; uninstalled = unit-test process = not a worker). This was a
// dead stub returning false until W2a — the CTAS-under-Gather postmortem's
// "debug asserts are not witnesses" law: unmarked worker-thread inserts must
// ERROR in release builds too.
fn is_parallel_worker() -> bool {
    parallel_seams::is_parallel_worker::is_installed() && parallel_seams::is_parallel_worker::call()
}

// W2a carve: the parallel-write capability token. C forbids ALL worker
// inserts (heap_prepare_insert's IsParallelWorker ereport); pgrust's W2a
// block-run write sink is the one sanctioned exception — it arms this token
// around exactly its own write calls (never thread-ambient), so the guard
// stays a live tripwire for every OTHER worker-side insert path. RAII so an
// unwinding write cannot leak the permit into later statements on a retained
// worker thread.
thread_local! {
    static PARALLEL_WRITE_PERMITS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII permit for heap inserts on a parallel-worker thread (W2a block-run
/// write sink only — see the module note on `is_parallel_worker`).
pub struct ParallelWriteGuard(());

impl ParallelWriteGuard {
    #[allow(clippy::new_without_default)]
    pub fn new() -> ParallelWriteGuard {
        PARALLEL_WRITE_PERMITS.with(|c| c.set(c.get() + 1));
        ParallelWriteGuard(())
    }
}

impl Drop for ParallelWriteGuard {
    fn drop(&mut self) {
        PARALLEL_WRITE_PERMITS.with(|c| c.set(c.get() - 1));
    }
}

fn parallel_write_permitted() -> bool {
    PARALLEL_WRITE_PERMITS.with(|c| c.get() > 0)
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
    (if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
        XLHL_XMAX_IS_MULTI
    } else {
        0
    }) | (if (infomask & HEAP_XMAX_LOCK_ONLY) != 0 {
        XLHL_XMAX_LOCK_ONLY
    } else {
        0
    }) | (if (infomask & HEAP_XMAX_EXCL_LOCK) != 0 {
        XLHL_XMAX_EXCL_LOCK
    } else {
        0
    }) | (if (infomask & HEAP_XMAX_KEYSHR_LOCK) != 0 {
        XLHL_XMAX_KEYSHR_LOCK
    } else {
        0
    }) | (if (infomask2 & HEAP_KEYS_UPDATED) != 0 {
        XLHL_KEYS_UPDATED
    } else {
        0
    })
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

#[track_caller]
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
    if is_parallel_worker() && !parallel_write_permitted() {
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

    predicate_seams::check_for_serializable_conflict_in::call(relation, None, InvalidBlockNumber)?;

    RelationPutHeapTuple(
        relation,
        &pin,
        heaptup,
        (options & HEAP_INSERT_SPECULATIVE) != 0,
    )?;

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
        if relation_is_accessible_in_logical_decoding(relation) {
            log_heap_new_cid(relation, heaptup)?;
        }

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
        if relation_is_logically_logged(relation)
            && (options & crate::hio::HEAP_INSERT_NO_LOGICAL) == 0
        {
            flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            bufflags |= REGBUF_KEEP_DATA;
            if catalog_seams::is_toast_relation::call(relation) {
                flags |= XLH_INSERT_ON_TOAST_RELATION;
            }
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
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
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
pub fn simple_heap_insert(
    relation: &RelationData<'_>,
    tup: &mut HeapTupleData<'_>,
) -> PgResult<()> {
    heap_insert(
        relation,
        tup,
        xact_seams::get_current_command_id::call(true)?,
        0,
        None,
    )
}

pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
const XLOG_HEAP2_NEW_CID: u8 = 0x70;
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
    let need_tuple_data = relation_is_logically_logged(relation);
    let need_cids = relation_is_accessible_in_logical_decoding(relation);
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
                    let erased = unsafe {
                        HeapTupleData::from_raw_parts(
                            ht.header_ptr().cast_mut(),
                            ht.t_len,
                            ht.t_self,
                            ht.t_tableOid,
                        )
                    };
                    // Dropping t is heap_freetuple: the aset free-list header
                    // would overwrite t_choice before placement. The image is
                    // bulk-freed with toast_ctx (C: dies with caller context).
                    core::mem::forget(t);
                    erased
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
    // C heapam.c:2360 carries one vmbuffer across the whole insert loop and
    // releases it after (heapam.c:2659-2668): consecutive heap pages nearly
    // always share a VM page, so the pin survives page switches.
    let mut vmb = visibilitymap::VmBuffer::new();
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

        // COPY FREEZE onto a page we started empty: every row the page will
        // ever hold in this batch is frozen, so the page and the VM can be
        // marked all-visible + all-frozen at insert time (heapam.c:2460-2461).
        let all_frozen_set =
            starting_with_empty_page && (options & crate::hio::HEAP_INSERT_FROZEN) != 0;

        RelationPutHeapTuple(relation, &pin, &mut heaptuples[ndone], false)?;
        if needwal && need_cids {
            log_heap_new_cid(relation, &heaptuples[ndone])?;
        }
        let mut nthispage = 1usize;
        while ndone + nthispage < ntuples {
            let need = ((heaptuples[ndone + nthispage].t_len as usize + 7) & !7) + save_free_space;
            if pin.page().heap_free_space() < need {
                break;
            }
            RelationPutHeapTuple(relation, &pin, &mut heaptuples[ndone + nthispage], false)?;
            if needwal && need_cids {
                log_heap_new_cid(relation, &heaptuples[ndone + nthispage])?;
            }
            nthispage += 1;
        }

        // Pin-at-clear divergence (heap_insert shape): C pins the vm page in
        // RelationGetBufferForTuple, before the content lock (hio.c:618-627,
        // 774-789 — C must not do VM-fork I/O under a content lock; our
        // single-threaded-per-page model tolerates it and every existing VM
        // touch point in this file already pins at use).
        //
        // C heapam.c:2496-2512: an all-visible page only loses its bit when
        // the incoming rows are NOT frozen; frozen rows keep it true. A page
        // we started empty under FREEZE becomes all-visible right here, so
        // the WAL record (and INIT_PAGE replay) carries the flag.
        let mut all_visible_cleared = false;
        if pin.page().is_all_visible() && (options & crate::hio::HEAP_INSERT_FROZEN) == 0 {
            all_visible_cleared = true;
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
        } else if all_frozen_set {
            // SAFETY: pinned + exclusive content lock since RelationGetBufferForTuple.
            let mut pm =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            pm.set_all_visible();
        }

        bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

        if needwal {
            let init = starting_with_empty_page;
            // C heapam.c:2555: the two VM-state flags are mutually exclusive.
            debug_assert!(!(all_visible_cleared && all_frozen_set));
            let mut xl_flags = 0u8;
            if all_visible_cleared {
                xl_flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
            }
            if all_frozen_set {
                xl_flags |= XLH_INSERT_ALL_FROZEN_SET;
            }
            if need_tuple_data {
                xl_flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
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
            if need_tuple_data {
                bufflags |= REGBUF_KEEP_DATA;
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

        // C heapam.c:2636-2654: set the VM bits after the multi-insert record,
        // still under the heap page's content lock. visibilitymap_set emits
        // its own XLOG_HEAP2_VISIBLE record for the VM-page change (C's WAL
        // shape: visibilitymap.c:288-293); a crash between the two records
        // replays PD_ALL_VISIBLE without the VM bit — the benign direction.
        // InvalidTransactionId cutoff as C: FROZEN intentionally violates
        // visibility rules, and only same-xact readers can see the table.
        if all_frozen_set {
            debug_assert!(pin.page().is_all_visible());
            visibilitymap::visibilitymap_pin(relation, pin.block_number(), &mut vmb)?;
            visibilitymap::visibilitymap_set(
                relation,
                pin.block_number(),
                pin.buffer(),
                InvalidXLogRecPtr,
                &vmb,
                InvalidTransactionId,
                visibilitymap::VISIBILITYMAP_ALL_VISIBLE | visibilitymap::VISIBILITYMAP_ALL_FROZEN,
            )?;
        }

        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        pin.release();
        ndone += nthispage;
    }
    // C heapam.c:2666-2668: done inserting; release the carried vmbuffer.
    vmb.release();

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

/// `get_mxact_status_for_lock` (tupleLockExtraInfo lockstatus/updstatus).
fn get_mxact_status_for_lock(mode: LockTupleMode, is_update: bool) -> PgResult<MultiXactStatus> {
    Ok(match (mode, is_update) {
        (LockTupleMode::LockTupleKeyShare, false) => MultiXactStatus::MultiXactStatusForKeyShare,
        (LockTupleMode::LockTupleShare, false) => MultiXactStatus::MultiXactStatusForShare,
        (LockTupleMode::LockTupleNoKeyExclusive, false) => {
            MultiXactStatus::MultiXactStatusForNoKeyUpdate
        }
        (LockTupleMode::LockTupleExclusive, false) => MultiXactStatus::MultiXactStatusForUpdate,
        (LockTupleMode::LockTupleNoKeyExclusive, true) => {
            MultiXactStatus::MultiXactStatusNoKeyUpdate
        }
        (LockTupleMode::LockTupleExclusive, true) => MultiXactStatus::MultiXactStatusUpdate,
        _ => {
            return Err(crate::elog_error(std::format!(
                "invalid lock tuple mode {}/{}",
                mode as i32,
                is_update
            )))
        }
    })
}

/// `TUPLOCK_from_mxstatus` (MultiXactStatusLock table).
const fn TUPLOCK_from_mxstatus(status: MultiXactStatus) -> LockTupleMode {
    match status {
        MultiXactStatus::MultiXactStatusForKeyShare => LockTupleMode::LockTupleKeyShare,
        MultiXactStatus::MultiXactStatusForShare => LockTupleMode::LockTupleShare,
        MultiXactStatus::MultiXactStatusForNoKeyUpdate
        | MultiXactStatus::MultiXactStatusNoKeyUpdate => LockTupleMode::LockTupleNoKeyExclusive,
        MultiXactStatus::MultiXactStatusForUpdate | MultiXactStatus::MultiXactStatusUpdate => {
            LockTupleMode::LockTupleExclusive
        }
    }
}

fn collect_multixact_members(
    multi: MultiXactId,
    is_lock_only: bool,
) -> PgResult<Vec<MultiXactMember>> {
    let mut out = Vec::new();
    multixact_seams::get_multi_xact_id_members::call(multi, false, is_lock_only, &mut |members| {
        out.extend_from_slice(members);
    })?;
    Ok(out)
}

/// `DoesMultiXactIdConflict`: does the multi conflict with the current
/// transaction grabbing a tuple lock of the given strength?
/// `current_is_member` is tracked only when `need_member` (C's non-NULL out
/// param).
pub(crate) fn DoesMultiXactIdConflict(
    multi: MultiXactId,
    infomask: u16,
    lockmode: LockTupleMode,
    need_member: bool,
) -> PgResult<MultiXactConflict> {
    let mut out = MultiXactConflict {
        conflict: false,
        current_is_member: false,
    };
    if HEAP_LOCKED_UPGRADED(infomask) {
        return Ok(out);
    }
    let wanted = tuple_lock_hwlock(lockmode);
    for m in collect_multixact_members(multi, HEAP_XMAX_IS_LOCKED_ONLY(infomask))? {
        if out.conflict && (!need_member || out.current_is_member) {
            break;
        }
        let memlockmode = tuple_lock_hwlock(TUPLOCK_from_mxstatus(m.status));
        if xact_seams::transaction_id_is_current_transaction_id::call(m.xid) {
            out.current_is_member = true;
            continue;
        } else if out.conflict {
            continue;
        }
        if !::lock_seams::do_lock_modes_conflict::call(memlockmode, wanted) {
            continue;
        }
        if ISUPDATE_from_mxstatus(m.status) {
            // aborted updaters don't conflict
            if transam_seams::transaction_id_did_abort::call(m.xid)? {
                continue;
            }
        } else if !procarray_seams::transaction_id_is_in_progress::call(m.xid)? {
            // lockers-only stop conflicting the moment they end
            continue;
        }
        out.conflict = true;
    }
    Ok(out)
}

/// `Do_MultiXactIdWait`: sleep on (or, `nowait`, conditionally probe) each
/// member whose status conflicts. Own-xact members are never waited on
/// (XactLockTableWait asserts against it). Returns false only when `nowait`
/// and a conflicting member's xact lock was unavailable; `remaining` receives
/// the count of members still running (not to be trusted on a false return).
fn Do_MultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    nowait: bool,
    relation: &RelationData<'_>,
    ctid: Option<&ItemPointerData>,
    oper: ::types_storage::lock::XLTW_Oper,
    remaining: Option<&mut i32>,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let mut result = true;
    let mut remain = 0;
    if !HEAP_LOCKED_UPGRADED(infomask) {
        for m in collect_multixact_members(multi, HEAP_XMAX_IS_LOCKED_ONLY(infomask))? {
            if xact_seams::transaction_id_is_current_transaction_id::call(m.xid) {
                remain += 1;
                continue;
            }
            if !::lock_seams::do_lock_modes_conflict::call(
                tuple_lock_hwlock(TUPLOCK_from_mxstatus(m.status)),
                tuple_lock_hwlock(TUPLOCK_from_mxstatus(status)),
            ) {
                if remaining.is_some()
                    && procarray_seams::transaction_id_is_in_progress::call(m.xid)?
                {
                    remain += 1;
                }
                continue;
            }
            if nowait {
                result = lmgr::ConditionalXactLockTableWait(m.xid, log_lock_failure)?;
                if !result {
                    break;
                }
            } else {
                lmgr::XactLockTableWait(m.xid, Some(relation), ctid, oper)?;
            }
        }
    }
    if let Some(r) = remaining {
        *r = remain;
    }
    Ok(result)
}

/// `MultiXactIdWait`.
pub(crate) fn MultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    relation: &RelationData<'_>,
    ctid: Option<&ItemPointerData>,
    oper: ::types_storage::lock::XLTW_Oper,
    remaining: Option<&mut i32>,
) -> PgResult<()> {
    Do_MultiXactIdWait(
        multi, status, infomask, false, relation, ctid, oper, remaining, false,
    )?;
    Ok(())
}

/// `ConditionalMultiXactIdWait`: as above, but only lock if we can get each
/// member's lock without blocking. True = the multixact is now all gone.
fn ConditionalMultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    relation: &RelationData<'_>,
    remaining: Option<&mut i32>,
    log_lock_failure: bool,
) -> PgResult<bool> {
    Do_MultiXactIdWait(
        multi,
        status,
        infomask,
        true,
        relation,
        None,
        ::types_storage::lock::XLTW_Oper::None,
        remaining,
        log_lock_failure,
    )
}

/// `compute_new_xmax_infomask`: (new_xmax, new_infomask, new_infomask2).
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
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            let new_xmax =
                multixact_seams::multi_xact_id_expand::call(xmax, add_to_xmax, new_status)?;
            let (new_infomask, new_infomask2) = crate::freeze::GetMultiXactIdHintBits(new_xmax)?;
            return Ok((new_xmax, new_infomask, new_infomask2));
        } else if (old_infomask & HEAP_XMAX_COMMITTED) != 0 {
            // committed update: preserve it as updater alongside the new locker
            let status = if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                MultiXactStatus::MultiXactStatusUpdate
            } else {
                MultiXactStatus::MultiXactStatusNoKeyUpdate
            };
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            let new_xmax =
                multixact_seams::multi_xact_id_create::call(xmax, status, add_to_xmax, new_status)?;
            let (new_infomask, new_infomask2) = crate::freeze::GetMultiXactIdHintBits(new_xmax)?;
            return Ok((new_xmax, new_infomask, new_infomask2));
        } else if procarray_seams::transaction_id_is_in_progress::call(xmax)? {
            let old_status = if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                if ::types_tuple::HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                    MultiXactStatus::MultiXactStatusForKeyShare
                } else if ::types_tuple::HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                    MultiXactStatus::MultiXactStatusForShare
                } else if ::types_tuple::HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                    if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                        MultiXactStatus::MultiXactStatusForUpdate
                    } else {
                        MultiXactStatus::MultiXactStatusForNoKeyUpdate
                    }
                } else {
                    // LOCK_ONLY without lock bits: pg_upgrade-only state, the
                    // locker cannot still be running (C emits a WARNING here)
                    old_infomask |= HEAP_XMAX_INVALID;
                    old_infomask &= !HEAP_XMAX_LOCK_ONLY;
                    continue;
                }
            } else if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                MultiXactStatus::MultiXactStatusUpdate
            } else {
                MultiXactStatus::MultiXactStatusNoKeyUpdate
            };
            let old_mode = TUPLOCK_from_mxstatus(old_status);
            if xmax == add_to_xmax {
                debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));
                // acquire the strongest of both; single-xid restart trick
                if (mode as u32) < (old_mode as u32) {
                    mode = old_mode;
                }
                old_infomask |= HEAP_XMAX_INVALID;
                continue;
            }
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            let new_xmax = multixact_seams::multi_xact_id_create::call(
                xmax,
                old_status,
                add_to_xmax,
                new_status,
            )?;
            let (new_infomask, new_infomask2) = crate::freeze::GetMultiXactIdHintBits(new_xmax)?;
            return Ok((new_xmax, new_infomask, new_infomask2));
        } else if !HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)
            && transam_seams::transaction_id_did_commit::call(xmax)?
        {
            // committed update whose hint bit is not yet set
            let status = if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                MultiXactStatus::MultiXactStatusUpdate
            } else {
                MultiXactStatus::MultiXactStatusNoKeyUpdate
            };
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            let new_xmax =
                multixact_seams::multi_xact_id_create::call(xmax, status, add_to_xmax, new_status)?;
            let (new_infomask, new_infomask2) = crate::freeze::GetMultiXactIdHintBits(new_xmax)?;
            return Ok((new_xmax, new_infomask, new_infomask2));
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

fn could_not_obtain_row_lock(relation: &RelationData<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(std::format!(
            "could not obtain lock on row in relation \"{}\"",
            relation.name()
        ))
        .with_sqlstate(::types_error::ERRCODE_LOCK_NOT_AVAILABLE),
    )
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
                return Err(could_not_obtain_row_lock(relation));
            }
        }
    }
    *have_tuple_lock = true;
    Ok(true)
}

const SizeOfHeapNewCid: usize = 34;

// No buffer registration: the record modifies no page.
fn log_heap_new_cid(relation: &RelationData<'_>, tup: &HeapTupleData<'_>) -> PgResult<()> {
    let hdr = tup.t_data();
    let top_xid = xact_seams::get_top_transaction_id_if_any::call();
    debug_assert!(TransactionIdIsValid(top_xid));

    let invalid = ::types_core::xact::InvalidCommandId;
    let (cmin, cmax, combocid) = if (hdr.t_infomask & HEAP_COMBOCID) != 0 {
        debug_assert!((hdr.t_infomask & HEAP_XMAX_INVALID) == 0);
        (
            combocid_seams::heap_tuple_header_get_cmin::call(hdr),
            combocid_seams::heap_tuple_header_get_cmax::call(hdr),
            hdr.raw_command_id(),
        )
    } else if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0 || HEAP_XMAX_IS_LOCKED_ONLY(hdr.t_infomask)
    {
        (hdr.raw_command_id(), invalid, invalid)
    } else {
        (invalid, hdr.raw_command_id(), invalid)
    };

    let loc = relation.rd_locator.get();
    let mut xlrec = [0u8; SizeOfHeapNewCid];
    xlrec[0..4].copy_from_slice(&top_xid.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&cmin.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&cmax.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&combocid.to_ne_bytes());
    xlrec[16..20].copy_from_slice(&loc.spcOid.to_ne_bytes());
    xlrec[20..24].copy_from_slice(&loc.dbOid.to_ne_bytes());
    xlrec[24..28].copy_from_slice(&loc.relNumber.to_ne_bytes());
    xlrec[28..30].copy_from_slice(&tup.t_self.ip_blkid.bi_hi.to_ne_bytes());
    xlrec[30..32].copy_from_slice(&tup.t_self.ip_blkid.bi_lo.to_ne_bytes());
    xlrec[32..34].copy_from_slice(&tup.t_self.ip_posid.to_ne_bytes());

    crate::wal::insert_record(RM_HEAP2_ID, XLOG_HEAP2_NEW_CID, 0, &[&xlrec], &[])?;
    Ok(())
}

pub(crate) struct OldKeyTuple {
    tup: HeapTupleData<'static>,
    _ctx: Option<::mcx::MemoryContext>,
}

fn erase_owned(mut t: heaptuple::HeapTuple<'_>) -> HeapTupleData<'static> {
    let ht = t.as_tuple_mut();
    // SAFETY: image owned by the caller-held context (page_tuple model).
    let tup = unsafe {
        HeapTupleData::from_raw_parts(
            ht.header_ptr().cast_mut(),
            ht.t_len,
            ht.t_self,
            ht.t_tableOid,
        )
    };
    // Drop is heap_freetuple; the image is bulk-freed with the context.
    core::mem::forget(t);
    tup
}

impl OldKeyTuple {
    fn header(&self) -> [u8; 5] {
        xl_heap_header(self.tup.t_data())
    }

    fn body(&self) -> &[u8] {
        // SAFETY: tuple image is t_len readable bytes.
        unsafe {
            core::slice::from_raw_parts(
                self.tup.header_ptr().add(SizeofHeapTupleHeader),
                self.tup.t_len as usize - SizeofHeapTupleHeader,
            )
        }
    }
}

// ExtractReplicaIdentity: the FULL-without-externals arm aliases `tp` (C
// returns tp itself), so later header stamps show through.
fn extract_replica_identity(
    relation: &RelationData<'_>,
    tp: &HeapTupleData<'_>,
    key_required: bool,
) -> PgResult<Option<OldKeyTuple>> {
    if !relation_is_logically_logged(relation) {
        return Ok(None);
    }
    let replident = relation.rd_rel.relreplident;
    if replident == REPLICA_IDENTITY_NOTHING {
        return Ok(None);
    }
    let desc = &relation.rd_att;
    if replident == REPLICA_IDENTITY_FULL {
        if tp.has_external() {
            let ctx = ::mcx::MemoryContext::new("ExtractReplicaIdentity");
            let tup = erase_owned(heaptoast_seams::toast_flatten_tuple::call(
                ctx.mcx(),
                tp,
                desc,
            )?);
            return Ok(Some(OldKeyTuple {
                tup,
                _ctx: Some(ctx),
            }));
        }
        // SAFETY: aliases the caller's tuple image, valid while its pin holds.
        let tup = unsafe {
            HeapTupleData::from_raw_parts(
                tp.header_ptr().cast_mut(),
                tp.t_len,
                tp.t_self,
                tp.t_tableOid,
            )
        };
        return Ok(Some(OldKeyTuple { tup, _ctx: None }));
    }

    if !key_required {
        return Ok(None);
    }

    let idattrs = relcache_seams::relation_get_index_attr_bitmap::call(relation.rd_id)?;
    if idattrs.identity.is_empty() {
        return Ok(None);
    }

    let ctx = ::mcx::MemoryContext::new("ExtractReplicaIdentity");
    let cmcx = ctx.mcx();
    let natts = desc.natts as usize;
    let mut values: ::mcx::PgVec<'_, ::datum::Datum> = ::mcx::PgVec::new_in(cmcx);
    values.resize(natts, ::datum::Datum::null());
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::PgVec::new_in(cmcx);
    nulls.resize(natts, false);
    ::types_tuple::heap_deform_tuple(tp, desc, &mut values, &mut nulls);

    for i in 0..natts {
        if idattrs.identity.binary_search(&((i + 1) as i16)).is_ok() {
            debug_assert!(!nulls[i]);
        } else {
            nulls[i] = true;
        }
    }

    let mut tup = erase_owned(heaptuple::heap_form_tuple(cmcx, desc, &values, &nulls)?);
    if tup.has_external() {
        tup = erase_owned(heaptoast_seams::toast_flatten_tuple::call(
            cmcx, &tup, desc,
        )?);
    }
    drop(values);
    drop(nulls);
    Ok(Some(OldKeyTuple {
        tup,
        _ctx: Some(ctx),
    }))
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

    // Pin the VM page before taking the content lock (deadlock rule: no IO
    // under the lock); rechecked at l1 since the bit can change until locked.
    let mut vmb = visibilitymap::VmBuffer::new();
    if pin.page().is_all_visible() {
        visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
    }
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let lp = pin.page().item_id(ItemPointerGetOffsetNumber(tid));
    debug_assert!(lp.is_normal());

    let mut have_tuple_lock = false;
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };

    let mut result = 'l1: loop {
        if !vmb.is_valid() && pin.page().is_all_visible() {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
        }
        let mut result = hv_seam::heap_tuple_satisfies_update::call(&mut tp, cid, pin.buffer())?;

        if result == TM_Result::TM_Invisible {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            pin.release();
            return Err(invisible_tuple("update"));
        } else if result == TM_Result::TM_BeingModified && wait {
            let xwait = tp.t_data().xmax_raw();
            let infomask = tp.t_data().t_infomask;

            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let conf = DoesMultiXactIdConflict(
                    xwait,
                    infomask,
                    LockTupleMode::LockTupleExclusive,
                    true,
                )?;
                if conf.conflict {
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

                    // Skip the tuple lock when we're already a member of the
                    // multi (avoids deadlock).
                    if !conf.current_is_member {
                        heap_acquire_tuplock(
                            relation,
                            &tp.t_self,
                            LockTupleMode::LockTupleExclusive,
                            LockWaitPolicy::LockWaitBlock,
                            &mut have_tuple_lock,
                        )?;
                    }
                    MultiXactIdWait(
                        xwait,
                        MultiXactStatus::MultiXactStatusUpdate,
                        infomask,
                        relation,
                        Some(&tp.t_self),
                        ::types_storage::lock::XLTW_Oper::Delete,
                        None,
                    )?;
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

                    if (!vmb.is_valid() && pin.page().is_all_visible())
                        || xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                        || tp.t_data().xmax_raw() != xwait
                    {
                        continue 'l1;
                    }
                }
                // Surviving members (our own xact or its subxacts) are legal
                // here; the xmax is about to be overwritten, so no hint bits.
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

                if (!vmb.is_valid() && pin.page().is_all_visible())
                    || xmax_infomask_changed(tp.t_data().t_infomask, infomask)
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
        vmb.release();
        return Ok(result);
    }
    let _ = &mut result;

    predicate_seams::check_for_serializable_conflict_in::call(
        relation,
        Some(tid),
        pin.block_number(),
    )?;

    let (cid, iscombo) = combocid_seams::heap_tuple_header_adjust_cmax::call(tp.t_data(), cid)?;

    let old_key_tuple = extract_replica_identity(relation, &tp, true)?;

    multixact_seams::multi_xact_id_set_oldest_member::call()?;

    let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        tp.t_data().xmax_raw(),
        tp.t_data().t_infomask,
        tp.t_data().t_infomask2,
        xid,
        LockTupleMode::LockTupleExclusive,
        true,
    )?;

    let mut all_visible_cleared = false;
    {
        // SAFETY: pin + exclusive lock held.
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        page_set_prunable(&mut pm, xid);
        if pm.as_ref().is_all_visible() {
            all_visible_cleared = true;
            pm.clear_all_visible();
            visibilitymap::visibilitymap_clear(
                relation,
                pin.block_number(),
                &vmb,
                visibilitymap::VISIBILITYMAP_VALID_BITS,
            )?;
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
        if relation_is_accessible_in_logical_decoding(relation) {
            log_heap_new_cid(relation, &tp)?;
        }
        let mut flags = 0u8;
        if all_visible_cleared {
            flags |= XLH_DELETE_ALL_VISIBLE_CLEARED;
        }
        if changing_part {
            flags |= XLH_DELETE_IS_PARTITION_MOVE;
        }
        if old_key_tuple.is_some() {
            flags |= if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
                XLH_DELETE_CONTAINS_OLD_TUPLE
            } else {
                XLH_DELETE_CONTAINS_OLD_KEY
            };
        }
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&new_xmax.to_ne_bytes());
        xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&tp.t_self).to_ne_bytes());
        xlrec[6] = compute_infobits(tp.t_data().t_infomask, tp.t_data().t_infomask2);
        xlrec[7] = flags;

        let old_key_hdr;
        let mut main_data: [&[u8]; 3] = [&xlrec, &[], &[]];
        let n_main = match &old_key_tuple {
            Some(k) => {
                old_key_hdr = k.header();
                main_data[1] = &old_key_hdr;
                main_data[2] = k.body();
                3
            }
            None => 1,
        };
        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_DELETE,
            XLOG_INCLUDE_ORIGIN,
            &main_data[..n_main],
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
    vmb.release();

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

/// `heap_lock_tuple` core (heapam.c). Live: single-locker + MultiXact stamp
/// paths, the LockWaitBlock wait-then-reread path, all-visible VM pin/clear.
/// LOUD: the NOWAIT/SKIP-LOCKED wait branches. Returns the pinned
/// (content-unlocked) buffer; the caller stores the locked on-page tuple
/// from it.
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

    // Pin the VM page before taking the content lock (deadlock rule: no IO
    // under the lock); rechecked after l3 since the bit can change until locked.
    let mut vmb = visibilitymap::VmBuffer::new();
    if pin.page().is_all_visible() {
        visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
    }
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    let lp = pin.page().item_id(offnum);
    debug_assert!(lp.is_normal());

    let mut have_tuple_lock = false;
    let mut first_time = true;
    let mut skip_tuple_lock = false;
    // SAFETY: pin + exclusive lock held.
    let mut tp = unsafe { page_tuple(pin.page(), lp, *tid, relation) };
    macro_rules! relock_tp {
        () => {{
            // SAFETY: pin + exclusive lock held.
            tp = unsafe { page_tuple(pin.page(), pin.page().item_id(offnum), *tid, relation) };
        }};
    }

    // C's `goto l3` from the post-failed VM re-pin window restarts the whole
    // qualification loop; the outer loop is that arc.
    let result = loop {
        let result = 'l3: loop {
            let mut result =
                hv_seam::heap_tuple_satisfies_update::call(&mut tp, cid, pin.buffer())?;

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

                // A subxact of ours already holding a lock >= mode means we
                // effectively hold it; must succeed without the tuple lock or
                // we deadlock against stronger-lock waiters. Only worth
                // testing on the first pass.
                if first_time {
                    first_time = false;
                    if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                        // Old multis can't reach here: HTSU would have said
                        // MayBeUpdated.
                        let members =
                            collect_multixact_members(xwait, HEAP_XMAX_IS_LOCKED_ONLY(infomask))?;
                        let mut held_strong_enough = false;
                        for m in &members {
                            if !xact_seams::transaction_id_is_current_transaction_id::call(m.xid) {
                                continue;
                            }
                            if (TUPLOCK_from_mxstatus(m.status) as u32) >= (mode as u32) {
                                held_strong_enough = true;
                                break;
                            }
                            // Weaker lock held: promoting under the
                            // heavyweight tuple lock can deadlock with a
                            // holder waiting on our xact; skip it (but still
                            // wait on the multi below).
                            skip_tuple_lock = true;
                        }
                        if held_strong_enough {
                            if have_tuple_lock {
                                lmgr::UnlockTuple(relation, tid, tuple_lock_hwlock(mode))?;
                            }
                            vmb.release();
                            return Ok((TM_Result::TM_Ok, pin));
                        }
                    } else if xact_seams::transaction_id_is_current_transaction_id::call(xwait) {
                        let already = match mode {
                            LockTupleMode::LockTupleKeyShare => true,
                            LockTupleMode::LockTupleShare => {
                                HEAP_XMAX_IS_SHR_LOCKED(infomask)
                                    || HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                            }
                            LockTupleMode::LockTupleNoKeyExclusive => {
                                HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                            }
                            LockTupleMode::LockTupleExclusive => {
                                HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                                    && (infomask2 & HEAP_KEYS_UPDATED) != 0
                            }
                        };
                        if already {
                            if have_tuple_lock {
                                lmgr::UnlockTuple(relation, tid, tuple_lock_hwlock(mode))?;
                            }
                            vmb.release();
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
                                && (((tp.t_data().t_infomask2 & HEAP_KEYS_UPDATED) != 0)
                                    || !updated)
                            {
                                continue 'l3;
                            }
                            require_sleep = false;
                        }
                    }
                    LockTupleMode::LockTupleShare => {
                        if HEAP_XMAX_IS_LOCKED_ONLY(infomask) && !HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                        {
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
                        if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                            if !DoesMultiXactIdConflict(xwait, infomask, mode, false)?.conflict {
                                // no conflict; restart if xmax moved meanwhile
                                bufmgr_seams::lock_buffer::call(
                                    pin.buffer(),
                                    BUFFER_LOCK_EXCLUSIVE,
                                )?;
                                relock_tp!();
                                if xmax_infomask_changed(tp.t_data().t_infomask, infomask)
                                    || tp.t_data().xmax_raw() != xwait
                                {
                                    continue 'l3;
                                }
                                require_sleep = false;
                            }
                        } else if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) {
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
                    if !skip_tuple_lock
                        && !heap_acquire_tuplock(
                            relation,
                            tid,
                            mode,
                            wait_policy,
                            &mut have_tuple_lock,
                        )?
                    {
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        relock_tp!();
                        break 'l3 TM_Result::TM_WouldBlock;
                    }
                    if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                        let status = get_mxact_status_for_lock(mode, false)?;
                        // we only ever lock tuples here, never update them
                        if status >= MultiXactStatus::MultiXactStatusNoKeyUpdate {
                            return Err(crate::elog_error("invalid lock mode in heap_lock_tuple"));
                        }
                        match wait_policy {
                            LockWaitPolicy::LockWaitBlock => {
                                MultiXactIdWait(
                                    xwait,
                                    status,
                                    infomask,
                                    relation,
                                    Some(&tp.t_self),
                                    ::types_storage::lock::XLTW_Oper::Lock,
                                    None,
                                )?;
                            }
                            LockWaitPolicy::LockWaitSkip => {
                                if !ConditionalMultiXactIdWait(
                                    xwait, status, infomask, relation, None, false,
                                )? {
                                    // C's goto failed expects the buffer lock held.
                                    bufmgr_seams::lock_buffer::call(
                                        pin.buffer(),
                                        BUFFER_LOCK_EXCLUSIVE,
                                    )?;
                                    relock_tp!();
                                    break 'l3 TM_Result::TM_WouldBlock;
                                }
                            }
                            LockWaitPolicy::LockWaitError => {
                                if !ConditionalMultiXactIdWait(
                                    xwait, status, infomask, relation, None, false,
                                )? {
                                    return Err(could_not_obtain_row_lock(relation));
                                }
                            }
                        }
                        // Survivors are preserved: light-mode lockers and our
                        // own (sub)xact members stay in the multi.
                    } else {
                        match wait_policy {
                            LockWaitPolicy::LockWaitBlock => {
                                lmgr::XactLockTableWait(
                                    xwait,
                                    Some(relation),
                                    Some(&tp.t_self),
                                    ::types_storage::lock::XLTW_Oper::Lock,
                                )?;
                            }
                            LockWaitPolicy::LockWaitSkip => {
                                if !lmgr::ConditionalXactLockTableWait(xwait, false)? {
                                    // C's goto failed expects the buffer lock held.
                                    bufmgr_seams::lock_buffer::call(
                                        pin.buffer(),
                                        BUFFER_LOCK_EXCLUSIVE,
                                    )?;
                                    relock_tp!();
                                    break 'l3 TM_Result::TM_WouldBlock;
                                }
                            }
                            LockWaitPolicy::LockWaitError => {
                                if !lmgr::ConditionalXactLockTableWait(xwait, false)? {
                                    return Err(could_not_obtain_row_lock(relation));
                                }
                            }
                        }
                    }
                    if follow_updates && !HEAP_XMAX_IS_LOCKED_ONLY(infomask) && tp.t_self != t_ctid
                    {
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
                    // Multi case skips the hint: some member lockers might
                    // still be running.
                    if (infomask & HEAP_XMAX_IS_MULTI) == 0 {
                        update_xmax_hint_bits(&mut tp, pin.buffer(), xwait)?;
                    }
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

        if result != TM_Result::TM_Ok || vmb.is_valid() || !pin.page().is_all_visible() {
            break result;
        }
        // Didn't pin the VM page and the page became all visible during an
        // unlocked window: unlock, pin, re-lock, and requalify (C's goto l3).
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
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
        vmb.release();
        return Ok((result, pin));
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

    // Locking doesn't change visibility, so only the all-frozen bit is
    // cleared (the locker's xmax falsifies it).
    let mut cleared_all_frozen = false;
    if pin.page().is_all_visible()
        && visibilitymap::visibilitymap_clear(
            relation,
            block,
            &vmb,
            visibilitymap::VISIBILITYMAP_ALL_FROZEN,
        )?
    {
        cleared_all_frozen = true;
    }

    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(relation) {
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&xid.to_ne_bytes());
        xlrec[4..6].copy_from_slice(&offnum.to_ne_bytes());
        xlrec[6] = compute_infobits(new_infomask, tp.t_data().t_infomask2);
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
    vmb.release();
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
        TM_Result::TM_SelfModified => {
            Err(Box::new(PgError::error("tuple already updated by self")))
        }
        TM_Result::TM_Updated => Err(Box::new(PgError::error("tuple concurrently updated"))),
        TM_Result::TM_Deleted => Err(Box::new(PgError::error("tuple concurrently deleted"))),
        _ => Err(Box::new(PgError::error(std::format!(
            "unexpected heap_delete status: {result:?}"
        )))),
    }
}

/// `heap_finish_speculative`: replace the speculative token in t_ctid with a
/// real self-pointing ctid.
pub fn heap_finish_speculative(relation: &RelationData<'_>, tid: &ItemPointerData) -> PgResult<()> {
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
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    pin.release();
    Ok(())
}

/// `heap_abort_speculative`: super-delete — xmin goes invalid so the tuple is
/// immediately dead to everyone, including our own transaction.
pub fn heap_abort_speculative(relation: &RelationData<'_>, tid: &ItemPointerData) -> PgResult<()> {
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
    // Toast chunks of a speculative tuple carry no token themselves.
    if !(catalog_seams::is_toast_relation::call(relation) || tp.t_data().is_speculative()) {
        return Err(Box::new(PgError::error(
            "attempted to kill a non-speculative tuple",
        )));
    }
    debug_assert!(!tp.t_data().is_heap_only());

    {
        // The tuple is DEAD immediately; the oldest cheap wraparound-safe
        // prune hint is TransactionXmin, clamped to relfrozenxid.
        let txmin = snapmgr_seams::transaction_xmin::call();
        debug_assert!(TransactionIdIsValid(txmin));
        let prune_xid =
            if ::types_core::xact::TransactionIdPrecedes(txmin, relation.rd_rel.relfrozenxid) {
                relation.rd_rel.relfrozenxid
            } else {
                txmin
            };
        // SAFETY: pin + exclusive lock held.
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
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
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
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

#[allow(clippy::too_many_arguments)]
fn log_heap_update(
    relation: &RelationData<'_>,
    oldbuf: &BufferPin,
    newbuf: &BufferPin,
    oldtup: &HeapTupleData<'_>,
    newtup: &HeapTupleData<'_>,
    old_key_tuple: Option<&OldKeyTuple>,
    all_visible_cleared: bool,
    new_all_visible_cleared: bool,
) -> PgResult<::types_core::XLogRecPtr> {
    debug_assert!(relation_needs_wal(relation));

    let need_tuple_data = relation_is_logically_logged(relation);

    let mut info = if newtup.t_data().is_heap_only() {
        XLOG_HEAP_HOT_UPDATE
    } else {
        XLOG_HEAP_UPDATE
    };

    // Prefix/suffix WAL compression needs XLogCheckBufferNeedsBackup; off
    // until xloginsert lands (C also disables it when need_tuple_data;
    // records stay redo-compatible, just larger).
    let mut flags = 0u8;
    if all_visible_cleared {
        flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    }
    if new_all_visible_cleared {
        flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    }
    if need_tuple_data {
        flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
        if old_key_tuple.is_some() {
            flags |= if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
                XLH_UPDATE_CONTAINS_OLD_TUPLE
            } else {
                XLH_UPDATE_CONTAINS_OLD_KEY
            };
        }
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
    if need_tuple_data {
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
    let old_key_hdr;
    let mut main_data: [&[u8]; 3] = [&xlrec, &[], &[]];
    let n_main = match old_key_tuple {
        Some(k) if need_tuple_data => {
            old_key_hdr = k.header();
            main_data[1] = &old_key_hdr;
            main_data[2] = k.body();
            3
        }
        _ => 1,
    };
    let main_data = &main_data[..n_main];
    if same_buf {
        crate::wal::insert_record(RM_HEAP_ID, info, XLOG_INCLUDE_ORIGIN, main_data, &[new_reg])
    } else {
        crate::wal::insert_record(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            main_data,
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
        Some(relcache_seams::relation_get_index_attr_bitmap::call(
            relation.rd_id,
        )?)
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
    let (hot_modified, sum_modified, key_modified, id_modified, id_has_external) =
        match &attr_bitmaps {
            Some(bm) => {
                let (idm, idext) = identity_attrs_info(relation, &oldtup, newtup, &bm.identity);
                (
                    any_attr_modified(relation, &oldtup, newtup, &bm.hot_blocking),
                    any_attr_modified(relation, &oldtup, newtup, &bm.summarized),
                    any_attr_modified(relation, &oldtup, newtup, &bm.key),
                    idm,
                    idext,
                )
            }
            None => (false, false, false, false, false),
        };
    let key_intact = !key_modified;
    let mxact_status;
    (*lockmode, mxact_status) = if key_intact {
        (
            LockTupleMode::LockTupleNoKeyExclusive,
            MultiXactStatus::MultiXactStatusNoKeyUpdate,
        )
    } else {
        (
            LockTupleMode::LockTupleExclusive,
            MultiXactStatus::MultiXactStatusUpdate,
        )
    };
    multixact_seams::multi_xact_id_set_oldest_member::call()?;

    let mut have_tuple_lock = false;
    let mut checked_lockers;
    let mut locker_remains;

    let mut result = 'l2: loop {
        checked_lockers = false;
        locker_remains = false;
        let mut result =
            hv_seam::heap_tuple_satisfies_update::call(&mut oldtup, cid, pin.buffer())?;
        debug_assert!(result != TM_Result::TM_BeingModified || wait);

        if result == TM_Result::TM_Invisible {
            // DEBUG(merge-lane triage): tuple forensics; revert to
            // invisible_tuple("update") before delivery.
            let td = oldtup.t_data();
            let dbg_xmin = td.xmin_raw();
            let dbg_is_cur = xact_seams::transaction_id_is_current_transaction_id::call(dbg_xmin);
            let dbg_committed =
                transam_seams::transaction_id_did_commit::call(dbg_xmin).unwrap_or(false);
            let dbg = std::format!(
                "attempted to update invisible tuple [rel={} tid=({},{}) xmin={} xmax={} \
                 infomask={:#x} cid={} myxid={} xmin_is_current={} xmin_did_commit={}]",
                relation.rd_id,
                ItemPointerGetBlockNumber(otid),
                ItemPointerGetOffsetNumber(otid),
                dbg_xmin,
                td.xmax_raw(),
                td.t_infomask,
                cid,
                xid,
                dbg_is_cur,
                dbg_committed,
            );
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            pin.release();
            return Err(Box::new(
                PgError::error(dbg).with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        } else if result == TM_Result::TM_BeingModified && wait {
            let xwait = oldtup.t_data().xmax_raw();
            let infomask = oldtup.t_data().t_infomask;
            let mut can_continue = false;

            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let conf = DoesMultiXactIdConflict(xwait, infomask, *lockmode, true)?;
                if conf.conflict {
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;

                    // Skip the tuple lock when we're already a member of the
                    // multi (avoids deadlock).
                    if !conf.current_is_member {
                        heap_acquire_tuplock(
                            relation,
                            &oldtup.t_self,
                            *lockmode,
                            LockWaitPolicy::LockWaitBlock,
                            &mut have_tuple_lock,
                        )?;
                    }
                    let mut remain = 0;
                    MultiXactIdWait(
                        xwait,
                        mxact_status,
                        infomask,
                        relation,
                        Some(&oldtup.t_self),
                        ::types_storage::lock::XLTW_Oper::Update,
                        Some(&mut remain),
                    )?;
                    checked_lockers = true;
                    locker_remains = remain != 0;
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

                    if xmax_infomask_changed(oldtup.t_data().t_infomask, infomask)
                        || oldtup.t_data().xmax_raw() != xwait
                    {
                        continue 'l2;
                    }
                }

                // Surviving members (our own (sub)xacts, key-share lockers
                // under NoKeyExclusive) are preserved in the new xmax. An
                // in-progress updater can't be in the multi anymore after
                // MultiXactIdWait, so committed-vs-aborted decides.
                let update_xact = if !HEAP_XMAX_IS_LOCKED_ONLY(oldtup.t_data().t_infomask) {
                    HeapTupleHeaderGetUpdateXid(oldtup.t_data())?
                } else {
                    InvalidTransactionId
                };
                if !TransactionIdIsValid(update_xact)
                    || transam_seams::transaction_id_did_abort::call(update_xact)?
                {
                    can_continue = true;
                }
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
        crate::freeze::GetMultiXactIdHintBits(xmax_new_tuple)?
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

    let (cid, iscombo) = combocid_seams::heap_tuple_header_adjust_cmax::call(oldtup.t_data(), cid)?;

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

    let mut pagefree = pin.page().heap_free_space();
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
            toasted
                .as_ref()
                .map_or(newtup.t_len, |t| t.as_tuple().t_len)
        } else {
            newtup.t_len
        };
        newtupsize = (ht_len as usize + 7) & !7;

        // Now, do we need a new page for the tuple, or not?  This is a bit
        // tricky since someone else could have added tuples to the page while
        // we weren't looking (backends are threads of one process here, so the
        // page is live shared state for the whole unlocked toast window).  We
        // have to recheck the available space after reacquiring the buffer
        // lock.  But don't bother to do that if the former amount of free
        // space is still not enough; it's unlikely there's more free now than
        // before.
        //
        // What's more, if we need to get a new page, we will need to acquire
        // buffer locks on both old and new pages.  To avoid deadlock against
        // some other backend trying to get the same two locks in the other
        // order, we must be consistent about the order we get the locks in.
        // We use the rule "lock the lower-numbered page of the relation
        // first".  To implement this, we must do RelationGetBufferForTuple
        // while not holding the lock on the old page, and we must rely on it
        // to get the locks on both pages in the correct order.  Hence we need
        // a loop.
        //
        // C additionally disjoins `vmbuffer == InvalidBuffer &&
        // PageIsAllVisible(page)` into the retry test, because its
        // all-visible clear at the end of heap_update consumes a *caller*-
        // supplied vmbuffer that must have been pinned before the content
        // lock was taken.  We have no such variable to be missing: our
        // clear_page_all_visible() pins the visibility-map page itself, at
        // clear time, under the content lock we already hold (the standing
        // pin-at-clear divergence, also used by the ALL_FROZEN clear above).
        // The clause is therefore vacuous here, not omitted.
        loop {
            if newtupsize > pagefree {
                // It doesn't fit, must use RelationGetBufferForTuple.
                newpin = Some(RelationGetBufferForTuple(
                    relation,
                    ht_len as usize,
                    Some(&pin),
                    0,
                    None,
                    0,
                )?);
                // We're all done.
                break;
            }
            // Re-acquire the lock on the old tuple's page.
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            // Re-check using the up-to-date free space.
            pagefree = pin.page().heap_free_space();
            if newtupsize > pagefree {
                // Rats, it doesn't fit anymore.  We must now unlock and loop
                // to avoid deadlock.  Fortunately, this path should seldom be
                // taken.
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            } else {
                // We're all done.
                newpin = None;
                break;
            }
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
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_full();
    }

    let old_key_tuple =
        extract_replica_identity(relation, &oldtup, id_modified || id_has_external)?;

    {
        // SAFETY: pin + exclusive lock held.
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
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
        if relation_is_accessible_in_logical_decoding(relation) {
            log_heap_new_cid(relation, &oldtup)?;
            log_heap_new_cid(relation, heaptup)?;
        }
        let recptr = log_heap_update(
            relation,
            &pin,
            put_pin,
            &oldtup,
            heaptup,
            old_key_tuple.as_ref(),
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
        let mut pm =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
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
        TM_Result::TM_SelfModified => {
            Err(Box::new(PgError::error("tuple already updated by self")))
        }
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

// HeapDetermineColumnsInfo, identity slice: any-modified plus has_external
// (an unmodified external identity attr must ride in the old_key_tuple).
fn identity_attrs_info(
    relation: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    newtup: &HeapTupleData<'_>,
    attnums: &[i16],
) -> (bool, bool) {
    let td = &relation.rd_att;
    let mut modified = false;
    let mut has_external = false;
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
            modified = true;
            continue;
        }
        if isnull1 {
            continue;
        }
        let att = td.attr(attnum as usize - 1);
        if !datum_is_equal(v1, v2, att.attbyval, att.attlen as i32) {
            modified = true;
            continue;
        }
        // VARATT_IS_EXTERNAL: the 1B_E header tag byte.
        if att.attlen as i32 == -1
            // SAFETY: non-null byref varlena datum off a live tuple.
            && unsafe { *(v1.as_usize() as *const u8) } == 0x01
        {
            has_external = true;
        }
    }
    (modified, has_external)
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
                (
                    ::types_tuple::varatt::varsize_any(p1),
                    ::types_tuple::varatt::varsize_any(p2),
                )
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
/// `test_lockmode_for_conflict` (heapam.c): can we lock this chain member
/// given its held (multixact-status-encoded) lock, or must we wait/fail?
enum LockmodeTest {
    SelfModified,
    Wait,
    Proceed,
    ConflictCommitted,
}

fn test_lockmode_for_conflict(
    status: MultiXactStatus,
    xid: TransactionId,
    mode: LockTupleMode,
) -> PgResult<LockmodeTest> {
    let wanted = get_mxact_status_for_lock(mode, false)?;
    let conflicts = || {
        ::lock_seams::do_lock_modes_conflict::call(
            tuple_lock_hwlock(TUPLOCK_from_mxstatus(status)),
            tuple_lock_hwlock(TUPLOCK_from_mxstatus(wanted)),
        )
    };
    if xact_seams::transaction_id_is_current_transaction_id::call(xid) {
        Ok(LockmodeTest::SelfModified)
    } else if procarray_seams::transaction_id_is_in_progress::call(xid)? {
        if conflicts() {
            Ok(LockmodeTest::Wait)
        } else {
            Ok(LockmodeTest::Proceed)
        }
    } else if transam_seams::transaction_id_did_abort::call(xid)? {
        Ok(LockmodeTest::Proceed)
    } else if transam_seams::transaction_id_did_commit::call(xid)? {
        if ISUPDATE_from_mxstatus(status) && conflicts() {
            Ok(LockmodeTest::ConflictCommitted)
        } else {
            Ok(LockmodeTest::Proceed)
        }
    } else {
        // not in progress, not aborted, not committed: crashed; locks gone
        Ok(LockmodeTest::Proceed)
    }
}

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
    // Chain-scope VM pin (C holds a possibly-stale vm page pin across chain
    // members; visibilitymap_pin swaps it when the map page differs).
    let mut vmb = visibilitymap::VmBuffer::new();
    'chain: loop {
        let block = ItemPointerGetBlockNumber(&tupid);
        let offnum = ItemPointerGetOffsetNumber(&tupid);
        let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
            .expect("ReadBuffer returned InvalidBuffer");

        'l4: loop {
            crate::check_for_interrupts()?;
            // Pin the VM page before taking the content lock (deadlock rule:
            // no IO under the lock); rechecked once locked.
            let pinned_desired_page = if pin.page().is_all_visible() {
                visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
                true
            } else {
                false
            };
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            if !pinned_desired_page && pin.page().is_all_visible() {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                visibilitymap::visibilitymap_pin(relation, block, &mut vmb)?;
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            }
            macro_rules! done {
                ($res:expr) => {{
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                    pin.release();
                    vmb.release();
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
                // wait_on set = C's goto l4 after XactLockTableWait; conflict
                // Updated/Deleted mapping (C's out_locked) stays with mytup.
                let mut wait_on = None;
                if (old_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    // Chain members postdate the caller's snapshot, so no
                    // pg_upgrade'd (HEAP_LOCKED_UPGRADED) multis here.
                    debug_assert!(!HEAP_LOCKED_UPGRADED(old_infomask));
                    let members =
                        collect_multixact_members(xmax, HEAP_XMAX_IS_LOCKED_ONLY(old_infomask))?;
                    for m in &members {
                        match test_lockmode_for_conflict(m.status, m.xid, mode)? {
                            LockmodeTest::SelfModified => {
                                // Already locked by us in a previous pass over
                                // this chain: skip stamping, keep following.
                                stamp = false;
                                break;
                            }
                            LockmodeTest::Wait => {
                                wait_on = Some(m.xid);
                                break;
                            }
                            LockmodeTest::ConflictCommitted => {
                                if mytup.t_self != mytup.t_data().t_ctid {
                                    done!(TM_Result::TM_Updated);
                                }
                                done!(TM_Result::TM_Deleted);
                            }
                            LockmodeTest::Proceed => {}
                        }
                    }
                } else {
                    let status = if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                        if HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                            MultiXactStatus::MultiXactStatusForKeyShare
                        } else if HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                            MultiXactStatus::MultiXactStatusForShare
                        } else if HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                            if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                                MultiXactStatus::MultiXactStatusForUpdate
                            } else {
                                MultiXactStatus::MultiXactStatusForNoKeyUpdate
                            }
                        } else {
                            return Err(Box::new(PgError::error("invalid lock status in tuple")));
                        }
                    } else if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                        MultiXactStatus::MultiXactStatusUpdate
                    } else {
                        MultiXactStatus::MultiXactStatusNoKeyUpdate
                    };
                    match test_lockmode_for_conflict(status, xmax, mode)? {
                        LockmodeTest::SelfModified => {
                            stamp = false;
                        }
                        LockmodeTest::Wait => {
                            wait_on = Some(xmax);
                        }
                        LockmodeTest::ConflictCommitted => {
                            if mytup.t_self != mytup.t_data().t_ctid {
                                done!(TM_Result::TM_Updated);
                            }
                            done!(TM_Result::TM_Deleted);
                        }
                        LockmodeTest::Proceed => {}
                    }
                }
                if let Some(xid) = wait_on {
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                    lmgr::XactLockTableWait(
                        xid,
                        Some(relation),
                        Some(&mytup.t_self),
                        ::types_storage::lock::XLTW_Oper::LockUpdated,
                    )?;
                    continue 'l4;
                }
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
                let mut cleared_all_frozen = false;
                if pin.page().is_all_visible()
                    && visibilitymap::visibilitymap_clear(
                        relation,
                        block,
                        &vmb,
                        visibilitymap::VISIBILITYMAP_ALL_FROZEN,
                    )?
                {
                    cleared_all_frozen = true;
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
                    xlrec[7] = if cleared_all_frozen {
                        XLH_LOCK_ALL_FROZEN_CLEARED
                    } else {
                        0
                    };
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
