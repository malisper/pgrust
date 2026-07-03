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
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
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
use ::xloginsert_seams::{XLogRegBuf, REGBUF_KEEP_DATA, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::hio::{RelationGetBufferForTuple, RelationPutHeapTuple, HEAP_INSERT_SPECULATIVE};
use crate::{unported, HeapTupleHeaderGetUpdateXid, MultiXactIdGetUpdateXid};
use heapam_visibility_seams as hv_seam;

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_INSERT_IS_SPECULATIVE: u8 = 1 << 2;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
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

fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
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
/// BulkInsertState is the multi_insert phase-3 lane.
pub fn heap_insert(
    relation: &RelationData<'_>,
    tup: &mut HeapTupleData<'_>,
    cid: CommandId,
    options: i32,
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

    let pin = RelationGetBufferForTuple(relation, heaptup.t_len as usize, None, options)?;

    predicate_seams::check_for_serializable_conflict_in::call(
        relation,
        None,
        InvalidBlockNumber,
    )?;

    RelationPutHeapTuple(relation, &pin, heaptup, (options & HEAP_INSERT_SPECULATIVE) != 0)?;

    if pin.page().is_all_visible() {
        unported("visibilitymap_clear (visibilitymap.c)");
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

        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: pin.buffer(),
                flags: bufflags,
                bufdata: &[&xlhdr, body],
            }],
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
    heap_insert(relation, tup, xact_seams::get_current_command_id::call(true)?, 0)
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

        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_HEAP_ID,
            XLOG_HEAP_DELETE,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: pin.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            }],
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

    let same_buf = oldbuf.buffer() == newbuf.buffer();
    let new_reg = XLogRegBuf {
        block_id: 0,
        buffer: newbuf.buffer(),
        flags: bufflags,
        bufdata: &[&xlhdr, body],
    };
    if same_buf {
        xloginsert_seams::xlog_insert_record::call(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[new_reg],
        )
    } else {
        xloginsert_seams::xlog_insert_record::call(
            RM_HEAP_ID,
            info,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec],
            &[
                new_reg,
                XLogRegBuf {
                    block_id: 1,
                    buffer: oldbuf.buffer(),
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
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

    if relation.rd_rel.relhasindex {
        unported("RelationGetIndexAttrBitmap (relcache index-attr bitmaps)");
    }
    // Empty hot/sum/key/id attr sets from here (indexless relation).

    let block = ItemPointerGetBlockNumber(otid);
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, block)?)
        .expect("ReadBuffer returned InvalidBuffer");
    if pin.page().is_all_visible() {
        unported("visibilitymap_clear write side (heap_update all-visible page, visibilitymap.c)");
    }
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

    // modified_attrs is empty; no key columns modified
    *lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let key_intact = true;
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

        if result != TM_Result::TM_Ok {
            break 'l2 result;
        }

        if pin.page().is_all_visible() {
            unported("visibilitymap_clear write side (heap_update all-visible page, visibilitymap.c)");
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
        if pin.page().is_all_visible() {
            unported("visibilitymap_clear (heap_update lock step, visibilitymap.c)");
        }

        bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

        if relation_needs_wal(relation) {
            let mut xlrec = [0u8; 8];
            xlrec[0..4].copy_from_slice(&xmax_lock_old_tuple.to_ne_bytes());
            xlrec[4..6].copy_from_slice(&ItemPointerGetOffsetNumber(&oldtup.t_self).to_ne_bytes());
            xlrec[6] = compute_infobits(oldtup.t_data().t_infomask, oldtup.t_data().t_infomask2);
            xlrec[7] = 0;
            let recptr = xloginsert_seams::xlog_insert_record::call(
                RM_HEAP_ID,
                XLOG_HEAP_LOCK,
                0,
                &[&xlrec],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: pin.buffer(),
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                }],
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
        // no hot-blocking attrs modified (empty sets)
        use_hot_update = true;
    } else {
        // SAFETY: pin + exclusive lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        pm.set_full();
    }

    let old_key_tuple = extract_replica_identity(relation, &oldtup, false);
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

    if pin.page().is_all_visible() {
        unported("visibilitymap_clear (heap_update old page, visibilitymap.c)");
    }
    if let Some(np) = &newpin {
        if np.page().is_all_visible() {
            unported("visibilitymap_clear (heap_update new page, visibilitymap.c)");
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
            false,
            false,
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
        TU_UpdateIndexes::TU_None
    } else {
        TU_UpdateIndexes::TU_All
    };

    let heaptup_self = heaptup.t_self;
    if toasted.is_some() {
        newtup.t_self = heaptup_self;
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
