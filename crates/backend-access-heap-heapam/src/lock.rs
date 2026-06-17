//! F4 (LOCK side) — heap tuple LOCK machinery (`access/heap/heapam.c`):
//! `heap_lock_tuple`, `heap_acquire_tuplock`, `heap_lock_updated_tuple` and its
//! recursive worker `heap_lock_updated_tuple_rec`, the lock-mode conflict
//! subroutine `test_lockmode_for_conflict`, and the multixact wait helpers
//! `DoesMultiXactIdConflict` / `MultiXactIdWait` / `ConditionalMultiXactIdWait`
//! (+ their shared core `Do_MultiXactIdWait`).
//!
//! Page model (the freespace.c / visibilitymap precedent, identical to F3
//! DELETE): the buffer manager owns the shared page. We pin + exclusively lock
//! the target buffer, materialize the on-page tuple header into `mcx` (C's
//! `tuple->t_data = PageGetItem(...)`), run all visibility / lock-wait /
//! xmax-compute logic on that materialized copy, and inside the critical
//! section write the mutated header back into the page (plus page-level flags)
//! through one `with_buffer_page` mutation.
//!
//! The heavyweight tuple lock (`LockTuple` / `UnlockTuple` /
//! `ConditionalLockTuple`) is taken through the lmgr seams; the regular and
//! conditional xact / multixact waits go through the lmgr seams and the
//! multixact owner. `heap_fetch` (the scan family, heapam.c) is reached through
//! an honest seam that panics until that family lands.

use mcx::Mcx;
use types_core::primitive::{BlockNumber, MultiXactId, Oid, TransactionId};
use types_core::xact::{CommandId, InvalidCommandId};
use types_error::{PgResult, ERRCODE_LOCK_NOT_AVAILABLE, ERROR};
use backend_utils_error::ereport;
use types_rel::{Relation, RelationData};
use types_snapshot::snapshot::{SnapshotData, SnapshotType};
use types_storage::lock::{
    XLTW_Oper, AccessExclusiveLock, AccessShareLock, ExclusiveLock, RowShareLock, LOCKMODE,
};
use types_storage::{Buffer, InvalidBuffer};
use types_tableam::tableam::{
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result,
};
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderChoice, HeapTupleHeaderData, ItemPointerData,
    HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_XMAX_COMMITTED,
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY,
};
use types_xlog_records::multixact::MultiXactStatus;

use backend_storage_page::{
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerIndicatesMovedPartitions, PageGetItem, PageGetItemId, PageRef,
};

use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HEAP_LOCKED_UPGRADED, HEAP_LOCK_MASK, HEAP_XMAX_IS_LOCKED_ONLY,
};
use backend_access_heap_heapam_visibility::htup::HeapTupleHeaderGetXmin;
use backend_access_heap_heapam_visibility::{
    HeapTupleHeaderGetUpdateXid, HeapTupleHeaderIsOnlyLocked, HeapTupleSatisfiesUpdate,
};
use backend_access_transam_transam::TransactionIdEquals;

use crate::{
    compute_infobits, xmax_infomask_changed, TUPLOCK_from_mxstatus, UpdateXmaxHintBits,
};
use crate::delete::{compute_new_xmax_infomask, HeapTupleHeaderGetCmax};

use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_heap_hio_seams as hio_seam;
use backend_access_heap_vacuumlazy_seams as page_seam;
use backend_access_transam_multixact_seams as multixact_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_lmgr_seams as lmgr_seam;
use backend_storage_lmgr_lock_seams as lock_seam;
use backend_utils_cache_relcache_seams as relcache_seam;

use types_wal::wal::{RM_HEAP_ID, RM_HEAP2_ID};
use types_wal::xloginsert::REGBUF_STANDARD;
use backend_rmgrdesc_next::heapdesc::{XLOG_HEAP_LOCK, XLOG_HEAP2_LOCK_UPDATED};
use types_xlog_records::heapam_xlog::{
    xl_heap_lock, xl_heap_lock_updated, SizeOfHeapLock, SizeOfHeapLockUpdated,
    XLH_LOCK_ALL_FROZEN_CLEARED,
};

// ---------------------------------------------------------------------------
// heapam-local vocabulary.
// ---------------------------------------------------------------------------

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

/// `BUFFER_LOCK_UNLOCK` (storage/bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `VISIBILITYMAP_ALL_FROZEN` (access/visibilitymapdefs.h).
const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;

/// `DEFAULT_LOCKMETHOD` (storage/lock.h).
const DEFAULT_LOCKMETHOD: u8 = 1;

/// `MaxMultiXactStatus + 1` — the multixact lock-strength table size.
/// `HEAP_XMAX_SHR_LOCK` (htup_details.h).
const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

/// `HEAP_XMAX_BITS` (htup_details.h).
const HEAP_XMAX_BITS: u16 =
    HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY;

// ===========================================================================
// heapam.h lock tables (htup_details.h / heapam.c static tables).
// ===========================================================================

/// `tupleLockExtraInfo[mode].hwlock` (heapam.c) — the heavyweight `LOCKMODE`
/// taken for a given `LockTupleMode`.
fn tuplock_hwlock(mode: LockTupleMode) -> LOCKMODE {
    match mode {
        LockTupleMode::LockTupleKeyShare => AccessShareLock,
        LockTupleMode::LockTupleShare => RowShareLock,
        LockTupleMode::LockTupleNoKeyExclusive => ExclusiveLock,
        LockTupleMode::LockTupleExclusive => AccessExclusiveLock,
    }
}

/// `LOCKMODE_from_mxstatus(status)` (heapam.c) — `tupleLockExtraInfo[
/// TUPLOCK_from_mxstatus(status)].hwlock`.
fn LOCKMODE_from_mxstatus(status: MultiXactStatus) -> LOCKMODE {
    tuplock_hwlock(TUPLOCK_from_mxstatus(status))
}

/// `ISUPDATE_from_mxstatus(status)` (multixact.h): `status > ForUpdate`.
fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    (status as i32) > (MultiXactStatus::ForUpdate as i32)
}

/// `get_mxact_status_for_lock(mode, is_update)` (heapam.c).
fn get_mxact_status_for_lock(mode: LockTupleMode, is_update: bool) -> PgResult<MultiXactStatus> {
    let retval: Option<MultiXactStatus> = if is_update {
        match mode {
            LockTupleMode::LockTupleKeyShare => None,
            LockTupleMode::LockTupleShare => None,
            LockTupleMode::LockTupleNoKeyExclusive => Some(MultiXactStatus::NoKeyUpdate),
            LockTupleMode::LockTupleExclusive => Some(MultiXactStatus::Update),
        }
    } else {
        match mode {
            LockTupleMode::LockTupleKeyShare => Some(MultiXactStatus::ForKeyShare),
            LockTupleMode::LockTupleShare => Some(MultiXactStatus::ForShare),
            LockTupleMode::LockTupleNoKeyExclusive => Some(MultiXactStatus::ForNoKeyUpdate),
            LockTupleMode::LockTupleExclusive => Some(MultiXactStatus::ForUpdate),
        }
    };
    retval.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("invalid lock tuple mode {}/{}", mode as i32, is_update))
            .into_error()
    })
}

/// `DoLockModesConflict(mode1, mode2)` (lock.c): `conflictTab[mode1] &
/// LOCKBIT_ON(mode2)`. The conflict table is read through the lock owner's
/// `conflict_tab` seam (the default lock method).
fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> PgResult<bool> {
    let conflicts = lock_seam::conflict_tab::call(DEFAULT_LOCKMETHOD, mode1);
    Ok((conflicts & types_storage::lock::LOCKBIT_ON(mode2)) != 0)
}

// ===========================================================================
// HEAP_XMAX_IS_*_LOCKED predicates (htup_details.h).
// ===========================================================================

fn HEAP_XMAX_IS_SHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_SHR_LOCK
}
fn HEAP_XMAX_IS_EXCL_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK
}
fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK
}

// ===========================================================================
// heap_acquire_tuplock — acquire the heavyweight tuple lock (heapam.c).
// ===========================================================================

/// `heap_acquire_tuplock(relation, tid, mode, wait_policy, &have_tuple_lock)`
/// (heapam.c). Returns the updated `*have_tuple_lock` (C's `bool` out param);
/// the `false` return (lock unavailable under Skip) is signalled by the
/// `LockWaitPolicy::LockWaitSkip` arm returning the *unchanged* `false`.
///
/// To preserve C's distinction (return value `bool` vs `*have_tuple_lock`),
/// this returns `(acquired: bool, have_tuple_lock: bool)`.
pub fn heap_acquire_tuplock<'mcx>(
    relation: &RelationData<'mcx>,
    tid: ItemPointerData,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    have_tuple_lock: bool,
) -> PgResult<(bool, bool)> {
    if have_tuple_lock {
        return Ok((true, true));
    }

    let hwlock = tuplock_hwlock(mode);
    match wait_policy {
        LockWaitPolicy::LockWaitBlock => {
            lmgr_seam::lock_tuple::call(relation.rd_id, tid, hwlock)?;
        }
        LockWaitPolicy::LockWaitSkip => {
            if !conditional_lock_tuple(relation.rd_id, tid, hwlock, false)? {
                return Ok((false, false));
            }
        }
        LockWaitPolicy::LockWaitError => {
            if !conditional_lock_tuple(relation.rd_id, tid, hwlock, log_lock_failures())? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                    .errmsg(format!(
                        "could not obtain lock on row in relation \"{}\"",
                        relation_get_relation_name(relation)
                    ))
                    .into_error());
            }
        }
    }

    Ok((true, true))
}

/// `UnlockTupleTuplock(relation, tid, mode)` (heapam.c macro over `UnlockTuple`)
/// — release the lmgr tuple lock taken by `heap_acquire_tuplock`. Installed as
/// the heapam-seams `unlock_tuple_tuplock`.
pub fn unlock_tuple_tuplock<'mcx>(
    relation: &RelationData<'mcx>,
    tid: ItemPointerData,
    mode: LockTupleMode,
) -> PgResult<()> {
    lmgr_seam::unlock_tuple::call(relation.rd_id, tid, tuplock_hwlock(mode))
}

/// `XactLockTableWait(xwait, rel, tid, oper)` (lmgr.c) — the heap-AM seam
/// wrapper routing to the lmgr `xact_lock_table_wait` seam with the address.
pub fn xact_lock_table_wait<'mcx>(
    xwait: TransactionId,
    rel: &RelationData<'mcx>,
    tid: ItemPointerData,
    oper: XLTW_Oper,
) -> PgResult<()> {
    lmgr_seam::xact_lock_table_wait::call(
        xwait,
        relation_get_relation_name(rel),
        tid,
        oper,
    )
}

/// `MultiXactIdWait` exposed for the heapam-seams `multi_xact_id_wait` install.
pub fn multi_xact_id_wait<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    rel: &Relation<'mcx>,
    ctid: ItemPointerData,
    oper: XLTW_Oper,
) -> PgResult<()> {
    MultiXactIdWait(mcx, multi, status, infomask, rel, ctid, oper)
}

// ===========================================================================
// heap_lock_tuple — lock a tuple in shared or exclusive mode (heapam.c).
// ===========================================================================

/// The result of [`heap_lock_tuple`] — C's `TM_Result` return plus the by-ptr
/// outputs (`*buffer` pinned-but-unlocked, `*tuple` filled, `*tmfd` on
/// failure).
#[derive(Clone, Debug)]
pub struct HeapLockResult<'mcx> {
    pub result: TM_Result,
    pub buffer: Buffer,
    pub tuple: HeapTupleData<'mcx>,
    pub tmfd: TM_FailureData,
}

/// `heap_lock_tuple(relation, tuple, cid, mode, wait_policy, follow_updates,
/// buffer, tmfd)` (heapam.c).
#[allow(clippy::too_many_arguments)]
pub fn heap_lock_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid_in: ItemPointerData,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    follow_updates: bool,
) -> PgResult<HeapLockResult<'mcx>> {
    let tid = tid_in;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let block = ItemPointerGetBlockNumber(&tid);
    let mut first_time = true;
    let mut skip_tuple_lock = false;
    let mut have_tuple_lock = false;
    let mut cleared_all_frozen = false;
    // `tmfd` is filled only on the failure paths (each builds its own); the
    // success exits pass the default.
    let tmfd = TM_FailureData::default();

    let buffer = hio_seam::read_buffer::call(relation.rd_id, block)?;

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.
     */
    if page_is_all_visible(buffer)? {
        vmbuffer = page_seam::visibilitymap_pin::call(relation.rd_id, block, vmbuffer)?;
    }

    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    // lp = PageGetItemId(page, off); Assert(ItemIdIsNormal(lp));
    // tuple = { t_data = PageGetItem(page, lp), t_len, t_tableOid }.
    let mut tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

    // We thread the final result/page-flag state through `result`. `'l3`
    // mirrors C's `goto l3` restart; `failed`/`out_locked`/`out_unlocked`
    // exits are mapped to early returns / a final block below.
    let result: TM_Result;
    'l3: loop {
        let mut result_l3 = HeapTupleSatisfiesUpdate(&mut tuple, cid, buffer)?;

        if result_l3 == TM_Result::TM_Invisible {
            // out_locked path: ON CONFLICT UPDATE can hit this.
            result = TM_Result::TM_Invisible;
            return finish_out_locked(buffer, vmbuffer, relation, &tuple, mode, have_tuple_lock, result, tmfd);
        } else if result_l3 == TM_Result::TM_BeingModified
            || result_l3 == TM_Result::TM_Updated
            || result_l3 == TM_Result::TM_Deleted
        {
            /* must copy state data before unlocking buffer */
            let xwait = HeapTupleHeaderGetRawXmax(data_ref(&tuple));
            let infomask = data_ref(&tuple).t_infomask;
            let infomask2 = data_ref(&tuple).t_infomask2;
            let t_ctid = data_ref(&tuple).t_ctid;

            lock_buffer_unlock(buffer)?;

            /*
             * If any subtransaction of our top transaction already holds a
             * lock as strong as or stronger than what we're requesting, we
             * effectively hold the desired lock already.
             */
            if first_time {
                first_time = false;

                if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let members =
                        multixact_seam::get_multi_xact_id_members::call(
                            mcx,
                            xwait,
                            false,
                            HEAP_XMAX_IS_LOCKED_ONLY(infomask),
                        )?;

                    let mut early: Option<TM_Result> = None;
                    for member in members.iter() {
                        /* only consider members of our own transaction */
                        if !xact_seam::transaction_id_is_current_transaction_id::call(member.xid) {
                            continue;
                        }
                        let status = member
                            .status
                            .expect("heap_lock_tuple: member with out-of-range status");
                        if (TUPLOCK_from_mxstatus(status) as i32) >= (mode as i32) {
                            early = Some(TM_Result::TM_Ok);
                            break;
                        } else {
                            /*
                             * Disable acquisition of the heavyweight tuple
                             * lock; we might deadlock otherwise.
                             */
                            skip_tuple_lock = true;
                        }
                    }
                    drop(members);
                    if let Some(res) = early {
                        // goto out_unlocked
                        return finish_out_unlocked(vmbuffer, relation, &tuple, mode, have_tuple_lock, res, tmfd, buffer);
                    }
                } else if xact_seam::transaction_id_is_current_transaction_id::call(xwait) {
                    match mode {
                        LockTupleMode::LockTupleKeyShare => {
                            debug_assert!(
                                HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)
                                    || HEAP_XMAX_IS_SHR_LOCKED(infomask)
                                    || HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                            );
                            return finish_out_unlocked(vmbuffer, relation, &tuple, mode, have_tuple_lock, TM_Result::TM_Ok, tmfd, buffer);
                        }
                        LockTupleMode::LockTupleShare => {
                            if HEAP_XMAX_IS_SHR_LOCKED(infomask) || HEAP_XMAX_IS_EXCL_LOCKED(infomask)
                            {
                                return finish_out_unlocked(vmbuffer, relation, &tuple, mode, have_tuple_lock, TM_Result::TM_Ok, tmfd, buffer);
                            }
                        }
                        LockTupleMode::LockTupleNoKeyExclusive => {
                            if HEAP_XMAX_IS_EXCL_LOCKED(infomask) {
                                return finish_out_unlocked(vmbuffer, relation, &tuple, mode, have_tuple_lock, TM_Result::TM_Ok, tmfd, buffer);
                            }
                        }
                        LockTupleMode::LockTupleExclusive => {
                            if HEAP_XMAX_IS_EXCL_LOCKED(infomask) && (infomask2 & HEAP_KEYS_UPDATED) != 0
                            {
                                return finish_out_unlocked(vmbuffer, relation, &tuple, mode, have_tuple_lock, TM_Result::TM_Ok, tmfd, buffer);
                            }
                        }
                    }
                }
            }

            /*
             * Initially assume that we will have to wait for the locking
             * transaction(s) to finish.
             */
            let mut require_sleep = true;
            if mode == LockTupleMode::LockTupleKeyShare {
                if (infomask2 & HEAP_KEYS_UPDATED) == 0 {
                    let updated = !HEAP_XMAX_IS_LOCKED_ONLY(infomask);

                    if follow_updates && updated && !ItemPointerEquals(&tuple.t_self, &t_ctid) {
                        let res = heap_lock_updated_tuple(
                            mcx,
                            relation,
                            infomask,
                            xwait,
                            &t_ctid,
                            xact_seam::get_current_transaction_id::call()?,
                            mode,
                        )?;
                        if res != TM_Result::TM_Ok {
                            /* recovery code expects to have buffer lock held */
                            bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                            // re-materialize for failed: path
                            tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                            result = res;
                            return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
                        }
                    }

                    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                    tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

                    /*
                     * Make sure it's still an appropriate lock, else start over.
                     */
                    if !HeapTupleHeaderIsOnlyLocked(data_ref(&tuple))?
                        && (((data_ref(&tuple).t_infomask2 & HEAP_KEYS_UPDATED) != 0) || !updated)
                    {
                        continue 'l3;
                    }

                    require_sleep = false;
                }
            } else if mode == LockTupleMode::LockTupleShare {
                if HEAP_XMAX_IS_LOCKED_ONLY(infomask) && !HEAP_XMAX_IS_EXCL_LOCKED(infomask) {
                    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                    tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                    if !HEAP_XMAX_IS_LOCKED_ONLY(data_ref(&tuple).t_infomask)
                        || HEAP_XMAX_IS_EXCL_LOCKED(data_ref(&tuple).t_infomask)
                    {
                        continue 'l3;
                    }
                    require_sleep = false;
                }
            } else if mode == LockTupleMode::LockTupleNoKeyExclusive {
                if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let conflict = DoesMultiXactIdConflict(mcx, xwait as MultiXactId, infomask, mode)?;
                    if !conflict.conflict {
                        bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                        tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                        if xmax_infomask_changed(data_ref(&tuple).t_infomask, infomask)
                            || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tuple)), xwait)
                        {
                            continue 'l3;
                        }
                        require_sleep = false;
                    }
                } else if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) {
                    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                    tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                    if xmax_infomask_changed(data_ref(&tuple).t_infomask, infomask)
                        || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tuple)), xwait)
                    {
                        continue 'l3;
                    }
                    require_sleep = false;
                }
            }

            /*
             * As an independent check, avoid sleeping if the current
             * transaction is the sole locker of the tuple.
             */
            if require_sleep
                && (infomask & HEAP_XMAX_IS_MULTI) == 0
                && xact_seam::transaction_id_is_current_transaction_id::call(xwait)
            {
                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                if xmax_infomask_changed(data_ref(&tuple).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tuple)), xwait)
                {
                    continue 'l3;
                }
                debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(data_ref(&tuple).t_infomask));
                require_sleep = false;
            }

            /*
             * Time to sleep on the other transaction/multixact, if necessary.
             */
            if require_sleep && (result_l3 == TM_Result::TM_Updated || result_l3 == TM_Result::TM_Deleted) {
                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                result = result_l3;
                return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
            } else if require_sleep {
                /*
                 * Acquire tuple lock to establish our priority for the tuple,
                 * or die trying.
                 */
                if !skip_tuple_lock {
                    let (acquired, htl) = heap_acquire_tuplock(relation, tid, mode, wait_policy, have_tuple_lock)?;
                    have_tuple_lock = htl;
                    if !acquired {
                        /* only with wait_policy Skip */
                        bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                        tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                        result = TM_Result::TM_WouldBlock;
                        return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
                    }
                }

                if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let status = get_mxact_status_for_lock(mode, false)?;
                    /* We only ever lock tuples, never update them */
                    if (status as i32) >= (MultiXactStatus::NoKeyUpdate as i32) {
                        return Err(elog_error("invalid lock mode in heap_lock_tuple"));
                    }

                    match wait_policy {
                        LockWaitPolicy::LockWaitBlock => {
                            MultiXactIdWait(mcx, xwait as MultiXactId, status, infomask, relation, tuple.t_self, XLTW_Oper::Lock)?;
                        }
                        LockWaitPolicy::LockWaitSkip => {
                            if !ConditionalMultiXactIdWait(mcx, xwait as MultiXactId, status, infomask, relation, false)? {
                                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                                tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                                result = TM_Result::TM_WouldBlock;
                                return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
                            }
                        }
                        LockWaitPolicy::LockWaitError => {
                            if !ConditionalMultiXactIdWait(mcx, xwait as MultiXactId, status, infomask, relation, log_lock_failures())? {
                                return Err(ereport(ERROR)
                                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                                    .errmsg(format!(
                                        "could not obtain lock on row in relation \"{}\"",
                                        relation_get_relation_name(relation)
                                    ))
                                    .into_error());
                            }
                        }
                    }
                } else {
                    /* wait for regular transaction to end, or die trying */
                    match wait_policy {
                        LockWaitPolicy::LockWaitBlock => {
                            heapam_seam::xact_lock_table_wait::call(xwait, relation, tuple.t_self, XLTW_Oper::Lock)?;
                        }
                        LockWaitPolicy::LockWaitSkip => {
                            if !lmgr_seam::conditional_xact_lock_table_wait::call(xwait, false)? {
                                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                                tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                                result = TM_Result::TM_WouldBlock;
                                return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
                            }
                        }
                        LockWaitPolicy::LockWaitError => {
                            if !lmgr_seam::conditional_xact_lock_table_wait::call(xwait, log_lock_failures())? {
                                return Err(ereport(ERROR)
                                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                                    .errmsg(format!(
                                        "could not obtain lock on row in relation \"{}\"",
                                        relation_get_relation_name(relation)
                                    ))
                                    .into_error());
                            }
                        }
                    }
                }

                /* if there are updates, follow the update chain */
                if follow_updates
                    && !HEAP_XMAX_IS_LOCKED_ONLY(infomask)
                    && !ItemPointerEquals(&tuple.t_self, &t_ctid)
                {
                    let res = heap_lock_updated_tuple(
                        mcx,
                        relation,
                        infomask,
                        xwait,
                        &t_ctid,
                        xact_seam::get_current_transaction_id::call()?,
                        mode,
                    )?;
                    if res != TM_Result::TM_Ok {
                        bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                        tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
                        result = res;
                        return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
                    }
                }

                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
                tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

                /*
                 * xwait is done; if it had just locked the tuple, restart.
                 */
                if xmax_infomask_changed(data_ref(&tuple).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tuple)), xwait)
                {
                    continue 'l3;
                }

                if (infomask & HEAP_XMAX_IS_MULTI) == 0 {
                    UpdateXmaxHintBits(data_mut(&mut tuple), buffer, xwait)?;
                }
            }

            /* By here, we hold buffer exclusive lock again */

            /*
             * We may lock if previous xmax aborted, or only locked without
             * updating, or if we didn't have to wait at all.
             */
            if !require_sleep
                || (data_ref(&tuple).t_infomask & HEAP_XMAX_INVALID) != 0
                || HEAP_XMAX_IS_LOCKED_ONLY(data_ref(&tuple).t_infomask)
                || HeapTupleHeaderIsOnlyLocked(data_ref(&tuple))?
            {
                result_l3 = TM_Result::TM_Ok;
            } else if !ItemPointerEquals(&tuple.t_self, &data_ref(&tuple).t_ctid) {
                result_l3 = TM_Result::TM_Updated;
            } else {
                result_l3 = TM_Result::TM_Deleted;
            }

            // require_sleep is consumed at the failed: path below.
            if result_l3 != TM_Result::TM_Ok {
                result = result_l3;
                return finish_failed(mcx, relation, buffer, vmbuffer, &mut tuple, tid, mode, have_tuple_lock, result, require_sleep, block, cid, &mut cleared_all_frozen);
            }
        }

        // result_l3 == TM_Ok and not in the BeingModified branch path that
        // returned via failed/out.

        /*
         * If we didn't pin the visibility map page and the page has become all
         * visible while we were busy locking the buffer (or during a subsequent
         * unlocked window), unlock + re-lock to avoid holding the buffer lock
         * across I/O, then start over (C's `goto l3`).
         */
        if vmbuffer == InvalidBuffer && page_is_all_visible(buffer)? {
            lock_buffer_unlock(buffer)?;
            vmbuffer = page_seam::visibilitymap_pin::call(relation.rd_id, block, vmbuffer)?;
            bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
            tuple = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
            continue 'l3;
        }

        // Stamp the locked tuple header, WAL it, and exit (C's path from
        // `xmax = ...` through `out_locked`).
        return commit_lock(
            mcx, relation, buffer, vmbuffer, &mut tuple, tid, block, mode,
            have_tuple_lock, &mut cleared_all_frozen,
        );
    }
}

/// The `failed:`/`out_locked:`/`out_unlocked:` tail of `heap_lock_tuple` when
/// the result is not `TM_Ok` and the buffer is exclusively locked: fill `tmfd`,
/// unlock, release, drop the tuple lock, return.
#[allow(clippy::too_many_arguments)]
fn finish_failed<'mcx>(
    _mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    buffer: Buffer,
    vmbuffer: Buffer,
    tuple: &mut HeapTupleData<'mcx>,
    _tid: ItemPointerData,
    mode: LockTupleMode,
    have_tuple_lock: bool,
    result: TM_Result,
    _require_sleep: bool,
    _block: BlockNumber,
    _cid: CommandId,
    _cleared_all_frozen: &mut bool,
) -> PgResult<HeapLockResult<'mcx>> {
    debug_assert!(
        result == TM_Result::TM_SelfModified
            || result == TM_Result::TM_Updated
            || result == TM_Result::TM_Deleted
            || result == TM_Result::TM_WouldBlock
    );
    let mut tmfd = TM_FailureData::default();
    tmfd.ctid = data_ref_h(tuple).t_ctid;
    tmfd.xmax = HeapTupleHeaderGetUpdateXid(data_ref_h(tuple))?;
    if result == TM_Result::TM_SelfModified {
        tmfd.cmax = HeapTupleHeaderGetCmax(data_ref_h(tuple));
    } else {
        tmfd.cmax = InvalidCommandId;
    }
    finish_out_locked(buffer, vmbuffer, relation, tuple, mode, have_tuple_lock, result, tmfd)
}

/// `out_locked:` — unlock the (exclusively-locked) buffer, release vmbuffer,
/// drop the lmgr tuple lock, return.
#[allow(clippy::too_many_arguments)]
fn finish_out_locked<'mcx>(
    buffer: Buffer,
    vmbuffer: Buffer,
    relation: &Relation<'mcx>,
    tuple: &HeapTupleData<'mcx>,
    mode: LockTupleMode,
    have_tuple_lock: bool,
    result: TM_Result,
    tmfd: TM_FailureData,
) -> PgResult<HeapLockResult<'mcx>> {
    lock_buffer_unlock(buffer)?;
    finish_out_unlocked(vmbuffer, relation, tuple, mode, have_tuple_lock, result, tmfd, buffer)
}

/// `out_unlocked:` — release vmbuffer, drop the lmgr tuple lock, return the
/// pinned-but-unlocked buffer.
#[allow(clippy::too_many_arguments)]
fn finish_out_unlocked<'mcx>(
    vmbuffer: Buffer,
    relation: &Relation<'mcx>,
    tuple: &HeapTupleData<'mcx>,
    mode: LockTupleMode,
    have_tuple_lock: bool,
    result: TM_Result,
    tmfd: TM_FailureData,
    buffer: Buffer,
) -> PgResult<HeapLockResult<'mcx>> {
    if vmbuffer != InvalidBuffer {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(vmbuffer);
    }
    /*
     * Now that we have marked the tuple locked, release the lmgr tuple lock.
     */
    if have_tuple_lock {
        lmgr_seam::unlock_tuple::call(relation.rd_id, tuple.t_self, tuplock_hwlock(mode))?;
    }
    Ok(HeapLockResult { result, buffer, tuple: tuple.clone(), tmfd })
}

/// The success tail of `heap_lock_tuple` from after the `failed:` label through
/// `out_locked:` — stamp the locked tuple header, WAL it, then exit.
#[allow(clippy::too_many_arguments)]
fn commit_lock<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    buffer: Buffer,
    vmbuffer: Buffer,
    tuple: &mut HeapTupleData<'mcx>,
    tid: ItemPointerData,
    block: BlockNumber,
    mode: LockTupleMode,
    have_tuple_lock: bool,
    cleared_all_frozen: &mut bool,
) -> PgResult<HeapLockResult<'mcx>> {
    // The page-became-all-visible `goto l3` window is handled by the caller
    // before invoking commit_lock.

    let xmax_in = HeapTupleHeaderGetRawXmax(data_ref_h(tuple));
    let old_infomask = data_ref_h(tuple).t_infomask;

    /*
     * If this is the first possibly-multixact-able operation, set my
     * per-backend OldestMemberMXactId.
     */
    multixact_seam::multi_xact_id_set_oldest_member::call()?;

    /*
     * Compute the new xmax and infomask.  Note we do not modify the tuple yet.
     */
    let (xid, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        mcx,
        xmax_in,
        old_infomask,
        data_ref_h(tuple).t_infomask2,
        xact_seam::get_current_transaction_id::call()?,
        mode,
        false,
    )?;

    // START_CRIT_SECTION()

    /*
     * Store transaction information of xact locking the tuple. Cmax is
     * meaningless here; don't set it.
     */
    {
        let hdr = data_mut_h(tuple);
        hdr.t_infomask &= !HEAP_XMAX_BITS;
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        hdr.t_infomask |= new_infomask;
        hdr.t_infomask2 |= new_infomask2;
        if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
            HeapTupleHeaderClearHotUpdated(hdr);
        }
        HeapTupleHeaderSetXmax(hdr, xid);
        /*
         * Make sure there is no forward chain link in t_ctid, but only if the
         * tuple was not updated (don't clobber the updater's t_ctid).
         */
        if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
            hdr.t_ctid = tid;
        }
    }

    /* Clear only the all-frozen bit on the VM if needed */
    if page_is_all_visible(buffer)?
        && page_seam::visibilitymap_clear::call(relation.rd_id, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN)?
    {
        *cleared_all_frozen = true;
    }

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);

    // Write the stamped header back into the on-page tuple.
    let offnum = ItemPointerGetOffsetNumber(&tuple.t_self);
    let header_image = data_ref_h(tuple).clone();
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let (off, len) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, item_id.lp_len() as usize)
        };
        let item = page_bytes
            .get_mut(off..off + len)
            .ok_or_else(|| types_error::PgError::error("item storage is outside page"))?;
        header_image.write_on_page(item)?;
        Ok(())
    })?;

    /* XLOG stuff */
    if relcache_seam::relation_needs_wal::call(relation) {
        let xlrec = xl_heap_lock {
            offnum: ItemPointerGetOffsetNumber(&tuple.t_self),
            xmax: xid,
            infobits_set: compute_infobits(new_infomask, data_ref_h(tuple).t_infomask2),
            flags: if *cleared_all_frozen { XLH_LOCK_ALL_FROZEN_CLEARED } else { 0 },
        };

        xloginsert_seam::xlog_begin_insert::call()?;
        xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapLock])?;

        /* we don't decode row locks atm, so no need to log the origin */

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_LOCK)?;
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()

    // out_locked: unlock + release vm + drop tuple lock.
    finish_out_locked(buffer, vmbuffer, relation, tuple, mode, have_tuple_lock, TM_Result::TM_Ok, TM_FailureData::default())
}

// ===========================================================================
// heap_lock_updated_tuple — follow the update chain (heapam.c).
// ===========================================================================

/// `heap_lock_updated_tuple(rel, prior_infomask, prior_raw_xmax, prior_ctid,
/// xid, mode)` (heapam.c).
#[allow(clippy::too_many_arguments)]
pub fn heap_lock_updated_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    prior_infomask: u16,
    prior_raw_xmax: TransactionId,
    prior_ctid: &ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> PgResult<TM_Result> {
    // INJECTION_POINT("heap_lock_updated_tuple", NULL) — no-op without the
    // injection-point framework.

    /*
     * If the tuple has moved into another partition (effectively a delete)
     * stop here.
     */
    if !ItemPointerIndicatesMovedPartitions(prior_ctid) {
        /*
         * If this is the first possibly-multixact-able operation in the
         * current transaction, set my per-backend OldestMemberMXactId.
         */
        multixact_seam::multi_xact_id_set_oldest_member::call()?;

        let prior_xmax = if (prior_infomask & HEAP_XMAX_IS_MULTI) != 0 {
            multixact_seam::multi_xact_id_get_update_xid::call(prior_raw_xmax, prior_infomask)?
        } else {
            prior_raw_xmax
        };
        return heap_lock_updated_tuple_rec(mcx, rel, prior_xmax, prior_ctid, xid, mode);
    }

    /* nothing to lock */
    Ok(TM_Result::TM_Ok)
}

/// `heap_lock_updated_tuple_rec(rel, priorXmax, tid, xid, mode)` (heapam.c) —
/// the iterative chain-walking worker (C uses tail recursion via a `for(;;)`).
fn heap_lock_updated_tuple_rec<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    mut prior_xmax: TransactionId,
    tid: &ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> PgResult<TM_Result> {
    let mut tupid = *tid;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let snapshot_any = SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY);

    let result;
    'outer: loop {
        let block = ItemPointerGetBlockNumber(&tupid);

        let fetched = heapam_seam::heap_fetch::call(mcx, rel, &snapshot_any, tupid, false)?;
        if !fetched.found {
            /*
             * If we fail to find the updated version, it was vacuumed/pruned
             * after its creator aborted. Behave as end-of-chain: success.
             */
            result = TM_Result::TM_Ok;
            // out_unlocked: no content lock held (heap_fetch released the
            // buffer on a miss because keep_buf=false).
            return finish_chain_unlocked(vmbuffer, result);
        }
        // heap_fetch (keep_buf=false) returns the visible tuple as an owned
        // FormedTuple; this chain-walker only needs the header (it re-reads the
        // on-page header under exclusive lock on every 'l4 pass via
        // reread_on_page_tuple). Take the owned HeapTupleData header out of the
        // FormedTuple, mirroring C's `mytup` seeded from `*tuple`.
        let fetched_tuple = fetched
            .tuple
            .expect("heap_fetch found==true must carry the tuple");
        let mut mytup = fetched_tuple.tuple.clone_in(mcx)?;
        let mut buf = fetched.userbuf;

        // 'l4 restart label.
        'l4: loop {
            // CHECK_FOR_INTERRUPTS() — owned by tcop/postgres.c.
            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            /*
             * Before locking the buffer, pin the VM page if it appears
             * necessary.
             */
            let pinned_desired_page;
            if page_is_all_visible(buf)? {
                vmbuffer = page_seam::visibilitymap_pin::call(rel.rd_id, block, vmbuffer)?;
                pinned_desired_page = true;
            } else {
                pinned_desired_page = false;
            }

            bufmgr_seam::lock_buffer_exclusive::call(buf)?;
            mytup = reread_on_page_tuple(mcx, rel.rd_id, buf, mytup.t_self)?;

            /*
             * If we didn't pin the VM page and the page became all visible
             * while locking, unlock and re-lock to avoid I/O under lock.
             */
            if !pinned_desired_page && page_is_all_visible(buf)? {
                lock_buffer_unlock(buf)?;
                vmbuffer = page_seam::visibilitymap_pin::call(rel.rd_id, block, vmbuffer)?;
                bufmgr_seam::lock_buffer_exclusive::call(buf)?;
                mytup = reread_on_page_tuple(mcx, rel.rd_id, buf, mytup.t_self)?;
            }

            /*
             * Check the tuple XMIN against prior XMAX, if any. End of chain ->
             * success.
             */
            if TransactionIdIsValid(prior_xmax)
                && !TransactionIdEquals(HeapTupleHeaderGetXmin(data_ref_h(&mytup)), prior_xmax)
            {
                result = TM_Result::TM_Ok;
                return finish_chain_locked(buf, vmbuffer, result);
            }

            /*
             * Also check Xmin: if created by an aborted (sub)xact, we already
             * locked the last live one. Done -> success.
             */
            if transaction_id_did_abort(HeapTupleHeaderGetXmin(data_ref_h(&mytup)))? {
                result = TM_Result::TM_Ok;
                return finish_chain_locked(buf, vmbuffer, result);
            }

            let old_infomask = data_ref_h(&mytup).t_infomask;
            let old_infomask2 = data_ref_h(&mytup).t_infomask2;
            let xmax = HeapTupleHeaderGetRawXmax(data_ref_h(&mytup));

            /*
             * If this version was updated/locked by concurrent xacts, decide
             * based on conflicts.
             */
            let mut go_next = false;
            if (old_infomask & HEAP_XMAX_INVALID) == 0 {
                let rawxmax = HeapTupleHeaderGetRawXmax(data_ref_h(&mytup));
                if (old_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    debug_assert!(!HEAP_LOCKED_UPGRADED(data_ref_h(&mytup).t_infomask));

                    let members = multixact_seam::get_multi_xact_id_members::call(
                        mcx,
                        rawxmax,
                        false,
                        HEAP_XMAX_IS_LOCKED_ONLY(old_infomask),
                    )?;
                    let mut handled = false;
                    for member in members.iter() {
                        let mstatus = member
                            .status
                            .expect("heap_lock_updated_tuple_rec: member out-of-range status");
                        let (res, needwait) =
                            test_lockmode_for_conflict(mstatus, member.xid, mode, &mytup)?;

                        if res == TM_Result::TM_SelfModified {
                            /* skip; we already hold the lock on this version */
                            go_next = true;
                            handled = true;
                            break;
                        }

                        if needwait {
                            lock_buffer_unlock(buf)?;
                            heapam_seam::xact_lock_table_wait::call(member.xid, rel, mytup.t_self, XLTW_Oper::LockUpdated)?;
                            handled = true;
                            // goto l4
                            break;
                        }
                        if res != TM_Result::TM_Ok {
                            result = res;
                            drop(members);
                            return finish_chain_locked(buf, vmbuffer, result);
                        }
                    }
                    drop(members);
                    if handled && !go_next {
                        // goto l4 (the needwait case)
                        continue 'l4;
                    }
                } else {
                    /*
                     * For a non-multi Xmax, compute the MultiXactStatus from
                     * the infomask bits.
                     */
                    let status = if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                        if HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                            MultiXactStatus::ForKeyShare
                        } else if HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                            MultiXactStatus::ForShare
                        } else if HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                            if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                                MultiXactStatus::ForUpdate
                            } else {
                                MultiXactStatus::ForNoKeyUpdate
                            }
                        } else {
                            return Err(elog_error("invalid lock status in tuple"));
                        }
                    } else if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                        MultiXactStatus::Update
                    } else {
                        MultiXactStatus::NoKeyUpdate
                    };

                    let (res, needwait) = test_lockmode_for_conflict(status, rawxmax, mode, &mytup)?;
                    if res == TM_Result::TM_SelfModified {
                        go_next = true;
                    } else if needwait {
                        lock_buffer_unlock(buf)?;
                        heapam_seam::xact_lock_table_wait::call(rawxmax, rel, mytup.t_self, XLTW_Oper::LockUpdated)?;
                        continue 'l4;
                    } else if res != TM_Result::TM_Ok {
                        result = res;
                        return finish_chain_locked(buf, vmbuffer, result);
                    }
                }
            }

            if !go_next {
                /* compute the new Xmax and infomask values for the tuple */
                let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
                    mcx,
                    xmax,
                    old_infomask,
                    data_ref_h(&mytup).t_infomask2,
                    xid,
                    mode,
                    false,
                )?;

                let mut cleared_all_frozen = false;
                if page_is_all_visible(buf)?
                    && page_seam::visibilitymap_clear::call(rel.rd_id, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN)?
                {
                    cleared_all_frozen = true;
                }

                // START_CRIT_SECTION()
                {
                    let hdr = data_mut_h(&mut mytup);
                    HeapTupleHeaderSetXmax(hdr, new_xmax);
                    hdr.t_infomask &= !HEAP_XMAX_BITS;
                    hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
                    hdr.t_infomask |= new_infomask;
                    hdr.t_infomask2 |= new_infomask2;
                }

                backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);

                // Write the stamped header back into the page.
                let offnum = ItemPointerGetOffsetNumber(&mytup.t_self);
                let header_image = data_ref_h(&mytup).clone();
                bufmgr_seam::with_buffer_page::call(buf, &mut |page_bytes| {
                    let (off, len) = {
                        let page = PageRef::new(page_bytes)?;
                        let item_id = PageGetItemId(&page, offnum)?;
                        (item_id.lp_off() as usize, item_id.lp_len() as usize)
                    };
                    let item = page_bytes
                        .get_mut(off..off + len)
                        .ok_or_else(|| types_error::PgError::error("item storage is outside page"))?;
                    header_image.write_on_page(item)?;
                    Ok(())
                })?;

                /* XLOG stuff */
                if relcache_seam::relation_needs_wal::call(rel) {
                    let xlrec = xl_heap_lock_updated {
                        offnum: ItemPointerGetOffsetNumber(&mytup.t_self),
                        xmax: new_xmax,
                        infobits_set: compute_infobits(new_infomask, new_infomask2),
                        flags: if cleared_all_frozen { XLH_LOCK_ALL_FROZEN_CLEARED } else { 0 },
                    };

                    xloginsert_seam::xlog_begin_insert::call()?;
                    xloginsert_seam::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
                    let recbuf = xlrec.to_bytes();
                    xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapLockUpdated])?;

                    let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED)?;
                    bufmgr_seam::page_set_lsn::call(buf, recptr)?;
                }
                // END_CRIT_SECTION()
            }

            // next:
            /* if we find the end of update chain, we're done */
            if (data_ref_h(&mytup).t_infomask & HEAP_XMAX_INVALID) != 0
                || HeapTupleHeaderIndicatesMovedPartitions(data_ref_h(&mytup))
                || ItemPointerEquals(&mytup.t_self, &data_ref_h(&mytup).t_ctid)
                || HeapTupleHeaderIsOnlyLocked(data_ref_h(&mytup))?
            {
                result = TM_Result::TM_Ok;
                return finish_chain_locked(buf, vmbuffer, result);
            }

            /* tail recursion */
            prior_xmax = HeapTupleHeaderGetUpdateXid(data_ref_h(&mytup))?;
            tupid = data_ref_h(&mytup).t_ctid;
            bufmgr_seam::unlock_release_buffer::call(buf);
            let _ = &mut buf;
            continue 'outer;
        }
    }
}

/// `out_locked:` for the chain walker — `UnlockReleaseBuffer(buf)`, release vm.
fn finish_chain_locked<'mcx>(
    buf: Buffer,
    vmbuffer: Buffer,
    result: TM_Result,
) -> PgResult<TM_Result> {
    bufmgr_seam::unlock_release_buffer::call(buf);
    if vmbuffer != InvalidBuffer {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(vmbuffer);
    }
    Ok(result)
}

/// `out_unlocked:` for the chain walker — release vm only (buffer already gone).
fn finish_chain_unlocked<'mcx>(vmbuffer: Buffer, result: TM_Result) -> PgResult<TM_Result> {
    if vmbuffer != InvalidBuffer {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(vmbuffer);
    }
    Ok(result)
}

// ===========================================================================
// test_lockmode_for_conflict — subroutine for the chain walker (heapam.c).
// ===========================================================================

/// `test_lockmode_for_conflict(status, xid, mode, tup, &needwait)` (heapam.c).
/// Returns `(TM_Result, needwait)`.
fn test_lockmode_for_conflict<'mcx>(
    status: MultiXactStatus,
    xid: TransactionId,
    mode: LockTupleMode,
    tup: &HeapTupleData<'mcx>,
) -> PgResult<(TM_Result, bool)> {
    let wantedstatus = get_mxact_status_for_lock(mode, false)?;

    /*
     * Note: we *must* check TransactionIdIsInProgress before
     * TransactionIdDidAbort/Commit.
     */
    if xact_seam::transaction_id_is_current_transaction_id::call(xid) {
        /* already locked by our own transaction */
        Ok((TM_Result::TM_SelfModified, false))
    } else if transaction_id_is_in_progress(xid)? {
        let needwait = DoLockModesConflict(LOCKMODE_from_mxstatus(status), LOCKMODE_from_mxstatus(wantedstatus))?;
        Ok((TM_Result::TM_Ok, needwait))
    } else if transaction_id_did_abort(xid)? {
        Ok((TM_Result::TM_Ok, false))
    } else if transaction_id_did_commit(xid)? {
        /*
         * Committed. If only a locker, lock is gone. If an update, depends on
         * conflict.
         */
        if !ISUPDATE_from_mxstatus(status) {
            return Ok((TM_Result::TM_Ok, false));
        }
        if DoLockModesConflict(LOCKMODE_from_mxstatus(status), LOCKMODE_from_mxstatus(wantedstatus))? {
            if !ItemPointerEquals(&tup.t_self, &data_ref_h(tup).t_ctid) {
                return Ok((TM_Result::TM_Updated, false));
            } else {
                return Ok((TM_Result::TM_Deleted, false));
            }
        }
        Ok((TM_Result::TM_Ok, false))
    } else {
        /* Not in progress, not aborted, not committed -- must have crashed */
        Ok((TM_Result::TM_Ok, false))
    }
}

// ===========================================================================
// DoesMultiXactIdConflict / MultiXactIdWait / ConditionalMultiXactIdWait
// (heapam.c).
// ===========================================================================

/// The result of [`DoesMultiXactIdConflict`] — C's `bool` return + the
/// `*current_is_member` out param.
pub use types_storage::multixact::MultiXactConflict;

/// `DoesMultiXactIdConflict(multi, infomask, lockmode, &current_is_member)`
/// (heapam.c). The repo's callers always pass a non-NULL `current_is_member`
/// (delete) or NULL (lock_tuple); here we always compute it (harmless extra
/// loop iteration), mirroring the `current_is_member != NULL` cases.
pub fn DoesMultiXactIdConflict<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    infomask: u16,
    lockmode: LockTupleMode,
) -> PgResult<MultiXactConflict> {
    let mut result = false;
    let mut current_is_member = false;
    let wanted = tuplock_hwlock(lockmode);

    if HEAP_LOCKED_UPGRADED(infomask) {
        return Ok(MultiXactConflict { conflict: false, current_is_member: false });
    }

    let members = multixact_seam::get_multi_xact_id_members::call(
        mcx,
        multi,
        false,
        HEAP_XMAX_IS_LOCKED_ONLY(infomask),
    )?;

    for member in members.iter() {
        // C: if (result && (current_is_member == NULL || *current_is_member))
        //        break;  — we always track current_is_member, so break when both.
        if result && current_is_member {
            break;
        }

        let status = member
            .status
            .expect("DoesMultiXactIdConflict: member out-of-range status");
        let memlockmode = LOCKMODE_from_mxstatus(status);

        let memxid = member.xid;
        /* ignore members from current xact (but track presence) */
        if xact_seam::transaction_id_is_current_transaction_id::call(memxid) {
            current_is_member = true;
            continue;
        } else if result {
            continue;
        }

        /* ignore members that don't conflict with the wanted lock */
        if !DoLockModesConflict(memlockmode, wanted)? {
            continue;
        }

        if ISUPDATE_from_mxstatus(status) {
            /* ignore aborted updaters */
            if transaction_id_did_abort(memxid)? {
                continue;
            }
        } else {
            /* ignore lockers-only no longer in progress */
            if !transaction_id_is_in_progress(memxid)? {
                continue;
            }
        }

        result = true;
    }

    drop(members);
    Ok(MultiXactConflict { conflict: result, current_is_member })
}

/// `Do_MultiXactIdWait(multi, status, infomask, nowait, rel, ctid, oper,
/// remaining, logLockFailure)` (heapam.c). Returns the C `bool` (success).
/// `remaining` (number still running) is computed but discarded — every repo
/// caller passes NULL or ignores it.
#[allow(clippy::too_many_arguments)]
fn Do_MultiXactIdWait<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    nowait: bool,
    rel: &Relation<'mcx>,
    ctid: ItemPointerData,
    oper: XLTW_Oper,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let mut result = true;

    /* for pre-pg_upgrade tuples, no need to sleep */
    if HEAP_LOCKED_UPGRADED(infomask) {
        return Ok(true);
    }

    let members = multixact_seam::get_multi_xact_id_members::call(
        mcx,
        multi,
        false,
        HEAP_XMAX_IS_LOCKED_ONLY(infomask),
    )?;

    for member in members.iter() {
        let memxid = member.xid;
        let memstatus = member
            .status
            .expect("Do_MultiXactIdWait: member out-of-range status");

        if xact_seam::transaction_id_is_current_transaction_id::call(memxid) {
            continue;
        }

        if !DoLockModesConflict(LOCKMODE_from_mxstatus(memstatus), LOCKMODE_from_mxstatus(status))? {
            continue;
        }

        if nowait {
            result = lmgr_seam::conditional_xact_lock_table_wait::call(memxid, log_lock_failure)?;
            if !result {
                break;
            }
        } else {
            heapam_seam::xact_lock_table_wait::call(memxid, rel, ctid, oper)?;
        }
    }

    drop(members);
    Ok(result)
}

/// `MultiXactIdWait(multi, status, infomask, rel, ctid, oper, remaining)`
/// (heapam.c).
fn MultiXactIdWait<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    rel: &Relation<'mcx>,
    ctid: ItemPointerData,
    oper: XLTW_Oper,
) -> PgResult<()> {
    let _ = Do_MultiXactIdWait(mcx, multi, status, infomask, false, rel, ctid, oper, false)?;
    Ok(())
}

/// `ConditionalMultiXactIdWait(multi, status, infomask, rel, remaining,
/// logLockFailure)` (heapam.c).
fn ConditionalMultiXactIdWait<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: u16,
    rel: &Relation<'mcx>,
    log_lock_failure: bool,
) -> PgResult<bool> {
    Do_MultiXactIdWait(
        mcx,
        multi,
        status,
        infomask,
        true,
        rel,
        ItemPointerData::default(),
        XLTW_Oper::None,
        log_lock_failure,
    )
}

// ===========================================================================
// Small helpers (shared idioms with delete.rs).
// ===========================================================================

/// Materialize the on-page tuple at `(buffer, tid)` into `mcx`.
fn read_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<HeapTupleData<'mcx>> {
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<(HeapTupleHeaderData<'mcx>, u32)> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        debug_assert!(item_id.has_storage());
        let item = PageGetItem(&page, &item_id)?;
        let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
        out = Some((hdr, item.len() as u32));
        Ok(())
    })?;
    let (hdr, t_len) = out.expect("with_buffer_page closure must have run");
    Ok(HeapTupleData {
        t_len,
        t_self: tid,
        t_tableOid: rel_id,
        t_data: Some(mcx::alloc_in(mcx, hdr)?),
    })
}

/// Re-read the on-page tuple header into an existing `HeapTupleData`'s identity.
fn reread_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<HeapTupleData<'mcx>> {
    read_on_page_tuple(mcx, rel_id, buffer, tid)
}

/// `tup->t_data` (shared).
fn data_ref<'a, 'mcx>(tuple: &'a HeapTupleData<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tuple.t_data.as_ref().expect("heap_lock_tuple: tuple has no t_data")
}
fn data_ref_h<'a, 'mcx>(tuple: &'a HeapTupleData<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tuple.t_data.as_ref().expect("heap_lock_tuple: tuple has no t_data")
}
/// `tup->t_data` (mutable).
fn data_mut<'a, 'mcx>(tuple: &'a mut HeapTupleData<'mcx>) -> &'a mut HeapTupleHeaderData<'mcx> {
    tuple.t_data.as_mut().expect("heap_lock_tuple: tuple has no t_data")
}
fn data_mut_h<'a, 'mcx>(tuple: &'a mut HeapTupleData<'mcx>) -> &'a mut HeapTupleHeaderData<'mcx> {
    tuple.t_data.as_mut().expect("heap_lock_tuple: tuple has no t_data")
}

/// `LockBuffer(buffer, BUFFER_LOCK_UNLOCK)`.
fn lock_buffer_unlock(buffer: Buffer) -> PgResult<()> {
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)
}

/// `PageIsAllVisible(BufferGetPage(buffer))` across the boundary.
fn page_is_all_visible(buffer: Buffer) -> PgResult<bool> {
    page_seam::page_is_all_visible::call(buffer)
}

/// `TransactionIdIsValid(xid)`.
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsInProgress(xid)` — procarray lookup.
fn transaction_id_is_in_progress(xid: TransactionId) -> PgResult<bool> {
    backend_storage_ipc_procarray_seams::transaction_id_is_in_progress::call(xid)
}

/// `TransactionIdDidCommit(xid)` — clog lookup, threading TransactionXmin.
fn transaction_id_did_commit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    backend_access_transam_transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)
}

/// `TransactionIdDidAbort(xid)` — clog lookup, threading TransactionXmin.
fn transaction_id_did_abort(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    backend_access_transam_transam_seams::transaction_id_did_abort::call(xid, transaction_xmin)
}

/// `ConditionalLockTuple(rel, tid, mode, log)` via the lmgr seam.
fn conditional_lock_tuple(
    relid: Oid,
    tid: ItemPointerData,
    mode: LOCKMODE,
    log_lock_failure: bool,
) -> PgResult<bool> {
    lmgr_seam::conditional_lock_tuple::call(relid, tid, mode, log_lock_failure)
}

/// `log_lock_failures` GUC (guc_tables.c) — read its installed value.
fn log_lock_failures() -> bool {
    backend_utils_misc_guc_tables::vars::log_lock_failures.read()
}

/// `RelationGetRelationName(relation)`.
fn relation_get_relation_name(relation: &RelationData<'_>) -> String {
    relation.rd_rel.relname.as_str().to_string()
}

// --- header setters (htup_details.h inline functions) ----------------------

fn HeapTupleHeaderSetXmax(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmax = xid;
    }
}

fn HeapTupleHeaderClearHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 &= !HEAP_HOT_UPDATED;
}

/// `HeapTupleHeaderIndicatesMovedPartitions(tup)` —
/// `ItemPointerIndicatesMovedPartitions(&tup->t_ctid)`.
fn HeapTupleHeaderIndicatesMovedPartitions(hdr: &HeapTupleHeaderData<'_>) -> bool {
    ItemPointerIndicatesMovedPartitions(&hdr.t_ctid)
}

/// `elog(ERROR, msg)` builder.
fn elog_error(msg: &str) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}
