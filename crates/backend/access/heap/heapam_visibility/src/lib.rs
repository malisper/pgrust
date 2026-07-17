#![allow(non_snake_case)]

use ::heapam::HeapTupleGetUpdateXid;
use ::procarray::TransactionIdIsInProgress;
use ::snapmgr::XidInMVCCSnapshot;
use ::tableam::TM_Result::{self, *};
use ::types_core::xact::{
    InvalidCommandId, InvalidTransactionId, TransactionIdFollowsOrEquals, TransactionIdIsNormal,
    TransactionIdIsValid, TransactionIdPrecedes,
};
use ::types_core::{Buffer, CommandId, GlobalVisStateHandle, InvalidOid, TransactionId};
use ::types_error::PgResult;
use ::types_snapshot::HTSV_Result::{self, *};
use ::types_snapshot::SnapshotData;
use ::types_snapshot::SnapshotType::*;
use ::types_snapshot::{
    XidVisMemo, XVM_COMMITTED, XVM_COMMIT_VALID, XVM_HINT_OK, XVM_IN_SNAPSHOT, XVM_SNAP_VALID,
};
use ::types_tuple::{
    HeapTupleData, HeapTupleHeaderData, ItemPointerEquals, ItemPointerIsValid,
    HEAP_LOCKED_UPGRADED, HEAP_MOVED_IN, HEAP_MOVED_OFF, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI, HEAP_XMAX_LOCK_ONLY, HEAP_XMIN_COMMITTED,
    HEAP_XMIN_INVALID,
};
use ::xact::TransactionIdIsCurrentTransactionId;

#[cfg(test)]
mod tests;

// Seamed while transam.c is in flight; collapse to a direct dep when it lands.
#[inline]
fn TransactionIdDidCommit(xid: TransactionId) -> PgResult<bool> {
    transam_seams::transaction_id_did_commit::call(xid)
}

#[inline]
fn HeapTupleHeaderGetCmin(tuple: &HeapTupleHeaderData) -> CommandId {
    combocid_seams::heap_tuple_header_get_cmin::call(tuple)
}

#[inline]
fn HeapTupleHeaderGetCmax(tuple: &HeapTupleHeaderData) -> CommandId {
    combocid_seams::heap_tuple_header_get_cmax::call(tuple)
}

#[inline]
fn MultiXactIdIsRunning(multi: TransactionId, is_lock_only: bool) -> PgResult<bool> {
    multixact_seams::multi_xact_id_is_running::call(multi, is_lock_only)
}

// C-exact `static inline` (heapam_visibility.c); per-tuple on hint-derivation.
#[inline]
fn SetHintBits(
    tuple: &mut HeapTupleHeaderData,
    buffer: Buffer,
    infomask: u16,
    xid: TransactionId,
) -> PgResult<()> {
    if TransactionIdIsValid(xid) {
        /* NB: xid must be known committed here! */
        let commit_lsn = transam_seams::transaction_id_get_commit_lsn::call(xid)?;

        if bufmgr_seams::buffer_is_permanent::call(buffer)
            && transam_xlog_seams::xlog_needs_flush::call(commit_lsn)
            && bufmgr_seams::buffer_get_lsn_atomic::call(buffer) < commit_lsn
        {
            /* not flushed and no LSN interlock, so don't set hint */
            return Ok(());
        }
    }

    tuple.t_infomask |= infomask;
    bufmgr_seams::mark_buffer_dirty_hint::call(buffer, true)
}

pub fn HeapTupleSetHintBits(
    tuple: &mut HeapTupleHeaderData,
    buffer: Buffer,
    infomask: u16,
    xid: TransactionId,
) -> PgResult<()> {
    SetHintBits(tuple, buffer, infomask, xid)
}

pub fn HeapTupleSatisfiesSelf(htup: &mut HeapTupleData<'_>, buffer: Buffer) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data_mut();

    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(false);
        }

        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(false);
            }
            if !TransactionIdIsInProgress(xvac)? {
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            }
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac)? {
                    return Ok(false);
                }
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.xmin_raw()) {
            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(true);
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                return Ok(true);
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple)?;

                debug_assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                return Ok(!TransactionIdIsCurrentTransactionId(xmax));
            }

            if !TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                return Ok(true);
            }

            return Ok(false);
        } else if TransactionIdIsInProgress(tuple.xmin_raw())? {
            return Ok(false);
        } else if TransactionIdDidCommit(tuple.xmin_raw())? {
            let raw_xmin = tuple.xmin_raw();
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(false);
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        return Ok(HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }

        let xmax = HeapTupleGetUpdateXid(tuple)?;

        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            return Ok(false);
        }
        if TransactionIdIsInProgress(xmax)? {
            return Ok(true);
        }
        if TransactionIdDidCommit(xmax)? {
            return Ok(false);
        }
        return Ok(true);
    }

    if TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
        return Ok(HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));
    }

    if TransactionIdIsInProgress(tuple.xmax_raw())? {
        return Ok(true);
    }

    if !TransactionIdDidCommit(tuple.xmax_raw())? {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    let raw_xmax = tuple.xmax_raw();
    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    Ok(false)
}

pub fn HeapTupleSatisfiesAny(_htup: &mut HeapTupleData<'_>, _buffer: Buffer) -> PgResult<bool> {
    Ok(true)
}

pub fn HeapTupleSatisfiesToast(htup: &mut HeapTupleData<'_>, buffer: Buffer) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data_mut();

    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(false);
        }

        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(false);
            }
            if !TransactionIdIsInProgress(xvac)? {
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            }
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac)? {
                    return Ok(false);
                }
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
            }
        } else if !TransactionIdIsValid(tuple.xmin()) {
            /* invalid xmin left by a canceled (super-deleted) speculative insertion */
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn HeapTupleSatisfiesUpdate(
    htup: &mut HeapTupleData<'_>,
    curcid: CommandId,
    buffer: Buffer,
) -> PgResult<TM_Result> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let t_self = htup.t_self;
    let tuple = htup.t_data_mut();

    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(TM_Invisible);
        }

        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(TM_Invisible);
            }
            if !TransactionIdIsInProgress(xvac)? {
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(TM_Invisible);
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            }
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac)? {
                    return Ok(TM_Invisible);
                }
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(TM_Invisible);
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.xmin_raw()) {
            if HeapTupleHeaderGetCmin(tuple) >= curcid {
                return Ok(TM_Invisible); /* inserted after scan started */
            }

            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(TM_Ok);
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                let xmax = tuple.xmax_raw();

                // Our own tuple may be locked by others: a key-share lock on
                // the prior version carries over on update.
                if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    if MultiXactIdIsRunning(xmax, true)? {
                        return Ok(TM_BeingModified);
                    }
                    return Ok(TM_Ok);
                }

                if !TransactionIdIsInProgress(xmax)? {
                    return Ok(TM_Ok);
                }
                return Ok(TM_BeingModified);
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple)?;

                debug_assert!(TransactionIdIsValid(xmax));

                if !TransactionIdIsCurrentTransactionId(xmax) {
                    /* deleting subtransaction must have aborted */
                    if MultiXactIdIsRunning(tuple.xmax_raw(), false)? {
                        return Ok(TM_BeingModified);
                    }
                    return Ok(TM_Ok);
                } else if HeapTupleHeaderGetCmax(tuple) >= curcid {
                    return Ok(TM_SelfModified); /* updated after scan started */
                } else {
                    return Ok(TM_Invisible); /* updated before scan started */
                }
            }

            if !TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                return Ok(TM_Ok);
            }

            if HeapTupleHeaderGetCmax(tuple) >= curcid {
                return Ok(TM_SelfModified);
            } else {
                return Ok(TM_Invisible);
            }
        } else if TransactionIdIsInProgress(tuple.xmin_raw())? {
            return Ok(TM_Invisible);
        } else if TransactionIdDidCommit(tuple.xmin_raw())? {
            let raw_xmin = tuple.xmin_raw();
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(TM_Invisible);
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(TM_Ok);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(TM_Ok);
        }
        if !ItemPointerEquals(&t_self, &tuple.t_ctid) {
            return Ok(TM_Updated);
        } else {
            return Ok(TM_Deleted);
        }
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_LOCKED_UPGRADED(tuple.t_infomask) {
            return Ok(TM_Ok);
        }

        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            if MultiXactIdIsRunning(tuple.xmax_raw(), true)? {
                return Ok(TM_BeingModified);
            }

            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(TM_Ok);
        }

        let xmax = HeapTupleGetUpdateXid(tuple)?;
        if !TransactionIdIsValid(xmax) && MultiXactIdIsRunning(tuple.xmax_raw(), false)? {
            return Ok(TM_BeingModified);
        }

        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= curcid {
                return Ok(TM_SelfModified);
            } else {
                return Ok(TM_Invisible);
            }
        }

        if MultiXactIdIsRunning(tuple.xmax_raw(), false)? {
            return Ok(TM_BeingModified);
        }

        if TransactionIdDidCommit(xmax)? {
            if !ItemPointerEquals(&t_self, &tuple.t_ctid) {
                return Ok(TM_Updated);
            }
            return Ok(TM_Deleted);
        }

        /* updater aborted or crashed; other members may still be running */

        if !MultiXactIdIsRunning(tuple.xmax_raw(), false)? {
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(TM_Ok);
        }

        return Ok(TM_BeingModified);
    }

    if TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(TM_BeingModified);
        }
        if HeapTupleHeaderGetCmax(tuple) >= curcid {
            return Ok(TM_SelfModified);
        } else {
            return Ok(TM_Invisible);
        }
    }

    if TransactionIdIsInProgress(tuple.xmax_raw())? {
        return Ok(TM_BeingModified);
    }

    if !TransactionIdDidCommit(tuple.xmax_raw())? {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(TM_Ok);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(TM_Ok);
    }

    let raw_xmax = tuple.xmax_raw();
    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    if !ItemPointerEquals(&t_self, &tuple.t_ctid) {
        Ok(TM_Updated)
    } else {
        Ok(TM_Deleted)
    }
}

pub fn HeapTupleSatisfiesDirty(
    htup: &mut HeapTupleData<'_>,
    snapshot: &mut SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data_mut();

    snapshot.xmin = InvalidTransactionId;
    snapshot.xmax = InvalidTransactionId;
    snapshot.speculativeToken = 0;

    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(false);
        }

        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(false);
            }
            if !TransactionIdIsInProgress(xvac)? {
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            }
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac)? {
                    return Ok(false);
                }
                if TransactionIdDidCommit(xvac)? {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.xmin_raw()) {
            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(true);
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                return Ok(true);
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple)?;

                debug_assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                return Ok(!TransactionIdIsCurrentTransactionId(xmax));
            }

            if !TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                return Ok(true);
            }

            return Ok(false);
        } else if TransactionIdIsInProgress(tuple.xmin_raw())? {
            // Hand the caller the speculative token; xmax is the caller's
            // concern since it needs a conclusively locked row anyway.
            if tuple.is_speculative() {
                snapshot.speculativeToken = tuple.speculative_token();
                debug_assert!(snapshot.speculativeToken != 0);
            }

            snapshot.xmin = tuple.xmin_raw();
            return Ok(true); /* in insertion by other */
        } else if TransactionIdDidCommit(tuple.xmin_raw())? {
            let raw_xmin = tuple.xmin_raw();
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(false);
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        return Ok(HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }

        let xmax = HeapTupleGetUpdateXid(tuple)?;

        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            return Ok(false);
        }
        if TransactionIdIsInProgress(xmax)? {
            snapshot.xmax = xmax;
            return Ok(true);
        }
        if TransactionIdDidCommit(xmax)? {
            return Ok(false);
        }
        return Ok(true);
    }

    if TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
        return Ok(HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));
    }

    if TransactionIdIsInProgress(tuple.xmax_raw())? {
        if !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            snapshot.xmax = tuple.xmax_raw();
        }
        return Ok(true);
    }

    if !TransactionIdDidCommit(tuple.xmax_raw())? {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    let raw_xmax = tuple.xmax_raw();
    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    Ok(false)
}

// The MVCC xid-status probes behind resolver seats: Direct is C's per-tuple
// evaluation; PageMemo resolves each distinct xid once per page-collect walk
// (batch visibility). Hint denials stay uncached (page LSN may advance).
trait MvccXidResolve {
    fn in_snapshot(&mut self, xid: TransactionId, snapshot: &SnapshotData<'_>) -> PgResult<bool>;
    fn did_commit(&mut self, xid: TransactionId) -> PgResult<bool>;
    fn set_hint_bits(
        &mut self,
        tuple: &mut HeapTupleHeaderData,
        buffer: Buffer,
        infomask: u16,
        xid: TransactionId,
    ) -> PgResult<()>;
}

struct DirectResolve;

impl MvccXidResolve for DirectResolve {
    #[inline(always)]
    fn in_snapshot(&mut self, xid: TransactionId, snapshot: &SnapshotData<'_>) -> PgResult<bool> {
        XidInMVCCSnapshot(xid, snapshot)
    }

    #[inline(always)]
    fn did_commit(&mut self, xid: TransactionId) -> PgResult<bool> {
        TransactionIdDidCommit(xid)
    }

    #[inline(always)]
    fn set_hint_bits(
        &mut self,
        tuple: &mut HeapTupleHeaderData,
        buffer: Buffer,
        infomask: u16,
        xid: TransactionId,
    ) -> PgResult<()> {
        SetHintBits(tuple, buffer, infomask, xid)
    }
}

struct PageMemoResolve<'a> {
    memo: &'a mut XidVisMemo,
}

impl MvccXidResolve for PageMemoResolve<'_> {
    #[inline]
    fn in_snapshot(&mut self, xid: TransactionId, snapshot: &SnapshotData<'_>) -> PgResult<bool> {
        // XidInMVCCSnapshot's first branch, hoisted: hinted pages resolve here
        // at the direct path's cost; the memo serves only >= xmin xids.
        if TransactionIdPrecedes(xid, snapshot.xmin) {
            return Ok(false);
        }
        let f = self.memo.get(xid);
        if f & XVM_SNAP_VALID != 0 {
            return Ok(f & XVM_IN_SNAPSHOT != 0);
        }
        let r = XidInMVCCSnapshot(xid, snapshot)?;
        self.memo.merge(xid, XVM_SNAP_VALID | if r { XVM_IN_SNAPSHOT } else { 0 });
        Ok(r)
    }

    #[inline]
    fn did_commit(&mut self, xid: TransactionId) -> PgResult<bool> {
        // TransactionLogFetch's permanent-xid arm, hoisted: non-normal xids
        // resolve at the direct path's cost and stay out of the memo, whose
        // empty slots are keyed xid == 0. A zero xid is REACHABLE here, not
        // corruption: heap_abort_speculative (heapam.c) sets a killed
        // speculative tuple's xmin to InvalidTransactionId without setting
        // HEAP_XMIN_INVALID, so the next MVCC scan of the page resolves raw
        // xmin == 0 — C's HeapTupleSatisfiesMVCC feeds it straight into
        // TransactionIdDidCommit, whose !TransactionIdIsNormal arm answers
        // "aborted" before touching its single-item cache (transam.c).
        if !TransactionIdIsNormal(xid) {
            return TransactionIdDidCommit(xid);
        }
        let f = self.memo.get(xid);
        if f & XVM_COMMIT_VALID != 0 {
            return Ok(f & XVM_COMMITTED != 0);
        }
        let r = TransactionIdDidCommit(xid)?;
        self.memo.merge(xid, XVM_COMMIT_VALID | if r { XVM_COMMITTED } else { 0 });
        Ok(r)
    }

    #[inline]
    fn set_hint_bits(
        &mut self,
        tuple: &mut HeapTupleHeaderData,
        buffer: Buffer,
        infomask: u16,
        xid: TransactionId,
    ) -> PgResult<()> {
        if TransactionIdIsValid(xid) && self.memo.get(xid) & XVM_HINT_OK == 0 {
            /* NB: xid must be known committed here! */
            let commit_lsn = transam_seams::transaction_id_get_commit_lsn::call(xid)?;
            if bufmgr_seams::buffer_is_permanent::call(buffer)
                && transam_xlog_seams::xlog_needs_flush::call(commit_lsn)
                && bufmgr_seams::buffer_get_lsn_atomic::call(buffer) < commit_lsn
            {
                return Ok(());
            }
            self.memo.merge(xid, XVM_HINT_OK);
        }
        tuple.t_infomask |= infomask;
        bufmgr_seams::mark_buffer_dirty_hint::call(buffer, true)
    }
}

pub fn HeapTupleSatisfiesMVCC(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    satisfies_mvcc_res(htup, snapshot, buffer, &mut DirectResolve)
}

pub fn HeapTupleSatisfiesMVCCPage(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
    memo: &mut XidVisMemo,
) -> PgResult<bool> {
    satisfies_mvcc_res(htup, snapshot, buffer, &mut PageMemoResolve { memo })
}

// inline(always): each resolver seat collapses into its wrapper, keeping the
// Direct seat's codegen the pre-generic concrete body (index-fetch lanes are
// sensitive to the extra call edge).
#[inline(always)]
fn satisfies_mvcc_res<R: MvccXidResolve>(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
    r: &mut R,
) -> PgResult<bool> {
    debug_assert!(snapshot.regd_count.get() > 0 || snapshot.active_count.get() > 0);
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data_mut();

    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(false);
        }

        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(false);
            }
            if !r.in_snapshot(xvac, snapshot)? {
                if r.did_commit(xvac)? {
                    r.set_hint_bits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
                r.set_hint_bits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            }
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if r.in_snapshot(xvac, snapshot)? {
                    return Ok(false);
                }
                if r.did_commit(xvac)? {
                    r.set_hint_bits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                } else {
                    r.set_hint_bits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                    return Ok(false);
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.xmin_raw()) {
            if HeapTupleHeaderGetCmin(tuple) >= snapshot.curcid.get() {
                return Ok(false);
            }

            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(true);
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                return Ok(true);
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple)?;

                debug_assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return Ok(true);
                } else if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid.get() {
                    return Ok(true); /* updated after scan started */
                } else {
                    return Ok(false); /* updated before scan started */
                }
            }

            if !TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
                /* deleting subtransaction must have aborted */
                r.set_hint_bits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                return Ok(true);
            }

            if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid.get() {
                return Ok(true); /* deleted after scan started */
            } else {
                return Ok(false); /* deleted before scan started */
            }
        } else if r.in_snapshot(tuple.xmin_raw(), snapshot)? {
            return Ok(false);
        } else if r.did_commit(tuple.xmin_raw())? {
            let raw_xmin = tuple.xmin_raw();
            r.set_hint_bits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            /* it must have aborted or crashed */
            r.set_hint_bits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(false);
        }
    } else {
        /* xmin is committed, but maybe not according to our snapshot */
        if !tuple.xmin_frozen() && r.in_snapshot(tuple.xmin_raw(), snapshot)? {
            return Ok(false); /* treat as still in progress */
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        debug_assert!(!HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));

        let xmax = HeapTupleGetUpdateXid(tuple)?;

        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid.get() {
                return Ok(true); /* deleted after scan started */
            } else {
                return Ok(false); /* deleted before scan started */
            }
        }
        if r.in_snapshot(xmax, snapshot)? {
            return Ok(true);
        }
        if r.did_commit(xmax)? {
            return Ok(false);
        }
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsCurrentTransactionId(tuple.xmax_raw()) {
            if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid.get() {
                return Ok(true); /* deleted after scan started */
            } else {
                return Ok(false); /* deleted before scan started */
            }
        }

        if r.in_snapshot(tuple.xmax_raw(), snapshot)? {
            return Ok(true);
        }

        if !r.did_commit(tuple.xmax_raw())? {
            /* it must have aborted or crashed */
            r.set_hint_bits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(true);
        }

        let raw_xmax = tuple.xmax_raw();
        r.set_hint_bits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    } else {
        /* xmax is committed, but maybe not according to our snapshot */
        if r.in_snapshot(tuple.xmax_raw(), snapshot)? {
            return Ok(true); /* treat as still in progress */
        }
    }

    /* xmax transaction committed */

    Ok(false)
}

pub fn HeapTupleSatisfiesVacuum(
    htup: &mut HeapTupleData<'_>,
    oldest_xmin: TransactionId,
    buffer: Buffer,
) -> PgResult<HTSV_Result> {
    let mut dead_after = InvalidTransactionId;

    let mut res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &mut dead_after)?;

    if res == HEAPTUPLE_RECENTLY_DEAD {
        debug_assert!(TransactionIdIsValid(dead_after));

        if TransactionIdPrecedes(dead_after, oldest_xmin) {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        debug_assert!(!TransactionIdIsValid(dead_after));
    }

    Ok(res)
}

pub fn HeapTupleSatisfiesVacuumHorizon(
    htup: &mut HeapTupleData<'_>,
    buffer: Buffer,
    dead_after: &mut TransactionId,
) -> PgResult<HTSV_Result> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data_mut();

    *dead_after = InvalidTransactionId;

    /* an aborted inserter means the tuple was never visible to anyone else */
    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return Ok(HEAPTUPLE_DEAD);
        } else if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
            }
            if TransactionIdIsInProgress(xvac)? {
                return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
            }
            if TransactionIdDidCommit(xvac)? {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(HEAPTUPLE_DEAD);
            }
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
        } else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.xvac();

            if TransactionIdIsCurrentTransactionId(xvac) {
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            }
            if TransactionIdIsInProgress(xvac)? {
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            }
            if TransactionIdDidCommit(xvac)? {
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
            } else {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(HEAPTUPLE_DEAD);
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.xmin_raw()) {
            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            }
            /* only locked? run infomask-only check first, for performance */
            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) || HeapTupleHeaderIsOnlyLocked(tuple)? {
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            }
            /* inserted and then deleted by same xact */
            if TransactionIdIsCurrentTransactionId(heapam::HeapTupleHeaderGetUpdateXid(tuple)?) {
                return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
            }
            /* deleting subtransaction must have aborted */
            return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
        } else if TransactionIdIsInProgress(tuple.xmin_raw())? {
            // INSERT_IN_PROGRESS without discerning DELETE: correct from other
            // backends' view, and callers should look at/wait on xmin (C note).
            return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
        } else if TransactionIdDidCommit(tuple.xmin_raw())? {
            let raw_xmin = tuple.xmin_raw();
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            /* not in progress, not committed: aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(HEAPTUPLE_DEAD);
        }

        /* xmin known committed here, but the hint bit may not have stuck */
    }

    /* the inserter committed; now what about the deleting transaction? */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(HEAPTUPLE_LIVE);
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        // Locker never updates: live either way, but hint XMAX_COMMITTED or
        // XMAX_INVALID once the xact is gone, for future readers.
        if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                /* a pre-pg_upgrade multixact cannot possibly be running */
                if !HEAP_LOCKED_UPGRADED(tuple.t_infomask)
                    && MultiXactIdIsRunning(tuple.xmax_raw(), true)?
                {
                    return Ok(HEAPTUPLE_LIVE);
                }
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            } else {
                if TransactionIdIsInProgress(tuple.xmax_raw())? {
                    return Ok(HEAPTUPLE_LIVE);
                }
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            }
        }

        return Ok(HEAPTUPLE_LIVE);
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = HeapTupleGetUpdateXid(tuple)?;

        debug_assert!(!HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));
        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsInProgress(xmax)? {
            return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
        } else if TransactionIdDidCommit(xmax)? {
            // Lockers may keep the multi running; report the update xid anyway
            // so below-horizon tuples stay prunable (remaining lockers also
            // appear in newer tuple versions).
            *dead_after = xmax;
            return Ok(HEAPTUPLE_RECENTLY_DEAD);
        } else if !MultiXactIdIsRunning(tuple.xmax_raw(), false)? {
            /* updater aborted or crashed, and no live members remain */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        }

        return Ok(HEAPTUPLE_LIVE);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsInProgress(tuple.xmax_raw())? {
            return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
        } else if TransactionIdDidCommit(tuple.xmax_raw())? {
            let raw_xmax = tuple.xmax_raw();
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
        } else {
            /* not in progress, not committed: aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(HEAPTUPLE_LIVE);
        }

        /* xmax known committed here, but the hint bit may not have stuck */
    }

    /* deleter committed; the caller compares dead_after with its horizon */
    *dead_after = tuple.xmax_raw();
    Ok(HEAPTUPLE_RECENTLY_DEAD)
}

pub fn HeapTupleSatisfiesNonVacuumable(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    let mut dead_after = InvalidTransactionId;

    let mut res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &mut dead_after)?;

    if res == HEAPTUPLE_RECENTLY_DEAD {
        debug_assert!(TransactionIdIsValid(dead_after));

        if procarray_seams::global_vis_test_is_removable_xid::call(snapshot.vistest, dead_after)? {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        debug_assert!(!TransactionIdIsValid(dead_after));
    }

    Ok(res != HEAPTUPLE_DEAD)
}

pub fn HeapTupleIsSurelyDead(
    htup: &HeapTupleData<'_>,
    vistest: GlobalVisStateHandle,
) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);
    let tuple = htup.t_data();

    // Consults neither procarray nor clog: unhinted states answer "in doubt"
    // (false), on the presumption the hint bits were set moments ago.
    if !tuple.xmin_committed() {
        return Ok(tuple.xmin_invalid());
    }

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(false);
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return Ok(false);
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        return Ok(false);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        return Ok(false);
    }

    procarray_seams::global_vis_test_is_removable_xid::call(vistest, tuple.xmax_raw())
}

pub fn HeapTupleHeaderIsOnlyLocked(tuple: &HeapTupleHeaderData) -> PgResult<bool> {
    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_LOCK_ONLY) != 0 {
        return Ok(true);
    }

    if !TransactionIdIsValid(tuple.xmax_raw()) {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) == 0 {
        return Ok(false);
    }

    /* a multi's updating xid may have aborted */
    let xmax = HeapTupleGetUpdateXid(tuple)?;

    debug_assert!(TransactionIdIsValid(xmax));

    if TransactionIdIsCurrentTransactionId(xmax) {
        return Ok(false);
    }
    if TransactionIdIsInProgress(xmax)? {
        return Ok(false);
    }
    if TransactionIdDidCommit(xmax)? {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
thread_local! {
    pub(crate) static TEST_HISTORIC_RLOCATOR:
        std::cell::Cell<Option<types_storage::RelFileLocator>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(not(test))]
fn historic_buffer_rlocator(buffer: Buffer) -> types_storage::RelFileLocator {
    let tag = bufmgr::BufferGetTag(buffer);
    types_storage::RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber)
}

#[cfg(test)]
fn historic_buffer_rlocator(_buffer: Buffer) -> types_storage::RelFileLocator {
    TEST_HISTORIC_RLOCATOR
        .with(|c| c.get())
        .expect("historic visibility test without an installed rlocator")
}

fn TransactionIdInArray(xid: TransactionId, xip: &[TransactionId]) -> bool {
    // xidComparator order (plain uint32).
    xip.binary_search(&xid).is_ok()
}

pub fn HeapTupleSatisfiesHistoricMVCC(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    let (xmin, raw_xmax, infomask, xmin_invalid, xmin_committed) = {
        let tuple = htup.t_data();
        (
            tuple.xmin(),
            tuple.xmax_raw(),
            tuple.t_infomask,
            tuple.xmin_invalid(),
            tuple.xmin_committed(),
        )
    };
    let mut xmax = raw_xmax;

    let subxip = &snapshot.subxip[..snapshot.subxcnt.max(0) as usize];
    let xip = &snapshot.xip[..snapshot.xcnt as usize];

    if xmin_invalid {
        return Ok(false);
    } else if TransactionIdInArray(xmin, subxip) {
        let rlocator = historic_buffer_rlocator(buffer);
        let resolved = ::reorderbuffer::ResolveCminCmaxDuringDecoding(
            ::snapmgr::HistoricSnapshotGetTupleCids().as_ref(),
            snapshot,
            htup,
            rlocator,
        )?;

        let Some((cmin, _cmax)) = resolved else {
            return Ok(false);
        };

        debug_assert!(cmin != InvalidCommandId);

        if cmin >= snapshot.curcid.get() {
            return Ok(false);
        }
    } else if TransactionIdPrecedes(xmin, snapshot.xmin) {
        debug_assert!(!(xmin_committed && !TransactionIdDidCommit(xmin)?));

        if !xmin_committed && !TransactionIdDidCommit(xmin)? {
            return Ok(false);
        }
    } else if TransactionIdFollowsOrEquals(xmin, snapshot.xmax) {
        return Ok(false);
    } else if TransactionIdInArray(xmin, xip) {
    } else {
        return Ok(false);
    }


    if (infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    } else if HEAP_XMAX_IS_LOCKED_ONLY(infomask) {
        return Ok(true);
    } else if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
        xmax = HeapTupleGetUpdateXid(htup.t_data())?;
    }

    if TransactionIdInArray(xmax, subxip) {
        let rlocator = historic_buffer_rlocator(buffer);
        let resolved = ::reorderbuffer::ResolveCminCmaxDuringDecoding(
            ::snapmgr::HistoricSnapshotGetTupleCids().as_ref(),
            snapshot,
            htup,
            rlocator,
        )?;

        let Some((_cmin, cmax)) = resolved else {
            return Ok(true);
        };
        if cmax == InvalidCommandId {
            return Ok(true);
        }

        Ok(cmax >= snapshot.curcid.get())
    } else if TransactionIdPrecedes(xmax, snapshot.xmin) {
        debug_assert!(
            !((infomask & HEAP_XMAX_COMMITTED) != 0 && !TransactionIdDidCommit(xmax)?)
        );

        if (infomask & HEAP_XMAX_COMMITTED) != 0 {
            return Ok(false);
        }

        Ok(!TransactionIdDidCommit(xmax)?)
    } else if TransactionIdFollowsOrEquals(xmax, snapshot.xmax) {
        Ok(true)
    } else if TransactionIdInArray(xmax, xip) {
        Ok(false)
    } else {
        Ok(true)
    }
}

pub fn HeapTupleSatisfiesVisibility(
    htup: &mut HeapTupleData<'_>,
    snapshot: &mut SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    match snapshot.snapshot_type {
        SNAPSHOT_MVCC => HeapTupleSatisfiesMVCC(htup, snapshot, buffer),
        SNAPSHOT_SELF => HeapTupleSatisfiesSelf(htup, buffer),
        SNAPSHOT_ANY => HeapTupleSatisfiesAny(htup, buffer),
        SNAPSHOT_TOAST => HeapTupleSatisfiesToast(htup, buffer),
        SNAPSHOT_DIRTY => HeapTupleSatisfiesDirty(htup, snapshot, buffer),
        SNAPSHOT_HISTORIC_MVCC => HeapTupleSatisfiesHistoricMVCC(htup, snapshot, buffer),
        SNAPSHOT_NON_VACUUMABLE => HeapTupleSatisfiesNonVacuumable(htup, snapshot, buffer),
    }
}

// Read-lane marshal for the &SnapshotData seam. DIRTY runs against a scratch
// snapshot and hands the write-back fields (xmin/xmax/speculativeToken) to
// the caller through the snapshot's dirty_* Cells; every probe overwrites
// them (C overwrites the plain fields per call the same way).
fn heap_tuple_satisfies_visibility_read(
    htup: &mut HeapTupleData<'_>,
    snapshot: &SnapshotData<'_>,
    buffer: Buffer,
) -> PgResult<bool> {
    match snapshot.snapshot_type {
        SNAPSHOT_MVCC => HeapTupleSatisfiesMVCC(htup, snapshot, buffer),
        SNAPSHOT_SELF => HeapTupleSatisfiesSelf(htup, buffer),
        SNAPSHOT_ANY => HeapTupleSatisfiesAny(htup, buffer),
        SNAPSHOT_TOAST => HeapTupleSatisfiesToast(htup, buffer),
        SNAPSHOT_DIRTY => DIRTY_SCRATCH.with(|cx| {
            let mut dirty = SnapshotData::sentinel(cx.mcx(), SNAPSHOT_DIRTY);
            let r = HeapTupleSatisfiesDirty(htup, &mut dirty, buffer)?;
            snapshot.dirty_xmin.set(dirty.xmin);
            snapshot.dirty_xmax.set(dirty.xmax);
            snapshot.dirty_speculative_token.set(dirty.speculativeToken);
            Ok(r)
        }),
        SNAPSHOT_HISTORIC_MVCC => HeapTupleSatisfiesHistoricMVCC(htup, snapshot, buffer),
        SNAPSHOT_NON_VACUUMABLE => HeapTupleSatisfiesNonVacuumable(htup, snapshot, buffer),
    }
}

thread_local! {
    static DIRTY_SCRATCH: ::mcx::MemoryContext = ::mcx::MemoryContext::new("DirtySnapshotScratch");
}

pub fn init_seams() {
    heapam_visibility_seams::heap_tuple_satisfies_visibility::set(
        heap_tuple_satisfies_visibility_read,
    );
    heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(HeapTupleSatisfiesMVCCPage);
    heapam_visibility_seams::heap_tuple_satisfies_dirty::set(HeapTupleSatisfiesDirty);
    heapam_visibility_seams::heap_tuple_satisfies_vacuum::set(HeapTupleSatisfiesVacuum);
    heapam_visibility_seams::heap_tuple_satisfies_update::set(HeapTupleSatisfiesUpdate);
    heapam_visibility_seams::heap_tuple_set_hint_bits::set(HeapTupleSetHintBits);
    heapam_visibility_seams::heap_tuple_is_surely_dead::set(HeapTupleIsSurelyDead);
    heapam_visibility_seams::heap_tuple_header_is_only_locked::set(HeapTupleHeaderIsOnlyLocked);
}
