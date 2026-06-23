#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
//! Port of `src/backend/access/heap/heapam_visibility.c` — the tuple-visibility
//! rules for heap tuples (the `HeapTupleSatisfies*` family), over the owned
//! [`HeapTupleData`] / [`SnapshotData`].
//!
//! Every visibility routine is transliterated branch-for-branch from the C
//! source: the hint-bit handling, branch order, the `Assert` checks (as
//! `debug_assert!`), and the return values. `XidInMVCCSnapshot` (snapmgr.c) and
//! `TransactionIdInArray` (the sorted-array binary search the historic path
//! relies on) are ported here too, since they belong to the visibility
//! algorithm even though they live in sibling files.
//!
//! Genuinely-external calls — transaction status, multixact, combo-CID
//! resolution, logical-decoding combo-CID lookup, the snapshot-horizon test, and
//! the buffer-manager / xlog calls `SetHintBits` makes — go through the owner
//! seam crates. Each defaults to a loud panic until its owning subsystem is
//! wired up; the `HeapTupleSatisfies*` bodies themselves are ported 1:1 here.
//! There is no silent fallback.

pub mod htup;

#[cfg(test)]
mod tests;

use types_core::primitive::{Size, TransactionId, XLogRecPtr};
use types_core::xact::{
    FirstNormalTransactionId, InvalidCommandId, InvalidTransactionId,
};
use types_core::{CommandId, InvalidOid};
use types_error::PgResult;
use types_storage::buf::BufferIsValid;
use types_storage::storage::Buffer;
use snapshot::snapshot::SnapshotType;
use snapshot::SnapshotData;
use types_tableam::tableam::TM_Result;
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderData, HeapTupleHeaderGetRawXmin, HeapTupleHeaderXminCommitted,
    HEAP_MOVED_IN, HEAP_MOVED_OFF, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_LOCK_ONLY, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
};

use htup::{
    HeapTupleHeaderGetRawXmax, HeapTupleHeaderGetSpeculativeToken, HeapTupleHeaderGetXmin,
    HeapTupleHeaderGetXvac, HeapTupleHeaderIsSpeculative, HeapTupleHeaderXminFrozen,
    HeapTupleHeaderXminInvalid, ItemPointerEquals, ItemPointerIsValid, HEAP_LOCKED_UPGRADED,
    HEAP_XMAX_IS_LOCKED_ONLY,
};

// `HeapTupleHeaderGetRawCommandId` lives in `types_tuple` (it is also used by
// combocid); reach it through the canonical path.
use types_tuple::heaptuple::HeapTupleHeaderGetRawCommandId;

pub use types_tableam::tableam::TM_Result::*;
pub use snapshot::snapshot::HTSV_Result::{self, *};

// Owner seam crates.
use heapam_seams as heapam_seam;
use multixact_seams as multixact_seam;
use subtrans_seams as subtrans_seam;
use transam_seams as transam_seam;
use transam_xact_seams as xact_seam;
use transam_xlog_seams as xlog_seam;
use reorderbuffer_seams as reorderbuffer_seam;
use bufmgr_seams as bufmgr_seam;
use procarray_seams as procarray_seam;
use combocid_seams as combocid_seam;
use snapmgr_pc_seams as snapmgr_seam;

/// This unit owns no inward seams.
pub fn init_seams() {}

/* ------------------------------------------------------------------ */
/* transam.h — pure xid predicate macros (not subsystem functions)     */
/* ------------------------------------------------------------------ */

/// `TransactionIdIsValid(xid)` — `xid != InvalidTransactionId`.
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` — `xid >= FirstNormalTransactionId`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` — modulo-2^32 logical precedence
/// (transam.c).
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `TransactionIdFollowsOrEquals(id1, id2)` (transam.c).
#[inline]
fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    (id1.wrapping_sub(id2) as i32) >= 0
}

/* ------------------------------------------------------------------ */
/* outward-seam thin wrappers                                          */
/* ------------------------------------------------------------------ */

/// `TransactionIdIsCurrentTransactionId(xid)` — backend-local lookup; cannot
/// `ereport` (the seam returns `bool`).
#[inline]
fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    xact_seam::transaction_id_is_current_transaction_id::call(xid)
}

/// `TransactionIdIsInProgress(xid)` — procarray lookup.
#[inline]
fn TransactionIdIsInProgress(xid: TransactionId) -> PgResult<bool> {
    procarray_seam::transaction_id_is_in_progress::call(xid)
}

/// `TransactionIdDidCommit(xid)` — clog lookup. The owner threads
/// `TransactionXmin` explicitly; read it from snapmgr (C's `TransactionXmin`
/// global) and pass it through, mirroring the other consumers.
#[inline]
fn TransactionIdDidCommit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = snapmgr_seam::transaction_xmin::call()?;
    transam_seam::transaction_id_did_commit::call(xid, transaction_xmin)
}

/// `HeapTupleGetUpdateXid(htup)` — resolve a multixact xmax to the update xid.
#[inline]
fn HeapTupleGetUpdateXid(tuple: &HeapTupleHeaderData) -> PgResult<TransactionId> {
    heapam_seam::heap_tuple_get_update_xid::call(tuple)
}

/// `MultiXactIdIsRunning(multi, isLockOnly)`.
#[inline]
fn MultiXactIdIsRunning(multi: TransactionId, is_lock_only: bool) -> PgResult<bool> {
    multixact_seam::multi_xact_id_is_running::call(multi, is_lock_only)
}

/// `HeapTupleHeaderGetCmin(tup)` — combo-CID resolution via the owner.
#[inline]
fn HeapTupleHeaderGetCmin(tuple: &HeapTupleHeaderData) -> CommandId {
    combocid_seam::heap_tuple_header_get_cmin::call(tuple)
}

/// `HeapTupleHeaderGetCmax(tup)` — combo-CID resolution via the owner.
#[inline]
fn HeapTupleHeaderGetCmax(tuple: &HeapTupleHeaderData) -> CommandId {
    combocid_seam::heap_tuple_header_get_cmax::call(tuple)
}

/* ------------------------------------------------------------------ */
/* tuple/header borrow helpers                                          */
/* ------------------------------------------------------------------ */

/// `&mut` to the tuple's owned header (`htup->t_data`).
#[inline]
fn data_mut<'a, 'mcx>(htup: &'a mut HeapTupleData<'mcx>) -> &'a mut HeapTupleHeaderData<'mcx> {
    htup.t_data
        .as_mut()
        .expect("HeapTupleData::t_data must be present for a visibility check")
}

/// `&` to the tuple's owned header (`htup->t_data`).
#[inline]
fn data_ref<'a, 'mcx>(htup: &'a HeapTupleData<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    htup.t_data
        .as_ref()
        .expect("HeapTupleData::t_data must be present for a visibility check")
}

/* ================================================================== */
/* SetHintBits / HeapTupleSetHintBits                                  */
/* ================================================================== */

/// `SetHintBits()` (heapam_visibility.c) — set commit/abort hint bits on a
/// tuple, if it is safe to do so at this time.
///
/// `xid` is the XID of the transaction to check, or `InvalidTransactionId` if no
/// check is needed. Operates on the owned header; `buffer` is the opaque handle
/// threaded to the seams.
fn SetHintBits(
    tuple: &mut HeapTupleHeaderData,
    buffer: Buffer,
    infomask: u16,
    xid: TransactionId,
) -> PgResult<()> {
    // When the tuple is not backed by a shared buffer (e.g. a query-context copy
    // checked by an RI/constraint trigger via SNAPSHOT_SELF), there is no buffer
    // to consult for the LSN interlock and none to dirty. C reaches the
    // BufferIsPermanent/MarkBufferDirtyHint calls with InvalidBuffer only with
    // asserts disabled (they dereference past the buffer array); the meaningful
    // behavior is simply to set the hint bit on the transient header and skip the
    // durability bookkeeping. Guard explicitly here so the port's checked buffer
    // primitives are never handed InvalidBuffer.
    if !BufferIsValid(buffer) {
        tuple.t_infomask |= infomask;
        return Ok(());
    }

    if TransactionIdIsValid(xid) {
        /* NB: xid must be known committed here! */
        let commitLSN: XLogRecPtr = transam_seam::transaction_id_get_commit_lsn::call(xid)?;

        if bufmgr_seam::buffer_is_permanent::call(buffer)?
            && xlog_seam::xlog_needs_flush::call(commitLSN)?
            && bufmgr_seam::buffer_get_lsn_atomic::call(buffer)? < commitLSN
        {
            /* not flushed and no LSN interlock, so don't set hint */
            return Ok(());
        }
    }

    tuple.t_infomask |= infomask;
    bufmgr_seam::mark_buffer_dirty_hint::call(buffer, true);
    Ok(())
}

/// `HeapTupleSetHintBits` — exported wrapper for `SetHintBits`.
pub fn HeapTupleSetHintBits(
    tuple: &mut HeapTupleHeaderData,
    buffer: Buffer,
    infomask: u16,
    xid: TransactionId,
) -> PgResult<()> {
    SetHintBits(tuple, buffer, infomask, xid)
}

/* ================================================================== */
/* HeapTupleSatisfiesSelf                                              */
/* ================================================================== */

/// `HeapTupleSatisfiesSelf` — true iff the tuple is valid "for itself"
/// (SNAPSHOT_SELF).
fn HeapTupleSatisfiesSelf(htup: &mut HeapTupleData, buffer: Buffer) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    let tuple = data_mut(htup);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return Ok(false);
        }

        /* Used by pre-9.0 binary upgrades */
        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = HeapTupleHeaderGetXvac(tuple);

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
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = HeapTupleHeaderGetXvac(tuple);

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
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return Ok(true);
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                /* not deleter */
                return Ok(true);
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple)?;

                /* not LOCKED_ONLY, so it has to have an xmax */
                debug_assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }

            if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                return Ok(true);
            }

            return Ok(false);
        } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))? {
            return Ok(false);
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))? {
            let raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
            return Ok(false);
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }
        return Ok(false); /* updated by other */
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }

        let xmax = HeapTupleGetUpdateXid(tuple)?;

        /* not LOCKED_ONLY, so it has to have an xmax */
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
        /* it must have aborted or crashed */
        return Ok(true);
    }

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }
        return Ok(false);
    }

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))? {
        return Ok(true);
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))? {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    let raw_xmax = HeapTupleHeaderGetRawXmax(tuple);
    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    Ok(false)
}

/* ================================================================== */
/* HeapTupleSatisfiesAny                                               */
/* ================================================================== */

/// `HeapTupleSatisfiesAny` — every tuple satisfies SnapshotAny.
fn HeapTupleSatisfiesAny(_htup: &mut HeapTupleData, _buffer: Buffer) -> PgResult<bool> {
    Ok(true)
}

/* ================================================================== */
/* HeapTupleSatisfiesToast                                             */
/* ================================================================== */

/// `HeapTupleSatisfiesToast` — true iff the tuple is valid as a TOAST row
/// (SNAPSHOT_TOAST).
fn HeapTupleSatisfiesToast(htup: &mut HeapTupleData, buffer: Buffer) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    let tuple = data_mut(htup);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return Ok(false);
        }

        /* Used by pre-9.0 binary upgrades */
        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = HeapTupleHeaderGetXvac(tuple);

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
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = HeapTupleHeaderGetXvac(tuple);

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
        }
        /*
         * An invalid Xmin can be left behind by a speculative insertion that is
         * canceled by super-deleting the tuple. This also applies to TOAST
         * tuples created during speculative insertion.
         */
        else if !TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)) {
            return Ok(false);
        }
    }

    /* otherwise assume the tuple is valid for TOAST. */
    Ok(true)
}

/* ================================================================== */
/* HeapTupleSatisfiesUpdate                                            */
/* ================================================================== */

/// `HeapTupleSatisfiesUpdate` — instant-snapshot visibility with a detailed
/// `TM_Result` and a user-supplied CommandId.
pub fn HeapTupleSatisfiesUpdate(
    htup: &mut HeapTupleData,
    curcid: CommandId,
    buffer: Buffer,
) -> PgResult<TM_Result> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    {
        let tuple = data_ref(htup);
        if !HeapTupleHeaderXminCommitted(tuple) {
            if HeapTupleHeaderXminInvalid(tuple) {
                return Ok(TM_Invisible);
            }

            /* Used by pre-9.0 binary upgrades */
            if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

                if TransactionIdIsCurrentTransactionId(xvac) {
                    return Ok(TM_Invisible);
                }
                if !TransactionIdIsInProgress(xvac)? {
                    if TransactionIdDidCommit(xvac)? {
                        SetHintBits(data_mut(htup), buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                        return Ok(TM_Invisible);
                    }
                    SetHintBits(data_mut(htup), buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                }
            }
            /* Used by pre-9.0 binary upgrades */
            else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

                if !TransactionIdIsCurrentTransactionId(xvac) {
                    if TransactionIdIsInProgress(xvac)? {
                        return Ok(TM_Invisible);
                    }
                    if TransactionIdDidCommit(xvac)? {
                        SetHintBits(data_mut(htup), buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                    } else {
                        SetHintBits(data_mut(htup), buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                        return Ok(TM_Invisible);
                    }
                }
            } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
                if HeapTupleHeaderGetCmin(tuple) >= curcid {
                    return Ok(TM_Invisible); /* inserted after scan started */
                }

                if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                    /* xid invalid */
                    return Ok(TM_Ok);
                }

                if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                    let xmax = HeapTupleHeaderGetRawXmax(tuple);

                    /*
                     * Careful here: even though this tuple was created by our own
                     * transaction, it might be locked by other transactions, if
                     * the original version was key-share locked when we updated
                     * it.
                     */

                    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                        if MultiXactIdIsRunning(xmax, true)? {
                            return Ok(TM_BeingModified);
                        } else {
                            return Ok(TM_Ok);
                        }
                    }

                    /*
                     * If the locker is gone, then there is nothing of interest
                     * left in this Xmax; otherwise, report the tuple as
                     * locked/updated.
                     */
                    if !TransactionIdIsInProgress(xmax)? {
                        return Ok(TM_Ok);
                    }
                    return Ok(TM_BeingModified);
                }

                if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let xmax = HeapTupleGetUpdateXid(tuple)?;

                    /* not LOCKED_ONLY, so it has to have an xmax */
                    debug_assert!(TransactionIdIsValid(xmax));

                    /* deleting subtransaction must have aborted */
                    if !TransactionIdIsCurrentTransactionId(xmax) {
                        if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false)? {
                            return Ok(TM_BeingModified);
                        }
                        return Ok(TM_Ok);
                    } else if HeapTupleHeaderGetCmax(tuple) >= curcid {
                        return Ok(TM_SelfModified); /* updated after scan started */
                    } else {
                        return Ok(TM_Invisible); /* updated before scan started */
                    }
                }

                if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                    /* deleting subtransaction must have aborted */
                    SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                    return Ok(TM_Ok);
                }

                if HeapTupleHeaderGetCmax(tuple) >= curcid {
                    return Ok(TM_SelfModified); /* updated after scan started */
                } else {
                    return Ok(TM_Invisible); /* updated before scan started */
                }
            } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))? {
                return Ok(TM_Invisible);
            } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))? {
                let raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
                SetHintBits(data_mut(htup), buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
            } else {
                /* it must have aborted or crashed */
                SetHintBits(data_mut(htup), buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(TM_Invisible);
            }
        }
    }

    /* by here, the inserting transaction has committed */

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return Ok(TM_Ok);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(TM_Ok);
        }
        if !ItemPointerEquals(&htup.t_self, &data_ref(htup).t_ctid) {
            return Ok(TM_Updated); /* updated by other */
        } else {
            return Ok(TM_Deleted); /* deleted by other */
        }
    }

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_LOCKED_UPGRADED(tuple.t_infomask) {
            return Ok(TM_Ok);
        }

        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true)? {
                return Ok(TM_BeingModified);
            }

            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(TM_Ok);
        }

        let xmax = HeapTupleGetUpdateXid(data_ref(htup))?;
        if !TransactionIdIsValid(xmax) {
            if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(data_ref(htup)), false)? {
                return Ok(TM_BeingModified);
            }
        }

        /* not LOCKED_ONLY, so it has to have an xmax */
        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(data_ref(htup)) >= curcid {
                return Ok(TM_SelfModified); /* updated after scan started */
            } else {
                return Ok(TM_Invisible); /* updated before scan started */
            }
        }

        if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(data_ref(htup)), false)? {
            return Ok(TM_BeingModified);
        }

        if TransactionIdDidCommit(xmax)? {
            if !ItemPointerEquals(&htup.t_self, &data_ref(htup).t_ctid) {
                return Ok(TM_Updated);
            } else {
                return Ok(TM_Deleted);
            }
        }

        /*
         * By here, the update in the Xmax is either aborted or crashed, but what
         * about the other members?
         */

        if !MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(data_ref(htup)), false)? {
            /*
             * There's no member, even just a locker, alive anymore, so we can
             * mark the Xmax as invalid.
             */
            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(TM_Ok);
        } else {
            /* There are lockers running */
            return Ok(TM_BeingModified);
        }
    }

    let tuple = data_ref(htup);

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(TM_BeingModified);
        }
        if HeapTupleHeaderGetCmax(data_ref(htup)) >= curcid {
            return Ok(TM_SelfModified); /* updated after scan started */
        } else {
            return Ok(TM_Invisible); /* updated before scan started */
        }
    }

    let tuple = data_ref(htup);

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))? {
        return Ok(TM_BeingModified);
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))? {
        /* it must have aborted or crashed */
        SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(TM_Ok);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(data_ref(htup).t_infomask) {
        SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(TM_Ok);
    }

    let raw_xmax = HeapTupleHeaderGetRawXmax(data_ref(htup));
    SetHintBits(data_mut(htup), buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    if !ItemPointerEquals(&htup.t_self, &data_ref(htup).t_ctid) {
        Ok(TM_Updated) /* updated by other */
    } else {
        Ok(TM_Deleted) /* deleted by other */
    }
}

/* ================================================================== */
/* HeapTupleSatisfiesDirty                                             */
/* ================================================================== */

/// `HeapTupleSatisfiesDirty` — like `HeapTupleSatisfiesSelf`, but also includes
/// effects of open transactions and uses the passed-in snapshot struct as an
/// output argument (SNAPSHOT_DIRTY).
fn HeapTupleSatisfiesDirty(
    htup: &mut HeapTupleData,
    snapshot: &mut SnapshotData,
    buffer: Buffer,
) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    snapshot.xmin = InvalidTransactionId;
    snapshot.xmax = InvalidTransactionId;
    snapshot.speculativeToken = 0;

    {
        let tuple = data_mut(htup);

        if !HeapTupleHeaderXminCommitted(tuple) {
            if HeapTupleHeaderXminInvalid(tuple) {
                return Ok(false);
            }

            /* Used by pre-9.0 binary upgrades */
            if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

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
            }
            /* Used by pre-9.0 binary upgrades */
            else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

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
            } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
                if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                    /* xid invalid */
                    return Ok(true);
                }

                if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                    /* not deleter */
                    return Ok(true);
                }

                if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let xmax = HeapTupleGetUpdateXid(tuple)?;

                    /* not LOCKED_ONLY, so it has to have an xmax */
                    debug_assert!(TransactionIdIsValid(xmax));

                    /* updating subtransaction must have aborted */
                    if !TransactionIdIsCurrentTransactionId(xmax) {
                        return Ok(true);
                    } else {
                        return Ok(false);
                    }
                }

                if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                    /* deleting subtransaction must have aborted */
                    SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                    return Ok(true);
                }

                return Ok(false);
            } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))? {
                /*
                 * Return the speculative token to caller. Caller can worry about
                 * xmax, since it requires a conclusively locked row version, and
                 * a concurrent update to this tuple is a conflict of its
                 * purposes.
                 */
                if HeapTupleHeaderIsSpeculative(tuple) {
                    snapshot.speculativeToken = HeapTupleHeaderGetSpeculativeToken(tuple);

                    debug_assert!(snapshot.speculativeToken != 0);
                }

                snapshot.xmin = HeapTupleHeaderGetRawXmin(tuple);
                /* XXX shouldn't we fall through to look at xmax? */
                return Ok(true); /* in insertion by other */
            } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))? {
                let raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
            } else {
                /* it must have aborted or crashed */
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(false);
            }
        }
    }

    /* by here, the inserting transaction has committed */

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }
        return Ok(false); /* updated by other */
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }

        let xmax = HeapTupleGetUpdateXid(data_ref(htup))?;

        /* not LOCKED_ONLY, so it has to have an xmax */
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
        /* it must have aborted or crashed */
        return Ok(true);
    }

    let tuple = data_ref(htup);

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return Ok(true);
        }
        return Ok(false);
    }

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))? {
        if !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            snapshot.xmax = HeapTupleHeaderGetRawXmax(tuple);
        }
        return Ok(true);
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))? {
        /* it must have aborted or crashed */
        SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY(data_ref(htup).t_infomask) {
        SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        return Ok(true);
    }

    let raw_xmax = HeapTupleHeaderGetRawXmax(data_ref(htup));
    SetHintBits(data_mut(htup), buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    Ok(false) /* updated by other */
}

/* ================================================================== */
/* HeapTupleSatisfiesMVCC                                              */
/* ================================================================== */

/// `HeapTupleSatisfiesMVCC` — true iff the tuple is valid for the given MVCC
/// snapshot (SNAPSHOT_MVCC).
fn HeapTupleSatisfiesMVCC(
    htup: &mut HeapTupleData,
    snapshot: &SnapshotData,
    buffer: Buffer,
) -> PgResult<bool> {
    /*
     * Assert that the caller has registered the snapshot.
     */
    debug_assert!(snapshot.regd_count > 0 || snapshot.active_count > 0);

    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    {
        let tuple = data_mut(htup);

        if !HeapTupleHeaderXminCommitted(tuple) {
            if HeapTupleHeaderXminInvalid(tuple) {
                return Ok(false);
            }

            /* Used by pre-9.0 binary upgrades */
            if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

                if TransactionIdIsCurrentTransactionId(xvac) {
                    return Ok(false);
                }
                if !XidInMVCCSnapshot(xvac, snapshot)? {
                    if TransactionIdDidCommit(xvac)? {
                        SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                        return Ok(false);
                    }
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                }
            }
            /* Used by pre-9.0 binary upgrades */
            else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

                if !TransactionIdIsCurrentTransactionId(xvac) {
                    if XidInMVCCSnapshot(xvac, snapshot)? {
                        return Ok(false);
                    }
                    if TransactionIdDidCommit(xvac)? {
                        SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId)?;
                    } else {
                        SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                        return Ok(false);
                    }
                }
            } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
                if HeapTupleHeaderGetCmin(tuple) >= snapshot.curcid {
                    return Ok(false); /* inserted after scan started */
                }

                if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                    /* xid invalid */
                    return Ok(true);
                }

                if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                    /* not deleter */
                    return Ok(true);
                }

                if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    let xmax = HeapTupleGetUpdateXid(tuple)?;

                    /* not LOCKED_ONLY, so it has to have an xmax */
                    debug_assert!(TransactionIdIsValid(xmax));

                    /* updating subtransaction must have aborted */
                    if !TransactionIdIsCurrentTransactionId(xmax) {
                        return Ok(true);
                    } else if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid {
                        return Ok(true); /* updated after scan started */
                    } else {
                        return Ok(false); /* updated before scan started */
                    }
                }

                if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                    /* deleting subtransaction must have aborted */
                    SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
                    return Ok(true);
                }

                if HeapTupleHeaderGetCmax(tuple) >= snapshot.curcid {
                    return Ok(true); /* deleted after scan started */
                } else {
                    return Ok(false); /* deleted before scan started */
                }
            } else if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)? {
                return Ok(false);
            } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))? {
                let raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
            } else {
                /* it must have aborted or crashed */
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(false);
            }
        } else {
            /* xmin is committed, but maybe not according to our snapshot */
            if !HeapTupleHeaderXminFrozen(tuple)
                && XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)?
            {
                return Ok(false); /* treat as still in progress */
            }
        }
    }

    /* by here, the inserting transaction has committed */

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return Ok(true);
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        /* already checked above */
        debug_assert!(!HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask));

        let xmax = HeapTupleGetUpdateXid(data_ref(htup))?;

        /* not LOCKED_ONLY, so it has to have an xmax */
        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(data_ref(htup)) >= snapshot.curcid {
                return Ok(true); /* deleted after scan started */
            } else {
                return Ok(false); /* deleted before scan started */
            }
        }
        if XidInMVCCSnapshot(xmax, snapshot)? {
            return Ok(true);
        }
        if TransactionIdDidCommit(xmax)? {
            return Ok(false); /* updating transaction committed */
        }
        /* it must have aborted or crashed */
        return Ok(true);
    }

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
            if HeapTupleHeaderGetCmax(data_ref(htup)) >= snapshot.curcid {
                return Ok(true); /* deleted after scan started */
            } else {
                return Ok(false); /* deleted before scan started */
            }
        }

        if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(data_ref(htup)), snapshot)? {
            return Ok(true);
        }

        if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(data_ref(htup)))? {
            /* it must have aborted or crashed */
            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(true);
        }

        /* xmax transaction committed */
        let raw_xmax = HeapTupleHeaderGetRawXmax(data_ref(htup));
        SetHintBits(data_mut(htup), buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
    } else {
        /* xmax is committed, but maybe not according to our snapshot */
        if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(data_ref(htup)), snapshot)? {
            return Ok(true); /* treat as still in progress */
        }
    }

    /* xmax transaction committed */

    Ok(false)
}

/* ================================================================== */
/* HeapTupleSatisfiesVacuum / VacuumHorizon                            */
/* ================================================================== */

/// `HeapTupleSatisfiesVacuum` — determine the status of a tuple for VACUUM
/// purposes against `OldestXmin`.
pub fn HeapTupleSatisfiesVacuum(
    htup: &mut HeapTupleData,
    OldestXmin: TransactionId,
    buffer: Buffer,
) -> PgResult<HTSV_Result> {
    let mut dead_after: TransactionId = InvalidTransactionId;

    let mut res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &mut dead_after)?;

    if res == HEAPTUPLE_RECENTLY_DEAD {
        debug_assert!(TransactionIdIsValid(dead_after));

        if TransactionIdPrecedes(dead_after, OldestXmin) {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        debug_assert!(!TransactionIdIsValid(dead_after));
    }

    Ok(res)
}

/// `HeapTupleSatisfiesVacuumHorizon` — work horse for the VACUUM routines;
/// stores the deleting xid in `dead_after` when the tuple could still be visible
/// to some backend.
pub fn HeapTupleSatisfiesVacuumHorizon(
    htup: &mut HeapTupleData,
    buffer: Buffer,
    dead_after: &mut TransactionId,
) -> PgResult<HTSV_Result> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    *dead_after = InvalidTransactionId;

    {
        let tuple = data_mut(htup);

        /*
         * Has inserting transaction committed?
         *
         * If the inserting transaction aborted, then the tuple was never visible
         * to any other transaction, so we can delete it immediately.
         */
        if !HeapTupleHeaderXminCommitted(tuple) {
            if HeapTupleHeaderXminInvalid(tuple) {
                return Ok(HEAPTUPLE_DEAD);
            }
            /* Used by pre-9.0 binary upgrades */
            else if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

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
            }
            /* Used by pre-9.0 binary upgrades */
            else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
                let xvac = HeapTupleHeaderGetXvac(tuple);

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
            } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
                if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                    /* xid invalid */
                    return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
                }
                /* only locked? run infomask-only check first, for performance */
                if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask)
                    || HeapTupleHeaderIsOnlyLocked(data_ref(htup))?
                {
                    return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
                }
                /* inserted and then deleted by same xact */
                if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(data_ref(htup))?)
                {
                    return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
                }
                /* deleting subtransaction must have aborted */
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))? {
                /*
                 * It'd be possible to discern between INSERT/DELETE in progress
                 * here by looking at xmax - but that doesn't seem beneficial for
                 * the majority of callers and even detrimental for some. We'd
                 * rather have callers look at/wait for xmin than xmax. It's
                 * always correct to return INSERT_IN_PROGRESS because that's
                 * what's happening from the view of other backends.
                 */
                return Ok(HEAPTUPLE_INSERT_IN_PROGRESS);
            } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))? {
                let raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, raw_xmin)?;
            } else {
                /*
                 * Not in Progress, Not Committed, so either Aborted or crashed
                 */
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId)?;
                return Ok(HEAPTUPLE_DEAD);
            }

            /*
             * At this point the xmin is known committed, but we might not have
             * been able to set the hint bit yet; so we can no longer Assert that
             * it's set.
             */
        }
    }

    /*
     * Okay, the inserter committed, so it was good at some point. Now what about
     * the deleting transaction?
     */
    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(HEAPTUPLE_LIVE);
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        /*
         * "Deleting" xact really only locked it, so the tuple is live in any
         * case. However, we should make sure that either XMAX_COMMITTED or
         * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
         * examining the tuple for future xacts.
         */
        if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                /*
                 * If it's a pre-pg_upgrade tuple, the multixact cannot possibly
                 * be running; otherwise have to check.
                 */
                if !HEAP_LOCKED_UPGRADED(tuple.t_infomask)
                    && MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true)?
                {
                    return Ok(HEAPTUPLE_LIVE);
                }
                SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            } else {
                if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))? {
                    return Ok(HEAPTUPLE_LIVE);
                }
                SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            }
        }

        /*
         * We don't really care whether xmax did commit, abort or crash. We know
         * that xmax did lock the tuple, but it did not and will never actually
         * update it.
         */

        return Ok(HEAPTUPLE_LIVE);
    }

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = HeapTupleGetUpdateXid(data_ref(htup))?;

        /* already checked above */
        debug_assert!(!HEAP_XMAX_IS_LOCKED_ONLY(data_ref(htup).t_infomask));

        /* not LOCKED_ONLY, so it has to have an xmax */
        debug_assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsInProgress(xmax)? {
            return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
        } else if TransactionIdDidCommit(xmax)? {
            /*
             * The multixact might still be running due to lockers. Need to allow
             * for pruning if below the xid horizon regardless -- otherwise we
             * could end up with a tuple where the updater has to be removed due
             * to the horizon, but is not pruned away. It's not a problem to
             * prune that tuple, because any remaining lockers will also be
             * present in newer tuple versions.
             */
            *dead_after = xmax;
            return Ok(HEAPTUPLE_RECENTLY_DEAD);
        } else if !MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(data_ref(htup)), false)? {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed. Mark
             * the Xmax as invalid.
             */
            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        }

        return Ok(HEAPTUPLE_LIVE);
    }

    let tuple = data_ref(htup);

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))? {
            return Ok(HEAPTUPLE_DELETE_IN_PROGRESS);
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))? {
            let raw_xmax = HeapTupleHeaderGetRawXmax(data_ref(htup));
            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_COMMITTED, raw_xmax)?;
        } else {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(data_mut(htup), buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
            return Ok(HEAPTUPLE_LIVE);
        }

        /*
         * At this point the xmax is known committed, but we might not have been
         * able to set the hint bit yet; so we can no longer Assert that it's set.
         */
    }

    /*
     * Deleter committed, allow caller to check if it was recent enough that some
     * open transactions could still see the tuple.
     */
    *dead_after = HeapTupleHeaderGetRawXmax(data_ref(htup));
    Ok(HEAPTUPLE_RECENTLY_DEAD)
}

/* ================================================================== */
/* HeapTupleSatisfiesNonVacuumable                                     */
/* ================================================================== */

/// `HeapTupleSatisfiesNonVacuumable` — true if the tuple might be visible to
/// some transaction; false if surely dead (vacuumable). Snapshot-style API for
/// `HeapTupleSatisfiesVacuum`.
fn HeapTupleSatisfiesNonVacuumable(
    htup: &mut HeapTupleData,
    snapshot: &SnapshotData,
    buffer: Buffer,
) -> PgResult<bool> {
    let mut dead_after: TransactionId = InvalidTransactionId;

    let mut res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &mut dead_after)?;

    if res == HEAPTUPLE_RECENTLY_DEAD {
        debug_assert!(TransactionIdIsValid(dead_after));

        if procarray_seam::global_vis_test_is_removable_xid::call(snapshot.vistest, dead_after)? {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        debug_assert!(!TransactionIdIsValid(dead_after));
    }

    Ok(res != HEAPTUPLE_DEAD)
}

/* ================================================================== */
/* HeapTupleIsSurelyDead                                               */
/* ================================================================== */

/// `HeapTupleIsSurelyDead` — cheaply determine whether the tuple is surely dead
/// to all onlookers, consulting neither procarray nor CLOG. May return false
/// when in doubt.
pub fn HeapTupleIsSurelyDead(
    htup: &HeapTupleData,
    vistest: snapshot::snapshot::GlobalVisStateHandle,
) -> PgResult<bool> {
    let tuple = data_ref(htup);

    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    /*
     * If the inserting transaction is marked invalid, then it aborted, and the
     * tuple is definitely dead. If it's marked neither committed nor invalid,
     * then we assume it's still alive (since the presumption is that all
     * relevant hint bits were just set moments ago).
     */
    if !HeapTupleHeaderXminCommitted(tuple) {
        return Ok(HeapTupleHeaderXminInvalid(tuple));
    }

    /*
     * If the inserting transaction committed, but any deleting transaction
     * aborted, the tuple is still alive.
     */
    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(false);
    }

    /*
     * If the XMAX is just a lock, the tuple is still alive.
     */
    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return Ok(false);
    }

    /*
     * If the Xmax is a MultiXact, it might be dead or alive, but we cannot know
     * without checking pg_multixact.
     */
    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        return Ok(false);
    }

    /* If deleter isn't known to have committed, assume it's still running. */
    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        return Ok(false);
    }

    /* Deleter committed, so tuple is dead if the XID is old enough. */
    procarray_seam::global_vis_test_is_removable_xid::call(
        vistest,
        HeapTupleHeaderGetRawXmax(tuple),
    )
}

/* ================================================================== */
/* HeapTupleHeaderIsOnlyLocked                                         */
/* ================================================================== */

/// `HeapTupleHeaderIsOnlyLocked` — is the tuple really only locked (not
/// updated)?
pub fn HeapTupleHeaderIsOnlyLocked(tuple: &HeapTupleHeaderData) -> PgResult<bool> {
    /* if there's no valid Xmax, then there's obviously no update either */
    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }

    if (tuple.t_infomask & HEAP_XMAX_LOCK_ONLY) != 0 {
        return Ok(true);
    }

    /* invalid xmax means no update */
    if !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)) {
        return Ok(true);
    }

    /*
     * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
     * necessarily have been updated
     */
    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) == 0 {
        return Ok(false);
    }

    /* ... but if it's a multi, then perhaps the updating Xid aborted. */
    let xmax = HeapTupleGetUpdateXid(tuple)?;

    /* not LOCKED_ONLY, so it has to have an xmax */
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

    /*
     * not current, not in progress, not committed -- must have aborted or
     * crashed
     */
    Ok(true)
}

/// `HeapTupleHeaderGetUpdateXid(tup)` (htup_details.h) — the update XID,
/// resolving a multixact xmax via the seam; used by
/// `HeapTupleSatisfiesVacuumHorizon`.
pub fn HeapTupleHeaderGetUpdateXid(tuple: &HeapTupleHeaderData) -> PgResult<TransactionId> {
    if (tuple.t_infomask & HEAP_XMAX_INVALID) == 0
        && (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0
        && (tuple.t_infomask & HEAP_XMAX_LOCK_ONLY) == 0
    {
        HeapTupleGetUpdateXid(tuple)
    } else {
        Ok(HeapTupleHeaderGetRawXmax(tuple))
    }
}

/* ================================================================== */
/* TransactionIdInArray                                                */
/* ================================================================== */

/// `TransactionIdInArray` — whether `xid` is in the pre-sorted slice `xip`
/// (sorted by `xidComparator`, i.e. unsigned u32), using binary search.
fn TransactionIdInArray(xid: TransactionId, xip: &[TransactionId], num: Size) -> bool {
    if num == 0 {
        return false;
    }
    // xidComparator is pg_cmp_u32 (plain unsigned compare); the array is sorted
    // accordingly, so a standard binary search matches bsearch() exactly.
    xip[..num].binary_search(&xid).is_ok()
}

/* ================================================================== */
/* HeapTupleSatisfiesHistoricMVCC                                      */
/* ================================================================== */

/// `HeapTupleSatisfiesHistoricMVCC` — MVCC rules for timetravel
/// (logical-decoding) catalog scans (SNAPSHOT_HISTORIC_MVCC). Only usable on
/// tuples from catalog tables; sets no hint bits.
fn HeapTupleSatisfiesHistoricMVCC(
    htup: &mut HeapTupleData,
    snapshot: &SnapshotData,
    buffer: Buffer,
) -> PgResult<bool> {
    debug_assert!(ItemPointerIsValid(&htup.t_self));
    debug_assert!(htup.t_tableOid != InvalidOid);

    let xmin = HeapTupleHeaderGetXmin(data_ref(htup));
    let mut xmax = HeapTupleHeaderGetRawXmax(data_ref(htup));

    /* inserting transaction aborted */
    if HeapTupleHeaderXminInvalid(data_ref(htup)) {
        debug_assert!(!TransactionIdDidCommit(xmin)?);
        return Ok(false);
    }
    /* check if it's one of our txids, toplevel is also in there */
    else if TransactionIdInArray(xmin, &snapshot.subxip, snapshot.subxcnt as Size) {
        let cmin: CommandId = HeapTupleHeaderGetRawCommandId(data_ref(htup));
        let cmax: CommandId = InvalidCommandId;

        /*
         * another transaction might have (tried to) delete this tuple or
         * cmin/cmax was stored in a combo CID. So we need to lookup the actual
         * values externally.
         */
        let r = reorderbuffer_seam::resolve_cmin_cmax_during_decoding::call(
            snapshot.clone(),
            htup.clone(),
            buffer,
            cmin,
            cmax,
        )?;

        /*
         * If we haven't resolved the combo CID to cmin/cmax, that means we have
         * not decoded the combo CID yet. That means the cmin is definitely in
         * the future, and we're not supposed to see the tuple yet.
         */
        if !r.resolved {
            return Ok(false);
        }

        debug_assert!(r.cmin != InvalidCommandId);

        if r.cmin >= snapshot.curcid {
            return Ok(false); /* inserted after scan started */
        }
        /* fall through */
    }
    /* committed before our xmin horizon. Do a normal visibility check. */
    else if TransactionIdPrecedes(xmin, snapshot.xmin) {
        debug_assert!(
            !(HeapTupleHeaderXminCommitted(data_ref(htup)) && !TransactionIdDidCommit(xmin)?)
        );

        /* check for hint bit first, consult clog afterwards */
        if !HeapTupleHeaderXminCommitted(data_ref(htup)) && !TransactionIdDidCommit(xmin)? {
            return Ok(false);
        }
        /* fall through */
    }
    /* beyond our xmax horizon, i.e. invisible */
    else if TransactionIdFollowsOrEquals(xmin, snapshot.xmax) {
        return Ok(false);
    }
    /* check if it's a committed transaction in [xmin, xmax) */
    else if TransactionIdInArray(xmin, &snapshot.xip, snapshot.xcnt as Size) {
        /* fall through */
    }
    /*
     * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
     * invisible.
     */
    else {
        return Ok(false);
    }

    /* at this point we know xmin is visible, go on to check xmax */

    let tuple = data_ref(htup);

    /* xid invalid or aborted */
    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return Ok(true);
    }
    /* locked tuples are always visible */
    else if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return Ok(true);
    }
    /*
     * We can see multis here if we're looking at user tables or if somebody
     * SELECT ... FOR SHARE/UPDATE a system table.
     */
    else if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        xmax = HeapTupleGetUpdateXid(data_ref(htup))?;
    }

    /* check if it's one of our txids, toplevel is also in there */
    if TransactionIdInArray(xmax, &snapshot.subxip, snapshot.subxcnt as Size) {
        let cmin: CommandId = InvalidCommandId;
        let cmax: CommandId = HeapTupleHeaderGetRawCommandId(data_ref(htup));

        /* Lookup actual cmin/cmax values */
        let r = reorderbuffer_seam::resolve_cmin_cmax_during_decoding::call(
            snapshot.clone(),
            htup.clone(),
            buffer,
            cmin,
            cmax,
        )?;

        /*
         * If we haven't resolved the combo CID to cmin/cmax, that means we have
         * not decoded the combo CID yet. That means the cmax is definitely in
         * the future, and we're still supposed to see the tuple.
         */
        if !r.resolved || r.cmax == InvalidCommandId {
            return Ok(true);
        }

        if r.cmax >= snapshot.curcid {
            return Ok(true); /* deleted after scan started */
        } else {
            return Ok(false); /* deleted before scan started */
        }
    }
    /* below xmin horizon, normal transaction state is valid */
    else if TransactionIdPrecedes(xmax, snapshot.xmin) {
        debug_assert!(
            !((data_ref(htup).t_infomask & HEAP_XMAX_COMMITTED) != 0
                && !TransactionIdDidCommit(xmax)?)
        );

        /* check hint bit first */
        if (data_ref(htup).t_infomask & HEAP_XMAX_COMMITTED) != 0 {
            return Ok(false);
        }

        /* check clog */
        Ok(!TransactionIdDidCommit(xmax)?)
    }
    /* above xmax horizon, we cannot possibly see the deleting transaction */
    else if TransactionIdFollowsOrEquals(xmax, snapshot.xmax) {
        Ok(true)
    }
    /* xmax is between [xmin, xmax), check known committed array */
    else if TransactionIdInArray(xmax, &snapshot.xip, snapshot.xcnt as Size) {
        Ok(false)
    }
    /* xmax is between [xmin, xmax), but known not to have committed yet */
    else {
        Ok(true)
    }
}

/* ================================================================== */
/* XidInMVCCSnapshot                                                   */
/* ================================================================== */

/// `XidInMVCCSnapshot` (snapmgr.c) — is `xid` still-in-progress according to the
/// snapshot? Ported here because the MVCC visibility path relies on it; only
/// `SubTransGetTopmostTransaction` is seamed.
pub fn XidInMVCCSnapshot(xid: TransactionId, snapshot: &SnapshotData) -> PgResult<bool> {
    let mut xid = xid;

    /*
     * Make a quick range check to eliminate most XIDs without looking at the xip
     * arrays.
     */

    /* Any xid < xmin is not in-progress */
    if TransactionIdPrecedes(xid, snapshot.xmin) {
        return Ok(false);
    }
    /* Any xid >= xmax is in-progress */
    if TransactionIdFollowsOrEquals(xid, snapshot.xmax) {
        return Ok(true);
    }

    /*
     * Snapshot information is stored slightly differently in snapshots taken
     * during recovery.
     */
    if !snapshot.takenDuringRecovery {
        /*
         * If the snapshot contains full subxact data, the fastest way to check
         * things is just to compare the given XID against both subxact XIDs and
         * top-level XIDs. If the snapshot overflowed, we have to use pg_subtrans
         * to convert a subxact XID to its parent XID, but then we need only look
         * at top-level XIDs not subxacts.
         */
        if !snapshot.suboverflowed {
            /* we have full data, so search subxip */
            if pg_lfind32(xid, &snapshot.subxip, snapshot.subxcnt as u32) {
                return Ok(true);
            }

            /* not there, fall through to search xip[] */
        } else {
            /*
             * Snapshot overflowed, so convert xid to top-level. This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = subtrans_seam::sub_trans_get_topmost_transaction::call(xid)?;

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin, so
             * recheck to avoid an array scan. No point in rechecking xmax.
             */
            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }

        if pg_lfind32(xid, &snapshot.xip, snapshot.xcnt) {
            return Ok(true);
        }
    } else {
        /*
         * In recovery we store all xids in the subxip array because it is by far
         * the bigger array, and we mostly don't know which xids are top-level and
         * which are subxacts. The xip array is empty.
         *
         * We start by searching subtrans, if we overflowed.
         */
        if snapshot.suboverflowed {
            /*
             * Snapshot overflowed, so convert xid to top-level. This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = subtrans_seam::sub_trans_get_topmost_transaction::call(xid)?;

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin, so
             * recheck to avoid an array scan. No point in rechecking xmax.
             */
            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }

        /*
         * We now have either a top-level xid higher than xmin or an
         * indeterminate xid. We don't know whether it's top level or subxact but
         * it doesn't matter. If it's present, the xid is visible.
         */
        if pg_lfind32(xid, &snapshot.subxip, snapshot.subxcnt as u32) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `pg_lfind32(value, base, nelem)` (pg_lfind.h) — linear search of a `u32`
/// slice. (The C version is SIMD-accelerated; the result is identical.)
#[inline]
fn pg_lfind32(value: TransactionId, base: &[TransactionId], nelem: u32) -> bool {
    if nelem == 0 {
        return false;
    }
    base[..(nelem as usize)].contains(&value)
}

/* ================================================================== */
/* HeapTupleSatisfiesVisibility                                        */
/* ================================================================== */

/// `HeapTupleSatisfiesVisibility` — dispatch to the routine for the snapshot's
/// type.
///
/// The snapshot crosses as `&mut SnapshotData` because the DIRTY path writes
/// `xmin`/`xmax`/`speculativeToken` into it; the other paths read it only.
pub fn HeapTupleSatisfiesVisibility(
    htup: &mut HeapTupleData,
    snapshot: &mut SnapshotData,
    buffer: Buffer,
) -> PgResult<bool> {
    match snapshot.snapshot_type {
        SnapshotType::SNAPSHOT_MVCC => HeapTupleSatisfiesMVCC(htup, snapshot, buffer),
        SnapshotType::SNAPSHOT_SELF => HeapTupleSatisfiesSelf(htup, buffer),
        SnapshotType::SNAPSHOT_ANY => HeapTupleSatisfiesAny(htup, buffer),
        SnapshotType::SNAPSHOT_TOAST => HeapTupleSatisfiesToast(htup, buffer),
        SnapshotType::SNAPSHOT_DIRTY => HeapTupleSatisfiesDirty(htup, snapshot, buffer),
        SnapshotType::SNAPSHOT_HISTORIC_MVCC => {
            HeapTupleSatisfiesHistoricMVCC(htup, snapshot, buffer)
        }
        SnapshotType::SNAPSHOT_NON_VACUUMABLE => {
            HeapTupleSatisfiesNonVacuumable(htup, snapshot, buffer)
        }
    }
}
