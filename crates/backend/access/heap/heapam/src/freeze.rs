//! F5 FREEZE family of `access/heap/heapam.c` — the VACUUM/CLUSTER tuple-freeze
//! machinery.
//!
//! Two layers:
//!   * **pure logic** over an owned `&mut HeapTupleHeaderData`
//!     ([`heap_prepare_freeze_tuple`], [`FreezeMultiXactId`],
//!     [`heap_execute_freeze_tuple`], [`heap_freeze_tuple`],
//!     [`heap_tuple_needs_eventual_freeze`], [`heap_tuple_should_freeze`],
//!     [`HeapTupleHeaderAdvanceConflictHorizon`]), and
//!   * **page-bound seam wrappers** ([`heap_pre_freeze_checks`],
//!     [`heap_freeze_prepared_tuples`], and the `vacuumlazy-seams`
//!     `heap_tuple_should_freeze` / `heap_tuple_needs_eventual_freeze` entry
//!     points) that materialize the on-page `HeapTupleHeader` at a `(Buffer,
//!     OffsetNumber)` through this repo's buffer-id page model.
//!
//! Ported 1:1 from PostgreSQL 18.3.

use ::mcx::Mcx;
use ::types_core::primitive::{MultiXactId, TransactionId};
use ::types_core::xact::{FrozenTransactionId, InvalidTransactionId};
use ::types_error::{PgError, PgResult};
use ::types_storage::Buffer;
use ::types_tuple::heaptuple::{
    HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice, HeapTupleHeaderData, HEAP_HOT_UPDATED,
    HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_MOVED_OFF, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_MULTI, HEAP_XMIN_FROZEN,
};
use ::types_vacuum::vacuum::VacuumCutoffs;
use ::xlog_records::multixact::{MultiXactMember, MultiXactStatus};

use ::heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HeapTupleHeaderGetXmin, HeapTupleHeaderGetXvac,
    HeapTupleHeaderXminInvalid, HEAP_LOCKED_UPGRADED, HEAP_XMAX_IS_LOCKED_ONLY,
};
use ::types_tuple::heaptuple::HeapTupleHeaderXminCommitted;
use ::heapam_visibility::HeapTupleHeaderGetUpdateXid;
use ::transam::{
    TransactionIdFollows, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};

use crate::{GetMultiXactIdHintBits, HeapPageFreeze, HeapTupleFreeze};

use multixact_seams as multixact_seam;
use transam_xact_seams as xact_seam;
use bufmgr_seams as bufmgr_seam;
use procarray_seams as procarray_seam;

// ===========================================================================
// heapam-local vocabulary (htup_details.h / heapam_xlog.h / heapam.c constants
// not yet hoisted into the shared types crates).
// ===========================================================================

use ::heapam_visibility::htup::HEAP_LOCK_MASK;

/// `HEAP_XMAX_BITS` (htup_details.h).
const HEAP_XMAX_BITS: u16 =
    HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY;

use ::types_tuple::heaptuple::HEAP_XMAX_LOCK_ONLY;

/// `XLH_FREEZE_XVAC` (heapam_xlog.h).
const XLH_FREEZE_XVAC: u8 = 0x02;
/// `XLH_INVALID_XVAC` (heapam_xlog.h).
const XLH_INVALID_XVAC: u8 = 0x04;

/// `HEAP_FREEZE_CHECK_XMIN_COMMITTED` (access/heapam.h).
pub const HEAP_FREEZE_CHECK_XMIN_COMMITTED: u8 = 0x01;
/// `HEAP_FREEZE_CHECK_XMAX_ABORTED` (access/heapam.h).
pub const HEAP_FREEZE_CHECK_XMAX_ABORTED: u8 = 0x02;

// `FreezeMultiXactId` *flags output bits (heapam.c).
const FRM_NOOP: u16 = 0x0001;
const FRM_INVALIDATE_XMAX: u16 = 0x0002;
const FRM_RETURN_IS_XID: u16 = 0x0004;
const FRM_RETURN_IS_MULTI: u16 = 0x0008;
const FRM_MARK_COMMITTED: u16 = 0x0010;

/// `InvalidMultiXactId` (multixact.h).
const InvalidMultiXactId: MultiXactId = 0;

/// `MultiXactIdIsValid(multi)` (multixact.h).
#[inline]
fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

/// `MultiXactIdPrecedes(multi1, multi2)` (multixact.c) — modulo-2^32 circular
/// comparison (`(int32)(multi1 - multi2) < 0`).
#[inline]
fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

/// `MultiXactIdPrecedesOrEquals(multi1, multi2)` (multixact.c).
#[inline]
fn MultiXactIdPrecedesOrEquals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) <= 0
}

/// `ISUPDATE_from_mxstatus(status)` (multixact.h): `status > ForUpdate`.
#[inline]
fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    (status as i32) > (MultiXactStatus::ForUpdate as i32)
}

// ===========================================================================
// MultiXactIdGetUpdateXid — heapam.c static; the updater XID carried by a
// (non-lock-only) multixact xmax.
// ===========================================================================

/// `MultiXactIdGetUpdateXid(xmax, t_infomask)` (heapam.c) — given a multixact
/// xmax (without `HEAP_XMAX_LOCK_ONLY`), return the updating transaction's XID.
fn MultiXactIdGetUpdateXid<'mcx>(
    mcx: Mcx<'mcx>,
    xmax: TransactionId,
    t_infomask: u16,
) -> PgResult<TransactionId> {
    let mut update_xact = InvalidTransactionId;

    debug_assert!(t_infomask & HEAP_XMAX_LOCK_ONLY == 0);
    debug_assert!(t_infomask & HEAP_XMAX_IS_MULTI != 0);

    // Since the LOCK_ONLY bit is not set, this cannot be a pre-pg_upgrade multi.
    let members = multixact_seam::get_multi_xact_id_members::call(mcx, xmax, false, false)?;

    for member in members.iter() {
        let status = member
            .status
            .expect("MultiXactIdGetUpdateXid: member with out-of-range status");
        // Ignore lockers.
        if !ISUPDATE_from_mxstatus(status) {
            continue;
        }
        // There can be at most one updater.
        debug_assert!(update_xact == InvalidTransactionId);
        update_xact = member.xid;
        break;
    }

    Ok(update_xact)
}

// ===========================================================================
// FreezeMultiXactId — heapam.c.
// ===========================================================================

/// `FreezeMultiXactId(multi, t_infomask, cutoffs, &flags, pagefrz)` (heapam.c).
/// Returns `(newxmax, flags)`.
fn FreezeMultiXactId<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    t_infomask: u16,
    cutoffs: &VacuumCutoffs,
    pagefrz: &mut HeapPageFreeze,
) -> PgResult<(TransactionId, u16)> {
    let newxmax: TransactionId;
    let mut flags: u16 = 0;

    // We should only be called in Multis.
    debug_assert!(t_infomask & HEAP_XMAX_IS_MULTI != 0);

    if !MultiXactIdIsValid(multi) || HEAP_LOCKED_UPGRADED(t_infomask) {
        flags |= FRM_INVALIDATE_XMAX;
        pagefrz.freeze_required = true;
        return Ok((InvalidTransactionId, flags));
    } else if MultiXactIdPrecedes(multi, cutoffs.relminmxid) {
        return Err(PgError::error(format_args_internal(&[
            "found multixact ",
            &multi.to_string(),
            " from before relminmxid ",
            &cutoffs.relminmxid.to_string(),
        ])));
    } else if MultiXactIdPrecedes(multi, cutoffs.OldestMxact) {
        // This old multi cannot possibly have members still running, but verify
        // just in case. If it was a locker only, it can be removed without any
        // further consideration; but if it contained an update, we might need to
        // preserve it.
        if multixact_seam::multi_xact_id_is_running::call(
            multi,
            HEAP_XMAX_IS_LOCKED_ONLY(t_infomask),
        )? {
            return Err(PgError::error(format_args_internal(&[
                "multixact ",
                &multi.to_string(),
                " from before multi freeze cutoff ",
                &cutoffs.OldestMxact.to_string(),
                " found to be still running",
            ])));
        }

        if HEAP_XMAX_IS_LOCKED_ONLY(t_infomask) {
            flags |= FRM_INVALIDATE_XMAX;
            pagefrz.freeze_required = true;
            return Ok((InvalidTransactionId, flags));
        }

        // replace multi with single XID for its updater?
        let update_xact = MultiXactIdGetUpdateXid(mcx, multi, t_infomask)?;
        if TransactionIdPrecedes(update_xact, cutoffs.relfrozenxid) {
            return Err(PgError::error(format_args_internal(&[
                "multixact ",
                &multi.to_string(),
                " contains update XID ",
                &update_xact.to_string(),
                " from before relfrozenxid ",
                &cutoffs.relfrozenxid.to_string(),
            ])));
        } else if TransactionIdPrecedes(update_xact, cutoffs.OldestXmin) {
            // Updater XID has to have aborted (otherwise the tuple would have
            // been pruned away instead, since updater XID is < OldestXmin).
            // Just remove xmax.
            if transaction_id_did_commit(update_xact)? {
                return Err(PgError::error(format_args_internal(&[
                    "multixact ",
                    &multi.to_string(),
                    " contains committed update XID ",
                    &update_xact.to_string(),
                    " from before removable cutoff ",
                    &cutoffs.OldestXmin.to_string(),
                ])));
            }
            flags |= FRM_INVALIDATE_XMAX;
            pagefrz.freeze_required = true;
            return Ok((InvalidTransactionId, flags));
        }

        // Have to keep updater XID as new xmax.
        flags |= FRM_RETURN_IS_XID;
        pagefrz.freeze_required = true;
        return Ok((update_xact, flags));
    }

    // Some member(s) of this Multi may be below FreezeLimit xid cutoff, so we
    // need to walk the whole members array to figure out what to do, if
    // anything.
    let members = multixact_seam::get_multi_xact_id_members::call(
        mcx,
        multi,
        false,
        HEAP_XMAX_IS_LOCKED_ONLY(t_infomask),
    )?;
    let nmembers = members.len();
    if nmembers == 0 {
        // Nothing worth keeping.
        flags |= FRM_INVALIDATE_XMAX;
        pagefrz.freeze_required = true;
        return Ok((InvalidTransactionId, flags));
    }

    // The FRM_NOOP case is the only case where we might need to ratchet back
    // FreezePageRelfrozenXid or FreezePageRelminMxid. (See the C comment.)
    let mut need_replace = false;
    let mut FreezePageRelfrozenXid = pagefrz.FreezePageRelfrozenXid;
    for member in members.iter() {
        let xid = member.xid;
        debug_assert!(!TransactionIdPrecedes(xid, cutoffs.relfrozenxid));

        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            // Can't violate the FreezeLimit postcondition.
            need_replace = true;
            break;
        }
        if TransactionIdPrecedes(xid, FreezePageRelfrozenXid) {
            FreezePageRelfrozenXid = xid;
        }
    }

    // Can't violate the MultiXactCutoff postcondition, either.
    if !need_replace {
        need_replace = MultiXactIdPrecedes(multi, cutoffs.MultiXactCutoff);
    }

    if !need_replace {
        // vacuumlazy.c might ratchet back NewRelminMxid, NewRelfrozenXid, or both
        // together to make it safe to retain this particular multi after freezing
        // its page.
        flags |= FRM_NOOP;
        pagefrz.FreezePageRelfrozenXid = FreezePageRelfrozenXid;
        if MultiXactIdPrecedes(multi, pagefrz.FreezePageRelminMxid) {
            pagefrz.FreezePageRelminMxid = multi;
        }
        // C pfree(members); the owned vec drops at scope end.
        return Ok((multi, flags));
    }

    // Do a more thorough second pass over the multi to figure out which member
    // XIDs actually need to be kept.
    let mut newmembers: std::vec::Vec<MultiXactMember> = std::vec::Vec::with_capacity(nmembers);
    let mut has_lockers = false;
    let mut update_xid = InvalidTransactionId;
    let mut update_committed = false;

    // Determine whether to keep each member xid, or to ignore it instead.
    for member in members.iter() {
        let xid = member.xid;
        let mstatus = member
            .status
            .expect("FreezeMultiXactId: member with out-of-range status");

        debug_assert!(!TransactionIdPrecedes(xid, cutoffs.relfrozenxid));

        if !ISUPDATE_from_mxstatus(mstatus) {
            // Locker XID (not updater XID). We only keep lockers that are still
            // running.
            if xact_seam::transaction_id_is_current_transaction_id::call(xid)
                || procarray_seam::transaction_id_is_in_progress::call(xid)?
            {
                if TransactionIdPrecedes(xid, cutoffs.OldestXmin) {
                    return Err(PgError::error(format_args_internal(&[
                        "multixact ",
                        &multi.to_string(),
                        " contains running locker XID ",
                        &xid.to_string(),
                        " from before removable cutoff ",
                        &cutoffs.OldestXmin.to_string(),
                    ])));
                }
                newmembers.push(*member);
                has_lockers = true;
            }
            continue;
        }

        // Updater XID (not locker XID). Should we keep it?
        //
        // In any case the Multi should never contain two updaters, whatever
        // their individual commit status. Check for that first, in passing.
        if TransactionIdIsValid(update_xid) {
            return Err(PgError::error(format_args_internal(&[
                "multixact ",
                &multi.to_string(),
                " has two or more updating members",
            ])));
        }

        // As with all tuple visibility routines, it's critical to test
        // TransactionIdIsInProgress before TransactionIdDidCommit.
        if xact_seam::transaction_id_is_current_transaction_id::call(xid)
            || procarray_seam::transaction_id_is_in_progress::call(xid)?
        {
            update_xid = xid;
        } else if transaction_id_did_commit(xid)? {
            // The transaction committed, so we can tell caller to set
            // HEAP_XMAX_COMMITTED.
            update_committed = true;
            update_xid = xid;
        } else {
            // Not in progress, not committed -- must be aborted or crashed; we
            // can ignore it.
            continue;
        }

        // We determined that updater must be kept -- add it to pending new
        // members list.
        if TransactionIdPrecedes(xid, cutoffs.OldestXmin) {
            return Err(PgError::error(format_args_internal(&[
                "multixact ",
                &multi.to_string(),
                " contains committed update XID ",
                &xid.to_string(),
                " from before removable cutoff ",
                &cutoffs.OldestXmin.to_string(),
            ])));
        }
        newmembers.push(*member);
    }

    // C pfree(members); the owned vec drops at scope end.

    // Determine what to do with caller's multi based on information gathered
    // during our second pass.
    if newmembers.is_empty() {
        // Nothing worth keeping.
        flags |= FRM_INVALIDATE_XMAX;
        newxmax = InvalidTransactionId;
    } else if TransactionIdIsValid(update_xid) && !has_lockers {
        // If there's a single member and it's an update, pass it back alone
        // without creating a new Multi.
        debug_assert!(newmembers.len() == 1);
        flags |= FRM_RETURN_IS_XID;
        if update_committed {
            flags |= FRM_MARK_COMMITTED;
        }
        newxmax = update_xid;
    } else {
        // Create a new multixact with the surviving members of the previous one,
        // to set as new Xmax in the tuple.
        newxmax = multixact_seam::multi_xact_id_create_from_members::call(&newmembers)?;
        flags |= FRM_RETURN_IS_MULTI;
    }

    // C pfree(newmembers); the owned vec drops at scope end.

    pagefrz.freeze_required = true;
    Ok((newxmax, flags))
}

// ===========================================================================
// heap_prepare_freeze_tuple — heapam.c.
// ===========================================================================

/// `heap_prepare_freeze_tuple(tuple, cutoffs, pagefrz, frz, &totally_frozen)`
/// (heapam.c). Returns `(do_freeze, totally_frozen)`; `frz` is filled with the
/// freeze plan when a usable one exists.
pub fn heap_prepare_freeze_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleHeaderData,
    cutoffs: &VacuumCutoffs,
    pagefrz: &mut HeapPageFreeze,
    frz: &mut HeapTupleFreeze,
) -> PgResult<(bool, bool)> {
    let mut xmin_already_frozen = false;
    let mut xmax_already_frozen = false;
    let mut freeze_xmin = false;
    let mut replace_xvac = false;
    let mut replace_xmax = false;
    let mut freeze_xmax = false;

    frz.xmax = HeapTupleHeaderGetRawXmax(tuple);
    frz.t_infomask2 = tuple.t_infomask2;
    frz.t_infomask = tuple.t_infomask;
    frz.frzflags = 0;
    frz.checkflags = 0;

    // Process xmin, while keeping track of whether it's already frozen, or will
    // become frozen iff our freeze plan is executed by caller.
    let mut xid = HeapTupleHeaderGetXmin(tuple);
    if !TransactionIdIsNormal(xid) {
        xmin_already_frozen = true;
    } else {
        if TransactionIdPrecedes(xid, cutoffs.relfrozenxid) {
            return Err(PgError::error(format_args_internal(&[
                "found xmin ",
                &xid.to_string(),
                " from before relfrozenxid ",
                &cutoffs.relfrozenxid.to_string(),
            ])));
        }

        // Will set freeze_xmin flags in freeze plan below.
        freeze_xmin = TransactionIdPrecedes(xid, cutoffs.OldestXmin);

        // Verify that xmin committed if and when freeze plan is executed.
        if freeze_xmin {
            frz.checkflags |= HEAP_FREEZE_CHECK_XMIN_COMMITTED;
        }
    }

    // Old-style VACUUM FULL is gone, but we have to process xvac for as long as
    // we support having MOVED_OFF/MOVED_IN tuples in the database.
    xid = HeapTupleHeaderGetXvac(tuple);
    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        debug_assert!(TransactionIdPrecedes(xid, cutoffs.OldestXmin));

        // For Xvac, we always freeze proactively.
        pagefrz.freeze_required = true;
        replace_xvac = true;
    }

    // Now process xmax.
    xid = frz.xmax;
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        // Raw xmax is a MultiXactId.
        let (newxmax, mflags) =
            FreezeMultiXactId(mcx, xid, tuple.t_infomask, cutoffs, pagefrz)?;

        if mflags & FRM_NOOP != 0 {
            // xmax is a MultiXactId, and nothing about it changes for now.
            debug_assert!(!MultiXactIdPrecedes(newxmax, cutoffs.MultiXactCutoff));
            debug_assert!(MultiXactIdIsValid(newxmax) && xid == newxmax);
        } else if mflags & FRM_RETURN_IS_XID != 0 {
            // xmax will become an updater Xid.
            debug_assert!(!TransactionIdPrecedes(newxmax, cutoffs.OldestXmin));
            frz.t_infomask &= !HEAP_XMAX_BITS;
            frz.xmax = newxmax;
            if mflags & FRM_MARK_COMMITTED != 0 {
                frz.t_infomask |= HEAP_XMAX_COMMITTED;
            }
            replace_xmax = true;
        } else if mflags & FRM_RETURN_IS_MULTI != 0 {
            // xmax is an old MultiXactId that we have to replace with a new one.
            debug_assert!(!MultiXactIdPrecedes(newxmax, cutoffs.OldestMxact));

            // We can't use GetMultiXactIdHintBits directly on the new multi here;
            // that routine initializes the masks to all zeroes, which would lose
            // other bits we need.
            frz.t_infomask &= !HEAP_XMAX_BITS;
            frz.t_infomask2 &= !HEAP_KEYS_UPDATED;
            let (newbits, newbits2) = GetMultiXactIdHintBits(mcx, newxmax)?;
            frz.t_infomask |= newbits;
            frz.t_infomask2 |= newbits2;
            frz.xmax = newxmax;
            replace_xmax = true;
        } else {
            // Freeze plan for tuple "freezes xmax" in the strictest sense.
            debug_assert!(mflags & FRM_INVALIDATE_XMAX != 0);
            debug_assert!(!TransactionIdIsValid(newxmax));
            // Will set freeze_xmax flags in freeze plan below.
            freeze_xmax = true;
        }

        // MultiXactId processing forces freezing (barring FRM_NOOP case).
        debug_assert!(pagefrz.freeze_required || (!freeze_xmax && !replace_xmax));
    } else if TransactionIdIsNormal(xid) {
        // Raw xmax is normal XID.
        if TransactionIdPrecedes(xid, cutoffs.relfrozenxid) {
            return Err(PgError::error(format_args_internal(&[
                "found xmax ",
                &xid.to_string(),
                " from before relfrozenxid ",
                &cutoffs.relfrozenxid.to_string(),
            ])));
        }

        // Will set freeze_xmax flags in freeze plan below.
        freeze_xmax = TransactionIdPrecedes(xid, cutoffs.OldestXmin);

        // Verify that xmax aborted if and when freeze plan is executed, provided
        // it's from an update.
        if freeze_xmax && !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            frz.checkflags |= HEAP_FREEZE_CHECK_XMAX_ABORTED;
        }
    } else if !TransactionIdIsValid(xid) {
        // Raw xmax is InvalidTransactionId XID.
        debug_assert!(tuple.t_infomask & HEAP_XMAX_IS_MULTI == 0);
        xmax_already_frozen = true;
    } else {
        return Err(PgError::error(format_args_internal(&[
            "found raw xmax ",
            &xid.to_string(),
            " (infomask not invalid and not multi)",
        ])));
    }

    if freeze_xmin {
        debug_assert!(!xmin_already_frozen);
        frz.t_infomask |= HEAP_XMIN_FROZEN;
    }
    if replace_xvac {
        // If a MOVED_OFF tuple is not dead, the xvac transaction must have
        // failed; whereas a non-dead MOVED_IN tuple must mean the xvac
        // transaction succeeded.
        debug_assert!(pagefrz.freeze_required);
        if tuple.t_infomask & HEAP_MOVED_OFF != 0 {
            frz.frzflags |= XLH_INVALID_XVAC;
        } else {
            frz.frzflags |= XLH_FREEZE_XVAC;
        }
    }
    if replace_xmax {
        debug_assert!(!xmax_already_frozen && !freeze_xmax);
        debug_assert!(pagefrz.freeze_required);
        // Already set replace_xmax flags in freeze plan earlier.
    }
    if freeze_xmax {
        debug_assert!(!xmax_already_frozen && !replace_xmax);

        frz.xmax = InvalidTransactionId;

        // The tuple might be marked either XMAX_INVALID or XMAX_COMMITTED +
        // LOCKED. Normalize to INVALID just to be sure no one gets confused. Also
        // get rid of the HEAP_KEYS_UPDATED bit.
        frz.t_infomask &= !HEAP_XMAX_BITS;
        frz.t_infomask |= HEAP_XMAX_INVALID;
        frz.t_infomask2 &= !HEAP_HOT_UPDATED;
        frz.t_infomask2 &= !HEAP_KEYS_UPDATED;
    }

    // Determine if this tuple is already totally frozen, or will become totally
    // frozen (provided caller executes freeze plans for the page).
    let totally_frozen = (freeze_xmin || xmin_already_frozen) && (freeze_xmax || xmax_already_frozen);

    if !pagefrz.freeze_required && !(xmin_already_frozen && xmax_already_frozen) {
        // So far no previous tuple from the page made freezing mandatory. Does
        // this tuple force caller to freeze the entire page?
        let (req, new_relfrozen, new_relmin) = heap_tuple_should_freeze(
            mcx,
            tuple,
            cutoffs,
            pagefrz.NoFreezePageRelfrozenXid,
            pagefrz.NoFreezePageRelminMxid,
        )?;
        pagefrz.freeze_required = req;
        pagefrz.NoFreezePageRelfrozenXid = new_relfrozen;
        pagefrz.NoFreezePageRelminMxid = new_relmin;
    }

    // Tell caller if this tuple has a usable freeze plan set in *frz.
    Ok((
        freeze_xmin || replace_xvac || replace_xmax || freeze_xmax,
        totally_frozen,
    ))
}

// ===========================================================================
// heap_execute_freeze_tuple — access/heapam.h (static inline).
// ===========================================================================

/// `heap_execute_freeze_tuple(tuple, frz)` (access/heapam.h) — execute the
/// prepared freezing of a tuple with caller's freeze plan, in place.
pub fn heap_execute_freeze_tuple(tuple: &mut HeapTupleHeaderData, frz: &HeapTupleFreeze) {
    HeapTupleHeaderSetXmax(tuple, frz.xmax);

    if frz.frzflags & XLH_FREEZE_XVAC != 0 {
        HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);
    }
    if frz.frzflags & XLH_INVALID_XVAC != 0 {
        HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);
    }

    tuple.t_infomask = frz.t_infomask;
    tuple.t_infomask2 = frz.t_infomask2;
}

/// `HeapTupleHeaderSetXmax(tup, xid)` (htup_details.h) — store the raw xmax in
/// the `THeap` fields (an on-page heap tuple is never the `TDatum` arm).
fn HeapTupleHeaderSetXmax(tup: &mut HeapTupleHeaderData, xid: TransactionId) {
    match &mut tup.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_xmax = xid,
        HeapTupleHeaderChoice::TDatum(_) => {
            // A TDatum tuple has no xmax field; convert to THeap to store it,
            // matching the C in-place union write through the t_heap view.
            tup.t_choice = HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0,
                t_xmax: xid,
                t_field3: HeapTupleField3::default(),
            });
        }
    }
}

/// `HeapTupleHeaderSetXvac(tup, xid)` (htup_details.h) — store `t_xvac` in the
/// `TXvac` arm of `t_field3` (only used on `HEAP_MOVED` tuples).
fn HeapTupleHeaderSetXvac(tup: &mut HeapTupleHeaderData, xid: TransactionId) {
    match &mut tup.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_field3 = HeapTupleField3::TXvac(xid),
        HeapTupleHeaderChoice::TDatum(_) => {
            tup.t_choice = HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0,
                t_xmax: 0,
                t_field3: HeapTupleField3::TXvac(xid),
            });
        }
    }
}

// ===========================================================================
// heap_freeze_tuple — heapam.c (CLUSTER's in-place no-WAL freeze).
// ===========================================================================

/// `heap_freeze_tuple(tuple, relfrozenxid, relminmxid, FreezeLimit,
/// MultiXactCutoff)` (heapam.c) — freeze tuple in place, without WAL logging.
pub fn heap_freeze_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &mut HeapTupleHeaderData,
    relfrozenxid: TransactionId,
    relminmxid: TransactionId,
    FreezeLimit: TransactionId,
    MultiXactCutoff: TransactionId,
) -> PgResult<bool> {
    let cutoffs = VacuumCutoffs {
        relfrozenxid,
        relminmxid,
        OldestXmin: FreezeLimit,
        OldestMxact: MultiXactCutoff,
        FreezeLimit,
        MultiXactCutoff,
    };

    let mut pagefrz = HeapPageFreeze {
        freeze_required: true,
        FreezePageRelfrozenXid: FreezeLimit,
        FreezePageRelminMxid: MultiXactCutoff,
        NoFreezePageRelfrozenXid: FreezeLimit,
        NoFreezePageRelminMxid: MultiXactCutoff,
    };

    let mut frz = HeapTupleFreeze::default();
    let (do_freeze, _totally_frozen) =
        heap_prepare_freeze_tuple(mcx, tuple, &cutoffs, &mut pagefrz, &mut frz)?;

    // Note that because this is not a WAL-logged operation, we don't need to
    // fill in the offset in the freeze record.
    if do_freeze {
        heap_execute_freeze_tuple(tuple, &frz);
    }
    Ok(do_freeze)
}

// ===========================================================================
// heap_tuple_needs_eventual_freeze — heapam.c.
// ===========================================================================

/// `heap_tuple_needs_eventual_freeze(tuple)` (heapam.c) — whether any of the XID
/// fields of a tuple will eventually require freezing.
pub fn heap_tuple_needs_eventual_freeze(tuple: &HeapTupleHeaderData) -> bool {
    // If xmin is a normal transaction ID, this tuple is definitely not frozen.
    let mut xid = HeapTupleHeaderGetXmin(tuple);
    if TransactionIdIsNormal(xid) {
        return true;
    }

    // If xmax is a valid xact or multixact, this tuple is also not frozen.
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        let multi: MultiXactId = HeapTupleHeaderGetRawXmax(tuple);
        if MultiXactIdIsValid(multi) {
            return true;
        }
    } else {
        xid = HeapTupleHeaderGetRawXmax(tuple);
        if TransactionIdIsNormal(xid) {
            return true;
        }
    }

    if tuple.t_infomask & HEAP_MOVED != 0 {
        xid = HeapTupleHeaderGetXvac(tuple);
        if TransactionIdIsNormal(xid) {
            return true;
        }
    }

    false
}

// ===========================================================================
// heap_tuple_should_freeze — heapam.c.
// ===========================================================================

/// `heap_tuple_should_freeze(tuple, cutoffs, &NoFreezePageRelfrozenXid,
/// &NoFreezePageRelminMxid)` (heapam.c) — would `heap_prepare_freeze_tuple`
/// force freezing of the heap page? Also advances the "no freeze" trackers.
/// Returns `(freeze, NoFreezePageRelfrozenXid, NoFreezePageRelminMxid)`.
pub fn heap_tuple_should_freeze<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleHeaderData,
    cutoffs: &VacuumCutoffs,
    mut no_freeze_relfrozen_xid: TransactionId,
    mut no_freeze_relmin_mxid: MultiXactId,
) -> PgResult<(bool, TransactionId, MultiXactId)> {
    let mut freeze = false;

    // First deal with xmin.
    let mut xid = HeapTupleHeaderGetXmin(tuple);
    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        if TransactionIdPrecedes(xid, no_freeze_relfrozen_xid) {
            no_freeze_relfrozen_xid = xid;
        }
        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            freeze = true;
        }
    }

    // Now deal with xmax.
    xid = InvalidTransactionId;
    let mut multi = InvalidMultiXactId;
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        multi = HeapTupleHeaderGetRawXmax(tuple);
    } else {
        xid = HeapTupleHeaderGetRawXmax(tuple);
    }

    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        // xmax is a non-permanent XID.
        if TransactionIdPrecedes(xid, no_freeze_relfrozen_xid) {
            no_freeze_relfrozen_xid = xid;
        }
        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            freeze = true;
        }
    } else if !MultiXactIdIsValid(multi) {
        // xmax is a permanent XID or invalid MultiXactId/XID.
    } else if HEAP_LOCKED_UPGRADED(tuple.t_infomask) {
        // xmax is a pg_upgrade'd MultiXact, which can't have updater XID.
        if MultiXactIdPrecedes(multi, no_freeze_relmin_mxid) {
            no_freeze_relmin_mxid = multi;
        }
        // heap_prepare_freeze_tuple always freezes pg_upgrade'd xmax.
        freeze = true;
    } else {
        // xmax is a MultiXactId that may have an updater XID.
        debug_assert!(MultiXactIdPrecedesOrEquals(cutoffs.relminmxid, multi));
        if MultiXactIdPrecedes(multi, no_freeze_relmin_mxid) {
            no_freeze_relmin_mxid = multi;
        }
        if MultiXactIdPrecedes(multi, cutoffs.MultiXactCutoff) {
            freeze = true;
        }

        // need to check whether any member of the mxact is old.
        let members = multixact_seam::get_multi_xact_id_members::call(
            mcx,
            multi,
            false,
            HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask),
        )?;
        for member in members.iter() {
            xid = member.xid;
            debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
            if TransactionIdPrecedes(xid, no_freeze_relfrozen_xid) {
                no_freeze_relfrozen_xid = xid;
            }
            if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
                freeze = true;
            }
        }
        // C pfree(members); owned vec drops here.
    }

    if tuple.t_infomask & HEAP_MOVED != 0 {
        xid = HeapTupleHeaderGetXvac(tuple);
        if TransactionIdIsNormal(xid) {
            debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
            if TransactionIdPrecedes(xid, no_freeze_relfrozen_xid) {
                no_freeze_relfrozen_xid = xid;
            }
            // heap_prepare_freeze_tuple forces xvac freezing.
            freeze = true;
        }
    }

    Ok((freeze, no_freeze_relfrozen_xid, no_freeze_relmin_mxid))
}

// ===========================================================================
// HeapTupleHeaderAdvanceConflictHorizon — heapam.c.
// ===========================================================================

/// `HeapTupleHeaderAdvanceConflictHorizon(tuple, &snapshotConflictHorizon)`
/// (heapam.c) — ratchet forward the snapshot conflict horizon using any
/// committed XIDs in an obsolescent tuple about to be physically removed.
pub fn HeapTupleHeaderAdvanceConflictHorizon(
    tuple: &HeapTupleHeaderData,
    snapshot_conflict_horizon: &mut TransactionId,
) -> PgResult<()> {
    let xmin = HeapTupleHeaderGetXmin(tuple);
    let xmax = HeapTupleHeaderGetUpdateXid(tuple)?;
    let xvac = HeapTupleHeaderGetXvac(tuple);

    if tuple.t_infomask & HEAP_MOVED != 0 {
        if TransactionIdPrecedes(*snapshot_conflict_horizon, xvac) {
            *snapshot_conflict_horizon = xvac;
        }
    }

    // Ignore tuples inserted by an aborted transaction or if the tuple was
    // updated/deleted by the inserting transaction.
    if HeapTupleHeaderXminCommitted(tuple)
        || (!HeapTupleHeaderXminInvalid(tuple) && transaction_id_did_commit(xmin)?)
    {
        if xmax != xmin && TransactionIdFollows(xmax, *snapshot_conflict_horizon) {
            *snapshot_conflict_horizon = xmax;
        }
    }
    Ok(())
}

// ===========================================================================
// heap_pre_freeze_checks / heap_freeze_prepared_tuples — page-bound seam-level
// routines that materialize the on-page HeapTupleHeader at each plan's offset.
// ===========================================================================

/// `heap_pre_freeze_checks(buffer, tuples, ntuples)` (heapam.c) — xmin/xmax XID
/// status sanity checks before executing freeze plans. Reads each plan's tuple
/// header off the buffer's page (deliberately avoiding hint bits).
pub fn heap_pre_freeze_checks<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    tuples: &[HeapTupleFreeze],
) -> PgResult<()> {
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = page::PageRef::new(page_bytes)?;
        for frz in tuples.iter() {
            let item_id = page::PageGetItemId(&page, frz.offset)?;
            let item = page::PageGetItem(&page, &item_id)?;
            let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;

            // Deliberately avoid relying on tuple hint bits here.
            if frz.checkflags & HEAP_FREEZE_CHECK_XMIN_COMMITTED != 0 {
                let xmin = ::types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(&htup);
                debug_assert!(!::heapam_visibility::htup::HeapTupleHeaderXminFrozen(&htup));
                if !transaction_id_did_commit(xmin)? {
                    return Err(PgError::error(format_args_internal(&[
                        "uncommitted xmin ",
                        &xmin.to_string(),
                        " needs to be frozen",
                    ])));
                }
            }

            // TransactionIdDidAbort won't work reliably in the presence of XIDs
            // left behind by transactions that were in progress during a crash,
            // so we can only check that xmax didn't commit.
            if frz.checkflags & HEAP_FREEZE_CHECK_XMAX_ABORTED != 0 {
                let xmax = HeapTupleHeaderGetRawXmax(&htup);
                debug_assert!(TransactionIdIsNormal(xmax));
                if transaction_id_did_commit(xmax)? {
                    return Err(PgError::error(format_args_internal(&[
                        "cannot freeze committed xmax ",
                        &xmax.to_string(),
                    ])));
                }
            }
        }
        Ok(())
    })
}

/// `heap_freeze_prepared_tuples(buffer, tuples, ntuples)` (heapam.c) — execute
/// freezing of one or more heap tuples on a page. Caller sets `offset` in each
/// plan. Must be called in a critical section that marks the buffer dirty and,
/// if needed, emits WAL.
pub fn heap_freeze_prepared_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    tuples: &[HeapTupleFreeze],
) -> PgResult<()> {
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        // Resolve each plan's item location off the (immutable) line pointers,
        // then write the executed header back into the page bytes in place —
        // mirroring C's `heap_execute_freeze_tuple(htup, frz)` through the
        // `HeapTupleHeader` page pointer.
        for frz in tuples.iter() {
            let (off, len) = {
                let page = page::PageRef::new(page_bytes)?;
                let item_id = page::PageGetItemId(&page, frz.offset)?;
                if !item_id.has_storage() {
                    return Err(PgError::error("item identifier has no storage"));
                }
                (item_id.lp_off() as usize, item_id.lp_len() as usize)
            };
            let item = page_bytes
                .get_mut(off..off + len)
                .ok_or_else(|| PgError::error("item storage is outside page"))?;
            let mut htup = HeapTupleHeaderData::read_on_page(mcx, item)?;
            heap_execute_freeze_tuple(&mut htup, frz);
            htup.write_on_page(item)?;
        }
        Ok(())
    })
}

// ===========================================================================
// TransactionIdDidCommit — clog lookup through the transam owner seam, threading
// C's TransactionXmin (snapmgr.c global) via the snapmgr seam.
// ===========================================================================

fn transaction_id_did_commit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = snapmgr_pc_seams::transaction_xmin::call()?;
    transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)
}

// ===========================================================================
// errmsg_internal helper — assemble a corruption-report message from parts.
// ===========================================================================

use std::string::{String, ToString};

/// Concatenate message fragments into a `String` for `errmsg_internal`. (The
/// repo's `PgError::error` takes a `String`; C uses `errmsg_internal("...%u...",
/// ...)`.)
fn format_args_internal(parts: &[&str]) -> String {
    let mut s = String::new();
    for p in parts {
        s.push_str(p);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;
    use ::types_tuple::heaptuple::{
        BlockIdData, HeapTupleHeaderData, ItemPointerData, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
    };

    /// A normal on-page heap-tuple header (THeap arm), with the given infomask,
    /// xmin, and a normal (non-multi) xmax.
    fn header<'mcx>(
        mcx: Mcx<'mcx>,
        infomask: u16,
        xmin: TransactionId,
        xmax: TransactionId,
    ) -> HeapTupleHeaderData<'mcx> {
        HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: xmin,
                t_xmax: xmax,
                t_field3: HeapTupleField3::TCid(0),
            }),
            t_ctid: ItemPointerData {
                ip_blkid: BlockIdData::new(0),
                ip_posid: 0,
            },
            t_infomask2: 0,
            t_infomask: infomask,
            t_hoff: 23,
            t_bits: ::mcx::PgVec::new_in(mcx),
        }
    }

    fn cutoffs(relfrozen: u32, oldest_xmin: u32, freeze_limit: u32) -> VacuumCutoffs {
        VacuumCutoffs {
            relfrozenxid: relfrozen,
            relminmxid: 1,
            OldestXmin: oldest_xmin,
            OldestMxact: 1,
            FreezeLimit: freeze_limit,
            MultiXactCutoff: 1,
        }
    }

    #[test]
    fn multixactid_precedes_is_circular() {
        assert!(MultiXactIdPrecedes(5, 6));
        assert!(!MultiXactIdPrecedes(6, 5));
        assert!(MultiXactIdPrecedesOrEquals(5, 5));
        // wraparound: a very large multi "precedes" a small one.
        assert!(MultiXactIdPrecedes(0xFFFF_FFF0, 8));
        assert!(MultiXactIdIsValid(1));
        assert!(!MultiXactIdIsValid(0));
    }

    #[test]
    fn isupdate_from_mxstatus_matches_c() {
        assert!(!ISUPDATE_from_mxstatus(MultiXactStatus::ForKeyShare));
        assert!(!ISUPDATE_from_mxstatus(MultiXactStatus::ForShare));
        assert!(!ISUPDATE_from_mxstatus(MultiXactStatus::ForNoKeyUpdate));
        assert!(!ISUPDATE_from_mxstatus(MultiXactStatus::ForUpdate));
        assert!(ISUPDATE_from_mxstatus(MultiXactStatus::NoKeyUpdate));
        assert!(ISUPDATE_from_mxstatus(MultiXactStatus::Update));
    }

    #[test]
    fn needs_eventual_freeze_tracks_live_xids() {
        let ctx = MemoryContext::new("test");
        // Normal xmin -> needs freeze.
        let t = header(ctx.mcx(), 0, 100, 0);
        assert!(heap_tuple_needs_eventual_freeze(&t));
        // Frozen xmin (committed|invalid), invalid xmax -> no freeze needed.
        let t = header(ctx.mcx(), HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID, 100, 0);
        assert!(!heap_tuple_needs_eventual_freeze(&t));
        // Frozen xmin but a normal xmax -> still needs freeze.
        let t = header(ctx.mcx(), HEAP_XMIN_FROZEN, 100, 555);
        assert!(heap_tuple_needs_eventual_freeze(&t));
    }

    #[test]
    fn prepare_freeze_normal_xmin_below_oldestxmin_freezes() {
        let ctx = MemoryContext::new("test");
        // xmin=100 (normal), xmax invalid. relfrozen=50, OldestXmin=200,
        // FreezeLimit=150 -> freeze_xmin true; XMIN_COMMITTED checkflag set.
        let t = header(ctx.mcx(), HEAP_XMAX_INVALID, 100, 0);
        let cut = cutoffs(50, 200, 150);
        let mut pagefrz = HeapPageFreeze {
            freeze_required: false,
            FreezePageRelfrozenXid: 200,
            FreezePageRelminMxid: 1,
            NoFreezePageRelfrozenXid: 200,
            NoFreezePageRelminMxid: 1,
        };
        let mut frz = HeapTupleFreeze::default();
        let (do_freeze, totally_frozen) =
            heap_prepare_freeze_tuple(ctx.mcx(), &t, &cut, &mut pagefrz, &mut frz).unwrap();
        assert!(do_freeze);
        assert!(totally_frozen); // xmin freezes, xmax already frozen
        assert!(frz.checkflags & HEAP_FREEZE_CHECK_XMIN_COMMITTED != 0);
        assert!(frz.t_infomask & HEAP_XMIN_FROZEN == HEAP_XMIN_FROZEN);
        // NoFreeze tracker for xmin was ratcheted back to 100.
    }

    #[test]
    fn prepare_freeze_recent_xmin_no_freeze() {
        let ctx = MemoryContext::new("test");
        // xmin=300 >= OldestXmin=200 -> no xmin freeze; xmax invalid (frozen).
        let t = header(ctx.mcx(), HEAP_XMAX_INVALID, 300, 0);
        let cut = cutoffs(50, 200, 150);
        let mut pagefrz = HeapPageFreeze {
            freeze_required: false,
            FreezePageRelfrozenXid: 200,
            FreezePageRelminMxid: 1,
            NoFreezePageRelfrozenXid: 200,
            NoFreezePageRelminMxid: 1,
        };
        let mut frz = HeapTupleFreeze::default();
        let (do_freeze, _totally) =
            heap_prepare_freeze_tuple(ctx.mcx(), &t, &cut, &mut pagefrz, &mut frz).unwrap();
        assert!(!do_freeze);
        // xmin 300 is below FreezeLimit? No (300 >= 150), so no forced freeze.
        assert!(!pagefrz.freeze_required);
        // The no-freeze relfrozen tracker only ratchets *back*; xmin=300 is
        // newer than the starting tracker (200), so it stays 200.
        assert_eq!(pagefrz.NoFreezePageRelfrozenXid, 200);
    }

    #[test]
    fn execute_freeze_sets_xmax_and_masks() {
        let ctx = MemoryContext::new("test");
        let mut t = header(ctx.mcx(), HEAP_XMIN_COMMITTED, 100, 555);
        let frz = HeapTupleFreeze {
            xmax: InvalidTransactionId,
            t_infomask2: 0x1,
            t_infomask: HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID,
            frzflags: 0,
            checkflags: 0,
            offset: 1,
        };
        heap_execute_freeze_tuple(&mut t, &frz);
        assert_eq!(HeapTupleHeaderGetRawXmax(&t), InvalidTransactionId);
        assert_eq!(t.t_infomask, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID);
        assert_eq!(t.t_infomask2, 0x1);
    }

    #[test]
    fn execute_freeze_xvac_paths() {
        let ctx = MemoryContext::new("test");
        // XLH_FREEZE_XVAC -> set xvac to FrozenTransactionId.
        let mut t = header(ctx.mcx(), HEAP_MOVED, 100, 0);
        let mut frz = HeapTupleFreeze::default();
        frz.frzflags = XLH_FREEZE_XVAC;
        frz.t_infomask = t.t_infomask;
        frz.t_infomask2 = t.t_infomask2;
        heap_execute_freeze_tuple(&mut t, &frz);
        assert_eq!(HeapTupleHeaderGetXvac(&t), FrozenTransactionId);

        // XLH_INVALID_XVAC -> set xvac to InvalidTransactionId.
        let mut t = header(ctx.mcx(), HEAP_MOVED_OFF, 100, 0);
        let mut frz = HeapTupleFreeze::default();
        frz.frzflags = XLH_INVALID_XVAC;
        frz.t_infomask = t.t_infomask;
        frz.t_infomask2 = t.t_infomask2;
        heap_execute_freeze_tuple(&mut t, &frz);
        assert_eq!(HeapTupleHeaderGetXvac(&t), InvalidTransactionId);
    }

    #[test]
    fn should_freeze_normal_xmin_below_freezelimit() {
        let ctx = MemoryContext::new("test");
        // xmin=100 < FreezeLimit=150 -> should freeze; tracker ratchets to 100.
        let t = header(ctx.mcx(), HEAP_XMAX_INVALID, 100, 0);
        let cut = cutoffs(50, 200, 150);
        let (freeze, relfrozen, relmin) =
            heap_tuple_should_freeze(ctx.mcx(), &t, &cut, 200, 1).unwrap();
        assert!(freeze);
        assert_eq!(relfrozen, 100);
        assert_eq!(relmin, 1);
    }

    #[test]
    fn advance_conflict_horizon_uses_committed_xmax() {
        // We can exercise the HEAP_MOVED xvac branch without hitting clog: a
        // non-committed, non-MOVED tuple leaves the horizon untouched. The
        // committed-xmin branch needs the transam seam, so it's covered by the
        // integration layer; here we check the xvac ratchet.
        let ctx = MemoryContext::new("test");
        let mut t = header(ctx.mcx(), HEAP_MOVED | HEAP_XMIN_INVALID, 100, 0);
        // Put a normal xvac.
        t.t_choice = HeapTupleHeaderChoice::THeap(HeapTupleFields {
            t_xmin: 100,
            t_xmax: 0,
            t_field3: HeapTupleField3::TXvac(777),
        });
        let mut horizon: TransactionId = 0;
        HeapTupleHeaderAdvanceConflictHorizon(&t, &mut horizon).unwrap();
        assert_eq!(horizon, 777);
    }

    #[test]
    fn on_page_header_round_trips() {
        let ctx = MemoryContext::new("test");
        // Build a 23-byte item buffer, write a header, read it back.
        let mut item = [0u8; 32];
        let src = HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0x1111_2222,
                t_xmax: 0x3333_4444,
                t_field3: HeapTupleField3::TCid(0x5555_6666),
            }),
            t_ctid: ItemPointerData {
                ip_blkid: BlockIdData::new(0x000A_BBCC),
                ip_posid: 9,
            },
            t_infomask2: 0x0102,
            t_infomask: HEAP_XMIN_COMMITTED,
            t_hoff: 24,
            t_bits: ::mcx::PgVec::new_in(ctx.mcx()),
        };
        src.write_on_page(&mut item).unwrap();
        let back = HeapTupleHeaderData::read_on_page(ctx.mcx(), &item).unwrap();
        assert_eq!(HeapTupleHeaderGetRawXmax(&back), 0x3333_4444);
        assert_eq!(
            ::types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(&back),
            0x1111_2222
        );
        assert_eq!(back.t_ctid.ip_blkid.block_number(), 0x000A_BBCC);
        assert_eq!(back.t_ctid.ip_posid, 9);
        assert_eq!(back.t_infomask2, 0x0102);
        assert_eq!(back.t_infomask, HEAP_XMIN_COMMITTED);
        assert_eq!(back.t_hoff, 24);
        // t_field3 is TCid when HEAP_MOVED isn't set.
        match back.t_choice {
            HeapTupleHeaderChoice::THeap(f) => match f.t_field3 {
                HeapTupleField3::TCid(c) => assert_eq!(c, 0x5555_6666),
                _ => panic!("expected TCid"),
            },
            _ => panic!("expected THeap"),
        }
    }

    #[test]
    fn on_page_header_moved_decodes_xvac() {
        let ctx = MemoryContext::new("test");
        let mut item = [0u8; 23];
        let src = HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 1,
                t_xmax: 2,
                t_field3: HeapTupleField3::TXvac(0xABCD),
            }),
            t_ctid: ItemPointerData::default(),
            t_infomask2: 0,
            t_infomask: HEAP_MOVED,
            t_hoff: 23,
            t_bits: ::mcx::PgVec::new_in(ctx.mcx()),
        };
        src.write_on_page(&mut item).unwrap();
        let back = HeapTupleHeaderData::read_on_page(ctx.mcx(), &item).unwrap();
        assert_eq!(HeapTupleHeaderGetXvac(&back), 0xABCD);
    }
}
