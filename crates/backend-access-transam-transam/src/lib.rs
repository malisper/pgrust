#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `src/backend/access/transam/transam.c` — the high-level
//! access-method interface to the transaction (commit) log.
//!
//! Answers "did xid commit / abort?" over pg_xact with a single-item result
//! cache, marks transaction trees committed/aborted, implements the
//! wraparound-aware xid comparisons, finds the latest xid in a tree, and
//! returns a commit-record LSN bound. The `transam.h` inline predicates
//! (`TransactionIdEquals` / `IsValid` / `IsNormal`) live here too.
//!
//! C's `ereport(ERROR)` channel (clog / subtrans SLRU I/O failures) surfaces
//! as `PgResult`. Outward boundaries: the clog status layer through
//! `backend-access-transam-clog-seams` and the pg_subtrans parent map through
//! `backend-access-transam-subtrans-seams` (both panic until their owners
//! land), plus `elog(WARNING, ...)` directly via `backend-utils-error`.
//! C reads the `TransactionXmin` global from snapmgr.c; here callers pass it
//! explicitly (read off their snapshot facet once snapmgr lands), so no
//! ambient-global seam exists.

use std::cell::Cell;

use backend_access_transam_clog_seams as clog_seams;
use backend_access_transam_subtrans_seams as subtrans_seams;
use backend_utils_error::elog;
use types_core::xact::{
    BootstrapTransactionId, FirstNormalTransactionId, FrozenTransactionId, InvalidTransactionId,
    InvalidXLogRecPtr, XidStatus, TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED,
    TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_SUB_COMMITTED,
};
use types_core::{TransactionId, XLogRecPtr};
use types_error::{PgResult, WARNING};

thread_local! {
    // Single-item cache for results of TransactionLogFetch. It's worth having
    // such a cache because we frequently find ourselves repeatedly checking
    // the same XID, for example when scanning a table just after a bulk
    // insert, update, or delete. Per-backend state in C (file-scope statics),
    // hence thread-local here.
    static cachedFetchXid: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    static cachedFetchXidStatus: Cell<XidStatus> = const { Cell::new(0) };
    static cachedCommitLSN: Cell<XLogRecPtr> = const { Cell::new(0) };
}

/// `TransactionLogFetch` --- fetch commit status of specified transaction id.
fn TransactionLogFetch(transactionId: TransactionId) -> PgResult<XidStatus> {
    // Before going to the commit log manager, check our single item cache to
    // see if we didn't just check the transaction status a moment ago.
    if TransactionIdEquals(transactionId, cachedFetchXid.get()) {
        return Ok(cachedFetchXidStatus.get());
    }

    // Also, check to see if the transaction ID is a permanent one.
    if !TransactionIdIsNormal(transactionId) {
        if TransactionIdEquals(transactionId, BootstrapTransactionId) {
            return Ok(TRANSACTION_STATUS_COMMITTED);
        }
        if TransactionIdEquals(transactionId, FrozenTransactionId) {
            return Ok(TRANSACTION_STATUS_COMMITTED);
        }
        return Ok(TRANSACTION_STATUS_ABORTED);
    }

    // Get the transaction status.
    let (xidstatus, xidlsn) = clog_seams::transaction_id_get_status::call(transactionId)?;

    // Cache it, but DO NOT cache status for unfinished or sub-committed
    // transactions! We only cache status that is guaranteed not to change.
    if xidstatus != TRANSACTION_STATUS_IN_PROGRESS && xidstatus != TRANSACTION_STATUS_SUB_COMMITTED
    {
        cachedFetchXid.set(transactionId);
        cachedFetchXidStatus.set(xidstatus);
        cachedCommitLSN.set(xidlsn);
    }

    Ok(xidstatus)
}

/// `TransactionIdDidCommit` --- true iff transaction associated with the
/// identifier did commit.
///
/// Note: Assumes transaction identifier is valid and exists in clog.
///
/// `transaction_xmin` is C's `TransactionXmin` global (snapmgr.c), passed
/// explicitly: the oldest xid still considered running by this backend's
/// snapshots.
pub fn TransactionIdDidCommit(
    transactionId: TransactionId,
    transaction_xmin: TransactionId,
) -> PgResult<bool> {
    let xidstatus = TransactionLogFetch(transactionId)?;

    // If it's marked committed, it's committed.
    if xidstatus == TRANSACTION_STATUS_COMMITTED {
        return Ok(true);
    }

    // If it's marked subcommitted, we have to check the parent recursively.
    // However, if it's older than TransactionXmin, we can't look at
    // pg_subtrans; instead assume that the parent crashed without cleaning up
    // its children.
    //
    // Originally we Assert'ed that the result of SubTransGetParent was not
    // zero. However with the introduction of prepared transactions, there can
    // be a window just after database startup where we do not have complete
    // knowledge in pg_subtrans of the transactions after TransactionXmin.
    // StartupSUBTRANS() has ensured that any missing information will be
    // zeroed. Since this case should not happen under normal conditions, it
    // seems reasonable to emit a WARNING for it.
    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        if TransactionIdPrecedes(transactionId, transaction_xmin) {
            return Ok(false);
        }
        let parentXid = subtrans_seams::sub_trans_get_parent::call(transactionId)?;
        if !TransactionIdIsValid(parentXid) {
            elog(
                WARNING,
                format!("no pg_subtrans entry for subcommitted XID {transactionId}"),
            )?;
            return Ok(false);
        }
        return TransactionIdDidCommit(parentXid, transaction_xmin);
    }

    // It's not committed.
    Ok(false)
}

/// `TransactionIdDidAbort` --- true iff transaction associated with the
/// identifier did abort.
///
/// Note: Assumes transaction identifier is valid and exists in clog.
///
/// Returns true only for explicitly aborted transactions, as transactions
/// implicitly aborted due to a crash will commonly still appear to be
/// in-progress in the clog. Most of the time TransactionIdDidCommit(), with a
/// preceding TransactionIdIsInProgress() check, should be used instead of
/// TransactionIdDidAbort().
///
/// `transaction_xmin` is C's `TransactionXmin` global (snapmgr.c), passed
/// explicitly: the oldest xid still considered running by this backend's
/// snapshots.
pub fn TransactionIdDidAbort(
    transactionId: TransactionId,
    transaction_xmin: TransactionId,
) -> PgResult<bool> {
    let xidstatus = TransactionLogFetch(transactionId)?;

    // If it's marked aborted, it's aborted.
    if xidstatus == TRANSACTION_STATUS_ABORTED {
        return Ok(true);
    }

    // If it's marked subcommitted, we have to check the parent recursively.
    // However, if it's older than TransactionXmin, we can't look at
    // pg_subtrans; instead assume that the parent crashed without cleaning up
    // its children.
    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        if TransactionIdPrecedes(transactionId, transaction_xmin) {
            return Ok(true);
        }
        let parentXid = subtrans_seams::sub_trans_get_parent::call(transactionId)?;
        if !TransactionIdIsValid(parentXid) {
            // see notes in TransactionIdDidCommit
            elog(
                WARNING,
                format!("no pg_subtrans entry for subcommitted XID {transactionId}"),
            )?;
            return Ok(true);
        }
        return TransactionIdDidAbort(parentXid, transaction_xmin);
    }

    // It's not aborted.
    Ok(false)
}

/// `TransactionIdCommitTree` --- marks the given transaction and children as
/// committed.
///
/// "xid" is a toplevel transaction commit, and the xids array contains its
/// committed subtransactions.
///
/// This commit operation is not guaranteed to be atomic, but if not, subxids
/// are correctly marked subcommit first.
pub fn TransactionIdCommitTree(xid: TransactionId, xids: &[TransactionId]) -> PgResult<()> {
    clog_seams::transaction_id_set_tree_status::call(
        xid,
        xids,
        TRANSACTION_STATUS_COMMITTED,
        InvalidXLogRecPtr,
    )
}

/// `TransactionIdAsyncCommitTree` --- same as above, but for async commits.
/// The commit record LSN is needed.
pub fn TransactionIdAsyncCommitTree(
    xid: TransactionId,
    xids: &[TransactionId],
    lsn: XLogRecPtr,
) -> PgResult<()> {
    clog_seams::transaction_id_set_tree_status::call(xid, xids, TRANSACTION_STATUS_COMMITTED, lsn)
}

/// `TransactionIdAbortTree` --- marks the given transaction and children as
/// aborted.
///
/// "xid" is a toplevel transaction commit, and the xids array contains its
/// committed subtransactions.
///
/// We don't need to worry about the non-atomic behavior, since any onlookers
/// will consider all the xacts as not-yet-committed anyway.
pub fn TransactionIdAbortTree(xid: TransactionId, xids: &[TransactionId]) -> PgResult<()> {
    clog_seams::transaction_id_set_tree_status::call(
        xid,
        xids,
        TRANSACTION_STATUS_ABORTED,
        InvalidXLogRecPtr,
    )
}

/// `TransactionIdEquals(id1, id2)` (transam.h).
#[inline]
pub const fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

/// `TransactionIdIsValid(xid)` (transam.h).
#[inline]
pub const fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` (transam.h).
#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes` --- is id1 logically < id2?
pub fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    // If either ID is a permanent XID then we can just do unsigned
    // comparison. If both are normal, do a modulo-2^32 comparison.
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `TransactionIdPrecedesOrEquals` --- is id1 logically <= id2?
pub fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff <= 0
}

/// `TransactionIdFollows` --- is id1 logically > id2?
pub fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff > 0
}

/// `TransactionIdFollowsOrEquals` --- is id1 logically >= id2?
pub fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff >= 0
}

/// `TransactionIdLatest` --- get latest XID among a main xact and its
/// children.
pub fn TransactionIdLatest(mainxid: TransactionId, xids: &[TransactionId]) -> TransactionId {
    // In practice it is highly likely that the xids[] array is sorted, and so
    // we could save some cycles by just taking the last child XID, but this
    // probably isn't so performance-critical that it's worth depending on
    // that assumption. But just to show we're not totally stupid, scan the
    // array back-to-front to avoid useless assignments.
    let mut result = mainxid;
    for &xid in xids.iter().rev() {
        if TransactionIdPrecedes(result, xid) {
            result = xid;
        }
    }
    result
}

/// `TransactionIdGetCommitLSN`.
///
/// This function returns an LSN that is late enough to be able to guarantee
/// that if we flush up to the LSN returned then we will have flushed the
/// transaction's commit record to disk.
///
/// The result is not necessarily the exact LSN of the transaction's commit
/// record! For example, for long-past transactions (those whose clog pages
/// already migrated to disk), we'll return InvalidXLogRecPtr. Also, because
/// we group transactions on the same clog page to conserve storage, we might
/// return the LSN of a later transaction that falls into the same group.
pub fn TransactionIdGetCommitLSN(xid: TransactionId) -> PgResult<XLogRecPtr> {
    // Currently, all uses of this function are for xids that were just
    // reported to be committed by TransactionLogFetch, so we expect that
    // checking TransactionLogFetch's cache will usually succeed and avoid an
    // extra trip to shared memory.
    if TransactionIdEquals(xid, cachedFetchXid.get()) {
        return Ok(cachedCommitLSN.get());
    }

    // Special XIDs are always known committed.
    if !TransactionIdIsNormal(xid) {
        return Ok(InvalidXLogRecPtr);
    }

    // Get the transaction status.
    let (_status, result) = clog_seams::transaction_id_get_status::call(xid)?;

    Ok(result)
}

#[cfg(test)]
mod tests;
