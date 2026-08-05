#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use elog::elog;
use procarray::TransactionXmin;
use types_core::xact::{
    InvalidXLogRecPtr, XidStatus, TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED,
    TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_SUB_COMMITTED,
};
use types_core::{BootstrapTransactionId, FrozenTransactionId, InvalidTransactionId, TransactionId, XLogRecPtr};
use types_error::{PgResult, WARNING};

pub use types_core::{
    TransactionIdEquals, TransactionIdFollows, TransactionIdFollowsOrEquals,
    TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};

thread_local! {
    // Single-item TransactionLogFetch cache: table scans right after a bulk
    // write re-check the same XID per tuple (rule-5 load-bearing).
    static cachedFetchXid: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    static cachedFetchXidStatus: Cell<XidStatus> = const { Cell::new(0) };
    static cachedCommitLSN: Cell<XLogRecPtr> = const { Cell::new(0) };
}

fn TransactionLogFetch(transactionId: TransactionId) -> PgResult<XidStatus> {
    if TransactionIdEquals(transactionId, cachedFetchXid.get()) {
        return Ok(cachedFetchXidStatus.get());
    }

    if !TransactionIdIsNormal(transactionId) {
        if TransactionIdEquals(transactionId, BootstrapTransactionId) {
            return Ok(TRANSACTION_STATUS_COMMITTED);
        }
        if TransactionIdEquals(transactionId, FrozenTransactionId) {
            return Ok(TRANSACTION_STATUS_COMMITTED);
        }
        return Ok(TRANSACTION_STATUS_ABORTED);
    }

    let (xidstatus, xidlsn) = clog::TransactionIdGetStatus(transactionId)?;

    // Only final states are cacheable (in-progress/subcommitted still change).
    if xidstatus != TRANSACTION_STATUS_IN_PROGRESS && xidstatus != TRANSACTION_STATUS_SUB_COMMITTED
    {
        cachedFetchXid.set(transactionId);
        cachedFetchXidStatus.set(xidstatus);
        cachedCommitLSN.set(xidlsn);
    }

    Ok(xidstatus)
}

pub fn TransactionIdDidCommit(transactionId: TransactionId) -> PgResult<bool> {
    let xidstatus = TransactionLogFetch(transactionId)?;

    if xidstatus == TRANSACTION_STATUS_COMMITTED {
        return Ok(true);
    }

    // Subcommitted: resolve through the parent. Below TransactionXmin
    // pg_subtrans may be truncated (treat as crashed parent); a missing
    // entry above xmin is a startup-window artifact -> WARN, per C.
    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        if TransactionIdPrecedes(transactionId, TransactionXmin()) {
            return Ok(false);
        }
        let parentXid = subtrans::SubTransGetParent(transactionId)?;
        if !TransactionIdIsValid(parentXid) {
            elog(
                WARNING,
                format!("no pg_subtrans entry for subcommitted XID {transactionId}"),
            )?;
            return Ok(false);
        }
        return TransactionIdDidCommit(parentXid);
    }

    Ok(false)
}

// True only for explicit aborts: crash-implicit aborts read as in-progress.
pub fn TransactionIdDidAbort(transactionId: TransactionId) -> PgResult<bool> {
    let xidstatus = TransactionLogFetch(transactionId)?;

    if xidstatus == TRANSACTION_STATUS_ABORTED {
        return Ok(true);
    }

    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        if TransactionIdPrecedes(transactionId, TransactionXmin()) {
            return Ok(true);
        }
        let parentXid = subtrans::SubTransGetParent(transactionId)?;
        if !TransactionIdIsValid(parentXid) {
            elog(
                WARNING,
                format!("no pg_subtrans entry for subcommitted XID {transactionId}"),
            )?;
            return Ok(true);
        }
        return TransactionIdDidAbort(parentXid);
    }

    Ok(false)
}

pub fn TransactionIdCommitTree(xid: TransactionId, xids: &[TransactionId]) -> PgResult<()> {
    clog::TransactionIdSetTreeStatus(xid, xids, TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr)
}

pub fn TransactionIdAsyncCommitTree(
    xid: TransactionId,
    xids: &[TransactionId],
    lsn: XLogRecPtr,
) -> PgResult<()> {
    clog::TransactionIdSetTreeStatus(xid, xids, TRANSACTION_STATUS_COMMITTED, lsn)
}

pub fn TransactionIdAbortTree(xid: TransactionId, xids: &[TransactionId]) -> PgResult<()> {
    clog::TransactionIdSetTreeStatus(xid, xids, TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr)
}

pub fn TransactionIdLatest(mainxid: TransactionId, xids: &[TransactionId]) -> TransactionId {
    let mut result = mainxid;
    for &xid in xids.iter().rev() {
        if TransactionIdPrecedes(result, xid) {
            result = xid;
        }
    }
    result
}

// An LSN late enough that flushing to it flushes the commit record; not
// necessarily the exact commit LSN (see clog LSN groups).
pub fn TransactionIdGetCommitLSN(xid: TransactionId) -> PgResult<XLogRecPtr> {
    if TransactionIdEquals(xid, cachedFetchXid.get()) {
        return Ok(cachedCommitLSN.get());
    }

    if !TransactionIdIsNormal(xid) {
        return Ok(InvalidXLogRecPtr);
    }

    let (_status, result) = clog::TransactionIdGetStatus(xid)?;

    Ok(result)
}

pub fn init_seams() {
    transam_seams::transaction_id_did_commit::set(TransactionIdDidCommit);
    transam_seams::transaction_id_did_abort::set(TransactionIdDidAbort);
    transam_seams::transaction_id_commit_tree::set(TransactionIdCommitTree);
    transam_seams::transaction_id_async_commit_tree::set(TransactionIdAsyncCommitTree);
    transam_seams::transaction_id_abort_tree::set(TransactionIdAbortTree);
    transam_seams::transaction_id_latest::set(TransactionIdLatest);
    transam_seams::transaction_id_get_commit_lsn::set(TransactionIdGetCommitLSN);
}

#[cfg(test)]
mod tests;
