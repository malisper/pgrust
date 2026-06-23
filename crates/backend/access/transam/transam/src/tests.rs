//! Unit tests for the `transam.c` port.
//!
//! The cross-subsystem boundaries are function-pointer seams installed once
//! (process-wide) over thread-local fixture state, so tests on different
//! threads stay isolated; each test resets its thread's fixture and result
//! cache first, so test order does not matter.

use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Once;
use ::types_error::PgError;

thread_local! {
    /// xid -> (status, lsn), as pg_xact would report via TransactionIdGetStatus.
    static STATUSES: RefCell<BTreeMap<TransactionId, (XidStatus, XLogRecPtr)>> =
        RefCell::new(BTreeMap::new());
    /// subxid -> parent xid, as pg_subtrans would report.
    static PARENTS: RefCell<BTreeMap<TransactionId, TransactionId>> = RefCell::new(BTreeMap::new());
    /// count of TransactionIdGetStatus calls (to assert the cache works).
    static GET_STATUS_CALLS: Cell<usize> = const { Cell::new(0) };
    /// recorded TransactionIdSetTreeStatus calls.
    static SET_STATUS_CALLS: RefCell<Vec<SetStatusCall>> = RefCell::new(Vec::new());
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SetStatusCall {
    xid: TransactionId,
    subxids: Vec<TransactionId>,
    status: XidStatus,
    lsn: XLogRecPtr,
}

fn fixture_get_status(xid: TransactionId) -> PgResult<(XidStatus, XLogRecPtr)> {
    GET_STATUS_CALLS.with(|c| c.set(c.get() + 1));
    STATUSES.with(|m| {
        m.borrow()
            .get(&xid)
            .copied()
            .ok_or_else(|| PgError::error(format!("missing status for xid {xid}")))
    })
}

fn fixture_set_tree_status(
    xid: TransactionId,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    SET_STATUS_CALLS.with(|c| {
        c.borrow_mut().push(SetStatusCall {
            xid,
            subxids: subxids.to_vec(),
            status,
            lsn,
        })
    });
    Ok(())
}

fn fixture_sub_trans_get_parent(xid: TransactionId) -> PgResult<TransactionId> {
    Ok(PARENTS.with(|m| m.borrow().get(&xid).copied().unwrap_or(InvalidTransactionId)))
}

static INSTALL: Once = Once::new();

/// Install the seams (once) and reset this thread's fixture and the
/// thread-local result cache for a fresh test.
fn setup() {
    INSTALL.call_once(|| {
        clog_seams::transaction_id_get_status::set(fixture_get_status);
        clog_seams::transaction_id_set_tree_status::set(fixture_set_tree_status);
        subtrans_seams::sub_trans_get_parent::set(fixture_sub_trans_get_parent);
    });
    STATUSES.with(|m| m.borrow_mut().clear());
    PARENTS.with(|m| m.borrow_mut().clear());
    GET_STATUS_CALLS.with(|c| c.set(0));
    SET_STATUS_CALLS.with(|c| c.borrow_mut().clear());
    cachedFetchXid.set(InvalidTransactionId);
    cachedFetchXidStatus.set(0);
    cachedCommitLSN.set(0);
}

fn set_status(xid: TransactionId, status: XidStatus, lsn: XLogRecPtr) {
    STATUSES.with(|m| {
        m.borrow_mut().insert(xid, (status, lsn));
    });
}

fn set_parent(xid: TransactionId, parent: TransactionId) {
    PARENTS.with(|m| {
        m.borrow_mut().insert(xid, parent);
    });
}

#[test]
fn special_xids_have_fixed_status_without_clog_lookup() {
    setup();

    assert_eq!(
        TransactionLogFetch(BootstrapTransactionId).unwrap(),
        TRANSACTION_STATUS_COMMITTED
    );
    assert_eq!(
        TransactionLogFetch(FrozenTransactionId).unwrap(),
        TRANSACTION_STATUS_COMMITTED
    );
    // A non-normal xid that is neither Bootstrap nor Frozen is aborted. First
    // move the cache off InvalidTransactionId (its reset value), so querying
    // InvalidTransactionId falls through the cache to the special-xid path.
    set_status(7, TRANSACTION_STATUS_COMMITTED, 0);
    TransactionLogFetch(7).unwrap();
    assert_eq!(cachedFetchXid.get(), 7);
    assert_eq!(
        TransactionLogFetch(InvalidTransactionId).unwrap(),
        TRANSACTION_STATUS_ABORTED
    );
    // Only the one normal-xid fetch (xid 7) reached the clog seam.
    assert_eq!(GET_STATUS_CALLS.with(|c| c.get()), 1);
}

#[test]
fn stable_statuses_are_cached_with_commit_lsn() {
    setup();
    set_status(10, TRANSACTION_STATUS_COMMITTED, 99);

    assert_eq!(TransactionLogFetch(10).unwrap(), TRANSACTION_STATUS_COMMITTED);
    assert_eq!(TransactionLogFetch(10).unwrap(), TRANSACTION_STATUS_COMMITTED);
    // second fetch served from the single-item cache.
    assert_eq!(GET_STATUS_CALLS.with(|c| c.get()), 1);
    // GetCommitLSN also hits the cache (no extra clog trip).
    assert_eq!(TransactionIdGetCommitLSN(10).unwrap(), 99);
    assert_eq!(GET_STATUS_CALLS.with(|c| c.get()), 1);
}

#[test]
fn in_progress_and_subcommitted_statuses_are_not_cached() {
    setup();
    set_status(10, TRANSACTION_STATUS_IN_PROGRESS, 11);
    set_status(11, TRANSACTION_STATUS_SUB_COMMITTED, 12);

    TransactionLogFetch(10).unwrap();
    TransactionLogFetch(11).unwrap();

    assert_eq!(cachedFetchXid.get(), InvalidTransactionId);
}

#[test]
fn subcommitted_commit_status_follows_parent() {
    setup();
    set_status(10, TRANSACTION_STATUS_SUB_COMMITTED, 0);
    set_status(9, TRANSACTION_STATUS_COMMITTED, 88);
    set_parent(10, 9);

    assert!(TransactionIdDidCommit(10, 3).unwrap());
    assert!(!TransactionIdDidAbort(10, 3).unwrap());
}

#[test]
fn old_subcommitted_xids_assume_crashed_parent() {
    setup();
    set_status(10, TRANSACTION_STATUS_SUB_COMMITTED, 0);

    // older than TransactionXmin: did-commit is false, did-abort is true.
    assert!(!TransactionIdDidCommit(10, 20).unwrap());
    assert!(TransactionIdDidAbort(10, 20).unwrap());
}

#[test]
fn subcommitted_missing_parent_warns_and_defaults() {
    setup();
    // subcommitted, not older than xmin, but no pg_subtrans parent recorded.
    set_status(10, TRANSACTION_STATUS_SUB_COMMITTED, 0);

    // WARNING is emitted; did-commit defaults to false, did-abort to true.
    assert!(!TransactionIdDidCommit(10, 3).unwrap());
    assert!(TransactionIdDidAbort(10, 3).unwrap());
}

#[test]
fn committed_and_aborted_leaf_statuses() {
    setup();
    set_status(5, TRANSACTION_STATUS_COMMITTED, 0);
    set_status(6, TRANSACTION_STATUS_ABORTED, 0);
    set_status(7, TRANSACTION_STATUS_IN_PROGRESS, 0);

    assert!(TransactionIdDidCommit(5, InvalidTransactionId).unwrap());
    assert!(!TransactionIdDidAbort(5, InvalidTransactionId).unwrap());

    assert!(!TransactionIdDidCommit(6, InvalidTransactionId).unwrap());
    assert!(TransactionIdDidAbort(6, InvalidTransactionId).unwrap());

    // in-progress: neither committed nor (explicitly) aborted.
    assert!(!TransactionIdDidCommit(7, InvalidTransactionId).unwrap());
    assert!(!TransactionIdDidAbort(7, InvalidTransactionId).unwrap());
}

#[test]
fn tree_status_wrappers_delegate_to_clog() {
    setup();

    TransactionIdCommitTree(20, &[21, 22]).unwrap();
    TransactionIdAsyncCommitTree(30, &[31], 44).unwrap();
    TransactionIdAbortTree(40, &[]).unwrap();

    assert_eq!(
        SET_STATUS_CALLS.with(|c| c.borrow().clone()),
        vec![
            SetStatusCall {
                xid: 20,
                subxids: vec![21, 22],
                status: TRANSACTION_STATUS_COMMITTED,
                lsn: InvalidXLogRecPtr,
            },
            SetStatusCall {
                xid: 30,
                subxids: vec![31],
                status: TRANSACTION_STATUS_COMMITTED,
                lsn: 44,
            },
            SetStatusCall {
                xid: 40,
                subxids: vec![],
                status: TRANSACTION_STATUS_ABORTED,
                lsn: InvalidXLogRecPtr,
            },
        ]
    );
}

#[test]
fn xid_ordering_uses_postgres_wraparound_rules() {
    assert!(TransactionIdPrecedes(3, 4));
    assert!(TransactionIdFollows(4, 3));
    assert!(!TransactionIdPrecedes(4, 4));
    assert!(TransactionIdPrecedesOrEquals(4, 4));
    assert!(TransactionIdFollowsOrEquals(4, 4));

    // wraparound: u32::MAX-10 logically precedes 20 (modulo 2^32).
    assert!(TransactionIdPrecedes(u32::MAX - 10, 20));
    assert!(TransactionIdFollows(20, u32::MAX - 10));

    // a non-normal id forces plain unsigned comparison.
    assert!(TransactionIdPrecedes(
        InvalidTransactionId,
        FirstNormalTransactionId
    ));
    assert!(!TransactionIdPrecedes(
        FirstNormalTransactionId,
        InvalidTransactionId
    ));
}

#[test]
fn latest_scans_subxids_from_the_back_but_checks_all() {
    assert_eq!(TransactionIdLatest(10, &[9, 11, 8]), 11);
    assert_eq!(TransactionIdLatest(10, &[8, 9]), 10);
    // wraparound: with main = MAX-1, the modular-later 3 and 4 win, 4 latest.
    assert_eq!(TransactionIdLatest(u32::MAX - 1, &[3, 4]), 4);
}

#[test]
fn get_commit_lsn_special_and_uncached_xids() {
    setup();
    // special xid: always InvalidXLogRecPtr, no clog trip.
    assert_eq!(
        TransactionIdGetCommitLSN(BootstrapTransactionId).unwrap(),
        InvalidXLogRecPtr
    );
    assert_eq!(GET_STATUS_CALLS.with(|c| c.get()), 0);

    // uncached normal xid: fetch from clog and return its lsn.
    set_status(100, TRANSACTION_STATUS_COMMITTED, 1234);
    assert_eq!(TransactionIdGetCommitLSN(100).unwrap(), 1234);
    assert_eq!(GET_STATUS_CALLS.with(|c| c.get()), 1);
}

#[test]
fn validity_and_normal_predicates() {
    assert!(!TransactionIdIsValid(InvalidTransactionId));
    assert!(TransactionIdIsValid(BootstrapTransactionId));
    assert!(TransactionIdIsValid(FirstNormalTransactionId));

    assert!(!TransactionIdIsNormal(InvalidTransactionId));
    assert!(!TransactionIdIsNormal(BootstrapTransactionId));
    assert!(!TransactionIdIsNormal(FrozenTransactionId));
    assert!(TransactionIdIsNormal(FirstNormalTransactionId));

    assert!(TransactionIdEquals(7, 7));
    assert!(!TransactionIdEquals(7, 8));
}
