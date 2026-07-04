// pg_serial control word (headPage/headXid/tailXid), guarded by
// SerialControlLock. The SLRU page store behind it is reachable only through
// SerialAdd, which only SummarizeOldestCommittedSxact calls — i.e. after
// exhausting (MaxBackends + max_prepared_xacts) * 10 SERIALIZABLEXACT slots —
// so the page store is a loud panic, not a port (headXid stays invalid and
// SerialGetMinConflictCommitSeqNo always takes C's early-out).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::OnceLock;

use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{
    InvalidTransactionId, TransactionId, TransactionIdFollows, TransactionIdIsValid,
    TransactionIdPrecedes,
};
use types_error::PgResult;

use crate::engine::{my_procno, recovery_in_progress, SerialControlLock};
use crate::internals::SerCommitSeqNo;

pub struct SerialControlData {
    pub headPage: i64,
    pub headXid: TransactionId,
    pub tailXid: TransactionId,
}

struct SerialControlPtr(*mut SerialControlData);
// SAFETY: all accesses go through SerialControlLock (C's discipline).
unsafe impl Send for SerialControlPtr {}
unsafe impl Sync for SerialControlPtr {}

static SERIAL_CONTROL: OnceLock<SerialControlPtr> = OnceLock::new();

fn serial_control<'a>() -> &'a mut SerialControlData {
    let p = SERIAL_CONTROL
        .get()
        .unwrap_or_else(|| panic!("serialControl accessed before SerialInit"));
    unsafe { &mut *p.0 }
}

pub fn SerialInit() -> PgResult<()> {
    if SERIAL_CONTROL.get().is_some() {
        return Ok(());
    }
    let c = Box::leak(Box::new(SerialControlData {
        headPage: -1,
        headXid: InvalidTransactionId,
        tailXid: InvalidTransactionId,
    }));
    let _ = SERIAL_CONTROL.set(SerialControlPtr(c));
    Ok(())
}

pub fn SerialResetAfterCrash() {
    if SERIAL_CONTROL.get().is_none() {
        return;
    }
    let c = serial_control();
    c.headPage = -1;
    c.headXid = InvalidTransactionId;
    c.tailXid = InvalidTransactionId;
}

pub fn SerialAdd(_xid: TransactionId, _min_conflict_commit_seqno: SerCommitSeqNo) -> PgResult<()> {
    panic!(
        "predicate.c SerialAdd: pg_serial SLRU page store not ported \
         (reached via SummarizeOldestCommittedSxact after SERIALIZABLEXACT exhaustion)"
    );
}

pub fn SerialGetMinConflictCommitSeqNo(xid: TransactionId) -> PgResult<SerCommitSeqNo> {
    debug_assert!(TransactionIdIsValid(xid));
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_SHARED, my_procno())?;
    let (headXid, tailXid) = {
        let c = serial_control();
        (c.headXid, c.tailXid)
    };
    LWLockRelease(lock)?;

    if !TransactionIdIsValid(headXid) {
        return Ok(0);
    }
    debug_assert!(TransactionIdIsValid(tailXid));
    if TransactionIdPrecedes(xid, tailXid) || TransactionIdFollows(xid, headXid) {
        return Ok(0);
    }
    // headXid can only become valid through SerialAdd, which panics above.
    panic!("predicate.c SerialGetMinConflictCommitSeqNo: pg_serial SLRU read not ported");
}

pub fn SerialSetActiveSerXmin(xid: TransactionId) -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_procno())?;

    if !TransactionIdIsValid(xid) {
        let c = serial_control();
        c.tailXid = InvalidTransactionId;
        c.headXid = InvalidTransactionId;
        LWLockRelease(lock)?;
        return Ok(());
    }

    if recovery_in_progress() {
        let c = serial_control();
        debug_assert!(c.headPage < 0);
        if !TransactionIdIsValid(c.tailXid) || TransactionIdPrecedes(xid, c.tailXid) {
            c.tailXid = xid;
        }
        LWLockRelease(lock)?;
        return Ok(());
    }

    {
        let c = serial_control();
        debug_assert!(!TransactionIdIsValid(c.tailXid) || TransactionIdFollows(xid, c.tailXid));
        c.tailXid = xid;
    }
    LWLockRelease(lock)?;
    Ok(())
}

pub fn CheckPointPredicate() -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_procno())?;
    let headPage = serial_control().headPage;
    LWLockRelease(lock)?;
    if headPage >= 0 {
        panic!("predicate.c CheckPointPredicate: pg_serial SLRU truncate not ported");
    }
    Ok(())
}
