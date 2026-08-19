// pg_serial: SLRU summarization of old committed serializable transactions
// (predicate.c). The control word (headPage/headXid/tailXid) is guarded by
// SerialControlLock; SerialAdd holds it together with the SLRU bank lock
// while the page store catches up with the new state.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::OnceLock;

use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use slru::{
    LwGuard, SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruTruncate, SimpleLruWriteAll, SimpleLruZeroPage, SlruCtlData,
    SlruPagePrecedesUnitTests,
};
use types_core::{
    FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId, Size, TransactionId,
    TransactionIdFollows, TransactionIdIsValid, TransactionIdPrecedes, BLCKSZ,
};
use types_error::PgResult;
use types_storage::storage::{LWTRANCHE_SERIAL_BUFFER, LWTRANCHE_SERIAL_SLRU};
use types_storage::sync::SyncRequestHandler;

use crate::engine::{my_procno, recovery_in_progress, SerialControlLock};
use crate::internals::SerCommitSeqNo;

const SERIAL_ENTRYSIZE: usize = core::mem::size_of::<SerCommitSeqNo>();
const SERIAL_ENTRIESPERPAGE: u32 = (BLCKSZ / SERIAL_ENTRYSIZE) as u32;

// Maximum page number, based on the number needed to track all transactions.
const SERIAL_MAX_PAGE: i64 = (MaxTransactionId / SERIAL_ENTRIESPERPAGE) as i64;

#[inline]
fn SerialNextPage(page: i64) -> i64 {
    if page >= SERIAL_MAX_PAGE {
        0
    } else {
        page + 1
    }
}

#[inline]
fn SerialPage(xid: TransactionId) -> i64 {
    (xid / SERIAL_ENTRIESPERPAGE) as i64
}

#[inline]
fn serial_entry_offset(xid: TransactionId) -> usize {
    (xid % SERIAL_ENTRIESPERPAGE) as usize * SERIAL_ENTRYSIZE
}

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
static SERIAL_SLRU: OnceLock<SlruCtlData> = OnceLock::new();

fn serial_control<'a>() -> &'a mut SerialControlData {
    let p = SERIAL_CONTROL
        .get()
        .unwrap_or_else(|| panic!("serialControl accessed before SerialInit"));
    unsafe { &mut *p.0 }
}

fn SerialSlruCtl() -> &'static SlruCtlData {
    SERIAL_SLRU
        .get()
        .unwrap_or_else(|| panic!("pg_serial SLRU accessed before SerialInit"))
}

// SerialPagePrecedesLogically (predicate.c): analogous to CLOGPagePrecedes;
// C narrows the int64 page number into a 32-bit TransactionId.
fn SerialPagePrecedesLogically(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(SERIAL_ENTRIESPERPAGE - 1))
}

#[cfg(debug_assertions)]
fn SerialPagePrecedesLogicallyUnitTests() {
    use slru::SLRU_PAGES_PER_SEGMENT;

    let per_page = SERIAL_ENTRIESPERPAGE;
    let offset = per_page / 2;

    // GetNewTransactionId() has assigned the last XID it can safely use.
    let newest_page: i64 = 2 * SLRU_PAGES_PER_SEGMENT - 1; // nothing special
    let newest_xact: TransactionId = (newest_page as u32).wrapping_mul(per_page) + offset;
    assert_eq!((newest_xact / per_page) as i64, newest_page);
    let oldest_xact = newest_xact.wrapping_add(1).wrapping_sub(1u32 << 31);
    let oldest_page = (oldest_xact / per_page) as i64;

    // The headPage pertains to the last ~1000 XIDs assigned; oldestXact
    // finishes ~2B XIDs later and gets summarized to tailPage. Must return
    // false so SerialAdd() doesn't zero tailPage and half the SLRU.
    assert!(!SerialPagePrecedesLogically(newest_page, oldest_page));

    // The headPage pertains to oldestXact; we summarize an XID near
    // newestXact. Today's implementation mishandles targetPage itself (C's
    // #if 0 assert); verify the prior page like C does.
    assert!(SerialPagePrecedesLogically(oldest_page, newest_page - 1));
}

pub fn SerialInit() -> PgResult<()> {
    if SERIAL_CONTROL.get().is_some() {
        return Ok(());
    }

    // Set up SLRU management of the pg_serial data.
    let mut ctl = SimpleLruInit(
        "serializable",
        init_small::globals::serializable_buffers(),
        0,
        "pg_serial",
        LWTRANCHE_SERIAL_BUFFER,
        LWTRANCHE_SERIAL_SLRU,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        false,
    )?;
    ctl.PagePrecedes = Some(SerialPagePrecedesLogically);
    #[cfg(debug_assertions)]
    SerialPagePrecedesLogicallyUnitTests();
    SlruPagePrecedesUnitTests(&ctl, SERIAL_ENTRIESPERPAGE as i32);
    if SERIAL_SLRU.set(ctl).is_err() {
        panic!("SerialInit called twice");
    }

    // Set control information to reflect empty SLRU.
    let c = Box::leak(Box::new(SerialControlData {
        headPage: -1,
        headXid: InvalidTransactionId,
        tailXid: InvalidTransactionId,
    }));
    let _ = SERIAL_CONTROL.set(SerialControlPtr(c));
    Ok(())
}

pub fn SerialShmemSize() -> Size {
    core::mem::size_of::<SerialControlData>()
        + slru::SimpleLruShmemSize(init_small::globals::serializable_buffers(), 0)
}

pub fn SerialResetAfterCrash() {
    if SERIAL_CONTROL.get().is_none() {
        return;
    }
    if let Some(ctl) = SERIAL_SLRU.get() {
        slru::SimpleLruResetAfterCrash(ctl);
    }
    let c = serial_control();
    c.headPage = -1;
    c.headXid = InvalidTransactionId;
    c.tailXid = InvalidTransactionId;
}

// Record a committed read write serializable xid and the minimum
// commitSeqNo of any transactions to which this xid had a rw-conflict out.
// An invalid commitSeqNo means that there were no conflicts out from xid.
pub fn SerialAdd(xid: TransactionId, min_conflict_commit_seqno: SerCommitSeqNo) -> PgResult<()> {
    debug_assert!(TransactionIdIsValid(xid));

    let ctl = SerialSlruCtl();
    let target_page = SerialPage(xid);

    // Hold both SerialControlLock and the SLRU bank lock simultaneously
    // while making the SLRU data catch up with the new state.
    LWLockAcquire(SerialControlLock(), LW_EXCLUSIVE, my_procno())?;

    // If 'xid' is older than the global xmin (== tailXid), there's no need
    // to store it: the oldest xmin-holding transaction just finished, making
    // 'xid' uninteresting, but ClearOldPredicateLocks() has not yet run.
    let tail_xid = serial_control().tailXid;
    if !TransactionIdIsValid(tail_xid) || TransactionIdPrecedes(xid, tail_xid) {
        LWLockRelease(SerialControlLock())?;
        return Ok(());
    }

    // If the SLRU is currently unused, zero out the whole active region from
    // tailXid to headXid before taking it into use. Otherwise zero out only
    // any new pages that enter the tailXid-headXid range as headXid advances.
    let (mut first_zero_page, is_new_page) = {
        let c = serial_control();
        if c.headPage < 0 {
            (SerialPage(tail_xid), true)
        } else {
            (
                SerialNextPage(c.headPage),
                SerialPagePrecedesLogically(c.headPage, target_page),
            )
        }
    };

    {
        let c = serial_control();
        if !TransactionIdIsValid(c.headXid) || TransactionIdFollows(xid, c.headXid) {
            c.headXid = xid;
        }
        if is_new_page {
            c.headPage = target_page;
        }
    }

    let (slotno, mut bank) = if is_new_page {
        // Initialize intervening pages; might involve trading locks.
        loop {
            let mut g = LwGuard::acquire(SimpleLruGetBankLock(ctl, first_zero_page), LW_EXCLUSIVE)?;
            let s = SimpleLruZeroPage(ctl, first_zero_page, &mut g)?;
            if first_zero_page == target_page {
                break (s, g);
            }
            first_zero_page = SerialNextPage(first_zero_page);
            g.release()?;
        }
    } else {
        let mut g = LwGuard::acquire(SimpleLruGetBankLock(ctl, target_page), LW_EXCLUSIVE)?;
        let s = SimpleLruReadPage(ctl, target_page, true, xid, &mut g)?;
        (s, g)
    };

    let entry = serial_entry_offset(xid);
    ctl.page_buffer_mut(slotno, &mut bank)[entry..entry + SERIAL_ENTRYSIZE]
        .copy_from_slice(&min_conflict_commit_seqno.to_ne_bytes());
    ctl.mark_page_dirty(slotno, &bank);

    bank.release()?;
    LWLockRelease(SerialControlLock())?;
    Ok(())
}

// Get the minimum commitSeqNo for any conflict out for the given xid. For a
// transaction which exists but has no conflict out, InvalidSerCommitSeqNo is
// returned.
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

    let ctl = SerialSlruCtl();
    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, SerialPage(xid), xid)?;
    let entry = serial_entry_offset(xid);
    let val = SerCommitSeqNo::from_ne_bytes(
        ctl.page_buffer(slotno, &bank)[entry..entry + SERIAL_ENTRYSIZE]
            .try_into()
            .expect("8-byte SerCommitSeqNo"),
    );
    bank.release()?;
    Ok(val)
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

#[cfg(test)]
pub(crate) fn test_serial_state() -> (i64, TransactionId, TransactionId) {
    let c = serial_control();
    (c.headPage, c.headXid, c.tailXid)
}

// Perform a checkpoint --- either during shutdown, or on-the-fly. No data
// needs to survive a restart, but this is a convenient place to truncate
// the SLRU.
pub fn CheckPointPredicate() -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_procno())?;

    // Exit quickly if the SLRU is currently not in use.
    if serial_control().headPage < 0 {
        LWLockRelease(lock)?;
        return Ok(());
    }

    let truncate_cutoff_page = {
        let c = serial_control();
        if TransactionIdIsValid(c.tailXid) {
            // tailXid can be ahead of headXid if we checkpoint while
            // in-progress serializable transactions advance the tail but the
            // summaries haven't caught up; cut off up to headPage then and
            // let the next summary advance headXid.
            let tail_page = SerialPage(c.tailXid);
            if SerialPagePrecedesLogically(tail_page, c.headPage) {
                tail_page
            } else {
                c.headPage
            }
        } else {
            // The SLRU is no longer needed. Truncate to head before setting
            // head invalid. (C notes the XID-wraparound corner where the
            // leftover headPage segment looks new again; harmless.)
            let p = c.headPage;
            c.headPage = -1;
            p
        }
    };

    LWLockRelease(lock)?;

    // Truncate away pages that are no longer required. No additional locking
    // needed: this only runs as part of a checkpoint, and the validity
    // limits have already been determined.
    SimpleLruTruncate(SerialSlruCtl(), truncate_cutoff_page)?;

    // Write dirty SLRU pages to disk. Not needed for correctness (debugging
    // aid); done after the truncation to avoid writing pages right before
    // deleting the file in which they sit.
    SimpleLruWriteAll(SerialSlruCtl(), true)
}
