// pg_serial SLRU store (predicate.c:324-343, 731-841, 858-1130): one
// SerCommitSeqNo per xid, holding the minimum commitSeqNo of any transaction
// to which that (old, committed, summarized-away) xid had a rw-conflict out.
// The control word (headPage/headXid/tailXid) is guarded by SerialControlLock;
// page access uses the SLRU bank locks (C's discipline, kept 1:1).

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
    FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId, TransactionId,
    TransactionIdFollows, TransactionIdIsValid, TransactionIdPrecedes, BLCKSZ,
};
use types_error::PgResult;
use types_storage::storage::{LWTRANCHE_SERIAL_BUFFER, LWTRANCHE_SERIAL_SLRU};
use types_storage::sync::SyncRequestHandler;

use crate::engine::{my_procno, recovery_in_progress, SerialControlLock};
use crate::internals::SerCommitSeqNo;

pub const SERIAL_PAGESIZE: usize = BLCKSZ;
pub const SERIAL_ENTRYSIZE: usize = core::mem::size_of::<SerCommitSeqNo>();
pub const SERIAL_ENTRIESPERPAGE: u32 = (SERIAL_PAGESIZE / SERIAL_ENTRYSIZE) as u32;

// Set maximum pages based on the number needed to track all transactions.
pub const SERIAL_MAX_PAGE: i64 = MaxTransactionId as i64 / SERIAL_ENTRIESPERPAGE as i64;

#[inline]
pub(crate) fn SerialNextPage(page: i64) -> i64 {
    if page >= SERIAL_MAX_PAGE {
        0
    } else {
        page + 1
    }
}

#[inline]
pub(crate) fn SerialPage(xid: TransactionId) -> i64 {
    (xid / SERIAL_ENTRIESPERPAGE) as i64
}

// Byte range of xid's SerCommitSeqNo entry within its page (C's SerialValue).
#[inline]
fn entry_bytes(xid: TransactionId) -> core::ops::Range<usize> {
    let off = (xid % SERIAL_ENTRIESPERPAGE) as usize * SERIAL_ENTRYSIZE;
    off..off + SERIAL_ENTRYSIZE
}

pub struct SerialControlData {
    pub headPage: i64,          // newest initialized page
    pub headXid: TransactionId, // newest valid Xid in the SLRU
    pub tailXid: TransactionId, // oldest xmin we might be interested in
}

struct SerialControlPtr(*mut SerialControlData);
// SAFETY: all accesses go through SerialControlLock (C's discipline).
unsafe impl Send for SerialControlPtr {}
unsafe impl Sync for SerialControlPtr {}

static SERIAL_CONTROL: OnceLock<SerialControlPtr> = OnceLock::new();
static SERIAL_CTL: OnceLock<SlruCtlData> = OnceLock::new();

fn serial_control<'a>() -> &'a mut SerialControlData {
    let p = SERIAL_CONTROL
        .get()
        .unwrap_or_else(|| panic!("serialControl accessed before SerialInit"));
    unsafe { &mut *p.0 }
}

fn SerialSlruCtl() -> &'static SlruCtlData {
    SERIAL_CTL
        .get()
        .unwrap_or_else(|| panic!("pg_serial SLRU accessed before SerialInit"))
}

// Decide whether a Serial page number is "older" for truncation purposes.
// Analogous to CLOGPagePrecedes (predicate.c:731).
pub(crate) fn SerialPagePrecedesLogically(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(SERIAL_ENTRIESPERPAGE - 1))
}

// C's USE_ASSERT_CHECKING SerialPagePrecedesLogicallyUnitTests (predicate.c:747).
#[cfg(debug_assertions)]
pub(crate) fn SerialPagePrecedesLogicallyUnitTests() {
    let per_page = SERIAL_ENTRIESPERPAGE as i64;
    let offset = per_page / 2;

    // GetNewTransactionId() has assigned the last XID it can safely use.
    let newestPage = 2 * slru::SLRU_PAGES_PER_SEGMENT - 1; // nothing special
    let newestXact = (newestPage * per_page + offset) as TransactionId;
    assert_eq!(newestXact as i64 / per_page, newestPage);
    let oldestXact = newestXact.wrapping_add(1).wrapping_sub(1u32 << 31);
    let oldestPage = oldestXact as i64 / per_page;

    // The SLRU headPage pertains to the last ~1000 XIDs assigned; oldestXact
    // finished ~2B XIDs ago and is being summarized to tailPage. Must return
    // false so SerialAdd() doesn't zero tailPage and half the SLRU.
    let headPage = newestPage;
    let targetPage = oldestPage;
    assert!(!SerialPagePrecedesLogically(headPage, targetPage));

    // The reverse scenario: headPage pertains to oldestXact and an XID near
    // newestXact is being summarized. C mishandles targetPage itself (a
    // known, tolerated defect requiring ~2B single-user-mode XIDs); verify
    // the prior page as C does.
    let headPage = oldestPage;
    let targetPage = newestPage;
    assert!(SerialPagePrecedesLogically(headPage, targetPage - 1));
}

// Initialize for the tracking of old serializable committed xids
// (predicate.c:806 SerialInit).
pub fn SerialInit() -> PgResult<()> {
    if SERIAL_CTL.get().is_none() {
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
        if SERIAL_CTL.set(ctl).is_err() {
            panic!("SerialInit called twice");
        }
    }

    if SERIAL_CONTROL.get().is_none() {
        // Set control information to reflect empty SLRU.
        let c = Box::leak(Box::new(SerialControlData {
            headPage: -1,
            headXid: InvalidTransactionId,
            tailXid: InvalidTransactionId,
        }));
        let _ = SERIAL_CONTROL.set(SerialControlPtr(c));
    }
    Ok(())
}

/// Crash-cycle reset in place (notes/crash-restart-design.md): back to the
/// empty-SLRU state C's postmaster gets from fresh shmem.
pub fn SerialResetAfterCrash() {
    if SERIAL_CONTROL.get().is_none() {
        return;
    }
    let c = serial_control();
    c.headPage = -1;
    c.headXid = InvalidTransactionId;
    c.tailXid = InvalidTransactionId;
    if let Some(ctl) = SERIAL_CTL.get() {
        slru::SimpleLruResetAfterCrash(ctl);
    }
}

// Record a committed read write serializable xid and the minimum commitSeqNo
// of any transactions to which this xid had a rw-conflict out. An invalid
// commitSeqNo means that there were no conflicts out from xid
// (predicate.c:858 SerialAdd).
pub fn SerialAdd(xid: TransactionId, minConflictCommitSeqNo: SerCommitSeqNo) -> PgResult<()> {
    debug_assert!(TransactionIdIsValid(xid));

    let ctl = SerialSlruCtl();
    let targetPage = SerialPage(xid);

    // In this routine, we must hold both SerialControlLock and the SLRU bank
    // lock simultaneously while making the SLRU data catch up with the new
    // state that we determine.
    let control_lock = SerialControlLock();
    LWLockAcquire(control_lock, LW_EXCLUSIVE, my_procno())?;

    // If 'xid' is older than the global xmin (== tailXid), there's no need to
    // store it, after all. This can happen if the oldest transaction holding
    // back the global xmin just finished, making 'xid' uninteresting, but
    // ClearOldPredicateLocks() has not yet run.
    let tailXid = serial_control().tailXid;
    if !TransactionIdIsValid(tailXid) || TransactionIdPrecedes(xid, tailXid) {
        LWLockRelease(control_lock)?;
        return Ok(());
    }

    // If the SLRU is currently unused, zero out the whole active region from
    // tailXid to headXid before taking it into use. Otherwise zero out only
    // any new pages that enter the tailXid-headXid range as we advance
    // headXid.
    let (mut firstZeroPage, isNewPage) = {
        let c = serial_control();
        if c.headPage < 0 {
            (SerialPage(tailXid), true)
        } else {
            (
                SerialNextPage(c.headPage),
                SerialPagePrecedesLogically(c.headPage, targetPage),
            )
        }
    };

    {
        let c = serial_control();
        if !TransactionIdIsValid(c.headXid) || TransactionIdFollows(xid, c.headXid) {
            c.headXid = xid;
        }
        if isNewPage {
            c.headPage = targetPage;
        }
    }

    let (slotno, mut bank) = if isNewPage {
        // Initialize intervening pages; might involve trading locks.
        loop {
            let mut bank =
                LwGuard::acquire(SimpleLruGetBankLock(ctl, firstZeroPage), LW_EXCLUSIVE)?;
            let slotno = SimpleLruZeroPage(ctl, firstZeroPage, &mut bank)?;
            if firstZeroPage == targetPage {
                break (slotno, bank);
            }
            firstZeroPage = SerialNextPage(firstZeroPage);
            bank.release()?;
        }
    } else {
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, targetPage), LW_EXCLUSIVE)?;
        let slotno = SimpleLruReadPage(ctl, targetPage, true, xid, &mut bank)?;
        (slotno, bank)
    };

    ctl.page_buffer_mut(slotno, &mut bank)[entry_bytes(xid)]
        .copy_from_slice(&minConflictCommitSeqNo.to_ne_bytes());
    ctl.mark_page_dirty(slotno, &bank);

    bank.release()?;
    LWLockRelease(control_lock)?;
    Ok(())
}

// Get the minimum commitSeqNo for any conflict out for the given xid. For a
// transaction which exists but has no conflict out, InvalidSerCommitSeqNo
// will be returned (predicate.c:949).
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

    // SimpleLruReadPage_ReadOnly returns with the bank lock held, which must
    // then be released.
    let ctl = SerialSlruCtl();
    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, SerialPage(xid), xid)?;
    let val = SerCommitSeqNo::from_ne_bytes(
        ctl.page_buffer(slotno, &bank)[entry_bytes(xid)]
            .try_into()
            .expect("8-byte pg_serial entry"),
    );
    bank.release()?;
    Ok(val)
}

// Call this whenever there is a new xmin for active serializable
// transactions. We don't need to keep information on transactions which
// precede that. InvalidTransactionId means none active, so everything in the
// SLRU can be discarded (predicate.c:990).
pub fn SerialSetActiveSerXmin(xid: TransactionId) -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_procno())?;

    // When no sxacts are active, nothing overlaps, set the xid values to
    // invalid to show that there are no valid entries. Don't clear headPage,
    // though. A new xmin might still land on that page, and we don't want to
    // repeatedly zero out the same page.
    if !TransactionIdIsValid(xid) {
        let c = serial_control();
        c.tailXid = InvalidTransactionId;
        c.headXid = InvalidTransactionId;
        LWLockRelease(lock)?;
        return Ok(());
    }

    // When we're recovering prepared transactions, the global xmin might move
    // backwards depending on the order they're recovered. Normally that's not
    // OK, but during recovery no serializable transactions will commit, so
    // the SLRU is empty and we can get away with it.
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

// Perform a checkpoint --- either during shutdown, or on-the-fly. We don't
// have any data that needs to survive a restart, but this is a convenient
// place to truncate the SLRU (predicate.c:1041).
pub fn CheckPointPredicate() -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_procno())?;

    let truncateCutoffPage;
    {
        let c = serial_control();

        // Exit quickly if the SLRU is currently not in use.
        if c.headPage < 0 {
            LWLockRelease(lock)?;
            return Ok(());
        }

        if TransactionIdIsValid(c.tailXid) {
            let tailPage = SerialPage(c.tailXid);

            // It is possible for the tailXid to be ahead of the headXid: a
            // checkpoint while in-progress serializable transactions advance
            // the tail but nothing has been summarized yet. Cut off up to the
            // headPage; the next summary will advance the headXid.
            if SerialPagePrecedesLogically(tailPage, c.headPage) {
                // We can truncate the SLRU up to the page containing tailXid.
                truncateCutoffPage = tailPage;
            } else {
                truncateCutoffPage = c.headPage;
            }
        } else {
            // The SLRU is no longer needed. Truncate to head before we set
            // head invalid. (See predicate.c:1080's XXX notes on the
            // wraparound corner this shares with C.)
            truncateCutoffPage = c.headPage;
            c.headPage = -1;
        }
    }

    LWLockRelease(lock)?;

    // Truncate away pages that are no longer required. Note that no
    // additional locking is required, because this is only called as part of
    // a checkpoint, and the validity limits have already been determined.
    SimpleLruTruncate(SerialSlruCtl(), truncateCutoffPage)?;

    // Write dirty SLRU pages to disk. This is not actually necessary from a
    // correctness point of view (a debugging aid in C, kept for parity);
    // after the truncation to avoid writing pages right before deleting the
    // file in which they sit.
    SimpleLruWriteAll(SerialSlruCtl(), true)
}
