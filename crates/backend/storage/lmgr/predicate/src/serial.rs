//! `pg_serial` SLRU tracking of the conflict-out commit seqno of old committed
//! serializable xids, plus the file-global SSI shared-state pointers. Mirrors
//! the `static` file globals of predicate.c.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::{Cell, RefCell};

use ::slru::{
    SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruTruncate, SimpleLruWriteAll, SimpleLruZeroPage, SlruCtlData, SlruPagePrecedesUnitTests,
};
use ::transam::{
    TransactionIdFollows, TransactionIdIsValid, TransactionIdPrecedes,
};
use ::ipc_shmem::ShmemInitStruct;
use ::lwlock::{LWLockAcquire, LWLockRelease};
use ::types_storage::{LWLock, LW_EXCLUSIVE, LW_SHARED};
use ::types_core::primitive::BLCKSZ;
use ::types_core::xact::{FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId};
use ::types_core::TransactionId;
use ::types_error::PgResult;
use ::types_storage::sync::SyncRequestHandler;
use ::types_storage::{LWTRANCHE_SERIAL_BUFFER, LWTRANCHE_SERIAL_SLRU};

use crate::globals::{my_proc_number, recovery_in_progress, serializable_buffers, SerialControlLock};
use crate::internals::SerCommitSeqNo;

// ---------------------------------------------------------------------------
// SLRU page geometry (predicate.c).
// ---------------------------------------------------------------------------

/// `SERIAL_PAGESIZE` == `BLCKSZ`.
pub const SERIAL_PAGESIZE: i64 = BLCKSZ as i64;
/// `SERIAL_ENTRYSIZE` == `sizeof(SerCommitSeqNo)`.
pub const SERIAL_ENTRYSIZE: i64 = core::mem::size_of::<SerCommitSeqNo>() as i64;
/// `SERIAL_ENTRIESPERPAGE`.
pub const SERIAL_ENTRIESPERPAGE: i64 = SERIAL_PAGESIZE / SERIAL_ENTRYSIZE;
/// `SERIAL_MAX_PAGE` == `MaxTransactionId / SERIAL_ENTRIESPERPAGE`.
pub const SERIAL_MAX_PAGE: i64 = (MaxTransactionId as i64) / SERIAL_ENTRIESPERPAGE;

/// `SerialNextPage(page)`.
#[inline]
pub fn SerialNextPage(page: i64) -> i64 {
    if page >= SERIAL_MAX_PAGE {
        0
    } else {
        page + 1
    }
}

/// `SerialPage(xid)`.
#[inline]
pub fn SerialPage(xid: TransactionId) -> i64 {
    ((xid as u32) as i64) / SERIAL_ENTRIESPERPAGE
}

/// `SerialControlData` (predicate.c file struct).
#[derive(Clone, Copy)]
pub struct SerialControlData {
    pub headPage: i64,
    pub headXid: TransactionId,
    pub tailXid: TransactionId,
}

// ---------------------------------------------------------------------------
// File globals (predicate.c statics), modelled as thread-locals over the real
// ported shmem/SLRU substrate (cf. the project's shmem-consumer convention).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static SlruCtlData SerialSlruCtlData;` — the owned SLRU control block
    /// (the repo's SimpleLruInit returns an owned value, not a shmem pointer).
    pub static SERIAL_SLRU: RefCell<Option<SlruCtlData>> = const { RefCell::new(None) };
    /// `static SerialControl serialControl;` (predicate.c:354) — a pointer to the
    /// `SerialControlData` struct in *shared* memory. C's `serialControl` is a
    /// `SerialControlData *` it stores from `ShmemInitStruct`; mirror that here.
    /// The pointer value itself is process-local (each forked backend caches its
    /// own copy of the shmem address, which is identical across the fork-COW
    /// mapping), but it dereferences to the ONE shared struct — so `headPage`/
    /// `headXid`/`tailXid` (and therefore the cross-backend summarization that
    /// reclaims SERIALIZABLEXACT slots) are shared, exactly as in C. A per-backend
    /// by-value copy (the previous bug) stalled summarization and exhausted the
    /// free-list ("out of shared memory") under heavy predicate-lock load.
    /// All accesses are serialized by `SerialControlLock`.
    pub static SERIAL_CONTROL: Cell<*mut SerialControlData> =
        const { Cell::new(core::ptr::null_mut()) };
    /// Whether SERIAL_CONTROL was already found in shmem (== IsUnderPostmaster).
    pub static SERIAL_CONTROL_FOUND: Cell<bool> = const { Cell::new(false) };
}

/// Dereference the shared `SerialControlData` pointer (`*serialControl` in C).
/// The caller must hold `SerialControlLock` for the access mode. Panics if the
/// pointer was never installed (a `SerialInit` ordering bug, not a runtime
/// condition).
#[inline]
fn serial_control<'a>() -> &'a mut SerialControlData {
    let p = SERIAL_CONTROL.with(|c| c.get());
    debug_assert!(!p.is_null(), "serialControl accessed before SerialInit");
    unsafe { &mut *p }
}

/// `SerialValue(slotno, xid) = value` — write the 8-byte SerCommitSeqNo at the
/// xid's slot in the SLRU page buffer.
fn serial_value_set(ctl: &mut SlruCtlData, slotno: usize, xid: TransactionId, value: SerCommitSeqNo) {
    let off = (((xid as u32) as i64 % SERIAL_ENTRIESPERPAGE) * SERIAL_ENTRYSIZE) as usize;
    let buf = ctl.shared.page_buffer_mut(slotno);
    buf[off..off + 8].copy_from_slice(&value.to_ne_bytes());
}

/// `SerialValue(slotno, xid)` — read the 8-byte SerCommitSeqNo.
fn serial_value_get(ctl: &mut SlruCtlData, slotno: usize, xid: TransactionId) -> SerCommitSeqNo {
    let off = (((xid as u32) as i64 % SERIAL_ENTRIESPERPAGE) * SERIAL_ENTRYSIZE) as usize;
    let buf = ctl.shared.page_buffer_mut(slotno);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[off..off + 8]);
    SerCommitSeqNo::from_ne_bytes(bytes)
}

/// `SerialPagePrecedesLogically(page1, page2)` (predicate.c) — decide whether
/// SLRU page1 precedes page2 in the circular xid space.
pub fn SerialPagePrecedesLogically(page1: i64, page2: i64) -> bool {
    let mut xid1: TransactionId = (page1 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE as TransactionId);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2: TransactionId = (page2 as TransactionId).wrapping_mul(SERIAL_ENTRIESPERPAGE as TransactionId);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(SERIAL_ENTRIESPERPAGE as TransactionId - 1))
}

/// `SLRU_PAGES_PER_SEGMENT` (access/slru.h).
const SLRU_PAGES_PER_SEGMENT: i64 = 32;

/// `SerialPagePrecedesLogicallyUnitTests` (USE_ASSERT_CHECKING) — verifies the
/// wraparound corner cases. Panics on regression (like a failed Assert).
pub fn SerialPagePrecedesLogicallyUnitTests() {
    let per_page: i64 = SERIAL_ENTRIESPERPAGE;
    let offset: i64 = per_page / 2;

    let newest_page: i64 = 2 * SLRU_PAGES_PER_SEGMENT - 1;
    let newest_xact: TransactionId = (newest_page * per_page + offset) as TransactionId;
    assert_eq!(((newest_xact as u32) as i64) / per_page, newest_page);
    let oldest_xact: TransactionId = newest_xact.wrapping_add(1).wrapping_sub(1u32 << 31);
    let oldest_page: i64 = ((oldest_xact as u32) as i64) / per_page;

    let head_page = newest_page;
    let target_page = oldest_page;
    assert!(!SerialPagePrecedesLogically(head_page, target_page));

    let head_page = oldest_page;
    let target_page = newest_page;
    assert!(SerialPagePrecedesLogically(head_page, target_page - 1));
}

/// `SerialInit()` (predicate.c) — set up SLRU management of pg_serial.
pub fn SerialInit() -> PgResult<()> {
    let mut ctl = SimpleLruInit(
        "serializable",
        serializable_buffers(),
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

    SERIAL_SLRU.with(|s| *s.borrow_mut() = Some(ctl));

    // Create or attach to the SerialControl structure. C:
    //   serialControl = (SerialControl) ShmemInitStruct("SerialControlData",
    //                                       sizeof(SerialControlData), &found);
    // Store the SHARED pointer so every backend reads/writes the one struct.
    let (ptr, found) = ShmemInitStruct(
        "SerialControlData",
        core::mem::size_of::<SerialControlData>(),
    )?;
    SERIAL_CONTROL.with(|c| c.set(ptr.as_ptr() as *mut SerialControlData));
    SERIAL_CONTROL_FOUND.with(|f| f.set(found));

    if !found {
        // Set control information to reflect empty SLRU.
        let lock = SerialControlLock();
        LWLockAcquire(lock, LW_EXCLUSIVE, my_proc_number())?;
        let c = serial_control();
        c.headPage = -1;
        c.headXid = InvalidTransactionId;
        c.tailXid = InvalidTransactionId;
        LWLockRelease(lock)?;
    }
    Ok(())
}

/// `SerialAdd(xid, minConflictCommitSeqNo)` (predicate.c).
pub fn SerialAdd(xid: TransactionId, minConflictCommitSeqNo: SerCommitSeqNo) -> PgResult<()> {
    debug_assert!(TransactionIdIsValid(xid));

    let targetPage = SerialPage(xid);
    let procno = my_proc_number();

    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, procno)?;

    let tailXid = serial_control().tailXid;
    if !TransactionIdIsValid(tailXid) || TransactionIdPrecedes(xid, tailXid) {
        LWLockRelease(lock)?;
        return Ok(());
    }

    let headPage = serial_control().headPage;
    let mut firstZeroPage;
    let isNewPage;
    if headPage < 0 {
        firstZeroPage = SerialPage(tailXid);
        isNewPage = true;
    } else {
        firstZeroPage = SerialNextPage(headPage);
        isNewPage = SerialPagePrecedesLogically(headPage, targetPage);
    }

    {
        let c = serial_control();
        if !TransactionIdIsValid(c.headXid) || TransactionIdFollows(xid, c.headXid) {
            c.headXid = xid;
        }
        if isNewPage {
            c.headPage = targetPage;
        }
    }

    SERIAL_SLRU.with(|s| -> PgResult<()> {
        let mut s = s.borrow_mut();
        let ctl = s.as_mut().expect("SerialSlruCtl not initialized");

        let slotno;
        if isNewPage {
            // Initialize intervening pages; might involve trading bank locks.
            loop {
                let bank_lock = SimpleLruGetBankLock(ctl, firstZeroPage) as *const LWLock;
                LWLockAcquire(unsafe { &*bank_lock }, LW_EXCLUSIVE, procno)?;
                let s = SimpleLruZeroPage(ctl, firstZeroPage)?;
                if firstZeroPage == targetPage {
                    slotno = s;
                    serial_value_set(ctl, slotno, xid, minConflictCommitSeqNo);
                    ctl.shared.page_dirty[slotno] = true;
                    LWLockRelease(unsafe { &*bank_lock })?;
                    break;
                }
                firstZeroPage = SerialNextPage(firstZeroPage);
                LWLockRelease(unsafe { &*bank_lock })?;
            }
        } else {
            let bank_lock = SimpleLruGetBankLock(ctl, targetPage) as *const LWLock;
            LWLockAcquire(unsafe { &*bank_lock }, LW_EXCLUSIVE, procno)?;
            slotno = SimpleLruReadPage(ctl, targetPage, true, xid)?;
            serial_value_set(ctl, slotno, xid, minConflictCommitSeqNo);
            ctl.shared.page_dirty[slotno] = true;
            LWLockRelease(unsafe { &*bank_lock })?;
        }
        Ok(())
    })?;

    LWLockRelease(lock)?;
    Ok(())
}

/// `SerialGetMinConflictCommitSeqNo(xid)` (predicate.c).
pub fn SerialGetMinConflictCommitSeqNo(xid: TransactionId) -> PgResult<SerCommitSeqNo> {
    debug_assert!(TransactionIdIsValid(xid));
    let procno = my_proc_number();

    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_SHARED, procno)?;
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

    SERIAL_SLRU.with(|s| -> PgResult<SerCommitSeqNo> {
        let mut s = s.borrow_mut();
        let ctl = s.as_mut().expect("SerialSlruCtl not initialized");
        let slotno = SimpleLruReadPage_ReadOnly(ctl, SerialPage(xid), xid)?;
        let val = serial_value_get(ctl, slotno, xid);
        let bank_lock = SimpleLruGetBankLock(ctl, SerialPage(xid)) as *const LWLock;
        LWLockRelease(unsafe { &*bank_lock })?;
        Ok(val)
    })
}

/// `SerialSetActiveSerXmin(xid)` (predicate.c).
pub fn SerialSetActiveSerXmin(xid: TransactionId) -> PgResult<()> {
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, my_proc_number())?;

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

/// `CheckPointPredicate()` (predicate.c) — truncate the SLRU at a checkpoint.
pub fn CheckPointPredicate() -> PgResult<()> {
    let procno = my_proc_number();
    let lock = SerialControlLock();
    LWLockAcquire(lock, LW_EXCLUSIVE, procno)?;

    let headPage = serial_control().headPage;
    if headPage < 0 {
        LWLockRelease(lock)?;
        return Ok(());
    }

    let tailXid = serial_control().tailXid;
    let truncateCutoffPage;
    if TransactionIdIsValid(tailXid) {
        let tailPage = SerialPage(tailXid);
        if SerialPagePrecedesLogically(tailPage, headPage) {
            truncateCutoffPage = tailPage;
        } else {
            truncateCutoffPage = headPage;
        }
    } else {
        truncateCutoffPage = headPage;
        serial_control().headPage = -1;
    }

    LWLockRelease(lock)?;

    SERIAL_SLRU.with(|s| -> PgResult<()> {
        let mut s = s.borrow_mut();
        let ctl = s.as_mut().expect("SerialSlruCtl not initialized");
        SimpleLruTruncate(ctl, truncateCutoffPage)?;
        SimpleLruWriteAll(ctl, true)?;
        Ok(())
    })
}
