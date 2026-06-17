//! `clog.c` — the transaction-commit-log (`pg_xact`) manager
//! (`src/backend/access/transam/clog.c`, PostgreSQL 18.3).
//!
//! CLOG records the commit status of every transaction in a dedicated SLRU,
//! two status bits per xid (IN_PROGRESS / COMMITTED / ABORTED / SUB_COMMITTED).
//!
//! # State
//!
//! C keeps the SLRU control struct in the file-static `XactCtlData`. That is a
//! per-backend (shared-memory-backed) global; mirrored here by the thread-local
//! [`XACT_CTL`]. Every entry point operates on it through [`with_xact_ctl`],
//! exactly where C dereferences `XactCtl`.
//!
//! # Boundaries
//!
//! The SLRU buffer machinery (`SimpleLru*`) and the LWLock manager are consumed
//! directly from the ported sibling crates — not seamed. Everything clog reaches
//! outside itself that is not the SLRU/LWLock layer goes through the owner's seam
//! crate (panics until that owner lands): WAL insert/flush
//! ([`xloginsert_seams`] / [`xlog_seams`]), the recovery-state flag
//! ([`xlog_seams::in_recovery`]), `TransamVariables` reads and
//! `AdvanceOldestClogXid` ([`varsup_seams`]), the `transaction_buffers` GUC
//! (read as the per-backend global, written via [`guc_seams`]), and the
//! PGPROC-driven group-commit fields ([`proc_seams`]).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;

use backend_utils_error::errno::current_errno;
use backend_utils_error::{PgError, PgResult};
use types_error::PANIC;

use backend_access_transam_slru::{
    SimpleLruAutotuneBuffers, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruShmemSize, SimpleLruTruncate, SimpleLruWriteAll, SimpleLruWritePage,
    SimpleLruZeroPage, SlruCtlData, SlruScanDirCbReportPresence, SlruScanDirectory, SlruSyncFileTag,
    SlruPagePrecedesUnitTests, SLRU_MAX_ALLOWED_BUFFERS,
};
use backend_access_transam_transam::TransactionIdPrecedes;
use backend_storage_lmgr_lwlock::{LWLockAcquire, LWLockConditionalAcquire, LWLockRelease};

use backend_utils_init_small::globals;

use backend_access_transam_clog_seams as clog_seams;
use backend_access_transam_varsup_seams as varsup_seams;
use backend_access_transam_xlog_seams as xlog_seams;
use backend_access_transam_xloginsert_seams as xloginsert_seams;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_activity_waitevent_seams as waitevent_seams;
use backend_utils_misc_guc_seams as guc_seams;

use types_core::xact::{
    FirstNormalTransactionId, InvalidTransactionId, InvalidXLogRecPtr, MaxTransactionId, XidStatus,
    TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED, TRANSACTION_STATUS_IN_PROGRESS,
    TRANSACTION_STATUS_SUB_COMMITTED,
};
use types_core::{Oid, Size, TransactionId, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER};
use types_guc::guc::{PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT, PGC_S_OVERRIDE};
use types_pgstat::wait_event::WAIT_EVENT_XACT_GROUP_UPDATE;
use types_storage::storage::{LWLockMode, LW_EXCLUSIVE};
use types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler};
use types_storage::{LWTRANCHE_XACT_BUFFER, LWTRANCHE_XACT_SLRU};
use types_wal::rmgr::XLogReaderState;
use types_wal::rmgrdesc::xl_clog_truncate;
use types_wal::wal::{CLOG_TRUNCATE, CLOG_ZEROPAGE, RM_CLOG_ID};

// ---------------------------------------------------------------------------
// CLOG page size / status-bit constants (clog.c #defines).
// ---------------------------------------------------------------------------

/// `CLOG_BITS_PER_XACT` — two status bits per transaction.
pub const CLOG_BITS_PER_XACT: u32 = 2;
/// `CLOG_XACTS_PER_BYTE` — four xacts fit in a byte.
pub const CLOG_XACTS_PER_BYTE: u32 = 4;
/// `CLOG_XACTS_PER_PAGE` — transactions per CLOG page.
pub const CLOG_XACTS_PER_PAGE: u32 = BLCKSZ as u32 * CLOG_XACTS_PER_BYTE;
/// `CLOG_XACT_BITMASK` — mask selecting a transaction's two status bits.
pub const CLOG_XACT_BITMASK: u32 = (1 << CLOG_BITS_PER_XACT) - 1;

/// `CLOG_XACTS_PER_LSN_GROUP` (keep a power of 2) — the latest async LSN is
/// stored per group of this many transactions.
const CLOG_XACTS_PER_LSN_GROUP: u32 = 32;
/// `CLOG_LSNS_PER_PAGE = CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP`.
const CLOG_LSNS_PER_PAGE: i32 = (CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP) as i32;

/// `THRESHOLD_SUBTRANS_CLOG_OPT` — the number of subtransactions below which we
/// consider applying the clog group update optimization.
const THRESHOLD_SUBTRANS_CLOG_OPT: usize = 5;

/// `CLOG_MAX_ALLOWED_BUFFERS`. Because the space used in CLOG by each
/// transaction is so small, we place a smaller limit on the number of CLOG
/// buffers than SLRU allows.
const fn clog_max_allowed_buffers() -> i32 {
    let a = SLRU_MAX_ALLOWED_BUFFERS as i64;
    let b = ((MaxTransactionId as i64 / 2) + (CLOG_XACTS_PER_PAGE as i64 - 1))
        / CLOG_XACTS_PER_PAGE as i64;
    if a < b {
        a as i32
    } else {
        b as i32
    }
}

// ---------------------------------------------------------------------------
// XactCtlData (clog.c file-static, per-backend shmem control struct).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static SlruCtlData XactCtlData;` / `#define XactCtl (&XactCtlData)`.
    /// `None` before `CLOGShmemInit`; the SLRU read paths against an
    /// uninitialized clog would be a C null-array deref, here a panic.
    static XACT_CTL: RefCell<Option<SlruCtlData>> = const { RefCell::new(None) };
}

/// Run `f` with mutable access to `XactCtl`, the way C dereferences the
/// file-static control struct.
fn with_xact_ctl<R>(f: impl FnOnce(&mut SlruCtlData) -> R) -> R {
    XACT_CTL.with(|c| {
        let mut borrow = c.borrow_mut();
        let ctl = borrow
            .as_mut()
            .expect("CLOG accessed before CLOGShmemInit (XactCtl is NULL)");
        f(ctl)
    })
}

// ---------------------------------------------------------------------------
// CLOG page-number arithmetic (clog.c static inlines / macros).
// ---------------------------------------------------------------------------

/// `TransactionIdToPage(xid)`.
#[inline]
fn TransactionIdToPage(xid: TransactionId) -> i64 {
    xid as i64 / CLOG_XACTS_PER_PAGE as i64
}

/// `TransactionIdToPgIndex(xid)`.
#[inline]
fn TransactionIdToPgIndex(xid: TransactionId) -> u32 {
    xid % CLOG_XACTS_PER_PAGE
}

/// `TransactionIdToByte(xid)`.
#[inline]
fn TransactionIdToByte(xid: TransactionId) -> u32 {
    TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE
}

/// `TransactionIdToBIndex(xid)`.
#[inline]
fn TransactionIdToBIndex(xid: TransactionId) -> u32 {
    xid % CLOG_XACTS_PER_BYTE
}

/// `GetLSNIndex(slotno, xid)`.
#[inline]
fn GetLSNIndex(slotno: usize, xid: TransactionId) -> usize {
    slotno * CLOG_LSNS_PER_PAGE as usize
        + ((xid % CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_LSN_GROUP) as usize
}

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

#[inline]
fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

#[inline]
fn XLogRecPtrIsInvalid(ptr: XLogRecPtr) -> bool {
    ptr == InvalidXLogRecPtr
}

// ---------------------------------------------------------------------------
// SLRU bank-lock helpers.
//
// In C `SimpleLruGetBankLock(XactCtl, pageno)` yields an `LWLock *` (a separate
// shmem object). Here the lock lives inside `ctl.shared.bank_locks`, so we
// re-borrow `ctl` for the duration of each acquire/release call (the borrow
// ends before any mutable SLRU access), exactly the protocol slru.c itself
// uses internally.
// ---------------------------------------------------------------------------

#[inline]
fn bank_number(ctl: &SlruCtlData, pageno: i64) -> usize {
    (pageno % ctl.nbanks as i64) as usize
}

fn acquire_bank_lock(ctl: &SlruCtlData, pageno: i64, mode: LWLockMode) -> PgResult<()> {
    let bankno = bank_number(ctl, pageno);
    LWLockAcquire(
        &ctl.shared.bank_locks[bankno].lock,
        mode,
        globals::MyProcNumber(),
    )?;
    Ok(())
}

fn conditional_acquire_bank_lock(
    ctl: &SlruCtlData,
    pageno: i64,
    mode: LWLockMode,
) -> PgResult<bool> {
    let bankno = bank_number(ctl, pageno);
    LWLockConditionalAcquire(&ctl.shared.bank_locks[bankno].lock, mode)
}

fn release_bank_lock(ctl: &SlruCtlData, pageno: i64) -> PgResult<()> {
    let bankno = bank_number(ctl, pageno);
    LWLockRelease(&ctl.shared.bank_locks[bankno].lock)
}

// ---------------------------------------------------------------------------
// TransactionIdSetTreeStatus and friends.
// ---------------------------------------------------------------------------

/// `TransactionIdSetTreeStatus(xid, nsubxids, subxids, status, lsn)`.
///
/// Record the final state of transaction entries in the commit log for a
/// transaction and its subtransaction tree, as atomically as possible.
pub fn TransactionIdSetTreeStatus(
    xid: TransactionId,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    let nsubxids = subxids.len();
    let pageno = TransactionIdToPage(xid); // get page of parent

    debug_assert!(status == TRANSACTION_STATUS_COMMITTED || status == TRANSACTION_STATUS_ABORTED);

    // See how many subxids, if any, are on the same page as the parent.
    let mut i = 0;
    while i < nsubxids {
        if TransactionIdToPage(subxids[i]) != pageno {
            break;
        }
        i += 1;
    }

    // Do all items fit on a single page?
    if i == nsubxids {
        // Set the parent and all subtransactions in a single call.
        TransactionIdSetPageStatus(xid, nsubxids, subxids, status, lsn, pageno, true)
    } else {
        let nsubxids_on_first_page = i;

        // If this is a commit then we care about doing this correctly (i.e.
        // using the subcommitted intermediate status). By here, we know we're
        // updating more than one page of clog, so we must mark entries that are
        // *not* on the first page so that they show as subcommitted before we
        // then return to update the status to fully committed. To avoid touching
        // the first page twice, skip marking subcommitted for the subxids on
        // that first page.
        if status == TRANSACTION_STATUS_COMMITTED {
            set_status_by_pages(
                &subxids[nsubxids_on_first_page..],
                TRANSACTION_STATUS_SUB_COMMITTED,
                lsn,
            )?;
        }

        // Now set the parent and subtransactions on same page as the parent.
        let pageno = TransactionIdToPage(xid);
        TransactionIdSetPageStatus(
            xid,
            nsubxids_on_first_page,
            subxids,
            status,
            lsn,
            pageno,
            false,
        )?;

        // Now work through the rest of the subxids one clog page at a time,
        // starting from the second page onwards, like we did above.
        set_status_by_pages(&subxids[nsubxids_on_first_page..], status, lsn)
    }
}

/// `set_status_by_pages` — set the status for a bunch of transactions, chunking
/// in the separate CLOG pages involved. We never pass the whole transaction tree
/// to this function, only subtransactions on different pages to the top level.
fn set_status_by_pages(
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    let nsubxids = subxids.len();
    debug_assert!(nsubxids > 0); // else the pageno fetch below is unsafe

    let mut pageno = TransactionIdToPage(subxids[0]);
    let mut offset = 0;
    let mut i = 0;

    while i < nsubxids {
        let mut num_on_page = 0;
        let mut nextpageno;

        loop {
            nextpageno = TransactionIdToPage(subxids[i]);
            if nextpageno != pageno {
                break;
            }
            num_on_page += 1;
            i += 1;
            if i >= nsubxids {
                break;
            }
        }

        TransactionIdSetPageStatus(
            InvalidTransactionId,
            num_on_page,
            &subxids[offset..],
            status,
            lsn,
            pageno,
            false,
        )?;
        offset = i;
        pageno = nextpageno;
    }
    Ok(())
}

/// `TransactionIdSetPageStatus` — record the final state of transaction entries
/// in the commit log for all entries on a single page. Atomic only on this page.
fn TransactionIdSetPageStatus(
    xid: TransactionId,
    nsubxids: usize,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
    all_xact_same_page: bool,
) -> PgResult<()> {
    // StaticAssertDecl(THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS):
    // a compile-time invariant of the proc layer; nothing to do at run time.

    // When there is contention on the SLRU bank lock we need, we try to group
    // multiple updates; a single leader process will perform transaction status
    // updates for multiple backends so that the number of times the bank lock
    // needs to be acquired is reduced.
    //
    // For this optimization to be safe, the XID and subxids in MyProc must be
    // the same as the ones for which we're setting the status; for it to be
    // efficient, we shouldn't have too many sub-XIDs and all of the XIDs should
    // be on the same page.
    if all_xact_same_page
        && TransactionIdEquals(xid, proc_seams::my_proc_xid::call())
        && nsubxids <= THRESHOLD_SUBTRANS_CLOG_OPT
        && my_proc_subxids_match(nsubxids, subxids)
    {
        // If we can immediately acquire the lock, we update the status of our
        // own XID and release the lock. If not, try use group XID update. If
        // that doesn't work out, fall back to waiting for the lock to perform an
        // update for this transaction only.
        if with_xact_ctl(|ctl| conditional_acquire_bank_lock(ctl, pageno, LW_EXCLUSIVE))? {
            // Got the lock without waiting! Do the update.
            let res =
                TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
            with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;
            return res;
        } else if TransactionGroupUpdateXidStatus(xid, status, lsn, pageno)? {
            // Group update mechanism has done the work.
            return Ok(());
        }

        // Fall through only if update isn't done yet.
    }

    // Group update not applicable, or couldn't accept this page number.
    with_xact_ctl(|ctl| acquire_bank_lock(ctl, pageno, LW_EXCLUSIVE))?;
    let res = TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
    with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;
    res
}

/// The `xid == MyProc->xid && nsubxids == MyProc->subxidStatus.count &&
/// (nsubxids == 0 || memcmp(subxids, MyProc->subxids.xids, ...) == 0)` portion
/// of the group-eligibility test (the `xid` / `nsubxids` parts are checked by
/// the caller).
fn my_proc_subxids_match(nsubxids: usize, subxids: &[TransactionId]) -> bool {
    let (count, my_subxids) = proc_seams::my_proc_subxids::call();
    if nsubxids != count as usize {
        return false;
    }
    nsubxids == 0 || subxids[..nsubxids] == my_subxids[..nsubxids]
}

/// `TransactionIdSetPageStatusInternal` — record the final state of transaction
/// entries in the commit log. We don't do any locking here; caller must handle
/// that.
fn TransactionIdSetPageStatusInternal(
    xid: TransactionId,
    nsubxids: usize,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
) -> PgResult<()> {
    debug_assert!(
        status == TRANSACTION_STATUS_COMMITTED
            || status == TRANSACTION_STATUS_ABORTED
            || (status == TRANSACTION_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid))
    );
    // Assert(LWLockHeldByMeInMode(SimpleLruGetBankLock(XactCtl, pageno),
    // LW_EXCLUSIVE)) — the bank lock is held by the caller.

    // If we're doing an async commit (ie, lsn is valid), then we must wait for
    // any active write on the page slot to complete. Otherwise our update could
    // reach disk in that write, which will not do since we mustn't let it reach
    // disk until we've done the appropriate WAL flush. But when lsn is invalid,
    // it's OK to scribble on a page while it is write-busy.
    let slotno =
        with_xact_ctl(|ctl| SimpleLruReadPage(ctl, pageno, XLogRecPtrIsInvalid(lsn), xid))?;

    // Set the main transaction id, if any.
    //
    // If we update more than one xid on this page while it is being written out,
    // we might find that some of the bits go to disk and others don't. If we are
    // updating commits on the page with the top-level xid that could break
    // atomicity, so we subcommit the subxids first before we mark the top-level
    // commit.
    if TransactionIdIsValid(xid) {
        // Subtransactions first, if needed ...
        if status == TRANSACTION_STATUS_COMMITTED {
            for &sub in subxids.iter().take(nsubxids) {
                debug_assert_eq!(
                    with_xact_ctl(|ctl| ctl.shared.page_number[slotno]),
                    TransactionIdToPage(sub)
                );
                TransactionIdSetStatusBit(sub, TRANSACTION_STATUS_SUB_COMMITTED, lsn, slotno)?;
            }
        }

        // ... then the main transaction.
        TransactionIdSetStatusBit(xid, status, lsn, slotno)?;
    }

    // Set the subtransactions.
    for &sub in subxids.iter().take(nsubxids) {
        debug_assert_eq!(
            with_xact_ctl(|ctl| ctl.shared.page_number[slotno]),
            TransactionIdToPage(sub)
        );
        TransactionIdSetStatusBit(sub, status, lsn, slotno)?;
    }

    with_xact_ctl(|ctl| ctl.shared.page_dirty[slotno] = true);
    Ok(())
}

/// `TransactionGroupUpdateXidStatus` — group-commit subroutine for
/// `TransactionIdSetPageStatus`.
///
/// When we cannot immediately acquire the SLRU bank lock in exclusive mode at
/// commit time, add ourselves to a list of processes that need their XIDs status
/// update. The first process to add itself to the list will acquire the lock in
/// exclusive mode and set transaction status for all group members.
///
/// Returns `true` when transaction status has been updated in clog; `false` if
/// we decided against applying the optimization because the page we need differs
/// from those processes already waiting.
fn TransactionGroupUpdateXidStatus(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
) -> PgResult<bool> {
    let my_proc_number = globals::MyProcNumber();

    // We should definitely have an XID whose status needs to be updated.
    debug_assert!(TransactionIdIsValid(xid));

    // Prepare to add ourselves to the list of processes needing a group XID
    // status update.
    proc_seams::set_my_proc_clog_group_member_data::call(xid, status, pageno, lsn);

    // We put ourselves in the queue by writing MyProcNumber to
    // ProcGlobal->clogGroupFirst. However, if there's already a process listed
    // there, we compare our pageno with that of that process; if it differs, we
    // cannot participate in the group, so we return for the caller to update
    // pg_xact in the normal way.
    let mut nextidx = proc_seams::clog_group_first_read::call();

    loop {
        // Add the proc to list, if the clog page where we need to update the
        // current transaction status is the same as the group leader's clog
        // page.
        //
        // There is a race condition here, which is that after doing the below
        // check and before adding this proc's clog update to a group, the group
        // leader might have already finished the group update for this page and
        // become group leader of another group, updating a different page. This
        // will lead to a situation where a single group can have different clog
        // page updates; we handle this case by switching bank locks in the loop
        // below.
        if nextidx != INVALID_PROC_NUMBER as u32
            && proc_seams::proc_clog_group_member_page::call(nextidx as i32) != pageno
        {
            // Ensure that this proc is not a member of any clog group that needs
            // an XID status update.
            proc_seams::set_my_proc_clog_group_member::call(false);
            proc_seams::set_my_proc_clog_group_next::call(INVALID_PROC_NUMBER as u32);
            return Ok(false);
        }

        proc_seams::set_my_proc_clog_group_next::call(nextidx);

        let (succeeded, seen) =
            proc_seams::clog_group_first_compare_exchange::call(nextidx, my_proc_number as u32);
        if succeeded {
            break;
        }
        nextidx = seen;
    }

    // If the list was not empty, the leader will update the status of our XID.
    // It is impossible to have followers without a leader because the first
    // process to add itself will always have nextidx as INVALID_PROC_NUMBER.
    if nextidx != INVALID_PROC_NUMBER as u32 {
        let mut extra_waits = 0_i32;

        // Sleep until the leader updates our XID status.
        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_XACT_GROUP_UPDATE);
        loop {
            // acts as a read barrier
            proc_seams::pg_semaphore_lock::call(my_proc_number);
            if !proc_seams::my_proc_clog_group_member::call() {
                break;
            }
            extra_waits += 1;
        }
        waitevent_seams::pgstat_report_wait_end::call();

        debug_assert_eq!(
            proc_seams::my_proc_clog_group_next::call(),
            INVALID_PROC_NUMBER as u32
        );

        // Fix semaphore count for any absorbed wakeups.
        while extra_waits > 0 {
            extra_waits -= 1;
            proc_seams::pg_semaphore_unlock::call(my_proc_number);
        }
        return Ok(true);
    }

    // By here, we know we're the leader process. Acquire the SLRU bank lock that
    // corresponds to the page we originally wanted to modify.
    let mut prevpageno = pageno;
    with_xact_ctl(|ctl| acquire_bank_lock(ctl, prevpageno, LW_EXCLUSIVE))?;

    // Now that we've got the lock, clear the list of processes waiting for group
    // XID status update, saving a pointer to the head of the list. (Trying to
    // pop elements one at a time could lead to an ABA problem.) At this point,
    // any processes trying to do this would create a separate group.
    let mut nextidx = proc_seams::clog_group_first_exchange::call(INVALID_PROC_NUMBER as u32);

    // Remember head of list so we can perform wakeups after dropping lock.
    let mut wakeidx = nextidx;

    // Walk the list and update the status of all XIDs.
    while nextidx != INVALID_PROC_NUMBER as u32 {
        let thispageno = proc_seams::proc_clog_group_member_page::call(nextidx as i32);

        // If the page to update belongs to a different bank than the previous
        // one, exchange bank lock to the new one. This should be quite rare.
        if thispageno != prevpageno {
            let same_bank = with_xact_ctl(|ctl| {
                bank_number(ctl, thispageno) == bank_number(ctl, prevpageno)
            });
            if !same_bank {
                with_xact_ctl(|ctl| release_bank_lock(ctl, prevpageno))?;
                with_xact_ctl(|ctl| acquire_bank_lock(ctl, thispageno, LW_EXCLUSIVE))?;
            }
            prevpageno = thispageno;
        }

        // Transactions with more than THRESHOLD_SUBTRANS_CLOG_OPT sub-XIDs should
        // not use group XID status update mechanism.
        let (count, subxids) = proc_seams::proc_subxids::call(nextidx as i32);
        debug_assert!(count as usize <= THRESHOLD_SUBTRANS_CLOG_OPT);
        let (member_xid, member_status, member_lsn) =
            proc_seams::proc_clog_group_member_update::call(nextidx as i32);

        TransactionIdSetPageStatusInternal(
            member_xid,
            count as usize,
            &subxids,
            member_status,
            member_lsn,
            proc_seams::proc_clog_group_member_page::call(nextidx as i32),
        )?;

        // Move to next proc in list.
        nextidx = proc_seams::proc_clog_group_next::call(nextidx as i32);
    }

    // We're done with the lock now.
    with_xact_ctl(|ctl| release_bank_lock(ctl, prevpageno))?;

    // Now that we've released the lock, go back and wake everybody up. We don't
    // do this under the lock so as to keep lock hold times to a minimum.
    while wakeidx != INVALID_PROC_NUMBER as u32 {
        let this = wakeidx;
        wakeidx = proc_seams::proc_clog_group_next::call(this as i32);
        proc_seams::set_proc_clog_group_next::call(this as i32, INVALID_PROC_NUMBER as u32);

        // ensure all previous writes are visible before follower continues.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        proc_seams::set_proc_clog_group_member::call(this as i32, false);

        if this != my_proc_number as u32 {
            proc_seams::pg_semaphore_unlock::call(this as i32);
        }
    }

    Ok(true)
}

/// `TransactionIdSetStatusBit` — set the commit status of a single transaction.
///
/// Caller must hold the corresponding SLRU bank lock, will be held at exit.
fn TransactionIdSetStatusBit(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    slotno: usize,
) -> PgResult<()> {
    let byteno = TransactionIdToByte(xid) as usize;
    let bshift = (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT) as i32;

    debug_assert_eq!(
        with_xact_ctl(|ctl| ctl.shared.page_number[slotno]),
        TransactionIdToPage(xid)
    );

    let curval = with_xact_ctl(|ctl| {
        ((ctl.shared.page_buffer(slotno)[byteno] as i32 >> bshift) & CLOG_XACT_BITMASK as i32)
            as XidStatus
    });

    // When replaying transactions during recovery we still need to perform the
    // two phases of subcommit and then commit. However, some transactions are
    // already correctly marked, so we just treat those as a no-op which allows
    // us to keep the following assert as restrictive as possible.
    if xlog_seams::in_recovery::call()
        && status == TRANSACTION_STATUS_SUB_COMMITTED
        && curval == TRANSACTION_STATUS_COMMITTED
    {
        return Ok(());
    }

    // Current state change should be from 0 or subcommitted to target state or
    // we should already be there when replaying changes during recovery.
    debug_assert!(
        curval == 0
            || (curval == TRANSACTION_STATUS_SUB_COMMITTED
                && status != TRANSACTION_STATUS_IN_PROGRESS)
            || curval == status
    );

    // note this assumes exclusive access to the clog page
    with_xact_ctl(|ctl| {
        let mut byteval = ctl.shared.page_buffer(slotno)[byteno] as i32;
        byteval &= !(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);
        byteval |= status << bshift;
        ctl.shared.page_buffer_mut(slotno)[byteno] = byteval as u8;
    });

    // Update the group LSN if the transaction completion LSN is higher.
    //
    // Note: lsn will be invalid when supplied during InRecovery processing, so
    // we don't need to do anything special to avoid LSN updates during recovery.
    if !XLogRecPtrIsInvalid(lsn) {
        let lsnindex = GetLSNIndex(slotno, xid);
        with_xact_ctl(|ctl| {
            if ctl.shared.group_lsn[lsnindex] < lsn {
                ctl.shared.group_lsn[lsnindex] = lsn;
            }
        });
    }
    Ok(())
}

/// `TransactionIdGetStatus(xid, *lsn)` — interrogate the state of a transaction
/// in the commit log, returning `(status, lsn)` where `lsn` is late enough to
/// guarantee that flushing up to it has flushed the transaction's commit record.
pub fn TransactionIdGetStatus(xid: TransactionId) -> PgResult<(XidStatus, XLogRecPtr)> {
    let pageno = TransactionIdToPage(xid);
    let byteno = TransactionIdToByte(xid) as usize;
    let bshift = (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT) as i32;

    // lock is acquired by SimpleLruReadPage_ReadOnly
    let slotno = with_xact_ctl(|ctl| SimpleLruReadPage_ReadOnly(ctl, pageno, xid))?;

    let status = with_xact_ctl(|ctl| {
        ((ctl.shared.page_buffer(slotno)[byteno] as i32 >> bshift) & CLOG_XACT_BITMASK as i32)
            as XidStatus
    });

    let lsnindex = GetLSNIndex(slotno, xid);
    let lsn = with_xact_ctl(|ctl| ctl.shared.group_lsn[lsnindex]);

    with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;

    Ok((status, lsn))
}

// ---------------------------------------------------------------------------
// Shared-memory sizing and initialization.
// ---------------------------------------------------------------------------

/// `CLOGShmemBuffers` — number of shared CLOG buffers.
///
/// If asked to autotune, use 2MB for every 1GB of shared buffers, up to 8MB.
/// Otherwise just cap the configured amount to be between 16 and the maximum
/// allowed.
fn CLOGShmemBuffers() -> i32 {
    // auto-tune based on shared buffers
    if globals::transaction_buffers() == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    globals::transaction_buffers()
        .max(16)
        .min(clog_max_allowed_buffers())
}

/// `CLOGShmemSize()` — shared-memory size for the CLOG SLRU.
pub fn CLOGShmemSize() -> Size {
    SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE)
}

/// `CLOGShmemInit()` — initialize the CLOG SLRU in shared memory and stash the
/// control struct in the per-backend `XactCtl`.
pub fn CLOGShmemInit() -> PgResult<()> {
    // If auto-tuning is requested, now is the time to do it.
    if globals::transaction_buffers() == 0 {
        let buf = format!("{}", CLOGShmemBuffers());
        guc_seams::set_config_option::call(
            "transaction_buffers",
            &buf,
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        )?;

        // We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
        // However, if the DBA explicitly set transaction_buffers = 0 in the
        // config file, then PGC_S_DYNAMIC_DEFAULT will fail to override that and
        // we must force the matter with PGC_S_OVERRIDE.
        if globals::transaction_buffers() == 0 {
            // failed to apply it?
            guc_seams::set_config_option::call(
                "transaction_buffers",
                &buf,
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(globals::transaction_buffers() != 0);

    let nslots = CLOGShmemBuffers();
    let mut ctl = SimpleLruInit(
        "transaction",
        nslots,
        CLOG_LSNS_PER_PAGE,
        "pg_xact",
        LWTRANCHE_XACT_BUFFER,
        LWTRANCHE_XACT_SLRU,
        SyncRequestHandler::SYNC_HANDLER_CLOG,
        false,
    )?;
    ctl.PagePrecedes = Some(CLOGPagePrecedes);
    SlruPagePrecedesUnitTests(&ctl, CLOG_XACTS_PER_PAGE as i32);

    XACT_CTL.with(|c| *c.borrow_mut() = Some(ctl));
    Ok(())
}

/// `check_transaction_buffers` — GUC check_hook for `transaction_buffers`.
pub fn check_transaction_buffers(newval: i32) -> (bool, Option<String>) {
    backend_access_transam_slru::check_slru_buffers("transaction_buffers", newval)
}

/// `BootStrapCLOG()` — create and zero the first CLOG page during bootstrap.
///
/// This func must be called ONCE on system install. (The CLOG directory is
/// assumed to have been created by initdb, and `CLOGShmemInit` must have been
/// called already.)
pub fn BootStrapCLOG() -> PgResult<()> {
    with_xact_ctl(|ctl| acquire_bank_lock(ctl, 0, LW_EXCLUSIVE))?;

    // Create and zero the first page of the commit log.
    let slotno = ZeroCLOGPage(0, false)?;

    // Make sure it's written out.
    with_xact_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
    debug_assert!(!with_xact_ctl(|ctl| ctl.shared.page_dirty[slotno]));

    with_xact_ctl(|ctl| release_bank_lock(ctl, 0))?;
    Ok(())
}

/// `ZeroCLOGPage(pageno, writeXlog)` — initialize (or reinitialize) a page of
/// CLOG to zeroes; if `writeXlog`, also emit an XLOG record. The page is not
/// actually written, just set up in shared memory; the slot number is returned.
/// Control lock must be held at entry, and will be held at exit.
fn ZeroCLOGPage(pageno: i64, writeXlog: bool) -> PgResult<usize> {
    let slotno = with_xact_ctl(|ctl| SimpleLruZeroPage(ctl, pageno))?;

    if writeXlog {
        WriteZeroPageXlogRec(pageno)?;
    }

    Ok(slotno)
}

/// `StartupCLOG()` — initialize our idea of the latest page number at startup.
///
/// This must be called ONCE during postmaster or standalone-backend startup,
/// after StartupXLOG has initialized TransamVariables->nextXid.
pub fn StartupCLOG() -> PgResult<()> {
    // TransactionId xid = XidFromFullTransactionId(TransamVariables->nextXid);
    let xid = varsup_seams::read_next_transaction_id::call();
    let pageno = TransactionIdToPage(xid);

    // Initialize our idea of the latest page number.
    with_xact_ctl(|ctl| ctl.shared.latest_page_number.write(pageno as u64));
    Ok(())
}

/// `TrimCLOG()` — zero out the tail of the current CLOG page at recovery end.
///
/// This must be called ONCE at the end of startup/recovery.
pub fn TrimCLOG() -> PgResult<()> {
    let xid = varsup_seams::read_next_transaction_id::call();
    let pageno = TransactionIdToPage(xid);
    with_xact_ctl(|ctl| acquire_bank_lock(ctl, pageno, LW_EXCLUSIVE))?;

    // Zero out the remainder of the current clog page. Under normal
    // circumstances it should be zeroes already, but it seems at least
    // theoretically possible that XLOG replay will have settled on a nextXID
    // value that is less than the last XID actually used and marked by the
    // previous database lifecycle (since subtransaction commit writes clog but
    // makes no WAL entry). Let's just be safe. (We need not worry about pages
    // beyond the current one, since those will be zeroed when first used. For
    // the same reason, there is no need to do anything when nextXid is exactly at
    // a page boundary; and it's likely that the "current" page doesn't exist yet
    // in that case.)
    if TransactionIdToPgIndex(xid) != 0 {
        let byteno = TransactionIdToByte(xid) as usize;
        let bshift = (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT) as i32;

        let slotno = with_xact_ctl(|ctl| SimpleLruReadPage(ctl, pageno, false, xid))?;
        with_xact_ctl(|ctl| {
            let buffer = ctl.shared.page_buffer_mut(slotno);
            // Zero so-far-unused positions in the current byte.
            buffer[byteno] &= ((1 << bshift) - 1) as u8;
            // Zero the rest of the page.
            for slot in buffer.iter_mut().take(BLCKSZ).skip(byteno + 1) {
                *slot = 0;
            }
            ctl.shared.page_dirty[slotno] = true;
        });
    }

    with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;
    Ok(())
}

/// `CheckPointCLOG()` — flush dirty CLOG pages at a checkpoint.
///
/// Writes dirty CLOG pages to disk; this may result in sync requests queued for
/// later handling by ProcessSyncRequests(), as part of the checkpoint.
pub fn CheckPointCLOG() -> PgResult<()> {
    // TRACE_POSTGRESQL_CLOG_CHECKPOINT_START/DONE are dtrace probes (no-ops).
    with_xact_ctl(|ctl| SimpleLruWriteAll(ctl, true))
}

/// `ExtendCLOG(newestXact)` — make sure that CLOG has room for a newly-allocated
/// XID.
///
/// NB: this is called while holding XidGenLock. We want it to be very fast most
/// of the time; even when it's not so fast, no actual I/O need happen unless
/// we're forced to write out a dirty clog or xlog page to make room in shared
/// memory.
pub fn ExtendCLOG(newestXact: TransactionId) -> PgResult<()> {
    // No work except at first XID of a page. But beware: just after wraparound,
    // the first XID of page zero is FirstNormalTransactionId.
    if TransactionIdToPgIndex(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return Ok(());
    }

    let pageno = TransactionIdToPage(newestXact);
    with_xact_ctl(|ctl| acquire_bank_lock(ctl, pageno, LW_EXCLUSIVE))?;

    // Zero the page and make an XLOG entry about it.
    ZeroCLOGPage(pageno, true)?;

    with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;
    Ok(())
}

/// `TruncateCLOG(oldestXact, oldestxid_datoid)` — remove all CLOG segments
/// before the one holding the passed transaction ID.
///
/// Before removing any CLOG data, we must flush XLOG to disk, to ensure that any
/// recently-emitted records with freeze plans have reached disk; otherwise a
/// crash and restart might leave us with some unfrozen tuples referencing removed
/// CLOG data. We also emit a special TRUNCATE XLOG record.
pub fn TruncateCLOG(oldestXact: TransactionId, oldestxid_datoid: Oid) -> PgResult<()> {
    // The cutoff point is the start of the segment containing oldestXact. We
    // pass the *page* containing oldestXact to SimpleLruTruncate.
    let cutoffPage = TransactionIdToPage(oldestXact);

    // Check to see if there's any files that could be removed.
    let present = with_xact_ctl(|ctl| {
        SlruScanDirectory(ctl, |ctl, filename, segpage| {
            SlruScanDirCbReportPresence(ctl, filename, segpage, cutoffPage)
        })
    })?;
    if !present {
        return Ok(()); // nothing to remove
    }

    // Advance oldestClogXid before truncating clog, so concurrent xact status
    // lookups can ensure they don't attempt to access truncated-away clog. It's
    // only necessary to do this if we will actually truncate away clog pages.
    varsup_seams::advance_oldest_clog_xid::call(oldestXact)?;

    // Write XLOG record and flush XLOG to disk. We record the oldest xid we're
    // keeping information about here so we can ensure that it's always ahead of
    // clog truncation in case we crash, and so a standby finds out the new valid
    // xid before the next checkpoint.
    WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid)?;

    // Now we can remove the old CLOG segment(s).
    with_xact_ctl(|ctl| SimpleLruTruncate(ctl, cutoffPage))
}

/// `CLOGPagePrecedes(page1, page2)` — decide whether a CLOG page number is
/// "older" for truncation purposes.
///
/// We need to use comparison of TransactionIds here in order to do the right
/// thing with wraparound XID arithmetic. However, `TransactionIdPrecedes()`
/// would get weird about permanent xact IDs. So, offset both such that `xid1`,
/// `xid2`, and `xid2 + CLOG_XACTS_PER_PAGE - 1` are all normal XIDs.
fn CLOGPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(CLOG_XACTS_PER_PAGE - 1))
}

// ---------------------------------------------------------------------------
// WAL record emission.
// ---------------------------------------------------------------------------

/// `WriteZeroPageXlogRec(pageno)` — write a ZEROPAGE xlog record.
fn WriteZeroPageXlogRec(pageno: i64) -> PgResult<()> {
    // XLogBeginInsert(); XLogRegisterData(&pageno, sizeof(pageno));
    // (void) XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
    xloginsert_seams::xlog_insert::call(RM_CLOG_ID, CLOG_ZEROPAGE, 0, &[&pageno.to_ne_bytes()])?;
    Ok(())
}

/// `WriteTruncateXlogRec(pageno, oldestXact, oldestXactDb)` — write a TRUNCATE
/// xlog record. We must flush the xlog record to disk before returning.
fn WriteTruncateXlogRec(pageno: i64, oldestXact: TransactionId, oldestXactDb: Oid) -> PgResult<()> {
    // xl_clog_truncate { pageno@0 (i64), oldestXact@8 (u32), oldestXactDb@12 (u32) }
    let mut xlrec = [0u8; 16];
    xlrec[0..8].copy_from_slice(&pageno.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&oldestXact.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&oldestXactDb.to_ne_bytes());

    // XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xl_clog_truncate));
    // recptr = XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE);
    let recptr = xloginsert_seams::xlog_insert::call(RM_CLOG_ID, CLOG_TRUNCATE, 0, &[&xlrec])?;
    xlog_seams::xlog_flush::call(recptr)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// CLOG resource manager's routines.
// ---------------------------------------------------------------------------

/// `clog_redo(record)` — WAL redo handler for CLOG records.
pub fn clog_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("clog_redo dispatched on a decoded record");
    // uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    let info = decoded.info() & !types_wal::wal::XLR_INFO_MASK;

    // Backup blocks are not used in clog records.
    debug_assert!(decoded.max_block_id() < 0);

    if info == CLOG_ZEROPAGE {
        // memcpy(&pageno, XLogRecGetData(record), sizeof(pageno));
        let data = decoded.data();
        let mut pageno_bytes = [0u8; 8];
        pageno_bytes.copy_from_slice(&data[..8]);
        let pageno = i64::from_ne_bytes(pageno_bytes);

        with_xact_ctl(|ctl| acquire_bank_lock(ctl, pageno, LW_EXCLUSIVE))?;

        let slotno = ZeroCLOGPage(pageno, false)?;
        with_xact_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
        debug_assert!(!with_xact_ctl(|ctl| ctl.shared.page_dirty[slotno]));

        with_xact_ctl(|ctl| release_bank_lock(ctl, pageno))?;
        Ok(())
    } else if info == CLOG_TRUNCATE {
        // memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_clog_truncate));
        let xlrec = xl_clog_truncate::from_bytes(decoded.data())
            .expect("clog_redo TRUNCATE record too short");

        varsup_seams::advance_oldest_clog_xid::call(xlrec.oldest_xact())?;

        with_xact_ctl(|ctl| SimpleLruTruncate(ctl, xlrec.pageno()))
    } else {
        Err(PgError::new(
            PANIC,
            format!("clog_redo: unknown op code {info}"),
        ))
    }
}

/// `clogsyncfiletag(ftag, path)` — entrypoint for sync.c to sync clog files.
pub fn clogsyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    // return SlruSyncFileTag(XactCtl, ftag, path);
    let (result, path) = with_xact_ctl(|ctl| SlruSyncFileTag(ctl, &ftag))?;
    let errno = current_errno();
    Ok(FileTagOpResult {
        result,
        path,
        errno,
    })
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam owned by `backend-access-transam-clog-seams`.
pub fn init_seams() {
    clog_seams::clog_redo::set(clog_redo);
    clog_seams::transaction_id_get_status::set(TransactionIdGetStatus);
    clog_seams::transaction_id_set_tree_status::set(TransactionIdSetTreeStatus);
    clog_seams::clogsyncfiletag::set(clogsyncfiletag);
    clog_seams::clog_shmem_size::set(clog_shmem_size_seam);
    clog_seams::clog_shmem_init::set(clog_shmem_init_seam);
    clog_seams::extend_clog::set(ExtendCLOG);

    // WAL-startup entry points called once by `StartupXLOG` (xlog.c) on the
    // clean DB_SHUTDOWNED / end-of-recovery path.
    clog_seams::startup_clog::set(StartupCLOG);
    clog_seams::trim_clog::set(TrimCLOG);

    // vacuum's `vac_truncate_clog` truncation entry point.
    clog_seams::truncate_clog::set(TruncateCLOG);

    // GUC check_hook for `transaction_buffers` (clog.c check_transaction_buffers).
    // Fired e.g. by CLOGShmemInit's auto-sizing SetConfigOption. The variable's
    // backing store + accessors live in backend-utils-init-small (the
    // `transaction_buffers` global); clog owns only the check hook.
    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<backend_utils_misc_guc_tables::GucHookExtra>,
        _source: types_guc::guc::GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_transaction_buffers(*newval);
        if ok {
            Ok(true)
        } else {
            // C sets GUC_check_errdetail and returns false; carry the detail on
            // Err per the GUC check-hook contract (mirrors subtrans).
            match detail {
                Some(d) => Err(types_error::PgError::error(d)),
                None => Ok(false),
            }
        }
    }
    backend_utils_misc_guc_tables::hooks::check_transaction_buffers.install(check_hook);
}

/// `CLOGShmemSize()` wrapper for the `clog_shmem_size` seam (`PgResult<Size>`:
/// the C accumulates via `add_size`/`mul_size`, whose overflow `ereport`s; the
/// owned sizing here is infallible, so this is always `Ok`).
fn clog_shmem_size_seam() -> PgResult<Size> {
    Ok(CLOGShmemSize())
}

/// `CLOGShmemInit()` wrapper for the `clog_shmem_init` seam.
fn clog_shmem_init_seam() -> PgResult<()> {
    CLOGShmemInit()
}

#[cfg(test)]
mod tests;
