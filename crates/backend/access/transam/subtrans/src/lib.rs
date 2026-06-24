//! `subtrans.c` — the subtransaction-log (`pg_subtrans`) manager
//! (`src/backend/access/transam/subtrans.c`, PostgreSQL 18.3).
//!
//! The pg_subtrans manager is a pg_xact-like manager that stores the parent
//! transaction id for each transaction. A main transaction has a parent of
//! `InvalidTransactionId`, and each subtransaction records its immediate
//! parent, so the tree can be walked from child to parent. It is *not*
//! WAL-logged: there is no need to preserve the data across a crash, and
//! during startup the currently-active page(s) are simply forced to zeroes.
//!
//! The SLRU buffer machinery (`SimpleLru*`) is consumed directly from the
//! ported sibling [`slru`]. The shared control struct
//! that C keeps in the file-static `SubTransCtlData` is owned here by
//! [`SubTransState`]; a backend creates it once via [`SUBTRANSShmemInit`] and
//! it lives in a per-backend [`thread_local`] (mirroring the C file static),
//! through which the installed seams reach it.
//!
//! `subtransaction_buffers` is the C GUC `int` variable; its backing store is
//! the `thread_local` here, exposed to the GUC machinery through the
//! `guc-tables` typed accessor slot installed in [`init_seams`].
//!
//! Cross-subsystem reads — `TransactionXmin` (snapmgr) and
//! `TransamVariables->nextXid` (varsup), plus the `SetConfigOption`
//! publish — go through the owners' seam crates; until those owners land the
//! call panics loudly rather than silently dropping subtrans state.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::cell::{Cell, RefCell};

use ::slru::{
    SimpleLruAutotuneBuffers, SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage,
    SimpleLruReadPage_ReadOnly, SimpleLruShmemSize, SimpleLruTruncate, SimpleLruWriteAll,
    SimpleLruWritePage, SimpleLruZeroPage, SlruCtlData, SlruPagePrecedesUnitTests,
    SLRU_MAX_ALLOWED_BUFFERS,
};
use ::transam::{
    TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdPrecedes,
};
use ::lwlock::{LWLockAcquire, LWLockRelease};
use ::utils_error::{ereport, PgError, PgResult};
use init_small_seams as globals;

use ::types_core::{
    FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId, Size, TransactionId, BLCKSZ,
};
use ::types_error::{ErrorLocation, ERROR};
use ::types_storage::sync::SyncRequestHandler;
use ::types_storage::{LWTRANCHE_SUBTRANS_BUFFER, LWTRANCHE_SUBTRANS_SLRU, LW_EXCLUSIVE};

/// `SUBTRANS_XACTS_PER_PAGE` — number of subtransaction parent slots per page:
/// `BLCKSZ / sizeof(TransactionId)`. We need four bytes per xact.
const SUBTRANS_XACTS_PER_PAGE: u32 =
    BLCKSZ as u32 / core::mem::size_of::<TransactionId>() as u32;

/// Source location stamped onto raised errors (subtrans.c).
fn here() -> ErrorLocation {
    ErrorLocation::new("../src/backend/access/transam/subtrans.c", 0, "subtrans")
}

thread_local! {
    /// `int subtransaction_buffers` (the C GUC variable). 0 means auto-tune
    /// from `shared_buffers` during shmem init. Per-backend GUC state, hence
    /// thread-local; the `guc-tables` slot reads/writes it through the
    /// accessors installed by [`init_seams`].
    static subtransaction_buffers: Cell<i32> = const { Cell::new(0) };

    /// `static SlruCtlData SubTransCtlData` (subtrans.c file static). A
    /// backend installs this once via [`SUBTRANSShmemInit`]; the seams reach
    /// it here.
    static SUBTRANS_CTL: RefCell<Option<SubTransState>> = const { RefCell::new(None) };
}

#[inline]
fn subtransaction_buffers_get() -> i32 {
    subtransaction_buffers.with(Cell::get)
}

#[inline]
fn subtransaction_buffers_set(v: i32) {
    subtransaction_buffers.with(|c| c.set(v));
}

/// Owner of the SUBTRANS SLRU control data — the Rust home for subtrans.c's
/// file-static `SubTransCtlData` (`#define SubTransCtl (&SubTransCtlData)`).
#[derive(Debug)]
pub struct SubTransState {
    /// `SubTransCtl` — the SLRU control data for pg_subtrans.
    pub SubTransCtl: SlruCtlData,
}

// ---------------------------------------------------------------------------
// TransactionId page / entry arithmetic (subtrans.c static inlines / macros).
// ---------------------------------------------------------------------------

/// `TransactionIdToPage` — the SLRU page number holding `xid`. Although the
/// return type is `int64` the actual value can't currently exceed
/// `0xFFFFFFFF / SUBTRANS_XACTS_PER_PAGE`.
#[inline]
fn TransactionIdToPage(xid: TransactionId) -> i64 {
    xid as i64 / SUBTRANS_XACTS_PER_PAGE as i64
}

/// `TransactionIdToEntry(xid)` — the entry index of `xid` within its page.
#[inline]
fn TransactionIdToEntry(xid: TransactionId) -> u32 {
    xid % SUBTRANS_XACTS_PER_PAGE
}

/// `TransactionIdRetreat` — back up an XID, handling wraparound correctly.
/// `do { (dest)--; } while ((dest) < FirstNormalTransactionId)`.
#[inline]
fn TransactionIdRetreat(dest: &mut TransactionId) {
    loop {
        *dest = dest.wrapping_sub(1);
        if *dest >= FirstNormalTransactionId {
            break;
        }
    }
}

/// `TransactionIdIsNormal(xid)` (transam.h).
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdIsValid(xid)` (transam.h).
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

// ---------------------------------------------------------------------------
// Page-buffer entry read/write.
//
// The C does `ptr = (TransactionId *) page_buffer[slotno]; ptr += entryno;`
// and dereferences. The page is an array of native-endian 32-bit xids;
// pg_subtrans is never persisted across crashes so no portable encoding is
// required.
// ---------------------------------------------------------------------------

#[inline]
fn read_entry(buffer: &[u8], entryno: u32) -> TransactionId {
    let off = entryno as usize * core::mem::size_of::<TransactionId>();
    let bytes: [u8; 4] = buffer[off..off + 4].try_into().unwrap();
    TransactionId::from_ne_bytes(bytes)
}

#[inline]
fn write_entry(buffer: &mut [u8], entryno: u32, parent: TransactionId) {
    let off = entryno as usize * core::mem::size_of::<TransactionId>();
    buffer[off..off + 4].copy_from_slice(&parent.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// Set / get parent.
// ---------------------------------------------------------------------------

/// `SubTransSetParent` — record the parent of a subtransaction in the subtrans
/// log.
pub fn SubTransSetParent(
    state: &mut SubTransState,
    xid: TransactionId,
    parent: TransactionId,
) -> PgResult<()> {
    let pageno = TransactionIdToPage(xid);
    let entryno = TransactionIdToEntry(xid);

    debug_assert!(TransactionIdIsValid(parent));
    debug_assert!(TransactionIdFollows(xid, parent));

    let myproc = globals::my_proc_number::call();
    LWLockAcquire(
        SimpleLruGetBankLock(&state.SubTransCtl, pageno),
        LW_EXCLUSIVE,
        myproc,
    )?;

    let result = (|| {
        let slotno = SimpleLruReadPage(&mut state.SubTransCtl, pageno, true, xid)?;
        let current = read_entry(state.SubTransCtl.shared.page_buffer(slotno), entryno);

        // It's possible we'll try to set the parent xid multiple times but we
        // shouldn't ever be changing the xid from one valid xid to another
        // valid xid, which would corrupt the data structure.
        if current != parent {
            debug_assert!(current == InvalidTransactionId);
            write_entry(
                state.SubTransCtl.shared.page_buffer_mut(slotno),
                entryno,
                parent,
            );
            state.SubTransCtl.shared.page_dirty[slotno] = true;
        }
        Ok(())
    })();

    LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, pageno))?;
    result
}

/// `SubTransGetParent` — interrogate the parent of a transaction in the
/// subtrans log.
pub fn SubTransGetParent(
    state: &mut SubTransState,
    xid: TransactionId,
) -> PgResult<TransactionId> {
    let pageno = TransactionIdToPage(xid);
    let entryno = TransactionIdToEntry(xid);

    // Can't ask about stuff that might not be around anymore.
    debug_assert!(TransactionIdFollowsOrEquals(xid, transaction_xmin()?));

    // Bootstrap and frozen XIDs have no parent.
    if !TransactionIdIsNormal(xid) {
        return Ok(InvalidTransactionId);
    }

    // lock is acquired by SimpleLruReadPage_ReadOnly
    let slotno = SimpleLruReadPage_ReadOnly(&mut state.SubTransCtl, pageno, xid)?;
    let parent = read_entry(state.SubTransCtl.shared.page_buffer(slotno), entryno);

    LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, pageno))?;

    Ok(parent)
}

/// `SubTransGetTopmostTransaction` — returns the topmost transaction of the
/// given transaction id.
///
/// Because we cannot look back further than `TransactionXmin`, it is possible
/// that this function will lie and return an intermediate subtransaction id
/// instead of the true topmost parent id. This is OK, because in practice we
/// only care about detecting whether the topmost parent is still running or is
/// part of a current snapshot's list of still-running transactions; therefore,
/// any XID before `TransactionXmin` is as good as any other.
pub fn SubTransGetTopmostTransaction(
    state: &mut SubTransState,
    xid: TransactionId,
) -> PgResult<TransactionId> {
    let mut parentXid = xid;
    let mut previousXid = xid;

    // Can't ask about stuff that might not be around anymore.
    debug_assert!(TransactionIdFollowsOrEquals(xid, transaction_xmin()?));

    while TransactionIdIsValid(parentXid) {
        previousXid = parentXid;
        if TransactionIdPrecedes(parentXid, transaction_xmin()?) {
            break;
        }
        parentXid = SubTransGetParent(state, parentXid)?;

        // By convention the parent xid gets allocated first, so should always
        // precede the child xid. Anything else points to a corrupted data
        // structure that could lead to an infinite loop, so exit.
        if !TransactionIdPrecedes(parentXid, previousXid) {
            ereport(ERROR)
                .errmsg(format!(
                    "pg_subtrans contains invalid entry: xid {previousXid} points to parent xid {parentXid}"
                ))
                .finish(here())?;
        }
    }

    debug_assert!(TransactionIdIsValid(previousXid));

    Ok(previousXid)
}

// ---------------------------------------------------------------------------
// Shared-memory sizing and initialization.
// ---------------------------------------------------------------------------

/// `SUBTRANSShmemBuffers` — number of shared SUBTRANS buffers.
///
/// If asked to autotune, use 2MB for every 1GB of shared buffers, up to 8MB.
/// Otherwise just cap the configured amount to be between 16 and the maximum
/// allowed.
fn SUBTRANSShmemBuffers() -> i32 {
    // auto-tune based on shared buffers
    if subtransaction_buffers_get() == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    // Min(Max(16, subtransaction_buffers), SLRU_MAX_ALLOWED_BUFFERS)
    subtransaction_buffers_get()
        .max(16)
        .min(SLRU_MAX_ALLOWED_BUFFERS)
}

/// `SUBTRANSShmemSize` — shared-memory size for the SUBTRANS SLRU.
pub fn SUBTRANSShmemSize() -> Size {
    SimpleLruShmemSize(SUBTRANSShmemBuffers(), 0)
}

/// `SUBTRANSShmemInit` — initialize the SUBTRANS SLRU in shared memory, and
/// install it into the per-backend control slot.
pub fn SUBTRANSShmemInit() -> PgResult<()> {
    // If auto-tuning is requested, now is the time to do it.
    if subtransaction_buffers_get() == 0 {
        let buf = format!("{}", SUBTRANSShmemBuffers());

        guc_seams::set_config_option::call(
            "subtransaction_buffers",
            &buf,
            ::types_guc::guc::GucContext::PGC_POSTMASTER,
            ::types_guc::guc::GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;

        // We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
        // However, if the DBA explicitly set subtransaction_buffers = 0 in the
        // config file, then PGC_S_DYNAMIC_DEFAULT will fail to override that
        // and we must force the matter with PGC_S_OVERRIDE.
        if subtransaction_buffers_get() == 0 {
            // failed to apply it?
            guc_seams::set_config_option::call(
                "subtransaction_buffers",
                &buf,
                ::types_guc::guc::GucContext::PGC_POSTMASTER,
                ::types_guc::guc::GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(subtransaction_buffers_get() != 0);

    let mut ctl = SimpleLruInit(
        "subtransaction",
        SUBTRANSShmemBuffers(),
        0,
        "pg_subtrans",
        LWTRANCHE_SUBTRANS_BUFFER,
        LWTRANCHE_SUBTRANS_SLRU,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        false,
    )?;
    ctl.PagePrecedes = Some(SubTransPagePrecedes);
    SlruPagePrecedesUnitTests(&ctl, SUBTRANS_XACTS_PER_PAGE as i32);

    SUBTRANS_CTL.with(|cell| {
        *cell.borrow_mut() = Some(SubTransState { SubTransCtl: ctl });
    });
    Ok(())
}

/// GUC `check_hook` for `subtransaction_buffers` (`check_subtrans_buffers`).
pub fn check_subtrans_buffers(newval: i32) -> PgResult<bool> {
    let (ok, detail) =
        ::slru::check_slru_buffers("subtransaction_buffers", newval);
    if ok {
        Ok(true)
    } else {
        // C sets GUC_check_errdetail and returns false; this port carries the
        // detail text on Err per the GUC check-hook contract.
        match detail {
            Some(d) => Err(PgError::error(d)),
            None => Ok(false),
        }
    }
}

/// `BootStrapSUBTRANS` — this func must be called ONCE on system install. It
/// creates the initial SUBTRANS segment. (The SUBTRANS directory is assumed to
/// have been created by `initdb`, and `SUBTRANSShmemInit` must have been
/// called already.)
///
/// Note: it's not really necessary to create the initial segment now, since
/// slru.c would create it on first write anyway. But we may as well do it to
/// be sure the directory is set up correctly.
pub fn BootStrapSUBTRANS(state: &mut SubTransState) -> PgResult<()> {
    let myproc = globals::my_proc_number::call();
    LWLockAcquire(
        SimpleLruGetBankLock(&state.SubTransCtl, 0),
        LW_EXCLUSIVE,
        myproc,
    )?;

    let result = (|| {
        // Create and zero the first page of the subtrans log.
        let slotno = ZeroSUBTRANSPage(state, 0)?;

        // Make sure it's written out.
        SimpleLruWritePage(&mut state.SubTransCtl, slotno)?;
        debug_assert!(!state.SubTransCtl.shared.page_dirty[slotno]);
        Ok(())
    })();

    LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, 0))?;
    result
}

/// `ZeroSUBTRANSPage` — initialize (or reinitialize) a page of SUBTRANS to
/// zeroes. The page is not actually written, just set up in shared memory. The
/// slot number of the new page is returned. Control lock must be held at
/// entry, and will be held at exit.
fn ZeroSUBTRANSPage(state: &mut SubTransState, pageno: i64) -> PgResult<usize> {
    SimpleLruZeroPage(&mut state.SubTransCtl, pageno)
}

/// `StartupSUBTRANS` — this must be called ONCE during postmaster or
/// standalone-backend startup, after StartupXLOG has initialized
/// `TransamVariables->nextXid`.
///
/// `oldestActiveXID` is the oldest XID of any prepared transaction, or
/// `nextXid` if there are none.
pub fn StartupSUBTRANS(
    state: &mut SubTransState,
    oldestActiveXID: TransactionId,
) -> PgResult<()> {
    // Since we don't expect pg_subtrans to be valid across crashes, we
    // initialize the currently-active page(s) to zeroes during startup.
    // Whenever we advance into a new page, ExtendSUBTRANS will likewise zero
    // the new page without regard to whatever was previously on disk.
    let mut startPage = TransactionIdToPage(oldestActiveXID);
    let nextXid = varsup_seams::read_next_full_transaction_id::call();
    let endPage = TransactionIdToPage(nextXid.xid());

    let myproc = globals::my_proc_number::call();

    // Mirror C's prevlock/lock release-reacquire: we identify the held bank
    // lock by its page number (any page in the bank yields the same lock).
    let mut prevpage: Option<i64> = None;
    let mut held: Option<i64> = None;

    let result = (|| {
        loop {
            // Determine whether the bank lock for startPage differs from the
            // one we already hold. Two pages share a lock iff
            // SimpleLruGetBankLock returns the same lock; compare by pointer.
            let same_as_prev = match prevpage {
                Some(pp) => std::ptr::eq(
                    SimpleLruGetBankLock(&state.SubTransCtl, pp),
                    SimpleLruGetBankLock(&state.SubTransCtl, startPage),
                ),
                None => false,
            };
            if !same_as_prev {
                if let Some(hp) = held.take() {
                    LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, hp))?;
                }
                LWLockAcquire(
                    SimpleLruGetBankLock(&state.SubTransCtl, startPage),
                    LW_EXCLUSIVE,
                    myproc,
                )?;
                held = Some(startPage);
                prevpage = Some(startPage);
            }

            ZeroSUBTRANSPage(state, startPage)?;
            if startPage == endPage {
                break;
            }

            startPage += 1;
            // must account for wraparound
            if startPage > TransactionIdToPage(MaxTransactionId) {
                startPage = 0;
            }
        }
        Ok(())
    })();

    if let Some(hp) = held.take() {
        LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, hp))?;
    }
    result
}

/// `CheckPointSUBTRANS` — perform a checkpoint, either during shutdown or
/// on-the-fly.
pub fn CheckPointSUBTRANS(state: &mut SubTransState) -> PgResult<()> {
    // Write dirty SUBTRANS pages to disk.
    //
    // This is not actually necessary from a correctness point of view. We do
    // it merely to improve the odds that writing of dirty pages is done by the
    // checkpoint process and not by backends.
    SimpleLruWriteAll(&mut state.SubTransCtl, true)
}

/// `ExtendSUBTRANS` — make sure that SUBTRANS has room for a newly-allocated
/// XID.
///
/// NB: this is called while holding XidGenLock. We want it to be very fast most
/// of the time; even when it's not so fast, no actual I/O need happen unless
/// we're forced to write out a dirty subtrans page to make room in shared
/// memory.
pub fn ExtendSUBTRANS(state: &mut SubTransState, newestXact: TransactionId) -> PgResult<()> {
    // No work except at first XID of a page. But beware: just after
    // wraparound, the first XID of page zero is FirstNormalTransactionId.
    if TransactionIdToEntry(newestXact) != 0 && newestXact != FirstNormalTransactionId {
        return Ok(());
    }

    let pageno = TransactionIdToPage(newestXact);

    let myproc = globals::my_proc_number::call();
    LWLockAcquire(
        SimpleLruGetBankLock(&state.SubTransCtl, pageno),
        LW_EXCLUSIVE,
        myproc,
    )?;

    // Zero the page.
    let result = ZeroSUBTRANSPage(state, pageno).map(|_| ());

    LWLockRelease(SimpleLruGetBankLock(&state.SubTransCtl, pageno))?;
    result
}

/// `TruncateSUBTRANS` — remove all SUBTRANS segments before the one holding the
/// passed transaction id.
///
/// `oldestXact` is the oldest `TransactionXmin` of any running transaction.
/// This is called only during checkpoint.
pub fn TruncateSUBTRANS(
    state: &mut SubTransState,
    mut oldestXact: TransactionId,
) -> PgResult<()> {
    // The cutoff point is the start of the segment containing oldestXact. We
    // pass the *page* containing oldestXact to SimpleLruTruncate. We step back
    // one transaction to avoid passing a cutoff page that hasn't been created
    // yet in the rare case that oldestXact would be the first item on a page
    // and oldestXact == next XID. In that case, if we didn't subtract one,
    // we'd trigger SimpleLruTruncate's wraparound detection.
    TransactionIdRetreat(&mut oldestXact);
    let cutoffPage = TransactionIdToPage(oldestXact);

    SimpleLruTruncate(&mut state.SubTransCtl, cutoffPage)
}

/// `SubTransPagePrecedes` — decide whether a SUBTRANS page number is "older"
/// for truncation purposes. Analogous to `CLOGPagePrecedes()`.
fn SubTransPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(SUBTRANS_XACTS_PER_PAGE - 1))
}

// ---------------------------------------------------------------------------
// `TransactionXmin` (snapmgr backend-global) accessor.
// ---------------------------------------------------------------------------

/// `TransactionXmin` — the oldest xid this backend may still need to interrogate
/// (snapmgr.c backend-global), reached through its owner's seam.
#[inline]
fn transaction_xmin() -> PgResult<TransactionId> {
    snapmgr_pc_seams::transaction_xmin::call()
}

// ---------------------------------------------------------------------------
// Seam installation. The inward seams take only an `xid`/`parent` (the C
// signatures); they reach the per-backend control slot here, mirroring the
// C file-static `SubTransCtlData`.
// ---------------------------------------------------------------------------

/// Run `f` with the per-backend SUBTRANS control state, panicking loudly if
/// `SUBTRANSShmemInit` has not installed it (the C uninitialized-static call).
fn with_ctl<R>(f: impl FnOnce(&mut SubTransState) -> R) -> R {
    SUBTRANS_CTL.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let state = borrow.as_mut().expect(
            "SUBTRANS control state not initialized (SUBTRANSShmemInit must run first)",
        );
        f(state)
    })
}

/// Install this crate's inward seams and GUC slots.
pub fn init_seams() {
    use subtrans_seams as seams;
    use ::guc_tables::{hooks, vars, GucHookExtra, GucVarAccessors};
    use ::types_guc::guc::GucSource;

    // `BootStrapSUBTRANS()` (subtrans.c) — called once by `BootStrapXLOG`
    // (xlog.c) at initdb to create + zero the first subtrans page.
    transam_xlog_seams::boot_strap_sub_trans::set(|| with_ctl(BootStrapSUBTRANS));

    seams::sub_trans_get_parent::set(|xid| with_ctl(|st| SubTransGetParent(st, xid)));
    seams::sub_trans_set_parent::set(|xid, parent| with_ctl(|st| SubTransSetParent(st, xid, parent)));
    seams::sub_trans_get_topmost_transaction::set(|xid| {
        with_ctl(|st| SubTransGetTopmostTransaction(st, xid))
    });
    seams::sub_trans_shmem_size::set(|| Ok(SUBTRANSShmemSize()));
    seams::sub_trans_shmem_init::set(SUBTRANSShmemInit);
    seams::extend_subtrans::set(|newest_xact| with_ctl(|st| ExtendSUBTRANS(st, newest_xact)));

    // WAL-startup entry point called by `StartupXLOG` (xlog.c) on the clean
    // DB_SHUTDOWNED / end-of-recovery path.
    seams::startup_subtrans::set(|oldest_active_xid| {
        with_ctl(|st| StartupSUBTRANS(st, oldest_active_xid))
    });

    // The C GUC `int subtransaction_buffers` lives in the thread_local here;
    // the GUC machinery reaches it through these accessors, and the check
    // hook validates new values via check_slru_buffers (subtrans.c
    // check_subtrans_buffers).
    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        check_subtrans_buffers(*newval)
    }
    hooks::check_subtrans_buffers.install(check_hook);
    vars::subtransaction_buffers.install(GucVarAccessors {
        get: subtransaction_buffers_get,
        set: subtransaction_buffers_set,
    });
}

#[cfg(test)]
mod tests;
