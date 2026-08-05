#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::OnceLock;

use elog::elog;
use init_small::globals;
use lwlock::LW_EXCLUSIVE;
use procarray::TransactionXmin;
use slru::{
    check_slru_buffers, LwGuard, SimpleLruAutotuneBuffers, SimpleLruGetBankLock, SimpleLruInit,
    SimpleLruReadPage, SimpleLruReadPage_ReadOnly, SimpleLruShmemSize, SimpleLruTruncate,
    SimpleLruWriteAll, SimpleLruWritePage, SimpleLruZeroPage, SlruCtlData,
    SlruPagePrecedesUnitTests, SLRU_MAX_ALLOWED_BUFFERS,
};
use types_core::{
    FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId, Size, TransactionId,
    TransactionIdEquals, TransactionIdFollows, TransactionIdFollowsOrEquals,
    TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes, BLCKSZ,
};
use types_error::{PgResult, ERROR};
use types_guc::{GucContext::PGC_POSTMASTER, GucSource};
use types_storage::storage::{LWTRANCHE_SUBTRANS_BUFFER, LWTRANCHE_SUBTRANS_SLRU};
use types_storage::sync::SyncRequestHandler;

pub const SUBTRANS_XACTS_PER_PAGE: u32 = (BLCKSZ / core::mem::size_of::<TransactionId>()) as u32;

static SUB_TRANS_CTL: OnceLock<SlruCtlData> = OnceLock::new();

fn SubTransCtl() -> &'static SlruCtlData {
    SUB_TRANS_CTL
        .get()
        .unwrap_or_else(|| panic!("SUBTRANS accessed before SUBTRANSShmemInit (SubTransCtl is NULL)"))
}

#[inline]
fn TransactionIdToPage(xid: TransactionId) -> i64 {
    xid as i64 / SUBTRANS_XACTS_PER_PAGE as i64
}

#[inline]
fn TransactionIdToEntry(xid: TransactionId) -> usize {
    (xid % SUBTRANS_XACTS_PER_PAGE) as usize
}

fn entry_bytes(entryno: usize) -> core::ops::Range<usize> {
    let off = entryno * core::mem::size_of::<TransactionId>();
    off..off + core::mem::size_of::<TransactionId>()
}

pub fn SubTransSetParent(xid: TransactionId, parent: TransactionId) -> PgResult<()> {
    let ctl = SubTransCtl();
    let pageno = TransactionIdToPage(xid);
    let entryno = TransactionIdToEntry(xid);

    debug_assert!(TransactionIdIsValid(parent));
    debug_assert!(TransactionIdFollows(xid, parent));

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    let slotno = SimpleLruReadPage(ctl, pageno, true, xid, &mut bank)?;
    let cur = TransactionId::from_ne_bytes(
        ctl.page_buffer(slotno, &bank)[entry_bytes(entryno)]
            .try_into()
            .expect("4-byte subtrans entry"),
    );

    // Re-set of the same parent is legal; valid -> different-valid is corruption.
    if cur != parent {
        debug_assert!(cur == InvalidTransactionId);
        ctl.page_buffer_mut(slotno, &mut bank)[entry_bytes(entryno)]
            .copy_from_slice(&parent.to_ne_bytes());
        ctl.mark_page_dirty(slotno, &bank);
    }

    bank.release()
}

pub fn SubTransGetParent(xid: TransactionId) -> PgResult<TransactionId> {
    let ctl = SubTransCtl();
    let pageno = TransactionIdToPage(xid);
    let entryno = TransactionIdToEntry(xid);

    debug_assert!(TransactionIdFollowsOrEquals(xid, TransactionXmin()));

    // Bootstrap and frozen XIDs have no parent.
    if !TransactionIdIsNormal(xid) {
        return Ok(InvalidTransactionId);
    }

    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, pageno, xid)?;

    let parent = TransactionId::from_ne_bytes(
        ctl.page_buffer(slotno, &bank)[entry_bytes(entryno)]
            .try_into()
            .expect("4-byte subtrans entry"),
    );

    bank.release()?;

    Ok(parent)
}

// May return an intermediate subtransaction instead of the true topmost
// parent when the chain passes TransactionXmin; any XID older than
// TransactionXmin is as good as any other for the callers' purposes.
pub fn SubTransGetTopmostTransaction(xid: TransactionId) -> PgResult<TransactionId> {
    let mut parentXid = xid;
    let mut previousXid = xid;

    debug_assert!(TransactionIdFollowsOrEquals(xid, TransactionXmin()));

    while TransactionIdIsValid(parentXid) {
        previousXid = parentXid;
        if TransactionIdPrecedes(parentXid, TransactionXmin()) {
            break;
        }
        parentXid = SubTransGetParent(parentXid)?;

        // Parents are allocated before children; anything else is a
        // corrupted structure that could loop forever.
        if !TransactionIdPrecedes(parentXid, previousXid) {
            elog(
                ERROR,
                format!(
                    "pg_subtrans contains invalid entry: xid {previousXid} points to parent xid {parentXid}"
                ),
            )?;
        }
    }

    debug_assert!(TransactionIdIsValid(previousXid));

    Ok(previousXid)
}

fn SUBTRANSShmemBuffers() -> i32 {
    if globals::subtransaction_buffers() == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    globals::subtransaction_buffers()
        .max(16)
        .min(SLRU_MAX_ALLOWED_BUFFERS)
}

pub fn SUBTRANSShmemSize() -> Size {
    SimpleLruShmemSize(SUBTRANSShmemBuffers(), 0)
}

pub fn SUBTRANSShmemInit() -> PgResult<()> {
    if globals::subtransaction_buffers() == 0 {
        let buf = SUBTRANSShmemBuffers().to_string();
        guc::SetConfigOption(
            "subtransaction_buffers",
            Some(&buf),
            PGC_POSTMASTER,
            GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;

        // An explicit subtransaction_buffers=0 in the config file outranks
        // PGC_S_DYNAMIC_DEFAULT; force the matter with PGC_S_OVERRIDE.
        if globals::subtransaction_buffers() == 0 {
            guc::SetConfigOption(
                "subtransaction_buffers",
                Some(&buf),
                PGC_POSTMASTER,
                GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(globals::subtransaction_buffers() != 0);

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

    if SUB_TRANS_CTL.set(ctl).is_err() {
        panic!("SUBTRANSShmemInit called twice");
    }
    Ok(())
}

pub fn check_subtrans_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("subtransaction_buffers", newval)
}

/// Crash-cycle reset in place (notes/crash-restart-design.md).
pub fn SUBTRANSShmemResetAfterCrash() {
    slru::SimpleLruResetAfterCrash(SubTransCtl());
}

pub fn BootStrapSUBTRANS() -> PgResult<()> {
    let ctl = SubTransCtl();
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, 0), LW_EXCLUSIVE)?;

    let slotno = ZeroSUBTRANSPage(0, &mut bank)?;

    SimpleLruWritePage(ctl, slotno, &mut bank)?;
    debug_assert!(!ctl.page_dirty(slotno, &bank));

    bank.release()
}

fn ZeroSUBTRANSPage(pageno: i64, bank: &mut LwGuard) -> PgResult<usize> {
    SimpleLruZeroPage(SubTransCtl(), pageno, bank)
}

// pg_subtrans is not preserved across crashes: zero every currently-active
// page (oldest prepared/active xid through nextXid) at startup.
pub fn StartupSUBTRANS(oldestActiveXID: TransactionId) -> PgResult<()> {
    let ctl = SubTransCtl();

    let mut startPage = TransactionIdToPage(oldestActiveXID);
    let endPage = TransactionIdToPage(varsup_seams::read_next_transaction_id::call()?);

    let mut bank: Option<LwGuard> = None;
    loop {
        let lock = SimpleLruGetBankLock(ctl, startPage);
        let covered = bank.as_ref().is_some_and(|b| b.covers(lock));
        if !covered {
            if let Some(prev) = bank.take() {
                prev.release()?;
            }
            bank = Some(LwGuard::acquire(lock, LW_EXCLUSIVE)?);
        }

        ZeroSUBTRANSPage(startPage, bank.as_mut().expect("bank lock held"))?;
        if startPage == endPage {
            break;
        }

        startPage += 1;
        if startPage > TransactionIdToPage(MaxTransactionId) {
            startPage = 0;
        }
    }

    bank.expect("bank lock held").release()
}

pub fn CheckPointSUBTRANS() -> PgResult<()> {
    // Correctness-optional: just biases dirty-page writes to the checkpointer.
    SimpleLruWriteAll(SubTransCtl(), true)
}

pub fn ExtendSUBTRANS(newestXact: TransactionId) -> PgResult<()> {
    // Work only at the first XID of a page; just after wraparound the first
    // XID of page zero is FirstNormalTransactionId.
    if TransactionIdToEntry(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return Ok(());
    }

    let ctl = SubTransCtl();
    let pageno = TransactionIdToPage(newestXact);
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    ZeroSUBTRANSPage(pageno, &mut bank)?;

    bank.release()
}

pub fn TruncateSUBTRANS(oldestXact: TransactionId) -> PgResult<()> {
    // Step back one xid: if oldestXact is the first item of a not-yet-created
    // page (oldestXact == next XID), the cutoff would trip the wraparound
    // backstop in SimpleLruTruncate.
    let mut oldestXact = oldestXact;
    TransactionIdRetreat(&mut oldestXact);
    let cutoffPage = TransactionIdToPage(oldestXact);

    SimpleLruTruncate(SubTransCtl(), cutoffPage)
}

fn TransactionIdRetreat(xid: &mut TransactionId) {
    *xid = xid.wrapping_sub(1);
    while *xid < FirstNormalTransactionId {
        *xid = xid.wrapping_sub(1);
    }
}

fn SubTransPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(SUBTRANS_XACTS_PER_PAGE - 1))
}

pub fn init_seams() {
    subtrans_seams::sub_trans_get_topmost_transaction::set(SubTransGetTopmostTransaction);
    subtrans_seams::startup_subtrans::set(StartupSUBTRANS);
    subtrans_seams::sub_trans_set_parent::set(SubTransSetParent);
    subtrans_seams::extend_subtrans::set(ExtendSUBTRANS);
    subtrans_seams::check_point_subtrans::set(CheckPointSUBTRANS);
    subtrans_seams::truncate_subtrans::set(TruncateSUBTRANS);

    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_subtrans_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    guc_tables::hooks::check_subtrans_buffers.install(check_hook);
}

#[cfg(test)]
mod tests;
