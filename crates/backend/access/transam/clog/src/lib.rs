#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::OnceLock;

use elog::ereport;
use init_small::globals;
use lmgr_proc::{GetPGProcByNumber, MyProc, ProcGlobal};
use lwlock::LW_EXCLUSIVE;
use slru::{
    check_slru_buffers, LwGuard, SimpleLruAutotuneBuffers, SimpleLruGetBankLock, SimpleLruInit,
    SimpleLruReadPage, SimpleLruReadPage_ReadOnly, SimpleLruShmemSize, SimpleLruTruncate,
    SimpleLruWriteAll, SimpleLruWritePage, SimpleLruZeroPage, SlruCtlData, SlruPagePrecedesUnitTests,
    SlruPath, SlruScanDirCbReportPresence, SlruScanDirectory, SlruSyncFileTag,
    SLRU_MAX_ALLOWED_BUFFERS,
};
use types_core::xact::{
    InvalidXLogRecPtr, XidStatus, TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED,
    TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_SUB_COMMITTED,
};
use types_core::{
    FirstNormalTransactionId, InvalidTransactionId, MaxTransactionId, Oid, Size, TransactionId,
    TransactionIdEquals, TransactionIdIsValid, TransactionIdPrecedes, XLogRecPtr, BLCKSZ,
    INVALID_PROC_NUMBER,
};
use types_error::{ErrorLocation, PgResult, PANIC};
use types_guc::{GucContext::PGC_POSTMASTER, GucSource};
use types_storage::storage::{
    LWTRANCHE_XACT_BUFFER, LWTRANCHE_XACT_SLRU, PGPROC, PGPROC_MAX_CACHED_SUBXIDS,
};
use types_storage::sync::{FileTag, SyncRequestHandler};
use xlogreader_seams::XLogReaderState;

pub const CLOG_BITS_PER_XACT: u32 = 2;
pub const CLOG_XACTS_PER_BYTE: u32 = 4;
pub const CLOG_XACTS_PER_PAGE: u32 = BLCKSZ as u32 * CLOG_XACTS_PER_BYTE;
pub const CLOG_XACT_BITMASK: u32 = (1 << CLOG_BITS_PER_XACT) - 1;

const CLOG_XACTS_PER_LSN_GROUP: u32 = 32;
const CLOG_LSNS_PER_PAGE: i32 = (CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP) as i32;

const THRESHOLD_SUBTRANS_CLOG_OPT: usize = 5;

pub const RM_CLOG_ID: u8 = 3;
pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;
const XLR_INFO_MASK: u8 = 0x0F;

// PG_WAIT_IPC | XactGroupUpdate's index in wait_event_names.txt's IPC section.
const WAIT_EVENT_XACT_GROUP_UPDATE: u32 = 0x0800_0000 | 56;

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

static XACT_CTL: OnceLock<SlruCtlData> = OnceLock::new();

fn XactCtl() -> &'static SlruCtlData {
    XACT_CTL
        .get()
        .unwrap_or_else(|| panic!("CLOG accessed before CLOGShmemInit (XactCtl is NULL)"))
}

#[inline]
fn TransactionIdToPage(xid: TransactionId) -> i64 {
    xid as i64 / CLOG_XACTS_PER_PAGE as i64
}

#[inline]
fn TransactionIdToPgIndex(xid: TransactionId) -> u32 {
    xid % CLOG_XACTS_PER_PAGE
}

#[inline]
fn TransactionIdToByte(xid: TransactionId) -> usize {
    (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE) as usize
}

#[inline]
fn TransactionIdToBIndex(xid: TransactionId) -> u32 {
    xid % CLOG_XACTS_PER_BYTE
}

#[inline]
fn GetLSNIndex(slotno: usize, xid: TransactionId) -> usize {
    slotno * CLOG_LSNS_PER_PAGE as usize
        + ((xid % CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_LSN_GROUP) as usize
}

pub fn TransactionIdSetTreeStatus(
    xid: TransactionId,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    let nsubxids = subxids.len();
    let pageno = TransactionIdToPage(xid);

    debug_assert!(status == TRANSACTION_STATUS_COMMITTED || status == TRANSACTION_STATUS_ABORTED);

    let mut i = 0;
    while i < nsubxids {
        if TransactionIdToPage(subxids[i]) != pageno {
            break;
        }
        i += 1;
    }

    if i == nsubxids {
        TransactionIdSetPageStatus(xid, subxids, status, lsn, pageno, true)
    } else {
        let nsubxids_on_first_page = i;

        // Multi-page commit: mark off-page subxids subcommitted first so the
        // top-level commit stays atomic for concurrent checkers.
        if status == TRANSACTION_STATUS_COMMITTED {
            set_status_by_pages(
                &subxids[nsubxids_on_first_page..],
                TRANSACTION_STATUS_SUB_COMMITTED,
                lsn,
            )?;
        }

        let pageno = TransactionIdToPage(xid);
        TransactionIdSetPageStatus(
            xid,
            &subxids[..nsubxids_on_first_page],
            status,
            lsn,
            pageno,
            false,
        )?;

        set_status_by_pages(&subxids[nsubxids_on_first_page..], status, lsn)
    }
}

fn set_status_by_pages(
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    let nsubxids = subxids.len();
    debug_assert!(nsubxids > 0);

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
            &subxids[offset..offset + num_on_page],
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

fn TransactionIdSetPageStatus(
    xid: TransactionId,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
    all_xact_same_page: bool,
) -> PgResult<()> {
    const {
        assert!(
            THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS,
            "group clog threshold less than PGPROC cached subxids"
        )
    };

    let ctl = XactCtl();
    let nsubxids = subxids.len();
    let lock = SimpleLruGetBankLock(ctl, pageno);

    // Under bank-lock contention a leader batches many backends' status
    // updates per lock acquisition; safe only when MyProc carries exactly
    // this xid + subxid set, efficient only for small same-page trees.
    if all_xact_same_page
        && nsubxids <= THRESHOLD_SUBTRANS_CLOG_OPT
        && my_proc_matches(xid, subxids)
    {
        if let Some(mut bank) = LwGuard::conditional_acquire(lock, LW_EXCLUSIVE)? {
            let res =
                TransactionIdSetPageStatusInternal(xid, subxids, status, lsn, pageno, &mut bank);
            bank.release()?;
            return res;
        } else if TransactionGroupUpdateXidStatus(xid, status, lsn, pageno)? {
            return Ok(());
        }
    }

    let mut bank = LwGuard::acquire(lock, LW_EXCLUSIVE)?;
    let res = TransactionIdSetPageStatusInternal(xid, subxids, status, lsn, pageno, &mut bank);
    bank.release()?;
    res
}

fn my_proc_matches(xid: TransactionId, subxids: &[TransactionId]) -> bool {
    let Some(procno) = MyProc() else {
        return false;
    };
    let proc = GetPGProcByNumber(procno);
    if !TransactionIdEquals(xid, proc.xid.read()) {
        return false;
    }
    if subxids.len() != proc.subxidStatus.get().count as usize {
        return false;
    }
    subxids.is_empty() || subxids == &proc.subxids.get().xids[..subxids.len()]
}

fn TransactionIdSetPageStatusInternal(
    xid: TransactionId,
    subxids: &[TransactionId],
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
    bank: &mut LwGuard,
) -> PgResult<()> {
    let ctl = XactCtl();

    debug_assert!(
        status == TRANSACTION_STATUS_COMMITTED
            || status == TRANSACTION_STATUS_ABORTED
            || (status == TRANSACTION_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid))
    );
    debug_assert!(bank.held_in_mode(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE));

    // Async commit (valid lsn) must wait out an active page write: the bits
    // must not reach disk before that commit record's WAL flush. Sync/abort
    // may scribble on a write-busy page.
    let slotno = SimpleLruReadPage(ctl, pageno, lsn == InvalidXLogRecPtr, xid, bank)?;

    if TransactionIdIsValid(xid) {
        // Subcommit the subxids first so a partial page flush can't expose a
        // committed top-level with in-progress children.
        if status == TRANSACTION_STATUS_COMMITTED {
            for &sub in subxids {
                debug_assert_eq!(ctl.page_number(slotno, bank), TransactionIdToPage(sub));
                TransactionIdSetStatusBit(sub, TRANSACTION_STATUS_SUB_COMMITTED, lsn, slotno, bank);
            }
        }

        TransactionIdSetStatusBit(xid, status, lsn, slotno, bank);
    }

    for &sub in subxids {
        debug_assert_eq!(ctl.page_number(slotno, bank), TransactionIdToPage(sub));
        TransactionIdSetStatusBit(sub, status, lsn, slotno, bank);
    }

    ctl.mark_page_dirty(slotno, bank);
    Ok(())
}

fn TransactionGroupUpdateXidStatus(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: i64,
) -> PgResult<bool> {
    let ctl = XactCtl();
    let procglobal = ProcGlobal();
    let my_procno = MyProc().expect("clog group update without MyProc");
    let proc: &PGPROC = GetPGProcByNumber(my_procno);

    debug_assert!(TransactionIdIsValid(xid));

    proc.clogGroupMember.store(true, Relaxed);
    proc.clogGroupMemberXid.store(xid, Relaxed);
    proc.clogGroupMemberXidStatus.store(status, Relaxed);
    proc.clogGroupMemberPage.store(pageno, Relaxed);
    proc.clogGroupMemberLsn.store(lsn, Relaxed);

    let mut nextidx = procglobal.clogGroupFirst.value.load(Relaxed);

    loop {
        // Joining a group whose head waits on a different page is refused
        // (caller updates pg_xact the normal way); a leader that has already
        // moved on to another page is tolerated by the bank-lock switch below.
        if nextidx != INVALID_PROC_NUMBER as u32
            && GetPGProcByNumber(nextidx as i32)
                .clogGroupMemberPage
                .load(Relaxed)
                != pageno
        {
            proc.clogGroupMember.store(false, Relaxed);
            proc.clogGroupNext
                .value
                .store(INVALID_PROC_NUMBER as u32, Relaxed);
            return Ok(false);
        }

        proc.clogGroupNext.value.store(nextidx, Relaxed);

        match procglobal.clogGroupFirst.value.compare_exchange(
            nextidx,
            my_procno as u32,
            AcqRel,
            Acquire,
        ) {
            Ok(_) => break,
            Err(cur) => nextidx = cur,
        }
    }

    if nextidx != INVALID_PROC_NUMBER as u32 {
        let mut extra_waits = 0;

        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_XACT_GROUP_UPDATE);
        loop {
            // PGSemaphoreLock acts as a read barrier.
            lmgr_proc_seams::pg_semaphore_lock::call(my_procno);
            if !proc.clogGroupMember.load(Acquire) {
                break;
            }
            extra_waits += 1;
        }
        waitevent_seams::pgstat_report_wait_end::call();

        debug_assert_eq!(
            proc.clogGroupNext.value.load(Relaxed),
            INVALID_PROC_NUMBER as u32
        );

        while extra_waits > 0 {
            lmgr_proc_seams::pg_semaphore_unlock::call(my_procno);
            extra_waits -= 1;
        }
        return Ok(true);
    }

    // We are the leader.
    let mut prevpageno = proc.clogGroupMemberPage.load(Relaxed);
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, prevpageno), LW_EXCLUSIVE)?;

    // Detach the whole list at once (popping one at a time risks ABA).
    let mut nextidx = procglobal
        .clogGroupFirst
        .value
        .swap(INVALID_PROC_NUMBER as u32, AcqRel);

    let wakeidx = nextidx;

    while nextidx != INVALID_PROC_NUMBER as u32 {
        let nextproc = GetPGProcByNumber(nextidx as i32);
        let thispageno = nextproc.clogGroupMemberPage.load(Relaxed);

        if thispageno != prevpageno {
            let lock = SimpleLruGetBankLock(ctl, thispageno);
            if !bank.covers(lock) {
                bank.release()?;
                bank = LwGuard::acquire(lock, LW_EXCLUSIVE)?;
            }
            prevpageno = thispageno;
        }

        let subxid_count = nextproc.subxidStatus.get().count as usize;
        debug_assert!(subxid_count <= THRESHOLD_SUBTRANS_CLOG_OPT);
        let subxids = nextproc.subxids.get();

        TransactionIdSetPageStatusInternal(
            nextproc.clogGroupMemberXid.load(Relaxed),
            &subxids.xids[..subxid_count],
            nextproc.clogGroupMemberXidStatus.load(Relaxed),
            nextproc.clogGroupMemberLsn.load(Relaxed),
            thispageno,
            &mut bank,
        )?;

        nextidx = nextproc.clogGroupNext.value.load(Relaxed);
    }

    bank.release()?;

    // Wake everybody outside the lock to keep hold times minimal.
    let mut wakeidx = wakeidx;
    while wakeidx != INVALID_PROC_NUMBER as u32 {
        let this = wakeidx;
        let wakeproc = GetPGProcByNumber(this as i32);
        wakeidx = wakeproc.clogGroupNext.value.load(Relaxed);
        wakeproc
            .clogGroupNext
            .value
            .store(INVALID_PROC_NUMBER as u32, Relaxed);

        // pg_write_barrier: prior writes visible before the follower runs.
        wakeproc.clogGroupMember.store(false, Release);

        if this != my_procno as u32 {
            lmgr_proc_seams::pg_semaphore_unlock::call(this as i32);
        }
    }

    Ok(true)
}

fn TransactionIdSetStatusBit(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    slotno: usize,
    bank: &mut LwGuard,
) {
    let ctl = XactCtl();
    let byteno = TransactionIdToByte(xid);
    let bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;

    debug_assert_eq!(ctl.page_number(slotno, bank), TransactionIdToPage(xid));
    debug_assert!(bank.held_in_mode(
        SimpleLruGetBankLock(ctl, ctl.page_number(slotno, bank)),
        LW_EXCLUSIVE
    ));

    let curval =
        ((ctl.page_buffer(slotno, bank)[byteno] as u32 >> bshift) & CLOG_XACT_BITMASK) as XidStatus;

    // Redo may replay subcommit over an already-committed xid; no-op it so
    // the assert below stays restrictive.
    if xlogutils_seams::in_recovery::call()
        && status == TRANSACTION_STATUS_SUB_COMMITTED
        && curval == TRANSACTION_STATUS_COMMITTED
    {
        return;
    }

    debug_assert!(
        curval == 0
            || (curval == TRANSACTION_STATUS_SUB_COMMITTED
                && status != TRANSACTION_STATUS_IN_PROGRESS)
            || curval == status
    );

    let byteptr = &mut ctl.page_buffer_mut(slotno, bank)[byteno];
    let mut byteval = *byteptr as u32;
    byteval &= !(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);
    byteval |= (status as u32) << bshift;
    *byteptr = byteval as u8;

    // lsn is invalid during recovery, so no special recovery guard needed.
    if lsn != InvalidXLogRecPtr {
        let lsnindex = GetLSNIndex(slotno, xid);
        if ctl.group_lsn(lsnindex, bank) < lsn {
            ctl.set_group_lsn(lsnindex, lsn, bank);
        }
    }
}

pub fn TransactionIdGetStatus(xid: TransactionId) -> PgResult<(XidStatus, XLogRecPtr)> {
    let ctl = XactCtl();
    let pageno = TransactionIdToPage(xid);
    let byteno = TransactionIdToByte(xid);
    let bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;

    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, pageno, xid)?;

    let status =
        ((ctl.page_buffer(slotno, &bank)[byteno] as u32 >> bshift) & CLOG_XACT_BITMASK) as XidStatus;

    let lsnindex = GetLSNIndex(slotno, xid);
    let lsn = ctl.group_lsn(lsnindex, &bank);

    bank.release()?;

    Ok((status, lsn))
}

fn CLOGShmemBuffers() -> i32 {
    if globals::transaction_buffers() == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    globals::transaction_buffers()
        .max(16)
        .min(clog_max_allowed_buffers())
}

pub fn CLOGShmemSize() -> Size {
    SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE)
}

pub fn CLOGShmemInit() -> PgResult<()> {
    if globals::transaction_buffers() == 0 {
        let buf = CLOGShmemBuffers().to_string();
        guc::SetConfigOption(
            "transaction_buffers",
            Some(&buf),
            PGC_POSTMASTER,
            GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;

        // An explicit transaction_buffers=0 in the config file outranks
        // PGC_S_DYNAMIC_DEFAULT; force the matter with PGC_S_OVERRIDE.
        if globals::transaction_buffers() == 0 {
            guc::SetConfigOption(
                "transaction_buffers",
                Some(&buf),
                PGC_POSTMASTER,
                GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(globals::transaction_buffers() != 0);

    let mut ctl = SimpleLruInit(
        "transaction",
        CLOGShmemBuffers(),
        CLOG_LSNS_PER_PAGE,
        "pg_xact",
        LWTRANCHE_XACT_BUFFER,
        LWTRANCHE_XACT_SLRU,
        SyncRequestHandler::SYNC_HANDLER_CLOG,
        false,
    )?;
    ctl.PagePrecedes = Some(CLOGPagePrecedes);
    SlruPagePrecedesUnitTests(&ctl, CLOG_XACTS_PER_PAGE as i32);

    if XACT_CTL.set(ctl).is_err() {
        panic!("CLOGShmemInit called twice");
    }
    Ok(())
}

pub fn check_transaction_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("transaction_buffers", newval)
}

/// Crash-cycle reset in place (notes/crash-restart-design.md); StartupCLOG
/// re-seeds as after C's shmem re-create.
pub fn CLOGShmemResetAfterCrash() {
    slru::SimpleLruResetAfterCrash(XactCtl());
}

pub fn BootStrapCLOG() -> PgResult<()> {
    let ctl = XactCtl();
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, 0), LW_EXCLUSIVE)?;

    let slotno = ZeroCLOGPage(0, false, &mut bank)?;

    SimpleLruWritePage(ctl, slotno, &mut bank)?;
    debug_assert!(!ctl.page_dirty(slotno, &bank));

    bank.release()
}

fn ZeroCLOGPage(pageno: i64, writeXlog: bool, bank: &mut LwGuard) -> PgResult<usize> {
    let slotno = SimpleLruZeroPage(XactCtl(), pageno, bank)?;

    if writeXlog {
        WriteZeroPageXlogRec(pageno)?;
    }

    Ok(slotno)
}

pub fn StartupCLOG() -> PgResult<()> {
    let xid = varsup_seams::read_next_transaction_id::call()?;
    let pageno = TransactionIdToPage(xid);

    XactCtl().set_latest_page_number(pageno);
    Ok(())
}

pub fn TrimCLOG() -> PgResult<()> {
    let ctl = XactCtl();
    let xid = varsup_seams::read_next_transaction_id::call()?;
    let pageno = TransactionIdToPage(xid);

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    // Replay may settle on a nextXid below XIDs the previous lifecycle
    // marked (subxact commit writes clog without WAL); zero the tail of the
    // current page so stale bits can't survive.
    if TransactionIdToPgIndex(xid) != 0 {
        let byteno = TransactionIdToByte(xid);
        let bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;

        let slotno = SimpleLruReadPage(ctl, pageno, false, xid, &mut bank)?;
        let buffer = ctl.page_buffer_mut(slotno, &mut bank);
        buffer[byteno] &= ((1u32 << bshift) - 1) as u8;
        buffer[byteno + 1..BLCKSZ].fill(0);
        ctl.mark_page_dirty(slotno, &bank);
    }

    bank.release()
}

pub fn CheckPointCLOG() -> PgResult<()> {
    SimpleLruWriteAll(XactCtl(), true)
}

pub fn ExtendCLOG(newestXact: TransactionId) -> PgResult<()> {
    // Work only at the first XID of a page; just after wraparound the first
    // XID of page zero is FirstNormalTransactionId.
    if TransactionIdToPgIndex(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return Ok(());
    }

    let ctl = XactCtl();
    let pageno = TransactionIdToPage(newestXact);
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    ZeroCLOGPage(pageno, true, &mut bank)?;

    bank.release()
}

pub fn TruncateCLOG(oldestXact: TransactionId, oldestxid_datoid: Oid) -> PgResult<()> {
    let ctl = XactCtl();
    let cutoffPage = TransactionIdToPage(oldestXact);

    if !SlruScanDirectory(ctl, |ctl, filename, segpage| {
        SlruScanDirCbReportPresence(ctl, filename, segpage, cutoffPage)
    })? {
        return Ok(());
    }

    // Advance oldestClogXid first so concurrent status lookups never chase
    // truncated-away clog.
    varsup_seams::advance_oldest_clog_xid::call(oldestXact)?;

    // WAL-log + flush the new oldest xid before removing data, so a crash
    // (or a standby) always learns it ahead of the truncation.
    WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid)?;

    SimpleLruTruncate(ctl, cutoffPage)
}

// TransactionIdPrecedes offset by FirstNormal+1 so page 0 and the page
// preceding it compare as normal XIDs (clog.c CLOGPagePrecedes).
fn CLOGPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(CLOG_XACTS_PER_PAGE - 1))
}

fn WriteZeroPageXlogRec(pageno: i64) -> PgResult<()> {
    xloginsert_seams::xlog_insert::call(RM_CLOG_ID, CLOG_ZEROPAGE, &[&pageno.to_ne_bytes()])?;
    Ok(())
}

fn WriteTruncateXlogRec(pageno: i64, oldestXact: TransactionId, oldestXactDb: Oid) -> PgResult<()> {
    let mut xlrec = [0u8; 16];
    xlrec[0..8].copy_from_slice(&pageno.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&oldestXact.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&oldestXactDb.to_ne_bytes());

    let recptr = xloginsert_seams::xlog_insert::call(RM_CLOG_ID, CLOG_TRUNCATE, &[&xlrec])?;
    transam_xlog_seams::xlog_flush::call(recptr)
}

pub fn clog_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let ctl = XactCtl();
    let decoded = record
        .record
        .as_ref()
        .expect("clog_redo dispatched on a reader with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;

    debug_assert!(decoded.max_block_id < 0);
    // SAFETY: the reader's current record stays `decoded` for this whole call.
    let data = unsafe { decoded.main_data_bytes() };

    if info == CLOG_ZEROPAGE {
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("short CLOG_ZEROPAGE record"));

        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

        let slotno = ZeroCLOGPage(pageno, false, &mut bank)?;
        SimpleLruWritePage(ctl, slotno, &mut bank)?;
        debug_assert!(!ctl.page_dirty(slotno, &bank));

        bank.release()
    } else if info == CLOG_TRUNCATE {
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("short CLOG_TRUNCATE record"));
        let oldest_xact = TransactionId::from_ne_bytes(
            data[8..12].try_into().expect("short CLOG_TRUNCATE record"),
        );

        varsup_seams::advance_oldest_clog_xid::call(oldest_xact)?;

        SimpleLruTruncate(ctl, pageno)
    } else {
        ereport(PANIC)
            .errmsg(format!("clog_redo: unknown op code {info}"))
            .finish(ErrorLocation::new(file!(), line!() as i32, "clog_redo"))?;
        unreachable!("clog_redo PANIC returned");
    }
}

pub fn clogsyncfiletag(ftag: &FileTag) -> PgResult<(i32, SlruPath)> {
    SlruSyncFileTag(XactCtl(), ftag)
}

pub fn init_seams() {
    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_transaction_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    guc_tables::hooks::check_transaction_buffers.install(check_hook);
}

#[cfg(test)]
mod tests;
