// Shared queue state (AsyncQueueControl + the pg_notify SLRU).
//
// SAFETY (whole module): AsyncQueueControl lives in shared memory and is
// mutated through raw pointers under C's locking protocol (async.c:252-280):
// holding NotifyQueueLock SHARED permits reading head/tail and reading or
// writing one's OWN backend[] entry; EXCLUSIVE permits touching other
// backends' entries and the head; tail moves under NotifyQueueTailLock +
// NotifyQueueLock both EXCLUSIVE. Lock order: NotifyQueueTailLock, then
// NotifyQueueLock, then SLRU bank lock.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use init_small::globals as g;
use lwlock::{LWLock, LW_EXCLUSIVE, LW_SHARED};
use slru::{
    LwGuard, SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruShmemSize, SimpleLruTruncate, SimpleLruZeroPage, SlruCtlData, SlruScanDirCbDeleteAll,
    SlruScanDirectory, SLRU_PAGES_PER_SEGMENT,
};
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::primitive::{InvalidOid, ProcNumber, TimestampTz, INVALID_PROC_NUMBER};
use types_core::{
    InvalidTransactionId, Oid, Size, TransactionId, TransactionIdIsNormal, TransactionIdPrecedes,
    BLCKSZ, NAMEDATALEN,
};
use types_error::{PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, WARNING};
use types_storage::lock::AccessExclusiveLock;
use types_storage::storage::{LWTRANCHE_NOTIFY_BUFFER, LWTRANCHE_NOTIFY_SLRU};
use types_storage::sync::SyncRequestHandler;

use elog::ereport;
use types_error::ErrorLocation;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub const NOTIFY_PAYLOAD_MAX_LENGTH: usize = BLCKSZ - NAMEDATALEN as usize - 128;

const QUEUE_PAGESIZE: usize = BLCKSZ;
const QUEUE_CLEANUP_DELAY: i64 = 4;
const QUEUE_FULL_WARN_INTERVAL: i64 = 5000;

const AQE_DATA_OFFSET: usize = 16;
const AQE_EMPTY_SIZE: usize = AQE_DATA_OFFSET + 2;

const fn queuealign(len: usize) -> usize {
    (len + 3) & !3
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct QueuePosition {
    pub page: i64,
    pub offset: i32,
}

pub const QUEUE_POS_ZERO: QueuePosition = QueuePosition { page: 0, offset: 0 };

fn async_queue_page_precedes(p: i64, q: i64) -> bool {
    p < q
}

impl QueuePosition {
    fn is_zero(self) -> bool {
        self == QUEUE_POS_ZERO
    }

    fn min(self, y: QueuePosition) -> QueuePosition {
        if self.page != y.page {
            if async_queue_page_precedes(self.page, y.page) { self } else { y }
        } else if self.offset < y.offset {
            self
        } else {
            y
        }
    }

    fn max(self, y: QueuePosition) -> QueuePosition {
        if self.page != y.page {
            if async_queue_page_precedes(self.page, y.page) { y } else { self }
        } else if self.offset > y.offset {
            self
        } else {
            y
        }
    }

    pub fn advance(&mut self, entry_length: i32) -> bool {
        let offset = self.offset + entry_length;
        debug_assert!(offset as usize <= QUEUE_PAGESIZE);
        if offset as usize + queuealign(AQE_EMPTY_SIZE) > QUEUE_PAGESIZE {
            self.page += 1;
            self.offset = 0;
            return true;
        }
        self.offset = offset;
        false
    }
}

#[repr(C)]
struct QueueBackendStatus {
    pid: i32,
    dboid: Oid,
    next_listener: ProcNumber,
    pos: QueuePosition,
}

#[repr(C)]
struct AsyncQueueControlHdr {
    head: QueuePosition,
    tail: QueuePosition,
    stop_page: i64,
    first_listener: ProcNumber,
    last_queue_fill_warn: TimestampTz,
}

const INVALID_PID: i32 = -1;

static QUEUE_CTL: AtomicUsize = AtomicUsize::new(0);
static NOTIFY_CTL: OnceLock<SlruCtlData> = OnceLock::new();

fn hdr() -> *mut AsyncQueueControlHdr {
    let p = QUEUE_CTL.load(Ordering::Acquire);
    assert!(p != 0, "async queue accessed before AsyncShmemInit");
    p as *mut AsyncQueueControlHdr
}

fn backend(i: ProcNumber) -> *mut QueueBackendStatus {
    debug_assert!(i >= 0 && i < g::MaxBackends());
    // SAFETY: backend[] follows the header; i < MaxBackends per shmem sizing.
    unsafe {
        hdr()
            .add(1)
            .cast::<QueueBackendStatus>()
            .add(i as usize)
    }
}

fn notify_ctl() -> &'static SlruCtlData {
    NOTIFY_CTL
        .get()
        .unwrap_or_else(|| panic!("pg_notify SLRU accessed before AsyncShmemInit"))
}

fn notify_queue_lock() -> &'static LWLock {
    lwlock::main_lock(types_storage::storage::NOTIFY_QUEUE_LOCK)
}

fn notify_queue_tail_lock() -> &'static LWLock {
    lwlock::main_lock(types_storage::storage::NOTIFY_QUEUE_TAIL_LOCK)
}

fn queue_head(_qlock: &LwGuard) -> QueuePosition {
    // SAFETY: NotifyQueueLock held (module protocol).
    unsafe { (*hdr()).head }
}

fn queue_tail(_qlock: &LwGuard) -> QueuePosition {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*hdr()).tail }
}

fn first_listener(_qlock: &LwGuard) -> ProcNumber {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*hdr()).first_listener }
}

fn next_listener(i: ProcNumber, _qlock: &LwGuard) -> ProcNumber {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*backend(i)).next_listener }
}

fn backend_pos(i: ProcNumber, _qlock: &LwGuard) -> QueuePosition {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*backend(i)).pos }
}

fn backend_pid(i: ProcNumber, _qlock: &LwGuard) -> i32 {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*backend(i)).pid }
}

fn backend_dboid(i: ProcNumber, _qlock: &LwGuard) -> Oid {
    // SAFETY: NotifyQueueLock held.
    unsafe { (*backend(i)).dboid }
}

fn notify_buffers() -> i32 {
    guc_tables::vars::notify_buffers.read()
}

fn max_notify_queue_pages() -> i64 {
    crate::max_notify_queue_pages() as i64
}

pub fn AsyncShmemSize() -> Size {
    let mut size = g::MaxBackends() as usize * core::mem::size_of::<QueueBackendStatus>();
    size += core::mem::size_of::<AsyncQueueControlHdr>();
    size + SimpleLruShmemSize(notify_buffers(), 0)
}

fn reset_queue_control() {
    // SAFETY: boot or crash-reset; no concurrent backend threads exist.
    unsafe {
        let h = hdr();
        (*h).head = QUEUE_POS_ZERO;
        (*h).tail = QUEUE_POS_ZERO;
        (*h).stop_page = 0;
        (*h).first_listener = INVALID_PROC_NUMBER;
        (*h).last_queue_fill_warn = 0;
        for i in 0..g::MaxBackends() {
            let b = backend(i);
            (*b).pid = INVALID_PID;
            (*b).dboid = InvalidOid;
            (*b).next_listener = INVALID_PROC_NUMBER;
            (*b).pos = QUEUE_POS_ZERO;
        }
    }
}

pub fn AsyncShmemInit() -> PgResult<()> {
    let size = g::MaxBackends() as usize * core::mem::size_of::<QueueBackendStatus>()
        + core::mem::size_of::<AsyncQueueControlHdr>();
    let (base, found) = shmem::ShmemInitStruct("Async Queue Control", size)?;
    QUEUE_CTL.store(base as usize, Ordering::Release);
    if !found {
        reset_queue_control();
    }

    let mut ctl = SimpleLruInit(
        "notify",
        notify_buffers(),
        0,
        "pg_notify",
        LWTRANCHE_NOTIFY_BUFFER,
        LWTRANCHE_NOTIFY_SLRU,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        true,
    )?;
    ctl.PagePrecedes = Some(async_queue_page_precedes);
    if NOTIFY_CTL.set(ctl).is_err() {
        panic!("AsyncShmemInit called twice");
    }

    if !found {
        SlruScanDirectory(notify_ctl(), SlruScanDirCbDeleteAll)?;
    }
    Ok(())
}

/// Crash-cycle reset in place; notifications do not survive a crash (C wipes
/// pg_notify at every postmaster start).
pub fn AsyncShmemResetAfterCrash() -> PgResult<()> {
    reset_queue_control();
    slru::SimpleLruResetAfterCrash(notify_ctl());
    SlruScanDirectory(notify_ctl(), SlruScanDirCbDeleteAll)?;
    Ok(())
}

fn async_queue_is_full(qlock: &LwGuard) -> bool {
    queue_head(qlock).page - queue_tail(qlock).page >= max_notify_queue_pages()
}

fn async_queue_usage(qlock: &LwGuard) -> f64 {
    let occupied = queue_head(qlock).page - queue_tail(qlock).page;
    if occupied == 0 {
        return 0.0;
    }
    occupied as f64 / max_notify_queue_pages() as f64
}

fn async_queue_fill_warning(qlock: &LwGuard) -> PgResult<()> {
    let fill_degree = async_queue_usage(qlock);
    if fill_degree < 0.5 {
        return Ok(());
    }

    let t = adt_timestamp::GetCurrentTimestamp();
    // SAFETY: NotifyQueueLock held EXCLUSIVE (caller).
    let last_warn = unsafe { (*hdr()).last_queue_fill_warn };
    if adt_timestamp::TimestampDifferenceExceeds(last_warn, t, QUEUE_FULL_WARN_INTERVAL as i32) {
        let mut min = queue_head(qlock);
        let mut min_pid = INVALID_PID;
        let mut i = first_listener(qlock);
        while i != INVALID_PROC_NUMBER {
            debug_assert!(backend_pid(i, qlock) != INVALID_PID);
            min = min.min(backend_pos(i, qlock));
            if min == backend_pos(i, qlock) {
                min_pid = backend_pid(i, qlock);
            }
            i = next_listener(i, qlock);
        }

        let mut b = ereport(WARNING).errmsg(format!("NOTIFY queue is {:.0}% full", fill_degree * 100.0));
        if min_pid != INVALID_PID {
            b = b
                .errdetail(format!(
                    "The server process with PID {min_pid} is among those with the oldest transactions."
                ))
                .errhint(
                    "The NOTIFY queue cannot be emptied until that process ends its current transaction.",
                );
        }
        b.finish(loc("async_queue_fill_warning"))?;

        // SAFETY: NotifyQueueLock held EXCLUSIVE.
        unsafe { (*hdr()).last_queue_fill_warn = t };
    }
    Ok(())
}

fn write_entry_header(
    page: &mut [u8],
    offset: usize,
    length: i32,
    dboid: Oid,
    xid: TransactionId,
    src_pid: i32,
) {
    page[offset..offset + 4].copy_from_slice(&length.to_ne_bytes());
    page[offset + 4..offset + 8].copy_from_slice(&dboid.to_ne_bytes());
    page[offset + 8..offset + 12].copy_from_slice(&xid.to_ne_bytes());
    page[offset + 12..offset + 16].copy_from_slice(&src_pid.to_ne_bytes());
}

/// asyncQueueAddEntries: write events[next..] page-by-page; returns the index
/// of the first unwritten event (events.len() = all done). Caller holds
/// NotifyQueueLock EXCLUSIVE.
fn async_queue_add_entries(
    events: &[crate::Notification],
    mut next: usize,
    xid: TransactionId,
    qlock: &LwGuard,
    try_advance_tail: &mut bool,
) -> PgResult<usize> {
    let ctl = notify_ctl();
    let dboid = g::MyDatabaseId();
    let src_pid = g::MyProcPid();

    // Local head copy: SimpleLruZeroPage can fail (disk full); the shared head
    // must not advance past a page slru.c doesn't know about.
    let mut queue_head_pos = queue_head(qlock);

    let mut pageno = queue_head_pos.page;
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    let mut slotno = if queue_head_pos.is_zero() {
        SimpleLruZeroPage(ctl, pageno, &mut bank)?
    } else {
        SimpleLruReadPage(ctl, pageno, true, InvalidTransactionId, &mut bank)?
    };

    ctl.mark_page_dirty(slotno, &bank);

    while next < events.len() {
        let n = &events[next];
        let channellen = n.channel_len as usize;
        let payloadlen = n.payload_len as usize;
        debug_assert!(channellen < NAMEDATALEN as usize);
        debug_assert!(payloadlen < NOTIFY_PAYLOAD_MAX_LENGTH);

        let entry_length = queuealign(AQE_EMPTY_SIZE + channellen + payloadlen);
        let offset = queue_head_pos.offset as usize;

        let written_length;
        {
            let page = ctl.page_buffer_mut(slotno, &mut bank);
            if offset + entry_length <= QUEUE_PAGESIZE {
                write_entry_header(page, offset, entry_length as i32, dboid, xid, src_pid);
                let data = &mut page[offset + AQE_DATA_OFFSET..offset + entry_length];
                data[..channellen + payloadlen + 2].copy_from_slice(&n.data);
                for b in &mut data[channellen + payloadlen + 2..] {
                    *b = 0;
                }
                written_length = entry_length;
                next += 1;
            } else {
                // Dummy entry filling the page; readers skip it by dboid.
                written_length = QUEUE_PAGESIZE - offset;
                write_entry_header(
                    page,
                    offset,
                    written_length as i32,
                    InvalidOid,
                    InvalidTransactionId,
                    0,
                );
                page[offset + AQE_DATA_OFFSET] = 0;
                page[offset + AQE_DATA_OFFSET + 1] = 0;
            }
        }

        if queue_head_pos.advance(written_length as i32) {
            pageno = queue_head_pos.page;
            let lock = SimpleLruGetBankLock(ctl, pageno);
            if !bank.covers(lock) {
                bank.release()?;
                bank = LwGuard::acquire(lock, LW_EXCLUSIVE)?;
            }

            // Zero the next page now so slru.c's notion of the head page
            // matches ours (SimpleLruTruncate boundary; asyncQueueIsFull
            // guaranteed the room).
            slotno = SimpleLruZeroPage(ctl, pageno, &mut bank)?;
            let _ = slotno;

            if pageno % QUEUE_CLEANUP_DELAY == 0 {
                *try_advance_tail = true;
            }
            break;
        }
    }

    // SAFETY: NotifyQueueLock held EXCLUSIVE (caller).
    unsafe { (*hdr()).head = queue_head_pos };

    bank.release()?;
    Ok(next)
}

/// PreCommit_Notify's enqueue half: serialize writers, then push all pending
/// events into the queue page-by-page.
pub(crate) fn enqueue_pending(events: &[crate::Notification]) -> PgResult<()> {
    let xid = xact::GetCurrentTransactionId()?;

    lmgr::LockSharedObject(DATABASE_RELATION_ID, InvalidOid, 0, AccessExclusiveLock)?;

    let mut next = 0;
    while next < events.len() {
        let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
        async_queue_fill_warning(&qlock)?;
        if async_queue_is_full(&qlock) {
            return Err(ereport(types_error::ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("too many notifications in the NOTIFY queue")
                .into_error()
                .into());
        }
        let mut try_advance = false;
        next = async_queue_add_entries(events, next, xid, &qlock, &mut try_advance)?;
        if try_advance {
            crate::set_try_advance_tail();
        }
        qlock.release()?;
    }
    Ok(())
}

/// Exec_ListenPreCommit's shared-state half: register in the listener array,
/// adopting the max position of same-db listeners; returns true if our
/// starting position is behind the head (caller then catches up).
pub(crate) fn register_listener() -> PgResult<bool> {
    let my_procno = g::MyProcNumber();
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
    let head = queue_head(&qlock);
    let mut max = queue_tail(&qlock);
    let mut prev_listener = INVALID_PROC_NUMBER;
    let mut i = first_listener(&qlock);
    while i != INVALID_PROC_NUMBER {
        if backend_dboid(i, &qlock) == g::MyDatabaseId() {
            max = max.max(backend_pos(i, &qlock));
        }
        if i < my_procno {
            prev_listener = i;
        }
        i = next_listener(i, &qlock);
    }
    // SAFETY: NotifyQueueLock held EXCLUSIVE; list links + own entry.
    unsafe {
        let b = backend(my_procno);
        (*b).pos = max;
        (*b).pid = g::MyProcPid();
        (*b).dboid = g::MyDatabaseId();
        if prev_listener != INVALID_PROC_NUMBER {
            (*b).next_listener = (*backend(prev_listener)).next_listener;
            (*backend(prev_listener)).next_listener = my_procno;
        } else {
            (*b).next_listener = (*hdr()).first_listener;
            (*hdr()).first_listener = my_procno;
        }
    }
    let behind = max != head;
    qlock.release()?;
    Ok(behind)
}

/// asyncQueueUnregister's shared-state half.
pub(crate) fn unregister_listener() -> PgResult<()> {
    let my_procno = g::MyProcNumber();
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
    // SAFETY: NotifyQueueLock held EXCLUSIVE.
    unsafe {
        let b = backend(my_procno);
        (*b).pid = INVALID_PID;
        (*b).dboid = InvalidOid;
        if (*hdr()).first_listener == my_procno {
            (*hdr()).first_listener = (*b).next_listener;
        } else {
            let mut i = (*hdr()).first_listener;
            while i != INVALID_PROC_NUMBER {
                if (*backend(i)).next_listener == my_procno {
                    (*backend(i)).next_listener = (*b).next_listener;
                    break;
                }
                i = (*backend(i)).next_listener;
            }
        }
        (*b).next_listener = INVALID_PROC_NUMBER;
    }
    qlock.release()
}

/// SignalBackends: wake every listener that isn't caught up (same-db) or is
/// far behind (other-db). Self-signal short-circuits to the local flag.
pub(crate) fn signal_backends() -> PgResult<()> {
    let mut targets: Vec<(i32, ProcNumber)> = Vec::with_capacity(g::MaxBackends() as usize);

    let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
    let head = queue_head(&qlock);
    let mut i = first_listener(&qlock);
    while i != INVALID_PROC_NUMBER {
        let pid = backend_pid(i, &qlock);
        debug_assert!(pid != INVALID_PID);
        let pos = backend_pos(i, &qlock);
        if backend_dboid(i, &qlock) == g::MyDatabaseId() {
            if pos == head {
                i = next_listener(i, &qlock);
                continue;
            }
        } else if head.page - pos.page < QUEUE_CLEANUP_DELAY {
            i = next_listener(i, &qlock);
            continue;
        }
        targets.push((pid, i));
        i = next_listener(i, &qlock);
    }
    qlock.release()?;

    for (pid, procno) in targets {
        if pid == g::MyProcPid() {
            crate::set_notify_interrupt_pending();
            continue;
        }
        let _ = procsignal::SendProcSignal(
            pid,
            types_storage::storage::ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT,
            procno,
        );
    }
    Ok(())
}

pub(crate) struct ReadPositions {
    pub pos: QueuePosition,
    pub head: QueuePosition,
}

pub(crate) fn fetch_read_positions() -> PgResult<ReadPositions> {
    let my_procno = g::MyProcNumber();
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_SHARED)?;
    debug_assert!(backend_pid(my_procno, &qlock) == g::MyProcPid());
    let r = ReadPositions { pos: backend_pos(my_procno, &qlock), head: queue_head(&qlock) };
    qlock.release()?;
    Ok(r)
}

pub(crate) fn update_my_read_position(pos: QueuePosition) -> PgResult<()> {
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_SHARED)?;
    // SAFETY: own entry under SHARED lock (module protocol).
    unsafe { (*backend(g::MyProcNumber())).pos = pos };
    qlock.release()
}

pub(crate) struct QueueEntry<'a> {
    pub dboid: Oid,
    pub xid: TransactionId,
    pub src_pid: i32,
    pub length: i32,
    pub data: &'a [u8],
}

pub(crate) fn parse_entry(buf: &[u8], offset: usize) -> QueueEntry<'_> {
    let f = |o: usize| -> [u8; 4] { buf[offset + o..offset + o + 4].try_into().unwrap() };
    let length = i32::from_ne_bytes(f(0));
    QueueEntry {
        length,
        dboid: Oid::from_ne_bytes(f(4)),
        xid: TransactionId::from_ne_bytes(f(8)),
        src_pid: i32::from_ne_bytes(f(12)),
        data: &buf[offset + AQE_DATA_OFFSET..offset + length as usize],
    }
}

/// asyncQueueProcessPageEntries: copy this page's deliverable entries into
/// `local` (drained by the caller after the bank lock drops); returns true
/// once the stop position or an uncommitted entry is reached.
pub(crate) fn process_page_entries(
    current: &mut QueuePosition,
    stop: QueuePosition,
    snapshot: &snapmgr::Snapshot,
    listening_on_any: bool,
    local: &mut Vec<u8>,
) -> PgResult<bool> {
    let ctl = notify_ctl();
    let curpage = current.page;
    let mut reached_stop = false;

    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, curpage, InvalidTransactionId)?;

    loop {
        let thisentry = *current;
        if thisentry == stop {
            break;
        }

        let page = ctl.page_buffer(slotno, &bank);
        let entry = parse_entry(page, thisentry.offset as usize);

        // Advance before any failable processing (message resend hazard).
        let reached_end_of_page = current.advance(entry.length);

        if entry.dboid == g::MyDatabaseId() {
            if snapmgr::XidInMVCCSnapshot(entry.xid, snapshot)? {
                // Uncommitted; back up and reprocess next time.
                *current = thisentry;
                reached_stop = true;
                break;
            }

            // Not-listening fast path also skips entries whose xid lookup
            // might fail, so a first LISTEN can't get stuck (async.c:2049).
            if listening_on_any && transam::TransactionIdDidCommit(entry.xid)? {
                let start = thisentry.offset as usize;
                local.extend_from_slice(&page[start..start + entry.length as usize]);
            }
        }

        if reached_end_of_page {
            break;
        }
    }

    bank.release()?;

    if *current == stop {
        reached_stop = true;
    }
    Ok(reached_stop)
}

/// asyncQueueAdvanceTail: move the shared tail to the min backend position,
/// truncating pg_notify segments that fall wholly behind it.
pub(crate) fn advance_tail() -> PgResult<()> {
    let tail_lock = LwGuard::acquire(notify_queue_tail_lock(), LW_EXCLUSIVE)?;

    let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
    let mut min = queue_head(&qlock);
    let mut i = first_listener(&qlock);
    while i != INVALID_PROC_NUMBER {
        debug_assert!(backend_pid(i, &qlock) != INVALID_PID);
        min = min.min(backend_pos(i, &qlock));
        i = next_listener(i, &qlock);
    }
    // SAFETY: NotifyQueueLock held EXCLUSIVE.
    let oldtailpage = unsafe {
        (*hdr()).tail = min;
        (*hdr()).stop_page
    };
    qlock.release()?;

    let newtailpage = min.page;
    let boundary = newtailpage - (newtailpage % SLRU_PAGES_PER_SEGMENT as i64);
    if async_queue_page_precedes(oldtailpage, boundary) {
        SimpleLruTruncate(notify_ctl(), newtailpage)?;
        let qlock = LwGuard::acquire(notify_queue_lock(), LW_EXCLUSIVE)?;
        // SAFETY: NotifyQueueLock held EXCLUSIVE.
        unsafe { (*hdr()).stop_page = newtailpage };
        qlock.release()?;
    }

    tail_lock.release()
}

pub(crate) fn queue_usage_fraction() -> PgResult<f64> {
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_SHARED)?;
    let usage = async_queue_usage(&qlock);
    qlock.release()?;
    Ok(usage)
}

/// AsyncNotifyFreezeXids: called by VACUUM before advancing datfrozenxid;
/// stamps queue entries older than the cutoff Frozen/Invalid so CLOG
/// truncation can't strand unreadable xids in the queue.
pub fn AsyncNotifyFreezeXids(new_frozen_xid: TransactionId) -> PgResult<()> {
    let ctl = notify_ctl();

    let tail_lock = LwGuard::acquire(notify_queue_tail_lock(), LW_SHARED)?;
    let qlock = LwGuard::acquire(notify_queue_lock(), LW_SHARED)?;
    let mut pos = queue_tail(&qlock);
    let head = queue_head(&qlock);
    qlock.release()?;

    let mut curpage: i64 = -1;
    let mut slotno: Option<usize> = None;
    let mut bank: Option<LwGuard> = None;

    while pos != head {
        let pageno = pos.page;
        if pageno != curpage {
            if let Some(prev) = bank.take() {
                prev.release()?;
            }
            let mut b = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;
            slotno = Some(SimpleLruReadPage(ctl, pageno, true, InvalidTransactionId, &mut b)?);
            bank = Some(b);
            curpage = pageno;
        }
        let b = bank.as_mut().expect("bank lock held");
        let slot = slotno.expect("slot read");
        let offset = pos.offset as usize;

        let (xid, length) = {
            let page = ctl.page_buffer(slot, b);
            let e = parse_entry(page, offset);
            (e.xid, e.length)
        };

        if TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, new_frozen_xid) {
            let frozen = if transam::TransactionIdDidCommit(xid)? {
                types_core::FrozenTransactionId
            } else {
                InvalidTransactionId
            };
            ctl.page_buffer_mut(slot, b)[offset + 8..offset + 12]
                .copy_from_slice(&frozen.to_ne_bytes());
            ctl.mark_page_dirty(slot, b);
        }

        pos.advance(length);
    }

    if let Some(b) = bank {
        b.release()?;
    }
    tail_lock.release()
}

pub fn check_notify_buffers(newval: i32) -> (bool, Option<String>) {
    slru::check_slru_buffers("notify_buffers", newval)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_c() {
        assert_eq!(core::mem::size_of::<QueuePosition>(), 16);
        assert_eq!(core::mem::size_of::<QueueBackendStatus>(), 32);
        assert_eq!(core::mem::size_of::<AsyncQueueControlHdr>(), 56);
        assert_eq!(NOTIFY_PAYLOAD_MAX_LENGTH, 8000);
        assert_eq!(queuealign(AQE_EMPTY_SIZE), 20);
    }

    #[test]
    fn advance_page_jump() {
        let mut p = QueuePosition { page: 0, offset: 0 };
        assert!(!p.advance(20));
        assert_eq!(p, QueuePosition { page: 0, offset: 20 });
        p.offset = (QUEUE_PAGESIZE - 20) as i32;
        assert!(p.advance(20));
        assert_eq!(p, QueuePosition { page: 1, offset: 0 });
        // Entry ending with < 20 bytes left also jumps.
        let mut p = QueuePosition { page: 3, offset: (QUEUE_PAGESIZE - 30) as i32 };
        assert!(p.advance(12));
    }

    #[test]
    fn pos_min_max() {
        let a = QueuePosition { page: 1, offset: 100 };
        let b = QueuePosition { page: 2, offset: 0 };
        let c = QueuePosition { page: 1, offset: 200 };
        assert_eq!(a.min(b), a);
        assert_eq!(a.max(b), b);
        assert_eq!(a.min(c), a);
        assert_eq!(a.max(c), c);
    }
}
