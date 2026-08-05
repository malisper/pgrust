#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

mod guard;
mod path;
mod shmem;

pub use guard::LwGuard;
pub use path::{SlruDir, SlruPath};

use ::elog::ereport;
use core::fmt::Write as _;
use core::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use init_small::globals;
use lwlock::{
    LWLock, LWLockInitialize, LWLockPadded, LW_EXCLUSIVE, LW_SHARED, LWLOCK_PADDED_SIZE,
};
use shmem::{shared_slice, ShmemPages, ShmemVals};
use types_core::{
    InvalidTransactionId, InvalidXLogRecPtr, Size, TransactionId, TransactionIdIsNormal,
    TransactionIdPrecedes, XLogRecPtr, BLCKSZ,
};
use types_error::{ErrorLocation, PgResult, DEBUG2, ERROR, LOG};
use types_storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};

pub const SLRU_MAX_ALLOWED_BUFFERS: i32 = ((1024 * 1024 * 1024) / BLCKSZ) as i32;
pub const SLRU_PAGES_PER_SEGMENT: i64 = 32;
pub const SLRU_BANK_BITSHIFT: u32 = 4;
pub const SLRU_BANK_SIZE: i32 = 1 << SLRU_BANK_BITSHIFT;
const MAX_WRITEALL_BUFFERS: usize = 16;

const PG_WAIT_IO: u32 = 0x0A00_0000;
pub const WAIT_EVENT_SLRU_FLUSH_SYNC: u32 = PG_WAIT_IO + 50;
pub const WAIT_EVENT_SLRU_READ: u32 = PG_WAIT_IO + 51;
pub const WAIT_EVENT_SLRU_SYNC: u32 = PG_WAIT_IO + 52;
pub const WAIT_EVENT_SLRU_WRITE: u32 = PG_WAIT_IO + 53;

#[inline]
fn SlotGetBankNumber(slotno: usize) -> usize {
    slotno >> SLRU_BANK_BITSHIFT
}

#[repr(i32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SlruPageStatus {
    #[default]
    SLRU_PAGE_EMPTY = 0,
    SLRU_PAGE_READ_IN_PROGRESS = 1,
    SLRU_PAGE_VALID = 2,
    SLRU_PAGE_WRITE_IN_PROGRESS = 3,
}
pub use SlruPageStatus::*;

pub type SlruPagePrecedes = fn(i64, i64) -> bool;

pub struct SlruSharedData {
    num_slots: i32,
    pages: ShmemPages,
    page_status: ShmemVals<SlruPageStatus>,
    page_dirty: ShmemVals<bool>,
    page_number: ShmemVals<i64>,
    // Wrapping counters; racy loss under a shared bank lock is benign in C —
    // relaxed atomics make that race defined here.
    page_lru_count: &'static [AtomicI32],
    buffer_locks: &'static [LWLockPadded],
    bank_locks: &'static [LWLockPadded],
    bank_cur_lru_count: &'static [AtomicI32],
    group_lsn: ShmemVals<XLogRecPtr>,
    lsn_groups_per_page: i32,
    latest_page_number: &'static AtomicU64,
    slru_stats_idx: i32,
}

pub struct SlruCtlData {
    pub shared: SlruSharedData,
    nbanks: u16,
    long_segment_names: bool,
    sync_handler: SyncRequestHandler,
    pub PagePrecedes: Option<SlruPagePrecedes>,
    dir: SlruDir,
}

impl SlruCtlData {
    fn page_precedes(&self) -> SlruPagePrecedes {
        self.PagePrecedes
            .expect("SLRU PagePrecedes callback not installed (null function pointer call in C)")
    }

    pub fn dir(&self) -> &str {
        self.dir.as_str()
    }

    pub fn sync_handler(&self) -> SyncRequestHandler {
        self.sync_handler
    }

    pub fn num_slots(&self) -> usize {
        self.shared.num_slots as usize
    }

    pub fn lsn_groups_per_page(&self) -> i32 {
        self.shared.lsn_groups_per_page
    }

    pub fn bank_lock_for_slot(&self, slotno: usize) -> &LWLock {
        &self.shared.bank_locks[SlotGetBankNumber(slotno)].lock
    }

    pub fn page_status(&self, slotno: usize, bank: &LwGuard) -> SlruPageStatus {
        debug_assert!(bank.covers(self.bank_lock_for_slot(slotno)) && bank.is_held());
        self.shared.page_status.get(slotno)
    }

    pub fn page_number(&self, slotno: usize, bank: &LwGuard) -> i64 {
        debug_assert!(bank.covers(self.bank_lock_for_slot(slotno)) && bank.is_held());
        self.shared.page_number.get(slotno)
    }

    pub fn page_dirty(&self, slotno: usize, bank: &LwGuard) -> bool {
        debug_assert!(bank.covers(self.bank_lock_for_slot(slotno)) && bank.is_held());
        self.shared.page_dirty.get(slotno)
    }

    // C consumers write latest_page_number with pg_atomic_write_u64 (StartupCLOG).
    pub fn set_latest_page_number(&self, pageno: i64) {
        self.shared.latest_page_number.store(pageno as u64, Ordering::Relaxed);
    }

    pub fn latest_page_number(&self) -> i64 {
        self.shared.latest_page_number.load(Ordering::Relaxed) as i64
    }

    pub fn mark_page_dirty(&self, slotno: usize, bank: &LwGuard) {
        debug_assert!(bank.held_in_mode(self.bank_lock_for_slot(slotno), LW_EXCLUSIVE));
        self.shared.page_dirty.set(slotno, true);
    }

    pub fn page_buffer<'g>(&self, slotno: usize, bank: &'g LwGuard) -> &'g [u8] {
        debug_assert!(bank.covers(self.bank_lock_for_slot(slotno)) && bank.is_held());
        // SAFETY: non-READ_IN_PROGRESS page bytes are mutated only under the
        // exclusive bank lock, which the guard pins for 'g.
        unsafe { core::slice::from_raw_parts(self.shared.pages.page_ptr(slotno), BLCKSZ) }
    }

    pub fn page_buffer_mut<'g>(&self, slotno: usize, bank: &'g mut LwGuard) -> &'g mut [u8] {
        debug_assert!(bank.held_in_mode(self.bank_lock_for_slot(slotno), LW_EXCLUSIVE));
        // SAFETY: as `page_buffer`, exclusively.
        unsafe { core::slice::from_raw_parts_mut(self.shared.pages.page_ptr(slotno), BLCKSZ) }
    }

    pub fn group_lsn(&self, lsnindex: usize, bank: &LwGuard) -> XLogRecPtr {
        debug_assert!(bank.is_held());
        self.shared.group_lsn.get(lsnindex)
    }

    pub fn set_group_lsn(&self, lsnindex: usize, lsn: XLogRecPtr, bank: &LwGuard) {
        debug_assert!(bank.is_held());
        self.shared.group_lsn.set(lsnindex, lsn);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlruErrorCause {
    SLRU_OPEN_FAILED,
    SLRU_SEEK_FAILED,
    SLRU_READ_FAILED,
    SLRU_WRITE_FAILED,
    SLRU_FSYNC_FAILED,
    SLRU_CLOSE_FAILED,
}
use SlruErrorCause::*;

// C's file-static slru_errcause/slru_errno result channel, carried by value.
#[derive(Clone, Copy, Debug)]
struct SlruIoError {
    cause: SlruErrorCause,
    errno: i32,
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: libc returns this thread's errno slot (all four fns below).
    unsafe { libc::__error() }
}
#[cfg(not(target_os = "macos"))]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

fn current_errno() -> i32 {
    unsafe { *errno_location() }
}

fn set_errno(value: i32) {
    unsafe { *errno_location() = value }
}

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

fn bufferalign(len: usize) -> usize {
    (len + 31) & !31
}

// C casts 8-aligned offsets to LWLockPadded*; Rust's align(128) slices reject
// that, so lock-array offsets round up to 128 (size accounting matches).
fn lwlockalign(len: usize) -> usize {
    (len + (LWLOCK_PADDED_SIZE - 1)) & !(LWLOCK_PADDED_SIZE - 1)
}

// C sizeof(SlruSharedData) on LP64; the header interior here holds only
// latest_page_number (offset 0) and num_slots (offset 8).
const SLRU_SHARED_DATA_SIZE: usize = 104;

pub fn SimpleLruShmemSize(nslots: i32, nlsns: i32) -> Size {
    let nbanks = (nslots / SLRU_BANK_SIZE) as usize;
    debug_assert!(nslots <= SLRU_MAX_ALLOWED_BUFFERS);
    debug_assert!(nslots % SLRU_BANK_SIZE == 0);
    let nslots = nslots as usize;

    let mut sz = maxalign(SLRU_SHARED_DATA_SIZE);
    sz += maxalign(nslots * core::mem::size_of::<*const u8>());
    sz += maxalign(nslots * core::mem::size_of::<i32>());
    sz += maxalign(nslots * core::mem::size_of::<bool>());
    sz += maxalign(nslots * core::mem::size_of::<i64>());
    sz += maxalign(nslots * core::mem::size_of::<i32>());
    sz = lwlockalign(sz);
    sz += maxalign(nslots * LWLOCK_PADDED_SIZE);
    sz = lwlockalign(sz);
    sz += maxalign(nbanks * LWLOCK_PADDED_SIZE);
    sz += maxalign(nbanks * core::mem::size_of::<i32>());

    if nlsns > 0 {
        sz += maxalign(nslots * nlsns as usize * core::mem::size_of::<XLogRecPtr>());
    }

    bufferalign(sz) + BLCKSZ * nslots
}

pub fn SimpleLruAutotuneBuffers(divisor: i32, max: i32) -> i32 {
    let nbuffers = globals::NBuffers();
    (max - max % SLRU_BANK_SIZE)
        .min(SLRU_BANK_SIZE.max(nbuffers / divisor - (nbuffers / divisor) % SLRU_BANK_SIZE))
}

pub fn SimpleLruInit(
    name: &str,
    nslots: i32,
    nlsns: i32,
    subdir: &str,
    buffer_tranche_id: i32,
    bank_tranche_id: i32,
    sync_handler: SyncRequestHandler,
    long_segment_names: bool,
) -> PgResult<SlruCtlData> {
    let nbanks = nslots / SLRU_BANK_SIZE;
    debug_assert!(nslots <= SLRU_MAX_ALLOWED_BUFFERS);
    debug_assert!(nslots % SLRU_BANK_SIZE == 0);

    let nslots_u = nslots as usize;
    let nbanks_u = nbanks as usize;
    let nlsns_u = nlsns.max(0) as usize;
    let nlsn_entries = if nlsns_u > 0 { nslots_u * nlsns_u } else { 0 };

    let shmem_size = SimpleLruShmemSize(nslots, nlsns);
    let (base, found) = shmem_seams::shmem_init_struct::call(name, shmem_size)?;
    assert!(base as usize % LWLOCK_PADDED_SIZE == 0);

    let mut off = maxalign(SLRU_SHARED_DATA_SIZE);
    // C's page_buffer[] pointer array, reserved only for offset parity.
    off += maxalign(nslots_u * core::mem::size_of::<*const u8>());
    let page_status_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<SlruPageStatus>());
    let page_dirty_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<bool>());
    let page_number_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<i64>());
    let page_lru_count_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<i32>());
    off = lwlockalign(off);
    let buffer_locks_off = off;
    off += maxalign(nslots_u * LWLOCK_PADDED_SIZE);
    off = lwlockalign(off);
    let bank_locks_off = off;
    off += maxalign(nbanks_u * LWLOCK_PADDED_SIZE);
    let bank_cur_lru_count_off = off;
    off += maxalign(nbanks_u * core::mem::size_of::<i32>());
    let group_lsn_off = off;
    if nlsns_u > 0 {
        off += maxalign(nlsn_entries * core::mem::size_of::<XLogRecPtr>());
    }
    let pages_off = bufferalign(off);

    // SAFETY: every region lies within the live `shmem_size` bytes at `base`
    // and is aligned for its element type (locks via lwlockalign'ed offsets
    // against the asserted 128-aligned base).
    unsafe {
        let num_slots_ptr = base.add(8).cast::<i32>();
        if !found {
            core::ptr::write_bytes(base, 0, maxalign(SLRU_SHARED_DATA_SIZE));
            num_slots_ptr.write(nslots);
            base.add(12).cast::<i32>().write(nlsns);

            let status = base.add(page_status_off).cast::<SlruPageStatus>();
            let dirty = base.add(page_dirty_off).cast::<bool>();
            let number = base.add(page_number_off).cast::<i64>();
            let lru = base.add(page_lru_count_off).cast::<i32>();
            for i in 0..nslots_u {
                status.add(i).write(SLRU_PAGE_EMPTY);
                dirty.add(i).write(false);
                number.add(i).write(0);
                lru.add(i).write(0);
                LWLockInitialize(
                    &mut (*base.add(buffer_locks_off).cast::<LWLockPadded>().add(i)).lock,
                    buffer_tranche_id,
                );
            }
            let bank_lru = base.add(bank_cur_lru_count_off).cast::<i32>();
            for b in 0..nbanks_u {
                bank_lru.add(b).write(0);
                LWLockInitialize(
                    &mut (*base.add(bank_locks_off).cast::<LWLockPadded>().add(b)).lock,
                    bank_tranche_id,
                );
            }
            let lsn = base.add(group_lsn_off).cast::<XLogRecPtr>();
            for i in 0..nlsn_entries {
                lsn.add(i).write(InvalidXLogRecPtr);
            }
            core::ptr::write_bytes(base.add(pages_off), 0, nslots_u * BLCKSZ);
        } else {
            debug_assert!(num_slots_ptr.read() == nslots);
        }
    }

    // SAFETY: regions as above, initialized by the creator; AtomicI32 is
    // layout-compatible with the i32s written there.
    let shared = unsafe {
        SlruSharedData {
            num_slots: nslots,
            pages: ShmemPages::from_raw(base.add(pages_off), nslots_u),
            page_status: ShmemVals::from_raw(
                base.add(page_status_off).cast::<SlruPageStatus>(),
                nslots_u,
            ),
            page_dirty: ShmemVals::from_raw(base.add(page_dirty_off).cast::<bool>(), nslots_u),
            page_number: ShmemVals::from_raw(base.add(page_number_off).cast::<i64>(), nslots_u),
            page_lru_count: shared_slice(
                base.add(page_lru_count_off).cast::<AtomicI32>(),
                nslots_u,
            ),
            buffer_locks: shared_slice(
                base.add(buffer_locks_off).cast::<LWLockPadded>(),
                nslots_u,
            ),
            bank_locks: shared_slice(base.add(bank_locks_off).cast::<LWLockPadded>(), nbanks_u),
            bank_cur_lru_count: shared_slice(
                base.add(bank_cur_lru_count_off).cast::<AtomicI32>(),
                nbanks_u,
            ),
            group_lsn: ShmemVals::from_raw(
                base.add(group_lsn_off).cast::<XLogRecPtr>(),
                nlsn_entries,
            ),
            lsn_groups_per_page: nlsns,
            latest_page_number: &*(base.cast::<AtomicU64>()),
            slru_stats_idx: pgstat_seams::pgstat_get_slru_index::call(name),
        }
    };

    Ok(SlruCtlData {
        shared,
        nbanks: nbanks as u16,
        long_segment_names,
        sync_handler,
        PagePrecedes: None,
        dir: SlruDir::new(subdir),
    })
}

fn reset_lwlock_in_place(lock: &LWLock) {
    lock.state.store(lwlock::LW_FLAG_RELEASE_OK, Ordering::Relaxed);
    // SAFETY: crash choreography drained every child before reset; the
    // postmaster thread has exclusive access, so no holder or waiter exists.
    unsafe {
        *lock.waiters.get() = lmgr_proc_seams::proclist_head {
            head: types_core::INVALID_PROC_NUMBER,
            tail: types_core::INVALID_PROC_NUMBER,
        };
    }
}

/// Crash-cycle reset in place to the post-SimpleLruInit boot image
/// (notes/crash-restart-design.md); startup re-seeds from pg_control/WAL.
pub fn SimpleLruResetAfterCrash(ctl: &SlruCtlData) {
    let sh = &ctl.shared;
    let nslots = sh.num_slots as usize;
    for i in 0..nslots {
        sh.page_status.set(i, SLRU_PAGE_EMPTY);
        sh.page_dirty.set(i, false);
        sh.page_number.set(i, 0);
        sh.page_lru_count[i].store(0, Ordering::Relaxed);
        reset_lwlock_in_place(&sh.buffer_locks[i].lock);
        // SAFETY: exclusive access as above; each slot page spans BLCKSZ bytes.
        unsafe { core::ptr::write_bytes(sh.pages.page_ptr(i), 0, BLCKSZ) };
    }
    for b in 0..ctl.nbanks as usize {
        sh.bank_cur_lru_count[b].store(0, Ordering::Relaxed);
        reset_lwlock_in_place(&sh.bank_locks[b].lock);
    }
    if sh.lsn_groups_per_page > 0 {
        for i in 0..nslots * sh.lsn_groups_per_page as usize {
            sh.group_lsn.set(i, InvalidXLogRecPtr);
        }
    }
    sh.latest_page_number.store(0, Ordering::Relaxed);
}

pub fn check_slru_buffers(name: &str, newval: i32) -> (bool, Option<String>) {
    if newval % SLRU_BANK_SIZE == 0 {
        (true, None)
    } else {
        (
            false,
            Some(format!("\"{name}\" must be a multiple of {SLRU_BANK_SIZE}.")),
        )
    }
}

pub fn SlruFileName(ctl: &SlruCtlData, segno: i64) -> SlruPath {
    let mut path = SlruPath::new();
    if ctl.long_segment_names {
        // 15 hex chars, not 16: stays distinguishable from WAL segment names.
        debug_assert!((0..=0x0FFF_FFFF_FFFF_FFFF).contains(&segno));
        write!(path, "{}/{:015X}", ctl.dir, segno)
    } else {
        debug_assert!((0..=0xFF_FFFF).contains(&segno));
        write!(path, "{}/{:04X}", ctl.dir, segno as u32)
    }
    .expect("SLRU segment path overflow");
    path
}

pub fn SimpleLruGetBankLock(ctl: &SlruCtlData, pageno: i64) -> &LWLock {
    let bankno = (pageno % ctl.nbanks as i64) as usize;
    &ctl.shared.bank_locks[bankno].lock
}

pub fn SimpleLruZeroPage(ctl: &SlruCtlData, pageno: i64, bank: &mut LwGuard) -> PgResult<usize> {
    let shared = &ctl.shared;
    debug_assert!(bank.held_in_mode(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE));

    let slotno = SlruSelectLRUPage(ctl, pageno, bank)?;
    debug_assert!(
        shared.page_status.get(slotno) == SLRU_PAGE_EMPTY
            || (shared.page_status.get(slotno) == SLRU_PAGE_VALID
                && !shared.page_dirty.get(slotno))
            || shared.page_number.get(slotno) == pageno
    );

    shared.page_number.set(slotno, pageno);
    shared.page_status.set(slotno, SLRU_PAGE_VALID);
    shared.page_dirty.set(slotno, true);
    SlruRecentlyUsed(shared, slotno);

    // SAFETY: exclusive bank lock held; slot is ours until released.
    unsafe { core::ptr::write_bytes(shared.pages.page_ptr(slotno), 0, BLCKSZ) };

    SimpleLruZeroLSNs(ctl, slotno);

    // Runs under the same bank lock as the victim search; no barrier needed.
    shared.latest_page_number.store(pageno as u64, Ordering::Relaxed);

    pgstat_seams::pgstat_count_slru_page_zeroed::call(shared.slru_stats_idx);

    Ok(slotno)
}

fn SimpleLruZeroLSNs(ctl: &SlruCtlData, slotno: usize) {
    let groups = ctl.shared.lsn_groups_per_page as usize;
    for i in slotno * groups..(slotno + 1) * groups {
        ctl.shared.group_lsn.set(i, InvalidXLogRecPtr);
    }
}

fn SimpleLruWaitIO(ctl: &SlruCtlData, slotno: usize, bank: &mut LwGuard) -> PgResult<()> {
    let shared = &ctl.shared;
    debug_assert!(shared.page_status.get(slotno) != SLRU_PAGE_EMPTY);

    bank.unlock()?;
    LwGuard::acquire(&shared.buffer_locks[slotno].lock, LW_SHARED)?.release()?;
    bank.relock(LW_EXCLUSIVE)?;

    if shared.page_status.get(slotno) == SLRU_PAGE_READ_IN_PROGRESS
        || shared.page_status.get(slotno) == SLRU_PAGE_WRITE_IN_PROGRESS
    {
        // In-progress state with a free buffer lock = failed I/O whose owner
        // aborted without resetting the state.
        if let Some(buf) = LwGuard::conditional_acquire(&shared.buffer_locks[slotno].lock, LW_SHARED)?
        {
            if shared.page_status.get(slotno) == SLRU_PAGE_READ_IN_PROGRESS {
                shared.page_status.set(slotno, SLRU_PAGE_EMPTY);
            } else {
                shared.page_status.set(slotno, SLRU_PAGE_VALID);
                shared.page_dirty.set(slotno, true);
            }
            buf.release()?;
        }
    }
    Ok(())
}

pub fn SimpleLruReadPage(
    ctl: &SlruCtlData,
    pageno: i64,
    write_ok: bool,
    xid: TransactionId,
    bank: &mut LwGuard,
) -> PgResult<usize> {
    let shared = &ctl.shared;
    debug_assert!(bank.held_in_mode(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE));

    loop {
        let slotno = SlruSelectLRUPage(ctl, pageno, bank)?;

        if shared.page_status.get(slotno) != SLRU_PAGE_EMPTY
            && shared.page_number.get(slotno) == pageno
        {
            if shared.page_status.get(slotno) == SLRU_PAGE_READ_IN_PROGRESS
                || (shared.page_status.get(slotno) == SLRU_PAGE_WRITE_IN_PROGRESS && !write_ok)
            {
                SimpleLruWaitIO(ctl, slotno, bank)?;
                continue;
            }
            SlruRecentlyUsed(shared, slotno);
            pgstat_seams::pgstat_count_slru_page_hit::call(shared.slru_stats_idx);
            return Ok(slotno);
        }

        debug_assert!(
            shared.page_status.get(slotno) == SLRU_PAGE_EMPTY
                || (shared.page_status.get(slotno) == SLRU_PAGE_VALID
                    && !shared.page_dirty.get(slotno))
        );

        shared.page_number.set(slotno, pageno);
        shared.page_status.set(slotno, SLRU_PAGE_READ_IN_PROGRESS);
        shared.page_dirty.set(slotno, false);

        let buf = LwGuard::acquire(&shared.buffer_locks[slotno].lock, LW_EXCLUSIVE)?;
        bank.unlock()?;

        let ok = SlruPhysicalReadPage(ctl, pageno, slotno)?;

        SimpleLruZeroLSNs(ctl, slotno);

        bank.relock(LW_EXCLUSIVE)?;

        debug_assert!(
            shared.page_number.get(slotno) == pageno
                && shared.page_status.get(slotno) == SLRU_PAGE_READ_IN_PROGRESS
                && !shared.page_dirty.get(slotno)
        );

        shared.page_status.set(
            slotno,
            if ok.is_ok() {
                SLRU_PAGE_VALID
            } else {
                SLRU_PAGE_EMPTY
            },
        );

        buf.release()?;

        if let Err(io_err) = ok {
            return SlruReportIOError(ctl, pageno, xid, io_err);
        }

        SlruRecentlyUsed(shared, slotno);
        pgstat_seams::pgstat_count_slru_page_read::call(shared.slru_stats_idx);
        return Ok(slotno);
    }
}

pub fn SimpleLruReadPage_ReadOnly(
    ctl: &SlruCtlData,
    pageno: i64,
    xid: TransactionId,
) -> PgResult<(usize, LwGuard)> {
    let shared = &ctl.shared;
    let banklock = SimpleLruGetBankLock(ctl, pageno);
    let bankno = (pageno % ctl.nbanks as i64) as usize;
    let bankstart = bankno * SLRU_BANK_SIZE as usize;
    let bankend = bankstart + SLRU_BANK_SIZE as usize;

    let mut bank = LwGuard::acquire(banklock, LW_SHARED)?;

    for slotno in bankstart..bankend {
        if shared.page_status.get(slotno) != SLRU_PAGE_EMPTY
            && shared.page_number.get(slotno) == pageno
            && shared.page_status.get(slotno) != SLRU_PAGE_READ_IN_PROGRESS
        {
            SlruRecentlyUsed(shared, slotno);
            pgstat_seams::pgstat_count_slru_page_hit::call(shared.slru_stats_idx);
            return Ok((slotno, bank));
        }
    }

    bank.unlock()?;
    bank.relock(LW_EXCLUSIVE)?;

    let slotno = SimpleLruReadPage(ctl, pageno, true, xid, &mut bank)?;
    Ok((slotno, bank))
}

struct SlruWriteAllData {
    num_files: usize,
    fd: [i32; MAX_WRITEALL_BUFFERS],
    segno: [i64; MAX_WRITEALL_BUFFERS],
}

impl SlruWriteAllData {
    fn new() -> Self {
        Self {
            num_files: 0,
            fd: [-1; MAX_WRITEALL_BUFFERS],
            segno: [0; MAX_WRITEALL_BUFFERS],
        }
    }
}

fn SlruInternalWritePage(
    ctl: &SlruCtlData,
    slotno: usize,
    mut fdata: Option<&mut SlruWriteAllData>,
    bank: &mut LwGuard,
) -> PgResult<()> {
    let shared = &ctl.shared;
    let pageno = shared.page_number.get(slotno);

    debug_assert!(shared.page_status.get(slotno) != SLRU_PAGE_EMPTY);
    debug_assert!(bank.held_in_mode(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE));

    while shared.page_status.get(slotno) == SLRU_PAGE_WRITE_IN_PROGRESS
        && shared.page_number.get(slotno) == pageno
    {
        SimpleLruWaitIO(ctl, slotno, bank)?;
    }

    if !shared.page_dirty.get(slotno)
        || shared.page_status.get(slotno) != SLRU_PAGE_VALID
        || shared.page_number.get(slotno) != pageno
    {
        return Ok(());
    }

    shared.page_status.set(slotno, SLRU_PAGE_WRITE_IN_PROGRESS);
    shared.page_dirty.set(slotno, false);

    let buf = LwGuard::acquire(&shared.buffer_locks[slotno].lock, LW_EXCLUSIVE)?;
    bank.unlock()?;

    let ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata.as_deref_mut())?;

    if ok.is_err() {
        if let Some(f) = fdata.as_deref_mut() {
            for i in 0..f.num_files {
                file_seams::close_transient_file::call(f.fd[i]);
            }
        }
    }

    bank.relock(LW_EXCLUSIVE)?;

    debug_assert!(
        shared.page_number.get(slotno) == pageno
            && shared.page_status.get(slotno) == SLRU_PAGE_WRITE_IN_PROGRESS
    );

    if ok.is_err() {
        shared.page_dirty.set(slotno, true);
    }
    shared.page_status.set(slotno, SLRU_PAGE_VALID);

    buf.release()?;

    if let Err(io_err) = ok {
        return SlruReportIOError(ctl, pageno, InvalidTransactionId, io_err);
    }

    if fdata.is_some() {
        transam_xlog_seams::count_ckpt_slru_written::call();
        pgstat_seams::pgstat_count_checkpointer_slru_written::call();
    }
    Ok(())
}

pub fn SimpleLruWritePage(ctl: &SlruCtlData, slotno: usize, bank: &mut LwGuard) -> PgResult<()> {
    debug_assert!(ctl.shared.page_status.get(slotno) != SLRU_PAGE_EMPTY);
    SlruInternalWritePage(ctl, slotno, None, bank)
}

pub fn SimpleLruDoesPhysicalPageExist(ctl: &SlruCtlData, pageno: i64) -> PgResult<bool> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;

    pgstat_seams::pgstat_count_slru_page_exists::call(ctl.shared.slru_stats_idx);

    let path = SlruFileName(ctl, segno);

    let fd = file_seams::open_transient_file::call(path.as_str(), libc::O_RDONLY)?;
    if fd < 0 {
        let en = current_errno();
        if en == libc::ENOENT {
            return Ok(false);
        }
        return SlruReportIOError(
            ctl,
            pageno,
            0,
            SlruIoError {
                cause: SLRU_OPEN_FAILED,
                errno: en,
            },
        );
    }

    let endpos = vfs::file_size(fd) as i64;
    if endpos < 0 {
        let en = current_errno();
        // C unwinds with the transient fd open; fd.c's abort cleanup owns it.
        return SlruReportIOError(
            ctl,
            pageno,
            0,
            SlruIoError {
                cause: SLRU_SEEK_FAILED,
                errno: en,
            },
        );
    }

    let result = endpos as i64 >= offset + BLCKSZ as i64;

    if file_seams::close_transient_file::call(fd) != 0 {
        // C records the close failure without reporting and returns false.
        return Ok(false);
    }

    Ok(result)
}

// Inner Err feeds SlruReportIOError after the caller undoes shared state;
// the outer Err is OpenTransientFile's own ereport surface.
fn SlruPhysicalReadPage(
    ctl: &SlruCtlData,
    pageno: i64,
    slotno: usize,
) -> PgResult<Result<(), SlruIoError>> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;

    let path = SlruFileName(ctl, segno);

    let fd = file_seams::open_transient_file::call(path.as_str(), libc::O_RDONLY)?;
    if fd < 0 {
        let en = current_errno();
        // Redo may reference truncated segments; in recovery read as zeroes.
        if en != libc::ENOENT || !xlogutils_seams::in_recovery::call() {
            return Ok(Err(SlruIoError {
                cause: SLRU_OPEN_FAILED,
                errno: en,
            }));
        }

        ereport(LOG)
            .errmsg(format!("file \"{path}\" doesn't exist, reading as zeroes"))
            .finish(loc("SlruPhysicalReadPage"))?;
        // SAFETY: caller holds the buffer lock; READ_IN_PROGRESS bytes are ours.
        unsafe { core::ptr::write_bytes(ctl.shared.pages.page_ptr(slotno), 0, BLCKSZ) };
        return Ok(Ok(()));
    }

    set_errno(0);
    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_READ);
    // SAFETY: the slot's BLCKSZ bytes are ours per the buffer-lock protocol.
    let page = unsafe {
        core::slice::from_raw_parts_mut(ctl.shared.pages.page_ptr(slotno), BLCKSZ)
    };
    let nread = vfs::pread(fd, page, offset as libc::off_t);
    if nread != BLCKSZ as isize {
        waitevent_seams::pgstat_report_wait_end::call();
        let en = if nread < 0 { current_errno() } else { 0 };
        file_seams::close_transient_file::call(fd);
        return Ok(Err(SlruIoError {
            cause: SLRU_READ_FAILED,
            errno: en,
        }));
    }
    waitevent_seams::pgstat_report_wait_end::call();

    if file_seams::close_transient_file::call(fd) != 0 {
        return Ok(Err(SlruIoError {
            cause: SLRU_CLOSE_FAILED,
            errno: current_errno(),
        }));
    }

    Ok(Ok(()))
}

fn SlruPhysicalWritePage(
    ctl: &SlruCtlData,
    pageno: i64,
    slotno: usize,
    fdata: Option<&mut SlruWriteAllData>,
) -> PgResult<Result<(), SlruIoError>> {
    let shared = &ctl.shared;
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;
    let mut fdata = fdata;

    pgstat_seams::pgstat_count_slru_page_written::call(shared.slru_stats_idx);

    // Write-WAL-before-data: flush through the page's largest group LSN.
    if shared.lsn_groups_per_page > 0 {
        let groups = shared.lsn_groups_per_page as usize;
        let lsnindex = slotno * groups;
        let mut max_lsn = shared.group_lsn.get(lsnindex);
        for lsnoff in 1..groups {
            let this_lsn = shared.group_lsn.get(lsnindex + lsnoff);
            if max_lsn < this_lsn {
                max_lsn = this_lsn;
            }
        }

        if max_lsn != InvalidXLogRecPtr {
            // A failing XLogFlush must PANIC; the crit section promotes it.
            globals::StartCriticalSection();
            transam_xlog_seams::xlog_flush::call(max_lsn)?;
            globals::EndCriticalSection();
        }
    }

    let mut fd: i32 = -1;
    if let Some(f) = fdata.as_deref_mut() {
        for i in 0..f.num_files {
            if f.segno[i] == segno {
                fd = f.fd[i];
                break;
            }
        }
    }

    if fd < 0 {
        // Redo may recreate truncated segments, possibly concurrently:
        // O_CREAT without O_EXCL/O_TRUNC.
        let path = SlruFileName(ctl, segno);
        fd = file_seams::open_transient_file::call(path.as_str(), libc::O_RDWR | libc::O_CREAT)?;
        if fd < 0 {
            return Ok(Err(SlruIoError {
                cause: SLRU_OPEN_FAILED,
                errno: current_errno(),
            }));
        }

        match fdata.as_deref_mut() {
            Some(f) if f.num_files < MAX_WRITEALL_BUFFERS => {
                f.fd[f.num_files] = fd;
                f.segno[f.num_files] = segno;
                f.num_files += 1;
            }
            Some(_) => {
                // past MAX_WRITEALL_BUFFERS: treat as a standalone write
                fdata = None;
            }
            None => {}
        }
    }

    set_errno(0);
    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_WRITE);
    // SAFETY: the slot's bytes are stable under the caller's buffer lock.
    let page = unsafe {
        core::slice::from_raw_parts(shared.pages.page_ptr(slotno).cast_const(), BLCKSZ)
    };
    let nwritten = vfs::pwrite(fd, page, offset as libc::off_t);
    if nwritten != BLCKSZ as isize {
        waitevent_seams::pgstat_report_wait_end::call();
        // If write didn't set errno, assume the problem is no disk space.
        let mut en = if nwritten < 0 { current_errno() } else { 0 };
        if en == 0 {
            en = libc::ENOSPC;
        }
        if fdata.is_none() {
            file_seams::close_transient_file::call(fd);
        }
        return Ok(Err(SlruIoError {
            cause: SLRU_WRITE_FAILED,
            errno: en,
        }));
    }
    waitevent_seams::pgstat_report_wait_end::call();

    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        let tag = FileTag::for_slru(ctl.sync_handler, segno as u64);
        if !sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_REQUEST, false)? {
            // No space to enqueue the sync request; do it synchronously.
            waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_SYNC);
            if file_seams::pg_fsync::call(fd) != 0 {
                waitevent_seams::pgstat_report_wait_end::call();
                let en = current_errno();
                file_seams::close_transient_file::call(fd);
                return Ok(Err(SlruIoError {
                    cause: SLRU_FSYNC_FAILED,
                    errno: en,
                }));
            }
            waitevent_seams::pgstat_report_wait_end::call();
        }
    }

    if fdata.is_none() && file_seams::close_transient_file::call(fd) != 0 {
        return Ok(Err(SlruIoError {
            cause: SLRU_CLOSE_FAILED,
            errno: current_errno(),
        }));
    }

    Ok(Ok(()))
}

#[cold]
fn SlruReportIOError<T>(
    ctl: &SlruCtlData,
    pageno: i64,
    xid: TransactionId,
    io_err: SlruIoError,
) -> PgResult<T> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;

    let path = SlruFileName(ctl, segno);
    let msg = format!("could not access status of transaction {xid}");
    let en = io_err.errno;
    let at = loc("SlruReportIOError");

    match io_err.cause {
        SLRU_OPEN_FAILED => ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not open file \"{path}\": %m."))
            .finish(at)?,
        SLRU_SEEK_FAILED => ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!(
                "Could not seek in file \"{path}\" to offset {offset}: %m."
            ))
            .finish(at)?,
        SLRU_READ_FAILED => {
            if en != 0 {
                ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not read from file \"{path}\" at offset {offset}: %m."
                    ))
                    .finish(at)?
            } else {
                ereport(ERROR)
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not read from file \"{path}\" at offset {offset}: read too few bytes."
                    ))
                    .finish(at)?
            }
        }
        SLRU_WRITE_FAILED => {
            if en != 0 {
                ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not write to file \"{path}\" at offset {offset}: %m."
                    ))
                    .finish(at)?
            } else {
                ereport(ERROR)
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not write to file \"{path}\" at offset {offset}: wrote too few bytes."
                    ))
                    .finish(at)?
            }
        }
        SLRU_FSYNC_FAILED => ereport(file_seams::data_sync_elevel::call(ERROR))
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not fsync file \"{path}\": %m."))
            .finish(at)?,
        SLRU_CLOSE_FAILED => ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not close file \"{path}\": %m."))
            .finish(at)?,
    }
    // Every cause reports at >= ERROR, so finish always returned Err above.
    unreachable!("SlruReportIOError reported below ERROR");
}

fn SlruRecentlyUsed(shared: &SlruSharedData, slotno: usize) {
    let bankno = SlotGetBankNumber(slotno);
    let cur = shared.bank_cur_lru_count[bankno].load(Ordering::Relaxed);

    debug_assert!(shared.page_status.get(slotno) != SLRU_PAGE_EMPTY);

    if cur != shared.page_lru_count[slotno].load(Ordering::Relaxed) {
        let new_count = cur.wrapping_add(1);
        shared.bank_cur_lru_count[bankno].store(new_count, Ordering::Relaxed);
        shared.page_lru_count[slotno].store(new_count, Ordering::Relaxed);
    }
}

fn SlruSelectLRUPage(ctl: &SlruCtlData, pageno: i64, bank: &mut LwGuard) -> PgResult<usize> {
    let shared = &ctl.shared;

    loop {
        let mut bestvalidslot = 0usize;
        let mut best_valid_delta = -1i32;
        let mut best_valid_page_number = 0i64;
        let mut bestinvalidslot = 0usize;
        let mut best_invalid_delta = -1i32;
        let mut best_invalid_page_number = 0i64;
        let bankno = (pageno % ctl.nbanks as i64) as usize;
        let bankstart = bankno * SLRU_BANK_SIZE as usize;
        let bankend = bankstart + SLRU_BANK_SIZE as usize;

        debug_assert!(bank.covers(SimpleLruGetBankLock(ctl, pageno)) && bank.is_held());

        for slotno in bankstart..bankend {
            if shared.page_status.get(slotno) != SLRU_PAGE_EMPTY
                && shared.page_number.get(slotno) == pageno
            {
                return Ok(slotno);
            }
        }

        // Prefer EMPTY, else the LRU valid page — never the latest page, and
        // an I/O-busy slot only as a last resort; ties break to the
        // PagePrecedes-oldest page. The increment pushes cur past every
        // page_lru_count so the next SlruRecentlyUsed re-marks its page.
        let cur_count = shared.bank_cur_lru_count[bankno].load(Ordering::Relaxed);
        shared.bank_cur_lru_count[bankno].store(cur_count.wrapping_add(1), Ordering::Relaxed);
        for slotno in bankstart..bankend {
            if shared.page_status.get(slotno) == SLRU_PAGE_EMPTY {
                return Ok(slotno);
            }

            let mut this_delta =
                cur_count.wrapping_sub(shared.page_lru_count[slotno].load(Ordering::Relaxed));
            if this_delta < 0 {
                // lost cur_count increments: back off the page count instead
                shared.page_lru_count[slotno].store(cur_count, Ordering::Relaxed);
                this_delta = 0;
            }

            let this_page_number = shared.page_number.get(slotno);
            if this_page_number == shared.latest_page_number.load(Ordering::Relaxed) as i64 {
                continue;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_VALID {
                if this_delta > best_valid_delta
                    || (this_delta == best_valid_delta
                        && (ctl.page_precedes())(this_page_number, best_valid_page_number))
                {
                    bestvalidslot = slotno;
                    best_valid_delta = this_delta;
                    best_valid_page_number = this_page_number;
                }
            } else if this_delta > best_invalid_delta
                || (this_delta == best_invalid_delta
                    && (ctl.page_precedes())(this_page_number, best_invalid_page_number))
            {
                bestinvalidslot = slotno;
                best_invalid_delta = this_delta;
                best_invalid_page_number = this_page_number;
            }
        }

        if best_valid_delta < 0 {
            // everything (except maybe the latest page) is I/O busy: wait
            SimpleLruWaitIO(ctl, bestinvalidslot, bank)?;
            continue;
        }

        if !shared.page_dirty.get(bestvalidslot) {
            return Ok(bestvalidslot);
        }

        // Write the victim, then loop: handles it being re-dirtied meanwhile.
        SlruInternalWritePage(ctl, bestvalidslot, None, bank)?;
    }
}

pub fn SimpleLruWriteAll(ctl: &SlruCtlData, allow_redirtied: bool) -> PgResult<()> {
    let shared = &ctl.shared;
    let mut pageno: i64 = 0;
    let mut prevbank = SlotGetBankNumber(0);

    pgstat_seams::pgstat_count_slru_flush::call(shared.slru_stats_idx);

    let mut fdata = SlruWriteAllData::new();

    let mut bank = LwGuard::acquire(&shared.bank_locks[prevbank].lock, LW_EXCLUSIVE)?;

    for slotno in 0..shared.num_slots as usize {
        let curbank = SlotGetBankNumber(slotno);

        if curbank != prevbank {
            bank.release()?;
            bank = LwGuard::acquire(&shared.bank_locks[curbank].lock, LW_EXCLUSIVE)?;
            prevbank = curbank;
        }

        if shared.page_status.get(slotno) == SLRU_PAGE_EMPTY {
            continue;
        }

        SlruInternalWritePage(ctl, slotno, Some(&mut fdata), &mut bank)?;

        // At a checkpoint another backend may have re-dirtied the slot.
        debug_assert!(
            allow_redirtied
                || shared.page_status.get(slotno) == SLRU_PAGE_EMPTY
                || (shared.page_status.get(slotno) == SLRU_PAGE_VALID
                    && !shared.page_dirty.get(slotno))
        );
    }

    bank.release()?;

    let mut ok = true;
    let mut close_errno = 0;
    for i in 0..fdata.num_files {
        if file_seams::close_transient_file::call(fdata.fd[i]) != 0 {
            close_errno = current_errno();
            pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
            ok = false;
        }
    }
    if !ok {
        return SlruReportIOError(
            ctl,
            pageno,
            InvalidTransactionId,
            SlruIoError {
                cause: SLRU_CLOSE_FAILED,
                errno: close_errno,
            },
        );
    }

    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        file_seams::fsync_fname::call(ctl.dir.as_str(), true)?;
    }
    Ok(())
}

pub fn SimpleLruTruncate(ctl: &SlruCtlData, cutoffPage: i64) -> PgResult<()> {
    let shared = &ctl.shared;

    pgstat_seams::pgstat_count_slru_truncate::call(shared.slru_stats_idx);

    'restart: loop {
        // Wraparound backstop: the endpoint page must never be truncatable.
        if (ctl.page_precedes())(
            shared.latest_page_number.load(Ordering::Relaxed) as i64,
            cutoffPage,
        ) {
            ereport(LOG)
                .errmsg(format!(
                    "could not truncate directory \"{}\": apparent wraparound",
                    ctl.dir
                ))
                .finish(loc("SimpleLruTruncate"))?;
            return Ok(());
        }

        let mut prevbank = SlotGetBankNumber(0);
        let mut bank = LwGuard::acquire(&shared.bank_locks[prevbank].lock, LW_EXCLUSIVE)?;
        for slotno in 0..shared.num_slots as usize {
            let curbank = SlotGetBankNumber(slotno);

            if curbank != prevbank {
                bank.release()?;
                bank = LwGuard::acquire(&shared.bank_locks[curbank].lock, LW_EXCLUSIVE)?;
                prevbank = curbank;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_EMPTY {
                continue;
            }
            if !(ctl.page_precedes())(shared.page_number.get(slotno), cutoffPage) {
                continue;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_VALID
                && !shared.page_dirty.get(slotno)
            {
                shared.page_status.set(slotno, SLRU_PAGE_EMPTY);
                continue;
            }

            // I/O may be acting on the page: settle it and restart.
            if shared.page_status.get(slotno) == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, None, &mut bank)?;
            } else {
                SimpleLruWaitIO(ctl, slotno, &mut bank)?;
            }

            bank.release()?;
            continue 'restart;
        }

        bank.release()?;
        break;
    }

    SlruScanDirectory(ctl, |ctl, filename, segpage| {
        SlruScanDirCbDeleteCutoff(ctl, filename, segpage, cutoffPage)
    })?;
    Ok(())
}

fn SlruInternalDeleteSegment(ctl: &SlruCtlData, segno: i64) -> PgResult<()> {
    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        let tag = FileTag::for_slru(ctl.sync_handler, segno as u64);
        sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_FORGET_REQUEST, true)?;
    }

    let path = SlruFileName(ctl, segno);
    ereport(DEBUG2)
        .errmsg_internal(format!("removing file \"{path}\""))
        .finish(loc("SlruInternalDeleteSegment"))?;
    // SAFETY: SlruPath keeps a NUL-terminated buffer (as_c_ptr invariant).
    vfs::unlink(unsafe { core::ffi::CStr::from_ptr(path.as_c_ptr().cast()) });
    Ok(())
}

pub fn SlruDeleteSegment(ctl: &SlruCtlData, segno: i64) -> PgResult<()> {
    let shared = &ctl.shared;
    let mut prevbank = SlotGetBankNumber(0);

    let mut bank = LwGuard::acquire(&shared.bank_locks[prevbank].lock, LW_EXCLUSIVE)?;
    loop {
        let mut did_write = false;
        for slotno in 0..shared.num_slots as usize {
            let curbank = SlotGetBankNumber(slotno);

            if curbank != prevbank {
                bank.release()?;
                bank = LwGuard::acquire(&shared.bank_locks[curbank].lock, LW_EXCLUSIVE)?;
                prevbank = curbank;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_EMPTY {
                continue;
            }

            let pagesegno = shared.page_number.get(slotno) / SLRU_PAGES_PER_SEGMENT;
            if pagesegno != segno {
                continue;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_VALID
                && !shared.page_dirty.get(slotno)
            {
                shared.page_status.set(slotno, SLRU_PAGE_EMPTY);
                continue;
            }

            if shared.page_status.get(slotno) == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, None, &mut bank)?;
            } else {
                SimpleLruWaitIO(ctl, slotno, &mut bank)?;
            }

            did_write = true;
        }

        // the I/O functions release the bank lock: re-check for new entries
        if !did_write {
            break;
        }
    }

    SlruInternalDeleteSegment(ctl, segno)?;

    bank.release()?;
    Ok(())
}

fn SlruMayDeleteSegment(ctl: &SlruCtlData, segpage: i64, cutoffPage: i64) -> bool {
    let seg_last_page = segpage + SLRU_PAGES_PER_SEGMENT - 1;

    debug_assert!(segpage % SLRU_PAGES_PER_SEGMENT == 0);

    (ctl.page_precedes())(segpage, cutoffPage) && (ctl.page_precedes())(seg_last_page, cutoffPage)
}

// transam.c parity, for the assert-only self-tests below.
fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    id1.wrapping_sub(id2) as i32 >= 0
}

fn SlruPagePrecedesTestOffset(ctl: &SlruCtlData, per_page: i32, offset: u32) {
    let precedes = ctl.page_precedes();
    let pp = per_page as u32;

    // Opposite-end XID pairs have undefined order (RFC 1982); each precedes
    // the other.
    let lhs: TransactionId = pp.wrapping_add(offset);
    let rhs: TransactionId = lhs.wrapping_add(1u32 << 31);
    debug_assert!(TransactionIdPrecedes(lhs, rhs));
    debug_assert!(TransactionIdPrecedes(rhs, lhs));
    debug_assert!(!TransactionIdPrecedes(lhs.wrapping_sub(1), rhs));
    debug_assert!(TransactionIdPrecedes(rhs, lhs.wrapping_sub(1)));
    debug_assert!(TransactionIdPrecedes(lhs.wrapping_add(1), rhs));
    debug_assert!(!TransactionIdPrecedes(rhs, lhs.wrapping_add(1)));
    debug_assert!(!TransactionIdFollowsOrEquals(lhs, rhs));
    debug_assert!(!TransactionIdFollowsOrEquals(rhs, lhs));

    // C divides uint32 XIDs by int per_page; i64 division reproduces it.
    let pp64 = per_page as i64;
    let page = |x: TransactionId| (x as i64) / pp64;
    debug_assert!(!precedes(page(lhs), page(lhs)));
    debug_assert!(!precedes(page(lhs), page(rhs)));
    debug_assert!(!precedes(page(rhs), page(lhs)));
    debug_assert!(!precedes(page(lhs.wrapping_sub(pp)), page(rhs)));
    debug_assert!(precedes(page(rhs), page(lhs.wrapping_sub(3u32.wrapping_mul(pp)))));
    debug_assert!(precedes(page(rhs), page(lhs.wrapping_sub(2u32.wrapping_mul(pp)))));
    debug_assert!(precedes(page(rhs), page(lhs.wrapping_sub(pp))) || (1u32 << 31) % pp != 0);
    debug_assert!(precedes(page(lhs.wrapping_add(pp)), page(rhs)) || (1u32 << 31) % pp != 0);
    debug_assert!(precedes(page(lhs.wrapping_add(2u32.wrapping_mul(pp))), page(rhs)));
    debug_assert!(precedes(page(lhs.wrapping_add(3u32.wrapping_mul(pp))), page(rhs)));
    debug_assert!(!precedes(page(rhs), page(lhs.wrapping_add(pp))));

    // the last safely assignable XID lives in the LAST page of the second
    // segment, which must never be deletable...
    let mut newestPage: i64 = 2 * SLRU_PAGES_PER_SEGMENT - 1;
    let mut newestXact: TransactionId = (newestPage as u32).wrapping_mul(pp).wrapping_add(offset);
    debug_assert!((newestXact as i64) / pp64 == newestPage);
    let mut oldestXact: TransactionId = newestXact.wrapping_add(1);
    oldestXact = oldestXact.wrapping_sub(1u32 << 31);
    let mut oldestPage: i64 = (oldestXact as i64) / pp64;
    debug_assert!(!SlruMayDeleteSegment(
        ctl,
        newestPage - newestPage % SLRU_PAGES_PER_SEGMENT,
        oldestPage
    ));

    // ...and likewise in the FIRST page of the second segment.
    newestPage = SLRU_PAGES_PER_SEGMENT;
    newestXact = (newestPage as u32).wrapping_mul(pp).wrapping_add(offset);
    debug_assert!((newestXact as i64) / pp64 == newestPage);
    oldestXact = newestXact.wrapping_add(1);
    oldestXact = oldestXact.wrapping_sub(1u32 << 31);
    oldestPage = (oldestXact as i64) / pp64;
    debug_assert!(!SlruMayDeleteSegment(
        ctl,
        newestPage - newestPage % SLRU_PAGES_PER_SEGMENT,
        oldestPage
    ));
}

pub fn SlruPagePrecedesUnitTests(ctl: &SlruCtlData, per_page: i32) {
    if cfg!(debug_assertions) {
        SlruPagePrecedesTestOffset(ctl, per_page, 0);
        SlruPagePrecedesTestOffset(ctl, per_page, (per_page / 2) as u32);
        SlruPagePrecedesTestOffset(ctl, per_page, (per_page - 1) as u32);
    }
}

pub fn SlruScanDirCbReportPresence(
    ctl: &SlruCtlData,
    _filename: &str,
    segpage: i64,
    cutoff_page: i64,
) -> PgResult<bool> {
    Ok(SlruMayDeleteSegment(ctl, segpage, cutoff_page))
}

fn SlruScanDirCbDeleteCutoff(
    ctl: &SlruCtlData,
    _filename: &str,
    segpage: i64,
    cutoff_page: i64,
) -> PgResult<bool> {
    if SlruMayDeleteSegment(ctl, segpage, cutoff_page) {
        SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT)?;
    }
    Ok(false)
}

pub fn SlruScanDirCbDeleteAll(ctl: &SlruCtlData, _filename: &str, segpage: i64) -> PgResult<bool> {
    SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT)?;
    Ok(false)
}

fn SlruCorrectSegmentFilenameLength(ctl: &SlruCtlData, len: usize) -> bool {
    if ctl.long_segment_names {
        len == 15
    } else {
        // Commit 638cf09e76d allowed 5-character names; 73c986adde5 allowed 6.
        len == 4 || len == 5 || len == 6
    }
}

pub fn SlruScanDirectory(
    ctl: &SlruCtlData,
    mut callback: impl FnMut(&SlruCtlData, &str, i64) -> PgResult<bool>,
) -> PgResult<bool> {
    file_seams::with_allocated_dir::call(ctl.dir.as_str(), &mut |d_name| {
        if SlruCorrectSegmentFilenameLength(ctl, d_name.len())
            && d_name
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'A'..=b'F'))
        {
            let segno = i64::from_str_radix(d_name, 16)
                .expect("checked hex SLRU segment name failed to parse");
            let segpage = segno * SLRU_PAGES_PER_SEGMENT;

            ::elog::elog(
                DEBUG2,
                format!("SlruScanDirectory invoking callback on {}/{}", ctl.dir, d_name),
            )?;
            return callback(ctl, d_name, segpage);
        }
        Ok(false)
    })
}

pub fn SlruSyncFileTag(ctl: &SlruCtlData, ftag: &FileTag) -> PgResult<(i32, SlruPath)> {
    let path = SlruFileName(ctl, ftag.segno as i64);

    let fd = file_seams::open_transient_file::call(path.as_str(), libc::O_RDWR)?;
    if fd < 0 {
        return Ok((-1, path));
    }

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_FLUSH_SYNC);
    let result = file_seams::pg_fsync::call(fd);
    waitevent_seams::pgstat_report_wait_end::call();
    let save_errno = current_errno();

    file_seams::close_transient_file::call(fd);

    set_errno(save_errno);
    Ok((result, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_after_crash_restores_boot_image() {
        shmem_seams::shmem_init_struct::set(|_, size| {
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            // SAFETY: non-zero size; leaked for the test-process lifetime.
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            Ok((p, false))
        });
        pgstat_seams::pgstat_get_slru_index::set(|_| 0);

        let nslots = 64;
        let nlsns = 2;
        let ctl = SimpleLruInit(
            "reset_test",
            nslots,
            nlsns,
            "pg_reset_test",
            100,
            101,
            SyncRequestHandler::SYNC_HANDLER_NONE,
            false,
        )
        .unwrap();

        let sh = &ctl.shared;
        sh.page_status.set(3, SLRU_PAGE_VALID);
        sh.page_dirty.set(3, true);
        sh.page_number.set(3, 777);
        sh.page_lru_count[3].store(9, Ordering::Relaxed);
        sh.bank_cur_lru_count[0].store(9, Ordering::Relaxed);
        sh.group_lsn.set(5, 0xDEAD);
        sh.latest_page_number.store(42, Ordering::Relaxed);
        sh.buffer_locks[3].lock.state.store(5, Ordering::Relaxed);
        sh.bank_locks[0].lock.state.store(5, Ordering::Relaxed);
        // SAFETY: single-threaded test, exclusive access.
        unsafe { *sh.pages.page_ptr(3) = 0xAB };

        SimpleLruResetAfterCrash(&ctl);

        assert_eq!(sh.page_status.get(3), SLRU_PAGE_EMPTY);
        assert!(!sh.page_dirty.get(3));
        assert_eq!(sh.page_number.get(3), 0);
        assert_eq!(sh.page_lru_count[3].load(Ordering::Relaxed), 0);
        assert_eq!(sh.bank_cur_lru_count[0].load(Ordering::Relaxed), 0);
        assert_eq!(sh.group_lsn.get(5), InvalidXLogRecPtr);
        assert_eq!(sh.latest_page_number.load(Ordering::Relaxed), 0);
        assert_eq!(
            sh.buffer_locks[3].lock.state.load(Ordering::Relaxed),
            lwlock::LW_FLAG_RELEASE_OK
        );
        assert_eq!(
            sh.bank_locks[0].lock.state.load(Ordering::Relaxed),
            lwlock::LW_FLAG_RELEASE_OK
        );
        // SAFETY: as above.
        assert_eq!(unsafe { *sh.pages.page_ptr(3) }, 0);
        for b in 0..(nslots / SLRU_BANK_SIZE) as usize {
            assert_eq!(
                // SAFETY: as above.
                unsafe { *sh.bank_locks[b].lock.waiters.get() },
                lmgr_proc_seams::proclist_head {
                    head: types_core::INVALID_PROC_NUMBER,
                    tail: types_core::INVALID_PROC_NUMBER,
                }
            );
        }
    }
}
