//! `backend-access-transam-slru` — Simple LRU buffering for
//! wrap-around-able permanent metadata (`src/backend/access/transam/slru.c`).
//!
//! SLRU maintains transaction status indexed by TransactionId (commit status,
//! parent xids, commit timestamps), multixact storage, serializable isolation
//! locks, and NOTIFY traffic. A pool of shared page buffers is split in banks
//! by the low bits of the page number; per-bank control LWLocks protect the
//! shared state and per-buffer LWLocks synchronize I/O, exactly as in C. The
//! locks live inside [`SlruSharedData`] and are driven through the
//! `backend-storage-lmgr-lwlock` crate directly.
//!
//! In C the per-slot arrays are carved out of one `ShmemInitStruct` block; in
//! the owned-tree model (same as `CreateLWLocks`'s `LWLockTable`) the arrays
//! are owned `Vec`s sized once at [`SimpleLruInit`], reserved fallibly so the
//! shmem-allocation failure surface stays an error. There is no attach
//! branch: a backend always initializes its tree.
//!
//! Lock discipline note: this module deliberately mirrors slru.c's
//! release/re-acquire protocol with bare `LWLockAcquire`/`LWLockRelease`
//! calls (no Drop guards): the C contract has callers entering with the bank
//! lock held and several functions returning with it held, and an
//! `ereport(ERROR)` path unwinds with locks held, to be released by the
//! transaction-abort `LWLockReleaseAll` — the same recovery path this port's
//! `Err` propagation relies on. Recorded in DESIGN_DEBT.md.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::ffi::CString;

use transam::{TransactionIdFollowsOrEquals, TransactionIdPrecedes};
use transam_xlog_seams as xlog_seams;
use xlogrecovery_seams as xlogrecovery_seams;
use file_seams as file_seams;
use ipc_shmem_seams as ipc_shmem;
use lwlock::{
    LWLockAcquire, LWLockConditionalAcquire, LWLockHeldByMe, LWLockHeldByMeInMode,
    LWLockInitialize, LWLockRelease,
};
use sync_seams as sync_seams;
use ::activity_small::pgstat_checkpointer::with_pending_checkpointer_stats;
use stat_seams as stat_seams;
use waitevent_seams as waitevent_seams;
use ::utils_error::errno::current_errno;
use utils_error::{config, ereport, PgResult};
use init_small_seams as globals;
use types_core::{
    InvalidTransactionId, InvalidXLogRecPtr, Size, TransactionId, XLogRecPtr, BLCKSZ,
};
use types_error::{ErrorLocation, DEBUG2, ERROR, LOG};
use ::types_pgstat::wait_event::{
    WAIT_EVENT_SLRU_FLUSH_SYNC, WAIT_EVENT_SLRU_READ, WAIT_EVENT_SLRU_SYNC, WAIT_EVENT_SLRU_WRITE,
};
use ::types_storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};
use types_storage::{
    pg_atomic_uint64, LWLock, LWLockPadded, LW_EXCLUSIVE, LW_SHARED, LWLOCK_PADDED_SIZE,
};

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("slru.c", 0, funcname)
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
#[cfg(not(target_os = "macos"))]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
fn set_errno(value: i32) {
    unsafe {
        *errno_location() = value;
    }
}

// ---------------------------------------------------------------------------
// slru.h constants
// ---------------------------------------------------------------------------

/// `SLRU_MAX_ALLOWED_BUFFERS` (slru.h) — buffer-count cap that keeps internal
/// arithmetic from overflowing.
pub const SLRU_MAX_ALLOWED_BUFFERS: i32 = ((1024 * 1024 * 1024) / BLCKSZ) as i32;

/// `SLRU_PAGES_PER_SEGMENT` (slru.h).
pub const SLRU_PAGES_PER_SEGMENT: i64 = 32;

/// `SLRU_BANK_BITSHIFT` / `SLRU_BANK_SIZE` (slru.c) — pages are assigned a
/// bank by page number; 16 buffers per bank keeps LRU victim search fast.
pub const SLRU_BANK_BITSHIFT: u32 = 4;
pub const SLRU_BANK_SIZE: i32 = 1 << SLRU_BANK_BITSHIFT;

/// `SlotGetBankNumber(slotno)` (slru.c).
#[inline]
fn SlotGetBankNumber(slotno: usize) -> usize {
    slotno >> SLRU_BANK_BITSHIFT
}

/// `MAX_WRITEALL_BUFFERS` (slru.c) — files kept open across one
/// `SimpleLruWriteAll`.
const MAX_WRITEALL_BUFFERS: usize = 16;

/// `SlruPageStatus` (slru.h). The "dirty" bit is separate; `page_dirty` can
/// be true only in the VALID or WRITE_IN_PROGRESS states.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SlruPageStatus {
    /// buffer is not in use
    #[default]
    SLRU_PAGE_EMPTY = 0,
    /// page is being read in
    SLRU_PAGE_READ_IN_PROGRESS = 1,
    /// page is valid and not being written
    SLRU_PAGE_VALID = 2,
    /// page is being written out
    SLRU_PAGE_WRITE_IN_PROGRESS = 3,
}
pub use SlruPageStatus::*;

/// A `PagePrecedes` callback (`bool (*PagePrecedes)(int64, int64)`,
/// slru.h) — page ordering in the wrapping XID/multixact space, installed by
/// each SLRU client after `SimpleLruInit`.
pub type SlruPagePrecedes = fn(i64, i64) -> bool;

// ---------------------------------------------------------------------------
// ShmemSlice — a fixed-length array carved out of the MAP_SHARED segment.
//
// In C, every SlruSharedData array (page_buffer[], page_status[], …) is a
// pointer into one `ShmemInitStruct` block; the block lives in the shared
// segment that every forked backend inherits (same address, MAP_SHARED), so a
// page a backend writes is immediately visible to a sibling backend's status
// lookup. The owned-`Vec` model placed those arrays on the *process-private*
// heap, so cross-backend commit visibility silently broke under fork.
//
// `ShmemSlice<T>` restores the C contract: a bare `*mut T` + `len` into the
// shared block, dereferencing to `&[T]`/`&mut [T]` so all the existing index /
// slice / fill / iterate call sites keep working unchanged. It is `Default`
// (null, empty) so the unit-test path can build an unbacked `SlruSharedData`.
// ---------------------------------------------------------------------------

/// A fixed-length slice into the shared-memory segment (the owned stand-in for
/// a C `T *` pointing into a `ShmemInitStruct` block). Synchronization is the
/// caller's responsibility, exactly as in slru.c (bank/buffer LWLocks).
#[derive(Debug)]
pub struct ShmemSlice<T> {
    ptr: *mut T,
    len: usize,
}

impl<T> Default for ShmemSlice<T> {
    fn default() -> Self {
        ShmemSlice {
            ptr: core::ptr::null_mut(),
            len: 0,
        }
    }
}

impl<T> ShmemSlice<T> {
    /// Wrap a region of `len` `T`s starting at `ptr` (which must point into the
    /// live shared segment and stay valid for the cluster's lifetime — the C
    /// invariant for an SLRU shmem array).
    ///
    /// # Safety
    /// `ptr` must be non-null, well-aligned for `T`, and own `len` initialized
    /// `T`s in the MAP_SHARED segment.
    unsafe fn from_raw(ptr: *mut T, len: usize) -> Self {
        ShmemSlice { ptr, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<T> core::ops::Deref for ShmemSlice<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: `from_raw`'s invariant — `ptr` owns `len` initialized `T`s in
        // the shared segment, valid for the cluster lifetime.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T> core::ops::DerefMut for ShmemSlice<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        if self.len == 0 {
            return &mut [];
        }
        // SAFETY: as `deref`; the bank/buffer LWLocks serialize cross-backend
        // mutation exactly as slru.c requires.
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

// SAFETY: `ShmemSlice` is a bare pointer into the process-shared segment; it is
// no less `Send`/`Sync` than the raw arrays C threads through SLRU shared
// state, whose access discipline is the SLRU bank/buffer LWLocks.
unsafe impl<T: Send> Send for ShmemSlice<T> {}
unsafe impl<T: Sync> Sync for ShmemSlice<T> {}

// ---------------------------------------------------------------------------
// Shared-memory state (slru.h SlruSharedData)
// ---------------------------------------------------------------------------

/// `SlruSharedData` (slru.h). Bank locks protect every field except
/// `latest_page_number`, which uses atomics. The C struct carries pointers
/// into one shmem block; here the arrays are owned.
#[derive(Debug, Default)]
pub struct SlruSharedData {
    /// Number of buffers managed by this SLRU structure.
    pub num_slots: i32,

    /// `page_buffer[slotno]` — flattened: slot `i` occupies bytes
    /// `i * BLCKSZ .. (i + 1) * BLCKSZ`. Access via
    /// [`SlruSharedData::page_buffer`] / [`SlruSharedData::page_buffer_mut`].
    /// Lives in the MAP_SHARED segment so a page one backend dirties is visible
    /// to a sibling's status lookup (the cross-connection commit-visibility
    /// invariant).
    page_buffer_bytes: ShmemSlice<u8>,
    /// Page number is undefined when status is EMPTY, as is `page_lru_count`.
    pub page_status: ShmemSlice<SlruPageStatus>,
    pub page_dirty: ShmemSlice<bool>,
    pub page_number: ShmemSlice<i64>,
    pub page_lru_count: ShmemSlice<i32>,

    /// The buffer_locks protect the I/O on each buffer slot. In shmem so the
    /// LWLocks actually serialize across forked backends.
    pub buffer_locks: ShmemSlice<LWLockPadded>,
    /// Locks to protect the in-memory buffer-slot access, one per bank.
    pub bank_locks: ShmemSlice<LWLockPadded>,

    /// A bank-wise LRU counter: a page is marked "most recently used" by
    /// `page_lru_count[slotno] = ++bank_cur_lru_count[bankno]`; the oldest
    /// page in a bank has the highest `bank_cur_lru_count - page_lru_count`.
    /// Wraps, which is fine while no page's age exceeds INT_MAX counts.
    pub bank_cur_lru_count: ShmemSlice<i32>,

    /// Optional WAL flush LSNs (`lsn_groups_per_page` entries per slot); if
    /// non-empty we must flush WAL before writing pages (pg_xact only).
    pub group_lsn: ShmemSlice<XLogRecPtr>,
    pub lsn_groups_per_page: i32,

    /// Page number of the current end of the log; used only to avoid
    /// swapping out the latest page (and for multixact/offsets replay).
    pub latest_page_number: pg_atomic_uint64,

    /// SLRU's index for statistics purposes (might not be unique).
    pub slru_stats_idx: i32,
}

impl SlruSharedData {
    /// `shared->page_buffer[slotno]` — the slot's `BLCKSZ`-byte page image.
    pub fn page_buffer(&self, slotno: usize) -> &[u8] {
        &self.page_buffer_bytes[slotno * BLCKSZ..(slotno + 1) * BLCKSZ]
    }

    pub fn page_buffer_mut(&mut self, slotno: usize) -> &mut [u8] {
        &mut self.page_buffer_bytes[slotno * BLCKSZ..(slotno + 1) * BLCKSZ]
    }
}

/// `SlruCtlData` (slru.h) — the unshared control structure.
#[derive(Debug)]
pub struct SlruCtlData {
    pub shared: SlruSharedData,

    /// Number of banks in this SLRU.
    pub nbanks: u16,

    /// If true, use long (15-character) segment file names; see
    /// [`SlruFileName`].
    pub long_segment_names: bool,

    /// Which sync handler function to use when handing off the fsync work to
    /// the checkpointer (`SYNC_HANDLER_NONE` if direct fsync).
    pub sync_handler: SyncRequestHandler,

    /// Decide whether a page is "older" for truncation purposes. Installed
    /// by the client after `SimpleLruInit`; calling the LRU/truncate paths
    /// without it set is the C null-pointer call, here a panic.
    pub PagePrecedes: Option<SlruPagePrecedes>,

    /// Dir is the directory where the log segments are stored. In C a fixed
    /// 64-byte in-struct buffer filled by `strlcpy` (no palloc).
    pub Dir: String,
}

impl SlruCtlData {
    fn page_precedes(&self) -> SlruPagePrecedes {
        self.PagePrecedes
            .expect("SLRU PagePrecedes callback not installed (null function pointer call in C)")
    }
}

// ---------------------------------------------------------------------------
// Saved info for SlruReportIOError (C statics slru_errcause / slru_errno,
// a result channel between the physical I/O routines and the reporter;
// carried by value here instead of file statics)
// ---------------------------------------------------------------------------

/// `SlruErrorCause` (slru.c).
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

#[derive(Clone, Copy, Debug)]
struct SlruIoError {
    cause: SlruErrorCause,
    /// The saved C `errno` (0 for the short-read/short-write cases that
    /// report "too few bytes" without `%m`).
    errno: i32,
}

// ---------------------------------------------------------------------------
// Initialization of shared memory
// ---------------------------------------------------------------------------

/// `sizeof(SlruSharedData)` on a 64-bit target, for the C shmem accounting:
/// int num_slots (4+4 pad) + 9 pointers (72) + int lsn_groups_per_page (4+4
/// pad) + pg_atomic_uint64 latest_page_number (8) + int slru_stats_idx (4) +
/// trailing pad to 8 = 104.
const SLRU_SHARED_DATA_SIZE: usize = 104;

fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

fn bufferalign(len: usize) -> usize {
    (len + 31) & !31
}

/// Align up to `LWLOCK_PADDED_SIZE` (128). `LWLockPadded` is `#[repr(align(128))]`,
/// so its in-block offset must be 128-aligned: unlike C (which casts a possibly
/// unaligned pointer, tolerated as UB), Rust's slice construction strictly
/// rejects a misaligned base. The block start is already cacheline-aligned by
/// `ShmemAllocRaw`, so aligning the offset suffices.
fn lwlockalign(len: usize) -> usize {
    (len + (LWLOCK_PADDED_SIZE - 1)) & !(LWLOCK_PADDED_SIZE - 1)
}

/// `SimpleLruShmemSize(nslots, nlsns)` — the shared-memory footprint of one
/// SLRU, mirroring the C accumulation per array.
pub fn SimpleLruShmemSize(nslots: i32, nlsns: i32) -> Size {
    let nbanks = (nslots / SLRU_BANK_SIZE) as usize;
    debug_assert!(nslots <= SLRU_MAX_ALLOWED_BUFFERS);
    debug_assert!(nslots % SLRU_BANK_SIZE == 0);
    let nslots = nslots as usize;

    // we assume nslots isn't so large as to risk overflow
    let mut sz = maxalign(SLRU_SHARED_DATA_SIZE);
    sz += maxalign(nslots * core::mem::size_of::<*const u8>()); /* page_buffer[] */
    sz += maxalign(nslots * core::mem::size_of::<i32>()); /* page_status[] */
    sz += maxalign(nslots * core::mem::size_of::<bool>()); /* page_dirty[] */
    sz += maxalign(nslots * core::mem::size_of::<i64>()); /* page_number[] */
    sz += maxalign(nslots * core::mem::size_of::<i32>()); /* page_lru_count[] */
    sz = lwlockalign(sz); /* align buffer_locks[] to 128 (LWLockPadded) */
    sz += maxalign(nslots * LWLOCK_PADDED_SIZE); /* buffer_locks[] */
    sz = lwlockalign(sz); /* align bank_locks[] to 128 (LWLockPadded) */
    sz += maxalign(nbanks * LWLOCK_PADDED_SIZE); /* bank_locks[] */
    sz += maxalign(nbanks * core::mem::size_of::<i32>()); /* bank_cur_lru_count[] */

    if nlsns > 0 {
        sz += maxalign(nslots * nlsns as usize * core::mem::size_of::<XLogRecPtr>()); /* group_lsn[] */
    }

    bufferalign(sz) + BLCKSZ * nslots
}

/// `SimpleLruAutotuneBuffers(divisor, max)` — `NBuffers / divisor` capped at
/// `max`, always at least `SLRU_BANK_SIZE`, rounded down to a bank multiple.
pub fn SimpleLruAutotuneBuffers(divisor: i32, max: i32) -> i32 {
    let nbuffers = globals::nbuffers::call();
    (max - max % SLRU_BANK_SIZE)
        .min(SLRU_BANK_SIZE.max(nbuffers / divisor - (nbuffers / divisor) % SLRU_BANK_SIZE))
}

/// `SimpleLruInit(ctl, name, nslots, nlsns, subdir, buffer_tranche_id,
/// bank_tranche_id, sync_handler, long_segment_names)` — initialize a simple
/// LRU cache. Like C, this creates (in the postmaster) or attaches to (in a
/// forked backend) one `ShmemInitStruct` block holding every shared array, so
/// pages, page status, the LRU state and the bank/buffer LWLocks live in the
/// MAP_SHARED segment and a commit written by one backend is visible to a
/// sibling's status lookup. The per-process control struct holds slice views
/// into that block (the C `*` pointers). The caller sets `PagePrecedes`.
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

    let nslots_u = nslots as usize;
    let nbanks_u = nbanks as usize;
    let nlsns_u = nlsns.max(0) as usize;

    // Create-or-attach the shared block (C: ShmemInitStruct(name, …, &found)).
    // The size must match SimpleLruShmemSize so the shmem-index entry-size
    // check passes for every backend that attaches.
    let shmem_size = SimpleLruShmemSize(nslots, nlsns);
    let (base, found) = ipc_shmem::shmem_init_struct::call(name, shmem_size)?;

    // Carve each array out of the block at the same MAXALIGNed offsets that
    // SimpleLruShmemSize accumulates (header first, then each array, then the
    // BUFFERALIGNed page buffer). `found` ⇒ a sibling already initialized the
    // bytes; we only re-derive the slice views.
    let mut off = maxalign(SLRU_SHARED_DATA_SIZE);

    // C lays page_buffer[] (an array of `nslots` pointers) here and the actual
    // page bytes at the BUFFERALIGNed tail. We index the tail directly via
    // page_buffer_bytes, so this region is reserved padding kept only to hold
    // the layout offsets identical to C / SimpleLruShmemSize.
    off += maxalign(nslots_u * core::mem::size_of::<*const u8>()); // page_buffer[] ptr array
    let page_status_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<SlruPageStatus>());
    let page_dirty_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<bool>());
    let page_number_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<i64>());
    let page_lru_count_off = off;
    off += maxalign(nslots_u * core::mem::size_of::<i32>());
    off = lwlockalign(off); // buffer_locks[] base must be 128-aligned
    let buffer_locks_off = off;
    off += maxalign(nslots_u * LWLOCK_PADDED_SIZE);
    off = lwlockalign(off); // bank_locks[] base must be 128-aligned
    let bank_locks_off = off;
    off += maxalign(nbanks_u * LWLOCK_PADDED_SIZE);
    let bank_cur_lru_count_off = off;
    off += maxalign(nbanks_u * core::mem::size_of::<i32>());
    let group_lsn_off = off;
    if nlsns_u > 0 {
        off += maxalign(nslots_u * nlsns_u * core::mem::size_of::<XLogRecPtr>());
    }
    // Page bytes live at the BUFFERALIGNed tail.
    let page_buffer_off = bufferalign(off);

    // SAFETY: every offset is within `shmem_size` bytes of `base`, which
    // `shmem_init_struct` returned as a live region of exactly that size in the
    // shared segment, and each region is sized/aligned for its element type
    // (MAXALIGN ≥ align_of for all these scalars and 8-byte-aligned padded
    // LWLocks). `from_raw`'s remaining obligation (initialized contents) is met
    // either by the `!found` zero-init below or by the sibling that created it.
    let (page_status, page_dirty, page_number, page_lru_count, buffer_locks, bank_locks,
         bank_cur_lru_count, group_lsn, page_buffer_bytes) = unsafe {
        (
            ShmemSlice::from_raw(base.add(page_status_off).cast::<SlruPageStatus>(), nslots_u),
            ShmemSlice::from_raw(base.add(page_dirty_off).cast::<bool>(), nslots_u),
            ShmemSlice::from_raw(base.add(page_number_off).cast::<i64>(), nslots_u),
            ShmemSlice::from_raw(base.add(page_lru_count_off).cast::<i32>(), nslots_u),
            ShmemSlice::from_raw(base.add(buffer_locks_off).cast::<LWLockPadded>(), nslots_u),
            ShmemSlice::from_raw(base.add(bank_locks_off).cast::<LWLockPadded>(), nbanks_u),
            ShmemSlice::from_raw(base.add(bank_cur_lru_count_off).cast::<i32>(), nbanks_u),
            ShmemSlice::from_raw(
                base.add(group_lsn_off).cast::<XLogRecPtr>(),
                if nlsns_u > 0 { nslots_u * nlsns_u } else { 0 },
            ),
            ShmemSlice::from_raw(base.add(page_buffer_off).cast::<u8>(), nslots_u * BLCKSZ),
        )
    };

    let mut shared = SlruSharedData {
        num_slots: nslots,
        lsn_groups_per_page: nlsns,
        latest_page_number: pg_atomic_uint64::new(0),
        slru_stats_idx: stat_seams::pgstat_get_slru_index::call(name),
        page_buffer_bytes,
        page_status,
        page_dirty,
        page_number,
        page_lru_count,
        buffer_locks,
        bank_locks,
        bank_cur_lru_count,
        group_lsn,
    };

    // Only the creator initializes the shared bytes; attachers inherit the
    // already-initialized block (C: the `!found` branch in SimpleLruInit).
    if !found {
        shared.page_buffer_bytes.fill(0u8);
        shared.page_status.fill(SLRU_PAGE_EMPTY);
        shared.page_dirty.fill(false);
        shared.page_number.fill(0i64);
        shared.page_lru_count.fill(0i32);
        shared.bank_cur_lru_count.fill(0i32);
        if nlsns_u > 0 {
            shared.group_lsn.fill(InvalidXLogRecPtr);
        }

        // Initialize LWLocks (buffer locks per slot, bank locks per bank).
        for slotno in 0..nslots_u {
            LWLockInitialize(&mut shared.buffer_locks[slotno].lock, buffer_tranche_id);
        }
        for bankno in 0..nbanks_u {
            LWLockInitialize(&mut shared.bank_locks[bankno].lock, bank_tranche_id);
        }
    }

    // Initialize the unshared control struct, including directory path. We
    // assume caller will set PagePrecedes.
    Ok(SlruCtlData {
        shared,
        nbanks: nbanks as u16,
        long_segment_names,
        sync_handler,
        PagePrecedes: None,
        Dir: subdir.to_owned(),
    })
}

/// `check_slru_buffers(name, newval)` — GUC check-hook helper: valid values
/// are multiples of `SLRU_BANK_SIZE`. The second element carries the
/// `GUC_check_errdetail` text when the check fails.
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

// ---------------------------------------------------------------------------
// Filename / bank helpers
// ---------------------------------------------------------------------------

/// `SlruFileName(ctl, path, segno)` (slru.c) — segment number to filename.
/// Long names are 15 hex digits (`[0, 2^60-1]`); short names are 4-6 hex
/// digits (`[0, 2^24-1]`); see `SlruCorrectSegmentFilenameLength`.
pub fn SlruFileName(ctl: &SlruCtlData, segno: i64) -> String {
    if ctl.long_segment_names {
        // 15 characters, not 16, so SLRU segments stay distinguishable from
        // WAL segments (and SLRU_PAGES_PER_SEGMENT can't easily shrink).
        debug_assert!((0..=0x0FFF_FFFF_FFFF_FFFF).contains(&segno));
        format!("{}/{:015X}", ctl.Dir, segno)
    } else {
        // Despite the %04X format, up to 24-bit numbers are allowed.
        debug_assert!((0..=0xFF_FFFF).contains(&segno));
        format!("{}/{:04X}", ctl.Dir, segno as u32)
    }
}

/// `SimpleLruGetBankLock(ctl, pageno)` (slru.h) — the bank lock covering the
/// given page.
pub fn SimpleLruGetBankLock(ctl: &SlruCtlData, pageno: i64) -> &LWLock {
    let bankno = (pageno % ctl.nbanks as i64) as usize;
    &ctl.shared.bank_locks[bankno].lock
}

// ---------------------------------------------------------------------------
// Zero page
// ---------------------------------------------------------------------------

/// `SimpleLruZeroPage(ctl, pageno)` — initialize (or reinitialize) a page to
/// zeroes, in shared memory only. Returns the slot number. Bank lock must be
/// held at entry, and will be held at exit.
pub fn SimpleLruZeroPage(ctl: &mut SlruCtlData, pageno: i64) -> PgResult<usize> {
    debug_assert!(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(ctl, pageno),
        LW_EXCLUSIVE
    ));

    // Find a suitable buffer slot for the page.
    let slotno = SlruSelectLRUPage(ctl, pageno)?;
    debug_assert!(
        ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY
            || (ctl.shared.page_status[slotno] == SLRU_PAGE_VALID
                && !ctl.shared.page_dirty[slotno])
            || ctl.shared.page_number[slotno] == pageno
    );

    // Mark the slot as containing this page.
    ctl.shared.page_number[slotno] = pageno;
    ctl.shared.page_status[slotno] = SLRU_PAGE_VALID;
    ctl.shared.page_dirty[slotno] = true;
    SlruRecentlyUsed(&mut ctl.shared, slotno);

    // Set the buffer to zeroes.
    ctl.shared.page_buffer_mut(slotno).fill(0);

    // Set the LSNs for this new page to zero.
    SimpleLruZeroLSNs(ctl, slotno);

    // Assume this page is now the latest active page. Both this routine and
    // SlruSelectLRUPage run with the bank lock held, so this cannot be
    // zeroing a page SlruSelectLRUPage is about to evict; no memory barrier.
    ctl.shared.latest_page_number.write(pageno as u64);

    stat_seams::pgstat_count_slru_page_zeroed::call(ctl.shared.slru_stats_idx);

    Ok(slotno)
}

/// `SimpleLruZeroLSNs(ctl, slotno)` — zero all LSNs stored for an slru page;
/// called when creating a new page and when reading a page in from disk
/// (such an old page cannot have any interesting LSNs). Assumes
/// `InvalidXLogRecPtr` is bitwise-all-0.
fn SimpleLruZeroLSNs(ctl: &mut SlruCtlData, slotno: usize) {
    let groups = ctl.shared.lsn_groups_per_page as usize;
    if groups > 0 {
        ctl.shared.group_lsn[slotno * groups..(slotno + 1) * groups].fill(InvalidXLogRecPtr);
    }
}

// ---------------------------------------------------------------------------
// Wait for I/O
// ---------------------------------------------------------------------------

/// `SimpleLruWaitIO(ctl, slotno)` — wait for any active I/O on a page slot to
/// finish. (New I/O may have started before we return; the slot might not
/// even contain the same page anymore.) Bank lock must be held at entry, and
/// will be held at exit.
fn SimpleLruWaitIO(ctl: &mut SlruCtlData, slotno: usize) -> PgResult<()> {
    let bankno = SlotGetBankNumber(slotno);

    debug_assert!(ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY);

    // See notes at top of slru.c.
    LWLockRelease(&ctl.shared.bank_locks[bankno].lock)?;
    LWLockAcquire(&ctl.shared.buffer_locks[slotno].lock, LW_SHARED, globals::my_proc_number::call())?;
    LWLockRelease(&ctl.shared.buffer_locks[slotno].lock)?;
    LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

    // If the slot is still in an io-in-progress state, then either someone
    // already started a new I/O on the slot, or a previous I/O failed and
    // neglected to reset the page state. We can cheaply test for failure by
    // seeing if the buffer lock is still held (transaction abort would have
    // released it).
    if ctl.shared.page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS
        || ctl.shared.page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS
    {
        if LWLockConditionalAcquire(&ctl.shared.buffer_locks[slotno].lock, LW_SHARED)? {
            // indeed, the I/O must have failed
            if ctl.shared.page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS {
                ctl.shared.page_status[slotno] = SLRU_PAGE_EMPTY;
            } else {
                // write_in_progress
                ctl.shared.page_status[slotno] = SLRU_PAGE_VALID;
                ctl.shared.page_dirty[slotno] = true;
            }
            LWLockRelease(&ctl.shared.buffer_locks[slotno].lock)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Read page
// ---------------------------------------------------------------------------

/// `SimpleLruReadPage(ctl, pageno, write_ok, xid)` — find a page in a shared
/// buffer, reading it in if necessary; the page number must correspond to an
/// already-initialized page. If `write_ok`, a WRITE_IN_PROGRESS page may be
/// returned. `xid` is for error reporting only. Returns the slot number; the
/// buffer's LRU access info is updated. The correct bank lock must be held
/// (exclusive) at entry, and will be held at exit.
pub fn SimpleLruReadPage(
    ctl: &mut SlruCtlData,
    pageno: i64,
    write_ok: bool,
    xid: TransactionId,
) -> PgResult<usize> {
    let bankno = (pageno % ctl.nbanks as i64) as usize;

    debug_assert!(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(ctl, pageno),
        LW_EXCLUSIVE
    ));

    // Outer loop handles restart if we must wait for someone else's I/O.
    loop {
        // See if page already is in memory; if not, pick victim slot.
        let slotno = SlruSelectLRUPage(ctl, pageno)?;

        // Did we find the page in memory?
        if ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY
            && ctl.shared.page_number[slotno] == pageno
        {
            // If page is still being read in, we must wait for I/O. Likewise
            // if the page is being written and the caller said that's not OK.
            if ctl.shared.page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS
                || (ctl.shared.page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS && !write_ok)
            {
                SimpleLruWaitIO(ctl, slotno)?;
                // Now we must recheck state from the top.
                continue;
            }
            // Otherwise, it's ready to use.
            SlruRecentlyUsed(&mut ctl.shared, slotno);

            stat_seams::pgstat_count_slru_page_hit::call(ctl.shared.slru_stats_idx);

            return Ok(slotno);
        }

        // We found no match; assert we selected a freeable slot.
        debug_assert!(
            ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY
                || (ctl.shared.page_status[slotno] == SLRU_PAGE_VALID
                    && !ctl.shared.page_dirty[slotno])
        );

        // Mark the slot read-busy.
        ctl.shared.page_number[slotno] = pageno;
        ctl.shared.page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
        ctl.shared.page_dirty[slotno] = false;

        // Acquire per-buffer lock (cannot deadlock, see notes at top).
        LWLockAcquire(&ctl.shared.buffer_locks[slotno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

        // Release bank lock while doing I/O.
        LWLockRelease(&ctl.shared.bank_locks[bankno].lock)?;

        // Do the read.
        let ok = SlruPhysicalReadPage(ctl, pageno, slotno)?;

        // Set the LSNs for this newly read-in page to zero.
        SimpleLruZeroLSNs(ctl, slotno);

        // Re-acquire bank control lock and update page state.
        LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

        debug_assert!(
            ctl.shared.page_number[slotno] == pageno
                && ctl.shared.page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS
                && !ctl.shared.page_dirty[slotno]
        );

        ctl.shared.page_status[slotno] = if ok.is_ok() {
            SLRU_PAGE_VALID
        } else {
            SLRU_PAGE_EMPTY
        };

        LWLockRelease(&ctl.shared.buffer_locks[slotno].lock)?;

        // Now it's okay to ereport if we failed.
        if let Err(io_err) = ok {
            return SlruReportIOError(ctl, pageno, xid, io_err);
        }

        SlruRecentlyUsed(&mut ctl.shared, slotno);

        stat_seams::pgstat_count_slru_page_read::call(ctl.shared.slru_stats_idx);

        return Ok(slotno);
    }
}

/// `SimpleLruReadPage_ReadOnly(ctl, pageno, xid)` — find a page for read-only
/// access. Bank control lock must NOT be held at entry, but WILL be held at
/// exit (shared or exclusive, unspecified).
pub fn SimpleLruReadPage_ReadOnly(
    ctl: &mut SlruCtlData,
    pageno: i64,
    xid: TransactionId,
) -> PgResult<usize> {
    let bankno = (pageno % ctl.nbanks as i64) as usize;
    let bankstart = bankno * SLRU_BANK_SIZE as usize;
    let bankend = bankstart + SLRU_BANK_SIZE as usize;

    // Try to find the page while holding only shared lock.
    LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, LW_SHARED, globals::my_proc_number::call())?;

    // See if page is already in a buffer.
    for slotno in bankstart..bankend {
        if ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY
            && ctl.shared.page_number[slotno] == pageno
            && ctl.shared.page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS
        {
            // See comments for SlruRecentlyUsed.
            SlruRecentlyUsed(&mut ctl.shared, slotno);

            stat_seams::pgstat_count_slru_page_hit::call(ctl.shared.slru_stats_idx);

            return Ok(slotno);
        }
    }

    // No luck, so switch to normal exclusive lock and do regular read.
    LWLockRelease(&ctl.shared.bank_locks[bankno].lock)?;
    LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

    SimpleLruReadPage(ctl, pageno, true, xid)
}

// ---------------------------------------------------------------------------
// Write page
// ---------------------------------------------------------------------------

/// `SlruWriteAllData` (slru.c) — files left open until control returns to
/// `SimpleLruWriteAll`.
struct SlruWriteAllData {
    /// # files actually open
    num_files: usize,
    /// their FDs
    fd: [i32; MAX_WRITEALL_BUFFERS],
    /// their log seg#s
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

/// `SlruInternalWritePage(ctl, slotno, fdata)` — write a page from a shared
/// buffer, if necessary. Does nothing if not dirty. Only one write attempt is
/// made, so the page may still be dirty at exit (re-dirtied during the
/// write); but a fresh write is attempted even if the page is already being
/// written (for checkpoints). Bank lock must be held at entry and exit.
fn SlruInternalWritePage(
    ctl: &mut SlruCtlData,
    slotno: usize,
    mut fdata: Option<&mut SlruWriteAllData>,
) -> PgResult<()> {
    let pageno = ctl.shared.page_number[slotno];
    let bankno = SlotGetBankNumber(slotno);

    debug_assert!(ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY);
    debug_assert!(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(ctl, pageno),
        LW_EXCLUSIVE
    ));

    // If a write is in progress, wait for it to finish.
    while ctl.shared.page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS
        && ctl.shared.page_number[slotno] == pageno
    {
        SimpleLruWaitIO(ctl, slotno)?;
    }

    // Do nothing if page is not dirty, or if buffer no longer contains the
    // same page we were called for.
    if !ctl.shared.page_dirty[slotno]
        || ctl.shared.page_status[slotno] != SLRU_PAGE_VALID
        || ctl.shared.page_number[slotno] != pageno
    {
        return Ok(());
    }

    // Mark the slot write-busy, and clear the dirtybit. After this point, a
    // transaction status update on this page will mark it dirty again.
    ctl.shared.page_status[slotno] = SLRU_PAGE_WRITE_IN_PROGRESS;
    ctl.shared.page_dirty[slotno] = false;

    // Acquire per-buffer lock (cannot deadlock, see notes at top).
    LWLockAcquire(&ctl.shared.buffer_locks[slotno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

    // Release bank lock while doing I/O.
    LWLockRelease(&ctl.shared.bank_locks[bankno].lock)?;

    // Do the write.
    let ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata.as_deref_mut())?;

    // If we failed, and we're in a flush, better close the files.
    if ok.is_err() {
        if let Some(f) = fdata.as_deref_mut() {
            for i in 0..f.num_files {
                file_seams::close_transient_file::call(f.fd[i]);
            }
        }
    }

    // Re-acquire bank lock and update page state.
    LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

    debug_assert!(
        ctl.shared.page_number[slotno] == pageno
            && ctl.shared.page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS
    );

    // If we failed to write, mark the page dirty again.
    if ok.is_err() {
        ctl.shared.page_dirty[slotno] = true;
    }

    ctl.shared.page_status[slotno] = SLRU_PAGE_VALID;

    LWLockRelease(&ctl.shared.buffer_locks[slotno].lock)?;

    // Now it's okay to ereport if we failed.
    if let Err(io_err) = ok {
        return SlruReportIOError(ctl, pageno, InvalidTransactionId, io_err);
    }

    // If part of a checkpoint, count this as a SLRU buffer written.
    if fdata.is_some() {
        xlog_seams::count_ckpt_slru_written::call();
        with_pending_checkpointer_stats(|p| p.slru_written += 1);
    }
    Ok(())
}

/// `SimpleLruWritePage(ctl, slotno)` — wrapper of `SlruInternalWritePage` for
/// external callers; fdata is always NULL here.
pub fn SimpleLruWritePage(ctl: &mut SlruCtlData, slotno: usize) -> PgResult<()> {
    debug_assert!(ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY);

    SlruInternalWritePage(ctl, slotno, None)
}

/// `SimpleLruDoesPhysicalPageExist(ctl, pageno)` — whether the given page
/// exists on disk. False means the file doesn't exist or is too short to
/// contain the page.
pub fn SimpleLruDoesPhysicalPageExist(ctl: &mut SlruCtlData, pageno: i64) -> PgResult<bool> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;

    stat_seams::pgstat_count_slru_page_exists::call(ctl.shared.slru_stats_idx);

    let path = SlruFileName(ctl, segno);

    let fd = file_seams::open_transient_file::call(&path, libc::O_RDONLY)?;
    if fd < 0 {
        let en = current_errno();

        // expected: file doesn't exist
        if en == libc::ENOENT {
            return Ok(false);
        }

        // report error normally
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

    let endpos = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
    if endpos < 0 {
        let en = current_errno();
        // (as in C, the ereport unwinds with the transient fd open; fd.c's
        // transaction-end cleanup closes it)
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
        // C records SLRU_CLOSE_FAILED/errno in the statics but returns false
        // without reporting.
        return Ok(false);
    }

    Ok(result)
}

/// `SlruPhysicalReadPage(ctl, pageno, slotno)` — physical read of a
/// (previously existing) page into a buffer slot. On failure we cannot just
/// ereport, since the caller has shared-memory state to undo: the inner
/// `Err(SlruIoError)` carries what `SlruReportIOError` needs; the outer `Err`
/// is `OpenTransientFile`'s own ereport surface.
fn SlruPhysicalReadPage(
    ctl: &mut SlruCtlData,
    pageno: i64,
    slotno: usize,
) -> PgResult<Result<(), SlruIoError>> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;

    let path = SlruFileName(ctl, segno);

    // In a crash-and-restart situation, it's possible to receive commands to
    // set the commit status of transactions whose bits are in
    // already-truncated segments of the commit log (see notes in
    // SlruPhysicalWritePage). Hence, if we are InRecovery, allow the case
    // where the file doesn't exist, and return zeroes instead.
    let fd = file_seams::open_transient_file::call(&path, libc::O_RDONLY)?;
    if fd < 0 {
        let en = current_errno();
        if en != libc::ENOENT || !xlogrecovery_seams::in_recovery::call() {
            return Ok(Err(SlruIoError {
                cause: SLRU_OPEN_FAILED,
                errno: en,
            }));
        }

        ereport(LOG)
            .errmsg(format!("file \"{path}\" doesn't exist, reading as zeroes"))
            .finish(loc("SlruPhysicalReadPage"))?;
        ctl.shared.page_buffer_mut(slotno).fill(0);
        return Ok(Ok(()));
    }

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_READ);
    let nread = {
        let buf = ctl.shared.page_buffer_mut(slotno);
        unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                BLCKSZ,
                offset as libc::off_t,
            )
        }
    };
    if nread != BLCKSZ as isize {
        waitevent_seams::pgstat_report_wait_end::call();
        // errno 0 = short read ("read too few bytes")
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

/// `SlruPhysicalWritePage(ctl, pageno, slotno, fdata)` — physical write of a
/// page from a buffer slot. Same failure-channel shape as
/// [`SlruPhysicalReadPage`]. `fdata` is None for a standalone write, the
/// open-file info during `SimpleLruWriteAll`.
fn SlruPhysicalWritePage(
    ctl: &mut SlruCtlData,
    pageno: i64,
    slotno: usize,
    fdata: Option<&mut SlruWriteAllData>,
) -> PgResult<Result<(), SlruIoError>> {
    let segno = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    let offset = rpageno * BLCKSZ as i64;
    let mut fdata = fdata;

    stat_seams::pgstat_count_slru_page_written::call(ctl.shared.slru_stats_idx);

    // Honor the write-WAL-before-data rule: determine the largest
    // async-commit LSN for the page and flush WAL through it. (Same action
    // as FlushBuffer() in the main buffer manager.)
    if !ctl.shared.group_lsn.is_empty() {
        let groups = ctl.shared.lsn_groups_per_page as usize;
        let lsnindex = slotno * groups;
        let mut max_lsn = ctl.shared.group_lsn[lsnindex];
        for lsnoff in 1..groups {
            let this_lsn = ctl.shared.group_lsn[lsnindex + lsnoff];
            if max_lsn < this_lsn {
                max_lsn = this_lsn;
            }
        }

        if max_lsn != InvalidXLogRecPtr {
            // elog(ERROR) is not acceptable here, so if XLogFlush were to
            // fail, we must PANIC: run it inside a critical section (the
            // error machinery promotes ERROR to PANIC while
            // CritSectionCount > 0).
            config::set_crit_section_count(config::crit_section_count() + 1);
            xlog_seams::xlog_flush::call(max_lsn)?;
            config::set_crit_section_count(config::crit_section_count() - 1);
        }
    }

    // During a SimpleLruWriteAll, we may already have the desired file open.
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
        // If the file doesn't already exist, we should create it. It is
        // possible for this to need to happen when writing a page that's not
        // first in its segment: after a crash and restart the REDO logic may
        // replay from a checkpoint before the latest one, producing commands
        // to set status of transactions already truncated from the commit
        // log; accept references to nonexistent files here and in
        // SlruPhysicalReadPage. More than one backend may run this
        // simultaneously for different pages of the same file, so no
        // O_EXCL/O_TRUNC.
        let path = SlruFileName(ctl, segno);
        fd = file_seams::open_transient_file::call(&path, libc::O_RDWR | libc::O_CREAT)?;
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
                // In the unlikely event that we exceed MAX_WRITEALL_BUFFERS,
                // fall back to treating it as a standalone write.
                fdata = None;
            }
            None => {}
        }
    }

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SLRU_WRITE);
    let nwritten = {
        let buf = ctl.shared.page_buffer(slotno);
        unsafe {
            libc::pwrite(
                fd,
                buf.as_ptr() as *const libc::c_void,
                BLCKSZ,
                offset as libc::off_t,
            )
        }
    };
    if nwritten != BLCKSZ as isize {
        waitevent_seams::pgstat_report_wait_end::call();
        // if write didn't set errno, assume problem is no disk space
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

    // Queue up a sync request for the checkpointer.
    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        let tag = FileTag::for_slru(ctl.sync_handler, segno as u64);
        if !sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_REQUEST, false)? {
            // No space to enqueue sync request. Do it synchronously.
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

    // Close file, unless part of flush request.
    if fdata.is_none() && file_seams::close_transient_file::call(fd) != 0 {
        return Ok(Err(SlruIoError {
            cause: SLRU_CLOSE_FAILED,
            errno: current_errno(),
        }));
    }

    Ok(Ok(()))
}

/// `SlruReportIOError(ctl, pageno, xid)` — issue the error message after a
/// failure of `SlruPhysicalReadPage`/`SlruPhysicalWritePage`, after the
/// shared-memory state has been cleaned up. The saved cause/errno pair (file
/// statics in C) is the `io_err` argument.
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
    match io_err.cause {
        SLRU_OPEN_FAILED => ereport(ERROR)
            .with_saved_errno(io_err.errno)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not open file \"{path}\": %m."))
            .finish(loc("SlruReportIOError"))?,
        SLRU_SEEK_FAILED => ereport(ERROR)
            .with_saved_errno(io_err.errno)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!(
                "Could not seek in file \"{path}\" to offset {offset}: %m."
            ))
            .finish(loc("SlruReportIOError"))?,
        SLRU_READ_FAILED => {
            if io_err.errno != 0 {
                ereport(ERROR)
                    .with_saved_errno(io_err.errno)
                    .errcode_for_file_access()
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not read from file \"{path}\" at offset {offset}: %m."
                    ))
                    .finish(loc("SlruReportIOError"))?
            } else {
                ereport(ERROR)
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not read from file \"{path}\" at offset {offset}: read too few bytes."
                    ))
                    .finish(loc("SlruReportIOError"))?
            }
        }
        SLRU_WRITE_FAILED => {
            if io_err.errno != 0 {
                ereport(ERROR)
                    .with_saved_errno(io_err.errno)
                    .errcode_for_file_access()
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not write to file \"{path}\" at offset {offset}: %m."
                    ))
                    .finish(loc("SlruReportIOError"))?
            } else {
                ereport(ERROR)
                    .errmsg(msg)
                    .errdetail(format!(
                        "Could not write to file \"{path}\" at offset {offset}: wrote too few bytes."
                    ))
                    .finish(loc("SlruReportIOError"))?
            }
        }
        SLRU_FSYNC_FAILED => ereport(file_seams::data_sync_elevel::call(ERROR))
            .with_saved_errno(io_err.errno)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not fsync file \"{path}\": %m."))
            .finish(loc("SlruReportIOError"))?,
        SLRU_CLOSE_FAILED => ereport(ERROR)
            .with_saved_errno(io_err.errno)
            .errcode_for_file_access()
            .errmsg(msg)
            .errdetail(format!("Could not close file \"{path}\": %m."))
            .finish(loc("SlruReportIOError"))?,
    }
    // Every cause reports at >= ERROR (data_sync_elevel never lowers), so
    // finish() always returned Err above. (The C default: arm is the enum's
    // unreachable.)
    unreachable!("SlruReportIOError reported below ERROR");
}

// ---------------------------------------------------------------------------
// LRU bookkeeping
// ---------------------------------------------------------------------------

/// `SlruRecentlyUsed(shared, slotno)` — mark a buffer slot "most recently
/// used". The if-test suppresses useless increments for consecutive accesses
/// to the same page, reducing the chance of count wraparound; concurrent
/// execution inside `SimpleLruReadPage_ReadOnly` can at worst "reset" counts
/// to lower values, costing only a nonoptimal eviction choice.
fn SlruRecentlyUsed(shared: &mut SlruSharedData, slotno: usize) {
    let bankno = SlotGetBankNumber(slotno);
    let mut new_lru_count = shared.bank_cur_lru_count[bankno];

    debug_assert!(shared.page_status[slotno] != SLRU_PAGE_EMPTY);

    if new_lru_count != shared.page_lru_count[slotno] {
        new_lru_count = new_lru_count.wrapping_add(1);
        shared.bank_cur_lru_count[bankno] = new_lru_count;
        shared.page_lru_count[slotno] = new_lru_count;
    }
}

/// `SlruSelectLRUPage(ctl, pageno)` — select the slot to re-use for the given
/// page. Returns either a slot already holding pageno (any state except
/// EMPTY) or a freeable slot (EMPTY or clean). The correct bank lock must be
/// held at entry, and will be held at exit.
fn SlruSelectLRUPage(ctl: &mut SlruCtlData, pageno: i64) -> PgResult<usize> {
    // Outer loop handles restart after I/O.
    loop {
        let mut bestvalidslot = 0usize; /* keep compiler quiet */
        let mut best_valid_delta = -1i32;
        let mut best_valid_page_number = 0i64; /* keep compiler quiet */
        let mut bestinvalidslot = 0usize; /* keep compiler quiet */
        let mut best_invalid_delta = -1i32;
        let mut best_invalid_page_number = 0i64; /* keep compiler quiet */
        let bankno = (pageno % ctl.nbanks as i64) as usize;
        let bankstart = bankno * SLRU_BANK_SIZE as usize;
        let bankend = bankstart + SLRU_BANK_SIZE as usize;

        debug_assert!(LWLockHeldByMe(SimpleLruGetBankLock(ctl, pageno)));

        // See if page already has a buffer assigned.
        for slotno in bankstart..bankend {
            if ctl.shared.page_status[slotno] != SLRU_PAGE_EMPTY
                && ctl.shared.page_number[slotno] == pageno
            {
                return Ok(slotno);
            }
        }

        // If we find any EMPTY slot, just select that one. Else choose the
        // least recently used valid page — but never the slot containing
        // latest_page_number, and an I/O-busy slot only if there is no other
        // choice. Concurrent SlruRecentlyUsed in ReadPage_ReadOnly can give
        // multiple pages the same lru_count; break ties by choosing the
        // furthest-back page. The post-increment forcibly advances
        // cur_lru_count beyond every page_lru_count so the next
        // SlruRecentlyUsed will mark its page newly used.
        let cur_count = ctl.shared.bank_cur_lru_count[bankno];
        ctl.shared.bank_cur_lru_count[bankno] = cur_count.wrapping_add(1);
        for slotno in bankstart..bankend {
            if ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY {
                return Ok(slotno);
            }

            let mut this_delta = cur_count.wrapping_sub(ctl.shared.page_lru_count[slotno]);
            if this_delta < 0 {
                // Clean up in case shared updates have caused cur_count
                // increments to get "lost": back off the page counts rather
                // than increasing cur_count, avoiding any question of
                // infinite loops with wrapped-around counts.
                ctl.shared.page_lru_count[slotno] = cur_count;
                this_delta = 0;
            }

            // If this page is the one most recently zeroed, don't consider
            // it an eviction candidate (see SimpleLruZeroPage on the lack of
            // a memory barrier).
            let this_page_number = ctl.shared.page_number[slotno];
            if this_page_number == ctl.shared.latest_page_number.read() as i64 {
                continue;
            }

            if ctl.shared.page_status[slotno] == SLRU_PAGE_VALID {
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

        // If all pages (except possibly the latest one) are I/O busy, wait
        // for the I/O on the least recently used slot and retry: it was
        // likely initiated first and may finish first.
        if best_valid_delta < 0 {
            SimpleLruWaitIO(ctl, bestinvalidslot)?;
            continue;
        }

        // If the selected page is clean, we're set.
        if !ctl.shared.page_dirty[bestvalidslot] {
            return Ok(bestvalidslot);
        }

        // Write the page, then loop back and try again: the easiest way to
        // handle corner cases such as the victim page being re-dirtied while
        // we wrote it.
        SlruInternalWritePage(ctl, bestvalidslot, None)?;
    }
}

// ---------------------------------------------------------------------------
// WriteAll / truncate / delete
// ---------------------------------------------------------------------------

/// `SimpleLruWriteAll(ctl, allow_redirtied)` — write dirty pages to disk
/// during checkpoint or database shutdown. Flushing is deferred to the next
/// `ProcessSyncRequests()`, but the containing directory is fsync'd here so
/// new directory entries are on disk.
pub fn SimpleLruWriteAll(ctl: &mut SlruCtlData, allow_redirtied: bool) -> PgResult<()> {
    let mut pageno: i64 = 0;
    let mut prevbank = SlotGetBankNumber(0);

    stat_seams::pgstat_count_slru_flush::call(ctl.shared.slru_stats_idx);

    // Find and write dirty pages.
    let mut fdata = SlruWriteAllData::new();

    LWLockAcquire(&ctl.shared.bank_locks[prevbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;

    for slotno in 0..ctl.shared.num_slots as usize {
        let curbank = SlotGetBankNumber(slotno);

        // If the current bank lock is not the same as the previous bank
        // lock, release the previous lock and acquire the new lock.
        if curbank != prevbank {
            LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
            LWLockAcquire(&ctl.shared.bank_locks[curbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;
            prevbank = curbank;
        }

        // Do nothing if slot is unused.
        if ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY {
            continue;
        }

        SlruInternalWritePage(ctl, slotno, Some(&mut fdata))?;

        // In some places (e.g. checkpoints) we cannot assert the slot is
        // clean now, since another process might have re-dirtied it already.
        debug_assert!(
            allow_redirtied
                || ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY
                || (ctl.shared.page_status[slotno] == SLRU_PAGE_VALID
                    && !ctl.shared.page_dirty[slotno])
        );
    }

    LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;

    // Now close any files that were open.
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

    // Ensure that directory entries for new files are on disk.
    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        file_seams::fsync_fname::call(&ctl.Dir, true)?;
    }
    Ok(())
}

/// `SimpleLruTruncate(ctl, cutoffPage)` — remove all segments before the one
/// holding the passed page number. All SLRUs prevent concurrent calls, either
/// with an LWLock or by calling only as part of a checkpoint; mutual
/// exclusion must begin before computing cutoffPage and end after any limit
/// update permitting fresh writes into the segment preceding the cutoff's.
pub fn SimpleLruTruncate(ctl: &mut SlruCtlData, cutoffPage: i64) -> PgResult<()> {
    stat_seams::pgstat_count_slru_truncate::call(ctl.shared.slru_stats_idx);

    // Scan shared memory and remove any pages preceding the cutoff page, to
    // ensure we won't rewrite them later. (Normally called in or just after
    // a checkpoint, so dirty pages should already be flushed; this is extra
    // care.)
    'restart: loop {
        // An important safety check: the current endpoint page must not be
        // eligible for removal — a backstop against wraparound bugs
        // elsewhere in SLRU handling. A slightly outdated read is fine, so
        // no memory barrier.
        if (ctl.page_precedes())(ctl.shared.latest_page_number.read() as i64, cutoffPage) {
            ereport(LOG)
                .errmsg(format!(
                    "could not truncate directory \"{}\": apparent wraparound",
                    ctl.Dir
                ))
                .finish(loc("SimpleLruTruncate"))?;
            return Ok(());
        }

        let mut prevbank = SlotGetBankNumber(0);
        LWLockAcquire(&ctl.shared.bank_locks[prevbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;
        for slotno in 0..ctl.shared.num_slots as usize {
            let curbank = SlotGetBankNumber(slotno);

            // If the current bank lock is not the same as the previous bank
            // lock, release the previous lock and acquire the new lock.
            if curbank != prevbank {
                LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
                LWLockAcquire(&ctl.shared.bank_locks[curbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;
                prevbank = curbank;
            }

            if ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY {
                continue;
            }
            if !(ctl.page_precedes())(ctl.shared.page_number[slotno], cutoffPage) {
                continue;
            }

            // If page is clean, just change state to EMPTY (expected case).
            if ctl.shared.page_status[slotno] == SLRU_PAGE_VALID
                && !ctl.shared.page_dirty[slotno]
            {
                ctl.shared.page_status[slotno] = SLRU_PAGE_EMPTY;
                continue;
            }

            // We have (or may have) I/O operations acting on the page, so
            // wait for them to finish and start again — the same logic as in
            // SlruSelectLRUPage.
            if ctl.shared.page_status[slotno] == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, None)?;
            } else {
                SimpleLruWaitIO(ctl, slotno)?;
            }

            LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
            continue 'restart;
        }

        LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
        break;
    }

    // Now we can remove the old segment(s).
    SlruScanDirectory(ctl, |ctl, filename, segpage| {
        SlruScanDirCbDeleteCutoff(ctl, filename, segpage, cutoffPage)
    })?;
    Ok(())
}

/// `SlruInternalDeleteSegment(ctl, segno)` — delete an individual SLRU
/// segment. Does not touch the SLRU buffers themselves: callers ensure they
/// can't yet contain anything or have been cleaned out.
fn SlruInternalDeleteSegment(ctl: &SlruCtlData, segno: i64) -> PgResult<()> {
    // Forget any fsync requests queued for this segment.
    if ctl.sync_handler != SyncRequestHandler::SYNC_HANDLER_NONE {
        let tag = FileTag::for_slru(ctl.sync_handler, segno as u64);
        sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_FORGET_REQUEST, true)?;
    }

    // Unlink the file.
    let path = SlruFileName(ctl, segno);
    ereport(DEBUG2)
        .errmsg_internal(format!("removing file \"{path}\""))
        .finish(loc("SlruInternalDeleteSegment"))?;
    let cpath = CString::new(path).expect("interior NUL in SLRU segment path");
    unsafe {
        libc::unlink(cpath.as_ptr());
    }
    Ok(())
}

/// `SlruDeleteSegment(ctl, segno)` — delete an individual SLRU segment,
/// identified by the segment number, cleaning out any buffered references.
pub fn SlruDeleteSegment(ctl: &mut SlruCtlData, segno: i64) -> PgResult<()> {
    let mut prevbank = SlotGetBankNumber(0);

    // Clean out any possibly existing references to the segment.
    LWLockAcquire(&ctl.shared.bank_locks[prevbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;
    loop {
        let mut did_write = false;
        for slotno in 0..ctl.shared.num_slots as usize {
            let curbank = SlotGetBankNumber(slotno);

            // If the current bank lock is not the same as the previous bank
            // lock, release the previous lock and acquire the new lock.
            if curbank != prevbank {
                LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
                LWLockAcquire(&ctl.shared.bank_locks[curbank].lock, LW_EXCLUSIVE, globals::my_proc_number::call())?;
                prevbank = curbank;
            }

            if ctl.shared.page_status[slotno] == SLRU_PAGE_EMPTY {
                continue;
            }

            let pagesegno = ctl.shared.page_number[slotno] / SLRU_PAGES_PER_SEGMENT;
            // not the segment we're looking for
            if pagesegno != segno {
                continue;
            }

            // If page is clean, just change state to EMPTY (expected case).
            if ctl.shared.page_status[slotno] == SLRU_PAGE_VALID
                && !ctl.shared.page_dirty[slotno]
            {
                ctl.shared.page_status[slotno] = SLRU_PAGE_EMPTY;
                continue;
            }

            // Same logic as SimpleLruTruncate().
            if ctl.shared.page_status[slotno] == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, None)?;
            } else {
                SimpleLruWaitIO(ctl, slotno)?;
            }

            did_write = true;
        }

        // Be extra careful and re-check: the IO functions release the
        // control lock, so new pages could have been read in.
        if !did_write {
            break;
        }
    }

    SlruInternalDeleteSegment(ctl, segno)?;

    LWLockRelease(&ctl.shared.bank_locks[prevbank].lock)?;
    Ok(())
}

/// `SlruMayDeleteSegment(ctl, segpage, cutoffPage)` — whether a segment is
/// okay to delete. `segpage` is the segment's first page; `cutoffPage` the
/// oldest (in PagePrecedes order) page still containing useful data. Since
/// every core PagePrecedes callback implements "wrap around", check the
/// segment's first and last pages.
fn SlruMayDeleteSegment(ctl: &SlruCtlData, segpage: i64, cutoffPage: i64) -> bool {
    let seg_last_page = segpage + SLRU_PAGES_PER_SEGMENT - 1;

    debug_assert!(segpage % SLRU_PAGES_PER_SEGMENT == 0);

    (ctl.page_precedes())(segpage, cutoffPage)
        && (ctl.page_precedes())(seg_last_page, cutoffPage)
}

// ---------------------------------------------------------------------------
// PagePrecedes self-tests (USE_ASSERT_CHECKING)
// ---------------------------------------------------------------------------

/// `SlruPagePrecedesTestOffset(ctl, per_page, offset)` — every check is an
/// `Assert` in C (assert-only build), mapped to `debug_assert!`.
fn SlruPagePrecedesTestOffset(ctl: &SlruCtlData, per_page: i32, offset: u32) {
    let precedes = ctl.page_precedes();
    let pp = per_page as u32;

    // Compare an XID pair having undefined order (see RFC 1982), a pair at
    // "opposite ends" of the XID space. TransactionIdPrecedes() treats each
    // as preceding the other. If RHS is oldestXact, LHS is the first XID we
    // must not assign.
    let lhs: TransactionId = pp.wrapping_add(offset); /* skip first page to avoid non-normal XIDs */
    let rhs: TransactionId = lhs.wrapping_add(1u32 << 31);
    debug_assert!(TransactionIdPrecedes(lhs, rhs));
    debug_assert!(TransactionIdPrecedes(rhs, lhs));
    debug_assert!(!TransactionIdPrecedes(lhs.wrapping_sub(1), rhs));
    debug_assert!(TransactionIdPrecedes(rhs, lhs.wrapping_sub(1)));
    debug_assert!(TransactionIdPrecedes(lhs.wrapping_add(1), rhs));
    debug_assert!(!TransactionIdPrecedes(rhs, lhs.wrapping_add(1)));
    debug_assert!(!TransactionIdFollowsOrEquals(lhs, rhs));
    debug_assert!(!TransactionIdFollowsOrEquals(rhs, lhs));

    // C divides uint32 XIDs by the int per_page; both operands are
    // non-negative, so i64 division reproduces it.
    let pp64 = per_page as i64;
    let page = |x: TransactionId| (x as i64) / pp64;
    debug_assert!(!precedes(page(lhs), page(lhs)));
    debug_assert!(!precedes(page(lhs), page(rhs)));
    debug_assert!(!precedes(page(rhs), page(lhs)));
    debug_assert!(!precedes(page(lhs.wrapping_sub(pp)), page(rhs)));
    debug_assert!(precedes(page(rhs), page(lhs.wrapping_sub(3u32.wrapping_mul(pp)))));
    debug_assert!(precedes(page(rhs), page(lhs.wrapping_sub(2u32.wrapping_mul(pp)))));
    debug_assert!(
        precedes(page(rhs), page(lhs.wrapping_sub(pp))) || (1u32 << 31) % pp != 0
    ); /* see CommitTsPagePrecedes() */
    debug_assert!(
        precedes(page(lhs.wrapping_add(pp)), page(rhs)) || (1u32 << 31) % pp != 0
    );
    debug_assert!(precedes(page(lhs.wrapping_add(2u32.wrapping_mul(pp))), page(rhs)));
    debug_assert!(precedes(page(lhs.wrapping_add(3u32.wrapping_mul(pp))), page(rhs)));
    debug_assert!(!precedes(page(rhs), page(lhs.wrapping_add(pp))));

    // GetNewTransactionId() has assigned the last XID it can safely use, and
    // that XID is in the *LAST* page of the second segment. We must not
    // delete that segment.
    let mut newestPage: i64 = 2 * SLRU_PAGES_PER_SEGMENT - 1;
    let mut newestXact: TransactionId =
        (newestPage as u32).wrapping_mul(pp).wrapping_add(offset);
    debug_assert!((newestXact as i64) / pp64 == newestPage);
    let mut oldestXact: TransactionId = newestXact.wrapping_add(1);
    oldestXact = oldestXact.wrapping_sub(1u32 << 31);
    let mut oldestPage: i64 = (oldestXact as i64) / pp64;
    debug_assert!(!SlruMayDeleteSegment(
        ctl,
        newestPage - newestPage % SLRU_PAGES_PER_SEGMENT,
        oldestPage
    ));

    // ... and likewise in the *FIRST* page of the second segment.
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

/// `SlruPagePrecedesUnitTests(ctl, per_page)` — unit-test a PagePrecedes
/// function (first, middle and last entries of a page). Assumes every uint32
/// >= FirstNormalTransactionId is a valid key occupying a contiguous
/// fixed-size region of SLRU bytes (so not MultiXactMemberCtl / NotifyCtl).
/// `USE_ASSERT_CHECKING`-only in C; a no-op without debug assertions.
pub fn SlruPagePrecedesUnitTests(ctl: &SlruCtlData, per_page: i32) {
    if cfg!(debug_assertions) {
        // Test first, middle and last entries of a page.
        SlruPagePrecedesTestOffset(ctl, per_page, 0);
        SlruPagePrecedesTestOffset(ctl, per_page, (per_page / 2) as u32);
        SlruPagePrecedesTestOffset(ctl, per_page, (per_page - 1) as u32);
    }
}

// ---------------------------------------------------------------------------
// Directory scan
// ---------------------------------------------------------------------------

/// `SlruScanDirCbReportPresence(ctl, filename, segpage, data)` — scan
/// callback reporting true if any segment is wholly prior to the one
/// containing `cutoff_page` (the C `void *data`).
pub fn SlruScanDirCbReportPresence(
    ctl: &SlruCtlData,
    _filename: &str,
    segpage: i64,
    cutoff_page: i64,
) -> PgResult<bool> {
    if SlruMayDeleteSegment(ctl, segpage, cutoff_page) {
        return Ok(true); /* found one; don't iterate any more */
    }

    Ok(false) /* keep going */
}

/// `SlruScanDirCbDeleteCutoff(ctl, filename, segpage, data)` — scan callback
/// deleting segments prior to `cutoff_page` (the C `void *data`).
fn SlruScanDirCbDeleteCutoff(
    ctl: &SlruCtlData,
    _filename: &str,
    segpage: i64,
    cutoff_page: i64,
) -> PgResult<bool> {
    if SlruMayDeleteSegment(ctl, segpage, cutoff_page) {
        SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT)?;
    }

    Ok(false) /* keep going */
}

/// `SlruScanDirCbDeleteAll(ctl, filename, segpage, data)` — scan callback
/// deleting all segments.
pub fn SlruScanDirCbDeleteAll(
    ctl: &SlruCtlData,
    _filename: &str,
    segpage: i64,
) -> PgResult<bool> {
    SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT)?;

    Ok(false) /* keep going */
}

/// `SlruCorrectSegmentFilenameLength(ctl, len)` — whether a file name of the
/// given length may be a correct SLRU segment name.
fn SlruCorrectSegmentFilenameLength(ctl: &SlruCtlData, len: usize) -> bool {
    if ctl.long_segment_names {
        len == 15 /* see SlruFileName() */
    } else {
        // Commit 638cf09e76d allowed 5-character lengths; 73c986adde5
        // allowed 6.
        len == 4 || len == 5 || len == 6
    }
}

/// `SlruScanDirectory(ctl, callback, data)` — scan the SLRU directory and
/// apply a callback to each file found; a true return from the callback
/// stops the scan, and the last callback return value is returned. (`data`
/// is closure capture here.) Ordering is not guaranteed; no locking is
/// applied.
pub fn SlruScanDirectory(
    ctl: &SlruCtlData,
    mut callback: impl FnMut(&SlruCtlData, &str, i64) -> PgResult<bool>,
) -> PgResult<bool> {
    file_seams::with_allocated_dir::call(&ctl.Dir, &mut |d_name| {
        let len = d_name.len();

        if SlruCorrectSegmentFilenameLength(ctl, len)
            && d_name
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'A'..=b'F'))
        {
            // strtoi64(..., 16); the length/charset checks make this
            // infallible for in-range names.
            let segno = i64::from_str_radix(d_name, 16)
                .expect("checked hex SLRU segment name failed to parse");
            let segpage = segno * SLRU_PAGES_PER_SEGMENT;

            ::utils_error::elog(
                DEBUG2,
                format!(
                    "SlruScanDirectory invoking callback on {}/{}",
                    ctl.Dir, d_name
                ),
            )?;
            return callback(ctl, d_name, segpage);
        }
        Ok(false)
    })
}

/// `SlruSyncFileTag(ctl, ftag, path)` — sync.c handler implementation shared
/// by the individual SLRUs: fsync the tagged segment. Returns the fsync
/// result (`0` ok, `-1` with `errno` preserved, as sync.c expects) together
/// with the path written into the C caller's buffer.
pub fn SlruSyncFileTag(ctl: &SlruCtlData, ftag: &FileTag) -> PgResult<(i32, String)> {
    let path = SlruFileName(ctl, ftag.segno as i64);

    let fd = file_seams::open_transient_file::call(&path, libc::O_RDWR)?;
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

    fn ctl(long_segment_names: bool) -> SlruCtlData {
        SlruCtlData {
            shared: SlruSharedData::default(),
            nbanks: 1,
            long_segment_names,
            sync_handler: SyncRequestHandler::SYNC_HANDLER_NONE,
            PagePrecedes: None,
            Dir: "pg_xact".to_string(),
        }
    }

    #[test]
    fn slru_file_name_short() {
        let c = ctl(false);
        assert_eq!(SlruFileName(&c, 0), "pg_xact/0000");
        assert_eq!(SlruFileName(&c, 0x1234), "pg_xact/1234");
        assert_eq!(SlruFileName(&c, 0x12345), "pg_xact/12345");
        assert_eq!(SlruFileName(&c, 0xFFFFFF), "pg_xact/FFFFFF");
    }

    #[test]
    fn slru_file_name_long() {
        let c = ctl(true);
        assert_eq!(SlruFileName(&c, 0), "pg_xact/000000000000000");
        assert_eq!(SlruFileName(&c, 0x123456789ABCDEF), "pg_xact/123456789ABCDEF");
    }

    #[test]
    fn segment_filename_lengths() {
        let short = ctl(false);
        for len in 0..20usize {
            assert_eq!(
                SlruCorrectSegmentFilenameLength(&short, len),
                matches!(len, 4 | 5 | 6)
            );
        }
        let long = ctl(true);
        for len in 0..20usize {
            assert_eq!(SlruCorrectSegmentFilenameLength(&long, len), len == 15);
        }
    }

    #[test]
    fn check_slru_buffers_bank_multiple() {
        assert_eq!(check_slru_buffers("xact_buffers", 32), (true, None));
        let (ok, detail) = check_slru_buffers("xact_buffers", 17);
        assert!(!ok);
        assert_eq!(
            detail.as_deref(),
            Some("\"xact_buffers\" must be a multiple of 16.")
        );
    }

    #[test]
    fn shmem_size_accumulation() {
        // 64 slots, 0 lsns: header 104 -> maxalign 104; +64*8 (ptrs) + 64*4
        // (status) + 64*1 (dirty, maxaligned to 64) + 64*8 + 64*4; then the
        // running size is lwlockaligned to 128 before each lock array (so the
        // 128-aligned LWLockPadded slices construct without panicking); + 64*128
        // (buffer locks) + 4*128 (bank locks) + 4*4 (bank lru, maxaligned to
        // 16); bufferalign; + 64*BLCKSZ.
        let sz = SimpleLruShmemSize(64, 0);
        let header = 104;
        // running size up to page_lru_count == 1704, lwlockalign -> 1792 (+88);
        // after buffer_locks == 9984 which is already 128-aligned (+0).
        let pre_locks = header + 512 + 256 + 64 + 512 + 256; // 1704
        let after_buffer_locks = lwlockalign(pre_locks) + 8192; // 1792 + 8192 = 9984
        let total = lwlockalign(after_buffer_locks) + 512 + 16; // + bank_locks + bank_lru
        let expected = bufferalign(total) + 64 * BLCKSZ;
        assert_eq!(sz, expected);
        // adding lsn groups grows it by maxalign(nslots*nlsns*8)
        assert_eq!(SimpleLruShmemSize(64, 2) - sz, 64 * 2 * 8);
    }

    /// Cross-process commit visibility, at the SLRU storage substrate: a
    /// `ShmemSlice` over a MAP_SHARED|MAP_ANONYMOUS region — exactly the
    /// segment a forked backend inherits — carries a value written by the
    /// parent into the child. This is the property the owned-`Vec` model lost:
    /// a CLOG status one backend writes is visible to a sibling's
    /// `TransactionIdGetStatus`. (Unix only; the production path is fork-based.)
    #[test]
    #[cfg(unix)]
    fn shmem_slice_is_visible_across_fork() {
        const N: usize = 8;
        let bytes = N * core::mem::size_of::<i64>();
        // The same MAP_SHARED|MAP_ANONYMOUS mapping ShmemCreate uses; fork
        // inherits it at the same address.
        let base = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(base, libc::MAP_FAILED, "mmap failed");

        // SAFETY: `base` owns `N` zeroed `i64`s in the shared mapping.
        let mut slice = unsafe { ShmemSlice::<i64>::from_raw(base.cast::<i64>(), N) };
        slice.fill(0);

        // Parent writes a "committed" marker (mirrors a CLOG status write).
        const MARKER: i64 = 0x5141_4c47; // "QALG"
        slice[3] = MARKER;

        // SAFETY: classic fork in a leaf test; the child only reads shmem and
        // _exits without running at-exit handlers.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Child: re-derive the slice over the inherited mapping and check
            // it sees the parent's write.
            let child_view = unsafe { ShmemSlice::<i64>::from_raw(base.cast::<i64>(), N) };
            let ok = child_view[3] == MARKER;
            unsafe { libc::_exit(if ok { 0 } else { 1 }) };
        }

        // Parent: reap the child and assert it observed the write.
        let mut status: libc::c_int = 0;
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(waited, pid, "waitpid failed");
        let exited_ok = libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
        unsafe {
            libc::munmap(base, bytes);
        }
        assert!(
            exited_ok,
            "child did not observe the parent's shmem write (cross-process visibility broken)"
        );
    }
}
