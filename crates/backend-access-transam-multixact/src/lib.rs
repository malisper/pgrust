//! `multixact.c` — the multi-transaction (MultiXact) manager
//! (`src/backend/access/transam/multixact.c`, PostgreSQL 18.3).
//!
//! A MultiXact groups several transactions that hold a tuple lock concurrently
//! (e.g. multiple `FOR SHARE` lockers, or a locker plus an updater). It is
//! stored across two SLRUs: the "offsets" SLRU maps a MultiXactId to the
//! starting offset of its members, and the "members" SLRU stores the
//! `(xid, status)` members at those offsets.
//!
//! # State
//!
//! C keeps two file-static `SlruCtlData` control structs
//! (`MultiXactOffsetCtlData` / `MultiXactMemberCtlData`) plus the shared
//! `MultiXactStateData` (with the two `perBackendXactIds[]` arrays) and a
//! backend-local MultiXactId cache. These are per-backend (shared-memory-backed
//! for the SLRU/state, backend-private for the cache) globals; mirrored here by
//! the thread-locals [`MXACT_OFFSET_CTL`], [`MXACT_MEMBER_CTL`], [`MXACT_STATE`]
//! and [`MXACT_CACHE`], exactly as the sibling clog port mirrors `XactCtl`.
//!
//! # Boundaries
//!
//! Both SLRUs and the LWLock manager are consumed directly from the ported
//! sibling crates ([`backend_access_transam_slru`] /
//! [`backend_storage_lmgr_lwlock`]) — not seamed. The `MultiXactGenLock` /
//! `MultiXactTruncationLock` fixed LWLocks are acquired in-crate via
//! `LWLockAcquireMain`. WAL records are emitted in-crate via the xloginsert
//! seams (the same idiom clog uses). Everything else `multixact.c` reaches —
//! recovery flags, varsup, transaction-status lookups, two-phase commit,
//! postmaster signaling, database-name lookup, critical-section markers, the
//! delay-chkpt flag — goes through the owner's seam crate (panics until that
//! owner lands).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::VecDeque;

use backend_utils_error::errno::current_errno;
use backend_utils_error::{elog, ereport, PgError, PgResult};
use types_error::{
    ErrorLocation, DEBUG1, ERRCODE_DATA_CORRUPTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, LOG, WARNING,
};

/// `ErrorLocation` for this translation unit's `ereport`s.
fn here(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("multixact.c", 0, funcname)
}

/// `dlog(level, msg)` for a sub-ERROR level (DEBUG/LOG), which never returns an
/// `Err`; the `Result` is discarded to mirror C's `void` `dlog()`.
fn dlog(level: types_error::ErrorLevel, message: String) {
    let _ = elog(level, message);
}

use backend_access_transam_slru::{
    check_slru_buffers, SimpleLruDoesPhysicalPageExist, SimpleLruInit, SimpleLruReadPage,
    SimpleLruReadPage_ReadOnly, SimpleLruShmemSize, SimpleLruTruncate, SimpleLruWriteAll,
    SimpleLruWritePage, SimpleLruZeroPage, SlruCtlData, SlruDeleteSegment,
    SlruPagePrecedesUnitTests, SlruScanDirectory, SlruSyncFileTag, SLRU_PAGES_PER_SEGMENT,
};
use backend_storage_lmgr_lwlock::{LWLockAcquire, LWLockAcquireMain, LWLockRelease, MainLWLockGuard};

use backend_utils_init_small::globals;

use backend_access_transam_multixact_seams as mx_seams;
use backend_access_transam_transam_seams as transam_seams;
use backend_access_transam_varsup_seams as varsup_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_access_transam_xlog_seams as xlog_seams;
use backend_access_transam_xloginsert_seams as xloginsert_seams;
use backend_commands_dbcommands_seams as dbcommands_seams;
use backend_storage_ipc_pmsignal_seams as pmsignal_seams;
use backend_storage_ipc_procarray_seams as procarray_seams;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_time_snapmgr_pc_seams as snapmgr_seams;
use backend_access_transam_twophase_seams as twophase_seams;

use mcx::{Mcx, MemoryContext, PgVec};

use types_core::{MultiXactId, MultiXactOffset, Oid, Size, TransactionId, BLCKSZ as BLCKSZ_U32};
use types_storage::storage::{LWLockMode, LW_EXCLUSIVE, MULTI_XACT_GEN_LOCK, MULTI_XACT_TRUNCATION_LOCK};
use types_storage::{
    LWTRANCHE_MULTIXACTMEMBER_BUFFER, LWTRANCHE_MULTIXACTMEMBER_SLRU,
    LWTRANCHE_MULTIXACTOFFSET_BUFFER, LWTRANCHE_MULTIXACTOFFSET_SLRU,
};
use types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler};
use types_wal::rmgr::XLogReaderState;
use types_wal::wal::{RM_MULTIXACT_ID, XLR_INFO_MASK};
use types_xlog_records::multixact::{
    xl_multixact_create, xl_multixact_truncate, MultiXactMember, MultiXactStatus,
    MAX_MULTI_XACT_STATUS, SIZE_OF_MULTI_XACT_CREATE, SIZE_OF_MULTI_XACT_TRUNCATE,
    XLOG_MULTIXACT_CREATE_ID, XLOG_MULTIXACT_TRUNCATE_ID, XLOG_MULTIXACT_ZERO_MEM_PAGE,
    XLOG_MULTIXACT_ZERO_OFF_PAGE,
};

/// `BLCKSZ` — the SLRU page size in bytes.
const BLCKSZ: usize = BLCKSZ_U32 as usize;

// ===========================================================================
// multixact.h / multixact.c constants
// ===========================================================================

/// `InvalidMultiXactId` — `(MultiXactId) 0`.
const InvalidMultiXactId: MultiXactId = 0;
/// `FirstMultiXactId` — `(MultiXactId) 1`.
const FirstMultiXactId: MultiXactId = 1;
/// `MaxMultiXactId` — `(MultiXactId) 0xFFFFFFFF`.
const MaxMultiXactId: MultiXactId = 0xFFFF_FFFF;
/// `MaxMultiXactOffset` — `(MultiXactOffset) 0xFFFFFFFF`.
const MaxMultiXactOffset: MultiXactOffset = 0xFFFF_FFFF;

// ===========================================================================
// Page / offset math (multixact.c lines 96-218)
// ===========================================================================

/// `sizeof(MultiXactOffset)` — four bytes per offset.
const SIZEOF_MULTIXACT_OFFSET: usize = core::mem::size_of::<MultiXactOffset>();
/// `sizeof(TransactionId)` — four bytes per member xid.
const SIZEOF_TRANSACTION_ID: usize = core::mem::size_of::<TransactionId>();

/// `MULTIXACT_OFFSETS_PER_PAGE` — multixact offset entries per offsets page.
pub const MULTIXACT_OFFSETS_PER_PAGE: u32 = (BLCKSZ / SIZEOF_MULTIXACT_OFFSET) as u32;

#[inline]
fn MultiXactIdToOffsetPage(multi: MultiXactId) -> i64 {
    (multi / MULTIXACT_OFFSETS_PER_PAGE) as i64
}

#[inline]
fn MultiXactIdToOffsetEntry(multi: MultiXactId) -> i32 {
    (multi % MULTIXACT_OFFSETS_PER_PAGE) as i32
}

#[inline]
fn MultiXactIdToOffsetSegment(multi: MultiXactId) -> i64 {
    MultiXactIdToOffsetPage(multi) / SLRU_PAGES_PER_SEGMENT
}

/* We need eight bits per xact, so one xact fits in a byte */
const MXACT_MEMBER_BITS_PER_XACT: u32 = 8;
const MXACT_MEMBER_XACT_BITMASK: u32 = (1 << MXACT_MEMBER_BITS_PER_XACT) - 1;

/* how many full bytes of flags are there in a group? */
const MULTIXACT_FLAGBYTES_PER_GROUP: usize = 4;
const MULTIXACT_MEMBERS_PER_MEMBERGROUP: u32 = MULTIXACT_FLAGBYTES_PER_GROUP as u32; // FLAGS_PER_BYTE == 1
/// size in bytes of a complete group.
const MULTIXACT_MEMBERGROUP_SIZE: usize = SIZEOF_TRANSACTION_ID
    * MULTIXACT_MEMBERS_PER_MEMBERGROUP as usize
    + MULTIXACT_FLAGBYTES_PER_GROUP;
const MULTIXACT_MEMBERGROUPS_PER_PAGE: u32 = (BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE) as u32;
const MULTIXACT_MEMBERS_PER_PAGE: u32 =
    MULTIXACT_MEMBERGROUPS_PER_PAGE * MULTIXACT_MEMBERS_PER_MEMBERGROUP;

/// number of members in the last page of the last segment.
const MAX_MEMBERS_IN_LAST_MEMBERS_PAGE: u32 = (0xFFFF_FFFFu32 % MULTIXACT_MEMBERS_PER_PAGE) + 1;

#[inline]
fn MXOffsetToMemberPage(offset: MultiXactOffset) -> i64 {
    (offset / MULTIXACT_MEMBERS_PER_PAGE) as i64
}

#[inline]
fn MXOffsetToMemberSegment(offset: MultiXactOffset) -> i64 {
    MXOffsetToMemberPage(offset) / SLRU_PAGES_PER_SEGMENT
}

/// Location (byte offset within page) of flag word for a given member.
#[inline]
fn MXOffsetToFlagsOffset(offset: MultiXactOffset) -> usize {
    let group = offset / MULTIXACT_MEMBERS_PER_MEMBERGROUP;
    let grouponpg = group % MULTIXACT_MEMBERGROUPS_PER_PAGE;
    grouponpg as usize * MULTIXACT_MEMBERGROUP_SIZE
}

#[inline]
fn MXOffsetToFlagsBitShift(offset: MultiXactOffset) -> u32 {
    let member_in_group = offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP;
    member_in_group * MXACT_MEMBER_BITS_PER_XACT
}

/// Location (byte offset within page) of TransactionId of given member.
#[inline]
fn MXOffsetToMemberOffset(offset: MultiXactOffset) -> usize {
    let member_in_group = offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP;
    MXOffsetToFlagsOffset(offset)
        + MULTIXACT_FLAGBYTES_PER_GROUP
        + member_in_group as usize * SIZEOF_TRANSACTION_ID
}

/* Multixact members wraparound thresholds. */
const MULTIXACT_MEMBER_SAFE_THRESHOLD: MultiXactOffset = MaxMultiXactOffset / 2;
const MULTIXACT_MEMBER_DANGER_THRESHOLD: MultiXactOffset =
    MaxMultiXactOffset - MaxMultiXactOffset / 4;

/// Number of segments-from-stop-limit at which we start warning.
const OFFSET_WARN_SEGMENTS: MultiXactOffset = 20;

#[inline]
fn PreviousMultiXactId(multi: MultiXactId) -> MultiXactId {
    if multi == FirstMultiXactId {
        MaxMultiXactId
    } else {
        multi - 1
    }
}

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != 0
}

#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    (id1.wrapping_sub(id2) as i32) < 0
}

// ===========================================================================
// Status helpers (status stored on-page as a u32 byte; we carry MultiXactStatus)
// ===========================================================================

/// `ISUPDATE_from_mxstatus(status)`.
#[inline]
fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    status.is_update()
}

/// The on-page byte value of a member status.
#[inline]
fn status_word(status: MultiXactStatus) -> u32 {
    status.as_i32() as u32
}

/// `mxstatus_to_string` — name a MultiXact member status (printed in
/// diagnostics). C's switch has no default; an out-of-range value `dlog(ERROR)`s.
fn mxstatus_to_string(status: MultiXactStatus) -> &'static str {
    match status {
        MultiXactStatus::ForKeyShare => "keysh",
        MultiXactStatus::ForShare => "sh",
        MultiXactStatus::ForNoKeyUpdate => "fornokeyupd",
        MultiXactStatus::ForUpdate => "forupd",
        MultiXactStatus::NoKeyUpdate => "nokeyupd",
        MultiXactStatus::Update => "upd",
    }
}

// ===========================================================================
// Shared state (multixact.c lines 242-336)
// ===========================================================================

/// The fixed (scalar) head of multixact.c's `MultiXactStateData` struct,
/// protected by `MultiXactGenLock`. The two per-backend `OldestMemberMXactId[]`
/// / `OldestVisibleMXactId[]` arrays are owned alongside it.
#[derive(Debug)]
struct MultiXactStateData {
    /// next-to-be-assigned MultiXactId
    nextMXact: MultiXactId,
    /// next-to-be-assigned offset
    nextOffset: MultiXactOffset,
    /// have we completed multixact startup?
    finishedStartup: bool,
    /// oldest multixact still potentially referenced by a relation
    oldestMultiXactId: MultiXactId,
    oldestMultiXactDB: Oid,
    /// oldest multixact offset potentially referenced (if known)
    oldestOffset: MultiXactOffset,
    oldestOffsetKnown: bool,
    /* anti-wraparound measures (multis) */
    multiVacLimit: MultiXactId,
    multiWarnLimit: MultiXactId,
    multiStopLimit: MultiXactId,
    multiWrapLimit: MultiXactId,
    /* anti-wraparound measures (members) */
    offsetStopLimit: MultiXactOffset,
    /// `OldestMemberMXactId` — `perBackendXactIds[0 .. MaxOldestSlot]`.
    oldest_member: Vec<MultiXactId>,
    /// `OldestVisibleMXactId` — `perBackendXactIds[MaxOldestSlot .. 2*MaxOldestSlot]`.
    oldest_visible: Vec<MultiXactId>,
}

impl MultiXactStateData {
    fn new(max_oldest_slot: usize) -> Self {
        MultiXactStateData {
            nextMXact: 0,
            nextOffset: 0,
            finishedStartup: false,
            oldestMultiXactId: 0,
            oldestMultiXactDB: 0,
            oldestOffset: 0,
            oldestOffsetKnown: false,
            multiVacLimit: 0,
            multiWarnLimit: 0,
            multiStopLimit: 0,
            multiWrapLimit: 0,
            offsetStopLimit: 0,
            oldest_member: vec![InvalidMultiXactId; max_oldest_slot],
            oldest_visible: vec![InvalidMultiXactId; max_oldest_slot],
        }
    }
}

/// Backend-local MultiXactId cache entry (`mXactCacheEnt`). Members kept sorted
/// by `mxactMemberComparator`.
#[derive(Clone)]
struct MXactCacheEnt {
    multi: MultiXactId,
    members: Vec<MultiXactMember>,
}

const MAX_CACHE_ENTRIES: usize = 256;

// ===========================================================================
// Thread-local backend/shared state (the multixact.c file-statics)
// ===========================================================================

thread_local! {
    /// `static SlruCtlData MultiXactOffsetCtlData;` / `#define MultiXactOffsetCtl`.
    static MXACT_OFFSET_CTL: RefCell<Option<SlruCtlData>> = const { RefCell::new(None) };
    /// `static SlruCtlData MultiXactMemberCtlData;` / `#define MultiXactMemberCtl`.
    static MXACT_MEMBER_CTL: RefCell<Option<SlruCtlData>> = const { RefCell::new(None) };
    /// `MultiXactState` (shared `MultiXactStateData` + per-backend arrays).
    static MXACT_STATE: RefCell<Option<MultiXactStateData>> = const { RefCell::new(None) };
    /// Backend-local cache (`MXactCache`), a dclist; head is most-recent.
    static MXACT_CACHE: RefCell<VecDeque<MXactCacheEnt>> = const { RefCell::new(VecDeque::new()) };
    /// The memory context the cache + arrays are charged to (`MultiXactMemoryContext`).
    static MXACT_MCXT: RefCell<Option<MemoryContext>> = RefCell::new(None);
    /// `pre_initialized_offsets_page` — hack for old-minor-version WAL replay.
    static PRE_INITIALIZED_OFFSETS_PAGE: RefCell<i64> = const { RefCell::new(-1) };
}

/// Run `f` with mutable access to `MultiXactOffsetCtl`.
fn with_offset_ctl<R>(f: impl FnOnce(&mut SlruCtlData) -> R) -> R {
    MXACT_OFFSET_CTL.with(|c| {
        let mut slot = c.borrow_mut();
        let ctl = slot
            .as_mut()
            .expect("MultiXactOffsetCtl used before MultiXactShmemInit");
        f(ctl)
    })
}

/// Run `f` with mutable access to `MultiXactMemberCtl`.
fn with_member_ctl<R>(f: impl FnOnce(&mut SlruCtlData) -> R) -> R {
    MXACT_MEMBER_CTL.with(|c| {
        let mut slot = c.borrow_mut();
        let ctl = slot
            .as_mut()
            .expect("MultiXactMemberCtl used before MultiXactShmemInit");
        f(ctl)
    })
}

/// Run `f` with mutable access to the shared `MultiXactState`.
fn with_state<R>(f: impl FnOnce(&mut MultiXactStateData) -> R) -> R {
    MXACT_STATE.with(|s| {
        let mut slot = s.borrow_mut();
        let st = slot
            .as_mut()
            .expect("MultiXactState used before MultiXactShmemInit");
        f(st)
    })
}

fn max_oldest_slot() -> usize {
    (globals::MaxBackends() + globals::max_prepared_xacts()) as usize
}

// ===========================================================================
// LWLock helpers (the fixed MultiXactGenLock / MultiXactTruncationLock)
// ===========================================================================

#[inline]
fn gen_lock_acquire(exclusive: bool) -> PgResult<MainLWLockGuard> {
    let mode = if exclusive {
        LWLockMode::LW_EXCLUSIVE
    } else {
        LWLockMode::LW_SHARED
    };
    LWLockAcquireMain(MULTI_XACT_GEN_LOCK, mode, globals::MyProcNumber())
}

#[inline]
fn trunc_lock_acquire(exclusive: bool) -> PgResult<MainLWLockGuard> {
    let mode = if exclusive {
        LWLockMode::LW_EXCLUSIVE
    } else {
        LWLockMode::LW_SHARED
    };
    LWLockAcquireMain(MULTI_XACT_TRUNCATION_LOCK, mode, globals::MyProcNumber())
}

// SLRU bank-lock helpers (mirror clog), recomputing bankno = pageno % nbanks.

#[inline]
fn bank_number(ctl: &SlruCtlData, pageno: i64) -> usize {
    (pageno % ctl.nbanks as i64) as usize
}

fn acquire_offset_bank_lock(pageno: i64, mode: LWLockMode) -> PgResult<()> {
    with_offset_ctl(|ctl| {
        let bankno = bank_number(ctl, pageno);
        LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, mode, globals::MyProcNumber())
    })?;
    Ok(())
}

fn release_offset_bank_lock(pageno: i64) -> PgResult<()> {
    with_offset_ctl(|ctl| {
        let bankno = bank_number(ctl, pageno);
        LWLockRelease(&ctl.shared.bank_locks[bankno].lock)
    })
}

fn acquire_member_bank_lock(pageno: i64, mode: LWLockMode) -> PgResult<()> {
    with_member_ctl(|ctl| {
        let bankno = bank_number(ctl, pageno);
        LWLockAcquire(&ctl.shared.bank_locks[bankno].lock, mode, globals::MyProcNumber())
    })?;
    Ok(())
}

fn release_member_bank_lock(pageno: i64) -> PgResult<()> {
    with_member_ctl(|ctl| {
        let bankno = bank_number(ctl, pageno);
        LWLockRelease(&ctl.shared.bank_locks[bankno].lock)
    })
}

// ===========================================================================
// SLRU page-buffer accessors (MultiXactOffset is 4-byte, native endian)
// ===========================================================================

#[inline]
fn read_offset_entry(ctl: &SlruCtlData, slotno: usize, entry: usize) -> MultiXactOffset {
    let start = entry * SIZEOF_MULTIXACT_OFFSET;
    let buf = ctl.shared.page_buffer(slotno);
    MultiXactOffset::from_ne_bytes(
        buf[start..start + SIZEOF_MULTIXACT_OFFSET]
            .try_into()
            .expect("4-byte offset"),
    )
}

#[inline]
fn write_offset_entry(ctl: &mut SlruCtlData, slotno: usize, entry: usize, value: MultiXactOffset) {
    let start = entry * SIZEOF_MULTIXACT_OFFSET;
    let buf = ctl.shared.page_buffer_mut(slotno);
    buf[start..start + SIZEOF_MULTIXACT_OFFSET].copy_from_slice(&value.to_ne_bytes());
}

// ===========================================================================
// MultiXact creation / expansion
// ===========================================================================

/// `MultiXactIdCreate` — construct a MultiXactId representing two TransactionIds.
pub fn MultiXactIdCreate(
    xid1: TransactionId,
    status1: MultiXactStatus,
    xid2: TransactionId,
    status2: MultiXactStatus,
) -> PgResult<MultiXactId> {
    debug_assert!(TransactionIdIsValid(xid1));
    debug_assert!(TransactionIdIsValid(xid2));
    debug_assert!(xid1 != xid2 || status1 != status2);

    // Note: unlike MultiXactIdExpand, we don't bother to check that both XIDs
    // are still running. In typical usage, xid2 will be our own XID and the
    // caller just did a check on xid1, so it would be wasted effort.
    let mut members = [
        MultiXactMember {
            xid: xid1,
            status: Some(status1),
        },
        MultiXactMember {
            xid: xid2,
            status: Some(status2),
        },
    ];

    MultiXactIdCreateFromMembers(&mut members)
}

/// `MultiXactIdExpand` — add a TransactionId to a pre-existing MultiXactId,
/// returning a new MultiXactId.
pub fn MultiXactIdExpand(
    multi: MultiXactId,
    xid: TransactionId,
    status: MultiXactStatus,
) -> PgResult<MultiXactId> {
    debug_assert!(MultiXactIdIsValid(multi));
    debug_assert!(TransactionIdIsValid(xid));

    let members = GetMultiXactIdMembers(multi, false, false)?;

    let Some(members) = members else {
        // The MultiXactId is obsolete. This can only happen if all the
        // original member XIDs are below GlobalXmin; create a singleton.
        let mut member = [MultiXactMember {
            xid,
            status: Some(status),
        }];
        return MultiXactIdCreateFromMembers(&mut member);
    };

    // If the xid is already a member of the multixact with the same status,
    // there is no need to expand it.
    for m in &members {
        if m.xid == xid && m.status == Some(status) {
            return Ok(multi);
        }
    }

    // Determine which of the members of the MultiXactId are still of interest.
    let transaction_xmin = snapmgr_seams::transaction_xmin::call()?;
    let mut new_members: Vec<MultiXactMember> = Vec::with_capacity(members.len() + 1);
    for m in &members {
        let keep = procarray_seams::transaction_id_is_in_progress::call(m.xid)?
            || (m.status.is_some_and(ISUPDATE_from_mxstatus)
                && transam_seams::transaction_id_did_commit::call(m.xid, transaction_xmin)?);
        if keep {
            new_members.push(*m);
        }
    }
    new_members.push(MultiXactMember {
        xid,
        status: Some(status),
    });

    MultiXactIdCreateFromMembers(&mut new_members)
}

/// `MultiXactIdIsRunning` — whether at least one member of `multi` is running.
pub fn MultiXactIdIsRunning(multi: MultiXactId, is_lock_only: bool) -> PgResult<bool> {
    let members = GetMultiXactIdMembers(multi, false, is_lock_only)?;

    let Some(members) = members else {
        return Ok(false);
    };
    if members.is_empty() {
        return Ok(false);
    }

    // Checking for myself is cheap compared to looking in shared memory;
    // return true if any live subtransaction of the current top-level
    // transaction is a member.
    for m in &members {
        if xact_seams::transaction_id_is_current_transaction_id::call(m.xid) {
            return Ok(true);
        }
    }

    // This could be made faster by sorting the array; but in practice the
    // array is small so it's probably not worth the trouble.
    for m in &members {
        if procarray_seams::transaction_id_is_in_progress::call(m.xid)? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `MultiXactIdSetOldestMember` — save the oldest MultiXactId this transaction
/// could be a member of.
pub fn MultiXactIdSetOldestMember() -> PgResult<()> {
    let me = globals::MyProcNumber() as usize;
    let already = with_state(|st| MultiXactIdIsValid(st.oldest_member[me]));
    if !already {
        let guard = gen_lock_acquire(false)?;

        with_state(|st| {
            // We have to beware of the possibility that nextMXact is in the
            // wrapped-around state. We don't fix the counter itself here, but
            // we must be sure to store a valid value in our array entry.
            let mut next_mxact = st.nextMXact;
            if next_mxact < FirstMultiXactId {
                next_mxact = FirstMultiXactId;
            }
            st.oldest_member[me] = next_mxact;
        });

        guard.release()?;
    }
    Ok(())
}

/// `MultiXactIdSetOldestVisible` — save the oldest MultiXactId this transaction
/// considers possibly live.
fn MultiXactIdSetOldestVisible() -> PgResult<()> {
    let me = globals::MyProcNumber() as usize;
    let already = with_state(|st| MultiXactIdIsValid(st.oldest_visible[me]));
    if !already {
        let nslots = max_oldest_slot();
        let guard = gen_lock_acquire(true)?;

        with_state(|st| {
            let mut oldest_mxact = st.nextMXact;
            if oldest_mxact < FirstMultiXactId {
                oldest_mxact = FirstMultiXactId;
            }

            for i in 0..nslots {
                let thisoldest = st.oldest_member[i];
                if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldest_mxact) {
                    oldest_mxact = thisoldest;
                }
            }

            st.oldest_visible[me] = oldest_mxact;
        });

        guard.release()?;
    }
    Ok(())
}

/// `ReadNextMultiXactId` — return the next MultiXactId to be assigned.
pub fn ReadNextMultiXactId() -> PgResult<MultiXactId> {
    let guard = gen_lock_acquire(false)?;
    let mut mxid = with_state(|st| st.nextMXact);
    guard.release()?;

    if mxid < FirstMultiXactId {
        mxid = FirstMultiXactId;
    }
    Ok(mxid)
}

/// `ReadMultiXactIdRange` — the (oldest, next) MultiXactId range.
pub fn ReadMultiXactIdRange() -> PgResult<(MultiXactId, MultiXactId)> {
    let guard = gen_lock_acquire(false)?;
    let (mut oldest, mut next) = with_state(|st| (st.oldestMultiXactId, st.nextMXact));
    guard.release()?;

    if oldest < FirstMultiXactId {
        oldest = FirstMultiXactId;
    }
    if next < FirstMultiXactId {
        next = FirstMultiXactId;
    }
    Ok((oldest, next))
}

/// `MultiXactIdCreateFromMembers` — make a new MultiXactId from an explicit
/// member set (sorted in-place). Records XLOG, SLRU and cache entries.
pub fn MultiXactIdCreateFromMembers(members: &mut [MultiXactMember]) -> PgResult<MultiXactId> {
    let nmembers = members.len();

    // See if the same set of members already exists in our cache; if so, just
    // re-use that MultiXactId. (Note: it might seem that looking in our cache
    // is insufficient, and we ought to search disk to see if a match exists.
    // But that would be wrong: it's conceivable that a member set could exist
    // in two different multixacts, so we can't rely on a member-set match.)
    let multi = mXactCacheGetBySet(members);
    if MultiXactIdIsValid(multi) {
        debug_assert!(false || true); // (debug elog skipped)
        return Ok(multi);
    }

    // Assert only one of the members is an update.
    {
        let mut has_update = false;
        for m in members.iter() {
            if m.status.is_some_and(ISUPDATE_from_mxstatus) {
                if has_update {
                    return Err(PgError::error(format!(
                        "new multixact has more than one updating member: {}",
                        mxid_to_string(InvalidMultiXactId, members)
                    )));
                }
                has_update = true;
            }
        }
    }

    // Assign the MXID and offsets range to use, and make sure there is space in
    // the OFFSETs and MEMBERs files. NB: this routine does START_CRIT_SECTION().
    let (multi, offset) = GetNewMultiXactId(nmembers as i32)?;

    // Make an XLOG entry describing the new MXID.
    {
        let header = xl_multixact_create {
            mid: multi,
            moff: offset,
            nmembers: nmembers as i32,
        };
        // XLogBeginInsert(); XLogRegisterData(&xlrec, SizeOfMultiXactCreate);
        // XLogRegisterData(members, nmembers * sizeof(MultiXactMember));
        let mut body: Vec<u8> = Vec::with_capacity(SIZE_OF_MULTI_XACT_CREATE + nmembers * 8);
        body.extend_from_slice(&header.to_bytes());
        for m in members.iter() {
            body.extend_from_slice(&m.to_bytes());
        }
        xloginsert_seams::xlog_insert::call(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, 0, &[&body])?;
    }

    // Now enter the information into the OFFSETs and MEMBERs logs.
    RecordNewMultiXact(multi, offset, members)?;

    // Done with critical section.
    miscinit_seams::end_crit_section::call();

    // Store the new MultiXactId in the local cache, too.
    mXactCachePut(multi, members)?;

    Ok(multi)
}

/// `RecordNewMultiXact` — write info about a new multixact into the offsets and
/// members files. Broken out so WAL replay can reuse it.
fn RecordNewMultiXact(
    multi: MultiXactId,
    offset: MultiXactOffset,
    members: &[MultiXactMember],
) -> PgResult<()> {
    let nmembers = members.len();

    let pageno = MultiXactIdToOffsetPage(multi);
    let entryno = MultiXactIdToOffsetEntry(multi);

    // Compute the next free offset, i.e. the offset of the next multixact.
    let mut next = multi.wrapping_add(1);
    if next < FirstMultiXactId {
        next = FirstMultiXactId;
    }
    let next_pageno = MultiXactIdToOffsetPage(next);
    let next_entryno = MultiXactIdToOffsetEntry(next);

    // Note: in older PG minor versions the page wasn't initialized here. Mirror
    // the modern fix: pre-zero the next page during recovery if needed.
    let latest_off = with_offset_ctl(|ctl| ctl.shared.latest_page_number.read() as i64);
    if xlog_seams::in_recovery::call() && next_pageno != pageno && latest_off == pageno {
        acquire_offset_bank_lock(next_pageno, LW_EXCLUSIVE)?;

        let slotno = with_offset_ctl(|ctl| SimpleLruZeroPage(ctl, next_pageno))?;
        with_offset_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
        debug_assert!(!with_offset_ctl(|ctl| ctl.shared.page_dirty[slotno]));

        release_offset_bank_lock(next_pageno)?;

        PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow_mut() = next_pageno);
    }

    // Set the offset for this multixact's first member.
    acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;

    let mut slotno = with_offset_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, multi))?;
    {
        let off_idx = entryno as usize;
        let existing = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, off_idx));
        if existing != offset {
            debug_assert_eq!(existing, 0);
            with_offset_ctl(|ctl| {
                write_offset_entry(ctl, slotno, off_idx, offset);
                ctl.shared.page_dirty[slotno] = true;
            });
        }
    }

    // Set the offset for the next multixact (= end of this one's members).
    let mut next_offset = offset.wrapping_add(nmembers as u32);
    if next_offset == 0 {
        next_offset = 1;
    }

    if next_pageno == pageno {
        let next_idx = entryno as usize + 1;
        let existing = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, next_idx));
        if existing != next_offset {
            debug_assert_eq!(existing, 0);
            with_offset_ctl(|ctl| {
                write_offset_entry(ctl, slotno, next_idx, next_offset);
                ctl.shared.page_dirty[slotno] = true;
            });
        }
    } else {
        debug_assert!(next_entryno == 0 || next == FirstMultiXactId);

        // Swap the lock for a lock on the next page.
        release_offset_bank_lock(pageno)?;
        acquire_offset_bank_lock(next_pageno, LW_EXCLUSIVE)?;

        slotno = with_offset_ctl(|ctl| SimpleLruReadPage(ctl, next_pageno, true, next))?;
        let next_idx = next_entryno as usize;
        let existing = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, next_idx));
        if existing != next_offset {
            debug_assert_eq!(existing, 0);
            with_offset_ctl(|ctl| {
                write_offset_entry(ctl, slotno, next_idx, next_offset);
                ctl.shared.page_dirty[slotno] = true;
            });
        }
        release_offset_bank_lock(next_pageno)?;
        // Re-acquire the original page lock so the unified release below works.
        acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;
    }

    // Release MultiXactOffset SLRU lock.
    release_offset_bank_lock(pageno)?;

    // Write the members.
    let mut prev_pageno: i64 = -1;
    let mut prev_bankno: Option<usize> = None;
    let mut held_member_page: Option<i64> = None;
    let mut mslotno: usize = 0;
    let mut off = offset;

    for m in members {
        let status = m.status.expect("multixact member status must be set on write");
        debug_assert!(status.as_i32() <= MAX_MULTI_XACT_STATUS);

        let mpageno = MXOffsetToMemberPage(off);
        let memberoff = MXOffsetToMemberOffset(off);
        let flagsoff = MXOffsetToFlagsOffset(off);
        let bshift = MXOffsetToFlagsBitShift(off);

        if mpageno != prev_pageno {
            let bankno = with_member_ctl(|ctl| bank_number(ctl, mpageno));
            if Some(bankno) != prev_bankno {
                if let Some(prev) = held_member_page.take() {
                    release_member_bank_lock(prev)?;
                }
                acquire_member_bank_lock(mpageno, LW_EXCLUSIVE)?;
                held_member_page = Some(mpageno);
                prev_bankno = Some(bankno);
            }
            mslotno = with_member_ctl(|ctl| SimpleLruReadPage(ctl, mpageno, true, multi))?;
            prev_pageno = mpageno;
        }

        with_member_ctl(|ctl| {
            let buf = ctl.shared.page_buffer_mut(mslotno);
            // *memberptr = members[i].xid
            buf[memberoff..memberoff + SIZEOF_TRANSACTION_ID].copy_from_slice(&m.xid.to_ne_bytes());

            // flags
            let mut flagsval = u32::from_ne_bytes(
                buf[flagsoff..flagsoff + 4].try_into().expect("4-byte flags"),
            );
            flagsval &= !(((1u32 << MXACT_MEMBER_BITS_PER_XACT) - 1) << bshift);
            flagsval |= status_word(status) << bshift;
            buf[flagsoff..flagsoff + 4].copy_from_slice(&flagsval.to_ne_bytes());

            ctl.shared.page_dirty[mslotno] = true;
        });

        off = off.wrapping_add(1);
    }

    if let Some(prev) = held_member_page.take() {
        release_member_bank_lock(prev)?;
    }

    Ok(())
}

/// `GetNewMultiXactId` — get the next MultiXactId and reserve member space.
/// Returns `(result, offset)`. Starts a critical section that the caller ends.
fn GetNewMultiXactId(mut nmembers: i32) -> PgResult<(MultiXactId, MultiXactOffset)> {
    // safety check, we should never get this far in a HS standby
    if xlog_seams::recovery_in_progress::call() {
        return Err(PgError::error(
            "cannot assign MultiXactIds during recovery".to_string(),
        ));
    }

    let mut guard = gen_lock_acquire(true)?;

    // Handle wraparound of the nextMXact counter.
    with_state(|st| {
        if st.nextMXact < FirstMultiXactId {
            st.nextMXact = FirstMultiXactId;
        }
    });

    // Assign the MXID.
    let mut result = with_state(|st| st.nextMXact);

    let past_vac_limit = with_state(|st| !MultiXactIdPrecedes(result, st.multiVacLimit));
    if past_vac_limit {
        let (multi_warn_limit, multi_stop_limit, multi_wrap_limit, oldest_datoid) = with_state(|st| {
            (
                st.multiWarnLimit,
                st.multiStopLimit,
                st.multiWrapLimit,
                st.oldestMultiXactDB,
            )
        });

        // emit warning/stop OUTSIDE the lock
        guard.release()?;

        if globals::IsUnderPostmaster() && !MultiXactIdPrecedes(result, multi_stop_limit) {
            let oldest_datname = get_database_name_string(oldest_datoid)?;

            // Immediately kick autovacuum into action.
            pmsignal_seams::send_postmaster_signal_start_autovac::call();

            let hint = "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.";
            return match oldest_datname {
                Some(name) => Err(PgError::error(format!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database \"{name}\""
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_hint(hint)),
                None => Err(PgError::error(format!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database with OID {oldest_datoid}"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_hint(hint)),
            };
        }

        // To avoid swamping the postmaster with signals, only request once per
        // 64K multis generated.
        if globals::IsUnderPostmaster() && result.is_multiple_of(65536) {
            pmsignal_seams::send_postmaster_signal_start_autovac::call();
        }

        if !MultiXactIdPrecedes(result, multi_warn_limit) {
            let oldest_datname = get_database_name_string(oldest_datoid)?;
            let remaining = multi_wrap_limit.wrapping_sub(result);
            let msg = match &oldest_datname {
                Some(name) => multixactid_warning_msg_named(name, remaining),
                None => multixactid_warning_msg_oid(oldest_datoid, remaining),
            };
            ereport(WARNING)
                .errmsg_internal(msg)
                .errhint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(here("GetNewMultiXactId"))?;
        }

        // Re-acquire lock and start over.
        guard = gen_lock_acquire(true)?;
        result = with_state(|st| {
            let mut r = st.nextMXact;
            if r < FirstMultiXactId {
                r = FirstMultiXactId;
            }
            r
        });
    }

    // Make sure there is room for the actual offset of the next multixact in
    // the offsets file. Assigning this MXID sets the next MXID's offset already.
    ExtendMultiXactOffset(result.wrapping_add(1))?;

    // Reserve the members space, similarly to above. Also avoid returning zero
    // as the starting offset for any multixact.
    let next_offset = with_state(|st| st.nextOffset);
    let offset;
    if next_offset == 0 {
        offset = 1;
        nmembers += 1; // allocate member slot 0 too
    } else {
        offset = next_offset;
    }

    // Protect against overrun of the members space as well.
    let (oldest_offset_known, offset_stop_limit, oldest_offset, oldest_db) = with_state(|st| {
        (
            st.oldestOffsetKnown,
            st.offsetStopLimit,
            st.oldestOffset,
            st.oldestMultiXactDB,
        )
    });

    if oldest_offset_known
        && MultiXactOffsetWouldWrap(offset_stop_limit, next_offset, nmembers as u32)
    {
        guard.release()?;
        pmsignal_seams::send_postmaster_signal_start_autovac::call();

        let remaining = offset_stop_limit.wrapping_sub(next_offset).wrapping_sub(1);
        return Err(PgError::error("multixact \"members\" limit exceeded".to_string())
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(members_limit_detail(remaining, nmembers as u32))
            .with_hint(format!(
                "Execute a database-wide VACUUM in database with OID {oldest_db} with reduced \"vacuum_multixact_freeze_min_age\" and \"vacuum_multixact_freeze_table_age\" settings."
            )));
    }

    // Check whether we should kick autovacuum into action, to prevent members
    // wraparound. NB: only do so when crossing a segment boundary.
    if (!oldest_offset_known
        || (next_offset.wrapping_sub(oldest_offset) > MULTIXACT_MEMBER_SAFE_THRESHOLD))
        && (MXOffsetToMemberPage(next_offset) / SLRU_PAGES_PER_SEGMENT)
            != (MXOffsetToMemberPage(next_offset.wrapping_add(nmembers as u32))
                / SLRU_PAGES_PER_SEGMENT)
    {
        pmsignal_seams::send_postmaster_signal_start_autovac::call();
    }

    if oldest_offset_known
        && MultiXactOffsetWouldWrap(
            offset_stop_limit,
            next_offset,
            nmembers as u32
                + MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32 * OFFSET_WARN_SEGMENTS,
        )
    {
        let remaining = offset_stop_limit
            .wrapping_sub(next_offset)
            .wrapping_add(nmembers as u32);
        ereport(WARNING)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg_internal(members_warning_msg(oldest_db, remaining))
            .errhint(
                "Execute a database-wide VACUUM in that database with reduced \"vacuum_multixact_freeze_min_age\" and \"vacuum_multixact_freeze_table_age\" settings.",
            )
            .finish(here("GetNewMultiXactId"))?;
    }

    ExtendMultiXactMember(next_offset, nmembers)?;

    // Critical section from here until caller has written the data into the
    // just-reserved SLRU space; we don't want to error out with a partly
    // written MultiXact structure in shared memory.
    miscinit_seams::start_crit_section::call();

    // Advance the counters, so that other backends can use the just-allocated
    // values.
    with_state(|st| {
        st.nextMXact = st.nextMXact.wrapping_add(1);
        st.nextOffset = st.nextOffset.wrapping_add(nmembers as u32);
    });

    guard.release()?;

    Ok((result, offset))
}

/// `GetMultiXactIdMembers` — return the members of `multi`, or `None` for an
/// empty/obsolete multixact (the C function's `-1` return).
pub fn GetMultiXactIdMembers(
    multi: MultiXactId,
    from_pgupgrade: bool,
    is_lock_only: bool,
) -> PgResult<Option<Vec<MultiXactMember>>> {
    // Fast-path returns.
    if !MultiXactIdIsValid(multi) || from_pgupgrade {
        return Ok(None);
    }

    // See if the MultiXactId is in the local cache.
    if let Some(cached) = mXactCacheGetById(multi) {
        return Ok(Some(cached));
    }

    // We need to set our OldestVisibleMXactId[] entry, but we don't want to do
    // that until after we've checked that the multixact does not precede
    // oldestMultiXactId.
    MultiXactIdSetOldestVisible()?;

    // If we know the multi is used only for locking and not for updates, then
    // we can skip checking if the value is older than our oldest visible multi.
    let me = globals::MyProcNumber() as usize;
    let oldest_visible_me = with_state(|st| st.oldest_visible[me]);
    if is_lock_only && MultiXactIdPrecedes(multi, oldest_visible_me) {
        return Ok(None);
    }

    // Acquire the shared lock just long enough to grab the current counter
    // values.
    let guard = gen_lock_acquire(false)?;
    let (oldest_mxact, next_mxact, next_offset) =
        with_state(|st| (st.oldestMultiXactId, st.nextMXact, st.nextOffset));
    guard.release()?;

    if MultiXactIdPrecedes(multi, oldest_mxact) {
        return Err(PgError::error(format!(
            "MultiXactId {multi} does no longer exist -- apparent wraparound"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    if !MultiXactIdPrecedes(multi, next_mxact) {
        return Err(PgError::error(format!(
            "MultiXactId {multi} has not been created yet -- apparent wraparound"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    // Find out the offset at which we need to start reading MultiXactMembers
    // and the number of members in the multixact.
    let mut pageno = MultiXactIdToOffsetPage(multi);
    let mut entryno = MultiXactIdToOffsetEntry(multi);

    acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;

    let mut slotno = with_offset_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, multi))?;
    let offset = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, entryno as usize));
    debug_assert!(offset != 0);

    // Use the same increment rule as GetNewMultiXactId(), that is, don't handle
    // wraparound explicitly until needed. Store the result in a signed int like
    // C, so a wrapped/corrupt difference becomes negative and skips the loop.
    let mut tmp_mxact = multi.wrapping_add(1);
    let length: i32;

    if next_mxact == tmp_mxact {
        // Corner case 1: there is no next multixact.
        length = next_offset.wrapping_sub(offset) as i32;
    } else {
        if tmp_mxact < FirstMultiXactId {
            tmp_mxact = FirstMultiXactId;
        }

        let prev_pageno = pageno;
        pageno = MultiXactIdToOffsetPage(tmp_mxact);
        entryno = MultiXactIdToOffsetEntry(tmp_mxact);

        if pageno != prev_pageno {
            let newbank = with_offset_ctl(|ctl| bank_number(ctl, pageno));
            let oldbank = with_offset_ctl(|ctl| bank_number(ctl, prev_pageno));
            if newbank != oldbank {
                release_offset_bank_lock(prev_pageno)?;
                acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;
            }
            slotno = with_offset_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, tmp_mxact))?;
        }

        let next_mx_offset = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, entryno as usize));
        if next_mx_offset == 0 {
            release_offset_bank_lock(pageno)?;
            return Err(PgError::error(format!("MultiXact {multi} has invalid next offset"))
                .with_sqlstate(ERRCODE_DATA_CORRUPTED));
        }

        length = next_mx_offset.wrapping_sub(offset) as i32;
    }

    release_offset_bank_lock(pageno)?;

    // C: ptr = palloc(length * sizeof(MultiXactMember)); a non-positive length
    // allocates nothing and the loop below does not execute.
    let capacity = length.max(0) as usize;
    let mut ptr: Vec<MultiXactMember> = Vec::with_capacity(capacity);

    let mut prev_pageno: i64 = -1;
    let mut off = offset;
    let mut prev_bankno: Option<usize> = None;
    let mut held_member_page: Option<i64> = None;
    let mut mslotno: usize = 0;

    for _ in 0..length {
        let mpageno = MXOffsetToMemberPage(off);
        let memberoff = MXOffsetToMemberOffset(off);

        if mpageno != prev_pageno {
            let bankno = with_member_ctl(|ctl| bank_number(ctl, mpageno));
            if Some(bankno) != prev_bankno {
                if let Some(prev) = held_member_page.take() {
                    release_member_bank_lock(prev)?;
                }
                acquire_member_bank_lock(mpageno, LW_EXCLUSIVE)?;
                held_member_page = Some(mpageno);
                prev_bankno = Some(bankno);
            }
            mslotno = with_member_ctl(|ctl| SimpleLruReadPage(ctl, mpageno, true, multi))?;
            prev_pageno = mpageno;
        }

        let xid = with_member_ctl(|ctl| {
            let buf = ctl.shared.page_buffer(mslotno);
            u32::from_ne_bytes(
                buf[memberoff..memberoff + SIZEOF_TRANSACTION_ID]
                    .try_into()
                    .expect("4-byte xid"),
            )
        });

        // Corner case 2: next multixact is wrapped around to its first
        // possible value (member offset 0). Skip the unused slot.
        if !TransactionIdIsValid(xid) {
            debug_assert_eq!(off, 0);
            off = off.wrapping_add(1);
            continue;
        }

        let flagsoff = MXOffsetToFlagsOffset(off);
        let bshift = MXOffsetToFlagsBitShift(off);
        let flagsval = with_member_ctl(|ctl| {
            let buf = ctl.shared.page_buffer(mslotno);
            u32::from_ne_bytes(buf[flagsoff..flagsoff + 4].try_into().expect("4-byte flags"))
        });

        let status_byte = (flagsval >> bshift) & MXACT_MEMBER_XACT_BITMASK;
        ptr.push(MultiXactMember {
            xid,
            status: MultiXactStatus::from_i32(status_byte as i32),
        });

        off = off.wrapping_add(1);
    }

    if let Some(prev) = held_member_page.take() {
        release_member_bank_lock(prev)?;
    }

    debug_assert!(!ptr.is_empty());

    // Copy the result into the local cache.
    mXactCachePut(multi, &ptr)?;

    Ok(Some(ptr))
}

// ===========================================================================
// MultiXact cache management (multixact.c lines 1644-1814)
// ===========================================================================

/// `mxactMemberComparator` ordering: by xid, then status. NOT wraparound-aware.
fn mxact_member_cmp(a: &MultiXactMember, b: &MultiXactMember) -> core::cmp::Ordering {
    let astat = a.status.map(|s| s.as_i32()).unwrap_or(-1);
    let bstat = b.status.map(|s| s.as_i32()).unwrap_or(-1);
    a.xid.cmp(&b.xid).then(astat.cmp(&bstat))
}

fn members_eq(a: &[MultiXactMember], b: &[MultiXactMember]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.xid == y.xid && x.status == y.status)
}

/// `mXactCacheGetBySet` — look up a cached MultiXactId by member set. Sorts
/// `members` in place.
fn mXactCacheGetBySet(members: &mut [MultiXactMember]) -> MultiXactId {
    members.sort_by(mxact_member_cmp);

    MXACT_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let found = cache
            .iter()
            .position(|entry| members_eq(&entry.members, members));
        if let Some(idx) = found {
            let entry = cache.remove(idx).expect("index within bounds");
            let multi = entry.multi;
            cache.push_front(entry); // dclist_move_head
            multi
        } else {
            InvalidMultiXactId
        }
    })
}

/// `mXactCacheGetById` — look up cached members for a MultiXactId.
fn mXactCacheGetById(multi: MultiXactId) -> Option<Vec<MultiXactMember>> {
    MXACT_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let idx = cache.iter().position(|entry| entry.multi == multi)?;
        let entry = cache.remove(idx).expect("index within bounds");
        let members = entry.members.clone();
        cache.push_front(entry); // dclist_move_head
        Some(members)
    })
}

/// `mXactCachePut` — add a MultiXactId and its set to the local cache.
fn mXactCachePut(multi: MultiXactId, members: &[MultiXactMember]) -> PgResult<()> {
    // C allocates MXactCacheContext lazily the first time we insert.
    ensure_mxact_mcxt();

    let mut sorted: Vec<MultiXactMember> = members.to_vec();
    sorted.sort_by(mxact_member_cmp);

    MXACT_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        cache.push_front(MXactCacheEnt {
            multi,
            members: sorted,
        });
        // Release oldest cache entry, if cache too large.
        if cache.len() > MAX_CACHE_ENTRIES {
            cache.pop_back();
        }
    });
    Ok(())
}

/// Clear the backend-local cache.
fn cache_clear() {
    MXACT_CACHE.with(|c| c.borrow_mut().clear());
}

/// Lazily create the multixact memory context (`MXactContext`) on first use.
fn ensure_mxact_mcxt() {
    MXACT_MCXT.with(|m| {
        let mut slot = m.borrow_mut();
        if slot.is_none() {
            *slot = Some(MemoryContext::new("MultiXact"));
        }
    });
}

/// `mxid_to_string` — format a MultiXact and its members for diagnostics.
pub fn mxid_to_string(multi: MultiXactId, members: &[MultiXactMember]) -> String {
    if members.is_empty() {
        return format!("{multi} 0[]");
    }
    let stat = |m: &MultiXactMember| m.status.map(mxstatus_to_string).unwrap_or("unknown");
    let mut buf = format!(
        "{multi} {}[{} ({})",
        members.len(),
        members[0].xid,
        stat(&members[0])
    );
    for m in &members[1..] {
        buf.push_str(&format!(", {} ({})", m.xid, stat(m)));
    }
    buf.push(']');
    buf
}

// ===========================================================================
// Transaction-boundary hooks (multixact.c lines 1864-2002)
// ===========================================================================

/// `AtEOXact_MultiXact` — reset per-transaction MultiXact state at xact end.
pub fn AtEOXact_MultiXact() {
    let me = globals::MyProcNumber() as usize;
    // The dummy assignments are not strictly necessary, but they help to keep
    // the state machine clean; the OldestMemberMXactId[]/OldestVisibleMXactId[]
    // entries are reset without locking (this backend owns them).
    with_state(|st| {
        st.oldest_member[me] = InvalidMultiXactId;
        st.oldest_visible[me] = InvalidMultiXactId;
    });

    // Discard the local MultiXactId cache.
    cache_clear();
}

/// `AtPrepare_MultiXact` — save multixact state at 2PC prepare.
pub fn AtPrepare_MultiXact() -> PgResult<()> {
    let me = globals::MyProcNumber() as usize;
    let my_oldest_member = with_state(|st| st.oldest_member[me]);

    if MultiXactIdIsValid(my_oldest_member) {
        twophase_seams::register_two_phase_record::call(
            TWOPHASE_RM_MULTIXACT_ID,
            0,
            &my_oldest_member.to_ne_bytes(),
        )?;
    }
    Ok(())
}

/// `PostPrepare_MultiXact` — clean up after successful PREPARE TRANSACTION.
pub fn PostPrepare_MultiXact(xid: TransactionId) -> PgResult<()> {
    let me = globals::MyProcNumber() as usize;
    let my_oldest_member = with_state(|st| st.oldest_member[me]);
    if MultiXactIdIsValid(my_oldest_member) {
        let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)? as usize;

        // Even though storing MultiXactId is atomic, acquire lock to make sure
        // others see both changes, not just the reset of the slot of the
        // current backend.
        let guard = gen_lock_acquire(true)?;
        with_state(|st| {
            st.oldest_member[dummy] = my_oldest_member;
            st.oldest_member[me] = InvalidMultiXactId;
        });
        guard.release()?;
    }

    // We don't need to transfer OldestVisibleMXactId value, because the
    // transaction is not going to be looking at any more multixacts once it's
    // prepared.

    // Discard the local MultiXactId cache like normal.
    cache_clear();
    Ok(())
}

/// `multixact_twophase_recover` — restore MultiXact state from a 2PC file.
pub fn multixact_twophase_recover(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)? as usize;

    // Get the oldest member XID from the state file record, and set it in the
    // OldestMemberMXactId slot reserved for this prepared transaction.
    debug_assert_eq!(recdata.len(), core::mem::size_of::<MultiXactId>());
    let oldest_member =
        MultiXactId::from_ne_bytes(recdata[..4].try_into().expect("4-byte MultiXactId"));

    with_state(|st| st.oldest_member[dummy] = oldest_member);
    Ok(())
}

/// `multixact_twophase_postcommit` — finalize MultiXact state after 2PC commit.
pub fn multixact_twophase_postcommit(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, true)? as usize;
    debug_assert_eq!(recdata.len(), core::mem::size_of::<MultiXactId>());
    with_state(|st| st.oldest_member[dummy] = InvalidMultiXactId);
    Ok(())
}

/// `multixact_twophase_postabort` — same as the COMMIT case.
pub fn multixact_twophase_postabort(
    xid: TransactionId,
    info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    multixact_twophase_postcommit(xid, info, recdata)
}

// ===========================================================================
// Shared memory / startup (multixact.c lines 2004-2346)
// ===========================================================================

/// `MultiXactOffsetCtl`/`MultiXactMemberCtl` buffer counts (GUC-driven).
fn MultiXactOffsetBuffers() -> i32 {
    globals::multixact_offset_buffers()
}
fn MultiXactMemberBuffers() -> i32 {
    globals::multixact_member_buffers()
}

/// `MultiXactShmemSize` — shared-memory size for both SLRUs + control.
pub fn MultiXactShmemSize() -> Size {
    let nslots = max_oldest_slot();
    // SHARED_MULTIXACT_STATE_SIZE: header + perBackendXactIds[2*nslots]. The
    // header offset is 48 bytes on a 64-bit target (12 scalar fields padded to
    // the array's 4-byte alignment).
    let mut size: Size = 48 + core::mem::size_of::<MultiXactId>() * 2 * nslots;
    size += SimpleLruShmemSize(MultiXactOffsetBuffers(), 0);
    size += SimpleLruShmemSize(MultiXactMemberBuffers(), 0);
    size
}

/// `MultiXactShmemInit` — initialize MultiXact shared state and SLRUs.
pub fn MultiXactShmemInit() -> PgResult<()> {
    let mut offset_ctl = SimpleLruInit(
        "multixact_offset",
        MultiXactOffsetBuffers(),
        0,
        "pg_multixact/offsets",
        LWTRANCHE_MULTIXACTOFFSET_BUFFER,
        LWTRANCHE_MULTIXACTOFFSET_SLRU,
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_OFFSET,
        false,
    )?;
    offset_ctl.PagePrecedes = Some(MultiXactOffsetPagePrecedes);
    SlruPagePrecedesUnitTests(&offset_ctl, MULTIXACT_OFFSETS_PER_PAGE as i32);

    let mut member_ctl = SimpleLruInit(
        "multixact_member",
        MultiXactMemberBuffers(),
        0,
        "pg_multixact/members",
        LWTRANCHE_MULTIXACTMEMBER_BUFFER,
        LWTRANCHE_MULTIXACTMEMBER_SLRU,
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_MEMBER,
        false,
    )?;
    member_ctl.PagePrecedes = Some(MultiXactMemberPagePrecedes);
    // doesn't divide evenly into a page, so the unit-test helper is skipped
    // for members in C (MULTIXACT_MEMBERS_PER_PAGE * MULTIXACT_MEMBERGROUP_SIZE
    // != BLCKSZ).

    let nslots = max_oldest_slot();

    MXACT_OFFSET_CTL.with(|c| *c.borrow_mut() = Some(offset_ctl));
    MXACT_MEMBER_CTL.with(|c| *c.borrow_mut() = Some(member_ctl));
    MXACT_STATE.with(|s| *s.borrow_mut() = Some(MultiXactStateData::new(nslots)));
    PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow_mut() = -1);
    Ok(())
}

/// `check_multixact_offset_buffers` GUC check_hook.
pub fn check_multixact_offset_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("multixact_offset_buffers", newval)
}

/// `check_multixact_member_buffers` GUC check_hook.
pub fn check_multixact_member_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("multixact_member_buffers", newval)
}

/// `BootStrapMultiXact` — create the initial MultiXact segments.
pub fn BootStrapMultiXact() -> PgResult<()> {
    acquire_offset_bank_lock(0, LW_EXCLUSIVE)?;
    let slotno = ZeroMultiXactOffsetPage(0, false)?;
    with_offset_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
    debug_assert!(!with_offset_ctl(|ctl| ctl.shared.page_dirty[slotno]));
    release_offset_bank_lock(0)?;

    acquire_member_bank_lock(0, LW_EXCLUSIVE)?;
    let slotno = ZeroMultiXactMemberPage(0, false)?;
    with_member_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
    debug_assert!(!with_member_ctl(|ctl| ctl.shared.page_dirty[slotno]));
    release_member_bank_lock(0)?;

    Ok(())
}

/// `ZeroMultiXactOffsetPage` — zero an offsets page, optionally WAL-logged.
fn ZeroMultiXactOffsetPage(pageno: i64, write_xlog: bool) -> PgResult<usize> {
    let slotno = with_offset_ctl(|ctl| SimpleLruZeroPage(ctl, pageno))?;
    if write_xlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_OFF_PAGE)?;
    }
    Ok(slotno)
}

/// `ZeroMultiXactMemberPage` — zero a members page, optionally WAL-logged.
fn ZeroMultiXactMemberPage(pageno: i64, write_xlog: bool) -> PgResult<usize> {
    let slotno = with_member_ctl(|ctl| SimpleLruZeroPage(ctl, pageno))?;
    if write_xlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_MEM_PAGE)?;
    }
    Ok(slotno)
}

/// `MaybeExtendOffsetSlru` — after binary upgrade, create missing offsets pages.
fn MaybeExtendOffsetSlru() -> PgResult<()> {
    let pageno = MultiXactIdToOffsetPage(with_state(|st| st.nextMXact));
    acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;

    if !with_offset_ctl(|ctl| SimpleLruDoesPhysicalPageExist(ctl, pageno))? {
        let slotno = ZeroMultiXactOffsetPage(pageno, false)?;
        with_offset_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
    }

    release_offset_bank_lock(pageno)?;
    Ok(())
}

/// `StartupMultiXact` — initialize the SLRUs' idea of their latest page number.
pub fn StartupMultiXact() -> PgResult<()> {
    let (multi, offset) = with_state(|st| (st.nextMXact, st.nextOffset));

    let pageno = MultiXactIdToOffsetPage(multi);
    with_offset_ctl(|ctl| ctl.shared.latest_page_number.write(pageno as u64));

    let pageno = MXOffsetToMemberPage(offset);
    with_member_ctl(|ctl| ctl.shared.latest_page_number.write(pageno as u64));

    Ok(())
}

/// `TrimMultiXact` — zero the tails of the current pages at recovery end.
pub fn TrimMultiXact() -> PgResult<()> {
    let guard = gen_lock_acquire(false)?;
    let (next_mxact, offset, oldest_mxact, oldest_mxact_db) = with_state(|st| {
        (
            st.nextMXact,
            st.nextOffset,
            st.oldestMultiXactId,
            st.oldestMultiXactDB,
        )
    });
    guard.release()?;

    // Clean up offsets state by re-initializing the latest page number.
    let pageno = MultiXactIdToOffsetPage(next_mxact);
    with_offset_ctl(|ctl| ctl.shared.latest_page_number.write(pageno as u64));

    // Zero out the remainder of the current offsets page. See notes in
    // TrimCLOG() for motivation.
    let entryno = MultiXactIdToOffsetEntry(next_mxact);
    {
        acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;
        let slotno = if entryno == 0 {
            with_offset_ctl(|ctl| SimpleLruZeroPage(ctl, pageno))?
        } else {
            with_offset_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, next_mxact))?
        };

        with_offset_ctl(|ctl| {
            write_offset_entry(ctl, slotno, entryno as usize, offset);
            if entryno != 0 && (entryno as usize + 1) * SIZEOF_MULTIXACT_OFFSET != BLCKSZ {
                let start = (entryno as usize + 1) * SIZEOF_MULTIXACT_OFFSET;
                ctl.shared.page_buffer_mut(slotno)[start..BLCKSZ].fill(0);
            }
            ctl.shared.page_dirty[slotno] = true;
        });
        release_offset_bank_lock(pageno)?;
    }

    // And the same for members.
    let pageno = MXOffsetToMemberPage(offset);
    with_member_ctl(|ctl| ctl.shared.latest_page_number.write(pageno as u64));

    let flagsoff = MXOffsetToFlagsOffset(offset);
    if flagsoff != 0 {
        acquire_member_bank_lock(pageno, LW_EXCLUSIVE)?;
        let memberoff = MXOffsetToMemberOffset(offset);
        let slotno = with_member_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, offset))?;

        with_member_ctl(|ctl| {
            ctl.shared.page_buffer_mut(slotno)[memberoff..BLCKSZ].fill(0);
            ctl.shared.page_dirty[slotno] = true;
        });
        release_member_bank_lock(pageno)?;
    }

    // signal that we're officially up
    let guard = gen_lock_acquire(true)?;
    with_state(|st| st.finishedStartup = true);
    guard.release()?;

    // Now compute how far away the next members wraparound is.
    SetMultiXactIdLimit(oldest_mxact, oldest_mxact_db, true)?;

    Ok(())
}

// ===========================================================================
// Checkpoint / limits / advancement (multixact.c lines 2348-2612)
// ===========================================================================

/// `MultiXactGetCheckptMulti` — snapshot of MultiXact state for a checkpoint.
pub fn MultiXactGetCheckptMulti(
    _is_shutdown: bool,
) -> PgResult<(MultiXactId, MultiXactOffset, MultiXactId, Oid)> {
    let guard = gen_lock_acquire(false)?;
    let result = with_state(|st| {
        (
            st.nextMXact,
            st.nextOffset,
            st.oldestMultiXactId,
            st.oldestMultiXactDB,
        )
    });
    guard.release()?;
    Ok(result)
}

/// `CheckPointMultiXact` — flush dirty MultiXact pages at a checkpoint.
pub fn CheckPointMultiXact() -> PgResult<()> {
    // Write dirty MultiXact pages to disk. This may result in sync requests
    // queued for later handling by ProcessSyncRequests(), as part of the
    // checkpoint.
    with_offset_ctl(|ctl| SimpleLruWriteAll(ctl, true))?;
    with_member_ctl(|ctl| SimpleLruWriteAll(ctl, true))?;
    Ok(())
}

/// `MultiXactSetNextMXact` — set the next MultiXactId and member offset.
pub fn MultiXactSetNextMXact(
    next_multi: MultiXactId,
    next_multi_offset: MultiXactOffset,
) -> PgResult<()> {
    let guard = gen_lock_acquire(true)?;
    with_state(|st| {
        st.nextMXact = next_multi;
        st.nextOffset = next_multi_offset;
    });
    guard.release()?;

    // During a binary upgrade, make sure that the offsets SLRU is large enough
    // to contain the next value that would be created.
    if globals::IsBinaryUpgrade() {
        MaybeExtendOffsetSlru()?;
    }
    Ok(())
}

/// `SetMultiXactIdLimit` — set wraparound-protection limits.
pub fn SetMultiXactIdLimit(
    oldest_datminmxid: MultiXactId,
    oldest_datoid: Oid,
    is_startup: bool,
) -> PgResult<()> {
    debug_assert!(MultiXactIdIsValid(oldest_datminmxid));

    // We pretend that a wrap will happen halfway through the multixact ID space,
    // but that's not really true, because multixacts wrap differently from
    // transaction IDs. Note that, separately from any concern about multixact
    // IDs wrapping, we must ensure that multixact members do not wrap.
    let mut multi_wrap_limit = oldest_datminmxid.wrapping_add(MaxMultiXactId >> 1);
    if multi_wrap_limit < FirstMultiXactId {
        multi_wrap_limit = multi_wrap_limit.wrapping_add(FirstMultiXactId);
    }

    // We'll refuse to continue assigning MultiXactIds once we get within 3M
    // multi of data loss.
    let mut multi_stop_limit = multi_wrap_limit.wrapping_sub(3_000_000);
    if multi_stop_limit < FirstMultiXactId {
        multi_stop_limit = multi_stop_limit.wrapping_sub(FirstMultiXactId);
    }

    // We'll start complaining loudly when we get within 40M multis of data loss.
    let mut multi_warn_limit = multi_wrap_limit.wrapping_sub(40_000_000);
    if multi_warn_limit < FirstMultiXactId {
        multi_warn_limit = multi_warn_limit.wrapping_sub(FirstMultiXactId);
    }

    // We'll start trying to force autovacuums when oldest_datminmxid gets to be
    // older than autovacuum_multixact_freeze_max_age mxids old.
    let freeze_max_age = backend_utils_misc_guc_tables::vars::autovacuum_multixact_freeze_max_age.read();
    let mut multi_vac_limit = oldest_datminmxid.wrapping_add(freeze_max_age as u32);
    if multi_vac_limit < FirstMultiXactId {
        multi_vac_limit = multi_vac_limit.wrapping_add(FirstMultiXactId);
    }

    // Grab lock for just long enough to set the new limit values.
    let guard = gen_lock_acquire(true)?;
    let (cur_multi, finished_startup) = with_state(|st| {
        st.oldestMultiXactId = oldest_datminmxid;
        st.oldestMultiXactDB = oldest_datoid;
        st.multiVacLimit = multi_vac_limit;
        st.multiWarnLimit = multi_warn_limit;
        st.multiStopLimit = multi_stop_limit;
        st.multiWrapLimit = multi_wrap_limit;
        (st.nextMXact, st.finishedStartup)
    });
    guard.release()?;

    dlog(
        DEBUG1,
        format!("MultiXactId wrap limit is {multi_wrap_limit}, limited by database with OID {oldest_datoid}"),
    );

    // Computing the actual limits is only possible once the data directory is in
    // a consistent state. There's no need to compute the limits while still
    // replaying WAL.
    if !finished_startup {
        return Ok(());
    }

    debug_assert!(!xlog_seams::in_recovery::call());

    // Set limits for offset vacuum.
    let needs_offset_vacuum = SetOffsetVacuumLimit(is_startup)?;

    // If past the autovacuum force point, immediately signal an autovac request.
    if (MultiXactIdPrecedes(multi_vac_limit, cur_multi) || needs_offset_vacuum)
        && globals::IsUnderPostmaster()
    {
        pmsignal_seams::send_postmaster_signal_start_autovac::call();
    }

    // Give an immediate warning if past the wrap warn point.
    if MultiXactIdPrecedes(multi_warn_limit, cur_multi) {
        let oldest_datname = if xact_seams::is_transaction_state::call() {
            get_database_name_string(oldest_datoid)?
        } else {
            None
        };
        let remaining = multi_wrap_limit.wrapping_sub(cur_multi);
        let msg = match &oldest_datname {
            Some(name) => multixactid_warning_msg_named(name, remaining),
            None => multixactid_warning_msg_oid(oldest_datoid, remaining),
        };
        ereport(WARNING)
            .errmsg_internal(msg)
            .errhint(
                "To avoid MultiXactId assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
            )
            .finish(here("SetMultiXactIdLimit"))?;
    }

    Ok(())
}

/// `MultiXactAdvanceNextMXact` — advance next id/offset during replay.
pub fn MultiXactAdvanceNextMXact(
    min_multi: MultiXactId,
    min_multi_offset: MultiXactOffset,
) -> PgResult<()> {
    let guard = gen_lock_acquire(true)?;
    let (set_multi, set_offset) = with_state(|st| {
        let mut set_multi = false;
        let mut set_offset = false;
        if MultiXactIdPrecedes(st.nextMXact, min_multi) {
            st.nextMXact = min_multi;
            set_multi = true;
        }
        if MultiXactOffsetPrecedes(st.nextOffset, min_multi_offset) {
            st.nextOffset = min_multi_offset;
            set_offset = true;
        }
        (set_multi, set_offset)
    });
    guard.release()?;
    if set_multi {
        dlog(DEBUG1, format!("MultiXact: setting next multi to {min_multi}"));
    }
    if set_offset {
        dlog(
            DEBUG1,
            format!("MultiXact: setting next offset to {min_multi_offset}"),
        );
    }
    Ok(())
}

/// `MultiXactAdvanceOldest` — advance the oldest tracked multi during replay.
pub fn MultiXactAdvanceOldest(oldest_multi: MultiXactId, oldest_multi_db: Oid) -> PgResult<()> {
    debug_assert!(xlog_seams::in_recovery::call());

    if with_state(|st| MultiXactIdPrecedes(st.oldestMultiXactId, oldest_multi)) {
        SetMultiXactIdLimit(oldest_multi, oldest_multi_db, false)?;
    }
    Ok(())
}

/// `ExtendMultiXactOffset` — make room for a newly-allocated MultiXactId.
fn ExtendMultiXactOffset(multi: MultiXactId) -> PgResult<()> {
    // No work except at first MultiXactId of a page. But beware: just after
    // wraparound, the first MultiXactId of page zero is FirstMultiXactId.
    if MultiXactIdToOffsetEntry(multi) != 0 && multi != FirstMultiXactId {
        return Ok(());
    }

    let pageno = MultiXactIdToOffsetPage(multi);
    acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;

    // Zero the page and make a WAL entry about it.
    ZeroMultiXactOffsetPage(pageno, true)?;

    release_offset_bank_lock(pageno)?;
    Ok(())
}

/// `ExtendMultiXactMember` — make room for the members of a new MultiXactId.
fn ExtendMultiXactMember(offset: MultiXactOffset, nmembers: i32) -> PgResult<()> {
    let mut nmembers = nmembers;
    let mut offset = offset;

    while nmembers > 0 {
        // Only zero when at first entry of a page.
        let flagsoff = MXOffsetToFlagsOffset(offset);
        let flagsbit = MXOffsetToFlagsBitShift(offset);
        if flagsoff == 0 && flagsbit == 0 {
            let pageno = MXOffsetToMemberPage(offset);
            acquire_member_bank_lock(pageno, LW_EXCLUSIVE)?;
            ZeroMultiXactMemberPage(pageno, true)?;
            release_member_bank_lock(pageno)?;
        }

        // Compute the number of items till end of current page. Careful: if
        // addition of n members forces an overflow, we want to clamp to the
        // last members page.
        let difference = if offset.wrapping_add(MAX_MEMBERS_IN_LAST_MEMBERS_PAGE) < offset {
            MaxMultiXactOffset - offset + 1
        } else {
            MULTIXACT_MEMBERS_PER_PAGE - offset % MULTIXACT_MEMBERS_PER_PAGE
        };

        nmembers -= difference as i32;
        offset = offset.wrapping_add(difference);
    }
    Ok(())
}

/// `GetOldestMultiXactId` — the oldest MultiXactId still possibly seen as live.
pub fn GetOldestMultiXactId() -> PgResult<MultiXactId> {
    let nslots = max_oldest_slot();
    let guard = gen_lock_acquire(false)?;

    let oldest_mxact = with_state(|st| {
        let mut next_mxact = st.nextMXact;
        if next_mxact < FirstMultiXactId {
            next_mxact = FirstMultiXactId;
        }

        let mut oldest_mxact = next_mxact;
        for i in 0..nslots {
            let thisoldest = st.oldest_member[i];
            if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldest_mxact) {
                oldest_mxact = thisoldest;
            }
            let thisoldest = st.oldest_visible[i];
            if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldest_mxact) {
                oldest_mxact = thisoldest;
            }
        }
        oldest_mxact
    });

    guard.release()?;
    Ok(oldest_mxact)
}

/// `SetOffsetVacuumLimit` — determine and install member-space vacuum limits.
/// Returns true if emergency autovacuum is required.
fn SetOffsetVacuumLimit(is_startup: bool) -> PgResult<bool> {
    let trunc_guard = trunc_lock_acquire(false)?;

    let guard = gen_lock_acquire(false)?;
    let (
        oldest_multixact_id,
        next_mxact,
        next_offset,
        prev_oldest_offset_known,
        prev_oldest_offset,
        prev_offset_stop_limit,
    ) = with_state(|st| {
        debug_assert!(st.finishedStartup);
        (
            st.oldestMultiXactId,
            st.nextMXact,
            st.nextOffset,
            st.oldestOffsetKnown,
            st.oldestOffset,
            st.offsetStopLimit,
        )
    });
    guard.release()?;

    let mut oldest_offset: MultiXactOffset = 0;
    let mut oldest_offset_known;

    if oldest_multixact_id == next_mxact {
        // Either there are no multixacts, or we calculated wrong limits last
        // time. Either way, the safe oldest offset is nextOffset.
        oldest_offset = next_offset;
        oldest_offset_known = true;
    } else {
        match find_multixact_start(oldest_multixact_id)? {
            Some(off) => {
                oldest_offset = off;
                oldest_offset_known = true;
                dlog(
                    DEBUG1,
                    format!("oldest MultiXactId member is at offset {oldest_offset}"),
                );
            }
            None => {
                oldest_offset_known = false;
                dlog(
                    LOG,
                    format!("MultiXact member wraparound protections are disabled because oldest checkpointed MultiXact {oldest_multixact_id} does not exist on disk"),
                );
            }
        }
    }

    trunc_guard.release()?;

    let mut offset_stop_limit: MultiXactOffset = 0;

    if oldest_offset_known {
        // Find the oldest offset that is still part of a valid segment, then
        // back off to the start of that segment, and back off one more segment
        // to leave a buffer.
        offset_stop_limit = oldest_offset
            - (oldest_offset % (MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32));
        offset_stop_limit = offset_stop_limit
            .wrapping_sub(MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32);

        if !prev_oldest_offset_known && !is_startup {
            dlog(
                LOG,
                "MultiXact member wraparound protections are now enabled".to_string(),
            );
        }

        dlog(
            DEBUG1,
            format!("MultiXact member stop limit is now {offset_stop_limit} based on MultiXact {oldest_multixact_id}"),
        );
    } else if prev_oldest_offset_known {
        // If we failed to get the oldest offset this time, but we have a value
        // from a previous pass through this function, use the old values.
        oldest_offset = prev_oldest_offset;
        oldest_offset_known = true;
        offset_stop_limit = prev_offset_stop_limit;
    }

    let guard = gen_lock_acquire(true)?;
    with_state(|st| {
        st.oldestOffset = oldest_offset;
        st.oldestOffsetKnown = oldest_offset_known;
        st.offsetStopLimit = offset_stop_limit;
    });
    guard.release()?;

    Ok(!oldest_offset_known
        || (next_offset.wrapping_sub(oldest_offset) > MULTIXACT_MEMBER_SAFE_THRESHOLD))
}

/// `MultiXactOffsetWouldWrap` — whether adding distance to start passes boundary.
fn MultiXactOffsetWouldWrap(
    boundary: MultiXactOffset,
    start: MultiXactOffset,
    distance: u32,
) -> bool {
    debug_assert!(distance > 0);
    let mut finish = start.wrapping_add(distance);

    // If finish has wrapped around, then we've used more than the entire range
    // of offsets; that's not allowed, so we always treat that as a wrap.
    if finish < start {
        finish = finish.wrapping_add(1);
    }

    if start < boundary {
        finish >= boundary || finish < start
    } else {
        finish >= boundary && finish < start
    }
}

/// `find_multixact_start` — starting offset of `multi`, or None if its file is
/// not on disk.
fn find_multixact_start(multi: MultiXactId) -> PgResult<Option<MultiXactOffset>> {
    debug_assert!(with_state(|st| st.finishedStartup));

    let pageno = MultiXactIdToOffsetPage(multi);
    let entryno = MultiXactIdToOffsetEntry(multi);

    // Flush out dirty data, so PhysicalPageExists can work correctly.
    with_offset_ctl(|ctl| SimpleLruWriteAll(ctl, true))?;
    with_member_ctl(|ctl| SimpleLruWriteAll(ctl, true))?;

    if !with_offset_ctl(|ctl| SimpleLruDoesPhysicalPageExist(ctl, pageno))? {
        return Ok(None);
    }

    // lock is acquired by SimpleLruReadPage_ReadOnly
    let slotno = with_offset_ctl(|ctl| SimpleLruReadPage_ReadOnly(ctl, pageno, multi))?;
    let offset = with_offset_ctl(|ctl| read_offset_entry(ctl, slotno, entryno as usize));
    release_offset_bank_lock(pageno)?;

    Ok(Some(offset))
}

/// `ReadMultiXactCounts` — how many multixacts and members currently exist.
fn ReadMultiXactCounts() -> PgResult<Option<(u32, MultiXactOffset)>> {
    let guard = gen_lock_acquire(false)?;
    let (next_offset, oldest_multixact_id, next_multixact_id, oldest_offset, oldest_offset_known) =
        with_state(|st| {
            (
                st.nextOffset,
                st.oldestMultiXactId,
                st.nextMXact,
                st.oldestOffset,
                st.oldestOffsetKnown,
            )
        });
    guard.release()?;

    if !oldest_offset_known {
        return Ok(None);
    }

    let members = next_offset.wrapping_sub(oldest_offset);
    let multixacts = next_multixact_id.wrapping_sub(oldest_multixact_id);
    Ok(Some((multixacts, members)))
}

/// `MultiXactMemberFreezeThreshold` — aggressive-freeze threshold based on
/// member-space pressure.
pub fn MultiXactMemberFreezeThreshold() -> PgResult<i32> {
    let freeze_max_age =
        backend_utils_misc_guc_tables::vars::autovacuum_multixact_freeze_max_age.read();

    let Some((multixacts, members)) = ReadMultiXactCounts()? else {
        return Ok(0);
    };

    // If we're below the safe threshold, we can keep the default behavior.
    if members <= MULTIXACT_MEMBER_SAFE_THRESHOLD {
        return Ok(freeze_max_age);
    }

    let fraction = (members - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64
        / (MULTIXACT_MEMBER_DANGER_THRESHOLD - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64;
    let victim_multixacts = (multixacts as f64 * fraction) as u32;

    // fraction could be > 1.0, but lowest possible freeze age is zero.
    if victim_multixacts > multixacts {
        return Ok(0);
    }
    let result = (multixacts - victim_multixacts) as i32;

    Ok(result.min(freeze_max_age))
}

// ===========================================================================
// Truncation (multixact.c lines 3085-3354)
// ===========================================================================

/// `PerformMembersTruncation` — delete members segments [oldest, newOldest).
fn PerformMembersTruncation(
    oldest_offset: MultiXactOffset,
    new_oldest_offset: MultiXactOffset,
) -> PgResult<()> {
    let maxsegment = MXOffsetToMemberSegment(MaxMultiXactOffset);
    let startsegment = MXOffsetToMemberSegment(oldest_offset);
    let endsegment = MXOffsetToMemberSegment(new_oldest_offset);
    let mut segment = startsegment;

    // Delete all the segments but the last one. The last segment can still
    // contain, possibly partially, valid data.
    while segment != endsegment {
        with_member_ctl(|ctl| SlruDeleteSegment(ctl, segment))?;

        // Move to the next segment, handling wraparound correctly.
        if segment == maxsegment {
            segment = 0;
        } else {
            segment += 1;
        }
    }
    Ok(())
}

/// `PerformOffsetsTruncation` — delete offsets segments [oldest, newOldest).
fn PerformOffsetsTruncation(
    _oldest_multi: MultiXactId,
    new_oldest_multi: MultiXactId,
) -> PgResult<()> {
    // We step back one multixact to avoid passing a cutoff page that hasn't
    // been created yet in the rare case that oldestMulti would be the first on
    // a page and oldestMulti == nextMulti.
    with_offset_ctl(|ctl| {
        SimpleLruTruncate(
            ctl,
            MultiXactIdToOffsetPage(PreviousMultiXactId(new_oldest_multi)),
        )
    })
}

/// `TruncateMultiXact` — remove SLRU segments below the new oldest multi.
pub fn TruncateMultiXact(
    new_oldest_multi: MultiXactId,
    new_oldest_multi_db: Oid,
) -> PgResult<()> {
    debug_assert!(!xlog_seams::recovery_in_progress::call());
    debug_assert!(with_state(|st| st.finishedStartup));

    // We can only allow one truncation to happen at once. Otherwise parts of
    // members might vanish while we're doing lookups or similar.
    let trunc_guard = trunc_lock_acquire(true)?;

    let guard = gen_lock_acquire(false)?;
    let (next_multi, next_offset, oldest_multi) =
        with_state(|st| (st.nextMXact, st.nextOffset, st.oldestMultiXactId));
    guard.release()?;
    debug_assert!(MultiXactIdIsValid(oldest_multi));

    // Make sure there is enough to truncate.
    if MultiXactIdPrecedesOrEquals(new_oldest_multi, oldest_multi) {
        trunc_guard.release()?;
        return Ok(());
    }

    // Note we can't just plow ahead with the truncation; it's possible that
    // there are no segments to truncate, which is a problem because we are
    // going to attempt to read the offsets page to determine where to truncate
    // the members. So we first scan the directory to determine the earliest
    // offsets page number that we can read without error.
    let mut earliest_existing_page: i64 = -1;
    with_offset_ctl(|ctl| {
        let pp = ctl.PagePrecedes;
        SlruScanDirectory(ctl, |_ctl, _fname, segpage| {
            if earliest_existing_page == -1
                || pp.is_some_and(|p| p(segpage, earliest_existing_page))
            {
                earliest_existing_page = segpage;
            }
            Ok(false)
        })
    })?;
    let mut earliest = (earliest_existing_page as u32).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE);
    if earliest < FirstMultiXactId {
        earliest = FirstMultiXactId;
    }

    // If there's nothing to remove, we can bail out early.
    if MultiXactIdPrecedes(oldest_multi, earliest) {
        trunc_guard.release()?;
        return Ok(());
    }

    // First, compute the safe truncation point for MultiXactMember. This is the
    // starting offset of the oldest multixact.
    let oldest_offset = if oldest_multi == next_multi {
        // there are NO MultiXacts.
        next_offset
    } else {
        match find_multixact_start(oldest_multi)? {
            Some(off) => off,
            None => {
                dlog(
                    LOG,
                    format!("oldest MultiXact {oldest_multi} not found, earliest MultiXact {earliest}, skipping truncation"),
                );
                trunc_guard.release()?;
                return Ok(());
            }
        }
    };

    // Secondly compute up to where to truncate.
    let new_oldest_offset = if new_oldest_multi == next_multi {
        // there are NO MultiXacts.
        next_offset
    } else {
        match find_multixact_start(new_oldest_multi)? {
            Some(off) => off,
            None => {
                dlog(
                    LOG,
                    format!("cannot truncate up to MultiXact {new_oldest_multi} because it does not exist on disk, skipping truncation"),
                );
                trunc_guard.release()?;
                return Ok(());
            }
        }
    };

    if new_oldest_offset == 0 {
        dlog(
            LOG,
            format!("cannot truncate up to MultiXact {new_oldest_multi} because it has invalid offset, skipping truncation"),
        );
        trunc_guard.release()?;
        return Ok(());
    }

    dlog(
        DEBUG1,
        format!(
            "performing multixact truncation: offsets [{}, {}), offsets segments [{:X}, {:X}), members [{}, {}), members segments [{:X}, {:X})",
            oldest_multi,
            new_oldest_multi,
            MultiXactIdToOffsetSegment(oldest_multi),
            MultiXactIdToOffsetSegment(new_oldest_multi),
            oldest_offset,
            new_oldest_offset,
            MXOffsetToMemberSegment(oldest_offset),
            MXOffsetToMemberSegment(new_oldest_offset),
        ),
    );

    // Do truncation, and the WAL logging of the truncation, in a critical
    // section. That way RecoveryRestartPoint can't observe the WAL record while
    // the SLRUs haven't been truncated yet.
    miscinit_seams::start_crit_section::call();

    // Prevent checkpoints from being scheduled while we're truncating; see
    // multixact.c comment about DELAY_CHKPT_START.
    proc_seams::set_delay_chkpt_start::call(true);

    // Write the truncation WAL record.
    {
        let xlrec = xl_multixact_truncate {
            oldestMultiDB: new_oldest_multi_db,
            startTruncOff: oldest_multi,
            endTruncOff: new_oldest_multi,
            startTruncMemb: oldest_offset,
            endTruncMemb: new_oldest_offset,
        };
        let recptr = xloginsert_seams::xlog_insert::call(
            RM_MULTIXACT_ID,
            XLOG_MULTIXACT_TRUNCATE_ID,
            0,
            &[&xlrec.to_bytes()],
        )?;
        xlog_seams::xlog_flush::call(recptr)?;
    }

    // Update in-memory limits before performing the truncation, while inside the
    // critical section: Have to do it before truncation, to prevent concurrent
    // lookups of those multixacts. Since we're holding TruncationLock, no
    // concurrent truncations will be possible; and since the MultiXactId can
    // only ever be lookable once we've finished, no lookups can fail.
    let guard = gen_lock_acquire(true)?;
    with_state(|st| {
        st.oldestMultiXactId = new_oldest_multi;
        st.oldestMultiXactDB = new_oldest_multi_db;
    });
    guard.release()?;

    PerformMembersTruncation(oldest_offset, new_oldest_offset)?;
    PerformOffsetsTruncation(oldest_multi, new_oldest_multi)?;

    proc_seams::set_delay_chkpt_start::call(false);

    miscinit_seams::end_crit_section::call();
    trunc_guard.release()?;

    Ok(())
}

// ===========================================================================
// Page-precedes comparisons (multixact.c lines 3356-3435)
// ===========================================================================

/// `MultiXactOffsetPagePrecedes` — offsets page "older" test for truncation.
fn MultiXactOffsetPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut multi1 = (page1 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE);
    multi1 = multi1.wrapping_add(FirstMultiXactId + 1);
    let mut multi2 = (page2 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE);
    multi2 = multi2.wrapping_add(FirstMultiXactId + 1);

    MultiXactIdPrecedes(multi1, multi2)
        && MultiXactIdPrecedes(multi1, multi2.wrapping_add(MULTIXACT_OFFSETS_PER_PAGE - 1))
}

/// `MultiXactMemberPagePrecedes` — members page "older" test for truncation.
fn MultiXactMemberPagePrecedes(page1: i64, page2: i64) -> bool {
    let offset1 = (page1 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE);
    let offset2 = (page2 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE);

    MultiXactOffsetPrecedes(offset1, offset2)
        && MultiXactOffsetPrecedes(offset1, offset2.wrapping_add(MULTIXACT_MEMBERS_PER_PAGE - 1))
}

/// `MultiXactIdPrecedes` — wraparound-aware comparison.
pub fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

/// `MultiXactIdPrecedesOrEquals` — wraparound-aware comparison.
pub fn MultiXactIdPrecedesOrEquals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) <= 0
}

/// `MultiXactOffsetPrecedes` — decide which of two offsets is earlier.
fn MultiXactOffsetPrecedes(offset1: MultiXactOffset, offset2: MultiXactOffset) -> bool {
    (offset1.wrapping_sub(offset2) as i32) < 0
}

/// `MultiXactIdIsValid` helper.
#[inline]
fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

// ===========================================================================
// WAL records & redo (multixact.c lines 3437-3690)
// ===========================================================================

/// `WriteMZeroPageXlogRec` — emit an OFFSETs/MEMBERs zero-page WAL record.
fn WriteMZeroPageXlogRec(pageno: i64, info: u8) -> PgResult<()> {
    // XLogBeginInsert(); XLogRegisterData(&pageno, sizeof(int64));
    // (void) XLogInsert(RM_MULTIXACT_ID, info);
    xloginsert_seams::xlog_insert::call(RM_MULTIXACT_ID, info, 0, &[&pageno.to_ne_bytes()])?;
    Ok(())
}

/// `multixact_redo` — WAL redo handler for MultiXact records.
pub fn multixact_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("multixact_redo dispatched on a decoded record");
    let info = decoded.info() & !XLR_INFO_MASK;

    // Backup blocks are not used in multixact records.
    debug_assert!(decoded.max_block_id() < 0);

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE {
        let data = decoded.data();
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("8-byte pageno"));

        // Skip if this page was already initialized as part of multixact
        // creation, in a record that we've already replayed.
        let pre = PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow());
        if pre != pageno {
            acquire_offset_bank_lock(pageno, LW_EXCLUSIVE)?;
            let slotno = ZeroMultiXactOffsetPage(pageno, false)?;
            with_offset_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
            debug_assert!(!with_offset_ctl(|ctl| ctl.shared.page_dirty[slotno]));
            release_offset_bank_lock(pageno)?;
        } else {
            dlog(
                DEBUG1,
                format!("skipping initialization of offsets page {pageno} because it was already initialized on multixid creation"),
            );
        }
        PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow_mut() = -1);
        Ok(())
    } else if info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        let data = decoded.data();
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("8-byte pageno"));

        acquire_member_bank_lock(pageno, LW_EXCLUSIVE)?;
        let slotno = ZeroMultiXactMemberPage(pageno, false)?;
        with_member_ctl(|ctl| SimpleLruWritePage(ctl, slotno))?;
        debug_assert!(!with_member_ctl(|ctl| ctl.shared.page_dirty[slotno]));
        release_member_bank_lock(pageno)?;
        Ok(())
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        let data = decoded.data();
        let header = xl_multixact_create::from_bytes(data);
        let nmembers = header.nmembers as usize;
        let body = xl_multixact_create::members(data);
        let members: Vec<MultiXactMember> = (0..nmembers).map(|i| body.get(i)).collect();

        // If we're replaying an XLOG_MULTIXACT_CREATE_ID without a previous
        // ZERO_OFF_PAGE for an implicitly-initialized page, complain.
        let pre = PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow());
        if pre != -1 {
            dlog(
                LOG,
                format!("expected to see an XLOG_MULTIXACT_ZERO_OFF_PAGE record for page {pre} that was implicitly initialized earlier"),
            );
            PRE_INITIALIZED_OFFSETS_PAGE.with(|p| *p.borrow_mut() = -1);
        }

        // Store the data back into the SLRU files.
        RecordNewMultiXact(header.mid, header.moff, &members)?;

        // Make sure nextMXact/nextOffset are beyond what this record has.
        MultiXactAdvanceNextMXact(
            header.mid.wrapping_add(1),
            header.moff.wrapping_add(nmembers as u32),
        )?;

        // Make sure nextXid is beyond any XID mentioned in the record. This
        // should be unnecessary, since any XID found here ought to have other
        // evidence in the XLOG, but let's be safe.
        let mut max_xid = decoded.xid();
        for m in &members {
            if TransactionIdPrecedes(max_xid, m.xid) {
                max_xid = m.xid;
            }
        }
        varsup_seams::advance_next_full_xid_past_xid::call(max_xid)?;
        Ok(())
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        let xlrec = xl_multixact_truncate::from_bytes(decoded.data());

        dlog(
            DEBUG1,
            format!(
                "replaying multixact truncation: offsets [{}, {}), offsets segments [{:X}, {:X}), members [{}, {}), members segments [{:X}, {:X})",
                xlrec.startTruncOff,
                xlrec.endTruncOff,
                MultiXactIdToOffsetSegment(xlrec.startTruncOff),
                MultiXactIdToOffsetSegment(xlrec.endTruncOff),
                xlrec.startTruncMemb,
                xlrec.endTruncMemb,
                MXOffsetToMemberSegment(xlrec.startTruncMemb),
                MXOffsetToMemberSegment(xlrec.endTruncMemb),
            ),
        );

        // Should not be a problem to take the lock since we're in recovery.
        let trunc_guard = trunc_lock_acquire(true)?;

        // Update our oldest-multi limits.
        SetMultiXactIdLimit(xlrec.endTruncOff, xlrec.oldestMultiDB, false)?;

        // Finally, perform the actual truncation.
        PerformMembersTruncation(xlrec.startTruncMemb, xlrec.endTruncMemb)?;
        PerformOffsetsTruncation(xlrec.startTruncOff, xlrec.endTruncOff)?;

        trunc_guard.release()?;
        Ok(())
    } else {
        Err(PgError::error(format!("multixact_redo: unknown op code {info}")))
    }
}

/// `multixact_identify` — name a MultiXact WAL record type.
pub fn multixact_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_MULTIXACT_ZERO_OFF_PAGE => Some("ZERO_OFF_PAGE"),
        XLOG_MULTIXACT_ZERO_MEM_PAGE => Some("ZERO_MEM_PAGE"),
        XLOG_MULTIXACT_CREATE_ID => Some("CREATE_ID"),
        XLOG_MULTIXACT_TRUNCATE_ID => Some("TRUNCATE_ID"),
        _ => None,
    }
}

/// `pg_get_multixact_members` — the (xid, status-string) tuples of `mxid`. The
/// SRF tuple plumbing (fmgr calling convention) is the caller's; this is the
/// portable core.
pub fn pg_get_multixact_members(
    mxid: MultiXactId,
) -> PgResult<Vec<(TransactionId, &'static str)>> {
    if mxid < FirstMultiXactId {
        return Err(PgError::error(format!("invalid MultiXactId: {mxid}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let members = GetMultiXactIdMembers(mxid, false, false)?;
    let Some(members) = members else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(members.len());
    for m in &members {
        let status = m
            .status
            .ok_or_else(|| PgError::error(format!("unrecognized multixact status in MultiXactId {mxid}")))?;
        out.push((m.xid, mxstatus_to_string(status)));
    }
    Ok(out)
}

/// `multixactoffsetssyncfiletag` — sync an offsets segment file.
pub fn multixactoffsetssyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    let (result, path) = with_offset_ctl(|ctl| SlruSyncFileTag(ctl, &ftag))?;
    let errno = current_errno();
    Ok(FileTagOpResult {
        result,
        path,
        errno,
    })
}

/// `multixactmemberssyncfiletag` — sync a members segment file.
pub fn multixactmemberssyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    let (result, path) = with_member_ctl(|ctl| SlruSyncFileTag(ctl, &ftag))?;
    let errno = current_errno();
    Ok(FileTagOpResult {
        result,
        path,
        errno,
    })
}

// ===========================================================================
// Two-phase / message helpers
// ===========================================================================

/// `TWOPHASE_RM_MULTIXACT_ID` (twophase_rmgr.h) — the 2PC RM slot index.
const TWOPHASE_RM_MULTIXACT_ID: u8 = 5;

/// Read a database's name into a `String` for warning messages (the warnings
/// only need the name; the borrowed `PgString` is copied out before its scratch
/// context drops). `None` if the database no longer exists.
fn get_database_name_string(dbid: Oid) -> PgResult<Option<String>> {
    let ctx = MemoryContext::new("multixact get_database_name scratch");
    let name = with_db_name_mcx(ctx.mcx(), dbid)?;
    Ok(name)
}

fn with_db_name_mcx(mcx: Mcx<'_>, dbid: Oid) -> PgResult<Option<String>> {
    let s = dbcommands_seams::get_database_name::call(mcx, dbid)?;
    Ok(s.map(|name| name.as_str().to_string()))
}

fn multixactid_warning_msg_named(name: &str, n: u32) -> String {
    if n == 1 {
        format!("database \"{name}\" must be vacuumed before {n} more MultiXactId is used")
    } else {
        format!("database \"{name}\" must be vacuumed before {n} more MultiXactIds are used")
    }
}

fn multixactid_warning_msg_oid(oid: Oid, n: u32) -> String {
    if n == 1 {
        format!("database with OID {oid} must be vacuumed before {n} more MultiXactId is used")
    } else {
        format!("database with OID {oid} must be vacuumed before {n} more MultiXactIds are used")
    }
}

fn members_limit_detail(remaining: u32, nmembers: u32) -> String {
    if remaining == 1 {
        format!("This command would create a multixact with {nmembers} members, but the remaining space is only enough for {remaining} member.")
    } else {
        format!("This command would create a multixact with {nmembers} members, but the remaining space is only enough for {remaining} members.")
    }
}

fn members_warning_msg(oid: Oid, n: u32) -> String {
    if n == 1 {
        format!("database with OID {oid} must be vacuumed before {n} more multixact member is used")
    } else {
        format!("database with OID {oid} must be vacuumed before {n} more multixact members are used")
    }
}

/// Compile-time check that `SizeOfMultiXactTruncate` matches the ABI struct.
const _: () = assert!(SIZE_OF_MULTI_XACT_TRUNCATE == 20);

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install every seam owned by `backend-access-transam-multixact-seams`.
pub fn init_seams() {
    mx_seams::multi_xact_id_is_running::set(MultiXactIdIsRunning);
    mx_seams::get_multi_xact_id_members::set(get_multi_xact_id_members_seam);
    mx_seams::multi_xact_id_create_from_members::set(multi_xact_id_create_from_members_seam);
    mx_seams::multi_xact_id_create::set(MultiXactIdCreate);
    mx_seams::multi_xact_id_expand::set(MultiXactIdExpand);
    mx_seams::multi_xact_id_get_update_xid::set(MultiXactIdGetUpdateXid);
    mx_seams::multi_xact_id_set_oldest_member::set(MultiXactIdSetOldestMember);
    mx_seams::multixact_twophase_recover::set(multixact_twophase_recover);
    mx_seams::multixact_twophase_postcommit::set(multixact_twophase_postcommit);
    mx_seams::multixact_twophase_postabort::set(multixact_twophase_postabort);
    mx_seams::multixact_redo::set(multixact_redo);
    mx_seams::at_eoxact_multixact::set(AtEOXact_MultiXact);
    mx_seams::at_prepare_multixact::set(AtPrepare_MultiXact);
    mx_seams::post_prepare_multixact::set(post_prepare_multixact_seam);
    mx_seams::multixactoffsetssyncfiletag::set(multixactoffsetssyncfiletag);
    mx_seams::multixactmemberssyncfiletag::set(multixactmemberssyncfiletag);
    mx_seams::multi_xact_shmem_size::set(multi_xact_shmem_size_seam);
    mx_seams::multi_xact_shmem_init::set(MultiXactShmemInit);
    mx_seams::get_oldest_multi_xact_id::set(GetOldestMultiXactId);
}

/// `get_multi_xact_id_members` seam: the inward contract returns a `PgVec` in
/// the caller's `mcx`, and the empty/obsolete `-1` case as an empty vector.
fn get_multi_xact_id_members_seam<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
    allow_old: bool,
    only_lockers: bool,
) -> PgResult<PgVec<'mcx, MultiXactMember>> {
    let members = GetMultiXactIdMembers(multi, allow_old, only_lockers)?;
    let members = members.unwrap_or_default();
    let mut out = mcx::vec_with_capacity_in(mcx, members.len())?;
    for m in members {
        out.push(m);
    }
    Ok(out)
}

/// `multi_xact_id_create_from_members` seam: a `&[MultiXactMember]` input
/// (sorted into a local mutable copy, as the in-crate fn sorts in place).
fn multi_xact_id_create_from_members_seam(
    members: &[MultiXactMember],
) -> PgResult<MultiXactId> {
    let mut owned: Vec<MultiXactMember> = members.to_vec();
    MultiXactIdCreateFromMembers(&mut owned)
}

/// `post_prepare_multixact` seam: void return; the PgResult is an
/// impossible-error path (lock release / dummy-proc lookup).
fn post_prepare_multixact_seam(xid: TransactionId) {
    PostPrepare_MultiXact(xid).expect("PostPrepare_MultiXact failed");
}

/// `multi_xact_shmem_size` seam wrapper (always Ok: the sizing is infallible).
fn multi_xact_shmem_size_seam() -> PgResult<Size> {
    Ok(MultiXactShmemSize())
}

// ===========================================================================
// MultiXactIdGetUpdateXid (multixact.c lines 1006-1086; called from heapam)
// ===========================================================================

/// `MultiXactIdGetUpdateXid` — the update XID carried by a multixact xmax (the
/// single member with an update status), or `InvalidTransactionId` if there is
/// none. `xmax` is the raw multixact id; `t_infomask` distinguishes lock-only.
pub fn MultiXactIdGetUpdateXid(xmax: TransactionId, t_infomask: u16) -> PgResult<TransactionId> {
    // HEAP_XMAX_LOCK_ONLY: if the tuple is locked-only, there is no update xid.
    const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080;
    if t_infomask & HEAP_XMAX_LOCK_ONLY != 0 {
        return Ok(0);
    }

    let members = GetMultiXactIdMembers(xmax, false, false)?;
    let Some(members) = members else {
        return Ok(0);
    };

    let mut update_xact: TransactionId = 0;
    for m in &members {
        // Ignore lockers; only updates/deletes carry the update xid.
        let Some(status) = m.status else { continue };
        if !ISUPDATE_from_mxstatus(status) {
            continue;
        }

        // ISUPDATE implies one of NoKeyUpdate / Update; there can be at most
        // one such member.
        debug_assert_eq!(update_xact, 0);
        debug_assert!(
            status == MultiXactStatus::NoKeyUpdate || status == MultiXactStatus::Update
        );
        update_xact = m.xid;
        #[cfg(not(debug_assertions))]
        break;
    }

    Ok(update_xact)
}

#[cfg(test)]
mod tests;
