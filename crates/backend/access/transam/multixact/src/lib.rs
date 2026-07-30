#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::OnceLock;

use elog::{elog, ereport};
use init_small::globals;
use lwlock::{LWLockAcquire, LWLockRelease, LWLock, LW_EXCLUSIVE, LW_SHARED};
use mcx::{MemoryContext, PgVec};
use pmsignal::{PMSignalReason, SendPostmasterSignal};
use slru::{
    check_slru_buffers, LwGuard, SimpleLruDoesPhysicalPageExist, SimpleLruGetBankLock,
    SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly, SimpleLruShmemSize,
    SimpleLruTruncate, SimpleLruWriteAll, SimpleLruWritePage, SimpleLruZeroPage, SlruCtlData,
    SlruDeleteSegment, SlruPagePrecedesUnitTests, SlruPath, SlruSyncFileTag,
    SLRU_PAGES_PER_SEGMENT,
};
use types_core::xact::{MultiXactIdPrecedes, MultiXactIdPrecedesOrEquals};
use types_core::{
    MultiXactId, MultiXactOffset, Oid, Size, TransactionId, TransactionIdIsValid,
    TransactionIdPrecedes, BLCKSZ,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_DATA_CORRUPTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, LOG, WARNING,
};
use types_guc::GucSource;
use types_storage::multixact::{ISUPDATE_from_mxstatus, MultiXactMember, MultiXactStatus};
use types_storage::storage::{
    LWTRANCHE_MULTIXACTMEMBER_BUFFER, LWTRANCHE_MULTIXACTMEMBER_SLRU,
    LWTRANCHE_MULTIXACTOFFSET_BUFFER, LWTRANCHE_MULTIXACTOFFSET_SLRU, NUM_AUXILIARY_PROCS,
};
use types_storage::storage::{MULTI_XACT_GEN_LOCK, MULTI_XACT_TRUNCATION_LOCK};
use types_storage::sync::{FileTag, SyncRequestHandler};
use xlogreader_seams::XLogReaderState;

pub const InvalidMultiXactId: MultiXactId = 0;
pub const FirstMultiXactId: MultiXactId = 1;
pub const MaxMultiXactId: MultiXactId = 0xFFFF_FFFF;
pub const MaxMultiXactOffset: MultiXactOffset = 0xFFFF_FFFF;

pub const RM_MULTIXACT_ID: u8 = 6;
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;
const XLR_INFO_MASK: u8 = 0x0F;

const SIZE_OF_MULTIXACT_OFFSET: usize = 4;
const SIZE_OF_TRANSACTION_ID: usize = 4;
const SIZE_OF_MULTIXACT_MEMBER: usize = 8;
const SIZE_OF_MULTIXACT_CREATE: usize = 12;
const SIZE_OF_MULTIXACT_TRUNCATE: usize = 20;

pub const MULTIXACT_OFFSETS_PER_PAGE: u32 = (BLCKSZ / SIZE_OF_MULTIXACT_OFFSET) as u32;

const MXACT_MEMBER_BITS_PER_XACT: u32 = 8;
const MXACT_MEMBER_XACT_BITMASK: u32 = (1 << MXACT_MEMBER_BITS_PER_XACT) - 1;
const MULTIXACT_FLAGBYTES_PER_GROUP: usize = 4;
const MULTIXACT_MEMBERS_PER_MEMBERGROUP: u32 = MULTIXACT_FLAGBYTES_PER_GROUP as u32;
const MULTIXACT_MEMBERGROUP_SIZE: usize =
    SIZE_OF_TRANSACTION_ID * MULTIXACT_MEMBERS_PER_MEMBERGROUP as usize
        + MULTIXACT_FLAGBYTES_PER_GROUP;
const MULTIXACT_MEMBERGROUPS_PER_PAGE: u32 = (BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE) as u32;
pub const MULTIXACT_MEMBERS_PER_PAGE: u32 =
    MULTIXACT_MEMBERGROUPS_PER_PAGE * MULTIXACT_MEMBERS_PER_MEMBERGROUP;
const MAX_MEMBERS_IN_LAST_MEMBERS_PAGE: u32 = (0xFFFF_FFFFu32 % MULTIXACT_MEMBERS_PER_PAGE) + 1;

const MULTIXACT_MEMBER_SAFE_THRESHOLD: MultiXactOffset = MaxMultiXactOffset / 2;
const MULTIXACT_MEMBER_DANGER_THRESHOLD: MultiXactOffset =
    MaxMultiXactOffset - MaxMultiXactOffset / 4;
const OFFSET_WARN_SEGMENTS: u32 = 20;

const MAX_CACHE_ENTRIES: usize = 256;

#[inline]
fn MultiXactIdToOffsetPage(multi: MultiXactId) -> i64 {
    (multi / MULTIXACT_OFFSETS_PER_PAGE) as i64
}

#[inline]
fn MultiXactIdToOffsetEntry(multi: MultiXactId) -> usize {
    (multi % MULTIXACT_OFFSETS_PER_PAGE) as usize
}

#[inline]
fn MultiXactIdToOffsetSegment(multi: MultiXactId) -> i64 {
    MultiXactIdToOffsetPage(multi) / SLRU_PAGES_PER_SEGMENT
}

#[inline]
fn MXOffsetToMemberPage(offset: MultiXactOffset) -> i64 {
    (offset / MULTIXACT_MEMBERS_PER_PAGE) as i64
}

#[inline]
fn MXOffsetToMemberSegment(offset: MultiXactOffset) -> i64 {
    MXOffsetToMemberPage(offset) / SLRU_PAGES_PER_SEGMENT
}

#[inline]
fn MXOffsetToFlagsOffset(offset: MultiXactOffset) -> usize {
    let group = offset / MULTIXACT_MEMBERS_PER_MEMBERGROUP;
    (group % MULTIXACT_MEMBERGROUPS_PER_PAGE) as usize * MULTIXACT_MEMBERGROUP_SIZE
}

#[inline]
fn MXOffsetToFlagsBitShift(offset: MultiXactOffset) -> u32 {
    (offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP) * MXACT_MEMBER_BITS_PER_XACT
}

#[inline]
fn MXOffsetToMemberOffset(offset: MultiXactOffset) -> usize {
    MXOffsetToFlagsOffset(offset)
        + MULTIXACT_FLAGBYTES_PER_GROUP
        + (offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP) as usize * SIZE_OF_TRANSACTION_ID
}

#[inline]
fn PreviousMultiXactId(multi: MultiXactId) -> MultiXactId {
    if multi == FirstMultiXactId {
        MaxMultiXactId
    } else {
        multi - 1
    }
}

#[inline]
pub fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

#[inline]
fn MultiXactOffsetPrecedes(offset1: MultiXactOffset, offset2: MultiXactOffset) -> bool {
    (offset1.wrapping_sub(offset2) as i32) < 0
}

fn mxstatus_from_word(word: u32) -> MultiXactStatus {
    match word {
        0 => MultiXactStatus::MultiXactStatusForKeyShare,
        1 => MultiXactStatus::MultiXactStatusForShare,
        2 => MultiXactStatus::MultiXactStatusForNoKeyUpdate,
        3 => MultiXactStatus::MultiXactStatusForUpdate,
        4 => MultiXactStatus::MultiXactStatusNoKeyUpdate,
        5 => MultiXactStatus::MultiXactStatusUpdate,
        w => panic!("invalid multixact member status {w} read from pg_multixact/members"),
    }
}

pub fn mxstatus_to_string(status: MultiXactStatus) -> &'static str {
    match status {
        MultiXactStatus::MultiXactStatusForKeyShare => "keysh",
        MultiXactStatus::MultiXactStatusForShare => "sh",
        MultiXactStatus::MultiXactStatusForNoKeyUpdate => "fornokeyupd",
        MultiXactStatus::MultiXactStatusForUpdate => "forupd",
        MultiXactStatus::MultiXactStatusNoKeyUpdate => "nokeyupd",
        MultiXactStatus::MultiXactStatusUpdate => "upd",
    }
}

pub fn mxid_to_string(multi: MultiXactId, members: &[MultiXactMember]) -> String {
    let mut buf = format!("{multi} {}[", members.len());
    for (i, m) in members.iter().enumerate() {
        if i > 0 {
            buf.push_str(", ");
        }
        buf.push_str(&format!("{} ({})", m.xid, mxstatus_to_string(m.status)));
    }
    buf.push(']');
    buf
}

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn dlog(level: types_error::ErrorLevel, message: String) {
    let _ = elog(level, message);
}

static MXOFFSET_CTL: OnceLock<SlruCtlData> = OnceLock::new();
static MXMEMBER_CTL: OnceLock<SlruCtlData> = OnceLock::new();

fn OffsetCtl() -> &'static SlruCtlData {
    MXOFFSET_CTL
        .get()
        .unwrap_or_else(|| panic!("MultiXact offsets SLRU accessed before MultiXactShmemInit"))
}

fn MemberCtl() -> &'static SlruCtlData {
    MXMEMBER_CTL
        .get()
        .unwrap_or_else(|| panic!("MultiXact members SLRU accessed before MultiXactShmemInit"))
}

// MultiXactStateData (multixact.c); every field is serialized by
// MultiXactGenLock except where C documents atomic single-word access, so
// all loads/stores are Relaxed (varsup precedent).
struct MultiXactStateShared {
    nextMXact: AtomicU32,
    nextOffset: AtomicU32,
    finishedStartup: AtomicBool,
    oldestMultiXactId: AtomicU32,
    oldestMultiXactDB: AtomicU32,
    oldestOffset: AtomicU32,
    oldestOffsetKnown: AtomicBool,
    multiVacLimit: AtomicU32,
    multiWarnLimit: AtomicU32,
    multiStopLimit: AtomicU32,
    multiWrapLimit: AtomicU32,
    offsetStopLimit: AtomicU32,
    // perBackendXactIds: OldestMemberMXactId[NumMemberSlots] (MaxBackends
    // backend slots then max_prepared_xacts prepared-xact slots) followed by
    // OldestVisibleMXactId[NumVisibleSlots] (MaxBackends slots only; prepared
    // xacts have no visible slot). Upstream 0a50ef09.
    perBackendXactIds: Box<[AtomicU32]>,
    num_member_slots: usize,
}

static MULTIXACT_STATE: OnceLock<&'static MultiXactStateShared> = OnceLock::new();

fn MultiXactState() -> &'static MultiXactStateShared {
    MULTIXACT_STATE
        .get()
        .unwrap_or_else(|| panic!("MultiXactState accessed before MultiXactShmemInit"))
}

// MaxBackends as captured at MultiXactShmemInit (globals are thread-local;
// the shared arrays are the process-wide truth).
fn state_max_backends(st: &MultiXactStateShared) -> usize {
    st.perBackendXactIds.len() - st.num_member_slots
}

fn my_slot() -> usize {
    let procno = globals::MyProcNumber();
    debug_assert!(
        procno >= 0 && (procno as usize) < state_max_backends(MultiXactState()),
        "multixact per-backend slot requires a regular backend MyProcNumber"
    );
    procno as usize
}

// PreparedXactOldestMemberMXactIdSlot: dummy proc numbers start at
// MaxBackends + NUM_AUXILIARY_PROCS (FIRST_PREPARED_XACT_PROC_NUMBER), but
// their member slots come directly after the MaxBackends backend slots.
fn prepared_xact_member_slot(procno: i32) -> usize {
    let st = MultiXactState();
    let first_prepared = state_max_backends(st) + NUM_AUXILIARY_PROCS as usize;
    debug_assert!(procno >= 0 && procno as usize >= first_prepared);
    let idx = state_max_backends(st) + (procno as usize - first_prepared);
    debug_assert!(idx < st.num_member_slots);
    idx
}

#[inline]
fn oldest_member(i: usize) -> MultiXactId {
    MultiXactState().perBackendXactIds[i].load(Relaxed)
}

#[inline]
fn set_oldest_member(i: usize, v: MultiXactId) {
    MultiXactState().perBackendXactIds[i].store(v, Relaxed);
}

#[inline]
fn oldest_visible(i: usize) -> MultiXactId {
    let st = MultiXactState();
    st.perBackendXactIds[st.num_member_slots + i].load(Relaxed)
}

#[inline]
fn set_oldest_visible(i: usize, v: MultiXactId) {
    let st = MultiXactState();
    st.perBackendXactIds[st.num_member_slots + i].store(v, Relaxed);
}

fn MultiXactGenLock() -> &'static LWLock {
    lwlock::main_lock(MULTI_XACT_GEN_LOCK)
}

fn MultiXactTruncationLock() -> &'static LWLock {
    lwlock::main_lock(MULTI_XACT_TRUNCATION_LOCK)
}

fn read_offset_entry(
    ctl: &SlruCtlData,
    slotno: usize,
    entryno: usize,
    bank: &LwGuard,
) -> MultiXactOffset {
    let start = entryno * SIZE_OF_MULTIXACT_OFFSET;
    let buf = ctl.page_buffer(slotno, bank);
    MultiXactOffset::from_ne_bytes(buf[start..start + 4].try_into().expect("4-byte offset"))
}

fn write_offset_entry(
    ctl: &SlruCtlData,
    slotno: usize,
    entryno: usize,
    value: MultiXactOffset,
    bank: &mut LwGuard,
) {
    let start = entryno * SIZE_OF_MULTIXACT_OFFSET;
    let buf = ctl.page_buffer_mut(slotno, bank);
    buf[start..start + 4].copy_from_slice(&value.to_ne_bytes());
}

struct MXactCacheEnt {
    multi: MultiXactId,
    members: PgVec<'static, MultiXactMember>,
}

// entries[..live] is the cache in recency order (head = most recent);
// entries[live..] are spare slots whose member buffers retain capacity across
// AtEOXact resets (C instead frees MXactContext per transaction).
struct MXactCache {
    cx: &'static MemoryContext,
    entries: PgVec<'static, MXactCacheEnt>,
    live: usize,
}

// Older-minor WAL compat (upstream 0852643e): LAST_INITIALIZED_OFFSETS_PAGE
// is the page of the last replayed XLOG_MULTIXACT_ZERO_OFF_PAGE record (-1 if
// none yet); PRE_INITIALIZED_OFFSETS_PAGE is the last page implicitly
// initialized by a CREATE_ID record before its ZERO_OFF_PAGE was seen.
thread_local! {
    static MXACT_CACHE: RefCell<Option<MXactCache>> = const { RefCell::new(None) };
    static PRE_INITIALIZED_OFFSETS_PAGE: std::cell::Cell<i64> = const { std::cell::Cell::new(-1) };
    static LAST_INITIALIZED_OFFSETS_PAGE: std::cell::Cell<i64> = const { std::cell::Cell::new(-1) };
}

#[cfg(test)]
thread_local! {
    pub(crate) static CACHE_SET_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static CACHE_ID_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn with_cache<R>(f: impl FnOnce(&mut MXactCache) -> R) -> R {
    MXACT_CACHE.with(|c| {
        let mut slot = c.borrow_mut();
        let cache = slot.get_or_insert_with(|| {
            let cx: &'static MemoryContext =
                ::mcx::session_root("MultiXact cache context");
            // LIFO: empty the droppy TLS cache before its context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                MXACT_CACHE.with(|c| drop(c.borrow_mut().take()));
            }));
            MXactCache {
                cx,
                entries: PgVec::new_in(cx.mcx()),
                live: 0,
            }
        });
        f(cache)
    })
}

fn mxact_member_cmp(a: &MultiXactMember, b: &MultiXactMember) -> core::cmp::Ordering {
    a.xid.cmp(&b.xid).then((a.status as i32).cmp(&(b.status as i32)))
}

fn members_eq(a: &[MultiXactMember], b: &[MultiXactMember]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.xid == y.xid && x.status == y.status)
}

// Sorts `members` in place, like C's qsort in mXactCacheGetBySet.
fn mXactCacheGetBySet(members: &mut [MultiXactMember]) -> MultiXactId {
    members.sort_unstable_by(mxact_member_cmp);

    with_cache(|c| {
        for i in 0..c.live {
            if members_eq(&c.entries[i].members, members) {
                let multi = c.entries[i].multi;
                c.entries[..=i].rotate_right(1);
                #[cfg(test)]
                CACHE_SET_HITS.with(|h| h.set(h.get() + 1));
                return multi;
            }
        }
        InvalidMultiXactId
    })
}

fn mXactCacheGetById(multi: MultiXactId, out: &mut PgVec<'static, MultiXactMember>) -> Option<i32> {
    with_cache(|c| {
        for i in 0..c.live {
            if c.entries[i].multi == multi {
                out.clear();
                out.extend_from_slice(&c.entries[i].members);
                let n = c.entries[i].members.len() as i32;
                c.entries[..=i].rotate_right(1);
                #[cfg(test)]
                CACHE_ID_HITS.with(|h| h.set(h.get() + 1));
                return Some(n);
            }
        }
        None
    })
}

fn mXactCachePut(multi: MultiXactId, members: &[MultiXactMember]) {
    with_cache(|c| {
        let slot = if c.live == MAX_CACHE_ENTRIES {
            c.live - 1
        } else if c.entries.len() > c.live {
            c.live += 1;
            c.live - 1
        } else {
            let ent = MXactCacheEnt {
                multi: InvalidMultiXactId,
                members: PgVec::new_in(c.cx.mcx()),
            };
            c.entries.push(ent);
            c.live += 1;
            c.live - 1
        };

        let ent = &mut c.entries[slot];
        ent.multi = multi;
        ent.members.clear();
        ent.members.extend_from_slice(members);
        ent.members.sort_unstable_by(mxact_member_cmp);

        c.entries[..=slot].rotate_right(1);
    })
}

fn cache_clear() {
    MXACT_CACHE.with(|c| {
        if let Some(cache) = c.borrow_mut().as_mut() {
            cache.live = 0;
        }
    });
}

struct MemberScratch {
    _cx: &'static MemoryContext,
    buf: PgVec<'static, MultiXactMember>,
}

thread_local! {
    static MEMBER_SCRATCH: RefCell<Option<MemberScratch>> = const { RefCell::new(None) };
    static MEMBER_SCRATCH_INIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn take_member_scratch() -> MemberScratch {
    if !MEMBER_SCRATCH_INIT.get() {
        MEMBER_SCRATCH_INIT.set(true);
        let cx: &'static MemoryContext =
            ::mcx::session_root("MultiXact member scratch");
        // LIFO: empty the droppy TLS slot before its context is freed.
        ::mcx::register_session_cleanup(Box::new(|| {
            MEMBER_SCRATCH.with(|s| drop(s.borrow_mut().take()));
        }));
        return MemberScratch {
            buf: PgVec::new_in(cx.mcx()),
            _cx: cx,
        };
    }
    MEMBER_SCRATCH.with(|s| {
        s.borrow_mut()
            .take()
            .unwrap_or_else(|| panic!("GetMultiXactIdMembers re-entered from its consumer"))
    })
}

fn put_member_scratch(scratch: MemberScratch) {
    MEMBER_SCRATCH.with(|s| *s.borrow_mut() = Some(scratch));
}

pub fn MultiXactIdCreate(
    xid1: TransactionId,
    status1: MultiXactStatus,
    xid2: TransactionId,
    status2: MultiXactStatus,
) -> PgResult<MultiXactId> {
    debug_assert!(TransactionIdIsValid(xid1));
    debug_assert!(TransactionIdIsValid(xid2));
    debug_assert!(xid1 != xid2 || status1 != status2);

    // No need to check that both XIDs are still running (multixact.c: xid2
    // is normally our own XID and the caller just checked xid1).
    let mut members = [
        MultiXactMember { xid: xid1, status: status1 },
        MultiXactMember { xid: xid2, status: status2 },
    ];
    MultiXactIdCreateFromMembers(&mut members)
}

pub fn MultiXactIdExpand(
    multi: MultiXactId,
    xid: TransactionId,
    status: MultiXactStatus,
) -> PgResult<MultiXactId> {
    debug_assert!(MultiXactIdIsValid(multi));
    debug_assert!(TransactionIdIsValid(xid));
    debug_assert!(MultiXactIdIsValid(oldest_member(my_slot())));

    // Cold path (row-lock expansion); the per-call context mirrors C's
    // palloc into CurrentMemoryContext.
    let cx = MemoryContext::new("MultiXactIdExpand");
    let mut members: PgVec<'_, MultiXactMember> = PgVec::new_in(cx.mcx());
    let nmembers = GetMultiXactIdMembers(multi, false, false, &mut |ms| {
        members.extend_from_slice(ms);
    })?;

    if nmembers < 0 {
        // All original members stopped running between the caller's check
        // and now; create a singleton.
        let mut member = [MultiXactMember { xid, status }];
        return MultiXactIdCreateFromMembers(&mut member);
    }

    for m in members.iter() {
        if m.xid == xid && m.status == status {
            return Ok(multi);
        }
    }

    let mut new_members: PgVec<'_, MultiXactMember> = PgVec::new_in(cx.mcx());
    for m in members.iter() {
        let keep = procarray_seams::transaction_id_is_in_progress::call(m.xid)?
            || (ISUPDATE_from_mxstatus(m.status)
                && transam_seams::transaction_id_did_commit::call(m.xid)?);
        if keep {
            new_members.push(*m);
        }
    }
    new_members.push(MultiXactMember { xid, status });

    MultiXactIdCreateFromMembers(&mut new_members)
}

pub fn MultiXactIdIsRunning(multi: MultiXactId, is_lock_only: bool) -> PgResult<bool> {
    let mut result: PgResult<bool> = Ok(false);
    let nmembers = GetMultiXactIdMembers(multi, false, is_lock_only, &mut |members| {
        // Checking for myself first is a cheap fast path, not needed for
        // correctness (multixact.c).
        for m in members {
            if xact_seams::transaction_id_is_current_transaction_id::call(m.xid) {
                result = Ok(true);
                return;
            }
        }
        for m in members {
            match procarray_seams::transaction_id_is_in_progress::call(m.xid) {
                Ok(true) => {
                    result = Ok(true);
                    return;
                }
                Ok(false) => {}
                Err(e) => {
                    result = Err(e);
                    return;
                }
            }
        }
    })?;

    if nmembers <= 0 {
        return Ok(false);
    }
    result
}

pub fn MultiXactIdSetOldestMember() -> PgResult<()> {
    let me = my_slot();
    if !MultiXactIdIsValid(oldest_member(me)) {
        // A shared lock suffices: it stops nextMXact advancing, and only
        // this backend writes its own entry (multixact.c).
        LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;

        let mut next_mxact = MultiXactState().nextMXact.load(Relaxed);
        if next_mxact < FirstMultiXactId {
            next_mxact = FirstMultiXactId;
        }
        set_oldest_member(me, next_mxact);

        LWLockRelease(MultiXactGenLock())?;
    }
    Ok(())
}

fn MultiXactIdSetOldestVisible() -> PgResult<()> {
    let me = my_slot();
    if !MultiXactIdIsValid(oldest_visible(me)) {
        let st = MultiXactState();
        LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;

        let mut oldest_mxact = st.nextMXact.load(Relaxed);
        if oldest_mxact < FirstMultiXactId {
            oldest_mxact = FirstMultiXactId;
        }
        for i in 0..st.num_member_slots {
            let thisoldest = oldest_member(i);
            if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldest_mxact) {
                oldest_mxact = thisoldest;
            }
        }
        set_oldest_visible(me, oldest_mxact);

        LWLockRelease(MultiXactGenLock())?;
    }
    Ok(())
}

pub fn ReadNextMultiXactId() -> PgResult<MultiXactId> {
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let mut mxid = MultiXactState().nextMXact.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    if mxid < FirstMultiXactId {
        mxid = FirstMultiXactId;
    }
    Ok(mxid)
}

pub fn ReadMultiXactIdRange() -> PgResult<(MultiXactId, MultiXactId)> {
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let st = MultiXactState();
    let mut oldest = st.oldestMultiXactId.load(Relaxed);
    let mut next = st.nextMXact.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    if oldest < FirstMultiXactId {
        oldest = FirstMultiXactId;
    }
    if next < FirstMultiXactId {
        next = FirstMultiXactId;
    }
    Ok((oldest, next))
}

pub fn MultiXactIdCreateFromMembers(members: &mut [MultiXactMember]) -> PgResult<MultiXactId> {
    // A member set can recur across different multixacts, so only the local
    // cache (never disk) is a safe dedup source (multixact.c).
    let multi = mXactCacheGetBySet(members);
    if MultiXactIdIsValid(multi) {
        return Ok(multi);
    }

    let mut has_update = false;
    for m in members.iter() {
        if ISUPDATE_from_mxstatus(m.status) {
            if has_update {
                ereport(ERROR)
                    .errmsg(format!(
                        "new multixact has more than one updating member: {}",
                        mxid_to_string(InvalidMultiXactId, members)
                    ))
                    .finish(loc("MultiXactIdCreateFromMembers"))?;
                unreachable!("ERROR finish returned");
            }
            has_update = true;
        }
    }

    // GetNewMultiXactId starts the critical section that ends below.
    let (multi, offset) = GetNewMultiXactId(members.len() as i32)?;

    let mut header = [0u8; SIZE_OF_MULTIXACT_CREATE];
    header[0..4].copy_from_slice(&multi.to_ne_bytes());
    header[4..8].copy_from_slice(&offset.to_ne_bytes());
    header[8..12].copy_from_slice(&(members.len() as i32).to_ne_bytes());
    let res =
        write_create_wal(&header, members).and_then(|_| RecordNewMultiXact(multi, offset, members));

    globals::EndCriticalSection();
    res?;

    mXactCachePut(multi, members);
    Ok(multi)
}

fn write_create_wal(header: &[u8], members: &[MultiXactMember]) -> PgResult<()> {
    thread_local! {
        static WAL_SCRATCH: RefCell<Option<(&'static MemoryContext, PgVec<'static, u8>)>> =
            const { RefCell::new(None) };
    }
    WAL_SCRATCH.with(|s| {
        let mut slot = s.borrow_mut();
        let (_, buf) = slot.get_or_insert_with(|| {
            let cx: &'static MemoryContext =
                ::mcx::session_root("MultiXact WAL scratch");
            // LIFO: empty the droppy TLS slot before its context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                WAL_SCRATCH.with(|s| drop(s.borrow_mut().take()));
            }));
            (cx, PgVec::new_in(cx.mcx()))
        });
        buf.clear();
        buf.reserve(members.len() * SIZE_OF_MULTIXACT_MEMBER);
        for m in members {
            buf.extend_from_slice(&m.xid.to_ne_bytes());
            buf.extend_from_slice(&(m.status as i32).to_ne_bytes());
        }
        xloginsert_seams::xlog_insert::call(
            RM_MULTIXACT_ID,
            XLOG_MULTIXACT_CREATE_ID,
            &[header, buf],
        )?;
        Ok(())
    })
}

fn RecordNewMultiXact(
    multi: MultiXactId,
    offset: MultiXactOffset,
    members: &[MultiXactMember],
) -> PgResult<()> {
    let octl = OffsetCtl();
    let pageno = MultiXactIdToOffsetPage(multi);
    let entryno = MultiXactIdToOffsetEntry(multi);

    let mut next = multi.wrapping_add(1);
    if next < FirstMultiXactId {
        next = FirstMultiXactId;
    }
    let next_pageno = MultiXactIdToOffsetPage(next);
    let next_entryno = MultiXactIdToOffsetEntry(next);

    // Pre-18 minors did not set the next multixid's offset here; when
    // replaying their WAL the next page may not exist yet. A CHECKPOINT
    // record can seed latest_page_number before the CREATE_ID for its
    // nextMulti is replayed, so latest_page_number cannot tell whether the
    // page is initialized; track the last ZERO_OFF_PAGE we replayed instead,
    // falling back to a physical-existence probe (after flushing the SLRU
    // buffers so it is accurate) until we have seen one (upstream 0852643e).
    if xlogutils_seams::in_recovery::call() && next_pageno != pageno {
        let init_needed = if LAST_INITIALIZED_OFFSETS_PAGE.get() == -1 {
            SimpleLruWriteAll(octl, false)?;
            !SimpleLruDoesPhysicalPageExist(octl, next_pageno)?
        } else {
            LAST_INITIALIZED_OFFSETS_PAGE.get() == pageno
        };

        if init_needed {
            dlog(DEBUG1, "next offsets page is not initialized, initializing it now".to_string());

            let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, next_pageno), LW_EXCLUSIVE)?;
            let slotno = SimpleLruZeroPage(octl, next_pageno, &mut bank)?;
            SimpleLruWritePage(octl, slotno, &mut bank)?;
            debug_assert!(!octl.page_dirty(slotno, &bank));
            bank.release()?;

            PRE_INITIALIZED_OFFSETS_PAGE.set(next_pageno);
            LAST_INITIALIZED_OFFSETS_PAGE.set(next_pageno);
        }
    }

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
    let mut slotno = SimpleLruReadPage(octl, pageno, true, multi, &mut bank)?;

    if read_offset_entry(octl, slotno, entryno, &bank) != offset {
        debug_assert_eq!(read_offset_entry(octl, slotno, entryno, &bank), 0);
        write_offset_entry(octl, slotno, entryno, offset, &mut bank);
        octl.mark_page_dirty(slotno, &bank);
    }

    let mut next_offset = offset.wrapping_add(members.len() as u32);
    if next_offset == 0 {
        next_offset = 1;
    }

    if next_pageno != pageno {
        debug_assert!(next_entryno == 0 || next == FirstMultiXactId);
        bank.release()?;
        bank = LwGuard::acquire(SimpleLruGetBankLock(octl, next_pageno), LW_EXCLUSIVE)?;
        slotno = SimpleLruReadPage(octl, next_pageno, true, next, &mut bank)?;
        if read_offset_entry(octl, slotno, next_entryno, &bank) != next_offset {
            debug_assert_eq!(read_offset_entry(octl, slotno, next_entryno, &bank), 0);
            write_offset_entry(octl, slotno, next_entryno, next_offset, &mut bank);
            octl.mark_page_dirty(slotno, &bank);
        }
    } else if read_offset_entry(octl, slotno, entryno + 1, &bank) != next_offset {
        debug_assert_eq!(read_offset_entry(octl, slotno, entryno + 1, &bank), 0);
        write_offset_entry(octl, slotno, entryno + 1, next_offset, &mut bank);
        octl.mark_page_dirty(slotno, &bank);
    }

    bank.release()?;

    let mctl = MemberCtl();
    let mut mguard: Option<LwGuard> = None;
    let mut prev_pageno: i64 = -1;
    let mut mslotno: usize = 0;
    let mut off = offset;

    for m in members {
        let mpageno = MXOffsetToMemberPage(off);
        let memberoff = MXOffsetToMemberOffset(off);
        let flagsoff = MXOffsetToFlagsOffset(off);
        let bshift = MXOffsetToFlagsBitShift(off);

        if mpageno != prev_pageno {
            let lock = SimpleLruGetBankLock(mctl, mpageno);
            let same_bank = mguard.as_ref().is_some_and(|g| g.covers(lock));
            if !same_bank {
                if let Some(g) = mguard.take() {
                    g.release()?;
                }
                mguard = Some(LwGuard::acquire(lock, LW_EXCLUSIVE)?);
            }
            let g = mguard.as_mut().expect("member bank lock held");
            mslotno = SimpleLruReadPage(mctl, mpageno, true, multi, g)?;
            prev_pageno = mpageno;
        }

        let g = mguard.as_mut().expect("member bank lock held");
        let buf = mctl.page_buffer_mut(mslotno, g);
        buf[memberoff..memberoff + 4].copy_from_slice(&m.xid.to_ne_bytes());
        let mut flagsval =
            u32::from_ne_bytes(buf[flagsoff..flagsoff + 4].try_into().expect("4-byte flags"));
        flagsval &= !(MXACT_MEMBER_XACT_BITMASK << bshift);
        flagsval |= (m.status as u32) << bshift;
        buf[flagsoff..flagsoff + 4].copy_from_slice(&flagsval.to_ne_bytes());
        mctl.mark_page_dirty(mslotno, g);

        off = off.wrapping_add(1);
    }

    if let Some(g) = mguard.take() {
        g.release()?;
    }
    Ok(())
}

fn GetNewMultiXactId(nmembers_in: i32) -> PgResult<(MultiXactId, MultiXactOffset)> {
    debug_assert!(nmembers_in > 0);
    let mut nmembers = nmembers_in;

    if transam_xlog_seams::recovery_in_progress::call() {
        elog(ERROR, "cannot assign MultiXactIds during recovery")?;
    }

    let st = MultiXactState();
    let genlock = MultiXactGenLock();
    LWLockAcquire(genlock, LW_EXCLUSIVE, globals::MyProcNumber())?;

    // C's ereport longjmp releases LWLocks via AbortTransaction; here every
    // Err exit must release explicitly.
    macro_rules! unlock_on_err {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => {
                    let _ = LWLockRelease(genlock);
                    return Err(e);
                }
            }
        };
    }

    if st.nextMXact.load(Relaxed) < FirstMultiXactId {
        st.nextMXact.store(FirstMultiXactId, Relaxed);
    }
    let mut result = st.nextMXact.load(Relaxed);

    if !MultiXactIdPrecedes(result, st.multiVacLimit.load(Relaxed)) {
        // Warnings/signals run unlocked: get_database_name under
        // MultiXactGenLock risks deadlock (multixact.c).
        let multi_warn_limit = st.multiWarnLimit.load(Relaxed);
        let multi_stop_limit = st.multiStopLimit.load(Relaxed);
        let multi_wrap_limit = st.multiWrapLimit.load(Relaxed);
        let oldest_datoid = st.oldestMultiXactDB.load(Relaxed);

        LWLockRelease(genlock)?;

        if globals::IsUnderPostmaster() && !MultiXactIdPrecedes(result, multi_stop_limit) {
            let oldest_datname = dbcommands_seams::get_database_name::call(oldest_datoid)?;
            SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);

            let msg = match oldest_datname {
                Some(name) => format!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database \"{name}\""
                ),
                None => format!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database with OID {oldest_datoid}"
                ),
            };
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(msg)
                .errhint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(loc("GetNewMultiXactId"))?;
        }

        // Issue the autovac request once per 64K multis to avoid swamping
        // the postmaster with signals.
        if globals::IsUnderPostmaster() && result % 65536 == 0 {
            SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }

        if !MultiXactIdPrecedes(result, multi_warn_limit) {
            let oldest_datname = dbcommands_seams::get_database_name::call(oldest_datoid)?;
            let remaining = multi_wrap_limit.wrapping_sub(result);
            let msg = match oldest_datname {
                Some(name) => multixactid_warning_msg_named(&name, remaining),
                None => multixactid_warning_msg_oid(oldest_datoid, remaining),
            };
            ereport(WARNING)
                .errmsg(msg)
                .errhint(
                    "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
                )
                .finish(loc("GetNewMultiXactId"))?;
        }

        LWLockAcquire(genlock, LW_EXCLUSIVE, globals::MyProcNumber())?;
        result = st.nextMXact.load(Relaxed);
        if result < FirstMultiXactId {
            result = FirstMultiXactId;
        }
    }

    // The zero-page WAL record must precede any use of the page, hence
    // extension happens under MultiXactGenLock (multixact.c).
    unlock_on_err!(ExtendMultiXactOffset(result.wrapping_add(1)));

    let next_offset = st.nextOffset.load(Relaxed);
    let offset = if next_offset == 0 {
        nmembers += 1; // reserve member slot 0 too; offset zero means "unset"
        1
    } else {
        next_offset
    };

    if st.oldestOffsetKnown.load(Relaxed)
        && MultiXactOffsetWouldWrap(st.offsetStopLimit.load(Relaxed), next_offset, nmembers as u32)
    {
        let offset_stop_limit = st.offsetStopLimit.load(Relaxed);
        let oldest_db = st.oldestMultiXactDB.load(Relaxed);
        LWLockRelease(genlock)?;
        SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);

        let remaining = offset_stop_limit.wrapping_sub(next_offset).wrapping_sub(1);
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("multixact \"members\" limit exceeded".to_string())
            .errdetail(members_limit_detail(remaining, nmembers as u32))
            .errhint(format!(
                "Execute a database-wide VACUUM in database with OID {oldest_db} with reduced \"vacuum_multixact_freeze_min_age\" and \"vacuum_multixact_freeze_table_age\" settings."
            ))
            .finish(loc("GetNewMultiXactId"))?;
    }

    if !st.oldestOffsetKnown.load(Relaxed)
        || st.nextOffset.load(Relaxed).wrapping_sub(st.oldestOffset.load(Relaxed))
            > MULTIXACT_MEMBER_SAFE_THRESHOLD
    {
        // Signal only on segment crossings so the postmaster isn't swamped.
        if MXOffsetToMemberPage(next_offset) / SLRU_PAGES_PER_SEGMENT
            != MXOffsetToMemberPage(next_offset.wrapping_add(nmembers as u32))
                / SLRU_PAGES_PER_SEGMENT
        {
            SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }
    }

    if st.oldestOffsetKnown.load(Relaxed)
        && MultiXactOffsetWouldWrap(
            st.offsetStopLimit.load(Relaxed),
            next_offset,
            (nmembers as u32).wrapping_add(
                MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32 * OFFSET_WARN_SEGMENTS,
            ),
        )
    {
        let remaining = st
            .offsetStopLimit
            .load(Relaxed)
            .wrapping_sub(next_offset)
            .wrapping_add(nmembers as u32);
        let oldest_db = st.oldestMultiXactDB.load(Relaxed);
        unlock_on_err!(ereport(WARNING)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(members_warning_msg(oldest_db, remaining))
            .errhint(
                "Execute a database-wide VACUUM in that database with reduced \"vacuum_multixact_freeze_min_age\" and \"vacuum_multixact_freeze_table_age\" settings.",
            )
            .finish(loc("GetNewMultiXactId")));
    }

    unlock_on_err!(ExtendMultiXactMember(next_offset, nmembers));

    // Critical section until the caller has written the reserved SLRU
    // space; erroring out mid-write would corrupt the previous MultiXact.
    globals::StartCriticalSection();

    st.nextMXact.store(st.nextMXact.load(Relaxed).wrapping_add(1), Relaxed);
    st.nextOffset
        .store(st.nextOffset.load(Relaxed).wrapping_add(nmembers as u32), Relaxed);

    LWLockRelease(genlock)?;

    Ok((result, offset))
}

pub fn GetMultiXactIdMembers(
    multi: MultiXactId,
    from_pgupgrade: bool,
    is_lock_only: bool,
    consume: &mut dyn FnMut(&[MultiXactMember]),
) -> PgResult<i32> {
    // A pg_upgraded (pre-9.3) multi cannot have running members and must
    // not be resolved against the current valid range.
    if !MultiXactIdIsValid(multi) || from_pgupgrade {
        return Ok(-1);
    }

    // Guard module Drop: the scratch must return to the slot even when
    // get_members_into or `consume` panics (converted-panic ERROR), or every
    // later call panics "re-entered" forever (the snapmgr with_state wedge
    // class, d1a86f62f) — this one in release builds too.
    struct PutBack(Option<MemberScratch>);
    impl Drop for PutBack {
        fn drop(&mut self) {
            put_member_scratch(self.0.take().expect("scratch present until drop"));
        }
    }
    let mut scratch = PutBack(Some(take_member_scratch()));
    let buf = &mut scratch.0.as_mut().expect("scratch present until drop").buf;
    let res = get_members_into(multi, is_lock_only, buf);
    if let Ok(n) = res {
        if n > 0 {
            consume(buf);
        }
    }
    res
}

fn get_members_into(
    multi: MultiXactId,
    is_lock_only: bool,
    out: &mut PgVec<'static, MultiXactMember>,
) -> PgResult<i32> {
    if let Some(n) = mXactCacheGetById(multi, out) {
        return Ok(n);
    }

    // Set OldestVisible only after the oldestMultiXactId check cannot make
    // us error out (multixact.c ordering).
    MultiXactIdSetOldestVisible()?;

    // A lock-only multi older than our oldest visible cannot still be
    // running, so skip the range check.
    if is_lock_only && MultiXactIdPrecedes(multi, oldest_visible(my_slot())) {
        return Ok(-1);
    }

    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let oldest_mxact = st.oldestMultiXactId.load(Relaxed);
    let next_mxact = st.nextMXact.load(Relaxed);
    let next_offset = st.nextOffset.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    if MultiXactIdPrecedes(multi, oldest_mxact) {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(format!(
                "MultiXactId {multi} does no longer exist -- apparent wraparound"
            ))
            .finish(loc("GetMultiXactIdMembers"))?;
    }
    if !MultiXactIdPrecedes(multi, next_mxact) {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(format!(
                "MultiXactId {multi} has not been created yet -- apparent wraparound"
            ))
            .finish(loc("GetMultiXactIdMembers"))?;
    }

    let octl = OffsetCtl();
    let mut pageno = MultiXactIdToOffsetPage(multi);
    let mut entryno = MultiXactIdToOffsetEntry(multi);

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
    let mut slotno = SimpleLruReadPage(octl, pageno, true, multi, &mut bank)?;
    let offset = read_offset_entry(octl, slotno, entryno, &bank);
    debug_assert!(offset != 0);

    // Length = next multi's offset minus ours. Corner case 1: we are the
    // newest multi, so nextOffset is the endpoint (multixact.c).
    let mut tmp_mxact = multi.wrapping_add(1);
    let length: i32;
    if next_mxact == tmp_mxact {
        length = next_offset.wrapping_sub(offset) as i32;
    } else {
        if tmp_mxact < FirstMultiXactId {
            tmp_mxact = FirstMultiXactId;
        }
        let prev_pageno = pageno;
        pageno = MultiXactIdToOffsetPage(tmp_mxact);
        entryno = MultiXactIdToOffsetEntry(tmp_mxact);

        if pageno != prev_pageno {
            let newlock = SimpleLruGetBankLock(octl, pageno);
            if !bank.covers(newlock) {
                bank.release()?;
                bank = LwGuard::acquire(newlock, LW_EXCLUSIVE)?;
            }
            slotno = SimpleLruReadPage(octl, pageno, true, tmp_mxact, &mut bank)?;
        }

        let next_mx_offset = read_offset_entry(octl, slotno, entryno, &bank);
        if next_mx_offset == 0 {
            bank.release()?;
            ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!("MultiXact {multi} has invalid next offset"))
                .finish(loc("GetMultiXactIdMembers"))?;
            unreachable!("ERROR finish returned");
        }
        length = next_mx_offset.wrapping_sub(offset) as i32;
    }

    bank.release()?;

    out.clear();
    out.reserve(length.max(0) as usize);

    let mctl = MemberCtl();
    let mut mguard: Option<LwGuard> = None;
    let mut prev_pageno: i64 = -1;
    let mut mslotno: usize = 0;
    let mut off = offset;

    for _ in 0..length {
        let mpageno = MXOffsetToMemberPage(off);
        let memberoff = MXOffsetToMemberOffset(off);

        if mpageno != prev_pageno {
            let lock = SimpleLruGetBankLock(mctl, mpageno);
            let same_bank = mguard.as_ref().is_some_and(|g| g.covers(lock));
            if !same_bank {
                if let Some(g) = mguard.take() {
                    g.release()?;
                }
                mguard = Some(LwGuard::acquire(lock, LW_EXCLUSIVE)?);
            }
            let g = mguard.as_mut().expect("member bank lock held");
            mslotno = SimpleLruReadPage(mctl, mpageno, true, multi, g)?;
            prev_pageno = mpageno;
        }

        let g = mguard.as_ref().expect("member bank lock held");
        let buf = mctl.page_buffer(mslotno, g);
        let xid = TransactionId::from_ne_bytes(
            buf[memberoff..memberoff + 4].try_into().expect("4-byte xid"),
        );

        // Corner case 2: the unused member slot zero after offset wraparound
        // reads as xid 0; skip it.
        if !TransactionIdIsValid(xid) {
            debug_assert_eq!(off, 0);
            off = off.wrapping_add(1);
            continue;
        }

        let flagsoff = MXOffsetToFlagsOffset(off);
        let bshift = MXOffsetToFlagsBitShift(off);
        let flagsval =
            u32::from_ne_bytes(buf[flagsoff..flagsoff + 4].try_into().expect("4-byte flags"));

        out.push(MultiXactMember {
            xid,
            status: mxstatus_from_word((flagsval >> bshift) & MXACT_MEMBER_XACT_BITMASK),
        });
        off = off.wrapping_add(1);
    }

    if let Some(g) = mguard.take() {
        g.release()?;
    }

    debug_assert!(!out.is_empty());
    mXactCachePut(multi, out);
    Ok(out.len() as i32)
}

pub fn MultiXactIdGetUpdateXid(xmax: MultiXactId, is_lock_only: bool) -> PgResult<TransactionId> {
    if is_lock_only {
        return Ok(0);
    }
    let mut update_xact: TransactionId = 0;
    GetMultiXactIdMembers(xmax, false, false, &mut |members| {
        for m in members {
            if !ISUPDATE_from_mxstatus(m.status) {
                continue;
            }
            debug_assert_eq!(update_xact, 0);
            update_xact = m.xid;
            if !cfg!(debug_assertions) {
                break;
            }
        }
    })?;
    Ok(update_xact)
}

pub fn AtEOXact_MultiXact() {
    // This backend owns its slots; single MultiXactId stores are atomic, so
    // no lock is needed (multixact.c).
    let me = my_slot();
    set_oldest_member(me, InvalidMultiXactId);
    set_oldest_visible(me, InvalidMultiXactId);
    cache_clear();
}

pub const TWOPHASE_RM_MULTIXACT_ID: u8 = 3;

pub fn AtPrepare_MultiXact() -> PgResult<()> {
    let my_oldest_member = oldest_member(my_slot());
    if MultiXactIdIsValid(my_oldest_member) {
        twophase_seams::register_two_phase_record::call(
            TWOPHASE_RM_MULTIXACT_ID,
            0,
            &my_oldest_member.to_ne_bytes(),
        )?;
    }
    Ok(())
}

pub fn PostPrepare_MultiXact(xid: TransactionId) {
    let my_oldest_member = oldest_member(my_slot());
    if MultiXactIdIsValid(my_oldest_member) {
        let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)
            .expect("PostPrepare_MultiXact: dummy proc");
        // Lock so others see both changes, not just the reset of our slot
        // (multixact.c).
        LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())
            .expect("PostPrepare_MultiXact: MultiXactGenLock");
        set_oldest_member(prepared_xact_member_slot(dummy), my_oldest_member);
        set_oldest_member(my_slot(), InvalidMultiXactId);
        LWLockRelease(MultiXactGenLock()).expect("PostPrepare_MultiXact: unlock");
    }
    set_oldest_visible(my_slot(), InvalidMultiXactId);
    cache_clear();
}

pub fn multixact_twophase_recover(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)?;
    assert_eq!(recdata.len(), 4);
    let oldest_member = MultiXactId::from_ne_bytes(recdata.try_into().unwrap());
    set_oldest_member(prepared_xact_member_slot(dummy), oldest_member);
    Ok(())
}

pub fn multixact_twophase_postcommit(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let dummy = twophase_seams::two_phase_get_dummy_proc_number::call(xid, true)?;
    assert_eq!(recdata.len(), 4);
    set_oldest_member(prepared_xact_member_slot(dummy), InvalidMultiXactId);
    Ok(())
}

pub fn multixact_twophase_postabort(
    xid: TransactionId,
    info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    multixact_twophase_postcommit(xid, info, recdata)
}

fn MultiXactOffsetBuffers() -> i32 {
    globals::multixact_offset_buffers()
}

fn MultiXactMemberBuffers() -> i32 {
    globals::multixact_member_buffers()
}

fn num_member_slots() -> usize {
    (globals::MaxBackends() + guc_tables::vars::max_prepared_xacts.read()) as usize
}

fn num_visible_slots() -> usize {
    globals::MaxBackends() as usize
}

// MultiXactSharedStateShmemSize: the C scalar header is 48 bytes
// (offsetof(MultiXactStateData, perBackendXactIds)); accounting only — the
// backing store is a leaked process-local struct (procarray precedent).
fn shared_multixact_state_size() -> Size {
    48 + core::mem::size_of::<MultiXactId>() * (num_member_slots() + num_visible_slots())
}

pub fn MultiXactShmemSize() -> Size {
    let mut size = shared_multixact_state_size();
    size += SimpleLruShmemSize(MultiXactOffsetBuffers(), 0);
    size += SimpleLruShmemSize(MultiXactMemberBuffers(), 0);
    size
}

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
    // Members per page doesn't divide BLCKSZ evenly, so C skips the
    // PagePrecedes unit tests for the members SLRU.

    if MXOFFSET_CTL.set(offset_ctl).is_err() || MXMEMBER_CTL.set(member_ctl).is_err() {
        panic!("MultiXactShmemInit called twice");
    }

    let member_slots = num_member_slots();
    let total_slots = member_slots + num_visible_slots();
    let mut per_backend = Vec::with_capacity(total_slots);
    per_backend.resize_with(total_slots, || AtomicU32::new(0));
    let state: &'static MultiXactStateShared = Box::leak(Box::new(MultiXactStateShared {
        nextMXact: AtomicU32::new(0),
        nextOffset: AtomicU32::new(0),
        finishedStartup: AtomicBool::new(false),
        oldestMultiXactId: AtomicU32::new(0),
        oldestMultiXactDB: AtomicU32::new(0),
        oldestOffset: AtomicU32::new(0),
        oldestOffsetKnown: AtomicBool::new(false),
        multiVacLimit: AtomicU32::new(0),
        multiWarnLimit: AtomicU32::new(0),
        multiStopLimit: AtomicU32::new(0),
        multiWrapLimit: AtomicU32::new(0),
        offsetStopLimit: AtomicU32::new(0),
        perBackendXactIds: per_backend.into_boxed_slice(),
        num_member_slots: member_slots,
    }));
    if MULTIXACT_STATE.set(state).is_err() {
        panic!("MultiXactShmemInit called twice");
    }
    PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
    LAST_INITIALIZED_OFFSETS_PAGE.set(-1);
    Ok(())
}

/// Crash-cycle reset in place to the post-MultiXactShmemInit boot image
/// (notes/crash-restart-design.md); TrimMultiXact re-seeds after recovery.
pub fn MultiXactShmemResetAfterCrash() {
    slru::SimpleLruResetAfterCrash(OffsetCtl());
    slru::SimpleLruResetAfterCrash(MemberCtl());

    let st = MultiXactState();
    assert_eq!(st.num_member_slots, num_member_slots());
    st.nextMXact.store(0, Relaxed);
    st.nextOffset.store(0, Relaxed);
    st.finishedStartup.store(false, Relaxed);
    st.oldestMultiXactId.store(0, Relaxed);
    st.oldestMultiXactDB.store(0, Relaxed);
    st.oldestOffset.store(0, Relaxed);
    st.oldestOffsetKnown.store(false, Relaxed);
    st.multiVacLimit.store(0, Relaxed);
    st.multiWarnLimit.store(0, Relaxed);
    st.multiStopLimit.store(0, Relaxed);
    st.multiWrapLimit.store(0, Relaxed);
    st.offsetStopLimit.store(0, Relaxed);
    for slot in st.perBackendXactIds.iter() {
        slot.store(0, Relaxed);
    }
}

pub fn check_multixact_offset_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("multixact_offset_buffers", newval)
}

pub fn check_multixact_member_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("multixact_member_buffers", newval)
}

pub fn BootStrapMultiXact() -> PgResult<()> {
    let octl = OffsetCtl();
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, 0), LW_EXCLUSIVE)?;
    let slotno = ZeroMultiXactOffsetPage(0, false, &mut bank)?;
    SimpleLruWritePage(octl, slotno, &mut bank)?;
    debug_assert!(!octl.page_dirty(slotno, &bank));
    bank.release()?;

    let mctl = MemberCtl();
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(mctl, 0), LW_EXCLUSIVE)?;
    let slotno = ZeroMultiXactMemberPage(0, false, &mut bank)?;
    SimpleLruWritePage(mctl, slotno, &mut bank)?;
    debug_assert!(!mctl.page_dirty(slotno, &bank));
    bank.release()
}

fn ZeroMultiXactOffsetPage(pageno: i64, write_xlog: bool, bank: &mut LwGuard) -> PgResult<usize> {
    let slotno = SimpleLruZeroPage(OffsetCtl(), pageno, bank)?;
    if write_xlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_OFF_PAGE)?;
    }
    Ok(slotno)
}

fn ZeroMultiXactMemberPage(pageno: i64, write_xlog: bool, bank: &mut LwGuard) -> PgResult<usize> {
    let slotno = SimpleLruZeroPage(MemberCtl(), pageno, bank)?;
    if write_xlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_MEM_PAGE)?;
    }
    Ok(slotno)
}

fn MaybeExtendOffsetSlru() -> PgResult<()> {
    let octl = OffsetCtl();
    let pageno = MultiXactIdToOffsetPage(MultiXactState().nextMXact.load(Relaxed));

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
    if !SimpleLruDoesPhysicalPageExist(octl, pageno)? {
        let slotno = ZeroMultiXactOffsetPage(pageno, false, &mut bank)?;
        SimpleLruWritePage(octl, slotno, &mut bank)?;
    }
    bank.release()
}

pub fn StartupMultiXact() -> PgResult<()> {
    let st = MultiXactState();
    let multi = st.nextMXact.load(Relaxed);
    let offset = st.nextOffset.load(Relaxed);

    OffsetCtl().set_latest_page_number(MultiXactIdToOffsetPage(multi));
    MemberCtl().set_latest_page_number(MXOffsetToMemberPage(offset));
    Ok(())
}

pub fn TrimMultiXact() -> PgResult<()> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let next_mxact = st.nextMXact.load(Relaxed);
    let offset = st.nextOffset.load(Relaxed);
    let oldest_mxact = st.oldestMultiXactId.load(Relaxed);
    let oldest_mxact_db = st.oldestMultiXactDB.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    let octl = OffsetCtl();
    let pageno = MultiXactIdToOffsetPage(next_mxact);
    octl.set_latest_page_number(pageno);

    // Set nextMXact's offset and zero the page remainder: multixact ignores
    // "WAL before data", so successors may carry stale nonzero offsets (see
    // TrimCLOG notes).
    let entryno = MultiXactIdToOffsetEntry(next_mxact);
    {
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
        let slotno = if entryno == 0 {
            SimpleLruZeroPage(octl, pageno, &mut bank)?
        } else {
            SimpleLruReadPage(octl, pageno, true, next_mxact, &mut bank)?
        };
        write_offset_entry(octl, slotno, entryno, offset, &mut bank);
        if entryno != 0 && (entryno + 1) * SIZE_OF_MULTIXACT_OFFSET != BLCKSZ {
            let start = (entryno + 1) * SIZE_OF_MULTIXACT_OFFSET;
            octl.page_buffer_mut(slotno, &mut bank)[start..BLCKSZ].fill(0);
        }
        octl.mark_page_dirty(slotno, &bank);
        bank.release()?;
    }

    let mctl = MemberCtl();
    let pageno = MXOffsetToMemberPage(offset);
    mctl.set_latest_page_number(pageno);

    let flagsoff = MXOffsetToFlagsOffset(offset);
    if flagsoff != 0 {
        let memberoff = MXOffsetToMemberOffset(offset);
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(mctl, pageno), LW_EXCLUSIVE)?;
        let slotno = SimpleLruReadPage(mctl, pageno, true, offset, &mut bank)?;
        // The current group's remaining flag bits are always reset before
        // writing, so only the area from the xid onward needs zeroing.
        mctl.page_buffer_mut(slotno, &mut bank)[memberoff..BLCKSZ].fill(0);
        mctl.mark_page_dirty(slotno, &bank);
        bank.release()?;
    }

    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    st.finishedStartup.store(true, Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    SetMultiXactIdLimit(oldest_mxact, oldest_mxact_db, true)
}

pub fn MultiXactGetCheckptMulti(
    _is_shutdown: bool,
) -> PgResult<(MultiXactId, MultiXactOffset, MultiXactId, Oid)> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let result = (
        st.nextMXact.load(Relaxed),
        st.nextOffset.load(Relaxed),
        st.oldestMultiXactId.load(Relaxed),
        Oid::from(st.oldestMultiXactDB.load(Relaxed)),
    );
    LWLockRelease(MultiXactGenLock())?;
    Ok(result)
}

pub fn CheckPointMultiXact() -> PgResult<()> {
    SimpleLruWriteAll(OffsetCtl(), true)?;
    SimpleLruWriteAll(MemberCtl(), true)
}

pub fn MultiXactSetNextMXact(
    next_multi: MultiXactId,
    next_multi_offset: MultiXactOffset,
) -> PgResult<()> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    st.nextMXact.store(next_multi, Relaxed);
    st.nextOffset.store(next_multi_offset, Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    // During binary upgrade the offsets SLRU must already contain the next
    // value's page.
    if globals::IsBinaryUpgrade() {
        MaybeExtendOffsetSlru()?;
    }
    Ok(())
}

pub fn SetMultiXactIdLimit(
    oldest_datminmxid: MultiXactId,
    oldest_datoid: Oid,
    is_startup: bool,
) -> PgResult<()> {
    debug_assert!(MultiXactIdIsValid(oldest_datminmxid));

    // The half-space wrap horizon is a pretense (multis don't wrap like
    // xids); member wraparound is handled separately below.
    let mut multi_wrap_limit = oldest_datminmxid.wrapping_add(MaxMultiXactId >> 1);
    if multi_wrap_limit < FirstMultiXactId {
        multi_wrap_limit = multi_wrap_limit.wrapping_add(FirstMultiXactId);
    }

    let mut multi_stop_limit = multi_wrap_limit.wrapping_sub(3_000_000);
    if multi_stop_limit < FirstMultiXactId {
        multi_stop_limit = multi_stop_limit.wrapping_sub(FirstMultiXactId);
    }

    let mut multi_warn_limit = multi_wrap_limit.wrapping_sub(40_000_000);
    if multi_warn_limit < FirstMultiXactId {
        multi_warn_limit = multi_warn_limit.wrapping_sub(FirstMultiXactId);
    }

    let freeze_max_age = guc_tables::vars::autovacuum_multixact_freeze_max_age.read();
    let mut multi_vac_limit = oldest_datminmxid.wrapping_add(freeze_max_age as u32);
    if multi_vac_limit < FirstMultiXactId {
        multi_vac_limit = multi_vac_limit.wrapping_add(FirstMultiXactId);
    }

    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    st.oldestMultiXactId.store(oldest_datminmxid, Relaxed);
    st.oldestMultiXactDB.store(oldest_datoid.into(), Relaxed);
    st.multiVacLimit.store(multi_vac_limit, Relaxed);
    st.multiWarnLimit.store(multi_warn_limit, Relaxed);
    st.multiStopLimit.store(multi_stop_limit, Relaxed);
    st.multiWrapLimit.store(multi_wrap_limit, Relaxed);
    let cur_multi = st.nextMXact.load(Relaxed);
    let finished_startup = st.finishedStartup.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    dlog(
        DEBUG1,
        format!("MultiXactId wrap limit is {multi_wrap_limit}, limited by database with OID {oldest_datoid}"),
    );

    // Actual limits need a consistent data directory; not while replaying.
    if !finished_startup {
        return Ok(());
    }

    debug_assert!(!xlogutils_seams::in_recovery::call());

    let needs_offset_vacuum = SetOffsetVacuumLimit(is_startup)?;

    if (MultiXactIdPrecedes(multi_vac_limit, cur_multi) || needs_offset_vacuum)
        && globals::IsUnderPostmaster()
    {
        SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER);
    }

    if MultiXactIdPrecedes(multi_warn_limit, cur_multi) {
        let oldest_datname = if xact_seams::is_transaction_or_transaction_block::call() {
            dbcommands_seams::get_database_name::call(oldest_datoid)?
        } else {
            None
        };
        let remaining = multi_wrap_limit.wrapping_sub(cur_multi);
        let msg = match &oldest_datname {
            Some(name) => multixactid_warning_msg_named(name, remaining),
            None => multixactid_warning_msg_oid(oldest_datoid, remaining),
        };
        ereport(WARNING)
            .errmsg(msg)
            .errhint(
                "To avoid MultiXactId assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
            )
            .finish(loc("SetMultiXactIdLimit"))?;
    }

    Ok(())
}

pub fn MultiXactAdvanceNextMXact(
    min_multi: MultiXactId,
    min_multi_offset: MultiXactOffset,
) -> PgResult<()> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    let mut set_multi = false;
    let mut set_offset = false;
    if MultiXactIdPrecedes(st.nextMXact.load(Relaxed), min_multi) {
        st.nextMXact.store(min_multi, Relaxed);
        set_multi = true;
    }
    if MultiXactOffsetPrecedes(st.nextOffset.load(Relaxed), min_multi_offset) {
        st.nextOffset.store(min_multi_offset, Relaxed);
        set_offset = true;
    }
    LWLockRelease(MultiXactGenLock())?;

    if set_multi {
        dlog(DEBUG1, format!("MultiXact: setting next multi to {min_multi}"));
    }
    if set_offset {
        dlog(DEBUG1, format!("MultiXact: setting next offset to {min_multi_offset}"));
    }
    Ok(())
}

pub fn MultiXactAdvanceOldest(oldest_multi: MultiXactId, oldest_multi_db: Oid) -> PgResult<()> {
    debug_assert!(xlogutils_seams::in_recovery::call());

    if MultiXactIdPrecedes(MultiXactState().oldestMultiXactId.load(Relaxed), oldest_multi) {
        SetMultiXactIdLimit(oldest_multi, oldest_multi_db, false)?;
    }
    Ok(())
}

fn ExtendMultiXactOffset(multi: MultiXactId) -> PgResult<()> {
    // Work only at the first entry of a page; just after wraparound the
    // first MultiXactId of page zero is FirstMultiXactId.
    if MultiXactIdToOffsetEntry(multi) != 0 && multi != FirstMultiXactId {
        return Ok(());
    }

    let octl = OffsetCtl();
    let pageno = MultiXactIdToOffsetPage(multi);
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
    ZeroMultiXactOffsetPage(pageno, true, &mut bank)?;
    bank.release()
}

fn ExtendMultiXactMember(offset: MultiXactOffset, nmembers: i32) -> PgResult<()> {
    let mut offset = offset;
    let mut nmembers = nmembers;

    while nmembers > 0 {
        if MXOffsetToFlagsOffset(offset) == 0 && MXOffsetToFlagsBitShift(offset) == 0 {
            let mctl = MemberCtl();
            let pageno = MXOffsetToMemberPage(offset);
            let mut bank = LwGuard::acquire(SimpleLruGetBankLock(mctl, pageno), LW_EXCLUSIVE)?;
            ZeroMultiXactMemberPage(pageno, true, &mut bank)?;
            bank.release()?;
        }

        // Items until end of page; clamp to the last members page when
        // adding n members would overflow the offset space.
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

pub fn GetOldestMultiXactId() -> PgResult<MultiXactId> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;

    let mut next_mxact = st.nextMXact.load(Relaxed);
    if next_mxact < FirstMultiXactId {
        next_mxact = FirstMultiXactId;
    }
    let mut oldest_mxact = next_mxact;
    for i in 0..st.num_member_slots {
        let this = oldest_member(i);
        if MultiXactIdIsValid(this) && MultiXactIdPrecedes(this, oldest_mxact) {
            oldest_mxact = this;
        }
    }
    for i in 0..(st.perBackendXactIds.len() - st.num_member_slots) {
        let this = oldest_visible(i);
        if MultiXactIdIsValid(this) && MultiXactIdPrecedes(this, oldest_mxact) {
            oldest_mxact = this;
        }
    }

    LWLockRelease(MultiXactGenLock())?;
    Ok(oldest_mxact)
}

fn SetOffsetVacuumLimit(is_startup: bool) -> PgResult<bool> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactTruncationLock(), LW_SHARED, globals::MyProcNumber())?;

    macro_rules! unlock_on_err {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => {
                    let _ = LWLockRelease(MultiXactTruncationLock());
                    return Err(e);
                }
            }
        };
    }

    unlock_on_err!(LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber()));
    debug_assert!(st.finishedStartup.load(Relaxed));
    let oldest_multixact_id = st.oldestMultiXactId.load(Relaxed);
    let next_mxact = st.nextMXact.load(Relaxed);
    let next_offset = st.nextOffset.load(Relaxed);
    let prev_oldest_offset_known = st.oldestOffsetKnown.load(Relaxed);
    let prev_oldest_offset = st.oldestOffset.load(Relaxed);
    let prev_offset_stop_limit = st.offsetStopLimit.load(Relaxed);
    unlock_on_err!(LWLockRelease(MultiXactGenLock()));

    let mut oldest_offset: MultiXactOffset = 0;
    let mut oldest_offset_known = false;

    if oldest_multixact_id == next_mxact {
        // No multixacts, or wrong limits last time: nextOffset is safe.
        oldest_offset = next_offset;
        oldest_offset_known = true;
    } else {
        match unlock_on_err!(find_multixact_start(oldest_multixact_id)) {
            Some(off) => {
                oldest_offset = off;
                oldest_offset_known = true;
                dlog(DEBUG1, format!("oldest MultiXactId member is at offset {oldest_offset}"));
            }
            None => {
                dlog(
                    LOG,
                    format!("MultiXact member wraparound protections are disabled because oldest checkpointed MultiXact {oldest_multixact_id} does not exist on disk"),
                );
            }
        }
    }

    LWLockRelease(MultiXactTruncationLock())?;

    let mut offset_stop_limit: MultiXactOffset = 0;
    if oldest_offset_known {
        // Back off to the start of the oldest offset's segment, then one
        // more segment as buffer.
        offset_stop_limit = oldest_offset
            - (oldest_offset % (MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32));
        offset_stop_limit = offset_stop_limit
            .wrapping_sub(MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT as u32);

        if !prev_oldest_offset_known && !is_startup {
            dlog(LOG, "MultiXact member wraparound protections are now enabled".to_string());
        }
        dlog(
            DEBUG1,
            format!("MultiXact member stop limit is now {offset_stop_limit} based on MultiXact {oldest_multixact_id}"),
        );
    } else if prev_oldest_offset_known {
        // Keep the previous pass's values rather than dropping protection.
        oldest_offset = prev_oldest_offset;
        oldest_offset_known = true;
        offset_stop_limit = prev_offset_stop_limit;
    }

    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    st.oldestOffset.store(oldest_offset, Relaxed);
    st.oldestOffsetKnown.store(oldest_offset_known, Relaxed);
    st.offsetStopLimit.store(offset_stop_limit, Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    Ok(!oldest_offset_known
        || next_offset.wrapping_sub(oldest_offset) > MULTIXACT_MEMBER_SAFE_THRESHOLD)
}

fn MultiXactOffsetWouldWrap(
    boundary: MultiXactOffset,
    start: MultiXactOffset,
    distance: u32,
) -> bool {
    debug_assert!(distance > 0);
    let mut finish = start.wrapping_add(distance);

    // A wrapped finish means more than the entire offset range was used;
    // always treat that as a wrap.
    if finish < start {
        finish = finish.wrapping_add(1);
    }

    if start < boundary {
        finish >= boundary || finish < start
    } else {
        finish >= boundary && finish < start
    }
}

fn find_multixact_start(multi: MultiXactId) -> PgResult<Option<MultiXactOffset>> {
    debug_assert!(MultiXactState().finishedStartup.load(Relaxed));

    let octl = OffsetCtl();
    let pageno = MultiXactIdToOffsetPage(multi);
    let entryno = MultiXactIdToOffsetEntry(multi);

    // Flush dirty data so DoesPhysicalPageExist sees current truth.
    SimpleLruWriteAll(octl, true)?;
    SimpleLruWriteAll(MemberCtl(), true)?;

    if !SimpleLruDoesPhysicalPageExist(octl, pageno)? {
        return Ok(None);
    }

    let (slotno, bank) = SimpleLruReadPage_ReadOnly(octl, pageno, multi)?;
    let offset = read_offset_entry(octl, slotno, entryno, &bank);
    bank.release()?;

    Ok(Some(offset))
}

fn ReadMultiXactCounts() -> PgResult<Option<(u32, MultiXactOffset)>> {
    let st = MultiXactState();
    LWLockAcquire(MultiXactGenLock(), LW_SHARED, globals::MyProcNumber())?;
    let next_offset = st.nextOffset.load(Relaxed);
    let oldest_multixact_id = st.oldestMultiXactId.load(Relaxed);
    let next_multixact_id = st.nextMXact.load(Relaxed);
    let oldest_offset = st.oldestOffset.load(Relaxed);
    let oldest_offset_known = st.oldestOffsetKnown.load(Relaxed);
    LWLockRelease(MultiXactGenLock())?;

    if !oldest_offset_known {
        return Ok(None);
    }
    Ok(Some((
        next_multixact_id.wrapping_sub(oldest_multixact_id),
        next_offset.wrapping_sub(oldest_offset),
    )))
}

pub fn MultiXactMemberFreezeThreshold() -> PgResult<i32> {
    let freeze_max_age = guc_tables::vars::autovacuum_multixact_freeze_max_age.read();

    let Some((multixacts, members)) = ReadMultiXactCounts()? else {
        return Ok(0);
    };
    if members <= MULTIXACT_MEMBER_SAFE_THRESHOLD {
        return Ok(freeze_max_age);
    }

    let fraction = (members - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64
        / (MULTIXACT_MEMBER_DANGER_THRESHOLD - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64;
    let victim_multixacts = (multixacts as f64 * fraction) as u32;

    if victim_multixacts > multixacts {
        return Ok(0);
    }
    Ok(((multixacts - victim_multixacts) as i32).min(freeze_max_age))
}

fn PerformMembersTruncation(
    oldest_offset: MultiXactOffset,
    new_oldest_offset: MultiXactOffset,
) -> PgResult<()> {
    let mctl = MemberCtl();
    let maxsegment = MXOffsetToMemberSegment(MaxMultiXactOffset);
    let endsegment = MXOffsetToMemberSegment(new_oldest_offset);
    let mut segment = MXOffsetToMemberSegment(oldest_offset);

    // The last segment can still contain valid (possibly partial) data.
    while segment != endsegment {
        SlruDeleteSegment(mctl, segment)?;
        segment = if segment == maxsegment { 0 } else { segment + 1 };
    }
    Ok(())
}

fn PerformOffsetsTruncation(
    _oldest_multi: MultiXactId,
    new_oldest_multi: MultiXactId,
) -> PgResult<()> {
    // Step back one multixact so the cutoff page can't be one that doesn't
    // exist yet when oldestMulti == nextMulti and would start a page.
    SimpleLruTruncate(
        OffsetCtl(),
        MultiXactIdToOffsetPage(PreviousMultiXactId(new_oldest_multi)),
    )
}

pub fn TruncateMultiXact(
    new_oldest_multi: MultiXactId,
    _new_oldest_multi_db: Oid,
) -> PgResult<()> {
    // C-exact early exit: nothing to truncate away unless the horizon moved
    // forward past the current oldest (datminmxid never advances until the
    // freeze lane lands, so this is the live arm).
    let oldest_multi = MultiXactState().oldestMultiXactId.load(Relaxed);
    debug_assert!(MultiXactIdIsValid(oldest_multi));
    if MultiXactIdPrecedesOrEquals(new_oldest_multi, oldest_multi) {
        return Ok(());
    }
    panic!(
        "unported caller path reached: TruncateMultiXact (multixact.c) — vacuum lane \
         (vac_truncate_clog); needs delay-chkpt seam + WAL truncate record"
    );
}

fn MultiXactOffsetPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut multi1 = (page1 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE);
    multi1 = multi1.wrapping_add(FirstMultiXactId + 1);
    let mut multi2 = (page2 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE);
    multi2 = multi2.wrapping_add(FirstMultiXactId + 1);

    MultiXactIdPrecedes(multi1, multi2)
        && MultiXactIdPrecedes(multi1, multi2.wrapping_add(MULTIXACT_OFFSETS_PER_PAGE - 1))
}

fn MultiXactMemberPagePrecedes(page1: i64, page2: i64) -> bool {
    let offset1 = (page1 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE);
    let offset2 = (page2 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE);

    MultiXactOffsetPrecedes(offset1, offset2)
        && MultiXactOffsetPrecedes(offset1, offset2.wrapping_add(MULTIXACT_MEMBERS_PER_PAGE - 1))
}

fn WriteMZeroPageXlogRec(pageno: i64, info: u8) -> PgResult<()> {
    xloginsert_seams::xlog_insert::call(RM_MULTIXACT_ID, info, &[&pageno.to_ne_bytes()])?;
    Ok(())
}

pub fn multixact_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("multixact_redo dispatched on a reader with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;
    let record_xid = decoded.xl_xid;

    debug_assert!(decoded.max_block_id < 0);
    // SAFETY: the reader's current record stays `decoded` for this whole call.
    let data = unsafe { decoded.main_data_bytes() };

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE {
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("short ZERO_OFF_PAGE record"));

        // Skip pages already initialized while replaying a CREATE record
        // from an older minor version.
        if PRE_INITIALIZED_OFFSETS_PAGE.get() != pageno {
            let octl = OffsetCtl();
            let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, pageno), LW_EXCLUSIVE)?;
            let slotno = ZeroMultiXactOffsetPage(pageno, false, &mut bank)?;
            SimpleLruWritePage(octl, slotno, &mut bank)?;
            debug_assert!(!octl.page_dirty(slotno, &bank));
            bank.release()?;

            LAST_INITIALIZED_OFFSETS_PAGE.set(pageno);
        } else {
            dlog(
                DEBUG1,
                format!("skipping initialization of offsets page {pageno} because it was already initialized on multixid creation"),
            );
        }
        PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
        Ok(())
    } else if info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        let pageno = i64::from_ne_bytes(data[..8].try_into().expect("short ZERO_MEM_PAGE record"));

        let mctl = MemberCtl();
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(mctl, pageno), LW_EXCLUSIVE)?;
        let slotno = ZeroMultiXactMemberPage(pageno, false, &mut bank)?;
        SimpleLruWritePage(mctl, slotno, &mut bank)?;
        debug_assert!(!mctl.page_dirty(slotno, &bank));
        bank.release()
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        let mid = MultiXactId::from_ne_bytes(data[0..4].try_into().expect("short CREATE record"));
        let moff =
            MultiXactOffset::from_ne_bytes(data[4..8].try_into().expect("short CREATE record"));
        let nmembers = i32::from_ne_bytes(data[8..12].try_into().expect("short CREATE record"));

        let pre = PRE_INITIALIZED_OFFSETS_PAGE.get();
        if pre != -1 {
            dlog(
                LOG,
                format!("expected to see an XLOG_MULTIXACT_ZERO_OFF_PAGE record for page {pre} that was implicitly initialized earlier"),
            );
            PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
        }

        let cx = MemoryContext::new("multixact_redo members");
        let mut members: PgVec<'_, MultiXactMember> = PgVec::new_in(cx.mcx());
        members.reserve(nmembers.max(0) as usize);
        let mut max_xid = record_xid;
        for i in 0..nmembers as usize {
            let base = SIZE_OF_MULTIXACT_CREATE + i * SIZE_OF_MULTIXACT_MEMBER;
            let xid = TransactionId::from_ne_bytes(
                data[base..base + 4].try_into().expect("short CREATE member"),
            );
            let status = i32::from_ne_bytes(
                data[base + 4..base + 8].try_into().expect("short CREATE member"),
            );
            members.push(MultiXactMember {
                xid,
                status: mxstatus_from_word(status as u32),
            });
            if TransactionIdPrecedes(max_xid, xid) {
                max_xid = xid;
            }
        }

        RecordNewMultiXact(mid, moff, &members)?;
        MultiXactAdvanceNextMXact(mid.wrapping_add(1), moff.wrapping_add(nmembers as u32))?;

        // Any XID here ought to have other WAL evidence, but be safe.
        varsup_seams::advance_next_full_transaction_id_past_xid::call(max_xid)?;
        Ok(())
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        debug_assert!(data.len() >= SIZE_OF_MULTIXACT_TRUNCATE);
        let oldest_multi_db =
            Oid::from(u32::from_ne_bytes(data[0..4].try_into().expect("short TRUNCATE record")));
        let start_trunc_off =
            MultiXactId::from_ne_bytes(data[4..8].try_into().expect("short TRUNCATE record"));
        let end_trunc_off =
            MultiXactId::from_ne_bytes(data[8..12].try_into().expect("short TRUNCATE record"));
        let start_trunc_memb =
            MultiXactOffset::from_ne_bytes(data[12..16].try_into().expect("short TRUNCATE record"));
        let end_trunc_memb =
            MultiXactOffset::from_ne_bytes(data[16..20].try_into().expect("short TRUNCATE record"));

        dlog(
            DEBUG1,
            format!(
                "replaying multixact truncation: offsets [{}, {}), offsets segments [{:X}, {:X}), members [{}, {}), members segments [{:X}, {:X})",
                start_trunc_off,
                end_trunc_off,
                MultiXactIdToOffsetSegment(start_trunc_off),
                MultiXactIdToOffsetSegment(end_trunc_off),
                start_trunc_memb,
                end_trunc_memb,
                MXOffsetToMemberSegment(start_trunc_memb),
                MXOffsetToMemberSegment(end_trunc_memb),
            ),
        );

        LWLockAcquire(MultiXactTruncationLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
        let res = (|| {
            SetMultiXactIdLimit(end_trunc_off, oldest_multi_db, false)?;
            PerformMembersTruncation(start_trunc_memb, end_trunc_memb)?;
            PerformOffsetsTruncation(start_trunc_off, end_trunc_off)
        })();
        let released = LWLockRelease(MultiXactTruncationLock());
        res?;
        released
    } else {
        panic!("multixact_redo: unknown op code {info}");
    }
}

pub fn multixactoffsetssyncfiletag(ftag: &FileTag) -> PgResult<(i32, SlruPath)> {
    SlruSyncFileTag(OffsetCtl(), ftag)
}

pub fn multixactmemberssyncfiletag(ftag: &FileTag) -> PgResult<(i32, SlruPath)> {
    SlruSyncFileTag(MemberCtl(), ftag)
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

pub fn init_seams() {
    multixact_seams::at_eoxact_multixact::set(AtEOXact_MultiXact);
    multixact_seams::at_prepare_multixact::set(AtPrepare_MultiXact);
    multixact_seams::post_prepare_multixact::set(PostPrepare_MultiXact);
    multixact_seams::get_multi_xact_id_members::set(GetMultiXactIdMembers);
    multixact_seams::multi_xact_id_is_running::set(MultiXactIdIsRunning);
    multixact_seams::multi_xact_id_create_from_members::set(MultiXactIdCreateFromMembers);
    multixact_seams::multi_xact_id_create::set(MultiXactIdCreate);
    multixact_seams::multi_xact_id_expand::set(MultiXactIdExpand);
    multixact_seams::multi_xact_id_set_oldest_member::set(MultiXactIdSetOldestMember);
    multixact_seams::startup_multixact::set(StartupMultiXact);
    multixact_seams::trim_multixact::set(TrimMultiXact);
    multixact_seams::check_point_multixact::set(CheckPointMultiXact);
    multixact_seams::multixact_set_next_mxact::set(|next_multi, next_offset| {
        MultiXactSetNextMXact(next_multi, next_offset).expect("MultiXactSetNextMXact failed")
    });
    multixact_seams::set_multixact_id_limit::set(|oldest_multi, oldest_db, is_startup| {
        SetMultiXactIdLimit(oldest_multi, oldest_db, is_startup)
            .expect("SetMultiXactIdLimit failed")
    });
    multixact_seams::multixact_advance_next_mxact::set(|min_multi, min_offset| {
        MultiXactAdvanceNextMXact(min_multi, min_offset).expect("MultiXactAdvanceNextMXact failed")
    });
    multixact_seams::multixact_advance_oldest::set(MultiXactAdvanceOldest);
    multixact_seams::multixact_get_checkpt_multi::set(|is_shutdown| {
        MultiXactGetCheckptMulti(is_shutdown).expect("MultiXactGetCheckptMulti failed")
    });

    fn check_offset_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_multixact_offset_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    fn check_member_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_multixact_member_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    guc_tables::hooks::check_multixact_offset_buffers.install(check_offset_hook);
    guc_tables::hooks::check_multixact_member_buffers.install(check_member_hook);
}

#[cfg(test)]
mod tests;
