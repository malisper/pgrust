// slot.c uses C identifier conventions (CamelCase functions, ALLCAPS globals
// and constants). Mirror them with crate-level allows.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `replication/slot.c` — replication slot management.
//!
//! Replication slots keep crash-safe, standby-allocatable state about
//! replication streams and prevent the premature removal of WAL or of old
//! tuple versions. This is a full port of PostgreSQL 18.3's `slot.c`.
//!
//! `ReplicationSlotCtl->replication_slots[]` is genuinely cross-backend shared
//! memory in C. A backend maps to a thread here, so the array is process-wide
//! shared state ([`SLOT_ARRAY`]); each slot's per-field `mutex` is the real
//! `Spinlock`, and the slot body lives behind a `SyncUnsafeCell` exactly as C
//! shared memory does, with access disciplined by the named
//! `ReplicationSlot{Allocation,Control}Lock` LWLocks (taken through the
//! lwlock-seam by main-array offset) and the per-slot spinlock. `MyReplicationSlot`
//! is this backend's acquired-slot index, a `thread_local`.
//!
//! Genuine externals (xlog, procarray, the file substrate, pgstat_replslot,
//! slotsync, walsender, the CV protocol, GUC reload) are reached through their
//! owners' seam crates and panic loudly until those owners land.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::cell::UnsafeCell;
use std::sync::OnceLock;

use ::utils_error::{ereport, elog};
use ::types_core::init::BackendType;
use ::types_core::{
    Oid, Size, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo, InvalidOid, NAMEDATALEN,
};
use ::types_error::{
    ErrorLevel, PgResult, SqlState, DEBUG1, ERROR, FATAL, LOG, PANIC, WARNING,
    ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_DATA_CORRUPTED, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NAME_TOO_LONG, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT,
};
use ::types_error::ErrorLocation;
use ::replication_slot_2::{
    slot_is_logical, slot_is_physical, ReplicationSlotHandle, ReplicationSlotInvalidationCause,
    ReplicationSlotOnDisk, ReplicationSlotPersistency, ReplicationSlotPersistentData,
    SlotInvalidationCauseMap, PG_REPLSLOT_DIR, RS_INVAL_MAX_CAUSES,
};
use ::types_storage::{
    LWLockMode, REPLICATION_SLOT_ALLOCATION_LOCK, REPLICATION_SLOT_CONTROL_LOCK,
    ProcSignalReason,
};
use ::types_tuple::heaptuple::NameData;
use ::condvar::ConditionVariable;

use ::transam::{TransactionIdPrecedes, TransactionIdPrecedesOrEquals};
use ::s_lock::Spinlock;

// Foreign-subsystem seams.
use transam_xlog_seams as xlog;
use slotsync_seams as slotsync;
use walsender_seams as walsender;
use fd_seams as fd;
use procarray_seams as procarray;
use procsignal_seams as procsignal;
use condition_variable_seams as cv;
use lwlock_seams as lwlock;
use pgstat_replslot_seams as pgstat_replslot;
use waitevent_seams as waitevent;
use miscinit_seams as miscinit;
use ::types_pgstat::wait_event;

#[cfg(test)]
mod tests;

/// C source path recorded in `ereport` locations raised by this crate.
const SLOT_C: &str = "../src/backend/replication/slot.c";

#[inline]
fn loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(SLOT_C, lineno, funcname)
}

/// `SLOT_MAGIC` (slot.c) — format identifier for the on-disk state file.
pub const SLOT_MAGIC: u32 = 0x1051CA1;
/// `SLOT_VERSION` (slot.c) — version for new files.
pub const SLOT_VERSION: u32 = 5;

/// `WalLevel` codes (`access/xlog.h`); `wal_level()` returns these as `i32`.
pub const WAL_LEVEL_MINIMAL: i32 = 0;
pub const WAL_LEVEL_REPLICA: i32 = 1;
pub const WAL_LEVEL_LOGICAL: i32 = 2;

/// `SlotInvalidationCauses[]` (slot.c) — lookup table mapping each invalidation
/// cause to its textual name.
pub static SLOT_INVALIDATION_CAUSES: [SlotInvalidationCauseMap; RS_INVAL_MAX_CAUSES + 1] = [
    SlotInvalidationCauseMap {
        cause: ReplicationSlotInvalidationCause::RS_INVAL_NONE,
        cause_name: "none",
    },
    SlotInvalidationCauseMap {
        cause: ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED,
        cause_name: "wal_removed",
    },
    SlotInvalidationCauseMap {
        cause: ReplicationSlotInvalidationCause::RS_INVAL_HORIZON,
        cause_name: "rows_removed",
    },
    SlotInvalidationCauseMap {
        cause: ReplicationSlotInvalidationCause::RS_INVAL_WAL_LEVEL,
        cause_name: "wal_level_insufficient",
    },
    SlotInvalidationCauseMap {
        cause: ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT,
        cause_name: "idle_timeout",
    },
];

/// `StaticAssertDecl(lengthof(SlotInvalidationCauses) == (RS_INVAL_MAX_CAUSES + 1))`.
const _SLOT_INVALIDATION_CAUSES_LEN_OK: () =
    assert!(SLOT_INVALIDATION_CAUSES.len() == RS_INVAL_MAX_CAUSES + 1);

// ---------------------------------------------------------------------------
// `ReplicationSlot` — the shared-memory state of a single replication slot.
//
// The persistent `data` is `ReplicationSlotPersistentData` (types crate). The
// in-shmem-only fields live here alongside the real `Spinlock` mutex. The
// `io_in_progress_lock` is a `LWLock` (types-storage) and `active_cv` is a
// `ConditionVariable` (types-condvar); both are operated via their owner seams.
// ---------------------------------------------------------------------------

/// `ReplicationSlot` (slot.h) — in-memory slot state. `mutex` guards the
/// spinlock-protected fields; `in_use` is flipped under
/// `ReplicationSlotControlLock`.
pub struct ReplicationSlot {
    /// `slock_t mutex` — per-slot spinlock.
    pub mutex: Spinlock,
    /// is this slot defined.
    pub in_use: bool,
    /// Who is streaming out changes for this slot? 0 in unused slots.
    pub active_pid: i32,
    /// any outstanding modifications?
    pub just_dirtied: bool,
    pub dirty: bool,
    /// latest xmin actually written to disk (logical) / persistent value (phys).
    pub effective_xmin: TransactionId,
    pub effective_catalog_xmin: TransactionId,
    /// data surviving shutdowns and crashes.
    pub data: ReplicationSlotPersistentData,
    /// is somebody performing io on this slot?
    pub io_in_progress_lock: ::types_storage::LWLock,
    /// Condition variable signaled when active_pid changes.
    pub active_cv: ConditionVariable,
    /// catalog xmin advance candidate (logical only).
    pub candidate_catalog_xmin: TransactionId,
    pub candidate_xmin_lsn: XLogRecPtr,
    pub candidate_restart_valid: XLogRecPtr,
    pub candidate_restart_lsn: XLogRecPtr,
    /// last confirmed_flush LSN flushed (shutdown-checkpoint decision).
    pub last_saved_confirmed_flush: XLogRecPtr,
    /// the time when the slot became inactive.
    pub inactive_since: TimestampTz,
    /// latest restart_lsn flushed to disk.
    pub last_saved_restart_lsn: XLogRecPtr,
}

impl ReplicationSlot {
    fn new_zeroed() -> Self {
        ReplicationSlot {
            mutex: Spinlock::new(),
            in_use: false,
            active_pid: 0,
            just_dirtied: false,
            dirty: false,
            effective_xmin: 0,
            effective_catalog_xmin: 0,
            data: ReplicationSlotPersistentData::default(),
            io_in_progress_lock: ::types_storage::LWLock::default(),
            active_cv: ConditionVariable::new(),
            candidate_catalog_xmin: 0,
            candidate_xmin_lsn: 0,
            candidate_restart_valid: 0,
            candidate_restart_lsn: 0,
            last_saved_confirmed_flush: 0,
            inactive_since: 0,
            last_saved_restart_lsn: 0,
        }
    }
}

/// One shared slot cell: the slot lives behind interior mutability, exactly as
/// C keeps it in shared memory. Access is disciplined by the named LWLocks and
/// the slot's own `mutex` spinlock, never by Rust's borrow checker.
struct SlotCell {
    inner: UnsafeCell<ReplicationSlot>,
}

// SAFETY: the slot array is genuinely cross-backend shared memory; in this
// thread-per-backend model concurrent access is disciplined by the named
// LWLocks and the per-slot spinlock exactly as in C, not by Rust aliasing.
unsafe impl Sync for SlotCell {}

/// `ReplicationSlotCtlData` — the shared control area (just the slot array).
struct ReplicationSlotCtlData {
    replication_slots: Box<[SlotCell]>,
}

/// `ReplicationSlotCtl` (slot.c) — process-wide (shared-memory) control area.
static SLOT_ARRAY: OnceLock<ReplicationSlotCtlData> = OnceLock::new();

thread_local! {
    /// `ReplicationSlot *MyReplicationSlot` — this backend's acquired slot,
    /// stored as the array index (`None` == NULL).
    static MY_REPLICATION_SLOT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// `int max_replication_slots = 10` (slot.c). This is a process-wide
/// `PGC_POSTMASTER` GUC, so a plain process-global mirrors the C `int`
/// variable; the GUC machinery reads/writes it through the accessors
/// installed in [`init_seams`].
static MAX_REPLICATION_SLOTS: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(10);

fn max_replication_slots() -> i32 {
    MAX_REPLICATION_SLOTS.load(std::sync::atomic::Ordering::Relaxed)
}

fn max_replication_slots_set(v: i32) {
    MAX_REPLICATION_SLOTS.store(v, std::sync::atomic::Ordering::Relaxed);
}

/// `int idle_replication_slot_timeout_secs = 0` (slot.c). Process-wide
/// `PGC_SIGHUP` GUC; reached by the GUC machinery through the accessors
/// installed in [`init_seams`].
static IDLE_REPLICATION_SLOT_TIMEOUT_SECS: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(0);

fn idle_replication_slot_timeout_secs() -> i32 {
    IDLE_REPLICATION_SLOT_TIMEOUT_SECS.load(std::sync::atomic::Ordering::Relaxed)
}

fn idle_replication_slot_timeout_secs_set(v: i32) {
    IDLE_REPLICATION_SLOT_TIMEOUT_SECS.store(v, std::sync::atomic::Ordering::Relaxed);
}

/// `char *synchronized_standby_slots` (slot.c) — the raw GUC string the
/// `synchronized_standby_slots` setting stores. The parsed config lives in
/// `SYNCHRONIZED_STANDBY_SLOTS_CONFIG` (computed by the check/assign hooks);
/// this holds the verbatim string `*conf->variable` for the GUC machinery,
/// reached through the accessors installed in [`init_seams`]. `None` mirrors
/// the C `NULL`.
static SYNCHRONIZED_STANDBY_SLOTS_RAW: std::sync::Mutex<Option<String>> =
    std::sync::Mutex::new(None);

fn synchronized_standby_slots_get() -> Option<String> {
    SYNCHRONIZED_STANDBY_SLOTS_RAW.lock().unwrap().clone()
}

fn synchronized_standby_slots_set(v: Option<String>) {
    *SYNCHRONIZED_STANDBY_SLOTS_RAW.lock().unwrap() = v;
}

fn ctl() -> &'static ReplicationSlotCtlData {
    SLOT_ARRAY
        .get()
        .expect("ReplicationSlotCtl accessed before ReplicationSlotsShmemInit")
}

/// Borrow slot `i` immutably. SAFETY: caller holds the appropriate
/// lock/spinlock per slot.c's locking model.
#[allow(clippy::mut_from_ref)]
unsafe fn slot_ref(i: usize) -> &'static ReplicationSlot {
    &*ctl().replication_slots[i].inner.get()
}

/// Borrow slot `i` mutably. SAFETY: caller holds the appropriate
/// lock/spinlock per slot.c's locking model.
#[allow(clippy::mut_from_ref)]
unsafe fn slot_mut(i: usize) -> &'static mut ReplicationSlot {
    &mut *ctl().replication_slots[i].inner.get()
}

fn my_replication_slot() -> Option<usize> {
    MY_REPLICATION_SLOT.with(std::cell::Cell::get)
}

fn set_my_replication_slot(idx: Option<usize>) {
    MY_REPLICATION_SLOT.with(|c| c.set(idx));
}

// ---------------------------------------------------------------------------
// Named-lock helpers (route through the lwlock main-array seams).
// ---------------------------------------------------------------------------

// slot.c acquires the named ReplicationSlotAllocationLock / ControlLock with
// `LWLockAcquire` and releases them later with an explicit `LWLockRelease`,
// often spanning a whole function body. The seam's `MainLWLockGuard` would
// release the lock the instant it is dropped (RAII), so we must NOT let it drop
// here — `std::mem::forget` the guard and let the matching `unlock_*` helper do
// the explicit `LWLockRelease`, exactly mirroring C's acquire/release pairing.
fn lock_allocation(mode: LWLockMode) -> PgResult<()> {
    let guard = lwlock::lwlock_acquire_main::call(REPLICATION_SLOT_ALLOCATION_LOCK, mode)?;
    std::mem::forget(guard);
    Ok(())
}
fn unlock_allocation() -> PgResult<()> {
    lwlock::lwlock_release_main::call(REPLICATION_SLOT_ALLOCATION_LOCK)
}
fn lock_control(mode: LWLockMode) -> PgResult<()> {
    let guard = lwlock::lwlock_acquire_main::call(REPLICATION_SLOT_CONTROL_LOCK, mode)?;
    std::mem::forget(guard);
    Ok(())
}
fn unlock_control() -> PgResult<()> {
    lwlock::lwlock_release_main::call(REPLICATION_SLOT_CONTROL_LOCK)
}

// ---------------------------------------------------------------------------
// Spinlock helpers — `SpinLockAcquire(&slot->mutex)` / `SpinLockRelease`.
// ---------------------------------------------------------------------------

fn spin_acquire(slot: &ReplicationSlot) {
    ::s_lock::s_lock_macro(&slot.mutex, Some(SLOT_C), 0, None);
}
fn spin_release(slot: &ReplicationSlot) {
    ::s_lock::s_unlock(&slot.mutex);
}

// ---------------------------------------------------------------------------
// `slot.h` predicates + `ReplicationSlotSetInactiveSince`.
// ---------------------------------------------------------------------------

/// `SlotIsPhysical(slot)`.
#[inline]
pub fn SlotIsPhysical(slot: &ReplicationSlot) -> bool {
    slot_is_physical(&slot.data)
}

/// `SlotIsLogical(slot)`.
#[inline]
pub fn SlotIsLogical(slot: &ReplicationSlot) -> bool {
    slot_is_logical(&slot.data)
}

/// `static inline void ReplicationSlotSetInactiveSince(ReplicationSlot *s,
/// TimestampTz ts, bool acquire_lock)` (slot.h).
#[inline]
pub fn ReplicationSlotSetInactiveSince(s: &mut ReplicationSlot, ts: TimestampTz, acquire_lock: bool) {
    if acquire_lock {
        spin_acquire(s);
    }
    if s.data.invalidated == ReplicationSlotInvalidationCause::RS_INVAL_NONE {
        s.inactive_since = ts;
    }
    if acquire_lock {
        spin_release(s);
    }
}

// ---------------------------------------------------------------------------
// slotfuncs.c support: locked slot-array snapshots + `MyReplicationSlot`
// field access used by `slotfuncs.c`. The control lock + per-slot spinlock
// substrate is owned here (slot.c); slotfuncs.c only orchestrates over these.
// ---------------------------------------------------------------------------

/// A spinlocked snapshot of a `ReplicationSlot` (`slot_contents = *slot`),
/// paired with the slot's array index. Mirrors the `ReplicationSlot
/// slot_contents` stack copy that slotfuncs.c takes under the per-slot mutex.
#[derive(Clone)]
pub struct SlotSnapshot {
    /// The slot's index in `ReplicationSlotCtl->replication_slots[]` (the C loop
    /// variable, used for the `WALAVAIL_REMOVED` re-read in
    /// `pg_get_replication_slots`).
    pub slotno: usize,
    pub active_pid: i32,
    pub effective_xmin: TransactionId,
    pub effective_catalog_xmin: TransactionId,
    pub inactive_since: TimestampTz,
    pub data: ReplicationSlotPersistentData,
}

impl SlotSnapshot {
    fn capture(i: usize) -> SlotSnapshot {
        // SAFETY: caller holds ReplicationSlotControlLock (shared).
        let slot = unsafe { slot_ref(i) };
        spin_acquire(slot);
        let snap = SlotSnapshot {
            slotno: i,
            active_pid: slot.active_pid,
            effective_xmin: slot.effective_xmin,
            effective_catalog_xmin: slot.effective_catalog_xmin,
            inactive_since: slot.inactive_since,
            data: slot.data.clone(),
        };
        spin_release(slot);
        snap
    }
}

/// `SlotIsPhysical`/`SlotIsLogical` over a snapshot's persistent data.
pub fn snapshot_is_physical(snap: &SlotSnapshot) -> bool {
    slot_is_physical(&snap.data)
}
pub fn snapshot_is_logical(snap: &SlotSnapshot) -> bool {
    slot_is_logical(&snap.data)
}

/// The `LWLockAcquire(ReplicationSlotControlLock, LW_SHARED)` walk over the slot
/// array that `pg_get_replication_slots` performs: take a spinlocked snapshot of
/// every in-use slot (`if (!slot->in_use) continue;`), in array order, then
/// release the control lock. Returns the snapshots; the array index is carried
/// on each so callers can re-read a single slot afterwards.
pub fn snapshot_all_slots() -> PgResult<Vec<SlotSnapshot>> {
    lock_control(LWLockMode::LW_SHARED)?;
    let mut out = Vec::new();
    for slotno in 0..max_replication_slots() as usize {
        // SAFETY: we hold ReplicationSlotControlLock (shared).
        let slot = unsafe { slot_ref(slotno) };
        if !slot.in_use {
            continue;
        }
        out.push(SlotSnapshot::capture(slotno));
    }
    unlock_control()?;
    Ok(out)
}

/// The `WALAVAIL_REMOVED` re-read in `pg_get_replication_slots`: under the
/// per-slot spinlock, read `(slot->active_pid, slot->data.restart_lsn)` for the
/// slot at array index `slotno`. The caller no longer holds the control lock,
/// matching the C which re-locks only the per-slot mutex.
pub fn reread_slot_active_pid_and_restart_lsn(slotno: usize) -> (i32, XLogRecPtr) {
    // SAFETY: the per-slot spinlock disciplines this read; the slot is in_use
    // (it was when snapshotted, and slots are never freed concurrently here).
    let slot = unsafe { slot_ref(slotno) };
    spin_acquire(slot);
    let pid = slot.active_pid;
    let restart_lsn = slot.data.restart_lsn;
    spin_release(slot);
    (pid, restart_lsn)
}

/// `copy_replication_slot`'s source-slot search: under
/// `ReplicationSlotControlLock` (shared), find the in-use slot named `src_name`
/// and take a spinlocked snapshot of it, then release the control lock. `None`
/// mirrors the C `src == NULL` (no such slot).
pub fn snapshot_slot_by_name(src_name: &str) -> PgResult<Option<SlotSnapshot>> {
    lock_control(LWLockMode::LW_SHARED)?;
    let mut found = None;
    for slotno in 0..max_replication_slots() as usize {
        // SAFETY: we hold ReplicationSlotControlLock (shared).
        let slot = unsafe { slot_ref(slotno) };
        if slot.in_use && name_str_string(&slot.data.name) == src_name {
            found = Some(SlotSnapshot::capture(slotno));
            break;
        }
    }
    unlock_control()?;
    Ok(found)
}

/// `copy_replication_slot`'s second spinlocked read of the source slot
/// (`SpinLockAcquire(&src->mutex); second_slot_contents = *src; ...`), by the
/// array index recorded in the first snapshot.
pub fn reread_slot_snapshot(slotno: usize) -> SlotSnapshot {
    SlotSnapshot::capture(slotno)
}

/// The copied source-slot values installed into `MyReplicationSlot` by
/// `copy_replication_slot` under the destination slot's spinlock.
pub struct CopiedSlotValues {
    pub effective_xmin: TransactionId,
    pub effective_catalog_xmin: TransactionId,
    pub xmin: TransactionId,
    pub catalog_xmin: TransactionId,
    pub restart_lsn: XLogRecPtr,
    pub confirmed_flush: XLogRecPtr,
}

/// `copy_replication_slot`'s install step:
/// ```c
/// SpinLockAcquire(&MyReplicationSlot->mutex);
/// MyReplicationSlot->effective_xmin = ...;
/// ...
/// SpinLockRelease(&MyReplicationSlot->mutex);
/// ```
pub fn install_my_slot_copied_values(v: &CopiedSlotValues) {
    let slot = my_slot_mut();
    spin_acquire(my_slot_ref());
    slot.effective_xmin = v.effective_xmin;
    slot.effective_catalog_xmin = v.effective_catalog_xmin;
    slot.data.xmin = v.xmin;
    slot.data.catalog_xmin = v.catalog_xmin;
    slot.data.restart_lsn = v.restart_lsn;
    slot.data.confirmed_flush = v.confirmed_flush;
    spin_release(my_slot_ref());
}

/// `MyReplicationSlot != NULL` (`Assert(!MyReplicationSlot)` / the
/// `OidIsValid(MyReplicationSlot->data.database)` type test sites).
pub fn my_replication_slot_is_set() -> bool {
    my_replication_slot().is_some()
}

/// `MyReplicationSlot->data.name` as a `NameData` (for `NameGetDatum`).
pub fn my_slot_name() -> NameData {
    my_slot_ref().data.name.clone()
}
/// `MyReplicationSlot->data.database` (`OidIsValid` type discriminator).
pub fn my_slot_database() -> Oid {
    my_slot_ref().data.database
}
/// `MyReplicationSlot->data.restart_lsn`.
pub fn my_slot_restart_lsn() -> XLogRecPtr {
    my_slot_ref().data.restart_lsn
}
/// `MyReplicationSlot->data.confirmed_flush`.
pub fn my_slot_confirmed_flush() -> XLogRecPtr {
    my_slot_ref().data.confirmed_flush
}

/// `create_physical_replication_slot`'s
/// `MyReplicationSlot->data.restart_lsn = restart_lsn;` (no lock; called right
/// after create, before the slot is published).
pub fn set_my_slot_restart_lsn(lsn: XLogRecPtr) {
    my_slot_mut().data.restart_lsn = lsn;
}

/// `pg_physical_replication_slot_advance`'s spinlocked
/// `MyReplicationSlot->data.restart_lsn = moveto;`.
pub fn set_my_slot_restart_lsn_locked(lsn: XLogRecPtr) {
    let slot = my_slot_mut();
    spin_acquire(my_slot_ref());
    slot.data.restart_lsn = lsn;
    spin_release(my_slot_ref());
}

// ---------------------------------------------------------------------------
// On-disk format sizing arithmetic.
// ---------------------------------------------------------------------------

/// `offsetof(ReplicationSlotOnDisk, version)` — `magic` (u32) + `checksum` (u32).
const fn offset_of_version() -> Size {
    core::mem::size_of::<u32>() + core::mem::size_of::<u32>()
}

/// `offsetof(ReplicationSlotOnDisk, slotdata)`.
const fn offset_of_slotdata() -> Size {
    let after_header = core::mem::size_of::<u32>() * 4;
    let align = core::mem::align_of::<ReplicationSlotPersistentData>();
    after_header.div_ceil(align) * align
}

/// `ReplicationSlotOnDiskConstantSize` — size of version-independent leading part.
pub const fn ReplicationSlotOnDiskConstantSize() -> Size {
    offset_of_slotdata()
}
/// `ReplicationSlotOnDiskNotChecksummedSize` — part not covered by the checksum.
pub const fn ReplicationSlotOnDiskNotChecksummedSize() -> Size {
    offset_of_version()
}
/// `ReplicationSlotOnDiskChecksummedSize` — part covered by the checksum.
pub const fn ReplicationSlotOnDiskChecksummedSize() -> Size {
    core::mem::size_of::<ReplicationSlotOnDisk>() - ReplicationSlotOnDiskNotChecksummedSize()
}
/// `ReplicationSlotOnDiskV2Size` — size of the version-dependent slot data.
pub const fn ReplicationSlotOnDiskV2Size() -> Size {
    core::mem::size_of::<ReplicationSlotOnDisk>() - ReplicationSlotOnDiskConstantSize()
}

/// `add_size(s1, s2)` (shmem.c).
#[inline]
fn add_size(s1: Size, s2: Size) -> Size {
    s1.checked_add(s2).expect("requested shared memory size overflows size_t")
}
/// `mul_size(s1, s2)` (shmem.c).
#[inline]
fn mul_size(s1: Size, s2: Size) -> Size {
    s1.checked_mul(s2).expect("requested shared memory size overflows size_t")
}

/// `Size ReplicationSlotsShmemSize(void)` (slot.c:186).
pub fn ReplicationSlotsShmemSize() -> Size {
    if max_replication_slots() == 0 {
        return 0;
    }
    // offsetof(ReplicationSlotCtlData, replication_slots) == 0 here.
    add_size(0, mul_size(max_replication_slots() as Size, size_of_replication_slot()))
}

/// `sizeof(ReplicationSlot)` — the in-shmem slot struct size.
fn size_of_replication_slot() -> Size {
    core::mem::size_of::<ReplicationSlot>()
}

/// `void ReplicationSlotsShmemInit(void)` (slot.c:204).
pub fn ReplicationSlotsShmemInit() {
    if max_replication_slots() == 0 {
        return;
    }
    // ShmemInitStruct + first-time zero-init: build the slot array once. The
    // per-slot SpinLockInit / LWLockInitialize / ConditionVariableInit happen
    // in `new_zeroed` (the lock primitives start free).
    let _ = SLOT_ARRAY.get_or_init(|| {
        let n = max_replication_slots() as usize;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let mut slot = ReplicationSlot::new_zeroed();
            lwlock::lwlock_initialize::call(
                &mut slot.io_in_progress_lock,
                ::types_storage::LWTRANCHE_REPLICATION_SLOT_IO,
            );
            v.push(SlotCell {
                inner: UnsafeCell::new(slot),
            });
        }
        ReplicationSlotCtlData {
            replication_slots: v.into_boxed_slice(),
        }
    });
}

/// `void ReplicationSlotInitialize(void)` (slot.c:239).
pub fn ReplicationSlotInitialize() -> PgResult<()> {
    // The `before_shmem_exit` seam carries its opaque callback token as the
    // canonical unified `::types_tuple::Datum<'static>` (the machine word the C
    // `Datum arg` carries, pinned to `'static` and stored by value in the
    // registration list). This crate's own logic constructs/reads no scalars,
    // so there is nothing else to migrate; the token here is `(Datum) 0` in C,
    // i.e. `Datum::null()`.
    dsm_core_seams::before_shmem_exit::call(
        replication_slot_shmem_exit_cb,
        ::types_tuple::Datum::null(),
    )
}

/// `static void ReplicationSlotShmemExit(int code, Datum arg)` (slot.c:248).
fn replication_slot_shmem_exit_cb(_code: i32, _arg: ::types_tuple::Datum<'static>) -> PgResult<()> {
    if my_replication_slot().is_some() {
        ReplicationSlotRelease()?;
    }
    ReplicationSlotCleanup(false)
}

// ---------------------------------------------------------------------------
// Name validation.
// ---------------------------------------------------------------------------

/// `bool ReplicationSlotValidateName(const char *name, int elevel)` (slot.c:264).
pub fn ReplicationSlotValidateName(name: &str, elevel: ErrorLevel) -> PgResult<bool> {
    match ReplicationSlotValidateNameInternal(name) {
        Ok(()) => Ok(true),
        Err((err_code, err_msg, err_hint)) => {
            // errmsg_internal / errhint_internal: the messages are already
            // translated, avoiding double translation.
            let mut builder = ereport(elevel).errcode(err_code).errmsg_internal(err_msg);
            if let Some(hint) = err_hint {
                builder = builder.errhint_internal(hint);
            }
            builder.finish(loc(279, "ReplicationSlotValidateName"))?;
            Ok(false)
        }
    }
}

/// `bool ReplicationSlotValidateNameInternal(const char *name, int *err_code,
/// char **err_msg, char **err_hint)` (slot.c:305).
pub fn ReplicationSlotValidateNameInternal(
    name: &str,
) -> Result<(), (SqlState, String, Option<String>)> {
    if name.is_empty() {
        return Err((
            ERRCODE_INVALID_NAME,
            format!("replication slot name \"{name}\" is too short"),
            None,
        ));
    }
    if name.len() >= NAMEDATALEN as usize {
        return Err((
            ERRCODE_NAME_TOO_LONG,
            format!("replication slot name \"{name}\" is too long"),
            None,
        ));
    }
    for &cp in name.as_bytes() {
        if !(cp.is_ascii_lowercase() || cp.is_ascii_digit() || cp == b'_') {
            return Err((
                ERRCODE_INVALID_NAME,
                format!("replication slot name \"{name}\" contains invalid character"),
                Some(
                    "Replication slot names may only contain lower case letters, numbers, and the underscore character."
                        .to_string(),
                ),
            ));
        }
    }
    Ok(())
}

/// Copy a `&str` into a `NameData` (`namestrcpy`).
fn namestrcpy(name: &str) -> NameData {
    let mut nd = NameData {
        data: [0u8; NAMEDATALEN as usize],
    };
    let bytes = name.as_bytes();
    let n = bytes.len().min(NAMEDATALEN as usize - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// `NameStr(name)` as a Rust `&str` (lossy if non-UTF-8, like a C identifier).
fn name_str_string(nd: &NameData) -> String {
    String::from_utf8_lossy(nd.name_str()).into_owned()
}

// ---------------------------------------------------------------------------
// Slot creation.
// ---------------------------------------------------------------------------

/// `void ReplicationSlotCreate(const char *name, bool db_specific,
/// ReplicationSlotPersistency persistency, bool two_phase, bool failover,
/// bool synced)` (slot.c:353). `my_database_id` is the caller's `MyDatabaseId`.
pub fn ReplicationSlotCreate(
    name: &str,
    db_specific: bool,
    persistency: ReplicationSlotPersistency,
    two_phase: bool,
    failover: bool,
    synced: bool,
    my_database_id: Oid,
) -> PgResult<()> {
    assert!(my_replication_slot().is_none());

    ReplicationSlotValidateName(name, ERROR)?;

    if failover {
        // Do not allow failover slots on the standby (except during sync).
        if xlog::recovery_in_progress::call() && !slotsync::is_syncing_replication_slots::call() {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a replication slot created on the standby")
                .finish(loc(375, "ReplicationSlotCreate"));
        }
        // Do not allow failover enabled temporary slots (except during sync).
        if persistency == ReplicationSlotPersistency::RS_TEMPORARY
            && !slotsync::is_syncing_replication_slots::call()
        {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a temporary replication slot")
                .finish(loc(387, "ReplicationSlotCreate"));
        }
    }

    // Serialize allocation against concurrent creators/droppers.
    lock_allocation(LWLockMode::LW_EXCLUSIVE)?;

    // Find an allocatable slot and check for a name collision.
    let mut slot: Option<usize> = None;
    if let Err(e) = (|| -> PgResult<()> {
        lock_control(LWLockMode::LW_SHARED)?;
        for i in 0..max_replication_slots() as usize {
            // SAFETY: ReplicationSlotControlLock held shared.
            let s = unsafe { slot_ref(i) };
            if s.in_use && name_str_string(&s.data.name) == name {
                unlock_control()?;
                return ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!("replication slot \"{name}\" already exists"))
                    .finish(loc(412, "ReplicationSlotCreate"));
            }
            if !s.in_use && slot.is_none() {
                slot = Some(i);
            }
        }
        unlock_control()?;
        Ok(())
    })() {
        // On error inside the scan we must drop the allocation lock.
        let _ = unlock_allocation();
        return Err(e);
    }

    // If all slots are in use, we're out of luck.
    let slot = match slot {
        Some(i) => i,
        None => {
            let _ = unlock_allocation();
            return ereport(ERROR)
                .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg("all replication slots are in use")
                .errhint("Free one or increase \"max_replication_slots\".")
                .finish(loc(422, "ReplicationSlotCreate"));
        }
    };

    // Safe to initialize the slot: we hold the allocation lock and the slot is
    // not in use.
    // SAFETY: allocation lock held; slot not in use, so nobody else touches it.
    let s = unsafe { slot_mut(slot) };
    debug_assert!(!s.in_use);
    debug_assert!(s.active_pid == 0);

    // First initialize persistent data.
    s.data = ReplicationSlotPersistentData::default();
    s.data.name = namestrcpy(name);
    s.data.database = if db_specific { my_database_id } else { InvalidOid };
    s.data.persistency = persistency;
    s.data.two_phase = two_phase;
    s.data.two_phase_at = 0;
    s.data.failover = failover;
    s.data.synced = synced as i8;

    // Data only present in shared memory.
    s.just_dirtied = false;
    s.dirty = false;
    s.effective_xmin = 0;
    s.effective_catalog_xmin = 0;
    s.candidate_catalog_xmin = 0;
    s.candidate_xmin_lsn = 0;
    s.candidate_restart_valid = 0;
    s.candidate_restart_lsn = 0;
    s.last_saved_confirmed_flush = 0;
    s.last_saved_restart_lsn = 0;
    s.inactive_since = 0;

    // Create the slot on disk. No special cleanup needed on error (not yet
    // marked allocated).
    if let Err(e) = CreateSlotOnDisk(slot) {
        let _ = unlock_allocation();
        return Err(e);
    }

    // Flip in_use under ControlLock exclusive; set active_pid under the mutex.
    lock_control(LWLockMode::LW_EXCLUSIVE)?;
    // SAFETY: ControlLock exclusive.
    let s = unsafe { slot_mut(slot) };
    s.in_use = true;
    spin_acquire(s);
    debug_assert!(s.active_pid == 0);
    s.active_pid = miscinit_my_proc_pid();
    spin_release(s);
    set_my_replication_slot(Some(slot));
    unlock_control()?;

    // Create stats entry for the new logical slot.
    // SAFETY: read-only of stable fields; we own the slot.
    let s = unsafe { slot_ref(slot) };
    if SlotIsLogical(s) {
        pgstat_replslot::pgstat_create_replslot::call(slot as i32, s.data.name);
    }

    // Let others allocate again.
    unlock_allocation()?;

    // Let everybody know we've modified this slot.
    cv::condition_variable_broadcast::call(&unsafe { slot_ref(slot) }.active_cv);

    Ok(())
}

fn miscinit_my_proc_pid() -> i32 {
    init_small_seams::my_proc_pid::call()
}

// ---------------------------------------------------------------------------
// Search / index / name.
// ---------------------------------------------------------------------------

/// `ReplicationSlot *SearchNamedReplicationSlot(const char *name, bool need_lock)`
/// (slot.c:509) — returns the slot index if found.
pub fn SearchNamedReplicationSlot(name: &str, need_lock: bool) -> PgResult<Option<usize>> {
    if need_lock {
        lock_control(LWLockMode::LW_SHARED)?;
    }
    let mut found = None;
    for i in 0..max_replication_slots() as usize {
        // SAFETY: ControlLock held (by us or the caller).
        let s = unsafe { slot_ref(i) };
        if s.in_use && name_str_string(&s.data.name) == name {
            found = Some(i);
            break;
        }
    }
    if need_lock {
        unlock_control()?;
    }
    Ok(found)
}

/// `int ReplicationSlotIndex(ReplicationSlot *slot)` (slot.c:542).
pub fn ReplicationSlotIndex(slot: usize) -> i32 {
    debug_assert!(slot < max_replication_slots() as usize);
    slot as i32
}

/// `bool ReplicationSlotName(int index, Name name)` (slot.c:558).
pub fn ReplicationSlotName(index: i32) -> PgResult<(bool, NameData)> {
    let i = index as usize;
    lock_control(LWLockMode::LW_SHARED)?;
    // SAFETY: ControlLock held shared.
    let s = unsafe { slot_ref(i) };
    let found = s.in_use;
    let mut name = NameData::default();
    if s.in_use {
        name = s.data.name;
    }
    unlock_control()?;
    Ok((found, name))
}

fn get_current_timestamp() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}

// ---------------------------------------------------------------------------
// Acquire / Release / Cleanup.
// ---------------------------------------------------------------------------

/// `void ReplicationSlotAcquire(const char *name, bool nowait,
/// bool error_if_invalid)` (slot.c:589).
pub fn ReplicationSlotAcquire(name: &str, nowait: bool, error_if_invalid: bool) -> PgResult<()> {
    // 'retry:' loop.
    'retry: loop {
        assert!(my_replication_slot().is_none());

        lock_control(LWLockMode::LW_SHARED)?;

        // Check if the slot exists with the given name.
        let s = SearchNamedReplicationSlot(name, false)?;
        let s = match s {
            // SAFETY: ControlLock held shared.
            Some(i) if unsafe { slot_ref(i) }.in_use => i,
            _ => {
                unlock_control()?;
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("replication slot \"{name}\" does not exist"))
                    .finish(loc(607, "ReplicationSlotAcquire"));
            }
        };

        let active_pid;
        let my_pid = miscinit_my_proc_pid();

        if init_small_seams::is_under_postmaster::call() {
            // Get ready to sleep on the slot in case it is active.
            if !nowait {
                // SAFETY: ControlLock held shared; CV lives in the slot.
                cv::condition_variable_prepare_to_sleep::call(&unsafe { slot_ref(s) }.active_cv);
            }

            // Reset inactive_since under spinlock (race with invalidation).
            // SAFETY: ControlLock held shared; we take the per-slot mutex.
            let slot = unsafe { slot_mut(s) };
            spin_acquire(slot);
            if slot.active_pid == 0 {
                slot.active_pid = my_pid;
            }
            active_pid = slot.active_pid;
            ReplicationSlotSetInactiveSince(slot, 0, false);
            spin_release(slot);
        } else {
            // SAFETY: single-user mode, ControlLock held shared.
            let slot = unsafe { slot_mut(s) };
            slot.active_pid = my_pid;
            active_pid = my_pid;
            ReplicationSlotSetInactiveSince(slot, 0, true);
        }
        unlock_control()?;

        // If the slot is active in another process, wait or error out.
        if active_pid != my_pid {
            if !nowait {
                // SAFETY: CV lives in the slot (stable across array lifetime).
                cv::condition_variable_sleep::call(
                    &unsafe { slot_ref(s) }.active_cv,
                    wait_event::WAIT_EVENT_REPLICATION_SLOT_DROP,
                )?;
                cv::condition_variable_cancel_sleep::call();
                continue 'retry;
            }
            // SAFETY: read of stable name.
            let nm = name_str_string(&unsafe { slot_ref(s) }.data.name);
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "replication slot \"{nm}\" is active for PID {active_pid}"
                ))
                .finish(loc(662, "ReplicationSlotAcquire"));
        } else if !nowait {
            cv::condition_variable_cancel_sleep::call(); // no sleep needed after all
        }

        // We made this slot active, so it's ours now.
        set_my_replication_slot(Some(s));

        // Check for invalidation after making the slot ours.
        // SAFETY: we own the slot.
        let slot = unsafe { slot_ref(s) };
        if error_if_invalid
            && slot.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE
        {
            let nm = name_str_string(&slot.data.name);
            let cause = GetSlotInvalidationCauseName(slot.data.invalidated);
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("can no longer access replication slot \"{nm}\""))
                .errdetail(format!(
                    "This replication slot has been invalidated due to \"{cause}\"."
                ))
                .finish(loc(679, "ReplicationSlotAcquire"));
        }

        // Let everybody know we've modified this slot.
        cv::condition_variable_broadcast::call(&slot.active_cv);

        // Protect against stale stats for a different slot.
        if SlotIsLogical(slot) {
            pgstat_replslot::pgstat_acquire_replslot::call(s as i32);
        }

        if walsender::am_walsender::call() {
            let level = if walsender::log_replication_commands::call() {
                LOG
            } else {
                DEBUG1
            };
            let nm = name_str_string(&slot.data.name);
            let msg = if SlotIsLogical(slot) {
                format!("acquired logical replication slot \"{nm}\"")
            } else {
                format!("acquired physical replication slot \"{nm}\"")
            };
            ereport(level).errmsg(msg).finish(loc(700, "ReplicationSlotAcquire"))?;
        }

        return Ok(());
    }
}

/// `void ReplicationSlotRelease(void)` (slot.c:716).
pub fn ReplicationSlotRelease() -> PgResult<()> {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot.
    let s = unsafe { slot_ref(slot) };
    assert!(s.active_pid != 0);

    let mut slotname = String::new();
    let mut is_logical = false;
    if walsender::am_walsender::call() {
        slotname = name_str_string(&s.data.name);
        is_logical = SlotIsLogical(s);
    }

    if s.data.persistency == ReplicationSlotPersistency::RS_EPHEMERAL {
        // Delete the slot. No !PANIC failure is allowed here.
        ReplicationSlotDropAcquired()?;
    }

    // If we temporarily restrained data+catalog xmin for the catalog snapshot,
    // remove that constraint.
    // SAFETY: we own the slot.
    let s = unsafe { slot_ref(slot) };
    if !TransactionIdIsValid(s.data.xmin) && TransactionIdIsValid(s.effective_xmin) {
        // SAFETY: we own the slot; take its mutex.
        let s = unsafe { slot_mut(slot) };
        spin_acquire(s);
        s.effective_xmin = 0;
        spin_release(s);
        ReplicationSlotsComputeRequiredXmin(false)?;
    }

    // Set inactive_since; get the time before taking the spinlock.
    let now = get_current_timestamp();

    // SAFETY: we own the slot.
    let persistency = unsafe { slot_ref(slot) }.data.persistency;
    if persistency == ReplicationSlotPersistency::RS_PERSISTENT {
        // Mark persistent slot inactive; wake waiters.
        // SAFETY: we own the slot.
        let s = unsafe { slot_mut(slot) };
        spin_acquire(s);
        s.active_pid = 0;
        ReplicationSlotSetInactiveSince(s, now, false);
        spin_release(s);
        cv::condition_variable_broadcast::call(&unsafe { slot_ref(slot) }.active_cv);
    } else if my_replication_slot().is_some() {
        // (still acquired: ephemeral path above may have dropped it)
        // SAFETY: we own the slot.
        let s = unsafe { slot_mut(slot) };
        ReplicationSlotSetInactiveSince(s, now, true);
    }

    set_my_replication_slot(None);

    // Clear PROC_IN_LOGICAL_DECODING under ProcArrayLock.
    procarray::proc_array_clear_logical_decoding_flag::call();

    if walsender::am_walsender::call() {
        let level = if walsender::log_replication_commands::call() {
            LOG
        } else {
            DEBUG1
        };
        let msg = if is_logical {
            format!("released logical replication slot \"{slotname}\"")
        } else {
            format!("released physical replication slot \"{slotname}\"")
        };
        ereport(level).errmsg(msg).finish(loc(787, "ReplicationSlotRelease"))?;
    }

    Ok(())
}

fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != 0
}

/// `void ReplicationSlotCleanup(bool synced_only)` (slot.c:805).
pub fn ReplicationSlotCleanup(synced_only: bool) -> PgResult<()> {
    assert!(my_replication_slot().is_none());
    let my_pid = miscinit_my_proc_pid();

    'restart: loop {
        lock_control(LWLockMode::LW_SHARED)?;
        for i in 0..max_replication_slots() as usize {
            // SAFETY: ControlLock held shared.
            let s = unsafe { slot_ref(i) };
            if !s.in_use {
                continue;
            }
            // SAFETY: take the per-slot mutex.
            let sm = unsafe { slot_mut(i) };
            spin_acquire(sm);
            if sm.active_pid == my_pid && (!synced_only || sm.data.synced != 0) {
                debug_assert!(sm.data.persistency == ReplicationSlotPersistency::RS_TEMPORARY);
                spin_release(sm);
                unlock_control()?; // avoid deadlock

                ReplicationSlotDropPtr(i)?;

                cv::condition_variable_broadcast::call(&unsafe { slot_ref(i) }.active_cv);
                continue 'restart;
            } else {
                spin_release(sm);
            }
        }
        unlock_control()?;
        return Ok(());
    }
}

/// `void ReplicationSlotDrop(const char *name, bool nowait)` (slot.c:844).
pub fn ReplicationSlotDrop(name: &str, nowait: bool) -> PgResult<()> {
    assert!(my_replication_slot().is_none());

    ReplicationSlotAcquire(name, nowait, false)?;

    // Do not allow dropping slots being synced from the primary.
    let slot = my_replication_slot().expect("acquired");
    // SAFETY: we own the slot.
    if xlog::recovery_in_progress::call() && unsafe { slot_ref(slot) }.data.synced != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot drop replication slot \"{name}\""))
            .errdetail("This replication slot is being synchronized from the primary server.")
            .finish(loc(855, "ReplicationSlotDrop"));
    }

    ReplicationSlotDropAcquired()
}

/// `void ReplicationSlotAlter(const char *name, const bool *failover,
/// const bool *two_phase)` (slot.c:877).
pub fn ReplicationSlotAlter(
    name: &str,
    failover: Option<bool>,
    two_phase: Option<bool>,
) -> PgResult<()> {
    let mut update_slot = false;

    assert!(my_replication_slot().is_none());
    assert!(failover.is_some() || two_phase.is_some());

    ReplicationSlotAcquire(name, false, true)?;
    let slot = my_replication_slot().expect("acquired");

    // SAFETY: we own the slot.
    if SlotIsPhysical(unsafe { slot_ref(slot) }) {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot use ALTER_REPLICATION_SLOT with a physical replication slot")
            .finish(loc(888, "ReplicationSlotAlter"));
    }

    if xlog::recovery_in_progress::call() {
        // SAFETY: we own the slot.
        if unsafe { slot_ref(slot) }.data.synced != 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("cannot alter replication slot \"{name}\""))
                .errdetail(
                    "This replication slot is being synchronized from the primary server.",
                )
                .finish(loc(900, "ReplicationSlotAlter"));
        }
        // Do not allow enabling failover on the standby.
        if failover == Some(true) {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a replication slot on the standby")
                .finish(loc(910, "ReplicationSlotAlter"));
        }
    }

    if let Some(failover) = failover {
        // Do not allow enabling failover for temporary slots.
        // SAFETY: we own the slot.
        if failover && unsafe { slot_ref(slot) }.data.persistency == ReplicationSlotPersistency::RS_TEMPORARY {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a temporary replication slot")
                .finish(loc(923, "ReplicationSlotAlter"));
        }
        // SAFETY: we own the slot.
        if unsafe { slot_ref(slot) }.data.failover != failover {
            let s = unsafe { slot_mut(slot) };
            spin_acquire(s);
            s.data.failover = failover;
            spin_release(s);
            update_slot = true;
        }
    }

    if let Some(two_phase) = two_phase {
        // SAFETY: we own the slot.
        if unsafe { slot_ref(slot) }.data.two_phase != two_phase {
            let s = unsafe { slot_mut(slot) };
            spin_acquire(s);
            s.data.two_phase = two_phase;
            spin_release(s);
            update_slot = true;
        }
    }

    if update_slot {
        ReplicationSlotMarkDirty();
        ReplicationSlotSave()?;
    }

    ReplicationSlotRelease()
}

/// `void ReplicationSlotDropAcquired(void)` (slot.c:959).
pub fn ReplicationSlotDropAcquired() -> PgResult<()> {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // slot isn't acquired anymore.
    set_my_replication_slot(None);
    ReplicationSlotDropPtr(slot)
}

/// Build the `%m` portion of a file-access error message from an errno.
fn errno_str(errno: i32) -> String {
    // strerror(errno) — mirrors C's `%m`.
    let s = unsafe { libc::strerror(errno) };
    if s.is_null() {
        return format!("errno {errno}");
    }
    unsafe { std::ffi::CStr::from_ptr(s) }
        .to_string_lossy()
        .into_owned()
}

/// Raise a file-access error at `elevel` with `%m` resolved from `errno`.
fn file_access_error(
    elevel: ErrorLevel,
    errno: i32,
    message: String,
    location: ErrorLocation,
) -> PgResult<()> {
    ereport(elevel)
        .errcode_for_file_access()
        .errmsg(format!("{message}: {}", errno_str(errno)))
        .finish(location)
}

// ---------------------------------------------------------------------------
// Drop the replication slot pointed at by `slot` (index).
// ---------------------------------------------------------------------------

/// `static void ReplicationSlotDropPtr(ReplicationSlot *slot)` (slot.c:976).
fn ReplicationSlotDropPtr(slot: usize) -> PgResult<()> {
    // Serialize against concurrent create/drop of a same-named slot.
    lock_allocation(LWLockMode::LW_EXCLUSIVE)?;

    // SAFETY: allocation lock held; the name cannot change.
    let name = name_str_string(&unsafe { slot_ref(slot) }.data.name);
    let path = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmppath = format!("{PG_REPLSLOT_DIR}/{name}.tmp");

    // Rename the slot directory so it's no longer a valid slot.
    if fd::rename_file::call(&path, &tmppath) == 0 {
        // fsync the renamed dir and its parent; panic on failure (recovery
        // straightens it out). START/END_CRIT_SECTION: handled by fsync_fname.
        fd::fsync_fname::call(&tmppath, true)?;
        fd::fsync_fname::call(PG_REPLSLOT_DIR, true)?;
    } else {
        let errno = fd::last_errno::call();
        // SAFETY: allocation lock held.
        let persistency = unsafe { slot_ref(slot) }.data.persistency;
        let fail_softly = persistency != ReplicationSlotPersistency::RS_PERSISTENT;

        // SAFETY: take the per-slot mutex.
        let s = unsafe { slot_mut(slot) };
        spin_acquire(s);
        s.active_pid = 0;
        spin_release(s);

        // Wake up anyone waiting on this slot.
        cv::condition_variable_broadcast::call(&unsafe { slot_ref(slot) }.active_cv);

        let elevel = if fail_softly { WARNING } else { ERROR };
        file_access_error(
            elevel,
            errno,
            format!("could not rename file \"{path}\" to \"{tmppath}\""),
            loc(1025, "ReplicationSlotDropPtr"),
        )?;
        // WARNING path falls through (does not return) in C.
    }

    // The slot is definitely gone: lock out scans long enough to clear it.
    lock_control(LWLockMode::LW_EXCLUSIVE)?;
    // SAFETY: ControlLock exclusive.
    let s = unsafe { slot_mut(slot) };
    s.active_pid = 0;
    s.in_use = false;
    unlock_control()?;
    cv::condition_variable_broadcast::call(&unsafe { slot_ref(slot) }.active_cv);

    // Slot no longer prevents resource removal; recompute limits.
    ReplicationSlotsComputeRequiredXmin(false)?;
    ReplicationSlotsComputeRequiredLSN()?;

    // Remove the renamed directory; warn (only) on failure.
    if !fd::rmtree::call(&tmppath, true) {
        ereport(WARNING)
            .errmsg(format!("could not remove directory \"{tmppath}\""))
            .finish(loc(1059, "ReplicationSlotDropPtr"))?;
    }

    // Drop the stats entry while holding the allocation lock.
    // SAFETY: allocation lock held.
    let s = unsafe { slot_ref(slot) };
    if SlotIsLogical(s) {
        pgstat_replslot::pgstat_drop_replslot::call(slot as i32);
    }

    unlock_allocation()
}

// ---------------------------------------------------------------------------
// In-memory dirty/save/persist.
// ---------------------------------------------------------------------------

/// `void ReplicationSlotSave(void)` (slot.c:1083).
pub fn ReplicationSlotSave() -> PgResult<()> {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot.
    let name = name_str_string(&unsafe { slot_ref(slot) }.data.name);
    let path = format!("{PG_REPLSLOT_DIR}/{name}");
    SaveSlotToPath(slot, &path, ERROR)
}

/// `void ReplicationSlotMarkDirty(void)` (slot.c:1101).
pub fn ReplicationSlotMarkDirty() {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot; take its mutex.
    let s = unsafe { slot_mut(slot) };
    spin_acquire(s);
    s.just_dirtied = true;
    s.dirty = true;
    spin_release(s);
}

/// `void ReplicationSlotPersist(void)` (slot.c:1118).
pub fn ReplicationSlotPersist() -> PgResult<()> {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot.
    debug_assert!(unsafe { slot_ref(slot) }.data.persistency != ReplicationSlotPersistency::RS_PERSISTENT);
    let s = unsafe { slot_mut(slot) };
    spin_acquire(s);
    s.data.persistency = ReplicationSlotPersistency::RS_PERSISTENT;
    spin_release(s);

    ReplicationSlotMarkDirty();
    ReplicationSlotSave()
}

// ---------------------------------------------------------------------------
// Required-resource computation.
// ---------------------------------------------------------------------------

/// `void ReplicationSlotsComputeRequiredXmin(bool already_locked)` (slot.c:1143).
pub fn ReplicationSlotsComputeRequiredXmin(already_locked: bool) -> PgResult<()> {
    let mut agg_xmin: TransactionId = 0;
    let mut agg_catalog_xmin: TransactionId = 0;

    debug_assert!(SLOT_ARRAY.get().is_some());

    if !already_locked {
        lock_control(LWLockMode::LW_SHARED)?;
    }

    for i in 0..max_replication_slots() as usize {
        // SAFETY: ControlLock held.
        let s = unsafe { slot_ref(i) };
        if !s.in_use {
            continue;
        }
        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(i) };
        spin_acquire(sm);
        let effective_xmin = sm.effective_xmin;
        let effective_catalog_xmin = sm.effective_catalog_xmin;
        let invalidated = sm.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE;
        spin_release(sm);

        if invalidated {
            continue;
        }

        if TransactionIdIsValid(effective_xmin)
            && (!TransactionIdIsValid(agg_xmin) || TransactionIdPrecedes(effective_xmin, agg_xmin))
        {
            agg_xmin = effective_xmin;
        }
        if TransactionIdIsValid(effective_catalog_xmin)
            && (!TransactionIdIsValid(agg_catalog_xmin)
                || TransactionIdPrecedes(effective_catalog_xmin, agg_catalog_xmin))
        {
            agg_catalog_xmin = effective_catalog_xmin;
        }
    }

    procarray::proc_array_set_replication_slot_xmin::call(agg_xmin, agg_catalog_xmin, already_locked);

    if !already_locked {
        unlock_control()?;
    }
    Ok(())
}

/// `void ReplicationSlotsComputeRequiredLSN(void)` (slot.c:1225).
pub fn ReplicationSlotsComputeRequiredLSN() -> PgResult<()> {
    let mut min_required: XLogRecPtr = 0;

    debug_assert!(SLOT_ARRAY.get().is_some());

    lock_control(LWLockMode::LW_SHARED)?;
    for i in 0..max_replication_slots() as usize {
        // SAFETY: ControlLock held shared.
        let s = unsafe { slot_ref(i) };
        if !s.in_use {
            continue;
        }
        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(i) };
        spin_acquire(sm);
        let persistency = sm.data.persistency;
        let mut restart_lsn = sm.data.restart_lsn;
        let invalidated = sm.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE;
        let last_saved_restart_lsn = sm.last_saved_restart_lsn;
        spin_release(sm);

        if invalidated {
            continue;
        }

        if persistency == ReplicationSlotPersistency::RS_PERSISTENT
            && last_saved_restart_lsn != 0
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }

        if restart_lsn != 0 && (min_required == 0 || restart_lsn < min_required) {
            min_required = restart_lsn;
        }
    }
    unlock_control()?;

    xlog::xlog_set_replication_slot_minimum_lsn::call(min_required);
    Ok(())
}

/// `XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void)` (slot.c:1295).
pub fn ReplicationSlotsComputeLogicalRestartLSN() -> PgResult<XLogRecPtr> {
    let mut result: XLogRecPtr = 0;

    if max_replication_slots() <= 0 {
        return Ok(0);
    }

    lock_control(LWLockMode::LW_SHARED)?;
    for i in 0..max_replication_slots() as usize {
        // SAFETY: ControlLock held shared.
        let s = unsafe { slot_ref(i) };
        if !s.in_use {
            continue;
        }
        if !SlotIsLogical(s) {
            continue;
        }
        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(i) };
        spin_acquire(sm);
        let persistency = sm.data.persistency;
        let mut restart_lsn = sm.data.restart_lsn;
        let invalidated = sm.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE;
        let last_saved_restart_lsn = sm.last_saved_restart_lsn;
        spin_release(sm);

        if invalidated {
            continue;
        }
        if persistency == ReplicationSlotPersistency::RS_PERSISTENT
            && last_saved_restart_lsn != 0
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }
        if restart_lsn == 0 {
            continue;
        }
        if result == 0 || restart_lsn < result {
            result = restart_lsn;
        }
    }
    unlock_control()?;
    Ok(result)
}

/// `bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive)`
/// (slot.c:1374) — returns `(found, nslots, nactive)`.
pub fn ReplicationSlotsCountDBSlots(dboid: Oid) -> PgResult<(bool, i32, i32)> {
    let mut nslots = 0;
    let mut nactive = 0;

    if max_replication_slots() <= 0 {
        return Ok((false, 0, 0));
    }

    lock_control(LWLockMode::LW_SHARED)?;
    for i in 0..max_replication_slots() as usize {
        // SAFETY: ControlLock held shared.
        let s = unsafe { slot_ref(i) };
        if !s.in_use {
            continue;
        }
        if !SlotIsLogical(s) {
            continue;
        }
        if s.data.database != dboid {
            continue;
        }
        // NB: intentionally counting invalidated slots.
        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(i) };
        spin_acquire(sm);
        nslots += 1;
        if sm.active_pid != 0 {
            nactive += 1;
        }
        spin_release(sm);
    }
    unlock_control()?;

    Ok((nslots > 0, nslots, nactive))
}

/// `void ReplicationSlotsDropDBSlots(Oid dboid)` (slot.c:1432). `my_proc_pid`
/// is the caller's `MyProcPid`.
pub fn ReplicationSlotsDropDBSlots(dboid: Oid, my_proc_pid: i32) -> PgResult<()> {
    if max_replication_slots() <= 0 {
        return Ok(());
    }

    'restart: loop {
        lock_control(LWLockMode::LW_SHARED)?;
        for i in 0..max_replication_slots() as usize {
            // SAFETY: ControlLock held shared.
            let s = unsafe { slot_ref(i) };
            if !s.in_use {
                continue;
            }
            if !SlotIsLogical(s) {
                continue;
            }
            if s.data.database != dboid {
                continue;
            }
            // NB: intentionally including invalidated slots.

            // SAFETY: take the per-slot mutex.
            let sm = unsafe { slot_mut(i) };
            spin_acquire(sm);
            let slotname = name_str_string(&sm.data.name);
            let active_pid = sm.active_pid;
            if active_pid == 0 {
                set_my_replication_slot(Some(i));
                sm.active_pid = my_proc_pid;
            }
            spin_release(sm);

            if active_pid != 0 {
                unlock_control()?;
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_IN_USE)
                    .errmsg(format!(
                        "replication slot \"{slotname}\" is active for PID {active_pid}"
                    ))
                    .finish(loc(1497, "ReplicationSlotsDropDBSlots"));
            }

            // Release ControlLock over filesystem ops; restart the scan.
            unlock_control()?;
            ReplicationSlotDropAcquired()?;
            continue 'restart;
        }
        unlock_control()?;
        return Ok(());
    }
}

// ---------------------------------------------------------------------------
// Requirements / permissions / WAL reservation.
// ---------------------------------------------------------------------------

/// `void CheckSlotRequirements(void)` (slot.c:1524). `wal_level` is the
/// `wal_level` GUC value.
pub fn CheckSlotRequirements(wal_level: i32) -> PgResult<()> {
    if max_replication_slots() == 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("replication slots can only be used if \"max_replication_slots\" > 0")
            .finish(loc(1532, "CheckSlotRequirements"));
    }
    if wal_level < WAL_LEVEL_REPLICA {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("replication slots can only be used if \"wal_level\" >= \"replica\"")
            .finish(loc(1537, "CheckSlotRequirements"));
    }
    Ok(())
}

/// `void CheckSlotPermissions(void)` (slot.c:1546). `user_id` is `GetUserId()`.
pub fn CheckSlotPermissions(mcx: mcx::Mcx<'_>, user_id: Oid) -> PgResult<()> {
    if !miscinit::has_rolreplication::call(mcx, user_id)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to use replication slots")
            .errdetail("Only roles with the REPLICATION attribute may use replication slots.")
            .finish(loc(1549, "CheckSlotPermissions"));
    }
    Ok(())
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` (`xlog_internal.h`).
fn xl_byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as XLogRecPtr
}

/// `void ReplicationSlotReserveWal(void)` (slot.c:1563).
pub fn ReplicationSlotReserveWal() -> PgResult<()> {
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot.
    {
        let s = unsafe { slot_ref(slot) };
        debug_assert!(s.data.restart_lsn == 0);
        debug_assert!(s.last_saved_restart_lsn == 0);
    }

    // Exclusive lock prevents the checkpointer computing the min slot LSN
    // concurrently (see CheckPointReplicationSlots).
    lock_allocation(LWLockMode::LW_EXCLUSIVE)?;

    // SAFETY: we own the slot.
    let is_physical = SlotIsPhysical(unsafe { slot_ref(slot) });
    let restart_lsn = if is_physical {
        xlog::get_redo_rec_ptr::call()
    } else if xlog::recovery_in_progress::call() {
        xlog::get_xlog_replay_rec_ptr::call()
    } else {
        xlog::get_xlog_insert_rec_ptr::call()
    };

    // SAFETY: take the per-slot mutex.
    let s = unsafe { slot_mut(slot) };
    spin_acquire(s);
    s.data.restart_lsn = restart_lsn;
    spin_release(s);

    // Prevent WAL removal ASAP.
    ReplicationSlotsComputeRequiredLSN()?;

    // Checkpoint shouldn't remove the required WAL.
    let wal_segsz = xlog::wal_segment_size::call();
    // SAFETY: we own the slot.
    let restart_lsn = unsafe { slot_ref(slot) }.data.restart_lsn;
    let segno = xl_byte_to_seg(restart_lsn, wal_segsz);
    if xlog::xlog_get_last_removed_segno::call() >= segno {
        // SAFETY: we own the slot.
        let nm = name_str_string(&unsafe { slot_ref(slot) }.data.name);
        let _ = unlock_allocation();
        return elog(
            ERROR,
            format!("WAL required by replication slot {nm} has been removed concurrently"),
        );
    }

    unlock_allocation()?;

    // SAFETY: we own the slot.
    let is_logical = SlotIsLogical(unsafe { slot_ref(slot) });
    if !xlog::recovery_in_progress::call() && is_logical {
        // Make sure we have enough information to start.
        let flushptr = xlog::log_standby_snapshot::call()?;
        // And make sure it's fsynced to disk.
        xlog::xlog_flush::call(flushptr)?;
    }

    Ok(())
}

fn timestamp_difference(start: TimestampTz, stop: TimestampTz) -> (i64, i32) {
    timestamp_seams::timestamp_difference::call(start, stop)
}
fn timestamp_difference_exceeds_seconds(start: TimestampTz, stop: TimestampTz, secs: i32) -> bool {
    timestamp_seams::timestamp_difference_exceeds_seconds::call(start, stop, secs)
}

// ---------------------------------------------------------------------------
// Invalidation.
// ---------------------------------------------------------------------------

/// `LSN_FORMAT_ARGS(lsn)` rendering — `%X/%X`.
fn lsn_format(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `static void ReportSlotInvalidation(...)` (slot.c:1642).
fn ReportSlotInvalidation(
    cause: ReplicationSlotInvalidationCause,
    terminating: bool,
    pid: i32,
    slotname: NameData,
    restart_lsn: XLogRecPtr,
    oldest_lsn: XLogRecPtr,
    snapshot_conflict_horizon: TransactionId,
    slot_idle_seconds: i64,
) -> PgResult<()> {
    // `initStringInfo(&err_detail)` / `initStringInfo(&err_hint)` — start
    // empty, then each cause arm appends (C never reaches the RS_INVAL_NONE arm).
    #[allow(unused_assignments)]
    let mut err_detail = String::new();
    let mut err_hint = String::new();

    match cause {
        ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED => {
            let ex: u64 = oldest_lsn.wrapping_sub(restart_lsn);
            let unit = if ex == 1 { "byte" } else { "bytes" };
            err_detail = format!(
                "The slot's restart_lsn {} exceeds the limit by {ex} {unit}.",
                lsn_format(restart_lsn)
            );
            err_hint = "You might need to increase \"max_slot_wal_keep_size\".".to_string();
        }
        ReplicationSlotInvalidationCause::RS_INVAL_HORIZON => {
            err_detail =
                format!("The slot conflicted with xid horizon {snapshot_conflict_horizon}.");
        }
        ReplicationSlotInvalidationCause::RS_INVAL_WAL_LEVEL => {
            err_detail = "Logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary server.".to_string();
        }
        ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT => {
            err_detail = format!(
                "The slot's idle time of {slot_idle_seconds}s exceeds the configured \"idle_replication_slot_timeout\" duration of {}s.",
                idle_replication_slot_timeout_secs()
            );
            err_hint =
                "You might need to increase \"idle_replication_slot_timeout\".".to_string();
        }
        ReplicationSlotInvalidationCause::RS_INVAL_NONE => unreachable!("pg_unreachable()"),
    }

    let name = name_str_string(&slotname);
    let msg = if terminating {
        format!("terminating process {pid} to release replication slot \"{name}\"")
    } else {
        format!("invalidating obsolete replication slot \"{name}\"")
    };

    let mut builder = ereport(LOG).errmsg(msg).errdetail_internal(err_detail);
    if !err_hint.is_empty() {
        builder = builder.errhint(err_hint);
    }
    builder.finish(loc(1698, "ReportSlotInvalidation"))
}

/// `static inline bool CanInvalidateIdleSlot(ReplicationSlot *s)` (slot.c:1723).
fn CanInvalidateIdleSlot(s: &ReplicationSlot) -> bool {
    idle_replication_slot_timeout_secs() != 0
        && s.data.restart_lsn != 0
        && s.inactive_since > 0
        && !(xlog::recovery_in_progress::call() && s.data.synced != 0)
}

/// `static ReplicationSlotInvalidationCause DetermineSlotInvalidationCause(...)`
/// (slot.c:1739). Returns the cause and (for idle-timeout) writes
/// `inactive_since`.
fn DetermineSlotInvalidationCause(
    possible_causes: u32,
    s: &ReplicationSlot,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    inactive_since: &mut TimestampTz,
    now: TimestampTz,
) -> ReplicationSlotInvalidationCause {
    debug_assert!(possible_causes != ReplicationSlotInvalidationCause::RS_INVAL_NONE as u32);

    if possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED as u32 != 0 {
        let restart_lsn = s.data.restart_lsn;
        if restart_lsn != 0 && restart_lsn < oldest_lsn {
            return ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED;
        }
    }

    if possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_HORIZON as u32 != 0 {
        // invalid DB oid signals a shared relation.
        if SlotIsLogical(s) && (dboid == InvalidOid || dboid == s.data.database) {
            let effective_xmin = s.effective_xmin;
            let catalog_effective_xmin = s.effective_catalog_xmin;
            if TransactionIdIsValid(effective_xmin)
                && TransactionIdPrecedesOrEquals(effective_xmin, snapshot_conflict_horizon)
            {
                return ReplicationSlotInvalidationCause::RS_INVAL_HORIZON;
            } else if TransactionIdIsValid(catalog_effective_xmin)
                && TransactionIdPrecedesOrEquals(catalog_effective_xmin, snapshot_conflict_horizon)
            {
                return ReplicationSlotInvalidationCause::RS_INVAL_HORIZON;
            }
        }
    }

    if possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_WAL_LEVEL as u32 != 0
        && SlotIsLogical(s)
    {
        return ReplicationSlotInvalidationCause::RS_INVAL_WAL_LEVEL;
    }

    if possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT as u32 != 0 {
        debug_assert!(now > 0);
        if CanInvalidateIdleSlot(s) {
            // IS_INJECTION_POINT_ATTACHED("slot-timeout-inval") — simulate the
            // idle_timeout invalidation promptly to test the timeout behavior
            // (044_invalidate_inactive_slots). Injection points are compiled in;
            // this is a cheap shmem check when nothing is attached.
            if injection_point_seams::is_injection_point_attached::call("slot-timeout-inval")
                .unwrap_or(false)
            {
                *inactive_since = 0; // since the beginning of time
                return ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT;
            }
            if timestamp_difference_exceeds_seconds(
                s.inactive_since,
                now,
                idle_replication_slot_timeout_secs(),
            ) {
                *inactive_since = s.inactive_since;
                return ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT;
            }
        }
    }

    ReplicationSlotInvalidationCause::RS_INVAL_NONE
}

/// `static bool InvalidatePossiblyObsoleteSlot(...)` (slot.c:1831). Returns
/// whether `ReplicationSlotControlLock` was released in the interim, and sets
/// `*invalidated` true if the slot was invalidated.
fn InvalidatePossiblyObsoleteSlot(
    possible_causes: u32,
    s: usize,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    invalidated: &mut bool,
) -> PgResult<bool> {
    let mut last_signaled_pid: i32 = 0;
    let mut released_lock = false;
    let mut inactive_since: TimestampTz = 0;
    let my_pid = miscinit_my_proc_pid();

    loop {
        let mut now: TimestampTz = 0;
        let mut slot_idle_secs: i64 = 0;

        // SAFETY: ControlLock held shared (per assertion).
        if !unsafe { slot_ref(s) }.in_use {
            if released_lock {
                unlock_control()?;
            }
            break;
        }

        if possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT as u32 != 0 {
            now = get_current_timestamp();
        }

        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(s) };
        spin_acquire(sm);

        let restart_lsn = sm.data.restart_lsn;

        let mut invalidation_cause = ReplicationSlotInvalidationCause::RS_INVAL_NONE;
        if sm.data.invalidated == ReplicationSlotInvalidationCause::RS_INVAL_NONE {
            invalidation_cause = DetermineSlotInvalidationCause(
                possible_causes,
                sm,
                oldest_lsn,
                dboid,
                snapshot_conflict_horizon,
                &mut inactive_since,
                now,
            );
        }

        if invalidation_cause == ReplicationSlotInvalidationCause::RS_INVAL_NONE {
            spin_release(sm);
            if released_lock {
                unlock_control()?;
            }
            break;
        }

        let slotname = sm.data.name;
        let active_pid = sm.active_pid;

        if active_pid == 0 {
            set_my_replication_slot(Some(s));
            sm.active_pid = my_pid;
            sm.data.invalidated = invalidation_cause;
            if invalidation_cause == ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED {
                sm.data.restart_lsn = 0;
                sm.last_saved_restart_lsn = 0;
            }
            *invalidated = true;
        }

        spin_release(sm);

        if invalidation_cause == ReplicationSlotInvalidationCause::RS_INVAL_IDLE_TIMEOUT {
            let (secs, _usecs) = timestamp_difference(inactive_since, now);
            slot_idle_secs = secs;
        }

        if active_pid != 0 {
            // Prepare the sleep before releasing the lock.
            cv::condition_variable_prepare_to_sleep::call(&unsafe { slot_ref(s) }.active_cv);

            unlock_control()?;
            released_lock = true;

            // Signal the owner to terminate, if not already signaled.
            if last_signaled_pid != active_pid {
                ReportSlotInvalidation(
                    invalidation_cause,
                    true,
                    active_pid,
                    slotname,
                    restart_lsn,
                    oldest_lsn,
                    snapshot_conflict_horizon,
                    slot_idle_secs,
                )?;

                if miscinit::my_backend_type::call() == BackendType::Startup {
                    let _ = procsignal::send_proc_signal::call(
                        active_pid,
                        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
                        ::types_core::INVALID_PROC_NUMBER,
                    );
                } else {
                    // SAFETY: kill(2) with a known pid and SIGTERM.
                    unsafe {
                        libc::kill(active_pid as libc::pid_t, libc::SIGTERM);
                    }
                }

                last_signaled_pid = active_pid;
            }

            // Wait until the slot is released.
            cv::condition_variable_sleep::call(
                &unsafe { slot_ref(s) }.active_cv,
                wait_event::WAIT_EVENT_REPLICATION_SLOT_DROP,
            )?;

            // Re-acquire and start over.
            lock_control(LWLockMode::LW_SHARED)?;
            continue;
        } else {
            // We hold the slot and already invalidated it; persist and release.
            unlock_control()?;
            released_lock = true;

            ReplicationSlotMarkDirty();
            ReplicationSlotSave()?;
            ReplicationSlotRelease()?;

            ReportSlotInvalidation(
                invalidation_cause,
                false,
                active_pid,
                slotname,
                restart_lsn,
                oldest_lsn,
                snapshot_conflict_horizon,
                slot_idle_secs,
            )?;

            break;
        }
    }

    Ok(released_lock)
}

/// `bool InvalidateObsoleteReplicationSlots(uint32 possible_causes,
/// XLogSegNo oldestSegno, Oid dboid, TransactionId snapshotConflictHorizon)`
/// (slot.c:2059).
pub fn InvalidateObsoleteReplicationSlots(
    possible_causes: u32,
    oldest_segno: XLogSegNo,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
) -> PgResult<bool> {
    let mut invalidated = false;

    debug_assert!(
        possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_HORIZON as u32 == 0
            || TransactionIdIsValid(snapshot_conflict_horizon)
    );
    debug_assert!(
        possible_causes & ReplicationSlotInvalidationCause::RS_INVAL_WAL_REMOVED as u32 == 0
            || oldest_segno > 0
    );
    debug_assert!(possible_causes != ReplicationSlotInvalidationCause::RS_INVAL_NONE as u32);

    if max_replication_slots() == 0 {
        return Ok(invalidated);
    }

    // XLogSegNoOffsetToRecPtr(oldestSegno, 0, wal_segment_size, oldestLSN).
    let wal_segsz = xlog::wal_segment_size::call();
    let oldest_lsn: XLogRecPtr = oldest_segno * wal_segsz as XLogRecPtr;

    'restart: loop {
        lock_control(LWLockMode::LW_SHARED)?;
        for i in 0..max_replication_slots() as usize {
            // SAFETY: ControlLock held shared.
            let s = unsafe { slot_ref(i) };
            if !s.in_use {
                continue;
            }
            // Prevent invalidation of logical slots during binary upgrade.
            if SlotIsLogical(s) && miscinit::is_binary_upgrade::call() {
                continue;
            }
            if InvalidatePossiblyObsoleteSlot(
                possible_causes,
                i,
                oldest_lsn,
                dboid,
                snapshot_conflict_horizon,
                &mut invalidated,
            )? {
                continue 'restart;
            }
        }
        unlock_control()?;
        break;
    }

    if invalidated {
        ReplicationSlotsComputeRequiredXmin(false)?;
        ReplicationSlotsComputeRequiredLSN()?;
    }

    Ok(invalidated)
}

// ---------------------------------------------------------------------------
// Checkpoint + startup.
// ---------------------------------------------------------------------------

/// `void CheckPointReplicationSlots(bool is_shutdown)` (slot.c:2119).
pub fn CheckPointReplicationSlots(is_shutdown: bool) -> PgResult<()> {
    let mut last_saved_restart_lsn_updated = false;

    elog(DEBUG1, "performing replication slot checkpoint".to_string())?;

    // Allocation lock (shared) is enough to freeze the in_use bits and to
    // serialize against concurrent WAL reservation.
    lock_allocation(LWLockMode::LW_SHARED)?;

    for i in 0..max_replication_slots() as usize {
        // SAFETY: allocation lock held shared.
        let s = unsafe { slot_ref(i) };
        if !s.in_use {
            continue;
        }

        let name = name_str_string(&s.data.name);
        let path = format!("{PG_REPLSLOT_DIR}/{name}");

        if is_shutdown && SlotIsLogical(s) {
            // SAFETY: take the per-slot mutex.
            let sm = unsafe { slot_mut(i) };
            spin_acquire(sm);
            if sm.data.invalidated == ReplicationSlotInvalidationCause::RS_INVAL_NONE
                && sm.data.confirmed_flush > sm.last_saved_confirmed_flush
            {
                sm.just_dirtied = true;
                sm.dirty = true;
            }
            spin_release(sm);
        }

        // SAFETY: allocation lock held shared.
        let s = unsafe { slot_ref(i) };
        if s.last_saved_restart_lsn != s.data.restart_lsn {
            last_saved_restart_lsn_updated = true;
        }

        SaveSlotToPath(i, &path, LOG)?;
    }
    unlock_allocation()?;

    if last_saved_restart_lsn_updated {
        ReplicationSlotsComputeRequiredLSN()?;
    }
    Ok(())
}

/// `void StartupReplicationSlots(void)` (slot.c:2197).
pub fn StartupReplicationSlots() -> PgResult<()> {
    elog(DEBUG1, "starting up replication slots".to_string())?;

    // PGFileType codes.
    const PGFILETYPE_ERROR: i32 = 0;
    const PGFILETYPE_DIR: i32 = 3;

    let entries = fd::read_dir_names::call(PG_REPLSLOT_DIR)?;
    for name in entries {
        if name == "." || name == ".." {
            continue;
        }
        let path = format!("{PG_REPLSLOT_DIR}/{name}");
        let de_type = fd::get_dirent_type::call(&path);

        // We're only creating directories here.
        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_DIR {
            continue;
        }

        // We crashed while a slot was being set up or deleted: clean up.
        if name.ends_with(".tmp") {
            if !fd::rmtree::call(&path, true) {
                ereport(WARNING)
                    .errmsg(format!("could not remove directory \"{path}\""))
                    .finish(loc(2228, "StartupReplicationSlots"))?;
                continue;
            }
            fd::fsync_fname::call(PG_REPLSLOT_DIR, true)?;
            continue;
        }

        // Looks like a slot in a normal state: restore.
        RestoreSlotFromDisk(&name)?;
    }

    if max_replication_slots() <= 0 {
        return Ok(());
    }

    ReplicationSlotsComputeRequiredXmin(false)?;
    ReplicationSlotsComputeRequiredLSN()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// On-disk state.
// ---------------------------------------------------------------------------

/// Byte view of a `#[repr(C)]` `ReplicationSlotOnDisk` (`write(fd, &cp, ...)`).
fn on_disk_as_bytes(cp: &ReplicationSlotOnDisk) -> &[u8] {
    // SAFETY: `ReplicationSlotOnDisk` is `#[repr(C)]` POD; we read its bytes.
    unsafe {
        std::slice::from_raw_parts(
            cp as *const ReplicationSlotOnDisk as *const u8,
            core::mem::size_of::<ReplicationSlotOnDisk>(),
        )
    }
}

/// Mutable byte view of a `ReplicationSlotOnDisk` (`read(fd, &cp, ...)`).
fn on_disk_as_bytes_mut(cp: &mut ReplicationSlotOnDisk) -> &mut [u8] {
    // SAFETY: `ReplicationSlotOnDisk` is `#[repr(C)]` POD.
    unsafe {
        std::slice::from_raw_parts_mut(
            cp as *mut ReplicationSlotOnDisk as *mut u8,
            core::mem::size_of::<ReplicationSlotOnDisk>(),
        )
    }
}

/// `INIT_CRC32C` + `COMP_CRC32C(checksummed region)` + `FIN_CRC32C`.
fn compute_on_disk_checksum(cp: &ReplicationSlotOnDisk) -> u32 {
    let bytes = on_disk_as_bytes(cp);
    let start = ReplicationSlotOnDiskNotChecksummedSize();
    let len = ReplicationSlotOnDiskChecksummedSize();
    let mut crc: u32 = 0xFFFF_FFFF; // INIT_CRC32C
    crc = pg_crc32c_seams::pg_comp_crc32c::call(crc, &bytes[start..start + len]);
    crc ^ 0xFFFF_FFFF // FIN_CRC32C
}

/// `static void CreateSlotOnDisk(ReplicationSlot *slot)` (slot.c:2258).
fn CreateSlotOnDisk(slot: usize) -> PgResult<()> {
    // SAFETY: caller holds the allocation lock; slot not visible yet.
    let name = name_str_string(&unsafe { slot_ref(slot) }.data.name);
    let path = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmppath = format!("{PG_REPLSLOT_DIR}/{name}.tmp");

    // Clean up a stray temp directory from a previous effort.
    if fd::path_is_dir::call(&tmppath) {
        fd::rmtree::call(&tmppath, true);
    }

    // Create and fsync the temporary slot directory.
    if fd::make_pg_directory::call(&tmppath) < 0 {
        let errno = fd::last_errno::call();
        return file_access_error(
            ERROR,
            errno,
            format!("could not create directory \"{tmppath}\""),
            loc(2285, "CreateSlotOnDisk"),
        );
    }
    fd::fsync_fname::call(&tmppath, true)?;

    // Write the actual state file.
    // SAFETY: allocation lock held.
    unsafe { slot_mut(slot) }.dirty = true; // signal that we really need to write
    SaveSlotToPath(slot, &tmppath, ERROR)?;

    // Rename the directory into place.
    if fd::rename_file::call(&tmppath, &path) != 0 {
        let errno = fd::last_errno::call();
        return file_access_error(
            ERROR,
            errno,
            format!("could not rename file \"{tmppath}\" to \"{path}\""),
            loc(2297, "CreateSlotOnDisk"),
        );
    }

    // Force a restart on failure (crit section); fsync_fname carries the error.
    fd::fsync_fname::call(&path, true)?;
    fd::fsync_fname::call(PG_REPLSLOT_DIR, true)?;
    Ok(())
}

/// `static void SaveSlotToPath(ReplicationSlot *slot, const char *dir,
/// int elevel)` (slot.c:2319).
fn SaveSlotToPath(slot: usize, dir: &str, elevel: ErrorLevel) -> PgResult<()> {
    // Check whether there's something to write out.
    // SAFETY: take the per-slot mutex.
    let sm = unsafe { slot_mut(slot) };
    spin_acquire(sm);
    let was_dirty = sm.dirty;
    sm.just_dirtied = false;
    spin_release(sm);

    if !was_dirty {
        return Ok(());
    }

    // SAFETY: io_in_progress_lock lives in the slot.
    let io_guard = lwlock::lwlock_acquire::call(
        &unsafe { slot_ref(slot) }.io_in_progress_lock,
        LWLockMode::LW_EXCLUSIVE,
        init_small_seams::my_proc_number::call(),
    )?;

    let mut cp = ReplicationSlotOnDisk::default(); // silence valgrind (memset 0)

    let tmppath = format!("{dir}/state.tmp");
    let path = format!("{dir}/state");

    // O_CREAT | O_EXCL | O_WRONLY | PG_BINARY.
    let flags = libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY;
    let fd_idx = fd::open_transient_file::call(&tmppath, flags);
    if fd_idx < 0 {
        // Release the io lock and report (guard drop also releases).
        let errno = -fd_idx;
        drop(io_guard);
        return file_access_error(
            elevel,
            errno,
            format!("could not create file \"{tmppath}\""),
            loc(2358, "SaveSlotToPath"),
        );
    }

    cp.magic = SLOT_MAGIC;
    // INIT_CRC32C(cp.checksum) — set to the running-CRC seed; recomputed below.
    cp.checksum = 0xFFFF_FFFF;
    cp.version = SLOT_VERSION;
    cp.length = ReplicationSlotOnDiskV2Size() as u32;

    // SAFETY: take the per-slot mutex to snapshot persistent data.
    let sm = unsafe { slot_ref(slot) };
    spin_acquire(sm);
    cp.slotdata = sm.data;
    spin_release(sm);

    cp.checksum = compute_on_disk_checksum(&cp);

    // write(fd, &cp, sizeof(cp)).
    waitevent::pgstat_report_wait_start::call(wait_event::WAIT_EVENT_REPLICATION_SLOT_WRITE);
    let bytes = on_disk_as_bytes(&cp);
    let written = fd::transient_write::call(fd_idx, bytes);
    if written != bytes.len() as isize {
        waitevent::pgstat_report_wait_end::call();
        fd::close_transient_file::call(fd_idx);
        fd::unlink_file::call(&tmppath);
        drop(io_guard);
        // If write didn't set errno, assume out of disk space.
        let mut errno = if written < 0 { -(written as i32) } else { 0 };
        if errno == 0 {
            errno = libc::ENOSPC;
        }
        return file_access_error(
            elevel,
            errno,
            format!("could not write to file \"{tmppath}\""),
            loc(2394, "SaveSlotToPath"),
        );
    }
    waitevent::pgstat_report_wait_end::call();

    // fsync the temporary file.
    waitevent::pgstat_report_wait_start::call(wait_event::WAIT_EVENT_REPLICATION_SLOT_SYNC);
    let rc = fd::pg_fsync::call(fd_idx);
    if rc != 0 {
        waitevent::pgstat_report_wait_end::call();
        fd::close_transient_file::call(fd_idx);
        fd::unlink_file::call(&tmppath);
        drop(io_guard);
        return file_access_error(
            elevel,
            -rc,
            format!("could not fsync file \"{tmppath}\""),
            loc(2414, "SaveSlotToPath"),
        );
    }
    waitevent::pgstat_report_wait_end::call();

    let rc = fd::close_transient_file::call(fd_idx);
    if rc != 0 {
        fd::unlink_file::call(&tmppath);
        drop(io_guard);
        return file_access_error(
            elevel,
            -rc,
            format!("could not close file \"{tmppath}\""),
            loc(2430, "SaveSlotToPath"),
        );
    }

    // Rename to permanent file, fsync file and directory.
    if fd::rename_file::call(&tmppath, &path) != 0 {
        let errno = fd::last_errno::call();
        fd::unlink_file::call(&tmppath);
        drop(io_guard);
        return file_access_error(
            elevel,
            errno,
            format!("could not rename file \"{tmppath}\" to \"{path}\""),
            loc(2446, "SaveSlotToPath"),
        );
    }

    // Crit section: fsync the renamed file and the dirs.
    fd::fsync_fname::call(&path, false)?;
    fd::fsync_fname::call(dir, true)?;
    fd::fsync_fname::call(PG_REPLSLOT_DIR, true)?;

    // Successfully wrote: unset dirty unless re-dirtied; remember flushed LSNs.
    // SAFETY: take the per-slot mutex.
    let sm = unsafe { slot_mut(slot) };
    spin_acquire(sm);
    if !sm.just_dirtied {
        sm.dirty = false;
    }
    sm.last_saved_confirmed_flush = cp.slotdata.confirmed_flush;
    sm.last_saved_restart_lsn = cp.slotdata.restart_lsn;
    spin_release(sm);

    io_guard.release()?;
    Ok(())
}

/// `static void RestoreSlotFromDisk(const char *name)` (slot.c:2482).
fn RestoreSlotFromDisk(name: &str) -> PgResult<()> {
    let mut cp = ReplicationSlotOnDisk::default();
    let mut now: TimestampTz = 0;

    // No locking here: no concurrent access allowed yet.
    let slotdir = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmp = format!("{slotdir}/state.tmp");
    // Delete temp file if it exists.
    let rc = fd::unlink_file::call(&tmp);
    if rc < 0 && -rc != libc::ENOENT {
        return file_access_error(
            PANIC,
            -rc,
            format!("could not remove file \"{tmp}\""),
            loc(2500, "RestoreSlotFromDisk"),
        );
    }

    let path = format!("{slotdir}/state");
    elog(DEBUG1, format!("restoring replication slot from \"{path}\""))?;

    // O_RDWR | PG_BINARY (fsync on some OSes requires O_RDWR).
    let fd_idx = fd::open_transient_file::call(&path, libc::O_RDWR);
    if fd_idx < 0 {
        return file_access_error(
            PANIC,
            -fd_idx,
            format!("could not open file \"{path}\""),
            loc(2516, "RestoreSlotFromDisk"),
        );
    }

    // Sync state file before reading from it.
    waitevent::pgstat_report_wait_start::call(wait_event::WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC);
    let rc = fd::pg_fsync::call(fd_idx);
    if rc != 0 {
        waitevent::pgstat_report_wait_end::call();
        return file_access_error(
            PANIC,
            -rc,
            format!("could not fsync file \"{path}\""),
            loc(2526, "RestoreSlotFromDisk"),
        );
    }
    waitevent::pgstat_report_wait_end::call();

    // Sync the parent directory.
    fd::fsync_fname::call(&slotdir, true)?;

    // Read the version-independent part.
    let constant_size = ReplicationSlotOnDiskConstantSize();
    waitevent::pgstat_report_wait_start::call(wait_event::WAIT_EVENT_REPLICATION_SLOT_READ);
    let read_bytes = {
        let buf = &mut on_disk_as_bytes_mut(&mut cp)[..constant_size];
        fd::transient_read::call(fd_idx, buf)
    };
    waitevent::pgstat_report_wait_end::call();
    if read_bytes != constant_size as isize {
        if read_bytes < 0 {
            return file_access_error(
                PANIC,
                -(read_bytes as i32),
                format!("could not read file \"{path}\""),
                loc(2545, "RestoreSlotFromDisk"),
            );
        } else {
            return ereport(PANIC)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "could not read file \"{path}\": read {read_bytes} of {constant_size}"
                ))
                .finish(loc(2549, "RestoreSlotFromDisk"));
        }
    }

    // Verify magic.
    if cp.magic != SLOT_MAGIC {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has wrong magic number: {} instead of {}",
                cp.magic, SLOT_MAGIC
            ))
            .finish(loc(2557, "RestoreSlotFromDisk"));
    }
    // Verify version.
    if cp.version != SLOT_VERSION {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has unsupported version {}",
                cp.version
            ))
            .finish(loc(2564, "RestoreSlotFromDisk"));
    }
    // Boundary check on length.
    let v2_size = ReplicationSlotOnDiskV2Size() as u32;
    if cp.length != v2_size {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has corrupted length {}",
                cp.length
            ))
            .finish(loc(2571, "RestoreSlotFromDisk"));
    }

    // Read the rest of the file now that we know the size.
    let length = cp.length as usize;
    waitevent::pgstat_report_wait_start::call(wait_event::WAIT_EVENT_REPLICATION_SLOT_READ);
    let read_bytes = {
        let buf = &mut on_disk_as_bytes_mut(&mut cp)[constant_size..constant_size + length];
        fd::transient_read::call(fd_idx, buf)
    };
    waitevent::pgstat_report_wait_end::call();
    if read_bytes != cp.length as isize {
        if read_bytes < 0 {
            return file_access_error(
                PANIC,
                -(read_bytes as i32),
                format!("could not read file \"{path}\""),
                loc(2585, "RestoreSlotFromDisk"),
            );
        } else {
            return ereport(PANIC)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "could not read file \"{path}\": read {read_bytes} of {}",
                    cp.length
                ))
                .finish(loc(2590, "RestoreSlotFromDisk"));
        }
    }

    let rc = fd::close_transient_file::call(fd_idx);
    if rc != 0 {
        return file_access_error(
            PANIC,
            -rc,
            format!("could not close file \"{path}\""),
            loc(2597, "RestoreSlotFromDisk"),
        );
    }

    // Verify the CRC.
    let checksum = compute_on_disk_checksum(&cp);
    if checksum != cp.checksum {
        return ereport(PANIC)
            .errmsg(format!(
                "checksum mismatch for replication slot file \"{path}\": is {}, should be {}",
                checksum, cp.checksum
            ))
            .finish(loc(2608, "RestoreSlotFromDisk"));
    }

    // If we crashed with an ephemeral slot active, delete instead of restore.
    if cp.slotdata.persistency != ReplicationSlotPersistency::RS_PERSISTENT {
        if !fd::rmtree::call(&slotdir, true) {
            ereport(WARNING)
                .errmsg(format!("could not remove directory \"{slotdir}\""))
                .finish(loc(2620, "RestoreSlotFromDisk"))?;
        }
        fd::fsync_fname::call(PG_REPLSLOT_DIR, true)?;
        return Ok(());
    }

    // Verify slot-type requirements.
    let wal_level = xlog::wal_level::call() as i32;
    if cp.slotdata.database != InvalidOid {
        if wal_level < WAL_LEVEL_LOGICAL {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication slot \"{}\" exists, but \"wal_level\" < \"logical\"",
                    name_str_string(&cp.slotdata.name)
                ))
                .errhint("Change \"wal_level\" to be \"logical\" or higher.")
                .finish(loc(2643, "RestoreSlotFromDisk"));
        }
        if xlog::standby_mode::call() && !xlog::enable_hot_standby::call() {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication slot \"{}\" exists on the standby, but \"hot_standby\" = \"off\"",
                    name_str_string(&cp.slotdata.name)
                ))
                .errhint("Change \"hot_standby\" to be \"on\".")
                .finish(loc(2657, "RestoreSlotFromDisk"));
        }
    } else if wal_level < WAL_LEVEL_REPLICA {
        return ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "physical replication slot \"{}\" exists, but \"wal_level\" < \"replica\"",
                name_str_string(&cp.slotdata.name)
            ))
            .errhint("Change \"wal_level\" to be \"replica\" or higher.")
            .finish(loc(2664, "RestoreSlotFromDisk"));
    }

    // Nothing can be active yet: don't lock anything.
    let mut restored = false;
    for i in 0..max_replication_slots() as usize {
        // SAFETY: no concurrent access at startup.
        let slot = unsafe { slot_mut(i) };
        if slot.in_use {
            continue;
        }

        slot.data = cp.slotdata;
        slot.effective_xmin = cp.slotdata.xmin;
        slot.effective_catalog_xmin = cp.slotdata.catalog_xmin;
        slot.last_saved_confirmed_flush = cp.slotdata.confirmed_flush;
        slot.last_saved_restart_lsn = cp.slotdata.restart_lsn;

        slot.candidate_catalog_xmin = 0;
        slot.candidate_xmin_lsn = 0;
        slot.candidate_restart_lsn = 0;
        slot.candidate_restart_valid = 0;

        slot.in_use = true;
        slot.active_pid = 0;

        if now == 0 {
            now = get_current_timestamp();
        }
        ReplicationSlotSetInactiveSince(slot, now, false);

        restored = true;
        break;
    }

    if !restored {
        return ereport(FATAL)
            .errmsg("too many replication slots active before shutdown")
            .errhint("Increase \"max_replication_slots\" and try again.")
            .finish(loc(2714, "RestoreSlotFromDisk"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Invalidation-cause name <-> enum mapping.
// ---------------------------------------------------------------------------

/// `ReplicationSlotInvalidationCause GetSlotInvalidationCause(const char *cause_name)`
/// (slot.c:2724).
pub fn GetSlotInvalidationCause(cause_name: &str) -> ReplicationSlotInvalidationCause {
    for entry in SLOT_INVALIDATION_CAUSES.iter() {
        if entry.cause_name == cause_name {
            return entry.cause;
        }
    }
    debug_assert!(false, "unknown slot invalidation cause name");
    ReplicationSlotInvalidationCause::RS_INVAL_NONE
}

/// `const char *GetSlotInvalidationCauseName(ReplicationSlotInvalidationCause cause)`
/// (slot.c:2744).
pub fn GetSlotInvalidationCauseName(cause: ReplicationSlotInvalidationCause) -> &'static str {
    for entry in SLOT_INVALIDATION_CAUSES.iter() {
        if entry.cause == cause {
            return entry.cause_name;
        }
    }
    debug_assert!(false, "unknown slot invalidation cause");
    "none"
}

// ---------------------------------------------------------------------------
// synchronized_standby_slots GUC config + standby-wait.
// ---------------------------------------------------------------------------

/// `SyncStandbySlotsConfigData` (slot.c) in owned form — the parsed
/// `synchronized_standby_slots` configuration (C stores a packed
/// `slot_names[FLEXIBLE_ARRAY_MEMBER]`; we keep the same names in a `Vec`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncStandbySlotsConfig {
    pub slot_names: Vec<String>,
}

impl SyncStandbySlotsConfig {
    /// `config->nslotnames`.
    #[inline]
    pub fn nslotnames(&self) -> usize {
        self.slot_names.len()
    }
}

thread_local! {
    /// `static SyncStandbySlotsConfigData *synchronized_standby_slots_config`.
    static SYNCHRONIZED_STANDBY_SLOTS_CONFIG: std::cell::RefCell<Option<SyncStandbySlotsConfig>> =
        const { std::cell::RefCell::new(None) };
    /// `static XLogRecPtr ss_oldest_flush_lsn = InvalidXLogRecPtr`.
    static SS_OLDEST_FLUSH_LSN: std::cell::Cell<XLogRecPtr> = const { std::cell::Cell::new(0) };
}

/// `static bool validate_sync_standby_slots(char *rawname, List **elemlist)`
/// (slot.c:2762) — parse + per-name validation. Returns the parsed names on
/// success; the error tuple mirrors the `GUC_check_*` surface.
fn validate_sync_standby_slots(
    rawname: &str,
) -> Result<Option<Vec<String>>, (Option<SqlState>, String, Option<String>)> {
    // SplitIdentifierString parses the raw string into a list of identifiers.
    // The C call pallocs the name list in the caller's context; here we use a
    // short-lived context for the parse and copy the validated names out.
    let parse_cx = mcx::MemoryContext::new("validate_sync_standby_slots");
    let parsed = varlena_seams::split_identifier_string::call(
        parse_cx.mcx(),
        rawname,
        ',',
    )
    .map_err(|e| (Some(e.sqlstate()), e.message().to_string(), None))?;
    let parsed = match parsed {
        Some(list) => list,
        None => {
            // GUC_check_errdetail("List syntax is invalid.")
            return Err((None, "List syntax is invalid.".to_string(), None));
        }
    };

    // Validate each slot name; copy the surviving names into owned Strings.
    let mut elemlist: Vec<String> = Vec::with_capacity(parsed.len());
    for name in parsed.iter() {
        let name = name.as_str();
        if let Err((err_code, err_msg, err_hint)) = ReplicationSlotValidateNameInternal(name) {
            return Err((Some(err_code), err_msg, err_hint));
        }
        elemlist.push(name.to_string());
    }

    Ok(Some(elemlist))
}

/// `bool check_synchronized_standby_slots(char **newval, void **extra,
/// GucSource source)` (slot.c:2796). Returns the parsed config to store as
/// the GUC `extra` (idiomatic owned form); `Ok(None)` is the empty value.
/// The `Err` tuple is the `GUC_check_*` surface.
pub fn check_synchronized_standby_slots(
    newval: &str,
    _source: types_guc::GucSource,
) -> Result<Option<SyncStandbySlotsConfig>, (Option<SqlState>, String, Option<String>)> {
    if newval.is_empty() {
        return Ok(None);
    }

    let elemlist = validate_sync_standby_slots(newval)?;

    let elemlist = match elemlist {
        Some(list) if !list.is_empty() => list,
        // !ok handled above; elemlist == NIL -> return ok (true) with no extra.
        _ => return Ok(None),
    };

    Ok(Some(SyncStandbySlotsConfig {
        slot_names: elemlist,
    }))
}

/// `void assign_synchronized_standby_slots(const char *newval, void *extra)`
/// (slot.c:2852).
pub fn assign_synchronized_standby_slots(extra: Option<SyncStandbySlotsConfig>) {
    // The standby slots may have changed, so recompute the oldest LSN.
    SS_OLDEST_FLUSH_LSN.with(|c| c.set(0));
    SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| *c.borrow_mut() = extra);
}

/// `bool SlotExistsInSyncStandbySlots(const char *slot_name)` (slot.c:2868).
pub fn SlotExistsInSyncStandbySlots(slot_name: &str) -> bool {
    SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| {
        let c = c.borrow();
        match c.as_ref() {
            None => false,
            Some(config) => config.slot_names.iter().any(|n| n == slot_name),
        }
    })
}

/// `bool StandbySlotsHaveCaughtup(XLogRecPtr wait_for_lsn, int elevel)`
/// (slot.c:2901).
pub fn StandbySlotsHaveCaughtup(wait_for_lsn: XLogRecPtr, elevel: ErrorLevel) -> PgResult<bool> {
    let mut caught_up_slot_num = 0usize;
    let mut min_restart_lsn: XLogRecPtr = 0;

    // No value -> no need to wait.
    let names: Vec<String> = match SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| c.borrow().clone()) {
        None => return Ok(true),
        Some(config) => config.slot_names,
    };

    // No need to wait on a standby (no cascading sync).
    if xlog::recovery_in_progress::call() {
        return Ok(true);
    }

    // Already beyond the target?
    let ss = SS_OLDEST_FLUSH_LSN.with(std::cell::Cell::get);
    if ss != 0 && ss >= wait_for_lsn {
        return Ok(true);
    }

    lock_control(LWLockMode::LW_SHARED)?;

    let nslotnames = names.len();
    let mut broke = false;
    for name in &names {
        let slot = SearchNamedReplicationSlot(name, false)?;

        let slot = match slot {
            Some(i) => i,
            None => {
                unlock_control()?;
                ereport(elevel)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "replication slot \"{name}\" specified in parameter \"synchronized_standby_slots\" does not exist"
                    ))
                    .errdetail(format!(
                        "Logical replication is waiting on the standby associated with replication slot \"{name}\"."
                    ))
                    .errhint(format!(
                        "Create the replication slot \"{name}\" or amend parameter \"synchronized_standby_slots\"."
                    ))
                    .finish(loc(2951, "StandbySlotsHaveCaughtup"))?;
                broke = true;
                break;
            }
        };

        // SAFETY: ControlLock held shared.
        if SlotIsLogical(unsafe { slot_ref(slot) }) {
            unlock_control()?;
            ereport(elevel)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "cannot specify logical replication slot \"{name}\" in parameter \"synchronized_standby_slots\""
                ))
                .errdetail(format!(
                    "Logical replication is waiting for correction on replication slot \"{name}\"."
                ))
                .errhint(format!(
                    "Remove the logical replication slot \"{name}\" from parameter \"synchronized_standby_slots\"."
                ))
                .finish(loc(2965, "StandbySlotsHaveCaughtup"))?;
            broke = true;
            break;
        }

        // SAFETY: take the per-slot mutex.
        let sm = unsafe { slot_mut(slot) };
        spin_acquire(sm);
        let restart_lsn = sm.data.restart_lsn;
        let invalidated = sm.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE;
        let inactive = sm.active_pid == 0;
        spin_release(sm);

        if invalidated {
            unlock_control()?;
            ereport(elevel)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "physical replication slot \"{name}\" specified in parameter \"synchronized_standby_slots\" has been invalidated"
                ))
                .errdetail(format!(
                    "Logical replication is waiting on the standby associated with replication slot \"{name}\"."
                ))
                .errhint(format!(
                    "Drop and recreate the replication slot \"{name}\", or amend parameter \"synchronized_standby_slots\"."
                ))
                .finish(loc(2985, "StandbySlotsHaveCaughtup"))?;
            broke = true;
            break;
        }

        if restart_lsn == 0 || restart_lsn < wait_for_lsn {
            if inactive {
                ereport(elevel)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "replication slot \"{name}\" specified in parameter \"synchronized_standby_slots\" does not have active_pid"
                    ))
                    .errdetail(format!(
                        "Logical replication is waiting on the standby associated with replication slot \"{name}\"."
                    ))
                    .errhint(format!(
                        "Start the standby associated with the replication slot \"{name}\", or amend parameter \"synchronized_standby_slots\"."
                    ))
                    .finish(loc(3000, "StandbySlotsHaveCaughtup"))?;
            }
            // Current slot hasn't caught up.
            unlock_control()?;
            broke = true;
            break;
        }

        debug_assert!(restart_lsn >= wait_for_lsn);

        if min_restart_lsn == 0 || min_restart_lsn > restart_lsn {
            min_restart_lsn = restart_lsn;
        }
        caught_up_slot_num += 1;
    }

    if !broke {
        unlock_control()?;
    }

    if caught_up_slot_num != nslotnames {
        return Ok(false);
    }

    SS_OLDEST_FLUSH_LSN.with(|c| c.set(min_restart_lsn));
    Ok(true)
}

/// `void WaitForStandbyConfirmation(XLogRecPtr wait_for_lsn)` (slot.c:3049).
pub fn WaitForStandbyConfirmation(wait_for_lsn: XLogRecPtr) -> PgResult<()> {
    // No wait needed unless the acquired slot is a logical failover slot and
    // synchronized_standby_slots is set.
    let slot = my_replication_slot().expect("MyReplicationSlot must be set");
    // SAFETY: we own the slot.
    let failover = unsafe { slot_ref(slot) }.data.failover;
    let have_config = SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| c.borrow().is_some());
    if !failover || !have_config {
        return Ok(());
    }

    walsender::with_wal_confirm_rcv_cv::call(&mut |cv_ref| {
        cv::condition_variable_prepare_to_sleep::call(cv_ref);
    });

    loop {
        postgres_seams::check_for_interrupts::call()?;

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file_seams::process_config_file::call(
                types_guc::GucContext::PGC_SIGHUP,
            )?;
        }

        // Exit if done waiting for every slot.
        if StandbySlotsHaveCaughtup(wait_for_lsn, WARNING)? {
            break;
        }

        // Wait with a 1s timeout so we can also re-check the GUC.
        let mut res = Ok(false);
        walsender::with_wal_confirm_rcv_cv::call(&mut |cv_ref| {
            res = cv::condition_variable_timed_sleep::call(
                cv_ref,
                1000,
                wait_event::WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION,
            );
        });
        res?;
    }

    cv::condition_variable_cancel_sleep::call();
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// `MyReplicationSlot` field accessors — slot.c owns the per-backend acquired
// slot; logical decoding (`logical.c`) reads/writes these fields under the
// per-slot spinlock. Each is installed as a seam.
// ---------------------------------------------------------------------------

/// Borrow `MyReplicationSlot` immutably. SAFETY: per slot.c's locking model.
fn my_slot_ref() -> &'static ReplicationSlot {
    let idx = my_replication_slot().expect("MyReplicationSlot must be set");
    unsafe { slot_ref(idx) }
}

/// Borrow `MyReplicationSlot` mutably. SAFETY: per slot.c's locking model.
#[allow(clippy::mut_from_ref)]
fn my_slot_mut() -> &'static mut ReplicationSlot {
    let idx = my_replication_slot().expect("MyReplicationSlot must be set");
    unsafe { slot_mut(idx) }
}

fn seam_my_replication_slot_is_set() -> bool {
    my_replication_slot().is_some()
}
fn seam_slot_is_physical() -> bool {
    SlotIsPhysical(my_slot_ref())
}
fn seam_slot_database() -> Oid {
    my_slot_ref().data.database
}
fn seam_slot_name() -> String {
    name_str_string(&my_slot_ref().data.name)
}
fn seam_slot_plugin() -> String {
    name_str_string(&my_slot_ref().data.plugin)
}
fn seam_slot_synced() -> bool {
    my_slot_ref().data.synced != 0
}
fn seam_slot_invalidated() -> ReplicationSlotInvalidationCause {
    my_slot_ref().data.invalidated
}
fn seam_slot_restart_lsn() -> XLogRecPtr {
    my_slot_ref().data.restart_lsn
}
fn seam_slot_confirmed_flush() -> XLogRecPtr {
    my_slot_ref().data.confirmed_flush
}
fn seam_slot_two_phase() -> bool {
    my_slot_ref().data.two_phase
}
fn seam_slot_failover() -> bool {
    my_slot_ref().data.failover
}
fn seam_slot_two_phase_at() -> XLogRecPtr {
    my_slot_ref().data.two_phase_at
}
fn seam_slot_xmin() -> TransactionId {
    my_slot_ref().data.xmin
}
fn seam_slot_catalog_xmin() -> TransactionId {
    my_slot_ref().data.catalog_xmin
}
fn seam_slot_candidate_xmin_lsn() -> XLogRecPtr {
    my_slot_ref().candidate_xmin_lsn
}
fn seam_slot_candidate_catalog_xmin() -> TransactionId {
    my_slot_ref().candidate_catalog_xmin
}
fn seam_slot_candidate_restart_lsn() -> XLogRecPtr {
    my_slot_ref().candidate_restart_lsn
}
fn seam_slot_candidate_restart_valid() -> XLogRecPtr {
    my_slot_ref().candidate_restart_valid
}
fn seam_slot_mutex_acquire() {
    spin_acquire(my_slot_ref());
}
fn seam_slot_mutex_release() {
    spin_release(my_slot_ref());
}
fn seam_slot_set_plugin(plugin: String) {
    my_slot_mut().data.plugin = namestrcpy(&plugin);
}
fn seam_slot_set_restart_lsn(lsn: XLogRecPtr) {
    my_slot_mut().data.restart_lsn = lsn;
}
fn seam_slot_set_effective_catalog_xmin(xid: TransactionId) {
    my_slot_mut().effective_catalog_xmin = xid;
}
fn seam_slot_set_catalog_xmin(xid: TransactionId) {
    my_slot_mut().data.catalog_xmin = xid;
}
fn seam_slot_set_xmin(xid: TransactionId) {
    my_slot_mut().data.xmin = xid;
}
fn seam_slot_set_effective_xmin(xid: TransactionId) {
    my_slot_mut().effective_xmin = xid;
}
fn seam_slot_set_confirmed_flush(lsn: XLogRecPtr) {
    my_slot_mut().data.confirmed_flush = lsn;
}
fn seam_slot_set_two_phase(value: bool) {
    my_slot_mut().data.two_phase = value;
}
fn seam_slot_set_two_phase_at(lsn: XLogRecPtr) {
    my_slot_mut().data.two_phase_at = lsn;
}
fn seam_slot_set_candidate_catalog_xmin(xid: TransactionId) {
    my_slot_mut().candidate_catalog_xmin = xid;
}
fn seam_slot_set_candidate_xmin_lsn(lsn: XLogRecPtr) {
    my_slot_mut().candidate_xmin_lsn = lsn;
}
fn seam_slot_set_candidate_restart_lsn(lsn: XLogRecPtr) {
    my_slot_mut().candidate_restart_lsn = lsn;
}
fn seam_slot_set_candidate_restart_valid(lsn: XLogRecPtr) {
    my_slot_mut().candidate_restart_valid = lsn;
}
fn seam_replication_slot_control_lock_acquire_exclusive() {
    lock_control(LWLockMode::LW_EXCLUSIVE).expect("ReplicationSlotControlLock acquire");
}
fn seam_replication_slot_control_lock_release() {
    unlock_control().expect("ReplicationSlotControlLock release");
}
fn seam_pgstat_report_replslot(stats: types_logical::ReorderBufferStats) {
    // logical.c calls `pgstat_report_replslot(ctx->slot, ...)`, which during
    // decoding is `MyReplicationSlot`; forward to pgstat by its array index.
    let idx = my_replication_slot().expect("MyReplicationSlot must be set");
    pgstat_replslot::pgstat_report_replslot::call(idx as i32, stats);
}

// `MyReplicationSlot` field mutators/accessors additionally needed by the
// slotsync consumer.
fn seam_slot_persistency() -> ReplicationSlotPersistency {
    my_slot_ref().data.persistency
}
fn seam_slot_set_invalidated(cause: ReplicationSlotInvalidationCause) {
    my_slot_mut().data.invalidated = cause;
}
fn seam_slot_set_database(dbid: Oid) {
    my_slot_mut().data.database = dbid;
}
fn seam_slot_set_failover(value: bool) {
    my_slot_mut().data.failover = value;
}

// ---------------------------------------------------------------------------
// By-`ReplicationSlotHandle` accessors — slot.c owns the shared
// `ReplicationSlotCtl->replication_slots[]` array; the slotsync array scan
// reaches non-`MyReplicationSlot` slots through these. The handle is the array
// index (`ReplicationSlotIndex`). Callers hold `ReplicationSlotControlLock`
// (shared) and/or the per-slot spinlock per slot.c's locking model.
// ---------------------------------------------------------------------------

/// Borrow `&replication_slots[handle]`. SAFETY: per slot.c's locking model
/// (caller holds ControlLock and/or the slot's mutex).
fn handle_ref(handle: ReplicationSlotHandle) -> &'static ReplicationSlot {
    unsafe { slot_ref(handle.0 as usize) }
}
#[allow(clippy::mut_from_ref)]
fn handle_mut(handle: ReplicationSlotHandle) -> &'static mut ReplicationSlot {
    unsafe { slot_mut(handle.0 as usize) }
}

fn seam_replication_slot(i: i32) -> ReplicationSlotHandle {
    ReplicationSlotHandle(i)
}
fn seam_search_named_replication_slot(
    name: &str,
    need_lock: bool,
) -> PgResult<ReplicationSlotHandle> {
    Ok(match SearchNamedReplicationSlot(name, need_lock)? {
        Some(i) => ReplicationSlotHandle(i as i32),
        None => ReplicationSlotHandle::NONE,
    })
}
/// Boolean-existence adapter over `SearchNamedReplicationSlot` for the
/// `SearchNamedReplicationSlot(name, need_lock) -> PgResult<bool>` seam that
/// `genfile.c::pg_ls_replslotdir` consumes (it only needs "does a slot with
/// this name exist?", not the slot itself).
fn seam_search_named_replication_slot_exists(
    name: &str,
    need_lock: bool,
) -> PgResult<bool> {
    Ok(SearchNamedReplicationSlot(name, need_lock)?.is_some())
}
fn seam_replication_slot_set_inactive_since(
    handle: ReplicationSlotHandle,
    now: TimestampTz,
    acquire_lock: bool,
) {
    ReplicationSlotSetInactiveSince(handle_mut(handle), now, acquire_lock);
}
fn seam_handle_spin_acquire(handle: ReplicationSlotHandle) {
    spin_acquire(handle_ref(handle));
}
fn seam_handle_spin_release(handle: ReplicationSlotHandle) {
    spin_release(handle_ref(handle));
}
fn seam_slot_in_use(handle: ReplicationSlotHandle) -> bool {
    handle_ref(handle).in_use
}
fn seam_slot_is_logical(handle: ReplicationSlotHandle) -> bool {
    SlotIsLogical(handle_ref(handle))
}
fn seam_slot_data_synced(handle: ReplicationSlotHandle) -> bool {
    handle_ref(handle).data.synced != 0
}
fn seam_slot_data_name(handle: ReplicationSlotHandle) -> String {
    name_str_string(&handle_ref(handle).data.name)
}
fn seam_slot_data_database(handle: ReplicationSlotHandle) -> Oid {
    handle_ref(handle).data.database
}
fn seam_slot_data_restart_lsn(handle: ReplicationSlotHandle) -> XLogRecPtr {
    handle_ref(handle).data.restart_lsn
}
fn seam_slot_active_pid(handle: ReplicationSlotHandle) -> i32 {
    handle_ref(handle).active_pid
}
fn seam_slot_data_invalidated(handle: ReplicationSlotHandle) -> ReplicationSlotInvalidationCause {
    handle_ref(handle).data.invalidated
}

/// Install every seam in `backend-replication-slot-seams`.
pub fn init_seams() {
    use slot_seams as s;
    s::replication_slot_validate_name_internal::set(ReplicationSlotValidateNameInternal);
    s::replication_slots_shmem_init::set(ReplicationSlotsShmemInit);
    s::replication_slots_shmem_size::set(|| Ok(ReplicationSlotsShmemSize()));
    s::replication_slot_initialize::set(ReplicationSlotInitialize);
    s::replication_slot_create::set(ReplicationSlotCreate);
    s::replication_slot_acquire::set(ReplicationSlotAcquire);
    s::replication_slot_release::set(ReplicationSlotRelease);
    s::replication_slot_cleanup::set(ReplicationSlotCleanup);
    s::replication_slot_drop::set(ReplicationSlotDrop);
    s::replication_slot_alter::set(ReplicationSlotAlter);
    s::replication_slot_drop_acquired::set(ReplicationSlotDropAcquired);
    s::replication_slot_save::set(ReplicationSlotSave);
    s::replication_slot_mark_dirty::set(ReplicationSlotMarkDirty);
    s::replication_slot_persist::set(ReplicationSlotPersist);
    s::replication_slots_compute_required_xmin::set(ReplicationSlotsComputeRequiredXmin);
    s::replication_slots_compute_required_lsn::set(ReplicationSlotsComputeRequiredLSN);
    s::replication_slots_compute_logical_restart_lsn::set(ReplicationSlotsComputeLogicalRestartLSN);
    s::replication_slots_count_db_slots::set(ReplicationSlotsCountDBSlots);
    s::replication_slots_drop_db_slots::set(ReplicationSlotsDropDBSlots);
    s::check_slot_requirements::set(CheckSlotRequirements);
    s::check_slot_permissions::set(CheckSlotPermissions);
    s::replication_slot_reserve_wal::set(ReplicationSlotReserveWal);
    s::invalidate_obsolete_replication_slots::set(InvalidateObsoleteReplicationSlots);
    s::checkpoint_replication_slots::set(CheckPointReplicationSlots);
    s::startup_replication_slots::set(StartupReplicationSlots);
    s::get_slot_invalidation_cause::set(GetSlotInvalidationCause);
    s::get_slot_invalidation_cause_name::set(GetSlotInvalidationCauseName);
    s::slot_exists_in_sync_standby_slots::set(SlotExistsInSyncStandbySlots);
    s::standby_slots_have_caughtup::set(StandbySlotsHaveCaughtup);
    s::wait_for_standby_confirmation::set(WaitForStandbyConfirmation);
    s::replication_slot_name::set(|index| {
        ReplicationSlotName(index).expect("ReplicationSlotName under ControlLock")
    });

    // MyReplicationSlot field accessors (logical decoding).
    s::my_replication_slot_is_set::set(seam_my_replication_slot_is_set);
    s::slot_is_physical::set(seam_slot_is_physical);
    s::slot_database::set(seam_slot_database);
    s::slot_name::set(seam_slot_name);
    s::slot_plugin::set(seam_slot_plugin);
    s::slot_synced::set(seam_slot_synced);
    s::slot_invalidated::set(seam_slot_invalidated);
    s::slot_restart_lsn::set(seam_slot_restart_lsn);
    s::slot_confirmed_flush::set(seam_slot_confirmed_flush);
    s::slot_two_phase::set(seam_slot_two_phase);
    s::slot_failover::set(seam_slot_failover);
    s::slot_two_phase_at::set(seam_slot_two_phase_at);
    s::slot_xmin::set(seam_slot_xmin);
    s::slot_catalog_xmin::set(seam_slot_catalog_xmin);
    s::slot_candidate_xmin_lsn::set(seam_slot_candidate_xmin_lsn);
    s::slot_candidate_catalog_xmin::set(seam_slot_candidate_catalog_xmin);
    s::slot_candidate_restart_lsn::set(seam_slot_candidate_restart_lsn);
    s::slot_candidate_restart_valid::set(seam_slot_candidate_restart_valid);
    s::slot_mutex_acquire::set(seam_slot_mutex_acquire);
    s::slot_mutex_release::set(seam_slot_mutex_release);
    s::slot_set_plugin::set(seam_slot_set_plugin);
    s::slot_set_restart_lsn::set(seam_slot_set_restart_lsn);
    s::slot_set_effective_catalog_xmin::set(seam_slot_set_effective_catalog_xmin);
    s::slot_set_catalog_xmin::set(seam_slot_set_catalog_xmin);
    s::slot_set_xmin::set(seam_slot_set_xmin);
    s::slot_set_effective_xmin::set(seam_slot_set_effective_xmin);
    s::slot_set_confirmed_flush::set(seam_slot_set_confirmed_flush);
    s::slot_set_two_phase::set(seam_slot_set_two_phase);
    s::slot_set_two_phase_at::set(seam_slot_set_two_phase_at);
    s::slot_set_candidate_catalog_xmin::set(seam_slot_set_candidate_catalog_xmin);
    s::slot_set_candidate_xmin_lsn::set(seam_slot_set_candidate_xmin_lsn);
    s::slot_set_candidate_restart_lsn::set(seam_slot_set_candidate_restart_lsn);
    s::slot_set_candidate_restart_valid::set(seam_slot_set_candidate_restart_valid);
    s::replication_slot_control_lock_acquire_exclusive::set(
        seam_replication_slot_control_lock_acquire_exclusive,
    );
    s::replication_slot_control_lock_release::set(seam_replication_slot_control_lock_release);
    // INJECTION_POINT("logical-replication-slot-advance-segment", NULL), fired
    // only when the slot's restart_lsn crosses a WAL segment boundary
    // (046_checkpoint_logical_slot attaches a 'wait' here).
    s::maybe_injection_point_slot_advance_segment::set(|old_restart_lsn, new_restart_lsn| {
        let wal_segsz = xlog::wal_segment_size::call();
        let seg1 = xl_byte_to_seg(old_restart_lsn, wal_segsz);
        let seg2 = xl_byte_to_seg(new_restart_lsn, wal_segsz);
        if seg1 != seg2 {
            let _ = injection_point_seams::injection_point_run::call(
                "logical-replication-slot-advance-segment",
                None,
            );
        }
    });
    s::pgstat_report_replslot::set(seam_pgstat_report_replslot);

    // MyReplicationSlot field accessors/mutators for the slotsync consumer.
    s::slot_persistency::set(seam_slot_persistency);
    s::slot_set_invalidated::set(seam_slot_set_invalidated);
    s::slot_set_database::set(seam_slot_set_database);
    s::slot_set_failover::set(seam_slot_set_failover);

    // By-ReplicationSlotHandle array surface for the slotsync consumer.
    s::max_replication_slots::set(max_replication_slots);
    s::replication_slot::set(seam_replication_slot);
    s::search_named_replication_slot::set(seam_search_named_replication_slot);
    s::replication_slot_set_inactive_since::set(seam_replication_slot_set_inactive_since);
    s::slot_spin_acquire::set(seam_handle_spin_acquire);
    s::slot_spin_release::set(seam_handle_spin_release);
    s::slot_in_use::set(seam_slot_in_use);
    s::slot_is_logical::set(seam_slot_is_logical);
    s::slot_data_synced::set(seam_slot_data_synced);
    s::slot_data_name::set(seam_slot_data_name);
    s::slot_data_database::set(seam_slot_data_database);
    s::slot_data_restart_lsn::set(seam_slot_data_restart_lsn);
    s::slot_active_pid::set(seam_slot_active_pid);
    s::slot_data_invalidated::set(seam_slot_data_invalidated);

    // Boolean-existence form consumed by genfile.c::pg_ls_replslotdir.
    s::SearchNamedReplicationSlot::set(seam_search_named_replication_slot_exists);

    // GUC variable accessors — `int max_replication_slots`,
    // `int idle_replication_slot_timeout_secs`, and the raw
    // `char *synchronized_standby_slots` string all live in slot.c, so this
    // owner installs the guc-tables slots' get/set accessors over the backing
    // storage above (mirroring C's `conf->variable` pointer). They are read
    // directly from the GUC slot (not the ControlFile).
    {
        use ::guc_tables::vars;
        use ::guc_tables::GucVarAccessors;
        vars::max_replication_slots.install(GucVarAccessors {
            get: max_replication_slots,
            set: max_replication_slots_set,
        });
        vars::idle_replication_slot_timeout_secs.install(GucVarAccessors {
            get: idle_replication_slot_timeout_secs,
            set: idle_replication_slot_timeout_secs_set,
        });
        vars::synchronized_standby_slots.install(GucVarAccessors {
            get: synchronized_standby_slots_get,
            set: synchronized_standby_slots_set,
        });
    }
}
