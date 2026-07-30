#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod ondisk;
#[cfg(test)]
mod tests;

pub use ondisk::*;

use core::mem::size_of;
use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::OnceLock;

use adt_timestamp::GetCurrentTimestamp;
use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use elog::{elog, ereport, errno};
use init_small::globals as g;
use lwlock::{LWLock, LWLockAcquire, LWLockPadded, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{
    InvalidOid, InvalidTransactionId, Oid, TimestampTz, TransactionId, TransactionIdIsValid,
    TransactionIdPrecedes, XLogRecPtr, XLogSegNo, NAMEDATALEN,
};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, SqlState, DEBUG1, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
    ERRCODE_DATA_CORRUPTED, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NAME_TOO_LONG, ERRCODE_OBJECT_IN_USE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_OBJECT, ERROR, FATAL, LOG, PANIC, WARNING,
};
use types_guc::GucSource;
use types_storage::storage::{
    Spinlock, SyncCell, LWTRANCHE_REPLICATION_SLOT_IO, PROC_IN_LOGICAL_DECODING,
    REPLICATION_SLOT_ALLOCATION_LOCK, REPLICATION_SLOT_CONTROL_LOCK,
};
use types_tuple::NameData;

pub const PG_REPLSLOT_DIR: &str = "pg_replslot";

// wait_event_names.txt IPC section index of ReplicationSlotDrop.
const WAIT_EVENT_REPLICATION_SLOT_DROP: u32 = 0x0800_0000 + 49;

const InvalidXLogRecPtr: XLogRecPtr = 0;

fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

pub struct ReplicationSlot {
    pub mutex: Spinlock,
    pub in_use: SyncCell<bool>,
    pub active_pid: SyncCell<i32>,
    pub just_dirtied: SyncCell<bool>,
    pub dirty: SyncCell<bool>,
    pub effective_xmin: SyncCell<TransactionId>,
    pub effective_catalog_xmin: SyncCell<TransactionId>,
    pub data: SyncCell<ReplicationSlotPersistentData>,
    pub io_in_progress_lock: LWLock,
    pub active_cv: ConditionVariable,
    pub candidate_catalog_xmin: SyncCell<TransactionId>,
    pub candidate_xmin_lsn: SyncCell<XLogRecPtr>,
    pub candidate_restart_valid: SyncCell<XLogRecPtr>,
    pub candidate_restart_lsn: SyncCell<XLogRecPtr>,
    pub last_saved_confirmed_flush: SyncCell<XLogRecPtr>,
    pub inactive_since: SyncCell<TimestampTz>,
    pub last_saved_restart_lsn: SyncCell<XLogRecPtr>,
}

impl ReplicationSlot {
    // SpinLockAcquire(&slot->mutex) .. SpinLockRelease pairing.
    pub fn with_mutex<R>(&self, f: impl FnOnce() -> R) -> R {
        if self.mutex.tas() != 0 {
            let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "mutex");
            while self.mutex.tas_spin() != 0 {
                s_lock_seams::perform_spin_delay::call(&mut delay);
            }
            s_lock_seams::finish_spin_delay::call(&delay);
        }
        let r = f();
        self.mutex.unlock();
        r
    }
}

pub fn SlotIsPhysical(slot: &ReplicationSlot) -> bool {
    slot.data.get().database == InvalidOid
}

pub fn SlotIsLogical(slot: &ReplicationSlot) -> bool {
    slot.data.get().database != InvalidOid
}

pub fn ReplicationSlotSetInactiveSince(s: &ReplicationSlot, ts: TimestampTz, acquire_lock: bool) {
    let set = || {
        if s.data.get().invalidated == RS_INVAL_NONE {
            s.inactive_since.set(ts);
        }
    };
    if acquire_lock {
        s.with_mutex(set);
    } else {
        set();
    }
}

// Raw pointer (not &'static): the crash-cycle reset rewrites slots in place,
// which requires write provenance.
struct SlotArrayPtr {
    ptr: *mut ReplicationSlot,
    len: usize,
}

// SAFETY: points at a leaked boot allocation; cross-thread access follows
// slot.c's locking model, in-place writes happen only with children dead.
unsafe impl Send for SlotArrayPtr {}
unsafe impl Sync for SlotArrayPtr {}

static REPLICATION_SLOT_CTL: OnceLock<SlotArrayPtr> = OnceLock::new();

pub fn ReplicationSlotCtl() -> &'static [ReplicationSlot] {
    match REPLICATION_SLOT_CTL.get() {
        // SAFETY: leaked boot allocation, valid for the process lifetime.
        Some(a) => unsafe { std::slice::from_raw_parts(a.ptr, a.len) },
        None if walsender_config::max_replication_slots() == 0 => &[],
        None => panic!("ReplicationSlotsShmemInit has not run"),
    }
}

thread_local! {
    static MY_REPLICATION_SLOT: Cell<Option<&'static ReplicationSlot>> = const { Cell::new(None) };
    static IDLE_REPLICATION_SLOT_TIMEOUT_SECS: Cell<i32> = const { Cell::new(0) };
    static SYNCHRONIZED_STANDBY_SLOTS_RAW: RefCell<Option<String>> = const { RefCell::new(None) };
    static SYNCHRONIZED_STANDBY_SLOTS_CONFIG: RefCell<Option<SyncStandbySlotsConfig>> =
        const { RefCell::new(None) };
    static SS_OLDEST_FLUSH_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
}

pub fn MyReplicationSlot() -> Option<&'static ReplicationSlot> {
    MY_REPLICATION_SLOT.get()
}

thread_local! {
    // slotsync.c's `syncing_slots` (thread = process here). STORAGE lives in
    // this crate so ReplicationSlotCreate can consult it without a dependency
    // on slotsync; slotsync owns all writes.
    static SYNCING_REPLICATION_SLOTS: Cell<bool> = const { Cell::new(false) };
}

/// IsSyncingReplicationSlots() for this thread (see slotsync).
pub fn syncing_replication_slots() -> bool {
    SYNCING_REPLICATION_SLOTS.get()
}

pub fn set_syncing_replication_slots(v: bool) {
    SYNCING_REPLICATION_SLOTS.set(v);
}

pub fn SetMyReplicationSlot(slot: Option<&'static ReplicationSlot>) {
    MY_REPLICATION_SLOT.set(slot);
}

pub fn idle_replication_slot_timeout_secs() -> i32 {
    IDLE_REPLICATION_SLOT_TIMEOUT_SECS.get()
}

fn allocation_lock() -> &'static LWLock {
    lwlock::main_lock(REPLICATION_SLOT_ALLOCATION_LOCK)
}

fn control_lock() -> &'static LWLock {
    lwlock::main_lock(REPLICATION_SLOT_CONTROL_LOCK)
}

fn lw(lock: &'static LWLock, mode: lwlock::LWLockMode) -> PgResult<()> {
    LWLockAcquire(lock, mode, g::MyProcNumber())?;
    Ok(())
}

fn name_string(nd: &NameData) -> String {
    String::from_utf8_lossy(nd.name_str()).into_owned()
}

fn cstring(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).expect("no NUL in slot path")
}

fn c_rename(old: &str, new: &str) -> i32 {
    // SAFETY: NUL-terminated paths.
    unsafe { libc::rename(cstring(old).as_ptr(), cstring(new).as_ptr()) }
}

fn c_unlink(path: &str) -> i32 {
    // SAFETY: NUL-terminated path.
    unsafe { libc::unlink(cstring(path).as_ptr()) }
}

fn initial_slot() -> ReplicationSlot {
    ReplicationSlot {
        mutex: Spinlock::new(),
        in_use: SyncCell::new(false),
        active_pid: SyncCell::new(0),
        just_dirtied: SyncCell::new(false),
        dirty: SyncCell::new(false),
        effective_xmin: SyncCell::new(InvalidTransactionId),
        effective_catalog_xmin: SyncCell::new(InvalidTransactionId),
        data: SyncCell::new(ReplicationSlotPersistentData::default()),
        io_in_progress_lock: LWLockPadded::new_unlocked(LWTRANCHE_REPLICATION_SLOT_IO).lock,
        active_cv: ConditionVariable::new(),
        candidate_catalog_xmin: SyncCell::new(InvalidTransactionId),
        candidate_xmin_lsn: SyncCell::new(InvalidXLogRecPtr),
        candidate_restart_valid: SyncCell::new(InvalidXLogRecPtr),
        candidate_restart_lsn: SyncCell::new(InvalidXLogRecPtr),
        last_saved_confirmed_flush: SyncCell::new(InvalidXLogRecPtr),
        inactive_since: SyncCell::new(0),
        last_saved_restart_lsn: SyncCell::new(InvalidXLogRecPtr),
    }
}

pub fn ReplicationSlotsShmemSize() -> usize {
    let n = walsender_config::max_replication_slots();
    if n == 0 {
        0
    } else {
        n as usize * size_of::<ReplicationSlot>()
    }
}

pub fn ReplicationSlotsShmemInit() {
    let n = walsender_config::max_replication_slots();
    if n == 0 {
        return;
    }
    let slots: Box<[ReplicationSlot]> = (0..n).map(|_| initial_slot()).collect();
    let len = slots.len();
    let ptr = Box::into_raw(slots) as *mut ReplicationSlot;
    if REPLICATION_SLOT_CTL.set(SlotArrayPtr { ptr, len }).is_err() {
        panic!("ReplicationSlotsShmemInit already ran");
    }
}

pub fn ReplicationSlotsShmemResetAfterCrash() {
    let Some(a) = REPLICATION_SLOT_CTL.get() else {
        return;
    };
    for i in 0..a.len {
        // SAFETY: crash-cycle reset on the postmaster thread with every
        // backend thread dead; no concurrent access exists.
        unsafe { std::ptr::write(a.ptr.add(i), initial_slot()) };
    }
    SS_OLDEST_FLUSH_LSN.set(InvalidXLogRecPtr);
}

pub fn ReplicationSlotInitialize() -> PgResult<()> {
    ipc_seams::before_shmem_exit::call(ReplicationSlotShmemExit, datum::Datum::from_usize(0))
}

fn ReplicationSlotShmemExit(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    if MyReplicationSlot().is_some() {
        ReplicationSlotRelease()?;
    }
    ReplicationSlotCleanup(false)
}

pub fn ReplicationSlotValidateName(name: &str, elevel: ErrorLevel) -> PgResult<bool> {
    if let Err((code, msg, hint)) = ReplicationSlotValidateNameInternal(name) {
        let mut b = ereport(elevel).errcode(code).errmsg_internal(msg);
        if let Some(hint) = hint {
            b = b.errhint_internal(hint);
        }
        b.finish(loc("ReplicationSlotValidateName"))?;
        return Ok(false);
    }
    Ok(true)
}

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
    for c in name.bytes() {
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_') {
            return Err((
                ERRCODE_INVALID_NAME,
                format!("replication slot name \"{name}\" contains invalid character"),
                Some(
                    "Replication slot names may only contain lower case letters, numbers, and \
                     the underscore character."
                        .into(),
                ),
            ));
        }
    }
    Ok(())
}

pub fn ReplicationSlotCreate(
    name: &str,
    db_specific: bool,
    persistency: ReplicationSlotPersistency,
    two_phase: bool,
    failover: bool,
    synced: bool,
) -> PgResult<()> {
    assert!(MyReplicationSlot().is_none());

    ReplicationSlotValidateName(name, ERROR)?;

    if failover {
        // C: failover on a standby is only allowed for the slot sync
        // machinery itself (IsSyncingReplicationSlots()).
        if transam_xlog::RecoveryInProgress() && !syncing_replication_slots() {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a replication slot created on the standby")
                .finish(loc("ReplicationSlotCreate"));
        }
        // Failover-enabled temporary slots are only allowed during slot
        // synchronization (slotsync creates RS_TEMPORARY synced slots that
        // must retain the remote's failover flag).
        if persistency == RS_TEMPORARY && !syncing_replication_slots() {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a temporary replication slot")
                .finish(loc("ReplicationSlotCreate"));
        }
    }

    lw(allocation_lock(), LW_EXCLUSIVE)?;
    lw(control_lock(), LW_SHARED)?;
    let mut slot: Option<&'static ReplicationSlot> = None;
    for s in ReplicationSlotCtl() {
        if s.in_use.get() && s.data.get().name.name_str() == name.as_bytes() {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("replication slot \"{name}\" already exists"))
                .finish(loc("ReplicationSlotCreate"));
        }
        if !s.in_use.get() && slot.is_none() {
            slot = Some(s);
        }
    }
    LWLockRelease(control_lock())?;

    let Some(slot) = slot else {
        return ereport(ERROR)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg("all replication slots are in use")
            .errhint("Free one or increase \"max_replication_slots\".")
            .finish(loc("ReplicationSlotCreate"));
    };

    assert!(!slot.in_use.get());
    assert!(slot.active_pid.get() == 0);

    let mut d = ReplicationSlotPersistentData::default();
    d.name.namestrcpy(name);
    d.database = if db_specific { g::MyDatabaseId() } else { InvalidOid };
    d.persistency = persistency;
    d.two_phase = two_phase;
    d.two_phase_at = InvalidXLogRecPtr;
    d.failover = failover;
    d.synced = synced as u8;
    slot.data.set(d);

    slot.just_dirtied.set(false);
    slot.dirty.set(false);
    slot.effective_xmin.set(InvalidTransactionId);
    slot.effective_catalog_xmin.set(InvalidTransactionId);
    slot.candidate_catalog_xmin.set(InvalidTransactionId);
    slot.candidate_xmin_lsn.set(InvalidXLogRecPtr);
    slot.candidate_restart_valid.set(InvalidXLogRecPtr);
    slot.candidate_restart_lsn.set(InvalidXLogRecPtr);
    slot.last_saved_confirmed_flush.set(InvalidXLogRecPtr);
    slot.last_saved_restart_lsn.set(InvalidXLogRecPtr);
    slot.inactive_since.set(0);

    CreateSlotOnDisk(slot)?;

    lw(control_lock(), LW_EXCLUSIVE)?;
    slot.in_use.set(true);
    slot.with_mutex(|| {
        assert!(slot.active_pid.get() == 0);
        slot.active_pid.set(g::MyProcPid());
    });
    SetMyReplicationSlot(Some(slot));
    LWLockRelease(control_lock())?;

    // Stats are only collected for logical slots.
    if SlotIsLogical(slot) {
        pgstat::replslot::pgstat_create_replslot(ReplicationSlotIndex(slot));
    }

    LWLockRelease(allocation_lock())?;
    ConditionVariableBroadcast(&slot.active_cv);
    Ok(())
}

pub fn SearchNamedReplicationSlot(
    name: &str,
    need_lock: bool,
) -> PgResult<Option<&'static ReplicationSlot>> {
    if need_lock {
        lw(control_lock(), LW_SHARED)?;
    }
    let mut slot = None;
    for s in ReplicationSlotCtl() {
        if s.in_use.get() && s.data.get().name.name_str() == name.as_bytes() {
            slot = Some(s);
            break;
        }
    }
    if need_lock {
        LWLockRelease(control_lock())?;
    }
    Ok(slot)
}

pub fn ReplicationSlotIndex(slot: &ReplicationSlot) -> i32 {
    let slots = ReplicationSlotCtl();
    let base = slots.as_ptr() as usize;
    let p = std::ptr::from_ref(slot) as usize;
    assert!(p >= base && p < base + slots.len() * size_of::<ReplicationSlot>());
    ((p - base) / size_of::<ReplicationSlot>()) as i32
}

pub fn ReplicationSlotName(index: i32) -> PgResult<Option<NameData>> {
    let slot = &ReplicationSlotCtl()[index as usize];
    lw(control_lock(), LW_SHARED)?;
    let found = if slot.in_use.get() {
        Some(slot.data.get().name)
    } else {
        None
    };
    LWLockRelease(control_lock())?;
    Ok(found)
}

pub fn ReplicationSlotAcquire(name: &str, nowait: bool, error_if_invalid: bool) -> PgResult<()> {
    loop {
        assert!(MyReplicationSlot().is_none());

        lw(control_lock(), LW_SHARED)?;
        let s = SearchNamedReplicationSlot(name, false)?;
        let Some(s) = s.filter(|s| s.in_use.get()) else {
            LWLockRelease(control_lock())?;
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("replication slot \"{name}\" does not exist"))
                .finish(loc("ReplicationSlotAcquire"));
        };

        let active_pid;
        if g::IsUnderPostmaster() {
            if !nowait {
                ConditionVariablePrepareToSleep(&s.active_cv);
            }
            active_pid = s.with_mutex(|| {
                if s.active_pid.get() == 0 {
                    s.active_pid.set(g::MyProcPid());
                }
                ReplicationSlotSetInactiveSince(s, 0, false);
                s.active_pid.get()
            });
        } else {
            s.active_pid.set(g::MyProcPid());
            active_pid = g::MyProcPid();
            ReplicationSlotSetInactiveSince(s, 0, true);
        }
        LWLockRelease(control_lock())?;

        if active_pid != g::MyProcPid() {
            if !nowait {
                ConditionVariableSleep(&s.active_cv, WAIT_EVENT_REPLICATION_SLOT_DROP)?;
                ConditionVariableCancelSleep();
                continue;
            }
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "replication slot \"{}\" is active for PID {active_pid}",
                    name_string(&s.data.get().name)
                ))
                .finish(loc("ReplicationSlotAcquire"));
        } else if !nowait {
            ConditionVariableCancelSleep();
        }

        SetMyReplicationSlot(Some(s));

        let invalidated = s.data.get().invalidated;
        if error_if_invalid && invalidated != RS_INVAL_NONE {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "can no longer access replication slot \"{}\"",
                    name_string(&s.data.get().name)
                ))
                .errdetail(format!(
                    "This replication slot has been invalidated due to \"{}\".",
                    GetSlotInvalidationCauseName(invalidated)
                ))
                .finish(loc("ReplicationSlotAcquire"));
        }

        ConditionVariableBroadcast(&s.active_cv);

        // Protects against stats from before a restart, for a different slot,
        // being present during pgstat_report_replslot.
        if SlotIsLogical(s) {
            pgstat::replslot::pgstat_acquire_replslot(ReplicationSlotIndex(s));
        }

        // am_walsender "acquired ... slot" log line: unit unported.
        return Ok(());
    }
}

pub fn ReplicationSlotRelease() -> PgResult<()> {
    let slot = MyReplicationSlot().expect("ReplicationSlotRelease: no slot acquired");
    assert!(slot.active_pid.get() != 0);

    if slot.data.get().persistency == RS_EPHEMERAL {
        ReplicationSlotDropAcquired()?;
    }

    if !TransactionIdIsValid(slot.data.get().xmin)
        && TransactionIdIsValid(slot.effective_xmin.get())
    {
        slot.with_mutex(|| slot.effective_xmin.set(InvalidTransactionId));
        ReplicationSlotsComputeRequiredXmin(false)?;
    }

    let now = GetCurrentTimestamp();

    if slot.data.get().persistency == RS_PERSISTENT {
        slot.with_mutex(|| {
            slot.active_pid.set(0);
            ReplicationSlotSetInactiveSince(slot, now, false);
        });
        ConditionVariableBroadcast(&slot.active_cv);
    } else {
        ReplicationSlotSetInactiveSince(slot, now, true);
    }

    SetMyReplicationSlot(None);

    lw(lwlock::main_lock(procarray::PROC_ARRAY_LOCK), LW_EXCLUSIVE)?;
    let proc = lmgr_proc::GetPGProcByNumber(g::MyProcNumber());
    let flags = proc.statusFlags.load(Relaxed) & !PROC_IN_LOGICAL_DECODING;
    proc.statusFlags.store(flags, Relaxed);
    lmgr_proc::ProcGlobal().statusFlags[proc.pgxactoff.load(Relaxed) as usize]
        .store(flags, Relaxed);
    LWLockRelease(lwlock::main_lock(procarray::PROC_ARRAY_LOCK))?;

    // am_walsender "released ... slot" log line: walsender unported.
    Ok(())
}

pub fn ReplicationSlotCleanup(synced_only: bool) -> PgResult<()> {
    assert!(MyReplicationSlot().is_none());

    // No slot array (test harnesses / slot-less boots): nothing to clean.
    if REPLICATION_SLOT_CTL.get().is_none() {
        return Ok(());
    }

    'restart: loop {
        lw(control_lock(), LW_SHARED)?;
        for s in ReplicationSlotCtl() {
            if !s.in_use.get() {
                continue;
            }
            let drop_it = s.with_mutex(|| {
                s.active_pid.get() == g::MyProcPid() && (!synced_only || s.data.get().synced != 0)
            });
            if drop_it {
                assert!(s.data.get().persistency == RS_TEMPORARY);
                LWLockRelease(control_lock())?;
                ReplicationSlotDropPtr(s)?;
                ConditionVariableBroadcast(&s.active_cv);
                continue 'restart;
            }
        }
        LWLockRelease(control_lock())?;
        return Ok(());
    }
}

pub fn ReplicationSlotDrop(name: &str, nowait: bool) -> PgResult<()> {
    assert!(MyReplicationSlot().is_none());

    ReplicationSlotAcquire(name, nowait, false)?;

    if transam_xlog::RecoveryInProgress() && MyReplicationSlot().unwrap().data.get().synced != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot drop replication slot \"{name}\""))
            .errdetail("This replication slot is being synchronized from the primary server.")
            .finish(loc("ReplicationSlotDrop"));
    }

    ReplicationSlotDropAcquired()
}

pub fn ReplicationSlotAlter(
    name: &str,
    failover: Option<bool>,
    two_phase: Option<bool>,
) -> PgResult<()> {
    assert!(MyReplicationSlot().is_none());
    assert!(failover.is_some() || two_phase.is_some());

    ReplicationSlotAcquire(name, false, true)?;
    let slot = MyReplicationSlot().unwrap();

    if SlotIsPhysical(slot) {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot use ALTER_REPLICATION_SLOT with a physical replication slot")
            .finish(loc("ReplicationSlotAlter"));
    }

    if transam_xlog::RecoveryInProgress() {
        if slot.data.get().synced != 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("cannot alter replication slot \"{name}\""))
                .errdetail("This replication slot is being synchronized from the primary server.")
                .finish(loc("ReplicationSlotAlter"));
        }
        if failover == Some(true) {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a replication slot on the standby")
                .finish(loc("ReplicationSlotAlter"));
        }
    }

    let mut update_slot = false;
    if let Some(failover) = failover {
        if failover && slot.data.get().persistency == RS_TEMPORARY {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot enable failover for a temporary replication slot")
                .finish(loc("ReplicationSlotAlter"));
        }
        if slot.data.get().failover != failover {
            slot.with_mutex(|| {
                let mut d = slot.data.get();
                d.failover = failover;
                slot.data.set(d);
            });
            update_slot = true;
        }
    }

    if let Some(two_phase) = two_phase {
        if slot.data.get().two_phase != two_phase {
            slot.with_mutex(|| {
                let mut d = slot.data.get();
                d.two_phase = two_phase;
                slot.data.set(d);
            });
            update_slot = true;
        }
    }

    if update_slot {
        ReplicationSlotMarkDirty();
        ReplicationSlotSave()?;
    }

    ReplicationSlotRelease()
}

pub fn ReplicationSlotDropAcquired() -> PgResult<()> {
    let slot = MyReplicationSlot().expect("ReplicationSlotDropAcquired: no slot acquired");
    SetMyReplicationSlot(None);
    ReplicationSlotDropPtr(slot)
}

fn ReplicationSlotDropPtr(slot: &'static ReplicationSlot) -> PgResult<()> {
    lw(allocation_lock(), LW_EXCLUSIVE)?;

    let name = name_string(&slot.data.get().name);
    let path = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmppath = format!("{PG_REPLSLOT_DIR}/{name}.tmp");

    if c_rename(&path, &tmppath) == 0 {
        g::StartCriticalSection();
        fd::fsync_fname(&tmppath, true)?;
        fd::fsync_fname(PG_REPLSLOT_DIR, true)?;
        g::EndCriticalSection();
    } else {
        let save_errno = errno::current_errno();
        let fail_softly = slot.data.get().persistency != RS_PERSISTENT;
        slot.with_mutex(|| slot.active_pid.set(0));
        ConditionVariableBroadcast(&slot.active_cv);
        ereport(if fail_softly { WARNING } else { ERROR })
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not rename file \"{path}\" to \"{tmppath}\": %m"))
            .finish(loc("ReplicationSlotDropPtr"))?;
    }

    lw(control_lock(), LW_EXCLUSIVE)?;
    slot.active_pid.set(0);
    slot.in_use.set(false);
    LWLockRelease(control_lock())?;
    ConditionVariableBroadcast(&slot.active_cv);

    ReplicationSlotsComputeRequiredXmin(false)?;
    ReplicationSlotsComputeRequiredLSN()?;

    if !fd::rmtree(&tmppath, true)? {
        ereport(WARNING)
            .errmsg(format!("could not remove directory \"{tmppath}\""))
            .finish(loc("ReplicationSlotDropPtr"))?;
    }

    // Under ReplicationSlotAllocationLock so a same-named slot created in
    // another session can't lose its just-created stats entry.
    if SlotIsLogical(slot) {
        pgstat::replslot::pgstat_drop_replslot(ReplicationSlotIndex(slot));
    }

    LWLockRelease(allocation_lock())?;
    Ok(())
}

pub fn ReplicationSlotSave() -> PgResult<()> {
    let slot = MyReplicationSlot().expect("ReplicationSlotSave: no slot acquired");
    let path = format!("{PG_REPLSLOT_DIR}/{}", name_string(&slot.data.get().name));
    SaveSlotToPath(slot, &path, ERROR)
}

pub fn ReplicationSlotMarkDirty() {
    let slot = MyReplicationSlot().expect("ReplicationSlotMarkDirty: no slot acquired");
    slot.with_mutex(|| {
        slot.just_dirtied.set(true);
        slot.dirty.set(true);
    });
}

pub fn ReplicationSlotPersist() -> PgResult<()> {
    let slot = MyReplicationSlot().expect("ReplicationSlotPersist: no slot acquired");
    assert!(slot.data.get().persistency != RS_PERSISTENT);

    slot.with_mutex(|| {
        let mut d = slot.data.get();
        d.persistency = RS_PERSISTENT;
        slot.data.set(d);
    });

    ReplicationSlotMarkDirty();
    ReplicationSlotSave()
}

pub fn ReplicationSlotsComputeRequiredXmin(already_locked: bool) -> PgResult<()> {
    let mut agg_xmin = InvalidTransactionId;
    let mut agg_catalog_xmin = InvalidTransactionId;

    if !already_locked {
        lw(control_lock(), LW_SHARED)?;
    }

    for s in ReplicationSlotCtl() {
        if !s.in_use.get() {
            continue;
        }
        let (effective_xmin, effective_catalog_xmin, invalidated) = s.with_mutex(|| {
            (
                s.effective_xmin.get(),
                s.effective_catalog_xmin.get(),
                s.data.get().invalidated != RS_INVAL_NONE,
            )
        });
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

    procarray::ProcArraySetReplicationSlotXmin(agg_xmin, agg_catalog_xmin, already_locked)?;

    if !already_locked {
        LWLockRelease(control_lock())?;
    }
    Ok(())
}

// XLogSetReplicationSlotMinimumLSN (xlog.c): inlined field store — slot.c is
// its only writer and this unit cannot edit xlog.c.
fn XLogSetReplicationSlotMinimumLSN(lsn: XLogRecPtr) {
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.info_lck.with(|| ctl.replicationSlotMinLSN.store(lsn, Relaxed));
}

fn XLogGetLastRemovedSegno() -> XLogSegNo {
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.info_lck.with(|| ctl.lastRemovedSegNo.load(Relaxed))
}

pub fn ReplicationSlotsComputeRequiredLSN() -> PgResult<()> {
    let mut min_required = InvalidXLogRecPtr;

    lw(control_lock(), LW_SHARED)?;
    for s in ReplicationSlotCtl() {
        if !s.in_use.get() {
            continue;
        }
        let (persistency, mut restart_lsn, invalidated, last_saved_restart_lsn) = s.with_mutex(|| {
            let d = s.data.get();
            (
                d.persistency,
                d.restart_lsn,
                d.invalidated != RS_INVAL_NONE,
                s.last_saved_restart_lsn.get(),
            )
        });
        if invalidated {
            continue;
        }
        if persistency == RS_PERSISTENT
            && last_saved_restart_lsn != InvalidXLogRecPtr
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }
        if restart_lsn != InvalidXLogRecPtr
            && (min_required == InvalidXLogRecPtr || restart_lsn < min_required)
        {
            min_required = restart_lsn;
        }
    }
    LWLockRelease(control_lock())?;

    XLogSetReplicationSlotMinimumLSN(min_required);
    Ok(())
}

pub fn ReplicationSlotsComputeLogicalRestartLSN() -> PgResult<XLogRecPtr> {
    let mut result = InvalidXLogRecPtr;

    if walsender_config::max_replication_slots() <= 0 {
        return Ok(InvalidXLogRecPtr);
    }

    lw(control_lock(), LW_SHARED)?;
    for s in ReplicationSlotCtl() {
        if !s.in_use.get() || !SlotIsLogical(s) {
            continue;
        }
        let (persistency, mut restart_lsn, invalidated, last_saved_restart_lsn) = s.with_mutex(|| {
            let d = s.data.get();
            (
                d.persistency,
                d.restart_lsn,
                d.invalidated != RS_INVAL_NONE,
                s.last_saved_restart_lsn.get(),
            )
        });
        if invalidated {
            continue;
        }
        if persistency == RS_PERSISTENT
            && last_saved_restart_lsn != InvalidXLogRecPtr
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }
        if restart_lsn == InvalidXLogRecPtr {
            continue;
        }
        if result == InvalidXLogRecPtr || restart_lsn < result {
            result = restart_lsn;
        }
    }
    LWLockRelease(control_lock())?;

    Ok(result)
}

pub fn ReplicationSlotsCountDBSlots(dboid: Oid) -> PgResult<(bool, i32, i32)> {
    let mut nslots = 0;
    let mut nactive = 0;

    if walsender_config::max_replication_slots() <= 0 {
        return Ok((false, 0, 0));
    }

    lw(control_lock(), LW_SHARED)?;
    for s in ReplicationSlotCtl() {
        if !s.in_use.get() || !SlotIsLogical(s) || s.data.get().database != dboid {
            continue;
        }
        s.with_mutex(|| {
            nslots += 1;
            if s.active_pid.get() != 0 {
                nactive += 1;
            }
        });
    }
    LWLockRelease(control_lock())?;

    Ok((nslots > 0, nslots, nactive))
}

pub fn ReplicationSlotsDropDBSlots(dboid: Oid) -> PgResult<()> {
    if walsender_config::max_replication_slots() <= 0 {
        return Ok(());
    }

    'restart: loop {
        lw(control_lock(), LW_SHARED)?;
        for s in ReplicationSlotCtl() {
            if !s.in_use.get() || !SlotIsLogical(s) || s.data.get().database != dboid {
                continue;
            }
            let slotname = name_string(&s.data.get().name);
            let active_pid = s.with_mutex(|| {
                let pid = s.active_pid.get();
                if pid == 0 {
                    SetMyReplicationSlot(Some(s));
                    s.active_pid.set(g::MyProcPid());
                }
                pid
            });
            if active_pid != 0 {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_IN_USE)
                    .errmsg(format!(
                        "replication slot \"{slotname}\" is active for PID {active_pid}"
                    ))
                    .finish(loc("ReplicationSlotsDropDBSlots"));
            }
            LWLockRelease(control_lock())?;
            ReplicationSlotDropAcquired()?;
            continue 'restart;
        }
        LWLockRelease(control_lock())?;
        return Ok(());
    }
}

pub fn CheckSlotRequirements() -> PgResult<()> {
    if walsender_config::max_replication_slots() == 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("replication slots can only be used if \"max_replication_slots\" > 0")
            .finish(loc("CheckSlotRequirements"));
    }
    if transam_xlog::wal_level() < transam_xlog::WAL_LEVEL_REPLICA {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("replication slots can only be used if \"wal_level\" >= \"replica\"")
            .finish(loc("CheckSlotRequirements"));
    }
    Ok(())
}

pub fn CheckSlotPermissions() -> PgResult<()> {
    if !miscinit_seams::has_rolreplication::call(miscinit::GetUserId())? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to use replication slots")
            .errdetail("Only roles with the REPLICATION attribute may use replication slots.")
            .finish(loc("CheckSlotPermissions"));
    }
    Ok(())
}

pub fn ReplicationSlotReserveWal() -> PgResult<()> {
    let slot = MyReplicationSlot().expect("ReplicationSlotReserveWal: no slot acquired");
    assert!(slot.data.get().restart_lsn == InvalidXLogRecPtr);
    assert!(slot.last_saved_restart_lsn.get() == InvalidXLogRecPtr);

    lw(allocation_lock(), LW_EXCLUSIVE)?;

    let restart_lsn = if SlotIsPhysical(slot) {
        transam_xlog::GetRedoRecPtr()
    } else if transam_xlog::RecoveryInProgress() {
        xlogrecovery_seams::get_xlog_replay_rec_ptr::call().0
    } else {
        transam_xlog::GetXLogInsertRecPtr()
    };

    slot.with_mutex(|| {
        let mut d = slot.data.get();
        d.restart_lsn = restart_lsn;
        slot.data.set(d);
    });

    ReplicationSlotsComputeRequiredLSN()?;

    let segno =
        transam_xlog::XLByteToSeg(slot.data.get().restart_lsn, transam_xlog::wal_segment_size());
    if XLogGetLastRemovedSegno() >= segno {
        return elog(
            ERROR,
            format!(
                "WAL required by replication slot {} has been removed concurrently",
                name_string(&slot.data.get().name)
            ),
        );
    }

    LWLockRelease(allocation_lock())?;

    if !transam_xlog::RecoveryInProgress() && SlotIsLogical(slot) {
        let flushptr = standby_seams::log_standby_snapshot::call()?;
        transam_xlog::XLogFlush(flushptr)?;
    }
    Ok(())
}

// CanInvalidateIdleSlot (slot.c:1722).
fn CanInvalidateIdleSlot(s: &ReplicationSlot) -> bool {
    idle_replication_slot_timeout_secs() != 0
        && !XLogRecPtrIsInvalid(s.data.get().restart_lsn)
        && s.inactive_since.get() > 0
        && !(transam_xlog::RecoveryInProgress() && s.data.get().synced != 0)
}

// DetermineSlotInvalidationCause (slot.c:1740). Sequentially checks the
// possible causes, returning the first the slot is eligible for.
#[allow(clippy::too_many_arguments)]
fn DetermineSlotInvalidationCause(
    possible_causes: u32,
    s: &ReplicationSlot,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    inactive_since: &mut TimestampTz,
    now: TimestampTz,
) -> ReplicationSlotInvalidationCause {
    debug_assert!(possible_causes != RS_INVAL_NONE.0 as u32);

    if possible_causes & RS_INVAL_WAL_REMOVED.0 as u32 != 0 {
        let restart_lsn = s.data.get().restart_lsn;
        if restart_lsn != InvalidXLogRecPtr && restart_lsn < oldest_lsn {
            return RS_INVAL_WAL_REMOVED;
        }
    }

    if possible_causes & RS_INVAL_HORIZON.0 as u32 != 0 {
        // invalid DB oid signals a shared relation
        if SlotIsLogical(s) && (dboid == InvalidOid || dboid == s.data.get().database) {
            let effective_xmin = s.effective_xmin.get();
            let catalog_effective_xmin = s.effective_catalog_xmin.get();
            if TransactionIdIsValid(effective_xmin)
                && types_core::TransactionIdPrecedesOrEquals(
                    effective_xmin,
                    snapshot_conflict_horizon,
                )
            {
                return RS_INVAL_HORIZON;
            } else if TransactionIdIsValid(catalog_effective_xmin)
                && types_core::TransactionIdPrecedesOrEquals(
                    catalog_effective_xmin,
                    snapshot_conflict_horizon,
                )
            {
                return RS_INVAL_HORIZON;
            }
        }
    }

    if possible_causes & RS_INVAL_WAL_LEVEL.0 as u32 != 0 && SlotIsLogical(s) {
        return RS_INVAL_WAL_LEVEL;
    }

    if possible_causes & RS_INVAL_IDLE_TIMEOUT.0 as u32 != 0 {
        debug_assert!(now > 0);
        if CanInvalidateIdleSlot(s) {
            // slot.c:1794 (USE_INJECTION_POINTS): tests force the idle-
            // timeout invalidation instead of idling for a real minute.
            if injection_point::is_attached("slot-timeout-inval") {
                *inactive_since = 0; // since the beginning of time
                return RS_INVAL_IDLE_TIMEOUT;
            }
            if adt_timestamp::TimestampDifferenceExceedsSeconds(
                s.inactive_since.get(),
                now,
                idle_replication_slot_timeout_secs(),
            ) {
                *inactive_since = s.inactive_since.get();
                return RS_INVAL_IDLE_TIMEOUT;
            }
        }
    }

    RS_INVAL_NONE
}

// ReportSlotInvalidation (slot.c:1642).
#[allow(clippy::too_many_arguments)]
fn ReportSlotInvalidation(
    cause: ReplicationSlotInvalidationCause,
    terminating: bool,
    pid: i32,
    slotname: &NameData,
    restart_lsn: XLogRecPtr,
    oldest_lsn: XLogRecPtr,
    snapshot_conflict_horizon: TransactionId,
    slot_idle_seconds: i64,
) {
    let lsn_fmt = |l: XLogRecPtr| format!("{:X}/{:X}", (l >> 32) as u32, l as u32);
    let mut err_hint = String::new();
    let err_detail = match cause {
        RS_INVAL_WAL_REMOVED => {
            let ex = oldest_lsn.wrapping_sub(restart_lsn);
            err_hint = "You might need to increase \"max_slot_wal_keep_size\".".to_string();
            format!(
                "The slot's restart_lsn {} exceeds the limit by {} byte{}.",
                lsn_fmt(restart_lsn),
                ex,
                if ex == 1 { "" } else { "s" }
            )
        }
        RS_INVAL_HORIZON => format!(
            "The slot conflicted with xid horizon {snapshot_conflict_horizon}."
        ),
        RS_INVAL_WAL_LEVEL => "Logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary server.".to_string(),
        RS_INVAL_IDLE_TIMEOUT => {
            err_hint =
                "You might need to increase \"idle_replication_slot_timeout\".".to_string();
            format!(
                "The slot's idle time of {slot_idle_seconds}s exceeds the configured \"idle_replication_slot_timeout\" duration of {}s.",
                idle_replication_slot_timeout_secs()
            )
        }
        _ => unreachable!("ReportSlotInvalidation with RS_INVAL_NONE"),
    };

    let msg = if terminating {
        format!(
            "terminating process {pid} to release replication slot \"{}\"",
            name_string(slotname)
        )
    } else {
        format!(
            "invalidating obsolete replication slot \"{}\"",
            name_string(slotname)
        )
    };
    let mut rep = ereport(LOG).errmsg(msg).errdetail(err_detail);
    if !err_hint.is_empty() {
        rep = rep.errhint(err_hint);
    }
    let _ = rep.finish(loc("ReportSlotInvalidation"));
}

// InvalidatePossiblyObsoleteSlot (slot.c:1831): acquire the slot and mark it
// invalid, if necessary and possible. Returns whether ControlLock was
// released in the interim (caller restarts its scan in that case).
fn InvalidatePossiblyObsoleteSlot(
    possible_causes: u32,
    s: &'static ReplicationSlot,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    invalidated: &mut bool,
) -> PgResult<bool> {
    let mut last_signaled_pid = 0;
    let mut released_lock = false;
    let mut inactive_since: TimestampTz = 0;

    loop {
        if !s.in_use.get() {
            if released_lock {
                LWLockRelease(control_lock())?;
            }
            break;
        }

        // Current time up front: no syscall under the spinlock below.
        let now = if possible_causes & RS_INVAL_IDLE_TIMEOUT.0 as u32 != 0 {
            GetCurrentTimestamp()
        } else {
            0
        };

        let mut invalidation_cause = RS_INVAL_NONE;
        let mut restart_lsn = InvalidXLogRecPtr;
        let mut slotname = NameData::default();
        let mut active_pid = 0;
        s.with_mutex(|| {
            restart_lsn = s.data.get().restart_lsn;

            // we do nothing if the slot is already invalid
            if s.data.get().invalidated == RS_INVAL_NONE {
                invalidation_cause = DetermineSlotInvalidationCause(
                    possible_causes,
                    s,
                    oldest_lsn,
                    dboid,
                    snapshot_conflict_horizon,
                    &mut inactive_since,
                    now,
                );
            }
            if invalidation_cause == RS_INVAL_NONE {
                return;
            }

            slotname = s.data.get().name;
            active_pid = s.active_pid.get();

            // If the slot can be acquired, do so and mark it invalidated
            // immediately. Otherwise we'll signal the owning process, below,
            // and retry.
            if active_pid == 0 {
                SetMyReplicationSlot(Some(s));
                s.active_pid.set(g::MyProcPid());
                let mut d = s.data.get();
                d.invalidated = invalidation_cause;
                if invalidation_cause == RS_INVAL_WAL_REMOVED {
                    d.restart_lsn = InvalidXLogRecPtr;
                    s.last_saved_restart_lsn.set(InvalidXLogRecPtr);
                }
                s.data.set(d);
                *invalidated = true;
            }
        });

        if invalidation_cause == RS_INVAL_NONE {
            if released_lock {
                LWLockRelease(control_lock())?;
            }
            break;
        }

        let slot_idle_secs = if invalidation_cause == RS_INVAL_IDLE_TIMEOUT {
            adt_timestamp::TimestampDifference(inactive_since, now).0
        } else {
            0
        };

        if active_pid != 0 {
            // Prepare the CV sleep before releasing the lock, closing the
            // race with the slot being released before the sleep below.
            ConditionVariablePrepareToSleep(&s.active_cv);
            LWLockRelease(control_lock())?;
            released_lock = true;

            // Signal the owner, once per distinct PID (the only reason this
            // routine loops; otherwise the caller's restart would do).
            if last_signaled_pid != active_pid {
                ReportSlotInvalidation(
                    invalidation_cause,
                    true,
                    active_pid,
                    &slotname,
                    restart_lsn,
                    oldest_lsn,
                    snapshot_conflict_horizon,
                    slot_idle_secs,
                );
                if miscinit::GetMyBackendType() == types_core::BackendType::Startup {
                    let _ = procsignal_seams::send_proc_signal::call(
                        active_pid,
                        types_storage::storage::ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
                        types_core::INVALID_PROC_NUMBER,
                    );
                } else {
                    // procsignal::signums, not libc::SIG*: the wasi libc
                    // crate exposes no SIG* names (signums law).
                    let _ = procsignal_seams::send_thread_signal::call(
                        active_pid,
                        procsignal::signums::SIGTERM,
                    );
                }
                last_signaled_pid = active_pid;
            }

            // Wait until the slot is released.
            ConditionVariableSleep(&s.active_cv, WAIT_EVENT_REPLICATION_SLOT_DROP)?;

            // Re-acquire the lock and start over; the slot may also have
            // caught up in the interim, which resolves the conflict without
            // invalidation.
            lw(control_lock(), LW_SHARED)?;
            continue;
        } else {
            // We hold the slot now and have already invalidated it; flush so
            // the state persists. No ControlLock across filesystem ops.
            LWLockRelease(control_lock())?;
            released_lock = true;

            ReplicationSlotMarkDirty();
            ReplicationSlotSave()?;
            ReplicationSlotRelease()?;

            ReportSlotInvalidation(
                invalidation_cause,
                false,
                active_pid,
                &slotname,
                restart_lsn,
                oldest_lsn,
                snapshot_conflict_horizon,
                slot_idle_secs,
            );
            break;
        }
    }

    Ok(released_lock)
}

// InvalidateObsoleteReplicationSlots (slot.c:2059): invalidate slots that
// require resources about to be removed. `possible_causes` is an RS_INVAL_*
// mask. Runs as part of checkpoint — avoid raising errors if possible.
pub fn InvalidateObsoleteReplicationSlots(
    possible_causes: u32,
    oldest_segno: XLogSegNo,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
) -> PgResult<bool> {
    let mut invalidated = false;

    debug_assert!(
        possible_causes & RS_INVAL_HORIZON.0 as u32 == 0
            || TransactionIdIsValid(snapshot_conflict_horizon)
    );
    debug_assert!(possible_causes & RS_INVAL_WAL_REMOVED.0 as u32 == 0 || oldest_segno > 0);
    debug_assert!(possible_causes != RS_INVAL_NONE.0 as u32);

    if walsender_config::max_replication_slots() == 0 {
        return Ok(invalidated);
    }
    let oldest_lsn =
        transam_xlog::XLogSegNoOffsetToRecPtr(oldest_segno, 0, transam_xlog::wal_segment_size());

    'restart: loop {
        lw(control_lock(), LW_SHARED)?;
        for s in ReplicationSlotCtl() {
            if !s.in_use.get() {
                continue;
            }
            // Prevent invalidation of logical slots during binary upgrade.
            if SlotIsLogical(s) && g::IsBinaryUpgrade() {
                continue;
            }
            if InvalidatePossiblyObsoleteSlot(
                possible_causes,
                s,
                oldest_lsn,
                dboid,
                snapshot_conflict_horizon,
                &mut invalidated,
            )? {
                // the lock was released: start from scratch
                continue 'restart;
            }
        }
        LWLockRelease(control_lock())?;
        break;
    }

    if invalidated {
        ReplicationSlotsComputeRequiredXmin(false)?;
        ReplicationSlotsComputeRequiredLSN()?;
    }
    Ok(invalidated)
}

pub fn CheckPointReplicationSlots(is_shutdown: bool) -> PgResult<()> {
    let mut last_saved_restart_lsn_updated = false;

    elog(DEBUG1, "performing replication slot checkpoint")?;

    lw(allocation_lock(), LW_SHARED)?;
    for s in ReplicationSlotCtl() {
        if !s.in_use.get() {
            continue;
        }
        let path = format!("{PG_REPLSLOT_DIR}/{}", name_string(&s.data.get().name));

        if is_shutdown && SlotIsLogical(s) {
            s.with_mutex(|| {
                let d = s.data.get();
                if d.invalidated == RS_INVAL_NONE
                    && d.confirmed_flush > s.last_saved_confirmed_flush.get()
                {
                    s.just_dirtied.set(true);
                    s.dirty.set(true);
                }
            });
        }

        if s.last_saved_restart_lsn.get() != s.data.get().restart_lsn {
            last_saved_restart_lsn_updated = true;
        }

        SaveSlotToPath(s, &path, LOG)?;
    }
    LWLockRelease(allocation_lock())?;

    if last_saved_restart_lsn_updated {
        ReplicationSlotsComputeRequiredLSN()?;
    }
    Ok(())
}

pub fn StartupReplicationSlots() -> PgResult<()> {
    elog(DEBUG1, "starting up replication slots")?;

    let entries = match std::fs::read_dir(PG_REPLSLOT_DIR) {
        Ok(entries) => entries,
        Err(e) => {
            return ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not open directory \"{PG_REPLSLOT_DIR}\": %m"))
                .finish(loc("StartupReplicationSlots"));
        }
    };
    for entry in entries {
        let Ok(entry) = entry else { break };
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else { continue };
        if name == "." || name == ".." {
            continue;
        }
        let path = format!("{PG_REPLSLOT_DIR}/{name}");
        // get_dirent_type: only slot directories matter here.
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(true) {
            continue;
        }
        if name.ends_with(".tmp") {
            if !fd::rmtree(&path, true)? {
                ereport(WARNING)
                    .errmsg(format!("could not remove directory \"{path}\""))
                    .finish(loc("StartupReplicationSlots"))?;
                continue;
            }
            fd::fsync_fname(PG_REPLSLOT_DIR, true)?;
            continue;
        }
        RestoreSlotFromDisk(name)?;
    }

    if walsender_config::max_replication_slots() <= 0 {
        return Ok(());
    }

    ReplicationSlotsComputeRequiredXmin(false)?;
    ReplicationSlotsComputeRequiredLSN()?;
    Ok(())
}

fn CreateSlotOnDisk(slot: &'static ReplicationSlot) -> PgResult<()> {
    let name = name_string(&slot.data.get().name);
    let path = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmppath = format!("{PG_REPLSLOT_DIR}/{name}.tmp");

    if std::path::Path::new(&tmppath).is_dir() {
        let _ = fd::rmtree(&tmppath, true);
    }

    if fd::MakePGDirectory(&tmppath) < 0 {
        return ereport(ERROR)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create directory \"{tmppath}\": %m"))
            .finish(loc("CreateSlotOnDisk"));
    }
    fd::fsync_fname(&tmppath, true)?;

    slot.dirty.set(true);
    SaveSlotToPath(slot, &tmppath, ERROR)?;

    if c_rename(&tmppath, &path) != 0 {
        return ereport(ERROR)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not rename file \"{tmppath}\" to \"{path}\": %m"))
            .finish(loc("CreateSlotOnDisk"));
    }

    g::StartCriticalSection();
    fd::fsync_fname(&path, true)?;
    fd::fsync_fname(PG_REPLSLOT_DIR, true)?;
    g::EndCriticalSection();
    Ok(())
}

fn SaveSlotToPath(slot: &'static ReplicationSlot, dir: &str, elevel: ErrorLevel) -> PgResult<()> {
    let was_dirty = slot.with_mutex(|| {
        let was_dirty = slot.dirty.get();
        slot.just_dirtied.set(false);
        was_dirty
    });
    if !was_dirty {
        return Ok(());
    }

    LWLockAcquire(&slot.io_in_progress_lock, LW_EXCLUSIVE, g::MyProcNumber())?;

    let tmppath = format!("{dir}/state.tmp");
    let path = format!("{dir}/state");

    let fd_ = fd::OpenTransientFile(&tmppath, libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY)?;
    if fd_ < 0 {
        let save_errno = errno::current_errno();
        LWLockRelease(&slot.io_in_progress_lock)?;
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
            .finish(loc("SaveSlotToPath"))?;
        return Ok(());
    }

    let slotdata = slot.with_mutex(|| slot.data.get());
    let image = ondisk::serialize_state_file(&slotdata);

    // SAFETY: image is a live readable buffer of ON_DISK_SIZE bytes.
    if unsafe { libc::write(fd_, image.as_ptr().cast(), image.len()) } != image.len() as isize {
        let mut save_errno = errno::current_errno();
        if save_errno == 0 {
            save_errno = libc::ENOSPC;
        }
        fd::CloseTransientFile(fd_);
        c_unlink(&tmppath);
        LWLockRelease(&slot.io_in_progress_lock)?;
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{tmppath}\": %m"))
            .finish(loc("SaveSlotToPath"))?;
        return Ok(());
    }

    if fd::pg_fsync(fd_) != 0 {
        let save_errno = errno::current_errno();
        fd::CloseTransientFile(fd_);
        c_unlink(&tmppath);
        LWLockRelease(&slot.io_in_progress_lock)?;
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .finish(loc("SaveSlotToPath"))?;
        return Ok(());
    }

    if fd::CloseTransientFile(fd_) != 0 {
        let save_errno = errno::current_errno();
        c_unlink(&tmppath);
        LWLockRelease(&slot.io_in_progress_lock)?;
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .finish(loc("SaveSlotToPath"))?;
        return Ok(());
    }

    if c_rename(&tmppath, &path) != 0 {
        let save_errno = errno::current_errno();
        c_unlink(&tmppath);
        LWLockRelease(&slot.io_in_progress_lock)?;
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not rename file \"{tmppath}\" to \"{path}\": %m"))
            .finish(loc("SaveSlotToPath"))?;
        return Ok(());
    }

    g::StartCriticalSection();
    fd::fsync_fname(&path, false)?;
    fd::fsync_fname(dir, true)?;
    fd::fsync_fname(PG_REPLSLOT_DIR, true)?;
    g::EndCriticalSection();

    slot.with_mutex(|| {
        if !slot.just_dirtied.get() {
            slot.dirty.set(false);
        }
        slot.last_saved_confirmed_flush.set(slotdata.confirmed_flush);
        slot.last_saved_restart_lsn.set(slotdata.restart_lsn);
    });

    LWLockRelease(&slot.io_in_progress_lock)?;
    Ok(())
}

fn RestoreSlotFromDisk(name: &str) -> PgResult<()> {
    let slotdir = format!("{PG_REPLSLOT_DIR}/{name}");
    let tmppath = format!("{slotdir}/state.tmp");

    if c_unlink(&tmppath) < 0 && errno::current_errno() != libc::ENOENT {
        return ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not remove file \"{tmppath}\": %m"))
            .finish(loc("RestoreSlotFromDisk"));
    }

    let path = format!("{slotdir}/state");
    elog(DEBUG1, format!("restoring replication slot from \"{path}\""))?;

    // On some operating systems fsyncing a file requires O_RDWR.
    let fd_ = fd::OpenTransientFile(&path, libc::O_RDWR)?;
    if fd_ < 0 {
        return ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc("RestoreSlotFromDisk"));
    }

    if fd::pg_fsync(fd_) != 0 {
        return ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{path}\": %m"))
            .finish(loc("RestoreSlotFromDisk"));
    }

    g::StartCriticalSection();
    fd::fsync_fname(&slotdir, true)?;
    g::EndCriticalSection();

    let mut buf = [0u8; ON_DISK_SIZE];
    // SAFETY: buf is a live writable buffer of ON_DISK_SIZE bytes.
    let read_bytes =
        unsafe { libc::read(fd_, buf.as_mut_ptr().cast(), ON_DISK_CONSTANT_SIZE) } as i64;
    if read_bytes != ON_DISK_CONSTANT_SIZE as i64 {
        if read_bytes < 0 {
            return ereport(PANIC)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc("RestoreSlotFromDisk"));
        }
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read file \"{path}\": read {read_bytes} of {ON_DISK_CONSTANT_SIZE}"
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    if header_magic(&buf) != SLOT_MAGIC {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has wrong magic number: {} instead of {}",
                header_magic(&buf),
                SLOT_MAGIC
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    if header_version(&buf) != SLOT_VERSION {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has unsupported version {}",
                header_version(&buf)
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    let length = header_length(&buf);
    if length != PERSISTENT_DATA_SIZE as u32 {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot file \"{path}\" has corrupted length {length}"
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    // SAFETY: buf's tail holds `length` == PERSISTENT_DATA_SIZE bytes.
    let read_bytes = unsafe {
        libc::read(fd_, buf.as_mut_ptr().add(ON_DISK_CONSTANT_SIZE).cast(), length as usize)
    } as i64;
    if read_bytes != length as i64 {
        if read_bytes < 0 {
            return ereport(PANIC)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc("RestoreSlotFromDisk"));
        }
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read file \"{path}\": read {read_bytes} of {length}"
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    if fd::CloseTransientFile(fd_) != 0 {
        return ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(loc("RestoreSlotFromDisk"));
    }

    let checksum = state_file_checksum(&buf);
    if checksum != header_checksum(&buf) {
        return ereport(PANIC)
            .errmsg(format!(
                "checksum mismatch for replication slot file \"{path}\": is {checksum}, should be {}",
                header_checksum(&buf)
            ))
            .finish(loc("RestoreSlotFromDisk"));
    }

    let slotdata = deserialize_persistent_data(&buf[ON_DISK_CONSTANT_SIZE..]);

    if slotdata.persistency != RS_PERSISTENT {
        if !fd::rmtree(&slotdir, true)? {
            ereport(WARNING)
                .errmsg(format!("could not remove directory \"{slotdir}\""))
                .finish(loc("RestoreSlotFromDisk"))?;
        }
        fd::fsync_fname(PG_REPLSLOT_DIR, true)?;
        return Ok(());
    }

    if slotdata.database != InvalidOid {
        if transam_xlog::wal_level() < transam_xlog::WAL_LEVEL_LOGICAL {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication slot \"{}\" exists, but \"wal_level\" < \"logical\"",
                    name_string(&slotdata.name)
                ))
                .errhint("Change \"wal_level\" to be \"logical\" or higher.")
                .finish(loc("RestoreSlotFromDisk"));
        }
        // slot.c:2656: in standby mode, hot standby must be enabled, else
        // logical slots could survive a primary wal_level downgrade and
        // remain valid after promotion. (Uninstalled seam = substrate test
        // binaries with no recovery machinery — never standby mode.)
        if xlogrecovery_seams::standby_mode::is_installed()
            && xlogrecovery_seams::standby_mode::call()
            && !guc_tables::vars::EnableHotStandby.read()
        {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication slot \"{}\" exists on the standby, but \"hot_standby\" = \"off\"",
                    name_string(&slotdata.name)
                ))
                .errhint("Change \"hot_standby\" to be \"on\".")
                .finish(loc("RestoreSlotFromDisk"));
        }
    } else if transam_xlog::wal_level() < transam_xlog::WAL_LEVEL_REPLICA {
        return ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "physical replication slot \"{}\" exists, but \"wal_level\" < \"replica\"",
                name_string(&slotdata.name)
            ))
            .errhint("Change \"wal_level\" to be \"replica\" or higher.")
            .finish(loc("RestoreSlotFromDisk"));
    }

    let mut now: TimestampTz = 0;
    let mut restored = false;
    for slot in ReplicationSlotCtl() {
        if slot.in_use.get() {
            continue;
        }
        slot.data.set(slotdata);

        slot.effective_xmin.set(slotdata.xmin);
        slot.effective_catalog_xmin.set(slotdata.catalog_xmin);
        slot.last_saved_confirmed_flush.set(slotdata.confirmed_flush);
        slot.last_saved_restart_lsn.set(slotdata.restart_lsn);

        slot.candidate_catalog_xmin.set(InvalidTransactionId);
        slot.candidate_xmin_lsn.set(InvalidXLogRecPtr);
        slot.candidate_restart_lsn.set(InvalidXLogRecPtr);
        slot.candidate_restart_valid.set(InvalidXLogRecPtr);

        slot.in_use.set(true);
        slot.active_pid.set(0);

        if now == 0 {
            now = GetCurrentTimestamp();
        }
        ReplicationSlotSetInactiveSince(slot, now, false);

        restored = true;
        break;
    }

    if !restored {
        return ereport(FATAL)
            .errmsg("too many replication slots active before shutdown")
            .errhint("Increase \"max_replication_slots\" and try again.")
            .finish(loc("RestoreSlotFromDisk"));
    }
    Ok(())
}

pub struct SlotInvalidationCauseMap {
    pub cause: ReplicationSlotInvalidationCause,
    pub cause_name: &'static str,
}

pub static SLOT_INVALIDATION_CAUSES: [SlotInvalidationCauseMap; RS_INVAL_MAX_CAUSES + 1] = [
    SlotInvalidationCauseMap { cause: RS_INVAL_NONE, cause_name: "none" },
    SlotInvalidationCauseMap { cause: RS_INVAL_WAL_REMOVED, cause_name: "wal_removed" },
    SlotInvalidationCauseMap { cause: RS_INVAL_HORIZON, cause_name: "rows_removed" },
    SlotInvalidationCauseMap { cause: RS_INVAL_WAL_LEVEL, cause_name: "wal_level_insufficient" },
    SlotInvalidationCauseMap { cause: RS_INVAL_IDLE_TIMEOUT, cause_name: "idle_timeout" },
];

pub fn GetSlotInvalidationCause(cause_name: &str) -> ReplicationSlotInvalidationCause {
    for m in &SLOT_INVALIDATION_CAUSES {
        if m.cause_name == cause_name {
            return m.cause;
        }
    }
    debug_assert!(false);
    RS_INVAL_NONE
}

pub fn GetSlotInvalidationCauseName(cause: ReplicationSlotInvalidationCause) -> &'static str {
    for m in &SLOT_INVALIDATION_CAUSES {
        if m.cause == cause {
            return m.cause_name;
        }
    }
    debug_assert!(false);
    "none"
}

// SyncStandbySlotsConfigData: the GUC "extra" payload; owned std strings
// mirror C's single guc_malloc'd blob (cold, config-lifetime).
#[derive(Clone)]
pub struct SyncStandbySlotsConfig {
    pub slot_names: Box<[String]>,
}

fn synchronized_standby_slots_get() -> Option<String> {
    SYNCHRONIZED_STANDBY_SLOTS_RAW.with(|c| c.borrow().clone())
}

fn synchronized_standby_slots_set(v: Option<String>) {
    SYNCHRONIZED_STANDBY_SLOTS_RAW.with(|c| *c.borrow_mut() = v);
}

fn validate_sync_standby_slots(rawname: &str) -> PgResult<Option<Vec<String>>> {
    let scratch = mcx::MemoryContext::new("validate_sync_standby_slots");
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    let Some(elemlist) = varlena::split_identifier_string(scratch.mcx(), rawname, b',', encoding)?
    else {
        guc::GUC_check_errdetail("List syntax is invalid.");
        return Ok(None);
    };

    for name in &elemlist {
        if let Err((code, msg, hint)) = ReplicationSlotValidateNameInternal(name) {
            guc::GUC_check_errcode(code);
            guc::GUC_check_errdetail(msg);
            if let Some(hint) = hint {
                guc::GUC_check_errhint(hint);
            }
            return Ok(None);
        }
    }
    Ok(Some(elemlist))
}

pub fn check_synchronized_standby_slots(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let rawname = match newval.as_deref() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Ok(true),
    };

    let Some(elemlist) = validate_sync_standby_slots(&rawname)? else {
        return Ok(false);
    };
    if elemlist.is_empty() {
        return Ok(true);
    }

    *extra = Some(Box::new(SyncStandbySlotsConfig {
        slot_names: elemlist.into_boxed_slice(),
    }));
    Ok(true)
}

pub fn assign_synchronized_standby_slots(
    _newval: Option<&str>,
    extra: Option<&guc_tables::GucHookExtra>,
) {
    SS_OLDEST_FLUSH_LSN.set(InvalidXLogRecPtr);
    let config = extra
        .and_then(|e| e.downcast_ref::<SyncStandbySlotsConfig>())
        .cloned();
    SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| *c.borrow_mut() = config);
}

pub fn SlotExistsInSyncStandbySlots(slot_name: &str) -> bool {
    SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| {
        c.borrow()
            .as_ref()
            .is_some_and(|cfg| cfg.slot_names.iter().any(|n| n == slot_name))
    })
}

pub fn StandbySlotsHaveCaughtup(wait_for_lsn: XLogRecPtr, elevel: ErrorLevel) -> PgResult<bool> {
    let config = SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| c.borrow().clone());
    let Some(config) = config else {
        return Ok(true);
    };

    if transam_xlog::RecoveryInProgress() {
        return Ok(true);
    }

    if !XLogRecPtrIsInvalid(SS_OLDEST_FLUSH_LSN.get()) && SS_OLDEST_FLUSH_LSN.get() >= wait_for_lsn
    {
        return Ok(true);
    }

    let mut caught_up_slot_num = 0usize;
    let mut min_restart_lsn = InvalidXLogRecPtr;

    lw(control_lock(), LW_SHARED)?;
    let scan: PgResult<()> = (|| {
        for name in config.slot_names.iter() {
            let slot = SearchNamedReplicationSlot(name, false)?;
            let Some(slot) = slot else {
                ereport(elevel)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "replication slot \"{name}\" specified in parameter \
                         \"synchronized_standby_slots\" does not exist"
                    ))
                    .errdetail(format!(
                        "Logical replication is waiting on the standby associated with \
                         replication slot \"{name}\"."
                    ))
                    .errhint(format!(
                        "Create the replication slot \"{name}\" or amend parameter \
                         \"synchronized_standby_slots\"."
                    ))
                    .finish(loc("StandbySlotsHaveCaughtup"))?;
                break;
            };
            if SlotIsLogical(slot) {
                ereport(elevel)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "cannot specify logical replication slot \"{name}\" in parameter \
                         \"synchronized_standby_slots\""
                    ))
                    .errdetail(format!(
                        "Logical replication is waiting for correction on replication slot \
                         \"{name}\"."
                    ))
                    .errhint(format!(
                        "Remove the logical replication slot \"{name}\" from parameter \
                         \"synchronized_standby_slots\"."
                    ))
                    .finish(loc("StandbySlotsHaveCaughtup"))?;
                break;
            }
            let (restart_lsn, invalidated, inactive) = slot.with_mutex(|| {
                let d = slot.data.get();
                (
                    d.restart_lsn,
                    d.invalidated != RS_INVAL_NONE,
                    slot.active_pid.get() == 0,
                )
            });
            if invalidated {
                ereport(elevel)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "physical replication slot \"{name}\" specified in parameter \
                         \"synchronized_standby_slots\" has been invalidated"
                    ))
                    .errdetail(format!(
                        "Logical replication is waiting on the standby associated with \
                         replication slot \"{name}\"."
                    ))
                    .errhint(format!(
                        "Drop and recreate the replication slot \"{name}\", or amend parameter \
                         \"synchronized_standby_slots\"."
                    ))
                    .finish(loc("StandbySlotsHaveCaughtup"))?;
                break;
            }
            if XLogRecPtrIsInvalid(restart_lsn) || restart_lsn < wait_for_lsn {
                if inactive {
                    ereport(elevel)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "replication slot \"{name}\" specified in parameter \
                             \"synchronized_standby_slots\" does not have active_pid"
                        ))
                        .errdetail(format!(
                            "Logical replication is waiting on the standby associated with \
                             replication slot \"{name}\"."
                        ))
                        .errhint(format!(
                            "Start the standby associated with the replication slot \"{name}\", \
                             or amend parameter \"synchronized_standby_slots\"."
                        ))
                        .finish(loc("StandbySlotsHaveCaughtup"))?;
                }
                break;
            }
            if XLogRecPtrIsInvalid(min_restart_lsn) || min_restart_lsn > restart_lsn {
                min_restart_lsn = restart_lsn;
            }
            caught_up_slot_num += 1;
        }
        Ok(())
    })();
    LWLockRelease(control_lock())?;
    scan?;

    if caught_up_slot_num != config.slot_names.len() {
        return Ok(false);
    }

    SS_OLDEST_FLUSH_LSN.set(min_restart_lsn);
    Ok(true)
}

pub fn WaitForStandbyConfirmation(wait_for_lsn: XLogRecPtr) -> PgResult<()> {
    let failover = MyReplicationSlot()
        .expect("WaitForStandbyConfirmation: no slot acquired")
        .data
        .get()
        .failover;
    let has_config = SYNCHRONIZED_STANDBY_SLOTS_CONFIG.with(|c| c.borrow().is_some());
    if !failover || !has_config {
        return Ok(());
    }
    if !walsender_seams::wait_for_standby_confirmation::is_installed() {
        panic!(
            "WaitForStandbyConfirmation past its early exits requires the \
             walsender crate (WalSndCtl->wal_confirm_rcv_cv)"
        );
    }
    walsender_seams::wait_for_standby_confirmation::call(wait_for_lsn)
}

pub fn ReplicationSlotNameForTablesync(suboid: Oid, relid: Oid) -> String {
    let mut name = format!(
        "pg_{suboid}_sync_{relid}_{}",
        transam_xlog::GetSystemIdentifier()
    );
    name.truncate(NAMEDATALEN as usize - 1);
    name
}

pub fn ReplicationSlotDropAtPubNode() -> PgResult<()> {
    panic!("ReplicationSlotDropAtPubNode not ported (WalReceiverConn; libpqwalreceiver unported)");
}

fn idle_timeout_get() -> i32 {
    IDLE_REPLICATION_SLOT_TIMEOUT_SECS.get()
}

fn idle_timeout_set(v: i32) {
    IDLE_REPLICATION_SLOT_TIMEOUT_SECS.set(v);
}

pub fn init_seams() {
    slot_seams::replication_slot_initialize::set(ReplicationSlotInitialize);
    slot_seams::replication_slots_compute_logical_restart_lsn::set(
        ReplicationSlotsComputeLogicalRestartLSN,
    );
    slot_seams::invalidate_obsolete_replication_slots::set(InvalidateObsoleteReplicationSlots);
    slot_seams::replication_slots_drop_db_slots::set(ReplicationSlotsDropDBSlots);
    slot_seams::startup_replication_slots::set(StartupReplicationSlots);
    slot_seams::check_point_replication_slots::set(CheckPointReplicationSlots);
    slot_seams::named_replication_slot_info::set(|name, need_lock| {
        Ok(match SearchNamedReplicationSlot(name, need_lock)? {
            Some(s) => (ReplicationSlotIndex(s), SlotIsLogical(s)),
            None => (-1, false),
        })
    });
    slot_seams::replication_slot_name::set(|index| {
        Ok(ReplicationSlotName(index)?.map(|n| {
            let mut buf = [0u8; 64];
            let s = n.name_str();
            buf[..s.len().min(63)].copy_from_slice(&s[..s.len().min(63)]);
            buf
        }))
    });

    guc_tables::vars::idle_replication_slot_timeout_secs.install(guc_tables::GucVarAccessors {
        get: idle_timeout_get,
        set: idle_timeout_set,
    });
    guc_tables::vars::synchronized_standby_slots.install(guc_tables::GucVarAccessors {
        get: synchronized_standby_slots_get,
        set: synchronized_standby_slots_set,
    });
    guc_tables::hooks::check_synchronized_standby_slots.install(check_synchronized_standby_slots);
    guc_tables::hooks::assign_synchronized_standby_slots.install(assign_synchronized_standby_slots);
    // C home: slotsync.c `bool sync_replication_slots` (slotsync.c unported;
    // the postmaster serverloop reads it in hot standby).
    guc_tables::vars::sync_replication_slots.install(guc_tables::GucVarAccessors {
        get: || SYNC_REPLICATION_SLOTS.load(std::sync::atomic::Ordering::Relaxed),
        set: |v| SYNC_REPLICATION_SLOTS.store(v, std::sync::atomic::Ordering::Relaxed),
    });
}

static SYNC_REPLICATION_SLOTS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Scoped lock helpers for slotsync.c (which takes these locks around its own
// slot-field manipulation, unlike everything above which is self-contained).
// ---------------------------------------------------------------------------

/// Run `f` holding ReplicationSlotAllocationLock exclusively
/// (slotsync.c reserve_wal_for_local_slot).
pub fn with_allocation_lock_exclusive<R>(f: impl FnOnce() -> PgResult<R>) -> PgResult<R> {
    lw(allocation_lock(), LW_EXCLUSIVE)?;
    let r = f();
    LWLockRelease(allocation_lock())?;
    r
}

/// Run `f` holding ReplicationSlotControlLock exclusively
/// (slotsync.c synchronize_one_slot xmin_horizon computation).
pub fn with_control_lock_exclusive<R>(f: impl FnOnce() -> PgResult<R>) -> PgResult<R> {
    lw(control_lock(), LW_EXCLUSIVE)?;
    let r = f();
    LWLockRelease(control_lock())?;
    r
}
