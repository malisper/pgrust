//! Seam declarations for `backend-replication-slot` (`replication/slot.c`).
//!
//! These are the slot.c entry points other crates (slotfuncs, slotsync,
//! walsender, checkpointer, xlog, logical decoding) call across a dependency
//! cycle. The owner installs every one of them from its `init_seams()`.
//!
//! The `MyReplicationSlot`-accessor seams read/write the current backend's
//! acquired slot. The live `ReplicationSlot` struct is owner-private (it
//! embeds real lock primitives), so logical decoding reaches its fields and
//! the per-slot spinlock through these narrow seams rather than a borrow.

#![allow(non_snake_case)]

use types_core::{Oid, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_replication_slot::{
    ReplicationSlotHandle, ReplicationSlotInvalidationCause, ReplicationSlotPersistency,
};
use types_tuple::heaptuple::NameData;

seam_core::seam!(
    /// `void ReplicationSlotsShmemInit(void)` (slot.c:204).
    pub fn replication_slots_shmem_init()
);

seam_core::seam!(
    /// `void ReplicationSlotInitialize(void)` (slot.c:239).
    pub fn replication_slot_initialize() -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotCreate(...)` (slot.c:353). `my_database_id` is
    /// `MyDatabaseId` (no ambient-global getter), used when `db_specific`.
    pub fn replication_slot_create(
        name: &str,
        db_specific: bool,
        persistency: ReplicationSlotPersistency,
        two_phase: bool,
        failover: bool,
        synced: bool,
        my_database_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotAcquire(const char *, bool, bool)` (slot.c:589).
    pub fn replication_slot_acquire(
        name: &str,
        nowait: bool,
        error_if_invalid: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotRelease(void)` (slot.c:716).
    pub fn replication_slot_release() -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotCleanup(bool synced_only)` (slot.c:805).
    pub fn replication_slot_cleanup(synced_only: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotDrop(const char *, bool)` (slot.c:844).
    pub fn replication_slot_drop(name: &str, nowait: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotAlter(const char *, const bool *, const bool *)`
    /// (slot.c:877). `failover`/`two_phase` are `Option` (the C NULL pointers).
    pub fn replication_slot_alter(
        name: &str,
        failover: Option<bool>,
        two_phase: Option<bool>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotDropAcquired(void)` (slot.c:959).
    pub fn replication_slot_drop_acquired() -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotSave(void)` (slot.c:1083).
    pub fn replication_slot_save() -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotMarkDirty(void)` (slot.c:1101).
    pub fn replication_slot_mark_dirty()
);

seam_core::seam!(
    /// `void ReplicationSlotPersist(void)` (slot.c:1118).
    pub fn replication_slot_persist() -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotsComputeRequiredXmin(bool)` (slot.c:1143).
    pub fn replication_slots_compute_required_xmin(already_locked: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotsComputeRequiredLSN(void)` (slot.c:1225).
    pub fn replication_slots_compute_required_lsn() -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void)` (slot.c:1295).
    pub fn replication_slots_compute_logical_restart_lsn() -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `bool ReplicationSlotsCountDBSlots(Oid, int *, int *)` (slot.c:1374) —
    /// returns `(found, nslots, nactive)`.
    pub fn replication_slots_count_db_slots(dboid: Oid) -> PgResult<(bool, i32, i32)>
);

seam_core::seam!(
    /// `void ReplicationSlotsDropDBSlots(Oid)` (slot.c:1432). `my_proc_pid`
    /// is the caller's `MyProcPid`.
    pub fn replication_slots_drop_db_slots(dboid: Oid, my_proc_pid: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `void CheckSlotRequirements(void)` (slot.c:1524). `wal_level` is the
    /// `wal_level` GUC value.
    pub fn check_slot_requirements(wal_level: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `void CheckSlotPermissions(void)` (slot.c:1546). `user_id` is
    /// `GetUserId()`.
    pub fn check_slot_permissions(user_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `void ReplicationSlotReserveWal(void)` (slot.c:1563).
    pub fn replication_slot_reserve_wal() -> PgResult<()>
);

seam_core::seam!(
    /// `bool InvalidateObsoleteReplicationSlots(uint32, XLogSegNo, Oid,
    /// TransactionId)` (slot.c:2059). `possible_causes` is the bitwise-OR of
    /// `ReplicationSlotInvalidationCause` codes the C call passes.
    pub fn invalidate_obsolete_replication_slots(
        possible_causes: u32,
        oldest_segno: XLogSegNo,
        dboid: Oid,
        snapshot_conflict_horizon: TransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `void CheckPointReplicationSlots(bool)` (slot.c:2119).
    pub fn checkpoint_replication_slots(is_shutdown: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `void StartupReplicationSlots(void)` (slot.c:2197).
    pub fn startup_replication_slots() -> PgResult<()>
);

seam_core::seam!(
    /// `ReplicationSlotInvalidationCause GetSlotInvalidationCause(const char *)`
    /// (slot.c:2724).
    pub fn get_slot_invalidation_cause(cause_name: &str) -> ReplicationSlotInvalidationCause
);

seam_core::seam!(
    /// `const char *GetSlotInvalidationCauseName(ReplicationSlotInvalidationCause)`
    /// (slot.c:2744).
    pub fn get_slot_invalidation_cause_name(cause: ReplicationSlotInvalidationCause) -> &'static str
);

seam_core::seam!(
    /// `bool SlotExistsInSyncStandbySlots(const char *)` (slot.c:2868).
    pub fn slot_exists_in_sync_standby_slots(slot_name: &str) -> bool
);

seam_core::seam!(
    /// `bool StandbySlotsHaveCaughtup(XLogRecPtr, int)` (slot.c:2901).
    pub fn standby_slots_have_caughtup(
        wait_for_lsn: XLogRecPtr,
        elevel: types_error::ErrorLevel,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `void WaitForStandbyConfirmation(XLogRecPtr)` (slot.c:3049).
    pub fn wait_for_standby_confirmation(wait_for_lsn: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    /// `bool ReplicationSlotName(int index, Name name)` (slot.c:558) — returns
    /// `(found, name)` where `name` is valid only when found.
    pub fn replication_slot_name(index: i32) -> (bool, NameData)
);

// ---------------------------------------------------------------------------
// `MyReplicationSlot` accessors (slot.c owns the per-backend acquired slot).
// Logical decoding (`logical.c`) reads/writes these fields under the per-slot
// spinlock; the owner installs each.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `MyReplicationSlot != NULL`.
    pub fn my_replication_slot_is_set() -> bool
);

seam_core::seam!(
    /// `SlotIsPhysical(MyReplicationSlot)` — `data.database == InvalidOid`.
    pub fn slot_is_physical() -> bool
);

seam_core::seam!(
    /// `MyReplicationSlot->data.database`.
    pub fn slot_database() -> Oid
);

seam_core::seam!(
    /// `NameStr(MyReplicationSlot->data.name)`.
    pub fn slot_name() -> String
);

seam_core::seam!(
    /// `NameStr(MyReplicationSlot->data.plugin)`.
    pub fn slot_plugin() -> String
);

seam_core::seam!(
    /// `MyReplicationSlot->data.synced`.
    pub fn slot_synced() -> bool
);

seam_core::seam!(
    /// `MyReplicationSlot->data.invalidated`.
    pub fn slot_invalidated() -> ReplicationSlotInvalidationCause
);

seam_core::seam!(
    /// `MyReplicationSlot->data.restart_lsn`.
    pub fn slot_restart_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// `MyReplicationSlot->data.confirmed_flush`.
    pub fn slot_confirmed_flush() -> XLogRecPtr
);

seam_core::seam!(
    /// `MyReplicationSlot->data.two_phase`.
    pub fn slot_two_phase() -> bool
);

seam_core::seam!(
    /// `MyReplicationSlot->data.failover`.
    pub fn slot_failover() -> bool
);
seam_core::seam!(
    /// `MyReplicationSlot->data.two_phase_at`.
    pub fn slot_two_phase_at() -> XLogRecPtr
);

seam_core::seam!(
    /// `MyReplicationSlot->data.catalog_xmin`.
    pub fn slot_catalog_xmin() -> TransactionId
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_xmin_lsn`.
    pub fn slot_candidate_xmin_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_catalog_xmin`.
    pub fn slot_candidate_catalog_xmin() -> TransactionId
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_restart_lsn`.
    pub fn slot_candidate_restart_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_restart_valid`.
    pub fn slot_candidate_restart_valid() -> XLogRecPtr
);

seam_core::seam!(
    /// `SpinLockAcquire(&MyReplicationSlot->mutex)`.
    pub fn slot_mutex_acquire()
);

seam_core::seam!(
    /// `SpinLockRelease(&MyReplicationSlot->mutex)`.
    pub fn slot_mutex_release()
);

seam_core::seam!(
    /// `namestrcpy(&MyReplicationSlot->data.plugin, plugin)`.
    pub fn slot_set_plugin(plugin: String)
);

seam_core::seam!(
    /// `MyReplicationSlot->data.restart_lsn = lsn` (caller holds the mutex).
    pub fn slot_set_restart_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `MyReplicationSlot->effective_catalog_xmin = xid` (caller holds mutex).
    pub fn slot_set_effective_catalog_xmin(xid: TransactionId)
);

seam_core::seam!(
    /// `MyReplicationSlot->data.catalog_xmin = xid` (caller holds mutex).
    pub fn slot_set_catalog_xmin(xid: TransactionId)
);

seam_core::seam!(
    /// `MyReplicationSlot->effective_xmin = xid` (caller holds mutex).
    pub fn slot_set_effective_xmin(xid: TransactionId)
);

seam_core::seam!(
    /// `MyReplicationSlot->data.confirmed_flush = lsn` (caller holds mutex).
    pub fn slot_set_confirmed_flush(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `MyReplicationSlot->data.two_phase = value` (caller holds mutex).
    pub fn slot_set_two_phase(value: bool)
);

seam_core::seam!(
    /// `MyReplicationSlot->data.two_phase_at = lsn` (caller holds mutex).
    pub fn slot_set_two_phase_at(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_catalog_xmin = xid` (caller holds mutex).
    pub fn slot_set_candidate_catalog_xmin(xid: TransactionId)
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_xmin_lsn = lsn` (caller holds mutex).
    pub fn slot_set_candidate_xmin_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_restart_lsn = lsn` (caller holds mutex).
    pub fn slot_set_candidate_restart_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `MyReplicationSlot->candidate_restart_valid = lsn` (caller holds mutex).
    pub fn slot_set_candidate_restart_valid(lsn: XLogRecPtr)
);

// MyReplicationSlot field accessors/mutators additionally needed by the
// slotsync consumer (slot.c owns `MyReplicationSlot`).
seam_core::seam!(
    /// `MyReplicationSlot->data.persistency`.
    pub fn slot_persistency() -> ReplicationSlotPersistency
);
seam_core::seam!(
    /// `MyReplicationSlot->data.invalidated = cause` (under the per-slot
    /// spinlock, held by the caller).
    pub fn slot_set_invalidated(cause: ReplicationSlotInvalidationCause)
);
seam_core::seam!(
    /// `MyReplicationSlot->data.database = dbid` (under the per-slot spinlock).
    pub fn slot_set_database(dbid: Oid)
);
seam_core::seam!(
    /// `MyReplicationSlot->data.failover = value` (under the per-slot spinlock).
    pub fn slot_set_failover(value: bool)
);

seam_core::seam!(
    /// `LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE)`.
    pub fn replication_slot_control_lock_acquire_exclusive()
);

seam_core::seam!(
    /// `LWLockRelease(ReplicationSlotControlLock)`.
    pub fn replication_slot_control_lock_release()
);

seam_core::seam!(
    /// The `USE_INJECTION_POINTS` `logical-replication-slot-advance-segment`
    /// block in `LogicalConfirmReceivedLocation`; no-op when not compiled in.
    pub fn maybe_injection_point_slot_advance_segment(
        old_restart_lsn: XLogRecPtr,
        new_restart_lsn: XLogRecPtr,
    )
);

seam_core::seam!(
    /// `pgstat_report_replslot(MyReplicationSlot, &repSlotStat)`
    /// (logical.c:1983). The owner resolves `MyReplicationSlot`'s index and
    /// forwards the decoding stats to `pgstat_replslot.c`.
    pub fn pgstat_report_replslot(stats: types_logical::ReorderBufferStats)
);

// ---------------------------------------------------------------------------
// By-handle slot accessors (slot.c owns the array; a `ReplicationSlotHandle`
// is the index into `ReplicationSlotCtl->replication_slots[]`). Consumers that
// genuinely operate on slots OTHER than `MyReplicationSlot` — e.g. slotsync's
// `get_local_synced_slots` array scan and `update_synced_slots_inactive_since`
// — reach those slots' fields and per-slot spinlock through these. The owner
// maps the handle back to `&replication_slots[i]`. ADDED for the slotsync
// consumer; installed by `backend-replication-slot`'s `init_seams()`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `int max_replication_slots` GUC (slot.c).
    pub fn max_replication_slots() -> i32
);
seam_core::seam!(
    /// `&ReplicationSlotCtl->replication_slots[i]` (slot.c) — the handle for
    /// array slot `i`.
    pub fn replication_slot(i: i32) -> ReplicationSlotHandle
);
seam_core::seam!(
    /// `SearchNamedReplicationSlot(name, need_lock)` (slot.c:646). Returns
    /// [`ReplicationSlotHandle::NONE`] when no slot of that name exists.
    pub fn search_named_replication_slot(
        name: &str,
        need_lock: bool,
    ) -> PgResult<ReplicationSlotHandle>
);
seam_core::seam!(
    /// `ReplicationSlotSetInactiveSince(slot, now, acquire_lock)` (slot.c:306).
    pub fn replication_slot_set_inactive_since(
        slot: ReplicationSlotHandle,
        now: TimestampTz,
        acquire_lock: bool,
    )
);

seam_core::seam!(
    /// `SpinLockAcquire(&slot->mutex)` for an arbitrary array slot.
    pub fn slot_spin_acquire(slot: ReplicationSlotHandle)
);
seam_core::seam!(
    /// `SpinLockRelease(&slot->mutex)` for an arbitrary array slot.
    pub fn slot_spin_release(slot: ReplicationSlotHandle)
);

seam_core::seam!(pub fn slot_in_use(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_is_logical(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_synced(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_name(slot: ReplicationSlotHandle) -> String);
seam_core::seam!(pub fn slot_data_database(slot: ReplicationSlotHandle) -> Oid);
seam_core::seam!(pub fn slot_active_pid(slot: ReplicationSlotHandle) -> i32);
seam_core::seam!(
    pub fn slot_data_invalidated(slot: ReplicationSlotHandle) -> ReplicationSlotInvalidationCause
);
