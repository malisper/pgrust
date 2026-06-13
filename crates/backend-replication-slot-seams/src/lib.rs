//! Seam declarations for `backend-replication-slot` (`replication/slot.c`).
//!
//! These are the slot.c entry points other crates (slotfuncs, slotsync,
//! walsender, checkpointer, xlog) call across a dependency cycle. The owner
//! installs every one of them from its `init_seams()`.

#![allow(non_snake_case)]

use types_core::{Oid, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_replication_slot::{ReplicationSlotInvalidationCause, ReplicationSlotPersistency};
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
    /// TransactionId)` (slot.c:2059).
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
