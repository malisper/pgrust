//! Seam declarations for the `backend-replication-slot` unit
//! (`replication/slot.c`): the shared-memory replication-slot machinery.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Every slot the seams touch is named explicitly by
//! its [`ReplicationSlotHandle`] (the narrowest capability — no ambient
//! `MyReplicationSlot` getter encodes the owner's per-backend global into a
//! zero-arg seam): field reads/writes and the per-slot spinlock take the slot
//! handle as a parameter so the owner maps it to the live shmem slot, and the
//! spinlock-protected writes happen while the owner holds `slot->mutex`.

#![allow(non_snake_case)]

use types_core::primitive::{Oid, TimestampTz, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_replication::{
    ReplicationSlotHandle, ReplicationSlotInvalidationCause, ReplicationSlotPersistency,
};

// --- The per-backend current slot + the slot array ---

seam_core::seam!(
    /// `MyReplicationSlot` (slot.c) — the slot this backend currently holds, or
    /// [`ReplicationSlotHandle::NONE`] when none is acquired.
    pub fn my_replication_slot() -> ReplicationSlotHandle
);
seam_core::seam!(
    /// `&ReplicationSlotCtl->replication_slots[i]` (slot.c).
    pub fn replication_slot(i: i32) -> ReplicationSlotHandle
);
seam_core::seam!(
    /// `max_replication_slots` GUC (slot.c).
    pub fn max_replication_slots() -> i32
);
seam_core::seam!(
    /// `SearchNamedReplicationSlot(name, need_lock)` (slot.c). Returns
    /// [`ReplicationSlotHandle::NONE`] when no slot of that name exists.
    pub fn search_named_replication_slot(name: &str, need_lock: bool) -> ReplicationSlotHandle
);

// --- Slot lifecycle operations (operate on MyReplicationSlot) ---

seam_core::seam!(
    /// `ReplicationSlotCreate(name, db_specific, persistency, two_phase,
    /// failover, synced)` (slot.c).
    pub fn replication_slot_create(
        name: &str,
        db_specific: bool,
        persistency: ReplicationSlotPersistency,
        two_phase: bool,
        failover: bool,
        synced: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotAcquire(name, nowait, error_if_invalid)` (slot.c).
    pub fn replication_slot_acquire(
        name: &str,
        nowait: bool,
        error_if_invalid: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotRelease()` (slot.c).
    pub fn replication_slot_release() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotDropAcquired()` (slot.c).
    pub fn replication_slot_drop_acquired() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotCleanup(synced_only)` (slot.c).
    pub fn replication_slot_cleanup(synced_only: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotPersist()` (slot.c).
    pub fn replication_slot_persist() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotMarkDirty()` (slot.c).
    pub fn replication_slot_mark_dirty() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotSave()` (slot.c).
    pub fn replication_slot_save() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotsComputeRequiredXmin(already_locked)` (slot.c).
    pub fn replication_slots_compute_required_xmin(already_locked: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotsComputeRequiredLSN()` (slot.c).
    pub fn replication_slots_compute_required_lsn() -> PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotSetInactiveSince(slot, now, acquire_lock)` (slot.c).
    pub fn replication_slot_set_inactive_since(
        slot: ReplicationSlotHandle,
        now: TimestampTz,
        acquire_lock: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `GetSlotInvalidationCause(invalidation_reason)` (slot.c).
    pub fn get_slot_invalidation_cause(
        conflict_reason: &str,
    ) -> PgResult<ReplicationSlotInvalidationCause>
);

// --- Per-slot spinlock (slot->mutex) ---

seam_core::seam!(
    /// `SpinLockAcquire(&slot->mutex)` (slot.c). Surfaces nothing in C, but the
    /// owner's mapping of the handle can fail; carried as `PgResult` so the
    /// failure surface mirrors the owner.
    pub fn slot_spin_acquire(slot: ReplicationSlotHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `SpinLockRelease(&slot->mutex)` (slot.c).
    pub fn slot_spin_release(slot: ReplicationSlotHandle) -> PgResult<()>
);

// --- Slot field reads (ReplicationSlot / ReplicationSlotPersistentData) ---

seam_core::seam!(pub fn slot_in_use(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_active_pid(slot: ReplicationSlotHandle) -> i32);
seam_core::seam!(pub fn slot_is_logical(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_synced(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_name(slot: ReplicationSlotHandle) -> String);
seam_core::seam!(pub fn slot_data_plugin(slot: ReplicationSlotHandle) -> String);
seam_core::seam!(pub fn slot_data_database(slot: ReplicationSlotHandle) -> Oid);
seam_core::seam!(pub fn slot_data_two_phase(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_two_phase_at(slot: ReplicationSlotHandle) -> XLogRecPtr);
seam_core::seam!(pub fn slot_data_failover(slot: ReplicationSlotHandle) -> bool);
seam_core::seam!(pub fn slot_data_restart_lsn(slot: ReplicationSlotHandle) -> XLogRecPtr);
seam_core::seam!(pub fn slot_data_confirmed_flush(slot: ReplicationSlotHandle) -> XLogRecPtr);
seam_core::seam!(pub fn slot_data_catalog_xmin(slot: ReplicationSlotHandle) -> TransactionId);
seam_core::seam!(pub fn slot_data_persistency(slot: ReplicationSlotHandle) -> ReplicationSlotPersistency);
seam_core::seam!(pub fn slot_data_invalidated(slot: ReplicationSlotHandle) -> ReplicationSlotInvalidationCause);

// --- Slot field writes (under slot->mutex held by the caller) ---

seam_core::seam!(pub fn set_slot_data_plugin(slot: ReplicationSlotHandle, name: &str));
seam_core::seam!(pub fn set_slot_data_database(slot: ReplicationSlotHandle, dbid: Oid));
seam_core::seam!(pub fn set_slot_data_two_phase(slot: ReplicationSlotHandle, v: bool));
seam_core::seam!(pub fn set_slot_data_two_phase_at(slot: ReplicationSlotHandle, lsn: XLogRecPtr));
seam_core::seam!(pub fn set_slot_data_failover(slot: ReplicationSlotHandle, v: bool));
seam_core::seam!(pub fn set_slot_data_restart_lsn(slot: ReplicationSlotHandle, lsn: XLogRecPtr));
seam_core::seam!(pub fn set_slot_data_confirmed_flush(slot: ReplicationSlotHandle, lsn: XLogRecPtr));
seam_core::seam!(pub fn set_slot_data_catalog_xmin(slot: ReplicationSlotHandle, xid: TransactionId));
seam_core::seam!(pub fn set_slot_data_invalidated(slot: ReplicationSlotHandle, cause: ReplicationSlotInvalidationCause));
seam_core::seam!(pub fn set_slot_effective_catalog_xmin(slot: ReplicationSlotHandle, xid: TransactionId));
