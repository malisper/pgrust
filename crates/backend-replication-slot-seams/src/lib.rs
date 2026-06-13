//! Seam declarations for the `backend-replication-slot` unit
//! (`replication/slot.c`) and its `MyReplicationSlot` global, as consumed by
//! logical decoding.
//!
//! These read/write the current backend's acquired `MyReplicationSlot` and the
//! slot-machinery routines. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::{Oid, TransactionId, XLogRecPtr};

seam_core::seam!(
    /// `CheckSlotRequirements()` — `ereport`s on misconfiguration.
    pub fn CheckSlotRequirements() -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `MyReplicationSlot != NULL`.
    pub fn MyReplicationSlot_is_set() -> bool
);
seam_core::seam!(
    /// `SlotIsPhysical(MyReplicationSlot)` — `data.database == InvalidOid`.
    pub fn SlotIsPhysical() -> bool
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
    /// `MyReplicationSlot->data.invalidated` (`RS_INVAL_NONE == 0`).
    pub fn slot_invalidated() -> i32
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

seam_core::seam!(
    /// `ReplicationSlotReserveWal()` — `ereport`s on failure.
    pub fn ReplicationSlotReserveWal() -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotMarkDirty()`.
    pub fn ReplicationSlotMarkDirty()
);
seam_core::seam!(
    /// `ReplicationSlotSave()` — `ereport`s on I/O failure.
    pub fn ReplicationSlotSave() -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `ReplicationSlotsComputeRequiredXmin(already_locked)`.
    pub fn ReplicationSlotsComputeRequiredXmin(already_locked: bool)
);
seam_core::seam!(
    /// `ReplicationSlotsComputeRequiredLSN()`.
    pub fn ReplicationSlotsComputeRequiredLSN()
);
seam_core::seam!(
    /// `IsSyncingReplicationSlots()`.
    pub fn IsSyncingReplicationSlots() -> bool
);
seam_core::seam!(
    /// `LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE)`.
    pub fn ReplicationSlotControlLock_acquire_exclusive()
);
seam_core::seam!(
    /// `LWLockRelease(ReplicationSlotControlLock)`.
    pub fn ReplicationSlotControlLock_release()
);
seam_core::seam!(
    /// `pgstat_report_replslot(MyReplicationSlot, &repSlotStat)`.
    pub fn pgstat_report_replslot(stats: types_logical::ReorderBufferStats)
);
seam_core::seam!(
    /// The `USE_INJECTION_POINTS` `logical-replication-slot-advance-segment`
    /// block in `LogicalConfirmReceivedLocation`; no-op when not compiled in.
    pub fn maybe_injection_point_slot_advance_segment(old_restart_lsn: XLogRecPtr, new_restart_lsn: XLogRecPtr)
);
