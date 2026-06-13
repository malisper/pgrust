//! Seam declarations for `replication/logical/slotsync.c` (the bits `slot.c`
//! calls).

seam_core::seam!(
    /// `bool IsSyncingReplicationSlots(void)` (slotsync.c) — true while the
    /// slot sync worker (or `pg_sync_replication_slots`) is creating slots.
    pub fn is_syncing_replication_slots() -> bool
);
