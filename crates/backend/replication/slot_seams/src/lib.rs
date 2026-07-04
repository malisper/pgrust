seam_core::seam!(
    // ReplicationSlotInitialize (slot.c): register the cleanup exit callback.
    pub fn replication_slot_initialize() -> types_error::PgResult<()>
);

seam_core::seam!(
    // StartupReplicationSlots (slot.c): restore state files before recovery.
    pub fn startup_replication_slots() -> types_error::PgResult<()>
);

seam_core::seam!(
    // CheckPointReplicationSlots (slot.c): flush dirty slot state files.
    pub fn check_point_replication_slots(is_shutdown: bool) -> types_error::PgResult<()>
);
