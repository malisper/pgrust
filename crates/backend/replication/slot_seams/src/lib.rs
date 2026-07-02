seam_core::seam!(
    // ReplicationSlotInitialize (replication/slot.c): clear MyReplicationSlot
    // state and register the cleanup exit callback.
    pub fn replication_slot_initialize() -> types_error::PgResult<()>
);
