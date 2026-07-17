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

seam_core::seam!(
    // get_replslot_index + SlotIsLogical (slot.c) for pgstat_replslot.c;
    // (index, is_logical), index -1 when no in-use slot has the name.
    pub fn named_replication_slot_info(
        name: &str,
        need_lock: bool,
    ) -> types_error::PgResult<(i32, bool)>
);

seam_core::seam!(
    // ReplicationSlotName (slot.c): NUL-padded NAMEDATALEN name of the slot
    // at index, None if not in use.
    pub fn replication_slot_name(index: i32) -> types_error::PgResult<Option<[u8; 64]>>
);
