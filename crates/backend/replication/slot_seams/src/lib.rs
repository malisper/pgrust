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

seam_core::seam!(
    // InvalidateObsoleteReplicationSlots (slot.c:2059) for the checkpoint /
    // restartpoint WAL-removal sweep (xlog.c:7383,7841). possible_causes is
    // an RS_INVAL_* mask; returns whether any slot got invalidated (caller
    // recomputes its old-segment horizon then).
    pub fn invalidate_obsolete_replication_slots(
        possible_causes: u32,
        oldest_segno: u64,
        dboid: types_core::Oid,
        snapshot_conflict_horizon: types_core::TransactionId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // ReplicationSlotsDropDBSlots (slot.c) — dropdb (dbcommands.c:1852) and
    // dbase_redo's XLOG_DBASE_DROP arm drop the database's slots.
    pub fn replication_slots_drop_db_slots(dboid: types_core::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    // ReplicationSlotsComputeLogicalRestartLSN (slot.c): oldest restart_lsn
    // across in-use logical slots; InvalidXLogRecPtr when none. The logical
    // rewrite-mapping checkpoint GC uses it as its removal cutoff.
    pub fn replication_slots_compute_logical_restart_lsn(
    ) -> types_error::PgResult<types_core::XLogRecPtr>
);
