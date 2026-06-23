//! Seam declarations for `utils/activity/pgstat_replslot.c` (the bits `slot.c`
//! calls). A replication slot is identified to pgstat by its array index
//! (`ReplicationSlotIndex`) and current name; slot.c holds the relevant locks
//! when it calls these.

use types_tuple::heaptuple::NameData;

seam_core::seam!(
    /// `void pgstat_create_replslot(ReplicationSlot *slot)` (pgstat_replslot.c).
    pub fn pgstat_create_replslot(slot_index: i32, name: NameData)
);

seam_core::seam!(
    /// `void pgstat_acquire_replslot(ReplicationSlot *slot)` (pgstat_replslot.c).
    pub fn pgstat_acquire_replslot(slot_index: i32)
);

seam_core::seam!(
    /// `void pgstat_drop_replslot(ReplicationSlot *slot)` (pgstat_replslot.c).
    pub fn pgstat_drop_replslot(slot_index: i32)
);

seam_core::seam!(
    /// `void pgstat_report_replslot(ReplicationSlot *slot,
    /// const PgStat_StatReplSlotEntry *repSlotStat)` (pgstat_replslot.c) —
    /// report a slot's decoding stats. The slot is identified by its array
    /// index; the stat fields come from the reorder buffer.
    pub fn pgstat_report_replslot(slot_index: i32, stats: types_logical::ReorderBufferStats)
);
