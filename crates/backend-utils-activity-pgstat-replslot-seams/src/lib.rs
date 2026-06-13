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
