//! Seam declarations for the `backend-replication-slot` unit
//! (`replication/slot.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{Oid, TransactionId, XLogSegNo};
use types_error::PgResult;
use types_wal::ReplicationSlotInvalidationCause;

seam_core::seam!(
    /// `InvalidateObsoleteReplicationSlots(possible_causes, oldestSegno,
    /// dboid, snapshotConflictHorizon)` — returns true when any slot was
    /// invalidated (and the caller should recompute resource limits).
    pub fn invalidate_obsolete_replication_slots(
        possible_causes: ReplicationSlotInvalidationCause,
        oldest_segno: XLogSegNo,
        dboid: Oid,
        snapshot_conflict_horizon: TransactionId,
    ) -> PgResult<bool>
);
