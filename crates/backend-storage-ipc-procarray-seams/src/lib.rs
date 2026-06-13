//! Seam declarations for `storage/ipc/procarray.c` (the bits `slot.c` calls).

use types_core::TransactionId;

seam_core::seam!(
    /// `void ProcArraySetReplicationSlotXmin(TransactionId xmin,
    /// TransactionId catalog_xmin, bool already_locked)` (procarray.c) —
    /// publish the aggregate slot xmin horizons into the ProcArray.
    pub fn proc_array_set_replication_slot_xmin(
        xmin: TransactionId,
        catalog_xmin: TransactionId,
        already_locked: bool,
    )
);

seam_core::seam!(
    /// Clear `PROC_IN_LOGICAL_DECODING` on `MyProc` and mirror it into
    /// `ProcGlobal->statusFlags[MyProc->pgxactoff]`, under `ProcArrayLock`
    /// exclusive (slot.c `ReplicationSlotRelease`). The acquire/release of
    /// `ProcArrayLock` is part of this operation in the owner.
    pub fn proc_array_clear_logical_decoding_flag()
);
