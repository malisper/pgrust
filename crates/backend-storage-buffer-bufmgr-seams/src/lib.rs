//! Seam declarations for the `backend-storage-buffer-bufmgr` unit
//! (`storage/buffer/bufmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `effective_io_concurrency` / `maintenance_io_concurrency` GUC globals
//! deliberately have no getter seams: per the no-ambient-global-seams rule,
//! consumers take the values as explicit parameters.

seam_core::seam!(
    /// `RelationGetNumberOfBlocksInFork(relation, forkNum)` (bufmgr.c): the
    /// current number of blocks in the relation fork (`smgrnblocks` under
    /// the covers — the `RelationGetNumberOfBlocks` macro is the
    /// `MAIN_FORKNUM` case). `Err` carries the smgr `ereport(ERROR)`s.
    pub fn relation_get_number_of_blocks_in_fork(
        relation: types_core::primitive::Oid,
        fork_num: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);

seam_core::seam!(
    /// `HoldingBufferPinThatDelaysRecovery()` — does this backend hold the
    /// buffer pin the Startup process is waiting for?
    pub fn holding_buffer_pin_that_delays_recovery() -> bool
);

seam_core::seam!(
    /// `AtEOXact_Buffers(isCommit)` — sanity-check that all buffer pins were
    /// released (Assert-only in production builds).
    pub fn at_eoxact_buffers(is_commit: bool)
);

seam_core::seam!(
    /// `UnlockBuffers()` — release buffer content locks on the abort path.
    pub fn unlock_buffers()
);
