//! Seam declarations for the `backend-postmaster-checkpointer` unit
//! (`src/backend/postmaster/checkpointer.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `CheckpointerMain(startup_data, startup_data_len)` (`src/backend/postmaster/checkpointer.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn checkpointer_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `AbsorbSyncRequests()` (checkpointer.c) — drain the checkpointer's shmem
    /// request queue into the local `pendingOps`/`pendingUnlinks` (via
    /// `RememberSyncRequest`). It allocates, so callers must not be in a
    /// critical section; `Err` carries any allocation `ereport`.
    pub fn absorb_sync_requests() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ForwardSyncRequest(ftag, type)` (checkpointer.c) — try to enqueue one
    /// request onto the checkpointer's shmem queue. Returns `true` if queued,
    /// `false` if the queue was full (caller may retry).
    pub fn forward_sync_request(
        ftag: types_storage::sync::FileTag,
        request_type: types_storage::sync::SyncRequestType,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// Store the post-sync metrics into `CheckpointStats` (xlog.c global):
    /// `ckpt_sync_rels`, `ckpt_longest_sync`, `ckpt_agg_sync_time`. Pure
    /// bookkeeping; infallible.
    pub fn checkpoint_stats_set(ckpt_sync_rels: i32, ckpt_longest_sync: u64, ckpt_agg_sync_time: u64)
);

seam_core::seam!(
    /// `RequestCheckpoint(flags)` (checkpointer.c) — signal the checkpointer to
    /// start (or, in single-user/bootstrap mode, run inline) a checkpoint with
    /// the given `CHECKPOINT_*` flag bits. `xlog.c`'s `XLogWrite` calls this with
    /// `CHECKPOINT_CAUSE_XLOG` when too much WAL has accrued since the last
    /// checkpoint. Owner unported; scaffolded slot.
    pub fn request_checkpoint(flags: i32)
);

seam_core::seam!(
    /// `CheckpointerShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn checkpointer_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `CheckpointerShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn checkpointer_shmem_init() -> types_error::PgResult<()>
);
