//! Seam declarations for the `backend-postmaster-walsummarizer` unit
//! (`src/backend/postmaster/walsummarizer.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WalSummarizerMain(startup_data, startup_data_len)` (`src/backend/postmaster/walsummarizer.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn wal_summarizer_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `WalSummarizerShmemSize()` (ipci.c `CalculateShmemSize` accumulator) —
    /// shared-memory bytes this subsystem needs. Infallible in C, so the seam
    /// returns a bare `Size`.
    pub fn wal_summarizer_shmem_size() -> types_core::Size
);

seam_core::seam!(
    /// `WalSummarizerShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn wal_summarizer_shmem_init() -> types_error::PgResult<()>
);
