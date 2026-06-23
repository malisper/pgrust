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

seam_core::seam!(
    /// `bool summarize_wal` (`src/backend/postmaster/walsummarizer.c` GUC) —
    /// whether WAL summarization is enabled. Read by `parse_basebackup_options`
    /// to reject incremental backups when summarization is off.
    pub fn summarize_wal() -> bool
);

seam_core::seam!(
    /// `GetOldestUnsummarizedLSN(NULL, NULL)`
    /// (`src/backend/postmaster/walsummarizer.c`) — the oldest LSN not yet WAL
    /// summarized, as `KeepLogSeg` reads it to retain WAL pending
    /// summarization. Returns `InvalidXLogRecPtr` when `summarize_wal` is off.
    pub fn get_oldest_unsummarized_lsn() -> types_error::PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    /// `WaitForWalSummarization(lsn)` (`src/backend/postmaster/walsummarizer.c`)
    /// — block until WAL summarization has reached `lsn`, throwing an error if
    /// the summarizer appears to be stuck. If summarization is disabled while
    /// waiting, returns immediately. `Err` carries the "stuck"
    /// `ereport(ERROR)`.
    pub fn wait_for_wal_summarization(lsn: types_core::XLogRecPtr) -> types_error::PgResult<()>
);
