//! Seam declarations for the `backend-postmaster-walsummarizer` unit
//! (`src/backend/postmaster/walsummarizer.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WalSummarizerMain(startup_data, startup_data_len)` (`src/backend/postmaster/walsummarizer.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn wal_summarizer_main(startup_data: &[u8]) -> !
);
