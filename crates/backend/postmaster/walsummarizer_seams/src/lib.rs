use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    pub fn wakeup_wal_summarizer()
);

seam_core::seam!(
    pub fn wait_for_wal_summarization(lsn: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    pub fn get_oldest_unsummarized_lsn() -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // GetWalSummarizerState flattened: (summarized_tli, summarized_lsn,
    // pending_lsn, summarizer_pid); pid < 0 means no summarizer.
    pub fn get_wal_summarizer_state() -> PgResult<(u32, XLogRecPtr, XLogRecPtr, i32)>
);
