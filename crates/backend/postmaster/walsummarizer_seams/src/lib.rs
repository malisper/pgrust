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
