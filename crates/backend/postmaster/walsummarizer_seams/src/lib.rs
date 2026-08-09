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

seam_core::seam!(
    // GetWalSummaries(0, InvalidXLogRecPtr, InvalidXLogRecPtr) flattened for
    // the SQL SRF pg_available_wal_summaries (walsummaryfuncs.c): one
    // (tli, start_lsn, end_lsn) per summary file, directory order (C emits
    // the list unsorted, straight from the pg_wal/summaries scan).
    pub fn get_available_wal_summaries() -> PgResult<Vec<(u32, XLogRecPtr, XLogRecPtr)>>
);

/// One output row of pg_wal_summary_contents (walsummaryfuncs.c), already in
/// the SRF's column order: relfilenode, reltablespace, reldatabase,
/// relforknumber, relblocknumber, is_limit_block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalSummaryContentsRow {
    pub relfilenode: u32,
    pub reltablespace: u32,
    pub reldatabase: u32,
    pub relforknumber: i16,
    pub relblocknumber: i64,
    pub is_limit_block: bool,
}

seam_core::seam!(
    // The row stream of pg_wal_summary_contents for one summary file
    // identified by (tli, start_lsn, end_lsn), in C's emission order: per
    // relation fork, the limit_block row first (only when the limit block is
    // valid), then the modified blocks in blkreftable reader order (C does
    // not sort them here). Errors carry C's OpenWalSummaryFile /
    // ReadWalSummary message texts.
    pub fn wal_summary_contents(
        tli: u32,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr
    ) -> PgResult<Vec<WalSummaryContentsRow>>
);
