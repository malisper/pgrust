//! Seam declarations for the `backend-backup-walsummary` unit
//! (`backup/walsummary.c`): listing, pruning, and writing WAL summary files.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;
use types_walsummarizer::WalSummaryFile;

seam_core::seam!(
    /// `GetWalSummaries(tli, start_lsn, end_lsn)` (walsummary.c) — return the
    /// summary files in `$PGDATA/pg_wal/summaries` matching `tli` (0 = any)
    /// and overlapping `[start_lsn, end_lsn)` (`InvalidXLogRecPtr` =
    /// unbounded). The `List *` and its `WalSummaryFile` cells are palloc'd in
    /// the caller's context, so the port allocates in `mcx`; `Err` carries the
    /// directory-scan `ereport(ERROR)` and OOM.
    pub fn get_wal_summaries<'mcx>(
        mcx: Mcx<'mcx>,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) -> PgResult<PgVec<'mcx, WalSummaryFile>>
);

seam_core::seam!(
    /// `RemoveWalSummaryIfOlderThan(ws, cutoff_time)` (walsummary.c) — unlink
    /// the summary file if its last-modification time precedes `cutoff_time`.
    /// `Err` carries the stat/unlink `ereport(ERROR)`.
    pub fn remove_wal_summary_if_older_than(
        ws: WalSummaryFile,
        cutoff_time: i64,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// Write a serialized summary to `temp_path`, then `durable_rename` it to
    /// `final_path` — the SummarizeWAL file-emit sequence
    /// (`PathNameOpenFile(O_WRONLY|O_CREAT|O_TRUNC)`, `WriteBlockRefTable`
    /// streaming `WriteWalSummary`, `FileClose`, `durable_rename(..., ERROR)`).
    /// The port hands the already-serialized `bytes`. `Err` carries the
    /// file-create/write/rename `ereport(ERROR)`.
    pub fn write_wal_summary_file(
        temp_path: &str,
        final_path: &str,
        bytes: &[u8],
    ) -> PgResult<()>
);
