//! Seam declarations for the `backend-backup-walsummary` unit
//! (`backup/walsummary.c`): listing, pruning, and writing WAL summary files.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use ::types_blkreftable::BlockRefTableReader;
use types_core::{TimeLineID, XLogRecPtr};
use ::types_error::PgResult;
use ::types_storage::file::File;
use ::types_walsummarizer::WalSummaryFile;

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

seam_core::seam!(
    /// The `pg_wal_summary_contents` reader setup
    /// (`OpenWalSummaryFile(&ws, false)` + `CreateBlockRefTableReader(
    /// ReadWalSummary, &io, FilePathName(io.file), ReportWalSummaryError,
    /// NULL)`): open the summary file identified by `ws` and wrap it in a
    /// `BlockRefTableReader`. The `File`, the `ReadWalSummary` read callback
    /// (over the `WalSummaryIO` cursor), and the `ReportWalSummaryError`
    /// error callback are all walsummary/fd-owned, so the open + reader
    /// construction is bundled into this single owner seam. It returns the
    /// owned `BlockRefTableReader` (the C `BlockRefTableReader *` — the open
    /// `File` is captured by the reader's `ReadWalSummary` read callback) plus
    /// the `File` handle the caller threads to the matching
    /// [`wal_summary_reader_file_close`] teardown (a `Copy` VFD descriptor, so
    /// it can be both captured by the callback and returned for the eventual
    /// `FileClose`). `Err` carries the file-open / reader-create
    /// `ereport(ERROR)`.
    pub fn wal_summary_create_reader<'mcx>(
        mcx: Mcx<'mcx>,
        ws: WalSummaryFile,
    ) -> PgResult<(BlockRefTableReader, File)>
);

seam_core::seam!(
    /// The `pg_wal_summary_contents` reader teardown: after
    /// `DestroyBlockRefTableReader(reader)` (blkreftable-owned), `FileClose(
    /// io.file)` closes the WAL summary file the reader was reading. The open
    /// `File` (a `Copy` VFD descriptor) is the one returned by
    /// [`wal_summary_create_reader`]; the caller threads it here. Infallible in C.
    pub fn wal_summary_reader_file_close(file: File)
);
