//! Seam declarations for the `backend-backup-walsummary` unit
//! (`backup/walsummary.c`): listing, pruning, and writing WAL summary files.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_blkreftable::BlockRefTableReaderHandle;
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

seam_core::seam!(
    /// The `pg_wal_summary_contents` reader setup
    /// (`OpenWalSummaryFile(&ws, false)` + `CreateBlockRefTableReader(
    /// ReadWalSummary, &io, FilePathName(io.file), ReportWalSummaryError,
    /// NULL)`): open the summary file identified by `ws` and wrap it in a
    /// `BlockRefTableReader`. The `File`, the `ReadWalSummary` read callback
    /// (over the `WalSummaryIO` cursor), and the `ReportWalSummaryError`
    /// error callback are all walsummary/fd-owned, so the open + reader
    /// construction is bundled into this single owner seam (it allocates the
    /// reader in `mcx` and threads the open `File` through the reader's
    /// callback arg). `Err` carries the file-open / reader-create
    /// `ereport(ERROR)`.
    pub fn wal_summary_create_reader<'mcx>(
        mcx: Mcx<'mcx>,
        ws: WalSummaryFile,
    ) -> PgResult<BlockRefTableReaderHandle>
);

seam_core::seam!(
    /// `ReadWalSummary(wal_summary_io, data, length)` (walsummary.c) — the
    /// `io_callback` registered with `CreateBlockRefTableReader`. Reads up to
    /// `length` bytes at the `WalSummaryIO` cursor's current `filepos`
    /// (`FileRead(io->file, ..., WAIT_EVENT_WAL_SUMMARY_READ)`), advancing the
    /// cursor by the count read. The `WalSummaryIO` (open `File` + `filepos`)
    /// lives in the walsummary owner keyed by the reader handle, so blkreftable
    /// invokes this through the same handle. `Err` carries the read
    /// `ereport(ERROR)` (`could not read file`); a short/zero read is the
    /// normal end-of-data signal returned in the byte count.
    pub fn wal_summary_read<'mcx>(
        mcx: Mcx<'mcx>,
        reader: BlockRefTableReaderHandle,
        length: usize,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `ReportWalSummaryError(callback_arg, fmt, ...)` (walsummary.c) — the
    /// `error_callback` registered with `CreateBlockRefTableReader`. The C
    /// assembles the message from a printf-style format + varargs; in the owned
    /// model blkreftable assembles the message and passes it here, and this
    /// raises `ereport(ERROR, errcode(ERRCODE_DATA_CORRUPTED),
    /// errmsg_internal("%s", msg))`. Always returns `Err` (it never returns in
    /// C); keyed by the reader handle for symmetry with the read callback.
    pub fn wal_summary_report_error(
        reader: BlockRefTableReaderHandle,
        message: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `pg_wal_summary_contents` reader teardown: after
    /// `DestroyBlockRefTableReader(reader)` (blkreftable-owned), `FileClose(
    /// io.file)` closes the WAL summary file the reader was reading. The open
    /// `File` lives in the reader's walsummary-owned callback arg, so the
    /// close is seamed here keyed by the same reader handle. Infallible in C.
    pub fn wal_summary_reader_file_close(reader: BlockRefTableReaderHandle)
);
