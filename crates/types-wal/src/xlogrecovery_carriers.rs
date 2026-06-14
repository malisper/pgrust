//! Seam carrier/signature types for `access/transam/xlogrecovery.c`, hoisted
//! here (out of the consuming `backend-access-transam-xlogrecovery` crate) so the
//! page-read owners' seam crates (`xlogreader-seams`, `xlogprefetcher-seams`,
//! `xlog-seams`) can name them while depending only on `types-wal`, avoiding a
//! dependency cycle. The recovery crate re-exports them from `core` so its own
//! API and the seam decls share one vocabulary.
//!
//! These are the recovery-driver seam vocabulary types: the opaque decoded-WAL
//! record handle the recovery crate reads its fields through, the page-read
//! result enum, the WAL-source enum, and the bundled `ReadRecord` outcome.

extern crate alloc;

use alloc::string::String;

use crate::wal::RelFileLocator;
use types_core::{BlockNumber, ForkNumber, TimeLineID, XLogRecPtr};

/// Codes indicating where a WAL file was obtained from during recovery, or where
/// to attempt to get one. (`XLogSource`, xlogrecovery.c:211)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum XLogSource {
    /// request to read WAL from any source
    #[default]
    Any = 0,
    /// restored using restore_command
    Archive,
    /// existing file in pg_wal
    PgWal,
    /// streamed from primary
    Stream,
}

/// Opaque handle to the externally-owned, decoded WAL record currently being
/// read/applied (the xlogreader/prefetcher's `XLogReaderState` + its decoded
/// `DecodedXLogRecord`). The recovery crate never owns or decodes the record; it
/// only reads its fields through the `xlog_rec_*` / `record_*` seams keyed by
/// this handle. A value of 0 is the C `NULL` reader.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecordRef(pub u64);

/// Result of a WAL page-read attempt (`XLogPageReadResult`, xlogreader.h):
/// success, hard failure, or (nonblocking) would-block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XLogPageReadResult {
    /// WAL page is valid and read into the buffer.
    Success,
    /// WAL page is not available (only in nonblocking mode).
    Fail,
    /// In nonblocking mode, no data available yet.
    WouldBlock,
}

/// A decoded block reference returned by `xlog_rec_block_tag` for
/// `xlog_block_info` / `verifyBackupPageConsistency`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedBlockTag {
    pub in_use: bool,
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blknum: BlockNumber,
}

/// The outcome of reading the next record via the prefetcher
/// (`XLogPrefetcherReadRecord`): the decoded-record handle plus the reader-state
/// fields the `ReadRecord` retry loop inspects, or an error message.
#[derive(Clone, Debug, Default)]
pub struct ReadRecordResult {
    /// `NULL` (handle 0) means end-of-WAL / no record decoded.
    pub record: RecordRef,
    pub read_rec_ptr: XLogRecPtr,
    pub end_rec_ptr: XLogRecPtr,
    /// The reader's `errormsg_buf` text, if a decode error was reported.
    pub errormsg: Option<String>,
    /// `xlogreader->abortedRecPtr` (set when WAL ends mid-record).
    pub aborted_rec_ptr: XLogRecPtr,
    /// `xlogreader->missingContrecPtr`.
    pub missing_contrec_ptr: XLogRecPtr,
    /// `xlogreader->latestPageTLI` — the TLI of the most recently read page,
    /// checked against the timeline history.
    pub latest_page_tli: TimeLineID,
    /// `xlogreader->latestPagePtr` — the LSN of that page.
    pub latest_page_ptr: XLogRecPtr,
    /// `xlogreader->seg.ws_tli` — the segment file TLI (for the error message).
    pub seg_tli: TimeLineID,
    /// where the read got its data from (`readSource`), reported back so the
    /// driver can track the current source.
    pub read_source: XLogSource,
}
