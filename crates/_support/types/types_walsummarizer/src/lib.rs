//! Seam-carrier vocabulary for `backend/postmaster/walsummarizer.c`.
//!
//! `WalSummarizerData` is the shared-memory control block (`#[repr(C)]`,
//! placed via `ShmemInitStruct` and guarded by `WALSummarizerLock`); the rest
//! are carriers crossing the summarizer's outward seams (the xlogreader
//! result/block-tag shapes, the on-disk summary-file descriptor, and the
//! relfilelocator list parsed from a commit/abort record).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use condvar::ConditionVariable;
use types_core::{BlockNumber, ForkNumber, ProcNumber, TimeLineID, XLogRecPtr};
use types_storage::RelFileLocator;

/// Data in shared memory related to WAL summarization (`WalSummarizerData`).
///
/// `summary_file_cv` embeds the faithful `#[repr(C)]` `ConditionVariable` so
/// this struct's layout matches C when placed into shmem via
/// `ShmemInitStruct`. All fields other than `summary_file_cv` are protected by
/// `WALSummarizerLock`.
#[repr(C)]
#[derive(Debug)]
pub struct WalSummarizerData {
    /// Until summary files on disk are discovered, `initialized` is false and
    /// the other fields contain no meaningful information.
    pub initialized: bool,
    /// TLI at which the next summary file will start.
    pub summarized_tli: TimeLineID,
    /// LSN at which the next summary file will start.
    pub summarized_lsn: XLogRecPtr,
    /// True if `summarized_lsn` is necessarily the start of a WAL record.
    pub lsn_is_exact: bool,
    /// Proc number of the summarizer process, or `INVALID_PROC_NUMBER`.
    pub summarizer_pgprocno: ProcNumber,
    /// Ending LSN of a record recently read; `>= summarized_lsn`.
    pub pending_lsn: XLogRecPtr,
    /// Handles its own synchronization.
    pub summary_file_cv: ConditionVariable,
}

/// The relevant fields parsed out of a commit/abort xact record
/// (`xl_xact_parsed_commit` / `xl_xact_parsed_abort` `xlocators`).
#[derive(Clone, Debug, Default)]
pub struct ParsedXactRels {
    pub xlocators: Vec<RelFileLocator>,
}

/// Opaque handle to the xlogreader allocated for one summary pass (the C
/// `XLogReaderState *` of the summarizer's private reader). A registry token
/// the xlogreader owner maps to the live reader; the summarizer keys its
/// `SummarizerReadLocalXLogPrivate` private-data by the same token. A newtype
/// rather than a bare `u64` so it cannot be confused with an LSN or any other
/// integer at a seam boundary (types.md rule 7).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct XLogReaderHandle(pub u64);

/// Result of one `XLogReadRecord` call.
#[derive(Clone, Debug)]
pub enum ReadRecordResult {
    /// A record was read; the reader's `ReadRecPtr`/`EndRecPtr` advanced.
    Record,
    /// `XLogReadRecord` returned NULL with an error message set.
    Error { errormsg: Option<String> },
    /// `XLogReadRecord` returned NULL because the reader hit end-of-WAL.
    EndOfWal,
}

/// A block tag as returned by `XLogRecGetBlockTagExtended`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockTag {
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blocknum: BlockNumber,
}

/// One WAL summary file on disk (`WalSummaryFile`, `backup/walsummary.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalSummaryFile {
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub tli: TimeLineID,
}
