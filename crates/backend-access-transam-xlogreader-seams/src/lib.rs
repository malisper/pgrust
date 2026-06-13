//! Seam declarations for the `backend-access-transam-xlogreader` unit
//! (`access/transam/xlogreader.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::XLogRecPtr;
use types_logical::{XLogReadResult, XLogReaderHandle, XLogReaderRoutineHandle};

seam_core::seam!(
    /// `XLogReaderAllocate(wal_segment_size, NULL, xl_routine, ctx_private)` —
    /// `None` on OOM (the caller `ereport`s).
    pub fn XLogReaderAllocate(wal_segment_size: i32, xl_routine: XLogReaderRoutineHandle) -> Option<XLogReaderHandle>
);
seam_core::seam!(
    /// `XLogReaderFree(reader)`.
    pub fn XLogReaderFree(reader: XLogReaderHandle)
);
seam_core::seam!(
    /// `XLogBeginRead(reader, lsn)`.
    pub fn XLogBeginRead(reader: XLogReaderHandle, lsn: XLogRecPtr)
);
seam_core::seam!(
    /// `XLogReadRecord(reader, &err)`.
    pub fn XLogReadRecord(reader: XLogReaderHandle) -> XLogReadResult
);
seam_core::seam!(
    /// `reader->EndRecPtr`.
    pub fn reader_EndRecPtr(reader: XLogReaderHandle) -> XLogRecPtr
);
