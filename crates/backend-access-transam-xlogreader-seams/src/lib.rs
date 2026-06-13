//! Seam declarations for the `backend-access-transam-xlogreader` unit
//! (`access/transam/xlogreader.c`): the generic WAL read/decode facility.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! Every seam takes the reader explicitly (`XLogReaderState`, the trimmed
//! shared shape in `types_wal::rmgr`) — the reader is a function parameter in
//! C, never ambient state. The reader's `ReadRecPtr`/`EndRecPtr`/`record`
//! fields are public on the shared struct and need no seam.
//!
//! The C WAL prefetcher holds the `DecodedXLogRecord *` that `XLogReadAhead`
//! returns and dereferences it across calls; that record lives in the
//! reader's decode buffer (it is the reader's decode-queue tail until the
//! prefetcher drops its reference), so Rust cannot hold the borrow across
//! further reader calls. `xlog_read_ahead` therefore returns the `Copy`
//! header facts ([`types_wal::ReadAheadRecordInfo`]) and the
//! `read_ahead_record_*` seams re-read the same record — the reader's
//! decode-queue tail — through a fresh borrow.

extern crate alloc;

use types_core::{Buffer, TimeLineID, XLogRecPtr};
use types_error::PgResult;
use types_wal::rmgr::XLogReaderState;
use types_wal::{DecodedBkpBlock, ReadAheadRecordInfo};
use types_walsummarizer::{BlockTag, ReadRecordResult, XLogReaderHandle};

seam_core::seam!(
    /// `XLogReaderHasQueuedRecordOrError(reader)` (xlogreader.h inline) —
    /// whether decoded records or a deferred error are queued up.
    pub fn xlog_reader_has_queued_record_or_error(reader: &XLogReaderState<'_>) -> bool
);

seam_core::seam!(
    /// `XLogReadAhead(reader, nonblocking)` (xlogreader.c) — attempt to
    /// decode one more record ahead of replay. `Ok(Some(_))` carries the
    /// newly decoded record's header facts (the record becomes the reader's
    /// decode-queue tail); `Ok(None)` is the C NULL return (error deferred or
    /// no data in nonblocking mode). `Err` carries an `ereport(ERROR)` from
    /// the page-read callback.
    pub fn xlog_read_ahead(
        reader: &mut XLogReaderState<'_>,
        nonblocking: bool,
    ) -> PgResult<Option<ReadAheadRecordInfo>>
);

seam_core::seam!(
    /// `reader->decode_queue_head->lsn` — start LSN of the oldest decoded
    /// record, `None` when the decode queue is empty (`decode_queue_head ==
    /// NULL`).
    pub fn decode_queue_head_lsn(reader: &XLogReaderState<'_>) -> Option<XLogRecPtr>
);

seam_core::seam!(
    /// `reader->decode_queue_tail->lsn` — start LSN of the newest decoded
    /// record, `None` when the decode queue is empty (`decode_queue_tail ==
    /// NULL`).
    pub fn decode_queue_tail_lsn(reader: &XLogReaderState<'_>) -> Option<XLogRecPtr>
);

seam_core::seam!(
    /// `&record->blocks[block_id]` of the record most recently returned by
    /// `xlog_read_ahead` (the reader's decode-queue tail): a copy of the
    /// decoded block reference.
    pub fn read_ahead_record_block<'r>(
        reader: &'r XLogReaderState<'_>,
        block_id: i32,
    ) -> DecodedBkpBlock<'r>
);

seam_core::seam!(
    /// `record->main_data` of the record most recently returned by
    /// `xlog_read_ahead` (the reader's decode-queue tail): the raw main-data
    /// payload the caller casts to an `xl_*` record struct.
    pub fn read_ahead_record_main_data<'r>(reader: &'r XLogReaderState<'_>) -> &'r [u8]
);

seam_core::seam!(
    /// `record->blocks[block_id].prefetch_buffer = buffer` on the record most
    /// recently returned by `xlog_read_ahead` (the reader's decode-queue
    /// tail): the prefetcher's write-back consumed later by
    /// `XLogReadBufferForRedoExtended`.
    pub fn set_read_ahead_record_prefetch_buffer(
        reader: &mut XLogReaderState<'_>,
        block_id: i32,
        buffer: Buffer,
    )
);

seam_core::seam!(
    /// `XLogReleasePreviousRecord(reader)` (xlogreader.c) — release the last
    /// record returned by `XLogNextRecord`. Returns the LSN past its end, or
    /// `InvalidXLogRecPtr` (0) if there was no such record. Infallible (pure
    /// decode-buffer bookkeeping).
    pub fn xlog_release_previous_record(reader: &mut XLogReaderState<'_>) -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogNextRecord(reader, &errmsg)` (xlogreader.c) — consume the next
    /// record off the decode queue (it becomes `reader->record`); `Some` is
    /// the record's start LSN (`record->lsn`), `None` the C NULL return.
    /// Infallible (errors are deferred into the reader; on `None` read them
    /// with [`xlog_reader_deferred_errmsg`]).
    pub fn xlog_next_record(reader: &mut XLogReaderState<'_>) -> Option<XLogRecPtr>
);

seam_core::seam!(
    /// The reader's deferred error message: `reader->errormsg_deferred ?
    /// reader->errormsg_buf : NULL` — what `XLogNextRecord` stores through
    /// its `char **errmsg` out-param on the NULL return.
    pub fn xlog_reader_deferred_errmsg<'r>(reader: &'r XLogReaderState<'_>) -> Option<&'r str>
);

seam_core::seam!(
    /// `XLogBeginRead(reader, RecPtr)` (xlogreader.c) — position the reader
    /// to read records starting at `rec_ptr`, forgetting queued-up decoded
    /// records. Infallible.
    pub fn xlog_begin_read(reader: &mut XLogReaderState<'_>, rec_ptr: XLogRecPtr)
);

// ---------------------------------------------------------------------------
// Handle-based private-reader API used by the WAL summarizer (walsummarizer.c
// `SummarizeWAL`). The summarizer allocates its own `XLogReaderState` with a
// page-read callback (`summarizer_read_local_xlog_page`) and a per-reader
// `SummarizerReadLocalXLogPrivate` private_data block that the summarizer
// owns. Because the reader's full shape is not the trimmed shared
// `XLogReaderState`, the reader is named by an opaque registry token
// (`XLogReaderHandle`); the summarizer owns the private_data keyed by the
// same token.
// ---------------------------------------------------------------------------

/// The page-read callback the summarizer installs (`XLogPageReadCB` with
/// `.page_read = &summarizer_read_local_xlog_page`). Called by the reader
/// while a record read is in flight; `Ok(n)` is the number of valid bytes in
/// `cur_page` (or `-1` at end of a historic timeline), `Err` an
/// `ereport(ERROR)` raised inside the callback.
pub type SummarizerPageReadCB =
    fn(reader: XLogReaderHandle, target_page_ptr: XLogRecPtr, req_len: i32, cur_page: &mut [u8]) -> PgResult<i32>;

seam_core::seam!(
    /// `XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(.page_read =
    /// page_read, .segment_open = wal_segment_open, .segment_close =
    /// wal_segment_close), NULL)` — allocate a private reader for one summary
    /// pass and return its registry handle. `Err` carries the C
    /// `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` for a failed allocation.
    pub fn summarizer_xlogreader_allocate(
        wal_segment_size: i32,
        page_read: SummarizerPageReadCB,
    ) -> PgResult<XLogReaderHandle>
);

seam_core::seam!(
    /// `XLogReaderFree(reader)` — free the private reader and its registry
    /// slot. Infallible.
    pub fn summarizer_xlogreader_free(reader: XLogReaderHandle)
);

seam_core::seam!(
    /// `XLogBeginRead(reader, start_lsn)` on the private reader.
    pub fn summarizer_xlog_begin_read(reader: XLogReaderHandle, start_lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogFindNextRecord(reader, start_lsn)` — search forward for the start
    /// of the next record; returns `InvalidXLogRecPtr` (0) when none is found
    /// before end-of-WAL. `Err` carries an `ereport(ERROR)` from the page
    /// read.
    pub fn summarizer_xlog_find_next_record(
        reader: XLogReaderHandle,
        start_lsn: XLogRecPtr,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `record = XLogReadRecord(reader, &errormsg)` — read the next record,
    /// discriminated into [`ReadRecordResult`] (record / deferred error /
    /// end-of-WAL, the latter via the summarizer's `private_data->end_of_wal`).
    /// `Err` carries an `ereport(ERROR)` from the page-read callback.
    pub fn summarizer_xlog_read_record(reader: XLogReaderHandle) -> PgResult<ReadRecordResult>
);

seam_core::seam!(
    /// `WALRead(reader, buf, startptr, count, tli, &errinfo)`; on failure C
    /// calls `WALReadRaiseError(&errinfo)`, so the read error is `Err`.
    pub fn summarizer_wal_read(
        reader: XLogReaderHandle,
        buf: &mut [u8],
        startptr: XLogRecPtr,
        count: i32,
        tli: TimeLineID,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `reader->EndRecPtr` of the private reader.
    pub fn summarizer_reader_end_rec_ptr(reader: XLogReaderHandle) -> XLogRecPtr
);

seam_core::seam!(
    /// `reader->ReadRecPtr` of the private reader.
    pub fn summarizer_reader_read_rec_ptr(reader: XLogReaderHandle) -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecGetRmid(reader)` — the resource-manager id of the current
    /// record.
    pub fn summarizer_rec_get_rmid(reader: XLogReaderHandle) -> u8
);

seam_core::seam!(
    /// `XLogRecGetInfo(reader)` — the `xl_info` byte of the current record.
    pub fn summarizer_rec_get_info(reader: XLogReaderHandle) -> u8
);

seam_core::seam!(
    /// `XLogRecGetData(reader)` — the main-data payload of the current record,
    /// copied into a fresh `Vec` (the C code reads it as a `char *`; an owned
    /// copy avoids holding a borrow into the reader's decode buffer across
    /// further seam calls).
    pub fn summarizer_rec_get_data(reader: XLogReaderHandle) -> alloc::vec::Vec<u8>
);

seam_core::seam!(
    /// `XLogRecMaxBlockId(reader)` — highest block id referenced by the
    /// current record.
    pub fn summarizer_rec_max_block_id(reader: XLogReaderHandle) -> i32
);

seam_core::seam!(
    /// `XLogRecGetBlockTagExtended(reader, block_id, &rlocator, &forknum,
    /// &blocknum, NULL)` — `None` when the block id has no tag (C returns
    /// `false`).
    pub fn summarizer_rec_get_block_tag_extended(
        reader: XLogReaderHandle,
        block_id: i32,
    ) -> Option<BlockTag>
);
