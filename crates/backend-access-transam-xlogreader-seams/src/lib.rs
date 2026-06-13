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

use types_core::{Buffer, XLogRecPtr};
use types_error::PgResult;
use types_wal::rmgr::XLogReaderState;
use types_wal::{DecodedBkpBlock, ReadAheadRecordInfo};

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
