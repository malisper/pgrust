//! Seam declarations for the `backend-access-transam-xlogreader` unit
//! (`access/transam/xlogreader.c`): the generic WAL read/decode facility,
//! including the handle-based subset consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! Every record/queue seam takes the reader explicitly (`XLogReaderState`, the
//! trimmed shared shape in `types_wal::rmgr`) — the reader is a function
//! parameter in C, never ambient state. The reader's
//! `ReadRecPtr`/`EndRecPtr`/`record` fields are public on the shared struct and
//! need no seam.
//!
//! The C WAL prefetcher holds the `DecodedXLogRecord *` that `XLogReadAhead`
//! returns and dereferences it across calls; that record lives in the
//! reader's decode buffer (it is the reader's decode-queue tail until the
//! prefetcher drops its reference), so Rust cannot hold the borrow across
//! further reader calls. `xlog_read_ahead` therefore returns the `Copy`
//! header facts ([`types_wal::ReadAheadRecordInfo`]) and the
//! `read_ahead_record_*` seams re-read the same record — the reader's
//! decode-queue tail — through a fresh borrow.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use types_core::primitive::{BlockNumber, ForkNumber};
use types_core::{Buffer, TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_storage::RelFileLocator;
use types_wal::rmgr::XLogReaderState;
use types_wal::{DecodedBkpBlock, ReadAheadRecordInfo};
use types_logical::{XLogReadResult, XLogReaderHandle, XLogReaderRoutineHandle};

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

// --- Handle-based subset consumed by logical decoding ---

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

// ---------------------------------------------------------------------------
// Decoded-record block accessors + reader field accessors consumed by
// xlogutils.c's redo fetchers and read_local_xlog_page / DetermineTimeline.
// The decoded record and the reader's seg/TLI/private fields live in the
// xlogreader-owned `XLogReaderState`; xlogutils reaches them through these.
// ---------------------------------------------------------------------------

/// The block tag of a registered block, from `XLogRecGetBlockTagExtended`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XLogBlockTag {
    /// `*rlocator` out-param.
    pub rlocator: RelFileLocator,
    /// `*forknum` out-param.
    pub forknum: ForkNumber,
    /// `*blknum` out-param.
    pub blkno: BlockNumber,
    /// `*prefetch_buffer` out-param.
    pub prefetch_buffer: Buffer,
}

seam_core::seam!(
    /// `XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum,
    /// &blknum, &prefetch_buffer)` (xlogreader.c) — `Some(tag)` when the block
    /// reference exists, `None` (C `false`) for a bogus `block_id`.
    pub fn xlog_rec_get_block_tag_extended(
        record: &XLogReaderState<'_>,
        block_id: u8,
    ) -> PgResult<Option<XLogBlockTag>>
);

seam_core::seam!(
    /// `XLogRecGetBlock(record, block_id)->flags` (xlogreader.h).
    pub fn xlog_rec_get_block_flags(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<u8>
);

seam_core::seam!(
    /// `XLogRecBlockImageApply(record, block_id)` (xlogreader.h) — whether the
    /// block's full-page image should be restored.
    pub fn xlog_rec_block_image_apply(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<bool>
);

seam_core::seam!(
    /// `XLogRecHasBlockImage(record, block_id)` (xlogreader.h).
    pub fn xlog_rec_has_block_image(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<bool>
);

seam_core::seam!(
    /// `RestoreBlockImage(record, block_id, BufferGetPage(buf))` (xlogreader.c)
    /// — decompress/copy the FPI onto the buffer's page. The page is owned by
    /// the bufmgr; the buffer id crosses the seam and the xlogreader side
    /// dereferences the page. Returns `false` (with `record->errormsg_buf`
    /// populated) on a decompress failure.
    pub fn restore_block_image(
        record: &XLogReaderState<'_>,
        block_id: u8,
        buf: Buffer,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `record->errormsg_buf` (xlogreader.c) — the reader's error text after a
    /// failed `RestoreBlockImage`.
    pub fn reader_errormsg_buf(record: &XLogReaderState<'_>) -> String
);

seam_core::seam!(
    /// `state->readLen` (xlogreader.c) — bytes of the current page already read.
    pub fn reader_read_len(reader: &XLogReaderState<'_>) -> u32
);

seam_core::seam!(
    /// `state->seg.ws_segno` (xlogreader.c).
    pub fn reader_seg_segno(reader: &XLogReaderState<'_>) -> XLogSegNo
);

seam_core::seam!(
    /// `state->segoff` (xlogreader.c).
    pub fn reader_segoff(reader: &XLogReaderState<'_>) -> u32
);

seam_core::seam!(
    /// `state->segcxt.ws_segsize` (xlogreader.c) — the WAL segment size (a C
    /// `int`).
    pub fn reader_seg_size(reader: &XLogReaderState<'_>) -> i32
);

seam_core::seam!(
    /// `state->currTLI` (xlogreader.c).
    pub fn reader_curr_tli(reader: &XLogReaderState<'_>) -> TimeLineID
);

seam_core::seam!(
    /// `state->currTLIValidUntil` (xlogreader.c).
    pub fn reader_curr_tli_valid_until(reader: &XLogReaderState<'_>) -> XLogRecPtr
);

seam_core::seam!(
    /// `state->currTLI = tli` (xlogreader.c).
    pub fn reader_set_curr_tli(reader: &mut XLogReaderState<'_>, tli: TimeLineID)
);

seam_core::seam!(
    /// `state->currTLIValidUntil = lsn` (xlogreader.c).
    pub fn reader_set_curr_tli_valid_until(reader: &mut XLogReaderState<'_>, lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `state->nextTLI = tli` (xlogreader.c).
    pub fn reader_set_next_tli(reader: &mut XLogReaderState<'_>, tli: TimeLineID)
);

seam_core::seam!(
    /// `((ReadLocalXLogPageNoWaitPrivate *) state->private_data)->end_of_wal =
    /// true` — flag the no-wait page-read caller that the end of WAL was
    /// reached. The private struct is owned by the reader's allocator.
    pub fn reader_set_private_end_of_wal(reader: &mut XLogReaderState<'_>)
);

seam_core::seam!(
    /// `state->seg.ws_file = fd` (xlogreader.c).
    pub fn reader_set_ws_file(reader: &mut XLogReaderState<'_>, fd: i32)
);

seam_core::seam!(
    /// `close(state->seg.ws_file); state->seg.ws_file = -1` (xlogreader.c).
    pub fn reader_close_ws_file(reader: &mut XLogReaderState<'_>)
);
