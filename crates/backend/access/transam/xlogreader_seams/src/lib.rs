//! Seam declarations for the `backend-access-transam-xlogreader` unit
//! (`access/transam/xlogreader.c`): the generic WAL read/decode facility,
//! including the handle-based subset consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! Every record/queue seam takes the reader explicitly (`XLogReaderState`, the
//! trimmed shared shape in `::wal::rmgr`) — the reader is a function
//! parameter in C, never ambient state. The reader's
//! `ReadRecPtr`/`EndRecPtr`/`record` fields are public on the shared struct and
//! need no seam.
//!
//! The C WAL prefetcher holds the `DecodedXLogRecord *` that `XLogReadAhead`
//! returns and dereferences it across calls; that record lives in the
//! reader's decode buffer (it is the reader's decode-queue tail until the
//! prefetcher drops its reference), so Rust cannot hold the borrow across
//! further reader calls. `xlog_read_ahead` therefore returns the `Copy`
//! header facts ([`::wal::ReadAheadRecordInfo`]) and the
//! `read_ahead_record_*` seams re-read the same record — the reader's
//! decode-queue tail — through a fresh borrow.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use ::types_core::primitive::{BlockNumber, ForkNumber, RepOriginId, TransactionId};
use ::types_core::{Buffer, TimeLineID, XLogRecPtr, XLogSegNo};
use ::types_error::PgResult;
use ::types_storage::RelFileLocator;
use ::wal::rmgr::{XLogReaderRoutine, XLogReaderState};
use ::wal::{DecodedBkpBlock, ReadAheadRecordInfo};
use ::types_logical::{XLogReadResult, XLogReaderHandle, XLogReaderRoutineHandle};
use ::types_walsummarizer::{BlockTag, ReadRecordResult};
use ::types_walsummarizer::XLogReaderHandle as SummarizerXLogReaderHandle;

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
    /// Resolve an opaque `XLogReaderRoutine *` (`XLogReaderRoutineHandle`) into
    /// the concrete `XLogReaderRoutine` (`page_read`/`segment_open`/
    /// `segment_close` callbacks) the reader dispatches through.
    ///
    /// `logical.c` only ever forwards the default handle (the
    /// `XL_ROUTINE(.page_read = read_local_xlog_page, .segment_open =
    /// wal_segment_open, .segment_close = wal_segment_close)` routine that lives
    /// in `xlogutils`), so the routine value crosses this OUTWARD seam from the
    /// `xlogutils` owner (downstream of `xlogreader`). `XLogReaderAllocate`
    /// stores the resolved routine into `state.routine`. Until `xlogutils`
    /// installs it (the routine-contract keystone), driving a page read
    /// mirror-pg-and-panics here.
    pub fn xlog_reader_routine_for_handle(handle: XLogReaderRoutineHandle) -> XLogReaderRoutine
);

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
    /// `RestoreBlockImage(record, block_id, page)` (xlogreader.c:2075) — the
    /// faithful C signature: decompress/copy the FPI onto a caller-provided
    /// `BLCKSZ` scratch page (C's `char *page`), never touching a shared
    /// buffer. Used by `verifyBackupPageConsistency` to mask + `memcmp`.
    /// Returns `false` (with `record->errormsg_buf` populated) on a decompress
    /// failure.
    pub fn restore_block_image_bytes(
        record: &XLogReaderState<'_>,
        block_id: u8,
        page: &mut [u8],
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
    fn(reader: SummarizerXLogReaderHandle, target_page_ptr: XLogRecPtr, req_len: i32, cur_page: &mut [u8]) -> PgResult<i32>;

seam_core::seam!(
    /// `XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(.page_read =
    /// page_read, .segment_open = wal_segment_open, .segment_close =
    /// wal_segment_close), NULL)` — allocate a private reader for one summary
    /// pass and return its registry handle. `Err` carries the C
    /// `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` for a failed allocation.
    pub fn summarizer_xlogreader_allocate(
        wal_segment_size: i32,
        page_read: SummarizerPageReadCB,
    ) -> PgResult<SummarizerXLogReaderHandle>
);

seam_core::seam!(
    /// `XLogReaderFree(reader)` — free the private reader and its registry
    /// slot. Infallible.
    pub fn summarizer_xlogreader_free(reader: SummarizerXLogReaderHandle)
);

seam_core::seam!(
    /// `XLogBeginRead(reader, start_lsn)` on the private reader.
    pub fn summarizer_xlog_begin_read(reader: SummarizerXLogReaderHandle, start_lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogFindNextRecord(reader, start_lsn)` — search forward for the start
    /// of the next record; returns `InvalidXLogRecPtr` (0) when none is found
    /// before end-of-WAL. `Err` carries an `ereport(ERROR)` from the page
    /// read.
    pub fn summarizer_xlog_find_next_record(
        reader: SummarizerXLogReaderHandle,
        start_lsn: XLogRecPtr,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `record = XLogReadRecord(reader, &errormsg)` — read the next record,
    /// discriminated into [`ReadRecordResult`] (record / deferred error /
    /// end-of-WAL, the latter via the summarizer's `private_data->end_of_wal`).
    /// `Err` carries an `ereport(ERROR)` from the page-read callback.
    pub fn summarizer_xlog_read_record(reader: SummarizerXLogReaderHandle) -> PgResult<ReadRecordResult>
);

seam_core::seam!(
    /// `WALRead(reader, buf, startptr, count, tli, &errinfo)`; on failure C
    /// calls `WALReadRaiseError(&errinfo)`, so the read error is `Err`.
    pub fn summarizer_wal_read(
        reader: SummarizerXLogReaderHandle,
        buf: &mut [u8],
        startptr: XLogRecPtr,
        count: i32,
        tli: TimeLineID,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `reader->EndRecPtr` of the private reader.
    pub fn summarizer_reader_end_rec_ptr(reader: SummarizerXLogReaderHandle) -> XLogRecPtr
);

seam_core::seam!(
    /// `reader->ReadRecPtr` of the private reader.
    pub fn summarizer_reader_read_rec_ptr(reader: SummarizerXLogReaderHandle) -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecGetRmid(reader)` — the resource-manager id of the current
    /// record.
    pub fn summarizer_rec_get_rmid(reader: SummarizerXLogReaderHandle) -> u8
);

seam_core::seam!(
    /// `XLogRecGetInfo(reader)` — the `xl_info` byte of the current record.
    pub fn summarizer_rec_get_info(reader: SummarizerXLogReaderHandle) -> u8
);

seam_core::seam!(
    /// `XLogRecGetData(reader)` — the main-data payload of the current record,
    /// copied into a fresh `Vec` (the C code reads it as a `char *`; an owned
    /// copy avoids holding a borrow into the reader's decode buffer across
    /// further seam calls).
    pub fn summarizer_rec_get_data(reader: SummarizerXLogReaderHandle) -> alloc::vec::Vec<u8>
);

seam_core::seam!(
    /// `XLogRecMaxBlockId(reader)` — highest block id referenced by the
    /// current record.
    pub fn summarizer_rec_max_block_id(reader: SummarizerXLogReaderHandle) -> i32
);

seam_core::seam!(
    /// `XLogRecGetBlockTagExtended(reader, block_id, &rlocator, &forknum,
    /// &blocknum, NULL)` — `None` when the block id has no tag (C returns
    /// `false`).
    pub fn summarizer_rec_get_block_tag_extended(
        reader: SummarizerXLogReaderHandle,
        block_id: i32,
    ) -> Option<BlockTag>
);

// ===========================================================================
// Record-field accessors consumed by `access/transam/xlogrecovery.c` during
// WAL replay, keyed by the recovery crate's decoded-record handle
// (`::wal::xlogrecovery_carriers::RecordRef`). These mirror the
// `XLogRecGetRmid` / `XLogRecGetInfo` / `XLogRecGetTotalLen` macros over the
// recovery driver's current `XLogReaderState *`. Declared here (the xlogreader
// owns the decoded record) but NOT installed: the recovery crate stays
// `needs-decomp` and the page-read driver that holds the live reader is not yet
// ported, so a call panics loudly until the owner lands.
// ===========================================================================

seam_core::seam!(
    /// `XLogRecGetRmid(record)` — the resource-manager id of the record the
    /// recovery driver is currently replaying.
    pub fn xlog_rec_rmid(
        record: ::wal::xlogrecovery_carriers::RecordRef,
    ) -> u8
);

seam_core::seam!(
    /// `XLogRecGetInfo(record)` — the `xl_info` byte of the record the recovery
    /// driver is currently replaying.
    pub fn xlog_rec_info(
        record: ::wal::xlogrecovery_carriers::RecordRef,
    ) -> u8
);

seam_core::seam!(
    /// `XLogRecGetTotalLen(record)` — the total on-disk length of the record the
    /// recovery driver is currently replaying.
    pub fn xlog_rec_total_len(
        record: ::wal::xlogrecovery_carriers::RecordRef,
    ) -> u32
);

// ---------------------------------------------------------------------------
// Handle-based decoded-record accessors consumed by logical decoding
// (`decode.c`).
//
// During logical decoding the live `XLogReaderState` (with its current
// `DecodedXLogRecord`) lives in the xlogreader owner's backend-local registry
// behind an `XLogReaderHandle`; it cannot be borrowed across a seam. These
// accessors re-read the reader's current decoded record (`reader->record`)
// through the handle, mirroring the C `XLogRecGet*` macros that dereference
// `record->record_origin` / `record->header.xl_*` / `record->main_data` /
// `record->blocks[block_id]`. Owned (`Vec<u8>`) returns copy the bytes out of
// the reader's decode arena (decode.c immediately `memcpy`s them into the
// reorder buffer's own context, so the copy is faithful and short-lived).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `reader->ReadRecPtr` — start of the record currently being decoded.
    pub fn reader_ReadRecPtr(reader: XLogReaderHandle) -> XLogRecPtr
);
seam_core::seam!(
    /// `XLogRecGetInfo(record)` — the `xl_info` byte of the reader's current
    /// decoded record.
    pub fn xlog_rec_get_info(reader: XLogReaderHandle) -> u8
);
seam_core::seam!(
    /// `XLogRecGetRmid(record)` — the resource-manager id of the current record.
    pub fn xlog_rec_get_rmid(reader: XLogReaderHandle) -> u8
);
seam_core::seam!(
    /// `XLogRecGetXid(record)` — the transaction id that produced the current
    /// record.
    pub fn xlog_rec_get_xid(reader: XLogReaderHandle) -> TransactionId
);
seam_core::seam!(
    /// `XLogRecGetTopXid(record)` — the top-level transaction id (`Invalid` when
    /// the record is not a subtransaction's).
    pub fn xlog_rec_get_top_xid(reader: XLogReaderHandle) -> TransactionId
);
seam_core::seam!(
    /// `XLogRecGetOrigin(record)` — the replication origin of the current
    /// record (`InvalidRepOriginId` when none).
    pub fn xlog_rec_get_origin(reader: XLogReaderHandle) -> RepOriginId
);
seam_core::seam!(
    /// `XLogRecGetData(record)` — the record's main data area, copied out.
    pub fn xlog_rec_get_main_data(reader: XLogReaderHandle) -> Vec<u8>
);
seam_core::seam!(
    /// `XLogRecGetDataLen(record)` — `record->main_data_len`.
    pub fn xlog_rec_get_main_data_len(reader: XLogReaderHandle) -> u32
);
seam_core::seam!(
    /// `XLogRecGetBlockTagExtended(record, block_id, &rlocator, ...)` — the
    /// relation locator of backup block `block_id`, or `None` when the block is
    /// not in use (the C `Assert(false)` / `return false` path).
    pub fn xlog_rec_get_block_tag(reader: XLogReaderHandle, block_id: u8) -> Option<RelFileLocator>
);
seam_core::seam!(
    /// `XLogRecGetBlockData(record, block_id, &len)` — the per-block data bytes
    /// of backup block `block_id`, copied out (`None` when the block has no
    /// data).
    pub fn xlog_rec_get_block_data(reader: XLogReaderHandle, block_id: u8) -> Option<Vec<u8>>
);
