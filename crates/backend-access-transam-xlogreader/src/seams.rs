//! Seam installation for the `backend-access-transam-xlogreader` unit.
//!
//! Installs the value-typed `XLogReaderState`-based seams this crate owns
//! (declared in `backend-access-transam-xlogreader-seams`) from
//! [`init_seams`], wired into `seams-init`. The handle-based logical-decoding
//! and walsummarizer seams in that crate are owned by *those* units (the
//! logical-decoding private reader / walsummarizer), not by xlogreader.c, and
//! are installed there.

extern crate alloc;

use alloc::string::String;

use types_core::primitive::{Buffer, TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_wal::rmgr::XLogReaderState;
use types_wal::{DecodedBkpBlock, ReadAheadRecordInfo};

use backend_access_transam_xlogreader_seams as seam;

/// Re-export of the seam's `XLogBlockTag` for the decoder accessors.
pub use seam::XLogBlockTag;

/// Re-export of the bufmgr write-page seam used by `RestoreBlockImage`.
pub use backend_storage_buffer_bufmgr_seams::with_buffer_page;

/// `XLogReadAhead` seam adapter: the seam returns the `Copy` header facts of
/// the newly decoded record (the decode-queue tail). On success our internal
/// `XLogReadAhead` returns `Some(())`; we then project the tail.
fn xlog_read_ahead(
    reader: &mut XLogReaderState<'_>,
    nonblocking: bool,
) -> PgResult<Option<ReadAheadRecordInfo>> {
    match super::XLogReadAhead(reader, nonblocking)? {
        Some(()) => Ok(super::read_ahead_record_info(reader)),
        None => Ok(None),
    }
}

/// `xlog_reader_deferred_errmsg` seam adapter: the seam borrows `&'r str` from
/// the reader's `errormsg_buf`. The reader's deferred message lives in the
/// arena-backed `errormsg_buf`; borrow it directly.
fn xlog_reader_deferred_errmsg<'r>(reader: &'r XLogReaderState<'_>) -> Option<&'r str> {
    reader
        .errormsg_buf
        .as_ref()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
}

/// `xlog_rec_get_block_tag_extended` seam adapter (the seam returns
/// `PgResult`; the decode accessor is infallible here — a bad block id is
/// `None`).
fn xlog_rec_get_block_tag_extended(
    record: &XLogReaderState<'_>,
    block_id: u8,
) -> PgResult<Option<XLogBlockTag>> {
    Ok(super::xlog_rec_get_block_tag_extended(record, block_id))
}

fn xlog_rec_get_block_flags(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<u8> {
    Ok(super::xlog_rec_get_block_flags(record, block_id))
}

fn xlog_rec_block_image_apply(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<bool> {
    Ok(super::xlog_rec_block_image_apply(record, block_id))
}

fn xlog_rec_has_block_image(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<bool> {
    Ok(super::xlog_rec_has_block_image(record, block_id))
}

fn read_ahead_record_block<'r>(
    reader: &'r XLogReaderState<'_>,
    block_id: i32,
) -> DecodedBkpBlock<'r> {
    super::read_ahead_record_block(reader, block_id)
}

fn read_ahead_record_main_data<'r>(reader: &'r XLogReaderState<'_>) -> &'r [u8] {
    super::read_ahead_record_main_data(reader)
}

fn reader_errormsg_buf(record: &XLogReaderState<'_>) -> String {
    super::reader_errormsg_buf(record)
}

fn decode_queue_head_lsn(reader: &XLogReaderState<'_>) -> Option<XLogRecPtr> {
    super::decode_queue_head_lsn(reader)
}

fn decode_queue_tail_lsn(reader: &XLogReaderState<'_>) -> Option<XLogRecPtr> {
    super::decode_queue_tail_lsn(reader)
}

fn xlog_reader_has_queued_record_or_error(reader: &XLogReaderState<'_>) -> bool {
    super::XLogReaderHasQueuedRecordOrError(reader)
}

fn xlog_release_previous_record(reader: &mut XLogReaderState<'_>) -> XLogRecPtr {
    super::XLogReleasePreviousRecord(reader)
}

fn xlog_next_record(reader: &mut XLogReaderState<'_>) -> Option<XLogRecPtr> {
    super::XLogNextRecord(reader)
}

fn xlog_begin_read(reader: &mut XLogReaderState<'_>, rec_ptr: XLogRecPtr) {
    super::XLogBeginRead(reader, rec_ptr)
}

fn set_read_ahead_record_prefetch_buffer(
    reader: &mut XLogReaderState<'_>,
    block_id: i32,
    buffer: Buffer,
) {
    super::set_read_ahead_record_prefetch_buffer(reader, block_id, buffer)
}

fn restore_block_image(
    record: &XLogReaderState<'_>,
    block_id: u8,
    buf: Buffer,
) -> PgResult<bool> {
    super::restore_block_image(record, block_id, buf)
}

fn restore_block_image_bytes(
    record: &XLogReaderState<'_>,
    block_id: u8,
    page: &mut [u8],
) -> PgResult<bool> {
    super::restore_block_image_bytes(record, block_id, page)
}

fn reader_read_len(reader: &XLogReaderState<'_>) -> u32 {
    super::reader_read_len(reader)
}
fn reader_seg_segno(reader: &XLogReaderState<'_>) -> XLogSegNo {
    super::reader_seg_segno(reader)
}
fn reader_segoff(reader: &XLogReaderState<'_>) -> u32 {
    super::reader_segoff(reader)
}
fn reader_seg_size(reader: &XLogReaderState<'_>) -> i32 {
    super::reader_seg_size(reader)
}
fn reader_curr_tli(reader: &XLogReaderState<'_>) -> TimeLineID {
    super::reader_curr_tli(reader)
}
fn reader_curr_tli_valid_until(reader: &XLogReaderState<'_>) -> XLogRecPtr {
    super::reader_curr_tli_valid_until(reader)
}
fn reader_set_curr_tli(reader: &mut XLogReaderState<'_>, tli: TimeLineID) {
    super::reader_set_curr_tli(reader, tli)
}
fn reader_set_curr_tli_valid_until(reader: &mut XLogReaderState<'_>, lsn: XLogRecPtr) {
    super::reader_set_curr_tli_valid_until(reader, lsn)
}
fn reader_set_next_tli(reader: &mut XLogReaderState<'_>, tli: TimeLineID) {
    super::reader_set_next_tli(reader, tli)
}
fn reader_set_private_end_of_wal(reader: &mut XLogReaderState<'_>) {
    super::reader_set_private_end_of_wal(reader)
}
fn reader_set_ws_file(reader: &mut XLogReaderState<'_>, fd: i32) {
    super::reader_set_ws_file(reader, fd)
}
fn reader_close_ws_file(reader: &mut XLogReaderState<'_>) {
    super::reader_close_ws_file(reader)
}

/// Install every inward seam this unit (`xlogreader.c`) owns.
pub fn init_seams() {
    seam::xlog_reader_has_queued_record_or_error::set(xlog_reader_has_queued_record_or_error);
    seam::xlog_read_ahead::set(xlog_read_ahead);
    seam::decode_queue_head_lsn::set(decode_queue_head_lsn);
    seam::decode_queue_tail_lsn::set(decode_queue_tail_lsn);
    seam::read_ahead_record_block::set(read_ahead_record_block);
    seam::read_ahead_record_main_data::set(read_ahead_record_main_data);
    seam::set_read_ahead_record_prefetch_buffer::set(set_read_ahead_record_prefetch_buffer);
    seam::xlog_release_previous_record::set(xlog_release_previous_record);
    seam::xlog_next_record::set(xlog_next_record);
    seam::xlog_reader_deferred_errmsg::set(xlog_reader_deferred_errmsg);
    seam::xlog_begin_read::set(xlog_begin_read);

    seam::xlog_rec_get_block_tag_extended::set(xlog_rec_get_block_tag_extended);
    seam::xlog_rec_get_block_flags::set(xlog_rec_get_block_flags);
    seam::xlog_rec_block_image_apply::set(xlog_rec_block_image_apply);
    seam::xlog_rec_has_block_image::set(xlog_rec_has_block_image);
    seam::restore_block_image::set(restore_block_image);
    seam::restore_block_image_bytes::set(restore_block_image_bytes);
    seam::reader_errormsg_buf::set(reader_errormsg_buf);

    seam::reader_read_len::set(reader_read_len);
    seam::reader_seg_segno::set(reader_seg_segno);
    seam::reader_segoff::set(reader_segoff);
    seam::reader_seg_size::set(reader_seg_size);
    seam::reader_curr_tli::set(reader_curr_tli);
    seam::reader_curr_tli_valid_until::set(reader_curr_tli_valid_until);
    seam::reader_set_curr_tli::set(reader_set_curr_tli);
    seam::reader_set_curr_tli_valid_until::set(reader_set_curr_tli_valid_until);
    seam::reader_set_next_tli::set(reader_set_next_tli);
    seam::reader_set_private_end_of_wal::set(reader_set_private_end_of_wal);
    seam::reader_set_ws_file::set(reader_set_ws_file);
    seam::reader_close_ws_file::set(reader_close_ws_file);
}
