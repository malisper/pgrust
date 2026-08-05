use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    // XLogBeginInsert + XLogRegisterData per fragment + XLogInsert(rmid, info)
    // (xloginsert.c). Must not re-enter inval: callers may hold state borrows.
    pub fn xlog_insert(rmid: u8, info: u8, fragments: &[&[u8]]) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // As xlog_insert, plus XLogSetRecordFlags(flags) before the insert
    // (commit/abort records pass XLOG_INCLUDE_ORIGIN).
    pub fn xlog_insert_with_flags(
        rmid: u8,
        info: u8,
        flags: u8,
        fragments: &[&[u8]],
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    pub fn xlog_reset_insertion()
);

seam_core::seam!(
    // InitXLogInsert (xloginsert.c): per-backend WAL construction buffers.
    pub fn init_xlog_insert() -> PgResult<()>
);

/// One `XLogRegisterBuffer(block_id, buffer, flags)` plus its
/// `XLogRegisterBufData` fragments.
pub struct XLogRegBuf<'a> {
    pub block_id: u8,
    pub buffer: types_core::Buffer,
    pub flags: u8,
    pub bufdata: &'a [&'a [u8]],
}

// REGBUF_* (xloginsert.h).
pub const REGBUF_FORCE_IMAGE: u8 = 0x01;
pub const REGBUF_NO_IMAGE: u8 = 0x02;
// C: (0x04 | REGBUF_NO_IMAGE) — WILL_INIT implies no full-page image; redo
// rebuilds the page from block data (heap_xlog_insert INIT_PAGE contract).
pub const REGBUF_WILL_INIT: u8 = 0x04 | 0x02;
pub const REGBUF_STANDARD: u8 = 0x08;
pub const REGBUF_KEEP_DATA: u8 = 0x10;
pub const REGBUF_NO_CHANGE: u8 = 0x20;

seam_core::seam!(
    // XLogBeginInsert + XLogRegisterData(main_data fragments) + one
    // XLogRegisterBuffer/XLogRegisterBufData group per entry +
    // XLogSetRecordFlags(record_flags) + XLogInsert(rmid, info): the
    // buffer-registering record form heap/nbtree DML needs.
    pub fn xlog_insert_record(
        rmid: u8,
        info: u8,
        record_flags: u8,
        main_data: &[&[u8]],
        bufs: &[XLogRegBuf<'_>],
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // log_newpage_buffer(buffer, page_std) (xloginsert.c): FPI of a pinned,
    // exclusively locked buffer inside a critical section.
    pub fn log_newpage_buffer(buffer: types_core::Buffer, page_std: bool) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // XLogSaveBufferForHint(buffer, buffer_std) (xloginsert.c): caller holds
    // pin + at least share lock; InvalidXLogRecPtr when no FPI was needed.
    pub fn xlog_save_buffer_for_hint(
        buffer: types_core::Buffer,
        buffer_std: bool,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // XLogCheckBufferNeedsBackup(buffer) (xloginsert.c).
    pub fn xlog_check_buffer_needs_backup(buffer: types_core::Buffer) -> bool
);
