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
