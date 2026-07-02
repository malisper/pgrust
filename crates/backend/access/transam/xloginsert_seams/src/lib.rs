use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    // XLogBeginInsert + XLogRegisterData per fragment + XLogInsert(rmid, info)
    // (xloginsert.c). Must not re-enter inval: callers may hold state borrows.
    pub fn xlog_insert(rmid: u8, info: u8, fragments: &[&[u8]]) -> PgResult<XLogRecPtr>
);
