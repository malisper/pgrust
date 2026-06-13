//! Seam declarations for the `backend-access-transam-xloginsert` unit
//! (`access/transam/xloginsert.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RmgrId, XLogRecPtr};
use types_error::PgResult;
use types_storage::Buffer;

seam_core::seam!(
    /// One whole-record insertion: C's `XLogBeginInsert()`, one
    /// `XLogRegisterData(data, len)` per `fragments` entry (in order),
    /// `XLogSetRecordFlags(flags)` (skipped when `flags == 0`), then
    /// `XLogInsert(rmid, info)`. Returns the record's end LSN. The ambient
    /// rdata-chain registration protocol stays on the owner's side of the
    /// boundary.
    pub fn xlog_insert(
        rmid: RmgrId,
        info: u8,
        flags: u8,
        fragments: &[&[u8]],
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `XLogBeginInsert()` — start building a WAL record. `elog(ERROR)`s if
    /// already in insert mode or during recovery.
    pub fn xlog_begin_insert() -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterData(data, len)` — append one chunk of record data (the
    /// installed impl copies the bytes; C keeps the caller's pointer alive
    /// until `XLogInsert`). `elog(ERROR)` on too many data chunks.
    pub fn xlog_register_data(data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterBuffer(block_id, buffer, flags)` — register a buffer with
    /// the record under construction (`flags` is the `REGBUF_*` bitmask).
    /// `elog(ERROR)` on a bad block_id or double registration.
    pub fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterBufData(block_id, data, len)` — append per-block data to a
    /// previously registered buffer (the installed impl copies the bytes; C
    /// keeps the caller's pointer alive until `XLogInsert`). `elog(ERROR)` on
    /// an unregistered block or too much data.
    pub fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogInsert(rmid, info)` — assemble and insert the WAL record built up
    /// by the preceding begin/register calls, returning its end LSN.
    /// `elog(ERROR)`/`PANIC` on insertion failure.
    pub fn xlog_insert_record(rmid: RmgrId, info: u8) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `XLogSetRecordFlags(flags)` — e.g. `XLOG_INCLUDE_ORIGIN`.
    pub fn xlog_set_record_flags(flags: u8)
);

seam_core::seam!(
    /// `XLogResetInsertion()` — forget a partially-constructed record (abort
    /// path).
    pub fn xlog_reset_insertion()
);
