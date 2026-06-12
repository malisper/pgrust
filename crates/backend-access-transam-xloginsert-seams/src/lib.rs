//! Seam declarations for the `backend-access-transam-xloginsert` unit
//! (`access/transam/xloginsert.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RmgrId, XLogRecPtr};
use types_error::PgResult;

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
    /// `XLogSetRecordFlags(flags)` — e.g. `XLOG_INCLUDE_ORIGIN`.
    pub fn xlog_set_record_flags(flags: u8)
);

seam_core::seam!(
    /// `XLogInsert(rmid, info)` — insert the registered record; returns its
    /// end LSN.
    pub fn xlog_insert(rmid: RmgrId, info: u8) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `XLogResetInsertion()` — forget a partially-constructed record (abort
    /// path).
    pub fn xlog_reset_insertion()
);
