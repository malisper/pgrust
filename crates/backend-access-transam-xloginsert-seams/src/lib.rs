//! Seam declarations for the `backend-access-transam-xloginsert` unit
//! (`access/transam/xloginsert.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RmgrId, XLogRecPtr};
use types_error::PgResult;

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
    /// `XLogSetRecordFlags(flags)` — e.g. `XLOG_INCLUDE_ORIGIN`.
    pub fn xlog_set_record_flags(flags: u8)
);

seam_core::seam!(
    /// `XLogResetInsertion()` — forget a partially-constructed record (abort
    /// path).
    pub fn xlog_reset_insertion()
);

// --- backend-utils-init-postinit consumer (xloginsert.c) ---

seam_core::seam!(
    /// `InitXLogInsert()` (xloginsert.c): allocate the backend-local WAL record
    /// construction buffers. `Err` carries its OOM surface.
    pub fn init_xlog_insert() -> types_error::PgResult<()>
);
