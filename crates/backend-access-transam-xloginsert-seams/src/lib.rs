//! Seam declarations for the `backend-access-transam-xloginsert` unit
//! (`access/transam/xloginsert.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RmgrId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// `XLogBeginInsert()`.
    pub fn xlog_begin_insert()
);

seam_core::seam!(
    /// `XLogSetRecordFlags(flags)`.
    pub fn xlog_set_record_flags(flags: u8)
);

seam_core::seam!(
    /// `XLogRegisterData(data, len)` — append a record fragment to the rdata
    /// chain. C keeps the caller's pointer; the installed implementation
    /// copies the bytes into its chain instead.
    pub fn xlog_register_data(data: &[u8])
);

seam_core::seam!(
    /// `XLogInsert(rmid, info)` — insert the assembled record; returns its
    /// end LSN.
    pub fn xlog_insert(rmid: RmgrId, info: u8) -> PgResult<XLogRecPtr>
);
