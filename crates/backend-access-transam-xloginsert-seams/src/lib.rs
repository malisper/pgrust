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
