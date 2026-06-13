//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TimestampTz;

seam_core::seam!(
    /// `GetXLogReceiptTime(*rtime, *fromStream)` — the last WAL receipt time
    /// and whether it arrived via streaming replication.
    pub fn get_xlog_receipt_time() -> (TimestampTz, bool)
);
