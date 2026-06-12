//! Seam declarations for the `backend-access-transam-subtrans` unit
//! (`access/transam/subtrans.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `SubTransSetParent(xid, parent)` — record the parent of a
    /// subtransaction in pg_subtrans; SLRU page access can `ereport(ERROR)`.
    pub fn sub_trans_set_parent(xid: TransactionId, parent: TransactionId) -> PgResult<()>
);
