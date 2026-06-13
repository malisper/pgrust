//! Seam declarations for the `backend-libpq-be-fsstubs` unit
//! (`libpq/be-fsstubs.c`, large-object descriptors). The owning unit installs
//! these from its `init_seams()` when it lands; until then a call panics
//! loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `AtEOXact_LargeObject(isCommit)` — close large-object descriptors at
    /// transaction end.
    pub fn at_eoxact_large_object(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_LargeObject(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_large_object(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);
