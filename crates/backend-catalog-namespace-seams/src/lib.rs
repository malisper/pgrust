//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::SubTransactionId;

seam_core::seam!(
    /// `AtEOXact_Namespace(isCommit, parallel)` — end-of-xact temp-namespace
    /// and search-path cleanup.
    pub fn at_eoxact_namespace(is_commit: bool, parallel: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Namespace(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_namespace(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);
