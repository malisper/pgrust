//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::SubTransactionId;

seam_core::seam!(
    /// `AtEOXact_Files(isCommit)` — close transaction-lifetime files; WARNs
    /// about leaks at commit.
    pub fn at_eoxact_files(is_commit: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Files(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_files(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);
