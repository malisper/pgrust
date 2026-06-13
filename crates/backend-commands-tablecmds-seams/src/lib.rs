//! Seam declarations for the `backend-commands-tablecmds` unit
//! (`commands/tablecmds.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `PreCommit_on_commit_actions()` — ON COMMIT DROP / DELETE ROWS work;
    /// can `ereport(ERROR)`.
    pub fn pre_commit_on_commit_actions() -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_on_commit_actions(isCommit)`.
    pub fn at_eoxact_on_commit_actions(is_commit: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_on_commit_actions(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_on_commit_actions(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);
