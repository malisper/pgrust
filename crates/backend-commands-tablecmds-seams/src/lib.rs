//! Seam declarations for the `backend-commands-tablecmds` unit
//! (`commands/tablecmds.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_storage::lock::LOCKMODE;

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

seam_core::seam!(
    /// `ATExecChangeOwner(relationOid, newOwnerId, recursing, lockmode)`
    /// (tablecmds.c): change a relation's owner (and its dependent objects:
    /// indexes, owned sequences, toast tables). REASSIGN OWNED passes
    /// `recursing = true` so visiting a dependent before its parent doesn't
    /// fail. Can `ereport(ERROR)`, carried on `Err`.
    pub fn at_exec_change_owner(
        relation_oid: Oid,
        new_owner_id: Oid,
        recursing: bool,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);
