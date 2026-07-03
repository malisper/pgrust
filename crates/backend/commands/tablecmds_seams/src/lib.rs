use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn pre_commit_on_commit_actions() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eoxact_on_commit_actions(is_commit: bool)
);

seam_core::seam!(
    pub fn at_eosubxact_on_commit_actions(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

seam_core::seam!(
    // remove_on_commit_action(relid) (tablecmds.c): infallible list marking.
    pub fn remove_on_commit_action(relid: types_core::Oid)
);

seam_core::seam!(
    // RenameRelationInternal (tablecmds.c) for cluster's toast renames.
    pub fn rename_relation_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::Oid,
        newname: &str,
        is_index: bool,
    ) -> PgResult<()>
);
