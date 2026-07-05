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

seam_core::seam!(
    // RangeVarCallbackMaintainsTable (tablecmds.c); cluster consumes via seam
    // (tablecmds depends on commands_cluster for the ALTER rewrite lane).
    pub fn range_var_callback_maintains_table(
        relation: &rel_vocab::RangeVar<'_>,
        rel_id: types_core::Oid,
        old_rel_id: types_core::Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // SetRelationHasSubclass; seam because tablecmds depends on catalog_index.
    pub fn set_relation_has_subclass<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::Oid,
        relhassubclass: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // check_of_type; seam because tablecmds depends on parse_utilcmd.
    pub fn check_of_type<'mcx>(mcx: mcx::Mcx<'mcx>, typeid: types_core::Oid) -> PgResult<()>
);
