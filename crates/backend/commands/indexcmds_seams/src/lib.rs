use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // ResolveOpClass (indexcmds.c) for tablecmds' ComputePartitionAttrs;
    // seam because indexcmds depends on tablecmds.
    pub fn resolve_opclass<'mcx>(
        opclass: &types_nodes::NodeList<'mcx>,
        attr_type: Oid,
        access_method_name: &str,
        access_method_id: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // DefineIndex (indexcmds.c) for tablecmds' ATExecAddIndex; indexcmds
    // depends on tablecmds, so the ALTER edge is a seam.
    pub fn define_index_for_alter<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        table_id: Oid,
        stmt: types_nodes::Node<'mcx>,
        is_rebuild: bool,
        skip_build: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // CheckIndexCompatible (indexcmds.c) for tablecmds' TryReuseIndex; seam
    // for the same dependency cycle.
    pub fn check_index_compatible<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        old_id: Oid,
        access_method_name: &str,
        attribute_list: &types_nodes::NodeList<'mcx>,
        exclusion_op_names: &types_nodes::NodeList<'mcx>,
        is_without_overlaps: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // DefineIndex; seam because indexcmds depends on tablecmds.
    pub fn define_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        table_id: Oid,
        stmt: &types_nodes::rawnodes::IndexStmt<'mcx>,
        index_relation_id: Oid,
        parent_index_id: Oid,
        parent_constraint_id: Oid,
        is_alter_table: bool,
        check_rights: bool,
        check_not_in_use: bool,
        skip_build: bool,
        quiet: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // IndexSetParentIndex (indexcmds.c); seam because indexcmds depends on
    // tablecmds.
    pub fn index_set_parent_index<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        partition_idx: &'a types_rel::Relation<'mcx>,
        parent_oid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // WaitForOlderSnapshots (indexcmds.c); seam because indexcmds depends on
    // tablecmds.
    pub fn wait_for_older_snapshots(limit_xmin: types_core::TransactionId) -> PgResult<()>
);

seam_core::seam!(
    // ChooseRelationName (indexcmds.c:2606, exported via defrem.h). C has
    // exactly ONE of these and parse_utilcmd.c:476 calls it; our port grew a
    // second, weaker copy in parse_utilcmd. It must be a seam rather than a
    // direct call because indexcmds already depends on parse_utilcmd.
    pub fn choose_relation_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        name1: &str,
        name2: Option<&str>,
        label: &str,
        namespaceid: Oid,
        isconstraint: bool,
    ) -> PgResult<mcx::PgString<'mcx>>
);
