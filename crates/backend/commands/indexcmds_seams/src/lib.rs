use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // DefineIndex (indexcmds.c) for tablecmds' ATExecAddIndex; indexcmds
    // depends on tablecmds, so the ALTER edge is a seam.
    pub fn define_index_for_alter<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        table_id: Oid,
        stmt: types_nodes::Node<'mcx>,
        skip_build: bool,
    ) -> PgResult<Oid>
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
