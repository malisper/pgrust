use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn alter_type_owner_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_oid: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // upstream 2780538433fc (18.5): is_readd — true from tablecmds'
    // AT_ReAddDomainConstraint rebuild, which must not re-check USAGE.
    pub fn alter_domain_add_constraint<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        names: &types_nodes::NodeList<'mcx>,
        new_constraint: types_nodes::Node<'mcx>,
        is_readd: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // AlterTypeNamespaceInternal (typecmds.c); tablecmds' SET SCHEMA rowtype leg.
    pub fn alter_type_namespace_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_oid: Oid,
        nsp_oid: Oid,
        is_implicit_array: bool,
        ignore_dependent: bool,
        error_on_table_type: bool,
        objs_moved: &mut mcx::PgVec<'mcx, pg_depend::ObjectAddress>,
    ) -> PgResult<Oid>
);
