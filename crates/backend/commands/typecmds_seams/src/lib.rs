use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // typecmds depends on tablecmds, so the ALTER OWNER edge is a seam.
    pub fn alter_type_owner_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_oid: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // AlterDomainAddConstraint (typecmds.c) for tablecmds'
    // AT_ReAddDomainConstraint; seam because typecmds depends on tablecmds.
    pub fn alter_domain_add_constraint<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        names: &types_nodes::NodeList<'mcx>,
        new_constraint: types_nodes::Node<'mcx>,
    ) -> PgResult<()>
);
