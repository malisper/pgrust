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
