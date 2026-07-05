use types_core::Oid;
use types_error::PgResult;

pub mod builtins;

seam_core::seam!(
    pub fn nextval_internal(relid: Oid, check_permissions: bool) -> PgResult<i64>
);

seam_core::seam!(
    pub fn currval_internal(relid: Oid) -> PgResult<i64>
);

seam_core::seam!(
    pub fn lastval_internal() -> PgResult<i64>
);

seam_core::seam!(
    pub fn do_setval(relid: Oid, next: i64, iscalled: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn delete_sequence_tuple(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    // DDL edges: sequence depends on tablecmds, so these are seams.
    pub fn define_sequence<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_nodes::rawnodes::CreateSeqStmt<'mcx>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn alter_sequence<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_nodes::AlterSeqStmt<'mcx>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn reset_sequence<'mcx>(mcx: mcx::Mcx<'mcx>, seq_relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn sequence_change_persistence<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
        newrelpersistence: u8,
    ) -> PgResult<()>
);
