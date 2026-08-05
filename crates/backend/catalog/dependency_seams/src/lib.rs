use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::DropBehavior;

seam_core::seam!(
    pub fn perform_deletion(
        mcx: Mcx<'_>,
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
        behavior: DropBehavior,
        flags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    // RemoveOperatorById (operatorcmds.c), reached from doDeletion.
    pub fn remove_operator_by_id(mcx: Mcx<'_>, oper_oid: Oid) -> PgResult<()>
);

seam_core::seam!(
    // recordDependencyOnExpr (dependency.c), reached from ProcedureCreate for
    // prosqlbody without a pg_proc -> catalog_dependency edge.
    pub fn record_dependency_on_expr<'mcx>(
        mcx: Mcx<'mcx>,
        depender: &pg_depend::ObjectAddress,
        expr: types_nodes::Node<'mcx>,
        rtable: &types_nodes::list::NodeList<'mcx>,
        behavior: pg_depend::DependencyType,
    ) -> PgResult<()>
);
