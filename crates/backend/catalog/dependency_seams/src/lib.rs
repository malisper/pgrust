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
