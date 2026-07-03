use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::DropBehavior;

seam_core::seam!(
    // performDeletion (dependency.c) marshalled as the flat ObjectAddress
    // triple; flags are the PERFORM_DELETION_* bits.
    pub fn perform_deletion(
        mcx: Mcx<'_>,
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
        behavior: DropBehavior,
        flags: i32,
    ) -> PgResult<()>
);
