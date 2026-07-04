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
