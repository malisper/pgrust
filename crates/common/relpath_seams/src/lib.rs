use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_database_path<'mcx>(
        mcx: Mcx<'mcx>,
        db_oid: Oid,
        spc_oid: Oid,
    ) -> PgResult<PgString<'mcx>>
);
