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

seam_core::seam!(
    // relpathperm(rlocator, forknum) (relpath.h): C returns a by-value stack
    // RelPathStr that only ever feeds *printf; owned String is that rendering
    // on the cold log/error paths this serves.
    pub fn relpathperm(
        rlocator: types_storage::RelFileLocator,
        forknum: types_core::ForkNumber,
    ) -> String
);
