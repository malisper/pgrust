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

seam_core::seam!(
    // GetRelationPath(dbOid, spcOid, relNumber, backend, forknum) via the
    // relpathbackend() macro (relpath.h); md.c's segment-open paths.
    pub fn relpathbackend(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::ProcNumber,
        forknum: types_core::ForkNumber,
    ) -> String
);
