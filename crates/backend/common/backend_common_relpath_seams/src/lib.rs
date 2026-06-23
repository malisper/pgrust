//! Seam declarations for `common/relpath.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::mcx::{Mcx, PgString};
use ::types_core::primitive::{ForkNumber, Oid, ProcNumber, RelFileNumber};
use ::types_error::PgResult;

seam_core::seam!(
    /// `GetDatabasePath(dbOid, spcOid)` (relpath.c): build the filesystem path
    /// of a database's directory under a tablespace. Allocates the result in
    /// the caller's context, so the path crosses as a `PgString` in `mcx`;
    /// `Err` carries OOM.
    pub fn get_database_path<'mcx>(
        mcx: Mcx<'mcx>,
        db_oid: Oid,
        spc_oid: Oid,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `GetRelationPath(dbOid, spcOid, relNumber, backend, forknum)` (relpath.c)
    /// — the `relpathbackend(rlocator, backend, forknum)` macro expansion: the
    /// filesystem path of one fork of a relation's file. Allocates the result
    /// in `mcx`; `Err` carries OOM.
    pub fn relpath_backend<'mcx>(
        mcx: Mcx<'mcx>,
        db_oid: Oid,
        spc_oid: Oid,
        rel_number: RelFileNumber,
        backend: ProcNumber,
        forknum: ForkNumber,
    ) -> PgResult<PgString<'mcx>>
);
