//! Seam declarations for the `common-relpath` unit (`src/common/relpath.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

extern crate alloc;

use alloc::string::String;

seam_core::seam!(
    /// `relpathbackend(rlocator, backend, forknum)` (relpath.h macro over
    /// `GetRelationPath`) — the on-disk path string of a relation fork.
    pub fn relpathbackend(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> String
);

seam_core::seam!(
    /// `GetDatabasePath(dbOid, spcOid)` (relpath.c): the filesystem path of a
    /// database's directory under a tablespace. C returns a `palloc`'d string;
    /// this value-shaped form returns an owned `String` for callers that have no
    /// ambient memory context (WAL redo / cache invalidation). The `Mcx`/
    /// `PgString` form lives in `backend-common-relpath-seams`.
    pub fn get_database_path(
        db_oid: types_core::primitive::Oid,
        spc_oid: types_core::primitive::Oid,
    ) -> String
);
