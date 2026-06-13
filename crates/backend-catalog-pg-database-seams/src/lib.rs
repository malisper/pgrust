//! Seam declarations for reading `pg_database` rows during backend startup.
//!
//! postinit.c's `GetDatabaseTuple`/`GetDatabaseTupleByOid` open `pg_database`
//! and scan it (`table_open` + `systable_beginscan` + `systable_getnext` +
//! `heap_copytuple` + `systable_endscan` + `table_close`); `CheckMyDatabase`
//! re-reads the same row via `SearchSysCache1(DATABASEOID, ...)`. Decoding the
//! variable-length locale columns (`datcollate`/`datctype`/`datlocale`/
//! `datcollversion`) requires the catalog read + fmgr/varlena layer the
//! consuming crate does not own, so each read crosses as one batched call that
//! returns a decoded [`FormPgDatabase`] (the `heap_copytuple` analog: an owned
//! copy in `mcx`). `None` is the C invalid-tuple / cache-miss result.
//!
//! The owning catalog-read unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

use mcx::Mcx;
use types_catalog::pg_database::FormPgDatabase;
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `GetDatabaseTuple(dbname)` (postinit.c): scan `pg_database` by name
    /// (`Anum_pg_database_datname` = `dbname`, index `DatabaseNameIndexId`
    /// when the critical shared relcache is built, else seqscan). Returns the
    /// decoded row, or `None` if no such database. `Err` carries the
    /// scan/catalog-open `ereport(ERROR)` surface plus OOM from the copy.
    pub fn get_database_tuple_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        dbname: &str,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);

seam_core::seam!(
    /// `GetDatabaseTupleByOid(dboid)` (postinit.c): as above, scanning by OID
    /// (`Anum_pg_database_oid` = `dboid`, index `DatabaseOidIndexId`).
    pub fn get_database_tuple_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid))` +
    /// `GETSTRUCT`/`SysCacheGetAttr*` decode (postinit.c `CheckMyDatabase`):
    /// read our own `pg_database` row through the syscache. Returns the
    /// decoded row, or `None` on a cache miss. `Err` carries the syscache
    /// lookup's `ereport(ERROR)` surface plus OOM.
    pub fn search_database_syscache<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<FormPgDatabase<'mcx>>>
);
