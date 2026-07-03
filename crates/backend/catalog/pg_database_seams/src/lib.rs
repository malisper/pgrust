use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

// Owner: the pg_database catalog-read unit (Form_pg_database decode over the
// genam/heapam scan and the DATABASEOID syscache probe). Field set is what
// postinit.c reads; lean single-projection getters can join later (rule 6).

pub const COLLPROVIDER_BUILTIN: u8 = b'b';
pub const COLLPROVIDER_ICU: u8 = b'i';
pub const COLLPROVIDER_LIBC: u8 = b'c';

/// Decoded pg_database row (pg_database.h fields postinit consumes).
pub struct PgDatabaseForm<'mcx> {
    pub oid: Oid,
    pub datname: PgString<'mcx>,
    pub datdba: Oid,
    pub datistemplate: bool,
    pub dattablespace: Oid,
    pub datallowconn: bool,
    pub dathasloginevt: bool,
    pub datconnlimit: i32,
    pub datfrozenxid: u32,
    pub datminmxid: u32,
    pub encoding: i32,
    pub datlocprovider: u8,
    pub datcollate: PgString<'mcx>,
    pub datctype: PgString<'mcx>,
    pub datlocale: Option<PgString<'mcx>>,
    pub daticurules: Option<PgString<'mcx>>,
    pub datcollversion: Option<PgString<'mcx>>,
}

seam_core::seam!(
    // GetDatabaseTuple(dbname) (postinit.c body): pg_database scan by datname
    // via DatabaseNameIndexId (2671) iff criticalSharedRelcachesBuilt, else
    // forced heap scan; heap_copytuple decode into mcx.
    pub fn get_database_tuple_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        dbname: &str,
    ) -> PgResult<Option<PgDatabaseForm<'mcx>>>
);

seam_core::seam!(
    // GetDatabaseTupleByOid(dboid): as above via DatabaseOidIndexId (2672).
    pub fn get_database_tuple_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<PgDatabaseForm<'mcx>>>
);

seam_core::seam!(
    // SearchSysCache1(DATABASEOID, dboid) + the SysCacheGetAttr reads of
    // CheckMyDatabase, decoded once (rule 7).
    pub fn search_database_syscache<'mcx>(
        mcx: Mcx<'mcx>,
        dboid: Oid,
    ) -> PgResult<Option<PgDatabaseForm<'mcx>>>
);
