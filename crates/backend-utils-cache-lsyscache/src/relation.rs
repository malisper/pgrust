//! `relation` family — `lsyscache.c` lookups keyed on `pg_class` /
//! `pg_index` (`RELOID` / `RELNAMENSP` / `INDEXRELID` syscaches).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here: `get_rel_name`, `get_rel_relkind`,
//! `get_rel_relispartition`, `get_rel_namespace`, `get_relname_relid`,
//! `get_index_isclustered`.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

/// `get_rel_name(relid)` (lsyscache.c).
pub fn get_rel_name<'mcx>(_mcx: Mcx<'mcx>, _relid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_rel_name: SearchSysCache(RELOID) -> pstrdup(relname)")
}

/// `get_rel_relkind(relid)` (lsyscache.c).
pub fn get_rel_relkind(_relid: Oid) -> PgResult<u8> {
    todo!("get_rel_relkind: SearchSysCache(RELOID) -> relkind, else '\\0'")
}

/// `get_rel_relispartition(relid)` (lsyscache.c).
pub fn get_rel_relispartition(_relid: Oid) -> PgResult<bool> {
    todo!("get_rel_relispartition: SearchSysCache(RELOID) -> relispartition, else false")
}

/// `get_rel_namespace(relid)` (lsyscache.c).
pub fn get_rel_namespace(_relid: Oid) -> PgResult<Oid> {
    todo!("get_rel_namespace: SearchSysCache(RELOID) -> relnamespace")
}

/// `get_relname_relid(relname, relnamespace)` (lsyscache.c).
pub fn get_relname_relid(_relname: &str, _relnamespace: Oid) -> PgResult<Oid> {
    todo!("get_relname_relid: GetSysCacheOid2(RELNAMENSP) -> oid")
}

/// `get_index_isclustered(index_oid)` (lsyscache.c).
pub fn get_index_isclustered(_index_oid: Oid) -> PgResult<bool> {
    todo!("get_index_isclustered: SearchSysCache(INDEXRELID) -> indisclustered")
}
