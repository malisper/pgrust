//! `relation` family — `lsyscache.c` lookups keyed on `pg_class` /
//! `pg_index` (`RELOID` / `RELNAMENSP` / `INDEXRELID` syscaches).
//!
//! Each lookup mirrors the C `SearchSysCache1` + `GETSTRUCT` field read +
//! `ReleaseSysCache` exactly. The `SearchSysCache*` / `GetSysCacheOid2`
//! primitives belong to `utils/cache/syscache.c`, so they route through that
//! owner's per-owner seam (`backend-utils-cache-syscache-seams`); the seam
//! folds the matching `ReleaseSysCache` into its return and panics loudly
//! until `syscache` lands. The C "not found" return (`NULL` / `'\0'` /
//! `false` / `elog`) is applied here, as in the original.
//!
//! C entry points covered here: `get_rel_name`, `get_rel_relkind`,
//! `get_rel_relispartition`, `get_rel_namespace`, `get_relname_relid`,
//! `get_index_isclustered`.

use backend_utils_cache_syscache_seams as syscache;
use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};

/// `get_rel_name(relid)` (lsyscache.c).
///
/// ```c
/// char *
/// get_rel_name(Oid relid)
/// {
///     HeapTuple   tp;
///
///     tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
///     if (HeapTupleIsValid(tp))
///     {
///         Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
///         char       *result;
///
///         result = pstrdup(NameStr(reltup->relname));
///         ReleaseSysCache(tp);
///         return result;
///     }
///     else
///         return NULL;
/// }
/// ```
pub fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    // `SearchSysCache1(RELOID, ...)` + `pstrdup(NameStr(relname))` +
    // `ReleaseSysCache`; `HeapTupleIsValid` false is the C `return NULL`.
    syscache::rel_name::call(mcx, relid)
}

/// `get_rel_relkind(relid)` (lsyscache.c).
///
/// ```c
/// char
/// get_rel_relkind(Oid relid)
/// {
///     HeapTuple   tp;
///
///     tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
///     if (HeapTupleIsValid(tp))
///     {
///         Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
///         char        result;
///
///         result = reltup->relkind;
///         ReleaseSysCache(tp);
///         return result;
///     }
///     else
///         return '\0';
/// }
/// ```
pub fn get_rel_relkind(relid: Oid) -> PgResult<u8> {
    match syscache::rel_relkind::call(relid)? {
        Some(relkind) => Ok(relkind),
        None => Ok(b'\0'),
    }
}

/// `get_rel_relispartition(relid)` (lsyscache.c).
///
/// ```c
/// bool
/// get_rel_relispartition(Oid relid)
/// {
///     HeapTuple   tp;
///
///     tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
///     if (HeapTupleIsValid(tp))
///     {
///         Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
///         bool        result;
///
///         result = reltup->relispartition;
///         ReleaseSysCache(tp);
///         return result;
///     }
///     else
///         return false;
/// }
/// ```
pub fn get_rel_relispartition(relid: Oid) -> PgResult<bool> {
    match syscache::rel_relispartition::call(relid)? {
        Some(relispartition) => Ok(relispartition),
        None => Ok(false),
    }
}

/// `get_rel_namespace(relid)` (lsyscache.c).
///
/// ```c
/// Oid
/// get_rel_namespace(Oid relid)
/// {
///     HeapTuple   tp;
///
///     tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
///     if (HeapTupleIsValid(tp))
///     {
///         Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
///         Oid         result;
///
///         result = reltup->relnamespace;
///         ReleaseSysCache(tp);
///         return result;
///     }
///     else
///         return InvalidOid;
/// }
/// ```
pub fn get_rel_namespace(relid: Oid) -> PgResult<Oid> {
    match syscache::rel_namespace::call(relid)? {
        Some(relnamespace) => Ok(relnamespace),
        None => Ok(InvalidOid),
    }
}

/// `get_relname_relid(relname, relnamespace)` (lsyscache.c).
///
/// ```c
/// Oid
/// get_relname_relid(const char *relname, Oid relnamespace)
/// {
///     return GetSysCacheOid2(RELNAMENSP, Anum_pg_class_oid,
///                            PointerGetDatum(relname),
///                            ObjectIdGetDatum(relnamespace));
/// }
/// ```
pub fn get_relname_relid(relname: &str, relnamespace: Oid) -> PgResult<Oid> {
    syscache::relname_relid::call(relname, relnamespace)
}

/// `get_index_isclustered(index_oid)` (lsyscache.c).
///
/// ```c
/// bool
/// get_index_isclustered(Oid index_oid)
/// {
///     bool        isclustered;
///     HeapTuple   tuple;
///     Form_pg_index rd_index;
///
///     tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
///     if (!HeapTupleIsValid(tuple))
///         elog(ERROR, "cache lookup failed for index %u", index_oid);
///
///     rd_index = (Form_pg_index) GETSTRUCT(tuple);
///     isclustered = rd_index->indisclustered;
///     ReleaseSysCache(tuple);
///
///     return isclustered;
/// }
/// ```
pub fn get_index_isclustered(index_oid: Oid) -> PgResult<bool> {
    match syscache::index_isclustered::call(index_oid)? {
        Some(isclustered) => Ok(isclustered),
        // `!HeapTupleIsValid(tuple)` → `elog(ERROR, "cache lookup failed for
        // index %u", index_oid)`.
        None => Err(PgError::error(format!(
            "cache lookup failed for index {index_oid}"
        ))),
    }
}
