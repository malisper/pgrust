//! `namespace-range-index-pubsub` family — `lsyscache.c` lookups keyed on
//! `pg_namespace` / `pg_am` and the range / index / publication-subscription
//! helpers.
//!
//! C entry points covered here: `get_namespace_name`,
//! `get_namespace_name_or_temp`, `get_am_name`. The remaining range / index /
//! pubsub helpers in this C section (`get_range_subtype`,
//! `get_range_collation`, `get_index_column_opclass`,
//! `get_publication_oid` / `get_subscription_oid`, ...) have no seam
//! declaration yet and will land here with their own decls.
//!
//! The `SearchSysCache1(NAMESPACEOID/AMOID)` + `GETSTRUCT` + `pstrdup(NameStr)`
//! projection is owned by `backend-utils-cache-syscache`; this family routes
//! through its `search_namespace_name` / `search_am_name` seams (the copy out
//! of the catcache into the caller's `mcx` subsumes the C `pstrdup` +
//! `ReleaseSysCache`). `isTempNamespace` is owned by `backend-catalog-namespace`
//! and reached through its `is_temp_namespace` seam.

use backend_catalog_namespace_seams as namespace_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

/// `get_namespace_name(nspid)` (lsyscache.c).
///
/// C:
/// ```text
/// tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
/// if (HeapTupleIsValid(tp)) {
///     Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
///     char *result = pstrdup(NameStr(nsptup->nspname));
///     ReleaseSysCache(tp);
///     return result;
/// } else
///     return NULL;
/// ```
pub fn get_namespace_name<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    // The syscache projection seam performs the GETSTRUCT + pstrdup(NameStr)
    // into `mcx` and the ReleaseSysCache; a cache miss returns `Ok(None)` (the
    // C `return NULL`).
    syscache_seams::search_namespace_name::call(mcx, nspid)
}

/// `get_namespace_name_or_temp(nspid)` (lsyscache.c).
///
/// C:
/// ```text
/// if (isTempNamespace(nspid))
///     return pstrdup("pg_temp");
/// else
///     return get_namespace_name(nspid);
/// ```
pub fn get_namespace_name_or_temp<'mcx>(
    mcx: Mcx<'mcx>,
    nspid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    if namespace_seams::is_temp_namespace::call(nspid)? {
        Ok(Some(PgString::from_str_in("pg_temp", mcx)?))
    } else {
        get_namespace_name(mcx, nspid)
    }
}

/// `get_am_name(amOid)` (lsyscache.c).
///
/// C:
/// ```text
/// char *result = NULL;
/// tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
/// if (HeapTupleIsValid(tup)) {
///     Form_pg_am amform = (Form_pg_am) GETSTRUCT(tup);
///     result = pstrdup(NameStr(amform->amname));
///     ReleaseSysCache(tup);
/// }
/// return result;
/// ```
pub fn get_am_name<'mcx>(mcx: Mcx<'mcx>, am_oid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    // The syscache projection seam performs the GETSTRUCT + pstrdup(NameStr)
    // into `mcx` and the ReleaseSysCache; a cache miss returns `Ok(None)` (the
    // C NULL initializer left untouched).
    syscache_seams::search_am_name::call(mcx, am_oid)
}
