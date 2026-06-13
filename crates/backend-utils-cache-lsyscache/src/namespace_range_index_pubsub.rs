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
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};

/// `OidIsValid(oid)` (`c.h`).
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

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

// ---- PG_RANGE caches ------------------------------------------------------

/// `get_range_subtype(rangeOid)` (lsyscache.c): the range's subtype, or
/// `InvalidOid` if not a range type.
pub fn get_range_subtype(range_oid: Oid) -> PgResult<Oid> {
    match syscache_seams::pg_range_fields::call(range_oid)? {
        Some(rng) => Ok(rng.rngsubtype),
        None => Ok(InvalidOid),
    }
}

/// `get_range_collation(rangeOid)` (lsyscache.c): the range's collation, or
/// `InvalidOid`.
pub fn get_range_collation(range_oid: Oid) -> PgResult<Oid> {
    match syscache_seams::pg_range_fields::call(range_oid)? {
        Some(rng) => Ok(rng.rngcollation),
        None => Ok(InvalidOid),
    }
}

/// `get_range_multirange(rangeOid)` (lsyscache.c): the range's multirange type
/// (`rngmultitypid`), or `InvalidOid`.
pub fn get_range_multirange(range_oid: Oid) -> PgResult<Oid> {
    match syscache_seams::pg_range_fields::call(range_oid)? {
        Some(rng) => Ok(rng.rngmultitypid),
        None => Ok(InvalidOid),
    }
}

// ---- publication / subscription -------------------------------------------

/// `get_publication_oid(pubname, missing_ok)` (lsyscache.c).
///
/// ```c
/// oid = GetSysCacheOid1(PUBLICATIONNAME, Anum_pg_publication_oid, CStringGetDatum(pubname));
/// if (!OidIsValid(oid) && !missing_ok)
///     ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
///             errmsg("publication \"%s\" does not exist", pubname));
/// return oid;
/// ```
pub fn get_publication_oid(pubname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::get_publication_oid_syscache::call(pubname)?;
    if !oid_is_valid(oid) && !missing_ok {
        return Err(PgError::error(format!(
            "publication \"{pubname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `get_publication_name(pubid, missing_ok)` (lsyscache.c): the publication's
/// name copied into `mcx`; with `missing_ok = false` a miss is `elog(ERROR,
/// "cache lookup failed for publication %u")`, else `Ok(None)`.
pub fn get_publication_name<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::get_publication_name_syscache::call(mcx, pubid)? {
        Some(name) => Ok(Some(name)),
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for publication {pubid}"
                )));
            }
            Ok(None)
        }
    }
}

/// `get_subscription_oid(subname, missing_ok)` (lsyscache.c).
///
/// ```c
/// oid = GetSysCacheOid2(SUBSCRIPTIONNAME, Anum_pg_subscription_oid,
///                       MyDatabaseId, CStringGetDatum(subname));
/// if (!OidIsValid(oid) && !missing_ok)
///     ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
///             errmsg("subscription \"%s\" does not exist", subname));
/// return oid;
/// ```
///
/// The `MyDatabaseId` key is the syscache owner's per-backend global, supplied
/// inside the seam installer.
pub fn get_subscription_oid(subname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::get_subscription_oid_syscache::call(subname)?;
    if !oid_is_valid(oid) && !missing_ok {
        return Err(PgError::error(format!(
            "subscription \"{subname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `get_subscription_name(subid, missing_ok)` (lsyscache.c): the subscription's
/// name copied into `mcx`; with `missing_ok = false` a miss is `elog(ERROR,
/// "cache lookup failed for subscription %u")`, else `Ok(None)`.
pub fn get_subscription_name<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::get_subscription_name_syscache::call(mcx, subid)? {
        Some(name) => Ok(Some(name)),
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for subscription {subid}"
                )));
            }
            Ok(None)
        }
    }
}
