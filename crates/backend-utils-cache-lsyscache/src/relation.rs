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

/// `get_relnatts(relid)` (lsyscache.c): the number of attributes, or
/// `InvalidAttrNumber` (0) if absent. (PG marks this `#ifdef NOT_USED`; ported
/// for C-source completeness.)
pub fn get_relnatts(relid: Oid) -> PgResult<i32> {
    match syscache::pg_class_extra::call(relid)? {
        Some(extra) => Ok(extra.relnatts as i32),
        None => Ok(0),
    }
}

/// `get_rel_type_id(relid)` (lsyscache.c): the relation's composite type OID
/// (`reltype`), or `InvalidOid`.
pub fn get_rel_type_id(relid: Oid) -> PgResult<Oid> {
    match syscache::pg_class_extra::call(relid)? {
        Some(extra) => Ok(extra.reltype),
        None => Ok(InvalidOid),
    }
}

/// `get_rel_tablespace(relid)` (lsyscache.c): the relation's tablespace OID
/// (`reltablespace`), or `InvalidOid`.
pub fn get_rel_tablespace(relid: Oid) -> PgResult<Oid> {
    match syscache::pg_class_extra::call(relid)? {
        Some(extra) => Ok(extra.reltablespace),
        None => Ok(InvalidOid),
    }
}

/// `get_rel_persistence(relid)` (lsyscache.c): `relpersistence` (`p`/`u`/`t`);
/// a missing relation is `elog(ERROR, "cache lookup failed for relation %u")`.
pub fn get_rel_persistence(relid: Oid) -> PgResult<u8> {
    match syscache::pg_class_extra::call(relid)? {
        Some(extra) => Ok(extra.relpersistence),
        None => Err(PgError::error(format!(
            "cache lookup failed for relation {relid}"
        ))),
    }
}

/// `get_rel_relam(relid)` (lsyscache.c): the relation's access method OID
/// (`relam`); a missing relation is `elog(ERROR)`.
pub fn get_rel_relam(relid: Oid) -> PgResult<Oid> {
    match syscache::pg_class_extra::call(relid)? {
        Some(extra) => Ok(extra.relam),
        None => Err(PgError::error(format!(
            "cache lookup failed for relation {relid}"
        ))),
    }
}

/// `get_index_isreplident(index_oid)` (lsyscache.c): `indisreplident`, or
/// `false` if absent.
pub fn get_index_isreplident(index_oid: Oid) -> PgResult<bool> {
    match syscache::pg_index_flags::call(index_oid)? {
        Some(flags) => Ok(flags.indisreplident),
        None => Ok(false),
    }
}

/// `get_index_isvalid(index_oid)` (lsyscache.c): `indisvalid`; a missing index
/// is `elog(ERROR, "cache lookup failed for index %u")`.
pub fn get_index_isvalid(index_oid: Oid) -> PgResult<bool> {
    match syscache::pg_index_flags::call(index_oid)? {
        Some(flags) => Ok(flags.indisvalid),
        None => Err(PgError::error(format!(
            "cache lookup failed for index {index_oid}"
        ))),
    }
}

/// `get_index_column_opclass(index_oid, attno)` (lsyscache.c): the opclass of
/// the index's `attno`th column (1-based), or `InvalidOid` if the index was not
/// found or `attno` is a non-key (INCLUDE) column.
///
/// ```c
/// tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
/// if (!HeapTupleIsValid(tuple)) return InvalidOid;
/// rd_index = (Form_pg_index) GETSTRUCT(tuple);
/// Assert(attno > 0 && attno <= rd_index->indnatts);
/// if (attno > rd_index->indnkeyatts) { ReleaseSysCache(tuple); return InvalidOid; }
/// datum = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indclass);
/// indclass = (oidvector *) DatumGetPointer(datum);
/// Assert(attno <= indclass->dim1);
/// opclass = indclass->values[attno - 1];
/// ReleaseSysCache(tuple);
/// return opclass;
/// ```
pub fn get_index_column_opclass(index_oid: Oid, attno: i32) -> PgResult<Oid> {
    // The seam folds `SearchSysCache1(INDEXRELID)` + the `indnatts`/`indnkeyatts`
    // reads + `SysCacheGetAttrNotNull(Anum_pg_index_indclass)`, returning the
    // per-column opclass `oidvector` copied into a scratch context.
    let scratch = mcx::MemoryContext::new("get_index_column_opclass");
    let (indnatts, indnkeyatts, indclass) =
        match syscache::pg_index_indclass::call(scratch.mcx(), index_oid)? {
            Some(t) => t,
            // `!HeapTupleIsValid(tuple)` → `return InvalidOid`.
            None => return Ok(InvalidOid),
        };

    // caller is supposed to guarantee this
    debug_assert!(attno > 0 && attno <= indnatts as i32);

    // Non-key attributes don't have an opclass
    if attno > indnkeyatts as i32 {
        return Ok(InvalidOid);
    }

    debug_assert!(attno <= indclass.len() as i32);
    Ok(indclass[(attno - 1) as usize])
}
