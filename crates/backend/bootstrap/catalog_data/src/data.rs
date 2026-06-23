//! GENERATED bootstrap catalog schema data — `Schema_pg_*[]` rows.
//!
//! Mirrors `src/include/catalog/schemapg.h` (genbki.pl output for
//! PostgreSQL 18.3), field-for-field. The `formrdesc` path in relcache
//! nails the bootstrap catalogs from these hardcoded
//! `FormData_pg_attribute` rows without any catalog access.
//!
//! DO NOT EDIT BY HAND — values copied verbatim from schemapg.h.

use ::types_core::primitive::Oid;
use ::relcache_entry::OwnedAttr;

/// `ATTNULLABLE_VALID` (`access/tupdesc.h`) — a valid not-null constraint.
const ATTNULLABLE_VALID: i8 = b'v' as i8;
/// `ATTNULLABLE_UNRESTRICTED` — no not-null constraint.
const ATTNULLABLE_UNRESTRICTED: i8 = b'f' as i8;

/// One `Schema_pg_*` attribute row as `(relid, OwnedAttr)` builder helper.
/// `relid` is the catalog relation OID (`FormData_pg_attribute.attrelid`),
/// identical for every row of a catalog; `formrdesc` reads it for `rd_id`.
fn attr(
    attname: &str,
    atttypid: Oid,
    attlen: i16,
    attnum: i16,
    atttypmod: i32,
    attbyval: bool,
    attalign: i8,
    attnotnull: bool,
    attidentity: i8,
    attgenerated: i8,
    attisdropped: bool,
    attcollation: Oid,
) -> OwnedAttr {
    OwnedAttr {
        attname: attname.to_string(),
        atttypid,
        attlen,
        attnum,
        atttypmod,
        attbyval,
        attalign,
        // For a nailed bootstrap catalog, `genbki` stamps each column's
        // attstorage from its type's typstorage: a fixed-length / cstring
        // column is PLAIN, a varlena column (`attlen == -1`) is EXTENDED. None
        // of these catalogs declares a non-default per-column compression.
        attstorage: if attlen == -1 { b'x' as i8 } else { b'p' as i8 },
        attcompression: b'\0' as i8,
        attnotnull,
        // Nailed bootstrap catalogs have no column defaults.
        atthasdef: false,
        // Nailed bootstrap catalogs have no fast-default missing values.
        atthasmissing: false,
        attndims: 0,
        attidentity,
        attgenerated,
        attisdropped,
        // `formrdesc` builds every nailed-catalog column as a locally-defined,
        // non-inherited attribute (C: `attislocal = true`, `attinhcount = 0`).
        attislocal: true,
        attinhcount: 0,
        attcollation,
        // `populate_compact_attribute` for a catalog relation:
        // VALID when not-null, else UNRESTRICTED (IsCatalogRelationOid
        // is always true for these nailed catalogs).
        attnullability: if attnotnull { ATTNULLABLE_VALID } else { ATTNULLABLE_UNRESTRICTED },
    }
}

/// Catalog OID (`attrelid`) of `pg_class`.
pub const PG_CLASS_RELID: Oid = 1259;
/// `Schema_pg_class[]` — 34 `FormData_pg_attribute` rows.
pub fn schema_pg_class() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("relnamespace", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reltype", 26, 4, 4, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reloftype", 26, 4, 5, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relowner", 26, 4, 6, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relam", 26, 4, 7, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relfilenode", 26, 4, 8, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reltablespace", 26, 4, 9, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relpages", 23, 4, 10, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reltuples", 700, 4, 11, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relallvisible", 23, 4, 12, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relallfrozen", 23, 4, 13, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reltoastrelid", 26, 4, 14, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relhasindex", 16, 1, 15, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relisshared", 16, 1, 16, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relpersistence", 18, 1, 17, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relkind", 18, 1, 18, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relnatts", 21, 2, 19, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relchecks", 21, 2, 20, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relhasrules", 16, 1, 21, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relhastriggers", 16, 1, 22, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relhassubclass", 16, 1, 23, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relrowsecurity", 16, 1, 24, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relforcerowsecurity", 16, 1, 25, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relispopulated", 16, 1, 26, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relreplident", 18, 1, 27, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relispartition", 16, 1, 28, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relrewrite", 26, 4, 29, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relfrozenxid", 28, 4, 30, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relminmxid", 28, 4, 31, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("relacl", 1034, -1, 32, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("reloptions", 1009, -1, 33, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("relpartbound", 194, -1, 34, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
    ]
}

/// Catalog OID (`attrelid`) of `pg_attribute`.
pub const PG_ATTRIBUTE_RELID: Oid = 1249;
/// `Schema_pg_attribute[]` — 25 `FormData_pg_attribute` rows.
pub fn schema_pg_attribute() -> Vec<OwnedAttr> {
    vec![
        attr("attrelid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("atttypid", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attlen", 21, 2, 4, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attnum", 21, 2, 5, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("atttypmod", 23, 4, 6, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attndims", 21, 2, 7, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attbyval", 16, 1, 8, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attalign", 18, 1, 9, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attstorage", 18, 1, 10, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attcompression", 18, 1, 11, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attnotnull", 16, 1, 12, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("atthasdef", 16, 1, 13, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("atthasmissing", 16, 1, 14, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attidentity", 18, 1, 15, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attgenerated", 18, 1, 16, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attisdropped", 16, 1, 17, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attislocal", 16, 1, 18, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attinhcount", 21, 2, 19, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attcollation", 26, 4, 20, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attstattarget", 21, 2, 21, -1, true, b's' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attacl", 1034, -1, 22, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("attoptions", 1009, -1, 23, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("attfdwoptions", 1009, -1, 24, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("attmissingval", 2277, -1, 25, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_proc`.
pub const PG_PROC_RELID: Oid = 1255;
/// `Schema_pg_proc[]` — 30 `FormData_pg_attribute` rows.
pub fn schema_pg_proc() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("pronamespace", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proowner", 26, 4, 4, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prolang", 26, 4, 5, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("procost", 700, 4, 6, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prorows", 700, 4, 7, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("provariadic", 26, 4, 8, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prosupport", 24, 4, 9, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prokind", 18, 1, 10, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prosecdef", 16, 1, 11, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proleakproof", 16, 1, 12, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proisstrict", 16, 1, 13, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proretset", 16, 1, 14, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("provolatile", 18, 1, 15, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proparallel", 18, 1, 16, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("pronargs", 21, 2, 17, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("pronargdefaults", 21, 2, 18, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prorettype", 26, 4, 19, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proargtypes", 30, -1, 20, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proallargtypes", 1028, -1, 21, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proargmodes", 1002, -1, 22, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("proargnames", 1009, -1, 23, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("proargdefaults", 194, -1, 24, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("protrftypes", 1028, -1, 25, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
        attr("prosrc", 25, -1, 26, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("probin", 25, -1, 27, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("prosqlbody", 194, -1, 28, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("proconfig", 1009, -1, 29, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("proacl", 1034, -1, 30, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_type`.
pub const PG_TYPE_RELID: Oid = 1247;
/// `Schema_pg_type[]` — 32 `FormData_pg_attribute` rows.
pub fn schema_pg_type() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("typnamespace", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typowner", 26, 4, 4, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typlen", 21, 2, 5, -1, true, b's' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typbyval", 16, 1, 6, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typtype", 18, 1, 7, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typcategory", 18, 1, 8, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typispreferred", 16, 1, 9, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typisdefined", 16, 1, 10, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typdelim", 18, 1, 11, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typrelid", 26, 4, 12, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typsubscript", 24, 4, 13, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typelem", 26, 4, 14, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typarray", 26, 4, 15, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typinput", 24, 4, 16, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typoutput", 24, 4, 17, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typreceive", 24, 4, 18, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typsend", 24, 4, 19, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typmodin", 24, 4, 20, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typmodout", 24, 4, 21, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typanalyze", 24, 4, 22, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typalign", 18, 1, 23, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typstorage", 18, 1, 24, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typnotnull", 16, 1, 25, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typbasetype", 26, 4, 26, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typtypmod", 23, 4, 27, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typndims", 23, 4, 28, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typcollation", 26, 4, 29, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("typdefaultbin", 194, -1, 30, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("typdefault", 25, -1, 31, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("typacl", 1034, -1, 32, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_database`.
pub const PG_DATABASE_RELID: Oid = 1262;
/// `Schema_pg_database[]` — 18 `FormData_pg_attribute` rows.
pub fn schema_pg_database() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("datdba", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("encoding", 23, 4, 4, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datlocprovider", 18, 1, 5, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datistemplate", 16, 1, 6, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datallowconn", 16, 1, 7, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("dathasloginevt", 16, 1, 8, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datconnlimit", 23, 4, 9, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datfrozenxid", 28, 4, 10, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datminmxid", 28, 4, 11, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("dattablespace", 26, 4, 12, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("datcollate", 25, -1, 13, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("datctype", 25, -1, 14, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("datlocale", 25, -1, 15, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("daticurules", 25, -1, 16, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("datcollversion", 25, -1, 17, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("datacl", 1034, -1, 18, -1, false, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_authid`.
pub const PG_AUTHID_RELID: Oid = 1260;
/// `Schema_pg_authid[]` — 12 `FormData_pg_attribute` rows.
pub fn schema_pg_authid() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolname", 19, 64, 2, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("rolsuper", 16, 1, 3, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolinherit", 16, 1, 4, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolcreaterole", 16, 1, 5, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolcreatedb", 16, 1, 6, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolcanlogin", 16, 1, 7, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolreplication", 16, 1, 8, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolbypassrls", 16, 1, 9, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolconnlimit", 23, 4, 10, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("rolpassword", 25, -1, 11, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("rolvaliduntil", 1184, 8, 12, -1, true, b'd' as i8, false, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_auth_members`.
pub const PG_AUTH_MEMBERS_RELID: Oid = 1261;
/// `Schema_pg_auth_members[]` — 7 `FormData_pg_attribute` rows.
pub fn schema_pg_auth_members() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("roleid", 26, 4, 2, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("member", 26, 4, 3, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("grantor", 26, 4, 4, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("admin_option", 16, 1, 5, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("inherit_option", 16, 1, 6, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("set_option", 16, 1, 7, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
    ]
}

/// Catalog OID (`attrelid`) of `pg_shseclabel`.
pub const PG_SHSECLABEL_RELID: Oid = 3592;
/// `Schema_pg_shseclabel[]` — 4 `FormData_pg_attribute` rows.
pub fn schema_pg_shseclabel() -> Vec<OwnedAttr> {
    vec![
        attr("objoid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("classoid", 26, 4, 2, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("provider", 25, -1, 3, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("label", 25, -1, 4, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
    ]
}

/// Catalog OID (`attrelid`) of `pg_subscription`.
pub const PG_SUBSCRIPTION_RELID: Oid = 6100;
/// `Schema_pg_subscription[]` — 18 `FormData_pg_attribute` rows.
pub fn schema_pg_subscription() -> Vec<OwnedAttr> {
    vec![
        attr("oid", 26, 4, 1, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subdbid", 26, 4, 2, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subskiplsn", 3220, 8, 3, -1, true, b'd' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subname", 19, 64, 4, -1, false, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("subowner", 26, 4, 5, -1, true, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subenabled", 16, 1, 6, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subbinary", 16, 1, 7, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("substream", 18, 1, 8, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subtwophasestate", 18, 1, 9, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subdisableonerr", 16, 1, 10, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subpasswordrequired", 16, 1, 11, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subrunasowner", 16, 1, 12, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subfailover", 16, 1, 13, -1, true, b'c' as i8, true, b'\0' as i8, b'\0' as i8, false, 0),
        attr("subconninfo", 25, -1, 14, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("subslotname", 19, 64, 15, -1, false, b'c' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
        attr("subsynccommit", 25, -1, 16, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("subpublications", 1009, -1, 17, -1, false, b'i' as i8, true, b'\0' as i8, b'\0' as i8, false, 950),
        attr("suborigin", 25, -1, 18, -1, false, b'i' as i8, false, b'\0' as i8, b'\0' as i8, false, 950),
    ]
}
