// GENERATED from PG 18.3 catalog headers (schemapg.h + pg_*_d.h,
// CATALOG_VERSION_NO 202506291) by scripts/gen_relcache_schemapg.py. Do not edit.
#![allow(clippy::too_many_arguments)]

use ::types_core::Oid;
use ::types_tuple::{FormData_pg_attribute, NameData};

const fn a(
    attrelid: Oid, name: &[u8], atttypid: Oid, attlen: i16, attnum: i16,
    atttypmod: i32, attndims: i16, attbyval: bool, attalign: u8, attstorage: u8,
    attcompression: u8, attnotnull: bool, atthasdef: bool, atthasmissing: bool,
    attidentity: u8, attgenerated: u8, attisdropped: bool, attislocal: bool,
    attinhcount: i16, attcollation: Oid,
) -> FormData_pg_attribute {
    let mut data = [0u8; 64];
    let mut i = 0;
    while i < name.len() {
        data[i] = name[i];
        i += 1;
    }
    FormData_pg_attribute {
        attrelid,
        attname: NameData { data },
        atttypid,
        attlen,
        attnum,
        atttypmod,
        attndims,
        attbyval,
        attalign: attalign as i8,
        attstorage: attstorage as i8,
        attcompression: attcompression as i8,
        attnotnull,
        atthasdef,
        atthasmissing,
        attidentity: attidentity as i8,
        attgenerated: attgenerated as i8,
        attisdropped,
        attislocal,
        attinhcount,
        attcollation,
    }
}

pub struct BootstrapCatalog {
    pub relid: Oid,
    pub rowtype_id: Oid,
    pub name: &'static str,
    pub shared: bool,
    pub attrs: &'static [FormData_pg_attribute],
}

pub static DESC_PG_CLASS: [FormData_pg_attribute; 34] = [
    a(1259, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1259, b"relnamespace", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reltype", 26, 4, 4, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reloftype", 26, 4, 5, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relowner", 26, 4, 6, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relam", 26, 4, 7, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relfilenode", 26, 4, 8, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reltablespace", 26, 4, 9, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relpages", 23, 4, 10, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reltuples", 700, 4, 11, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relallvisible", 23, 4, 12, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relallfrozen", 23, 4, 13, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reltoastrelid", 26, 4, 14, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relhasindex", 16, 1, 15, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relisshared", 16, 1, 16, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relpersistence", 18, 1, 17, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relkind", 18, 1, 18, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relnatts", 21, 2, 19, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relchecks", 21, 2, 20, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relhasrules", 16, 1, 21, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relhastriggers", 16, 1, 22, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relhassubclass", 16, 1, 23, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relrowsecurity", 16, 1, 24, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relforcerowsecurity", 16, 1, 25, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relispopulated", 16, 1, 26, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relreplident", 18, 1, 27, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relispartition", 16, 1, 28, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relrewrite", 26, 4, 29, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relfrozenxid", 28, 4, 30, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relminmxid", 28, 4, 31, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"relacl", 1034, -1, 32, -1, 1, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1259, b"reloptions", 1009, -1, 33, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1259, b"relpartbound", 194, -1, 34, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
];

pub static CAT_PG_CLASS: BootstrapCatalog = BootstrapCatalog {
    relid: 1259,
    rowtype_id: 83,
    name: "pg_class",
    shared: false,
    attrs: &DESC_PG_CLASS,
};

pub static DESC_PG_ATTRIBUTE: [FormData_pg_attribute; 25] = [
    a(1249, b"attrelid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1249, b"atttypid", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attlen", 21, 2, 4, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attnum", 21, 2, 5, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"atttypmod", 23, 4, 6, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attndims", 21, 2, 7, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attbyval", 16, 1, 8, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attalign", 18, 1, 9, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attstorage", 18, 1, 10, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attcompression", 18, 1, 11, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attnotnull", 16, 1, 12, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"atthasdef", 16, 1, 13, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"atthasmissing", 16, 1, 14, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attidentity", 18, 1, 15, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attgenerated", 18, 1, 16, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attisdropped", 16, 1, 17, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attislocal", 16, 1, 18, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attinhcount", 21, 2, 19, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attcollation", 26, 4, 20, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attstattarget", 21, 2, 21, -1, 0, true, b's', b'p', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attacl", 1034, -1, 22, -1, 1, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1249, b"attoptions", 1009, -1, 23, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1249, b"attfdwoptions", 1009, -1, 24, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1249, b"attmissingval", 2277, -1, 25, -1, 0, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_ATTRIBUTE: BootstrapCatalog = BootstrapCatalog {
    relid: 1249,
    rowtype_id: 75,
    name: "pg_attribute",
    shared: false,
    attrs: &DESC_PG_ATTRIBUTE,
};

pub static DESC_PG_PROC: [FormData_pg_attribute; 30] = [
    a(1255, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"pronamespace", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proowner", 26, 4, 4, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prolang", 26, 4, 5, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"procost", 700, 4, 6, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prorows", 700, 4, 7, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"provariadic", 26, 4, 8, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prosupport", 24, 4, 9, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prokind", 18, 1, 10, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prosecdef", 16, 1, 11, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proleakproof", 16, 1, 12, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proisstrict", 16, 1, 13, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proretset", 16, 1, 14, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"provolatile", 18, 1, 15, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proparallel", 18, 1, 16, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"pronargs", 21, 2, 17, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"pronargdefaults", 21, 2, 18, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prorettype", 26, 4, 19, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proargtypes", 30, -1, 20, -1, 1, false, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proallargtypes", 1028, -1, 21, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proargmodes", 1002, -1, 22, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"proargnames", 1009, -1, 23, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"proargdefaults", 194, -1, 24, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"protrftypes", 1028, -1, 25, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
    a(1255, b"prosrc", 25, -1, 26, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"probin", 25, -1, 27, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"prosqlbody", 194, -1, 28, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"proconfig", 1009, -1, 29, -1, 1, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1255, b"proacl", 1034, -1, 30, -1, 1, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_PROC: BootstrapCatalog = BootstrapCatalog {
    relid: 1255,
    rowtype_id: 81,
    name: "pg_proc",
    shared: false,
    attrs: &DESC_PG_PROC,
};

pub static DESC_PG_TYPE: [FormData_pg_attribute; 32] = [
    a(1247, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1247, b"typnamespace", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typowner", 26, 4, 4, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typlen", 21, 2, 5, -1, 0, true, b's', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typbyval", 16, 1, 6, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typtype", 18, 1, 7, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typcategory", 18, 1, 8, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typispreferred", 16, 1, 9, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typisdefined", 16, 1, 10, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typdelim", 18, 1, 11, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typrelid", 26, 4, 12, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typsubscript", 24, 4, 13, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typelem", 26, 4, 14, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typarray", 26, 4, 15, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typinput", 24, 4, 16, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typoutput", 24, 4, 17, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typreceive", 24, 4, 18, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typsend", 24, 4, 19, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typmodin", 24, 4, 20, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typmodout", 24, 4, 21, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typanalyze", 24, 4, 22, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typalign", 18, 1, 23, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typstorage", 18, 1, 24, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typnotnull", 16, 1, 25, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typbasetype", 26, 4, 26, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typtypmod", 23, 4, 27, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typndims", 23, 4, 28, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typcollation", 26, 4, 29, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1247, b"typdefaultbin", 194, -1, 30, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1247, b"typdefault", 25, -1, 31, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1247, b"typacl", 1034, -1, 32, -1, 1, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_TYPE: BootstrapCatalog = BootstrapCatalog {
    relid: 1247,
    rowtype_id: 71,
    name: "pg_type",
    shared: false,
    attrs: &DESC_PG_TYPE,
};

pub static DESC_PG_DATABASE: [FormData_pg_attribute; 18] = [
    a(1262, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"datdba", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"encoding", 23, 4, 4, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datlocprovider", 18, 1, 5, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datistemplate", 16, 1, 6, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datallowconn", 16, 1, 7, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"dathasloginevt", 16, 1, 8, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datconnlimit", 23, 4, 9, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datfrozenxid", 28, 4, 10, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datminmxid", 28, 4, 11, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"dattablespace", 26, 4, 12, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1262, b"datcollate", 25, -1, 13, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"datctype", 25, -1, 14, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"datlocale", 25, -1, 15, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"daticurules", 25, -1, 16, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"datcollversion", 25, -1, 17, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1262, b"datacl", 1034, -1, 18, -1, 1, false, b'd', b'x', 0, false, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_DATABASE: BootstrapCatalog = BootstrapCatalog {
    relid: 1262,
    rowtype_id: 1248,
    name: "pg_database",
    shared: true,
    attrs: &DESC_PG_DATABASE,
};

pub static DESC_PG_AUTHID: [FormData_pg_attribute; 12] = [
    a(1260, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolname", 19, 64, 2, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(1260, b"rolsuper", 16, 1, 3, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolinherit", 16, 1, 4, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolcreaterole", 16, 1, 5, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolcreatedb", 16, 1, 6, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolcanlogin", 16, 1, 7, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolreplication", 16, 1, 8, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolbypassrls", 16, 1, 9, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolconnlimit", 23, 4, 10, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1260, b"rolpassword", 25, -1, 11, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(1260, b"rolvaliduntil", 1184, 8, 12, -1, 0, true, b'd', b'p', 0, false, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_AUTHID: BootstrapCatalog = BootstrapCatalog {
    relid: 1260,
    rowtype_id: 2842,
    name: "pg_authid",
    shared: true,
    attrs: &DESC_PG_AUTHID,
};

pub static DESC_PG_AUTH_MEMBERS: [FormData_pg_attribute; 7] = [
    a(1261, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"roleid", 26, 4, 2, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"member", 26, 4, 3, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"grantor", 26, 4, 4, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"admin_option", 16, 1, 5, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"inherit_option", 16, 1, 6, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(1261, b"set_option", 16, 1, 7, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
];

pub static CAT_PG_AUTH_MEMBERS: BootstrapCatalog = BootstrapCatalog {
    relid: 1261,
    rowtype_id: 2843,
    name: "pg_auth_members",
    shared: true,
    attrs: &DESC_PG_AUTH_MEMBERS,
};

pub static DESC_PG_SHSECLABEL: [FormData_pg_attribute; 4] = [
    a(3592, b"objoid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(3592, b"classoid", 26, 4, 2, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(3592, b"provider", 25, -1, 3, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(3592, b"label", 25, -1, 4, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
];

pub static CAT_PG_SHSECLABEL: BootstrapCatalog = BootstrapCatalog {
    relid: 3592,
    rowtype_id: 4066,
    name: "pg_shseclabel",
    shared: true,
    attrs: &DESC_PG_SHSECLABEL,
};

pub static DESC_PG_SUBSCRIPTION: [FormData_pg_attribute; 18] = [
    a(6100, b"oid", 26, 4, 1, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subdbid", 26, 4, 2, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subskiplsn", 3220, 8, 3, -1, 0, true, b'd', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subname", 19, 64, 4, -1, 0, false, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(6100, b"subowner", 26, 4, 5, -1, 0, true, b'i', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subenabled", 16, 1, 6, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subbinary", 16, 1, 7, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"substream", 18, 1, 8, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subtwophasestate", 18, 1, 9, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subdisableonerr", 16, 1, 10, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subpasswordrequired", 16, 1, 11, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subrunasowner", 16, 1, 12, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subfailover", 16, 1, 13, -1, 0, true, b'c', b'p', 0, true, false, false, 0, 0, false, true, 0, 0),
    a(6100, b"subconninfo", 25, -1, 14, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(6100, b"subslotname", 19, 64, 15, -1, 0, false, b'c', b'p', 0, false, false, false, 0, 0, false, true, 0, 950),
    a(6100, b"subsynccommit", 25, -1, 16, -1, 0, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(6100, b"subpublications", 1009, -1, 17, -1, 1, false, b'i', b'x', 0, true, false, false, 0, 0, false, true, 0, 950),
    a(6100, b"suborigin", 25, -1, 18, -1, 0, false, b'i', b'x', 0, false, false, false, 0, 0, false, true, 0, 950),
];

pub static CAT_PG_SUBSCRIPTION: BootstrapCatalog = BootstrapCatalog {
    relid: 6100,
    rowtype_id: 6101,
    name: "pg_subscription",
    shared: true,
    attrs: &DESC_PG_SUBSCRIPTION,
};

// RelationCacheInitializePhase2 list (NUM_CRITICAL_SHARED_RELS).
pub static SHARED_BOOTSTRAP_CATALOGS: [&BootstrapCatalog; 5] = [
    &CAT_PG_DATABASE,
    &CAT_PG_AUTHID,
    &CAT_PG_AUTH_MEMBERS,
    &CAT_PG_SHSECLABEL,
    &CAT_PG_SUBSCRIPTION,
];

// RelationCacheInitializePhase3 list (NUM_CRITICAL_LOCAL_RELS).
pub static LOCAL_BOOTSTRAP_CATALOGS: [&BootstrapCatalog; 4] = [
    &CAT_PG_CLASS,
    &CAT_PG_ATTRIBUTE,
    &CAT_PG_PROC,
    &CAT_PG_TYPE,
];

pub const CLASS_OID_INDEX_ID: Oid = 2662;
pub const ATTRIBUTE_RELID_NUM_INDEX_ID: Oid = 2659;
pub const INDEX_RELID_INDEX_ID: Oid = 2679;
pub const INDEX_RELATION_ID: Oid = 2610;
pub const OPCLASS_OID_INDEX_ID: Oid = 2687;
pub const OPERATOR_CLASS_RELATION_ID: Oid = 2616;
pub const ACCESS_METHOD_PROCEDURE_INDEX_ID: Oid = 2655;
pub const ACCESS_METHOD_PROCEDURE_RELATION_ID: Oid = 2603;
pub const REWRITE_REL_RULENAME_INDEX_ID: Oid = 2693;
pub const REWRITE_RELATION_ID: Oid = 2618;
pub const TRIGGER_RELID_NAME_INDEX_ID: Oid = 2701;
pub const TRIGGER_RELATION_ID: Oid = 2620;
pub const DATABASE_NAME_INDEX_ID: Oid = 2671;
pub const DATABASE_OID_INDEX_ID: Oid = 2672;
pub const AUTH_ID_ROLNAME_INDEX_ID: Oid = 2676;
pub const AUTH_ID_OID_INDEX_ID: Oid = 2677;
pub const AUTH_MEM_MEM_ROLE_INDEX_ID: Oid = 2695;
pub const SHARED_SEC_LABEL_OBJECT_INDEX_ID: Oid = 3593;
