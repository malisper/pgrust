// heap.c, SysAtt slice only (attnums/types vs sysattr.h + pg_type.dat);
// relation DDL (heap_create...) stays with the rest of the unit.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod create;
pub mod drop;
pub mod partition;
pub mod truncate;
pub use truncate::{heap_truncate, heap_truncate_check_FKs, heap_truncate_find_FKs, heap_truncate_one_rel};
pub use create::{heap_create, heap_create_with_catalog, CheckAttributeNamesTypes, CheckAttributeType, FormExtraData_pg_attribute, InsertPgAttributeTuples, HeapCreateParams, InsertPgClassTuple, take_next_heap_pg_class_relfilenumber, NextToastPgClassOidIsSet, RelationClearMissing, SetAttrMissing, SetNextHeapPgClassOid, SetNextHeapPgClassRelfilenumber, SetNextToastPgClassOid, SetNextToastPgClassRelfilenumber, StoreAttrMissingVal, CHKATYPE_ANYARRAY, CHKATYPE_ANYRECORD, CHKATYPE_IS_PARTKEY, CHKATYPE_IS_VIRTUAL};
pub use partition::{update_default_partition_oid, RemovePartitionKeyByRelId, StorePartitionBound, StorePartitionKey};
pub use drop::{heap_drop_with_catalog, CheckTableNotInUse, CopyStatistics, DeleteAttributeTuples, DeleteRelationTuple, DeleteSystemAttributeTuples, RemoveAttributeById, RemoveStatistics};

use types_core::catalog::{CIDOID, OIDOID, TIDOID, XIDOID};
use types_core::{AttrNumber, InvalidOid, NAMEDATALEN};
use types_tuple::htup::{
    MaxCommandIdAttributeNumber, MaxTransactionIdAttributeNumber, MinCommandIdAttributeNumber,
    MinTransactionIdAttributeNumber, SelfItemPointerAttributeNumber, TableOidAttributeNumber,
};
use types_tuple::{
    FormData_pg_attribute, NameData, TYPALIGN_INT, TYPALIGN_SHORT, TYPSTORAGE_PLAIN,
};

// heap.c reports every one of these catalog-probe misses with
// `elog(ERROR, "cache lookup failed for ...", ...)` (:1603, :1725, :1820,
// :1874, :2015, :2067, :2129, :4070, :4107) -- a catchable error carrying
// elog's default SQLSTATE for ERROR, XX000.  Never a backend abort.
#[cold]
#[inline(never)]
pub(crate) fn relation_lookup_failed(relid: types_core::Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!("cache lookup failed for relation {relid}")))
}

#[cold]
#[inline(never)]
pub(crate) fn attribute_lookup_failed(
    attnum: AttrNumber,
    relid: types_core::Oid,
) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    )))
}

// heap.c:2129 formats the column by *name* here (SetAttrMissing's
// get_attnum/SearchSysCacheAttName path), not by attnum.
#[cold]
#[inline(never)]
pub(crate) fn attribute_name_lookup_failed(
    attname: &str,
    relid: types_core::Oid,
) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for attribute {attname} of relation {relid}"
    )))
}

#[cold]
#[inline(never)]
pub(crate) fn foreign_table_lookup_failed(relid: types_core::Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for foreign table {relid}"
    )))
}

#[cold]
#[inline(never)]
pub(crate) fn partition_key_lookup_failed(relid: types_core::Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for partition key of relation {relid}"
    )))
}

const fn name(s: &str) -> NameData {
    let mut data = [0u8; NAMEDATALEN as usize];
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        data[i] = b[i];
        i += 1;
    }
    NameData { data }
}

const fn sysatt(
    attname: &str,
    atttypid: types_core::Oid,
    attlen: i16,
    attnum: i32,
    attbyval: bool,
    attalign: i8,
) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attrelid: InvalidOid,
        attname: name(attname),
        atttypid,
        attlen,
        attnum: attnum as i16,
        atttypmod: -1,
        attndims: 0,
        attbyval,
        attalign,
        attstorage: TYPSTORAGE_PLAIN,
        attcompression: 0,
        attnotnull: true,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: 0,
        attisdropped: false,
        attislocal: true,
        attinhcount: 0,
        attcollation: InvalidOid,
    }
}

pub static SysAtt: [FormData_pg_attribute; 6] = [
    sysatt("ctid", TIDOID, 6, SelfItemPointerAttributeNumber, false, TYPALIGN_SHORT),
    sysatt("xmin", XIDOID, 4, MinTransactionIdAttributeNumber, true, TYPALIGN_INT),
    sysatt("cmin", CIDOID, 4, MinCommandIdAttributeNumber, true, TYPALIGN_INT),
    sysatt("xmax", XIDOID, 4, MaxTransactionIdAttributeNumber, true, TYPALIGN_INT),
    sysatt("cmax", CIDOID, 4, MaxCommandIdAttributeNumber, true, TYPALIGN_INT),
    sysatt("tableoid", OIDOID, 4, TableOidAttributeNumber, true, TYPALIGN_INT),
];

pub fn SystemAttributeDefinition(attno: AttrNumber) -> &'static FormData_pg_attribute {
    let attno = attno as i32;
    if attno >= 0 || attno < -(SysAtt.len() as i32) {
        panic!("invalid system attribute number {attno}");
    }
    &SysAtt[(-attno - 1) as usize]
}

pub fn SystemAttributeByName(attname: &str) -> Option<&'static FormData_pg_attribute> {
    SysAtt.iter().find(|att| att.attname.name_str() == attname.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    // heap.c reports each of these misses with
    // elog(ERROR, "cache lookup failed for ...") -- catchable, level ERROR,
    // SQLSTATE XX000.  pgrust used to panic!(), which aborts the backend.
    #[test]
    fn cache_lookup_failures_are_catchable_xx000() {
        let cases: [(Box<types_error::PgError>, &str); 5] = [
            (relation_lookup_failed(16384), "cache lookup failed for relation 16384"),
            (
                attribute_lookup_failed(3, 16384),
                "cache lookup failed for attribute 3 of relation 16384",
            ),
            (
                attribute_name_lookup_failed("c1", 16384),
                "cache lookup failed for attribute c1 of relation 16384",
            ),
            (foreign_table_lookup_failed(16385), "cache lookup failed for foreign table 16385"),
            (
                partition_key_lookup_failed(16386),
                "cache lookup failed for partition key of relation 16386",
            ),
        ];
        for (e, want) in cases {
            assert_eq!(e.message(), want);
            assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
            assert_eq!(e.level(), types_error::ERROR);
        }
    }

    #[test]
    fn sysatt_rows_match_sysattr_h_and_pg_type() {
        let expect = [
            ("ctid", -1, 27, 6, false),
            ("xmin", -2, 28, 4, true),
            ("cmin", -3, 29, 4, true),
            ("xmax", -4, 28, 4, true),
            ("cmax", -5, 29, 4, true),
            ("tableoid", -6, 26, 4, true),
        ];
        for (name, attnum, typid, len, byval) in expect {
            let att = SystemAttributeByName(name).unwrap();
            assert_eq!(att.attnum as i32, attnum);
            assert_eq!(att.atttypid, typid);
            assert_eq!(att.attlen, len);
            assert_eq!(att.attbyval, byval);
            assert_eq!(att.atttypmod, -1);
            assert!(att.attnotnull && att.attislocal && !att.attisdropped);
            assert!(core::ptr::eq(att, SystemAttributeDefinition(attnum as AttrNumber)));
        }
        assert!(SystemAttributeByName("oid").is_none());
        assert!(SystemAttributeByName("nope").is_none());
    }

    #[test]
    #[should_panic(expected = "invalid system attribute number")]
    fn out_of_range_attno_is_loud() {
        SystemAttributeDefinition(-7);
    }
}
