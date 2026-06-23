//! Builder / copy / compare / hash tests over the owned `TupleDescData`.
//!
//! Seam slots are process-global, so the cross-subsystem seams this unit calls
//! (`is_catalog_relation_oid`, `search_type_attr_info`, `hash_bytes_uint32`)
//! are installed once for the whole test binary. Each test threads a
//! `MemoryContext` handle (the translation of C's `CurrentMemoryContext`).

extern crate std;

use std::sync::Once;

use mcx::{Mcx, MemoryContext};
use ::types_error::PgResult;
use types_tuple::tupdesc::PgTypeInfo;
use ::types_tuple::heaptuple::{
    ATTNULLABLE_UNKNOWN, ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID, BOOLOID, INT4OID, INT8OID,
    OIDOID, RECORDOID, TEXTOID, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPSTORAGE_EXTENDED,
    TYPSTORAGE_PLAIN,
};
use ::types_core::primitive::InvalidOid;

use crate::*;

/// Catalog OID we mark as "is a catalog relation" in the test seam.
const TEST_CATALOG_RELID: Oid = 1249; // pg_attribute

fn install_test_seams() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        catalog_seams::is_catalog_relation_oid::set(|relid| {
            relid == TEST_CATALOG_RELID
        });
        syscache_seams::search_type_attr_info::set(stub_type_info);
        // A deterministic stand-in for hash_bytes_uint32 so hashRowType is
        // exercisable without the unported hashfn owner; the equality contract
        // (equal row types -> equal hash) only needs a pure function of `k`.
        hashfn_seams::hash_bytes_uint32::set(|k| k.wrapping_mul(2654435761));
    });
}

fn stub_type_info(oidtypeid: Oid) -> PgResult<Option<PgTypeInfo>> {
    Ok(match oidtypeid {
        TEXTOID => Some(PgTypeInfo {
            typlen: -1,
            typbyval: false,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_EXTENDED,
            typcollation: 100,
        }),
        INT4OID | OIDOID => Some(PgTypeInfo {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        INT8OID => Some(PgTypeInfo {
            typlen: 8,
            typbyval: true,
            typalign: TYPALIGN_DOUBLE,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        BOOLOID => Some(PgTypeInfo {
            typlen: 1,
            typbyval: true,
            typalign: TYPALIGN_CHAR,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        _ => None,
    })
}

fn with_mcx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    install_test_seams();
    let cx = MemoryContext::new("tupdesc-test");
    f(cx.mcx())
}

#[test]
fn template_is_anonymous_record() {
    with_mcx(|mcx| {
        let desc = CreateTemplateTupleDesc(mcx, 3).unwrap();
        assert_eq!(desc.natts, 3);
        assert_eq!(desc.tdtypeid, RECORDOID);
        assert_eq!(desc.tdtypmod, -1);
        assert_eq!(desc.tdrefcount, -1);
        assert!(desc.constr.is_none());
        assert_eq!(desc.attrs.len(), 3);
        assert_eq!(desc.compact_attrs.len(), 3);
    });
}

#[test]
fn negative_natts_errors() {
    with_mcx(|mcx| {
        assert!(CreateTemplateTupleDesc(mcx, -1).is_err());
    });
}

#[test]
fn init_entry_text_fields() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut desc, 1, Some("col"), TEXTOID, -1, 0).unwrap();
        let att = desc.attr(0);
        assert_eq!(att.atttypid, TEXTOID);
        assert_eq!(att.attlen, -1);
        assert!(!att.attbyval);
        assert_eq!(att.attalign, TYPALIGN_INT);
        assert_eq!(att.attstorage, TYPSTORAGE_EXTENDED);
        assert_eq!(att.attcollation, 100);
        assert_eq!(att.attname.name_str(), b"col");
        assert_eq!(att.attnum, 1);
        assert!(att.attislocal);
        // compact attr derived
        assert_eq!(desc.compact_attr(0).attlen, -1);
        assert!(desc.compact_attr(0).attispackable);
        assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_UNRESTRICTED);
    });
}

#[test]
fn init_entry_cache_miss_errors() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        // type 9999 is not in the stub table -> Ok(None) -> "cache lookup failed".
        assert!(TupleDescInitEntry(&mut desc, 1, Some("x"), 9999, -1, 0).is_err());
    });
}

#[test]
fn builtin_entry_int8() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitBuiltinEntry(&mut desc, 1, "n", INT8OID, -1, 0).unwrap();
        let att = desc.attr(0);
        assert_eq!(att.attlen, 8);
        assert!(att.attbyval);
        assert_eq!(att.attalign, TYPALIGN_DOUBLE);
    });
}

#[test]
fn builtin_unsupported_errors() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        assert!(TupleDescInitBuiltinEntry(&mut desc, 1, "x", 9999, -1, 0).is_err());
    });
}

#[test]
fn attno_out_of_range_errors() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        assert!(TupleDescInitEntry(&mut desc, 2, Some("x"), INT4OID, -1, 0).is_err());
        assert!(TupleDescInitEntry(&mut desc, 0, Some("x"), INT4OID, -1, 0).is_err());
    });
}

#[test]
fn collation_override() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut desc, 1, Some("c"), TEXTOID, -1, 0).unwrap();
        TupleDescInitEntryCollation(&mut desc, 1, 12345).unwrap();
        assert_eq!(desc.attr(0).attcollation, 12345);
    });
}

#[test]
fn attnullability_catalog_vs_not() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut desc, 1, Some("c"), INT4OID, -1, 0).unwrap();
        // not-null + catalog relid -> VALID
        desc.attr_mut(0).attnotnull = true;
        desc.attr_mut(0).attrelid = TEST_CATALOG_RELID;
        populate_compact_attribute(&mut desc, 0).unwrap();
        assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_VALID);
        // not-null + non-catalog relid -> UNKNOWN
        desc.attr_mut(0).attrelid = 99999;
        populate_compact_attribute(&mut desc, 0).unwrap();
        assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_UNKNOWN);
    });
}

#[test]
fn copy_clears_constraint_fields() {
    with_mcx(|mcx| {
        let mut src = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut src, 1, Some("c"), INT4OID, -1, 0).unwrap();
        src.attr_mut(0).attnotnull = true;
        src.attr_mut(0).atthasdef = true;
        src.tdtypeid = 16385;
        src.tdtypmod = 7;

        let copy = CreateTupleDescCopy(mcx, &src).unwrap();
        assert_eq!(copy.tdtypeid, 16385);
        assert_eq!(copy.tdtypmod, 7);
        assert!(!copy.attr(0).attnotnull);
        assert!(!copy.attr(0).atthasdef);
        // type fields preserved
        assert_eq!(copy.attr(0).atttypid, INT4OID);
    });
}

#[test]
fn truncated_copy_keeps_prefix() {
    with_mcx(|mcx| {
        let mut src = CreateTemplateTupleDesc(mcx, 3).unwrap();
        TupleDescInitEntry(&mut src, 1, Some("a"), INT4OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut src, 2, Some("b"), INT8OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut src, 3, Some("c"), TEXTOID, -1, 0).unwrap();

        let copy = CreateTupleDescTruncatedCopy(mcx, &src, 2).unwrap();
        assert_eq!(copy.natts, 2);
        assert_eq!(copy.attr(0).attname.name_str(), b"a");
        assert_eq!(copy.attr(1).attname.name_str(), b"b");
    });
}

#[test]
fn copy_entry_sets_attnum() {
    with_mcx(|mcx| {
        let mut src = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut src, 1, Some("src"), TEXTOID, -1, 0).unwrap();
        let mut dst = CreateTemplateTupleDesc(mcx, 2).unwrap();
        TupleDescInitEntry(&mut dst, 1, Some("x"), INT4OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut dst, 2, Some("y"), INT4OID, -1, 0).unwrap();

        TupleDescCopyEntry(&mut dst, 2, &src, 1).unwrap();
        assert_eq!(dst.attr(1).attname.name_str(), b"src");
        assert_eq!(dst.attr(1).atttypid, TEXTOID);
        assert_eq!(dst.attr(1).attnum, 2);
    });
}

#[test]
fn tuple_desc_copy_into() {
    with_mcx(|mcx| {
        let mut src = CreateTemplateTupleDesc(mcx, 2).unwrap();
        TupleDescInitEntry(&mut src, 1, Some("a"), INT4OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut src, 2, Some("b"), TEXTOID, -1, 0).unwrap();
        src.tdtypeid = 16400;
        let mut dst = CreateTemplateTupleDesc(mcx, 2).unwrap();
        TupleDescCopy(&mut dst, &src).unwrap();
        assert_eq!(dst.tdtypeid, 16400);
        assert_eq!(dst.tdrefcount, -1);
        assert_eq!(dst.attr(1).attname.name_str(), b"b");
    });
}

#[test]
fn equal_and_unequal_descs() {
    with_mcx(|mcx| {
        let mut a = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut a, 1, Some("c"), INT4OID, -1, 0).unwrap();
        let mut b = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut b, 1, Some("c"), INT4OID, -1, 0).unwrap();
        assert!(equalTupleDescs(&a, &b));
        assert!(equalRowTypes(&a, &b));

        // different name
        let mut c = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut c, 1, Some("other"), INT4OID, -1, 0).unwrap();
        assert!(!equalTupleDescs(&a, &c));
        assert!(!equalRowTypes(&a, &c));
    });
}

#[test]
fn hash_matches_equal_row_types() {
    with_mcx(|mcx| {
        let mut a = CreateTemplateTupleDesc(mcx, 2).unwrap();
        TupleDescInitEntry(&mut a, 1, Some("x"), INT4OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut a, 2, Some("y"), TEXTOID, -1, 0).unwrap();
        let mut b = CreateTemplateTupleDesc(mcx, 2).unwrap();
        TupleDescInitEntry(&mut b, 1, Some("x"), INT4OID, -1, 0).unwrap();
        TupleDescInitEntry(&mut b, 2, Some("y"), TEXTOID, -1, 0).unwrap();
        assert!(equalRowTypes(&a, &b));
        assert_eq!(hashRowType(&a), hashRowType(&b));
    });
}

#[test]
fn refcount_lifecycle() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        desc.tdrefcount = 0;
        IncrTupleDescRefCount(&mut desc).unwrap();
        assert_eq!(desc.tdrefcount, 1);
        IncrTupleDescRefCount(&mut desc).unwrap();
        assert_eq!(desc.tdrefcount, 2);
        let still = DecrTupleDescRefCount(desc).unwrap();
        let still = still.expect("one ref remains");
        assert_eq!(still.tdrefcount, 1);
        let gone = DecrTupleDescRefCount(still).unwrap();
        assert!(gone.is_none());
    });
}

#[test]
fn free_rejects_pinned() {
    with_mcx(|mcx| {
        let mut desc = CreateTemplateTupleDesc(mcx, 1).unwrap();
        desc.tdrefcount = 1;
        assert!(FreeTupleDesc(desc).is_err());
    });
}

#[test]
fn create_tuple_desc_from_attrs() {
    with_mcx(|mcx| {
        let mut src = CreateTemplateTupleDesc(mcx, 1).unwrap();
        TupleDescInitEntry(&mut src, 1, Some("a"), INT4OID, -1, 0).unwrap();
        let attrs: alloc::vec::Vec<_> = (0..src.natts as usize).map(|i| *src.attr(i)).collect();
        let desc = CreateTupleDesc(mcx, &attrs).unwrap();
        assert_eq!(desc.natts, 1);
        assert_eq!(desc.attr(0).atttypid, INT4OID);
    });
}
