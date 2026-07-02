use core::cell::Cell;

use cache_syscache::SysCacheKey;
use catcache::CCFastKind;
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_pathnodes::{COMPARE_LT, COMPARE_NE};
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, ATTNULLABLE_UNRESTRICTED,
};

use super::*;

fn attr(attlen: i16, attbyval: bool, attalignby: u8) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen,
        attbyval,
        attispackable: attlen == -1,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: ATTNULLABLE_UNRESTRICTED,
        attalignby,
    }
}

fn pg_am_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let cols = [
        attr(4, true, 4),   // oid
        attr(64, false, 1), // amname
        attr(4, true, 4),   // amhandler
        attr(1, true, 1),   // amtype
    ];
    let mut compact: PgVec<CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<FormData_pg_attribute> = PgVec::new_in(mcx);
    for c in &cols {
        compact.push(c.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 2601,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn insert_am_row(td: &TupleDescData<'_>, oid: Oid, name: &str, handler: Oid, amtype: u8) {
    let cx = MemoryContext::new("amapi-test-row");
    let mcx = cx.mcx();
    let mut name_buf = [0u8; NAMEDATALEN];
    name_buf[..name.len()].copy_from_slice(name.as_bytes());
    let values = [
        Datum::from_oid(oid),
        Datum::from_usize(name_buf.as_ptr() as usize),
        Datum::from_oid(handler),
        Datum::from_char(amtype as i8),
    ];
    let tup = heaptuple::heap_form_tuple(mcx, td, &values, &[false; 4]).unwrap();
    let t = tup.as_tuple();
    // SAFETY: contiguous formed image, t_len bytes from the header.
    let image = unsafe { core::slice::from_raw_parts(t.header_ptr(), t.t_len as usize) };
    catcache::testing::insert_positive(
        AMOID,
        &[
            SysCacheKey::Value(Datum::from_oid(oid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ],
        image,
    );
}

fn boot_am_fixture() {
    let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("amapi-test")));
    let td: &'static TupleDescData<'static> = Box::leak(Box::new(pg_am_tupdesc(cx.mcx())));
    catcache::testing::init_cache_bare(AMOID, 1, [CCFastKind::Int4; 4], 8, Some(td));
    insert_am_row(td, BTREE_AM_OID, "btree", F_BTHANDLER, b'i');
    insert_am_row(td, 2, "heap", 3, b't');
    insert_am_row(td, 777, "broken", InvalidOid, b'i');
    catcache::testing::insert_negative(
        AMOID,
        &[
            SysCacheKey::Value(Datum::from_oid(4242)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ],
    );
}

#[test]
fn constants_match_pg_am_h() {
    assert_eq!(F_BTHANDLER, 330);
    assert_eq!(AMTYPE_INDEX, b'i' as i8);
    assert_eq!(
        (Anum_pg_am_amname, Anum_pg_am_amhandler, Anum_pg_am_amtype),
        (2, 3, 4)
    );
}

#[test]
fn handler_dispatch_is_the_closed_set() {
    assert_eq!(GetIndexAmRoutine(F_BTHANDLER), IndexAmKind::Btree);
    let unknown = std::panic::catch_unwind(|| GetIndexAmRoutine(999));
    assert!(unknown.is_err());
}

#[test]
fn by_am_id_probes_pg_am() {
    boot_am_fixture();

    assert_eq!(
        GetIndexAmRoutineByAmId(BTREE_AM_OID, false).unwrap(),
        Some(IndexAmKind::Btree)
    );

    assert_eq!(GetIndexAmRoutineByAmId(2, true).unwrap(), None);
    let e = GetIndexAmRoutineByAmId(2, false).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(e.message, "access method \"heap\" is not of type INDEX");

    assert_eq!(GetIndexAmRoutineByAmId(777, true).unwrap(), None);
    let e = GetIndexAmRoutineByAmId(777, false).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(e.message, "index access method \"broken\" does not have a handler");

    assert_eq!(GetIndexAmRoutineByAmId(4242, true).unwrap(), None);
    let e = GetIndexAmRoutineByAmId(4242, false).unwrap_err();
    assert_eq!(e.message, "cache lookup failed for access method 4242");
}

#[test]
fn translate_btree_shortcut_matches_c() {
    for s in 1..=BTMaxStrategyNumber {
        assert_eq!(
            IndexAmTranslateStrategy(s, BTREE_AM_OID, InvalidOid, false).unwrap(),
            s as CompareType
        );
    }
    for c in COMPARE_LT..=COMPARE_GT {
        assert_eq!(
            IndexAmTranslateCompareType(c, BTREE_AM_OID, InvalidOid, false).unwrap(),
            c as StrategyNumber
        );
    }
}

#[test]
fn translate_out_of_range_needs_the_am_row() {
    boot_am_fixture();

    assert_eq!(
        IndexAmTranslateStrategy(6, BTREE_AM_OID, InvalidOid, true).unwrap(),
        COMPARE_INVALID
    );
    let e = IndexAmTranslateStrategy(6, BTREE_AM_OID, InvalidOid, false).unwrap_err();
    assert_eq!(e.message, "could not translate strategy number 6 for index AM 403");

    assert_eq!(
        IndexAmTranslateCompareType(COMPARE_NE, BTREE_AM_OID, InvalidOid, true).unwrap(),
        InvalidStrategy
    );
    let e = IndexAmTranslateCompareType(COMPARE_NE, BTREE_AM_OID, InvalidOid, false).unwrap_err();
    assert_eq!(e.message, "could not translate compare type 6 for index AM 403");
}

#[test]
fn amvalidate_is_loud() {
    let r = std::panic::catch_unwind(|| amvalidate(403));
    assert!(r.is_err());
}
