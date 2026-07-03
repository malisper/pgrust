use core::cell::Cell;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, ATTNULLABLE_UNRESTRICTED,
};

use crate::{attrs, int2vector_values, oidvector_values, pg_class};

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

fn tupdesc<'mcx>(mcx: Mcx<'mcx>, cols: &[CompactAttribute]) -> TupleDescData<'mcx> {
    let mut compact: PgVec<'_, CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<'_, FormData_pg_attribute> = PgVec::new_in(mcx);
    for c in cols {
        compact.push(c.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn pg_class_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let n1 = || attr(1, true, 1);
    let o4 = || attr(4, true, 4);
    let cols = [
        o4(),               // 1 oid
        attr(64, false, 1), // 2 relname
        o4(),               // 3 relnamespace
        o4(),               // 4 reltype
        o4(),               // 5 reloftype
        o4(),               // 6 relowner
        o4(),               // 7 relam
        o4(),               // 8 relfilenode
        o4(),               // 9 reltablespace
        o4(),               // 10 relpages
        o4(),               // 11 reltuples (float4)
        o4(),               // 12 relallvisible
        o4(),               // 13 relallfrozen
        o4(),               // 14 reltoastrelid
        n1(),               // 15 relhasindex
        n1(),               // 16 relisshared
        n1(),               // 17 relpersistence
        n1(),               // 18 relkind
        attr(2, true, 2),   // 19 relnatts
        attr(2, true, 2),   // 20 relchecks
        n1(),               // 21 relhasrules
        n1(),               // 22 relhastriggers
        n1(),               // 23 relhassubclass
        n1(),               // 24 relrowsecurity
        n1(),               // 25 relforcerowsecurity
        n1(),               // 26 relispopulated
        n1(),               // 27 relreplident
        n1(),               // 28 relispartition
        o4(),               // 29 relrewrite
        o4(),               // 30 relfrozenxid
        o4(),               // 31 relminmxid
        attr(-1, false, 4), // 32 relacl
        attr(-1, false, 4), // 33 reloptions
        attr(-1, false, 4), // 34 relpartbound
    ];
    tupdesc(mcx, &cols)
}

fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> (PgVec<'mcx, u8>, Datum) {
    let mut n = NameData::default();
    n.namestrcpy(s);
    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    mcx::vec_append_bytes(&mut buf, &n.data).unwrap();
    let d = Datum::from_usize(buf.as_ptr() as usize);
    (buf, d)
}

#[test]
fn decode_pg_class_roundtrips_a_formed_row() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let td = pg_class_tupdesc(mcx);
    let (_nb, relname) = name_datum(mcx, "pg_class");

    let mut values = [Datum::null(); 34];
    let mut nulls = [false; 34];
    values[0] = Datum::from_oid(1259);
    values[1] = relname;
    values[2] = Datum::from_oid(11);
    values[3] = Datum::from_oid(83);
    values[5] = Datum::from_oid(10);
    values[6] = Datum::from_oid(2);
    values[7] = Datum::from_oid(0);
    values[8] = Datum::from_oid(0);
    values[9] = Datum::from_i32(14);
    values[10] = Datum::from_f32(415.0);
    values[11] = Datum::from_i32(12);
    values[13] = Datum::from_oid(0);
    values[14] = Datum::from_bool(true);
    values[16] = Datum::from_u8(b'p');
    values[17] = Datum::from_u8(b'r');
    values[18] = Datum::from_i16(34);
    values[26] = Datum::from_u8(b'n');
    values[29] = Datum::from_u32(722);
    values[30] = Datum::from_u32(1);
    nulls[31] = true;
    nulls[32] = true;
    nulls[33] = true;

    let tup = heaptuple::heap_form_tuple(mcx, &td, &values, &nulls).unwrap();
    let scanned = pg_class::decode(mcx, &td, tup.as_tuple(), 1259).unwrap();
    let f = &scanned.form;
    assert_eq!(f.relname.name_str(), b"pg_class");
    assert_eq!(f.relnamespace, 11);
    assert_eq!(f.reltype, 83);
    assert_eq!(f.relowner, 10);
    assert_eq!(f.relam, 2);
    assert_eq!(f.reltablespace, 0);
    assert_eq!(f.relpages, 14);
    assert_eq!(f.reltuples, 415.0);
    assert!(f.relhasindex);
    assert!(!f.relisshared);
    assert_eq!(f.relpersistence, b'p');
    assert_eq!(f.relkind, b'r');
    assert_eq!(f.relreplident, b'n');
    assert_eq!(f.relfrozenxid, 722);
    assert_eq!(f.relminmxid, 1);
    assert!(scanned.options.is_none());
}

fn pg_attribute_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let n1 = || attr(1, true, 1);
    let cols = [
        attr(4, true, 4),   // 1 attrelid
        attr(64, false, 1), // 2 attname
        attr(4, true, 4),   // 3 atttypid
        attr(2, true, 2),   // 4 attlen
        attr(2, true, 2),   // 5 attnum
        attr(4, true, 4),   // 6 atttypmod
        attr(2, true, 2),   // 7 attndims
        n1(),               // 8 attbyval
        n1(),               // 9 attalign
        n1(),               // 10 attstorage
        n1(),               // 11 attcompression
        n1(),               // 12 attnotnull
        n1(),               // 13 atthasdef
        n1(),               // 14 atthasmissing
        n1(),               // 15 attidentity
        n1(),               // 16 attgenerated
        n1(),               // 17 attisdropped
        n1(),               // 18 attislocal
        attr(2, true, 2),   // 19 attinhcount
        attr(4, true, 4),   // 20 attcollation
        attr(2, true, 2),   // 21 attstattarget
        attr(-1, false, 4), // 22 attacl
        attr(-1, false, 4), // 23 attoptions
        attr(-1, false, 4), // 24 attfdwoptions
        attr(-1, false, 8), // 25 attmissingval
    ];
    tupdesc(mcx, &cols)
}

#[test]
fn decode_pg_attribute_roundtrips_a_formed_row() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let td = pg_attribute_tupdesc(mcx);
    let (_nb, attname) = name_datum(mcx, "relnamespace");

    let mut values = [Datum::null(); 25];
    let mut nulls = [false; 25];
    values[0] = Datum::from_oid(1259);
    values[1] = attname;
    values[2] = Datum::from_oid(26);
    values[3] = Datum::from_i16(4);
    values[4] = Datum::from_i16(3);
    values[5] = Datum::from_i32(-1);
    values[6] = Datum::from_i16(0);
    values[7] = Datum::from_bool(true);
    values[8] = Datum::from_i8(b'i' as i8);
    values[9] = Datum::from_i8(b'p' as i8);
    values[10] = Datum::from_i8(0);
    values[11] = Datum::from_bool(true);
    values[17] = Datum::from_bool(true);
    values[18] = Datum::from_i16(0);
    values[19] = Datum::from_oid(0);
    nulls[20] = true;
    nulls[21] = true;
    nulls[22] = true;
    nulls[23] = true;
    nulls[24] = true;

    let tup = heaptuple::heap_form_tuple(mcx, &td, &values, &nulls).unwrap();
    let a = attrs::decode(&td, tup.as_tuple(), 1259).unwrap();
    assert_eq!(a.attrelid, 1259);
    assert_eq!(a.attname.name_str(), b"relnamespace");
    assert_eq!(a.atttypid, 26);
    assert_eq!(a.attlen, 4);
    assert_eq!(a.attnum, 3);
    assert_eq!(a.atttypmod, -1);
    assert!(a.attbyval);
    assert_eq!(a.attalign, b'i' as i8);
    assert_eq!(a.attstorage, b'p' as i8);
    assert!(a.attnotnull);
    assert!(!a.atthasdef);
    assert!(!a.atthasmissing);
    assert!(!a.attisdropped);
    assert!(a.attislocal);
}

#[repr(C)]
struct FakeOidVector {
    hdr: array::oidvector,
    values: [Oid; 3],
}

#[repr(C)]
struct FakeInt2Vector {
    hdr: array::int2vector,
    values: [i16; 3],
}

#[test]
fn vector_decoders_read_inline_values() {
    let ov = FakeOidVector {
        hdr: array::oidvector {
            vl_len_: 0,
            ndim: 1,
            dataoffset: 0,
            elemtype: 26,
            dim1: 3,
            lbound1: 0,
        },
        values: [1981, 1979, 10],
    };
    let d = Datum::from_usize(&ov as *const _ as usize);
    // SAFETY: live inline image matching the plain-storage layout.
    assert_eq!(unsafe { oidvector_values(d) }, &[1981, 1979, 10]);

    let iv = FakeInt2Vector {
        hdr: array::int2vector {
            vl_len_: 0,
            ndim: 1,
            dataoffset: 0,
            elemtype: 21,
            dim1: 3,
            lbound1: 0,
        },
        values: [1, -2, 3],
    };
    let d = Datum::from_usize(&iv as *const _ as usize);
    // SAFETY: as above.
    assert_eq!(unsafe { int2vector_values(d) }, &[1, -2, 3]);
}
