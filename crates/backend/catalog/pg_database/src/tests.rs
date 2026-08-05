use core::cell::Cell;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
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

// pg_database row shape: attlen/attbyval/attalign per pg_attribute.
fn pg_database_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let cols = [
        attr(4, true, 4),   // oid
        attr(64, false, 1), // datname
        attr(4, true, 4),   // datdba
        attr(4, true, 4),   // encoding
        attr(1, true, 1),   // datlocprovider
        attr(1, true, 1),   // datistemplate
        attr(1, true, 1),   // datallowconn
        attr(1, true, 1),   // dathasloginevt
        attr(4, true, 4),   // datconnlimit
        attr(4, true, 4),   // datfrozenxid
        attr(4, true, 4),   // datminmxid
        attr(4, true, 4),   // dattablespace
        attr(-1, false, 4), // datcollate
        attr(-1, false, 4), // datctype
        attr(-1, false, 4), // datlocale
        attr(-1, false, 4), // daticurules
        attr(-1, false, 4), // datcollversion
        attr(-1, false, 4), // datacl
    ];
    let mut compact: PgVec<CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<FormData_pg_attribute> = PgVec::new_in(mcx);
    for c in &cols {
        compact.push(c.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 1248,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> (datum::varlena::Varlena<'mcx>, Datum) {
    let v = varlena::cstring_to_text(mcx, s.as_bytes()).unwrap();
    let d = Datum::from_usize(v.as_bytes().as_ptr() as usize);
    (v, d)
}

#[test]
fn constants_match_pg_database_h() {
    assert_eq!(DATABASE_RELATION_ID, 1262);
    assert_eq!(DatabaseNameIndexId, 2671);
    assert_eq!(DatabaseOidIndexId, 2672);
    assert_eq!(Natts_pg_database, 18);
    assert_eq!(Anum_pg_database_datname, 2);
    assert_eq!(Anum_pg_database_datacl, 18);
    assert_eq!(DATCONNLIMIT_UNLIMITED, -1);
    assert_eq!(DATCONNLIMIT_INVALID_DB, -2);
    assert_eq!(DATABASEOID, 21);
    assert_eq!(F_NAMEEQ, 62);
    assert_eq!(F_OIDEQ, 184);
}

#[test]
fn name_arg_is_zero_padded_namedata() {
    let cx = MemoryContext::new("t");
    let (buf, d) = name_arg(cx.mcx(), "postgres").unwrap();
    assert_eq!(buf.len(), 64);
    assert_eq!(&buf[..8], b"postgres");
    assert!(buf[8..].iter().all(|&b| b == 0));
    assert_eq!(d.as_usize(), buf.as_ptr() as usize);

    let long = "x".repeat(80);
    let (buf, _) = name_arg(cx.mcx(), &long).unwrap();
    assert_eq!(buf.len(), 64);
    assert_eq!(&buf[..63], long.as_bytes()[..63].to_vec().as_slice());
    assert_eq!(buf[63], 0);
}

#[test]
fn eq_key_shape() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        fmgr_seams::fmgr_info::set(|oid| {
            Ok(types_fmgr::FmgrInfo::new(
                |_, _| panic!("not invoked"),
                oid,
                2,
                true,
                false,
            ))
        });
    });
    let key = eq_key(Anum_pg_database_oid, F_OIDEQ, Datum::from_oid(5));
    assert_eq!(key.sk_attno, 1);
    assert_eq!(key.sk_strategy, BTEqualStrategyNumber);
    assert_eq!(key.sk_collation, C_COLLATION_OID);
    assert_eq!(key.sk_func.fn_oid, F_OIDEQ);
    assert_eq!(key.sk_argument.as_oid(), 5);
    assert_eq!(key.sk_flags, 0);
    assert_eq!(key.sk_subtype, 0);
}

#[test]
fn decode_form_roundtrips_a_formed_pg_database_row() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let td = pg_database_tupdesc(mcx);

    let (name_buf, datname) = name_arg(mcx, "template1").unwrap();
    let (_c1, datcollate) = text_datum(mcx, "en_US.UTF-8");
    let (_c2, datctype) = text_datum(mcx, "C");
    let (_c3, datcollversion) = text_datum(mcx, "2.41");
    let _ = name_buf;

    let mut values = [Datum::null(); Natts_pg_database];
    let mut nulls = [false; Natts_pg_database];
    let idx = |a: i32| (a - 1) as usize;
    values[idx(Anum_pg_database_oid)] = Datum::from_oid(1);
    values[idx(Anum_pg_database_datname)] = datname;
    values[idx(Anum_pg_database_datdba)] = Datum::from_oid(10);
    values[idx(Anum_pg_database_encoding)] = Datum::from_i32(6);
    values[idx(Anum_pg_database_datlocprovider)] = Datum::from_u8(b'c');
    values[idx(Anum_pg_database_datistemplate)] = Datum::from_bool(true);
    values[idx(Anum_pg_database_datallowconn)] = Datum::from_bool(true);
    values[idx(Anum_pg_database_dathasloginevt)] = Datum::from_bool(false);
    values[idx(Anum_pg_database_datconnlimit)] = Datum::from_i32(DATCONNLIMIT_UNLIMITED);
    values[idx(Anum_pg_database_datfrozenxid)] = Datum::from_u32(722);
    values[idx(Anum_pg_database_datminmxid)] = Datum::from_u32(1);
    values[idx(Anum_pg_database_dattablespace)] = Datum::from_oid(1663);
    values[idx(Anum_pg_database_datcollate)] = datcollate;
    values[idx(Anum_pg_database_datctype)] = datctype;
    nulls[idx(Anum_pg_database_datlocale)] = true;
    nulls[idx(Anum_pg_database_daticurules)] = true;
    values[idx(Anum_pg_database_datcollversion)] = datcollversion;
    nulls[idx(Anum_pg_database_datacl)] = true;

    let tup = heaptuple::heap_form_tuple(mcx, &td, &values, &nulls).unwrap();
    let form = decode_tuple(mcx, &td, tup.as_tuple()).unwrap();

    assert_eq!(form.oid, 1);
    assert_eq!(form.datname.as_str(), "template1");
    assert_eq!(form.encoding, 6);
    assert_eq!(form.datlocprovider, b'c');
    assert!(form.datallowconn);
    assert!(!form.dathasloginevt);
    assert_eq!(form.datconnlimit, DATCONNLIMIT_UNLIMITED);
    assert_eq!(form.dattablespace, 1663);
    assert_eq!(form.datcollate.as_str(), "en_US.UTF-8");
    assert_eq!(form.datctype.as_str(), "C");
    assert!(form.datlocale.is_none());
    assert_eq!(form.datcollversion.as_ref().unwrap().as_str(), "2.41");
}

#[test]
fn decode_form_rejects_null_in_not_null_column() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let err = match decode_form(mcx, |_| Ok((Datum::null(), true))) {
        Err(e) => e,
        Ok(_) => panic!("null in NOT NULL column must fail"),
    };
    assert!(format!("{err:?}").contains("unexpected null"));
}
