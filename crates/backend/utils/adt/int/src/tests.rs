use super::*;
use crate::builtins::*;
use crate::series::*;

use ::datum::Datum;
use ::types_fmgr::{FmgrInfo, LocalFcinfo};

extern crate std;
use std::string::String;
use std::vec::Vec;

fn out_str(f: impl Fn(&mut [u8]) -> usize) -> String {
    let mut buf = [0u8; 32];
    let n = f(&mut buf);
    core::str::from_utf8(&buf[..n]).unwrap().into()
}

#[test]
fn io_boundaries_match_c() {
    for (v, s) in [
        (0i16, "0"),
        (1, "1"),
        (-1, "-1"),
        (i16::MAX, "32767"),
        (i16::MIN, "-32768"),
    ] {
        assert_eq!(out_str(|b| int2out(v, b)), s);
        assert_eq!(int2in(s, None).unwrap(), v);
    }
    for (v, s) in [
        (0i32, "0"),
        (42, "42"),
        (-7, "-7"),
        (i32::MAX, "2147483647"),
        (i32::MIN, "-2147483648"),
        (1000000, "1000000"),
    ] {
        assert_eq!(out_str(|b| int4out(v, b)), s);
        assert_eq!(int4in(s, None).unwrap(), v);
    }
}

#[test]
fn in_error_surface_matches_c() {
    let err = int4in("2147483648", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "value \"2147483648\" is out of range for type integer"
    );
    let err = int4in("-2147483649", None).unwrap_err();
    assert_eq!(
        err.message(),
        "value \"-2147483649\" is out of range for type integer"
    );
    let err = int4in("xyz", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type integer: \"xyz\"");
    let err = int4in("", None).unwrap_err();
    assert_eq!(err.message(), "invalid input syntax for type integer: \"\"");
    let err = int2in("32768", None).unwrap_err();
    assert_eq!(
        err.message(),
        "value \"32768\" is out of range for type smallint"
    );
    assert_eq!(int2in("-32768", None).unwrap(), i16::MIN);
    assert_eq!(int4in(" 42 ", None).unwrap(), 42);
    assert_eq!(int4in("0x2A", None).unwrap(), 42);
    assert_eq!(int4in("1_000", None).unwrap(), 1000);
    assert!(int4in("42.0", None).is_err());
}

#[test]
fn arithmetic_overflow_boundaries() {
    assert_eq!(int4pl(i32::MAX - 1, 1).unwrap(), i32::MAX);
    let err = int4pl(i32::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "integer out of range");
    assert!(int4mi(i32::MIN, 1).is_err());
    assert!(int4mul(i32::MAX, 2).is_err());
    assert!(int4mul(i32::MIN, -1).is_err());
    assert_eq!(int4mi(i32::MIN + 1, 1).unwrap(), i32::MIN);

    assert!(int2pl(i16::MAX, 1).is_err());
    assert_eq!(int2pl(i16::MAX, 1).unwrap_err().message(), "smallint out of range");
    assert!(int2mi(i16::MIN, 1).is_err());
    assert!(int2mul(i16::MAX, 2).is_err());

    assert!(int24pl(i16::MAX, i32::MAX).is_err());
    assert_eq!(int24pl(i16::MAX, 1).unwrap(), 32768);
    assert_eq!(int42pl(i32::MAX - 40000, i16::MAX).unwrap(), i32::MAX - 40000 + 32767);
}

#[test]
fn division_semantics() {
    let err = int4div(1, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.message(), "division by zero");
    assert!(int2div(1, 0).is_err());
    assert!(int24div(1, 0).is_err());
    assert!(int42div(1, 0).is_err());
    assert!(int4mod(1, 0).is_err());
    assert!(int2mod(1, 0).is_err());

    // MIN / -1 is the overflow error; MIN % -1 is zero.
    assert_eq!(int4div(i32::MIN, -1).unwrap_err().message(), "integer out of range");
    assert_eq!(int2div(i16::MIN, -1).unwrap_err().message(), "smallint out of range");
    assert_eq!(int42div(i32::MIN, -1).unwrap_err().message(), "integer out of range");
    assert_eq!(int4mod(i32::MIN, -1).unwrap(), 0);
    assert_eq!(int2mod(i16::MIN, -1).unwrap(), 0);
    assert_eq!(int4div(7, -2).unwrap(), -3);
    assert_eq!(int4mod(7, -2).unwrap(), 1);
    assert_eq!(int4mod(-7, 2).unwrap(), -1);
    assert_eq!(int24div(i16::MIN, -1).unwrap(), 32768);
}

#[test]
fn unary_and_casts() {
    assert!(int4um(i32::MIN).is_err());
    assert_eq!(int4um(5).unwrap(), -5);
    assert!(int2um(i16::MIN).is_err());
    assert!(int4abs(i32::MIN).is_err());
    assert_eq!(int4abs(-7).unwrap(), 7);
    assert!(int2abs(i16::MIN).is_err());
    assert!(int4inc(i32::MAX).is_err());
    assert_eq!(int4inc(41).unwrap(), 42);
    assert_eq!(i2toi4(-5), -5);
    assert_eq!(i4toi2(32767).unwrap(), 32767);
    assert_eq!(i4toi2(-32768).unwrap(), -32768);
    assert!(i4toi2(32768).is_err());
    assert!(i4toi2(-32769).is_err());
    assert!(int4_bool(7));
    assert!(!int4_bool(0));
    assert_eq!(bool_int4(true), 1);
    assert_eq!(bool_int4(false), 0);
}

#[test]
fn gcd_lcm_rows_from_int4_sql() {
    assert_eq!(int4gcd(0, 0).unwrap(), 0);
    assert_eq!(int4gcd(0, 6410818).unwrap(), 6410818);
    assert_eq!(int4gcd(61866666, 6410818).unwrap(), 1466);
    assert_eq!(int4gcd(-61866666, 6410818).unwrap(), 1466);
    assert_eq!(int4gcd(i32::MIN, 1).unwrap(), 1);
    assert_eq!(int4gcd(i32::MIN, -1).unwrap(), 1);
    assert_eq!(int4gcd(i32::MIN, 1073741824).unwrap(), 1073741824);
    assert!(int4gcd(i32::MIN, 0).is_err());
    assert!(int4gcd(i32::MIN, i32::MIN).is_err());

    assert_eq!(int4lcm(0, 0).unwrap(), 0);
    assert_eq!(int4lcm(i32::MIN, 0).unwrap(), 0);
    assert_eq!(int4lcm(330, 462).unwrap(), 2310);
    assert_eq!(int4lcm(-330, 462).unwrap(), 2310);
    assert!(int4lcm(i32::MIN, 1).is_err());
    assert!(int4lcm(2, i32::MAX).is_err());
}

#[test]
fn bit_ops_and_shifts() {
    assert_eq!(int4and(0b1100, 0b1010), 0b1000);
    assert_eq!(int4or(0b1100, 0b1010), 0b1110);
    assert_eq!(int4xor(0b1100, 0b1010), 0b0110);
    assert_eq!(int4not(0), -1);
    assert_eq!(int4shl(1, 4), 16);
    assert_eq!(int4shr(-16, 2), -4);
    assert_eq!(int2shl(1, 4), 16);
    assert_eq!(int2shl(i16::MAX, 1), -2);
    assert_eq!(int2shr(-16, 2), -4);
    assert_eq!(int2not(0), -1);
}

#[test]
fn in_range_branches() {
    let err = in_range_int4_int4(0, 0, -1, false, true).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
    assert_eq!(
        err.message(),
        "invalid preceding or following size in window function"
    );
    assert!(in_range_int4_int4(5, i32::MAX, 10, false, true).unwrap());
    assert!(!in_range_int4_int4(5, i32::MAX, 10, false, false).unwrap());
    assert!(!in_range_int4_int4(5, i32::MIN, 10, true, true).unwrap());
    assert!(in_range_int4_int4(5, i32::MIN, 10, true, false).unwrap());
    assert!(in_range_int4_int4(5, 3, 4, false, true).unwrap());
    assert!(!in_range_int4_int4(8, 3, 4, false, true).unwrap());
    assert!(in_range_int4_int8(5, 3, 4, false, true).unwrap());
    assert!(in_range_int2_int8(2, 1, 3, false, true).unwrap());
    assert!(in_range_int2_int2(2, 1, 3, false, true).unwrap());
    assert!(in_range_int2_int4(2, 1, 3, false, true).unwrap());
    assert!(in_range_int4_int2(2, 1, 3, false, true).unwrap());
}

#[test]
fn series_step_and_rows() {
    let mut g = GenerateSeriesInt4::new(1, 3, 1).unwrap();
    assert_eq!(
        core::iter::from_fn(|| g.next()).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let mut g = GenerateSeriesInt4::new(3, 1, -1).unwrap();
    assert_eq!(
        core::iter::from_fn(|| g.next()).collect::<Vec<_>>(),
        [3, 2, 1]
    );
    let mut g = GenerateSeriesInt4::new(i32::MAX - 1, i32::MAX, 1).unwrap();
    assert_eq!(
        core::iter::from_fn(|| g.next()).collect::<Vec<_>>(),
        [i32::MAX - 1, i32::MAX]
    );
    let err = GenerateSeriesInt4::new(1, 10, 0).unwrap_err();
    assert_eq!(err.message(), "step size cannot equal zero");
    assert_eq!(generate_series_int4_rows(1.0, 10.0, 1.0), Some(10.0));
    assert_eq!(generate_series_int4_rows(1.0, 10.0, 0.0), None);
}

#[test]
fn int2vector_image_and_io() {
    let m = mcx::MemoryContext::new("t");
    let mcx = m.mcx();
    let v = buildint2vector(mcx, &[1, -2, 32767]).unwrap();
    assert_eq!(v.len(), INT2VECTOR_HDRSZ + 6);
    // vl_len_ == SET_VARSIZE(size), ndim 1, dataoffset 0, elemtype INT2OID,
    // dim1 3, lbound1 0.
    let words: Vec<i32> = v[..24]
        .chunks(4)
        .map(|c| i32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(words, [(30 << 2), 1, 0, INT2OID as i32, 3, 0]);
    assert_eq!(&v[24..], &[1u8, 0, 0xFE, 0xFF, 0xFF, 0x7F]);

    let v = int2vectorin(mcx, " \t1 -2 32767 ", None).unwrap().unwrap();
    assert_eq!(&v[24..], &[1u8, 0, 0xFE, 0xFF, 0xFF, 0x7F]);
    // Only a literal space may follow a number (C: `*endp && *endp != ' '`).
    assert!(int2vectorin(mcx, "1\t2", None).is_err());
    let out = int2vectorout(mcx, 1, 0, INT2OID, &[1, -2, 32767]).unwrap();
    assert_eq!(core::str::from_utf8(&out).unwrap(), "1 -2 32767");

    let err = int2vectorin(mcx, "1 abc", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type smallint: \"abc\""
    );
    let err = int2vectorin(mcx, "99999", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "value \"99999\" is out of range for type smallint"
    );
    let err = int2vectorin(mcx, "1x", None).unwrap_err();
    assert_eq!(err.message(), "invalid input syntax for type smallint: \"1x\"");
    let err = int2vectorout(mcx, 2, 0, INT2OID, &[]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "array is not a valid int2vector");

    let mut soft = SoftErrorContext::new(true);
    let r = int2vectorin(mcx, "bogus", None.or(Some(&mut soft))).unwrap();
    assert!(r.is_none());
    assert!(soft.error_occurred());
}

#[test]
fn fmgr_wrappers_and_table() {
    let mut flinfo = FmgrInfo::new(fc_int4pl, 177, 2, true, false);
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(40));
    fci.set_arg(1, Datum::from_i32(2));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 42);
    fci.set_arg(0, Datum::from_i32(i32::MAX));
    fci.set_arg(1, Datum::from_i32(1));
    let err = flinfo.invoke(&mut fci).unwrap_err();
    assert_eq!(err.message(), "integer out of range");

    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(7));
    fci.set_arg(1, Datum::from_i32(7));
    assert!(fc_int4eq(None, &mut fci).unwrap().as_bool());
    fci.set_arg(1, Datum::from_i32(8));
    assert!(!fc_int4eq(None, &mut fci).unwrap().as_bool());

    // int4out through a resolved carrier: NUL-terminated cstring in the
    // flinfo scratch.
    let mut flinfo = FmgrInfo::new(fc_int4out, 43, 1, true, false);
    let mut fci = LocalFcinfo::<1>::new(0);
    for v in [0i32, i32::MIN, i32::MAX, -1] {
        fci.set_arg(0, Datum::from_i32(v));
        let d = flinfo.invoke(&mut fci).unwrap();
        let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        let mut expect = [0u8; 16];
        let n = int4out(v, &mut expect);
        assert_eq!(s.to_bytes(), &expect[..n]);
    }

    // int4in through the cstring arg lane.
    let mut fci = LocalFcinfo::<1>::new(0);
    let num = b"-2147483648\0";
    fci.set_arg(0, Datum::from_usize(num.as_ptr() as usize));
    assert_eq!(fc_int4in(None, &mut fci).unwrap().as_i32(), i32::MIN);

    // Table sanity: unique OIDs, wrapper set covers the table.
    let mut oids: Vec<u32> = INT_BUILTINS.iter().map(|b| b.foid).collect();
    oids.sort_unstable();
    let n = oids.len();
    oids.dedup();
    assert_eq!(n, oids.len());
    assert_eq!(n, 100);
    for b in INT_BUILTINS {
        assert!(b.strict);
        assert_eq!(b.retset, b.foid == 1066 || b.foid == 1067);
        assert!(matches!(b.nargs, 1 | 2 | 3 | 5));
    }
}

#[test]
fn generate_series_srf_value_per_call() {
    use ::types_fmgr::{ExprDoneCond, ReturnSetInfo, SFRM_ValuePerCall};

    let mut flinfo = FmgrInfo::new(fc_generate_series_step_int4, 1067, 2, true, true);
    let mut rsinfo = ReturnSetInfo::new(SFRM_ValuePerCall);
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(1));
    fci.set_arg(1, Datum::from_i32(3));

    let mut out = Vec::new();
    loop {
        fci.isnull = false;
        rsinfo.isDone = ExprDoneCond::ExprSingleResult;
        // Re-arm per invoke: the isDone write above invalidates a previously
        // armed pointer's provenance (miri F6).
        fci.resultinfo = rsinfo.as_fmnode_ptr();
        let d = flinfo.invoke(&mut fci).unwrap();
        if rsinfo.isDone == ExprDoneCond::ExprEndResult {
            assert!(fci.isnull);
            break;
        }
        assert_eq!(rsinfo.isDone, ExprDoneCond::ExprMultipleResult);
        out.push(d.as_i32());
    }
    assert_eq!(out, [1, 2, 3]);
    assert!(!flinfo.has_fn_extra(), "SRF_RETURN_DONE tears down the multi-call frame");

    // Zero step errors before the frame is created.
    let mut fci3 = LocalFcinfo::<3>::new(0);
    fci3.resultinfo = rsinfo.as_fmnode_ptr();
    fci3.set_arg(0, Datum::from_i32(1));
    fci3.set_arg(1, Datum::from_i32(3));
    fci3.set_arg(2, Datum::from_i32(0));
    let mut flinfo3 = FmgrInfo::new(fc_generate_series_step_int4, 1066, 3, true, true);
    let err = flinfo3.invoke(&mut fci3).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(err.message().contains("step size cannot equal zero"));
}

#[test]
fn generate_series_support_rows_estimate() {
    use ::types_nodes::supportnodes::SupportRequestRows;
    use ::types_nodes::{Node, NodeList};

    let ctx = ::mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut args = NodeList::nil();
    for v in [1i32, 10] {
        args.lappend(
            mcx,
            Node::mk(
                mcx,
                ::types_nodes::Const {
                    consttype: 23,
                    consttypmod: -1,
                    constcollid: 0,
                    constlen: 4,
                    constvalue: Datum::from_i32(v),
                    constisnull: false,
                    constbyval: true,
                    location: -1,
                },
            )
            .unwrap(),
        )
        .unwrap();
    }
    let fe = Node::mk(
        mcx,
        ::types_nodes::FuncExpr { funcid: 1067, funcresulttype: 23, funcretset: true, args, ..Default::default() },
    )
    .unwrap();

    let mut req = SupportRequestRows::new(1067, Some(fe));
    let addr = core::ptr::from_mut(&mut req) as usize;
    let mut fci = LocalFcinfo::<1>::new(0);
    fci.set_arg(0, Datum::from_usize(addr));
    let d = fc_generate_series_int4_support(None, &mut fci).unwrap();
    assert_eq!(d.as_usize(), addr, "support fn claims the request");
    assert_eq!(req.rows, 10.0);
}

#[test]
fn hashchar_zero_extends_high_bit() {
    // Ruling 2026-07-29: pin the aarch64 (unsigned char) arm — byte 0x80
    // hashes as 128, not sign-extended -128.
    let mut f = types_fmgr::LocalFcinfo::<1>::new(0);
    f.args[0] = datum::NullableDatum::value(datum::Datum::from_i8(-128)); // 0x80
    let h = crate::builtins::fc_hashchar(None, &mut f).unwrap().as_u32();
    let expect = ::hashfn::hash_bytes_uint32(128u32); // unsigned, not -128
    assert_eq!(h, expect);
}
