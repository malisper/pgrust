//! fmgr wrappers for the ported geo_ops.c subset.

use ::datum::Datum;
use ::types_core::geo::{Point, BOX};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, cstring_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};

use crate::{
    box_contain_box, box_contain_point, box_in, box_ov, path_encode_none, point_dt,
    point_eq_point, point_in, FPge, FPgt, FPle, FPlt,
};

// SAFETY (all arg helpers): strict fns, catalog arg types point (16B) / box
// (32B) by-ref; pointers live for the call.
unsafe fn arg_box(fcinfo: &Fcinfo, i: usize) -> BOX {
    let b = fcinfo.arg_fixed(i, 32);
    BOX::from_datum_bytes(b)
}

unsafe fn arg_point(fcinfo: &Fcinfo, i: usize) -> Point {
    let b = fcinfo.arg_fixed(i, 16);
    Point::from_datum_bytes(b)
}

unsafe fn arg_cstr<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    fcinfo
        .arg_cstring(i)
        .to_str()
        .map_err(|_| Box::new(::types_error::PgError::error("invalid UTF-8 in cstring arg")))
}

fn out_cstring(fcinfo: &Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut v: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len() + 1)?;
    v.extend_from_slice(bytes);
    v.push(0);
    Ok(cstring_result(v))
}

fn fc_box_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let b = box_in(s)?;
    byref_result(fcinfo.result_mcx(), &b.to_datum_bytes())
}

fn fc_box_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let b = unsafe { arg_box(fcinfo, 0) };
    let mut out = Vec::with_capacity(64);
    path_encode_none(&[b.high, b.low], &mut out);
    out_cstring(fcinfo, &out)
}

fn fc_point_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let p = point_in(s)?;
    byref_result(fcinfo.result_mcx(), &p.to_datum_bytes())
}

fn fc_point_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_point(fcinfo, 0) };
    let mut out = Vec::with_capacity(32);
    path_encode_none(&[p], &mut out);
    out_cstring(fcinfo, &out)
}

macro_rules! box_bool_op {
    ($fname:ident, $b1:ident, $b2:ident, $expr:expr) => {
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let $b1 = unsafe { arg_box(fcinfo, 0) };
            let $b2 = unsafe { arg_box(fcinfo, 1) };
            Ok(Datum::from_bool($expr))
        }
    };
}

box_bool_op!(fc_box_overlap, b1, b2, box_ov(&b1, &b2));
box_bool_op!(fc_box_left, b1, b2, FPlt(b1.high.x, b2.low.x));
box_bool_op!(fc_box_overleft, b1, b2, FPle(b1.high.x, b2.high.x));
box_bool_op!(fc_box_right, b1, b2, FPgt(b1.low.x, b2.high.x));
box_bool_op!(fc_box_overright, b1, b2, FPge(b1.low.x, b2.low.x));
box_bool_op!(fc_box_below, b1, b2, FPlt(b1.high.y, b2.low.y));
box_bool_op!(fc_box_overbelow, b1, b2, FPle(b1.high.y, b2.high.y));
box_bool_op!(fc_box_above, b1, b2, FPgt(b1.low.y, b2.high.y));
box_bool_op!(fc_box_overabove, b1, b2, FPge(b1.low.y, b2.low.y));
box_bool_op!(fc_box_contained, b1, b2, box_contain_box(&b2, &b1));
box_bool_op!(fc_box_contain, b1, b2, box_contain_box(&b1, &b2));
box_bool_op!(
    fc_box_same,
    b1,
    b2,
    point_eq_point(&b1.high, &b2.high) && point_eq_point(&b1.low, &b2.low)
);

macro_rules! point_bool_op {
    ($fname:ident, $p1:ident, $p2:ident, $expr:expr) => {
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let $p1 = unsafe { arg_point(fcinfo, 0) };
            let $p2 = unsafe { arg_point(fcinfo, 1) };
            Ok(Datum::from_bool($expr))
        }
    };
}

point_bool_op!(fc_point_left, p1, p2, FPlt(p1.x, p2.x));
point_bool_op!(fc_point_right, p1, p2, FPgt(p1.x, p2.x));
point_bool_op!(fc_point_above, p1, p2, FPgt(p1.y, p2.y));
point_bool_op!(fc_point_below, p1, p2, FPlt(p1.y, p2.y));
point_bool_op!(fc_point_eq, p1, p2, point_eq_point(&p1, &p2));
point_bool_op!(fc_point_ne, p1, p2, !point_eq_point(&p1, &p2));

fn fc_on_pb(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 0) };
    let b = unsafe { arg_box(fcinfo, 1) };
    Ok(Datum::from_bool(box_contain_point(&b, &pt)))
}

fn fc_point_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p1 = unsafe { arg_point(fcinfo, 0) };
    let p2 = unsafe { arg_point(fcinfo, 1) };
    let d = point_dt(&p1, &p2)?;
    Ok(Datum::from_f64(d))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const GEO_BUILTINS: &[FmgrBuiltin] = &[
    b(117, "point_in", 1, fc_point_in),
    b(118, "point_out", 1, fc_point_out),
    b(123, "box_in", 1, fc_box_in),
    b(124, "box_out", 1, fc_box_out),
    b(125, "box_overlap", 2, fc_box_overlap),
    b(131, "point_above", 2, fc_point_above),
    b(132, "point_left", 2, fc_point_left),
    b(133, "point_right", 2, fc_point_right),
    b(134, "point_below", 2, fc_point_below),
    b(135, "point_eq", 2, fc_point_eq),
    b(136, "on_pb", 2, fc_on_pb),
    b(186, "box_same", 2, fc_box_same),
    b(187, "box_contain", 2, fc_box_contain),
    b(188, "box_left", 2, fc_box_left),
    b(189, "box_overleft", 2, fc_box_overleft),
    b(190, "box_overright", 2, fc_box_overright),
    b(191, "box_right", 2, fc_box_right),
    b(192, "box_contained", 2, fc_box_contained),
    b(988, "point_ne", 2, fc_point_ne),
    b(991, "point_distance", 2, fc_point_distance),
    b(2562, "box_below", 2, fc_box_below),
    b(2563, "box_overbelow", 2, fc_box_overbelow),
    b(2564, "box_overabove", 2, fc_box_overabove),
    b(2565, "box_above", 2, fc_box_above),
];
