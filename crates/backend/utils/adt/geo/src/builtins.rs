//! fmgr wrappers for geo_ops.c builtins.

use ::datum::Datum;
use ::types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};
use ::types_core::Oid;
use ::types_error::{PgResult, SoftErrorContext};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo,
};

use crate::{io, PathRef, PolyRef, Pts};

// SAFETY (all arg helpers): strict fns, catalog arg types point (16B) / box
// (32B) / lseg (32B) / line (24B) / circle (24B) by-ref; pointers live for the
// call.
unsafe fn arg_box(fcinfo: &Fcinfo, i: usize) -> BOX {
    BOX::from_datum_bytes(fcinfo.arg_fixed(i, 32))
}

unsafe fn arg_point(fcinfo: &Fcinfo, i: usize) -> Point {
    Point::from_datum_bytes(fcinfo.arg_fixed(i, 16))
}

unsafe fn arg_lseg(fcinfo: &Fcinfo, i: usize) -> LSEG {
    LSEG::from_datum_bytes(fcinfo.arg_fixed(i, 32))
}

unsafe fn arg_line(fcinfo: &Fcinfo, i: usize) -> LINE {
    LINE::from_datum_bytes(fcinfo.arg_fixed(i, 24))
}

unsafe fn arg_circle(fcinfo: &Fcinfo, i: usize) -> CIRCLE {
    CIRCLE::from_datum_bytes(fcinfo.arg_fixed(i, 24))
}

// SAFETY: arg i is a non-null path/polygon varlena, live for the call. The
// PathRef/PolyRef readers are unaligned-safe, so short-header payloads are
// read in place (no C-style un-packing copy); external/compressed images
// detoast inside arg_varlena_packed.
unsafe fn varlena_payload<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    Ok(unsafe { fcinfo.arg_varlena_packed(i) }?.data())
}

unsafe fn arg_path<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<PathRef<'a>> {
    Ok(PathRef::from_payload(unsafe { varlena_payload(fcinfo, i) }?))
}

unsafe fn arg_poly<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<PolyRef<'a>> {
    Ok(PolyRef::from_payload(unsafe { varlena_payload(fcinfo, i) }?))
}

unsafe fn arg_cstr<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    fcinfo
        .arg_cstring(i)
        .to_str()
        .map_err(|_| Box::new(::types_error::PgError::error("invalid UTF-8 in cstring arg")))
}

// SAFETY: fcinfo.context, if set, rides per the ErrorSaveNode contract.
fn escontext<'a>(fcinfo: &Fcinfo) -> Option<&'a mut SoftErrorContext> {
    unsafe { fcinfo.soft_error_context() }
}

// Out functions run under printtup with no armed result frame; the cstring
// rides the resolved FmgrInfo's retained scratch (numeric_out precedent).
struct OutBuf(Vec<u8>);

fn out_cstring(
    flinfo: Option<&mut FmgrInfo>,
    fill: impl FnOnce(&mut Vec<u8>),
) -> PgResult<Datum> {
    let Some(flinfo) = flinfo else {
        panic!("geo out: cstring result needs a resolved FmgrInfo's scratch")
    };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    fill(buf);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

fn ret_point(fcinfo: &Fcinfo, p: Point) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &p.to_datum_bytes())
}

fn ret_box(fcinfo: &Fcinfo, b: BOX) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &b.to_datum_bytes())
}

fn ret_lseg(fcinfo: &Fcinfo, ls: LSEG) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &ls.to_datum_bytes())
}

fn ret_line(fcinfo: &Fcinfo, l: LINE) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &l.to_datum_bytes())
}

fn ret_circle(fcinfo: &Fcinfo, c: CIRCLE) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &c.to_datum_bytes())
}

fn ret_opt_point(fcinfo: &mut Fcinfo, p: Option<Point>) -> PgResult<Datum> {
    match p {
        Some(p) => ret_point(fcinfo, p),
        None => Ok(fcinfo.return_null()),
    }
}

fn fc_point_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let p = io::point_in(s, escontext(fcinfo))?;
    ret_point(fcinfo, p)
}

fn fc_point_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_point(fcinfo, 0) };
    out_cstring(f, |buf| io::point_out(&p, buf))
}

fn fc_box_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let b = io::box_in(s, escontext(fcinfo))?;
    ret_box(fcinfo, b)
}

fn fc_box_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let b = unsafe { arg_box(fcinfo, 0) };
    out_cstring(f, |buf| io::box_out(&b, buf))
}

fn fc_lseg_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let ls = io::lseg_in(s, escontext(fcinfo))?;
    ret_lseg(fcinfo, ls)
}

fn fc_lseg_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let ls = unsafe { arg_lseg(fcinfo, 0) };
    out_cstring(f, |buf| io::lseg_out(&ls, buf))
}

fn fc_line_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let l = io::line_in(s, escontext(fcinfo))?;
    ret_line(fcinfo, l)
}

fn fc_line_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let l = unsafe { arg_line(fcinfo, 0) };
    out_cstring(f, |buf| io::line_out(&l, buf))
}

fn fc_circle_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let c = io::circle_in(s, escontext(fcinfo))?;
    ret_circle(fcinfo, c)
}

fn fc_circle_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let c = unsafe { arg_circle(fcinfo, 0) };
    out_cstring(f, |buf| io::circle_out(&c, buf))
}

fn fc_path_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let v = io::path_in(fcinfo.result_mcx(), s, escontext(fcinfo))?;
    Ok(varlena_result(v))
}

fn fc_path_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    out_cstring(f, |buf| io::path_out(&p, buf))
}

fn fc_poly_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let s = unsafe { arg_cstr(fcinfo, 0) }?;
    let v = io::poly_in(fcinfo.result_mcx(), s, escontext(fcinfo))?;
    Ok(varlena_result(v))
}

fn fc_poly_out(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    out_cstring(f, |buf| io::poly_out(&p, buf))
}

macro_rules! fc_recv {
    ($fc:ident, $core:path, $ret:ident) => {
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
            let buf = unsafe { fcinfo.arg_stringinfo(0) };
            let v = $core(buf)?;
            $ret(fcinfo, v)
        }
    };
}
fc_recv!(fc_point_recv, io::point_recv, ret_point);
fc_recv!(fc_box_recv, io::box_recv, ret_box);
fc_recv!(fc_lseg_recv, io::lseg_recv, ret_lseg);
fc_recv!(fc_line_recv, io::line_recv, ret_line);
fc_recv!(fc_circle_recv, io::circle_recv, ret_circle);

fn fc_path_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let v = io::path_recv(fcinfo.result_mcx(), buf)?;
    Ok(varlena_result(v))
}

fn fc_poly_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let v = io::poly_recv(fcinfo.result_mcx(), buf)?;
    Ok(varlena_result(v))
}

macro_rules! fc_send {
    ($fc:ident, $arg:ident, $core:path) => {
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let v = unsafe { $arg(fcinfo, 0) };
            Ok(varlena_result($core(fcinfo.result_mcx(), &v)?))
        }
    };
}
fc_send!(fc_point_send, arg_point, io::point_send);
fc_send!(fc_box_send, arg_box, io::box_send);
fc_send!(fc_lseg_send, arg_lseg, io::lseg_send);
fc_send!(fc_line_send, arg_line, io::line_send);
fc_send!(fc_circle_send, arg_circle, io::circle_send);

fn fc_path_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    Ok(varlena_result(io::path_send(fcinfo.result_mcx(), &p)?))
}

fn fc_poly_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    Ok(varlena_result(io::poly_send(fcinfo.result_mcx(), &p)?))
}

macro_rules! fc2 {
    ($fc:ident, $a0:ident, $a1:ident, $x:ident, $y:ident, $body:expr) => {
        // pub: visibility-only, exposes the shipped wrapper to the Kani
        // equivalence harnesses (proofs/geo-cmp).
        pub fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let $x = unsafe { $a0(fcinfo, 0) };
            // SAFETY: module contract.
            let $y = unsafe { $a1(fcinfo, 1) };
            $body(fcinfo, $x, $y)
        }
    };
}

macro_rules! fc1 {
    ($fc:ident, $a0:ident, $x:ident, $body:expr) => {
        // pub: visibility-only, exposes the shipped wrapper to the Kani
        // equivalence harnesses (proofs/geo-cmp).
        pub fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let $x = unsafe { $a0(fcinfo, 0) };
            $body(fcinfo, $x)
        }
    };
}

macro_rules! bool2 {
    ($($fc:ident: $a0:ident, $a1:ident => $core:path;)*) => {$(
        fc2!($fc, $a0, $a1, a, b, |_fcinfo: &mut Fcinfo, a, b| Ok(Datum::from_bool($core(&a, &b))));
    )*};
}

macro_rules! bool2r {
    ($($fc:ident: $a0:ident, $a1:ident => $core:path;)*) => {$(
        fc2!($fc, $a0, $a1, a, b, |_fcinfo: &mut Fcinfo, a, b| Ok(Datum::from_bool($core(&a, &b)?)));
    )*};
}

macro_rules! f64_2r {
    ($($fc:ident: $a0:ident, $a1:ident => $core:path;)*) => {$(
        fc2!($fc, $a0, $a1, a, b, |_fcinfo: &mut Fcinfo, a, b| Ok(Datum::from_f64($core(&a, &b)?)));
    )*};
}

// point predicates + arithmetic
bool2! {
    fc_point_left: arg_point, arg_point => crate::point::point_left;
    fc_point_right: arg_point, arg_point => crate::point::point_right;
    fc_point_above: arg_point, arg_point => crate::point::point_above;
    fc_point_below: arg_point, arg_point => crate::point::point_below;
    fc_point_vert: arg_point, arg_point => crate::point::point_vert;
    fc_point_horiz: arg_point, arg_point => crate::point::point_horiz;
    fc_point_eq: arg_point, arg_point => crate::point::point_eq;
    fc_point_ne: arg_point, arg_point => crate::point::point_ne;
}

f64_2r! {
    fc_point_distance: arg_point, arg_point => crate::point::point_distance;
    fc_point_slope: arg_point, arg_point => crate::point::point_slope;
    fc_lseg_distance: arg_lseg, arg_lseg => crate::proximity::lseg_distance;
    fc_box_distance: arg_box, arg_box => crate::boxes::box_distance;
    fc_circle_distance: arg_circle, arg_circle => crate::circle::circle_distance;
    fc_line_distance: arg_line, arg_line => crate::line::line_distance;
    fc_dist_pl: arg_point, arg_line => crate::proximity::dist_pl;
    fc_dist_lp: arg_line, arg_point => crate::proximity::dist_lp;
    fc_dist_ps: arg_point, arg_lseg => crate::proximity::dist_ps;
    fc_dist_sp: arg_lseg, arg_point => crate::proximity::dist_sp;
    fc_dist_pb: arg_point, arg_box => crate::proximity::dist_pb;
    fc_dist_bp: arg_box, arg_point => crate::proximity::dist_bp;
    fc_dist_sl: arg_lseg, arg_line => crate::proximity::dist_sl;
    fc_dist_ls: arg_line, arg_lseg => crate::proximity::dist_ls;
    fc_dist_sb: arg_lseg, arg_box => crate::proximity::dist_sb;
    fc_dist_bs: arg_box, arg_lseg => crate::proximity::dist_bs;
    fc_dist_pc: arg_point, arg_circle => crate::proximity::dist_pc;
    fc_dist_cpoint: arg_circle, arg_point => crate::proximity::dist_cpoint;
}

macro_rules! point2 {
    ($($fc:ident => $core:path;)*) => {$(
        fc2!($fc, arg_point, arg_point, a, b, |fcinfo: &mut Fcinfo, a, b| ret_point(fcinfo, $core(&a, &b)?));
    )*};
}
point2! {
    fc_point_add => crate::point::point_add_point;
    fc_point_sub => crate::point::point_sub_point;
    fc_point_mul => crate::point::point_mul_point;
    fc_point_div => crate::point::point_div_point;
}

// pub for proofs/geo-cmp (visibility-only; prove-target ruling 2026-07-28)
pub fn fc_construct_point(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let x = fcinfo.arg_f64(0);
    let y = fcinfo.arg_f64(1);
    ret_point(fcinfo, crate::point::construct_point(x, y))
}

fc1!(fc_point_box, arg_point, p, |fcinfo: &mut Fcinfo, p: Point| ret_box(
    fcinfo,
    crate::boxes::point_box(&p)
));

// box predicates
bool2! {
    fc_box_overlap: arg_box, arg_box => crate::boxes::box_overlap;
    fc_box_left: arg_box, arg_box => crate::boxes::box_left;
    fc_box_overleft: arg_box, arg_box => crate::boxes::box_overleft;
    fc_box_right: arg_box, arg_box => crate::boxes::box_right;
    fc_box_overright: arg_box, arg_box => crate::boxes::box_overright;
    fc_box_below: arg_box, arg_box => crate::boxes::box_below;
    fc_box_overbelow: arg_box, arg_box => crate::boxes::box_overbelow;
    fc_box_above: arg_box, arg_box => crate::boxes::box_above;
    fc_box_overabove: arg_box, arg_box => crate::boxes::box_overabove;
    fc_box_contained: arg_box, arg_box => crate::boxes::box_contained;
    fc_box_contain: arg_box, arg_box => crate::boxes::box_contain;
    fc_box_same: arg_box, arg_box => crate::boxes::box_same;
    fc_box_below_eq: arg_box, arg_box => crate::boxes::box_below_eq;
    fc_box_above_eq: arg_box, arg_box => crate::boxes::box_above_eq;
    fc_box_contain_pt: arg_box, arg_point => crate::proximity::box_contain_pt;
}

bool2r! {
    fc_box_lt: arg_box, arg_box => crate::boxes::box_lt;
    fc_box_le: arg_box, arg_box => crate::boxes::box_le;
    fc_box_gt: arg_box, arg_box => crate::boxes::box_gt;
    fc_box_ge: arg_box, arg_box => crate::boxes::box_ge;
    fc_box_eq: arg_box, arg_box => crate::boxes::box_eq;
}

macro_rules! f64_1r {
    ($($fc:ident: $a0:ident => $core:path;)*) => {$(
        fc1!($fc, $a0, a, |_fcinfo: &mut Fcinfo, a| Ok(Datum::from_f64($core(&a)?)));
    )*};
}
f64_1r! {
    fc_box_area: arg_box => crate::boxes::box_area;
    fc_box_width: arg_box => crate::boxes::box_width;
    fc_box_height: arg_box => crate::boxes::box_height;
    fc_circle_area: arg_circle => crate::circle::circle_area;
    fc_circle_diameter: arg_circle => crate::circle::circle_diameter;
    fc_lseg_length: arg_lseg => crate::lseg::lseg_length;
}

fc1!(fc_circle_radius, arg_circle, c, |_fcinfo: &mut Fcinfo, c: CIRCLE| Ok(
    Datum::from_f64(crate::circle::circle_radius(&c))
));

fc1!(fc_box_center, arg_box, b, |fcinfo: &mut Fcinfo, b: BOX| {
    ret_point(fcinfo, crate::boxes::box_cn(&b)?)
});
fc1!(fc_circle_center, arg_circle, c, |fcinfo: &mut Fcinfo, c: CIRCLE| {
    ret_point(fcinfo, crate::circle::circle_center(&c))
});
fc1!(fc_lseg_center, arg_lseg, ls, |fcinfo: &mut Fcinfo, ls: LSEG| {
    ret_point(fcinfo, crate::lseg::lseg_center(&ls)?)
});
fc1!(fc_box_diagonal, arg_box, b, |fcinfo: &mut Fcinfo, b: BOX| {
    ret_lseg(fcinfo, crate::boxes::box_diagonal(&b))
});
fc1!(fc_box_circle, arg_box, b, |fcinfo: &mut Fcinfo, b: BOX| {
    ret_circle(fcinfo, crate::boxes::box_circle(&b)?)
});
fc1!(fc_circle_box, arg_circle, c, |fcinfo: &mut Fcinfo, c: CIRCLE| {
    ret_box(fcinfo, crate::boxes::circle_box(&c)?)
});

macro_rules! box_pt {
    ($($fc:ident => $core:path;)*) => {$(
        fc2!($fc, arg_box, arg_point, b, p, |fcinfo: &mut Fcinfo, b, p| ret_box(fcinfo, $core(&b, &p)?));
    )*};
}
box_pt! {
    fc_box_add => crate::boxes::box_add;
    fc_box_sub => crate::boxes::box_sub;
    fc_box_mul => crate::boxes::box_mul;
    fc_box_div => crate::boxes::box_div;
}

fc2!(fc_points_box, arg_point, arg_point, a, b, |fcinfo: &mut Fcinfo, a, b| {
    ret_box(fcinfo, crate::boxes::points_box(&a, &b))
});
fc2!(fc_boxes_bound_box, arg_box, arg_box, a, b, |fcinfo: &mut Fcinfo, a, b| {
    ret_box(fcinfo, crate::boxes::boxes_bound_box(&a, &b))
});

// pub for proofs/geo-cmp (visibility-only; prove-target ruling 2026-07-28)
pub fn fc_box_intersect(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_box(fcinfo, 0) };
    // SAFETY: module contract.
    let b = unsafe { arg_box(fcinfo, 1) };
    match crate::boxes::box_intersect(&a, &b) {
        Some(r) => ret_box(fcinfo, r),
        None => Ok(fcinfo.return_null()),
    }
}

// lseg
bool2! {
    fc_lseg_eq: arg_lseg, arg_lseg => crate::lseg::lseg_eq;
    fc_lseg_ne: arg_lseg, arg_lseg => crate::lseg::lseg_ne;
}
bool2r! {
    fc_lseg_lt: arg_lseg, arg_lseg => crate::lseg::lseg_lt;
    fc_lseg_le: arg_lseg, arg_lseg => crate::lseg::lseg_le;
    fc_lseg_gt: arg_lseg, arg_lseg => crate::lseg::lseg_gt;
    fc_lseg_ge: arg_lseg, arg_lseg => crate::lseg::lseg_ge;
    fc_lseg_parallel: arg_lseg, arg_lseg => crate::lseg::lseg_parallel;
    fc_lseg_perp: arg_lseg, arg_lseg => crate::lseg::lseg_perp;
    fc_lseg_intersect: arg_lseg, arg_lseg => crate::lseg::lseg_intersect;
}

fc1!(fc_lseg_vertical, arg_lseg, ls, |_fcinfo: &mut Fcinfo, ls: LSEG| Ok(
    Datum::from_bool(crate::lseg::lseg_vertical(&ls))
));
fc1!(fc_lseg_horizontal, arg_lseg, ls, |_fcinfo: &mut Fcinfo, ls: LSEG| Ok(
    Datum::from_bool(crate::lseg::lseg_horizontal(&ls))
));

fc2!(fc_lseg_construct, arg_point, arg_point, a, b, |fcinfo: &mut Fcinfo, a, b| {
    ret_lseg(fcinfo, crate::lseg::lseg_construct(&a, &b))
});

fn fc_lseg_interpt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_lseg(fcinfo, 0) };
    // SAFETY: module contract.
    let b = unsafe { arg_lseg(fcinfo, 1) };
    let p = crate::lseg::lseg_interpt(&a, &b)?;
    ret_opt_point(fcinfo, p)
}

// line
bool2r! {
    fc_line_eq: arg_line, arg_line => crate::line::line_eq;
    fc_line_intersect: arg_line, arg_line => crate::line::line_intersect;
    fc_line_parallel: arg_line, arg_line => crate::line::line_parallel;
    fc_line_perp: arg_line, arg_line => crate::line::line_perp;
}

fc1!(fc_line_vertical, arg_line, l, |_fcinfo: &mut Fcinfo, l: LINE| Ok(
    Datum::from_bool(crate::line::line_vertical(&l))
));
fc1!(fc_line_horizontal, arg_line, l, |_fcinfo: &mut Fcinfo, l: LINE| Ok(
    Datum::from_bool(crate::line::line_horizontal(&l))
));

fc2!(fc_line_construct_pp, arg_point, arg_point, a, b, |fcinfo: &mut Fcinfo, a, b| {
    ret_line(fcinfo, crate::line::line_construct_pp(&a, &b)?)
});

fn fc_line_interpt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_line(fcinfo, 0) };
    // SAFETY: module contract.
    let b = unsafe { arg_line(fcinfo, 1) };
    let p = crate::line::line_interpt(&a, &b)?;
    ret_opt_point(fcinfo, p)
}

// circle
bool2! {
    fc_circle_same: arg_circle, arg_circle => crate::circle::circle_same;
}
bool2r! {
    fc_circle_overlap: arg_circle, arg_circle => crate::circle::circle_overlap;
    fc_circle_overleft: arg_circle, arg_circle => crate::circle::circle_overleft;
    fc_circle_left: arg_circle, arg_circle => crate::circle::circle_left;
    fc_circle_right: arg_circle, arg_circle => crate::circle::circle_right;
    fc_circle_overright: arg_circle, arg_circle => crate::circle::circle_overright;
    fc_circle_contained: arg_circle, arg_circle => crate::circle::circle_contained;
    fc_circle_contain: arg_circle, arg_circle => crate::circle::circle_contain;
    fc_circle_below: arg_circle, arg_circle => crate::circle::circle_below;
    fc_circle_above: arg_circle, arg_circle => crate::circle::circle_above;
    fc_circle_overbelow: arg_circle, arg_circle => crate::circle::circle_overbelow;
    fc_circle_overabove: arg_circle, arg_circle => crate::circle::circle_overabove;
    fc_circle_eq: arg_circle, arg_circle => crate::circle::circle_eq;
    fc_circle_ne: arg_circle, arg_circle => crate::circle::circle_ne;
    fc_circle_lt: arg_circle, arg_circle => crate::circle::circle_lt;
    fc_circle_gt: arg_circle, arg_circle => crate::circle::circle_gt;
    fc_circle_le: arg_circle, arg_circle => crate::circle::circle_le;
    fc_circle_ge: arg_circle, arg_circle => crate::circle::circle_ge;
    fc_circle_contain_pt: arg_circle, arg_point => crate::circle::circle_contain_pt;
    fc_pt_contained_circle: arg_point, arg_circle => crate::circle::pt_contained_circle;
}

macro_rules! circle_pt {
    ($($fc:ident => $core:path;)*) => {$(
        fc2!($fc, arg_circle, arg_point, c, p, |fcinfo: &mut Fcinfo, c, p| ret_circle(fcinfo, $core(&c, &p)?));
    )*};
}
circle_pt! {
    fc_circle_add_pt => crate::circle::circle_add_pt;
    fc_circle_sub_pt => crate::circle::circle_sub_pt;
    fc_circle_mul_pt => crate::circle::circle_mul_pt;
    fc_circle_div_pt => crate::circle::circle_div_pt;
}

// pub for proofs/geo-cmp (visibility-only; prove-target ruling 2026-07-28)
pub fn fc_cr_circle(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let center = unsafe { arg_point(fcinfo, 0) };
    let radius = fcinfo.arg_f64(1);
    ret_circle(fcinfo, crate::circle::cr_circle(&center, radius))
}

fn fc_circle_poly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let npts = fcinfo.arg_i32(0);
    // SAFETY: module contract.
    let circle = unsafe { arg_circle(fcinfo, 1) };
    crate::circle::circle_poly_checks(npts, &circle)?;
    let anglestep = ::adt_float::float8_div(2.0 * crate::M_PI, npts as f64)?;
    let v = io::poly_image(fcinfo.result_mcx(), npts as usize, |i| {
        crate::circle::circle_poly_vertex(&circle, anglestep, i as i32)
    })?;
    Ok(varlena_result(v))
}

// on_* / close_* / inter_*
bool2r! {
    fc_on_pl: arg_point, arg_line => crate::proximity::on_pl;
    fc_on_ps: arg_point, arg_lseg => crate::proximity::on_ps;
    fc_on_sl: arg_lseg, arg_line => crate::proximity::on_sl;
    fc_inter_sl: arg_lseg, arg_line => crate::proximity::inter_sl;
    fc_inter_lb: arg_line, arg_box => crate::proximity::inter_lb;
    fc_inter_sb: arg_lseg, arg_box => crate::proximity::inter_sb;
}
bool2! {
    fc_on_pb: arg_point, arg_box => crate::proximity::on_pb;
    fc_on_sb: arg_lseg, arg_box => crate::proximity::on_sb;
}

macro_rules! close2 {
    ($($fc:ident: $a0:ident, $a1:ident => $core:path;)*) => {$(
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let a = unsafe { $a0(fcinfo, 0) };
            // SAFETY: module contract.
            let b = unsafe { $a1(fcinfo, 1) };
            let p = $core(&a, &b)?;
            ret_opt_point(fcinfo, p)
        }
    )*};
}
close2! {
    fc_close_pl: arg_point, arg_line => crate::proximity::close_pl;
    fc_close_ps: arg_point, arg_lseg => crate::proximity::close_ps;
    fc_close_pb: arg_point, arg_box => crate::proximity::close_pb;
    fc_close_lseg: arg_lseg, arg_lseg => crate::proximity::close_lseg;
    fc_close_ls: arg_line, arg_lseg => crate::proximity::close_ls;
    fc_close_sb: arg_lseg, arg_box => crate::proximity::close_sb;
}

// path
macro_rules! path_cmp {
    ($($fc:ident => $op:tt;)*) => {$(
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let a = unsafe { arg_path(fcinfo, 0) }?;
            // SAFETY: module contract.
            let b = unsafe { arg_path(fcinfo, 1) }?;
            Ok(Datum::from_bool(a.n() $op b.n()))
        }
    )*};
}
path_cmp! {
    fc_path_n_lt => <;
    fc_path_n_gt => >;
    fc_path_n_eq => ==;
    fc_path_n_le => <=;
    fc_path_n_ge => >=;
}

fn fc_path_inter(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_path(fcinfo, 0) }?;
    // SAFETY: module contract.
    let b = unsafe { arg_path(fcinfo, 1) }?;
    Ok(Datum::from_bool(crate::path::path_inter(&a, &b)?))
}

fn fc_path_isclosed(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    Ok(Datum::from_bool(crate::path::path_isclosed(&p)))
}

fn fc_path_isopen(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    Ok(Datum::from_bool(crate::path::path_isopen(&p)))
}

fn fc_path_npoints(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    Ok(Datum::from_i32(crate::path::path_npoints(&p)))
}

fn fc_path_close(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    let v = io::path_image(fcinfo.result_mcx(), true, p.n(), |i| Ok(p.pt(i)))?;
    Ok(varlena_result(v))
}

fn fc_path_open(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    let v = io::path_image(fcinfo.result_mcx(), false, p.n(), |i| Ok(p.pt(i)))?;
    Ok(varlena_result(v))
}

fn fc_path_area(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    match crate::path::path_area(&p)? {
        Some(v) => Ok(Datum::from_f64(v)),
        None => Ok(fcinfo.return_null()),
    }
}

fn fc_path_length(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    Ok(Datum::from_f64(crate::path::path_length(&p)?))
}

fn fc_path_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_path(fcinfo, 0) }?;
    // SAFETY: module contract.
    let b = unsafe { arg_path(fcinfo, 1) }?;
    match crate::path::path_distance(&a, &b)? {
        Some(v) => Ok(Datum::from_f64(v)),
        None => Ok(fcinfo.return_null()),
    }
}

fn fc_path_add(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_path(fcinfo, 0) }?;
    // SAFETY: module contract.
    let b = unsafe { arg_path(fcinfo, 1) }?;
    if a.closed || b.closed {
        return Ok(fcinfo.return_null());
    }
    let total = a.n() + b.n();
    crate::path::path_add_checks(total)?;
    let v = io::path_image(fcinfo.result_mcx(), false, total, |i| {
        Ok(if i < a.n() { a.pt(i) } else { b.pt(i - a.n()) })
    })?;
    Ok(varlena_result(v))
}

macro_rules! path_pt {
    ($($fc:ident => $core:path;)*) => {$(
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let p = unsafe { arg_path(fcinfo, 0) }?;
            // SAFETY: module contract.
            let pt = unsafe { arg_point(fcinfo, 1) };
            let v = io::path_image(fcinfo.result_mcx(), p.closed, p.n(), |i| $core(&p.pt(i), &pt))?;
            Ok(varlena_result(v))
        }
    )*};
}
path_pt! {
    fc_path_add_pt => crate::point::point_add_point;
    fc_path_sub_pt => crate::point::point_sub_point;
    fc_path_mul_pt => crate::point::point_mul_point;
    fc_path_div_pt => crate::point::point_div_point;
}

fn fc_path_poly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_path(fcinfo, 0) }?;
    if !p.closed {
        return Err(crate::path::open_path_to_polygon_error());
    }
    let v = io::poly_image(fcinfo.result_mcx(), p.n(), |i| Ok(p.pt(i)))?;
    Ok(varlena_result(v))
}

fn fc_poly_path(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    let v = io::path_image(fcinfo.result_mcx(), true, p.n(), |i| Ok(p.pt(i)))?;
    Ok(varlena_result(v))
}

// polygon
macro_rules! poly2 {
    ($($fc:ident => $core:path;)*) => {$(
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let a = unsafe { arg_poly(fcinfo, 0) }?;
            // SAFETY: module contract.
            let b = unsafe { arg_poly(fcinfo, 1) }?;
            Ok(Datum::from_bool($core(&a, &b)))
        }
    )*};
}
poly2! {
    fc_poly_left => crate::poly::poly_left;
    fc_poly_overleft => crate::poly::poly_overleft;
    fc_poly_right => crate::poly::poly_right;
    fc_poly_overright => crate::poly::poly_overright;
    fc_poly_below => crate::poly::poly_below;
    fc_poly_overbelow => crate::poly::poly_overbelow;
    fc_poly_above => crate::poly::poly_above;
    fc_poly_overabove => crate::poly::poly_overabove;
    fc_poly_same => crate::poly::poly_same;
}

macro_rules! poly2r {
    ($($fc:ident => $core:path;)*) => {$(
        fn $fc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: module contract.
            let a = unsafe { arg_poly(fcinfo, 0) }?;
            // SAFETY: module contract.
            let b = unsafe { arg_poly(fcinfo, 1) }?;
            Ok(Datum::from_bool($core(&a, &b)?))
        }
    )*};
}
poly2r! {
    fc_poly_overlap => crate::poly::poly_overlap;
    fc_poly_contain => crate::poly::poly_contain;
    fc_poly_contained => crate::poly::poly_contained;
}

fn fc_poly_contain_pt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let poly = unsafe { arg_poly(fcinfo, 0) }?;
    // SAFETY: module contract.
    let p = unsafe { arg_point(fcinfo, 1) };
    Ok(Datum::from_bool(crate::poly::poly_contain_pt(&poly, &p)?))
}

fn fc_pt_contained_poly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_point(fcinfo, 0) };
    // SAFETY: module contract.
    let poly = unsafe { arg_poly(fcinfo, 1) }?;
    Ok(Datum::from_bool(crate::poly::pt_contained_poly(&p, &poly)?))
}

fn fc_poly_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let a = unsafe { arg_poly(fcinfo, 0) }?;
    // SAFETY: module contract.
    let b = unsafe { arg_poly(fcinfo, 1) }?;
    match crate::poly::poly_distance(&a, &b)? {
        Some(v) => Ok(Datum::from_f64(v)),
        None => Ok(fcinfo.return_null()),
    }
}

fn fc_poly_npoints(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    Ok(Datum::from_i32(crate::poly::poly_npoints(&p)))
}

fn fc_poly_center(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    let c = crate::poly::poly_center(&p)?;
    ret_point(fcinfo, c)
}

fn fc_poly_box(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    ret_box(fcinfo, crate::poly::poly_box(&p))
}

fn fc_box_poly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let b = unsafe { arg_box(fcinfo, 0) };
    let pts = [
        Point { x: b.low.x, y: b.low.y },
        Point { x: b.low.x, y: b.high.y },
        Point { x: b.high.x, y: b.high.y },
        Point { x: b.high.x, y: b.low.y },
    ];
    let v = io::poly_image(fcinfo.result_mcx(), 4, |i| Ok(pts[i]))?;
    Ok(varlena_result(v))
}

fn fc_poly_circle(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    ret_circle(fcinfo, crate::poly::poly_circle(&p)?)
}

fn fc_on_ppath(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 0) };
    // SAFETY: module contract.
    let path = unsafe { arg_path(fcinfo, 1) }?;
    Ok(Datum::from_bool(crate::proximity::on_ppath(&pt, &path)?))
}

fn fc_dist_ppath(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 0) };
    // SAFETY: module contract.
    let path = unsafe { arg_path(fcinfo, 1) }?;
    Ok(Datum::from_f64(crate::proximity::dist_ppath(&pt, &path)?))
}

fn fc_dist_pathp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let path = unsafe { arg_path(fcinfo, 0) }?;
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 1) };
    Ok(Datum::from_f64(crate::proximity::dist_pathp(&path, &pt)?))
}

fn fc_dist_cpoly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let c = unsafe { arg_circle(fcinfo, 0) };
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 1) }?;
    Ok(Datum::from_f64(crate::proximity::dist_cpoly(&c, &p)?))
}

fn fc_dist_polyc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    // SAFETY: module contract.
    let c = unsafe { arg_circle(fcinfo, 1) };
    Ok(Datum::from_f64(crate::proximity::dist_polyc(&p, &c)?))
}

fn fc_dist_ppoly(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 0) };
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 1) }?;
    Ok(Datum::from_f64(crate::proximity::dist_ppoly(&pt, &p)?))
}

fn fc_dist_polyp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: module contract.
    let p = unsafe { arg_poly(fcinfo, 0) }?;
    // SAFETY: module contract.
    let pt = unsafe { arg_point(fcinfo, 1) };
    Ok(Datum::from_f64(crate::proximity::dist_polyp(&p, &pt)?))
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
    b(119, "lseg_in", 1, fc_lseg_in),
    b(120, "lseg_out", 1, fc_lseg_out),
    b(121, "path_in", 1, fc_path_in),
    b(122, "path_out", 1, fc_path_out),
    b(123, "box_in", 1, fc_box_in),
    b(124, "box_out", 1, fc_box_out),
    b(347, "poly_in", 1, fc_poly_in),
    b(348, "poly_out", 1, fc_poly_out),
    b(1450, "circle_in", 1, fc_circle_in),
    b(1451, "circle_out", 1, fc_circle_out),
    b(1490, "line_in", 1, fc_line_in),
    b(1491, "line_out", 1, fc_line_out),
    b(2428, "point_recv", 1, fc_point_recv),
    b(2429, "point_send", 1, fc_point_send),
    b(2480, "lseg_recv", 1, fc_lseg_recv),
    b(2481, "lseg_send", 1, fc_lseg_send),
    b(2482, "path_recv", 1, fc_path_recv),
    b(2483, "path_send", 1, fc_path_send),
    b(2484, "box_recv", 1, fc_box_recv),
    b(2485, "box_send", 1, fc_box_send),
    b(2486, "poly_recv", 1, fc_poly_recv),
    b(2487, "poly_send", 1, fc_poly_send),
    b(2488, "line_recv", 1, fc_line_recv),
    b(2489, "line_send", 1, fc_line_send),
    b(2490, "circle_recv", 1, fc_circle_recv),
    b(2491, "circle_send", 1, fc_circle_send),
    b(131, "point_above", 2, fc_point_above),
    b(132, "point_left", 2, fc_point_left),
    b(133, "point_right", 2, fc_point_right),
    b(134, "point_below", 2, fc_point_below),
    b(135, "point_eq", 2, fc_point_eq),
    b(988, "point_ne", 2, fc_point_ne),
    b(989, "point_vert", 2, fc_point_vert),
    b(1406, "point_vert", 2, fc_point_vert),
    b(990, "point_horiz", 2, fc_point_horiz),
    b(1407, "point_horiz", 2, fc_point_horiz),
    b(991, "point_distance", 2, fc_point_distance),
    b(992, "point_slope", 2, fc_point_slope),
    b(1440, "construct_point", 2, fc_construct_point),
    b(1441, "point_add", 2, fc_point_add),
    b(1442, "point_sub", 2, fc_point_sub),
    b(1443, "point_mul", 2, fc_point_mul),
    b(1444, "point_div", 2, fc_point_div),
    b(4091, "point_box", 1, fc_point_box),
    b(115, "box_above_eq", 2, fc_box_above_eq),
    b(116, "box_below_eq", 2, fc_box_below_eq),
    b(125, "box_overlap", 2, fc_box_overlap),
    b(126, "box_ge", 2, fc_box_ge),
    b(127, "box_gt", 2, fc_box_gt),
    b(128, "box_eq", 2, fc_box_eq),
    b(129, "box_lt", 2, fc_box_lt),
    b(130, "box_le", 2, fc_box_le),
    b(186, "box_same", 2, fc_box_same),
    b(187, "box_contain", 2, fc_box_contain),
    b(188, "box_left", 2, fc_box_left),
    b(189, "box_overleft", 2, fc_box_overleft),
    b(190, "box_overright", 2, fc_box_overright),
    b(191, "box_right", 2, fc_box_right),
    b(192, "box_contained", 2, fc_box_contained),
    b(193, "box_contain_pt", 2, fc_box_contain_pt),
    b(2562, "box_below", 2, fc_box_below),
    b(2563, "box_overbelow", 2, fc_box_overbelow),
    b(2564, "box_overabove", 2, fc_box_overabove),
    b(2565, "box_above", 2, fc_box_above),
    b(975, "box_area", 1, fc_box_area),
    b(976, "box_width", 1, fc_box_width),
    b(977, "box_height", 1, fc_box_height),
    b(978, "box_distance", 2, fc_box_distance),
    b(980, "box_intersect", 2, fc_box_intersect),
    b(981, "box_diagonal", 1, fc_box_diagonal),
    b(1541, "box_diagonal", 1, fc_box_diagonal),
    b(138, "box_center", 1, fc_box_center),
    b(1534, "box_center", 1, fc_box_center),
    b(1542, "box_center", 1, fc_box_center),
    b(1421, "points_box", 2, fc_points_box),
    b(1422, "box_add", 2, fc_box_add),
    b(1423, "box_sub", 2, fc_box_sub),
    b(1424, "box_mul", 2, fc_box_mul),
    b(1425, "box_div", 2, fc_box_div),
    b(1479, "box_circle", 1, fc_box_circle),
    b(1480, "circle_box", 1, fc_circle_box),
    b(4067, "boxes_bound_box", 2, fc_boxes_bound_box),
    b(225, "lseg_center", 1, fc_lseg_center),
    b(1532, "lseg_center", 1, fc_lseg_center),
    b(361, "lseg_distance", 2, fc_lseg_distance),
    b(362, "lseg_interpt", 2, fc_lseg_interpt),
    b(993, "lseg_construct", 2, fc_lseg_construct),
    b(994, "lseg_intersect", 2, fc_lseg_intersect),
    b(995, "lseg_parallel", 2, fc_lseg_parallel),
    b(1408, "lseg_parallel", 2, fc_lseg_parallel),
    b(996, "lseg_perp", 2, fc_lseg_perp),
    b(1409, "lseg_perp", 2, fc_lseg_perp),
    b(997, "lseg_vertical", 1, fc_lseg_vertical),
    b(1410, "lseg_vertical", 1, fc_lseg_vertical),
    b(998, "lseg_horizontal", 1, fc_lseg_horizontal),
    b(1411, "lseg_horizontal", 1, fc_lseg_horizontal),
    b(999, "lseg_eq", 2, fc_lseg_eq),
    b(1482, "lseg_ne", 2, fc_lseg_ne),
    b(1483, "lseg_lt", 2, fc_lseg_lt),
    b(1484, "lseg_le", 2, fc_lseg_le),
    b(1485, "lseg_gt", 2, fc_lseg_gt),
    b(1486, "lseg_ge", 2, fc_lseg_ge),
    b(1487, "lseg_length", 1, fc_lseg_length),
    b(1530, "lseg_length", 1, fc_lseg_length),
    b(239, "line_distance", 2, fc_line_distance),
    b(1412, "line_parallel", 2, fc_line_parallel),
    b(1496, "line_parallel", 2, fc_line_parallel),
    b(1413, "line_perp", 2, fc_line_perp),
    b(1497, "line_perp", 2, fc_line_perp),
    b(1414, "line_vertical", 1, fc_line_vertical),
    b(1498, "line_vertical", 1, fc_line_vertical),
    b(1415, "line_horizontal", 1, fc_line_horizontal),
    b(1499, "line_horizontal", 1, fc_line_horizontal),
    b(1492, "line_eq", 2, fc_line_eq),
    b(1493, "line_construct_pp", 2, fc_line_construct_pp),
    b(1494, "line_interpt", 2, fc_line_interpt),
    b(1495, "line_intersect", 2, fc_line_intersect),
    b(1146, "circle_add_pt", 2, fc_circle_add_pt),
    b(1147, "circle_sub_pt", 2, fc_circle_sub_pt),
    b(1148, "circle_mul_pt", 2, fc_circle_mul_pt),
    b(1149, "circle_div_pt", 2, fc_circle_div_pt),
    b(1416, "circle_center", 1, fc_circle_center),
    b(1472, "circle_center", 1, fc_circle_center),
    b(1543, "circle_center", 1, fc_circle_center),
    b(1452, "circle_same", 2, fc_circle_same),
    b(1453, "circle_contain", 2, fc_circle_contain),
    b(1454, "circle_left", 2, fc_circle_left),
    b(1455, "circle_overleft", 2, fc_circle_overleft),
    b(1456, "circle_overright", 2, fc_circle_overright),
    b(1457, "circle_right", 2, fc_circle_right),
    b(1458, "circle_contained", 2, fc_circle_contained),
    b(1459, "circle_overlap", 2, fc_circle_overlap),
    b(1460, "circle_below", 2, fc_circle_below),
    b(1461, "circle_above", 2, fc_circle_above),
    b(2587, "circle_overbelow", 2, fc_circle_overbelow),
    b(2588, "circle_overabove", 2, fc_circle_overabove),
    b(1462, "circle_eq", 2, fc_circle_eq),
    b(1463, "circle_ne", 2, fc_circle_ne),
    b(1464, "circle_lt", 2, fc_circle_lt),
    b(1465, "circle_gt", 2, fc_circle_gt),
    b(1466, "circle_le", 2, fc_circle_le),
    b(1467, "circle_ge", 2, fc_circle_ge),
    b(1468, "circle_area", 1, fc_circle_area),
    b(1469, "circle_diameter", 1, fc_circle_diameter),
    b(1470, "circle_radius", 1, fc_circle_radius),
    b(1471, "circle_distance", 2, fc_circle_distance),
    b(1473, "cr_circle", 2, fc_cr_circle),
    b(1475, "circle_poly", 2, fc_circle_poly),
    b(1476, "dist_pc", 2, fc_dist_pc),
    b(3290, "dist_cpoint", 2, fc_dist_cpoint),
    b(1477, "circle_contain_pt", 2, fc_circle_contain_pt),
    b(1478, "pt_contained_circle", 2, fc_pt_contained_circle),
    b(136, "on_pb", 2, fc_on_pb),
    b(137, "on_ppath", 2, fc_on_ppath),
    b(369, "on_ps", 2, fc_on_ps),
    b(372, "on_sb", 2, fc_on_sb),
    b(959, "on_pl", 2, fc_on_pl),
    b(960, "on_sl", 2, fc_on_sl),
    b(277, "inter_sl", 2, fc_inter_sl),
    b(278, "inter_lb", 2, fc_inter_lb),
    b(373, "inter_sb", 2, fc_inter_sb),
    b(366, "close_ps", 2, fc_close_ps),
    b(367, "close_pb", 2, fc_close_pb),
    b(368, "close_sb", 2, fc_close_sb),
    b(961, "close_pl", 2, fc_close_pl),
    b(1488, "close_ls", 2, fc_close_ls),
    b(1489, "close_lseg", 2, fc_close_lseg),
    b(357, "dist_bp", 2, fc_dist_bp),
    b(363, "dist_ps", 2, fc_dist_ps),
    b(364, "dist_pb", 2, fc_dist_pb),
    b(365, "dist_sb", 2, fc_dist_sb),
    b(380, "dist_sp", 2, fc_dist_sp),
    b(381, "dist_bs", 2, fc_dist_bs),
    b(702, "dist_lp", 2, fc_dist_lp),
    b(704, "dist_ls", 2, fc_dist_ls),
    b(725, "dist_pl", 2, fc_dist_pl),
    b(727, "dist_sl", 2, fc_dist_sl),
    b(371, "dist_ppath", 2, fc_dist_ppath),
    b(421, "dist_pathp", 2, fc_dist_pathp),
    b(728, "dist_cpoly", 2, fc_dist_cpoly),
    b(785, "dist_polyc", 2, fc_dist_polyc),
    b(3275, "dist_ppoly", 2, fc_dist_ppoly),
    b(3292, "dist_polyp", 2, fc_dist_polyp),
    b(973, "path_inter", 2, fc_path_inter),
    b(979, "path_area", 1, fc_path_area),
    b(982, "path_n_lt", 2, fc_path_n_lt),
    b(983, "path_n_gt", 2, fc_path_n_gt),
    b(984, "path_n_eq", 2, fc_path_n_eq),
    b(985, "path_n_le", 2, fc_path_n_le),
    b(986, "path_n_ge", 2, fc_path_n_ge),
    b(987, "path_length", 1, fc_path_length),
    b(1531, "path_length", 1, fc_path_length),
    b(370, "path_distance", 2, fc_path_distance),
    b(1430, "path_isclosed", 1, fc_path_isclosed),
    b(1431, "path_isopen", 1, fc_path_isopen),
    b(1432, "path_npoints", 1, fc_path_npoints),
    b(1545, "path_npoints", 1, fc_path_npoints),
    b(1433, "path_close", 1, fc_path_close),
    b(1434, "path_open", 1, fc_path_open),
    b(1435, "path_add", 2, fc_path_add),
    b(1436, "path_add_pt", 2, fc_path_add_pt),
    b(1437, "path_sub_pt", 2, fc_path_sub_pt),
    b(1438, "path_mul_pt", 2, fc_path_mul_pt),
    b(1439, "path_div_pt", 2, fc_path_div_pt),
    b(1449, "path_poly", 1, fc_path_poly),
    b(1447, "poly_path", 1, fc_poly_path),
    b(339, "poly_same", 2, fc_poly_same),
    b(340, "poly_contain", 2, fc_poly_contain),
    b(341, "poly_left", 2, fc_poly_left),
    b(342, "poly_overleft", 2, fc_poly_overleft),
    b(343, "poly_overright", 2, fc_poly_overright),
    b(344, "poly_right", 2, fc_poly_right),
    b(345, "poly_contained", 2, fc_poly_contained),
    b(346, "poly_overlap", 2, fc_poly_overlap),
    b(2566, "poly_below", 2, fc_poly_below),
    b(2567, "poly_overbelow", 2, fc_poly_overbelow),
    b(2568, "poly_overabove", 2, fc_poly_overabove),
    b(2569, "poly_above", 2, fc_poly_above),
    b(1428, "poly_contain_pt", 2, fc_poly_contain_pt),
    b(1429, "pt_contained_poly", 2, fc_pt_contained_poly),
    b(729, "poly_distance", 2, fc_poly_distance),
    b(1445, "poly_npoints", 1, fc_poly_npoints),
    b(1556, "poly_npoints", 1, fc_poly_npoints),
    b(227, "poly_center", 1, fc_poly_center),
    b(1540, "poly_center", 1, fc_poly_center),
    b(1446, "poly_box", 1, fc_poly_box),
    b(1448, "box_poly", 1, fc_box_poly),
    b(1474, "poly_circle", 1, fc_poly_circle),
];
