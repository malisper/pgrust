//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `geo_ops.c` functions over the fixed-size pass-by-reference geometric types
//! (`point`, `box`, `lseg`, `line`, `circle`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_geo_ops_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (the `{ oid => 'N',` line that opens each proc block).
//!
//! # The by-reference geometric-type convention
//!
//! `point`, `box`, `lseg`, `line`, and `circle` are pass-by-reference (but NOT
//! varlena) fixed-size types. Unlike `numeric`/`text`, their `ByRef` byte image
//! is the raw native-byte-order C struct image with NO varlena header:
//!   - `point`  = `{ float8 x, y }` (16 bytes)
//!   - `box`    = `{ Point high, low }` (32 bytes)
//!   - `lseg`   = `{ Point p[2] }` (32 bytes)
//!   - `line`   = `{ float8 A, B, C }` (24 bytes)
//!   - `circle` = `{ Point center; float8 radius }` (24 bytes)
//!
//! `Point`/`BOX`/`CIRCLE` carry `from_datum_bytes`/`to_datum_bytes` codecs
//! (matching the C `DatumGetPointP`/`PointPGetDatum` macros); `lseg`/`line` are
//! serialized here field-for-field in the same native order. A by-ref arg
//! arrives as `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(image))` (the
//! bridge carries any `ByRef` bytes on the `Varlena` lane verbatim) and a by-ref
//! result is set via `fcinfo.set_ref_result(RefPayload::Varlena(image))`. The
//! bare by-value word is the null/dummy word, exactly as the canonical->ABI
//! bridge `datum_to_ref_arg`/`ref_out_to_datum` arranges.
//!
//! `cstring` I/O (`*_in`/`*_out`) crosses on the `RefPayload::Cstring` lane.

use datum::Datum;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};

use crate::{Path, Polygon};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// The raw native by-ref struct image of arg `i` (carried on the `Varlena`
/// lane by the bridge — no varlena header for these fixed-size types).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("geo fn: by-ref geometric arg missing from by-ref lane")
}

/// `PG_GETARG_POINT_P(i)`.
#[inline]
fn arg_point(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Point {
    Point::from_datum_bytes(arg_bytes(fcinfo, i))
}

/// `PG_GETARG_BOX_P(i)`.
#[inline]
fn arg_box(fcinfo: &FunctionCallInfoBaseData, i: usize) -> BOX {
    BOX::from_datum_bytes(arg_bytes(fcinfo, i))
}

/// `PG_GETARG_CIRCLE_P(i)`.
#[inline]
fn arg_circle(fcinfo: &FunctionCallInfoBaseData, i: usize) -> CIRCLE {
    CIRCLE::from_datum_bytes(arg_bytes(fcinfo, i))
}

/// `PG_GETARG_LSEG_P(i)`: `{ Point p[2] }`, native order (32 bytes).
#[inline]
fn arg_lseg(fcinfo: &FunctionCallInfoBaseData, i: usize) -> LSEG {
    let b = arg_bytes(fcinfo, i);
    LSEG {
        p: [
            Point::from_datum_bytes(&b[0..16]),
            Point::from_datum_bytes(&b[16..32]),
        ],
    }
}

/// `PG_GETARG_LINE_P(i)`: `{ float8 A, B, C }`, native order (24 bytes).
#[inline]
fn arg_line(fcinfo: &FunctionCallInfoBaseData, i: usize) -> LINE {
    let b = arg_bytes(fcinfo, i);
    let f = |o: usize| {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[o..o + 8]);
        f64::from_ne_bytes(a)
    };
    LINE {
        A: f(0),
        B: f(8),
        C: f(16),
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("geo fn: cstring arg missing from by-ref lane")
}

/// `LSEG`'s native by-ref image: the two endpoints' images, back-to-back.
#[inline]
fn lseg_bytes(ls: &LSEG) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(&ls.p[0].to_datum_bytes());
    out.extend_from_slice(&ls.p[1].to_datum_bytes());
    out
}

/// `LINE`'s native by-ref image: `A`, `B`, `C` as native-order f64.
#[inline]
fn line_bytes(l: &LINE) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&l.A.to_ne_bytes());
    out.extend_from_slice(&l.B.to_ne_bytes());
    out.extend_from_slice(&l.C.to_ne_bytes());
    out
}

/// Set a by-ref geometric result on the by-ref lane, returning the dummy word.
#[inline]
fn ret_ref(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `*_send` result (a `bytea` of wire bytes) on the by-ref lane. C's send
/// functions return a header-ful `bytea*` (pq_endtypsend); the send seam
/// (oid_send_function_call_seam) recovers the wire payload by stripping VARHDRSZ,
/// so stamp the 4-byte SET_VARSIZE header here. Without it the strip eats the
/// first 4 wire bytes ("insufficient data left in message").
#[inline]
fn ret_send(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    const VARHDRSZ: usize = 4;
    let mut image = Vec::with_capacity(payload.len() + VARHDRSZ);
    image.extend_from_slice(&datum::varlena::set_varsize_4b(payload.len() + VARHDRSZ));
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

#[inline]
fn ret_point(fcinfo: &mut FunctionCallInfoBaseData, p: Point) -> Datum {
    ret_ref(fcinfo, p.to_datum_bytes().to_vec())
}

#[inline]
fn ret_box(fcinfo: &mut FunctionCallInfoBaseData, b: BOX) -> Datum {
    ret_ref(fcinfo, b.to_datum_bytes().to_vec())
}

#[inline]
fn ret_circle(fcinfo: &mut FunctionCallInfoBaseData, c: CIRCLE) -> Datum {
    ret_ref(fcinfo, c.to_datum_bytes().to_vec())
}

#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// `PG_RETURN_FLOAT8(v)`: float8 is by-value (the word holds its bit pattern).
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}

/// `PG_RETURN_NULL()`: mark the result null and return the dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = true;
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// I/O adapters (cstring <-> by-ref geometric).
// ---------------------------------------------------------------------------

fn fc_point_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: `point_in` forwards `fcinfo->context` for soft `pg_input_is_valid`.
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let p = crate::io::point_in(&s, escontext)?;
    Ok(ret_point(fcinfo, p))
}
fn fc_point_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = arg_point(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::point_out(&p)))
}
fn fc_box_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let b = crate::io::box_in(&s, escontext)?;
    Ok(ret_box(fcinfo, b))
}
fn fc_box_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::box_out(&b)))
}
fn fc_lseg_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let ls = crate::io::lseg_in(&s, escontext)?;
    Ok(ret_ref(fcinfo, lseg_bytes(&ls)))
}
fn fc_lseg_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::lseg_out(&ls)))
}
fn fc_line_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let l = crate::io::line_in(&s, escontext)?;
    Ok(ret_ref(fcinfo, line_bytes(&l)))
}
fn fc_line_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let l = arg_line(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::line_out(&l)))
}
fn fc_circle_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let c = crate::io::circle_in(&s, escontext)?;
    Ok(ret_circle(fcinfo, c))
}
fn fc_circle_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::circle_out(&c)))
}

// ---------------------------------------------------------------------------
// Comparison/predicate macros: (T, T) -> bool, where the core is pure or
// `PgResult<bool>`. `_p` = pure `fn(&T,&T)->bool`, `_r` = `fn(&T,&T)->PgResult<bool>`.
// ---------------------------------------------------------------------------

macro_rules! fc_pred_point {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}
macro_rules! fc_pred_box {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}
macro_rules! fc_pred_circle {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}
macro_rules! fc_pred_lseg {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}

// point predicates
fc_pred_point!(fc_point_eq, crate::point::point_eq, pure);
fc_pred_point!(fc_point_ne, crate::point::point_ne, pure);
fc_pred_point!(fc_point_left, crate::point::point_left, pure);
fc_pred_point!(fc_point_right, crate::point::point_right, pure);
fc_pred_point!(fc_point_above, crate::point::point_above, pure);
fc_pred_point!(fc_point_below, crate::point::point_below, pure);

// box predicates
fc_pred_box!(fc_box_same, crate::boxes::box_same, pure);
fc_pred_box!(fc_box_overlap, crate::boxes::box_overlap, pure);
fc_pred_box!(fc_box_left, crate::boxes::box_left, pure);
fc_pred_box!(fc_box_right, crate::boxes::box_right, pure);
fc_pred_box!(fc_box_below, crate::boxes::box_below, pure);
fc_pred_box!(fc_box_above, crate::boxes::box_above, pure);
fc_pred_box!(fc_box_contain, crate::boxes::box_contain, pure);
fc_pred_box!(fc_box_contained, crate::boxes::box_contained, pure);
fc_pred_box!(fc_box_below_eq, crate::boxes::box_below_eq, pure);
fc_pred_box!(fc_box_above_eq, crate::boxes::box_above_eq, pure);
fc_pred_box!(fc_box_lt, crate::boxes::box_lt, res);
fc_pred_box!(fc_box_le, crate::boxes::box_le, res);
fc_pred_box!(fc_box_gt, crate::boxes::box_gt, res);
fc_pred_box!(fc_box_ge, crate::boxes::box_ge, res);
fc_pred_box!(fc_box_eq, crate::boxes::box_eq, res);

// circle predicates
fc_pred_circle!(fc_circle_same, crate::circle::circle_same, pure);
fc_pred_circle!(fc_circle_overlap, crate::circle::circle_overlap, res);
fc_pred_circle!(fc_circle_left, crate::circle::circle_left, res);
fc_pred_circle!(fc_circle_right, crate::circle::circle_right, res);
fc_pred_circle!(fc_circle_below, crate::circle::circle_below, res);
fc_pred_circle!(fc_circle_above, crate::circle::circle_above, res);
fc_pred_circle!(fc_circle_contain, crate::circle::circle_contain, res);
fc_pred_circle!(fc_circle_contained, crate::circle::circle_contained, res);
fc_pred_circle!(fc_circle_eq, crate::circle::circle_eq, res);
fc_pred_circle!(fc_circle_ne, crate::circle::circle_ne, res);
fc_pred_circle!(fc_circle_lt, crate::circle::circle_lt, res);
fc_pred_circle!(fc_circle_le, crate::circle::circle_le, res);
fc_pred_circle!(fc_circle_gt, crate::circle::circle_gt, res);
fc_pred_circle!(fc_circle_ge, crate::circle::circle_ge, res);

// lseg predicates
fc_pred_lseg!(fc_lseg_eq, crate::lseg::lseg_eq, pure);
fc_pred_lseg!(fc_lseg_ne, crate::lseg::lseg_ne, pure);
fc_pred_lseg!(fc_lseg_lt, crate::lseg::lseg_lt, res);
fc_pred_lseg!(fc_lseg_le, crate::lseg::lseg_le, res);
fc_pred_lseg!(fc_lseg_gt, crate::lseg::lseg_gt, res);
fc_pred_lseg!(fc_lseg_ge, crate::lseg::lseg_ge, res);
fc_pred_lseg!(fc_lseg_parallel, crate::lseg::lseg_parallel, res);
fc_pred_lseg!(fc_lseg_perp, crate::lseg::lseg_perp, res);

// intersection predicates (mixed geometric types)
/// `inter_sl(lseg, line) -> bool` (geo_ops.c).
fn fc_inter_sl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    let l = arg_line(fcinfo, 1);
    Ok(ret_bool(crate::proximity::inter_sl(&ls, &l)?))
}
/// `inter_lb(line, box) -> bool` (geo_ops.c).
fn fc_inter_lb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let l = arg_line(fcinfo, 0);
    let b = arg_box(fcinfo, 1);
    Ok(ret_bool(crate::proximity::inter_lb(&l, &b)?))
}
/// `inter_sb(lseg, box) -> bool` (geo_ops.c).
fn fc_inter_sb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    let b = arg_box(fcinfo, 1);
    Ok(ret_bool(crate::proximity::inter_sb(&ls, &b)?))
}

/// `boxes_bound_box(box, box) -> box` (geo_ops.c) — the bounding box of two boxes.
fn fc_boxes_bound_box(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b1 = arg_box(fcinfo, 0);
    let b2 = arg_box(fcinfo, 1);
    Ok(ret_box(fcinfo, crate::boxes::boxes_bound_box(&b1, &b2)))
}

// lseg unary predicates
fn fc_lseg_vertical(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    Ok(ret_bool(crate::lseg::lseg_vertical(&ls)))
}
fn fc_lseg_horizontal(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    Ok(ret_bool(crate::lseg::lseg_horizontal(&ls)))
}

// line predicates
fn fc_line_parallel(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::line::line_parallel(&a, &b)?))
}
fn fc_line_perp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::line::line_perp(&a, &b)?))
}
fn fc_line_vertical(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let l = arg_line(fcinfo, 0);
    Ok(ret_bool(crate::line::line_vertical(&l)))
}
fn fc_line_horizontal(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let l = arg_line(fcinfo, 0);
    Ok(ret_bool(crate::line::line_horizontal(&l)))
}

// circle/point predicate (mixed arg types).
fn fc_circle_contain_pt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    let p = arg_point(fcinfo, 1);
    Ok(ret_bool(crate::circle::circle_contain_pt(&c, &p)?))
}
fn fc_pt_contained_circle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = arg_point(fcinfo, 0);
    let c = arg_circle(fcinfo, 1);
    Ok(ret_bool(crate::circle::pt_contained_circle(&p, &c)?))
}

// ---------------------------------------------------------------------------
// Distance/measurement -> float8.
// ---------------------------------------------------------------------------

fn fc_point_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_f64(crate::point::point_distance(&a, &b)?))
}
fn fc_box_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
    Ok(ret_f64(crate::boxes::box_distance(&a, &b)?))
}
fn fc_circle_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
    Ok(ret_f64(crate::circle::circle_distance(&a, &b)?))
}
fn fc_lseg_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_f64(crate::proximity::lseg_distance(&a, &b)?))
}
fn fc_box_area(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    Ok(ret_f64(crate::boxes::box_area(&b)?))
}
fn fc_box_width(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    Ok(ret_f64(crate::boxes::box_width(&b)?))
}
fn fc_box_height(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    Ok(ret_f64(crate::boxes::box_height(&b)?))
}
fn fc_circle_area(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    Ok(ret_f64(crate::circle::circle_area(&c)?))
}
fn fc_circle_diameter(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    Ok(ret_f64(crate::circle::circle_diameter(&c)?))
}
fn fc_circle_radius(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    Ok(ret_f64(crate::circle::circle_radius(&c)))
}
fn fc_lseg_length(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    Ok(ret_f64(crate::lseg::lseg_length(&ls)?))
}

// ---------------------------------------------------------------------------
// "center" -> point.
// ---------------------------------------------------------------------------

fn fc_box_center(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    let p = crate::boxes::box_center(&b)?;
    Ok(ret_point(fcinfo, p))
}
fn fc_circle_center(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = arg_circle(fcinfo, 0);
    let p = crate::circle::circle_center(&c);
    Ok(ret_point(fcinfo, p))
}
fn fc_lseg_center(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = arg_lseg(fcinfo, 0);
    let p = crate::lseg::lseg_center(&ls)?;
    Ok(ret_point(fcinfo, p))
}

// ---------------------------------------------------------------------------
// Arithmetic -> point / box / circle.
// ---------------------------------------------------------------------------

macro_rules! fc_arith_point {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            let p = $core(&a, &b)?;
            Ok(ret_point(fcinfo, p))
        }
    };
}
fc_arith_point!(fc_point_add, crate::point::point_add);
fc_arith_point!(fc_point_sub, crate::point::point_sub);
fc_arith_point!(fc_point_mul, crate::point::point_mul);
fc_arith_point!(fc_point_div, crate::point::point_div);

macro_rules! fc_arith_box {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let b = arg_box(fcinfo, 0);
            let p = arg_point(fcinfo, 1);
            let r = $core(&b, &p)?;
            Ok(ret_box(fcinfo, r))
        }
    };
}
fc_arith_box!(fc_box_add, crate::boxes::box_add);
fc_arith_box!(fc_box_sub, crate::boxes::box_sub);
fc_arith_box!(fc_box_mul, crate::boxes::box_mul);
fc_arith_box!(fc_box_div, crate::boxes::box_div);

fn fc_box_intersect(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
    match crate::boxes::box_intersect(&a, &b) {
        Some(r) => Ok(ret_box(fcinfo, r)),
        None => Ok(ret_null(fcinfo)),
    }
}

macro_rules! fc_arith_circle {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let c = arg_circle(fcinfo, 0);
            let p = arg_point(fcinfo, 1);
            let r = $core(&c, &p)?;
            Ok(ret_circle(fcinfo, r))
        }
    };
}
fc_arith_circle!(fc_circle_add_pt, crate::circle::circle_add_pt);
fc_arith_circle!(fc_circle_sub_pt, crate::circle::circle_sub_pt);
fc_arith_circle!(fc_circle_mul_pt, crate::circle::circle_mul_pt);
fc_arith_circle!(fc_circle_div_pt, crate::circle::circle_div_pt);

// ---------------------------------------------------------------------------
// Cross-type / mixed-arg adapters (the second fan-out leg over the fixed-size
// `point`/`box`/`lseg`/`line`/`circle` types): the `dist_*` distance family,
// the `on_*` containment predicates, the `close_*` closest-point family, the
// line/lseg/circle constructors + conversions, and the duplicate-OID type
// coercions (`box_center`/`circle_center`/etc. each have two `pg_proc` rows).
//
// The `point`/`lseg`/`box`/`circle`/`path`/`polygon` cores live across all the
// per-shape modules; the `dist_*`/`on_*`/`close_*` cores live in `proximity`.
// Argument readers / result writers reuse the by-ref-lane helpers above; the
// `float8`/`int4` scalar args/results cross by-value on the `args` word lane.
// ---------------------------------------------------------------------------

/// `PG_GETARG_FLOAT8(i)`: a by-value float8 arg (its bit pattern in the word).
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.args[i].value.as_f64()
}

/// `PG_GETARG_INT32(i)`: a by-value int4 arg.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.args[i].value.as_i32()
}

/// `PG_RETURN_LSEG_P(ls)`: the lseg's native by-ref image on the by-ref lane.
#[inline]
fn ret_lseg(fcinfo: &mut FunctionCallInfoBaseData, ls: LSEG) -> Datum {
    ret_ref(fcinfo, lseg_bytes(&ls))
}

/// `PG_RETURN_LINE_P(l)`: the line's native by-ref image on the by-ref lane.
#[inline]
fn ret_line(fcinfo: &mut FunctionCallInfoBaseData, l: LINE) -> Datum {
    ret_ref(fcinfo, line_bytes(&l))
}

/// Set an `Option<Point>` result: `Some(p)` -> the point image, `None` ->
/// `PG_RETURN_NULL()` (the `close_*`/`*_interpt` "no intersection" case).
#[inline]
fn ret_opt_point(fcinfo: &mut FunctionCallInfoBaseData, p: Option<Point>) -> Datum {
    match p {
        Some(p) => ret_point(fcinfo, p),
        None => ret_null(fcinfo),
    }
}

// --- point: vert/horiz/slope + the float8 constructor + point->box coercion ---

fn fc_point_vert(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_bool(crate::point::point_vert(&a, &b)))
}
fn fc_point_horiz(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_bool(crate::point::point_horiz(&a, &b)))
}
fn fc_point_slope(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_f64(crate::point::point_slope(&a, &b)?))
}
fn fc_construct_point(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = crate::point::construct_point(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1));
    Ok(ret_point(fcinfo, p))
}
fn fc_point_box(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = crate::boxes::point_box(&arg_point(fcinfo, 0));
    Ok(ret_box(fcinfo, b))
}

// --- box: overleft/overright/overbelow/overabove + contain_pt + diagonal +
//     box<->circle conversions ---

fc_pred_box!(fc_box_overleft, crate::boxes::box_overleft, pure);
fc_pred_box!(fc_box_overright, crate::boxes::box_overright, pure);
fc_pred_box!(fc_box_overbelow, crate::boxes::box_overbelow, pure);
fc_pred_box!(fc_box_overabove, crate::boxes::box_overabove, pure);

fn fc_box_contain_pt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_box(fcinfo, 0);
    let p = arg_point(fcinfo, 1);
    Ok(ret_bool(crate::proximity::box_contain_pt(&b, &p)))
}
fn fc_box_diagonal(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let ls = crate::boxes::box_diagonal(&arg_box(fcinfo, 0));
    Ok(ret_lseg(fcinfo, ls))
}
fn fc_points_box(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_box(fcinfo, crate::boxes::points_box(&a, &b)))
}
fn fc_box_circle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = crate::boxes::box_circle(&arg_box(fcinfo, 0))?;
    Ok(ret_circle(fcinfo, c))
}
fn fc_circle_box(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = crate::boxes::circle_box(&arg_circle(fcinfo, 0))?;
    Ok(ret_box(fcinfo, b))
}

// --- lseg: construct / intersect / interpt ---

fn fc_lseg_construct(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_lseg(fcinfo, crate::lseg::lseg_construct(&a, &b)))
}
fn fc_lseg_intersect(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_bool(crate::lseg::lseg_intersect(&a, &b)?))
}
fn fc_lseg_interpt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::lseg::lseg_interpt(&a, &b)?))
}

// --- line: eq / construct_pp / interpt / intersect / distance + isparallel etc.
//     (the duplicate `line_*` OIDs 1412-1415 share the already-defined adapters) ---

fn fc_line_eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::line::line_eq(&a, &b)?))
}
fn fc_line_construct_pp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    Ok(ret_line(fcinfo, crate::line::line_construct_pp(&a, &b)?))
}
fn fc_line_interpt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::line::line_interpt(&a, &b)?))
}
fn fc_line_intersect(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::line::line_intersect(&a, &b)?))
}
fn fc_line_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_f64(crate::line::line_distance(&a, &b)?))
}

// --- circle: overleft/overright/overbelow/overabove + cr_circle + circle_poly ---

fc_pred_circle!(fc_circle_overleft, crate::circle::circle_overleft, res);
fc_pred_circle!(fc_circle_overright, crate::circle::circle_overright, res);
fc_pred_circle!(fc_circle_overbelow, crate::circle::circle_overbelow, res);
fc_pred_circle!(fc_circle_overabove, crate::circle::circle_overabove, res);

fn fc_cr_circle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let center = arg_point(fcinfo, 0);
    let radius = arg_f64(fcinfo, 1);
    Ok(ret_circle(fcinfo, crate::circle::cr_circle(&center, radius)))
}
fn fc_circle_poly(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let npts = arg_i32(fcinfo, 0);
    let circle = arg_circle(fcinfo, 1);
    let poly = crate::circle::circle_poly(npts, &circle)?;
    Ok(ret_poly(fcinfo, poly))
}

// --- `on_*` containment predicates (point/lseg on line/lseg/box/path) ---

fn fc_on_pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, l) = (arg_point(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_pl(&p, &l)?))
}
fn fc_on_ps(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, ls) = (arg_point(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_ps(&p, &ls)?))
}
fn fc_on_pb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, b) = (arg_point(fcinfo, 0), arg_box(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_pb(&p, &b)))
}
fn fc_on_ppath(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, path) = (arg_point(fcinfo, 0), arg_path(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_ppath(&p, &path)?))
}
fn fc_on_sl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (ls, l) = (arg_lseg(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_sl(&ls, &l)?))
}
fn fc_on_sb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (ls, b) = (arg_lseg(fcinfo, 0), arg_box(fcinfo, 1));
    Ok(ret_bool(crate::proximity::on_sb(&ls, &b)))
}

// --- `close_*` closest-point family (-> point, may be NULL) ---

fn fc_close_pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, l) = (arg_point(fcinfo, 0), arg_line(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_pl(&p, &l)?))
}
fn fc_close_ps(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, ls) = (arg_point(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_ps(&p, &ls)?))
}
fn fc_close_pb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (p, b) = (arg_point(fcinfo, 0), arg_box(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_pb(&p, &b)?))
}
fn fc_close_lseg(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_lseg(&a, &b)?))
}
fn fc_close_ls(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (l, ls) = (arg_line(fcinfo, 0), arg_lseg(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_ls(&l, &ls)?))
}
fn fc_close_sb(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (ls, b) = (arg_lseg(fcinfo, 0), arg_box(fcinfo, 1));
    Ok(ret_opt_point(fcinfo, crate::proximity::close_sb(&ls, &b)?))
}

// --- `dist_*` distance family (-> float8) ---

macro_rules! fc_dist {
    ($fc:ident, $core:path, $ra:ident, $rb:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let a = $ra(fcinfo, 0);
            let b = $rb(fcinfo, 1);
            Ok(ret_f64($core(&a, &b)?))
        }
    };
}
fc_dist!(fc_dist_pl, crate::proximity::dist_pl, arg_point, arg_line);
fc_dist!(fc_dist_lp, crate::proximity::dist_lp, arg_line, arg_point);
fc_dist!(fc_dist_ps, crate::proximity::dist_ps, arg_point, arg_lseg);
fc_dist!(fc_dist_sp, crate::proximity::dist_sp, arg_lseg, arg_point);
fc_dist!(fc_dist_pb, crate::proximity::dist_pb, arg_point, arg_box);
fc_dist!(fc_dist_bp, crate::proximity::dist_bp, arg_box, arg_point);
fc_dist!(fc_dist_sl, crate::proximity::dist_sl, arg_lseg, arg_line);
fc_dist!(fc_dist_ls, crate::proximity::dist_ls, arg_line, arg_lseg);
fc_dist!(fc_dist_sb, crate::proximity::dist_sb, arg_lseg, arg_box);
fc_dist!(fc_dist_bs, crate::proximity::dist_bs, arg_box, arg_lseg);
fc_dist!(fc_dist_pc, crate::proximity::dist_pc, arg_point, arg_circle);
fc_dist!(fc_dist_cpoint, crate::proximity::dist_cpoint, arg_circle, arg_point);
fc_dist!(fc_dist_ppath, crate::proximity::dist_ppath, arg_point, arg_path);
fc_dist!(fc_dist_pathp, crate::proximity::dist_pathp, arg_path, arg_point);
fc_dist!(fc_dist_cpoly, crate::proximity::dist_cpoly, arg_circle, arg_poly);
fc_dist!(fc_dist_polyc, crate::proximity::dist_polyc, arg_poly, arg_circle);
fc_dist!(fc_dist_ppoly, crate::proximity::dist_ppoly, arg_point, arg_poly);
fc_dist!(fc_dist_polyp, crate::proximity::dist_polyp, arg_poly, arg_point);

// --- duplicate-OID type-coercion accessors that reuse existing cores ---
//     (these share a `prosrc` with an already-registered OID; the registry
//     keys by OID so each duplicate OID needs its own row, reusing the adapter.)

/// Register the cross-type `geo_ops.c` builtins (the `dist_*`/`on_*`/`close_*`
/// families, the line/lseg/circle constructors & conversions, and the
/// duplicate-OID type coercions). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`; all strict,
/// none retset. The `name` is the `prosrc` C symbol. The `*_recv` functions take
/// `internal` (a `StringInfo`) which is not expressible at this fmgr boundary,
/// so they remain deferred (as for every other type's `*_recv`).
pub fn register_geo_ops_cross_builtins() {
    fmgr_core::register_builtins_native([
        // point: vert/horiz/slope (two OIDs each for the named SQL fns).
        builtin(989, "point_vert", 2, true, false, fc_point_vert),
        builtin(1406, "point_vert", 2, true, false, fc_point_vert),
        builtin(990, "point_horiz", 2, true, false, fc_point_horiz),
        builtin(1407, "point_horiz", 2, true, false, fc_point_horiz),
        builtin(992, "point_slope", 2, true, false, fc_point_slope),
        // point constructor / point->box coercion.
        builtin(1440, "construct_point", 2, true, false, fc_construct_point),
        builtin(4091, "point_box", 1, true, false, fc_point_box),
        // box: over* predicates + contain_pt + diagonal + duplicate center OID +
        // box<->circle + box<->points.
        builtin(189, "box_overleft", 2, true, false, fc_box_overleft),
        builtin(190, "box_overright", 2, true, false, fc_box_overright),
        builtin(2563, "box_overbelow", 2, true, false, fc_box_overbelow),
        builtin(2564, "box_overabove", 2, true, false, fc_box_overabove),
        builtin(193, "box_contain_pt", 2, true, false, fc_box_contain_pt),
        builtin(138, "box_center", 1, true, false, fc_box_center),
        builtin(1534, "box_center", 1, true, false, fc_box_center),
        builtin(981, "box_diagonal", 1, true, false, fc_box_diagonal),
        builtin(1541, "box_diagonal", 1, true, false, fc_box_diagonal),
        builtin(1421, "points_box", 2, true, false, fc_points_box),
        builtin(1479, "box_circle", 1, true, false, fc_box_circle),
        builtin(1480, "circle_box", 1, true, false, fc_circle_box),
        // lseg: construct / intersect / interpt + duplicate center/length OIDs.
        builtin(993, "lseg_construct", 2, true, false, fc_lseg_construct),
        builtin(994, "lseg_intersect", 2, true, false, fc_lseg_intersect),
        builtin(362, "lseg_interpt", 2, true, false, fc_lseg_interpt),
        builtin(225, "lseg_center", 1, true, false, fc_lseg_center),
        // line: eq / construct_pp / interpt / intersect / distance + duplicate
        // isparallel/isperp/isvertical/ishorizontal OIDs.
        builtin(1492, "line_eq", 2, true, false, fc_line_eq),
        builtin(1493, "line_construct_pp", 2, true, false, fc_line_construct_pp),
        builtin(1494, "line_interpt", 2, true, false, fc_line_interpt),
        builtin(1495, "line_intersect", 2, true, false, fc_line_intersect),
        builtin(239, "line_distance", 2, true, false, fc_line_distance),
        builtin(1412, "line_parallel", 2, true, false, fc_line_parallel),
        builtin(1413, "line_perp", 2, true, false, fc_line_perp),
        builtin(1414, "line_vertical", 1, true, false, fc_line_vertical),
        builtin(1415, "line_horizontal", 1, true, false, fc_line_horizontal),
        // circle: over* predicates + cr_circle + circle_poly + duplicate center OID.
        builtin(1455, "circle_overleft", 2, true, false, fc_circle_overleft),
        builtin(1456, "circle_overright", 2, true, false, fc_circle_overright),
        builtin(2587, "circle_overbelow", 2, true, false, fc_circle_overbelow),
        builtin(2588, "circle_overabove", 2, true, false, fc_circle_overabove),
        builtin(1473, "cr_circle", 2, true, false, fc_cr_circle),
        builtin(1475, "circle_poly", 2, true, false, fc_circle_poly),
        builtin(1416, "circle_center", 1, true, false, fc_circle_center),
        builtin(1472, "circle_center", 1, true, false, fc_circle_center),
        // poly: duplicate center/npoints OIDs.
        builtin(1540, "poly_center", 1, true, false, fc_poly_center),
        builtin(1556, "poly_npoints", 1, true, false, fc_poly_npoints),
        // path: duplicate length/npoints OIDs.
        builtin(1531, "path_length", 1, true, false, fc_path_length),
        builtin(1545, "path_npoints", 1, true, false, fc_path_npoints),
        // on_* containment predicates.
        builtin(959, "on_pl", 2, true, false, fc_on_pl),
        builtin(369, "on_ps", 2, true, false, fc_on_ps),
        builtin(136, "on_pb", 2, true, false, fc_on_pb),
        builtin(137, "on_ppath", 2, true, false, fc_on_ppath),
        builtin(960, "on_sl", 2, true, false, fc_on_sl),
        builtin(372, "on_sb", 2, true, false, fc_on_sb),
        // close_* closest-point family.
        builtin(961, "close_pl", 2, true, false, fc_close_pl),
        builtin(366, "close_ps", 2, true, false, fc_close_ps),
        builtin(367, "close_pb", 2, true, false, fc_close_pb),
        builtin(1489, "close_lseg", 2, true, false, fc_close_lseg),
        builtin(1488, "close_ls", 2, true, false, fc_close_ls),
        builtin(368, "close_sb", 2, true, false, fc_close_sb),
        // dist_* distance family.
        builtin(725, "dist_pl", 2, true, false, fc_dist_pl),
        builtin(702, "dist_lp", 2, true, false, fc_dist_lp),
        builtin(363, "dist_ps", 2, true, false, fc_dist_ps),
        builtin(380, "dist_sp", 2, true, false, fc_dist_sp),
        builtin(364, "dist_pb", 2, true, false, fc_dist_pb),
        builtin(357, "dist_bp", 2, true, false, fc_dist_bp),
        builtin(727, "dist_sl", 2, true, false, fc_dist_sl),
        builtin(704, "dist_ls", 2, true, false, fc_dist_ls),
        builtin(365, "dist_sb", 2, true, false, fc_dist_sb),
        builtin(381, "dist_bs", 2, true, false, fc_dist_bs),
        builtin(1476, "dist_pc", 2, true, false, fc_dist_pc),
        builtin(3290, "dist_cpoint", 2, true, false, fc_dist_cpoint),
        builtin(371, "dist_ppath", 2, true, false, fc_dist_ppath),
        builtin(421, "dist_pathp", 2, true, false, fc_dist_pathp),
        builtin(728, "dist_cpoly", 2, true, false, fc_dist_cpoly),
        builtin(785, "dist_polyc", 2, true, false, fc_dist_polyc),
        builtin(3275, "dist_ppoly", 2, true, false, fc_dist_ppoly),
        builtin(3292, "dist_polyp", 2, true, false, fc_dist_polyp),
    ]);
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every expressible by-ref `geo_ops.c` builtin over the fixed-size
/// `point`/`box`/`lseg`/`line`/`circle` types (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset are
/// transcribed exactly from `pg_proc.dat`; all are strict, none retset.
pub fn register_geo_ops_builtins() {
    fmgr_core::register_builtins_native([
        // I/O: cstring <-> geometric.
        builtin(117, "point_in", 1, true, false, fc_point_in),
        builtin(118, "point_out", 1, true, false, fc_point_out),
        builtin(123, "box_in", 1, true, false, fc_box_in),
        builtin(124, "box_out", 1, true, false, fc_box_out),
        builtin(119, "lseg_in", 1, true, false, fc_lseg_in),
        builtin(120, "lseg_out", 1, true, false, fc_lseg_out),
        builtin(1490, "line_in", 1, true, false, fc_line_in),
        builtin(1491, "line_out", 1, true, false, fc_line_out),
        builtin(1450, "circle_in", 1, true, false, fc_circle_in),
        builtin(1451, "circle_out", 1, true, false, fc_circle_out),
        // point predicates.
        builtin(135, "point_eq", 2, true, false, fc_point_eq),
        builtin(988, "point_ne", 2, true, false, fc_point_ne),
        builtin(132, "point_left", 2, true, false, fc_point_left),
        builtin(133, "point_right", 2, true, false, fc_point_right),
        builtin(131, "point_above", 2, true, false, fc_point_above),
        builtin(134, "point_below", 2, true, false, fc_point_below),
        // point arithmetic.
        builtin(1441, "point_add", 2, true, false, fc_point_add),
        builtin(1442, "point_sub", 2, true, false, fc_point_sub),
        builtin(1443, "point_mul", 2, true, false, fc_point_mul),
        builtin(1444, "point_div", 2, true, false, fc_point_div),
        // point distance.
        builtin(991, "point_distance", 2, true, false, fc_point_distance),
        // box predicates.
        builtin(128, "box_eq", 2, true, false, fc_box_eq),
        builtin(129, "box_lt", 2, true, false, fc_box_lt),
        builtin(130, "box_le", 2, true, false, fc_box_le),
        builtin(127, "box_gt", 2, true, false, fc_box_gt),
        builtin(126, "box_ge", 2, true, false, fc_box_ge),
        builtin(186, "box_same", 2, true, false, fc_box_same),
        builtin(125, "box_overlap", 2, true, false, fc_box_overlap),
        builtin(188, "box_left", 2, true, false, fc_box_left),
        builtin(191, "box_right", 2, true, false, fc_box_right),
        builtin(2562, "box_below", 2, true, false, fc_box_below),
        builtin(2565, "box_above", 2, true, false, fc_box_above),
        builtin(187, "box_contain", 2, true, false, fc_box_contain),
        builtin(192, "box_contained", 2, true, false, fc_box_contained),
        builtin(116, "box_below_eq", 2, true, false, fc_box_below_eq),
        builtin(115, "box_above_eq", 2, true, false, fc_box_above_eq),
        // box measurement / center.
        builtin(978, "box_distance", 2, true, false, fc_box_distance),
        builtin(975, "box_area", 1, true, false, fc_box_area),
        builtin(976, "box_width", 1, true, false, fc_box_width),
        builtin(977, "box_height", 1, true, false, fc_box_height),
        builtin(1542, "box_center", 1, true, false, fc_box_center),
        // box arithmetic.
        builtin(1422, "box_add", 2, true, false, fc_box_add),
        builtin(1423, "box_sub", 2, true, false, fc_box_sub),
        builtin(1424, "box_mul", 2, true, false, fc_box_mul),
        builtin(1425, "box_div", 2, true, false, fc_box_div),
        builtin(980, "box_intersect", 2, true, false, fc_box_intersect),
        // circle predicates.
        builtin(1462, "circle_eq", 2, true, false, fc_circle_eq),
        builtin(1463, "circle_ne", 2, true, false, fc_circle_ne),
        builtin(1464, "circle_lt", 2, true, false, fc_circle_lt),
        builtin(1466, "circle_le", 2, true, false, fc_circle_le),
        builtin(1465, "circle_gt", 2, true, false, fc_circle_gt),
        builtin(1467, "circle_ge", 2, true, false, fc_circle_ge),
        builtin(1452, "circle_same", 2, true, false, fc_circle_same),
        builtin(1459, "circle_overlap", 2, true, false, fc_circle_overlap),
        builtin(1454, "circle_left", 2, true, false, fc_circle_left),
        builtin(1457, "circle_right", 2, true, false, fc_circle_right),
        builtin(1460, "circle_below", 2, true, false, fc_circle_below),
        builtin(1461, "circle_above", 2, true, false, fc_circle_above),
        builtin(1453, "circle_contain", 2, true, false, fc_circle_contain),
        builtin(1458, "circle_contained", 2, true, false, fc_circle_contained),
        builtin(1477, "circle_contain_pt", 2, true, false, fc_circle_contain_pt),
        builtin(1478, "pt_contained_circle", 2, true, false, fc_pt_contained_circle),
        // circle measurement / center.
        builtin(1471, "circle_distance", 2, true, false, fc_circle_distance),
        builtin(1468, "circle_area", 1, true, false, fc_circle_area),
        builtin(1469, "circle_diameter", 1, true, false, fc_circle_diameter),
        builtin(1470, "circle_radius", 1, true, false, fc_circle_radius),
        builtin(1543, "circle_center", 1, true, false, fc_circle_center),
        // circle arithmetic.
        builtin(1146, "circle_add_pt", 2, true, false, fc_circle_add_pt),
        builtin(1147, "circle_sub_pt", 2, true, false, fc_circle_sub_pt),
        builtin(1148, "circle_mul_pt", 2, true, false, fc_circle_mul_pt),
        builtin(1149, "circle_div_pt", 2, true, false, fc_circle_div_pt),
        // lseg predicates.
        builtin(999, "lseg_eq", 2, true, false, fc_lseg_eq),
        builtin(1482, "lseg_ne", 2, true, false, fc_lseg_ne),
        builtin(1483, "lseg_lt", 2, true, false, fc_lseg_lt),
        builtin(1484, "lseg_le", 2, true, false, fc_lseg_le),
        builtin(1485, "lseg_gt", 2, true, false, fc_lseg_gt),
        builtin(1486, "lseg_ge", 2, true, false, fc_lseg_ge),
        builtin(1408, "lseg_parallel", 2, true, false, fc_lseg_parallel),
        builtin(1409, "lseg_perp", 2, true, false, fc_lseg_perp),
        builtin(1410, "lseg_vertical", 1, true, false, fc_lseg_vertical),
        builtin(1411, "lseg_horizontal", 1, true, false, fc_lseg_horizontal),
        // lseg measurement / center.
        builtin(1530, "lseg_length", 1, true, false, fc_lseg_length),
        builtin(1532, "lseg_center", 1, true, false, fc_lseg_center),
        builtin(361, "lseg_distance", 2, true, false, fc_lseg_distance),
        // line predicates.
        builtin(1496, "line_parallel", 2, true, false, fc_line_parallel),
        builtin(1497, "line_perp", 2, true, false, fc_line_perp),
        builtin(1498, "line_vertical", 1, true, false, fc_line_vertical),
        builtin(1499, "line_horizontal", 1, true, false, fc_line_horizontal),
        // operator-row OIDs for the lseg predicate/measurement cores (same
        // prosrc as the 1408-1411/1530 rows above; pg_proc gives these OIDs).
        builtin(995, "lseg_parallel", 2, true, false, fc_lseg_parallel),
        builtin(996, "lseg_perp", 2, true, false, fc_lseg_perp),
        builtin(997, "lseg_vertical", 1, true, false, fc_lseg_vertical),
        builtin(998, "lseg_horizontal", 1, true, false, fc_lseg_horizontal),
        builtin(1487, "lseg_length", 1, true, false, fc_lseg_length),
        // mixed-type intersection predicates + box bounding box.
        builtin(277, "inter_sl", 2, true, false, fc_inter_sl),
        builtin(278, "inter_lb", 2, true, false, fc_inter_lb),
        builtin(373, "inter_sb", 2, true, false, fc_inter_sb),
        builtin(4067, "boxes_bound_box", 2, true, false, fc_boxes_bound_box),
    ]);
}

// ---------------------------------------------------------------------------
// fc_ adapters — the varlena `path` / `polygon` family, plus the binary `*_send`
// functions over all geometric types (the broader fan-out leg). `path` and
// `polygon` are TOASTable varlena types: their `ByRef` byte image is the FULL
// varlena image INCLUDING the 4-byte `vl_len_` header, exactly as
// `Path::from_datum_image`/`Polygon::from_datum_image` (the codec is
// header-aware, like `numeric`). A `path`/`polygon` arg arrives as
// `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(full_image))`; a `path`/
// `polygon` result is set via `set_ref_result(RefPayload::Varlena(full_image))`.
//
// `*_send` returns `bytea`: its wire bytes cross header-stripped on the by-ref
// `Varlena` lane (the varlena/bytea convention), symmetric with `byteasend`.
//
// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`; every
// row here is `proisstrict => 't'` and not `proretset`. The fmgr builtin `name`
// is the `prosrc` C symbol (canonical `fmgr_builtins[]` keys on prosrc).
// ---------------------------------------------------------------------------

/// `PG_GETARG_PATH_P(i)`: decode the full `PATH` varlena image off the by-ref
/// lane.
#[inline]
fn arg_path(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Path {
    Path::from_datum_image(arg_bytes(fcinfo, i))
}

/// `PG_GETARG_POLYGON_P(i)`: decode the full `POLYGON` varlena image.
#[inline]
fn arg_poly(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Polygon {
    Polygon::from_datum_image(arg_bytes(fcinfo, i))
}

/// `PG_RETURN_PATH_P(p)`: set the full `PATH` varlena image on the by-ref lane.
#[inline]
fn ret_path(fcinfo: &mut FunctionCallInfoBaseData, p: Path) -> Datum {
    ret_ref(fcinfo, p.to_datum_image())
}

/// `PG_RETURN_POLYGON_P(p)`: set the full `POLYGON` varlena image.
#[inline]
fn ret_poly(fcinfo: &mut FunctionCallInfoBaseData, p: Polygon) -> Datum {
    ret_ref(fcinfo, p.to_datum_image())
}

/// `PG_RETURN_INT32(v)`.
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// --- path I/O (cstring <-> path) ---

fn fc_path_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let p = crate::io::path_in(&s, escontext)?;
    Ok(ret_path(fcinfo, p))
}
fn fc_path_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = arg_path(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::path_out(&p)))
}

// --- polygon I/O (cstring <-> polygon) ---

fn fc_poly_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    let p = crate::io::poly_in(&s, escontext)?;
    Ok(ret_poly(fcinfo, p))
}
fn fc_poly_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = arg_poly(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::io::poly_out(&p)))
}

// --- path comparison predicates: (path, path) -> bool (pure) ---

macro_rules! fc_pred_path {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_path(fcinfo, 0), arg_path(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_path(fcinfo, 0), arg_path(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}
fc_pred_path!(fc_path_n_lt, crate::path::path_n_lt, pure);
fc_pred_path!(fc_path_n_gt, crate::path::path_n_gt, pure);
fc_pred_path!(fc_path_n_eq, crate::path::path_n_eq, pure);
fc_pred_path!(fc_path_n_le, crate::path::path_n_le, pure);
fc_pred_path!(fc_path_n_ge, crate::path::path_n_ge, pure);
fc_pred_path!(fc_path_inter, crate::path::path_inter, res);

// --- path unary predicates / accessors ---

fn fc_path_isclosed(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::path::path_isclosed(&arg_path(fcinfo, 0))))
}
fn fc_path_isopen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::path::path_isopen(&arg_path(fcinfo, 0))))
}
fn fc_path_npoints(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::path::path_npoints(&arg_path(fcinfo, 0))))
}

// --- path close/open -> path ---

fn fc_path_close(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = crate::path::path_close(&arg_path(fcinfo, 0));
    Ok(ret_path(fcinfo, p))
}
fn fc_path_open(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = crate::path::path_open(&arg_path(fcinfo, 0));
    Ok(ret_path(fcinfo, p))
}

// --- path measurement -> float8 (area/length can be NULL on an empty/open path) ---

fn fc_path_area(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::path::path_area(&arg_path(fcinfo, 0))? {
        Some(v) => Ok(ret_f64(v)),
        None => Ok(ret_null(fcinfo)),
    }
}
fn fc_path_length(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::path::path_length(&arg_path(fcinfo, 0))?))
}
fn fc_path_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_path(fcinfo, 0), arg_path(fcinfo, 1));
    match crate::path::path_distance(&a, &b)? {
        Some(v) => Ok(ret_f64(v)),
        None => Ok(ret_null(fcinfo)),
    }
}

// --- path arithmetic -> path ---

fn fc_path_add(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_path(fcinfo, 0), arg_path(fcinfo, 1));
    match crate::path::path_add(&a, &b)? {
        Some(p) => Ok(ret_path(fcinfo, p)),
        None => Ok(ret_null(fcinfo)),
    }
}
macro_rules! fc_path_pt {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let path = arg_path(fcinfo, 0);
            let pt = arg_point(fcinfo, 1);
            let p = $core(&path, &pt)?;
            Ok(ret_path(fcinfo, p))
        }
    };
}
fc_path_pt!(fc_path_add_pt, crate::path::path_add_pt);
fc_path_pt!(fc_path_sub_pt, crate::path::path_sub_pt);
fc_path_pt!(fc_path_mul_pt, crate::path::path_mul_pt);
fc_path_pt!(fc_path_div_pt, crate::path::path_div_pt);

// --- path <-> polygon conversions ---

fn fc_path_poly(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let poly = crate::path::path_poly(&arg_path(fcinfo, 0))?;
    Ok(ret_poly(fcinfo, poly))
}
fn fc_poly_path(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let path = crate::path::poly_path(&arg_poly(fcinfo, 0));
    Ok(ret_path(fcinfo, path))
}

// --- polygon comparison/containment predicates: (poly, poly) -> bool ---

macro_rules! fc_pred_poly {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_poly(fcinfo, 0), arg_poly(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let (a, b) = (arg_poly(fcinfo, 0), arg_poly(fcinfo, 1));
            Ok(ret_bool($core(&a, &b)?))
        }
    };
}
fc_pred_poly!(fc_poly_left, crate::poly::poly_left, pure);
fc_pred_poly!(fc_poly_overleft, crate::poly::poly_overleft, pure);
fc_pred_poly!(fc_poly_right, crate::poly::poly_right, pure);
fc_pred_poly!(fc_poly_overright, crate::poly::poly_overright, pure);
fc_pred_poly!(fc_poly_below, crate::poly::poly_below, pure);
fc_pred_poly!(fc_poly_overbelow, crate::poly::poly_overbelow, pure);
fc_pred_poly!(fc_poly_above, crate::poly::poly_above, pure);
fc_pred_poly!(fc_poly_overabove, crate::poly::poly_overabove, pure);
fc_pred_poly!(fc_poly_same, crate::poly::poly_same, pure);
fc_pred_poly!(fc_poly_overlap, crate::poly::poly_overlap, res);
fc_pred_poly!(fc_poly_contain, crate::poly::poly_contain, res);
fc_pred_poly!(fc_poly_contained, crate::poly::poly_contained, res);

// --- polygon/point predicates ---

fn fc_poly_contain_pt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let poly = arg_poly(fcinfo, 0);
    let p = arg_point(fcinfo, 1);
    Ok(ret_bool(crate::poly::poly_contain_pt(&poly, &p)?))
}
fn fc_pt_contained_poly(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = arg_point(fcinfo, 0);
    let poly = arg_poly(fcinfo, 1);
    Ok(ret_bool(crate::poly::pt_contained_poly(&p, &poly)?))
}

// --- polygon measurement / accessors ---

fn fc_poly_distance(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let (a, b) = (arg_poly(fcinfo, 0), arg_poly(fcinfo, 1));
    match crate::poly::poly_distance(&a, &b)? {
        Some(v) => Ok(ret_f64(v)),
        None => Ok(ret_null(fcinfo)),
    }
}
fn fc_poly_npoints(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::poly::poly_npoints(&arg_poly(fcinfo, 0))))
}
fn fc_poly_center(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let p = crate::poly::poly_center(&arg_poly(fcinfo, 0))?;
    Ok(ret_point(fcinfo, p))
}

// --- polygon <-> box / circle conversions ---

fn fc_poly_box(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = crate::poly::poly_box(&arg_poly(fcinfo, 0));
    Ok(ret_box(fcinfo, b))
}
fn fc_box_poly(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let poly = crate::io::box_poly(&arg_box(fcinfo, 0));
    Ok(ret_poly(fcinfo, poly))
}
fn fc_poly_circle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = crate::poly::poly_circle(&arg_poly(fcinfo, 0))?;
    Ok(ret_circle(fcinfo, c))
}

// --- binary `*_send` -> bytea (wire bytes, header-stripped on the by-ref lane) ---

fn fc_point_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::point_send(&arg_point(fcinfo, 0))))
}
fn fc_box_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::box_send(&arg_box(fcinfo, 0))))
}
fn fc_lseg_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::lseg_send(&arg_lseg(fcinfo, 0))))
}
fn fc_line_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::line_send(&arg_line(fcinfo, 0))))
}
fn fc_circle_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::circle_send(&arg_circle(fcinfo, 0))))
}
fn fc_path_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::path_send(&arg_path(fcinfo, 0))))
}
fn fc_poly_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_send(fcinfo, crate::io::poly_send(&arg_poly(fcinfo, 0))))
}

// --- binary `*_recv` <- internal (the wire message rides the by-ref lane) ---
//
// The wire message arrives verbatim on the by-ref lane (RefPayload::Varlena);
// each recv core walks it through a `&mut &[u8]` cursor and builds the geo
// value, which crosses back as its header-ful varlena image via the existing
// `ret_*` helpers.

fn fc_point_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let p = crate::io::point_recv(&mut buf)?;
    Ok(ret_point(fcinfo, p))
}
fn fc_box_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let b = crate::io::box_recv(&mut buf)?;
    Ok(ret_box(fcinfo, b))
}
fn fc_lseg_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let ls = crate::io::lseg_recv(&mut buf)?;
    Ok(ret_lseg(fcinfo, ls))
}
fn fc_line_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let l = crate::io::line_recv(&mut buf)?;
    Ok(ret_line(fcinfo, l))
}
fn fc_circle_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let c = crate::io::circle_recv(&mut buf)?;
    Ok(ret_circle(fcinfo, c))
}
fn fc_path_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let p = crate::io::path_recv(&mut buf)?;
    Ok(ret_path(fcinfo, p))
}
fn fc_poly_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut buf = arg_bytes(fcinfo, 0);
    let p = crate::io::poly_recv(&mut buf)?;
    Ok(ret_poly(fcinfo, p))
}

/// `spg_poly_quad_compress` (geo_spgist.c:876): SP-GiST compress support for the
/// `spgist/poly_ops` opclass -- yields the polygon's bounding box (the lossy box
/// representation the box-quad tree indexes).  C:
/// `POLYGON *polygon = PG_GETARG_POLYGON_P(0); box = palloc(sizeof(BOX));
/// *box = polygon->boundbox; PG_RETURN_BOX_P(box);`
fn fc_spg_poly_quad_compress(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let polygon = arg_poly(fcinfo, 0);
    Ok(ret_box(fcinfo, polygon.boundbox))
}

/// Register the varlena `path`/`polygon` `geo_ops.c` builtins (I/O, comparison,
/// containment, measurement, arithmetic, conversions) plus every geometric type's
/// binary `*_send` and `*_recv`. Called from this crate's `init_seams()`. The
/// `*_recv` wire message rides the by-ref lane (each core walks a `&mut &[u8]`
/// cursor). OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`; all
/// strict, none retset. The builtin `name` is the `prosrc` C symbol.
pub fn register_geo_ops_path_poly_builtins() {
    fmgr_core::register_builtins_native([
        // path I/O.
        builtin(121, "path_in", 1, true, false, fc_path_in),
        builtin(122, "path_out", 1, true, false, fc_path_out),
        // polygon I/O.
        builtin(347, "poly_in", 1, true, false, fc_poly_in),
        builtin(348, "poly_out", 1, true, false, fc_poly_out),
        // path comparison predicates.
        builtin(982, "path_n_lt", 2, true, false, fc_path_n_lt),
        builtin(983, "path_n_gt", 2, true, false, fc_path_n_gt),
        builtin(984, "path_n_eq", 2, true, false, fc_path_n_eq),
        builtin(985, "path_n_le", 2, true, false, fc_path_n_le),
        builtin(986, "path_n_ge", 2, true, false, fc_path_n_ge),
        builtin(973, "path_inter", 2, true, false, fc_path_inter),
        // path accessors.
        builtin(1430, "path_isclosed", 1, true, false, fc_path_isclosed),
        builtin(1431, "path_isopen", 1, true, false, fc_path_isopen),
        builtin(1432, "path_npoints", 1, true, false, fc_path_npoints),
        // path close/open.
        builtin(1433, "path_close", 1, true, false, fc_path_close),
        builtin(1434, "path_open", 1, true, false, fc_path_open),
        // path measurement.
        builtin(979, "path_area", 1, true, false, fc_path_area),
        builtin(987, "path_length", 1, true, false, fc_path_length),
        builtin(370, "path_distance", 2, true, false, fc_path_distance),
        // path arithmetic.
        builtin(1435, "path_add", 2, true, false, fc_path_add),
        builtin(1436, "path_add_pt", 2, true, false, fc_path_add_pt),
        builtin(1437, "path_sub_pt", 2, true, false, fc_path_sub_pt),
        builtin(1438, "path_mul_pt", 2, true, false, fc_path_mul_pt),
        builtin(1439, "path_div_pt", 2, true, false, fc_path_div_pt),
        // path <-> polygon.
        builtin(1449, "path_poly", 1, true, false, fc_path_poly),
        builtin(1447, "poly_path", 1, true, false, fc_poly_path),
        // polygon comparison / containment predicates.
        builtin(341, "poly_left", 2, true, false, fc_poly_left),
        builtin(342, "poly_overleft", 2, true, false, fc_poly_overleft),
        builtin(344, "poly_right", 2, true, false, fc_poly_right),
        builtin(343, "poly_overright", 2, true, false, fc_poly_overright),
        builtin(2566, "poly_below", 2, true, false, fc_poly_below),
        builtin(2567, "poly_overbelow", 2, true, false, fc_poly_overbelow),
        builtin(2569, "poly_above", 2, true, false, fc_poly_above),
        builtin(2568, "poly_overabove", 2, true, false, fc_poly_overabove),
        builtin(339, "poly_same", 2, true, false, fc_poly_same),
        builtin(346, "poly_overlap", 2, true, false, fc_poly_overlap),
        builtin(340, "poly_contain", 2, true, false, fc_poly_contain),
        builtin(345, "poly_contained", 2, true, false, fc_poly_contained),
        builtin(1428, "poly_contain_pt", 2, true, false, fc_poly_contain_pt),
        builtin(1429, "pt_contained_poly", 2, true, false, fc_pt_contained_poly),
        // polygon measurement / accessors.
        builtin(729, "poly_distance", 2, true, false, fc_poly_distance),
        builtin(1445, "poly_npoints", 1, true, false, fc_poly_npoints),
        builtin(227, "poly_center", 1, true, false, fc_poly_center),
        // polygon <-> box / circle.
        builtin(1446, "poly_box", 1, true, false, fc_poly_box),
        builtin(1448, "box_poly", 1, true, false, fc_box_poly),
        builtin(1474, "poly_circle", 1, true, false, fc_poly_circle),
        // SP-GiST `spgist/poly_ops` compress: polygon -> bounding box.
        builtin(5011, "spg_poly_quad_compress", 1, true, false, fc_spg_poly_quad_compress),
        // binary `*_send` over all geometric types.
        builtin(2429, "point_send", 1, true, false, fc_point_send),
        builtin(2485, "box_send", 1, true, false, fc_box_send),
        builtin(2481, "lseg_send", 1, true, false, fc_lseg_send),
        builtin(2489, "line_send", 1, true, false, fc_line_send),
        builtin(2491, "circle_send", 1, true, false, fc_circle_send),
        builtin(2483, "path_send", 1, true, false, fc_path_send),
        builtin(2487, "poly_send", 1, true, false, fc_poly_send),
        // binary `*_recv` over all geometric types.
        builtin(2428, "point_recv", 1, true, false, fc_point_recv),
        builtin(2484, "box_recv", 1, true, false, fc_box_recv),
        builtin(2480, "lseg_recv", 1, true, false, fc_lseg_recv),
        builtin(2488, "line_recv", 1, true, false, fc_line_recv),
        builtin(2490, "circle_recv", 1, true, false, fc_circle_recv),
        builtin(2482, "path_recv", 1, true, false, fc_path_recv),
        builtin(2486, "poly_recv", 1, true, false, fc_poly_recv),
    ]);
}

// ===========================================================================
// End-to-end proof: by-reference geometric builtins are genuinely callable
// through the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use datum::NullableDatum;

    /// Install the float8 text-I/O seams (the geo `*_in`/`*_out` cores route
    /// float parsing/formatting through `backend-utils-adt-float`) plus this
    /// crate's own seams (which registers the builtins). Shares the crate-wide
    /// one-time guard so seams are never installed twice.
    fn setup() {
        crate::test_setup();
        // The fmgr builtin registry is thread-local; register on THIS test thread
        // (the global one-time `test_setup` only ran on one thread).
        register_geo_ops_builtins();
        register_geo_ops_path_poly_builtins();
        register_geo_ops_cross_builtins();
    }

    /// Build a fresh by-ref struct image from text via the registered `*_in`.
    fn image_in(oid: u32, s: &str) -> Vec<u8> {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let entry = fmgr_core::native_builtin(oid).expect("in registered");
        entry(&mut fcinfo).expect("native call ok");
        match fcinfo.take_ref_result().expect("in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("in: unexpected lane {other:?}"),
        }
    }

    /// Render a by-ref struct image to text via the registered `*_out`.
    fn image_out(oid: u32, image: Vec<u8>) -> String {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image))];
        let entry = fmgr_core::native_builtin(oid).expect("out registered");
        entry(&mut fcinfo).expect("native call ok");
        match fcinfo.take_ref_result().expect("out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("out: unexpected lane {other:?}"),
        }
    }

    /// Invoke a registered (img, img) -> bool builtin by OID.
    fn call_pred2(oid: u32, a: Vec<u8>, b: Vec<u8>) -> bool {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a)), Some(RefPayload::Varlena(b))];
        let entry = fmgr_core::native_builtin(oid).expect("pred registered");
        let r = entry(&mut fcinfo).expect("native call ok");
        r.as_bool()
    }

    /// Invoke a registered (img, img) -> float8 builtin by OID.
    fn call_dist2(oid: u32, a: Vec<u8>, b: Vec<u8>) -> f64 {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a)), Some(RefPayload::Varlena(b))];
        let entry = fmgr_core::native_builtin(oid).expect("dist registered");
        let r = entry(&mut fcinfo).expect("native call ok");
        r.as_f64()
    }

    /// Invoke a registered (img, img) -> img builtin by OID, reading the by-ref
    /// result back off the lane.
    fn call_binary_ref(oid: u32, a: Vec<u8>, b: Vec<u8>) -> Vec<u8> {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a)), Some(RefPayload::Varlena(b))];
        let entry = fmgr_core::native_builtin(oid).expect("binary registered");
        entry(&mut fcinfo).expect("native call ok");
        match fcinfo.take_ref_result().expect("binary produced a result") {
            RefPayload::Varlena(out) => out,
            other => panic!("binary: unexpected lane {other:?}"),
        }
    }

    #[test]
    fn point_in_out_roundtrip() {
        // point_in(117) -> 16-byte image; point_out(118) renders it back.
        let img = image_in(117, "(1.5,2.5)");
        assert_eq!(img.len(), 16);
        assert_eq!(image_out(118, img.clone()), "(1.5,2.5)");
        let p = Point::from_datum_bytes(&img);
        assert_eq!(p, Point { x: 1.5, y: 2.5 });
    }

    #[test]
    fn point_eq_and_add_by_oid() {
        let a = image_in(117, "(1,2)");
        let b = image_in(117, "(1,2)");
        let c = image_in(117, "(3,4)");
        assert!(call_pred2(135, a.clone(), b.clone())); // point_eq
        assert!(!call_pred2(135, a.clone(), c.clone())); // point_eq
        assert!(call_pred2(988, a.clone(), c.clone())); // point_ne
        // point_add(1441): (1,2)+(3,4) = (4,6).
        let sum = call_binary_ref(1441, a, c);
        assert_eq!(Point::from_datum_bytes(&sum), Point { x: 4.0, y: 6.0 });
    }

    #[test]
    fn point_distance_by_oid() {
        let a = image_in(117, "(0,0)");
        let b = image_in(117, "(3,4)");
        assert_eq!(call_dist2(991, a, b), 5.0); // point_distance = hypot(3,4)
    }

    #[test]
    fn box_in_out_and_contain() {
        // box_in(123) sorts corners high>=low; box_out(124) renders.
        let big = image_in(123, "(0,0),(10,10)");
        assert_eq!(big.len(), 32);
        assert_eq!(image_out(124, big.clone()), "(10,10),(0,0)");
        let small = image_in(123, "(2,2),(4,4)");
        // box_contain(187): big contains small.
        assert!(call_pred2(187, big.clone(), small.clone()));
        // box_contained(192): small contained in big.
        assert!(call_pred2(192, small, big.clone()));
        // box_same(186) with itself.
        assert!(call_pred2(186, big.clone(), big));
    }

    #[test]
    fn circle_in_out_eq_contain_pt() {
        // circle_in(1450) "<(x,y),r>"; circle_out(1451) renders.
        let c = image_in(1450, "<(0,0),5>");
        assert_eq!(c.len(), 24);
        assert_eq!(image_out(1451, c.clone()), "<(0,0),5>");
        assert_eq!(CIRCLE::from_datum_bytes(&c), CIRCLE { center: Point { x: 0.0, y: 0.0 }, radius: 5.0 });
        // circle_contain_pt(1477): origin circle r=5 contains (3,4) [dist 5 == r].
        let p = image_in(117, "(3,4)");
        assert!(call_pred2(1477, c.clone(), p.clone()));
        // pt_contained_circle(1478): reverse.
        assert!(call_pred2(1478, p, c.clone()));
        // circle_eq(1462) with itself.
        assert!(call_pred2(1462, c.clone(), c));
    }

    #[test]
    fn lseg_in_out_and_length() {
        // lseg_in(119) "[(x1,y1),(x2,y2)]"; lseg_out(120).
        let ls = image_in(119, "[(0,0),(3,4)]");
        assert_eq!(ls.len(), 32);
        assert_eq!(image_out(120, ls.clone()), "[(0,0),(3,4)]");
        // lseg_length(1530) = 5.
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(ls.clone()))];
        let entry = fmgr_core::native_builtin(1530).unwrap();
        let r = entry(&mut fcinfo).expect("native call ok");
        assert_eq!(r.as_f64(), 5.0);
        // lseg_eq(999) with itself.
        assert!(call_pred2(999, ls.clone(), ls));
    }

    #[test]
    fn line_in_out_and_vertical() {
        // line_in(1490) accepts "{A,B,C}"; vertical line x=2 -> {1,0,-2}.
        let l = image_in(1490, "{1,0,-2}");
        assert_eq!(l.len(), 24);
        // line_vertical(1498): B==0 => vertical.
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(l.clone()))];
        let entry = fmgr_core::native_builtin(1498).unwrap();
        let r = entry(&mut fcinfo).expect("native call ok");
        assert!(r.as_bool());
        // line_out(1491) round-trips through the registry without panic.
        let _ = image_out(1491, l);
    }

    #[test]
    fn box_intersect_null_when_disjoint() {
        // box_intersect(980) returns NULL when the two boxes do not overlap.
        setup();
        let a = image_in(123, "(0,0),(1,1)");
        let b = image_in(123, "(5,5),(6,6)");
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a)), Some(RefPayload::Varlena(b))];
        let entry = fmgr_core::native_builtin(980).unwrap();
        entry(&mut fcinfo).expect("native call ok");
        assert!(fcinfo.isnull, "disjoint box_intersect must be NULL");
        assert!(fcinfo.take_ref_result().is_none());
    }

    /// Invoke a registered (img,) -> int4 builtin by OID.
    fn call_int1(oid: u32, a: Vec<u8>) -> i32 {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a))];
        let entry = fmgr_core::native_builtin(oid).expect("int1 registered");
        entry(&mut fcinfo).expect("native call ok").as_i32()
    }

    /// Invoke a registered (img,) -> bool builtin by OID.
    fn call_pred1(oid: u32, a: Vec<u8>) -> bool {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a))];
        let entry = fmgr_core::native_builtin(oid).expect("pred1 registered");
        entry(&mut fcinfo).expect("native call ok").as_bool()
    }

    /// Invoke a registered (img,) -> img builtin by OID, reading the result lane.
    fn call_unary_ref(oid: u32, a: Vec<u8>) -> Vec<u8> {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a))];
        let entry = fmgr_core::native_builtin(oid).expect("unary registered");
        entry(&mut fcinfo).expect("native call ok");
        match fcinfo.take_ref_result().expect("unary produced a result") {
            RefPayload::Varlena(out) => out,
            other => panic!("unary: unexpected lane {other:?}"),
        }
    }

    /// `path_in`/`path_out` round-trip the FULL varlena image through the registry,
    /// and `path_npoints`/`path_isopen` read the decoded structure.
    #[test]
    fn path_in_out_npoints_isopen_by_oid() {
        // An open path of 3 vertices.
        let img = image_in(121, "[(0,0),(1,1),(2,0)]");
        // path_out (122) renders it back.
        assert_eq!(image_out(122, img.clone()), "[(0,0),(1,1),(2,0)]");
        // path_npoints (1432) = 3.
        assert_eq!(call_int1(1432, img.clone()), 3);
        // path_isopen (1431) true; path_isclosed (1430) false.
        assert!(call_pred1(1431, img.clone()));
        assert!(!call_pred1(1430, img.clone()));
        // path_close (1433) -> closed path; path_isclosed now true.
        let closed = call_unary_ref(1433, img);
        assert!(call_pred1(1430, closed));
    }

    /// `poly_in`/`poly_out` round-trip the FULL `POLYGON` varlena image, and
    /// `poly_npoints`/`poly_contain_pt` operate over it.
    #[test]
    fn poly_in_out_npoints_contain_by_oid() {
        // A unit square (closed quad).
        let img = image_in(347, "((0,0),(0,2),(2,2),(2,0))");
        // poly_out (348) renders it back.
        assert_eq!(image_out(348, img.clone()), "((0,0),(0,2),(2,2),(2,0))");
        // poly_npoints (1445) = 4.
        assert_eq!(call_int1(1445, img.clone()), 4);
        // poly_contain_pt (1428): the square contains (1,1).
        let p = image_in(117, "(1,1)");
        assert!(call_pred2(1428, img.clone(), p));
        // poly_same (339) with itself.
        assert!(call_pred2(339, img.clone(), img));
    }

    /// `point_send` (2429) emits a header-ful `bytea` (C: pq_endtypsend) carrying
    /// the 16-byte big-endian wire image of a point (two float8 in network byte
    /// order). The send seam strips VARHDRSZ to recover the wire payload.
    #[test]
    fn point_send_by_oid() {
        const VARHDRSZ: usize = 4;
        let p = image_in(117, "(1,2)");
        let image = call_unary_ref(2429, p);
        // bytea image = 4-byte SET_VARSIZE header + 2 * float8send (16 bytes).
        assert_eq!(image.len(), VARHDRSZ + 16);
        let wire = &image[VARHDRSZ..];
        assert_eq!(&wire[0..8], &1.0f64.to_be_bytes());
        assert_eq!(&wire[8..16], &2.0f64.to_be_bytes());
    }
}
