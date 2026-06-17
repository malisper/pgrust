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

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};

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

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// I/O adapters (cstring <-> by-ref geometric).
// ---------------------------------------------------------------------------

fn fc_point_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = ok(crate::io::point_in(arg_cstring(fcinfo, 0)));
    ret_point(fcinfo, p)
}
fn fc_point_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = arg_point(fcinfo, 0);
    ret_cstring(fcinfo, crate::io::point_out(&p))
}
fn fc_box_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = ok(crate::io::box_in(arg_cstring(fcinfo, 0)));
    ret_box(fcinfo, b)
}
fn fc_box_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_box(fcinfo, 0);
    ret_cstring(fcinfo, crate::io::box_out(&b))
}
fn fc_lseg_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = ok(crate::io::lseg_in(arg_cstring(fcinfo, 0)));
    ret_ref(fcinfo, lseg_bytes(&ls))
}
fn fc_lseg_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = arg_lseg(fcinfo, 0);
    ret_cstring(fcinfo, crate::io::lseg_out(&ls))
}
fn fc_line_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let l = ok(crate::io::line_in(arg_cstring(fcinfo, 0)));
    ret_ref(fcinfo, line_bytes(&l))
}
fn fc_line_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let l = arg_line(fcinfo, 0);
    ret_cstring(fcinfo, crate::io::line_out(&l))
}
fn fc_circle_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = ok(crate::io::circle_in(arg_cstring(fcinfo, 0)));
    ret_circle(fcinfo, c)
}
fn fc_circle_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    ret_cstring(fcinfo, crate::io::circle_out(&c))
}

// ---------------------------------------------------------------------------
// Comparison/predicate macros: (T, T) -> bool, where the core is pure or
// `PgResult<bool>`. `_p` = pure `fn(&T,&T)->bool`, `_r` = `fn(&T,&T)->PgResult<bool>`.
// ---------------------------------------------------------------------------

macro_rules! fc_pred_point {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            ret_bool($core(&a, &b))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            ret_bool(ok($core(&a, &b)))
        }
    };
}
macro_rules! fc_pred_box {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
            ret_bool($core(&a, &b))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
            ret_bool(ok($core(&a, &b)))
        }
    };
}
macro_rules! fc_pred_circle {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
            ret_bool($core(&a, &b))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
            ret_bool(ok($core(&a, &b)))
        }
    };
}
macro_rules! fc_pred_lseg {
    ($fc:ident, $core:path, pure) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
            ret_bool($core(&a, &b))
        }
    };
    ($fc:ident, $core:path, res) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
            ret_bool(ok($core(&a, &b)))
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

// lseg unary predicates
fn fc_lseg_vertical(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = arg_lseg(fcinfo, 0);
    ret_bool(crate::lseg::lseg_vertical(&ls))
}
fn fc_lseg_horizontal(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = arg_lseg(fcinfo, 0);
    ret_bool(crate::lseg::lseg_horizontal(&ls))
}

// line predicates
fn fc_line_parallel(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    ret_bool(ok(crate::line::line_parallel(&a, &b)))
}
fn fc_line_perp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_line(fcinfo, 0), arg_line(fcinfo, 1));
    ret_bool(ok(crate::line::line_perp(&a, &b)))
}
fn fc_line_vertical(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let l = arg_line(fcinfo, 0);
    ret_bool(crate::line::line_vertical(&l))
}
fn fc_line_horizontal(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let l = arg_line(fcinfo, 0);
    ret_bool(crate::line::line_horizontal(&l))
}

// circle/point predicate (mixed arg types).
fn fc_circle_contain_pt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    let p = arg_point(fcinfo, 1);
    ret_bool(ok(crate::circle::circle_contain_pt(&c, &p)))
}
fn fc_pt_contained_circle(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = arg_point(fcinfo, 0);
    let c = arg_circle(fcinfo, 1);
    ret_bool(ok(crate::circle::pt_contained_circle(&p, &c)))
}

// ---------------------------------------------------------------------------
// Distance/measurement -> float8.
// ---------------------------------------------------------------------------

fn fc_point_distance(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
    ret_f64(ok(crate::point::point_distance(&a, &b)))
}
fn fc_box_distance(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
    ret_f64(ok(crate::boxes::box_distance(&a, &b)))
}
fn fc_circle_distance(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_circle(fcinfo, 0), arg_circle(fcinfo, 1));
    ret_f64(ok(crate::circle::circle_distance(&a, &b)))
}
fn fc_lseg_distance(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_lseg(fcinfo, 0), arg_lseg(fcinfo, 1));
    ret_f64(ok(crate::proximity::lseg_distance(&a, &b)))
}
fn fc_box_area(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_box(fcinfo, 0);
    ret_f64(ok(crate::boxes::box_area(&b)))
}
fn fc_box_width(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_box(fcinfo, 0);
    ret_f64(ok(crate::boxes::box_width(&b)))
}
fn fc_box_height(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_box(fcinfo, 0);
    ret_f64(ok(crate::boxes::box_height(&b)))
}
fn fc_circle_area(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    ret_f64(ok(crate::circle::circle_area(&c)))
}
fn fc_circle_diameter(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    ret_f64(ok(crate::circle::circle_diameter(&c)))
}
fn fc_circle_radius(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    ret_f64(crate::circle::circle_radius(&c))
}
fn fc_lseg_length(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = arg_lseg(fcinfo, 0);
    ret_f64(ok(crate::lseg::lseg_length(&ls)))
}

// ---------------------------------------------------------------------------
// "center" -> point.
// ---------------------------------------------------------------------------

fn fc_box_center(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_box(fcinfo, 0);
    let p = ok(crate::boxes::box_center(&b));
    ret_point(fcinfo, p)
}
fn fc_circle_center(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_circle(fcinfo, 0);
    let p = crate::circle::circle_center(&c);
    ret_point(fcinfo, p)
}
fn fc_lseg_center(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ls = arg_lseg(fcinfo, 0);
    let p = ok(crate::lseg::lseg_center(&ls));
    ret_point(fcinfo, p)
}

// ---------------------------------------------------------------------------
// Arithmetic -> point / box / circle.
// ---------------------------------------------------------------------------

macro_rules! fc_arith_point {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let (a, b) = (arg_point(fcinfo, 0), arg_point(fcinfo, 1));
            let p = ok($core(&a, &b));
            ret_point(fcinfo, p)
        }
    };
}
fc_arith_point!(fc_point_add, crate::point::point_add);
fc_arith_point!(fc_point_sub, crate::point::point_sub);
fc_arith_point!(fc_point_mul, crate::point::point_mul);
fc_arith_point!(fc_point_div, crate::point::point_div);

macro_rules! fc_arith_box {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let b = arg_box(fcinfo, 0);
            let p = arg_point(fcinfo, 1);
            let r = ok($core(&b, &p));
            ret_box(fcinfo, r)
        }
    };
}
fc_arith_box!(fc_box_add, crate::boxes::box_add);
fc_arith_box!(fc_box_sub, crate::boxes::box_sub);
fc_arith_box!(fc_box_mul, crate::boxes::box_mul);
fc_arith_box!(fc_box_div, crate::boxes::box_div);

fn fc_box_intersect(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (arg_box(fcinfo, 0), arg_box(fcinfo, 1));
    match crate::boxes::box_intersect(&a, &b) {
        Some(r) => ret_box(fcinfo, r),
        None => ret_null(fcinfo),
    }
}

macro_rules! fc_arith_circle {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let c = arg_circle(fcinfo, 0);
            let p = arg_point(fcinfo, 1);
            let r = ok($core(&c, &p));
            ret_circle(fcinfo, r)
        }
    };
}
fc_arith_circle!(fc_circle_add_pt, crate::circle::circle_add_pt);
fc_arith_circle!(fc_circle_sub_pt, crate::circle::circle_sub_pt);
fc_arith_circle!(fc_circle_mul_pt, crate::circle::circle_mul_pt);
fc_arith_circle!(fc_circle_div_pt, crate::circle::circle_div_pt);

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every expressible by-ref `geo_ops.c` builtin over the fixed-size
/// `point`/`box`/`lseg`/`line`/`circle` types (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset are
/// transcribed exactly from `pg_proc.dat`; all are strict, none retset.
pub fn register_geo_ops_builtins() {
    backend_utils_fmgr_core::register_builtins([
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
    ]);
}

// ===========================================================================
// End-to-end proof: by-reference geometric builtins are genuinely callable
// through the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    /// Install the float8 text-I/O seams (the geo `*_in`/`*_out` cores route
    /// float parsing/formatting through `backend-utils-adt-float`) plus this
    /// crate's own seams (which registers the builtins). Shares the crate-wide
    /// one-time guard so seams are never installed twice.
    fn setup() {
        crate::test_setup();
        // The fmgr builtin registry is thread-local; register on THIS test thread
        // (the global one-time `test_setup` only ran on one thread).
        register_geo_ops_builtins();
    }

    /// Build a fresh by-ref struct image from text via the registered `*_in`.
    fn image_in(oid: u32, s: &str) -> Vec<u8> {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("in registered");
        (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("out registered");
        (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("pred registered");
        let r = (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("dist registered");
        let r = (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("binary registered");
        (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1530).unwrap();
        let r = (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1498).unwrap();
        let r = (entry.func.unwrap())(&mut fcinfo);
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(980).unwrap();
        (entry.func.unwrap())(&mut fcinfo);
        assert!(fcinfo.isnull, "disjoint box_intersect must be NULL");
        assert!(fcinfo.take_ref_result().is_none());
    }
}
