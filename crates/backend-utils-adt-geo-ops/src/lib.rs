//! Port of `src/backend/utils/adt/geo_ops.c` (PostgreSQL 18.3) -- the 2-D
//! geometric types and operators (`point`, `line`, `lseg`, `box`, `path`,
//! `polygon`, `circle`) as safe, owned-value Rust.
//!
//! Scope: the computational cores of geo_ops.c, with the original C function
//! names preserved. The `Datum NAME(PG_FUNCTION_ARGS)` fmgr shims (argument
//! unpacking via `PG_GETARG_*`, `PG_RETURN_*`, `PG_FREE_IF_COPY`, the varlena
//! palloc / `SET_VARSIZE` serialization of `path` / `polygon`) are part of the
//! project-wide deferred fmgr/Datum boundary; each core is exposed behind a
//! stable, typed signature over the value structs from [`types_core::geo`]
//! (`Point`, `LSEG`, `LINE`, `BOX`, `CIRCLE`) plus the owned [`Path`] /
//! [`Polygon`] stand-ins for the toastable `PATH` / `POLYGON` varlena values.
//!
//! Fidelity: behavior preserved branch-for-branch. The "fuzzy" comparisons
//! (`FPeq`, etc.) use the exact `EPSILON = 1e-6` from `<utils/geo_decls.h>`.
//! The float8 arithmetic, NaN-aware comparisons, infinity/NaN generators,
//! over/underflow error reporters, and float8 text I/O all live in
//! `utils/adt/float.c` (the `backend-utils-adt-float` subsystem); they are
//! reached across that cross-subsystem boundary through the float seams (see the
//! [`f8`] module). Overflow / underflow / divide-by-zero errors propagate with
//! the same SQLSTATE and message text as C. Input syntax errors raise
//! `ERRCODE_INVALID_TEXT_REPRESENTATION` (22P02); binary-format errors raise
//! `ERRCODE_INVALID_BINARY_REPRESENTATION` (22P03).
//!
//! This crate owns and installs the seams declared in
//! `backend-utils-adt-geo-ops-seams`: the fuzzy float comparators, `pg_hypot` /
//! `HYPOT`, and the box / point boolean operators used by the `box` / `point`
//! GiST / SP-GiST opclasses.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::needless_range_loop)]

use types_error::{
    make_sqlstate, PgError, PgResult, SqlState, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub use types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};

// ---------------------------------------------------------------------------
// Owned safe-Rust stand-ins for the toastable varlena `PATH` / `POLYGON` types
// (geo_decls.h:115, 151). The palloc / `SET_VARSIZE` varlena serialization is
// part of the deferred Datum fmgr layer; the points are stored out of line in an
// owned `Vec<Point>`.
// ---------------------------------------------------------------------------

/// `DatumGetPathP`/`DatumGetPolygonP` are `PG_DETOAST_DATUM`, which un-packs a
/// short (1-byte header) varlena to the canonical 4-byte form. Under
/// `SHORT_VARLENA_PACKING` a small `path`/`polygon` (toastable: typlen == -1,
/// typstorage == 'x') can be heap-stored with a 1-byte header; the codecs below
/// read `npts` at a FIXED 4-byte offset, so a short image must be un-packed
/// first. Returns the un-packed bytes (or the input borrowed verbatim for a
/// 4-byte / external / compressed image — the latter are detoasted upstream).
/// Behavior-preserving with the flag OFF.
fn unpack_short_geo(bytes: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    use std::borrow::Cow;
    // VARATT_IS_1B && !VARATT_IS_1B_E (short inline header, low bit set, != 0x01).
    if bytes.first().is_some_and(|&b| b != 0x01 && (b & 0x01) == 0x01) {
        const VARHDRSZ: usize = 4;
        const VARHDRSZ_SHORT: usize = 1;
        let data_size = ((bytes[0] >> 1) & 0x7f) as usize - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut out = Vec::with_capacity(new_size);
        out.extend_from_slice(&((new_size as u32) << 2).to_ne_bytes());
        out.extend_from_slice(&bytes[VARHDRSZ_SHORT..VARHDRSZ_SHORT + data_size]);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(bytes)
    }
}

/// Safe-Rust representation of the varlena `PATH` type (geo_decls.h:115).
#[derive(Clone, Debug, PartialEq)]
pub struct Path {
    /// `closed` (`!isopen`) -- whether this is a closed polyline.
    pub closed: bool,
    /// The vertex points (`p[npts]`). Always nonempty for a valid path.
    pub points: Vec<Point>,
}

impl Path {
    /// `path->npts`.
    #[inline]
    pub fn npts(&self) -> i32 {
        self.points.len() as i32
    }

    /// `offsetof(PATH, p)` (geo_decls.h:115): a 4-byte `vl_len_` varlena header,
    /// then `int32 npts`, `int32 closed`, `int32 dummy` (double-align padding) =
    /// 16 bytes before the `Point p[npts]` flexible array.
    pub const HEADER_SIZE: usize = 16;

    /// `DatumGetPathP(datum)` analogue: decode the in-memory `PATH` varlena image
    /// into the owned value. The image layout matches the C `struct PATH`: a
    /// 4-byte `vl_len_` length word, `int32 npts`, `int32 closed`, `int32 dummy`,
    /// then `Point p[npts]` (16 bytes each).
    ///
    /// Panics on a too-short image — a caller bug, exactly as C would misread a
    /// truncated detoasted pointer.
    pub fn from_datum_image(raw_bytes: &[u8]) -> Path {
        let unpacked = unpack_short_geo(raw_bytes);
        let bytes: &[u8] = &unpacked;
        let mut npts_b = [0u8; 4];
        npts_b.copy_from_slice(&bytes[4..8]);
        let npts = i32::from_ne_bytes(npts_b) as usize;
        let mut closed_b = [0u8; 4];
        closed_b.copy_from_slice(&bytes[8..12]);
        let closed = i32::from_ne_bytes(closed_b) != 0;
        let mut points: Vec<Point> = Vec::with_capacity(npts);
        for i in 0..npts {
            let off = Path::HEADER_SIZE + i * 16;
            points.push(Point::from_datum_bytes(&bytes[off..off + 16]));
        }
        Path { closed, points }
    }

    /// `PathPGetDatum(path)` analogue: serialize this path to its in-memory `PATH`
    /// varlena image (4-byte `vl_len_`, `int32 npts`, `int32 closed`, `int32
    /// dummy`, then `Point p[npts]`). The caller wraps the bytes in a
    /// `Datum::ByRef`.
    pub fn to_datum_image(&self) -> Vec<u8> {
        let npts = self.points.len();
        let total = Path::HEADER_SIZE + npts * 16;
        let mut out = vec![0u8; total];
        out[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
        out[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
        out[8..12].copy_from_slice(&(self.closed as i32).to_ne_bytes());
        // bytes[12..16] = dummy padding, left zero.
        for (i, p) in self.points.iter().enumerate() {
            let off = Path::HEADER_SIZE + i * 16;
            out[off..off + 16].copy_from_slice(&p.to_datum_bytes());
        }
        out
    }
}

/// Safe-Rust representation of the varlena `POLYGON` type (geo_decls.h:151).
#[derive(Clone, Debug, PartialEq)]
pub struct Polygon {
    /// `boundbox` -- the bounding box, kept for speed (recomputed on read).
    pub boundbox: BOX,
    /// The vertex points (`p[npts]`). Always nonempty for a valid polygon.
    pub points: Vec<Point>,
}

impl Polygon {
    /// `poly->npts`.
    #[inline]
    pub fn npts(&self) -> i32 {
        self.points.len() as i32
    }

    /// `DatumGetPolygonP(datum)` analogue: decode the in-memory `POLYGON`
    /// varlena image into the owned value. The image layout matches the C
    /// `struct POLYGON` (geo_decls.h:151): a 4-byte `vl_len_` varlena header,
    /// then `int32 npts`, then `BOX boundbox` (32 bytes), then `Point p[npts]`
    /// (16 bytes each) — `POLYGON_HEADER_SIZE` (40) is `offsetof(POLYGON, p)`
    /// from the start of the struct (including the 4-byte length word and 4
    /// bytes of `npts`).
    ///
    /// Panics on a too-short image — a caller bug, exactly as C would misread a
    /// truncated detoasted pointer.
    pub fn from_datum_image(raw_bytes: &[u8]) -> Polygon {
        let unpacked = unpack_short_geo(raw_bytes);
        let bytes: &[u8] = &unpacked;
        // Skip the 4-byte varlena length word; read npts (int32, native order).
        let mut npts_b = [0u8; 4];
        npts_b.copy_from_slice(&bytes[4..8]);
        let npts = i32::from_ne_bytes(npts_b) as usize;

        // boundbox: BOX (32 bytes) at offset 8.
        let boundbox = BOX::from_datum_bytes(&bytes[8..40]);

        // points: npts * Point (16 bytes each) starting at offset 40
        // (= POLYGON_HEADER_SIZE).
        let mut points: Vec<Point> = Vec::with_capacity(npts);
        let base = types_core::geo::POLYGON_HEADER_SIZE;
        for i in 0..npts {
            let off = base + i * 16;
            points.push(Point::from_datum_bytes(&bytes[off..off + 16]));
        }
        Polygon { boundbox, points }
    }

    /// `PolygonPGetDatum(poly)` analogue: serialize this polygon to its in-memory
    /// `POLYGON` varlena image (4-byte `vl_len_` length word, `int32 npts`,
    /// `BOX boundbox`, then `Point p[npts]`). The caller wraps the bytes in a
    /// `Datum::ByRef`.
    pub fn to_datum_image(&self) -> Vec<u8> {
        let npts = self.points.len();
        let total = types_core::geo::POLYGON_HEADER_SIZE + npts * 16;
        let mut out = vec![0u8; total];
        // vl_len_: the standard 4-byte varlena length word (length << 2, lowest
        // bits clear). Mirrors `SET_VARSIZE(poly, total)`.
        out[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
        out[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
        out[8..40].copy_from_slice(&self.boundbox.to_datum_bytes());
        let base = types_core::geo::POLYGON_HEADER_SIZE;
        for (i, p) in self.points.iter().enumerate() {
            let off = base + i * 16;
            out[off..off + 16].copy_from_slice(&p.to_datum_bytes());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Thin wrappers over the float8 cores reached through the float seam, so the
// per-type modules can read exactly like the C (`crate::f8::float8_pl(...)`)
// while all the genuine `utils/adt/float.c` crossings stay centralized.
// ---------------------------------------------------------------------------
pub(crate) mod f8 {
    use backend_utils_adt_float_seams as s;
    use types_error::PgResult;

    #[inline]
    pub fn float8_pl(a: f64, b: f64) -> PgResult<f64> {
        s::float8_pl::call(a, b)
    }
    #[inline]
    pub fn float8_mi(a: f64, b: f64) -> PgResult<f64> {
        s::float8_mi::call(a, b)
    }
    #[inline]
    pub fn float8_mul(a: f64, b: f64) -> PgResult<f64> {
        s::float8_mul::call(a, b)
    }
    #[inline]
    pub fn float8_div(a: f64, b: f64) -> PgResult<f64> {
        s::float8_div::call(a, b)
    }
    #[inline]
    pub fn float8_eq(a: f64, b: f64) -> bool {
        s::float8_eq::call(a, b)
    }
    #[inline]
    pub fn float8_lt(a: f64, b: f64) -> bool {
        s::float8_lt::call(a, b)
    }
    #[inline]
    pub fn float8_gt(a: f64, b: f64) -> bool {
        s::float8_gt::call(a, b)
    }
    #[inline]
    pub fn float8_min(a: f64, b: f64) -> f64 {
        s::float8_min::call(a, b)
    }
    #[inline]
    pub fn float8_max(a: f64, b: f64) -> f64 {
        s::float8_max::call(a, b)
    }
    #[inline]
    pub fn get_float8_infinity() -> f64 {
        s::get_float8_infinity::call()
    }
    #[inline]
    pub fn get_float8_nan() -> f64 {
        s::get_float8_nan::call()
    }
}

pub mod boxes;
pub mod circle;
pub mod fmgr_builtins;
pub mod io;
pub mod line;
pub mod lseg;
pub mod path;
pub mod point;
pub mod poly;
pub mod proximity;

pub use boxes::*;
pub use circle::*;
pub use io::*;
pub use line::*;
pub use lseg::*;
pub use path::*;
pub use point::*;
pub use poly::*;
pub use proximity::*;

// ---------------------------------------------------------------------------
// Useful floating point utilities and constants (<utils/geo_decls.h>:41-91).
// ---------------------------------------------------------------------------

/// `EPSILON` (geo_decls.h:41): the "fuzz" for the `FP*` comparisons.
pub const EPSILON: f64 = 1.0e-6;

/// `FPzero(A)` (geo_decls.h:44): `fabs(A) <= EPSILON`.
#[inline]
pub fn FPzero(a: f64) -> bool {
    a.abs() <= EPSILON
}

/// `FPeq(A, B)` (geo_decls.h:46). Not NaN-aware; false for any NaN input.
#[inline]
pub fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}

/// `FPne(A, B)` (geo_decls.h:52).
#[inline]
pub fn FPne(a: f64, b: f64) -> bool {
    a != b && (a - b).abs() > EPSILON
}

/// `FPlt(A, B)` (geo_decls.h:58).
#[inline]
pub fn FPlt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}

/// `FPle(A, B)` (geo_decls.h:64).
#[inline]
pub fn FPle(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}

/// `FPgt(A, B)` (geo_decls.h:70).
#[inline]
pub fn FPgt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}

/// `FPge(A, B)` (geo_decls.h:76).
#[inline]
pub fn FPge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

/// `HYPOT(A, B)` (geo_decls.h:91): the [`pg_hypot`] of the two values.
#[inline]
pub fn HYPOT(a: f64, b: f64) -> PgResult<f64> {
    pg_hypot(a, b)
}

/// `M_PI` (`<utils/float.h>`): PostgreSQL defines this exact literal.
pub(crate) const M_PI: f64 = core::f64::consts::PI;

// ---------------------------------------------------------------------------
// SQLSTATE helpers used by the geometric input / structural errors.
// ---------------------------------------------------------------------------

/// `ERRCODE_INVALID_TEXT_REPRESENTATION` (22P02).
#[inline]
pub(crate) fn errcode_invalid_text() -> SqlState {
    ERRCODE_INVALID_TEXT_REPRESENTATION
}

/// `ERRCODE_INVALID_BINARY_REPRESENTATION` (22P03).
#[inline]
pub(crate) fn errcode_invalid_binary() -> SqlState {
    ERRCODE_INVALID_BINARY_REPRESENTATION
}

/// `ERRCODE_INVALID_PARAMETER_VALUE` (22023).
#[inline]
pub(crate) fn errcode_invalid_parameter() -> SqlState {
    ERRCODE_INVALID_PARAMETER_VALUE
}

/// `ERRCODE_PROGRAM_LIMIT_EXCEEDED` (54000).
#[inline]
pub(crate) fn errcode_program_limit() -> SqlState {
    ERRCODE_PROGRAM_LIMIT_EXCEEDED
}

/// `ERRCODE_FEATURE_NOT_SUPPORTED` (0A000).
#[inline]
pub(crate) fn errcode_feature_not_supported() -> SqlState {
    ERRCODE_FEATURE_NOT_SUPPORTED
}

// Keep `make_sqlstate` referenced even though every code is a named constant.
#[allow(dead_code)]
const _: fn([u8; 5]) -> SqlState = make_sqlstate;

/// Build the geometric input syntax error (geo_ops.c, e.g. line 250):
/// `invalid input syntax for type <type_name>: "<orig_string>"`, SQLSTATE
/// 22P02.
pub(crate) fn invalid_input(type_name: &str, orig_string: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {type_name}: \"{orig_string}\""
    ))
    .with_sqlstate(errcode_invalid_text())
}

// ---------------------------------------------------------------------------
// pg_hypot (geo_ops.c:5518) -- the only function declared in geo_decls.h.
// ---------------------------------------------------------------------------

/// `float_overflow_error()` (float.c:85).
fn float_overflow_error() -> PgError {
    PgError::error("value out of range: overflow").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `float_underflow_error()` (float.c:93).
fn float_underflow_error() -> PgError {
    PgError::error("value out of range: underflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `pg_hypot(x, y)` (geo_ops.c:5518): the hypotenuse `sqrt(x^2 + y^2)`,
/// computed in a way that avoids spurious overflow. `hypot(inf, nan)` is INF
/// (per IEEE 1003.1 / glibc). Overflow / underflow of the final result raise
/// the shared float over/underflow errors.
pub fn pg_hypot(mut x: f64, mut y: f64) -> PgResult<f64> {
    // Handle INF and NaN properly.
    if x.is_infinite() || y.is_infinite() {
        return Ok(f8::get_float8_infinity());
    }
    if x.is_nan() || y.is_nan() {
        return Ok(f8::get_float8_nan());
    }

    // Else, drop any minus signs.
    x = x.abs();
    y = y.abs();

    // Swap x and y if needed to make x the larger one.
    if x < y {
        core::mem::swap(&mut x, &mut y);
    }

    // If y is zero, the hypotenuse is x. This test saves a few cycles in such
    // cases, but more importantly it also protects against divide-by-zero
    // errors, since now x >= y.
    if y == 0.0 {
        return Ok(x);
    }

    // Determine the hypotenuse.
    let yx = y / x;
    let result = x * (1.0 + (yx * yx)).sqrt();

    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 {
        return Err(float_underflow_error());
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Point-in-polygon test + polygon equality (geo_ops.c:5337-5505).
// ---------------------------------------------------------------------------

/// Sentinel for "the point lies exactly on the polygon" (`POINT_ON_POLYGON =
/// INT_MAX`, geo_ops.c:5337).
const POINT_ON_POLYGON: i32 = i32::MAX;

/// `point_inside(p, npts, plist)` (geo_ops.c:5339): 1 if `p` is strictly inside
/// the polygon, 2 if it is on the polygon, 0 otherwise.
///
/// Returns `PgResult` because the underlying [`lseg_crossing`] routes its `z`
/// determinant through `float8_mul`/`float8_mi`, which `ereport` an overflow
/// (`ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE`) just as C does; that error must
/// propagate rather than silently producing `Inf`/`NaN`.
pub fn point_inside(p: &Point, plist: &[Point]) -> PgResult<i32> {
    debug_assert!(!plist.is_empty());

    // First polygon point relative to the test point. C computes these deltas
    // with the overflow-checked float8_mi (geo_ops.c:5355-5356,5364-5365), so an
    // overflowing difference raises 22003 here rather than flowing into the
    // crossing logic as an Inf.
    let x0 = f8::float8_mi(plist[0].x, p.x)?;
    let y0 = f8::float8_mi(plist[0].y, p.y)?;

    let mut prev_x = x0;
    let mut prev_y = y0;
    let mut total_cross = 0i32;

    for pt in &plist[1..] {
        let x = f8::float8_mi(pt.x, p.x)?;
        let y = f8::float8_mi(pt.y, p.y)?;

        let cross = lseg_crossing(x, y, prev_x, prev_y)?;
        if cross == POINT_ON_POLYGON {
            return Ok(2);
        }
        total_cross += cross;

        prev_x = x;
        prev_y = y;
    }

    // Now do the first point.
    let cross = lseg_crossing(x0, y0, prev_x, prev_y)?;
    if cross == POINT_ON_POLYGON {
        return Ok(2);
    }
    total_cross += cross;

    Ok(if total_cross != 0 { 1 } else { 0 })
}

/// `lseg_crossing(x, y, prev_x, prev_y)` (geo_ops.c:5396).
///
/// Returns +/-2 if the segment crosses the positive X-axis in a +/- direction,
/// +/-1 if one point is on the positive X-axis, 0 if both points are on the
/// positive X-axis (or there is no crossing), and `POINT_ON_POLYGON` if the
/// segment contains the origin.
fn lseg_crossing(x: f64, y: f64, prev_x: f64, prev_y: f64) -> PgResult<i32> {
    if FPzero(y) {
        // y == 0, on X axis.
        if FPzero(x) {
            // (x, y) is (0, 0)?
            Ok(POINT_ON_POLYGON)
        } else if FPgt(x, 0.0) {
            // x > 0.
            if FPzero(prev_y) {
                // y and prev_y are zero; prev_x > 0?
                return Ok(if FPgt(prev_x, 0.0) {
                    0
                } else {
                    POINT_ON_POLYGON
                });
            }
            Ok(if FPlt(prev_y, 0.0) { 1 } else { -1 })
        } else {
            // x < 0, x not on positive X axis.
            if FPzero(prev_y) {
                // prev_x < 0?
                return Ok(if FPlt(prev_x, 0.0) {
                    0
                } else {
                    POINT_ON_POLYGON
                });
            }
            Ok(0)
        }
    } else {
        // y != 0.
        let y_sign = if FPgt(y, 0.0) { 1 } else { -1 };

        if FPzero(prev_y) {
            // Previous point was on X axis, so new point is either off or on.
            Ok(if FPlt(prev_x, 0.0) { 0 } else { y_sign })
        } else if (y_sign < 0 && FPlt(prev_y, 0.0)) || (y_sign > 0 && FPgt(prev_y, 0.0)) {
            // Both above or below X axis (same sign).
            Ok(0)
        } else {
            // y and prev_y cross X-axis.
            if FPge(x, 0.0) && FPgt(prev_x, 0.0) {
                // Both non-negative so cross positive X-axis.
                return Ok(2 * y_sign);
            }
            if FPlt(x, 0.0) && FPle(prev_x, 0.0) {
                // Both non-positive so do not cross positive X-axis.
                return Ok(0);
            }

            // x and y cross axes (geo_ops.c:5443). The determinant goes through
            // `float8_mul`/`float8_mi` so a genuine overflow raises 22003 exactly
            // as in C, instead of silently yielding Inf/NaN:
            //   z = (x - prev_x) * y - (y - prev_y) * x
            let z = f8::float8_mi(
                f8::float8_mul(f8::float8_mi(x, prev_x)?, y)?,
                f8::float8_mul(f8::float8_mi(y, prev_y)?, x)?,
            )?;
            if FPzero(z) {
                return Ok(POINT_ON_POLYGON);
            }
            if (y_sign < 0 && FPlt(z, 0.0)) || (y_sign > 0 && FPgt(z, 0.0)) {
                return Ok(0);
            }
            Ok(2 * y_sign)
        }
    }
}

/// `plist_same(npts, p1, p2)` (geo_ops.c:5456): whether two point lists
/// describe the same closed polygon (matching forward or backward from any
/// rotation). `p1.len() == p2.len() == npts` is required by the caller.
pub fn plist_same(p1: &[Point], p2: &[Point]) -> bool {
    use crate::point::point_eq_point;

    let npts = p1.len();
    debug_assert_eq!(npts, p2.len());

    // Find a match for the first point of p1 in p2.
    for i in 0..npts {
        if point_eq_point(&p2[i], &p1[0]) {
            // Match found: look forward through the remaining points.
            let mut ii = 1usize;
            let mut j = i + 1;
            while ii < npts {
                if j >= npts {
                    j = 0;
                }
                if !point_eq_point(&p2[j], &p1[ii]) {
                    break;
                }
                ii += 1;
                j += 1;
            }
            if ii == npts {
                return true;
            }

            // Not found forwards: look backwards.
            ii = 1;
            let mut jb: isize = i as isize - 1;
            while ii < npts {
                if jb < 0 {
                    jb = npts as isize - 1;
                }
                if !point_eq_point(&p2[jb as usize], &p1[ii]) {
                    break;
                }
                ii += 1;
                jb -= 1;
            }
            if ii == npts {
                return true;
            }
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam owned by this crate.
pub fn init_seams() {
    use backend_utils_adt_geo_ops_seams as seams;

    seams::FPlt::set(FPlt);
    seams::FPle::set(FPle);
    seams::FPgt::set(FPgt);
    seams::FPge::set(FPge);

    seams::HYPOT::set(pg_hypot);

    seams::box_overlap::set(box_overlap);
    seams::box_contain::set(box_contain);
    seams::box_contained::set(box_contained);
    seams::box_same::set(box_same);
    seams::box_left::set(box_left);
    seams::box_overleft::set(box_overleft);
    seams::box_right::set(box_right);
    seams::box_overright::set(box_overright);
    seams::box_above::set(box_above);
    seams::box_overabove::set(box_overabove);
    seams::box_below::set(box_below);
    seams::box_overbelow::set(box_overbelow);

    seams::point_left::set(point_left);
    seams::point_right::set(point_right);
    seams::point_above::set(point_above);
    seams::point_below::set(point_below);
    seams::point_horiz::set(point_horiz);
    seams::point_vert::set(point_vert);
    seams::point_eq::set(point_eq);

    seams::box_contain_pt::set(box_contain_pt);

    seams::poly_query_boundbox::set(poly_query_boundbox);
    seams::poly_contain_pt_image::set(poly_contain_pt_image);
    seams::circle_contain_pt::set(circle_contain_pt);

    // Register the by-reference fmgr-ABI builtin wrappers (C: fmgr_builtins[]).
    crate::fmgr_builtins::register_geo_ops_builtins();
    crate::fmgr_builtins::register_geo_ops_path_poly_builtins();
    crate::fmgr_builtins::register_geo_ops_cross_builtins();
}

/// Shared one-time test-seam setup (used by this crate's `mod tests` and by
/// `fmgr_builtins::tests`). Installs the float8 seams (this crate routes all
/// float arithmetic and float8 text I/O through `backend-utils-adt-float`),
/// this crate's own seams (which registers the fmgr builtins), plus the
/// `check_for_interrupts` / `check_stack_depth` seams. Goes through a single
/// `Once` so the seams are never installed twice across the two test modules.
#[cfg(test)]
pub(crate) fn test_setup() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        backend_utils_adt_float::init_seams();
        init_seams();
        backend_tcop_postgres_seams::check_for_interrupts::set(|| Ok(()));
        backend_utils_misc_stack_depth_seams::check_stack_depth::set(|| Ok(()));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        crate::test_setup();
    }

    fn p(x: f64, y: f64) -> Point {
        Point { x, y }
    }

    #[test]
    fn polygon_datum_image_roundtrip() {
        // Triangle; boundbox recomputed on decode via from_datum_image carries
        // the serialized boundbox bytes.
        let mut poly = Polygon {
            boundbox: BOX::default(),
            points: vec![p(0.0, 0.0), p(4.0, 0.0), p(2.0, 3.0)],
        };
        make_bound_box(&mut poly);

        let image = poly.to_datum_image();
        // POLYGON_HEADER_SIZE (40) + 3 points * 16 bytes.
        assert_eq!(image.len(), 40 + 3 * 16);

        let back = Polygon::from_datum_image(&image);
        assert_eq!(back.points, poly.points);
        assert_eq!(back.boundbox, poly.boundbox);

        // The boundbox extracted directly from the image matches.
        assert_eq!(poly_query_boundbox(&image), poly.boundbox);
        // poly_contain_pt_image: an interior point is contained, an exterior is not.
        setup();
        assert!(poly_contain_pt_image(&image, &p(2.0, 1.0)).unwrap());
        assert!(!poly_contain_pt_image(&image, &p(10.0, 10.0)).unwrap());
    }

    #[test]
    fn fp_comparisons_use_epsilon() {
        // Within EPSILON => equal; not NaN-aware.
        assert!(FPeq(1.0, 1.0 + 5e-7));
        assert!(!FPeq(1.0, 1.0 + 5e-6));
        assert!(FPzero(1e-7));
        assert!(!FPzero(1e-5));
        assert!(FPlt(1.0, 1.0 + 1e-3));
        assert!(!FPlt(1.0, 1.0 + 5e-7));
        assert!(!FPeq(f64::NAN, f64::NAN));
    }

    #[test]
    fn pg_hypot_matches_c_special_cases() {
        setup();
        assert_eq!(pg_hypot(3.0, 4.0).unwrap(), 5.0);
        // hypot(inf, nan) == inf (per IEEE 1003.1 / glibc).
        assert_eq!(pg_hypot(f64::INFINITY, f64::NAN).unwrap(), f64::INFINITY);
        assert!(pg_hypot(f64::NAN, 1.0).unwrap().is_nan());
        assert_eq!(pg_hypot(7.0, 0.0).unwrap(), 7.0);
        assert_eq!(pg_hypot(0.0, 0.0).unwrap(), 0.0);
        let err = pg_hypot(f64::MAX, f64::MAX).unwrap_err();
        assert_eq!(err.message(), "value out of range: overflow");
    }

    #[test]
    fn point_in_out_roundtrip() {
        setup();
        let pt = point_in("(1,2)", None).unwrap();
        assert_eq!(pt, p(1.0, 2.0));
        assert_eq!(point_out(&pt), "(1,2)");
        assert_eq!(point_in(" 3 , 4 ", None).unwrap(), p(3.0, 4.0));
        assert!(point_in("(1,2)x", None).is_err());
    }

    #[test]
    fn box_in_sorts_corners() {
        setup();
        let b = box_in("(0,0),(2,3)", None).unwrap();
        assert_eq!(b.high, p(2.0, 3.0));
        assert_eq!(b.low, p(0.0, 0.0));
        let b2 = box_in("(2,3),(0,0)", None).unwrap();
        assert_eq!(b2.high, p(2.0, 3.0));
        assert_eq!(b2.low, p(0.0, 0.0));
        assert_eq!(box_out(&b), "(2,3),(0,0)");
    }

    #[test]
    fn line_in_general_form_and_two_points() {
        setup();
        let l = line_in("{1,2,3}", None).unwrap();
        assert_eq!(l, LINE { A: 1.0, B: 2.0, C: 3.0 });
        assert_eq!(line_out(&l), "{1,2,3}");
        let err = line_in("{0,0,5}", None).unwrap_err();
        assert_eq!(
            err.message(),
            "invalid line specification: A and B cannot both be zero"
        );
        let l2 = line_in("[(0,0),(1,0)]", None).unwrap();
        assert_eq!(l2, LINE { A: 0.0, B: -1.0, C: 0.0 });
        assert!(line_in("[(0,0),(0,0)]", None).is_err());
    }

    #[test]
    fn lseg_path_poly_circle_io() {
        setup();
        let ls = lseg_in("[(0,0),(1,1)]", None).unwrap();
        assert_eq!(lseg_out(&ls), "[(0,0),(1,1)]");

        let path = path_in("((0,0),(1,0),(1,1))", None).unwrap();
        assert!(path.closed);
        assert_eq!(path.points.len(), 3);
        assert_eq!(path_out(&path), "((0,0),(1,0),(1,1))");

        let open = path_in("[(0,0),(1,0)]", None).unwrap();
        assert!(!open.closed);
        assert_eq!(path_out(&open), "[(0,0),(1,0)]");

        let poly = poly_in("((0,0),(2,0),(2,2),(0,2))", None).unwrap();
        assert_eq!(poly.points.len(), 4);
        assert_eq!(poly_out(&poly), "((0,0),(2,0),(2,2),(0,2))");
        assert_eq!(poly.boundbox.low, p(0.0, 0.0));
        assert_eq!(poly.boundbox.high, p(2.0, 2.0));

        let c = circle_in("<(1,2),3>", None).unwrap();
        assert_eq!(c.center, p(1.0, 2.0));
        assert_eq!(c.radius, 3.0);
        assert_eq!(circle_out(&c), "<(1,2),3>");
        assert!(circle_in("<(1,2),-1>", None).is_err());
    }

    #[test]
    fn binary_recv_send_roundtrip() {
        setup();
        let pt = p(1.5, -2.5);
        let bytes = point_send(&pt);
        let mut cur = &bytes[..];
        assert_eq!(point_recv(&mut cur).unwrap(), pt);
        assert!(cur.is_empty());

        let path = Path {
            closed: true,
            points: vec![p(0.0, 0.0), p(1.0, 1.0)],
        };
        let pbytes = path_send(&path);
        let mut pcur = &pbytes[..];
        assert_eq!(path_recv(&mut pcur).unwrap(), path);

        let mut short = &pbytes[..3];
        assert!(path_recv(&mut short).is_err());
    }

    #[test]
    fn box_poly_conversion() {
        setup();
        let b = box_in("(0,0),(2,2)", None).unwrap();
        let poly = box_poly(&b);
        assert_eq!(poly.points.len(), 4);
        assert_eq!(poly.points[0], p(0.0, 0.0));
        assert_eq!(poly.points[2], p(2.0, 2.0));
    }

    #[test]
    fn lseg_to_lseg_distance() {
        setup();
        let l1 = lseg_in("[(0,0),(2,0)]", None).unwrap();
        let l2 = lseg_in("[(0,1),(2,1)]", None).unwrap();
        assert_eq!(lseg_closept_lseg(None, &l1, &l2).unwrap(), 1.0);
    }

    #[test]
    fn box_to_point_distance_and_closest() {
        setup();
        let b = box_in("(0,0),(2,2)", None).unwrap();
        let d = dist_pb(&p(5.0, 5.0), &b).unwrap();
        assert!((d - 4.242_640_687_119_286).abs() < 1e-12);
        assert_eq!(close_pb(&p(5.0, 1.0), &b).unwrap(), Some(p(2.0, 1.0)));
    }

    #[test]
    fn polygon_overlap_and_contain() {
        setup();
        let a = poly_in("((0,0),(2,0),(2,2),(0,2))", None).unwrap();
        let b = poly_in("((1,1),(3,1),(3,3),(1,3))", None).unwrap();
        assert!(poly_overlap(&a, &b).unwrap());

        let c = poly_in("((0,0),(1,0),(1,1),(0,1))", None).unwrap();
        let d = poly_in("((5,5),(6,5),(6,6),(5,6))", None).unwrap();
        assert!(!poly_overlap(&c, &d).unwrap());

        let outer = poly_in("((0,0),(10,0),(10,10),(0,10))", None).unwrap();
        let inner = poly_in("((2,2),(3,2),(3,3),(2,3))", None).unwrap();
        assert!(poly_contain(&outer, &inner).unwrap());
    }

    #[test]
    fn close_lseg_parallel_is_none() {
        setup();
        let l1 = lseg_in("[(0,0),(2,0)]", None).unwrap();
        let l2 = lseg_in("[(0,1),(2,1)]", None).unwrap();
        assert_eq!(close_lseg(&l1, &l2).unwrap(), None);
    }

    #[test]
    fn circle_to_polygon_distance() {
        setup();
        let circle = circle_in("<(10,10),1>", None).unwrap();
        let poly = poly_in("((0,0),(2,0),(2,2),(0,2))", None).unwrap();
        let d = dist_cpoly(&circle, &poly).unwrap();
        assert!((d - 10.313_708_498_984_761).abs() < 1e-12);
    }

    #[test]
    fn point_arithmetic_and_inside() {
        setup();
        // complex multiply: (1+2i)*(3+4i) = (3-8) + (4+6)i = (-5,10)
        assert_eq!(point_mul(&p(1.0, 2.0), &p(3.0, 4.0)).unwrap(), p(-5.0, 10.0));
        assert_eq!(point_add(&p(1.0, 2.0), &p(3.0, 4.0)).unwrap(), p(4.0, 6.0));

        let square = vec![p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 2.0)];
        assert_eq!(point_inside(&p(1.0, 1.0), &square).unwrap(), 1); // inside
        assert_eq!(point_inside(&p(0.0, 1.0), &square).unwrap(), 2); // on edge
        assert_eq!(point_inside(&p(5.0, 5.0), &square).unwrap(), 0); // outside
    }

    #[test]
    fn circle_poly_overflow_and_success() {
        setup();
        let c = CIRCLE {
            center: p(0.0, 0.0),
            radius: 4.0,
        };
        let err = circle_poly(i32::MAX, &c).unwrap_err();
        assert_eq!(err.message(), "too many points requested");
        assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        let poly = circle_poly(8, &c).unwrap();
        assert_eq!(poly.points.len(), 8);
    }
}
