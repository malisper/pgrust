//! geo_ops.c: the 2-D geometric types (point/lseg/line/box/path/polygon/circle)
//! and their operators. PATH and POLYGON are varlena; the fixed-size types are
//! by-ref native struct images.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod boxes;
pub mod builtins;
pub mod circle;
pub mod io;
pub mod line;
pub mod lseg;
pub mod path;
pub mod point;
pub mod poly;
pub mod proximity;
#[cfg(test)]
mod tests_full;

use ::adt_float::{float8_max, float8_mi, float8_min, float8_mul};
use ::types_core::geo::{Point, BOX, PATH_HEADER_SIZE, POLYGON_HEADER_SIZE};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION};

pub const EPSILON: f64 = 1.0E-06;

pub(crate) const M_PI: f64 = core::f64::consts::PI;

#[inline]
pub fn FPzero(a: f64) -> bool {
    a.abs() <= EPSILON
}
#[inline]
pub fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}
// Not !FPeq: C's FPne is NaN-false ((NaN-NaN) > EPSILON is false).
#[inline]
pub fn FPne(a: f64, b: f64) -> bool {
    a != b && (a - b).abs() > EPSILON
}
#[inline]
pub fn FPlt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}
#[inline]
pub fn FPle(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}
#[inline]
pub fn FPgt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}
#[inline]
pub fn FPge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

// point_eq_point: NaNs insist on float8_eq equality (NaN == NaN there).
pub fn point_eq_point(pt1: &Point, pt2: &Point) -> bool {
    if pt1.x.is_nan() || pt1.y.is_nan() || pt2.x.is_nan() || pt2.y.is_nan() {
        return ::adt_float::float8_eq(pt1.x, pt2.x) && ::adt_float::float8_eq(pt1.y, pt2.y);
    }
    FPeq(pt1.x, pt2.x) && FPeq(pt1.y, pt2.y)
}

pub fn box_ov(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.low.x, box2.high.x)
        && FPle(box2.low.x, box1.high.x)
        && FPle(box1.low.y, box2.high.y)
        && FPle(box2.low.y, box1.high.y)
}

pub fn box_contain_box(contains: &BOX, contained: &BOX) -> bool {
    FPge(contains.high.x, contained.high.x)
        && FPle(contains.low.x, contained.low.x)
        && FPge(contains.high.y, contained.high.y)
        && FPle(contains.low.y, contained.low.y)
}

// box_contain_point: deliberately exact, not fuzzy (C comment on on_pb).
pub fn box_contain_point(b: &BOX, point: &Point) -> bool {
    b.high.x >= point.x && b.low.x <= point.x && b.high.y >= point.y && b.low.y <= point.y
}

/// pg_hypot.
pub fn pg_hypot(x: f64, y: f64) -> PgResult<f64> {
    if x.is_infinite() || y.is_infinite() {
        return Ok(f64::INFINITY);
    }
    if x.is_nan() || y.is_nan() {
        return Ok(f64::NAN);
    }
    let (mut x, mut y) = (x.abs(), y.abs());
    if x < y {
        core::mem::swap(&mut x, &mut y);
    }
    if y == 0.0 {
        return Ok(x);
    }
    let yx = y / x;
    // C computes `x * sqrt(1.0 + (yx * yx))` as RAW arithmetic (no float8_*
    // checked helpers), so gcc (-ffp-contract=fast, the default) and clang
    // (-ffp-contract=on) contract `1.0 + yx*yx` into fma(yx, yx, 1.0) on
    // aarch64 — the C twin's radius differs in the last ulp from the
    // uncontracted form, which float8out's shortest-round-trip then prints
    // as a different decimal string (t25 soak P1: s101 idx 42355,
    // circle(box) on fast_emp4000 — 160/3378 rect.data rows diverged).
    // Rust never contracts implicitly; fuse explicitly for byte parity.
    // (On targets without hardware FMA, mul_add is a correctly-rounded
    // soft fma — same value.)
    let result = x * f64::mul_add(yx, yx, 1.0).sqrt();
    if result.is_infinite() {
        return Err(Box::new(::adt_float::float_overflow_error()));
    }
    if result == 0.0 {
        return Err(Box::new(::adt_float::float_underflow_error()));
    }
    Ok(result)
}

/// point_dt.
pub fn point_dt(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    pg_hypot(float8_mi(pt1.x, pt2.x)?, float8_mi(pt1.y, pt2.y)?)
}

#[cold]
pub(crate) fn invalid_input(type_name: &str, orig: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {type_name}: \"{orig}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// box_in core.
pub fn box_in(s: &str) -> PgResult<BOX> {
    io::box_in(s, None)
}

/// point_in core.
pub fn point_in(s: &str) -> PgResult<Point> {
    io::point_in(s, None)
}

pub(crate) fn pair_encode(x: f64, y: f64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 64];
    let n = ::adt_float::float8out_internal(x, &mut buf);
    out.extend_from_slice(&buf[..n]);
    out.push(b',');
    let n = ::adt_float::float8out_internal(y, &mut buf);
    out.extend_from_slice(&buf[..n]);
}

/// path_encode(PATH_NONE) over the given points.
pub fn path_encode_none(pts: &[Point], out: &mut Vec<u8>) {
    for (i, p) in pts.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'(');
        pair_encode(p.x, p.y, out);
        out.push(b')');
    }
}

/// adjustBox (gistproc.c): grow b to include addon (also usable here).
///
/// The comparisons must be the NaN-aware float8_lt/float8_gt, not raw
/// `<`/`>`: C switched to them in 1acf757255 (bug #14238, back-patched to
/// 9.2) because raw double compares assume `x == x`, which NaN breaks —
/// upstream saw an unkillable infinite loop in GiST build plus qsort
/// misordering of ordinary values. Raw compares here kept the finite side
/// where C adopts the NaN (proofs/gist-geo divergence).
pub fn adjust_box(b: &mut BOX, addon: &BOX) {
    if ::adt_float::float8_lt(b.high.x, addon.high.x) {
        b.high.x = addon.high.x;
    }
    if ::adt_float::float8_gt(b.low.x, addon.low.x) {
        b.low.x = addon.low.x;
    }
    if ::adt_float::float8_lt(b.high.y, addon.high.y) {
        b.high.y = addon.high.y;
    }
    if ::adt_float::float8_gt(b.low.y, addon.low.y) {
        b.low.y = addon.low.y;
    }
}

/// rt_box_union.
pub fn rt_box_union(a: &BOX, b: &BOX) -> BOX {
    BOX {
        high: Point {
            x: float8_max(a.high.x, b.high.x),
            y: float8_max(a.high.y, b.high.y),
        },
        low: Point {
            x: float8_min(a.low.x, b.low.x),
            y: float8_min(a.low.y, b.low.y),
        },
    }
}

// Indexed point access shared by owned slices and varlena image views.
pub trait Pts {
    fn n(&self) -> usize;
    fn pt(&self, i: usize) -> Point;
}

impl Pts for &[Point] {
    #[inline]
    fn n(&self) -> usize {
        self.len()
    }
    #[inline]
    fn pt(&self, i: usize) -> Point {
        self[i]
    }
}

pub const POINT_SIZE: usize = 16;

/// Borrowed view over a PATH varlena payload (bytes after the 4-byte header).
#[derive(Clone, Copy)]
pub struct PathRef<'a> {
    pub closed: bool,
    npts: usize,
    pts: &'a [u8],
}

impl<'a> PathRef<'a> {
    pub fn from_payload(data: &'a [u8]) -> PathRef<'a> {
        let npts = i32::from_ne_bytes(data[0..4].try_into().unwrap()) as usize;
        let closed = i32::from_ne_bytes(data[4..8].try_into().unwrap()) != 0;
        let base = PATH_HEADER_SIZE - 4;
        PathRef {
            closed,
            npts,
            pts: &data[base..base + npts * POINT_SIZE],
        }
    }
}

impl Pts for PathRef<'_> {
    #[inline]
    fn n(&self) -> usize {
        self.npts
    }
    #[inline]
    fn pt(&self, i: usize) -> Point {
        Point::from_datum_bytes(&self.pts[i * POINT_SIZE..i * POINT_SIZE + POINT_SIZE])
    }
}

/// Borrowed view over a POLYGON varlena payload (bytes after the 4-byte header).
#[derive(Clone, Copy)]
pub struct PolyRef<'a> {
    pub boundbox: BOX,
    npts: usize,
    pts: &'a [u8],
}

impl<'a> PolyRef<'a> {
    pub fn from_payload(data: &'a [u8]) -> PolyRef<'a> {
        let npts = i32::from_ne_bytes(data[0..4].try_into().unwrap()) as usize;
        let boundbox = BOX::from_datum_bytes(&data[4..36]);
        let base = POLYGON_HEADER_SIZE - 4;
        PolyRef {
            boundbox,
            npts,
            pts: &data[base..base + npts * POINT_SIZE],
        }
    }
}

impl Pts for PolyRef<'_> {
    #[inline]
    fn n(&self) -> usize {
        self.npts
    }
    #[inline]
    fn pt(&self, i: usize) -> Point {
        Point::from_datum_bytes(&self.pts[i * POINT_SIZE..i * POINT_SIZE + POINT_SIZE])
    }
}

/// make_bound_box over any point sequence.
pub fn bound_box(pts: &impl Pts) -> BOX {
    debug_assert!(pts.n() != 0);
    let p0 = pts.pt(0);
    let (mut x1, mut x2, mut y1, mut y2) = (p0.x, p0.x, p0.y, p0.y);
    for i in 1..pts.n() {
        let p = pts.pt(i);
        if ::adt_float::float8_lt(p.x, x1) {
            x1 = p.x;
        }
        if ::adt_float::float8_gt(p.x, x2) {
            x2 = p.x;
        }
        if ::adt_float::float8_lt(p.y, y1) {
            y1 = p.y;
        }
        if ::adt_float::float8_gt(p.y, y2) {
            y2 = p.y;
        }
    }
    BOX {
        high: Point { x: x2, y: y2 },
        low: Point { x: x1, y: y1 },
    }
}

const POINT_ON_POLYGON: i32 = i32::MAX;

pub fn point_inside(p: &Point, plist: &impl Pts) -> PgResult<i32> {
    debug_assert!(plist.n() != 0);

    let p0 = plist.pt(0);
    let x0 = float8_mi(p0.x, p.x)?;
    let y0 = float8_mi(p0.y, p.y)?;

    let mut prev_x = x0;
    let mut prev_y = y0;
    let mut total_cross = 0i32;

    for i in 1..plist.n() {
        let pt = plist.pt(i);
        let x = float8_mi(pt.x, p.x)?;
        let y = float8_mi(pt.y, p.y)?;

        let cross = lseg_crossing(x, y, prev_x, prev_y)?;
        if cross == POINT_ON_POLYGON {
            return Ok(2);
        }
        total_cross += cross;

        prev_x = x;
        prev_y = y;
    }

    let cross = lseg_crossing(x0, y0, prev_x, prev_y)?;
    if cross == POINT_ON_POLYGON {
        return Ok(2);
    }
    total_cross += cross;

    Ok(if total_cross != 0 { 1 } else { 0 })
}

fn lseg_crossing(x: f64, y: f64, prev_x: f64, prev_y: f64) -> PgResult<i32> {
    if FPzero(y) {
        if FPzero(x) {
            Ok(POINT_ON_POLYGON)
        } else if FPgt(x, 0.0) {
            if FPzero(prev_y) {
                return Ok(if FPgt(prev_x, 0.0) {
                    0
                } else {
                    POINT_ON_POLYGON
                });
            }
            Ok(if FPlt(prev_y, 0.0) { 1 } else { -1 })
        } else {
            if FPzero(prev_y) {
                return Ok(if FPlt(prev_x, 0.0) {
                    0
                } else {
                    POINT_ON_POLYGON
                });
            }
            Ok(0)
        }
    } else {
        let y_sign = if FPgt(y, 0.0) { 1 } else { -1 };

        if FPzero(prev_y) {
            Ok(if FPlt(prev_x, 0.0) { 0 } else { y_sign })
        } else if (y_sign < 0 && FPlt(prev_y, 0.0)) || (y_sign > 0 && FPgt(prev_y, 0.0)) {
            Ok(0)
        } else {
            if FPge(x, 0.0) && FPgt(prev_x, 0.0) {
                return Ok(2 * y_sign);
            }
            if FPlt(x, 0.0) && FPle(prev_x, 0.0) {
                return Ok(0);
            }

            let z = float8_mi(
                float8_mul(float8_mi(x, prev_x)?, y)?,
                float8_mul(float8_mi(y, prev_y)?, x)?,
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

pub fn plist_same(p1: &impl Pts, p2: &impl Pts) -> bool {
    let npts = p1.n();
    debug_assert_eq!(npts, p2.n());

    for i in 0..npts {
        if point_eq_point(&p2.pt(i), &p1.pt(0)) {
            let mut ii = 1usize;
            let mut j = i + 1;
            while ii < npts {
                if j >= npts {
                    j = 0;
                }
                if !point_eq_point(&p2.pt(j), &p1.pt(ii)) {
                    break;
                }
                ii += 1;
                j += 1;
            }
            if ii == npts {
                return true;
            }

            ii = 1;
            let mut jb: isize = i as isize - 1;
            while ii < npts {
                if jb < 0 {
                    jb = npts as isize - 1;
                }
                if !point_eq_point(&p2.pt(jb as usize), &p1.pt(ii)) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_io_roundtrip() {
        for s in ["((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", " ( (1, 2) , (3 ,4) ) "] {
            let b = box_in(s).unwrap();
            assert_eq!(b.high.x, 3.0);
            assert_eq!(b.high.y, 4.0);
            assert_eq!(b.low.x, 1.0);
            assert_eq!(b.low.y, 2.0);
        }
        let b = box_in("(3,4),(1,2)").unwrap();
        assert_eq!((b.high.x, b.low.x), (3.0, 1.0));
        assert!(box_in("((1,2),(3,4)").is_err());
        assert!(box_in("1,2,3").is_err());

        let mut out = Vec::new();
        path_encode_none(&[b.high, b.low], &mut out);
        assert_eq!(out, b"(3,4),(1,2)");
    }

    #[test]
    fn point_io() {
        let p = point_in("(1.5,-2)").unwrap();
        assert_eq!((p.x, p.y), (1.5, -2.0));
        assert!(point_in("(1,2) x").is_err());
        let mut out = Vec::new();
        path_encode_none(&[p], &mut out);
        assert_eq!(out, b"(1.5,-2)");
    }

    #[test]
    fn fp_macros() {
        assert!(FPeq(1.0, 1.0 + 5e-7));
        assert!(!FPeq(1.0, 1.0 + 2e-6));
        assert!(FPlt(1.0, 1.0 + 2e-6));
        assert!(!FPlt(1.0, 1.0 + 5e-7));
    }

    #[test]
    fn hypot_matches_c_shape() {
        assert_eq!(pg_hypot(3.0, 4.0).unwrap(), 5.0);
        assert_eq!(pg_hypot(0.0, 0.0).unwrap(), 0.0);
        assert!(pg_hypot(f64::INFINITY, f64::NAN).unwrap().is_infinite());
        // Byte-parity pin for the fp-contracted form (fma(yx,yx,1.0)): the
        // circle(box '(137,582),(867,821)') radius from the t25 soak family.
        // The uncontracted form gives ...886 (last-ulp high); C-on-aarch64
        // (and thus the shipped oracle) gives ...874.
        assert_eq!(pg_hypot(365.0, 119.5).unwrap(), 384.06412225043874_f64);
        // rect.data witness row (box '(11003,10859),(10950,10765)'): the
        // circle(box) radius that flipped the s101 idx 42355 md5
        // (<(10976.5,10812),53.95600059307583>; uncontracted gives ...584).
        assert_eq!(pg_hypot(26.5, 47.0).unwrap(), 53.95600059307583_f64);
    }

    #[test]
    fn point_inside_square() {
        let square: &[Point] = &[
            Point { x: 0.0, y: 0.0 },
            Point { x: 2.0, y: 0.0 },
            Point { x: 2.0, y: 2.0 },
            Point { x: 0.0, y: 2.0 },
        ];
        assert_eq!(point_inside(&Point { x: 1.0, y: 1.0 }, &square).unwrap(), 1);
        assert_eq!(point_inside(&Point { x: 0.0, y: 1.0 }, &square).unwrap(), 2);
        assert_eq!(point_inside(&Point { x: 5.0, y: 5.0 }, &square).unwrap(), 0);
    }
}

#[cfg(test)]
mod nan_comparator_regression {
    use super::*;

    fn bx(lx: f64, ly: f64, hx: f64, hy: f64) -> BOX {
        BOX { high: Point { x: hx, y: hy }, low: Point { x: lx, y: ly } }
    }

    /// C (gistproc.c adjustBox, since 1acf757255 / bug #14238) uses the
    /// NaN-aware float8_lt/gt, which sort NaN above every non-NaN — so a
    /// NaN addon coordinate is ADOPTED into the union. Raw </> kept the
    /// finite value.
    #[test]
    fn adjust_box_adopts_nan_like_c() {
        let nan = f64::NAN;
        let mut b = bx(0.0, 0.0, 1.0, 1.0);
        adjust_box(&mut b, &bx(0.0, 0.0, nan, 1.0));
        assert!(b.high.x.is_nan(), "NaN high.x must be adopted");

        // low side: float8_gt(low, addon.low) — NaN sorts high, so a NaN
        // low bound is NOT adopted over a finite one, and a finite addon
        // IS adopted over a NaN low.
        let mut b2 = bx(nan, 0.0, 1.0, 1.0);
        adjust_box(&mut b2, &bx(-5.0, 0.0, 1.0, 1.0));
        assert_eq!(b2.low.x, -5.0, "finite low must replace a NaN low");
    }
}
