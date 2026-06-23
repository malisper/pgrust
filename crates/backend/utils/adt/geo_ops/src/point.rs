//! 2-D point routines (geo_ops.c:1817-2046, 4089-4207).
//!
//! The fmgr shims (`point_in`/`point_out`/`point_recv`/`point_send`) live in
//! [`crate::io`]; this module has the constructor, the relational operators,
//! the arithmetic operators, and the internal slope / distance helpers.

use types_core::geo::Point;
use types_error::PgResult;

use crate::f8::{float8_div, float8_eq, float8_mi, float8_mul, float8_pl, get_float8_infinity};
use crate::{FPeq, FPgt, FPlt, HYPOT};

// ---------------------------------------------------------------------------
// Constructor (geo_ops.c:1883, 4095).
// ---------------------------------------------------------------------------

/// `point_construct(result, x, y)` (geo_ops.c:1883): initialize a point.
#[inline]
pub fn point_construct(x: f64, y: f64) -> Point {
    Point { x, y }
}

/// `construct_point(x, y)` (geo_ops.c:4095): SQL `point(float8, float8)`.
#[inline]
pub fn construct_point(x: f64, y: f64) -> Point {
    point_construct(x, y)
}

// ---------------------------------------------------------------------------
// Relational operators (geo_ops.c:1900-1985).
// ---------------------------------------------------------------------------

/// `point_left()` (geo_ops.c:1900): is `pt1` strictly left of `pt2`?
#[inline]
pub fn point_left(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.x, pt2.x)
}

/// `point_right()` (geo_ops.c:1909): is `pt1` strictly right of `pt2`?
#[inline]
pub fn point_right(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.x, pt2.x)
}

/// `point_above()` (geo_ops.c:1918): is `pt1` strictly above `pt2`?
#[inline]
pub fn point_above(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.y, pt2.y)
}

/// `point_below()` (geo_ops.c:1927): is `pt1` strictly below `pt2`?
#[inline]
pub fn point_below(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.y, pt2.y)
}

/// `point_vert()` (geo_ops.c:1936): do `pt1` and `pt2` share an x-coordinate?
#[inline]
pub fn point_vert(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.x, pt2.x)
}

/// `point_horiz()` (geo_ops.c:1945): do `pt1` and `pt2` share a y-coordinate?
#[inline]
pub fn point_horiz(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.y, pt2.y)
}

/// `point_eq()` (geo_ops.c:1954).
#[inline]
pub fn point_eq(pt1: &Point, pt2: &Point) -> bool {
    point_eq_point(pt1, pt2)
}

/// `point_ne()` (geo_ops.c:1963).
#[inline]
pub fn point_ne(pt1: &Point, pt2: &Point) -> bool {
    !point_eq_point(pt1, pt2)
}

/// `point_eq_point()` (geo_ops.c:1976): whether the two points are the same.
/// If any NaNs are involved, insist on exact (`float8_eq`) equality; otherwise
/// use the fuzzy `FPeq`.
#[inline]
pub fn point_eq_point(pt1: &Point, pt2: &Point) -> bool {
    if pt1.x.is_nan() || pt1.y.is_nan() || pt2.x.is_nan() || pt2.y.is_nan() {
        return float8_eq(pt1.x, pt2.x) && float8_eq(pt1.y, pt2.y);
    }
    FPeq(pt1.x, pt2.x) && FPeq(pt1.y, pt2.y)
}

// ---------------------------------------------------------------------------
// "Arithmetic" operators (geo_ops.c:1992-2046, 4110-4207).
// ---------------------------------------------------------------------------

/// `point_distance()` (geo_ops.c:1992) / `point_dt()` (geo_ops.c:2001):
/// Euclidean distance between two points.
#[inline]
pub fn point_distance(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    point_dt(pt1, pt2)
}

/// `point_dt()` (geo_ops.c:2001): `HYPOT(pt1.x - pt2.x, pt1.y - pt2.y)`.
#[inline]
pub fn point_dt(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    HYPOT(float8_mi(pt1.x, pt2.x)?, float8_mi(pt1.y, pt2.y)?)
}

/// `point_slope()` (geo_ops.c:2007) / `point_sl()` (geo_ops.c:2022).
#[inline]
pub fn point_slope(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    point_sl(pt1, pt2)
}

/// `point_sl()` (geo_ops.c:2022): slope of the line through two points.
/// Returns `+Inf` when the points share an x-coordinate (i.e. are the same).
#[inline]
pub fn point_sl(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    if FPeq(pt1.x, pt2.x) {
        return Ok(get_float8_infinity());
    }
    if FPeq(pt1.y, pt2.y) {
        return Ok(0.0);
    }
    float8_div(float8_mi(pt1.y, pt2.y)?, float8_mi(pt1.x, pt2.x)?)
}

/// `point_invsl()` (geo_ops.c:2038): inverse slope of the line through two
/// points. Returns `0.0` when the points are the same.
#[inline]
pub fn point_invsl(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    if FPeq(pt1.x, pt2.x) {
        return Ok(0.0);
    }
    if FPeq(pt1.y, pt2.y) {
        return Ok(get_float8_infinity());
    }
    float8_div(float8_mi(pt1.x, pt2.x)?, float8_mi(pt2.y, pt1.y)?)
}

/// `point_add_point(result, pt1, pt2)` (geo_ops.c:4110).
#[inline]
pub fn point_add_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_pl(pt1.x, pt2.x)?,
        float8_pl(pt1.y, pt2.y)?,
    ))
}

/// `point_add()` (geo_ops.c:4118): SQL `point + point`.
#[inline]
pub fn point_add(p1: &Point, p2: &Point) -> PgResult<Point> {
    point_add_point(p1, p2)
}

/// `point_sub_point(result, pt1, pt2)` (geo_ops.c:4133).
#[inline]
pub fn point_sub_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_mi(pt1.x, pt2.x)?,
        float8_mi(pt1.y, pt2.y)?,
    ))
}

/// `point_sub()` (geo_ops.c:4141): SQL `point - point`.
#[inline]
pub fn point_sub(p1: &Point, p2: &Point) -> PgResult<Point> {
    point_sub_point(p1, p2)
}

/// `point_mul_point(result, pt1, pt2)` (geo_ops.c:4156): complex multiplication.
#[inline]
pub fn point_mul_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_mi(float8_mul(pt1.x, pt2.x)?, float8_mul(pt1.y, pt2.y)?)?,
        float8_pl(float8_mul(pt1.x, pt2.y)?, float8_mul(pt1.y, pt2.x)?)?,
    ))
}

/// `point_mul()` (geo_ops.c:4166): SQL `point * point`.
#[inline]
pub fn point_mul(p1: &Point, p2: &Point) -> PgResult<Point> {
    point_mul_point(p1, p2)
}

/// `point_div_point(result, pt1, pt2)` (geo_ops.c:4181): complex division.
#[inline]
pub fn point_div_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    let div = float8_pl(float8_mul(pt2.x, pt2.x)?, float8_mul(pt2.y, pt2.y)?)?;
    Ok(point_construct(
        float8_div(
            float8_pl(float8_mul(pt1.x, pt2.x)?, float8_mul(pt1.y, pt2.y)?)?,
            div,
        )?,
        float8_div(
            float8_mi(float8_mul(pt1.y, pt2.x)?, float8_mul(pt1.x, pt2.y)?)?,
            div,
        )?,
    ))
}

/// `point_div()` (geo_ops.c:4195): SQL `point / point`.
#[inline]
pub fn point_div(p1: &Point, p2: &Point) -> PgResult<Point> {
    point_div_point(p1, p2)
}
