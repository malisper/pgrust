use ::adt_float::{float8_div, float8_mi, float8_mul, float8_pl, get_float8_infinity};
use ::types_core::geo::Point;
use ::types_error::PgResult;

use crate::{point_eq_point, FPeq, FPgt, FPlt};

#[inline]
pub fn point_construct(x: f64, y: f64) -> Point {
    Point { x, y }
}

#[inline]
pub fn construct_point(x: f64, y: f64) -> Point {
    point_construct(x, y)
}

#[inline]
pub fn point_left(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.x, pt2.x)
}

#[inline]
pub fn point_right(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.x, pt2.x)
}

#[inline]
pub fn point_above(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.y, pt2.y)
}

#[inline]
pub fn point_below(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.y, pt2.y)
}

#[inline]
pub fn point_vert(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.x, pt2.x)
}

#[inline]
pub fn point_horiz(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.y, pt2.y)
}

#[inline]
pub fn point_eq(pt1: &Point, pt2: &Point) -> bool {
    point_eq_point(pt1, pt2)
}

#[inline]
pub fn point_ne(pt1: &Point, pt2: &Point) -> bool {
    !point_eq_point(pt1, pt2)
}

#[inline]
pub fn point_distance(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    crate::point_dt(pt1, pt2)
}

#[inline]
pub fn point_slope(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    point_sl(pt1, pt2)
}

// point_sl: +Inf when the x-coordinates fuzzily match (vertical).
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

#[inline]
pub fn point_add_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_pl(pt1.x, pt2.x)?,
        float8_pl(pt1.y, pt2.y)?,
    ))
}

#[inline]
pub fn point_sub_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_mi(pt1.x, pt2.x)?,
        float8_mi(pt1.y, pt2.y)?,
    ))
}

// Complex multiplication.
#[inline]
pub fn point_mul_point(pt1: &Point, pt2: &Point) -> PgResult<Point> {
    Ok(point_construct(
        float8_mi(float8_mul(pt1.x, pt2.x)?, float8_mul(pt1.y, pt2.y)?)?,
        float8_pl(float8_mul(pt1.x, pt2.y)?, float8_mul(pt1.y, pt2.x)?)?,
    ))
}

// Complex division; division by (0,0) raises 22012 via float8_div.
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
