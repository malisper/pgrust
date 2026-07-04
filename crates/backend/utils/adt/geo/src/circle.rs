use ::adt_float::{float8_div, float8_mi, float8_mul, float8_pl};
use ::types_core::geo::{Point, CIRCLE};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
};

use crate::point::{point_add_point, point_div_point, point_mul_point, point_sub_point};
use crate::{pg_hypot, point_dt, point_eq_point, FPeq, FPge, FPgt, FPle, FPlt, FPne, FPzero, M_PI};

pub fn cr_circle(center: &Point, radius: f64) -> CIRCLE {
    CIRCLE {
        center: *center,
        radius,
    }
}

// NaN radii compare equal to each other (geo_ops.c:4754).
pub fn circle_same(c1: &CIRCLE, c2: &CIRCLE) -> bool {
    ((c1.radius.is_nan() && c2.radius.is_nan()) || FPeq(c1.radius, c2.radius))
        && point_eq_point(&c1.center, &c2.center)
}

pub fn circle_overlap(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_pl(c1.radius, c2.radius)?,
    ))
}

pub fn circle_overleft(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        float8_pl(c1.center.x, c1.radius)?,
        float8_pl(c2.center.x, c2.radius)?,
    ))
}

pub fn circle_left(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(
        float8_pl(c1.center.x, c1.radius)?,
        float8_mi(c2.center.x, c2.radius)?,
    ))
}

pub fn circle_right(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(
        float8_mi(c1.center.x, c1.radius)?,
        float8_pl(c2.center.x, c2.radius)?,
    ))
}

pub fn circle_overright(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(
        float8_mi(c1.center.x, c1.radius)?,
        float8_mi(c2.center.x, c2.radius)?,
    ))
}

pub fn circle_contained(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_mi(c2.radius, c1.radius)?,
    ))
}

pub fn circle_contain(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_mi(c1.radius, c2.radius)?,
    ))
}

pub fn circle_below(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(
        float8_pl(c1.center.y, c1.radius)?,
        float8_mi(c2.center.y, c2.radius)?,
    ))
}

pub fn circle_above(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(
        float8_mi(c1.center.y, c1.radius)?,
        float8_pl(c2.center.y, c2.radius)?,
    ))
}

pub fn circle_overbelow(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        float8_pl(c1.center.y, c1.radius)?,
        float8_pl(c2.center.y, c2.radius)?,
    ))
}

pub fn circle_overabove(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(
        float8_mi(c1.center.y, c1.radius)?,
        float8_mi(c2.center.y, c2.radius)?,
    ))
}

pub fn circle_eq(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPeq(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_ne(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPne(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_lt(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_gt(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_le(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_ge(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(circle_ar(c1)?, circle_ar(c2)?))
}

pub fn circle_add_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_add_point(&circle.center, point)?,
        radius: circle.radius,
    })
}

pub fn circle_sub_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_sub_point(&circle.center, point)?,
        radius: circle.radius,
    })
}

pub fn circle_mul_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_mul_point(&circle.center, point)?,
        radius: float8_mul(circle.radius, pg_hypot(point.x, point.y)?)?,
    })
}

pub fn circle_div_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_div_point(&circle.center, point)?,
        radius: float8_div(circle.radius, pg_hypot(point.x, point.y)?)?,
    })
}

pub fn circle_area(circle: &CIRCLE) -> PgResult<f64> {
    circle_ar(circle)
}

pub fn circle_ar(circle: &CIRCLE) -> PgResult<f64> {
    float8_mul(float8_mul(circle.radius, circle.radius)?, M_PI)
}

pub fn circle_diameter(circle: &CIRCLE) -> PgResult<f64> {
    float8_mul(circle.radius, 2.0)
}

pub fn circle_radius(circle: &CIRCLE) -> f64 {
    circle.radius
}

pub fn circle_center(circle: &CIRCLE) -> Point {
    circle.center
}

pub fn circle_distance(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<f64> {
    let result = float8_mi(
        point_dt(&c1.center, &c2.center)?,
        float8_pl(c1.radius, c2.radius)?,
    )?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

pub fn circle_contain_pt(circle: &CIRCLE, point: &Point) -> PgResult<bool> {
    let d = point_dt(&circle.center, point)?;
    Ok(d <= circle.radius)
}

pub fn pt_contained_circle(point: &Point, circle: &CIRCLE) -> PgResult<bool> {
    let d = point_dt(&circle.center, point)?;
    Ok(d <= circle.radius)
}

pub fn circle_poly_checks(npts: i32, circle: &CIRCLE) -> PgResult<()> {
    if FPzero(circle.radius) {
        return Err(Box::new(
            PgError::error("cannot convert circle with radius zero to polygon")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if npts < 2 {
        return Err(Box::new(
            PgError::error("must request at least 2 points")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    crate::io::check_points_overflow(npts, ::types_core::geo::POLYGON_HEADER_SIZE)
}

pub fn circle_poly_vertex(circle: &CIRCLE, anglestep: f64, i: i32) -> PgResult<Point> {
    let angle = float8_mul(anglestep, i as f64)?;
    Ok(Point {
        x: float8_mi(circle.center.x, float8_mul(circle.radius, angle.cos())?)?,
        y: float8_pl(circle.center.y, float8_mul(circle.radius, angle.sin())?)?,
    })
}
