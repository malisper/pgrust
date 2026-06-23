//! 2-D circle routines (geo_ops.c:4594-5317).
//!
//! The fmgr shims (`circle_in`/`circle_out`/`circle_recv`/`circle_send`) live
//! in [`crate::io`]; `box_circle`/`circle_box`/`box_poly` are in
//! [`crate::boxes`] / [`crate::io`].

use ::types_core::geo::{Point, BOX, CIRCLE};
use ::types_error::{PgError, PgResult};

use crate::f8::{float8_div, float8_mi, float8_mul, float8_pl};
use crate::poly::make_bound_box;
use crate::point::{
    point_add_point, point_div_point, point_dt, point_eq_point, point_mul_point, point_sub_point,
};
use crate::{
    errcode_feature_not_supported, errcode_invalid_parameter, FPeq, FPge, FPgt, FPle, FPlt, FPne,
    FPzero, Polygon, HYPOT, M_PI,
};

// ---------------------------------------------------------------------------
// Constructor (geo_ops.c:5169).
// ---------------------------------------------------------------------------

/// `cr_circle(center, radius)` (geo_ops.c:5169): SQL `circle(point, float8)`.
pub fn cr_circle(center: &Point, radius: f64) -> CIRCLE {
    CIRCLE {
        center: *center,
        radius,
    }
}

// ---------------------------------------------------------------------------
// Relational operators (geo_ops.c:4750-4954).
// ---------------------------------------------------------------------------

/// `circle_same()` (geo_ops.c:4750): NaN radii are equal to each other.
pub fn circle_same(c1: &CIRCLE, c2: &CIRCLE) -> bool {
    ((c1.radius.is_nan() && c2.radius.is_nan()) || FPeq(c1.radius, c2.radius))
        && point_eq_point(&c1.center, &c2.center)
}

/// `circle_overlap()` (geo_ops.c:4763).
pub fn circle_overlap(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_pl(c1.radius, c2.radius)?,
    ))
}

/// `circle_overleft()` (geo_ops.c:4776).
pub fn circle_overleft(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        float8_pl(c1.center.x, c1.radius)?,
        float8_pl(c2.center.x, c2.radius)?,
    ))
}

/// `circle_left()` (geo_ops.c:4788).
pub fn circle_left(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(
        float8_pl(c1.center.x, c1.radius)?,
        float8_mi(c2.center.x, c2.radius)?,
    ))
}

/// `circle_right()` (geo_ops.c:4800).
pub fn circle_right(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(
        float8_mi(c1.center.x, c1.radius)?,
        float8_pl(c2.center.x, c2.radius)?,
    ))
}

/// `circle_overright()` (geo_ops.c:4813).
pub fn circle_overright(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(
        float8_mi(c1.center.x, c1.radius)?,
        float8_mi(c2.center.x, c2.radius)?,
    ))
}

/// `circle_contained()` (geo_ops.c:4825): is c1 contained by c2?
pub fn circle_contained(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_mi(c2.radius, c1.radius)?,
    ))
}

/// `circle_contain()` (geo_ops.c:4837): does c1 contain c2?
pub fn circle_contain(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&c1.center, &c2.center)?,
        float8_mi(c1.radius, c2.radius)?,
    ))
}

/// `circle_below()` (geo_ops.c:4850).
pub fn circle_below(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(
        float8_pl(c1.center.y, c1.radius)?,
        float8_mi(c2.center.y, c2.radius)?,
    ))
}

/// `circle_above()` (geo_ops.c:4862).
pub fn circle_above(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(
        float8_mi(c1.center.y, c1.radius)?,
        float8_pl(c2.center.y, c2.radius)?,
    ))
}

/// `circle_overbelow()` (geo_ops.c:4875).
pub fn circle_overbelow(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(
        float8_pl(c1.center.y, c1.radius)?,
        float8_pl(c2.center.y, c2.radius)?,
    ))
}

/// `circle_overabove()` (geo_ops.c:4888).
pub fn circle_overabove(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(
        float8_mi(c1.center.y, c1.radius)?,
        float8_mi(c2.center.y, c2.radius)?,
    ))
}

/// `circle_eq()` (geo_ops.c:4902): compares by area.
pub fn circle_eq(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPeq(circle_ar(c1)?, circle_ar(c2)?))
}

/// `circle_ne()` (geo_ops.c:4911).
pub fn circle_ne(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPne(circle_ar(c1)?, circle_ar(c2)?))
}

/// `circle_lt()` (geo_ops.c:4920).
pub fn circle_lt(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPlt(circle_ar(c1)?, circle_ar(c2)?))
}

/// `circle_gt()` (geo_ops.c:4929).
pub fn circle_gt(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPgt(circle_ar(c1)?, circle_ar(c2)?))
}

/// `circle_le()` (geo_ops.c:4938).
pub fn circle_le(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPle(circle_ar(c1)?, circle_ar(c2)?))
}

/// `circle_ge()` (geo_ops.c:4947).
pub fn circle_ge(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<bool> {
    Ok(FPge(circle_ar(c1)?, circle_ar(c2)?))
}

// ---------------------------------------------------------------------------
// "Arithmetic" operators (geo_ops.c:4964-5026).
// ---------------------------------------------------------------------------

/// `circle_add_pt()` (geo_ops.c:4964): translate the circle by a point.
pub fn circle_add_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_add_point(&circle.center, point)?,
        radius: circle.radius,
    })
}

/// `circle_sub_pt()` (geo_ops.c:4979): translate the circle by `-point`.
pub fn circle_sub_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_sub_point(&circle.center, point)?,
        radius: circle.radius,
    })
}

/// `circle_mul_pt()` (geo_ops.c:4998): rotate / scale the circle by a point.
pub fn circle_mul_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_mul_point(&circle.center, point)?,
        radius: float8_mul(circle.radius, HYPOT(point.x, point.y)?)?,
    })
}

/// `circle_div_pt()` (geo_ops.c:5013): rotate / scale the circle by `1/point`.
pub fn circle_div_pt(circle: &CIRCLE, point: &Point) -> PgResult<CIRCLE> {
    Ok(CIRCLE {
        center: point_div_point(&circle.center, point)?,
        radius: float8_div(circle.radius, HYPOT(point.x, point.y)?)?,
    })
}

// ---------------------------------------------------------------------------
// Accessors / distance (geo_ops.c:5031-5153).
// ---------------------------------------------------------------------------

/// `circle_area()` (geo_ops.c:5031) / `circle_ar()` (geo_ops.c:5158).
pub fn circle_area(circle: &CIRCLE) -> PgResult<f64> {
    circle_ar(circle)
}

/// `circle_ar(circle)` (geo_ops.c:5158): `pi * r^2`.
pub fn circle_ar(circle: &CIRCLE) -> PgResult<f64> {
    float8_mul(float8_mul(circle.radius, circle.radius)?, M_PI)
}

/// `circle_diameter()` (geo_ops.c:5042).
pub fn circle_diameter(circle: &CIRCLE) -> PgResult<f64> {
    float8_mul(circle.radius, 2.0)
}

/// `circle_radius()` (geo_ops.c:5053).
pub fn circle_radius(circle: &CIRCLE) -> f64 {
    circle.radius
}

/// `circle_center()` (geo_ops.c:5142).
pub fn circle_center(circle: &CIRCLE) -> Point {
    circle.center
}

/// `circle_distance()` (geo_ops.c:5065): edge-to-edge distance (0 if they
/// overlap).
pub fn circle_distance(c1: &CIRCLE, c2: &CIRCLE) -> PgResult<f64> {
    let result = float8_mi(
        point_dt(&c1.center, &c2.center)?,
        float8_pl(c1.radius, c2.radius)?,
    )?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

/// `circle_contain_pt()` (geo_ops.c:5081): is the point in/on the circle?
pub fn circle_contain_pt(circle: &CIRCLE, point: &Point) -> PgResult<bool> {
    let d = point_dt(&circle.center, point)?;
    Ok(d <= circle.radius)
}

/// `pt_contained_circle()` (geo_ops.c:5093): is the point in/on the circle?
pub fn pt_contained_circle(point: &Point, circle: &CIRCLE) -> PgResult<bool> {
    let d = point_dt(&circle.center, point)?;
    Ok(d <= circle.radius)
}

// ---------------------------------------------------------------------------
// Conversion: circle -> polygon (geo_ops.c:5224).
// ---------------------------------------------------------------------------

/// `circle_poly(npts, circle)` (geo_ops.c:5224): approximate the circle with an
/// `npts`-vertex polygon. Raises 0A000 (radius zero), 22023 (`npts < 2`), or
/// 54000 (overflow).
pub fn circle_poly(npts: i32, circle: &CIRCLE) -> PgResult<Polygon> {
    if FPzero(circle.radius) {
        return Err(
            PgError::error("cannot convert circle with radius zero to polygon")
                .with_sqlstate(errcode_feature_not_supported()),
        );
    }

    if npts < 2 {
        return Err(PgError::error("must request at least 2 points")
            .with_sqlstate(errcode_invalid_parameter()));
    }

    // Check for integer overflow (matches the C 32-bit `int` base_size/size
    // computation; see `io::check_points_overflow`). `npts >= 2` here, so the
    // `usize` cast is well-defined.
    let n = npts as usize;
    crate::io::check_points_overflow(n, ::types_core::geo::POLYGON_HEADER_SIZE)?;

    let anglestep = float8_div(2.0 * M_PI, npts as f64)?;

    let mut points: Vec<Point> = Vec::with_capacity(n);
    for i in 0..npts {
        points.push(circle_poly_vertex(circle, anglestep, i)?);
    }

    let mut poly = Polygon {
        boundbox: BOX::default(),
        points,
    };
    make_bound_box(&mut poly);
    Ok(poly)
}

/// Compute the `i`-th vertex of the circle approximation (the body of the
/// `circle_poly` loop), routing through the overflow-checked float seams.
fn circle_poly_vertex(circle: &CIRCLE, anglestep: f64, i: i32) -> PgResult<Point> {
    let angle = float8_mul(anglestep, i as f64)?;
    Ok(Point {
        x: float8_mi(circle.center.x, float8_mul(circle.radius, angle.cos())?)?,
        y: float8_pl(circle.center.y, float8_mul(circle.radius, angle.sin())?)?,
    })
}
