//! 2-D box routines (geo_ops.c:405-941, 4210-4335, 4534-4560, 5204-5221).
//!
//! Boxes are stored as two opposite corners, with `high` >= `low` on both
//! axes ([`BOX`]). The fmgr shims (`box_in`/`box_out`/`box_recv`/`box_send`)
//! live in [`crate::io`].

use ::types_core::geo::{Point, BOX, CIRCLE, LSEG};
use ::types_error::PgResult;

use crate::f8::{float8_div, float8_gt, float8_max, float8_mi, float8_min, float8_mul, float8_pl};
use crate::lseg::statlseg_construct;
use crate::point::{
    point_add_point, point_div_point, point_dt, point_eq_point, point_mul_point, point_sub_point,
};
use crate::{FPeq, FPge, FPgt, FPle, FPlt};

// ---------------------------------------------------------------------------
// Construction (geo_ops.c:517).
// ---------------------------------------------------------------------------

/// `box_construct(result, pt1, pt2)` (geo_ops.c:517): build a box with sorted
/// corners.
pub fn box_construct(pt1: &Point, pt2: &Point) -> BOX {
    let mut result = BOX::default();
    if float8_gt(pt1.x, pt2.x) {
        result.high.x = pt1.x;
        result.low.x = pt2.x;
    } else {
        result.high.x = pt2.x;
        result.low.x = pt1.x;
    }
    if float8_gt(pt1.y, pt2.y) {
        result.high.y = pt1.y;
        result.low.y = pt2.y;
    } else {
        result.high.y = pt2.y;
        result.low.y = pt1.y;
    }
    result
}

/// `points_box(p1, p2)` (geo_ops.c:4216): SQL `box(point, point)`.
pub fn points_box(p1: &Point, p2: &Point) -> BOX {
    box_construct(p1, p2)
}

// ---------------------------------------------------------------------------
// Relational operators (geo_ops.c:548-786).
// ---------------------------------------------------------------------------

/// `box_same()` (geo_ops.c:550): are the two boxes identical?
pub fn box_same(box1: &BOX, box2: &BOX) -> bool {
    point_eq_point(&box1.high, &box2.high) && point_eq_point(&box1.low, &box2.low)
}

/// `box_overlap()` (geo_ops.c:562) / `box_ov()` (geo_ops.c:571).
pub fn box_overlap(box1: &BOX, box2: &BOX) -> bool {
    box_ov(box1, box2)
}

/// `box_ov(box1, box2)` (geo_ops.c:571): do the boxes overlap?
pub fn box_ov(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.low.x, box2.high.x)
        && FPle(box2.low.x, box1.high.x)
        && FPle(box1.low.y, box2.high.y)
        && FPle(box2.low.y, box1.high.y)
}

/// `box_left()` (geo_ops.c:582).
pub fn box_left(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.x, box2.low.x)
}

/// `box_overleft()` (geo_ops.c:597).
pub fn box_overleft(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.x, box2.high.x)
}

/// `box_right()` (geo_ops.c:608).
pub fn box_right(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.x, box2.high.x)
}

/// `box_overright()` (geo_ops.c:623).
pub fn box_overright(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.x, box2.low.x)
}

/// `box_below()` (geo_ops.c:634).
pub fn box_below(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.y, box2.low.y)
}

/// `box_overbelow()` (geo_ops.c:646).
pub fn box_overbelow(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.y, box2.high.y)
}

/// `box_above()` (geo_ops.c:657).
pub fn box_above(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.y, box2.high.y)
}

/// `box_overabove()` (geo_ops.c:669).
pub fn box_overabove(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.y, box2.low.y)
}

/// `box_contained()` (geo_ops.c:680): is box1 contained by box2?
pub fn box_contained(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box2, box1)
}

/// `box_contain()` (geo_ops.c:691): does box1 contain box2?
pub fn box_contain(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box1, box2)
}

/// `box_contain_box(contains_box, contained_box)` (geo_ops.c:703).
pub fn box_contain_box(contains_box: &BOX, contained_box: &BOX) -> bool {
    FPge(contains_box.high.x, contained_box.high.x)
        && FPle(contains_box.low.x, contained_box.low.x)
        && FPge(contains_box.high.y, contained_box.high.y)
        && FPle(contains_box.low.y, contained_box.low.y)
}

/// `box_below_eq()` (geo_ops.c:721): obsolete (accepts equal boundaries).
pub fn box_below_eq(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.y, box2.low.y)
}

/// `box_above_eq()` (geo_ops.c:730): obsolete (accepts equal boundaries).
pub fn box_above_eq(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.y, box2.high.y)
}

/// `box_lt()` (geo_ops.c:744): compares by area.
pub fn box_lt(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPlt(box_ar(box1)?, box_ar(box2)?))
}

/// `box_gt()` (geo_ops.c:752).
pub fn box_gt(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPgt(box_ar(box1)?, box_ar(box2)?))
}

/// `box_eq()` (geo_ops.c:761).
pub fn box_eq(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPeq(box_ar(box1)?, box_ar(box2)?))
}

/// `box_le()` (geo_ops.c:770).
pub fn box_le(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPle(box_ar(box1)?, box_ar(box2)?))
}

/// `box_ge()` (geo_ops.c:779).
pub fn box_ge(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPge(box_ar(box1)?, box_ar(box2)?))
}

// ---------------------------------------------------------------------------
// "Arithmetic" / accessor operators (geo_ops.c:793-896).
// ---------------------------------------------------------------------------

/// `box_area()` (geo_ops.c:795) / `box_ar()` (geo_ops.c:862).
pub fn box_area(b: &BOX) -> PgResult<f64> {
    box_ar(b)
}

/// `box_ar(box)` (geo_ops.c:862): area of the box.
pub fn box_ar(b: &BOX) -> PgResult<f64> {
    float8_mul(box_wd(b)?, box_ht(b)?)
}

/// `box_width()` (geo_ops.c:807) / `box_wd()` (geo_ops.c:882).
pub fn box_width(b: &BOX) -> PgResult<f64> {
    box_wd(b)
}

/// `box_wd(box)` (geo_ops.c:882): horizontal magnitude.
pub fn box_wd(b: &BOX) -> PgResult<f64> {
    float8_mi(b.high.x, b.low.x)
}

/// `box_height()` (geo_ops.c:819) / `box_ht()` (geo_ops.c:892).
pub fn box_height(b: &BOX) -> PgResult<f64> {
    box_ht(b)
}

/// `box_ht(box)` (geo_ops.c:892): vertical magnitude.
pub fn box_ht(b: &BOX) -> PgResult<f64> {
    float8_mi(b.high.y, b.low.y)
}

/// `box_distance()` (geo_ops.c:831): distance between the boxes' centers.
pub fn box_distance(box1: &BOX, box2: &BOX) -> PgResult<f64> {
    let a = box_cn(box1)?;
    let b = box_cn(box2)?;
    point_dt(&a, &b)
}

/// `box_center()` (geo_ops.c:848) / `box_cn()` (geo_ops.c:871).
pub fn box_center(b: &BOX) -> PgResult<Point> {
    box_cn(b)
}

/// `box_cn(center, box)` (geo_ops.c:871): center point of the box.
pub fn box_cn(b: &BOX) -> PgResult<Point> {
    Ok(Point {
        x: float8_div(float8_pl(b.high.x, b.low.x)?, 2.0)?,
        y: float8_div(float8_pl(b.high.y, b.low.y)?, 2.0)?,
    })
}

// ---------------------------------------------------------------------------
// Funky operations (geo_ops.c:903-941).
// ---------------------------------------------------------------------------

/// `box_intersect()` (geo_ops.c:907): overlapping portion of two boxes, or
/// `None` if disjoint.
pub fn box_intersect(box1: &BOX, box2: &BOX) -> Option<BOX> {
    if !box_ov(box1, box2) {
        return None;
    }
    Some(BOX {
        high: Point {
            x: float8_min(box1.high.x, box2.high.x),
            y: float8_min(box1.high.y, box2.high.y),
        },
        low: Point {
            x: float8_max(box1.low.x, box2.low.x),
            y: float8_max(box1.low.y, box2.low.y),
        },
    })
}

/// `box_diagonal()` (geo_ops.c:932): positive-slope diagonal of the box.
pub fn box_diagonal(b: &BOX) -> LSEG {
    statlseg_construct(&b.high, &b.low)
}

// ---------------------------------------------------------------------------
// Containment of point / lseg (geo_ops.c:3129, 3216).
// ---------------------------------------------------------------------------

/// `box_contain_point(box, point)` (geo_ops.c:3129): is the point in/on the
/// box? (Uses plain `>=`/`<=`, not the fuzzy comparisons.)
pub fn box_contain_point(b: &BOX, point: &Point) -> bool {
    b.high.x >= point.x && b.low.x <= point.x && b.high.y >= point.y && b.low.y <= point.y
}

/// `box_contain_lseg(box, lseg)` (geo_ops.c:3216): are both endpoints in/on
/// the box?
pub fn box_contain_lseg(b: &BOX, lseg: &LSEG) -> bool {
    box_contain_point(b, &lseg.p[0]) && box_contain_point(b, &lseg.p[1])
}

// ---------------------------------------------------------------------------
// Translation / scaling operators (geo_ops.c:4230-4335).
// ---------------------------------------------------------------------------

/// `box_add()` (geo_ops.c:4230): translate a box by a point.
pub fn box_add(b: &BOX, p: &Point) -> PgResult<BOX> {
    Ok(BOX {
        high: point_add_point(&b.high, p)?,
        low: point_add_point(&b.low, p)?,
    })
}

/// `box_sub()` (geo_ops.c:4245): translate a box by `-point`.
pub fn box_sub(b: &BOX, p: &Point) -> PgResult<BOX> {
    Ok(BOX {
        high: point_sub_point(&b.high, p)?,
        low: point_sub_point(&b.low, p)?,
    })
}

/// `box_mul()` (geo_ops.c:4260): rotate / scale a box by a point.
pub fn box_mul(b: &BOX, p: &Point) -> PgResult<BOX> {
    let high = point_mul_point(&b.high, p)?;
    let low = point_mul_point(&b.low, p)?;
    Ok(box_construct(&high, &low))
}

/// `box_div()` (geo_ops.c:4279): rotate / scale a box by `1/point`.
pub fn box_div(b: &BOX, p: &Point) -> PgResult<BOX> {
    let high = point_div_point(&b.high, p)?;
    let low = point_div_point(&b.low, p)?;
    Ok(box_construct(&high, &low))
}

/// `point_box()` (geo_ops.c:4301): convert a point to an empty box.
pub fn point_box(pt: &Point) -> BOX {
    BOX {
        high: *pt,
        low: *pt,
    }
}

/// `boxes_bound_box()` (geo_ops.c:4320): smallest box containing both boxes.
pub fn boxes_bound_box(box1: &BOX, box2: &BOX) -> BOX {
    BOX {
        high: Point {
            x: float8_max(box1.high.x, box2.high.x),
            y: float8_max(box1.high.y, box2.high.y),
        },
        low: Point {
            x: float8_min(box1.low.x, box2.low.x),
            y: float8_min(box1.low.y, box2.low.y),
        },
    }
}

// ---------------------------------------------------------------------------
// Conversions box <-> circle (geo_ops.c:5186-5221).
// ---------------------------------------------------------------------------

/// `circle_box()` (geo_ops.c:5186): the largest box inscribed in the circle.
pub fn circle_box(circle: &CIRCLE) -> PgResult<BOX> {
    let delta = float8_div(circle.radius, 2.0_f64.sqrt())?;
    Ok(BOX {
        high: Point {
            x: float8_pl(circle.center.x, delta)?,
            y: float8_pl(circle.center.y, delta)?,
        },
        low: Point {
            x: float8_mi(circle.center.x, delta)?,
            y: float8_mi(circle.center.y, delta)?,
        },
    })
}

/// `box_circle()` (geo_ops.c:5207): the smallest circle circumscribing the box.
pub fn box_circle(b: &BOX) -> PgResult<CIRCLE> {
    let center = Point {
        x: float8_div(float8_pl(b.high.x, b.low.x)?, 2.0)?,
        y: float8_div(float8_pl(b.high.y, b.low.y)?, 2.0)?,
    };
    let radius = point_dt(&center, &b.high)?;
    Ok(CIRCLE { center, radius })
}
