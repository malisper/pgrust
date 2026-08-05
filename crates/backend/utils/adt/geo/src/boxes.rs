use ::adt_float::{float8_div, float8_gt, float8_max, float8_mi, float8_min, float8_mul, float8_pl};
use ::types_core::geo::{Point, BOX, CIRCLE, LSEG};
use ::types_error::PgResult;

use crate::lseg::statlseg_construct;
use crate::point::{point_add_point, point_div_point, point_mul_point, point_sub_point};
use crate::{box_contain_box, box_contain_point, box_ov, point_dt, point_eq_point, FPeq, FPge, FPgt, FPle, FPlt};

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

pub fn points_box(p1: &Point, p2: &Point) -> BOX {
    box_construct(p1, p2)
}

pub fn box_same(box1: &BOX, box2: &BOX) -> bool {
    point_eq_point(&box1.high, &box2.high) && point_eq_point(&box1.low, &box2.low)
}

pub fn box_overlap(box1: &BOX, box2: &BOX) -> bool {
    box_ov(box1, box2)
}

pub fn box_left(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.x, box2.low.x)
}

pub fn box_overleft(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.x, box2.high.x)
}

pub fn box_right(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.x, box2.high.x)
}

pub fn box_overright(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.x, box2.low.x)
}

pub fn box_below(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.y, box2.low.y)
}

pub fn box_overbelow(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.y, box2.high.y)
}

pub fn box_above(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.y, box2.high.y)
}

pub fn box_overabove(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.y, box2.low.y)
}

pub fn box_contained(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box2, box1)
}

pub fn box_contain(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box1, box2)
}

pub fn box_below_eq(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.y, box2.low.y)
}

pub fn box_above_eq(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.y, box2.high.y)
}

pub fn box_lt(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPlt(box_ar(box1)?, box_ar(box2)?))
}

pub fn box_gt(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPgt(box_ar(box1)?, box_ar(box2)?))
}

pub fn box_eq(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPeq(box_ar(box1)?, box_ar(box2)?))
}

pub fn box_le(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPle(box_ar(box1)?, box_ar(box2)?))
}

pub fn box_ge(box1: &BOX, box2: &BOX) -> PgResult<bool> {
    Ok(FPge(box_ar(box1)?, box_ar(box2)?))
}

pub fn box_area(b: &BOX) -> PgResult<f64> {
    box_ar(b)
}

pub fn box_ar(b: &BOX) -> PgResult<f64> {
    float8_mul(box_wd(b)?, box_ht(b)?)
}

pub fn box_width(b: &BOX) -> PgResult<f64> {
    box_wd(b)
}

pub fn box_wd(b: &BOX) -> PgResult<f64> {
    float8_mi(b.high.x, b.low.x)
}

pub fn box_height(b: &BOX) -> PgResult<f64> {
    box_ht(b)
}

pub fn box_ht(b: &BOX) -> PgResult<f64> {
    float8_mi(b.high.y, b.low.y)
}

pub fn box_distance(box1: &BOX, box2: &BOX) -> PgResult<f64> {
    let a = box_cn(box1)?;
    let b = box_cn(box2)?;
    point_dt(&a, &b)
}

pub fn box_center(b: &BOX) -> PgResult<Point> {
    box_cn(b)
}

pub fn box_cn(b: &BOX) -> PgResult<Point> {
    Ok(Point {
        x: float8_div(float8_pl(b.high.x, b.low.x)?, 2.0)?,
        y: float8_div(float8_pl(b.high.y, b.low.y)?, 2.0)?,
    })
}

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

pub fn box_diagonal(b: &BOX) -> LSEG {
    statlseg_construct(&b.high, &b.low)
}

pub fn box_contain_lseg(b: &BOX, lseg: &LSEG) -> bool {
    box_contain_point(b, &lseg.p[0]) && box_contain_point(b, &lseg.p[1])
}

pub fn box_add(b: &BOX, p: &Point) -> PgResult<BOX> {
    Ok(BOX {
        high: point_add_point(&b.high, p)?,
        low: point_add_point(&b.low, p)?,
    })
}

pub fn box_sub(b: &BOX, p: &Point) -> PgResult<BOX> {
    Ok(BOX {
        high: point_sub_point(&b.high, p)?,
        low: point_sub_point(&b.low, p)?,
    })
}

pub fn box_mul(b: &BOX, p: &Point) -> PgResult<BOX> {
    let high = point_mul_point(&b.high, p)?;
    let low = point_mul_point(&b.low, p)?;
    Ok(box_construct(&high, &low))
}

pub fn box_div(b: &BOX, p: &Point) -> PgResult<BOX> {
    let high = point_div_point(&b.high, p)?;
    let low = point_div_point(&b.low, p)?;
    Ok(box_construct(&high, &low))
}

pub fn point_box(pt: &Point) -> BOX {
    BOX { high: *pt, low: *pt }
}

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

pub fn box_circle(b: &BOX) -> PgResult<CIRCLE> {
    let center = Point {
        x: float8_div(float8_pl(b.high.x, b.low.x)?, 2.0)?,
        y: float8_div(float8_pl(b.high.y, b.low.y)?, 2.0)?,
    };
    let radius = point_dt(&center, &b.high)?;
    Ok(CIRCLE { center, radius })
}
