//! 2-D line segment routines (geo_ops.c:2049-2372, 2674-2711, 3108-3114).
//!
//! The fmgr shims (`lseg_in`/`lseg_out`/`lseg_recv`/`lseg_send`) live in
//! [`crate::io`].

use ::types_core::geo::{Point, LINE, LSEG};
use ::types_error::PgResult;

use crate::f8::{float8_div, float8_pl};
use crate::line::{line_construct, line_interpt_line};
use crate::point::{point_dt, point_eq_point, point_invsl, point_sl};
use crate::{FPeq, FPge, FPgt, FPle, FPlt};

// ---------------------------------------------------------------------------
// Construction (geo_ops.c:2128-2148).
// ---------------------------------------------------------------------------

/// `lseg_construct(pt1, pt2)` (geo_ops.c:2128): SQL `lseg(point, point)`.
#[inline]
pub fn lseg_construct(pt1: &Point, pt2: &Point) -> LSEG {
    statlseg_construct(pt1, pt2)
}

/// `statlseg_construct(lseg, pt1, pt2)` (geo_ops.c:2141): form an LSEG from two
/// points (the "already-allocated" variant; here just returns the value).
#[inline]
pub fn statlseg_construct(pt1: &Point, pt2: &Point) -> LSEG {
    LSEG {
        p: [*pt1, *pt2],
    }
}

/// `lseg_sl(lseg)` (geo_ops.c:2154): slope of the line segment.
#[inline]
pub fn lseg_sl(lseg: &LSEG) -> PgResult<f64> {
    point_sl(&lseg.p[0], &lseg.p[1])
}

/// `lseg_invsl(lseg)` (geo_ops.c:2164): inverse slope of the line segment.
#[inline]
pub fn lseg_invsl(lseg: &LSEG) -> PgResult<f64> {
    point_invsl(&lseg.p[0], &lseg.p[1])
}

/// `lseg_length()` (geo_ops.c:2171): length of the line segment.
#[inline]
pub fn lseg_length(lseg: &LSEG) -> PgResult<f64> {
    point_dt(&lseg.p[0], &lseg.p[1])
}

// ---------------------------------------------------------------------------
// Relative position (geo_ops.c:2187-2293).
// ---------------------------------------------------------------------------

/// `lseg_intersect()` (geo_ops.c:2187): do two segments intersect?
pub fn lseg_intersect(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    lseg_interpt_lseg(None, l1, l2)
}

/// `lseg_parallel()` (geo_ops.c:2197).
pub fn lseg_parallel(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPeq(lseg_sl(l1)?, lseg_sl(l2)?))
}

/// `lseg_perp()` (geo_ops.c:2209): are two segments perpendicular?
pub fn lseg_perp(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPeq(lseg_sl(l1)?, lseg_invsl(l2)?))
}

/// `lseg_vertical()` (geo_ops.c:2218).
pub fn lseg_vertical(lseg: &LSEG) -> bool {
    FPeq(lseg.p[0].x, lseg.p[1].x)
}

/// `lseg_horizontal()` (geo_ops.c:2226).
pub fn lseg_horizontal(lseg: &LSEG) -> bool {
    FPeq(lseg.p[0].y, lseg.p[1].y)
}

/// `lseg_eq()` (geo_ops.c:2235).
pub fn lseg_eq(l1: &LSEG, l2: &LSEG) -> bool {
    point_eq_point(&l1.p[0], &l2.p[0]) && point_eq_point(&l1.p[1], &l2.p[1])
}

/// `lseg_ne()` (geo_ops.c:2245).
pub fn lseg_ne(l1: &LSEG, l2: &LSEG) -> bool {
    !point_eq_point(&l1.p[0], &l2.p[0]) || !point_eq_point(&l1.p[1], &l2.p[1])
}

/// `lseg_lt()` (geo_ops.c:2255): compares by length.
pub fn lseg_lt(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPlt(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

/// `lseg_le()` (geo_ops.c:2265).
pub fn lseg_le(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

/// `lseg_gt()` (geo_ops.c:2275).
pub fn lseg_gt(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPgt(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

/// `lseg_ge()` (geo_ops.c:2285).
pub fn lseg_ge(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPge(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

/// `lseg_center()` (geo_ops.c:2315): midpoint of the line segment.
pub fn lseg_center(lseg: &LSEG) -> PgResult<Point> {
    Ok(Point {
        x: float8_div(float8_pl(lseg.p[0].x, lseg.p[1].x)?, 2.0)?,
        y: float8_div(float8_pl(lseg.p[0].y, lseg.p[1].y)?, 2.0)?,
    })
}

// ---------------------------------------------------------------------------
// Intersection points (geo_ops.c:2337-2372, 2674-2711).
// ---------------------------------------------------------------------------

/// `lseg_interpt_lseg(result, l1, l2)` (geo_ops.c:2337): whether two segments
/// intersect; if `result` is `Some`, it is set to the intersection point.
pub fn lseg_interpt_lseg(result: Option<&mut Point>, l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    let tmp = line_construct(&l2.p[0], lseg_sl(l2)?)?;
    let mut interpt = Point::default();
    if !lseg_interpt_line(Some(&mut interpt), l1, &tmp)? {
        return Ok(false);
    }

    // If the line intersection point isn't within l2, there's no valid
    // segment intersection point at all.
    if !lseg_contain_point(l2, &interpt)? {
        return Ok(false);
    }

    if let Some(slot) = result {
        *slot = interpt;
    }

    Ok(true)
}

/// `lseg_interpt()` (geo_ops.c:2360): intersection point of two segments, or
/// `None`.
pub fn lseg_interpt(l1: &LSEG, l2: &LSEG) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if !lseg_interpt_lseg(Some(&mut result), l1, l2)? {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `lseg_interpt_line(result, lseg, line)` (geo_ops.c:2674): whether the line
/// segment intersects the line; if `result` is `Some`, it is set to the
/// intersection point (snapped to a matching endpoint to defeat LSB residue).
pub fn lseg_interpt_line(result: Option<&mut Point>, lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    // Promote the line segment to a line and find the lines' intersection.
    let tmp = line_construct(&lseg.p[0], lseg_sl(lseg)?)?;
    let mut interpt = Point::default();
    if !line_interpt_line(Some(&mut interpt), &tmp, line)? {
        return Ok(false);
    }

    // Check the intersection point is actually on the segment.
    if !lseg_contain_point(lseg, &interpt)? {
        return Ok(false);
    }
    if let Some(slot) = result {
        // Snap to a matching endpoint to avoid LSB residue.
        if point_eq_point(&lseg.p[0], &interpt) {
            *slot = lseg.p[0];
        } else if point_eq_point(&lseg.p[1], &interpt) {
            *slot = lseg.p[1];
        } else {
            *slot = interpt;
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Containment (geo_ops.c:3108).
// ---------------------------------------------------------------------------

/// `lseg_contain_point(lseg, pt)` (geo_ops.c:3108): is the point on the
/// segment? Detected by a triangle-inequality test.
pub fn lseg_contain_point(lseg: &LSEG, pt: &Point) -> PgResult<bool> {
    Ok(FPeq(
        point_dt(pt, &lseg.p[0])? + point_dt(pt, &lseg.p[1])?,
        point_dt(&lseg.p[0], &lseg.p[1])?,
    ))
}
