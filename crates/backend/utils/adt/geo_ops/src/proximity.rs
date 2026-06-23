//! Cross-type proximity / position routines (geo_ops.c:2381-3362, 5105-5138).
//!
//! These implement the `dist_*` (minimum distance), `close_*` (closest point),
//! `on_*` (containment), and `inter_*` (intersection-test) SQL operators across
//! differently-typed objects, plus the internal closest-point helpers
//! (`lseg_closept_*`, `box_closept_*`, `box_interpt_lseg`).

use types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};
use types_error::PgResult;

use crate::boxes::{box_cn, box_contain_lseg, box_contain_point, box_ov};
use crate::f8::{float8_lt, float8_max, float8_mi, float8_min};
use crate::line::line_construct;
use crate::line::{line_closept_point, line_contain_point, line_sl};
use crate::lseg::{
    lseg_contain_point, lseg_interpt_line, lseg_interpt_lseg, lseg_sl, statlseg_construct,
};
use crate::point::{point_dt, point_invsl};
use crate::{point_inside, FPeq, Path, Polygon};

// ===========================================================================
// dist_  -- minimum distance from one object to another (geo_ops.c:2389-2660).
// ===========================================================================

/// `dist_pl()` (geo_ops.c:2389) / `dist_lp()` (geo_ops.c:2401): point<->line.
pub fn dist_pl(pt: &Point, line: &LINE) -> PgResult<f64> {
    line_closept_point(None, line, pt)
}

/// `dist_lp()` (geo_ops.c:2401).
pub fn dist_lp(line: &LINE, pt: &Point) -> PgResult<f64> {
    line_closept_point(None, line, pt)
}

/// `dist_ps()` (geo_ops.c:2413) / `dist_sp()` (geo_ops.c:2425): point<->lseg.
pub fn dist_ps(pt: &Point, lseg: &LSEG) -> PgResult<f64> {
    lseg_closept_point(None, lseg, pt)
}

/// `dist_sp()` (geo_ops.c:2425).
pub fn dist_sp(lseg: &LSEG, pt: &Point) -> PgResult<f64> {
    lseg_closept_point(None, lseg, pt)
}

/// `dist_ppath_internal(pt, path)` (geo_ops.c:2434).
fn dist_ppath_internal(pt: &Point, path: &Path) -> PgResult<f64> {
    debug_assert!(!path.points.is_empty());

    let mut result = 0.0;
    let mut have_min = false;
    let npts = path.points.len();
    for i in 0..npts {
        let iprev = if i > 0 {
            i - 1
        } else if !path.closed {
            continue;
        } else {
            npts - 1
        };
        let lseg = statlseg_construct(&path.points[iprev], &path.points[i]);
        let tmp = lseg_closept_point(None, &lseg, pt)?;
        if !have_min || float8_lt(tmp, result) {
            result = tmp;
            have_min = true;
        }
    }
    Ok(result)
}

/// `dist_ppath()` (geo_ops.c:2477) / `dist_pathp()` (geo_ops.c:2489).
pub fn dist_ppath(pt: &Point, path: &Path) -> PgResult<f64> {
    dist_ppath_internal(pt, path)
}

/// `dist_pathp()` (geo_ops.c:2489).
pub fn dist_pathp(path: &Path, pt: &Point) -> PgResult<f64> {
    dist_ppath_internal(pt, path)
}

/// `dist_pb()` (geo_ops.c:2501) / `dist_bp()` (geo_ops.c:2513): point<->box.
pub fn dist_pb(pt: &Point, b: &BOX) -> PgResult<f64> {
    box_closept_point(None, b, pt)
}

/// `dist_bp()` (geo_ops.c:2513).
pub fn dist_bp(b: &BOX, pt: &Point) -> PgResult<f64> {
    box_closept_point(None, b, pt)
}

/// `dist_sl()` (geo_ops.c:2525) / `dist_ls()` (geo_ops.c:2537): lseg<->line.
pub fn dist_sl(lseg: &LSEG, line: &LINE) -> PgResult<f64> {
    lseg_closept_line(None, lseg, line)
}

/// `dist_ls()` (geo_ops.c:2537).
pub fn dist_ls(line: &LINE, lseg: &LSEG) -> PgResult<f64> {
    lseg_closept_line(None, lseg, line)
}

/// `dist_sb()` (geo_ops.c:2549) / `dist_bs()` (geo_ops.c:2561): lseg<->box.
pub fn dist_sb(lseg: &LSEG, b: &BOX) -> PgResult<f64> {
    box_closept_lseg(None, b, lseg)
}

/// `dist_bs()` (geo_ops.c:2561).
pub fn dist_bs(b: &BOX, lseg: &LSEG) -> PgResult<f64> {
    box_closept_lseg(None, b, lseg)
}

/// `dist_cpoly_internal(circle, poly)` (geo_ops.c:2570).
fn dist_cpoly_internal(circle: &CIRCLE, poly: &Polygon) -> PgResult<f64> {
    let result = float8_mi(dist_ppoly_internal(&circle.center, poly)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

/// `dist_cpoly()` (geo_ops.c:2587) / `dist_polyc()` (geo_ops.c:2599):
/// circle<->polygon.
pub fn dist_cpoly(circle: &CIRCLE, poly: &Polygon) -> PgResult<f64> {
    dist_cpoly_internal(circle, poly)
}

/// `dist_polyc()` (geo_ops.c:2599).
pub fn dist_polyc(poly: &Polygon, circle: &CIRCLE) -> PgResult<f64> {
    dist_cpoly_internal(circle, poly)
}

/// `dist_ppoly()` (geo_ops.c:2611) / `dist_polyp()` (geo_ops.c:2620):
/// point<->polygon.
pub fn dist_ppoly(point: &Point, poly: &Polygon) -> PgResult<f64> {
    dist_ppoly_internal(point, poly)
}

/// `dist_polyp()` (geo_ops.c:2620).
pub fn dist_polyp(poly: &Polygon, point: &Point) -> PgResult<f64> {
    dist_ppoly_internal(point, poly)
}

/// `dist_ppoly_internal(pt, poly)` (geo_ops.c:2629).
fn dist_ppoly_internal(pt: &Point, poly: &Polygon) -> PgResult<f64> {
    if point_inside(pt, &poly.points)? != 0 {
        return Ok(0.0);
    }

    let npts = poly.points.len();
    // Start with the closure segment between the first and last points.
    let seg = LSEG {
        p: [poly.points[0], poly.points[npts - 1]],
    };
    let mut result = lseg_closept_point(None, &seg, pt)?;

    for i in 0..npts - 1 {
        let seg = LSEG {
            p: [poly.points[i], poly.points[i + 1]],
        };
        let d = lseg_closept_point(None, &seg, pt)?;
        if float8_lt(d, result) {
            result = d;
        }
    }

    Ok(result)
}

/// `dist_pc()` (geo_ops.c:5108) / `dist_cpoint()` (geo_ops.c:5126):
/// point<->circle.
pub fn dist_pc(point: &Point, circle: &CIRCLE) -> PgResult<f64> {
    let result = float8_mi(point_dt(point, &circle.center)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

/// `dist_cpoint()` (geo_ops.c:5126).
pub fn dist_cpoint(circle: &CIRCLE, point: &Point) -> PgResult<f64> {
    let result = float8_mi(point_dt(point, &circle.center)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

/// `lseg_distance()` (geo_ops.c:2305): distance between two line segments.
pub fn lseg_distance(l1: &LSEG, l2: &LSEG) -> PgResult<f64> {
    lseg_closept_lseg(None, l1, l2)
}

// ===========================================================================
// close_  -- closest point between objects (geo_ops.c:2749-3075).
// ===========================================================================

/// `lseg_closept_point(result, lseg, pt)` (geo_ops.c:2771): closest point on
/// the segment to `pt`; returns the distance.
pub fn lseg_closept_point(result: Option<&mut Point>, lseg: &LSEG, pt: &Point) -> PgResult<f64> {
    let tmp = line_construct(pt, point_invsl(&lseg.p[0], &lseg.p[1])?)?;
    let mut closept = Point::default();
    lseg_closept_line(Some(&mut closept), lseg, &tmp)?;

    if let Some(slot) = result {
        *slot = closept;
    }

    point_dt(&closept, pt)
}

/// `lseg_closept_line(result, lseg, line)` (geo_ops.c:2959): closest point on
/// the segment to the line; returns the distance.
pub fn lseg_closept_line(
    mut result: Option<&mut Point>,
    lseg: &LSEG,
    line: &LINE,
) -> PgResult<f64> {
    // The C code passes `result` straight through to lseg_interpt_line.
    if lseg_interpt_line(reborrow(&mut result), lseg, line)? {
        return Ok(0.0);
    }

    let dist1 = line_closept_point(None, line, &lseg.p[0])?;
    let dist2 = line_closept_point(None, line, &lseg.p[1])?;

    if dist1 < dist2 {
        if let Some(slot) = result {
            *slot = lseg.p[0];
        }
        Ok(dist1)
    } else {
        if let Some(slot) = result {
            *slot = lseg.p[1];
        }
        Ok(dist2)
    }
}

/// `lseg_closept_lseg(result, on_lseg, to_lseg)` (geo_ops.c:2809): closest
/// point on `on_lseg` to `to_lseg`; returns the distance.
pub fn lseg_closept_lseg(
    mut result: Option<&mut Point>,
    on_lseg: &LSEG,
    to_lseg: &LSEG,
) -> PgResult<f64> {
    // If the segments intersect, the closest point is the intersection.
    if lseg_interpt_lseg(reborrow(&mut result), on_lseg, to_lseg)? {
        return Ok(0.0);
    }

    // Closest points from each endpoint of `to_lseg`.
    let mut dist = lseg_closept_point(reborrow(&mut result), on_lseg, &to_lseg.p[0])?;
    let mut point = Point::default();
    let d = lseg_closept_point(Some(&mut point), on_lseg, &to_lseg.p[1])?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = point;
        }
    }

    // The closest point can still be one of the endpoints; test them.
    let d = lseg_closept_point(None, to_lseg, &on_lseg.p[0])?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = on_lseg.p[0];
        }
    }
    let d = lseg_closept_point(None, to_lseg, &on_lseg.p[1])?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result {
            *slot = on_lseg.p[1];
        }
    }

    Ok(dist)
}

/// Re-borrow an `Option<&mut Point>` for a nested call without moving it.
#[inline]
fn reborrow<'a>(r: &'a mut Option<&mut Point>) -> Option<&'a mut Point> {
    r.as_deref_mut()
}

/// `box_closept_point(result, box, pt)` (geo_ops.c:2877): closest point on/in
/// the box to `pt`; returns the distance.
pub fn box_closept_point(mut result: Option<&mut Point>, b: &BOX, pt: &Point) -> PgResult<f64> {
    if box_contain_point(b, pt) {
        if let Some(slot) = result.as_deref_mut() {
            *slot = *pt;
        }
        return Ok(0.0);
    }

    // pairwise check lseg distances
    let mut point = Point {
        x: b.low.x,
        y: b.high.y,
    };
    let lseg = statlseg_construct(&b.low, &point);
    let mut dist = lseg_closept_point(reborrow(&mut result), &lseg, pt)?;

    let lseg = statlseg_construct(&b.high, &point);
    let mut closept = Point::default();
    let d = lseg_closept_point(Some(&mut closept), &lseg, pt)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = closept;
        }
    }

    point.x = b.high.x;
    point.y = b.low.y;
    let lseg = statlseg_construct(&b.low, &point);
    let d = lseg_closept_point(Some(&mut closept), &lseg, pt)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = closept;
        }
    }

    let lseg = statlseg_construct(&b.high, &point);
    let d = lseg_closept_point(Some(&mut closept), &lseg, pt)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result {
            *slot = closept;
        }
    }

    Ok(dist)
}

/// `box_closept_lseg(result, box, lseg)` (geo_ops.c:3012): closest point on/in
/// the box to the line segment; returns the distance.
pub fn box_closept_lseg(mut result: Option<&mut Point>, b: &BOX, lseg: &LSEG) -> PgResult<f64> {
    if box_interpt_lseg(reborrow(&mut result), b, lseg)? {
        return Ok(0.0);
    }

    let mut point = Point {
        x: b.low.x,
        y: b.high.y,
    };
    let bseg = statlseg_construct(&b.low, &point);
    let mut dist = lseg_closept_lseg(reborrow(&mut result), &bseg, lseg)?;

    let bseg = statlseg_construct(&b.high, &point);
    let mut closept = Point::default();
    let d = lseg_closept_lseg(Some(&mut closept), &bseg, lseg)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = closept;
        }
    }

    point.x = b.high.x;
    point.y = b.low.y;
    let bseg = statlseg_construct(&b.low, &point);
    let d = lseg_closept_lseg(Some(&mut closept), &bseg, lseg)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = closept;
        }
    }

    let bseg = statlseg_construct(&b.high, &point);
    let d = lseg_closept_lseg(Some(&mut closept), &bseg, lseg)?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result {
            *slot = closept;
        }
    }

    Ok(dist)
}

/// `close_pl()` (geo_ops.c:2749): closest point on the line to a point, or
/// `None` if the perpendicular drop fails (NaN distance).
pub fn close_pl(pt: &Point, line: &LINE) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if line_closept_point(Some(&mut result), line, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `close_ps()` (geo_ops.c:2790): closest point on the segment to a point.
pub fn close_ps(pt: &Point, lseg: &LSEG) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if lseg_closept_point(Some(&mut result), lseg, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `close_lseg()` (geo_ops.c:2852): closest point between two segments, or
/// `None` if they are parallel (or NaN distance).
pub fn close_lseg(l1: &LSEG, l2: &LSEG) -> PgResult<Option<Point>> {
    if lseg_sl(l1)? == lseg_sl(l2)? {
        return Ok(None);
    }
    let mut result = Point::default();
    if lseg_closept_lseg(Some(&mut result), l2, l1)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `close_pb()` (geo_ops.c:2932): closest point on the box to a point.
pub fn close_pb(pt: &Point, b: &BOX) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if box_closept_point(Some(&mut result), b, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `close_ls()` (geo_ops.c:2987): closest point on the segment to the line, or
/// `None` if they are parallel.
pub fn close_ls(line: &LINE, lseg: &LSEG) -> PgResult<Option<Point>> {
    if lseg_sl(lseg)? == line_sl(line)? {
        return Ok(None);
    }
    let mut result = Point::default();
    if lseg_closept_line(Some(&mut result), lseg, line)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `close_sb()` (geo_ops.c:3062): closest point on the box to the segment.
pub fn close_sb(lseg: &LSEG, b: &BOX) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if box_closept_lseg(Some(&mut result), b, lseg)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

// ===========================================================================
// on_  -- whether one object lies within another (geo_ops.c:3094-3230).
// ===========================================================================

/// `on_pl()` (geo_ops.c:3094): is the point on the line?
pub fn on_pl(pt: &Point, line: &LINE) -> PgResult<bool> {
    line_contain_point(line, pt)
}

/// `on_ps()` (geo_ops.c:3116): is the point on the segment?
pub fn on_ps(pt: &Point, lseg: &LSEG) -> PgResult<bool> {
    lseg_contain_point(lseg, pt)
}

/// `on_pb()` (geo_ops.c:3136) / `box_contain_pt()` (geo_ops.c:3145):
/// point in/on box.
pub fn on_pb(pt: &Point, b: &BOX) -> bool {
    box_contain_point(b, pt)
}

/// `box_contain_pt()` (geo_ops.c:3145).
pub fn box_contain_pt(b: &BOX, pt: &Point) -> bool {
    box_contain_point(b, pt)
}

/// `on_ppath()` (geo_ops.c:3165): is the point on the polyline?
pub fn on_ppath(pt: &Point, path: &Path) -> PgResult<bool> {
    if !path.closed {
        // OPEN: check each segment via the triangle-inequality test.
        let n = path.points.len() - 1;
        let mut a = point_dt(pt, &path.points[0])?;
        for i in 0..n {
            let b = point_dt(pt, &path.points[i + 1])?;
            if FPeq(a + b, point_dt(&path.points[i], &path.points[i + 1])?) {
                return Ok(true);
            }
            a = b;
        }
        return Ok(false);
    }

    // CLOSED: ray method.
    Ok(point_inside(pt, &path.points)? != 0)
}

/// `on_sl()` (geo_ops.c:3200): is the segment on the line?
pub fn on_sl(lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    Ok(line_contain_point(line, &lseg.p[0])? && line_contain_point(line, &lseg.p[1])?)
}

/// `on_sb()` (geo_ops.c:3223): is the segment in/on the box?
pub fn on_sb(lseg: &LSEG, b: &BOX) -> bool {
    box_contain_lseg(b, lseg)
}

// ===========================================================================
// inter_  -- whether one object intersects another (geo_ops.c:3237-3362).
// ===========================================================================

/// `inter_sl()` (geo_ops.c:3237): does the segment intersect the line?
pub fn inter_sl(lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    lseg_interpt_line(None, lseg, line)
}

/// `box_interpt_lseg(result, box, lseg)` (geo_ops.c:3262): do the box and
/// segment intersect? Sets `result` to the closest point on the segment to the
/// box center when they overlap.
pub fn box_interpt_lseg(result: Option<&mut Point>, b: &BOX, lseg: &LSEG) -> PgResult<bool> {
    let lbox = BOX {
        low: Point {
            x: float8_min(lseg.p[0].x, lseg.p[1].x),
            y: float8_min(lseg.p[0].y, lseg.p[1].y),
        },
        high: Point {
            x: float8_max(lseg.p[0].x, lseg.p[1].x),
            y: float8_max(lseg.p[0].y, lseg.p[1].y),
        },
    };

    // Nothing close to overlap? then not going to intersect.
    if !box_ov(&lbox, b) {
        return Ok(false);
    }

    if result.is_some() {
        let center = box_cn(b)?;
        let mut p = Point::default();
        lseg_closept_point(Some(&mut p), lseg, &center)?;
        if let Some(slot) = result {
            *slot = p;
        }
    }

    // An endpoint of segment inside box? then clearly intersects.
    if box_contain_point(b, &lseg.p[0]) || box_contain_point(b, &lseg.p[1]) {
        return Ok(true);
    }

    // pairwise check lseg intersections
    let mut point = Point {
        x: b.low.x,
        y: b.high.y,
    };
    let bseg = statlseg_construct(&b.low, &point);
    if lseg_interpt_lseg(None, &bseg, lseg)? {
        return Ok(true);
    }

    let bseg = statlseg_construct(&b.high, &point);
    if lseg_interpt_lseg(None, &bseg, lseg)? {
        return Ok(true);
    }

    point.x = b.high.x;
    point.y = b.low.y;
    let bseg = statlseg_construct(&b.low, &point);
    if lseg_interpt_lseg(None, &bseg, lseg)? {
        return Ok(true);
    }

    let bseg = statlseg_construct(&b.high, &point);
    if lseg_interpt_lseg(None, &bseg, lseg)? {
        return Ok(true);
    }

    Ok(false)
}

/// `inter_sb()` (geo_ops.c:3314): does the segment intersect the box?
pub fn inter_sb(lseg: &LSEG, b: &BOX) -> PgResult<bool> {
    box_interpt_lseg(None, b, lseg)
}

/// `inter_lb()` (geo_ops.c:3327): does the line intersect the box?
pub fn inter_lb(line: &LINE, b: &BOX) -> PgResult<bool> {
    // pairwise check lseg intersections
    let mut p1 = Point {
        x: b.low.x,
        y: b.low.y,
    };
    let mut p2 = Point {
        x: b.low.x,
        y: b.high.y,
    };
    let bseg = statlseg_construct(&p1, &p2);
    if lseg_interpt_line(None, &bseg, line)? {
        return Ok(true);
    }
    p1.x = b.high.x;
    p1.y = b.high.y;
    let bseg = statlseg_construct(&p1, &p2);
    if lseg_interpt_line(None, &bseg, line)? {
        return Ok(true);
    }
    p2.x = b.high.x;
    p2.y = b.low.y;
    let bseg = statlseg_construct(&p1, &p2);
    if lseg_interpt_line(None, &bseg, line)? {
        return Ok(true);
    }
    p1.x = b.low.x;
    p1.y = b.low.y;
    let bseg = statlseg_construct(&p1, &p2);
    if lseg_interpt_line(None, &bseg, line)? {
        return Ok(true);
    }

    Ok(false)
}
