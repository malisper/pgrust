//! 2-D polygon routines (geo_ops.c:3375-3398, 3527-4086, 4493-4591).
//!
//! Polygons are non-intersecting closed paths. The owned [`Polygon`] type
//! stands in for the toastable varlena `POLYGON`; the fmgr shims
//! (`poly_in`/`poly_out`/`poly_recv`/`poly_send`) live in [`crate::io`].

use types_core::geo::{Point, BOX, CIRCLE, LSEG};
use types_error::PgResult;

use postgres_seams::check_for_interrupts;
use stack_depth_seams::check_stack_depth;

use crate::f8::{float8_div, float8_gt, float8_lt, float8_pl};
use crate::lseg::{lseg_contain_point, lseg_interpt_lseg};
use crate::point::{point_add_point, point_dt, point_eq_point};
use crate::{box_contain_box, box_ov, plist_same, point_inside, Polygon};

// ---------------------------------------------------------------------------
// Bounding box (geo_ops.c:3375).
// ---------------------------------------------------------------------------

/// `make_bound_box(poly)` (geo_ops.c:3375): compute the smallest bounding box
/// of the polygon and store it in `poly.boundbox`.
pub fn make_bound_box(poly: &mut Polygon) {
    debug_assert!(!poly.points.is_empty());

    let mut x1 = poly.points[0].x;
    let mut x2 = poly.points[0].x;
    let mut y1 = poly.points[0].y;
    let mut y2 = poly.points[0].y;
    for pt in &poly.points[1..] {
        if float8_lt(pt.x, x1) {
            x1 = pt.x;
        }
        if float8_gt(pt.x, x2) {
            x2 = pt.x;
        }
        if float8_lt(pt.y, y1) {
            y1 = pt.y;
        }
        if float8_gt(pt.y, y2) {
            y2 = pt.y;
        }
    }
    poly.boundbox.low.x = x1;
    poly.boundbox.high.x = x2;
    poly.boundbox.low.y = y1;
    poly.boundbox.high.y = y2;
}

// ---------------------------------------------------------------------------
// Positional comparisons by bounding box (geo_ops.c:3532-3709).
// ---------------------------------------------------------------------------

/// `poly_left()` (geo_ops.c:3532).
pub fn poly_left(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.high.x < b.boundbox.low.x
}

/// `poly_overleft()` (geo_ops.c:3555).
pub fn poly_overleft(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.high.x <= b.boundbox.high.x
}

/// `poly_right()` (geo_ops.c:3578).
pub fn poly_right(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.low.x > b.boundbox.high.x
}

/// `poly_overright()` (geo_ops.c:3601).
pub fn poly_overright(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.low.x >= b.boundbox.low.x
}

/// `poly_below()` (geo_ops.c:3624).
pub fn poly_below(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.high.y < b.boundbox.low.y
}

/// `poly_overbelow()` (geo_ops.c:3647).
pub fn poly_overbelow(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.high.y <= b.boundbox.high.y
}

/// `poly_above()` (geo_ops.c:3670).
pub fn poly_above(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.low.y > b.boundbox.high.y
}

/// `poly_overabove()` (geo_ops.c:3693).
pub fn poly_overabove(a: &Polygon, b: &Polygon) -> bool {
    a.boundbox.low.y >= b.boundbox.low.y
}

// ---------------------------------------------------------------------------
// Same / overlap / contain (geo_ops.c:3719-4004).
// ---------------------------------------------------------------------------

/// `poly_same()` (geo_ops.c:3719): same point sets (rotation/direction
/// independent).
pub fn poly_same(a: &Polygon, b: &Polygon) -> bool {
    if a.npts() != b.npts() {
        false
    } else {
        plist_same(&a.points, &b.points)
    }
}

/// `poly_overlap_internal(polya, polyb)` (geo_ops.c:3743).
pub fn poly_overlap_internal(a: &Polygon, b: &Polygon) -> PgResult<bool> {
    debug_assert!(!a.points.is_empty() && !b.points.is_empty());

    // Quick check by bounding box.
    if !box_ov(&a.boundbox, &b.boundbox) {
        return Ok(false);
    }

    // Brute force: try to find intersecting edges.
    let na = a.points.len();
    let nb = b.points.len();
    let mut sa = LSEG {
        p: [a.points[na - 1], Point::default()],
    };

    for ia in 0..na {
        sa.p[1] = a.points[ia];

        let mut sb = LSEG {
            p: [b.points[nb - 1], Point::default()],
        };
        for ib in 0..nb {
            sb.p[1] = b.points[ib];
            if lseg_interpt_lseg(None, &sa, &sb)? {
                return Ok(true);
            }
            sb.p[0] = sb.p[1];
        }
        sa.p[0] = sa.p[1];
    }

    // No intersecting edges: check whether one polygon is inside the other.
    Ok(point_inside(&a.points[0], &b.points)? != 0 || point_inside(&b.points[0], &a.points)? != 0)
}

/// `poly_overlap()` (geo_ops.c:3800).
pub fn poly_overlap(a: &Polygon, b: &Polygon) -> PgResult<bool> {
    poly_overlap_internal(a, b)
}

/// `touched_lseg_inside_poly(a, b, s, poly, start)` (geo_ops.c:3829).
fn touched_lseg_inside_poly(
    a: &Point,
    b: &Point,
    s: &LSEG,
    poly: &Polygon,
    start: usize,
) -> PgResult<bool> {
    // point a is on s, b is not
    let t = LSEG { p: [*a, *b] };

    if point_eq_point(a, &s.p[0]) {
        if lseg_contain_point(&t, &s.p[1])? {
            return lseg_inside_poly(b, &s.p[1], poly, start);
        }
    } else if point_eq_point(a, &s.p[1]) {
        if lseg_contain_point(&t, &s.p[0])? {
            return lseg_inside_poly(b, &s.p[0], poly, start);
        }
    } else if lseg_contain_point(&t, &s.p[0])? {
        return lseg_inside_poly(b, &s.p[0], poly, start);
    } else if lseg_contain_point(&t, &s.p[1])? {
        return lseg_inside_poly(b, &s.p[1], poly, start);
    }

    Ok(true) // may not be true, but that will be checked later
}

/// `lseg_inside_poly(a, b, poly, start)` (geo_ops.c:3865): is segment (a, b)
/// inside the polygon, checking edges from `start`?
fn lseg_inside_poly(a: &Point, b: &Point, poly: &Polygon, start: usize) -> PgResult<bool> {
    // geo_ops.c:3875 — this function recurses, so guard against stack overflow.
    check_stack_depth::call()?;

    let t = LSEG { p: [*a, *b] };
    let mut res = true;
    let mut intersection = false;

    let npts = poly.points.len();
    let first = if start == 0 { npts - 1 } else { start - 1 };
    let mut s = LSEG {
        p: [poly.points[first], Point::default()],
    };

    let mut i = start;
    while i < npts && res {
        // geo_ops.c:3885 — make the per-edge scan cancelable.
        check_for_interrupts::call()?;

        s.p[1] = poly.points[i];

        if lseg_contain_point(&s, &t.p[0])? {
            if lseg_contain_point(&s, &t.p[1])? {
                return Ok(true); // t is contained by s
            }
            // Y-cross
            res = touched_lseg_inside_poly(&t.p[0], &t.p[1], &s, poly, i + 1)?;
        } else if lseg_contain_point(&s, &t.p[1])? {
            // Y-cross
            res = touched_lseg_inside_poly(&t.p[1], &t.p[0], &s, poly, i + 1)?;
        } else {
            let mut interpt = Point::default();
            if lseg_interpt_lseg(Some(&mut interpt), &t, &s)? {
                // X-crossing: check each subsegment
                intersection = true;
                res = lseg_inside_poly(&t.p[0], &interpt, poly, i + 1)?;
                if res {
                    res = lseg_inside_poly(&t.p[1], &interpt, poly, i + 1)?;
                }
            }
        }

        s.p[0] = s.p[1];
        i += 1;
    }

    if res && !intersection {
        // No X-intersection: check the central point of the tested segment.
        let p = Point {
            x: float8_div(float8_pl(t.p[0].x, t.p[1].x)?, 2.0)?,
            y: float8_div(float8_pl(t.p[0].y, t.p[1].y)?, 2.0)?,
        };
        res = point_inside(&p, &poly.points)? != 0;
    }

    Ok(res)
}

/// `poly_contain_poly(contains_poly, contained_poly)` (geo_ops.c:3937).
pub fn poly_contain_poly(contains: &Polygon, contained: &Polygon) -> PgResult<bool> {
    debug_assert!(!contains.points.is_empty() && !contained.points.is_empty());

    if !box_contain_box(&contains.boundbox, &contained.boundbox) {
        return Ok(false);
    }

    let nb = contained.points.len();
    let mut s = LSEG {
        p: [contained.points[nb - 1], Point::default()],
    };
    for i in 0..nb {
        s.p[1] = contained.points[i];
        if !lseg_inside_poly(&s.p[0], &s.p[1], contains, 0)? {
            return Ok(false);
        }
        s.p[0] = s.p[1];
    }

    Ok(true)
}

/// `poly_contain()` (geo_ops.c:3965): does polya contain polyb?
pub fn poly_contain(a: &Polygon, b: &Polygon) -> PgResult<bool> {
    poly_contain_poly(a, b)
}

/// `poly_contained()` (geo_ops.c:3987): is polya contained by polyb?
pub fn poly_contained(a: &Polygon, b: &Polygon) -> PgResult<bool> {
    poly_contain_poly(b, a)
}

/// `poly_contain_pt()` (geo_ops.c:4007): is the point in/on the polygon?
/// Returns `PgResult` because `point_inside` can raise an overflow (22003) on
/// astronomical coordinates, exactly as the C function's `ereport` does.
pub fn poly_contain_pt(poly: &Polygon, p: &Point) -> PgResult<bool> {
    Ok(point_inside(p, &poly.points)? != 0)
}

/// `pt_contained_poly()` (geo_ops.c:4016): is the point in/on the polygon?
pub fn pt_contained_poly(p: &Point, poly: &Polygon) -> PgResult<bool> {
    Ok(point_inside(p, &poly.points)? != 0)
}

/// Seam body for `poly_contain_pt_image`: decode the in-memory `POLYGON`
/// varlena image (`DatumGetPolygonP`) and apply [`poly_contain_pt`]. Used by the
/// GiST polygon/point opclasses, which carry the query polygon as a raw image.
pub fn poly_contain_pt_image(image: &[u8], p: &Point) -> PgResult<bool> {
    let poly = Polygon::from_datum_image(image);
    poly_contain_pt(&poly, p)
}

/// Seam body for `poly_query_boundbox`: extract the bounding box of an in-memory
/// `POLYGON` varlena image (`(DatumGetPolygonP(q))->boundbox`).
pub fn poly_query_boundbox(image: &[u8]) -> BOX {
    Polygon::from_datum_image(image).boundbox
}

/// `poly_distance()` (geo_ops.c:4026): minimum distance between two polygons
/// (0 if they overlap), or `None` if either is empty.
pub fn poly_distance(a: &Polygon, b: &Polygon) -> PgResult<Option<f64>> {
    use crate::lseg::statlseg_construct;
    use crate::lseg_closept_lseg;

    if poly_overlap_internal(a, b)? {
        return Ok(Some(0.0));
    }

    let mut min = 0.0;
    let mut have_min = false;
    let na = a.points.len();
    let nb = b.points.len();

    for i in 0..na {
        let iprev = if i > 0 { i - 1 } else { na - 1 };
        for j in 0..nb {
            let jprev = if j > 0 { j - 1 } else { nb - 1 };
            let seg1 = statlseg_construct(&a.points[iprev], &a.points[i]);
            let seg2 = statlseg_construct(&b.points[jprev], &b.points[j]);
            let tmp = lseg_closept_lseg(None, &seg1, &seg2)?;
            if !have_min || float8_lt(tmp, min) {
                min = tmp;
                have_min = true;
            }
        }
    }

    if !have_min {
        return Ok(None);
    }
    Ok(Some(min))
}

// ---------------------------------------------------------------------------
// Accessors / conversions (geo_ops.c:4493-4591, 5285-5317).
// ---------------------------------------------------------------------------

/// `poly_npoints()` (geo_ops.c:4493).
pub fn poly_npoints(poly: &Polygon) -> i32 {
    poly.npts()
}

/// `poly_center()` (geo_ops.c:4502): the center of the bounding circle.
pub fn poly_center(poly: &Polygon) -> PgResult<Point> {
    let circle = poly_to_circle(poly)?;
    Ok(circle.center)
}

/// `poly_box()` (geo_ops.c:4518): the polygon's bounding box.
pub fn poly_box(poly: &Polygon) -> BOX {
    poly.boundbox
}

/// `poly_to_circle(result, poly)` (geo_ops.c:5285): the average-of-points
/// center and average-distance radius.
pub fn poly_to_circle(poly: &Polygon) -> PgResult<CIRCLE> {
    debug_assert!(!poly.points.is_empty());

    let npts = poly.points.len() as f64;
    let mut center = Point { x: 0.0, y: 0.0 };
    for p in &poly.points {
        center = point_add_point(&center, p)?;
    }
    center.x = float8_div(center.x, npts)?;
    center.y = float8_div(center.y, npts)?;

    let mut radius = 0.0;
    for p in &poly.points {
        radius = float8_pl(radius, point_dt(p, &center)?)?;
    }
    radius = float8_div(radius, npts)?;

    Ok(CIRCLE { center, radius })
}

/// `poly_circle()` (geo_ops.c:5306): convert a polygon to its bounding circle.
pub fn poly_circle(poly: &Polygon) -> PgResult<CIRCLE> {
    poly_to_circle(poly)
}
