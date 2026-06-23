//! 2-D path routines (geo_ops.c:1379-1815, 4344-4484).
//!
//! Paths are sequences of line segments ("polylines"). The owned [`Path`]
//! type stands in for the toastable varlena `PATH`; the fmgr shims
//! (`path_in`/`path_out`/`path_recv`/`path_send`) live in [`crate::io`].

use types_core::geo::{Point, BOX};
use types_error::{PgError, PgResult};

use crate::f8::{float8_div, float8_lt, float8_max, float8_mi, float8_min, float8_mul, float8_pl};
use crate::lseg::{lseg_interpt_lseg, statlseg_construct};
use crate::point::{point_add_point, point_div_point, point_dt, point_mul_point, point_sub_point};
use crate::poly::make_bound_box;
use crate::{box_ov, errcode_program_limit, lseg_closept_lseg, Path, Polygon};

// ---------------------------------------------------------------------------
// Accessors / relational operators (geo_ops.c:1552-1623).
// ---------------------------------------------------------------------------

/// `path_n_lt()` (geo_ops.c:1552): compares by point count.
pub fn path_n_lt(p1: &Path, p2: &Path) -> bool {
    p1.npts() < p2.npts()
}

/// `path_n_gt()` (geo_ops.c:1561).
pub fn path_n_gt(p1: &Path, p2: &Path) -> bool {
    p1.npts() > p2.npts()
}

/// `path_n_eq()` (geo_ops.c:1570).
pub fn path_n_eq(p1: &Path, p2: &Path) -> bool {
    p1.npts() == p2.npts()
}

/// `path_n_le()` (geo_ops.c:1579).
pub fn path_n_le(p1: &Path, p2: &Path) -> bool {
    p1.npts() <= p2.npts()
}

/// `path_n_ge()` (geo_ops.c:1588).
pub fn path_n_ge(p1: &Path, p2: &Path) -> bool {
    p1.npts() >= p2.npts()
}

/// `path_isclosed()` (geo_ops.c:1601).
pub fn path_isclosed(path: &Path) -> bool {
    path.closed
}

/// `path_isopen()` (geo_ops.c:1609).
pub fn path_isopen(path: &Path) -> bool {
    !path.closed
}

/// `path_npoints()` (geo_ops.c:1617).
pub fn path_npoints(path: &Path) -> i32 {
    path.npts()
}

/// `path_close()` (geo_ops.c:1626): a closed copy of the path.
pub fn path_close(path: &Path) -> Path {
    let mut p = path.clone();
    p.closed = true;
    p
}

/// `path_open()` (geo_ops.c:1636): an open copy of the path.
pub fn path_open(path: &Path) -> Path {
    let mut p = path.clone();
    p.closed = false;
    p
}

// ---------------------------------------------------------------------------
// Area / length / distance / intersection (geo_ops.c:1379, 1652-1815).
// ---------------------------------------------------------------------------

/// `path_area()` (geo_ops.c:1379): signed area of a closed path (shoelace),
/// or `None` for an open path.
pub fn path_area(path: &Path) -> PgResult<Option<f64>> {
    if !path.closed {
        return Ok(None);
    }
    let n = path.points.len();
    let mut area = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        area = float8_pl(area, float8_mul(path.points[i].x, path.points[j].y)?)?;
        area = float8_mi(area, float8_mul(path.points[i].y, path.points[j].x)?)?;
    }
    Ok(Some(float8_div(area.abs(), 2.0)?))
}

/// `path_length()` (geo_ops.c:1791): total length (including the closure
/// segment when closed).
pub fn path_length(path: &Path) -> PgResult<f64> {
    let mut result = 0.0;
    let npts = path.points.len();
    for i in 0..npts {
        let iprev = if i > 0 {
            i - 1
        } else if !path.closed {
            continue;
        } else {
            npts - 1
        };
        result = float8_pl(result, point_dt(&path.points[iprev], &path.points[i])?)?;
    }
    Ok(result)
}

/// `path_inter()` (geo_ops.c:1652): do `p1` and `p2` intersect anywhere?
pub fn path_inter(p1: &Path, p2: &Path) -> PgResult<bool> {
    debug_assert!(!p1.points.is_empty() && !p2.points.is_empty());

    let b1 = path_bound_box(p1);
    let b2 = path_bound_box(p2);
    if !box_ov(&b1, &b2) {
        return Ok(false);
    }

    // pairwise check lseg intersections
    for i in 0..p1.points.len() {
        let iprev = if i > 0 {
            i - 1
        } else if !p1.closed {
            continue;
        } else {
            p1.points.len() - 1
        };

        for j in 0..p2.points.len() {
            let jprev = if j > 0 {
                j - 1
            } else if !p2.closed {
                continue;
            } else {
                p2.points.len() - 1
            };

            let seg1 = statlseg_construct(&p1.points[iprev], &p1.points[i]);
            let seg2 = statlseg_construct(&p2.points[jprev], &p2.points[j]);
            if lseg_interpt_lseg(None, &seg1, &seg2)? {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// `path_distance()` (geo_ops.c:1729): minimum distance between any two of the
/// paths' segments, or `None` if neither path has any segment.
pub fn path_distance(p1: &Path, p2: &Path) -> PgResult<Option<f64>> {
    let mut min = 0.0;
    let mut have_min = false;

    for i in 0..p1.points.len() {
        let iprev = if i > 0 {
            i - 1
        } else if !p1.closed {
            continue;
        } else {
            p1.points.len() - 1
        };

        for j in 0..p2.points.len() {
            let jprev = if j > 0 {
                j - 1
            } else if !p2.closed {
                continue;
            } else {
                p2.points.len() - 1
            };

            let seg1 = statlseg_construct(&p1.points[iprev], &p1.points[i]);
            let seg2 = statlseg_construct(&p2.points[jprev], &p2.points[j]);
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

/// Internal: bounding box of a path's points (the inline body in `path_inter`,
/// geo_ops.c:1666).
fn path_bound_box(p: &Path) -> BOX {
    let mut b = BOX {
        high: p.points[0],
        low: p.points[0],
    };
    for pt in &p.points[1..] {
        b.high.x = float8_max(pt.x, b.high.x);
        b.high.y = float8_max(pt.y, b.high.y);
        b.low.x = float8_min(pt.x, b.low.x);
        b.low.y = float8_min(pt.y, b.low.y);
    }
    b
}

// ---------------------------------------------------------------------------
// "Arithmetic" / conversion operators (geo_ops.c:4347-4484).
// ---------------------------------------------------------------------------

/// `path_add()` (geo_ops.c:4347): concatenate two open paths, or `None` if
/// either is closed. Raises "too many points requested" (54000) on overflow.
pub fn path_add(p1: &Path, p2: &Path) -> PgResult<Option<Path>> {
    if p1.closed || p2.closed {
        return Ok(None);
    }

    let total = p1.points.len() + p2.points.len();
    // Check for integer overflow (matches the C base_size computation).
    let point_size = core::mem::size_of::<Point>();
    let base_size = point_size.wrapping_mul(total);
    let size = types_core::geo::PATH_HEADER_SIZE.wrapping_add(base_size);
    if base_size / point_size != total || size <= base_size {
        return Err(
            PgError::error("too many points requested").with_sqlstate(errcode_program_limit()),
        );
    }

    let mut points: Vec<Point> = Vec::with_capacity(total);
    points.extend(p1.points.iter().copied());
    points.extend(p2.points.iter().copied());

    Ok(Some(Path {
        closed: p1.closed,
        points,
    }))
}

/// `path_add_pt()` (geo_ops.c:4395): translate the path by a point.
pub fn path_add_pt(path: &Path, point: &Point) -> PgResult<Path> {
    let mut p = path.clone();
    for pt in &mut p.points {
        *pt = point_add_point(pt, point)?;
    }
    Ok(p)
}

/// `path_sub_pt()` (geo_ops.c:4408): translate the path by `-point`.
pub fn path_sub_pt(path: &Path, point: &Point) -> PgResult<Path> {
    let mut p = path.clone();
    for pt in &mut p.points {
        *pt = point_sub_point(pt, point)?;
    }
    Ok(p)
}

/// `path_mul_pt()` (geo_ops.c:4424): rotate / scale the path by a point.
pub fn path_mul_pt(path: &Path, point: &Point) -> PgResult<Path> {
    let mut p = path.clone();
    for pt in &mut p.points {
        *pt = point_mul_point(pt, point)?;
    }
    Ok(p)
}

/// `path_div_pt()` (geo_ops.c:4437): rotate / scale the path by `1/point`.
pub fn path_div_pt(path: &Path, point: &Point) -> PgResult<Path> {
    let mut p = path.clone();
    for pt in &mut p.points {
        *pt = point_div_point(pt, point)?;
    }
    Ok(p)
}

/// `path_poly()` (geo_ops.c:4451): convert a closed path to a polygon. Raises
/// `ERRCODE_INVALID_PARAMETER_VALUE` (22023) for an open path.
pub fn path_poly(path: &Path) -> PgResult<Polygon> {
    if !path.closed {
        return Err(PgError::error("open path cannot be converted to polygon")
            .with_sqlstate(crate::errcode_invalid_parameter()));
    }
    let mut poly = Polygon {
        boundbox: BOX::default(),
        points: path.points.clone(),
    };
    make_bound_box(&mut poly);
    Ok(poly)
}

/// `poly_path()` (geo_ops.c:4563): convert a polygon to a closed path.
pub fn poly_path(poly: &Polygon) -> Path {
    Path {
        closed: true,
        points: poly.points.clone(),
    }
}
