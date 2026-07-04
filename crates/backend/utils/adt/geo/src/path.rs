use ::adt_float::{float8_div, float8_lt, float8_max, float8_mi, float8_min, float8_mul, float8_pl};
use ::types_core::geo::{Point, BOX, PATH_HEADER_SIZE};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use crate::lseg::{lseg_interpt_lseg, statlseg_construct};
use crate::proximity::lseg_closept_lseg;
use crate::{box_ov, point_dt, PathRef, Pts, POINT_SIZE};

pub fn path_isclosed(path: &PathRef<'_>) -> bool {
    path.closed
}

pub fn path_isopen(path: &PathRef<'_>) -> bool {
    !path.closed
}

pub fn path_npoints(path: &PathRef<'_>) -> i32 {
    path.n() as i32
}

// Shoelace |area|/2; None for an open path.
pub fn path_area(path: &PathRef<'_>) -> PgResult<Option<f64>> {
    if !path.closed {
        return Ok(None);
    }
    let n = path.n();
    let mut area = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        let pi = path.pt(i);
        let pj = path.pt(j);
        area = float8_pl(area, float8_mul(pi.x, pj.y)?)?;
        area = float8_mi(area, float8_mul(pi.y, pj.x)?)?;
    }
    Ok(Some(float8_div(area.abs(), 2.0)?))
}

pub fn path_length(path: &PathRef<'_>) -> PgResult<f64> {
    let mut result = 0.0;
    let npts = path.n();
    for i in 0..npts {
        let iprev = if i > 0 {
            i - 1
        } else if !path.closed {
            continue;
        } else {
            npts - 1
        };
        result = float8_pl(result, point_dt(&path.pt(iprev), &path.pt(i))?)?;
    }
    Ok(result)
}

pub fn path_inter(p1: &PathRef<'_>, p2: &PathRef<'_>) -> PgResult<bool> {
    debug_assert!(p1.n() != 0 && p2.n() != 0);

    let b1 = path_bound_box(p1);
    let b2 = path_bound_box(p2);
    if !box_ov(&b1, &b2) {
        return Ok(false);
    }

    for i in 0..p1.n() {
        let iprev = if i > 0 {
            i - 1
        } else if !p1.closed {
            continue;
        } else {
            p1.n() - 1
        };

        for j in 0..p2.n() {
            let jprev = if j > 0 {
                j - 1
            } else if !p2.closed {
                continue;
            } else {
                p2.n() - 1
            };

            let seg1 = statlseg_construct(&p1.pt(iprev), &p1.pt(i));
            let seg2 = statlseg_construct(&p2.pt(jprev), &p2.pt(j));
            if lseg_interpt_lseg(None, &seg1, &seg2)? {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

pub fn path_distance(p1: &PathRef<'_>, p2: &PathRef<'_>) -> PgResult<Option<f64>> {
    let mut min = 0.0;
    let mut have_min = false;

    for i in 0..p1.n() {
        let iprev = if i > 0 {
            i - 1
        } else if !p1.closed {
            continue;
        } else {
            p1.n() - 1
        };

        for j in 0..p2.n() {
            let jprev = if j > 0 {
                j - 1
            } else if !p2.closed {
                continue;
            } else {
                p2.n() - 1
            };

            let seg1 = statlseg_construct(&p1.pt(iprev), &p1.pt(i));
            let seg2 = statlseg_construct(&p2.pt(jprev), &p2.pt(j));
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

fn path_bound_box(p: &PathRef<'_>) -> BOX {
    let p0 = p.pt(0);
    let mut b = BOX { high: p0, low: p0 };
    for i in 1..p.n() {
        let pt = p.pt(i);
        b.high.x = float8_max(pt.x, b.high.x);
        b.high.y = float8_max(pt.y, b.high.y);
        b.low.x = float8_min(pt.x, b.low.x);
        b.low.y = float8_min(pt.y, b.low.y);
    }
    b
}

// path_add's 32-bit overflow guard (C computes size_t but tests int wraparound
// via the same base_size/size relations as path_in).
pub fn path_add_checks(total: usize) -> PgResult<()> {
    let base_size = POINT_SIZE.wrapping_mul(total);
    let size = PATH_HEADER_SIZE.wrapping_add(base_size);
    if base_size / POINT_SIZE != total || size <= base_size {
        return Err(Box::new(
            PgError::error("too many points requested")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    Ok(())
}

#[cold]
pub fn open_path_to_polygon_error() -> Box<PgError> {
    Box::new(
        PgError::error("open path cannot be converted to polygon")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}
