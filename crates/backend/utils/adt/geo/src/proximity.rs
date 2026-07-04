use ::adt_float::{float8_lt, float8_max, float8_mi, float8_min};
use ::types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};
use ::types_error::PgResult;

use crate::boxes::{box_cn, box_contain_lseg};
use crate::line::{line_closept_point, line_contain_point, line_construct, line_sl};
use crate::lseg::{lseg_contain_point, lseg_interpt_line, lseg_interpt_lseg, lseg_sl, statlseg_construct};
use crate::point::point_invsl;
use crate::{box_contain_point, box_ov, point_dt, point_inside, FPeq, PathRef, PolyRef, Pts};

pub fn dist_pl(pt: &Point, line: &LINE) -> PgResult<f64> {
    line_closept_point(None, line, pt)
}

pub fn dist_lp(line: &LINE, pt: &Point) -> PgResult<f64> {
    line_closept_point(None, line, pt)
}

pub fn dist_ps(pt: &Point, lseg: &LSEG) -> PgResult<f64> {
    lseg_closept_point(None, lseg, pt)
}

pub fn dist_sp(lseg: &LSEG, pt: &Point) -> PgResult<f64> {
    lseg_closept_point(None, lseg, pt)
}

fn dist_ppath_internal(pt: &Point, path: &PathRef<'_>) -> PgResult<f64> {
    debug_assert!(path.n() != 0);

    let mut result = 0.0;
    let mut have_min = false;
    let npts = path.n();
    for i in 0..npts {
        let iprev = if i > 0 {
            i - 1
        } else if !path.closed {
            continue;
        } else {
            npts - 1
        };
        let lseg = statlseg_construct(&path.pt(iprev), &path.pt(i));
        let tmp = lseg_closept_point(None, &lseg, pt)?;
        if !have_min || float8_lt(tmp, result) {
            result = tmp;
            have_min = true;
        }
    }
    Ok(result)
}

pub fn dist_ppath(pt: &Point, path: &PathRef<'_>) -> PgResult<f64> {
    dist_ppath_internal(pt, path)
}

pub fn dist_pathp(path: &PathRef<'_>, pt: &Point) -> PgResult<f64> {
    dist_ppath_internal(pt, path)
}

pub fn dist_pb(pt: &Point, b: &BOX) -> PgResult<f64> {
    box_closept_point(None, b, pt)
}

pub fn dist_bp(b: &BOX, pt: &Point) -> PgResult<f64> {
    box_closept_point(None, b, pt)
}

pub fn dist_sl(lseg: &LSEG, line: &LINE) -> PgResult<f64> {
    lseg_closept_line(None, lseg, line)
}

pub fn dist_ls(line: &LINE, lseg: &LSEG) -> PgResult<f64> {
    lseg_closept_line(None, lseg, line)
}

pub fn dist_sb(lseg: &LSEG, b: &BOX) -> PgResult<f64> {
    box_closept_lseg(None, b, lseg)
}

pub fn dist_bs(b: &BOX, lseg: &LSEG) -> PgResult<f64> {
    box_closept_lseg(None, b, lseg)
}

fn dist_cpoly_internal(circle: &CIRCLE, poly: &PolyRef<'_>) -> PgResult<f64> {
    let result = float8_mi(dist_ppoly_internal(&circle.center, poly)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

pub fn dist_cpoly(circle: &CIRCLE, poly: &PolyRef<'_>) -> PgResult<f64> {
    dist_cpoly_internal(circle, poly)
}

pub fn dist_polyc(poly: &PolyRef<'_>, circle: &CIRCLE) -> PgResult<f64> {
    dist_cpoly_internal(circle, poly)
}

pub fn dist_ppoly(point: &Point, poly: &PolyRef<'_>) -> PgResult<f64> {
    dist_ppoly_internal(point, poly)
}

pub fn dist_polyp(poly: &PolyRef<'_>, point: &Point) -> PgResult<f64> {
    dist_ppoly_internal(point, poly)
}

fn dist_ppoly_internal(pt: &Point, poly: &PolyRef<'_>) -> PgResult<f64> {
    if point_inside(pt, poly)? != 0 {
        return Ok(0.0);
    }

    let npts = poly.n();
    let seg = LSEG {
        p: [poly.pt(0), poly.pt(npts - 1)],
    };
    let mut result = lseg_closept_point(None, &seg, pt)?;

    for i in 0..npts - 1 {
        let seg = LSEG {
            p: [poly.pt(i), poly.pt(i + 1)],
        };
        let d = lseg_closept_point(None, &seg, pt)?;
        if float8_lt(d, result) {
            result = d;
        }
    }

    Ok(result)
}

pub fn dist_pc(point: &Point, circle: &CIRCLE) -> PgResult<f64> {
    let result = float8_mi(point_dt(point, &circle.center)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

pub fn dist_cpoint(circle: &CIRCLE, point: &Point) -> PgResult<f64> {
    let result = float8_mi(point_dt(point, &circle.center)?, circle.radius)?;
    Ok(if result < 0.0 { 0.0 } else { result })
}

pub fn lseg_distance(l1: &LSEG, l2: &LSEG) -> PgResult<f64> {
    lseg_closept_lseg(None, l1, l2)
}

pub fn lseg_closept_point(result: Option<&mut Point>, lseg: &LSEG, pt: &Point) -> PgResult<f64> {
    let tmp = line_construct(pt, point_invsl(&lseg.p[0], &lseg.p[1])?)?;
    let mut closept = Point::default();
    lseg_closept_line(Some(&mut closept), lseg, &tmp)?;

    if let Some(slot) = result {
        *slot = closept;
    }

    point_dt(&closept, pt)
}

pub fn lseg_closept_line(
    mut result: Option<&mut Point>,
    lseg: &LSEG,
    line: &LINE,
) -> PgResult<f64> {
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

pub fn lseg_closept_lseg(
    mut result: Option<&mut Point>,
    on_lseg: &LSEG,
    to_lseg: &LSEG,
) -> PgResult<f64> {
    if lseg_interpt_lseg(reborrow(&mut result), on_lseg, to_lseg)? {
        return Ok(0.0);
    }

    let mut dist = lseg_closept_point(reborrow(&mut result), on_lseg, &to_lseg.p[0])?;
    let mut point = Point::default();
    let d = lseg_closept_point(Some(&mut point), on_lseg, &to_lseg.p[1])?;
    if float8_lt(d, dist) {
        dist = d;
        if let Some(slot) = result.as_deref_mut() {
            *slot = point;
        }
    }

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

#[inline]
fn reborrow<'a>(r: &'a mut Option<&mut Point>) -> Option<&'a mut Point> {
    r.as_deref_mut()
}

pub fn box_closept_point(mut result: Option<&mut Point>, b: &BOX, pt: &Point) -> PgResult<f64> {
    if box_contain_point(b, pt) {
        if let Some(slot) = result.as_deref_mut() {
            *slot = *pt;
        }
        return Ok(0.0);
    }

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

pub fn close_pl(pt: &Point, line: &LINE) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if line_closept_point(Some(&mut result), line, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

pub fn close_ps(pt: &Point, lseg: &LSEG) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if lseg_closept_point(Some(&mut result), lseg, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

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

pub fn close_pb(pt: &Point, b: &BOX) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if box_closept_point(Some(&mut result), b, pt)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

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

pub fn close_sb(lseg: &LSEG, b: &BOX) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if box_closept_lseg(Some(&mut result), b, lseg)?.is_nan() {
        return Ok(None);
    }
    Ok(Some(result))
}

pub fn on_pl(pt: &Point, line: &LINE) -> PgResult<bool> {
    line_contain_point(line, pt)
}

pub fn on_ps(pt: &Point, lseg: &LSEG) -> PgResult<bool> {
    lseg_contain_point(lseg, pt)
}

pub fn on_pb(pt: &Point, b: &BOX) -> bool {
    box_contain_point(b, pt)
}

pub fn box_contain_pt(b: &BOX, pt: &Point) -> bool {
    box_contain_point(b, pt)
}

pub fn on_ppath(pt: &Point, path: &PathRef<'_>) -> PgResult<bool> {
    if !path.closed {
        let n = path.n() - 1;
        let mut a = point_dt(pt, &path.pt(0))?;
        for i in 0..n {
            let b = point_dt(pt, &path.pt(i + 1))?;
            if FPeq(a + b, point_dt(&path.pt(i), &path.pt(i + 1))?) {
                return Ok(true);
            }
            a = b;
        }
        return Ok(false);
    }

    Ok(point_inside(pt, path)? != 0)
}

pub fn on_sl(lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    Ok(line_contain_point(line, &lseg.p[0])? && line_contain_point(line, &lseg.p[1])?)
}

pub fn on_sb(lseg: &LSEG, b: &BOX) -> bool {
    box_contain_lseg(b, lseg)
}

pub fn inter_sl(lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    lseg_interpt_line(None, lseg, line)
}

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

    if box_contain_point(b, &lseg.p[0]) || box_contain_point(b, &lseg.p[1]) {
        return Ok(true);
    }

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

pub fn inter_sb(lseg: &LSEG, b: &BOX) -> PgResult<bool> {
    box_interpt_lseg(None, b, lseg)
}

pub fn inter_lb(line: &LINE, b: &BOX) -> PgResult<bool> {
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
