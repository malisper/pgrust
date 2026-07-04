use ::adt_float::{float8_div, float8_pl};
use ::types_core::geo::{Point, LINE, LSEG};
use ::types_error::PgResult;

use crate::line::{line_construct, line_interpt_line};
use crate::point::{point_invsl, point_sl};
use crate::{point_dt, point_eq_point, FPeq, FPge, FPgt, FPle, FPlt};

#[inline]
pub fn lseg_construct(pt1: &Point, pt2: &Point) -> LSEG {
    statlseg_construct(pt1, pt2)
}

#[inline]
pub fn statlseg_construct(pt1: &Point, pt2: &Point) -> LSEG {
    LSEG { p: [*pt1, *pt2] }
}

#[inline]
pub fn lseg_sl(lseg: &LSEG) -> PgResult<f64> {
    point_sl(&lseg.p[0], &lseg.p[1])
}

#[inline]
pub fn lseg_invsl(lseg: &LSEG) -> PgResult<f64> {
    point_invsl(&lseg.p[0], &lseg.p[1])
}

#[inline]
pub fn lseg_length(lseg: &LSEG) -> PgResult<f64> {
    point_dt(&lseg.p[0], &lseg.p[1])
}

pub fn lseg_intersect(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    lseg_interpt_lseg(None, l1, l2)
}

pub fn lseg_parallel(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPeq(lseg_sl(l1)?, lseg_sl(l2)?))
}

pub fn lseg_perp(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPeq(lseg_sl(l1)?, lseg_invsl(l2)?))
}

pub fn lseg_vertical(lseg: &LSEG) -> bool {
    FPeq(lseg.p[0].x, lseg.p[1].x)
}

pub fn lseg_horizontal(lseg: &LSEG) -> bool {
    FPeq(lseg.p[0].y, lseg.p[1].y)
}

pub fn lseg_eq(l1: &LSEG, l2: &LSEG) -> bool {
    point_eq_point(&l1.p[0], &l2.p[0]) && point_eq_point(&l1.p[1], &l2.p[1])
}

pub fn lseg_ne(l1: &LSEG, l2: &LSEG) -> bool {
    !point_eq_point(&l1.p[0], &l2.p[0]) || !point_eq_point(&l1.p[1], &l2.p[1])
}

pub fn lseg_lt(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPlt(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

pub fn lseg_le(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPle(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

pub fn lseg_gt(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPgt(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

pub fn lseg_ge(l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    Ok(FPge(
        point_dt(&l1.p[0], &l1.p[1])?,
        point_dt(&l2.p[0], &l2.p[1])?,
    ))
}

pub fn lseg_center(lseg: &LSEG) -> PgResult<Point> {
    Ok(Point {
        x: float8_div(float8_pl(lseg.p[0].x, lseg.p[1].x)?, 2.0)?,
        y: float8_div(float8_pl(lseg.p[0].y, lseg.p[1].y)?, 2.0)?,
    })
}

pub fn lseg_interpt_lseg(result: Option<&mut Point>, l1: &LSEG, l2: &LSEG) -> PgResult<bool> {
    let tmp = line_construct(&l2.p[0], lseg_sl(l2)?)?;
    let mut interpt = Point::default();
    if !lseg_interpt_line(Some(&mut interpt), l1, &tmp)? {
        return Ok(false);
    }

    if !lseg_contain_point(l2, &interpt)? {
        return Ok(false);
    }

    if let Some(slot) = result {
        *slot = interpt;
    }

    Ok(true)
}

pub fn lseg_interpt(l1: &LSEG, l2: &LSEG) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if !lseg_interpt_lseg(Some(&mut result), l1, l2)? {
        return Ok(None);
    }
    Ok(Some(result))
}

pub fn lseg_interpt_line(result: Option<&mut Point>, lseg: &LSEG, line: &LINE) -> PgResult<bool> {
    let tmp = line_construct(&lseg.p[0], lseg_sl(lseg)?)?;
    let mut interpt = Point::default();
    if !line_interpt_line(Some(&mut interpt), &tmp, line)? {
        return Ok(false);
    }

    if !lseg_contain_point(lseg, &interpt)? {
        return Ok(false);
    }
    if let Some(slot) = result {
        // Snap to a matching endpoint to avoid LSB residue (C geo_ops.c:2697).
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

pub fn lseg_contain_point(lseg: &LSEG, pt: &Point) -> PgResult<bool> {
    Ok(FPeq(
        point_dt(pt, &lseg.p[0])? + point_dt(pt, &lseg.p[1])?,
        point_dt(&lseg.p[0], &lseg.p[1])?,
    ))
}
