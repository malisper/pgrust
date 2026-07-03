//! geo_ops.c, the box/point subset the gist box_ops/point_ops lanes need:
//! point/box I/O, box comparison operators, point comparison operators,
//! on_pb, point_dt/pg_hypot. Everything else in geo_ops.c stays unported.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod builtins;

use ::adt_float::{float8_mi, float8_min, float8_max};
use ::types_core::geo::{Point, BOX};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION};

pub const EPSILON: f64 = 1.0E-06;

#[inline]
pub fn FPzero(a: f64) -> bool {
    a.abs() <= EPSILON
}
#[inline]
pub fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}
#[inline]
pub fn FPne(a: f64, b: f64) -> bool {
    !FPeq(a, b)
}
#[inline]
pub fn FPlt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}
#[inline]
pub fn FPle(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}
#[inline]
pub fn FPgt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}
#[inline]
pub fn FPge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

// point_eq_point: NaNs insist on exact equality.
pub fn point_eq_point(pt1: &Point, pt2: &Point) -> bool {
    if pt1.x.is_nan() || pt1.y.is_nan() || pt2.x.is_nan() || pt2.y.is_nan() {
        return pt1.x == pt2.x && pt1.y == pt2.y;
    }
    FPeq(pt1.x, pt2.x) && FPeq(pt1.y, pt2.y)
}

pub fn box_ov(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.low.x, box2.high.x)
        && FPle(box2.low.x, box1.high.x)
        && FPle(box1.low.y, box2.high.y)
        && FPle(box2.low.y, box1.high.y)
}

pub fn box_contain_box(contains: &BOX, contained: &BOX) -> bool {
    FPge(contains.high.x, contained.high.x)
        && FPle(contains.low.x, contained.low.x)
        && FPge(contains.high.y, contained.high.y)
        && FPle(contains.low.y, contained.low.y)
}

// box_contain_point: deliberately exact, not fuzzy (C comment on on_pb).
pub fn box_contain_point(b: &BOX, point: &Point) -> bool {
    b.high.x >= point.x && b.low.x <= point.x && b.high.y >= point.y && b.low.y <= point.y
}

/// pg_hypot.
pub fn pg_hypot(x: f64, y: f64) -> PgResult<f64> {
    if x.is_infinite() || y.is_infinite() {
        return Ok(f64::INFINITY);
    }
    if x.is_nan() || y.is_nan() {
        return Ok(f64::NAN);
    }
    let (mut x, mut y) = (x.abs(), y.abs());
    if x < y {
        core::mem::swap(&mut x, &mut y);
    }
    if y == 0.0 {
        return Ok(x);
    }
    let yx = y / x;
    let result = x * (1.0 + yx * yx).sqrt();
    if result.is_infinite() {
        return Err(Box::new(::adt_float::float_overflow_error()));
    }
    if result == 0.0 {
        return Err(Box::new(::adt_float::float_underflow_error()));
    }
    Ok(result)
}

/// point_dt.
pub fn point_dt(pt1: &Point, pt2: &Point) -> PgResult<f64> {
    pg_hypot(float8_mi(pt1.x, pt2.x)?, float8_mi(pt1.y, pt2.y)?)
}

#[cold]
fn invalid_input(type_name: &str, orig: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid input syntax for type {type_name}: \"{orig}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

fn skip_space(s: &[u8], mut i: usize) -> usize {
    while i < s.len() && (s[i] as char).is_ascii_whitespace() {
        i += 1;
    }
    i
}

// single_decode via float8in_internal; returns (value, next index).
fn single_decode(s: &str, i: usize, type_name: &str, orig: &str) -> PgResult<(f64, usize)> {
    let mut consumed = 0usize;
    let v = ::adt_float::float8in_internal(&s[i..], Some(&mut consumed), type_name, orig, None)?;
    Ok((v, i + consumed))
}

// pair_decode; returns (x, y, next index).
fn pair_decode(
    s: &str,
    mut i: usize,
    type_name: &str,
    orig: &str,
    stop_at: bool,
) -> PgResult<(f64, f64, usize)> {
    let b = s.as_bytes();
    i = skip_space(b, i);
    let has_delim = i < b.len() && b[i] == b'(';
    if has_delim {
        i += 1;
    }
    let (x, mut i) = single_decode(s, i, type_name, orig)?;
    if i >= b.len() || b[i] != b',' {
        return Err(invalid_input(type_name, orig));
    }
    i += 1;
    let (y, mut i) = {
        let (y, ni) = single_decode(s, i, type_name, orig)?;
        (y, ni)
    };
    if has_delim {
        if i >= b.len() || b[i] != b')' {
            return Err(invalid_input(type_name, orig));
        }
        i += 1;
        i = skip_space(b, i);
    }
    if !stop_at && i != b.len() {
        return Err(invalid_input(type_name, orig));
    }
    Ok((x, y, i))
}

// path_decode for closed types (opentype=false), npts points.
fn path_decode(
    s: &str,
    npts: usize,
    type_name: &str,
    orig: &str,
) -> PgResult<Vec<Point>> {
    let b = s.as_bytes();
    let mut i = skip_space(b, 0);
    let mut depth = 0usize;
    if i < b.len() && b[i] == b'[' {
        // no open delimiter allowed for box
        return Err(invalid_input(type_name, orig));
    }
    if i < b.len() && b[i] == b'(' {
        let cp = skip_space(b, i + 1);
        if cp < b.len() && b[cp] == b'(' {
            depth += 1;
            i = cp;
        } else if s.rfind('(') == Some(i) {
            depth += 1;
            i = cp;
        }
    }

    let mut pts = Vec::with_capacity(npts);
    for _ in 0..npts {
        let (x, y, ni) = pair_decode(s, i, type_name, orig, true)?;
        i = ni;
        if i < b.len() && b[i] == b',' {
            i += 1;
        }
        pts.push(Point { x, y });
    }

    while depth > 0 {
        if i < b.len() && b[i] == b')' {
            depth -= 1;
            i += 1;
            i = skip_space(b, i);
        } else {
            return Err(invalid_input(type_name, orig));
        }
    }

    if i != b.len() {
        return Err(invalid_input(type_name, orig));
    }
    Ok(pts)
}

/// box_in core: parse + reorder corners.
pub fn box_in(s: &str) -> PgResult<BOX> {
    let pts = path_decode(s, 2, "box", s)?;
    let mut bx = BOX {
        high: pts[0],
        low: pts[1],
    };
    if bx.high.x < bx.low.x {
        core::mem::swap(&mut bx.high.x, &mut bx.low.x);
    }
    if bx.high.y < bx.low.y {
        core::mem::swap(&mut bx.high.y, &mut bx.low.y);
    }
    Ok(bx)
}

/// point_in core.
pub fn point_in(s: &str) -> PgResult<Point> {
    let (x, y, _) = pair_decode(s, 0, "point", s, false)?;
    Ok(Point { x, y })
}

fn pair_encode(x: f64, y: f64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 64];
    let n = ::adt_float::float8out_internal(x, &mut buf);
    out.extend_from_slice(&buf[..n]);
    out.push(b',');
    let n = ::adt_float::float8out_internal(y, &mut buf);
    out.extend_from_slice(&buf[..n]);
}

/// path_encode(PATH_NONE) over the given points.
pub fn path_encode_none(pts: &[Point], out: &mut Vec<u8>) {
    for (i, p) in pts.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'(');
        pair_encode(p.x, p.y, out);
        out.push(b')');
    }
}

/// adjustBox (gistproc.c): grow b to include addon (also usable here).
pub fn adjust_box(b: &mut BOX, addon: &BOX) {
    if b.high.x < addon.high.x {
        b.high.x = addon.high.x;
    }
    if b.low.x > addon.low.x {
        b.low.x = addon.low.x;
    }
    if b.high.y < addon.high.y {
        b.high.y = addon.high.y;
    }
    if b.low.y > addon.low.y {
        b.low.y = addon.low.y;
    }
}

/// rt_box_union.
pub fn rt_box_union(a: &BOX, b: &BOX) -> BOX {
    BOX {
        high: Point {
            x: float8_max(a.high.x, b.high.x),
            y: float8_max(a.high.y, b.high.y),
        },
        low: Point {
            x: float8_min(a.low.x, b.low.x),
            y: float8_min(a.low.y, b.low.y),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_io_roundtrip() {
        for s in ["((1,2),(3,4))", "(1,2),(3,4)", "1,2,3,4", " ( (1, 2) , (3 ,4) ) "] {
            let b = box_in(s).unwrap();
            assert_eq!(b.high.x, 3.0);
            assert_eq!(b.high.y, 4.0);
            assert_eq!(b.low.x, 1.0);
            assert_eq!(b.low.y, 2.0);
        }
        let b = box_in("(3,4),(1,2)").unwrap();
        assert_eq!((b.high.x, b.low.x), (3.0, 1.0));
        assert!(box_in("((1,2),(3,4)").is_err());
        assert!(box_in("1,2,3").is_err());

        let mut out = Vec::new();
        path_encode_none(&[b.high, b.low], &mut out);
        assert_eq!(out, b"(3,4),(1,2)");
    }

    #[test]
    fn point_io() {
        let p = point_in("(1.5,-2)").unwrap();
        assert_eq!((p.x, p.y), (1.5, -2.0));
        assert!(point_in("(1,2) x").is_err());
        let mut out = Vec::new();
        path_encode_none(&[p], &mut out);
        assert_eq!(out, b"(1.5,-2)");
    }

    #[test]
    fn fp_macros() {
        assert!(FPeq(1.0, 1.0 + 5e-7));
        assert!(!FPeq(1.0, 1.0 + 2e-6));
        assert!(FPlt(1.0, 1.0 + 2e-6));
        assert!(!FPlt(1.0, 1.0 + 5e-7));
    }

    #[test]
    fn hypot_matches_c_shape() {
        assert_eq!(pg_hypot(3.0, 4.0).unwrap(), 5.0);
        assert_eq!(pg_hypot(0.0, 0.0).unwrap(), 0.0);
        assert!(pg_hypot(f64::INFINITY, f64::NAN).unwrap().is_infinite());
    }
}
