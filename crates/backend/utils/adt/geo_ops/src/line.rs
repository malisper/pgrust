//! 2-D line routines (geo_ops.c:943-1354).
//!
//! Lines are stored in the general form `Ax + By + C = 0` ([`LINE`]). The
//! fmgr shims (`line_in`/`line_out`/`line_recv`/`line_send`) live in
//! [`crate::io`]; this module has the construction, slope, intersection,
//! containment, and relational routines.

use ::types_core::geo::{Point, LINE};
use ::types_error::{PgError, PgResult};

use crate::f8::{float8_div, float8_eq, float8_mi, float8_mul, float8_pl, get_float8_infinity};
use crate::point::{point_construct, point_sl};
use crate::{FPeq, FPzero, HYPOT};

// ---------------------------------------------------------------------------
// Construction (geo_ops.c:1082-1129).
// ---------------------------------------------------------------------------

/// `line_construct(result, pt, m)` (geo_ops.c:1082): fill a LINE from a point
/// and a slope `m`. Vertical (`m == Inf`) -> `x = C`; horizontal (`m == 0`)
/// -> `y = C`; otherwise `mx - y + yinter = 0`.
pub fn line_construct(pt: &Point, m: f64) -> PgResult<LINE> {
    let mut result = LINE::default();
    if m.is_infinite() {
        // vertical - use "x = C"
        result.A = -1.0;
        result.B = 0.0;
        result.C = pt.x;
    } else if m == 0.0 {
        // horizontal - use "y = C"
        result.A = 0.0;
        result.B = -1.0;
        result.C = pt.y;
    } else {
        // use "mx - y + yinter = 0"
        result.A = m;
        result.B = -1.0;
        result.C = float8_mi(pt.y, float8_mul(m, pt.x)?)?;
        // on some platforms, the preceding expression tends to produce -0
        if result.C == 0.0 {
            result.C = 0.0;
        }
    }
    Ok(result)
}

/// `line_construct_pp(pt1, pt2)` (geo_ops.c:1114): construct the line through
/// two points. Raises `ERRCODE_INVALID_PARAMETER_VALUE` (22023) if the points
/// are equal.
pub fn line_construct_pp(pt1: &Point, pt2: &Point) -> PgResult<LINE> {
    if crate::point::point_eq_point(pt1, pt2) {
        return Err(
            PgError::error("invalid line specification: must be two distinct points")
                .with_sqlstate(crate::errcode_invalid_parameter()),
        );
    }
    line_construct(pt1, point_sl(pt1, pt2)?)
}

// ---------------------------------------------------------------------------
// Slope (geo_ops.c:1232-1254).
// ---------------------------------------------------------------------------

/// `line_sl(line)` (geo_ops.c:1232): slope of the line.
pub fn line_sl(line: &LINE) -> PgResult<f64> {
    if FPzero(line.A) {
        return Ok(0.0);
    }
    if FPzero(line.B) {
        return Ok(get_float8_infinity());
    }
    float8_div(line.A, -line.B)
}

/// `line_invsl(line)` (geo_ops.c:1246): inverse slope of the line.
pub fn line_invsl(line: &LINE) -> PgResult<f64> {
    if FPzero(line.A) {
        return Ok(get_float8_infinity());
    }
    if FPzero(line.B) {
        return Ok(0.0);
    }
    float8_div(line.B, line.A)
}

// ---------------------------------------------------------------------------
// Relative position (geo_ops.c:1136-1222).
// ---------------------------------------------------------------------------

/// `line_intersect()` (geo_ops.c:1136): do the two lines intersect?
pub fn line_intersect(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    line_interpt_line(None, l1, l2)
}

/// `line_parallel()` (geo_ops.c:1145): are the lines parallel (do not
/// intersect)?
pub fn line_parallel(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    Ok(!line_interpt_line(None, l1, l2)?)
}

/// `line_perp()` (geo_ops.c:1154): are the lines perpendicular?
pub fn line_perp(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    if FPzero(l1.A) {
        return Ok(FPzero(l2.B));
    }
    if FPzero(l2.A) {
        return Ok(FPzero(l1.B));
    }
    if FPzero(l1.B) {
        return Ok(FPzero(l2.A));
    }
    if FPzero(l2.B) {
        return Ok(FPzero(l1.A));
    }
    Ok(FPeq(
        float8_div(float8_mul(l1.A, l2.A)?, float8_mul(l1.B, l2.B)?)?,
        -1.0,
    ))
}

/// `line_vertical()` (geo_ops.c:1173).
pub fn line_vertical(line: &LINE) -> bool {
    FPzero(line.B)
}

/// `line_horizontal()` (geo_ops.c:1181).
pub fn line_horizontal(line: &LINE) -> bool {
    FPzero(line.A)
}

/// `line_eq()` (geo_ops.c:1193): whether two lines are the same. With any NaN
/// constants, insist on exact equality; otherwise lines whose parameters are
/// proportional are equal.
pub fn line_eq(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    // If any NaNs are involved, insist on exact equality.
    if l1.A.is_nan()
        || l1.B.is_nan()
        || l1.C.is_nan()
        || l2.A.is_nan()
        || l2.B.is_nan()
        || l2.C.is_nan()
    {
        return Ok(float8_eq(l1.A, l2.A) && float8_eq(l1.B, l2.B) && float8_eq(l1.C, l2.C));
    }

    // Otherwise, lines whose parameters are proportional are the same.
    let ratio = if !FPzero(l2.A) {
        float8_div(l1.A, l2.A)?
    } else if !FPzero(l2.B) {
        float8_div(l1.B, l2.B)?
    } else if !FPzero(l2.C) {
        float8_div(l1.C, l2.C)?
    } else {
        1.0
    };

    Ok(FPeq(l1.A, float8_mul(ratio, l2.A)?)
        && FPeq(l1.B, float8_mul(ratio, l2.B)?)
        && FPeq(l1.C, float8_mul(ratio, l2.C)?))
}

// ---------------------------------------------------------------------------
// Distance / intersection point (geo_ops.c:1260-1354).
// ---------------------------------------------------------------------------

/// `line_distance()` (geo_ops.c:1260): distance between two lines (0 if they
/// intersect).
pub fn line_distance(l1: &LINE, l2: &LINE) -> PgResult<f64> {
    if line_interpt_line(None, l1, l2)? {
        return Ok(0.0);
    }

    let ratio = if !FPzero(l1.A) && !l1.A.is_nan() && !FPzero(l2.A) && !l2.A.is_nan() {
        float8_div(l1.A, l2.A)?
    } else if !FPzero(l1.B) && !l1.B.is_nan() && !FPzero(l2.B) && !l2.B.is_nan() {
        float8_div(l1.B, l2.B)?
    } else {
        1.0
    };

    float8_div(
        float8_mi(l1.C, float8_mul(ratio, l2.C)?)?.abs(),
        HYPOT(l1.A, l1.B)?,
    )
}

/// `line_interpt()` (geo_ops.c:1285): the intersection point of two lines, or
/// `None` if they are parallel.
pub fn line_interpt(l1: &LINE, l2: &LINE) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if !line_interpt_line(Some(&mut result), l1, l2)? {
        return Ok(None);
    }
    Ok(Some(result))
}

/// `line_interpt_line(result, l1, l2)` (geo_ops.c:1313): whether two lines
/// intersect; if `result` is `Some`, it is set to the intersection point.
///
/// Identical lines are reported as parallel ("no intersection"). Lines with
/// NaN constants return true with NaN coordinates.
pub fn line_interpt_line(result: Option<&mut Point>, l1: &LINE, l2: &LINE) -> PgResult<bool> {
    let x;
    let y;

    if !FPzero(l1.B) {
        if FPeq(l2.A, float8_mul(l1.A, float8_div(l2.B, l1.B)?)?) {
            return Ok(false);
        }

        x = float8_div(
            float8_mi(float8_mul(l1.B, l2.C)?, float8_mul(l2.B, l1.C)?)?,
            float8_mi(float8_mul(l1.A, l2.B)?, float8_mul(l2.A, l1.B)?)?,
        )?;
        y = float8_div(-float8_pl(float8_mul(l1.A, x)?, l1.C)?, l1.B)?;
    } else if !FPzero(l2.B) {
        if FPeq(l1.A, float8_mul(l2.A, float8_div(l1.B, l2.B)?)?) {
            return Ok(false);
        }

        x = float8_div(
            float8_mi(float8_mul(l2.B, l1.C)?, float8_mul(l1.B, l2.C)?)?,
            float8_mi(float8_mul(l2.A, l1.B)?, float8_mul(l1.A, l2.B)?)?,
        )?;
        y = float8_div(-float8_pl(float8_mul(l2.A, x)?, l2.C)?, l2.B)?;
    } else {
        return Ok(false);
    }

    // On some platforms, the preceding expressions tend to produce -0.
    let x = if x == 0.0 { 0.0 } else { x };
    let y = if y == 0.0 { 0.0 } else { y };

    if let Some(slot) = result {
        *slot = point_construct(x, y);
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Containment / closest point (geo_ops.c:3086, 2723).
// ---------------------------------------------------------------------------

/// `line_contain_point(line, point)` (geo_ops.c:3086): does the point satisfy
/// the line equation (within EPSILON)?
pub fn line_contain_point(line: &LINE, point: &Point) -> PgResult<bool> {
    Ok(FPzero(float8_pl(
        float8_pl(float8_mul(line.A, point.x)?, float8_mul(line.B, point.y)?)?,
        line.C,
    )?))
}

/// `line_closept_point(result, line, point)` (geo_ops.c:2723): closest point on
/// the line to the given point. Returns the distance; `NaN` (with `result` set
/// to `point`) if the perpendicular cannot be dropped (e.g. NaN coordinates).
pub fn line_closept_point(result: Option<&mut Point>, line: &LINE, point: &Point) -> PgResult<f64> {
    let tmp = line_construct(point, line_invsl(line)?)?;
    let mut closept = Point::default();
    if !line_interpt_line(Some(&mut closept), &tmp, line)? {
        if let Some(slot) = result {
            *slot = *point;
        }
        return Ok(crate::f8::get_float8_nan());
    }

    if let Some(slot) = result {
        *slot = closept;
    }

    crate::point::point_dt(&closept, point)
}
