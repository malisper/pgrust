use ::adt_float::{float8_div, float8_eq, float8_mi, float8_mul, float8_pl, get_float8_infinity, get_float8_nan};
use ::types_core::geo::{Point, LINE};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::point::{point_construct, point_sl};
use crate::{pg_hypot, point_eq_point, FPeq, FPzero};

pub fn line_construct(pt: &Point, m: f64) -> PgResult<LINE> {
    let mut result = LINE::default();
    if m.is_infinite() {
        result.A = -1.0;
        result.B = 0.0;
        result.C = pt.x;
    } else if m == 0.0 {
        result.A = 0.0;
        result.B = -1.0;
        result.C = pt.y;
    } else {
        result.A = m;
        result.B = -1.0;
        result.C = float8_mi(pt.y, float8_mul(m, pt.x)?)?;
        // C flushes a -0 C term to 0 (geo_ops.c:1103).
        if result.C == 0.0 {
            result.C = 0.0;
        }
    }
    Ok(result)
}

pub fn line_construct_pp(pt1: &Point, pt2: &Point) -> PgResult<LINE> {
    if point_eq_point(pt1, pt2) {
        return Err(Box::new(
            PgError::error("invalid line specification: must be two distinct points")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    line_construct(pt1, point_sl(pt1, pt2)?)
}

pub fn line_sl(line: &LINE) -> PgResult<f64> {
    if FPzero(line.A) {
        return Ok(0.0);
    }
    if FPzero(line.B) {
        return Ok(get_float8_infinity());
    }
    float8_div(line.A, -line.B)
}

pub fn line_invsl(line: &LINE) -> PgResult<f64> {
    if FPzero(line.A) {
        return Ok(get_float8_infinity());
    }
    if FPzero(line.B) {
        return Ok(0.0);
    }
    float8_div(line.B, line.A)
}

pub fn line_intersect(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    line_interpt_line(None, l1, l2)
}

pub fn line_parallel(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    Ok(!line_interpt_line(None, l1, l2)?)
}

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

pub fn line_vertical(line: &LINE) -> bool {
    FPzero(line.B)
}

pub fn line_horizontal(line: &LINE) -> bool {
    FPzero(line.A)
}

pub fn line_eq(l1: &LINE, l2: &LINE) -> PgResult<bool> {
    // Any NaN parameter forces exact comparison (geo_ops.c:1197).
    if l1.A.is_nan()
        || l1.B.is_nan()
        || l1.C.is_nan()
        || l2.A.is_nan()
        || l2.B.is_nan()
        || l2.C.is_nan()
    {
        return Ok(float8_eq(l1.A, l2.A) && float8_eq(l1.B, l2.B) && float8_eq(l1.C, l2.C));
    }

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
        pg_hypot(l1.A, l1.B)?,
    )
}

pub fn line_interpt(l1: &LINE, l2: &LINE) -> PgResult<Option<Point>> {
    let mut result = Point::default();
    if !line_interpt_line(Some(&mut result), l1, l2)? {
        return Ok(None);
    }
    Ok(Some(result))
}

// Identical lines report as parallel ("no intersection"), as C.
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

    // -0 flushed to 0 (geo_ops.c:1349).
    let x = if x == 0.0 { 0.0 } else { x };
    let y = if y == 0.0 { 0.0 } else { y };

    if let Some(slot) = result {
        *slot = point_construct(x, y);
    }

    Ok(true)
}

pub fn line_contain_point(line: &LINE, point: &Point) -> PgResult<bool> {
    Ok(FPzero(float8_pl(
        float8_pl(float8_mul(line.A, point.x)?, float8_mul(line.B, point.y)?)?,
        line.C,
    )?))
}

// Returns NaN (result = point) when the perpendicular cannot be dropped.
pub fn line_closept_point(result: Option<&mut Point>, line: &LINE, point: &Point) -> PgResult<f64> {
    let tmp = line_construct(point, line_invsl(line)?)?;
    let mut closept = Point::default();
    if !line_interpt_line(Some(&mut closept), &tmp, line)? {
        if let Some(slot) = result {
            *slot = *point;
        }
        return Ok(get_float8_nan());
    }

    if let Some(slot) = result {
        *slot = closept;
    }

    crate::point_dt(&closept, point)
}
