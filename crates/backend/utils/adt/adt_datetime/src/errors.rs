#![allow(non_snake_case)]

use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INTERVAL_FIELD_OVERFLOW,
    ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};

use crate::consts::*;

fn lossy(b: Option<&[u8]>) -> String {
    String::from_utf8_lossy(b.unwrap_or_default()).into_owned()
}

#[cold]
pub fn DateTimeParseError(
    dterr: i32,
    extra: Option<&DateTimeErrorExtra<'_>>,
    str_: &str,
    datatype: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    // Each arm's location is that errsave's closing line in datetime.c
    // (18.6), routine DateTimeParseError — the wire F/L/R fields C sends.
    let at = |e: PgError, line: i32| e.with_location("datetime.c", line, "DateTimeParseError");
    let err = match dterr {
        DTERR_FIELD_OVERFLOW => at(
            PgError::error(format!("date/time field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
            4224,
        ),
        DTERR_MD_FIELD_OVERFLOW => at(
            PgError::error(format!("date/time field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .with_hint("Perhaps you need a different \"DateStyle\" setting."),
            4232,
        ),
        DTERR_INTERVAL_OVERFLOW => at(
            PgError::error(format!("interval field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_INTERVAL_FIELD_OVERFLOW),
            4238,
        ),
        DTERR_TZDISP_OVERFLOW => at(
            PgError::error(format!("time zone displacement out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE),
            4244,
        ),
        DTERR_BAD_TIMEZONE => {
            let zone = lossy(extra.and_then(|e| e.dtee_timezone));
            at(
                PgError::error(format!("time zone \"{zone}\" not recognized"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                4250,
            )
        }
        DTERR_BAD_ZONE_ABBREV => {
            let zone = lossy(extra.and_then(|e| e.dtee_timezone));
            let abbr = lossy(extra.and_then(|e| e.dtee_abbrev));
            at(
                PgError::error(format!("time zone \"{zone}\" not recognized"))
                    .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
                    .with_detail(format!(
                        "This time zone name appears in the configuration file for time zone abbreviation \"{abbr}\"."
                    )),
                4258,
            )
        }
        _ => at(
            PgError::error(format!("invalid input syntax for type {datatype}: \"{str_}\""))
                .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
            4265,
        ),
    };
    ereturn(escontext, (), err)
}

// C "%02g" for the seconds field of make_time/make_timestamp error messages
// (date.c make_time, timestamp.c make_timestamp_internal). Semantics are
// PostgreSQL's own snprintf (src/port/snprintf.c fmtfloat): NaN → "NaN",
// infinities → "Infinity"/"-Infinity", finite values delegate to the
// platform's %g (C99: default precision 6; %e style when the decimal
// exponent X < -4 or X >= 6, else %f style; trailing zeros and a bare '.'
// stripped), then zero-pad to width 2 between sign and digits.
pub fn fmt_sec_g02(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-Infinity".to_string() } else { "Infinity".to_string() };
    }
    const P: i32 = 6;
    let s = if v == 0.0 {
        // %g of ±0 prints "0"/"-0".
        if v.is_sign_negative() { "-0".to_string() } else { "0".to_string() }
    } else {
        // Decimal exponent of the value after rounding to P significant
        // digits (rounding can carry, e.g. 999999.5 → 1e+06).
        let e_str = format!("{:.*e}", (P - 1) as usize, v);
        let x: i32 = e_str[e_str.rfind('e').unwrap() + 1..].parse().unwrap();
        if x < -4 || x >= P {
            // %e style: mantissa with P-1 decimals, exponent 'e±NN' (>= 2
            // digits), trailing zeros stripped from the mantissa.
            let (mant, _) = e_str.split_at(e_str.rfind('e').unwrap());
            let mant = strip_g_zeros(mant);
            format!("{mant}e{}{:02}", if x < 0 { '-' } else { '+' }, x.abs())
        } else {
            // %f style with precision P-1-X, trailing zeros stripped.
            strip_g_zeros(&format!("{:.*}", (P - 1 - x).max(0) as usize, v))
        }
    };
    // %02g zero padding (PG snprintf fmtfloat zpad): zeros go after the sign.
    if s.len() >= 2 {
        return s;
    }
    debug_assert!(!s.starts_with('-'));
    format!("0{s}")
}

fn strip_g_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}
