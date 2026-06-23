//! GUC unit machinery: the memory/time conversion tables and the
//! `convert_to_base_unit` / `convert_int_from_base_unit` /
//! `convert_real_from_base_unit` / `get_config_unit_name` / `parse_int` /
//! `parse_real` functions from `src/backend/utils/misc/guc.c`.
//!
//! 1:1 with the C source, including the table ordering (greatest unit first),
//! the fractional-value rounding in `convert_to_base_unit`, and the
//! re-parse-as-float fallback in `parse_int`.

use ::types_core::BLCKSZ;
use ::types_guc::{
    GUC_UNIT, GUC_UNIT_BLOCKS, GUC_UNIT_BYTE, GUC_UNIT_KB, GUC_UNIT_MB, GUC_UNIT_MEMORY,
    GUC_UNIT_MIN, GUC_UNIT_MS, GUC_UNIT_S, GUC_UNIT_XBLOCKS,
};

use crate::cnum::{c_strtod, c_strtol_base0};

/// `#define XLOG_BLCKSZ 8192` (`pg_config.h`). Equal to `BLCKSZ` in the proven
/// build; the GUC unit math divides by it for the `GUC_UNIT_XBLOCKS` rows.
const XLOG_BLCKSZ: usize = 8192;

/// `#define MAX_UNIT_LEN 3` (guc.c:101).
pub const MAX_UNIT_LEN: usize = 3;

/// `gettext_noop(...)` memory-units HINT (guc.c:119).
pub const MEMORY_UNITS_HINT: &str =
    "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".";

/// `gettext_noop(...)` time-units HINT (guc.c:156).
pub const TIME_UNITS_HINT: &str =
    "Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".";

/// `typedef struct { char unit[]; int base_unit; double multiplier; }`
/// (guc.c:103). The table sentinel is an entry whose `unit` is empty.
#[derive(Clone, Copy)]
struct UnitConversion {
    unit: &'static str,
    base_unit: i32,
    multiplier: f64,
}

// `BLCKSZ / 1024` and `XLOG_BLCKSZ / 1024` — integer division as in C.
const fn blk_kb() -> f64 {
    (BLCKSZ / 1024) as f64
}
const fn xlog_blk_kb() -> f64 {
    (XLOG_BLCKSZ / 1024) as f64
}

/// `memory_unit_conversion_table[]` (guc.c:121). Greatest unit first within each
/// base unit, with a final empty-`unit` sentinel.
fn memory_unit_conversion_table() -> [UnitConversion; 26] {
    let blk = blk_kb();
    let xblk = xlog_blk_kb();
    [
        UnitConversion { unit: "TB", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 * 1024.0 * 1024.0 },
        UnitConversion { unit: "GB", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 * 1024.0 },
        UnitConversion { unit: "MB", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 },
        UnitConversion { unit: "kB", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 },
        UnitConversion { unit: "B", base_unit: GUC_UNIT_BYTE, multiplier: 1.0 },

        UnitConversion { unit: "TB", base_unit: GUC_UNIT_KB, multiplier: 1024.0 * 1024.0 * 1024.0 },
        UnitConversion { unit: "GB", base_unit: GUC_UNIT_KB, multiplier: 1024.0 * 1024.0 },
        UnitConversion { unit: "MB", base_unit: GUC_UNIT_KB, multiplier: 1024.0 },
        UnitConversion { unit: "kB", base_unit: GUC_UNIT_KB, multiplier: 1.0 },
        UnitConversion { unit: "B", base_unit: GUC_UNIT_KB, multiplier: 1.0 / 1024.0 },

        UnitConversion { unit: "TB", base_unit: GUC_UNIT_MB, multiplier: 1024.0 * 1024.0 },
        UnitConversion { unit: "GB", base_unit: GUC_UNIT_MB, multiplier: 1024.0 },
        UnitConversion { unit: "MB", base_unit: GUC_UNIT_MB, multiplier: 1.0 },
        UnitConversion { unit: "kB", base_unit: GUC_UNIT_MB, multiplier: 1.0 / 1024.0 },
        UnitConversion { unit: "B", base_unit: GUC_UNIT_MB, multiplier: 1.0 / (1024.0 * 1024.0) },

        UnitConversion { unit: "TB", base_unit: GUC_UNIT_BLOCKS, multiplier: (1024.0 * 1024.0 * 1024.0) / blk },
        UnitConversion { unit: "GB", base_unit: GUC_UNIT_BLOCKS, multiplier: (1024.0 * 1024.0) / blk },
        UnitConversion { unit: "MB", base_unit: GUC_UNIT_BLOCKS, multiplier: 1024.0 / blk },
        UnitConversion { unit: "kB", base_unit: GUC_UNIT_BLOCKS, multiplier: 1.0 / blk },
        UnitConversion { unit: "B", base_unit: GUC_UNIT_BLOCKS, multiplier: 1.0 / BLCKSZ as f64 },

        UnitConversion { unit: "TB", base_unit: GUC_UNIT_XBLOCKS, multiplier: (1024.0 * 1024.0 * 1024.0) / xblk },
        UnitConversion { unit: "GB", base_unit: GUC_UNIT_XBLOCKS, multiplier: (1024.0 * 1024.0) / xblk },
        UnitConversion { unit: "MB", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1024.0 / xblk },
        UnitConversion { unit: "kB", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1.0 / xblk },
        UnitConversion { unit: "B", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1.0 / XLOG_BLCKSZ as f64 },

        UnitConversion { unit: "", base_unit: 0, multiplier: 0.0 }, // sentinel
    ]
}

/// `time_unit_conversion_table[]` (guc.c:158).
fn time_unit_conversion_table() -> [UnitConversion; 19] {
    [
        UnitConversion { unit: "d", base_unit: GUC_UNIT_MS, multiplier: (1000 * 60 * 60 * 24) as f64 },
        UnitConversion { unit: "h", base_unit: GUC_UNIT_MS, multiplier: (1000 * 60 * 60) as f64 },
        UnitConversion { unit: "min", base_unit: GUC_UNIT_MS, multiplier: (1000 * 60) as f64 },
        UnitConversion { unit: "s", base_unit: GUC_UNIT_MS, multiplier: 1000.0 },
        UnitConversion { unit: "ms", base_unit: GUC_UNIT_MS, multiplier: 1.0 },
        UnitConversion { unit: "us", base_unit: GUC_UNIT_MS, multiplier: 1.0 / 1000.0 },

        UnitConversion { unit: "d", base_unit: GUC_UNIT_S, multiplier: (60 * 60 * 24) as f64 },
        UnitConversion { unit: "h", base_unit: GUC_UNIT_S, multiplier: (60 * 60) as f64 },
        UnitConversion { unit: "min", base_unit: GUC_UNIT_S, multiplier: 60.0 },
        UnitConversion { unit: "s", base_unit: GUC_UNIT_S, multiplier: 1.0 },
        UnitConversion { unit: "ms", base_unit: GUC_UNIT_S, multiplier: 1.0 / 1000.0 },
        UnitConversion { unit: "us", base_unit: GUC_UNIT_S, multiplier: 1.0 / (1000.0 * 1000.0) },

        UnitConversion { unit: "d", base_unit: GUC_UNIT_MIN, multiplier: (60 * 24) as f64 },
        UnitConversion { unit: "h", base_unit: GUC_UNIT_MIN, multiplier: 60.0 },
        UnitConversion { unit: "min", base_unit: GUC_UNIT_MIN, multiplier: 1.0 },
        UnitConversion { unit: "s", base_unit: GUC_UNIT_MIN, multiplier: 1.0 / 60.0 },
        UnitConversion { unit: "ms", base_unit: GUC_UNIT_MIN, multiplier: 1.0 / (1000.0 * 60.0) },
        UnitConversion { unit: "us", base_unit: GUC_UNIT_MIN, multiplier: 1.0 / (1000.0 * 1000.0 * 60.0) },

        UnitConversion { unit: "", base_unit: 0, multiplier: 0.0 }, // sentinel
    ]
}

#[inline]
fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// C `rint()`: round to nearest, ties to even.
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

/// `convert_to_base_unit(value, unit, base_unit, &base_value)` (guc.c:2670).
///
/// `unit` is the byte slice starting at the unit (which may have trailing
/// whitespace).  Returns `Some(base_value)` on success, `None` if the unit is
/// unrecognized or there is garbage after it.
pub fn convert_to_base_unit(value: f64, unit: &[u8], base_unit: i32) -> Option<f64> {
    // Extract the unit string to compare to table entries (≤ MAX_UNIT_LEN).
    let mut unitstr = [0u8; MAX_UNIT_LEN];
    let mut unitlen = 0usize;
    let mut i = 0usize;
    while i < unit.len() && unit[i] != 0 && !is_c_space(unit[i]) && unitlen < MAX_UNIT_LEN {
        unitstr[unitlen] = unit[i];
        unitlen += 1;
        i += 1;
    }
    // Allow whitespace after the unit.
    while i < unit.len() && is_c_space(unit[i]) {
        i += 1;
    }
    // Anything left (including an over-long unit) is garbage.
    if i < unit.len() && unit[i] != 0 {
        return None;
    }
    let unitstr = &unitstr[..unitlen];

    let memory = memory_unit_conversion_table();
    let time = time_unit_conversion_table();
    let table: &[UnitConversion] = if base_unit & GUC_UNIT_MEMORY != 0 {
        &memory
    } else {
        &time
    };

    let mut idx = 0usize;
    while !table[idx].unit.is_empty() {
        if base_unit == table[idx].base_unit && unitstr == table[idx].unit.as_bytes() {
            let mut cvalue = value * table[idx].multiplier;

            // Round a fractional value to the nearest multiple of the next
            // smaller unit, if there is one (same base unit).
            let next = &table[idx + 1];
            if !next.unit.is_empty() && base_unit == next.base_unit {
                cvalue = rint(cvalue / next.multiplier) * next.multiplier;
            }
            return Some(cvalue);
        }
        idx += 1;
    }
    None
}

/// `convert_int_from_base_unit(base_value, base_unit, &value, &unit)`
/// (guc.c:2728). Returns `(value, unit)`.
pub fn convert_int_from_base_unit(base_value: i64, base_unit: i32) -> (i64, &'static str) {
    let memory = memory_unit_conversion_table();
    let time = time_unit_conversion_table();
    let table: &[UnitConversion] = if base_unit & GUC_UNIT_MEMORY != 0 {
        &memory
    } else {
        &time
    };

    let mut idx = 0usize;
    while !table[idx].unit.is_empty() {
        if base_unit == table[idx].base_unit {
            // Accept the first conversion that divides the value evenly (the
            // table is ordered greatest -> smallest).
            if table[idx].multiplier <= 1.0
                || base_value % (table[idx].multiplier as i64) == 0
            {
                let value = rint(base_value as f64 / table[idx].multiplier) as i64;
                return (value, table[idx].unit);
            }
        }
        idx += 1;
    }
    // Assert(*unit != NULL) — a well-formed table always matches; the smallest
    // unit (multiplier 1.0) divides evenly.  Unreachable in practice.
    (base_value, "")
}

/// `convert_real_from_base_unit(base_value, base_unit, &value, &unit)`
/// (guc.c:2770). Returns `(value, unit)`.
pub fn convert_real_from_base_unit(base_value: f64, base_unit: i32) -> (f64, &'static str) {
    let memory = memory_unit_conversion_table();
    let time = time_unit_conversion_table();
    let table: &[UnitConversion] = if base_unit & GUC_UNIT_MEMORY != 0 {
        &memory
    } else {
        &time
    };

    let mut value = base_value;
    let mut unit = "";
    let mut idx = 0usize;
    while !table[idx].unit.is_empty() {
        if base_unit == table[idx].base_unit {
            value = base_value / table[idx].multiplier;
            unit = table[idx].unit;
            // Accept a divisor within 1e-8 of producing an integer; otherwise
            // fall through to the smallest (last) target unit.
            if value > 0.0 && ((rint(value) / value) - 1.0).abs() <= 1e-8 {
                break;
            }
        }
        idx += 1;
    }
    (value, unit)
}

/// `get_config_unit_name(flags)` (guc.c:2813). Returns the GUC's base-unit name,
/// or `None` if unitless. Mirrors C's `static` block buffers for BLOCKS/XBLOCKS
/// with the constant `BLCKSZ`/`XLOG_BLCKSZ` (both 8192 -> "8kB").
pub fn get_config_unit_name(flags: i32) -> Option<&'static str> {
    match flags & GUC_UNIT {
        0 => None,
        GUC_UNIT_BYTE => Some("B"),
        GUC_UNIT_KB => Some("kB"),
        GUC_UNIT_MB => Some("MB"),
        GUC_UNIT_BLOCKS => Some(BLCKSZ_KB_STR),
        GUC_UNIT_XBLOCKS => Some(XLOG_BLCKSZ_KB_STR),
        GUC_UNIT_MS => Some("ms"),
        GUC_UNIT_S => Some("s"),
        GUC_UNIT_MIN => Some("min"),
        _ => None, // C elog(ERROR, "unrecognized GUC units value"); unreachable
    }
}

// `snprintf(bbuf, "%dkB", BLCKSZ / 1024)`. With BLCKSZ == 8192 this is "8kB".
const BLCKSZ_KB_STR: &str = "8kB";
const XLOG_BLCKSZ_KB_STR: &str = "8kB";

const _: () = {
    // Keep the cached unit-name strings honest if the block sizes ever change.
    assert!(BLCKSZ / 1024 == 8);
    assert!(XLOG_BLCKSZ / 1024 == 8);
};

/// Outcome of `parse_int` / `parse_real`: either a value, or a failure carrying
/// an optional HINT (the C `*hintmsg` out-parameter).
pub enum ParseNum<T> {
    Ok(T),
    Err { hint: Option<&'static str> },
}

/// `parse_int(value, &result, flags, &hintmsg)` (guc.c:2870). Returns the parsed
/// integer (after units conversion + rounding + range check to `i32`), or a
/// failure (with HINT for a bad unit / out-of-int-range).
pub fn parse_int(value: &str, flags: i32) -> ParseNum<i32> {
    let bytes = value.as_bytes();

    // strtol(value, &endptr, 0); if it stops at '.'/'e'/'E' or overflows,
    // re-parse as float.
    let s = c_strtol_base0(bytes);
    let mut val: f64;
    let mut endptr: usize;
    let stop = bytes.get(s.consumed).copied().unwrap_or(0);
    if stop == b'.' || stop == b'e' || stop == b'E' || s.erange {
        let d = c_strtod(bytes);
        val = d.value;
        endptr = d.consumed;
        if d.consumed == 0 || d.erange {
            return ParseNum::Err { hint: None };
        }
    } else {
        val = s.value as f64;
        endptr = s.consumed;
        if s.consumed == 0 {
            return ParseNum::Err { hint: None };
        }
    }

    if val.is_nan() {
        return ParseNum::Err { hint: None };
    }

    // Allow whitespace between number and unit.
    while endptr < bytes.len() && is_c_space(bytes[endptr]) {
        endptr += 1;
    }

    // Handle a possible unit.
    if endptr < bytes.len() && bytes[endptr] != 0 {
        if flags & GUC_UNIT == 0 {
            return ParseNum::Err { hint: None };
        }
        match convert_to_base_unit(val, &bytes[endptr..], flags & GUC_UNIT) {
            Some(cv) => val = cv,
            None => {
                let hint = if flags & GUC_UNIT_MEMORY != 0 {
                    MEMORY_UNITS_HINT
                } else {
                    TIME_UNITS_HINT
                };
                return ParseNum::Err { hint: Some(hint) };
            }
        }
    }

    // Round to int, then check overflow.
    val = rint(val);
    if val > i32::MAX as f64 || val < i32::MIN as f64 {
        return ParseNum::Err {
            hint: Some("Value exceeds integer range."),
        };
    }

    ParseNum::Ok(val as i32)
}

/// `parse_real(value, &result, flags, &hintmsg)` (guc.c:2960).
pub fn parse_real(value: &str, flags: i32) -> ParseNum<f64> {
    let bytes = value.as_bytes();

    let d = c_strtod(bytes);
    if d.consumed == 0 || d.erange {
        return ParseNum::Err { hint: None };
    }
    let mut val = d.value;
    let mut endptr = d.consumed;

    if val.is_nan() {
        return ParseNum::Err { hint: None };
    }

    while endptr < bytes.len() && is_c_space(bytes[endptr]) {
        endptr += 1;
    }

    if endptr < bytes.len() && bytes[endptr] != 0 {
        if flags & GUC_UNIT == 0 {
            return ParseNum::Err { hint: None };
        }
        match convert_to_base_unit(val, &bytes[endptr..], flags & GUC_UNIT) {
            Some(cv) => val = cv,
            None => {
                let hint = if flags & GUC_UNIT_MEMORY != 0 {
                    MEMORY_UNITS_HINT
                } else {
                    TIME_UNITS_HINT
                };
                return ParseNum::Err { hint: Some(hint) };
            }
        }
    }

    ParseNum::Ok(val)
}

// ---------------------------------------------------------------------------
// Real-value rendering, matching C `snprintf("%g", v)` / `snprintf("%.*e", p, v)`.
//
// `ShowGUCOption` renders `PGC_REAL` GUCs with `"%g%s"`; `serialize_variable`
// uses `"%.*e"` with `REALTYPE_PRECISION = 17`. C's printf `%g` and `%e`
// semantics are reproduced here so the rendered text is byte-identical to the C
// backend's (this is the value SHOW / current_setting() / a parallel worker
// reads back).
// ---------------------------------------------------------------------------

/// `snprintf(buf, "%g", v)` (C printf, default precision 6).
///
/// C `%g` with precision `P` (treated as 1 if 0) computes the value's decimal
/// exponent `X` after rounding to `P` significant digits, then renders with
/// `%e` style if `X < -4 || X >= P`, else `%f` style; in both cases trailing
/// zeros (and a trailing `.`) are removed (the `#` flag is not used here).
pub fn fmt_g(v: f64) -> String {
    fmt_g_prec(v, 6)
}

/// `snprintf(buf, "%.*g", precision, v)`: C `%g` with an explicit precision.
pub fn fmt_g_prec(v: f64, precision: usize) -> String {
    // Non-finite values: C printf prints "inf"/"-inf"/"nan".
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf".to_string() } else { "inf".to_string() };
    }

    // C: "if precision is 0, it is treated as 1".
    let p = precision.max(1);

    if v == 0.0 {
        // %g of 0 is "0" (sign of -0.0 is preserved by C: "-0").
        return if v.is_sign_negative() { "-0".to_string() } else { "0".to_string() };
    }

    // Determine X = the exponent that would be used with %e (i.e. after
    // rounding the magnitude to `p` significant digits). Render with %e at
    // precision p-1 and read the exponent back; this both rounds and tells us X.
    let e_str = format!("{:.*e}", p - 1, v);
    // Rust's {:e} format is like "1.23456e7" / "1.2e-3"; split mantissa/exp.
    let exp: i32 = e_str
        .rsplit('e')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let x = exp;

    let out = if x < -4 || x >= p as i32 {
        // %e style with precision p-1, then strip trailing zeros in the mantissa.
        format_e_style(v, p - 1)
    } else {
        // %f style with precision (p - 1 - X), then strip trailing zeros.
        let f_prec = (p as i32 - 1 - x).max(0) as usize;
        let s = format!("{:.*}", f_prec, v);
        strip_trailing_zeros(&s)
    };
    out
}

/// `snprintf(buf, "%.*e", precision, v)` (C printf `%e`): mantissa with exactly
/// `precision` fractional digits, `e`, sign, and at least two exponent digits.
/// Used by `serialize_variable` with `precision = REALTYPE_PRECISION (17)`.
pub fn fmt_e(v: f64, precision: usize) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf".to_string() } else { "inf".to_string() };
    }
    let r = format!("{:.*e}", precision, v);
    normalize_e(&r)
}

/// `%e`-style rendering at the given mantissa precision, with C's trailing-zero
/// stripping (used by the `%g`/`%e` branch of `fmt_g`).
fn format_e_style(v: f64, mantissa_prec: usize) -> String {
    let r = format!("{:.*e}", mantissa_prec, v);
    let normalized = normalize_e(&r);
    strip_trailing_zeros_e(&normalized)
}

/// Convert Rust's `{:e}` ("1.5e-3", "1e7") to C printf `%e`
/// ("1.500000e-03", "1.000000e+07"): force the `+`/`-` exponent sign and pad
/// the exponent to at least two digits. The mantissa already carries the
/// requested fractional digits from the format precision.
fn normalize_e(rust_e: &str) -> String {
    let (mantissa, exp) = match rust_e.split_once('e') {
        Some((m, e)) => (m, e),
        None => return rust_e.to_string(),
    };
    let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
        ('-', rest)
    } else if let Some(rest) = exp.strip_prefix('+') {
        ('+', rest)
    } else {
        ('+', exp)
    };
    let padded = if digits.len() < 2 {
        format!("{:0>2}", digits)
    } else {
        digits.to_string()
    };
    format!("{mantissa}e{sign}{padded}")
}

/// Strip trailing zeros (and a trailing `.`) from a plain `%f` rendering, the
/// way C `%g` (without the `#` flag) does.
fn strip_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let trimmed = s.trim_end_matches('0');
    let trimmed = trimmed.strip_suffix('.').unwrap_or(trimmed);
    trimmed.to_string()
}

/// Strip trailing zeros from the mantissa of a `%e` rendering (C `%g`'s
/// exponential branch), preserving the exponent suffix.
fn strip_trailing_zeros_e(s: &str) -> String {
    let (mantissa, exp) = match s.split_once('e') {
        Some((m, e)) => (m, e),
        None => return strip_trailing_zeros(s),
    };
    format!("{}e{}", strip_trailing_zeros(mantissa), exp)
}

#[cfg(test)]
mod fmt_tests {
    use super::*;

    #[test]
    fn fmt_g_matches_c_printf_g() {
        // Integers and simple decimals.
        assert_eq!(fmt_g(0.0), "0");
        assert_eq!(fmt_g(1.0), "1");
        assert_eq!(fmt_g(-1.0), "-1");
        assert_eq!(fmt_g(1.5), "1.5");
        assert_eq!(fmt_g(0.1), "0.1");
        assert_eq!(fmt_g(100.0), "100");
        // 6 significant digits, rounding.
        assert_eq!(fmt_g(1.23456789), "1.23457");
        assert_eq!(fmt_g(123456.0), "123456");
        // X >= 6 -> exponential.
        assert_eq!(fmt_g(1234567.0), "1.23457e+06");
        assert_eq!(fmt_g(1e20), "1e+20");
        assert_eq!(fmt_g(1.5e20), "1.5e+20");
        // X < -4 -> exponential.
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.00001), "1e-05");
        assert_eq!(fmt_g(1.5e-10), "1.5e-10");
        // Common GUC reals.
        assert_eq!(fmt_g(0.005), "0.005");
        assert_eq!(fmt_g(1.1), "1.1");
        assert_eq!(fmt_g(4.0), "4");
    }

    #[test]
    fn fmt_e_matches_c_printf_e() {
        // %.*e with REALTYPE_PRECISION-style fixed mantissa digits.
        assert_eq!(fmt_e(1.0, 2), "1.00e+00");
        assert_eq!(fmt_e(1.5, 2), "1.50e+00");
        assert_eq!(fmt_e(0.0, 2), "0.00e+00");
        assert_eq!(fmt_e(-3.5, 2), "-3.50e+00");
        assert_eq!(fmt_e(1234.0, 2), "1.23e+03");
        assert_eq!(fmt_e(0.001, 2), "1.00e-03");
        // 17-digit precision (REALTYPE_PRECISION) round-trips exactly.
        assert_eq!(fmt_e(1.0, 17), "1.00000000000000000e+00");
    }
}
