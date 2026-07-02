use types_guc::{
    GUC_UNIT, GUC_UNIT_BLOCKS, GUC_UNIT_BYTE, GUC_UNIT_KB, GUC_UNIT_MB, GUC_UNIT_MEMORY,
    GUC_UNIT_MIN, GUC_UNIT_MS, GUC_UNIT_S, GUC_UNIT_XBLOCKS,
};

use crate::cnum::{c_strtod, c_strtol_base0, is_c_space};

const BLCKSZ: i32 = guc_tables::consts::BLCKSZ;
const XLOG_BLCKSZ: i32 = 8192;

pub const MAX_UNIT_LEN: usize = 3;

pub const MEMORY_UNITS_HINT: &str =
    "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".";
pub const TIME_UNITS_HINT: &str =
    "Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".";

#[derive(Clone, Copy)]
struct UnitConversion {
    unit: &'static str,
    base_unit: i32,
    multiplier: f64,
}

const fn uc(unit: &'static str, base_unit: i32, multiplier: f64) -> UnitConversion {
    UnitConversion { unit, base_unit, multiplier }
}

const BLK_KB: f64 = (BLCKSZ / 1024) as f64;
const XBLK_KB: f64 = (XLOG_BLCKSZ / 1024) as f64;

static MEMORY_UNIT_CONVERSION_TABLE: [UnitConversion; 25] = [
    uc("TB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0 * 1024.0),
    uc("GB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0),
    uc("MB", GUC_UNIT_BYTE, 1024.0 * 1024.0),
    uc("kB", GUC_UNIT_BYTE, 1024.0),
    uc("B", GUC_UNIT_BYTE, 1.0),
    uc("TB", GUC_UNIT_KB, 1024.0 * 1024.0 * 1024.0),
    uc("GB", GUC_UNIT_KB, 1024.0 * 1024.0),
    uc("MB", GUC_UNIT_KB, 1024.0),
    uc("kB", GUC_UNIT_KB, 1.0),
    uc("B", GUC_UNIT_KB, 1.0 / 1024.0),
    uc("TB", GUC_UNIT_MB, 1024.0 * 1024.0),
    uc("GB", GUC_UNIT_MB, 1024.0),
    uc("MB", GUC_UNIT_MB, 1.0),
    uc("kB", GUC_UNIT_MB, 1.0 / 1024.0),
    uc("B", GUC_UNIT_MB, 1.0 / (1024.0 * 1024.0)),
    uc("TB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0 * 1024.0) / BLK_KB),
    uc("GB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0) / BLK_KB),
    uc("MB", GUC_UNIT_BLOCKS, 1024.0 / BLK_KB),
    uc("kB", GUC_UNIT_BLOCKS, 1.0 / BLK_KB),
    uc("B", GUC_UNIT_BLOCKS, 1.0 / BLCKSZ as f64),
    uc("TB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0 * 1024.0) / XBLK_KB),
    uc("GB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0) / XBLK_KB),
    uc("MB", GUC_UNIT_XBLOCKS, 1024.0 / XBLK_KB),
    uc("kB", GUC_UNIT_XBLOCKS, 1.0 / XBLK_KB),
    uc("B", GUC_UNIT_XBLOCKS, 1.0 / XLOG_BLCKSZ as f64),
];

static TIME_UNIT_CONVERSION_TABLE: [UnitConversion; 18] = [
    uc("d", GUC_UNIT_MS, (1000 * 60 * 60 * 24) as f64),
    uc("h", GUC_UNIT_MS, (1000 * 60 * 60) as f64),
    uc("min", GUC_UNIT_MS, (1000 * 60) as f64),
    uc("s", GUC_UNIT_MS, 1000.0),
    uc("ms", GUC_UNIT_MS, 1.0),
    uc("us", GUC_UNIT_MS, 1.0 / 1000.0),
    uc("d", GUC_UNIT_S, (60 * 60 * 24) as f64),
    uc("h", GUC_UNIT_S, (60 * 60) as f64),
    uc("min", GUC_UNIT_S, 60.0),
    uc("s", GUC_UNIT_S, 1.0),
    uc("ms", GUC_UNIT_S, 1.0 / 1000.0),
    uc("us", GUC_UNIT_S, 1.0 / (1000.0 * 1000.0)),
    uc("d", GUC_UNIT_MIN, (60 * 24) as f64),
    uc("h", GUC_UNIT_MIN, 60.0),
    uc("min", GUC_UNIT_MIN, 1.0),
    uc("s", GUC_UNIT_MIN, 1.0 / 60.0),
    uc("ms", GUC_UNIT_MIN, 1.0 / (1000.0 * 60.0)),
    uc("us", GUC_UNIT_MIN, 1.0 / (1000.0 * 1000.0 * 60.0)),
];

#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

fn table_for(base_unit: i32) -> &'static [UnitConversion] {
    if base_unit & GUC_UNIT_MEMORY != 0 {
        &MEMORY_UNIT_CONVERSION_TABLE
    } else {
        &TIME_UNIT_CONVERSION_TABLE
    }
}

pub fn convert_to_base_unit(value: f64, unit: &[u8], base_unit: i32) -> Option<f64> {
    let mut unitstr = [0u8; MAX_UNIT_LEN];
    let mut unitlen = 0usize;
    let mut i = 0usize;
    while i < unit.len() && unit[i] != 0 && !is_c_space(unit[i]) && unitlen < MAX_UNIT_LEN {
        unitstr[unitlen] = unit[i];
        unitlen += 1;
        i += 1;
    }
    while i < unit.len() && is_c_space(unit[i]) {
        i += 1;
    }
    if i < unit.len() && unit[i] != 0 {
        return None;
    }
    let unitstr = &unitstr[..unitlen];

    let table = table_for(base_unit);
    for (idx, entry) in table.iter().enumerate() {
        if base_unit == entry.base_unit && unitstr == entry.unit.as_bytes() {
            let mut cvalue = value * entry.multiplier;
            // Round a fractional value to the nearest multiple of the next
            // smaller unit of the same base unit.
            if let Some(next) = table.get(idx + 1) {
                if base_unit == next.base_unit {
                    cvalue = rint(cvalue / next.multiplier) * next.multiplier;
                }
            }
            return Some(cvalue);
        }
    }
    None
}

pub fn convert_int_from_base_unit(base_value: i64, base_unit: i32) -> (i64, &'static str) {
    for entry in table_for(base_unit) {
        if base_unit == entry.base_unit
            && (entry.multiplier <= 1.0 || base_value % (entry.multiplier as i64) == 0)
        {
            return (rint(base_value as f64 / entry.multiplier) as i64, entry.unit);
        }
    }
    (base_value, "")
}

pub fn convert_real_from_base_unit(base_value: f64, base_unit: i32) -> (f64, &'static str) {
    let mut value = base_value;
    let mut unit = "";
    for entry in table_for(base_unit) {
        if base_unit == entry.base_unit {
            value = base_value / entry.multiplier;
            unit = entry.unit;
            if value > 0.0 && ((rint(value) / value) - 1.0).abs() <= 1e-8 {
                break;
            }
        }
    }
    (value, unit)
}

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
        _ => None,
    }
}

const BLCKSZ_KB_STR: &str = "8kB";
const XLOG_BLCKSZ_KB_STR: &str = "8kB";
const _: () = {
    assert!(BLCKSZ / 1024 == 8);
    assert!(XLOG_BLCKSZ / 1024 == 8);
};

pub enum ParseNum<T> {
    Ok(T),
    Err { hint: Option<&'static str> },
}

pub fn parse_int(value: &str, flags: i32) -> ParseNum<i32> {
    let bytes = value.as_bytes();

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

    while endptr < bytes.len() && is_c_space(bytes[endptr]) {
        endptr += 1;
    }

    if endptr < bytes.len() && bytes[endptr] != 0 {
        if flags & GUC_UNIT == 0 {
            return ParseNum::Err { hint: None };
        }
        match convert_to_base_unit(val, &bytes[endptr..], flags & GUC_UNIT) {
            Some(cv) => val = cv,
            None => return ParseNum::Err { hint: Some(units_hint(flags)) },
        }
    }

    val = rint(val);
    if val > i32::MAX as f64 || val < i32::MIN as f64 {
        return ParseNum::Err { hint: Some("Value exceeds integer range.") };
    }

    ParseNum::Ok(val as i32)
}

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
            None => return ParseNum::Err { hint: Some(units_hint(flags)) },
        }
    }

    ParseNum::Ok(val)
}

fn units_hint(flags: i32) -> &'static str {
    if flags & GUC_UNIT_MEMORY != 0 {
        MEMORY_UNITS_HINT
    } else {
        TIME_UNITS_HINT
    }
}

// C printf "%g" (default precision 6): %e style when X < -4 || X >= P, else %f
// style; trailing zeros and a bare '.' stripped.
pub fn fmt_g(v: f64) -> String {
    fmt_g_prec(v, 6)
}

pub fn fmt_g_prec(v: f64, precision: usize) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf".to_string() } else { "inf".to_string() };
    }

    let p = precision.max(1);
    if v == 0.0 {
        return if v.is_sign_negative() { "-0".to_string() } else { "0".to_string() };
    }

    let e_str = format!("{:.*e}", p - 1, v);
    let x: i32 = e_str.rsplit('e').next().and_then(|s| s.parse().ok()).unwrap_or(0);

    if x < -4 || x >= p as i32 {
        format_e_style(v, p - 1)
    } else {
        let f_prec = (p as i32 - 1 - x).max(0) as usize;
        strip_trailing_zeros(&format!("{:.*}", f_prec, v))
    }
}

pub fn fmt_e(v: f64, precision: usize) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf".to_string() } else { "inf".to_string() };
    }
    normalize_e(&format!("{:.*e}", precision, v))
}

fn format_e_style(v: f64, mantissa_prec: usize) -> String {
    strip_trailing_zeros_e(&normalize_e(&format!("{:.*e}", mantissa_prec, v)))
}

// Rust {:e} ("1.5e-3") -> C %e ("1.500000e-03"): force exponent sign, pad to
// two digits.
fn normalize_e(rust_e: &str) -> String {
    let Some((mantissa, exp)) = rust_e.split_once('e') else {
        return rust_e.to_string();
    };
    let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
        ('-', rest)
    } else if let Some(rest) = exp.strip_prefix('+') {
        ('+', rest)
    } else {
        ('+', exp)
    };
    format!("{mantissa}e{sign}{digits:0>2}")
}

fn strip_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let trimmed = s.trim_end_matches('0');
    trimmed.strip_suffix('.').unwrap_or(trimmed).to_string()
}

fn strip_trailing_zeros_e(s: &str) -> String {
    let Some((mantissa, exp)) = s.split_once('e') else {
        return strip_trailing_zeros(s);
    };
    format!("{}e{}", strip_trailing_zeros(mantissa), exp)
}
