use std::str::FromStr;

use num_bigint::BigInt;
use num_traits::One;

use crate::compat::pgrust::session::ByteaOutputFormat;
use crate::error::ExprError;
use crate::expr_backend::utils::misc::guc_datetime::DateTimeConfig;

#[derive(Debug, Clone)]
pub struct FloatFormatOptions {
    pub extra_float_digits: i32,
    pub bytea_output: ByteaOutputFormat,
    pub datetime_config: DateTimeConfig,
}

impl Default for FloatFormatOptions {
    fn default() -> Self {
        Self {
            extra_float_digits: 1,
            bytea_output: ByteaOutputFormat::Hex,
            datetime_config: DateTimeConfig::default(),
        }
    }
}

pub fn format_bytea_text(bytes: &[u8], output: ByteaOutputFormat) -> String {
    match output {
        ByteaOutputFormat::Hex => {
            let mut out = String::with_capacity(bytes.len() * 2 + 2);
            out.push_str("\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            out
        }
        ByteaOutputFormat::Escape => {
            let mut out = String::new();
            for byte in bytes {
                match *byte {
                    b'\\' => out.push_str("\\\\"),
                    0x20..=0x7e => out.push(char::from(*byte)),
                    other => out.push_str(&format!("\\{:03o}", other)),
                }
            }
            out
        }
    }
}

pub fn format_float8_text(value: f64, options: FloatFormatOptions) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f64::INFINITY {
        return "Infinity".into();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".into();
    }

    if options.extra_float_digits <= 0 {
        return format_float_with_precision(value, 15 + options.extra_float_digits);
    }
    format_float_shortest(value, false)
}

pub fn format_float4_text(value: f64, options: FloatFormatOptions) -> String {
    let value = value as f32;
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f32::INFINITY {
        return "Infinity".into();
    }
    if value == f32::NEG_INFINITY {
        return "-Infinity".into();
    }

    if options.extra_float_digits <= 0 {
        return format_float_with_precision(value as f64, 6 + options.extra_float_digits);
    }
    format_float_shortest(value as f64, true)
}

fn format_float_shortest(value: f64, is_float4: bool) -> String {
    let normalized = if is_float4 {
        let mut buffer = ryu::Buffer::new();
        normalize_float_rendering(buffer.format_finite(value as f32), true)
    } else {
        let mut buffer = ryu::Buffer::new();
        normalize_float_rendering(buffer.format_finite(value), false)
    };
    if let Some(repaired) = repair_midpoint_render(value, is_float4, &normalized) {
        repaired
    } else {
        normalized
    }
}

#[derive(Clone)]
struct ExactRational {
    num: BigInt,
    den: BigInt,
}

fn repair_midpoint_render(value: f64, is_float4: bool, shortest: &str) -> Option<String> {
    if !is_exact_midpoint_render(value, is_float4, shortest) {
        return None;
    }

    let start_digits = significand_digit_count(shortest);
    let max_digits = if is_float4 { 9 } else { 17 };
    for digits in (start_digits + 1)..=max_digits {
        let candidate = rounded_decimal_candidate(value, is_float4, digits);
        if !parses_same_float(&candidate, value, is_float4) {
            continue;
        }
        if !is_exact_midpoint_render(value, is_float4, &candidate) {
            return Some(candidate);
        }
    }

    None
}

fn rounded_decimal_candidate(value: f64, is_float4: bool, digits: usize) -> String {
    let precision = digits.saturating_sub(1);
    let raw = if is_float4 {
        format!("{:.*e}", precision, value as f32)
    } else {
        format!("{:.*e}", precision, value)
    };
    normalize_float_rendering(&raw, is_float4)
}

fn parses_same_float(candidate: &str, value: f64, is_float4: bool) -> bool {
    if is_float4 {
        candidate
            .parse::<f32>()
            .map(|parsed| parsed.to_bits() == (value as f32).to_bits())
            .unwrap_or(false)
    } else {
        candidate
            .parse::<f64>()
            .map(|parsed| parsed.to_bits() == value.to_bits())
            .unwrap_or(false)
    }
}

fn is_exact_midpoint_render(value: f64, is_float4: bool, rendered: &str) -> bool {
    let Some(candidate) = decimal_rational(rendered) else {
        return false;
    };

    if is_float4 {
        let target = value as f32;
        if !target.is_finite() {
            return false;
        }
        let exact = rational_from_f32(target);
        let lower = rational_from_f32(next_down_f32(target));
        let upper = rational_from_f32(next_up_f32(target));
        rational_is_midpoint(&candidate, &lower, &exact)
            || rational_is_midpoint(&candidate, &exact, &upper)
    } else {
        if !value.is_finite() {
            return false;
        }
        let exact = rational_from_f64(value);
        let lower = rational_from_f64(next_down_f64(value));
        let upper = rational_from_f64(next_up_f64(value));
        rational_is_midpoint(&candidate, &lower, &exact)
            || rational_is_midpoint(&candidate, &exact, &upper)
    }
}

fn significand_digit_count(text: &str) -> usize {
    let unsigned = text.trim_start_matches('-');
    let significand = unsigned
        .split_once(['e', 'E'])
        .map(|(mantissa, _)| mantissa)
        .unwrap_or(unsigned);
    let digits = significand.replace('.', "");
    let trimmed = digits.trim_start_matches('0');
    trimmed.len().max(1)
}

fn decimal_rational(text: &str) -> Option<ExactRational> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exp)) => (mantissa, exp.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(whole);
    digits.push_str(frac);
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(ExactRational {
            num: BigInt::from(0u8),
            den: BigInt::one(),
        });
    }

    let mut num = BigInt::from_str(digits).ok()?;
    let scale = frac.len() as i32 - exponent;
    let den = if scale >= 0 {
        pow10(scale as u32)
    } else {
        num *= pow10((-scale) as u32);
        BigInt::one()
    };
    if negative {
        num = -num;
    }
    Some(ExactRational { num, den })
}

fn rational_from_f64(value: f64) -> ExactRational {
    let bits = value.to_bits();
    let negative = (bits >> 63) != 0;
    let ieee_mantissa = bits & ((1u64 << 52) - 1);
    let ieee_exponent = ((bits >> 52) & 0x7ff) as i32;
    let (mantissa, exp2) = if ieee_exponent == 0 {
        (ieee_mantissa, 1 - 1023 - 52)
    } else {
        ((1u64 << 52) | ieee_mantissa, ieee_exponent - 1023 - 52)
    };
    rational_from_binary_parts(negative, BigInt::from(mantissa), exp2)
}

fn rational_from_f32(value: f32) -> ExactRational {
    let bits = value.to_bits();
    let negative = (bits >> 31) != 0;
    let ieee_mantissa = bits & ((1u32 << 23) - 1);
    let ieee_exponent = ((bits >> 23) & 0xff) as i32;
    let (mantissa, exp2) = if ieee_exponent == 0 {
        (ieee_mantissa, 1 - 127 - 23)
    } else {
        ((1u32 << 23) | ieee_mantissa, ieee_exponent - 127 - 23)
    };
    rational_from_binary_parts(negative, BigInt::from(mantissa), exp2)
}

fn rational_from_binary_parts(negative: bool, mut num: BigInt, exp2: i32) -> ExactRational {
    if negative {
        num = -num;
    }
    if exp2 >= 0 {
        num <<= exp2 as usize;
        ExactRational {
            num,
            den: BigInt::one(),
        }
    } else {
        ExactRational {
            num,
            den: BigInt::one() << (-exp2 as usize),
        }
    }
}

fn rational_is_midpoint(
    candidate: &ExactRational,
    left: &ExactRational,
    right: &ExactRational,
) -> bool {
    let lhs = &candidate.num * BigInt::from(2u8) * &left.den * &right.den;
    let rhs = &candidate.den * (&left.num * &right.den + &right.num * &left.den);
    lhs == rhs
}

fn pow10(exp: u32) -> BigInt {
    BigInt::from(10u8).pow(exp)
}

fn next_up_f64(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f64::from_bits(bits - 1)
    } else {
        f64::from_bits(bits + 1)
    }
}

fn next_down_f64(value: f64) -> f64 {
    if value.is_nan() || value == f64::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits((1u64 << 63) | 1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}

fn next_up_f32(value: f32) -> f32 {
    if value.is_nan() || value == f32::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits(1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    }
}

fn next_down_f32(value: f32) -> f32 {
    if value.is_nan() || value == f32::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits((1u32 << 31) | 1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    }
}

fn format_float_with_precision(value: f64, precision: i32) -> String {
    let precision = precision.clamp(1, 32) as usize;
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let abs = value.abs();
    if abs == 0.0 {
        return format!("{sign}0");
    }

    let rendered = format!("{:.*e}", precision - 1, abs);
    let (mantissa, exponent) = rendered.split_once('e').unwrap_or((&rendered, "0"));
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    let digits = mantissa.replace('.', "");
    let body = if exponent < -4 || exponent >= precision as i32 {
        let mantissa = trim_fractional_zeros(mantissa);
        format_scientific_mantissa(mantissa, exponent, true)
    } else {
        let decimal_pos = exponent + 1;
        let rendered = if decimal_pos <= 0 {
            format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits)
        } else if decimal_pos as usize >= digits.len() {
            format!(
                "{digits}{}",
                "0".repeat(decimal_pos as usize - digits.len())
            )
        } else {
            format!(
                "{}.{}",
                &digits[..decimal_pos as usize],
                &digits[decimal_pos as usize..]
            )
        };
        trim_fractional_zeros(&rendered).to_string()
    };
    format!("{sign}{body}")
}

fn normalize_float_rendering(raw: &str, is_float4: bool) -> String {
    let (sign, unsigned) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw),
    };
    let scientific_threshold = if is_float4 { 6 } else { 15 };

    let (mut digits, exponent) = if let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E'])
    {
        let exponent = exponent.parse::<i32>().unwrap_or(0);
        let fractional_digits = mantissa
            .split_once('.')
            .map(|(_, frac)| frac.len())
            .unwrap_or(0);
        (
            mantissa.replace('.', ""),
            exponent - fractional_digits as i32,
        )
    } else if let Some((whole, frac)) = unsigned.split_once('.') {
        (format!("{whole}{frac}"), -(frac.len() as i32))
    } else {
        (unsigned.to_string(), 0)
    };

    digits = digits.trim_start_matches('0').to_string();
    if digits.is_empty() {
        return format!("{sign}0");
    }

    let display_exponent = exponent + digits.len() as i32 - 1;
    if display_exponent < -4 || display_exponent >= scientific_threshold {
        let significant_digits = digits.trim_end_matches('0');
        let mantissa = if significant_digits.len() == 1 {
            significant_digits.to_string()
        } else {
            format!("{}.{}", &significant_digits[..1], &significant_digits[1..])
        };
        return format!(
            "{sign}{}",
            format_scientific_mantissa(&mantissa, display_exponent, true)
        );
    }

    if exponent >= 0 {
        digits.push_str(&"0".repeat(exponent as usize));
        return format!("{sign}{digits}");
    }

    let decimal_pos = digits.len() as i32 + exponent;
    let rendered = if decimal_pos > 0 {
        format!(
            "{}.{}",
            &digits[..decimal_pos as usize],
            &digits[decimal_pos as usize..]
        )
    } else {
        format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits)
    };
    format!("{sign}{}", trim_fractional_zeros(&rendered))
}

fn format_scientific_mantissa(mantissa: &str, exponent: i32, pad_exponent: bool) -> String {
    let mantissa = trim_fractional_zeros(mantissa);
    if pad_exponent {
        let sign = if exponent < 0 { '-' } else { '+' };
        let digits = exponent.abs();
        if digits < 10 {
            return format!("{mantissa}e{sign}0{digits}");
        }
        return format!("{mantissa}e{sign}{digits}");
    }
    format!("{mantissa}e{exponent:+}")
}

fn trim_fractional_zeros(text: &str) -> &str {
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-" {
        if text.starts_with('-') { "-0" } else { "0" }
    } else {
        trimmed
    }
}

pub fn format_exec_error(error: &ExprError) -> String {
    match error {
        ExprError::WithContext { source, .. } => format_exec_error(source),
        ExprError::Parse(error) => format!("{error:?}"),
        ExprError::DetailedError { message, .. }
        | ExprError::DiagnosticError { message, .. }
        | ExprError::JsonInput { message, .. }
        | ExprError::XmlInput { message, .. }
        | ExprError::ArrayInput { message, .. } => message.clone(),
        ExprError::DivisionByZero(_) => "division by zero".into(),
        ExprError::InvalidByteaInput { .. } => "invalid input syntax for type bytea".into(),
        ExprError::InvalidUuidInput { value } => {
            format!("invalid input syntax for type uuid: \"{value}\"")
        }
        other => format!("{other:?}"),
    }
}
