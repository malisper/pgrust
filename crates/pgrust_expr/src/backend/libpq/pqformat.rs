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
    let mut buffer = ryu::Buffer::new();
    normalize_float_rendering(buffer.format_finite(value), false)
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
    let mut buffer = ryu::Buffer::new();
    normalize_float_rendering(buffer.format_finite(value), true)
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
