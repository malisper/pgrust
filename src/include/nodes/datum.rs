use crate::pgrust::compact_string::CompactString;
use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, Zero};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BitString {
    pub bit_len: i32,
    pub bytes: Vec<u8>,
}

impl BitString {
    pub fn new(bit_len: i32, mut bytes: Vec<u8>) -> Self {
        let required = Self::byte_len(bit_len);
        bytes.truncate(required);
        if bytes.len() < required {
            bytes.resize(required, 0);
        }
        if let Some(last) = bytes.last_mut() {
            let used_bits = (bit_len as usize) % 8;
            if used_bits != 0 {
                *last &= 0xff << (8 - used_bits);
            }
        }
        Self { bit_len, bytes }
    }

    pub fn byte_len(bit_len: i32) -> usize {
        (bit_len.max(0) as usize).div_ceil(8)
    }

    pub fn render(&self) -> String {
        let mut out = String::with_capacity(self.bit_len.max(0) as usize);
        for bit_idx in 0..self.bit_len.max(0) as usize {
            let byte = self.bytes[bit_idx / 8];
            let shift = 7 - (bit_idx % 8);
            out.push(if ((byte >> shift) & 1) != 0 { '1' } else { '0' });
        }
        out
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Bit(BitString),
    Bytea(Vec<u8>),
    Float64(f64),
    Numeric(NumericValue),
    Json(CompactString),
    Jsonb(Vec<u8>),
    JsonPath(CompactString),
    Text(CompactString),
    /// Raw pointer to on-page text bytes. Valid while the buffer page is pinned.
    TextRef(*const u8, u32),
    InternalChar(u8),
    Bool(bool),
    Array(Vec<Value>),
    Null,
}

#[derive(Debug, Clone)]
pub enum NumericValue {
    Finite { coeff: BigInt, scale: u32 },
    NaN,
}

impl NumericValue {
    pub fn zero() -> Self {
        Self::Finite {
            coeff: BigInt::zero(),
            scale: 0,
        }
    }

    pub fn from_i64(value: i64) -> Self {
        Self::Finite {
            coeff: BigInt::from(value),
            scale: 0,
        }
    }

    pub fn normalize(self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite {
                mut coeff,
                mut scale,
            } => {
                if coeff.is_zero() {
                    return Self::Finite { coeff, scale };
                }
                let ten = BigInt::from(10u8);
                while scale > 0 {
                    let (q, r) = coeff.div_rem(&ten);
                    if !r.is_zero() {
                        break;
                    }
                    coeff = q;
                    scale -= 1;
                }
                Self::Finite { coeff, scale }
            }
        }
    }

    fn canonical_eq(&self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite { coeff, .. } if coeff.is_zero() => Self::zero(),
            _ => self.clone().normalize(),
        }
    }

    pub fn digit_count(&self) -> i32 {
        match self {
            Self::NaN => 0,
            Self::Finite { coeff, .. } => coeff
                .to_str_radix(10)
                .trim_start_matches('-')
                .trim_start_matches('0')
                .len()
                .max(1) as i32,
        }
    }

    pub fn negate(&self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite { coeff, scale } => Self::Finite {
                coeff: -coeff.clone(),
                scale: *scale,
            },
        }
    }

    pub fn abs(&self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite { coeff, scale } => Self::Finite {
                coeff: coeff.abs(),
                scale: *scale,
            },
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::NaN => "NaN".to_string(),
            Self::Finite { coeff, scale } => {
                let negative = coeff.is_negative();
                let digits = coeff.abs().to_str_radix(10);
                if *scale == 0 {
                    if negative {
                        format!("-{digits}")
                    } else {
                        digits
                    }
                } else {
                    let scale = *scale as usize;
                    let mut out = String::new();
                    if negative {
                        out.push('-');
                    }
                    if digits.len() <= scale {
                        out.push('0');
                        out.push('.');
                        for _ in 0..(scale - digits.len()) {
                            out.push('0');
                        }
                        out.push_str(&digits);
                    } else {
                        let split = digits.len() - scale;
                        out.push_str(&digits[..split]);
                        out.push('.');
                        out.push_str(&digits[split..]);
                    }
                    out
                }
            }
        }
    }
}

impl PartialEq for NumericValue {
    fn eq(&self, other: &Self) -> bool {
        match (self.canonical_eq(), other.canonical_eq()) {
            (Self::NaN, Self::NaN) => true,
            (
                Self::Finite {
                    coeff: left_coeff,
                    scale: left_scale,
                },
                Self::Finite {
                    coeff: right_coeff,
                    scale: right_scale,
                },
            ) => left_coeff == right_coeff && left_scale == right_scale,
            _ => false,
        }
    }
}

impl Eq for NumericValue {}

impl Hash for NumericValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.canonical_eq() {
            Self::NaN => {
                0u8.hash(state);
            }
            Self::Finite { coeff, scale } => {
                1u8.hash(state);
                coeff.hash(state);
                scale.hash(state);
            }
        }
    }
}

impl From<&str> for NumericValue {
    fn from(value: &str) -> Self {
        parse_numeric_literal(value).unwrap_or_else(NumericValue::zero)
    }
}

impl From<String> for NumericValue {
    fn from(value: String) -> Self {
        NumericValue::from(value.as_str())
    }
}

fn parse_numeric_literal(text: &str) -> Option<NumericValue> {
    if text.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => (&trimmed[..index], trimmed[index + 1..].parse::<i32>().ok()?),
        None => (trimmed, 0),
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let parts: Vec<&str> = unsigned.split('.').collect();
    if parts.len() > 2 {
        return None;
    }
    let whole = parts[0];
    let frac = parts.get(1).copied().unwrap_or("");
    if (!whole.is_empty() && !whole.chars().all(|ch| ch.is_ascii_digit()))
        || !frac.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{frac}");
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = frac.len() as i32 - exponent;
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', (-scale) as usize));
        scale = 0;
    }
    let mut coeff = digits.parse::<BigInt>().ok()?;
    if negative {
        coeff = -coeff;
    }
    Some(
        NumericValue::Finite {
            coeff,
            scale: scale as u32,
        }
        .normalize(),
    )
}

impl Value {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::JsonPath(s) => Some(s.as_str()),
            Value::Text(s) => Some(s.as_str()),
            Value::TextRef(ptr, len) => Some(unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
            }),
            _ => None,
        }
    }

    pub fn to_owned_value(&self) -> Value {
        match self {
            Value::Int16(v) => Value::Int16(*v),
            Value::Int32(v) => Value::Int32(*v),
            Value::Int64(v) => Value::Int64(*v),
            Value::Bit(v) => Value::Bit(v.clone()),
            Value::Bytea(v) => Value::Bytea(v.clone()),
            Value::Float64(v) => Value::Float64(*v),
            Value::Numeric(v) => Value::Numeric(v.clone()),
            Value::Json(s) => Value::Json(s.clone()),
            Value::Jsonb(bytes) => Value::Jsonb(bytes.clone()),
            Value::JsonPath(s) => Value::JsonPath(s.clone()),
            Value::TextRef(ptr, len) => {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                Value::Text(CompactString::new(s))
            }
            Value::Text(s) => Value::Text(s.clone()),
            Value::InternalChar(v) => Value::InternalChar(*v),
            Value::Bool(v) => Value::Bool(*v),
            Value::Array(values) => Value::Array(values.iter().map(Value::to_owned_value).collect()),
            Value::Null => Value::Null,
        }
    }

    pub fn materialize_all(values: &mut Vec<Value>) {
        for v in values.iter_mut() {
            if let Value::TextRef(ptr, len) = *v {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
                };
                *v = Value::Text(CompactString::new(s));
            } else if let Value::Array(items) = v {
                for item in items.iter_mut() {
                    *item = item.to_owned_value();
                }
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Bit(a), Value::Bit(b)) => a == b,
            (Value::Bytea(a), Value::Bytea(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Jsonb(a), Value::Jsonb(b)) => a == b,
            (Value::JsonPath(a), Value::JsonPath(b)) => a == b,
            (Value::InternalChar(a), Value::InternalChar(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
                a.as_text().unwrap() == b.as_text().unwrap()
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Int16(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            Value::Int32(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            Value::Int64(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            Value::Bit(v) => {
                14u8.hash(state);
                v.hash(state);
            }
            Value::Bytea(v) => {
                13u8.hash(state);
                v.hash(state);
            }
            Value::Float64(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::Numeric(v) => {
                4u8.hash(state);
                v.hash(state);
            }
            Value::Json(s) => {
                9u8.hash(state);
                s.as_str().hash(state);
            }
            Value::Jsonb(bytes) => {
                10u8.hash(state);
                bytes.hash(state);
            }
            Value::JsonPath(s) => {
                11u8.hash(state);
                s.as_str().hash(state);
            }
            Value::Text(s) => {
                5u8.hash(state);
                s.as_str().hash(state);
            }
            Value::TextRef(ptr, len) => {
                5u8.hash(state);
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                s.hash(state);
            }
            Value::InternalChar(v) => {
                12u8.hash(state);
                v.hash(state);
            }
            Value::Bool(v) => {
                6u8.hash(state);
                v.hash(state);
            }
            Value::Array(values) => {
                7u8.hash(state);
                values.hash(state);
            }
            Value::Null => {
                8u8.hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NumericValue;

    #[test]
    fn numeric_zero_preserves_display_scale() {
        assert_eq!(NumericValue::from("0.0").render(), "0.0");
        assert_eq!(NumericValue::from("0.00").render(), "0.00");
    }

    #[test]
    fn numeric_zero_equality_ignores_scale() {
        assert_eq!(NumericValue::from("0"), NumericValue::from("0.0"));
        assert_eq!(NumericValue::from("0.0"), NumericValue::from("0.00"));
    }
}

unsafe impl Send for Value {}
unsafe impl Sync for Value {}
