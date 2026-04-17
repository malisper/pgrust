use crate::include::nodes::datetime::{DateADT, TimeADT, TimeTzADT, TimestampADT, TimestampTzADT};
use crate::include::nodes::tsearch::{TsQuery, TsVector};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrayDimension {
    pub lower_bound: i32,
    pub length: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrayValue {
    pub element_type_oid: Option<u32>,
    pub dimensions: Vec<ArrayDimension>,
    pub elements: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordValue {
    pub fields: Vec<(String, Value)>,
}

impl ArrayValue {
    pub fn empty() -> Self {
        Self {
            element_type_oid: None,
            dimensions: Vec::new(),
            elements: Vec::new(),
        }
    }

    pub fn from_1d(elements: Vec<Value>) -> Self {
        if elements.is_empty() {
            Self::empty()
        } else {
            Self {
                element_type_oid: None,
                dimensions: vec![ArrayDimension {
                    lower_bound: 1,
                    length: elements.len(),
                }],
                elements,
            }
        }
    }

    pub fn from_dimensions(dimensions: Vec<ArrayDimension>, elements: Vec<Value>) -> Self {
        Self {
            element_type_oid: None,
            dimensions,
            elements,
        }
    }

    pub fn with_element_type_oid(mut self, element_type_oid: u32) -> Self {
        self.element_type_oid = Some(element_type_oid);
        self
    }

    pub fn ndim(&self) -> usize {
        self.dimensions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn lower_bound(&self, dim: usize) -> Option<i32> {
        self.dimensions.get(dim).map(|dim| dim.lower_bound)
    }

    pub fn upper_bound(&self, dim: usize) -> Option<i32> {
        self.dimensions
            .get(dim)
            .map(|entry| entry.lower_bound + entry.length as i32 - 1)
    }

    pub fn axis_len(&self, dim: usize) -> Option<usize> {
        self.dimensions.get(dim).map(|dim| dim.length)
    }

    pub fn shape(&self) -> Vec<usize> {
        self.dimensions.iter().map(|dim| dim.length).collect()
    }

    pub fn to_owned_value(&self) -> Self {
        Self {
            element_type_oid: self.element_type_oid,
            dimensions: self.dimensions.clone(),
            elements: self.elements.iter().map(Value::to_owned_value).collect(),
        }
    }

    pub fn from_nested_values(values: Vec<Value>, lower_bounds: Vec<i32>) -> Result<Self, String> {
        let mut lengths = Vec::new();
        let mut elements = Vec::new();
        flatten_nested_values(values, 0, &mut lengths, &mut elements)?;
        if lengths.is_empty() {
            return Ok(Self::empty());
        }
        let dimensions = lengths
            .into_iter()
            .enumerate()
            .map(|(idx, length)| ArrayDimension {
                lower_bound: lower_bounds.get(idx).copied().unwrap_or(1),
                length,
            })
            .collect();
        Ok(Self {
            element_type_oid: None,
            dimensions,
            elements,
        })
    }

    pub fn to_nested_values(&self) -> Vec<Value> {
        if self.dimensions.is_empty() {
            return Vec::new();
        }
        let mut offset = 0usize;
        build_nested_values(self, 0, &mut offset)
    }
}

pub fn array_value_from_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::Array(items) => ArrayValue::from_nested_values(items.clone(), vec![1]).ok(),
        Value::PgArray(array) => Some(array.clone()),
        _ => None,
    }
}

fn flatten_nested_values(
    values: Vec<Value>,
    depth: usize,
    lengths: &mut Vec<usize>,
    elements: &mut Vec<Value>,
) -> Result<(), String> {
    set_array_length(lengths, depth, values.len())?;
    if values.is_empty() {
        return Ok(());
    }
    let all_arrays = values
        .iter()
        .all(|value| matches!(value, Value::Array(_) | Value::PgArray(_)));
    let any_arrays = values
        .iter()
        .any(|value| matches!(value, Value::Array(_) | Value::PgArray(_)));
    if any_arrays && !all_arrays {
        return Err("multidimensional arrays must have matching extents".into());
    }
    if all_arrays {
        for value in values {
            let nested = match value {
                Value::Array(values) => values,
                Value::PgArray(array) => array.to_nested_values(),
                _ => unreachable!(),
            };
            flatten_nested_values(nested, depth + 1, lengths, elements)?;
        }
        return Ok(());
    }
    elements.extend(values);
    Ok(())
}

fn set_array_length(lengths: &mut Vec<usize>, depth: usize, length: usize) -> Result<(), String> {
    if let Some(existing) = lengths.get(depth) {
        if *existing != length {
            return Err("multidimensional arrays must have matching extents".into());
        }
        return Ok(());
    }
    lengths.push(length);
    Ok(())
}

fn build_nested_values(array: &ArrayValue, depth: usize, offset: &mut usize) -> Vec<Value> {
    let len = array.dimensions[depth].length;
    if depth + 1 == array.dimensions.len() {
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(array.elements[*offset].clone());
            *offset += 1;
        }
        return out;
    }
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let nested = ArrayValue {
            element_type_oid: None,
            dimensions: array.dimensions[depth + 1..].to_vec(),
            elements: {
                let start = *offset;
                let width = array.dimensions[depth + 1..]
                    .iter()
                    .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
                *offset += width;
                array.elements[start..start + width].to_vec()
            },
        };
        out.push(Value::PgArray(nested));
    }
    out
}

#[derive(Debug, Clone)]
pub struct GeoPoint {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone)]
pub struct GeoLseg {
    pub p: [GeoPoint; 2],
}

#[derive(Debug, Clone)]
pub struct GeoPath {
    pub closed: bool,
    pub points: Vec<GeoPoint>,
}

#[derive(Debug, Clone)]
pub struct GeoLine {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

#[derive(Debug, Clone)]
pub struct GeoBox {
    pub high: GeoPoint,
    pub low: GeoPoint,
}

#[derive(Debug, Clone)]
pub struct GeoPolygon {
    pub bound_box: GeoBox,
    pub points: Vec<GeoPoint>,
}

#[derive(Debug, Clone)]
pub struct GeoCircle {
    pub center: GeoPoint,
    pub radius: f64,
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
    Money(i64),
    Date(DateADT),
    Time(TimeADT),
    TimeTz(TimeTzADT),
    Timestamp(TimestampADT),
    TimestampTz(TimestampTzADT),
    Bit(BitString),
    Bytea(Vec<u8>),
    Point(GeoPoint),
    Lseg(GeoLseg),
    Path(GeoPath),
    Line(GeoLine),
    Box(GeoBox),
    Polygon(GeoPolygon),
    Circle(GeoCircle),
    Float64(f64),
    Numeric(NumericValue),
    Json(CompactString),
    Jsonb(Vec<u8>),
    JsonPath(CompactString),
    TsVector(TsVector),
    TsQuery(TsQuery),
    Text(CompactString),
    /// Raw pointer to on-page text bytes. Valid while the buffer page is pinned.
    TextRef(*const u8, u32),
    InternalChar(u8),
    Bool(bool),
    Array(Vec<Value>),
    PgArray(ArrayValue),
    Record(RecordValue),
    Null,
}

#[derive(Debug, Clone)]
pub enum NumericValue {
    Finite {
        coeff: BigInt,
        scale: u32,
        dscale: u32,
    },
    PosInf,
    NegInf,
    NaN,
}

impl NumericValue {
    pub fn zero() -> Self {
        Self::Finite {
            coeff: BigInt::zero(),
            scale: 0,
            dscale: 0,
        }
    }

    pub fn from_i64(value: i64) -> Self {
        Self::Finite {
            coeff: BigInt::from(value),
            scale: 0,
            dscale: 0,
        }
    }

    pub fn finite(coeff: BigInt, scale: u32) -> Self {
        Self::Finite {
            coeff,
            scale,
            dscale: scale,
        }
    }

    pub fn with_dscale(self, dscale: u32) -> Self {
        match self {
            Self::Finite { coeff, scale, .. } => Self::Finite {
                coeff,
                scale,
                dscale,
            },
            other => other,
        }
    }

    pub fn normalize(self) -> Self {
        match self {
            Self::PosInf | Self::NegInf => self,
            Self::NaN => Self::NaN,
            Self::Finite {
                mut coeff,
                mut scale,
                dscale,
            } => {
                if coeff.is_zero() {
                    return Self::Finite {
                        coeff,
                        scale,
                        dscale,
                    };
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
                Self::Finite {
                    coeff,
                    scale,
                    dscale,
                }
            }
        }
    }

    fn canonical_eq(&self) -> Self {
        match self {
            Self::PosInf | Self::NegInf => self.clone(),
            Self::NaN => Self::NaN,
            Self::Finite { coeff, .. } if coeff.is_zero() => Self::zero(),
            _ => self.clone().normalize(),
        }
    }

    pub fn digit_count(&self) -> i32 {
        match self {
            Self::PosInf | Self::NegInf | Self::NaN => 0,
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
            Self::PosInf => Self::NegInf,
            Self::NegInf => Self::PosInf,
            Self::NaN => Self::NaN,
            Self::Finite {
                coeff,
                scale,
                dscale,
            } => Self::Finite {
                coeff: -coeff.clone(),
                scale: *scale,
                dscale: *dscale,
            },
        }
    }

    pub fn abs(&self) -> Self {
        match self {
            Self::PosInf | Self::NegInf => Self::PosInf,
            Self::NaN => Self::NaN,
            Self::Finite {
                coeff,
                scale,
                dscale,
            } => Self::Finite {
                coeff: coeff.abs(),
                scale: *scale,
                dscale: *dscale,
            },
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::PosInf => "Infinity".to_string(),
            Self::NegInf => "-Infinity".to_string(),
            Self::NaN => "NaN".to_string(),
            Self::Finite {
                coeff,
                scale,
                dscale,
            } => {
                let negative = coeff.is_negative();
                let digits = coeff.abs().to_str_radix(10);
                if *scale == 0 {
                    let mut out = if negative {
                        format!("-{digits}")
                    } else {
                        digits
                    };
                    if *dscale > 0 {
                        out.push('.');
                        for _ in 0..*dscale {
                            out.push('0');
                        }
                    }
                    out
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
                    if (*dscale as usize) > scale {
                        for _ in 0..((*dscale as usize) - scale) {
                            out.push('0');
                        }
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
            (Self::PosInf, Self::PosInf) | (Self::NegInf, Self::NegInf) => true,
            (Self::NaN, Self::NaN) => true,
            (
                Self::Finite {
                    coeff: left_coeff,
                    scale: left_scale,
                    ..
                },
                Self::Finite {
                    coeff: right_coeff,
                    scale: right_scale,
                    ..
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
            Self::PosInf => {
                1u8.hash(state);
            }
            Self::NegInf => {
                2u8.hash(state);
            }
            Self::Finite { coeff, scale, .. } => {
                3u8.hash(state);
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
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => return Some(NumericValue::PosInf),
        "-inf" | "-infinity" => return Some(NumericValue::NegInf),
        _ => {}
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
    Some(NumericValue::finite(coeff, scale as u32).normalize())
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
            Value::Money(v) => Value::Money(*v),
            Value::Date(v) => Value::Date(*v),
            Value::Time(v) => Value::Time(*v),
            Value::TimeTz(v) => Value::TimeTz(*v),
            Value::Timestamp(v) => Value::Timestamp(*v),
            Value::TimestampTz(v) => Value::TimestampTz(*v),
            Value::Bit(v) => Value::Bit(v.clone()),
            Value::Bytea(v) => Value::Bytea(v.clone()),
            Value::Point(v) => Value::Point(v.clone()),
            Value::Lseg(v) => Value::Lseg(v.clone()),
            Value::Path(v) => Value::Path(v.clone()),
            Value::Line(v) => Value::Line(v.clone()),
            Value::Box(v) => Value::Box(v.clone()),
            Value::Polygon(v) => Value::Polygon(v.clone()),
            Value::Circle(v) => Value::Circle(v.clone()),
            Value::Float64(v) => Value::Float64(*v),
            Value::Numeric(v) => Value::Numeric(v.clone()),
            Value::Json(s) => Value::Json(s.clone()),
            Value::Jsonb(bytes) => Value::Jsonb(bytes.clone()),
            Value::JsonPath(s) => Value::JsonPath(s.clone()),
            Value::TsVector(v) => Value::TsVector(v.clone()),
            Value::TsQuery(q) => Value::TsQuery(q.clone()),
            Value::TextRef(ptr, len) => {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                Value::Text(CompactString::new(s))
            }
            Value::Text(s) => Value::Text(s.clone()),
            Value::InternalChar(v) => Value::InternalChar(*v),
            Value::Bool(v) => Value::Bool(*v),
            Value::Array(values) => {
                Value::Array(values.iter().map(Value::to_owned_value).collect())
            }
            Value::PgArray(array) => Value::PgArray(array.to_owned_value()),
            Value::Record(record) => Value::Record(RecordValue {
                fields: record
                    .fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_owned_value()))
                    .collect(),
            }),
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
            } else if let Value::PgArray(array) = v {
                for item in array.elements.iter_mut() {
                    *item = item.to_owned_value();
                }
            } else if let Value::Record(record) = v {
                for (_, value) in record.fields.iter_mut() {
                    *value = value.to_owned_value();
                }
            }
        }
    }

    pub fn as_array_value(&self) -> Option<ArrayValue> {
        array_value_from_value(self)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(left), Some(right)) = (self.as_array_value(), other.as_array_value()) {
            return left == right;
        }
        match (self, other) {
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Money(a), Value::Money(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::TimeTz(a), Value::TimeTz(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a == b,
            (Value::Bit(a), Value::Bit(b)) => a == b,
            (Value::Bytea(a), Value::Bytea(b)) => a == b,
            (Value::Point(a), Value::Point(b)) => {
                a.x.to_bits() == b.x.to_bits() && a.y.to_bits() == b.y.to_bits()
            }
            (Value::Lseg(a), Value::Lseg(b)) => {
                a.p[0].x.to_bits() == b.p[0].x.to_bits()
                    && a.p[0].y.to_bits() == b.p[0].y.to_bits()
                    && a.p[1].x.to_bits() == b.p[1].x.to_bits()
                    && a.p[1].y.to_bits() == b.p[1].y.to_bits()
            }
            (Value::Path(a), Value::Path(b)) => {
                a.closed == b.closed
                    && a.points.len() == b.points.len()
                    && a.points.iter().zip(&b.points).all(|(left, right)| {
                        left.x.to_bits() == right.x.to_bits()
                            && left.y.to_bits() == right.y.to_bits()
                    })
            }
            (Value::Line(a), Value::Line(b)) => {
                a.a.to_bits() == b.a.to_bits()
                    && a.b.to_bits() == b.b.to_bits()
                    && a.c.to_bits() == b.c.to_bits()
            }
            (Value::Box(a), Value::Box(b)) => {
                a.high.x.to_bits() == b.high.x.to_bits()
                    && a.high.y.to_bits() == b.high.y.to_bits()
                    && a.low.x.to_bits() == b.low.x.to_bits()
                    && a.low.y.to_bits() == b.low.y.to_bits()
            }
            (Value::Polygon(a), Value::Polygon(b)) => {
                a.bound_box.high.x.to_bits() == b.bound_box.high.x.to_bits()
                    && a.bound_box.high.y.to_bits() == b.bound_box.high.y.to_bits()
                    && a.bound_box.low.x.to_bits() == b.bound_box.low.x.to_bits()
                    && a.bound_box.low.y.to_bits() == b.bound_box.low.y.to_bits()
                    && a.points.len() == b.points.len()
                    && a.points.iter().zip(&b.points).all(|(left, right)| {
                        left.x.to_bits() == right.x.to_bits()
                            && left.y.to_bits() == right.y.to_bits()
                    })
            }
            (Value::Circle(a), Value::Circle(b)) => {
                a.center.x.to_bits() == b.center.x.to_bits()
                    && a.center.y.to_bits() == b.center.y.to_bits()
                    && a.radius.to_bits() == b.radius.to_bits()
            }
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Jsonb(a), Value::Jsonb(b)) => a == b,
            (Value::JsonPath(a), Value::JsonPath(b)) => a == b,
            (Value::TsVector(a), Value::TsVector(b)) => a == b,
            (Value::TsQuery(a), Value::TsQuery(b)) => a == b,
            (Value::InternalChar(a), Value::InternalChar(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Record(a), Value::Record(b)) => a == b,
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
        if let Some(array) = self.as_array_value() {
            7u8.hash(state);
            array.hash(state);
            return;
        }
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
            Value::Money(v) => {
                22u8.hash(state);
                v.hash(state);
            }
            Value::Date(v) => {
                15u8.hash(state);
                v.hash(state);
            }
            Value::Time(v) => {
                16u8.hash(state);
                v.hash(state);
            }
            Value::TimeTz(v) => {
                17u8.hash(state);
                v.hash(state);
            }
            Value::Timestamp(v) => {
                18u8.hash(state);
                v.hash(state);
            }
            Value::TimestampTz(v) => {
                19u8.hash(state);
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
            Value::Point(v) => {
                15u8.hash(state);
                v.x.to_bits().hash(state);
                v.y.to_bits().hash(state);
            }
            Value::Lseg(v) => {
                16u8.hash(state);
                v.p[0].x.to_bits().hash(state);
                v.p[0].y.to_bits().hash(state);
                v.p[1].x.to_bits().hash(state);
                v.p[1].y.to_bits().hash(state);
            }
            Value::Path(v) => {
                17u8.hash(state);
                v.closed.hash(state);
                for point in &v.points {
                    point.x.to_bits().hash(state);
                    point.y.to_bits().hash(state);
                }
            }
            Value::Line(v) => {
                18u8.hash(state);
                v.a.to_bits().hash(state);
                v.b.to_bits().hash(state);
                v.c.to_bits().hash(state);
            }
            Value::Box(v) => {
                19u8.hash(state);
                v.high.x.to_bits().hash(state);
                v.high.y.to_bits().hash(state);
                v.low.x.to_bits().hash(state);
                v.low.y.to_bits().hash(state);
            }
            Value::Polygon(v) => {
                20u8.hash(state);
                v.bound_box.high.x.to_bits().hash(state);
                v.bound_box.high.y.to_bits().hash(state);
                v.bound_box.low.x.to_bits().hash(state);
                v.bound_box.low.y.to_bits().hash(state);
                for point in &v.points {
                    point.x.to_bits().hash(state);
                    point.y.to_bits().hash(state);
                }
            }
            Value::Circle(v) => {
                21u8.hash(state);
                v.center.x.to_bits().hash(state);
                v.center.y.to_bits().hash(state);
                v.radius.to_bits().hash(state);
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
            Value::TsVector(v) => {
                15u8.hash(state);
                v.hash(state);
            }
            Value::TsQuery(q) => {
                16u8.hash(state);
                q.hash(state);
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
            Value::Record(v) => {
                23u8.hash(state);
                v.hash(state);
            }
            Value::Array(_) | Value::PgArray(_) => unreachable!("array values hashed above"),
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
