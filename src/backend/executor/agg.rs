use super::render_bit_text;
use super::{compare_order_values, parse_numeric_text, render_datetime_value_text};
use crate::backend::executor::ExecError;
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, NumericValue, Value};
use crate::include::nodes::primnodes::AggFunc;
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;

use num_traits::{Signed, Zero};
use std::cmp::Ordering;
use std::collections::HashSet;

use super::expr_range::{range_intersection_agg_transition, render_range_text};
use super::jsonb::{JsonbValue, encode_jsonb, jsonb_from_value, render_jsonb_bytes};

pub(crate) type AggTransitionFn = fn(&mut AccumState, &[Value]) -> Result<(), ExecError>;

#[derive(Debug, Clone)]
pub(crate) enum NumericAccum {
    Int(i64),
    Float(f64),
    Numeric(NumericValue),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NumericStatsMode {
    PopulationVariance,
    SampleVariance,
    PopulationStddev,
    SampleStddev,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RegrStatsMode {
    RegrCount,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrAvgx,
    RegrAvgy,
    RegrR2,
    RegrSlope,
    RegrIntercept,
    CovarPop,
    CovarSamp,
    Corr,
}

#[derive(Debug, Clone)]
pub(crate) enum AccumState {
    Count {
        count: i64,
    },
    AnyValue {
        value: Option<Value>,
    },
    CountDistinct {
        seen: HashSet<Value>,
    },
    Sum {
        sum: Option<NumericAccum>,
        result_type: SqlType,
    },
    Avg {
        sum: Option<NumericAccum>,
        count: i64,
        result_type: SqlType,
    },
    NumericStats {
        count: i64,
        sum: NumericValue,
        sum_sq: NumericValue,
        result_type: SqlType,
        mode: NumericStatsMode,
    },
    RegrStats {
        count: i64,
        sum_x: f64,
        sum_y: f64,
        sum_x_sq: f64,
        sum_y_sq: f64,
        sum_xy: f64,
        mode: RegrStatsMode,
    },
    JsonAgg {
        values: Vec<Value>,
        jsonb: bool,
    },
    ArrayAgg {
        values: Vec<Value>,
    },
    JsonObjectAgg {
        pairs: Vec<(Value, Value)>,
        jsonb: bool,
    },
    StringAgg {
        bytes: Vec<u8>,
        first_delim_len: Option<usize>,
        bytea: bool,
    },
    Min {
        min: Option<Value>,
    },
    Max {
        max: Option<Value>,
    },
    RangeIntersect {
        current: Option<Value>,
    },
}

impl AccumState {
    pub(crate) fn new(func: AggFunc, distinct: bool, sql_type: SqlType) -> Self {
        match (func, distinct) {
            (AggFunc::Count, true) => AccumState::CountDistinct {
                seen: HashSet::new(),
            },
            (AggFunc::Count, false) => AccumState::Count { count: 0 },
            (AggFunc::AnyValue, _) => AccumState::AnyValue { value: None },
            (AggFunc::Sum, _) => AccumState::Sum {
                sum: None,
                result_type: sql_type,
            },
            (AggFunc::Avg, _) => AccumState::Avg {
                sum: None,
                count: 0,
                result_type: sql_type,
            },
            (
                AggFunc::VarPop | AggFunc::VarSamp | AggFunc::Variance | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::Stddev,
                _,
            ) => AccumState::NumericStats {
                count: 0,
                sum: NumericValue::zero(),
                sum_sq: NumericValue::zero(),
                result_type: sql_type,
                mode: numeric_stats_mode(func),
            },
            (
                AggFunc::RegrCount
                | AggFunc::RegrSxx
                | AggFunc::RegrSyy
                | AggFunc::RegrSxy
                | AggFunc::RegrAvgx
                | AggFunc::RegrAvgy
                | AggFunc::RegrR2
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::Corr,
                _,
            ) => AccumState::RegrStats {
                count: 0,
                sum_x: 0.0,
                sum_y: 0.0,
                sum_x_sq: 0.0,
                sum_y_sq: 0.0,
                sum_xy: 0.0,
                mode: regr_stats_mode(func),
            },
            (AggFunc::JsonAgg, _) => AccumState::JsonAgg {
                values: Vec::new(),
                jsonb: false,
            },
            (AggFunc::ArrayAgg, _) => AccumState::ArrayAgg { values: Vec::new() },
            (AggFunc::JsonbAgg, _) => AccumState::JsonAgg {
                values: Vec::new(),
                jsonb: true,
            },
            (AggFunc::JsonObjectAgg, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: false,
            },
            (AggFunc::JsonbObjectAgg, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: true,
            },
            (AggFunc::StringAgg, _) => AccumState::StringAgg {
                bytes: Vec::new(),
                first_delim_len: None,
                bytea: matches!(sql_type.kind, SqlTypeKind::Bytea),
            },
            (AggFunc::Min, _) => AccumState::Min { min: None },
            (AggFunc::Max, _) => AccumState::Max { max: None },
            (AggFunc::RangeIntersectAgg, _) => AccumState::RangeIntersect { current: None },
        }
    }

    pub(crate) fn transition_fn(
        func: AggFunc,
        arg_count: usize,
        distinct: bool,
    ) -> AggTransitionFn {
        match (func, arg_count, distinct) {
            (AggFunc::Count, _, true) => |state, values| {
                if let AccumState::CountDistinct { seen } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        seen.insert(value.to_owned_value());
                    }
                }
                Ok(())
            },
            (AggFunc::Count, 0, false) => |state, _values| {
                if let AccumState::Count { count } = state {
                    *count += 1;
                }
                Ok(())
            },
            (AggFunc::Count, _, false) => |state, values| {
                if let AccumState::Count { count } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *count += 1;
                    }
                }
                Ok(())
            },
            (AggFunc::AnyValue, _, _) => |state, values| {
                if let AccumState::AnyValue { value: current } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if current.is_none() && !matches!(value, Value::Null) {
                        *current = Some(value.to_owned_value());
                    }
                }
                Ok(())
            },
            (AggFunc::Sum, _, _) => |state, values| {
                if let AccumState::Sum { sum, result_type } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    *sum = accumulate_value(sum.take(), *result_type, value);
                }
                Ok(())
            },
            (AggFunc::Avg, _, _) => |state, values| {
                if let AccumState::Avg {
                    sum,
                    count,
                    result_type,
                } = state
                {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *sum = accumulate_value(sum.take(), *result_type, value);
                        *count += 1;
                    }
                }
                Ok(())
            },
            (
                AggFunc::VarPop
                | AggFunc::VarSamp
                | AggFunc::Variance
                | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::Stddev,
                _,
                _,
            ) => |state, values| {
                if let AccumState::NumericStats {
                    count, sum, sum_sq, ..
                } = state
                {
                    let value = values.first().unwrap_or(&Value::Null);
                    if let Some(numeric) = aggregate_numeric_value(value) {
                        *sum = sum.add(&numeric);
                        *sum_sq = sum_sq.add(&numeric.mul(&numeric));
                        *count += 1;
                    }
                }
                Ok(())
            },
            (
                AggFunc::RegrCount
                | AggFunc::RegrSxx
                | AggFunc::RegrSyy
                | AggFunc::RegrSxy
                | AggFunc::RegrAvgx
                | AggFunc::RegrAvgy
                | AggFunc::RegrR2
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::Corr,
                _,
                _,
            ) => |state, values| {
                if let AccumState::RegrStats {
                    count,
                    sum_x,
                    sum_y,
                    sum_x_sq,
                    sum_y_sq,
                    sum_xy,
                    ..
                } = state
                {
                    let y = values.first().unwrap_or(&Value::Null);
                    let x = values.get(1).unwrap_or(&Value::Null);
                    if let (Some(y), Some(x)) = (aggregate_float8_value(y), aggregate_float8_value(x))
                    {
                        *count += 1;
                        *sum_x += x;
                        *sum_y += y;
                        *sum_x_sq += x * x;
                        *sum_y_sq += y * y;
                        *sum_xy += x * y;
                    }
                }
                Ok(())
            },
            (AggFunc::JsonAgg | AggFunc::JsonbAgg, _, _) => |state, arg_values| {
                if let AccumState::JsonAgg { values, .. } = state {
                    let value = arg_values.first().unwrap_or(&Value::Null);
                    values.push(value.to_owned_value());
                }
                Ok(())
            },
            (AggFunc::ArrayAgg, _, _) => |state, arg_values| {
                if let AccumState::ArrayAgg { values } = state {
                    let value = arg_values.first().unwrap_or(&Value::Null);
                    values.push(value.to_owned_value());
                }
                Ok(())
            },
            (AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg, _, _) => |state, values| {
                if let AccumState::JsonObjectAgg { pairs, .. } = state {
                    let key = values.first().unwrap_or(&Value::Null);
                    if matches!(key, Value::Null) {
                        return Err(ExecError::DetailedError {
                            message: "field name must not be null".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "22004",
                        });
                    }
                    let value = values.get(1).unwrap_or(&Value::Null);
                    pairs.push((key.to_owned_value(), value.to_owned_value()));
                }
                Ok(())
            },
            (AggFunc::StringAgg, _, _) => |state, values| {
                if let AccumState::StringAgg {
                    bytes,
                    first_delim_len,
                    bytea,
                } = state
                {
                    let value = values.first().unwrap_or(&Value::Null);
                    if matches!(value, Value::Null) {
                        return Ok(());
                    }
                    let delimiter = values.get(1).unwrap_or(&Value::Null);
                    let delimiter_bytes = string_agg_input_bytes(delimiter, *bytea);
                    if first_delim_len.is_none() {
                        *first_delim_len = Some(delimiter_bytes.len());
                    }
                    bytes.extend_from_slice(&delimiter_bytes);
                    bytes.extend_from_slice(&string_agg_input_bytes(value, *bytea));
                }
                Ok(())
            },
            (AggFunc::Min, _, _) => |state, values| {
                if let AccumState::Min { min } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *min = Some(match min.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false)
                                    == Ordering::Less
                                {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
                Ok(())
            },
            (AggFunc::Max, _, _) => |state, values| {
                if let AccumState::Max { max } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *max = Some(match max.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false)
                                    == Ordering::Greater
                                {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
                Ok(())
            },
            (AggFunc::RangeIntersectAgg, _, _) => |state, values| {
                if let AccumState::RangeIntersect { current } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    *current = range_intersection_agg_transition(current.take(), value)
                        .expect("range_intersect_agg inputs should be typechecked");
                }
                Ok(())
            },
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AccumState::Count { count } => Value::Int64(*count),
            AccumState::AnyValue { value } => value.clone().unwrap_or(Value::Null),
            AccumState::CountDistinct { seen } => Value::Int64(seen.len() as i64),
            AccumState::Sum { sum, result_type } => match sum {
                Some(NumericAccum::Int(v)) if matches!(result_type.kind, SqlTypeKind::Money) => {
                    Value::Money(*v)
                }
                Some(NumericAccum::Int(v)) => Value::Int64(*v),
                Some(NumericAccum::Float(v)) => Value::Float64(*v),
                Some(NumericAccum::Numeric(v)) => {
                    Value::Numeric(format_numeric_result(v.clone(), *result_type))
                }
                None => Value::Null,
            },
            AccumState::Avg {
                sum,
                count,
                result_type,
            } => {
                if *count == 0 {
                    Value::Null
                } else {
                    match sum {
                        Some(NumericAccum::Int(v)) => {
                            if matches!(result_type.kind, SqlTypeKind::Numeric) {
                                let sum = NumericValue::from_i64(*v);
                                let count_numeric = NumericValue::from_i64(*count);
                                let avg = sum
                                    .div(
                                        &count_numeric,
                                        numeric_div_display_scale(&sum, &count_numeric),
                                    )
                                    .unwrap_or_else(|| NumericValue::from_i64(*v / *count));
                                Value::Numeric(format_numeric_result(avg, *result_type))
                            } else {
                                Value::Int64(*v / *count)
                            }
                        }
                        Some(NumericAccum::Float(v)) => Value::Float64(*v / *count as f64),
                        Some(NumericAccum::Numeric(v)) => {
                            let count_numeric = NumericValue::from_i64(*count);
                            let avg = v
                                .div(&count_numeric, numeric_div_display_scale(v, &count_numeric))
                                .unwrap_or_else(|| v.clone());
                            Value::Numeric(format_numeric_result(avg, *result_type))
                        }
                        None => Value::Null,
                    }
                }
            }
            AccumState::NumericStats {
                count,
                sum,
                sum_sq,
                result_type,
                mode,
            } => {
                if *count == 0 || (numeric_stats_is_sample(*mode) && *count < 2) {
                    Value::Null
                } else {
                    let n = NumericValue::from_i64(*count);
                    let mean_square = sum.mul(sum).div(&n, 32).unwrap_or_else(NumericValue::zero);
                    let divisor = if numeric_stats_is_sample(*mode) {
                        NumericValue::from_i64(*count - 1)
                    } else {
                        n.clone()
                    };
                    let variance = sum_sq
                        .sub(&mean_square)
                        .div(&divisor, 32)
                        .unwrap_or_else(NumericValue::zero);
                    let result = if numeric_stats_is_stddev(*mode) {
                        numeric_sqrt(&variance, 20)
                    } else {
                        variance.round_to_scale(20).unwrap_or(variance)
                    };
                    match result_type.kind {
                        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                            Value::Float64(result.render().parse().unwrap_or(0.0))
                        }
                        _ => Value::Numeric(format_numeric_result(result, *result_type)),
                    }
                }
            }
            AccumState::RegrStats {
                count,
                sum_x,
                sum_y,
                sum_x_sq,
                sum_y_sq,
                sum_xy,
                mode,
            } => finalize_regr_stats(*count, *sum_x, *sum_y, *sum_x_sq, *sum_y_sq, *sum_xy, *mode),
            AccumState::JsonAgg { values, jsonb } => {
                if *jsonb {
                    let mut items = Vec::with_capacity(values.len());
                    for value in values {
                        items.push(jsonb_from_value(value).unwrap_or(JsonbValue::Null));
                    }
                    Value::Jsonb(encode_jsonb(&JsonbValue::Array(items)))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_array(values),
                    ))
                }
            }
            AccumState::ArrayAgg { values } => finalize_array_agg(values),
            AccumState::JsonObjectAgg { pairs, jsonb } => {
                if *jsonb {
                    let built = JsonbValue::Object(
                        pairs
                            .iter()
                            .map(|(k, v)| {
                                (
                                    json_object_agg_key(k),
                                    jsonb_from_value(v).unwrap_or(JsonbValue::Null),
                                )
                            })
                            .collect(),
                    );
                    Value::Jsonb(encode_jsonb(&built))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_object(pairs),
                    ))
                }
            }
            AccumState::StringAgg {
                bytes,
                first_delim_len,
                bytea,
            } => {
                let Some(prefix_len) = first_delim_len else {
                    return Value::Null;
                };
                let rendered = bytes[*prefix_len..].to_vec();
                if *bytea {
                    Value::Bytea(rendered)
                } else {
                    Value::Text(CompactString::from_owned(
                        String::from_utf8(rendered).expect("text string_agg state must be utf-8"),
                    ))
                }
            }
            AccumState::Min { min } => min.clone().unwrap_or(Value::Null),
            AccumState::Max { max } => max.clone().unwrap_or(Value::Null),
            AccumState::RangeIntersect { current } => current.clone().unwrap_or(Value::Null),
        }
    }
}

fn string_agg_input_bytes(value: &Value, bytea: bool) -> Vec<u8> {
    match value {
        Value::Null => Vec::new(),
        Value::Bytea(bytes) if bytea => bytes.clone(),
        _ if !bytea => value
            .as_text()
            .expect("text string_agg input must be text")
            .as_bytes()
            .to_vec(),
        _ => panic!("bytea string_agg input must be bytea"),
    }
}

fn finalize_array_agg(values: &[Value]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let first_non_null = values.iter().find(|value| !matches!(value, Value::Null));
    let Some(first_non_null) = first_non_null else {
        return Value::PgArray(ArrayValue::from_1d(values.to_vec()));
    };
    if let Some(first_array) = normalize_array_value(first_non_null) {
        let mut elements = Vec::new();
        let mut inner_dims: Option<Vec<ArrayDimension>> = None;
        for value in values {
            let Some(array) = normalize_array_value(value) else {
                return Value::Null;
            };
            if array.dimensions.is_empty() {
                return Value::Null;
            }
            match &inner_dims {
                None => inner_dims = Some(array.dimensions.clone()),
                Some(existing) if *existing != array.dimensions => return Value::Null,
                Some(_) => {}
            }
            elements.extend(array.elements.clone());
        }
        let mut dimensions = vec![ArrayDimension {
            lower_bound: 1,
            length: values.len(),
        }];
        dimensions.extend(first_array.dimensions);
        return Value::PgArray(ArrayValue::from_dimensions(dimensions, elements));
    }
    Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: 1,
            length: values.len(),
        }],
        values.to_vec(),
    ))
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

fn numeric_stats_mode(func: AggFunc) -> NumericStatsMode {
    match func {
        AggFunc::VarPop => NumericStatsMode::PopulationVariance,
        AggFunc::VarSamp | AggFunc::Variance => NumericStatsMode::SampleVariance,
        AggFunc::StddevPop => NumericStatsMode::PopulationStddev,
        AggFunc::StddevSamp | AggFunc::Stddev => NumericStatsMode::SampleStddev,
        _ => unreachable!("non-numeric aggregate in numeric_stats_mode"),
    }
}

fn numeric_stats_is_sample(mode: NumericStatsMode) -> bool {
    matches!(
        mode,
        NumericStatsMode::SampleVariance | NumericStatsMode::SampleStddev
    )
}

fn numeric_stats_is_stddev(mode: NumericStatsMode) -> bool {
    matches!(
        mode,
        NumericStatsMode::PopulationStddev | NumericStatsMode::SampleStddev
    )
}

fn regr_stats_mode(func: AggFunc) -> RegrStatsMode {
    match func {
        AggFunc::RegrCount => RegrStatsMode::RegrCount,
        AggFunc::RegrSxx => RegrStatsMode::RegrSxx,
        AggFunc::RegrSyy => RegrStatsMode::RegrSyy,
        AggFunc::RegrSxy => RegrStatsMode::RegrSxy,
        AggFunc::RegrAvgx => RegrStatsMode::RegrAvgx,
        AggFunc::RegrAvgy => RegrStatsMode::RegrAvgy,
        AggFunc::RegrR2 => RegrStatsMode::RegrR2,
        AggFunc::RegrSlope => RegrStatsMode::RegrSlope,
        AggFunc::RegrIntercept => RegrStatsMode::RegrIntercept,
        AggFunc::CovarPop => RegrStatsMode::CovarPop,
        AggFunc::CovarSamp => RegrStatsMode::CovarSamp,
        AggFunc::Corr => RegrStatsMode::Corr,
        _ => unreachable!("non-bivariate aggregate in regr_stats_mode"),
    }
}

fn finalize_regr_stats(
    count: i64,
    sum_x: f64,
    sum_y: f64,
    sum_x_sq: f64,
    sum_y_sq: f64,
    sum_xy: f64,
    mode: RegrStatsMode,
) -> Value {
    let n = count as f64;
    if n < 1.0 {
        return Value::Null;
    }
    let sxx = sum_x_sq - (sum_x * sum_x) / n;
    let syy = sum_y_sq - (sum_y * sum_y) / n;
    let sxy = sum_xy - (sum_x * sum_y) / n;
    let value = match mode {
        RegrStatsMode::RegrCount => return Value::Int64(count),
        RegrStatsMode::RegrSxx => sxx,
        RegrStatsMode::RegrSyy => syy,
        RegrStatsMode::RegrSxy => sxy,
        RegrStatsMode::RegrAvgx => sum_x / n,
        RegrStatsMode::RegrAvgy => sum_y / n,
        RegrStatsMode::CovarPop => sxy / n,
        RegrStatsMode::CovarSamp => {
            if n < 2.0 {
                return Value::Null;
            }
            sxy / (n - 1.0)
        }
        RegrStatsMode::Corr => {
            if sxx == 0.0 || syy == 0.0 {
                return Value::Null;
            }
            sxy / (sxx * syy).sqrt()
        }
        RegrStatsMode::RegrR2 => {
            if sxx == 0.0 {
                return Value::Null;
            }
            if syy == 0.0 {
                1.0
            } else {
                (sxy * sxy) / (sxx * syy)
            }
        }
        RegrStatsMode::RegrSlope => {
            if sxx == 0.0 {
                return Value::Null;
            }
            sxy / sxx
        }
        RegrStatsMode::RegrIntercept => {
            if sxx == 0.0 {
                return Value::Null;
            }
            (sum_y - sum_x * sxy / sxx) / n
        }
    };
    Value::Float64(value)
}

fn render_json_array(values: &[Value]) -> String {
    let mut out = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value_to_json_text(value));
    }
    out.push(']');
    out
}

fn render_json_object(pairs: &[(Value, Value)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let key_text = json_object_agg_key(key);
        out.push_str(&serde_json::to_string(&key_text).unwrap());
        out.push(':');
        out.push_str(&value_to_json_text(value));
    }
    out.push('}');
    out
}

fn json_object_agg_key(key: &Value) -> String {
    match key {
        Value::Null => "null".to_string(),
        Value::Text(_) | Value::TextRef(_, _) => key.as_text().unwrap().to_string(),
        Value::Bit(v) => render_bit_text(v),
        Value::Bytea(v) => format_bytea_text(v, ByteaOutputFormat::Hex),
        Value::InternalChar(v) => crate::backend::executor::render_internal_char_text(*v),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => render_jsonb_bytes(v).unwrap_or_else(|_| "null".into()),
        Value::Numeric(v) => v.render(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::JsonPath(v) => v.to_string(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => render_datetime_value_text(key).expect("datetime values render"),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            crate::backend::executor::render_geometry_text(key, Default::default())
                .unwrap_or_default()
        }
        Value::Range(_) => render_range_text(key).unwrap_or_default(),
        Value::TsVector(v) => crate::backend::executor::render_tsvector_text(v),
        Value::TsQuery(v) => crate::backend::executor::render_tsquery_text(v),
        Value::Array(_) | Value::PgArray(_) | Value::Record(_) => value_to_json_text(key),
    }
}

fn value_to_json_text(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::Bit(v) => serde_json::to_string(&render_bit_text(v)).unwrap(),
        Value::JsonPath(v) => serde_json::to_string(v.as_str()).unwrap(),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => render_jsonb_bytes(v).unwrap_or_else(|_| "null".into()),
        Value::Text(_) | Value::TextRef(_, _) => {
            serde_json::to_string(value.as_text().unwrap()).unwrap()
        }
        Value::Bytea(v) => {
            serde_json::to_string(&format_bytea_text(v, ByteaOutputFormat::Hex)).unwrap()
        }
        Value::InternalChar(v) => {
            serde_json::to_string(&crate::backend::executor::render_internal_char_text(*v)).unwrap()
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => serde_json::to_string(
            &render_datetime_value_text(value).expect("datetime values render"),
        )
        .unwrap(),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => serde_json::to_string(
            &crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default(),
        )
        .unwrap(),
        Value::Range(_) => {
            serde_json::to_string(&render_range_text(value).unwrap_or_default()).unwrap()
        }
        Value::TsVector(v) => {
            serde_json::to_string(&crate::backend::executor::render_tsvector_text(v)).unwrap()
        }
        Value::TsQuery(v) => {
            serde_json::to_string(&crate::backend::executor::render_tsquery_text(v)).unwrap()
        }
        Value::Array(items) => render_json_array(items),
        Value::Record(record) => render_json_object(
            &record
                .iter()
                .map(|(field, value)| (Value::Text(field.name.clone().into()), value.clone()))
                .collect::<Vec<_>>(),
        ),
        Value::PgArray(array) => render_json_array(&array.to_nested_values()),
    }
}

fn accumulate_value(
    sum: Option<NumericAccum>,
    result_type: SqlType,
    value: &Value,
) -> Option<NumericAccum> {
    match value {
        Value::Null => sum,
        Value::Int16(v) => Some(accumulate_integral(sum, result_type, *v as i64)),
        Value::Int32(v) => Some(accumulate_integral(sum, result_type, *v as i64)),
        Value::Int64(v) => Some(accumulate_integral(sum, result_type, *v)),
        Value::Money(v) => Some(accumulate_integral(sum, result_type, *v)),
        Value::Float64(v) => Some(match sum {
            Some(NumericAccum::Numeric(cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                NumericAccum::Numeric(cur.add(&rhs))
            }
            Some(NumericAccum::Int(cur)) => NumericAccum::Float(cur as f64 + *v),
            Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + *v),
            None => {
                if matches!(result_type.kind, SqlTypeKind::Numeric) {
                    NumericAccum::Numeric(
                        parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero),
                    )
                } else {
                    NumericAccum::Float(*v)
                }
            }
        }),
        Value::Numeric(v) => {
            let parsed = v.clone();
            Some(match sum {
                Some(NumericAccum::Numeric(cur)) => NumericAccum::Numeric(cur.add(&parsed)),
                Some(NumericAccum::Int(cur)) => {
                    NumericAccum::Numeric(NumericValue::from_i64(cur).add(&parsed))
                }
                Some(NumericAccum::Float(cur)) => {
                    let left =
                        parse_numeric_text(&cur.to_string()).unwrap_or_else(NumericValue::zero);
                    NumericAccum::Numeric(left.add(&parsed))
                }
                None => NumericAccum::Numeric(parsed),
            })
        }
        _ => sum,
    }
}

fn aggregate_numeric_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(NumericValue::from_i64(i64::from(*v))),
        Value::Int32(v) => Some(NumericValue::from_i64(i64::from(*v))),
        Value::Int64(v) => Some(NumericValue::from_i64(*v)),
        Value::Float64(v) => parse_numeric_text(&v.to_string()),
        Value::Numeric(v) => Some(v.clone()),
        _ => None,
    }
}

fn aggregate_float8_value(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        Value::Numeric(v) => v.render().parse().ok(),
        _ => None,
    }
}

fn numeric_sqrt(value: &NumericValue, scale: u32) -> NumericValue {
    match value {
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => NumericValue::zero(),
        NumericValue::Finite { .. } => {
            let seed = value
                .render()
                .parse::<f64>()
                .ok()
                .map(|v| v.sqrt())
                .and_then(|v| parse_numeric_text(&format!("{v:.24}")))
                .unwrap_or_else(|| NumericValue::from_i64(1));
            let two = NumericValue::from_i64(2);
            let mut current = seed;
            for _ in 0..24 {
                let next = current
                    .add(
                        &value
                            .div(&current, scale + 12)
                            .unwrap_or_else(NumericValue::zero),
                    )
                    .div(&two, scale + 12)
                    .unwrap_or_else(|| current.clone());
                if next.cmp(&current) == Ordering::Equal {
                    current = next;
                    break;
                }
                current = next;
            }
            current.round_to_scale(scale).unwrap_or(current)
        }
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf | NumericValue::NaN => NumericValue::NaN,
    }
}

fn accumulate_integral(
    sum: Option<NumericAccum>,
    result_type: SqlType,
    value: i64,
) -> NumericAccum {
    match sum {
        Some(NumericAccum::Numeric(cur)) => {
            NumericAccum::Numeric(cur.add(&NumericValue::from_i64(value)))
        }
        Some(NumericAccum::Int(cur)) => NumericAccum::Int(cur + value),
        Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + value as f64),
        None => {
            if matches!(result_type.kind, SqlTypeKind::Numeric) {
                NumericAccum::Numeric(NumericValue::from_i64(value))
            } else {
                NumericAccum::Int(value)
            }
        }
    }
}

fn format_numeric_result(value: NumericValue, sql_type: SqlType) -> NumericValue {
    if let Some((_, scale)) = sql_type.numeric_precision_scale() {
        value.round_to_scale(scale as u32).unwrap_or(value)
    } else {
        value
    }
}

fn floor_div_i32(value: i32, divisor: i32) -> i32 {
    if value >= 0 {
        value / divisor
    } else {
        -(((-value) + divisor - 1) / divisor)
    }
}

fn numeric_div_display_scale(lhs: &NumericValue, rhs: &NumericValue) -> u32 {
    const NUMERIC_MIN_SIG_DIGITS: i32 = 16;
    const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
    const NUMERIC_MAX_DISPLAY_SCALE: i32 = 16383;
    const DEC_DIGITS: i32 = 4;

    fn normalized_weight_and_first_group(value: &NumericValue) -> (i32, i32, i32) {
        match value {
            NumericValue::Finite {
                coeff,
                scale,
                dscale,
            } if !coeff.is_zero() => {
                let digits = coeff.abs().to_str_radix(10);
                let dec_weight = digits.len() as i32 - (*scale as i32) - 1;
                let group_weight = floor_div_i32(dec_weight, DEC_DIGITS);
                let lead_len = (dec_weight - group_weight * DEC_DIGITS + 1).clamp(1, DEC_DIGITS);
                let first_group = digits
                    .chars()
                    .take(lead_len as usize)
                    .collect::<String>()
                    .parse::<i32>()
                    .unwrap_or(0);
                (group_weight, first_group, *dscale as i32)
            }
            NumericValue::Finite { dscale, .. } => (0, 0, *dscale as i32),
            _ => (0, 0, 0),
        }
    }

    let (weight1, first1, dscale1) = normalized_weight_and_first_group(lhs);
    let (weight2, first2, dscale2) = normalized_weight_and_first_group(rhs);
    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = rscale.max(dscale1);
    rscale = rscale.max(dscale2);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
    rscale as u32
}

#[derive(Debug, Clone)]
pub(crate) struct AggGroup {
    pub(crate) key_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
    pub(crate) ordered_inputs: Vec<Vec<OrderedAggInput>>,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderedAggInput {
    pub(crate) sort_keys: Vec<Value>,
    pub(crate) arg_values: Vec<Value>,
}
