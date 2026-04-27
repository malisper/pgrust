use super::render_bit_text;
use super::{
    compare_order_values, parse_numeric_text, render_datetime_value_text, render_interval_text,
    render_macaddr_text, render_macaddr8_text,
};
use crate::backend::executor::ExecError;
use crate::backend::executor::exec_expr::{expect_float8_arg, float8_regr_accum_state};
use crate::backend::executor::expr_agg_support::execute_scalar_function_value_call;
use crate::backend::executor::expr_ops::{
    bitwise_and_values, bitwise_or_values, bitwise_xor_values, compare_order_by_keys,
    interval_div_float,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::{
    ArrayDimension, ArrayValue, IntervalValue, NumericValue, Value,
};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, HypotheticalAggFunc, expr_sql_type_hint,
};
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;

use num_traits::{Signed, Zero};
use std::cmp::Ordering;
use std::collections::HashSet;

use super::expr_multirange::{multirange_intersection_agg_transition, range_agg_transition};
use super::expr_range::{range_intersection_agg_transition, render_range_text};
use super::expr_xml::concat_xml_texts;
use super::jsonb::{JsonbValue, encode_jsonb, jsonb_from_value, render_jsonb_bytes};

pub(crate) type AggTransitionFn = fn(&mut AccumState, &[Value]) -> Result<(), ExecError>;

#[derive(Debug, Clone)]
pub(crate) struct CustomAggregateRuntime {
    pub(crate) transfn_oid: u32,
    pub(crate) transfn_strict: bool,
    pub(crate) finalfn_oid: Option<u32>,
    pub(crate) finalfn_strict: bool,
    pub(crate) transtype: SqlType,
    pub(crate) init_value: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) enum AggregateRuntime {
    Builtin {
        func: AggFunc,
        transition: AggTransitionFn,
    },
    Hypothetical {
        func: HypotheticalAggFunc,
    },
    Custom(CustomAggregateRuntime),
}

#[derive(Debug, Clone)]
pub(crate) enum NumericAccum {
    Int(i64),
    Float(f64),
    Numeric(NumericValue),
}

#[derive(Debug, Clone)]
pub(crate) enum AccumState {
    Count {
        count: i64,
    },
    AnyValue {
        value: Option<Value>,
    },
    BoolAnd {
        seen_nonnull: bool,
        value: bool,
    },
    BoolOr {
        seen_nonnull: bool,
        value: bool,
    },
    Bitwise {
        value: Option<Value>,
        transition: fn(Value, Value) -> Result<Value, ExecError>,
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
    IntervalAvg {
        sum: Option<IntervalValue>,
        count: i64,
    },
    FloatStats {
        count: f64,
        sum: f64,
        sum_sq: f64,
        stddev: bool,
        sample: bool,
    },
    NumericStats {
        count: i64,
        sum: NumericValue,
        sum_sq: NumericValue,
        result_type: SqlType,
        stddev: bool,
        sample: bool,
    },
    RegrStats {
        func: AggFunc,
        count: f64,
        sum_x: f64,
        sum_sq_x: f64,
        sum_y: f64,
        sum_sq_y: f64,
        sum_xy: f64,
        first_x: f64,
        all_x_equal: bool,
        first_y: f64,
        all_y_equal: bool,
    },
    JsonAgg {
        values: Vec<Value>,
        jsonb: bool,
    },
    ArrayAgg {
        values: Vec<Value>,
        input_is_array: bool,
        inner_dims: Option<Vec<ArrayDimension>>,
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
    XmlAgg {
        values: Vec<CompactString>,
    },
    Min {
        min: Option<Value>,
    },
    Max {
        max: Option<Value>,
    },
    RangeAgg {
        current: Option<crate::include::nodes::datum::MultirangeValue>,
    },
    RangeIntersect {
        current: Option<Value>,
    },
    Hypothetical,
    Custom {
        value: Value,
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
            (AggFunc::BoolAnd, _) => AccumState::BoolAnd {
                seen_nonnull: false,
                value: true,
            },
            (AggFunc::BoolOr, _) => AccumState::BoolOr {
                seen_nonnull: false,
                value: false,
            },
            (AggFunc::BitAnd, _) => AccumState::Bitwise {
                value: None,
                transition: bitwise_and_values,
            },
            (AggFunc::BitOr, _) => AccumState::Bitwise {
                value: None,
                transition: bitwise_or_values,
            },
            (AggFunc::BitXor, _) => AccumState::Bitwise {
                value: None,
                transition: bitwise_xor_values,
            },
            (AggFunc::Sum, _) => AccumState::Sum {
                sum: None,
                result_type: sql_type,
            },
            (AggFunc::Avg, _) if matches!(sql_type.kind, SqlTypeKind::Interval) => {
                AccumState::IntervalAvg {
                    sum: None,
                    count: 0,
                }
            }
            (AggFunc::Avg, _) => AccumState::Avg {
                sum: None,
                count: 0,
                result_type: sql_type,
            },
            (AggFunc::VarPop, _) => {
                if matches!(sql_type.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
                    AccumState::FloatStats {
                        count: 0.0,
                        sum: 0.0,
                        sum_sq: 0.0,
                        stddev: false,
                        sample: false,
                    }
                } else {
                    AccumState::NumericStats {
                        count: 0,
                        sum: NumericValue::zero(),
                        sum_sq: NumericValue::zero(),
                        result_type: sql_type,
                        stddev: false,
                        sample: false,
                    }
                }
            }
            (AggFunc::VarSamp, _) => {
                if matches!(sql_type.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
                    AccumState::FloatStats {
                        count: 0.0,
                        sum: 0.0,
                        sum_sq: 0.0,
                        stddev: false,
                        sample: true,
                    }
                } else {
                    AccumState::NumericStats {
                        count: 0,
                        sum: NumericValue::zero(),
                        sum_sq: NumericValue::zero(),
                        result_type: sql_type,
                        stddev: false,
                        sample: true,
                    }
                }
            }
            (AggFunc::StddevPop, _) => {
                if matches!(sql_type.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
                    AccumState::FloatStats {
                        count: 0.0,
                        sum: 0.0,
                        sum_sq: 0.0,
                        stddev: true,
                        sample: false,
                    }
                } else {
                    AccumState::NumericStats {
                        count: 0,
                        sum: NumericValue::zero(),
                        sum_sq: NumericValue::zero(),
                        result_type: sql_type,
                        stddev: true,
                        sample: false,
                    }
                }
            }
            (AggFunc::StddevSamp, _) => {
                if matches!(sql_type.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
                    AccumState::FloatStats {
                        count: 0.0,
                        sum: 0.0,
                        sum_sq: 0.0,
                        stddev: true,
                        sample: true,
                    }
                } else {
                    AccumState::NumericStats {
                        count: 0,
                        sum: NumericValue::zero(),
                        sum_sq: NumericValue::zero(),
                        result_type: sql_type,
                        stddev: true,
                        sample: true,
                    }
                }
            }
            (
                AggFunc::RegrCount
                | AggFunc::RegrSxx
                | AggFunc::RegrSyy
                | AggFunc::RegrSxy
                | AggFunc::RegrAvgX
                | AggFunc::RegrAvgY
                | AggFunc::RegrR2
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::Corr,
                _,
            ) => AccumState::RegrStats {
                func,
                count: 0.0,
                sum_x: 0.0,
                sum_sq_x: 0.0,
                sum_y: 0.0,
                sum_sq_y: 0.0,
                sum_xy: 0.0,
                first_x: 0.0,
                all_x_equal: true,
                first_y: 0.0,
                all_y_equal: true,
            },
            (AggFunc::JsonAgg, _) => AccumState::JsonAgg {
                values: Vec::new(),
                jsonb: false,
            },
            (AggFunc::ArrayAgg, _) => AccumState::ArrayAgg {
                values: Vec::new(),
                input_is_array: false,
                inner_dims: None,
            },
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
            (AggFunc::XmlAgg, _) => AccumState::XmlAgg { values: Vec::new() },
            (AggFunc::Min, _) => AccumState::Min { min: None },
            (AggFunc::Max, _) => AccumState::Max { max: None },
            (AggFunc::RangeAgg, _) => AccumState::RangeAgg { current: None },
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
            (AggFunc::BoolAnd, _, _) => |state, values| {
                if let AccumState::BoolAnd {
                    seen_nonnull,
                    value,
                } = state
                {
                    match values.first().unwrap_or(&Value::Null) {
                        Value::Bool(next) => {
                            *seen_nonnull = true;
                            *value &= *next;
                        }
                        Value::Null => {}
                        _ => {}
                    }
                }
                Ok(())
            },
            (AggFunc::BoolOr, _, _) => |state, values| {
                if let AccumState::BoolOr {
                    seen_nonnull,
                    value,
                } = state
                {
                    match values.first().unwrap_or(&Value::Null) {
                        Value::Bool(next) => {
                            *seen_nonnull = true;
                            *value |= *next;
                        }
                        Value::Null => {}
                        _ => {}
                    }
                }
                Ok(())
            },
            (AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor, _, _) => |state, values| {
                if let AccumState::Bitwise { value, transition } = state {
                    let next = values.first().unwrap_or(&Value::Null);
                    if matches!(next, Value::Null) {
                        return Ok(());
                    }
                    *value = Some(match value.take() {
                        Some(current) => transition(current, next.to_owned_value())?,
                        None => next.to_owned_value(),
                    });
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
                let value = values.first().unwrap_or(&Value::Null);
                if !matches!(value, Value::Null) {
                    match state {
                        AccumState::Avg {
                            sum,
                            count,
                            result_type,
                        } => {
                            *sum = accumulate_value(sum.take(), *result_type, value);
                            *count += 1;
                        }
                        AccumState::IntervalAvg { sum, count } => {
                            let Value::Interval(next) = value else {
                                return Ok(());
                            };
                            *sum = Some(match *sum {
                                Some(current) => current
                                    .checked_add(*next)
                                    .ok_or_else(interval_avg_out_of_range)?,
                                None => *next,
                            });
                            *count += 1;
                        }
                        _ => {}
                    }
                }
                Ok(())
            },
            (
                AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp,
                _,
                _,
            ) => |state, values| {
                let value = values.first().unwrap_or(&Value::Null);
                match state {
                    AccumState::FloatStats {
                        count, sum, sum_sq, ..
                    } => {
                        if let Some(value) = aggregate_float_value(value) {
                            let next_count = *count + 1.0;
                            let next_sum = *sum + value;
                            if *count > 0.0 {
                                let tmp = value * next_count - next_sum;
                                *sum_sq += tmp * tmp / (next_count * *count);
                                if next_sum.is_infinite() || sum_sq.is_infinite() {
                                    if !sum.is_infinite() && !value.is_infinite() {
                                        return Err(float8_overflow_error());
                                    }
                                    *sum_sq = f64::NAN;
                                }
                            } else if value.is_nan() || value.is_infinite() {
                                *sum_sq = f64::NAN;
                            }
                            *count = next_count;
                            *sum = next_sum;
                        }
                    }
                    AccumState::NumericStats {
                        count, sum, sum_sq, ..
                    } => {
                        if let Some(numeric) = aggregate_numeric_value(value) {
                            *sum = sum.add(&numeric);
                            *sum_sq = sum_sq.add(&numeric.mul(&numeric));
                            *count += 1;
                        }
                    }
                    _ => {}
                }
                Ok(())
            },
            (
                AggFunc::RegrCount
                | AggFunc::RegrSxx
                | AggFunc::RegrSyy
                | AggFunc::RegrSxy
                | AggFunc::RegrAvgX
                | AggFunc::RegrAvgY
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
                    sum_sq_x,
                    sum_y,
                    sum_sq_y,
                    sum_xy,
                    first_x,
                    all_x_equal,
                    first_y,
                    all_y_equal,
                    ..
                } = state
                {
                    let y = values.first().unwrap_or(&Value::Null);
                    let x = values.get(1).unwrap_or(&Value::Null);
                    if matches!(y, Value::Null) || matches!(x, Value::Null) {
                        return Ok(());
                    }
                    let y = expect_float8_arg("regr aggregate", y)?;
                    let x = expect_float8_arg("regr aggregate", x)?;
                    if *count == 0.0 {
                        *first_x = x;
                        *all_x_equal = !x.is_nan();
                        *first_y = y;
                        *all_y_equal = !y.is_nan();
                    } else {
                        *all_x_equal &= float8_regr_constant_value_eq(x, *first_x);
                        *all_y_equal &= float8_regr_constant_value_eq(y, *first_y);
                    }
                    [*count, *sum_x, *sum_sq_x, *sum_y, *sum_sq_y, *sum_xy] =
                        float8_regr_accum_state(
                            *count, *sum_x, *sum_sq_x, *sum_y, *sum_sq_y, *sum_xy, y, x,
                        )?;
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
                if let AccumState::ArrayAgg {
                    values,
                    input_is_array,
                    inner_dims,
                } = state
                {
                    let value = arg_values.first().unwrap_or(&Value::Null);
                    if *input_is_array {
                        validate_array_agg_array_input(value, inner_dims)?;
                    }
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
            (AggFunc::XmlAgg, _, _) => |state, values| {
                if let AccumState::XmlAgg { values: out } = state {
                    match values.first().unwrap_or(&Value::Null) {
                        Value::Null => {}
                        Value::Xml(text) => out.push(text.clone()),
                        other => {
                            if let Some(text) = other.as_text() {
                                out.push(CompactString::new(text));
                            }
                        }
                    }
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
                                if compare_order_values(value, &current, None, None, false)?
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
                                if compare_order_values(value, &current, None, None, false)?
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
            (AggFunc::RangeAgg, _, _) => |state, values| {
                if let AccumState::RangeAgg { current } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    *current = range_agg_transition(current.take(), value)
                        .expect("range_agg inputs should be typechecked");
                }
                Ok(())
            },
            (AggFunc::RangeIntersectAgg, _, _) => |state, values| {
                if let AccumState::RangeIntersect { current } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    *current = match value {
                        Value::Range(_) => range_intersection_agg_transition(current.take(), value),
                        _ => multirange_intersection_agg_transition(current.take(), value),
                    }
                    .expect("range_intersect_agg inputs should be typechecked");
                }
                Ok(())
            },
        }
    }

    pub(crate) fn custom(value: Value) -> Self {
        Self::Custom { value }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AccumState::Count { count } => Value::Int64(*count),
            AccumState::CountDistinct { seen } => Value::Int64(seen.len() as i64),
            AccumState::AnyValue { value } => value.clone().unwrap_or(Value::Null),
            AccumState::BoolAnd {
                seen_nonnull,
                value,
            } => {
                if *seen_nonnull {
                    Value::Bool(*value)
                } else {
                    Value::Null
                }
            }
            AccumState::BoolOr {
                seen_nonnull,
                value,
            } => {
                if *seen_nonnull {
                    Value::Bool(*value)
                } else {
                    Value::Null
                }
            }
            AccumState::Bitwise { value, .. } => value.clone().unwrap_or(Value::Null),
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
            AccumState::IntervalAvg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    sum.as_ref()
                        .and_then(|value| interval_div_float(*value, *count as f64))
                        .map(Value::Interval)
                        .unwrap_or(Value::Null)
                }
            }
            AccumState::FloatStats {
                count,
                sum: _,
                sum_sq,
                stddev,
                sample,
            } => {
                if *count == 0.0 || (*sample && *count <= 1.0) {
                    Value::Null
                } else {
                    let variance = if *sample {
                        *sum_sq / (*count - 1.0)
                    } else {
                        *sum_sq / *count
                    };
                    let result = if *stddev { variance.sqrt() } else { variance };
                    Value::Float64(result)
                }
            }
            AccumState::NumericStats {
                count,
                sum,
                sum_sq,
                result_type,
                stddev,
                sample,
            } => {
                if *count == 0 || (*sample && *count < 2) {
                    Value::Null
                } else {
                    let n = NumericValue::from_i64(*count);
                    let mul_rscale = sum.dscale().saturating_mul(2);
                    let numerator = n
                        .mul(sum_sq)
                        .with_dscale(mul_rscale)
                        .sub(&sum.mul(sum).with_dscale(mul_rscale));
                    let denominator = if *sample {
                        n.mul(&NumericValue::from_i64(*count - 1))
                    } else {
                        n.mul(&n)
                    };
                    let variance = if numerator.cmp(&NumericValue::zero()) != Ordering::Greater {
                        NumericValue::zero()
                    } else {
                        let mut rscale = numeric_div_display_scale(&numerator, &denominator);
                        if numeric_quotient_decimal_weight(&numerator, &denominator) >= 8 {
                            rscale = rscale.max(20);
                        }
                        numerator
                            .div(&denominator, rscale)
                            .unwrap_or_else(NumericValue::zero)
                    };
                    let result = if *stddev {
                        numeric_sqrt(&variance, numeric_visible_scale(&variance))
                    } else {
                        variance
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
                func,
                count,
                sum_x,
                sum_sq_x,
                sum_y,
                sum_sq_y,
                sum_xy,
                all_x_equal,
                all_y_equal,
                ..
            } => finalize_regr_stats(
                *func,
                *count,
                *sum_x,
                *sum_sq_x,
                *sum_y,
                *sum_sq_y,
                *sum_xy,
                *all_x_equal,
                *all_y_equal,
            ),
            AccumState::JsonAgg { values, jsonb } => {
                if *jsonb {
                    let mut items = Vec::with_capacity(values.len());
                    for value in values {
                        items.push(
                            jsonb_from_value(
                                value,
                                &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(
                                ),
                            )
                            .unwrap_or(JsonbValue::Null),
                        );
                    }
                    Value::Jsonb(encode_jsonb(&JsonbValue::Array(items)))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_agg_array(values),
                    ))
                }
            }
            AccumState::ArrayAgg { values, .. } => finalize_array_agg(values),
            AccumState::JsonObjectAgg { pairs, jsonb } => {
                if *jsonb {
                    let built = JsonbValue::Object(
                        pairs
                            .iter()
                            .map(|(k, v)| {
                                (
                                    json_object_agg_key(k),
                                    jsonb_from_value(
                                        v,
                                        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                                    )
                                    .unwrap_or(JsonbValue::Null),
                                )
                            })
                            .collect(),
                    );
                    Value::Jsonb(encode_jsonb(&built))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_object_agg(pairs),
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
            AccumState::XmlAgg { values } => {
                if values.is_empty() {
                    Value::Null
                } else {
                    Value::Xml(CompactString::from_owned(concat_xml_texts(
                        values.iter().map(|value| value.as_str()),
                    )))
                }
            }
            AccumState::Min { min } => min.clone().unwrap_or(Value::Null),
            AccumState::Max { max } => max.clone().unwrap_or(Value::Null),
            AccumState::RangeAgg { current } => current
                .clone()
                .map(Value::Multirange)
                .unwrap_or(Value::Null),
            AccumState::RangeIntersect { current } => current.clone().unwrap_or(Value::Null),
            AccumState::Hypothetical => Value::Null,
            AccumState::Custom { value } => value.clone(),
        }
    }
}

impl AggregateRuntime {
    pub(crate) fn initialize_state(&self, accum: &AggAccum) -> AccumState {
        match self {
            AggregateRuntime::Builtin { func, .. } => {
                let mut state = AccumState::new(*func, accum.distinct, accum.sql_type);
                if matches!(func, AggFunc::ArrayAgg)
                    && let AccumState::ArrayAgg { input_is_array, .. } = &mut state
                {
                    *input_is_array = accum
                        .args
                        .first()
                        .and_then(expr_sql_type_hint)
                        .is_some_and(|ty| ty.is_array);
                }
                state
            }
            AggregateRuntime::Hypothetical { .. } => AccumState::Hypothetical,
            AggregateRuntime::Custom(custom) => {
                AccumState::custom(custom.init_value.clone().unwrap_or(Value::Null))
            }
        }
    }

    pub(crate) fn transition(
        &self,
        state: &mut AccumState,
        arg_values: &[Value],
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<(), ExecError> {
        match self {
            AggregateRuntime::Builtin { transition, .. } => transition(state, arg_values),
            AggregateRuntime::Hypothetical { .. } => Ok(()),
            AggregateRuntime::Custom(custom) => {
                let mut call_args = Vec::with_capacity(arg_values.len() + 1);
                let current_state = match state {
                    AccumState::Custom { value } => value.clone(),
                    other => {
                        return Err(ExecError::DetailedError {
                            message: "custom aggregate state shape mismatch".into(),
                            detail: Some(format!("{other:?}")),
                            hint: None,
                            sqlstate: "XX000",
                        });
                    }
                };
                call_args.push(current_state);
                call_args.extend(arg_values.iter().cloned());
                if custom.transfn_strict
                    && call_args.iter().any(|value| matches!(value, Value::Null))
                {
                    return Ok(());
                }
                let value =
                    execute_scalar_function_value_call(custom.transfn_oid, &call_args, ctx)?;
                *state = AccumState::custom(super::cast_value(value, custom.transtype)?);
                Ok(())
            }
        }
    }

    pub(crate) fn finalize(
        &self,
        accum: &AggAccum,
        state: &AccumState,
        ordered_inputs: &[OrderedAggInput],
        direct_arg_values: &[Value],
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<Value, ExecError> {
        match self {
            AggregateRuntime::Builtin { .. } => Ok(state.finalize()),
            AggregateRuntime::Hypothetical { func } => {
                finalize_hypothetical_aggregate(*func, accum, ordered_inputs, direct_arg_values)
            }
            AggregateRuntime::Custom(custom) => {
                let state_value = match state {
                    AccumState::Custom { value } => value.clone(),
                    other => {
                        return Err(ExecError::DetailedError {
                            message: "custom aggregate state shape mismatch".into(),
                            detail: Some(format!("{other:?}")),
                            hint: None,
                            sqlstate: "XX000",
                        });
                    }
                };
                if let Some(finalfn_oid) = custom.finalfn_oid {
                    if custom.finalfn_strict && matches!(state_value, Value::Null) {
                        return Ok(Value::Null);
                    }
                    execute_scalar_function_value_call(finalfn_oid, &[state_value], ctx)
                } else {
                    Ok(state_value)
                }
            }
        }
    }
}

fn finalize_hypothetical_aggregate(
    func: HypotheticalAggFunc,
    accum: &AggAccum,
    ordered_inputs: &[OrderedAggInput],
    direct_arg_values: &[Value],
) -> Result<Value, ExecError> {
    if direct_arg_values.len() != accum.order_by.len() {
        return Err(ExecError::DetailedError {
            message: "ordered-set aggregate direct-argument count mismatch".into(),
            detail: Some(format!(
                "direct args = {}, order columns = {}",
                direct_arg_values.len(),
                accum.order_by.len(),
            )),
            hint: None,
            sqlstate: "XX000",
        });
    }

    let mut preceding_rows = 0usize;
    let mut peer_rows = 0usize;
    let mut preceding_groups = 0usize;
    let mut previous_preceding_keys: Option<&[Value]> = None;

    for input in ordered_inputs {
        match compare_order_by_keys(&accum.order_by, &input.sort_keys, direct_arg_values)? {
            Ordering::Less => {
                preceding_rows += 1;
                let is_new_group = match previous_preceding_keys {
                    Some(previous) => {
                        compare_order_by_keys(&accum.order_by, previous, &input.sort_keys)?
                            != Ordering::Equal
                    }
                    None => true,
                };
                if is_new_group {
                    preceding_groups += 1;
                }
                previous_preceding_keys = Some(&input.sort_keys);
            }
            Ordering::Equal => peer_rows += 1,
            Ordering::Greater => break,
        }
    }

    Ok(match func {
        HypotheticalAggFunc::Rank => Value::Int64((preceding_rows + 1) as i64),
        HypotheticalAggFunc::DenseRank => Value::Int64((preceding_groups + 1) as i64),
        HypotheticalAggFunc::PercentRank => {
            if ordered_inputs.is_empty() {
                Value::Float64(0.0)
            } else {
                Value::Float64(preceding_rows as f64 / ordered_inputs.len() as f64)
            }
        }
        HypotheticalAggFunc::CumeDist => Value::Float64(
            (preceding_rows + peer_rows + 1) as f64 / (ordered_inputs.len() + 1) as f64,
        ),
    })
}

fn finalize_regr_stats(
    func: AggFunc,
    count: f64,
    sum_x: f64,
    sum_sq_x: f64,
    sum_y: f64,
    sum_sq_y: f64,
    sum_xy: f64,
    all_x_equal: bool,
    all_y_equal: bool,
) -> Value {
    let sum_sq_x = stable_regr_semidefinite_sum(sum_sq_x, sum_x, count);
    let sum_sq_y = stable_regr_semidefinite_sum(sum_sq_y, sum_y, count);
    match func {
        AggFunc::RegrCount => Value::Int64(count as i64),
        AggFunc::RegrSxx => regr_value_or_null(count, sum_sq_x),
        AggFunc::RegrSyy => regr_value_or_null(count, sum_sq_y),
        AggFunc::RegrSxy => regr_value_or_null(count, sum_xy),
        AggFunc::RegrAvgX => regr_value_or_null(count, sum_x / count),
        AggFunc::RegrAvgY => regr_value_or_null(count, sum_y / count),
        AggFunc::CovarPop => regr_value_or_null(count, sum_xy / count),
        AggFunc::CovarSamp => {
            if count < 2.0 {
                Value::Null
            } else {
                Value::Float64(sum_xy / (count - 1.0))
            }
        }
        AggFunc::Corr => {
            if count < 1.0 || all_x_equal || all_y_equal {
                Value::Null
            } else {
                Value::Float64(clamp_corr(sum_xy / (sum_sq_x.sqrt() * sum_sq_y.sqrt())))
            }
        }
        AggFunc::RegrR2 => {
            if count < 1.0 || all_x_equal {
                Value::Null
            } else if all_y_equal {
                Value::Float64(1.0)
            } else {
                let corr = clamp_corr(sum_xy / (sum_sq_x.sqrt() * sum_sq_y.sqrt()));
                Value::Float64(clamp_regr_r2(corr * corr))
            }
        }
        AggFunc::RegrSlope => {
            if count < 1.0 || all_x_equal {
                Value::Null
            } else {
                Value::Float64(sum_xy / sum_sq_x)
            }
        }
        AggFunc::RegrIntercept => {
            if count < 1.0 || all_x_equal {
                Value::Null
            } else {
                Value::Float64((sum_y - sum_x * sum_xy / sum_sq_x) / count)
            }
        }
        AggFunc::BoolAnd
        | AggFunc::BoolOr
        | AggFunc::Count
        | AggFunc::AnyValue
        | AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::VarPop
        | AggFunc::VarSamp
        | AggFunc::StddevPop
        | AggFunc::StddevSamp
        | AggFunc::BitAnd
        | AggFunc::BitOr
        | AggFunc::BitXor
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::StringAgg
        | AggFunc::ArrayAgg
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg
        | AggFunc::JsonObjectAgg
        | AggFunc::JsonbObjectAgg
        | AggFunc::RangeAgg
        | AggFunc::XmlAgg
        | AggFunc::RangeIntersectAgg => unreachable!("non-regression aggregate"),
    }
}

fn stable_regr_semidefinite_sum(sum_sq: f64, sum: f64, count: f64) -> f64 {
    if !sum_sq.is_finite() || !sum.is_finite() || count < 1.0 {
        return sum_sq;
    }

    let mean = sum / count;
    let tolerance = 8.0 * f64::EPSILON * count * mean * mean;
    if sum_sq.abs() <= tolerance {
        0.0
    } else {
        sum_sq
    }
}

fn clamp_corr(value: f64) -> f64 {
    if !value.is_finite() {
        value
    } else {
        let tolerance = 8.0 * f64::EPSILON;
        if (value - 1.0).abs() <= tolerance {
            1.0
        } else if (value + 1.0).abs() <= tolerance {
            -1.0
        } else if value.abs() <= tolerance {
            0.0
        } else {
            value.clamp(-1.0, 1.0)
        }
    }
}

fn clamp_regr_r2(value: f64) -> f64 {
    if !value.is_finite() {
        value
    } else {
        let tolerance = 8.0 * f64::EPSILON;
        if value.abs() <= tolerance {
            0.0
        } else if (value - 1.0).abs() <= tolerance {
            1.0
        } else {
            value.clamp(0.0, 1.0)
        }
    }
}

fn float8_regr_constant_value_eq(value: f64, first: f64) -> bool {
    !value.is_nan() && !first.is_nan() && value == first
}

fn regr_value_or_null(count: f64, value: f64) -> Value {
    if count < 1.0 {
        Value::Null
    } else {
        Value::Float64(value)
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

fn validate_array_agg_array_input(
    value: &Value,
    inner_dims: &mut Option<Vec<ArrayDimension>>,
) -> Result<(), ExecError> {
    let Some(array) = normalize_array_value(value) else {
        return Err(ExecError::DetailedError {
            message: "cannot accumulate null arrays".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        });
    };
    if array.dimensions.is_empty() {
        return Err(ExecError::DetailedError {
            message: "cannot accumulate empty arrays".into(),
            detail: None,
            hint: None,
            sqlstate: "2202E",
        });
    }
    match inner_dims {
        None => *inner_dims = Some(array.dimensions),
        Some(existing) if existing.as_slice() != array.dimensions.as_slice() => {
            return Err(ExecError::DetailedError {
                message: "cannot accumulate arrays of different dimensionality".into(),
                detail: None,
                hint: None,
                sqlstate: "2202E",
            });
        }
        Some(_) => {}
    }
    Ok(())
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

fn render_json_agg_array(values: &[Value]) -> String {
    let mut out = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
            if matches!(
                value,
                Value::Array(_) | Value::PgArray(_) | Value::Record(_)
            ) {
                out.push_str("\n ");
            }
        }
        out.push_str(&value_to_json_text(value));
    }
    out.push(']');
    out
}

fn render_json_object_agg(pairs: &[(Value, Value)]) -> String {
    let mut out = String::from("{ ");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        let key_text = json_object_agg_key(key);
        out.push_str(&serde_json::to_string(&key_text).unwrap());
        out.push_str(" : ");
        out.push_str(&value_to_json_text(value));
    }
    out.push_str(" }");
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
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => render_macaddr_text(v),
        Value::MacAddr8(v) => render_macaddr8_text(v),
        Value::InternalChar(v) => crate::backend::executor::render_internal_char_text(*v),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => render_jsonb_bytes(v).unwrap_or_else(|_| "null".into()),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => render_interval_text(*v),
        Value::Uuid(v) => crate::backend::executor::value_io::render_uuid_text(v),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::EnumOid(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => crate::backend::executor::render_pg_lsn_text(*v),
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
        Value::Xml(v) => v.to_string(),
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
        Value::Multirange(_) => {
            crate::backend::executor::render_multirange_text(key).unwrap_or_default()
        }
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
        Value::EnumOid(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => {
            serde_json::to_string(&crate::backend::executor::render_pg_lsn_text(*v)).unwrap()
        }
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => serde_json::to_string(&render_interval_text(*v)).unwrap(),
        Value::Uuid(v) => {
            serde_json::to_string(&crate::backend::executor::value_io::render_uuid_text(v)).unwrap()
        }
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
        Value::Xml(v) => serde_json::to_string(v.as_str()).unwrap(),
        Value::Text(_) | Value::TextRef(_, _) => {
            serde_json::to_string(value.as_text().unwrap()).unwrap()
        }
        Value::Bytea(v) => {
            serde_json::to_string(&format_bytea_text(v, ByteaOutputFormat::Hex)).unwrap()
        }
        Value::Inet(v) => serde_json::to_string(&v.render_inet()).unwrap(),
        Value::Cidr(v) => serde_json::to_string(&v.render_cidr()).unwrap(),
        Value::MacAddr(v) => serde_json::to_string(&render_macaddr_text(v)).unwrap(),
        Value::MacAddr8(v) => serde_json::to_string(&render_macaddr8_text(v)).unwrap(),
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
        Value::Multirange(_) => serde_json::to_string(
            &crate::backend::executor::render_multirange_text(value).unwrap_or_default(),
        )
        .unwrap(),
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

fn interval_avg_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
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

fn aggregate_float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}

fn float8_overflow_error() -> ExecError {
    ExecError::DetailedError {
        message: "value out of range: overflow".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
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

fn numeric_visible_scale(value: &NumericValue) -> u32 {
    value
        .render()
        .split_once('.')
        .map(|(_, frac)| frac.len() as u32)
        .unwrap_or(0)
}

fn numeric_quotient_decimal_weight(lhs: &NumericValue, rhs: &NumericValue) -> i32 {
    fn decimal_weight(value: &NumericValue) -> i32 {
        match value {
            NumericValue::Finite { coeff, scale, .. } if !coeff.is_zero() => {
                coeff.abs().to_str_radix(10).len() as i32 - *scale as i32 - 1
            }
            _ => 0,
        }
    }

    decimal_weight(lhs) - decimal_weight(rhs)
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
    const NUMERIC_MAX_DISPLAY_SCALE: i32 = 1000;
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
    pub(crate) passthrough_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
    pub(crate) distinct_inputs: Vec<Option<HashSet<Vec<Value>>>>,
    pub(crate) direct_arg_values: Vec<Option<Vec<Value>>>,
    pub(crate) ordered_inputs: Vec<Vec<OrderedAggInput>>,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderedAggInput {
    pub(crate) sort_keys: Vec<Value>,
    pub(crate) arg_values: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn xmlagg_finalizes_with_xmlconcat_semantics() {
        let mut state = AccumState::new(AggFunc::XmlAgg, false, SqlType::new(SqlTypeKind::Xml));
        let transition = AccumState::transition_fn(AggFunc::XmlAgg, 1, false);
        transition(
            &mut state,
            &[Value::Xml(CompactString::from(
                "<?xml version=\"1.1\"?><foo/>",
            ))],
        )
        .unwrap();
        transition(
            &mut state,
            &[Value::Xml(CompactString::from(
                "<?xml version=\"1.1\" standalone=\"no\"?><bar/>",
            ))],
        )
        .unwrap();

        assert_eq!(
            state.finalize(),
            Value::Xml(CompactString::from("<?xml version=\"1.1\"?><foo/><bar/>"))
        );
    }
}
