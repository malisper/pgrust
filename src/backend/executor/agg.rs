use super::render_bit_text;
use super::{
    cast_value_with_source_type_catalog_and_config, compare_order_values,
    render_datetime_value_text, render_interval_text, render_macaddr_text, render_macaddr8_text,
};
use crate::backend::executor::ExecError;
use crate::backend::executor::exec_expr::cast_record_value_for_target;
use crate::backend::executor::expr_agg_support::{
    aggregate_support_error, execute_scalar_function_value_call_with_arg_types,
};
use crate::backend::executor::expr_ops::{
    bitwise_and_values, bitwise_or_values, bitwise_xor_values, compare_order_by_keys,
    interval_div_float,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{FLOAT8_TYPE_OID, INTERVAL_TYPE_OID};
use crate::include::nodes::datum::{
    ArrayDimension, ArrayValue, IntervalValue, NumericValue, Value,
};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, HypotheticalAggFunc, OrderedSetAggFunc, expr_sql_type_hint,
};
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;

use std::cmp::Ordering;
use std::collections::HashSet;

use super::expr_multirange::{multirange_intersection_agg_transition, range_agg_transition};
use super::expr_range::{range_intersection_agg_transition, render_range_text};
use super::expr_xml::concat_xml_texts;
use super::jsonb::{JsonbValue, encode_jsonb, jsonb_from_value, render_jsonb_bytes};
pub(crate) use pgrust_executor::{
    CustomAggregateRuntime, NumericAccum, accumulate_sum_value, accumulate_value,
    aggregate_float_value, aggregate_numeric_value, format_numeric_result, numeric_accum_to_value,
    numeric_div_display_scale, numeric_quotient_decimal_weight, numeric_sqrt,
    numeric_visible_scale,
};

pub(crate) type AggTransitionFn = fn(&mut AccumState, &[Value]) -> Result<(), ExecError>;

#[derive(Debug, Clone)]
pub(crate) enum AggregateRuntime {
    Builtin {
        func: AggFunc,
        transition: AggTransitionFn,
    },
    Hypothetical {
        func: HypotheticalAggFunc,
    },
    OrderedSet {
        func: OrderedSetAggFunc,
    },
    Custom(CustomAggregateRuntime),
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
        unique_keys: bool,
        strict_values: bool,
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
                unique_keys: false,
                strict_values: false,
            },
            (AggFunc::JsonObjectAggUnique, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: false,
                unique_keys: true,
                strict_values: false,
            },
            (AggFunc::JsonObjectAggUniqueStrict, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: false,
                unique_keys: true,
                strict_values: true,
            },
            (AggFunc::JsonbObjectAgg, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: true,
                unique_keys: false,
                strict_values: false,
            },
            (AggFunc::JsonbObjectAggUnique, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: true,
                unique_keys: true,
                strict_values: false,
            },
            (AggFunc::JsonbObjectAggUniqueStrict, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: true,
                unique_keys: true,
                strict_values: true,
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
                    if let Value::Interval(next) = value {
                        *sum = Some(match sum.take() {
                            Some(NumericAccum::Interval(current)) => NumericAccum::Interval(
                                current
                                    .checked_add(*next)
                                    .ok_or_else(interval_avg_out_of_range)?,
                            ),
                            _ => NumericAccum::Interval(*next),
                        });
                    } else {
                        *sum = accumulate_sum_value(sum.take(), *result_type, value);
                    }
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
                    let y = pgrust_executor::expect_float8_arg("regr aggregate", y)
                        .map_err(aggregate_support_error)?;
                    let x = pgrust_executor::expect_float8_arg("regr aggregate", x)
                        .map_err(aggregate_support_error)?;
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
                        pgrust_executor::float8_regr_accum_state(
                            *count, *sum_x, *sum_sq_x, *sum_y, *sum_sq_y, *sum_xy, y, x,
                        )
                        .map_err(aggregate_support_error)?;
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
            (
                AggFunc::JsonObjectAgg
                | AggFunc::JsonObjectAggUnique
                | AggFunc::JsonObjectAggUniqueStrict
                | AggFunc::JsonbObjectAgg
                | AggFunc::JsonbObjectAggUnique
                | AggFunc::JsonbObjectAggUniqueStrict,
                _,
                _,
            ) => |state, values| {
                if let AccumState::JsonObjectAgg {
                    pairs,
                    unique_keys,
                    strict_values,
                    jsonb,
                    ..
                } = state
                {
                    let key = values.first().unwrap_or(&Value::Null);
                    if matches!(key, Value::Null) {
                        return Err(ExecError::DetailedError {
                            message: if *jsonb {
                                "field name must not be null".into()
                            } else {
                                "null value not allowed for object key".into()
                            },
                            detail: None,
                            hint: None,
                            sqlstate: "22004",
                        });
                    }
                    let value = values.get(1).unwrap_or(&Value::Null);
                    let key_text = json_object_agg_key(key);
                    if *unique_keys
                        && pairs
                            .iter()
                            .any(|(existing_key, _)| json_object_agg_key(existing_key) == key_text)
                    {
                        return Err(ExecError::DetailedError {
                            message: format!("duplicate JSON object key value: \"{key_text}\""),
                            detail: None,
                            hint: None,
                            sqlstate: "22030",
                        });
                    }
                    if *strict_values && matches!(value, Value::Null) {
                        return Ok(());
                    }
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
                Some(NumericAccum::NumericSum(v)) => {
                    Value::Numeric(format_numeric_result(v.to_numeric(), *result_type))
                }
                Some(NumericAccum::Interval(v)) => Value::Interval(*v),
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
                        Some(NumericAccum::NumericSum(v)) => {
                            let sum = v.to_numeric();
                            let count_numeric = NumericValue::from_i64(*count);
                            let avg = sum
                                .div(
                                    &count_numeric,
                                    numeric_div_display_scale(&sum, &count_numeric),
                                )
                                .unwrap_or(sum);
                            Value::Numeric(format_numeric_result(avg, *result_type))
                        }
                        Some(NumericAccum::Interval(_)) => Value::Null,
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
            AccumState::JsonObjectAgg { pairs, jsonb, .. } => {
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
    pub(crate) fn supports_custom_combine(&self) -> bool {
        matches!(
            self,
            AggregateRuntime::Custom(CustomAggregateRuntime {
                combinefn_oid: Some(_),
                ..
            })
        )
    }

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
            AggregateRuntime::Hypothetical { .. } | AggregateRuntime::OrderedSet { .. } => {
                AccumState::Hypothetical
            }
            AggregateRuntime::Custom(custom) => {
                AccumState::custom(custom.init_value.clone().unwrap_or(Value::Null))
            }
        }
    }

    pub(crate) fn supports_moving_transition(&self) -> bool {
        matches!(
            self,
            AggregateRuntime::Custom(CustomAggregateRuntime {
                mtransfn_oid: Some(_),
                minvtransfn_oid: Some(_),
                ..
            })
        )
    }

    pub(crate) fn moving_transition_is_strict(&self) -> bool {
        matches!(
            self,
            AggregateRuntime::Custom(CustomAggregateRuntime {
                mtransfn_strict: true,
                minvtransfn_strict: true,
                ..
            })
        )
    }

    pub(crate) fn initialize_moving_state(&self) -> AccumState {
        match self {
            AggregateRuntime::Custom(custom) => {
                AccumState::custom(custom.minit_value.clone().unwrap_or(Value::Null))
            }
            _ => AccumState::Hypothetical,
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
            AggregateRuntime::Hypothetical { .. } | AggregateRuntime::OrderedSet { .. } => Ok(()),
            AggregateRuntime::Custom(custom) => custom_transition(
                state,
                arg_values,
                ctx,
                CustomTransitionCall {
                    proc_oid: custom.transfn_oid,
                    strict: custom.transfn_strict,
                    init_from_first_arg: custom.init_value.is_none()
                        && custom
                            .transfn_arg_types
                            .get(1)
                            .is_some_and(|arg_type| *arg_type == custom.transtype),
                    state_type: custom.transtype,
                    arg_types: &custom.transfn_arg_types,
                },
            ),
        }
    }

    pub(crate) fn moving_transition(
        &self,
        state: &mut AccumState,
        arg_values: &[Value],
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<(), ExecError> {
        match self {
            AggregateRuntime::Custom(custom) => {
                let Some(proc_oid) = custom.mtransfn_oid else {
                    return Ok(());
                };
                custom_transition(
                    state,
                    arg_values,
                    ctx,
                    CustomTransitionCall {
                        proc_oid,
                        strict: custom.mtransfn_strict,
                        init_from_first_arg: custom.minit_value.is_none()
                            && custom
                                .mtransfn_arg_types
                                .get(1)
                                .is_some_and(|arg_type| *arg_type == custom.mtranstype),
                        state_type: custom.mtranstype,
                        arg_types: &custom.mtransfn_arg_types,
                    },
                )
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn moving_inverse(
        &self,
        state: &mut AccumState,
        arg_values: &[Value],
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<bool, ExecError> {
        match self {
            AggregateRuntime::Custom(custom) => {
                let Some(proc_oid) = custom.minvtransfn_oid else {
                    return Ok(false);
                };
                let value = custom_transition_value(
                    state,
                    arg_values,
                    ctx,
                    CustomTransitionCall {
                        proc_oid,
                        strict: custom.minvtransfn_strict,
                        init_from_first_arg: false,
                        state_type: custom.mtranstype,
                        arg_types: &custom.minvtransfn_arg_types,
                    },
                )?;
                if matches!(value, Value::Null) {
                    return Ok(false);
                }
                *state =
                    AccumState::custom(cast_custom_aggregate_value(value, custom.mtranstype, ctx)?);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(crate) fn partial_value(
        &self,
        accum: &AggAccum,
        state: &AccumState,
    ) -> Result<Value, ExecError> {
        match (self, state) {
            (
                AggregateRuntime::Builtin {
                    func: AggFunc::Count,
                    ..
                },
                AccumState::Count { count },
            ) => Ok(Value::Int64(*count)),
            (
                AggregateRuntime::Builtin {
                    func: AggFunc::Sum, ..
                },
                AccumState::Sum { .. },
            )
            | (
                AggregateRuntime::Builtin {
                    func: AggFunc::Min, ..
                },
                AccumState::Min { .. },
            )
            | (
                AggregateRuntime::Builtin {
                    func: AggFunc::Max, ..
                },
                AccumState::Max { .. },
            ) => Ok(state.finalize()),
            (
                AggregateRuntime::Builtin {
                    func: AggFunc::Avg, ..
                },
                AccumState::Avg {
                    sum,
                    count,
                    result_type,
                },
            ) => Ok(Value::Record(
                crate::include::nodes::datum::RecordValue::anonymous(vec![
                    (
                        "sum".into(),
                        numeric_accum_to_value(sum.as_ref(), *result_type),
                    ),
                    ("count".into(), Value::Int64(*count)),
                ]),
            )),
            (
                AggregateRuntime::Builtin {
                    func: AggFunc::Avg, ..
                },
                AccumState::IntervalAvg { sum, count },
            ) => Ok(Value::Record(
                crate::include::nodes::datum::RecordValue::anonymous(vec![
                    (
                        "sum".into(),
                        sum.clone().map(Value::Interval).unwrap_or(Value::Null),
                    ),
                    ("count".into(), Value::Int64(*count)),
                ]),
            )),
            (AggregateRuntime::Builtin { func, .. }, _) => Err(ExecError::DetailedError {
                message: format!("aggregate {func:?} is not partial-safe"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            }),
            _ => Err(ExecError::DetailedError {
                message: "only builtin aggregates support partial aggregation".into(),
                detail: Some(format!("aggregate oid {}", accum.aggfnoid)),
                hint: None,
                sqlstate: "0A000",
            }),
        }
    }

    pub(crate) fn combine_partial(
        &self,
        accum: &AggAccum,
        state: &mut AccumState,
        partial: &Value,
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<(), ExecError> {
        match self {
            AggregateRuntime::Builtin {
                func: AggFunc::Count,
                ..
            } => {
                let Value::Int64(value) = partial else {
                    return Ok(());
                };
                if let AccumState::Count { count } = state {
                    *count += *value;
                }
                Ok(())
            }
            AggregateRuntime::Builtin {
                func: AggFunc::Avg, ..
            } => {
                let Value::Record(record) = partial else {
                    return Ok(());
                };
                let Some(sum_value) = record.fields.first() else {
                    return Ok(());
                };
                let partial_count = match record.fields.get(1) {
                    Some(Value::Int64(value)) => *value,
                    _ => 0,
                };
                match state {
                    AccumState::Avg {
                        sum,
                        count,
                        result_type,
                    } => {
                        *sum = accumulate_value(sum.take(), *result_type, sum_value);
                        *count += partial_count;
                    }
                    AccumState::IntervalAvg { sum, count } => {
                        if let Value::Interval(value) = sum_value {
                            *sum = Some(match sum.take() {
                                Some(current) => current
                                    .checked_add(*value)
                                    .ok_or_else(interval_avg_out_of_range)?,
                                None => value.clone(),
                            });
                        }
                        *count += partial_count;
                    }
                    _ => {}
                }
                Ok(())
            }
            AggregateRuntime::Builtin {
                func: AggFunc::Sum, ..
            }
            | AggregateRuntime::Builtin {
                func: AggFunc::Min, ..
            }
            | AggregateRuntime::Builtin {
                func: AggFunc::Max, ..
            } => self.transition(state, std::slice::from_ref(partial), ctx),
            AggregateRuntime::Custom(custom) => {
                let Some(proc_oid) = custom.combinefn_oid else {
                    return Err(ExecError::DetailedError {
                        message: "custom aggregate does not have a combine function".into(),
                        detail: Some(format!("aggregate oid {}", accum.aggfnoid)),
                        hint: None,
                        sqlstate: "0A000",
                    });
                };
                let value = custom_transition_value(
                    state,
                    std::slice::from_ref(partial),
                    ctx,
                    CustomTransitionCall {
                        proc_oid,
                        strict: custom.combinefn_strict,
                        init_from_first_arg: false,
                        state_type: custom.transtype,
                        arg_types: &custom.combinefn_arg_types,
                    },
                )?;
                *state =
                    AccumState::custom(cast_custom_aggregate_value(value, custom.transtype, ctx)?);
                Ok(())
            }
            _ => Err(ExecError::DetailedError {
                message: "only builtin aggregates support partial aggregation".into(),
                detail: Some(format!("aggregate oid {}", accum.aggfnoid)),
                hint: None,
                sqlstate: "0A000",
            }),
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
            AggregateRuntime::OrderedSet { func } => {
                finalize_ordered_set_aggregate(*func, accum, ordered_inputs, direct_arg_values)
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
                    let mut final_args = Vec::with_capacity(custom.finalfn_arg_types.len());
                    final_args.push(state_value);
                    final_args.extend(std::iter::repeat_n(
                        Value::Null,
                        custom.finalfn_arg_types.len() - 1,
                    ));
                    execute_scalar_function_value_call_with_arg_types(
                        finalfn_oid,
                        &final_args,
                        Some(&custom.finalfn_arg_types),
                        ctx,
                    )
                    .map(|value| value.to_owned_value())
                } else {
                    Ok(state_value.to_owned_value())
                }
            }
        }
    }

    pub(crate) fn finalize_moving(
        &self,
        state: &AccumState,
        ctx: &mut crate::backend::executor::ExecutorContext,
    ) -> Result<Value, ExecError> {
        match self {
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
                if let Some(finalfn_oid) = custom.mfinalfn_oid {
                    if custom.mfinalfn_strict && matches!(state_value, Value::Null) {
                        return Ok(Value::Null);
                    }
                    let mut final_args = Vec::with_capacity(custom.mfinalfn_arg_types.len());
                    final_args.push(state_value);
                    final_args.extend(std::iter::repeat_n(
                        Value::Null,
                        custom.mfinalfn_arg_types.len() - 1,
                    ));
                    execute_scalar_function_value_call_with_arg_types(
                        finalfn_oid,
                        &final_args,
                        Some(&custom.mfinalfn_arg_types),
                        ctx,
                    )
                    .map(|value| value.to_owned_value())
                } else {
                    Ok(state_value.to_owned_value())
                }
            }
            _ => Ok(state.finalize()),
        }
    }
}

#[derive(Clone, Copy)]
struct CustomTransitionCall<'a> {
    proc_oid: u32,
    strict: bool,
    init_from_first_arg: bool,
    state_type: SqlType,
    arg_types: &'a [SqlType],
}

fn custom_transition(
    state: &mut AccumState,
    arg_values: &[Value],
    ctx: &mut crate::backend::executor::ExecutorContext,
    call: CustomTransitionCall<'_>,
) -> Result<(), ExecError> {
    let value = custom_transition_value(state, arg_values, ctx, call)?;
    *state = AccumState::custom(cast_custom_aggregate_value(value, call.state_type, ctx)?);
    Ok(())
}

fn custom_transition_value(
    state: &AccumState,
    arg_values: &[Value],
    ctx: &mut crate::backend::executor::ExecutorContext,
    call: CustomTransitionCall<'_>,
) -> Result<Value, ExecError> {
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
    if call.strict && matches!(current_state, Value::Null) {
        if arg_values.iter().any(|value| matches!(value, Value::Null)) {
            return Ok(Value::Null);
        }
        if call.init_from_first_arg
            && let [first] = arg_values
        {
            return cast_custom_aggregate_value(first.clone(), call.state_type, ctx);
        }
        return Ok(Value::Null);
    }
    let mut call_args = Vec::with_capacity(arg_values.len() + 1);
    call_args.push(current_state);
    call_args.extend(arg_values.iter().cloned());
    if call.strict && call_args.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(match state {
            AccumState::Custom { value } => value.clone(),
            _ => Value::Null,
        });
    }
    execute_scalar_function_value_call_with_arg_types(
        call.proc_oid,
        &call_args,
        Some(call.arg_types),
        ctx,
    )
}

fn cast_custom_aggregate_value(
    value: Value,
    target_type: SqlType,
    ctx: &crate::backend::executor::ExecutorContext,
) -> Result<Value, ExecError> {
    if let Value::Record(record) = value {
        return cast_record_value_for_target(record, target_type, ctx);
    }
    cast_value_with_source_type_catalog_and_config(
        value,
        None,
        target_type,
        ctx.catalog.as_deref(),
        &ctx.datetime_config,
    )
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
    let hypothetical_keys = direct_arg_values
        .iter()
        .zip(accum.order_by.iter())
        .map(|(value, order_by)| {
            expr_sql_type_hint(&order_by.expr)
                .map(|target| super::cast_value(value.clone(), target))
                .unwrap_or_else(|| Ok(value.clone()))
        })
        .collect::<Result<Vec<_>, ExecError>>()?;

    for input in ordered_inputs {
        match compare_order_by_keys(&accum.order_by, &input.sort_keys, &hypothetical_keys)? {
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

fn finalize_ordered_set_aggregate(
    func: OrderedSetAggFunc,
    accum: &AggAccum,
    ordered_inputs: &[OrderedAggInput],
    direct_arg_values: &[Value],
) -> Result<Value, ExecError> {
    match func {
        OrderedSetAggFunc::PercentileDisc | OrderedSetAggFunc::PercentileDiscMulti => {
            let Some(percentile) = direct_arg_values.first() else {
                return Err(ExecError::DetailedError {
                    message: "ordered-set aggregate direct-argument count mismatch".into(),
                    detail: Some("percentile_disc expects one percentile argument".into()),
                    hint: None,
                    sqlstate: "XX000",
                });
            };
            let values = ordered_inputs
                .iter()
                .filter_map(|input| input.arg_values.first())
                .filter(|value| !matches!(value, Value::Null))
                .collect::<Vec<_>>();
            if matches!(func, OrderedSetAggFunc::PercentileDiscMulti) {
                return finalize_percentile_disc_multi(accum, percentile, &values);
            }
            let Some(percentile) = checked_percentile_value("percentile_disc", percentile)? else {
                return Ok(Value::Null);
            };
            Ok(percentile_disc_value(percentile, &values).unwrap_or(Value::Null))
        }
        OrderedSetAggFunc::PercentileCont | OrderedSetAggFunc::PercentileContMulti => {
            let Some(percentile) = direct_arg_values.first() else {
                return Err(ExecError::DetailedError {
                    message: "ordered-set aggregate direct-argument count mismatch".into(),
                    detail: Some("percentile_cont expects one percentile argument".into()),
                    hint: None,
                    sqlstate: "XX000",
                });
            };
            let values = ordered_inputs
                .iter()
                .filter_map(|input| input.arg_values.first())
                .filter(|value| !matches!(value, Value::Null))
                .collect::<Vec<_>>();
            if matches!(func, OrderedSetAggFunc::PercentileContMulti) {
                return finalize_percentile_cont_multi(accum, percentile, &values);
            }
            let Some(percentile) = checked_percentile_value("percentile_cont", percentile)? else {
                return Ok(Value::Null);
            };
            percentile_cont_value(percentile, &values)
        }
        OrderedSetAggFunc::Mode => finalize_mode_aggregate(ordered_inputs),
    }
}

fn checked_percentile_value(op: &'static str, value: &Value) -> Result<Option<f64>, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let percentile =
        pgrust_executor::expect_float8_arg(op, value).map_err(aggregate_support_error)?;
    if !(0.0..=1.0).contains(&percentile) || percentile.is_nan() {
        return Err(ExecError::DetailedError {
            message: format!("percentile value {percentile} is not between 0 and 1"),
            detail: None,
            hint: None,
            sqlstate: "22003",
        });
    }
    Ok(Some(percentile))
}

fn percentile_array_arg(
    op: &'static str,
    value: &Value,
) -> Result<Option<(Vec<ArrayDimension>, Vec<Value>)>, ExecError> {
    match value {
        Value::Null => Ok(None),
        Value::PgArray(array) => Ok(Some((array.dimensions.clone(), array.elements.clone()))),
        Value::Array(elements) => Ok(Some((
            vec![ArrayDimension {
                lower_bound: 1,
                length: elements.len(),
            }],
            elements.clone(),
        ))),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::PgArray(ArrayValue::empty()),
        }),
    }
}

fn finalize_percentile_disc_multi(
    accum: &AggAccum,
    percentile_arg: &Value,
    values: &[&Value],
) -> Result<Value, ExecError> {
    let Some((dimensions, percentiles)) = percentile_array_arg("percentile_disc", percentile_arg)?
    else {
        return Ok(Value::Null);
    };
    let elements = percentiles
        .iter()
        .map(|percentile| {
            let Some(percentile) = checked_percentile_value("percentile_disc", percentile)? else {
                return Ok(Value::Null);
            };
            Ok(percentile_disc_value(percentile, values).unwrap_or(Value::Null))
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let mut array = ArrayValue::from_dimensions(dimensions, elements);
    if let Some(element_type_oid) = accum
        .args
        .first()
        .and_then(expr_sql_type_hint)
        .map(|ty| sql_type_oid(ty.element_type()))
        .filter(|oid| *oid != 0)
    {
        array = array.with_element_type_oid(element_type_oid);
    }
    Ok(Value::PgArray(array))
}

fn percentile_disc_value(percentile: f64, values: &[&Value]) -> Option<Value> {
    if values.is_empty() {
        return None;
    }
    let rank = (percentile * values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    Some(values[index].to_owned_value())
}

fn finalize_percentile_cont_multi(
    _accum: &AggAccum,
    percentile_arg: &Value,
    values: &[&Value],
) -> Result<Value, ExecError> {
    let Some((dimensions, percentiles)) = percentile_array_arg("percentile_cont", percentile_arg)?
    else {
        return Ok(Value::Null);
    };
    let elements = percentiles
        .iter()
        .map(|percentile| {
            let Some(percentile) = checked_percentile_value("percentile_cont", percentile)? else {
                return Ok(Value::Null);
            };
            percentile_cont_value(percentile, values)
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let mut array = ArrayValue::from_dimensions(dimensions, elements);
    if values
        .first()
        .is_some_and(|value| matches!(value, Value::Interval(_)))
    {
        array = array.with_element_type_oid(INTERVAL_TYPE_OID);
    } else {
        array = array.with_element_type_oid(FLOAT8_TYPE_OID);
    }
    Ok(Value::PgArray(array))
}

fn percentile_cont_value(percentile: f64, values: &[&Value]) -> Result<Value, ExecError> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let pos = percentile * (values.len() - 1) as f64;
    let lower_index = pos.floor() as usize;
    let upper_index = pos.ceil() as usize;
    let fraction = pos - lower_index as f64;
    let lower = values[lower_index];
    let upper = values[upper_index];
    if lower_index == upper_index || fraction == 0.0 {
        return Ok(lower.to_owned_value());
    }
    match (lower, upper) {
        (Value::Interval(lower), Value::Interval(upper)) => {
            let delta = upper
                .checked_sub(*lower)
                .ok_or_else(ordered_set_interval_out_of_range)?;
            let scaled = scale_interval_for_percentile(delta, fraction)?;
            Ok(Value::Interval(
                lower
                    .checked_add(scaled)
                    .ok_or_else(ordered_set_interval_out_of_range)?,
            ))
        }
        _ => {
            let lower = pgrust_executor::expect_float8_arg("percentile_cont", lower)
                .map_err(aggregate_support_error)?;
            let upper = pgrust_executor::expect_float8_arg("percentile_cont", upper)
                .map_err(aggregate_support_error)?;
            Ok(Value::Float64(lower + (upper - lower) * fraction))
        }
    }
}

fn scale_interval_for_percentile(
    interval: IntervalValue,
    factor: f64,
) -> Result<IntervalValue, ExecError> {
    if factor == 0.0 {
        return Ok(IntervalValue::zero());
    }
    interval_div_float(interval, 1.0 / factor).ok_or_else(ordered_set_interval_out_of_range)
}

fn ordered_set_interval_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn finalize_mode_aggregate(ordered_inputs: &[OrderedAggInput]) -> Result<Value, ExecError> {
    let mut best_value = Value::Null;
    let mut best_count = 0usize;
    let mut current_value: Option<Value> = None;
    let mut current_count = 0usize;

    for value in ordered_inputs
        .iter()
        .filter_map(|input| input.arg_values.first())
        .filter(|value| !matches!(value, Value::Null))
    {
        let same_group = current_value
            .as_ref()
            .is_some_and(|current| current == value);
        if same_group {
            current_count += 1;
            continue;
        }
        if current_count > best_count
            && let Some(current) = current_value.take()
        {
            best_value = current;
            best_count = current_count;
        }
        current_value = Some(value.to_owned_value());
        current_count = 1;
    }
    if current_count > best_count
        && let Some(current) = current_value
    {
        best_value = current;
    }
    Ok(best_value)
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
    pgrust_executor::finalize_regr_stats(
        func,
        count,
        sum_x,
        sum_sq_x,
        sum_y,
        sum_sq_y,
        sum_xy,
        all_x_equal,
        all_y_equal,
    )
}

fn stable_regr_semidefinite_sum(sum_sq: f64, sum: f64, count: f64) -> f64 {
    pgrust_executor::stable_regr_semidefinite_sum(sum_sq, sum, count)
}

fn clamp_corr(value: f64) -> f64 {
    pgrust_executor::clamp_corr(value)
}

fn clamp_regr_r2(value: f64) -> f64 {
    pgrust_executor::clamp_regr_r2(value)
}

fn float8_regr_constant_value_eq(value: f64, first: f64) -> bool {
    !value.is_nan() && !first.is_nan() && value == first
}

fn regr_value_or_null(count: f64, value: f64) -> Value {
    pgrust_executor::regr_value_or_null(count, value)
}

fn string_agg_input_bytes(value: &Value, bytea: bool) -> Vec<u8> {
    pgrust_executor::string_agg_input_bytes(value, bytea)
}

fn validate_array_agg_array_input(
    value: &Value,
    inner_dims: &mut Option<Vec<ArrayDimension>>,
) -> Result<(), ExecError> {
    pgrust_executor::validate_array_agg_array_input(value, inner_dims)
        .map_err(super::expr_agg_support::aggregate_support_error)
}

fn finalize_array_agg(values: &[Value]) -> Value {
    pgrust_executor::finalize_array_agg(values)
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    pgrust_executor::normalize_aggregate_array_value(value)
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
        Value::Tid(v) => crate::backend::executor::value_io::render_tid_text(v),
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
            pgrust_expr::render_geometry_text(key, Default::default()).unwrap_or_default()
        }
        Value::Range(_) => render_range_text(key).unwrap_or_default(),
        Value::Multirange(_) => {
            crate::backend::executor::render_multirange_text(key).unwrap_or_default()
        }
        Value::TsVector(v) => crate::backend::executor::render_tsvector_text(v),
        Value::TsQuery(v) => crate::backend::executor::render_tsquery_text(v),
        Value::Array(_) | Value::PgArray(_) | Value::Record(_) => value_to_json_text(key),
        Value::IndirectVarlena(indirect) => {
            crate::backend::executor::value_io::indirect_varlena_to_value(indirect)
                .map(|decoded| json_object_agg_key(&decoded))
                .unwrap_or_else(|_| "null".to_string())
        }
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => "null".to_string(),
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
        Value::Tid(v) => {
            serde_json::to_string(&crate::backend::executor::value_io::render_tid_text(v)).unwrap()
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
            &pgrust_expr::render_geometry_text(value, Default::default()).unwrap_or_default(),
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
        Value::IndirectVarlena(indirect) => {
            crate::backend::executor::value_io::indirect_varlena_to_value(indirect)
                .map(|decoded| value_to_json_text(&decoded))
                .unwrap_or_else(|_| "null".into())
        }
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => "null".into(),
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

fn float8_overflow_error() -> ExecError {
    ExecError::DetailedError {
        message: "value out of range: overflow".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AggGroup {
    pub(crate) key_values: Vec<Value>,
    pub(crate) passthrough_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
    pub(crate) custom_combine_states: Vec<Option<AccumState>>,
    pub(crate) custom_combine_state_has_rows: Vec<bool>,
    pub(crate) input_row_index: usize,
    pub(crate) distinct_inputs: Vec<Option<Vec<Vec<Value>>>>,
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
