use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use pgrust_catalog_data::{
    FLOAT8_TYPE_OID, HYPOTHETICAL_RANK_FINAL_PROC_OID, MODE_FINAL_PROC_OID,
    PERCENTILE_CONT_FLOAT8_FINAL_PROC_OID, PERCENTILE_CONT_FLOAT8_MULTI_FINAL_PROC_OID,
    PERCENTILE_CONT_INTERVAL_FINAL_PROC_OID, PERCENTILE_CONT_INTERVAL_MULTI_FINAL_PROC_OID,
    PERCENTILE_DISC_FINAL_PROC_OID, PERCENTILE_DISC_MULTI_FINAL_PROC_OID,
    aggregate_func_for_dynamic_range_proc_oid, builtin_aggregate_function_for_proc_oid,
    builtin_hypothetical_aggregate_function_for_proc_oid,
    builtin_ordered_set_aggregate_function_for_proc_oid,
};
use pgrust_expr::parse_numeric_text;
use pgrust_nodes::datum::{ArrayDimension, ArrayValue};
use pgrust_nodes::datum::{IntervalValue, NumericValue};
use pgrust_nodes::parsenodes::SqlTypeKind;
use pgrust_nodes::primnodes::{AggFunc, HypotheticalAggFunc, OrderedSetAggFunc};
use pgrust_nodes::{SqlType, Value};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateSupportError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    InvalidInt8PairState {
        func: &'static str,
        state: Value,
    },
    CannotAccumulateNullArrays,
    CannotAccumulateEmptyArrays,
    ArrayDimensionalityMismatch,
    InvalidFloat8TransitionCall {
        expected: &'static str,
        actual_args: usize,
    },
    InvalidFloat8TransitionState {
        op: &'static str,
        expected_len: usize,
    },
    Float8Overflow,
}

#[derive(Debug, Clone)]
pub struct CustomAggregateRuntime {
    pub transfn_oid: u32,
    pub transfn_strict: bool,
    pub combinefn_oid: Option<u32>,
    pub combinefn_strict: bool,
    pub finalfn_oid: Option<u32>,
    pub finalfn_strict: bool,
    pub mtransfn_oid: Option<u32>,
    pub mtransfn_strict: bool,
    pub minvtransfn_oid: Option<u32>,
    pub minvtransfn_strict: bool,
    pub mfinalfn_oid: Option<u32>,
    pub mfinalfn_strict: bool,
    pub transtype: SqlType,
    pub mtranstype: SqlType,
    pub transfn_arg_types: Vec<SqlType>,
    pub combinefn_arg_types: Vec<SqlType>,
    pub finalfn_arg_types: Vec<SqlType>,
    pub mtransfn_arg_types: Vec<SqlType>,
    pub minvtransfn_arg_types: Vec<SqlType>,
    pub mfinalfn_arg_types: Vec<SqlType>,
    pub init_value: Option<Value>,
    pub minit_value: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum NumericAccum {
    Int(i64),
    Float(f64),
    Numeric(NumericValue),
    NumericSum(NumericSumAccum),
    Interval(IntervalValue),
}

#[derive(Debug, Clone)]
pub enum NumericSumAccum {
    Finite {
        coeff: BigInt,
        scale: u32,
        dscale: u32,
    },
    Special(NumericValue),
}

impl NumericSumAccum {
    pub fn new(value: &NumericValue) -> Self {
        match value {
            NumericValue::Finite {
                coeff,
                scale,
                dscale,
            } => NumericSumAccum::Finite {
                coeff: coeff.clone(),
                scale: *scale,
                dscale: *dscale,
            },
            other => NumericSumAccum::Special(other.clone()),
        }
    }

    pub fn add_numeric(&mut self, value: &NumericValue) {
        match (self, value) {
            (
                NumericSumAccum::Finite {
                    coeff,
                    scale,
                    dscale,
                },
                NumericValue::Finite {
                    coeff: rhs,
                    scale: rhs_scale,
                    dscale: rhs_dscale,
                },
            ) if scale == rhs_scale => {
                *coeff += rhs;
                *dscale = (*dscale).max(*rhs_dscale);
            }
            (accum, value) => {
                let sum = accum.to_numeric().add(value);
                *accum = NumericSumAccum::new(&sum);
            }
        }
    }

    pub fn to_numeric(&self) -> NumericValue {
        match self {
            NumericSumAccum::Finite {
                coeff,
                scale,
                dscale,
            } => NumericValue::Finite {
                coeff: coeff.clone(),
                scale: *scale,
                dscale: *dscale,
            }
            .normalize(),
            NumericSumAccum::Special(value) => value.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateRuntimeSelection {
    Builtin(AggFunc),
    Hypothetical(HypotheticalAggFunc),
    OrderedSet(OrderedSetAggFunc),
    PlainCustom,
    UnsupportedKind(char),
}

pub fn aggregate_runtime_selection(
    aggfnoid: u32,
    aggregate_kind: Option<char>,
    final_proc_oid: Option<u32>,
) -> AggregateRuntimeSelection {
    if let Some(func) = builtin_aggregate_function_for_proc_oid(aggfnoid) {
        return AggregateRuntimeSelection::Builtin(func);
    }
    if let Some(func) = aggregate_func_for_dynamic_range_proc_oid(aggfnoid) {
        return AggregateRuntimeSelection::Builtin(func);
    }
    if let Some(func) = builtin_hypothetical_aggregate_function_for_proc_oid(aggfnoid) {
        return AggregateRuntimeSelection::Hypothetical(func);
    }
    if let Some(func) = builtin_ordered_set_aggregate_function_for_proc_oid(aggfnoid) {
        return AggregateRuntimeSelection::OrderedSet(func);
    }
    if aggregate_kind == Some('o')
        && let Some(func) =
            final_proc_oid.and_then(ordered_set_aggregate_function_for_final_proc_oid)
    {
        return AggregateRuntimeSelection::OrderedSet(func);
    }
    if aggregate_kind == Some('h')
        && let Some(func) =
            final_proc_oid.and_then(hypothetical_aggregate_function_for_final_proc_oid)
    {
        return AggregateRuntimeSelection::Hypothetical(func);
    }
    match aggregate_kind {
        None | Some('n') => AggregateRuntimeSelection::PlainCustom,
        Some(kind) => AggregateRuntimeSelection::UnsupportedKind(kind),
    }
}

pub fn concrete_custom_aggregate_transtype(
    declared_transtype: SqlType,
    input_arg_types: &[SqlType],
) -> SqlType {
    match declared_transtype.kind {
        SqlTypeKind::AnyArray | SqlTypeKind::AnyCompatibleArray => input_arg_types
            .iter()
            .copied()
            .find(|ty| ty.is_array)
            .or_else(|| input_arg_types.first().copied().map(SqlType::array_of))
            .unwrap_or(declared_transtype),
        SqlTypeKind::AnyElement | SqlTypeKind::AnyCompatible => input_arg_types
            .iter()
            .copied()
            .find(|ty| ty.is_array)
            .map(SqlType::element_type)
            .or_else(|| input_arg_types.first().copied())
            .unwrap_or(declared_transtype),
        _ => declared_transtype,
    }
}

pub fn aggregate_int8_pair(
    value: &Value,
    func: &'static str,
) -> Result<(i64, i64), AggregateSupportError> {
    let elements = match value {
        Value::PgArray(array) => &array.elements,
        Value::Array(elements) => elements,
        other => {
            return Err(AggregateSupportError::TypeMismatch {
                op: func,
                left: other.clone(),
                right: Value::PgArray(ArrayValue::from_1d(vec![Value::Int64(0), Value::Int64(0)])),
            });
        }
    };
    match elements.as_slice() {
        [Value::Int64(count), Value::Int64(sum)] => Ok((*count, *sum)),
        [Value::Int32(count), Value::Int32(sum)] => Ok((i64::from(*count), i64::from(*sum))),
        _ => Err(AggregateSupportError::InvalidInt8PairState {
            func,
            state: value.clone(),
        }),
    }
}

pub fn finalize_regr_stats(
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
        | AggFunc::ArrayAggArray
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg
        | AggFunc::JsonObjectAgg
        | AggFunc::JsonObjectAggUnique
        | AggFunc::JsonObjectAggUniqueStrict
        | AggFunc::JsonbObjectAgg
        | AggFunc::JsonbObjectAggUnique
        | AggFunc::JsonbObjectAggUniqueStrict
        | AggFunc::RangeAgg
        | AggFunc::XmlAgg
        | AggFunc::RangeIntersectAgg => unreachable!("non-regression aggregate"),
    }
}

pub fn stable_regr_semidefinite_sum(sum_sq: f64, sum: f64, count: f64) -> f64 {
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

pub fn clamp_corr(value: f64) -> f64 {
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

pub fn clamp_regr_r2(value: f64) -> f64 {
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

pub fn regr_value_or_null(count: f64, value: f64) -> Value {
    if count < 1.0 {
        Value::Null
    } else {
        Value::Float64(value)
    }
}

pub fn string_agg_input_bytes(value: &Value, bytea: bool) -> Vec<u8> {
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

pub fn validate_array_agg_array_input(
    value: &Value,
    inner_dims: &mut Option<Vec<ArrayDimension>>,
) -> Result<(), AggregateSupportError> {
    let Some(array) = normalize_array_value(value) else {
        return Err(AggregateSupportError::CannotAccumulateNullArrays);
    };
    if array.dimensions.is_empty() {
        return Err(AggregateSupportError::CannotAccumulateEmptyArrays);
    }
    match inner_dims {
        None => *inner_dims = Some(array.dimensions),
        Some(existing) if existing.as_slice() != array.dimensions.as_slice() => {
            return Err(AggregateSupportError::ArrayDimensionalityMismatch);
        }
        Some(_) => {}
    }
    Ok(())
}

pub fn finalize_array_agg(values: &[Value]) -> Value {
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

pub fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

pub fn accumulate_value(
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
            Some(NumericAccum::NumericSum(mut cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                cur.add_numeric(&rhs);
                NumericAccum::NumericSum(cur)
            }
            Some(NumericAccum::Numeric(cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                NumericAccum::Numeric(cur.add(&rhs))
            }
            Some(NumericAccum::Int(cur)) => NumericAccum::Float(cur as f64 + *v),
            Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + *v),
            Some(NumericAccum::Interval(_)) | None => {
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
                Some(NumericAccum::NumericSum(mut cur)) => {
                    cur.add_numeric(&parsed);
                    NumericAccum::NumericSum(cur)
                }
                Some(NumericAccum::Numeric(cur)) => NumericAccum::Numeric(cur.add(&parsed)),
                Some(NumericAccum::Int(cur)) => {
                    NumericAccum::Numeric(NumericValue::from_i64(cur).add(&parsed))
                }
                Some(NumericAccum::Float(cur)) => {
                    let left =
                        parse_numeric_text(&cur.to_string()).unwrap_or_else(NumericValue::zero);
                    NumericAccum::Numeric(left.add(&parsed))
                }
                Some(NumericAccum::Interval(_)) | None => NumericAccum::Numeric(parsed),
            })
        }
        _ => sum,
    }
}

pub fn numeric_accum_to_value(sum: Option<&NumericAccum>, result_type: SqlType) -> Value {
    match sum {
        Some(NumericAccum::Int(value)) if matches!(result_type.kind, SqlTypeKind::Numeric) => {
            Value::Numeric(NumericValue::from_i64(*value))
        }
        Some(NumericAccum::Int(value)) => Value::Int64(*value),
        Some(NumericAccum::Float(value)) => Value::Float64(*value),
        Some(NumericAccum::Numeric(value)) => Value::Numeric(value.clone()),
        Some(NumericAccum::NumericSum(value)) => Value::Numeric(value.to_numeric()),
        Some(NumericAccum::Interval(value)) => Value::Interval(*value),
        None => Value::Null,
    }
}

pub fn accumulate_sum_value(
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
            Some(NumericAccum::NumericSum(mut cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                cur.add_numeric(&rhs);
                NumericAccum::NumericSum(cur)
            }
            Some(NumericAccum::Numeric(cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                NumericAccum::Numeric(cur.add(&rhs))
            }
            Some(NumericAccum::Int(cur)) => NumericAccum::Float(cur as f64 + *v),
            Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + *v),
            Some(NumericAccum::Interval(_)) | None => {
                if matches!(result_type.kind, SqlTypeKind::Numeric) {
                    let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                    NumericAccum::NumericSum(NumericSumAccum::new(&rhs))
                } else {
                    NumericAccum::Float(*v)
                }
            }
        }),
        Value::Numeric(v) => Some(match sum {
            Some(NumericAccum::NumericSum(mut cur)) => {
                cur.add_numeric(v);
                NumericAccum::NumericSum(cur)
            }
            Some(NumericAccum::Numeric(cur)) => {
                let mut accum = NumericSumAccum::new(&cur);
                accum.add_numeric(v);
                NumericAccum::NumericSum(accum)
            }
            Some(NumericAccum::Int(cur)) => {
                let mut accum = NumericSumAccum::new(&NumericValue::from_i64(cur));
                accum.add_numeric(v);
                NumericAccum::NumericSum(accum)
            }
            Some(NumericAccum::Float(cur)) => {
                let left = parse_numeric_text(&cur.to_string()).unwrap_or_else(NumericValue::zero);
                let mut accum = NumericSumAccum::new(&left);
                accum.add_numeric(v);
                NumericAccum::NumericSum(accum)
            }
            Some(NumericAccum::Interval(_)) | None => {
                NumericAccum::NumericSum(NumericSumAccum::new(v))
            }
        }),
        _ => sum,
    }
}

pub fn aggregate_numeric_value(value: &Value) -> Option<NumericValue> {
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

pub fn aggregate_float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}

pub fn eval_float8_accum_function(values: &[Value]) -> Result<Value, AggregateSupportError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [state, newval] => {
            let state = expect_float8_transition_state("float8_accum", state, 3)?;
            let newval = expect_float8_arg("float8_accum", newval)?;
            let [count, sum, sum_sq] = float8_accum_state(state[0], state[1], state[2], newval)?;
            Ok(encode_float8_transition_state([count, sum, sum_sq]))
        }
        _ => Err(AggregateSupportError::InvalidFloat8TransitionCall {
            expected: "float8_accum(state, value)",
            actual_args: values.len(),
        }),
    }
}

pub fn eval_float8_combine_function(values: &[Value]) -> Result<Value, AggregateSupportError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left = expect_float8_transition_state("float8_combine", left, 3)?;
            let right = expect_float8_transition_state("float8_combine", right, 3)?;
            let [count, sum, sum_sq] =
                float8_combine_state(left[0], left[1], left[2], right[0], right[1], right[2])?;
            Ok(encode_float8_transition_state([count, sum, sum_sq]))
        }
        _ => Err(AggregateSupportError::InvalidFloat8TransitionCall {
            expected: "float8_combine(state1, state2)",
            actual_args: values.len(),
        }),
    }
}

pub fn eval_float8_regr_accum_function(values: &[Value]) -> Result<Value, AggregateSupportError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [state, y, x] => {
            let state = expect_float8_transition_state("float8_regr_accum", state, 6)?;
            let y = expect_float8_arg("float8_regr_accum", y)?;
            let x = expect_float8_arg("float8_regr_accum", x)?;
            let [count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy] = float8_regr_accum_state(
                state[0], state[1], state[2], state[3], state[4], state[5], y, x,
            )?;
            Ok(encode_float8_transition_state([
                count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy,
            ]))
        }
        _ => Err(AggregateSupportError::InvalidFloat8TransitionCall {
            expected: "float8_regr_accum(state, y, x)",
            actual_args: values.len(),
        }),
    }
}

pub fn eval_float8_regr_combine_function(values: &[Value]) -> Result<Value, AggregateSupportError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left = expect_float8_transition_state("float8_regr_combine", left, 6)?;
            let right = expect_float8_transition_state("float8_regr_combine", right, 6)?;
            let [count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy] = float8_regr_combine_state(
                [left[0], left[1], left[2], left[3], left[4], left[5]],
                [right[0], right[1], right[2], right[3], right[4], right[5]],
            )?;
            Ok(encode_float8_transition_state([
                count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy,
            ]))
        }
        _ => Err(AggregateSupportError::InvalidFloat8TransitionCall {
            expected: "float8_regr_combine(state1, state2)",
            actual_args: values.len(),
        }),
    }
}

fn expect_float8_transition_state(
    op: &'static str,
    value: &Value,
    expected_len: usize,
) -> Result<Vec<f64>, AggregateSupportError> {
    let array = value
        .as_array_value()
        .ok_or_else(|| AggregateSupportError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::PgArray(ArrayValue::empty().with_element_type_oid(FLOAT8_TYPE_OID)),
        })?;
    if array.dimensions.len() != 1 || array.dimensions[0].length != expected_len {
        return Err(AggregateSupportError::InvalidFloat8TransitionState { op, expected_len });
    }
    array
        .elements
        .iter()
        .map(|element| expect_float8_arg(op, element))
        .collect()
}

pub fn expect_float8_arg(op: &'static str, value: &Value) -> Result<f64, AggregateSupportError> {
    match value {
        Value::Int16(v) => Ok(f64::from(*v)),
        Value::Int32(v) => Ok(f64::from(*v)),
        Value::Int64(v) => Ok(*v as f64),
        Value::Float64(v) => Ok(*v),
        Value::Numeric(numeric) => match numeric {
            NumericValue::PosInf => Ok(f64::INFINITY),
            NumericValue::NegInf => Ok(f64::NEG_INFINITY),
            NumericValue::NaN => Ok(f64::NAN),
            NumericValue::Finite { coeff, scale, .. } => {
                let coeff = coeff
                    .to_f64()
                    .ok_or_else(|| AggregateSupportError::TypeMismatch {
                        op,
                        left: value.clone(),
                        right: Value::Float64(0.0),
                    })?;
                Ok(coeff / 10f64.powi(*scale as i32))
            }
        },
        _ => Err(AggregateSupportError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Float64(0.0),
        }),
    }
}

fn encode_float8_transition_state<const N: usize>(values: [f64; N]) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(values.into_iter().map(Value::Float64).collect())
            .with_element_type_oid(FLOAT8_TYPE_OID),
    )
}

fn float8_accum_state(
    prev_count: f64,
    prev_sum: f64,
    mut prev_sum_sq: f64,
    newval: f64,
) -> Result<[f64; 3], AggregateSupportError> {
    let count = prev_count + 1.0;
    let sum = prev_sum + newval;
    if prev_count > 0.0 {
        let tmp = newval * count - sum;
        prev_sum_sq += tmp * tmp / (count * prev_count);
        if sum.is_infinite() || prev_sum_sq.is_infinite() {
            if !prev_sum.is_infinite() && !newval.is_infinite() {
                return Err(AggregateSupportError::Float8Overflow);
            }
            prev_sum_sq = f64::NAN;
        }
    } else if newval.is_nan() || newval.is_infinite() {
        prev_sum_sq = f64::NAN;
    }
    Ok([count, sum, prev_sum_sq])
}

fn float8_combine_state(
    count1: f64,
    sum1: f64,
    sum_sq1: f64,
    count2: f64,
    sum2: f64,
    sum_sq2: f64,
) -> Result<[f64; 3], AggregateSupportError> {
    if count1 == 0.0 {
        return Ok([count2, sum2, sum_sq2]);
    }
    if count2 == 0.0 {
        return Ok([count1, sum1, sum_sq1]);
    }
    let count = count1 + count2;
    let sum = sum1 + sum2;
    let tmp = sum1 / count1 - sum2 / count2;
    let sum_sq = sum_sq1 + sum_sq2 + count1 * count2 * tmp * tmp / count;
    if sum_sq.is_infinite() && !sum_sq1.is_infinite() && !sum_sq2.is_infinite() {
        return Err(AggregateSupportError::Float8Overflow);
    }
    Ok([count, sum, sum_sq])
}

pub fn float8_regr_accum_state(
    prev_count: f64,
    prev_sum_x: f64,
    mut prev_sum_sq_x: f64,
    prev_sum_y: f64,
    mut prev_sum_sq_y: f64,
    mut prev_sum_xy: f64,
    new_y: f64,
    new_x: f64,
) -> Result<[f64; 6], AggregateSupportError> {
    let count = prev_count + 1.0;
    let sum_x = prev_sum_x + new_x;
    let sum_y = prev_sum_y + new_y;
    if prev_count > 0.0 {
        let tmp_x = new_x * count - sum_x;
        let tmp_y = new_y * count - sum_y;
        let scale = 1.0 / (count * prev_count);
        prev_sum_sq_x += tmp_x * tmp_x * scale;
        prev_sum_sq_y += tmp_y * tmp_y * scale;
        prev_sum_xy += tmp_x * tmp_y * scale;
        if sum_x.is_infinite()
            || prev_sum_sq_x.is_infinite()
            || sum_y.is_infinite()
            || prev_sum_sq_y.is_infinite()
            || prev_sum_xy.is_infinite()
        {
            if ((sum_x.is_infinite() || prev_sum_sq_x.is_infinite())
                && !prev_sum_x.is_infinite()
                && !new_x.is_infinite())
                || ((sum_y.is_infinite() || prev_sum_sq_y.is_infinite())
                    && !prev_sum_y.is_infinite()
                    && !new_y.is_infinite())
                || (prev_sum_xy.is_infinite()
                    && !prev_sum_x.is_infinite()
                    && !new_x.is_infinite()
                    && !prev_sum_y.is_infinite()
                    && !new_y.is_infinite())
            {
                return Err(AggregateSupportError::Float8Overflow);
            }
            if prev_sum_sq_x.is_infinite() {
                prev_sum_sq_x = f64::NAN;
            }
            if prev_sum_sq_y.is_infinite() {
                prev_sum_sq_y = f64::NAN;
            }
            if prev_sum_xy.is_infinite() {
                prev_sum_xy = f64::NAN;
            }
        }
    } else {
        if new_x.is_nan() || new_x.is_infinite() {
            prev_sum_sq_x = f64::NAN;
            prev_sum_xy = f64::NAN;
        }
        if new_y.is_nan() || new_y.is_infinite() {
            prev_sum_sq_y = f64::NAN;
            prev_sum_xy = f64::NAN;
        }
    }
    Ok([
        count,
        sum_x,
        prev_sum_sq_x,
        sum_y,
        prev_sum_sq_y,
        prev_sum_xy,
    ])
}

fn float8_regr_combine_state(
    left: [f64; 6],
    right: [f64; 6],
) -> Result<[f64; 6], AggregateSupportError> {
    let [count1, sum_x1, sum_sq_x1, sum_y1, sum_sq_y1, sum_xy1] = left;
    let [count2, sum_x2, sum_sq_x2, sum_y2, sum_sq_y2, sum_xy2] = right;
    if count1 == 0.0 {
        return Ok(right);
    }
    if count2 == 0.0 {
        return Ok(left);
    }
    let count = count1 + count2;
    let sum_x = sum_x1 + sum_x2;
    let sum_y = sum_y1 + sum_y2;
    let tmp_x = sum_x1 / count1 - sum_x2 / count2;
    let tmp_y = sum_y1 / count1 - sum_y2 / count2;
    let sum_sq_x = sum_sq_x1 + sum_sq_x2 + count1 * count2 * tmp_x * tmp_x / count;
    let sum_sq_y = sum_sq_y1 + sum_sq_y2 + count1 * count2 * tmp_y * tmp_y / count;
    let sum_xy = sum_xy1 + sum_xy2 + count1 * count2 * tmp_x * tmp_y / count;
    if (sum_sq_x.is_infinite() && !sum_sq_x1.is_infinite() && !sum_sq_x2.is_infinite())
        || (sum_sq_y.is_infinite() && !sum_sq_y1.is_infinite() && !sum_sq_y2.is_infinite())
        || (sum_xy.is_infinite() && !sum_xy1.is_infinite() && !sum_xy2.is_infinite())
    {
        return Err(AggregateSupportError::Float8Overflow);
    }
    Ok([count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy])
}

pub fn numeric_sqrt(value: &NumericValue, scale: u32) -> NumericValue {
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

pub fn numeric_visible_scale(value: &NumericValue) -> u32 {
    value
        .render()
        .split_once('.')
        .map(|(_, frac)| frac.len() as u32)
        .unwrap_or(0)
}

pub fn numeric_quotient_decimal_weight(lhs: &NumericValue, rhs: &NumericValue) -> i32 {
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
        Some(NumericAccum::NumericSum(mut cur)) => {
            cur.add_numeric(&NumericValue::from_i64(value));
            NumericAccum::NumericSum(cur)
        }
        Some(NumericAccum::Numeric(cur)) => {
            NumericAccum::Numeric(cur.add(&NumericValue::from_i64(value)))
        }
        Some(NumericAccum::Int(cur)) => NumericAccum::Int(cur + value),
        Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + value as f64),
        Some(NumericAccum::Interval(_)) | None => {
            if matches!(result_type.kind, SqlTypeKind::Numeric) {
                NumericAccum::Numeric(NumericValue::from_i64(value))
            } else {
                NumericAccum::Int(value)
            }
        }
    }
}

pub fn format_numeric_result(value: NumericValue, sql_type: SqlType) -> NumericValue {
    if let Some((_, scale)) = sql_type.numeric_precision_scale() {
        value.round_to_scale(scale as u32).unwrap_or(value)
    } else {
        value
    }
}

pub fn floor_div_i32(value: i32, divisor: i32) -> i32 {
    if value >= 0 {
        value / divisor
    } else {
        -(((-value) + divisor - 1) / divisor)
    }
}

pub fn numeric_div_display_scale(lhs: &NumericValue, rhs: &NumericValue) -> u32 {
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

fn ordered_set_aggregate_function_for_final_proc_oid(proc_oid: u32) -> Option<OrderedSetAggFunc> {
    match proc_oid {
        PERCENTILE_DISC_FINAL_PROC_OID => Some(OrderedSetAggFunc::PercentileDisc),
        PERCENTILE_DISC_MULTI_FINAL_PROC_OID => Some(OrderedSetAggFunc::PercentileDiscMulti),
        PERCENTILE_CONT_FLOAT8_FINAL_PROC_OID | PERCENTILE_CONT_INTERVAL_FINAL_PROC_OID => {
            Some(OrderedSetAggFunc::PercentileCont)
        }
        PERCENTILE_CONT_FLOAT8_MULTI_FINAL_PROC_OID
        | PERCENTILE_CONT_INTERVAL_MULTI_FINAL_PROC_OID => {
            Some(OrderedSetAggFunc::PercentileContMulti)
        }
        MODE_FINAL_PROC_OID => Some(OrderedSetAggFunc::Mode),
        _ => None,
    }
}

fn hypothetical_aggregate_function_for_final_proc_oid(
    proc_oid: u32,
) -> Option<HypotheticalAggFunc> {
    match proc_oid {
        HYPOTHETICAL_RANK_FINAL_PROC_OID => Some(HypotheticalAggFunc::Rank),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_range_aggregate_oids_use_builtin_runtime_selection() {
        let type_rows = pgrust_catalog_data::builtin_type_rows();
        let range_rows = pgrust_catalog_data::builtin_range_rows();

        let range_agg =
            pgrust_catalog_data::synthetic_range_aggregate_rows(&type_rows, &range_rows)
                .into_iter()
                .find(|row| {
                    aggregate_func_for_dynamic_range_proc_oid(row.aggfnoid)
                        == Some(AggFunc::RangeAgg)
                })
                .expect("dynamic range_agg aggregate");
        assert_eq!(
            aggregate_runtime_selection(range_agg.aggfnoid, None, None),
            AggregateRuntimeSelection::Builtin(AggFunc::RangeAgg)
        );

        let range_intersect_agg =
            pgrust_catalog_data::synthetic_range_aggregate_rows(&type_rows, &range_rows)
                .into_iter()
                .find(|row| {
                    aggregate_func_for_dynamic_range_proc_oid(row.aggfnoid)
                        == Some(AggFunc::RangeIntersectAgg)
                })
                .expect("dynamic range_intersect_agg aggregate");
        assert_eq!(
            aggregate_runtime_selection(range_intersect_agg.aggfnoid, None, None),
            AggregateRuntimeSelection::Builtin(AggFunc::RangeIntersectAgg)
        );
    }

    #[test]
    fn aggregate_int8_pair_accepts_int8_and_int4_state_arrays() {
        assert_eq!(
            aggregate_int8_pair(
                &Value::PgArray(ArrayValue::from_1d(vec![Value::Int64(2), Value::Int64(10)])),
                "int8_avg",
            )
            .unwrap(),
            (2, 10)
        );
        assert_eq!(
            aggregate_int8_pair(
                &Value::Array(vec![Value::Int32(2), Value::Int32(10)]),
                "int8_avg"
            )
            .unwrap(),
            (2, 10)
        );
    }

    #[test]
    fn aggregate_int8_pair_reports_shape_errors() {
        assert!(matches!(
            aggregate_int8_pair(&Value::Int32(1), "int8_avg"),
            Err(AggregateSupportError::TypeMismatch { op: "int8_avg", .. })
        ));
        assert!(matches!(
            aggregate_int8_pair(&Value::Array(vec![Value::Int64(1)]), "int8_avg"),
            Err(AggregateSupportError::InvalidInt8PairState {
                func: "int8_avg",
                ..
            })
        ));
    }

    #[test]
    fn regression_aggregate_helpers_clamp_near_boundary_values() {
        assert_eq!(stable_regr_semidefinite_sum(1.0e-30, 1.0, 10.0), 0.0);
        assert_eq!(clamp_corr(1.0 + f64::EPSILON), 1.0);
        assert_eq!(clamp_corr(-1.0 - f64::EPSILON), -1.0);
        assert_eq!(clamp_regr_r2(f64::EPSILON), 0.0);
        assert_eq!(regr_value_or_null(0.0, 10.0), Value::Null);
    }

    #[test]
    fn finalize_regr_stats_returns_postgres_shaped_values() {
        assert_eq!(
            finalize_regr_stats(
                AggFunc::RegrCount,
                2.0,
                3.0,
                5.0,
                7.0,
                11.0,
                13.0,
                false,
                false,
            ),
            Value::Int64(2)
        );
        assert_eq!(
            finalize_regr_stats(
                AggFunc::CovarSamp,
                1.0,
                3.0,
                5.0,
                7.0,
                11.0,
                13.0,
                false,
                false,
            ),
            Value::Null
        );
    }

    #[test]
    fn array_agg_helpers_validate_and_finalize_nested_arrays() {
        let array = Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }],
            vec![Value::Int32(1), Value::Int32(2)],
        ));
        let mut dims = None;
        validate_array_agg_array_input(&array, &mut dims).unwrap();
        assert_eq!(
            dims,
            Some(vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }])
        );
        let finalized = finalize_array_agg(&[array.clone(), array]);
        let Value::PgArray(finalized) = finalized else {
            panic!("array_agg should return pg array");
        };
        assert_eq!(
            finalized.dimensions,
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ]
        );
        assert_eq!(finalized.elements.len(), 4);
    }

    #[test]
    fn string_agg_bytes_follow_text_and_bytea_modes() {
        assert_eq!(
            string_agg_input_bytes(&Value::Text("abc".into()), false),
            b"abc"
        );
        assert_eq!(
            string_agg_input_bytes(&Value::Bytea(vec![1, 2, 3]), true),
            vec![1, 2, 3]
        );
        assert_eq!(
            string_agg_input_bytes(&Value::Null, false),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn numeric_accumulator_preserves_sum_semantics() {
        let numeric_type = SqlType::new(SqlTypeKind::Numeric);
        let sum = accumulate_sum_value(None, numeric_type, &Value::Int64(10));
        let sum = accumulate_sum_value(
            sum,
            numeric_type,
            &Value::Numeric(NumericValue::from_i64(5)),
        );

        assert_eq!(
            numeric_accum_to_value(sum.as_ref(), numeric_type),
            Value::Numeric(NumericValue::from_i64(15))
        );
    }

    #[test]
    fn numeric_helpers_compute_display_scale_and_sqrt() {
        let one = NumericValue::from_i64(1);
        let three = NumericValue::from_i64(3);
        assert_eq!(floor_div_i32(-1, 4), -1);
        assert!(numeric_div_display_scale(&one, &three) >= 16);
        assert_eq!(numeric_sqrt(&NumericValue::from_i64(4), 0).render(), "2");
    }
}
