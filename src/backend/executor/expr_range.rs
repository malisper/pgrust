use std::cmp::Ordering;

use super::ExecError;
use super::expr_casts::cast_value;
use super::expr_datetime::render_datetime_value_text;
use super::expr_ops::compare_order_values;
use super::node_types::{BuiltinScalarFunction, RangeBound, RangeTypeRef, RangeValue, Value};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    DATE_TYPE_OID, DATERANGE_TYPE_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID, INT8_TYPE_OID,
    INT8RANGE_TYPE_OID, NUMERIC_TYPE_OID, NUMRANGE_TYPE_OID, RangeCanonicalization,
    TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, TSRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
    builtin_range_name_for_sql_type, range_type_ref_for_sql_type,
};
use crate::include::nodes::datetime::DateADT;

const RANGE_EMPTY_FLAG: u8 = 0x01;
const RANGE_LOWER_INC_FLAG: u8 = 0x02;
const RANGE_UPPER_INC_FLAG: u8 = 0x04;
const RANGE_LOWER_PRESENT_FLAG: u8 = 0x08;
const RANGE_UPPER_PRESENT_FLAG: u8 = 0x10;

pub(crate) fn parse_range_text(text: &str, ty: SqlType) -> Result<Value, ExecError> {
    let Some(range_type) = range_type_ref_for_sql_type(ty) else {
        return Err(ExecError::TypeMismatch {
            op: "::range",
            left: Value::Text(text.into()),
            right: Value::Null,
        });
    };
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("empty") {
        return Ok(Value::Range(empty_range(range_type)));
    }
    if trimmed.len() < 2 {
        return Err(invalid_range_input(range_type, text));
    }
    let lower_ch = trimmed.as_bytes()[0] as char;
    let upper_ch = trimmed.as_bytes()[trimmed.len() - 1] as char;
    if !matches!(lower_ch, '[' | '(') || !matches!(upper_ch, ']' | ')') {
        return Err(invalid_range_input(range_type, text));
    }
    let (lower_raw, upper_raw) =
        split_range_body(&trimmed[1..trimmed.len() - 1], range_type, text)?;
    let lower = if lower_raw.is_empty() {
        None
    } else {
        Some(parse_range_bound_text(lower_raw, range_type.subtype)?)
    };
    let upper = if upper_raw.is_empty() {
        None
    } else {
        Some(parse_range_bound_text(upper_raw, range_type.subtype)?)
    };
    Ok(Value::Range(normalize_range(
        range_type,
        lower.map(|value| RangeBound {
            value: Box::new(value),
            inclusive: lower_ch == '[',
        }),
        upper.map(|value| RangeBound {
            value: Box::new(value),
            inclusive: upper_ch == ']',
        }),
    )?))
}

pub fn render_range_text(value: &Value) -> Option<String> {
    let Value::Range(range) = value else {
        return None;
    };
    Some(render_range_value(range))
}

pub(crate) fn render_range_value(range: &RangeValue) -> String {
    if range.empty {
        return "empty".to_string();
    }
    let mut out = String::new();
    out.push(
        if range.lower.as_ref().is_some_and(|bound| bound.inclusive) {
            '['
        } else {
            '('
        },
    );
    if let Some(lower) = &range.lower {
        out.push_str(&render_bound_text(lower.value.as_ref()));
    }
    out.push(',');
    if let Some(upper) = &range.upper {
        out.push_str(&render_bound_text(upper.value.as_ref()));
    }
    out.push(
        if range.upper.as_ref().is_some_and(|bound| bound.inclusive) {
            ']'
        } else {
            ')'
        },
    );
    out
}

pub(crate) fn compare_range_values(left: &RangeValue, right: &RangeValue) -> Ordering {
    match left.range_type.type_oid().cmp(&right.range_type.type_oid()) {
        Ordering::Equal => {}
        other => return other,
    }
    match (left.empty, right.empty) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }
    match compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref()) {
        Ordering::Equal => compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref()),
        other => other,
    }
}

pub(crate) fn encode_range_bytes(range: &RangeValue) -> Result<Vec<u8>, ExecError> {
    let mut flags = 0u8;
    if range.empty {
        flags |= RANGE_EMPTY_FLAG;
    }
    if range.lower.as_ref().is_some_and(|bound| bound.inclusive) {
        flags |= RANGE_LOWER_INC_FLAG;
    }
    if range.upper.as_ref().is_some_and(|bound| bound.inclusive) {
        flags |= RANGE_UPPER_INC_FLAG;
    }
    if range.lower.is_some() {
        flags |= RANGE_LOWER_PRESENT_FLAG;
    }
    if range.upper.is_some() {
        flags |= RANGE_UPPER_PRESENT_FLAG;
    }
    let mut out = vec![flags];
    if let Some(lower) = &range.lower {
        append_bound_bytes(&mut out, range.range_type, lower.value.as_ref())?;
    }
    if let Some(upper) = &range.upper {
        append_bound_bytes(&mut out, range.range_type, upper.value.as_ref())?;
    }
    Ok(out)
}

pub(crate) fn decode_range_bytes(
    range_type: RangeTypeRef,
    bytes: &[u8],
) -> Result<RangeValue, ExecError> {
    let Some((&flags, mut rest)) = bytes.split_first() else {
        return Err(ExecError::InvalidStorageValue {
            column: "<range>".into(),
            details: "range payload too short".into(),
        });
    };
    if flags & RANGE_EMPTY_FLAG != 0 {
        return Ok(empty_range(range_type));
    }
    let lower = if flags & RANGE_LOWER_PRESENT_FLAG != 0 {
        let (value, remaining) = take_bound_bytes(range_type, rest)?;
        rest = remaining;
        Some(RangeBound {
            value: Box::new(value),
            inclusive: flags & RANGE_LOWER_INC_FLAG != 0,
        })
    } else {
        None
    };
    let upper = if flags & RANGE_UPPER_PRESENT_FLAG != 0 {
        let (value, remaining) = take_bound_bytes(range_type, rest)?;
        rest = remaining;
        Some(RangeBound {
            value: Box::new(value),
            inclusive: flags & RANGE_UPPER_INC_FLAG != 0,
        })
    } else {
        None
    };
    if !rest.is_empty() {
        return Err(ExecError::InvalidStorageValue {
            column: "<range>".into(),
            details: "range payload has trailing bytes".into(),
        });
    }
    normalize_range(range_type, lower, upper)
}

pub(crate) fn eval_range_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
) -> Option<Result<Value, ExecError>> {
    use BuiltinScalarFunction::*;

    let result = match func {
        RangeConstructor => eval_range_constructor(values, result_type),
        RangeIsEmpty => unary_range_bool(values, "isempty", |range| Ok(Value::Bool(range.empty))),
        RangeLower => unary_range_value(values, "lower", range_lower_value),
        RangeUpper => unary_range_value(values, "upper", range_upper_value),
        RangeLowerInc => unary_range_bool(values, "lower_inc", |range| {
            Ok(Value::Bool(
                !range.empty && range.lower.as_ref().is_some_and(|bound| bound.inclusive),
            ))
        }),
        RangeUpperInc => unary_range_bool(values, "upper_inc", |range| {
            Ok(Value::Bool(
                !range.empty && range.upper.as_ref().is_some_and(|bound| bound.inclusive),
            ))
        }),
        RangeLowerInf => unary_range_bool(values, "lower_inf", |range| {
            Ok(Value::Bool(!range.empty && range.lower.is_none()))
        }),
        RangeUpperInf => unary_range_bool(values, "upper_inf", |range| {
            Ok(Value::Bool(!range.empty && range.upper.is_none()))
        }),
        RangeContains => eval_range_contains(values),
        RangeContainedBy => eval_range_contained_by(values),
        RangeOverlap => binary_range_bool(values, "&&", |left, right| {
            Ok(Value::Bool(range_overlap(left, right)))
        }),
        RangeStrictLeft => binary_range_bool(values, "<<", |left, right| {
            Ok(Value::Bool(range_strict_left(left, right)))
        }),
        RangeStrictRight => binary_range_bool(values, ">>", |left, right| {
            Ok(Value::Bool(range_strict_right(left, right)))
        }),
        RangeOverLeft => binary_range_bool(values, "&<", |left, right| {
            Ok(Value::Bool(
                compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref())
                    != Ordering::Greater,
            ))
        }),
        RangeOverRight => binary_range_bool(values, "&>", |left, right| {
            Ok(Value::Bool(
                compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref()) != Ordering::Less,
            ))
        }),
        RangeAdjacent => binary_range_bool(values, "-|-", |left, right| {
            Ok(Value::Bool(range_adjacent(left, right)))
        }),
        RangeUnion => binary_range_range(values, "+", range_union),
        RangeIntersect => binary_range_range(values, "*", |left, right| {
            Ok(range_intersection(left, right))
        }),
        RangeDifference => binary_range_range(values, "-", range_difference),
        RangeMerge => binary_range_range(values, "range_merge", |left, right| {
            Ok(range_merge(left, right))
        }),
        _ => return None,
    };
    Some(result)
}

pub(crate) fn range_intersection_agg_transition(
    current: Option<Value>,
    input: &Value,
) -> Result<Option<Value>, ExecError> {
    if matches!(input, Value::Null) {
        return Ok(current);
    }
    match current {
        None => Ok(Some(input.to_owned_value())),
        Some(existing) => match (&existing, input) {
            (Value::Range(left), Value::Range(right))
                if left.range_type.type_oid() == right.range_type.type_oid() =>
            {
                Ok(Some(Value::Range(range_intersection(left, right))))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "range_intersect_agg",
                left: existing,
                right: input.clone(),
            }),
        },
    }
}

fn eval_range_constructor(
    values: &[Value],
    result_type: Option<SqlType>,
) -> Result<Value, ExecError> {
    let range_type = if let Some(range_type) = result_type.and_then(range_type_ref_for_sql_type) {
        range_type
    } else {
        values
            .iter()
            .find_map(range_type_for_scalar_value)
            .ok_or_else(|| ExecError::DetailedError {
                message: "could not determine range type".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            })?
    };
    let (lower_inc, upper_inc) = match values {
        [_, _] => (true, false),
        [_, _, Value::Null] => {
            return Err(ExecError::DetailedError {
                message: "range constructor flags argument must not be null".into(),
                detail: None,
                hint: None,
                sqlstate: "22004",
            });
        }
        [_, _, flags] => parse_range_flags(flags)?,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "range constructor",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    let lower = values
        .first()
        .and_then(value_to_constructor_bound)
        .map(|value| RangeBound {
            value: Box::new(value),
            inclusive: lower_inc,
        });
    let upper = values
        .get(1)
        .and_then(value_to_constructor_bound)
        .map(|value| RangeBound {
            value: Box::new(value),
            inclusive: upper_inc,
        });
    Ok(Value::Range(normalize_range(range_type, lower, upper)?))
}

fn value_to_constructor_bound(value: &Value) -> Option<Value> {
    (!matches!(value, Value::Null)).then(|| value.to_owned_value())
}

fn range_lower_value(range: &RangeValue) -> Result<Value, ExecError> {
    Ok(match &range.lower {
        Some(bound) if !range.empty => bound.value.to_owned_value(),
        _ => Value::Null,
    })
}

fn range_upper_value(range: &RangeValue) -> Result<Value, ExecError> {
    Ok(match &range.upper {
        Some(bound) if !range.empty => bound.value.to_owned_value(),
        _ => Value::Null,
    })
}

fn eval_range_contains(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Range(left), Value::Range(right)] => {
            ensure_same_range_kind("@>", left, right)?;
            Ok(Value::Bool(range_contains_range(left, right)))
        }
        [Value::Range(range), value] => Ok(Value::Bool(range_contains_element(range, value)?)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "@>",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

fn eval_range_contained_by(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Range(left), Value::Range(right)] => {
            ensure_same_range_kind("<@", left, right)?;
            Ok(Value::Bool(range_contains_range(right, left)))
        }
        [value, Value::Range(range)] => Ok(Value::Bool(range_contains_element(range, value)?)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "<@",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

fn unary_range_bool(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&RangeValue) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [Value::Range(range)] => f(range),
        [value] => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
        _ => unreachable!(),
    }
}

fn unary_range_value(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&RangeValue) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    unary_range_bool(values, op, f)
}

fn binary_range_bool(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&RangeValue, &RangeValue) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Range(left), Value::Range(right)] => {
            ensure_same_range_kind(op, left, right)?;
            f(left, right)
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op,
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

fn binary_range_range(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&RangeValue, &RangeValue) -> Result<RangeValue, ExecError>,
) -> Result<Value, ExecError> {
    binary_range_bool(values, op, |left, right| Ok(Value::Range(f(left, right)?)))
}

pub(crate) fn normalize_range(
    range_type: RangeTypeRef,
    mut lower: Option<RangeBound>,
    mut upper: Option<RangeBound>,
) -> Result<RangeValue, ExecError> {
    if lower.is_none() {
        if let Some(bound) = &mut lower {
            bound.inclusive = false;
        }
    }
    if upper.is_none() {
        if let Some(bound) = &mut upper {
            bound.inclusive = false;
        }
    }
    if matches!(range_type.canonicalization, RangeCanonicalization::Discrete) {
        if let Some(bound) = &mut lower
            && !bound.inclusive
        {
            *bound.value = successor_value(range_type, bound.value.as_ref())?;
            bound.inclusive = true;
        }
        if let Some(bound) = &mut upper
            && bound.inclusive
        {
            *bound.value = successor_value(range_type, bound.value.as_ref())?;
            bound.inclusive = false;
        }
    }
    if let (Some(lower_bound), Some(upper_bound)) = (&lower, &upper) {
        match compare_scalar_values(lower_bound.value.as_ref(), upper_bound.value.as_ref()) {
            Ordering::Greater => return Err(range_bounds_error(range_type)),
            Ordering::Equal => {
                let non_empty = match range_type.canonicalization {
                    RangeCanonicalization::Discrete => false,
                    RangeCanonicalization::Continuous => {
                        lower_bound.inclusive && upper_bound.inclusive
                    }
                };
                if !non_empty {
                    return Ok(empty_range(range_type));
                }
            }
            Ordering::Less => {}
        }
    }
    Ok(RangeValue {
        range_type,
        empty: false,
        lower,
        upper,
    })
}

pub(crate) fn empty_range(range_type: RangeTypeRef) -> RangeValue {
    RangeValue {
        range_type,
        empty: true,
        lower: None,
        upper: None,
    }
}

pub(crate) fn range_contains_range(left: &RangeValue, right: &RangeValue) -> bool {
    if right.empty {
        return true;
    }
    if left.empty {
        return false;
    }
    compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref()) != Ordering::Greater
        && compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref()) != Ordering::Less
}

pub(crate) fn range_contains_element(range: &RangeValue, value: &Value) -> Result<bool, ExecError> {
    ensure_range_subtype(range, value)?;
    if range.empty {
        return Ok(false);
    }
    if let Some(lower) = &range.lower {
        match compare_scalar_values(value, lower.value.as_ref()) {
            Ordering::Less => return Ok(false),
            Ordering::Equal if !lower.inclusive => return Ok(false),
            _ => {}
        }
    }
    if let Some(upper) = &range.upper {
        match compare_scalar_values(value, upper.value.as_ref()) {
            Ordering::Greater => return Ok(false),
            Ordering::Equal if !upper.inclusive => return Ok(false),
            _ => {}
        }
    }
    Ok(true)
}

pub(crate) fn range_overlap(left: &RangeValue, right: &RangeValue) -> bool {
    if left.empty || right.empty {
        return false;
    }
    cmp_upper_to_lower(left.upper.as_ref(), right.lower.as_ref()) != Ordering::Less
        && cmp_upper_to_lower(right.upper.as_ref(), left.lower.as_ref()) != Ordering::Less
}

pub(crate) fn range_adjacent(left: &RangeValue, right: &RangeValue) -> bool {
    if left.empty || right.empty {
        return false;
    }
    bounds_adjacent(left.upper.as_ref(), right.lower.as_ref())
        || bounds_adjacent(right.upper.as_ref(), left.lower.as_ref())
}

pub(crate) fn range_strict_left(left: &RangeValue, right: &RangeValue) -> bool {
    !left.empty
        && !right.empty
        && cmp_upper_to_lower(left.upper.as_ref(), right.lower.as_ref()) == Ordering::Less
}

pub(crate) fn range_strict_right(left: &RangeValue, right: &RangeValue) -> bool {
    range_strict_left(right, left)
}

fn range_intersection(left: &RangeValue, right: &RangeValue) -> RangeValue {
    if !range_overlap(left, right) {
        return empty_range(left.range_type);
    }
    let lower = max_lower_bound(left.lower.as_ref(), right.lower.as_ref());
    let upper = min_upper_bound(left.upper.as_ref(), right.upper.as_ref());
    normalize_range(left.range_type, lower, upper).unwrap_or_else(|_| empty_range(left.range_type))
}

pub(crate) fn range_merge(left: &RangeValue, right: &RangeValue) -> RangeValue {
    if left.empty {
        return right.clone();
    }
    if right.empty {
        return left.clone();
    }
    RangeValue {
        range_type: left.range_type,
        empty: false,
        lower: min_lower_bound(left.lower.as_ref(), right.lower.as_ref()),
        upper: max_upper_bound(left.upper.as_ref(), right.upper.as_ref()),
    }
}

pub(crate) fn range_union(left: &RangeValue, right: &RangeValue) -> Result<RangeValue, ExecError> {
    if !range_overlap(left, right) && !range_adjacent(left, right) {
        return Err(ExecError::DetailedError {
            message: "result of range union would not be contiguous".into(),
            detail: None,
            hint: None,
            sqlstate: "22000",
        });
    }
    Ok(range_merge(left, right))
}

fn range_difference(left: &RangeValue, right: &RangeValue) -> Result<RangeValue, ExecError> {
    if left.empty || right.empty || !range_overlap(left, right) {
        return Ok(left.clone());
    }
    if range_contains_range(right, left) {
        return Ok(empty_range(left.range_type));
    }
    let left_piece =
        if compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref()) == Ordering::Less {
            Some(normalize_range(
                left.range_type,
                left.lower.clone(),
                right.lower.as_ref().map(toggle_lower_to_upper_bound),
            )?)
        } else {
            None
        };
    let right_piece =
        if compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref()) == Ordering::Greater {
            Some(normalize_range(
                left.range_type,
                right.upper.as_ref().map(toggle_upper_to_lower_bound),
                left.upper.clone(),
            )?)
        } else {
            None
        };
    let left_non_empty = left_piece.as_ref().is_some_and(|range| !range.empty);
    let right_non_empty = right_piece.as_ref().is_some_and(|range| !range.empty);
    if left_non_empty && right_non_empty {
        return Err(ExecError::DetailedError {
            message: "result of range difference would not be contiguous".into(),
            detail: None,
            hint: None,
            sqlstate: "22000",
        });
    }
    if let Some(range) = left_piece
        && !range.empty
    {
        return Ok(range);
    }
    if let Some(range) = right_piece
        && !range.empty
    {
        return Ok(range);
    }
    Ok(empty_range(left.range_type))
}

fn toggle_lower_to_upper_bound(bound: &RangeBound) -> RangeBound {
    RangeBound {
        value: bound.value.clone(),
        inclusive: !bound.inclusive,
    }
}

fn toggle_upper_to_lower_bound(bound: &RangeBound) -> RangeBound {
    RangeBound {
        value: bound.value.clone(),
        inclusive: !bound.inclusive,
    }
}

fn min_lower_bound(left: Option<&RangeBound>, right: Option<&RangeBound>) -> Option<RangeBound> {
    match compare_lower_bounds(left, right) {
        Ordering::Greater => right.cloned(),
        _ => left.cloned(),
    }
}

fn max_lower_bound(left: Option<&RangeBound>, right: Option<&RangeBound>) -> Option<RangeBound> {
    match compare_lower_bounds(left, right) {
        Ordering::Less => right.cloned(),
        _ => left.cloned(),
    }
}

fn min_upper_bound(left: Option<&RangeBound>, right: Option<&RangeBound>) -> Option<RangeBound> {
    match compare_upper_bounds(left, right) {
        Ordering::Greater => right.cloned(),
        _ => left.cloned(),
    }
}

fn max_upper_bound(left: Option<&RangeBound>, right: Option<&RangeBound>) -> Option<RangeBound> {
    match compare_upper_bounds(left, right) {
        Ordering::Less => right.cloned(),
        _ => left.cloned(),
    }
}

pub(crate) fn compare_lower_bounds(
    left: Option<&RangeBound>,
    right: Option<&RangeBound>,
) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => {
            match compare_scalar_values(left.value.as_ref(), right.value.as_ref()) {
                Ordering::Equal => match (left.inclusive, right.inclusive) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => Ordering::Equal,
                },
                other => other,
            }
        }
    }
}

pub(crate) fn compare_upper_bounds(
    left: Option<&RangeBound>,
    right: Option<&RangeBound>,
) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => {
            match compare_scalar_values(left.value.as_ref(), right.value.as_ref()) {
                Ordering::Equal => match (left.inclusive, right.inclusive) {
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => Ordering::Equal,
                },
                other => other,
            }
        }
    }
}

fn cmp_upper_to_lower(upper: Option<&RangeBound>, lower: Option<&RangeBound>) -> Ordering {
    match (upper, lower) {
        (None, _) => Ordering::Greater,
        (Some(_), None) => Ordering::Greater,
        (Some(upper), Some(lower)) => {
            match compare_scalar_values(upper.value.as_ref(), lower.value.as_ref()) {
                Ordering::Equal => {
                    if upper.inclusive && lower.inclusive {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                other => other,
            }
        }
    }
}

fn bounds_adjacent(upper: Option<&RangeBound>, lower: Option<&RangeBound>) -> bool {
    match (upper, lower) {
        (Some(upper), Some(lower))
            if compare_scalar_values(upper.value.as_ref(), lower.value.as_ref())
                == Ordering::Equal =>
        {
            upper.inclusive != lower.inclusive
        }
        _ => false,
    }
}

pub(crate) fn compare_scalar_values(left: &Value, right: &Value) -> Ordering {
    compare_order_values(left, right, Some(false), false)
}

fn ensure_same_range_kind(
    op: &'static str,
    left: &RangeValue,
    right: &RangeValue,
) -> Result<(), ExecError> {
    if left.range_type.type_oid() == right.range_type.type_oid() {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch {
            op,
            left: Value::Range(left.clone()),
            right: Value::Range(right.clone()),
        })
    }
}

fn ensure_range_subtype(range: &RangeValue, value: &Value) -> Result<(), ExecError> {
    let expected = range.range_type.subtype.element_type();
    let matches = value
        .sql_type_hint()
        .map(SqlType::element_type)
        .map(|actual| {
            if actual.type_oid != 0 && expected.type_oid != 0 {
                actual.type_oid == expected.type_oid
            } else {
                actual.kind == expected.kind && actual.is_array == expected.is_array
            }
        })
        .unwrap_or(false);
    if matches {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch {
            op: "range subtype",
            left: Value::Range(range.clone()),
            right: value.clone(),
        })
    }
}

fn range_type_for_scalar_value(value: &Value) -> Option<RangeTypeRef> {
    match value {
        Value::Range(range) => Some(range.range_type),
        Value::Int32(_) => {
            range_type_ref_for_sql_type(SqlType::range(INT4RANGE_TYPE_OID, INT4_TYPE_OID))
        }
        Value::Int64(_) => {
            range_type_ref_for_sql_type(SqlType::range(INT8RANGE_TYPE_OID, INT8_TYPE_OID))
        }
        Value::Numeric(_) => {
            range_type_ref_for_sql_type(SqlType::range(NUMRANGE_TYPE_OID, NUMERIC_TYPE_OID))
        }
        Value::Date(_) => {
            range_type_ref_for_sql_type(SqlType::range(DATERANGE_TYPE_OID, DATE_TYPE_OID))
        }
        Value::Timestamp(_) => {
            range_type_ref_for_sql_type(SqlType::range(TSRANGE_TYPE_OID, TIMESTAMP_TYPE_OID))
        }
        Value::TimestampTz(_) => {
            range_type_ref_for_sql_type(SqlType::range(TSTZRANGE_TYPE_OID, TIMESTAMPTZ_TYPE_OID))
        }
        _ => None,
    }
}

fn successor_value(range_type: RangeTypeRef, value: &Value) -> Result<Value, ExecError> {
    match (range_type.subtype.kind, value) {
        (SqlTypeKind::Int4, Value::Int32(v)) => v
            .checked_add(1)
            .map(Value::Int32)
            .ok_or_else(range_bound_overflow),
        (SqlTypeKind::Int8, Value::Int64(v)) => v
            .checked_add(1)
            .map(Value::Int64)
            .ok_or_else(range_bound_overflow),
        (SqlTypeKind::Date, Value::Date(v)) => {
            v.0.checked_add(1)
                .map(|days| Value::Date(DateADT(days)))
                .ok_or_else(range_bound_overflow)
        }
        _ => Err(ExecError::TypeMismatch {
            op: "range canonicalization",
            left: value.clone(),
            right: Value::Null,
        }),
    }
}

fn append_bound_bytes(
    out: &mut Vec<u8>,
    range_type: RangeTypeRef,
    value: &Value,
) -> Result<(), ExecError> {
    let bytes = encode_bound_value(range_type, value)?;
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&bytes);
    Ok(())
}

fn take_bound_bytes<'a>(
    range_type: RangeTypeRef,
    bytes: &'a [u8],
) -> Result<(Value, &'a [u8]), ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<range>".into(),
            details: "range bound length missing".into(),
        });
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + len {
        return Err(ExecError::InvalidStorageValue {
            column: "<range>".into(),
            details: "range bound payload truncated".into(),
        });
    }
    let value = decode_bound_value(range_type, &bytes[4..4 + len])?;
    Ok((value, &bytes[4 + len..]))
}

fn encode_bound_value(range_type: RangeTypeRef, value: &Value) -> Result<Vec<u8>, ExecError> {
    ensure_range_subtype(
        &RangeValue {
            range_type,
            empty: false,
            lower: None,
            upper: None,
        },
        value,
    )?;
    Ok(render_bound_text(value).into_bytes())
}

fn decode_bound_value(range_type: RangeTypeRef, bytes: &[u8]) -> Result<Value, ExecError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ExecError::InvalidStorageValue {
        column: "<range>".into(),
        details: "range bound is not utf8".into(),
    })?;
    cast_value(Value::Text(text.into()), range_type.subtype)
}

fn split_range_body<'a>(
    body: &'a str,
    range_type: RangeTypeRef,
    original: &str,
) -> Result<(&'a str, &'a str), ExecError> {
    let bytes = body.as_bytes();
    let mut idx = 0usize;
    let mut in_quotes = false;
    while idx < bytes.len() {
        match bytes[idx] as char {
            '\\' => idx += 2,
            '"' => {
                in_quotes = !in_quotes;
                idx += 1;
            }
            ',' if !in_quotes => return Ok((&body[..idx], &body[idx + 1..])),
            _ => idx += 1,
        }
    }
    Err(invalid_range_input(range_type, original))
}

fn parse_range_bound_text(text: &str, subtype: SqlType) -> Result<Value, ExecError> {
    let decoded = decode_range_bound_text(text);
    cast_value(Value::Text(decoded.into()), subtype)
}

fn decode_range_bound_text(text: &str) -> String {
    let trimmed = text.trim();
    if !trimmed.starts_with('"') {
        return trimmed.to_string();
    }
    let inner = &trimmed[1..trimmed.len().saturating_sub(1)];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn render_bound_text(value: &Value) -> String {
    let raw = match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Date(_) | Value::Timestamp(_) | Value::TimestampTz(_) => {
            render_datetime_value_text(value).unwrap_or_default()
        }
        Value::Bool(v) => v.to_string(),
        Value::Bit(bits) => bits.render(),
        other => other.as_text().unwrap_or_default().to_string(),
    };
    if needs_range_quotes(&raw) {
        let escaped = raw.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        raw
    }
}

fn needs_range_quotes(text: &str) -> bool {
    text.is_empty()
        || text.chars().any(|ch| {
            matches!(ch, '"' | '\\' | '[' | ']' | '(' | ')' | ',' | ' ') || ch.is_ascii_whitespace()
        })
}

fn parse_range_flags(value: &Value) -> Result<(bool, bool), ExecError> {
    let text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "range flags",
        left: value.clone(),
        right: Value::Text("".into()),
    })?;
    match text {
        "[)" => Ok((true, false)),
        "[]" => Ok((true, true)),
        "(]" => Ok((false, true)),
        "()" => Ok((false, false)),
        _ => Err(ExecError::DetailedError {
            message:
                "range constructor flags argument must be one of \"()\", \"(]\", \"[)\", or \"[]\""
                    .into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn invalid_range_input(range_type: RangeTypeRef, value: &str) -> ExecError {
    ExecError::InvalidRangeInput {
        ty: builtin_range_name_for_sql_type(range_type.sql_type).unwrap_or("range"),
        value: value.to_string(),
    }
}

fn range_bounds_error(_range_type: RangeTypeRef) -> ExecError {
    ExecError::DetailedError {
        message: "range lower bound must be less than or equal to range upper bound".into(),
        detail: None,
        hint: None,
        sqlstate: "22000",
    }
}

fn range_bound_overflow() -> ExecError {
    ExecError::DetailedError {
        message: "range bound value out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::datum::NumericValue;

    fn test_range_type(sql_type: SqlType) -> RangeTypeRef {
        range_type_ref_for_sql_type(sql_type).expect("range type")
    }

    #[test]
    fn int4_range_canonicalizes_closed_upper() {
        let range = normalize_range(
            test_range_type(SqlType::new(SqlTypeKind::Int4Range)),
            Some(RangeBound {
                value: Box::new(Value::Int32(1)),
                inclusive: true,
            }),
            Some(RangeBound {
                value: Box::new(Value::Int32(10)),
                inclusive: true,
            }),
        )
        .unwrap();
        assert_eq!(render_range_value(&range), "[1,11)");
    }

    #[test]
    fn numrange_equal_closed_bounds_is_non_empty() {
        let range = normalize_range(
            test_range_type(SqlType::new(SqlTypeKind::NumericRange)),
            Some(RangeBound {
                value: Box::new(Value::Numeric(NumericValue::from("1.7"))),
                inclusive: true,
            }),
            Some(RangeBound {
                value: Box::new(Value::Numeric(NumericValue::from("1.7"))),
                inclusive: true,
            }),
        )
        .unwrap();
        assert!(!range.empty);
    }

    #[test]
    fn numrange_equal_half_open_bounds_is_empty() {
        let range = normalize_range(
            test_range_type(SqlType::new(SqlTypeKind::NumericRange)),
            Some(RangeBound {
                value: Box::new(Value::Numeric(NumericValue::from("1.7"))),
                inclusive: true,
            }),
            Some(RangeBound {
                value: Box::new(Value::Numeric(NumericValue::from("1.7"))),
                inclusive: false,
            }),
        )
        .unwrap();
        assert!(range.empty);
    }

    #[test]
    fn parse_and_render_timestamp_range_quotes_bounds() {
        let value = parse_range_text(
            "[\"2000-01-01 00:00:00\",\"2000-01-02 00:00:00\")",
            SqlType::new(SqlTypeKind::TimestampRange),
        )
        .unwrap();
        assert_eq!(
            render_range_text(&value).unwrap(),
            "[\"2000-01-01 00:00:00\",\"2000-01-02 00:00:00\")"
        );
    }

    #[test]
    fn empty_range_sorts_before_non_empty() {
        let empty = empty_range(test_range_type(SqlType::new(SqlTypeKind::Int4Range)));
        let non_empty = normalize_range(
            test_range_type(SqlType::new(SqlTypeKind::Int4Range)),
            Some(RangeBound {
                value: Box::new(Value::Int32(1)),
                inclusive: true,
            }),
            Some(RangeBound {
                value: Box::new(Value::Int32(4)),
                inclusive: false,
            }),
        )
        .unwrap();
        assert_eq!(compare_range_values(&empty, &non_empty), Ordering::Less);
    }

    #[test]
    fn range_binary_storage_round_trips() {
        let range = normalize_range(
            test_range_type(SqlType::new(SqlTypeKind::Int4Range)),
            Some(RangeBound {
                value: Box::new(Value::Int32(1)),
                inclusive: true,
            }),
            Some(RangeBound {
                value: Box::new(Value::Int32(10)),
                inclusive: true,
            }),
        )
        .unwrap();
        let encoded = encode_range_bytes(&range).unwrap();
        let decoded = decode_range_bytes(
            test_range_type(SqlType::new(SqlTypeKind::Int4Range)),
            &encoded,
        )
        .unwrap();
        assert_eq!(decoded, range);
    }
}
