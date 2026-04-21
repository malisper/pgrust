use std::cmp::Ordering;

use super::ExecError;
use super::expr_range::{
    empty_range, normalize_range, parse_range_text, range_adjacent, range_contains_element,
    range_contains_range, range_difference_segments, range_intersection, range_merge,
    range_over_left_bounds, range_over_right_bounds, range_overlap, range_strict_left,
    range_strict_right, render_range_value,
};
use super::node_types::{
    BuiltinScalarFunction, MultirangeTypeRef, MultirangeValue, RangeValue, Value,
};
use crate::backend::parser::SqlType;
use crate::include::catalog::multirange_type_ref_for_sql_type;

pub(crate) fn parse_multirange_text(text: &str, ty: SqlType) -> Result<Value, ExecError> {
    let Some(multirange_type) = multirange_type_ref_for_sql_type(ty) else {
        return Err(ExecError::TypeMismatch {
            op: "::multirange",
            left: Value::Text(text.into()),
            right: Value::Null,
        });
    };
    let mut idx = 0usize;
    skip_ascii_whitespace(text, &mut idx);
    if !matches!(char_at(text, idx), Some('{')) {
        return Err(malformed_multirange_literal(text, "Missing left brace."));
    };
    idx += 1;
    enum ParseState {
        BeforeRange,
        AfterRange,
    }
    let mut state = ParseState::BeforeRange;
    let mut saw_range = false;
    let mut ranges = Vec::new();
    loop {
        skip_ascii_whitespace(text, &mut idx);
        let Some(ch) = char_at(text, idx) else {
            return Err(malformed_multirange_literal(
                text,
                "Unexpected end of input.",
            ));
        };
        match state {
            ParseState::BeforeRange => {
                if matches!(ch, '[' | '(') {
                    let end = scan_range_item_end(text, idx).ok_or_else(|| {
                        malformed_multirange_literal(text, "Unexpected end of input.")
                    })?;
                    let parsed =
                        parse_range_text(&text[idx..end], multirange_type.range_type.sql_type)?;
                    let Value::Range(range) = parsed else {
                        unreachable!("range parser must return a range value");
                    };
                    ranges.push(range);
                    idx = end;
                    saw_range = true;
                    state = ParseState::AfterRange;
                } else if ch == '}' && !saw_range {
                    idx += 1;
                    break;
                } else if starts_with_keyword(&text[idx..], "empty") {
                    idx += "empty".len();
                    ranges.push(empty_range(multirange_type.range_type));
                    saw_range = true;
                    state = ParseState::AfterRange;
                } else {
                    return Err(malformed_multirange_literal(text, "Expected range start."));
                }
            }
            ParseState::AfterRange => {
                if ch == ',' {
                    idx += 1;
                    state = ParseState::BeforeRange;
                } else if ch == '}' {
                    idx += 1;
                    break;
                } else {
                    return Err(malformed_multirange_literal(
                        text,
                        "Expected comma or end of multirange.",
                    ));
                }
            }
        }
    }
    skip_ascii_whitespace(text, &mut idx);
    if idx != text.len() {
        return Err(malformed_multirange_literal(
            text,
            "Junk after closing right brace.",
        ));
    }
    Ok(Value::Multirange(normalize_multirange(
        multirange_type,
        ranges,
    )?))
}

pub fn render_multirange_text(value: &Value) -> Option<String> {
    let Value::Multirange(multirange) = value else {
        return None;
    };
    Some(render_multirange(multirange))
}

pub(crate) fn render_multirange(multirange: &MultirangeValue) -> String {
    if multirange.ranges.is_empty() {
        return "{}".to_string();
    }
    let parts = multirange
        .ranges
        .iter()
        .map(render_range_value)
        .collect::<Vec<_>>();
    format!("{{{}}}", parts.join(","))
}

pub(crate) fn compare_multirange_values(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Ordering {
    match left
        .multirange_type
        .type_oid()
        .cmp(&right.multirange_type.type_oid())
    {
        Ordering::Equal => {}
        other => return other,
    }
    for (left_range, right_range) in left.ranges.iter().zip(right.ranges.iter()) {
        match super::compare_range_values(left_range, right_range) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    left.ranges.len().cmp(&right.ranges.len())
}

pub(crate) fn encode_multirange_bytes(multirange: &MultirangeValue) -> Result<Vec<u8>, ExecError> {
    let mut out = Vec::new();
    out.extend_from_slice(&(multirange.ranges.len() as u32).to_le_bytes());
    for range in &multirange.ranges {
        let bytes = super::encode_range_bytes(range)?;
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

pub(crate) fn decode_multirange_bytes(
    multirange_type: MultirangeTypeRef,
    bytes: &[u8],
) -> Result<MultirangeValue, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<multirange>".into(),
            details: "multirange payload too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut idx = 4usize;
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes.len() < idx + 4 {
            return Err(ExecError::InvalidStorageValue {
                column: "<multirange>".into(),
                details: "multirange item length missing".into(),
            });
        }
        let len = u32::from_le_bytes(bytes[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        if bytes.len() < idx + len {
            return Err(ExecError::InvalidStorageValue {
                column: "<multirange>".into(),
                details: "multirange item payload truncated".into(),
            });
        }
        ranges.push(super::decode_range_bytes(
            multirange_type.range_type,
            &bytes[idx..idx + len],
        )?);
        idx += len;
    }
    if idx != bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<multirange>".into(),
            details: "multirange payload has trailing bytes".into(),
        });
    }
    normalize_multirange(multirange_type, ranges)
}

pub(crate) fn multirange_from_range(range: &RangeValue) -> Result<MultirangeValue, ExecError> {
    let Some(multirange_type) = multirange_type_ref_for_sql_type(
        SqlType::multirange(
            range.range_type.multirange_type_oid,
            range.range_type.type_oid(),
        )
        .with_range_metadata(
            range.range_type.subtype_oid(),
            range.range_type.multirange_type_oid,
            range.range_type.is_discrete(),
        )
        .with_multirange_range_oid(range.range_type.type_oid()),
    ) else {
        return Err(ExecError::TypeMismatch {
            op: "multirange",
            left: Value::Range(range.clone()),
            right: Value::Null,
        });
    };
    normalize_multirange(multirange_type, vec![range.clone()])
}

pub(crate) fn normalize_multirange(
    multirange_type: MultirangeTypeRef,
    ranges: Vec<RangeValue>,
) -> Result<MultirangeValue, ExecError> {
    let mut normalized = Vec::new();
    for range in ranges {
        if range.range_type.type_oid() != multirange_type.range_type.type_oid() {
            return Err(ExecError::TypeMismatch {
                op: "multirange normalization",
                left: Value::Multirange(MultirangeValue {
                    multirange_type,
                    ranges: normalized,
                }),
                right: Value::Range(range),
            });
        }
        if range.empty {
            continue;
        }
        let range = normalize_range(range.range_type, range.lower.clone(), range.upper.clone())?;
        normalized.push(range);
    }
    normalized.sort_by(super::compare_range_values);
    let mut merged: Vec<RangeValue> = Vec::with_capacity(normalized.len());
    for range in normalized {
        if let Some(last) = merged.last_mut()
            && (range_overlap(last, &range) || range_adjacent(last, &range))
        {
            *last = range_merge(last, &range);
            continue;
        }
        merged.push(range);
    }
    Ok(MultirangeValue {
        multirange_type,
        ranges: merged,
    })
}

pub(crate) fn range_agg_transition(
    current: Option<MultirangeValue>,
    input: &Value,
) -> Result<Option<MultirangeValue>, ExecError> {
    let Some(input_multirange) = multirange_value_for_input(input)? else {
        return Ok(current);
    };
    match current {
        None => Ok(Some(input_multirange)),
        Some(current) => {
            ensure_same_multirange_kind("range_agg", &current, &input_multirange)?;
            Ok(Some(multirange_union(&current, &input_multirange)?))
        }
    }
}

pub(crate) fn multirange_intersection_agg_transition(
    current: Option<Value>,
    input: &Value,
) -> Result<Option<Value>, ExecError> {
    if matches!(input, Value::Null) {
        return Ok(current);
    }
    let Some(input_multirange) = multirange_value_for_input(input)? else {
        return Ok(current);
    };
    match current {
        None => Ok(Some(Value::Multirange(input_multirange))),
        Some(existing) => {
            let Some(existing_multirange) = multirange_value_for_input(&existing)? else {
                return Err(ExecError::TypeMismatch {
                    op: "range_intersect_agg",
                    left: existing,
                    right: input.clone(),
                });
            };
            ensure_same_multirange_kind(
                "range_intersect_agg",
                &existing_multirange,
                &input_multirange,
            )?;
            Ok(Some(Value::Multirange(multirange_intersection(
                &existing_multirange,
                &input_multirange,
            )?)))
        }
    }
}

pub(crate) fn eval_multirange_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
) -> Option<Result<Value, ExecError>> {
    use BuiltinScalarFunction::*;

    let result = match func {
        RangeConstructor => eval_multirange_constructor(values, result_type, func_variadic),
        RangeIsEmpty => unary_multirange_bool(values, "isempty", |multirange| {
            Ok(Value::Bool(multirange.ranges.is_empty()))
        }),
        RangeLower => unary_multirange_value(values, "lower", multirange_lower_value),
        RangeUpper => unary_multirange_value(values, "upper", multirange_upper_value),
        RangeLowerInc => unary_multirange_bool(values, "lower_inc", |multirange| {
            Ok(Value::Bool(
                multirange
                    .ranges
                    .first()
                    .and_then(|range| range.lower.as_ref())
                    .is_some_and(|bound| bound.inclusive),
            ))
        }),
        RangeUpperInc => unary_multirange_bool(values, "upper_inc", |multirange| {
            Ok(Value::Bool(
                multirange
                    .ranges
                    .last()
                    .and_then(|range| range.upper.as_ref())
                    .is_some_and(|bound| bound.inclusive),
            ))
        }),
        RangeLowerInf => unary_multirange_bool(values, "lower_inf", |multirange| {
            Ok(Value::Bool(multirange.ranges.first().is_some_and(
                |range| !range.empty && range.lower.is_none(),
            )))
        }),
        RangeUpperInf => unary_multirange_bool(values, "upper_inf", |multirange| {
            Ok(Value::Bool(multirange.ranges.last().is_some_and(|range| {
                !range.empty && range.upper.is_none()
            })))
        }),
        RangeContains => eval_multirange_contains(values),
        RangeContainedBy => eval_multirange_contained_by(values),
        RangeOverlap => binary_multirange_bool(values, "&&", multirange_overlap_values),
        RangeStrictLeft => binary_multirange_bool(values, "<<", multirange_strict_left_values),
        RangeStrictRight => binary_multirange_bool(values, ">>", multirange_strict_right_values),
        RangeOverLeft => binary_multirange_bool(values, "&<", multirange_over_left_values),
        RangeOverRight => binary_multirange_bool(values, "&>", multirange_over_right_values),
        RangeAdjacent => binary_multirange_bool(values, "-|-", multirange_adjacent_values),
        RangeUnion => binary_multirange_multirange(values, "+", multirange_union_values),
        RangeIntersect => binary_multirange_multirange(values, "*", multirange_intersect_values),
        RangeDifference => binary_multirange_multirange(values, "-", multirange_difference_values),
        RangeMerge => eval_multirange_merge(values),
        _ => return None,
    };
    Some(result)
}

fn eval_multirange_constructor(
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
) -> Result<Value, ExecError> {
    let multirange_type = infer_constructor_multirange_type(values, result_type, func_variadic)?;
    let ranges = collect_constructor_ranges(values, multirange_type, func_variadic)?;
    Ok(Value::Multirange(normalize_multirange(
        multirange_type,
        ranges,
    )?))
}

fn infer_constructor_multirange_type(
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
) -> Result<MultirangeTypeRef, ExecError> {
    if let Some(multirange_type) = result_type.and_then(multirange_type_ref_for_sql_type) {
        return Ok(multirange_type);
    }
    for value in values {
        match value {
            Value::Range(range) => {
                return multirange_from_range(range).map(|value| value.multirange_type);
            }
            Value::Multirange(multirange) => return Ok(multirange.multirange_type),
            Value::Array(items) if func_variadic => {
                if let Some(range) = items.iter().find_map(range_from_value) {
                    return multirange_from_range(&range).map(|value| value.multirange_type);
                }
            }
            Value::PgArray(array) if func_variadic => {
                if let Some(range) = array.elements.iter().find_map(range_from_value) {
                    return multirange_from_range(&range).map(|value| value.multirange_type);
                }
            }
            _ => {}
        }
    }
    Err(ExecError::DetailedError {
        message: "could not determine multirange type".into(),
        detail: None,
        hint: None,
        sqlstate: "42804",
    })
}

fn collect_constructor_ranges(
    values: &[Value],
    multirange_type: MultirangeTypeRef,
    func_variadic: bool,
) -> Result<Vec<RangeValue>, ExecError> {
    let mut ranges = Vec::new();
    if func_variadic && values.len() == 1 {
        match &values[0] {
            Value::Null => return Ok(ranges),
            Value::Array(items) => {
                for item in items {
                    extend_constructor_ranges(&mut ranges, item, multirange_type)?;
                }
                return Ok(ranges);
            }
            Value::PgArray(array) => {
                for item in &array.elements {
                    extend_constructor_ranges(&mut ranges, item, multirange_type)?;
                }
                return Ok(ranges);
            }
            _ => {}
        }
    }
    for value in values {
        extend_constructor_ranges(&mut ranges, value, multirange_type)?;
    }
    Ok(ranges)
}

fn extend_constructor_ranges(
    out: &mut Vec<RangeValue>,
    value: &Value,
    multirange_type: MultirangeTypeRef,
) -> Result<(), ExecError> {
    match value {
        Value::Null => Ok(()),
        Value::Range(range) => {
            if range.range_type.type_oid() != multirange_type.range_type.type_oid() {
                return Err(ExecError::TypeMismatch {
                    op: "multirange constructor",
                    left: Value::Range(range.clone()),
                    right: Value::Multirange(MultirangeValue {
                        multirange_type,
                        ranges: Vec::new(),
                    }),
                });
            }
            out.push(range.clone());
            Ok(())
        }
        Value::Multirange(multirange) => {
            ensure_same_multirange_kind(
                "multirange constructor",
                &MultirangeValue {
                    multirange_type,
                    ranges: Vec::new(),
                },
                multirange,
            )?;
            out.extend(multirange.ranges.clone());
            Ok(())
        }
        other => Err(ExecError::TypeMismatch {
            op: "multirange constructor",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn unary_multirange_bool(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&MultirangeValue) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [Value::Multirange(multirange)] => f(multirange),
        [value] => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
        _ => unreachable!(),
    }
}

fn unary_multirange_value(
    values: &[Value],
    op: &'static str,
    f: impl FnOnce(&MultirangeValue) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    unary_multirange_bool(values, op, f)
}

fn binary_multirange_bool(
    values: &[Value],
    _op: &'static str,
    f: impl FnOnce(&Value, &Value) -> Result<bool, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => Ok(Value::Bool(f(left, right)?)),
        _ => unreachable!(),
    }
}

fn binary_multirange_multirange(
    values: &[Value],
    _op: &'static str,
    f: impl FnOnce(&Value, &Value) -> Result<MultirangeValue, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => Ok(Value::Multirange(f(left, right)?)),
        _ => unreachable!(),
    }
}

fn multirange_lower_value(multirange: &MultirangeValue) -> Result<Value, ExecError> {
    Ok(multirange
        .ranges
        .first()
        .and_then(|range| range.lower.as_ref())
        .map(|bound| bound.value.to_owned_value())
        .unwrap_or(Value::Null))
}

fn multirange_upper_value(multirange: &MultirangeValue) -> Result<Value, ExecError> {
    Ok(multirange
        .ranges
        .last()
        .and_then(|range| range.upper.as_ref())
        .map(|bound| bound.value.to_owned_value())
        .unwrap_or(Value::Null))
}

fn eval_multirange_contains(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Multirange(multirange), Value::Range(range)] => {
            ensure_same_range_kind(
                "@>",
                &multirange.multirange_type.range_type.sql_type,
                &range.range_type.sql_type,
                Value::Multirange(multirange.clone()),
                Value::Range(range.clone()),
            )?;
            Ok(Value::Bool(multirange_contains_range(multirange, range)))
        }
        [Value::Multirange(multirange), Value::Multirange(other)] => {
            ensure_same_multirange_kind("@>", multirange, other)?;
            Ok(Value::Bool(multirange_contains_multirange(
                multirange, other,
            )))
        }
        [Value::Range(range), Value::Multirange(multirange)] => {
            ensure_same_range_kind(
                "@>",
                &range.range_type.sql_type,
                &multirange.multirange_type.range_type.sql_type,
                Value::Range(range.clone()),
                Value::Multirange(multirange.clone()),
            )?;
            Ok(Value::Bool(range_contains_multirange(range, multirange)))
        }
        [Value::Multirange(multirange), value] => {
            Ok(Value::Bool(multirange_contains_element(multirange, value)?))
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "@>",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

fn eval_multirange_contained_by(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Range(range), Value::Multirange(multirange)] => {
            ensure_same_range_kind(
                "<@",
                &range.range_type.sql_type,
                &multirange.multirange_type.range_type.sql_type,
                Value::Range(range.clone()),
                Value::Multirange(multirange.clone()),
            )?;
            Ok(Value::Bool(multirange_contains_range(multirange, range)))
        }
        [Value::Multirange(multirange), Value::Range(range)] => {
            ensure_same_range_kind(
                "<@",
                &multirange.multirange_type.range_type.sql_type,
                &range.range_type.sql_type,
                Value::Multirange(multirange.clone()),
                Value::Range(range.clone()),
            )?;
            Ok(Value::Bool(range_contains_multirange(range, multirange)))
        }
        [Value::Multirange(left), Value::Multirange(right)] => {
            ensure_same_multirange_kind("<@", left, right)?;
            Ok(Value::Bool(multirange_contains_multirange(right, left)))
        }
        [value, Value::Multirange(multirange)] => {
            Ok(Value::Bool(multirange_contains_element(multirange, value)?))
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "<@",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => unreachable!(),
    }
}

fn multirange_overlap_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    match (left, right) {
        (Value::Multirange(left), Value::Range(right)) => {
            ensure_same_range_kind(
                "&&",
                &left.multirange_type.range_type.sql_type,
                &right.range_type.sql_type,
                Value::Multirange(left.clone()),
                Value::Range(right.clone()),
            )?;
            Ok(multirange_overlaps_range(left, right))
        }
        (Value::Range(left), Value::Multirange(right)) => {
            ensure_same_range_kind(
                "&&",
                &left.range_type.sql_type,
                &right.multirange_type.range_type.sql_type,
                Value::Range(left.clone()),
                Value::Multirange(right.clone()),
            )?;
            Ok(multirange_overlaps_range(right, left))
        }
        (Value::Multirange(left), Value::Multirange(right)) => {
            ensure_same_multirange_kind("&&", left, right)?;
            Ok(multirange_overlaps_multirange(left, right))
        }
        (left, right) => Err(ExecError::TypeMismatch {
            op: "&&",
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

fn multirange_strict_left_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    let left_span = span_value(left, "<<")?;
    let right_span = span_value(right, "<<")?;
    ensure_same_range_kind(
        "<<",
        &left_span.range_type.sql_type,
        &right_span.range_type.sql_type,
        Value::Range(left_span.clone()),
        Value::Range(right_span.clone()),
    )?;
    Ok(range_strict_left(&left_span, &right_span))
}

fn multirange_strict_right_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    let left_span = span_value(left, ">>")?;
    let right_span = span_value(right, ">>")?;
    ensure_same_range_kind(
        ">>",
        &left_span.range_type.sql_type,
        &right_span.range_type.sql_type,
        Value::Range(left_span.clone()),
        Value::Range(right_span.clone()),
    )?;
    Ok(range_strict_right(&left_span, &right_span))
}

fn multirange_over_left_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    let left_span = span_value(left, "&<")?;
    let right_span = span_value(right, "&<")?;
    ensure_same_range_kind(
        "&<",
        &left_span.range_type.sql_type,
        &right_span.range_type.sql_type,
        Value::Range(left_span.clone()),
        Value::Range(right_span.clone()),
    )?;
    Ok(range_over_left_bounds(&left_span, &right_span))
}

fn multirange_over_right_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    let left_span = span_value(left, "&>")?;
    let right_span = span_value(right, "&>")?;
    ensure_same_range_kind(
        "&>",
        &left_span.range_type.sql_type,
        &right_span.range_type.sql_type,
        Value::Range(left_span.clone()),
        Value::Range(right_span.clone()),
    )?;
    Ok(range_over_right_bounds(&left_span, &right_span))
}

fn multirange_adjacent_values(left: &Value, right: &Value) -> Result<bool, ExecError> {
    match (left, right) {
        (Value::Multirange(left), Value::Range(right)) => {
            ensure_same_range_kind(
                "-|-",
                &left.multirange_type.range_type.sql_type,
                &right.range_type.sql_type,
                Value::Multirange(left.clone()),
                Value::Range(right.clone()),
            )?;
            Ok(multirange_adjacent_range(left, right))
        }
        (Value::Range(left), Value::Multirange(right)) => {
            ensure_same_range_kind(
                "-|-",
                &left.range_type.sql_type,
                &right.multirange_type.range_type.sql_type,
                Value::Range(left.clone()),
                Value::Multirange(right.clone()),
            )?;
            Ok(multirange_adjacent_range(right, left))
        }
        (Value::Multirange(left), Value::Multirange(right)) => {
            ensure_same_multirange_kind("-|-", left, right)?;
            Ok(multirange_adjacent_multirange(left, right))
        }
        (left, right) => Err(ExecError::TypeMismatch {
            op: "-|-",
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

fn multirange_union_values(left: &Value, right: &Value) -> Result<MultirangeValue, ExecError> {
    let left = multirange_value_for_binary(left, "+")?;
    let right = multirange_value_for_binary(right, "+")?;
    ensure_same_multirange_kind("+", &left, &right)?;
    multirange_union(&left, &right)
}

fn multirange_intersect_values(left: &Value, right: &Value) -> Result<MultirangeValue, ExecError> {
    let left = multirange_value_for_binary(left, "*")?;
    let right = multirange_value_for_binary(right, "*")?;
    ensure_same_multirange_kind("*", &left, &right)?;
    multirange_intersection(&left, &right)
}

fn multirange_difference_values(left: &Value, right: &Value) -> Result<MultirangeValue, ExecError> {
    let left = multirange_value_for_binary(left, "-")?;
    let right = multirange_value_for_binary(right, "-")?;
    ensure_same_multirange_kind("-", &left, &right)?;
    multirange_difference(&left, &right)
}

fn eval_multirange_merge(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [Value::Multirange(multirange)] => Ok(Value::Range(span_multirange(multirange))),
        _ => Err(ExecError::TypeMismatch {
            op: "range_merge",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn multirange_contains_element(
    multirange: &MultirangeValue,
    value: &Value,
) -> Result<bool, ExecError> {
    for range in &multirange.ranges {
        if range_contains_element(range, value)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn multirange_contains_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    if range.empty {
        return true;
    }
    multirange
        .ranges
        .iter()
        .any(|candidate| range_contains_range(candidate, range))
}

fn range_contains_multirange(range: &RangeValue, multirange: &MultirangeValue) -> bool {
    multirange
        .ranges
        .iter()
        .all(|candidate| range_contains_range(range, candidate))
}

fn multirange_contains_multirange(left: &MultirangeValue, right: &MultirangeValue) -> bool {
    right
        .ranges
        .iter()
        .all(|candidate| multirange_contains_range(left, candidate))
}

fn multirange_overlaps_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    multirange
        .ranges
        .iter()
        .any(|candidate| range_overlap(candidate, range))
}

fn multirange_overlaps_multirange(left: &MultirangeValue, right: &MultirangeValue) -> bool {
    left.ranges.iter().any(|left_range| {
        right
            .ranges
            .iter()
            .any(|right_range| range_overlap(left_range, right_range))
    })
}

fn multirange_adjacent_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    !multirange_overlaps_range(multirange, range)
        && multirange
            .ranges
            .iter()
            .any(|candidate| range_adjacent(candidate, range))
}

fn multirange_adjacent_multirange(left: &MultirangeValue, right: &MultirangeValue) -> bool {
    !multirange_overlaps_multirange(left, right)
        && left.ranges.iter().any(|left_range| {
            right
                .ranges
                .iter()
                .any(|right_range| range_adjacent(left_range, right_range))
        })
}

fn multirange_union(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Result<MultirangeValue, ExecError> {
    let mut ranges = left.ranges.clone();
    ranges.extend(right.ranges.clone());
    normalize_multirange(left.multirange_type, ranges)
}

fn multirange_intersection(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Result<MultirangeValue, ExecError> {
    let mut ranges = Vec::new();
    for left_range in &left.ranges {
        for right_range in &right.ranges {
            if range_overlap(left_range, right_range) {
                let intersection = range_intersection(left_range, right_range);
                if !intersection.empty {
                    ranges.push(intersection);
                }
            }
        }
    }
    normalize_multirange(left.multirange_type, ranges)
}

fn multirange_difference(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Result<MultirangeValue, ExecError> {
    let mut remaining = left.ranges.clone();
    for right_range in &right.ranges {
        let mut next = Vec::new();
        for left_range in &remaining {
            next.extend(range_difference_segments(left_range, right_range)?);
        }
        remaining = next;
        if remaining.is_empty() {
            break;
        }
    }
    normalize_multirange(left.multirange_type, remaining)
}

fn multirange_value_for_input(value: &Value) -> Result<Option<MultirangeValue>, ExecError> {
    match value {
        Value::Null => Ok(None),
        Value::Range(range) => Ok(Some(multirange_from_range(range)?)),
        Value::Multirange(multirange) => Ok(Some(multirange.clone())),
        other => Err(ExecError::TypeMismatch {
            op: "multirange input",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn multirange_value_for_binary(
    value: &Value,
    op: &'static str,
) -> Result<MultirangeValue, ExecError> {
    multirange_value_for_input(value)?.ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: Value::Null,
        right: Value::Null,
    })
}

fn range_from_value(value: &Value) -> Option<RangeValue> {
    match value {
        Value::Range(range) => Some(range.clone()),
        _ => None,
    }
}

fn span_value(value: &Value, op: &'static str) -> Result<RangeValue, ExecError> {
    match value {
        Value::Range(range) => Ok(range.clone()),
        Value::Multirange(multirange) => Ok(span_multirange(multirange)),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn span_multirange(multirange: &MultirangeValue) -> RangeValue {
    match (multirange.ranges.first(), multirange.ranges.last()) {
        (Some(first), Some(last)) => RangeValue {
            range_type: multirange.multirange_type.range_type,
            empty: false,
            lower: first.lower.clone(),
            upper: last.upper.clone(),
        },
        _ => empty_range(multirange.multirange_type.range_type),
    }
}

fn ensure_same_multirange_kind(
    op: &'static str,
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Result<(), ExecError> {
    if left.multirange_type.type_oid() == right.multirange_type.type_oid() {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch {
            op,
            left: Value::Multirange(left.clone()),
            right: Value::Multirange(right.clone()),
        })
    }
}

fn ensure_same_range_kind(
    op: &'static str,
    left: &SqlType,
    right: &SqlType,
    left_value: Value,
    right_value: Value,
) -> Result<(), ExecError> {
    if left.type_oid == right.type_oid {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch {
            op,
            left: left_value,
            right: right_value,
        })
    }
}

fn malformed_multirange_literal(value: &str, detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("malformed multirange literal: \"{value}\""),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "22P02",
    }
}

fn starts_with_keyword(text: &str, keyword: &str) -> bool {
    text.get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
}

fn skip_ascii_whitespace(text: &str, idx: &mut usize) {
    while *idx < text.len() && text.as_bytes()[*idx].is_ascii_whitespace() {
        *idx += 1;
    }
}

fn scan_range_item_end(text: &str, start: usize) -> Option<usize> {
    let mut in_quotes = false;
    let mut escaped = false;
    let mut idx = start;
    while let Some(ch) = char_at(text, idx) {
        if escaped {
            escaped = false;
            idx += ch.len_utf8();
            continue;
        }
        match ch {
            '\\' => {
                escaped = true;
                idx += ch.len_utf8();
            }
            '"' => {
                idx += ch.len_utf8();
                if in_quotes && matches!(char_at(text, idx), Some('"')) {
                    idx += 1;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            ']' | ')' if !in_quotes => return Some(idx + ch.len_utf8()),
            _ => idx += ch.len_utf8(),
        }
    }
    None
}

fn char_at(text: &str, idx: usize) -> Option<char> {
    text.get(idx..)?.chars().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{INT4MULTIRANGE_TYPE_OID, INT4RANGE_TYPE_OID};

    fn int4_multirange_type() -> SqlType {
        SqlType::multirange(INT4MULTIRANGE_TYPE_OID, INT4RANGE_TYPE_OID)
            .with_multirange_range_oid(INT4RANGE_TYPE_OID)
    }

    #[test]
    fn normalize_multirange_drops_empty_ranges() {
        let range_type = multirange_type_ref_for_sql_type(int4_multirange_type())
            .expect("int4multirange")
            .range_type;
        let multirange = normalize_multirange(
            multirange_type_ref_for_sql_type(int4_multirange_type()).expect("int4multirange"),
            vec![empty_range(range_type)],
        )
        .unwrap();
        assert!(multirange.ranges.is_empty());
        assert_eq!(render_multirange(&multirange), "{}");
    }

    #[test]
    fn parse_multirange_rejects_trailing_comma() {
        let err = parse_multirange_text("{[1,3),}", int4_multirange_type()).unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "malformed multirange literal: \"{[1,3),}\"");
                assert_eq!(detail.as_deref(), Some("Expected range start."));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

impl From<RangeValue> for Value {
    fn from(value: RangeValue) -> Self {
        Value::Range(value)
    }
}
