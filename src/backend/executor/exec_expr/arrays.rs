use super::subquery::compare_subquery_values;
use super::*;

pub(super) fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    array_value: &Value,
) -> Result<Value, ExecError> {
    if matches!(array_value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(array) = normalize_array_value(array_value) {
        if array.elements.is_empty() {
            return Ok(Value::Bool(is_all));
        }
        let mut saw_null = false;
        for item in &array.elements {
            if matches!(item, Value::Null) {
                saw_null = true;
                continue;
            }
            match compare_subquery_values(left_value, item, op)? {
                Value::Bool(result) => {
                    if !is_all && result {
                        return Ok(Value::Bool(true));
                    }
                    if is_all && !result {
                        return Ok(Value::Bool(false));
                    }
                }
                Value::Null => saw_null = true,
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }
        if saw_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(is_all))
        }
    } else {
        Err(ExecError::TypeMismatch {
            op: if is_all { "ALL" } else { "ANY" },
            left: array_value.clone(),
            right: Value::Null,
        })
    }
}

pub(super) fn eval_array_subscript(
    value: Value,
    subscripts: &[crate::include::nodes::primnodes::ExprArraySubscript],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedArraySubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    apply_array_subscripts(value, &resolved)
}

pub(super) fn eval_array_subscript_plpgsql(
    value: Value,
    subscripts: &[crate::include::nodes::primnodes::ExprArraySubscript],
    slot: &mut TupleSlot,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedArraySubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    apply_array_subscripts(value, &resolved)
}

#[derive(Clone)]
struct ResolvedArraySubscript {
    is_slice: bool,
    lower: Option<Value>,
    upper: Option<Value>,
}

fn apply_array_subscripts(
    value: Value,
    subscripts: &[ResolvedArraySubscript],
) -> Result<Value, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let array = normalize_array_value(&value).ok_or_else(|| ExecError::TypeMismatch {
        op: "array subscript",
        left: value.clone(),
        right: Value::Null,
    })?;
    let any_slice = subscripts.iter().any(|subscript| subscript.is_slice);
    if array.dimensions.is_empty() {
        return if any_slice {
            Ok(Value::PgArray(ArrayValue::empty()))
        } else {
            Ok(Value::Null)
        };
    }
    if subscripts.len() > array.ndim() {
        return Ok(Value::Null);
    }
    apply_array_subscripts_to_value(&array, subscripts, any_slice)
}

fn apply_array_subscripts_to_value(
    array: &ArrayValue,
    subscripts: &[ResolvedArraySubscript],
    any_slice: bool,
) -> Result<Value, ExecError> {
    let mut selectors = Vec::with_capacity(array.ndim());
    let mut result_dimensions = Vec::new();
    for (dim_idx, dim) in array.dimensions.iter().enumerate() {
        if let Some(subscript) = subscripts.get(dim_idx) {
            if any_slice {
                let (lower, upper) = if subscript.is_slice {
                    (
                        array_slice_bound_index(subscript.lower.as_ref())?
                            .unwrap_or(dim.lower_bound),
                        array_slice_bound_index(subscript.upper.as_ref())?
                            .unwrap_or(dim.lower_bound + dim.length as i32 - 1),
                    )
                } else {
                    let Some(index) = array_subscript_index(subscript.lower.as_ref())? else {
                        return Ok(Value::Null);
                    };
                    (1, index)
                };
                let clamped_lower = lower.max(dim.lower_bound);
                let clamped_upper = upper.min(dim.lower_bound + dim.length as i32 - 1);
                let length = if clamped_upper < clamped_lower {
                    0
                } else {
                    (clamped_upper - clamped_lower + 1) as usize
                };
                selectors.push(ArraySelector::Slice {
                    lower: clamped_lower,
                    upper: clamped_upper,
                });
                result_dimensions.push(ArrayDimension {
                    lower_bound: clamped_lower,
                    length,
                });
            } else {
                let Some(index) = array_subscript_index(subscript.lower.as_ref())? else {
                    return Ok(Value::Null);
                };
                selectors.push(ArraySelector::Index(index));
            }
        } else {
            selectors.push(ArraySelector::Slice {
                lower: dim.lower_bound,
                upper: dim.lower_bound + dim.length as i32 - 1,
            });
            result_dimensions.push(dim.clone());
        }
    }

    let mut matched = Vec::new();
    for (offset, item) in array.elements.iter().enumerate() {
        let coords = linear_index_to_coords(offset, &array.dimensions);
        if coords_match_selectors(&coords, &selectors) {
            matched.push(item.clone());
        }
    }
    if result_dimensions.is_empty() {
        return Ok(matched.into_iter().next().unwrap_or(Value::Null));
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        result_dimensions,
        matched,
    )))
}

#[derive(Clone)]
enum ArraySelector {
    Index(i32),
    Slice { lower: i32, upper: i32 },
}

fn coords_match_selectors(coords: &[i32], selectors: &[ArraySelector]) -> bool {
    coords
        .iter()
        .zip(selectors.iter())
        .all(|(coord, selector)| match selector {
            ArraySelector::Index(index) => coord == index,
            ArraySelector::Slice { lower, upper } => coord >= lower && coord <= upper,
        })
}

fn linear_index_to_coords(offset: usize, dimensions: &[ArrayDimension]) -> Vec<i32> {
    if dimensions.is_empty() {
        return Vec::new();
    }
    let mut coords = vec![0; dimensions.len()];
    let mut remaining = offset;
    for dim_idx in 0..dimensions.len() {
        let stride = dimensions[dim_idx + 1..]
            .iter()
            .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
        let axis_offset = if stride == 0 { 0 } else { remaining / stride };
        coords[dim_idx] = dimensions[dim_idx].lower_bound + axis_offset as i32;
        remaining %= stride.max(1);
    }
    coords
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

pub(super) fn eval_array_ndims_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => Ok(normalize_array_value(value)
            .and_then(|array| {
                (!array.dimensions.is_empty()).then_some(Value::Int32(array.ndim() as i32))
            })
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_ndims(array)",
            actual: format!("ArrayNdims({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_dims_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let Some(array) = normalize_array_value(value) else {
                return Ok(Value::Null);
            };
            if array.dimensions.is_empty() {
                return Ok(Value::Null);
            }
            let mut out = String::new();
            for dim in &array.dimensions {
                let upper = dim.lower_bound + dim.length as i32 - 1;
                out.push('[');
                out.push_str(&dim.lower_bound.to_string());
                out.push(':');
                out.push_str(&upper.to_string());
                out.push(']');
            }
            Ok(Value::Text(out.into()))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_dims(array)",
            actual: format!("ArrayDims({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_fill_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [fill, dims] => build_filled_array(fill, dims, None),
        [fill, dims, lbs] => build_filled_array(fill, dims, Some(lbs)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_fill(value, dimensions [, lower_bounds])",
            actual: format!("ArrayFill({} args)", values.len()),
        })),
    }
}

fn build_filled_array(
    fill: &Value,
    dims: &Value,
    lower_bounds: Option<&Value>,
) -> Result<Value, ExecError> {
    if matches!(dims, Value::Null) || lower_bounds.is_some_and(|value| matches!(value, Value::Null))
    {
        return Ok(Value::Null);
    }
    let dims = parse_int_array_argument("array_fill", dims)?;
    let lower_bounds = lower_bounds
        .map(|value| parse_int_array_argument("array_fill", value))
        .transpose()?;
    if let Some(lbs) = &lower_bounds {
        if lbs.len() != dims.len() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "matching dimension and lower-bound array lengths",
                actual: "array_fill".into(),
            }));
        }
    }
    if dims.iter().any(|dim| dim.is_none()) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "dimension values cannot be null",
            actual: "array_fill".into(),
        }));
    }
    let dims = dims.into_iter().map(|dim| dim.unwrap()).collect::<Vec<_>>();
    if lower_bounds
        .as_ref()
        .is_some_and(|lbs| lbs.iter().any(|lb| lb.is_none()))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "low bound values cannot be null",
            actual: "array_fill".into(),
        }));
    }
    if dims.is_empty() || dims.iter().any(|dim| *dim == 0) {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    let dimensions = dims
        .iter()
        .enumerate()
        .map(|(idx, dim)| ArrayDimension {
            lower_bound: lower_bounds
                .as_ref()
                .and_then(|lbs| lbs.get(idx).and_then(|lb| *lb))
                .unwrap_or(1),
            length: *dim as usize,
        })
        .collect::<Vec<_>>();
    let total = dimensions
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions,
        std::iter::repeat_with(|| fill.to_owned_value())
            .take(total)
            .collect(),
    )))
}

pub(super) fn eval_string_to_array_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] | [Value::Null, _, _] | [_, Value::Null, _] => {
            Ok(Value::Null)
        }
        [input, delimiter] => string_to_array_values(input, delimiter, None),
        [input, delimiter, null_text] => {
            if matches!(input, Value::Null) || matches!(delimiter, Value::Null) {
                return Ok(Value::Null);
            }
            string_to_array_values(input, delimiter, Some(null_text))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "string_to_array(text, delimiter [, null_string])",
            actual: format!("StringToArray({} args)", values.len()),
        })),
    }
}

fn string_to_array_values(
    input: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Value, ExecError> {
    let input = input.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "string_to_array",
        left: input.clone(),
        right: delimiter.clone(),
    })?;
    let delimiter = delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "string_to_array",
        left: delimiter.clone(),
        right: Value::Text(input.into()),
    })?;
    let null_text = null_text.and_then(Value::as_text);
    let parts: Vec<String> = if delimiter.is_empty() {
        input.chars().map(|ch| ch.to_string()).collect()
    } else if input.is_empty() {
        Vec::new()
    } else {
        input
            .split(delimiter)
            .map(|part| part.to_string())
            .collect()
    };
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: 1,
            length: parts.len(),
        }],
        parts
            .into_iter()
            .map(|part| match null_text {
                Some(null_marker) if part == null_marker => Value::Null,
                _ => Value::Text(part.into()),
            })
            .collect(),
    )))
}

pub(super) fn eval_array_to_string_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] => Ok(Value::Null),
        [_, Value::Null] | [_, Value::Null, _] => Ok(Value::Null),
        [array, delimiter] => array_to_string_value(array, delimiter, None),
        [array, delimiter, null_text] => array_to_string_value(array, delimiter, Some(null_text)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_to_string(array, delimiter [, null_string])",
            actual: format!("ArrayToString({} args)", values.len()),
        })),
    }
}

fn array_to_string_value(
    array: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Value, ExecError> {
    let array = normalize_array_value(array).ok_or_else(|| ExecError::TypeMismatch {
        op: "array_to_string",
        left: array.clone(),
        right: delimiter.clone(),
    })?;
    let delimiter = delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "array_to_string",
        left: delimiter.clone(),
        right: Value::Null,
    })?;
    let null_text = null_text.and_then(Value::as_text);
    let mut out = String::new();
    let mut first = true;
    for item in &array.elements {
        if matches!(item, Value::Null) && null_text.is_none() {
            continue;
        }
        if !first {
            out.push_str(delimiter);
        }
        first = false;
        if matches!(item, Value::Null) {
            out.push_str(null_text.unwrap_or_default());
        } else {
            out.push_str(&render_scalar_text(item)?);
        }
    }
    Ok(Value::Text(out.into()))
}

pub(super) fn eval_array_length_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [array, dim] => {
            let Some(array) = normalize_array_value(array) else {
                return Ok(Value::Null);
            };
            let dim = array_subscript_index(Some(dim))?.unwrap_or(0);
            if dim < 1 {
                return Ok(Value::Null);
            }
            Ok(array
                .axis_len((dim - 1) as usize)
                .map(|len| Value::Int32(len as i32))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_length(array, dimension)",
            actual: format!("ArrayLength({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_cardinality_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [array] => Ok(normalize_array_value(array)
            .map(|array| Value::Int32(array.elements.len() as i32))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "cardinality(array)",
            actual: format!("Cardinality({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_position_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] => Ok(Value::Null),
        [array, needle] => array_position_value(array, needle, None, false),
        [array, needle, start] => {
            let start = array_subscript_index(Some(start))?;
            array_position_value(array, needle, start, false)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_position(array, value [, start])",
            actual: format!("ArrayPosition({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_positions_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] => Ok(Value::Null),
        [array, needle] => array_position_value(array, needle, None, true),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_positions(array, value)",
            actual: format!("ArrayPositions({} args)", values.len()),
        })),
    }
}

fn array_position_value(
    array: &Value,
    needle: &Value,
    start: Option<i32>,
    all: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.ndim() > 1 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "one-dimensional array",
            actual: if all {
                "array_positions"
            } else {
                "array_position"
            }
            .into(),
        }));
    }
    let lower_bound = array.lower_bound(0).unwrap_or(1);
    let start = start.unwrap_or(lower_bound);
    let mut matches = Vec::new();
    for (idx, item) in array.elements.iter().enumerate() {
        let position = lower_bound + idx as i32;
        if position < start {
            continue;
        }
        let is_match = if matches!(needle, Value::Null) {
            matches!(item, Value::Null)
        } else if matches!(item, Value::Null) {
            false
        } else {
            matches!(
                compare_values("=", item.clone(), needle.clone())?,
                Value::Bool(true)
            )
        };
        if is_match {
            if !all {
                return Ok(Value::Int32(position));
            }
            matches.push(Value::Int32(position));
        }
    }
    if all {
        Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: matches.len(),
            }],
            matches,
        )))
    } else {
        Ok(Value::Null)
    }
}

pub(super) fn eval_array_remove_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] => Ok(Value::Null),
        [array, target] => array_replace_like(array, target, None, true),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_remove(array, value)",
            actual: format!("ArrayRemove({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_replace_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] => Ok(Value::Null),
        [array, search, replace] => array_replace_like(array, search, Some(replace), false),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_replace(array, search, replace)",
            actual: format!("ArrayReplace({} args)", values.len()),
        })),
    }
}

fn array_replace_like(
    array: &Value,
    search: &Value,
    replace: Option<&Value>,
    remove: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.ndim() > 1 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "one-dimensional array",
            actual: if remove {
                "array_remove"
            } else {
                "array_replace"
            }
            .into(),
        }));
    }
    let mut items = Vec::new();
    for item in &array.elements {
        let matched = if matches!(search, Value::Null) {
            matches!(item, Value::Null)
        } else if matches!(item, Value::Null) {
            false
        } else {
            matches!(
                compare_values("=", item.clone(), search.clone())?,
                Value::Bool(true)
            )
        };
        if matched {
            if remove {
                continue;
            }
            items.push(replace.unwrap_or(&Value::Null).to_owned_value());
        } else {
            items.push(item.to_owned_value());
        }
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: array.lower_bound(0).unwrap_or(1),
            length: items.len(),
        }],
        items,
    )))
}

pub(super) fn eval_array_sort_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] | [Value::Null, ..] => Ok(Value::Null),
        [array] => array_sort_value(array, false, false),
        [array, Value::Bool(desc)] => array_sort_value(array, *desc, false),
        [array, Value::Bool(desc), Value::Bool(nulls_first)] => {
            array_sort_value(array, *desc, *nulls_first)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_sort(array [, descending [, nulls_first]])",
            actual: format!("ArraySort({} args)", values.len()),
        })),
    }
}

fn array_sort_value(
    array: &Value,
    descending: bool,
    nulls_first: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.dimensions.is_empty() {
        return Ok(Value::PgArray(array));
    }
    if array.ndim() == 1 {
        let mut items = array.elements.clone();
        items.sort_by(|left, right| {
            compare_order_values(left, right, Some(nulls_first), descending)
        });
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            array.dimensions,
            items,
        )));
    }
    let slice_dims = array.dimensions[1..].to_vec();
    let slice_len = slice_dims
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    let mut slices = array
        .elements
        .chunks(slice_len)
        .map(|chunk| {
            Value::PgArray(ArrayValue::from_dimensions(
                slice_dims.clone(),
                chunk.to_vec(),
            ))
        })
        .collect::<Vec<_>>();
    slices.sort_by(|left, right| compare_order_values(left, right, Some(nulls_first), descending));
    let mut elements = Vec::with_capacity(array.elements.len());
    for slice in slices {
        if let Value::PgArray(slice_array) = slice {
            elements.extend(slice_array.elements);
        }
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        array.dimensions,
        elements,
    )))
}

fn parse_int_array_argument(
    op: &'static str,
    value: &Value,
) -> Result<Vec<Option<i32>>, ExecError> {
    let Some(array) = normalize_array_value(value) else {
        return Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        });
    };
    if array.ndim() > 1 {
        return Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        });
    }
    array
        .elements
        .iter()
        .map(|item| array_subscript_index(Some(item)))
        .collect()
}

fn render_scalar_text(value: &Value) -> Result<String, ExecError> {
    match value {
        Value::PgArray(array) => Ok(format_array_value_text(array)),
        Value::Array(items) => Ok(format_array_text(items)),
        _ => cast_value(value.to_owned_value(), SqlType::new(SqlTypeKind::Text))?
            .as_text()
            .map(|text| text.to_string())
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "::text",
                left: value.clone(),
                right: Value::Text("".into()),
            }),
    }
}

pub(super) fn eval_width_bucket_thresholds(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [operand, thresholds] => {
            let Some(thresholds) = normalize_array_value(thresholds) else {
                return Err(ExecError::TypeMismatch {
                    op: "width_bucket",
                    left: operand.clone(),
                    right: thresholds.clone(),
                });
            };
            if thresholds.ndim() != 1 {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "one-dimensional thresholds array",
                    actual: "width_bucket".into(),
                }));
            }
            if thresholds
                .elements
                .iter()
                .any(|value| matches!(value, Value::Null))
            {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "thresholds array without NULLs",
                    actual: "width_bucket".into(),
                }));
            }
            if thresholds.elements.is_empty() {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "non-empty thresholds array",
                    actual: "width_bucket".into(),
                }));
            }
            let mut bucket = 0i32;
            for threshold in &thresholds.elements {
                if matches!(
                    order_values("<", operand.clone(), threshold.clone())?,
                    Value::Bool(true)
                ) {
                    break;
                }
                bucket = bucket.checked_add(1).ok_or(ExecError::Int4OutOfRange)?;
            }
            Ok(Value::Int32(bucket))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "width_bucket(operand, thresholds)",
            actual: format!("WidthBucket({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_lower_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [value, Value::Int16(dim)] => eval_array_lower_value(value, *dim as i32),
        [value, Value::Int32(dim)] => eval_array_lower_value(value, *dim),
        [value, Value::Int64(dim)] => {
            let dim = i32::try_from(*dim).map_err(|_| ExecError::Int4OutOfRange)?;
            eval_array_lower_value(value, dim)
        }
        [value, other] => Err(ExecError::TypeMismatch {
            op: "array_lower",
            left: value.clone(),
            right: other.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_lower(array, dimension)",
            actual: format!("ArrayLower({} args)", values.len()),
        })),
    }
}

fn eval_array_lower_value(value: &Value, dimension: i32) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(value) else {
        return Ok(Value::Null);
    };
    if dimension < 1 {
        return Ok(Value::Null);
    }
    Ok(array
        .lower_bound((dimension - 1) as usize)
        .map(Value::Int32)
        .unwrap_or(Value::Null))
}

fn array_subscript_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array subscript",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn array_slice_bound_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array subscript",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub(super) fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(left_array) = normalize_array_value(&left) else {
        return Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right: right.clone(),
        });
    };
    let Some(right_array) = normalize_array_value(&right) else {
        return Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right,
        });
    };
    for left_item in &left_array.elements {
        if matches!(left_item, Value::Null) {
            continue;
        }
        for right_item in &right_array.elements {
            if matches!(right_item, Value::Null) {
                continue;
            }
            if matches!(
                compare_values("=", left_item.clone(), right_item.clone())?,
                Value::Bool(true)
            ) {
                return Ok(Value::Bool(true));
            }
        }
    }
    Ok(Value::Bool(false))
}
