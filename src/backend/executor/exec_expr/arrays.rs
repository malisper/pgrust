use super::subquery::compare_subquery_values;
use super::*;

pub(super) fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    collation_oid: Option<u32>,
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
            match compare_subquery_values(left_value, item, op, collation_oid)? {
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
                lower_provided: subscript.lower.is_some(),
                upper_provided: subscript.upper.is_some(),
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
                lower_provided: subscript.lower.is_some(),
                upper_provided: subscript.upper.is_some(),
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
    lower_provided: bool,
    upper_provided: bool,
    lower: Option<Value>,
    upper: Option<Value>,
}

fn apply_array_subscripts(
    value: Value,
    subscripts: &[ResolvedArraySubscript],
) -> Result<Value, ExecError> {
    if subscripts.len() > 6 {
        return Err(ExecError::DetailedError {
            message: format!(
                "number of array dimensions ({}) exceeds the maximum allowed (6)",
                subscripts.len()
            ),
            detail: None,
            hint: None,
            sqlstate: "54000",
        });
    }
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
        return if any_slice {
            Ok(Value::PgArray(ArrayValue::empty()))
        } else {
            Ok(Value::Null)
        };
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
                        match array_slice_bound_index(
                            subscript.lower.as_ref(),
                            subscript.lower_provided,
                        )? {
                            SliceBound::Omitted => dim.lower_bound,
                            SliceBound::Null => return Ok(Value::Null),
                            SliceBound::Value(value) => value,
                        },
                        match array_slice_bound_index(
                            subscript.upper.as_ref(),
                            subscript.upper_provided,
                        )? {
                            SliceBound::Omitted => dim.lower_bound + dim.length as i32 - 1,
                            SliceBound::Null => return Ok(Value::Null),
                            SliceBound::Value(value) => value,
                        },
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
                    // PostgreSQL rebases all slice results to 1-based bounds.
                    lower_bound: 1,
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
            result_dimensions.push(ArrayDimension {
                lower_bound: 1,
                length: dim.length,
            });
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

enum SliceBound {
    Omitted,
    Null,
    Value(i32),
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

pub(crate) fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        Value::Text(text) => parse_vector_array_text(text.as_str()),
        Value::TextRef(_, _) => parse_vector_array_text(value.as_text()?),
        _ => None,
    }
}

fn parse_vector_array_text(text: &str) -> Option<ArrayValue> {
    let trimmed = text.trim();
    if let Some(array) = parse_bounded_vector_array_text(trimmed) {
        return Some(array);
    }
    if trimmed.starts_with('{') {
        return None;
    }
    if trimmed.is_empty() {
        return Some(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 0,
                length: 0,
            }],
            Vec::new(),
        ));
    }
    let mut items = Vec::new();
    for item in trimmed.split_whitespace() {
        let value = item.parse::<u32>().ok()?;
        items.push(Value::Int64(value as i64));
    }
    Some(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: 0,
            length: items.len(),
        }],
        items,
    ))
}

fn parse_bounded_vector_array_text(text: &str) -> Option<ArrayValue> {
    if !text.starts_with('[') {
        return None;
    }
    let equals = text.find('=')?;
    let mut dimensions = Vec::new();
    let mut remaining = &text[..equals];
    while let Some(rest) = remaining.strip_prefix('[') {
        let end = rest.find(']')?;
        let (lower, upper) = rest[..end].split_once(':')?;
        let lower = lower.trim().parse::<i32>().ok()?;
        let upper = upper.trim().parse::<i32>().ok()?;
        if upper < lower {
            return None;
        }
        dimensions.push(ArrayDimension {
            lower_bound: lower,
            length: (upper - lower + 1) as usize,
        });
        remaining = &rest[end + 1..];
    }
    if !remaining.is_empty() || dimensions.is_empty() {
        return None;
    }

    let body = text[equals + 1..].trim();
    let inner = body.strip_prefix('{')?.strip_suffix('}')?.trim();
    if inner.is_empty() {
        return Some(ArrayValue::empty());
    }
    let mut items = Vec::new();
    for item in inner.split(',') {
        let item = item.trim().trim_matches('"');
        let value = item.parse::<u32>().ok()?;
        items.push(Value::Int64(value as i64));
    }
    Some(ArrayValue::from_dimensions(dimensions, items))
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
        return Err(array_fill_null_array_error());
    }
    let dims = parse_int_array_argument("array_fill", dims, ArrayFillArgKind::Dimension)?;
    let lower_bounds = lower_bounds
        .map(|value| parse_int_array_argument("array_fill", value, ArrayFillArgKind::LowerBound))
        .transpose()?;
    if let Some(lbs) = &lower_bounds {
        if lbs.len() != dims.len() {
            return Err(array_fill_low_bound_mismatch_error());
        }
    }
    if dims.iter().any(|dim| dim.is_none()) {
        return Err(ExecError::DetailedError {
            message: "dimension values cannot be null".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        });
    }
    let dims = dims.into_iter().map(|dim| dim.unwrap()).collect::<Vec<_>>();
    if lower_bounds
        .as_ref()
        .is_some_and(|lbs| lbs.iter().any(|lb| lb.is_none()))
    {
        return Err(array_fill_null_array_error());
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
        [Value::Null, _] | [Value::Null, _, _] => Ok(Value::Null),
        [input, delimiter] => string_to_array_values(input, delimiter, None),
        [input, delimiter, null_text] => string_to_array_values(input, delimiter, Some(null_text)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "string_to_array(text, delimiter [, null_string])",
            actual: format!("StringToArray({} args)", values.len()),
        })),
    }
}

pub(crate) fn eval_string_to_table_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] => Ok(Vec::new()),
        [input, delimiter] => string_to_table_values(input, delimiter, None),
        [input, delimiter, null_text] => string_to_table_values(input, delimiter, Some(null_text)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "string_to_table(text, delimiter [, null_string])",
            actual: format!("StringToTable({} args)", values.len()),
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
    let delimiter = if matches!(delimiter, Value::Null) {
        None
    } else {
        Some(delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op: "string_to_array",
            left: delimiter.clone(),
            right: Value::Text(input.into()),
        })?)
    };
    let null_text = null_text.and_then(Value::as_text);
    let parts = split_text_values(input, delimiter, null_text);
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: 1,
            length: parts.len(),
        }],
        parts,
    )))
}

fn string_to_table_values(
    input: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Vec<Value>, ExecError> {
    let input = input.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "string_to_table",
        left: input.clone(),
        right: delimiter.clone(),
    })?;
    let delimiter = if matches!(delimiter, Value::Null) {
        None
    } else {
        Some(delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op: "string_to_table",
            left: delimiter.clone(),
            right: Value::Text(input.into()),
        })?)
    };
    let null_text = null_text.and_then(Value::as_text);
    Ok(split_text_values(input, delimiter, null_text))
}

fn split_text_values(input: &str, delimiter: Option<&str>, null_text: Option<&str>) -> Vec<Value> {
    let parts: Vec<String> = match delimiter {
        Some(_) if input.is_empty() => Vec::new(),
        Some(delimiter) if delimiter.is_empty() => vec![input.to_string()],
        Some(delimiter) => input
            .split(delimiter)
            .map(|part| part.to_string())
            .collect(),
        None => input.chars().map(|ch| ch.to_string()).collect(),
    };
    parts
        .into_iter()
        .map(|part| match null_text {
            Some(null_marker) if part == null_marker => Value::Null,
            _ => Value::Text(part.into()),
        })
        .collect()
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

pub(super) fn eval_array_upper_function(values: &[Value]) -> Result<Value, ExecError> {
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
                .upper_bound((dim - 1) as usize)
                .map(Value::Int32)
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_upper(array, dimension)",
            actual: format!("ArrayUpper({} args)", values.len()),
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

pub(super) fn eval_array_append_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [array, element] => append_array_value(array, element, false),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_append(array, element)",
            actual: format!("ArrayAppend({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_prepend_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [element, array] => append_array_value(array, element, true),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_prepend(element, array)",
            actual: format!("ArrayPrepend({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_cat_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left_array =
                normalize_array_value(left).ok_or_else(|| ExecError::TypeMismatch {
                    op: "array_cat",
                    left: left.clone(),
                    right: right.clone(),
                })?;
            let right_array =
                normalize_array_value(right).ok_or_else(|| ExecError::TypeMismatch {
                    op: "array_cat",
                    left: left.clone(),
                    right: right.clone(),
                })?;
            Ok(Value::PgArray(concatenate_arrays(left_array, right_array)?))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_cat(left, right)",
            actual: format!("ArrayCat({} args)", values.len()),
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
        return Err(ExecError::DetailedError {
            message: "searching for elements in multidimensional arrays is not supported".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
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
                compare_values("=", item.clone(), needle.clone(), None)?,
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

pub(crate) fn append_array_value(
    array: &Value,
    element: &Value,
    prepend: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::PgArray(ArrayValue::from_1d(vec![
            element.to_owned_value(),
        ])));
    };
    let element_type_oid = array.element_type_oid;
    if array.ndim() == 0 {
        let mut result = ArrayValue::from_1d(vec![element.clone()]);
        if let Some(element_type_oid) = element_type_oid {
            result = result.with_element_type_oid(element_type_oid);
        }
        return Ok(Value::PgArray(result));
    }

    let mut result = if let Some(element_array) = normalize_array_value(element) {
        concatenate_rank_mismatched_arrays(array, element_array, prepend)?
    } else {
        if array.ndim() != 1 {
            return incompatible_array_concat_error();
        }
        let mut elements = array.elements.clone();
        if prepend {
            elements.insert(0, element.clone());
        } else {
            elements.push(element.clone());
        }
        let mut dimensions = array.dimensions.clone();
        dimensions[0].length += 1;
        ArrayValue::from_dimensions(dimensions, elements)
    };
    if let Some(element_type_oid) = element_type_oid {
        result = result.with_element_type_oid(element_type_oid);
    }
    Ok(Value::PgArray(result))
}

pub(crate) fn concatenate_arrays(
    left: ArrayValue,
    right: ArrayValue,
) -> Result<ArrayValue, ExecError> {
    if left.ndim() == 0 {
        return Ok(right);
    }
    if right.ndim() == 0 {
        return Ok(left);
    }
    if left.ndim() == right.ndim() {
        if left.dimensions[1..] != right.dimensions[1..] {
            return incompatible_array_concat_error();
        }
        let mut dimensions = left.dimensions.clone();
        dimensions[0].length += right.dimensions[0].length;
        let mut elements = left.elements;
        elements.extend(right.elements);
        return Ok(ArrayValue {
            element_type_oid: left.element_type_oid.or(right.element_type_oid),
            dimensions,
            elements,
        });
    }
    if left.ndim() + 1 == right.ndim() {
        return concatenate_rank_mismatched_arrays(right, left, true);
    }
    if right.ndim() + 1 == left.ndim() {
        return concatenate_rank_mismatched_arrays(left, right, false);
    }
    incompatible_array_concat_error()
}

fn concatenate_rank_mismatched_arrays(
    outer: ArrayValue,
    inner: ArrayValue,
    prepend: bool,
) -> Result<ArrayValue, ExecError> {
    if outer.ndim() != inner.ndim() + 1 || outer.dimensions[1..] != inner.dimensions[..] {
        return incompatible_array_concat_error();
    }
    let mut dimensions = outer.dimensions.clone();
    dimensions[0].length += 1;
    let mut elements = Vec::with_capacity(outer.elements.len() + inner.elements.len());
    if prepend {
        elements.extend(inner.elements);
        elements.extend(outer.elements);
    } else {
        elements.extend(outer.elements);
        elements.extend(inner.elements);
    }
    Ok(ArrayValue {
        element_type_oid: outer.element_type_oid.or(inner.element_type_oid),
        dimensions,
        elements,
    })
}

fn incompatible_array_concat_error<T>() -> Result<T, ExecError> {
    Err(ExecError::DetailedError {
        message: "cannot concatenate incompatible arrays".into(),
        detail: None,
        hint: None,
        sqlstate: "2202E",
    })
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
    let Some(array) = normalize_array_for_replace_like(array, search, replace)? else {
        return Ok(Value::Null);
    };
    if array.ndim() > 1 {
        if remove {
            return Err(ExecError::DetailedError {
                message: "removing elements from multidimensional arrays is not supported".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        return Err(ExecError::DetailedError {
            message: "replacing elements in multidimensional arrays is not supported".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let mut items = Vec::new();
    for item in &array.elements {
        let matched = if matches!(search, Value::Null) {
            matches!(item, Value::Null)
        } else if matches!(item, Value::Null) {
            false
        } else {
            matches!(
                compare_values("=", item.clone(), search.clone(), None)?,
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
    if items.is_empty() {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: array.lower_bound(0).unwrap_or(1),
            length: items.len(),
        }],
        items,
    )))
}

fn normalize_array_for_replace_like(
    array: &Value,
    search: &Value,
    replace: Option<&Value>,
) -> Result<Option<ArrayValue>, ExecError> {
    if let Some(array) = normalize_array_value(array) {
        return Ok(Some(array));
    }
    let Some(text) = array.as_text() else {
        return Ok(None);
    };
    let element_type = search
        .sql_type_hint()
        .filter(|ty| !ty.is_array && !matches!(ty.kind, SqlTypeKind::Text))
        .or_else(|| {
            replace.and_then(|value| {
                value
                    .sql_type_hint()
                    .filter(|ty| !ty.is_array && !matches!(ty.kind, SqlTypeKind::Text))
            })
        })
        .unwrap_or(SqlType::new(SqlTypeKind::Text));
    match cast_value(
        Value::Text(text.into()),
        SqlType::array_of(element_type.element_type()),
    )? {
        Value::PgArray(array) => Ok(Some(array)),
        Value::Array(items) => Ok(Some(ArrayValue::from_1d(items))),
        _ => Ok(None),
    }
}

pub(super) fn eval_trim_array_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [array, count] => trim_array_value(array, int4_array_count_arg(count, "trim_array")?),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "trim_array(array, n)",
            actual: format!("TrimArray({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_shuffle_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [array] => normalize_array_value(array)
            .map(Value::PgArray)
            .ok_or_else(|| array_function_type_error("array_shuffle", array.clone())),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_shuffle(array)",
            actual: format!("ArrayShuffle({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_sample_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [array, count] => sample_array_value(array, int4_array_count_arg(count, "array_sample")?),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_sample(array, n)",
            actual: format!("ArraySample({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_array_reverse_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [array] => reverse_array_value(array),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_reverse(array)",
            actual: format!("ArrayReverse({} args)", values.len()),
        })),
    }
}

fn trim_array_value(array: &Value, count: i32) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Err(array_function_type_error("trim_array", array.clone()));
    };
    let len = array.dimensions.first().map(|dim| dim.length).unwrap_or(0);
    let count_usize = validate_array_count("number of elements to trim", count, len)?;
    let keep = len - count_usize;
    Ok(Value::PgArray(array_take_first_axis(array, keep, 1)))
}

fn sample_array_value(array: &Value, count: i32) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Err(array_function_type_error("array_sample", array.clone()));
    };
    let len = array.dimensions.first().map(|dim| dim.length).unwrap_or(0);
    let count_usize = validate_array_count("sample size", count, len)?;
    Ok(Value::PgArray(array_take_first_axis(array, count_usize, 1)))
}

fn reverse_array_value(array: &Value) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Err(array_function_type_error("array_reverse", array.clone()));
    };
    if array.dimensions.is_empty() {
        return Ok(Value::PgArray(array));
    }
    let first_len = array.dimensions[0].length;
    let stride = first_axis_stride(&array);
    let mut elements = Vec::with_capacity(array.elements.len());
    for idx in (0..first_len).rev() {
        let start = idx * stride;
        elements.extend(array.elements[start..start + stride].iter().cloned());
    }
    Ok(Value::PgArray(ArrayValue {
        element_type_oid: array.element_type_oid,
        dimensions: array.dimensions,
        elements,
    }))
}

fn array_take_first_axis(array: ArrayValue, first_len: usize, first_lower: i32) -> ArrayValue {
    if first_len == 0 || array.dimensions.is_empty() {
        return ArrayValue {
            element_type_oid: array.element_type_oid,
            ..ArrayValue::empty()
        };
    }
    let stride = first_axis_stride(&array);
    let mut dimensions = array.dimensions.clone();
    dimensions[0].lower_bound = first_lower;
    dimensions[0].length = first_len;
    let elements = array
        .elements
        .into_iter()
        .take(first_len.saturating_mul(stride))
        .collect();
    ArrayValue {
        element_type_oid: array.element_type_oid,
        dimensions,
        elements,
    }
}

fn first_axis_stride(array: &ArrayValue) -> usize {
    array.dimensions[1..]
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length))
}

fn int4_array_count_arg(value: &Value, op: &'static str) -> Result<i32, ExecError> {
    array_subscript_index(Some(value))?.ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: value.clone(),
        right: Value::Null,
    })
}

fn validate_array_count(label: &'static str, count: i32, max: usize) -> Result<usize, ExecError> {
    if count < 0 || count as usize > max {
        return Err(ExecError::DetailedError {
            message: format!("{label} must be between 0 and {max}"),
            detail: None,
            hint: None,
            sqlstate: "2202E",
        });
    }
    Ok(count as usize)
}

fn array_function_type_error(op: &'static str, value: Value) -> ExecError {
    ExecError::TypeMismatch {
        op,
        left: value,
        right: Value::Null,
    }
}

pub(super) fn eval_array_sort_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] | [Value::Null, ..] => Ok(Value::Null),
        [array] => array_sort_value(array, false, false),
        [array, Value::Bool(desc)] => array_sort_value(array, *desc, *desc),
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
    if array.element_type_oid == Some(crate::include::catalog::XID_TYPE_OID)
        && array.elements.len() > 1
    {
        return Err(ExecError::DetailedError {
            message: "could not identify a comparison function for type xid".into(),
            detail: None,
            hint: None,
            sqlstate: "42883",
        });
    }
    if array.ndim() == 1 {
        let mut items = array.elements.clone();
        let mut sort_error = None;
        items.sort_by(|left, right| {
            match compare_array_sort_values(left, right, nulls_first, descending) {
                Ok(ordering) => ordering,
                Err(err) => {
                    if sort_error.is_none() {
                        sort_error = Some(err);
                    }
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(err) = sort_error {
            return Err(err);
        }
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
    let mut sort_error = None;
    slices.sort_by(|left, right| {
        match compare_array_sort_values(left, right, nulls_first, descending) {
            Ok(ordering) => ordering,
            Err(err) => {
                if sort_error.is_none() {
                    sort_error = Some(err);
                }
                std::cmp::Ordering::Equal
            }
        }
    });
    if let Some(err) = sort_error {
        return Err(err);
    }
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

fn compare_array_sort_values(
    left: &Value,
    right: &Value,
    nulls_first: bool,
    descending: bool,
) -> Result<std::cmp::Ordering, ExecError> {
    let ordering = compare_order_values(left, right, None, Some(nulls_first), false)?;
    if descending && !matches!((left, right), (Value::Null, _) | (_, Value::Null)) {
        Ok(ordering.reverse())
    } else {
        Ok(ordering)
    }
}

#[derive(Clone, Copy)]
enum ArrayFillArgKind {
    Dimension,
    LowerBound,
}

fn parse_int_array_argument(
    op: &'static str,
    value: &Value,
    kind: ArrayFillArgKind,
) -> Result<Vec<Option<i32>>, ExecError> {
    let array = match normalize_array_value(value) {
        Some(array) => array,
        None if value.as_text().is_some() => {
            let text = value.as_text().unwrap();
            match cast_value(
                Value::Text(text.into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            )? {
                Value::PgArray(array) => array,
                Value::Array(items) => ArrayValue::from_1d(items),
                other => {
                    return Err(ExecError::TypeMismatch {
                        op,
                        left: other,
                        right: Value::Null,
                    });
                }
            }
        }
        None => {
            return Err(ExecError::TypeMismatch {
                op,
                left: value.clone(),
                right: Value::Null,
            });
        }
    };
    if array.ndim() > 1 {
        return Err(match kind {
            ArrayFillArgKind::Dimension => ExecError::DetailedError {
                message: "wrong number of array subscripts".into(),
                detail: Some("Dimension array must be one dimensional.".into()),
                hint: None,
                sqlstate: "2202E",
            },
            ArrayFillArgKind::LowerBound => array_fill_low_bound_mismatch_error(),
        });
    }
    array
        .elements
        .iter()
        .map(|item| array_subscript_index(Some(item)))
        .collect()
}

fn array_fill_null_array_error() -> ExecError {
    ExecError::DetailedError {
        message: "dimension array or low bound array cannot be null".into(),
        detail: None,
        hint: None,
        sqlstate: "22004",
    }
}

fn array_fill_low_bound_mismatch_error() -> ExecError {
    ExecError::DetailedError {
        message: "wrong number of array subscripts".into(),
        detail: Some("Low bound array has different size than dimensions array.".into()),
        hint: None,
        sqlstate: "2202E",
    }
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
            let thresholds = normalize_width_bucket_thresholds(operand, thresholds)?;
            if thresholds.elements.is_empty() {
                return Ok(Value::Int32(0));
            }
            if thresholds.ndim() != 1 {
                return Err(ExecError::DetailedError {
                    message: "thresholds must be one-dimensional array".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "2202E",
                });
            }
            if thresholds
                .elements
                .iter()
                .any(|value| matches!(value, Value::Null))
            {
                return Err(ExecError::DetailedError {
                    message: "thresholds array must not contain NULLs".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22004",
                });
            }
            let mut bucket = 0i32;
            for threshold in &thresholds.elements {
                if matches!(
                    order_values("<", operand.clone(), threshold.clone(), None)?,
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

fn normalize_width_bucket_thresholds(
    operand: &Value,
    thresholds: &Value,
) -> Result<ArrayValue, ExecError> {
    if let Some(array) = normalize_array_value(thresholds) {
        return Ok(array);
    }
    if let Some(text) = thresholds.as_text() {
        let element_type = operand
            .sql_type_hint()
            .filter(|ty| !ty.is_array && !matches!(ty.kind, SqlTypeKind::Text))
            .unwrap_or(SqlType::new(SqlTypeKind::Int4));
        return match cast_value(Value::Text(text.into()), SqlType::array_of(element_type))? {
            Value::PgArray(array) => Ok(array),
            Value::Array(items) => Ok(ArrayValue::from_1d(items)),
            other => Err(ExecError::TypeMismatch {
                op: "width_bucket",
                left: operand.clone(),
                right: other,
            }),
        };
    }
    Err(ExecError::TypeMismatch {
        op: "width_bucket",
        left: operand.clone(),
        right: thresholds.clone(),
    })
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

fn array_slice_bound_index(value: Option<&Value>, provided: bool) -> Result<SliceBound, ExecError> {
    if !provided {
        return Ok(SliceBound::Omitted);
    }
    match value {
        None | Some(Value::Null) => Ok(SliceBound::Null),
        Some(Value::Int16(v)) => Ok(SliceBound::Value(*v as i32)),
        Some(Value::Int32(v)) => Ok(SliceBound::Value(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v)
            .map(SliceBound::Value)
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
                compare_values("=", left_item.clone(), right_item.clone(), None)?,
                Value::Bool(true)
            ) {
                return Ok(Value::Bool(true));
            }
        }
    }
    Ok(Value::Bool(false))
}

pub(super) fn eval_array_contains(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_array_contains_internal("@>", left, right)
}

pub(super) fn eval_array_contained(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_array_contains_internal("<@", right, left)
}

fn eval_array_contains_internal(
    op: &'static str,
    left: Value,
    right: Value,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(left_array) = normalize_array_value(&left) else {
        return Err(ExecError::TypeMismatch {
            op,
            left,
            right: right.clone(),
        });
    };
    let Some(right_array) = normalize_array_value(&right) else {
        return Err(ExecError::TypeMismatch { op, left, right });
    };
    for right_item in &right_array.elements {
        if matches!(right_item, Value::Null) {
            return Ok(Value::Bool(false));
        }
        let mut matched = false;
        for left_item in &left_array.elements {
            if matches!(left_item, Value::Null) {
                continue;
            }
            if matches!(
                compare_values("=", left_item.clone(), right_item.clone(), None)?,
                Value::Bool(true)
            ) {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(Value::Bool(false));
        }
    }
    Ok(Value::Bool(true))
}

#[cfg(test)]
mod tests {
    use super::eval_quantified_array;
    use crate::backend::executor::ExecError;
    use crate::backend::parser::SubqueryComparisonOp;
    use crate::include::catalog::C_COLLATION_OID;
    use crate::include::nodes::datum::{ArrayValue, Value};

    #[test]
    fn eval_quantified_array_accepts_builtin_collation() {
        let array = Value::PgArray(ArrayValue::from_1d(vec![
            Value::Text("alpha".into()),
            Value::Text("beta".into()),
        ]));
        assert_eq!(
            eval_quantified_array(
                &Value::Text("alpha".into()),
                SubqueryComparisonOp::Eq,
                Some(C_COLLATION_OID),
                false,
                &array,
            )
            .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn eval_quantified_array_rejects_unsupported_collation_oid() {
        let array = Value::PgArray(ArrayValue::from_1d(vec![Value::Text("alpha".into())]));
        assert!(matches!(
            eval_quantified_array(
                &Value::Text("alpha".into()),
                SubqueryComparisonOp::Eq,
                Some(123_456),
                false,
                &array,
            ),
            Err(ExecError::DetailedError { sqlstate, .. }) if sqlstate == "0A000"
        ));
    }
}
