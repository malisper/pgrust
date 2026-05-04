use super::ExecError;
use super::jsonb::{
    JsonbValue, RawJsonValue, decode_json_string_text, decode_jsonb, encode_jsonb,
    parse_json_text_input, parse_jsonb_text, validate_json_text_input,
};
use super::jsonpath::canonicalize_jsonpath;
use pgrust_core::CompactString;
use pgrust_nodes::datum::Value;
use serde_json::Value as SerdeJsonValue;

pub fn validate_json_text(text: &str) -> Result<(), ExecError> {
    validate_json_text_input(text)
}

pub fn canonicalize_jsonpath_text(text: &str) -> Result<CompactString, ExecError> {
    canonicalize_jsonpath(text).map(CompactString::from_owned)
}

fn parse_json_text(text: &str) -> Result<SerdeJsonValue, ExecError> {
    parse_json_text_input(text)
}

fn parse_jsonb_target(value: &Value, op: &'static str) -> Result<JsonbValue, ExecError> {
    match value {
        Value::Null => Ok(JsonbValue::Null),
        Value::Jsonb(bytes) => decode_jsonb(bytes),
        Value::Json(text) => JsonbValue::from_serde(parse_json_text(text.as_str())?),
        Value::Text(text) => decode_jsonb(&parse_jsonb_text(text.as_str())?),
        Value::TextRef(_, _) => decode_jsonb(&parse_jsonb_text(value.as_text().unwrap())?),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub fn strip_json_nulls(value: &SerdeJsonValue, strip_in_arrays: bool) -> SerdeJsonValue {
    match value {
        SerdeJsonValue::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                if matches!(value, SerdeJsonValue::Null) {
                    continue;
                }
                out.insert(key.clone(), strip_json_nulls(value, strip_in_arrays));
            }
            SerdeJsonValue::Object(out)
        }
        SerdeJsonValue::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                if strip_in_arrays && matches!(item, SerdeJsonValue::Null) {
                    continue;
                }
                out.push(strip_json_nulls(item, strip_in_arrays));
            }
            SerdeJsonValue::Array(out)
        }
        other => other.clone(),
    }
}

pub fn strip_jsonb_nulls(value: &JsonbValue, strip_in_arrays: bool) -> JsonbValue {
    match value {
        JsonbValue::Object(items) => JsonbValue::Object(
            items
                .iter()
                .filter_map(|(key, value)| {
                    (!matches!(value, JsonbValue::Null))
                        .then_some((key.clone(), strip_jsonb_nulls(value, strip_in_arrays)))
                })
                .collect(),
        ),
        JsonbValue::Array(items) => JsonbValue::Array(
            items
                .iter()
                .filter_map(|item| {
                    if strip_in_arrays && matches!(item, JsonbValue::Null) {
                        None
                    } else {
                        Some(strip_jsonb_nulls(item, strip_in_arrays))
                    }
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

pub fn apply_jsonb_delete(target: &JsonbValue, key: &Value) -> Result<JsonbValue, ExecError> {
    Ok(match key {
        Value::Text(_) | Value::TextRef(_, _) => {
            let key = key.as_text().unwrap();
            match target {
                JsonbValue::Object(items) => JsonbValue::Object(
                    items
                        .iter()
                        .filter(|(name, _)| name != key)
                        .cloned()
                        .collect(),
                ),
                JsonbValue::Array(items) => JsonbValue::Array(
                    items
                        .iter()
                        .filter(|item| !matches!(item, JsonbValue::String(text) if text == key))
                        .cloned()
                        .collect(),
                ),
                JsonbValue::Null
                | JsonbValue::String(_)
                | JsonbValue::Numeric(_)
                | JsonbValue::Bool(_)
                | JsonbValue::Date(_)
                | JsonbValue::Time(_)
                | JsonbValue::TimeTz(_)
                | JsonbValue::Timestamp(_)
                | JsonbValue::TimestampTz(_)
                | JsonbValue::TimestampTzWithOffset(_, _) => {
                    return Err(ExecError::InvalidStorageValue {
                        column: "jsonb".into(),
                        details: "cannot delete from scalar".into(),
                    });
                }
            }
        }
        Value::Int16(index) => delete_jsonb_array_index(target, i32::from(*index))?,
        Value::Int32(index) => delete_jsonb_array_index(target, *index)?,
        Value::Int64(index) => {
            delete_jsonb_array_index(target, i32::try_from(*index).unwrap_or(i32::MIN))?
        }
        Value::Array(keys) => {
            let mut result = target.clone();
            for key in keys {
                let text = match key {
                    Value::Null => continue,
                    Value::Text(_) | Value::TextRef(_, _) => key.as_text().unwrap(),
                    other => {
                        return Err(ExecError::TypeMismatch {
                            op: "jsonb_delete",
                            left: other.clone(),
                            right: Value::Null,
                        });
                    }
                };
                result = apply_jsonb_delete(&result, &Value::Text(CompactString::new(text)))?;
            }
            result
        }
        Value::PgArray(keys) => {
            let mut result = target.clone();
            for key in &keys.elements {
                let text = match key {
                    Value::Null => continue,
                    Value::Text(_) | Value::TextRef(_, _) => key.as_text().unwrap(),
                    other => {
                        return Err(ExecError::TypeMismatch {
                            op: "jsonb_delete",
                            left: other.clone(),
                            right: Value::Null,
                        });
                    }
                };
                result = apply_jsonb_delete(&result, &Value::Text(CompactString::new(text)))?;
            }
            result
        }
        other => {
            return Err(ExecError::TypeMismatch {
                op: "jsonb_delete",
                left: other.clone(),
                right: Value::Null,
            });
        }
    })
}

fn delete_jsonb_array_index(target: &JsonbValue, index: i32) -> Result<JsonbValue, ExecError> {
    let JsonbValue::Array(items) = target else {
        return match target {
            JsonbValue::Object(_) => Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "cannot delete from object using integer index".into(),
            }),
            JsonbValue::Null
            | JsonbValue::String(_)
            | JsonbValue::Numeric(_)
            | JsonbValue::Bool(_)
            | JsonbValue::Date(_)
            | JsonbValue::Time(_)
            | JsonbValue::TimeTz(_)
            | JsonbValue::Timestamp(_)
            | JsonbValue::TimestampTz(_)
            | JsonbValue::TimestampTzWithOffset(_, _) => Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "cannot delete from scalar".into(),
            }),
            JsonbValue::Array(_) => unreachable!(),
        };
    };
    let Some(index) = normalize_array_index(items.len(), index) else {
        return Ok(JsonbValue::Array(items.clone()));
    };
    let mut out = items.clone();
    out.remove(index);
    Ok(JsonbValue::Array(out))
}

pub fn delete_jsonb_path(
    target: &JsonbValue,
    path: &[Option<String>],
) -> Result<JsonbValue, ExecError> {
    if path.is_empty() {
        return Ok(target.clone());
    }
    validate_jsonb_path_not_null(path)?;
    delete_jsonb_path_inner(target, path, 0)
}

fn delete_jsonb_path_inner(
    target: &JsonbValue,
    path: &[Option<String>],
    path_index: usize,
) -> Result<JsonbValue, ExecError> {
    let step = path[0].as_ref().unwrap();
    if path.len() == 1 {
        return Ok(match target {
            JsonbValue::Object(items) => JsonbValue::Object(
                items
                    .iter()
                    .filter(|(key, _)| key != step)
                    .cloned()
                    .collect(),
            ),
            JsonbValue::Array(items) => {
                let index = if path_index == 0 {
                    parse_optional_jsonb_path_array_index(step, items.len())
                } else {
                    parse_jsonb_path_array_index(step, items.len(), path_index + 1)?
                };
                let Some(index) = index else {
                    return Ok(JsonbValue::Array(items.clone()));
                };
                let mut out = items.clone();
                out.remove(index);
                JsonbValue::Array(out)
            }
            JsonbValue::Null
            | JsonbValue::String(_)
            | JsonbValue::Numeric(_)
            | JsonbValue::Bool(_)
            | JsonbValue::Date(_)
            | JsonbValue::Time(_)
            | JsonbValue::TimeTz(_)
            | JsonbValue::Timestamp(_)
            | JsonbValue::TimestampTz(_)
            | JsonbValue::TimestampTzWithOffset(_, _) => {
                return Err(ExecError::InvalidStorageValue {
                    column: "jsonb".into(),
                    details: "cannot delete path in scalar".into(),
                });
            }
        });
    }
    Ok(match target {
        JsonbValue::Object(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (key, value) in items {
                if key == step {
                    out.push((
                        key.clone(),
                        delete_jsonb_path_inner(value, &path[1..], path_index + 1)?,
                    ));
                } else {
                    out.push((key.clone(), value.clone()));
                }
            }
            JsonbValue::Object(out)
        }
        JsonbValue::Array(items) => {
            let Some(index) = parse_jsonb_path_array_index(step, items.len(), path_index + 1)?
            else {
                return Ok(JsonbValue::Array(items.clone()));
            };
            let mut out = items.clone();
            out[index] = delete_jsonb_path_inner(&out[index], &path[1..], path_index + 1)?;
            JsonbValue::Array(out)
        }
        JsonbValue::Null
        | JsonbValue::String(_)
        | JsonbValue::Numeric(_)
        | JsonbValue::Bool(_)
        | JsonbValue::Date(_)
        | JsonbValue::Time(_)
        | JsonbValue::TimeTz(_)
        | JsonbValue::Timestamp(_)
        | JsonbValue::TimestampTz(_)
        | JsonbValue::TimestampTzWithOffset(_, _) => {
            return Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "cannot delete path in scalar".into(),
            });
        }
    })
}

pub fn set_jsonb_path(
    target: &JsonbValue,
    path: &[Option<String>],
    replacement: JsonbValue,
    create_missing: bool,
    insert_after: bool,
    insert_mode: bool,
) -> Result<JsonbValue, ExecError> {
    if path.is_empty() {
        return Ok(target.clone());
    }
    validate_jsonb_path_not_null(path)?;
    set_jsonb_path_inner(
        target,
        path,
        0,
        replacement,
        create_missing,
        insert_after,
        insert_mode,
    )
}

fn jsonb_insert_existing_key_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot replace existing key".into(),
        detail: None,
        hint: Some("Try using the function jsonb_set to replace key value.".into()),
        sqlstate: "22023",
    }
}

fn set_jsonb_path_inner(
    target: &JsonbValue,
    path: &[Option<String>],
    path_index: usize,
    replacement: JsonbValue,
    create_missing: bool,
    insert_after: bool,
    insert_mode: bool,
) -> Result<JsonbValue, ExecError> {
    let step = path[0].as_ref().unwrap();
    if path.len() == 1 {
        return match target {
            JsonbValue::Object(items) => {
                let mut out = items.clone();
                if let Some((_, value)) = out.iter_mut().find(|(key, _)| key == step) {
                    if insert_mode {
                        return Err(jsonb_insert_existing_key_error());
                    }
                    *value = replacement;
                } else if create_missing {
                    out.push((step.clone(), replacement));
                }
                Ok(JsonbValue::Object(out))
            }
            JsonbValue::Array(items) => {
                let mut out = items.clone();
                match parse_array_insert_target(step, items.len(), path_index + 1)? {
                    Some((index, in_range)) => {
                        if insert_mode {
                            let insert_at = if insert_after && in_range {
                                index + 1
                            } else {
                                index
                            };
                            out.insert(insert_at.min(out.len()), replacement);
                        } else if insert_after {
                            let insert_at = if in_range { index + 1 } else { index };
                            out.insert(insert_at.min(out.len()), replacement);
                        } else if in_range {
                            out[index] = replacement;
                        } else if create_missing {
                            out.insert(index.min(out.len()), replacement);
                        }
                        Ok(JsonbValue::Array(out))
                    }
                    None => Ok(JsonbValue::Array(out)),
                }
            }
            _ => Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "cannot set path in scalar".into(),
            }),
        };
    }

    match target {
        JsonbValue::Object(items) => {
            let mut out = items.clone();
            if let Some((_, value)) = out.iter_mut().find(|(key, _)| key == step) {
                *value = set_jsonb_path_inner(
                    value,
                    &path[1..],
                    path_index + 1,
                    replacement,
                    create_missing,
                    insert_after,
                    insert_mode,
                )?;
                Ok(JsonbValue::Object(out))
            } else {
                Ok(JsonbValue::Object(out))
            }
        }
        JsonbValue::Array(items) => {
            let Some(index) = parse_jsonb_path_array_index(step, items.len(), path_index + 1)?
            else {
                return Ok(JsonbValue::Array(items.clone()));
            };
            let mut out = items.clone();
            out[index] = set_jsonb_path_inner(
                &out[index],
                &path[1..],
                path_index + 1,
                replacement,
                create_missing,
                insert_after,
                insert_mode,
            )?;
            Ok(JsonbValue::Array(out))
        }
        _ => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "cannot set path in scalar".into(),
        }),
    }
}

fn normalize_array_index(len: usize, index: i32) -> Option<usize> {
    let len_i32 = i32::try_from(len).ok()?;
    let idx = if index < 0 { len_i32 + index } else { index };
    if idx < 0 || idx >= len_i32 {
        None
    } else {
        usize::try_from(idx).ok()
    }
}

fn validate_jsonb_path_not_null(path: &[Option<String>]) -> Result<(), ExecError> {
    if let Some(position) = path.iter().position(|step| step.is_none()) {
        return Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: format!("path element at position {} is null", position + 1),
        });
    }
    Ok(())
}

fn parse_jsonb_path_array_index(
    step: &str,
    len: usize,
    remaining_path_len: usize,
) -> Result<Option<usize>, ExecError> {
    let index = step
        .parse::<i32>()
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: format!(
                "path element at position {} is not an integer: \"{}\"",
                remaining_path_len, step
            ),
        })?;
    Ok(normalize_array_index(len, index))
}

fn parse_optional_jsonb_path_array_index(step: &str, len: usize) -> Option<usize> {
    step.parse::<i32>()
        .ok()
        .and_then(|index| normalize_array_index(len, index))
}

#[derive(Debug, Clone)]
enum JsonbAssignmentStep {
    Key(String),
    Index(i32),
}

pub fn apply_jsonb_subscript_assignment(
    target: &Value,
    subscripts: &[Value],
    new_value: &Value,
) -> Result<Value, ExecError> {
    let replacement = parse_jsonb_target(new_value, "jsonb subscript assignment")?;
    let updated = if matches!(target, Value::Null) && !subscripts.is_empty() {
        let seed = seed_container_for_assignment(&subscripts[0]);
        assign_jsonb_subscripts(&seed, subscripts, replacement)?
    } else {
        let target = parse_jsonb_target(target, "jsonb subscript assignment")?;
        assign_jsonb_subscripts(&target, subscripts, replacement)?
    };
    Ok(Value::Jsonb(encode_jsonb(&updated)))
}

fn assign_jsonb_subscripts(
    target: &JsonbValue,
    subscripts: &[Value],
    replacement: JsonbValue,
) -> Result<JsonbValue, ExecError> {
    if subscripts.is_empty() {
        return Ok(replacement);
    }
    assign_jsonb_subscripts_inner(target, subscripts, 0, replacement)
}

fn assign_jsonb_subscripts_inner(
    target: &JsonbValue,
    subscripts: &[Value],
    position: usize,
    replacement: JsonbValue,
) -> Result<JsonbValue, ExecError> {
    let step = parse_assignment_step(target, &subscripts[position])?;
    let last = position + 1 == subscripts.len();
    match (target, step) {
        (JsonbValue::Object(items), JsonbAssignmentStep::Key(key)) => {
            let mut out = items.clone();
            let value = if last {
                replacement
            } else if let Some((_, existing)) =
                out.iter().find(|(existing_key, _)| *existing_key == key)
            {
                assign_jsonb_subscripts_inner(existing, subscripts, position + 1, replacement)?
            } else {
                let seed = seed_container_for_assignment(&subscripts[position + 1]);
                assign_jsonb_subscripts_inner(&seed, subscripts, position + 1, replacement)?
            };
            if let Some((_, existing)) = out
                .iter_mut()
                .find(|(existing_key, _)| *existing_key == key)
            {
                *existing = value;
            } else {
                out.push((key, value));
            }
            Ok(JsonbValue::Object(out))
        }
        (JsonbValue::Array(items), JsonbAssignmentStep::Index(index)) => {
            assign_jsonb_array_index(items, index, subscripts, position, replacement)
        }
        (JsonbValue::Array(_), JsonbAssignmentStep::Key(_)) => {
            Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "array subscript must be integer".into(),
            })
        }
        (_, _) => Err(jsonb_scalar_assignment_error()),
    }
}

fn jsonb_scalar_assignment_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot replace existing key".into(),
        detail: Some(
            "The path assumes key is a composite object, but it is a scalar value.".into(),
        ),
        hint: None,
        sqlstate: "22023",
    }
}

fn assign_jsonb_array_index(
    items: &[JsonbValue],
    index: i32,
    subscripts: &[Value],
    position: usize,
    replacement: JsonbValue,
) -> Result<JsonbValue, ExecError> {
    let len = items.len();
    let len_i32 = i32::try_from(len).unwrap_or(i32::MAX);
    if index < 0 && index + len_i32 < 0 {
        return Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: format!(
                "path element at position {} is out of range: {}",
                position + 1,
                index
            ),
        });
    }
    let target_index = if index < 0 {
        usize::try_from(len_i32 + index).unwrap_or(0)
    } else {
        usize::try_from(index).unwrap_or(usize::MAX)
    };
    let mut out = items.to_vec();
    while out.len() < target_index {
        out.push(JsonbValue::Null);
    }
    if position + 1 == subscripts.len() {
        if target_index < out.len() {
            out[target_index] = replacement;
        } else {
            out.push(replacement);
        }
        return Ok(JsonbValue::Array(out));
    }
    let seed = if target_index < out.len() {
        out[target_index].clone()
    } else {
        seed_container_for_assignment(&subscripts[position + 1])
    };
    let updated = assign_jsonb_subscripts_inner(&seed, subscripts, position + 1, replacement)?;
    if target_index < out.len() {
        out[target_index] = updated;
    } else {
        out.push(updated);
    }
    Ok(JsonbValue::Array(out))
}

fn parse_assignment_step(
    target: &JsonbValue,
    value: &Value,
) -> Result<JsonbAssignmentStep, ExecError> {
    match value {
        Value::Null => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "jsonb subscript in assignment must not be null".into(),
        }),
        Value::Text(text) => Ok(JsonbAssignmentStep::Key(text.to_string())),
        Value::TextRef(_, _) => Ok(JsonbAssignmentStep::Key(
            value.as_text().unwrap().to_string(),
        )),
        Value::Int16(v) => Ok(match target {
            JsonbValue::Object(_) => JsonbAssignmentStep::Key(v.to_string()),
            _ => JsonbAssignmentStep::Index(i32::from(*v)),
        }),
        Value::Int32(v) => Ok(match target {
            JsonbValue::Object(_) => JsonbAssignmentStep::Key(v.to_string()),
            _ => JsonbAssignmentStep::Index(*v),
        }),
        Value::Int64(v) => Ok(match target {
            JsonbValue::Object(_) => JsonbAssignmentStep::Key(v.to_string()),
            _ => JsonbAssignmentStep::Index(i32::try_from(*v).unwrap_or(i32::MIN)),
        }),
        other => Err(ExecError::TypeMismatch {
            op: "jsonb subscript assignment",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn seed_container_for_assignment(next: &Value) -> JsonbValue {
    match next {
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => JsonbValue::Array(Vec::new()),
        _ => JsonbValue::Object(Vec::new()),
    }
}

fn parse_array_insert_target(
    step: &str,
    len: usize,
    remaining_path_len: usize,
) -> Result<Option<(usize, bool)>, ExecError> {
    let index = step
        .parse::<i32>()
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: format!(
                "path element at position {} is not an integer: \"{}\"",
                remaining_path_len, step
            ),
        })?;
    let Some(len_i32) = i32::try_from(len).ok() else {
        return Ok(None);
    };
    if index < 0 {
        let idx = len_i32 + index;
        if idx < 0 {
            Ok(Some((0, false)))
        } else if idx >= len_i32 {
            Ok(Some((len, false)))
        } else {
            Ok(usize::try_from(idx).ok().map(|idx| (idx, true)))
        }
    } else if index >= len_i32 {
        Ok(Some((len, false)))
    } else {
        Ok(usize::try_from(index).ok().map(|idx| (idx, true)))
    }
}

pub fn parse_json_path_value(
    value: &Value,
    op: &'static str,
    left: Value,
) -> Result<Vec<String>, ExecError> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Text(_) | Value::TextRef(_, _) => Ok(item.as_text().unwrap().to_string()),
                Value::Null => Ok(String::new()),
                other => Err(ExecError::TypeMismatch {
                    op,
                    left: left.clone(),
                    right: other.clone(),
                }),
            })
            .collect(),
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|item| match item {
                Value::Text(_) | Value::TextRef(_, _) => Ok(item.as_text().unwrap().to_string()),
                Value::Null => Ok(String::new()),
                other => Err(ExecError::TypeMismatch {
                    op,
                    left: left.clone(),
                    right: other.clone(),
                }),
            })
            .collect(),
        other => Err(ExecError::TypeMismatch {
            op,
            left,
            right: other.clone(),
        }),
    }
}

pub fn json_lookup_index<'a>(json: &'a SerdeJsonValue, index: i32) -> Option<&'a SerdeJsonValue> {
    let items = match json {
        SerdeJsonValue::Array(items) => items,
        _ => return None,
    };
    let len = items.len() as i32;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 {
        None
    } else {
        items.get(idx as usize)
    }
}

pub fn json_lookup_path<'a>(
    json: &'a SerdeJsonValue,
    path: &[String],
) -> Option<&'a SerdeJsonValue> {
    let mut current = json;
    for step in path {
        current = match current {
            SerdeJsonValue::Object(map) => map.get(step)?,
            SerdeJsonValue::Array(_) => {
                let index = step.parse::<i32>().ok()?;
                json_lookup_index(current, index)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn json_value_to_text(value: &SerdeJsonValue) -> Option<String> {
    match value {
        SerdeJsonValue::Null => None,
        SerdeJsonValue::String(text) => Some(text.clone()),
        other => Some(render_serde_json_value_text(other)),
    }
}

pub fn raw_json_result_to_value(
    value: Option<&RawJsonValue<'_>>,
    as_text: bool,
) -> Result<Value, ExecError> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    if as_text {
        if value.is_null() {
            return Ok(Value::Null);
        }
        if value.is_string() {
            return Ok(Value::Text(CompactString::from_owned(
                decode_json_string_text(value.raw_text())?,
            )));
        }
        Ok(Value::Text(CompactString::from_owned(
            value.raw_text().to_string(),
        )))
    } else {
        Ok(Value::Json(CompactString::new(value.raw_text())))
    }
}

pub fn json_value_to_value(
    value: &SerdeJsonValue,
    as_text: bool,
    render_jsonb_style: bool,
) -> Value {
    if as_text {
        json_value_to_text(value)
            .map(|text| Value::Text(CompactString::from_owned(text)))
            .unwrap_or(Value::Null)
    } else if render_jsonb_style {
        Value::Json(CompactString::from_owned(
            render_serde_json_value_text_with_jsonb_spacing(value),
        ))
    } else {
        Value::Json(CompactString::from_owned(render_serde_json_value_text(
            value,
        )))
    }
}

pub fn render_serde_json_value_text_with_jsonb_spacing(value: &SerdeJsonValue) -> String {
    match value {
        SerdeJsonValue::Array(items) => {
            let mut out = String::from("[");
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                out.push_str(&render_serde_json_value_text_with_jsonb_spacing(item));
            }
            out.push(']');
            out
        }
        SerdeJsonValue::Object(map) => {
            let mut out = String::from("{");
            for (idx, (key, value)) in map.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                out.push_str(&serde_json::to_string(key).unwrap());
                out.push_str(": ");
                out.push_str(&render_serde_json_value_text_with_jsonb_spacing(value));
            }
            out.push('}');
            out
        }
        _ => render_serde_json_value_text(value),
    }
}

pub fn render_serde_json_value_text(value: &SerdeJsonValue) -> String {
    match value {
        SerdeJsonValue::Null => "null".into(),
        SerdeJsonValue::Bool(true) => "true".into(),
        SerdeJsonValue::Bool(false) => "false".into(),
        SerdeJsonValue::Number(number) => number.to_string(),
        SerdeJsonValue::String(text) => serde_json::to_string(text).unwrap(),
        SerdeJsonValue::Array(items) => {
            let mut out = String::from("[");
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push_str(&render_serde_json_value_text(item));
            }
            out.push(']');
            out
        }
        SerdeJsonValue::Object(map) => {
            let mut out = String::from("{");
            for (idx, (key, value)) in map.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap());
                out.push(':');
                out.push_str(&render_serde_json_value_text(value));
            }
            out.push('}');
            out
        }
    }
}

pub fn float_json_scalar_text(value: f64) -> String {
    if value.is_finite() {
        return value.to_string();
    }
    if value.is_nan() {
        "NaN".into()
    } else if value.is_sign_positive() {
        "Infinity".into()
    } else {
        "-Infinity".into()
    }
}

pub fn render_float_json_text(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        serde_json::to_string(&float_json_scalar_text(value)).unwrap()
    }
}

pub fn float_json_serde_value(value: f64) -> SerdeJsonValue {
    serde_json::Number::from_f64(value)
        .map(SerdeJsonValue::Number)
        .unwrap_or_else(|| SerdeJsonValue::String(float_json_scalar_text(value)))
}
