use super::exec_expr::eval_expr;
use super::node_types::*;
use super::{ExecError, ExecutorContext};
use crate::backend::executor::jsonb::{
    JsonbValue, decode_jsonb, encode_jsonb, jsonb_builder_key, jsonb_from_value, jsonb_get,
    jsonb_object_from_pairs, jsonb_path, jsonb_to_text_value, jsonb_to_value, parse_jsonb_text,
    render_jsonb_bytes,
};
use crate::backend::executor::jsonpath::{
    EvaluationContext as JsonPathEvaluationContext, canonicalize_jsonpath, evaluate_jsonpath,
    parse_jsonpath, validate_jsonpath,
};
use crate::include::nodes::plannodes::BuiltinScalarFunction;
use crate::pgrust::compact_string::CompactString;
use serde_json::Value as SerdeJsonValue;

pub(crate) fn validate_json_text(text: &str) -> Result<(), ExecError> {
    serde_json::from_str::<SerdeJsonValue>(text)
        .map(|_| ())
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "json".into(),
            details: format!("invalid input syntax for type json: \"{text}\""),
        })
}

fn parse_json_text(text: &str) -> Result<SerdeJsonValue, ExecError> {
    serde_json::from_str::<SerdeJsonValue>(text).map_err(|_| ExecError::InvalidStorageValue {
        column: "json".into(),
        details: format!("invalid input syntax for type json: \"{text}\""),
    })
}

fn validate_jsonpath_text(text: &str) -> Result<(), ExecError> {
    validate_jsonpath(text).map_err(|_| ExecError::InvalidStorageValue {
        column: "jsonpath".into(),
        details: format!("invalid input syntax for type jsonpath: \"{text}\""),
    })
}

pub(crate) fn canonicalize_jsonpath_text(text: &str) -> Result<CompactString, ExecError> {
    canonicalize_jsonpath(text)
        .map(CompactString::from_owned)
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "jsonpath".into(),
            details: format!("invalid input syntax for type jsonpath: \"{text}\""),
        })
}

enum ParsedJsonValue {
    Json(SerdeJsonValue),
    Jsonb(JsonbValue),
}

impl ParsedJsonValue {
    fn from_value(value: &Value) -> Result<Self, ExecError> {
        match value {
            Value::Json(text) => Ok(Self::Json(parse_json_text(text.as_str())?)),
            Value::Jsonb(bytes) => Ok(Self::Jsonb(decode_jsonb(bytes)?)),
            Value::Text(text) => Ok(Self::Json(parse_json_text(text.as_str())?)),
            Value::TextRef(_, _) => Ok(Self::Json(parse_json_text(value.as_text().unwrap())?)),
            other => Err(ExecError::TypeMismatch {
                op: "json",
                left: other.clone(),
                right: Value::Null,
            }),
        }
    }

    fn typeof_name(&self) -> &'static str {
        match self {
            Self::Json(value) => match value {
                SerdeJsonValue::Null => "null",
                SerdeJsonValue::Bool(_) => "boolean",
                SerdeJsonValue::Number(_) => "number",
                SerdeJsonValue::String(_) => "string",
                SerdeJsonValue::Array(_) => "array",
                SerdeJsonValue::Object(_) => "object",
            },
            Self::Jsonb(value) => match value {
                JsonbValue::Null => "null",
                JsonbValue::Bool(_) => "boolean",
                JsonbValue::Numeric(_) => "number",
                JsonbValue::String(_) => "string",
                JsonbValue::Array(_) => "array",
                JsonbValue::Object(_) => "object",
            },
        }
    }
}

pub(crate) fn eval_json_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    let eval = || -> Result<Value, ExecError> {
        match func {
        BuiltinScalarFunction::ToJson => {
            let value = values.first().cloned().unwrap_or(Value::Null);
            Ok(Value::Json(CompactString::from_owned(value_to_json_text(
                &value, false,
            ))))
        }
        BuiltinScalarFunction::ToJsonb => {
            let value = values.first().cloned().unwrap_or(Value::Null);
            Ok(Value::Jsonb(encode_jsonb(&jsonb_from_value(&value)?)))
        }
        BuiltinScalarFunction::ArrayToJson => {
            let value = values.first().cloned().unwrap_or(Value::Null);
            let pretty = values
                .get(1)
                .and_then(|value| match value {
                    Value::Bool(v) => Some(*v),
                    _ => None,
                })
                .unwrap_or(false);
            Ok(Value::Json(CompactString::from_owned(value_to_json_text(
                &value, pretty,
            ))))
        }
        BuiltinScalarFunction::JsonBuildArray => Ok(Value::Json(CompactString::from_owned(
            render_json_builder_array(values),
        ))),
        BuiltinScalarFunction::JsonBuildObject => Ok(Value::Json(CompactString::from_owned(
            render_json_builder_object(values)?,
        ))),
        BuiltinScalarFunction::JsonObject => Ok(Value::Json(CompactString::from_owned(
            render_json_object_function(values)?,
        ))),
        BuiltinScalarFunction::JsonTypeof => {
            let json = ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))?;
            Ok(Value::Text(CompactString::new(json.typeof_name())))
        }
        BuiltinScalarFunction::JsonbTypeof => {
            let json = ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))?;
            Ok(Value::Text(CompactString::new(json.typeof_name())))
        }
        BuiltinScalarFunction::JsonArrayLength => {
            match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                ParsedJsonValue::Json(SerdeJsonValue::Array(items)) => {
                    Ok(Value::Int32(items.len() as i32))
                }
                ParsedJsonValue::Jsonb(JsonbValue::Array(items)) => {
                    Ok(Value::Int32(items.len() as i32))
                }
                ParsedJsonValue::Json(other) => Err(ExecError::TypeMismatch {
                    op: "json_array_length",
                    left: json_value_to_value(&other, false),
                    right: Value::Null,
                }),
                ParsedJsonValue::Jsonb(other) => Err(ExecError::TypeMismatch {
                    op: "json_array_length",
                    left: jsonb_to_value(&other),
                    right: Value::Null,
                }),
            }
        }
        BuiltinScalarFunction::JsonbArrayLength => {
            match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                ParsedJsonValue::Json(SerdeJsonValue::Array(items)) => {
                    Ok(Value::Int32(items.len() as i32))
                }
                ParsedJsonValue::Jsonb(JsonbValue::Array(items)) => {
                    Ok(Value::Int32(items.len() as i32))
                }
                ParsedJsonValue::Json(other) => Err(ExecError::TypeMismatch {
                    op: "jsonb_array_length",
                    left: json_value_to_value(&other, false),
                    right: Value::Null,
                }),
                ParsedJsonValue::Jsonb(other) => Err(ExecError::TypeMismatch {
                    op: "jsonb_array_length",
                    left: jsonb_to_value(&other),
                    right: Value::Null,
                }),
            }
        }
        BuiltinScalarFunction::JsonExtractPath => {
            let path = parse_json_path_args(&values[1..])?;
            Ok(
                match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                    ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                        .map(|value| json_value_to_value(value, false))
                        .unwrap_or(Value::Null),
                    ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                        .map(jsonb_to_value)
                        .unwrap_or(Value::Null),
                },
            )
        }
        BuiltinScalarFunction::JsonExtractPathText => {
            let path = parse_json_path_args(&values[1..])?;
            Ok(
                match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                    ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                        .map(|value| json_value_to_value(value, true))
                        .unwrap_or(Value::Null),
                    ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                        .map(jsonb_to_text_value)
                        .unwrap_or(Value::Null),
                },
            )
        }
        BuiltinScalarFunction::JsonbExtractPath => {
            let path = parse_json_path_args(&values[1..])?;
            Ok(
                match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                    ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                        .map(|value| Value::Jsonb(parse_jsonb_text(&value.to_string()).unwrap()))
                        .unwrap_or(Value::Null),
                    ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                        .map(jsonb_to_value)
                        .unwrap_or(Value::Null),
                },
            )
        }
        BuiltinScalarFunction::JsonbExtractPathText => {
            let path = parse_json_path_args(&values[1..])?;
            Ok(
                match ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))? {
                    ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                        .map(|value| json_value_to_value(value, true))
                        .unwrap_or(Value::Null),
                    ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                        .map(jsonb_to_text_value)
                        .unwrap_or(Value::Null),
                },
            )
        }
        BuiltinScalarFunction::JsonbBuildArray => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(jsonb_from_value(value)?);
            }
            Ok(Value::Jsonb(encode_jsonb(&JsonbValue::Array(items))))
        }
        BuiltinScalarFunction::JsonbBuildObject => {
            let pairs = json_builder_pairs(values, "jsonb_build_object")?;
            Ok(Value::Jsonb(encode_jsonb(&jsonb_object_from_pairs(&pairs)?)))
        }
        BuiltinScalarFunction::JsonbPathExists => {
            eval_jsonpath_function(values, JsonPathFunctionKind::Exists)
        }
        BuiltinScalarFunction::JsonbPathMatch => {
            eval_jsonpath_function(values, JsonPathFunctionKind::Match)
        }
        BuiltinScalarFunction::JsonbPathQueryArray => {
            eval_jsonpath_function(values, JsonPathFunctionKind::QueryArray)
        }
        BuiltinScalarFunction::JsonbPathQueryFirst => {
            eval_jsonpath_function(values, JsonPathFunctionKind::QueryFirst)
        }
        _ => unreachable!(),
        }
    };

    match func {
        BuiltinScalarFunction::ToJson
        | BuiltinScalarFunction::ToJsonb
        | BuiltinScalarFunction::ArrayToJson
        | BuiltinScalarFunction::JsonBuildArray
        | BuiltinScalarFunction::JsonBuildObject
        | BuiltinScalarFunction::JsonObject
        | BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonbArrayLength
        | BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText
        | BuiltinScalarFunction::JsonbBuildArray
        | BuiltinScalarFunction::JsonbBuildObject
        | BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::JsonbPathQueryArray
        | BuiltinScalarFunction::JsonbPathQueryFirst => Some(eval()),
        _ => None,
    }
}

fn render_json_builder_array(values: &[Value]) -> String {
    let mut out = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value_to_json_text(value, false));
    }
    out.push(']');
    out
}

fn render_json_builder_object(values: &[Value]) -> Result<String, ExecError> {
    let pairs = json_builder_pairs(values, "json_build_object")?;
    Ok(render_json_pairs(&pairs))
}

fn render_json_object_function(values: &[Value]) -> Result<String, ExecError> {
    match values {
        [single] => {
            let items = array_values_for_json_object(single, "json_object")?;
            if items.len() % 2 != 0 {
                return Err(ExecError::InvalidStorageValue {
                    column: "json".into(),
                    details: "argument list must have even number of elements".into(),
                });
            }
            let pairs = items
                .chunks(2)
                .map(|chunk| {
                    Ok((
                        json_object_key_text(&chunk[0], "json_object")?,
                        chunk.get(1).cloned().unwrap_or(Value::Null),
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(render_json_string_pairs(&pairs))
        }
        [keys, vals] => {
            let keys = array_values_for_json_object(keys, "json_object")?;
            let vals = array_values_for_json_object(vals, "json_object")?;
            if keys.len() != vals.len() {
                return Err(ExecError::InvalidStorageValue {
                    column: "json".into(),
                    details: "mismatched array dimensions".into(),
                });
            }
            let pairs = keys
                .into_iter()
                .zip(vals)
                .map(|(k, v)| Ok((json_object_key_text(&k, "json_object")?, v)))
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(render_json_string_pairs(&pairs))
        }
        _ => Err(ExecError::InvalidStorageValue {
            column: "json".into(),
            details: "json_object expects one or two array arguments".into(),
        }),
    }
}

fn json_builder_pairs(
    values: &[Value],
    op: &'static str,
) -> Result<Vec<(String, Value)>, ExecError> {
    if values.len() % 2 != 0 {
        return Err(ExecError::InvalidStorageValue {
            column: "json".into(),
            details: format!("{op} arguments must alternate keys and values"),
        });
    }
    values
        .chunks(2)
        .map(|chunk| {
            Ok((
                jsonb_builder_key(&chunk[0])?,
                chunk.get(1).cloned().unwrap_or(Value::Null),
            ))
        })
        .collect()
}

fn render_json_pairs(pairs: &[(String, Value)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(key).unwrap());
        out.push(':');
        out.push_str(&value_to_json_text(value, false));
    }
    out.push('}');
    out
}

fn render_json_string_pairs(pairs: &[(String, Value)]) -> String {
    render_json_pairs(pairs)
}

fn array_values_for_json_object(value: &Value, op: &'static str) -> Result<Vec<Value>, ExecError> {
    match value {
        Value::Array(items) => Ok(items.clone()),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn json_object_key_text(value: &Value, op: &'static str) -> Result<String, ExecError> {
    match value {
        Value::Null => Ok("".into()),
        Value::Text(_) | Value::TextRef(_, _) => Ok(value.as_text().unwrap().to_string()),
        Value::Int16(v) => Ok(v.to_string()),
        Value::Int32(v) => Ok(v.to_string()),
        Value::Int64(v) => Ok(v.to_string()),
        Value::Float64(v) => Ok(v.to_string()),
        Value::Numeric(v) => Ok(v.render()),
        Value::Bool(v) => Ok(if *v { "true".into() } else { "false".into() }),
        Value::JsonPath(v) => Ok(v.to_string()),
        Value::Json(v) => Ok(v.to_string()),
        Value::Jsonb(v) => render_jsonb_bytes(v),
        Value::Array(_) => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
    }
}

pub(crate) fn eval_json_get(
    left: &Expr,
    right: &Expr,
    as_text: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let json_value = eval_expr(left, slot, ctx)?;
    let key = eval_expr(right, slot, ctx)?;
    if matches!(json_value, Value::Null) || matches!(key, Value::Null) {
        return Ok(Value::Null);
    }
    match ParsedJsonValue::from_value(&json_value)? {
        ParsedJsonValue::Json(parsed) => {
            let selected = match key {
                Value::Text(_) | Value::TextRef(_, _) => {
                    let name = key.as_text().unwrap();
                    match &parsed {
                        SerdeJsonValue::Object(map) => map.get(name),
                        _ => None,
                    }
                }
                Value::Int16(index) => json_lookup_index(&parsed, index as i32),
                Value::Int32(index) => json_lookup_index(&parsed, index),
                Value::Int64(index) => i32::try_from(index)
                    .ok()
                    .and_then(|index| json_lookup_index(&parsed, index)),
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: if as_text { "->>" } else { "->" },
                        left: json_value,
                        right: other,
                    });
                }
            };
            Ok(selected
                .map(|value| json_value_to_value(value, as_text))
                .unwrap_or(Value::Null))
        }
        ParsedJsonValue::Jsonb(parsed) => Ok(jsonb_get(&parsed, &key)?
            .map(|value| {
                if as_text {
                    jsonb_to_text_value(value)
                } else {
                    jsonb_to_value(value)
                }
            })
            .unwrap_or(Value::Null)),
    }
}

pub(crate) fn eval_json_path(
    left: &Expr,
    right: &Expr,
    as_text: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let json_value = eval_expr(left, slot, ctx)?;
    let path_value = eval_expr(right, slot, ctx)?;
    if matches!(json_value, Value::Null) || matches!(path_value, Value::Null) {
        return Ok(Value::Null);
    }
    let path = parse_json_path_value(
        &path_value,
        if as_text { "#>>" } else { "#>" },
        json_value.clone(),
    )?;
    Ok(match ParsedJsonValue::from_value(&json_value)? {
        ParsedJsonValue::Json(parsed) => json_lookup_path(&parsed, &path)
            .map(|value| json_value_to_value(value, as_text))
            .unwrap_or(Value::Null),
        ParsedJsonValue::Jsonb(parsed) => jsonb_path(&parsed, &path)
            .map(|value| {
                if as_text {
                    jsonb_to_text_value(value)
                } else {
                    jsonb_to_value(value)
                }
            })
            .unwrap_or(Value::Null),
    })
}

#[derive(Debug, Clone, Copy)]
enum JsonPathFunctionKind {
    Exists,
    Match,
    QueryArray,
    QueryFirst,
}

pub(crate) fn eval_jsonpath_operator(
    left: &Expr,
    right: &Expr,
    as_match: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let json_value = eval_expr(left, slot, ctx)?;
    let path_value = eval_expr(right, slot, ctx)?;
    if matches!(json_value, Value::Null) || matches!(path_value, Value::Null) {
        return Ok(Value::Null);
    }
    let target = parse_jsonpath_target_value(&json_value)?;
    let path = parse_jsonpath_value_text(&path_value)?;
    let parsed = parse_jsonpath(path.as_str())?;
    let eval_ctx = JsonPathEvaluationContext {
        root: &target,
        vars: None,
    };
    let result = evaluate_jsonpath(&parsed, &eval_ctx);
    if as_match {
        jsonpath_match_result(result, true)
    } else {
        Ok(Value::Bool(result.map(|items| !items.is_empty()).unwrap_or(false)))
    }
}

fn eval_jsonpath_function(values: &[Value], kind: JsonPathFunctionKind) -> Result<Value, ExecError> {
    let target = values.first().unwrap_or(&Value::Null);
    let path = values.get(1).unwrap_or(&Value::Null);
    if matches!(target, Value::Null) || matches!(path, Value::Null) {
        return Ok(Value::Null);
    }
    let vars = values.get(2);
    let silent = values
        .get(3)
        .map(|value| match value {
            Value::Bool(flag) => Ok(*flag),
            Value::Null => Ok(false),
            other => Err(ExecError::TypeMismatch {
                op: "jsonpath silent",
                left: other.clone(),
                right: Value::Bool(false),
            }),
        })
        .transpose()?
        .unwrap_or(false);
    let target = parse_jsonpath_target_value(target)?;
    let parsed = parse_jsonpath(parse_jsonpath_value_text(path)?.as_str())?;
    let vars_json = match vars {
        Some(Value::Null) | None => None,
        Some(value) => Some(parse_jsonpath_target_value(value)?),
    };
    let eval_ctx = JsonPathEvaluationContext {
        root: &target,
        vars: vars_json.as_ref(),
    };
    let result = evaluate_jsonpath(&parsed, &eval_ctx);
    match kind {
        JsonPathFunctionKind::Exists => {
            Ok(Value::Bool(result.map(|items| !items.is_empty()).unwrap_or(false)))
        }
        JsonPathFunctionKind::Match => jsonpath_match_result(result, silent),
        JsonPathFunctionKind::QueryArray => match result {
            Ok(items) => Ok(Value::Jsonb(encode_jsonb(&JsonbValue::Array(items)))),
            Err(_) if silent => Ok(Value::Jsonb(encode_jsonb(&JsonbValue::Array(vec![])))),
            Err(err) => Err(err),
        },
        JsonPathFunctionKind::QueryFirst => match result {
            Ok(items) => Ok(items.first().map(jsonb_to_value).unwrap_or(Value::Null)),
            Err(_) if silent => Ok(Value::Null),
            Err(err) => Err(err),
        },
    }
}

fn jsonpath_match_result(
    result: Result<Vec<JsonbValue>, ExecError>,
    silent: bool,
) -> Result<Value, ExecError> {
    match result {
        Ok(items) => {
            if items.len() == 1 {
                return Ok(match &items[0] {
                    JsonbValue::Bool(value) => Value::Bool(*value),
                    JsonbValue::Null => Value::Null,
                    _ if silent => Value::Null,
                    _ => {
                        return Err(ExecError::InvalidStorageValue {
                            column: "jsonpath".into(),
                            details: "single boolean result is expected".into(),
                        });
                    }
                });
            }
            if silent {
                Ok(Value::Null)
            } else {
                Err(ExecError::InvalidStorageValue {
                    column: "jsonpath".into(),
                    details: "single boolean result is expected".into(),
                })
            }
        }
        Err(_) if silent => Ok(Value::Null),
        Err(err) => Err(err),
    }
}

fn parse_jsonpath_target_value(value: &Value) -> Result<JsonbValue, ExecError> {
    match value {
        Value::Jsonb(bytes) => decode_jsonb(bytes),
        Value::Json(text) => Ok(JsonbValue::from_serde(parse_json_text(text.as_str())?)?),
        Value::Text(text) => Ok(decode_jsonb(&parse_jsonb_text(text.as_str())?)?),
        Value::TextRef(_, _) => Ok(decode_jsonb(&parse_jsonb_text(value.as_text().unwrap())?)?),
        other => Err(ExecError::TypeMismatch {
            op: "jsonpath target",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn parse_jsonpath_value_text(value: &Value) -> Result<CompactString, ExecError> {
    match value {
        Value::JsonPath(text) => Ok(text.clone()),
        Value::Text(text) => {
            validate_jsonpath_text(text.as_str())?;
            Ok(text.clone())
        }
        Value::TextRef(_, _) => {
            let text = value.as_text().unwrap();
            validate_jsonpath_text(text)?;
            Ok(CompactString::new(text))
        }
        other => Err(ExecError::TypeMismatch {
            op: "jsonpath",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn parse_json_path_args(args: &[Value]) -> Result<Vec<String>, ExecError> {
    args.iter()
        .map(|arg| match arg {
            Value::Text(_) | Value::TextRef(_, _) => Ok(arg.as_text().unwrap().to_string()),
            Value::Null => Ok(String::new()),
            other => Err(ExecError::TypeMismatch {
                op: "json path",
                left: other.clone(),
                right: Value::Null,
            }),
        })
        .collect()
}

fn parse_json_path_value(
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
        other => Err(ExecError::TypeMismatch {
            op,
            left,
            right: other.clone(),
        }),
    }
}

fn json_lookup_index<'a>(json: &'a SerdeJsonValue, index: i32) -> Option<&'a SerdeJsonValue> {
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

fn json_lookup_path<'a>(json: &'a SerdeJsonValue, path: &[String]) -> Option<&'a SerdeJsonValue> {
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
        other => Some(other.to_string()),
    }
}

fn json_value_to_value(value: &SerdeJsonValue, as_text: bool) -> Value {
    if as_text {
        json_value_to_text(value)
            .map(|text| Value::Text(CompactString::from_owned(text)))
            .unwrap_or(Value::Null)
    } else {
        Value::Json(CompactString::from_owned(value.to_string()))
    }
}

fn value_to_json_serde(value: &Value) -> SerdeJsonValue {
    match value {
        Value::Null => SerdeJsonValue::Null,
        Value::Int16(v) => SerdeJsonValue::from(*v),
        Value::Int32(v) => SerdeJsonValue::from(*v),
        Value::Int64(v) => SerdeJsonValue::from(*v),
        Value::Float64(v) => serde_json::Number::from_f64(*v)
            .map(SerdeJsonValue::Number)
            .unwrap_or(SerdeJsonValue::Null),
        Value::Numeric(v) => parse_json_text(&v.render()).unwrap_or(SerdeJsonValue::Null),
        Value::Bool(v) => SerdeJsonValue::Bool(*v),
        Value::JsonPath(text) => SerdeJsonValue::String(text.to_string()),
        Value::Json(text) => parse_json_text(text.as_str()).unwrap_or(SerdeJsonValue::Null),
        Value::Jsonb(bytes) => decode_jsonb(bytes)
            .map(|value| value.to_serde())
            .unwrap_or(SerdeJsonValue::Null),
        Value::Text(_) | Value::TextRef(_, _) => {
            SerdeJsonValue::String(value.as_text().unwrap().to_string())
        }
        Value::Array(items) => SerdeJsonValue::Array(items.iter().map(value_to_json_serde).collect()),
    }
}

fn value_to_json_text(value: &Value, pretty: bool) -> String {
    let json = value_to_json_serde(value);
    if pretty {
        serde_json::to_string_pretty(&json).unwrap()
    } else {
        serde_json::to_string(&json).unwrap()
    }
}

pub(crate) fn eval_json_table_function(
    kind: JsonTableFunction,
    arg: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let value = eval_expr(arg, slot, ctx)?;
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    match (kind, ParsedJsonValue::from_value(&value)?) {
        (JsonTableFunction::ObjectKeys, ParsedJsonValue::Json(json))
        | (JsonTableFunction::JsonbObjectKeys, ParsedJsonValue::Json(json)) => {
            let map = match json {
                SerdeJsonValue::Object(map) => map,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "json_object_keys",
                        left: json_value_to_value(&other, false),
                        right: Value::Null,
                    });
                }
            };
            for (key, _) in map {
                rows.push(TupleSlot::virtual_row(vec![Value::Text(CompactString::from_owned(key))]));
            }
        }
        (JsonTableFunction::JsonbObjectKeys, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Object(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "jsonb_object_keys",
                        left: jsonb_to_value(&other),
                        right: Value::Null,
                    });
                }
            };
            for (key, _) in items {
                rows.push(TupleSlot::virtual_row(vec![Value::Text(CompactString::from_owned(key))]));
            }
        }
        (JsonTableFunction::Each, ParsedJsonValue::Json(json)) => {
            let map = match json {
                SerdeJsonValue::Object(map) => map,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "json_each",
                        left: json_value_to_value(&other, false),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in map {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    json_value_to_value(&value, false),
                ]));
            }
        }
        (JsonTableFunction::JsonbEach, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Object(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "jsonb_each",
                        left: jsonb_to_value(&other),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in items {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    jsonb_to_value(&value),
                ]));
            }
        }
        (JsonTableFunction::EachText, ParsedJsonValue::Json(json)) => {
            let map = match json {
                SerdeJsonValue::Object(map) => map,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "json_each_text",
                        left: json_value_to_value(&other, false),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in map {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    json_value_to_value(&value, true),
                ]));
            }
        }
        (JsonTableFunction::JsonbEachText, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Object(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "jsonb_each_text",
                        left: jsonb_to_value(&other),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in items {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    jsonb_to_text_value(&value),
                ]));
            }
        }
        (JsonTableFunction::ArrayElements, ParsedJsonValue::Json(json)) => {
            let items = match json {
                SerdeJsonValue::Array(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "json_array_elements",
                        left: json_value_to_value(&other, false),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(&value, false)]));
            }
        }
        (JsonTableFunction::JsonbArrayElements, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Array(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "jsonb_array_elements",
                        left: jsonb_to_value(&other),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![jsonb_to_value(&value)]));
            }
        }
        (JsonTableFunction::ArrayElementsText, ParsedJsonValue::Json(json)) => {
            let items = match json {
                SerdeJsonValue::Array(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "json_array_elements_text",
                        left: json_value_to_value(&other, false),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(&value, true)]));
            }
        }
        (JsonTableFunction::JsonbArrayElementsText, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Array(items) => items,
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "jsonb_array_elements_text",
                        left: jsonb_to_value(&other),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![jsonb_to_text_value(&value)]));
            }
        }
        (kind, ParsedJsonValue::Jsonb(json)) => {
            return Err(ExecError::TypeMismatch {
                op: match kind {
                    JsonTableFunction::ObjectKeys => "json_object_keys",
                    JsonTableFunction::Each => "json_each",
                    JsonTableFunction::EachText => "json_each_text",
                    JsonTableFunction::ArrayElements => "json_array_elements",
                    JsonTableFunction::ArrayElementsText => "json_array_elements_text",
                    JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
                    JsonTableFunction::JsonbEach => "jsonb_each",
                    JsonTableFunction::JsonbEachText => "jsonb_each_text",
                    JsonTableFunction::JsonbArrayElements => "jsonb_array_elements",
                    JsonTableFunction::JsonbArrayElementsText => "jsonb_array_elements_text",
                },
                left: jsonb_to_value(&json),
                right: Value::Null,
            });
        }
        (kind, ParsedJsonValue::Json(json)) => {
            return Err(ExecError::TypeMismatch {
                op: match kind {
                    JsonTableFunction::ObjectKeys => "json_object_keys",
                    JsonTableFunction::Each => "json_each",
                    JsonTableFunction::EachText => "json_each_text",
                    JsonTableFunction::ArrayElements => "json_array_elements",
                    JsonTableFunction::ArrayElementsText => "json_array_elements_text",
                    JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
                    JsonTableFunction::JsonbEach => "jsonb_each",
                    JsonTableFunction::JsonbEachText => "jsonb_each_text",
                    JsonTableFunction::JsonbArrayElements => "jsonb_array_elements",
                    JsonTableFunction::JsonbArrayElementsText => "jsonb_array_elements_text",
                },
                left: json_value_to_value(&json, false),
                right: Value::Null,
            });
        }
    }
    Ok(rows)
}
