use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, Zero};
use serde_json::Value as SerdeJsonValue;

use super::nodes::*;
use super::{ExecError, ExecutorContext, exec_next, executor_start};
use crate::backend::executor::jsonb::{
    JsonbValue, compare_jsonb, decode_jsonb, encode_jsonb, jsonb_builder_key, jsonb_concat,
    jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any, jsonb_from_value, jsonb_get,
    jsonb_object_from_pairs, jsonb_path, jsonb_to_text_value, jsonb_to_value, parse_jsonb_text,
    render_jsonb_bytes,
};
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::pgrust::compact_string::CompactString;

extern crate rand;

fn validate_json_text(text: &str) -> Result<(), ExecError> {
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

pub fn eval_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => {
            let val = slot.get_attr(*index)?;
            Ok(val.clone())
        }
        Expr::OuterColumn { depth, index } => ctx
            .outer_rows
            .get(*depth)
            .and_then(|row| row.get(*index))
            .cloned()
            .ok_or(ExecError::UnboundOuterColumn {
                depth: *depth,
                index: *index,
            }),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Add(left, right) => {
            add_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Sub(left, right) => {
            sub_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Mul(left, right) => {
            mul_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Div(left, right) => {
            div_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Mod(left, right) => {
            mod_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Concat(left, right) => {
            concat_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::UnaryPlus(inner) => eval_expr(inner, slot, ctx),
        Expr::Negate(inner) => negate_value(eval_expr(inner, slot, ctx)?),
        Expr::Cast(inner, ty) => cast_value(eval_expr(inner, slot, ctx)?, *ty),
        Expr::Eq(left, right) => compare_values(
            "=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::NotEq(left, right) => {
            not_equal_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Lt(left, right) => order_values(
            "<",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::LtEq(left, right) => order_values(
            "<=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::GtEq(left, right) => order_values(
            ">=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::RegexMatch(left, right) => {
            let text = eval_expr(left, slot, ctx)?;
            let pattern = eval_expr(right, slot, ctx)?;
            if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                return Ok(Value::Null);
            }
            let text_str = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~",
                left: text.clone(),
                right: pattern.clone(),
            })?;
            let pat_str = pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~",
                left: text.clone(),
                right: pattern.clone(),
            })?;
            let re =
                regex::Regex::new(pat_str).map_err(|e| ExecError::InvalidRegex(e.to_string()))?;
            Ok(Value::Bool(re.is_match(text_str)))
        }
        Expr::And(left, right) => {
            eval_and(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Or(left, right) => eval_or(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Not(inner) => match eval_expr(inner, slot, ctx)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            for expr in elements {
                values.push(cast_value(eval_expr(expr, slot, ctx)?, element_type)?);
            }
            Ok(Value::Array(values))
        }
        Expr::ArrayOverlap(left, right) => {
            eval_array_overlap(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbContains(left, right) => {
            eval_jsonb_contains(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbContained(left, right) => {
            eval_jsonb_contained(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExists(left, right) => {
            eval_jsonb_exists(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExistsAny(left, right) => {
            eval_jsonb_exists_any(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExistsAll(left, right) => {
            eval_jsonb_exists_all(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::ScalarSubquery(plan) => eval_scalar_subquery(plan, slot, ctx),
        Expr::ExistsSubquery(plan) => eval_exists_subquery(plan, slot, ctx),
        Expr::AnySubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, false, subquery, slot, ctx)
        }
        Expr::AllSubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, true, subquery, slot, ctx)
        }
        Expr::AnyArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, false, &right_value)
        }
        Expr::AllArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, true, &right_value)
        }
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::JsonGet(left, right) => eval_json_get(left, right, false, slot, ctx),
        Expr::JsonGetText(left, right) => eval_json_get(left, right, true, slot, ctx),
        Expr::JsonPath(left, right) => eval_json_path(left, right, false, slot, ctx),
        Expr::JsonPathText(left, right) => eval_json_path(left, right, true, slot, ctx),
        Expr::FuncCall { func, args } => eval_builtin_function(*func, args, slot, ctx),
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(
            render_current_timestamp(),
        ))),
    }
}

fn eval_builtin_function(
    func: BuiltinScalarFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    match func {
        BuiltinScalarFunction::Random => Ok(Value::Float64(rand::random::<f64>())),
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
            render_json_builder_array(&values),
        ))),
        BuiltinScalarFunction::JsonBuildObject => Ok(Value::Json(CompactString::from_owned(
            render_json_builder_object(&values)?,
        ))),
        BuiltinScalarFunction::JsonObject => Ok(Value::Json(CompactString::from_owned(
            render_json_object_function(&values)?,
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
            for value in &values {
                items.push(jsonb_from_value(value)?);
            }
            Ok(Value::Jsonb(encode_jsonb(&JsonbValue::Array(items))))
        }
        BuiltinScalarFunction::JsonbBuildObject => {
            let pairs = json_builder_pairs(&values, "jsonb_build_object")?;
            Ok(Value::Jsonb(encode_jsonb(&jsonb_object_from_pairs(
                &pairs,
            )?)))
        }
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
        Value::Json(v) => Ok(v.to_string()),
        Value::Jsonb(v) => render_jsonb_bytes(v),
        Value::Array(_) => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
    }
}

fn eval_json_get(
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

fn eval_json_path(
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
        Value::Json(text) => parse_json_text(text.as_str()).unwrap_or(SerdeJsonValue::Null),
        Value::Jsonb(bytes) => decode_jsonb(bytes)
            .map(|value| value.to_serde())
            .unwrap_or(SerdeJsonValue::Null),
        Value::Text(_) | Value::TextRef(_, _) => {
            SerdeJsonValue::String(value.as_text().unwrap().to_string())
        }
        Value::Array(items) => {
            SerdeJsonValue::Array(items.iter().map(value_to_json_serde).collect())
        }
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
                rows.push(TupleSlot::virtual_row(vec![Value::Text(
                    CompactString::from_owned(key),
                )]));
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
                rows.push(TupleSlot::virtual_row(vec![Value::Text(
                    CompactString::from_owned(key),
                )]));
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
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(
                    &value, false,
                )]));
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
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(
                    &value, true,
                )]));
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

fn eval_jsonb_contains(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(&left)?;
    let right_jsonb = jsonb_from_value(&right)?;
    Ok(Value::Bool(jsonb_contains(&left_jsonb, &right_jsonb)))
}

fn eval_jsonb_contained(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(&left)?;
    let right_jsonb = jsonb_from_value(&right)?;
    Ok(Value::Bool(jsonb_contains(&right_jsonb, &left_jsonb)))
}

fn eval_jsonb_exists(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let key = right.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "?",
        left: left.clone(),
        right: right.clone(),
    })?;
    let jsonb = jsonb_from_value(&left)?;
    Ok(Value::Bool(jsonb_exists(&jsonb, key)))
}

fn eval_jsonb_exists_any(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?|", jsonb_exists_any)
}

fn eval_jsonb_exists_all(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?&", jsonb_exists_all)
}

fn eval_jsonb_exists_list(
    left: Value,
    right: Value,
    op: &'static str,
    pred: fn(&JsonbValue, &[String]) -> bool,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let keys = match right {
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_text()
                    .map(|text| text.to_string())
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: left.clone(),
                        right: item.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left,
                right: other,
            });
        }
    };
    let jsonb = jsonb_from_value(&left)?;
    Ok(Value::Bool(pred(&jsonb, &keys)))
}

fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    array_value: &Value,
) -> Result<Value, ExecError> {
    match array_value {
        Value::Null => Ok(Value::Null),
        Value::Array(items) => {
            let mut saw_null = false;
            for item in items {
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
            if items.is_empty() {
                Ok(Value::Bool(is_all))
            } else if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(is_all))
            }
        }
        other => Err(ExecError::TypeMismatch {
            op: if is_all { "ALL" } else { "ANY" },
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(left_items), Value::Array(right_items)) => {
            for left_item in &left_items {
                if matches!(left_item, Value::Null) {
                    continue;
                }
                for right_item in &right_items {
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
        (left, right) => Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right,
        }),
    }
}

fn eval_scalar_subquery(
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut first_value = None;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            if first_value.is_some() {
                return Err(ExecError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                ));
            }
            first_value = Some(values[0].clone());
        }
        Ok(first_value.unwrap_or(Value::Null))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_exists_subquery(
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        Ok(Value::Bool(exec_next(&mut state, ctx)?.is_some()))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_quantified_subquery(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut saw_row = false;
        let mut saw_null = false;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            saw_row = true;
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            match compare_subquery_values(left_value, &values[0], op)? {
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
        if !saw_row {
            Ok(Value::Bool(is_all))
        } else if saw_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(is_all))
        }
    })();
    ctx.outer_rows.remove(0);
    result
}

fn compare_subquery_values(
    left: &Value,
    right: &Value,
    op: SubqueryComparisonOp,
) -> Result<Value, ExecError> {
    match op {
        SubqueryComparisonOp::Eq => compare_values("=", left.clone(), right.clone()),
        SubqueryComparisonOp::NotEq => not_equal_values(left.clone(), right.clone()),
        SubqueryComparisonOp::Lt => order_values("<", left.clone(), right.clone()),
        SubqueryComparisonOp::LtEq => order_values("<=", left.clone(), right.clone()),
        SubqueryComparisonOp::Gt => order_values(">", left.clone(), right.clone()),
        SubqueryComparisonOp::GtEq => order_values(">=", left.clone(), right.clone()),
    }
}

fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    if ty.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(items.len());
                for item in items {
                    casted.push(cast_value(item, element_type)?);
                }
                Ok(Value::Array(casted))
            }
            other => Err(ExecError::TypeMismatch {
                op: "::array",
                left: other,
                right: Value::Null,
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => Ok(Value::Int16(v)),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => Ok(Value::Int32(v as i32)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v as i64)),
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v as i64))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int16(v),
                right: Value::Bool(false),
            }),
        },
        Value::Int32(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int2",
                    left: Value::Int32(v),
                    right: Value::Int16(0),
                }),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => Ok(Value::Int32(v)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v as i64)),
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v as i64))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int32(v),
                right: Value::Bool(false),
            }),
        },
        Value::Bool(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(Value::Bool(v)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb,
                ..
            } => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind:
                    SqlTypeKind::Int2
                    | SqlTypeKind::Int4
                    | SqlTypeKind::Int8
                    | SqlTypeKind::Float4
                    | SqlTypeKind::Float8
                    | SqlTypeKind::Numeric,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Bool(v),
                right: Value::Int32(0),
            }),
        },
        Value::Text(text) => cast_text_value(text.as_str(), ty, true),
        Value::TextRef(ptr, len) => {
            let text = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
            };
            cast_text_value(text, ty, true)
        }
        Value::Json(text) => cast_text_value(text.as_str(), ty, true),
        Value::Jsonb(bytes) => match ty.kind {
            SqlTypeKind::Jsonb => Ok(Value::Jsonb(bytes)),
            SqlTypeKind::Json => Ok(Value::Json(CompactString::from_owned(render_jsonb_bytes(
                &bytes,
            )?))),
            SqlTypeKind::Text
            | SqlTypeKind::Timestamp
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar => cast_text_value(&render_jsonb_bytes(&bytes)?, ty, true),
            _ => Err(ExecError::TypeMismatch {
                op: "::jsonb",
                left: Value::Jsonb(bytes),
                right: Value::Null,
            }),
        },
        Value::Int64(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int2",
                    left: Value::Int64(v),
                    right: Value::Int16(0),
                }),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => i32::try_from(v)
                .map(Value::Int32)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int4",
                    left: Value::Int64(v),
                    right: Value::Int32(0),
                }),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v)),
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int64(v),
                right: Value::Bool(false),
            }),
        },
        Value::Float64(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                (v as f32) as f64
            } else {
                v
            })),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(
                parse_numeric_text(&v.to_string())
                    .ok_or_else(|| ExecError::InvalidNumericInput(v.to_string()))?,
            )),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Bool,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::",
                left: Value::Float64(v),
                right: match ty {
                    SqlType {
                        kind: SqlTypeKind::Int2,
                        ..
                    } => Value::Int16(0),
                    SqlType {
                        kind: SqlTypeKind::Int4,
                        ..
                    } => Value::Int32(0),
                    SqlType {
                        kind: SqlTypeKind::Int8,
                        ..
                    } => Value::Int64(0),
                    SqlType {
                        kind: SqlTypeKind::Bool,
                        ..
                    } => Value::Bool(false),
                    _ => Value::Text(CompactString::new("")),
                },
            }),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric, ty, true),
        Value::Array(items) => Ok(Value::Array(items)),
    }
}

fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Text | SqlTypeKind::Timestamp => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::Json => {
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => Ok(Value::Jsonb(parse_jsonb_text(text)?)),
        SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::from_owned(
            coerce_character_string(text, ty, explicit)?,
        ))),
        SqlTypeKind::Int2 => {
            text.parse::<i16>()
                .map(Value::Int16)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int2",
                    left: Value::Text(CompactString::new(text)),
                    right: Value::Int16(0),
                })
        }
        SqlTypeKind::Int4 => {
            text.parse::<i32>()
                .map(Value::Int32)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int4",
                    left: Value::Text(CompactString::new(text)),
                    right: Value::Int32(0),
                })
        }
        SqlTypeKind::Int8 => {
            text.parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int8",
                    left: Value::Text(CompactString::new(text)),
                    right: Value::Int64(0),
                })
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => parse_pg_float(text)
            .map(|v| {
                Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                    (v as f32) as f64
                } else {
                    v
                })
            })
            .map_err(|_| ExecError::InvalidFloatInput(text.to_string())),
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(
            parse_numeric_text(text)
                .ok_or_else(|| ExecError::InvalidNumericInput(text.to_string()))?,
            ty,
        )?)),
        SqlTypeKind::Bool => match text.to_ascii_lowercase().as_str() {
            "true" | "t" => Ok(Value::Bool(true)),
            "false" | "f" => Ok(Value::Bool(false)),
            _ => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Text(CompactString::new(text)),
                right: Value::Bool(false),
            }),
        },
    }
}

fn cast_numeric_value(
    value: NumericValue,
    ty: SqlType,
    explicit: bool,
) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(value, ty)?)),
        SqlTypeKind::Text | SqlTypeKind::Timestamp => {
            Ok(Value::Text(CompactString::from_owned(value.render())))
        }
        SqlTypeKind::Json => {
            let rendered = value.render();
            validate_json_text(&rendered)?;
            Ok(Value::Json(CompactString::from_owned(rendered)))
        }
        SqlTypeKind::Jsonb => {
            let rendered = value.render();
            Ok(Value::Jsonb(parse_jsonb_text(&rendered)?))
        }
        SqlTypeKind::Char | SqlTypeKind::Varchar => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Float4 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered)
                .map_err(|_| ExecError::InvalidFloatInput(rendered.clone()))?;
            Ok(Value::Float64((v as f32) as f64))
        }
        SqlTypeKind::Float8 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered)
                .map_err(|_| ExecError::InvalidFloatInput(rendered.clone()))?;
            Ok(Value::Float64(v))
        }
        SqlTypeKind::Int2 => {
            let rendered = value.render();
            rendered
                .parse::<i16>()
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange)
        }
        SqlTypeKind::Int4 => {
            let rendered = value.render();
            rendered
                .parse::<i32>()
                .map(Value::Int32)
                .map_err(|_| ExecError::Int4OutOfRange)
        }
        SqlTypeKind::Int8 => {
            let rendered = value.render();
            rendered
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| ExecError::Int8OutOfRange)
        }
        SqlTypeKind::Bool => Err(ExecError::TypeMismatch {
            op: "::bool",
            left: Value::Numeric(value),
            right: Value::Bool(false),
        }),
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let Some(max_chars) = ty.char_len() else {
        return Ok(text.to_string());
    };

    let char_count = text.chars().count() as i32;
    if char_count <= max_chars {
        return Ok(text.to_string());
    }

    let clip_idx = text
        .char_indices()
        .nth(max_chars as usize)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let truncated = &text[..clip_idx];
    let remainder = &text[clip_idx..];
    if explicit || remainder.chars().all(|ch| ch == ' ') {
        Ok(truncated.to_string())
    } else {
        Err(ExecError::StringDataRightTruncation {
            ty: format!("character varying({max_chars})"),
        })
    }
}

fn coerce_numeric_value(parsed: NumericValue, ty: SqlType) -> Result<NumericValue, ExecError> {
    let Some((precision, scale)) = ty.numeric_precision_scale() else {
        return Ok(parsed);
    };

    let rounded = parsed
        .round_to_scale(scale as u32)
        .ok_or_else(|| ExecError::NumericFieldOverflow)?;

    if rounded.digit_count() > precision {
        return Err(ExecError::NumericFieldOverflow);
    }

    Ok(rounded)
}

fn parse_pg_float(text: &str) -> Result<f64, ()> {
    if text.eq_ignore_ascii_case("infinity") || text.eq_ignore_ascii_case("+infinity") {
        Ok(f64::INFINITY)
    } else if text.eq_ignore_ascii_case("-infinity") {
        Ok(f64::NEG_INFINITY)
    } else {
        text.parse::<f64>().map_err(|_| ())
    }
}

pub(crate) fn compare_order_by_keys(
    items: &[OrderByEntry],
    left_keys: &[Value],
    right_keys: &[Value],
) -> Ordering {
    for (item, (left_value, right_value)) in
        items.iter().zip(left_keys.iter().zip(right_keys.iter()))
    {
        let ordering =
            compare_order_values(left_value, right_value, item.nulls_first, item.descending);
        if ordering != Ordering::Equal {
            return if item.descending
                && !matches!(
                    (left_value, right_value),
                    (Value::Null, _) | (_, Value::Null)
                ) {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}

pub(crate) fn compare_order_values(
    left: &Value,
    right: &Value,
    nulls_first: Option<bool>,
    descending: bool,
) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(descending);
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => pg_float_cmp(*a, *b),
        (a, b) if parsed_numeric_value(a).is_some() && parsed_numeric_value(b).is_some() => {
            parsed_numeric_value(a)
                .and_then(|left| parsed_numeric_value(b).map(|right| left.cmp(&right)))
                .unwrap_or(Ordering::Equal)
        }
        (Value::Jsonb(a), Value::Jsonb(b)) => compare_jsonb(
            &decode_jsonb(a).unwrap_or(JsonbValue::Null),
            &decode_jsonb(b).unwrap_or(JsonbValue::Null),
        ),
        (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
            a.as_text().unwrap().cmp(b.as_text().unwrap())
        }
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => format_array_text(a).cmp(&format_array_text(b)),
        _ => Ordering::Equal,
    }
}

/// A predicate compiled at plan time into a specialized closure, like PG's
/// ExecInitQual which resolves expression evaluation steps once. Eliminates
/// per-tuple recursive eval_expr dispatch. Allocated once at plan time;
/// per-tuple cost is just an indirect function call.
pub(crate) type CompiledPredicate =
    Box<dyn Fn(&mut TupleSlot, &mut ExecutorContext) -> Result<bool, ExecError>>;

/// Compile a predicate with access to the tuple decoder for direct byte access.
/// Like PG's heap_getattr fast path for fixed-offset attributes.
pub(crate) fn compile_predicate_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> CompiledPredicate {
    // Try to compile with fixed-offset fast path first.
    if let Some(pred) = try_compile_fixed_offset(expr, decoder) {
        return pred;
    }
    // Fall back to the generic compiled predicate.
    compile_predicate(expr)
}

/// Try to compile a predicate that reads fixed-offset int32 columns directly
/// from raw tuple bytes, bypassing slot_getsomeattrs entirely.
fn try_compile_fixed_offset(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Option<CompiledPredicate> {
    match expr {
        Expr::Gt(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v > val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v > val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: ">",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            } else {
            }
        }
        Expr::Lt(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v < val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v < val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: "<",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            } else {
            }
        }
        Expr::Eq(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v == val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v == val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: "=",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            } else {
            }
        }
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }));
        }
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }));
        }
        _ => {}
    }
    None
}

fn flatten_and_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_and_with_decoder_inner(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate>,
) {
    if let Expr::And(left, right) = expr {
        flatten_and_with_decoder_inner(left, decoder, out);
        flatten_and_with_decoder_inner(right, decoder, out);
    } else {
        out.push(compile_predicate_with_decoder(expr, decoder));
    }
}

fn flatten_or_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_or_with_decoder_inner(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate>,
) {
    if let Expr::Or(left, right) = expr {
        flatten_or_with_decoder_inner(left, decoder, out);
        flatten_or_with_decoder_inner(right, decoder, out);
    } else {
        out.push(compile_predicate_with_decoder(expr, decoder));
    }
}

/// Compile an expression into a specialized predicate closure.
pub(crate) fn compile_predicate(expr: &Expr) -> CompiledPredicate {
    match expr {
        Expr::Gt(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, val) = (*col, *val);
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v > val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: ">",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            } else {
            }
        }
        Expr::Lt(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, val) = (*col, *val);
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v < val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: "<",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            } else {
            }
        }
        Expr::Eq(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Int32(val))) =
                (left.as_ref(), right.as_ref())
            {
                let (col, val) = (*col, *val);
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v == val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: "=",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            } else {
            }
        }
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            });
        }
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            });
        }
        Expr::RegexMatch(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Text(pat))) =
                (left.as_ref(), right.as_ref())
            {
                let col = *col;
                if let Ok(re) = regex::Regex::new(pat.as_str()) {
                    let re = std::sync::Arc::new(re);
                    return Box::new(move |slot, _ctx| {
                        let val = slot.get_attr(col)?;
                        if let Some(s) = val.as_text() {
                            Ok(re.is_match(s))
                        } else if matches!(val, Value::Null) {
                            Ok(false)
                        } else {
                            Err(ExecError::TypeMismatch {
                                op: "~",
                                left: val.clone(),
                                right: Value::Null,
                            })
                        }
                    });
                }
            }
        }
        _ => {}
    }
    // Fallback: generic eval_expr
    let expr = expr.clone();
    Box::new(move |slot, ctx| match eval_expr(&expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    })
}

fn flatten_and(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_inner(expr, &mut out);
    out
}

fn flatten_and_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    if let Expr::And(left, right) = expr {
        flatten_and_inner(left, out);
        flatten_and_inner(right, out);
    } else {
        out.push(compile_predicate(expr));
    }
}

fn flatten_or(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_inner(expr, &mut out);
    out
}

fn flatten_or_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    if let Expr::Or(left, right) = expr {
        flatten_or_inner(left, out);
        flatten_or_inner(right, out);
    } else {
        out.push(compile_predicate(expr));
    }
}

pub(crate) fn tuple_from_values(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<crate::include::access::htup::HeapTuple, ExecError> {
    let tuple_values = desc
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()?;
    crate::include::access::htup::HeapTuple::from_values(&desc.attribute_descs(), &tuple_values)
        .map_err(ExecError::from)
}

pub(crate) fn encode_value(
    column: &ColumnDesc,
    value: &Value,
) -> Result<crate::include::access::htup::TupleValue, ExecError> {
    use crate::include::access::htup::TupleValue;
    match (&column.ty, value) {
        (_, Value::Null) => {
            if !column.storage.nullable {
                Err(ExecError::MissingRequiredColumn(column.name.clone()))
            } else {
                Ok(TupleValue::Null)
            }
        }
        (ScalarType::Int16, Value::Int16(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int64, Value::Int64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Float32, Value::Float64(v)) => {
            Ok(TupleValue::Bytes((*v as f32).to_le_bytes().to_vec()))
        }
        (ScalarType::Float64, Value::Float64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Numeric, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            match coerced {
                Value::Numeric(numeric) => Ok(TupleValue::Bytes(numeric.render().into_bytes())),
                other => Err(ExecError::TypeMismatch {
                    op: "assignment",
                    left: Value::Null,
                    right: other,
                }),
            }
        }
        (ScalarType::Json, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            match coerced {
                Value::Json(text) => Ok(TupleValue::Bytes(text.as_bytes().to_vec())),
                other => Err(ExecError::TypeMismatch {
                    op: "assignment",
                    left: Value::Null,
                    right: other,
                }),
            }
        }
        (ScalarType::Jsonb, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            match coerced {
                Value::Jsonb(bytes) => Ok(TupleValue::Bytes(bytes)),
                other => Err(ExecError::TypeMismatch {
                    op: "assignment",
                    left: Value::Null,
                    right: other,
                }),
            }
        }
        (ScalarType::Text, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            Ok(TupleValue::Bytes(
                coerced.as_text().unwrap().as_bytes().to_vec(),
            ))
        }
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(*v)])),
        (ScalarType::Array(_), v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            match coerced {
                Value::Array(items) => Ok(TupleValue::Bytes(encode_array_bytes(
                    column.sql_type.element_type(),
                    &items,
                )?)),
                other => Err(ExecError::TypeMismatch {
                    op: "assignment",
                    left: Value::Null,
                    right: other,
                }),
            }
        }
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other.clone(),
        }),
    }
}

fn coerce_assignment_value(value: &Value, target: SqlType) -> Result<Value, ExecError> {
    if target.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = target.element_type();
                let mut coerced = Vec::with_capacity(items.len());
                for item in items {
                    coerced.push(coerce_assignment_value(item, element_type)?);
                }
                Ok(Value::Array(coerced))
            }
            other => Err(ExecError::TypeMismatch {
                op: "copy assignment",
                left: Value::Null,
                right: other.clone(),
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int32(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Bool(v) => cast_text_value(if *v { "true" } else { "false" }, target, false),
        Value::Float64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Numeric(numeric) => cast_numeric_value(numeric.clone(), target, false),
        Value::Json(text) => cast_text_value(text.as_str(), target, false),
        Value::Jsonb(bytes) => cast_text_value(&render_jsonb_bytes(bytes)?, target, false),
        Value::Text(text) => cast_text_value(text.as_str(), target, false),
        Value::TextRef(_, _) => cast_text_value(value.as_text().unwrap(), target, false),
        Value::Array(items) => Ok(Value::Array(items.clone())),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };

    match column.ty {
        ScalarType::Int16 => {
            if column.storage.attlen != 2 || bytes.len() != 2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int2 must be exactly 2 bytes".into(),
                },
            )?)))
        }
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int4 must be exactly 4 bytes".into(),
                },
            )?)))
        }
        ScalarType::Int64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int8 must be exactly 8 bytes".into(),
                },
            )?)))
        }
        ScalarType::Float32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Float64(
                f32::from_le_bytes(bytes.try_into().map_err(|_| {
                    ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float4 must be exactly 4 bytes".into(),
                    }
                })?) as f64,
            ))
        }
        ScalarType::Float64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Float64(f64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float8 must be exactly 8 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Numeric => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Numeric(
                parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(
                    || ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "invalid numeric text".into(),
                    },
                )?,
            ))
        }
        ScalarType::Json => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        ScalarType::Jsonb => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            // SAFETY: text columns are stored as valid UTF-8 by the insert path.
            Ok(Value::Text(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            match bytes[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: format!("invalid bool byte {}", other),
                }),
            }
        }
        ScalarType::Array(_) => {
            if column.storage.attlen != -1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            decode_array_bytes(column.sql_type.element_type(), bytes)
        }
    }
}

fn eval_and(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true), Value::Null)
        | (Value::Null, Value::Bool(true))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "AND",
            left,
            right,
        }),
    }
}

fn eval_or(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false), Value::Null)
        | (Value::Null, Value::Bool(false))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "OR",
            left,
            right,
        }),
    }
}

fn compare_values(op: &'static str, left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(l == r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool((*l as i32) == *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(l == r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l == r)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(pg_float_eq(*l, *r))),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            Ok(Value::Bool(
                parsed_numeric_value(l)
                    .unwrap()
                    .cmp(&parsed_numeric_value(r).unwrap())
                    == Ordering::Equal,
            ))
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => Ok(Value::Bool(
            compare_jsonb(&decode_jsonb(l)?, &decode_jsonb(r)?) == Ordering::Equal,
        )),
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => {
            Ok(Value::Bool(l.as_text() == r.as_text()))
        }
        (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(l == r)),
        (Value::Array(l), Value::Array(r)) => Ok(Value::Bool(l == r)),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn not_equal_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match compare_values("=", left.clone(), right.clone())? {
        Value::Bool(value) => Ok(Value::Bool(!value)),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn values_are_distinct(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => false,
        (Value::Null, _) | (_, Value::Null) => true,
        (Value::Int16(l), Value::Int16(r)) => l != r,
        (Value::Int16(l), Value::Int32(r)) => (*l as i32) != *r,
        (Value::Int16(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int32(l), Value::Int32(r)) => l != r,
        (Value::Int32(l), Value::Int16(r)) => *l != (*r as i32),
        (Value::Int32(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int64(l), Value::Int16(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int32(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int64(r)) => l != r,
        (Value::Float64(l), Value::Float64(r)) => !pg_float_eq(*l, *r),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            parsed_numeric_value(l)
                .unwrap()
                .cmp(&parsed_numeric_value(r).unwrap())
                != Ordering::Equal
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => decode_jsonb(l)
            .ok()
            .zip(decode_jsonb(r).ok())
            .map(|(l, r)| compare_jsonb(&l, &r) != Ordering::Equal)
            .unwrap_or(true),
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => l.as_text() != r.as_text(),
        (Value::Bool(l), Value::Bool(r)) => l != r,
        (Value::Array(l), Value::Array(r)) => l != r,
        _ => true,
    }
}

fn add_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l + r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) + *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) + *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l + (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l + r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) + *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l + (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l + (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.add(rv)), "+")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "+",
            left,
            right,
        }),
    }
}

fn sub_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l - r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) - *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) - *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l - (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l - r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) - *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l - (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l - (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.sub(rv)), "-")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "-",
            left,
            right,
        }),
    }
}

fn mul_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l * r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) * *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) * *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l * (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l * r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) * *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l * (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l * (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.mul(rv)), "*")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "*",
            left,
            right,
        }),
    }
}

fn div_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        Value::Float64(v) => *v == 0.0,
        Value::Numeric(v) => *v == NumericValue::zero(),
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("/"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_div_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_div_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_div_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_div_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_div_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_div_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l, *r)?)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| lv.div(rv, 16), "/")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "/",
            left,
            right,
        }),
    }
}

fn mod_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        Value::Numeric(v) => *v == NumericValue::zero(),
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("%"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_rem_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_rem_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_rem_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_rem_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r)?)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| lv.rem(rv), "%")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "%",
            left,
            right,
        }),
    }
}

fn concat_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Jsonb(l), Value::Jsonb(r)) => Ok(Value::Jsonb(encode_jsonb(&jsonb_concat(
            &decode_jsonb(l)?,
            &decode_jsonb(r)?,
        )))),
        (Value::Array(l), Value::Array(r)) => {
            let mut items = l.clone();
            items.extend(r.iter().cloned());
            Ok(Value::Array(items))
        }
        (Value::Array(l), _) => {
            let mut items = l.clone();
            items.push(right);
            Ok(Value::Array(items))
        }
        (_, Value::Array(r)) => {
            let mut items = Vec::with_capacity(r.len() + 1);
            items.push(left);
            items.extend(r.iter().cloned());
            Ok(Value::Array(items))
        }
        _ => {
            let text_type = SqlType::new(SqlTypeKind::Text);
            let left_text = cast_value(left, text_type)?;
            let right_text = cast_value(right, text_type)?;
            let mut out = String::new();
            out.push_str(left_text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "||",
                left: left_text.clone(),
                right: right_text.clone(),
            })?);
            out.push_str(right_text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "||",
                left: left_text.clone(),
                right: right_text.clone(),
            })?);
            Ok(Value::Text(CompactString::from_owned(out)))
        }
    }
}

fn checked_div_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_div_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_div_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int8OutOfRange)
}

fn checked_rem_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_rem(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_rem_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_rem(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_rem_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_rem(right).ok_or(ExecError::Int8OutOfRange)
}

fn negate_value(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => Ok(Value::Int16(-v)),
        Value::Int32(v) => Ok(Value::Int32(-v)),
        Value::Int64(v) => Ok(Value::Int64(-v)),
        Value::Float64(v) => Ok(Value::Float64(-v)),
        Value::Numeric(v) => Ok(Value::Numeric(v.negate())),
        other => Err(ExecError::TypeMismatch {
            op: "unary -",
            left: other,
            right: Value::Null,
        }),
    }
}

fn order_values(op: &'static str, left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l as i32, *r, op))),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i32, op))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(match op {
            "<" => pg_float_cmp(*l, *r) == Ordering::Less,
            "<=" => pg_float_cmp(*l, *r) != Ordering::Greater,
            ">" => pg_float_cmp(*l, *r) == Ordering::Greater,
            ">=" => pg_float_cmp(*l, *r) != Ordering::Less,
            _ => unreachable!(),
        })),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            let ordering = parsed_numeric_value(l)
                .zip(parsed_numeric_value(r))
                .map(|(lv, rv)| lv.cmp(&rv))
                .ok_or_else(|| ExecError::TypeMismatch {
                    op,
                    left: left.clone(),
                    right: right.clone(),
                })?;
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => {
            let ordering = compare_jsonb(&decode_jsonb(l)?, &decode_jsonb(r)?);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(match op {
            "<" => l.as_text().unwrap() < r.as_text().unwrap(),
            "<=" => l.as_text().unwrap() <= r.as_text().unwrap(),
            ">" => l.as_text().unwrap() > r.as_text().unwrap(),
            ">=" => l.as_text().unwrap() >= r.as_text().unwrap(),
            _ => unreachable!(),
        })),
        (Value::Array(l), Value::Array(r)) => {
            let left = format_array_text(l);
            let right = format_array_text(r);
            Ok(Value::Bool(match op {
                "<" => left < right,
                "<=" => left <= right,
                ">" => left > right,
                ">=" => left >= right,
                _ => unreachable!(),
            }))
        }
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn compare_ord<T: Ord>(left: T, right: T, op: &'static str) -> bool {
    match op {
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => unreachable!(),
    }
}

fn pg_float_eq(left: f64, right: f64) -> bool {
    if left.is_nan() && right.is_nan() {
        true
    } else {
        left == right
    }
}

fn pg_float_cmp(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

fn encode_array_bytes(element_type: SqlType, items: &[Value]) -> Result<Vec<u8>, ExecError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        match item {
            Value::Null => bytes.extend_from_slice(&(-1_i32).to_le_bytes()),
            _ => {
                let payload = encode_array_element(element_type, item)?;
                bytes.extend_from_slice(&(payload.len() as i32).to_le_bytes());
                bytes.extend_from_slice(&payload);
            }
        }
    }
    Ok(bytes)
}

fn encode_array_element(element_type: SqlType, value: &Value) -> Result<Vec<u8>, ExecError> {
    let coerced = coerce_assignment_value(value, element_type)?;
    match coerced {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Bool(v) => Ok(vec![u8::from(v)]),
        Value::Numeric(text) => Ok(text.render().into_bytes()),
        Value::Json(text) => Ok(text.as_bytes().to_vec()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(coerced.as_text().unwrap().as_bytes().to_vec()),
        Value::Float64(v) => Ok(v.to_string().into_bytes()),
        Value::Array(_) => Err(ExecError::TypeMismatch {
            op: "array element",
            left: coerced,
            right: Value::Null,
        }),
        Value::Jsonb(bytes) => Ok(bytes),
    }
}

fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array payload too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array length header truncated".into(),
            });
        }
        let len = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len == -1 {
            items.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array element payload truncated".into(),
            });
        }
        items.push(decode_array_element(
            element_type,
            &bytes[offset..offset + len],
        )?);
        offset += len;
    }
    Ok(Value::Array(items))
}

fn decode_array_element(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    match element_type.kind {
        SqlTypeKind::Int2 => {
            if bytes.len() != 2 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int2 array element must be 2 bytes".into(),
                });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int4 => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int4 array element must be 4 bytes".into(),
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int8 => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int8 array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
            if bytes.len()
                != if matches!(element_type.kind, SqlTypeKind::Float4) {
                    4
                } else {
                    8
                }
            {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "float array element has wrong width".into(),
                });
            }
            if matches!(element_type.kind, SqlTypeKind::Float4) {
                Ok(Value::Float64(
                    f32::from_le_bytes(bytes.try_into().unwrap()) as f64,
                ))
            } else {
                Ok(Value::Float64(f64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )))
            }
        }
        SqlTypeKind::Numeric => Ok(Value::Numeric(
            parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(
                || ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "invalid numeric array element".into(),
                },
            )?,
        )),
        SqlTypeKind::Json => {
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => {
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        SqlTypeKind::Bool => {
            if bytes.len() != 1 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "bool array element must be 1 byte".into(),
                });
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar => {
            Ok(Value::Text(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
    }
}

pub(crate) fn format_array_text(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Int16(v) => out.push_str(&v.to_string()),
            Value::Int32(v) => out.push_str(&v.to_string()),
            Value::Int64(v) => out.push_str(&v.to_string()),
            Value::Float64(v) => out.push_str(&v.to_string()),
            Value::Numeric(v) => out.push_str(&v.render()),
            Value::Json(v) => {
                out.push('"');
                for ch in v.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Jsonb(v) => {
                let rendered = render_jsonb_bytes(v).unwrap_or_else(|_| "null".into());
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
            Value::Text(_) | Value::TextRef(_, _) => {
                out.push('"');
                for ch in item.as_text().unwrap().chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Array(nested) => out.push_str(&format_array_text(nested)),
        }
    }
    out.push('}');
    out
}

impl NumericValue {
    pub(crate) fn round_to_scale(&self, target_scale: u32) -> Option<Self> {
        match self {
            Self::NaN => Some(Self::NaN),
            Self::Finite { coeff, scale } => {
                if *scale <= target_scale {
                    let factor = pow10_bigint(target_scale - *scale);
                    return Some(
                        Self::Finite {
                            coeff: coeff * factor,
                            scale: target_scale,
                        }
                        .normalize(),
                    );
                }
                let diff = *scale - target_scale;
                let factor = pow10_bigint(diff);
                let (quotient, remainder) = coeff.div_rem(&factor);
                let twice = remainder.abs() * 2u8;
                let rounded = if twice >= factor.abs() {
                    quotient + coeff.signum()
                } else {
                    quotient
                };
                Some(
                    Self::Finite {
                        coeff: rounded,
                        scale: target_scale,
                    }
                    .normalize(),
                )
            }
        }
    }

    pub(crate) fn add(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Self::NaN,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                Self::Finite {
                    coeff: left + right,
                    scale,
                }
                .normalize()
            }
        }
    }

    fn sub(&self, other: &Self) -> Self {
        self.add(&other.negate())
    }

    fn mul(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Self::NaN,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                },
            ) => Self::Finite {
                coeff: lcoeff * rcoeff,
                scale: lscale.saturating_add(*rscale),
            }
            .normalize(),
        }
    }

    fn rem(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Some(Self::NaN),
            (_, Self::Finite { coeff, .. }) if coeff.is_zero() => None,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                Some(
                    Self::Finite {
                        coeff: left % right,
                        scale,
                    }
                    .normalize(),
                )
            }
        }
    }

    pub(crate) fn div(&self, other: &Self, out_scale: u32) -> Option<Self> {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Some(Self::NaN),
            (_, Self::Finite { coeff, .. }) if coeff.is_zero() => None,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                },
            ) => {
                let exp = out_scale.checked_add(*rscale)?.checked_sub(*lscale)?;
                let factor = pow10_bigint(exp);
                let num = lcoeff * factor;
                let (quotient, remainder) = num.div_rem(rcoeff);
                let twice = remainder.abs() * 2u8;
                let rounded = if twice >= rcoeff.abs() {
                    quotient + (num.signum() * rcoeff.signum())
                } else {
                    quotient
                };
                Some(
                    Self::Finite {
                        coeff: rounded,
                        scale: out_scale,
                    }
                    .normalize(),
                )
            }
        }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::NaN, Self::NaN) => Ordering::Equal,
            (Self::NaN, _) => Ordering::Greater,
            (_, Self::NaN) => Ordering::Less,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                left.cmp(&right)
            }
        }
    }
}

fn align_coeff(coeff: BigInt, from_scale: u32, to_scale: u32) -> BigInt {
    coeff * pow10_bigint(to_scale - from_scale)
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

pub(crate) fn parse_numeric_text(text: &str) -> Option<NumericValue> {
    if text.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }

    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => {
            let exponent = trimmed[index + 1..].parse::<i32>().ok()?;
            (&trimmed[..index], exponent)
        }
        None => (trimmed, 0),
    };

    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let parts: Vec<&str> = unsigned.split('.').collect();
    if parts.len() > 2 {
        return None;
    }
    let whole = parts[0];
    let frac = parts.get(1).copied().unwrap_or("");
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit()) || !frac.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let mut digits = format!("{whole}{frac}");
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = frac.len() as i32 - exponent;
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', (-scale) as usize));
        scale = 0;
    }
    let mut coeff = digits.parse::<BigInt>().ok()?;
    if negative {
        coeff = -coeff;
    }
    Some(
        NumericValue::Finite {
            coeff,
            scale: scale as u32,
        }
        .normalize(),
    )
}

fn parsed_numeric_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(v) => Some(NumericValue::from_i64(*v as i64)),
        Value::Int32(v) => Some(NumericValue::from_i64(*v as i64)),
        Value::Int64(v) => Some(NumericValue::from_i64(*v)),
        Value::Numeric(v) => Some(v.clone()),
        Value::Float64(_) => None,
        _ => None,
    }
}

fn exact_numeric_binary(
    left: &Value,
    right: &Value,
    op: impl Fn(&NumericValue, &NumericValue) -> Option<NumericValue>,
    opname: &'static str,
) -> Result<Value, ExecError> {
    let left_num = parsed_numeric_value(left).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    let right_num = parsed_numeric_value(right).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    let result = op(&left_num, &right_num).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    Ok(Value::Numeric(result))
}
