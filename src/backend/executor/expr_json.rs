use super::exec_expr::eval_expr;
use super::expr_casts::{
    cast_text_value_with_catalog_and_config, cast_text_value_with_config,
    cast_value_with_source_type_catalog_and_config, enforce_domain_constraints_for_value,
    parse_text_array_literal_with_op,
};
use super::node_types::*;
use super::{ExecError, ExecutorContext};
use crate::backend::executor::expr_datetime::render_json_datetime_value_text_with_config;
use crate::backend::executor::jsonb::{
    JsonbValue, decode_jsonb, encode_jsonb, jsonb_builder_key, jsonb_from_value, jsonb_get,
    jsonb_object_from_pairs, jsonb_path, jsonb_to_text_value, jsonb_to_value,
    parse_json_text_input, parse_jsonb_text, render_jsonb_bytes, render_jsonb_value_text,
    render_temporal_jsonb_value,
};
use crate::backend::executor::jsonpath::{
    EvaluationContext as JsonPathEvaluationContext, canonicalize_jsonpath, evaluate_jsonpath,
    parse_jsonpath, validate_jsonpath,
};
use crate::backend::executor::render_bit_text;
use crate::backend::executor::render_datetime_value_text;
use crate::backend::executor::render_datetime_value_text_with_config;
use crate::backend::executor::render_interval_text;
use crate::backend::executor::render_interval_text_with_config;
use crate::backend::executor::render_macaddr_text;
use crate::backend::executor::render_macaddr8_text;
use crate::backend::executor::render_range_text;
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{CatalogLookup, ParseError};
use crate::backend::utils::record::lookup_anonymous_record_descriptor;
use crate::include::catalog::RECORD_TYPE_OID;
use crate::include::nodes::datum::{ArrayValue, RecordDescriptor, RecordValue};
use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};
use crate::include::nodes::primnodes::{
    BuiltinScalarFunction, Expr, JsonRecordFunction, QueryColumn, SqlJsonQueryFunction,
    SqlJsonQueryFunctionKind, SqlJsonTable, SqlJsonTableBehavior, SqlJsonTableColumnKind,
    SqlJsonTablePassingArg, SqlJsonTablePlan, SqlJsonTableQuotes, SqlJsonTableWrapper,
    expr_sql_type_hint,
};
use crate::include::nodes::tsearch::{TsLexeme, TsVector};
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;
use serde_json::Value as SerdeJsonValue;

pub(crate) fn validate_json_text(text: &str) -> Result<(), ExecError> {
    parse_json_text_input(text).map(|_| ())
}

fn parse_json_text(text: &str) -> Result<SerdeJsonValue, ExecError> {
    parse_json_text_input(text)
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
                JsonbValue::Date(_) => "date",
                JsonbValue::Time(_) => "time without time zone",
                JsonbValue::TimeTz(_) => "time with time zone",
                JsonbValue::Timestamp(_) => "timestamp without time zone",
                JsonbValue::TimestampTz(_) => "timestamp with time zone",
                JsonbValue::Array(_) => "array",
                JsonbValue::Object(_) => "object",
            },
        }
    }
}

#[derive(Clone, Copy)]
struct JsonbTsVectorFlags {
    key: bool,
    string: bool,
    numeric: bool,
    boolean: bool,
}

impl JsonbTsVectorFlags {
    fn strings_only() -> Self {
        Self {
            key: false,
            string: true,
            numeric: false,
            boolean: false,
        }
    }

    fn all() -> Self {
        Self {
            key: true,
            string: true,
            numeric: true,
            boolean: true,
        }
    }

    fn empty() -> Self {
        Self {
            key: false,
            string: false,
            numeric: false,
            boolean: false,
        }
    }

    fn enable(&mut self, flag: &str) -> Result<(), ExecError> {
        match flag {
            "all" => *self = Self::all(),
            "key" => self.key = true,
            "string" => self.string = true,
            "numeric" => self.numeric = true,
            "boolean" => self.boolean = true,
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!("wrong flag in flag array: \"{flag}\""),
                    detail: None,
                    hint: Some(jsonb_to_tsvector_flags_hint()),
                    sqlstate: "22023",
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn jsonb_to_tsvector_value(
    config_name: Option<&str>,
    jsonb: &Value,
    flags: Option<&Value>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let Value::Jsonb(bytes) = jsonb else {
        return Err(ExecError::TypeMismatch {
            op: "jsonb_to_tsvector",
            left: jsonb.clone(),
            right: Value::Null,
        });
    };
    let flags = match flags {
        Some(value) => parse_jsonb_to_tsvector_flags(value)?,
        None => JsonbTsVectorFlags::strings_only(),
    };
    let jsonb = decode_jsonb(bytes)?;
    let mut builder = JsonbTsVectorBuilder {
        config_name,
        catalog,
        flags,
        next_position: 1,
        lexemes: Vec::new(),
    };
    builder.add_value(&jsonb)?;
    Ok(Value::TsVector(TsVector::new(builder.lexemes)))
}

fn parse_jsonb_to_tsvector_flags(value: &Value) -> Result<JsonbTsVectorFlags, ExecError> {
    let parsed = ParsedJsonValue::from_value(value)?;
    let json = match parsed {
        ParsedJsonValue::Json(value) => value,
        ParsedJsonValue::Jsonb(value) => value.to_serde(),
    };
    match json {
        SerdeJsonValue::String(flag) => {
            let mut flags = JsonbTsVectorFlags::empty();
            flags.enable(&flag)?;
            Ok(flags)
        }
        SerdeJsonValue::Array(items) => {
            let mut flags = JsonbTsVectorFlags::empty();
            for item in items {
                let SerdeJsonValue::String(flag) = item else {
                    return Err(ExecError::DetailedError {
                        message: "flag array element is not a string".into(),
                        detail: None,
                        hint: Some(jsonb_to_tsvector_flags_hint()),
                        sqlstate: "22023",
                    });
                };
                flags.enable(&flag)?;
            }
            Ok(flags)
        }
        SerdeJsonValue::Null => Err(ExecError::DetailedError {
            message: "flag array element is not a string".into(),
            detail: None,
            hint: Some(jsonb_to_tsvector_flags_hint()),
            sqlstate: "22023",
        }),
        _ => Err(ExecError::DetailedError {
            message: "wrong flag type, only arrays and scalars are allowed".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn jsonb_to_tsvector_flags_hint() -> String {
    "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".".into()
}

struct JsonbTsVectorBuilder<'a> {
    config_name: Option<&'a str>,
    catalog: Option<&'a dyn CatalogLookup>,
    flags: JsonbTsVectorFlags,
    next_position: u16,
    lexemes: Vec<TsLexeme>,
}

impl JsonbTsVectorBuilder<'_> {
    fn add_value(&mut self, value: &JsonbValue) -> Result<(), ExecError> {
        match value {
            JsonbValue::Null => {}
            JsonbValue::String(text) => {
                if self.flags.string {
                    self.add_part(text, true)?;
                }
            }
            JsonbValue::Numeric(value) => {
                if self.flags.numeric {
                    self.add_part(&value.render(), true)?;
                }
            }
            JsonbValue::Bool(value) => {
                if self.flags.boolean {
                    self.add_part(if *value { "true" } else { "false" }, true)?;
                }
            }
            JsonbValue::Array(items) => {
                for item in items {
                    self.add_value(item)?;
                }
            }
            JsonbValue::Object(items) => {
                for (key, value) in items {
                    self.add_object_item(key, value)?;
                }
            }
            JsonbValue::Date(_)
            | JsonbValue::Time(_)
            | JsonbValue::TimeTz(_)
            | JsonbValue::Timestamp(_)
            | JsonbValue::TimestampTz(_) => {
                if self.flags.string {
                    self.add_part(&render_temporal_jsonb_value(value), true)?;
                }
            }
        }
        Ok(())
    }

    fn add_object_item(&mut self, key: &str, value: &JsonbValue) -> Result<(), ExecError> {
        let key_emitted = if self.flags.key {
            self.add_part(key, false)?
        } else {
            false
        };
        if key_emitted {
            self.next_position = self.next_position.saturating_add(1);
        }
        self.add_value(value)?;
        Ok(())
    }

    fn add_part(&mut self, text: &str, add_gap: bool) -> Result<bool, ExecError> {
        let (mut lexemes, next_position) =
            crate::backend::tsearch::tsvector_lexemes_with_config_name(
                self.config_name,
                text,
                self.next_position,
                self.catalog,
            )
            .map_err(|e| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "valid text search input",
                    actual: format!("jsonb_to_tsvector: {e}"),
                })
            })?;
        let emitted = !lexemes.is_empty();
        self.lexemes.append(&mut lexemes);
        self.next_position = next_position;
        if add_gap && next_position != 1 {
            self.next_position = self.next_position.saturating_add(1);
        }
        Ok(emitted)
    }
}

fn json_array_length_error(value: ParsedJsonValue) -> ExecError {
    let message = match value {
        ParsedJsonValue::Json(SerdeJsonValue::Object(_))
        | ParsedJsonValue::Jsonb(JsonbValue::Object(_)) => "cannot get array length of a non-array",
        ParsedJsonValue::Json(SerdeJsonValue::Array(_))
        | ParsedJsonValue::Jsonb(JsonbValue::Array(_)) => {
            unreachable!("array length errors should only be raised for non-arrays")
        }
        ParsedJsonValue::Json(_) | ParsedJsonValue::Jsonb(_) => {
            "cannot get array length of a scalar"
        }
    };
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn json_object_keys_non_object_error(func_name: &'static str, value: &SerdeJsonValue) -> ExecError {
    let target = match value {
        SerdeJsonValue::Array(_) => "an array",
        _ => "a scalar",
    };
    ExecError::DetailedError {
        message: format!("cannot call {func_name} on {target}"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn jsonb_non_object_error(func_name: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot call {func_name} on a non-object"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

#[derive(Clone, Default)]
struct JsonRecordPath {
    key: Option<String>,
    indexes: Vec<usize>,
}

impl JsonRecordPath {
    fn with_key(&self, key: &str) -> Self {
        Self {
            key: Some(key.to_string()),
            indexes: Vec::new(),
        }
    }

    fn with_index(&self, index: usize) -> Self {
        let mut next = self.clone();
        next.indexes.push(index);
        next
    }

    fn hint(&self) -> Option<String> {
        let key = self.key.as_deref()?;
        if self.indexes.is_empty() {
            Some(format!("See the value of key \"{key}\"."))
        } else {
            let suffix = self
                .indexes
                .iter()
                .map(|index| format!("[{index}]"))
                .collect::<String>();
            Some(format!("See the array element {suffix} of key \"{key}\"."))
        }
    }
}

pub(crate) fn eval_json_record_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Option<Result<Value, ExecError>> {
    let result = match func {
        BuiltinScalarFunction::JsonPopulateRecord => eval_json_record_scalar_function(
            "json_populate_record",
            true,
            true,
            result_type,
            args,
            slot,
            ctx,
        ),
        BuiltinScalarFunction::JsonPopulateRecordValid => {
            eval_json_record_valid_function("json_populate_record_valid", true, args, slot, ctx)
        }
        BuiltinScalarFunction::JsonToRecord => eval_json_record_scalar_function(
            "json_to_record",
            false,
            true,
            result_type,
            args,
            slot,
            ctx,
        ),
        BuiltinScalarFunction::JsonbPopulateRecord => eval_json_record_scalar_function(
            "jsonb_populate_record",
            true,
            false,
            result_type,
            args,
            slot,
            ctx,
        ),
        BuiltinScalarFunction::JsonbPopulateRecordValid => {
            eval_json_record_valid_function("jsonb_populate_record_valid", false, args, slot, ctx)
        }
        BuiltinScalarFunction::JsonbToRecord => eval_json_record_scalar_function(
            "jsonb_to_record",
            false,
            false,
            result_type,
            args,
            slot,
            ctx,
        ),
        _ => return None,
    };
    Some(result)
}

pub(crate) fn eval_json_record_set_returning_function(
    kind: JsonRecordFunction,
    args: &[Expr],
    output_columns: &[QueryColumn],
    record_type: Option<SqlType>,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let func_name = kind.name();
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let (populate, expect_json) = match kind {
        JsonRecordFunction::PopulateRecord | JsonRecordFunction::PopulateRecordSet => (true, true),
        JsonRecordFunction::ToRecord | JsonRecordFunction::ToRecordSet => (false, true),
        JsonRecordFunction::JsonbPopulateRecord | JsonRecordFunction::JsonbPopulateRecordSet => {
            (true, false)
        }
        JsonRecordFunction::JsonbToRecord | JsonRecordFunction::JsonbToRecordSet => (false, false),
    };
    let render_jsonb_style = !expect_json;

    let base_value = if populate {
        values.first().cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let Some(json_value) = values.get(if populate { 1 } else { 0 }) else {
        return Err(ExecError::RaiseException(format!(
            "missing arguments for {func_name}"
        )));
    };

    if matches!(json_value, Value::Null) {
        if kind.is_set_returning() {
            return Ok(Vec::new());
        }
        return Ok(vec![TupleSlot::virtual_row(
            output_columns.iter().map(|_| Value::Null).collect(),
        )]);
    }
    ensure_json_record_runtime_row_type(func_name, &base_value, output_columns, record_type, ctx)?;

    let parsed = parsed_json_record_input(json_value, expect_json)?;
    let rows = if kind.is_set_returning() {
        match parsed {
            SerdeJsonValue::Array(items) => items
                .iter()
                .map(|item| {
                    json_record_row_for_output(
                        func_name,
                        item,
                        &base_value,
                        output_columns,
                        record_type,
                        render_jsonb_style,
                        ctx,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(expected_json_array_error(&JsonRecordPath::default(), None)),
        }
    } else {
        vec![json_record_row_for_output(
            func_name,
            &parsed,
            &base_value,
            output_columns,
            record_type,
            render_jsonb_style,
            ctx,
        )?]
    };

    Ok(rows)
}

pub(crate) fn eval_json_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<Result<Value, ExecError>> {
    let eval = || -> Result<Value, ExecError> {
        match func {
            BuiltinScalarFunction::ToJson => {
                let value = values.first().cloned().unwrap_or(Value::Null);
                Ok(Value::Json(CompactString::from_owned(value_to_json_text(
                    &value,
                    false,
                    datetime_config,
                    catalog,
                ))))
            }
            BuiltinScalarFunction::ToJsonb => {
                let value = values.first().cloned().unwrap_or(Value::Null);
                Ok(Value::Jsonb(encode_jsonb(&jsonb_from_value(
                    &value,
                    datetime_config,
                )?)))
            }
            BuiltinScalarFunction::SqlJsonConstructor => {
                eval_sql_json_constructor(values.first().unwrap_or(&Value::Null), result_type)
            }
            BuiltinScalarFunction::SqlJsonScalar => {
                let value = values.first().cloned().unwrap_or(Value::Null);
                let result = if matches!(value, Value::Null) {
                    Value::Null
                } else {
                    Value::Json(CompactString::from_owned(value_to_json_text(
                        &value,
                        false,
                        datetime_config,
                        catalog,
                    )))
                };
                coerce_sql_json_return_value(result, result_type)
            }
            BuiltinScalarFunction::SqlJsonSerialize => {
                eval_sql_json_serialize(values.first().unwrap_or(&Value::Null), result_type)
            }
            BuiltinScalarFunction::SqlJsonObject => {
                eval_sql_json_object(values, result_type, datetime_config, catalog)
            }
            BuiltinScalarFunction::SqlJsonArray => {
                eval_sql_json_array(values, result_type, datetime_config, catalog)
            }
            BuiltinScalarFunction::SqlJsonIsJson => eval_sql_json_is_json(values),
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
                    &value,
                    pretty,
                    datetime_config,
                    catalog,
                ))))
            }
            BuiltinScalarFunction::RowToJson => {
                let value = values.first().cloned().unwrap_or(Value::Null);
                let pretty = values
                    .get(1)
                    .and_then(|value| match value {
                        Value::Bool(v) => Some(*v),
                        _ => None,
                    })
                    .unwrap_or(false);
                Ok(Value::Json(CompactString::from_owned(value_to_json_text(
                    &value,
                    pretty,
                    datetime_config,
                    catalog,
                ))))
            }
            BuiltinScalarFunction::JsonBuildArray => {
                let Some(args) = variadic_args(values, func_variadic, 0, "json_build_array")?
                else {
                    return Ok(Value::Null);
                };
                Ok(Value::Json(CompactString::from_owned(
                    render_json_builder_array(&args),
                )))
            }
            BuiltinScalarFunction::JsonBuildObject => {
                let Some(args) = variadic_args(values, func_variadic, 0, "json_build_object")?
                else {
                    return Ok(Value::Null);
                };
                Ok(Value::Json(CompactString::from_owned(
                    render_json_builder_object(&args)?,
                )))
            }
            BuiltinScalarFunction::JsonObject => Ok(Value::Json(CompactString::from_owned(
                render_json_object_function(values)?,
            ))),
            BuiltinScalarFunction::JsonStripNulls => {
                if matches!(values.first(), None | Some(Value::Null)) {
                    return Ok(Value::Null);
                }
                let strip_in_arrays =
                    parse_optional_bool_flag(values.get(1), false, "json_strip_nulls")?;
                let json = ParsedJsonValue::from_value(values.first().unwrap_or(&Value::Null))?;
                let parsed = match json {
                    ParsedJsonValue::Json(json) => json,
                    ParsedJsonValue::Jsonb(jsonb) => jsonb.to_serde(),
                };
                Ok(Value::Json(CompactString::from_owned(
                    render_serde_json_value_text(&strip_json_nulls(&parsed, strip_in_arrays)),
                )))
            }
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
                    other => Err(json_array_length_error(other)),
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
                    other => Err(json_array_length_error(other)),
                }
            }
            BuiltinScalarFunction::JsonExtractPath => {
                let Some(args) = variadic_args(values, func_variadic, 1, "json_extract_path")?
                else {
                    return Ok(Value::Null);
                };
                let path = parse_json_path_args(&args[1..])?;
                Ok(
                    match ParsedJsonValue::from_value(args.first().unwrap_or(&Value::Null))? {
                        ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                            .map(|value| json_value_to_value(value, false, false))
                            .unwrap_or(Value::Null),
                        ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                            .map(jsonb_to_value)
                            .unwrap_or(Value::Null),
                    },
                )
            }
            BuiltinScalarFunction::JsonExtractPathText => {
                let Some(args) = variadic_args(values, func_variadic, 1, "json_extract_path_text")?
                else {
                    return Ok(Value::Null);
                };
                let path = parse_json_path_args(&args[1..])?;
                Ok(
                    match ParsedJsonValue::from_value(args.first().unwrap_or(&Value::Null))? {
                        ParsedJsonValue::Json(json) => json_lookup_path(&json, &path)
                            .map(|value| json_value_to_value(value, true, false))
                            .unwrap_or(Value::Null),
                        ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                            .map(jsonb_to_text_value)
                            .unwrap_or(Value::Null),
                    },
                )
            }
            BuiltinScalarFunction::JsonbExtractPath => {
                let Some(args) = variadic_args(values, func_variadic, 1, "jsonb_extract_path")?
                else {
                    return Ok(Value::Null);
                };
                let path = parse_json_path_args(&args[1..])?;
                Ok(
                    match ParsedJsonValue::from_value(args.first().unwrap_or(&Value::Null))? {
                        ParsedJsonValue::Json(json) => match JsonbValue::from_serde(json) {
                            Ok(jsonb) => jsonb_path(&jsonb, &path)
                                .map(jsonb_to_value)
                                .unwrap_or(Value::Null),
                            Err(_) => Value::Null,
                        },
                        ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                            .map(jsonb_to_value)
                            .unwrap_or(Value::Null),
                    },
                )
            }
            BuiltinScalarFunction::JsonbExtractPathText => {
                let Some(args) =
                    variadic_args(values, func_variadic, 1, "jsonb_extract_path_text")?
                else {
                    return Ok(Value::Null);
                };
                let path = parse_json_path_args(&args[1..])?;
                Ok(
                    match ParsedJsonValue::from_value(args.first().unwrap_or(&Value::Null))? {
                        ParsedJsonValue::Json(json) => match JsonbValue::from_serde(json) {
                            Ok(jsonb) => jsonb_path(&jsonb, &path)
                                .map(jsonb_to_text_value)
                                .unwrap_or(Value::Null),
                            Err(_) => Value::Null,
                        },
                        ParsedJsonValue::Jsonb(jsonb) => jsonb_path(&jsonb, &path)
                            .map(jsonb_to_text_value)
                            .unwrap_or(Value::Null),
                    },
                )
            }
            BuiltinScalarFunction::JsonbBuildArray => {
                let Some(args) = variadic_args(values, func_variadic, 0, "jsonb_build_array")?
                else {
                    return Ok(Value::Null);
                };
                let mut items = Vec::with_capacity(args.len());
                for value in &args {
                    items.push(jsonb_from_value(value, datetime_config)?);
                }
                Ok(Value::Jsonb(encode_jsonb(&JsonbValue::Array(items))))
            }
            BuiltinScalarFunction::JsonbBuildObject => {
                let Some(args) = variadic_args(values, func_variadic, 0, "jsonb_build_object")?
                else {
                    return Ok(Value::Null);
                };
                let pairs = json_builder_pairs(&args, "jsonb_build_object")?;
                Ok(Value::Jsonb(encode_jsonb(&jsonb_object_from_pairs(
                    &pairs,
                )?)))
            }
            BuiltinScalarFunction::JsonbContains => {
                let left =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_contains")?;
                let right =
                    parse_jsonb_target(values.get(1).unwrap_or(&Value::Null), "jsonb_contains")?;
                Ok(Value::Bool(
                    crate::backend::executor::jsonb::jsonb_contains(&left, &right),
                ))
            }
            BuiltinScalarFunction::JsonbContained => {
                let left =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_contained")?;
                let right =
                    parse_jsonb_target(values.get(1).unwrap_or(&Value::Null), "jsonb_contained")?;
                Ok(Value::Bool(
                    crate::backend::executor::jsonb::jsonb_contains(&right, &left),
                ))
            }
            BuiltinScalarFunction::JsonbObject => Ok(Value::Jsonb(encode_jsonb(
                &render_jsonb_object_function(values)?,
            ))),
            BuiltinScalarFunction::JsonbStripNulls => {
                if matches!(values.first(), None | Some(Value::Null)) {
                    return Ok(Value::Null);
                }
                let strip_in_arrays =
                    parse_optional_bool_flag(values.get(1), false, "jsonb_strip_nulls")?;
                let json = parse_jsonb_target(
                    values.first().unwrap_or(&Value::Null),
                    "jsonb_strip_nulls",
                )?;
                Ok(Value::Jsonb(encode_jsonb(&strip_jsonb_nulls(
                    &json,
                    strip_in_arrays,
                ))))
            }
            BuiltinScalarFunction::JsonbPretty => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_pretty")?;
                Ok(Value::Text(CompactString::from_owned(
                    serde_json::to_string_pretty(&json.to_serde()).unwrap(),
                )))
            }
            BuiltinScalarFunction::JsonbDelete => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_delete")?;
                Ok(Value::Jsonb(encode_jsonb(&apply_jsonb_delete(
                    &json,
                    values.get(1).unwrap_or(&Value::Null),
                )?)))
            }
            BuiltinScalarFunction::JsonbExists => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_exists")?;
                let key = values.get(1).and_then(Value::as_text).ok_or_else(|| {
                    ExecError::TypeMismatch {
                        op: "jsonb_exists",
                        left: values.get(1).cloned().unwrap_or(Value::Null),
                        right: Value::Text("key".into()),
                    }
                })?;
                Ok(Value::Bool(crate::backend::executor::jsonb::jsonb_exists(
                    &json, key,
                )))
            }
            BuiltinScalarFunction::JsonbExistsAny => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_exists_any")?;
                let keys = parse_text_array_arg(
                    values.get(1).unwrap_or(&Value::Null),
                    "jsonb_exists_any",
                )?;
                Ok(Value::Bool(
                    crate::backend::executor::jsonb::jsonb_exists_any(&json, &keys),
                ))
            }
            BuiltinScalarFunction::JsonbExistsAll => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_exists_all")?;
                let keys = parse_text_array_arg(
                    values.get(1).unwrap_or(&Value::Null),
                    "jsonb_exists_all",
                )?;
                Ok(Value::Bool(
                    crate::backend::executor::jsonb::jsonb_exists_all(&json, &keys),
                ))
            }
            BuiltinScalarFunction::JsonbDeletePath => {
                let json = parse_jsonb_target(
                    values.first().unwrap_or(&Value::Null),
                    "jsonb_delete_path",
                )?;
                let path = parse_jsonb_path_arg(
                    values.get(1).unwrap_or(&Value::Null),
                    "jsonb_delete_path",
                )?;
                Ok(Value::Jsonb(encode_jsonb(&delete_jsonb_path(
                    &json, &path,
                )?)))
            }
            BuiltinScalarFunction::JsonbSet => {
                let json = parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_set")?;
                let path =
                    parse_jsonb_path_arg(values.get(1).unwrap_or(&Value::Null), "jsonb_set")?;
                let replacement =
                    parse_jsonb_target(values.get(2).unwrap_or(&Value::Null), "jsonb_set")?;
                let create_missing = parse_optional_bool_flag(values.get(3), true, "jsonb_set")?;
                Ok(Value::Jsonb(encode_jsonb(&set_jsonb_path(
                    &json,
                    &path,
                    replacement,
                    create_missing,
                    false,
                    false,
                )?)))
            }
            BuiltinScalarFunction::JsonbSetLax => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_set_lax")?;
                let path =
                    parse_jsonb_path_arg(values.get(1).unwrap_or(&Value::Null), "jsonb_set_lax")?;
                let create_missing =
                    parse_optional_bool_flag(values.get(3), true, "jsonb_set_lax")?;
                match values.get(2).unwrap_or(&Value::Null) {
                    Value::Null => {
                        let treatment = parse_jsonb_set_lax_treatment(values.get(4))?;
                        let result = match treatment.as_str() {
                            "use_json_null" => set_jsonb_path(
                                &json,
                                &path,
                                JsonbValue::Null,
                                create_missing,
                                false,
                                false,
                            )?,
                            "delete_key" => delete_jsonb_path(&json, &path)?,
                            "return_target" => json,
                            "raise_exception" => {
                                return Err(ExecError::DetailedError {
                                    message: "JSON value must not be null".into(),
                                    detail: Some(
                                        "Exception was raised because null_value_treatment is \"raise_exception\"."
                                            .into(),
                                    ),
                                    hint: Some(
                                        "To avoid, either change the null_value_treatment argument or ensure that an SQL NULL is not passed."
                                            .into(),
                                    ),
                                    sqlstate: "22023",
                                });
                            }
                            _ => unreachable!(),
                        };
                        Ok(Value::Jsonb(encode_jsonb(&result)))
                    }
                    value => {
                        let replacement = parse_jsonb_target(value, "jsonb_set_lax")?;
                        Ok(Value::Jsonb(encode_jsonb(&set_jsonb_path(
                            &json,
                            &path,
                            replacement,
                            create_missing,
                            false,
                            false,
                        )?)))
                    }
                }
            }
            BuiltinScalarFunction::JsonbInsert => {
                let json =
                    parse_jsonb_target(values.first().unwrap_or(&Value::Null), "jsonb_insert")?;
                let path =
                    parse_jsonb_path_arg(values.get(1).unwrap_or(&Value::Null), "jsonb_insert")?;
                let replacement =
                    parse_jsonb_target(values.get(2).unwrap_or(&Value::Null), "jsonb_insert")?;
                let insert_after = parse_optional_bool_flag(values.get(3), false, "jsonb_insert")?;
                Ok(Value::Jsonb(encode_jsonb(&set_jsonb_path(
                    &json,
                    &path,
                    replacement,
                    true,
                    insert_after,
                    true,
                )?)))
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
            BuiltinScalarFunction::JsonExists => eval_sql_json_exists_function(values),
            BuiltinScalarFunction::JsonValue => eval_sql_json_value_function(values),
            BuiltinScalarFunction::JsonQuery => eval_sql_json_query_function(values),
            _ => unreachable!(),
        }
    };

    match func {
        BuiltinScalarFunction::ToJson
        | BuiltinScalarFunction::ToJsonb
        | BuiltinScalarFunction::SqlJsonConstructor
        | BuiltinScalarFunction::SqlJsonScalar
        | BuiltinScalarFunction::SqlJsonSerialize
        | BuiltinScalarFunction::SqlJsonObject
        | BuiltinScalarFunction::SqlJsonArray
        | BuiltinScalarFunction::SqlJsonIsJson
        | BuiltinScalarFunction::ArrayToJson
        | BuiltinScalarFunction::RowToJson
        | BuiltinScalarFunction::JsonBuildArray
        | BuiltinScalarFunction::JsonBuildObject
        | BuiltinScalarFunction::JsonObject
        | BuiltinScalarFunction::JsonStripNulls
        | BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonbArrayLength
        | BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbObject
        | BuiltinScalarFunction::JsonbStripNulls
        | BuiltinScalarFunction::JsonbPretty
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText
        | BuiltinScalarFunction::JsonbBuildArray
        | BuiltinScalarFunction::JsonbBuildObject
        | BuiltinScalarFunction::JsonbContains
        | BuiltinScalarFunction::JsonbContained
        | BuiltinScalarFunction::JsonbDelete
        | BuiltinScalarFunction::JsonbDeletePath
        | BuiltinScalarFunction::JsonbExists
        | BuiltinScalarFunction::JsonbExistsAny
        | BuiltinScalarFunction::JsonbExistsAll
        | BuiltinScalarFunction::JsonbSet
        | BuiltinScalarFunction::JsonbSetLax
        | BuiltinScalarFunction::JsonbInsert
        | BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::JsonbPathQueryArray
        | BuiltinScalarFunction::JsonbPathQueryFirst
        | BuiltinScalarFunction::JsonExists
        | BuiltinScalarFunction::JsonValue
        | BuiltinScalarFunction::JsonQuery => Some(eval()),
        _ => None,
    }
}

fn eval_json_record_scalar_function(
    func_name: &'static str,
    populate: bool,
    expect_json: bool,
    result_type: Option<SqlType>,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let base_value = if populate {
        values.first().cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let Some(json_value) = values.get(if populate { 1 } else { 0 }) else {
        return Err(ExecError::RaiseException(format!(
            "missing arguments for {func_name}"
        )));
    };
    if matches!(json_value, Value::Null) {
        return Ok(Value::Null);
    }
    let descriptor = scalar_json_record_descriptor(func_name, result_type, args, &base_value, ctx)?;
    let parsed = parsed_json_record_input(json_value, expect_json)?;
    let row = json_record_row_from_value(
        func_name,
        &parsed,
        &base_value,
        &record_columns_from_descriptor(&descriptor),
        !expect_json,
        ctx,
    )?;
    let record = Value::Record(RecordValue::from_descriptor(descriptor, row));
    if let Some(record_type) = result_type.or_else(|| args.first().and_then(expr_sql_type_hint)) {
        json_record_enforce_domain(record, record_type, &JsonRecordPath::default(), ctx)
    } else {
        Ok(record)
    }
}

fn eval_json_record_valid_function(
    func_name: &'static str,
    expect_json: bool,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let base_value = values.first().cloned().unwrap_or(Value::Null);
    let Some(json_value) = values.get(1) else {
        return Err(ExecError::RaiseException(format!(
            "missing arguments for {func_name}"
        )));
    };
    if matches!(json_value, Value::Null) {
        return Ok(Value::Null);
    }
    let row_type = args
        .first()
        .and_then(expr_sql_type_hint)
        .ok_or_else(|| json_record_row_type_error(func_name))?;
    let Some(descriptor) = record_descriptor_from_sql_type(row_type, ctx)? else {
        return Err(json_record_row_type_error(func_name));
    };
    let parsed = parsed_json_record_input(json_value, expect_json)?;
    Ok(Value::Bool(
        json_record_row_from_value(
            func_name,
            &parsed,
            &base_value,
            &record_columns_from_descriptor(&descriptor),
            !expect_json,
            ctx,
        )
        .is_ok(),
    ))
}

fn scalar_json_record_descriptor(
    func_name: &'static str,
    result_type: Option<SqlType>,
    args: &[Expr],
    base_value: &Value,
    ctx: &ExecutorContext,
) -> Result<RecordDescriptor, ExecError> {
    if let Value::Record(record) = base_value {
        return Ok(record.descriptor.clone());
    }
    if let Some(descriptor) = result_type
        .map(|ty| record_descriptor_from_sql_type(ty, ctx))
        .transpose()?
        .flatten()
    {
        return Ok(descriptor);
    }
    if let Some(descriptor) = args
        .first()
        .and_then(expr_sql_type_hint)
        .map(|ty| record_descriptor_from_sql_type(ty, ctx))
        .transpose()?
        .flatten()
    {
        return Ok(descriptor);
    }
    Err(json_record_row_type_error(func_name))
}

fn parsed_json_record_input(value: &Value, expect_json: bool) -> Result<SerdeJsonValue, ExecError> {
    match ParsedJsonValue::from_value(value)? {
        ParsedJsonValue::Json(json) if expect_json => Ok(json),
        ParsedJsonValue::Jsonb(jsonb) if !expect_json => Ok(jsonb.to_serde()),
        ParsedJsonValue::Json(json) => Ok(json),
        ParsedJsonValue::Jsonb(jsonb) => Ok(jsonb.to_serde()),
    }
}

fn json_record_row_for_output(
    func_name: &'static str,
    value: &SerdeJsonValue,
    base_value: &Value,
    output_columns: &[QueryColumn],
    record_type: Option<SqlType>,
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<TupleSlot, ExecError> {
    if let Some(record_type) = record_type {
        let return_as_record = output_columns.len() == 1
            && output_columns[0].sql_type.kind == record_type.kind
            && output_columns[0].sql_type.type_oid == record_type.type_oid
            && output_columns[0].sql_type.typrelid == record_type.typrelid;
        let descriptor = if let Value::Record(record) = base_value {
            record.descriptor.clone()
        } else if let Some(descriptor) = record_descriptor_from_sql_type(record_type, ctx)? {
            descriptor
        } else if return_as_record {
            return Err(json_record_row_type_error(func_name));
        } else {
            record_descriptor_from_query_columns(output_columns)
        };
        let row = json_record_row_from_value(
            func_name,
            value,
            base_value,
            &record_columns_from_descriptor(&descriptor),
            render_jsonb_style,
            ctx,
        )?;
        let record = json_record_enforce_domain(
            Value::Record(RecordValue::from_descriptor(descriptor, row)),
            record_type,
            &JsonRecordPath::default(),
            ctx,
        )?;
        if return_as_record {
            Ok(TupleSlot::virtual_row(vec![record]))
        } else {
            let Value::Record(record) = record else {
                unreachable!("record enforcement preserves record values");
            };
            Ok(TupleSlot::virtual_row(record.fields))
        }
    } else {
        let row = json_record_row_from_value(
            func_name,
            value,
            base_value,
            output_columns,
            render_jsonb_style,
            ctx,
        )?;
        Ok(TupleSlot::virtual_row(row))
    }
}

fn ensure_json_record_runtime_row_type(
    func_name: &'static str,
    base_value: &Value,
    output_columns: &[QueryColumn],
    record_type: Option<SqlType>,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    let Some(record_type) = record_type else {
        return Ok(());
    };
    let return_as_record = output_columns.len() == 1
        && output_columns[0].sql_type.kind == record_type.kind
        && output_columns[0].sql_type.type_oid == record_type.type_oid
        && output_columns[0].sql_type.typrelid == record_type.typrelid;
    if !return_as_record || matches!(base_value, Value::Record(_)) {
        return Ok(());
    }
    if record_descriptor_from_sql_type(record_type, ctx)?.is_some() {
        return Ok(());
    }
    Err(json_record_row_type_error(func_name))
}

fn json_record_row_from_value(
    func_name: &'static str,
    value: &SerdeJsonValue,
    base_value: &Value,
    output_columns: &[QueryColumn],
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let object = match value {
        SerdeJsonValue::Object(object) => object,
        _ => {
            return Err(ExecError::DetailedError {
                message: format!("expected JSON object for {func_name}"),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
    };
    let base_fields = match base_value {
        Value::Record(record) if record.fields.len() == output_columns.len() => {
            Some(&record.fields)
        }
        Value::Null => None,
        Value::Record(_) => None,
        other => {
            return Err(ExecError::TypeMismatch {
                op: func_name,
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    output_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if let Some(value) = object.get(&column.name) {
                json_record_field_to_value(
                    value,
                    column.sql_type,
                    base_fields.and_then(|fields| fields.get(index)),
                    &JsonRecordPath::default().with_key(&column.name),
                    render_jsonb_style,
                    ctx,
                )
            } else {
                match base_fields.and_then(|fields| fields.get(index)).cloned() {
                    Some(value) => Ok(value),
                    None => json_record_missing_field_to_value(
                        column.sql_type,
                        &JsonRecordPath::default().with_key(&column.name),
                        ctx,
                    ),
                }
            }
        })
        .collect()
}

fn json_record_field_to_value(
    value: &SerdeJsonValue,
    ty: SqlType,
    base_value: Option<&Value>,
    path: &JsonRecordPath,
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(value, SerdeJsonValue::Null) {
        return json_record_null_to_value(ty, path, ctx);
    }
    if ty.is_array {
        return json_record_array_to_value(value, ty, path, render_jsonb_style, ctx);
    }
    match ty.kind {
        SqlTypeKind::Json => json_record_enforce_domain(
            json_value_to_value(value, false, render_jsonb_style),
            ty,
            path,
            ctx,
        ),
        SqlTypeKind::Jsonb => json_record_enforce_domain(
            Value::Jsonb(encode_jsonb(&JsonbValue::from_serde(value.clone())?)),
            ty,
            path,
            ctx,
        ),
        SqlTypeKind::Record | SqlTypeKind::Composite => {
            let Some(descriptor) = record_descriptor_from_sql_type(ty, ctx)? else {
                return Err(json_record_row_type_error("json record expansion"));
            };
            match value {
                SerdeJsonValue::Object(_) => {
                    let nested_base = match base_value {
                        Some(Value::Record(record))
                            if record.fields.len() == descriptor.fields.len() =>
                        {
                            Value::Record(record.clone())
                        }
                        _ => Value::Null,
                    };
                    let fields = json_record_row_from_value(
                        "json record expansion",
                        value,
                        &nested_base,
                        &record_columns_from_descriptor(&descriptor),
                        render_jsonb_style,
                        ctx,
                    )?;
                    json_record_enforce_domain(
                        Value::Record(RecordValue::from_descriptor(descriptor, fields)),
                        ty,
                        path,
                        ctx,
                    )
                }
                SerdeJsonValue::String(text) => json_record_enforce_domain(
                    parse_record_literal_to_value(text, descriptor, ctx)?,
                    ty,
                    path,
                    ctx,
                ),
                _ => Err(populate_composite_type_error(value)),
            }
        }
        _ => cast_json_scalar_value(value, ty, path, render_jsonb_style, ctx),
    }
}

fn json_record_missing_field_to_value(
    ty: SqlType,
    path: &JsonRecordPath,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    json_record_null_to_value(ty, path, ctx)
}

fn json_record_null_to_value(
    ty: SqlType,
    path: &JsonRecordPath,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    json_record_enforce_domain(Value::Null, ty, path, ctx)
}

fn json_record_enforce_domain(
    value: Value,
    ty: SqlType,
    path: &JsonRecordPath,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    enforce_domain_constraints_for_value(value, ty, ctx.catalog.as_deref())
        .map_err(|err| json_record_error_with_hint(err, path.hint()))
}

fn cast_json_scalar_value(
    value: &SerdeJsonValue,
    ty: SqlType,
    path: &JsonRecordPath,
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let text = json_record_scalar_text(value, render_jsonb_style)?;
    json_record_cast_value(Value::Text(CompactString::from_owned(text)), ty, path, ctx)
}

fn json_record_array_to_value(
    value: &SerdeJsonValue,
    ty: SqlType,
    path: &JsonRecordPath,
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match value {
        SerdeJsonValue::String(text) => {
            if let Some(value) = json_record_composite_array_literal_to_value(text, ty, path, ctx)?
            {
                return Ok(value);
            }
            json_record_cast_value(
                Value::Text(CompactString::from_owned(text.clone())),
                ty,
                path,
                ctx,
            )
        }
        SerdeJsonValue::Array(items) => {
            let mut saw_array = false;
            let mut saw_scalar = false;
            let element_type = json_record_array_element_type(ty, ctx);
            let nested = items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let next_path = path.with_index(index);
                    let is_array = matches!(item, SerdeJsonValue::Array(_));
                    if matches!(element_type.kind, SqlTypeKind::Json | SqlTypeKind::Jsonb)
                        && !element_type.is_array
                    {
                        return json_record_field_to_value(
                            item,
                            element_type,
                            None,
                            &next_path,
                            render_jsonb_style,
                            ctx,
                        );
                    }
                    if is_array {
                        if saw_scalar {
                            return Err(expected_json_array_error(&next_path, next_path.hint()));
                        }
                        saw_array = true;
                    } else if saw_array {
                        return Err(expected_json_array_error(&next_path, next_path.hint()));
                    } else {
                        saw_scalar = true;
                    }
                    json_record_array_nested_value(
                        item,
                        element_type,
                        &next_path,
                        render_jsonb_style,
                        ctx,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let array = ArrayValue::from_nested_values(nested, vec![1]).map_err(|details| {
                ExecError::DetailedError {
                    message: "malformed JSON array".into(),
                    detail: Some(json_record_array_shape_detail(&details)),
                    hint: None,
                    sqlstate: "22P02",
                }
            })?;
            json_record_enforce_domain(Value::PgArray(array), ty, path, ctx)
        }
        _ => Err(expected_json_array_error(path, path.hint())),
    }
}

fn json_record_cast_value(
    value: Value,
    ty: SqlType,
    path: &JsonRecordPath,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if !ty.is_array
        && let Some(text) = value.as_text()
    {
        let casted = cast_text_value_with_config(text, ty, false, &ctx.datetime_config)
            .map_err(|err| json_record_error_with_hint(err, path.hint()))?;
        return json_record_enforce_domain(casted, ty, path, ctx);
    }
    cast_value_with_source_type_catalog_and_config(
        value,
        None,
        ty,
        ctx.catalog.as_deref(),
        &ctx.datetime_config,
    )
    .map_err(|err| json_record_error_with_hint(err, path.hint()))
}

fn json_record_array_element_type(ty: SqlType, ctx: &ExecutorContext) -> SqlType {
    if let Some(domain) = ctx
        .catalog
        .as_deref()
        .and_then(|catalog| catalog.domain_by_type_oid(ty.type_oid))
        && domain.sql_type.is_array
    {
        return domain.sql_type.element_type();
    }
    ty.element_type()
}

fn json_record_composite_array_literal_to_value(
    text: &str,
    ty: SqlType,
    path: &JsonRecordPath,
    ctx: &ExecutorContext,
) -> Result<Option<Value>, ExecError> {
    let element_type = json_record_array_element_type(ty, ctx);
    if !matches!(
        element_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) {
        return Ok(None);
    }
    let Some(descriptor) = record_descriptor_from_sql_type(element_type, ctx)? else {
        return Ok(None);
    };
    let elements = parse_record_array_literal_elements(text)
        .map_err(|err| json_record_error_with_hint(err, path.hint()))?;
    let values = elements
        .into_iter()
        .map(|element| match element {
            None => json_record_null_to_value(element_type, path, ctx),
            Some(raw) => json_record_enforce_domain(
                parse_record_literal_to_value(&raw, descriptor.clone(), ctx)?,
                element_type,
                path,
                ctx,
            ),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut array = ArrayValue::from_1d(values);
    if element_type.type_oid != 0 {
        array = array.with_element_type_oid(element_type.type_oid);
    }
    json_record_enforce_domain(Value::PgArray(array), ty, path, ctx).map(Some)
}

fn parse_record_array_literal_elements(text: &str) -> Result<Vec<Option<String>>, ExecError> {
    let body = text
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| ExecError::ArrayInput {
            message: format!("malformed array literal: \"{text}\""),
            value: text.into(),
            detail: Some("Array value must start with \"{\" or dimension information.".into()),
            sqlstate: "22P02",
        })?;
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut token_was_quoted = false;
    for ch in body.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quoted => escaped = true,
            '"' => {
                quoted = !quoted;
                token_was_quoted = true;
            }
            ',' if !quoted => {
                elements.push(record_array_literal_element(&current, token_was_quoted));
                current.clear();
                token_was_quoted = false;
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err(ExecError::ArrayInput {
            message: format!("malformed array literal: \"{text}\""),
            value: text.into(),
            detail: Some("Unexpected end of input.".into()),
            sqlstate: "22P02",
        });
    }
    elements.push(record_array_literal_element(&current, token_was_quoted));
    Ok(elements)
}

fn record_array_literal_element(text: &str, quoted: bool) -> Option<String> {
    if !quoted && text.trim().eq_ignore_ascii_case("NULL") {
        None
    } else {
        Some(text.trim().to_string())
    }
}

fn json_record_array_shape_detail(details: &str) -> String {
    if details == "multidimensional arrays must have matching extents" {
        "Multidimensional arrays must have sub-arrays with matching dimensions.".into()
    } else {
        details.into()
    }
}

fn json_record_array_nested_value(
    value: &SerdeJsonValue,
    element_type: SqlType,
    path: &JsonRecordPath,
    render_jsonb_style: bool,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(value, SerdeJsonValue::Array(_)) {
        return json_record_array_to_value(
            value,
            SqlType::array_of(element_type),
            path,
            render_jsonb_style,
            ctx,
        );
    }
    json_record_field_to_value(value, element_type, None, path, render_jsonb_style, ctx)
}

fn json_record_scalar_text(
    value: &SerdeJsonValue,
    render_jsonb_style: bool,
) -> Result<String, ExecError> {
    match value {
        SerdeJsonValue::String(text) => Ok(text.clone()),
        _ if render_jsonb_style => Ok(render_serde_json_value_text_with_jsonb_spacing(value)),
        _ => Ok(render_serde_json_value_text(value)),
    }
}

fn json_record_row_type_error(func_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("could not determine row type for result of {func_name}"),
        detail: None,
        hint: Some(
            "Provide a non-null record argument, or call the function in the FROM clause using a column definition list."
                .into(),
        ),
        sqlstate: "42804",
    }
}

fn record_descriptor_from_sql_type(
    ty: SqlType,
    ctx: &ExecutorContext,
) -> Result<Option<RecordDescriptor>, ExecError> {
    match ty.kind {
        SqlTypeKind::Composite if ty.typrelid != 0 => {
            let catalog = ctx
                .catalog
                .as_ref()
                .ok_or_else(|| ExecError::DetailedError {
                    message: "named composite record expansion requires catalog context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
            let relation = catalog.lookup_relation_by_oid(ty.typrelid).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("unknown composite relation oid {}", ty.typrelid),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
            Ok(Some(RecordDescriptor::named(
                ty.type_oid.max(RECORD_TYPE_OID),
                ty.typrelid,
                ty.typmod,
                relation
                    .desc
                    .columns
                    .into_iter()
                    .filter(|column| !column.dropped)
                    .map(|column| (column.name, column.sql_type))
                    .collect(),
            )))
        }
        SqlTypeKind::Record if ty.typmod > 0 => Ok(lookup_anonymous_record_descriptor(ty.typmod)),
        _ => Ok(None),
    }
}

fn record_columns_from_descriptor(descriptor: &RecordDescriptor) -> Vec<QueryColumn> {
    descriptor
        .fields
        .iter()
        .map(|field| QueryColumn {
            name: field.name.clone(),
            sql_type: field.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

fn record_descriptor_from_query_columns(output_columns: &[QueryColumn]) -> RecordDescriptor {
    RecordDescriptor::anonymous(
        output_columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
        -1,
    )
}

fn parse_record_literal_to_value(
    text: &str,
    descriptor: RecordDescriptor,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let fields = parse_record_literal_fields(text)?;
    if fields.len() != descriptor.fields.len() {
        return Err(ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!(
                "record literal has {} fields but type expects {}",
                fields.len(),
                descriptor.fields.len()
            )),
            hint: None,
            sqlstate: "22P02",
        });
    }
    let converted = descriptor
        .fields
        .iter()
        .zip(fields)
        .map(|(field, raw)| match raw {
            None => json_record_null_to_value(field.sql_type, &JsonRecordPath::default(), ctx),
            Some(raw) => json_record_cast_value(
                Value::Text(CompactString::from_owned(raw)),
                field.sql_type,
                &JsonRecordPath::default(),
                ctx,
            ),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Record(RecordValue::from_descriptor(
        descriptor, converted,
    )))
}

fn parse_record_literal_fields(text: &str) -> Result<Vec<Option<String>>, ExecError> {
    let body = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!("missing left parenthesis in \"{text}\"")),
            hint: None,
            sqlstate: "22P02",
        })?;
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for ch in body.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ',' if !quoted => {
                fields.push((!current.is_empty()).then(|| current.clone()));
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err(ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!("unterminated quoted string in \"{text}\"")),
            hint: None,
            sqlstate: "22P02",
        });
    }
    fields.push((!current.is_empty()).then_some(current));
    Ok(fields)
}

fn expected_json_array_error(path: &JsonRecordPath, hint: Option<String>) -> ExecError {
    let hint = hint.or_else(|| path.hint());
    ExecError::DetailedError {
        message: "expected JSON array".into(),
        detail: None,
        hint,
        sqlstate: "22023",
    }
}

fn populate_composite_type_error(value: &SerdeJsonValue) -> ExecError {
    let message = if matches!(value, SerdeJsonValue::Array(_)) {
        "cannot call populate_composite on an array"
    } else {
        "cannot call populate_composite on a scalar"
    };
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn json_record_error_with_hint(err: ExecError, hint: Option<String>) -> ExecError {
    match err {
        ExecError::DetailedError {
            message,
            detail,
            hint: None,
            sqlstate,
        } if !matches!(sqlstate, "23502" | "23514") => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        other => other,
    }
}

fn variadic_args(
    values: &[Value],
    func_variadic: bool,
    fixed_prefix: usize,
    op: &'static str,
) -> Result<Option<Vec<Value>>, ExecError> {
    fn flatten_variadic_items(value: &Value, out: &mut Vec<Value>) {
        match value {
            Value::Array(items) => {
                for item in items {
                    flatten_variadic_items(item, out);
                }
            }
            Value::PgArray(array) => {
                for item in array.to_nested_values() {
                    flatten_variadic_items(&item, out);
                }
            }
            other => out.push(other.clone()),
        }
    }

    if !func_variadic {
        return Ok(Some(values.to_vec()));
    }
    let Some(variadic_value) = values.get(fixed_prefix) else {
        return Ok(Some(values.to_vec()));
    };
    match variadic_value {
        Value::Null => Ok(None),
        Value::Array(items) => {
            let mut out = values[..fixed_prefix].to_vec();
            for item in items {
                flatten_variadic_items(item, &mut out);
            }
            Ok(Some(out))
        }
        Value::PgArray(array) => {
            let mut out = values[..fixed_prefix].to_vec();
            for item in array.to_nested_values() {
                flatten_variadic_items(&item, &mut out);
            }
            Ok(Some(out))
        }
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::PgArray(crate::include::nodes::datum::ArrayValue::empty()),
        }),
    }
}

fn coerce_sql_json_return_value(
    value: Value,
    result_type: Option<SqlType>,
) -> Result<Value, ExecError> {
    let Some(result_type) = result_type else {
        return Ok(value);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = match &value {
        Value::Json(text) => text.to_string(),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)?,
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap().to_string(),
        other => value_to_json_text(
            other,
            false,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            None,
        ),
    };
    match result_type.kind {
        SqlTypeKind::Json => Ok(Value::Json(CompactString::from_owned(text))),
        SqlTypeKind::Jsonb => Ok(Value::Jsonb(parse_jsonb_text(&text)?)),
        SqlTypeKind::Bytea => Ok(Value::Bytea(text.into_bytes())),
        SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char => {
            Ok(Value::Text(CompactString::from_owned(text)))
        }
        _ => Err(ExecError::DetailedError {
            message: format!(
                "cannot use type {} in RETURNING clause of SQL/JSON constructor",
                format!("{:?}", result_type.kind).to_ascii_lowercase()
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
    }
}

fn eval_sql_json_constructor(
    value: &Value,
    result_type: Option<SqlType>,
) -> Result<Value, ExecError> {
    let result = match value {
        Value::Null => Value::Null,
        Value::Json(text) | Value::Text(text) | Value::JsonPath(text) | Value::Xml(text) => {
            validate_json_text(text.as_str())?;
            Value::Json(text.clone())
        }
        Value::TextRef(_, _) => {
            let text = value.as_text().unwrap();
            validate_json_text(text)?;
            Value::Json(CompactString::new(text))
        }
        Value::Jsonb(bytes) => Value::Json(CompactString::from_owned(render_jsonb_bytes(bytes)?)),
        Value::Bytea(bytes) => {
            let text = String::from_utf8(bytes.clone()).map_err(|_| ExecError::DetailedError {
                message: "invalid byte sequence for encoding \"UTF8\"".into(),
                detail: None,
                hint: None,
                sqlstate: "22021",
            })?;
            validate_json_text(&text)?;
            Value::Json(CompactString::from_owned(text))
        }
        other => {
            return Err(ExecError::TypeMismatch {
                op: "JSON",
                left: other.clone(),
                right: Value::Json(CompactString::new("null")),
            });
        }
    };
    coerce_sql_json_return_value(result, result_type)
}

fn eval_sql_json_serialize(
    value: &Value,
    result_type: Option<SqlType>,
) -> Result<Value, ExecError> {
    let result = match value {
        Value::Null => Value::Null,
        Value::Json(text) | Value::Text(text) => {
            validate_json_text(text.as_str())?;
            Value::Text(text.clone())
        }
        Value::TextRef(_, _) => {
            let text = value.as_text().unwrap();
            validate_json_text(text)?;
            Value::Text(CompactString::new(text))
        }
        Value::Jsonb(bytes) => Value::Text(CompactString::from_owned(render_jsonb_bytes(bytes)?)),
        other => Value::Text(CompactString::from_owned(value_to_json_text(
            other,
            false,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            None,
        ))),
    };
    coerce_sql_json_return_value(
        result,
        Some(result_type.unwrap_or(SqlType::new(SqlTypeKind::Text))),
    )
}

fn eval_sql_json_object(
    values: &[Value],
    result_type: Option<SqlType>,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let absent_on_null = matches!(values.first(), Some(Value::Bool(true)));
    let unique_keys = matches!(values.get(1), Some(Value::Bool(true)));
    let mut seen_keys = std::collections::HashSet::new();
    let mut out = String::from("{");
    let mut emitted = 0usize;
    for pair in values.get(2..).unwrap_or(&[]).chunks(2) {
        let key = pair.first().unwrap_or(&Value::Null);
        if matches!(key, Value::Null) {
            return Err(ExecError::DetailedError {
                message: "null value not allowed for object key".into(),
                detail: None,
                hint: None,
                sqlstate: "22004",
            });
        }
        let key_text = json_object_key_text(key, "JSON_OBJECT")?;
        if unique_keys && !seen_keys.insert(key_text.clone()) {
            return Err(ExecError::DetailedError {
                message: format!("duplicate JSON object key value: \"{key_text}\""),
                detail: None,
                hint: None,
                sqlstate: "22030",
            });
        }
        let value = pair.get(1).unwrap_or(&Value::Null);
        if absent_on_null && matches!(value, Value::Null) {
            continue;
        }
        if emitted > 0 {
            out.push_str(", ");
        }
        emitted += 1;
        out.push_str(&serde_json::to_string(&key_text).unwrap());
        out.push_str(" : ");
        out.push_str(&value_to_json_text(value, false, datetime_config, catalog));
    }
    out.push('}');
    coerce_sql_json_return_value(Value::Json(CompactString::from_owned(out)), result_type)
}

fn eval_sql_json_array(
    values: &[Value],
    result_type: Option<SqlType>,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let null_on_null = matches!(values.first(), Some(Value::Bool(true)));
    let query_constructor = matches!(values.get(1), Some(Value::Bool(true)));
    let mut out = String::from("[");
    let mut emitted = 0usize;
    let items = if query_constructor {
        match values.get(2).unwrap_or(&Value::Null) {
            Value::Array(items) => items.clone(),
            Value::PgArray(array) => array.to_nested_values(),
            Value::Null => Vec::new(),
            other => vec![other.clone()],
        }
    } else {
        values.get(2..).unwrap_or(&[]).to_vec()
    };
    for value in &items {
        if !null_on_null && matches!(value, Value::Null) {
            continue;
        }
        if emitted > 0 {
            out.push_str(", ");
        }
        emitted += 1;
        out.push_str(&value_to_json_text(value, false, datetime_config, catalog));
    }
    out.push(']');
    coerce_sql_json_return_value(Value::Json(CompactString::from_owned(out)), result_type)
}

fn eval_sql_json_is_json(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Bool(false));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let predicate_type = values
        .get(1)
        .and_then(Value::as_text)
        .unwrap_or("value")
        .to_ascii_lowercase();
    let parsed = match value {
        Value::Json(text) | Value::Text(text) | Value::JsonPath(text) => {
            parse_json_text(text.as_str())
        }
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes).and_then(|text| parse_json_text(&text)),
        Value::TextRef(_, _) => parse_json_text(value.as_text().unwrap()),
        Value::Bytea(bytes) => String::from_utf8(bytes.clone())
            .map_err(|_| ExecError::DetailedError {
                message: "invalid byte sequence for encoding \"UTF8\"".into(),
                detail: None,
                hint: None,
                sqlstate: "22021",
            })
            .and_then(|text| parse_json_text(&text)),
        _ => return Ok(Value::Bool(false)),
    };
    let Ok(json) = parsed else {
        return Ok(Value::Bool(false));
    };
    let matches_type = match predicate_type.as_str() {
        "scalar" => !matches!(json, SerdeJsonValue::Array(_) | SerdeJsonValue::Object(_)),
        "array" => matches!(json, SerdeJsonValue::Array(_)),
        "object" => matches!(json, SerdeJsonValue::Object(_)),
        _ => true,
    };
    Ok(Value::Bool(matches_type))
}

fn render_json_builder_array(values: &[Value]) -> String {
    let mut out = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(&value_to_json_text(
            value,
            false,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            None,
        ));
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
            let items = json_object_one_arg_items(single)?;
            if items.len() % 2 != 0 {
                return Err(ExecError::InvalidStorageValue {
                    column: "json".into(),
                    details: "array must have even number of elements".into(),
                });
            }
            let pairs = items
                .chunks(2)
                .map(|chunk| {
                    Ok((
                        json_object_function_key(&chunk[0], "json")?,
                        chunk.get(1).cloned().unwrap_or(Value::Null),
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(render_json_text_pairs(&pairs)?)
        }
        [keys, vals] => {
            let keys = json_object_two_arg_items(keys)?;
            let vals = json_object_two_arg_items(vals)?;
            if keys.len() != vals.len() {
                return Err(ExecError::InvalidStorageValue {
                    column: "json".into(),
                    details: "mismatched array dimensions".into(),
                });
            }
            let pairs = keys
                .into_iter()
                .zip(vals)
                .map(|(k, v)| Ok((json_object_function_key(&k, "json")?, v)))
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(render_json_text_pairs(&pairs)?)
        }
        _ => Err(ExecError::InvalidStorageValue {
            column: "json".into(),
            details: "json_object expects one or two array arguments".into(),
        }),
    }
}

fn json_object_one_arg_items(value: &Value) -> Result<Vec<Value>, ExecError> {
    match parse_json_object_array_arg(value)? {
        Value::PgArray(array) => match array.ndim() {
            0 => Ok(Vec::new()),
            1 => Ok(array.elements.clone()),
            2 => {
                if array.dimensions[1].length != 2 {
                    return Err(ExecError::InvalidStorageValue {
                        column: "json".into(),
                        details: "array must have two columns".into(),
                    });
                }
                Ok(array.elements.clone())
            }
            _ => Err(ExecError::InvalidStorageValue {
                column: "json".into(),
                details: "wrong number of array subscripts".into(),
            }),
        },
        Value::Array(items) => json_object_one_arg_array_items(items),
        other => Err(ExecError::TypeMismatch {
            op: "json_object",
            left: other,
            right: Value::Null,
        }),
    }
}

fn json_object_one_arg_array_items(items: Vec<Value>) -> Result<Vec<Value>, ExecError> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    if !items.iter().all(|item| matches!(item, Value::Array(_))) {
        return Ok(items);
    }

    let mut out = Vec::with_capacity(items.len() * 2);
    for item in items {
        let Value::Array(parts) = item else {
            unreachable!();
        };
        if parts.iter().any(|part| matches!(part, Value::Array(_))) {
            return Err(ExecError::InvalidStorageValue {
                column: "json".into(),
                details: "wrong number of array subscripts".into(),
            });
        }
        if parts.len() != 2 {
            return Err(ExecError::InvalidStorageValue {
                column: "json".into(),
                details: "array must have two columns".into(),
            });
        }
        out.extend(parts);
    }
    Ok(out)
}

fn json_object_two_arg_items(value: &Value) -> Result<Vec<Value>, ExecError> {
    match parse_json_object_array_arg(value)? {
        Value::PgArray(array) if array.ndim() == 0 => Ok(Vec::new()),
        Value::PgArray(array) if array.ndim() == 1 => Ok(array.elements.clone()),
        Value::PgArray(_) => Err(ExecError::InvalidStorageValue {
            column: "json".into(),
            details: "wrong number of array subscripts".into(),
        }),
        Value::Array(items) if items.iter().any(|item| matches!(item, Value::Array(_))) => {
            Err(ExecError::InvalidStorageValue {
                column: "json".into(),
                details: "wrong number of array subscripts".into(),
            })
        }
        Value::Array(items) => Ok(items),
        other => Err(ExecError::TypeMismatch {
            op: "json_object",
            left: other,
            right: Value::Null,
        }),
    }
}

fn parse_json_object_array_arg(value: &Value) -> Result<Value, ExecError> {
    match value {
        Value::Text(text) => parse_text_array_literal_with_op(
            text.as_str(),
            SqlType::new(SqlTypeKind::Text),
            "json_object",
        ),
        Value::TextRef(_, _) => parse_text_array_literal_with_op(
            value.as_text().unwrap(),
            SqlType::new(SqlTypeKind::Text),
            "json_object",
        ),
        _ => Ok(value.clone()),
    }
}

fn render_jsonb_object_function(values: &[Value]) -> Result<JsonbValue, ExecError> {
    match values {
        [single] => {
            let items = jsonb_object_one_arg_items(single)?;
            if items.iter().all(|item| matches!(item, Value::Array(_))) {
                let mut pairs = Vec::with_capacity(items.len());
                for item in items {
                    let Value::Array(parts) = item else {
                        unreachable!();
                    };
                    if parts.len() != 2 {
                        return Err(ExecError::InvalidStorageValue {
                            column: "jsonb".into(),
                            details: "array must have two columns".into(),
                        });
                    }
                    pairs.push((
                        json_object_function_key(&parts[0], "jsonb")?,
                        parts[1].clone(),
                    ));
                }
                return jsonb_object_from_pairs(&pairs);
            }
            if items.len() % 2 != 0 {
                return Err(ExecError::InvalidStorageValue {
                    column: "jsonb".into(),
                    details: "argument list must have even number of elements".into(),
                });
            }
            let pairs = items
                .chunks(2)
                .map(|chunk| {
                    Ok((
                        json_object_function_key(&chunk[0], "jsonb")?,
                        chunk[1].clone(),
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            jsonb_object_from_pairs(&pairs)
        }
        [keys, vals] => {
            let keys = jsonb_object_two_arg_items(keys)?;
            let vals = jsonb_object_two_arg_items(vals)?;
            if keys.len() != vals.len() {
                return Err(ExecError::InvalidStorageValue {
                    column: "jsonb".into(),
                    details: "mismatched array dimensions".into(),
                });
            }
            let pairs = keys
                .into_iter()
                .zip(vals)
                .map(|(k, v)| Ok((json_object_function_key(&k, "jsonb")?, v)))
                .collect::<Result<Vec<_>, ExecError>>()?;
            jsonb_object_from_pairs(&pairs)
        }
        _ => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "jsonb_object expects one or two array arguments".into(),
        }),
    }
}

fn jsonb_object_one_arg_items(value: &Value) -> Result<Vec<Value>, ExecError> {
    match value {
        Value::PgArray(array) => match array.ndim() {
            0 => Ok(Vec::new()),
            1 => {
                if array.elements.len() % 2 != 0 {
                    return Err(ExecError::InvalidStorageValue {
                        column: "jsonb".into(),
                        details: "array must have even number of elements".into(),
                    });
                }
                Ok(array.elements.clone())
            }
            2 => {
                if array.dimensions[1].length != 2 {
                    return Err(ExecError::InvalidStorageValue {
                        column: "jsonb".into(),
                        details: "array must have two columns".into(),
                    });
                }
                Ok(array.elements.clone())
            }
            _ => Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "wrong number of array subscripts".into(),
            }),
        },
        other => array_values_for_json_object(other, "jsonb_object"),
    }
}

fn jsonb_object_two_arg_items(value: &Value) -> Result<Vec<Value>, ExecError> {
    match value {
        Value::PgArray(array) if array.ndim() > 1 => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "wrong number of array subscripts".into(),
        }),
        other => array_values_for_json_object(other, "jsonb_object"),
    }
}

fn json_builder_pairs(
    values: &[Value],
    op: &'static str,
) -> Result<Vec<(String, Value)>, ExecError> {
    if values.len() % 2 != 0 {
        return Err(ExecError::DetailedError {
            message: "argument list must have even number of elements".into(),
            detail: None,
            hint: Some(format!(
                "The arguments of {op}() must consist of alternating keys and values."
            )),
            sqlstate: "22023",
        });
    }
    values
        .chunks(2)
        .enumerate()
        .map(|(index, chunk)| {
            Ok((
                json_builder_key_for_object_constructor(&chunk[0], op, index * 2 + 1)?,
                chunk.get(1).cloned().unwrap_or(Value::Null),
            ))
        })
        .collect()
}

fn render_json_pairs(pairs: &[(String, Value)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(&serde_json::to_string(key).unwrap());
        out.push_str(" : ");
        out.push_str(&value_to_json_text(
            value,
            false,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            None,
        ));
    }
    out.push('}');
    out
}

fn render_json_text_pairs(pairs: &[(String, Value)]) -> Result<String, ExecError> {
    let mut out = String::from("{");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(&serde_json::to_string(key).unwrap());
        out.push_str(" : ");
        match json_object_text_value(value, "json_object")? {
            Some(text) => out.push_str(&serde_json::to_string(&text).unwrap()),
            None => out.push_str("null"),
        }
    }
    out.push('}');
    Ok(out)
}

fn array_values_for_json_object(value: &Value, op: &'static str) -> Result<Vec<Value>, ExecError> {
    match value {
        Value::Array(items) => Ok(items.clone()),
        Value::PgArray(array) => Ok(array.to_nested_values()),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn json_object_key_text(value: &Value, op: &'static str) -> Result<String, ExecError> {
    match value {
        Value::Null => Err(ExecError::DetailedError {
            message: "null value not allowed for object key".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        }),
        Value::JsonPath(_)
        | Value::Xml(_)
        | Value::Json(_)
        | Value::Jsonb(_)
        | Value::EnumOid(_)
        | Value::Array(_)
        | Value::PgArray(_)
        | Value::Record(_) => Err(ExecError::DetailedError {
            message: "key value must be scalar, not array, composite, or json".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        _ => json_object_text_value(value, op)?.ok_or_else(|| ExecError::DetailedError {
            message: "null value not allowed for object key".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        }),
    }
}

fn json_object_text_value(value: &Value, op: &'static str) -> Result<Option<String>, ExecError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(_) | Value::TextRef(_, _) => Ok(Some(value.as_text().unwrap().to_string())),
        Value::Bit(v) => Ok(Some(render_bit_text(v))),
        Value::Bytea(v) => Ok(Some(format_bytea_text(v, ByteaOutputFormat::Hex))),
        Value::Inet(v) => Ok(Some(v.render_inet())),
        Value::Cidr(v) => Ok(Some(v.render_cidr())),
        Value::MacAddr(v) => Ok(Some(render_macaddr_text(v))),
        Value::MacAddr8(v) => Ok(Some(render_macaddr8_text(v))),
        Value::InternalChar(v) => Ok(Some(crate::backend::executor::render_internal_char_text(
            *v,
        ))),
        Value::EnumOid(v) => Ok(Some(v.to_string())),
        Value::Int16(v) => Ok(Some(v.to_string())),
        Value::Int32(v) => Ok(Some(v.to_string())),
        Value::Int64(v) => Ok(Some(v.to_string())),
        Value::Xid8(v) => Ok(Some(v.to_string())),
        Value::PgLsn(v) => Ok(Some(crate::backend::executor::render_pg_lsn_text(*v))),
        Value::Money(v) => Ok(Some(crate::backend::executor::money_format_text(*v))),
        Value::Float64(v) => Ok(Some(float_json_scalar_text(*v))),
        Value::Numeric(v) => Ok(Some(v.render())),
        Value::Interval(v) => Ok(Some(render_interval_text(*v))),
        Value::Uuid(v) => Ok(Some(crate::backend::executor::value_io::render_uuid_text(
            v,
        ))),
        Value::Bool(v) => Ok(Some(if *v { "true".into() } else { "false".into() })),
        Value::JsonPath(v) => Ok(Some(v.to_string())),
        Value::Xml(v) => Ok(Some(v.to_string())),
        Value::Json(v) => Ok(Some(v.to_string())),
        Value::Jsonb(v) => render_jsonb_bytes(v).map(Some),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => Ok(Some(
            render_datetime_value_text(value).expect("datetime values render"),
        )),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => Ok(Some(
            crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default(),
        )),
        Value::Range(_) => Ok(Some(render_range_text(value).unwrap_or_default())),
        Value::Multirange(_) => Ok(Some(
            crate::backend::executor::render_multirange_text(value).unwrap_or_default(),
        )),
        Value::TsVector(v) => Ok(Some(crate::backend::executor::render_tsvector_text(v))),
        Value::TsQuery(v) => Ok(Some(crate::backend::executor::render_tsquery_text(v))),
        Value::Array(_) | Value::PgArray(_) => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
        Value::Record(_) => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        }),
    }
}

fn json_builder_key_for_object_constructor(
    value: &Value,
    _op: &'static str,
    arg_index: usize,
) -> Result<String, ExecError> {
    match value {
        Value::Null => Err(ExecError::DetailedError {
            message: format!("argument {arg_index}: key must not be null"),
            detail: None,
            hint: None,
            sqlstate: "22004",
        }),
        Value::Array(_)
        | Value::PgArray(_)
        | Value::Record(_)
        | Value::Json(_)
        | Value::Jsonb(_) => Err(ExecError::DetailedError {
            message: "key value must be scalar, not array, composite, or json".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        _ => jsonb_builder_key(value),
    }
}

fn json_object_function_key(value: &Value, json_kind: &'static str) -> Result<String, ExecError> {
    match value {
        Value::Null => Err(ExecError::DetailedError {
            message: "null value not allowed for object key".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        }),
        _ => match json_kind {
            "jsonb" => jsonb_builder_key(value),
            _ => json_object_key_text(value, "json_object"),
        },
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
                .map(|value| json_value_to_value(value, as_text, false))
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
            .map(|value| json_value_to_value(value, as_text, false))
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
        jsonpath_exists_result(result, true)
    }
}

fn eval_jsonpath_function(
    values: &[Value],
    kind: JsonPathFunctionKind,
) -> Result<Value, ExecError> {
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
        JsonPathFunctionKind::Exists => jsonpath_exists_result(result, silent),
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

fn eval_sql_json_exists_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(items) = eval_sql_json_query_path(values)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Bool(!items.is_empty()))
}

fn eval_sql_json_value_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(items) = eval_sql_json_query_path(values)? else {
        return Ok(Value::Null);
    };
    let [item] = items.as_slice() else {
        return Ok(Value::Null);
    };
    Ok(sql_json_value_default_text(item))
}

fn eval_sql_json_query_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(items) = eval_sql_json_query_path(values)? else {
        return Ok(Value::Null);
    };
    let [item] = items.as_slice() else {
        return Ok(Value::Null);
    };
    Ok(jsonb_to_value(item))
}

fn eval_sql_json_query_path(values: &[Value]) -> Result<Option<Vec<JsonbValue>>, ExecError> {
    let target = values.first().unwrap_or(&Value::Null);
    let path = values.get(1).unwrap_or(&Value::Null);
    if matches!(target, Value::Null) || matches!(path, Value::Null) {
        return Ok(None);
    }

    let target = parse_jsonpath_target_value(target)?;
    let parsed = parse_jsonpath(parse_jsonpath_value_text(path)?.as_str())?;
    let eval_ctx = JsonPathEvaluationContext {
        root: &target,
        vars: None,
    };
    // SQL/JSON query functions default to NULL/FALSE ON ERROR. Keep jsonpath
    // parse and input conversion errors visible, but suppress path evaluation
    // errors such as strict-mode structural mismatches.
    Ok(Some(
        evaluate_jsonpath(&parsed, &eval_ctx).unwrap_or_default(),
    ))
}

pub(crate) fn eval_sql_json_query_function_expr(
    func: &SqlJsonQueryFunction,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let context = eval_expr(&func.context, slot, ctx)?;
    let path = eval_expr(&func.path, slot, ctx)?;
    if matches!(context, Value::Null) || matches!(path, Value::Null) {
        return Ok(Value::Null);
    }
    let document = parse_jsonpath_target_value(&context)?;
    let path = parse_jsonpath_value_text(&path)?;
    let vars = eval_sql_json_passing_vars(&func.passing, slot, ctx)?;
    match func.kind {
        SqlJsonQueryFunctionKind::Exists => {
            eval_sql_json_exists_expr(&document, path.as_str(), vars.as_ref(), func, slot, ctx)
        }
        SqlJsonQueryFunctionKind::Value => {
            eval_sql_json_value_expr(&document, path.as_str(), vars.as_ref(), func, slot, ctx)
        }
        SqlJsonQueryFunctionKind::Query => {
            eval_sql_json_query_expr(&document, path.as_str(), vars.as_ref(), func, slot, ctx)
        }
    }
}

fn eval_sql_json_passing_vars(
    passing: &[SqlJsonTablePassingArg],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Option<JsonbValue>, ExecError> {
    if passing.is_empty() {
        return Ok(None);
    }
    let mut pairs = Vec::with_capacity(passing.len());
    for arg in passing {
        let value = eval_expr(&arg.expr, slot, ctx)?;
        pairs.push((
            arg.name.clone(),
            sql_json_passing_value_from_value(&value, &ctx.datetime_config)?,
        ));
    }
    Ok(Some(JsonbValue::Object(pairs)))
}

fn sql_json_passing_value_from_value(
    value: &Value,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
) -> Result<JsonbValue, ExecError> {
    Ok(match value {
        Value::Date(v) => JsonbValue::Date(*v),
        Value::Time(v) => JsonbValue::Time(*v),
        Value::TimeTz(v) => JsonbValue::TimeTz(*v),
        Value::Timestamp(v) => JsonbValue::Timestamp(*v),
        Value::TimestampTz(v) => JsonbValue::TimestampTz(*v),
        Value::Record(record) => JsonbValue::Object(
            record
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.name.clone(),
                        sql_json_passing_value_from_value(value, datetime_config)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
        ),
        Value::Array(items) => JsonbValue::Array(
            items
                .iter()
                .map(|value| sql_json_passing_value_from_value(value, datetime_config))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::PgArray(array) => JsonbValue::Array(
            array
                .to_nested_values()
                .iter()
                .map(|value| sql_json_passing_value_from_value(value, datetime_config))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        other => jsonb_from_value(other, datetime_config)?,
    })
}

fn eval_sql_json_exists_expr(
    document: &JsonbValue,
    path: &str,
    vars: Option<&JsonbValue>,
    func: &SqlJsonQueryFunction,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let exists = match eval_sql_json_path(document, path, vars, true) {
        Ok(values) => !values.is_empty(),
        Err(err) if is_missing_jsonpath_variable_error(&err) => return Err(err),
        Err(err) => {
            return if matches!(func.on_error, SqlJsonTableBehavior::Error) {
                Err(err)
            } else {
                eval_sql_json_exists_behavior(&func.on_error, func.result_type, slot, ctx)
            };
        }
    };
    coerce_sql_json_exists_bool(exists, func.result_type, ctx)
}

fn eval_sql_json_value_expr(
    document: &JsonbValue,
    path: &str,
    vars: Option<&JsonbValue>,
    func: &SqlJsonQueryFunction,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = match eval_sql_json_path(document, path, vars, true) {
        Ok(values) => values,
        Err(err) if is_missing_jsonpath_variable_error(&err) => return Err(err),
        Err(err) => {
            return eval_sql_json_value_error_behavior(
                &func.on_error,
                func.result_type,
                err,
                slot,
                ctx,
            );
        }
    };
    if values.is_empty() {
        return eval_sql_json_value_empty_behavior(&func.on_empty, func.result_type, slot, ctx);
    }
    if values.len() != 1 || matches!(values[0], JsonbValue::Array(_) | JsonbValue::Object(_)) {
        return eval_sql_json_value_error_behavior(
            &func.on_error,
            func.result_type,
            sql_json_value_single_scalar_error(),
            slot,
            ctx,
        );
    }
    let value = &values[0];
    if matches!(value, JsonbValue::Null) {
        return coerce_sql_json_null_value(func.result_type, ctx).or_else(|err| {
            eval_sql_json_value_error_behavior(&func.on_error, func.result_type, err, slot, ctx)
        });
    }
    let text = if !func.result_type.is_array
        && matches!(
            func.result_type.kind,
            SqlTypeKind::Json | SqlTypeKind::Jsonb
        ) {
        value.render()
    } else {
        jsonb_scalar_sql_text_for_type(value, func.result_type, &ctx.datetime_config)
    };
    cast_sql_json_text_value(&text, func.result_type, ctx).or_else(|err| {
        eval_sql_json_value_error_behavior(&func.on_error, func.result_type, err, slot, ctx)
    })
}

fn is_missing_jsonpath_variable_error(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { source, .. } => is_missing_jsonpath_variable_error(source),
        ExecError::DetailedError { message, .. } => {
            message.starts_with("could not find jsonpath variable ")
        }
        ExecError::InvalidStorageValue { column, details } => {
            column == "jsonpath" && details.starts_with("could not find jsonpath variable ")
        }
        _ => false,
    }
}

fn eval_sql_json_value_empty_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(behavior, SqlJsonTableBehavior::Error) {
        Err(sql_json_no_item_error())
    } else {
        eval_sql_json_behavior(behavior, target_type, "EMPTY", slot, ctx)
    }
}

fn eval_sql_json_value_error_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    err: ExecError,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(behavior, SqlJsonTableBehavior::Error) {
        Err(err)
    } else {
        eval_sql_json_behavior(behavior, target_type, "ERROR", slot, ctx)
    }
}

fn sql_json_value_single_scalar_error() -> ExecError {
    ExecError::DetailedError {
        message: "JSON path expression in JSON_VALUE must return single scalar item".into(),
        detail: None,
        hint: None,
        sqlstate: "22034",
    }
}

fn eval_sql_json_query_expr(
    document: &JsonbValue,
    path: &str,
    vars: Option<&JsonbValue>,
    func: &SqlJsonQueryFunction,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = match eval_sql_json_path(document, path, vars, true) {
        Ok(values) => values,
        Err(err) if is_missing_jsonpath_variable_error(&err) => return Err(err),
        Err(err) => {
            return eval_sql_json_query_error_behavior(
                &func.on_error,
                func.result_type,
                err,
                "ERROR",
                func.quotes,
                slot,
                ctx,
            );
        }
    };
    if values.is_empty() {
        return eval_sql_json_query_behavior(
            &func.on_empty,
            func.result_type,
            "EMPTY",
            func.quotes,
            slot,
            ctx,
        );
    }
    let value = match func.wrapper {
        SqlJsonTableWrapper::Unconditional => JsonbValue::Array(values),
        SqlJsonTableWrapper::Conditional if values.len() == 1 => values[0].clone(),
        SqlJsonTableWrapper::Conditional => JsonbValue::Array(values),
        SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without if values.len() == 1 => {
            values[0].clone()
        }
        SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without => {
            return eval_sql_json_query_error_behavior(
                &func.on_error,
                func.result_type,
                sql_json_query_single_item_error(),
                "ERROR",
                func.quotes,
                slot,
                ctx,
            );
        }
    };
    cast_sql_json_query_formatted_value(&value, func.result_type, func.quotes, ctx).or_else(|err| {
        eval_sql_json_query_error_behavior(
            &func.on_error,
            func.result_type,
            err,
            "ERROR",
            func.quotes,
            slot,
            ctx,
        )
    })
}

fn eval_sql_json_query_error_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    err: ExecError,
    target: &'static str,
    quotes: SqlJsonTableQuotes,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(behavior, SqlJsonTableBehavior::Error) {
        Err(err)
    } else {
        eval_sql_json_query_behavior(behavior, target_type, target, quotes, slot, ctx)
    }
}

fn eval_sql_json_query_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    target: &'static str,
    quotes: SqlJsonTableQuotes,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match behavior {
        SqlJsonTableBehavior::Empty | SqlJsonTableBehavior::EmptyArray => {
            let behavior_name = if matches!(behavior, SqlJsonTableBehavior::Empty) {
                "EMPTY"
            } else {
                "EMPTY ARRAY"
            };
            cast_sql_json_query_formatted_value(
                &JsonbValue::Array(Vec::new()),
                target_type,
                SqlJsonTableQuotes::Unspecified,
                ctx,
            )
            .map_err(|err| sql_json_behavior_coercion_error(target, behavior_name, err))
        }
        SqlJsonTableBehavior::EmptyObject => cast_sql_json_query_formatted_value(
            &JsonbValue::Object(Vec::new()),
            target_type,
            SqlJsonTableQuotes::Unspecified,
            ctx,
        )
        .map_err(|err| sql_json_behavior_coercion_error(target, "EMPTY OBJECT", err)),
        SqlJsonTableBehavior::Default(expr) => {
            eval_sql_json_query_default_behavior(expr, target_type, target, quotes, slot, ctx)
        }
        SqlJsonTableBehavior::Error if target == "EMPTY" => Err(sql_json_no_item_error()),
        _ => eval_sql_json_behavior(behavior, target_type, target, slot, ctx),
    }
}

fn eval_sql_json_query_default_behavior(
    expr: &Expr,
    target_type: SqlType,
    target: &'static str,
    quotes: SqlJsonTableQuotes,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let source_type = expr_sql_type_hint(expr);
    let result = eval_expr(expr, slot, ctx).and_then(|value| {
        if !source_type.is_some_and(|ty| {
            !ty.is_array && matches!(ty.kind, SqlTypeKind::Json | SqlTypeKind::Jsonb)
        }) {
            return cast_sql_json_default_value(value, source_type, target_type, ctx);
        }
        let parsed = ParsedJsonValue::from_value(&value)?;
        let jsonb = match parsed {
            ParsedJsonValue::Json(value) => JsonbValue::from_serde(value)?,
            ParsedJsonValue::Jsonb(value) => value,
        };
        cast_sql_json_query_formatted_value(&jsonb, target_type, quotes, ctx)
    });
    match result {
        Ok(value) => Ok(value),
        Err(err) if sql_json_default_error_is_direct(&err) => Err(err),
        Err(err) => Err(sql_json_behavior_coercion_error(target, "DEFAULT", err)),
    }
}

fn sql_json_no_item_error() -> ExecError {
    ExecError::DetailedError {
        message: "no SQL/JSON item found for specified path".into(),
        detail: None,
        hint: None,
        sqlstate: "22034",
    }
}

fn sql_json_query_single_item_error() -> ExecError {
    ExecError::DetailedError {
        message: "JSON path expression in JSON_QUERY must return single item when no wrapper is requested".into(),
        detail: None,
        hint: Some("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.".into()),
        sqlstate: "22034",
    }
}

fn cast_sql_json_query_formatted_value(
    value: &JsonbValue,
    target_type: SqlType,
    quotes: SqlJsonTableQuotes,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(quotes, SqlJsonTableQuotes::Omit)
        && let JsonbValue::String(text) = value
    {
        return cast_sql_json_text_value(text, target_type, ctx);
    }
    if let Some(value) = cast_sql_json_query_structured_value(value, target_type, quotes, ctx) {
        return value;
    }
    match target_type.kind {
        SqlTypeKind::Json if !target_type.is_array => {
            Ok(Value::Json(CompactString::from_owned(value.render())))
        }
        SqlTypeKind::Jsonb if !target_type.is_array => Ok(Value::Jsonb(encode_jsonb(value))),
        _ => {
            let text = value.render();
            cast_sql_json_text_value(&text, target_type, ctx)
        }
    }
}

fn cast_sql_json_query_structured_value(
    value: &JsonbValue,
    target_type: SqlType,
    quotes: SqlJsonTableQuotes,
    ctx: &ExecutorContext,
) -> Option<Result<Value, ExecError>> {
    if target_type.is_array {
        if !matches!(value, JsonbValue::Array(_)) {
            return Some(Err(expected_json_array_error(
                &JsonRecordPath::default(),
                None,
            )));
        }
        let json = value.to_serde();
        return Some(json_record_field_to_value(
            &json,
            target_type,
            None,
            &JsonRecordPath::default(),
            true,
            ctx,
        ));
    }

    if matches!(
        target_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) && record_descriptor_from_sql_type(target_type, ctx)
        .ok()
        .flatten()
        .is_some()
    {
        match value {
            JsonbValue::Object(_) => {
                let json = value.to_serde();
                Some(json_record_field_to_value(
                    &json,
                    target_type,
                    None,
                    &JsonRecordPath::default(),
                    true,
                    ctx,
                ))
            }
            JsonbValue::String(_) if !matches!(quotes, SqlJsonTableQuotes::Omit) => {
                Some(Err(ExecError::DetailedError {
                    message: "cannot call populate_composite on a scalar".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                }))
            }
            _ => None,
        }
    } else {
        None
    }
}

fn sql_json_value_default_text(value: &JsonbValue) -> Value {
    match value {
        JsonbValue::Array(_) | JsonbValue::Object(_) | JsonbValue::Null => Value::Null,
        JsonbValue::Bool(true) => Value::Text("t".into()),
        JsonbValue::Bool(false) => Value::Text("f".into()),
        other => jsonb_to_text_value(other),
    }
}

fn jsonpath_exists_result(
    result: Result<Vec<JsonbValue>, ExecError>,
    silent: bool,
) -> Result<Value, ExecError> {
    match result {
        Ok(items) => Ok(Value::Bool(!items.is_empty())),
        Err(_) if silent => Ok(Value::Bool(false)),
        Err(err) => Err(err),
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

fn parse_optional_bool_flag(
    value: Option<&Value>,
    default: bool,
    op: &'static str,
) -> Result<bool, ExecError> {
    match value {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(other) => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Bool(default),
        }),
    }
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

fn parse_jsonb_path_arg(value: &Value, op: &'static str) -> Result<Vec<Option<String>>, ExecError> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => Ok(None),
                Value::Text(_) | Value::TextRef(_, _) => {
                    Ok(Some(item.as_text().unwrap().to_string()))
                }
                other => Err(ExecError::TypeMismatch {
                    op,
                    left: other.clone(),
                    right: Value::Null,
                }),
            })
            .collect(),
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|item| match item {
                Value::Null => Ok(None),
                Value::Text(_) | Value::TextRef(_, _) => {
                    Ok(Some(item.as_text().unwrap().to_string()))
                }
                other => Err(ExecError::TypeMismatch {
                    op,
                    left: other.clone(),
                    right: Value::Null,
                }),
            })
            .collect(),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn parse_text_array_arg(value: &Value, op: &'static str) -> Result<Vec<String>, ExecError> {
    let items = match value {
        Value::Array(items) => items,
        Value::PgArray(array) => &array.elements,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    items
        .iter()
        .map(|item| match item {
            Value::Text(_) | Value::TextRef(_, _) => Ok(item.as_text().unwrap().to_string()),
            other => Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Text(String::new().into()),
            }),
        })
        .collect()
}

fn parse_jsonb_set_lax_treatment(value: Option<&Value>) -> Result<String, ExecError> {
    let treatment = match value {
        None => Ok("use_json_null".into()),
        Some(Value::Null) => {
            return Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"".into(),
            });
        }
        Some(Value::Text(text)) => Ok(text.to_ascii_lowercase()),
        Some(Value::TextRef(_, _)) => Ok(value.unwrap().as_text().unwrap().to_ascii_lowercase()),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "jsonb_set_lax",
            left: other.clone(),
            right: Value::Text("use_json_null".into()),
        }),
    }?;

    match treatment.as_str() {
        "raise_exception" | "use_json_null" | "delete_key" | "return_target" => Ok(treatment),
        _ => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"".into(),
        }),
    }
}

fn strip_json_nulls(value: &SerdeJsonValue, strip_in_arrays: bool) -> SerdeJsonValue {
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

fn strip_jsonb_nulls(value: &JsonbValue, strip_in_arrays: bool) -> JsonbValue {
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

fn apply_jsonb_delete(target: &JsonbValue, key: &Value) -> Result<JsonbValue, ExecError> {
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
                | JsonbValue::TimestampTz(_) => {
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
            | JsonbValue::TimestampTz(_) => Err(ExecError::InvalidStorageValue {
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

fn delete_jsonb_path(
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
                let Some(index) = parse_jsonb_path_array_index(step, items.len(), path_index + 1)?
                else {
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
            | JsonbValue::TimestampTz(_) => {
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
        | JsonbValue::TimestampTz(_) => {
            return Err(ExecError::InvalidStorageValue {
                column: "jsonb".into(),
                details: "cannot delete path in scalar".into(),
            });
        }
    })
}

fn set_jsonb_path(
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

#[derive(Debug, Clone)]
enum JsonbAssignmentStep {
    Key(String),
    Index(i32),
}

pub(crate) fn apply_jsonb_subscript_assignment(
    target: &Value,
    subscripts: &[Value],
    new_value: &Value,
) -> Result<Value, ExecError> {
    let target = parse_jsonb_target(target, "jsonb subscript assignment")?;
    let replacement = parse_jsonb_target(new_value, "jsonb subscript assignment")?;
    let updated = assign_jsonb_subscripts(&target, subscripts, replacement)?;
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
        (JsonbValue::Null, JsonbAssignmentStep::Key(key)) => {
            let mut out = Vec::new();
            let value = if last {
                replacement
            } else {
                let seed = seed_container_for_assignment(&subscripts[position + 1]);
                assign_jsonb_subscripts_inner(&seed, subscripts, position + 1, replacement)?
            };
            out.push((key, value));
            Ok(JsonbValue::Object(out))
        }
        (JsonbValue::Null, JsonbAssignmentStep::Index(_)) => {
            let seeded = JsonbValue::Array(Vec::new());
            assign_jsonb_subscripts_inner(&seeded, subscripts, position, replacement)
        }
        (_, _) => Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: "cannot replace existing key".into(),
        }),
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
        other => Some(render_serde_json_value_text(other)),
    }
}

fn json_value_to_value(value: &SerdeJsonValue, as_text: bool, render_jsonb_style: bool) -> Value {
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

fn render_serde_json_value_text_with_jsonb_spacing(value: &SerdeJsonValue) -> String {
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

fn render_serde_json_value_text(value: &SerdeJsonValue) -> String {
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

fn float_json_scalar_text(value: f64) -> String {
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

fn render_float_json_text(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        serde_json::to_string(&float_json_scalar_text(value)).unwrap()
    }
}

fn float_json_serde_value(value: f64) -> SerdeJsonValue {
    serde_json::Number::from_f64(value)
        .map(SerdeJsonValue::Number)
        .unwrap_or_else(|| SerdeJsonValue::String(float_json_scalar_text(value)))
}

fn value_to_json_serde_with_config(
    value: &Value,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
) -> SerdeJsonValue {
    match value {
        Value::Null => SerdeJsonValue::Null,
        Value::Int16(v) => SerdeJsonValue::from(*v),
        Value::Int32(v) => SerdeJsonValue::from(*v),
        Value::Int64(v) => SerdeJsonValue::from(*v),
        Value::Xid8(v) => SerdeJsonValue::from(*v),
        Value::PgLsn(v) => SerdeJsonValue::String(crate::backend::executor::render_pg_lsn_text(*v)),
        Value::Money(v) => SerdeJsonValue::String(crate::backend::executor::money_format_text(*v)),
        Value::Float64(v) => float_json_serde_value(*v),
        Value::Numeric(v) => parse_json_text(&v.render()).unwrap_or(SerdeJsonValue::Null),
        Value::Interval(v) => SerdeJsonValue::String(render_interval_text(*v)),
        Value::Uuid(v) => {
            SerdeJsonValue::String(crate::backend::executor::value_io::render_uuid_text(v))
        }
        Value::Bool(v) => SerdeJsonValue::Bool(*v),
        Value::Bit(v) => SerdeJsonValue::String(render_bit_text(v)),
        Value::JsonPath(text) => SerdeJsonValue::String(text.to_string()),
        Value::Xml(text) => SerdeJsonValue::String(text.to_string()),
        Value::Json(text) => parse_json_text(text.as_str()).unwrap_or(SerdeJsonValue::Null),
        Value::Jsonb(bytes) => decode_jsonb(bytes)
            .map(|value| value.to_serde())
            .unwrap_or(SerdeJsonValue::Null),
        Value::Text(_) | Value::TextRef(_, _) => {
            SerdeJsonValue::String(value.as_text().unwrap().to_string())
        }
        Value::Bytea(v) => SerdeJsonValue::String(format_bytea_text(v, ByteaOutputFormat::Hex)),
        Value::Inet(v) => SerdeJsonValue::String(v.render_inet()),
        Value::Cidr(v) => SerdeJsonValue::String(v.render_cidr()),
        Value::MacAddr(v) => SerdeJsonValue::String(render_macaddr_text(v)),
        Value::MacAddr8(v) => SerdeJsonValue::String(render_macaddr8_text(v)),
        Value::InternalChar(v) => {
            SerdeJsonValue::String(crate::backend::executor::render_internal_char_text(*v))
        }
        Value::EnumOid(v) => SerdeJsonValue::String(v.to_string()),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => SerdeJsonValue::String(
            render_json_datetime_value_text_with_config(value, datetime_config)
                .expect("datetime values render"),
        ),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => SerdeJsonValue::String(
            crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default(),
        ),
        Value::Range(_) => SerdeJsonValue::String(render_range_text(value).unwrap_or_default()),
        Value::Multirange(_) => SerdeJsonValue::String(
            crate::backend::executor::render_multirange_text(value).unwrap_or_default(),
        ),
        Value::TsVector(v) => {
            SerdeJsonValue::String(crate::backend::executor::render_tsvector_text(v))
        }
        Value::TsQuery(v) => {
            SerdeJsonValue::String(crate::backend::executor::render_tsquery_text(v))
        }
        Value::Array(items) => SerdeJsonValue::Array(
            items
                .iter()
                .map(|value| value_to_json_serde_with_config(value, datetime_config))
                .collect(),
        ),
        Value::Record(record) => SerdeJsonValue::Object(
            record
                .iter()
                .map(|(field, value)| {
                    (
                        field.name.clone(),
                        value_to_json_serde_with_config(value, datetime_config),
                    )
                })
                .collect(),
        ),
        Value::PgArray(array) => SerdeJsonValue::Array(
            array
                .to_nested_values()
                .iter()
                .map(|value| value_to_json_serde_with_config(value, datetime_config))
                .collect(),
        ),
    }
}

fn value_to_json_serde(value: &Value) -> SerdeJsonValue {
    value_to_json_serde_with_config(
        value,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )
}

fn value_to_json_text(
    value: &Value,
    pretty: bool,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    if pretty {
        render_json_value_text_pretty(value, datetime_config, catalog)
    } else {
        render_json_value_text_with_config(value, datetime_config, catalog)
    }
}

fn render_json_value_text_pretty(
    value: &Value,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    match value {
        Value::Array(items) => render_json_array_values(items, true, datetime_config, catalog),
        Value::PgArray(array) => {
            render_json_array_values(&array.to_nested_values(), true, datetime_config, catalog)
        }
        Value::Record(record) => render_json_record_value(record, true, datetime_config, catalog),
        _ => render_json_value_text_with_config(value, datetime_config, catalog),
    }
}

fn render_json_value_text_with_config(
    value: &Value,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => {
            serde_json::to_string(&crate::backend::executor::render_pg_lsn_text(*v)).unwrap()
        }
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => render_float_json_text(*v),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => {
            serde_json::to_string(&render_interval_text_with_config(*v, datetime_config)).unwrap()
        }
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
        Value::Text(_) | Value::TextRef(_, _) | Value::Xml(_) => {
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
        Value::EnumOid(v) => serde_json::to_string(&v.to_string()).unwrap(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => serde_json::to_string(
            &render_json_datetime_value_text_with_config(value, datetime_config)
                .expect("datetime values render"),
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
        Value::Array(items) => render_json_array_values(items, false, datetime_config, catalog),
        Value::Record(record) => render_json_record_value(record, false, datetime_config, catalog),
        Value::PgArray(array) => {
            render_json_array_values(&array.to_nested_values(), false, datetime_config, catalog)
        }
    }
}

fn render_json_array_values(
    items: &[Value],
    use_line_feeds: bool,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    let mut out = String::from("[");
    let sep = if use_line_feeds { ",\n " } else { "," };
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push_str(sep);
        }
        out.push_str(&render_json_value_text_with_config(
            item,
            datetime_config,
            catalog,
        ));
    }
    out.push(']');
    out
}

fn render_json_record_value(
    record: &RecordValue,
    use_line_feeds: bool,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    let mut out = String::from("{");
    let sep = if use_line_feeds { ",\n " } else { "," };
    for (idx, (field, item)) in record.iter().enumerate() {
        if idx > 0 {
            out.push_str(sep);
        }
        out.push_str(&serde_json::to_string(&field.name).unwrap());
        out.push(':');
        out.push_str(&render_json_field_value_text(
            item,
            field.sql_type,
            datetime_config,
            catalog,
        ));
    }
    out.push('}');
    out
}

fn render_json_field_value_text(
    value: &Value,
    sql_type: SqlType,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    if sql_type.kind == SqlTypeKind::RegClass && !sql_type.is_array {
        if let Some(relation_name) = regclass_name_for_value(value, catalog) {
            return serde_json::to_string(&relation_name).unwrap();
        }
        return serde_json::to_string(&oid_value_to_string(value)).unwrap();
    }
    render_json_value_text_with_config(value, datetime_config, catalog)
}

fn regclass_name_for_value(value: &Value, catalog: Option<&dyn CatalogLookup>) -> Option<String> {
    let oid = match value {
        Value::Int32(v) => u32::try_from(*v).ok()?,
        Value::Int64(v) => u32::try_from(*v).ok()?,
        _ => return None,
    };
    let catalog = catalog?;
    catalog.class_row_by_oid(oid).map(|row| row.relname)
}

fn oid_value_to_string(value: &Value) -> String {
    match value {
        Value::Int32(v) => (*v as u32).to_string(),
        Value::Int64(v) => (*v as u32).to_string(),
        _ => value.as_text().unwrap_or_default().to_string(),
    }
}

pub(crate) fn eval_json_table_function(
    kind: JsonTableFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    if kind == JsonTableFunction::JsonbPathQuery {
        let values = args
            .iter()
            .map(|arg| eval_expr(arg, slot, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        return eval_jsonb_path_query_rows(&values);
    }

    let value = eval_expr(
        args.first()
            .ok_or_else(|| ExecError::RaiseException("missing json function argument".into()))?,
        slot,
        ctx,
    )?;
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
                    return Err(json_object_keys_non_object_error(
                        "json_object_keys",
                        &other,
                    ));
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
                JsonbValue::Array(_) => {
                    return Err(ExecError::DetailedError {
                        message: "cannot call jsonb_object_keys on an array".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
                _ => {
                    return Err(ExecError::DetailedError {
                        message: "cannot call jsonb_object_keys on a scalar".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
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
                        left: json_value_to_value(&other, false, false),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in map {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    json_value_to_value(&value, false, false),
                ]));
            }
        }
        (JsonTableFunction::JsonbEach, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Object(items) => items,
                other => {
                    let _ = other;
                    return Err(jsonb_non_object_error("jsonb_each"));
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
                        left: json_value_to_value(&other, false, false),
                        right: Value::Null,
                    });
                }
            };
            for (key, value) in map {
                rows.push(TupleSlot::virtual_row(vec![
                    Value::Text(CompactString::from_owned(key)),
                    json_value_to_value(&value, true, false),
                ]));
            }
        }
        (JsonTableFunction::JsonbEachText, ParsedJsonValue::Jsonb(json)) => {
            let items = match json {
                JsonbValue::Object(items) => items,
                other => {
                    let _ = other;
                    return Err(jsonb_non_object_error("jsonb_each_text"));
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
                        left: json_value_to_value(&other, false, false),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(
                    &value, false, false,
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
                        left: json_value_to_value(&other, false, false),
                        right: Value::Null,
                    });
                }
            };
            for value in items {
                rows.push(TupleSlot::virtual_row(vec![json_value_to_value(
                    &value, true, false,
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
                    JsonTableFunction::JsonbPathQuery => "jsonb_path_query",
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
                    JsonTableFunction::JsonbPathQuery => "jsonb_path_query",
                    JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
                    JsonTableFunction::JsonbEach => "jsonb_each",
                    JsonTableFunction::JsonbEachText => "jsonb_each_text",
                    JsonTableFunction::JsonbArrayElements => "jsonb_array_elements",
                    JsonTableFunction::JsonbArrayElementsText => "jsonb_array_elements_text",
                },
                left: json_value_to_value(&json, false, false),
                right: Value::Null,
            });
        }
    }
    Ok(rows)
}

pub(crate) fn eval_sql_json_table(
    table: &SqlJsonTable,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let context = eval_expr(&table.context, slot, ctx)?;
    if matches!(context, Value::Null) {
        return Ok(Vec::new());
    }
    let document = parse_jsonpath_target_value(&context)?;
    let vars = eval_sql_json_table_vars(table, slot, ctx)?;
    let row_values = eval_sql_json_table_plan(
        &table.plan,
        &document,
        vars.as_ref(),
        table,
        slot,
        ctx,
        table.columns.len(),
    )?;
    Ok(row_values.into_iter().map(TupleSlot::virtual_row).collect())
}

fn eval_sql_json_table_vars(
    table: &SqlJsonTable,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Option<JsonbValue>, ExecError> {
    if table.passing.is_empty() {
        return Ok(None);
    }
    let mut pairs = Vec::with_capacity(table.passing.len());
    for arg in &table.passing {
        let value = eval_expr(&arg.expr, slot, ctx)?;
        pairs.push((
            arg.name.clone(),
            sql_json_passing_value_from_value(&value, &ctx.datetime_config)?,
        ));
    }
    Ok(Some(JsonbValue::Object(pairs)))
}

fn eval_sql_json_table_plan(
    plan: &SqlJsonTablePlan,
    source: &JsonbValue,
    vars: Option<&JsonbValue>,
    table: &SqlJsonTable,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
    width: usize,
) -> Result<Vec<Vec<Value>>, ExecError> {
    match plan {
        SqlJsonTablePlan::PathScan {
            path,
            column_indexes,
            error_on_error,
            child,
            ..
        } => {
            let matches = eval_sql_json_path(source, path, vars, *error_on_error)?;
            let mut rows = Vec::new();
            for (ordinal, item) in matches.iter().enumerate() {
                let mut row = vec![Value::Null; width];
                for index in column_indexes {
                    row[*index] = eval_sql_json_table_column(
                        &table.columns[*index],
                        item,
                        vars,
                        (ordinal + 1) as i32,
                        slot,
                        ctx,
                    )?;
                }
                if let Some(child) = child {
                    let child_rows =
                        eval_sql_json_table_plan(child, item, vars, table, slot, ctx, width)?;
                    if child_rows.is_empty() {
                        rows.push(row);
                    } else {
                        for child_row in child_rows {
                            let mut combined = row.clone();
                            for (idx, value) in child_row.into_iter().enumerate() {
                                if !matches!(value, Value::Null) {
                                    combined[idx] = value;
                                }
                            }
                            rows.push(combined);
                        }
                    }
                } else {
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rows = eval_sql_json_table_plan(left, source, vars, table, slot, ctx, width)?;
            rows.extend(eval_sql_json_table_plan(
                right, source, vars, table, slot, ctx, width,
            )?);
            Ok(rows)
        }
    }
}

fn eval_sql_json_path(
    source: &JsonbValue,
    path: &str,
    vars: Option<&JsonbValue>,
    error_on_error: bool,
) -> Result<Vec<JsonbValue>, ExecError> {
    let parsed = parse_jsonpath(path)?;
    let ctx = JsonPathEvaluationContext { root: source, vars };
    match evaluate_jsonpath(&parsed, &ctx) {
        Ok(values) => Ok(values),
        Err(err) if !error_on_error => {
            let _ = err;
            Ok(Vec::new())
        }
        Err(err) => Err(err),
    }
}

fn eval_sql_json_table_column(
    column: &crate::include::nodes::primnodes::SqlJsonTableColumn,
    source: &JsonbValue,
    vars: Option<&JsonbValue>,
    ordinal: i32,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match &column.kind {
        SqlJsonTableColumnKind::Ordinality => Ok(Value::Int32(ordinal)),
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => eval_sql_json_table_scalar_column(
            source,
            vars,
            path,
            column.sql_type,
            on_empty,
            on_error,
            slot,
            ctx,
        ),
        SqlJsonTableColumnKind::Formatted {
            path,
            format_json: _,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } => eval_sql_json_table_formatted_column(
            source,
            vars,
            path,
            column.sql_type,
            *wrapper,
            *quotes,
            on_empty,
            on_error,
            slot,
            ctx,
        ),
        SqlJsonTableColumnKind::Exists { path, on_error } => eval_sql_json_table_exists_column(
            source,
            vars,
            path,
            column.sql_type,
            on_error,
            slot,
            ctx,
        ),
    }
}

fn eval_sql_json_table_scalar_column(
    source: &JsonbValue,
    vars: Option<&JsonbValue>,
    path: &str,
    target_type: SqlType,
    on_empty: &SqlJsonTableBehavior,
    on_error: &SqlJsonTableBehavior,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = match eval_sql_json_path(source, path, vars, true) {
        Ok(values) => values,
        Err(_) => return eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx),
    };
    if values.is_empty() {
        return eval_sql_json_behavior(on_empty, target_type, "EMPTY", slot, ctx);
    }
    if values.len() != 1 {
        return eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx);
    }
    let value = &values[0];
    if matches!(value, JsonbValue::Array(_) | JsonbValue::Object(_)) {
        return eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx);
    }
    if matches!(value, JsonbValue::Null) {
        return Ok(Value::Null);
    }
    let text = jsonb_scalar_sql_text_for_type(value, target_type, &ctx.datetime_config);
    cast_sql_json_text_value(&text, target_type, ctx)
        .or_else(|_| eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx))
}

#[allow(clippy::too_many_arguments)]
fn eval_sql_json_table_formatted_column(
    source: &JsonbValue,
    vars: Option<&JsonbValue>,
    path: &str,
    target_type: SqlType,
    wrapper: SqlJsonTableWrapper,
    quotes: SqlJsonTableQuotes,
    on_empty: &SqlJsonTableBehavior,
    on_error: &SqlJsonTableBehavior,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = match eval_sql_json_path(source, path, vars, true) {
        Ok(values) => values,
        Err(_) => return eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx),
    };
    if values.is_empty() {
        return eval_sql_json_behavior(on_empty, target_type, "EMPTY", slot, ctx);
    }
    let value = match wrapper {
        SqlJsonTableWrapper::Unconditional => JsonbValue::Array(values),
        SqlJsonTableWrapper::Conditional if values.len() == 1 => values[0].clone(),
        SqlJsonTableWrapper::Conditional => JsonbValue::Array(values),
        SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without if values.len() == 1 => {
            values[0].clone()
        }
        SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without => {
            return eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx);
        }
    };
    cast_sql_json_formatted_value(&value, target_type, quotes, on_error, slot, ctx)
}

fn eval_sql_json_table_exists_column(
    source: &JsonbValue,
    vars: Option<&JsonbValue>,
    path: &str,
    target_type: SqlType,
    on_error: &SqlJsonTableBehavior,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let exists = match eval_sql_json_path(source, path, vars, true) {
        Ok(values) => !values.is_empty(),
        Err(_) => return eval_sql_json_exists_behavior(on_error, target_type, slot, ctx),
    };
    coerce_sql_json_exists_bool(exists, target_type, ctx).or_else(|err| {
        if matches!(on_error, SqlJsonTableBehavior::Error) {
            Err(err)
        } else if matches!(on_error, SqlJsonTableBehavior::True) {
            eval_sql_json_exists_behavior(on_error, target_type, slot, ctx)
        } else {
            Err(sql_json_behavior_coercion_error("ERROR", "FALSE", err))
        }
    })
}

fn eval_sql_json_exists_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match behavior {
        SqlJsonTableBehavior::True => coerce_sql_json_exists_bool(true, target_type, ctx)
            .map_err(|err| sql_json_behavior_coercion_error("ERROR", "TRUE", err)),
        SqlJsonTableBehavior::False => coerce_sql_json_exists_bool(false, target_type, ctx)
            .map_err(|err| sql_json_behavior_coercion_error("ERROR", "FALSE", err)),
        _ => eval_sql_json_behavior(behavior, target_type, "ERROR", slot, ctx),
    }
}

fn coerce_sql_json_exists_bool(
    exists: bool,
    target_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if !target_type.is_array && matches!(target_type.kind, SqlTypeKind::Bool) {
        return Ok(Value::Bool(exists));
    }
    if !target_type.is_array && matches!(target_type.kind, SqlTypeKind::Int4) {
        return cast_value_with_source_type_catalog_and_config(
            Value::Int32(if exists { 1 } else { 0 }),
            Some(SqlType::new(SqlTypeKind::Int4)),
            target_type,
            executor_catalog(ctx),
            &ctx.datetime_config,
        );
    }
    let text = if exists { "true" } else { "false" };
    reject_sql_json_overlength_text(text, target_type)?;
    cast_sql_json_text_value(text, target_type, ctx)
}

fn reject_sql_json_overlength_text(text: &str, target_type: SqlType) -> Result<(), ExecError> {
    if target_type.is_array {
        return Ok(());
    }
    let Some(max_chars) = target_type.char_len() else {
        return Ok(());
    };
    if !matches!(target_type.kind, SqlTypeKind::Char | SqlTypeKind::Varchar) {
        return Ok(());
    }
    if text.chars().count() <= max_chars as usize {
        return Ok(());
    }
    Err(ExecError::StringDataRightTruncation {
        ty: match target_type.kind {
            SqlTypeKind::Char => format!("character({max_chars})"),
            SqlTypeKind::Varchar => format!("character varying({max_chars})"),
            _ => format!("character varying({max_chars})"),
        },
    })
}

fn eval_sql_json_behavior(
    behavior: &SqlJsonTableBehavior,
    target_type: SqlType,
    target: &'static str,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match behavior {
        SqlJsonTableBehavior::Null
        | SqlJsonTableBehavior::Empty
        | SqlJsonTableBehavior::EmptyArray
        | SqlJsonTableBehavior::EmptyObject
        | SqlJsonTableBehavior::Unknown => {
            let behavior_name = match behavior {
                SqlJsonTableBehavior::Null => "NULL",
                SqlJsonTableBehavior::Empty => "EMPTY",
                SqlJsonTableBehavior::EmptyArray => "EMPTY ARRAY",
                SqlJsonTableBehavior::EmptyObject => "EMPTY OBJECT",
                SqlJsonTableBehavior::Unknown => "UNKNOWN",
                _ => unreachable!("matched above"),
            };
            coerce_sql_json_null_value(target_type, ctx)
                .map_err(|err| sql_json_behavior_coercion_error(target, behavior_name, err))
        }
        SqlJsonTableBehavior::Error => Err(ExecError::DetailedError {
            message: "JSON_TABLE column evaluation failed".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        SqlJsonTableBehavior::Default(expr) => {
            let source_type = expr_sql_type_hint(expr);
            match eval_expr(expr, slot, ctx)
                .and_then(|value| cast_sql_json_default_value(value, source_type, target_type, ctx))
            {
                Ok(value) => Ok(value),
                Err(err) if sql_json_default_error_is_direct(&err) => Err(err),
                Err(err) => Err(sql_json_behavior_coercion_error(target, "DEFAULT", err)),
            }
        }
        SqlJsonTableBehavior::True => cast_value_with_source_type_catalog_and_config(
            Value::Bool(true),
            Some(SqlType::new(SqlTypeKind::Bool)),
            target_type,
            executor_catalog(ctx),
            &ctx.datetime_config,
        ),
        SqlJsonTableBehavior::False => cast_value_with_source_type_catalog_and_config(
            Value::Bool(false),
            Some(SqlType::new(SqlTypeKind::Bool)),
            target_type,
            executor_catalog(ctx),
            &ctx.datetime_config,
        ),
    }
}

fn cast_sql_json_default_value(
    value: Value,
    source_type: Option<SqlType>,
    target_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if let Some(text) = value.as_text()
        && is_sql_json_bit_string_target(target_type)
    {
        return cast_text_value_with_catalog_and_config(
            text,
            target_type,
            false,
            executor_catalog(ctx),
            &ctx.datetime_config,
        );
    }
    cast_value_with_source_type_catalog_and_config(
        value,
        source_type,
        target_type,
        executor_catalog(ctx),
        &ctx.datetime_config,
    )
}

fn sql_json_default_error_is_direct(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { source, .. } => sql_json_default_error_is_direct(source),
        ExecError::BitStringLengthMismatch { .. }
        | ExecError::BitStringTooLong { .. }
        | ExecError::StringDataRightTruncation { .. } => true,
        ExecError::DetailedError {
            message, sqlstate, ..
        } if *sqlstate == "23502"
            && message.starts_with("domain ")
            && message.ends_with(" does not allow null values") =>
        {
            true
        }
        _ => false,
    }
}

fn coerce_sql_json_null_value(
    target_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(catalog) = executor_catalog(ctx) else {
        return Ok(Value::Null);
    };
    let Some(domain) = catalog.domain_by_type_oid(target_type.type_oid) else {
        return Ok(Value::Null);
    };
    if domain.not_null {
        Err(ExecError::DetailedError {
            message: format!("domain {} does not allow null values", domain.name),
            detail: None,
            hint: None,
            sqlstate: "23502",
        })
    } else {
        Ok(Value::Null)
    }
}

fn sql_json_behavior_coercion_error(
    target: &'static str,
    behavior: &'static str,
    err: ExecError,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "could not coerce ON {target} expression ({behavior}) to the RETURNING type"
        ),
        detail: Some(sql_json_error_message(&err)),
        hint: None,
        sqlstate: "22023",
    }
}

fn sql_json_error_message(err: &ExecError) -> String {
    match err {
        ExecError::WithContext { source, .. } => sql_json_error_message(source),
        ExecError::Parse(parse) => parse.to_string(),
        ExecError::DetailedError { message, .. } => message.clone(),
        ExecError::JsonInput { message, .. } => message.clone(),
        ExecError::XmlInput { message, .. } => message.clone(),
        ExecError::ArrayInput { message, .. } => message.clone(),
        ExecError::StringDataRightTruncation { ty } => format!("value too long for type {ty}"),
        ExecError::BitStringLengthMismatch { actual, expected } => {
            format!("bit string length {actual} does not match type bit({expected})")
        }
        ExecError::BitStringTooLong { limit, .. } => {
            format!("bit string too long for type bit varying({limit})")
        }
        ExecError::InvalidIntegerInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::InvalidFloatInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::InvalidBooleanInput { value } => {
            format!("invalid input syntax for type boolean: \"{value}\"")
        }
        other => format!("{other:?}"),
    }
}

fn executor_catalog(ctx: &ExecutorContext) -> Option<&dyn CatalogLookup> {
    ctx.catalog.as_deref()
}

fn jsonb_scalar_sql_text(
    value: &JsonbValue,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
) -> String {
    match value {
        JsonbValue::String(text) => text.clone(),
        JsonbValue::Bool(true) => "true".into(),
        JsonbValue::Bool(false) => "false".into(),
        JsonbValue::Numeric(value) => value.render(),
        JsonbValue::Date(v) => {
            render_datetime_value_text_with_config(&Value::Date(*v), datetime_config)
                .expect("datetime values render")
        }
        JsonbValue::Time(v) => {
            render_datetime_value_text_with_config(&Value::Time(*v), datetime_config)
                .expect("datetime values render")
        }
        JsonbValue::TimeTz(v) => {
            render_datetime_value_text_with_config(&Value::TimeTz(*v), datetime_config)
                .expect("datetime values render")
        }
        JsonbValue::Timestamp(v) => {
            render_datetime_value_text_with_config(&Value::Timestamp(*v), datetime_config)
                .expect("datetime values render")
        }
        JsonbValue::TimestampTz(v) => {
            render_datetime_value_text_with_config(&Value::TimestampTz(*v), datetime_config)
                .expect("datetime values render")
        }
        JsonbValue::Null | JsonbValue::Array(_) | JsonbValue::Object(_) => {
            render_jsonb_value_text(value)
        }
    }
}

fn jsonb_scalar_sql_text_for_type(
    value: &JsonbValue,
    target_type: SqlType,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
) -> String {
    if target_type.type_oid == 0
        && !target_type.is_array
        && matches!(
            target_type.kind,
            SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
    {
        match value {
            JsonbValue::Bool(true) => return "t".into(),
            JsonbValue::Bool(false) => return "f".into(),
            _ => {}
        }
    }
    jsonb_scalar_sql_text(value, datetime_config)
}

fn cast_sql_json_formatted_value(
    value: &JsonbValue,
    target_type: SqlType,
    quotes: SqlJsonTableQuotes,
    on_error: &SqlJsonTableBehavior,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(quotes, SqlJsonTableQuotes::Omit)
        && let JsonbValue::String(text) = value
    {
        return cast_sql_json_text_value(text, target_type, ctx)
            .or_else(|_| eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx));
    }
    match target_type.kind {
        SqlTypeKind::Json if !target_type.is_array => {
            Ok(Value::Json(CompactString::from_owned(value.render())))
        }
        SqlTypeKind::Jsonb if !target_type.is_array => Ok(Value::Jsonb(encode_jsonb(value))),
        _ => {
            let text = value.render();
            cast_sql_json_text_value(&text, target_type, ctx)
                .or_else(|_| eval_sql_json_behavior(on_error, target_type, "ERROR", slot, ctx))
        }
    }
}

fn cast_sql_json_text_value(
    text: &str,
    target_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    reject_sql_json_overlength_text(text, target_type)?;
    if is_sql_json_bit_string_target(target_type) {
        return cast_text_value_with_catalog_and_config(
            text,
            target_type,
            false,
            executor_catalog(ctx),
            &ctx.datetime_config,
        );
    }
    if !target_type.is_array
        && matches!(
            target_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
        && let Some(descriptor) = record_descriptor_from_sql_type(target_type, ctx)?
    {
        return parse_record_literal_to_value(text, descriptor, ctx);
    }
    cast_value_with_source_type_catalog_and_config(
        Value::Text(CompactString::new(text)),
        Some(SqlType::new(SqlTypeKind::Text)),
        target_type,
        executor_catalog(ctx),
        &ctx.datetime_config,
    )
}

fn is_sql_json_bit_string_target(target_type: SqlType) -> bool {
    !target_type.is_array && matches!(target_type.kind, SqlTypeKind::Bit | SqlTypeKind::VarBit)
}

fn eval_jsonb_path_query_rows(values: &[Value]) -> Result<Vec<TupleSlot>, ExecError> {
    let target = values.first().unwrap_or(&Value::Null);
    let path = values.get(1).unwrap_or(&Value::Null);
    if matches!(target, Value::Null) || matches!(path, Value::Null) {
        return Ok(Vec::new());
    }
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
    let vars_json = match values.get(2) {
        Some(Value::Null) | None => None,
        Some(value) => Some(parse_jsonpath_target_value(value)?),
    };
    let eval_ctx = JsonPathEvaluationContext {
        root: &target,
        vars: vars_json.as_ref(),
    };
    let result = evaluate_jsonpath(&parsed, &eval_ctx);
    match result {
        Ok(items) => Ok(items
            .into_iter()
            .map(|item| TupleSlot::virtual_row(vec![jsonb_to_value(&item)]))
            .collect()),
        Err(_) if silent => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}
