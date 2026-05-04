use std::sync::Mutex;
use std::time::Duration;

use pgrust_expr::backend::access::hash::support::hash_bytes_extended;
use pgrust_expr::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use pgrust_expr::backend::{
    executor::expr_casts::parse_uuid_text, executor::value_io::render_uuid_text,
};
use pgrust_expr::parse_interval_text_value;
use pgrust_nodes::Value;
use pgrust_nodes::datetime::TimestampTzADT;
use pgrust_nodes::datum::IntervalValue;
use pgrust_nodes::datum::array_value_from_value;
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use rand::RngCore;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum MiscBuiltinError {
    MalformedCall {
        op: &'static str,
    },
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    InvalidSleepDuration {
        actual: String,
    },
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    NegativeTerminateTimeout,
    Expr(pgrust_expr::ExprError),
}

impl From<pgrust_expr::ExprError> for MiscBuiltinError {
    fn from(error: pgrust_expr::ExprError) -> Self {
        Self::Expr(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSignalPermission {
    pub message: &'static str,
    pub detail: &'static str,
}

pub fn int32_arg(value: &Value, op: &'static str) -> Result<i32, MiscBuiltinError> {
    match value {
        Value::Int16(v) => Ok(i32::from(*v)),
        Value::Int32(v) => Ok(*v),
        Value::Int64(v) => i32::try_from(*v).map_err(|_| MiscBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
        _ => Err(MiscBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
    }
}

pub fn int64_arg(value: &Value, op: &'static str) -> Result<i64, MiscBuiltinError> {
    match value {
        Value::Int16(v) => Ok(i64::from(*v)),
        Value::Int32(v) => Ok(i64::from(*v)),
        Value::Int64(v) => Ok(*v),
        _ => Err(MiscBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(0),
        }),
    }
}

pub fn canonicalize_path_text(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            if parts.last().is_some_and(|last| *last != "..") {
                parts.pop();
            } else if !absolute {
                parts.push(part);
            }
        } else {
            parts.push(part);
        }
    }
    if absolute {
        if parts.is_empty() {
            "/".into()
        } else {
            format!("/{}", parts.join("/"))
        }
    } else if parts.is_empty() {
        ".".into()
    } else {
        parts.join("/")
    }
}

pub fn eval_test_canonicalize_path(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let [value] = values else {
        return Err(MiscBuiltinError::MalformedCall {
            op: "test_canonicalize_path",
        });
    };
    Ok(value
        .as_text()
        .map(canonicalize_path_text)
        .map(Into::into)
        .map(Value::Text)
        .unwrap_or(Value::Null))
}

pub fn eval_gist_translate_cmptype_common(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let [value] = values else {
        return Err(MiscBuiltinError::MalformedCall {
            op: "gist_translate_cmptype_common",
        });
    };
    let strategy = int32_arg(value, "gist_translate_cmptype_common")?;
    Ok(Value::Int16(match strategy {
        3 => 18,
        7 => 3,
        other => other as i16,
    }))
}

pub fn eval_pg_log_backend_memory_contexts(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let [value] = values else {
        return Err(MiscBuiltinError::MalformedCall {
            op: "pg_log_backend_memory_contexts",
        });
    };
    let _pid = int32_arg(value, "pg_log_backend_memory_contexts")?;
    Ok(Value::Bool(true))
}

pub fn eval_pg_current_logfile(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    if values.len() > 1 {
        return Err(MiscBuiltinError::MalformedCall {
            op: "pg_current_logfile",
        });
    }
    Ok(Value::Null)
}

pub fn eval_pg_sleep_function(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let seconds = match values {
        [Value::Null] => return Ok(Value::Null),
        [Value::Float64(value)] => *value,
        [Value::Int32(value)] => *value as f64,
        [Value::Int64(value)] => *value as f64,
        [Value::Interval(value)] if value.is_finite() => value.cmp_key() as f64 / 1_000_000.0,
        [value] if value.as_text().is_some() => {
            let interval = parse_interval_text_value(value.as_text().expect("guarded above"))?;
            interval.cmp_key() as f64 / 1_000_000.0
        }
        [other] => {
            return Err(MiscBuiltinError::TypeMismatch {
                op: "pg_sleep",
                left: other.clone(),
                right: Value::Null,
            });
        }
        _ => {
            return Err(MiscBuiltinError::TypeMismatch {
                op: "pg_sleep",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(MiscBuiltinError::InvalidSleepDuration {
            actual: seconds.to_string(),
        });
    }
    std::thread::sleep(Duration::from_secs_f64(seconds));
    Ok(Value::Null)
}

pub fn int4_array_from_client_ids(pids: impl IntoIterator<Item = u32>) -> Value {
    Value::PgArray(
        pgrust_nodes::datum::ArrayValue::from_1d(
            pids.into_iter()
                .map(|pid| Value::Int32(pid as i32))
                .collect(),
        )
        .with_element_type_oid(pgrust_catalog_data::INT4_TYPE_OID),
    )
}

pub fn client_id_arg(value: &Value, op: &'static str) -> Result<Option<u32>, MiscBuiltinError> {
    match value {
        Value::Null => Ok(None),
        Value::Int32(pid) if *pid > 0 => Ok(Some(*pid as u32)),
        Value::Int32(_) => Ok(None),
        other => Err(MiscBuiltinError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int32(0),
        }),
    }
}

pub fn backend_signal_op(func: BuiltinScalarFunction) -> &'static str {
    match func {
        BuiltinScalarFunction::PgCancelBackend => "pg_cancel_backend",
        BuiltinScalarFunction::PgTerminateBackend => "pg_terminate_backend",
        _ => "pg_signal_backend",
    }
}

pub fn validate_backend_signal_args(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Result<Option<u32>, MiscBuiltinError> {
    let Some(pid_value) = values.first() else {
        return Err(MiscBuiltinError::MalformedCall {
            op: "pg_signal_backend",
        });
    };
    if matches!(pid_value, Value::Null) {
        return Ok(None);
    }
    if matches!(func, BuiltinScalarFunction::PgTerminateBackend) {
        let timeout = values
            .get(1)
            .map(|value| int64_arg(value, "pg_terminate_backend"))
            .transpose()?
            .unwrap_or(0);
        if timeout < 0 {
            return Err(MiscBuiltinError::NegativeTerminateTimeout);
        }
    }
    client_id_arg(pid_value, backend_signal_op(func))
}

pub fn backend_signal_permission(
    func: BuiltinScalarFunction,
    superuser_target: bool,
) -> BackendSignalPermission {
    let (message, detail) = match (func, superuser_target) {
        (BuiltinScalarFunction::PgCancelBackend, true) => (
            "permission denied to cancel query",
            "Only roles with the SUPERUSER attribute may cancel queries of roles with the SUPERUSER attribute.",
        ),
        (BuiltinScalarFunction::PgCancelBackend, false) => (
            "permission denied to cancel query",
            "Only roles with privileges of the role whose query is being canceled or with privileges of the \"pg_signal_backend\" role may cancel this query.",
        ),
        (BuiltinScalarFunction::PgTerminateBackend, true) => (
            "permission denied to terminate process",
            "Only roles with the SUPERUSER attribute may terminate processes of roles with the SUPERUSER attribute.",
        ),
        (BuiltinScalarFunction::PgTerminateBackend, false) => (
            "permission denied to terminate process",
            "Only roles with privileges of the role whose process is being terminated or with privileges of the \"pg_signal_backend\" role may terminate this process.",
        ),
        _ => (
            "permission denied to signal backend",
            "Insufficient privileges.",
        ),
    };
    BackendSignalPermission { message, detail }
}

pub fn isolation_session_is_blocked(blockers: &[u32], interesting_pids: &[u32]) -> Value {
    Value::Bool(
        blockers
            .iter()
            .any(|blocker| interesting_pids.contains(blocker)),
    )
}

pub fn configured_current_schema_search_path(raw_search_path: Option<&str>) -> Vec<String> {
    raw_search_path
        .filter(|value| !value.trim().eq_ignore_ascii_case("default"))
        .map(|value| {
            value
                .split(',')
                .map(|schema| {
                    schema
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_ascii_lowercase()
                })
                .filter(|schema| !schema.is_empty())
                .collect()
        })
        .unwrap_or_else(|| vec!["public".into()])
}

pub fn current_schema_is_temp(schema: &str) -> bool {
    schema.eq_ignore_ascii_case("pg_temp") || schema.to_ascii_lowercase().starts_with("pg_temp_")
}

pub fn current_schema_from_search_path(
    catalog_search_path: Vec<String>,
    configured_search_path: Vec<String>,
    namespace_names: Vec<String>,
) -> Value {
    let mut search_path = if catalog_search_path.is_empty() {
        configured_search_path
    } else {
        catalog_search_path
    };
    if search_path.len() > 1
        && search_path
            .first()
            .is_some_and(|schema| schema == "pg_catalog")
    {
        search_path.remove(0);
    }
    search_path
        .into_iter()
        .filter(|schema| schema != "$user" && !current_schema_is_temp(schema))
        .find(|schema| {
            namespace_names
                .iter()
                .any(|namespace| namespace.eq_ignore_ascii_case(schema))
        })
        .map(|schema| Value::Text(schema.into()))
        .unwrap_or(Value::Null)
}

pub fn current_schemas_value(
    include_implicit: bool,
    temp_schema: Option<String>,
    catalog_search_path: Vec<String>,
    configured_search_path: Vec<String>,
) -> Value {
    let mut schemas = Vec::<String>::new();
    let mut push_schema = |schema: String| {
        if !schemas
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&schema))
        {
            schemas.push(schema);
        }
    };

    if include_implicit {
        if let Some(temp_schema) = temp_schema.as_ref() {
            push_schema(temp_schema.clone());
        }
        push_schema("pg_catalog".into());
    }

    let configured_path = if catalog_search_path.is_empty() {
        configured_search_path
    } else {
        catalog_search_path
    };
    for schema in configured_path {
        if schema == "$user" {
            continue;
        }
        if schema.eq_ignore_ascii_case("pg_temp") {
            if include_implicit && let Some(temp_schema) = temp_schema.as_ref() {
                push_schema(temp_schema.clone());
            }
            continue;
        }
        if schema.eq_ignore_ascii_case("pg_catalog") && include_implicit {
            continue;
        }
        push_schema(schema);
    }

    Value::PgArray(
        pgrust_nodes::datum::ArrayValue::from_1d(
            schemas
                .into_iter()
                .map(|schema| Value::Text(schema.into()))
                .collect(),
        )
        .with_element_type_oid(pgrust_catalog_data::NAME_TYPE_OID),
    )
}

pub fn time_precision_overflow_warning(
    precision: Option<i32>,
    max_precision: i32,
    type_name: &str,
    suffix: &str,
) -> Option<String> {
    precision
        .filter(|precision| *precision > max_precision)
        .map(|precision| {
            format!(
                "{type_name}({precision}){suffix} precision reduced to maximum allowed, {max_precision}"
            )
        })
}

pub fn eval_num_nulls(values: &[Value], func_variadic: bool, count_nulls: bool) -> Value {
    if func_variadic {
        let Some(value) = values.first() else {
            return Value::Int32(0);
        };
        if matches!(value, Value::Null) {
            return Value::Null;
        }
        let Some(array) = array_value_from_value(value) else {
            return Value::Int32(if matches!(value, Value::Null) == count_nulls {
                1
            } else {
                0
            });
        };
        let count = array
            .elements
            .iter()
            .filter(|value| matches!(value, Value::Null) == count_nulls)
            .count();
        return Value::Int32(count as i32);
    }
    Value::Int32(
        values
            .iter()
            .filter(|value| matches!(value, Value::Null) == count_nulls)
            .count() as i32,
    )
}

pub fn eval_greatest(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let mut best: Option<Value> = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let replace = match best.as_ref() {
            None => true,
            Some(current) => {
                pgrust_expr::compare_order_values(current, value, None, None, false)?
                    == Ordering::Less
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

pub fn eval_least(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let mut best: Option<Value> = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let replace = match best.as_ref() {
            None => true,
            Some(current) => {
                pgrust_expr::compare_order_values(current, value, None, None, false)?
                    == Ordering::Greater
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

pub fn eval_convert(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [Value::Bytea(bytes), _, _] => Ok(Value::Bytea(bytes.clone())),
        _ => Err(MiscBuiltinError::TypeMismatch {
            op: "convert",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

pub fn eval_pg_column_toast_chunk_id_values(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [_] => Ok(Value::Null),
        _ => Err(MiscBuiltinError::UnexpectedToken {
            expected: "pg_column_toast_chunk_id(any)",
            actual: format!("PgColumnToastChunkId({} args)", values.len()),
        }),
    }
}

pub fn eval_get_database_encoding() -> Value {
    Value::Text("UTF8".into())
}

pub fn eval_pg_encoding_to_char(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let encoding = match values {
        [Value::Int16(value)] => i32::from(*value),
        [Value::Int32(value)] => *value,
        [Value::Int64(value)] => i32::try_from(*value).unwrap_or(-1),
        [Value::Null] => return Ok(Value::Null),
        _ => {
            return Err(MiscBuiltinError::TypeMismatch {
                op: "pg_encoding_to_char",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Int32(0),
            });
        }
    };
    let name = match encoding {
        0 => "SQL_ASCII",
        6 => "UTF8",
        _ => "",
    };
    Ok(Value::Text(name.into()))
}

pub fn eval_pg_char_to_encoding(values: &[Value]) -> Result<Value, MiscBuiltinError> {
    let name = match values {
        [value] if value.as_text().is_some() => value.as_text().expect("guarded above"),
        [Value::Null] => return Ok(Value::Null),
        _ => {
            return Err(MiscBuiltinError::TypeMismatch {
                op: "pg_char_to_encoding",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Text(String::new().into()),
            });
        }
    };
    let cleaned: String = name
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect();
    let encoding = match cleaned.as_str() {
        "" => -1,
        "sqlascii" => 0,
        "eucjp" => 1,
        "euccn" => 2,
        "euckr" => 3,
        "euctw" => 4,
        "eucjis2004" => 5,
        "utf8" | "unicode" => 6,
        "muleinternal" => 7,
        "latin1" | "iso88591" => 8,
        "latin2" | "iso88592" => 9,
        "latin3" | "iso88593" => 10,
        "latin4" | "iso88594" => 11,
        "latin5" | "iso88599" => 12,
        "latin6" | "iso885910" => 13,
        "latin7" | "iso885913" => 14,
        "latin8" | "iso885914" => 15,
        "latin9" | "iso885915" => 16,
        "latin10" | "iso885916" => 17,
        "win1256" | "windows1256" => 18,
        "win1258" | "windows1258" | "abc" | "tcvn" | "tcvn5712" | "vscii" => 19,
        "win866" | "windows866" | "alt" => 20,
        "win874" | "windows874" => 21,
        "koi8r" | "koi8" => 22,
        "win1251" | "windows1251" | "win" => 23,
        "win1252" | "windows1252" => 24,
        "iso88595" => 25,
        "iso88596" => 26,
        "iso88597" => 27,
        "iso88598" => 28,
        "win1250" | "windows1250" => 29,
        "win1253" | "windows1253" => 30,
        "win1254" | "windows1254" => 31,
        "win1255" | "windows1255" => 32,
        "win1257" | "windows1257" => 33,
        "koi8u" => 34,
        "sjis" | "shiftjis" | "mskanji" | "win932" | "windows932" => 35,
        "big5" | "win950" | "windows950" => 36,
        "gbk" | "win936" | "windows936" => 37,
        "uhc" | "win949" | "windows949" => 38,
        "gb18030" => 39,
        "johab" => 40,
        "shiftjis2004" => 41,
        _ => -1,
    };
    Ok(Value::Int32(encoding))
}

pub fn eval_uuid_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Result<Value, MiscBuiltinError> {
    match func {
        BuiltinScalarFunction::UuidIn => match values {
            [Value::Text(text)] => Ok(Value::Uuid(parse_uuid_text(text)?)),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_in",
                left: value.clone(),
                right: Value::Text("".into()),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid_in" }),
        },
        BuiltinScalarFunction::UuidOut => match values {
            [Value::Uuid(value)] => Ok(Value::Text(render_uuid_text(value).into())),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_out",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid_out" }),
        },
        BuiltinScalarFunction::UuidRecv => match values {
            [Value::Bytea(bytes)] if bytes.len() == 16 => {
                Ok(Value::Uuid(bytes.as_slice().try_into().unwrap()))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_recv",
                left: value.clone(),
                right: Value::Bytea(vec![0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid_recv" }),
        },
        BuiltinScalarFunction::UuidSend => match values {
            [Value::Uuid(value)] => Ok(Value::Bytea(value.to_vec())),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_send",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid_send" }),
        },
        BuiltinScalarFunction::UuidEq
        | BuiltinScalarFunction::UuidNe
        | BuiltinScalarFunction::UuidLt
        | BuiltinScalarFunction::UuidLe
        | BuiltinScalarFunction::UuidGt
        | BuiltinScalarFunction::UuidGe
        | BuiltinScalarFunction::UuidCmp => match values {
            [Value::Uuid(left), Value::Uuid(right)] => Ok(match func {
                BuiltinScalarFunction::UuidEq => Value::Bool(left == right),
                BuiltinScalarFunction::UuidNe => Value::Bool(left != right),
                BuiltinScalarFunction::UuidLt => Value::Bool(left < right),
                BuiltinScalarFunction::UuidLe => Value::Bool(left <= right),
                BuiltinScalarFunction::UuidGt => Value::Bool(left > right),
                BuiltinScalarFunction::UuidGe => Value::Bool(left >= right),
                BuiltinScalarFunction::UuidCmp => Value::Int32(match left.cmp(right) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                }),
                _ => unreachable!(),
            }),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [left, right] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid" }),
        },
        BuiltinScalarFunction::UuidHash => match values {
            [Value::Uuid(value)] => Ok(Value::Int32(uuid_hash(value) as i32)),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_hash",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuid_hash" }),
        },
        BuiltinScalarFunction::UuidHashExtended => match values {
            [Value::Uuid(value), Value::Int64(seed)] => {
                Ok(Value::Int64(uuid_hash_extended(value, *seed as u64) as i64))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [left, right] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_hash_extended",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(MiscBuiltinError::MalformedCall {
                op: "uuid_hash_extended",
            }),
        },
        BuiltinScalarFunction::GenRandomUuid => match values {
            [] => Ok(Value::Uuid(generate_uuid_v4())),
            _ => Err(MiscBuiltinError::MalformedCall {
                op: "gen_random_uuid",
            }),
        },
        BuiltinScalarFunction::UuidV7 => match values {
            [] => Ok(Value::Uuid(generate_uuid_v7(0))),
            [Value::Interval(interval)] => {
                let shift_millis = interval.time_micros / 1_000
                    + i64::from(interval.days) * 86_400_000
                    + i64::from(interval.months) * 30 * 86_400_000;
                Ok(Value::Uuid(generate_uuid_v7(shift_millis)))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuidv7",
                left: value.clone(),
                right: Value::Interval(IntervalValue::zero()),
            }),
            _ => Err(MiscBuiltinError::MalformedCall { op: "uuidv7" }),
        },
        BuiltinScalarFunction::UuidExtractVersion => match values {
            [Value::Uuid(value)] => {
                Ok(uuid_version(value).map(Value::Int16).unwrap_or(Value::Null))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_extract_version",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall {
                op: "uuid_extract_version",
            }),
        },
        BuiltinScalarFunction::UuidExtractTimestamp => match values {
            [Value::Uuid(value)] if uuid_version(value) == Some(1) => uuid_v1_timestamp(value)
                .map_or(Ok(Value::Null), |postgres_usecs| {
                    Ok(Value::TimestampTz(TimestampTzADT(postgres_usecs)))
                }),
            [Value::Uuid(value)] if uuid_version(value) == Some(7) => {
                Ok(Value::TimestampTz(TimestampTzADT(uuid_v7_timestamp(value))))
            }
            [Value::Uuid(_)] | [Value::Null] => Ok(Value::Null),
            [value] => Err(MiscBuiltinError::TypeMismatch {
                op: "uuid_extract_timestamp",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(MiscBuiltinError::MalformedCall {
                op: "uuid_extract_timestamp",
            }),
        },
        _ => unreachable!("uuid dispatcher called for non-uuid builtin"),
    }
}

fn generate_uuid_v4() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

static UUID_V7_STATE: Mutex<(u64, u64)> = Mutex::new((0, 0));

fn generate_uuid_v7(shift_millis: i64) -> [u8; 16] {
    let millis = current_postgres_timestamp_usecs()
        .saturating_div(1_000)
        .saturating_add(10_957 * 86_400_000)
        .saturating_add(shift_millis)
        .max(0) as u64;
    let mut bytes = [0u8; 16];
    bytes[0] = (millis >> 40) as u8;
    bytes[1] = (millis >> 32) as u8;
    bytes[2] = (millis >> 24) as u8;
    bytes[3] = (millis >> 16) as u8;
    bytes[4] = (millis >> 8) as u8;
    bytes[5] = millis as u8;
    rand::thread_rng().fill_bytes(&mut bytes[6..]);
    let sequence = {
        let mut state = UUID_V7_STATE.lock().expect("uuidv7 state mutex poisoned");
        if state.0 == millis {
            state.1 = state.1.wrapping_add(1) & ((1u64 << 42) - 1);
        } else {
            state.0 = millis;
            state.1 = 0;
        }
        state.1
    };
    bytes[6] = 0x70 | (((sequence >> 38) as u8) & 0x0f);
    bytes[7] = (sequence >> 30) as u8;
    bytes[8] = 0x80 | (((sequence >> 24) as u8) & 0x3f);
    bytes[9] = (sequence >> 16) as u8;
    bytes[10] = (sequence >> 8) as u8;
    bytes[11] = sequence as u8;
    bytes
}

fn uuid_version(value: &[u8; 16]) -> Option<i16> {
    ((value[8] & 0xc0) == 0x80).then_some(i16::from(value[6] >> 4))
}

fn uuid_v1_timestamp(value: &[u8; 16]) -> Option<i64> {
    let timestamp_100ns = ((u64::from(value[6] & 0x0f)) << 56)
        | (u64::from(value[7]) << 48)
        | (u64::from(value[4]) << 40)
        | (u64::from(value[5]) << 32)
        | (u64::from(value[0]) << 24)
        | (u64::from(value[1]) << 16)
        | (u64::from(value[2]) << 8)
        | u64::from(value[3]);
    let unix_100ns = timestamp_100ns.checked_sub(122_192_928_000_000_000)?;
    let unix_usecs = i64::try_from(unix_100ns / 10).ok()?;
    Some(unix_usecs - 10_957 * 86_400_000_000)
}

fn uuid_v7_timestamp(value: &[u8; 16]) -> i64 {
    let millis = ((value[0] as i64) << 40)
        | ((value[1] as i64) << 32)
        | ((value[2] as i64) << 24)
        | ((value[3] as i64) << 16)
        | ((value[4] as i64) << 8)
        | value[5] as i64;
    millis * 1_000 - 10_957 * 86_400_000_000
}

fn uuid_hash(value: &[u8; 16]) -> u32 {
    hash_bytes_extended(value, 0) as u32
}

fn uuid_hash_extended(value: &[u8; 16], seed: u64) -> u64 {
    hash_bytes_extended(value, seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_paths_like_postgres_test_helper() {
        assert_eq!(canonicalize_path_text("/a/./b/../c"), "/a/c");
        assert_eq!(canonicalize_path_text("a/../../b"), "../b");
        assert_eq!(canonicalize_path_text("."), ".");
    }

    #[test]
    fn backend_signal_args_validate_pid_and_timeout() {
        assert_eq!(
            validate_backend_signal_args(
                BuiltinScalarFunction::PgCancelBackend,
                &[Value::Int32(12)]
            )
            .unwrap(),
            Some(12)
        );
        assert!(matches!(
            validate_backend_signal_args(
                BuiltinScalarFunction::PgTerminateBackend,
                &[Value::Int32(12), Value::Int64(-1)]
            ),
            Err(MiscBuiltinError::NegativeTerminateTimeout)
        ));
    }

    #[test]
    fn misc_builtin_values_match_sql_shapes() {
        assert_eq!(eval_pg_current_logfile(&[]).unwrap(), Value::Null);
        assert_eq!(
            eval_pg_log_backend_memory_contexts(&[Value::Int32(1)]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_gist_translate_cmptype_common(&[Value::Int32(3)]).unwrap(),
            Value::Int16(18)
        );
    }

    #[test]
    fn current_schema_helpers_normalize_search_paths() {
        assert_eq!(
            configured_current_schema_search_path(Some("'$user', public")),
            vec!["$user".to_string(), "public".to_string()]
        );
        assert_eq!(
            current_schema_from_search_path(
                vec!["pg_catalog".into(), "app".into()],
                vec!["public".into()],
                vec!["app".into(), "public".into()]
            ),
            Value::Text("app".into())
        );
        let Value::PgArray(schemas) = current_schemas_value(
            true,
            Some("pg_temp_3".into()),
            vec!["pg_catalog".into(), "public".into()],
            vec!["ignored".into()],
        ) else {
            panic!("current_schemas should return an array");
        };
        assert_eq!(
            schemas.elements,
            vec![
                Value::Text("pg_temp_3".into()),
                Value::Text("pg_catalog".into()),
                Value::Text("public".into()),
            ]
        );
        assert_eq!(
            time_precision_overflow_warning(Some(9), 6, "time", ""),
            Some("time(9) precision reduced to maximum allowed, 6".into())
        );
    }

    #[test]
    fn null_count_and_extreme_value_helpers_follow_sql_semantics() {
        let values = vec![Value::Int32(2), Value::Null, Value::Int32(5)];
        assert_eq!(eval_num_nulls(&values, false, true), Value::Int32(1));
        assert_eq!(eval_num_nulls(&values, false, false), Value::Int32(2));
        assert_eq!(eval_greatest(&values).unwrap(), Value::Int32(5));
        assert_eq!(eval_least(&values).unwrap(), Value::Int32(2));
        assert_eq!(
            eval_convert(&[
                Value::Bytea(vec![1, 2]),
                Value::Text("UTF8".into()),
                Value::Text("UTF8".into()),
            ])
            .unwrap(),
            Value::Bytea(vec![1, 2])
        );
        assert_eq!(
            eval_pg_column_toast_chunk_id_values(&[Value::Int32(1)]).unwrap(),
            Value::Null
        );
        assert_eq!(eval_get_database_encoding(), Value::Text("UTF8".into()));
        assert_eq!(
            eval_pg_encoding_to_char(&[Value::Int32(6)]).unwrap(),
            Value::Text("UTF8".into())
        );
        assert_eq!(
            eval_pg_char_to_encoding(&[Value::Text("unicode".into())]).unwrap(),
            Value::Int32(6)
        );
    }
}
