use crate::backend::executor::{
    Value, format_array_value_text, render_datetime_value_text, render_internal_char_text,
    render_tsquery_text, render_tsvector_text, render_uuid_text,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::include::nodes::datum::ArrayValue;
use crate::pgrust::session::ByteaOutputFormat;

use super::serialize::{decode_payload, encode_payload};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgNdistinctPayload {
    pub items: Vec<PgNdistinctItem>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgNdistinctItem {
    pub dimensions: Vec<i16>,
    pub ndistinct: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgDependenciesPayload {
    pub items: Vec<PgDependencyItem>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgDependencyItem {
    pub from: Vec<i16>,
    pub to: Vec<i16>,
    pub degree: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgMcvListPayload {
    pub items: Vec<PgMcvItem>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PgMcvItem {
    pub values: Vec<Option<String>>,
    pub frequency: f64,
    pub base_frequency: f64,
}

pub fn statistics_value_key(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(v.to_string()),
        Value::Int32(v) => Some(v.to_string()),
        Value::Int64(v) => Some(v.to_string()),
        Value::Float64(v) => Some(v.to_string()),
        Value::Bool(v) => Some(v.to_string()),
        Value::Text(text) => Some(text.to_string()),
        Value::TextRef(_, _) => Some(value.as_text().unwrap_or_default().to_string()),
        Value::Numeric(v) => Some(v.render()),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            Some(render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}")))
        }
        Value::Bytea(v) => Some(format!("{v:?}")),
        Value::Uuid(v) => Some(render_uuid_text(v)),
        Value::Bit(v) => Some(format!("{:?}", v.bytes)),
        Value::Array(values) => Some(format_array_value_text(&ArrayValue::from_1d(
            values.clone(),
        ))),
        Value::PgArray(array) => Some(format_array_value_text(array)),
        Value::TsVector(v) => Some(render_tsvector_text(v)),
        Value::TsQuery(v) => Some(render_tsquery_text(v)),
        Value::InternalChar(v) => Some(render_internal_char_text(*v)),
        Value::Json(text) => Some(text.to_string()),
        Value::Jsonb(bytes) => Some(format!("{bytes:?}")),
        Value::JsonPath(text) => Some(text.to_string()),
        other => Some(format!("{other:?}")),
    }
}

pub fn encode_pg_ndistinct_payload(payload: &PgNdistinctPayload) -> Result<Vec<u8>, String> {
    encode_payload(payload).map_err(|err| err.to_string())
}

pub fn decode_pg_ndistinct_payload(bytes: &[u8]) -> Result<PgNdistinctPayload, String> {
    decode_payload(bytes)
}

pub fn encode_pg_dependencies_payload(payload: &PgDependenciesPayload) -> Result<Vec<u8>, String> {
    encode_payload(payload).map_err(|err| err.to_string())
}

pub fn decode_pg_dependencies_payload(bytes: &[u8]) -> Result<PgDependenciesPayload, String> {
    decode_payload(bytes)
}

pub fn encode_pg_mcv_list_payload(payload: &PgMcvListPayload) -> Result<Vec<u8>, String> {
    encode_payload(payload).map_err(|err| err.to_string())
}

pub fn decode_pg_mcv_list_payload(bytes: &[u8]) -> Result<PgMcvListPayload, String> {
    decode_payload(bytes)
}

pub fn render_pg_ndistinct_text(bytes: &[u8]) -> Result<String, String> {
    let payload = decode_pg_ndistinct_payload(bytes)?;
    let rendered = payload
        .items
        .iter()
        .map(|item| {
            let dims = item
                .dimensions
                .iter()
                .map(i16::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{dims}}}: {}", item.ndistinct)
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{{{rendered}}}"))
}

pub fn render_pg_dependencies_text(bytes: &[u8]) -> Result<String, String> {
    let payload = decode_pg_dependencies_payload(bytes)?;
    let rendered = payload
        .items
        .iter()
        .map(|item| {
            let from = item
                .from
                .iter()
                .map(i16::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let to = item
                .to
                .iter()
                .map(i16::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{from}}} => {{{to}}}: {}", item.degree)
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{{{rendered}}}"))
}

pub fn render_pg_mcv_list_text(bytes: &[u8]) -> String {
    format_bytea_text(bytes, ByteaOutputFormat::Hex)
}
