use crate::backend::executor::{
    Value, format_array_value_text, render_datetime_value_text, render_internal_char_text,
    render_tsquery_text, render_tsvector_text, render_uuid_text,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::include::nodes::datum::ArrayValue;
use crate::pgrust::session::ByteaOutputFormat;

pub use pgrust_catalog_data::statistics_payload::{
    PgDependenciesPayload, PgDependencyItem, PgMcvItem, PgMcvListPayload, PgNdistinctItem,
    PgNdistinctPayload, decode_pg_dependencies_payload, decode_pg_mcv_list_payload,
    decode_pg_ndistinct_payload, encode_pg_dependencies_payload, encode_pg_mcv_list_payload,
    encode_pg_ndistinct_payload,
};

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
            format!("\"{dims}\": {}", item.ndistinct)
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
            format!("\"{from} => {to}\": {:.6}", item.degree)
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{{{rendered}}}"))
}

pub fn render_pg_mcv_list_text(bytes: &[u8]) -> String {
    format_bytea_text(bytes, ByteaOutputFormat::Hex)
}
