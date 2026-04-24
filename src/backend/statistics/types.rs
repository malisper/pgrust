use crate::backend::libpq::pqformat::format_bytea_text;
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
