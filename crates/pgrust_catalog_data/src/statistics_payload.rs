use serde::Serialize;
use serde::de::DeserializeOwned;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StatisticsEnvelope<T> {
    magic: String,
    version: u32,
    payload: T,
}

const STATISTICS_ENVELOPE_MAGIC: &str = "pgrust.stats";
const STATISTICS_ENVELOPE_VERSION: u32 = 1;

fn encode_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&StatisticsEnvelope {
        magic: STATISTICS_ENVELOPE_MAGIC.to_string(),
        version: STATISTICS_ENVELOPE_VERSION,
        payload,
    })
}

fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let envelope = serde_json::from_slice::<StatisticsEnvelope<T>>(bytes)
        .map_err(|err| format!("invalid statistics payload: {err}"))?;
    if envelope.magic != STATISTICS_ENVELOPE_MAGIC {
        return Err("invalid statistics payload magic".into());
    }
    if envelope.version != STATISTICS_ENVELOPE_VERSION {
        return Err(format!(
            "unsupported statistics payload version {}",
            envelope.version
        ));
    }
    Ok(envelope.payload)
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
