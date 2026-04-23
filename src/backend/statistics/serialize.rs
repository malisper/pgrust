use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StatisticsEnvelope<T> {
    magic: String,
    version: u32,
    payload: T,
}

const STATISTICS_ENVELOPE_MAGIC: &str = "pgrust.stats";
const STATISTICS_ENVELOPE_VERSION: u32 = 1;

pub fn encode_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&StatisticsEnvelope {
        magic: STATISTICS_ENVELOPE_MAGIC.to_string(),
        version: STATISTICS_ENVELOPE_VERSION,
        payload,
    })
}

pub fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
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
