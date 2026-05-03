use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::{ColumnDesc, RelationDesc};
use pgrust_storage::page::bufpage::MAX_HEAP_TUPLE_SIZE;

use crate::access::htup::{AttributeCompression, AttributeStorage, TupleValue};
use crate::common::toast_compression::compress_inline_datum;
use crate::{AccessError, AccessResult, AccessScalarServices};

const TOAST_INDEX_TARGET: usize = MAX_HEAP_TUPLE_SIZE / 16;

fn maybe_compress_index_value(
    column: &ColumnDesc,
    bytes: Vec<u8>,
    default_toast_compression: AttributeCompression,
) -> AccessResult<Vec<u8>> {
    if column.storage.attlen != -1
        || bytes.len() <= TOAST_INDEX_TARGET
        || !matches!(
            column.storage.attstorage,
            AttributeStorage::Extended | AttributeStorage::Main
        )
    {
        return Ok(bytes);
    }

    match compress_inline_datum(
        &bytes,
        column.storage.attcompression,
        default_toast_compression,
    ) {
        Ok(Some(compressed)) => Ok(compressed.encoded),
        Ok(None) => Ok(bytes),
        Err(err) => Err(AccessError::Unsupported(format!(
            "btree index key compression failed: {err:?}"
        ))),
    }
}

pub fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
    default_toast_compression: AttributeCompression,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for (column, value) in desc.columns.iter().zip(values.iter()) {
        match value {
            Value::Null => {
                payload.push(1);
                payload.extend_from_slice(&0u32.to_le_bytes());
            }
            _ => {
                payload.push(0);
                let encoded = services.encode_value(column, value)?;
                let bytes = match encoded {
                    TupleValue::Null => Vec::new(),
                    TupleValue::Bytes(bytes) | TupleValue::EncodedVarlena(bytes) => bytes,
                };
                let bytes = maybe_compress_index_value(column, bytes, default_toast_compression)?;
                payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                payload.extend_from_slice(&bytes);
            }
        }
    }
    Ok(payload)
}

pub fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<Value>> {
    if payload.len() < 2 {
        return Err(AccessError::Corrupt("index tuple payload too short"));
    }
    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut values = Vec::with_capacity(count);
    for column in desc.columns.iter().take(count) {
        if offset + 5 > payload.len() {
            return Err(AccessError::Corrupt("index tuple payload truncated"));
        }
        let is_null = payload[offset] != 0;
        offset += 1;
        let len = u32::from_le_bytes(
            payload[offset..offset + 4]
                .try_into()
                .map_err(|_| AccessError::Corrupt("index tuple payload length"))?,
        ) as usize;
        offset += 4;
        if is_null {
            values.push(Value::Null);
            continue;
        }
        if offset + len > payload.len() {
            return Err(AccessError::Corrupt("index tuple payload overflow"));
        }
        values.push(services.decode_value(column, Some(&payload[offset..offset + len]))?);
        offset += len;
    }
    Ok(values)
}
