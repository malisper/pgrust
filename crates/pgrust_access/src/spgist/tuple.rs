use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;

use crate::access::htup::TupleValue;
use crate::access::itemptr::ItemPointerData;
use crate::access::itup::IndexTupleData;
use crate::{AccessError, AccessResult, AccessScalarServices};

pub fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
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
                let bytes = match services.encode_value(column, value)? {
                    TupleValue::Null => Vec::new(),
                    TupleValue::Bytes(bytes) => bytes,
                    TupleValue::EncodedVarlena(bytes) => bytes,
                };
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
        return Err(AccessError::Corrupt("spgist tuple payload too short"));
    }
    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut values = Vec::with_capacity(count);
    for column in desc.columns.iter().take(count) {
        if offset + 5 > payload.len() {
            return Err(AccessError::Corrupt("spgist tuple payload truncated"));
        }
        let is_null = payload[offset] != 0;
        offset += 1;
        let len = u32::from_le_bytes(
            payload[offset..offset + 4]
                .try_into()
                .map_err(|_| AccessError::Corrupt("spgist tuple payload length"))?,
        ) as usize;
        offset += 4;
        if is_null {
            values.push(Value::Null);
            continue;
        }
        if offset + len > payload.len() {
            return Err(AccessError::Corrupt("spgist tuple payload overflow"));
        }
        values.push(services.decode_value(column, Some(&payload[offset..offset + len]))?);
        offset += len;
    }
    Ok(values)
}

pub fn decode_tuple_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<Value>> {
    decode_key_payload(desc, &tuple.payload, services)
}

pub fn make_leaf_tuple(
    desc: &RelationDesc,
    values: &[Value],
    heap_tid: ItemPointerData,
    services: &dyn AccessScalarServices,
) -> AccessResult<IndexTupleData> {
    Ok(IndexTupleData::new_raw(
        heap_tid,
        values.iter().any(|value| matches!(value, Value::Null)),
        true,
        false,
        encode_key_payload(desc, values, services)?,
    ))
}

pub fn tuple_storage_size(
    desc: &RelationDesc,
    values: &[Value],
    services: &dyn AccessScalarServices,
) -> AccessResult<usize> {
    Ok(8 + encode_key_payload(desc, values, services)?.len())
}
