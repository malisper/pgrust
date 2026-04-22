use crate::backend::catalog::CatalogError;
use crate::backend::executor::value_io::{decode_value, encode_value};
use crate::include::access::htup::TupleValue;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<Vec<u8>, CatalogError> {
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
                let bytes = match encode_value(column, value).map_err(|err| {
                    CatalogError::Io(format!("spgist encode key failed: {err:?}"))
                })? {
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

pub(crate) fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
) -> Result<Vec<Value>, CatalogError> {
    if payload.len() < 2 {
        return Err(CatalogError::Corrupt("spgist tuple payload too short"));
    }
    let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut values = Vec::with_capacity(count);
    for column in desc.columns.iter().take(count) {
        if offset + 5 > payload.len() {
            return Err(CatalogError::Corrupt("spgist tuple payload truncated"));
        }
        let is_null = payload[offset] != 0;
        offset += 1;
        let len = u32::from_le_bytes(
            payload[offset..offset + 4]
                .try_into()
                .map_err(|_| CatalogError::Corrupt("spgist tuple payload length"))?,
        ) as usize;
        offset += 4;
        if is_null {
            values.push(Value::Null);
            continue;
        }
        if offset + len > payload.len() {
            return Err(CatalogError::Corrupt("spgist tuple payload overflow"));
        }
        values.push(
            decode_value(column, Some(&payload[offset..offset + len])).map_err(|err| {
                CatalogError::Io(format!("spgist decode key failed: {err:?}"))
            })?,
        );
        offset += len;
    }
    Ok(values)
}

pub(crate) fn decode_tuple_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    decode_key_payload(desc, &tuple.payload)
}

pub(crate) fn make_leaf_tuple(
    desc: &RelationDesc,
    values: &[Value],
    heap_tid: ItemPointerData,
) -> Result<IndexTupleData, CatalogError> {
    Ok(IndexTupleData::new_raw(
        heap_tid,
        values.iter().any(|value| matches!(value, Value::Null)),
        true,
        false,
        encode_key_payload(desc, values)?,
    ))
}

pub(crate) fn tuple_storage_size(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<usize, CatalogError> {
    Ok(8 + encode_key_payload(desc, values)?.len())
}
