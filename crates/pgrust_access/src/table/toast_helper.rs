pub use crate::access::toast_helper::*;

use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::Mutex;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_storage::page::bufpage::MAX_HEAP_TUPLE_SIZE;

use crate::access::heaptoast::{ExternalToastValueInput, StoredToastValue, encoded_pointer_bytes};
use crate::access::htup::{AttributeStorage, HeapTuple, TupleValue};
use crate::access::toast_compression::ToastCompressionId;
use crate::common::toast_compression::compress_inline_datum;
use crate::varatt::{
    VARHDRSZ, compressed_inline_compression_method, compressed_inline_extsize,
    is_compressed_inline_datum, is_ondisk_toast_pointer,
};
use crate::{AccessError, AccessResult};

const TOAST_TUPLE_TARGET: usize = 2_040;

static TOAST_TUPLE_TARGETS: OnceLock<Mutex<HashMap<u32, usize>>> = OnceLock::new();

fn toast_tuple_targets() -> &'static Mutex<HashMap<u32, usize>> {
    TOAST_TUPLE_TARGETS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_toast_tuple_target_for_toast_relation(toast_oid: u32, target: usize) {
    toast_tuple_targets().lock().insert(toast_oid, target);
}

pub fn toast_tuple_target(toast_oid: u32) -> usize {
    toast_tuple_targets()
        .lock()
        .get(&toast_oid)
        .copied()
        .unwrap_or(TOAST_TUPLE_TARGET)
}

fn build_tuple(desc: &RelationDesc, values: &[TupleValue]) -> AccessResult<HeapTuple> {
    HeapTuple::from_values(&desc.attribute_descs(), values)
        .map_err(|err| AccessError::Scalar(format!("toast tuple build failed: {err:?}")))
}

fn maybe_compress_column(
    desc: &RelationDesc,
    values: &mut [TupleValue],
    column_index: usize,
    default_toast_compression: crate::access::htup::AttributeCompression,
    ttc: &mut ToastTupleContext,
) -> AccessResult<bool> {
    let column = &desc.columns[column_index];
    if column.storage.attlen != -1 {
        return Ok(false);
    }
    if !matches!(
        column.storage.attstorage,
        AttributeStorage::Extended | AttributeStorage::Main
    ) {
        return Ok(false);
    }

    let TupleValue::Bytes(bytes) = &values[column_index] else {
        return Ok(false);
    };
    let Some(compressed) = compress_inline_datum(
        bytes,
        column.storage.attcompression,
        default_toast_compression,
    )?
    else {
        ttc.attr[column_index].colflags |= TOASTCOL_INCOMPRESSIBLE;
        return Ok(false);
    };

    ttc.attr[column_index].compression = compressed.method;
    ttc.flags |= TOAST_NEEDS_CHANGE;
    values[column_index] = TupleValue::EncodedVarlena(compressed.encoded);
    Ok(true)
}

fn external_input(value: &TupleValue) -> Option<ExternalToastValueInput> {
    match value {
        TupleValue::Bytes(bytes) => Some(ExternalToastValueInput {
            data: bytes.clone(),
            rawsize: i32::try_from(bytes.len().saturating_add(VARHDRSZ)).unwrap_or(i32::MAX),
            compression_id: ToastCompressionId::Invalid,
        }),
        TupleValue::EncodedVarlena(bytes) if is_ondisk_toast_pointer(bytes) => None,
        TupleValue::EncodedVarlena(bytes) if is_compressed_inline_datum(bytes) => {
            let method = compressed_inline_compression_method(bytes)
                .and_then(ToastCompressionId::from_u32)
                .unwrap_or(ToastCompressionId::Invalid);
            Some(ExternalToastValueInput {
                data: bytes[VARHDRSZ..].to_vec(),
                rawsize: i32::try_from(
                    compressed_inline_extsize(bytes)
                        .unwrap_or_default()
                        .saturating_add(VARHDRSZ as u32),
                )
                .unwrap_or(i32::MAX),
                compression_id: method,
            })
        }
        _ => None,
    }
}

fn externalize_largest_column(
    desc: &RelationDesc,
    values: &mut [TupleValue],
    allowed_storage: &[AttributeStorage],
    store_external_value: &mut dyn FnMut(ExternalToastValueInput) -> AccessResult<StoredToastValue>,
) -> AccessResult<Option<StoredToastValue>> {
    let candidate = desc
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.storage.attlen == -1)
        .filter(|(_, column)| allowed_storage.contains(&column.storage.attstorage))
        .filter_map(|(index, _)| {
            external_input(&values[index]).map(|input| (index, input.data.len(), input))
        })
        .max_by_key(|(_, len, _)| *len);

    let Some((index, _, input)) = candidate else {
        return Ok(None);
    };

    let stored = store_external_value(input)?;
    values[index] = TupleValue::EncodedVarlena(encoded_pointer_bytes(stored.pointer));
    Ok(Some(stored))
}

pub fn toast_tuple_values_for_write_with_store(
    desc: &RelationDesc,
    values: &mut [TupleValue],
    toast_relation_oid: u32,
    default_toast_compression: crate::access::htup::AttributeCompression,
    store_external_value: &mut dyn FnMut(ExternalToastValueInput) -> AccessResult<StoredToastValue>,
) -> AccessResult<Vec<StoredToastValue>> {
    let mut ttc = ToastTupleContext {
        attr: vec![ToastAttrInfo::default(); desc.columns.len()],
        ..ToastTupleContext::default()
    };
    let mut stored = Vec::new();
    let mut tuple = build_tuple(desc, values)?;
    let target = toast_tuple_target(toast_relation_oid);
    if tuple.serialized_len() <= target {
        return Ok(stored);
    }

    for index in 0..desc.columns.len() {
        let changed =
            maybe_compress_column(desc, values, index, default_toast_compression, &mut ttc)?;
        if changed {
            tuple = build_tuple(desc, values)?;
            if tuple.serialized_len() <= target {
                return Ok(stored);
            }
        }
    }

    while tuple.serialized_len() > target {
        let Some(toasted) = externalize_largest_column(
            desc,
            values,
            &[AttributeStorage::Extended, AttributeStorage::External],
            store_external_value,
        )?
        else {
            break;
        };
        stored.push(toasted);
        tuple = build_tuple(desc, values)?;
    }

    if tuple.serialized_len() <= MAX_HEAP_TUPLE_SIZE {
        return Ok(stored);
    }

    while tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE {
        let Some(toasted) = externalize_largest_column(
            desc,
            values,
            &[AttributeStorage::Main],
            store_external_value,
        )?
        else {
            break;
        };
        stored.push(toasted);
        tuple = build_tuple(desc, values)?;
    }

    Ok(stored)
}
