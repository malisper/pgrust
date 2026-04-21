pub use crate::include::access::toast_helper::*;

use crate::backend::access::common::toast_compression::compress_inline_datum;
use crate::backend::access::heap::heaptoast::{
    ExternalToastValueInput, StoredToastValue, encoded_pointer_bytes, store_external_value,
};
use crate::backend::executor::{ExecError, ExecutorContext, RelationDesc};
use crate::backend::parser::BoundIndexRelation;
use crate::backend::storage::page::bufpage::MAX_HEAP_TUPLE_SIZE;
use crate::include::access::htup::{AttributeStorage, HeapTuple, TupleValue};
use crate::include::access::toast_compression::ToastCompressionId;
use crate::include::nodes::primnodes::ToastRelationRef;
use crate::include::varatt::{
    VARHDRSZ, compressed_inline_compression_method, compressed_inline_extsize,
    is_compressed_inline_datum, is_ondisk_toast_pointer,
};
use std::cmp::Reverse;

fn build_tuple(desc: &RelationDesc, values: &[TupleValue]) -> Result<HeapTuple, ExecError> {
    HeapTuple::from_values(&desc.attribute_descs(), values).map_err(ExecError::from)
}

fn maybe_compress_column(
    desc: &RelationDesc,
    values: &mut [TupleValue],
    column_index: usize,
    ctx: &ExecutorContext,
    ttc: &mut ToastTupleContext,
) -> Result<bool, ExecError> {
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
        ctx.default_toast_compression,
    )? else {
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
    ctx: &mut ExecutorContext,
    toast: ToastRelationRef,
    toast_index: Option<&BoundIndexRelation>,
    xid: crate::backend::access::transam::xact::TransactionId,
    cid: crate::backend::access::transam::xact::CommandId,
    allowed_storage: &[AttributeStorage],
) -> Result<Option<StoredToastValue>, ExecError> {
    let candidate = desc
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.storage.attlen == -1)
        .filter(|(_, column)| allowed_storage.contains(&column.storage.attstorage))
        .filter_map(|(index, _)| {
            external_input(&values[index]).map(|input| (index, Reverse(input.data.len()), input))
        })
        .max_by_key(|(_, len, _)| *len);

    let Some((index, _, input)) = candidate else {
        return Ok(None);
    };

    let stored = store_external_value(ctx, toast, toast_index, &input, xid, cid)?;
    values[index] = TupleValue::EncodedVarlena(encoded_pointer_bytes(stored.pointer));
    Ok(Some(stored))
}

pub(crate) fn toast_tuple_values_for_write(
    desc: &RelationDesc,
    values: &mut [TupleValue],
    toast: ToastRelationRef,
    toast_index: Option<&BoundIndexRelation>,
    ctx: &mut ExecutorContext,
    xid: crate::backend::access::transam::xact::TransactionId,
    cid: crate::backend::access::transam::xact::CommandId,
) -> Result<Vec<StoredToastValue>, ExecError> {
    let mut ttc = ToastTupleContext {
        attr: vec![ToastAttrInfo::default(); desc.columns.len()],
        ..ToastTupleContext::default()
    };
    let mut stored = Vec::new();
    let mut tuple = build_tuple(desc, values)?;
    if tuple.serialized_len() <= MAX_HEAP_TUPLE_SIZE {
        return Ok(stored);
    }

    for index in 0..desc.columns.len() {
        let changed = maybe_compress_column(desc, values, index, ctx, &mut ttc)?;
        if changed {
            tuple = build_tuple(desc, values)?;
            if tuple.serialized_len() <= MAX_HEAP_TUPLE_SIZE {
                return Ok(stored);
            }
        }
    }

    while tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE {
        let Some(toasted) = externalize_largest_column(
            desc,
            values,
            ctx,
            toast,
            toast_index,
            xid,
            cid,
            &[AttributeStorage::Extended, AttributeStorage::External],
        )? else {
            break;
        };
        stored.push(toasted);
        tuple = build_tuple(desc, values)?;
    }

    while tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE {
        let Some(toasted) = externalize_largest_column(
            desc,
            values,
            ctx,
            toast,
            toast_index,
            xid,
            cid,
            &[AttributeStorage::Main],
        )? else {
            break;
        };
        stored.push(toasted);
        tuple = build_tuple(desc, values)?;
    }

    Ok(stored)
}
