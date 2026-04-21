use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::common::toast_compression::decompress_external_payload;
use crate::backend::executor::ExecError;
use crate::include::access::detoast::{
    decode_ondisk_toast_pointer, varatt_external_get_compression_method,
    varatt_external_is_compressed,
};
use crate::include::varatt::VARHDRSZ;
use crate::include::nodes::execnodes::ToastFetchContext;

fn toast_chunk_desc() -> crate::backend::executor::RelationDesc {
    crate::backend::executor::RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "chunk_id",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Oid),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "chunk_seq",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "chunk_data",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Bytea),
                false,
            ),
        ],
    }
}

pub(crate) fn detoast_value_bytes(
    toast: &ToastFetchContext,
    bytes: &[u8],
) -> Result<Vec<u8>, ExecError> {
    let pointer =
        decode_ondisk_toast_pointer(bytes).ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: "invalid on-disk toast pointer".into(),
        })?;
    let toastrelid = pointer.va_toastrelid;
    let value_id = pointer.va_valueid;
    if toastrelid != toast.relation.relation_oid {
        return Err(ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: format!(
                "toast pointer relid {} does not match relation {}",
                toastrelid, toast.relation.relation_oid
            ),
        });
    }

    let desc = toast_chunk_desc();
    let attr_descs = desc.attribute_descs();
    let mut scan = heap_scan_begin_visible(
        &toast.pool,
        toast.client_id,
        toast.relation.rel,
        toast.snapshot.clone(),
    )?;
    let mut chunks = Vec::new();
    {
        let txns = toast.txns.read();
        while let Some((_tid, tuple)) =
            heap_scan_next_visible(&toast.pool, toast.client_id, &txns, &mut scan)?
        {
            let values = tuple.deform(&attr_descs)?;
            let Some(chunk_id_bytes) = values.first().and_then(|value| *value) else {
                continue;
            };
            let chunk_id = u32::from_le_bytes(chunk_id_bytes.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "chunk_id".into(),
                    details: "toast chunk_id must be 4 bytes".into(),
                }
            })?);
            if chunk_id != value_id {
                continue;
            }

            let Some(chunk_seq_bytes) = values.get(1).and_then(|value| *value) else {
                continue;
            };
            let chunk_seq = i32::from_le_bytes(chunk_seq_bytes.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "chunk_seq".into(),
                    details: "toast chunk_seq must be 4 bytes".into(),
                }
            })?);
            let chunk_data = values.get(2).and_then(|value| *value).ok_or_else(|| {
                ExecError::InvalidStorageValue {
                    column: "chunk_data".into(),
                    details: "toast chunk missing data".into(),
                }
            })?;
            chunks.push((chunk_seq, chunk_data.to_vec()));
        }
    }
    chunks.sort_by_key(|(seq, _)| *seq);
    let mut data = Vec::new();
    for (_, chunk) in chunks {
        data.extend_from_slice(&chunk);
    }

    let expected = crate::include::varatt::varatt_external_get_extsize(pointer) as usize;
    if data.len() != expected {
        return Err(ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: format!(
                "toast value {} reconstructed to {} bytes, expected {}",
                value_id,
                data.len(),
                expected
            ),
        });
    }
    if varatt_external_is_compressed(pointer) {
        let rawsize = usize::try_from(pointer.va_rawsize)
            .unwrap_or_default()
            .saturating_sub(VARHDRSZ);
        decompress_external_payload(
            &data,
            rawsize,
            varatt_external_get_compression_method(pointer),
        )
    } else {
        Ok(data)
    }
}
