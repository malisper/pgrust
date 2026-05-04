pub use pgrust_catalog_store::rowcodec::*;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{heap_scan_begin, heap_scan_next};
use crate::backend::access::heap::heapam_visibility::SnapshotVisibility;
use crate::backend::access::transam::xact::{Snapshot, TransactionManager};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::bootstrap::bootstrap_catalog_toast_rel;
use crate::backend::executor::value_io::{
    decode_value, decode_value_with_external_toast, missing_column_value,
};
use crate::backend::executor::{ExecError, RelationDesc};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::StorageManager;
use crate::include::catalog::BootstrapCatalogKind;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{ColumnDesc, ToastRelationRef};

// :HACK: `HeapTuple` is still a root storage/access type. Keep only this
// root bridge here while the portable row codecs live in `pgrust_catalog_store`.
pub(crate) fn decode_catalog_tuple_values(
    desc: &RelationDesc,
    tuple: &crate::include::access::htup::HeapTuple,
) -> Result<Vec<Value>, CatalogError> {
    let raw = tuple
        .deform(&desc.attribute_descs())
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    pgrust_catalog_store::rowcodec::decode_catalog_tuple_values_from_raw(desc, &raw)
}

pub(crate) fn decode_catalog_tuple_values_with_toast(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    kind: BootstrapCatalogKind,
    db_oid: u32,
    desc: &RelationDesc,
    tuple: &crate::include::access::htup::HeapTuple,
) -> Result<Vec<Value>, CatalogError> {
    let raw = tuple
        .deform(&desc.attribute_descs())
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    let toast = bootstrap_catalog_toast_rel(kind, db_oid).map(|rel| ToastRelationRef {
        rel,
        relation_oid: kind.toast_relation_oid(),
    });
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if let Some(datum) = raw.get(index) {
                decode_catalog_value(pool, txns, snapshot, client_id, toast, column, *datum)
            } else {
                Ok(missing_column_value(column))
            }
        })
        .collect()
}

fn decode_catalog_value(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    toast: Option<ToastRelationRef>,
    column: &ColumnDesc,
    raw: Option<&[u8]>,
) -> Result<Value, CatalogError> {
    let Some(toast) = toast else {
        return decode_value(column, raw)
            .map_err(|e| CatalogError::Io(format!("catalog decode failed: {e:?}")));
    };
    let mut fetch_external =
        |bytes: &[u8]| detoast_catalog_value_bytes(pool, txns, snapshot, client_id, toast, bytes);
    decode_value_with_external_toast(column, raw, Some(&mut fetch_external))
        .map_err(|e| CatalogError::Io(format!("catalog decode failed: {e:?}")))
}

fn detoast_catalog_value_bytes(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    toast: ToastRelationRef,
    bytes: &[u8],
) -> Result<Vec<u8>, ExecError> {
    pgrust_access::common::detoast::detoast_value_bytes_with_fetch(bytes, |pointer| {
        fetch_catalog_toast_chunks(pool, txns, snapshot, client_id, toast, pointer)
    })
    .map_err(|e| ExecError::DetailedError {
        message: e.to_string(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

fn fetch_catalog_toast_chunks(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    toast: ToastRelationRef,
    pointer: pgrust_access::varatt::VarattExternal,
) -> pgrust_access::AccessResult<Vec<pgrust_access::access::heaptoast::ToastChunk>> {
    let pointer_toastrelid = pointer.va_toastrelid;
    let pointer_valueid = pointer.va_valueid;
    if pointer_toastrelid != toast.relation_oid {
        return Err(pgrust_access::AccessError::Scalar(format!(
            "toast pointer relid {} does not match relation {}",
            pointer_toastrelid, toast.relation_oid
        )));
    }

    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    pool.with_storage_mut(|storage| storage.smgr.open(toast.rel))
        .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))?;
    let mut scan = heap_scan_begin(pool, toast.rel)
        .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))?;
    let mut chunks = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
        .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))?
    {
        if !snapshot.tuple_visible(txns, &tuple) {
            continue;
        }
        let values = tuple
            .deform(&attr_descs)
            .map_err(|e| pgrust_access::AccessError::Scalar(format!("{e:?}")))?;
        let Some(chunk) = pgrust_access::access::heaptoast::toast_chunk_from_values(&values)?
        else {
            continue;
        };
        if chunk.id == pointer_valueid {
            chunks.push(chunk);
        }
    }
    if chunks.is_empty() {
        // :HACK: Catalog toast chunks are owned by the visible parent catalog
        // row. During startup, chunk visibility can lag parent visibility, so
        // fall back to a raw scan before treating the external pointer as bad.
        let mut scan = heap_scan_begin(pool, toast.rel)
            .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))?;
        while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
            .map_err(|e| pgrust_access::AccessError::Io(format!("{e:?}")))?
        {
            let values = tuple
                .deform(&attr_descs)
                .map_err(|e| pgrust_access::AccessError::Scalar(format!("{e:?}")))?;
            let Some(chunk) = pgrust_access::access::heaptoast::toast_chunk_from_values(&values)?
            else {
                continue;
            };
            if chunk.id == pointer_valueid {
                chunks.push(chunk);
            }
        }
    }
    Ok(chunks)
}
