// :HACK: Compatibility adapter while portable detoast reconstruction lives in
// `pgrust_access`. Root still owns heap scanning and executor error mapping.
use crate::backend::access::heap::heapam::{heap_scan_begin, heap_scan_next};
use crate::backend::access::heap::heapam_visibility::SnapshotVisibility;
use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::executor::ExecError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::include::nodes::primnodes::ToastRelationRef;
use crate::{BufferPool, ClientId};
use pgrust_core::Snapshot;

pub(crate) fn detoast_value_bytes(
    toast: &ToastFetchContext,
    bytes: &[u8],
) -> Result<Vec<u8>, ExecError> {
    pgrust_access::common::detoast::detoast_value_bytes_with_fetch(bytes, |pointer| {
        fetch_toast_chunks(toast, pointer)
    })
    .map_err(access_error_to_exec)
}

pub(crate) fn detoast_value_bytes_from_parts(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: ClientId,
    relation: ToastRelationRef,
    bytes: &[u8],
) -> Result<Vec<u8>, ExecError> {
    pgrust_access::common::detoast::detoast_value_bytes_with_fetch(bytes, |pointer| {
        fetch_toast_chunks_from_parts(pool, txns, snapshot, client_id, relation, pointer)
    })
    .map_err(access_error_to_exec)
}

fn fetch_toast_chunks(
    toast: &ToastFetchContext,
    pointer: pgrust_access::varatt::VarattExternal,
) -> pgrust_access::AccessResult<Vec<pgrust_access::access::heaptoast::ToastChunk>> {
    let toastrelid = pointer.va_toastrelid;
    if toastrelid != toast.relation.relation_oid {
        return Err(pgrust_access::AccessError::Scalar(format!(
            "toast pointer relid {} does not match relation {}",
            toastrelid, toast.relation.relation_oid
        )));
    }

    let txns = toast.txns.read();
    fetch_toast_chunks_from_parts(
        &toast.pool,
        &txns,
        &toast.snapshot,
        toast.client_id,
        toast.relation,
        pointer,
    )
}

fn fetch_toast_chunks_from_parts(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: ClientId,
    relation: ToastRelationRef,
    pointer: pgrust_access::varatt::VarattExternal,
) -> pgrust_access::AccessResult<Vec<pgrust_access::access::heaptoast::ToastChunk>> {
    let toastrelid = pointer.va_toastrelid;
    let value_id = pointer.va_valueid;
    if toastrelid != relation.relation_oid {
        return Err(pgrust_access::AccessError::Scalar(format!(
            "toast pointer relid {} does not match relation {}",
            toastrelid, relation.relation_oid
        )));
    }

    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    let mut scan = heap_scan_begin(pool, relation.rel)
        .map_err(|err| pgrust_access::AccessError::Io(format!("{err:?}")))?;
    let mut chunks = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
        .map_err(|err| pgrust_access::AccessError::Io(format!("{err:?}")))?
    {
        if !snapshot.tuple_visible(txns, &tuple) {
            continue;
        }
        let values = tuple
            .deform(&attr_descs)
            .map_err(|err| pgrust_access::AccessError::Scalar(format!("{err:?}")))?;
        let Some(chunk) = pgrust_access::access::heaptoast::toast_chunk_from_values(&values)?
        else {
            continue;
        };
        if chunk.id == value_id {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

fn access_error_to_exec(error: pgrust_access::AccessError) -> ExecError {
    match error {
        pgrust_access::AccessError::Corrupt(message) => ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: message.into(),
        },
        pgrust_access::AccessError::Scalar(message)
            if message.starts_with("toast ")
                || message.starts_with("invalid ")
                || message.contains("reconstructed") =>
        {
            ExecError::InvalidStorageValue {
                column: "<toast>".into(),
                details: message,
            }
        }
        pgrust_access::AccessError::Interrupted(reason) => ExecError::Interrupted(reason),
        pgrust_access::AccessError::Io(message)
        | pgrust_access::AccessError::Scalar(message)
        | pgrust_access::AccessError::UniqueViolation(message)
        | pgrust_access::AccessError::Unsupported(message) => ExecError::DetailedError {
            message,
            detail: None,
            hint: None,
            sqlstate: "XX000",
        },
    }
}
