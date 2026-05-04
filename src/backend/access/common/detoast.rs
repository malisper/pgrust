// :HACK: Compatibility adapter while portable detoast reconstruction lives in
// `pgrust_access`. Root still owns heap scanning and executor error mapping.
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::executor::ExecError;
use crate::include::nodes::execnodes::ToastFetchContext;

pub(crate) fn detoast_value_bytes(
    toast: &ToastFetchContext,
    bytes: &[u8],
) -> Result<Vec<u8>, ExecError> {
    pgrust_access::common::detoast::detoast_value_bytes_with_fetch(bytes, |pointer| {
        fetch_toast_chunks(toast, pointer)
    })
    .map_err(access_error_to_exec)
}

fn fetch_toast_chunks(
    toast: &ToastFetchContext,
    pointer: pgrust_access::varatt::VarattExternal,
) -> pgrust_access::AccessResult<Vec<pgrust_access::access::heaptoast::ToastChunk>> {
    let toastrelid = pointer.va_toastrelid;
    let value_id = pointer.va_valueid;
    if toastrelid != toast.relation.relation_oid {
        return Err(pgrust_access::AccessError::Scalar(format!(
            "toast pointer relid {} does not match relation {}",
            toastrelid, toast.relation.relation_oid
        )));
    }

    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    let mut scan = heap_scan_begin_visible(
        &toast.pool,
        toast.client_id,
        toast.relation.rel,
        toast.snapshot.clone(),
    )
    .map_err(|err| pgrust_access::AccessError::Io(format!("{err:?}")))?;
    let mut chunks = Vec::new();
    {
        let txns = toast.txns.read();
        while let Some((_tid, tuple)) =
            heap_scan_next_visible(&toast.pool, toast.client_id, &txns, &mut scan)
                .map_err(|err| pgrust_access::AccessError::Io(format!("{err:?}")))?
        {
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
