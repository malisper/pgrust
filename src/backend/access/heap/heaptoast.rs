// :HACK: Compatibility adapter while portable TOAST tuple/pointer helpers live
// in `pgrust_access`. Root still owns executor/index orchestration for TOAST
// heap storage.
pub use crate::include::access::heaptoast::*;

use crate::backend::access::heap::heapam::{
    HeapError, heap_delete_with_waiter, heap_insert_mvcc_with_cid, heap_scan_begin, heap_scan_next,
};
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::executor::{ExecError, ExecutorContext, RelationDesc};
use crate::backend::parser::BoundIndexRelation;
use crate::include::access::htup::{HeapTuple, ItemPointerData};
use crate::include::nodes::primnodes::ToastRelationRef;
use pgrust_access::access::toast_compression::ToastCompressionId;
use pgrust_access::varatt::{VarattExternal, varatt_external_set_size_and_compression_method};

fn next_toast_value_id(ctx: &ExecutorContext, toast: ToastRelationRef) -> Result<u32, ExecError> {
    let mut scan = heap_scan_begin(&ctx.pool, toast.rel)?;
    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    let mut max_value_id = 0u32;
    while let Some((_tid, tuple)) = heap_scan_next(&ctx.pool, ctx.client_id, &mut scan)? {
        let values = tuple.deform(&attr_descs)?;
        let Some(chunk_id) = pgrust_access::access::heaptoast::toast_chunk_id_from_values(&values)
            .map_err(access_error_to_exec)?
        else {
            continue;
        };
        max_value_id = max_value_id.max(chunk_id);
    }
    Ok(max_value_id.saturating_add(1))
}

pub(crate) fn store_external_value(
    ctx: &mut ExecutorContext,
    toast: ToastRelationRef,
    toast_index: Option<&BoundIndexRelation>,
    value: &ExternalToastValueInput,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StoredToastValue, ExecError> {
    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let value_id = next_toast_value_id(ctx, toast)?;
    let mut chunk_tids = Vec::new();

    for (chunk_seq, chunk) in value.data.chunks(TOAST_MAX_CHUNK_SIZE).enumerate() {
        let row = pgrust_access::access::heaptoast::toast_chunk_row_values(
            value_id,
            chunk_seq as i32,
            chunk,
        );
        let tuple = tuple_from_values(&desc, &row)?;
        let tid = heap_insert_mvcc_with_cid(&ctx.pool, ctx.client_id, toast.rel, xid, cid, &tuple)?;
        if let Some(index) = toast_index
            && index.index_meta.indisvalid
            && index.index_meta.indisready
        {
            indexam::index_insert_stub(
                &crate::include::access::amapi::IndexInsertContext {
                    pool: ctx.pool.clone(),
                    txns: ctx.txns.clone(),
                    txn_waiter: ctx.txn_waiter.clone(),
                    client_id: ctx.client_id,
                    interrupts: ctx.interrupts.clone(),
                    snapshot: ctx.snapshot.clone(),
                    heap_relation: toast.rel,
                    heap_desc: desc.clone(),
                    index_relation: index.rel,
                    index_name: index.name.clone(),
                    index_desc: index.desc.clone(),
                    index_meta: index.index_meta.clone(),
                    default_toast_compression: ctx.default_toast_compression,
                    heap_tid: tid,
                    old_heap_tid: None,
                    values: row,
                    unique_check: if index.index_meta.indisunique {
                        crate::include::access::amapi::IndexUniqueCheck::Yes
                    } else {
                        crate::include::access::amapi::IndexUniqueCheck::No
                    },
                },
                index.index_meta.am_oid,
            )?;
        }
        chunk_tids.push(tid);
    }

    Ok(StoredToastValue {
        pointer: VarattExternal {
            va_rawsize: value.rawsize,
            va_extinfo: if value.compression_id == ToastCompressionId::Invalid {
                value.data.len() as u32
            } else {
                varatt_external_set_size_and_compression_method(
                    value.data.len() as u32,
                    value.compression_id as u32,
                )
            },
            va_valueid: value_id,
            va_toastrelid: toast.relation_oid,
        },
        chunk_tids,
    })
}

pub(crate) fn cleanup_new_toast_value(
    ctx: &ExecutorContext,
    toast: ToastRelationRef,
    chunk_tids: &[ItemPointerData],
    xid: TransactionId,
) -> Result<(), ExecError> {
    for tid in chunk_tids {
        match heap_delete_with_waiter(
            &ctx.pool,
            ctx.client_id,
            toast.rel,
            &ctx.txns,
            xid,
            *tid,
            &ctx.snapshot,
            ctx.txn_waiter
                .as_deref()
                .map(|waiter| (&*ctx.txns, waiter, ctx.interrupts.as_ref())),
        ) {
            Ok(()) | Err(HeapError::TupleAlreadyModified(_)) => {}
            Err(HeapError::TupleUpdated(_, _)) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

pub(crate) fn delete_external_value(
    ctx: &ExecutorContext,
    toast: ToastRelationRef,
    value_id: u32,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let mut scan = heap_scan_begin(&ctx.pool, toast.rel)?;
    let desc = pgrust_access::access::heaptoast::toast_relation_desc();
    let attr_descs = desc.attribute_descs();
    let mut tids = Vec::new();
    while let Some((tid, tuple)) = heap_scan_next(&ctx.pool, ctx.client_id, &mut scan)? {
        let values = tuple.deform(&attr_descs)?;
        let Some(chunk_id) = pgrust_access::access::heaptoast::toast_chunk_id_from_values(&values)
            .map_err(access_error_to_exec)?
        else {
            continue;
        };
        if chunk_id == value_id {
            tids.push(tid);
        }
    }
    cleanup_new_toast_value(ctx, toast, &tids, xid)
}

pub(crate) fn delete_external_from_tuple(
    ctx: &ExecutorContext,
    toast: ToastRelationRef,
    desc: &RelationDesc,
    tuple: &HeapTuple,
    xid: TransactionId,
) -> Result<(), ExecError> {
    for pointer in extract_external_pointers_from_tuple(desc, tuple)? {
        delete_external_value(ctx, toast, pointer.va_valueid, xid)?;
    }
    Ok(())
}

fn extract_external_pointers_from_tuple(
    desc: &RelationDesc,
    tuple: &HeapTuple,
) -> Result<Vec<pgrust_access::varatt::VarattExternal>, ExecError> {
    pgrust_access::access::heaptoast::extract_external_pointers(desc, tuple)
        .map_err(access_error_to_exec)
}

fn access_error_to_exec(error: pgrust_access::AccessError) -> ExecError {
    match error {
        pgrust_access::AccessError::Corrupt(message) => ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: message.into(),
        },
        pgrust_access::AccessError::Scalar(message)
            if message.starts_with("toast ") || message.contains("deform") =>
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
