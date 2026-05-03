// :HACK: Compatibility shim while portable TOAST tuple planning lives in
// `pgrust_access`. Root keeps executor/index orchestration for external chunk
// storage.
pub use crate::include::access::toast_helper::*;

use crate::backend::access::heap::heaptoast::{StoredToastValue, store_external_value};
use crate::backend::executor::{ExecError, ExecutorContext, RelationDesc};
use crate::backend::parser::BoundIndexRelation;
use crate::include::access::htup::TupleValue;
use crate::include::nodes::primnodes::ToastRelationRef;

pub(crate) fn set_toast_tuple_target_for_toast_relation(toast_oid: u32, target: usize) {
    pgrust_access::table::toast_helper::set_toast_tuple_target_for_toast_relation(
        toast_oid, target,
    );
}

fn access_error_to_exec(error: pgrust_access::AccessError) -> ExecError {
    match error {
        pgrust_access::AccessError::Corrupt(message) => ExecError::InvalidStorageValue {
            column: "<toast>".into(),
            details: message.into(),
        },
        pgrust_access::AccessError::Io(message)
        | pgrust_access::AccessError::Scalar(message)
        | pgrust_access::AccessError::UniqueViolation(message)
        | pgrust_access::AccessError::Unsupported(message) => ExecError::DetailedError {
            message,
            detail: None,
            hint: None,
            sqlstate: "XX000",
        },
        pgrust_access::AccessError::Interrupted(reason) => ExecError::Interrupted(reason),
    }
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
    let default_toast_compression = ctx.default_toast_compression;
    pgrust_access::table::toast_helper::toast_tuple_values_for_write_with_store(
        desc,
        values,
        toast.relation_oid,
        default_toast_compression,
        &mut |input| {
            store_external_value(ctx, toast, toast_index, &input, xid, cid)
                .map_err(|err| pgrust_access::AccessError::Scalar(format!("{err:?}")))
        },
    )
    .map_err(access_error_to_exec)
}
