// :HACK: Keep transaction-context txid functions in root while pure txid snapshot
// parsing and visibility live in pgrust_expr.
use super::{ExecError, ExecutorContext, Value};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::include::nodes::primnodes::BuiltinScalarFunction;

fn map_expr_error(error: pgrust_expr::ExprError) -> ExecError {
    error.into()
}

pub(crate) fn is_txid_snapshot_type_oid(type_oid: u32) -> bool {
    pgrust_expr::backend::executor::expr_txid::is_txid_snapshot_type_oid(type_oid)
}

pub(crate) fn cast_text_to_txid_snapshot(text: &str) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_txid::cast_text_to_txid_snapshot(text)
        .map_err(map_expr_error)
}

pub(crate) fn eval_txid_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    pgrust_executor::eval_txid_builtin_function(func, values, ctx).map_err(txid_builtin_error)
}

pub(crate) fn eval_txid_snapshot_xip_values(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    pgrust_expr::backend::executor::expr_txid::eval_txid_snapshot_xip_values(values)
        .map_err(map_expr_error)
}

impl pgrust_executor::TxidRuntime for ExecutorContext {
    type Error = ExecError;

    fn ensure_write_xid(
        &mut self,
    ) -> Result<crate::backend::access::transam::xact::TransactionId, Self::Error> {
        ExecutorContext::ensure_write_xid(self)
    }

    fn transaction_xid(&self) -> Option<crate::backend::access::transam::xact::TransactionId> {
        ExecutorContext::transaction_xid(self)
    }

    fn snapshot(&self) -> &crate::backend::access::transam::xact::Snapshot {
        &self.snapshot
    }

    fn txid_status_inputs(
        &self,
        xid: u64,
    ) -> (
        crate::backend::access::transam::xact::TransactionId,
        Option<crate::backend::access::transam::xact::TransactionStatus>,
    ) {
        let txns = self.txns.read();
        let status = (xid <= u64::from(u32::MAX))
            .then(|| txns.status(xid as u32))
            .flatten();
        (txns.next_xid(), status)
    }
}

fn txid_builtin_error(error: pgrust_executor::TxidBuiltinError<ExecError>) -> ExecError {
    match error {
        pgrust_executor::TxidBuiltinError::Runtime(error) => error,
        pgrust_executor::TxidBuiltinError::Expr(error) => map_expr_error(error),
        pgrust_executor::TxidBuiltinError::Status(
            pgrust_executor::TxidStatusError::FutureTransaction { xid },
        ) => ExecError::DetailedError {
            message: format!("transaction ID {xid} is in the future"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        },
        pgrust_executor::TxidBuiltinError::TypeMismatch { op, left, right } => {
            ExecError::TypeMismatch { op, left, right }
        }
        pgrust_executor::TxidBuiltinError::MalformedCall => ExecError::DetailedError {
            message: "malformed txid builtin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txid_snapshot_input_canonicalizes_duplicates() {
        let value = cast_text_to_txid_snapshot("12:16:14,14").unwrap();
        assert_eq!(value, Value::Text("12:16:14".into()));
    }

    #[test]
    fn txid_snapshot_input_rejects_unsorted_or_out_of_range_xips() {
        assert!(cast_text_to_txid_snapshot("12:16:14,13").is_err());
        assert!(cast_text_to_txid_snapshot("12:13:0").is_err());
        assert!(cast_text_to_txid_snapshot("31:12:").is_err());
    }
}
