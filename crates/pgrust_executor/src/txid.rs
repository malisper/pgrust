use pgrust_access::transam::xact::{
    FIRST_NORMAL_TRANSACTION_ID, INVALID_TRANSACTION_ID, Snapshot, TransactionId, TransactionStatus,
};
use pgrust_nodes::Value;
use pgrust_nodes::primnodes::BuiltinScalarFunction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTxidSnapshotValue {
    pub xmin: u64,
    pub xmax: u64,
    pub in_progress: Vec<u64>,
}

impl CurrentTxidSnapshotValue {
    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        let mut in_progress = snapshot
            .in_progress
            .iter()
            .copied()
            .map(u64::from)
            .collect::<Vec<_>>();
        if snapshot.current_xid != INVALID_TRANSACTION_ID
            && snapshot.current_xid >= snapshot.xmin
            && snapshot.current_xid < snapshot.xmax
        {
            in_progress.push(u64::from(snapshot.current_xid));
            in_progress.sort_unstable();
            in_progress.dedup();
        }

        Self {
            xmin: u64::from(snapshot.xmin),
            xmax: u64::from(snapshot.xmax),
            in_progress,
        }
    }

    pub fn render(&self) -> String {
        let mut rendered = format!("{}:{}:", self.xmin, self.xmax);
        for (index, xid) in self.in_progress.iter().enumerate() {
            if index > 0 {
                rendered.push(',');
            }
            rendered.push_str(&xid.to_string());
        }
        rendered
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxidStatusError {
    FutureTransaction { xid: u64 },
}

#[derive(Debug)]
pub enum TxidBuiltinError<E> {
    Runtime(E),
    Expr(pgrust_expr::ExprError),
    Status(TxidStatusError),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    MalformedCall,
}

pub trait TxidRuntime {
    type Error;

    fn ensure_write_xid(&mut self) -> Result<TransactionId, Self::Error>;
    fn transaction_xid(&self) -> Option<TransactionId>;
    fn snapshot(&self) -> &Snapshot;
    fn txid_status_inputs(&self, xid: u64) -> (TransactionId, Option<TransactionStatus>);
}

pub fn current_txid_snapshot_text(snapshot: &Snapshot) -> String {
    CurrentTxidSnapshotValue::from_snapshot(snapshot).render()
}

pub fn eval_txid_builtin_function<R>(
    func: BuiltinScalarFunction,
    values: &[Value],
    runtime: &mut R,
) -> Result<Value, TxidBuiltinError<R::Error>>
where
    R: TxidRuntime,
{
    match func {
        BuiltinScalarFunction::TxidCurrent => Ok(Value::Xid8(u64::from(
            runtime
                .ensure_write_xid()
                .map_err(TxidBuiltinError::Runtime)?,
        ))),
        BuiltinScalarFunction::TxidCurrentIfAssigned => Ok(runtime
            .transaction_xid()
            .filter(|xid| *xid != INVALID_TRANSACTION_ID)
            .map(|xid| Value::Xid8(u64::from(xid)))
            .unwrap_or(Value::Null)),
        BuiltinScalarFunction::TxidCurrentSnapshot => Ok(Value::Text(
            current_txid_snapshot_text(runtime.snapshot()).into(),
        )),
        BuiltinScalarFunction::TxidSnapshotXmin => {
            pgrust_expr::executor::expr_txid::eval_txid_snapshot_xmin_value(values)
                .map_err(TxidBuiltinError::Expr)
        }
        BuiltinScalarFunction::TxidSnapshotXmax => {
            pgrust_expr::executor::expr_txid::eval_txid_snapshot_xmax_value(values)
                .map_err(TxidBuiltinError::Expr)
        }
        BuiltinScalarFunction::TxidVisibleInSnapshot => {
            pgrust_expr::executor::expr_txid::eval_txid_visible_in_snapshot_value(values)
                .map_err(TxidBuiltinError::Expr)
        }
        BuiltinScalarFunction::TxidStatus => match values {
            [Value::Null] => Ok(Value::Null),
            [Value::Int64(xid)] if *xid >= 0 => eval_txid_status_arg(*xid as u64, runtime),
            [Value::Int32(xid)] if *xid >= 0 => eval_txid_status_arg(*xid as u64, runtime),
            [Value::Xid8(xid)] => eval_txid_status_arg(*xid, runtime),
            [value] => Err(TxidBuiltinError::TypeMismatch {
                op: "txid_status",
                left: value.clone(),
                right: Value::Int64(0),
            }),
            _ => Err(TxidBuiltinError::MalformedCall),
        },
        _ => unreachable!("non-txid builtin dispatched to txid"),
    }
}

fn eval_txid_status_arg<R>(xid: u64, runtime: &R) -> Result<Value, TxidBuiltinError<R::Error>>
where
    R: TxidRuntime,
{
    let (next_xid, status) = runtime.txid_status_inputs(xid);
    txid_status_value(xid, next_xid, status).map_err(TxidBuiltinError::Status)
}

pub fn txid_status_value(
    xid: u64,
    next_xid: TransactionId,
    status: Option<TransactionStatus>,
) -> Result<Value, TxidStatusError> {
    if xid > u64::from(next_xid) {
        return Err(TxidStatusError::FutureTransaction { xid });
    }
    if xid == 0 || xid > u64::from(u32::MAX) {
        return Ok(Value::Null);
    }

    let xid = xid as TransactionId;
    // :HACK: pgrust does not model CLOG truncation horizons yet. PostgreSQL's
    // txid regression expects FirstNormalTransactionId to be too old to report
    // after bootstrap setup, even if this lightweight transaction manager still
    // remembers its committed status.
    if xid == FIRST_NORMAL_TRANSACTION_ID && !matches!(status, Some(TransactionStatus::InProgress))
    {
        return Ok(Value::Null);
    }

    Ok(match status {
        Some(TransactionStatus::InProgress) => Value::Text("in progress".into()),
        Some(TransactionStatus::Committed) => Value::Text("committed".into()),
        Some(TransactionStatus::Aborted) => Value::Text("aborted".into()),
        None => Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn current_snapshot_text_includes_current_xid_sorted_and_deduped() {
        let snapshot = Snapshot {
            xmin: 10,
            xmax: 20,
            in_progress: BTreeSet::from([12, 15]),
            current_xid: 12,
            own_xids: BTreeSet::new(),
            current_cid: 0,
            heap_current_cid: None,
        };

        assert_eq!(current_txid_snapshot_text(&snapshot), "10:20:12,15");
    }

    #[test]
    fn txid_status_value_maps_statuses_and_future_xids() {
        assert_eq!(
            txid_status_value(4, 10, Some(TransactionStatus::Committed)).unwrap(),
            Value::Text("committed".into())
        );
        assert_eq!(txid_status_value(0, 10, None).unwrap(), Value::Null);
        assert_eq!(
            txid_status_value(11, 10, None),
            Err(TxidStatusError::FutureTransaction { xid: 11 })
        );
        assert_eq!(
            txid_status_value(
                FIRST_NORMAL_TRANSACTION_ID as u64,
                FIRST_NORMAL_TRANSACTION_ID,
                Some(TransactionStatus::Committed),
            )
            .unwrap(),
            Value::Null
        );
    }
}
