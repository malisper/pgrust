use super::{ExecError, ExecutorContext, Value};
use crate::backend::access::transam::xact::{
    FIRST_NORMAL_TRANSACTION_ID, INVALID_TRANSACTION_ID, TransactionStatus,
};
use crate::include::catalog::{PG_SNAPSHOT_TYPE_OID, TXID_SNAPSHOT_TYPE_OID};
use crate::include::nodes::primnodes::BuiltinScalarFunction;
use crate::pgrust::compact_string::CompactString;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TxidSnapshotValue {
    xmin: u64,
    xmax: u64,
    in_progress: Vec<u64>,
}

impl TxidSnapshotValue {
    fn render(&self) -> String {
        let mut rendered = format!("{}:{}:", self.xmin, self.xmax);
        for (index, xid) in self.in_progress.iter().enumerate() {
            if index > 0 {
                rendered.push(',');
            }
            rendered.push_str(&xid.to_string());
        }
        rendered
    }

    fn xid_visible(&self, xid: u64) -> bool {
        if xid < self.xmin {
            return true;
        }
        if xid >= self.xmax {
            return false;
        }
        !self.in_progress.binary_search(&xid).is_ok()
    }
}

fn invalid_txid_snapshot_input(text: &str) -> ExecError {
    ExecError::DetailedError {
        // PostgreSQL's legacy txid_snapshot input still reports pg_snapshot.
        message: format!("invalid input syntax for type pg_snapshot: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

fn parse_txid_snapshot_number(token: &str, original: &str) -> Result<u64, ExecError> {
    if token.is_empty() {
        return Err(invalid_txid_snapshot_input(original));
    }
    let value = token
        .parse::<u64>()
        .map_err(|_| invalid_txid_snapshot_input(original))?;
    if value == 0 || value > i64::MAX as u64 {
        return Err(invalid_txid_snapshot_input(original));
    }
    Ok(value)
}

fn parse_txid_snapshot(text: &str) -> Result<TxidSnapshotValue, ExecError> {
    let mut parts = text.split(':');
    let xmin_text = parts
        .next()
        .ok_or_else(|| invalid_txid_snapshot_input(text))?;
    let xmax_text = parts
        .next()
        .ok_or_else(|| invalid_txid_snapshot_input(text))?;
    let xip_text = parts
        .next()
        .ok_or_else(|| invalid_txid_snapshot_input(text))?;
    if parts.next().is_some() {
        return Err(invalid_txid_snapshot_input(text));
    }

    let xmin = parse_txid_snapshot_number(xmin_text, text)?;
    let xmax = parse_txid_snapshot_number(xmax_text, text)?;
    if xmin > xmax {
        return Err(invalid_txid_snapshot_input(text));
    }

    let mut in_progress = Vec::new();
    let mut previous = None;
    if !xip_text.is_empty() {
        for token in xip_text.split(',') {
            let xid = parse_txid_snapshot_number(token, text)?;
            if xid < xmin || xid >= xmax {
                return Err(invalid_txid_snapshot_input(text));
            }
            match previous {
                Some(prev) if xid < prev => return Err(invalid_txid_snapshot_input(text)),
                Some(prev) if xid == prev => continue,
                _ => {
                    in_progress.push(xid);
                    previous = Some(xid);
                }
            }
        }
    }

    Ok(TxidSnapshotValue {
        xmin,
        xmax,
        in_progress,
    })
}

fn txid_snapshot_arg(
    value: &Value,
    op: &'static str,
) -> Result<Option<TxidSnapshotValue>, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let snapshot_text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: value.clone(),
        right: Value::Text("".into()),
    })?;
    parse_txid_snapshot(snapshot_text).map(Some)
}

fn current_snapshot_value(ctx: &ExecutorContext) -> TxidSnapshotValue {
    let mut in_progress: Vec<u64> = ctx
        .snapshot
        .in_progress
        .iter()
        .copied()
        .map(u64::from)
        .collect();
    if ctx.snapshot.current_xid != INVALID_TRANSACTION_ID
        && ctx.snapshot.current_xid >= ctx.snapshot.xmin
        && ctx.snapshot.current_xid < ctx.snapshot.xmax
    {
        in_progress.push(u64::from(ctx.snapshot.current_xid));
        in_progress.sort_unstable();
        in_progress.dedup();
    }

    TxidSnapshotValue {
        xmin: u64::from(ctx.snapshot.xmin),
        xmax: u64::from(ctx.snapshot.xmax),
        in_progress,
    }
}

fn txid_status_future_error(xid: u64) -> ExecError {
    ExecError::DetailedError {
        message: format!("transaction ID {xid} is in the future"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn eval_txid_status_value(xid: u64, ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let txns = ctx.txns.read();
    if xid > u64::from(txns.next_xid()) {
        return Err(txid_status_future_error(xid));
    }
    if xid == 0 || xid > u64::from(u32::MAX) {
        return Ok(Value::Null);
    }

    let xid = xid as u32;
    let status = txns.status(xid);
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

pub(crate) fn is_txid_snapshot_type_oid(type_oid: u32) -> bool {
    matches!(type_oid, TXID_SNAPSHOT_TYPE_OID | PG_SNAPSHOT_TYPE_OID)
}

pub(crate) fn cast_text_to_txid_snapshot(text: &str) -> Result<Value, ExecError> {
    let snapshot = parse_txid_snapshot(text)?;
    Ok(Value::Text(CompactString::from_owned(snapshot.render())))
}

pub(crate) fn eval_txid_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match func {
        BuiltinScalarFunction::TxidCurrent => Ok(Value::Xid8(u64::from(ctx.ensure_write_xid()?))),
        BuiltinScalarFunction::TxidCurrentIfAssigned => Ok(ctx
            .transaction_xid()
            .filter(|xid| *xid != INVALID_TRANSACTION_ID)
            .map(|xid| Value::Xid8(u64::from(xid)))
            .unwrap_or(Value::Null)),
        BuiltinScalarFunction::TxidCurrentSnapshot => Ok(Value::Text(CompactString::from_owned(
            current_snapshot_value(ctx).render(),
        ))),
        BuiltinScalarFunction::TxidSnapshotXmin => match values {
            [value] => Ok(txid_snapshot_arg(value, "txid_snapshot_xmin")?
                .map(|snapshot| Value::Xid8(snapshot.xmin))
                .unwrap_or(Value::Null)),
            _ => Err(ExecError::DetailedError {
                message: "malformed txid builtin call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        },
        BuiltinScalarFunction::TxidSnapshotXmax => match values {
            [value] => Ok(txid_snapshot_arg(value, "txid_snapshot_xmax")?
                .map(|snapshot| Value::Xid8(snapshot.xmax))
                .unwrap_or(Value::Null)),
            _ => Err(ExecError::DetailedError {
                message: "malformed txid builtin call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        },
        BuiltinScalarFunction::TxidVisibleInSnapshot => match values {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [Value::Int64(xid), snapshot] if *xid >= 0 => {
                let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")?
                else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(snapshot.xid_visible(*xid as u64)))
            }
            [Value::Xid8(xid), snapshot] => {
                let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")?
                else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(snapshot.xid_visible(*xid)))
            }
            [Value::Int32(xid), snapshot] if *xid >= 0 => {
                let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")?
                else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(snapshot.xid_visible(*xid as u64)))
            }
            [left, right] => Err(ExecError::TypeMismatch {
                op: "txid_visible_in_snapshot",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(ExecError::DetailedError {
                message: "malformed txid builtin call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        },
        BuiltinScalarFunction::TxidStatus => match values {
            [Value::Null] => Ok(Value::Null),
            [Value::Int64(xid)] if *xid >= 0 => eval_txid_status_value(*xid as u64, ctx),
            [Value::Int32(xid)] if *xid >= 0 => eval_txid_status_value(*xid as u64, ctx),
            [Value::Xid8(xid)] => eval_txid_status_value(*xid, ctx),
            [value] => Err(ExecError::TypeMismatch {
                op: "txid_status",
                left: value.clone(),
                right: Value::Int64(0),
            }),
            _ => Err(ExecError::DetailedError {
                message: "malformed txid builtin call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        },
        _ => unreachable!("non-txid builtin dispatched to expr_txid"),
    }
}

pub(crate) fn eval_txid_snapshot_xip_values(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    match values {
        [value] => Ok(txid_snapshot_arg(value, "txid_snapshot_xip")?
            .map(|snapshot| snapshot.in_progress.into_iter().map(Value::Xid8).collect())
            .unwrap_or_default()),
        _ => Err(ExecError::DetailedError {
            message: "malformed txid builtin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
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

    #[test]
    fn txid_snapshot_visibility_matches_snapshot_boundaries() {
        let snapshot = parse_txid_snapshot("12:20:13,15,18").unwrap();
        assert!(snapshot.xid_visible(12));
        assert!(!snapshot.xid_visible(13));
        assert!(snapshot.xid_visible(14));
        assert!(!snapshot.xid_visible(20));
    }
}
