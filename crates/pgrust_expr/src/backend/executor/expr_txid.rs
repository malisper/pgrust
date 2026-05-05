use super::{ExecError, Value};
use pgrust_catalog_data::{PG_SNAPSHOT_TYPE_OID, TXID_SNAPSHOT_TYPE_OID};
use pgrust_core::CompactString;

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

pub fn is_txid_snapshot_type_oid(type_oid: u32) -> bool {
    matches!(type_oid, TXID_SNAPSHOT_TYPE_OID | PG_SNAPSHOT_TYPE_OID)
}

pub fn cast_text_to_txid_snapshot(text: &str) -> Result<Value, ExecError> {
    let snapshot = parse_txid_snapshot(text)?;
    Ok(Value::Text(CompactString::from_owned(snapshot.render())))
}

pub fn eval_txid_snapshot_xmin_value(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [value] => Ok(txid_snapshot_arg(value, "txid_snapshot_xmin")?
            .map(|snapshot| Value::Xid8(snapshot.xmin))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::DetailedError {
            message: "malformed txid builtin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

pub fn eval_txid_snapshot_xmax_value(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [value] => Ok(txid_snapshot_arg(value, "txid_snapshot_xmax")?
            .map(|snapshot| Value::Xid8(snapshot.xmax))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::DetailedError {
            message: "malformed txid builtin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

pub fn eval_txid_visible_in_snapshot_value(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Int64(xid), snapshot] if *xid >= 0 => {
            let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")? else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(snapshot.xid_visible(*xid as u64)))
        }
        [Value::Xid8(xid), snapshot] => {
            let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")? else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(snapshot.xid_visible(*xid)))
        }
        [Value::Int32(xid), snapshot] if *xid >= 0 => {
            let Some(snapshot) = txid_snapshot_arg(snapshot, "txid_visible_in_snapshot")? else {
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
    }
}

pub fn eval_txid_snapshot_xip_values(values: &[Value]) -> Result<Vec<Value>, ExecError> {
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
