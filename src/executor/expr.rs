use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compact_string::CompactString;
use super::nodes::*;
use super::ExecError;

extern crate rand;

pub fn eval_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => slot
            .values()?
            .get(*index)
            .cloned()
            .ok_or(ExecError::InvalidColumn(*index)),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Add(left, right) => add_values(eval_expr(left, slot)?, eval_expr(right, slot)?),
        Expr::Negate(inner) => negate_value(eval_expr(inner, slot)?),
        Expr::Eq(left, right) => {
            compare_values("=", eval_expr(left, slot)?, eval_expr(right, slot)?)
        }
        Expr::Lt(left, right) => order_values(
            "<",
            eval_expr(left, slot)?,
            eval_expr(right, slot)?,
            |a, b| a < b,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_expr(left, slot)?,
            eval_expr(right, slot)?,
            |a, b| a > b,
        ),
        Expr::And(left, right) => eval_and(eval_expr(left, slot)?, eval_expr(right, slot)?),
        Expr::Or(left, right) => eval_or(eval_expr(left, slot)?, eval_expr(right, slot)?),
        Expr::Not(inner) => match eval_expr(inner, slot)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(eval_expr(inner, slot)?, Value::Null))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(eval_expr(inner, slot)?, Value::Null))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_expr(left, slot)?,
            &eval_expr(right, slot)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_expr(left, slot)?,
            &eval_expr(right, slot)?,
        ))),
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(render_current_timestamp()))),
    }
}

pub(crate) fn compare_order_by_keys(
    items: &[OrderByEntry],
    left_keys: &[Value],
    right_keys: &[Value],
) -> Ordering {
    for (item, (left_value, right_value)) in items.iter().zip(left_keys.iter().zip(right_keys.iter())) {
        let ordering = compare_order_values(left_value, right_value, item.nulls_first, item.descending);
        if ordering != Ordering::Equal {
            return if item.descending && !matches!((left_value, right_value), (Value::Null, _) | (_, Value::Null)) {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}

pub(crate) fn compare_order_values(
    left: &Value,
    right: &Value,
    nulls_first: Option<bool>,
    descending: bool,
) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(descending);
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first { Ordering::Less } else { Ordering::Greater }
        }
        (_, Value::Null) => {
            if nulls_first { Ordering::Greater } else { Ordering::Less }
        }
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

pub(crate) fn predicate_matches(predicate: Option<&Expr>, slot: &mut TupleSlot) -> Result<bool, ExecError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    match eval_expr(predicate, slot)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

pub(crate) fn tuple_from_values(desc: &RelationDesc, values: &[Value]) -> Result<crate::access::heap::tuple::HeapTuple, ExecError> {
    let tuple_values = desc
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()?;
    crate::access::heap::tuple::HeapTuple::from_values(&desc.attribute_descs(), &tuple_values).map_err(ExecError::from)
}

pub(crate) fn encode_value(column: &ColumnDesc, value: &Value) -> Result<crate::access::heap::tuple::TupleValue, ExecError> {
    use crate::access::heap::tuple::TupleValue;
    match (column.ty, value) {
        (_, Value::Null) => {
            if !column.storage.nullable {
                Err(ExecError::MissingRequiredColumn(column.name.clone()))
            } else {
                Ok(TupleValue::Null)
            }
        }
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Text, Value::Text(v)) => Ok(TupleValue::Bytes(v.as_str().as_bytes().to_vec())),
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(*v)])),
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other.clone(),
        }),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };

    match column.ty {
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "int4 must be exactly 4 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            // SAFETY: text columns are stored as valid UTF-8 by the insert path.
            Ok(Value::Text(CompactString::new(unsafe { std::str::from_utf8_unchecked(bytes) })))
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            match bytes[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: format!("invalid bool byte {}", other),
                }),
            }
        }
    }
}

/// Decode all values from raw on-page tuple bytes in a single pass.
/// This fuses deform + decode, avoiding the intermediate Vec<Option<&[u8]>>.
#[inline]
pub(crate) fn decode_tuple_from_bytes(
    tuple_bytes: &[u8],
    desc: &RelationDesc,
    attr_descs: &[crate::access::heap::tuple::AttributeDesc],
) -> Result<Vec<Value>, ExecError> {
    use crate::access::heap::tuple::{HEAP_HASNULL, SIZEOF_HEAP_TUPLE_HEADER};

    if tuple_bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
        return Err(ExecError::Tuple(crate::access::heap::tuple::TupleError::HeaderTooShort));
    }
    let hoff = tuple_bytes[22];
    let _infomask2 = u16::from_le_bytes([tuple_bytes[18], tuple_bytes[19]]);
    let infomask = u16::from_le_bytes([tuple_bytes[20], tuple_bytes[21]]);
    let has_null = infomask & HEAP_HASNULL != 0;
    let null_bitmap = if has_null {
        &tuple_bytes[SIZEOF_HEAP_TUPLE_HEADER..]
    } else {
        &[] as &[u8]
    };
    let data = &tuple_bytes[usize::from(hoff)..];

    let mut values = Vec::with_capacity(desc.columns.len());
    let mut off = 0usize;

    for (i, (column, attr)) in desc.columns.iter().zip(attr_descs.iter()).enumerate() {
        let is_null = has_null && crate::access::heap::tuple::att_isnull(i, null_bitmap);
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match attr.attlen {
            len if len > 0 => {
                off = attr.attalign.align_offset(off);
                let end = off + len as usize;
                let bytes = &data[off..end];
                off = end;
                values.push(decode_fixed_value(column, bytes)?);
            }
            -1 => {
                off = attr.attalign.align_offset(off);
                let total_len = u32::from_le_bytes([
                    data[off], data[off + 1], data[off + 2], data[off + 3],
                ]) as usize;
                let start = off + 4;
                let end = off + total_len;
                let bytes = &data[start..end];
                off = end;
                values.push(decode_varlen_value(column, bytes)?);
            }
            -2 => {
                let mut end = off;
                while data[end] != 0 {
                    end += 1;
                }
                let bytes = &data[off..end];
                off = end + 1;
                values.push(decode_varlen_value(column, bytes)?);
            }
            _other => {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: attr.attlen,
                });
            }
        }
    }

    Ok(values)
}

#[inline]
fn decode_fixed_value(column: &ColumnDesc, bytes: &[u8]) -> Result<Value, ExecError> {
    match column.ty {
        ScalarType::Int32 => Ok(Value::Int32(i32::from_le_bytes(
            bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "int4 must be exactly 4 bytes".into(),
            })?,
        ))),
        ScalarType::Bool => match bytes[0] {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            other => Err(ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: format!("invalid bool byte {}", other),
            }),
        },
        ScalarType::Text => Err(ExecError::UnsupportedStorageType {
            column: column.name.clone(),
            ty: column.ty,
            attlen: column.storage.attlen,
        }),
    }
}

#[inline]
fn decode_varlen_value(column: &ColumnDesc, bytes: &[u8]) -> Result<Value, ExecError> {
    match column.ty {
        ScalarType::Text => {
            // SAFETY: text columns are stored as valid UTF-8 by the insert path.
            Ok(Value::Text(CompactString::new(unsafe { std::str::from_utf8_unchecked(bytes) })))
        }
        _ => Err(ExecError::UnsupportedStorageType {
            column: column.name.clone(),
            ty: column.ty,
            attlen: column.storage.attlen,
        }),
    }
}

fn eval_and(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true), Value::Null)
        | (Value::Null, Value::Bool(true))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "AND",
            left,
            right,
        }),
    }
}

fn eval_or(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false), Value::Null)
        | (Value::Null, Value::Bool(false))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "OR",
            left,
            right,
        }),
    }
}

fn compare_values(op: &'static str, left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(l == r)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(l.to_bits() == r.to_bits())),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(l == r)),
        (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(l == r)),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn values_are_distinct(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => false,
        (Value::Null, _) | (_, Value::Null) => true,
        (Value::Int32(l), Value::Int32(r)) => l != r,
        (Value::Float64(l), Value::Float64(r)) => l.to_bits() != r.to_bits(),
        (Value::Text(l), Value::Text(r)) => l != r,
        (Value::Bool(l), Value::Bool(r)) => l != r,
        _ => true,
    }
}

fn add_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l + r)),
        _ => Err(ExecError::TypeMismatch {
            op: "+",
            left,
            right,
        }),
    }
}

fn negate_value(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int32(v) => Ok(Value::Int32(-v)),
        other => Err(ExecError::TypeMismatch {
            op: "unary -",
            left: other,
            right: Value::Null,
        }),
    }
}

fn order_values<F>(op: &'static str, left: Value, right: Value, cmp: F) -> Result<Value, ExecError>
where
    F: FnOnce(i32, i32) -> bool + Copy,
{
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(cmp(*l, *r))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(match op {
            "<" => l < r,
            ">" => l > r,
            _ => unreachable!(),
        })),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(match op {
            "<" => l < r,
            ">" => l > r,
            _ => unreachable!(),
        })),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}
