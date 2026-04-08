use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compact_string::CompactString;
use super::nodes::*;
use super::ExecError;

extern crate rand;

pub fn eval_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => {
            let val = slot.get_attr(*index)?;
            Ok(val.clone())
        }
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
        Expr::RegexMatch(left, right) => {
            let text = eval_expr(left, slot)?;
            let pattern = eval_expr(right, slot)?;
            if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                return Ok(Value::Null);
            }
            let text_str = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~", left: text.clone(), right: pattern.clone(),
            })?;
            let pat_str = pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~", left: text.clone(), right: pattern.clone(),
            })?;
            let re = regex::Regex::new(pat_str)
                .map_err(|e| ExecError::InvalidRegex(e.to_string()))?;
            Ok(Value::Bool(re.is_match(text_str)))
        }
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
        (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
            a.as_text().unwrap().cmp(b.as_text().unwrap())
        }
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// A predicate compiled at plan time into a specialized closure, like PG's
/// ExecInitQual which resolves expression evaluation steps once. Eliminates
/// per-tuple recursive eval_expr dispatch. Allocated once at plan time;
/// per-tuple cost is just an indirect function call.
pub(crate) type CompiledPredicate = Box<dyn Fn(&mut TupleSlot) -> Result<bool, ExecError>>;

/// Compile a predicate with access to the tuple decoder for direct byte access.
/// Like PG's heap_getattr fast path for fixed-offset attributes.
pub(crate) fn compile_predicate_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> CompiledPredicate {
    // Try to compile with fixed-offset fast path first.
    if let Some(pred) = try_compile_fixed_offset(expr, decoder) {
        return pred;
    }
    // Fall back to the generic compiled predicate.
    compile_predicate(expr)
}

/// Try to compile a predicate that reads fixed-offset int32 columns directly
/// from raw tuple bytes, bypassing slot_getsomeattrs entirely.
fn try_compile_fixed_offset(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Option<CompiledPredicate> {
    match expr {
        Expr::Gt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let off = decoder.fixed_int32_offset(*col)?;
            let val = *val;
            return Some(Box::new(move |slot| Ok(slot.get_fixed_int32(off).map_or(false, |v| v > val))));
        } else { },
        Expr::Lt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let off = decoder.fixed_int32_offset(*col)?;
            let val = *val;
            return Some(Box::new(move |slot| Ok(slot.get_fixed_int32(off).map_or(false, |v| v < val))));
        } else { },
        Expr::Eq(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let off = decoder.fixed_int32_offset(*col)?;
            let val = *val;
            return Some(Box::new(move |slot| Ok(slot.get_fixed_int32(off).map_or(false, |v| v == val))));
        } else { },
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and_with_decoder(expr, decoder);
            return Some(Box::new(move |slot| {
                for part in &parts {
                    if !part(slot)? { return Ok(false); }
                }
                Ok(true)
            }));
        },
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or_with_decoder(expr, decoder);
            return Some(Box::new(move |slot| {
                for part in &parts {
                    if part(slot)? { return Ok(true); }
                }
                Ok(false)
            }));
        },
        _ => { },
    }
    None
}

fn flatten_and_with_decoder(expr: &Expr, decoder: &super::tuple_decoder::CompiledTupleDecoder) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_and_with_decoder_inner(expr: &Expr, decoder: &super::tuple_decoder::CompiledTupleDecoder, out: &mut Vec<CompiledPredicate>) {
    if let Expr::And(left, right) = expr {
        flatten_and_with_decoder_inner(left, decoder, out);
        flatten_and_with_decoder_inner(right, decoder, out);
    } else {
        out.push(compile_predicate_with_decoder(expr, decoder));
    }
}

fn flatten_or_with_decoder(expr: &Expr, decoder: &super::tuple_decoder::CompiledTupleDecoder) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_or_with_decoder_inner(expr: &Expr, decoder: &super::tuple_decoder::CompiledTupleDecoder, out: &mut Vec<CompiledPredicate>) {
    if let Expr::Or(left, right) = expr {
        flatten_or_with_decoder_inner(left, decoder, out);
        flatten_or_with_decoder_inner(right, decoder, out);
    } else {
        out.push(compile_predicate_with_decoder(expr, decoder));
    }
}

/// Compile an expression into a specialized predicate closure.
pub(crate) fn compile_predicate(expr: &Expr) -> CompiledPredicate {
    match expr {
        Expr::Gt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, val) = (*col, *val);
            return Box::new(move |slot| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v > val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: ">", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::Lt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, val) = (*col, *val);
            return Box::new(move |slot| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v < val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: "<", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::Eq(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, val) = (*col, *val);
            return Box::new(move |slot| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v == val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: "=", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and(expr);
            return Box::new(move |slot| {
                for part in &parts {
                    if !part(slot)? { return Ok(false); }
                }
                Ok(true)
            });
        },
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or(expr);
            return Box::new(move |slot| {
                for part in &parts {
                    if part(slot)? { return Ok(true); }
                }
                Ok(false)
            });
        },
        Expr::RegexMatch(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Text(pat))) = (left.as_ref(), right.as_ref()) {
                let col = *col;
                if let Ok(re) = regex::Regex::new(pat.as_str()) {
                    let re = std::sync::Arc::new(re);
                    return Box::new(move |slot| {
                        let val = slot.get_attr(col)?;
                        if let Some(s) = val.as_text() {
                            Ok(re.is_match(s))
                        } else if matches!(val, Value::Null) {
                            Ok(false)
                        } else {
                            Err(ExecError::TypeMismatch {
                                op: "~",
                                left: val.clone(),
                                right: Value::Null,
                            })
                        }
                    });
                }
            }
        },
        _ => { },
    }
    // Fallback: generic eval_expr
    let expr = expr.clone();
    Box::new(move |slot| match eval_expr(&expr, slot)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    })
}

fn flatten_and(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_inner(expr, &mut out);
    out
}

fn flatten_and_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    if let Expr::And(left, right) = expr {
        flatten_and_inner(left, out);
        flatten_and_inner(right, out);
    } else {
        out.push(compile_predicate(expr));
    }
}

fn flatten_or(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_inner(expr, &mut out);
    out
}

fn flatten_or_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    if let Expr::Or(left, right) = expr {
        flatten_or_inner(left, out);
        flatten_or_inner(right, out);
    } else {
        out.push(compile_predicate(expr));
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
        (ScalarType::Text, v) if v.as_text().is_some() => Ok(TupleValue::Bytes(v.as_text().unwrap().as_bytes().to_vec())),
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
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(l.as_text() == r.as_text())),
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
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => l.as_text() != r.as_text(),
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
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(match op {
            "<" => l.as_text().unwrap() < r.as_text().unwrap(),
            ">" => l.as_text().unwrap() > r.as_text().unwrap(),
            _ => unreachable!(),
        })),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}
