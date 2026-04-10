use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::pgrust::compact_string::CompactString;
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use super::nodes::*;
use super::{ExecError, ExecutorContext, executor_start, exec_next};

extern crate rand;

pub fn eval_expr(expr: &Expr, slot: &mut TupleSlot, ctx: &mut ExecutorContext) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => {
            let val = slot.get_attr(*index)?;
            Ok(val.clone())
        }
        Expr::OuterColumn { depth, index } => ctx
            .outer_rows
            .get(*depth)
            .and_then(|row| row.get(*index))
            .cloned()
            .ok_or(ExecError::UnboundOuterColumn { depth: *depth, index: *index }),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Add(left, right) => add_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Sub(left, right) => sub_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Mul(left, right) => mul_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Div(left, right) => div_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Mod(left, right) => mod_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::UnaryPlus(inner) => eval_expr(inner, slot, ctx),
        Expr::Negate(inner) => negate_value(eval_expr(inner, slot, ctx)?),
        Expr::Cast(inner, ty) => cast_value(eval_expr(inner, slot, ctx)?, *ty),
        Expr::Eq(left, right) => {
            compare_values("=", eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::NotEq(left, right) => {
            not_equal_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Lt(left, right) => order_values(
            "<",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::LtEq(left, right) => order_values(
            "<=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::GtEq(left, right) => order_values(
            ">=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::RegexMatch(left, right) => {
            let text = eval_expr(left, slot, ctx)?;
            let pattern = eval_expr(right, slot, ctx)?;
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
        Expr::And(left, right) => eval_and(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Or(left, right) => eval_or(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Not(inner) => match eval_expr(inner, slot, ctx)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(eval_expr(inner, slot, ctx)?, Value::Null))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(eval_expr(inner, slot, ctx)?, Value::Null))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::ArrayLiteral { elements, array_type } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            for expr in elements {
                values.push(cast_value(eval_expr(expr, slot, ctx)?, element_type)?);
            }
            Ok(Value::Array(values))
        }
        Expr::ArrayOverlap(left, right) => {
            eval_array_overlap(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::ScalarSubquery(plan) => eval_scalar_subquery(plan, slot, ctx),
        Expr::ExistsSubquery(plan) => eval_exists_subquery(plan, slot, ctx),
        Expr::AnySubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, false, subquery, slot, ctx)
        }
        Expr::AllSubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, true, subquery, slot, ctx)
        }
        Expr::AnyArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, false, &right_value)
        }
        Expr::AllArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, true, &right_value)
        }
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(render_current_timestamp()))),
    }
}

fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    array_value: &Value,
) -> Result<Value, ExecError> {
    match array_value {
        Value::Null => Ok(Value::Null),
        Value::Array(items) => {
            let mut saw_null = false;
            for item in items {
                match compare_subquery_values(left_value, item, op)? {
                    Value::Bool(result) => {
                        if !is_all && result {
                            return Ok(Value::Bool(true));
                        }
                        if is_all && !result {
                            return Ok(Value::Bool(false));
                        }
                    }
                    Value::Null => saw_null = true,
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            if items.is_empty() {
                Ok(Value::Bool(is_all))
            } else if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(is_all))
            }
        }
        other => Err(ExecError::TypeMismatch {
            op: if is_all { "ALL" } else { "ANY" },
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(left_items), Value::Array(right_items)) => {
            for left_item in &left_items {
                if matches!(left_item, Value::Null) {
                    continue;
                }
                for right_item in &right_items {
                    if matches!(right_item, Value::Null) {
                        continue;
                    }
                    if matches!(compare_values("=", left_item.clone(), right_item.clone())?, Value::Bool(true)) {
                        return Ok(Value::Bool(true));
                    }
                }
            }
            Ok(Value::Bool(false))
        }
        (left, right) => Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right,
        }),
    }
}

fn eval_scalar_subquery(plan: &Plan, slot: &mut TupleSlot, ctx: &mut ExecutorContext) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut first_value = None;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            if first_value.is_some() {
                return Err(ExecError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                ));
            }
            first_value = Some(values[0].clone());
        }
        Ok(first_value.unwrap_or(Value::Null))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_exists_subquery(plan: &Plan, slot: &mut TupleSlot, ctx: &mut ExecutorContext) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        Ok(Value::Bool(exec_next(&mut state, ctx)?.is_some()))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_quantified_subquery(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut saw_row = false;
        let mut saw_null = false;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            saw_row = true;
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            match compare_subquery_values(left_value, &values[0], op)? {
                Value::Bool(result) => {
                    if !is_all && result {
                        return Ok(Value::Bool(true));
                    }
                    if is_all && !result {
                        return Ok(Value::Bool(false));
                    }
                }
                Value::Null => saw_null = true,
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }
        if !saw_row {
            Ok(Value::Bool(is_all))
        } else if saw_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(is_all))
        }
    })();
    ctx.outer_rows.remove(0);
    result
}

fn compare_subquery_values(
    left: &Value,
    right: &Value,
    op: SubqueryComparisonOp,
) -> Result<Value, ExecError> {
    match op {
        SubqueryComparisonOp::Eq => compare_values("=", left.clone(), right.clone()),
        SubqueryComparisonOp::NotEq => not_equal_values(left.clone(), right.clone()),
        SubqueryComparisonOp::Lt => order_values("<", left.clone(), right.clone()),
        SubqueryComparisonOp::LtEq => order_values("<=", left.clone(), right.clone()),
        SubqueryComparisonOp::Gt => order_values(">", left.clone(), right.clone()),
        SubqueryComparisonOp::GtEq => order_values(">=", left.clone(), right.clone()),
    }
}

fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    if ty.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(items.len());
                for item in items {
                    casted.push(cast_value(item, element_type)?);
                }
                Ok(Value::Array(casted))
            }
            other => Err(ExecError::TypeMismatch {
                op: "::array",
                left: other,
                right: Value::Null,
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => match ty {
            SqlType { kind: SqlTypeKind::Int2, .. } => Ok(Value::Int16(v)),
            SqlType { kind: SqlTypeKind::Int4, .. } => Ok(Value::Int32(v as i32)),
            SqlType { kind: SqlTypeKind::Int8, .. } => Ok(Value::Int64(v as i64)),
            SqlType { kind: SqlTypeKind::Float4 | SqlTypeKind::Float8, .. } => Ok(Value::Float64(v as f64)),
            SqlType { kind: SqlTypeKind::Numeric, .. } => Ok(Value::Numeric(CompactString::new(&v.to_string()))),
            SqlType { kind: SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar, .. } => {
                cast_text_value(&v.to_string(), ty, true)
            }
            SqlType { kind: SqlTypeKind::Bool, .. } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int16(v),
                right: Value::Bool(false),
            }),
        },
        Value::Int32(v) => match ty {
            SqlType { kind: SqlTypeKind::Int2, .. } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int2",
                    left: Value::Int32(v),
                    right: Value::Int16(0),
                }),
            SqlType { kind: SqlTypeKind::Int4, .. } => Ok(Value::Int32(v)),
            SqlType { kind: SqlTypeKind::Int8, .. } => Ok(Value::Int64(v as i64)),
            SqlType { kind: SqlTypeKind::Float4 | SqlTypeKind::Float8, .. } => Ok(Value::Float64(v as f64)),
            SqlType { kind: SqlTypeKind::Numeric, .. } => Ok(Value::Numeric(CompactString::new(&v.to_string()))),
            SqlType { kind: SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar, .. } => {
                cast_text_value(&v.to_string(), ty, true)
            }
            SqlType { kind: SqlTypeKind::Bool, .. } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int32(v),
                right: Value::Bool(false),
            }),
        },
        Value::Bool(v) => match ty {
            SqlType { kind: SqlTypeKind::Bool, .. } => Ok(Value::Bool(v)),
            SqlType { kind: SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar, .. } => {
                cast_text_value(if v { "true" } else { "false" }, ty, true)
            }
            SqlType { kind: SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Float4 | SqlTypeKind::Float8 | SqlTypeKind::Numeric, .. } => Err(ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Bool(v),
                right: Value::Int32(0),
            }),
        },
        Value::Text(text) => cast_text_value(text.as_str(), ty, true),
        Value::TextRef(ptr, len) => {
            let text = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
            };
            cast_text_value(text, ty, true)
        }
        Value::Int64(v) => match ty {
            SqlType { kind: SqlTypeKind::Int2, .. } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int2",
                    left: Value::Int64(v),
                    right: Value::Int16(0),
                }),
            SqlType { kind: SqlTypeKind::Int4, .. } => i32::try_from(v)
                .map(Value::Int32)
                .map_err(|_| ExecError::TypeMismatch {
                    op: "::int4",
                    left: Value::Int64(v),
                    right: Value::Int32(0),
                }),
            SqlType { kind: SqlTypeKind::Int8, .. } => Ok(Value::Int64(v)),
            SqlType { kind: SqlTypeKind::Float4 | SqlTypeKind::Float8, .. } => Ok(Value::Float64(v as f64)),
            SqlType { kind: SqlTypeKind::Numeric, .. } => Ok(Value::Numeric(CompactString::new(&v.to_string()))),
            SqlType { kind: SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar, .. } => {
                cast_text_value(&v.to_string(), ty, true)
            }
            SqlType { kind: SqlTypeKind::Bool, .. } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int64(v),
                right: Value::Bool(false),
            }),
        },
        Value::Float64(v) => match ty {
            SqlType { kind: SqlTypeKind::Float4 | SqlTypeKind::Float8, .. } => Ok(Value::Float64(v)),
            SqlType { kind: SqlTypeKind::Numeric, .. } => Ok(Value::Numeric(CompactString::new(&v.to_string()))),
            SqlType { kind: SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar, .. } => {
                cast_text_value(&v.to_string(), ty, true)
            }
            SqlType { kind: SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Bool, .. } => Err(ExecError::TypeMismatch {
                op: "::",
                left: Value::Float64(v),
                right: match ty {
                    SqlType { kind: SqlTypeKind::Int2, .. } => Value::Int16(0),
                    SqlType { kind: SqlTypeKind::Int4, .. } => Value::Int32(0),
                    SqlType { kind: SqlTypeKind::Int8, .. } => Value::Int64(0),
                    SqlType { kind: SqlTypeKind::Bool, .. } => Value::Bool(false),
                    _ => Value::Text(CompactString::new("")),
                },
            }),
        },
        Value::Numeric(text) => cast_text_value(text.as_str(), ty, true),
        Value::Array(items) => Ok(Value::Array(items)),
    }
}

fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Text | SqlTypeKind::Timestamp => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::from_owned(
            coerce_character_string(text, ty, explicit)?,
        ))),
        SqlTypeKind::Int2 => text.parse::<i16>()
            .map(Value::Int16)
            .map_err(|_| ExecError::TypeMismatch {
                op: "::int2",
                left: Value::Text(CompactString::new(text)),
                right: Value::Int16(0),
            }),
        SqlTypeKind::Int4 => text.parse::<i32>()
            .map(Value::Int32)
            .map_err(|_| ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Text(CompactString::new(text)),
                right: Value::Int32(0),
            }),
        SqlTypeKind::Int8 => text.parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| ExecError::TypeMismatch {
                op: "::int8",
                left: Value::Text(CompactString::new(text)),
                right: Value::Int64(0),
            }),
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => text.parse::<f64>()
            .map(Value::Float64)
            .map_err(|_| ExecError::TypeMismatch {
                op: "::float8",
                left: Value::Text(CompactString::new(text)),
                right: Value::Float64(0.0),
            }),
        SqlTypeKind::Numeric => Ok(Value::Numeric(CompactString::new(text))),
        SqlTypeKind::Bool => match text.to_ascii_lowercase().as_str() {
            "true" | "t" => Ok(Value::Bool(true)),
            "false" | "f" => Ok(Value::Bool(false)),
            _ => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Text(CompactString::new(text)),
                right: Value::Bool(false),
            }),
        },
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let Some(max_chars) = ty.char_len() else {
        return Ok(text.to_string());
    };

    let char_count = text.chars().count() as i32;
    if char_count <= max_chars {
        return Ok(text.to_string());
    }

    let clip_idx = text
        .char_indices()
        .nth(max_chars as usize)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let truncated = &text[..clip_idx];
    let remainder = &text[clip_idx..];
    if explicit || remainder.chars().all(|ch| ch == ' ') {
        Ok(truncated.to_string())
    } else {
        Err(ExecError::StringDataRightTruncation {
            ty: format!("character varying({max_chars})"),
        })
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
        (Value::Array(a), Value::Array(b)) => format_array_text(a).cmp(&format_array_text(b)),
        _ => Ordering::Equal,
    }
}

/// A predicate compiled at plan time into a specialized closure, like PG's
/// ExecInitQual which resolves expression evaluation steps once. Eliminates
/// per-tuple recursive eval_expr dispatch. Allocated once at plan time;
/// per-tuple cost is just an indirect function call.
pub(crate) type CompiledPredicate = Box<dyn Fn(&mut TupleSlot, &mut ExecutorContext) -> Result<bool, ExecError>>;

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
            let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
            return Some(Box::new(move |slot, _ctx| {
                if let Some(v) = slot.get_fixed_int32(off) { return Ok(v > val); }
                match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v > val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch { op: ">", left: other.clone(), right: Value::Int32(val) }),
                }
            }));
        } else { },
        Expr::Lt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
            return Some(Box::new(move |slot, _ctx| {
                if let Some(v) = slot.get_fixed_int32(off) { return Ok(v < val); }
                match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v < val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch { op: "<", left: other.clone(), right: Value::Int32(val) }),
                }
            }));
        } else { },
        Expr::Eq(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, off, val) = (*col, decoder.fixed_int32_offset(*col)?, *val);
            return Some(Box::new(move |slot, _ctx| {
                if let Some(v) = slot.get_fixed_int32(off) { return Ok(v == val); }
                match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v == val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch { op: "=", left: other.clone(), right: Value::Int32(val) }),
                }
            }));
        } else { },
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? { return Ok(false); }
                }
                Ok(true)
            }));
        },
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? { return Ok(true); }
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
            return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v > val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: ">", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::Lt(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, val) = (*col, *val);
            return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v < val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: "<", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::Eq(left, right) => if let (Expr::Column(col), Expr::Const(Value::Int32(val))) = (left.as_ref(), right.as_ref()) {
            let (col, val) = (*col, *val);
            return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                Value::Int32(v) => Ok(*v == val),
                Value::Null => Ok(false),
                other => Err(ExecError::TypeMismatch { op: "=", left: other.clone(), right: Value::Int32(val) }),
            });
        } else { },
        Expr::And(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_and(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? { return Ok(false); }
                }
                Ok(true)
            });
        },
        Expr::Or(_, _) => {
            let parts: Vec<CompiledPredicate> = flatten_or(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? { return Ok(true); }
                }
                Ok(false)
            });
        },
        Expr::RegexMatch(left, right) => {
            if let (Expr::Column(col), Expr::Const(Value::Text(pat))) = (left.as_ref(), right.as_ref()) {
                let col = *col;
                if let Ok(re) = regex::Regex::new(pat.as_str()) {
                    let re = std::sync::Arc::new(re);
                    return Box::new(move |slot, _ctx| {
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
    Box::new(move |slot, ctx| match eval_expr(&expr, slot, ctx)? {
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


pub(crate) fn tuple_from_values(desc: &RelationDesc, values: &[Value]) -> Result<crate::include::access::htup::HeapTuple, ExecError> {
    let tuple_values = desc
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()?;
    crate::include::access::htup::HeapTuple::from_values(&desc.attribute_descs(), &tuple_values).map_err(ExecError::from)
}

pub(crate) fn encode_value(column: &ColumnDesc, value: &Value) -> Result<crate::include::access::htup::TupleValue, ExecError> {
    use crate::include::access::htup::TupleValue;
    match (&column.ty, value) {
        (_, Value::Null) => {
            if !column.storage.nullable {
                Err(ExecError::MissingRequiredColumn(column.name.clone()))
            } else {
                Ok(TupleValue::Null)
            }
        }
        (ScalarType::Int16, Value::Int16(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int64, Value::Int64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Float32, Value::Float64(v)) => Ok(TupleValue::Bytes((*v as f32).to_le_bytes().to_vec())),
        (ScalarType::Float64, Value::Float64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Numeric, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            Ok(TupleValue::Bytes(coerced.as_text().unwrap().as_bytes().to_vec()))
        }
        (ScalarType::Text, v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            Ok(TupleValue::Bytes(coerced.as_text().unwrap().as_bytes().to_vec()))
        }
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(*v)])),
        (ScalarType::Array(_), v) => {
            let coerced = coerce_assignment_value(v, column.sql_type)?;
            match coerced {
                Value::Array(items) => Ok(TupleValue::Bytes(encode_array_bytes(column.sql_type.element_type(), &items)?)),
                other => Err(ExecError::TypeMismatch {
                    op: "assignment",
                    left: Value::Null,
                    right: other,
                }),
            }
        }
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other.clone(),
        }),
    }
}

fn coerce_assignment_value(value: &Value, target: SqlType) -> Result<Value, ExecError> {
    if target.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = target.element_type();
                let mut coerced = Vec::with_capacity(items.len());
                for item in items {
                    coerced.push(coerce_assignment_value(item, element_type)?);
                }
                Ok(Value::Array(coerced))
            }
            other => Err(ExecError::TypeMismatch {
                op: "copy assignment",
                left: Value::Null,
                right: other.clone(),
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int32(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Bool(v) => cast_text_value(if *v { "true" } else { "false" }, target, false),
        Value::Float64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Numeric(text) => cast_text_value(text.as_str(), target, false),
        Value::Text(text) => cast_text_value(text.as_str(), target, false),
        Value::TextRef(_, _) => cast_text_value(value.as_text().unwrap(), target, false),
        Value::Array(items) => Ok(Value::Array(items.clone())),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };

    match column.ty {
        ScalarType::Int16 => {
            if column.storage.attlen != 2 || bytes.len() != 2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int16(i16::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "int2 must be exactly 2 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
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
        ScalarType::Int64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "int8 must be exactly 8 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Float32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Float64(f32::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float4 must be exactly 4 bytes".into(),
                    })?,
            ) as f64))
        }
        ScalarType::Float64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Float64(f64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float8 must be exactly 8 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Numeric => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Numeric(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
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
                    ty: column.ty.clone(),
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
        ScalarType::Array(_) => {
            if column.storage.attlen != -1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            decode_array_bytes(column.sql_type.element_type(), bytes)
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
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(l == r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool((*l as i32) == *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(l == r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l == r)),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => {
            Ok(Value::Bool(numeric_as_f64(l).unwrap() == numeric_as_f64(r).unwrap()))
        }
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(l.as_text() == r.as_text())),
        (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(l == r)),
        (Value::Array(l), Value::Array(r)) => Ok(Value::Bool(l == r)),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn not_equal_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match compare_values("=", left.clone(), right.clone())? {
        Value::Bool(value) => Ok(Value::Bool(!value)),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn values_are_distinct(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => false,
        (Value::Null, _) | (_, Value::Null) => true,
        (Value::Int16(l), Value::Int16(r)) => l != r,
        (Value::Int16(l), Value::Int32(r)) => (*l as i32) != *r,
        (Value::Int16(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int32(l), Value::Int32(r)) => l != r,
        (Value::Int32(l), Value::Int16(r)) => *l != (*r as i32),
        (Value::Int32(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int64(l), Value::Int16(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int32(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int64(r)) => l != r,
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => numeric_as_f64(l).unwrap() != numeric_as_f64(r).unwrap(),
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => l.as_text() != r.as_text(),
        (Value::Bool(l), Value::Bool(r)) => l != r,
        (Value::Array(l), Value::Array(r)) => l != r,
        _ => true,
    }
}

fn add_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l + r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) + *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) + *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l + (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l + r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) + *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l + (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l + (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => {
            Ok(Value::Float64(numeric_as_f64(l).unwrap() + numeric_as_f64(r).unwrap()))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "+",
            left,
            right,
        }),
    }
}

fn sub_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l - r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) - *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) - *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l - (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l - r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) - *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l - (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l - (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => {
            Ok(Value::Float64(numeric_as_f64(l).unwrap() - numeric_as_f64(r).unwrap()))
        }
        _ => Err(ExecError::TypeMismatch { op: "-", left, right }),
    }
}

fn mul_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l * r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) * *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) * *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l * (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l * r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) * *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l * (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l * (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => {
            Ok(Value::Float64(numeric_as_f64(l).unwrap() * numeric_as_f64(r).unwrap()))
        }
        _ => Err(ExecError::TypeMismatch { op: "*", left, right }),
    }
}

fn div_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        Value::Float64(v) => *v == 0.0,
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("/"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l / r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) / *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) / *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l / (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l / r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) / *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l / (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l / (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l / r)),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => {
            Ok(Value::Float64(numeric_as_f64(l).unwrap() / numeric_as_f64(r).unwrap()))
        }
        _ => Err(ExecError::TypeMismatch { op: "/", left, right }),
    }
}

fn mod_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("%"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l % r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32) % *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) % *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(*l % (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l % r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64) % *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(*l % (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(*l % (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l % r)),
        _ => Err(ExecError::TypeMismatch { op: "%", left, right }),
    }
}

fn negate_value(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => Ok(Value::Int16(-v)),
        Value::Int32(v) => Ok(Value::Int32(-v)),
        Value::Int64(v) => Ok(Value::Int64(-v)),
        Value::Float64(v) => Ok(Value::Float64(-v)),
        other => Err(ExecError::TypeMismatch {
            op: "unary -",
            left: other,
            right: Value::Null,
        }),
    }
}

fn order_values(op: &'static str, left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l as i32, *r, op))),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i32, op))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(match op {
            "<" => l < r,
            "<=" => l <= r,
            ">" => l > r,
            ">=" => l >= r,
            _ => unreachable!(),
        })),
        (l, r) if numeric_as_f64(l).is_some() && numeric_as_f64(r).is_some() => Ok(Value::Bool(match op {
            "<" => numeric_as_f64(l).unwrap() < numeric_as_f64(r).unwrap(),
            "<=" => numeric_as_f64(l).unwrap() <= numeric_as_f64(r).unwrap(),
            ">" => numeric_as_f64(l).unwrap() > numeric_as_f64(r).unwrap(),
            ">=" => numeric_as_f64(l).unwrap() >= numeric_as_f64(r).unwrap(),
            _ => unreachable!(),
        })),
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(match op {
            "<" => l.as_text().unwrap() < r.as_text().unwrap(),
            "<=" => l.as_text().unwrap() <= r.as_text().unwrap(),
            ">" => l.as_text().unwrap() > r.as_text().unwrap(),
            ">=" => l.as_text().unwrap() >= r.as_text().unwrap(),
            _ => unreachable!(),
        })),
        (Value::Array(l), Value::Array(r)) => {
            let left = format_array_text(l);
            let right = format_array_text(r);
            Ok(Value::Bool(match op {
                "<" => left < right,
                "<=" => left <= right,
                ">" => left > right,
                ">=" => left >= right,
                _ => unreachable!(),
            }))
        }
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn compare_ord<T: Ord>(left: T, right: T, op: &'static str) -> bool {
    match op {
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => unreachable!(),
    }
}

fn encode_array_bytes(element_type: SqlType, items: &[Value]) -> Result<Vec<u8>, ExecError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        match item {
            Value::Null => bytes.extend_from_slice(&(-1_i32).to_le_bytes()),
            _ => {
                let payload = encode_array_element(element_type, item)?;
                bytes.extend_from_slice(&(payload.len() as i32).to_le_bytes());
                bytes.extend_from_slice(&payload);
            }
        }
    }
    Ok(bytes)
}

fn encode_array_element(element_type: SqlType, value: &Value) -> Result<Vec<u8>, ExecError> {
    let coerced = coerce_assignment_value(value, element_type)?;
    match coerced {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Bool(v) => Ok(vec![u8::from(v)]),
        Value::Numeric(text) => Ok(text.as_bytes().to_vec()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(coerced.as_text().unwrap().as_bytes().to_vec()),
        Value::Float64(v) => Ok(v.to_string().into_bytes()),
        Value::Array(_) => Err(ExecError::TypeMismatch {
            op: "array element",
            left: coerced,
            right: Value::Null,
        }),
    }
}

fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array payload too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array length header truncated".into(),
            });
        }
        let len = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len == -1 {
            items.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array element payload truncated".into(),
            });
        }
        items.push(decode_array_element(element_type, &bytes[offset..offset + len])?);
        offset += len;
    }
    Ok(Value::Array(items))
}

fn decode_array_element(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    match element_type.kind {
        SqlTypeKind::Int2 => {
            if bytes.len() != 2 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int2 array element must be 2 bytes".into(),
                });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int4 => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int4 array element must be 4 bytes".into(),
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int8 => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int8 array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
            if bytes.len() != if matches!(element_type.kind, SqlTypeKind::Float4) { 4 } else { 8 } {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "float array element has wrong width".into(),
                });
            }
            if matches!(element_type.kind, SqlTypeKind::Float4) {
                Ok(Value::Float64(f32::from_le_bytes(bytes.try_into().unwrap()) as f64))
            } else {
                Ok(Value::Float64(f64::from_le_bytes(bytes.try_into().unwrap())))
            }
        }
        SqlTypeKind::Numeric => {
            Ok(Value::Numeric(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
        SqlTypeKind::Bool => {
            if bytes.len() != 1 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "bool array element must be 1 byte".into(),
                });
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char | SqlTypeKind::Varchar => {
            Ok(Value::Text(CompactString::new(unsafe { std::str::from_utf8_unchecked(bytes) })))
        }
    }
}

pub(crate) fn format_array_text(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Int16(v) => out.push_str(&v.to_string()),
            Value::Int32(v) => out.push_str(&v.to_string()),
            Value::Int64(v) => out.push_str(&v.to_string()),
            Value::Float64(v) => out.push_str(&v.to_string()),
            Value::Numeric(v) => out.push_str(v.as_str()),
            Value::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
            Value::Text(_) | Value::TextRef(_, _) => {
                out.push('"');
                for ch in item.as_text().unwrap().chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Array(nested) => out.push_str(&format_array_text(nested)),
        }
    }
    out.push('}');
    out
}

fn numeric_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(*v as f64),
        Value::Int32(v) => Some(*v as f64),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}
