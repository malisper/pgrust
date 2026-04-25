use super::exec_expr::eval_expr;
use super::node_types::*;
use super::pg_regex::{compile_pg_regex_predicate, pg_regex_is_match};
use super::{ExecError, ExecutorContext};
use crate::include::nodes::parsenodes::SqlTypeKind;
use crate::include::nodes::primnodes::{
    BoolExprType, OpExprKind, Var, attrno_index, is_special_varno,
};

pub(crate) type CompiledPredicate =
    Box<dyn Fn(&mut TupleSlot, &mut ExecutorContext) -> Result<bool, ExecError>>;

pub(crate) fn compile_predicate_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> CompiledPredicate {
    if let Some(pred) = try_compile_fixed_offset(expr, decoder) {
        return pred;
    }
    compile_predicate(expr)
}

fn try_compile_fixed_offset(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Option<CompiledPredicate> {
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Gt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let col = local_var_index(var)?;
                let (col, off, val) = (col, decoder.fixed_int32_offset(col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v > val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v > val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: ">",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Lt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let col = local_var_index(var)?;
                let (col, off, val) = (col, decoder.fixed_int32_offset(col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v < val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v < val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: "<",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Eq => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let col = local_var_index(var)?;
                let (col, off, val) = (col, decoder.fixed_int32_offset(col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v == val);
                    }
                    match slot.get_attr(col)? {
                        Value::Int32(v) => Ok(*v == val),
                        Value::Null => Ok(false),
                        other => Err(ExecError::TypeMismatch {
                            op: "=",
                            left: other.clone(),
                            right: Value::Int32(val),
                        }),
                    }
                }));
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let parts = flatten_and_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }));
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            let parts = flatten_or_with_decoder(expr, decoder);
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }));
        }
        _ => {}
    }
    None
}

fn flatten_and_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_and_with_decoder_inner(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate>,
) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            for arg in &bool_expr.args {
                flatten_and_with_decoder_inner(arg, decoder, out);
            }
        }
        _ => {
            out.push(compile_predicate_with_decoder(expr, decoder));
        }
    }
}

fn flatten_or_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_with_decoder_inner(expr, decoder, &mut out);
    out
}

fn flatten_or_with_decoder_inner(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate>,
) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            for arg in &bool_expr.args {
                flatten_or_with_decoder_inner(arg, decoder, out);
            }
        }
        _ => {
            out.push(compile_predicate_with_decoder(expr, decoder));
        }
    }
}

pub(crate) fn compile_predicate(expr: &Expr) -> CompiledPredicate {
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Gt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = match local_var_index(var) {
                    Some(col) => (col, *val),
                    None => return compile_fallback(expr),
                };
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v > val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: ">",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Lt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = match local_var_index(var) {
                    Some(col) => (col, *val),
                    None => return compile_fallback(expr),
                };
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v < val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: "<",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Eq => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = match local_var_index(var) {
                    Some(col) => (col, *val),
                    None => return compile_fallback(expr),
                };
                return Box::new(move |slot, _ctx| match slot.get_attr(col)? {
                    Value::Int32(v) => Ok(*v == val),
                    Value::Null => Ok(false),
                    other => Err(ExecError::TypeMismatch {
                        op: "=",
                        left: other.clone(),
                        right: Value::Int32(val),
                    }),
                });
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let parts = flatten_and(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if !part(slot, ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            });
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            let parts = flatten_or(expr);
            return Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            });
        }
        Expr::Op(op) if op.op == OpExprKind::RegexMatch => {
            if let [Expr::Var(var), pattern] = op.args.as_slice() {
                let col = match local_var_index(var) {
                    Some(col) => col,
                    None => return compile_fallback(expr),
                };
                if let Some(pattern) = const_text_pattern(pattern)
                    && let Ok(regex) = compile_pg_regex_predicate(pattern)
                {
                    let regex = std::sync::Arc::new(regex);
                    return Box::new(move |slot, _ctx| {
                        let val = slot.get_attr(col)?;
                        if let Some(s) = val.as_text() {
                            pg_regex_is_match(&regex, s)
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
        }
        _ => {}
    }

    compile_fallback(expr)
}

fn compile_fallback(expr: &Expr) -> CompiledPredicate {
    let expr = expr.clone();
    Box::new(move |slot, ctx| match eval_expr(&expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    })
}

fn const_text_pattern(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Const(value) => value.as_text(),
        Expr::Cast(inner, ty) if ty.kind == SqlTypeKind::Text && !ty.is_array => {
            const_text_pattern(inner)
        }
        Expr::Collate { expr, .. } => const_text_pattern(expr),
        _ => None,
    }
}

fn flatten_and(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_and_inner(expr, &mut out);
    out
}

fn flatten_and_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            for arg in &bool_expr.args {
                flatten_and_inner(arg, out);
            }
        }
        _ => {
            out.push(compile_predicate(expr));
        }
    }
}

fn local_var_index(var: &Var) -> Option<usize> {
    (var.varlevelsup == 0 && !is_special_varno(var.varno))
        .then_some(())
        .and_then(|_| attrno_index(var.varattno))
}

fn flatten_or(expr: &Expr) -> Vec<CompiledPredicate> {
    let mut out = Vec::new();
    flatten_or_inner(expr, &mut out);
    out
}

fn flatten_or_inner(expr: &Expr, out: &mut Vec<CompiledPredicate>) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            for arg in &bool_expr.args {
                flatten_or_inner(arg, out);
            }
        }
        _ => {
            out.push(compile_predicate(expr));
        }
    }
}
