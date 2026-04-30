use super::exec_expr::eval_expr;
use super::node_types::*;
use super::pg_regex::{compile_pg_regex_predicate, pg_regex_is_match};
use super::{ExecError, ExecutorContext};
use crate::include::nodes::parsenodes::SqlTypeKind;
use crate::include::nodes::primnodes::{
    BoolExprType, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExprKind, RULE_NEW_VAR, RULE_OLD_VAR, Var,
    attrno_index, is_special_varno,
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
            if let Some((source, pat)) = regex_match_fast_path_parts(&op.args) {
                if let Ok(regex) = compile_pg_regex_predicate(pat) {
                    let regex = std::sync::Arc::new(regex);
                    return Box::new(move |slot, ctx| {
                        let val = fast_var_value(source, slot, ctx)?;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FastVarSource {
    Slot(usize),
    Outer(usize),
    Inner(usize),
    Index(usize),
    RuleOld(usize),
    RuleNew(usize),
}

fn regex_match_fast_path_parts(args: &[Expr]) -> Option<(FastVarSource, &str)> {
    let [text, pattern] = args else {
        return None;
    };
    let source = text_var_source(text)?;
    let pattern = const_text_pattern(pattern)?;
    Some((source, pattern))
}

fn text_var_source(expr: &Expr) -> Option<FastVarSource> {
    match expr {
        Expr::Var(var) => fast_var_source(var),
        Expr::Cast(inner, ty) if ty.kind == SqlTypeKind::Text && !ty.is_array => {
            text_var_source(inner)
        }
        Expr::Collate { expr, .. } => text_var_source(expr),
        _ => None,
    }
}

fn fast_var_source(var: &Var) -> Option<FastVarSource> {
    if var.varlevelsup != 0 {
        return None;
    }
    let index = attrno_index(var.varattno)?;
    match var.varno {
        OUTER_VAR => Some(FastVarSource::Outer(index)),
        INNER_VAR => Some(FastVarSource::Inner(index)),
        INDEX_VAR => Some(FastVarSource::Index(index)),
        RULE_OLD_VAR => Some(FastVarSource::RuleOld(index)),
        RULE_NEW_VAR => Some(FastVarSource::RuleNew(index)),
        varno if !is_special_varno(varno) => Some(FastVarSource::Slot(index)),
        _ => None,
    }
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

fn fast_var_value<'a>(
    source: FastVarSource,
    slot: &'a mut TupleSlot,
    ctx: &'a ExecutorContext,
) -> Result<&'a Value, ExecError> {
    match source {
        FastVarSource::Slot(index) => slot.get_attr(index),
        FastVarSource::Outer(index) => {
            bound_tuple_value("outer", ctx.expr_bindings.outer_tuple.as_ref(), index)
        }
        FastVarSource::Inner(index) => {
            bound_tuple_value("inner", ctx.expr_bindings.inner_tuple.as_ref(), index)
        }
        FastVarSource::Index(index) => {
            bound_tuple_value("index", ctx.expr_bindings.index_tuple.as_ref(), index)
        }
        FastVarSource::RuleOld(index) => {
            bound_tuple_value("rule old", ctx.expr_bindings.rule_old_tuple.as_ref(), index)
        }
        FastVarSource::RuleNew(index) => {
            bound_tuple_value("rule new", ctx.expr_bindings.rule_new_tuple.as_ref(), index)
        }
    }
}

fn bound_tuple_value<'a>(
    binding: &str,
    tuple: Option<&'a Vec<Value>>,
    index: usize,
) -> Result<&'a Value, ExecError> {
    let row = tuple.ok_or_else(|| ExecError::DetailedError {
        message: format!("compiled regex predicate referenced missing {binding} tuple"),
        detail: Some(format!("index={index}")),
        hint: None,
        sqlstate: "XX000",
    })?;
    row.get(index).ok_or_else(|| ExecError::DetailedError {
        message: format!("compiled regex predicate referenced beyond {binding} tuple width"),
        detail: Some(format!("index={index}, tuple_width={}", row.len())),
        hint: None,
        sqlstate: "XX000",
    })
}

fn compile_fallback(expr: &Expr) -> CompiledPredicate {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::primnodes::user_attrno;

    fn text_var(index: usize) -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(index),
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
        })
    }

    #[test]
    fn regex_fast_path_accepts_casted_text_constant_pattern() {
        let expr = Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                text_var(0),
                Expr::Cast(
                    Box::new(Expr::Const(Value::Text("I- .*".into()))),
                    SqlType::new(SqlTypeKind::Text),
                ),
            ],
        );
        let Expr::Op(op) = expr else {
            panic!("expected regex operator expression");
        };

        assert_eq!(
            regex_match_fast_path_parts(&op.args),
            Some((FastVarSource::Slot(0), "I- .*"))
        );
    }

    #[test]
    fn regex_fast_path_accepts_casted_text_var() {
        let expr = Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                Expr::Cast(Box::new(text_var(0)), SqlType::new(SqlTypeKind::Text)),
                Expr::Const(Value::Text("I- .*".into())),
            ],
        );
        let Expr::Op(op) = expr else {
            panic!("expected regex operator expression");
        };

        assert_eq!(
            regex_match_fast_path_parts(&op.args),
            Some((FastVarSource::Slot(0), "I- .*"))
        );
    }

    #[test]
    fn regex_fast_path_accepts_outer_text_var() {
        let expr = Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                Expr::Var(Var {
                    varno: OUTER_VAR,
                    varattno: user_attrno(0),
                    varlevelsup: 0,
                    vartype: SqlType::new(SqlTypeKind::Text),
                }),
                Expr::Const(Value::Text("I- .*".into())),
            ],
        );
        let Expr::Op(op) = expr else {
            panic!("expected regex operator expression");
        };

        assert_eq!(
            regex_match_fast_path_parts(&op.args),
            Some((FastVarSource::Outer(0), "I- .*"))
        );
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
