use pgrust_nodes::Value;
use pgrust_nodes::parsenodes::SqlTypeKind;
use pgrust_nodes::primnodes::{
    BoolExprType, Expr, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExprKind, RULE_NEW_VAR, RULE_OLD_VAR,
    Var, attrno_index, is_special_varno,
};

use crate::{CompiledTupleDecoder, ExprEvalBindings};

pub type CompiledPredicate<S, C, E> = Box<dyn Fn(&mut S, &mut C) -> Result<bool, E>>;

pub trait PredicateSlot {
    type Error;

    fn get_fixed_int32(&self, data_offset: usize) -> Option<i32>;
    fn get_attr_value(&mut self, index: usize) -> Result<&Value, Self::Error>;
}

pub trait PredicateContext {
    fn expr_bindings(&self) -> &ExprEvalBindings;
}

#[derive(Debug, Clone)]
pub enum PredicateEvalError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    NonBoolQual(Value),
    DetailedError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    Expr(pgrust_expr::ExprError),
}

impl From<pgrust_expr::ExprError> for PredicateEvalError {
    fn from(error: pgrust_expr::ExprError) -> Self {
        Self::Expr(error)
    }
}

pub fn compile_fast_predicate_with_decoder<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
) -> Option<CompiledPredicate<S, C, E>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    try_compile_fixed_offset(expr, decoder)
}

pub fn compile_fast_predicate<S, C, E>(expr: &Expr) -> Option<CompiledPredicate<S, C, E>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    compile_fast_predicate_inner(expr)
}

fn try_compile_fixed_offset<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
) -> Option<CompiledPredicate<S, C, E>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Gt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let col = local_var_index(var)?;
                let (col, off, val) = (col, decoder.fixed_int32_offset(col)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    if let Some(v) = slot.get_fixed_int32(off) {
                        return Ok(v > val);
                    }
                    int32_attr_cmp(slot, col, ">", Value::Int32(val), |left| left > val)
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
                    int32_attr_cmp(slot, col, "<", Value::Int32(val), |left| left < val)
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
                    int32_attr_cmp(slot, col, "=", Value::Int32(val), |left| left == val)
                }));
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let parts = flatten_and_with_decoder::<S, C, E>(expr, decoder)?;
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
            let parts = flatten_or_with_decoder::<S, C, E>(expr, decoder)?;
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

fn compile_fast_predicate_inner<S, C, E>(expr: &Expr) -> Option<CompiledPredicate<S, C, E>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Gt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = (local_var_index(var)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    int32_attr_cmp(slot, col, ">", Value::Int32(val), |left| left > val)
                }));
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Lt => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = (local_var_index(var)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    int32_attr_cmp(slot, col, "<", Value::Int32(val), |left| left < val)
                }));
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Eq => {
            if let [Expr::Var(var), Expr::Const(Value::Int32(val))] = op.args.as_slice() {
                let (col, val) = (local_var_index(var)?, *val);
                return Some(Box::new(move |slot, _ctx| {
                    int32_attr_cmp(slot, col, "=", Value::Int32(val), |left| left == val)
                }));
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let parts = flatten_and::<S, C, E>(expr)?;
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
            let parts = flatten_or::<S, C, E>(expr)?;
            return Some(Box::new(move |slot, ctx| {
                for part in &parts {
                    if part(slot, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }));
        }
        Expr::Op(op) if op.op == OpExprKind::RegexMatch => {
            if let Some((source, pat)) = regex_match_fast_path_parts(&op.args) {
                if let Ok(regex) = pgrust_expr::pg_regex::compile_pg_regex_predicate(pat) {
                    let regex = std::sync::Arc::new(regex);
                    return Some(Box::new(move |slot, ctx| {
                        let val = fast_var_value::<S, C, E>(source, slot, ctx)?;
                        if let Some(s) = val.as_text() {
                            pgrust_expr::pg_regex::pg_regex_is_match(&regex, s)
                                .map_err(PredicateEvalError::from)
                                .map_err(Into::into)
                        } else if matches!(val, Value::Null) {
                            Ok(false)
                        } else {
                            Err(PredicateEvalError::TypeMismatch {
                                op: "~",
                                left: val.clone(),
                                right: Value::Null,
                            }
                            .into())
                        }
                    }));
                }
            }
        }
        _ => {}
    }

    None
}

fn int32_attr_cmp<S, E>(
    slot: &mut S,
    col: usize,
    op: &'static str,
    right: Value,
    cmp: impl FnOnce(i32) -> bool,
) -> Result<bool, E>
where
    S: PredicateSlot,
    E: From<PredicateEvalError> + From<S::Error>,
{
    match slot.get_attr_value(col)? {
        Value::Int32(v) => Ok(cmp(*v)),
        Value::Null => Ok(false),
        other => Err(PredicateEvalError::TypeMismatch {
            op,
            left: other.clone(),
            right,
        }
        .into()),
    }
}

fn flatten_and_with_decoder<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
) -> Option<Vec<CompiledPredicate<S, C, E>>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    let mut out = Vec::new();
    flatten_and_with_decoder_inner(expr, decoder, &mut out).then_some(out)
}

fn flatten_and_with_decoder_inner<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate<S, C, E>>,
) -> bool
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            for arg in &bool_expr.args {
                if !flatten_and_with_decoder_inner(arg, decoder, out) {
                    return false;
                }
            }
            true
        }
        _ => {
            if let Some(predicate) = compile_fast_predicate_with_decoder(expr, decoder) {
                out.push(predicate);
                true
            } else if let Some(predicate) = compile_fast_predicate(expr) {
                out.push(predicate);
                true
            } else {
                false
            }
        }
    }
}

fn flatten_or_with_decoder<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
) -> Option<Vec<CompiledPredicate<S, C, E>>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    let mut out = Vec::new();
    flatten_or_with_decoder_inner(expr, decoder, &mut out).then_some(out)
}

fn flatten_or_with_decoder_inner<S, C, E>(
    expr: &Expr,
    decoder: &CompiledTupleDecoder,
    out: &mut Vec<CompiledPredicate<S, C, E>>,
) -> bool
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            for arg in &bool_expr.args {
                if !flatten_or_with_decoder_inner(arg, decoder, out) {
                    return false;
                }
            }
            true
        }
        _ => {
            if let Some(predicate) = compile_fast_predicate_with_decoder(expr, decoder) {
                out.push(predicate);
                true
            } else if let Some(predicate) = compile_fast_predicate(expr) {
                out.push(predicate);
                true
            } else {
                false
            }
        }
    }
}

fn flatten_and<S, C, E>(expr: &Expr) -> Option<Vec<CompiledPredicate<S, C, E>>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    let mut out = Vec::new();
    flatten_and_inner(expr, &mut out).then_some(out)
}

fn flatten_and_inner<S, C, E>(expr: &Expr, out: &mut Vec<CompiledPredicate<S, C, E>>) -> bool
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            for arg in &bool_expr.args {
                if !flatten_and_inner(arg, out) {
                    return false;
                }
            }
            true
        }
        _ => {
            if let Some(predicate) = compile_fast_predicate(expr) {
                out.push(predicate);
                true
            } else {
                false
            }
        }
    }
}

fn flatten_or<S, C, E>(expr: &Expr) -> Option<Vec<CompiledPredicate<S, C, E>>>
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    let mut out = Vec::new();
    flatten_or_inner(expr, &mut out).then_some(out)
}

fn flatten_or_inner<S, C, E>(expr: &Expr, out: &mut Vec<CompiledPredicate<S, C, E>>) -> bool
where
    S: PredicateSlot + 'static,
    C: PredicateContext + 'static,
    E: From<PredicateEvalError> + From<S::Error> + 'static,
{
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            for arg in &bool_expr.args {
                if !flatten_or_inner(arg, out) {
                    return false;
                }
            }
            true
        }
        _ => {
            if let Some(predicate) = compile_fast_predicate(expr) {
                out.push(predicate);
                true
            } else {
                false
            }
        }
    }
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

fn fast_var_value<'a, S, C, E>(
    source: FastVarSource,
    slot: &'a mut S,
    ctx: &'a C,
) -> Result<&'a Value, E>
where
    S: PredicateSlot,
    C: PredicateContext,
    E: From<PredicateEvalError> + From<S::Error>,
{
    match source {
        FastVarSource::Slot(index) => slot.get_attr_value(index).map_err(Into::into),
        FastVarSource::Outer(index) => {
            bound_tuple_value("outer", ctx.expr_bindings().outer_tuple.as_ref(), index)
        }
        FastVarSource::Inner(index) => {
            bound_tuple_value("inner", ctx.expr_bindings().inner_tuple.as_ref(), index)
        }
        FastVarSource::Index(index) => {
            bound_tuple_value("index", ctx.expr_bindings().index_tuple.as_ref(), index)
        }
        FastVarSource::RuleOld(index) => bound_tuple_value(
            "rule old",
            ctx.expr_bindings().rule_old_tuple.as_ref(),
            index,
        ),
        FastVarSource::RuleNew(index) => bound_tuple_value(
            "rule new",
            ctx.expr_bindings().rule_new_tuple.as_ref(),
            index,
        ),
    }
}

fn bound_tuple_value<'a, E>(
    binding: &str,
    tuple: Option<&'a Vec<Value>>,
    index: usize,
) -> Result<&'a Value, E>
where
    E: From<PredicateEvalError>,
{
    let row = tuple.ok_or_else(|| PredicateEvalError::DetailedError {
        message: format!("compiled regex predicate referenced missing {binding} tuple"),
        detail: Some(format!("index={index}")),
        hint: None,
        sqlstate: "XX000",
    })?;
    row.get(index)
        .ok_or_else(|| PredicateEvalError::DetailedError {
            message: format!("compiled regex predicate referenced beyond {binding} tuple width"),
            detail: Some(format!("index={index}, tuple_width={}", row.len())),
            hint: None,
            sqlstate: "XX000",
        })
        .map_err(Into::into)
}

fn local_var_index(var: &Var) -> Option<usize> {
    (var.varlevelsup == 0 && !is_special_varno(var.varno))
        .then_some(())
        .and_then(|_| attrno_index(var.varattno))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::SqlType;
    use pgrust_nodes::primnodes::user_attrno;

    fn text_var(index: usize) -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(index),
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
            collation_oid: None,
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
                    collation_oid: None,
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
