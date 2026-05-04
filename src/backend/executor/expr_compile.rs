use super::exec_expr::eval_expr;
use super::node_types::*;
use super::{ExecError, ExecutorContext};

pub(crate) type CompiledPredicate =
    Box<dyn Fn(&mut TupleSlot, &mut ExecutorContext) -> Result<bool, ExecError>>;

impl pgrust_executor::PredicateSlot for TupleSlot {
    type Error = ExecError;

    fn get_fixed_int32(&self, data_offset: usize) -> Option<i32> {
        TupleSlot::get_fixed_int32(self, data_offset)
    }

    fn get_attr_value(&mut self, index: usize) -> Result<&Value, Self::Error> {
        self.get_attr(index)
    }
}

impl pgrust_executor::PredicateContext for ExecutorContext {
    fn expr_bindings(&self) -> &pgrust_executor::ExprEvalBindings {
        &self.expr_bindings
    }
}

impl From<pgrust_executor::PredicateEvalError> for ExecError {
    fn from(error: pgrust_executor::PredicateEvalError) -> Self {
        match error {
            pgrust_executor::PredicateEvalError::TypeMismatch { op, left, right } => {
                ExecError::TypeMismatch { op, left, right }
            }
            pgrust_executor::PredicateEvalError::NonBoolQual(value) => {
                ExecError::NonBoolQual(value)
            }
            pgrust_executor::PredicateEvalError::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            } => ExecError::DetailedError {
                message: message.into(),
                detail: detail.map(Into::into),
                hint: hint.map(Into::into),
                sqlstate,
            },
            pgrust_executor::PredicateEvalError::Expr(error) => error.into(),
        }
    }
}

pub(crate) fn compile_predicate_with_decoder(
    expr: &Expr,
    decoder: &super::tuple_decoder::CompiledTupleDecoder,
) -> CompiledPredicate {
    pgrust_executor::compile_fast_predicate_with_decoder::<TupleSlot, ExecutorContext, ExecError>(
        expr, decoder,
    )
    .unwrap_or_else(|| compile_fallback(expr))
}

pub(crate) fn compile_predicate(expr: &Expr) -> CompiledPredicate {
    pgrust_executor::compile_fast_predicate::<TupleSlot, ExecutorContext, ExecError>(expr)
        .unwrap_or_else(|| compile_fallback(expr))
}

fn compile_fallback(expr: &Expr) -> CompiledPredicate {
    let expr = expr.clone();
    Box::new(move |slot, ctx| match eval_expr(&expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    })
}
