use super::*;
use crate::include::nodes::primnodes::{WindowClause, WindowFuncExpr, WindowFuncKind, WindowSpec};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct WindowBindingState {
    pub(super) clauses: Vec<WindowClause>,
    next_winno: usize,
}

#[derive(Clone)]
struct WindowBindScope {
    state: Rc<RefCell<WindowBindingState>>,
    allow_windows: bool,
}

thread_local! {
    // :HACK: Window binding spans a broad recursive binder surface today. Keep
    // the mutable per-query collection state thread-local until the remaining
    // expression binders grow an explicit statement-local context parameter.
    static WINDOW_BIND_STACK: RefCell<Vec<WindowBindScope>> = const { RefCell::new(Vec::new()) };
}

struct WindowBindGuard;

impl Drop for WindowBindGuard {
    fn drop(&mut self) {
        WINDOW_BIND_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

pub(super) fn with_window_binding<T>(
    state: Rc<RefCell<WindowBindingState>>,
    allow_windows: bool,
    f: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    WINDOW_BIND_STACK.with(|stack| {
        stack.borrow_mut().push(WindowBindScope {
            state,
            allow_windows,
        });
    });
    let _guard = WindowBindGuard;
    f()
}

pub(super) fn with_windows_disallowed<T>(
    f: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    match current_window_scope() {
        Some(scope) => with_window_binding(scope.state, false, f),
        None => f(),
    }
}

pub(super) fn windows_allowed() -> bool {
    current_window_scope().is_some_and(|scope| scope.allow_windows)
}

pub(super) fn current_window_state() -> Option<Rc<RefCell<WindowBindingState>>> {
    current_window_scope().map(|scope| scope.state)
}

fn current_window_scope() -> Option<WindowBindScope> {
    WINDOW_BIND_STACK.with(|stack| stack.borrow().last().cloned())
}

pub(super) fn take_window_clauses(state: &Rc<RefCell<WindowBindingState>>) -> Vec<WindowClause> {
    state.borrow().clauses.clone()
}

pub(super) fn nested_window_error() -> ParseError {
    ParseError::WindowingError("window function calls cannot be nested".into())
}

pub(super) fn window_not_allowed_error() -> ParseError {
    ParseError::WindowingError("window functions are not allowed in this context".into())
}

pub(super) fn window_function_requires_over_error(name: &str) -> ParseError {
    ParseError::WindowingError(format!("window function {name} requires an OVER clause"))
}

pub(super) fn reject_window_clause(expr: &SqlExpr, clause: &'static str) -> Result<(), ParseError> {
    if expr_contains_window(expr) {
        Err(ParseError::WindowingError(format!(
            "window functions are not allowed in {clause}"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn expr_contains_window(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::AggCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            over.is_some()
                || args.iter().any(|arg| expr_contains_window(&arg.value))
                || order_by.iter().any(|item| expr_contains_window(&item.expr))
                || filter.as_deref().is_some_and(expr_contains_window)
        }
        SqlExpr::FuncCall { args, over, .. } => {
            over.is_some() || args.iter().any(|arg| expr_contains_window(&arg.value))
        }
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(expr_contains_window)
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            expr_contains_window(expr)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            expr_contains_window(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_deref().is_some_and(expr_contains_window)
                        || subscript.upper.as_deref().is_some_and(expr_contains_window)
                })
        }
        SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::QuantifiedArray {
            left, array: right, ..
        }
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right) => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_window(expr)
                || expr_contains_window(pattern)
                || escape.as_deref().is_some_and(expr_contains_window)
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref().is_some_and(expr_contains_window)
                || args
                    .iter()
                    .any(|arm| expr_contains_window(&arm.expr) || expr_contains_window(&arm.result))
                || defresult.as_deref().is_some_and(expr_contains_window)
        }
        SqlExpr::Cast(inner, _)
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => expr_contains_window(inner),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
    }
}

pub(super) fn bind_window_spec(
    raw_spec: &RawWindowSpec,
    mut bind_expr: impl FnMut(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<WindowSpec, ParseError> {
    let partition_by = raw_spec
        .partition_by
        .iter()
        .map(|expr| {
            if expr_contains_window(expr) {
                return Err(nested_window_error());
            }
            bind_expr(expr)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = raw_spec
        .order_by
        .iter()
        .map(|item| {
            if expr_contains_window(&item.expr) {
                return Err(nested_window_error());
            }
            Ok(OrderByEntry {
                expr: bind_expr(&item.expr)?,
                ressortgroupref: 0,
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(WindowSpec {
        partition_by,
        order_by,
    })
}

pub(super) fn register_window_expr(
    state: &Rc<RefCell<WindowBindingState>>,
    spec: WindowSpec,
    kind: WindowFuncKind,
    args: Vec<Expr>,
    result_type: SqlType,
) -> Expr {
    let mut state = state.borrow_mut();
    let winref = match state.clauses.iter().position(|clause| clause.spec == spec) {
        Some(index) => index + 1,
        None => {
            state.clauses.push(WindowClause {
                spec: spec.clone(),
                functions: Vec::new(),
            });
            state.clauses.len()
        }
    };
    let winno = state.next_winno;
    state.next_winno += 1;
    let expr = WindowFuncExpr {
        kind,
        winref,
        winno,
        args,
        result_type,
    };
    state.clauses[winref - 1].functions.push(expr.clone());
    Expr::WindowFunc(Box::new(expr))
}
