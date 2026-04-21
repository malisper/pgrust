use super::*;
use crate::include::nodes::parsenodes::{RawWindowFrame, RawWindowFrameBound, WindowFrameMode};
use crate::include::nodes::primnodes::{
    WindowClause, WindowFrame, WindowFrameBound, WindowFuncExpr, WindowFuncKind, WindowSpec,
};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct WindowBindingState {
    pub(super) clauses: Vec<WindowClause>,
    named_specs: Vec<RawWindowClause>,
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

pub(super) fn register_named_window_specs(
    state: &Rc<RefCell<WindowBindingState>>,
    clauses: &[RawWindowClause],
) -> Result<(), ParseError> {
    let mut state = state.borrow_mut();
    for clause in clauses {
        if state
            .named_specs
            .iter()
            .any(|existing| existing.name == clause.name)
        {
            return Err(ParseError::WindowingError(format!(
                "window \"{}\" is already defined",
                clause.name
            )));
        }
        state.named_specs.push(clause.clone());
    }
    Ok(())
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
        SqlExpr::FuncCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            over.is_some()
                || args
                    .args()
                    .iter()
                    .any(|arg| expr_contains_window(&arg.value))
                || order_by.iter().any(|item| expr_contains_window(&item.expr))
                || filter.as_deref().is_some_and(expr_contains_window)
        }
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
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
    let is_bare_named_ref = raw_spec.name.is_some()
        && raw_spec.partition_by.is_empty()
        && raw_spec.order_by.is_empty()
        && raw_spec.frame.is_none();
    let inherited = if let Some(name) = raw_spec.name.as_ref() {
        let state = current_window_state().ok_or_else(window_not_allowed_error)?;
        let named = state
            .borrow()
            .named_specs
            .iter()
            .find(|clause| clause.name == *name)
            .map(|clause| clause.spec.clone())
            .ok_or_else(|| {
                ParseError::WindowingError(format!("window \"{name}\" does not exist"))
            })?;
        Some(named)
    } else {
        None
    };

    if is_bare_named_ref {
        return bind_window_spec(
            inherited.as_ref().expect("resolved named window"),
            bind_expr,
        );
    }

    if inherited.is_some() && !raw_spec.partition_by.is_empty() {
        return Err(ParseError::WindowingError(format!(
            "cannot override PARTITION BY clause of window \"{}\"",
            raw_spec.name.as_deref().unwrap_or_default()
        )));
    }

    let partition_source = inherited
        .as_ref()
        .map(|spec| spec.partition_by.as_slice())
        .unwrap_or(raw_spec.partition_by.as_slice());
    let partition_by = partition_source
        .iter()
        .map(|expr| {
            if expr_contains_window(expr) {
                return Err(nested_window_error());
            }
            bind_expr(expr)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if inherited.is_some()
        && !raw_spec.order_by.is_empty()
        && inherited
            .as_ref()
            .is_some_and(|spec| !spec.order_by.is_empty())
    {
        return Err(ParseError::WindowingError(format!(
            "cannot override ORDER BY clause of window \"{}\"",
            raw_spec.name.as_deref().unwrap_or_default()
        )));
    }

    let order_source = if raw_spec.order_by.is_empty() {
        inherited
            .as_ref()
            .map(|spec| spec.order_by.as_slice())
            .unwrap_or(&[])
    } else {
        raw_spec.order_by.as_slice()
    };
    let order_by = order_source
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

    if inherited
        .as_ref()
        .and_then(|spec| spec.frame.as_ref())
        .is_some()
    {
        return Err(ParseError::WindowingError(format!(
            "cannot copy window \"{}\" because it has a frame clause",
            raw_spec.name.as_deref().unwrap_or_default()
        )));
    }

    let frame = bind_window_frame(raw_spec.frame.as_deref(), &order_by, &mut bind_expr)?;
    Ok(WindowSpec {
        partition_by,
        order_by,
        frame,
    })
}

fn bind_window_frame_bound(
    raw_bound: &RawWindowFrameBound,
    bind_expr: &mut impl FnMut(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<WindowFrameBound, ParseError> {
    Ok(match raw_bound {
        RawWindowFrameBound::UnboundedPreceding => WindowFrameBound::UnboundedPreceding,
        RawWindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        RawWindowFrameBound::UnboundedFollowing => WindowFrameBound::UnboundedFollowing,
        RawWindowFrameBound::OffsetPreceding(expr) => {
            if expr_contains_window(expr) {
                return Err(nested_window_error());
            }
            WindowFrameBound::OffsetPreceding(with_windows_disallowed(|| bind_expr(expr))?)
        }
        RawWindowFrameBound::OffsetFollowing(expr) => {
            if expr_contains_window(expr) {
                return Err(nested_window_error());
            }
            WindowFrameBound::OffsetFollowing(with_windows_disallowed(|| bind_expr(expr))?)
        }
    })
}

fn bind_window_frame(
    raw_frame: Option<&RawWindowFrame>,
    order_by: &[OrderByEntry],
    bind_expr: &mut impl FnMut(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<WindowFrame, ParseError> {
    let Some(raw_frame) = raw_frame else {
        return Ok(WindowFrame {
            mode: WindowFrameMode::Range,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
        });
    };

    if raw_frame.mode == WindowFrameMode::Groups && order_by.is_empty() {
        return Err(ParseError::WindowingError(
            "GROUPS mode requires an ORDER BY clause".into(),
        ));
    }
    if raw_frame.mode == WindowFrameMode::Range
        && (matches!(
            raw_frame.start_bound,
            RawWindowFrameBound::OffsetPreceding(_) | RawWindowFrameBound::OffsetFollowing(_)
        ) || matches!(
            raw_frame.end_bound,
            RawWindowFrameBound::OffsetPreceding(_) | RawWindowFrameBound::OffsetFollowing(_)
        ))
        && order_by.len() != 1
    {
        return Err(ParseError::WindowingError(
            "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column".into(),
        ));
    }

    let frame = WindowFrame {
        mode: raw_frame.mode,
        start_bound: bind_window_frame_bound(&raw_frame.start_bound, bind_expr)?,
        end_bound: bind_window_frame_bound(&raw_frame.end_bound, bind_expr)?,
    };
    validate_window_frame(&frame)?;
    Ok(frame)
}

fn validate_window_frame(frame: &WindowFrame) -> Result<(), ParseError> {
    if matches!(frame.start_bound, WindowFrameBound::UnboundedFollowing) {
        return Err(ParseError::WindowingError(
            "frame start cannot be UNBOUNDED FOLLOWING".into(),
        ));
    }
    if matches!(frame.end_bound, WindowFrameBound::UnboundedPreceding) {
        return Err(ParseError::WindowingError(
            "frame end cannot be UNBOUNDED PRECEDING".into(),
        ));
    }
    if matches!(
        (&frame.start_bound, &frame.end_bound),
        (
            WindowFrameBound::CurrentRow,
            WindowFrameBound::OffsetPreceding(_)
        ) | (
            WindowFrameBound::OffsetFollowing(_),
            WindowFrameBound::CurrentRow
        ) | (
            WindowFrameBound::OffsetFollowing(_),
            WindowFrameBound::OffsetPreceding(_)
        ) | (WindowFrameBound::UnboundedFollowing, _)
            | (_, WindowFrameBound::UnboundedPreceding)
    ) {
        return Err(ParseError::WindowingError(
            "frame starting from following row cannot have preceding rows".into(),
        ));
    }
    Ok(())
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
