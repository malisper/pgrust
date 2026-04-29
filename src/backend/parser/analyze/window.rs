use super::*;
use crate::include::nodes::parsenodes::{
    RawWindowFrame, RawWindowFrameBound, WindowFrameExclusion, WindowFrameMode, WindowNullTreatment,
};
use crate::include::nodes::primnodes::{
    BuiltinWindowFunction, WindowClause, WindowFrame, WindowFrameBound, WindowFrameOffset,
    WindowFuncExpr, WindowFuncKind, WindowSpec, expr_sql_type_hint,
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
        SqlExpr::Xml(xml) => xml.child_exprs().any(expr_contains_window),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .iter()
            .any(|expr| expr_contains_window(expr)),
        SqlExpr::Column(_)
        | SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
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
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
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
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
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
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        } => expr_contains_window(left) || expr_contains_window(right),
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
        | SqlExpr::Collate { expr: inner, .. }
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
    catalog: &dyn CatalogLookup,
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
            catalog,
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
            let bound_expr = bind_expr(&item.expr)?;
            build_bound_order_by_entry(item, bound_expr, 0, catalog)
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
    frame_mode: WindowFrameMode,
    order_by: &[OrderByEntry],
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
            let bound_expr = with_windows_disallowed(|| bind_expr(expr))?;
            WindowFrameBound::OffsetPreceding(bind_window_frame_offset(
                bound_expr, frame_mode, order_by,
            )?)
        }
        RawWindowFrameBound::OffsetFollowing(expr) => {
            if expr_contains_window(expr) {
                return Err(nested_window_error());
            }
            let bound_expr = with_windows_disallowed(|| bind_expr(expr))?;
            WindowFrameBound::OffsetFollowing(bind_window_frame_offset(
                bound_expr, frame_mode, order_by,
            )?)
        }
    })
}

fn bind_window_frame_offset(
    expr: Expr,
    frame_mode: WindowFrameMode,
    order_by: &[OrderByEntry],
) -> Result<WindowFrameOffset, ParseError> {
    if frame_mode != WindowFrameMode::Range {
        if expr_contains_current_level_var(&expr, 0) {
            let construct_name = match frame_mode {
                WindowFrameMode::Rows => "ROWS",
                WindowFrameMode::Groups => "GROUPS",
                WindowFrameMode::Range => unreachable!("range handled above"),
            };
            return Err(ParseError::DetailedError {
                message: format!("argument of {construct_name} must not contain variables"),
                detail: None,
                hint: None,
                sqlstate: "42P10",
            });
        }
        let target_type = SqlType::new(SqlTypeKind::Int8);
        let offset_type = expr_sql_type_hint(&expr).unwrap_or(target_type);
        let expr = if offset_type == target_type {
            expr
        } else {
            Expr::Cast(Box::new(expr), target_type)
        };
        return Ok(WindowFrameOffset::rows_or_groups(expr));
    }

    let order_type = order_by
        .first()
        .and_then(|item| expr_sql_type_hint(&item.expr))
        .unwrap_or(SqlType::new(SqlTypeKind::Text));
    let offset_type = expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let target_type = range_offset_target_type(order_type, offset_type, &expr)?;
    let expr = if offset_type == target_type {
        expr
    } else {
        Expr::Cast(Box::new(expr), target_type)
    };
    Ok(WindowFrameOffset {
        expr,
        offset_type: target_type,
        in_range_func: None,
    })
}

fn expr_contains_current_level_var(expr: &Expr, sublevels_up: usize) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == sublevels_up,
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || aggref
                    .args
                    .iter()
                    .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_contains_current_level_var(&entry.expr, sublevels_up))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_current_level_var(&arm.expr, sublevels_up)
                        || expr_contains_current_level_var(&arm.result, sublevels_up)
                })
                || expr_contains_current_level_var(&case_expr.defresult, sublevels_up)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::SetReturning(set_returning) => {
            crate::include::nodes::primnodes::set_returning_call_exprs(&set_returning.call)
                .into_iter()
                .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
        }
        Expr::SubLink(sublink) => {
            sublink
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || query_contains_current_level_var(&sublink.subselect, sublevels_up + 1)
        }
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || subplan
                    .args
                    .iter()
                    .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
        }
        Expr::ScalarArrayOp(op) => {
            expr_contains_current_level_var(&op.left, sublevels_up)
                || expr_contains_current_level_var(&op.right, sublevels_up)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            expr_contains_current_level_var(inner, sublevels_up)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_current_level_var(expr, sublevels_up)
                || expr_contains_current_level_var(pattern, sublevels_up)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_current_level_var(left, sublevels_up)
                || expr_contains_current_level_var(right, sublevels_up)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_current_level_var(expr, sublevels_up)),
        Expr::Coalesce(left, right) => {
            expr_contains_current_level_var(left, sublevels_up)
                || expr_contains_current_level_var(right, sublevels_up)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_current_level_var(array, sublevels_up)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
                })
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn query_contains_current_level_var(
    query: &crate::backend::parser::Query,
    sublevels_up: usize,
) -> bool {
    query
        .target_list
        .iter()
        .any(|target| expr_contains_current_level_var(&target.expr, sublevels_up))
        || query
            .where_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
        || query
            .group_by
            .iter()
            .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
        || query.accumulators.iter().any(|accum| {
            accum
                .args
                .iter()
                .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || accum
                    .filter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
        })
        || query.window_clauses.iter().any(|clause| {
            clause
                .spec
                .partition_by
                .iter()
                .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
                || clause
                    .spec
                    .order_by
                    .iter()
                    .any(|entry| expr_contains_current_level_var(&entry.expr, sublevels_up))
                || clause.functions.iter().any(|func| {
                    func.args
                        .iter()
                        .any(|expr| expr_contains_current_level_var(expr, sublevels_up))
                })
        })
        || query
            .having_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_current_level_var(expr, sublevels_up))
        || query
            .sort_clause
            .iter()
            .any(|clause| expr_contains_current_level_var(&clause.expr, sublevels_up))
}

fn range_offset_target_type(
    order_type: SqlType,
    offset_type: SqlType,
    offset_expr: &Expr,
) -> Result<SqlType, ParseError> {
    let is_text_const = matches!(offset_expr, Expr::Const(Value::Text(_)));
    let is_numeric_const = matches!(offset_expr, Expr::Const(Value::Numeric(_)));
    let target = match order_type.kind {
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 => {
            if matches!(
                offset_type.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
            ) || is_text_const
            {
                SqlType::new(SqlTypeKind::Int8)
            } else {
                return Err(range_offset_pair_error(order_type, offset_type));
            }
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
            if matches!(
                offset_type.kind,
                SqlTypeKind::Int2
                    | SqlTypeKind::Int4
                    | SqlTypeKind::Int8
                    | SqlTypeKind::Float4
                    | SqlTypeKind::Float8
            ) || is_text_const
                || is_numeric_const
            {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                return Err(range_offset_pair_error(order_type, offset_type));
            }
        }
        SqlTypeKind::Numeric => {
            if matches!(
                offset_type.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
            ) || is_text_const
            {
                SqlType::new(SqlTypeKind::Numeric)
            } else {
                return Err(range_offset_pair_error(order_type, offset_type));
            }
        }
        SqlTypeKind::Date
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Interval => {
            if offset_type.kind == SqlTypeKind::Interval
                || matches!(offset_expr, Expr::Const(Value::Text(_)))
            {
                SqlType::new(SqlTypeKind::Interval)
            } else {
                return Err(range_offset_pair_error(order_type, offset_type));
            }
        }
        _ => {
            return Err(ParseError::WindowingError(format!(
                "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
                range_offset_error_type_name(order_type)
            )));
        }
    };
    Ok(target)
}

fn range_offset_pair_error(order_type: SqlType, offset_type: SqlType) -> ParseError {
    ParseError::WindowingError(format!(
        "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {} and offset type {}",
        range_offset_error_type_name(order_type),
        range_offset_error_type_name(offset_type)
    ))
}

fn range_offset_error_type_name(sql_type: SqlType) -> String {
    match sql_type.kind {
        SqlTypeKind::Varchar | SqlTypeKind::Char => "text".into(),
        _ => super::coerce::sql_type_name(sql_type),
    }
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
            exclusion: WindowFrameExclusion::NoOthers,
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
        start_bound: bind_window_frame_bound(
            &raw_frame.start_bound,
            raw_frame.mode,
            order_by,
            bind_expr,
        )?,
        end_bound: bind_window_frame_bound(
            &raw_frame.end_bound,
            raw_frame.mode,
            order_by,
            bind_expr,
        )?,
        exclusion: raw_frame.exclusion,
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
    ignore_nulls: bool,
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
    if let Some(existing) = state.clauses[winref - 1].functions.iter().find(|func| {
        func.kind == kind
            && func.args == args
            && func.result_type == result_type
            && func.ignore_nulls == ignore_nulls
    }) {
        return Expr::WindowFunc(Box::new(existing.clone()));
    }
    let winno = state.next_winno;
    state.next_winno += 1;
    let expr = WindowFuncExpr {
        kind,
        winref,
        winno,
        args,
        result_type,
        ignore_nulls,
    };
    state.clauses[winref - 1].functions.push(expr.clone());
    Expr::WindowFunc(Box::new(expr))
}

pub(super) fn reject_aggregate_null_treatment(
    null_treatment: Option<WindowNullTreatment>,
) -> Result<(), ParseError> {
    if null_treatment.is_some() {
        return Err(ParseError::DetailedError {
            message: "aggregate functions do not accept RESPECT/IGNORE NULLS".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

pub(super) fn reject_function_null_treatment(
    name: &str,
    null_treatment: Option<WindowNullTreatment>,
) -> Result<(), ParseError> {
    if null_treatment.is_some() {
        return Err(function_null_treatment_error(name));
    }
    Ok(())
}

pub(super) fn window_ignore_nulls_for_builtin(
    func: BuiltinWindowFunction,
    null_treatment: Option<WindowNullTreatment>,
) -> Result<bool, ParseError> {
    let Some(null_treatment) = null_treatment else {
        return Ok(false);
    };
    if !matches!(
        func,
        BuiltinWindowFunction::Lag
            | BuiltinWindowFunction::Lead
            | BuiltinWindowFunction::FirstValue
            | BuiltinWindowFunction::LastValue
            | BuiltinWindowFunction::NthValue
    ) {
        return Err(function_null_treatment_error(func.name()));
    }
    Ok(matches!(null_treatment, WindowNullTreatment::Ignore))
}

fn function_null_treatment_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("function {name} does not allow RESPECT/IGNORE NULLS"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}
