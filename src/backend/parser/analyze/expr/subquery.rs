use super::*;

fn child_outer_scopes(scope: &BoundScope, outer_scopes: &[BoundScope]) -> Vec<BoundScope> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    child_outer
}

pub(super) fn bind_scalar_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    let plan = build_plan_with_outer(select, catalog, &child_outer, None, ctes, &[])?;
    ensure_single_column_subquery(&plan)?;
    Ok(Expr::ScalarSubquery(Box::new(plan)))
}

pub(super) fn bind_exists_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    Ok(Expr::ExistsSubquery(Box::new(build_plan_with_outer(
        select,
        catalog,
        &child_outer,
        None,
        ctes,
        &[],
    )?)))
}

pub(super) fn bind_in_subquery_expr(
    expr: &SqlExpr,
    subquery: &SelectStatement,
    negated: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None, ctes, &[])?;
    ensure_single_column_subquery(&subquery_plan)?;
    let any_expr = Expr::AnySubquery {
        left: Box::new(bind_expr_with_outer_and_ctes(
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?),
        op: SubqueryComparisonOp::Eq,
        subquery: Box::new(subquery_plan),
    };
    if negated {
        Ok(Expr::Not(Box::new(any_expr)))
    } else {
        Ok(any_expr)
    }
}

pub(super) fn bind_quantified_subquery_expr(
    left: &SqlExpr,
    op: SubqueryComparisonOp,
    is_all: bool,
    subquery: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None, ctes, &[])?;
    ensure_single_column_subquery(&subquery_plan)?;
    let left = Box::new(bind_expr_with_outer_and_ctes(
        left,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?);
    Ok(if is_all {
        Expr::AllSubquery {
            left,
            op,
            subquery: Box::new(subquery_plan),
        }
    } else {
        Expr::AnySubquery {
            left,
            op,
            subquery: Box::new(subquery_plan),
        }
    })
}

pub(super) fn bind_quantified_array_expr(
    left: &SqlExpr,
    op: SubqueryComparisonOp,
    is_all: bool,
    array: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_array_type =
        infer_sql_expr_type_with_ctes(array, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type =
        coerce_unknown_string_literal_type(left, raw_left_type, raw_array_type.element_type());
    let target_array_type = if matches!(op, SubqueryComparisonOp::Match)
        && matches!(left_type.kind, SqlTypeKind::TsVector)
        && matches!(
            array,
            SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
        ) {
        SqlType::array_of(SqlType::new(SqlTypeKind::TsQuery))
    } else if raw_array_type.is_array {
        coerce_unknown_string_literal_type(array, raw_array_type, raw_left_type)
    } else {
        SqlType::array_of(left_type.element_type())
    };
    let bound_array =
        bind_expr_with_outer_and_ctes(array, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let left = Box::new(coerce_bound_expr(
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_left_type,
        left_type,
    ));
    let right = Box::new(coerce_bound_expr(
        bound_array,
        raw_array_type,
        target_array_type,
    ));
    Ok(if is_all {
        Expr::AllArray { left, op, right }
    } else {
        Expr::AnyArray { left, op, right }
    })
}
