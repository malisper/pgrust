use super::*;
use crate::include::nodes::primnodes::{SubLink, SubLinkType};

fn child_outer_scopes(scope: &BoundScope, outer_scopes: &[BoundScope]) -> Vec<BoundScope> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    child_outer
}

fn bind_subquery_query(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Query, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    let (query, _) =
        analyze_select_query_with_outer(select, catalog, &child_outer, None, ctes, &[])?;
    Ok(query)
}

pub(super) fn bind_scalar_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let query = bind_subquery_query(select, scope, catalog, outer_scopes, ctes)?;
    ensure_single_column_subquery(query.columns().len())?;
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::ExprSubLink,
        testexpr: None,
        subselect: Box::new(query),
    })))
}

pub(super) fn bind_exists_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::ExistsSubLink,
        testexpr: None,
        subselect: Box::new(bind_subquery_query(
            select,
            scope,
            catalog,
            outer_scopes,
            ctes,
        )?),
    })))
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
    let subquery = bind_subquery_query(subquery, scope, catalog, outer_scopes, ctes)?;
    ensure_single_column_subquery(subquery.columns().len())?;
    let any_expr = Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::AnySubLink(SubqueryComparisonOp::Eq),
        testexpr: Some(Box::new(bind_expr_with_outer_and_ctes(
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        subselect: Box::new(subquery),
    }));
    if negated {
        Ok(Expr::bool_expr(
            crate::include::nodes::primnodes::BoolExprType::Not,
            vec![any_expr],
        ))
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
    let subquery = bind_subquery_query(subquery, scope, catalog, outer_scopes, ctes)?;
    ensure_single_column_subquery(subquery.columns().len())?;
    let left = Box::new(bind_expr_with_outer_and_ctes(
        left,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?);
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: if is_all {
            SubLinkType::AllSubLink(op)
        } else {
            SubLinkType::AnySubLink(op)
        },
        testexpr: Some(left),
        subselect: Box::new(subquery),
    })))
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
    let left = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_left_type,
        left_type,
    );
    let right = coerce_bound_expr(
        bound_array,
        raw_array_type,
        target_array_type,
    );
    Ok(Expr::scalar_array_op(op, !is_all, left, right))
}
