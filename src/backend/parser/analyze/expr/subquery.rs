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
    visible_agg_scope: Option<&VisibleAggregateScope>,
    ctes: &[BoundCte],
) -> Result<Query, ParseError> {
    let child_outer = child_outer_scopes(scope, outer_scopes);
    let (query, _) = analyze_select_query_with_outer(
        select,
        catalog,
        &child_outer,
        None,
        visible_agg_scope,
        ctes,
        &[],
    )?;
    Ok(query)
}

fn comparison_operator_for_quantified_array(op: SubqueryComparisonOp) -> Option<&'static str> {
    match op {
        SubqueryComparisonOp::Eq => Some("="),
        SubqueryComparisonOp::NotEq => Some("<>"),
        SubqueryComparisonOp::Lt => Some("<"),
        SubqueryComparisonOp::LtEq => Some("<="),
        SubqueryComparisonOp::Gt => Some(">"),
        SubqueryComparisonOp::GtEq => Some(">="),
        _ => None,
    }
}

fn infer_quantified_array_literal_type(
    elements: &[SqlExpr],
    left_type: SqlType,
    op: SubqueryComparisonOp,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<SqlType, ParseError> {
    let left_element_type = left_type.element_type();
    let comparison_op = comparison_operator_for_quantified_array(op);
    let mut common = Some(left_element_type);
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let raw_element_type = infer_sql_expr_type_with_ctes(
            element,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .element_type();
        let element_type =
            coerce_unknown_string_literal_type(element, raw_element_type, left_element_type);
        if let Some(comparison_op) = comparison_op {
            let compatible = supports_comparison_operator(
                catalog,
                comparison_op,
                left_element_type,
                element_type,
            ) || resolve_common_scalar_type(left_element_type, element_type)
                .is_some_and(|common| {
                    supports_comparison_operator(catalog, comparison_op, common, common)
                });
            if !compatible {
                return Err(ParseError::UndefinedOperator {
                    op: comparison_op,
                    left_type: sql_type_name(left_element_type),
                    right_type: sql_type_name(element_type),
                });
            }
        }
        common = Some(match common {
            None => element_type,
            Some(existing) => {
                resolve_common_scalar_type(existing, element_type).unwrap_or(left_element_type)
            }
        });
    }
    Ok(SqlType::array_of(common.unwrap_or(left_element_type)))
}

fn bind_array_literal_elements_as_type(
    elements: &[SqlExpr],
    target_array_type: SqlType,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let target_element_type = target_array_type.element_type();
    let elements = elements
        .iter()
        .map(|element| {
            let raw_type = infer_sql_expr_type_with_ctes(
                element,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound = bind_expr_with_outer_and_ctes(
                element,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Ok(coerce_bound_expr(bound, raw_type, target_element_type))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::ArrayLiteral {
        elements,
        array_type: target_array_type,
    })
}

fn bind_single_column_sublink(
    select: &SelectStatement,
    sublink_type: SubLinkType,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let child_visible_agg_scope = child_visible_aggregate_scope();
    let query = bind_subquery_query(
        select,
        scope,
        catalog,
        outer_scopes,
        child_visible_agg_scope.as_ref(),
        ctes,
    )?;
    ensure_single_column_subquery(query.columns().len())?;
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type,
        testexpr: None,
        subselect: Box::new(query),
    })))
}

fn ensure_row_subquery_width(width: usize, expected: usize) -> Result<(), ParseError> {
    if width == expected {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "row subquery with matching column count",
            actual: format!("subquery returned {width} columns"),
        })
    }
}

pub(super) fn bind_scalar_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_single_column_sublink(
        select,
        SubLinkType::ExprSubLink,
        scope,
        catalog,
        outer_scopes,
        ctes,
    )
}

pub(super) fn bind_array_subquery_expr(
    select: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_single_column_sublink(
        select,
        SubLinkType::ArraySubLink,
        scope,
        catalog,
        outer_scopes,
        ctes,
    )
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
        subselect: Box::new({
            let child_visible_agg_scope = child_visible_aggregate_scope();
            bind_subquery_query(
                select,
                scope,
                catalog,
                outer_scopes,
                child_visible_agg_scope.as_ref(),
                ctes,
            )?
        }),
    })))
}

pub(super) fn bind_row_compare_subquery_expr(
    row: &SqlExpr,
    op: SubqueryComparisonOp,
    subquery: &SelectStatement,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let SqlExpr::Row(items) = row else {
        return Err(ParseError::UnexpectedToken {
            expected: "row expression",
            actual: format!("{row:?}"),
        });
    };
    let child_visible_agg_scope = child_visible_aggregate_scope();
    let subquery = bind_subquery_query(
        subquery,
        scope,
        catalog,
        outer_scopes,
        child_visible_agg_scope.as_ref(),
        ctes,
    )?;
    ensure_row_subquery_width(subquery.columns().len(), items.len())?;
    let left =
        bind_expr_with_outer_and_ctes(row, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::RowCompareSubLink(op),
        testexpr: Some(Box::new(left)),
        subselect: Box::new(subquery),
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
    let child_visible_agg_scope = child_visible_aggregate_scope();
    let subquery = bind_subquery_query(
        subquery,
        scope,
        catalog,
        outer_scopes,
        child_visible_agg_scope.as_ref(),
        ctes,
    )?;
    let row_width = match expr {
        SqlExpr::Row(items) => Some(items.len()),
        _ => None,
    };
    if let Some(width) = row_width {
        ensure_row_subquery_width(subquery.columns().len(), width)?;
    } else {
        ensure_single_column_subquery(subquery.columns().len())?;
    }
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
    let child_visible_agg_scope = child_visible_aggregate_scope();
    let subquery = bind_subquery_query(
        subquery,
        scope,
        catalog,
        outer_scopes,
        child_visible_agg_scope.as_ref(),
        ctes,
    )?;
    let row_width = match left {
        SqlExpr::Row(items) => Some(items.len()),
        _ => None,
    };
    if let Some(width) = row_width {
        ensure_row_subquery_width(subquery.columns().len(), width)?;
    } else {
        ensure_single_column_subquery(subquery.columns().len())?;
    }
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
    } else if let SqlExpr::ArrayLiteral(elements) = array {
        infer_quantified_array_literal_type(
            elements,
            left_type,
            op,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?
    } else if raw_array_type.is_array {
        coerce_unknown_string_literal_type(array, raw_array_type, raw_left_type)
    } else {
        SqlType::array_of(left_type.element_type())
    };
    let bound_array = if let SqlExpr::ArrayLiteral(elements) = array {
        bind_array_literal_elements_as_type(
            elements,
            target_array_type,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?
    } else {
        let bound_array = bind_expr_with_outer_and_ctes(
            array,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        coerce_bound_expr(bound_array, raw_array_type, target_array_type)
    };
    let bound_left =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let (bound_left, left_explicit_collation) = strip_explicit_collation(bound_left);
    let comparison_left_type = if matches!(array, SqlExpr::ArrayLiteral(_)) {
        target_array_type.element_type()
    } else {
        left_type
    };
    let left = coerce_bound_expr(bound_left, raw_left_type, comparison_left_type);
    let collation_oid = consumer_for_subquery_comparison_op(op)
        .map(|consumer| {
            derive_consumer_collation(
                catalog,
                consumer,
                &[
                    (comparison_left_type, left_explicit_collation),
                    (target_array_type.element_type(), None),
                ],
            )
        })
        .transpose()?
        .flatten();
    Ok(Expr::scalar_array_op_with_collation(
        op,
        !is_all,
        left,
        bound_array,
        collation_oid,
    ))
}
