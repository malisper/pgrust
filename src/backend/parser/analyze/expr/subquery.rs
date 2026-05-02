use super::*;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::nodes::datum::{RecordDescriptor, Value};
use crate::include::nodes::primnodes::{SubLink, SubLinkType, SubqueryComparison, TargetEntry};

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

pub(in crate::backend::parser::analyze) fn exists_subquery_query(mut query: Query) -> Query {
    query.target_list = vec![TargetEntry::new(
        "?column?",
        Expr::Const(Value::Int32(1)),
        SqlType::new(SqlTypeKind::Int4),
        1,
    )];
    query.has_target_srfs = false;
    query
}

fn comparison_operator_for_quantified_array(op: SubqueryComparisonOp) -> Option<&'static str> {
    match op {
        SubqueryComparisonOp::Eq => Some("="),
        SubqueryComparisonOp::NotEq => Some("<>"),
        SubqueryComparisonOp::Lt => Some("<"),
        SubqueryComparisonOp::LtEq => Some("<="),
        SubqueryComparisonOp::Gt => Some(">"),
        SubqueryComparisonOp::GtEq => Some(">="),
        SubqueryComparisonOp::RegexMatch => Some("~"),
        SubqueryComparisonOp::NotRegexMatch => Some("!~"),
        _ => None,
    }
}

fn scalar_subquery_comparison_op(
    op: SubqueryComparisonOp,
) -> Option<(&'static str, crate::include::nodes::primnodes::OpExprKind)> {
    use crate::include::nodes::primnodes::OpExprKind;

    Some(match op {
        SubqueryComparisonOp::Eq => ("=", OpExprKind::Eq),
        SubqueryComparisonOp::NotEq => ("<>", OpExprKind::NotEq),
        SubqueryComparisonOp::Lt => ("<", OpExprKind::Lt),
        SubqueryComparisonOp::LtEq => ("<=", OpExprKind::LtEq),
        SubqueryComparisonOp::Gt => (">", OpExprKind::Gt),
        SubqueryComparisonOp::GtEq => (">=", OpExprKind::GtEq),
        SubqueryComparisonOp::Match
        | SubqueryComparisonOp::RegexMatch
        | SubqueryComparisonOp::NotRegexMatch
        | SubqueryComparisonOp::Like
        | SubqueryComparisonOp::NotLike
        | SubqueryComparisonOp::ILike
        | SubqueryComparisonOp::NotILike
        | SubqueryComparisonOp::Similar
        | SubqueryComparisonOp::NotSimilar => return None,
    })
}

fn coerce_single_column_subquery_comparison(
    left: Expr,
    subquery: &mut Query,
    op: SubqueryComparisonOp,
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, Option<SubqueryComparison>), ParseError> {
    let Some((op_text, op_kind)) = scalar_subquery_comparison_op(op) else {
        return Ok((left, None));
    };
    let Some(target) = subquery.target_list.first_mut() else {
        return Ok((left, None));
    };
    let raw_left_type = expr_sql_type_hint(&left).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let right_type = target.sql_type;
    let comparison_left_type = if matches!(left, Expr::Const(Value::Null)) {
        right_type
    } else {
        raw_left_type
    };
    let comparison_right_type = if matches!(target.expr, Expr::Const(Value::Null)) {
        comparison_left_type
    } else {
        right_type
    };
    let comparison = bind_lowered_comparison_expr(
        op_text,
        op_kind,
        left,
        raw_left_type,
        comparison_left_type,
        target.expr.clone(),
        right_type,
        comparison_right_type,
        None,
        None,
        catalog,
    )?;
    let Expr::Op(op_expr) = comparison else {
        return Err(ParseError::DetailedError {
            message: "failed to bind subquery comparison".into(),
            detail: Some("comparison binding did not produce an operator expression".into()),
            hint: None,
            sqlstate: "XX000",
        });
    };
    let collation_oid = op_expr.collation_oid;
    let mut args = op_expr.args.into_iter();
    let coerced_left = args.next().ok_or_else(|| ParseError::DetailedError {
        message: "failed to bind subquery comparison".into(),
        detail: Some("comparison operator did not include a left argument".into()),
        hint: None,
        sqlstate: "XX000",
    })?;
    let coerced_right = args.next().ok_or_else(|| ParseError::DetailedError {
        message: "failed to bind subquery comparison".into(),
        detail: Some("comparison operator did not include a right argument".into()),
        hint: None,
        sqlstate: "XX000",
    })?;
    let comparison = resolve_subquery_comparison_metadata(
        catalog,
        op_text,
        op_kind,
        &coerced_left,
        &coerced_right,
        collation_oid,
    )?;
    target.sql_type = expr_sql_type_hint(&coerced_right).unwrap_or(right_type);
    target.expr = coerced_right;
    Ok((coerced_left, Some(comparison)))
}

fn resolve_subquery_comparison_metadata(
    catalog: &dyn CatalogLookup,
    op_text: &'static str,
    op_kind: crate::include::nodes::primnodes::OpExprKind,
    left: &Expr,
    right: &Expr,
    collation_oid: Option<u32>,
) -> Result<SubqueryComparison, ParseError> {
    let left_type = expr_sql_type_hint(left).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let right_type = expr_sql_type_hint(right).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let left_oid = catalog
        .type_oid_for_sql_type(left_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(left_type)))?;
    let right_oid = catalog
        .type_oid_for_sql_type(right_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(right_type)))?;
    let left_lookup_oid = if matches!(left_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record)
    {
        crate::include::catalog::RECORD_TYPE_OID
    } else {
        left_oid
    };
    let right_lookup_oid = if matches!(
        right_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) {
        crate::include::catalog::RECORD_TYPE_OID
    } else {
        right_oid
    };
    let operator = catalog.operator_by_name_left_right(op_text, left_lookup_oid, right_lookup_oid);
    Ok(SubqueryComparison {
        opno: operator.as_ref().map(|row| row.oid).unwrap_or(0),
        opfuncid: operator.as_ref().map(|row| row.oprcode).unwrap_or(0),
        op: op_kind,
        left_type,
        right_type,
        collation_oid,
    })
}

fn quantified_array_literal_prefers_left_type(element: &SqlExpr) -> bool {
    matches!(element, SqlExpr::Const(_) | SqlExpr::IntegerLiteral(_))
}

fn infer_quantified_array_literal_type(
    elements: &[SqlExpr],
    bound_elements: &[TypedExpr],
    left_type: SqlType,
    op: SubqueryComparisonOp,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let left_element_type = left_type.element_type();
    if matches!(
        op,
        SubqueryComparisonOp::RegexMatch | SubqueryComparisonOp::NotRegexMatch
    ) {
        return Ok(SqlType::array_of(SqlType::new(SqlTypeKind::Text)));
    }
    let comparison_op = comparison_operator_for_quantified_array(op);
    let mut common = Some(left_element_type);
    for (element, bound) in elements.iter().zip(bound_elements) {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let raw_element_type = bound.sql_type.element_type();
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
                    op: comparison_op.into(),
                    left_type: sql_type_name(left_element_type),
                    right_type: sql_type_name(element_type),
                });
            }
        }
        if common == Some(left_element_type) && quantified_array_literal_prefers_left_type(element)
        {
            continue;
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
    raw_elements: &[SqlExpr],
    bound_elements: Vec<TypedExpr>,
    target_array_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    let target_element_type = target_array_type.element_type();
    let elements = bound_elements
        .into_iter()
        .zip(raw_elements.iter())
        .map(|(element, raw_element)| {
            if target_element_type.kind == SqlTypeKind::RegClass
                && let Some(regclass) =
                    bind_regclass_literal_cast(raw_element, target_element_type, catalog)?
            {
                return Ok(regclass);
            }
            Ok(coerce_bound_expr(
                element.expr,
                element.sql_type,
                target_element_type,
            ))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(Expr::ArrayLiteral {
        elements,
        array_type: target_array_type,
    })
}

fn validate_record_quantified_array_literal_types(
    bound_left: &TypedExpr,
    elements: &[SqlExpr],
    bound_elements: &[TypedExpr],
    op: SubqueryComparisonOp,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if !matches!(op, SubqueryComparisonOp::Eq | SubqueryComparisonOp::NotEq) {
        return Ok(());
    }
    let left_type = expression_navigation_sql_type(bound_left.sql_type, catalog);
    if !matches!(left_type.kind, SqlTypeKind::Composite) || left_type.typrelid == 0 {
        return Ok(());
    }
    let Some(left_descriptor) = record_descriptor_for_sql_type(left_type, catalog) else {
        return Ok(());
    };
    for (raw_element, bound_element) in elements.iter().zip(bound_elements) {
        if !matches!(raw_element, SqlExpr::Row(_)) {
            continue;
        }
        let Some(element_descriptor) =
            record_descriptor_for_sql_type(bound_element.sql_type, catalog)
        else {
            continue;
        };
        if left_descriptor.fields.len() != element_descriptor.fields.len() {
            return Err(ParseError::DetailedError {
                message: "cannot compare record types with different numbers of columns".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        for (idx, (left_field, element_field)) in left_descriptor
            .fields
            .iter()
            .zip(element_descriptor.fields.iter())
            .enumerate()
        {
            if left_field.sql_type != element_field.sql_type {
                return Err(ParseError::DetailedError {
                    message: format!(
                        "cannot compare dissimilar column types {} and {} at record column {}",
                        sql_type_name(left_field.sql_type),
                        sql_type_name(element_field.sql_type),
                        idx + 1
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42804",
                });
            }
        }
    }
    Ok(())
}

fn record_descriptor_for_sql_type(
    mut sql_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Option<RecordDescriptor> {
    if !sql_type.is_array
        && matches!(sql_type.kind, SqlTypeKind::Composite)
        && sql_type.typrelid == 0
        && let Some(row) = catalog.type_by_oid(sql_type.type_oid)
        && row.typrelid != 0
    {
        sql_type = sql_type.with_identity(row.oid, row.typrelid);
    }
    if matches!(sql_type.kind, SqlTypeKind::Composite) && sql_type.typrelid != 0 {
        let relation = catalog.lookup_relation_by_oid(sql_type.typrelid)?;
        return Some(RecordDescriptor::named(
            sql_type.type_oid,
            sql_type.typrelid,
            sql_type.typmod,
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        ));
    }
    if matches!(sql_type.kind, SqlTypeKind::Record) && sql_type.typmod > 0 {
        return lookup_anonymous_record_descriptor(sql_type.typmod);
    }
    None
}

fn bind_array_left_quantified_list_expr(
    bound_left: &TypedExpr,
    elements: &[SqlExpr],
    bound_elements: &[TypedExpr],
    op: SubqueryComparisonOp,
    is_all: bool,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    if !bound_left.sql_type.is_array {
        return Ok(None);
    }
    let (op_text, op_kind, bool_kind) = match (op, is_all) {
        (SubqueryComparisonOp::Eq, false) => (
            "=",
            OpExprKind::Eq,
            crate::include::nodes::primnodes::BoolExprType::Or,
        ),
        (SubqueryComparisonOp::NotEq, true) => (
            "<>",
            OpExprKind::NotEq,
            crate::include::nodes::primnodes::BoolExprType::And,
        ),
        _ => return Ok(None),
    };
    let mut arms = Vec::with_capacity(bound_elements.len());
    for (element, bound_element) in elements.iter().zip(bound_elements) {
        let element_type = coerce_unknown_string_literal_type(
            element,
            bound_element.sql_type,
            bound_left.sql_type,
        );
        arms.push(bind_lowered_comparison_expr(
            op_text,
            op_kind,
            bound_left.expr.clone(),
            bound_left.sql_type,
            bound_left.sql_type,
            bound_element.expr.clone(),
            bound_element.sql_type,
            element_type,
            None,
            None,
            catalog,
        )?);
    }
    Ok(Some(match arms.as_slice() {
        [single] => single.clone(),
        _ => Expr::bool_expr(bool_kind, arms),
    }))
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
        comparison: None,
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
        comparison: None,
        subselect: Box::new(exists_subquery_query({
            let child_visible_agg_scope = child_visible_aggregate_scope();
            bind_subquery_query(
                select,
                scope,
                catalog,
                outer_scopes,
                child_visible_agg_scope.as_ref(),
                ctes,
            )?
        })),
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
        comparison: None,
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
    let mut subquery = bind_subquery_query(
        subquery,
        scope,
        catalog,
        outer_scopes,
        child_visible_agg_scope.as_ref(),
        ctes,
    )?;
    let subquery_width = subquery.columns().len();
    let (testexpr, comparison) = if let SqlExpr::Row(items) = expr {
        ensure_row_subquery_width(subquery_width, items.len())?;
        (
            bind_row_valued_in_testexpr(
                expr,
                &mut subquery,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            None,
        )
    } else {
        ensure_single_column_subquery(subquery_width)?;
        let bound =
            bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        coerce_single_column_subquery_comparison(
            bound,
            &mut subquery,
            SubqueryComparisonOp::Eq,
            catalog,
        )?
    };
    let any_expr = Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::AnySubLink(SubqueryComparisonOp::Eq),
        testexpr: Some(Box::new(testexpr)),
        comparison,
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

fn bind_row_valued_in_testexpr(
    expr: &SqlExpr,
    subquery: &mut Query,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left =
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let Expr::Row { fields, .. } = left else {
        ensure_single_column_subquery(subquery.columns().len())?;
        unreachable!("ensure_single_column_subquery returned for multi-column subquery");
    };
    if fields.len() != subquery.target_list.len() {
        return Err(ParseError::DetailedError {
            message: "unequal number of entries in row expressions".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }

    let mut coerced_fields = Vec::with_capacity(fields.len());
    for ((field_name, field_expr), target) in
        fields.into_iter().zip(subquery.target_list.iter_mut())
    {
        let left_type = expr_sql_type_hint(&field_expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
        let right_type = target.sql_type;
        let common = resolve_common_scalar_type(left_type, right_type).ok_or_else(|| {
            ParseError::UndefinedOperator {
                op: "=".into(),
                left_type: sql_type_name(left_type),
                right_type: sql_type_name(right_type),
            }
        })?;
        if !supports_comparison_operator(catalog, "=", common, common) {
            return Err(ParseError::UndefinedOperator {
                op: "=".into(),
                left_type: sql_type_name(common),
                right_type: sql_type_name(common),
            });
        }
        coerced_fields.push((field_name, coerce_bound_expr(field_expr, left_type, common)));
        target.expr = coerce_bound_expr(target.expr.clone(), right_type, common);
        target.sql_type = common;
    }
    let descriptor = assign_anonymous_record_descriptor(
        coerced_fields
            .iter()
            .map(|(field_name, field_expr)| {
                (
                    field_name.clone(),
                    expr_sql_type_hint(field_expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                )
            })
            .collect(),
    );
    Ok(Expr::Row {
        descriptor,
        fields: coerced_fields,
    })
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
    let mut subquery = bind_subquery_query(
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
    let mut left =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    if row_width.is_none()
        && let Some(right_type) = subquery.columns().first().map(|column| column.sql_type)
        && right_type.is_array
        && let Some(comparison_op) = comparison_operator_for_quantified_array(op)
    {
        let left_type = expr_sql_type_hint(&left).unwrap_or(SqlType::new(SqlTypeKind::Text));
        return Err(ParseError::UndefinedOperator {
            op: comparison_op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    let comparison = if row_width.is_none() {
        let (coerced_left, comparison) =
            coerce_single_column_subquery_comparison(left, &mut subquery, op, catalog)?;
        left = coerced_left;
        comparison
    } else {
        None
    };
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: if is_all {
            SubLinkType::AllSubLink(op)
        } else {
            SubLinkType::AnySubLink(op)
        },
        testexpr: Some(Box::new(left)),
        comparison,
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
    let bound_left = bind_typed_expr_with_outer_and_ctes(
        left,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let raw_left_type = bound_left.sql_type;
    let regex_array_op = matches!(
        op,
        SubqueryComparisonOp::RegexMatch | SubqueryComparisonOp::NotRegexMatch
    );
    let (target_array_type, bound_array, _left_type, comparison_left_type) =
        if let SqlExpr::ArrayLiteral(elements) = array {
            let bound_elements = elements
                .iter()
                .map(|element| {
                    bind_typed_expr_with_outer_and_ctes(
                        element,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            validate_record_quantified_array_literal_types(
                &bound_left,
                elements,
                &bound_elements,
                op,
                catalog,
            )?;
            if !regex_array_op
                && let Some(expr) = bind_array_left_quantified_list_expr(
                    &bound_left,
                    elements,
                    &bound_elements,
                    op,
                    is_all,
                    catalog,
                )?
            {
                return Ok(expr);
            }
            let raw_array_element_type = elements
                .iter()
                .zip(bound_elements.iter())
                .find_map(|(element, bound)| {
                    (!matches!(element, SqlExpr::Const(Value::Null)))
                        .then_some(bound.sql_type.element_type())
                })
                .unwrap_or(SqlType::new(SqlTypeKind::Text));
            let left_type = if regex_array_op {
                SqlType::new(SqlTypeKind::Text)
            } else {
                coerce_unknown_string_literal_type(left, raw_left_type, raw_array_element_type)
            };
            let target_array_type = infer_quantified_array_literal_type(
                elements,
                &bound_elements,
                left_type,
                op,
                catalog,
            )?;
            let comparison_left_type = target_array_type.element_type();
            let bound_array = bind_array_literal_elements_as_type(
                elements,
                bound_elements,
                target_array_type,
                catalog,
            )?;
            (
                target_array_type,
                bound_array,
                left_type,
                comparison_left_type,
            )
        } else {
            let bound_array = bind_typed_expr_with_outer_and_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let raw_array_type = bound_array.sql_type;
            let left_type = if regex_array_op {
                SqlType::new(SqlTypeKind::Text)
            } else {
                coerce_unknown_string_literal_type(
                    left,
                    raw_left_type,
                    raw_array_type.element_type(),
                )
            };
            if matches!(array, SqlExpr::ScalarSubquery(_))
                && raw_array_type.is_array
                && let Some(comparison_op) = comparison_operator_for_quantified_array(op)
            {
                return Err(ParseError::UndefinedOperator {
                    op: comparison_op.into(),
                    left_type: sql_type_name(left_type),
                    right_type: sql_type_name(raw_array_type),
                });
            }
            let target_array_type = if regex_array_op {
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            } else if matches!(op, SubqueryComparisonOp::Match)
                && matches!(left_type.kind, SqlTypeKind::TsVector)
                && matches!(
                    array,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                )
            {
                SqlType::array_of(SqlType::new(SqlTypeKind::TsQuery))
            } else if raw_array_type.is_array {
                coerce_unknown_string_literal_type(array, raw_array_type, raw_left_type)
            } else {
                SqlType::array_of(left_type.element_type())
            };
            let bound_array =
                coerce_bound_expr(bound_array.expr, raw_array_type, target_array_type);
            (target_array_type, bound_array, left_type, left_type)
        };
    let (bound_left, left_explicit_collation) = strip_explicit_collation(bound_left.expr);
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
