use super::functions::*;
use super::infer::*;
use super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::parser::parse_type_name;
use crate::backend::utils::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
};
use crate::include::catalog::{
    ANYOID, PG_LANGUAGE_INTERNAL_OID, builtin_scalar_function_for_proc_oid,
    builtin_type_name_for_oid, multirange_type_ref_for_sql_type, range_type_ref_for_sql_type,
};
use crate::include::nodes::primnodes::{
    BoolExprType, CaseExpr as BoundCaseExpr, CaseTestExpr as BoundCaseTestExpr,
    CaseWhen as BoundCaseWhen, ExprArraySubscript, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExprKind,
    ScalarFunctionImpl, WindowFuncKind, expr_contains_set_returning, expr_sql_type_hint,
};

mod func;
mod json;
mod ops;
mod subquery;
mod targets;

use self::func::{
    bind_row_to_json_call, bind_scalar_function_call, bind_user_defined_scalar_function_call,
};
use self::json::{
    bind_json_binary_expr, bind_jsonb_contained_expr, bind_jsonb_contains_expr,
    bind_jsonb_exists_all_expr, bind_jsonb_exists_any_expr, bind_jsonb_exists_expr,
    bind_jsonb_path_binary_expr, bind_jsonb_subscript_expr, bind_maybe_jsonb_delete,
};
pub(crate) use self::ops::bind_concat_operands;
pub(super) use self::ops::bind_lowered_comparison_expr;
use self::ops::bind_order_by_using_direction;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_catalog_binary_operator_expr,
    bind_comparison_expr, bind_concat_expr, bind_maybe_network_arithmetic,
    bind_maybe_network_bitwise, bind_maybe_network_operator, bind_overloaded_binary_expr,
    bind_prefix_operator_expr, bind_shift_expr, bind_text_pattern_comparison_expr,
    bind_text_starts_with_expr, supports_comparison_operator,
};
use self::subquery::{
    bind_array_subquery_expr, bind_exists_subquery_expr, bind_in_subquery_expr,
    bind_quantified_array_expr, bind_quantified_subquery_expr, bind_row_compare_subquery_expr,
    bind_scalar_subquery_expr,
};
pub(crate) use self::targets::{BoundSelectTargets, bind_select_targets};
use self::targets::{bind_set_returning_expr_from_parts, root_call_returns_set};
use super::multiranges::{
    bind_maybe_multirange_arithmetic, bind_maybe_multirange_comparison,
    bind_maybe_multirange_contains, bind_maybe_multirange_over_position,
    bind_maybe_multirange_shift,
};
use super::ranges::{
    bind_maybe_range_arithmetic, bind_maybe_range_comparison, bind_maybe_range_contains,
    bind_maybe_range_over_position, bind_maybe_range_shift,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TypedExpr {
    pub expr: Expr,
    pub sql_type: SqlType,
    pub contains_srf: bool,
}

pub(super) fn bind_typed_expr_with_outer_and_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<TypedExpr, ParseError> {
    let bound =
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let sql_type = expr_sql_type_hint(&bound).unwrap_or_else(|| {
        infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    });
    Ok(TypedExpr {
        contains_srf: expr_contains_set_returning(&bound),
        expr: bound,
        sql_type,
    })
}

fn set_returning_not_allowed_error(context: &'static str) -> ParseError {
    ParseError::FeatureNotSupported(format!(
        "set-returning functions are not allowed in {context}"
    ))
}

pub(super) fn build_bound_order_by_entry(
    item: &OrderByItem,
    bound_expr: Expr,
    ressortgroupref: usize,
    catalog: &dyn CatalogLookup,
) -> Result<OrderByEntry, ParseError> {
    let expr_type = expr_sql_type_hint(&bound_expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let descending = match item.using_operator.as_deref() {
        Some(operator) => bind_order_by_using_direction(catalog, operator, expr_type)?,
        None => item.descending,
    };
    let (expr, collation_oid) = finalize_order_by_expr(bound_expr, catalog)?;
    Ok(OrderByEntry {
        expr,
        ressortgroupref,
        descending,
        nulls_first: item.nulls_first,
        collation_oid,
    })
}

fn reject_typed_srf(expr: &TypedExpr, context: &'static str) -> Result<(), ParseError> {
    if expr.contains_srf {
        Err(set_returning_not_allowed_error(context))
    } else {
        Ok(())
    }
}

fn common_type_for_typed_exprs(
    exprs: &[TypedExpr],
    expected: &'static str,
) -> Result<SqlType, ParseError> {
    let mut common: Option<SqlType> = None;
    for expr in exprs {
        if matches!(expr.expr, Expr::Const(Value::Null)) {
            continue;
        }
        let ty = expr.sql_type.element_type();
        common = Some(match common {
            None => ty,
            Some(current) => resolve_common_scalar_type(current, ty).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected,
                    actual: format!("{} and {}", sql_type_name(current), sql_type_name(ty)),
                }
            })?,
        });
    }
    Ok(common.unwrap_or(SqlType::new(SqlTypeKind::Text)))
}

pub(super) fn bind_resolved_scalar_function_call(
    resolved: &ResolvedFunctionCall,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    self::func::bind_resolved_scalar_function_call(
        resolved,
        args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

fn supports_array_subscripts(array_type: SqlType) -> bool {
    array_type.is_array
        || matches!(
            array_type.kind,
            SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
        )
}

fn unsupported_subscript_type_error(sql_type: SqlType) -> ParseError {
    ParseError::DetailedError {
        message: format!(
            "cannot subscript type {} because it does not support subscripting",
            sql_type_name(sql_type)
        ),
        detail: None,
        hint: None,
        sqlstate: "42804",
    }
}

fn fixed_length_array_slice_error() -> ParseError {
    ParseError::DetailedError {
        message: "slices of fixed-length arrays not implemented".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn point_coordinate_subscript(
    subscripts: &[crate::include::nodes::parsenodes::ArraySubscript],
) -> Option<i32> {
    let [subscript] = subscripts else {
        return None;
    };
    if subscript.is_slice || subscript.upper.is_some() {
        return None;
    }
    match subscript.lower.as_deref()? {
        SqlExpr::IntegerLiteral(value) => value.parse::<i32>().ok(),
        SqlExpr::Const(Value::Int16(value)) => Some(i32::from(*value)),
        SqlExpr::Const(Value::Int32(value)) => Some(*value),
        _ => None,
    }
}

#[allow(dead_code)]
pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub(crate) fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

fn build_whole_row_expr(fields: Vec<(String, Expr)>) -> Expr {
    Expr::Row {
        descriptor: assign_anonymous_record_descriptor(
            fields
                .iter()
                .map(|(field_name, expr)| {
                    (
                        field_name.clone(),
                        expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                    )
                })
                .collect(),
        ),
        fields,
    }
}

fn bind_row_expr_fields(
    items: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<(String, Expr)>, ParseError> {
    let mut field_exprs = Vec::new();
    for item in items {
        if let SqlExpr::Column(name) = item
            && let Some(relation_name) = name.strip_suffix(".*")
        {
            let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
            for (_, expr) in fields {
                let field_name = format!("f{}", field_exprs.len() + 1);
                field_exprs.push((field_name, expr));
            }
            continue;
        }
        if let SqlExpr::FieldSelect { expr, field } = item
            && field == "*"
        {
            let bound_expr = bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            for (_, expr) in expand_bound_record_expr_fields(&bound_expr, catalog)? {
                let field_name = format!("f{}", field_exprs.len() + 1);
                field_exprs.push((field_name, expr));
            }
            continue;
        }
        let expr =
            bind_expr_with_outer_and_ctes(item, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        let field_name = format!("f{}", field_exprs.len() + 1);
        field_exprs.push((field_name, expr));
    }
    Ok(field_exprs)
}

fn overlaps_row_items(expr: &SqlExpr) -> Result<(&SqlExpr, &SqlExpr), ParseError> {
    let SqlExpr::Row(items) = expr else {
        return Err(ParseError::UnexpectedToken {
            expected: "row expression",
            actual: format!("{expr:?}"),
        });
    };
    match items.as_slice() {
        [start, end] => Ok((start, end)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "two-element OVERLAPS row",
            actual: format!("{} elements", items.len()),
        }),
    }
}

fn overlaps_end_expr(
    start: &SqlExpr,
    end_or_interval: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlExpr {
    let end_type = infer_sql_expr_type_with_ctes(
        end_or_interval,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    if !end_type.is_array && matches!(end_type.kind, SqlTypeKind::Interval) {
        SqlExpr::Add(Box::new(start.clone()), Box::new(end_or_interval.clone()))
    } else {
        end_or_interval.clone()
    }
}

fn bind_overlaps_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let (left_start, left_end_or_interval) = overlaps_row_items(left)?;
    let (right_start, right_end_or_interval) = overlaps_row_items(right)?;
    let left_end = overlaps_end_expr(
        left_start,
        left_end_or_interval,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let right_end = overlaps_end_expr(
        right_start,
        right_end_or_interval,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let lowered = SqlExpr::And(
        Box::new(SqlExpr::Lt(
            Box::new(left_start.clone()),
            Box::new(right_end),
        )),
        Box::new(SqlExpr::Lt(
            Box::new(right_start.clone()),
            Box::new(left_end),
        )),
    );
    bind_expr_with_outer_and_ctes(&lowered, scope, catalog, outer_scopes, grouped_outer, ctes)
}

fn expand_bound_record_expr_fields(
    expr: &Expr,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<(String, Expr)>, ParseError> {
    if let Expr::Row { fields, .. } = expr {
        return Ok(fields.clone());
    }

    let Some(sql_type) = expr_sql_type_hint(expr) else {
        return Err(ParseError::UnexpectedToken {
            expected: "record expression",
            actual: "field expansion .*".into(),
        });
    };

    let fields = if matches!(sql_type.kind, SqlTypeKind::Composite) && sql_type.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(sql_type.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "named composite type",
                actual: format!("type relation {} not found", sql_type.typrelid),
            })?;
        relation
            .desc
            .columns
            .into_iter()
            .filter(|column| !column.dropped)
            .map(|column| (column.name, column.sql_type))
            .collect::<Vec<_>>()
    } else if matches!(sql_type.kind, SqlTypeKind::Record) && sql_type.typmod > 0 {
        lookup_anonymous_record_descriptor(sql_type.typmod)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "record expression",
                actual: "field expansion .*".into(),
            })?
            .fields
            .into_iter()
            .map(|field| (field.name, field.sql_type))
            .collect::<Vec<_>>()
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "record expression",
            actual: "field expansion .*".into(),
        });
    };

    Ok(fields
        .into_iter()
        .map(|(field, field_type)| {
            (
                field.clone(),
                Expr::FieldSelect {
                    expr: Box::new(expr.clone()),
                    field,
                    field_type,
                },
            )
        })
        .collect())
}

fn bind_row_comparison_expr(
    op: &'static str,
    make: OpExprKind,
    left_items: &[SqlExpr],
    right_items: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_fields = bind_row_expr_fields(
        left_items,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let right_fields = bind_row_expr_fields(
        right_items,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    if left_fields.len() != right_fields.len() {
        return Err(ParseError::DetailedError {
            message: "unequal number of entries in row expressions".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if left_fields.is_empty() {
        return Err(ParseError::FeatureNotSupported(
            "cannot compare rows of zero length".into(),
        ));
    }

    let mut parts = Vec::with_capacity(left_fields.len());
    for ((_, left), (_, right)) in left_fields.into_iter().zip(right_fields) {
        let left_type = expr_sql_type_hint(&left).unwrap_or(SqlType::new(SqlTypeKind::Text));
        let right_type = expr_sql_type_hint(&right).unwrap_or(SqlType::new(SqlTypeKind::Text));
        parts.push(bind_lowered_comparison_expr(
            op, make, left, left_type, left_type, right, right_type, right_type, None, None,
            catalog,
        )?);
    }

    if parts.len() == 1 {
        return Ok(parts.pop().expect("single row comparison part"));
    }
    Ok(Expr::bool_expr(
        if make == OpExprKind::Eq {
            BoolExprType::And
        } else {
            BoolExprType::Or
        },
        parts,
    ))
}

fn bind_row_distinct_expr(
    negated: bool,
    left_items: &[SqlExpr],
    right_items: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_fields = bind_row_expr_fields(
        left_items,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let right_fields = bind_row_expr_fields(
        right_items,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    if left_fields.len() != right_fields.len() {
        return Err(ParseError::DetailedError {
            message: "unequal number of entries in row expressions".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }

    let mut parts = left_fields
        .into_iter()
        .zip(right_fields)
        .map(|((_, left), (_, right))| {
            if negated {
                Expr::IsNotDistinctFrom(Box::new(left), Box::new(right))
            } else {
                Expr::IsDistinctFrom(Box::new(left), Box::new(right))
            }
        })
        .collect::<Vec<_>>();

    if parts.is_empty() {
        return Ok(Expr::Const(Value::Bool(negated)));
    }
    if parts.len() == 1 {
        return Ok(parts.pop().expect("single row distinct part"));
    }
    Ok(Expr::bool_expr(
        if negated {
            BoolExprType::And
        } else {
            BoolExprType::Or
        },
        parts,
    ))
}

fn bind_named_composite_row_cast(
    items: &[SqlExpr],
    target_type: SqlType,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<Expr>, ParseError> {
    if !matches!(target_type.kind, SqlTypeKind::Composite) || target_type.typrelid == 0 {
        return Ok(None);
    }
    let relation = catalog
        .lookup_relation_by_oid(target_type.typrelid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "named composite type",
            actual: format!("type relation {} not found", target_type.typrelid),
        })?;
    let target_fields = relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| (column.name.clone(), column.sql_type))
        .collect::<Vec<_>>();
    let field_exprs =
        bind_row_expr_fields(items, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    if field_exprs.len() != target_fields.len() {
        return Err(ParseError::DetailedError {
            message: format!("cannot cast type record to {}", sql_type_name(target_type)),
            detail: Some(format!(
                "Input has {} columns but target row type has {}.",
                field_exprs.len(),
                target_fields.len()
            )),
            hint: None,
            sqlstate: "42846",
        });
    }
    let fields = field_exprs
        .into_iter()
        .zip(target_fields.iter())
        .map(|((_, expr), (field_name, field_type))| {
            let source_type = expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
            (
                field_name.clone(),
                coerce_bound_expr(expr, source_type, *field_type),
            )
        })
        .collect::<Vec<_>>();
    Ok(Some(Expr::Row {
        descriptor: crate::include::nodes::datum::RecordDescriptor::named(
            target_type
                .type_oid
                .max(crate::include::catalog::RECORD_TYPE_OID),
            target_type.typrelid,
            target_type.typmod,
            target_fields,
        ),
        fields,
    }))
}

pub(super) fn raise_expr_varlevels(expr: Expr, levels: usize) -> Expr {
    if levels == 0 {
        return expr;
    }
    match expr {
        Expr::Var(mut var) => {
            if !matches!(var.varno, OUTER_VAR | INNER_VAR | INDEX_VAR) {
                var.varlevelsup += levels;
            }
            Expr::Var(var)
        }
        Expr::Aggref(mut aggref) => {
            aggref.agglevelsup += levels;
            Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
                direct_args: aggref
                    .direct_args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
                args: aggref
                    .args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
                aggorder: aggref
                    .aggorder
                    .into_iter()
                    .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                        expr: raise_expr_varlevels(item.expr, levels),
                        ..item
                    })
                    .collect(),
                aggfilter: aggref
                    .aggfilter
                    .map(|expr| raise_expr_varlevels(expr, levels)),
                ..*aggref
            }))
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(raise_expr_varlevels(*saop.left, levels)),
                right: Box::new(raise_expr_varlevels(*saop.right, levels)),
                ..*saop
            },
        )),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            op: xml.op,
            name: xml.name,
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            arg_names: xml.arg_names,
            args: xml
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            xml_option: xml.xml_option,
            indent: xml.indent,
            target_type: xml.target_type,
            standalone: xml.standalone,
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(raise_expr_varlevels(*inner, levels)), ty),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(raise_expr_varlevels(*inner, levels))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(raise_expr_varlevels(*inner, levels))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            negated,
            collation_oid,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| raise_expr_varlevels(element, levels))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, raise_expr_varlevels(expr, levels)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            field,
            field_type,
        },
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(raise_expr_varlevels(*arg, levels))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: raise_expr_varlevels(arm.expr, levels),
                    result: raise_expr_varlevels(arm.result, levels),
                })
                .collect(),
            defresult: Box::new(raise_expr_varlevels(*case_expr.defresult, levels)),
            ..*case_expr
        })),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(raise_expr_varlevels(*array, levels)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| raise_expr_varlevels(expr, levels)),
                    upper: subscript
                        .upper
                        .map(|expr| raise_expr_varlevels(expr, levels)),
                })
                .collect(),
        },
        other => other,
    }
}

fn current_window_state_or_error()
-> Result<std::rc::Rc<std::cell::RefCell<WindowBindingState>>, ParseError> {
    match current_window_state() {
        Some(state) if windows_allowed() => Ok(state),
        Some(_) => Err(nested_window_error()),
        None => Err(window_not_allowed_error()),
    }
}

fn bind_window_agg_call(
    func: AggFunc,
    args: &[SqlFunctionArg],
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    over: &RawWindowSpec,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let state = current_window_state_or_error()?;
    if aggregate_args_are_named(args) {
        return Err(ParseError::UnexpectedToken {
            expected: "aggregate arguments without names",
            actual: func.name().into(),
        });
    }
    let arg_values = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    validate_distinct_aggregate_order_by(&arg_values, order_by, distinct)?;
    validate_aggregate_arity(func, &arg_values)?;
    let arg_types = arg_values
        .iter()
        .map(|expr| infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, None, ctes))
        .collect::<Vec<_>>();
    let resolved = resolve_builtin_aggregate_call(catalog, func, &arg_types, func_variadic);
    let bound_args = arg_values
        .iter()
        .map(|expr| {
            with_windows_disallowed(|| {
                bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if bound_args.iter().any(expr_contains_set_returning) {
        return Err(set_returning_not_allowed_error(
            "window aggregate arguments",
        ));
    }
    let coerced_args = if let Some(resolved) = &resolved {
        bound_args
            .into_iter()
            .zip(arg_types.iter().copied())
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect()
    } else {
        bound_args
    };
    let bound_filter = filter
        .map(|expr| {
            with_windows_disallowed(|| {
                bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        })
        .transpose()?;
    if bound_filter
        .as_ref()
        .is_some_and(expr_contains_set_returning)
    {
        return Err(set_returning_not_allowed_error("aggregate FILTER"));
    }
    let bound_order_by = order_by
        .iter()
        .map(|item| {
            let bound_expr = bind_expr_with_outer_and_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            build_bound_order_by_entry(item, bound_expr, 0, catalog)
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    for item in &bound_order_by {
        reject_nested_local_ctes_in_agg_expr(&item.expr)?;
        if expr_contains_set_returning(&item.expr) {
            return Err(set_returning_not_allowed_error("aggregate ORDER BY"));
        }
    }
    let spec = bind_window_spec(over, catalog, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    let kind = WindowFuncKind::Aggregate(crate::include::nodes::primnodes::Aggref {
        aggfnoid: resolved
            .as_ref()
            .map(|call| call.proc_oid)
            .or_else(|| proc_oid_for_builtin_aggregate_function(func))
            .unwrap_or(0),
        aggtype: aggregate_sql_type(func, arg_types.first().copied()),
        aggvariadic: resolved
            .as_ref()
            .map(|call| call.func_variadic)
            .unwrap_or(func_variadic),
        aggdistinct: distinct,
        direct_args: Vec::new(),
        args: coerced_args.clone(),
        aggorder: bound_order_by,
        aggfilter: bound_filter,
        agglevelsup: 0,
        aggno: 0,
    });
    Ok(register_window_expr(
        &state,
        spec,
        kind,
        coerced_args,
        aggregate_sql_type(func, arg_types.first().copied()),
    ))
}

fn bind_visible_outer_aggregate_call(
    name: &str,
    direct_args: &[SqlFunctionArg],
    args: &SqlCallArgs,
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    _scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    _grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<Expr>, ParseError> {
    let hypothetical =
        resolve_builtin_hypothetical_aggregate(name).is_some() && !direct_args.is_empty();
    let Some((aggno, visible_scope)) = match_visible_aggregate_call(
        name,
        direct_args,
        args,
        order_by,
        distinct,
        func_variadic,
        filter,
        catalog,
        outer_scopes,
        ctes,
    ) else {
        return Ok(None);
    };
    let owner_scope = &visible_scope.input_scope;
    let owner_outer_scopes = outer_scopes.get(visible_scope.levelsup..).unwrap_or(&[]);
    let arg_values = args
        .args()
        .iter()
        .map(|arg| arg.value.clone())
        .collect::<Vec<_>>();
    if !hypothetical && let Some(func) = resolve_builtin_aggregate(name) {
        validate_aggregate_arity(func, &arg_values)?;
    }
    let arg_types = arg_values
        .iter()
        .map(|expr| {
            infer_sql_expr_type_with_ctes(
                expr,
                owner_scope,
                catalog,
                owner_outer_scopes,
                None,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let resolved = if hypothetical {
        None
    } else {
        Some(
            resolve_aggregate_call(catalog, name, &arg_types, func_variadic).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected: "supported aggregate",
                    actual: name.to_string(),
                }
            })?,
        )
    };
    if let Some(resolved) = resolved.as_ref()
        && resolved.is_custom()
    {
        if distinct {
            return Err(ParseError::FeatureNotSupported(format!(
                "DISTINCT on custom aggregate {name}"
            )));
        }
        if !order_by.is_empty() {
            return Err(ParseError::FeatureNotSupported(format!(
                "aggregate ORDER BY on custom aggregate {name}"
            )));
        }
    }
    let bound_args = arg_values
        .iter()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                owner_scope,
                catalog,
                owner_outer_scopes,
                None,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for arg in &bound_args {
        reject_nested_local_ctes_in_agg_expr(arg)?;
        if expr_contains_set_returning(arg) {
            return Err(set_returning_not_allowed_error("aggregate arguments"));
        }
    }
    let bound_direct_args = if hypothetical {
        if aggregate_args_are_named(direct_args) {
            return Err(ParseError::UnexpectedToken {
                expected: "aggregate arguments without names",
                actual: name.to_string(),
            });
        }
        direct_args
            .iter()
            .map(|arg| {
                bind_expr_with_outer_and_ctes(
                    &arg.value,
                    owner_scope,
                    catalog,
                    owner_outer_scopes,
                    None,
                    ctes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    for arg in &bound_direct_args {
        reject_nested_local_ctes_in_agg_expr(arg)?;
        if expr_contains_set_returning(arg) {
            return Err(set_returning_not_allowed_error(
                "ordered-set aggregate direct arguments",
            ));
        }
    }
    let bound_filter = filter
        .map(|expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                owner_scope,
                catalog,
                owner_outer_scopes,
                None,
                ctes,
            )
        })
        .transpose()?;
    if let Some(filter) = &bound_filter {
        reject_nested_local_ctes_in_agg_expr(filter)?;
        if expr_contains_set_returning(filter) {
            return Err(set_returning_not_allowed_error("aggregate FILTER"));
        }
    }
    let bound_order_exprs = order_by
        .iter()
        .map(|item| {
            bind_expr_with_outer_and_ctes(
                &item.expr,
                owner_scope,
                catalog,
                owner_outer_scopes,
                None,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for item in &bound_order_exprs {
        reject_nested_local_ctes_in_agg_expr(item)?;
        if expr_contains_set_returning(item) {
            return Err(set_returning_not_allowed_error("aggregate ORDER BY"));
        }
    }
    let (coerced_direct_args, coerced_args, bound_order_by) = if hypothetical {
        let direct_arg_types = direct_args
            .iter()
            .map(|arg| {
                infer_sql_expr_type_with_ctes(
                    &arg.value,
                    owner_scope,
                    catalog,
                    owner_outer_scopes,
                    None,
                    ctes,
                )
            })
            .collect::<Vec<_>>();
        coerce_hypothetical_aggregate_inputs(
            name,
            direct_args,
            &direct_arg_types,
            bound_direct_args,
            args.args(),
            &arg_types,
            bound_args,
            order_by,
            bound_order_exprs,
            catalog,
        )?
    } else {
        let bound_order_by = bound_order_exprs
            .into_iter()
            .zip(order_by.iter())
            .map(|(bound_expr, item)| build_bound_order_by_entry(item, bound_expr, 0, catalog))
            .collect::<Result<Vec<_>, ParseError>>()?;
        let resolved = resolved
            .as_ref()
            .expect("non-hypothetical aggregate resolution should exist");
        let coerced_args = bound_args
            .into_iter()
            .zip(arg_types.iter().copied())
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect();
        let coerced_args =
            preserve_array_agg_array_arg_type(resolved.builtin_impl, &arg_types, coerced_args);
        (Vec::new(), coerced_args, bound_order_by)
    };
    let (aggfnoid, aggtype, aggvariadic) = if hypothetical {
        let resolved = resolve_hypothetical_aggregate_call(name).ok_or_else(|| {
            ParseError::UnexpectedToken {
                expected: "supported aggregate",
                actual: name.to_string(),
            }
        })?;
        (resolved.proc_oid, resolved.result_type, false)
    } else {
        let resolved = resolved
            .as_ref()
            .expect("non-hypothetical aggregate resolution should exist");
        (
            resolved.proc_oid,
            resolved.result_type,
            resolved.func_variadic,
        )
    };
    Ok(Some(Expr::Aggref(Box::new(
        crate::include::nodes::primnodes::Aggref {
            aggfnoid,
            aggtype,
            aggvariadic,
            aggdistinct: distinct,
            direct_args: coerced_direct_args,
            args: coerced_args,
            aggorder: bound_order_by,
            aggfilter: bound_filter,
            agglevelsup: visible_scope.levelsup,
            aggno,
        },
    ))))
}

fn preserve_array_agg_array_arg_type(
    func: Option<AggFunc>,
    arg_types: &[SqlType],
    mut args: Vec<Expr>,
) -> Vec<Expr> {
    if func == Some(AggFunc::ArrayAgg)
        && let (Some(arg_type), Some(first_arg)) = (arg_types.first().copied(), args.first_mut())
        && arg_type.is_array
        && !expr_sql_type_hint(first_arg).is_some_and(|ty| ty.is_array)
    {
        *first_arg = Expr::Cast(Box::new(first_arg.clone()), arg_type);
    }
    args
}

fn bind_window_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    over: &RawWindowSpec,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let state = current_window_state_or_error()?;
    let actual_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let mut resolution_types = actual_types.clone();
    if matches!(args.len(), 3)
        && !func_variadic
        && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
    {
        let common_type = infer_common_scalar_expr_type_with_ctes(
            &[args[0].value.clone(), args[2].value.clone()],
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
            "lag/lead value and default arguments with a common type",
        )?;
        resolution_types[0] = common_type;
        resolution_types[2] = common_type;
    }
    let resolved = resolve_function_call(catalog, name, &resolution_types, func_variadic)?;
    if resolved.proretset || !matches!(resolved.prokind, 'w' | 'a') {
        return Err(ParseError::UnexpectedToken {
            expected: "window or aggregate function",
            actual: name.to_string(),
        });
    }
    let spec = bind_window_spec(over, catalog, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    if let Some(window_impl) = resolved.window_impl {
        if args.iter().any(|arg| arg.name.is_some()) {
            return Err(ParseError::FeatureNotSupported(
                "named arguments are not supported for window functions".into(),
            ));
        }
        let bound_args = args
            .iter()
            .map(|arg| {
                with_windows_disallowed(|| {
                    bind_expr_with_outer_and_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if bound_args.iter().any(expr_contains_set_returning) {
            return Err(set_returning_not_allowed_error("window function arguments"));
        }
        let coerced_args = bound_args
            .into_iter()
            .zip(actual_types.iter().copied())
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect::<Vec<_>>();
        return Ok(register_window_expr(
            &state,
            spec,
            WindowFuncKind::Builtin(window_impl),
            coerced_args,
            resolved.result_type,
        ));
    }
    if resolved.prokind == 'a' {
        if let Some(agg_impl) = resolved.agg_impl {
            return bind_window_agg_call(
                agg_impl,
                args,
                &[],
                false,
                resolved.func_variadic,
                None,
                over,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        return Err(ParseError::FeatureNotSupported(format!(
            "window execution for custom aggregate {name}"
        )));
    }
    Err(ParseError::FeatureNotSupported(format!(
        "window function {name}"
    )))
}

pub(crate) fn bind_expr_with_outer_and_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if matches_grouped_outer_expr(expr, grouped_outer) {
        return bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, None, ctes);
    }

    Ok(match expr {
        SqlExpr::Xml(xml) => {
            return bind_xml_expr(xml, scope, catalog, outer_scopes, grouped_outer, ctes);
        }
        SqlExpr::Column(name) => {
            if let Some(relation_name) = name.strip_suffix(".*") {
                let fields =
                    resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                        .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
                build_whole_row_expr(fields)
            } else if let Some(system_column) =
                resolve_system_column_with_outer(scope, outer_scopes, name)?
            {
                Expr::Var(crate::include::nodes::primnodes::Var {
                    varno: system_column.varno,
                    varattno: system_column.varattno,
                    varlevelsup: system_column.varlevelsup,
                    vartype: system_column.sql_type,
                })
            } else {
                match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                    Ok(ResolvedColumn::Local(index)) => scope.output_exprs.get(index).cloned().unwrap_or_else(|| {
                        panic!("bound scope output_exprs missing local column {index} for {name}")
                    }),
                    Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                        .get(depth)
                        .and_then(|scope| scope.output_exprs.get(index))
                        .cloned()
                        .map(|expr| raise_expr_varlevels(expr, depth + 1))
                        .unwrap_or_else(|| {
                            panic!(
                                "outer scope output_exprs missing outer column depth={} index={} for {}",
                                depth, index, name
                            )
                        }),
                    Err(ParseError::UnknownColumn(_))
                        if resolve_relation_row_expr_with_outer(scope, outer_scopes, name).is_some() =>
                    {
                        let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                            .expect("checked above");
                        build_whole_row_expr(fields)
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        SqlExpr::Default => {
            return Err(ParseError::UnexpectedToken {
                expected: "expression",
                actual: "DEFAULT".into(),
            });
        }
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::IntegerLiteral(value) => Expr::Const(bind_integer_literal(value)?),
        SqlExpr::NumericLiteral(value) => Expr::Const(bind_numeric_literal(value)?),
        SqlExpr::Row(items) => {
            let field_exprs =
                bind_row_expr_fields(items, scope, catalog, outer_scopes, grouped_outer, ctes)?;
            let descriptor = assign_anonymous_record_descriptor(
                field_exprs
                    .iter()
                    .map(|(name, expr)| {
                        (
                            name.clone(),
                            expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                        )
                    })
                    .collect(),
            );
            Expr::Row {
                descriptor,
                fields: field_exprs,
            }
        }
        SqlExpr::Overlaps(left, right) => {
            return bind_overlaps_expr(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::BinaryOperator { op, left, right } => match op.as_str() {
            "@@" => bind_overloaded_binary_expr(
                "@@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "&&" => bind_overloaded_binary_expr(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "~<~" => bind_text_pattern_comparison_expr(
                "~<~",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "~<=~" => bind_text_pattern_comparison_expr(
                "~<=~",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "~>=~" => bind_text_pattern_comparison_expr(
                "~>=~",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "~>~" => bind_text_pattern_comparison_expr(
                "~>~",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "^@" => bind_text_starts_with_expr(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "<<=" => bind_overloaded_binary_expr(
                "<<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            ">>=" => bind_overloaded_binary_expr(
                ">>=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "-|-" => bind_overloaded_binary_expr(
                "-|-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "<%" => bind_catalog_binary_operator_expr(
                "<%",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "bound builtin operator",
                    actual: format!("unsupported operator {op}"),
                });
            }
        },
        SqlExpr::Add(left, right) => {
            if let Some(result) = bind_maybe_multirange_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_network_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "+",
                    OpExprKind::Add,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Sub(left, right) => {
            if let Some(result) = bind_maybe_jsonb_delete(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_multirange_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_network_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "-",
                    OpExprKind::Sub,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::BitAnd(left, right) => {
            if let Some(result) = bind_maybe_network_bitwise(
                "&",
                OpExprKind::BitAnd,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_bitwise_expr(
                    "&",
                    OpExprKind::BitAnd,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::BitOr(left, right) => {
            if let Some(result) = bind_maybe_network_bitwise(
                "|",
                OpExprKind::BitOr,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_bitwise_expr(
                    "|",
                    OpExprKind::BitOr,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::BitXor(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "#",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_bitwise_expr(
                    "#",
                    OpExprKind::BitXor,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shl(left, right) => {
            if let Some(result) = bind_maybe_network_operator(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_multirange_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    "<<",
                    OpExprKind::Shl,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shr(left, right) => {
            if let Some(result) = bind_maybe_network_operator(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_multirange_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    ">>",
                    OpExprKind::Shr,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mul(left, right) => {
            if let Some(result) = bind_maybe_multirange_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "*",
                    OpExprKind::Mul,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Div(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "/",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "/",
                    OpExprKind::Div,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mod(left, right) => bind_arithmetic_expr(
            "%",
            OpExprKind::Mod,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Concat(left, right) => bind_concat_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::UnaryPlus(inner) => Expr::op_auto(
            OpExprKind::UnaryPlus,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::PrefixOperator { op, expr } => bind_prefix_operator_expr(
            op.as_str(),
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Negate(inner) => Expr::op_auto(
            OpExprKind::Negate,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::BitNot(inner) => {
            let inner_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let is_network = !inner_type.is_array
                && matches!(inner_type.kind, SqlTypeKind::Inet | SqlTypeKind::Cidr);
            if !is_integer_family(inner_type)
                && !is_bit_string_type(inner_type)
                && !is_macaddr_type(inner_type)
                && !is_network
            {
                return Err(ParseError::UndefinedOperator {
                    op: "~",
                    left_type: sql_type_name(inner_type),
                    right_type: "unknown".to_string(),
                });
            }
            let result_type = if is_network {
                SqlType::new(SqlTypeKind::Inet)
            } else {
                inner_type
            };
            Expr::unary_op(
                OpExprKind::BitNot,
                result_type,
                bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            )
        }
        SqlExpr::Cast(inner, ty) => {
            let target_type = resolve_raw_type_name(ty, catalog)?;
            let source_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_expr_with_outer_and_ctes(
                                element,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: target_type,
                }
            } else {
                bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            };
            let domain = domain_lookup_for_raw_type_name(ty, catalog);
            if let SqlExpr::Negate(negated_inner) = inner.as_ref()
                && matches!(
                    target_type.kind,
                    SqlTypeKind::Float4
                        | SqlTypeKind::Float8
                        | SqlTypeKind::Numeric
                        | SqlTypeKind::Money
                        | SqlTypeKind::Interval
                )
            {
                let negated_source_type = infer_sql_expr_type_with_ctes(
                    negated_inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !matches!(negated_inner.as_ref(), SqlExpr::Const(Value::Null)) {
                    validate_catalog_backed_explicit_cast(
                        negated_source_type,
                        target_type,
                        catalog,
                    )?;
                }
                let bound_negated_inner = bind_expr_with_outer_and_ctes(
                    negated_inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let coerced_inner =
                    coerce_bound_expr(bound_negated_inner, negated_source_type, target_type);
                return Ok(bind_domain_constraint_expr(
                    Expr::op_auto(OpExprKind::Negate, vec![coerced_inner]),
                    target_type,
                    domain.as_ref(),
                ));
            }
            if target_type.kind == SqlTypeKind::RegRole
                && let Some(bound_regrole) = bind_regrole_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regrole);
            }
            if target_type.kind == SqlTypeKind::RegClass
                && let Some(bound_regclass) =
                    bind_regclass_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regclass);
            }
            if target_type.kind == SqlTypeKind::RegOperator
                && let Some(bound_regoperator) =
                    bind_regoperator_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regoperator);
            }
            if target_type.kind == SqlTypeKind::RegType
                && let Some(bound_regtype) = bind_regtype_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regtype);
            }
            if target_type.kind == SqlTypeKind::RegProcedure
                && let Some(bound_regprocedure) =
                    bind_regprocedure_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regprocedure);
            }
            if let SqlExpr::Row(items) = inner.as_ref()
                && let Some(bound_row) = bind_named_composite_row_cast(
                    items,
                    target_type,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            {
                return Ok(bound_row);
            }
            if !matches!(inner.as_ref(), SqlExpr::Const(Value::Null)) {
                validate_catalog_backed_explicit_cast(source_type, target_type, catalog)?;
            }
            let cast_expr =
                bind_explicit_cast_expr(bound_inner, source_type, target_type, catalog)?;
            bind_domain_constraint_expr(cast_expr, target_type, domain.as_ref())
        }
        SqlExpr::Collate { expr, collation } => {
            let inner_type = infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound_inner = bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            bind_explicit_collation(bound_inner, inner_type, collation, catalog)?
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            let source_type = infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let zone_type = infer_sql_expr_type_with_ctes(
                zone,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let source_is_timestamptz = matches!(source_type.kind, SqlTypeKind::TimestampTz)
                || matches!(
                    expr.as_ref(),
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                );
            let source_is_time = matches!(source_type.kind, SqlTypeKind::Time);
            let source_is_timetz = matches!(source_type.kind, SqlTypeKind::TimeTz);
            let target_source_type = if source_is_timetz {
                source_type
            } else if source_is_time {
                source_type
            } else if source_is_timestamptz {
                SqlType::new(SqlTypeKind::TimestampTz)
            } else {
                SqlType::new(SqlTypeKind::Timestamp)
            };
            let result_type = if source_is_timetz || source_is_time {
                SqlType::new(SqlTypeKind::TimeTz)
            } else if source_is_timestamptz {
                SqlType::new(SqlTypeKind::Timestamp)
            } else {
                SqlType::new(SqlTypeKind::TimestampTz)
            };
            let zone_target_type = if matches!(
                zone_type.kind,
                SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
            ) {
                SqlType::new(SqlTypeKind::Text)
            } else {
                SqlType::new(SqlTypeKind::Interval)
            };
            let bound_expr = bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let bound_zone = bind_expr_with_outer_and_ctes(
                zone,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Expr::builtin_func(
                BuiltinScalarFunction::Timezone,
                Some(result_type),
                false,
                vec![
                    coerce_bound_expr(bound_zone, zone_type, zone_target_type),
                    coerce_bound_expr(bound_expr, source_type, target_source_type),
                ],
            )
        }
        SqlExpr::Eq(left, right) => {
            if let (SqlExpr::Row(left_items), SqlExpr::Row(right_items)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_comparison_expr(
                    "=",
                    OpExprKind::Eq,
                    left_items,
                    right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let (SqlExpr::Row(_), SqlExpr::ScalarSubquery(subquery)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_compare_subquery_expr(
                    left,
                    SubqueryComparisonOp::Eq,
                    subquery,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "=",
                    OpExprKind::Eq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::NotEq(left, right) => {
            if let (SqlExpr::Row(left_items), SqlExpr::Row(right_items)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_comparison_expr(
                    "<>",
                    OpExprKind::NotEq,
                    left_items,
                    right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let (SqlExpr::Row(_), SqlExpr::ScalarSubquery(subquery)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_compare_subquery_expr(
                    left,
                    SubqueryComparisonOp::NotEq,
                    subquery,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<>",
                    OpExprKind::NotEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Lt(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<",
                    OpExprKind::Lt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::LtEq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<=",
                    OpExprKind::LtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Gt(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">",
                    OpExprKind::Gt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::GtEq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">=",
                    OpExprKind::GtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::RegexMatch(left, right) => Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new({
                let bound = bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                strip_explicit_collation(bound).0
            }),
            pattern: Box::new({
                let bound = bind_expr_with_outer_and_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                strip_explicit_collation(bound).0
            }),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            case_insensitive: *case_insensitive,
            negated: *negated,
            collation_oid: {
                let bound_expr = bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let bound_pattern = bind_expr_with_outer_and_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let (_, expr_explicit_collation) = strip_explicit_collation(bound_expr);
                let (_, pattern_explicit_collation) = strip_explicit_collation(bound_pattern);
                derive_consumer_collation(
                    catalog,
                    if *case_insensitive {
                        CollationConsumer::ILike
                    } else {
                        CollationConsumer::Like
                    },
                    &[
                        (
                            infer_sql_expr_type_with_ctes(
                                expr,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            ),
                            expr_explicit_collation,
                        ),
                        (
                            infer_sql_expr_type_with_ctes(
                                pattern,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            ),
                            pattern_explicit_collation,
                        ),
                    ],
                )?
            },
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new({
                let bound = bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                strip_explicit_collation(bound).0
            }),
            pattern: Box::new({
                let bound = bind_expr_with_outer_and_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                strip_explicit_collation(bound).0
            }),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            negated: *negated,
            collation_oid: {
                let bound_expr = bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let bound_pattern = bind_expr_with_outer_and_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let (_, expr_explicit_collation) = strip_explicit_collation(bound_expr);
                let (_, pattern_explicit_collation) = strip_explicit_collation(bound_pattern);
                derive_consumer_collation(
                    catalog,
                    CollationConsumer::Similar,
                    &[
                        (
                            infer_sql_expr_type_with_ctes(
                                expr,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            ),
                            expr_explicit_collation,
                        ),
                        (
                            infer_sql_expr_type_with_ctes(
                                pattern,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            ),
                            pattern_explicit_collation,
                        ),
                    ],
                )?
            },
        },
        SqlExpr::And(left, right) => Expr::bool_expr(
            BoolExprType::And,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Or(left, right) => Expr::bool_expr(
            BoolExprType::Or,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Not(inner) => Expr::bool_expr(
            BoolExprType::Not,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsDistinctFrom(left, right) => {
            if let (SqlExpr::Row(left_items), SqlExpr::Row(right_items)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_distinct_expr(
                    false,
                    left_items,
                    right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else {
                Expr::IsDistinctFrom(
                    Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                    Box::new(bind_expr_with_outer_and_ctes(
                        right,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                )
            }
        }
        SqlExpr::IsNotDistinctFrom(left, right) => {
            if let (SqlExpr::Row(left_items), SqlExpr::Row(right_items)) =
                (left.as_ref(), right.as_ref())
            {
                bind_row_distinct_expr(
                    true,
                    left_items,
                    right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else {
                Expr::IsNotDistinctFrom(
                    Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                    Box::new(bind_expr_with_outer_and_ctes(
                        right,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                )
            }
        }
        SqlExpr::ArrayLiteral(elements) => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_expr_with_outer_and_ctes(
                        element,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type_with_ctes(
                elements,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
            .ok_or_else(|| ParseError::DetailedError {
                message: "cannot determine type of empty array".into(),
                detail: None,
                hint: Some(
                    "Explicitly cast to the desired type, for example ARRAY[]::integer[].".into(),
                ),
                sqlstate: "42P18",
            })?,
        },
        SqlExpr::ArraySubscript { array, subscripts } => {
            let array_type = infer_sql_expr_type_with_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if array_type.kind == SqlTypeKind::Jsonb && !array_type.is_array {
                return bind_jsonb_subscript_expr(
                    array,
                    subscripts,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if array_type.kind == SqlTypeKind::Point
                && !array_type.is_array
                && let Some(index) = point_coordinate_subscript(subscripts)
            {
                return bind_geometry_subscript(
                    array,
                    index,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if array_type.kind == SqlTypeKind::Point
                && subscripts.iter().any(|subscript| subscript.is_slice)
            {
                return Err(fixed_length_array_slice_error());
            }
            if !supports_array_subscripts(array_type) {
                return Err(unsupported_subscript_type_error(array_type));
            }
            Expr::ArraySubscript {
                array: Box::new(bind_expr_with_outer_and_ctes(
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| {
                        Ok(crate::include::nodes::primnodes::ExprArraySubscript {
                            is_slice: subscript.is_slice,
                            lower: subscript
                                .lower
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                            upper: subscript
                                .upper
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                        })
                    })
                    .collect::<Result<_, ParseError>>()?,
            }
        }
        SqlExpr::ArrayOverlap(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                let raw_left_type = infer_sql_expr_type_with_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let raw_right_type = infer_sql_expr_type_with_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let left_bound = bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let right_bound = bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let mut left_type =
                    coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
                let mut right_type =
                    coerce_unknown_string_literal_type(right, raw_right_type, left_type);
                let left_expr = if matches!(
                    &**left,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !left_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &right_bound {
                        left_type = *array_type;
                    }
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                } else {
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                };
                let right_expr = if matches!(
                    &**right,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !right_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &left_expr {
                        right_type = *array_type;
                    }
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                } else {
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                };
                Expr::op_auto(OpExprKind::ArrayOverlap, vec![left_expr, right_expr])
            }
        }
        SqlExpr::ArrayContains(left, right) => bind_array_membership_expr(
            OpExprKind::ArrayContains,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::ArrayContained(left, right) => bind_array_membership_expr(
            OpExprKind::ArrayContained,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::ScalarSubquery(select) => {
            bind_scalar_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::ArraySubquery(select) => {
            bind_array_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::Exists(select) => {
            bind_exists_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_in_subquery_expr(
            expr,
            subquery,
            *negated,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => bind_quantified_subquery_expr(
            left,
            *op,
            *is_all,
            subquery,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => bind_quantified_array_expr(
            left,
            *op,
            *is_all,
            array,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGet,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonGetText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGetText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPath(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPath,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPathText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPathText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbContains(left, right) => {
            if let Some(result) = bind_maybe_multirange_contains(
                "@>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_contains(
                "@>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_array_membership_expr(
                OpExprKind::ArrayContains,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_jsonb_contains_expr(
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::JsonbContained(left, right) => {
            if let Some(result) = bind_maybe_multirange_contains(
                "<@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_contains(
                "<@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_array_membership_expr(
                OpExprKind::ArrayContained,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_jsonb_contained_expr(
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::JsonbExists(left, right) => bind_jsonb_exists_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAny(left, right) => bind_jsonb_exists_any_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAll(left, right) => bind_jsonb_exists_all_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathExists(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathExists,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathMatch(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathMatch,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            over,
        } => {
            let args_list = args.args();
            let (direct_args, aggregate_args, aggregate_order_by) =
                normalize_aggregate_call(args, order_by, within_group.as_deref());
            if over.is_none()
                && within_group.is_none()
                && resolve_builtin_hypothetical_aggregate(name).is_some()
            {
                return Err(ordered_set_requires_within_group_error(name));
            }
            if within_group.is_some() && resolve_builtin_hypothetical_aggregate(name).is_none() {
                return Err(not_ordered_set_aggregate_error(name));
            }
            if let Some(func) = resolve_builtin_aggregate(name) {
                if let Some(raw_over) = over {
                    return bind_window_agg_call(
                        func,
                        args_list,
                        order_by,
                        *distinct,
                        *func_variadic,
                        filter.as_deref(),
                        raw_over,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                if let Some(bound_outer_agg) = bind_visible_outer_aggregate_call(
                    name,
                    &direct_args,
                    &aggregate_args,
                    &aggregate_order_by,
                    *distinct,
                    *func_variadic,
                    filter.as_deref(),
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )? {
                    return Ok(bound_outer_agg);
                }
                return Err(ParseError::UnexpectedToken {
                    expected: "non-aggregate expression",
                    actual: "aggregate function".into(),
                });
            }
            if within_group.is_some() {
                if let Some(bound_outer_agg) = bind_visible_outer_aggregate_call(
                    name,
                    &direct_args,
                    &aggregate_args,
                    &aggregate_order_by,
                    *distinct,
                    *func_variadic,
                    filter.as_deref(),
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )? {
                    return Ok(bound_outer_agg);
                }
                return Err(ParseError::UnexpectedToken {
                    expected: "non-aggregate expression",
                    actual: "aggregate function".into(),
                });
            }
            if let Some(raw_over) = over {
                return bind_window_func_call(
                    name,
                    args_list,
                    *func_variadic,
                    raw_over,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("row_to_json") {
                return bind_row_to_json_call(
                    name,
                    args_list,
                    *func_variadic,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("coalesce") {
                return bind_coalesce_call(
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("nullif") {
                return bind_nullif_call(
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if !order_by.is_empty() || *distinct || filter.is_some() || args.is_star() {
                return Err(ParseError::UnexpectedToken {
                    expected: "supported scalar function",
                    actual: name.clone(),
                });
            }
            if !*func_variadic
                && !name.eq_ignore_ascii_case("pg_lsn")
                && let Some(target_type) = resolve_function_cast_type(catalog, name)
                && args_list.len() == 1
                && args_list.iter().all(|arg| arg.name.is_none())
            {
                let arg_type = infer_sql_expr_type_with_ctes(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let bound_arg = bind_expr_with_outer_and_ctes(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                if catalog_backed_explicit_cast_allowed(arg_type, target_type, catalog) {
                    return Ok(Expr::Cast(
                        Box::new(bound_arg),
                        if arg_type == target_type {
                            arg_type
                        } else {
                            target_type
                        },
                    ));
                }
            }
            let actual_types = args_list
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            let mut resolution_types = actual_types.clone();
            if matches!(args_list.len(), 3)
                && !*func_variadic
                && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
            {
                let common_type = infer_common_scalar_expr_type_with_ctes(
                    &[args_list[0].value.clone(), args_list[2].value.clone()],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "lag/lead value and default arguments with a common type",
                )?;
                resolution_types[0] = common_type;
                resolution_types[2] = common_type;
            }
            let proc_resolution_error =
                match resolve_function_call(catalog, name, &resolution_types, *func_variadic) {
                    Ok(resolved) => {
                        if resolved.window_impl.is_some() {
                            return Err(window_function_requires_over_error(name));
                        }
                        if resolved.prokind != 'f' {
                            return Err(ParseError::DetailedError {
                                message: format!(
                                    "{} is a procedure",
                                    function_call_signature_text(name, args_list, &actual_types)
                                ),
                                detail: None,
                                hint: Some("To call a procedure, use CALL.".into()),
                                sqlstate: "42809",
                            });
                        }
                        if resolved.proretset {
                            return bind_set_returning_expr_from_parts(
                                name,
                                args_list,
                                *func_variadic,
                                None,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            );
                        }
                        if let Some(func) = resolved.scalar_impl {
                            let lowered_args = lower_named_scalar_function_args(func, args_list)?;
                            return bind_scalar_function_call(
                                func,
                                resolved.proc_oid,
                                Some(resolved.result_type),
                                resolved.func_variadic,
                                resolved.nvargs,
                                resolved.vatype_oid,
                                &resolved.declared_arg_types,
                                &lowered_args,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            );
                        }
                        return bind_user_defined_scalar_function_call(
                            resolved.proc_oid,
                            Some(resolved.proname.clone()),
                            resolved.result_type,
                            &resolved.declared_arg_types,
                            args_list,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                    }
                    Err(err) => Some(err),
                };
            if name.eq_ignore_ascii_case("xmlconcat") {
                if args.args().iter().any(|arg| arg.name.is_some()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "positional xmlconcat arguments",
                        actual: "named argument".into(),
                    });
                }
                let xml_type = SqlType::new(SqlTypeKind::Xml);
                let bound_args = args
                    .args()
                    .iter()
                    .map(|arg| {
                        let source = infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        Ok(coerce_bound_expr(
                            bind_expr_with_outer_and_ctes(
                                &arg.value,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )?,
                            source,
                            xml_type,
                        ))
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?;
                return Ok(Expr::Xml(Box::new(
                    crate::include::nodes::primnodes::XmlExpr {
                        op: crate::include::nodes::primnodes::XmlExprOp::Concat,
                        name: None,
                        named_args: Vec::new(),
                        arg_names: Vec::new(),
                        args: bound_args,
                        xml_option: None,
                        indent: None,
                        target_type: None,
                        standalone: None,
                    },
                )));
            }
            if root_call_returns_set(
                name,
                args_list,
                *func_variadic,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                return bind_set_returning_expr_from_parts(
                    name,
                    args_list,
                    *func_variadic,
                    None,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            let legacy_func = match resolve_scalar_function(name) {
                Some(func) => func,
                None => {
                    if !catalog.proc_rows_by_name(name).is_empty()
                        && let Some(err) = proc_resolution_error
                    {
                        if matches!(
                            err,
                            ParseError::UnexpectedToken {
                                expected: "supported function",
                                ..
                            }
                        ) {
                            return Err(function_does_not_exist_error(
                                name,
                                &actual_types,
                                catalog,
                            ));
                        }
                        return Err(err);
                    }
                    return Err(ParseError::UnexpectedToken {
                        expected: "supported builtin function",
                        actual: name.clone(),
                    });
                }
            };
            let lowered_args = lower_named_scalar_function_args(legacy_func, args_list)?;
            let actual_types = lowered_args
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            validate_scalar_function_arity(legacy_func, &lowered_args)?;
            let legacy_result_type =
                if matches!(legacy_func, BuiltinScalarFunction::RangeConstructor) {
                    resolve_function_cast_type(catalog, name)
                        .filter(|ty| range_type_ref_for_sql_type(*ty).is_some())
                } else if matches!(
                    legacy_func,
                    BuiltinScalarFunction::Greatest | BuiltinScalarFunction::Least
                ) {
                    infer_common_scalar_expr_type_with_ctes(
                        &lowered_args,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                        "GREATEST/LEAST arguments with a common type",
                    )
                    .ok()
                } else {
                    None
                };
            let legacy_vatype_oid = if *func_variadic
                && matches!(
                    legacy_func,
                    BuiltinScalarFunction::Concat
                        | BuiltinScalarFunction::ConcatWs
                        | BuiltinScalarFunction::Format
                        | BuiltinScalarFunction::JsonBuildArray
                        | BuiltinScalarFunction::JsonBuildObject
                        | BuiltinScalarFunction::JsonbBuildArray
                        | BuiltinScalarFunction::JsonbBuildObject
                ) {
                ANYOID
            } else {
                0
            };
            let legacy_declared_arg_types = if let Some(range_type) =
                legacy_result_type.and_then(range_type_ref_for_sql_type)
            {
                let mut declared = vec![range_type.subtype, range_type.subtype];
                if lowered_args.len() == 3 {
                    declared.push(SqlType::new(SqlTypeKind::Text));
                }
                declared
            } else {
                actual_types.clone()
            };
            bind_scalar_function_call(
                legacy_func,
                0,
                legacy_result_type,
                *func_variadic,
                0,
                legacy_vatype_oid,
                &legacy_declared_arg_types,
                &lowered_args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        }
        SqlExpr::Subscript { expr, index } => bind_geometry_subscript(
            expr,
            *index,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::GeometryUnaryOp { op, expr } => {
            bind_geometry_unary_expr(*op, expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
        SqlExpr::GeometryBinaryOp { op, left, right } => {
            if matches!(op, GeometryBinaryOp::OverLeft | GeometryBinaryOp::OverRight) {
                let range_op = if matches!(op, GeometryBinaryOp::OverLeft) {
                    "&<"
                } else {
                    "&>"
                };
                if let Some(result) = bind_maybe_multirange_over_position(
                    range_op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ) {
                    result?
                } else if let Some(result) = bind_maybe_range_over_position(
                    range_op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ) {
                    result?
                } else {
                    bind_geometry_binary_expr(
                        *op,
                        left,
                        right,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?
                }
            } else {
                bind_geometry_binary_expr(
                    *op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => bind_case_expr(
            arg.as_deref(),
            args,
            defresult.as_deref(),
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FieldSelect { expr, field } => {
            if let SqlExpr::FuncCall {
                name,
                args,
                order_by,
                within_group,
                distinct,
                func_variadic,
                filter,
                over,
            } = expr.as_ref()
                && order_by.is_empty()
                && within_group.is_none()
                && !*distinct
                && filter.is_none()
                && over.is_none()
                && root_call_returns_set(
                    name,
                    args.args(),
                    *func_variadic,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            {
                bind_set_returning_expr_from_parts(
                    name,
                    args.args(),
                    *func_variadic,
                    Some(field),
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else {
                bind_field_select_expr(
                    expr,
                    field,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::CurrentDate => Expr::CurrentDate,
        SqlExpr::CurrentCatalog => Expr::CurrentCatalog,
        SqlExpr::CurrentSchema => Expr::CurrentSchema,
        SqlExpr::CurrentUser => Expr::CurrentUser,
        SqlExpr::SessionUser => Expr::SessionUser,
        SqlExpr::CurrentRole => Expr::CurrentRole,
        SqlExpr::CurrentTime { precision } => Expr::CurrentTime {
            precision: *precision,
        },
        SqlExpr::CurrentTimestamp { precision } => Expr::CurrentTimestamp {
            precision: *precision,
        },
        SqlExpr::LocalTime { precision } => Expr::LocalTime {
            precision: *precision,
        },
        SqlExpr::LocalTimestamp { precision } => Expr::LocalTimestamp {
            precision: *precision,
        },
    })
}

fn bind_field_select_expr(
    expr: &SqlExpr,
    field: &str,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_inner = match expr {
        SqlExpr::Column(name)
            if resolve_relation_row_expr_with_outer(scope, outer_scopes, name).is_some() =>
        {
            let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                .expect("checked above");
            build_whole_row_expr(fields)
        }
        _ => {
            bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
    };
    let field_type = resolve_bound_field_select_type(&bound_inner, field, catalog)?;
    Ok(Expr::FieldSelect {
        expr: Box::new(bound_inner),
        field: field.to_string(),
        field_type,
    })
}

pub(crate) fn resolve_bound_field_select_type(
    expr: &Expr,
    field: &str,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    if let Expr::Row { descriptor, .. } = expr {
        if let Some(found) = descriptor
            .fields
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(field))
        {
            return Ok(found.sql_type);
        }
    }

    let Some(row_type) = expr_sql_type_hint(expr) else {
        return Err(ParseError::UnexpectedToken {
            expected: "record expression",
            actual: format!("field selection .{field}"),
        });
    };

    if matches!(row_type.kind, SqlTypeKind::Composite) && row_type.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(row_type.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "named composite type",
                actual: format!("type relation {} not found", row_type.typrelid),
            })?;
        if let Some(found) = relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(field))
        {
            return Ok(found.sql_type);
        }
    }

    if matches!(row_type.kind, SqlTypeKind::Record)
        && row_type.typmod > 0
        && let Some(descriptor) = lookup_anonymous_record_descriptor(row_type.typmod)
        && let Some(found) = descriptor
            .fields
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(field))
    {
        return Ok(found.sql_type);
    }

    Err(ParseError::UnexpectedToken {
        expected: "record field",
        actual: format!("field selection .{field}"),
    })
}

fn bind_case_expr(
    arg: Option<&SqlExpr>,
    args: &[SqlCaseWhen],
    defresult: Option<&SqlExpr>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "at least one WHEN clause",
            actual: "CASE".into(),
        });
    }

    let default_sql_expr = SqlExpr::Const(Value::Null);
    let default_expr = defresult.unwrap_or(&default_sql_expr);
    let bound_default = bind_typed_expr_with_outer_and_ctes(
        default_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    reject_typed_srf(&bound_default, "CASE")?;
    let mut bound_results = Vec::with_capacity(args.len() + 1);
    bound_results.push(bound_default);
    for arm in args {
        let bound_result = bind_typed_expr_with_outer_and_ctes(
            &arm.result,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        reject_typed_srf(&bound_result, "CASE")?;
        bound_results.push(bound_result);
    }
    let result_type =
        common_type_for_typed_exprs(&bound_results, "CASE result expressions with a common type")?;

    let (bound_arg, arg_type) = if let Some(arg) = arg {
        let bound_arg = bind_typed_expr_with_outer_and_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        reject_typed_srf(&bound_arg, "CASE")?;
        (Some(bound_arg.expr), Some(bound_arg.sql_type))
    } else {
        (None, None)
    };

    let mut bound_arms = Vec::with_capacity(args.len());
    for (arm, bound_result) in args.iter().zip(bound_results.iter().skip(1)) {
        let condition = if let Some(arg_type) = arg_type {
            let bound_expr = bind_typed_expr_with_outer_and_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            reject_typed_srf(&bound_expr, "CASE")?;
            bind_lowered_comparison_expr(
                "=",
                OpExprKind::Eq,
                Expr::CaseTest(Box::new(BoundCaseTestExpr { type_id: arg_type })),
                arg_type,
                arg_type,
                bound_expr.expr,
                bound_expr.sql_type,
                bound_expr.sql_type,
                None,
                None,
                catalog,
            )?
        } else {
            let bound_expr = bind_typed_expr_with_outer_and_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            reject_typed_srf(&bound_expr, "CASE")?;
            if bound_expr.sql_type != SqlType::new(SqlTypeKind::Bool) {
                return Err(ParseError::UnexpectedToken {
                    expected: "boolean CASE condition",
                    actual: "CASE WHEN expression must return boolean".into(),
                });
            }
            bound_expr.expr
        };
        bound_arms.push(BoundCaseWhen {
            expr: condition,
            result: coerce_bound_expr(
                bound_result.expr.clone(),
                bound_result.sql_type,
                result_type,
            ),
        });
    }

    let bound_default = bound_results
        .into_iter()
        .next()
        .expect("CASE default result bound before arms");

    Ok(Expr::Case(Box::new(BoundCaseExpr {
        casetype: result_type,
        arg: bound_arg.map(Box::new),
        args: bound_arms,
        defresult: Box::new(coerce_bound_expr(
            bound_default.expr,
            bound_default.sql_type,
            result_type,
        )),
    })))
}

fn bind_coalesce_call(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional COALESCE arguments",
            actual: "COALESCE with named arguments".into(),
        });
    }
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "at least one COALESCE argument",
            actual: format!("COALESCE({} args)", args.len()),
        });
    }
    let mut bound_args = Vec::with_capacity(args.len());
    for arg in args {
        let bound = bind_typed_expr_with_outer_and_ctes(
            &arg.value,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        reject_typed_srf(&bound, "COALESCE")?;
        bound_args.push(bound);
    }
    let common_type =
        common_type_for_typed_exprs(&bound_args, "COALESCE arguments with a common type")?;
    let mut iter = bound_args
        .into_iter()
        .map(|arg| coerce_bound_expr(arg.expr, arg.sql_type, common_type))
        .rev();
    let mut expr = iter.next().expect("coalesce arity validated");
    for arg in iter {
        expr = Expr::Coalesce(Box::new(arg), Box::new(expr));
    }
    Ok(expr)
}

fn bind_nullif_call(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional NULLIF arguments",
            actual: "NULLIF with named arguments".into(),
        });
    }
    if args.len() != 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "exactly two NULLIF arguments",
            actual: format!("NULLIF({} args)", args.len()),
        });
    }

    let left = bind_typed_expr_with_outer_and_ctes(
        &args[0].value,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    reject_typed_srf(&left, "NULLIF")?;
    let right = bind_typed_expr_with_outer_and_ctes(
        &args[1].value,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    reject_typed_srf(&right, "NULLIF")?;
    let (right_expr, right_type) = if matches!(right.expr, Expr::Const(Value::Null)) {
        (
            coerce_bound_expr(right.expr, right.sql_type, left.sql_type),
            left.sql_type,
        )
    } else {
        (right.expr, right.sql_type)
    };
    let comparison = bind_lowered_comparison_expr(
        "=",
        OpExprKind::Eq,
        left.expr.clone(),
        left.sql_type,
        left.sql_type,
        right_expr,
        right.sql_type,
        right_type,
        None,
        None,
        catalog,
    )?;

    Ok(Expr::Case(Box::new(BoundCaseExpr {
        casetype: left.sql_type,
        arg: None,
        args: vec![BoundCaseWhen {
            expr: comparison,
            result: Expr::Cast(Box::new(Expr::Const(Value::Null)), left.sql_type),
        }],
        defresult: Box::new(left.expr),
    })))
}

fn validate_catalog_backed_explicit_cast(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if matches!(
        (source_type.kind, target_type.kind),
        (SqlTypeKind::TimeTz, SqlTypeKind::Interval) | (SqlTypeKind::Interval, SqlTypeKind::TimeTz)
    ) && !source_type.is_array
        && !target_type.is_array
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot cast type {} to {}",
                sql_type_name(source_type),
                sql_type_name(target_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "42846",
        });
    }
    if catalog_backed_explicit_cast_allowed(source_type, target_type, catalog) {
        return Ok(());
    }
    Err(ParseError::UnexpectedToken {
        expected: "supported explicit cast",
        actual: format!(
            "cannot cast type {} to {}",
            catalog_sql_type_name(source_type, catalog),
            catalog_sql_type_name(target_type, catalog)
        ),
    })
}

fn catalog_sql_type_name(ty: SqlType, catalog: &dyn CatalogLookup) -> String {
    if !ty.is_array
        && ty.type_oid != 0
        && builtin_type_name_for_oid(ty.type_oid).is_none()
        && let Some(row) = catalog.type_by_oid(ty.type_oid)
    {
        return row.typname;
    }
    sql_type_name(ty)
}

fn bind_explicit_cast_expr(
    bound_inner: Expr,
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    let Some(source_oid) = catalog.type_oid_for_sql_type(source_type) else {
        return Ok(coerce_bound_expr(bound_inner, source_type, target_type));
    };
    let Some(target_oid) = catalog.type_oid_for_sql_type(target_type) else {
        return Ok(coerce_bound_expr(bound_inner, source_type, target_type));
    };
    let Some(cast_row) = catalog.cast_by_source_target(source_oid, target_oid) else {
        return Ok(fallback_explicit_cast_expr(
            bound_inner,
            source_type,
            target_type,
        ));
    };
    if cast_row.castmethod != 'f' || cast_row.castfunc == 0 {
        return Ok(coerce_bound_expr(bound_inner, source_type, target_type));
    }
    let Some(proc_row) = catalog.proc_row_by_oid(cast_row.castfunc) else {
        return Err(ParseError::UnexpectedToken {
            expected: "existing cast function",
            actual: format!("function OID {}", cast_row.castfunc),
        });
    };
    let builtin_impl = builtin_scalar_function_for_proc_oid(cast_row.castfunc);
    // Built-in pg_cast rows can point at internal C functions that pgrust's
    // generic cast executor already handles.
    if proc_row.prolang == PG_LANGUAGE_INTERNAL_OID && builtin_impl.is_none() {
        return Ok(fallback_explicit_cast_expr(
            bound_inner,
            source_type,
            target_type,
        ));
    }
    let first_arg_oid = proc_row
        .proargtypes
        .split_whitespace()
        .next()
        .and_then(|oid| oid.parse::<u32>().ok())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "cast function with at least one argument",
            actual: proc_row.proname.clone(),
        })?;
    let first_arg_type = catalog
        .type_by_oid(first_arg_oid)
        .map(|row| row.sql_type)
        .ok_or_else(|| ParseError::UnsupportedType(first_arg_oid.to_string()))?;
    let result_type = catalog
        .type_by_oid(proc_row.prorettype)
        .map(|row| row.sql_type)
        .ok_or_else(|| ParseError::UnsupportedType(proc_row.prorettype.to_string()))?;
    let arg = coerce_bound_expr(bound_inner, source_type, first_arg_type);
    let func_expr = Expr::func_with_impl(
        cast_row.castfunc,
        Some(result_type),
        false,
        builtin_impl
            .map(ScalarFunctionImpl::Builtin)
            .unwrap_or(ScalarFunctionImpl::UserDefined {
                proc_oid: cast_row.castfunc,
            }),
        vec![arg],
    );
    Ok(
        if proc_row.prorettype == target_oid || result_type == target_type {
            func_expr
        } else {
            coerce_bound_expr(func_expr, result_type, target_type)
        },
    )
}

fn fallback_explicit_cast_expr(
    bound_inner: Expr,
    source_type: SqlType,
    target_type: SqlType,
) -> Expr {
    let coerced = coerce_bound_expr(bound_inner, source_type, target_type);
    if expr_sql_type_hint(&coerced) == Some(target_type) {
        coerced
    } else {
        Expr::Cast(Box::new(coerced), target_type)
    }
}

pub(super) fn catalog_backed_explicit_cast_allowed(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> bool {
    if source_type.element_type() == target_type.element_type() {
        return true;
    }
    let source_oid = catalog.type_oid_for_sql_type(source_type);
    let target_oid = catalog.type_oid_for_sql_type(target_type);
    if let (Some(source_oid), Some(target_oid)) = (source_oid, target_oid) {
        if source_oid == target_oid
            || catalog
                .cast_by_source_target(source_oid, target_oid)
                .is_some()
        {
            return true;
        }
        if is_user_defined_base_type_oid(source_oid, catalog)
            || is_user_defined_base_type_oid(target_oid, catalog)
        {
            return false;
        }
    }
    if source_type.is_range()
        && target_type.is_multirange()
        && let Some(multirange_type) = multirange_type_ref_for_sql_type(target_type)
    {
        return source_type == multirange_type.range_type.sql_type;
    }
    if source_type.is_array || !is_text_like_type(source_type) {
        return true;
    }
    if target_type.is_array {
        return true;
    }
    if explicit_text_input_cast_exists(catalog, target_type) {
        return true;
    }
    false
}

fn is_user_defined_base_type_oid(type_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    type_oid != 0
        && builtin_type_name_for_oid(type_oid).is_none()
        && catalog.type_by_oid(type_oid).is_some_and(|row| {
            !row.sql_type.is_array
                && !row.sql_type.is_range()
                && !row.sql_type.is_multirange()
                && matches!(row.sql_type.kind, SqlTypeKind::Text)
                && row.typrelid == 0
        })
}

fn bind_regprocedure_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(signature) = regprocedure_literal_text(expr) else {
        return Ok(None);
    };
    let proc_oid = resolve_regprocedure_signature(signature, catalog)?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(proc_oid as i64))),
        target_type,
    )))
}

fn bind_regclass_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(relation_name) = regclass_literal_text(expr) else {
        return Ok(None);
    };
    let relation_oid = relation_name
        .parse::<u32>()
        .ok()
        .or_else(|| {
            catalog
                .lookup_any_relation(relation_name)
                .map(|entry| entry.relation_oid)
        })
        .ok_or_else(|| ParseError::UnknownTable(relation_name.to_string()))?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(relation_oid as i64))),
        target_type,
    )))
}

fn bind_regtype_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(type_name) = regtype_literal_text(expr) else {
        return Ok(None);
    };
    let raw_type = parse_type_name(type_name)?;
    let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
    let type_oid = catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| ParseError::UnsupportedType(type_name.to_string()))?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(type_oid as i64))),
        target_type,
    )))
}

fn bind_regoperator_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(signature) = regoperator_literal_text(expr) else {
        return Ok(None);
    };
    let operator_oid = resolve_regoperator_signature(signature, catalog)?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(operator_oid as i64))),
        target_type,
    )))
}

fn bind_regrole_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(role_name) = regrole_literal_text(expr) else {
        return Ok(None);
    };
    let Some(visible_catalog) = catalog.materialize_visible_catalog() else {
        return Ok(None);
    };
    let authid_rows = visible_catalog.authid_rows();
    let role =
        find_role_by_name(&authid_rows, role_name).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "existing role name",
            actual: role_name.to_string(),
        })?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(role.oid as i64))),
        target_type,
    )))
}

fn regrole_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn regclass_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn regoperator_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn regtype_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn regprocedure_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn bind_xml_expr(
    xml: &crate::include::nodes::parsenodes::RawXmlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let text_type = SqlType::new(SqlTypeKind::Text);
    let xml_type = SqlType::new(SqlTypeKind::Xml);
    let bind_child = |expr: &SqlExpr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    };
    let bind_as = |expr: &SqlExpr, target: SqlType| -> Result<Expr, ParseError> {
        let source =
            infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes);
        Ok(coerce_bound_expr(bind_child(expr)?, source, target))
    };

    let mut name = xml.name.clone();
    let mut named_args = Vec::new();
    let mut arg_names = xml.arg_names.clone();
    let mut args = Vec::new();
    let mut target_type = None;

    match xml.op {
        crate::include::nodes::parsenodes::RawXmlExprOp::Parse => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Serialize => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
            let resolved = resolve_raw_type_name(
                &xml.target_type.clone().ok_or(ParseError::UnexpectedEof)?,
                catalog,
            )?;
            if resolved.is_array
                || !matches!(
                    resolved.kind,
                    SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char
                )
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text, character, or character varying",
                    actual: sql_type_name(resolved),
                });
            }
            target_type = Some(resolved);
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Root => {
            if let Some(first) = xml.args.first() {
                args.push(bind_as(first, xml_type)?);
            }
            if let Some(version) = xml.args.get(1) {
                args.push(bind_as(version, text_type)?);
            }
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Pi => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::IsDocument => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Element => {
            let mut seen_names = BTreeSet::new();
            for (raw_expr, raw_name) in xml.named_args.iter().zip(xml.arg_names.iter()) {
                let inferred_name = if raw_name.is_empty() {
                    match raw_expr {
                        SqlExpr::Column(column)
                            if !column.contains('.') && !column.ends_with(".*") =>
                        {
                            column.clone()
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "attribute alias for non-column XMLATTRIBUTES expression",
                                actual: "XMLATTRIBUTES expression".into(),
                            });
                        }
                    }
                } else {
                    raw_name.clone()
                };
                if !seen_names.insert(inferred_name.clone()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "distinct XML attribute names",
                        actual: inferred_name,
                    });
                }
                named_args.push(bind_child(raw_expr)?);
                arg_names.push(inferred_name);
            }
            args = xml
                .args
                .iter()
                .map(bind_child)
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Forest => {
            arg_names.clear();
            for (raw_expr, raw_name) in xml.args.iter().zip(xml.arg_names.iter()) {
                let inferred_name = if raw_name.is_empty() {
                    match raw_expr {
                        SqlExpr::Column(column)
                            if !column.contains('.') && !column.ends_with(".*") =>
                        {
                            column.clone()
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "element alias for non-column XMLFOREST expression",
                                actual: "XMLFOREST expression".into(),
                            });
                        }
                    }
                } else {
                    raw_name.clone()
                };
                arg_names.push(inferred_name);
                args.push(bind_child(raw_expr)?);
            }
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Concat => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
    }

    Ok(Expr::Xml(Box::new(
        crate::include::nodes::primnodes::XmlExpr {
            op: match xml.op {
                crate::include::nodes::parsenodes::RawXmlExprOp::Concat => {
                    crate::include::nodes::primnodes::XmlExprOp::Concat
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Element => {
                    crate::include::nodes::primnodes::XmlExprOp::Element
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Forest => {
                    crate::include::nodes::primnodes::XmlExprOp::Forest
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Parse => {
                    crate::include::nodes::primnodes::XmlExprOp::Parse
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Pi => {
                    crate::include::nodes::primnodes::XmlExprOp::Pi
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Root => {
                    crate::include::nodes::primnodes::XmlExprOp::Root
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Serialize => {
                    crate::include::nodes::primnodes::XmlExprOp::Serialize
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::IsDocument => {
                    crate::include::nodes::primnodes::XmlExprOp::IsDocument
                }
            },
            name: name.take(),
            named_args,
            arg_names,
            args,
            xml_option: xml.xml_option,
            indent: xml.indent,
            target_type,
            standalone: xml.standalone,
        },
    )))
}

fn function_does_not_exist_error(
    name: &str,
    actual_types: &[SqlType],
    catalog: &dyn CatalogLookup,
) -> ParseError {
    let signature = actual_types
        .iter()
        .map(|ty| function_signature_type_name(*ty, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    ParseError::DetailedError {
        message: format!("function {name}({signature}) does not exist"),
        detail: None,
        hint: Some(
            "No function matches the given name and argument types. You might need to add explicit type casts."
                .into(),
        ),
        sqlstate: "42883",
    }
}

fn function_signature_type_name(ty: SqlType, catalog: &dyn CatalogLookup) -> String {
    let oid = range_type_ref_for_sql_type(ty)
        .map(|range_type| range_type.type_oid())
        .or_else(|| multirange_type_ref_for_sql_type(ty).map(|multirange| multirange.type_oid()));
    if let Some(row) = oid.and_then(|oid| catalog.type_by_oid(oid)) {
        return row.typname;
    }
    sql_type_name(ty)
}

fn resolve_regprocedure_signature(
    signature: &str,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let Some(open_paren) = signature.rfind('(') else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    let Some(arg_sql) = signature.get(open_paren + 1..signature.len().saturating_sub(1)) else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    if !signature.ends_with(')') {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    }
    let proc_name = signature[..open_paren].trim();
    if proc_name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: signature.to_string(),
        });
    }
    let arg_type_oids = if arg_sql.trim().is_empty() {
        Vec::new()
    } else {
        arg_sql
            .split(',')
            .map(|arg| {
                let raw_type = crate::backend::parser::parse_type_name(arg.trim())?;
                let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
                catalog
                    .type_oid_for_sql_type(sql_type)
                    .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(sql_type)))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let normalized_name = normalize_catalog_lookup_name(proc_name);
    let matches = catalog
        .proc_rows_by_name(normalized_name)
        .into_iter()
        .filter(|row| parse_proc_argtype_oids(&row.proargtypes) == Some(arg_type_oids.clone()))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.oid),
        [] => Err(ParseError::UnexpectedToken {
            expected: "existing function signature",
            actual: signature.to_string(),
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "unambiguous function signature",
            actual: signature.to_string(),
        }),
    }
}

fn resolve_regoperator_signature(
    signature: &str,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let Some(open_paren) = signature.rfind('(') else {
        return Err(ParseError::UnexpectedToken {
            expected: "operator signature",
            actual: signature.to_string(),
        });
    };
    let Some(arg_sql) = signature.get(open_paren + 1..signature.len().saturating_sub(1)) else {
        return Err(ParseError::UnexpectedToken {
            expected: "operator signature",
            actual: signature.to_string(),
        });
    };
    if !signature.ends_with(')') {
        return Err(ParseError::UnexpectedToken {
            expected: "operator signature",
            actual: signature.to_string(),
        });
    }
    let operator_name = signature[..open_paren].trim();
    if operator_name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "operator name",
            actual: signature.to_string(),
        });
    }
    let args = arg_sql.split(',').map(str::trim).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "operator signature",
            actual: signature.to_string(),
        });
    }
    let parse_arg = |arg: &str| -> Result<u32, ParseError> {
        if arg.eq_ignore_ascii_case("none") {
            return Ok(0);
        }
        let raw_type = crate::backend::parser::parse_type_name(arg)?;
        let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
        catalog
            .type_oid_for_sql_type(sql_type)
            .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(sql_type)))
    };
    let left_type_oid = parse_arg(args[0])?;
    let right_type_oid = parse_arg(args[1])?;
    catalog
        .operator_by_name_left_right(operator_name, left_type_oid, right_type_oid)
        .map(|row| row.oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "existing operator signature",
            actual: signature.to_string(),
        })
}

fn bind_array_membership_expr(
    op: OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let left_expr = if matches!(
        left,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    ) && !left_type.is_array
    {
        if let Expr::ArrayLiteral { array_type, .. } = &right_bound {
            left_type = *array_type;
        }
        coerce_bound_expr(left_bound, raw_left_type, left_type)
    } else {
        coerce_bound_expr(left_bound, raw_left_type, left_type)
    };
    let right_expr = if matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    ) && !right_type.is_array
    {
        if let Expr::ArrayLiteral { array_type, .. } = &left_expr {
            right_type = *array_type;
        }
        coerce_bound_expr(right_bound, raw_right_type, right_type)
    } else {
        coerce_bound_expr(right_bound, raw_right_type, right_type)
    };
    Ok(Expr::op_auto(op, vec![left_expr, right_expr]))
}

fn bind_maybe_array_membership_expr(
    op: OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    (left_type.is_array || right_type.is_array).then(|| {
        bind_array_membership_expr(
            op,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    })
}

fn domain_lookup_for_raw_type_name(
    raw: &RawTypeName,
    catalog: &dyn CatalogLookup,
) -> Option<DomainLookup> {
    match raw {
        RawTypeName::Named {
            name,
            array_bounds: 0,
        } => catalog.domain_by_name(name),
        _ => None,
    }
}

fn bind_domain_constraint_expr(
    expr: Expr,
    target_type: SqlType,
    domain: Option<&DomainLookup>,
) -> Expr {
    let Some(domain) = domain else {
        return expr;
    };
    let Some(check) = domain.check.as_deref() else {
        return expr;
    };
    let Some(limit) = parse_domain_upper_less_than_check(check) else {
        return expr;
    };
    Expr::func_with_impl(
        0,
        Some(target_type),
        false,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::PgRustDomainCheckUpperLessThan),
        vec![
            expr,
            Expr::Const(Value::Text(domain.name.clone().into())),
            Expr::Const(Value::Int32(limit)),
        ],
    )
}

fn parse_domain_upper_less_than_check(check: &str) -> Option<i32> {
    let normalized = check
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let limit = normalized.strip_prefix("upper(value)<")?;
    limit.parse::<i32>().ok()
}

fn function_call_signature_text(
    name: &str,
    args: &[SqlFunctionArg],
    actual_types: &[SqlType],
) -> String {
    let arg_types = args
        .iter()
        .zip(actual_types.iter().copied())
        .map(|(arg, actual_type)| match &arg.value {
            SqlExpr::Const(Value::Text(_))
            | SqlExpr::Const(Value::TextRef(_, _))
            | SqlExpr::Const(Value::Null) => "unknown".to_string(),
            _ => sql_type_name(actual_type),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({arg_types})")
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|oid| oid.parse::<u32>().ok())
        .collect()
}
