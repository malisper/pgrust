use super::*;
use crate::include::nodes::primnodes::{
    ExprArraySubscript, ScalarArrayOpExpr, SubLinkType, TABLE_OID_ATTR_NO, WindowFuncKind,
    attrno_index, set_returning_call_exprs,
};

pub(crate) fn validate_generated_columns(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    for (index, column) in desc.columns.iter().enumerate() {
        if column.generated.is_some() {
            bind_generated_expr(desc, index, catalog)?;
        }
    }
    Ok(())
}

pub(crate) fn bind_generated_expr(
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(column) = desc.columns.get(column_index) else {
        return Ok(None);
    };
    let Some(_kind) = column.generated else {
        return Ok(None);
    };
    let sql = column.default_expr.as_deref().ok_or_else(|| {
        ParseError::InvalidTableDefinition(format!(
            "generation expression missing for column \"{}\"",
            column.name
        ))
    })?;
    let parsed = crate::backend::parser::parse_expr(sql)?;
    let scope = scope_for_relation(None, desc);
    let bound = bind_expr_with_outer_and_ctes(&parsed, &scope, catalog, &[], None, &[])?;
    validate_generated_expr(&bound, desc, column_index, catalog)?;
    let from_type = infer_sql_expr_type(&parsed, &scope, catalog, &[], None);
    Ok(Some(coerce_bound_expr(bound, from_type, column.sql_type)))
}

pub(crate) fn generated_relation_output_exprs(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if column.generated == Some(ColumnGeneratedKind::Virtual) {
                bind_generated_expr(desc, index, catalog)?.ok_or_else(|| {
                    ParseError::InvalidTableDefinition(format!(
                        "generation expression missing for column \"{}\"",
                        column.name
                    ))
                })
            } else {
                Ok(Expr::Var(Var {
                    varno: 1,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                }))
            }
        })
        .collect()
}

pub(crate) fn scope_for_base_relation_with_generated(
    relation_name: &str,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<BoundScope, ParseError> {
    let mut scope = scope_for_base_relation(relation_name, desc);
    scope.output_exprs = generated_relation_output_exprs(desc, catalog)?;
    Ok(scope)
}

pub(crate) fn scope_for_relation_with_generated(
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<BoundScope, ParseError> {
    let mut scope = scope_for_relation(relation_name, desc);
    scope.output_exprs = generated_relation_output_exprs(desc, catalog)?;
    Ok(scope)
}

pub(crate) fn expr_references_column(expr: &Expr, column_index: usize) -> bool {
    expr_references_column_inner(expr, column_index)
}

fn validate_generated_expr(
    expr: &Expr,
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    validate_generated_expr_inner(expr, desc, column_index, catalog)
}

fn validate_generated_expr_inner(
    expr: &Expr,
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup != 0 {
                return Err(generation_error(
                    "cannot use subquery column reference in column generation expression",
                ));
            }
            if var.varattno == TABLE_OID_ATTR_NO {
                return Ok(());
            }
            if var.varattno <= 0 {
                return Err(generation_error(format!(
                    "cannot use system column in column generation expression"
                )));
            }
            if let Some(index) = attrno_index(var.varattno) {
                if index == column_index {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "cannot use generated column \"{}\" in column generation expression",
                            desc.columns[column_index].name
                        ),
                        detail: Some(
                            "This would cause the generated column to depend on its own value."
                                .into(),
                        ),
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                if desc
                    .columns
                    .get(index)
                    .is_some_and(|column| column.generated.is_some())
                {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "cannot use generated column \"{}\" in column generation expression",
                            desc.columns[index].name
                        ),
                        detail: Some(
                            "A generated column cannot reference another generated column.".into(),
                        ),
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
            }
            Ok(())
        }
        Expr::Aggref(_) => Err(generation_error(
            "aggregate functions are not allowed in column generation expressions",
        )),
        Expr::WindowFunc(_) => Err(generation_error(
            "window functions are not allowed in column generation expressions",
        )),
        Expr::SubLink(_) | Expr::SubPlan(_) => Err(generation_error(
            "cannot use subquery in column generation expression",
        )),
        Expr::SetReturning(_) => Err(generation_error(
            "set-returning functions are not allowed in column generation expressions",
        )),
        Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {
            Err(generation_error("generation expression is not immutable"))
        }
        Expr::Func(func) => {
            ensure_immutable_function(func.funcid, catalog)?;
            validate_exprs(&func.args, desc, column_index, catalog)
        }
        Expr::Op(op) => validate_exprs(&op.args, desc, column_index, catalog),
        Expr::Bool(bool_expr) => validate_exprs(&bool_expr.args, desc, column_index, catalog),
        Expr::Case(case_expr) => {
            if let Some(arg) = case_expr.arg.as_deref() {
                validate_generated_expr_inner(arg, desc, column_index, catalog)?;
            }
            for arm in &case_expr.args {
                validate_generated_expr_inner(&arm.expr, desc, column_index, catalog)?;
                validate_generated_expr_inner(&arm.result, desc, column_index, catalog)?;
            }
            validate_generated_expr_inner(&case_expr.defresult, desc, column_index, catalog)
        }
        Expr::ScalarArrayOp(saop) => validate_scalar_array_op(saop, desc, column_index, catalog),
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                validate_generated_expr_inner(child, desc, column_index, catalog)?;
            }
            Ok(())
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            validate_generated_expr_inner(inner, desc, column_index, catalog)
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
            validate_generated_expr_inner(expr, desc, column_index, catalog)?;
            validate_generated_expr_inner(pattern, desc, column_index, catalog)?;
            if let Some(escape) = escape.as_deref() {
                validate_generated_expr_inner(escape, desc, column_index, catalog)?;
            }
            Ok(())
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            validate_generated_expr_inner(left, desc, column_index, catalog)?;
            validate_generated_expr_inner(right, desc, column_index, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => {
            validate_exprs(elements, desc, column_index, catalog)
        }
        Expr::Row { fields, .. } => fields.iter().try_for_each(|(_, expr)| {
            validate_generated_expr_inner(expr, desc, column_index, catalog)
        }),
        Expr::FieldSelect { expr, .. } => {
            validate_generated_expr_inner(expr, desc, column_index, catalog)
        }
        Expr::ArraySubscript { array, subscripts } => {
            validate_generated_expr_inner(array, desc, column_index, catalog)?;
            for subscript in subscripts {
                validate_array_subscript(subscript, desc, column_index, catalog)?;
            }
            Ok(())
        }
        Expr::Const(_) | Expr::Param(_) | Expr::CaseTest(_) => Ok(()),
    }
}

fn validate_exprs(
    exprs: &[Expr],
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    exprs
        .iter()
        .try_for_each(|expr| validate_generated_expr_inner(expr, desc, column_index, catalog))
}

fn validate_scalar_array_op(
    saop: &ScalarArrayOpExpr,
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    validate_generated_expr_inner(&saop.left, desc, column_index, catalog)?;
    validate_generated_expr_inner(&saop.right, desc, column_index, catalog)
}

fn validate_array_subscript(
    subscript: &ExprArraySubscript,
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if let Some(lower) = subscript.lower.as_ref() {
        validate_generated_expr_inner(lower, desc, column_index, catalog)?;
    }
    if let Some(upper) = subscript.upper.as_ref() {
        validate_generated_expr_inner(upper, desc, column_index, catalog)?;
    }
    Ok(())
}

fn ensure_immutable_function(proc_oid: u32, catalog: &dyn CatalogLookup) -> Result<(), ParseError> {
    if proc_oid != 0
        && catalog
            .proc_row_by_oid(proc_oid)
            .is_some_and(|row| row.provolatile == 'i')
    {
        return Ok(());
    }
    Err(generation_error("generation expression is not immutable"))
}

fn expr_references_column_inner(expr: &Expr, column_index: usize) -> bool {
    match expr {
        Expr::Var(var) => attrno_index(var.varattno) == Some(column_index),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(|expr| expr_references_column_inner(expr, column_index))
                || case_expr.args.iter().any(|arm| {
                    expr_references_column_inner(&arm.expr, column_index)
                        || expr_references_column_inner(&arm.result, column_index)
                })
                || expr_references_column_inner(&case_expr.defresult, column_index)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            expr_references_column_inner(inner, column_index)
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
            expr_references_column_inner(expr, column_index)
                || expr_references_column_inner(pattern, column_index)
                || escape
                    .as_deref()
                    .is_some_and(|expr| expr_references_column_inner(expr, column_index))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_references_column_inner(left, column_index)
                || expr_references_column_inner(right, column_index)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_references_column_inner(&saop.left, column_index)
                || expr_references_column_inner(&saop.right, column_index)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_references_column_inner(expr, column_index)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_references_column_inner(array, column_index)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_references_column_inner(expr, column_index))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_references_column_inner(expr, column_index))
                })
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_references_column_inner(expr, column_index)),
        Expr::Aggref(aggref) => {
            aggref
                .args
                .iter()
                .any(|expr| expr_references_column_inner(expr, column_index))
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_references_column_inner(&entry.expr, column_index))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_references_column_inner(expr, column_index))
        }
        Expr::WindowFunc(window_func) => match &window_func.kind {
            WindowFuncKind::Aggregate(aggref) => aggref
                .args
                .iter()
                .any(|expr| expr_references_column_inner(expr, column_index)),
            WindowFuncKind::Builtin(_) => window_func
                .args
                .iter()
                .any(|expr| expr_references_column_inner(expr, column_index)),
        },
        Expr::SubLink(sublink) => {
            sublink
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_references_column_inner(expr, column_index))
                || matches!(sublink.sublink_type, SubLinkType::ExprSubLink)
        }
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_references_column_inner(expr, column_index))
                || subplan
                    .args
                    .iter()
                    .any(|expr| expr_references_column_inner(expr, column_index))
        }
        Expr::Const(_)
        | Expr::Param(_)
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

fn generation_error(message: impl Into<String>) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42P17",
    }
}
