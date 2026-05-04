use super::*;
use pgrust_catalog_data::PG_CATALOG_NAMESPACE_OID;
use pgrust_nodes::primnodes::{
    CMAX_ATTR_NO, CMIN_ATTR_NO, ExprArraySubscript, SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr,
    ScalarFunctionImpl, SubLinkType, TABLE_OID_ATTR_NO, WindowFuncKind, XMAX_ATTR_NO, XMIN_ATTR_NO,
    attrno_index, set_returning_call_exprs,
};

pub fn validate_generated_columns(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    validate_generated_columns_for_relation(desc, None, catalog)
}

pub fn validate_generated_columns_for_relation(
    desc: &RelationDesc,
    relation_name: Option<&str>,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    for (index, column) in desc.columns.iter().enumerate() {
        if column.generated.is_some() {
            bind_generated_expr_for_relation(desc, relation_name, index, catalog)?;
        }
    }
    Ok(())
}

pub fn bind_generated_expr(
    desc: &RelationDesc,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    bind_generated_expr_for_relation(desc, None, column_index, catalog)
}

fn bind_generated_expr_for_relation(
    desc: &RelationDesc,
    relation_name: Option<&str>,
    column_index: usize,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(column) = desc.columns.get(column_index) else {
        return Ok(None);
    };
    let Some(kind) = column.generated else {
        return Ok(None);
    };
    let sql = column.default_expr.as_deref().ok_or_else(|| {
        ParseError::InvalidTableDefinition(format!(
            "generation expression missing for column \"{}\"",
            column.name
        ))
    })?;
    if kind == ColumnGeneratedKind::Virtual
        && sql_type_uses_user_defined_type(column.sql_type, catalog)
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "virtual generated column \"{}\" cannot have a user-defined type",
                column.name
            ),
            detail: Some(
                "Virtual generated columns that make use of user-defined types are not yet supported."
                    .into(),
            ),
            hint: None,
            sqlstate: "0A000",
        });
    }
    let parsed = pgrust_parser::parse_expr(sql)?;
    let scope = scope_for_base_relation_with_optional_name(relation_name, desc);
    if kind == ColumnGeneratedKind::Virtual && raw_expr_uses_user_defined_function(&parsed, catalog)
    {
        return Err(virtual_generated_user_function_error());
    }
    let bound = match bind_expr_with_outer_and_ctes(&parsed, &scope, catalog, &[], None, &[]) {
        Err(ParseError::UnknownTable(name))
            if relation_name
                .is_some_and(|relation_name| name.eq_ignore_ascii_case(relation_name))
                && relation_name.is_some_and(|relation_name| {
                    generated_sql_mentions_self_regclass(sql, relation_name)
                }) =>
        {
            let relation_name = relation_name.expect("checked above");
            let rewritten_sql = rewrite_self_regclass_for_validation(sql, relation_name);
            let rewritten = pgrust_parser::parse_expr(&rewritten_sql)?;
            bind_expr_with_outer_and_ctes(&rewritten, &scope, catalog, &[], None, &[])?
        }
        other => other?,
    };
    validate_generated_expr(&bound, desc, column_index, catalog)?;
    if kind == ColumnGeneratedKind::Virtual && expr_uses_user_defined_function(&bound, catalog) {
        return Err(virtual_generated_user_function_error());
    }
    let from_type = infer_sql_expr_type(&parsed, &scope, catalog, &[], None);
    Ok(Some(coerce_bound_expr(bound, from_type, column.sql_type)))
}

pub fn generated_relation_output_exprs(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    generated_relation_output_exprs_for_relation(None, desc, catalog)
}

fn generated_relation_output_exprs_for_relation(
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if column.generated == Some(ColumnGeneratedKind::Virtual) {
                bind_generated_expr_for_relation(desc, relation_name, index, catalog)?.ok_or_else(
                    || {
                        ParseError::InvalidTableDefinition(format!(
                            "generation expression missing for column \"{}\"",
                            column.name
                        ))
                    },
                )
            } else {
                Ok(Expr::Var(Var {
                    varno: 1,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                    collation_oid: (column.collation_oid != 0).then_some(column.collation_oid),
                }))
            }
        })
        .collect()
}

pub fn scope_for_base_relation_with_generated(
    relation_name: &str,
    desc: &RelationDesc,
    relation_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
) -> Result<BoundScope, ParseError> {
    let mut scope = scope_for_base_relation(relation_name, desc, relation_oid);
    scope.output_exprs =
        generated_relation_output_exprs_for_relation(Some(relation_name), desc, catalog)?;
    Ok(scope)
}

pub fn scope_for_relation_with_generated(
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<BoundScope, ParseError> {
    let mut scope = scope_for_base_relation_with_optional_name(relation_name, desc);
    scope.output_exprs =
        generated_relation_output_exprs_for_relation(relation_name, desc, catalog)?;
    Ok(scope)
}

pub fn expr_references_column(expr: &Expr, column_index: usize) -> bool {
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

fn virtual_generated_user_function_error() -> ParseError {
    ParseError::DetailedError {
        message: "generation expression uses user-defined function".into(),
        detail: Some(
            "Virtual generated columns that make use of user-defined functions are not yet supported."
                .into(),
        ),
        hint: None,
        sqlstate: "0A000",
    }
}

fn sql_type_uses_user_defined_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> bool {
    let element_type = sql_type.element_type();
    element_type.type_oid != 0
        && catalog
            .type_by_oid(element_type.type_oid)
            .is_some_and(|row| row.typnamespace != PG_CATALOG_NAMESPACE_OID)
}

fn generated_sql_mentions_self_regclass(sql: &str, relation_name: &str) -> bool {
    sql.contains(&format!("'{relation_name}'::regclass"))
        || sql.contains(&format!("'{relation_name}'::pg_catalog.regclass"))
}

fn rewrite_self_regclass_for_validation(sql: &str, relation_name: &str) -> String {
    sql.replace(&format!("'{relation_name}'::regclass"), "0::regclass")
        .replace(
            &format!("'{relation_name}'::pg_catalog.regclass"),
            "0::pg_catalog.regclass",
        )
}

fn raw_expr_uses_user_defined_function(
    expr: &pgrust_nodes::parsenodes::SqlExpr,
    catalog: &dyn CatalogLookup,
) -> bool {
    use pgrust_nodes::parsenodes::{SqlExpr, function_arg_values};
    match expr {
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            let function_name = name.rsplit('.').next().unwrap_or(name);
            catalog
                .proc_rows_by_name(function_name)
                .into_iter()
                .any(|row| row.pronamespace != PG_CATALOG_NAMESPACE_OID)
                || function_arg_values(args)
                    .any(|arg| raw_expr_uses_user_defined_function(arg, catalog))
                || order_by
                    .iter()
                    .any(|item| raw_expr_uses_user_defined_function(&item.expr, catalog))
                || within_group.as_ref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| raw_expr_uses_user_defined_function(&item.expr, catalog))
                })
                || filter
                    .as_deref()
                    .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
        }
        SqlExpr::Add(left, right)
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
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => {
            raw_expr_uses_user_defined_function(left, catalog)
                || raw_expr_uses_user_defined_function(right, catalog)
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            raw_expr_uses_user_defined_function(left, catalog)
                || raw_expr_uses_user_defined_function(right, catalog)
        }
        SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::Not(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::Cast(expr, _)
        | SqlExpr::FieldSelect { expr, .. }
        | SqlExpr::Collate { expr, .. }
        | SqlExpr::Subscript { expr, .. } => raw_expr_uses_user_defined_function(expr, catalog),
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::GeometryUnaryOp { expr, .. } => {
            raw_expr_uses_user_defined_function(expr, catalog)
        }
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            raw_expr_uses_user_defined_function(left, catalog)
                || raw_expr_uses_user_defined_function(right, catalog)
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            raw_expr_uses_user_defined_function(expr, catalog)
                || raw_expr_uses_user_defined_function(zone, catalog)
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
            raw_expr_uses_user_defined_function(expr, catalog)
                || raw_expr_uses_user_defined_function(pattern, catalog)
                || escape
                    .as_deref()
                    .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref()
                .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
                || args.iter().any(|arm| {
                    raw_expr_uses_user_defined_function(&arm.expr, catalog)
                        || raw_expr_uses_user_defined_function(&arm.result, catalog)
                })
                || defresult
                    .as_deref()
                    .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
        }
        SqlExpr::ArrayLiteral(exprs) | SqlExpr::Row(exprs) => exprs
            .iter()
            .any(|expr| raw_expr_uses_user_defined_function(expr, catalog)),
        SqlExpr::QuantifiedArray { left, array, .. } => {
            raw_expr_uses_user_defined_function(left, catalog)
                || raw_expr_uses_user_defined_function(array, catalog)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            raw_expr_uses_user_defined_function(array, catalog)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(|expr| raw_expr_uses_user_defined_function(expr, catalog))
                })
        }
        SqlExpr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| raw_expr_uses_user_defined_function(expr, catalog)),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| raw_expr_uses_user_defined_function(expr, catalog)),
        _ => false,
    }
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
                let column_name = system_column_name(var.varattno).unwrap_or("unknown");
                return Err(generation_error(format!(
                    "cannot use system column \"{column_name}\" in column generation expression"
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
        Expr::GroupingKey(grouping_key) => {
            validate_generated_expr_inner(&grouping_key.expr, desc, column_index, catalog)
        }
        Expr::GroupingFunc(_) => Err(generation_error(
            "grouping operations are not allowed in column generation expressions",
        )),
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
        | Expr::User
        | Expr::SystemUser
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
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_generated_expr_inner(child, desc, column_index, catalog)?;
            }
            Ok(())
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
        Expr::Row { fields, .. } if is_whole_row_relation_expr(fields, desc) => {
            Err(ParseError::DetailedError {
                message: "cannot use whole-row variable in column generation expression".into(),
                detail: Some(
                    "This would cause the generated column to depend on its own value.".into(),
                ),
                hint: None,
                sqlstate: "42P17",
            })
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

fn is_whole_row_relation_expr(fields: &[(String, Expr)], desc: &RelationDesc) -> bool {
    fields.len() == desc.columns.len()
        && fields.iter().enumerate().all(|(index, (_, expr))| {
            matches!(
                expr,
                Expr::Var(var)
                    if var.varlevelsup == 0 && attrno_index(var.varattno) == Some(index)
            )
        })
}

fn system_column_name(varattno: i32) -> Option<&'static str> {
    match varattno {
        TABLE_OID_ATTR_NO => Some("tableoid"),
        SELF_ITEM_POINTER_ATTR_NO => Some("ctid"),
        XMIN_ATTR_NO => Some("xmin"),
        CMIN_ATTR_NO => Some("cmin"),
        XMAX_ATTR_NO => Some("xmax"),
        CMAX_ATTR_NO => Some("cmax"),
        _ => None,
    }
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

fn function_oid_is_user_defined(proc_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    proc_oid != 0
        && catalog
            .proc_row_by_oid(proc_oid)
            .is_some_and(|row| row.pronamespace != PG_CATALOG_NAMESPACE_OID)
}

fn expr_uses_user_defined_function(expr: &Expr, catalog: &dyn CatalogLookup) -> bool {
    match expr {
        Expr::GroupingKey(grouping_key) => {
            expr_uses_user_defined_function(&grouping_key.expr, catalog)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::Func(func) => {
            matches!(func.implementation, ScalarFunctionImpl::UserDefined { .. })
                || function_oid_is_user_defined(func.funcid, catalog)
                || func
                    .args
                    .iter()
                    .any(|expr| expr_uses_user_defined_function(expr, catalog))
        }
        Expr::Op(op) => {
            function_oid_is_user_defined(op.opfuncid, catalog)
                || op
                    .args
                    .iter()
                    .any(|expr| expr_uses_user_defined_function(expr, catalog))
        }
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
                || case_expr.args.iter().any(|arm| {
                    expr_uses_user_defined_function(&arm.expr, catalog)
                        || expr_uses_user_defined_function(&arm.result, catalog)
                })
                || expr_uses_user_defined_function(&case_expr.defresult, catalog)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_uses_user_defined_function(&saop.left, catalog)
                || expr_uses_user_defined_function(&saop.right, catalog)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_uses_user_defined_function(inner, catalog),
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
            expr_uses_user_defined_function(expr, catalog)
                || expr_uses_user_defined_function(pattern, catalog)
                || escape
                    .as_deref()
                    .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_uses_user_defined_function(left, catalog)
                || expr_uses_user_defined_function(right, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_uses_user_defined_function(expr, catalog)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_user_defined_function(array, catalog)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
                })
        }
        Expr::Aggref(aggref) => {
            aggref
                .args
                .iter()
                .any(|expr| expr_uses_user_defined_function(expr, catalog))
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_uses_user_defined_function(&entry.expr, catalog))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
        }
        Expr::WindowFunc(window_func) => match &window_func.kind {
            WindowFuncKind::Aggregate(aggref) => aggref
                .args
                .iter()
                .any(|expr| expr_uses_user_defined_function(expr, catalog)),
            WindowFuncKind::Builtin(_) => window_func
                .args
                .iter()
                .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        },
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_ref()
            .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_uses_user_defined_function(expr, catalog))
                || subplan
                    .args
                    .iter()
                    .any(|expr| expr_uses_user_defined_function(expr, catalog))
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(|expr| expr_uses_user_defined_function(expr, catalog)),
        Expr::Var(_) | Expr::Const(_) | Expr::Param(_) | Expr::CaseTest(_) | Expr::Random => false,
        Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
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

fn expr_references_column_inner(expr: &Expr, column_index: usize) -> bool {
    match expr {
        Expr::Var(var) => attrno_index(var.varattno) == Some(column_index),
        Expr::GroupingKey(grouping_key) => {
            expr_references_column_inner(&grouping_key.expr, column_index)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|expr| expr_references_column_inner(expr, column_index)),
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
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
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
        | Expr::User
        | Expr::SystemUser
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
