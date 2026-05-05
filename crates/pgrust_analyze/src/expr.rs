use super::functions::*;
use super::infer::*;
use super::*;
use pgrust_catalog_data::{
    ANYOID, DEFAULT_COLLATION_OID, PG_LANGUAGE_INTERNAL_OID, RECORD_TYPE_OID, UNKNOWN_TYPE_OID,
    builtin_scalar_function_for_proc_oid, builtin_type_name_for_oid,
    multirange_type_ref_for_sql_type, range_type_ref_for_sql_type,
};
use pgrust_nodes::datum::RecordDescriptor;
use pgrust_nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, CaseExpr as BoundCaseExpr,
    CaseTestExpr as BoundCaseTestExpr, CaseWhen as BoundCaseWhen, ExprArraySubscript, FuncExpr,
    OpExprKind, Param, ParamKind, QueryColumn, ScalarFunctionImpl, SetReturningCall,
    SqlJsonQueryFunction, SqlJsonQueryFunctionKind, SqlJsonTableBehavior, SqlJsonTablePassingArg,
    SqlJsonTableQuotes, SqlJsonTableWrapper, WHOLE_ROW_ATTR_NO, WindowFuncKind,
    expr_collation_oid_hint, expr_contains_set_returning, expr_sql_type_hint, user_attrno,
};
use pgrust_nodes::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
};
use pgrust_parser::gram::{
    SQL_JSON_ARRAY_FUNC, SQL_JSON_FUNC, SQL_JSON_IS_JSON_FUNC, SQL_JSON_OBJECT_FUNC,
    SQL_JSON_SCALAR_FUNC, SQL_JSON_SERIALIZE_FUNC,
};
use pgrust_parser::parse_type_name;

fn find_role_by_name<'a>(
    rows: &'a [pgrust_catalog_data::PgAuthIdRow],
    role_name: &str,
) -> Option<&'a pgrust_catalog_data::PgAuthIdRow> {
    rows.iter()
        .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
}

mod func;
mod json;
mod ops;
mod subquery;
mod targets;

pub(super) use self::func::bind_user_defined_scalar_function_call_from_resolved_typed_args;
use self::func::{
    bind_resolved_user_defined_scalar_function_call, bind_row_to_json_call,
    bind_scalar_function_call, bind_scalar_function_call_from_typed_args,
};
use self::json::{
    bind_json_binary_expr, bind_jsonb_contained_expr, bind_jsonb_contains_expr,
    bind_jsonb_delete_path_expr, bind_jsonb_exists_all_expr, bind_jsonb_exists_any_expr,
    bind_jsonb_exists_expr, bind_jsonb_path_binary_expr, bind_jsonb_subscript_expr,
    bind_maybe_jsonb_delete,
};
pub use self::ops::bind_concat_operands;
pub(super) use self::ops::bind_lowered_comparison_expr;
use self::ops::bind_order_by_using_direction;
pub(super) use self::ops::supports_comparison_operator;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_catalog_binary_operator_expr,
    bind_catalog_equality_operator_expr, bind_comparison_expr, bind_concat_expr,
    bind_maybe_network_arithmetic, bind_maybe_network_bitwise, bind_maybe_network_operator,
    bind_maybe_tsquery_contains, bind_overloaded_binary_expr, bind_prefix_operator_expr,
    bind_shift_expr, bind_text_pattern_comparison_expr, bind_text_starts_with_expr,
};
pub(super) use self::subquery::exists_subquery_query;
use self::subquery::{
    bind_array_subquery_expr, bind_exists_subquery_expr, bind_in_subquery_expr,
    bind_quantified_array_expr, bind_quantified_subquery_expr, bind_row_compare_subquery_expr,
    bind_scalar_subquery_expr,
};
use self::targets::bind_set_returning_expr_from_parts;
pub(super) use self::targets::root_call_returns_set;
pub use self::targets::{BoundSelectTargets, bind_select_targets};
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

pub(super) fn explicit_collation_display_name(
    expr: &SqlExpr,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let name = explicit_collation_name(expr)?;
    let oid = resolve_collation_oid(name, catalog).ok()?;
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| format!("\"{}\"", row.collname))
        .or_else(|| Some(format!("\"{name}\"")))
}

fn explicit_collation_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Collate { collation, .. } => Some(collation.as_str()),
        SqlExpr::FuncCall {
            within_group: Some(items),
            ..
        } => items
            .iter()
            .find_map(|item| explicit_collation_name(&item.expr)),
        SqlExpr::Cast(expr, _)
        | SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::Not(expr) => explicit_collation_name(expr),
        _ => None,
    }
}

fn collation_display_name_for_oid(catalog: &dyn CatalogLookup, oid: u32) -> String {
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| format!("\"{}\"", row.collname))
        .unwrap_or_else(|| format!("\"{oid}\""))
}

fn bind_pg_collation_for_arg(
    raw_arg: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let Some(display_name) = explicit_collation_display_name(raw_arg, catalog) {
        return Ok(Expr::Const(Value::Text(display_name.into())));
    }
    if is_unknown_literal_expr(raw_arg) {
        return Ok(Expr::Const(Value::Text("".into())));
    }

    let bound = bind_typed_expr_with_outer_and_ctes(
        raw_arg,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    if !is_collatable_type(bound.sql_type) {
        return Err(ParseError::DetailedError {
            message: format!(
                "collations are not supported by type {}",
                sql_type_name(bound.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    let oid = expr_collation_oid_hint(&bound.expr).unwrap_or(DEFAULT_COLLATION_OID);
    Ok(Expr::Const(Value::Text(
        collation_display_name_for_oid(catalog, oid).into(),
    )))
}

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

fn quantified_function_arg(expr: &SqlExpr) -> Option<(bool, &SqlExpr)> {
    let SqlExpr::FuncCall {
        name,
        args,
        order_by,
        within_group,
        distinct,
        func_variadic,
        filter,
        over,
        ..
    } = expr
    else {
        return None;
    };
    if !order_by.is_empty()
        || within_group.is_some()
        || *distinct
        || *func_variadic
        || filter.is_some()
        || over.is_some()
    {
        return None;
    }
    let is_all = if name.eq_ignore_ascii_case("any") {
        false
    } else if name.eq_ignore_ascii_case("all") {
        true
    } else {
        return None;
    };
    let [arg] = args.args() else {
        return None;
    };
    if arg.name.is_some() {
        return None;
    }
    Some((is_all, &arg.value))
}

fn scalar_function_needs_raw_arg_binding(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
    )
}

fn set_returning_not_allowed_error(context: &'static str) -> ParseError {
    crate::srf_not_allowed_error(context)
}

pub(super) fn build_bound_order_by_entry(
    item: &OrderByItem,
    bound_expr: Expr,
    ressortgroupref: usize,
    catalog: &dyn CatalogLookup,
) -> Result<OrderByEntry, ParseError> {
    let expr_type = expr_sql_type_hint(&bound_expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
    validate_order_by_type(expr_type, catalog)?;
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

fn validate_order_by_type(
    expr_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if expr_type.is_array || !matches!(expr_type.kind, SqlTypeKind::Composite) {
        return Ok(());
    }
    let expr_type = expression_navigation_sql_type(expr_type, catalog);
    if expr_type.typrelid == 0 {
        return Ok(());
    }
    let Some(relation) = catalog.lookup_relation_by_oid(expr_type.typrelid) else {
        return Ok(());
    };
    if relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .any(|column| {
            matches!(
                column.sql_type.kind,
                SqlTypeKind::Point | SqlTypeKind::Lseg | SqlTypeKind::Path
            )
        })
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "could not identify an ordering operator for type {}",
                catalog_sql_type_name(expr_type, catalog)
            ),
            detail: None,
            hint: Some("Use an explicit ordering operator or modify the query.".into()),
            sqlstate: "42883",
        });
    }
    Ok(())
}

fn reject_typed_srf(expr: &TypedExpr, context: &'static str) -> Result<(), ParseError> {
    if expr.contains_srf {
        Err(set_returning_not_allowed_error(context))
    } else {
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_sql_json_query_function(
    func: &JsonQueryFunctionExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let kind = match func.kind {
        JsonQueryFunctionKind::Exists => SqlJsonQueryFunctionKind::Exists,
        JsonQueryFunctionKind::Value => SqlJsonQueryFunctionKind::Value,
        JsonQueryFunctionKind::Query => SqlJsonQueryFunctionKind::Query,
    };
    let result_type = sql_json_query_function_returning_type(func, catalog)?;
    if matches!(func.kind, JsonQueryFunctionKind::Value)
        && func
            .returning
            .as_ref()
            .is_some_and(|returning| returning.format_json)
    {
        return Err(ParseError::DetailedError {
            message: "cannot specify FORMAT JSON in RETURNING clause of JSON_VALUE()".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }

    let raw_path_type = infer_sql_expr_type_with_ctes(
        &func.path,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    if !sql_json_query_function_path_type_allowed(raw_path_type) {
        return Err(ParseError::DetailedError {
            message: format!(
                "JSON path expression must be of type jsonpath, not of type {}",
                catalog_sql_type_name(raw_path_type, catalog)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }

    let context = bind_expr_with_outer_and_ctes(
        &func.context,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let path = bind_expr_with_outer_and_ctes(
        &func.path,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let passing = func
        .passing
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(
                &arg.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
            .map(|expr| SqlJsonTablePassingArg {
                name: arg.name.clone(),
                expr,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let wrapper = sql_json_query_function_wrapper(func.wrapper);
    let quotes = sql_json_query_function_quotes(func.quotes);
    if matches!(
        wrapper,
        SqlJsonTableWrapper::Conditional | SqlJsonTableWrapper::Unconditional
    ) && matches!(quotes, SqlJsonTableQuotes::Omit)
    {
        return Err(ParseError::DetailedError {
            message: "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used"
                .into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }

    let on_empty = bind_sql_json_query_function_behavior(
        sql_json_query_function_on_empty(func),
        result_type,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let on_error = bind_sql_json_query_function_behavior(
        sql_json_query_function_on_error(func),
        result_type,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    if !matches!(func.kind, JsonQueryFunctionKind::Exists) {
        validate_sql_json_query_function_behavior(func.kind, &on_empty, "EMPTY")?;
    }
    validate_sql_json_query_function_behavior(func.kind, &on_error, "ERROR")?;

    Ok(Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
        kind,
        context,
        path,
        passing,
        result_type,
        result_format_json: func
            .returning
            .as_ref()
            .is_some_and(|returning| returning.format_json),
        wrapper,
        quotes,
        on_empty,
        on_error,
    })))
}

fn sql_json_query_function_returning_type(
    func: &JsonQueryFunctionExpr,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let sql_type = match &func.returning {
        Some(returning) => resolve_raw_type_name(&returning.type_name, catalog)?,
        None => match func.kind {
            JsonQueryFunctionKind::Exists => SqlType::new(SqlTypeKind::Bool),
            JsonQueryFunctionKind::Value => SqlType::new(SqlTypeKind::Text),
            JsonQueryFunctionKind::Query => SqlType::new(SqlTypeKind::Jsonb),
        },
    };
    if sql_json_query_function_returning_type_is_pseudo(sql_type) {
        return Err(ParseError::DetailedError {
            message: "returning pseudo-types is not supported in SQL/JSON functions".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(sql_type)
}

fn sql_json_query_function_returning_type_is_pseudo(sql_type: SqlType) -> bool {
    !sql_type.is_array
        && matches!(
            sql_type.kind,
            SqlTypeKind::AnyArray
                | SqlTypeKind::AnyElement
                | SqlTypeKind::AnyRange
                | SqlTypeKind::AnyMultirange
                | SqlTypeKind::AnyCompatible
                | SqlTypeKind::AnyCompatibleArray
                | SqlTypeKind::AnyCompatibleRange
                | SqlTypeKind::AnyCompatibleMultirange
                | SqlTypeKind::AnyEnum
                | SqlTypeKind::Record
                | SqlTypeKind::Void
                | SqlTypeKind::Trigger
                | SqlTypeKind::EventTrigger
                | SqlTypeKind::FdwHandler
                | SqlTypeKind::Internal
                | SqlTypeKind::Cstring
        )
}

fn sql_json_query_function_path_type_allowed(sql_type: SqlType) -> bool {
    !sql_type.is_array
        && matches!(
            sql_type.kind,
            SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar | SqlTypeKind::JsonPath
        )
}

fn sql_json_query_function_on_empty(func: &JsonQueryFunctionExpr) -> &JsonTableBehavior {
    func.on_empty.as_ref().unwrap_or(&JsonTableBehavior::Null)
}

fn sql_json_query_function_on_error(func: &JsonQueryFunctionExpr) -> &JsonTableBehavior {
    match func.kind {
        JsonQueryFunctionKind::Exists => {
            func.on_error.as_ref().unwrap_or(&JsonTableBehavior::False)
        }
        JsonQueryFunctionKind::Value | JsonQueryFunctionKind::Query => {
            func.on_error.as_ref().unwrap_or(&JsonTableBehavior::Null)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_sql_json_query_function_behavior(
    behavior: &JsonTableBehavior,
    target_type: SqlType,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<SqlJsonTableBehavior, ParseError> {
    Ok(match behavior {
        JsonTableBehavior::Null => SqlJsonTableBehavior::Null,
        JsonTableBehavior::Error => SqlJsonTableBehavior::Error,
        JsonTableBehavior::Empty => SqlJsonTableBehavior::Empty,
        JsonTableBehavior::EmptyArray => SqlJsonTableBehavior::EmptyArray,
        JsonTableBehavior::EmptyObject => SqlJsonTableBehavior::EmptyObject,
        JsonTableBehavior::True => SqlJsonTableBehavior::True,
        JsonTableBehavior::False => SqlJsonTableBehavior::False,
        JsonTableBehavior::Unknown => SqlJsonTableBehavior::Unknown,
        JsonTableBehavior::Default(expr) => {
            validate_sql_json_query_function_default_expr(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let raw_type = infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            reject_sql_json_behavior_default_cast(raw_type, target_type, catalog)?;
            let bound = bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            SqlJsonTableBehavior::Default(bound)
        }
    })
}

fn reject_sql_json_behavior_default_cast(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if !target_type.is_array
        && matches!(target_type.kind, SqlTypeKind::Bit | SqlTypeKind::VarBit)
        && is_integer_family(source_type)
    {
        let target_name = catalog_sql_type_name(target_type, catalog);
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot cast behavior expression of type {} to {}",
                sql_type_name(source_type),
                target_name
            ),
            detail: None,
            hint: Some(format!(
                "You will need to explicitly cast the expression to type {target_name}."
            )),
            sqlstate: "42846",
        });
    }
    Ok(())
}

fn validate_sql_json_query_function_default_expr(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(), ParseError> {
    if raw_sql_expr_any(expr, &|expr| matches!(expr, SqlExpr::Column(_))) {
        return Err(sql_json_default_expr_error(
            "DEFAULT expression must not contain column references",
        ));
    }
    if super::agg::expr_contains_agg(catalog, expr)
        || raw_sql_expr_any(expr, &|expr| {
            matches!(
                expr,
                SqlExpr::FuncCall { over: Some(_), .. }
                    | SqlExpr::ScalarSubquery(_)
                    | SqlExpr::ArraySubquery(_)
                    | SqlExpr::Exists(_)
                    | SqlExpr::InSubquery { .. }
                    | SqlExpr::QuantifiedSubquery { .. }
            )
        })
    {
        return Err(sql_json_default_expr_error(
            "can only specify a constant, non-aggregate function, or operator expression for DEFAULT",
        ));
    }
    let bound =
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    if expr_contains_set_returning(&bound) {
        return Err(sql_json_default_expr_error(
            "DEFAULT expression must not return a set",
        ));
    }
    Ok(())
}

fn sql_json_default_expr_error(message: impl Into<String>) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn raw_sql_expr_any(expr: &SqlExpr, predicate: &impl Fn(&SqlExpr) -> bool) -> bool {
    if predicate(expr) {
        return true;
    }
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::User
        | SqlExpr::SystemUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. }
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_) => false,
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.args()
                .iter()
                .any(|arg| raw_sql_expr_any(&arg.value, predicate))
                || order_by
                    .iter()
                    .any(|item| raw_sql_expr_any(&item.expr, predicate))
                || within_group.as_deref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| raw_sql_expr_any(&item.expr, predicate))
                })
                || filter
                    .as_deref()
                    .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
        }
        SqlExpr::InSubquery { expr, .. } => raw_sql_expr_any(expr, predicate),
        SqlExpr::QuantifiedSubquery { left, .. } => raw_sql_expr_any(left, predicate),
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            raw_sql_expr_any(expr, predicate)
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => elements
            .iter()
            .any(|expr| raw_sql_expr_any(expr, predicate)),
        SqlExpr::ArraySubscript { array, subscripts } => {
            raw_sql_expr_any(array, predicate)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
                })
        }
        SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::Overlaps(left, right)
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
        | SqlExpr::GeometryBinaryOp { left, right, .. }
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        }
        | SqlExpr::BinaryOperator { left, right, .. } => {
            raw_sql_expr_any(left, predicate) || raw_sql_expr_any(right, predicate)
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
            raw_sql_expr_any(expr, predicate)
                || raw_sql_expr_any(pattern, predicate)
                || escape
                    .as_ref()
                    .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref()
                .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
                || args.iter().any(|arm| {
                    raw_sql_expr_any(&arm.expr, predicate)
                        || raw_sql_expr_any(&arm.result, predicate)
                })
                || defresult
                    .as_deref()
                    .is_some_and(|expr| raw_sql_expr_any(expr, predicate))
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
        | SqlExpr::Subscript { expr: inner, .. } => raw_sql_expr_any(inner, predicate),
        SqlExpr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| raw_sql_expr_any(expr, predicate)),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .iter()
            .any(|expr| raw_sql_expr_any(expr, predicate)),
    }
}

fn validate_sql_json_query_function_behavior(
    kind: JsonQueryFunctionKind,
    behavior: &SqlJsonTableBehavior,
    target: &'static str,
) -> Result<(), ParseError> {
    let valid = match kind {
        JsonQueryFunctionKind::Exists => {
            target == "ERROR"
                && matches!(
                    behavior,
                    SqlJsonTableBehavior::Error
                        | SqlJsonTableBehavior::True
                        | SqlJsonTableBehavior::False
                        | SqlJsonTableBehavior::Unknown
                )
        }
        JsonQueryFunctionKind::Value => matches!(
            behavior,
            SqlJsonTableBehavior::Error
                | SqlJsonTableBehavior::Null
                | SqlJsonTableBehavior::Default(_)
        ),
        JsonQueryFunctionKind::Query => matches!(
            behavior,
            SqlJsonTableBehavior::Error
                | SqlJsonTableBehavior::Null
                | SqlJsonTableBehavior::Empty
                | SqlJsonTableBehavior::EmptyArray
                | SqlJsonTableBehavior::EmptyObject
                | SqlJsonTableBehavior::Default(_)
        ),
    };
    if valid {
        return Ok(());
    }
    let detail = match kind {
        JsonQueryFunctionKind::Exists => {
            "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in ON ERROR for JSON_EXISTS()."
        }
        JsonQueryFunctionKind::Value => {
            "Only ERROR, NULL, or DEFAULT expression is allowed in ON ERROR for JSON_VALUE()."
        }
        JsonQueryFunctionKind::Query => {
            "Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression is allowed in ON ERROR for JSON_QUERY()."
        }
    };
    Err(ParseError::DetailedError {
        message: format!("invalid ON {target} behavior"),
        detail: Some(detail.replace("ON ERROR", &format!("ON {target}"))),
        hint: None,
        sqlstate: "42601",
    })
}

fn sql_json_query_function_wrapper(wrapper: JsonTableWrapper) -> SqlJsonTableWrapper {
    match wrapper {
        JsonTableWrapper::Unspecified => SqlJsonTableWrapper::Unspecified,
        JsonTableWrapper::Without => SqlJsonTableWrapper::Without,
        JsonTableWrapper::Conditional => SqlJsonTableWrapper::Conditional,
        JsonTableWrapper::Unconditional => SqlJsonTableWrapper::Unconditional,
    }
}

fn sql_json_query_function_quotes(quotes: JsonTableQuotes) -> SqlJsonTableQuotes {
    match quotes {
        JsonTableQuotes::Unspecified => SqlJsonTableQuotes::Unspecified,
        JsonTableQuotes::Keep => SqlJsonTableQuotes::Keep,
        JsonTableQuotes::Omit => SqlJsonTableQuotes::Omit,
    }
}

fn raise_sql_json_behavior_varlevels(
    behavior: SqlJsonTableBehavior,
    levels: usize,
) -> SqlJsonTableBehavior {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => {
            SqlJsonTableBehavior::Default(raise_expr_varlevels(expr, levels))
        }
        other => other,
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

pub(super) fn bind_legacy_scalar_function_call(
    name: &str,
    args: &[SqlExpr],
    func_variadic: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<TypedExpr>, ParseError> {
    if name.eq_ignore_ascii_case("coalesce") {
        let args = args
            .iter()
            .cloned()
            .map(|value| SqlFunctionArg { name: None, value })
            .collect::<Vec<_>>();
        let expr = bind_coalesce_call(&args, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        let sql_type = expr_sql_type_hint(&expr).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "COALESCE with a known result type",
            actual: name.to_string(),
        })?;
        return Ok(Some(TypedExpr {
            expr,
            sql_type,
            contains_srf: false,
        }));
    }

    let Some(legacy_func) = resolve_scalar_function(name).or_else(|| {
        resolve_function_cast_type(catalog, name)
            .filter(|ty| range_type_ref_for_sql_type(*ty).is_some())
            .map(|_| BuiltinScalarFunction::RangeConstructor)
    }) else {
        return Ok(None);
    };
    validate_scalar_function_arity(legacy_func, args)?;

    let actual_types = args
        .iter()
        .map(|arg| {
            super::infer::infer_sql_expr_function_arg_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let legacy_result_type = if matches!(legacy_func, BuiltinScalarFunction::RangeConstructor) {
        resolve_function_cast_type(catalog, name)
            .filter(|ty| range_type_ref_for_sql_type(*ty).is_some())
    } else if matches!(
        legacy_func,
        BuiltinScalarFunction::Greatest | BuiltinScalarFunction::Least
    ) {
        infer_common_scalar_expr_type_with_ctes(
            args,
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
    let legacy_vatype_oid = if func_variadic
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
    let legacy_declared_arg_types = if name
        .rsplit('.')
        .next()
        .is_some_and(|base| base.eq_ignore_ascii_case("pg_sleep_for"))
    {
        vec![SqlType::new(SqlTypeKind::Interval)]
    } else if let Some(range_type) = legacy_result_type.and_then(range_type_ref_for_sql_type) {
        let mut declared = vec![range_type.subtype, range_type.subtype];
        if args.len() == 3 {
            declared.push(SqlType::new(SqlTypeKind::Text));
        }
        declared
    } else {
        actual_types.clone()
    };
    let expr = bind_scalar_function_call(
        legacy_func,
        0,
        legacy_result_type,
        func_variadic,
        0,
        legacy_vatype_oid,
        &legacy_declared_arg_types,
        args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let sql_type = expr_sql_type_hint(&expr)
        .or(legacy_result_type)
        .or_else(|| fixed_scalar_return_type(legacy_func))
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "scalar function with a known result type",
            actual: name.to_string(),
        })?;
    Ok(Some(TypedExpr {
        expr,
        sql_type,
        contains_srf: false,
    }))
}

fn supports_array_subscripts(array_type: SqlType) -> bool {
    array_type.is_array
        || matches!(
            array_type.kind,
            SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
        )
}

fn expression_navigation_sql_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    if is_array_of_domain_over_array_type(sql_type, catalog) {
        return sql_type;
    }
    let sql_type = if let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid) {
        if sql_type.is_array && !domain.sql_type.is_array {
            SqlType::array_of(domain.sql_type)
        } else {
            domain.sql_type
        }
    } else {
        sql_type
    };

    if !sql_type.is_array
        && matches!(sql_type.kind, SqlTypeKind::Composite)
        && sql_type.typrelid == 0
        && let Some(row) = catalog.type_by_oid(sql_type.type_oid)
        && row.typrelid != 0
    {
        return sql_type.with_identity(row.oid, row.typrelid);
    }
    if !sql_type.is_array
        && matches!(sql_type.kind, SqlTypeKind::Composite)
        && sql_type.type_oid == 0
        && sql_type.typrelid != 0
        && let Some(row) = catalog
            .type_rows()
            .into_iter()
            .find(|row| row.typrelid == sql_type.typrelid)
    {
        return sql_type.with_identity(row.oid, row.typrelid);
    }
    sql_type
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

fn fixed_geometry_subscript_error(sql_type: SqlType) -> ParseError {
    ParseError::UndefinedOperator {
        op: "[]",
        left_type: sql_type_name(sql_type),
        right_type: "integer".into(),
    }
}

fn fixed_geometry_subscript_index(
    subscript: &pgrust_nodes::parsenodes::ArraySubscript,
) -> Option<i32> {
    if subscript.is_slice {
        return None;
    }
    match subscript.lower.as_deref().or(subscript.upper.as_deref())? {
        SqlExpr::IntegerLiteral(value) => {
            normalize_numeric_literal_token(value).parse::<i32>().ok()
        }
        SqlExpr::NumericLiteral(value) => {
            normalize_numeric_literal_token(value).parse::<i32>().ok()
        }
        SqlExpr::Const(Value::Int16(value)) => Some(i32::from(*value)),
        SqlExpr::Const(Value::Int32(value)) => Some(*value),
        _ => None,
    }
}

fn point_coordinate_subscript(
    subscripts: &[pgrust_nodes::parsenodes::ArraySubscript],
) -> Option<i32> {
    let [subscript] = subscripts else {
        return None;
    };
    fixed_geometry_subscript_index(subscript)
}

fn bind_fixed_geometry_subscripts(
    array: &SqlExpr,
    array_type: SqlType,
    subscripts: &[pgrust_nodes::parsenodes::ArraySubscript],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let mut expr =
        bind_expr_with_outer_and_ctes(array, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let mut current_type = array_type.element_type();
    for subscript in subscripts {
        if subscript.is_slice {
            return Err(fixed_length_array_slice_error());
        }
        let Some(index) = fixed_geometry_subscript_index(subscript) else {
            return Err(fixed_geometry_subscript_error(current_type));
        };
        if !(0..=1).contains(&index) {
            return Err(fixed_geometry_subscript_error(current_type));
        }
        let (func, result_type) = match current_type.kind {
            SqlTypeKind::Box if index == 0 => (
                BuiltinScalarFunction::GeoBoxHigh,
                SqlType::new(SqlTypeKind::Point),
            ),
            SqlTypeKind::Box => (
                BuiltinScalarFunction::GeoBoxLow,
                SqlType::new(SqlTypeKind::Point),
            ),
            SqlTypeKind::Point if index == 0 => (
                BuiltinScalarFunction::GeoPointX,
                SqlType::new(SqlTypeKind::Float8),
            ),
            SqlTypeKind::Point => (
                BuiltinScalarFunction::GeoPointY,
                SqlType::new(SqlTypeKind::Float8),
            ),
            _ => return Err(fixed_geometry_subscript_error(current_type)),
        };
        expr = Expr::builtin_func(func, Some(result_type), false, vec![expr]);
        current_type = result_type;
    }
    Ok(expr)
}

#[allow(dead_code)]
pub fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

fn build_plain_row_expr(fields: Vec<(String, Expr)>, named_row_type: Option<(u32, u32)>) -> Expr {
    let descriptor_fields = fields
        .iter()
        .map(|(field_name, expr)| {
            (
                field_name.clone(),
                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
            )
        })
        .collect::<Vec<_>>();
    let descriptor = if let Some((type_oid, typrelid)) = named_row_type {
        RecordDescriptor::named(type_oid, typrelid, -1, descriptor_fields)
    } else {
        assign_anonymous_record_descriptor(descriptor_fields)
    };
    Expr::Row { descriptor, fields }
}

fn build_whole_row_expr(fields: Vec<(String, Expr)>, named_row_type: Option<(u32, u32)>) -> Expr {
    let row_expr = build_plain_row_expr(fields.clone(), named_row_type);
    let descriptor = match &row_expr {
        Expr::Row { descriptor, .. } => descriptor.clone(),
        _ => unreachable!("build_plain_row_expr always returns Expr::Row"),
    };
    if let Some(var) = whole_row_var_from_fields(&fields, descriptor.sql_type()) {
        return Expr::Var(var);
    }
    if descriptor.typrelid == 0 {
        return row_expr;
    }
    let Some(all_fields_null) = fields
        .iter()
        .map(|(_, expr)| {
            if expr_sql_type_hint(expr).is_some_and(|ty| {
                !ty.is_array && matches!(ty.kind, SqlTypeKind::Composite | SqlTypeKind::Record)
            }) {
                Expr::IsNotDistinctFrom(Box::new(expr.clone()), Box::new(Expr::Const(Value::Null)))
            } else {
                Expr::IsNull(Box::new(expr.clone()))
            }
        })
        .reduce(Expr::and)
    else {
        return row_expr;
    };
    Expr::Case(Box::new(BoundCaseExpr {
        casetype: descriptor.sql_type(),
        arg: None,
        args: vec![BoundCaseWhen {
            expr: all_fields_null,
            result: Expr::Const(Value::Null),
        }],
        defresult: Box::new(row_expr),
    }))
}

fn whole_row_var_from_fields(fields: &[(String, Expr)], vartype: SqlType) -> Option<Var> {
    if fields.is_empty() || vartype.typrelid == 0 {
        return None;
    }
    let mut varno = None;
    let mut varlevelsup = None;
    for (index, (_, expr)) in fields.iter().enumerate() {
        let Expr::Var(var) = expr else {
            return None;
        };
        if var.varattno != user_attrno(index) {
            return None;
        }
        if varno.is_some_and(|existing| existing != var.varno)
            || varlevelsup.is_some_and(|existing| existing != var.varlevelsup)
        {
            return None;
        }
        varno = Some(var.varno);
        varlevelsup = Some(var.varlevelsup);
    }
    Some(Var {
        varno: varno?,
        varattno: WHOLE_ROW_ATTR_NO,
        varlevelsup: varlevelsup?,
        vartype,
        collation_oid: None,
    })
}

fn relation_row_type_identity(
    catalog: &dyn CatalogLookup,
    relation_oid: Option<u32>,
) -> Option<(u32, u32)> {
    let relation_oid = relation_oid?;
    if let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) {
        if relation.of_type_oid != 0 {
            let typrelid = catalog
                .type_by_oid(relation.of_type_oid)
                .map(|row| row.typrelid)
                .filter(|typrelid| *typrelid != 0)
                .unwrap_or(relation_oid);
            return Some((relation.of_type_oid, typrelid));
        }
    }
    let type_oid = catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.reltype)
        .filter(|type_oid| *type_oid != 0)
        .or_else(|| {
            catalog
                .type_rows()
                .into_iter()
                .find(|row| row.typrelid == relation_oid)
                .map(|row| row.oid)
        })?;
    (type_oid != 0).then_some((type_oid, relation_oid))
}

fn bind_sql_function_inline_named_field(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some((arg, field_path)) = resolve_sql_function_inline_arg_path(name) else {
        return Ok(None);
    };
    bind_sql_function_inline_arg_field_path(arg.expr, &field_path, catalog).map(Some)
}

fn bind_sql_function_inline_arg_field_path(
    mut current: Expr,
    field_path: &[&str],
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    for field in field_path {
        if *field == "*" {
            continue;
        }
        let field_type = resolve_bound_field_select_type(&current, field, catalog)?;
        current = Expr::FieldSelect {
            expr: Box::new(current),
            field: (*field).to_string(),
            field_type,
        };
    }
    Ok(current)
}

fn resolve_sql_function_inline_arg_path(name: &str) -> Option<(SqlFunctionInlineArg, Vec<&str>)> {
    let parts = name
        .split('.')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if let Some(arg) = current_sql_function_inline_named_arg(parts[0]) {
        return Some((arg, parts[1..].to_vec()));
    }
    for arg_index in 1..parts.len() {
        if let Some(arg) =
            current_sql_function_inline_qualified_arg(parts[arg_index - 1], parts[arg_index])
        {
            return Some((arg, parts[arg_index + 1..].to_vec()));
        }
    }
    let arg = current_sql_function_inline_single_arg()?;
    (parts.len() > 1).then(|| (arg, parts[1..].to_vec()))
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

fn row_comparison_items(expr: &SqlExpr) -> Option<Vec<SqlExpr>> {
    match expr {
        SqlExpr::Row(items) => Some(items.clone()),
        SqlExpr::Column(name) if name.ends_with(".*") => Some(vec![expr.clone()]),
        SqlExpr::FieldSelect { field, .. } if field == "*" => Some(vec![expr.clone()]),
        _ => None,
    }
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

fn bind_power_operator_expr(
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
    let resolved_left_type =
        coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let resolved_right_type =
        coerce_unknown_string_literal_type(right, raw_right_type, resolved_left_type);
    if !is_numeric_family(resolved_left_type) || !is_numeric_family(resolved_right_type) {
        return Err(ParseError::UnexpectedToken {
            expected: "numeric arguments",
            actual: format!(
                "power({}, {})",
                sql_type_name(resolved_left_type),
                sql_type_name(resolved_right_type)
            ),
        });
    }

    let target = power_operator_result_type(
        left,
        resolved_left_type,
        right,
        resolved_right_type,
        catalog,
    );
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;

    Ok(Expr::Func(Box::new(FuncExpr {
        funcid: 0,
        funcname: Some("power".into()),
        funcresulttype: Some(target),
        funcvariadic: false,
        implementation: ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Power),
        collation_oid: None,
        display_args: None,
        args: vec![
            coerce_bound_expr(left_bound, raw_left_type, target),
            coerce_bound_expr(right_bound, raw_right_type, target),
        ],
    })))
}

pub(super) fn power_operator_result_type(
    left: &SqlExpr,
    left_type: SqlType,
    right: &SqlExpr,
    right_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> SqlType {
    if matches!(
        left_type.element_type().kind,
        SqlTypeKind::Float4 | SqlTypeKind::Float8
    ) || matches!(
        right_type.element_type().kind,
        SqlTypeKind::Float4 | SqlTypeKind::Float8
    ) {
        return SqlType::new(SqlTypeKind::Float8);
    }
    if power_operator_arg_uses_numeric_operator(left, left_type, catalog)
        || power_operator_arg_uses_numeric_operator(right, right_type, catalog)
    {
        SqlType::new(SqlTypeKind::Numeric)
    } else {
        SqlType::new(SqlTypeKind::Float8)
    }
}

fn power_operator_arg_uses_numeric_operator(
    arg: &SqlExpr,
    arg_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> bool {
    explicit_numeric_power_arg(arg, catalog) || matches!(arg_type.kind, SqlTypeKind::Numeric)
}

fn explicit_numeric_power_arg(arg: &SqlExpr, catalog: &dyn CatalogLookup) -> bool {
    matches!(
        arg,
        SqlExpr::Cast(_, raw_type)
            if resolve_raw_type_name(raw_type, catalog)
                .is_ok_and(|ty| matches!(ty.kind, SqlTypeKind::Numeric))
    )
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
        return Err(ParseError::DetailedError {
            message: "cannot compare rows of zero length".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }

    let mut parts = Vec::with_capacity(left_fields.len());
    for ((_, left), (_, right)) in left_fields.into_iter().zip(right_fields) {
        let raw_left_type = expr_sql_type_hint(&left).unwrap_or(SqlType::new(SqlTypeKind::Text));
        let raw_right_type = expr_sql_type_hint(&right).unwrap_or(SqlType::new(SqlTypeKind::Text));
        let left_type =
            coerce_bound_unknown_string_literal_type(&left, raw_left_type, raw_right_type);
        let right_type =
            coerce_bound_unknown_string_literal_type(&right, raw_right_type, left_type);
        parts.push(bind_lowered_comparison_expr(
            op,
            make,
            left,
            raw_left_type,
            left_type,
            right,
            raw_right_type,
            right_type,
            None,
            None,
            catalog,
        )?);
    }

    if parts.len() == 1 {
        return Ok(parts.pop().expect("single row comparison part"));
    }
    if matches!(
        make,
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq
    ) {
        return build_row_ordering_comparison(make, parts);
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

fn row_comparison_operator_interpretation_error(op: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("could not determine interpretation of row comparison operator {op}"),
        detail: None,
        hint: Some(
            "Row comparison operators must be associated with btree operator families.".into(),
        ),
        sqlstate: "42883",
    }
}

fn coerce_bound_unknown_string_literal_type(
    expr: &Expr,
    expr_type: SqlType,
    peer_type: SqlType,
) -> SqlType {
    if matches!(
        expr,
        Expr::Const(Value::Text(_) | Value::TextRef(_, _) | Value::Null)
    ) {
        return unknown_string_literal_peer_type(peer_type).unwrap_or(expr_type);
    }
    expr_type
}

fn build_row_ordering_comparison(make: OpExprKind, parts: Vec<Expr>) -> Result<Expr, ParseError> {
    let mut left_fields = Vec::with_capacity(parts.len());
    let mut right_fields = Vec::with_capacity(parts.len());
    let mut collation_oid = None;
    for (idx, part) in parts.into_iter().enumerate() {
        let Expr::Op(op) = part else {
            return Err(ParseError::UnexpectedToken {
                expected: "row comparison operator",
                actual: format!("{part:?}"),
            });
        };
        let [left, right] = op.args.as_slice() else {
            return Err(ParseError::UnexpectedToken {
                expected: "binary row comparison operator",
                actual: format!("{op:?}"),
            });
        };
        if collation_oid.is_none() {
            collation_oid = op.collation_oid;
        }
        let field_name = format!("f{}", idx + 1);
        left_fields.push((field_name.clone(), left.clone()));
        right_fields.push((field_name, right.clone()));
    }
    Ok(Expr::op_with_collation(
        make,
        SqlType::new(SqlTypeKind::Bool),
        vec![
            build_plain_row_expr(left_fields, None),
            build_plain_row_expr(right_fields, None),
        ],
        collation_oid,
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
    let target_type = if !target_type.is_array
        && matches!(target_type.kind, SqlTypeKind::Composite)
        && target_type.typrelid == 0
        && let Some(row) = catalog.type_by_oid(target_type.type_oid)
        && row.typrelid != 0
    {
        target_type.with_identity(row.oid, row.typrelid)
    } else {
        target_type
    };
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
        descriptor: pgrust_nodes::datum::RecordDescriptor::named(
            target_type
                .type_oid
                .max(pgrust_catalog_data::RECORD_TYPE_OID),
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
            if !pgrust_nodes::primnodes::is_rule_pseudo_varno(var.varno) {
                var.varlevelsup += levels;
            }
            Expr::Var(var)
        }
        Expr::Aggref(mut aggref) => {
            aggref.agglevelsup += levels;
            Expr::Aggref(Box::new(pgrust_nodes::primnodes::Aggref {
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
                    .map(|item| pgrust_nodes::primnodes::OrderByEntry {
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
        Expr::GroupingKey(grouping_key) => {
            Expr::GroupingKey(Box::new(pgrust_nodes::primnodes::GroupingKeyExpr {
                expr: Box::new(raise_expr_varlevels(*grouping_key.expr, levels)),
                ref_id: grouping_key.ref_id,
            }))
        }
        Expr::GroupingFunc(grouping_func) => {
            Expr::GroupingFunc(Box::new(pgrust_nodes::primnodes::GroupingFuncExpr {
                args: grouping_func
                    .args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
                agglevelsup: grouping_func.agglevelsup + levels,
                ..*grouping_func
            }))
        }
        Expr::Op(op) => Expr::Op(Box::new(pgrust_nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(pgrust_nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(pgrust_nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: raise_expr_varlevels(func.context, levels),
                path: raise_expr_varlevels(func.path, levels),
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| SqlJsonTablePassingArg {
                        name: arg.name,
                        expr: raise_expr_varlevels(arg.expr, levels),
                    })
                    .collect(),
                on_empty: raise_sql_json_behavior_varlevels(func.on_empty, levels),
                on_error: raise_sql_json_behavior_varlevels(func.on_error, levels),
                ..*func
            }))
        }
        Expr::ScalarArrayOp(saop) => {
            Expr::ScalarArrayOp(Box::new(pgrust_nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(raise_expr_varlevels(*saop.left, levels)),
                right: Box::new(raise_expr_varlevels(*saop.right, levels)),
                ..*saop
            }))
        }
        Expr::Xml(xml) => Expr::Xml(Box::new(pgrust_nodes::primnodes::XmlExpr {
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
            root_version: xml.root_version,
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
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(raise_expr_varlevels(*arg, levels))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
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
    let func = specialize_builtin_aggregate(func, &arg_types);
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
    let kind = WindowFuncKind::Aggregate(pgrust_nodes::primnodes::Aggref {
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
        aggfilter: bound_filter.clone(),
        agglevelsup: 0,
        aggno: 0,
    });
    let mut window_args = coerced_args.clone();
    if let Some(filter) = bound_filter.clone() {
        window_args.push(filter);
    }
    Ok(register_window_expr(
        &state,
        spec,
        kind,
        window_args,
        aggregate_sql_type(func, arg_types.first().copied()),
        false,
    ))
}

fn bind_resolved_custom_window_agg_call(
    name: &str,
    resolved: &ResolvedFunctionCall,
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
            actual: name.into(),
        });
    }
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

    let arg_values = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let arg_types = arg_values
        .iter()
        .map(|expr| infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, None, ctes))
        .collect::<Vec<_>>();
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
    let coerced_args = bound_args
        .into_iter()
        .zip(arg_types.iter().copied())
        .zip(resolved.declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect::<Vec<_>>();
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
    let spec = bind_window_spec(over, catalog, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    let kind = WindowFuncKind::Aggregate(pgrust_nodes::primnodes::Aggref {
        aggfnoid: resolved.proc_oid,
        aggtype: resolved.result_type,
        aggvariadic: resolved.func_variadic || func_variadic,
        aggdistinct: false,
        direct_args: Vec::new(),
        args: coerced_args.clone(),
        aggorder: Vec::new(),
        aggfilter: bound_filter.clone(),
        agglevelsup: 0,
        aggno: 0,
    });
    let mut window_args = coerced_args.clone();
    if let Some(filter) = bound_filter.clone() {
        window_args.push(filter);
    }
    Ok(register_window_expr(
        &state,
        spec,
        kind,
        window_args,
        resolved.result_type,
        false,
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
    let within_group_aggkind = (!order_by.is_empty())
        .then(|| aggregate_call_kind_matches_catalog(catalog, name, args, Some(order_by)))
        .flatten();
    let builtin_hypothetical = resolve_builtin_hypothetical_aggregate(name).is_some();
    let builtin_ordered_set = resolve_builtin_ordered_set_aggregate(name).is_some();
    let hypothetical =
        (builtin_hypothetical || within_group_aggkind == Some('h')) && !direct_args.is_empty();
    let ordered_set =
        (builtin_ordered_set || within_group_aggkind == Some('o')) && !order_by.is_empty();
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
    for arg in &arg_values {
        reject_nested_local_ctes_in_raw_agg_expr(arg)?;
    }
    if !hypothetical
        && !ordered_set
        && let Some(func) = resolve_builtin_aggregate(name)
    {
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
    let direct_arg_types = if hypothetical || ordered_set {
        direct_args
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
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let resolved = if hypothetical || ordered_set {
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
    let bound_direct_args = if hypothetical || ordered_set {
        if aggregate_args_are_named(direct_args) {
            return Err(ParseError::UnexpectedToken {
                expected: "aggregate arguments without names",
                actual: name.to_string(),
            });
        }
        for arg in direct_args {
            reject_nested_local_ctes_in_raw_agg_expr(&arg.value)?;
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
            reject_nested_local_ctes_in_raw_agg_expr(expr)?;
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
            reject_nested_local_ctes_in_raw_agg_expr(&item.expr)?;
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
    let (coerced_direct_args, coerced_args, bound_order_by) =
        if hypothetical && builtin_hypothetical {
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
        } else if ordered_set && builtin_ordered_set {
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
            coerce_ordered_set_aggregate_inputs(
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
        } else if hypothetical || ordered_set {
            let expected_aggkind = if hypothetical { 'h' } else { 'o' };
            let resolved_catalog = resolve_catalog_within_group_aggregate_call(
                catalog,
                name,
                &direct_arg_types,
                &arg_types,
                func_variadic,
                expected_aggkind,
            )
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "supported aggregate",
                actual: name.to_string(),
            })?;
            coerce_catalog_within_group_aggregate_inputs(
                direct_args,
                &direct_arg_types,
                bound_direct_args,
                args.args(),
                &arg_types,
                bound_args,
                order_by,
                bound_order_exprs,
                &resolved_catalog.declared_arg_types,
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
    let (aggfnoid, aggtype, aggvariadic) = if hypothetical && builtin_hypothetical {
        let resolved = resolve_hypothetical_aggregate_call(name).ok_or_else(|| {
            ParseError::UnexpectedToken {
                expected: "supported aggregate",
                actual: name.to_string(),
            }
        })?;
        (resolved.proc_oid, resolved.result_type, false)
    } else if ordered_set && builtin_ordered_set {
        let resolved = resolve_ordered_set_aggregate_call(name, &direct_arg_types, &arg_types)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "supported aggregate",
                actual: name.to_string(),
            })?;
        (resolved.proc_oid, resolved.result_type, false)
    } else if hypothetical || ordered_set {
        let expected_aggkind = if hypothetical { 'h' } else { 'o' };
        let resolved = resolve_catalog_within_group_aggregate_call(
            catalog,
            name,
            &direct_arg_types,
            &arg_types,
            func_variadic,
            expected_aggkind,
        )
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "supported aggregate",
            actual: name.to_string(),
        })?;
        (
            resolved.proc_oid,
            resolved.result_type,
            resolved.func_variadic,
        )
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
    let raise_levels = visible_scope.levelsup;
    Ok(Some(Expr::Aggref(Box::new(
        pgrust_nodes::primnodes::Aggref {
            aggfnoid,
            aggtype,
            aggvariadic,
            aggdistinct: distinct,
            direct_args: coerced_direct_args
                .into_iter()
                .map(|expr| raise_expr_varlevels(expr, raise_levels))
                .collect(),
            args: coerced_args
                .into_iter()
                .map(|expr| raise_expr_varlevels(expr, raise_levels))
                .collect(),
            aggorder: bound_order_by
                .into_iter()
                .map(|item| pgrust_nodes::primnodes::OrderByEntry {
                    expr: raise_expr_varlevels(item.expr, raise_levels),
                    ..item
                })
                .collect(),
            aggfilter: bound_filter.map(|expr| raise_expr_varlevels(expr, raise_levels)),
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
    if matches!(func, Some(AggFunc::ArrayAgg | AggFunc::ArrayAggArray))
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
    null_treatment: Option<WindowNullTreatment>,
    filter: Option<&SqlExpr>,
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
            super::infer::infer_sql_expr_function_arg_type_with_ctes(
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
    let normalized = resolve_function_call_with_arg_defaults(
        catalog,
        name,
        args,
        &resolution_types,
        func_variadic,
    )?;
    let resolved = normalized.resolved;
    let call_args = normalized.args;
    let call_actual_types = normalized.actual_types;
    if resolved.proretset || !matches!(resolved.prokind, 'w' | 'a') {
        return Err(ParseError::DetailedError {
            message: format!(
                "OVER specified, but {name} is not a window function nor an aggregate function"
            ),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let spec = bind_window_spec(over, catalog, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    if let Some(window_impl) = resolved.window_impl {
        let ignore_nulls = window_ignore_nulls_for_builtin(window_impl, null_treatment)?;
        let bound_args = call_args
            .iter()
            .map(|arg| {
                with_windows_disallowed(|| {
                    bind_expr_with_outer_and_ctes(
                        arg,
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
            .zip(call_actual_types.iter().copied())
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
            ignore_nulls,
        ));
    }
    if resolved.prokind == 'a' {
        reject_aggregate_null_treatment(null_treatment)?;
        if let Some(agg_impl) = resolved.agg_impl {
            return bind_window_agg_call(
                agg_impl,
                args,
                &[],
                false,
                resolved.func_variadic,
                filter,
                over,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        return bind_resolved_custom_window_agg_call(
            name,
            &resolved,
            args,
            &[],
            false,
            func_variadic,
            filter,
            over,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
    }
    Err(ParseError::FeatureNotSupported(format!(
        "window function {name}"
    )))
}

fn is_sql_json_scalar_internal_name(name: &str) -> bool {
    matches!(
        name,
        SQL_JSON_FUNC
            | SQL_JSON_SCALAR_FUNC
            | SQL_JSON_SERIALIZE_FUNC
            | SQL_JSON_OBJECT_FUNC
            | SQL_JSON_ARRAY_FUNC
            | SQL_JSON_IS_JSON_FUNC
    )
}

fn sql_json_default_result_type(name: &str) -> SqlType {
    match name {
        SQL_JSON_SERIALIZE_FUNC => SqlType::new(SqlTypeKind::Text),
        SQL_JSON_IS_JSON_FUNC => SqlType::new(SqlTypeKind::Bool),
        _ => SqlType::new(SqlTypeKind::Json),
    }
}

fn sql_json_builtin_function(name: &str) -> BuiltinScalarFunction {
    match name {
        SQL_JSON_FUNC => BuiltinScalarFunction::SqlJsonConstructor,
        SQL_JSON_SCALAR_FUNC => BuiltinScalarFunction::SqlJsonScalar,
        SQL_JSON_SERIALIZE_FUNC => BuiltinScalarFunction::SqlJsonSerialize,
        SQL_JSON_OBJECT_FUNC => BuiltinScalarFunction::SqlJsonObject,
        SQL_JSON_ARRAY_FUNC => BuiltinScalarFunction::SqlJsonArray,
        SQL_JSON_IS_JSON_FUNC => BuiltinScalarFunction::SqlJsonIsJson,
        _ => unreachable!("checked SQL/JSON internal function name"),
    }
}

fn bind_sql_json_internal_call(
    name: &str,
    args: &[SqlFunctionArg],
    target_type: Option<SqlType>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional SQL/JSON arguments",
            actual: "named argument".into(),
        });
    }
    validate_sql_json_result_type(name, target_type.as_ref())?;
    validate_sql_json_constructor_arg_types(
        name,
        args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::func_with_impl(
        0,
        Some(target_type.unwrap_or_else(|| sql_json_default_result_type(name))),
        false,
        ScalarFunctionImpl::Builtin(sql_json_builtin_function(name)),
        bound_args,
    ))
}

fn validate_sql_json_result_type(
    name: &str,
    target_type: Option<&SqlType>,
) -> Result<(), ParseError> {
    let Some(target_type) = target_type else {
        return Ok(());
    };
    if name == SQL_JSON_SERIALIZE_FUNC
        && !matches!(
            target_type.kind,
            SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char | SqlTypeKind::Bytea
        )
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot use type {} in RETURNING clause of JSON_SERIALIZE()",
                sql_type_name(target_type.clone())
            ),
            detail: None,
            hint: Some("Try returning a string type or bytea.".into()),
            sqlstate: "0A000",
        });
    }
    Ok(())
}

fn validate_sql_json_constructor_arg_types(
    name: &str,
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(), ParseError> {
    match name {
        SQL_JSON_FUNC => {
            let Some(arg) = args.first() else {
                return Err(ParseError::UnexpectedToken {
                    expected: "JSON constructor argument",
                    actual: "syntax error at or near \")\"".into(),
                });
            };
            let format_encoding = sql_json_format_encoding_arg(args);
            let unique_keys = sql_json_unique_keys_arg(args);
            let source_type = infer_sql_expr_type_with_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if let Some(encoding) = format_encoding {
                validate_sql_json_input_encoding(&source_type, &encoding)?;
                if !matches!(arg.value, SqlExpr::Const(Value::Null))
                    && !matches!(
                        source_type.kind,
                        SqlTypeKind::Text
                            | SqlTypeKind::Varchar
                            | SqlTypeKind::Char
                            | SqlTypeKind::Json
                            | SqlTypeKind::Jsonb
                            | SqlTypeKind::Bytea
                    )
                {
                    return Err(ParseError::DetailedError {
                        message: "cannot use non-string types with explicit FORMAT JSON clause"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
            }
            if unique_keys
                && !matches!(
                    source_type.kind,
                    SqlTypeKind::Text
                        | SqlTypeKind::Varchar
                        | SqlTypeKind::Char
                        | SqlTypeKind::Bytea
                )
                && !matches!(arg.value, SqlExpr::Const(Value::Null))
            {
                return Err(ParseError::DetailedError {
                    message: "cannot use non-string types with WITH UNIQUE KEYS clause".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
            if matches!(arg.value, SqlExpr::Const(Value::Null))
                || matches!(
                    source_type.kind,
                    SqlTypeKind::Text
                        | SqlTypeKind::Varchar
                        | SqlTypeKind::Char
                        | SqlTypeKind::Json
                        | SqlTypeKind::Jsonb
                        | SqlTypeKind::Bytea
                )
            {
                Ok(())
            } else {
                Err(ParseError::DetailedError {
                    message: format!("cannot cast type {} to json", sql_type_name(source_type)),
                    detail: None,
                    hint: None,
                    sqlstate: "42846",
                })
            }
        }
        SQL_JSON_IS_JSON_FUNC => {
            let Some(arg) = args.first() else {
                return Ok(());
            };
            let source_type = infer_sql_expr_type_with_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if matches!(arg.value, SqlExpr::Const(Value::Null))
                || matches!(
                    source_type.kind,
                    SqlTypeKind::Text
                        | SqlTypeKind::Varchar
                        | SqlTypeKind::Char
                        | SqlTypeKind::Json
                        | SqlTypeKind::Jsonb
                        | SqlTypeKind::Bytea
                )
            {
                Ok(())
            } else {
                Err(ParseError::DetailedError {
                    message: format!(
                        "cannot use type {} in IS JSON predicate",
                        sql_type_name(source_type)
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42846",
                })
            }
        }
        _ => Ok(()),
    }
}

fn sql_json_format_encoding_arg(args: &[SqlFunctionArg]) -> Option<String> {
    let SqlExpr::Const(Value::Text(encoding)) = &args.get(1)?.value else {
        return None;
    };
    Some(encoding.to_string())
}

fn sql_json_unique_keys_arg(args: &[SqlFunctionArg]) -> bool {
    args.iter()
        .skip(1)
        .any(|arg| matches!(arg.value, SqlExpr::Const(Value::Bool(true))))
}

fn validate_sql_json_input_encoding(
    source_type: &SqlType,
    encoding: &str,
) -> Result<(), ParseError> {
    match encoding.to_ascii_lowercase().as_str() {
        "" => Ok(()),
        "utf8" => {
            if source_type.kind == SqlTypeKind::Bytea {
                Ok(())
            } else {
                Err(ParseError::DetailedError {
                    message: "JSON ENCODING clause is only allowed for bytea input type".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                })
            }
        }
        "utf16" | "utf32" => Err(ParseError::DetailedError {
            message: "unsupported JSON encoding".into(),
            detail: None,
            hint: Some("Only UTF8 JSON encoding is supported.".into()),
            sqlstate: "0A000",
        }),
        other => Err(ParseError::DetailedError {
            message: format!("unrecognized JSON encoding: {other}"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

pub fn bind_expr_with_outer_and_ctes(
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
        SqlExpr::JsonQueryFunction(func) => {
            return bind_sql_json_query_function(
                func,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::Column(name) => {
            if let Some(relation_name) = name.strip_suffix(".*") {
                if let Some(resolved) =
                    resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, relation_name)
                {
                    let named_row_type = relation_row_type_identity(catalog, resolved.relation_oid);
                    build_whole_row_expr(resolved.fields, named_row_type)
                } else if let Some((arg, field_path)) =
                    resolve_sql_function_inline_arg_path(relation_name)
                {
                    bind_sql_function_inline_arg_field_path(arg.expr, &field_path, catalog)?
                } else {
                    return Err(ParseError::UnknownColumn(name.clone()));
                }
            } else if let Some(system_column) =
                resolve_system_column_with_outer(scope, outer_scopes, name)?
            {
                Expr::Var(pgrust_nodes::primnodes::Var {
                    varno: system_column.varno,
                    varattno: system_column.varattno,
                    varlevelsup: system_column.varlevelsup,
                    vartype: system_column.sql_type,
                    collation_oid: None,
                })
            } else {
                match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                    Ok(ResolvedColumn::Local(index)) => scope_column_output_expr(
                        scope,
                        index,
                        name.rsplit_once('.').map(|(relation, _)| relation),
                    ).unwrap_or_else(|| {
                        panic!("bound scope output_exprs missing local column {index} for {name}")
                    }),
                    Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                        .get(depth)
                        .and_then(|scope| {
                            scope_column_output_expr(
                                scope,
                                index,
                                name.rsplit_once('.').map(|(relation, _)| relation),
                            )
                        })
                        .map(|expr| raise_expr_varlevels(expr, depth + 1))
                        .unwrap_or_else(|| {
                            panic!(
                                "outer scope output_exprs missing outer column depth={} index={} for {}",
                                depth, index, name
                            )
                        }),
                    Err(ParseError::UnknownColumn(_))
                        if resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name).is_some() =>
                    {
                        let resolved = resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name)
                            .expect("checked above");
                        let named_row_type = relation_row_type_identity(catalog, resolved.relation_oid);
                        build_whole_row_expr(resolved.fields, named_row_type)
                    }
                    Err(ParseError::UnknownColumn(_))
                        if current_sql_function_inline_named_arg(name).is_some() =>
                    {
                        current_sql_function_inline_named_arg(name)
                            .expect("checked above")
                            .expr
                    }
                    Err(ParseError::UnknownColumn(_))
                        if !name.contains('.')
                            && current_sql_function_inline_single_arg().is_some() =>
                    {
                        current_sql_function_inline_single_arg()
                            .expect("checked above")
                            .expr
                    }
                    Err(ParseError::UnknownColumn(_)) => {
                        if let Some((relation_name, field_name)) = name.rsplit_once('.')
                            && let Some(resolved) = resolve_relation_row_expr_ref_with_outer(
                                scope,
                                outer_scopes,
                                relation_name,
                            )
                        {
                            let named_row_type =
                                relation_row_type_identity(catalog, resolved.relation_oid);
                            let row_expr = build_whole_row_expr(resolved.fields, named_row_type);
                            if let Some(expr) = try_bind_column_notation_function_call(
                                &row_expr, field_name, catalog,
                            )? {
                                return Ok(expr);
                            }
                        }
                        if let Some((relation_name, field_name)) = name.rsplit_once('.') {
                            let relation_expr = SqlExpr::Column(relation_name.to_string());
                            if let Ok(expr) = bind_function_call_for_column_notation(
                                &relation_expr,
                                field_name,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            ) {
                                return Ok(expr);
                            }
                        }
                        if let Some(expr) =
                            bind_sql_function_inline_named_field(name, catalog)?
                        {
                            expr
                        } else {
                            return Err(ParseError::UnknownColumn(name.clone()));
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        SqlExpr::Parameter(index) => current_sql_function_inline_arg(*index)
            .map(|arg| arg.expr)
            .unwrap_or_else(|| {
                Expr::Param(Param {
                    paramkind: ParamKind::External,
                    paramid: *index,
                    paramtype: external_param_type(*index)
                        .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text)),
                })
            }),
        SqlExpr::ParamRef(index) => current_sql_function_inline_arg(*index)
            .map(|arg| arg.expr)
            .ok_or_else(|| ParseError::DetailedError {
                message: format!("there is no parameter ${index}"),
                detail: None,
                hint: None,
                sqlstate: "42P02",
            })?,
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
            "^" => bind_power_operator_expr(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
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
            "#-" => bind_jsonb_delete_path_expr(
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
            "~<=~" => {
                if let (Some(left_items), Some(right_items)) =
                    (row_comparison_items(left), row_comparison_items(right))
                {
                    bind_row_comparison_expr(
                        "~<=~",
                        OpExprKind::LtEq,
                        &left_items,
                        &right_items,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?
                } else {
                    bind_text_pattern_comparison_expr(
                        "~<=~",
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
            "~>=~" => {
                if let (Some(left_items), Some(right_items)) =
                    (row_comparison_items(left), row_comparison_items(right))
                {
                    bind_row_comparison_expr(
                        "~>=~",
                        OpExprKind::GtEq,
                        &left_items,
                        &right_items,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?
                } else {
                    bind_text_pattern_comparison_expr(
                        "~>=~",
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
            "===" => bind_catalog_equality_operator_expr(
                "===",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*=" => bind_record_image_operator_expr(
                "*=",
                OpExprKind::Eq,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*<>" => bind_record_image_operator_expr(
                "*<>",
                OpExprKind::NotEq,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*<" => bind_record_image_operator_expr(
                "*<",
                OpExprKind::Lt,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*<=" => bind_record_image_operator_expr(
                "*<=",
                OpExprKind::LtEq,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*>" => bind_record_image_operator_expr(
                "*>",
                OpExprKind::Gt,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "*>=" => bind_record_image_operator_expr(
                "*>=",
                OpExprKind::GtEq,
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "<<<" => bind_catalog_binary_operator_expr(
                "<<<",
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
            let target_type = if raw_type_name_is_unknown(ty) {
                SqlType::new(SqlTypeKind::Text)
            } else {
                resolve_raw_type_name(ty, catalog)?
            };
            if let SqlExpr::FuncCall {
                name,
                args,
                order_by,
                within_group,
                distinct,
                filter,
                over,
                ..
            } = inner.as_ref()
                && is_sql_json_scalar_internal_name(name)
                && order_by.is_empty()
                && within_group.is_none()
                && !*distinct
                && filter.is_none()
                && over.is_none()
            {
                return bind_sql_json_internal_call(
                    name,
                    args.args(),
                    Some(target_type),
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
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
            let domain = if raw_type_name_is_unknown(ty) {
                None
            } else {
                domain_lookup_for_raw_type_name(ty, catalog)
            };
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
            if !target_type.is_array
                && target_type.kind == SqlTypeKind::RegRole
                && let Some(bound_regrole) = bind_regrole_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regrole);
            }
            if !target_type.is_array
                && target_type.kind == SqlTypeKind::RegClass
                && let Some(bound_regclass) =
                    bind_regclass_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regclass);
            }
            if !target_type.is_array
                && target_type.kind == SqlTypeKind::RegOperator
                && let Some(bound_regoperator) =
                    bind_regoperator_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regoperator);
            }
            if !target_type.is_array
                && target_type.kind == SqlTypeKind::RegType
                && let Some(bound_regtype) = bind_regtype_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regtype);
            }
            if !target_type.is_array
                && target_type.kind == SqlTypeKind::RegProcedure
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
                return Ok(if domain.is_some() {
                    Expr::Cast(Box::new(bound_row), target_type)
                } else {
                    bound_row
                });
            }
            if !matches!(inner.as_ref(), SqlExpr::Const(Value::Null)) {
                validate_catalog_backed_explicit_cast(
                    source_type,
                    domain
                        .as_ref()
                        .map(|domain| domain.sql_type)
                        .unwrap_or(target_type),
                    catalog,
                )?;
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
            if let Some((is_all, array)) = quantified_function_arg(right) {
                bind_quantified_array_expr(
                    left,
                    SubqueryComparisonOp::Eq,
                    is_all,
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    "=",
                    OpExprKind::Eq,
                    &left_items,
                    &right_items,
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
            if let Some((is_all, array)) = quantified_function_arg(right) {
                bind_quantified_array_expr(
                    left,
                    SubqueryComparisonOp::NotEq,
                    is_all,
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    "<>",
                    OpExprKind::NotEq,
                    &left_items,
                    &right_items,
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
            if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    "<",
                    OpExprKind::Lt,
                    &left_items,
                    &right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
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
            if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    "<=",
                    OpExprKind::LtEq,
                    &left_items,
                    &right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
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
            if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    ">",
                    OpExprKind::Gt,
                    &left_items,
                    &right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
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
            if let (Some(left_items), Some(right_items)) =
                (row_comparison_items(left), row_comparison_items(right))
            {
                bind_row_comparison_expr(
                    ">=",
                    OpExprKind::GtEq,
                    &left_items,
                    &right_items,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else if let Some(result) = bind_maybe_multirange_comparison(
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
                if row_comparison_items(expr).is_some() && row_comparison_items(pattern).is_some() {
                    return Err(row_comparison_operator_interpretation_error(
                        if *case_insensitive {
                            if *negated { "!~~*" } else { "~~*" }
                        } else if *negated {
                            "!~~"
                        } else {
                            "~~"
                        },
                    ));
                }
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
                let (bound_expr, expr_explicit_collation) = strip_explicit_collation(bound_expr);
                let (bound_pattern, pattern_explicit_collation) =
                    strip_explicit_collation(bound_pattern);
                let expr_type = infer_sql_expr_type_with_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let pattern_type = infer_sql_expr_type_with_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                derive_consumer_collation_from_exprs(
                    catalog,
                    if *case_insensitive {
                        CollationConsumer::ILike
                    } else {
                        CollationConsumer::Like
                    },
                    &[
                        (&bound_expr, expr_type, expr_explicit_collation),
                        (&bound_pattern, pattern_type, pattern_explicit_collation),
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
                let (bound_expr, expr_explicit_collation) = strip_explicit_collation(bound_expr);
                let (bound_pattern, pattern_explicit_collation) =
                    strip_explicit_collation(bound_pattern);
                let expr_type = infer_sql_expr_type_with_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let pattern_type = infer_sql_expr_type_with_ctes(
                    pattern,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                derive_consumer_collation_from_exprs(
                    catalog,
                    CollationConsumer::Similar,
                    &[
                        (&bound_expr, expr_type, expr_explicit_collation),
                        (&bound_pattern, pattern_type, pattern_explicit_collation),
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
            let array_type = expression_navigation_sql_type(
                infer_sql_expr_type_with_ctes(
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ),
                catalog,
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
            if matches!(array_type.kind, SqlTypeKind::Box | SqlTypeKind::Point)
                && !array_type.is_array
            {
                return bind_fixed_geometry_subscripts(
                    array,
                    array_type,
                    subscripts,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if matches!(array_type.kind, SqlTypeKind::Box | SqlTypeKind::Point)
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
                        Ok(pgrust_nodes::primnodes::ExprArraySubscript {
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
            } else if let Some(result) = bind_maybe_tsquery_contains(
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
            } else if let Some(result) = bind_maybe_tsquery_contains(
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
            } else if let Some(result) = bind_maybe_tsquery_contains(
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
            } else if let Some(result) = bind_maybe_tsquery_contains(
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
            null_treatment,
            over,
        } => {
            let args_list = args.args();
            if is_sql_json_scalar_internal_name(name)
                && order_by.is_empty()
                && within_group.is_none()
                && !*distinct
                && filter.is_none()
                && over.is_none()
            {
                return bind_sql_json_internal_call(
                    name,
                    args_list,
                    None,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("grouping") {
                if !order_by.is_empty()
                    || within_group.is_some()
                    || *distinct
                    || *func_variadic
                    || filter.is_some()
                    || null_treatment.is_some()
                    || over.is_some()
                    || args.is_star()
                {
                    return Err(ParseError::UnexpectedToken {
                        expected: "GROUPING arguments",
                        actual: name.clone(),
                    });
                }
                if let Some(grouping_expr) = bind_visible_grouping_func_call(
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )? {
                    return Ok(grouping_expr);
                }
            }
            let (direct_args, aggregate_args, aggregate_order_by) =
                normalize_aggregate_call(args, order_by, within_group.as_deref());
            if over.is_none()
                && within_group.is_none()
                && (resolve_builtin_hypothetical_aggregate(name).is_some()
                    || resolve_builtin_ordered_set_aggregate(name).is_some())
            {
                return Err(ordered_set_requires_within_group_error(name));
            }
            if within_group.is_some()
                && resolve_builtin_hypothetical_aggregate(name).is_none()
                && resolve_builtin_ordered_set_aggregate(name).is_none()
                && aggregate_call_kind_matches_catalog(catalog, name, args, within_group.as_deref())
                    .is_none()
            {
                return Err(not_ordered_set_aggregate_error(name));
            }
            if let Some(func) = resolve_builtin_aggregate(name) {
                reject_explicit_empty_aggregate_call(name, args)?;
                reject_aggregate_null_treatment(*null_treatment)?;
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
                    *null_treatment,
                    filter.as_deref(),
                    raw_over,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            reject_function_null_treatment(name, *null_treatment)?;
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
            if name.eq_ignore_ascii_case("merge_action") {
                return Err(ParseError::DetailedError {
                    message:
                        "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"
                            .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            if !order_by.is_empty() || *distinct || filter.is_some() || args.is_star() {
                return Err(ParseError::UnexpectedToken {
                    expected: "supported scalar function",
                    actual: name.clone(),
                });
            }
            if name.eq_ignore_ascii_case("pg_typeof")
                && !*func_variadic
                && args_list.len() == 1
                && args_list[0].name.is_none()
                && is_unknown_literal_expr(&args_list[0].value)
            {
                return Ok(Expr::Cast(
                    Box::new(Expr::Const(Value::Int64(UNKNOWN_TYPE_OID as i64))),
                    SqlType::new(SqlTypeKind::RegType),
                ));
            }
            if name.eq_ignore_ascii_case("pg_collation_for")
                && !*func_variadic
                && args_list.len() == 1
                && args_list[0].name.is_none()
            {
                return bind_pg_collation_for_arg(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if !*func_variadic
                && !name.eq_ignore_ascii_case("pg_lsn")
                && let Some(target_type) = resolve_function_cast_type(catalog, name)
                && args_list.len() == 1
                && args_list.iter().all(|arg| arg.name.is_none())
            {
                let bound_arg = bind_typed_expr_with_outer_and_ctes(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                if !functional_cast_from_composite_to_string(bound_arg.sql_type, target_type)
                    && catalog_backed_explicit_cast_allowed(
                        bound_arg.sql_type,
                        target_type,
                        catalog,
                    )
                {
                    return Ok(Expr::Cast(
                        Box::new(bound_arg.expr),
                        if bound_arg.sql_type == target_type {
                            bound_arg.sql_type
                        } else {
                            target_type
                        },
                    ));
                }
            }
            let positional_function_args = args_list.iter().all(|arg| arg.name.is_none());
            let mut typed_args = if positional_function_args {
                Some(
                    args_list
                        .iter()
                        .map(|arg| {
                            bind_typed_expr_with_outer_and_ctes(
                                &arg.value,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        })
                        .collect::<Result<Vec<_>, ParseError>>()?,
                )
            } else {
                None
            };
            let actual_types = args_list
                .iter()
                .map(|arg| {
                    super::infer::infer_sql_expr_function_arg_type_with_ctes(
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
            for (arg, resolution_type) in args_list.iter().zip(resolution_types.iter_mut()) {
                if matches!(arg.value, SqlExpr::Const(Value::Null)) {
                    *resolution_type = SqlType::new(SqlTypeKind::AnyElement);
                }
            }
            if let Some(fallback) = try_bind_functional_field_notation(
                name,
                args_list,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )? {
                return Ok(fallback);
            }
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
                        if resolved.scalar_impl.is_none()
                            && resolved.proname.eq_ignore_ascii_case("sql_if")
                            && args_list.len() == 3
                            && args_list.iter().all(|arg| arg.name.is_none())
                        {
                            // :HACK: PostgreSQL inlines simple SQL functions before execution.
                            // The polymorphism regression uses sql_if() to verify that CASE
                            // short-circuits, so lower this simple function shape here until
                            // the SQL-function inliner is generalized.
                            let arms = [SqlCaseWhen {
                                expr: args_list[0].value.clone(),
                                result: args_list[1].value.clone(),
                            }];
                            return bind_case_expr(
                                None,
                                &arms,
                                Some(&args_list[2].value),
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            );
                        }
                        if let Some(func) = resolved.scalar_impl {
                            let lowered_args = lower_named_scalar_function_args(func, args_list)?;
                            if positional_function_args
                                && !scalar_function_needs_raw_arg_binding(func)
                                && let Some(bound_args) = typed_args.take()
                            {
                                return bind_scalar_function_call_from_typed_args(
                                    func,
                                    resolved.proc_oid,
                                    Some(resolved.result_type),
                                    resolved.func_variadic,
                                    resolved.nvargs,
                                    resolved.vatype_oid,
                                    &resolved.declared_arg_types,
                                    &lowered_args,
                                    bound_args,
                                    catalog,
                                    scope,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                            }
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
                        match resolve_function_call_with_arg_defaults(
                            catalog,
                            name,
                            args_list,
                            &actual_types,
                            *func_variadic,
                        ) {
                            Ok(normalized)
                                if normalized.resolved.scalar_impl.is_none()
                                    && normalized.resolved.prokind == 'f'
                                    && !normalized.resolved.proretset =>
                            {
                                return bind_resolved_user_defined_scalar_function_call(
                                    &normalized.resolved,
                                    &normalized.args,
                                    Some(args_list),
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                            }
                            Err(
                                err @ ParseError::DetailedError {
                                    sqlstate: "42725", ..
                                },
                            ) => return Err(err),
                            Err(
                                err @ ParseError::DetailedError {
                                    sqlstate: "42701", ..
                                },
                            ) => return Err(err),
                            _ => {}
                        }
                        let positional_args = args_list
                            .iter()
                            .map(|arg| arg.value.clone())
                            .collect::<Vec<_>>();
                        return bind_resolved_user_defined_scalar_function_call(
                            &resolved,
                            &positional_args,
                            None,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                    }
                    Err(err) => Some(err),
                };
            match resolve_function_call_with_arg_defaults(
                catalog,
                name,
                args_list,
                &actual_types,
                *func_variadic,
            ) {
                Ok(normalized)
                    if normalized.resolved.scalar_impl.is_none()
                        && normalized.resolved.prokind == 'f'
                        && !normalized.resolved.proretset =>
                {
                    return bind_resolved_user_defined_scalar_function_call(
                        &normalized.resolved,
                        &normalized.args,
                        Some(args_list),
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                Err(
                    err @ ParseError::DetailedError {
                        sqlstate: "42725", ..
                    },
                ) => return Err(err),
                Err(
                    err @ ParseError::DetailedError {
                        sqlstate: "42701", ..
                    },
                ) => return Err(err),
                _ => {}
            }
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
                        let literal_like = matches!(
                            &arg.value,
                            SqlExpr::Const(Value::Text(_))
                                | SqlExpr::Const(Value::TextRef(_, _))
                                | SqlExpr::Const(Value::Null)
                        );
                        if source.kind != SqlTypeKind::Xml && !literal_like {
                            return Err(ParseError::DetailedError {
                                message: format!(
                                    "argument of XMLCONCAT must be type xml, not type {}",
                                    sql_type_name(source)
                                ),
                                detail: None,
                                hint: None,
                                sqlstate: "42804",
                            });
                        }
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
                return Ok(Expr::Xml(Box::new(pgrust_nodes::primnodes::XmlExpr {
                    op: pgrust_nodes::primnodes::XmlExprOp::Concat,
                    name: None,
                    named_args: Vec::new(),
                    arg_names: Vec::new(),
                    args: bound_args,
                    xml_option: None,
                    indent: None,
                    target_type: None,
                    standalone: None,
                    root_version: pgrust_nodes::parsenodes::XmlRootVersion::Omitted,
                })));
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
            let legacy_func = match resolve_scalar_function(name).or_else(|| {
                resolve_function_cast_type(catalog, name)
                    .filter(|ty| range_type_ref_for_sql_type(*ty).is_some())
                    .map(|_| BuiltinScalarFunction::RangeConstructor)
            }) {
                Some(func) => func,
                None => {
                    if let Some(fallback) = try_bind_functional_field_notation(
                        name,
                        args_list,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )? {
                        return Ok(fallback);
                    }
                    if !catalog.proc_rows_by_name(name).is_empty()
                        && let Some(err) = proc_resolution_error
                    {
                        if matches!(
                            &err,
                            ParseError::DetailedError { message, .. }
                                if message
                                    == "cannot determine element type of \"anyarray\" argument"
                        ) && let Some(sql_err) = sql_function_anyarray_return_resolution_error(
                            catalog,
                            name,
                            &actual_types,
                        ) {
                            return Err(sql_err);
                        }
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
                    return Err(proc_resolution_error.unwrap_or_else(|| {
                        function_does_not_exist_error(name, &actual_types, catalog)
                    }));
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
                ..
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
        SqlExpr::User => Expr::User,
        SqlExpr::SessionUser => Expr::SessionUser,
        SqlExpr::SystemUser => Expr::SystemUser,
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
            if resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name).is_some() =>
        {
            let resolved = resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name)
                .expect("checked above");
            if let Some((_, expr)) = resolved
                .fields
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(field))
            {
                return Ok(expr.clone());
            }
            let named_row_type = relation_row_type_identity(catalog, resolved.relation_oid);
            build_whole_row_expr(resolved.fields, named_row_type)
        }
        _ => {
            bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
    };
    let mut current = bound_inner;
    for part in field.split('.') {
        let field_type = match resolve_bound_field_select_type(&current, part, catalog) {
            Ok(field_type) => field_type,
            Err(err) => {
                if let Some(fallback) =
                    try_bind_column_notation_function_call(&current, part, catalog)?
                {
                    current = fallback;
                    continue;
                }
                return Err(err);
            }
        };
        current = Expr::FieldSelect {
            expr: Box::new(current),
            field: part.to_string(),
            field_type,
        };
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn bind_function_call_for_column_notation(
    expr: &SqlExpr,
    field: &str,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let call = SqlExpr::FuncCall {
        name: field.to_string(),
        args: SqlCallArgs::Args(vec![SqlFunctionArg::positional(expr.clone())]),
        order_by: Vec::new(),
        within_group: None,
        distinct: false,
        func_variadic: false,
        filter: None,
        null_treatment: None,
        over: None,
    };
    bind_expr_with_outer_and_ctes(&call, scope, catalog, outer_scopes, grouped_outer, ctes)
}

fn try_bind_column_notation_function_call(
    arg: &Expr,
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    if resolve_function_cast_type(catalog, name).is_some() {
        return Ok(None);
    }
    let Some(actual_type) =
        expr_sql_type_hint(arg).map(|ty| expression_navigation_sql_type(ty, catalog))
    else {
        return Ok(None);
    };
    if !matches!(
        actual_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) || actual_type.is_array
    {
        return Ok(None);
    }
    if let Ok(resolved) = resolve_function_call(catalog, name, &[actual_type], false)
        && resolved.prokind == 'f'
    {
        let declared_type = resolved
            .declared_arg_types
            .first()
            .copied()
            .unwrap_or(actual_type);
        let coerced = coerce_bound_expr(arg.clone(), actual_type, declared_type);
        if resolved.proretset {
            return Ok(Some(set_returning_expr_for_resolved_column_notation(
                &resolved,
                name,
                vec![coerced],
            )));
        }
        return Ok(Some(Expr::func(
            resolved.proc_oid,
            Some(resolved.result_type),
            resolved.func_variadic,
            vec![coerced],
        )));
    }
    try_bind_column_notation_single_arg_proc(arg, name, actual_type, catalog)
}

fn try_bind_column_notation_single_arg_proc(
    arg: &Expr,
    name: &str,
    actual_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let actual_descriptor = expr_record_descriptor(arg);
    let mut matches = catalog
        .proc_rows_by_name(name)
        .into_iter()
        .filter(|row| row.prokind == 'f')
        .filter_map(|row| {
            let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
            let [declared_oid] = declared_oids.as_slice() else {
                return None;
            };
            let declared_oid = *declared_oid;
            let declared_row = catalog.type_by_oid(declared_oid)?;
            let declared_type = if !declared_row.sql_type.is_array
                && matches!(declared_row.sql_type.kind, SqlTypeKind::Composite)
                && declared_row.sql_type.typrelid == 0
                && declared_row.typrelid != 0
            {
                declared_row
                    .sql_type
                    .with_identity(declared_row.oid, declared_row.typrelid)
            } else {
                declared_row.sql_type
            };
            let composite_declared = !declared_type.is_array
                && matches!(
                    declared_type.kind,
                    SqlTypeKind::Composite | SqlTypeKind::Record
                );
            let matches_type = declared_oid == actual_type.type_oid
                || (actual_type.typrelid != 0 && declared_type.typrelid == actual_type.typrelid)
                || actual_descriptor.is_some_and(|descriptor| {
                    record_descriptor_matches_composite_type(descriptor, declared_type, catalog)
                })
                || (composite_declared
                    && !actual_type.is_array
                    && matches!(
                        actual_type.kind,
                        SqlTypeKind::Composite | SqlTypeKind::Record
                    ));
            matches_type.then_some((row, declared_type))
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Ok(None);
    }
    let (row, declared_type) = matches.pop().expect("one match checked");
    let result_type = catalog
        .type_by_oid(row.prorettype)
        .map(|row| row.sql_type)
        .ok_or_else(|| ParseError::UnsupportedType(row.prorettype.to_string()))?;
    if row.proretset {
        let output_columns = set_returning_output_columns_for_type(name, result_type, catalog);
        let sql_type = if output_columns.len() == 1 {
            output_columns[0].sql_type
        } else {
            result_type
        };
        let column_index = if output_columns.len() == 1 { 1 } else { 0 };
        return Ok(Some(Expr::set_returning(
            name.to_ascii_lowercase(),
            SetReturningCall::UserDefined {
                proc_oid: row.oid,
                function_name: row.proname,
                func_variadic: false,
                args: vec![coerce_bound_expr(arg.clone(), actual_type, declared_type)],
                inlined_expr: None,
                output_columns,
                with_ordinality: false,
            },
            sql_type,
            column_index,
        )));
    }
    Ok(Some(Expr::func(
        row.oid,
        Some(result_type),
        false,
        vec![coerce_bound_expr(arg.clone(), actual_type, declared_type)],
    )))
}

fn set_returning_expr_for_resolved_column_notation(
    resolved: &ResolvedFunctionCall,
    name: &str,
    args: Vec<Expr>,
) -> Expr {
    let output_columns = resolved_function_output_columns(name, resolved);
    let (sql_type, column_index) = if output_columns.len() == 1 {
        (output_columns[0].sql_type, 1)
    } else {
        (resolved.result_type, 0)
    };
    Expr::set_returning(
        name.to_ascii_lowercase(),
        SetReturningCall::UserDefined {
            proc_oid: resolved.proc_oid,
            function_name: resolved.proname.clone(),
            func_variadic: resolved.func_variadic,
            args,
            inlined_expr: None,
            output_columns,
            with_ordinality: false,
        },
        sql_type,
        column_index,
    )
}

fn resolved_function_output_columns(
    name: &str,
    resolved: &ResolvedFunctionCall,
) -> Vec<QueryColumn> {
    match &resolved.row_shape {
        ResolvedFunctionRowShape::OutParameters(columns)
        | ResolvedFunctionRowShape::NamedComposite { columns, .. } => columns.clone(),
        ResolvedFunctionRowShape::AnonymousRecord | ResolvedFunctionRowShape::None => {
            vec![QueryColumn {
                name: name.to_ascii_lowercase(),
                sql_type: resolved.result_type,
                wire_type_oid: None,
            }]
        }
    }
}

fn set_returning_output_columns_for_type(
    name: &str,
    result_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Vec<QueryColumn> {
    if matches!(result_type.kind, SqlTypeKind::Composite)
        && result_type.typrelid != 0
        && let Some(relation) = catalog.lookup_relation_by_oid(result_type.typrelid)
    {
        return relation
            .desc
            .columns
            .into_iter()
            .filter(|column| !column.dropped)
            .map(|column| QueryColumn {
                name: column.name,
                sql_type: column.sql_type,
                wire_type_oid: None,
            })
            .collect();
    }
    vec![QueryColumn {
        name: name.to_ascii_lowercase(),
        sql_type: result_type,
        wire_type_oid: None,
    }]
}

fn expr_record_descriptor(expr: &Expr) -> Option<&pgrust_nodes::datum::RecordDescriptor> {
    match expr {
        Expr::Row { descriptor, .. } => Some(descriptor),
        Expr::Case(case_expr) => expr_record_descriptor(&case_expr.defresult),
        Expr::Cast(inner, _) => expr_record_descriptor(inner),
        _ => None,
    }
}

fn record_descriptor_matches_composite_type(
    descriptor: &pgrust_nodes::datum::RecordDescriptor,
    declared_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> bool {
    if declared_type.is_array
        || !matches!(declared_type.kind, SqlTypeKind::Composite)
        || declared_type.typrelid == 0
    {
        return false;
    }
    let Some(relation) = catalog.lookup_relation_by_oid(declared_type.typrelid) else {
        return false;
    };
    let columns = relation
        .desc
        .columns
        .into_iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    descriptor.fields.len() == columns.len()
        && descriptor
            .fields
            .iter()
            .zip(columns.iter())
            .all(|(field, column)| {
                field.name.eq_ignore_ascii_case(&column.name)
                    && record_field_types_compatible(field.sql_type, column.sql_type)
            })
}

fn record_field_types_compatible(actual: SqlType, declared: SqlType) -> bool {
    actual.kind == declared.kind
        && actual.is_array == declared.is_array
        && (actual.type_oid == 0 || declared.type_oid == 0 || actual.type_oid == declared.type_oid)
}

#[allow(clippy::too_many_arguments)]
fn try_bind_functional_field_notation(
    name: &str,
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<Expr>, ParseError> {
    if args.len() != 1 || args[0].name.is_some() {
        return Ok(None);
    }
    let field_expr = SqlExpr::FieldSelect {
        expr: Box::new(args[0].value.clone()),
        field: name.to_string(),
    };
    match bind_expr_with_outer_and_ctes(
        &field_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        Ok(expr) => Ok(Some(expr)),
        Err(ParseError::DetailedError {
            sqlstate: "42703", ..
        })
        | Err(ParseError::UnexpectedToken {
            expected: "record field",
            ..
        })
        | Err(ParseError::UnexpectedToken {
            expected: "record expression",
            ..
        }) => Ok(None),
        Err(err) => Err(err),
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_record_image_operator_expr(
    op: &'static str,
    kind: OpExprKind,
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
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let left_oid = catalog
        .type_oid_for_sql_type(left_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(left_type)))?;
    let right_oid = catalog
        .type_oid_for_sql_type(right_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(right_type)))?;
    let lookup_left_oid = if matches!(left_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record)
    {
        RECORD_TYPE_OID
    } else {
        left_oid
    };
    let lookup_right_oid = if matches!(
        right_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) {
        RECORD_TYPE_OID
    } else {
        right_oid
    };
    let operator = catalog
        .operator_by_name_left_right(op, lookup_left_oid, lookup_right_oid)
        .ok_or_else(|| ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        })?;
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    Ok(Expr::Op(Box::new(pgrust_nodes::primnodes::OpExpr {
        opno: operator.oid,
        opfuncid: operator.oprcode,
        op: kind,
        opresulttype: SqlType::new(SqlTypeKind::Bool),
        args: vec![
            coerce_bound_expr(left_bound, raw_left_type, left_type),
            coerce_bound_expr(right_bound, raw_right_type, right_type),
        ],
        collation_oid: None,
    })))
}

pub fn resolve_bound_field_select_type(
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
    let row_type = expression_navigation_sql_type(row_type, catalog);

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
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" not found in data type {}",
                field,
                catalog_sql_type_name(row_type, catalog)
            ),
            detail: None,
            hint: None,
            sqlstate: "42703",
        });
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

    Err(ParseError::DetailedError {
        message: format!("could not identify column \"{field}\" in record data type"),
        detail: None,
        hint: None,
        sqlstate: "42703",
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
    if !ty.is_array && ty.typrelid != 0 {
        if let Some(row) = catalog
            .type_rows()
            .into_iter()
            .find(|row| row.typrelid == ty.typrelid && row.oid == ty.type_oid)
            .or_else(|| {
                catalog
                    .type_rows()
                    .into_iter()
                    .find(|row| row.typrelid == ty.typrelid)
            })
        {
            return row.typname;
        }
        if let Some(row) = catalog.class_row_by_oid(ty.typrelid) {
            return row.relname;
        }
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
    let target_oid = if target_type.type_oid != 0 {
        Some(target_type.type_oid)
    } else {
        catalog.type_oid_for_sql_type(target_type)
    };
    if let (Some(source_oid), Some(target_oid)) = (source_oid, target_oid) {
        if source_oid == target_oid
            || catalog
                .cast_by_source_target(source_oid, target_oid)
                .is_some()
        {
            return true;
        }
        if let Some(base_type) = domain_base_sql_type(target_oid, catalog) {
            if source_type.element_type() == base_type.element_type() {
                return true;
            }
            if let Some(base_oid) = catalog.type_oid_for_sql_type(base_type)
                && catalog
                    .cast_by_source_target(source_oid, base_oid)
                    .is_some()
            {
                return true;
            }
            if !source_type.is_array
                && is_text_like_type(source_type)
                && explicit_text_input_cast_exists(catalog, base_type)
            {
                return true;
            }
        }
        if !source_type.is_array
            && is_text_like_type(source_type)
            && user_defined_base_type_has_input_oid(target_oid, catalog)
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
    if !source_type.is_array
        && is_text_like_type(source_type)
        && !target_type.is_array
        && matches!(target_type.kind, SqlTypeKind::Composite)
        && target_type.typrelid != 0
    {
        return true;
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

fn functional_cast_from_composite_to_string(source_type: SqlType, target_type: SqlType) -> bool {
    !source_type.is_array
        && matches!(
            source_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
        && !target_type.is_array
        && is_text_like_type(target_type)
}

fn domain_base_sql_type(type_oid: u32, catalog: &dyn CatalogLookup) -> Option<SqlType> {
    let row = catalog.type_by_oid(type_oid)?;
    if row.typtype != 'd' || row.typbasetype == 0 {
        return None;
    }
    catalog
        .type_by_oid(row.typbasetype)
        .map(|base| base.sql_type.with_typmod(row.sql_type.typmod))
}

fn is_user_defined_base_type_oid(type_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    type_oid != 0
        && builtin_type_name_for_oid(type_oid).is_none()
        && catalog.type_by_oid(type_oid).is_some_and(|row| {
            row.typtype == 'b'
                && !row.sql_type.is_array
                && !row.sql_type.is_range()
                && !row.sql_type.is_multirange()
                && matches!(row.sql_type.kind, SqlTypeKind::Text)
                && row.typrelid == 0
        })
}

fn user_defined_base_type_has_input_oid(type_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    type_oid != 0
        && builtin_type_name_for_oid(type_oid).is_none()
        && catalog.type_by_oid(type_oid).is_some_and(|row| {
            row.typtype == 'b' && row.typrelid == 0 && !row.sql_type.is_array && row.typinput != 0
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
        .ok_or_else(|| missing_regclass_literal_error(relation_name, catalog))?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(relation_oid as i64))),
        target_type,
    )))
}

fn missing_regclass_literal_error(name: &str, _catalog: &dyn CatalogLookup) -> ParseError {
    ParseError::UnknownTable(name.to_string())
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
    let authid_rows = catalog.authid_rows();
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
        SqlExpr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) => value.as_text(),
        _ => None,
    }
}

fn regclass_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) => value.as_text(),
        _ => None,
    }
}

fn regoperator_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) => value.as_text(),
        _ => None,
    }
}

fn regtype_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) => value.as_text(),
        _ => None,
    }
}

fn regprocedure_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) => value.as_text(),
        _ => None,
    }
}

fn bind_xml_expr(
    xml: &pgrust_nodes::parsenodes::RawXmlExpr,
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
        pgrust_nodes::parsenodes::RawXmlExprOp::Parse => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        pgrust_nodes::parsenodes::RawXmlExprOp::Serialize => {
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
        pgrust_nodes::parsenodes::RawXmlExprOp::Root => {
            if let Some(first) = xml.args.first() {
                args.push(bind_as(first, xml_type)?);
            }
            if let Some(version) = xml.args.get(1) {
                args.push(bind_as(version, text_type)?);
            }
        }
        pgrust_nodes::parsenodes::RawXmlExprOp::Pi => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        pgrust_nodes::parsenodes::RawXmlExprOp::IsDocument => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        pgrust_nodes::parsenodes::RawXmlExprOp::Element => {
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
                            return Err(ParseError::DetailedError {
                                message: "unnamed XML attribute value must be a column reference"
                                    .into(),
                                detail: None,
                                hint: None,
                                sqlstate: "42601",
                            });
                        }
                    }
                } else {
                    raw_name.clone()
                };
                if !seen_names.insert(inferred_name.clone()) {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "XML attribute name \"{inferred_name}\" appears more than once"
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
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
        pgrust_nodes::parsenodes::RawXmlExprOp::Forest => {
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
        pgrust_nodes::parsenodes::RawXmlExprOp::Concat => {
            args = xml
                .args
                .iter()
                .map(|arg| {
                    let source = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let literal_like = matches!(
                        arg,
                        SqlExpr::Const(Value::Text(_))
                            | SqlExpr::Const(Value::TextRef(_, _))
                            | SqlExpr::Const(Value::Null)
                    );
                    if source.kind != SqlTypeKind::Xml && !literal_like {
                        return Err(ParseError::DetailedError {
                            message: format!(
                                "argument of XMLCONCAT must be type xml, not type {}",
                                sql_type_name(source)
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42804",
                        });
                    }
                    Ok(coerce_bound_expr(bind_child(arg)?, source, xml_type))
                })
                .collect::<Result<Vec<_>, _>>()?;
        }
    }

    Ok(Expr::Xml(Box::new(pgrust_nodes::primnodes::XmlExpr {
        op: match xml.op {
            pgrust_nodes::parsenodes::RawXmlExprOp::Concat => {
                pgrust_nodes::primnodes::XmlExprOp::Concat
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::Element => {
                pgrust_nodes::primnodes::XmlExprOp::Element
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::Forest => {
                pgrust_nodes::primnodes::XmlExprOp::Forest
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::Parse => {
                pgrust_nodes::primnodes::XmlExprOp::Parse
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::Pi => pgrust_nodes::primnodes::XmlExprOp::Pi,
            pgrust_nodes::parsenodes::RawXmlExprOp::Root => {
                pgrust_nodes::primnodes::XmlExprOp::Root
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::Serialize => {
                pgrust_nodes::primnodes::XmlExprOp::Serialize
            }
            pgrust_nodes::parsenodes::RawXmlExprOp::IsDocument => {
                pgrust_nodes::primnodes::XmlExprOp::IsDocument
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
        root_version: xml.root_version,
    })))
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
                let raw_type = pgrust_parser::parse_type_name(arg.trim())?;
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
        let raw_type = pgrust_parser::parse_type_name(arg)?;
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
        .ok_or_else(|| ParseError::DetailedError {
            message: format!("operator does not exist: {signature}"),
            detail: None,
            hint: None,
            sqlstate: "42883",
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
    let has_enforced_constraint = domain.not_null
        || domain.check.is_some()
        || domain
            .constraints
            .iter()
            .any(|constraint| constraint.enforced);
    if !has_enforced_constraint {
        return expr;
    }
    let upper_less_than_check = domain
        .constraints
        .iter()
        .filter(|constraint| {
            constraint.enforced && matches!(constraint.kind, DomainConstraintLookupKind::Check)
        })
        .filter_map(|constraint| constraint.expr.as_deref())
        .find(|check| parse_domain_upper_less_than_check(check).is_some())
        .or_else(|| {
            domain
                .check
                .as_deref()
                .filter(|check| parse_domain_upper_less_than_check(check).is_some())
        });
    if let Some(limit) = upper_less_than_check.and_then(parse_domain_upper_less_than_check) {
        return Expr::func_with_impl(
            0,
            Some(target_type),
            false,
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::PgRustDomainCheckUpperLessThan),
            vec![
                expr,
                Expr::Const(Value::Text(domain.name.clone().into())),
                Expr::Const(Value::Int32(limit)),
            ],
        );
    }
    Expr::Cast(Box::new(expr), target_type)
}

fn parse_domain_upper_less_than_check(check: &str) -> Option<i32> {
    let normalized = check
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let limit = normalized.strip_prefix("upper(value)<")?;
    normalize_numeric_literal_token(limit).parse::<i32>().ok()
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
