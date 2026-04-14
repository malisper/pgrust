use super::*;

fn bind_json_binary_operands(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, Expr), ParseError> {
    Ok((
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?,
    ))
}

pub(super) fn bind_maybe_jsonb_delete(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let right_is_string_literal = matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    if left_type != SqlType::new(SqlTypeKind::Jsonb) {
        return None;
    }
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = if right_is_string_literal {
        SqlType::new(SqlTypeKind::Text)
    } else if raw_right_type.is_array {
        SqlType::array_of(SqlType::new(SqlTypeKind::Text))
    } else if is_integer_family(raw_right_type) {
        SqlType::new(SqlTypeKind::Int4)
    } else {
        return None;
    };
    Some(
        bind_json_binary_operands(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .map(|(left_bound, right_bound)| Expr::FuncCall {
            func_oid: 0,
            func: BuiltinScalarFunction::JsonbDelete,
            args: vec![
                coerce_bound_expr(left_bound, left_type, SqlType::new(SqlTypeKind::Jsonb)),
                coerce_bound_expr(right_bound, raw_right_type, right_type),
            ],
            func_variadic: false,
        }),
    )
}

pub(super) fn bind_json_binary_expr(
    constructor: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let (left, right) = bind_json_binary_operands(
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    Ok(constructor(Box::new(left), Box::new(right)))
}

pub(super) fn bind_jsonb_contains_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let Some(result) = bind_maybe_geometry_comparison(
        "@>",
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        result
    } else {
        bind_json_binary_expr(
            Expr::JsonbContains,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    }
}

pub(super) fn bind_jsonb_contained_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let Some(result) = bind_maybe_geometry_comparison(
        "<@",
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        result
    } else {
        bind_json_binary_expr(
            Expr::JsonbContained,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    }
}

pub(super) fn bind_jsonb_exists_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_json_binary_expr(
        Expr::JsonbExists,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_jsonb_exists_any_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if is_geometry_type(left_type) || is_geometry_type(right_type) {
        bind_geometry_binary_expr(
            GeometryBinaryOp::IsVertical,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    } else {
        bind_json_binary_expr(
            Expr::JsonbExistsAny,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    }
}

pub(super) fn bind_jsonb_exists_all_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_json_binary_expr(
        Expr::JsonbExistsAll,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_jsonb_path_binary_expr(
    constructor: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_json_binary_expr(
        constructor,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}
