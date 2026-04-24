use super::*;

fn jsonb_operator_metadata(op: crate::include::nodes::primnodes::OpExprKind) -> Option<(u32, u32)> {
    use crate::include::catalog::{
        JSONB_CONTAINED_OPERATOR_OID, JSONB_CONTAINED_PROC_OID, JSONB_CONTAINS_OPERATOR_OID,
        JSONB_CONTAINS_PROC_OID, JSONB_EXISTS_ALL_OPERATOR_OID, JSONB_EXISTS_ALL_PROC_OID,
        JSONB_EXISTS_ANY_OPERATOR_OID, JSONB_EXISTS_ANY_PROC_OID, JSONB_EXISTS_OPERATOR_OID,
        JSONB_EXISTS_PROC_OID,
    };
    Some(match op {
        crate::include::nodes::primnodes::OpExprKind::JsonbContains => {
            (JSONB_CONTAINS_OPERATOR_OID, JSONB_CONTAINS_PROC_OID)
        }
        crate::include::nodes::primnodes::OpExprKind::JsonbContained => {
            (JSONB_CONTAINED_OPERATOR_OID, JSONB_CONTAINED_PROC_OID)
        }
        crate::include::nodes::primnodes::OpExprKind::JsonbExists => {
            (JSONB_EXISTS_OPERATOR_OID, JSONB_EXISTS_PROC_OID)
        }
        crate::include::nodes::primnodes::OpExprKind::JsonbExistsAny => {
            (JSONB_EXISTS_ANY_OPERATOR_OID, JSONB_EXISTS_ANY_PROC_OID)
        }
        crate::include::nodes::primnodes::OpExprKind::JsonbExistsAll => {
            (JSONB_EXISTS_ALL_OPERATOR_OID, JSONB_EXISTS_ALL_PROC_OID)
        }
        _ => return None,
    })
}

fn jsonb_op_expr(op: crate::include::nodes::primnodes::OpExprKind, args: Vec<Expr>) -> Expr {
    if let Some((opno, opfuncid)) = jsonb_operator_metadata(op) {
        Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            opno,
            opfuncid,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args,
            collation_oid: None,
        }))
    } else {
        Expr::op_auto(op, args)
    }
}

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

pub(super) fn bind_jsonb_subscript_expr(
    array: &SqlExpr,
    subscripts: &[crate::include::nodes::parsenodes::ArraySubscript],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        return Err(ParseError::DetailedError {
            message: "jsonb subscript does not support slices".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let mut bound =
        bind_expr_with_outer_and_ctes(array, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    for subscript in subscripts {
        let key = if let Some(lower) = &subscript.lower {
            bind_expr_with_outer_and_ctes(lower, scope, catalog, outer_scopes, grouped_outer, ctes)?
        } else {
            Expr::Const(Value::Int64(1))
        };
        bound = Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::JsonGet,
            vec![bound, key],
        );
    }
    Ok(bound)
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
        .map(|(left_bound, right_bound)| {
            Expr::builtin_func(
                BuiltinScalarFunction::JsonbDelete,
                Some(SqlType::new(SqlTypeKind::Jsonb)),
                false,
                vec![
                    coerce_bound_expr(left_bound, left_type, SqlType::new(SqlTypeKind::Jsonb)),
                    coerce_bound_expr(right_bound, raw_right_type, right_type),
                ],
            )
        }),
    )
}

pub(super) fn bind_json_binary_expr(
    op: crate::include::nodes::primnodes::OpExprKind,
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
    let (left_bound, right_bound) = bind_json_binary_operands(
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let right = match op {
        crate::include::nodes::primnodes::OpExprKind::JsonPath
        | crate::include::nodes::primnodes::OpExprKind::JsonPathText => {
            let target = SqlType::array_of(SqlType::new(SqlTypeKind::Text));
            let resolved = coerce_unknown_string_literal_type(right, raw_right_type, target);
            if resolved == target && raw_right_type != target {
                coerce_bound_expr(right_bound, raw_right_type, target)
            } else {
                right_bound
            }
        }
        _ => right_bound,
    };
    let left = left_bound;
    let _ = raw_left_type;
    Ok(jsonb_op_expr(op, vec![left, right]))
}

fn bind_jsonb_containment_expr(
    op: crate::include::nodes::primnodes::OpExprKind,
    op_name: &'static str,
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
    let jsonb_type = SqlType::new(SqlTypeKind::Jsonb);

    let left_is_string_literal = matches!(
        left,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let right_is_string_literal = matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );

    let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, raw_left_type);

    if left_is_string_literal && raw_right_type.kind == SqlTypeKind::Jsonb {
        left_type = jsonb_type;
    }
    if right_is_string_literal && raw_left_type.kind == SqlTypeKind::Jsonb {
        right_type = jsonb_type;
    }
    if left_is_string_literal && right_is_string_literal {
        left_type = jsonb_type;
        right_type = jsonb_type;
    }

    if left_type.kind != SqlTypeKind::Jsonb || right_type.kind != SqlTypeKind::Jsonb {
        return Err(ParseError::UndefinedOperator {
            op: op_name.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }

    let (left_bound, right_bound) = bind_json_binary_operands(
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    Ok(jsonb_op_expr(
        op,
        vec![
            coerce_bound_expr(left_bound, raw_left_type, jsonb_type),
            coerce_bound_expr(right_bound, raw_right_type, jsonb_type),
        ],
    ))
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
        bind_jsonb_containment_expr(
            crate::include::nodes::primnodes::OpExprKind::JsonbContains,
            "@>",
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
        bind_jsonb_containment_expr(
            crate::include::nodes::primnodes::OpExprKind::JsonbContained,
            "<@",
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
        crate::include::nodes::primnodes::OpExprKind::JsonbExists,
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
            crate::include::nodes::primnodes::OpExprKind::JsonbExistsAny,
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
        crate::include::nodes::primnodes::OpExprKind::JsonbExistsAll,
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
    op: crate::include::nodes::primnodes::OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    bind_json_binary_expr(
        op,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}
