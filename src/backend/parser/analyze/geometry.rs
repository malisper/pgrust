use super::*;

fn is_string_literal_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    )
}

fn bind_geometry_call(
    func: BuiltinScalarFunction,
    args: &[&SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let arg_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    let peer_geometry_type = if args.len() == 2 {
        match (arg_types[0], arg_types[1]) {
            (left, _) if is_geometry_type(left) && is_string_literal_expr(args[1]) => {
                Some((1usize, left.element_type()))
            }
            (_, right) if is_string_literal_expr(args[0]) && is_geometry_type(right) => {
                Some((0usize, right.element_type()))
            }
            _ => None,
        }
    } else {
        None
    };
    Ok(Expr::builtin_func(
        func,
        None,
        false,
        args.iter()
            .enumerate()
            .map(|(idx, arg)| {
                let bound = bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                Ok(match peer_geometry_type {
                    Some((peer_idx, target_ty))
                        if peer_idx == idx && is_string_literal_expr(arg) =>
                    {
                        coerce_bound_expr(bound, arg_types[idx], target_ty)
                    }
                    _ => bound,
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn infer_arg_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
}

fn scalar_function_may_return_geometry(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::GeoPoint
            | BuiltinScalarFunction::GeoBox
            | BuiltinScalarFunction::GeoLine
            | BuiltinScalarFunction::GeoLseg
            | BuiltinScalarFunction::GeoPath
            | BuiltinScalarFunction::GeoPolygon
            | BuiltinScalarFunction::GeoCircle
            | BuiltinScalarFunction::GeoCenter
            | BuiltinScalarFunction::GeoPolyCenter
            | BuiltinScalarFunction::GeoBoundBox
            | BuiltinScalarFunction::GeoDiagonal
            | BuiltinScalarFunction::GeoClosestPoint
            | BuiltinScalarFunction::GeoIntersection
            | BuiltinScalarFunction::GeoAdd
            | BuiltinScalarFunction::GeoSub
            | BuiltinScalarFunction::GeoMul
            | BuiltinScalarFunction::GeoDiv
            | BuiltinScalarFunction::GeoBoxHigh
            | BuiltinScalarFunction::GeoBoxLow
    )
}

fn expr_may_resolve_to_geometry(
    expr: &SqlExpr,
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> bool {
    match expr {
        SqlExpr::Const(
            Value::Point(_)
            | Value::Lseg(_)
            | Value::Path(_)
            | Value::Line(_)
            | Value::Box(_)
            | Value::Polygon(_)
            | Value::Circle(_),
        ) => true,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _)) => true,
        SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random => false,
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                Ok(ResolvedColumn::Local(index)) => scope
                    .desc
                    .columns
                    .get(index)
                    .is_some_and(|column| is_geometry_type(column.sql_type)),
                Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                    .get(depth)
                    .and_then(|scope| scope.desc.columns.get(index))
                    .is_some_and(|column| is_geometry_type(column.sql_type)),
                Err(_) => true,
            }
        }
        SqlExpr::Cast(_, ty) => is_geometry_type(raw_type_name_hint(ty)),
        SqlExpr::FuncCall { name, .. } => resolve_scalar_function(name)
            .map(scalar_function_may_return_geometry)
            .unwrap_or(true),
        SqlExpr::GeometryUnaryOp { .. } | SqlExpr::GeometryBinaryOp { .. } => true,
        SqlExpr::UnaryPlus(inner) | SqlExpr::Negate(inner) | SqlExpr::BitNot(inner) => {
            expr_may_resolve_to_geometry(inner, scope, outer_scopes, grouped_outer)
        }
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right) => {
            expr_may_resolve_to_geometry(left, scope, outer_scopes, grouped_outer)
                || expr_may_resolve_to_geometry(right, scope, outer_scopes, grouped_outer)
        }
        _ => false,
    }
}

fn geometry_arithmetic_result_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    match (left.element_type().kind, right.element_type().kind) {
        (SqlTypeKind::Point, SqlTypeKind::Point) => Some(SqlType::new(SqlTypeKind::Point)),
        (SqlTypeKind::Box, SqlTypeKind::Point) => Some(SqlType::new(SqlTypeKind::Box)),
        (SqlTypeKind::Path, SqlTypeKind::Path) => Some(SqlType::new(SqlTypeKind::Path)),
        (SqlTypeKind::Path, SqlTypeKind::Point) => Some(SqlType::new(SqlTypeKind::Path)),
        (SqlTypeKind::Circle, SqlTypeKind::Point) => Some(SqlType::new(SqlTypeKind::Circle)),
        _ => None,
    }
}

fn geometry_intersection_result_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    match (left.element_type().kind, right.element_type().kind) {
        (SqlTypeKind::Line, SqlTypeKind::Line) | (SqlTypeKind::Lseg, SqlTypeKind::Lseg) => {
            Some(SqlType::new(SqlTypeKind::Point))
        }
        (SqlTypeKind::Box, SqlTypeKind::Box) => Some(SqlType::new(SqlTypeKind::Box)),
        _ => None,
    }
}

pub(super) fn bind_maybe_geometry_arithmetic(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if !is_geometry_type(left_type) && !is_geometry_type(right_type) {
        return None;
    }
    let func = match op {
        "+" => BuiltinScalarFunction::GeoAdd,
        "-" => BuiltinScalarFunction::GeoSub,
        "*" => BuiltinScalarFunction::GeoMul,
        "/" => BuiltinScalarFunction::GeoDiv,
        "#" => BuiltinScalarFunction::GeoIntersection,
        _ => return None,
    };
    if op == "#" && geometry_intersection_result_type(left_type, right_type).is_none() {
        return Some(Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        }));
    }
    Some(bind_geometry_call(
        func,
        &[left, right],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_geometry_comparison(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if !is_geometry_type(left_type) && !is_geometry_type(right_type) {
        return None;
    }
    let func = match op {
        "=" => BuiltinScalarFunction::GeoEq,
        "<>" => BuiltinScalarFunction::GeoNe,
        "<" => BuiltinScalarFunction::GeoLt,
        "<=" => BuiltinScalarFunction::GeoLe,
        ">" => BuiltinScalarFunction::GeoGt,
        ">=" => BuiltinScalarFunction::GeoGe,
        "&&" => BuiltinScalarFunction::GeoOverlap,
        "@>" => BuiltinScalarFunction::GeoContains,
        "<@" => BuiltinScalarFunction::GeoContainedBy,
        _ => return None,
    };
    Some(bind_geometry_call(
        func,
        &[left, right],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_geometry_shift(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if !is_geometry_type(left_type) && !is_geometry_type(right_type) {
        return None;
    }
    let func = match op {
        "<<" => BuiltinScalarFunction::GeoLeft,
        ">>" => BuiltinScalarFunction::GeoRight,
        _ => return None,
    };
    Some(bind_geometry_call(
        func,
        &[left, right],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_geometry_unary_expr(
    op: GeometryUnaryOp,
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let func = match op {
        GeometryUnaryOp::Center => BuiltinScalarFunction::GeoCenter,
        GeometryUnaryOp::Length => BuiltinScalarFunction::GeoLength,
        GeometryUnaryOp::Npoints => BuiltinScalarFunction::GeoNpoints,
        GeometryUnaryOp::IsVertical => BuiltinScalarFunction::GeoIsVertical,
        GeometryUnaryOp::IsHorizontal => BuiltinScalarFunction::GeoIsHorizontal,
    };
    bind_geometry_call(
        func,
        &[expr],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_geometry_binary_expr(
    op: GeometryBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if matches!(op, GeometryBinaryOp::Distance) {
        let raw_left_type =
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
        let raw_right_type =
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
        let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
        let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
        if matches!(left_type.kind, SqlTypeKind::TsQuery) && is_string_literal_expr(right) {
            right_type = SqlType::new(SqlTypeKind::TsQuery);
        } else if matches!(right_type.kind, SqlTypeKind::TsQuery) && is_string_literal_expr(left) {
            left_type = SqlType::new(SqlTypeKind::TsQuery);
        }
        if matches!(left_type.kind, SqlTypeKind::TsQuery)
            && matches!(right_type.kind, SqlTypeKind::TsQuery)
        {
            let left = bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let right = bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            return Ok(Expr::builtin_func(
                BuiltinScalarFunction::TsQueryPhrase,
                Some(SqlType::new(SqlTypeKind::TsQuery)),
                false,
                vec![
                    coerce_bound_expr(left, raw_left_type, SqlType::new(SqlTypeKind::TsQuery)),
                    coerce_bound_expr(right, raw_right_type, SqlType::new(SqlTypeKind::TsQuery)),
                ],
            ));
        }
    }
    let func = match op {
        GeometryBinaryOp::Same => BuiltinScalarFunction::GeoSame,
        GeometryBinaryOp::Distance => BuiltinScalarFunction::GeoDistance,
        GeometryBinaryOp::ClosestPoint => BuiltinScalarFunction::GeoClosestPoint,
        GeometryBinaryOp::Intersects => BuiltinScalarFunction::GeoIntersects,
        GeometryBinaryOp::Parallel => BuiltinScalarFunction::GeoParallel,
        GeometryBinaryOp::Perpendicular => BuiltinScalarFunction::GeoPerpendicular,
        GeometryBinaryOp::IsVertical => BuiltinScalarFunction::GeoIsVertical,
        GeometryBinaryOp::IsHorizontal => BuiltinScalarFunction::GeoIsHorizontal,
        GeometryBinaryOp::OverLeft => BuiltinScalarFunction::GeoOverLeft,
        GeometryBinaryOp::OverRight => BuiltinScalarFunction::GeoOverRight,
        GeometryBinaryOp::Below => BuiltinScalarFunction::GeoBelow,
        GeometryBinaryOp::Above => BuiltinScalarFunction::GeoAbove,
        GeometryBinaryOp::OverBelow => BuiltinScalarFunction::GeoOverBelow,
        GeometryBinaryOp::OverAbove => BuiltinScalarFunction::GeoOverAbove,
    };
    bind_geometry_call(
        func,
        &[left, right],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_geometry_subscript(
    expr: &SqlExpr,
    index: i32,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let ty = infer_arg_type(expr, scope, catalog, outer_scopes, grouped_outer, ctes);
    if ty.is_array || !(0..=1).contains(&index) {
        return Err(ParseError::UndefinedOperator {
            op: "[]".into(),
            left_type: sql_type_name(ty),
            right_type: "integer".into(),
        });
    }
    let func = match ty.element_type().kind {
        SqlTypeKind::Box if index == 0 => BuiltinScalarFunction::GeoBoxHigh,
        SqlTypeKind::Box => BuiltinScalarFunction::GeoBoxLow,
        SqlTypeKind::Point if index == 0 => BuiltinScalarFunction::GeoPointX,
        SqlTypeKind::Point => BuiltinScalarFunction::GeoPointY,
        _ => {
            return Err(ParseError::UndefinedOperator {
                op: "[]".into(),
                left_type: sql_type_name(ty),
                right_type: "integer".into(),
            });
        }
    };
    bind_geometry_call(
        func,
        &[expr],
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn infer_geometry_special_expr_type_with_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<SqlType> {
    match expr {
        SqlExpr::Subscript { expr, index } => {
            let ty = infer_arg_type(expr, scope, catalog, outer_scopes, grouped_outer, ctes);
            if ty.is_array || !(0..=1).contains(index) {
                return None;
            }
            match ty.element_type().kind {
                SqlTypeKind::Box => Some(SqlType::new(SqlTypeKind::Point)),
                SqlTypeKind::Point => Some(SqlType::new(SqlTypeKind::Float8)),
                _ => None,
            }
        }
        SqlExpr::GeometryUnaryOp { op, .. } => Some(match op {
            GeometryUnaryOp::Center => SqlType::new(SqlTypeKind::Point),
            GeometryUnaryOp::Length => SqlType::new(SqlTypeKind::Float8),
            GeometryUnaryOp::Npoints => SqlType::new(SqlTypeKind::Int4),
            GeometryUnaryOp::IsVertical | GeometryUnaryOp::IsHorizontal => {
                SqlType::new(SqlTypeKind::Bool)
            }
        }),
        SqlExpr::GeometryBinaryOp { op, .. } => Some(match op {
            GeometryBinaryOp::Distance => {
                if let SqlExpr::GeometryBinaryOp { left, right, .. } = expr {
                    let left_type =
                        infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
                    let right_type =
                        infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
                    if (matches!(left_type.kind, SqlTypeKind::TsQuery)
                        && (matches!(right_type.kind, SqlTypeKind::TsQuery)
                            || is_string_literal_expr(right)))
                        || (matches!(right_type.kind, SqlTypeKind::TsQuery)
                            && is_string_literal_expr(left))
                    {
                        SqlType::new(SqlTypeKind::TsQuery)
                    } else {
                        SqlType::new(SqlTypeKind::Float8)
                    }
                } else {
                    SqlType::new(SqlTypeKind::Float8)
                }
            }
            GeometryBinaryOp::ClosestPoint => SqlType::new(SqlTypeKind::Point),
            GeometryBinaryOp::Same
            | GeometryBinaryOp::Intersects
            | GeometryBinaryOp::Parallel
            | GeometryBinaryOp::Perpendicular
            | GeometryBinaryOp::IsVertical
            | GeometryBinaryOp::IsHorizontal
            | GeometryBinaryOp::OverLeft
            | GeometryBinaryOp::OverRight
            | GeometryBinaryOp::Below
            | GeometryBinaryOp::Above
            | GeometryBinaryOp::OverBelow
            | GeometryBinaryOp::OverAbove => SqlType::new(SqlTypeKind::Bool),
        }),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right) => {
            if !expr_may_resolve_to_geometry(left, scope, outer_scopes, grouped_outer)
                && !expr_may_resolve_to_geometry(right, scope, outer_scopes, grouped_outer)
            {
                return None;
            }
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if is_geometry_type(left_type) || is_geometry_type(right_type) {
                geometry_arithmetic_result_type(left_type, right_type)
            } else {
                None
            }
        }
        SqlExpr::BitXor(left, right) => {
            if !expr_may_resolve_to_geometry(left, scope, outer_scopes, grouped_outer)
                && !expr_may_resolve_to_geometry(right, scope, outer_scopes, grouped_outer)
            {
                return None;
            }
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if is_geometry_type(left_type) || is_geometry_type(right_type) {
                geometry_intersection_result_type(left_type, right_type)
            } else {
                None
            }
        }
        SqlExpr::Shl(left, right) | SqlExpr::Shr(left, right) => {
            if !expr_may_resolve_to_geometry(left, scope, outer_scopes, grouped_outer)
                && !expr_may_resolve_to_geometry(right, scope, outer_scopes, grouped_outer)
            {
                return None;
            }
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if is_geometry_type(left_type) || is_geometry_type(right_type) {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right) => {
            if !expr_may_resolve_to_geometry(left, scope, outer_scopes, grouped_outer)
                && !expr_may_resolve_to_geometry(right, scope, outer_scopes, grouped_outer)
            {
                return None;
            }
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if is_geometry_type(left_type) || is_geometry_type(right_type) {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(super) fn infer_geometry_function_return_type_with_ctes(
    func: BuiltinScalarFunction,
    args: &SqlCallArgs,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<SqlType> {
    let arg_types = function_arg_values(args)
        .map(|arg| infer_arg_type(arg, scope, catalog, outer_scopes, grouped_outer, ctes))
        .collect::<Vec<_>>();
    Some(match func {
        BuiltinScalarFunction::GeoPoint => SqlType::new(SqlTypeKind::Point),
        BuiltinScalarFunction::GeoBox => SqlType::new(SqlTypeKind::Box),
        BuiltinScalarFunction::GeoLine => SqlType::new(SqlTypeKind::Line),
        BuiltinScalarFunction::GeoLseg => SqlType::new(SqlTypeKind::Lseg),
        BuiltinScalarFunction::GeoPath => SqlType::new(SqlTypeKind::Path),
        BuiltinScalarFunction::GeoPolygon => SqlType::new(SqlTypeKind::Polygon),
        BuiltinScalarFunction::GeoCircle => SqlType::new(SqlTypeKind::Circle),
        BuiltinScalarFunction::GeoArea
        | BuiltinScalarFunction::GeoLength
        | BuiltinScalarFunction::GeoRadius
        | BuiltinScalarFunction::GeoDiameter
        | BuiltinScalarFunction::GeoSlope
        | BuiltinScalarFunction::GeoDistance
        | BuiltinScalarFunction::GeoHeight
        | BuiltinScalarFunction::GeoWidth
        | BuiltinScalarFunction::GeoPointX
        | BuiltinScalarFunction::GeoPointY => SqlType::new(SqlTypeKind::Float8),
        BuiltinScalarFunction::GeoCenter
        | BuiltinScalarFunction::GeoPolyCenter
        | BuiltinScalarFunction::GeoClosestPoint
        | BuiltinScalarFunction::GeoBoxHigh
        | BuiltinScalarFunction::GeoBoxLow => SqlType::new(SqlTypeKind::Point),
        BuiltinScalarFunction::GeoBoundBox => SqlType::new(SqlTypeKind::Box),
        BuiltinScalarFunction::GeoDiagonal => SqlType::new(SqlTypeKind::Lseg),
        BuiltinScalarFunction::GeoNpoints => SqlType::new(SqlTypeKind::Int4),
        BuiltinScalarFunction::GeoPclose | BuiltinScalarFunction::GeoPopen => {
            SqlType::new(SqlTypeKind::Path)
        }
        BuiltinScalarFunction::GeoIsOpen
        | BuiltinScalarFunction::GeoIsClosed
        | BuiltinScalarFunction::GeoIsVertical
        | BuiltinScalarFunction::GeoIsHorizontal
        | BuiltinScalarFunction::GeoEq
        | BuiltinScalarFunction::GeoNe
        | BuiltinScalarFunction::GeoLt
        | BuiltinScalarFunction::GeoLe
        | BuiltinScalarFunction::GeoGt
        | BuiltinScalarFunction::GeoGe
        | BuiltinScalarFunction::GeoSame
        | BuiltinScalarFunction::GeoIntersects
        | BuiltinScalarFunction::GeoParallel
        | BuiltinScalarFunction::GeoPerpendicular
        | BuiltinScalarFunction::GeoContains
        | BuiltinScalarFunction::GeoContainedBy
        | BuiltinScalarFunction::GeoOverlap
        | BuiltinScalarFunction::GeoLeft
        | BuiltinScalarFunction::GeoOverLeft
        | BuiltinScalarFunction::GeoRight
        | BuiltinScalarFunction::GeoOverRight
        | BuiltinScalarFunction::GeoBelow
        | BuiltinScalarFunction::GeoOverBelow
        | BuiltinScalarFunction::GeoAbove
        | BuiltinScalarFunction::GeoOverAbove => SqlType::new(SqlTypeKind::Bool),
        BuiltinScalarFunction::GeoIntersection => {
            let left = *arg_types
                .first()
                .unwrap_or(&SqlType::new(SqlTypeKind::Text));
            let right = *arg_types.get(1).unwrap_or(&SqlType::new(SqlTypeKind::Text));
            geometry_intersection_result_type(left, right)
                .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        BuiltinScalarFunction::GeoAdd
        | BuiltinScalarFunction::GeoSub
        | BuiltinScalarFunction::GeoMul
        | BuiltinScalarFunction::GeoDiv => {
            let left = *arg_types
                .first()
                .unwrap_or(&SqlType::new(SqlTypeKind::Text));
            let right = *arg_types.get(1).unwrap_or(&SqlType::new(SqlTypeKind::Text));
            geometry_arithmetic_result_type(left, right).unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        _ => return None,
    })
}
