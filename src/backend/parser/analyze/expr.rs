use super::functions::*;
use super::infer::*;
use super::*;
use crate::include::nodes::primnodes::{
    BoolExprType, CaseExpr as BoundCaseExpr, CaseTestExpr as BoundCaseTestExpr,
    CaseWhen as BoundCaseWhen, ExprArraySubscript, OpExprKind,
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
    bind_jsonb_path_binary_expr, bind_maybe_jsonb_delete,
};
pub(crate) use self::ops::bind_concat_operands;
pub(super) use self::ops::bind_lowered_comparison_expr;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_bound_comparison_expr, bind_comparison_expr,
    bind_concat_expr, bind_overloaded_binary_expr, bind_prefix_operator_expr, bind_shift_expr,
};
use self::subquery::{
    bind_exists_subquery_expr, bind_in_subquery_expr, bind_quantified_array_expr,
    bind_quantified_subquery_expr, bind_scalar_subquery_expr,
};
pub(crate) use self::targets::{
    BoundSelectTargets, bind_select_targets, select_targets_contain_set_returning_call,
};

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

pub(super) fn raise_expr_varlevels(expr: Expr, levels: usize) -> Expr {
    if levels == 0 {
        return expr;
    }
    match expr {
        Expr::Var(mut var) => {
            var.varlevelsup += levels;
            Expr::Var(var)
        }
        Expr::Aggref(mut aggref) => {
            aggref.agglevelsup += levels;
            Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
                args: aggref
                    .args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
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
        Expr::Bool(bool_expr) => {
            Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
                args: bool_expr
                    .args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
                ..*bool_expr
            }))
        }
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
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(raise_expr_varlevels(*inner, levels)), ty),
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
        } => Expr::Like {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            negated,
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
        Expr::Row { fields } => Expr::Row {
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, raise_expr_varlevels(expr, levels)))
                .collect(),
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

pub(crate) fn bind_expr_with_outer_and_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => {
            if let Some(relation_name) = name.strip_suffix(".*") {
                let fields =
                    resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                        .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
                Expr::Row { fields }
            } else if let Some(system_column) =
                resolve_system_column_with_outer(scope, outer_scopes, name)?
            {
                Expr::Var(crate::include::nodes::primnodes::Var {
                    varno: system_column.varno,
                    varattno: crate::include::nodes::primnodes::TABLE_OID_ATTR_NO,
                    varlevelsup: system_column.varlevelsup,
                    vartype: system_column.sql_type,
                })
            } else {
                match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer)? {
                    ResolvedColumn::Local(index) => scope.output_exprs.get(index).cloned().unwrap_or_else(|| {
                        panic!("bound scope output_exprs missing local column {index} for {name}")
                    }),
                    ResolvedColumn::Outer { depth, index } => outer_scopes
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
            let mut field_exprs = Vec::new();
            for item in items {
                if let SqlExpr::Column(name) = item
                    && let Some(relation_name) = name.strip_suffix(".*")
                {
                    let fields =
                        resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                            .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
                    field_exprs.extend(fields.into_iter().map(|(_, expr)| expr));
                    continue;
                }
                field_exprs.push(bind_expr_with_outer_and_ctes(
                    item,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?);
            }
            Expr::Row {
                fields: field_exprs
                    .into_iter()
                    .enumerate()
                    .map(|(index, expr)| (format!("f{}", index + 1), expr))
                    .collect(),
            }
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
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "bound builtin operator",
                    actual: format!("unsupported operator {op}"),
                });
            }
        },
        SqlExpr::Add(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
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
        SqlExpr::BitAnd(left, right) => bind_bitwise_expr(
            "&",
            OpExprKind::BitAnd,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitOr(left, right) => bind_bitwise_expr(
            "|",
            OpExprKind::BitOr,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
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
            if let Some(result) = bind_maybe_geometry_shift(
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
            if let Some(result) = bind_maybe_geometry_shift(
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
            if let Some(result) = bind_maybe_geometry_arithmetic(
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
            if !is_integer_family(inner_type) && !is_bit_string_type(inner_type) {
                return Err(ParseError::UndefinedOperator {
                    op: "~",
                    left_type: sql_type_name(inner_type),
                    right_type: "unknown".to_string(),
                });
            }
            Expr::op_auto(
                OpExprKind::BitNot,
                vec![bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?],
            )
        }
        SqlExpr::Cast(inner, ty) => {
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
                    array_type: raw_type_name_hint(ty),
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
            let target_type = resolve_raw_type_name(ty, catalog)?;
            if !matches!(inner.as_ref(), SqlExpr::Const(Value::Null)) {
                validate_catalog_backed_explicit_cast(source_type, target_type, catalog)?;
            }
            Expr::Cast(Box::new(bound_inner), target_type)
        }
        SqlExpr::Eq(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
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
            if let Some(result) = bind_maybe_geometry_comparison(
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
            if let Some(result) = bind_maybe_geometry_comparison(
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
            if let Some(result) = bind_maybe_geometry_comparison(
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
            if let Some(result) = bind_maybe_geometry_comparison(
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
            if let Some(result) = bind_maybe_geometry_comparison(
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
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
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
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
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
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
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
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
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
        ),
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
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "ARRAY[...] with a typed element or explicit cast",
                actual: "ARRAY[]".into(),
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
            if !array_type.is_array {
                return Err(ParseError::UnexpectedToken {
                    expected: "array expression",
                    actual: sql_type_name(array_type).into(),
                });
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
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            });
        }
        SqlExpr::ScalarSubquery(select) => {
            bind_scalar_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
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
        SqlExpr::JsonbContains(left, right) => bind_jsonb_contains_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbContained(left, right) => bind_jsonb_contained_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
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
            func_variadic,
        } => {
            if name.eq_ignore_ascii_case("row_to_json") {
                return bind_row_to_json_call(
                    name,
                    args,
                    *func_variadic,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("coalesce") {
                return bind_coalesce_call(args, scope, catalog, outer_scopes, grouped_outer, ctes);
            }
            if let Some(target_type) = resolve_function_cast_type(catalog, name) {
                if args.iter().any(|arg| arg.name.is_some()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "positional cast argument",
                        actual: format!("{name} with named arguments"),
                    });
                }
                if args.len() != 1 {
                    return Err(ParseError::UnexpectedToken {
                        expected: "single-argument cast function",
                        actual: format!("{name}({} args)", args.len()),
                    });
                }
                let arg_type = infer_sql_expr_type_with_ctes(
                    &args[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let bound_arg = bind_expr_with_outer_and_ctes(
                    &args[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                validate_catalog_backed_explicit_cast(arg_type, target_type, catalog)?;
                return Ok(Expr::Cast(
                    Box::new(bound_arg),
                    if arg_type == target_type {
                        arg_type
                    } else {
                        target_type
                    },
                ));
            }
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
            if let Ok(resolved) =
                resolve_function_call(catalog, name, &actual_types, *func_variadic)
            {
                if resolved.prokind != 'f' || resolved.proretset {
                    return Err(ParseError::UnexpectedToken {
                        expected: "supported scalar function",
                        actual: name.clone(),
                    });
                }
                if let Some(func) = resolved.scalar_impl {
                    let lowered_args = lower_named_scalar_function_args(func, args)?;
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
                    resolved.result_type,
                    &resolved.declared_arg_types,
                    args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            let legacy_func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            let lowered_args = lower_named_scalar_function_args(legacy_func, args)?;
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
            bind_scalar_function_call(
                legacy_func,
                0,
                None,
                false,
                0,
                0,
                &actual_types,
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
        SqlExpr::GeometryBinaryOp { op, left, right } => bind_geometry_binary_expr(
            *op,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
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
        SqlExpr::FieldSelect { field, .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "scalar expression",
                actual: format!("field selection .{field} is not bound yet"),
            });
        }
        SqlExpr::CurrentDate => Expr::CurrentDate,
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
    let mut result_exprs = Vec::with_capacity(args.len() + 1);
    result_exprs.push(default_expr.clone());
    result_exprs.extend(args.iter().map(|arm| arm.result.clone()));
    let result_type = infer_common_scalar_expr_type_with_ctes(
        &result_exprs,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "CASE result expressions with a common type",
    )?;

    let (bound_arg, arg_type) = if let Some(arg) = arg {
        (
            Some(bind_expr_with_outer_and_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Some(infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )),
        )
    } else {
        (None, None)
    };

    let mut bound_arms = Vec::with_capacity(args.len());
    for arm in args {
        let condition = if let Some(arg_type) = arg_type {
            bind_bound_comparison_expr(
                "=",
                OpExprKind::Eq,
                Expr::CaseTest(Box::new(BoundCaseTestExpr { type_id: arg_type })),
                arg_type,
                arg_type,
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        } else {
            let expr_type = infer_sql_expr_type_with_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if expr_type != SqlType::new(SqlTypeKind::Bool) {
                return Err(ParseError::UnexpectedToken {
                    expected: "boolean CASE condition",
                    actual: "CASE WHEN expression must return boolean".into(),
                });
            }
            bind_expr_with_outer_and_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        };
        let raw_result_type = infer_sql_expr_type_with_ctes(
            &arm.result,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        let bound_result = bind_expr_with_outer_and_ctes(
            &arm.result,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        bound_arms.push(BoundCaseWhen {
            expr: condition,
            result: coerce_bound_expr(bound_result, raw_result_type, result_type),
        });
    }

    let raw_default_type = infer_sql_expr_type_with_ctes(
        default_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let bound_default = bind_expr_with_outer_and_ctes(
        default_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;

    Ok(Expr::Case(Box::new(BoundCaseExpr {
        casetype: result_type,
        arg: bound_arg.map(Box::new),
        args: bound_arms,
        defresult: Box::new(coerce_bound_expr(
            bound_default,
            raw_default_type,
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
    let lowered_args = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let common_type = infer_common_scalar_expr_type_with_ctes(
        &lowered_args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "COALESCE arguments with a common type",
    )?;
    let mut bound_args = Vec::with_capacity(lowered_args.len());
    for arg in &lowered_args {
        let arg_type =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        let bound =
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        bound_args.push(coerce_bound_expr(bound, arg_type, common_type));
    }
    let mut iter = bound_args.into_iter().rev();
    let mut expr = iter.next().expect("coalesce arity validated");
    for arg in iter {
        expr = Expr::Coalesce(Box::new(arg), Box::new(expr));
    }
    Ok(expr)
}

fn validate_catalog_backed_explicit_cast(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if source_type.element_type() == target_type.element_type() {
        return Ok(());
    }
    if source_type.is_array || !is_text_like_type(source_type) {
        return Ok(());
    }
    if target_type.is_array {
        return Ok(());
    }
    if explicit_text_input_cast_exists(catalog, target_type) {
        return Ok(());
    }
    Err(ParseError::UnexpectedToken {
        expected: "supported explicit cast",
        actual: format!(
            "cannot cast type {} to {}",
            sql_type_name(source_type),
            sql_type_name(target_type)
        ),
    })
}
