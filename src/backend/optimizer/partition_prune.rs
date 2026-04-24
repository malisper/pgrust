use std::cmp::Ordering;

use crate::backend::executor::compare_order_values;
use crate::backend::parser::{
    CatalogLookup, PartitionBoundSpec, PartitionRangeDatumValue, PartitionStrategy,
    SerializedPartitionValue, deserialize_partition_bound, partition_value_to_value,
    relation_partition_spec,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, OpExprKind, ScalarFunctionImpl, Var,
};

pub(super) fn partition_may_satisfy_filter(
    catalog: &dyn CatalogLookup,
    parent_oid: u32,
    child_oid: u32,
    filter: Option<&Expr>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let (Some(parent), Some(child)) = (
        catalog.relation_by_oid(parent_oid),
        catalog.relation_by_oid(child_oid),
    ) else {
        return true;
    };
    let Ok(spec) = relation_partition_spec(&parent) else {
        return true;
    };
    if spec.partattrs.len() != 1 {
        return true;
    }
    let Some(bound) = child
        .relpartbound
        .as_deref()
        .and_then(|text| deserialize_partition_bound(text).ok())
    else {
        return true;
    };
    let sibling_bounds = catalog
        .inheritance_children(parent_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
        .filter_map(|rel| {
            rel.relpartbound
                .and_then(|text| deserialize_partition_bound(&text).ok())
        })
        .collect::<Vec<_>>();
    let key_attno = i32::from(spec.partattrs[0]);
    expr_may_match_bound(filter, key_attno, &spec.strategy, &bound, &sibling_bounds)
}

fn expr_may_match_bound(
    expr: &Expr,
    key_attno: i32,
    strategy: &PartitionStrategy,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
) -> bool {
    match expr {
        Expr::Bool(bool_expr) => {
            match bool_expr.boolop {
                BoolExprType::And => bool_expr.args.iter().all(|arg| {
                    expr_may_match_bound(arg, key_attno, strategy, bound, sibling_bounds)
                }),
                BoolExprType::Or => bool_expr.args.iter().any(|arg| {
                    expr_may_match_bound(arg, key_attno, strategy, bound, sibling_bounds)
                }),
                BoolExprType::Not => true,
            }
        }
        Expr::IsNull(inner) => key_expr_var(inner, key_attno)
            .map(|_| bound_may_contain_value(strategy, bound, sibling_bounds, &Value::Null))
            .unwrap_or(true),
        Expr::IsNotNull(inner) => key_expr_var(inner, key_attno)
            .map(|_| bound_may_contain_non_null(strategy, bound))
            .unwrap_or(true),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return true;
            };
            let Some((key_on_left, value)) = partition_key_const_cmp(left, right, key_attno) else {
                return true;
            };
            bound_may_satisfy_comparison(
                strategy,
                bound,
                sibling_bounds,
                key_on_left,
                op.op,
                &value,
                op.collation_oid,
            )
        }
        _ => true,
    }
}

fn partition_key_const_cmp(left: &Expr, right: &Expr, key_attno: i32) -> Option<(bool, Value)> {
    if key_expr_var(left, key_attno).is_some()
        && let Some(value) = const_value(right)
    {
        return Some((true, value));
    }
    if key_expr_var(right, key_attno).is_some()
        && let Some(value) = const_value(left)
    {
        return Some((false, value));
    }
    None
}

fn key_expr_var(expr: &Expr, key_attno: i32) -> Option<&Var> {
    match expr {
        Expr::Var(var) if var.varattno == key_attno && var.varlevelsup == 0 => Some(var),
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            key_expr_var(&func.args[0], key_attno)
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => key_expr_var(inner, key_attno),
        _ => None,
    }
}

fn const_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Const(value) => Some(value.clone()),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => const_value(inner),
        _ => None,
    }
}

fn bound_may_satisfy_comparison(
    strategy: &PartitionStrategy,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    key_on_left: bool,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    let op = if key_on_left { op } else { commute_op(op) };
    match op {
        OpExprKind::Eq => bound_may_contain_value(strategy, bound, sibling_bounds, value),
        OpExprKind::NotEq => {
            bound_may_contain_non_equal_value(strategy, bound, value, collation_oid)
        }
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            bound_may_overlap_inequality(strategy, bound, op, value, collation_oid)
        }
        _ => true,
    }
}

fn commute_op(op: OpExprKind) -> OpExprKind {
    match op {
        OpExprKind::Lt => OpExprKind::Gt,
        OpExprKind::LtEq => OpExprKind::GtEq,
        OpExprKind::Gt => OpExprKind::Lt,
        OpExprKind::GtEq => OpExprKind::LtEq,
        other => other,
    }
}

fn bound_may_contain_value(
    strategy: &PartitionStrategy,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    value: &Value,
) -> bool {
    match (strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if *is_default {
                !sibling_bounds.iter().any(|sibling| {
                    matches!(sibling, PartitionBoundSpec::List { values, is_default: false } if values.iter().any(|item| serialized_value_eq(item, value)))
                })
            } else {
                values.iter().any(|item| serialized_value_eq(item, value))
            }
        }
        (
            PartitionStrategy::Range,
            PartitionBoundSpec::Range {
                from,
                to,
                is_default,
            },
        ) => *is_default || range_contains_value(from, to, value),
        _ => true,
    }
}

fn bound_may_contain_non_null(strategy: &PartitionStrategy, bound: &PartitionBoundSpec) -> bool {
    match (strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            *is_default
                || values
                    .iter()
                    .any(|value| !matches!(value, SerializedPartitionValue::Null))
        }
        (PartitionStrategy::Range, PartitionBoundSpec::Range { is_default, .. }) => *is_default,
        _ => true,
    }
}

fn bound_may_contain_non_equal_value(
    strategy: &PartitionStrategy,
    bound: &PartitionBoundSpec,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            *is_default
                || values.iter().any(|item| {
                    compare_partition_value(item, value, collation_oid) != Some(Ordering::Equal)
                })
        }
        _ => true,
    }
}

fn bound_may_overlap_inequality(
    strategy: &PartitionStrategy,
    bound: &PartitionBoundSpec,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            *is_default
                || values.iter().any(|item| {
                    compare_partition_value(item, value, collation_oid)
                        .map(|cmp| cmp_satisfies(cmp, op))
                        .unwrap_or(true)
                })
        }
        (
            PartitionStrategy::Range,
            PartitionBoundSpec::Range {
                from,
                to,
                is_default,
            },
        ) => *is_default || range_may_overlap_inequality(from, to, op, value),
        _ => true,
    }
}

fn serialized_value_eq(value: &SerializedPartitionValue, other: &Value) -> bool {
    partition_value_to_value(value) == *other
}

fn compare_partition_value(
    value: &SerializedPartitionValue,
    other: &Value,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    compare_order_values(
        &partition_value_to_value(value),
        other,
        collation_oid,
        None,
        false,
    )
    .ok()
}

fn cmp_satisfies(cmp: Ordering, op: OpExprKind) -> bool {
    match op {
        OpExprKind::Lt => cmp == Ordering::Less,
        OpExprKind::LtEq => cmp != Ordering::Greater,
        OpExprKind::Gt => cmp == Ordering::Greater,
        OpExprKind::GtEq => cmp != Ordering::Less,
        _ => true,
    }
}

fn range_contains_value(
    from: &[PartitionRangeDatumValue],
    to: &[PartitionRangeDatumValue],
    value: &Value,
) -> bool {
    !matches!(value, Value::Null)
        && compare_value_to_range_bound(value, from) != Some(Ordering::Less)
        && compare_value_to_range_bound(value, to) == Some(Ordering::Less)
}

fn range_may_overlap_inequality(
    from: &[PartitionRangeDatumValue],
    to: &[PartitionRangeDatumValue],
    op: OpExprKind,
    value: &Value,
) -> bool {
    if matches!(value, Value::Null) {
        return false;
    }
    match op {
        OpExprKind::Lt => compare_value_to_range_bound(value, from) != Some(Ordering::Less),
        OpExprKind::LtEq => compare_value_to_range_bound(value, from) != Some(Ordering::Less),
        OpExprKind::Gt => compare_value_to_range_bound(value, to) != Some(Ordering::Greater),
        OpExprKind::GtEq => compare_value_to_range_bound(value, to) != Some(Ordering::Greater),
        _ => true,
    }
}

fn compare_value_to_range_bound(
    value: &Value,
    bound: &[PartitionRangeDatumValue],
) -> Option<Ordering> {
    let Some(bound) = bound.first() else {
        return Some(Ordering::Equal);
    };
    match bound {
        PartitionRangeDatumValue::MinValue => Some(Ordering::Greater),
        PartitionRangeDatumValue::MaxValue => Some(Ordering::Less),
        PartitionRangeDatumValue::Value(bound_value) => compare_order_values(
            value,
            &partition_value_to_value(bound_value),
            None,
            None,
            false,
        )
        .ok(),
    }
}
