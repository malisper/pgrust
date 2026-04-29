use std::cmp::Ordering;

use crate::backend::executor::compare_order_values;
use crate::backend::parser::{
    CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec, PartitionRangeDatumValue,
    PartitionStrategy, SerializedPartitionValue, SqlType, SqlTypeKind, SubqueryComparisonOp,
    deserialize_partition_bound, partition_value_to_value, relation_partition_spec,
};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{ANYOID, PG_LANGUAGE_SQL_OID, builtin_scalar_function_for_proc_oid};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, OpExprKind, RelationDesc, ScalarFunctionImpl,
    attrno_index, expr_sql_type_hint, user_attrno,
};

pub(super) fn partition_may_satisfy_filter(
    spec: Option<&LoweredPartitionSpec>,
    bound: Option<&PartitionBoundSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    parent_context: Option<&ParentPartitionPruningContext>,
    filter: Option<&Expr>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Some(spec) = spec else {
        return true;
    };
    let Some(bound) = bound else {
        return true;
    };
    expr_may_match_bound(filter, spec, bound, sibling_bounds, parent_context, catalog)
}

#[derive(Clone, Debug)]
pub(super) struct ParentPartitionPruningContext {
    spec: LoweredPartitionSpec,
    bound: PartitionBoundSpec,
    sibling_bounds: Vec<PartitionBoundSpec>,
}

pub(crate) fn relation_may_satisfy_own_partition_bound(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    filter: Option<&Expr>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return true;
    };
    let Some(bound) = relation
        .relpartbound
        .as_deref()
        .and_then(|text| deserialize_partition_bound(text).ok())
    else {
        return true;
    };
    catalog
        .inheritance_parents(relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .all(|row| {
            let Some(parent) = catalog.relation_by_oid(row.inhparent) else {
                return true;
            };
            let Ok(spec) = relation_partition_spec(&parent) else {
                return true;
            };
            let Some(spec) =
                partition_spec_for_relation_filter(&spec, &parent.desc, &relation.desc)
            else {
                return true;
            };
            let sibling_bounds = catalog
                .inheritance_children(row.inhparent)
                .into_iter()
                .filter(|row| !row.inhdetachpending)
                .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
                .filter_map(|rel| {
                    rel.relpartbound
                        .and_then(|text| deserialize_partition_bound(text.as_str()).ok())
                })
                .collect::<Vec<_>>();
            expr_may_match_bound(filter, &spec, &bound, &sibling_bounds, None, Some(catalog))
        })
}

pub(super) fn relation_parent_pruning_context(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Option<ParentPartitionPruningContext> {
    let relation = catalog.relation_by_oid(relation_oid)?;
    let bound = relation
        .relpartbound
        .as_deref()
        .and_then(|text| deserialize_partition_bound(text).ok())?;
    let parents = catalog
        .inheritance_parents(relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .collect::<Vec<_>>();
    let [parent_row] = parents.as_slice() else {
        return None;
    };
    let parent = catalog.relation_by_oid(parent_row.inhparent)?;
    let Ok(parent_spec) = relation_partition_spec(&parent) else {
        return None;
    };
    let spec = partition_spec_for_relation_filter(&parent_spec, &parent.desc, &relation.desc)?;
    let sibling_bounds = catalog
        .inheritance_children(parent_row.inhparent)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
        .filter_map(|rel| {
            rel.relpartbound
                .and_then(|text| deserialize_partition_bound(text.as_str()).ok())
        })
        .collect();
    Some(ParentPartitionPruningContext {
        spec,
        bound,
        sibling_bounds,
    })
}

fn partition_spec_for_relation_filter(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    parent_desc: &RelationDesc,
    relation_desc: &RelationDesc,
) -> Option<crate::backend::parser::LoweredPartitionSpec> {
    let translated_key_exprs = spec
        .key_exprs
        .iter()
        .map(|key_expr| {
            translate_partition_key_expr_to_relation(key_expr, parent_desc, relation_desc)
        })
        .collect::<Option<Vec<_>>>()?;
    let translated_partattrs = translated_key_exprs
        .iter()
        .map(|key_expr| match key_expr {
            Expr::Var(var) => Some(var.varattno as i16),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    let mut translated = spec.clone();
    translated.key_exprs = translated_key_exprs;
    translated.partattrs = translated_partattrs;
    Some(translated)
}

fn translate_partition_key_expr_to_relation(
    key_expr: &Expr,
    parent_desc: &RelationDesc,
    relation_desc: &RelationDesc,
) -> Option<Expr> {
    let Expr::Var(var) = key_expr else {
        return None;
    };
    if var.varlevelsup != 0 {
        return None;
    }
    let parent_index = attrno_index(var.varattno)?;
    let parent_column = parent_desc.columns.get(parent_index)?;
    if parent_column.dropped {
        return None;
    }
    let relation_index = relation_desc.columns.iter().position(|column| {
        !column.dropped
            && column.name == parent_column.name
            && column.sql_type == parent_column.sql_type
    })?;
    Some(Expr::Var(crate::include::nodes::primnodes::Var {
        varno: var.varno,
        varattno: user_attrno(relation_index),
        varlevelsup: var.varlevelsup,
        vartype: parent_column.sql_type.clone(),
    }))
}

fn partition_specs_match_for_bound_context(
    parent_spec: &LoweredPartitionSpec,
    child_spec: &LoweredPartitionSpec,
) -> bool {
    parent_spec.strategy == child_spec.strategy
        && parent_spec.key_exprs == child_spec.key_exprs
        && parent_spec.key_types == child_spec.key_types
        && parent_spec.partclass == child_spec.partclass
        && parent_spec.partcollation == child_spec.partcollation
}

fn matching_parent_bound<'a>(
    parent_context: Option<&'a ParentPartitionPruningContext>,
    child_spec: &LoweredPartitionSpec,
) -> Option<&'a PartitionBoundSpec> {
    let context = parent_context?;
    partition_specs_match_for_bound_context(&context.spec, child_spec).then_some(&context.bound)
}

fn parent_context_may_match_expr(
    context: &ParentPartitionPruningContext,
    expr: &Expr,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    expr_may_match_bound_inner(
        expr,
        &context.spec,
        &context.bound,
        &context.sibling_bounds,
        None,
        catalog,
        false,
    )
}

fn expr_may_match_bound(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_context: Option<&ParentPartitionPruningContext>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    expr_may_match_bound_inner(
        expr,
        spec,
        bound,
        sibling_bounds,
        parent_context,
        catalog,
        false,
    )
}

fn expr_may_match_bound_inner(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_context: Option<&ParentPartitionPruningContext>,
    catalog: Option<&dyn CatalogLookup>,
    in_or_arm: bool,
) -> bool {
    match expr {
        Expr::Const(Value::Bool(false)) | Expr::Const(Value::Null) => return false,
        Expr::Const(Value::Bool(true)) => return true,
        _ => {}
    }
    if let Some(result) =
        explicit_list_bound_may_match_expr(expr, spec, bound, parent_context, catalog)
    {
        return result;
    }
    if let Some((key_index, value)) = partition_key_bool_equality_predicate(expr, spec) {
        return bound_may_contain_value(
            spec,
            bound,
            sibling_bounds,
            matching_parent_bound(parent_context, spec),
            key_index,
            &Value::Bool(value),
            catalog,
        );
    }
    match expr {
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => {
                range_may_satisfy_conjunction(
                    expr,
                    spec,
                    bound,
                    sibling_bounds,
                    matching_parent_bound(parent_context, spec),
                    in_or_arm,
                )
                .unwrap_or(true)
                    && hash_may_satisfy_conjunction(expr, spec, bound, catalog).unwrap_or(true)
                    && bool_expr.args.iter().all(|arg| {
                        expr_may_match_bound_inner(
                            arg,
                            spec,
                            bound,
                            sibling_bounds,
                            parent_context,
                            catalog,
                            in_or_arm,
                        )
                    })
            }
            BoolExprType::Or => bool_expr.args.iter().any(|arg| {
                parent_context
                    .map(|context| parent_context_may_match_expr(context, arg, catalog))
                    .unwrap_or(true)
                    && expr_may_match_bound_inner(
                        arg,
                        spec,
                        bound,
                        sibling_bounds,
                        parent_context,
                        catalog,
                        true,
                    )
            }),
            BoolExprType::Not => true,
        },
        Expr::IsNull(inner) => partition_key_index(inner, spec)
            .map(|index| {
                bound_may_contain_value(
                    spec,
                    bound,
                    sibling_bounds,
                    matching_parent_bound(parent_context, spec),
                    index,
                    &Value::Null,
                    catalog,
                )
            })
            .unwrap_or(true),
        Expr::IsNotNull(inner) => partition_key_index(inner, spec)
            .map(|index| bound_may_contain_non_null(spec, bound, index))
            .unwrap_or(true),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return true;
            };
            let Some((key_index, key_on_left, value)) =
                partition_key_const_cmp(left, right, spec, op.collation_oid)
            else {
                return true;
            };
            bound_may_satisfy_comparison(
                spec,
                bound,
                sibling_bounds,
                matching_parent_bound(parent_context, spec),
                key_index,
                key_on_left,
                op.op,
                &value,
                op.collation_oid,
                catalog,
            )
        }
        Expr::IsDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, value)| {
                bound_may_satisfy_distinctness(
                    spec,
                    bound,
                    sibling_bounds,
                    matching_parent_bound(parent_context, spec),
                    key_index,
                    &value,
                    true,
                )
            })
            .unwrap_or(true),
        Expr::IsNotDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, value)| {
                bound_may_satisfy_distinctness(
                    spec,
                    bound,
                    sibling_bounds,
                    matching_parent_bound(parent_context, spec),
                    key_index,
                    &value,
                    false,
                )
            })
            .unwrap_or(true),
        Expr::ScalarArrayOp(saop) => {
            if scalar_array_right_is_null(&saop.right) {
                return false;
            }
            let Some((key_index, op, values)) = partition_key_const_array_cmp(
                &saop.left,
                &saop.right,
                spec,
                saop.op,
                saop.collation_oid,
            ) else {
                return true;
            };
            if saop.use_or {
                values.into_iter().any(|value| {
                    bound_may_satisfy_comparison(
                        spec,
                        bound,
                        sibling_bounds,
                        matching_parent_bound(parent_context, spec),
                        key_index,
                        true,
                        op,
                        &value,
                        saop.collation_oid,
                        catalog,
                    )
                })
            } else {
                values.into_iter().all(|value| {
                    bound_may_satisfy_comparison(
                        spec,
                        bound,
                        sibling_bounds,
                        matching_parent_bound(parent_context, spec),
                        key_index,
                        true,
                        op,
                        &value,
                        saop.collation_oid,
                        catalog,
                    )
                })
            }
        }
        _ => true,
    }
}

fn scalar_array_right_is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Const(Value::Null) => true,
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            scalar_array_right_is_null(inner)
        }
        _ => false,
    }
}

fn explicit_list_bound_may_match_expr(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    parent_context: Option<&ParentPartitionPruningContext>,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<bool> {
    let (
        PartitionStrategy::List,
        PartitionBoundSpec::List {
            values,
            is_default: false,
        },
    ) = (&spec.strategy, bound)
    else {
        return None;
    };
    Some(
        values
            .iter()
            .any(|value| list_value_may_match_expr(value, expr, spec, parent_context, catalog)),
    )
}

fn list_value_may_match_expr(
    value: &SerializedPartitionValue,
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    parent_context: Option<&ParentPartitionPruningContext>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    if let Some((key_index, required_value)) = partition_key_bool_equality_predicate(expr, spec)
        && key_index == 0
    {
        return list_value_satisfies_comparison(
            value,
            OpExprKind::Eq,
            &Value::Bool(required_value),
            None,
        );
    }
    match expr {
        Expr::Const(Value::Bool(false)) | Expr::Const(Value::Null) => false,
        Expr::Const(Value::Bool(true)) => true,
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => bool_expr
                .args
                .iter()
                .all(|arg| list_value_may_match_expr(value, arg, spec, parent_context, catalog)),
            BoolExprType::Or => bool_expr.args.iter().any(|arg| {
                parent_context
                    .map(|context| parent_context_may_match_expr(context, arg, catalog))
                    .unwrap_or(true)
                    && list_value_may_match_expr(value, arg, spec, parent_context, catalog)
            }),
            BoolExprType::Not => true,
        },
        Expr::IsNull(inner) => partition_key_index(inner, spec)
            .map(|index| index != 0 || serialized_partition_value_is_null(value))
            .unwrap_or(true),
        Expr::IsNotNull(inner) => partition_key_index(inner, spec)
            .map(|index| index != 0 || !serialized_partition_value_is_null(value))
            .unwrap_or(true),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return true;
            };
            let collation_oid = op.collation_oid;
            let Some((key_index, key_on_left, constant)) =
                partition_key_const_cmp(left, right, spec, collation_oid)
            else {
                return true;
            };
            if key_index != 0 {
                return true;
            }
            let op = if key_on_left {
                op.op
            } else {
                commute_op(op.op)
            };
            list_value_satisfies_comparison(value, op, &constant, collation_oid)
        }
        Expr::ScalarArrayOp(saop) => {
            let Some((key_index, op, values)) = partition_key_const_array_cmp(
                &saop.left,
                &saop.right,
                spec,
                saop.op,
                saop.collation_oid,
            ) else {
                return true;
            };
            if key_index != 0 {
                return true;
            }
            if saop.use_or {
                values.into_iter().any(|constant| {
                    list_value_satisfies_comparison(value, op, &constant, saop.collation_oid)
                })
            } else {
                values.into_iter().all(|constant| {
                    list_value_satisfies_comparison(value, op, &constant, saop.collation_oid)
                })
            }
        }
        Expr::IsDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, constant)| {
                key_index != 0 || list_value_satisfies_distinctness(value, &constant, true, None)
            })
            .unwrap_or(true),
        Expr::IsNotDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, constant)| {
                key_index != 0 || list_value_satisfies_distinctness(value, &constant, false, None)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn partition_key_const_cmp(
    left: &Expr,
    right: &Expr,
    spec: &LoweredPartitionSpec,
    query_collation_oid: Option<u32>,
) -> Option<(usize, bool, Value)> {
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        let key_collation_oid = spec.partcollation.get(index).copied().unwrap_or(0);
        if collation_mismatch(query_collation_oid, key_collation_oid) {
            continue;
        }
        if partition_key_expr_matches(left, key_expr).is_some()
            && let Some((value, value_type)) = const_value_with_type(right)
            && pruning_constant_type_compatible(spec, index, value_type)
        {
            return Some((index, true, value));
        }
        if partition_key_expr_matches(right, key_expr).is_some()
            && let Some((value, value_type)) = const_value_with_type(left)
            && pruning_constant_type_compatible(spec, index, value_type)
        {
            return Some((index, false, value));
        }
    }
    None
}

fn partition_key_const_distinct_cmp(
    left: &Expr,
    right: &Expr,
    spec: &LoweredPartitionSpec,
) -> Option<(usize, Value)> {
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        if partition_key_expr_matches(left, key_expr).is_some()
            && let Some((value, value_type)) = const_value_with_type(right)
            && pruning_constant_type_compatible(spec, index, value_type)
        {
            return Some((index, value));
        }
        if partition_key_expr_matches(right, key_expr).is_some()
            && let Some((value, value_type)) = const_value_with_type(left)
            && pruning_constant_type_compatible(spec, index, value_type)
        {
            return Some((index, value));
        }
    }
    None
}

fn partition_key_index(expr: &Expr, spec: &LoweredPartitionSpec) -> Option<usize> {
    spec.key_exprs
        .iter()
        .position(|key_expr| partition_key_expr_matches(expr, key_expr).is_some())
}

fn partition_key_bool_equality_predicate(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
) -> Option<(usize, bool)> {
    if let Some((key_index, key_value_when_true)) = bool_expr_partition_key_truth_value(expr, spec)
    {
        return Some((key_index, key_value_when_true));
    }
    match expr {
        Expr::Bool(bool_expr)
            if bool_expr.boolop == BoolExprType::Not && bool_expr.args.len() == 1 =>
        {
            let (key_index, value) =
                partition_key_bool_equality_predicate(bool_expr.args.first()?, spec)?;
            Some((key_index, !value))
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::Eq | OpExprKind::NotEq) => {
            let [left, right] = op.args.as_slice() else {
                return None;
            };
            let (key_index, key_value_when_left_true, constant) =
                partition_key_bool_const_cmp(left, right, spec)?;
            let equal_value = if constant {
                key_value_when_left_true
            } else {
                !key_value_when_left_true
            };
            Some((
                key_index,
                if op.op == OpExprKind::Eq {
                    equal_value
                } else {
                    !equal_value
                },
            ))
        }
        _ => None,
    }
}

fn partition_key_bool_const_cmp(
    left: &Expr,
    right: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
) -> Option<(usize, bool, bool)> {
    if let Some(constant) = const_bool_value(right)
        && let Some((key_index, key_value_when_true)) =
            bool_expr_partition_key_truth_value(left, spec)
    {
        return Some((key_index, key_value_when_true, constant));
    }
    if let Some(constant) = const_bool_value(left)
        && let Some((key_index, key_value_when_true)) =
            bool_expr_partition_key_truth_value(right, spec)
    {
        return Some((key_index, key_value_when_true, constant));
    }
    None
}

fn bool_expr_partition_key_truth_value(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
) -> Option<(usize, bool)> {
    spec.key_exprs
        .iter()
        .enumerate()
        .filter(|(index, _)| partition_key_type_is_bool(spec, *index))
        .find_map(|(index, key_expr)| {
            if partition_key_expr_matches(expr, key_expr).is_some() {
                return Some((index, true));
            }
            if bool_expr_is_negation_of_partition_key(expr, key_expr) {
                return Some((index, false));
            }
            None
        })
}

fn bool_expr_is_negation_of_partition_key(expr: &Expr, key_expr: &Expr) -> bool {
    bool_not_arg(expr).is_some_and(|inner| partition_key_expr_matches(inner, key_expr).is_some())
        || bool_not_arg(key_expr)
            .is_some_and(|inner| partition_key_expr_matches(expr, inner).is_some())
}

fn bool_not_arg(expr: &Expr) -> Option<&Expr> {
    let Expr::Bool(bool_expr) = expr else {
        return None;
    };
    (bool_expr.boolop == BoolExprType::Not && bool_expr.args.len() == 1)
        .then(|| bool_expr.args.first())
        .flatten()
}

fn partition_key_type_is_bool(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    key_index: usize,
) -> bool {
    spec.key_types
        .get(key_index)
        .is_some_and(|ty| !ty.is_array && ty.kind == crate::backend::parser::SqlTypeKind::Bool)
}

fn partition_key_expr_matches<'a>(expr: &'a Expr, key_expr: &Expr) -> Option<&'a Expr> {
    expr_matches_partition_key(expr, key_expr).then_some(expr)
}

fn expr_matches_partition_key(expr: &Expr, key_expr: &Expr) -> bool {
    let normalized_key = normalize_key_expr(key_expr);
    let normalized = normalize_key_expr(expr);
    if normalized == normalized_key || simple_var_matches(normalized, normalized_key) {
        return true;
    }
    match expr {
        Expr::Cast(inner, target)
            if partition_key_cast_is_transparent(inner, *target, key_expr) =>
        {
            expr_matches_partition_key(inner, key_expr)
        }
        Expr::Collate { expr: inner, .. } => expr_matches_partition_key(inner, key_expr),
        _ => false,
    }
}

fn simple_var_matches(left: &Expr, right: &Expr) -> bool {
    matches!(
        (left, right),
        (Expr::Var(left), Expr::Var(right))
            if left.varlevelsup == 0
                && right.varlevelsup == 0
                && left.varattno == right.varattno
                && left.vartype == right.vartype
    )
}

fn normalize_key_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            normalize_key_expr(&func.args[0])
        }
        Expr::Collate { expr: inner, .. } => normalize_key_expr(inner),
        other => other,
    }
}

fn partition_key_cast_is_transparent(inner: &Expr, target: SqlType, key_expr: &Expr) -> bool {
    let Some(inner_type) = expr_sql_type_hint(inner) else {
        return false;
    };
    let Some(key_type) = expr_sql_type_hint(key_expr) else {
        return false;
    };
    same_pruning_type(inner_type, target) && same_pruning_type(target, key_type)
        || integer_family_type(inner_type)
            && integer_family_type(target)
            && integer_family_type(key_type)
        || text_family_type(inner_type) && text_family_type(target) && text_family_type(key_type)
}

fn pruning_constant_type_compatible(
    spec: &LoweredPartitionSpec,
    key_index: usize,
    constant_type: Option<SqlType>,
) -> bool {
    let Some(constant_type) = constant_type else {
        return true;
    };
    let Some(key_type) = spec.key_types.get(key_index).copied() else {
        return true;
    };
    same_pruning_type(key_type, constant_type)
        || (integer_family_type(key_type) && integer_family_type(constant_type))
        || (!key_type.is_array
            && key_type.kind == SqlTypeKind::Numeric
            && numeric_family_type(constant_type))
        || (text_family_type(key_type) && text_family_type(constant_type))
}

fn same_pruning_type(left: SqlType, right: SqlType) -> bool {
    left.element_type().kind == right.element_type().kind
        && left.is_array == right.is_array
        && left.type_oid == right.type_oid
}

fn integer_family_type(sql_type: SqlType) -> bool {
    !sql_type.is_array
        && matches!(
            sql_type.kind,
            SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
        )
}

fn numeric_family_type(sql_type: SqlType) -> bool {
    integer_family_type(sql_type) || (!sql_type.is_array && sql_type.kind == SqlTypeKind::Numeric)
}

fn text_family_type(sql_type: SqlType) -> bool {
    !sql_type.is_array
        && matches!(
            sql_type.kind,
            SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char | SqlTypeKind::Name
        )
}

fn collation_mismatch(query_collation_oid: Option<u32>, key_collation_oid: u32) -> bool {
    query_collation_oid.is_some_and(|oid| key_collation_oid != 0 && oid != key_collation_oid)
}

fn const_value(expr: &Expr) -> Option<Value> {
    const_value_with_type(expr).map(|(value, _)| value)
}

fn const_value_with_type(expr: &Expr) -> Option<(Value, Option<SqlType>)> {
    match expr {
        Expr::Const(value) => Some((value.clone(), expr_sql_type_hint(expr))),
        Expr::Cast(inner, ty) => const_value_with_type(inner).map(|(value, _)| (value, Some(*ty))),
        Expr::Collate { expr: inner, .. } => const_value_with_type(inner),
        _ => None,
    }
}

fn const_bool_value(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Const(Value::Bool(value)) => Some(*value),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => const_bool_value(inner),
        _ => None,
    }
}

fn partition_key_const_array_cmp(
    left: &Expr,
    right: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    op: SubqueryComparisonOp,
    query_collation_oid: Option<u32>,
) -> Option<(usize, OpExprKind, Vec<Value>)> {
    let op = subquery_comparison_op_kind(op)?;
    let (values, array_type) = const_array_values_with_type(right)?;
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        let key_collation_oid = spec.partcollation.get(index).copied().unwrap_or(0);
        if collation_mismatch(query_collation_oid, key_collation_oid) {
            continue;
        }
        if partition_key_expr_matches(left, key_expr).is_some()
            && pruning_array_constant_type_compatible(spec, index, array_type)
        {
            return Some((index, op, values));
        }
    }
    None
}

fn const_array_values(expr: &Expr) -> Option<Vec<Value>> {
    const_array_values_with_type(expr).map(|(values, _)| values)
}

fn const_array_values_with_type(expr: &Expr) -> Option<(Vec<Value>, Option<SqlType>)> {
    match expr {
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => elements
            .iter()
            .map(const_value)
            .collect::<Option<Vec<_>>>()
            .map(|values| (values, Some(*array_type))),
        Expr::Const(Value::Array(values)) => Some((values.clone(), expr_sql_type_hint(expr))),
        Expr::Const(Value::PgArray(array)) => {
            Some((array.to_nested_values(), expr_sql_type_hint(expr)))
        }
        Expr::Cast(inner, ty) => {
            const_array_values_with_type(inner).map(|(values, _)| (values, Some(*ty)))
        }
        Expr::Collate { expr: inner, .. } => const_array_values_with_type(inner),
        _ => None,
    }
}

fn pruning_array_constant_type_compatible(
    spec: &LoweredPartitionSpec,
    key_index: usize,
    array_type: Option<SqlType>,
) -> bool {
    let Some(array_type) = array_type else {
        return true;
    };
    pruning_constant_type_compatible(spec, key_index, Some(array_type.element_type()))
}

fn subquery_comparison_op_kind(op: SubqueryComparisonOp) -> Option<OpExprKind> {
    match op {
        SubqueryComparisonOp::Eq => Some(OpExprKind::Eq),
        SubqueryComparisonOp::NotEq => Some(OpExprKind::NotEq),
        SubqueryComparisonOp::Lt => Some(OpExprKind::Lt),
        SubqueryComparisonOp::LtEq => Some(OpExprKind::LtEq),
        SubqueryComparisonOp::Gt => Some(OpExprKind::Gt),
        SubqueryComparisonOp::GtEq => Some(OpExprKind::GtEq),
        _ => None,
    }
}

fn bound_may_satisfy_comparison(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    key_index: usize,
    key_on_left: bool,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    if matches!(value, Value::Null) {
        return false;
    }
    let op = if key_on_left { op } else { commute_op(op) };
    match op {
        OpExprKind::Eq => bound_may_contain_value(
            spec,
            bound,
            sibling_bounds,
            parent_bound,
            key_index,
            value,
            catalog,
        ),
        OpExprKind::NotEq => {
            bound_may_contain_non_equal_value(spec, bound, key_index, value, collation_oid)
        }
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            bound_may_overlap_inequality(
                spec,
                bound,
                sibling_bounds,
                parent_bound,
                key_index,
                op,
                value,
                collation_oid,
            )
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
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    key_index: usize,
    value: &Value,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            if *is_default {
                !sibling_bounds.iter().any(|sibling| {
                    matches!(sibling, PartitionBoundSpec::List { values, is_default: false } if values.iter().any(|item| serialized_value_eq(item, value)))
                })
            } else {
                values.iter().any(|item| serialized_value_eq(item, value))
            }
        }
        (PartitionStrategy::Range, _) => range_may_satisfy_conjunction_value(
            spec,
            bound,
            sibling_bounds,
            parent_bound,
            key_index,
            value,
            OpExprKind::Eq,
            None,
        )
        .unwrap_or(true),
        (PartitionStrategy::Hash, PartitionBoundSpec::Hash { .. }) => {
            hash_bound_may_contain_values(spec, bound, &[value.clone()], catalog).unwrap_or(true)
        }
        _ => true,
    }
}

fn bound_may_contain_non_null(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    key_index: usize,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values
                    .iter()
                    .any(|value| !matches!(value, SerializedPartitionValue::Null))
        }
        (PartitionStrategy::Range, PartitionBoundSpec::Range { .. }) => true,
        _ => true,
    }
}

fn bound_may_contain_non_equal_value(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    key_index: usize,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values.iter().any(|item| {
                    list_value_satisfies_comparison(item, OpExprKind::NotEq, value, collation_oid)
                })
        }
        _ => true,
    }
}

fn bound_may_satisfy_distinctness(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    key_index: usize,
    value: &Value,
    distinct: bool,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            if *is_default {
                if !distinct {
                    return bound_may_contain_value(
                        spec,
                        bound,
                        sibling_bounds,
                        parent_bound,
                        key_index,
                        value,
                        None,
                    );
                }
                return list_default_may_contain_distinct_bool_value(
                    sibling_bounds,
                    value,
                    distinct,
                )
                .unwrap_or(true);
            }
            values
                .iter()
                .any(|item| list_value_satisfies_distinctness(item, value, distinct, None))
        }
        (PartitionStrategy::Range, _) if !distinct => {
            if matches!(value, Value::Null) {
                bound_may_contain_value(
                    spec,
                    bound,
                    sibling_bounds,
                    parent_bound,
                    key_index,
                    value,
                    None,
                )
            } else {
                range_may_satisfy_conjunction_value(
                    spec,
                    bound,
                    sibling_bounds,
                    parent_bound,
                    key_index,
                    value,
                    OpExprKind::Eq,
                    None,
                )
                .unwrap_or(true)
            }
        }
        (PartitionStrategy::Range, PartitionBoundSpec::Range { is_default, .. })
            if distinct && matches!(value, Value::Bool(_)) =>
        {
            if *is_default {
                return true;
            }
            let Value::Bool(value) = value else {
                return true;
            };
            range_may_satisfy_conjunction_value(
                spec,
                bound,
                sibling_bounds,
                parent_bound,
                key_index,
                &Value::Bool(!value),
                OpExprKind::Eq,
                None,
            )
            .unwrap_or(true)
        }
        (PartitionStrategy::Hash, PartitionBoundSpec::Hash { .. }) if !distinct => {
            hash_bound_may_contain_values(spec, bound, &[value.clone()], None).unwrap_or(true)
        }
        _ => true,
    }
}

fn bound_may_overlap_inequality(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    key_index: usize,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values
                    .iter()
                    .any(|item| list_value_satisfies_comparison(item, op, value, collation_oid))
        }
        (PartitionStrategy::Range, _) => range_may_satisfy_conjunction_value(
            spec,
            bound,
            sibling_bounds,
            parent_bound,
            key_index,
            value,
            op,
            collation_oid,
        )
        .unwrap_or(true),
        _ => true,
    }
}

fn serialized_value_eq(value: &SerializedPartitionValue, other: &Value) -> bool {
    partition_value_to_value(value) == *other
}

fn serialized_partition_value_is_null(value: &SerializedPartitionValue) -> bool {
    matches!(value, SerializedPartitionValue::Null)
}

fn list_value_satisfies_comparison(
    item: &SerializedPartitionValue,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    if serialized_partition_value_is_null(item) || matches!(value, Value::Null) {
        return false;
    }
    let Some(cmp) = compare_partition_value(item, value, collation_oid) else {
        return true;
    };
    match op {
        OpExprKind::Eq => cmp == Ordering::Equal,
        OpExprKind::NotEq => cmp != Ordering::Equal,
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            cmp_satisfies(cmp, op)
        }
        _ => true,
    }
}

fn list_value_satisfies_distinctness(
    item: &SerializedPartitionValue,
    value: &Value,
    distinct: bool,
    collation_oid: Option<u32>,
) -> bool {
    let item_value = partition_value_to_value(item);
    let values_distinct =
        values_are_distinct_for_pruning(&item_value, value, collation_oid).unwrap_or(distinct);
    values_distinct == distinct
}

fn values_are_distinct_for_pruning(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
) -> Option<bool> {
    match (left, right) {
        (Value::Null, Value::Null) => Some(false),
        (Value::Null, _) | (_, Value::Null) => Some(true),
        _ => Some(
            compare_order_values(left, right, collation_oid, None, false).ok()? != Ordering::Equal,
        ),
    }
}

fn list_default_may_contain_distinct_bool_value(
    sibling_bounds: &[PartitionBoundSpec],
    value: &Value,
    distinct: bool,
) -> Option<bool> {
    let possible_values = [Value::Null, Value::Bool(false), Value::Bool(true)]
        .into_iter()
        .filter(|candidate| {
            values_are_distinct_for_pruning(candidate, value, None).unwrap_or(true) == distinct
        })
        .collect::<Vec<_>>();
    if possible_values.is_empty() || !matches!(value, Value::Null | Value::Bool(_)) {
        return None;
    }
    Some(possible_values.into_iter().any(|candidate| {
        !sibling_bounds.iter().any(|sibling| {
            matches!(
                sibling,
                PartitionBoundSpec::List {
                    values,
                    is_default: false,
                } if values.iter().any(|value| serialized_value_eq(value, &candidate))
            )
        })
    }))
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

fn serialized_partition_value_cmp(
    left: &SerializedPartitionValue,
    right: &SerializedPartitionValue,
) -> Ordering {
    compare_order_values(
        &partition_value_to_value(left),
        &partition_value_to_value(right),
        None,
        None,
        false,
    )
    .unwrap_or_else(|_| format!("{left:?}").cmp(&format!("{right:?}")))
}

fn serialized_partition_list_value_cmp(
    left: &SerializedPartitionValue,
    right: &SerializedPartitionValue,
) -> Ordering {
    match (left, right) {
        (SerializedPartitionValue::Null, SerializedPartitionValue::Null) => Ordering::Equal,
        (SerializedPartitionValue::Null, _) => Ordering::Greater,
        (_, SerializedPartitionValue::Null) => Ordering::Less,
        _ => serialized_partition_value_cmp(left, right),
    }
}

fn range_datum_cmp(left: &PartitionRangeDatumValue, right: &PartitionRangeDatumValue) -> Ordering {
    match (left, right) {
        (PartitionRangeDatumValue::MinValue, PartitionRangeDatumValue::MinValue)
        | (PartitionRangeDatumValue::MaxValue, PartitionRangeDatumValue::MaxValue) => {
            Ordering::Equal
        }
        (PartitionRangeDatumValue::MinValue, _) | (_, PartitionRangeDatumValue::MaxValue) => {
            Ordering::Less
        }
        (PartitionRangeDatumValue::MaxValue, _) | (_, PartitionRangeDatumValue::MinValue) => {
            Ordering::Greater
        }
        (PartitionRangeDatumValue::Value(left), PartitionRangeDatumValue::Value(right)) => {
            serialized_partition_value_cmp(left, right)
        }
    }
}

fn range_datums_cmp(
    left: &[PartitionRangeDatumValue],
    right: &[PartitionRangeDatumValue],
) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| range_datum_cmp(left, right))
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

pub(super) fn partition_bound_cmp(
    left: &PartitionBoundSpec,
    right: &PartitionBoundSpec,
) -> Ordering {
    match (left, right) {
        (
            PartitionBoundSpec::Range {
                from: left_from,
                to: left_to,
                is_default: left_default,
            },
            PartitionBoundSpec::Range {
                from: right_from,
                to: right_to,
                is_default: right_default,
            },
        ) => left_default.cmp(right_default).then_with(|| {
            range_datums_cmp(left_from, right_from)
                .then_with(|| range_datums_cmp(left_to, right_to))
        }),
        (
            PartitionBoundSpec::List {
                values: left_values,
                is_default: left_default,
            },
            PartitionBoundSpec::List {
                values: right_values,
                is_default: right_default,
            },
        ) => left_default.cmp(right_default).then_with(|| {
            left_values
                .iter()
                .zip(right_values)
                .map(|(left, right)| serialized_partition_list_value_cmp(left, right))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| left_values.len().cmp(&right_values.len()))
        }),
        (
            PartitionBoundSpec::Hash {
                modulus: left_modulus,
                remainder: left_remainder,
            },
            PartitionBoundSpec::Hash {
                modulus: right_modulus,
                remainder: right_remainder,
            },
        ) => left_modulus
            .cmp(right_modulus)
            .then_with(|| left_remainder.cmp(right_remainder)),
        _ => format!("{left:?}").cmp(&format!("{right:?}")),
    }
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

#[derive(Clone, Debug)]
struct ScalarBound {
    value: Value,
    inclusive: bool,
}

#[derive(Clone, Debug, Default)]
struct KeyConstraint {
    lower: Option<ScalarBound>,
    upper: Option<ScalarBound>,
    equal: Option<Value>,
    requires_null: bool,
}

#[derive(Debug)]
enum ConstraintApplyResult {
    Applied,
    Ignored,
    Contradiction,
}

fn range_may_satisfy_conjunction(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    in_or_arm: bool,
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return None;
    }
    let mut constraints = vec![KeyConstraint::default(); spec.key_exprs.len()];
    let mut saw_constraint = false;
    for conjunct in flatten_and_exprs(expr) {
        match apply_range_constraint(conjunct, spec, &mut constraints) {
            ConstraintApplyResult::Applied => saw_constraint = true,
            ConstraintApplyResult::Ignored => {}
            ConstraintApplyResult::Contradiction if in_or_arm => return None,
            ConstraintApplyResult::Contradiction => return Some(false),
        }
    }
    if !saw_constraint {
        return None;
    }
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, parent_bound, &constraints)
}

fn range_may_satisfy_conjunction_value(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    key_index: usize,
    value: &Value,
    op: OpExprKind,
    collation_oid: Option<u32>,
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return None;
    }
    let mut constraints = vec![KeyConstraint::default(); spec.key_exprs.len()];
    if !add_comparison_constraint(
        &mut constraints,
        spec,
        key_index,
        op,
        value.clone(),
        collation_oid,
    ) {
        return Some(false);
    }
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, parent_bound, &constraints)
}

#[derive(Clone, Debug, Default)]
struct HashKeyConstraint {
    value: Option<Value>,
    constrained: bool,
}

fn hash_may_satisfy_conjunction(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Hash) {
        return None;
    }
    let mut constraints = vec![HashKeyConstraint::default(); spec.key_exprs.len()];
    let mut saw_constraint = false;
    for conjunct in flatten_and_exprs(expr) {
        match apply_hash_constraint(conjunct, spec, &mut constraints) {
            ConstraintApplyResult::Applied => saw_constraint = true,
            ConstraintApplyResult::Ignored => {}
            ConstraintApplyResult::Contradiction => return Some(false),
        }
    }
    if !saw_constraint || constraints.iter().any(|constraint| !constraint.constrained) {
        return None;
    }
    let values = constraints
        .into_iter()
        .map(|constraint| constraint.value.unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    hash_bound_may_contain_values(spec, bound, &values, catalog)
}

fn apply_hash_constraint(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    constraints: &mut [HashKeyConstraint],
) -> ConstraintApplyResult {
    if let Some((key_index, value)) = partition_key_bool_equality_predicate(expr, spec) {
        return add_hash_equality_constraint(constraints, key_index, Value::Bool(value));
    }
    match expr {
        Expr::IsNull(inner) => {
            let Some(index) = partition_key_index(inner, spec) else {
                return ConstraintApplyResult::Ignored;
            };
            add_hash_equality_constraint(constraints, index, Value::Null)
        }
        Expr::Op(op) if op.op == OpExprKind::Eq => {
            let [left, right] = op.args.as_slice() else {
                return ConstraintApplyResult::Ignored;
            };
            let Some((key_index, _, value)) =
                partition_key_const_cmp(left, right, spec, op.collation_oid)
            else {
                return ConstraintApplyResult::Ignored;
            };
            if matches!(value, Value::Null) {
                return ConstraintApplyResult::Contradiction;
            }
            add_hash_equality_constraint(constraints, key_index, value)
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let Some((key_index, value)) = partition_key_const_distinct_cmp(left, right, spec)
            else {
                return ConstraintApplyResult::Ignored;
            };
            add_hash_equality_constraint(constraints, key_index, value)
        }
        _ => ConstraintApplyResult::Ignored,
    }
}

fn add_hash_equality_constraint(
    constraints: &mut [HashKeyConstraint],
    key_index: usize,
    value: Value,
) -> ConstraintApplyResult {
    let Some(constraint) = constraints.get_mut(key_index) else {
        return ConstraintApplyResult::Ignored;
    };
    if constraint.constrained {
        let existing = constraint.value.as_ref().unwrap_or(&Value::Null);
        if values_are_distinct_for_pruning(existing, &value, None).unwrap_or(true) {
            return ConstraintApplyResult::Contradiction;
        }
    }
    constraint.value = Some(value);
    constraint.constrained = true;
    ConstraintApplyResult::Applied
}

fn hash_bound_may_contain_values(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    values: &[Value],
    catalog: Option<&dyn CatalogLookup>,
) -> Option<bool> {
    let PartitionBoundSpec::Hash { modulus, remainder } = bound else {
        return None;
    };
    if values.len() != spec.key_exprs.len() {
        return None;
    }
    let hash = partition_prune_hash_values_combined(values, spec, catalog)?;
    Some(hash % (*modulus as u64) == *remainder as u64)
}

fn partition_prune_hash_values_combined(
    values: &[Value],
    spec: &crate::backend::parser::LoweredPartitionSpec,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u64> {
    let mut row_hash = 0_u64;
    for (index, value) in values.iter().enumerate() {
        if matches!(value, Value::Null) {
            continue;
        }
        let value_hash = partition_prune_hash_value(value, spec, index, catalog)?;
        row_hash = crate::backend::access::hash::support::hash_combine64(row_hash, value_hash);
    }
    Some(row_hash)
}

fn partition_prune_hash_value(
    value: &Value,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    key_index: usize,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u64> {
    if let Some(proc_oid) = partition_hash_support_proc(key_index, spec, catalog) {
        return eval_partition_hash_support_proc(proc_oid, value, catalog);
    }
    crate::backend::access::hash::support::hash_value_extended(
        value,
        spec.partclass.get(key_index).copied(),
        crate::backend::access::hash::support::HASH_PARTITION_SEED,
    )
    .ok()
    .flatten()
}

fn partition_hash_support_proc(
    key_index: usize,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u32> {
    let catalog = catalog?;
    let opclass_oid = *spec.partclass.get(key_index)?;
    let opclass = catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    let key_type_oid = sql_type_oid(*spec.key_types.get(key_index)?);
    catalog
        .amproc_rows()
        .into_iter()
        .find(|row| {
            row.amprocfamily == opclass.opcfamily
                && row.amprocnum == 2
                && (row.amproclefttype == key_type_oid || row.amproclefttype == ANYOID)
                && (row.amprocrighttype == key_type_oid || row.amprocrighttype == ANYOID)
        })
        .map(|row| row.amproc)
}

fn eval_partition_hash_support_proc(
    proc_oid: u32,
    value: &Value,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u64> {
    if matches!(value, Value::Null) {
        return None;
    }
    let catalog = catalog?;
    if let Some(func) = builtin_scalar_function_for_proc_oid(proc_oid)
        && let BuiltinScalarFunction::HashValueExtended(kind) = func
    {
        let opclass = (kind == crate::include::nodes::primnodes::HashFunctionKind::BpChar)
            .then_some(crate::include::catalog::BPCHAR_HASH_OPCLASS_OID);
        return crate::backend::access::hash::support::hash_value_extended(
            value,
            opclass,
            crate::backend::access::hash::support::HASH_PARTITION_SEED,
        )
        .ok()
        .flatten();
    }
    let row = catalog.proc_row_by_oid(proc_oid)?;
    if row.prolang != PG_LANGUAGE_SQL_OID || row.provolatile != 'i' {
        return None;
    }
    eval_lightweight_partition_hash_sql_proc(&row.prosrc, value)
}

fn eval_lightweight_partition_hash_sql_proc(source: &str, value: &Value) -> Option<u64> {
    let compact = source
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<String>();
    match compact.as_str() {
        "selectvalue+seed" => {
            let value = match value {
                Value::Int16(value) => i64::from(*value),
                Value::Int32(value) => i64::from(*value),
                Value::Int64(value) => *value,
                _ => return None,
            };
            Some(
                value
                    .wrapping_add(crate::backend::access::hash::support::HASH_PARTITION_SEED as i64)
                    as u64,
            )
        }
        "selectlength(coalesce(value,''))::int8" => Some(value.as_text()?.chars().count() as u64),
        _ => None,
    }
}

fn flatten_and_exprs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            bool_expr.args.iter().flat_map(flatten_and_exprs).collect()
        }
        other => vec![other],
    }
}

fn apply_range_constraint(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    constraints: &mut [KeyConstraint],
) -> ConstraintApplyResult {
    if let Some((key_index, value)) = partition_key_bool_equality_predicate(expr, spec) {
        return if add_comparison_constraint(
            constraints,
            spec,
            key_index,
            OpExprKind::Eq,
            Value::Bool(value),
            None,
        ) {
            ConstraintApplyResult::Applied
        } else {
            ConstraintApplyResult::Contradiction
        };
    }
    match expr {
        Expr::IsNull(inner) => {
            let Some(index) = partition_key_index(inner, spec) else {
                return ConstraintApplyResult::Ignored;
            };
            constraints[index].requires_null = true;
            ConstraintApplyResult::Applied
        }
        Expr::IsNotNull(inner) => {
            if partition_key_index(inner, spec).is_some() {
                ConstraintApplyResult::Applied
            } else {
                ConstraintApplyResult::Ignored
            }
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return ConstraintApplyResult::Ignored;
            };
            let Some((key_index, key_on_left, value)) =
                partition_key_const_cmp(left, right, spec, op.collation_oid)
            else {
                return ConstraintApplyResult::Ignored;
            };
            let collation_oid = op.collation_oid;
            let op = if key_on_left {
                op.op
            } else {
                commute_op(op.op)
            };
            if add_comparison_constraint(constraints, spec, key_index, op, value, collation_oid) {
                ConstraintApplyResult::Applied
            } else {
                ConstraintApplyResult::Contradiction
            }
        }
        _ => ConstraintApplyResult::Ignored,
    }
}

fn add_comparison_constraint(
    constraints: &mut [KeyConstraint],
    spec: &LoweredPartitionSpec,
    key_index: usize,
    op: OpExprKind,
    value: Value,
    collation_oid: Option<u32>,
) -> bool {
    if key_index >= constraints.len() {
        return true;
    }
    if matches!(value, Value::Null) {
        if op == OpExprKind::Eq {
            constraints[key_index].requires_null = true;
            return true;
        }
        return true;
    }
    let key_collation_oid = spec.partcollation.get(key_index).copied().unwrap_or(0);
    let collation_oid = collation_oid.or((key_collation_oid != 0).then_some(key_collation_oid));
    let constraint = &mut constraints[key_index];
    match op {
        OpExprKind::Eq => add_equality_constraint(constraint, value, collation_oid),
        OpExprKind::Lt => {
            add_upper_constraint(constraint, value, false, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::LtEq => {
            add_upper_constraint(constraint, value, true, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::Gt => {
            add_lower_constraint(constraint, value, false, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::GtEq => {
            add_lower_constraint(constraint, value, true, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        _ => true,
    }
}

fn add_equality_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    collation_oid: Option<u32>,
) -> bool {
    if let Some(existing) = &constraint.equal
        && compare_order_values(existing, &value, collation_oid, None, false).ok()
            != Some(Ordering::Equal)
    {
        return false;
    }
    constraint.equal = Some(value);
    constraint_is_possible(constraint, collation_oid)
}

fn add_lower_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    inclusive: bool,
    collation_oid: Option<u32>,
) {
    let new_bound = ScalarBound { value, inclusive };
    let should_replace = match &constraint.lower {
        None => true,
        Some(existing) => match compare_order_values(
            &new_bound.value,
            &existing.value,
            collation_oid,
            None,
            false,
        )
        .ok()
        {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => !new_bound.inclusive && existing.inclusive,
            _ => false,
        },
    };
    if should_replace {
        constraint.lower = Some(new_bound);
    }
}

fn add_upper_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    inclusive: bool,
    collation_oid: Option<u32>,
) {
    let new_bound = ScalarBound { value, inclusive };
    let should_replace = match &constraint.upper {
        None => true,
        Some(existing) => match compare_order_values(
            &new_bound.value,
            &existing.value,
            collation_oid,
            None,
            false,
        )
        .ok()
        {
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => !new_bound.inclusive && existing.inclusive,
            _ => false,
        },
    };
    if should_replace {
        constraint.upper = Some(new_bound);
    }
}

fn constraint_is_possible(constraint: &KeyConstraint, collation_oid: Option<u32>) -> bool {
    let Some(lower) = &constraint.lower else {
        return equality_satisfies_bounds(constraint, collation_oid);
    };
    let Some(upper) = &constraint.upper else {
        return equality_satisfies_bounds(constraint, collation_oid);
    };
    match compare_order_values(&lower.value, &upper.value, collation_oid, None, false).ok() {
        Some(Ordering::Less) => equality_satisfies_bounds(constraint, collation_oid),
        Some(Ordering::Equal) if lower.inclusive && upper.inclusive => {
            equality_satisfies_bounds(constraint, collation_oid)
        }
        Some(_) => false,
        None => true,
    }
}

fn equality_satisfies_bounds(constraint: &KeyConstraint, collation_oid: Option<u32>) -> bool {
    let Some(equal) = &constraint.equal else {
        return true;
    };
    if let Some(lower) = &constraint.lower {
        match compare_order_values(equal, &lower.value, collation_oid, None, false).ok() {
            Some(Ordering::Less) => return false,
            Some(Ordering::Equal) if !lower.inclusive => return false,
            _ => {}
        }
    }
    if let Some(upper) = &constraint.upper {
        match compare_order_values(equal, &upper.value, collation_oid, None, false).ok() {
            Some(Ordering::Greater) => return false,
            Some(Ordering::Equal) if !upper.inclusive => return false,
            _ => {}
        }
    }
    true
}

fn range_bound_may_overlap_constraints(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    constraints: &[KeyConstraint],
) -> Option<bool> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default,
    } = bound
    else {
        return None;
    };
    if *is_default {
        if let Some(values) = exact_constraint_values(constraints) {
            return Some(!sibling_bounds.iter().any(|sibling| {
                !sibling.is_default()
                    && range_bound_contains_tuple(sibling, &values, &spec.partcollation)
                        == Some(true)
            }));
        }
        return range_default_may_overlap_constraints(
            spec,
            sibling_bounds,
            parent_bound,
            constraints,
        );
    }
    if constraints
        .iter()
        .any(|constraint| constraint.requires_null)
    {
        return Some(false);
    }
    let query_min = query_bound_components(constraints, QueryBoundSide::Lower);
    let query_max = query_bound_components(constraints, QueryBoundSide::Upper);
    if compare_query_bound_to_range_bound(
        &query_max,
        QueryBoundSide::Upper,
        from,
        &spec.partcollation,
    ) == Some(Ordering::Less)
    {
        return Some(false);
    }
    if compare_query_bound_to_range_bound(
        &query_min,
        QueryBoundSide::Lower,
        to,
        &spec.partcollation,
    )
    .is_some_and(|cmp| cmp != Ordering::Less)
    {
        return Some(false);
    }
    Some(true)
}

fn range_default_may_overlap_constraints(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    sibling_bounds: &[PartitionBoundSpec],
    parent_bound: Option<&PartitionBoundSpec>,
    constraints: &[KeyConstraint],
) -> Option<bool> {
    if spec.key_exprs.len() != constraints.len() {
        return Some(true);
    }
    if constraints
        .iter()
        .any(|constraint| constraint.requires_null)
    {
        return Some(true);
    }
    if spec.key_exprs.len() > 1 {
        if !constraints.iter().all(key_constraint_has_range_bound) {
            return Some(true);
        }
        let interval = query_tuple_interval_from_constraints(constraints);
        return range_tuple_interval_covered_by_non_default_siblings(
            &interval,
            sibling_bounds,
            &spec.partcollation,
        )
        .map(|covered| !covered)
        .or(Some(true));
    }
    let constraint = &constraints[0];
    let Some(mut interval) = query_interval_from_constraint(constraint) else {
        return Some(true);
    };
    let collation_oid = spec.partcollation.first().copied().filter(|oid| *oid != 0);
    if let Some(parent_range) = parent_bound.and_then(range_interval_from_non_default_bound) {
        interval =
            match intersect_query_interval_with_range(&interval, &parent_range, collation_oid) {
                Some(Some(interval)) => interval,
                Some(None) => return Some(false),
                None => return Some(true),
            };
    }
    range_interval_covered_by_non_default_siblings(&interval, sibling_bounds, collation_oid)
        .map(|covered| !covered)
        .or(Some(true))
}

fn key_constraint_has_range_bound(constraint: &KeyConstraint) -> bool {
    constraint.equal.is_some() || constraint.lower.is_some() || constraint.upper.is_some()
}

#[derive(Clone, Debug)]
struct QueryInterval {
    lower: BoundPoint,
    upper: BoundPoint,
    upper_inclusive: bool,
}

#[derive(Clone, Debug)]
struct RangeInterval {
    lower: BoundPoint,
    upper: BoundPoint,
}

#[derive(Clone, Debug)]
struct QueryTupleInterval {
    lower: Vec<BoundPoint>,
    upper: Vec<BoundPoint>,
    upper_inclusive: bool,
}

#[derive(Clone, Debug)]
struct RangeTupleInterval {
    lower: Vec<BoundPoint>,
    upper: Vec<BoundPoint>,
}

#[derive(Clone, Debug)]
enum BoundPoint {
    NegInfinity,
    Value(Value),
    PosInfinity,
}

fn query_interval_from_constraint(constraint: &KeyConstraint) -> Option<QueryInterval> {
    if constraint.equal.is_some() {
        return None;
    }
    if constraint.lower.is_none() && constraint.upper.is_none() {
        return None;
    }
    Some(QueryInterval {
        lower: constraint
            .lower
            .as_ref()
            .map(|bound| BoundPoint::Value(bound.value.clone()))
            .unwrap_or(BoundPoint::NegInfinity),
        upper: constraint
            .upper
            .as_ref()
            .map(|bound| BoundPoint::Value(bound.value.clone()))
            .unwrap_or(BoundPoint::PosInfinity),
        upper_inclusive: constraint
            .upper
            .as_ref()
            .map(|bound| bound.inclusive)
            .unwrap_or(false),
    })
}

fn query_tuple_interval_from_constraints(constraints: &[KeyConstraint]) -> QueryTupleInterval {
    let lower_components = query_bound_components(constraints, QueryBoundSide::Lower);
    let upper_components = query_bound_components(constraints, QueryBoundSide::Upper);
    QueryTupleInterval {
        lower: lower_components
            .iter()
            .map(bound_point_from_query_component)
            .collect(),
        upper_inclusive: upper_components.iter().all(|component| {
            matches!(
                component,
                QueryBoundComponent::Value {
                    inclusive: true,
                    ..
                }
            )
        }),
        upper: upper_components
            .iter()
            .map(bound_point_from_query_component)
            .collect(),
    }
}

fn bound_point_from_query_component(component: &QueryBoundComponent<'_>) -> BoundPoint {
    match component {
        QueryBoundComponent::NegInfinity => BoundPoint::NegInfinity,
        QueryBoundComponent::PosInfinity => BoundPoint::PosInfinity,
        QueryBoundComponent::Value { value, .. } => BoundPoint::Value((*value).clone()),
    }
}

fn intersect_query_interval_with_range(
    query: &QueryInterval,
    range: &RangeInterval,
    collation_oid: Option<u32>,
) -> Option<Option<QueryInterval>> {
    match compare_bound_points(&query.upper, &range.lower, collation_oid)? {
        Ordering::Less => return Some(None),
        Ordering::Equal if !query.upper_inclusive => return Some(None),
        _ => {}
    }
    if compare_bound_points(&query.lower, &range.upper, collation_oid)? != Ordering::Less {
        return Some(None);
    }

    let lower =
        if compare_bound_points(&query.lower, &range.lower, collation_oid)? == Ordering::Less {
            range.lower.clone()
        } else {
            query.lower.clone()
        };
    let upper_cmp = compare_bound_points(&query.upper, &range.upper, collation_oid)?;
    let (upper, upper_inclusive) = match upper_cmp {
        Ordering::Less => (query.upper.clone(), query.upper_inclusive),
        Ordering::Equal => (query.upper.clone(), false),
        Ordering::Greater => (range.upper.clone(), false),
    };
    Some(Some(QueryInterval {
        lower,
        upper,
        upper_inclusive,
    }))
}

fn range_interval_covered_by_non_default_siblings(
    query: &QueryInterval,
    sibling_bounds: &[PartitionBoundSpec],
    collation_oid: Option<u32>,
) -> Option<bool> {
    let mut ranges = sibling_bounds
        .iter()
        .filter_map(range_interval_from_non_default_bound)
        .collect::<Vec<_>>();
    ranges.sort_by(|left, right| {
        compare_bound_points(&left.lower, &right.lower, collation_oid).unwrap_or(Ordering::Equal)
    });
    let mut covered_until = query.lower.clone();
    for range in ranges {
        if compare_bound_points(&range.upper, &covered_until, collation_oid)? != Ordering::Greater {
            continue;
        }
        if compare_bound_points(&range.lower, &covered_until, collation_oid)? == Ordering::Greater {
            return Some(false);
        }
        covered_until = range.upper;
        match compare_bound_points(&covered_until, &query.upper, collation_oid)? {
            Ordering::Greater => return Some(true),
            Ordering::Equal if !query.upper_inclusive => return Some(true),
            _ => {}
        }
    }
    Some(false)
}

fn range_tuple_interval_covered_by_non_default_siblings(
    query: &QueryTupleInterval,
    sibling_bounds: &[PartitionBoundSpec],
    collations: &[u32],
) -> Option<bool> {
    let mut ranges = sibling_bounds
        .iter()
        .filter_map(range_tuple_interval_from_non_default_bound)
        .collect::<Vec<_>>();
    ranges.sort_by(|left, right| {
        compare_tuple_bound_points(&left.lower, &right.lower, collations).unwrap_or(Ordering::Equal)
    });
    let mut covered_until = query.lower.clone();
    for range in ranges {
        if compare_tuple_bound_points(&range.upper, &covered_until, collations)?
            != Ordering::Greater
        {
            continue;
        }
        if compare_tuple_bound_points(&range.lower, &covered_until, collations)?
            == Ordering::Greater
        {
            return Some(false);
        }
        covered_until = range.upper;
        match compare_tuple_bound_points(&covered_until, &query.upper, collations)? {
            Ordering::Greater => return Some(true),
            Ordering::Equal if !query.upper_inclusive => return Some(true),
            _ => {}
        }
    }
    Some(false)
}

fn range_interval_from_non_default_bound(bound: &PartitionBoundSpec) -> Option<RangeInterval> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default: false,
    } = bound
    else {
        return None;
    };
    if from.len() != 1 || to.len() != 1 {
        return None;
    }
    Some(RangeInterval {
        lower: bound_point_from_range_datum(&from[0]),
        upper: bound_point_from_range_datum(&to[0]),
    })
}

fn range_tuple_interval_from_non_default_bound(
    bound: &PartitionBoundSpec,
) -> Option<RangeTupleInterval> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default: false,
    } = bound
    else {
        return None;
    };
    Some(RangeTupleInterval {
        lower: from.iter().map(bound_point_from_range_datum).collect(),
        upper: to.iter().map(bound_point_from_range_datum).collect(),
    })
}

fn bound_point_from_range_datum(value: &PartitionRangeDatumValue) -> BoundPoint {
    match value {
        PartitionRangeDatumValue::MinValue => BoundPoint::NegInfinity,
        PartitionRangeDatumValue::MaxValue => BoundPoint::PosInfinity,
        PartitionRangeDatumValue::Value(value) => {
            BoundPoint::Value(partition_value_to_value(value))
        }
    }
}

fn compare_bound_points(
    left: &BoundPoint,
    right: &BoundPoint,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    match (left, right) {
        (BoundPoint::NegInfinity, BoundPoint::NegInfinity)
        | (BoundPoint::PosInfinity, BoundPoint::PosInfinity) => Some(Ordering::Equal),
        (BoundPoint::NegInfinity, _) | (_, BoundPoint::PosInfinity) => Some(Ordering::Less),
        (BoundPoint::PosInfinity, _) | (_, BoundPoint::NegInfinity) => Some(Ordering::Greater),
        (BoundPoint::Value(left), BoundPoint::Value(right)) => {
            compare_order_values(left, right, collation_oid, None, false).ok()
        }
    }
}

fn compare_tuple_bound_points(
    left: &[BoundPoint],
    right: &[BoundPoint],
    collations: &[u32],
) -> Option<Ordering> {
    let len = left.len().max(right.len());
    for index in 0..len {
        let default = BoundPoint::PosInfinity;
        let left_point = left.get(index).unwrap_or(&default);
        let right_point = right.get(index).unwrap_or(&default);
        let collation_oid = collations.get(index).copied().filter(|oid| *oid != 0);
        let cmp = compare_bound_points(left_point, right_point, collation_oid)?;
        if cmp != Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(Ordering::Equal)
}

fn exact_constraint_values(constraints: &[KeyConstraint]) -> Option<Vec<Value>> {
    constraints
        .iter()
        .map(|constraint| {
            if constraint.requires_null {
                return None;
            }
            constraint.equal.clone()
        })
        .collect()
}

fn range_bound_contains_tuple(
    bound: &PartitionBoundSpec,
    values: &[Value],
    collations: &[u32],
) -> Option<bool> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default,
    } = bound
    else {
        return None;
    };
    if *is_default || values.iter().any(|value| matches!(value, Value::Null)) {
        return Some(false);
    }
    Some(
        compare_tuple_to_range_bound(values, from, collations) != Some(Ordering::Less)
            && compare_tuple_to_range_bound(values, to, collations) == Some(Ordering::Less),
    )
}

fn compare_tuple_to_range_bound(
    values: &[Value],
    range: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Option<Ordering> {
    for (index, value) in values.iter().enumerate() {
        let range_component = range
            .get(index)
            .unwrap_or(&PartitionRangeDatumValue::MaxValue);
        let collation_oid = collations.get(index).copied().filter(|oid| *oid != 0);
        let cmp = match range_component {
            PartitionRangeDatumValue::MinValue => Ordering::Greater,
            PartitionRangeDatumValue::MaxValue => Ordering::Less,
            PartitionRangeDatumValue::Value(range_value) => compare_order_values(
                value,
                &partition_value_to_value(range_value),
                collation_oid,
                None,
                false,
            )
            .ok()?,
        };
        if cmp != Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(Ordering::Equal)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryBoundSide {
    Lower,
    Upper,
}

#[derive(Clone, Debug)]
enum QueryBoundComponent<'a> {
    NegInfinity,
    PosInfinity,
    Value { value: &'a Value, inclusive: bool },
}

fn query_bound_components(
    constraints: &[KeyConstraint],
    side: QueryBoundSide,
) -> Vec<QueryBoundComponent<'_>> {
    constraints
        .iter()
        .map(|constraint| {
            if let Some(equal) = &constraint.equal {
                return QueryBoundComponent::Value {
                    value: equal,
                    inclusive: true,
                };
            }
            match side {
                QueryBoundSide::Lower => constraint
                    .lower
                    .as_ref()
                    .map(|bound| QueryBoundComponent::Value {
                        value: &bound.value,
                        inclusive: bound.inclusive,
                    })
                    .unwrap_or(QueryBoundComponent::NegInfinity),
                QueryBoundSide::Upper => constraint
                    .upper
                    .as_ref()
                    .map(|bound| QueryBoundComponent::Value {
                        value: &bound.value,
                        inclusive: bound.inclusive,
                    })
                    .unwrap_or(QueryBoundComponent::PosInfinity),
            }
        })
        .collect()
}

fn compare_query_bound_to_range_bound(
    query: &[QueryBoundComponent<'_>],
    side: QueryBoundSide,
    range: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Option<Ordering> {
    for (index, query_component) in query.iter().enumerate() {
        let range_component = range
            .get(index)
            .unwrap_or(&PartitionRangeDatumValue::MaxValue);
        let collation_oid = collations.get(index).copied().filter(|oid| *oid != 0);
        let cmp = compare_query_component_to_range_component(
            query_component,
            range_component,
            side,
            collation_oid,
        )?;
        if cmp != Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(Ordering::Equal)
}

fn compare_query_component_to_range_component(
    query: &QueryBoundComponent<'_>,
    range: &PartitionRangeDatumValue,
    side: QueryBoundSide,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    match (query, range) {
        (QueryBoundComponent::NegInfinity, PartitionRangeDatumValue::MinValue)
        | (QueryBoundComponent::PosInfinity, PartitionRangeDatumValue::MaxValue) => {
            Some(Ordering::Equal)
        }
        (QueryBoundComponent::NegInfinity, _) => Some(Ordering::Less),
        (QueryBoundComponent::PosInfinity, _) => Some(Ordering::Greater),
        (_, PartitionRangeDatumValue::MinValue) => Some(Ordering::Greater),
        (_, PartitionRangeDatumValue::MaxValue) => Some(Ordering::Less),
        (
            QueryBoundComponent::Value { value, inclusive },
            PartitionRangeDatumValue::Value(range_value),
        ) => {
            let cmp = compare_order_values(
                value,
                &partition_value_to_value(range_value),
                collation_oid,
                None,
                false,
            )
            .ok()?;
            if cmp != Ordering::Equal || *inclusive {
                return Some(cmp);
            }
            match side {
                QueryBoundSide::Lower => Some(Ordering::Greater),
                QueryBoundSide::Upper => Some(Ordering::Less),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelFileLocator;
    use crate::backend::parser::{
        BoundRelation, LoweredPartitionSpec, SqlType, SqlTypeKind, pg_partitioned_table_row,
        serialize_partition_bound,
    };
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_OID, PUBLIC_NAMESPACE_OID, PgInheritsRow,
    };
    use crate::include::nodes::primnodes::RelationDesc;
    use crate::include::nodes::primnodes::{ScalarArrayOpExpr, Var};

    #[derive(Default)]
    struct MockCatalog {
        relations: Vec<BoundRelation>,
        inherits: Vec<PgInheritsRow>,
    }

    impl CatalogLookup for MockCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }

        fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
            self.relations
                .iter()
                .find(|relation| relation.relation_oid == relation_oid)
                .cloned()
        }

        fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
            self.inherits
                .iter()
                .filter(|row| row.inhrelid == relation_oid)
                .cloned()
                .collect()
        }

        fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
            self.inherits
                .iter()
                .filter(|row| row.inhparent == relation_oid)
                .cloned()
                .collect()
        }
    }

    fn expr_may_match_bound(
        expr: &Expr,
        spec: &LoweredPartitionSpec,
        bound: &PartitionBoundSpec,
        sibling_bounds: &[PartitionBoundSpec],
    ) -> bool {
        super::expr_may_match_bound(expr, spec, bound, sibling_bounds, None, None)
    }

    fn expr_may_match_bound_with_parent(
        expr: &Expr,
        spec: &LoweredPartitionSpec,
        bound: &PartitionBoundSpec,
        sibling_bounds: &[PartitionBoundSpec],
        parent_bound: &PartitionBoundSpec,
    ) -> bool {
        let parent_context = ParentPartitionPruningContext {
            spec: spec.clone(),
            bound: parent_bound.clone(),
            sibling_bounds: Vec::new(),
        };
        super::expr_may_match_bound(
            expr,
            spec,
            bound,
            sibling_bounds,
            Some(&parent_context),
            None,
        )
    }

    fn expr_may_match_bound_with_parent_context(
        expr: &Expr,
        spec: &LoweredPartitionSpec,
        bound: &PartitionBoundSpec,
        sibling_bounds: &[PartitionBoundSpec],
        parent_context: &ParentPartitionPruningContext,
    ) -> bool {
        super::expr_may_match_bound(
            expr,
            spec,
            bound,
            sibling_bounds,
            Some(parent_context),
            None,
        )
    }

    fn list_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::List,
            key_columns: vec!["a".into()],
            key_exprs: vec![key_expr()],
            key_types: vec![SqlType::new(SqlTypeKind::Text)],
            key_sqls: vec!["a".into()],
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
        }
    }

    fn range_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: vec!["a".into()],
            key_exprs: vec![int_key_expr(1)],
            key_types: vec![SqlType::new(SqlTypeKind::Int4)],
            key_sqls: vec!["a".into()],
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
        }
    }

    fn hash_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::Hash,
            key_columns: vec!["a".into(), "b".into()],
            key_exprs: vec![int_key_att_expr(1, 1), text_key_att_expr(1, 2)],
            key_types: vec![
                SqlType::new(SqlTypeKind::Int4),
                SqlType::new(SqlTypeKind::Text),
            ],
            key_sqls: vec!["a".into(), "b".into()],
            partattrs: vec![1, 2],
            partclass: vec![0, 0],
            partcollation: vec![0, 0],
        }
    }

    fn range_spec_for_attrs(attrs: &[i32]) -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: attrs.iter().map(|attr| format!("a{attr}")).collect(),
            key_exprs: attrs
                .iter()
                .map(|attr| int_key_att_expr(1, *attr))
                .collect(),
            key_types: vec![SqlType::new(SqlTypeKind::Int4); attrs.len()],
            key_sqls: attrs.iter().map(|attr| format!("a{attr}")).collect(),
            partattrs: attrs.iter().map(|attr| *attr as i16).collect(),
            partclass: vec![0; attrs.len()],
            partcollation: vec![0; attrs.len()],
        }
    }

    fn bool_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::List,
            key_columns: vec!["a".into()],
            key_exprs: vec![bool_key_expr()],
            key_types: vec![SqlType::new(SqlTypeKind::Bool)],
            key_sqls: vec!["a".into()],
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
        }
    }

    fn key_expr() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
        })
    }

    fn int_key_expr(varno: usize) -> Expr {
        int_key_att_expr(varno, 1)
    }

    fn int_key_att_expr(varno: usize, varattno: i32) -> Expr {
        Expr::Var(Var {
            varno,
            varattno,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
        })
    }

    fn text_key_att_expr(varno: usize, varattno: i32) -> Expr {
        Expr::Var(Var {
            varno,
            varattno,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
        })
    }

    fn bool_key_expr() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Bool),
        })
    }

    fn text_const(value: &str) -> Expr {
        Expr::Const(Value::Text(value.into()))
    }

    fn text_bound(values: &[&str]) -> PartitionBoundSpec {
        PartitionBoundSpec::List {
            values: values
                .iter()
                .map(|value| SerializedPartitionValue::Text((*value).into()))
                .collect(),
            is_default: false,
        }
    }

    fn int_range_bound(
        from: PartitionRangeDatumValue,
        to: PartitionRangeDatumValue,
    ) -> PartitionBoundSpec {
        PartitionBoundSpec::Range {
            from: vec![from],
            to: vec![to],
            is_default: false,
        }
    }

    fn int_range_bound_multi(
        from: Vec<PartitionRangeDatumValue>,
        to: Vec<PartitionRangeDatumValue>,
    ) -> PartitionBoundSpec {
        PartitionBoundSpec::Range {
            from,
            to,
            is_default: false,
        }
    }

    fn int_range_default_bound() -> PartitionBoundSpec {
        PartitionBoundSpec::Range {
            from: Vec::new(),
            to: Vec::new(),
            is_default: true,
        }
    }

    fn int_value(value: i32) -> PartitionRangeDatumValue {
        PartitionRangeDatumValue::Value(SerializedPartitionValue::Int32(value))
    }

    fn int_cmp(op: OpExprKind, value: i32) -> Expr {
        Expr::op_auto(op, vec![int_key_expr(42), Expr::Const(Value::Int32(value))])
    }

    fn int_att_cmp(attr: i32, op: OpExprKind, value: i32) -> Expr {
        Expr::op_auto(
            op,
            vec![int_key_att_expr(42, attr), Expr::Const(Value::Int32(value))],
        )
    }

    fn int_cmp_with_const(op: OpExprKind, value: Expr) -> Expr {
        Expr::op_auto(op, vec![int_key_expr(42), value])
    }

    fn null_bound() -> PartitionBoundSpec {
        PartitionBoundSpec::List {
            values: vec![SerializedPartitionValue::Null],
            is_default: false,
        }
    }

    fn bool_bound(value: bool) -> PartitionBoundSpec {
        PartitionBoundSpec::List {
            values: vec![SerializedPartitionValue::Bool(value)],
            is_default: false,
        }
    }

    fn list_default_bound() -> PartitionBoundSpec {
        PartitionBoundSpec::List {
            values: Vec::new(),
            is_default: true,
        }
    }

    fn hash_bound(modulus: i32, remainder: i32) -> PartitionBoundSpec {
        PartitionBoundSpec::Hash { modulus, remainder }
    }

    fn cmp(op: OpExprKind, value: &str) -> Expr {
        Expr::op_auto(op, vec![key_expr(), text_const(value)])
    }

    fn scalar_array(op: SubqueryComparisonOp, use_or: bool, values: &[&str]) -> Expr {
        Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op,
            use_or,
            left: Box::new(key_expr()),
            right: Box::new(Expr::ArrayLiteral {
                elements: values.iter().map(|value| text_const(value)).collect(),
                array_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
            }),
            collation_oid: None,
        }))
    }

    fn int_scalar_array(op: SubqueryComparisonOp, use_or: bool, values: &[i32]) -> Expr {
        Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op,
            use_or,
            left: Box::new(int_key_expr(42)),
            right: Box::new(Expr::ArrayLiteral {
                elements: values
                    .iter()
                    .map(|value| Expr::Const(Value::Int32(*value)))
                    .collect(),
                array_type: SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            }),
            collation_oid: None,
        }))
    }

    fn hash_expr(a: i32, b: Option<&str>) -> Expr {
        Expr::and(
            Expr::op_auto(
                OpExprKind::Eq,
                vec![int_key_att_expr(42, 1), Expr::Const(Value::Int32(a))],
            ),
            match b {
                Some(value) => Expr::op_auto(
                    OpExprKind::Eq,
                    vec![
                        text_key_att_expr(42, 2),
                        Expr::Const(Value::Text(value.into())),
                    ],
                ),
                None => Expr::IsNull(Box::new(text_key_att_expr(42, 2))),
            },
        )
    }

    fn hash_remainder(values: &[Value], modulus: i32) -> i32 {
        (crate::backend::access::hash::support::hash_values_combined(values, &[0, 0]).unwrap()
            % modulus as u64) as i32
    }

    fn relation(
        relation_oid: u32,
        relkind: char,
        partition_spec: Option<LoweredPartitionSpec>,
        bound: Option<PartitionBoundSpec>,
    ) -> BoundRelation {
        relation_with_desc(
            relation_oid,
            relkind,
            partition_spec,
            bound,
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "a",
                    SqlType::new(SqlTypeKind::Int4),
                    true,
                )],
            },
        )
    }

    fn relation_with_desc(
        relation_oid: u32,
        relkind: char,
        partition_spec: Option<LoweredPartitionSpec>,
        bound: Option<PartitionBoundSpec>,
        desc: RelationDesc,
    ) -> BoundRelation {
        BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: CURRENT_DATABASE_OID,
                rel_number: relation_oid,
            },
            relation_oid,
            toast: None,
            namespace_oid: PUBLIC_NAMESPACE_OID,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind,
            relispopulated: true,
            relispartition: bound.is_some(),
            relpartbound: bound.map(|bound| serialize_partition_bound(&bound).unwrap()),
            desc,
            partitioned_table: partition_spec
                .as_ref()
                .map(|spec| pg_partitioned_table_row(relation_oid, spec, 0)),
            partition_spec,
        }
    }

    fn inherit(parent_oid: u32, child_oid: u32, seqno: i32) -> PgInheritsRow {
        PgInheritsRow {
            inhrelid: child_oid,
            inhparent: parent_oid,
            inhseqno: seqno,
            inhdetachpending: false,
        }
    }

    #[test]
    fn explicit_list_partition_and_requires_one_value_to_satisfy_all_clauses() {
        let spec = list_spec();
        let bound = text_bound(&["a", "d"]);
        let expr = Expr::and(cmp(OpExprKind::Gt, "a"), cmp(OpExprKind::Lt, "d"));

        assert!(!expr_may_match_bound(&expr, &spec, &bound, &[]));

        let expr = Expr::and(cmp(OpExprKind::Gt, "a"), cmp(OpExprKind::LtEq, "d"));
        assert!(expr_may_match_bound(&expr, &spec, &bound, &[]));
    }

    #[test]
    fn strict_list_comparisons_do_not_match_null_partition_value() {
        let spec = list_spec();
        let bound = null_bound();

        assert!(!expr_may_match_bound(
            &cmp(OpExprKind::NotEq, "g"),
            &spec,
            &bound,
            &[]
        ));
        assert!(!expr_may_match_bound(
            &cmp(OpExprKind::Gt, "a"),
            &spec,
            &bound,
            &[]
        ));
        assert!(expr_may_match_bound(
            &Expr::IsNull(Box::new(key_expr())),
            &spec,
            &bound,
            &[]
        ));
        assert!(!expr_may_match_bound(
            &Expr::IsNotNull(Box::new(key_expr())),
            &spec,
            &bound,
            &[]
        ));
    }

    #[test]
    fn scalar_array_all_uses_and_semantics_for_explicit_list_values() {
        let spec = list_spec();
        let bound = text_bound(&["a", "d"]);
        let not_in_ad = scalar_array(SubqueryComparisonOp::NotEq, false, &["a", "d"]);
        let not_in_ax = scalar_array(SubqueryComparisonOp::NotEq, false, &["a", "x"]);

        assert!(!expr_may_match_bound(&not_in_ad, &spec, &bound, &[]));
        assert!(expr_may_match_bound(&not_in_ax, &spec, &bound, &[]));
    }

    #[test]
    fn range_default_is_pruned_when_query_interval_is_fully_covered_by_siblings() {
        let spec = range_spec();
        let default_bound = int_range_default_bound();
        let siblings = vec![
            int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1)),
            int_range_bound(int_value(1), int_value(10)),
            default_bound.clone(),
        ];

        assert!(!expr_may_match_bound(
            &int_cmp(OpExprKind::Lt, 1),
            &spec,
            &default_bound,
            &siblings
        ));
        assert!(!expr_may_match_bound(
            &int_cmp(OpExprKind::LtEq, 1),
            &spec,
            &default_bound,
            &siblings
        ));
    }

    #[test]
    fn range_default_uses_parent_bound_when_pruning_subpartitions() {
        let spec = range_spec();
        let default_bound = int_range_default_bound();
        let siblings = vec![
            int_range_bound(int_value(31), int_value(40)),
            default_bound.clone(),
        ];
        let parent_bound = int_range_bound(int_value(31), PartitionRangeDatumValue::MaxValue);

        assert!(expr_may_match_bound(
            &int_cmp(OpExprKind::LtEq, 31),
            &spec,
            &default_bound,
            &siblings
        ));
        assert!(!expr_may_match_bound_with_parent(
            &int_cmp(OpExprKind::LtEq, 31),
            &spec,
            &default_bound,
            &siblings,
            &parent_bound,
        ));
    }

    #[test]
    fn range_default_multicolumn_prunes_when_all_key_bounds_are_covered() {
        let spec = range_spec_for_attrs(&[1, 2, 3]);
        let default_bound = int_range_default_bound();
        let siblings = vec![
            int_range_bound_multi(
                vec![
                    PartitionRangeDatumValue::MinValue,
                    PartitionRangeDatumValue::MinValue,
                    PartitionRangeDatumValue::MinValue,
                ],
                vec![int_value(1), int_value(1), int_value(1)],
            ),
            int_range_bound_multi(
                vec![int_value(1), int_value(1), int_value(1)],
                vec![int_value(10), int_value(5), int_value(10)],
            ),
            default_bound.clone(),
        ];
        let covered = Expr::and(
            Expr::and(
                int_att_cmp(1, OpExprKind::Eq, 1),
                int_att_cmp(2, OpExprKind::Eq, 1),
            ),
            int_att_cmp(3, OpExprKind::Lt, 8),
        );
        let partial = Expr::and(
            int_att_cmp(1, OpExprKind::Eq, 1),
            int_att_cmp(2, OpExprKind::Eq, 1),
        );

        assert!(!expr_may_match_bound(
            &covered,
            &spec,
            &default_bound,
            &siblings,
        ));
        assert!(expr_may_match_bound(
            &partial,
            &spec,
            &default_bound,
            &siblings,
        ));
    }

    #[test]
    fn range_default_multicolumn_prunes_simple_prefix_interval() {
        let spec = range_spec_for_attrs(&[1, 2]);
        let default_bound = int_range_default_bound();
        let siblings = vec![
            int_range_bound_multi(
                vec![int_value(2), PartitionRangeDatumValue::MinValue],
                vec![int_value(2), int_value(1)],
            ),
            default_bound.clone(),
        ];
        let expr = Expr::and(
            int_att_cmp(1, OpExprKind::Eq, 2),
            int_att_cmp(2, OpExprKind::Lt, 1),
        );

        assert!(!expr_may_match_bound(
            &expr,
            &spec,
            &default_bound,
            &siblings,
        ));
    }

    #[test]
    fn or_pruning_ignores_arms_excluded_by_parent_bound() {
        let child_spec = list_spec();
        let ef_bound = text_bound(&["ef"]);
        let ab_bound = text_bound(&["ab"]);
        let child_siblings = vec![ab_bound.clone(), ef_bound.clone()];
        let parent_context = ParentPartitionPruningContext {
            spec: LoweredPartitionSpec {
                strategy: PartitionStrategy::Range,
                key_columns: vec!["a".into()],
                key_exprs: vec![int_key_att_expr(1, 2)],
                key_types: vec![SqlType::new(SqlTypeKind::Int4)],
                key_sqls: vec!["a".into()],
                partattrs: vec![2],
                partclass: vec![0],
                partcollation: vec![0],
            },
            bound: int_range_bound(int_value(15), int_value(20)),
            sibling_bounds: vec![int_range_bound(int_value(15), int_value(20))],
        };
        let expr = Expr::or(
            Expr::op_auto(
                OpExprKind::Eq,
                vec![int_key_att_expr(42, 2), Expr::Const(Value::Int32(1))],
            ),
            cmp(OpExprKind::Eq, "ab"),
        );

        assert!(expr_may_match_bound(
            &expr,
            &child_spec,
            &ef_bound,
            &child_siblings,
        ));
        assert!(!expr_may_match_bound_with_parent_context(
            &expr,
            &child_spec,
            &ef_bound,
            &child_siblings,
            &parent_context,
        ));
        assert!(expr_may_match_bound_with_parent_context(
            &expr,
            &child_spec,
            &ab_bound,
            &child_siblings,
            &parent_context,
        ));
    }

    #[test]
    fn range_default_is_kept_for_gaps_nulls_and_default_only_values() {
        let spec = range_spec();
        let default_bound = int_range_default_bound();
        let siblings = vec![
            int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1)),
            int_range_bound(int_value(1), int_value(10)),
            int_range_bound(int_value(31), PartitionRangeDatumValue::MaxValue),
            default_bound.clone(),
        ];

        for expr in [
            int_cmp(OpExprKind::Eq, 10),
            int_cmp(OpExprKind::LtEq, 10),
            int_cmp(OpExprKind::Gt, 30),
            Expr::IsNull(Box::new(int_key_expr(42))),
        ] {
            assert!(expr_may_match_bound(
                &expr,
                &spec,
                &default_bound,
                &siblings
            ));
        }
    }

    #[test]
    fn own_partition_bound_prunes_contradictory_subpartitioned_relation() {
        let spec = range_spec();
        let parent_oid = 10;
        let child_oid = 20;
        let child_bound = int_range_bound(int_value(15), int_value(20));
        let catalog = MockCatalog {
            relations: vec![
                relation(parent_oid, 'p', Some(spec), None),
                relation(child_oid, 'p', None, Some(child_bound)),
            ],
            inherits: vec![inherit(parent_oid, child_oid, 1)],
        };

        assert!(!relation_may_satisfy_own_partition_bound(
            &catalog,
            child_oid,
            Some(&int_cmp(OpExprKind::Eq, 20))
        ));
        assert!(relation_may_satisfy_own_partition_bound(
            &catalog,
            child_oid,
            Some(&int_cmp(OpExprKind::Eq, 16))
        ));
    }

    #[test]
    fn own_partition_bound_maps_parent_key_to_child_column() {
        let spec = range_spec();
        let parent_oid = 10;
        let child_oid = 20;
        let child_bound = int_range_bound(int_value(15), int_value(20));
        let child_desc = RelationDesc {
            columns: vec![
                crate::backend::catalog::catalog::column_desc(
                    "b",
                    SqlType::new(SqlTypeKind::Text),
                    true,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "a",
                    SqlType::new(SqlTypeKind::Int4),
                    true,
                ),
            ],
        };
        let catalog = MockCatalog {
            relations: vec![
                relation(parent_oid, 'p', Some(spec), None),
                relation_with_desc(child_oid, 'p', None, Some(child_bound), child_desc),
            ],
            inherits: vec![inherit(parent_oid, child_oid, 1)],
        };
        let child_a_eq_20 = Expr::op_auto(
            OpExprKind::Eq,
            vec![int_key_att_expr(99, 2), Expr::Const(Value::Int32(20))],
        );
        let child_a_eq_16 = Expr::op_auto(
            OpExprKind::Eq,
            vec![int_key_att_expr(99, 2), Expr::Const(Value::Int32(16))],
        );

        assert!(!relation_may_satisfy_own_partition_bound(
            &catalog,
            child_oid,
            Some(&child_a_eq_20)
        ));
        assert!(relation_may_satisfy_own_partition_bound(
            &catalog,
            child_oid,
            Some(&child_a_eq_16)
        ));
    }

    #[test]
    fn scalar_array_any_prunes_range_partitions() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let second = int_range_bound(int_value(1), int_value(10));
        let expr = int_scalar_array(SubqueryComparisonOp::Eq, true, &[1, 7]);

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
    }

    #[test]
    fn scalar_array_null_never_matches_partitions() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let expr = Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op: SubqueryComparisonOp::Eq,
            use_or: true,
            left: Box::new(int_key_expr(42)),
            right: Box::new(Expr::Cast(
                Box::new(Expr::Const(Value::Null)),
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            )),
            collation_oid: None,
        }));

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
    }

    #[test]
    fn range_pruning_rejects_numeric_cast_on_integer_key() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let second = int_range_bound(int_value(1), int_value(10));
        let expr = Expr::op_auto(
            OpExprKind::Eq,
            vec![
                Expr::Cast(
                    Box::new(int_key_expr(42)),
                    SqlType::new(SqlTypeKind::Numeric),
                ),
                Expr::Const(Value::Numeric(
                    crate::include::nodes::datum::NumericValue::from_i64(1),
                )),
            ],
        );

        assert!(expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
    }

    #[test]
    fn range_pruning_keeps_safe_integer_casted_constants() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let second = int_range_bound(int_value(1), int_value(10));
        let expr = int_cmp_with_const(
            OpExprKind::Eq,
            Expr::Cast(
                Box::new(Expr::Const(Value::Int32(1))),
                SqlType::new(SqlTypeKind::Int8),
            ),
        );

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
    }

    #[test]
    fn or_arm_contradictions_do_not_over_prune_range_partitions() {
        let spec = range_spec();
        let second = int_range_bound(int_value(1), int_value(10));
        let third = int_range_bound(int_value(15), int_value(20));
        let expr = Expr::or(
            Expr::and(int_cmp(OpExprKind::Eq, 1), int_cmp(OpExprKind::Eq, 3)),
            Expr::and(int_cmp(OpExprKind::Gt, 1), int_cmp(OpExprKind::Eq, 15)),
        );

        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &third, &[]));
    }

    #[test]
    fn hash_pruning_uses_full_key_equality_and_null_constraints() {
        let spec = hash_spec();
        let expr = hash_expr(1, None);
        let matching_remainder = hash_remainder(&[Value::Int32(1), Value::Null], 4);
        let nonmatching_remainder = (matching_remainder + 1) % 4;

        assert!(expr_may_match_bound(
            &expr,
            &spec,
            &hash_bound(4, matching_remainder),
            &[]
        ));
        assert!(!expr_may_match_bound(
            &expr,
            &spec,
            &hash_bound(4, nonmatching_remainder),
            &[]
        ));

        let partial_key_expr = Expr::op_auto(
            OpExprKind::Eq,
            vec![int_key_att_expr(42, 1), Expr::Const(Value::Int32(1))],
        );
        assert!(expr_may_match_bound(
            &partial_key_expr,
            &spec,
            &hash_bound(4, nonmatching_remainder),
            &[]
        ));
    }

    #[test]
    fn hash_pruning_uses_full_key_constraints_inside_or_arms() {
        let spec = hash_spec();
        let first = hash_remainder(&[Value::Int32(1), Value::Null], 4);
        let second = hash_remainder(&[Value::Int32(2), Value::Text("xxx".into())], 4);
        let expr = Expr::or(hash_expr(1, None), hash_expr(2, Some("xxx")));

        for remainder in 0..4 {
            let may_match = expr_may_match_bound(&expr, &spec, &hash_bound(4, remainder), &[]);
            assert_eq!(
                may_match,
                remainder == first || remainder == second,
                "unexpected hash pruning result for remainder {remainder}"
            );
        }
    }

    #[test]
    fn bool_distinctness_prunes_list_partitions() {
        let spec = bool_spec();
        let true_bound = bool_bound(true);
        let false_bound = bool_bound(false);
        let null_bound = null_bound();
        let default_bound = list_default_bound();
        let siblings = vec![
            true_bound.clone(),
            false_bound.clone(),
            null_bound.clone(),
            default_bound.clone(),
        ];
        let is_not_true = Expr::IsDistinctFrom(
            Box::new(bool_key_expr()),
            Box::new(Expr::Const(Value::Bool(true))),
        );
        let is_false = Expr::IsNotDistinctFrom(
            Box::new(bool_key_expr()),
            Box::new(Expr::Const(Value::Bool(false))),
        );
        let false_and_unknown =
            Expr::and(is_false.clone(), Expr::IsNull(Box::new(bool_key_expr())));

        assert!(!expr_may_match_bound(
            &is_not_true,
            &spec,
            &true_bound,
            &siblings
        ));
        assert!(expr_may_match_bound(
            &is_not_true,
            &spec,
            &false_bound,
            &siblings
        ));
        assert!(expr_may_match_bound(
            &is_not_true,
            &spec,
            &null_bound,
            &siblings
        ));
        assert!(!expr_may_match_bound(
            &is_not_true,
            &spec,
            &default_bound,
            &siblings
        ));
        assert!(!expr_may_match_bound(
            &false_and_unknown,
            &spec,
            &null_bound,
            &siblings
        ));
    }
}
