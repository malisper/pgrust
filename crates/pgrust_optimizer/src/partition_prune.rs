use std::cmp::Ordering;

use crate::runtime::{
    DateTimeConfig, cast_value_with_source_type_catalog_and_config, compare_order_values,
};
use pgrust_analyze::{
    CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec, PartitionRangeDatumValue,
    SerializedPartitionValue, deserialize_partition_bound, partition_value_to_value,
    relation_partition_spec,
};
use pgrust_catalog_data::sql_type_oid;
use pgrust_catalog_data::{
    ANYARRAYOID, ANYOID, PG_LANGUAGE_SQL_OID, builtin_scalar_function_for_proc_oid,
};
use pgrust_nodes::datum::Value;
use pgrust_nodes::parsenodes::{PartitionStrategy, SqlType, SqlTypeKind, SubqueryComparisonOp};
use pgrust_nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, OpExpr, OpExprKind, RelationDesc,
    ScalarFunctionImpl, attrno_index, expr_sql_type_hint, user_attrno,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartitionPruneValueMode {
    Static,
    Runtime,
}

pub(super) fn partition_may_satisfy_filter(
    spec: Option<&LoweredPartitionSpec>,
    bound: Option<&PartitionBoundSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    filter: Option<&Expr>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    partition_may_satisfy_filter_with_ancestor_bound(
        spec,
        bound,
        sibling_bounds,
        None,
        filter,
        catalog,
    )
}

pub(super) fn partition_may_satisfy_filter_with_ancestor_bound(
    spec: Option<&LoweredPartitionSpec>,
    bound: Option<&PartitionBoundSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
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
    expr_may_match_bound(
        filter,
        spec,
        bound,
        sibling_bounds,
        ancestor_bound,
        catalog,
        None,
        PartitionPruneValueMode::Static,
    )
}

pub(super) fn partition_may_satisfy_filter_for_relation(
    spec: Option<&LoweredPartitionSpec>,
    bound: Option<&PartitionBoundSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
    filter: Option<&Expr>,
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
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
    expr_may_match_bound(
        filter,
        spec,
        bound,
        sibling_bounds,
        ancestor_bound,
        Some(catalog),
        Some(relation_oid),
        PartitionPruneValueMode::Static,
    )
}

pub fn partition_may_satisfy_filter_with_runtime_values(
    spec: &LoweredPartitionSpec,
    bound: Option<&PartitionBoundSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    filter: &Expr,
    catalog: Option<&dyn CatalogLookup>,
    mut eval_runtime_value: impl FnMut(&Expr) -> Option<Value>,
) -> bool {
    let Some(bound) = bound else {
        return true;
    };
    let filter = substitute_runtime_prune_values(filter, &mut eval_runtime_value);
    expr_may_match_bound(
        &filter,
        spec,
        bound,
        sibling_bounds,
        None,
        catalog,
        None,
        PartitionPruneValueMode::Runtime,
    )
}

fn substitute_runtime_prune_values(
    expr: &Expr,
    eval_runtime_value: &mut impl FnMut(&Expr) -> Option<Value>,
) -> Expr {
    if !expr_contains_tuple_var(expr)
        && !matches!(expr, Expr::Random)
        && let Some(value) = eval_runtime_value(expr)
    {
        return Expr::Const(value);
    }
    match expr {
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(pgrust_nodes::primnodes::BoolExpr {
            boolop: bool_expr.boolop,
            args: bool_expr
                .args
                .iter()
                .map(|arg| substitute_runtime_prune_values(arg, eval_runtime_value))
                .collect(),
        })),
        Expr::Op(op) => {
            let mut op = op.clone();
            op.args = op
                .args
                .iter()
                .map(|arg| substitute_runtime_prune_values(arg, eval_runtime_value))
                .collect();
            Expr::Op(op)
        }
        Expr::ScalarArrayOp(saop) => {
            Expr::ScalarArrayOp(Box::new(pgrust_nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(substitute_runtime_prune_values(
                    &saop.left,
                    eval_runtime_value,
                )),
                op: saop.op,
                use_or: saop.use_or,
                right: Box::new(substitute_runtime_prune_values(
                    &saop.right,
                    eval_runtime_value,
                )),
                collation_oid: saop.collation_oid,
            }))
        }
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(substitute_runtime_prune_values(inner, eval_runtime_value)),
            *ty,
        ),
        Expr::Collate {
            expr: inner,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(substitute_runtime_prune_values(inner, eval_runtime_value)),
            collation_oid: *collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_runtime_prune_values(
            inner,
            eval_runtime_value,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(substitute_runtime_prune_values(
            inner,
            eval_runtime_value,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(substitute_runtime_prune_values(left, eval_runtime_value)),
            Box::new(substitute_runtime_prune_values(right, eval_runtime_value)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(substitute_runtime_prune_values(left, eval_runtime_value)),
            Box::new(substitute_runtime_prune_values(right, eval_runtime_value)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| substitute_runtime_prune_values(element, eval_runtime_value))
                .collect(),
            array_type: *array_type,
        },
        Expr::Func(func) => {
            let mut func = func.clone();
            func.args = func
                .args
                .iter()
                .map(|arg| substitute_runtime_prune_values(arg, eval_runtime_value))
                .collect();
            Expr::Func(func)
        }
        other => other.clone(),
    }
}

fn expr_contains_tuple_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(_) => true,
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_tuple_var),
        Expr::Op(op) => op.args.iter().any(expr_contains_tuple_var),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_tuple_var(&saop.left) || expr_contains_tuple_var(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_tuple_var(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_tuple_var(left) || expr_contains_tuple_var(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_tuple_var),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_tuple_var(expr)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_tuple_var(expr))
                || case_expr.args.iter().any(|when| {
                    expr_contains_tuple_var(&when.expr) || expr_contains_tuple_var(&when.result)
                })
                || expr_contains_tuple_var(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_tuple_var),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_tuple_var),
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
            expr_contains_tuple_var(expr)
                || expr_contains_tuple_var(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_tuple_var(expr))
        }
        Expr::FieldSelect { expr, .. } => expr_contains_tuple_var(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_tuple_var(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_tuple_var)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_tuple_var)
                })
        }
        _ => false,
    }
}

pub fn relation_may_satisfy_own_partition_bound(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    filter: Option<&Expr>,
) -> bool {
    relation_may_satisfy_own_partition_bound_inner(catalog, relation_oid, filter, false)
}

fn relation_may_satisfy_own_partition_bound_inner(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    filter: Option<&Expr>,
    inside_or_arm: bool,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if let Expr::Bool(bool_expr) = filter
        && bool_expr.boolop == BoolExprType::Or
    {
        return bool_expr.args.iter().any(|arg| {
            relation_may_satisfy_own_partition_bound_inner(catalog, relation_oid, Some(arg), true)
        });
    }
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
            expr_may_match_bound_inner(
                filter,
                &spec,
                &bound,
                &sibling_bounds,
                None,
                Some(catalog),
                None,
                PartitionPruneValueMode::Static,
                inside_or_arm,
            ) && relation_may_satisfy_own_partition_bound_inner(
                catalog,
                row.inhparent,
                Some(filter),
                inside_or_arm,
            )
        })
}

pub fn relation_may_satisfy_own_partition_bound_with_runtime_values(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    filter: Option<&Expr>,
    eval_runtime_value: &mut dyn FnMut(&Expr) -> Option<Value>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if let Expr::Bool(bool_expr) = filter
        && bool_expr.boolop == BoolExprType::Or
    {
        return bool_expr.args.iter().any(|arg| {
            relation_may_satisfy_own_partition_bound_with_runtime_values(
                catalog,
                relation_oid,
                Some(arg),
                eval_runtime_value,
            )
        });
    }
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
            partition_may_satisfy_filter_with_runtime_values(
                &spec,
                Some(&bound),
                &sibling_bounds,
                filter,
                Some(catalog),
                |expr| eval_runtime_value(expr),
            ) && relation_may_satisfy_own_partition_bound_with_runtime_values(
                catalog,
                row.inhparent,
                Some(filter),
                eval_runtime_value,
            )
        })
}

fn partition_spec_for_relation_filter(
    spec: &pgrust_analyze::LoweredPartitionSpec,
    parent_desc: &RelationDesc,
    relation_desc: &RelationDesc,
) -> Option<pgrust_analyze::LoweredPartitionSpec> {
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
    Some(Expr::Var(pgrust_nodes::primnodes::Var {
        varno: var.varno,
        varattno: user_attrno(relation_index),
        varlevelsup: var.varlevelsup,
        vartype: parent_column.sql_type.clone(),
        collation_oid: None,
    }))
}

fn expr_may_match_bound(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
    catalog: Option<&dyn CatalogLookup>,
    relation_oid: Option<u32>,
    value_mode: PartitionPruneValueMode,
) -> bool {
    expr_may_match_bound_inner(
        expr,
        spec,
        bound,
        sibling_bounds,
        ancestor_bound,
        catalog,
        relation_oid,
        value_mode,
        false,
    )
}

fn expr_may_match_bound_inner(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
    catalog: Option<&dyn CatalogLookup>,
    relation_oid: Option<u32>,
    value_mode: PartitionPruneValueMode,
    inside_or_arm: bool,
) -> bool {
    if let (Some(catalog), Some(relation_oid)) = (catalog, relation_oid)
        && !relation_may_satisfy_own_partition_bound_inner(
            catalog,
            relation_oid,
            Some(expr),
            inside_or_arm,
        )
    {
        return false;
    }
    if let Some(result) =
        explicit_list_bound_may_match_expr(expr, spec, bound, catalog, relation_oid, value_mode)
    {
        return result;
    }
    if let Some((key_index, value)) = partition_key_bool_equality_predicate(expr, spec) {
        return bound_may_contain_value(
            spec,
            bound,
            sibling_bounds,
            key_index,
            &Value::Bool(value),
            catalog,
            ancestor_bound,
        );
    }
    match expr {
        Expr::Const(Value::Bool(value)) => *value,
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => {
                let range_mode = if inside_or_arm {
                    RangeConstraintMode::PermissiveConflicts
                } else {
                    RangeConstraintMode::Strict
                };
                let range_result = range_may_satisfy_conjunction(
                    expr,
                    spec,
                    bound,
                    sibling_bounds,
                    ancestor_bound,
                    range_mode,
                )
                .unwrap_or(true);
                let hash_result =
                    hash_may_satisfy_conjunction(expr, spec, bound, catalog).unwrap_or(true);
                range_result
                    && hash_result
                    && bool_expr.args.iter().all(|arg| {
                        if inside_or_arm && range_conjunct_applies_to_spec(arg, spec) {
                            // :HACK: PostgreSQL's OR-arm pruning relies on the
                            // combined range clause set and does not reprove
                            // every individual contradictory range conjunct.
                            return true;
                        }
                        expr_may_match_bound_inner(
                            arg,
                            spec,
                            bound,
                            sibling_bounds,
                            ancestor_bound,
                            catalog,
                            relation_oid,
                            value_mode,
                            inside_or_arm,
                        )
                    })
            }
            BoolExprType::Or => bool_expr.args.iter().any(|arg| {
                expr_may_match_bound_inner(
                    arg,
                    spec,
                    bound,
                    sibling_bounds,
                    ancestor_bound,
                    catalog,
                    relation_oid,
                    value_mode,
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
                    index,
                    &Value::Null,
                    catalog,
                    ancestor_bound,
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
            let collation_oid = op_pruning_collation(op);
            let Some((key_index, key_on_left, value)) =
                partition_key_const_cmp(left, right, spec, collation_oid, catalog, value_mode)
            else {
                return true;
            };
            bound_may_satisfy_comparison(
                spec,
                bound,
                sibling_bounds,
                key_index,
                key_on_left,
                op.op,
                &value,
                collation_oid,
                catalog,
                ancestor_bound,
            )
        }
        Expr::IsDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, value)| {
                bound_may_satisfy_distinctness(spec, bound, sibling_bounds, key_index, &value, true)
            })
            .unwrap_or(true),
        Expr::IsNotDistinctFrom(left, right) => partition_key_const_distinct_cmp(left, right, spec)
            .map(|(key_index, value)| {
                bound_may_satisfy_distinctness(
                    spec,
                    bound,
                    sibling_bounds,
                    key_index,
                    &value,
                    false,
                )
            })
            .unwrap_or(true),
        Expr::ScalarArrayOp(saop) => {
            let Some((key_index, op, values)) = partition_key_const_array_cmp(
                &saop.left,
                &saop.right,
                spec,
                saop.op,
                saop.collation_oid,
                catalog,
                value_mode,
            ) else {
                return true;
            };
            if saop.use_or {
                values.into_iter().any(|value| {
                    bound_may_satisfy_comparison(
                        spec,
                        bound,
                        sibling_bounds,
                        key_index,
                        true,
                        op,
                        &value,
                        saop.collation_oid,
                        catalog,
                        ancestor_bound,
                    )
                })
            } else {
                values.into_iter().all(|value| {
                    bound_may_satisfy_comparison(
                        spec,
                        bound,
                        sibling_bounds,
                        key_index,
                        true,
                        op,
                        &value,
                        saop.collation_oid,
                        catalog,
                        ancestor_bound,
                    )
                })
            }
        }
        _ => true,
    }
}

fn range_conjunct_applies_to_spec(expr: &Expr, spec: &LoweredPartitionSpec) -> bool {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return false;
    }
    match expr {
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => partition_key_index(inner, spec).is_some(),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return false;
            };
            let collation_oid = op_pruning_collation(op);
            partition_key_const_cmp(
                left,
                right,
                spec,
                collation_oid,
                None,
                PartitionPruneValueMode::Static,
            )
            .is_some()
        }
        _ => partition_key_bool_equality_predicate(expr, spec).is_some(),
    }
}

fn explicit_list_bound_may_match_expr(
    expr: &Expr,
    spec: &pgrust_analyze::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    catalog: Option<&dyn CatalogLookup>,
    relation_oid: Option<u32>,
    value_mode: PartitionPruneValueMode,
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
    Some(values.iter().any(|value| {
        list_value_may_match_expr(value, expr, spec, catalog, relation_oid, value_mode)
    }))
}

fn list_value_may_match_expr(
    value: &SerializedPartitionValue,
    expr: &Expr,
    spec: &pgrust_analyze::LoweredPartitionSpec,
    catalog: Option<&dyn CatalogLookup>,
    relation_oid: Option<u32>,
    value_mode: PartitionPruneValueMode,
) -> bool {
    if let (Some(catalog), Some(relation_oid)) = (catalog, relation_oid)
        && !relation_may_satisfy_own_partition_bound(catalog, relation_oid, Some(expr))
    {
        return false;
    }
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
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => bool_expr.args.iter().all(|arg| {
                list_value_may_match_expr(value, arg, spec, catalog, relation_oid, value_mode)
            }),
            BoolExprType::Or => bool_expr.args.iter().any(|arg| {
                list_value_may_match_expr(value, arg, spec, catalog, relation_oid, value_mode)
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
            let collation_oid = op_pruning_collation(op);
            let Some((key_index, key_on_left, constant)) =
                partition_key_const_cmp(left, right, spec, collation_oid, catalog, value_mode)
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
                catalog,
                value_mode,
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
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<(usize, bool, Value)> {
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        let key_collation_oid = spec.partcollation.get(index).copied().unwrap_or(0);
        if collation_mismatch(query_collation_oid, key_collation_oid) {
            continue;
        }
        let key_type = spec.key_types.get(index).copied();
        if partition_key_expr_matches(left, key_expr, value_mode).is_some()
            && let Some(value) = const_value_for_partition_key(right, key_type, catalog, value_mode)
        {
            return Some((index, true, value));
        }
        if partition_key_expr_matches(right, key_expr, value_mode).is_some()
            && let Some(value) = const_value_for_partition_key(left, key_type, catalog, value_mode)
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
        if partition_key_expr_matches(left, key_expr, PartitionPruneValueMode::Static).is_some()
            && let Some(value) = const_value(right)
        {
            return Some((index, value));
        }
        if partition_key_expr_matches(right, key_expr, PartitionPruneValueMode::Static).is_some()
            && let Some(value) = const_value(left)
        {
            return Some((index, value));
        }
    }
    None
}

fn partition_key_index(expr: &Expr, spec: &LoweredPartitionSpec) -> Option<usize> {
    spec.key_exprs.iter().position(|key_expr| {
        partition_key_expr_matches(expr, key_expr, PartitionPruneValueMode::Static).is_some()
    })
}

fn partition_key_bool_equality_predicate(
    expr: &Expr,
    spec: &pgrust_analyze::LoweredPartitionSpec,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
) -> Option<(usize, bool)> {
    spec.key_exprs
        .iter()
        .enumerate()
        .filter(|(index, _)| partition_key_type_is_bool(spec, *index))
        .find_map(|(index, key_expr)| {
            if partition_key_expr_matches(expr, key_expr, PartitionPruneValueMode::Static).is_some()
            {
                return Some((index, true));
            }
            if bool_expr_is_negation_of_partition_key(expr, key_expr) {
                return Some((index, false));
            }
            None
        })
}

fn bool_expr_is_negation_of_partition_key(expr: &Expr, key_expr: &Expr) -> bool {
    bool_not_arg(expr).is_some_and(|inner| {
        partition_key_expr_matches(inner, key_expr, PartitionPruneValueMode::Static).is_some()
    }) || bool_not_arg(key_expr).is_some_and(|inner| {
        partition_key_expr_matches(expr, inner, PartitionPruneValueMode::Static).is_some()
    })
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
    key_index: usize,
) -> bool {
    spec.key_types
        .get(key_index)
        .is_some_and(|ty| !ty.is_array && ty.kind == pgrust_nodes::parsenodes::SqlTypeKind::Bool)
}

fn partition_key_expr_matches<'a>(
    expr: &'a Expr,
    key_expr: &Expr,
    value_mode: PartitionPruneValueMode,
) -> Option<&'a Expr> {
    let normalized = normalize_key_expr(expr);
    let normalized_key = normalize_key_expr(key_expr);
    (normalized == normalized_key
        || simple_var_matches(normalized, normalized_key, value_mode)
        || casted_partition_key_expr_matches(expr, key_expr, value_mode))
    .then_some(expr)
}

fn casted_partition_key_expr_matches(
    expr: &Expr,
    key_expr: &Expr,
    value_mode: PartitionPruneValueMode,
) -> bool {
    let Expr::Cast(inner, cast_ty) = expr else {
        return false;
    };
    partition_key_expr_matches(inner, key_expr, value_mode).is_some()
        && partition_key_cast_is_prune_compatible(key_expr, cast_ty, value_mode)
}

fn partition_key_cast_is_prune_compatible(
    key_expr: &Expr,
    cast_ty: &pgrust_nodes::parsenodes::SqlType,
    value_mode: PartitionPruneValueMode,
) -> bool {
    let Some(key_ty) = expr_sql_type_hint(normalize_key_expr(key_expr)) else {
        return false;
    };
    partition_key_cast_types_match(&key_ty, cast_ty, value_mode)
}

fn partition_key_cast_types_match(
    key_ty: &pgrust_nodes::parsenodes::SqlType,
    cast_ty: &pgrust_nodes::parsenodes::SqlType,
    value_mode: PartitionPruneValueMode,
) -> bool {
    key_ty == cast_ty
        || (integer_partition_cast_type(key_ty) && integer_partition_cast_type(cast_ty))
        || (text_relabel_partition_cast_type(key_ty) && text_relabel_partition_cast_type(cast_ty))
        || (value_mode == PartitionPruneValueMode::Runtime
            && stable_datetime_partition_cast_type(key_ty, cast_ty))
}

fn integer_partition_cast_type(ty: &pgrust_nodes::parsenodes::SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            pgrust_nodes::parsenodes::SqlTypeKind::Int2
                | pgrust_nodes::parsenodes::SqlTypeKind::Int4
                | pgrust_nodes::parsenodes::SqlTypeKind::Int8
        )
}

fn text_relabel_partition_cast_type(ty: &pgrust_nodes::parsenodes::SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            pgrust_nodes::parsenodes::SqlTypeKind::Text
                | pgrust_nodes::parsenodes::SqlTypeKind::Varchar
                | pgrust_nodes::parsenodes::SqlTypeKind::Char
                | pgrust_nodes::parsenodes::SqlTypeKind::Name
        )
}

fn stable_datetime_partition_cast_type(
    key_ty: &pgrust_nodes::parsenodes::SqlType,
    cast_ty: &pgrust_nodes::parsenodes::SqlType,
) -> bool {
    // :HACK: PostgreSQL treats timestamp/timestamptz partition-key comparisons as
    // stable because the cast depends on TimeZone. Keep them out of static
    // pruning while still allowing executor-startup pruning to use the fixed
    // statement/session timezone. Long term this should be driven by operator
    // volatility metadata rather than this type-pair check.
    !key_ty.is_array
        && !cast_ty.is_array
        && matches!(
            (key_ty.kind, cast_ty.kind),
            (
                pgrust_nodes::parsenodes::SqlTypeKind::Timestamp,
                pgrust_nodes::parsenodes::SqlTypeKind::TimestampTz
            ) | (
                pgrust_nodes::parsenodes::SqlTypeKind::TimestampTz,
                pgrust_nodes::parsenodes::SqlTypeKind::Timestamp
            )
        )
}

fn simple_var_matches(left: &Expr, right: &Expr, value_mode: PartitionPruneValueMode) -> bool {
    matches!(
        (left, right),
        (Expr::Var(left), Expr::Var(right))
            if left.varlevelsup == 0
                && right.varlevelsup == 0
                && left.varattno == right.varattno
                && partition_key_cast_types_match(&left.vartype, &right.vartype, value_mode)
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

fn collation_mismatch(query_collation_oid: Option<u32>, key_collation_oid: u32) -> bool {
    query_collation_oid.is_some_and(|oid| key_collation_oid != 0 && oid != key_collation_oid)
}

fn op_pruning_collation(op: &OpExpr) -> Option<u32> {
    op.collation_oid
        .or_else(|| op.args.iter().find_map(top_level_explicit_collation))
}

fn top_level_explicit_collation(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        Expr::Cast(inner, _) => top_level_explicit_collation(inner),
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

fn const_value_for_partition_key(
    expr: &Expr,
    key_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<Value> {
    match expr {
        Expr::Const(value) => {
            coerce_const_for_partition_key(value.clone(), key_type, catalog, value_mode)
        }
        Expr::Collate { expr: inner, .. } => {
            const_value_for_partition_key(inner, key_type, catalog, value_mode)
        }
        Expr::Cast(inner, target_type) => {
            let key_type = key_type?;
            if !partition_key_cast_types_match(&key_type, target_type, value_mode) {
                return None;
            }
            if value_mode == PartitionPruneValueMode::Static
                && stable_datetime_partition_cast_type(&key_type, target_type)
            {
                return None;
            }
            let value = const_value(inner)?;
            let source_type = expr_sql_type_hint(inner).or_else(|| value.sql_type_hint())?;
            cast_value_with_source_type_catalog_and_config(
                value,
                Some(source_type),
                key_type,
                catalog,
                &DateTimeConfig::default(),
            )
            .ok()
        }
        _ => None,
    }
}

fn coerce_const_for_partition_key(
    value: Value,
    key_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<Value> {
    let key_type = key_type?;
    if !partition_prune_should_fold_const_cast(key_type) {
        return Some(value);
    }
    let source_type = value.sql_type_hint()?;
    if value_mode == PartitionPruneValueMode::Static
        && stable_datetime_partition_cast_type(&key_type, &source_type)
    {
        return None;
    }
    catalog
        .and_then(|catalog| {
            cast_value_with_source_type_catalog_and_config(
                value.clone(),
                Some(source_type),
                key_type,
                Some(catalog),
                &DateTimeConfig::default(),
            )
            .ok()
        })
        .or(Some(value))
}

fn partition_prune_should_fold_const_cast(target_type: SqlType) -> bool {
    !target_type.is_array
        && matches!(
            target_type.kind,
            SqlTypeKind::Date
                | SqlTypeKind::Time
                | SqlTypeKind::TimeTz
                | SqlTypeKind::Timestamp
                | SqlTypeKind::TimestampTz
                | SqlTypeKind::Enum
                | SqlTypeKind::Composite
                | SqlTypeKind::Record
        )
}

fn coerce_array_const_for_partition_key(
    value: Value,
    key_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<Value> {
    let key_type = key_type?;
    if !partition_prune_should_fold_const_cast(key_type) {
        return Some(value);
    }
    let source_type = value.sql_type_hint()?;
    if value_mode == PartitionPruneValueMode::Static
        && stable_datetime_partition_cast_type(&key_type, &source_type)
    {
        return None;
    }
    let catalog = catalog?;
    cast_value_with_source_type_catalog_and_config(
        value,
        Some(source_type),
        key_type,
        Some(catalog),
        &DateTimeConfig::default(),
    )
    .ok()
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
    op: SubqueryComparisonOp,
    query_collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<(usize, OpExprKind, Vec<Value>)> {
    let op = subquery_comparison_op_kind(op)?;
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        let key_collation_oid = spec.partcollation.get(index).copied().unwrap_or(0);
        if collation_mismatch(query_collation_oid, key_collation_oid) {
            continue;
        }
        if partition_key_expr_matches(left, key_expr, value_mode).is_some() {
            let key_type = spec.key_types.get(index).copied();
            let values =
                const_array_values_for_partition_key(right, key_type, catalog, value_mode)?;
            return Some((index, op, values));
        }
    }
    None
}

fn const_array_values_for_partition_key(
    expr: &Expr,
    key_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
    value_mode: PartitionPruneValueMode,
) -> Option<Vec<Value>> {
    if value_mode == PartitionPruneValueMode::Static
        && let (Some(key_type), Some(array_type)) = (key_type, expr_sql_type_hint(expr))
        && stable_datetime_partition_cast_type(&key_type, &array_type.element_type())
    {
        return None;
    }
    const_array_values(expr)?
        .into_iter()
        .map(|value| coerce_array_const_for_partition_key(value, key_type, catalog, value_mode))
        .collect()
}

fn const_array_values(expr: &Expr) -> Option<Vec<Value>> {
    match expr {
        Expr::ArrayLiteral { elements, .. } => elements.iter().map(const_value).collect(),
        Expr::Const(Value::Array(values)) => Some(values.clone()),
        Expr::Const(Value::PgArray(array)) => Some(array.to_nested_values()),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => const_array_values(inner),
        _ => None,
    }
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
    key_index: usize,
    key_on_left: bool,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
    ancestor_bound: Option<&PartitionBoundSpec>,
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
            key_index,
            value,
            catalog,
            ancestor_bound,
        ),
        OpExprKind::NotEq => {
            bound_may_contain_non_equal_value(spec, bound, key_index, value, collation_oid)
        }
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            bound_may_overlap_inequality(
                spec,
                bound,
                sibling_bounds,
                key_index,
                op,
                value,
                collation_oid,
                ancestor_bound,
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
    key_index: usize,
    value: &Value,
    catalog: Option<&dyn CatalogLookup>,
    ancestor_bound: Option<&PartitionBoundSpec>,
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
            key_index,
            value,
            OpExprKind::Eq,
            None,
            ancestor_bound,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
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
                        key_index,
                        value,
                        None,
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
                bound_may_contain_value(spec, bound, sibling_bounds, key_index, value, None, None)
            } else {
                range_may_satisfy_conjunction_value(
                    spec,
                    bound,
                    sibling_bounds,
                    key_index,
                    value,
                    OpExprKind::Eq,
                    None,
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
                key_index,
                &Value::Bool(!value),
                OpExprKind::Eq,
                None,
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
    key_index: usize,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
    ancestor_bound: Option<&PartitionBoundSpec>,
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
            key_index,
            value,
            op,
            collation_oid,
            ancestor_bound,
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
    collation_oid: Option<u32>,
}

#[derive(Debug)]
enum ConstraintApplyResult {
    Applied,
    Ignored,
    Contradiction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeConstraintMode {
    Strict,
    PermissiveConflicts,
}

fn range_may_satisfy_conjunction(
    expr: &Expr,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
    mode: RangeConstraintMode,
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return None;
    }
    let mut constraints = vec![KeyConstraint::default(); spec.key_exprs.len()];
    let mut saw_constraint = false;
    for conjunct in flatten_and_exprs(expr) {
        match apply_range_constraint(conjunct, spec, &mut constraints, mode) {
            ConstraintApplyResult::Applied => saw_constraint = true,
            ConstraintApplyResult::Ignored => {}
            ConstraintApplyResult::Contradiction => return Some(false),
        }
    }
    if !saw_constraint {
        return None;
    }
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, ancestor_bound, &constraints)
}

fn range_may_satisfy_conjunction_value(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    key_index: usize,
    value: &Value,
    op: OpExprKind,
    collation_oid: Option<u32>,
    ancestor_bound: Option<&PartitionBoundSpec>,
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
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, ancestor_bound, &constraints)
}

#[derive(Clone, Debug, Default)]
struct HashKeyConstraint {
    value: Option<Value>,
    constrained: bool,
}

fn hash_may_satisfy_conjunction(
    expr: &Expr,
    spec: &pgrust_analyze::LoweredPartitionSpec,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
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
            let collation_oid = op_pruning_collation(op);
            let Some((key_index, _, value)) = partition_key_const_cmp(
                left,
                right,
                spec,
                collation_oid,
                None,
                PartitionPruneValueMode::Static,
            ) else {
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u64> {
    let mut row_hash = 0_u64;
    for (index, value) in values.iter().enumerate() {
        if matches!(value, Value::Null) {
            continue;
        }
        let value_hash = partition_prune_hash_value(value, spec, index, catalog)?;
        row_hash = crate::runtime::hash_combine64(row_hash, value_hash);
    }
    Some(row_hash)
}

fn partition_prune_hash_value(
    value: &Value,
    spec: &pgrust_analyze::LoweredPartitionSpec,
    key_index: usize,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u64> {
    if let Some(proc_oid) = partition_hash_support_proc(key_index, spec, catalog) {
        return eval_partition_hash_support_proc(proc_oid, value, catalog);
    }
    crate::runtime::hash_value_extended(
        value,
        spec.partclass.get(key_index).copied(),
        crate::runtime::HASH_PARTITION_SEED,
    )
    .ok()
    .flatten()
}

fn partition_hash_support_proc(
    key_index: usize,
    spec: &pgrust_analyze::LoweredPartitionSpec,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<u32> {
    let catalog = catalog?;
    let opclass_oid = *spec.partclass.get(key_index)?;
    let opclass = catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    let key_type = *spec.key_types.get(key_index)?;
    let key_type_oid = sql_type_oid(key_type);
    catalog
        .amproc_rows()
        .into_iter()
        .find(|row| {
            row.amprocfamily == opclass.opcfamily
                && row.amprocnum == 2
                && hash_amproc_type_matches(row.amproclefttype, key_type_oid, key_type)
                && hash_amproc_type_matches(row.amprocrighttype, key_type_oid, key_type)
        })
        .map(|row| row.amproc)
}

fn hash_amproc_type_matches(
    proc_type_oid: u32,
    key_type_oid: u32,
    key_type: pgrust_nodes::parsenodes::SqlType,
) -> bool {
    proc_type_oid == key_type_oid
        || proc_type_oid == ANYOID
        || (key_type.is_array && proc_type_oid == ANYARRAYOID)
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
        let opclass = (kind == pgrust_nodes::primnodes::HashFunctionKind::BpChar)
            .then_some(pgrust_catalog_data::BPCHAR_HASH_OPCLASS_OID);
        return crate::runtime::hash_value_extended(
            value,
            opclass,
            crate::runtime::HASH_PARTITION_SEED,
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
            Some(value.wrapping_add(crate::runtime::HASH_PARTITION_SEED as i64) as u64)
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
    mode: RangeConstraintMode,
) -> ConstraintApplyResult {
    if let Some((key_index, value)) = partition_key_bool_equality_predicate(expr, spec) {
        return if apply_range_comparison_constraint(
            constraints,
            spec,
            key_index,
            OpExprKind::Eq,
            Value::Bool(value),
            None,
            mode,
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
            let collation_oid = op_pruning_collation(op);
            let Some((key_index, key_on_left, value)) = partition_key_const_cmp(
                left,
                right,
                spec,
                collation_oid,
                None,
                PartitionPruneValueMode::Static,
            ) else {
                return ConstraintApplyResult::Ignored;
            };
            let op = if key_on_left {
                op.op
            } else {
                commute_op(op.op)
            };
            if apply_range_comparison_constraint(
                constraints,
                spec,
                key_index,
                op,
                value,
                collation_oid,
                mode,
            ) {
                ConstraintApplyResult::Applied
            } else {
                ConstraintApplyResult::Contradiction
            }
        }
        _ => ConstraintApplyResult::Ignored,
    }
}

fn apply_range_comparison_constraint(
    constraints: &mut [KeyConstraint],
    spec: &LoweredPartitionSpec,
    key_index: usize,
    op: OpExprKind,
    value: Value,
    collation_oid: Option<u32>,
    mode: RangeConstraintMode,
) -> bool {
    if mode == RangeConstraintMode::Strict || key_index >= constraints.len() {
        return add_comparison_constraint(constraints, spec, key_index, op, value, collation_oid);
    }
    let saved = constraints[key_index].clone();
    if add_comparison_constraint(constraints, spec, key_index, op, value, collation_oid) {
        true
    } else {
        // :HACK: PostgreSQL's partition pruning does not prove every
        // contradictory conjunct inside OR arms. Keep the first useful
        // per-key range constraint for pruning, but let execution enforce the
        // full filter.
        constraints[key_index] = saved;
        true
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
    if let Some(existing_collation_oid) = constraint.collation_oid
        && collation_oid.is_some_and(|oid| oid != existing_collation_oid)
    {
        return true;
    }
    if constraint.collation_oid.is_none() {
        constraint.collation_oid = collation_oid;
    }
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
    ancestor_bound: Option<&PartitionBoundSpec>,
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
            if let Some(ancestor_bound) = ancestor_bound
                && !ancestor_bound.is_default()
                && range_bound_contains_tuple(ancestor_bound, &values, &spec.partcollation)
                    == Some(false)
            {
                return Some(false);
            }
            return Some(!sibling_bounds.iter().any(|sibling| {
                !sibling.is_default()
                    && range_bound_contains_tuple(sibling, &values, &spec.partcollation)
                        == Some(true)
            }));
        }
        return range_default_may_overlap_constraints(
            spec,
            sibling_bounds,
            ancestor_bound,
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
    spec: &pgrust_analyze::LoweredPartitionSpec,
    sibling_bounds: &[PartitionBoundSpec],
    ancestor_bound: Option<&PartitionBoundSpec>,
    constraints: &[KeyConstraint],
) -> Option<bool> {
    if constraints
        .iter()
        .any(|constraint| constraint.requires_null)
    {
        return Some(true);
    }
    if spec.key_exprs.len() > 1 && !multi_key_default_interval_supported(constraints) {
        return Some(true);
    }
    let Some(mut interval) = query_interval_from_constraints(constraints) else {
        return Some(true);
    };
    if let Some(ancestor_bound) = ancestor_bound {
        let Some(intersection) = intersect_query_interval_with_range_bound(
            &interval,
            ancestor_bound,
            &spec.partcollation,
        ) else {
            return Some(true);
        };
        let Some(intersection) = intersection else {
            return Some(false);
        };
        interval = intersection;
    }
    range_interval_covered_by_non_default_siblings(&interval, sibling_bounds, &spec.partcollation)
        .map(|covered| !covered)
        .or(Some(true))
}

fn multi_key_default_interval_supported(constraints: &[KeyConstraint]) -> bool {
    let Some((last, prefix)) = constraints.split_last() else {
        return true;
    };
    prefix
        .iter()
        .all(|constraint| constraint.equal.is_some() && !constraint.requires_null)
        && (last.lower.is_some() || last.upper.is_some())
}

#[derive(Clone, Debug)]
struct QueryInterval {
    lower: Vec<BoundPoint>,
    lower_inclusive: bool,
    upper: Vec<BoundPoint>,
    upper_inclusive: bool,
}

#[derive(Clone, Debug)]
struct RangeInterval {
    lower: Vec<BoundPoint>,
    upper: Vec<BoundPoint>,
}

#[derive(Clone, Debug)]
enum BoundPoint {
    NegInfinity,
    Value(Value),
    PosInfinity,
}

fn query_interval_from_constraints(constraints: &[KeyConstraint]) -> Option<QueryInterval> {
    if constraints
        .iter()
        .all(|constraint| constraint.equal.is_some())
    {
        return None;
    }
    if constraints
        .iter()
        .all(|constraint| constraint.lower.is_none() && constraint.upper.is_none())
    {
        return None;
    }
    if !constraints_form_single_prefix_interval(constraints) {
        return None;
    }
    let lower = query_bound_components(constraints, QueryBoundSide::Lower)
        .into_iter()
        .map(|component| bound_point_from_query_component(&component))
        .collect::<Vec<_>>();
    let upper_components = query_bound_components(constraints, QueryBoundSide::Upper);
    let upper = upper_components
        .iter()
        .map(bound_point_from_query_component)
        .collect::<Vec<_>>();
    let lower_inclusive = constraints
        .iter()
        .find_map(|constraint| constraint.lower.as_ref().map(|bound| bound.inclusive))
        .unwrap_or(true);
    let upper_inclusive = upper_components
        .iter()
        .rev()
        .find_map(|component| match component {
            QueryBoundComponent::Value { inclusive, .. } => Some(*inclusive),
            QueryBoundComponent::NegInfinity | QueryBoundComponent::PosInfinity => None,
        })
        .unwrap_or(false);
    Some(QueryInterval {
        lower,
        lower_inclusive,
        upper,
        upper_inclusive,
    })
}

fn constraints_form_single_prefix_interval(constraints: &[KeyConstraint]) -> bool {
    let mut saw_range_key = false;
    for (index, constraint) in constraints.iter().enumerate() {
        if saw_range_key {
            if constraint_has_bound(constraint) {
                return false;
            }
            continue;
        }
        if constraint.equal.is_some() {
            continue;
        }
        if constraint.lower.is_some() || constraint.upper.is_some() {
            saw_range_key = true;
            continue;
        }
        return constraints
            .iter()
            .skip(index + 1)
            .all(|constraint| !constraint_has_bound(constraint));
    }
    true
}

fn constraint_has_bound(constraint: &KeyConstraint) -> bool {
    constraint.equal.is_some() || constraint.lower.is_some() || constraint.upper.is_some()
}

fn bound_point_from_query_component(component: &QueryBoundComponent<'_>) -> BoundPoint {
    match component {
        QueryBoundComponent::NegInfinity => BoundPoint::NegInfinity,
        QueryBoundComponent::PosInfinity => BoundPoint::PosInfinity,
        QueryBoundComponent::Value { value, .. } => BoundPoint::Value((*value).clone()),
    }
}

fn intersect_query_interval_with_range_bound(
    query: &QueryInterval,
    bound: &PartitionBoundSpec,
    collations: &[u32],
) -> Option<Option<QueryInterval>> {
    let Some(range) = range_interval_from_non_default_bound(bound) else {
        return Some(Some(query.clone()));
    };
    let (lower, lower_inclusive) =
        match compare_bound_tuples(&query.lower, &range.lower, collations)? {
            Ordering::Less => (range.lower, true),
            Ordering::Equal => (query.lower.clone(), query.lower_inclusive),
            Ordering::Greater => (query.lower.clone(), query.lower_inclusive),
        };
    let (upper, upper_inclusive) =
        match compare_bound_tuples(&query.upper, &range.upper, collations)? {
            Ordering::Less => (query.upper.clone(), query.upper_inclusive),
            Ordering::Equal => (query.upper.clone(), false),
            Ordering::Greater => (range.upper, false),
        };
    match compare_bound_tuples(&lower, &upper, collations)? {
        Ordering::Greater => Some(None),
        Ordering::Equal if !(lower_inclusive && upper_inclusive) => Some(None),
        _ => Some(Some(QueryInterval {
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
        })),
    }
}

fn range_interval_covered_by_non_default_siblings(
    query: &QueryInterval,
    sibling_bounds: &[PartitionBoundSpec],
    collations: &[u32],
) -> Option<bool> {
    let mut ranges = sibling_bounds
        .iter()
        .filter_map(range_interval_from_non_default_bound)
        .collect::<Vec<_>>();
    ranges.sort_by(|left, right| {
        compare_bound_tuples(&left.lower, &right.lower, collations).unwrap_or(Ordering::Equal)
    });
    let mut covered_until = query.lower.clone();
    for range in ranges {
        if compare_bound_tuples(&range.upper, &covered_until, collations)? != Ordering::Greater {
            continue;
        }
        if compare_bound_tuples(&range.lower, &covered_until, collations)? == Ordering::Greater {
            return Some(false);
        }
        covered_until = range.upper;
        match compare_bound_tuples(&covered_until, &query.upper, collations)? {
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
    Some(RangeInterval {
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

fn compare_bound_tuples(
    left: &[BoundPoint],
    right: &[BoundPoint],
    collations: &[u32],
) -> Option<Ordering> {
    let width = left.len().max(right.len());
    for index in 0..width {
        let left_point = left.get(index).unwrap_or(&BoundPoint::PosInfinity);
        let right_point = right.get(index).unwrap_or(&BoundPoint::PosInfinity);
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
    let mut components = Vec::with_capacity(constraints.len());
    let mut trailing_fill = None;
    for constraint in constraints {
        if let Some(fill) = trailing_fill.clone() {
            components.push(fill);
            continue;
        }
        if let Some(equal) = &constraint.equal {
            components.push(QueryBoundComponent::Value {
                value: equal,
                inclusive: true,
            });
            continue;
        }
        let bound = match side {
            QueryBoundSide::Lower => constraint.lower.as_ref(),
            QueryBoundSide::Upper => constraint.upper.as_ref(),
        };
        if let Some(bound) = bound {
            components.push(QueryBoundComponent::Value {
                value: &bound.value,
                inclusive: bound.inclusive,
            });
            if !bound.inclusive {
                trailing_fill = Some(trailing_component_for_range_key(side, bound.inclusive));
            }
        } else {
            components.push(unbounded_component_for_side(side));
        }
    }
    components
}

fn unbounded_component_for_side(side: QueryBoundSide) -> QueryBoundComponent<'static> {
    match side {
        QueryBoundSide::Lower => QueryBoundComponent::NegInfinity,
        QueryBoundSide::Upper => QueryBoundComponent::PosInfinity,
    }
}

fn trailing_component_for_range_key(
    side: QueryBoundSide,
    inclusive: bool,
) -> QueryBoundComponent<'static> {
    match (side, inclusive) {
        (QueryBoundSide::Lower, true) => QueryBoundComponent::NegInfinity,
        (QueryBoundSide::Lower, false) => QueryBoundComponent::PosInfinity,
        (QueryBoundSide::Upper, true) => QueryBoundComponent::PosInfinity,
        (QueryBoundSide::Upper, false) => QueryBoundComponent::NegInfinity,
    }
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
    use pgrust_analyze::{
        BoundRelation, LoweredPartitionSpec, pg_partitioned_table_row, serialize_partition_bound,
    };
    use pgrust_catalog_data::{
        BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_OID, PUBLIC_NAMESPACE_OID, PgInheritsRow,
    };
    use pgrust_catalog_data::{
        C_COLLATION_OID, POSIX_COLLATION_OID, proc_oid_for_builtin_scalar_function,
    };
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::datetime::TimestampTzADT;
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::primnodes::RelationDesc;
    use pgrust_nodes::primnodes::{
        BuiltinScalarFunction, FuncExpr, ScalarArrayOpExpr, ScalarFunctionImpl, Var,
    };

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
        super::expr_may_match_bound(
            expr,
            spec,
            bound,
            sibling_bounds,
            None,
            None,
            None,
            PartitionPruneValueMode::Static,
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

    fn timestamp_range_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: vec!["a".into()],
            key_exprs: vec![timestamp_key_expr()],
            key_types: vec![SqlType::new(SqlTypeKind::Timestamp)],
            key_sqls: vec!["a".into()],
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
        }
    }

    fn multi_int_range_spec(width: usize) -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: (0..width).map(|index| format!("k{index}")).collect(),
            key_exprs: (0..width)
                .map(|index| int_key_att_expr(1, (index + 1) as i32))
                .collect(),
            key_types: vec![SqlType::new(SqlTypeKind::Int4); width],
            key_sqls: (0..width).map(|index| format!("k{index}")).collect(),
            partattrs: (1..=width as i16).collect(),
            partclass: vec![0; width],
            partcollation: vec![0; width],
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

    fn list_spec_on_b() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::List,
            key_columns: vec!["b".into()],
            key_exprs: vec![text_key_att_expr(1, 2)],
            key_types: vec![SqlType::new(SqlTypeKind::Text)],
            key_sqls: vec!["b".into()],
            partattrs: vec![2],
            partclass: vec![0],
            partcollation: vec![0],
        }
    }

    fn char_list_spec() -> LoweredPartitionSpec {
        LoweredPartitionSpec {
            strategy: PartitionStrategy::List,
            key_columns: vec!["a".into()],
            key_exprs: vec![Expr::Var(Var {
                varno: 1,
                varattno: 1,
                varlevelsup: 0,
                vartype: SqlType::with_char_len(SqlTypeKind::Char, 1),
                collation_oid: None,
            })],
            key_types: vec![SqlType::with_char_len(SqlTypeKind::Char, 1)],
            key_sqls: vec!["a".into()],
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
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
            collation_oid: None,
        })
    }

    fn substr_key_expr() -> Expr {
        Expr::Func(Box::new(FuncExpr {
            funcid: proc_oid_for_builtin_scalar_function(BuiltinScalarFunction::Substring)
                .expect("substring proc oid"),
            funcname: Some("substr".into()),
            funcresulttype: Some(SqlType::new(SqlTypeKind::Text)),
            funcvariadic: false,
            implementation: ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Substring),
            collation_oid: None,
            display_args: None,
            args: vec![key_expr(), Expr::Const(Value::Int32(1))],
        }))
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
            collation_oid: None,
        })
    }

    fn timestamp_key_expr() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Timestamp),
            collation_oid: None,
        })
    }

    fn text_key_att_expr(varno: usize, varattno: i32) -> Expr {
        Expr::Var(Var {
            varno,
            varattno,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
            collation_oid: None,
        })
    }

    fn bool_key_expr() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Bool),
            collation_oid: None,
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

    fn multi_int_range_bound(
        from: Vec<PartitionRangeDatumValue>,
        to: Vec<PartitionRangeDatumValue>,
    ) -> PartitionBoundSpec {
        PartitionBoundSpec::Range {
            from,
            to,
            is_default: false,
        }
    }

    fn multi_text_range_bound(from: &[&str], to: &[&str]) -> PartitionBoundSpec {
        PartitionBoundSpec::Range {
            from: from.iter().map(|value| text_range_value(value)).collect(),
            to: to.iter().map(|value| text_range_value(value)).collect(),
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

    fn timestamp_value(value: i64) -> PartitionRangeDatumValue {
        PartitionRangeDatumValue::Value(SerializedPartitionValue::Timestamp(value))
    }

    fn text_range_value(value: &str) -> PartitionRangeDatumValue {
        PartitionRangeDatumValue::Value(SerializedPartitionValue::Text(value.into()))
    }

    fn int_cmp(op: OpExprKind, value: i32) -> Expr {
        Expr::op_auto(op, vec![int_key_expr(42), Expr::Const(Value::Int32(value))])
    }

    fn int_att_cmp(attno: i32, op: OpExprKind, value: i32) -> Expr {
        Expr::op_auto(
            op,
            vec![
                int_key_att_expr(42, attno),
                Expr::Const(Value::Int32(value)),
            ],
        )
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

    fn collated_text_eq(value: &str, collation_oid: u32) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op: OpExprKind::Eq,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![
                key_expr(),
                Expr::Collate {
                    expr: Box::new(text_const(value)),
                    collation_oid,
                },
            ],
            collation_oid: None,
        }))
    }

    fn collated_substr_eq(value: &str, collation_oid: u32) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op: OpExprKind::Eq,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![
                substr_key_expr(),
                Expr::Collate {
                    expr: Box::new(text_const(value)),
                    collation_oid,
                },
            ],
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
        let mut row_hash = 0;
        for value in values {
            if matches!(value, Value::Null) {
                continue;
            }
            let value_hash = crate::runtime::hash_value_extended(
                value,
                None,
                crate::runtime::HASH_PARTITION_SEED,
            )
            .unwrap()
            .unwrap_or(0);
            row_hash = crate::runtime::hash_combine64(row_hash, value_hash);
        }
        (row_hash % modulus as u64) as i32
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
                columns: vec![pgrust_catalog_data::desc::column_desc(
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

    fn int_text_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                pgrust_catalog_data::desc::column_desc("a", SqlType::new(SqlTypeKind::Int4), true),
                pgrust_catalog_data::desc::column_desc("b", SqlType::new(SqlTypeKind::Text), true),
            ],
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
    fn char_partition_key_relabel_cast_matches_partition_key() {
        let spec = char_list_spec();
        let ad_bound = text_bound(&["a", "d"]);
        let bc_bound = text_bound(&["b", "c"]);
        let casted_key = Expr::Cast(
            Box::new(Expr::Var(Var {
                varno: 1,
                varattno: 1,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Char),
                collation_oid: None,
            })),
            SqlType::new(SqlTypeKind::Char),
        );
        let expr = Expr::and(
            Expr::op_auto(
                OpExprKind::Gt,
                vec![casted_key.clone(), Expr::Const(Value::Text("a".into()))],
            ),
            Expr::op_auto(
                OpExprKind::Lt,
                vec![casted_key, Expr::Const(Value::Text("d".into()))],
            ),
        );

        assert!(!expr_may_match_bound(&expr, &spec, &ad_bound, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &bc_bound, &[]));
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
    fn range_default_with_ancestor_uses_intersected_domain() {
        let spec = range_spec();
        let default_bound = int_range_default_bound();
        let ancestor = int_range_bound(int_value(31), PartitionRangeDatumValue::MaxValue);
        let siblings = vec![int_range_bound(
            int_value(31),
            PartitionRangeDatumValue::MaxValue,
        )];

        assert!(!super::expr_may_match_bound(
            &int_cmp(OpExprKind::LtEq, 31),
            &spec,
            &default_bound,
            &siblings,
            Some(&ancestor),
            None,
            None,
            PartitionPruneValueMode::Static,
        ));
    }

    #[test]
    fn range_or_arm_keeps_first_constraint_from_conflicting_and_arm() {
        let spec = range_spec();
        let one_to_ten = int_range_bound(int_value(1), int_value(10));
        let fifteen_to_twenty = int_range_bound(int_value(15), int_value(20));
        let contradictory_arm = Expr::and(int_cmp(OpExprKind::Eq, 1), int_cmp(OpExprKind::Eq, 3));
        let exact_arm = Expr::and(int_cmp(OpExprKind::Gt, 1), int_cmp(OpExprKind::Eq, 15));
        let expr = Expr::or(contradictory_arm.clone(), exact_arm);

        assert!(!expr_may_match_bound(
            &contradictory_arm,
            &spec,
            &one_to_ten,
            &[]
        ));
        assert!(expr_may_match_bound(&expr, &spec, &one_to_ten, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &fifteen_to_twenty, &[]));
    }

    #[test]
    fn multi_key_range_default_keeps_unsupported_prefix_intervals() {
        let spec = multi_int_range_spec(2);
        let default_bound = int_range_default_bound();
        let siblings = vec![
            multi_int_range_bound(
                vec![
                    PartitionRangeDatumValue::MinValue,
                    PartitionRangeDatumValue::MinValue,
                ],
                vec![int_value(1), PartitionRangeDatumValue::MinValue],
            ),
            multi_int_range_bound(
                vec![int_value(1), PartitionRangeDatumValue::MinValue],
                vec![int_value(1), int_value(1)],
            ),
            multi_int_range_bound(
                vec![int_value(1), int_value(1)],
                vec![int_value(2), PartitionRangeDatumValue::MinValue],
            ),
        ];

        assert!(expr_may_match_bound(
            &int_att_cmp(1, OpExprKind::Lt, 2),
            &spec,
            &default_bound,
            &siblings,
        ));
    }

    #[test]
    fn multi_key_range_default_prunes_final_key_interval_with_equal_prefix() {
        let spec = multi_int_range_spec(2);
        let default_bound = int_range_default_bound();
        let siblings = vec![multi_int_range_bound(
            vec![int_value(2), PartitionRangeDatumValue::MinValue],
            vec![int_value(2), int_value(1)],
        )];
        let expr = Expr::and(
            int_att_cmp(1, OpExprKind::Eq, 2),
            int_att_cmp(2, OpExprKind::Lt, 1),
        );

        assert!(!expr_may_match_bound(
            &expr,
            &spec,
            &default_bound,
            &siblings
        ));
    }

    fn mc3p_bounds() -> Vec<PartitionBoundSpec> {
        vec![
            multi_int_range_bound(
                vec![
                    PartitionRangeDatumValue::MinValue,
                    PartitionRangeDatumValue::MinValue,
                    PartitionRangeDatumValue::MinValue,
                ],
                vec![int_value(1), int_value(1), int_value(1)],
            ),
            multi_int_range_bound(
                vec![int_value(1), int_value(1), int_value(1)],
                vec![int_value(10), int_value(5), int_value(10)],
            ),
            multi_int_range_bound(
                vec![int_value(10), int_value(5), int_value(10)],
                vec![int_value(10), int_value(10), int_value(10)],
            ),
            multi_int_range_bound(
                vec![int_value(10), int_value(10), int_value(10)],
                vec![int_value(10), int_value(10), int_value(20)],
            ),
            multi_int_range_bound(
                vec![int_value(10), int_value(10), int_value(20)],
                vec![
                    int_value(10),
                    PartitionRangeDatumValue::MaxValue,
                    PartitionRangeDatumValue::MaxValue,
                ],
            ),
            multi_int_range_bound(
                vec![int_value(11), int_value(1), int_value(1)],
                vec![int_value(20), int_value(10), int_value(10)],
            ),
            multi_int_range_bound(
                vec![int_value(20), int_value(10), int_value(10)],
                vec![int_value(20), int_value(20), int_value(20)],
            ),
            multi_int_range_bound(
                vec![int_value(20), int_value(20), int_value(20)],
                vec![
                    PartitionRangeDatumValue::MaxValue,
                    PartitionRangeDatumValue::MaxValue,
                    PartitionRangeDatumValue::MaxValue,
                ],
            ),
            int_range_default_bound(),
        ]
    }

    fn mc3p_expr() -> Expr {
        Expr::or(
            Expr::or(
                Expr::and(
                    Expr::and(
                        int_att_cmp(1, OpExprKind::Eq, 1),
                        int_att_cmp(2, OpExprKind::Eq, 1),
                    ),
                    int_att_cmp(3, OpExprKind::Eq, 1),
                ),
                Expr::and(
                    Expr::and(
                        int_att_cmp(1, OpExprKind::Eq, 10),
                        int_att_cmp(2, OpExprKind::Eq, 5),
                    ),
                    int_att_cmp(3, OpExprKind::Eq, 10),
                ),
            ),
            Expr::and(
                int_att_cmp(1, OpExprKind::Gt, 11),
                int_att_cmp(1, OpExprKind::Lt, 20),
            ),
        )
    }

    #[test]
    fn multi_key_range_or_uses_full_arm_constraints() {
        let spec = multi_int_range_spec(3);
        let bounds = mc3p_bounds();
        let expr = mc3p_expr();
        let visible = bounds
            .iter()
            .enumerate()
            .filter_map(|(index, bound)| {
                expr_may_match_bound(&expr, &spec, bound, &bounds).then_some(index)
            })
            .collect::<Vec<_>>();

        assert_eq!(visible, vec![1, 2, 5, 8]);
    }

    #[test]
    fn multi_key_range_uses_operand_collation_to_match_duplicate_key_exprs() {
        let spec = LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: vec!["a_posix".into(), "a_c".into()],
            key_exprs: vec![key_expr(), key_expr()],
            key_types: vec![SqlType::new(SqlTypeKind::Text); 2],
            key_sqls: vec!["a".into(), "a".into()],
            partattrs: vec![0, 0],
            partclass: vec![0, 0],
            partcollation: vec![POSIX_COLLATION_OID, C_COLLATION_OID],
        };
        let first = multi_text_range_bound(&["a", "a"], &["a", "e"]);
        let second = multi_text_range_bound(&["a", "e"], &["a", "z"]);
        let third = multi_text_range_bound(&["b", "a"], &["b", "e"]);
        let expr = Expr::and(
            collated_text_eq("e", C_COLLATION_OID),
            collated_text_eq("a", POSIX_COLLATION_OID),
        );

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
        assert!(!expr_may_match_bound(&expr, &spec, &third, &[]));
    }

    #[test]
    fn multi_key_range_uses_operand_collation_for_duplicate_substr_keys() {
        let posix_key = Expr::Collate {
            expr: Box::new(substr_key_expr()),
            collation_oid: POSIX_COLLATION_OID,
        };
        let c_key = Expr::Collate {
            expr: Box::new(substr_key_expr()),
            collation_oid: C_COLLATION_OID,
        };
        let spec = LoweredPartitionSpec {
            strategy: PartitionStrategy::Range,
            key_columns: vec!["a_posix".into(), "a_c".into()],
            key_exprs: vec![posix_key, c_key],
            key_types: vec![SqlType::new(SqlTypeKind::Text); 2],
            key_sqls: vec![
                "substr(a, 1) COLLATE \"POSIX\"".into(),
                "substr(a, 1) COLLATE \"C\"".into(),
            ],
            partattrs: vec![0, 0],
            partclass: vec![0, 0],
            partcollation: vec![POSIX_COLLATION_OID, C_COLLATION_OID],
        };
        let first = multi_text_range_bound(&["a", "a"], &["a", "e"]);
        let second = multi_text_range_bound(&["a", "e"], &["a", "z"]);
        let third = multi_text_range_bound(&["b", "a"], &["b", "e"]);
        let expr = Expr::and(
            collated_substr_eq("e", C_COLLATION_OID),
            collated_substr_eq("a", POSIX_COLLATION_OID),
        );

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
        assert!(!expr_may_match_bound(&expr, &spec, &third, &[]));
    }

    #[test]
    fn range_is_not_null_keeps_non_default_partitions() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let second = int_range_bound(int_value(1), int_value(10));
        let expr = Expr::IsNotNull(Box::new(int_key_expr(42)));

        assert!(expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
    }

    #[test]
    fn casted_partition_key_is_not_treated_as_same_opfamily_match() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let casted_key = Expr::Cast(
            Box::new(int_key_expr(42)),
            SqlType::new(SqlTypeKind::Numeric),
        );
        let expr = Expr::op_auto(
            OpExprKind::Eq,
            vec![casted_key, Expr::Const(Value::Numeric("1".into()))],
        );

        assert!(expr_may_match_bound(&expr, &spec, &first, &[]));
    }

    #[test]
    fn integer_family_partition_key_cast_still_prunes_range_partitions() {
        let spec = range_spec();
        let first = int_range_bound(PartitionRangeDatumValue::MinValue, int_value(1));
        let second = int_range_bound(int_value(1), int_value(10));
        let casted_key = Expr::Cast(Box::new(int_key_expr(42)), SqlType::new(SqlTypeKind::Int8));
        let expr = Expr::op_auto(
            OpExprKind::Eq,
            vec![casted_key, Expr::Const(Value::Int64(1))],
        );

        assert!(!expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
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
                pgrust_catalog_data::desc::column_desc("b", SqlType::new(SqlTypeKind::Text), true),
                pgrust_catalog_data::desc::column_desc("a", SqlType::new(SqlTypeKind::Int4), true),
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
    fn own_partition_bound_checks_or_arms_against_all_ancestors() {
        let root_oid = 10;
        let range_child_oid = 20;
        let list_leaf_oid = 30;
        let desc = int_text_desc();
        let catalog = MockCatalog {
            relations: vec![
                relation_with_desc(root_oid, 'p', Some(range_spec()), None, desc.clone()),
                relation_with_desc(
                    range_child_oid,
                    'p',
                    Some(list_spec_on_b()),
                    Some(int_range_bound(int_value(10), int_value(20))),
                    desc.clone(),
                ),
                relation_with_desc(list_leaf_oid, 'r', None, Some(text_bound(&["efgh"])), desc),
            ],
            inherits: vec![
                inherit(root_oid, range_child_oid, 1),
                inherit(range_child_oid, list_leaf_oid, 1),
            ],
        };
        let expr = Expr::or(
            Expr::op_auto(
                OpExprKind::Eq,
                vec![int_key_att_expr(42, 1), Expr::Const(Value::Int32(1))],
            ),
            Expr::op_auto(
                OpExprKind::Eq,
                vec![
                    text_key_att_expr(42, 2),
                    Expr::Const(Value::Text("ab".into())),
                ],
            ),
        );

        assert!(!relation_may_satisfy_own_partition_bound(
            &catalog,
            list_leaf_oid,
            Some(&expr)
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
    fn timestamptz_comparison_is_runtime_only_for_timestamp_partition_key() {
        let spec = timestamp_range_spec();
        let first = int_range_bound(timestamp_value(0), timestamp_value(10));
        let second = int_range_bound(timestamp_value(10), timestamp_value(20));
        let expr = Expr::op_auto(
            OpExprKind::Lt,
            vec![
                Expr::Cast(
                    Box::new(timestamp_key_expr()),
                    SqlType::new(SqlTypeKind::TimestampTz),
                ),
                Expr::Const(Value::TimestampTz(TimestampTzADT(10))),
            ],
        );
        let catalog = MockCatalog::default();

        assert!(expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
        assert!(partition_may_satisfy_filter_with_runtime_values(
            &spec,
            Some(&first),
            &[],
            &expr,
            Some(&catalog),
            runtime_test_value
        ));
        assert!(!partition_may_satisfy_filter_with_runtime_values(
            &spec,
            Some(&second),
            &[],
            &expr,
            Some(&catalog),
            runtime_test_value
        ));
    }

    #[test]
    fn timestamptz_scalar_array_is_runtime_only_for_timestamp_partition_key() {
        let spec = timestamp_range_spec();
        let first = int_range_bound(timestamp_value(0), timestamp_value(10));
        let second = int_range_bound(timestamp_value(10), timestamp_value(20));
        let expr = Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op: SubqueryComparisonOp::Eq,
            use_or: true,
            left: Box::new(timestamp_key_expr()),
            right: Box::new(Expr::ArrayLiteral {
                elements: vec![Expr::Const(Value::TimestampTz(TimestampTzADT(15)))],
                array_type: SqlType::array_of(SqlType::new(SqlTypeKind::TimestampTz)),
            }),
            collation_oid: None,
        }));
        let catalog = MockCatalog::default();

        assert!(expr_may_match_bound(&expr, &spec, &first, &[]));
        assert!(expr_may_match_bound(&expr, &spec, &second, &[]));
        assert!(!partition_may_satisfy_filter_with_runtime_values(
            &spec,
            Some(&first),
            &[],
            &expr,
            Some(&catalog),
            runtime_test_value
        ));
        assert!(partition_may_satisfy_filter_with_runtime_values(
            &spec,
            Some(&second),
            &[],
            &expr,
            Some(&catalog),
            runtime_test_value
        ));
    }

    fn runtime_test_value(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Const(value) => Some(value.clone()),
            _ => None,
        }
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
