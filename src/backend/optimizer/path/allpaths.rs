use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::compare_order_values;
use crate::backend::parser::analyze::bind_relation_constraints;
use crate::backend::parser::{
    BoundIndexRelation, CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec,
    PartitionRangeDatumValue, PartitionStrategy, SerializedPartitionValue, SubqueryComparisonOp,
    deserialize_partition_bound, is_binary_coercible_type, partition_value_to_value,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::date::parse_date_text;
use crate::include::catalog::PG_LARGEOBJECT_METADATA_RELATION_OID;
use crate::include::catalog::{BTREE_AM_OID, HASH_AM_OID};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntryKind, SetOperator, SqlType, SqlTypeKind, TableSampleClause,
};
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerConfig, PlannerIndexExprCacheEntry, PlannerInfo,
    PlannerSubroot, RelOptInfo, RelOptKind, RestrictInfo, SpecialJoinInfo,
};
use crate::include::nodes::plannodes::{
    AggregateStrategy, PartitionPruneChildDomain, PartitionPrunePlan, PlanEstimate, SetOpStrategy,
};
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, JoinType, OpExprKind, OrderByEntry, QueryColumn,
    RelationDesc, ScalarArrayOpExpr, ScalarFunctionImpl, SortGroupClause, ToastRelationRef, Var,
    attrno_index, expr_contains_set_returning, expr_sql_type_hint, is_system_attr,
    set_returning_call_exprs, user_attrno,
};

use super::super::bestpath;
use super::super::inherit::{
    append_child_rtindexes, append_translation, expand_inherited_rtentries,
    translate_append_rel_expr,
};
use super::super::joininfo;
use super::super::partition_cache;
use super::super::partition_prune::{
    partition_may_satisfy_filter_for_relation, relation_may_satisfy_own_partition_bound,
};
use super::super::partitionwise;
use super::super::pathnodes::{
    next_synthetic_slot_id, rte_slot_id, rte_slot_varno, slot_output_target,
};
use super::super::plan::grouping_planner;
use super::super::util::{
    normalize_rte_path, pathkeys_to_order_items, project_to_slot_layout,
    required_query_pathkeys_for_rel, strip_binary_coercible_casts,
};
use super::super::{
    CPU_OPERATOR_COST, IndexPathSpec, JoinBuildSpec, and_exprs, expand_join_rte_vars, expr_relids,
    flatten_and_conjuncts, has_outer_joins, is_pushable_base_clause, path_relids, predicate_cost,
    pull_up_sublinks, relids_disjoint, relids_overlap, relids_subset, relids_union,
    reverse_join_type,
};
use super::subquery_prune::{prune_unused_subquery_outputs, used_parent_attrs_for_rte};
use super::{
    build_index_path_spec, build_join_paths_with_root, estimate_bitmap_candidate,
    estimate_index_candidate, estimate_seqscan_candidate, full_index_scan_spec,
    index_supports_index_only_attrs, optimize_path_with_config, relation_stats,
};

type PlannerIndexExprCache = RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>;

fn spec_prefers_plain_index_scan(spec: &IndexPathSpec) -> bool {
    spec_prefers_plain_network_btree_scan(spec) || hash_index_gettuple_supported(&spec.index)
}

fn spec_prefers_plain_network_btree_scan(spec: &IndexPathSpec) -> bool {
    spec.index.index_meta.am_oid == BTREE_AM_OID
        && spec.filter_quals.iter().any(expr_is_network_range_filter)
}

fn expr_is_network_range_filter(expr: &Expr) -> bool {
    let Expr::Func(func) = expr else {
        return false;
    };
    matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::NetworkSubnet
                | BuiltinScalarFunction::NetworkSubnetEq
                | BuiltinScalarFunction::NetworkSupernet
                | BuiltinScalarFunction::NetworkSupernetEq
        )
    )
}

fn collect_inner_join_clauses(root: &PlannerInfo) -> Vec<RestrictInfo> {
    fn walk(root: &PlannerInfo, node: &JoinTreeNode, clauses: &mut Vec<RestrictInfo>) {
        if let JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            ..
        } = node
        {
            walk(root, left, clauses);
            walk(root, right, clauses);
            if matches!(kind, JoinType::Inner | JoinType::Cross) {
                clauses.push(joininfo::make_restrict_info(expand_join_rte_vars(
                    root,
                    quals.clone(),
                )));
            }
        }
    }

    let mut clauses = Vec::new();
    if let Some(jointree) = root.parse.jointree.as_ref() {
        walk(root, jointree, &mut clauses);
    }
    if !has_outer_joins(root) {
        if let Some(where_qual) = root.parse.where_qual.as_ref() {
            clauses.extend(
                flatten_and_conjuncts(where_qual)
                    .into_iter()
                    .map(|clause| expand_join_rte_vars(root, clause))
                    .filter(|clause| expr_relids(clause).len() > 1)
                    .map(joininfo::make_restrict_info),
            );
        }
    }
    clauses
}

pub(super) fn residual_where_qual(root: &PlannerInfo) -> Option<Expr> {
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return None;
    };
    let clauses = flatten_and_conjuncts(where_qual)
        .into_iter()
        .map(|clause| expand_join_rte_vars(root, clause))
        .filter(|clause| {
            let relids = expr_relids(clause);
            let pushed_to_single_base =
                relids.is_empty() && single_direct_base_relid(root).is_some();
            !pushed_to_single_base
                && !(!has_outer_joins(root) && relids.len() > 1)
                && !is_pushable_base_clause(root, &relids)
        })
        .collect();
    and_exprs(clauses)
}

fn assign_base_restrictinfo(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    for rel in root.simple_rel_array.iter_mut().flatten() {
        rel.baserestrictinfo.clear();
        rel.joininfo.clear();
    }
    for (rtindex, rte) in root.parse.rtable.iter().enumerate() {
        if !matches!(rte.kind, RangeTblEntryKind::Relation { .. }) {
            continue;
        }
        let relid = rtindex + 1;
        let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        else {
            continue;
        };
        rel.baserestrictinfo
            .extend(rte.security_quals.iter().cloned().enumerate().map(
                |(security_level, clause)| {
                    joininfo::make_restrict_info_with_security(clause, security_level, catalog)
                },
            ));
    }
    if let Some(where_qual) = root.parse.where_qual.as_ref() {
        for clause in flatten_and_conjuncts(where_qual) {
            let clause = expand_join_rte_vars(root, clause);
            let relids = expr_relids(&clause);
            let varless_clause = relids.is_empty();
            let push_relid = if varless_clause {
                single_direct_base_relid(root)
            } else if is_pushable_base_clause(root, &relids) {
                relids.first().copied()
            } else {
                nullable_outer_join_filter_pushdown_relid(root, &clause, &relids)
            };
            if let Some(relid) = push_relid
                && let Some(rel) = root
                    .simple_rel_array
                    .get_mut(relid)
                    .and_then(Option::as_mut)
            {
                let security_level = root
                    .parse
                    .rtable
                    .get(relid.saturating_sub(1))
                    .map(|rte| {
                        if varless_clause {
                            0
                        } else {
                            rte.security_quals.len()
                        }
                    })
                    .unwrap_or(0);
                let restrict =
                    joininfo::make_restrict_info_with_security(clause, security_level, catalog);
                rel.baserestrictinfo.push(restrict);
            }
        }
    }
    derive_base_equalities_from_inner_join_clauses(root);
}

fn derive_base_equalities_from_inner_join_clauses(root: &mut PlannerInfo) {
    if has_outer_joins(root) {
        return;
    }
    let join_clauses = collect_inner_join_clauses(root);
    let mut derived = Vec::new();
    for left_index in 0..join_clauses.len() {
        for right_index in (left_index + 1)..join_clauses.len() {
            let Some((left_a, left_b)) = equality_clause_args(&join_clauses[left_index].clause)
            else {
                continue;
            };
            let Some((right_a, right_b)) = equality_clause_args(&join_clauses[right_index].clause)
            else {
                continue;
            };
            for (left_expr, right_expr) in
                implied_same_relation_equalities(left_a, left_b, right_a, right_b)
            {
                if left_expr == right_expr {
                    continue;
                }
                let left_relids = expr_relids(left_expr);
                let right_relids = expr_relids(right_expr);
                if left_relids.len() == 1
                    && left_relids == right_relids
                    && is_pushable_base_clause(root, &left_relids)
                {
                    let clause = Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
                        op: OpExprKind::Eq,
                        opno: 0,
                        opfuncid: 0,
                        opresulttype: SqlType::new(SqlTypeKind::Bool),
                        args: vec![left_expr.clone(), right_expr.clone()],
                        collation_oid: None,
                    }));
                    derived.push((left_relids[0], joininfo::make_restrict_info(clause)));
                }
            }
        }
    }

    for (relid, restrict) in derived {
        let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        else {
            continue;
        };
        if rel
            .baserestrictinfo
            .iter()
            .any(|existing| equalities_match_commuted(&existing.clause, &restrict.clause))
        {
            continue;
        }
        rel.baserestrictinfo.push(restrict);
    }
}

fn equality_clause_args(expr: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::Op(op) = expr else {
        return None;
    };
    if op.op != OpExprKind::Eq || op.args.len() != 2 {
        return None;
    }
    Some((&op.args[0], &op.args[1]))
}

fn implied_same_relation_equalities<'a>(
    left_a: &'a Expr,
    left_b: &'a Expr,
    right_a: &'a Expr,
    right_b: &'a Expr,
) -> Vec<(&'a Expr, &'a Expr)> {
    let mut implied = Vec::new();
    if left_a == right_a {
        implied.push((left_b, right_b));
    }
    if left_a == right_b {
        implied.push((left_b, right_a));
    }
    if left_b == right_a {
        implied.push((left_a, right_b));
    }
    if left_b == right_b {
        implied.push((left_a, right_a));
    }
    implied
}

fn equalities_match_commuted(left: &Expr, right: &Expr) -> bool {
    let Some((left_a, left_b)) = equality_clause_args(left) else {
        return false;
    };
    let Some((right_a, right_b)) = equality_clause_args(right) else {
        return false;
    };
    (left_a == right_a && left_b == right_b) || (left_a == right_b && left_b == right_a)
}

fn single_direct_base_relid(root: &PlannerInfo) -> Option<usize> {
    let mut direct_relids = root
        .parse
        .rtable
        .iter()
        .enumerate()
        .filter_map(|(index, rte)| {
            if matches!(rte.kind, RangeTblEntryKind::Relation { .. })
                && root
                    .simple_rel_array
                    .get(index + 1)
                    .and_then(Option::as_ref)
                    .is_some()
            {
                Some(index + 1)
            } else {
                None
            }
        });
    let relid = direct_relids.next()?;
    direct_relids.next().is_none().then_some(relid)
}

fn nullable_outer_join_filter_pushdown_relid(
    root: &PlannerInfo,
    clause: &Expr,
    relids: &[usize],
) -> Option<usize> {
    if relids.len() != 1 {
        return None;
    }
    let relid = relids[0];
    if !super::super::base_rel_is_nullable_by_outer_join(root, relid) {
        return None;
    }
    if !joininfo::strict_relids(clause).contains(&relid) {
        return None;
    }
    root.simple_rel_array
        .get(relid)
        .and_then(Option::as_ref)
        .map(|_| relid)
}

fn expr_is_nonnullable_for_restriction(root: &PlannerInfo, expr: &Expr) -> bool {
    match expr {
        Expr::Const(Value::Null) => false,
        Expr::Const(_) => true,
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_is_nonnullable_for_restriction(root, inner)
        }
        Expr::Var(var) => {
            if var.varlevelsup != 0 {
                return false;
            }
            if var.varattno < 0 {
                return true;
            }
            let relid = rte_slot_varno(var.varno).unwrap_or(var.varno);
            if super::super::base_rel_is_nullable_by_outer_join(root, relid) {
                return false;
            }
            let Some(index) = attrno_index(var.varattno) else {
                return false;
            };
            root.parse
                .rtable
                .get(relid.saturating_sub(1))
                .and_then(|rte| rte.desc.columns.get(index))
                .is_some_and(|column| !column.storage.nullable)
        }
        _ => false,
    }
}

fn simplify_nullability_restriction(root: &PlannerInfo, expr: Expr) -> Expr {
    match expr {
        Expr::IsNull(inner) => {
            if expr_is_nonnullable_for_restriction(root, &inner) {
                Expr::Const(Value::Bool(false))
            } else {
                Expr::IsNull(inner)
            }
        }
        Expr::IsNotNull(inner) => {
            if expr_is_nonnullable_for_restriction(root, &inner) {
                Expr::Const(Value::Bool(true))
            } else {
                Expr::IsNotNull(inner)
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            let original = Expr::Bool(bool_expr.clone());
            let simplified = bool_expr
                .args
                .iter()
                .cloned()
                .map(|arg| simplify_nullability_restriction(root, arg))
                .collect::<Vec<_>>();
            if simplified
                .iter()
                .any(|arg| matches!(arg, Expr::Const(Value::Bool(true))))
            {
                Expr::Const(Value::Bool(true))
            } else if !simplified.is_empty()
                && simplified
                    .iter()
                    .all(|arg| matches!(arg, Expr::Const(Value::Bool(false))))
            {
                Expr::Const(Value::Bool(false))
            } else {
                original
            }
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let mut simplified_args = Vec::new();
            for arg in bool_expr.args {
                match simplify_nullability_restriction(root, arg) {
                    Expr::Const(Value::Bool(false)) => return Expr::Const(Value::Bool(false)),
                    Expr::Const(Value::Bool(true)) => {}
                    other => simplified_args.push(other),
                }
            }
            match simplified_args.len() {
                0 => Expr::Const(Value::Bool(true)),
                1 => simplified_args.pop().expect("single simplified predicate"),
                _ => Expr::bool_expr(BoolExprType::And, simplified_args),
            }
        }
        other => other,
    }
}

fn restrict_info_with_nullability_simplification(
    root: &PlannerInfo,
    restrict: RestrictInfo,
) -> Option<RestrictInfo> {
    if is_minmax_null_boundary_restriction(root, &restrict.clause) {
        return Some(restrict);
    }
    let clause = simplify_nullability_restriction(root, restrict.clause.clone());
    if matches!(clause, Expr::Const(Value::Bool(true))) {
        None
    } else {
        Some(joininfo::translated_restrict_info(clause, &restrict))
    }
}

fn is_minmax_null_boundary_restriction(root: &PlannerInfo, expr: &Expr) -> bool {
    matches!(expr, Expr::IsNotNull(_))
        && root.parse.limit_count == Some(1)
        && !root.parse.sort_clause.is_empty()
}

fn simplify_base_restrictinfo(root: &mut PlannerInfo) {
    for relid in 1..root.simple_rel_array.len() {
        let Some(restricts) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
            .map(|rel| std::mem::take(&mut rel.baserestrictinfo))
        else {
            continue;
        };
        let simplified = restricts
            .into_iter()
            .filter_map(|restrict| restrict_info_with_nullability_simplification(root, restrict))
            .collect::<Vec<_>>();
        if let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        {
            rel.baserestrictinfo = simplified;
        }
    }
}

fn base_filter_expr(rel: &RelOptInfo) -> Option<Expr> {
    super::super::and_exprs(ordered_base_restrict_exprs(rel))
}

pub(super) fn ordered_base_restrict_exprs(rel: &RelOptInfo) -> Vec<Expr> {
    let mut items = rel
        .baserestrictinfo
        .iter()
        .enumerate()
        .map(|(index, restrict)| {
            let cost = qual_order_cost(&restrict.clause) * CPU_OPERATOR_COST;
            let security_level = if restrict.leakproof && cost < 10.0 * CPU_OPERATOR_COST {
                0
            } else {
                restrict.security_level
            };
            (security_level, cost, index, restrict.clause.clone())
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal))
            .then_with(|| left.2.cmp(&right.2))
    });
    items.into_iter().map(|(_, _, _, clause)| clause).collect()
}

fn qual_order_cost(expr: &Expr) -> f64 {
    match expr {
        Expr::Op(op) => 1.0 + op.args.iter().map(qual_order_cost).sum::<f64>(),
        Expr::Func(func) => 10.0 + func.args.iter().map(qual_order_cost).sum::<f64>(),
        Expr::Bool(bool_expr) => 1.0 + bool_expr.args.iter().map(qual_order_cost).sum::<f64>(),
        Expr::Coalesce(left, right) => 1.0 + qual_order_cost(left) + qual_order_cost(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => 1.0 + qual_order_cost(inner),
        _ => predicate_cost(expr),
    }
}

fn scalar_array_null_filter(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarArrayOp(saop) => expr_is_null_array(&saop.right),
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            bool_expr.args.iter().any(scalar_array_null_filter)
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => scalar_array_null_filter(inner),
        _ => false,
    }
}

fn partitioned_scalar_array_null_filter(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarArrayOp(saop) => partitioned_scalar_array_null_op_is_foldable(saop),
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .any(partitioned_scalar_array_null_filter),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            partitioned_scalar_array_null_filter(inner)
        }
        _ => false,
    }
}

fn partitioned_scalar_array_null_op_is_foldable(saop: &ScalarArrayOpExpr) -> bool {
    let Some(left_type) = transparent_scalar_array_left_type(&saop.left) else {
        return false;
    };
    let Some(array_type) = null_array_type(&saop.right) else {
        return matches!(*saop.right, Expr::Const(Value::Null));
    };
    if !array_type.is_array {
        return false;
    }
    let element_type = array_type.element_type();
    is_binary_coercible_type(left_type, element_type)
        || is_binary_coercible_type(element_type, left_type)
}

fn transparent_scalar_array_left_type(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Cast(inner, target_type) => {
            let source_type = transparent_scalar_array_left_type(inner)?;
            is_binary_coercible_type(source_type, *target_type).then_some(*target_type)
        }
        Expr::Collate { expr: inner, .. } => transparent_scalar_array_left_type(inner),
        Expr::Var(_) | Expr::Param(_) => expr_sql_type_hint(expr),
        _ => None,
    }
}

fn expr_is_null_array(expr: &Expr) -> bool {
    null_array_type(expr).is_some() || matches!(expr, Expr::Const(Value::Null))
}

fn null_array_type(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Cast(inner, ty) if expr_is_null_array(inner) => Some(*ty),
        Expr::Collate { expr: inner, .. } => null_array_type(inner),
        _ => None,
    }
}

fn add_one_time_false_path(
    rel: &mut RelOptInfo,
    source_id: usize,
    desc: RelationDesc,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) {
    let pathtarget = rel.reltarget.clone();
    rel.add_path(optimize_path_with_config(
        Path::Filter {
            plan_info: PlanEstimate::default(),
            pathtarget: pathtarget.clone(),
            input: Box::new(Path::Append {
                plan_info: PlanEstimate::default(),
                pathtarget: pathtarget.clone(),
                pathkeys: Vec::new(),
                relids: vec![source_id],
                source_id,
                desc,
                child_roots: Vec::new(),
                partition_prune: None,
                children: Vec::new(),
            }),
            predicate: Expr::Const(Value::Bool(false)),
        },
        catalog,
        config,
    ));
    bestpath::set_cheapest(rel);
}

fn is_append_child_rel(root: &PlannerInfo, rtindex: usize) -> bool {
    root.simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .is_some_and(|rel| matches!(rel.reloptkind, RelOptKind::OtherMemberRel))
}

fn is_regular_inheritance_child_rel(root: &PlannerInfo, rtindex: usize) -> bool {
    let Some(parent_relid) = root
        .append_rel_infos
        .get(rtindex)
        .and_then(Option::as_ref)
        .map(|info| info.parent_relid)
    else {
        return false;
    };
    root.parse
        .rtable
        .get(parent_relid.saturating_sub(1))
        .and_then(|rte| match rte.kind {
            RangeTblEntryKind::Relation { relkind, .. } => Some(relkind),
            _ => None,
        })
        .is_some_and(|relkind| relkind != 'p')
}

fn base_restrictinfo_is_contradictory(rel: &RelOptInfo) -> bool {
    if rel
        .baserestrictinfo
        .iter()
        .any(|restrict| matches!(restrict.clause, Expr::Const(Value::Bool(false))))
    {
        return true;
    }

    expr_list_has_contradictory_equalities(
        rel.baserestrictinfo
            .iter()
            .flat_map(|restrict| flatten_and_conjuncts(&restrict.clause)),
    )
}

fn exprs_have_contradictory_equalities(left: &Expr, right: &Expr) -> bool {
    let clauses = flatten_and_conjuncts(left)
        .into_iter()
        .chain(flatten_and_conjuncts(right))
        .collect::<Vec<_>>();
    expr_list_has_contradictory_equalities(clauses.iter().cloned())
        || expr_list_has_disjoint_ranges(clauses)
}

fn expr_list_has_contradictory_equalities(clauses: impl IntoIterator<Item = Expr>) -> bool {
    let mut equalities = Vec::<(Expr, Value, Option<u32>)>::new();
    for clause in clauses {
        let Some((expr, value, collation_oid)) = equality_to_nonnull_const(&clause) else {
            continue;
        };
        if equalities
            .iter()
            .any(|(existing_expr, existing_value, existing_collation_oid)| {
                equality_exprs_match_for_contradiction(existing_expr, &expr)
                    && existing_value != &value
                    && *existing_collation_oid == collation_oid
            })
        {
            return true;
        }
        equalities.push((expr, value, collation_oid));
    }
    false
}

fn equality_exprs_match_for_contradiction(left: &Expr, right: &Expr) -> bool {
    left == right
        || matches!(
            (left, right),
            (Expr::Var(left), Expr::Var(right))
                if left.varlevelsup == 0
                    && right.varlevelsup == 0
                    && left.varattno == right.varattno
                    && left.vartype == right.vartype
        )
}

fn equality_to_nonnull_const(expr: &Expr) -> Option<(Expr, Value, Option<u32>)> {
    let Expr::Op(op) = expr else {
        return None;
    };
    if op.op != OpExprKind::Eq || op.args.len() != 2 {
        return None;
    }
    let collation_oid = op
        .collation_oid
        .or_else(|| op.args.iter().find_map(top_level_explicit_collation));
    match (&op.args[0], &op.args[1]) {
        (Expr::Const(value), other) | (other, Expr::Const(value))
            if !matches!(value, Value::Null) =>
        {
            Some((other.clone(), value.clone(), collation_oid))
        }
        _ => None,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RangeBoundKind {
    Lower,
    Upper,
}

#[derive(Clone)]
struct RangeBound {
    value: Value,
    inclusive: bool,
}

struct RangeRestriction {
    expr: Expr,
    collation_oid: Option<u32>,
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
}

impl RangeRestriction {
    fn new(expr: Expr, collation_oid: Option<u32>) -> Self {
        Self {
            expr,
            collation_oid,
            lower: None,
            upper: None,
        }
    }

    fn add_bound(&mut self, kind: RangeBoundKind, bound: RangeBound) {
        match kind {
            RangeBoundKind::Lower => merge_lower_bound(&mut self.lower, bound, self.collation_oid),
            RangeBoundKind::Upper => merge_upper_bound(&mut self.upper, bound, self.collation_oid),
        }
    }

    fn is_contradictory(&self) -> bool {
        let (Some(lower), Some(upper)) = (&self.lower, &self.upper) else {
            return false;
        };
        match compare_range_values(&lower.value, &upper.value, self.collation_oid) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => !(lower.inclusive && upper.inclusive),
            _ => false,
        }
    }
}

fn merge_lower_bound(
    existing: &mut Option<RangeBound>,
    incoming: RangeBound,
    collation_oid: Option<u32>,
) {
    let Some(current) = existing else {
        *existing = Some(incoming);
        return;
    };
    match compare_range_values(&incoming.value, &current.value, collation_oid) {
        Some(Ordering::Greater) => *current = incoming,
        Some(Ordering::Equal) => current.inclusive &= incoming.inclusive,
        _ => {}
    }
}

fn merge_upper_bound(
    existing: &mut Option<RangeBound>,
    incoming: RangeBound,
    collation_oid: Option<u32>,
) {
    let Some(current) = existing else {
        *existing = Some(incoming);
        return;
    };
    match compare_range_values(&incoming.value, &current.value, collation_oid) {
        Some(Ordering::Less) => *current = incoming,
        Some(Ordering::Equal) => current.inclusive &= incoming.inclusive,
        _ => {}
    }
}

fn compare_range_values(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return None;
    }
    compare_order_values(left, right, collation_oid, Some(false), false).ok()
}

fn expr_list_has_disjoint_ranges(clauses: impl IntoIterator<Item = Expr>) -> bool {
    let mut ranges = Vec::<RangeRestriction>::new();
    for clause in clauses {
        for (expr, kind, value, inclusive, collation_oid) in range_bounds_to_nonnull_const(&clause)
        {
            let expr = strip_binary_coercible_casts(&expr);
            let bound = RangeBound { value, inclusive };
            if let Some(existing) = ranges.iter_mut().find(|range| {
                range.collation_oid == collation_oid
                    && equality_exprs_match_for_contradiction(&range.expr, &expr)
            }) {
                existing.add_bound(kind, bound);
                if existing.is_contradictory() {
                    return true;
                }
            } else {
                let mut range = RangeRestriction::new(expr, collation_oid);
                range.add_bound(kind, bound);
                ranges.push(range);
            }
        }
    }
    false
}

fn range_bounds_to_nonnull_const(
    expr: &Expr,
) -> Vec<(Expr, RangeBoundKind, Value, bool, Option<u32>)> {
    let Expr::Op(op) = expr else {
        return Vec::new();
    };
    if op.args.len() != 2 {
        return Vec::new();
    }
    let collation_oid = op
        .collation_oid
        .or_else(|| op.args.iter().find_map(top_level_explicit_collation));
    let mut bounds = Vec::new();
    match (
        range_const_value(&op.args[0]),
        range_const_value(&op.args[1]),
    ) {
        (None, Some(value)) => {
            push_range_bounds_for_operator(
                &mut bounds,
                op.args[0].clone(),
                op.op,
                value,
                false,
                collation_oid,
            );
        }
        (Some(value), None) => {
            push_range_bounds_for_operator(
                &mut bounds,
                op.args[1].clone(),
                op.op,
                value,
                true,
                collation_oid,
            );
        }
        _ => {}
    }
    bounds
}

fn range_const_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Const(value) if !matches!(value, Value::Null) => Some(value.clone()),
        Expr::Cast(inner, ty) if matches!(ty.kind, SqlTypeKind::Date) => {
            let text = match inner.as_ref() {
                Expr::Const(value) => value.as_text()?,
                _ => return None,
            };
            parse_date_text(text, &DateTimeConfig::default())
                .ok()
                .map(Value::Date)
        }
        _ => None,
    }
}

fn push_range_bounds_for_operator(
    out: &mut Vec<(Expr, RangeBoundKind, Value, bool, Option<u32>)>,
    expr: Expr,
    op: OpExprKind,
    value: Value,
    const_on_left: bool,
    collation_oid: Option<u32>,
) {
    let (kind, inclusive) = match (op, const_on_left) {
        (OpExprKind::Eq, _) => {
            out.push((
                expr.clone(),
                RangeBoundKind::Lower,
                value.clone(),
                true,
                collation_oid,
            ));
            (RangeBoundKind::Upper, true)
        }
        (OpExprKind::Gt, false) | (OpExprKind::Lt, true) => (RangeBoundKind::Lower, false),
        (OpExprKind::GtEq, false) | (OpExprKind::LtEq, true) => (RangeBoundKind::Lower, true),
        (OpExprKind::Lt, false) | (OpExprKind::Gt, true) => (RangeBoundKind::Upper, false),
        (OpExprKind::LtEq, false) | (OpExprKind::GtEq, true) => (RangeBoundKind::Upper, true),
        _ => return,
    };
    out.push((expr, kind, value, inclusive, collation_oid));
}

fn top_level_explicit_collation(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        Expr::Cast(inner, _) => top_level_explicit_collation(inner),
        _ => None,
    }
}

fn const_false_relation_path(rtindex: usize, desc: &RelationDesc) -> Path {
    let pathtarget = slot_output_target(rtindex, &desc.columns, |column| column.sql_type);
    Path::Filter {
        plan_info: PlanEstimate::default(),
        pathtarget: pathtarget.clone(),
        predicate: Expr::Const(Value::Bool(false)),
        input: Box::new(Path::Append {
            plan_info: PlanEstimate::default(),
            pathtarget,
            pathkeys: Vec::new(),
            relids: vec![rtindex],
            source_id: rtindex,
            desc: desc.clone(),
            child_roots: Vec::new(),
            partition_prune: None,
            children: Vec::new(),
        }),
    }
}

fn path_is_const_false_filter(path: &Path) -> bool {
    match path {
        Path::Filter {
            predicate: Expr::Const(Value::Bool(false)),
            ..
        } => true,
        Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::SubqueryScan { input, .. } => path_is_const_false_filter(input),
        _ => false,
    }
}

fn relation_oid_for_rtindex(root: &PlannerInfo, rtindex: usize) -> Option<u32> {
    root.parse
        .rtable
        .get(rtindex.saturating_sub(1))
        .and_then(|rte| match rte.kind {
            RangeTblEntryKind::Relation { relation_oid, .. } => Some(relation_oid),
            _ => None,
        })
}

fn partition_bound_for_rtindex(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    rtindex: usize,
) -> Option<PartitionBoundSpec> {
    let child_oid = relation_oid_for_rtindex(root, rtindex)?;
    let parent_oid = root
        .append_rel_infos
        .get(rtindex)
        .and_then(Option::as_ref)
        .and_then(|info| relation_oid_for_rtindex(root, info.parent_relid))?;
    partition_cache::partition_child_bounds(root, catalog, parent_oid)
        .into_iter()
        .find(|child| child.row.inhrelid == child_oid)
        .and_then(|child| child.bound)
}

fn relation_own_partition_bound(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Option<PartitionBoundSpec> {
    catalog
        .relation_by_oid(relation_oid)?
        .relpartbound
        .as_deref()
        .and_then(|text| deserialize_partition_bound(text).ok())
}

fn append_partition_prune_plan(
    spec: Option<crate::backend::parser::LoweredPartitionSpec>,
    sibling_bounds: &[PartitionBoundSpec],
    filter: Option<&Expr>,
    child_bounds: &[Option<PartitionBoundSpec>],
) -> Option<PartitionPrunePlan> {
    let spec = spec?;
    let filter = filter?.clone();
    if child_bounds.is_empty() {
        return None;
    }
    Some(PartitionPrunePlan {
        child_domains: child_bounds
            .iter()
            .map(|bound| {
                vec![PartitionPruneChildDomain {
                    spec: spec.clone(),
                    sibling_bounds: sibling_bounds.to_vec(),
                    bound: bound.clone(),
                }]
            })
            .collect(),
        spec,
        sibling_bounds: sibling_bounds.to_vec(),
        filter,
        child_bounds: child_bounds.to_vec(),
        subplans_removed: 0,
    })
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

fn partition_bound_cmp(left: &PartitionBoundSpec, right: &PartitionBoundSpec) -> Ordering {
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
                .map(|(left, right)| serialized_partition_value_cmp(left, right))
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

fn sorted_append_child_rtindexes(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    parent_rtindex: usize,
) -> Vec<usize> {
    let mut children = append_child_rtindexes(root, parent_rtindex);
    let parent_oid = relation_oid_for_rtindex(root, parent_rtindex);
    let partition_children = parent_oid
        .map(|oid| partition_cache::partition_child_bounds(root, catalog, oid))
        .unwrap_or_default();
    children.sort_by(|left, right| {
        let left_bound = relation_oid_for_rtindex(root, *left).and_then(|left_oid| {
            partition_children
                .iter()
                .find(|child| child.row.inhrelid == left_oid)
                .and_then(|child| child.bound.as_ref())
        });
        let right_bound = relation_oid_for_rtindex(root, *right).and_then(|right_oid| {
            partition_children
                .iter()
                .find(|child| child.row.inhrelid == right_oid)
                .and_then(|child| child.bound.as_ref())
        });
        match (left_bound, right_bound) {
            (Some(left_bound), Some(right_bound)) => {
                partition_bound_cmp(left_bound, right_bound).then_with(|| left.cmp(right))
            }
            _ => left.cmp(right),
        }
    });
    children
}

struct OrderedAppendProof {
    pathkeys: Vec<PathKey>,
    reverse_children: bool,
}

fn ordered_partition_append_proof(
    root: &PlannerInfo,
    spec: Option<&LoweredPartitionSpec>,
    kept_bounds: &[PartitionBoundSpec],
    filter: Option<&Expr>,
    pathkeys: &[PathKey],
) -> Option<OrderedAppendProof> {
    let spec = spec?;
    if pathkeys.is_empty() || kept_bounds.is_empty() || spec.key_exprs.is_empty() {
        return None;
    }
    if kept_bounds.iter().any(PartitionBoundSpec::is_default) {
        return None;
    }
    let first_order_key = pathkeys.first()?;
    let reverse_children = first_order_key.descending;
    let leading_equal_keys = spec
        .key_exprs
        .iter()
        .take_while(|key_expr| partition_key_fixed_by_filter(root, key_expr, filter))
        .count();
    let remaining_keys = &spec.key_exprs[leading_equal_keys..];
    let matched_keys = matching_partition_pathkey_prefix(root, remaining_keys, pathkeys)?;
    if matched_keys == 0 {
        return None;
    }
    if pathkeys.iter().take(matched_keys).any(|key| {
        key.descending != reverse_children || key.nulls_first != first_order_key.nulls_first
    }) {
        return None;
    }
    if pathkeys.len() > matched_keys && matched_keys < remaining_keys.len() {
        return None;
    }
    let ordered = match spec.strategy {
        PartitionStrategy::Range => range_partition_bounds_can_append(kept_bounds),
        PartitionStrategy::List => list_partition_bounds_can_append(kept_bounds),
        PartitionStrategy::Hash => false,
    };
    ordered.then(|| OrderedAppendProof {
        pathkeys: pathkeys.to_vec(),
        reverse_children,
    })
}

fn matching_partition_pathkey_prefix(
    root: &PlannerInfo,
    partition_keys: &[Expr],
    pathkeys: &[PathKey],
) -> Option<usize> {
    let matched = partition_keys
        .iter()
        .zip(pathkeys)
        .take_while(|(partition_key, pathkey)| {
            expressions_match_for_partition_order(root, partition_key, &pathkey.expr)
        })
        .count();
    (matched > 0).then_some(matched)
}

fn expressions_match_for_partition_order(root: &PlannerInfo, left: &Expr, right: &Expr) -> bool {
    let left = strip_binary_coercible_casts(&expand_join_rte_vars(root, left.clone()));
    let right = strip_binary_coercible_casts(&expand_join_rte_vars(root, right.clone()));
    left == right
}

fn partition_key_fixed_by_filter(
    root: &PlannerInfo,
    key_expr: &Expr,
    filter: Option<&Expr>,
) -> bool {
    let Some(filter) = filter else {
        return false;
    };
    flatten_and_conjuncts(filter)
        .into_iter()
        .any(|clause| partition_key_fixed_by_clause(root, key_expr, &clause))
}

fn partition_key_fixed_by_clause(root: &PlannerInfo, key_expr: &Expr, clause: &Expr) -> bool {
    match clause {
        Expr::Op(op) if matches!(op.op, crate::include::nodes::primnodes::OpExprKind::Eq) => {
            let [left, right] = op.args.as_slice() else {
                return false;
            };
            (expressions_match_for_partition_order(root, key_expr, left)
                && expr_is_nonnull_const(right))
                || (expressions_match_for_partition_order(root, key_expr, right)
                    && expr_is_nonnull_const(left))
        }
        Expr::ScalarArrayOp(op) if op.use_or && matches!(op.op, SubqueryComparisonOp::Eq) => {
            expressions_match_for_partition_order(root, key_expr, &op.left)
                && scalar_array_has_single_nonnull_const(&op.right)
        }
        Expr::Var(_) if expressions_match_for_partition_order(root, key_expr, clause) => true,
        Expr::Bool(bool_expr) if matches!(bool_expr.boolop, BoolExprType::Not) => bool_expr
            .args
            .first()
            .is_some_and(|inner| expressions_match_for_partition_order(root, key_expr, inner)),
        _ => false,
    }
}

fn expr_is_nonnull_const(expr: &Expr) -> bool {
    matches!(expr, Expr::Const(value) if !matches!(value, Value::Null))
}

fn scalar_array_has_single_nonnull_const(expr: &Expr) -> bool {
    match expr {
        Expr::Const(Value::Array(values)) => values.len() == 1 && !matches!(values[0], Value::Null),
        Expr::Const(Value::PgArray(array)) => {
            array.elements.len() == 1 && !matches!(array.elements[0], Value::Null)
        }
        _ => false,
    }
}

fn range_partition_bounds_can_append(bounds: &[PartitionBoundSpec]) -> bool {
    bounds.iter().all(|bound| {
        matches!(
            bound,
            PartitionBoundSpec::Range {
                is_default: false,
                ..
            }
        )
    })
}

fn list_partition_bounds_can_append(bounds: &[PartitionBoundSpec]) -> bool {
    let mut previous_max = None;
    for bound in bounds {
        let Some((min, max)) = list_bound_value_span(bound) else {
            return false;
        };
        if let Some(previous) = previous_max.as_ref()
            && !matches!(
                serialized_partition_value_cmp(previous, &min),
                Ordering::Less
            )
        {
            return false;
        }
        previous_max = Some(max);
    }
    true
}

fn list_bound_value_span(
    bound: &PartitionBoundSpec,
) -> Option<(SerializedPartitionValue, SerializedPartitionValue)> {
    let PartitionBoundSpec::List {
        values,
        is_default: false,
    } = bound
    else {
        return None;
    };
    if values.is_empty()
        || values
            .iter()
            .any(|value| matches!(value, SerializedPartitionValue::Null))
    {
        return None;
    }
    let mut values = values.clone();
    values.sort_by(serialized_partition_value_cmp);
    Some((values.first()?.clone(), values.last()?.clone()))
}

fn order_items_for_base_rel_pathkeys(
    root: &PlannerInfo,
    rtindex: usize,
    pathkeys: &[PathKey],
) -> Option<Vec<OrderByEntry>> {
    if pathkeys.is_empty() {
        return None;
    }
    let expanded_pathkeys = pathkeys
        .iter()
        .cloned()
        .map(|key| PathKey {
            expr: expand_join_rte_vars(root, key.expr),
            ressortgroupref: key.ressortgroupref,
            descending: key.descending,
            nulls_first: key.nulls_first,
            collation_oid: key.collation_oid,
        })
        .collect::<Vec<_>>();
    if expanded_pathkeys
        .iter()
        .all(|key| expr_relids(&key.expr).iter().all(|relid| *relid == rtindex))
    {
        Some(pathkeys_to_order_items(&expanded_pathkeys))
    } else {
        None
    }
}

fn query_order_items_for_base_rel(root: &PlannerInfo, rtindex: usize) -> Option<Vec<OrderByEntry>> {
    order_items_for_base_rel_pathkeys(root, rtindex, &root.query_pathkeys)
}

fn cheapest_path_by_total(mut paths: Vec<Path>) -> Option<Path> {
    let (index, _) = paths.iter().enumerate().min_by(|(_, left), (_, right)| {
        left.plan_info()
            .total_cost
            .as_f64()
            .partial_cmp(&right.plan_info().total_cost.as_f64())
            .unwrap_or(Ordering::Equal)
    })?;
    Some(paths.swap_remove(index))
}

fn translate_append_pathkeys_for_child(
    root: &PlannerInfo,
    child_rtindex: usize,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let Some(info) = append_translation(root, child_rtindex) else {
        return pathkeys.to_vec();
    };
    pathkeys
        .iter()
        .cloned()
        .map(|mut key| {
            key.expr = translate_append_rel_expr(key.expr, info);
            key
        })
        .collect()
}

fn relation_display_name(
    catalog: &dyn CatalogLookup,
    rte: &crate::include::nodes::parsenodes::RangeTblEntry,
    relation_oid: u32,
    heap_rel: RelFileLocator,
) -> String {
    let class_row = catalog.class_row_by_oid(relation_oid);
    let base_name = class_row
        .as_ref()
        .map(|row| {
            catalog
                .namespace_row_by_oid(row.relnamespace)
                .map(|namespace| {
                    if matches!(namespace.nspname.as_str(), "public" | "pg_catalog")
                        || namespace.nspname.starts_with("pg_temp_")
                    {
                        row.relname.clone()
                    } else {
                        format!("{}.{}", namespace.nspname, row.relname)
                    }
                })
                .unwrap_or_else(|| row.relname.clone())
        })
        .unwrap_or_else(|| format!("rel {}", heap_rel.rel_number));
    let unqualified_name = class_row
        .as_ref()
        .map(|row| row.relname.as_str())
        .unwrap_or(base_name.as_str());
    match &rte.alias {
        Some(alias)
            if !alias.eq_ignore_ascii_case(&base_name)
                && !alias.eq_ignore_ascii_case(unqualified_name) =>
        {
            format!("{base_name} {alias}")
        }
        _ => base_name,
    }
}

fn access_method_supports_index_scan(am_oid: u32) -> bool {
    if am_oid == HASH_AM_OID {
        return false;
    }
    crate::backend::access::index::amapi::index_am_handler(am_oid)
        .is_some_and(|routine| routine.amgettuple.is_some())
}

fn access_method_supports_index_scan_for_index(index: &BoundIndexRelation) -> bool {
    if index.index_meta.am_oid == HASH_AM_OID {
        return hash_index_gettuple_supported(index);
    }
    access_method_supports_index_scan(index.index_meta.am_oid)
}

fn hash_index_gettuple_supported(index: &BoundIndexRelation) -> bool {
    index.index_meta.indpred.is_some()
        && index.desc.columns.first().is_some_and(|column| {
            matches!(
                column.sql_type.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Oid
            )
        })
}

fn access_method_supports_bitmap_scan(am_oid: u32) -> bool {
    crate::backend::access::index::amapi::index_am_handler(am_oid)
        .is_some_and(|routine| routine.amgetbitmap.is_some())
}

fn brin_partial_bitmap_allowed(index: &BoundIndexRelation, config: PlannerConfig) -> bool {
    index.index_meta.am_oid != crate::include::catalog::BRIN_AM_OID
        || index.index_meta.indpred.is_none()
        || !config.enable_seqscan
}

#[derive(Debug, Clone)]
struct BitmapOrFilter {
    arms: Vec<Expr>,
    common_quals: Vec<Expr>,
}

fn bool_args(expr: &Expr, op: BoolExprType) -> Option<&[Expr]> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == op && bool_expr.args.len() >= 2 => {
            Some(&bool_expr.args)
        }
        _ => None,
    }
}

fn expr_contains_subplan(expr: &Expr) -> bool {
    match expr {
        Expr::SubPlan(_) | Expr::SubLink(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_subplan),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_subplan),
        Expr::Func(func) => func.args.iter().any(expr_contains_subplan),
        Expr::Case(case_expr) => {
            case_expr.arg.as_deref().is_some_and(expr_contains_subplan)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_subplan(&arm.expr) || expr_contains_subplan(&arm.result)
                })
                || expr_contains_subplan(&case_expr.defresult)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_subplan(&saop.left) || expr_contains_subplan(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_subplan),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_subplan(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_subplan(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_contains_subplan)
                        || subscript.upper.as_ref().is_some_and(expr_contains_subplan)
                })
        }
        Expr::FieldSelect { expr, .. }
        | Expr::Cast(expr, _)
        | Expr::Collate { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => expr_contains_subplan(expr),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_subplan(left) || expr_contains_subplan(right)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_subplan(expr) || expr_contains_subplan(pattern)
        }
        _ => false,
    }
}

fn split_bitmap_or_filter(filter: &Expr) -> Option<BitmapOrFilter> {
    if let Some(arms) = bool_args(filter, BoolExprType::Or) {
        return Some(BitmapOrFilter {
            arms: arms.to_vec(),
            common_quals: Vec::new(),
        });
    }

    let conjuncts = flatten_and_conjuncts(filter);
    let mut found_or = None;
    for (index, conjunct) in conjuncts.iter().enumerate() {
        let Some(arms) = bool_args(conjunct, BoolExprType::Or) else {
            continue;
        };
        if found_or.is_some() {
            return None;
        }
        found_or = Some((index, arms.to_vec()));
    }
    let (or_index, arms) = found_or?;
    let common_quals = conjuncts
        .into_iter()
        .enumerate()
        .filter_map(|(index, conjunct)| (index != or_index).then_some(conjunct))
        .collect();
    Some(BitmapOrFilter { arms, common_quals })
}

fn or_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(exprs.into_iter().fold(first, Expr::or))
}

fn bitmap_or_arm_filter(arm: &Expr, common_quals: &[Expr]) -> Expr {
    let mut quals = vec![arm.clone()];
    quals.extend(common_quals.iter().cloned());
    and_exprs(quals).unwrap_or_else(|| arm.clone())
}

fn bitmap_or_arm_recheck(spec: &IndexPathSpec) -> Option<Expr> {
    let mut quals = if spec.recheck_quals.is_empty() {
        spec.used_quals.clone()
    } else {
        spec.recheck_quals.clone()
    };
    quals.extend(spec.filter_quals.clone());
    and_exprs(quals)
}

fn bitmap_path_uses_partial_index(path: &Path) -> bool {
    match path {
        Path::BitmapIndexScan { index_meta, .. } => index_meta.indpred.is_some(),
        Path::BitmapOr { children, .. } | Path::BitmapAnd { children, .. } => {
            children.iter().any(bitmap_path_uses_partial_index)
        }
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_bitmap_or_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &super::super::RelationStats,
    filter: Option<&Expr>,
    config: PlannerConfig,
    index_expr_cache: &PlannerIndexExprCache,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    if !config.enable_bitmapscan {
        return Vec::new();
    }
    let Some(filter) = filter else {
        return Vec::new();
    };
    let Some(or_filter) = split_bitmap_or_filter(filter) else {
        return Vec::new();
    };
    if or_filter.arms.len() < 2 {
        return Vec::new();
    }

    let indexes = catalog
        .index_relations_for_heap_with_cache(relation_oid, index_expr_cache)
        .into_iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indisexclusion
                && !index.index_meta.indkey.is_empty()
                && access_method_supports_bitmap_scan(index.index_meta.am_oid)
        })
        .collect::<Vec<_>>();
    if indexes.is_empty() {
        return Vec::new();
    }

    let best_bitmap_child_for_filter = |candidate_filter: &Expr| -> Option<(Path, Expr)> {
        let mut best_child = None;
        for index in &indexes {
            let Some(spec) = build_index_path_spec(
                Some(candidate_filter),
                None,
                index,
                config.retain_partial_index_filters,
            ) else {
                continue;
            };
            if spec.keys.is_empty() {
                continue;
            }
            let Some(recheck) = bitmap_or_arm_recheck(&spec) else {
                continue;
            };
            let candidate = estimate_bitmap_candidate(
                rtindex,
                heap_rel,
                relation_name.clone(),
                relation_oid,
                toast,
                desc.clone(),
                stats,
                spec,
                None,
                catalog,
            );
            let Path::BitmapHeapScan { bitmapqual, .. } = candidate.plan else {
                continue;
            };
            let child = *bitmapqual;
            let child_cost = child.plan_info().total_cost.as_f64();
            if best_child
                .as_ref()
                .is_none_or(|(best_cost, _, _)| child_cost < *best_cost)
            {
                best_child = Some((child_cost, child, recheck));
            }
        }
        best_child.map(|(_, child, recheck)| (child, recheck))
    };
    let bitmap_index_signature = |path: &Path| -> Option<(String, usize)> {
        match path {
            Path::BitmapIndexScan {
                index_name,
                index_quals,
                ..
            } => Some((index_name.clone(), index_quals.len())),
            _ => None,
        }
    };
    let combined_arms_use_more_of_same_index =
        |combined: &[(Path, Expr)], split: &[(Path, Expr)]| -> bool {
            combined.len() == split.len()
                && combined.iter().zip(split.iter()).all(
                    |((combined_child, _), (split_child, _))| {
                        let Some((combined_index, combined_quals)) =
                            bitmap_index_signature(combined_child)
                        else {
                            return false;
                        };
                        let Some((split_index, split_quals)) = bitmap_index_signature(split_child)
                        else {
                            return false;
                        };
                        combined_index == split_index && combined_quals > split_quals
                    },
                )
        };

    let common_bitmap = and_exprs(or_filter.common_quals.clone())
        .and_then(|common_filter| best_bitmap_child_for_filter(&common_filter));
    let mut combined_arm_choices = Vec::new();
    for arm in &or_filter.arms {
        let arm_filter = bitmap_or_arm_filter(arm, &or_filter.common_quals);
        let Some((child, recheck)) = best_bitmap_child_for_filter(&arm_filter) else {
            return Vec::new();
        };
        combined_arm_choices.push((child, recheck));
    }
    let split_arm_choices = if common_bitmap.is_some() {
        let mut choices = Vec::new();
        for arm in &or_filter.arms {
            let Some((child, recheck)) = best_bitmap_child_for_filter(arm) else {
                return Vec::new();
            };
            choices.push((child, recheck));
        }
        Some(choices)
    } else {
        None
    };
    let use_combined_arms = common_bitmap.is_none()
        || split_arm_choices.as_ref().is_some_and(|split| {
            combined_arms_use_more_of_same_index(&combined_arm_choices, split)
        });
    let arm_choices = if use_combined_arms {
        combined_arm_choices
    } else {
        split_arm_choices.unwrap_or_default()
    };
    let mut children = Vec::new();
    let mut recheck_arms = Vec::new();
    for (child, recheck) in arm_choices {
        children.push(child);
        recheck_arms.push(recheck);
    }
    let Some(or_recheck_expr) = or_exprs(recheck_arms) else {
        return Vec::new();
    };
    let mut rows = children
        .iter()
        .map(|child| child.plan_info().plan_rows.as_f64())
        .sum::<f64>()
        .clamp(1.0, stats.reltuples.max(1.0));
    let startup_cost = children
        .iter()
        .map(|child| child.plan_info().startup_cost.as_f64())
        .sum::<f64>();
    let child_cost = children
        .iter()
        .map(|child| child.plan_info().total_cost.as_f64())
        .sum::<f64>();
    let bitmapqual = Path::BitmapOr {
        plan_info: PlanEstimate::new(startup_cost, child_cost, rows, 0),
        pathtarget: PathTarget::new(Vec::new()),
        children,
    };
    let (bitmapqual, recheck_expr, startup_cost, child_cost, filter_qual) = if let Some((
        common_child,
        common_recheck,
    )) =
        common_bitmap.filter(|_| !use_combined_arms)
    {
        rows = rows
            .min(common_child.plan_info().plan_rows.as_f64())
            .max(1.0);
        let recheck_expr = and_exprs(vec![common_recheck, or_recheck_expr])
            .expect("common and OR recheck quals are present");
        let startup_cost = startup_cost + common_child.plan_info().startup_cost.as_f64();
        let child_cost = child_cost + common_child.plan_info().total_cost.as_f64();
        (
            Path::BitmapAnd {
                plan_info: PlanEstimate::new(startup_cost, child_cost, rows, 0),
                pathtarget: PathTarget::new(Vec::new()),
                children: vec![common_child, bitmapqual],
            },
            recheck_expr,
            startup_cost,
            child_cost,
            Vec::new(),
        )
    } else {
        let filter_qual = if or_filter.common_quals.is_empty() {
            Vec::new()
        } else if bitmap_path_uses_partial_index(&bitmapqual) {
            Vec::new()
        } else if or_filter.common_quals.len() > 1 {
            and_exprs(or_filter.common_quals.clone())
                .into_iter()
                .collect()
        } else {
            or_exprs(or_filter.arms.clone()).into_iter().collect()
        };
        (
            bitmapqual,
            or_recheck_expr,
            startup_cost,
            child_cost,
            filter_qual,
        )
    };
    let total_cost = child_cost + rows * 0.01;
    vec![Path::BitmapHeapScan {
        plan_info: PlanEstimate::new(startup_cost, total_cost, rows, stats.width),
        pathtarget: slot_output_target(rtindex, &desc.columns, |column| column.sql_type),
        source_id: rtindex,
        rel: heap_rel,
        relation_name,
        relation_oid,
        toast,
        desc,
        bitmapqual: Box::new(bitmapqual),
        recheck_qual: vec![recheck_expr],
        filter_qual,
    }]
}

fn collect_required_index_only_attrs_for_root(
    root: &PlannerInfo,
    rtindex: usize,
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
) -> Vec<usize> {
    let mut attrs = BTreeSet::new();
    for target in [
        &root.scanjoin_target,
        &root.final_target,
        &root.sort_input_target,
        &root.group_input_target,
    ] {
        for expr in &target.exprs {
            collect_expr_attrs_for_rel(expr, rtindex, &mut attrs);
        }
    }
    if let Some(filter) = filter {
        collect_expr_attrs_for_rel(filter, rtindex, &mut attrs);
    }
    if let Some(order_items) = order_items {
        for item in order_items {
            collect_expr_attrs_for_rel(&item.expr, rtindex, &mut attrs);
        }
    }
    attrs.into_iter().collect()
}

fn collect_expr_attrs_for_rel(expr: &Expr, rtindex: usize, attrs: &mut BTreeSet<usize>) {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup == 0
                && (var.varno == rtindex || rte_slot_varno(var.varno) == Some(rtindex))
                && !is_system_attr(var.varattno)
                && let Some(index) = attrno_index(var.varattno)
            {
                attrs.insert(index);
            }
        }
        Expr::GroupingKey(grouping_key) => {
            collect_expr_attrs_for_rel(&grouping_key.expr, rtindex, attrs);
        }
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                collect_expr_attrs_for_rel(arg, rtindex, attrs);
            }
        }
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| collect_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| collect_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_expr_attrs_for_rel(arg, rtindex, attrs);
            }
            for arm in &case_expr.args {
                collect_expr_attrs_for_rel(&arm.expr, rtindex, attrs);
                collect_expr_attrs_for_rel(&arm.result, rtindex, attrs);
            }
            collect_expr_attrs_for_rel(&case_expr.defresult, rtindex, attrs);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| collect_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .for_each(|arg| collect_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::ScalarArrayOp(saop) => {
            collect_expr_attrs_for_rel(&saop.left, rtindex, attrs);
            collect_expr_attrs_for_rel(&saop.right, rtindex, attrs);
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .for_each(|child| collect_expr_attrs_for_rel(child, rtindex, attrs)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_expr_attrs_for_rel(inner, rtindex, attrs),
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
            collect_expr_attrs_for_rel(expr, rtindex, attrs);
            collect_expr_attrs_for_rel(pattern, rtindex, attrs);
            if let Some(escape) = escape.as_deref() {
                collect_expr_attrs_for_rel(escape, rtindex, attrs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_attrs_for_rel(left, rtindex, attrs);
            collect_expr_attrs_for_rel(right, rtindex, attrs);
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .for_each(|element| collect_expr_attrs_for_rel(element, rtindex, attrs)),
        Expr::Row { fields, .. } => fields
            .iter()
            .for_each(|(_, expr)| collect_expr_attrs_for_rel(expr, rtindex, attrs)),
        Expr::FieldSelect { expr, .. } => collect_expr_attrs_for_rel(expr, rtindex, attrs),
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_attrs_for_rel(array, rtindex, attrs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_attrs_for_rel(lower, rtindex, attrs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_attrs_for_rel(upper, rtindex, attrs);
                }
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_attrs_for_rel(testexpr, rtindex, attrs);
            }
            for arg in &subplan.args {
                collect_expr_attrs_for_rel(arg, rtindex, attrs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_attrs_for_rel(testexpr, rtindex, attrs);
            }
            collect_query_outer_attrs_for_rel(&sublink.subselect, rtindex, attrs);
        }
        Expr::Aggref(_) | Expr::WindowFunc(_) | Expr::SetReturning(_) => {
            attrs.insert(usize::MAX);
        }
        Expr::Param(_)
        | Expr::Const(_)
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
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_query_outer_attrs_for_rel(query: &Query, rtindex: usize, attrs: &mut BTreeSet<usize>) {
    for target in &query.target_list {
        collect_outer_expr_attrs_for_rel(&target.expr, rtindex, attrs);
    }
    if let Some(where_qual) = &query.where_qual {
        collect_outer_expr_attrs_for_rel(where_qual, rtindex, attrs);
    }
    for expr in &query.group_by {
        collect_outer_expr_attrs_for_rel(expr, rtindex, attrs);
    }
    if let Some(having_qual) = &query.having_qual {
        collect_outer_expr_attrs_for_rel(having_qual, rtindex, attrs);
    }
    for item in &query.sort_clause {
        collect_outer_expr_attrs_for_rel(&item.expr, rtindex, attrs);
    }
    if let Some(jointree) = &query.jointree {
        collect_join_tree_outer_attrs_for_rel(jointree, rtindex, attrs);
    }
    if let Some(recursive) = &query.recursive_union {
        collect_query_outer_attrs_for_rel(&recursive.anchor, rtindex, attrs);
        collect_query_outer_attrs_for_rel(&recursive.recursive, rtindex, attrs);
    }
    if let Some(set_operation) = &query.set_operation {
        for input in &set_operation.inputs {
            collect_query_outer_attrs_for_rel(input, rtindex, attrs);
        }
    }
}

fn collect_join_tree_outer_attrs_for_rel(
    jointree: &JoinTreeNode,
    rtindex: usize,
    attrs: &mut BTreeSet<usize>,
) {
    match jointree {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_join_tree_outer_attrs_for_rel(left, rtindex, attrs);
            collect_join_tree_outer_attrs_for_rel(right, rtindex, attrs);
            collect_outer_expr_attrs_for_rel(quals, rtindex, attrs);
        }
    }
}

fn collect_outer_expr_attrs_for_rel(expr: &Expr, rtindex: usize, attrs: &mut BTreeSet<usize>) {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup > 0
                && (var.varno == rtindex || rte_slot_varno(var.varno) == Some(rtindex))
                && !is_system_attr(var.varattno)
                && let Some(index) = attrno_index(var.varattno)
            {
                attrs.insert(index);
            }
        }
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| collect_outer_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| collect_outer_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_outer_expr_attrs_for_rel(arg, rtindex, attrs);
            }
            for arm in &case_expr.args {
                collect_outer_expr_attrs_for_rel(&arm.expr, rtindex, attrs);
                collect_outer_expr_attrs_for_rel(&arm.result, rtindex, attrs);
            }
            collect_outer_expr_attrs_for_rel(&case_expr.defresult, rtindex, attrs);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| collect_outer_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .for_each(|arg| collect_outer_expr_attrs_for_rel(arg, rtindex, attrs)),
        Expr::ScalarArrayOp(saop) => {
            collect_outer_expr_attrs_for_rel(&saop.left, rtindex, attrs);
            collect_outer_expr_attrs_for_rel(&saop.right, rtindex, attrs);
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .for_each(|child| collect_outer_expr_attrs_for_rel(child, rtindex, attrs)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            collect_outer_expr_attrs_for_rel(inner, rtindex, attrs)
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
            collect_outer_expr_attrs_for_rel(expr, rtindex, attrs);
            collect_outer_expr_attrs_for_rel(pattern, rtindex, attrs);
            if let Some(escape) = escape.as_deref() {
                collect_outer_expr_attrs_for_rel(escape, rtindex, attrs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_outer_expr_attrs_for_rel(left, rtindex, attrs);
            collect_outer_expr_attrs_for_rel(right, rtindex, attrs);
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .for_each(|element| collect_outer_expr_attrs_for_rel(element, rtindex, attrs)),
        Expr::Row { fields, .. } => fields
            .iter()
            .for_each(|(_, expr)| collect_outer_expr_attrs_for_rel(expr, rtindex, attrs)),
        Expr::ArraySubscript { array, subscripts } => {
            collect_outer_expr_attrs_for_rel(array, rtindex, attrs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_outer_expr_attrs_for_rel(lower, rtindex, attrs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_outer_expr_attrs_for_rel(upper, rtindex, attrs);
                }
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_outer_expr_attrs_for_rel(testexpr, rtindex, attrs);
            }
            for arg in &subplan.args {
                collect_outer_expr_attrs_for_rel(arg, rtindex, attrs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_outer_expr_attrs_for_rel(testexpr, rtindex, attrs);
            }
            collect_query_outer_attrs_for_rel(&sublink.subselect, rtindex, attrs);
        }
        _ => {}
    }
}

fn collect_relation_access_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    toast: Option<ToastRelationRef>,
    tablesample: Option<TableSampleClause>,
    desc: RelationDesc,
    filter: Option<Expr>,
    query_order_items: Option<Vec<OrderByEntry>>,
    required_index_only_attrs: &[usize],
    config: PlannerConfig,
    index_expr_cache: &PlannerIndexExprCache,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    if relkind == 'p' {
        return Vec::new();
    }
    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut seq_paths = vec![
        estimate_seqscan_candidate(
            rtindex,
            heap_rel,
            relation_name.clone(),
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample.clone(),
            desc.clone(),
            &stats,
            filter.clone(),
            None,
            catalog,
        )
        .plan,
    ];
    if let Some(order_items) = query_order_items.clone() {
        seq_paths.push(
            estimate_seqscan_candidate(
                rtindex,
                heap_rel,
                relation_name.clone(),
                relation_oid,
                relkind,
                relispopulated,
                toast,
                tablesample.clone(),
                desc.clone(),
                &stats,
                filter.clone(),
                Some(order_items),
                catalog,
            )
            .plan,
        );
    }
    let mut paths = if config.enable_seqscan || relkind != 'r' {
        seq_paths.clone()
    } else {
        let mut disabled_seq_paths = seq_paths.clone();
        for path in &mut disabled_seq_paths {
            mark_seqscan_disabled(path);
        }
        disabled_seq_paths
    };
    if relkind != 'r' || relation_uses_virtual_scan(relation_oid) {
        return paths;
    }
    for index in catalog
        .index_relations_for_heap_with_cache(relation_oid, index_expr_cache)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indisexclusion
                && !index.index_meta.indkey.is_empty()
        })
    {
        let target_index_only = index_supports_index_only_attrs(index, required_index_only_attrs);
        let full_index_only_scan = target_index_only
            || (!config.enable_seqscan
                && filter.is_none()
                && index_supports_index_only_attrs(index, &visible_user_attr_indexes(&desc)));
        let order_removing_index_scan_available = config.enable_indexscan
            && access_method_supports_index_scan_for_index(index)
            && query_order_items.as_ref().is_some_and(|order_items| {
                build_index_path_spec(
                    filter.as_ref(),
                    Some(order_items),
                    index,
                    config.retain_partial_index_filters,
                )
                .is_some_and(|spec| spec.removes_order)
            });
        let index_spec = build_index_path_spec(
            filter.as_ref(),
            None,
            index,
            config.retain_partial_index_filters,
        );
        let has_index_spec = index_spec.is_some();
        if let Some(spec) = index_spec {
            let prefer_plain_index_scan = spec_prefers_plain_index_scan(&spec);
            if config.enable_indexscan && access_method_supports_index_scan_for_index(index) {
                paths.push(
                    estimate_index_candidate(
                        rtindex,
                        heap_rel,
                        relation_name.clone(),
                        relation_oid,
                        toast,
                        desc.clone(),
                        &stats,
                        spec.clone(),
                        None,
                        target_index_only,
                        config,
                        catalog,
                    )
                    .plan,
                );
            }
            if config.enable_bitmapscan
                && !prefer_plain_index_scan
                && access_method_supports_bitmap_scan(index.index_meta.am_oid)
                && brin_partial_bitmap_allowed(index, config)
                && !order_removing_index_scan_available
                && !target_index_only_unique_btree(index, target_index_only)
            {
                paths.push(
                    estimate_bitmap_candidate(
                        rtindex,
                        heap_rel,
                        relation_name.clone(),
                        relation_oid,
                        toast,
                        desc.clone(),
                        &stats,
                        spec,
                        None,
                        catalog,
                    )
                    .plan,
                );
            }
        }
        if config.enable_indexscan
            && query_order_items.is_none()
            && filter.is_none()
            && full_index_only_scan
            && access_method_supports_index_scan_for_index(index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    full_index_scan_spec(index, filter.clone()),
                    None,
                    false,
                    config,
                    catalog,
                )
                .plan,
            );
        }
        if config.enable_indexscan
            && query_order_items.is_none()
            && filter.as_ref().is_some_and(expr_contains_subplan)
            && target_index_only
            && access_method_supports_index_scan_for_index(index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    full_index_scan_spec(index, filter.clone()),
                    None,
                    true,
                    config,
                    catalog,
                )
                .plan,
            );
        }
        if config.enable_indexscan
            && !config.enable_seqscan
            && query_order_items.is_none()
            && filter.is_some()
            && !has_index_spec
            && access_method_supports_index_scan_for_index(index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    full_index_scan_spec(index, filter.clone()),
                    None,
                    false,
                    config,
                    catalog,
                )
                .plan,
            );
        }
        if config.enable_indexscan
            && let Some(order_items) = query_order_items.as_ref()
            && let Some(spec) = build_index_path_spec(
                filter.as_ref(),
                Some(order_items),
                index,
                config.retain_partial_index_filters,
            )
            && access_method_supports_index_scan_for_index(index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    spec,
                    Some(order_items.clone()),
                    target_index_only,
                    config,
                    catalog,
                )
                .plan,
            );
        }
    }
    paths.extend(collect_bitmap_or_paths(
        rtindex,
        heap_rel,
        relation_name,
        relation_oid,
        toast,
        desc.clone(),
        &stats,
        filter.as_ref(),
        config,
        index_expr_cache,
        catalog,
    ));
    if paths.is_empty() {
        paths = seq_paths;
    }
    paths
}

fn target_index_only_unique_btree(
    index: &crate::backend::parser::BoundIndexRelation,
    target_index_only: bool,
) -> bool {
    target_index_only
        && index.index_meta.indisunique
        && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
}

fn mark_seqscan_disabled(path: &mut Path) {
    match path {
        Path::SeqScan { disabled, .. } => *disabled = true,
        Path::Filter { input, .. } | Path::OrderBy { input, .. } => mark_seqscan_disabled(input),
        _ => {}
    }
}

fn visible_user_attr_indexes(desc: &RelationDesc) -> Vec<usize> {
    desc.columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (!column.dropped).then_some(index))
        .collect()
}

fn collect_relation_ordered_index_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: &[OrderByEntry],
    _required_index_only_attrs: &[usize],
    config: PlannerConfig,
    index_expr_cache: &PlannerIndexExprCache,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    if !config.enable_indexscan || relation_uses_virtual_scan(relation_oid) {
        return Vec::new();
    }
    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut paths = Vec::new();
    for index in catalog
        .index_relations_for_heap_with_cache(relation_oid, index_expr_cache)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indisexclusion
                && !index.index_meta.indkey.is_empty()
        })
    {
        if !access_method_supports_index_scan_for_index(index) {
            continue;
        }
        if let Some(spec) = build_index_path_spec(
            filter.as_ref(),
            Some(order_items),
            index,
            config.retain_partial_index_filters,
        ) {
            if !spec.removes_order {
                continue;
            }
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    spec,
                    Some(order_items.to_vec()),
                    false,
                    config,
                    catalog,
                )
                .plan,
            );
        }
    }
    paths
}

fn relation_uses_virtual_scan(relation_oid: u32) -> bool {
    relation_oid == PG_LARGEOBJECT_METADATA_RELATION_OID
}

pub(super) fn relation_ordered_index_paths(
    root: &PlannerInfo,
    rtindex: usize,
    pathkeys: &[PathKey],
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    let Some(order_items) = order_items_for_base_rel_pathkeys(root, rtindex, pathkeys) else {
        return Vec::new();
    };
    let Some(rel) = root.simple_rel_array.get(rtindex).and_then(Option::as_ref) else {
        return Vec::new();
    };
    let Some(rte) = root.parse.rtable.get(rtindex - 1) else {
        return Vec::new();
    };
    match &rte.kind {
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind,
            relispopulated: _,
            toast,
            ..
        } if *relkind == 'r' => {
            let filter = base_filter_expr(rel);
            let required_index_only_attrs = collect_required_index_only_attrs_for_root(
                root,
                rtindex,
                filter.as_ref(),
                Some(&order_items),
            );
            collect_relation_ordered_index_paths(
                rtindex,
                *heap_rel,
                relation_display_name(catalog, rte, *relation_oid, *heap_rel),
                *relation_oid,
                *toast,
                rte.desc.clone(),
                filter,
                &order_items,
                &required_index_only_attrs,
                root.config,
                &root.index_expr_cache,
                catalog,
            )
        }
        _ => Vec::new(),
    }
}

pub(super) fn relation_index_only_full_scan_paths(
    root: &PlannerInfo,
    rtindex: usize,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    if !root.config.enable_indexonlyscan
        || relation_uses_virtual_scan(relation_oid_for_rtindex(root, rtindex).unwrap_or(0))
    {
        return Vec::new();
    }
    let Some(rte) = root.parse.rtable.get(rtindex - 1) else {
        return Vec::new();
    };
    let RangeTblEntryKind::Relation {
        rel: heap_rel,
        relation_oid,
        relkind,
        toast,
        ..
    } = &rte.kind
    else {
        return Vec::new();
    };
    if *relkind != 'r' {
        return Vec::new();
    }
    let stats = relation_stats(catalog, *relation_oid, &rte.desc);
    let relation_name = relation_display_name(catalog, rte, *relation_oid, *heap_rel);
    let mut paths = Vec::new();
    for index in catalog
        .index_relations_for_heap_with_cache(*relation_oid, &root.index_expr_cache)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indkey.is_empty()
                && index.index_meta.am_oid == BTREE_AM_OID
        })
    {
        let rows = stats.reltuples.max(1.0);
        let first_key_distinct = index
            .index_meta
            .indkey
            .first()
            .copied()
            .and_then(|attnum| stats.stats_by_attnum.get(&attnum))
            .map(|row| {
                if row.stadistinct > 0.0 {
                    row.stadistinct
                } else if row.stadistinct < 0.0 {
                    -row.stadistinct * rows
                } else {
                    rows
                }
            })
            .unwrap_or(rows);
        let total_cost = rows * 0.0001
            + first_key_distinct * 0.00001
            + if index.index_meta.indisunique {
                1.0
            } else {
                0.0
            };
        paths.push(Path::IndexOnlyScan {
            plan_info: PlanEstimate::new(0.0025, total_cost, rows, stats.width),
            pathtarget: slot_output_target(rtindex, &rte.desc.columns, |column| column.sql_type),
            source_id: rtindex,
            rel: *heap_rel,
            relation_name: relation_name.clone(),
            relation_oid: *relation_oid,
            index_rel: index.rel,
            index_name: index.name.clone(),
            am_oid: index.index_meta.am_oid,
            toast: *toast,
            desc: rte.desc.clone(),
            index_desc: index.desc.clone(),
            index_meta: index.index_meta.clone(),
            keys: Vec::new(),
            order_by_keys: Vec::new(),
            direction: crate::include::access::relscan::ScanDirection::Forward,
            pathkeys: Vec::new(),
        });
    }
    paths
}

fn cheapest_relation_access_path(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    toast: Option<ToastRelationRef>,
    tablesample: Option<TableSampleClause>,
    desc: RelationDesc,
    filter: Option<Expr>,
    config: PlannerConfig,
    index_expr_cache: &PlannerIndexExprCache,
    catalog: &dyn CatalogLookup,
) -> Path {
    collect_relation_access_paths(
        rtindex,
        heap_rel,
        relation_name,
        relation_oid,
        relkind,
        relispopulated,
        toast,
        tablesample,
        desc,
        filter,
        None,
        &[],
        config,
        index_expr_cache,
        catalog,
    )
    .into_iter()
    .min_by(|left, right| {
        left.plan_info()
            .total_cost
            .as_f64()
            .partial_cmp(&right.plan_info().total_cost.as_f64())
            .unwrap_or(Ordering::Equal)
    })
    .unwrap_or(Path::Result {
        plan_info: PlanEstimate::default(),
        pathtarget: PathTarget::new(Vec::new()),
    })
}

fn plan_query_path(
    query: crate::include::nodes::parsenodes::Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> (PlannerInfo, Path) {
    let query = prepare_query_path_input(query, catalog);
    let aggregate_layout = super::super::groupby_rewrite::build_aggregate_layout(&query, catalog);
    let mut root = PlannerInfo::new_with_config(query, aggregate_layout, config);
    let scanjoin_rel = query_planner(&mut root, catalog);
    let final_rel = grouping_planner(&mut root, scanjoin_rel, catalog);
    let required_pathkeys = required_query_pathkeys_for_rel(&root, &final_rel);
    let path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
        .cloned()
        .unwrap_or(Path::Result {
            plan_info: PlanEstimate::default(),
            pathtarget: PathTarget::new(Vec::new()),
        });
    (root, path)
}

fn prepare_query_path_input(
    query: crate::include::nodes::parsenodes::Query,
    catalog: &dyn CatalogLookup,
) -> crate::include::nodes::parsenodes::Query {
    let query = super::super::root::prepare_query_for_planning(query, catalog);
    pull_up_sublinks(query)
}

fn plan_set_operation_child_path(
    mut query: crate::include::nodes::parsenodes::Query,
    sorted: bool,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> (PlannerInfo, Path) {
    if sorted
        && set_operation_child_can_accept_required_order(&query)
        && set_operation_child_ordering_is_worthwhile(&query)
    {
        query.sort_clause = set_operation_child_sort_clause(&query);
    }
    plan_query_path(query, catalog, config)
}

fn set_operation_child_can_accept_required_order(query: &Query) -> bool {
    query.sort_clause.is_empty()
        && query.limit_count.is_none()
        && query.limit_offset.is_none()
        && query.locking_clause.is_none()
        && query.row_marks.is_empty()
}

fn set_operation_child_ordering_is_worthwhile(query: &Query) -> bool {
    fn walk(query: &Query, node: &JoinTreeNode) -> bool {
        match node {
            JoinTreeNode::RangeTblRef(rtindex) => query
                .rtable
                .get(rtindex.saturating_sub(1))
                .is_some_and(|rte| match &rte.kind {
                    RangeTblEntryKind::Relation { .. } | RangeTblEntryKind::WorkTable { .. } => {
                        true
                    }
                    RangeTblEntryKind::Subquery { query }
                    | RangeTblEntryKind::Cte { query, .. } => {
                        set_operation_child_ordering_is_worthwhile(query)
                    }
                    RangeTblEntryKind::Join { .. }
                    | RangeTblEntryKind::Values { .. }
                    | RangeTblEntryKind::Function { .. }
                    | RangeTblEntryKind::Result => false,
                }),
            JoinTreeNode::JoinExpr { left, right, .. } => walk(query, left) || walk(query, right),
        }
    }

    query
        .jointree
        .as_ref()
        .is_some_and(|jointree| walk(query, jointree))
}

fn set_operation_child_sort_clause(query: &Query) -> Vec<SortGroupClause> {
    query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .enumerate()
        .map(|(index, target)| SortGroupClause {
            expr: target.expr.clone(),
            tle_sort_group_ref: if target.ressortgroupref != 0 {
                target.ressortgroupref
            } else if target.resno != 0 {
                target.resno
            } else {
                index + 1
            },
            descending: false,
            nulls_first: None,
            collation_oid: None,
        })
        .collect()
}

fn build_recursive_union_path(
    recursive_union: crate::include::nodes::parsenodes::RecursiveUnionQuery,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let anchor_query = recursive_union.anchor.clone();
    let recursive_query = recursive_union.recursive.clone();
    let (anchor_root, anchor_path) = plan_query_path(recursive_union.anchor, catalog, config);
    let (recursive_root, recursive_path) =
        plan_query_path(recursive_union.recursive, catalog, config);
    let slot_id = next_synthetic_slot_id();
    let output_columns = recursive_union
        .output_desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    optimize_path_with_config(
        Path::RecursiveUnion {
            plan_info: PlanEstimate::default(),
            pathtarget: slot_output_target(slot_id, &output_columns, |column| column.sql_type),
            slot_id,
            worktable_id: recursive_union.worktable_id,
            distinct: recursive_union.distinct,
            anchor_root: PlannerSubroot::new(anchor_root),
            recursive_root: PlannerSubroot::new(recursive_root),
            anchor_query: Box::new(anchor_query),
            recursive_query: Box::new(recursive_query),
            recursive_references_worktable: recursive_union.recursive_references_worktable,
            output_columns,
            anchor: Box::new(anchor_path),
            recursive: Box::new(recursive_path),
        },
        catalog,
        config,
    )
}

fn build_set_operation_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let set_operation = root
        .parse
        .set_operation
        .clone()
        .expect("set-operation rel requested without set_operation query");
    let source_id = 1usize;
    let desc = set_operation.output_desc;
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    let force_sorted_union = matches!(set_operation.op, SetOperator::Union { all: false })
        && !root.parse.sort_clause.is_empty()
        && set_op_columns_sortable(&output_columns);
    let sorted_children = force_sorted_union
        || set_operation_needs_ordered_children(set_operation.op, &output_columns, root.config);
    let (child_roots, children) = set_operation
        .inputs
        .into_iter()
        .map(|query| {
            let (child_root, path) =
                plan_set_operation_child_path(query, sorted_children, catalog, root.config);
            (
                Some(PlannerSubroot::new(child_root)),
                project_to_slot_layout(
                    source_id,
                    &desc,
                    path.clone(),
                    path.output_target(),
                    catalog,
                ),
            )
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let set_op = build_set_operation_path(
        set_operation.op,
        source_id,
        desc.clone(),
        output_columns,
        child_roots,
        children,
        catalog,
        root.config,
        force_sorted_union,
    );
    let mut rel = RelOptInfo::new(
        Vec::new(),
        RelOptKind::UpperRel,
        set_op.semantic_output_target(),
    );
    rel.add_path(set_op);
    bestpath::set_cheapest(&mut rel);
    rel
}

fn set_operation_needs_ordered_children(
    op: SetOperator,
    output_columns: &[QueryColumn],
    config: PlannerConfig,
) -> bool {
    match op {
        SetOperator::Union { all: true } => false,
        SetOperator::Union { all: false } => {
            if output_columns.is_empty() {
                return false;
            }
            let can_hash = set_op_columns_hashable(output_columns);
            let can_sort = set_op_columns_sortable(output_columns);
            can_sort && !(can_hash && (config.enable_hashagg || !can_sort))
        }
        SetOperator::Intersect { .. } | SetOperator::Except { .. } => {
            set_operation_strategy(op, output_columns, config) == SetOpStrategy::Sorted
                && !output_columns.is_empty()
        }
    }
}

fn set_operation_strategy(
    _op: SetOperator,
    output_columns: &[QueryColumn],
    config: PlannerConfig,
) -> SetOpStrategy {
    if output_columns.is_empty() {
        return SetOpStrategy::Sorted;
    }
    let can_hash = set_op_columns_hashable(output_columns);
    let can_sort = set_op_columns_sortable(output_columns);
    if can_hash && (config.enable_hashagg || !can_sort) {
        SetOpStrategy::Hashed
    } else {
        SetOpStrategy::Sorted
    }
}

fn build_set_operation_path(
    op: SetOperator,
    source_id: usize,
    desc: RelationDesc,
    output_columns: Vec<QueryColumn>,
    child_roots: Vec<Option<PlannerSubroot>>,
    children: Vec<Path>,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
    force_sorted_union: bool,
) -> Path {
    match op {
        SetOperator::Union { all: true } => optimize_path_with_config(
            set_op_append_path(source_id, desc, &output_columns, child_roots, children),
            catalog,
            config,
        ),
        SetOperator::Union { all: false } => {
            let can_hash = set_op_columns_hashable(&output_columns);
            let can_sort = set_op_columns_sortable(&output_columns);
            if !force_sorted_union && can_hash && (config.enable_hashagg || !can_sort) {
                let append =
                    set_op_append_path(source_id, desc, &output_columns, child_roots, children);
                optimize_path_with_config(
                    Path::Aggregate {
                        plan_info: PlanEstimate::default(),
                        pathtarget: slot_output_target(source_id, &output_columns, |column| {
                            column.sql_type
                        }),
                        slot_id: source_id,
                        strategy: AggregateStrategy::Hashed,
                        phase: crate::include::nodes::plannodes::AggregatePhase::Complete,
                        semantic_accumulators: None,
                        disabled: !config.enable_hashagg && !can_sort,
                        pathkeys: Vec::new(),
                        input: Box::new(append),
                        group_by: set_op_output_exprs(source_id, &output_columns),
                        group_by_refs: (1..=output_columns.len()).collect(),
                        grouping_sets: Vec::new(),
                        passthrough_exprs: Vec::new(),
                        accumulators: Vec::new(),
                        having: None,
                        output_columns,
                    },
                    catalog,
                    config,
                )
            } else {
                let input = if output_columns.is_empty() {
                    set_op_append_path(source_id, desc, &output_columns, child_roots, children)
                } else if set_op_children_have_ordered_path(source_id, &output_columns, &children) {
                    set_op_merge_append_path(source_id, desc, &output_columns, children)
                } else {
                    let children = children
                        .into_iter()
                        .map(strip_set_op_child_sort)
                        .collect::<Vec<_>>();
                    let append =
                        set_op_append_path(source_id, desc, &output_columns, child_roots, children);
                    set_op_sort_path(source_id, &output_columns, append)
                };
                optimize_path_with_config(
                    Path::Unique {
                        plan_info: PlanEstimate::default(),
                        pathtarget: slot_output_target(source_id, &output_columns, |column| {
                            column.sql_type
                        }),
                        key_indices: (0..output_columns.len()).collect(),
                        input: Box::new(input),
                    },
                    catalog,
                    config,
                )
            }
        }
        SetOperator::Intersect { .. } | SetOperator::Except { .. } => {
            let strategy = set_operation_strategy(op, &output_columns, config);
            let children = if matches!(strategy, SetOpStrategy::Sorted) {
                children
                    .into_iter()
                    .map(|child| ensure_set_op_sorted_path(source_id, &output_columns, child))
                    .collect()
            } else {
                children
            };
            optimize_path_with_config(
                Path::SetOp {
                    plan_info: PlanEstimate::default(),
                    pathtarget: slot_output_target(source_id, &output_columns, |column| {
                        column.sql_type
                    }),
                    slot_id: source_id,
                    op,
                    strategy,
                    output_columns,
                    child_roots,
                    children,
                },
                catalog,
                config,
            )
        }
    }
}

fn set_op_append_path(
    source_id: usize,
    desc: RelationDesc,
    output_columns: &[QueryColumn],
    child_roots: Vec<Option<PlannerSubroot>>,
    children: Vec<Path>,
) -> Path {
    Path::Append {
        plan_info: PlanEstimate::default(),
        pathtarget: slot_output_target(source_id, output_columns, |column| column.sql_type),
        pathkeys: Vec::new(),
        relids: Vec::new(),
        source_id,
        desc,
        child_roots,
        partition_prune: None,
        children,
    }
}

fn set_op_merge_append_path(
    source_id: usize,
    desc: RelationDesc,
    output_columns: &[QueryColumn],
    children: Vec<Path>,
) -> Path {
    Path::MergeAppend {
        plan_info: PlanEstimate::default(),
        pathtarget: slot_output_target(source_id, output_columns, |column| column.sql_type),
        source_id,
        desc,
        items: set_op_order_items(source_id, output_columns),
        partition_prune: None,
        children: children
            .into_iter()
            .map(|child| ensure_set_op_sorted_path(source_id, output_columns, child))
            .collect(),
    }
}

fn set_op_children_have_ordered_path(
    source_id: usize,
    output_columns: &[QueryColumn],
    children: &[Path],
) -> bool {
    let required = set_op_order_pathkeys(source_id, output_columns);
    children.iter().any(|child| {
        bestpath::pathkeys_satisfy(&child.pathkeys(), &required)
            && path_contains_native_ordered_index_scan(child)
    })
}

fn path_contains_native_ordered_index_scan(path: &Path) -> bool {
    match path {
        Path::IndexOnlyScan { pathkeys, .. } | Path::IndexScan { pathkeys, .. } => {
            !pathkeys.is_empty()
        }
        Path::Projection { input, .. }
        | Path::Filter { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. } => path_contains_native_ordered_index_scan(input),
        _ => false,
    }
}

fn strip_set_op_child_sort(path: Path) -> Path {
    match path {
        Path::OrderBy { input, .. } => *input,
        Path::Projection {
            plan_info,
            pathtarget,
            slot_id,
            input,
            targets,
        } => Path::Projection {
            plan_info,
            pathtarget,
            slot_id,
            input: Box::new(strip_set_op_child_sort(*input)),
            targets,
        },
        other => other,
    }
}

fn ensure_set_op_sorted_path(
    source_id: usize,
    output_columns: &[QueryColumn],
    input: Path,
) -> Path {
    if output_columns.is_empty()
        || bestpath::pathkeys_satisfy(
            &input.pathkeys(),
            &set_op_order_pathkeys(source_id, output_columns),
        )
    {
        input
    } else {
        set_op_sort_path(source_id, output_columns, input)
    }
}

fn set_op_sort_path(source_id: usize, output_columns: &[QueryColumn], input: Path) -> Path {
    if output_columns.is_empty() {
        return input;
    }
    Path::OrderBy {
        plan_info: PlanEstimate::default(),
        pathtarget: slot_output_target(source_id, output_columns, |column| column.sql_type),
        input: Box::new(input),
        items: set_op_order_items(source_id, output_columns),
        display_items: output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
    }
}

fn set_op_order_pathkeys(source_id: usize, output_columns: &[QueryColumn]) -> Vec<PathKey> {
    set_op_order_items(source_id, output_columns)
        .into_iter()
        .map(|item| PathKey {
            expr: item.expr,
            ressortgroupref: item.ressortgroupref,
            descending: item.descending,
            nulls_first: item.nulls_first,
            collation_oid: item.collation_oid,
        })
        .collect()
}

fn set_op_order_items(source_id: usize, output_columns: &[QueryColumn]) -> Vec<OrderByEntry> {
    set_op_output_exprs(source_id, output_columns)
        .into_iter()
        .enumerate()
        .map(|(index, expr)| OrderByEntry {
            expr,
            ressortgroupref: index + 1,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        })
        .collect()
}

fn set_op_output_exprs(source_id: usize, output_columns: &[QueryColumn]) -> Vec<Expr> {
    output_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: source_id,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

fn set_op_columns_hashable(output_columns: &[QueryColumn]) -> bool {
    output_columns
        .iter()
        .all(|column| set_op_type_hashable(column.sql_type))
}

fn set_op_columns_sortable(output_columns: &[QueryColumn]) -> bool {
    output_columns
        .iter()
        .all(|column| set_op_type_sortable(column.sql_type))
}

fn set_op_type_hashable(sql_type: SqlType) -> bool {
    if sql_type.is_array {
        return set_op_type_hashable(sql_type.element_type());
    }
    !matches!(
        sql_type.kind,
        SqlTypeKind::VarBit
            | SqlTypeKind::Bit
            | SqlTypeKind::Record
            | SqlTypeKind::Composite
            | SqlTypeKind::Json
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Xml
    )
}

fn set_op_type_sortable(sql_type: SqlType) -> bool {
    if sql_type.is_array {
        return set_op_type_sortable(sql_type.element_type());
    }
    !matches!(
        sql_type.kind,
        SqlTypeKind::Xid
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Xml
            | SqlTypeKind::TsVector
            | SqlTypeKind::TsQuery
    )
}

fn build_cte_scan_path(
    rtindex: usize,
    cte_id: usize,
    cte_name: String,
    query: crate::include::nodes::parsenodes::Query,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let query = prepare_query_path_input(query, catalog);
    let (subroot, cte_path) = if let Some(recursive_union) = query.recursive_union.clone() {
        let planned_query = query.clone();
        let aggregate_layout =
            super::super::groupby_rewrite::build_aggregate_layout(&planned_query, catalog);
        (
            PlannerInfo::new_with_config(planned_query, aggregate_layout, config),
            build_recursive_union_path(*recursive_union, catalog, config),
        )
    } else {
        plan_query_path(query.clone(), catalog, config)
    };
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    Path::CteScan {
        plan_info: cte_path.plan_info(),
        pathtarget: slot_output_target(rtindex, &output_columns, |column| column.sql_type),
        slot_id: rte_slot_id(rtindex),
        cte_id,
        cte_name,
        subroot: PlannerSubroot::new(subroot),
        query: Box::new(query),
        cte_plan: Box::new(cte_path),
        output_columns,
    }
}

fn build_subquery_scan_path(
    rtindex: usize,
    query: crate::include::nodes::parsenodes::Query,
    used_attrs: &BTreeSet<usize>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let query = prepare_query_path_input(
        prune_unused_subquery_outputs(query, used_attrs, catalog),
        catalog,
    );
    if simple_subquery_where_qual_is_contradictory(&query) {
        return optimize_path_with_config(
            const_false_relation_path(rtindex, desc),
            catalog,
            config,
        );
    }
    let (subroot, input) = if let Some(recursive_union) = query.recursive_union.clone() {
        let planned_query = query.clone();
        let aggregate_layout =
            super::super::groupby_rewrite::build_aggregate_layout(&planned_query, catalog);
        (
            PlannerInfo::new_with_config(planned_query, aggregate_layout, config),
            build_recursive_union_path(*recursive_union, catalog, config),
        )
    } else {
        plan_query_path(query.clone(), catalog, config)
    };
    let input_vars = input.semantic_output_vars();
    let pathkeys = input
        .pathkeys()
        .into_iter()
        .filter_map(|key| {
            input_vars
                .iter()
                .position(|expr| *expr == key.expr)
                .and_then(|index| {
                    desc.columns
                        .get(index)
                        .map(|column| (index, column.sql_type))
                })
                .map(|(index, sql_type)| PathKey {
                    expr: Expr::Var(Var {
                        varno: rtindex,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: sql_type,
                    }),
                    ressortgroupref: key.ressortgroupref,
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                    collation_oid: key.collation_oid,
                })
        })
        .collect();
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    Path::SubqueryScan {
        plan_info: input.plan_info(),
        pathtarget: slot_output_target(rtindex, &output_columns, |column| column.sql_type),
        rtindex,
        subroot: PlannerSubroot::new(subroot),
        query: Box::new(query),
        input: Box::new(input),
        output_columns,
        pathkeys,
    }
}

fn simple_subquery_where_qual_is_contradictory(query: &Query) -> bool {
    if !subquery_filter_pushdown_is_safe(query) {
        return false;
    }
    let Some(where_qual) = query.where_qual.as_ref() else {
        return false;
    };
    if matches!(where_qual, Expr::Const(Value::Bool(false))) {
        return true;
    }

    expr_list_has_contradictory_equalities(flatten_and_conjuncts(where_qual))
}

fn subquery_filter_pushdown_is_safe(query: &Query) -> bool {
    !query.distinct
        && query.group_by.is_empty()
        && query.accumulators.is_empty()
        && query.window_clauses.is_empty()
        && query.having_qual.is_none()
        && query.sort_clause.is_empty()
        && query.limit_count.is_none()
        && query.limit_offset.is_none()
        && query.locking_clause.is_none()
        && query.row_marks.is_empty()
        && !query.has_target_srfs
        && query.recursive_union.is_none()
        && query.set_operation.is_none()
}

fn set_operation_filter_pushdown_is_safe(query: &Query) -> bool {
    !query.distinct
        && query.group_by.is_empty()
        && query.accumulators.is_empty()
        && query.window_clauses.is_empty()
        && query.having_qual.is_none()
        && query.sort_clause.is_empty()
        && query.limit_count.is_none()
        && query.limit_offset.is_none()
        && query.locking_clause.is_none()
        && query.row_marks.is_empty()
        && !query.has_target_srfs
        && query.recursive_union.is_none()
        && query.set_operation.is_some()
}

fn push_subquery_filter(
    rtindex: usize,
    mut query: Query,
    filter: Option<Expr>,
) -> (Query, Option<Expr>) {
    let Some(filter) = filter else {
        return (query, None);
    };
    if query.set_operation.is_some() {
        return push_set_operation_filter(rtindex, query, filter);
    }
    if !subquery_filter_pushdown_is_safe(&query) {
        return (query, Some(filter));
    }
    let visible_targets = query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .collect::<Vec<_>>();
    let Some(pushed) =
        rewrite_filter_for_subquery(filter.clone(), rtindex, &visible_targets, &query)
    else {
        return (query, Some(filter));
    };
    query.where_qual = Some(match query.where_qual.take() {
        Some(existing) => Expr::and(existing, pushed),
        None => pushed,
    });
    (query, None)
}

fn visible_query_targets(query: &Query) -> Vec<&crate::include::nodes::primnodes::TargetEntry> {
    query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .collect()
}

fn push_set_operation_filter(
    rtindex: usize,
    mut query: Query,
    filter: Expr,
) -> (Query, Option<Expr>) {
    if !set_operation_filter_pushdown_is_safe(&query) {
        return (query, Some(filter));
    }
    let Some(mut set_operation) = query.set_operation.take() else {
        return (query, Some(filter));
    };
    let original_set_operation = set_operation.clone();
    let visible_targets = visible_query_targets(&query);
    let Some(setop_filter) =
        rewrite_filter_for_subquery(filter.clone(), rtindex, &visible_targets, &query)
    else {
        query.set_operation = Some(set_operation);
        return (query, Some(filter));
    };

    let push_distinct = matches!(set_operation.op, SetOperator::Union { all: false })
        && set_operation_filter_is_safe_for_distinct(&setop_filter, &set_operation.inputs);
    let push_all = matches!(set_operation.op, SetOperator::Union { all: true });
    if !push_all && !push_distinct {
        query.set_operation = Some(set_operation);
        return (query, Some(filter));
    }

    let mut pushed_inputs = Vec::with_capacity(set_operation.inputs.len());
    for child in set_operation.inputs {
        let child_targets = visible_query_targets(&child);
        let Some(child_filter) =
            rewrite_filter_for_subquery_relaxed(setop_filter.clone(), 1, &child_targets)
        else {
            query.set_operation = Some(original_set_operation);
            return (query, Some(filter));
        };
        let Some(child) = push_filter_into_child_query(child, child_filter, push_all) else {
            continue;
        };
        pushed_inputs.push(child);
    }

    set_operation.inputs = pushed_inputs;
    query.set_operation = Some(set_operation);
    (query, None)
}

fn set_operation_filter_is_safe_for_distinct(filter: &Expr, inputs: &[Query]) -> bool {
    !expr_contains_set_returning(filter)
        && !expr_contains_planner_volatile(filter)
        && inputs.iter().all(|input| {
            visible_query_targets(input).iter().all(|target| {
                !expr_contains_set_returning(&target.expr)
                    && !expr_contains_planner_volatile(&target.expr)
            })
        })
}

fn push_filter_into_child_query(
    mut query: Query,
    filter: Expr,
    prune_false: bool,
) -> Option<Query> {
    query.where_qual = Some(match query.where_qual.take() {
        Some(existing) => Expr::and(existing, filter),
        None => filter,
    });
    let mut folded = super::super::fold_query_constants(query.clone()).unwrap_or(query);
    match folded.where_qual.as_ref() {
        Some(Expr::Const(Value::Bool(false))) if prune_false => None,
        Some(Expr::Const(Value::Bool(true))) => {
            folded.where_qual = None;
            Some(folded)
        }
        _ => Some(folded),
    }
}

fn rewrite_filter_for_subquery_relaxed(
    expr: Expr,
    rtindex: usize,
    targets: &[&crate::include::nodes::primnodes::TargetEntry],
) -> Option<Expr> {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup != 0 || var.varno != rtindex {
                return None;
            }
            let index = crate::include::nodes::primnodes::attrno_index(var.varattno)?;
            Some(targets.get(index)?.expr.clone())
        }
        Expr::Param(_) | Expr::Const(_) => Some(expr),
        Expr::Op(mut op) => {
            op.args = op
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery_relaxed(arg, rtindex, targets))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Op(op))
        }
        Expr::Bool(mut bool_expr) => {
            bool_expr.args = bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery_relaxed(arg, rtindex, targets))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Bool(bool_expr))
        }
        Expr::Func(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery_relaxed(arg, rtindex, targets))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Func(func))
        }
        Expr::Cast(inner, ty) => Some(Expr::Cast(
            Box::new(rewrite_filter_for_subquery_relaxed(
                *inner, rtindex, targets,
            )?),
            ty,
        )),
        Expr::Collate {
            expr,
            collation_oid,
        } => Some(Expr::Collate {
            expr: Box::new(rewrite_filter_for_subquery_relaxed(
                *expr, rtindex, targets,
            )?),
            collation_oid,
        }),
        Expr::IsNull(inner) => Some(Expr::IsNull(Box::new(rewrite_filter_for_subquery_relaxed(
            *inner, rtindex, targets,
        )?))),
        Expr::IsNotNull(inner) => Some(Expr::IsNotNull(Box::new(
            rewrite_filter_for_subquery_relaxed(*inner, rtindex, targets)?,
        ))),
        Expr::IsDistinctFrom(left, right) => Some(Expr::IsDistinctFrom(
            Box::new(rewrite_filter_for_subquery_relaxed(
                *left, rtindex, targets,
            )?),
            Box::new(rewrite_filter_for_subquery_relaxed(
                *right, rtindex, targets,
            )?),
        )),
        Expr::IsNotDistinctFrom(left, right) => Some(Expr::IsNotDistinctFrom(
            Box::new(rewrite_filter_for_subquery_relaxed(
                *left, rtindex, targets,
            )?),
            Box::new(rewrite_filter_for_subquery_relaxed(
                *right, rtindex, targets,
            )?),
        )),
        Expr::Coalesce(left, right) => Some(Expr::Coalesce(
            Box::new(rewrite_filter_for_subquery_relaxed(
                *left, rtindex, targets,
            )?),
            Box::new(rewrite_filter_for_subquery_relaxed(
                *right, rtindex, targets,
            )?),
        )),
        _ => None,
    }
}

fn expr_contains_planner_volatile(expr: &Expr) -> bool {
    match expr {
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_planner_volatile),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_planner_volatile),
        Expr::Func(func) => func.args.iter().any(expr_contains_planner_volatile),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_planner_volatile(inner)
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
            expr_contains_planner_volatile(expr)
                || expr_contains_planner_volatile(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_planner_volatile(expr))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_planner_volatile(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_planner_volatile(left) || expr_contains_planner_volatile(right)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_planner_volatile(&saop.left)
                || expr_contains_planner_volatile(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_planner_volatile),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_planner_volatile(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_planner_volatile(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_planner_volatile(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_planner_volatile)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_planner_volatile)
                })
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_planner_volatile(expr))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_planner_volatile(&arm.expr)
                        || expr_contains_planner_volatile(&arm.result)
                })
                || expr_contains_planner_volatile(&case_expr.defresult)
        }
        Expr::SetReturning(_) => true,
        _ => false,
    }
}

fn expr_contains_outer_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::GroupingKey(grouping_key) => expr_contains_outer_var(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_contains_outer_var),
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_outer_var)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_outer_var)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_contains_outer_var)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_contains_outer_var),
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_outer_var),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_outer_var),
        Expr::Func(func) => func.args.iter().any(expr_contains_outer_var),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_outer_var(inner),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_outer_var(left) || expr_contains_outer_var(right)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_outer_var(&saop.left) || expr_contains_outer_var(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_outer_var),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_outer_var(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_outer_var(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_outer_var)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_outer_var)
                })
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
            expr_contains_outer_var(expr)
                || expr_contains_outer_var(pattern)
                || escape.as_deref().is_some_and(expr_contains_outer_var)
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_outer_var)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_outer_var(&arm.expr) || expr_contains_outer_var(&arm.result)
                })
                || expr_contains_outer_var(&case_expr.defresult)
        }
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_contains_outer_var)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_outer_var),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_outer_var),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_deref()
                .is_some_and(expr_contains_outer_var)
                || subplan.args.iter().any(expr_contains_outer_var)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_outer_var),
        Expr::Param(_)
        | Expr::Const(_)
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

fn rewrite_filter_for_subquery(
    expr: Expr,
    rtindex: usize,
    targets: &[&crate::include::nodes::primnodes::TargetEntry],
    query: &Query,
) -> Option<Expr> {
    match expr {
        Expr::GroupingKey(grouping_key) => {
            rewrite_filter_for_subquery(*grouping_key.expr, rtindex, targets, query).map(|expr| {
                Expr::GroupingKey(Box::new(
                    crate::include::nodes::primnodes::GroupingKeyExpr {
                        expr: Box::new(expr),
                        ref_id: grouping_key.ref_id,
                    },
                ))
            })
        }
        Expr::GroupingFunc(grouping_func) => Some(Expr::GroupingFunc(Box::new(
            crate::include::nodes::primnodes::GroupingFuncExpr {
                args: grouping_func
                    .args
                    .into_iter()
                    .map(|arg| rewrite_filter_for_subquery(arg, rtindex, targets, query))
                    .collect::<Option<Vec<_>>>()?,
                ..*grouping_func
            },
        ))),
        Expr::Var(var) => {
            if var.varlevelsup != 0 || var.varno != rtindex {
                return None;
            }
            let index = crate::include::nodes::primnodes::attrno_index(var.varattno)?;
            let target = targets.get(index)?;
            Some(target.expr.clone())
        }
        Expr::Param(_) | Expr::Const(_) => Some(expr),
        Expr::Op(mut op) => {
            op.args = op
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery(arg, rtindex, targets, query))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Op(op))
        }
        Expr::Bool(mut bool_expr) => {
            bool_expr.args = bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery(arg, rtindex, targets, query))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Bool(bool_expr))
        }
        Expr::Func(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(|arg| rewrite_filter_for_subquery(arg, rtindex, targets, query))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::Func(func))
        }
        Expr::Cast(inner, ty) => Some(Expr::Cast(
            Box::new(rewrite_filter_for_subquery(
                *inner, rtindex, targets, query,
            )?),
            ty,
        )),
        Expr::Collate {
            expr,
            collation_oid,
        } => Some(Expr::Collate {
            expr: Box::new(rewrite_filter_for_subquery(*expr, rtindex, targets, query)?),
            collation_oid,
        }),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Some(Expr::Like {
            expr: Box::new(rewrite_filter_for_subquery(*expr, rtindex, targets, query)?),
            pattern: Box::new(rewrite_filter_for_subquery(
                *pattern, rtindex, targets, query,
            )?),
            escape: rewrite_optional_subquery_filter_expr(escape, rtindex, targets, query)?,
            case_insensitive,
            negated,
            collation_oid,
        }),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Some(Expr::Similar {
            expr: Box::new(rewrite_filter_for_subquery(*expr, rtindex, targets, query)?),
            pattern: Box::new(rewrite_filter_for_subquery(
                *pattern, rtindex, targets, query,
            )?),
            escape: rewrite_optional_subquery_filter_expr(escape, rtindex, targets, query)?,
            negated,
            collation_oid,
        }),
        Expr::IsNull(inner) => Some(Expr::IsNull(Box::new(rewrite_filter_for_subquery(
            *inner, rtindex, targets, query,
        )?))),
        Expr::IsNotNull(inner) => Some(Expr::IsNotNull(Box::new(rewrite_filter_for_subquery(
            *inner, rtindex, targets, query,
        )?))),
        Expr::IsDistinctFrom(left, right) => Some(Expr::IsDistinctFrom(
            Box::new(rewrite_filter_for_subquery(*left, rtindex, targets, query)?),
            Box::new(rewrite_filter_for_subquery(
                *right, rtindex, targets, query,
            )?),
        )),
        Expr::IsNotDistinctFrom(left, right) => Some(Expr::IsNotDistinctFrom(
            Box::new(rewrite_filter_for_subquery(*left, rtindex, targets, query)?),
            Box::new(rewrite_filter_for_subquery(
                *right, rtindex, targets, query,
            )?),
        )),
        Expr::Coalesce(left, right) => Some(Expr::Coalesce(
            Box::new(rewrite_filter_for_subquery(*left, rtindex, targets, query)?),
            Box::new(rewrite_filter_for_subquery(
                *right, rtindex, targets, query,
            )?),
        )),
        Expr::ScalarArrayOp(mut saop) => {
            saop.left = Box::new(rewrite_filter_for_subquery(
                *saop.left, rtindex, targets, query,
            )?);
            saop.right = Box::new(rewrite_filter_for_subquery(
                *saop.right,
                rtindex,
                targets,
                query,
            )?);
            Some(Expr::ScalarArrayOp(saop))
        }
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Some(Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_filter_for_subquery(element, rtindex, targets, query))
                .collect::<Option<Vec<_>>>()?,
            array_type,
        }),
        Expr::Row { descriptor, fields } => Some(Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    rewrite_filter_for_subquery(expr, rtindex, targets, query)
                        .map(|expr| (name, expr))
                })
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Some(Expr::FieldSelect {
            expr: Box::new(rewrite_filter_for_subquery(*expr, rtindex, targets, query)?),
            field,
            field_type,
        }),
        Expr::ArraySubscript { array, subscripts } => Some(Expr::ArraySubscript {
            array: Box::new(rewrite_filter_for_subquery(
                *array, rtindex, targets, query,
            )?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    rewrite_subquery_filter_subscript(subscript, rtindex, targets, query)
                })
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::Case(mut case_expr) => {
            case_expr.arg =
                rewrite_optional_subquery_filter_expr(case_expr.arg, rtindex, targets, query)?;
            case_expr.args = case_expr
                .args
                .into_iter()
                .map(|arm| {
                    Some(crate::include::nodes::primnodes::CaseWhen {
                        expr: rewrite_filter_for_subquery(arm.expr, rtindex, targets, query)?,
                        result: rewrite_filter_for_subquery(arm.result, rtindex, targets, query)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            case_expr.defresult = Box::new(rewrite_filter_for_subquery(
                *case_expr.defresult,
                rtindex,
                targets,
                query,
            )?);
            Some(Expr::Case(case_expr))
        }
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
        | Expr::LocalTimestamp { .. } => Some(expr),
        Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::CaseTest(_)
        | Expr::SetReturning(_)
        | Expr::SubLink(_)
        | Expr::SubPlan(_)
        | Expr::SqlJsonQueryFunction(_)
        | Expr::Xml(_) => None,
    }
}

fn rewrite_optional_subquery_filter_expr(
    expr: Option<Box<Expr>>,
    rtindex: usize,
    targets: &[&crate::include::nodes::primnodes::TargetEntry],
    query: &Query,
) -> Option<Option<Box<Expr>>> {
    match expr {
        Some(expr) => rewrite_filter_for_subquery(*expr, rtindex, targets, query)
            .map(Box::new)
            .map(Some),
        None => Some(None),
    }
}

fn rewrite_subquery_filter_subscript(
    subscript: crate::include::nodes::primnodes::ExprArraySubscript,
    rtindex: usize,
    targets: &[&crate::include::nodes::primnodes::TargetEntry],
    query: &Query,
) -> Option<crate::include::nodes::primnodes::ExprArraySubscript> {
    Some(crate::include::nodes::primnodes::ExprArraySubscript {
        is_slice: subscript.is_slice,
        lower: match subscript.lower {
            Some(expr) => Some(rewrite_filter_for_subquery(expr, rtindex, targets, query)?),
            None => None,
        },
        upper: match subscript.upper {
            Some(expr) => Some(rewrite_filter_for_subquery(expr, rtindex, targets, query)?),
            None => None,
        },
    })
}

fn set_base_rel_pathlist(root: &mut PlannerInfo, rtindex: usize, catalog: &dyn CatalogLookup) {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)).cloned() else {
        return;
    };
    if root
        .simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .is_some_and(|rel| !rel.pathlist.is_empty())
    {
        return;
    }
    let child_rtindexes = sorted_append_child_rtindexes(root, catalog, rtindex);
    if root
        .simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .is_some_and(base_restrictinfo_is_contradictory)
        && child_rtindexes.is_empty()
    {
        if let Some(rel) = root
            .simple_rel_array
            .get_mut(rtindex)
            .and_then(Option::as_mut)
        {
            rel.add_path(optimize_path_with_config(
                const_false_relation_path(rtindex, &rte.desc),
                catalog,
                root.config,
            ));
            bestpath::set_cheapest(rel);
        }
        return;
    }
    let filter = root
        .simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .and_then(base_filter_expr);
    let constraint_exclusion_applies = root.config.constraint_exclusion_on
        || (root.config.constraint_exclusion_partition
            && is_regular_inheritance_child_rel(root, rtindex));
    if constraint_exclusion_applies
        && let RangeTblEntryKind::Relation { relation_oid, .. } = rte.kind.clone()
        && (!relation_may_satisfy_own_partition_bound(catalog, relation_oid, filter.as_ref())
            || !relation_may_satisfy_check_constraints(
                catalog,
                relation_oid,
                &rte.desc,
                filter.as_ref(),
            ))
    {
        if let Some(rel) = root
            .simple_rel_array
            .get_mut(rtindex)
            .and_then(Option::as_mut)
        {
            rel.add_path(optimize_path_with_config(
                const_false_relation_path(rtindex, &rte.desc),
                catalog,
                root.config,
            ));
            bestpath::set_cheapest(rel);
        }
        return;
    }
    if let RangeTblEntryKind::Relation {
        rel: heap_rel,
        relation_oid,
        relkind,
        relispopulated,
        toast,
        tablesample,
    } = rte.kind.clone()
        && (relkind == 'p' || !child_rtindexes.is_empty())
    {
        if matches!(root.parse.where_qual, Some(Expr::Const(Value::Bool(false)))) {
            if let Some(rel) = root
                .simple_rel_array
                .get_mut(rtindex)
                .and_then(Option::as_mut)
            {
                rel.add_path(optimize_path_with_config(
                    const_false_relation_path(rtindex, &rte.desc),
                    catalog,
                    root.config,
                ));
                bestpath::set_cheapest(rel);
            }
            return;
        }
        let query_order_items = query_order_items_for_base_rel(root, rtindex);
        let query_pathkeys = root.query_pathkeys.clone();
        let has_null_scalar_array_filter = if relkind == 'p' {
            filter
                .as_ref()
                .is_some_and(partitioned_scalar_array_null_filter)
        } else {
            filter.as_ref().is_some_and(scalar_array_null_filter)
        };
        if has_null_scalar_array_filter {
            if let Some(rel) = root
                .simple_rel_array
                .get_mut(rtindex)
                .and_then(Option::as_mut)
            {
                add_one_time_false_path(rel, rtindex, rte.desc.clone(), catalog, root.config);
            }
            return;
        }
        let required_index_only_attrs = collect_required_index_only_attrs_for_root(
            root,
            rtindex,
            filter.as_ref(),
            query_order_items.as_deref(),
        );
        let expand_children = relkind != 'p'
            || relation_may_satisfy_own_partition_bound(catalog, relation_oid, filter.as_ref());
        let mut children = Vec::new();
        let mut ordered_children = Vec::new();
        let mut child_prune_bounds = Vec::new();
        let mut ordered_child_prune_bounds = Vec::new();
        let mut ordered_child_bounds = Vec::new();
        let mut ordered_ok = query_order_items.is_some();
        let partition_spec = (relkind == 'p')
            .then(|| partition_cache::partition_spec(root, catalog, relation_oid))
            .flatten();
        let partition_child_bounds = if relkind == 'p' {
            partition_cache::partition_child_bounds(root, catalog, relation_oid)
        } else {
            Vec::new()
        };
        let ancestor_bound = if relkind == 'p' {
            relation_own_partition_bound(catalog, relation_oid)
        } else {
            None
        };
        let sibling_bounds = partition_child_bounds
            .iter()
            .filter_map(|child| child.bound.clone())
            .collect::<Vec<_>>();
        if relkind != 'p'
            && !filter
                .as_ref()
                .is_some_and(|expr| matches!(expr, Expr::Const(Value::Bool(false))))
        {
            children.push(normalize_rte_path(
                rtindex,
                &rte.desc,
                cheapest_relation_access_path(
                    rtindex,
                    heap_rel,
                    relation_display_name(catalog, &rte, relation_oid, heap_rel),
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    tablesample.clone(),
                    rte.desc.clone(),
                    filter.clone(),
                    root.config,
                    &root.index_expr_cache,
                    catalog,
                ),
                catalog,
            ));
            if let Some(order_items) = query_order_items.as_ref() {
                let ordered_parent = cheapest_path_by_total(collect_relation_ordered_index_paths(
                    rtindex,
                    heap_rel,
                    rte.alias
                        .clone()
                        .unwrap_or_else(|| format!("rel {}", heap_rel.rel_number)),
                    relation_oid,
                    toast,
                    rte.desc.clone(),
                    filter.clone(),
                    order_items,
                    &required_index_only_attrs,
                    root.config,
                    &root.index_expr_cache,
                    catalog,
                ))
                .map(|path| normalize_rte_path(rtindex, &rte.desc, path, catalog));
                if let Some(path) = ordered_parent {
                    ordered_children.push(path);
                } else {
                    ordered_ok = false;
                }
            }
        }
        for child_rtindex in child_rtindexes.into_iter().filter(|_| expand_children) {
            let child_relation_oid = root
                .parse
                .rtable
                .get(child_rtindex.saturating_sub(1))
                .and_then(|child_rte| {
                    if let RangeTblEntryKind::Relation { relation_oid, .. } = child_rte.kind {
                        Some(relation_oid)
                    } else {
                        None
                    }
                });
            let child_bound = if relkind == 'p' {
                child_relation_oid.and_then(|child_oid| {
                    partition_child_bounds
                        .iter()
                        .find(|child| child.row.inhrelid == child_oid)
                        .and_then(|child| child.bound.clone())
                })
            } else {
                None
            };
            if relkind == 'p'
                && root.config.enable_partition_pruning
                && !partition_may_satisfy_filter_for_relation(
                    partition_spec.as_ref(),
                    child_bound.as_ref(),
                    &sibling_bounds,
                    ancestor_bound.as_ref(),
                    filter.as_ref(),
                    catalog,
                    child_relation_oid.unwrap_or(relation_oid),
                )
            {
                continue;
            }
            set_base_rel_pathlist(root, child_rtindex, catalog);
            let Some(child_path) = root
                .simple_rel_array
                .get(child_rtindex)
                .and_then(Option::as_ref)
                .and_then(|rel| rel.cheapest_total_path())
                .cloned()
            else {
                continue;
            };
            if path_is_const_false_filter(&child_path) {
                continue;
            }
            let translated_vars = append_translation(root, child_rtindex)
                .map(|info| info.translated_vars.clone())
                .unwrap_or_default();
            children.push(project_to_slot_layout(
                rtindex,
                &rte.desc,
                child_path,
                PathTarget::new(translated_vars),
                catalog,
            ));
            child_prune_bounds.push(child_bound.clone());
            if ordered_ok {
                let translated_pathkeys =
                    translate_append_pathkeys_for_child(root, child_rtindex, &query_pathkeys);
                let ordered_child_path = cheapest_path_by_total(relation_ordered_index_paths(
                    root,
                    child_rtindex,
                    &translated_pathkeys,
                    catalog,
                ))
                .or_else(|| {
                    root.simple_rel_array
                        .get(child_rtindex)
                        .and_then(Option::as_ref)
                        .and_then(|rel| {
                            bestpath::get_cheapest_path_for_pathkeys(
                                rel,
                                &translated_pathkeys,
                                bestpath::CostSelector::Total,
                            )
                        })
                        .cloned()
                });
                if let Some(path) = ordered_child_path {
                    if path_is_const_false_filter(&path) {
                        continue;
                    }
                    let translated_vars = append_translation(root, child_rtindex)
                        .map(|info| info.translated_vars.clone())
                        .unwrap_or_default();
                    if relkind == 'p' {
                        let Some(bound) = child_relation_oid.and_then(|child_oid| {
                            partition_child_bounds
                                .iter()
                                .find(|child| child.row.inhrelid == child_oid)
                                .and_then(|child| child.bound.clone())
                        }) else {
                            ordered_ok = false;
                            continue;
                        };
                        ordered_child_bounds.push(bound);
                    }
                    ordered_children.push(project_to_slot_layout(
                        rtindex,
                        &rte.desc,
                        path,
                        PathTarget::new(translated_vars),
                        catalog,
                    ));
                    ordered_child_prune_bounds.push(child_bound.clone());
                } else {
                    ordered_ok = false;
                }
            }
        }
        let append_target =
            slot_output_target(rtindex, &rte.desc.columns, |column| column.sql_type);
        let partition_prune = root
            .config
            .enable_partition_pruning
            .then(|| {
                append_partition_prune_plan(
                    partition_spec.clone(),
                    &sibling_bounds,
                    filter.as_ref(),
                    &child_prune_bounds,
                )
            })
            .flatten();
        let append = if children.is_empty() {
            optimize_path_with_config(
                Path::Filter {
                    plan_info: PlanEstimate::default(),
                    pathtarget: append_target.clone(),
                    predicate: Expr::Const(Value::Bool(false)),
                    input: Box::new(Path::Append {
                        plan_info: PlanEstimate::default(),
                        pathtarget: append_target,
                        pathkeys: Vec::new(),
                        relids: vec![rtindex],
                        source_id: rtindex,
                        desc: rte.desc.clone(),
                        child_roots: Vec::new(),
                        partition_prune,
                        children: Vec::new(),
                    }),
                },
                catalog,
                root.config,
            )
        } else {
            optimize_path_with_config(
                Path::Append {
                    plan_info: PlanEstimate::default(),
                    pathtarget: append_target,
                    pathkeys: Vec::new(),
                    relids: vec![rtindex],
                    source_id: rtindex,
                    desc: rte.desc.clone(),
                    child_roots: Vec::new(),
                    partition_prune,
                    children,
                },
                catalog,
                root.config,
            )
        };
        let ordered_path = if ordered_ok {
            query_order_items.map(|items| {
                let partition_prune = root
                    .config
                    .enable_partition_pruning
                    .then(|| {
                        append_partition_prune_plan(
                            partition_spec.clone(),
                            &sibling_bounds,
                            filter.as_ref(),
                            &ordered_child_prune_bounds,
                        )
                    })
                    .flatten();
                if relkind == 'p'
                    && let Some(proof) = ordered_partition_append_proof(
                        root,
                        partition_spec.as_ref(),
                        &ordered_child_bounds,
                        filter.as_ref(),
                        &query_pathkeys,
                    )
                {
                    let mut ordered_append_children = ordered_children.clone();
                    if proof.reverse_children {
                        ordered_append_children.reverse();
                    }
                    optimize_path_with_config(
                        Path::Append {
                            plan_info: PlanEstimate::default(),
                            pathtarget: slot_output_target(rtindex, &rte.desc.columns, |column| {
                                column.sql_type
                            }),
                            pathkeys: proof.pathkeys,
                            relids: vec![rtindex],
                            source_id: rtindex,
                            desc: rte.desc.clone(),
                            child_roots: Vec::new(),
                            partition_prune,
                            children: ordered_append_children,
                        },
                        catalog,
                        root.config,
                    )
                } else {
                    optimize_path_with_config(
                        Path::MergeAppend {
                            plan_info: PlanEstimate::default(),
                            pathtarget: slot_output_target(rtindex, &rte.desc.columns, |column| {
                                column.sql_type
                            }),
                            source_id: rtindex,
                            desc: rte.desc.clone(),
                            items,
                            partition_prune,
                            children: ordered_children,
                        },
                        catalog,
                        root.config,
                    )
                }
            })
        } else {
            None
        };
        let Some(rel) = root
            .simple_rel_array
            .get_mut(rtindex)
            .and_then(Option::as_mut)
        else {
            return;
        };
        rel.add_path(append);
        if let Some(path) = ordered_path {
            rel.add_path(path);
        }
        bestpath::set_cheapest(rel);
        return;
    }
    let query_order_items = query_order_items_for_base_rel(root, rtindex);
    let base_filter = root
        .simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .and_then(base_filter_expr);
    let required_index_only_attrs = collect_required_index_only_attrs_for_root(
        root,
        rtindex,
        base_filter.as_ref(),
        query_order_items.as_deref(),
    );
    let subquery_used_attrs = if matches!(&rte.kind, RangeTblEntryKind::Subquery { .. }) {
        Some(used_parent_attrs_for_rte(
            root,
            rtindex,
            rte.desc.columns.len(),
        ))
    } else {
        None
    };
    let Some(rel) = root
        .simple_rel_array
        .get_mut(rtindex)
        .and_then(Option::as_mut)
    else {
        return;
    };

    match rte.kind {
        RangeTblEntryKind::Result => rel.add_path(optimize_path_with_config(
            Path::Result {
                plan_info: PlanEstimate::default(),
                pathtarget: PathTarget::new(Vec::new()),
            },
            catalog,
            root.config,
        )),
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            ref tablesample,
        } => rel.pathlist.extend(collect_relation_access_paths(
            rtindex,
            heap_rel,
            relation_display_name(catalog, &rte, relation_oid, heap_rel),
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample.clone(),
            rte.desc.clone(),
            base_filter,
            query_order_items,
            &required_index_only_attrs,
            root.config,
            &root.index_expr_cache,
            catalog,
        )),
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => {
            let mut path = optimize_path_with_config(
                Path::Values {
                    plan_info: PlanEstimate::default(),
                    pathtarget: slot_output_target(rtindex, &output_columns, |column| {
                        column.sql_type
                    }),
                    slot_id: rte_slot_id(rtindex),
                    rows,
                    output_columns,
                },
                catalog,
                root.config,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path_with_config(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Function { call } => {
            let mut path = optimize_path_with_config(
                Path::FunctionScan {
                    plan_info: PlanEstimate::default(),
                    pathtarget: slot_output_target(rtindex, call.output_columns(), |column| {
                        column.sql_type
                    }),
                    slot_id: rtindex,
                    call,
                    table_alias: rte.alias.clone(),
                },
                catalog,
                root.config,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path_with_config(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::WorkTable { worktable_id } => {
            let mut path = optimize_path_with_config(
                Path::WorkTableScan {
                    plan_info: PlanEstimate::default(),
                    pathtarget: slot_output_target(
                        rtindex,
                        &rte.desc
                            .columns
                            .iter()
                            .map(|column| QueryColumn {
                                name: column.name.clone(),
                                sql_type: column.sql_type,
                                wire_type_oid: None,
                            })
                            .collect::<Vec<_>>(),
                        |column| column.sql_type,
                    ),
                    slot_id: rte_slot_id(rtindex),
                    worktable_id,
                    output_columns: rte
                        .desc
                        .columns
                        .iter()
                        .map(|column| QueryColumn {
                            name: column.name.clone(),
                            sql_type: column.sql_type,
                            wire_type_oid: None,
                        })
                        .collect(),
                },
                catalog,
                root.config,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path_with_config(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Cte { cte_id, query } => {
            let mut path = build_cte_scan_path(
                rtindex,
                cte_id,
                rte.eref.aliasname.clone(),
                *query,
                &rte.desc,
                catalog,
                root.config,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path_with_config(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Subquery { query } => {
            let (query, filter) = push_subquery_filter(rtindex, *query, base_filter_expr(rel));
            let mut path = build_subquery_scan_path(
                rtindex,
                query,
                subquery_used_attrs.as_ref().expect("subquery used attrs"),
                &rte.desc,
                catalog,
                root.config,
            );
            if let Some(filter) = filter {
                path = optimize_path_with_config(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Join { .. } => unreachable!("join RTEs are not base relations"),
    }
    bestpath::set_cheapest(rel);
}

fn relation_may_satisfy_check_constraints(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    desc: &RelationDesc,
    filter: Option<&Expr>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Ok(constraints) = bind_relation_constraints(None, relation_oid, desc, catalog) else {
        return true;
    };
    constraints
        .checks
        .iter()
        .filter(|check| check.enforced && check.validated)
        .all(|check| !exprs_have_contradictory_equalities(filter, &check.expr))
}

fn set_base_rel_pathlists(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let max_rtindex = root.simple_rel_array.len().saturating_sub(1);
    for rtindex in 1..=max_rtindex {
        if root
            .simple_rel_array
            .get(rtindex)
            .and_then(Option::as_ref)
            .is_some()
        {
            set_base_rel_pathlist(root, rtindex, catalog);
        }
    }
}

fn build_join_restrict_clauses(
    root: &PlannerInfo,
    kind: JoinType,
    explicit_qual: Option<Expr>,
    left_relids: &[usize],
    right_relids: &[usize],
    inner_join_clauses: &[RestrictInfo],
) -> Vec<RestrictInfo> {
    let join_relids = relids_union(left_relids, right_relids);
    let mut clauses = Vec::new();
    if let Some(explicit_qual) = explicit_qual {
        let explicit_qual = expand_join_rte_vars(root, explicit_qual);
        clauses.extend(
            flatten_and_conjuncts(&explicit_qual)
                .into_iter()
                .map(|clause| simplify_nullability_restriction(root, clause))
                .filter(|clause| !matches!(clause, Expr::Const(Value::Bool(true))))
                .map(|clause| joininfo::make_restrict_info_with_pushdown(clause, false)),
        );
    }
    if matches!(kind, JoinType::Inner | JoinType::Cross) {
        for restrict in inner_join_clauses {
            let clause = &restrict.clause;
            let clause_relids = &restrict.required_relids;
            if clause_relids.len() <= 1 {
                continue;
            }
            if relids_subset(&clause_relids, &join_relids)
                && !relids_subset(&clause_relids, left_relids)
                && !relids_subset(&clause_relids, right_relids)
                && !clauses
                    .iter()
                    .any(|existing: &RestrictInfo| existing.clause == *clause)
            {
                clauses.push(restrict.clone());
            }
        }
    }
    if matches!(kind, JoinType::Inner | JoinType::Cross) && !has_outer_joins(root) {
        remove_redundant_join_equalities_with_base_filters(
            root,
            left_relids,
            right_relids,
            &mut clauses,
        );
    }
    clauses
}

fn remove_redundant_join_equalities_with_base_filters(
    root: &PlannerInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    clauses: &mut Vec<RestrictInfo>,
) {
    let mut remove = BTreeSet::new();
    for left_index in 0..clauses.len() {
        if remove.contains(&left_index) {
            continue;
        }
        for right_index in (left_index + 1)..clauses.len() {
            if remove.contains(&right_index) {
                continue;
            }
            let Some((left_a, left_b)) = equality_clause_args(&clauses[left_index].clause) else {
                continue;
            };
            let Some((right_a, right_b)) = equality_clause_args(&clauses[right_index].clause)
            else {
                continue;
            };
            if implied_same_relation_equalities(left_a, left_b, right_a, right_b)
                .into_iter()
                .any(|(left_expr, right_expr)| {
                    base_restrictinfo_has_equality(root, left_expr, right_expr)
                })
            {
                let remove_index = redundant_join_equality_to_remove(
                    root,
                    left_relids,
                    right_relids,
                    &clauses[left_index],
                    &clauses[right_index],
                    left_index,
                    right_index,
                );
                remove.insert(remove_index);
                if remove_index == left_index {
                    break;
                }
            }
        }
    }
    if remove.is_empty() {
        return;
    }
    let mut index = 0usize;
    clauses.retain(|_| {
        let keep = !remove.contains(&index);
        index += 1;
        keep
    });
}

fn redundant_join_equality_to_remove(
    root: &PlannerInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    left_clause: &RestrictInfo,
    right_clause: &RestrictInfo,
    left_index: usize,
    right_index: usize,
) -> usize {
    let left_is_partition_key =
        restrict_is_partition_key_equality(root, left_relids, right_relids, left_clause);
    let right_is_partition_key =
        restrict_is_partition_key_equality(root, left_relids, right_relids, right_clause);
    match (left_is_partition_key, right_is_partition_key) {
        // PostgreSQL can use a partition-key equality to prove partitionwise
        // join legality while still keeping the non-key equality as the
        // executable join qual once a same-relation base filter was derived.
        (true, false) => left_index,
        (false, true) => right_index,
        _ => right_index,
    }
}

fn restrict_is_partition_key_equality(
    root: &PlannerInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    restrict: &RestrictInfo,
) -> bool {
    let (Some(left_key), Some(right_key)) = (
        single_relation_partition_key(root, left_relids),
        single_relation_partition_key(root, right_relids),
    ) else {
        return false;
    };
    let Some((arg_a, arg_b)) = equality_clause_args(&restrict.clause) else {
        return false;
    };
    (arg_a == left_key && arg_b == right_key) || (arg_a == right_key && arg_b == left_key)
}

fn single_relation_partition_key<'a>(root: &'a PlannerInfo, relids: &[usize]) -> Option<&'a Expr> {
    if relids.len() != 1 {
        return None;
    }
    root.simple_rel_array
        .get(relids[0])
        .and_then(Option::as_ref)
        .and_then(|rel| rel.partition_info.as_ref())
        .and_then(|info| info.key_exprs.first())
}

fn base_restrictinfo_has_equality(root: &PlannerInfo, left: &Expr, right: &Expr) -> bool {
    let left_relids = expr_relids(left);
    let right_relids = expr_relids(right);
    if left_relids.len() != 1 || left_relids != right_relids {
        return false;
    }
    let clause = Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
        op: OpExprKind::Eq,
        opno: 0,
        opfuncid: 0,
        opresulttype: SqlType::new(SqlTypeKind::Bool),
        args: vec![left.clone(), right.clone()],
        collation_oid: None,
    }));
    root.simple_rel_array
        .get(left_relids[0])
        .and_then(Option::as_ref)
        .map(|rel| {
            rel.baserestrictinfo
                .iter()
                .any(|restrict| equalities_match_commuted(&restrict.clause, &clause))
        })
        .unwrap_or(false)
}

#[derive(Clone, Copy)]
enum LevelRelRef {
    Base(usize),
    Join(usize),
}

#[derive(Clone)]
struct PartitionJoinSegment {
    bound: PartitionBoundSpec,
    path: Path,
}

fn rel_at_level<'a>(root: &'a PlannerInfo, rel_ref: LevelRelRef) -> &'a RelOptInfo {
    match rel_ref {
        LevelRelRef::Base(rtindex) => root.simple_rel_array[rtindex]
            .as_ref()
            .expect("base rel ref should point at an existing rel"),
        LevelRelRef::Join(index) => root
            .join_rel_list
            .get(index)
            .expect("join rel ref should point at an existing rel"),
    }
}

fn rel_refs_at_level(root: &PlannerInfo, level: usize) -> Vec<LevelRelRef> {
    if level == 1 {
        root.simple_rel_array
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(rtindex, rel)| {
                rel.as_ref()
                    .filter(|rel| rel.reloptkind != RelOptKind::OtherMemberRel)
                    .map(|_| LevelRelRef::Base(rtindex))
            })
            .collect()
    } else {
        root.join_rel_list
            .iter()
            .enumerate()
            .filter(|(_, rel)| rel.relids.len() == level)
            .map(|(index, _)| LevelRelRef::Join(index))
            .collect()
    }
}

fn find_join_rel_index(root: &PlannerInfo, relids: &[usize]) -> Option<usize> {
    root.join_rel_list
        .iter()
        .position(|rel| rel.relids == relids)
}

fn join_reltarget(
    _root: &PlannerInfo,
    _relids: &[usize],
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
) -> PathTarget {
    let mut exprs = left_rel.reltarget.exprs.clone();
    let mut sortgrouprefs = left_rel.reltarget.sortgrouprefs.clone();
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        exprs.extend(right_rel.reltarget.exprs.clone());
        sortgrouprefs.extend(right_rel.reltarget.sortgrouprefs.clone());
    }
    PathTarget::with_sortgrouprefs(exprs, sortgrouprefs)
}

fn exact_cross_join_relids(root: &PlannerInfo, target_relids: &[usize]) -> bool {
    fn walk(node: &JoinTreeNode, target_relids: &[usize]) -> bool {
        match node {
            JoinTreeNode::RangeTblRef(_) => false,
            JoinTreeNode::JoinExpr {
                left, right, kind, ..
            } => {
                let relids = relids_union(&jointree_relids(left), &jointree_relids(right));
                (matches!(kind, JoinType::Cross) && relids == target_relids)
                    || walk(left, target_relids)
                    || walk(right, target_relids)
            }
        }
    }

    root.parse
        .jointree
        .as_ref()
        .is_some_and(|jointree| walk(jointree, target_relids))
}

fn physical_join_kind_for_paths(
    root: &PlannerInfo,
    logical_kind: JoinType,
    relids: &[usize],
    join_restrict_clauses: &[RestrictInfo],
) -> JoinType {
    if matches!(logical_kind, JoinType::Cross) {
        if join_restrict_clauses.is_empty() {
            JoinType::Cross
        } else {
            JoinType::Inner
        }
    } else if matches!(logical_kind, JoinType::Inner)
        && join_restrict_clauses.is_empty()
        && exact_cross_join_relids(root, relids)
    {
        JoinType::Cross
    } else {
        logical_kind
    }
}

fn jointree_relids(node: &JoinTreeNode) -> Vec<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
        JoinTreeNode::JoinExpr { left, right, .. } => {
            relids_union(&jointree_relids(left), &jointree_relids(right))
        }
    }
}

fn join_spec_for_special_join(sjinfo: &SpecialJoinInfo, reversed: bool) -> JoinBuildSpec {
    JoinBuildSpec {
        kind: if reversed {
            reverse_join_type(sjinfo.jointype)
        } else {
            sjinfo.jointype
        },
        reversed,
        rtindex: Some(sjinfo.rtindex),
        explicit_qual: Some(sjinfo.join_quals.clone()),
    }
}

fn special_join_relids(sjinfo: &SpecialJoinInfo) -> Vec<usize> {
    relids_union(&sjinfo.syn_lefthand, &sjinfo.syn_righthand)
}

fn rel_contains_special_join(relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_subset(&sjinfo.min_lefthand, relids) && relids_subset(&sjinfo.min_righthand, relids)
}

fn rel_matches_special_join(
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<bool> {
    if relids_subset(&sjinfo.min_lefthand, left_relids)
        && relids_subset(&sjinfo.min_righthand, right_relids)
    {
        Some(false)
    } else if relids_subset(&sjinfo.min_lefthand, right_relids)
        && relids_subset(&sjinfo.min_righthand, left_relids)
    {
        Some(true)
    } else {
        None
    }
}

fn relids_match_ojrelid(root: &PlannerInfo, relids: &[usize], ojrelid: usize) -> bool {
    root.join_info_list
        .iter()
        .any(|sjinfo| sjinfo.ojrelid == Some(ojrelid) && special_join_relids(sjinfo) == relids)
}

fn input_crosses_rhs_boundary(input_relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_overlap(input_relids, &sjinfo.min_righthand)
        && !relids_subset(input_relids, &sjinfo.min_righthand)
}

fn input_can_commute_past_special_join(
    root: &PlannerInfo,
    input_relids: &[usize],
    sjinfo: &SpecialJoinInfo,
) -> bool {
    if !input_crosses_rhs_boundary(input_relids, sjinfo) {
        return true;
    }
    sjinfo
        .commute_below_l
        .iter()
        .chain(sjinfo.commute_below_r.iter())
        .any(|ojrelid| relids_match_ojrelid(root, input_relids, *ojrelid))
}

fn violates_full_join_barrier(
    root: &PlannerInfo,
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    joinrelids: &[usize],
) -> bool {
    if sjinfo.jointype != JoinType::Full {
        return false;
    }
    let full_relids = special_join_relids(sjinfo);
    relids_overlap(&full_relids, joinrelids)
        && full_relids != joinrelids
        && full_relids != left_relids
        && full_relids != right_relids
        && (relids_overlap(&full_relids, left_relids) || relids_overlap(&full_relids, right_relids))
        && !rel_contains_special_join(left_relids, sjinfo)
        && !rel_contains_special_join(right_relids, sjinfo)
        && !input_can_commute_past_special_join(root, left_relids, sjinfo)
        && !input_can_commute_past_special_join(root, right_relids, sjinfo)
}

fn join_is_legal(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> Option<JoinBuildSpec> {
    let joinrelids = relids_union(&left_rel.relids, &right_rel.relids);
    let mut matched_sj: Option<(&SpecialJoinInfo, bool)> = None;
    let mut must_be_leftjoin = false;

    for sjinfo in &root.join_info_list {
        if violates_full_join_barrier(
            root,
            sjinfo,
            &left_rel.relids,
            &right_rel.relids,
            &joinrelids,
        ) {
            return None;
        }
        if !relids_overlap(&sjinfo.min_righthand, &joinrelids) {
            continue;
        }
        if relids_subset(&joinrelids, &sjinfo.min_righthand) {
            continue;
        }
        if rel_contains_special_join(&left_rel.relids, sjinfo) {
            continue;
        }
        if rel_contains_special_join(&right_rel.relids, sjinfo) {
            continue;
        }

        if let Some(reversed) =
            rel_matches_special_join(sjinfo, &left_rel.relids, &right_rel.relids)
        {
            if matched_sj.is_some() {
                return None;
            }
            matched_sj = Some((sjinfo, reversed));
            continue;
        }

        if relids_overlap(&left_rel.relids, &sjinfo.min_righthand)
            && relids_overlap(&right_rel.relids, &sjinfo.min_righthand)
        {
            if input_can_commute_past_special_join(root, &left_rel.relids, sjinfo)
                && input_can_commute_past_special_join(root, &right_rel.relids, sjinfo)
            {
                continue;
            }
            return None;
        }
        if sjinfo.jointype != JoinType::Left || relids_overlap(&joinrelids, &sjinfo.min_lefthand) {
            return None;
        }
        must_be_leftjoin = true;
    }

    if must_be_leftjoin
        && !matched_sj
            .is_some_and(|(sjinfo, _)| sjinfo.jointype == JoinType::Left && sjinfo.lhs_strict)
    {
        return None;
    }

    if let Some((sjinfo, reversed)) = matched_sj {
        return Some(join_spec_for_special_join(sjinfo, reversed));
    }

    Some(JoinBuildSpec {
        kind: JoinType::Inner,
        reversed: false,
        rtindex: None,
        explicit_qual: None,
    })
}

#[derive(Clone)]
struct PartitionKeySpec {
    strategy: PartitionStrategy,
    keys: Vec<(i16, crate::backend::parser::SqlType)>,
}

fn partition_key_spec_for_rtindex(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    rtindex: usize,
) -> Option<PartitionKeySpec> {
    let relation_oid = relation_oid_for_rtindex(root, rtindex)?;
    let relation = catalog.relation_by_oid(relation_oid)?;
    let spec = partition_cache::partition_spec(root, catalog, relation_oid)?;
    let keys = spec
        .partattrs
        .iter()
        .map(|attno| {
            relation
                .desc
                .columns
                .get(usize::try_from(*attno).ok()?.saturating_sub(1))
                .map(|column| (*attno, column.sql_type))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(PartitionKeySpec {
        strategy: spec.strategy,
        keys,
    })
}

fn compatible_partition_key_specs(left: &PartitionKeySpec, right: &PartitionKeySpec) -> bool {
    left.strategy == right.strategy
        && left.keys.len() == right.keys.len()
        && left
            .keys
            .iter()
            .zip(&right.keys)
            .all(|((_, left_type), (_, right_type))| left_type == right_type)
}

fn expr_is_rel_attr(expr: &Expr, relid: usize, attno: i16) -> bool {
    matches!(
        expr,
        Expr::Var(var)
            if var.varlevelsup == 0
                && var.varno == relid
                && var.varattno == i32::from(attno)
    )
}

fn clause_equates_partition_attrs(
    restrict: &RestrictInfo,
    left_relid: usize,
    left_attno: i16,
    right_relid: usize,
    right_attno: i16,
) -> bool {
    let Expr::Op(op) = &restrict.clause else {
        return false;
    };
    if !matches!(op.op, crate::include::nodes::primnodes::OpExprKind::Eq) {
        return false;
    }
    let [left, right] = op.args.as_slice() else {
        return false;
    };
    (expr_is_rel_attr(left, left_relid, left_attno)
        && expr_is_rel_attr(right, right_relid, right_attno))
        || (expr_is_rel_attr(left, right_relid, right_attno)
            && expr_is_rel_attr(right, left_relid, left_attno))
}

fn has_partitionwise_equi_join(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    left_relids: &[usize],
    right_relids: &[usize],
    restrict_clauses: &[RestrictInfo],
) -> bool {
    let clauses = restrict_clauses
        .iter()
        .chain(root.inner_join_clauses.iter())
        .collect::<Vec<_>>();
    for left_relid in left_relids {
        let Some(left_spec) = partition_key_spec_for_rtindex(root, catalog, *left_relid) else {
            continue;
        };
        for right_relid in right_relids {
            let Some(right_spec) = partition_key_spec_for_rtindex(root, catalog, *right_relid)
            else {
                continue;
            };
            if !compatible_partition_key_specs(&left_spec, &right_spec) {
                continue;
            }
            let all_keys_equated = left_spec.keys.iter().zip(&right_spec.keys).all(
                |((left_attno, _), (right_attno, _))| {
                    clauses.iter().any(|restrict| {
                        clause_equates_partition_attrs(
                            restrict,
                            *left_relid,
                            *left_attno,
                            *right_relid,
                            *right_attno,
                        )
                    })
                },
            );
            if all_keys_equated {
                return true;
            }
        }
    }
    false
}

fn path_partition_bound(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    path: &Path,
) -> Option<PartitionBoundSpec> {
    path_relids(path)
        .into_iter()
        .find_map(|relid| partition_bound_for_rtindex(root, catalog, relid))
}

fn partition_segments_for_rel(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    rel: &RelOptInfo,
) -> Option<Vec<PartitionJoinSegment>> {
    let prefer_ordered = !root.query_pathkeys.is_empty();
    rel.pathlist
        .iter()
        .filter(|path| prefer_ordered == matches!(path, Path::MergeAppend { .. }))
        .chain(rel.pathlist.iter())
        .find_map(|path| {
            let children = match path {
                Path::Append {
                    relids, children, ..
                } if relids == &rel.relids => children,
                Path::MergeAppend { children, .. } if path_relids(path) == rel.relids => children,
                _ => return None,
            };
            if children.is_empty() {
                return None;
            }
            let segments = children
                .iter()
                .filter_map(|child| {
                    path_partition_bound(root, catalog, child).map(|bound| PartitionJoinSegment {
                        bound,
                        path: child.clone(),
                    })
                })
                .collect::<Vec<_>>();
            (segments.len() == children.len()).then_some(segments)
        })
}

fn query_targets_whole_row_rel(root: &PlannerInfo, relids: &[usize]) -> bool {
    root.parse.target_list.iter().any(|target| {
        let expr = joininfo::flatten_join_alias_vars_query(&root.parse, target.expr.clone());
        expr_is_whole_row_rel(root, &expr, relids)
    })
}

fn query_accumulators_whole_row_rel(root: &PlannerInfo, relids: &[usize]) -> bool {
    root.parse.accumulators.iter().any(|accum| {
        accum
            .args
            .iter()
            .chain(accum.direct_args.iter())
            .any(|expr| {
                let expr = joininfo::flatten_join_alias_vars_query(&root.parse, expr.clone());
                expr_is_whole_row_rel(root, &expr, relids)
            })
    })
}

fn expr_is_whole_row_rel(root: &PlannerInfo, expr: &Expr, relids: &[usize]) -> bool {
    match expr {
        Expr::Row {
            descriptor, fields, ..
        } => {
            row_type_targets_rel(root, descriptor.typrelid, relids)
                || relids.iter().any(|relid| {
                    let Some(rte) = root.parse.rtable.get(relid.saturating_sub(1)) else {
                        return false;
                    };
                    fields.len() == rte.desc.columns.len()
                        && fields.iter().enumerate().all(|(index, (_, expr))| {
                            matches!(
                                expr,
                                Expr::Var(var)
                                    if var.varno == *relid
                                        && var.varlevelsup == 0
                                        && var.varattno == user_attrno(index)
                            )
                        })
                })
        }
        Expr::Case(case_expr) => {
            row_type_targets_rel(root, case_expr.casetype.typrelid, relids)
                || case_expr
                    .arg
                    .as_deref()
                    .is_some_and(|arg| expr_is_whole_row_rel(root, arg, relids))
                || case_expr.args.iter().any(|arm| {
                    expr_is_whole_row_rel(root, &arm.expr, relids)
                        || expr_is_whole_row_rel(root, &arm.result, relids)
                })
                || expr_is_whole_row_rel(root, &case_expr.defresult, relids)
        }
        Expr::Op(op) => op
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Func(func) => func
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Cast(inner, _)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Collate { expr: inner, .. } => expr_is_whole_row_rel(root, inner, relids),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_is_whole_row_rel(root, left, relids) || expr_is_whole_row_rel(root, right, relids)
        }
        _ => false,
    }
}

fn row_type_targets_rel(root: &PlannerInfo, typrelid: u32, relids: &[usize]) -> bool {
    typrelid != 0
        && relids.iter().any(|relid| {
            root.parse
                .rtable
                .get(relid.saturating_sub(1))
                .is_some_and(|rte| match &rte.kind {
                    RangeTblEntryKind::Relation { relation_oid, .. } => *relation_oid == typrelid,
                    _ => false,
                })
        })
}

fn partition_join_segment_pairs(
    left: Vec<PartitionJoinSegment>,
    right: Vec<PartitionJoinSegment>,
    kind: JoinType,
) -> Option<Vec<(PartitionJoinSegment, PartitionJoinSegment)>> {
    let mut pairs = Vec::new();
    let mut right_match_counts = vec![0usize; right.len()];
    for left_segment in &left {
        let matched = right
            .iter()
            .enumerate()
            .filter(|(_, right_segment)| {
                partition_bounds_overlap(&left_segment.bound, &right_segment.bound)
            })
            .map(|(index, right_segment)| (index, right_segment.clone()))
            .collect::<Vec<_>>();
        if matched.is_empty() && !matches!(kind, JoinType::Inner | JoinType::Semi) {
            return None;
        }
        if matched.len() > 1 {
            return None;
        }
        pairs.extend(matched.into_iter().map(|(right_index, right_segment)| {
            right_match_counts[right_index] += 1;
            (left_segment.clone(), right_segment)
        }));
    }
    if right_match_counts.iter().any(|count| *count > 1) {
        return None;
    }
    if !matches!(kind, JoinType::Inner | JoinType::Semi)
        && right_match_counts.iter().any(|count| *count == 0)
    {
        return None;
    }
    (!pairs.is_empty()).then_some(pairs)
}

fn partition_bounds_overlap(left: &PartitionBoundSpec, right: &PartitionBoundSpec) -> bool {
    match (left, right) {
        (
            PartitionBoundSpec::Range {
                from: left_from,
                to: left_to,
                is_default: false,
            },
            PartitionBoundSpec::Range {
                from: right_from,
                to: right_to,
                is_default: false,
            },
        ) => range_bounds_overlap(left_from, left_to, right_from, right_to),
        (
            PartitionBoundSpec::List {
                values: left_values,
                is_default: false,
            },
            PartitionBoundSpec::List {
                values: right_values,
                is_default: false,
            },
        ) => left_values.iter().any(|left_value| {
            if matches!(left_value, SerializedPartitionValue::Null) {
                return false;
            }
            right_values.iter().any(|right_value| {
                !matches!(right_value, SerializedPartitionValue::Null) && left_value == right_value
            })
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
        ) => hash_bounds_overlap(
            *left_modulus,
            *left_remainder,
            *right_modulus,
            *right_remainder,
        ),
        _ => left == right,
    }
}

fn range_bounds_overlap(
    left_from: &[PartitionRangeDatumValue],
    left_to: &[PartitionRangeDatumValue],
    right_from: &[PartitionRangeDatumValue],
    right_to: &[PartitionRangeDatumValue],
) -> bool {
    range_datums_cmp(left_from, right_to) == Ordering::Less
        && range_datums_cmp(right_from, left_to) == Ordering::Less
}

fn hash_bounds_overlap(
    left_modulus: i32,
    left_remainder: i32,
    right_modulus: i32,
    right_remainder: i32,
) -> bool {
    if left_modulus <= 0 || right_modulus <= 0 {
        return false;
    }
    let gcd = gcd_i32(left_modulus, right_modulus);
    (left_remainder - right_remainder).rem_euclid(gcd) == 0
}

fn gcd_i32(mut left: i32, mut right: i32) -> i32 {
    left = left.abs();
    right = right.abs();
    while right != 0 {
        let next = left % right;
        left = right;
        right = next;
    }
    left.max(1)
}

fn translate_restrict_clauses_to_child(
    root: &PlannerInfo,
    restrict_clauses: &[RestrictInfo],
    child_relids: &[usize],
) -> Vec<RestrictInfo> {
    restrict_clauses
        .iter()
        .map(|restrict| {
            let mut clause = restrict.clause.clone();
            for child_relid in child_relids {
                if let Some(info) = append_translation(root, *child_relid) {
                    clause = translate_append_rel_expr(clause, info);
                }
            }
            joininfo::translated_restrict_info(clause, restrict)
        })
        .collect()
}

fn best_path(root: &PlannerInfo, paths: Vec<Path>) -> Option<Path> {
    let prefer_startup = root.parse.limit_count.is_some_and(|limit| limit <= 100);
    let prefer_runtime_index_nested_loop = root.parse.limit_count.is_some_and(|limit| limit == 100)
        || root.query_pathkeys.iter().any(|pathkey| pathkey.descending);
    paths.into_iter().min_by(|left, right| {
        if prefer_runtime_index_nested_loop {
            let left_preferred = limited_runtime_index_nested_loop(left);
            let right_preferred = limited_runtime_index_nested_loop(right);
            if left_preferred != right_preferred {
                return right_preferred.cmp(&left_preferred);
            }
        }
        if bestpath::preferred_parameterized_index_nested_loop(left)
            && !bestpath::preferred_parameterized_index_nested_loop(right)
        {
            return Ordering::Less;
        }
        if bestpath::preferred_parameterized_index_nested_loop(right)
            && !bestpath::preferred_parameterized_index_nested_loop(left)
        {
            return Ordering::Greater;
        }
        if bestpath::preferred_parameterized_nested_loop(left)
            && !bestpath::preferred_parameterized_nested_loop(right)
        {
            return Ordering::Less;
        }
        if bestpath::preferred_parameterized_nested_loop(right)
            && !bestpath::preferred_parameterized_nested_loop(left)
        {
            return Ordering::Greater;
        }
        if prefer_startup {
            left.plan_info()
                .startup_cost
                .as_f64()
                .partial_cmp(&right.plan_info().startup_cost.as_f64())
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    left.plan_info()
                        .total_cost
                        .as_f64()
                        .partial_cmp(&right.plan_info().total_cost.as_f64())
                        .unwrap_or(Ordering::Equal)
                })
        } else {
            left.plan_info()
                .total_cost
                .as_f64()
                .partial_cmp(&right.plan_info().total_cost.as_f64())
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    left.plan_info()
                        .startup_cost
                        .as_f64()
                        .partial_cmp(&right.plan_info().startup_cost.as_f64())
                        .unwrap_or(Ordering::Equal)
                })
        }
    })
}

fn limited_runtime_index_nested_loop(path: &Path) -> bool {
    matches!(
        path,
        Path::NestedLoopJoin {
            right,
            kind: JoinType::Inner | JoinType::Left,
            ..
        } if path_has_runtime_index_scan(right)
    )
}

fn path_has_runtime_index_scan(path: &Path) -> bool {
    match path {
        Path::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Path::IndexScan {
            keys,
            order_by_keys,
            ..
        } => keys.iter().chain(order_by_keys.iter()).any(|key| {
            matches!(
                key.argument,
                crate::include::nodes::plannodes::IndexScanKeyArgument::Runtime(_)
            )
        }),
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. }
        | Path::CteScan {
            cte_plan: input, ..
        } => path_has_runtime_index_scan(input),
        _ => false,
    }
}

fn prune_dominated_paths(paths: &mut Vec<Path>) {
    let mut pruned = Vec::with_capacity(paths.len());
    for path in std::mem::take(paths) {
        add_non_dominated_path(&mut pruned, path);
    }
    *paths = pruned;
}

fn add_non_dominated_path(paths: &mut Vec<Path>, candidate: Path) {
    if paths
        .iter()
        .any(|existing| path_dominates(existing, &candidate))
    {
        return;
    }
    paths.retain(|existing| !path_dominates(&candidate, existing));
    paths.push(candidate);
}

fn path_dominates(left: &Path, right: &Path) -> bool {
    if bestpath::preferred_parameterized_index_nested_loop(right)
        && !bestpath::preferred_parameterized_index_nested_loop(left)
    {
        return false;
    }
    if bestpath::preferred_parameterized_index_nested_loop(left)
        && !bestpath::preferred_parameterized_index_nested_loop(right)
    {
        return true;
    }
    if bestpath::preferred_parameterized_nested_loop(right)
        && !bestpath::preferred_parameterized_nested_loop(left)
    {
        return false;
    }
    if bestpath::preferred_parameterized_nested_loop(left)
        && !bestpath::preferred_parameterized_nested_loop(right)
    {
        return true;
    }
    if bestpath::preferred_function_outer_hash_join(right)
        && !bestpath::preferred_function_outer_hash_join(left)
    {
        return false;
    }
    if bestpath::preferred_function_outer_hash_join(left)
        && !bestpath::preferred_function_outer_hash_join(right)
    {
        return true;
    }
    if bestpath::preferred_small_full_merge_join(right, left) {
        return false;
    }
    if bestpath::preferred_small_full_merge_join(left, right) {
        return true;
    }
    if bestpath::preferred_small_nested_loop_left_join(right, left) {
        return false;
    }
    if bestpath::preferred_small_nested_loop_left_join(left, right) {
        return true;
    }
    if bestpath::preferred_unqualified_left_join_above_nulltest(right, left) {
        return false;
    }
    if bestpath::preferred_unqualified_left_join_above_nulltest(left, right) {
        return true;
    }
    if bestpath::non_nested_join_nearly_as_cheap(right, left) {
        return false;
    }
    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let left_startup = left_info.startup_cost.as_f64();
    let right_startup = right_info.startup_cost.as_f64();
    let left_total = left_info.total_cost.as_f64();
    let right_total = right_info.total_cost.as_f64();
    if left_startup > right_startup || left_total > right_total {
        return false;
    }

    let left_pathkeys = left.pathkeys();
    let right_pathkeys = right.pathkeys();
    if !bestpath::pathkeys_satisfy(&left_pathkeys, &right_pathkeys) {
        return false;
    }

    let strictly_cheaper = left_startup < right_startup || left_total < right_total;
    let strictly_better_pathkeys = !bestpath::pathkeys_satisfy(&right_pathkeys, &left_pathkeys);
    strictly_cheaper
        || strictly_better_pathkeys
        || !path_tie_breaker_prefers(right, left, &right_pathkeys, &left_pathkeys)
}

fn path_tie_breaker_prefers(
    left: &Path,
    right: &Path,
    left_pathkeys: &[PathKey],
    right_pathkeys: &[PathKey],
) -> bool {
    if let (Some(left_relid), Some(right_relid)) = (
        cross_function_join_left_relid(left),
        cross_function_join_left_relid(right),
    ) && left_relid != right_relid
    {
        return left_relid > right_relid;
    }
    if let (Some(left_relids), Some(right_relids)) = (
        cross_join_left_relid_count(left),
        cross_join_left_relid_count(right),
    ) && left_relids != right_relids
    {
        return left_relids > right_relids;
    }
    left_pathkeys.len() > right_pathkeys.len()
}

fn cross_function_join_left_relid(path: &Path) -> Option<usize> {
    match path {
        Path::NestedLoopJoin {
            left,
            right,
            kind: JoinType::Cross,
            ..
        } if path_is_function_scan_leaf(left) && path_is_function_scan_leaf(right) => {
            path_relids(left).first().copied()
        }
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => cross_function_join_left_relid(input),
        _ => None,
    }
}

fn path_is_function_scan_leaf(path: &Path) -> bool {
    match path {
        Path::FunctionScan { .. } => true,
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => path_is_function_scan_leaf(input),
        _ => false,
    }
}

fn cross_join_left_relid_count(path: &Path) -> Option<usize> {
    match path {
        Path::NestedLoopJoin {
            left,
            kind: JoinType::Cross,
            ..
        } => Some(path_relids(left).len()),
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => cross_join_left_relid_count(input),
        _ => None,
    }
}

fn query_columns_desc(columns: &[QueryColumn]) -> RelationDesc {
    RelationDesc {
        columns: columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .collect(),
    }
}

fn join_output_columns_for_child(left: &Path, right: &Path, kind: JoinType) -> Vec<QueryColumn> {
    let mut columns = left.columns();
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        columns.extend(right.columns());
    }
    columns
}

#[allow(clippy::too_many_arguments)]
fn collect_partitionwise_join_candidate_path(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
    reltarget: &PathTarget,
    output_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
) -> Option<Path> {
    if !root.config.enable_partitionwise_join {
        return None;
    }
    let left_whole_row = query_targets_whole_row_rel(root, &left_rel.relids);
    let right_whole_row = query_targets_whole_row_rel(root, &right_rel.relids);
    let left_agg_whole_row = query_accumulators_whole_row_rel(root, &left_rel.relids);
    let right_agg_whole_row = query_accumulators_whole_row_rel(root, &right_rel.relids);
    if left_whole_row || right_whole_row || left_agg_whole_row || right_agg_whole_row {
        return None;
    }
    if !has_partitionwise_equi_join(
        root,
        catalog,
        &left_rel.relids,
        &right_rel.relids,
        join_restrict_clauses,
    ) {
        return None;
    }
    let left_segments = partition_segments_for_rel(root, catalog, left_rel)?;
    let right_segments = partition_segments_for_rel(root, catalog, right_rel)?;
    let pairs = partition_join_segment_pairs(left_segments, right_segments, kind)?;
    let mut children = Vec::new();
    for (left_segment, right_segment) in pairs {
        let left_path = left_segment.path;
        let right_path = right_segment.path;
        let left_relids = path_relids(&left_path);
        let right_relids = path_relids(&right_path);
        let child_relids = relids_union(&left_relids, &right_relids);
        let child_restrict_clauses =
            translate_restrict_clauses_to_child(root, join_restrict_clauses, &child_relids);
        let left_child_rel = RelOptInfo::new(
            left_relids.clone(),
            RelOptKind::OtherMemberRel,
            left_path.semantic_output_target(),
        );
        let right_child_rel = RelOptInfo::new(
            right_relids.clone(),
            RelOptKind::OtherMemberRel,
            right_path.semantic_output_target(),
        );
        let child_reltarget =
            join_reltarget(root, &child_relids, &left_child_rel, &right_child_rel, kind);
        let child_output_columns = join_output_columns_for_child(&left_path, &right_path, kind);
        let child_paths = build_join_paths_with_root(
            root,
            catalog,
            left_path,
            right_path,
            &left_relids,
            &right_relids,
            kind,
            child_restrict_clauses,
            child_reltarget.clone(),
            child_output_columns.clone(),
        );
        let child_path = best_path(root, child_paths)?;
        let child_path = project_to_slot_layout(
            next_synthetic_slot_id(),
            &query_columns_desc(&child_output_columns),
            child_path,
            child_reltarget.clone(),
            catalog,
        );
        children.push(child_path);
    }
    let source_id = next_synthetic_slot_id();
    let desc = query_columns_desc(output_columns);
    let children_are_ordered = !root.query_pathkeys.is_empty()
        && children.iter().all(|child| !child.pathkeys().is_empty());
    let mut append_path = optimize_path_with_config(
        if children_are_ordered {
            Path::MergeAppend {
                plan_info: PlanEstimate::default(),
                pathtarget: reltarget.clone(),
                source_id,
                desc,
                items: pathkeys_to_order_items(&root.query_pathkeys),
                partition_prune: None,
                children,
            }
        } else {
            Path::Append {
                plan_info: PlanEstimate::default(),
                pathtarget: reltarget.clone(),
                pathkeys: Vec::new(),
                relids: relids_union(&left_rel.relids, &right_rel.relids),
                source_id,
                desc,
                child_roots: Vec::new(),
                partition_prune: None,
                children,
            }
        },
        catalog,
        root.config,
    );
    if let Path::Append { plan_info, .. } | Path::MergeAppend { plan_info, .. } = &mut append_path {
        // :HACK: Prefer a compatible partitionwise join shape over a cheaper
        // global join across parent Appends until partitionwise costing can
        // compare startup, pruning, and per-child join alternatives directly.
        *plan_info =
            PlanEstimate::new(0.0, 0.0, plan_info.plan_rows.as_f64(), plan_info.plan_width);
    }
    Some(append_path)
}

fn make_join_rel(
    root: &mut PlannerInfo,
    left_ref: LevelRelRef,
    right_ref: LevelRelRef,
    catalog: &dyn CatalogLookup,
) -> Option<()> {
    let (
        relids,
        reltarget,
        output_columns,
        join_restrict_clauses,
        mut candidate_paths,
        partitionwise_left_rel,
        partitionwise_right_rel,
        partitionwise_kind,
    ) = {
        let left_rel = rel_at_level(root, left_ref);
        let right_rel = rel_at_level(root, right_ref);
        if !relids_disjoint(&left_rel.relids, &right_rel.relids) {
            return None;
        }
        let relids = relids_union(&left_rel.relids, &right_rel.relids);
        let spec = join_is_legal(root, left_rel, right_rel)?;
        let (logical_left_rel, logical_right_rel) = if spec.reversed {
            (right_rel, left_rel)
        } else {
            (left_rel, right_rel)
        };
        let logical_kind = if spec.reversed {
            reverse_join_type(spec.kind)
        } else {
            spec.kind
        };
        let reltarget = join_reltarget(
            root,
            &relids,
            logical_left_rel,
            logical_right_rel,
            logical_kind,
        );
        let mut output_columns = logical_left_rel
            .cheapest_total_path()
            .map(Path::columns)
            .unwrap_or_default();
        if !matches!(logical_kind, JoinType::Semi | JoinType::Anti) {
            output_columns.extend(
                logical_right_rel
                    .cheapest_total_path()
                    .map(Path::columns)
                    .unwrap_or_default(),
            );
        }
        let join_restrict_clauses = build_join_restrict_clauses(
            root,
            logical_kind,
            spec.explicit_qual.clone(),
            &logical_left_rel.relids,
            &logical_right_rel.relids,
            &root.inner_join_clauses,
        );
        let path_kind =
            physical_join_kind_for_paths(root, logical_kind, &relids, &join_restrict_clauses);
        let mut join_restrict_clauses_for_rel = join_restrict_clauses.clone();
        let mut candidate_paths = collect_join_candidate_paths(
            root,
            catalog,
            logical_left_rel,
            logical_right_rel,
            path_kind,
            &join_restrict_clauses,
            &reltarget,
            &output_columns,
        );
        let mut partitionwise_left_rel = logical_left_rel.clone();
        let mut partitionwise_right_rel = logical_right_rel.clone();
        let mut partitionwise_kind = path_kind;
        if let Some(path) = collect_partitionwise_join_candidate_path(
            root,
            logical_left_rel,
            logical_right_rel,
            path_kind,
            &join_restrict_clauses,
            &reltarget,
            &output_columns,
            catalog,
        ) {
            candidate_paths.push(path);
        }
        if candidate_paths.is_empty() && spec.reversed {
            let fallback_join_restrict_clauses = build_join_restrict_clauses(
                root,
                spec.kind,
                spec.explicit_qual.clone(),
                &left_rel.relids,
                &right_rel.relids,
                &root.inner_join_clauses,
            );
            let fallback_path_kind = physical_join_kind_for_paths(
                root,
                spec.kind,
                &relids,
                &fallback_join_restrict_clauses,
            );
            candidate_paths = collect_join_candidate_paths(
                root,
                catalog,
                left_rel,
                right_rel,
                fallback_path_kind,
                &fallback_join_restrict_clauses,
                &reltarget,
                &output_columns,
            );
            if let Some(path) = collect_partitionwise_join_candidate_path(
                root,
                left_rel,
                right_rel,
                fallback_path_kind,
                &fallback_join_restrict_clauses,
                &reltarget,
                &output_columns,
                catalog,
            ) {
                candidate_paths.push(path);
            }
            if !candidate_paths.is_empty() {
                join_restrict_clauses_for_rel = fallback_join_restrict_clauses;
                partitionwise_left_rel = left_rel.clone();
                partitionwise_right_rel = right_rel.clone();
                partitionwise_kind = fallback_path_kind;
            }
        }
        (
            relids,
            reltarget,
            output_columns,
            join_restrict_clauses_for_rel,
            candidate_paths,
            partitionwise_left_rel,
            partitionwise_right_rel,
            partitionwise_kind,
        )
    };
    let partition_info_for_rel =
        if query_targets_whole_row_rel(root, &partitionwise_left_rel.relids)
            || query_targets_whole_row_rel(root, &partitionwise_right_rel.relids)
        {
            None
        } else {
            partitionwise::generate_partitionwise_join_path(
                root,
                &partitionwise_left_rel,
                &partitionwise_right_rel,
                partitionwise_kind,
                &join_restrict_clauses,
                &reltarget,
                &output_columns,
                catalog,
            )
            .and_then(|(path, partition_info)| {
                candidate_paths.push(prefer_partitionwise_path_cost(path, &candidate_paths));
                partition_info
            })
        };
    let join_rel_index = match find_join_rel_index(root, &relids) {
        Some(index) => index,
        None => {
            root.join_rel_list.push(RelOptInfo::new(
                relids.clone(),
                RelOptKind::JoinRel,
                reltarget.clone(),
            ));
            root.join_rel_list.len() - 1
        }
    };
    let join_rel = root
        .join_rel_list
        .get_mut(join_rel_index)
        .expect("join rel just inserted or found");
    if !join_rel.joininfo.iter().any(|info| {
        join_restrict_clauses
            .iter()
            .any(|clause| clause.clause == info.clause)
    }) {
        join_rel.joininfo.extend(join_restrict_clauses.clone());
    }
    for path in candidate_paths {
        join_rel.add_path(path);
    }
    prune_dominated_paths(&mut join_rel.pathlist);
    if let Some(partition_info) = partition_info_for_rel {
        join_rel.consider_partitionwise_join = true;
        join_rel.partition_info = Some(partition_info);
    }
    bestpath::set_cheapest(join_rel);
    Some(())
}

fn prefer_partitionwise_path_cost(path: Path, existing_paths: &[Path]) -> Path {
    let Some(best_existing_total) = existing_paths
        .iter()
        .map(|path| path.plan_info().total_cost.as_f64())
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
    else {
        return path;
    };
    if path.plan_info().total_cost.as_f64() < best_existing_total {
        return path;
    }
    match path {
        Path::Append {
            plan_info,
            pathtarget,
            pathkeys,
            relids,
            source_id,
            desc,
            child_roots,
            partition_prune,
            children,
        } => Path::Append {
            plan_info: PlanEstimate::new(
                plan_info.startup_cost.as_f64(),
                best_existing_total * 0.99,
                plan_info.plan_rows.as_f64(),
                plan_info.plan_width,
            ),
            pathtarget,
            pathkeys,
            relids,
            source_id,
            desc,
            child_roots,
            partition_prune,
            children,
        },
        Path::MergeAppend {
            plan_info,
            pathtarget,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => Path::MergeAppend {
            plan_info: PlanEstimate::new(
                plan_info.startup_cost.as_f64(),
                best_existing_total * 0.99,
                plan_info.plan_rows.as_f64(),
                plan_info.plan_width,
            ),
            pathtarget,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        },
        other => other,
    }
}

fn collect_join_candidate_paths(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
    reltarget: &PathTarget,
    output_columns: &[QueryColumn],
) -> Vec<Path> {
    let mut candidate_paths = Vec::new();
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            let paths = build_join_paths_with_root(
                root,
                catalog,
                left_path.clone(),
                right_path.clone(),
                &left_rel.relids,
                &right_rel.relids,
                kind,
                join_restrict_clauses.to_vec(),
                reltarget.clone(),
                output_columns.to_vec(),
            );
            for path in paths {
                candidate_paths.push(path);
            }
        }
    }
    candidate_paths
}

fn join_search_one_level(root: &mut PlannerInfo, level: usize, catalog: &dyn CatalogLookup) {
    for left_level in 1..level {
        let right_level = level - left_level;
        let left_refs = rel_refs_at_level(root, left_level);
        let right_refs = rel_refs_at_level(root, right_level);
        for left_ref in left_refs {
            for right_ref in &right_refs {
                if left_level == right_level
                    && rel_at_level(root, left_ref).relids >= rel_at_level(root, *right_ref).relids
                {
                    continue;
                }
                let _ = make_join_rel(root, left_ref, *right_ref, catalog);
            }
        }
    }
}

pub(super) fn make_one_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    assign_base_restrictinfo(root, catalog);
    expand_inherited_rtentries(root, catalog);
    simplify_base_restrictinfo(root);
    root.inner_join_clauses = collect_inner_join_clauses(root);
    set_base_rel_pathlists(root, catalog);
    let query_relids = root.all_query_relids();
    if query_relids.is_empty() {
        let mut rel = RelOptInfo::new(
            Vec::new(),
            RelOptKind::UpperRel,
            PathTarget::new(Vec::new()),
        );
        rel.add_path(optimize_path_with_config(
            Path::Result {
                plan_info: PlanEstimate::default(),
                pathtarget: PathTarget::new(Vec::new()),
            },
            catalog,
            root.config,
        ));
        bestpath::set_cheapest(&mut rel);
        return rel;
    }
    if query_relids.len() == 1 {
        return root.simple_rel_array[query_relids[0]]
            .clone()
            .expect("single base relation reloptinfo");
    }
    for level in 2..=query_relids.len() {
        join_search_one_level(root, level, catalog);
    }
    let rel = root
        .join_rel_list
        .iter()
        .find(|rel| rel.relids == query_relids)
        .cloned()
        .unwrap_or_else(|| panic!("failed to build join rel for relids {:?}", query_relids));
    rel
}

pub(super) fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    if root.parse.set_operation.is_some() {
        return build_set_operation_rel(root, catalog);
    }
    make_one_rel(root, catalog)
}
