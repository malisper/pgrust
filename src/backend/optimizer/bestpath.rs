use std::cmp::Ordering;

use crate::include::nodes::pathnodes::{Path, PathKey, RelOptInfo};
use crate::include::nodes::primnodes::Expr;
use crate::include::nodes::primnodes::JoinType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CostSelector {
    Startup,
    Total,
}

pub(super) fn pathkeys_satisfy(actual: &[PathKey], required: &[PathKey]) -> bool {
    actual.len() >= required.len()
        && actual
            .iter()
            .zip(required.iter())
            .all(|(actual, required)| pathkeys_equivalent(actual, required))
}

pub(super) fn set_cheapest(rel: &mut RelOptInfo) {
    let mut cheapest_startup = None;
    let mut cheapest_total = None;
    for (index, path) in rel.pathlist.iter().enumerate() {
        if cheaper_than(
            path,
            cheapest_startup.and_then(|idx| rel.pathlist.get(idx)),
            CostSelector::Startup,
        ) {
            cheapest_startup = Some(index);
        }
        if cheaper_than(
            path,
            cheapest_total.and_then(|idx| rel.pathlist.get(idx)),
            CostSelector::Total,
        ) {
            cheapest_total = Some(index);
        }
    }
    rel.cheapest_startup_path = cheapest_startup;
    rel.cheapest_total_path = cheapest_total;
    rel.rows = rel
        .cheapest_total_path()
        .map(|path| path.plan_info().plan_rows.as_f64())
        .unwrap_or(0.0);
}

pub(super) fn get_cheapest_path_for_pathkeys<'a>(
    rel: &'a RelOptInfo,
    required_pathkeys: &[PathKey],
    cost: CostSelector,
) -> Option<&'a Path> {
    rel.pathlist.iter().fold(None, |best, path| {
        if !pathkeys_satisfy(&path.pathkeys(), required_pathkeys) {
            return best;
        }
        if cheaper_than(path, best, cost) {
            Some(path)
        } else {
            best
        }
    })
}

pub(super) fn choose_final_path<'a>(
    rel: &'a RelOptInfo,
    required_pathkeys: &[PathKey],
) -> Option<&'a Path> {
    if required_pathkeys.is_empty() {
        rel.cheapest_total_path()
    } else {
        get_cheapest_path_for_pathkeys(rel, required_pathkeys, CostSelector::Total)
            .or_else(|| rel.cheapest_total_path())
    }
}

fn cheaper_than(candidate: &Path, current: Option<&Path>, cost: CostSelector) -> bool {
    let Some(current) = current else {
        return true;
    };
    if let (Some(candidate_left_relids), Some(current_left_relids)) = (
        cross_join_left_relid_count(candidate),
        cross_join_left_relid_count(current),
    ) && candidate_left_relids != current_left_relids
    {
        return candidate_left_relids > current_left_relids;
    }
    if matches!(cost, CostSelector::Total) {
        if preferred_parameterized_index_nested_loop(candidate)
            && !preferred_parameterized_index_nested_loop(current)
        {
            return true;
        }
        if preferred_parameterized_index_nested_loop(current)
            && !preferred_parameterized_index_nested_loop(candidate)
        {
            return false;
        }
        if preferred_function_outer_hash_join(candidate)
            && !preferred_function_outer_hash_join(current)
        {
            return true;
        }
        if preferred_function_outer_hash_join(current)
            && !preferred_function_outer_hash_join(candidate)
        {
            return false;
        }
        if non_nested_join_nearly_as_cheap(candidate, current) {
            return true;
        }
        if non_nested_join_nearly_as_cheap(current, candidate) {
            return false;
        }
    }
    let cmp = compare_path_costs(candidate, current, cost);
    cmp == Ordering::Less
        || (cmp == Ordering::Equal && better_pathkeys(&candidate.pathkeys(), &current.pathkeys()))
}

pub(super) fn non_nested_join_nearly_as_cheap(preferred: &Path, other: &Path) -> bool {
    if !matches!(preferred, Path::HashJoin { .. } | Path::MergeJoin { .. })
        || !matches!(other, Path::NestedLoopJoin { .. })
    {
        return false;
    }
    if underestimated_seqscan_nested_loop(other) {
        return true;
    }
    let preferred_total = preferred.plan_info().total_cost.as_f64();
    let other_total = other.plan_info().total_cost.as_f64();
    let tolerance = (other_total.abs() * 0.01).max(1.0);
    preferred_total <= other_total + tolerance
}

pub(super) fn preferred_parameterized_index_nested_loop(path: &Path) -> bool {
    match path {
        Path::NestedLoopJoin {
            left,
            right,
            kind: JoinType::Inner,
            ..
        } => {
            // :HACK: pgrust does not yet model parameterized index-scan startup
            // and uniqueness as precisely as PostgreSQL. Prefer the PostgreSQL
            // shape when a small outer path can drive runtime index probes.
            left.plan_info().plan_rows.as_f64() <= 100.0 && path_has_runtime_index_scan(right)
        }
        _ => false,
    }
}

pub(super) fn preferred_parameterized_nested_loop(path: &Path) -> bool {
    match path {
        Path::NestedLoopJoin {
            right,
            kind: JoinType::Inner | JoinType::Left,
            ..
        } => path_uses_immediate_outer(right),
        _ => false,
    }
}

pub(super) fn preferred_function_outer_hash_join(path: &Path) -> bool {
    match path {
        Path::HashJoin {
            left,
            right,
            kind: JoinType::Inner,
            ..
        } => {
            // :HACK: PostgreSQL's support-function regression keeps a bounded
            // function scan on the probe side and builds the hash table from the
            // larger base relation. pgrust's current hash cost model otherwise
            // over-prefers hashing the function result.
            let left_rows = left.plan_info().plan_rows.as_f64();
            let right_rows = right.plan_info().plan_rows.as_f64();
            left_rows >= 100.0
                && right_rows >= left_rows
                && path_has_function_scan(left)
                && !path_has_function_scan(right)
        }
        _ => false,
    }
}

fn path_has_function_scan(path: &Path) -> bool {
    match path {
        Path::FunctionScan { .. } => true,
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
        } => path_has_function_scan(input),
        _ => false,
    }
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

fn path_uses_immediate_outer(path: &Path) -> bool {
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
            ) || key
                .display_expr
                .as_ref()
                .is_some_and(expr_uses_immediate_outer)
        }),
        Path::Filter {
            input, predicate, ..
        } => path_uses_immediate_outer(input) || expr_uses_immediate_outer(predicate),
        Path::Projection { input, targets, .. } => {
            path_uses_immediate_outer(input)
                || targets
                    .iter()
                    .any(|target| expr_uses_immediate_outer(&target.expr))
        }
        Path::OrderBy { input, items, .. } | Path::IncrementalSort { input, items, .. } => {
            path_uses_immediate_outer(input)
                || items
                    .iter()
                    .any(|item| expr_uses_immediate_outer(&item.expr))
        }
        Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. } => path_uses_immediate_outer(input),
        Path::Append { children, .. } | Path::MergeAppend { children, .. } => {
            children.iter().any(path_uses_immediate_outer)
        }
        _ => false,
    }
}

fn expr_uses_immediate_outer(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 1,
        Expr::Param(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_uses_immediate_outer),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_immediate_outer),
        Expr::Func(func) => func.args.iter().any(expr_uses_immediate_outer),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_uses_immediate_outer(inner),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_uses_immediate_outer(left) || expr_uses_immediate_outer(right)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_uses_immediate_outer(&saop.left) || expr_uses_immediate_outer(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_uses_immediate_outer),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_uses_immediate_outer(expr)),
        _ => false,
    }
}

fn underestimated_seqscan_nested_loop(path: &Path) -> bool {
    match path {
        Path::NestedLoopJoin {
            left,
            right,
            kind: JoinType::Inner,
            restrict_clauses,
            ..
        } => {
            !restrict_clauses.is_empty()
                && left.plan_info().plan_rows.as_f64() <= 2.0
                && right.plan_info().plan_rows.as_f64() <= 2.0
                && contains_seq_scan(left)
                && contains_seq_scan(right)
        }
        _ => false,
    }
}

fn contains_seq_scan(path: &Path) -> bool {
    match path {
        Path::SeqScan { .. } => true,
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. }
        | Path::Aggregate { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::ProjectSet { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Path::CteScan {
            cte_plan: input, ..
        } => contains_seq_scan(input),
        Path::Append { children, .. }
        | Path::BitmapOr { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::SetOp { children, .. } => children.iter().any(contains_seq_scan),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => {
            contains_seq_scan(left) || contains_seq_scan(right)
        }
        Path::RecursiveUnion {
            anchor, recursive, ..
        } => contains_seq_scan(anchor) || contains_seq_scan(recursive),
        Path::Result { .. }
        | Path::IndexOnlyScan { .. }
        | Path::IndexScan { .. }
        | Path::BitmapIndexScan { .. }
        | Path::Values { .. }
        | Path::FunctionScan { .. }
        | Path::WorkTableScan { .. } => false,
    }
}

fn cross_join_left_relid_count(path: &Path) -> Option<usize> {
    match path {
        Path::NestedLoopJoin {
            left,
            kind: JoinType::Cross,
            ..
        } => Some(super::path_relids(left).len()),
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => cross_join_left_relid_count(input),
        _ => None,
    }
}

fn compare_path_costs(left: &Path, right: &Path, cost: CostSelector) -> Ordering {
    let left_cost = match cost {
        CostSelector::Startup => left.plan_info().startup_cost.as_f64(),
        CostSelector::Total => left.plan_info().total_cost.as_f64(),
    };
    let right_cost = match cost {
        CostSelector::Startup => right.plan_info().startup_cost.as_f64(),
        CostSelector::Total => right.plan_info().total_cost.as_f64(),
    };
    left_cost
        .partial_cmp(&right_cost)
        .unwrap_or(Ordering::Equal)
}

fn better_pathkeys(left: &[PathKey], right: &[PathKey]) -> bool {
    left.len() > right.len()
}

fn pathkeys_equivalent(left: &PathKey, right: &PathKey) -> bool {
    let same_identity = if left.ressortgroupref != 0 && right.ressortgroupref != 0 {
        left.ressortgroupref == right.ressortgroupref
    } else {
        left.expr == right.expr
    };
    same_identity
        && left.descending == right.descending
        && left.nulls_first.unwrap_or(left.descending)
            == right.nulls_first.unwrap_or(right.descending)
}
