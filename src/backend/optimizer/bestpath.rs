use std::cmp::Ordering;

use crate::include::nodes::pathnodes::{Path, PathKey, RelOptInfo};
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
    if let (Some(candidate_pathkeys), Some(current_pathkeys)) =
        (join_pathkey_count(candidate), join_pathkey_count(current))
        && candidate_pathkeys != current_pathkeys
    {
        return candidate_pathkeys > current_pathkeys;
    }
    let cmp = compare_path_costs(candidate, current, cost);
    cmp == Ordering::Less
        || (cmp == Ordering::Equal && better_pathkeys(&candidate.pathkeys(), &current.pathkeys()))
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
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => cross_join_left_relid_count(input),
        _ => None,
    }
}

fn join_pathkey_count(path: &Path) -> Option<usize> {
    match path {
        Path::NestedLoopJoin { .. } | Path::HashJoin { .. } | Path::MergeJoin { .. } => {
            Some(path.pathkeys().len())
        }
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => join_pathkey_count(input),
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
