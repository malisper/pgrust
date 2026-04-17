use std::cmp::Ordering;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::Query;
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerGlobal, PlannerInfo, RelOptInfo, RelOptKind, UpperRelKind,
};
use crate::include::nodes::plannodes::{PlanEstimate, PlannedStmt};
use crate::include::nodes::primnodes::{Expr, ProjectSetTarget, TargetEntry, WindowClause};

use super::super::bestpath;
use super::super::create_plan_with_param_base;
use super::super::has_grouping;
use super::super::path::{query_planner, residual_where_qual};
use super::super::pathnodes::{next_synthetic_slot_id, window_output_columns};
use super::super::root;
use super::super::upperrels;
use super::super::util::{
    annotate_targets_for_input, build_aggregate_output_columns, pathkeys_to_order_items,
    projection_is_identity, required_query_pathkeys_for_path, required_query_pathkeys_for_rel,
};
use super::super::{expand_join_rte_vars, optimize_path};

pub(super) fn make_pathtarget_projection_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    reltarget: &PathTarget,
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    let targets = root::build_projection_targets_for_pathtarget(reltarget);
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        reltarget.clone(),
    );
    for path in input_rel.pathlist {
        let targets = annotate_targets_for_input(Some(root), &path, &targets);
        if allow_identity_elision && projection_is_identity(&path, &targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets: targets.clone(),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_aggregate_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::GroupAgg,
        &input_rel.relids,
        root.grouped_target.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        root.grouped_target.clone(),
    );
    for path in input_rel.pathlist {
        let group_by = root
            .parse
            .group_by
            .iter()
            .cloned()
            .map(|expr| expand_join_rte_vars(root, expr))
            .collect::<Vec<_>>();
        let accumulators = root
            .parse
            .accumulators
            .iter()
            .cloned()
            .map(|mut accum| {
                accum.args = accum
                    .args
                    .into_iter()
                    .map(|arg| expand_join_rte_vars(root, arg))
                    .collect();
                accum.filter = accum
                    .filter
                    .map(|filter| expand_join_rte_vars(root, filter));
                accum
            })
            .collect::<Vec<_>>();
        let having = root
            .parse
            .having_qual
            .clone()
            .map(|expr| expand_join_rte_vars(root, expr));
        rel.add_path(optimize_path(
            Path::Aggregate {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                group_by: group_by.clone(),
                accumulators: accumulators.clone(),
                having,
                output_columns: build_aggregate_output_columns(&group_by, &accumulators),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_filter_rel(
    _root: &PlannerInfo,
    input_rel: RelOptInfo,
    predicate: Expr,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::Filter {
                plan_info: PlanEstimate::default(),
                input: Box::new(path),
                predicate: predicate.clone(),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn has_windowing(root: &PlannerInfo) -> bool {
    !root.parse.window_clauses.is_empty()
}

fn expand_window_clause(root: &PlannerInfo, clause: &WindowClause) -> WindowClause {
    WindowClause {
        spec: crate::include::nodes::primnodes::WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .iter()
                .cloned()
                .map(|expr| expand_join_rte_vars(root, expr))
                .collect(),
            order_by: clause
                .spec
                .order_by
                .iter()
                .cloned()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: expand_join_rte_vars(root, item.expr),
                    ..item
                })
                .collect(),
        },
        functions: clause
            .functions
            .iter()
            .cloned()
            .map(|func| crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            crate::include::nodes::primnodes::Aggref {
                                args: aggref
                                    .args
                                    .into_iter()
                                    .map(|arg| expand_join_rte_vars(root, arg))
                                    .collect(),
                                aggfilter: aggref
                                    .aggfilter
                                    .map(|expr| expand_join_rte_vars(root, expr)),
                                ..aggref
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: func
                    .args
                    .into_iter()
                    .map(|arg| expand_join_rte_vars(root, arg))
                    .collect(),
                ..func
            })
            .collect(),
    }
}

fn window_target(input_target: &PathTarget, clause: &WindowClause) -> PathTarget {
    let mut exprs = input_target.exprs.clone();
    let mut sortgrouprefs = input_target.sortgrouprefs.clone();
    for func in &clause.functions {
        exprs.push(Expr::WindowFunc(Box::new(func.clone())));
        sortgrouprefs.push(0);
    }
    PathTarget::with_sortgrouprefs(exprs, sortgrouprefs)
}

fn window_pathkeys(clause: &WindowClause) -> Vec<PathKey> {
    let mut pathkeys =
        Vec::with_capacity(clause.spec.partition_by.len() + clause.spec.order_by.len());
    pathkeys.extend(
        clause
            .spec
            .partition_by
            .iter()
            .cloned()
            .map(|expr| PathKey {
                expr,
                ressortgroupref: 0,
                descending: false,
                nulls_first: None,
            }),
    );
    pathkeys.extend(clause.spec.order_by.iter().cloned().map(|item| PathKey {
        expr: item.expr,
        ressortgroupref: item.ressortgroupref,
        descending: item.descending,
        nulls_first: item.nulls_first,
    }));
    pathkeys
}

fn make_window_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    clause: &WindowClause,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let clause = expand_window_clause(root, clause);
    let reltarget = window_target(&input_rel.reltarget, &clause);
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Window,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let required_pathkeys = window_pathkeys(&clause);
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_rel.pathlist {
        let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    input: Box::new(path),
                },
                catalog,
            )
        } else {
            path
        };
        rel.add_path(optimize_path(
            Path::WindowAgg {
                plan_info: PlanEstimate::default(),
                slot_id,
                output_columns: window_output_columns(&path, &clause),
                input: Box::new(path),
                clause: clause.clone(),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_project_set_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    reltarget: PathTarget,
    targets: &[ProjectSetTarget],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::ProjectSet,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::ProjectSet {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets: targets.to_vec(),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn project_set_targets_for_target_list(
    query: &Query,
    target_list: &[TargetEntry],
) -> Vec<ProjectSetTarget> {
    let Some(project_set) = query.project_set.as_ref() else {
        return target_list
            .iter()
            .cloned()
            .map(ProjectSetTarget::Scalar)
            .collect();
    };
    let base_width = root::project_set_base_width(project_set);
    target_list
        .iter()
        .cloned()
        .map(|target| {
            match target
                .input_resno
                .and_then(|input_resno| input_resno.checked_sub(1))
                .filter(|index| *index >= base_width)
            {
                Some(index) => project_set
                    .get(index)
                    .cloned()
                    .map(|project_target| match project_target {
                        ProjectSetTarget::Set {
                            call,
                            sql_type,
                            column_index,
                            ..
                        } => ProjectSetTarget::Set {
                            name: target.name.clone(),
                            call,
                            sql_type,
                            column_index,
                        },
                        ProjectSetTarget::Scalar(_) => ProjectSetTarget::Scalar(target.clone()),
                    })
                    .unwrap_or(ProjectSetTarget::Scalar(target)),
                None => ProjectSetTarget::Scalar(target),
            }
        })
        .collect()
}

fn query_has_postponed_srfs(root: &PlannerInfo) -> bool {
    let Some(project_set) = root.parse.project_set.as_ref() else {
        return false;
    };
    if root.parse.sort_clause.is_empty() {
        return false;
    }
    let base_width = root::project_set_base_width(project_set);
    !root.processed_tlist.iter().any(|target| {
        target.ressortgroupref != 0
            && root::target_references_project_set_output(target, base_width)
    })
}

fn adjust_paths_for_srfs(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    target_list: &[TargetEntry],
    reltarget: PathTarget,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let project_set_targets = project_set_targets_for_target_list(&root.parse, target_list);
    make_project_set_rel(root, input_rel, reltarget, &project_set_targets, catalog)
}

fn make_ordered_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Ordered,
        &input_rel.relids,
        input_rel.reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    let cheapest_presorted = input_rel
        .pathlist
        .iter()
        .filter(|path| {
            let required = required_query_pathkeys_for_path(root, path);
            bestpath::pathkeys_satisfy(&path.pathkeys(), &required)
        })
        .min_by(|left, right| {
            left.plan_info()
                .total_cost
                .as_f64()
                .partial_cmp(&right.plan_info().total_cost.as_f64())
                .unwrap_or(Ordering::Equal)
        });
    if let Some(path) = cheapest_presorted {
        rel.add_path(path.clone());
    }
    if let Some(path) = input_rel.cheapest_total_path() {
        let required_pathkeys = required_query_pathkeys_for_path(root, path);
        if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            rel.add_path(optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    input: Box::new(path.clone()),
                },
                catalog,
            ));
        } else if rel.pathlist.is_empty() {
            rel.add_path(path.clone());
        }
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_limit_rel(
    _root: &PlannerInfo,
    input_rel: RelOptInfo,
    limit: Option<usize>,
    offset: usize,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::Limit {
                plan_info: PlanEstimate::default(),
                input: Box::new(path),
                limit,
                offset,
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_projection_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    let reltarget = PathTarget::from_target_list(targets);
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Final,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_rel.pathlist {
        let targets = annotate_targets_for_input(Some(root), &path, targets);
        if allow_identity_elision && projection_is_identity(&path, &targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets,
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

pub(super) fn grouping_planner(
    root: &mut PlannerInfo,
    scanjoin_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut current_rel = scanjoin_rel;
    if let Some(predicate) = residual_where_qual(root) {
        current_rel = make_filter_rel(root, current_rel, predicate, catalog);
    }

    let has_grouping = has_grouping(root);
    if has_grouping && current_rel.relids.len() > 1 && current_rel.reltarget != root.scanjoin_target
    {
        current_rel = make_pathtarget_projection_rel(
            root,
            current_rel,
            &root.scanjoin_target,
            catalog,
            false,
        );
    }
    let mut projection_done = false;
    let final_targets = root.parse.target_list.clone();
    let processed_tlist = root.processed_tlist.clone();
    let processed_target = PathTarget::from_target_list(&processed_tlist);
    let postponed_srfs = query_has_postponed_srfs(root);
    if has_grouping {
        current_rel = make_aggregate_rel(root, current_rel, catalog);
    } else if root.parse.project_set.is_some() {
        if postponed_srfs {
            if current_rel.reltarget != root.sort_input_target {
                current_rel = make_pathtarget_projection_rel(
                    root,
                    current_rel,
                    &root.sort_input_target,
                    catalog,
                    false,
                );
            }
        } else {
            let project_set_target = if root.query_pathkeys.is_empty() {
                root.final_target.clone()
            } else {
                processed_target.clone()
            };
            let project_set_tlist = if root.query_pathkeys.is_empty() {
                final_targets.as_slice()
            } else {
                processed_tlist.as_slice()
            };
            current_rel = adjust_paths_for_srfs(
                root,
                current_rel,
                project_set_tlist,
                project_set_target,
                catalog,
            );
            projection_done = root.query_pathkeys.is_empty();
        }
    }

    if has_windowing(root) {
        if current_rel.reltarget != root.window_input_target {
            current_rel = make_pathtarget_projection_rel(
                root,
                current_rel,
                &root.window_input_target,
                catalog,
                false,
            );
        }
        for clause in root.parse.window_clauses.clone() {
            current_rel = make_window_rel(root, current_rel, &clause, catalog);
        }
    }

    if !root.query_pathkeys.is_empty() {
        current_rel = make_ordered_rel(root, current_rel, catalog);
    }

    if root.parse.project_set.is_some() && postponed_srfs {
        current_rel = adjust_paths_for_srfs(
            root,
            current_rel,
            &final_targets,
            root.final_target.clone(),
            catalog,
        );
        projection_done = true;
    }

    if root.parse.limit_count.is_some() || root.parse.limit_offset != 0 {
        current_rel = make_limit_rel(
            root,
            current_rel,
            root.parse.limit_count,
            root.parse.limit_offset,
            catalog,
        );
    }

    if has_grouping || has_windowing(root) {
        current_rel = make_projection_rel(root, current_rel, &final_targets, catalog, false);
    } else if !projection_done {
        current_rel = make_projection_rel(root, current_rel, &final_targets, catalog, true);
    }

    root.final_rel = Some(current_rel.clone());
    current_rel
}

fn standard_planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> (PlannedStmt, usize) {
    let mut glob = PlannerGlobal::new();
    let mut root = PlannerInfo::new(query);
    let command_type = root.parse.command_type;
    let scanjoin_rel = query_planner(&mut root, catalog);
    let final_rel = grouping_planner(&mut root, scanjoin_rel, catalog);
    let required_pathkeys = required_query_pathkeys_for_rel(&root, &final_rel);
    let best_path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
        .cloned()
        .unwrap_or(Path::Result {
            plan_info: PlanEstimate::default(),
        });
    let (plan_tree, ext_params, next_param_id) =
        create_plan_with_param_base(&root, best_path, catalog, &mut glob.subplans, next_param_id);
    (
        PlannedStmt {
            command_type,
            plan_tree,
            subplans: glob.subplans,
            ext_params,
        },
        next_param_id,
    )
}

fn standard_planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    standard_planner_with_param_base(query, catalog, 0).0
}

pub(crate) fn planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    standard_planner(query, catalog)
}

pub(crate) fn planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> (PlannedStmt, usize) {
    standard_planner_with_param_base(query, catalog, next_param_id)
}
