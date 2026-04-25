use std::cmp::Ordering;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::Query;
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerConfig, PlannerGlobal, PlannerInfo, RelOptInfo, RelOptKind,
    UpperRelKind,
};
use crate::include::nodes::plannodes::{AggregateStrategy, PlanEstimate, PlannedStmt};
use crate::include::nodes::primnodes::{
    Expr, ProjectSetTarget, TargetEntry, WindowClause, expr_contains_set_returning,
    set_returning_call_exprs,
};

use super::super::bestpath;
use super::super::create_plan_with_param_base;
use super::super::groupby_rewrite;
use super::super::has_grouping;
use super::super::path::{query_planner, relation_ordered_index_paths, residual_where_qual};
use super::super::pathnodes::{next_synthetic_slot_id, window_output_columns};
use super::super::root;
use super::super::upperrels;
use super::super::util::{
    annotate_targets_for_input, build_aggregate_output_columns, pathkeys_are_fully_identified,
    pathkeys_to_order_items, projection_is_identity, required_query_pathkeys_for_path,
    required_query_pathkeys_for_rel,
};
use super::super::{expand_join_rte_vars, optimize_path, pull_up_sublinks};

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
                pathtarget: PathTarget::from_target_list(&targets),
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

fn group_pathkeys(group_by: &[Expr]) -> Vec<PathKey> {
    group_by
        .iter()
        .cloned()
        .map(|expr| PathKey {
            expr,
            ressortgroupref: 0,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        })
        .collect()
}

fn accumulators_require_sorted_grouping(
    accumulators: &[crate::include::nodes::primnodes::AggAccum],
) -> bool {
    accumulators
        .iter()
        .any(|accum| accum.distinct || !accum.order_by.is_empty())
}

fn ordered_group_input(path: Path, group_pathkeys: &[PathKey]) -> Path {
    if bestpath::pathkeys_satisfy(&path.pathkeys(), group_pathkeys) {
        path
    } else {
        Path::OrderBy {
            plan_info: PlanEstimate::default(),
            pathtarget: path.semantic_output_target(),
            items: pathkeys_to_order_items(group_pathkeys),
            input: Box::new(path),
        }
    }
}

fn aggregate_path(
    strategy: AggregateStrategy,
    pathkeys: Vec<PathKey>,
    slot_id: usize,
    input: Path,
    group_by: Vec<Expr>,
    passthrough_exprs: Vec<Expr>,
    accumulators: Vec<crate::include::nodes::primnodes::AggAccum>,
    having: Option<Expr>,
    output_columns: Vec<crate::include::nodes::primnodes::QueryColumn>,
    reltarget: PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    optimize_path(
        Path::Aggregate {
            plan_info: PlanEstimate::default(),
            pathtarget: reltarget,
            slot_id,
            strategy,
            pathkeys,
            input: Box::new(input),
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            output_columns,
        },
        catalog,
    )
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
            .aggregate_group_by()
            .iter()
            .cloned()
            .map(|expr| expand_join_rte_vars(root, expr))
            .collect::<Vec<_>>();
        let passthrough_exprs = root
            .aggregate_passthrough_exprs()
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
        let output_columns =
            build_aggregate_output_columns(&group_by, &passthrough_exprs, &accumulators);
        if group_by.is_empty() {
            rel.add_path(aggregate_path(
                AggregateStrategy::Plain,
                Vec::new(),
                slot_id,
                path,
                group_by,
                passthrough_exprs,
                accumulators,
                having,
                output_columns,
                root.grouped_target.clone(),
                catalog,
            ));
            continue;
        }

        let group_pathkeys = group_pathkeys(&group_by);
        if accumulators_require_sorted_grouping(&accumulators) {
            rel.add_path(aggregate_path(
                AggregateStrategy::Sorted,
                group_pathkeys.clone(),
                slot_id,
                ordered_group_input(path, &group_pathkeys),
                group_by,
                passthrough_exprs,
                accumulators,
                having,
                output_columns,
                root.grouped_target.clone(),
                catalog,
            ));
        } else {
            let path_satisfies_group_order =
                bestpath::pathkeys_satisfy(&path.pathkeys(), &group_pathkeys);
            rel.add_path(aggregate_path(
                AggregateStrategy::Hashed,
                Vec::new(),
                slot_id,
                path.clone(),
                group_by.clone(),
                passthrough_exprs.clone(),
                accumulators.clone(),
                having.clone(),
                output_columns.clone(),
                root.grouped_target.clone(),
                catalog,
            ));
            if path_satisfies_group_order {
                rel.add_path(aggregate_path(
                    AggregateStrategy::Sorted,
                    group_pathkeys,
                    slot_id,
                    path,
                    group_by,
                    passthrough_exprs,
                    accumulators,
                    having,
                    output_columns,
                    root.grouped_target.clone(),
                    catalog,
                ));
            }
        }
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
                pathtarget: path.semantic_output_target(),
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
    let expand_frame_bound = |bound: crate::include::nodes::primnodes::WindowFrameBound| match bound
    {
        crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
            let expr = expand_join_rte_vars(root, offset.expr.clone());
            crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(
                offset.with_expr(expr),
            )
        }
        crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
            let expr = expand_join_rte_vars(root, offset.expr.clone());
            crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(
                offset.with_expr(expr),
            )
        }
        other => other,
    };
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
            frame: crate::include::nodes::primnodes::WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: expand_frame_bound(clause.spec.frame.start_bound.clone()),
                end_bound: expand_frame_bound(clause.spec.frame.end_bound.clone()),
            },
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
                                aggorder: aggref
                                    .aggorder
                                    .into_iter()
                                    .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                                        expr: expand_join_rte_vars(root, item.expr),
                                        ..item
                                    })
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
                collation_oid: None,
            }),
    );
    pathkeys.extend(clause.spec.order_by.iter().cloned().map(|item| PathKey {
        expr: item.expr,
        ressortgroupref: item.ressortgroupref,
        descending: item.descending,
        nulls_first: item.nulls_first,
        collation_oid: item.collation_oid,
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
    let mut ordered_input_paths = Vec::new();
    if !has_grouping(root)
        && !root.parse.has_target_srfs
        && input_rel.reltarget == root.window_input_target
        && !input_rel
            .pathlist
            .iter()
            .any(|path| bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys))
        && let [rtindex] = input_rel.relids.as_slice()
    {
        let ordered_paths =
            relation_ordered_index_paths(root, *rtindex, &required_pathkeys, catalog);
        if !ordered_paths.is_empty() {
            let base_target = root
                .simple_rel_array
                .get(*rtindex)
                .and_then(Option::as_ref)
                .map(|base_rel| base_rel.reltarget.clone())
                .unwrap_or_else(|| input_rel.reltarget.clone());
            let mut ordered_rel = RelOptInfo::new(vec![*rtindex], RelOptKind::BaseRel, base_target);
            ordered_rel.pathlist = ordered_paths;
            bestpath::set_cheapest(&mut ordered_rel);
            let ordered_rel = if ordered_rel.reltarget != input_rel.reltarget {
                make_pathtarget_projection_rel(
                    root,
                    ordered_rel,
                    &input_rel.reltarget,
                    catalog,
                    false,
                )
            } else {
                ordered_rel
            };
            ordered_input_paths.extend(
                ordered_rel.pathlist.into_iter().filter(|path| {
                    bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys)
                }),
            );
        }
    }
    for path in input_rel
        .pathlist
        .into_iter()
        .chain(ordered_input_paths.into_iter())
    {
        let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
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
                pathtarget: rel.reltarget.clone(),
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
                pathtarget: rel.reltarget.clone(),
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

fn query_has_postponed_srfs(root: &PlannerInfo) -> bool {
    if !root.parse.has_target_srfs || root.parse.sort_clause.is_empty() {
        return false;
    }
    !root
        .processed_tlist
        .iter()
        .any(|target| target.ressortgroupref != 0 && expr_contains_set_returning(&target.expr))
}

fn project_set_pathtarget_for_targets(targets: &[ProjectSetTarget]) -> PathTarget {
    PathTarget::with_sortgrouprefs(
        targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.expr.clone(),
                ProjectSetTarget::Set { source_expr, .. } => source_expr.clone(),
            })
            .collect(),
        targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.ressortgroupref,
                ProjectSetTarget::Set { .. } => 0,
            })
            .collect(),
    )
}

fn expr_srf_depth(expr: &Expr) -> usize {
    match expr {
        Expr::SetReturning(srf) => {
            1 + set_returning_call_exprs(&srf.call)
                .into_iter()
                .map(expr_srf_depth)
                .max()
                .unwrap_or(0)
        }
        Expr::Aggref(aggref) => aggref
            .args
            .iter()
            .map(expr_srf_depth)
            .chain(
                aggref
                    .aggorder
                    .iter()
                    .map(|entry| expr_srf_depth(&entry.expr)),
            )
            .chain(aggref.aggfilter.as_ref().map(expr_srf_depth))
            .max()
            .unwrap_or(0),
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .map(expr_srf_depth)
            .max()
            .unwrap_or(0),
        Expr::Op(op) => op.args.iter().map(expr_srf_depth).max().unwrap_or(0),
        Expr::Bool(bool_expr) => bool_expr.args.iter().map(expr_srf_depth).max().unwrap_or(0),
        Expr::Case(case_expr) => case_expr
            .arg
            .as_deref()
            .map(expr_srf_depth)
            .into_iter()
            .chain(
                case_expr
                    .args
                    .iter()
                    .flat_map(|arm| [expr_srf_depth(&arm.expr), expr_srf_depth(&arm.result)]),
            )
            .chain(std::iter::once(expr_srf_depth(&case_expr.defresult)))
            .max()
            .unwrap_or(0),
        Expr::Func(func) => func.args.iter().map(expr_srf_depth).max().unwrap_or(0),
        Expr::SubLink(sublink) => sublink.testexpr.as_deref().map(expr_srf_depth).unwrap_or(0),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .map(expr_srf_depth)
            .into_iter()
            .chain(subplan.args.iter().map(expr_srf_depth))
            .max()
            .unwrap_or(0),
        Expr::ScalarArrayOp(saop) => expr_srf_depth(&saop.left).max(expr_srf_depth(&saop.right)),
        Expr::Xml(xml) => xml.child_exprs().map(expr_srf_depth).max().unwrap_or(0),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_srf_depth(inner),
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
        } => [expr_srf_depth(expr), expr_srf_depth(pattern)]
            .into_iter()
            .chain(escape.as_deref().map(expr_srf_depth))
            .max()
            .unwrap_or(0),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => expr_srf_depth(left).max(expr_srf_depth(right)),
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().map(expr_srf_depth).max().unwrap_or(0)
        }
        Expr::Row { fields, .. } => fields
            .iter()
            .map(|(_, expr)| expr_srf_depth(expr))
            .max()
            .unwrap_or(0),
        Expr::ArraySubscript { array, subscripts } => std::iter::once(expr_srf_depth(array))
            .chain(subscripts.iter().flat_map(|subscript| {
                subscript
                    .lower
                    .as_ref()
                    .map(expr_srf_depth)
                    .into_iter()
                    .chain(subscript.upper.as_ref().map(expr_srf_depth))
            }))
            .max()
            .unwrap_or(0),
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => 0,
    }
}

fn collect_srfs_at_depth(expr: &Expr, depth: usize, out: &mut Vec<Expr>) {
    if let Expr::SetReturning(srf) = expr {
        if expr_srf_depth(expr) == depth && !out.iter().any(|candidate| candidate == expr) {
            out.push(expr.clone());
        }
        for arg in set_returning_call_exprs(&srf.call) {
            collect_srfs_at_depth(arg, depth, out);
        }
        return;
    }
    match expr {
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_srfs_at_depth(arg, depth, out);
            }
            for entry in &aggref.aggorder {
                collect_srfs_at_depth(&entry.expr, depth, out);
            }
            if let Some(filter) = &aggref.aggfilter {
                collect_srfs_at_depth(filter, depth, out);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_srfs_at_depth(arg, depth, out);
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_srfs_at_depth(arg, depth, out);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_srfs_at_depth(arg, depth, out);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_srfs_at_depth(arg, depth, out);
            }
            for arm in &case_expr.args {
                collect_srfs_at_depth(&arm.expr, depth, out);
                collect_srfs_at_depth(&arm.result, depth, out);
            }
            collect_srfs_at_depth(&case_expr.defresult, depth, out);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_srfs_at_depth(arg, depth, out);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_srfs_at_depth(testexpr, depth, out);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_srfs_at_depth(testexpr, depth, out);
            }
            for arg in &subplan.args {
                collect_srfs_at_depth(arg, depth, out);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_srfs_at_depth(&saop.left, depth, out);
            collect_srfs_at_depth(&saop.right, depth, out);
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_srfs_at_depth(child, depth, out);
            }
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => collect_srfs_at_depth(inner, depth, out),
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
            collect_srfs_at_depth(expr, depth, out);
            collect_srfs_at_depth(pattern, depth, out);
            if let Some(escape) = escape {
                collect_srfs_at_depth(escape, depth, out);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_srfs_at_depth(left, depth, out);
            collect_srfs_at_depth(right, depth, out);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_srfs_at_depth(element, depth, out);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_srfs_at_depth(expr, depth, out);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_srfs_at_depth(array, depth, out);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_srfs_at_depth(lower, depth, out);
                }
                if let Some(upper) = &subscript.upper {
                    collect_srfs_at_depth(upper, depth, out);
                }
            }
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::SetReturning(_) => unreachable!("handled before recursive match"),
    }
}

fn project_set_targets_for_srf_level(
    input_target: &PathTarget,
    target_list: &[TargetEntry],
    level: usize,
) -> Vec<ProjectSetTarget> {
    let mut targets = root::build_projection_targets_for_pathtarget(input_target)
        .into_iter()
        .map(ProjectSetTarget::Scalar)
        .collect::<Vec<_>>();
    for target in target_list {
        if !expr_contains_set_returning(&target.expr)
            && !targets.iter().any(|candidate| {
                matches!(candidate, ProjectSetTarget::Scalar(entry) if entry.expr == target.expr)
            })
        {
            targets.push(ProjectSetTarget::Scalar(target.clone()));
        }
    }
    let mut srfs = Vec::new();
    for target in target_list {
        collect_srfs_at_depth(&target.expr, level, &mut srfs);
    }
    for expr in srfs {
        let Expr::SetReturning(srf) = expr.clone() else {
            unreachable!("SRF collector only returns Expr::SetReturning")
        };
        targets.push(ProjectSetTarget::Set {
            name: srf.name.clone(),
            source_expr: expr,
            call: srf.call.clone(),
            sql_type: srf.sql_type,
            column_index: srf.column_index,
        });
    }
    targets
}

fn adjust_paths_for_target_srfs(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    target_list: &[TargetEntry],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let max_depth = target_list
        .iter()
        .map(|target| expr_srf_depth(&target.expr))
        .max()
        .unwrap_or(0);
    let mut current_rel = input_rel;
    for level in 1..=max_depth {
        let targets = project_set_targets_for_srf_level(&current_rel.reltarget, target_list, level);
        if targets
            .iter()
            .any(|target| matches!(target, ProjectSetTarget::Set { .. }))
        {
            let reltarget = project_set_pathtarget_for_targets(&targets);
            current_rel = make_project_set_rel(root, current_rel, reltarget, &targets, catalog);
        }
    }
    current_rel
}

fn adjust_paths_for_srfs(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    target_list: &[TargetEntry],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    adjust_paths_for_target_srfs(root, input_rel, target_list, catalog)
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
    let required_pathkeys = required_query_pathkeys_for_rel(root, &input_rel);
    let mut extra_presorted_paths = Vec::new();
    if (root.parse.limit_count.is_some() || root.parse.limit_offset != 0)
        && let [rtindex] = input_rel.relids.as_slice()
    {
        extra_presorted_paths =
            relation_ordered_index_paths(root, *rtindex, &required_pathkeys, catalog);
    }
    let required_matches = |path: &Path| {
        if pathkeys_are_fully_identified(&root.query_pathkeys) {
            bestpath::pathkeys_satisfy(&path.pathkeys(), &root.query_pathkeys)
        } else {
            let required = required_query_pathkeys_for_path(root, path);
            bestpath::pathkeys_satisfy(&path.pathkeys(), &required)
        }
    };
    let cheapest_presorted = input_rel
        .pathlist
        .iter()
        .chain(extra_presorted_paths.iter())
        .filter(|path| required_matches(path))
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
    if root.parse.limit_count.is_some() || root.parse.limit_offset != 0 {
        let cheapest_presorted_startup = input_rel
            .pathlist
            .iter()
            .chain(extra_presorted_paths.iter())
            .filter(|path| required_matches(path))
            .min_by(|left, right| {
                left.plan_info()
                    .startup_cost
                    .as_f64()
                    .partial_cmp(&right.plan_info().startup_cost.as_f64())
                    .unwrap_or(Ordering::Equal)
            });
        if let Some(path) = cheapest_presorted_startup
            && !rel.pathlist.iter().any(|existing| existing == path)
        {
            rel.add_path(path.clone());
        }
    }
    if let Some(path) = input_rel.cheapest_total_path() {
        let required_pathkeys = required_query_pathkeys_for_path(root, path);
        if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            rel.add_path(optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
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

fn distinct_pathkeys(targets: &[TargetEntry]) -> Vec<PathKey> {
    targets
        .iter()
        .map(|target| PathKey {
            expr: target.expr.clone(),
            ressortgroupref: target.ressortgroupref,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        })
        .collect()
}

fn make_distinct_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let reltarget = PathTarget::from_target_list(targets);
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Distinct,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }

    let required_pathkeys = distinct_pathkeys(targets);
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_rel.pathlist {
        let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    input: Box::new(path),
                },
                catalog,
            )
        } else {
            path
        };
        rel.add_path(optimize_path(
            Path::Unique {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                input: Box::new(path),
            },
            catalog,
        ));
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
                pathtarget: path.semantic_output_target(),
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

fn make_lock_rows_rel(
    input_rel: RelOptInfo,
    row_marks: &[crate::include::nodes::parsenodes::QueryRowMark],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::LockRows {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                input: Box::new(path),
                row_marks: row_marks.to_vec(),
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
                pathtarget: PathTarget::from_target_list(&targets),
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
    let has_target_srfs = root.parse.has_target_srfs;
    let postponed_srfs = query_has_postponed_srfs(root);
    if has_grouping {
        current_rel = make_aggregate_rel(root, current_rel, catalog);
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

    if has_target_srfs && !postponed_srfs {
        let project_set_tlist = if root.query_pathkeys.is_empty() {
            final_targets.as_slice()
        } else {
            processed_tlist.as_slice()
        };
        current_rel = adjust_paths_for_srfs(root, current_rel, project_set_tlist, catalog);
        projection_done = current_rel.reltarget == root.final_target;
    }

    if root.parse.distinct {
        if current_rel.reltarget != root.final_target {
            current_rel = make_projection_rel(root, current_rel, &final_targets, catalog, false);
        }
        current_rel = make_distinct_rel(root, current_rel, &final_targets, catalog);
        projection_done = current_rel.reltarget == root.final_target;
    }

    if !root.query_pathkeys.is_empty() {
        current_rel = make_ordered_rel(root, current_rel, catalog);
    }

    if has_target_srfs && postponed_srfs {
        current_rel = adjust_paths_for_srfs(root, current_rel, &final_targets, catalog);
        projection_done = current_rel.reltarget == root.final_target;
    }

    if !root.parse.row_marks.is_empty() {
        current_rel = make_lock_rows_rel(current_rel, &root.parse.row_marks, catalog);
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
    config: PlannerConfig,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    let mut glob = PlannerGlobal::new();
    let query = root::prepare_query_for_planning(root::prepare_query_for_locking(query)?, catalog);
    let query = pull_up_sublinks(query);
    let aggregate_layout = groupby_rewrite::build_aggregate_layout(&query, catalog);
    let mut root = PlannerInfo::new_with_config(query, aggregate_layout, config);
    let command_type = root.parse.command_type;
    let scanjoin_rel = query_planner(&mut root, catalog);
    let final_rel = grouping_planner(&mut root, scanjoin_rel, catalog);
    let required_pathkeys = required_query_pathkeys_for_rel(&root, &final_rel);
    let best_path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
        .cloned()
        .unwrap_or(Path::Result {
            plan_info: PlanEstimate::default(),
            pathtarget: PathTarget::new(Vec::new()),
        });
    let (plan_tree, ext_params, next_param_id) =
        create_plan_with_param_base(&root, best_path, catalog, &mut glob.subplans, next_param_id);
    Ok((
        PlannedStmt {
            command_type,
            depends_on_row_security: root.parse.depends_on_row_security,
            plan_tree,
            subplans: glob.subplans,
            ext_params,
        },
        next_param_id,
    ))
}

fn standard_planner(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    Ok(standard_planner_with_param_base(query, catalog, 0, config)?.0)
}

pub(crate) fn planner(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    standard_planner(query, catalog, PlannerConfig::default())
}

pub(crate) fn planner_with_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    standard_planner(query, catalog, config)
}

pub(crate) fn planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    standard_planner_with_param_base(query, catalog, next_param_id, PlannerConfig::default())
}

pub(crate) fn planner_with_param_base_and_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
    config: PlannerConfig,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    standard_planner_with_param_base(query, catalog, next_param_id, config)
}
