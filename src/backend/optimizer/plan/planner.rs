use std::cmp::Ordering;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerConfig, PlannerGlobal, PlannerInfo, RelOptInfo, RelOptKind,
    UpperRelKind,
};
use crate::include::nodes::plannodes::{AggregateStrategy, PlanEstimate, PlannedStmt};
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, OpExprKind, ProjectSetTarget, QueryColumn,
    ScalarFunctionImpl, TargetEntry, WindowClause, expr_contains_set_returning,
    set_returning_call_exprs,
};

use super::super::bestpath;
use super::super::create_plan_with_param_base;
use super::super::groupby_rewrite;
use super::super::has_grouping;
use super::super::path::{
    query_planner, relation_index_only_full_scan_paths, relation_ordered_index_paths,
    residual_where_qual,
};
use super::super::pathnodes::{expr_sql_type, next_synthetic_slot_id, window_output_columns};
use super::super::root;
use super::super::upperrels;
use super::super::util::{
    annotate_targets_for_input, build_aggregate_output_columns, pathkeys_are_fully_identified,
    pathkeys_to_order_items, projection_is_identity, required_query_pathkeys_for_path,
    required_query_pathkeys_for_rel,
};
use super::super::{
    expand_join_rte_vars, expr_relids, optimize_path_with_config, pull_up_sublinks,
};

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
        rel.add_path(optimize_path_with_config(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                pathtarget: PathTarget::from_target_list(&targets),
                slot_id,
                input: Box::new(path),
                targets: targets.clone(),
            },
            catalog,
            root.config,
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
            display_items: Vec::new(),
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
    config: PlannerConfig,
) -> Path {
    optimize_path_with_config(
        Path::Aggregate {
            plan_info: PlanEstimate::default(),
            pathtarget: reltarget,
            slot_id,
            strategy,
            disabled: false,
            pathkeys,
            input: Box::new(input),
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            output_columns,
        },
        catalog,
        config,
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
                root.config,
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
                root.config,
            ));
        } else {
            let path_satisfies_group_order =
                bestpath::pathkeys_satisfy(&path.pathkeys(), &group_pathkeys);
            if root.config.enable_hashagg {
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
                    root.config,
                ));
            }
            if path_satisfies_group_order || !root.config.enable_hashagg {
                rel.add_path(aggregate_path(
                    AggregateStrategy::Sorted,
                    group_pathkeys.clone(),
                    slot_id,
                    if path_satisfies_group_order {
                        path
                    } else {
                        ordered_group_input(path, &group_pathkeys)
                    },
                    group_by,
                    passthrough_exprs,
                    accumulators,
                    having,
                    output_columns,
                    root.grouped_target.clone(),
                    catalog,
                    root.config,
                ));
            }
        }
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_filter_rel(
    root: &PlannerInfo,
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
        rel.add_path(optimize_path_with_config(
            Path::Filter {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                input: Box::new(path),
                predicate: predicate.clone(),
            },
            catalog,
            root.config,
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
        && !rtindex_has_inheritance_children(root, catalog, *rtindex)
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
    let has_ordered_input_path = !ordered_input_paths.is_empty();
    // When an index can deliver the required window order, keep that native
    // ordering instead of adding a cheaper explicit sort on an unordered path.
    for path in input_rel
        .pathlist
        .into_iter()
        .filter(|path| {
            !has_ordered_input_path
                || bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys)
        })
        .chain(ordered_input_paths.into_iter())
    {
        let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            optimize_path_with_config(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    display_items: Vec::new(),
                    input: Box::new(path),
                },
                catalog,
                root.config,
            )
        } else {
            path
        };
        rel.add_path(optimize_path_with_config(
            Path::WindowAgg {
                plan_info: PlanEstimate::default(),
                pathtarget: rel.reltarget.clone(),
                slot_id,
                output_columns: window_output_columns(&path, &clause),
                input: Box::new(path),
                clause: clause.clone(),
            },
            catalog,
            root.config,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn rtindex_has_inheritance_children(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    rtindex: usize,
) -> bool {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return false;
    };
    if !rte.inh {
        return false;
    }
    let RangeTblEntryKind::Relation {
        relation_oid,
        relkind,
        ..
    } = rte.kind
    else {
        return false;
    };
    match relkind {
        'p' => !catalog.inheritance_children(relation_oid).is_empty(),
        'r' => catalog
            .find_all_inheritors(relation_oid)
            .into_iter()
            .any(|oid| oid != relation_oid),
        _ => false,
    }
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
        rel.add_path(optimize_path_with_config(
            Path::ProjectSet {
                plan_info: PlanEstimate::default(),
                pathtarget: rel.reltarget.clone(),
                slot_id,
                input: Box::new(path),
                targets: targets.to_vec(),
            },
            catalog,
            root.config,
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
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
        let name = target_list
            .iter()
            .find(|target| target.expr == expr)
            .map(|target| target.name.clone())
            .unwrap_or_else(|| srf.name.clone());
        targets.push(ProjectSetTarget::Set {
            name,
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
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    let required_pathkeys = required_query_pathkeys_for_rel(root, &input_rel);
    let mut extra_presorted_paths = Vec::new();
    if (root.parse.limit_count.is_some() || root.parse.limit_offset != 0)
        && let [rtindex] = input_rel.relids.as_slice()
        && !rtindex_has_inheritance_children(root, catalog, *rtindex)
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
        let display_items = sort_key_display_items(root, &root.query_pathkeys, catalog);
        rel.add_path(path_with_sort_display_items(path.clone(), &display_items));
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
            let display_items = sort_key_display_items(root, &root.query_pathkeys, catalog);
            rel.add_path(optimize_path_with_config(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    display_items,
                    input: Box::new(path.clone()),
                },
                catalog,
                root.config,
            ));
        } else if rel.pathlist.is_empty() {
            rel.add_path(path.clone());
        }
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn distinct_pathkeys(root: &PlannerInfo, targets: &[TargetEntry]) -> Vec<PathKey> {
    let target_refs = targets
        .iter()
        .map(|target| target.ressortgroupref)
        .collect::<Vec<_>>();
    if !root.query_pathkeys.is_empty()
        && root.query_pathkeys.len() == targets.len()
        && root.query_pathkeys.iter().all(|key| {
            (key.ressortgroupref != 0 && target_refs.contains(&key.ressortgroupref))
                || targets.iter().any(|target| target.expr == key.expr)
        })
    {
        return root.query_pathkeys.clone();
    }

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

fn sort_group_pathkeys(
    clauses: &[crate::include::nodes::primnodes::SortGroupClause],
    target_list: &[TargetEntry],
) -> Vec<PathKey> {
    PathTarget::from_sort_clause(clauses, target_list)
}

fn pathkey_same_expr(left: &PathKey, right: &PathKey) -> bool {
    (left.ressortgroupref != 0 && left.ressortgroupref == right.ressortgroupref)
        || left.expr == right.expr
}

fn pathkey_position(target: &PathTarget, key: &PathKey) -> Option<usize> {
    if key.ressortgroupref != 0
        && let Some(index) = target
            .sortgrouprefs
            .iter()
            .position(|ressortgroupref| *ressortgroupref == key.ressortgroupref)
    {
        return Some(index);
    }
    target.exprs.iter().position(|expr| *expr == key.expr)
}

fn unique_key_indices(path: &Path, key_pathkeys: &[PathKey]) -> Vec<usize> {
    let target = path.semantic_output_target();
    key_pathkeys
        .iter()
        .filter_map(|key| pathkey_position(&target, key))
        .collect()
}

fn append_missing_distinct_pathkeys(required: &mut Vec<PathKey>, key_pathkeys: &[PathKey]) {
    for key in key_pathkeys {
        if !required
            .iter()
            .any(|existing| pathkey_same_expr(existing, key))
        {
            required.push(key.clone());
        }
    }
}

fn distinct_on_has_order_by_tiebreakers(root: &PlannerInfo, key_pathkeys: &[PathKey]) -> bool {
    root.query_pathkeys.iter().any(|key| {
        !key_pathkeys
            .iter()
            .any(|distinct_key| pathkey_same_expr(key, distinct_key))
    })
}

fn reordered_distinct_pathkeys_for_path(
    path: &Path,
    key_pathkeys: &[PathKey],
) -> Option<Vec<PathKey>> {
    let actual = path.pathkeys();
    if actual.len() < key_pathkeys.len() {
        return None;
    }
    let mut required = Vec::with_capacity(key_pathkeys.len());
    for actual_key in actual.iter().take(key_pathkeys.len()) {
        let distinct_key = key_pathkeys
            .iter()
            .find(|key| pathkey_same_expr(actual_key, key))?;
        if required
            .iter()
            .any(|existing: &PathKey| pathkey_same_expr(existing, distinct_key))
        {
            return None;
        }
        required.push(PathKey {
            expr: distinct_key.expr.clone(),
            ressortgroupref: distinct_key.ressortgroupref,
            descending: actual_key.descending,
            nulls_first: actual_key.nulls_first,
            collation_oid: actual_key.collation_oid,
        });
    }
    Some(required)
}

fn distinct_on_required_pathkeys_for_path(
    root: &PlannerInfo,
    path: &Path,
    key_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    if !root.query_pathkeys.is_empty() && distinct_on_has_order_by_tiebreakers(root, key_pathkeys) {
        return root.query_pathkeys.clone();
    }
    if let Some(reordered) = reordered_distinct_pathkeys_for_path(path, key_pathkeys) {
        return reordered;
    }
    let mut required = if root.query_pathkeys.is_empty() {
        key_pathkeys.to_vec()
    } else {
        root.query_pathkeys.clone()
    };
    append_missing_distinct_pathkeys(&mut required, key_pathkeys);
    required
}

fn distinct_on_index_pathkeys(key_pathkeys: &[PathKey]) -> Vec<Vec<PathKey>> {
    let mut candidates = vec![key_pathkeys.to_vec()];
    if key_pathkeys.len() == 2 {
        candidates.push(vec![key_pathkeys[1].clone(), key_pathkeys[0].clone()]);
    }
    candidates
}

fn expr_equated_to_constant(predicate: &Expr, key: &Expr) -> bool {
    match predicate {
        Expr::Bool(bool_expr)
            if matches!(
                bool_expr.boolop,
                crate::include::nodes::primnodes::BoolExprType::And
            ) =>
        {
            bool_expr
                .args
                .iter()
                .any(|arg| expr_equated_to_constant(arg, key))
        }
        Expr::Op(op)
            if matches!(op.op, crate::include::nodes::primnodes::OpExprKind::Eq)
                && op.args.len() == 2 =>
        {
            (op.args[0] == *key && matches!(op.args[1], Expr::Const(_)))
                || (op.args[1] == *key && matches!(op.args[0], Expr::Const(_)))
        }
        _ => false,
    }
}

fn distinct_on_keys_are_constant(root: &PlannerInfo, key_pathkeys: &[PathKey]) -> bool {
    let Some(predicate) = root.parse.where_qual.as_ref() else {
        return false;
    };
    key_pathkeys
        .iter()
        .all(|key| expr_equated_to_constant(predicate, &key.expr))
}

fn nonconstant_order_pathkeys(root: &PlannerInfo, key_pathkeys: &[PathKey]) -> Vec<PathKey> {
    root.query_pathkeys
        .iter()
        .filter(|key| {
            !key_pathkeys
                .iter()
                .any(|distinct_key| pathkey_same_expr(key, distinct_key))
        })
        .cloned()
        .collect()
}

fn push_distinct_pathkeys_candidate(candidates: &mut Vec<Vec<PathKey>>, pathkeys: Vec<PathKey>) {
    if pathkeys.is_empty() {
        return;
    }
    if candidates.iter().any(|existing| {
        existing.len() == pathkeys.len() && bestpath::pathkeys_satisfy(existing, &pathkeys)
    }) {
        return;
    }
    candidates.push(pathkeys);
}

fn add_distinct_pathkey_prefixes(candidates: &mut Vec<Vec<PathKey>>, pathkeys: &[PathKey]) {
    for len in 1..=pathkeys.len() {
        push_distinct_pathkeys_candidate(candidates, pathkeys[..len].to_vec());
    }
}

fn permute_distinct_pathkeys(
    remaining: &[PathKey],
    current: &mut Vec<PathKey>,
    candidates: &mut Vec<Vec<PathKey>>,
) {
    if remaining.is_empty() {
        add_distinct_pathkey_prefixes(candidates, current);
        return;
    }
    for index in 0..remaining.len() {
        current.push(remaining[index].clone());
        let next = remaining
            .iter()
            .enumerate()
            .filter(|(candidate_index, _)| *candidate_index != index)
            .map(|(_, pathkey)| pathkey.clone())
            .collect::<Vec<_>>();
        permute_distinct_pathkeys(&next, current, candidates);
        current.pop();
    }
}

fn distinct_index_pathkey_candidates(
    required_pathkeys: &[PathKey],
    input_paths: &[Path],
) -> Vec<Vec<PathKey>> {
    let mut candidates = Vec::new();
    add_distinct_pathkey_prefixes(&mut candidates, required_pathkeys);
    if required_pathkeys.len() <= 4 {
        permute_distinct_pathkeys(required_pathkeys, &mut Vec::new(), &mut candidates);
    }
    for path in input_paths {
        for pathkeys in useful_distinct_pathkeys(required_pathkeys, &path.pathkeys()) {
            add_distinct_pathkey_prefixes(&mut candidates, &pathkeys);
        }
    }
    candidates
}

fn pathkey_matches(left: &PathKey, right: &PathKey) -> bool {
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

fn useful_distinct_pathkeys(required: &[PathKey], input_pathkeys: &[PathKey]) -> Vec<Vec<PathKey>> {
    let mut result = vec![required.to_vec()];
    let mut prefix = Vec::new();
    for input_key in input_pathkeys {
        if required
            .iter()
            .any(|required_key| pathkey_matches(input_key, required_key))
        {
            prefix.push(input_key.clone());
        } else {
            break;
        }
    }
    if prefix.is_empty() {
        return result;
    }
    let mut reordered = prefix.clone();
    for required_key in required {
        if !reordered
            .iter()
            .any(|existing| pathkey_matches(existing, required_key))
        {
            reordered.push(required_key.clone());
        }
    }
    if reordered.len() == required.len()
        && !result.iter().any(|existing| {
            existing.len() == reordered.len() && bestpath::pathkeys_satisfy(existing, &reordered)
        })
    {
        result.push(reordered);
    }
    result
}

fn common_presorted_prefix_len(pathkeys: &[PathKey], required: &[PathKey]) -> usize {
    pathkeys
        .iter()
        .zip(required.iter())
        .take_while(|(left, right)| pathkey_matches(left, right))
        .count()
}

fn order_path_for_distinct(
    root: &PlannerInfo,
    path: Path,
    required_pathkeys: &[PathKey],
    catalog: &dyn CatalogLookup,
) -> Option<Path> {
    if bestpath::pathkeys_satisfy(&path.pathkeys(), required_pathkeys) {
        return Some(path_with_sort_display_items(
            path,
            &sort_key_display_items(root, required_pathkeys, catalog),
        ));
    }
    if !root.config.enable_sort {
        return None;
    }
    let presorted_count = common_presorted_prefix_len(&path.pathkeys(), required_pathkeys);
    let display_items = sort_key_display_items(root, required_pathkeys, catalog);
    if presorted_count > 0 && presorted_count < required_pathkeys.len() {
        let presorted_display_items =
            sort_key_display_items(root, &required_pathkeys[..presorted_count], catalog);
        Some(optimize_path_with_config(
            Path::IncrementalSort {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                items: pathkeys_to_order_items(required_pathkeys),
                presorted_count,
                display_items,
                presorted_display_items,
                input: Box::new(path),
            },
            catalog,
            root.config,
        ))
    } else {
        Some(optimize_path_with_config(
            Path::OrderBy {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                items: pathkeys_to_order_items(required_pathkeys),
                display_items,
                input: Box::new(path),
            },
            catalog,
            root.config,
        ))
    }
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .flat_map(flatten_and_conjuncts)
            .collect(),
        other => vec![other],
    }
}

fn equality_const_expr<'a>(expr: &'a Expr) -> Option<(&'a Expr, &'a Value)> {
    let Expr::Op(op) = expr else {
        return None;
    };
    if op.op != OpExprKind::Eq || op.args.len() != 2 {
        return None;
    }
    match (&op.args[0], &op.args[1]) {
        (left, Expr::Const(value)) => Some((left, value)),
        (Expr::Const(value), right) => Some((right, value)),
        _ => None,
    }
}

fn target_is_single_valued(root: &PlannerInfo, target: &TargetEntry) -> bool {
    if matches!(target.expr, Expr::Const(_)) {
        return true;
    }
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return false;
    };
    flatten_and_conjuncts(where_qual)
        .into_iter()
        .filter_map(equality_const_expr)
        .any(|(expr, _)| *expr == target.expr)
}

fn distinct_targets_single_valued(root: &PlannerInfo, targets: &[TargetEntry]) -> bool {
    !targets.is_empty()
        && targets
            .iter()
            .all(|target| target_is_single_valued(root, target))
}

fn distinct_targets_reference_rel(
    root: &PlannerInfo,
    targets: &[TargetEntry],
    rtindex: usize,
) -> bool {
    targets.iter().any(|target| {
        expr_relids(&expand_join_rte_vars(root, target.expr.clone()))
            .iter()
            .any(|relid| *relid == rtindex)
    })
}

fn expr_contains_user_defined_function(expr: &Expr) -> bool {
    match expr {
        Expr::Func(func) => {
            matches!(func.implementation, ScalarFunctionImpl::UserDefined { .. })
                || func.args.iter().any(expr_contains_user_defined_function)
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_user_defined_function),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_user_defined_function),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_user_defined_function(inner)
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_user_defined_function(left) || expr_contains_user_defined_function(right)
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
            expr_contains_user_defined_function(expr)
                || expr_contains_user_defined_function(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_user_defined_function)
        }
        _ => false,
    }
}

fn distinct_targets_hashable(targets: &[TargetEntry]) -> bool {
    targets
        .iter()
        .all(|target| !expr_contains_user_defined_function(&target.expr))
}

fn distinct_output_columns(targets: &[TargetEntry]) -> Vec<QueryColumn> {
    targets
        .iter()
        .map(|target| QueryColumn {
            name: target.name.clone(),
            sql_type: target.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

fn project_path_to_distinct_target(
    root: &PlannerInfo,
    path: Path,
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
) -> Path {
    let reltarget = PathTarget::from_target_list(targets);
    if path.semantic_output_target() == reltarget {
        return path;
    }
    let targets = annotate_targets_for_input(Some(root), &path, targets);
    optimize_path_with_config(
        Path::Projection {
            plan_info: PlanEstimate::default(),
            pathtarget: PathTarget::from_target_list(&targets),
            slot_id: next_synthetic_slot_id(),
            input: Box::new(path),
            targets,
        },
        catalog,
        root.config,
    )
}

fn path_uses_seqscan(path: &Path) -> bool {
    match path {
        Path::SeqScan { .. } => true,
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. } => children.iter().any(path_uses_seqscan),
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Aggregate { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::ProjectSet { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::BitmapHeapScan {
            bitmapqual: input, ..
        } => path_uses_seqscan(input),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => path_uses_seqscan(left) || path_uses_seqscan(right),
        Path::Result { .. }
        | Path::IndexOnlyScan { .. }
        | Path::IndexScan { .. }
        | Path::BitmapIndexScan { .. }
        | Path::Values { .. }
        | Path::FunctionScan { .. }
        | Path::CteScan { .. }
        | Path::WorkTableScan { .. }
        | Path::SetOp { .. } => false,
    }
}

fn path_uses_indexscan(path: &Path) -> bool {
    match path {
        Path::IndexOnlyScan { .. } | Path::IndexScan { .. } | Path::BitmapIndexScan { .. } => true,
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. } => children.iter().any(path_uses_indexscan),
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Aggregate { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::ProjectSet { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::BitmapHeapScan {
            bitmapqual: input, ..
        } => path_uses_indexscan(input),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => path_uses_indexscan(left) || path_uses_indexscan(right),
        Path::Result { .. }
        | Path::SeqScan { .. }
        | Path::Values { .. }
        | Path::FunctionScan { .. }
        | Path::CteScan { .. }
        | Path::WorkTableScan { .. }
        | Path::SetOp { .. } => false,
    }
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

    let required_pathkeys = distinct_pathkeys(root, targets);
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    let raw_input_paths = input_rel.pathlist;
    let mut ordered_input_paths = raw_input_paths.clone();
    if let [rtindex] = input_rel.relids.as_slice() {
        if !distinct_targets_reference_rel(root, targets, *rtindex)
            && root.parse.where_qual.is_none()
        {
            for path in relation_index_only_full_scan_paths(root, *rtindex, catalog) {
                if !ordered_input_paths.iter().any(|existing| existing == &path) {
                    ordered_input_paths.push(path);
                }
            }
        }
        let ordered_pathkeys =
            distinct_index_pathkey_candidates(&required_pathkeys, &ordered_input_paths);
        for pathkeys in ordered_pathkeys {
            for path in relation_ordered_index_paths(root, *rtindex, &pathkeys, catalog) {
                if !ordered_input_paths.iter().any(|existing| existing == &path) {
                    ordered_input_paths.push(path);
                }
            }
        }
    }

    if distinct_targets_single_valued(root, targets) {
        for path in ordered_input_paths {
            let path = project_path_to_distinct_target(root, path, targets, catalog);
            rel.add_path(optimize_path_with_config(
                Path::Limit {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    input: Box::new(path),
                    limit: Some(1),
                    offset: 0,
                },
                catalog,
                root.config,
            ));
        }
        bestpath::set_cheapest(&mut rel);
        root.upper_rels[upper_rel_index].rel = rel.clone();
        return rel;
    }

    let skip_seqscan_distinct_paths = !root.config.enable_seqscan
        && ordered_input_paths
            .iter()
            .any(|path| !path_uses_seqscan(path));
    for path in ordered_input_paths.iter().cloned() {
        if skip_seqscan_distinct_paths && path_uses_seqscan(&path) {
            continue;
        }
        let path = project_path_to_distinct_target(root, path, targets, catalog);
        for pathkeys in useful_distinct_pathkeys(&required_pathkeys, &path.pathkeys()) {
            let Some(path) = order_path_for_distinct(root, path.clone(), &pathkeys, catalog) else {
                continue;
            };
            let key_indices = unique_key_indices(&path, &pathkeys);
            rel.add_path(optimize_path_with_config(
                Path::Unique {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    key_indices,
                    input: Box::new(path),
                },
                catalog,
                root.config,
            ));
        }
    }

    let has_index_unique_path = rel
        .pathlist
        .iter()
        .any(|path| matches!(path, Path::Unique { .. }) && path_uses_indexscan(path));

    if root.config.enable_hashagg
        && !has_index_unique_path
        && distinct_targets_hashable(targets)
        && let Some(path) = raw_input_paths.into_iter().min_by(|left, right| {
            left.plan_info()
                .total_cost
                .as_f64()
                .partial_cmp(&right.plan_info().total_cost.as_f64())
                .unwrap_or(Ordering::Equal)
        })
    {
        let group_by = targets
            .iter()
            .map(|target| target.expr.clone())
            .collect::<Vec<_>>();
        rel.add_path(aggregate_path(
            AggregateStrategy::Hashed,
            Vec::new(),
            next_synthetic_slot_id(),
            path,
            group_by,
            Vec::new(),
            Vec::new(),
            None,
            distinct_output_columns(targets),
            rel.reltarget.clone(),
            catalog,
            root.config,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_distinct_on_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let reltarget = input_rel.reltarget.clone();
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Distinct,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }

    let key_pathkeys = sort_group_pathkeys(&root.parse.distinct_on, &root.processed_tlist);
    if distinct_on_keys_are_constant(root, &key_pathkeys) {
        let required_pathkeys = nonconstant_order_pathkeys(root, &key_pathkeys);
        let mut input_paths = input_rel.pathlist.clone();
        if let [rtindex] = input_rel.relids.as_slice() {
            for path in relation_ordered_index_paths(root, *rtindex, &required_pathkeys, catalog) {
                if !input_paths.iter().any(|existing| existing == &path) {
                    input_paths.push(path);
                }
            }
        }
        let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
        for path in input_paths {
            let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
                let display_items = sort_key_display_items(root, &required_pathkeys, catalog);
                optimize_path_with_config(
                    Path::OrderBy {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        items: pathkeys_to_order_items(&required_pathkeys),
                        display_items,
                        input: Box::new(path),
                    },
                    catalog,
                    root.config,
                )
            } else {
                path
            };
            rel.add_path(optimize_path_with_config(
                Path::Limit {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    input: Box::new(path),
                    limit: Some(1),
                    offset: 0,
                },
                catalog,
                root.config,
            ));
        }
        bestpath::set_cheapest(&mut rel);
        root.upper_rels[upper_rel_index].rel = rel.clone();
        return rel;
    }

    let mut input_paths = input_rel.pathlist.clone();
    if let [rtindex] = input_rel.relids.as_slice() {
        for pathkeys in distinct_on_index_pathkeys(&key_pathkeys) {
            for path in relation_ordered_index_paths(root, *rtindex, &pathkeys, catalog) {
                if !input_paths.iter().any(|existing| existing == &path) {
                    input_paths.push(path);
                }
            }
        }
    }

    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_paths {
        let required_pathkeys = distinct_on_required_pathkeys_for_path(root, &path, &key_pathkeys);
        let path = if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            let display_items = sort_key_display_items(root, &required_pathkeys, catalog);
            optimize_path_with_config(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    pathtarget: path.semantic_output_target(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    display_items,
                    input: Box::new(path),
                },
                catalog,
                root.config,
            )
        } else {
            path
        };
        let key_indices = unique_key_indices(&path, &key_pathkeys);
        rel.add_path(optimize_path_with_config(
            Path::Unique {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                key_indices,
                input: Box::new(path),
            },
            catalog,
            root.config,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_limit_rel(
    root: &PlannerInfo,
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
        rel.add_path(optimize_path_with_config(
            Path::Limit {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                input: Box::new(path),
                limit,
                offset,
            },
            catalog,
            root.config,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_lock_rows_rel(
    root: &PlannerInfo,
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
        rel.add_path(optimize_path_with_config(
            Path::LockRows {
                plan_info: PlanEstimate::default(),
                pathtarget: path.semantic_output_target(),
                input: Box::new(path),
                row_marks: row_marks.to_vec(),
            },
            catalog,
            root.config,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn sort_key_display_items(
    root: &PlannerInfo,
    pathkeys: &[PathKey],
    catalog: &dyn CatalogLookup,
) -> Vec<String> {
    let mut display_items = Vec::new();
    let mut display_exprs = Vec::new();
    for key in pathkeys {
        let display_expr = root
            .query_pathkeys
            .iter()
            .find(|query_key| {
                key.ressortgroupref != 0 && query_key.ressortgroupref == key.ressortgroupref
            })
            .map(|query_key| query_key.expr.clone())
            .unwrap_or_else(|| key.expr.clone());
        let dedupe_expr = expand_join_rte_vars(root, display_expr.clone());
        if display_exprs
            .iter()
            .any(|existing| inner_join_equates_exprs(root, existing, &dedupe_expr))
        {
            continue;
        }
        let mut rendered = render_sort_key_expr(root, &display_expr, catalog);
        if sort_key_needs_extra_expression_parens(&display_expr)
            || sort_key_rendering_needs_expression_parens(&rendered)
        {
            rendered = format!("({rendered})");
        }
        if key.descending {
            rendered.push_str(" DESC");
        }
        if let Some(nulls_first) = key.nulls_first {
            rendered.push_str(if nulls_first {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            });
        }
        display_exprs.push(dedupe_expr);
        display_items.push(rendered);
    }
    display_items
}

fn sort_key_needs_extra_expression_parens(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            sort_key_needs_extra_expression_parens(inner)
        }
        Expr::Op(_) => true,
        _ => false,
    }
}

fn sort_key_rendering_needs_expression_parens(rendered: &str) -> bool {
    rendered.contains('(') && !rendered.starts_with('(')
}

fn path_with_sort_display_items(mut path: Path, display_items: &[String]) -> Path {
    set_sort_display_items(&mut path, display_items);
    path
}

fn set_sort_display_items(path: &mut Path, display_items: &[String]) -> bool {
    match path {
        Path::OrderBy {
            display_items: existing,
            input,
            ..
        } => {
            if existing.is_empty() && !sort_input_is_index_scan(input) {
                *existing = display_items.to_vec();
            }
            true
        }
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => set_sort_display_items(input, display_items),
        _ => false,
    }
}

fn sort_input_is_index_scan(path: &Path) -> bool {
    matches!(path, Path::IndexOnlyScan { .. } | Path::IndexScan { .. })
}

fn inner_join_equates_exprs(root: &PlannerInfo, left: &Expr, right: &Expr) -> bool {
    if left == right {
        return true;
    }
    root.inner_join_clauses.iter().any(|restrict| {
        let Expr::Op(op) = &restrict.clause else {
            return false;
        };
        if !matches!(op.op, crate::include::nodes::primnodes::OpExprKind::Eq) {
            return false;
        }
        let [op_left, op_right] = op.args.as_slice() else {
            return false;
        };
        (op_left == left && op_right == right) || (op_left == right && op_right == left)
    })
}

fn render_sort_key_expr(root: &PlannerInfo, expr: &Expr, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => root
            .parse
            .rtable
            .get(var.varno.saturating_sub(1))
            .and_then(|rte| {
                crate::include::nodes::primnodes::attrno_index(var.varattno).and_then(|index| {
                    if let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind
                        && let Some(alias_expr) = joinaliasvars.get(index)
                    {
                        return Some(render_sort_key_expr(root, alias_expr, catalog));
                    }
                    rte.desc.columns.get(index).map(|column| {
                        let qualifier = rte.alias.as_deref();
                        match (qualifier, &rte.kind) {
                            (Some(qualifier), RangeTblEntryKind::Relation { relation_oid, .. })
                                if catalog.class_row_by_oid(*relation_oid).is_some_and(
                                    |class_row| class_row.relname.eq_ignore_ascii_case(qualifier),
                                ) =>
                            {
                                column.name.clone()
                            }
                            (Some(qualifier), _) if qualifier == column.name => column.name.clone(),
                            (Some(qualifier), _) => format!("{qualifier}.{}", column.name),
                            (None, _) => column.name.clone(),
                        }
                    })
                })
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return format!("{expr:?}");
            };
            let op_text = match op.op {
                crate::include::nodes::primnodes::OpExprKind::Add => "+",
                crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                crate::include::nodes::primnodes::OpExprKind::Div => "/",
                crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                crate::include::nodes::primnodes::OpExprKind::Eq => "=",
                crate::include::nodes::primnodes::OpExprKind::NotEq => "<>",
                crate::include::nodes::primnodes::OpExprKind::Lt => "<",
                crate::include::nodes::primnodes::OpExprKind::LtEq => "<=",
                crate::include::nodes::primnodes::OpExprKind::Gt => ">",
                crate::include::nodes::primnodes::OpExprKind::GtEq => ">=",
                _ => return format!("{expr:?}"),
            };
            format!(
                "({} {} {})",
                render_sort_key_expr(root, left, catalog),
                op_text,
                render_sort_key_expr(root, right, catalog)
            )
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            render_sort_key_expr(root, inner, catalog)
        }
        Expr::Func(func) => match func.implementation {
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoArea) => func
                .args
                .first()
                .map(|arg| format!("(area({}))", render_geometry_sort_arg(root, arg, catalog)))
                .unwrap_or_else(|| crate::backend::executor::render_explain_expr(expr, &[])),
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPolyCenter) => func
                .args
                .first()
                .map(|arg| {
                    format!(
                        "poly_center({})",
                        render_geometry_sort_arg(root, arg, catalog)
                    )
                })
                .unwrap_or_else(|| crate::backend::executor::render_explain_expr(expr, &[])),
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPoint)
                if func.args.len() == 1
                    && expr_sql_type(&func.args[0]).kind
                        == crate::backend::parser::SqlTypeKind::Polygon =>
            {
                format!(
                    "poly_center({})",
                    render_geometry_sort_arg(root, &func.args[0], catalog)
                )
            }
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointX)
            | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointY) => {
                let Some(arg) = func.args.first() else {
                    return crate::backend::executor::render_explain_expr(expr, &[]);
                };
                let index = match func.implementation {
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointX) => 0,
                    _ => 1,
                };
                format!("(({})[{index}])", render_sort_key_expr(root, arg, catalog))
            }
            _ => crate::backend::executor::render_explain_expr(expr, &[]),
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_sort_key_expr(root, left, catalog),
            render_sort_key_expr(root, right, catalog)
        ),
        Expr::Const(value) => {
            let rendered =
                crate::backend::executor::render_explain_expr(&Expr::Const(value.clone()), &[]);
            rendered
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
                .unwrap_or(&rendered)
                .to_string()
        }
        _ => crate::backend::executor::render_explain_expr(expr, &[]),
    }
}

fn render_geometry_sort_arg(
    root: &PlannerInfo,
    expr: &Expr,
    catalog: &dyn CatalogLookup,
) -> String {
    let rendered = render_sort_key_expr(root, expr, catalog);
    let Some((qualifier, name)) = rendered.rsplit_once('.') else {
        return rendered;
    };
    let simple_ident = |part: &str| {
        !part.is_empty()
            && part
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    };
    if simple_ident(qualifier) && simple_ident(name) {
        name.to_string()
    } else {
        rendered
    }
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
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(input_rel.relids.clone(), RelOptKind::UpperRel, reltarget);
    for path in input_rel.pathlist {
        let targets = annotate_targets_for_input(Some(root), &path, targets);
        if allow_identity_elision && projection_is_identity(&path, &targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path_with_config(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                pathtarget: PathTarget::from_target_list(&targets),
                slot_id,
                input: Box::new(path),
                targets,
            },
            catalog,
            root.config,
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
        if !root.parse.distinct_on.is_empty() {
            if current_rel.reltarget != root.sort_input_target {
                current_rel = make_pathtarget_projection_rel(
                    root,
                    current_rel,
                    &root.sort_input_target.clone(),
                    catalog,
                    false,
                );
            }
            current_rel = make_distinct_on_rel(root, current_rel, catalog);
            projection_done = current_rel.reltarget == root.final_target;
        } else {
            if current_rel.reltarget != root.final_target {
                current_rel = make_pathtarget_projection_rel(
                    root,
                    current_rel,
                    &root.final_target,
                    catalog,
                    false,
                );
            }
            current_rel = make_distinct_rel(root, current_rel, &final_targets, catalog);
            projection_done = false;
        }
    }

    let distinct_on_constant = root.parse.distinct
        && !root.parse.distinct_on.is_empty()
        && distinct_on_keys_are_constant(
            root,
            &sort_group_pathkeys(&root.parse.distinct_on, &root.processed_tlist),
        );
    if !has_grouping
        && !has_windowing(root)
        && !has_target_srfs
        && !root.query_pathkeys.is_empty()
        && !distinct_on_constant
        && current_rel.reltarget != root.sort_input_target
    {
        current_rel = make_pathtarget_projection_rel(
            root,
            current_rel,
            &root.sort_input_target,
            catalog,
            false,
        );
        projection_done = current_rel.reltarget == root.final_target;
    }

    if !root.query_pathkeys.is_empty() && !distinct_on_constant {
        current_rel = make_ordered_rel(root, current_rel, catalog);
    }

    if has_target_srfs && postponed_srfs {
        current_rel = adjust_paths_for_srfs(root, current_rel, &final_targets, catalog);
        projection_done = current_rel.reltarget == root.final_target;
    }

    if !root.parse.row_marks.is_empty() {
        current_rel = make_lock_rows_rel(root, current_rel, &root.parse.row_marks, catalog);
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
