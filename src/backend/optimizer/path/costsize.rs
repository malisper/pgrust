use std::cmp::Ordering;
use std::collections::HashMap;

use crate::RelFileLocator;
use crate::backend::executor::{Value, compare_order_values};
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{BTREE_AM_OID, PgStatisticRow};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::pathnodes::{Path, PathKey, PlannerInfo, RestrictInfo};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::SetReturningCall;
use crate::include::nodes::primnodes::{
    BoolExprType, Expr, ExprArraySubscript, JoinType, OpExprKind, OrderByEntry, ProjectSetTarget,
    QueryColumn, RelationDesc, TargetEntry, ToastRelationRef, attrno_index,
};

use super::super::pathnodes::next_synthetic_slot_id;
use super::super::rewrite::{
    layout_candidate_for_expr, rewrite_expr_for_path, rewrite_semantic_expr_for_path,
    rewrite_semantic_expr_for_path_or_expand_join_vars,
};
use super::super::{
    AccessCandidate, CPU_INDEX_TUPLE_COST, CPU_OPERATOR_COST, CPU_TUPLE_COST, DEFAULT_BOOL_SEL,
    DEFAULT_EQ_SEL, DEFAULT_INEQ_SEL, DEFAULT_NUM_PAGES, DEFAULT_NUM_ROWS, HashJoinClauses,
    IndexPathSpec, IndexableQual, RANDOM_PAGE_COST, RelationStats, SEQ_PAGE_COST,
    STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV, expr_relids, path_relids, relids_subset,
};

pub(super) fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    if plan.plan_info() != PlanEstimate::default() {
        return plan;
    }
    match try_optimize_access_subtree(plan, catalog) {
        Ok(plan) => plan,
        Err(plan) => match plan {
            Path::Result { .. } => Path::Result {
                plan_info: PlanEstimate::new(0.0, 0.0, 1.0, 0),
            },
            Path::Append {
                source_id,
                desc,
                children,
                ..
            } => {
                let children = children
                    .into_iter()
                    .map(|child| optimize_path(child, catalog))
                    .collect::<Vec<_>>();
                let startup_cost = children
                    .iter()
                    .map(|child| child.plan_info().startup_cost.as_f64())
                    .fold(0.0, f64::max);
                let total_cost = children
                    .iter()
                    .map(|child| child.plan_info().total_cost.as_f64())
                    .sum::<f64>();
                let rows = clamp_rows(
                    children
                        .iter()
                        .map(|child| child.plan_info().plan_rows.as_f64())
                        .sum::<f64>(),
                );
                let width = desc
                    .columns
                    .iter()
                    .map(|column| estimate_sql_type_width(column.sql_type))
                    .sum();
                Path::Append {
                    plan_info: PlanEstimate::new(startup_cost, total_cost, rows, width),
                    source_id,
                    desc,
                    children,
                }
            }
            Path::SeqScan {
                source_id,
                rel,
                relation_name,
                relation_oid,
                toast,
                desc,
                ..
            } => {
                let stats = relation_stats(catalog, relation_oid, &desc);
                let base = seq_scan_estimate(&stats);
                Path::SeqScan {
                    plan_info: base,
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    toast,
                    desc,
                }
            }
            Path::IndexScan {
                source_id,
                rel,
                relation_oid,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
                pathkeys,
                ..
            } => {
                let stats = relation_stats(catalog, index_meta.indrelid, &desc);
                let rows = clamp_rows(stats.reltuples * DEFAULT_EQ_SEL);
                let pages = catalog
                    .class_row_by_oid(index_meta.indrelid)
                    .map(|row| row.relpages.max(1) as f64)
                    .unwrap_or(DEFAULT_NUM_PAGES);
                let plan_info = PlanEstimate::new(
                    CPU_OPERATOR_COST,
                    RANDOM_PAGE_COST + pages.min(rows.max(1.0)) + rows * CPU_INDEX_TUPLE_COST,
                    rows,
                    stats.width,
                );
                Path::IndexScan {
                    plan_info,
                    source_id,
                    rel,
                    relation_oid,
                    index_rel,
                    am_oid,
                    toast,
                    desc,
                    index_meta,
                    keys,
                    direction,
                    pathkeys,
                }
            }
            Path::Filter {
                input, predicate, ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let selectivity = clause_selectivity(&predicate, None, input_rows);
                let rows = clamp_rows(input_rows * selectivity);
                let qual_cost = predicate_cost(&predicate) * input_rows * CPU_OPERATOR_COST;
                Path::Filter {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64() + qual_cost,
                        rows,
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    predicate,
                }
            }
            Path::OrderBy { input, items, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let sort_cost = estimate_sort_cost(input_info.plan_rows.as_f64(), items.len());
                Path::OrderBy {
                    plan_info: PlanEstimate::new(
                        input_info.total_cost.as_f64(),
                        input_info.total_cost.as_f64() + sort_cost,
                        input_info.plan_rows.as_f64(),
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    items,
                }
            }
            Path::Limit {
                input,
                limit,
                offset,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let requested = limit
                    .map(|limit| limit.saturating_add(offset) as f64)
                    .unwrap_or(input_rows);
                let fraction = if input_rows <= 0.0 {
                    1.0
                } else {
                    (requested / input_rows).clamp(0.0, 1.0)
                };
                let rows = limit
                    .map(|limit| {
                        clamp_rows((input_rows - offset as f64).max(0.0).min(limit as f64))
                    })
                    .unwrap_or_else(|| clamp_rows((input_rows - offset as f64).max(0.0)));
                let total = input_info.startup_cost.as_f64()
                    + (input_info.total_cost.as_f64() - input_info.startup_cost.as_f64())
                        * fraction;
                Path::Limit {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        total,
                        rows,
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    limit,
                    offset,
                }
            }
            Path::Projection {
                input,
                targets,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let width = targets
                    .iter()
                    .map(|target| estimate_sql_type_width(target.sql_type))
                    .sum();
                Path::Projection {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        input_info.plan_rows.as_f64(),
                        width,
                    ),
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
            Path::Aggregate {
                input,
                group_by,
                accumulators,
                having,
                output_columns,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let rows = if group_by.is_empty() {
                    1.0
                } else {
                    clamp_rows((input_info.plan_rows.as_f64() * 0.1).max(1.0))
                };
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let total = input_info.total_cost.as_f64()
                    + input_info.plan_rows.as_f64()
                        * (accumulators.len().max(1) as f64)
                        * CPU_OPERATOR_COST;
                Path::Aggregate {
                    plan_info: PlanEstimate::new(total, total, rows, width),
                    slot_id,
                    input: Box::new(input),
                    group_by,
                    accumulators,
                    having,
                    output_columns,
                }
            }
            Path::CteScan {
                slot_id,
                cte_id,
                query,
                cte_plan,
                output_columns,
                ..
            } => {
                let cte_plan = optimize_path(*cte_plan, catalog);
                let cte_info = cte_plan.plan_info();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::CteScan {
                    plan_info: PlanEstimate::new(
                        cte_info.startup_cost.as_f64(),
                        cte_info.total_cost.as_f64() + CPU_TUPLE_COST,
                        cte_info.plan_rows.as_f64(),
                        width,
                    ),
                    slot_id,
                    cte_id,
                    query,
                    cte_plan: Box::new(cte_plan),
                    output_columns,
                }
            }
            Path::SubqueryScan {
                rtindex,
                query,
                input,
                output_columns,
                pathkeys,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::SubqueryScan {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64() + CPU_TUPLE_COST,
                        input_info.plan_rows.as_f64(),
                        width,
                    ),
                    rtindex,
                    query,
                    input: Box::new(input),
                    output_columns,
                    pathkeys,
                }
            }
            Path::WorkTableScan {
                slot_id,
                worktable_id,
                output_columns,
                ..
            } => {
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::WorkTableScan {
                    plan_info: PlanEstimate::new(0.0, CPU_TUPLE_COST, 100.0, width),
                    slot_id,
                    worktable_id,
                    output_columns,
                }
            }
            Path::RecursiveUnion {
                slot_id,
                worktable_id,
                distinct,
                anchor_query,
                recursive_query,
                output_columns,
                anchor,
                recursive,
                ..
            } => {
                let anchor = optimize_path(*anchor, catalog);
                let recursive = optimize_path(*recursive, catalog);
                let anchor_info = anchor.plan_info();
                let recursive_info = recursive.plan_info();
                let rows = clamp_rows(
                    anchor_info.plan_rows.as_f64() + recursive_info.plan_rows.as_f64() * 10.0,
                );
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let duplicate_cost = if distinct {
                    rows * CPU_OPERATOR_COST
                } else {
                    0.0
                };
                Path::RecursiveUnion {
                    plan_info: PlanEstimate::new(
                        anchor_info.startup_cost.as_f64(),
                        anchor_info.total_cost.as_f64()
                            + recursive_info.total_cost.as_f64() * 10.0
                            + duplicate_cost,
                        rows,
                        width,
                    ),
                    slot_id,
                    worktable_id,
                    distinct,
                    anchor_query,
                    recursive_query,
                    output_columns,
                    anchor: Box::new(anchor),
                    recursive: Box::new(recursive),
                }
            }
            Path::NestedLoopJoin {
                left,
                right,
                kind,
                restrict_clauses,
                ..
            } => {
                let left = optimize_path(*left, catalog);
                let right = optimize_path(*right, catalog);
                choose_join_plan(left, right, kind, restrict_clauses)
            }
            Path::HashJoin {
                left,
                right,
                kind,
                hash_clauses,
                outer_hash_keys,
                inner_hash_keys,
                restrict_clauses,
                ..
            } => {
                let left = optimize_path(*left, catalog);
                let right = optimize_path(*right, catalog);
                let left_relids = path_relids(&left);
                let right_relids = path_relids(&right);
                let join_clauses =
                    extract_hash_join_clauses(&restrict_clauses, &left_relids, &right_relids)
                        .map(|clauses| clauses.join_clauses)
                        .unwrap_or_default();
                estimate_hash_join(
                    left,
                    right,
                    kind,
                    hash_clauses,
                    outer_hash_keys,
                    inner_hash_keys,
                    join_clauses,
                    restrict_clauses,
                )
            }
            Path::FunctionScan { call, slot_id, .. } => {
                let output_columns = call.output_columns();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::FunctionScan {
                    plan_info: PlanEstimate::new(0.0, 10.0, 1000.0, width),
                    slot_id,
                    call,
                }
            }
            Path::Values {
                rows,
                output_columns,
                slot_id,
                ..
            } => {
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let row_count = rows.len().max(1) as f64;
                Path::Values {
                    plan_info: PlanEstimate::new(0.0, row_count * CPU_TUPLE_COST, row_count, width),
                    slot_id,
                    rows,
                    output_columns,
                }
            }
            Path::ProjectSet {
                input,
                targets,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let rows = clamp_rows(input_info.plan_rows.as_f64() * 10.0);
                let width = targets
                    .iter()
                    .map(|target| match target {
                        ProjectSetTarget::Scalar(entry) => estimate_sql_type_width(entry.sql_type),
                        ProjectSetTarget::Set { sql_type, .. } => {
                            estimate_sql_type_width(*sql_type)
                        }
                    })
                    .sum();
                Path::ProjectSet {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        rows,
                        width,
                    ),
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
        },
    }
}

fn try_optimize_access_subtree(plan: Path, catalog: &dyn CatalogLookup) -> Result<Path, Path> {
    let (source_id, rel, relation_name, relation_oid, toast, desc, filter, order_items) = match plan
    {
        Path::SeqScan {
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            ..
        } => (
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            None,
            None,
        ),
        Path::Filter {
            input, predicate, ..
        } => match *input {
            Path::SeqScan {
                source_id,
                rel,
                relation_name,
                relation_oid,
                toast,
                desc,
                ..
            } => (
                source_id,
                rel,
                relation_name,
                relation_oid,
                toast,
                desc,
                Some(predicate),
                None,
            ),
            other => {
                return Err(Path::Filter {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    predicate,
                });
            }
        },
        Path::OrderBy { input, items, .. } => match *input {
            Path::SeqScan {
                source_id,
                rel,
                relation_name,
                relation_oid,
                toast,
                desc,
                ..
            } => (
                source_id,
                rel,
                relation_name,
                relation_oid,
                toast,
                desc,
                None,
                Some(items),
            ),
            Path::Filter {
                input, predicate, ..
            } => match *input {
                Path::SeqScan {
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    toast,
                    desc,
                    ..
                } => (
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    toast,
                    desc,
                    Some(predicate),
                    Some(items),
                ),
                other => {
                    return Err(Path::OrderBy {
                        plan_info: PlanEstimate::default(),
                        input: Box::new(Path::Filter {
                            plan_info: PlanEstimate::default(),
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    });
                }
            },
            other => {
                return Err(Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    items,
                });
            }
        },
        other => return Err(other),
    };

    let filter = filter;
    let order_items = order_items;

    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut best = estimate_seqscan_candidate(
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc.clone(),
        &stats,
        filter.clone(),
        order_items.clone(),
    );
    let indexes = catalog.index_relations_for_heap(relation_oid);
    for index in indexes.iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
            && index.index_meta.am_oid == BTREE_AM_OID
    }) {
        let Some(spec) = build_index_path_spec(filter.as_ref(), order_items.as_deref(), index)
        else {
            continue;
        };
        let candidate = estimate_index_candidate(
            source_id,
            rel,
            relation_oid,
            toast,
            desc.clone(),
            &stats,
            spec,
            order_items.clone(),
            catalog,
        );
        if candidate.total_cost < best.total_cost {
            best = candidate;
        }
    }
    Ok(best.plan)
}

pub(super) fn relation_stats(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    desc: &RelationDesc,
) -> RelationStats {
    let class_row = catalog.class_row_by_oid(relation_oid);
    let relpages = class_row
        .as_ref()
        .map(|row| row.relpages.max(1) as f64)
        .unwrap_or(DEFAULT_NUM_PAGES);
    let reltuples = class_row
        .as_ref()
        .map(|row| {
            if row.reltuples > 0.0 {
                row.reltuples
            } else {
                DEFAULT_NUM_ROWS
            }
        })
        .unwrap_or(DEFAULT_NUM_ROWS);
    let stats = catalog
        .statistic_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| !row.stainherit)
        .map(|row| (row.staattnum, row))
        .collect::<HashMap<_, _>>();
    RelationStats {
        relpages,
        reltuples,
        width: estimate_relation_width(desc, &stats),
        stats_by_attnum: stats,
    }
}

pub(super) fn estimate_seqscan_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> AccessCandidate {
    let scan_info = seq_scan_estimate(stats);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut plan = Path::SeqScan {
        plan_info: scan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc,
    };
    let mut current_rows = scan_info.plan_rows.as_f64();
    let width = scan_info.plan_width;

    if let Some(predicate) = filter {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(stats.reltuples * selectivity);
        total_cost += stats.reltuples * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = Path::Filter {
            plan_info: PlanEstimate::new(
                scan_info.startup_cost.as_f64(),
                total_cost,
                current_rows,
                width,
            ),
            input: Box::new(plan),
            predicate,
        };
    }

    if let Some(items) = order_items {
        total_cost += estimate_sort_cost(current_rows, items.len());
        plan = Path::OrderBy {
            plan_info: PlanEstimate::new(
                total_cost - estimate_sort_cost(current_rows, items.len()),
                total_cost,
                current_rows,
                width,
            ),
            input: Box::new(plan),
            items,
        };
    }

    AccessCandidate { total_cost, plan }
}

pub(super) fn estimate_index_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: IndexPathSpec,
    order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    let index_class = catalog.class_row_by_oid(spec.index.relation_oid);
    let index_pages = index_class
        .as_ref()
        .map(|row| row.relpages.max(1) as f64)
        .unwrap_or(DEFAULT_NUM_PAGES);

    let used_sel = spec
        .used_quals
        .iter()
        .map(|expr| clause_selectivity(expr, Some(stats), stats.reltuples))
        .product::<f64>()
        .clamp(0.0, 1.0);
    let index_rows = clamp_rows(stats.reltuples * used_sel);
    let base_cost = RANDOM_PAGE_COST
        + index_pages.min(index_rows.max(1.0)) * RANDOM_PAGE_COST
        + index_rows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
    let scan_info = PlanEstimate::new(CPU_OPERATOR_COST, base_cost, index_rows, stats.width);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut current_rows = scan_info.plan_rows.as_f64();
    let native_pathkeys = if spec.removes_order {
        order_items
            .as_ref()
            .map(|items| {
                items
                    .iter()
                    .map(|item| PathKey {
                        expr: item.expr.clone(),
                        ressortgroupref: item.ressortgroupref,
                        descending: item.descending,
                        nulls_first: item.nulls_first,
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut plan = Path::IndexScan {
        plan_info: scan_info,
        source_id,
        rel,
        relation_oid,
        index_rel: spec.index.rel,
        am_oid: spec.index.index_meta.am_oid,
        toast,
        desc,
        index_meta: spec.index.index_meta,
        keys: spec.keys,
        direction: spec.direction,
        pathkeys: native_pathkeys,
    };

    if let Some(predicate) = spec.residual {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(current_rows * selectivity);
        total_cost += current_rows * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = Path::Filter {
            plan_info: PlanEstimate::new(
                scan_info.startup_cost.as_f64(),
                total_cost,
                current_rows,
                stats.width,
            ),
            input: Box::new(plan),
            predicate,
        };
    }

    if !spec.removes_order
        && let Some(items) = order_items
    {
        let sort_cost = estimate_sort_cost(current_rows, items.len());
        total_cost += sort_cost;
        plan = Path::OrderBy {
            plan_info: PlanEstimate::new(
                total_cost - sort_cost,
                total_cost,
                current_rows,
                stats.width,
            ),
            input: Box::new(plan),
            items,
        };
    }

    AccessCandidate { total_cost, plan }
}

fn seq_scan_estimate(stats: &RelationStats) -> PlanEstimate {
    let total_cost = stats.relpages * SEQ_PAGE_COST + stats.reltuples * CPU_TUPLE_COST;
    PlanEstimate::new(0.0, total_cost, clamp_rows(stats.reltuples), stats.width)
}

fn choose_join_plan(
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    let left_relids = path_relids(&left);
    let right_relids = path_relids(&right);
    select_best_join_path(build_join_paths(
        left,
        right,
        &left_relids,
        &right_relids,
        kind,
        restrict_clauses,
    ))
}

pub(super) fn build_join_paths(
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Vec<Path> {
    build_join_paths_internal(
        None,
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
    )
}

pub(super) fn build_join_paths_with_root(
    root: &PlannerInfo,
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Vec<Path> {
    build_join_paths_internal(
        Some(root),
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
    )
}

fn build_join_paths_internal(
    root: Option<&PlannerInfo>,
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Vec<Path> {
    let left_uses_immediate_outer = path_uses_immediate_outer_columns(&left);
    let right_uses_immediate_outer = path_uses_immediate_outer_columns(&right);
    let lateral_orientation_locked = left_uses_immediate_outer ^ right_uses_immediate_outer;
    let allow_default_orientation = !left_uses_immediate_outer || !lateral_orientation_locked;
    let allow_swapped_orientation = matches!(kind, JoinType::Inner)
        && (!right_uses_immediate_outer || !lateral_orientation_locked);

    let mut paths = Vec::new();
    if allow_default_orientation {
        paths.push(estimate_nested_loop_join_internal(
            root,
            left.clone(),
            right.clone(),
            kind,
            restrict_clauses.clone(),
        ));
    }

    if allow_swapped_orientation {
        let left_columns = left.columns();
        let right_columns = right.columns();
        let left_vars = left.output_vars();
        let right_vars = right.output_vars();
        let swapped_join = estimate_nested_loop_join_internal(
            root,
            right.clone(),
            left.clone(),
            kind,
            restrict_clauses.clone(),
        );
        paths.push(restore_join_output_order(
            swapped_join,
            &left_columns,
            &right_columns,
            &left_vars,
            &right_vars,
        ));
    }

    if !lateral_orientation_locked
        && !matches!(kind, JoinType::Cross)
        && let Some(hash_join) =
            extract_hash_join_clauses(&restrict_clauses, left_relids, right_relids)
        && hash_join_inputs_rewrite_cleanly(root, &left, &right, &hash_join)
    {
        paths.push(estimate_hash_join_internal(
            root,
            left.clone(),
            right.clone(),
            kind,
            hash_join.hash_clauses,
            hash_join.outer_hash_keys,
            hash_join.inner_hash_keys,
            hash_join.join_clauses,
            restrict_clauses.clone(),
        ));
    }

    if !lateral_orientation_locked
        && matches!(kind, JoinType::Inner)
        && let Some(hash_join) =
            extract_hash_join_clauses(&restrict_clauses, right_relids, left_relids)
        && hash_join_inputs_rewrite_cleanly(root, &right, &left, &hash_join)
    {
        let left_columns = left.columns();
        let right_columns = right.columns();
        let left_vars = left.output_vars();
        let right_vars = right.output_vars();
        let swapped_join = estimate_hash_join_internal(
            root,
            right,
            left,
            kind,
            hash_join.hash_clauses,
            hash_join.outer_hash_keys,
            hash_join.inner_hash_keys,
            hash_join.join_clauses,
            restrict_clauses,
        );
        paths.push(restore_join_output_order(
            swapped_join,
            &left_columns,
            &right_columns,
            &left_vars,
            &right_vars,
        ));
    }

    paths
}

fn select_best_join_path(paths: Vec<Path>) -> Path {
    paths
        .into_iter()
        .reduce(|best, candidate| {
            if better_join_path(&candidate, &best) {
                candidate
            } else {
                best
            }
        })
        .expect("join planning should produce at least one path")
}

fn better_join_path(candidate: &Path, current: &Path) -> bool {
    let candidate_info = candidate.plan_info();
    let current_info = current.plan_info();
    let total_cmp = candidate_info
        .total_cost
        .as_f64()
        .partial_cmp(&current_info.total_cost.as_f64())
        .unwrap_or(Ordering::Equal);
    if total_cmp != Ordering::Equal {
        return total_cmp == Ordering::Less;
    }
    let startup_cmp = candidate_info
        .startup_cost
        .as_f64()
        .partial_cmp(&current_info.startup_cost.as_f64())
        .unwrap_or(Ordering::Equal);
    startup_cmp == Ordering::Less
        || (startup_cmp == Ordering::Equal && candidate.pathkeys().len() > current.pathkeys().len())
}

pub(super) fn extract_hash_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<HashJoinClauses> {
    let mut hash_clauses = Vec::new();
    let mut outer_hash_keys = Vec::new();
    let mut inner_hash_keys = Vec::new();
    let mut residual = Vec::new();

    for restrict in restrict_clauses {
        if let Some((outer_key, inner_key)) =
            hash_join_clause_sides(&restrict.clause, left_relids, right_relids)
        {
            hash_clauses.push(restrict.clone());
            outer_hash_keys.push(outer_key);
            inner_hash_keys.push(inner_key);
        } else {
            residual.push(restrict.clone());
        }
    }

    (!hash_clauses.is_empty()).then_some(HashJoinClauses {
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        join_clauses: residual,
    })
}

fn clause_exprs(clauses: &[RestrictInfo]) -> Vec<Expr> {
    clauses
        .iter()
        .map(|restrict| restrict.clause.clone())
        .collect()
}

fn selectivity_for_restrict_clauses(clauses: &[RestrictInfo], rows: f64) -> f64 {
    clauses
        .iter()
        .map(|restrict| clause_selectivity(&restrict.clause, None, rows))
        .product::<f64>()
        .clamp(0.0, 1.0)
}

fn predicate_cost_for_restrict_clauses(clauses: &[RestrictInfo]) -> f64 {
    clauses
        .iter()
        .map(|restrict| predicate_cost(&restrict.clause))
        .sum()
}

fn hash_join_clause_sides(
    clause: &Expr,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<(Expr, Expr)> {
    let Expr::Op(op) = clause else {
        return None;
    };
    if !matches!(op.op, OpExprKind::Eq) || op.args.len() != 2 {
        return None;
    }

    let left_expr = op.args[0].clone();
    let right_expr = op.args[1].clone();
    let left_side_relids = expr_relids(&left_expr);
    let right_side_relids = expr_relids(&right_expr);

    if relids_match_hash_side(&left_side_relids, left_relids)
        && relids_match_hash_side(&right_side_relids, right_relids)
    {
        Some((left_expr, right_expr))
    } else if relids_match_hash_side(&left_side_relids, right_relids)
        && relids_match_hash_side(&right_side_relids, left_relids)
    {
        Some((right_expr, left_expr))
    } else {
        None
    }
}

fn relids_match_hash_side(expr_relids: &[usize], side_relids: &[usize]) -> bool {
    !expr_relids.is_empty() && relids_subset(expr_relids, side_relids)
}

fn expr_uses_only_layout_vars(expr: &Expr, layout: &[Expr]) -> bool {
    if layout.contains(expr) {
        return true;
    }
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::Const(_) => true,
        Expr::Aggref(aggref) => aggref
            .args
            .iter()
            .all(|arg| expr_uses_only_layout_vars(arg, layout)),
        Expr::Op(op) => op
            .args
            .iter()
            .all(|arg| expr_uses_only_layout_vars(arg, layout)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .all(|arg| expr_uses_only_layout_vars(arg, layout)),
        Expr::Func(func) => func
            .args
            .iter()
            .all(|arg| expr_uses_only_layout_vars(arg, layout)),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_none_or(|expr| expr_uses_only_layout_vars(expr, layout)),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_none_or(|expr| expr_uses_only_layout_vars(expr, layout)),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_only_layout_vars(&saop.left, layout)
                && expr_uses_only_layout_vars(&saop.right, layout)
        }
        Expr::Cast(inner, _) => expr_uses_only_layout_vars(inner, layout),
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
            expr_uses_only_layout_vars(expr, layout)
                && expr_uses_only_layout_vars(pattern, layout)
                && escape
                    .as_deref()
                    .is_none_or(|expr| expr_uses_only_layout_vars(expr, layout))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_uses_only_layout_vars(inner, layout),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_uses_only_layout_vars(left, layout) && expr_uses_only_layout_vars(right, layout)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .all(|element| expr_uses_only_layout_vars(element, layout)),
        Expr::Coalesce(left, right) => {
            expr_uses_only_layout_vars(left, layout) && expr_uses_only_layout_vars(right, layout)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_only_layout_vars(array, layout)
                && subscripts.iter().all(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_none_or(|expr| expr_uses_only_layout_vars(expr, layout))
                        && subscript
                            .upper
                            .as_ref()
                            .is_none_or(|expr| expr_uses_only_layout_vars(expr, layout))
                })
        }
        _ => true,
    }
}

fn hash_join_inputs_rewrite_cleanly(
    root: Option<&PlannerInfo>,
    left: &Path,
    right: &Path,
    clauses: &HashJoinClauses,
) -> bool {
    let left_layout = left.output_vars();
    let right_layout = right.output_vars();
    let mut join_layout = left_layout.clone();
    join_layout.extend(right_layout.clone());

    clauses.hash_clauses.iter().all(|clause| {
        expr_uses_only_layout_vars(
            &rewrite_semantic_expr_for_join_inputs(
                root,
                clause.clause.clone(),
                left,
                right,
                &join_layout,
            ),
            &join_layout,
        )
    }) && clauses.outer_hash_keys.iter().all(|expr| {
        expr_uses_only_layout_vars(
            &rewrite_join_input_expr(root, expr.clone(), left, &left_layout),
            &left_layout,
        )
    }) && clauses.inner_hash_keys.iter().all(|expr| {
        expr_uses_only_layout_vars(
            &rewrite_join_input_expr(root, expr.clone(), right, &right_layout),
            &right_layout,
        )
    }) && clauses.join_clauses.iter().all(|clause| {
        expr_uses_only_layout_vars(
            &rewrite_semantic_expr_for_join_inputs(
                root,
                clause.clause.clone(),
                left,
                right,
                &join_layout,
            ),
            &join_layout,
        )
    })
}

pub(super) fn rewrite_join_input_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    match root {
        Some(root) => rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, path, layout),
        None => rewrite_semantic_expr_for_path(expr, path, layout),
    }
}

fn layout_candidate_for_join_expr(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    layout: &[Expr],
) -> Option<Expr> {
    layout
        .iter()
        .find(|candidate| **candidate == *expr)
        .cloned()
        .or_else(|| root.and_then(|root| layout_candidate_for_expr(root, expr, layout)))
}

pub(super) fn rewrite_semantic_expr_for_join_inputs(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    right: &Path,
    join_layout: &[Expr],
) -> Expr {
    let original = expr;
    if let Some(candidate) = layout_candidate_for_join_expr(root, &original, join_layout) {
        return candidate;
    }
    let left_layout = left.output_vars();
    let right_layout = right.output_vars();
    if left_layout.contains(&original) {
        return rewrite_expr_for_path(original, left, &left_layout);
    }
    if right_layout.contains(&original) {
        return rewrite_expr_for_path(original, right, &right_layout);
    }
    if matches!(original, Expr::Var(_)) {
        let rewritten_left = rewrite_join_input_expr(root, original.clone(), left, &left_layout);
        if rewritten_left != original || left_layout.contains(&original) {
            return rewritten_left;
        }
        let rewritten_right = rewrite_join_input_expr(root, original.clone(), right, &right_layout);
        if rewritten_right != original || right_layout.contains(&original) {
            return rewritten_right;
        }
    }
    let rebuilt = match original.clone() {
        Expr::Var(_) => original,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr_for_join_inputs(root, arg, left, right, join_layout)
                })
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr_for_join_inputs(root, arg, left, right, join_layout)
                })
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr_for_join_inputs(root, arg, left, right, join_layout)
                })
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr_for_join_inputs(root, arg, left, right, join_layout)
                })
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_join_inputs(
                        root,
                        *expr,
                        left,
                        right,
                        join_layout,
                    ))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_join_inputs(
                        root,
                        *expr,
                        left,
                        right,
                        join_layout,
                    ))
                }),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_semantic_expr_for_join_inputs(
                    root,
                    *saop.left,
                    left,
                    right,
                    join_layout,
                )),
                right: Box::new(rewrite_semantic_expr_for_join_inputs(
                    root,
                    *saop.right,
                    left,
                    right,
                    join_layout,
                )),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *inner,
                left,
                right,
                join_layout,
            )),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *expr,
                left,
                right,
                join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *pattern,
                left,
                right,
                join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    root,
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *expr,
                left,
                right,
                join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *pattern,
                left,
                right,
                join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    root,
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            root,
            *inner,
            left,
            right,
            join_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            root,
            *inner,
            left,
            right,
            join_layout,
        ))),
        Expr::IsDistinctFrom(left_expr, right_expr) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::IsNotDistinctFrom(left_expr, right_expr) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| {
                    rewrite_semantic_expr_for_join_inputs(root, element, left, right, join_layout)
                })
                .collect(),
            array_type,
        },
        Expr::Coalesce(left_expr, right_expr) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr_for_join_inputs(
                root,
                *array,
                left,
                right,
                join_layout,
            )),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| {
                        rewrite_semantic_expr_for_join_inputs(root, expr, left, right, join_layout)
                    }),
                    upper: subscript.upper.map(|expr| {
                        rewrite_semantic_expr_for_join_inputs(root, expr, left, right, join_layout)
                    }),
                })
                .collect(),
        },
        other => other,
    };
    if let Some(candidate) = layout_candidate_for_join_expr(root, &rebuilt, join_layout) {
        return candidate;
    }
    if left_layout.contains(&rebuilt) {
        return rewrite_expr_for_path(rebuilt, left, &left_layout);
    }
    if right_layout.contains(&rebuilt) {
        return rewrite_expr_for_path(rebuilt, right, &right_layout);
    }
    if matches!(rebuilt, Expr::Var(_)) {
        let rewritten_left = rewrite_join_input_expr(root, rebuilt.clone(), left, &left_layout);
        if rewritten_left != rebuilt || left_layout.contains(&rebuilt) {
            return rewritten_left;
        }
        let rewritten_right = rewrite_join_input_expr(root, rebuilt.clone(), right, &right_layout);
        if rewritten_right != rebuilt || right_layout.contains(&rebuilt) {
            return rewritten_right;
        }
    }
    rebuilt
}

fn expr_uses_immediate_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 1,
        Expr::Param(_) => true,
        Expr::Aggref(aggref) => aggref.args.iter().any(expr_uses_immediate_outer_columns),
        Expr::Op(op) => op.args.iter().any(expr_uses_immediate_outer_columns),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_immediate_outer_columns),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_uses_immediate_outer_columns)
                || case_expr.args.iter().any(|arm| {
                    expr_uses_immediate_outer_columns(&arm.expr)
                        || expr_uses_immediate_outer_columns(&arm.result)
                })
                || expr_uses_immediate_outer_columns(&case_expr.defresult)
        }
        Expr::CaseTest(_) => false,
        Expr::Func(func) => func.args.iter().any(expr_uses_immediate_outer_columns),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_immediate_outer_columns),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_immediate_outer_columns),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_immediate_outer_columns(&saop.left)
                || expr_uses_immediate_outer_columns(&saop.right)
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_uses_immediate_outer_columns(inner)
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
            expr_uses_immediate_outer_columns(expr)
                || expr_uses_immediate_outer_columns(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_uses_immediate_outer_columns)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_uses_immediate_outer_columns(left) || expr_uses_immediate_outer_columns(right)
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().any(expr_uses_immediate_outer_columns)
        }
        Expr::Row { fields } => fields
            .iter()
            .any(|(_, expr)| expr_uses_immediate_outer_columns(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_immediate_outer_columns(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_uses_immediate_outer_columns)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_uses_immediate_outer_columns)
                })
        }
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn set_returning_call_uses_immediate_outer_columns(call: &SetReturningCall) -> bool {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            expr_uses_immediate_outer_columns(start)
                || expr_uses_immediate_outer_columns(stop)
                || expr_uses_immediate_outer_columns(step)
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            args.iter().any(expr_uses_immediate_outer_columns)
        }
    }
}

fn project_set_target_uses_immediate_outer_columns(target: &ProjectSetTarget) -> bool {
    match target {
        ProjectSetTarget::Scalar(entry) => expr_uses_immediate_outer_columns(&entry.expr),
        ProjectSetTarget::Set { call, .. } => set_returning_call_uses_immediate_outer_columns(call),
    }
}

fn path_uses_immediate_outer_columns(path: &Path) -> bool {
    match path {
        Path::Result { .. }
        | Path::SeqScan { .. }
        | Path::IndexScan { .. }
        | Path::WorkTableScan { .. } => false,
        Path::Append { children, .. } => children.iter().any(path_uses_immediate_outer_columns),
        Path::Filter {
            input, predicate, ..
        } => {
            path_uses_immediate_outer_columns(input) || expr_uses_immediate_outer_columns(predicate)
        }
        Path::NestedLoopJoin {
            left,
            right,
            restrict_clauses,
            ..
        }
        | Path::HashJoin {
            left,
            right,
            restrict_clauses,
            ..
        } => {
            path_uses_immediate_outer_columns(left)
                || path_uses_immediate_outer_columns(right)
                || restrict_clauses
                    .iter()
                    .any(|restrict| expr_uses_immediate_outer_columns(&restrict.clause))
        }
        Path::Projection { input, targets, .. } => {
            path_uses_immediate_outer_columns(input)
                || targets
                    .iter()
                    .any(|target| expr_uses_immediate_outer_columns(&target.expr))
        }
        Path::OrderBy { input, items, .. } => {
            path_uses_immediate_outer_columns(input)
                || items
                    .iter()
                    .any(|item| expr_uses_immediate_outer_columns(&item.expr))
        }
        Path::Limit { input, .. } => path_uses_immediate_outer_columns(input),
        Path::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            path_uses_immediate_outer_columns(input)
                || group_by.iter().any(expr_uses_immediate_outer_columns)
                || accumulators
                    .iter()
                    .any(|accum| accum.args.iter().any(expr_uses_immediate_outer_columns))
                || having
                    .as_ref()
                    .is_some_and(expr_uses_immediate_outer_columns)
        }
        Path::Values { rows, .. } => rows.iter().flatten().any(expr_uses_immediate_outer_columns),
        Path::FunctionScan { call, .. } => set_returning_call_uses_immediate_outer_columns(call),
        Path::SubqueryScan { input, .. } => path_uses_immediate_outer_columns(input),
        Path::CteScan { cte_plan, .. } => path_uses_immediate_outer_columns(cte_plan),
        Path::RecursiveUnion {
            anchor, recursive, ..
        } => {
            path_uses_immediate_outer_columns(anchor)
                || path_uses_immediate_outer_columns(recursive)
        }
        Path::ProjectSet { input, targets, .. } => {
            path_uses_immediate_outer_columns(input)
                || targets
                    .iter()
                    .any(project_set_target_uses_immediate_outer_columns)
        }
    }
}

fn estimate_nested_loop_join_internal(
    root: Option<&PlannerInfo>,
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let join_sel =
        selectivity_for_restrict_clauses(&restrict_clauses, left_info.plan_rows.as_f64());
    let rows = estimate_join_rows(
        left_info.plan_rows.as_f64(),
        right_info.plan_rows.as_f64(),
        kind,
        join_sel,
    );
    let total = left_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64() * right_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64()
            * right_info.plan_rows.as_f64()
            * predicate_cost_for_restrict_clauses(&restrict_clauses)
            * CPU_OPERATOR_COST;
    Path::NestedLoopJoin {
        plan_info: PlanEstimate::new(
            left_info.startup_cost.as_f64() + right_info.startup_cost.as_f64(),
            total,
            rows,
            left_info.plan_width + right_info.plan_width,
        ),
        left: Box::new(left),
        right: Box::new(right),
        kind,
        restrict_clauses,
    }
}

fn estimate_nested_loop_join(
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    estimate_nested_loop_join_internal(None, left, right, kind, restrict_clauses)
}

fn estimate_nested_loop_join_with_root(
    root: &PlannerInfo,
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    estimate_nested_loop_join_internal(Some(root), left, right, kind, restrict_clauses)
}

fn hash_join_selectivity(hash_clauses: &[Expr], join_qual: &[Expr], left_rows: f64) -> f64 {
    hash_clauses
        .iter()
        .chain(join_qual.iter())
        .map(|expr| clause_selectivity(expr, None, left_rows))
        .product::<f64>()
        .clamp(0.0, 1.0)
}

fn estimate_join_rows(left_rows: f64, right_rows: f64, kind: JoinType, join_sel: f64) -> f64 {
    let left_rows = clamp_rows(left_rows);
    let right_rows = clamp_rows(right_rows);
    let inner_rows = clamp_rows(left_rows * right_rows * join_sel.clamp(0.0, 1.0));
    match kind {
        JoinType::Inner | JoinType::Cross => inner_rows,
        JoinType::Left => inner_rows.max(left_rows),
        JoinType::Right => inner_rows.max(right_rows),
        JoinType::Full => inner_rows.max(left_rows).max(right_rows),
    }
}

fn estimate_hash_join(
    left: Path,
    right: Path,
    kind: JoinType,
    hash_clauses: Vec<RestrictInfo>,
    outer_hash_keys: Vec<Expr>,
    inner_hash_keys: Vec<Expr>,
    join_clauses: Vec<RestrictInfo>,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    estimate_hash_join_internal(
        None,
        left,
        right,
        kind,
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        join_clauses,
        restrict_clauses,
    )
}

fn estimate_hash_join_internal(
    root: Option<&PlannerInfo>,
    left: Path,
    right: Path,
    kind: JoinType,
    hash_clauses: Vec<RestrictInfo>,
    outer_hash_keys: Vec<Expr>,
    inner_hash_keys: Vec<Expr>,
    join_clauses: Vec<RestrictInfo>,
    restrict_clauses: Vec<RestrictInfo>,
) -> Path {
    debug_assert!(
        !hash_clauses.is_empty(),
        "hash join should only be built with at least one hash clause"
    );
    debug_assert!(
        !matches!(kind, JoinType::Cross),
        "hash join does not support cross joins"
    );

    let left_layout = left.output_vars();
    let right_layout = right.output_vars();
    let mut join_layout = left_layout.clone();
    join_layout.extend(right_layout.clone());
    let rewritten_hash_clauses = hash_clauses
        .iter()
        .map(|restrict| RestrictInfo {
            clause: rewrite_semantic_expr_for_join_inputs(
                root,
                restrict.clause.clone(),
                &left,
                &right,
                &join_layout,
            ),
            required_relids: restrict.required_relids.clone(),
            is_pushed_down: restrict.is_pushed_down,
        })
        .collect::<Vec<_>>();
    let rewritten_outer_hash_keys = outer_hash_keys
        .into_iter()
        .map(|expr| rewrite_join_input_expr(root, expr, &left, &left_layout))
        .collect::<Vec<_>>();
    let rewritten_inner_hash_keys = inner_hash_keys
        .into_iter()
        .map(|expr| rewrite_join_input_expr(root, expr, &right, &right_layout))
        .collect::<Vec<_>>();
    let rewritten_join_clauses = join_clauses
        .into_iter()
        .map(|restrict| RestrictInfo {
            clause: rewrite_semantic_expr_for_join_inputs(
                root,
                restrict.clause,
                &left,
                &right,
                &join_layout,
            ),
            required_relids: restrict.required_relids,
            is_pushed_down: restrict.is_pushed_down,
        })
        .collect::<Vec<_>>();
    let canonical_hash_clauses = rewritten_outer_hash_keys
        .iter()
        .cloned()
        .zip(rewritten_inner_hash_keys.iter().cloned())
        .map(|(outer, inner)| Expr::op_auto(OpExprKind::Eq, vec![outer, inner]))
        .collect::<Vec<_>>();
    let rewritten_join_qual = clause_exprs(&rewritten_join_clauses);

    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let join_sel = hash_join_selectivity(
        &canonical_hash_clauses,
        &rewritten_join_qual,
        left_info.plan_rows.as_f64(),
    );
    let rows = estimate_join_rows(
        left_info.plan_rows.as_f64(),
        right_info.plan_rows.as_f64(),
        kind,
        join_sel,
    );
    let build_cpu = right_info.plan_rows.as_f64()
        * ((rewritten_inner_hash_keys.len() as f64) * CPU_OPERATOR_COST + CPU_TUPLE_COST);
    let probe_cpu =
        left_info.plan_rows.as_f64() * (rewritten_outer_hash_keys.len() as f64) * CPU_OPERATOR_COST;
    let recheck_cpu =
        rows * predicate_cost_for_restrict_clauses(&rewritten_join_clauses) * CPU_OPERATOR_COST;
    let startup = left_info.startup_cost.as_f64() + right_info.total_cost.as_f64() + build_cpu;
    let total = startup + left_info.total_cost.as_f64() + probe_cpu + recheck_cpu;

    Path::HashJoin {
        plan_info: PlanEstimate::new(
            startup,
            total,
            rows,
            left_info.plan_width + right_info.plan_width,
        ),
        left: Box::new(left),
        right: Box::new(right),
        kind,
        hash_clauses: rewritten_hash_clauses,
        outer_hash_keys: rewritten_outer_hash_keys,
        inner_hash_keys: rewritten_inner_hash_keys,
        restrict_clauses,
    }
}

pub(super) fn restore_join_output_order(
    join: Path,
    left_columns: &[QueryColumn],
    right_columns: &[QueryColumn],
    left_vars: &[Expr],
    right_vars: &[Expr],
) -> Path {
    let join_info = join.plan_info();
    let join_layout = join.output_vars();
    let (join_left, join_right) = match &join {
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            (&**left, &**right)
        }
        _ => return join,
    };
    let mut targets = Vec::with_capacity(left_columns.len() + right_columns.len());
    for (column, expr) in left_columns.iter().zip(left_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: rewrite_semantic_expr_for_join_inputs(
                None,
                expr.clone(),
                join_left,
                join_right,
                &join_layout,
            ),
            sql_type: column.sql_type,
            resno: targets.len() + 1,
            ressortgroupref: 0,
            input_resno: None,
            resjunk: false,
        });
    }
    for (column, expr) in right_columns.iter().zip(right_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: rewrite_semantic_expr_for_join_inputs(
                None,
                expr.clone(),
                join_left,
                join_right,
                &join_layout,
            ),
            sql_type: column.sql_type,
            resno: targets.len() + 1,
            ressortgroupref: 0,
            input_resno: None,
            resjunk: false,
        });
    }
    let width = targets
        .iter()
        .map(|target| estimate_sql_type_width(target.sql_type))
        .sum();
    Path::Projection {
        plan_info: PlanEstimate::new(
            join_info.startup_cost.as_f64(),
            join_info.total_cost.as_f64() + join_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
            join_info.plan_rows.as_f64(),
            width,
        ),
        slot_id: next_synthetic_slot_id(),
        input: Box::new(join),
        targets,
    }
}

pub(super) fn build_index_path_spec(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    index: &BoundIndexRelation,
) -> Option<IndexPathSpec> {
    let conjuncts = filter.map(flatten_and_conjuncts).unwrap_or_default();
    let parsed_quals = conjuncts
        .iter()
        .filter_map(indexable_qual)
        .collect::<Vec<_>>();
    let mut used = vec![false; parsed_quals.len()];
    let mut keys = Vec::new();
    let mut used_quals = Vec::new();
    let mut equality_prefix = 0usize;

    for attnum in &index.index_meta.indkey {
        let column = attnum.saturating_sub(1) as usize;
        if let Some((qual_idx, qual)) = parsed_quals
            .iter()
            .enumerate()
            .find(|(idx, qual)| !used[*idx] && qual.column == column && qual.strategy == 3)
        {
            used[qual_idx] = true;
            used_quals.push(qual.expr.clone());
            equality_prefix += 1;
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: equality_prefix as i16,
                strategy: qual.strategy,
                argument: qual.argument.clone(),
            });
            continue;
        }
        if let Some((qual_idx, qual)) = parsed_quals
            .iter()
            .enumerate()
            .find(|(idx, qual)| !used[*idx] && qual.column == column)
        {
            used[qual_idx] = true;
            used_quals.push(qual.expr.clone());
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (equality_prefix + 1) as i16,
                strategy: qual.strategy,
                argument: qual.argument.clone(),
            });
        }
        break;
    }

    let order_match =
        order_items.and_then(|items| index_order_match(items, index, equality_prefix));
    if keys.is_empty() && order_match.is_none() {
        return None;
    }

    let used_exprs = parsed_quals
        .iter()
        .enumerate()
        .filter_map(|(idx, qual)| {
            used.get(idx)
                .copied()
                .unwrap_or(false)
                .then_some(&qual.expr)
        })
        .collect::<Vec<_>>();
    let residual = and_exprs(
        conjuncts
            .iter()
            .filter(|expr| !used_exprs.iter().any(|used_expr| *used_expr == *expr))
            .cloned()
            .collect(),
    );

    Some(IndexPathSpec {
        index: index.clone(),
        keys,
        residual,
        used_quals,
        direction: order_match
            .as_ref()
            .map(|(_, direction)| *direction)
            .unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
        removes_order: order_match.is_some(),
    })
}

fn clause_selectivity(expr: &Expr, stats: Option<&RelationStats>, reltuples: f64) -> f64 {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .fold(1.0, |acc, arg| {
                acc * clause_selectivity(arg, stats, reltuples)
            })
            .clamp(0.0, 1.0),
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            let mut result = 0.0;
            for arg in &bool_expr.args {
                let selectivity = clause_selectivity(arg, stats, reltuples);
                result = result + selectivity - result * selectivity;
            }
            result.clamp(0.0, 1.0)
        }
        Expr::IsNull(inner) => {
            column_selectivity(inner, stats, |row, _| row.stanullfrac).unwrap_or(DEFAULT_EQ_SEL)
        }
        Expr::IsNotNull(inner) => column_selectivity(inner, stats, |row, _| 1.0 - row.stanullfrac)
            .unwrap_or(1.0 - DEFAULT_EQ_SEL),
        Expr::Op(op) if matches!(op.op, OpExprKind::Eq) && op.args.len() == 2 => {
            eq_selectivity(&op.args[0], &op.args[1], stats, reltuples)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::NotEq) && op.args.len() == 2 => {
            1.0 - eq_selectivity(&op.args[0], &op.args[1], stats, reltuples)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::Lt) && op.args.len() == 2 => {
            ineq_selectivity(&op.args[0], &op.args[1], stats, reltuples, Ordering::Less)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::LtEq) && op.args.len() == 2 => {
            ineq_selectivity(&op.args[0], &op.args[1], stats, reltuples, Ordering::Less)
                .max(eq_selectivity(&op.args[0], &op.args[1], stats, reltuples))
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::Gt) && op.args.len() == 2 => ineq_selectivity(
            &op.args[0],
            &op.args[1],
            stats,
            reltuples,
            Ordering::Greater,
        ),
        Expr::Op(op) if matches!(op.op, OpExprKind::GtEq) && op.args.len() == 2 => {
            ineq_selectivity(
                &op.args[0],
                &op.args[1],
                stats,
                reltuples,
                Ordering::Greater,
            )
            .max(eq_selectivity(&op.args[0], &op.args[1], stats, reltuples))
        }
        _ => DEFAULT_BOOL_SEL,
    }
    .clamp(0.0, 1.0)
}

fn eq_selectivity(left: &Expr, right: &Expr, stats: Option<&RelationStats>, reltuples: f64) -> f64 {
    let Some((column, constant)) = column_const_pair(left, right) else {
        return DEFAULT_EQ_SEL;
    };
    let Some(stats) = stats else {
        return DEFAULT_EQ_SEL;
    };
    let Some(row) = stats.stats_by_attnum.get(&((column + 1) as i16)) else {
        return DEFAULT_EQ_SEL;
    };
    if let Some((values, freqs)) = slot_values_and_numbers(row, STATISTIC_KIND_MCV) {
        for (value, freq) in values.elements.iter().zip(freqs.elements.iter()) {
            if values_equal(value, &constant) {
                return float_value(freq).unwrap_or(DEFAULT_EQ_SEL).clamp(0.0, 1.0);
            }
        }
    }

    let ndistinct = effective_ndistinct(row, reltuples).unwrap_or(200.0);
    let mcv_count = slot_values(row, STATISTIC_KIND_MCV)
        .map(|array| array.elements.len() as f64)
        .unwrap_or(0.0);
    let mcv_total = slot_numbers(row, STATISTIC_KIND_MCV)
        .map(|array| array.elements.iter().filter_map(float_value).sum::<f64>())
        .unwrap_or(0.0);
    let remaining = (1.0 - row.stanullfrac - mcv_total).max(0.0);
    let distinct_remaining = (ndistinct - mcv_count).max(1.0);
    (remaining / distinct_remaining).clamp(0.0, 1.0)
}

fn ineq_selectivity(
    left: &Expr,
    right: &Expr,
    stats: Option<&RelationStats>,
    _reltuples: f64,
    wanted: Ordering,
) -> f64 {
    let Some((column, constant, flipped)) = ordered_column_const_pair(left, right) else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(stats) = stats else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(row) = stats.stats_by_attnum.get(&((column + 1) as i16)) else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(hist) = slot_values(row, STATISTIC_KIND_HISTOGRAM) else {
        return DEFAULT_INEQ_SEL;
    };
    let fraction = histogram_fraction(&hist, &constant);
    let lt_fraction = fraction * (1.0 - row.stanullfrac);
    let gt_fraction = (1.0 - fraction) * (1.0 - row.stanullfrac);
    match (wanted, flipped) {
        (Ordering::Less, false) => lt_fraction,
        (Ordering::Greater, false) => gt_fraction,
        (Ordering::Less, true) => gt_fraction,
        (Ordering::Greater, true) => lt_fraction,
        _ => DEFAULT_INEQ_SEL,
    }
}

fn column_selectivity(
    expr: &Expr,
    stats: Option<&RelationStats>,
    f: impl FnOnce(&PgStatisticRow, f64) -> f64,
) -> Option<f64> {
    let column = expr_column_index(expr)?;
    let stats = stats?;
    let row = stats.stats_by_attnum.get(&((column + 1) as i16))?;
    Some(f(row, stats.reltuples))
}

fn column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value)> {
    match (left, right) {
        (expr, Expr::Const(value)) => Some((expr_column_index(expr)?, value.clone())),
        (Expr::Const(value), expr) => Some((expr_column_index(expr)?, value.clone())),
        _ => None,
    }
}

fn ordered_column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value, bool)> {
    match (left, right) {
        (expr, Expr::Const(value)) => Some((expr_column_index(expr)?, value.clone(), false)),
        (Expr::Const(value), expr) => Some((expr_column_index(expr)?, value.clone(), true)),
        _ => None,
    }
}

fn histogram_fraction(hist: &ArrayValue, constant: &Value) -> f64 {
    if hist.elements.len() < 2 {
        return DEFAULT_INEQ_SEL;
    }
    let bins = (hist.elements.len() - 1) as f64;
    for (idx, value) in hist.elements.iter().enumerate() {
        match compare_order_values(value, constant, None, false) {
            Ordering::Greater => {
                return (idx.saturating_sub(1) as f64 / bins).clamp(0.0, 1.0);
            }
            Ordering::Equal => return (idx as f64 / bins).clamp(0.0, 1.0),
            Ordering::Less => {}
        }
    }
    1.0
}

fn effective_ndistinct(row: &PgStatisticRow, reltuples: f64) -> Option<f64> {
    if row.stadistinct > 0.0 {
        Some(row.stadistinct)
    } else if row.stadistinct < 0.0 && reltuples > 0.0 {
        Some((-row.stadistinct) * reltuples)
    } else {
        None
    }
}

fn slot_values_and_numbers(row: &PgStatisticRow, kind: i16) -> Option<(ArrayValue, ArrayValue)> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    Some((row.stavalues[idx].clone()?, row.stanumbers[idx].clone()?))
}

fn slot_values(row: &PgStatisticRow, kind: i16) -> Option<ArrayValue> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    row.stavalues[idx].clone()
}

fn slot_numbers(row: &PgStatisticRow, kind: i16) -> Option<ArrayValue> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    row.stanumbers[idx].clone()
}

fn values_equal(left: &Value, right: &Value) -> bool {
    compare_order_values(left, right, None, false) == Ordering::Equal
}

fn float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Float64(v) => Some(*v),
        Value::Int16(v) => Some(*v as f64),
        Value::Int32(v) => Some(*v as f64),
        Value::Int64(v) => Some(*v as f64),
        _ => None,
    }
}

fn estimate_relation_width(desc: &RelationDesc, stats: &HashMap<i16, PgStatisticRow>) -> usize {
    desc.columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            stats
                .get(&((idx + 1) as i16))
                .map(|row| row.stawidth.max(1) as usize)
                .unwrap_or_else(|| {
                    if column.storage.attlen > 0 {
                        column.storage.attlen as usize
                    } else {
                        estimate_sql_type_width(column.sql_type)
                    }
                })
        })
        .sum::<usize>()
        .max(1)
}

pub(super) fn estimate_sql_type_width(sql_type: SqlType) -> usize {
    match sql_type.kind {
        SqlTypeKind::Bool => 1,
        SqlTypeKind::Int2 => 2,
        SqlTypeKind::Int4 | SqlTypeKind::Oid | SqlTypeKind::Date | SqlTypeKind::Float4 => 4,
        SqlTypeKind::Int8
        | SqlTypeKind::Money
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Float8 => 8,
        SqlTypeKind::Numeric => 16,
        SqlTypeKind::Bit | SqlTypeKind::VarBit | SqlTypeKind::Bytea => 16,
        SqlTypeKind::Text
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar
        | SqlTypeKind::Name
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::AnyArray
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Path
        | SqlTypeKind::Line
        | SqlTypeKind::Box
        | SqlTypeKind::Polygon
        | SqlTypeKind::Circle
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::Record
        | SqlTypeKind::Composite => 32,
    }
}

fn estimate_sort_cost(rows: f64, keys: usize) -> f64 {
    if rows <= 1.0 {
        0.0
    } else {
        rows * rows.log2().max(1.0) * (keys.max(1) as f64) * CPU_OPERATOR_COST
    }
}

pub(super) fn predicate_cost(expr: &Expr) -> f64 {
    match expr {
        Expr::Op(op) => 1.0 + op.args.iter().map(predicate_cost).sum::<f64>(),
        Expr::Bool(bool_expr) => 1.0 + bool_expr.args.iter().map(predicate_cost).sum::<f64>(),
        Expr::Coalesce(left, right) => 1.0 + predicate_cost(left) + predicate_cost(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => 1.0 + predicate_cost(inner),
        _ => 1.0,
    }
}

pub(super) fn clamp_rows(rows: f64) -> f64 {
    if !rows.is_finite() {
        1.0
    } else {
        rows.max(1.0)
    }
}

pub(super) fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .flat_map(flatten_and_conjuncts)
            .collect(),
        other => vec![other.clone()],
    }
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(column: usize, strategy: u16, argument: &Value, expr: &Expr) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            strategy,
            argument: argument.clone(),
            expr: expr.clone(),
        })
    }

    match expr {
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Eq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 3, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 3, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Lt) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 1, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 5, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::LtEq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 2, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 4, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Gt) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 5, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 1, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::GtEq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 4, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 2, value, expr),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(super) fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(exprs.into_iter().fold(first, Expr::and))
}

fn index_order_match(
    items: &[OrderByEntry],
    index: &BoundIndexRelation,
    equality_prefix: usize,
) -> Option<(usize, crate::include::access::relscan::ScanDirection)> {
    if items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Some(column) = expr_column_index(&item.expr) else {
            break;
        };
        let Some(attnum) = index.index_meta.indkey.get(equality_prefix + idx) else {
            break;
        };
        if *attnum as usize != column + 1 {
            break;
        }
        let item_direction = if item.descending {
            crate::include::access::relscan::ScanDirection::Backward
        } else {
            crate::include::access::relscan::ScanDirection::Forward
        };
        if let Some(existing) = direction {
            if existing != item_direction {
                return None;
            }
        } else {
            direction = Some(item_direction);
        }
        matched += 1;
    }
    (matched == items.len()).then_some((
        matched,
        direction.unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
    ))
}

fn expr_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => attrno_index(var.varattno),
        _ => None,
    }
}
