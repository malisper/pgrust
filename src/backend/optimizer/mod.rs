use std::cmp::Ordering;
use std::collections::HashMap;

mod pathnodes;

use crate::RelFileLocator;
use crate::backend::executor::{
    Expr, OrderByEntry, Plan, PlanEstimate, QueryColumn, RelationDesc, ToastRelationRef,
    Value, compare_order_values,
};
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{BTREE_AM_OID, PgStatisticRow};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::plannodes::JoinType;
use pathnodes::{PlannerJoinExpr, PlannerOrderByEntry, PlannerPath, PlannerTargetEntry};

const DEFAULT_EQ_SEL: f64 = 0.005;
const DEFAULT_INEQ_SEL: f64 = 1.0 / 3.0;
const DEFAULT_BOOL_SEL: f64 = 0.5;
const DEFAULT_NUM_ROWS: f64 = 1000.0;
const DEFAULT_NUM_PAGES: f64 = 10.0;

const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_INDEX_TUPLE_COST: f64 = 0.005;
const CPU_OPERATOR_COST: f64 = 0.0025;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;

#[derive(Debug, Clone)]
struct RelationStats {
    relpages: f64,
    reltuples: f64,
    width: usize,
    stats_by_attnum: HashMap<i16, PgStatisticRow>,
}

#[derive(Debug, Clone)]
struct IndexableQual {
    column: usize,
    strategy: u16,
    argument: Value,
    expr: Expr,
}

#[derive(Debug, Clone)]
struct IndexPathSpec {
    index: BoundIndexRelation,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    residual: Option<Expr>,
    used_quals: Vec<Expr>,
    direction: crate::include::access::relscan::ScanDirection,
    removes_order: bool,
}

#[derive(Debug, Clone)]
struct AccessCandidate {
    total_cost: f64,
    plan: PlannerPath,
}

pub fn optimize_plan(plan: Plan, catalog: &dyn CatalogLookup) -> Plan {
    optimize_path(PlannerPath::from_plan(plan), catalog).into_plan()
}

pub(super) fn optimize_path(plan: PlannerPath, catalog: &dyn CatalogLookup) -> PlannerPath {
    match try_optimize_access_subtree(plan, catalog) {
        Ok(plan) => plan,
        Err(plan) => match plan {
            PlannerPath::Result { .. } => PlannerPath::Result {
                plan_info: PlanEstimate::new(0.0, 0.0, 1.0, 0),
            },
            PlannerPath::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => {
                let stats = relation_stats(catalog, relation_oid, &desc);
                let base = seq_scan_estimate(&stats);
                PlannerPath::SeqScan {
                    plan_info: base,
                    rel,
                    relation_oid,
                    toast,
                    desc,
                }
            }
            PlannerPath::IndexScan {
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
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
                PlannerPath::IndexScan {
                    plan_info,
                    rel,
                    index_rel,
                    am_oid,
                    toast,
                    desc,
                    index_meta,
                    keys,
                    direction,
                }
            }
            PlannerPath::Filter {
                input, predicate, ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let predicate_expr = predicate.clone().into_input_expr();
                let selectivity = clause_selectivity(&predicate_expr, None, input_rows);
                let rows = clamp_rows(input_rows * selectivity);
                let qual_cost = predicate_cost(&predicate_expr) * input_rows * CPU_OPERATOR_COST;
                PlannerPath::Filter {
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
            PlannerPath::OrderBy { input, items, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let sort_cost = estimate_sort_cost(input_info.plan_rows.as_f64(), items.len());
                PlannerPath::OrderBy {
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
            PlannerPath::Limit {
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
                    .map(|limit| clamp_rows((input_rows - offset as f64).max(0.0).min(limit as f64)))
                    .unwrap_or_else(|| clamp_rows((input_rows - offset as f64).max(0.0)));
                let total = input_info.startup_cost.as_f64()
                    + (input_info.total_cost.as_f64() - input_info.startup_cost.as_f64())
                        * fraction;
                PlannerPath::Limit {
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
            PlannerPath::Projection { input, targets, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let width = targets
                    .iter()
                    .map(|target| estimate_sql_type_width(target.sql_type))
                    .sum();
                PlannerPath::Projection {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        input_info.plan_rows.as_f64(),
                        width,
                    ),
                    input: Box::new(input),
                    targets,
                }
            }
            PlannerPath::Aggregate {
                input,
                group_by,
                accumulators,
                having,
                output_columns,
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
                PlannerPath::Aggregate {
                    plan_info: PlanEstimate::new(total, total, rows, width),
                    input: Box::new(input),
                    group_by,
                    accumulators,
                    having,
                    output_columns,
                }
            }
            PlannerPath::NestedLoopJoin {
                left,
                right,
                kind,
                on,
                ..
            } => {
                let left = optimize_path(*left, catalog);
                let right = optimize_path(*right, catalog);
                choose_join_plan(left, right, kind, on)
            }
            PlannerPath::FunctionScan { call, .. } => {
                let output_columns = call.output_columns();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                PlannerPath::FunctionScan {
                    plan_info: PlanEstimate::new(0.0, 10.0, 1000.0, width),
                    call,
                }
            }
            PlannerPath::Values {
                rows,
                output_columns,
                ..
            } => {
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let row_count = rows.len().max(1) as f64;
                PlannerPath::Values {
                    plan_info: PlanEstimate::new(0.0, row_count * CPU_TUPLE_COST, row_count, width),
                    rows,
                    output_columns,
                }
            }
            PlannerPath::ProjectSet { input, targets, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let rows = clamp_rows(input_info.plan_rows.as_f64() * 10.0);
                let width = targets
                    .iter()
                    .map(|target| match target {
                        crate::backend::executor::ProjectSetTarget::Scalar(entry) => {
                            estimate_sql_type_width(entry.sql_type)
                        }
                        crate::backend::executor::ProjectSetTarget::Set { sql_type, .. } => {
                            estimate_sql_type_width(*sql_type)
                        }
                    })
                    .sum();
                PlannerPath::ProjectSet {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        rows,
                        width,
                    ),
                    input: Box::new(input),
                    targets,
                }
            }
        },
    }
}

fn try_optimize_access_subtree(
    plan: PlannerPath,
    catalog: &dyn CatalogLookup,
) -> Result<PlannerPath, PlannerPath> {
    let (rel, relation_oid, toast, desc, filter, order_items) = match plan {
        PlannerPath::SeqScan {
            rel,
            relation_oid,
            toast,
            desc,
            ..
        } => (rel, relation_oid, toast, desc, None, None),
        PlannerPath::Filter {
            input, predicate, ..
        } => match *input {
            PlannerPath::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => (rel, relation_oid, toast, desc, Some(predicate), None),
            other => {
                return Err(PlannerPath::Filter {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    predicate,
                });
            }
        },
        PlannerPath::OrderBy { input, items, .. } => match *input {
            PlannerPath::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => (rel, relation_oid, toast, desc, None, Some(items)),
            PlannerPath::Filter {
                input, predicate, ..
            } => match *input {
                PlannerPath::SeqScan {
                    rel,
                    relation_oid,
                    toast,
                    desc,
                    ..
                } => (rel, relation_oid, toast, desc, Some(predicate), Some(items)),
                other => {
                    return Err(PlannerPath::OrderBy {
                        plan_info: PlanEstimate::default(),
                        input: Box::new(PlannerPath::Filter {
                            plan_info: PlanEstimate::default(),
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    });
                }
            },
            other => {
                return Err(PlannerPath::OrderBy {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    items,
                });
            }
        },
        other => return Err(other),
    };

    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut best = estimate_seqscan_candidate(
        rel,
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
        let filter_expr = filter.as_ref().cloned().map(PlannerJoinExpr::into_input_expr);
        let order_exprs = order_items
            .as_ref()
            .map(|items| items.iter().cloned().map(PlannerOrderByEntry::into_order_by_entry).collect::<Vec<_>>());
        let Some(spec) = build_index_path_spec(filter_expr.as_ref(), order_exprs.as_deref(), index) else {
            continue;
        };
        let candidate = estimate_index_candidate(rel, toast, desc.clone(), &stats, spec, order_items.clone(), catalog);
        if candidate.total_cost < best.total_cost {
            best = candidate;
        }
    }
    Ok(best.plan)
}

fn relation_stats(catalog: &dyn CatalogLookup, relation_oid: u32, desc: &RelationDesc) -> RelationStats {
    let class_row = catalog.class_row_by_oid(relation_oid);
    let relpages = class_row
        .as_ref()
        .map(|row| row.relpages.max(1) as f64)
        .unwrap_or(DEFAULT_NUM_PAGES);
    let reltuples = class_row
        .as_ref()
        .map(|row| if row.reltuples > 0.0 { row.reltuples } else { DEFAULT_NUM_ROWS })
        .unwrap_or(DEFAULT_NUM_ROWS);
    let stats = catalog
        .statistic_rows_for_relation(relation_oid)
        .into_iter()
        .map(|row| (row.staattnum, row))
        .collect::<HashMap<_, _>>();
    RelationStats {
        relpages,
        reltuples,
        width: estimate_relation_width(desc, &stats),
        stats_by_attnum: stats,
    }
}

fn estimate_seqscan_candidate(
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<PlannerJoinExpr>,
    order_items: Option<Vec<PlannerOrderByEntry>>,
) -> AccessCandidate {
    let scan_info = seq_scan_estimate(stats);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut plan = PlannerPath::SeqScan {
        plan_info: scan_info,
        rel,
        relation_oid,
        toast,
        desc,
    };
    let mut current_rows = scan_info.plan_rows.as_f64();
    let width = scan_info.plan_width;

    if let Some(predicate) = filter {
        let predicate_expr = predicate.clone().into_input_expr();
        let selectivity = clause_selectivity(&predicate_expr, Some(stats), stats.reltuples);
        current_rows = clamp_rows(stats.reltuples * selectivity);
        total_cost += stats.reltuples * predicate_cost(&predicate_expr) * CPU_OPERATOR_COST;
        plan = PlannerPath::Filter {
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
        plan = PlannerPath::OrderBy {
            plan_info: PlanEstimate::new(total_cost - estimate_sort_cost(current_rows, items.len()), total_cost, current_rows, width),
            input: Box::new(plan),
            items,
        };
    }

    AccessCandidate { total_cost, plan }
}

fn estimate_index_candidate(
    rel: RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: IndexPathSpec,
    order_items: Option<Vec<PlannerOrderByEntry>>,
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
    let mut plan = PlannerPath::IndexScan {
        plan_info: scan_info,
        rel,
        index_rel: spec.index.rel,
        am_oid: spec.index.index_meta.am_oid,
        toast,
        desc,
        index_meta: spec.index.index_meta,
        keys: spec.keys,
        direction: spec.direction,
    };

    if let Some(predicate) = spec.residual {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(current_rows * selectivity);
        total_cost += current_rows * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = PlannerPath::Filter {
            plan_info: PlanEstimate::new(
                scan_info.startup_cost.as_f64(),
                total_cost,
                current_rows,
                stats.width,
            ),
            input: Box::new(plan),
            predicate: PlannerJoinExpr::from_input_expr(&predicate),
        };
    }

    if !spec.removes_order
        && let Some(items) = order_items
    {
        let sort_cost = estimate_sort_cost(current_rows, items.len());
        total_cost += sort_cost;
        plan = PlannerPath::OrderBy {
            plan_info: PlanEstimate::new(total_cost - sort_cost, total_cost, current_rows, stats.width),
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
    left: PlannerPath,
    right: PlannerPath,
    kind: JoinType,
    on: PlannerJoinExpr,
) -> PlannerPath {
    let original = estimate_nested_loop_join(left.clone(), right.clone(), kind, on.clone());
    if !matches!(kind, JoinType::Inner | JoinType::Cross) {
        return original;
    }

    let left_columns = left.columns();
    let right_columns = right.columns();
    let swapped_join = estimate_nested_loop_join(right, left, kind, on.swap_inputs());
    let swapped = restore_join_output_order(swapped_join, &left_columns, &right_columns);
    if swapped.plan_info().total_cost.as_f64() < original.plan_info().total_cost.as_f64() {
        swapped
    } else {
        original
    }
}

fn estimate_nested_loop_join(
    left: PlannerPath,
    right: PlannerPath,
    kind: JoinType,
    on: PlannerJoinExpr,
) -> PlannerPath {
    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let on_expr = on.clone().into_expr(left.columns().len());
    let join_sel = clause_selectivity(&on_expr, None, left_info.plan_rows.as_f64());
    let rows =
        clamp_rows(left_info.plan_rows.as_f64() * right_info.plan_rows.as_f64() * join_sel);
    let total = left_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64() * right_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64()
            * right_info.plan_rows.as_f64()
            * predicate_cost(&on_expr)
            * CPU_OPERATOR_COST;
    PlannerPath::NestedLoopJoin {
        plan_info: PlanEstimate::new(
            left_info.startup_cost.as_f64() + right_info.startup_cost.as_f64(),
            total,
            rows,
            left_info.plan_width + right_info.plan_width,
        ),
        left: Box::new(left),
        right: Box::new(right),
        kind,
        on,
    }
}

fn restore_join_output_order(
    join: PlannerPath,
    left_columns: &[QueryColumn],
    right_columns: &[QueryColumn],
) -> PlannerPath {
    let join_info = join.plan_info();
    let right_width = right_columns.len();
    let mut targets = Vec::with_capacity(left_columns.len() + right_columns.len());
    for (idx, column) in left_columns.iter().enumerate() {
        targets.push(PlannerTargetEntry {
            name: column.name.clone(),
            expr: PlannerJoinExpr::InputColumn(right_width + idx),
            sql_type: column.sql_type,
        });
    }
    for (idx, column) in right_columns.iter().enumerate() {
        targets.push(PlannerTargetEntry {
            name: column.name.clone(),
            expr: PlannerJoinExpr::InputColumn(idx),
            sql_type: column.sql_type,
        });
    }
    let width = targets
        .iter()
        .map(|target| estimate_sql_type_width(target.sql_type))
        .sum();
    PlannerPath::Projection {
        plan_info: PlanEstimate::new(
            join_info.startup_cost.as_f64(),
            join_info.total_cost.as_f64() + join_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
            join_info.plan_rows.as_f64(),
            width,
        ),
        input: Box::new(join),
        targets,
    }
}

fn build_index_path_spec(
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

    let order_match = order_items.and_then(|items| index_order_match(items, index, equality_prefix));
    if keys.is_empty() && order_match.is_none() {
        return None;
    }

    let used_exprs = parsed_quals
        .iter()
        .enumerate()
        .filter_map(|(idx, qual)| used.get(idx).copied().unwrap_or(false).then_some(&qual.expr))
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
        Expr::And(left, right) => {
            (clause_selectivity(left, stats, reltuples)
                * clause_selectivity(right, stats, reltuples))
            .clamp(0.0, 1.0)
        }
        Expr::Or(left, right) => {
            let left = clause_selectivity(left, stats, reltuples);
            let right = clause_selectivity(right, stats, reltuples);
            (left + right - left * right).clamp(0.0, 1.0)
        }
        Expr::IsNull(inner) => column_selectivity(inner, stats, |row, _| row.stanullfrac)
            .unwrap_or(DEFAULT_EQ_SEL),
        Expr::IsNotNull(inner) => column_selectivity(inner, stats, |row, _| 1.0 - row.stanullfrac)
            .unwrap_or(1.0 - DEFAULT_EQ_SEL),
        Expr::Eq(left, right) => eq_selectivity(left, right, stats, reltuples),
        Expr::NotEq(left, right) => 1.0 - eq_selectivity(left, right, stats, reltuples),
        Expr::Lt(left, right) => ineq_selectivity(left, right, stats, reltuples, Ordering::Less),
        Expr::LtEq(left, right) => {
            ineq_selectivity(left, right, stats, reltuples, Ordering::Less)
                .max(eq_selectivity(left, right, stats, reltuples))
        }
        Expr::Gt(left, right) => ineq_selectivity(left, right, stats, reltuples, Ordering::Greater),
        Expr::GtEq(left, right) => {
            ineq_selectivity(left, right, stats, reltuples, Ordering::Greater)
                .max(eq_selectivity(left, right, stats, reltuples))
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
    let Expr::Column(column) = expr else {
        return None;
    };
    let stats = stats?;
    let row = stats.stats_by_attnum.get(&((*column + 1) as i16))?;
    Some(f(row, stats.reltuples))
}

fn column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value)> {
    match (left, right) {
        (Expr::Column(column), Expr::Const(value)) => Some((*column, value.clone())),
        (Expr::Const(value), Expr::Column(column)) => Some((*column, value.clone())),
        _ => None,
    }
}

fn ordered_column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value, bool)> {
    match (left, right) {
        (Expr::Column(column), Expr::Const(value)) => Some((*column, value.clone(), false)),
        (Expr::Const(value), Expr::Column(column)) => Some((*column, value.clone(), true)),
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

fn estimate_sql_type_width(sql_type: SqlType) -> usize {
    match sql_type.kind {
        SqlTypeKind::Bool => 1,
        SqlTypeKind::Int2 => 2,
        SqlTypeKind::Int4 | SqlTypeKind::Oid | SqlTypeKind::Date | SqlTypeKind::Float4 => 4,
        SqlTypeKind::Int8
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
        | SqlTypeKind::PgNodeTree => 32,
    }
}

fn estimate_sort_cost(rows: f64, keys: usize) -> f64 {
    if rows <= 1.0 {
        0.0
    } else {
        rows * rows.log2().max(1.0) * (keys.max(1) as f64) * CPU_OPERATOR_COST
    }
}

fn predicate_cost(expr: &Expr) -> f64 {
    match expr {
        Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::Eq(left, right)
        | Expr::NotEq(left, right)
        | Expr::Lt(left, right)
        | Expr::LtEq(left, right)
        | Expr::Gt(left, right)
        | Expr::GtEq(left, right)
        | Expr::RegexMatch(left, right)
        | Expr::Coalesce(left, right) => 1.0 + predicate_cost(left) + predicate_cost(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => 1.0 + predicate_cost(inner),
        _ => 1.0,
    }
}

fn clamp_rows(rows: f64) -> f64 {
    if !rows.is_finite() {
        1.0
    } else {
        rows.max(1.0)
    }
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::And(left, right) => {
            let mut out = flatten_and_conjuncts(left);
            out.extend(flatten_and_conjuncts(right));
            out
        }
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
        Expr::Eq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 3, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 3, value, expr),
            _ => None,
        },
        Expr::Lt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 1, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 5, value, expr),
            _ => None,
        },
        Expr::LtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 2, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 4, value, expr),
            _ => None,
        },
        Expr::Gt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 5, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 1, value, expr),
            _ => None,
        },
        Expr::GtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 4, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 2, value, expr),
            _ => None,
        },
        _ => None,
    }
}

fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(
        exprs
            .into_iter()
            .fold(first, |acc, expr| Expr::And(Box::new(acc), Box::new(expr))),
    )
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
        let Expr::Column(column) = item.expr else {
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
