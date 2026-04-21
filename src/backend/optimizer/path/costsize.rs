use std::cmp::Ordering;
use std::collections::HashMap;

use crate::RelFileLocator;
use crate::backend::executor::{Value, cast_value, compare_order_values};
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::backend::storage::page::bufpage::{ITEM_ID_SIZE, MAXALIGN, SIZE_OF_PAGE_HEADER_DATA};
use crate::backend::storage::smgr::BLCKSZ;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::access::htup::SIZEOF_HEAP_TUPLE_HEADER;
use crate::include::catalog::{
    BTREE_AM_OID, GIST_AM_OID, PgStatisticRow, bootstrap_pg_operator_rows,
    builtin_scalar_function_for_proc_oid, proc_oid_for_builtin_scalar_function,
    relkind_has_storage,
};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RestrictInfo};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::SetReturningCall;
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, JoinType, OpExprKind, OrderByEntry,
    ProjectSetTarget, QueryColumn, RelationDesc, ToastRelationRef, attrno_index,
};

use super::super::pathnodes::slot_output_target;
use super::super::{
    AccessCandidate, CPU_INDEX_TUPLE_COST, CPU_OPERATOR_COST, CPU_TUPLE_COST, DEFAULT_BOOL_SEL,
    DEFAULT_EQ_SEL, DEFAULT_INEQ_SEL, DEFAULT_NUM_PAGES, DEFAULT_NUM_ROWS, HashJoinClauses,
    IndexPathSpec, IndexableQual, RANDOM_PAGE_COST, RelationStats, SEQ_PAGE_COST,
    STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV, path_relids, relids_subset,
};
use super::gistcost::estimate_gist_scan_cost;

pub(super) fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    if plan.plan_info() != PlanEstimate::default() {
        return plan;
    }
    match try_optimize_access_subtree(plan, catalog) {
        Ok(plan) => plan,
        Err(plan) => match plan {
            Path::Result { pathtarget, .. } => Path::Result {
                plan_info: PlanEstimate::new(0.0, 0.0, 1.0, 0),
                pathtarget,
            },
            Path::Append {
                pathtarget,
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
                    pathtarget,
                    source_id,
                    desc,
                    children,
                }
            }
            Path::SetOp {
                pathtarget,
                slot_id,
                op,
                output_columns,
                child_roots,
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
                let width = output_columns
                    .iter()
                    .map(|column| estimate_sql_type_width(column.sql_type))
                    .sum();
                Path::SetOp {
                    plan_info: PlanEstimate::new(startup_cost, total_cost, rows, width),
                    pathtarget,
                    slot_id,
                    op,
                    output_columns,
                    child_roots,
                    children,
                }
            }
            Path::SeqScan {
                pathtarget,
                source_id,
                rel,
                relation_name,
                relation_oid,
                relkind,
                toast,
                desc,
                ..
            } => {
                let stats = relation_stats(catalog, relation_oid, &desc);
                let base = seq_scan_estimate(&stats);
                Path::SeqScan {
                    plan_info: base,
                    pathtarget,
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    relkind,
                    toast,
                    desc,
                }
            }
            Path::IndexScan {
                pathtarget,
                source_id,
                rel,
                relation_oid,
                index_rel,
                am_oid,
                toast,
                desc,
                index_desc,
                index_meta,
                keys,
                order_by_keys,
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
                    pathtarget,
                    source_id,
                    rel,
                    relation_oid,
                    index_rel,
                    am_oid,
                    toast,
                    desc,
                    index_desc,
                    index_meta,
                    keys,
                    order_by_keys,
                    direction,
                    pathkeys,
                }
            }
            Path::Filter {
                pathtarget,
                input,
                predicate,
                ..
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
                    pathtarget,
                    input: Box::new(input),
                    predicate,
                }
            }
            Path::OrderBy {
                pathtarget,
                input,
                items,
                ..
            } => {
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
                    pathtarget,
                    input: Box::new(input),
                    items,
                }
            }
            Path::Limit {
                pathtarget,
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
                    pathtarget,
                    input: Box::new(input),
                    limit,
                    offset,
                }
            }
            Path::Projection {
                pathtarget,
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
                    pathtarget,
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
            Path::Aggregate {
                pathtarget,
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
                    pathtarget,
                    slot_id,
                    input: Box::new(input),
                    group_by,
                    accumulators,
                    having,
                    output_columns,
                }
            }
            Path::WindowAgg {
                pathtarget,
                input,
                clause,
                output_columns,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let function_cost = clause.functions.len().max(1) as f64 * CPU_OPERATOR_COST;
                Path::WindowAgg {
                    plan_info: PlanEstimate::new(
                        input_info.total_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * function_cost,
                        input_info.plan_rows.as_f64(),
                        width,
                    ),
                    pathtarget,
                    slot_id,
                    input: Box::new(input),
                    clause,
                    output_columns,
                }
            }
            Path::CteScan {
                pathtarget,
                slot_id,
                cte_id,
                subroot,
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
                    pathtarget,
                    slot_id,
                    cte_id,
                    subroot,
                    query,
                    cte_plan: Box::new(cte_plan),
                    output_columns,
                }
            }
            Path::SubqueryScan {
                pathtarget,
                rtindex,
                subroot,
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
                    pathtarget,
                    rtindex,
                    subroot,
                    query,
                    input: Box::new(input),
                    output_columns,
                    pathkeys,
                }
            }
            Path::WorkTableScan {
                pathtarget,
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
                    pathtarget,
                    slot_id,
                    worktable_id,
                    output_columns,
                }
            }
            Path::RecursiveUnion {
                pathtarget,
                slot_id,
                worktable_id,
                distinct,
                anchor_root,
                recursive_root,
                recursive_references_worktable,
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
                    pathtarget,
                    slot_id,
                    worktable_id,
                    distinct,
                    anchor_root,
                    recursive_root,
                    recursive_references_worktable,
                    anchor_query,
                    recursive_query,
                    output_columns,
                    anchor: Box::new(anchor),
                    recursive: Box::new(recursive),
                }
            }
            Path::NestedLoopJoin {
                pathtarget,
                output_columns,
                left,
                right,
                kind,
                restrict_clauses,
                ..
            } => {
                let left = optimize_path(*left, catalog);
                let right = optimize_path(*right, catalog);
                choose_join_plan(
                    left,
                    right,
                    kind,
                    restrict_clauses,
                    pathtarget,
                    output_columns,
                )
            }
            Path::HashJoin {
                pathtarget,
                output_columns,
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
                    pathtarget,
                    output_columns,
                    hash_clauses,
                    outer_hash_keys,
                    inner_hash_keys,
                    join_clauses,
                    restrict_clauses,
                )
            }
            Path::FunctionScan {
                pathtarget,
                call,
                slot_id,
                ..
            } => {
                let output_columns = call.output_columns();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::FunctionScan {
                    plan_info: PlanEstimate::new(0.0, 10.0, 1000.0, width),
                    pathtarget,
                    slot_id,
                    call,
                }
            }
            Path::Values {
                pathtarget,
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
                    pathtarget,
                    slot_id,
                    rows,
                    output_columns,
                }
            }
            Path::ProjectSet {
                pathtarget,
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
                    pathtarget,
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
        },
    }
}

fn try_optimize_access_subtree(plan: Path, catalog: &dyn CatalogLookup) -> Result<Path, Path> {
    let (source_id, rel, relation_name, relation_oid, relkind, toast, desc, filter, order_items) =
        match plan {
            Path::SeqScan {
                source_id,
                rel,
                relation_name,
                relation_oid,
                relkind,
                toast,
                desc,
                ..
            } => (
                source_id,
                rel,
                relation_name,
                relation_oid,
                relkind,
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
                    relkind,
                    toast,
                    desc,
                    ..
                } => (
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    relkind,
                    toast,
                    desc,
                    Some(predicate),
                    None,
                ),
                other => {
                    return Err(Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: other.semantic_output_target(),
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
                    relkind,
                    toast,
                    desc,
                    ..
                } => (
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    relkind,
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
                        relkind,
                        toast,
                        desc,
                        ..
                    } => (
                        source_id,
                        rel,
                        relation_name,
                        relation_oid,
                        relkind,
                        toast,
                        desc,
                        Some(predicate),
                        Some(items),
                    ),
                    other => {
                        let input = Path::Filter {
                            plan_info: PlanEstimate::default(),
                            pathtarget: other.semantic_output_target(),
                            input: Box::new(other),
                            predicate,
                        };
                        return Err(Path::OrderBy {
                            plan_info: PlanEstimate::default(),
                            pathtarget: input.semantic_output_target(),
                            input: Box::new(input),
                            items,
                        });
                    }
                },
                other => {
                    return Err(Path::OrderBy {
                        plan_info: PlanEstimate::default(),
                        pathtarget: other.semantic_output_target(),
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
        relkind,
        toast,
        desc.clone(),
        &stats,
        filter.clone(),
        order_items.clone(),
    );
    if relkind != 'r' {
        return Ok(best.plan);
    }
    let indexes = catalog.index_relations_for_heap(relation_oid);
    for index in indexes.iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
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
    let stats = catalog
        .statistic_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| !row.stainherit)
        .map(|row| (row.staattnum, row))
        .collect::<HashMap<_, _>>();
    let width = estimate_relation_width(desc, &stats);
    let class_row = catalog.class_row_by_oid(relation_oid);
    if class_row.as_ref().is_some_and(|row| row.relkind == 'S') {
        return RelationStats {
            relpages: 1.0,
            reltuples: 1.0,
            width,
            stats_by_attnum: stats,
        };
    }

    let (relpages, reltuples) = if let Some(class_row) = class_row.as_ref() {
        if relkind_has_storage(class_row.relkind) {
            if let Some(mut current_pages) = catalog.current_relation_pages(relation_oid) {
                if current_pages < 10 && class_row.reltuples < 0.0 && !class_row.relhassubclass {
                    current_pages = 10;
                }

                let relpages = current_pages as f64;
                let reltuples = if current_pages == 0 {
                    0.0
                } else if class_row.reltuples >= 0.0 && class_row.relpages > 0 {
                    (class_row.reltuples / class_row.relpages as f64 * relpages).round()
                } else {
                    (heap_fallback_density(width) * relpages).round()
                };
                (relpages, reltuples)
            } else {
                metadata_only_relation_stats(class_row.relpages, class_row.reltuples)
            }
        } else {
            metadata_only_relation_stats(class_row.relpages, class_row.reltuples)
        }
    } else {
        (DEFAULT_NUM_PAGES, DEFAULT_NUM_ROWS)
    };

    RelationStats {
        relpages,
        reltuples,
        width,
        stats_by_attnum: stats,
    }
}

fn metadata_only_relation_stats(relpages: i32, reltuples: f64) -> (f64, f64) {
    (
        relpages.max(1) as f64,
        if reltuples > 0.0 {
            reltuples
        } else {
            DEFAULT_NUM_ROWS
        },
    )
}

fn heap_fallback_density(width: usize) -> f64 {
    const HEAP_DEFAULT_FILLFACTOR: usize = 100;
    let tuple_width = width
        .saturating_add(max_align_size(SIZEOF_HEAP_TUPLE_HEADER))
        .saturating_add(ITEM_ID_SIZE)
        .max(1);
    let usable_bytes_per_page = BLCKSZ.saturating_sub(SIZE_OF_PAGE_HEADER_DATA);
    (((usable_bytes_per_page * HEAP_DEFAULT_FILLFACTOR / 100) / tuple_width).max(1)) as f64
}

fn max_align_size(size: usize) -> usize {
    (size + (MAXALIGN - 1)) & !(MAXALIGN - 1)
}

pub(super) fn estimate_seqscan_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> AccessCandidate {
    let scan_info = seq_scan_estimate(stats);
    let base_pathtarget = slot_output_target(source_id, &desc.columns, |column| column.sql_type);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut plan = Path::SeqScan {
        plan_info: scan_info,
        pathtarget: base_pathtarget.clone(),
        source_id,
        rel,
        relation_name,
        relation_oid,
        relkind,
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
            pathtarget: plan.semantic_output_target(),
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
            pathtarget: plan.semantic_output_target(),
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
    let (startup_cost, base_cost) = if spec.index.index_meta.am_oid == GIST_AM_OID {
        estimate_gist_scan_cost(
            index_pages,
            index_rows,
            stats.reltuples,
            spec.removes_order,
            spec.order_by_keys.len(),
        )
    } else {
        let total = RANDOM_PAGE_COST
            + index_pages.min(index_rows.max(1.0)) * RANDOM_PAGE_COST
            + index_rows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
        (CPU_OPERATOR_COST, total)
    };
    let scan_info = PlanEstimate::new(startup_cost, base_cost, index_rows, stats.width);
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
        pathtarget: slot_output_target(source_id, &desc.columns, |column| column.sql_type),
        source_id,
        rel,
        relation_oid,
        index_rel: spec.index.rel,
        am_oid: spec.index.index_meta.am_oid,
        toast,
        desc,
        index_desc: spec.index.desc,
        index_meta: spec.index.index_meta,
        keys: spec.keys,
        order_by_keys: spec.order_by_keys,
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
            pathtarget: plan.semantic_output_target(),
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
            pathtarget: plan.semantic_output_target(),
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
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
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
        pathtarget,
        output_columns,
    ))
}

pub(super) fn build_join_paths(
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Vec<Path> {
    build_join_paths_internal(
        None,
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
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
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Vec<Path> {
    build_join_paths_internal(
        Some(root),
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
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
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
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
            pathtarget.clone(),
            output_columns.clone(),
        ));
    }

    if allow_swapped_orientation {
        paths.push(estimate_nested_loop_join_internal(
            root,
            right.clone(),
            left.clone(),
            kind,
            restrict_clauses.clone(),
            pathtarget.clone(),
            output_columns.clone(),
        ));
    }

    if !lateral_orientation_locked
        && !matches!(kind, JoinType::Cross)
        && let Some(hash_join) =
            extract_hash_join_clauses(&restrict_clauses, left_relids, right_relids)
    {
        paths.push(estimate_hash_join_internal(
            root,
            left.clone(),
            right.clone(),
            kind,
            pathtarget.clone(),
            output_columns.clone(),
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
    {
        paths.push(estimate_hash_join_internal(
            root,
            right,
            left,
            kind,
            pathtarget,
            output_columns,
            hash_join.hash_clauses,
            hash_join.outer_hash_keys,
            hash_join.inner_hash_keys,
            hash_join.join_clauses,
            restrict_clauses,
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
            clause_sides_match_join(restrict, left_relids, right_relids)
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

fn clause_sides_match_join(
    restrict: &RestrictInfo,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<(Expr, Expr)> {
    if !restrict.can_join || restrict.hashjoin_operator.is_none() {
        return None;
    }
    let Expr::Op(op) = &restrict.clause else {
        return None;
    };

    if relids_match_hash_side(&restrict.left_relids, left_relids)
        && relids_match_hash_side(&restrict.right_relids, right_relids)
    {
        Some((op.args[0].clone(), op.args[1].clone()))
    } else if relids_match_hash_side(&restrict.left_relids, right_relids)
        && relids_match_hash_side(&restrict.right_relids, left_relids)
    {
        Some((op.args[1].clone(), op.args[0].clone()))
    } else {
        None
    }
}

fn relids_match_hash_side(expr_relids: &[usize], side_relids: &[usize]) -> bool {
    !expr_relids.is_empty() && relids_subset(expr_relids, side_relids)
}

fn expr_uses_immediate_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 1,
        Expr::Param(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_immediate_outer_columns)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_uses_immediate_outer_columns)
        }
        Expr::WindowFunc(window_func) => {
            window_func
                .args
                .iter()
                .any(expr_uses_immediate_outer_columns)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_uses_immediate_outer_columns),
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
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
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_uses_immediate_outer_columns(expr)),
        Expr::FieldSelect { expr, .. } => expr_uses_immediate_outer_columns(expr),
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
        Expr::Xml(xml) => xml.child_exprs().any(expr_uses_immediate_outer_columns),
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
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
        | SetReturningCall::JsonRecordFunction { args, .. }
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
        Path::Append { children, .. } | Path::SetOp { children, .. } => {
            children.iter().any(path_uses_immediate_outer_columns)
        }
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
                || accumulators.iter().any(|accum| {
                    accum.args.iter().any(expr_uses_immediate_outer_columns)
                        || accum
                            .filter
                            .as_ref()
                            .is_some_and(expr_uses_immediate_outer_columns)
                })
                || having
                    .as_ref()
                    .is_some_and(expr_uses_immediate_outer_columns)
        }
        Path::WindowAgg { input, clause, .. } => {
            path_uses_immediate_outer_columns(input)
                || clause
                    .spec
                    .partition_by
                    .iter()
                    .any(expr_uses_immediate_outer_columns)
                || clause
                    .spec
                    .order_by
                    .iter()
                    .any(|item| expr_uses_immediate_outer_columns(&item.expr))
                || clause.functions.iter().any(|func| {
                    func.args.iter().any(expr_uses_immediate_outer_columns)
                        || match &func.kind {
                            crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                                aggref
                                    .aggfilter
                                    .as_ref()
                                    .is_some_and(expr_uses_immediate_outer_columns)
                            }
                            crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                        }
                })
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
    _root: Option<&PlannerInfo>,
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
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
        pathtarget,
        output_columns,
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
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Path {
    estimate_nested_loop_join_internal(
        None,
        left,
        right,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
    )
}

fn estimate_nested_loop_join_with_root(
    root: &PlannerInfo,
    left: Path,
    right: Path,
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Path {
    estimate_nested_loop_join_internal(
        Some(root),
        left,
        right,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
    )
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
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
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
        pathtarget,
        output_columns,
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        join_clauses,
        restrict_clauses,
    )
}

fn estimate_hash_join_internal(
    _root: Option<&PlannerInfo>,
    left: Path,
    right: Path,
    kind: JoinType,
    pathtarget: PathTarget,
    output_columns: Vec<QueryColumn>,
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

    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let join_sel = hash_join_selectivity(
        &clause_exprs(&hash_clauses),
        &clause_exprs(&join_clauses),
        left_info.plan_rows.as_f64(),
    );
    let rows = estimate_join_rows(
        left_info.plan_rows.as_f64(),
        right_info.plan_rows.as_f64(),
        kind,
        join_sel,
    );
    let build_cpu = right_info.plan_rows.as_f64()
        * ((inner_hash_keys.len() as f64) * CPU_OPERATOR_COST + CPU_TUPLE_COST);
    let probe_cpu =
        left_info.plan_rows.as_f64() * (outer_hash_keys.len() as f64) * CPU_OPERATOR_COST;
    let recheck_cpu = rows * predicate_cost_for_restrict_clauses(&join_clauses) * CPU_OPERATOR_COST;
    let startup = left_info.startup_cost.as_f64() + right_info.total_cost.as_f64() + build_cpu;
    let total = startup + left_info.total_cost.as_f64() + probe_cpu + recheck_cpu;

    Path::HashJoin {
        plan_info: PlanEstimate::new(
            startup,
            total,
            rows,
            left_info.plan_width + right_info.plan_width,
        ),
        pathtarget,
        output_columns,
        left: Box::new(left),
        right: Box::new(right),
        kind,
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        restrict_clauses,
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
    let (keys, used_indexes, equality_prefix) = match index.index_meta.am_oid {
        BTREE_AM_OID => build_btree_index_keys(index, &parsed_quals),
        GIST_AM_OID => {
            let (keys, used_indexes) = build_gist_index_keys(index, &parsed_quals);
            (keys, used_indexes, 0)
        }
        _ => return None,
    };
    let used_quals = used_indexes
        .into_iter()
        .filter_map(|idx| parsed_quals.get(idx).map(|qual| qual.expr.clone()))
        .collect::<Vec<_>>();
    let (order_by_keys, order_match) = if index.index_meta.am_oid == BTREE_AM_OID {
        (
            Vec::new(),
            order_items.and_then(|items| index_order_match(items, index, equality_prefix)),
        )
    } else if index.index_meta.am_oid == GIST_AM_OID {
        gist_order_match(order_items.unwrap_or(&[]), index)
    } else {
        (Vec::new(), None)
    };
    if keys.is_empty() && order_match.is_none() {
        return None;
    }

    let used_exprs = used_quals.iter().collect::<Vec<_>>();
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
        order_by_keys,
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
        Expr::Func(func) => builtin_index_qual_selectivity(func.funcid).unwrap_or(DEFAULT_BOOL_SEL),
        _ => DEFAULT_BOOL_SEL,
    }
    .clamp(0.0, 1.0)
}

fn builtin_index_qual_selectivity(funcid: u32) -> Option<f64> {
    let builtin = builtin_scalar_function_for_proc_oid(funcid)?;
    // :HACK: PostgreSQL uses operator-specific selectivity estimators for these
    // GiST-searchable predicates. Until pgrust grows that estimator plumbing,
    // use a small fallback so the planner can meaningfully rank GiST paths.
    Some(match builtin {
        BuiltinScalarFunction::GeoOverlap
        | BuiltinScalarFunction::GeoContains
        | BuiltinScalarFunction::GeoContainedBy
        | BuiltinScalarFunction::GeoLeft
        | BuiltinScalarFunction::GeoRight
        | BuiltinScalarFunction::GeoOverLeft
        | BuiltinScalarFunction::GeoOverRight
        | BuiltinScalarFunction::GeoSame
        | BuiltinScalarFunction::GeoOverBelow
        | BuiltinScalarFunction::GeoBelow
        | BuiltinScalarFunction::GeoAbove
        | BuiltinScalarFunction::GeoOverAbove
        | BuiltinScalarFunction::RangeOverlap
        | BuiltinScalarFunction::RangeAdjacent
        | BuiltinScalarFunction::RangeContains
        | BuiltinScalarFunction::RangeContainedBy
        | BuiltinScalarFunction::RangeStrictLeft
        | BuiltinScalarFunction::RangeStrictRight
        | BuiltinScalarFunction::RangeOverLeft
        | BuiltinScalarFunction::RangeOverRight => DEFAULT_EQ_SEL,
        _ => return None,
    })
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
    if sql_type.is_range() {
        return 32;
    }
    if sql_type.is_multirange() {
        return 48;
    }

    match sql_type.kind {
        SqlTypeKind::Bool => 1,
        SqlTypeKind::Int2 => 2,
        SqlTypeKind::Int4
        | SqlTypeKind::Oid
        | SqlTypeKind::RegType
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::Xid
        | SqlTypeKind::Date
        | SqlTypeKind::Float4 => 4,
        SqlTypeKind::Int8
        | SqlTypeKind::Money
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Tid
        | SqlTypeKind::Float8 => 8,
        SqlTypeKind::Numeric => 16,
        SqlTypeKind::Bit | SqlTypeKind::VarBit | SqlTypeKind::Bytea => 16,
        SqlTypeKind::Text
        | SqlTypeKind::Interval
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar
        | SqlTypeKind::Name
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::Xml
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::Void
        | SqlTypeKind::FdwHandler
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::AnyArray
        | SqlTypeKind::AnyElement
        | SqlTypeKind::AnyRange
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::AnyCompatibleArray
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyCompatibleMultirange
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
        | SqlTypeKind::Composite
        | SqlTypeKind::Trigger => 32,
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => 48,
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

fn strip_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, _) => strip_casts(inner),
        other => other,
    }
}

fn const_argument(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Const(value) => Some(value.clone()),
        Expr::Cast(inner, ty) => {
            const_argument(inner).and_then(|value| cast_value(value, *ty).ok())
        }
        _ => None,
    }
}

fn simple_index_column(index: &BoundIndexRelation, index_pos: usize) -> Option<usize> {
    // :HACK: Costing still assumes index keys map directly to heap columns.
    // Expression GiST indexes can be built and maintained, but until planner
    // matching learns indexprs, they remain invisible to this path logic too.
    let attnum = *index.index_meta.indkey.get(index_pos)?;
    (attnum > 0).then_some((attnum - 1) as usize)
}

fn operator_commutator_oid(operator_oid: u32) -> Option<u32> {
    bootstrap_pg_operator_rows()
        .into_iter()
        .find(|row| row.oid == operator_oid)
        .and_then(|row| (row.oprcom != 0).then_some(row.oprcom))
}

fn commuted_builtin_function(func: BuiltinScalarFunction) -> Option<BuiltinScalarFunction> {
    Some(match func {
        BuiltinScalarFunction::GeoDistance => BuiltinScalarFunction::GeoDistance,
        BuiltinScalarFunction::GeoLeft => BuiltinScalarFunction::GeoRight,
        BuiltinScalarFunction::GeoRight => BuiltinScalarFunction::GeoLeft,
        BuiltinScalarFunction::GeoOverLeft => BuiltinScalarFunction::GeoOverRight,
        BuiltinScalarFunction::GeoOverRight => BuiltinScalarFunction::GeoOverLeft,
        BuiltinScalarFunction::GeoOverlap => BuiltinScalarFunction::GeoOverlap,
        BuiltinScalarFunction::GeoSame => BuiltinScalarFunction::GeoSame,
        BuiltinScalarFunction::GeoContains => BuiltinScalarFunction::GeoContainedBy,
        BuiltinScalarFunction::GeoContainedBy => BuiltinScalarFunction::GeoContains,
        BuiltinScalarFunction::GeoOverBelow => BuiltinScalarFunction::GeoOverAbove,
        BuiltinScalarFunction::GeoOverAbove => BuiltinScalarFunction::GeoOverBelow,
        BuiltinScalarFunction::GeoBelow => BuiltinScalarFunction::GeoAbove,
        BuiltinScalarFunction::GeoAbove => BuiltinScalarFunction::GeoBelow,
        BuiltinScalarFunction::RangeStrictLeft => BuiltinScalarFunction::RangeStrictRight,
        BuiltinScalarFunction::RangeStrictRight => BuiltinScalarFunction::RangeStrictLeft,
        BuiltinScalarFunction::RangeOverLeft => BuiltinScalarFunction::RangeOverRight,
        BuiltinScalarFunction::RangeOverRight => BuiltinScalarFunction::RangeOverLeft,
        BuiltinScalarFunction::RangeOverlap => BuiltinScalarFunction::RangeOverlap,
        BuiltinScalarFunction::RangeAdjacent => BuiltinScalarFunction::RangeAdjacent,
        BuiltinScalarFunction::RangeContains => BuiltinScalarFunction::RangeContainedBy,
        BuiltinScalarFunction::RangeContainedBy => BuiltinScalarFunction::RangeContains,
        _ => return None,
    })
}

fn commuted_op_expr_kind(kind: OpExprKind) -> Option<OpExprKind> {
    Some(match kind {
        OpExprKind::Eq => OpExprKind::Eq,
        OpExprKind::Lt => OpExprKind::Gt,
        OpExprKind::LtEq => OpExprKind::GtEq,
        OpExprKind::Gt => OpExprKind::Lt,
        OpExprKind::GtEq => OpExprKind::LtEq,
        _ => return None,
    })
}

fn btree_builtin_strategy(kind: OpExprKind) -> Option<u16> {
    Some(match kind {
        OpExprKind::Lt => 1,
        OpExprKind::LtEq => 2,
        OpExprKind::Eq => 3,
        OpExprKind::GtEq => 4,
        OpExprKind::Gt => 5,
        _ => return None,
    })
}

fn commuted_function_proc_oid(funcid: u32) -> Option<u32> {
    let builtin = builtin_scalar_function_for_proc_oid(funcid)?;
    let commuted = commuted_builtin_function(builtin)?;
    proc_oid_for_builtin_scalar_function(commuted)
}

fn value_type_oid(value: &Value) -> Option<u32> {
    value.sql_type_hint().map(sql_type_oid)
}

fn gist_ordering_operator_oid(
    operator_proc_oid: u32,
    left_type_oid: u32,
    right_type_oid: u32,
) -> Option<u32> {
    let operator_name = match builtin_scalar_function_for_proc_oid(operator_proc_oid)? {
        BuiltinScalarFunction::GeoDistance => "<->",
        _ => return None,
    };
    bootstrap_pg_operator_rows()
        .into_iter()
        .find(|row| {
            row.oprname == operator_name
                && row.oprleft == left_type_oid
                && row.oprright == right_type_oid
        })
        .map(|row| row.oid)
}

fn gist_builtin_strategy(proc_oid: u32, argument: &Value) -> Option<u16> {
    let builtin = builtin_scalar_function_for_proc_oid(proc_oid)?;
    Some(match builtin {
        BuiltinScalarFunction::GeoLeft => 1,
        BuiltinScalarFunction::GeoOverLeft => 2,
        BuiltinScalarFunction::GeoOverlap => 3,
        BuiltinScalarFunction::GeoOverRight => 4,
        BuiltinScalarFunction::GeoRight => 5,
        BuiltinScalarFunction::GeoSame => 6,
        BuiltinScalarFunction::GeoContains => 7,
        BuiltinScalarFunction::GeoContainedBy => 8,
        BuiltinScalarFunction::GeoOverBelow => 9,
        BuiltinScalarFunction::GeoBelow => 10,
        BuiltinScalarFunction::GeoAbove => 11,
        BuiltinScalarFunction::GeoOverAbove => 12,
        BuiltinScalarFunction::RangeStrictLeft => 1,
        BuiltinScalarFunction::RangeOverLeft => 2,
        BuiltinScalarFunction::RangeOverlap => 3,
        BuiltinScalarFunction::RangeOverRight => 4,
        BuiltinScalarFunction::RangeStrictRight => 5,
        BuiltinScalarFunction::RangeAdjacent => 6,
        BuiltinScalarFunction::RangeContains => {
            if matches!(argument, Value::Range(_)) {
                7
            } else {
                16
            }
        }
        BuiltinScalarFunction::RangeContainedBy => 8,
        _ => return None,
    })
}

fn qual_strategy(
    index: &BoundIndexRelation,
    index_pos: usize,
    qual: &IndexableQual,
) -> Option<u16> {
    match qual.lookup {
        super::super::IndexStrategyLookup::Operator { oid, kind } => index
            .index_meta
            .amop_strategy_for_operator(&index.desc, index_pos, oid, value_type_oid(&qual.argument))
            .or_else(|| {
                (index.index_meta.am_oid == BTREE_AM_OID)
                    .then(|| btree_builtin_strategy(kind))
                    .flatten()
            }),
        super::super::IndexStrategyLookup::Proc(proc_oid) => index
            .index_meta
            .amop_strategy_for_proc(
                &index.desc,
                index_pos,
                proc_oid,
                value_type_oid(&qual.argument),
            )
            .or_else(|| {
                (index.index_meta.am_oid == GIST_AM_OID)
                    .then(|| gist_builtin_strategy(proc_oid, &qual.argument))
                    .flatten()
            }),
    }
}

fn build_btree_index_keys(
    index: &BoundIndexRelation,
    parsed_quals: &[IndexableQual],
) -> (
    Vec<crate::include::access::scankey::ScanKeyData>,
    Vec<usize>,
    usize,
) {
    let mut used = vec![false; parsed_quals.len()];
    let mut used_qual_indexes = Vec::new();
    let mut keys = Vec::new();
    let mut equality_prefix = 0usize;

    for index_pos in 0..index.index_meta.indkey.len() {
        let Some(column) = simple_index_column(index, index_pos) else {
            break;
        };
        if let Some((qual_idx, strategy, argument)) =
            parsed_quals.iter().enumerate().find_map(|(idx, qual)| {
                if used[idx] || qual.column != column {
                    return None;
                }
                let strategy = qual_strategy(index, index_pos, qual)?;
                (strategy == 3).then_some((idx, strategy, qual.argument.clone()))
            })
        {
            used[qual_idx] = true;
            used_qual_indexes.push(qual_idx);
            equality_prefix += 1;
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument,
            });
            continue;
        }
        if let Some((qual_idx, strategy, argument)) =
            parsed_quals.iter().enumerate().find_map(|(idx, qual)| {
                if used[idx] || qual.column != column {
                    return None;
                }
                let strategy = qual_strategy(index, index_pos, qual)?;
                Some((idx, strategy, qual.argument.clone()))
            })
        {
            used[qual_idx] = true;
            used_qual_indexes.push(qual_idx);
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument,
            });
        }
        break;
    }

    (keys, used_qual_indexes, equality_prefix)
}

fn build_gist_index_keys(
    index: &BoundIndexRelation,
    parsed_quals: &[IndexableQual],
) -> (
    Vec<crate::include::access::scankey::ScanKeyData>,
    Vec<usize>,
) {
    let mut used_qual_indexes = Vec::new();
    let keys = parsed_quals
        .iter()
        .enumerate()
        .filter_map(|(qual_idx, qual)| {
            let (index_pos, strategy) =
                (0..index.index_meta.indkey.len()).find_map(|index_pos| {
                    (simple_index_column(index, index_pos) == Some(qual.column))
                        .then(|| qual_strategy(index, index_pos, qual))
                        .flatten()
                        .map(|strategy| (index_pos, strategy))
                })?;
            used_qual_indexes.push(qual_idx);
            Some(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument: qual.argument.clone(),
            })
        })
        .collect();
    (keys, used_qual_indexes)
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(
        column: usize,
        lookup: super::super::IndexStrategyLookup,
        argument: Value,
        expr: &Expr,
    ) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            lookup,
            argument,
            expr: expr.clone(),
        })
    }

    match strip_casts(expr) {
        Expr::Op(op) if op.args.len() == 2 => {
            let left = strip_casts(&op.args[0]);
            let right = &op.args[1];
            if let (Some(column), Some(value)) = (expr_column_index(left), const_argument(right)) {
                return mk(
                    column,
                    super::super::IndexStrategyLookup::Operator {
                        oid: op.opno,
                        kind: op.op,
                    },
                    value,
                    expr,
                );
            }
            if let (Some(value), Some(column)) = (
                const_argument(&op.args[0]),
                expr_column_index(strip_casts(&op.args[1])),
            ) {
                return mk(
                    column,
                    super::super::IndexStrategyLookup::Operator {
                        oid: operator_commutator_oid(op.opno).unwrap_or(0),
                        kind: commuted_op_expr_kind(op.op)?,
                    },
                    value,
                    expr,
                );
            }
            None
        }
        Expr::Func(func) if func.args.len() == 2 => {
            let left = strip_casts(&func.args[0]);
            let right = &func.args[1];
            if let (Some(column), Some(value)) = (expr_column_index(left), const_argument(right)) {
                return mk(
                    column,
                    super::super::IndexStrategyLookup::Proc(func.funcid),
                    value,
                    expr,
                );
            }
            if let (Some(value), Some(column)) = (
                const_argument(&func.args[0]),
                expr_column_index(strip_casts(&func.args[1])),
            ) {
                return mk(
                    column,
                    super::super::IndexStrategyLookup::Proc(commuted_function_proc_oid(
                        func.funcid,
                    )?),
                    value,
                    expr,
                );
            }
            None
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
    if index.index_meta.am_oid != BTREE_AM_OID || items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Some(column) = expr_column_index(&item.expr) else {
            break;
        };
        let Some(index_column) = simple_index_column(index, equality_prefix + idx) else {
            break;
        };
        if index_column != column {
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

fn gist_order_match(
    items: &[OrderByEntry],
    index: &BoundIndexRelation,
) -> (
    Vec<crate::include::access::scankey::ScanKeyData>,
    Option<(usize, crate::include::access::relscan::ScanDirection)>,
) {
    if items.is_empty() || index.index_meta.am_oid != GIST_AM_OID {
        return (Vec::new(), None);
    }
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        if item.descending {
            return (Vec::new(), None);
        }
        let Some((column, proc_oid, argument)) = gist_order_item(item) else {
            return (Vec::new(), None);
        };
        let Some((index_pos, strategy)) =
            (0..index.index_meta.indkey.len()).find_map(|index_pos| {
                if simple_index_column(index, index_pos) != Some(column) {
                    return None;
                }
                let right_type_oid = value_type_oid(&argument);
                let left_type_oid = index
                    .index_meta
                    .opckeytype_oids
                    .get(index_pos)
                    .copied()
                    .filter(|oid| *oid != 0)
                    .or_else(|| {
                        index
                            .desc
                            .columns
                            .get(index_pos)
                            .map(|column| sql_type_oid(column.sql_type))
                    });
                let strategy = left_type_oid
                    .zip(right_type_oid)
                    .and_then(|(left_type_oid, right_type_oid)| {
                        gist_ordering_operator_oid(proc_oid, left_type_oid, right_type_oid)
                            .and_then(|operator_oid| {
                                index.index_meta.amop_ordering_strategy_for_operator(
                                    &index.desc,
                                    index_pos,
                                    operator_oid,
                                    Some(right_type_oid),
                                )
                            })
                    })
                    .or_else(|| {
                        index.index_meta.amop_ordering_strategy_for_proc(
                            &index.desc,
                            index_pos,
                            proc_oid,
                            right_type_oid,
                        )
                    })?;
                Some((index_pos, strategy))
            })
        else {
            return (Vec::new(), None);
        };
        keys.push(crate::include::access::scankey::ScanKeyData {
            attribute_number: (index_pos + 1) as i16,
            strategy,
            argument,
        });
    }
    (
        keys,
        Some((
            items.len(),
            crate::include::access::relscan::ScanDirection::Forward,
        )),
    )
}

fn gist_order_item(item: &OrderByEntry) -> Option<(usize, u32, Value)> {
    match strip_casts(&item.expr) {
        Expr::Func(func) if func.args.len() == 2 => {
            let left = strip_casts(&func.args[0]);
            let right = &func.args[1];
            if let (Some(column), Some(value)) = (expr_column_index(left), const_argument(right)) {
                return Some((column, func.funcid, value));
            }
            if let (Some(value), Some(column)) = (
                const_argument(&func.args[0]),
                expr_column_index(strip_casts(&func.args[1])),
            ) {
                return Some((column, commuted_function_proc_oid(func.funcid)?, value));
            }
            None
        }
        _ => None,
    }
}

fn expr_column_index(expr: &Expr) -> Option<usize> {
    match strip_casts(expr) {
        Expr::Var(var) if var.varlevelsup == 0 => attrno_index(var.varattno),
        _ => None,
    }
}
