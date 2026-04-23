use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use parking_lot::RwLock;

use crate::backend::access::heap::heapam::{
    HeapError, heap_delete_with_waiter, heap_fetch, heap_fetch_visible_with_txns,
    heap_insert_mvcc_with_cid, heap_scan_begin_visible, heap_scan_end, heap_scan_page_next_tuple,
    heap_scan_prepare_next_page, heap_update_with_waiter,
};
use crate::backend::access::heap::heaptoast::{
    StoredToastValue, cleanup_new_toast_value, delete_external_from_tuple,
};
use crate::backend::access::index::indexam;
use crate::backend::access::table::toast_helper::toast_tuple_values_for_write;
use crate::backend::access::transam::xact::CommandId;
use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::optimizer::{finalize_expr_subqueries, planner};
use crate::backend::parser::{
    AnalyzeStatement, BoundArraySubscript, BoundAssignment, BoundAssignmentTarget,
    BoundDeleteStatement, BoundDeleteTarget, BoundForeignKeyConstraint, BoundIndexRelation,
    BoundInsertSource, BoundInsertStatement, BoundMergeAction, BoundMergeStatement,
    BoundMergeWhenClause, BoundModifyRowSource, BoundOnConflictAction, BoundReferencedByForeignKey,
    BoundRelationConstraints, BoundUpdateStatement, BoundUpdateTarget, Catalog, CatalogLookup,
    DropTableStatement, ExplainStatement, ForeignKeyAction, MaintenanceTarget, MergeStatement,
    ParseError, SelectStatement, SqlType, SqlTypeKind, Statement, TruncateTableStatement,
    UpdateStatement, VacuumStatement, bind_create_table, bind_generated_expr,
    bind_referenced_by_foreign_keys, bind_relation_constraints, bind_scalar_expr_in_scope,
    bind_update,
};
use crate::backend::rewrite::RlsWriteCheck;
use crate::backend::rewrite::pg_rewrite_query;
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::StorageManager;
use crate::backend::utils::time::instant::Instant;
use crate::pgrust::database::TransactionWaiter;
use crate::pl::plpgsql::TriggerOperation;

use super::explain::{
    format_buffer_usage, format_explain_lines_with_costs, format_explain_plan_with_subplans,
    format_verbose_explain_plan_with_subplans, push_explain_line,
};
use super::partition::route_partition_target;
use super::trigger::RuntimeTriggers;
use super::upsert::execute_insert_on_conflict_rows;
use crate::backend::executor::exec_expr::{compile_predicate_with_decoder, eval_expr};
use crate::backend::executor::exec_tuples::CompiledTupleDecoder;
use crate::backend::executor::value_io::{coerce_assignment_value, encode_tuple_values};
use crate::backend::executor::{
    ExecError, ExecutorContext, Expr, StatementResult, ToastRelationRef,
    apply_jsonb_subscript_assignment, compare_order_values, create_query_desc, executor_start,
};
use crate::include::access::amapi::IndexUniqueCheck;
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::catalog::builtin_range_name_for_sql_type;
use crate::include::nodes::datum::{
    ArrayDimension, ArrayValue, RecordDescriptor, RecordValue, Value, array_value_from_value,
};
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::execnodes::*;
use crate::include::nodes::primnodes::{QueryColumn, TargetEntry};

fn finalize_bound_insert(
    mut stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
) -> BoundInsertStatement {
    let mut subplans = Vec::new();
    stmt.column_defaults = stmt
        .column_defaults
        .into_iter()
        .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
        .collect();
    stmt.source = match stmt.source {
        BoundInsertSource::Values(rows) => BoundInsertSource::Values(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
                        .collect()
                })
                .collect(),
        ),
        BoundInsertSource::DefaultValues(defaults) => BoundInsertSource::DefaultValues(
            defaults
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
                .collect(),
        ),
        BoundInsertSource::Select(query) => BoundInsertSource::Select(query),
    };
    stmt.on_conflict =
        stmt.on_conflict
            .map(|clause| crate::backend::parser::BoundOnConflictClause {
                arbiter_indexes: clause.arbiter_indexes,
                action: match clause.action {
                    BoundOnConflictAction::Nothing => BoundOnConflictAction::Nothing,
                    BoundOnConflictAction::Update {
                        assignments,
                        predicate,
                    } => BoundOnConflictAction::Update {
                        assignments: assignments
                            .into_iter()
                            .map(|assignment| BoundAssignment {
                                column_index: assignment.column_index,
                                expr: finalize_expr_subqueries(
                                    assignment.expr,
                                    catalog,
                                    &mut subplans,
                                ),
                                field_path: assignment.field_path,
                                target_sql_type: assignment.target_sql_type,
                                subscripts: assignment
                                    .subscripts
                                    .into_iter()
                                    .map(|subscript| BoundArraySubscript {
                                        is_slice: subscript.is_slice,
                                        lower: subscript.lower.map(|expr| {
                                            finalize_expr_subqueries(expr, catalog, &mut subplans)
                                        }),
                                        upper: subscript.upper.map(|expr| {
                                            finalize_expr_subqueries(expr, catalog, &mut subplans)
                                        }),
                                    })
                                    .collect(),
                            })
                            .collect(),
                        predicate: predicate
                            .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
                    },
                },
            });
    stmt.returning = stmt
        .returning
        .into_iter()
        .map(|target| TargetEntry {
            expr: finalize_expr_subqueries(target.expr, catalog, &mut subplans),
            ..target
        })
        .collect();
    stmt.rls_write_checks = stmt
        .rls_write_checks
        .into_iter()
        .map(|check| RlsWriteCheck {
            expr: finalize_expr_subqueries(check.expr, catalog, &mut subplans),
            ..check
        })
        .collect();
    stmt.subplans = subplans;
    stmt
}

pub(crate) fn finalize_bound_insert_stmt(
    stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
) -> BoundInsertStatement {
    finalize_bound_insert(stmt, catalog)
}

fn finalize_bound_update(
    mut stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
) -> BoundUpdateStatement {
    let mut subplans = stmt
        .input_plan
        .as_mut()
        .map(|plan| std::mem::take(&mut plan.subplans))
        .unwrap_or_default();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|target| crate::backend::parser::BoundUpdateTarget {
            assignments: target
                .assignments
                .into_iter()
                .map(|assignment| BoundAssignment {
                    column_index: assignment.column_index,
                    expr: finalize_expr_subqueries(assignment.expr, catalog, &mut subplans),
                    field_path: assignment.field_path,
                    target_sql_type: assignment.target_sql_type,
                    subscripts: assignment
                        .subscripts
                        .into_iter()
                        .map(|subscript| BoundArraySubscript {
                            is_slice: subscript.is_slice,
                            lower: subscript
                                .lower
                                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
                            upper: subscript
                                .upper
                                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
                        })
                        .collect(),
                })
                .collect(),
            predicate: target
                .predicate
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
            rls_write_checks: target
                .rls_write_checks
                .into_iter()
                .map(|check| RlsWriteCheck {
                    expr: finalize_expr_subqueries(check.expr, catalog, &mut subplans),
                    ..check
                })
                .collect(),
            ..target
        })
        .collect();
    stmt.returning = stmt
        .returning
        .into_iter()
        .map(|target| TargetEntry {
            expr: finalize_expr_subqueries(target.expr, catalog, &mut subplans),
            ..target
        })
        .collect();
    if let Some(input_plan) = &mut stmt.input_plan {
        input_plan.subplans = subplans.clone();
    }
    stmt.subplans = subplans;
    stmt
}

pub(crate) fn finalize_bound_update_stmt(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
) -> BoundUpdateStatement {
    finalize_bound_update(stmt, catalog)
}

fn finalize_bound_delete(
    mut stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
) -> BoundDeleteStatement {
    let mut subplans = Vec::new();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|target| crate::backend::parser::BoundDeleteTarget {
            predicate: target
                .predicate
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
            ..target
        })
        .collect();
    stmt.returning = stmt
        .returning
        .into_iter()
        .map(|target| TargetEntry {
            expr: finalize_expr_subqueries(target.expr, catalog, &mut subplans),
            ..target
        })
        .collect();
    stmt.subplans = subplans;
    stmt
}

pub(crate) fn finalize_bound_delete_stmt(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
) -> BoundDeleteStatement {
    finalize_bound_delete(stmt, catalog)
}

fn finalize_bound_merge(
    mut stmt: BoundMergeStatement,
    catalog: &dyn CatalogLookup,
) -> BoundMergeStatement {
    let mut subplans = std::mem::take(&mut stmt.input_plan.subplans);
    stmt.column_defaults = stmt
        .column_defaults
        .into_iter()
        .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
        .collect();
    stmt.when_clauses = stmt
        .when_clauses
        .into_iter()
        .map(|clause| BoundMergeWhenClause {
            condition: clause
                .condition
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
            action: match clause.action {
                BoundMergeAction::DoNothing => BoundMergeAction::DoNothing,
                BoundMergeAction::Delete => BoundMergeAction::Delete,
                BoundMergeAction::Update { assignments } => BoundMergeAction::Update {
                    assignments: assignments
                        .into_iter()
                        .map(|assignment| BoundAssignment {
                            expr: finalize_expr_subqueries(assignment.expr, catalog, &mut subplans),
                            ..assignment
                        })
                        .collect(),
                },
                BoundMergeAction::Insert {
                    target_columns,
                    values,
                } => BoundMergeAction::Insert {
                    target_columns,
                    values: values.map(|values: Vec<Expr>| {
                        values
                            .into_iter()
                            .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
                            .collect()
                    }),
                },
            },
            ..clause
        })
        .collect();
    stmt.input_plan.subplans = subplans;
    stmt
}

pub(crate) fn execute_explain(
    stmt: ExplainStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let ExplainStatement {
        analyze,
        buffers,
        costs,
        timing,
        verbose,
        statement,
    } = stmt;
    let statement = *statement;
    if let Statement::Update(update) = statement {
        return execute_explain_update(update, analyze, costs, verbose, catalog);
    }

    let explain_target = match statement {
        Statement::Select(select) => EitherExplainTarget::Select(select),
        Statement::Insert(_) => {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "EXPLAIN INSERT".into(),
            )));
        }
        Statement::Merge(merge) => EitherExplainTarget::Merge(merge),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "SELECT, UPDATE, or MERGE statement after EXPLAIN",
                actual: "unsupported statement".into(),
            }));
        }
    };

    ctx.pool.reset_usage_stats();
    let plan_start = Instant::now();
    let (query_desc, merge_target_name) = match explain_target {
        EitherExplainTarget::Select(select) => (
            create_query_desc(
                crate::backend::parser::pg_plan_query(&select, catalog)?,
                None,
            ),
            None,
        ),
        EitherExplainTarget::Merge(merge) => {
            let bound = crate::backend::parser::plan_merge(&merge, catalog)?;
            (
                create_query_desc(bound.input_plan, None),
                Some(bound.explain_target_name),
            )
        }
    };
    let planning_elapsed = plan_start.elapsed();
    let planning_buffer_stats = ctx.pool.usage_stats();
    let mut lines = Vec::new();
    if analyze && merge_target_name.is_some() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "EXPLAIN ANALYZE MERGE".into(),
        )));
    }
    if analyze {
        ctx.pool.reset_usage_stats();
        ctx.timed = timing;
        let saved_subplans =
            std::mem::replace(&mut ctx.subplans, query_desc.planned_stmt.subplans.clone());
        let exec_result: Result<(_, _, _), ExecError> = (|| {
            let mut state = executor_start(query_desc.planned_stmt.plan_tree.clone());
            let mut row_count: u64 = 0;
            let started_at = Instant::now();
            while let Some(_slot) = state.exec_proc_node(ctx)? {
                row_count += 1;
            }
            Ok((state, row_count, started_at.elapsed()))
        })();
        ctx.subplans = saved_subplans;
        ctx.timed = false;
        let execution_buffer_stats = ctx.pool.usage_stats();
        let (state, row_count, elapsed) = exec_result?;
        format_explain_lines_with_costs(state.as_ref(), 0, true, costs, &mut lines);
        if buffers {
            lines.push("Planning:".into());
            lines.push(format!("  {}", format_buffer_usage(planning_buffer_stats)));
        }
        lines.push(format!(
            "Planning Time: {:.3} ms",
            planning_elapsed.as_secs_f64() * 1000.0
        ));
        lines.push(format!(
            "Execution Time: {:.3} ms",
            elapsed.as_secs_f64() * 1000.0
        ));
        if buffers {
            lines.push(format_buffer_usage(execution_buffer_stats));
        }
        lines.push(format!("Result Rows: {}", row_count));
    } else {
        let plan_tree = query_desc.planned_stmt.plan_tree;
        let subplans = query_desc.planned_stmt.subplans;
        if let Some(target_name) = merge_target_name {
            let state = executor_start(plan_tree);
            push_explain_line(
                &format!("Merge on {target_name}"),
                state.plan_info(),
                costs,
                &mut lines,
            );
            format_explain_lines_with_costs(state.as_ref(), 1, false, costs, &mut lines);
        } else {
            if verbose {
                format_verbose_explain_plan_with_subplans(
                    &plan_tree, &subplans, 0, costs, &mut lines,
                );
            } else {
                format_explain_plan_with_subplans(&plan_tree, &subplans, 0, costs, &mut lines);
            }
        }
    }

    Ok(StatementResult::Query {
        columns: vec![QueryColumn::text("QUERY PLAN")],
        column_names: vec!["QUERY PLAN".into()],
        rows: lines
            .into_iter()
            .map(|line| vec![Value::Text(line.into())])
            .collect(),
    })
}

enum EitherExplainTarget {
    Select(SelectStatement),
    Merge(MergeStatement),
}

fn execute_explain_update(
    stmt: UpdateStatement,
    analyze: bool,
    costs: bool,
    verbose: bool,
    catalog: &dyn CatalogLookup,
) -> Result<StatementResult, ExecError> {
    if analyze {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "EXPLAIN ANALYZE UPDATE".into(),
        )));
    }

    let bound = finalize_bound_update_stmt(bind_update(&stmt, catalog)?, catalog);
    let lines = explain_update_lines(&stmt, &bound, costs, verbose);
    Ok(StatementResult::Query {
        columns: vec![QueryColumn::text("QUERY PLAN")],
        column_names: vec!["QUERY PLAN".into()],
        rows: lines
            .into_iter()
            .map(|line| vec![Value::Text(line.into())])
            .collect(),
    })
}

fn explain_update_lines(
    stmt: &UpdateStatement,
    bound: &BoundUpdateStatement,
    show_costs: bool,
    verbose: bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    push_explain_line(
        &format!(
            "Update on {}",
            explain_update_target_name(&bound.explain_target_name, verbose)
        ),
        crate::include::nodes::plannodes::PlanEstimate::default(),
        show_costs,
        &mut lines,
    );
    if verbose && !bound.returning.is_empty() {
        lines.push(format!(
            "  Output: {}",
            render_update_returning_targets(&bound.target_relation_name, &bound.returning)
        ));
    }
    if let Some(input_plan) = &bound.input_plan {
        format_explain_plan_with_subplans(
            &input_plan.plan_tree,
            &input_plan.subplans,
            1,
            show_costs,
            &mut lines,
        );
        return lines;
    }

    let child_targets = bound
        .targets
        .iter()
        .filter(|target| target.relation_name != stmt.table_name)
        .collect::<Vec<_>>();
    if let Some(target) = explain_update_scan_target(&stmt.table_name, &bound.targets) {
        let alias = child_targets
            .iter()
            .position(|candidate| candidate.relation_oid == target.relation_oid)
            .map(|index| format!("{}_{}", stmt.table_name, index + 1));
        if let Some(alias) = &alias {
            lines.push(format!("  Update on {} {}", target.relation_name, alias));
        }
        push_explain_line(
            "  ->  Result",
            crate::include::nodes::plannodes::PlanEstimate::default(),
            show_costs,
            &mut lines,
        );
        if is_const_false(target.predicate.as_ref()) {
            if verbose {
                lines.push(format!(
                    "        Output: {}",
                    render_update_projection_output(&stmt.table_name, target)
                ));
            }
            lines.push("        One-Time Filter: false".into());
            return lines;
        }
        push_explain_line(
            &format!(
                "        ->  {}",
                explain_update_scan_label(target, alias.as_deref())
            ),
            crate::include::nodes::plannodes::PlanEstimate::default(),
            show_costs,
            &mut lines,
        );
        if let Some(index_cond) = explain_update_index_cond(target) {
            lines.push(format!("              Index Cond: {index_cond}"));
        } else if let Some(predicate) = &target.predicate {
            lines.push(format!(
                "              Filter: {}",
                crate::backend::executor::render_explain_expr(
                    predicate,
                    &target
                        .desc
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect::<Vec<_>>(),
                )
            ));
        }
    } else {
        push_explain_line(
            "  ->  Result",
            crate::include::nodes::plannodes::PlanEstimate::default(),
            show_costs,
            &mut lines,
        );
        lines.push("        One-Time Filter: false".into());
    }
    lines
}

fn explain_update_scan_target<'a>(
    base_name: &str,
    targets: &'a [BoundUpdateTarget],
) -> Option<&'a BoundUpdateTarget> {
    targets
        .iter()
        .find(|target| {
            target.relation_name != base_name && !is_const_false(target.predicate.as_ref())
        })
        .or_else(|| {
            targets
                .iter()
                .find(|target| !is_const_false(target.predicate.as_ref()))
        })
        .or_else(|| targets.first())
}

fn explain_update_target_name(table_name: &str, verbose: bool) -> String {
    if !verbose || table_name.contains('.') {
        return table_name.to_string();
    }
    format!("public.{table_name}")
}

fn render_update_returning_targets(target_name: &str, returning: &[TargetEntry]) -> String {
    returning
        .iter()
        .map(|target| format!("{target_name}.{}", target.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_update_projection_output(target_name: &str, target: &BoundUpdateTarget) -> String {
    let column_names = target
        .desc
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let assignment_outputs = target
        .assignments
        .iter()
        .map(|assignment| {
            crate::backend::executor::render_explain_projection_expr_with_qualifier(
                &assignment.expr,
                Some(target_name),
                &column_names,
            )
        })
        .collect::<Vec<_>>();
    let mut outputs = assignment_outputs;
    outputs.push("NULL::oid".into());
    outputs.push("NULL::tid".into());
    outputs.join(", ")
}

fn explain_update_scan_label(target: &BoundUpdateTarget, alias: Option<&str>) -> String {
    match &target.row_source {
        BoundModifyRowSource::Heap => match alias {
            Some(alias) => format!("Seq Scan on {} {alias}", target.relation_name),
            None => format!("Seq Scan on {}", target.relation_name),
        },
        BoundModifyRowSource::Index { index, .. } => match alias {
            Some(alias) => format!(
                "Index Scan using {} on {} {alias}",
                index.name, target.relation_name
            ),
            None => format!(
                "Index Scan using {} on {}",
                index.name, target.relation_name
            ),
        },
    }
}

fn explain_update_index_cond(target: &BoundUpdateTarget) -> Option<String> {
    let BoundModifyRowSource::Index { index, keys } = &target.row_source else {
        return None;
    };
    let rendered = keys
        .iter()
        .filter_map(|key| {
            let index_attno = usize::try_from(key.attribute_number).ok()?.checked_sub(1)?;
            let heap_attno = usize::try_from(*index.index_meta.indkey.get(index_attno)?)
                .ok()?
                .checked_sub(1)?;
            let column_name = target.desc.columns.get(heap_attno)?.name.clone();
            Some(format!(
                "({column_name} {} {})",
                explain_strategy_operator(key.strategy),
                render_explain_index_value(&key.argument)
            ))
        })
        .collect::<Vec<_>>();
    (!rendered.is_empty()).then(|| format!("({})", rendered.join(" AND ")))
}

fn render_explain_index_value(value: &Value) -> String {
    crate::backend::executor::render_explain_expr(&Expr::Const(value.clone()), &[])
}

fn explain_strategy_operator(strategy: u16) -> &'static str {
    match strategy {
        1 => "<",
        2 => "<=",
        3 => "=",
        4 => ">=",
        5 => ">",
        _ => "=",
    }
}

fn is_const_false(expr: Option<&Expr>) -> bool {
    matches!(expr, Some(Expr::Const(Value::Bool(false))))
}

fn validate_maintenance_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if entry.relkind == 'r' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        for column in &target.columns {
            if !entry
                .desc
                .columns
                .iter()
                .any(|desc| desc.name.eq_ignore_ascii_case(column))
            {
                return Err(ExecError::Parse(ParseError::UnknownColumn(column.clone())));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteUpdatedRowResult {
    Updated(ItemPointerData),
    TupleUpdated(ItemPointerData),
    AlreadyModified,
}

pub(crate) fn build_index_insert_context(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    _heap_desc: &RelationDesc,
    index: &BoundIndexRelation,
    key_values: Vec<Value>,
    heap_tid: ItemPointerData,
    ctx: &ExecutorContext,
) -> crate::include::access::amapi::IndexInsertContext {
    let mut index_meta = index.index_meta.clone();
    index_meta.indkey = (1..=key_values.len())
        .map(|attnum| attnum as i16)
        .collect::<Vec<_>>();
    index_meta.indexprs = None;
    crate::include::access::amapi::IndexInsertContext {
        pool: ctx.pool.clone(),
        txns: ctx.txns.clone(),
        txn_waiter: ctx.txn_waiter.clone(),
        client_id: ctx.client_id,
        interrupts: ctx.interrupts.clone(),
        snapshot: ctx.snapshot.clone(),
        heap_relation: heap_rel,
        heap_desc: index.desc.clone(),
        index_relation: index.rel,
        index_name: index.name.clone(),
        index_desc: index.desc.clone(),
        index_meta,
        default_toast_compression: ctx.default_toast_compression,
        values: key_values,
        heap_tid,
        unique_check: if index.index_meta.indisunique {
            IndexUniqueCheck::Yes
        } else {
            IndexUniqueCheck::No
        },
    }
}

fn map_index_insert_error(err: crate::backend::catalog::CatalogError) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(constraint) => {
            ExecError::UniqueViolation {
                constraint,
                detail: None,
            }
        }
        crate::backend::catalog::CatalogError::Io(message)
            if message.starts_with("index row size ") =>
        {
            ExecError::DetailedError {
                message,
                detail: None,
                hint: Some("Values larger than 1/3 of a buffer page cannot be indexed.".into()),
                sqlstate: "54000",
            }
        }
        crate::backend::catalog::CatalogError::Interrupted(reason) => {
            ExecError::Interrupted(reason)
        }
        other => ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index insertion",
            actual: format!("{other:?}"),
        }),
    }
}

pub(crate) fn insert_index_key_values(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    heap_desc: &RelationDesc,
    index: &BoundIndexRelation,
    key_values: Vec<Value>,
    heap_tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let insert_ctx =
        build_index_insert_context(heap_rel, heap_desc, index, key_values, heap_tid, ctx);
    indexam::index_insert_stub(&insert_ctx, index.index_meta.am_oid).map_err(|err| match err {
        crate::backend::catalog::CatalogError::UniqueViolation(constraint) => {
            ExecError::UniqueViolation {
                constraint,
                detail: Some(
                    crate::backend::executor::value_io::format_unique_key_detail(
                        &insert_ctx.index_desc.columns,
                        &insert_ctx.values,
                    ),
                ),
            }
        }
        other => map_index_insert_error(other),
    })?;
    Ok(())
}

pub(crate) fn row_matches_index_predicate(
    index: &BoundIndexRelation,
    values: &[Value],
    heap_tid: Option<ItemPointerData>,
    relation_oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(predicate) = index.index_predicate.as_ref() else {
        return Ok(true);
    };
    let mut slot =
        TupleSlot::virtual_row_with_metadata(values.to_vec(), heap_tid, Some(relation_oid));
    match eval_expr(predicate, &mut slot, ctx)? {
        Value::Bool(value) => Ok(value),
        Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

pub(crate) fn insert_index_entry_for_row(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    heap_desc: &RelationDesc,
    index: &BoundIndexRelation,
    values: &[Value],
    heap_tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if !row_matches_index_predicate(
        index,
        values,
        Some(heap_tid),
        index.index_meta.indrelid,
        ctx,
    )? {
        return Ok(());
    }
    let key_values = index_key_values_for_row(index, heap_desc, values, ctx)?;
    insert_index_key_values(heap_rel, heap_desc, index, key_values, heap_tid, ctx)
}

pub(crate) fn maintain_indexes_for_row(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    heap_desc: &RelationDesc,
    indexes: &[BoundIndexRelation],
    values: &[Value],
    heap_tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
        for index in indexes
            .iter()
            .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
        {
            insert_index_entry_for_row(heap_rel, heap_desc, index, values, heap_tid, ctx)?;
        }
        Ok(())
    })
}

pub(crate) fn index_key_values_for_row(
    index: &BoundIndexRelation,
    heap_desc: &RelationDesc,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
        let mut slot = TupleSlot::virtual_row(values.to_vec());
        let fallback_exprs;
        let mut exprs = if !index.index_exprs.is_empty() {
            index.index_exprs.iter()
        } else if index.index_meta.indexprs.is_some() {
            let catalog = ctx.catalog.as_ref().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "catalog for expression index evaluation",
                    actual: "missing visible catalog".into(),
                })
            })?;
            fallback_exprs =
                crate::backend::parser::bind_index_exprs(&index.index_meta, heap_desc, catalog)
                    .map_err(ExecError::Parse)?;
            fallback_exprs.iter()
        } else {
            [].iter()
        };

        let mut key_values = Vec::with_capacity(index.index_meta.indkey.len());
        for attnum in &index.index_meta.indkey {
            if *attnum > 0 {
                let idx = attnum.saturating_sub(1) as usize;
                key_values.push(values.get(idx).cloned().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index key column",
                        actual: "index key attnum out of range".into(),
                    })
                })?);
            } else {
                let expr = exprs.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index expression",
                        actual: "missing expression for index key".into(),
                    })
                })?;
                key_values.push(eval_expr(expr, &mut slot, ctx)?);
            }
        }
        Ok(key_values)
    })
}

pub(crate) fn slot_toast_context(
    toast: Option<ToastRelationRef>,
    ctx: &ExecutorContext,
) -> Option<crate::include::nodes::execnodes::ToastFetchContext> {
    toast.map(
        |relation| crate::include::nodes::execnodes::ToastFetchContext {
            relation,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        },
    )
}

pub(crate) fn toast_tuple_for_write(
    desc: &RelationDesc,
    values: &[Value],
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(HeapTuple, Vec<StoredToastValue>), ExecError> {
    let mut tuple_values = encode_tuple_values(desc, values)?;
    let attr_descs = desc.attribute_descs();
    let Some(toast) = toast else {
        let tuple = HeapTuple::from_values(&attr_descs, &tuple_values)?;
        return Ok((tuple, Vec::new()));
    };
    let stored =
        toast_tuple_values_for_write(desc, &mut tuple_values, toast, toast_index, ctx, xid, cid)?;
    let tuple = HeapTuple::from_values(&attr_descs, &tuple_values)?;
    Ok((tuple, stored))
}

pub(crate) fn cleanup_toast_attempt(
    toast: Option<ToastRelationRef>,
    toasted: &[StoredToastValue],
    ctx: &ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let Some(toast) = toast else {
        return Ok(());
    };
    for value in toasted {
        cleanup_new_toast_value(ctx, toast, &value.chunk_tids, xid)?;
    }
    Ok(())
}

pub(crate) fn write_insert_heap_row(
    relation_name: &str,
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &BoundRelationConstraints,
    rls_write_checks: &[RlsWriteCheck],
    values: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<ItemPointerData, ExecError> {
    crate::backend::executor::enforce_row_security_write_checks(
        relation_name,
        desc,
        rls_write_checks,
        values,
        ctx,
    )?;
    crate::backend::executor::enforce_relation_constraints(
        relation_name,
        desc,
        relation_constraints,
        values,
        ctx,
    )?;
    crate::backend::executor::enforce_outbound_foreign_keys(
        relation_name,
        &relation_constraints.foreign_keys,
        None,
        values,
        ctx,
    )?;
    let (tuple, _toasted) = toast_tuple_for_write(desc, values, toast, toast_index, ctx, xid, cid)?;
    heap_insert_mvcc_with_cid(&*ctx.pool, ctx.client_id, rel, xid, cid, &tuple).map_err(Into::into)
}

pub(crate) fn rollback_inserted_row(
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: &RelationDesc,
    heap_tid: ItemPointerData,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let tuple = if toast.is_some() {
        Some(heap_fetch(&*ctx.pool, ctx.client_id, rel, heap_tid)?)
    } else {
        None
    };
    let delete_snapshot = ctx.txns.read().snapshot(xid)?;
    match heap_delete_with_waiter(
        &*ctx.pool,
        ctx.client_id,
        rel,
        &ctx.txns,
        xid,
        heap_tid,
        &delete_snapshot,
        None,
    ) {
        Ok(()) | Err(HeapError::TupleAlreadyModified(_)) => {}
        Err(err) => return Err(err.into()),
    }
    if let (Some(toast), Some(tuple)) = (toast, tuple.as_ref()) {
        delete_external_from_tuple(ctx, toast, desc, tuple, xid)?;
    }
    Ok(())
}

pub(crate) fn write_updated_row(
    relation_name: &str,
    rel: crate::backend::storage::smgr::RelFileLocator,
    _relation_oid: u32,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &BoundRelationConstraints,
    rls_write_checks: &[RlsWriteCheck],
    referenced_by_foreign_keys: &[BoundReferencedByForeignKey],
    indexes: &[BoundIndexRelation],
    current_tid: ItemPointerData,
    current_old_values: &[Value],
    current_values: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<WriteUpdatedRowResult, ExecError> {
    let mut current_values = current_values.to_vec();
    materialize_generated_columns(desc, &mut current_values, ctx)?;
    let old_tuple = if toast.is_some() {
        Some(heap_fetch(&*ctx.pool, ctx.client_id, rel, current_tid)?)
    } else {
        None
    };
    crate::backend::executor::enforce_row_security_write_checks(
        relation_name,
        desc,
        rls_write_checks,
        &current_values,
        ctx,
    )?;
    crate::backend::executor::enforce_relation_constraints(
        relation_name,
        desc,
        relation_constraints,
        &current_values,
        ctx,
    )?;
    crate::backend::executor::enforce_outbound_foreign_keys(
        relation_name,
        &relation_constraints.foreign_keys,
        Some(current_old_values),
        &current_values,
        ctx,
    )?;
    let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_update(
        relation_name,
        referenced_by_foreign_keys,
        current_old_values,
        &current_values,
        ctx,
        xid,
        cid,
        waiter,
    )?;
    let (replacement, toasted) =
        toast_tuple_for_write(desc, &current_values, toast, toast_index, ctx, xid, cid)?;
    match heap_update_with_waiter(
        &*ctx.pool,
        ctx.client_id,
        rel,
        &ctx.txns,
        xid,
        cid,
        current_tid,
        &replacement,
        waiter,
    ) {
        Ok(new_tid) => {
            if let (Some(toast), Some(old_tuple)) = (toast, old_tuple.as_ref()) {
                delete_external_from_tuple(ctx, toast, desc, old_tuple, xid)?;
            }
            maintain_indexes_for_row(rel, desc, indexes, &current_values, new_tid, ctx)?;
            validate_pending_set_default_rechecks(pending_set_default_rechecks, ctx)?;
            Ok(WriteUpdatedRowResult::Updated(new_tid))
        }
        Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
            cleanup_toast_attempt(toast, &toasted, ctx, xid)?;
            Ok(WriteUpdatedRowResult::TupleUpdated(new_ctid))
        }
        Err(HeapError::TupleAlreadyModified(_)) => {
            cleanup_toast_attempt(toast, &toasted, ctx, xid)?;
            Ok(WriteUpdatedRowResult::AlreadyModified)
        }
        Err(err) => {
            cleanup_toast_attempt(toast, &toasted, ctx, xid)?;
            Err(err.into())
        }
    }
}

pub(crate) fn reinitialize_index_relation(
    index: &BoundIndexRelation,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let _ = ctx.pool.invalidate_relation(index.rel);
    ctx.pool
        .with_storage_mut(|s| s.smgr.truncate(index.rel, ForkNumber::Main, 0))
        .map_err(HeapError::Storage)?;
    crate::backend::access::index::indexam::index_build_empty_stub(
        &crate::include::access::amapi::IndexBuildEmptyContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            xid,
            index_relation: index.rel,
            index_desc: index.desc.clone(),
            index_meta: index.index_meta.clone(),
        },
        index.index_meta.am_oid,
    )
    .map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index reinitialization",
            actual: format!("{err:?}"),
        })
    })?;
    Ok(())
}

pub(crate) fn collect_matching_rows_heap(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    toast: Option<ToastRelationRef>,
    predicate: Option<&Expr>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, rel, ctx.snapshot.clone())?;

    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = predicate.map(|p| compile_predicate_with_decoder(p, &decoder));

    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));
    slot.toast = slot_toast_context(toast, ctx);
    let mut rows = Vec::new();

    loop {
        ctx.check_for_interrupts()?;
        let next: Result<Option<usize>, ExecError> =
            heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, &mut scan);
        let Some(buffer_id) = next? else {
            break;
        };

        let page =
            unsafe { ctx.pool.page_unlocked(buffer_id) }.expect("pinned buffer must be valid");

        let pin = scan
            .pinned_buffer_rc()
            .expect("buffer must be pinned after prepare_next_page");

        let mut page_rows = Vec::new();
        while let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, &mut scan) {
            ctx.check_for_interrupts()?;
            slot.kind = SlotKind::BufferHeapTuple {
                desc: Rc::clone(&desc),
                attr_descs: Rc::clone(&attr_descs),
                tid,
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin: Rc::clone(&pin),
            };
            slot.tts_nvalid = 0;
            slot.tts_values.clear();
            slot.decode_offset = 0;
            slot.values()?;
            Value::materialize_all(&mut slot.tts_values);
            page_rows.push((tid, slot.tts_values.clone()));
        }
        drop(pin);

        for (tid, values) in page_rows {
            ctx.check_for_interrupts()?;
            let mut slot = TupleSlot::virtual_row(values.clone());
            if let Some(q) = &qual {
                if !q(&mut slot, ctx)? {
                    continue;
                }
            }
            rows.push((tid, values));
        }
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(rows)
}

fn collect_matching_rows_index(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    toast: Option<ToastRelationRef>,
    index: &BoundIndexRelation,
    keys: &[crate::include::access::scankey::ScanKeyData],
    predicate: Option<&Expr>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = predicate.map(|p| compile_predicate_with_decoder(p, &decoder));

    let begin = crate::include::access::amapi::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: rel,
        index_relation: index.rel,
        index_desc: index.desc.clone(),
        index_meta: index.index_meta.clone(),
        key_data: keys.to_vec(),
        order_by_data: Vec::new(),
        direction: crate::include::access::relscan::ScanDirection::Forward,
        want_itup: false,
    };
    let mut scan = indexam::index_beginscan(&begin, index.index_meta.am_oid).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index access method begin scan",
            actual: format!("{err:?}"),
        })
    })?;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();

    loop {
        ctx.check_for_interrupts()?;
        let has_tuple =
            indexam::index_getnext(&mut scan, index.index_meta.am_oid).map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method tuple",
                    actual: format!("{err:?}"),
                })
            })?;
        if !has_tuple {
            break;
        }
        let tid = scan.xs_heaptid.expect("index scan tuple must set heap tid");
        if !seen.insert(tid) {
            continue;
        }
        let Some(tuple) = heap_fetch_visible_with_txns(
            &ctx.pool,
            ctx.client_id,
            rel,
            tid,
            &ctx.txns,
            &ctx.snapshot,
        )?
        else {
            continue;
        };
        let mut slot =
            TupleSlot::from_heap_tuple(Rc::clone(&desc), Rc::clone(&attr_descs), tid, tuple);
        slot.toast = slot_toast_context(toast, ctx);
        if let Some(q) = &qual {
            if !q(&mut slot, ctx)? {
                continue;
            }
        }
        rows.push((tid, slot.into_values()?));
    }

    indexam::index_endscan(scan, index.index_meta.am_oid).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index access method end scan",
            actual: format!("{err:?}"),
        })
    })?;
    Ok(rows)
}

fn first_toast_index(
    catalog: &dyn CatalogLookup,
    toast: Option<ToastRelationRef>,
) -> Option<BoundIndexRelation> {
    let toast = toast?;
    catalog
        .index_relations_for_heap(toast.relation_oid)
        .into_iter()
        .next()
}

fn build_equality_scan_keys(
    key_values: &[Value],
) -> Vec<crate::include::access::scankey::ScanKeyData> {
    key_values
        .iter()
        .enumerate()
        .map(
            |(index, value)| crate::include::access::scankey::ScanKeyData {
                attribute_number: index.saturating_add(1) as i16,
                strategy: 3,
                argument: value.to_owned_value(),
            },
        )
        .collect()
}

fn row_matches_key(values: &[Value], key_indexes: &[usize], key_values: &[Value]) -> bool {
    key_indexes.iter().zip(key_values).all(|(index, expected)| {
        values.get(*index).is_some_and(|actual| {
            compare_order_values(actual, expected, None, None, false)
                .expect("foreign-key key comparisons use implicit default collation")
                == std::cmp::Ordering::Equal
        })
    })
}

fn key_columns_changed(previous_values: &[Value], values: &[Value], indexes: &[usize]) -> bool {
    indexes.iter().any(|index| {
        let previous = previous_values.get(*index).unwrap_or(&Value::Null);
        let current = values.get(*index).unwrap_or(&Value::Null);
        compare_order_values(previous, current, None, None, false)
            .expect("foreign-key key comparisons use implicit default collation")
            != std::cmp::Ordering::Equal
    })
}

fn relation_write_state_for_foreign_key(
    constraint: &BoundReferencedByForeignKey,
    ctx: &ExecutorContext,
) -> Result<
    (
        BoundRelationConstraints,
        Vec<BoundReferencedByForeignKey>,
        Vec<BoundIndexRelation>,
        Option<BoundIndexRelation>,
    ),
    ExecError,
> {
    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "foreign key action failed".into(),
            detail: Some("executor context missing visible catalog".into()),
            hint: None,
            sqlstate: "XX000",
        })?;
    let constraints = BoundRelationConstraints {
        not_nulls: constraint
            .child_desc
            .columns
            .iter()
            .enumerate()
            .filter_map(|(column_index, column)| {
                column
                    .not_null_constraint_name
                    .as_ref()
                    .map(
                        |constraint_name| crate::backend::parser::BoundNotNullConstraint {
                            column_index,
                            constraint_name: constraint_name.clone(),
                        },
                    )
            })
            .collect(),
        checks: Vec::new(),
        foreign_keys: Vec::new(),
    };
    let referenced_by = bind_referenced_by_foreign_keys(
        constraint.child_relation_oid,
        &constraint.child_desc,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    Ok((
        constraints,
        referenced_by,
        catalog.index_relations_for_heap(constraint.child_relation_oid),
        first_toast_index(catalog, constraint.child_toast),
    ))
}

fn collect_referencing_rows(
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    if key_values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Vec::new());
    }
    if let Some(index) = &constraint.child_index {
        return collect_matching_rows_index(
            constraint.child_rel,
            &constraint.child_desc,
            constraint.child_toast,
            index,
            &build_equality_scan_keys(key_values),
            None,
            ctx,
        );
    }
    let rows = collect_matching_rows_heap(
        constraint.child_rel,
        &constraint.child_desc,
        constraint.child_toast,
        None,
        ctx,
    )?;
    Ok(rows
        .into_iter()
        .filter(|(_, values)| row_matches_key(values, &constraint.child_column_indexes, key_values))
        .collect())
}

fn evaluate_default_value(
    desc: &RelationDesc,
    column_index: usize,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(default_sql) = desc.columns[column_index].default_expr.as_deref() else {
        return Ok(Value::Null);
    };
    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "foreign key action failed".into(),
            detail: Some("executor context missing visible catalog".into()),
            hint: None,
            sqlstate: "XX000",
        })?;
    let parsed = crate::backend::parser::parse_expr(default_sql).map_err(ExecError::Parse)?;
    let (bound, _) = bind_scalar_expr_in_scope(&parsed, &[], catalog).map_err(ExecError::Parse)?;
    let mut slot = TupleSlot::virtual_row(vec![Value::Null; desc.columns.len()]);
    eval_expr(&bound, &mut slot, ctx)
}

pub(super) fn materialize_generated_columns(
    desc: &RelationDesc,
    values: &mut [Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if !desc.columns.iter().any(|column| column.generated.is_some()) {
        return Ok(());
    }
    let generated_exprs = {
        let catalog = ctx
            .catalog
            .as_ref()
            .ok_or_else(|| ExecError::DetailedError {
                message: "generated column evaluation failed".into(),
                detail: Some("executor context missing visible catalog".into()),
                hint: None,
                sqlstate: "XX000",
            })?;
        desc.columns
            .iter()
            .enumerate()
            .filter_map(|(column_index, column)| match column.generated {
                Some(crate::backend::parser::ColumnGeneratedKind::Stored) => Some(
                    bind_generated_expr(desc, column_index, catalog)
                        .map_err(ExecError::Parse)
                        .and_then(|expr| {
                            expr.ok_or_else(|| {
                                ExecError::Parse(ParseError::InvalidTableDefinition(format!(
                                    "generation expression missing for column \"{}\"",
                                    column.name
                                )))
                            })
                        })
                        .map(|expr| (column_index, expr)),
                ),
                _ => None,
            })
            .collect::<Result<Vec<_>, ExecError>>()?
    };
    let mut slot = TupleSlot::virtual_row(values.to_vec());
    for (column_index, expr) in generated_exprs {
        values[column_index] = eval_expr(&expr, &mut slot, ctx)?.to_owned_value();
    }
    for (column_index, column) in desc.columns.iter().enumerate() {
        match column.generated {
            Some(crate::backend::parser::ColumnGeneratedKind::Virtual) => {
                values[column_index] = Value::Null;
            }
            Some(crate::backend::parser::ColumnGeneratedKind::Stored) => {}
            None => {}
        }
    }
    Ok(())
}

struct AppliedSetDefaultAction {
    outbound_constraint: BoundForeignKeyConstraint,
    updated_rows: Vec<Vec<Value>>,
}

struct PendingSetDefaultRecheck {
    relation_name: String,
    inbound_constraint: BoundReferencedByForeignKey,
    old_key_values: Vec<Value>,
    outbound_constraint: BoundForeignKeyConstraint,
    updated_rows: Vec<Vec<Value>>,
}

fn validate_pending_set_default_rechecks(
    pending: Vec<PendingSetDefaultRecheck>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for recheck in pending {
        crate::backend::executor::enforce_inbound_foreign_key_reference(
            &recheck.relation_name,
            &recheck.inbound_constraint,
            &recheck.old_key_values,
            ctx,
        )?;
        for updated_values in &recheck.updated_rows {
            crate::backend::executor::enforce_outbound_foreign_keys(
                &recheck.outbound_constraint.relation_name,
                std::slice::from_ref(&recheck.outbound_constraint),
                None,
                updated_values,
                ctx,
            )?;
        }
    }
    Ok(())
}

fn apply_referential_action_to_rows(
    constraint: &BoundReferencedByForeignKey,
    action: ForeignKeyAction,
    key_values: &[Value],
    replacement_key_values: Option<&[Value]>,
    delete_set_column_indexes: Option<&[usize]>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<Option<AppliedSetDefaultAction>, ExecError> {
    let rows = collect_referencing_rows(constraint, key_values, ctx)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "foreign key action failed".into(),
            detail: Some("executor context missing visible catalog".into()),
            hint: None,
            sqlstate: "XX000",
        })?;
    let (relation_constraints, referenced_by_foreign_keys, indexes, toast_index) =
        relation_write_state_for_foreign_key(constraint, ctx)?;
    let full_relation_constraints = matches!(action, ForeignKeyAction::SetDefault)
        .then(|| {
            bind_relation_constraints(
                Some(&constraint.child_relation_name),
                constraint.child_relation_oid,
                &constraint.child_desc,
                catalog,
            )
            .map_err(ExecError::Parse)
        })
        .transpose()?;
    let outbound_constraint = full_relation_constraints.as_ref().and_then(|constraints| {
        constraints
            .foreign_keys
            .iter()
            .find(|foreign_key| foreign_key.constraint_oid == constraint.constraint_oid)
            .cloned()
    });
    let sibling_outbound_constraints = full_relation_constraints.as_ref().map(|constraints| {
        constraints
            .foreign_keys
            .iter()
            .filter(|foreign_key| foreign_key.constraint_oid != constraint.constraint_oid)
            .cloned()
            .collect::<Vec<_>>()
    });
    let triggers = RuntimeTriggers::load(
        catalog,
        constraint.child_relation_oid,
        &constraint.child_relation_name,
        &constraint.child_desc,
        TriggerOperation::Update,
        &[],
        ctx.session_replication_role,
    )?;
    triggers.before_statement(ctx)?;
    let mut transition_capture = triggers.new_transition_capture();
    let mut updated_rows = Vec::new();
    for (tid, current_values) in rows {
        ctx.check_for_interrupts()?;
        match action {
            ForeignKeyAction::Cascade
            | ForeignKeyAction::SetNull
            | ForeignKeyAction::SetDefault => {
                let mut updated_values = current_values.clone();
                match action {
                    ForeignKeyAction::Cascade => {
                        for (position, column_index) in
                            constraint.child_column_indexes.iter().enumerate()
                        {
                            updated_values[*column_index] = replacement_key_values
                                .and_then(|values| values.get(position))
                                .cloned()
                                .unwrap_or(Value::Null)
                                .to_owned_value();
                        }
                    }
                    ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                        let target_columns =
                            delete_set_column_indexes.unwrap_or(&constraint.child_column_indexes);
                        for column_index in target_columns {
                            updated_values[*column_index] = match action {
                                ForeignKeyAction::SetNull => Value::Null,
                                ForeignKeyAction::SetDefault => evaluate_default_value(
                                    &constraint.child_desc,
                                    *column_index,
                                    ctx,
                                )?,
                                ForeignKeyAction::NoAction
                                | ForeignKeyAction::Restrict
                                | ForeignKeyAction::Cascade => unreachable!(),
                            };
                        }
                    }
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => unreachable!(),
                }
                let Some(updated_values) =
                    triggers.before_row_update(&current_values, updated_values, ctx)?
                else {
                    continue;
                };
                if let Some(full_relation_constraints) = full_relation_constraints.as_ref() {
                    crate::backend::executor::enforce_relation_constraints(
                        &constraint.child_relation_name,
                        &constraint.child_desc,
                        full_relation_constraints,
                        &updated_values,
                        ctx,
                    )?;
                    crate::backend::executor::enforce_outbound_foreign_keys(
                        &constraint.child_relation_name,
                        sibling_outbound_constraints
                            .as_deref()
                            .expect("sibling outbound constraints must be present"),
                        Some(&current_values),
                        &updated_values,
                        ctx,
                    )?;
                }
                let _ = write_updated_row(
                    &constraint.child_relation_name,
                    constraint.child_rel,
                    constraint.child_relation_oid,
                    constraint.child_toast,
                    toast_index.as_ref(),
                    &constraint.child_desc,
                    &relation_constraints,
                    &[],
                    &referenced_by_foreign_keys,
                    &indexes,
                    tid,
                    &current_values,
                    &updated_values,
                    ctx,
                    xid,
                    cid,
                    waiter,
                )?;
                triggers.capture_update_row(
                    &mut transition_capture,
                    &current_values,
                    &updated_values,
                );
                triggers.after_row_update(&current_values, &updated_values, ctx)?;
                if matches!(action, ForeignKeyAction::SetDefault) {
                    updated_rows.push(updated_values);
                }
            }
            ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => unreachable!(),
        }
    }
    triggers.after_transition_rows(&transition_capture, ctx)?;
    triggers.after_statement(Some(&transition_capture), ctx)?;
    if matches!(action, ForeignKeyAction::SetDefault) {
        let outbound_constraint = outbound_constraint.ok_or_else(|| ExecError::DetailedError {
            message: "foreign key action failed".into(),
            detail: Some(format!(
                "could not bind outbound foreign key constraint {} on relation \"{}\"",
                constraint.constraint_name, constraint.child_relation_name
            )),
            hint: None,
            sqlstate: "XX000",
        })?;
        return Ok(Some(AppliedSetDefaultAction {
            outbound_constraint,
            updated_rows,
        }));
    }
    Ok(None)
}

fn apply_inbound_foreign_key_actions_on_update(
    relation_name: &str,
    constraints: &[BoundReferencedByForeignKey],
    previous_values: &[Value],
    values: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<Vec<PendingSetDefaultRecheck>, ExecError> {
    let mut pending = Vec::new();
    for constraint in constraints {
        if !constraint.enforced
            || !key_columns_changed(
                previous_values,
                values,
                &constraint.referenced_column_indexes,
            )
        {
            continue;
        }
        match constraint.on_update {
            ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                crate::backend::executor::enforce_inbound_foreign_keys_on_update(
                    relation_name,
                    std::slice::from_ref(constraint),
                    previous_values,
                    values,
                    ctx,
                )?;
            }
            ForeignKeyAction::Cascade => {
                let old_key_values = constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|index| previous_values.get(*index).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let new_key_values = constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|index| values.get(*index).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                apply_referential_action_to_rows(
                    constraint,
                    ForeignKeyAction::Cascade,
                    &old_key_values,
                    Some(&new_key_values),
                    None,
                    ctx,
                    xid,
                    cid,
                    waiter,
                )?;
            }
            ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                let old_key_values = constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|index| previous_values.get(*index).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let applied = apply_referential_action_to_rows(
                    constraint,
                    constraint.on_update,
                    &old_key_values,
                    None,
                    None,
                    ctx,
                    xid,
                    cid,
                    waiter,
                )?;
                if let Some(applied) = applied {
                    pending.push(PendingSetDefaultRecheck {
                        relation_name: relation_name.to_string(),
                        inbound_constraint: constraint.clone(),
                        old_key_values,
                        outbound_constraint: applied.outbound_constraint,
                        updated_rows: applied.updated_rows,
                    });
                }
            }
        }
    }
    Ok(pending)
}

fn apply_inbound_foreign_key_actions_on_delete(
    relation_name: &str,
    constraints: &[BoundReferencedByForeignKey],
    values: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<Vec<PendingSetDefaultRecheck>, ExecError> {
    let cid = ctx.next_command_id;
    let mut pending = Vec::new();
    for constraint in constraints {
        if !constraint.enforced {
            continue;
        }
        match constraint.on_delete {
            ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                crate::backend::executor::enforce_inbound_foreign_keys_on_delete(
                    relation_name,
                    std::slice::from_ref(constraint),
                    values,
                    ctx,
                )?;
            }
            ForeignKeyAction::Cascade => {
                let key_values = constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|index| values.get(*index).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let rows = collect_referencing_rows(constraint, &key_values, ctx)?;
                let catalog = ctx
                    .catalog
                    .as_ref()
                    .ok_or_else(|| ExecError::DetailedError {
                        message: "foreign key action failed".into(),
                        detail: Some("executor context missing visible catalog".into()),
                        hint: None,
                        sqlstate: "XX000",
                    })?;
                let triggers = RuntimeTriggers::load(
                    catalog,
                    constraint.child_relation_oid,
                    &constraint.child_relation_name,
                    &constraint.child_desc,
                    TriggerOperation::Delete,
                    &[],
                    ctx.session_replication_role,
                )?;
                triggers.before_statement(ctx)?;
                let mut transition_capture = triggers.new_transition_capture();
                for (tid, child_values) in rows {
                    if !triggers.before_row_delete(&child_values, ctx)? {
                        continue;
                    }
                    let target = BoundDeleteTarget {
                        relation_name: constraint.child_relation_name.clone(),
                        rel: constraint.child_rel,
                        relation_oid: constraint.child_relation_oid,
                        relkind: 'r',
                        toast: constraint.child_toast,
                        desc: constraint.child_desc.clone(),
                        referenced_by_foreign_keys: relation_write_state_for_foreign_key(
                            constraint, ctx,
                        )?
                        .1,
                        row_source: BoundModifyRowSource::Heap,
                        predicate: None,
                    };
                    let _ = apply_base_delete_row(
                        &target,
                        tid,
                        child_values.clone(),
                        ctx,
                        xid,
                        waiter,
                    )?;
                    triggers.capture_delete_row(&mut transition_capture, &child_values);
                    triggers.after_row_delete(&child_values, ctx)?;
                }
                triggers.after_transition_rows(&transition_capture, ctx)?;
                triggers.after_statement(Some(&transition_capture), ctx)?;
            }
            ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                let key_values = constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|index| values.get(*index).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let applied = apply_referential_action_to_rows(
                    constraint,
                    constraint.on_delete,
                    &key_values,
                    None,
                    constraint.on_delete_set_column_indexes.as_deref(),
                    ctx,
                    xid,
                    cid,
                    waiter,
                )?;
                if let Some(applied) = applied {
                    pending.push(PendingSetDefaultRecheck {
                        relation_name: relation_name.to_string(),
                        inbound_constraint: constraint.clone(),
                        old_key_values: key_values,
                        outbound_constraint: applied.outbound_constraint,
                        updated_rows: applied.updated_rows,
                    });
                }
            }
        }
    }
    Ok(pending)
}

pub fn execute_analyze(
    stmt: AnalyzeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<StatementResult, ExecError> {
    validate_maintenance_targets(&stmt.targets, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_vacuum(
    stmt: VacuumStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    validate_maintenance_targets(&stmt.targets, catalog)?;
    let _ = collect_vacuum_stats(&stmt.targets, catalog, ctx)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn collect_vacuum_stats(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<crate::backend::access::heap::vacuumlazy::VacuumRelationStats>, ExecError> {
    let mut processed = 0u64;
    let mut stats = Vec::with_capacity(targets.len());
    for target in targets {
        let Some(entry) = catalog.lookup_relation(&target.table_name) else {
            continue;
        };
        let scan = crate::backend::access::heap::vacuumlazy::vacuum_relation_scan(
            &ctx.pool,
            ctx.client_id,
            entry.rel,
            &ctx.txns,
        )
        .map_err(ExecError::Heap)?;
        let indexes = catalog.index_relations_for_heap(entry.relation_oid);
        let dead_items = &scan.dead_tids;
        for index in indexes {
            let vacuum_ctx = crate::include::access::amapi::IndexVacuumContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                client_id: ctx.client_id,
                interrupts: ctx.interrupts.clone(),
                heap_relation: entry.rel,
                heap_desc: entry.desc.clone(),
                index_relation: index.rel,
                index_name: index.name.clone(),
                index_desc: index.desc.clone(),
                index_meta: index.index_meta.clone(),
            };
            let dead_item_callback = |tid| dead_items.contains(&tid);
            let stats = indexam::index_bulk_delete(
                &vacuum_ctx,
                index.index_meta.am_oid,
                &dead_item_callback,
                None,
            )
            .map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "VACUUM bulk delete",
                    actual: format!("{err:?}"),
                })
            })?;
            let _ =
                indexam::index_vacuum_cleanup(&vacuum_ctx, index.index_meta.am_oid, Some(stats))
                    .map_err(|err| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "VACUUM cleanup",
                            actual: format!("{err:?}"),
                        })
                    })?;
        }
        let previous_relfrozenxid = catalog
            .class_row_by_oid(entry.relation_oid)
            .map(|row| row.relfrozenxid);
        let relation_stats = crate::backend::access::heap::vacuumlazy::vacuum_relation_pages(
            &ctx.pool,
            ctx.client_id,
            entry.rel,
            entry.relation_oid,
            &ctx.txns,
            &scan,
            previous_relfrozenxid,
        )
        .map_err(ExecError::Heap)?;
        stats.push(relation_stats);
        processed += 1;
    }
    let _ = processed;
    Ok(stats)
}

pub fn execute_create_table(
    stmt: crate::backend::parser::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<StatementResult, ExecError> {
    let _entry = bind_create_table(&stmt, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_create_index(
    stmt: crate::backend::parser::CreateIndexStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    if stmt
        .using_method
        .as_deref()
        .is_some_and(|method| !method.eq_ignore_ascii_case("btree"))
    {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "unsupported index access method".into(),
        )));
    }
    if !stmt.include_columns.is_empty() || stmt.predicate.is_some() || !stmt.options.is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CREATE INDEX options".into(),
        )));
    }
    let entry = match catalog.create_index(
        stmt.index_name,
        &stmt.table_name,
        stmt.unique,
        &stmt.columns,
    ) {
        Ok(entry) => entry,
        Err(crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(_))
            if stmt.if_not_exists =>
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        Err(crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(name)) => {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(name)));
        }
        Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name)) => {
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
        }
        Err(crate::backend::catalog::catalog::CatalogError::UnknownColumn(name)) => {
            return Err(ExecError::Parse(ParseError::UnknownColumn(name)));
        }
        Err(other) => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "catalog index creation",
                actual: format!("{other:?}"),
            }));
        }
    };
    let _ = ctx;
    let _ = entry;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_drop_table(
    stmt: DropTableStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    if stmt.cascade {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP TABLE CASCADE handled by database/session layer",
            actual: "DROP TABLE ... CASCADE".into(),
        }));
    }
    let mut dropped = 0;
    for table_name in stmt.table_names {
        match catalog.drop_table(&table_name) {
            Ok(entry) => {
                let _ = ctx.pool.invalidate_relation(entry.rel);
                ctx.pool
                    .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                dropped += 1;
            }
            Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name))
                if stmt.if_exists =>
            {
                let _ = name;
            }
            Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name)) => {
                return Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
            }
            Err(other) => {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "droppable table",
                    actual: format!("{other:?}"),
                }));
            }
        }
    }
    Ok(StatementResult::AffectedRows(dropped))
}

pub fn execute_truncate_table(
    stmt: TruncateTableStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    for table_name in stmt.table_names {
        let entry = match catalog.lookup_any_relation(&table_name) {
            Some(entry) if entry.relkind == 'r' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    table_name.clone(),
                )));
            }
        };
        if catalog.has_subclass(entry.relation_oid) {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "TRUNCATE on inherited parents is not supported yet".into(),
            )));
        }
        let indexes = catalog.index_relations_for_heap(entry.relation_oid);
        let _ = ctx.pool.invalidate_relation(entry.rel);
        ctx.pool
            .with_storage_mut(|s| {
                s.smgr.truncate(entry.rel, ForkNumber::Main, 0)?;
                if s.smgr.exists(entry.rel, ForkNumber::VisibilityMap) {
                    s.smgr.truncate(entry.rel, ForkNumber::VisibilityMap, 0)?;
                }
                Ok(())
            })
            .map_err(HeapError::Storage)?;
        for index in indexes
            .iter()
            .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
        {
            reinitialize_index_relation(index, ctx, xid)?;
        }
        ctx.session_stats
            .write()
            .note_relation_truncate(entry.relation_oid);
    }
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_insert(
    stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_insert(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let values = materialize_insert_rows(&stmt, catalog, ctx)?;

        let returned_rows = if let Some(on_conflict) = stmt.on_conflict.as_ref() {
            let returned_rows =
                execute_insert_on_conflict_rows(&stmt, on_conflict, &values, ctx, xid, cid)?;
            for _ in 0..returned_rows.len() {
                ctx.session_stats
                    .write()
                    .note_relation_insert(stmt.relation_oid);
            }
            returned_rows
        } else {
            let returned_rows = execute_insert_rows_with_routing(
                catalog,
                &stmt.relation_name,
                stmt.relation_oid,
                stmt.rel,
                stmt.toast,
                stmt.toast_index.as_ref(),
                &stmt.desc,
                &stmt.relation_constraints,
                &stmt.rls_write_checks,
                &stmt.indexes,
                &values,
                ctx,
                xid,
                cid,
            )?;
            for _ in 0..returned_rows.len() {
                ctx.session_stats
                    .write()
                    .note_relation_insert(stmt.relation_oid);
            }
            returned_rows
        };
        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(returned_rows.len()))
        } else {
            let projected_rows = returned_rows
                .iter()
                .map(|row| project_returning_row(&stmt.returning, row, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(build_returning_result(
                returning_result_columns(&stmt.returning),
                projected_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

fn first_toast_index_for_relation(
    catalog: &dyn CatalogLookup,
    toast: Option<ToastRelationRef>,
) -> Option<BoundIndexRelation> {
    let toast = toast?;
    catalog
        .index_relations_for_heap(toast.relation_oid)
        .into_iter()
        .next()
}

#[allow(clippy::too_many_arguments)]
fn execute_insert_rows_with_routing(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    relation_oid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &BoundRelationConstraints,
    rls_write_checks: &[RlsWriteCheck],
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let Some(target_relation) = catalog.relation_by_oid(relation_oid) else {
        return execute_insert_rows(
            relation_name,
            relation_oid,
            rel,
            toast,
            toast_index,
            desc,
            relation_constraints,
            rls_write_checks,
            indexes,
            rows,
            ctx,
            xid,
            cid,
        );
    };
    if target_relation.relkind != 'p' && !target_relation.relispartition {
        return execute_insert_rows(
            relation_name,
            relation_oid,
            rel,
            toast,
            toast_index,
            desc,
            relation_constraints,
            rls_write_checks,
            indexes,
            rows,
            ctx,
            xid,
            cid,
        );
    }

    let mut routed = BTreeMap::<
        u32,
        (
            String,
            crate::backend::parser::BoundRelation,
            Vec<Vec<Value>>,
        ),
    >::new();
    for row in rows {
        let leaf = route_partition_target(catalog, &target_relation, row, &ctx.datetime_config)?;
        let leaf_name = catalog
            .class_row_by_oid(leaf.relation_oid)
            .map(|row| row.relname)
            .unwrap_or_else(|| relation_name.to_string());
        routed
            .entry(leaf.relation_oid)
            .or_insert_with(|| (leaf_name, leaf.clone(), Vec::new()))
            .2
            .push(row.clone());
    }

    let mut inserted_rows = Vec::new();
    for (_, (leaf_name, leaf, leaf_rows)) in routed {
        let leaf_constraints = if leaf.relation_oid == relation_oid {
            relation_constraints.clone()
        } else {
            bind_relation_constraints(Some(&leaf_name), leaf.relation_oid, &leaf.desc, catalog)?
        };
        let leaf_indexes = if leaf.relation_oid == relation_oid {
            indexes.to_vec()
        } else {
            catalog.index_relations_for_heap(leaf.relation_oid)
        };
        let leaf_toast_index = if leaf.relation_oid == relation_oid {
            toast_index.cloned()
        } else {
            first_toast_index_for_relation(catalog, leaf.toast)
        };
        inserted_rows.extend(execute_insert_rows(
            &leaf_name,
            leaf.relation_oid,
            leaf.rel,
            leaf.toast,
            leaf_toast_index.as_ref(),
            &leaf.desc,
            &leaf_constraints,
            rls_write_checks,
            &leaf_indexes,
            &leaf_rows,
            ctx,
            xid,
            cid,
        )?);
    }
    Ok(inserted_rows)
}

fn parse_tid_text(value: &Value) -> Result<Option<ItemPointerData>, ExecError> {
    let text = match value {
        Value::Null => return Ok(None),
        Value::Text(text) => text.as_str(),
        Value::TextRef(_, _) => {
            return Err(ExecError::DetailedError {
                message: "row ctid marker must be materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
        other => {
            return Err(ExecError::DetailedError {
                message: format!("row ctid marker has unexpected value {:?}", other),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };
    let inner = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or(ExecError::DetailedError {
            message: format!("invalid row ctid marker: {text}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    let (block, offset) = inner.split_once(',').ok_or(ExecError::DetailedError {
        message: format!("invalid row ctid marker: {text}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    Ok(Some(ItemPointerData {
        block_number: block.parse().map_err(|_| ExecError::DetailedError {
            message: format!("invalid row ctid marker: {text}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?,
        offset_number: offset.parse().map_err(|_| ExecError::DetailedError {
            message: format!("invalid row ctid marker: {text}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?,
    }))
}

fn parse_update_tableoid(value: &Value) -> Result<u32, ExecError> {
    match value {
        Value::Int32(value) => u32::try_from(*value).map_err(|_| ExecError::DetailedError {
            message: format!("invalid update tableoid marker: {value}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
        Value::Int64(value) => u32::try_from(*value).map_err(|_| ExecError::DetailedError {
            message: format!("invalid update tableoid marker: {value}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
        Value::Null => Err(ExecError::DetailedError {
            message: "update input row is missing target tableoid marker".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
        other => Err(ExecError::DetailedError {
            message: format!("update tableoid marker has unexpected value {:?}", other),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn merge_source_present(value: &Value) -> Result<bool, ExecError> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Null => Ok(false),
        other => Err(ExecError::DetailedError {
            message: format!("merge source marker has unexpected value {:?}", other),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn merge_condition_matches(
    condition: Option<&Expr>,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(condition) = condition else {
        return Ok(true);
    };
    Ok(matches!(
        eval_expr(condition, slot, ctx)?,
        Value::Bool(true)
    ))
}

fn execute_merge_insert_action(
    stmt: &BoundMergeStatement,
    target_columns: &[BoundAssignmentTarget],
    values: Option<&[Expr]>,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<bool, ExecError> {
    let mut row_values = vec![Value::Null; stmt.desc.columns.len()];
    let mut default_slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
    for (column_index, expr) in stmt.column_defaults.iter().enumerate() {
        row_values[column_index] = eval_expr(expr, &mut default_slot, ctx)?;
    }
    if let Some(values) = values {
        for (target, expr) in target_columns.iter().zip(values.iter()) {
            let value = eval_expr(expr, slot, ctx)?;
            apply_assignment_target(&stmt.desc, &mut row_values, target, value, slot, ctx)?;
        }
    }
    let inserted = execute_insert_values(
        &stmt.relation_name,
        stmt.relation_oid,
        stmt.rel,
        stmt.toast,
        stmt.toast_index.as_ref(),
        &stmt.desc,
        &stmt.relation_constraints,
        &[],
        &stmt.indexes,
        &[row_values],
        ctx,
        xid,
        cid,
    )?;
    if inserted > 0 {
        ctx.session_stats
            .write()
            .note_relation_insert(stmt.relation_oid);
    }
    Ok(inserted > 0)
}

fn execute_merge_update_row(
    stmt: &BoundMergeStatement,
    target_tid: ItemPointerData,
    original_values: &[Value],
    assignments: &[BoundAssignment],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<bool, ExecError> {
    let mut updated_values = original_values.to_vec();
    for assignment in assignments {
        let value = eval_expr(&assignment.expr, slot, ctx)?;
        apply_assignment_target(
            &stmt.desc,
            &mut updated_values,
            &BoundAssignmentTarget {
                column_index: assignment.column_index,
                subscripts: assignment.subscripts.clone(),
                field_path: assignment.field_path.clone(),
                target_sql_type: assignment.target_sql_type,
            },
            value,
            slot,
            ctx,
        )?;
    }
    materialize_generated_columns(&stmt.desc, &mut updated_values, ctx)?;
    let old_tuple = heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, target_tid)?;
    crate::backend::executor::enforce_relation_constraints(
        &stmt.relation_name,
        &stmt.desc,
        &stmt.relation_constraints,
        &updated_values,
        ctx,
    )?;
    crate::backend::executor::enforce_outbound_foreign_keys(
        &stmt.relation_name,
        &stmt.relation_constraints.foreign_keys,
        Some(original_values),
        &updated_values,
        ctx,
    )?;
    let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_update(
        &stmt.relation_name,
        &stmt.referenced_by_foreign_keys,
        original_values,
        &updated_values,
        ctx,
        xid,
        cid,
        None,
    )?;
    let (replacement, toasted) = toast_tuple_for_write(
        &stmt.desc,
        &updated_values,
        stmt.toast,
        stmt.toast_index.as_ref(),
        ctx,
        xid,
        cid,
    )?;
    match heap_update_with_waiter(
        &*ctx.pool,
        ctx.client_id,
        stmt.rel,
        &ctx.txns,
        xid,
        cid,
        target_tid,
        &replacement,
        None,
    ) {
        Ok(new_tid) => {
            if let Some(toast) = stmt.toast {
                delete_external_from_tuple(ctx, toast, &stmt.desc, &old_tuple, xid)?;
            }
            maintain_indexes_for_row(
                stmt.rel,
                &stmt.desc,
                &stmt.indexes,
                &updated_values,
                new_tid,
                ctx,
            )?;
            validate_pending_set_default_rechecks(pending_set_default_rechecks, ctx)?;
            ctx.session_stats
                .write()
                .note_relation_update(stmt.relation_oid);
            Ok(true)
        }
        Err(HeapError::TupleUpdated(_, _)) | Err(HeapError::TupleAlreadyModified(_)) => {
            cleanup_toast_attempt(stmt.toast, &toasted, ctx, xid)?;
            Ok(false)
        }
        Err(err) => {
            cleanup_toast_attempt(stmt.toast, &toasted, ctx, xid)?;
            Err(err.into())
        }
    }
}

fn execute_merge_delete_row(
    stmt: &BoundMergeStatement,
    target_tid: ItemPointerData,
    original_values: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<bool, ExecError> {
    let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_delete(
        &stmt.relation_name,
        &stmt.referenced_by_foreign_keys,
        original_values,
        ctx,
        xid,
        None,
    )?;
    let old_tuple = if stmt.toast.is_some() {
        Some(heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, target_tid)?)
    } else {
        None
    };
    match heap_delete_with_waiter(
        &*ctx.pool,
        ctx.client_id,
        stmt.rel,
        &ctx.txns,
        xid,
        target_tid,
        &ctx.snapshot,
        None,
    ) {
        Ok(()) => {
            if let (Some(toast), Some(old_tuple)) = (stmt.toast, old_tuple.as_ref()) {
                delete_external_from_tuple(ctx, toast, &stmt.desc, old_tuple, xid)?;
            }
            validate_pending_set_default_rechecks(pending_set_default_rechecks, ctx)?;
            ctx.session_stats
                .write()
                .note_relation_delete(stmt.relation_oid);
            Ok(true)
        }
        Err(HeapError::TupleUpdated(_, _)) | Err(HeapError::TupleAlreadyModified(_)) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn execute_merge(
    stmt: BoundMergeStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_merge(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.input_plan.subplans.clone());
    let result = (|| {
        let mut state = executor_start(stmt.input_plan.plan_tree.clone());
        let mut affected_rows = 0usize;
        let mut matched_target_rows = HashSet::new();
        while let Some(slot) = state.exec_proc_node(ctx)? {
            ctx.check_for_interrupts()?;
            let mut row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
            Value::materialize_all(&mut row_values);
            let target_tid = row_values
                .get(stmt.target_ctid_index)
                .ok_or(ExecError::DetailedError {
                    message: "merge input row is missing target ctid marker".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
                .and_then(parse_tid_text)?;
            let source_present = row_values
                .get(stmt.source_present_index)
                .ok_or(ExecError::DetailedError {
                    message: "merge input row is missing source-present marker".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
                .and_then(merge_source_present)?;
            if source_present
                && let Some(target_tid) = target_tid
                && !matched_target_rows.insert(target_tid)
            {
                return Err(ExecError::DetailedError {
                    message: "MERGE command cannot affect row a second time".into(),
                    detail: Some(
                        "Ensure that not more than one source row matches any one target row."
                            .into(),
                    ),
                    hint: None,
                    sqlstate: "21000",
                });
            }

            let target_matched = target_tid.is_some();
            let visible_values = row_values[..stmt.visible_column_count].to_vec();
            let target_values = visible_values[..stmt.desc.columns.len()].to_vec();
            let mut eval_slot = TupleSlot::virtual_row(visible_values);

            for clause in &stmt.when_clauses {
                let matches = match clause.match_kind {
                    crate::backend::parser::MergeMatchKind::Matched => {
                        target_matched && source_present
                    }
                    crate::backend::parser::MergeMatchKind::NotMatchedBySource => {
                        target_matched && !source_present
                    }
                    crate::backend::parser::MergeMatchKind::NotMatchedByTarget => {
                        !target_matched && source_present
                    }
                };
                if !matches
                    || !merge_condition_matches(clause.condition.as_ref(), &mut eval_slot, ctx)?
                {
                    continue;
                }
                let changed = match &clause.action {
                    BoundMergeAction::DoNothing => false,
                    BoundMergeAction::Delete => {
                        if let Some(target_tid) = target_tid {
                            execute_merge_delete_row(&stmt, target_tid, &target_values, ctx, xid)?
                        } else {
                            false
                        }
                    }
                    BoundMergeAction::Update { assignments } => {
                        if let Some(target_tid) = target_tid {
                            execute_merge_update_row(
                                &stmt,
                                target_tid,
                                &target_values,
                                assignments,
                                &mut eval_slot,
                                ctx,
                                xid,
                                cid,
                            )?
                        } else {
                            false
                        }
                    }
                    BoundMergeAction::Insert {
                        target_columns,
                        values,
                    } => execute_merge_insert_action(
                        &stmt,
                        target_columns,
                        values.as_deref(),
                        &mut eval_slot,
                        ctx,
                        xid,
                        cid,
                    )?,
                };
                if changed {
                    affected_rows += 1;
                }
                break;
            }
        }
        Ok(StatementResult::AffectedRows(affected_rows))
    })();
    ctx.subplans = saved_subplans;
    result
}

fn eval_implicit_insert_defaults(
    defaults: &[crate::backend::executor::Expr],
    targets: &[BoundAssignmentTarget],
    width: usize,
    ctx: &mut ExecutorContext,
) -> Result<(TupleSlot, Vec<Value>), ExecError> {
    let mut slot = TupleSlot::virtual_row(vec![Value::Null; width]);
    let mut targeted = vec![false; width];
    for target in targets {
        if let Some(mark) = targeted.get_mut(target.column_index) {
            *mark = true;
        }
    }
    let mut values = vec![Value::Null; width];
    for (column_index, expr) in defaults.iter().enumerate() {
        if targeted.get(column_index).copied().unwrap_or(false) {
            continue;
        }
        values[column_index] = eval_expr(expr, &mut slot, ctx)?;
    }
    Ok((slot, values))
}

pub(crate) fn materialize_insert_rows(
    stmt: &BoundInsertStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Vec<Value>>, ExecError> {
    match &stmt.source {
        BoundInsertSource::Values(rows) => rows
            .iter()
            .map(|row| {
                let (mut slot, mut values) = eval_implicit_insert_defaults(
                    &stmt.column_defaults,
                    &stmt.target_columns,
                    stmt.desc.columns.len(),
                    ctx,
                )?;
                for (target, expr) in stmt.target_columns.iter().zip(row.iter()) {
                    let value = eval_expr(expr, &mut slot, ctx)?;
                    apply_assignment_target(
                        &stmt.desc,
                        &mut values,
                        target,
                        value,
                        &mut slot,
                        ctx,
                    )?;
                }
                Ok(values)
            })
            .collect::<Result<Vec<_>, ExecError>>(),
        BoundInsertSource::DefaultValues(defaults) => {
            let mut slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
            let mut values = vec![Value::Null; stmt.desc.columns.len()];
            for (target, expr) in stmt.target_columns.iter().zip(defaults.iter()) {
                let value = eval_expr(expr, &mut slot, ctx)?;
                apply_assignment_target(&stmt.desc, &mut values, target, value, &mut slot, ctx)?;
            }
            Ok(vec![values])
        }
        BoundInsertSource::Select(query) => {
            let [query] = pg_rewrite_query((**query).clone(), catalog)
                .map_err(ExecError::Parse)?
                .try_into()
                .expect("insert-select rewrite should return a single query");
            let query =
                crate::backend::optimizer::fold_query_constants(query).map_err(ExecError::Parse)?;
            let planned = planner(query, catalog).map_err(ExecError::Parse)?;
            let result: Result<Vec<Vec<Value>>, ExecError> = (|| {
                let saved_subplans = std::mem::replace(&mut ctx.subplans, planned.subplans.clone());
                let mut state = executor_start(planned.plan_tree.clone());
                let mut rows = Vec::new();
                while let Some(slot) = state.exec_proc_node(ctx)? {
                    ctx.check_for_interrupts()?;
                    let row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                    let (_, mut values) = eval_implicit_insert_defaults(
                        &stmt.column_defaults,
                        &stmt.target_columns,
                        stmt.desc.columns.len(),
                        ctx,
                    )?;
                    for (target, value) in stmt.target_columns.iter().zip(row_values.into_iter()) {
                        apply_assignment_target(&stmt.desc, &mut values, target, value, slot, ctx)?;
                    }
                    rows.push(values);
                }
                ctx.subplans = saved_subplans;
                Ok(rows)
            })();
            result
        }
    }
}

pub(crate) fn apply_assignment_target(
    desc: &RelationDesc,
    values: &mut [Value],
    target: &BoundAssignmentTarget,
    value: Value,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let assignment_type = assignment_target_sql_type(desc, target);
    let value = coerce_assignment_value(&value, assignment_type)
        .map_err(|err| rewrite_subscripted_assignment_error(desc, target, &value, err))?;
    let resolved = target
        .subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedAssignmentSubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let current = values[target.column_index].clone();
    let column_type = desc.columns[target.column_index].sql_type;
    values[target.column_index] = assign_typed_value(
        current,
        column_type,
        &resolved,
        &target.field_path,
        value,
        ctx,
    )?;
    Ok(())
}

fn rewrite_subscripted_assignment_error(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    value: &Value,
    err: ExecError,
) -> ExecError {
    if target.subscripts.is_empty() || !matches!(err, ExecError::TypeMismatch { .. }) {
        return err;
    }

    let Some(actual_type) = value.sql_type_hint() else {
        return err;
    };

    ExecError::DetailedError {
        message: format!(
            "subscripted assignment to \"{}\" requires type {} but expression is of type {}",
            desc.columns[target.column_index].name,
            sql_type_display_name(assignment_target_sql_type(desc, target)),
            sql_type_display_name(actual_type),
        ),
        detail: None,
        hint: Some("You will need to rewrite or cast the expression.".into()),
        sqlstate: "42804",
    }
}

fn sql_type_display_name(ty: SqlType) -> String {
    if ty.is_range() {
        let base = builtin_range_name_for_sql_type(ty).unwrap_or("range");
        return if ty.is_array {
            format!("{base}[]")
        } else {
            base.to_string()
        };
    }
    if ty.is_multirange() {
        let base = crate::include::catalog::builtin_multirange_name_for_sql_type(ty)
            .unwrap_or("multirange");
        return if ty.is_array {
            format!("{base}[]")
        } else {
            base.to_string()
        };
    }
    let base = match ty.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Void => "void",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Inet => "inet",
        SqlTypeKind::Cidr => "cidr",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    };

    if ty.is_array {
        format!("{base}[]")
    } else {
        base.to_string()
    }
}

fn assignment_target_sql_type(desc: &RelationDesc, target: &BoundAssignmentTarget) -> SqlType {
    let _ = desc;
    target.target_sql_type
}

#[derive(Clone)]
struct ResolvedAssignmentSubscript {
    is_slice: bool,
    lower: Option<Value>,
    upper: Option<Value>,
}

fn assign_point_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    if subscripts.len() != 1 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }
    let subscript = &subscripts[0];
    if subscript.is_slice {
        return Err(ExecError::DetailedError {
            message: "slices of fixed-length arrays not implemented".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
        return Err(assignment_null_subscript_error());
    };
    if !(0..=1).contains(&index) {
        return Err(array_assignment_error("array subscript out of range"));
    }
    let Value::Point(mut point) = current else {
        return if matches!(current, Value::Null) {
            Ok(Value::Null)
        } else {
            Err(ExecError::TypeMismatch {
                op: "array assignment",
                left: current,
                right: Value::Null,
            })
        };
    };
    let coordinate = match replacement {
        Value::Null => return Ok(Value::Point(point)),
        Value::Float64(value) => value,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "array assignment",
                left: Value::Point(point),
                right: other,
            });
        }
    };
    if index == 0 {
        point.x = coordinate;
    } else {
        point.y = coordinate;
    }
    Ok(Value::Point(point))
}

fn assign_typed_value(
    current: Value,
    sql_type: SqlType,
    subscripts: &[ResolvedAssignmentSubscript],
    field_path: &[String],
    replacement: Value,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if subscripts.is_empty() {
        if field_path.is_empty() {
            return Ok(replacement);
        }
        return assign_record_field_path(current, sql_type, field_path, replacement, ctx);
    }

    if sql_type.kind == SqlTypeKind::Point && !sql_type.is_array {
        if !field_path.is_empty() {
            return Err(ExecError::DetailedError {
                message: "cannot assign to a named field of type point".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        return assign_point_value(current, subscripts, replacement);
    }

    if sql_type.kind == SqlTypeKind::Jsonb && !sql_type.is_array {
        if !field_path.is_empty() {
            return Err(ExecError::DetailedError {
                message: "cannot assign to a named field of type jsonb".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        return assign_jsonb_value(current, subscripts, replacement);
    }

    if field_path.is_empty() {
        return assign_array_value(current, subscripts, replacement);
    }

    assign_array_value_with_fields(current, sql_type, subscripts, field_path, replacement, ctx)
}

fn assign_jsonb_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    let mut path = Vec::with_capacity(subscripts.len());
    for subscript in subscripts {
        if subscript.is_slice {
            return Err(ExecError::DetailedError {
                message: "jsonb subscript does not support slices".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        path.push(subscript.lower.clone().unwrap_or(Value::Int64(1)));
    }
    apply_jsonb_subscript_assignment(&current, &path, &replacement)
}

fn assignment_record_descriptor(
    sql_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<RecordDescriptor, ExecError> {
    if matches!(sql_type.kind, SqlTypeKind::Composite) && sql_type.typrelid != 0 {
        let catalog = ctx
            .catalog
            .as_ref()
            .ok_or_else(|| ExecError::DetailedError {
                message: "named composite assignment requires catalog context".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            })?;
        let relation = catalog
            .lookup_relation_by_oid(sql_type.typrelid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("unknown composite relation oid {}", sql_type.typrelid),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        return Ok(RecordDescriptor::named(
            sql_type.type_oid,
            sql_type.typrelid,
            sql_type.typmod,
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        ));
    }

    if matches!(sql_type.kind, SqlTypeKind::Record)
        && sql_type.typmod > 0
        && let Some(descriptor) =
            crate::backend::utils::record::lookup_anonymous_record_descriptor(sql_type.typmod)
    {
        return Ok(descriptor);
    }

    Err(ExecError::DetailedError {
        message: format!(
            "cannot assign to field of type {} because it is not a composite value",
            sql_type_display_name(sql_type)
        ),
        detail: None,
        hint: None,
        sqlstate: "42804",
    })
}

fn assignment_record_value(
    current: Value,
    sql_type: SqlType,
    ctx: &ExecutorContext,
) -> Result<RecordValue, ExecError> {
    match current {
        Value::Record(record) => Ok(record),
        Value::Null => {
            let descriptor = assignment_record_descriptor(sql_type, ctx)?;
            Ok(RecordValue::from_descriptor(
                descriptor.clone(),
                vec![Value::Null; descriptor.fields.len()],
            ))
        }
        other => Err(ExecError::TypeMismatch {
            op: "record assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn assign_record_field_path(
    current: Value,
    sql_type: SqlType,
    field_path: &[String],
    replacement: Value,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut record = assignment_record_value(current, sql_type, ctx)?;
    let field = field_path.first().ok_or_else(|| ExecError::DetailedError {
        message: "empty record field assignment path".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    let (field_index, field_type) = record
        .descriptor
        .fields
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name.eq_ignore_ascii_case(field))
        .map(|(index, candidate)| (index, candidate.sql_type))
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("record has no field \"{field}\""),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })?;

    record.fields[field_index] = if field_path.len() == 1 {
        replacement
    } else {
        assign_record_field_path(
            record.fields[field_index].clone(),
            field_type,
            &field_path[1..],
            replacement,
            ctx,
        )?
    };
    Ok(Value::Record(record))
}

fn assign_array_value_with_fields(
    current: Value,
    array_type: SqlType,
    subscripts: &[ResolvedAssignmentSubscript],
    field_path: &[String],
    replacement: Value,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if subscripts.is_empty() {
        return assign_record_field_path(current, array_type, field_path, replacement, ctx);
    }
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        return Err(ExecError::DetailedError {
            message: "sliced assignment into composite fields is not supported".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let subscript = &subscripts[0];
    let (mut lower_bound, mut items) = assignment_top_level(current)?;
    let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
        return Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array subscript in assignment must not be null".into(),
        });
    };
    if items.is_empty() {
        lower_bound = index;
    }
    extend_assignment_items(&mut lower_bound, &mut items, index, index)?;
    let index = usize::try_from(i64::from(index) - i64::from(lower_bound))
        .map_err(|_| array_assignment_limit_error())?;
    items[index] = assign_typed_value(
        items[index].clone(),
        array_type.element_type(),
        &subscripts[1..],
        field_path,
        replacement,
        ctx,
    )?;
    build_assignment_array_value(lower_bound, items)
}

fn assign_array_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    if subscripts.is_empty() {
        return Ok(replacement);
    }
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        return assign_array_slice_value(current, subscripts, replacement);
    }
    let subscript = &subscripts[0];
    let (mut lower_bound, mut items) = assignment_top_level(current)?;
    if subscript.is_slice {
        let Some(start) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        let Some(end) = assignment_subscript_index(subscript.upper.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        let replacement_items = assignment_replacement_items(replacement.clone())?;
        if items.is_empty() {
            lower_bound = start;
        }
        extend_assignment_items(&mut lower_bound, &mut items, start, end)?;
        let start_idx = usize::try_from(i64::from(start) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let end_idx = usize::try_from(i64::from(end) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let span = end_idx - start_idx + 1;
        if replacement_items.len() != span {
            return Err(ExecError::TypeMismatch {
                op: "array slice assignment",
                left: build_assignment_array_value(lower_bound, items.clone())?,
                right: replacement,
            });
        }
        for (idx, item) in replacement_items.into_iter().enumerate() {
            items[start_idx + idx] = if subscripts.len() == 1 {
                item
            } else {
                assign_array_value(items[start_idx + idx].clone(), &subscripts[1..], item)?
            };
        }
        build_assignment_array_value(lower_bound, items)
    } else {
        let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        if items.is_empty() {
            lower_bound = index;
        }
        extend_assignment_items(&mut lower_bound, &mut items, index, index)?;
        let index = usize::try_from(i64::from(index) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        items[index] = if subscripts.len() == 1 {
            replacement
        } else {
            assign_array_value(items[index].clone(), &subscripts[1..], replacement)?
        };
        build_assignment_array_value(lower_bound, items)
    }
}

fn assign_array_slice_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    if matches!(replacement, Value::Null) {
        return Ok(current);
    }

    let current_array = assignment_current_array(current)?;
    let source_array = assignment_source_array(replacement)?;

    if subscripts.len() > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    if current_array.ndim() == 0 {
        return assign_array_slice_into_empty(subscripts, source_array);
    }

    let ndim = current_array.ndim();
    if ndim < subscripts.len() || ndim > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    let mut dimensions = current_array.dimensions.clone();
    let mut lower_bounds = Vec::with_capacity(ndim);
    let mut upper_bounds = Vec::with_capacity(ndim);

    for (dim_idx, subscript) in subscripts.iter().enumerate() {
        let dim = &dimensions[dim_idx];
        let lower = resolve_assignment_slice_bound(
            subscript.lower.as_ref(),
            dim.lower_bound,
            subscript.is_slice,
        )?;
        let upper = resolve_assignment_slice_bound(
            if subscript.is_slice {
                subscript.upper.as_ref()
            } else {
                subscript.lower.as_ref()
            },
            checked_array_upper_bound(dim.lower_bound, dim.length)?,
            subscript.is_slice,
        )?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }

        if ndim == 1 {
            if lower < dimensions[0].lower_bound {
                let extension =
                    usize::try_from(i64::from(dimensions[0].lower_bound) - i64::from(lower))
                        .map_err(|_| array_assignment_limit_error())?;
                dimensions[0].lower_bound = lower;
                dimensions[0].length = dimensions[0]
                    .length
                    .checked_add(extension)
                    .ok_or_else(array_assignment_limit_error)?;
                dimensions[0].length = checked_array_item_count(dimensions[0].length)?;
            }
            let current_upper =
                checked_array_upper_bound(dimensions[0].lower_bound, dimensions[0].length)?;
            if upper > current_upper {
                let extension = usize::try_from(i64::from(upper) - i64::from(current_upper))
                    .map_err(|_| array_assignment_limit_error())?;
                dimensions[0].length = dimensions[0]
                    .length
                    .checked_add(extension)
                    .ok_or_else(array_assignment_limit_error)?;
                dimensions[0].length = checked_array_item_count(dimensions[0].length)?;
            }
        } else if lower < dim.lower_bound
            || upper > checked_array_upper_bound(dim.lower_bound, dim.length)?
        {
            return Err(array_assignment_error("array subscript out of range"));
        }

        lower_bounds.push(lower);
        upper_bounds.push(upper);
    }

    for dim in dimensions.iter().skip(subscripts.len()) {
        lower_bounds.push(dim.lower_bound);
        upper_bounds.push(checked_array_upper_bound(dim.lower_bound, dim.length)?);
    }

    let span_lengths = lower_bounds
        .iter()
        .zip(upper_bounds.iter())
        .map(|(lower, upper)| checked_array_span_length(*lower, *upper))
        .collect::<Result<Vec<_>, _>>()?;
    let target_items = span_lengths
        .iter()
        .try_fold(1usize, |count, span| count.checked_mul(*span))
        .ok_or_else(|| array_assignment_limit_error())
        .and_then(checked_array_item_count)?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    let element_type_oid = current_array
        .element_type_oid
        .or(source_array.element_type_oid);
    if ndim == 1 {
        let mut elements = vec![Value::Null; dimensions[0].length];
        let original_lower = current_array.lower_bound(0).unwrap_or(1);
        for (idx, value) in current_array.elements.iter().enumerate() {
            let target_idx = usize::try_from(
                i64::from(original_lower)
                    + i64::try_from(idx).map_err(|_| array_assignment_limit_error())?
                    - i64::from(dimensions[0].lower_bound),
            )
            .map_err(|_| array_assignment_limit_error())?;
            elements[target_idx] = value.clone();
        }
        let start_idx =
            usize::try_from(i64::from(lower_bounds[0]) - i64::from(dimensions[0].lower_bound))
                .map_err(|_| array_assignment_limit_error())?;
        for (offset, value) in source_array
            .elements
            .into_iter()
            .take(target_items)
            .enumerate()
        {
            elements[start_idx + offset] = value;
        }
        return Ok(Value::PgArray(array_with_element_type(
            ArrayValue::from_dimensions(dimensions, elements),
            element_type_oid,
        )));
    }

    let mut elements = current_array.elements.clone();
    for (offset, value) in source_array
        .elements
        .into_iter()
        .take(target_items)
        .enumerate()
    {
        let coords = linear_index_to_assignment_coords(offset, &lower_bounds, &span_lengths);
        let target_idx = assignment_coords_to_linear_index(&coords, &dimensions);
        elements[target_idx] = value;
    }
    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(dimensions, elements),
        element_type_oid,
    )))
}

fn assign_array_slice_into_empty(
    subscripts: &[ResolvedAssignmentSubscript],
    source_array: ArrayValue,
) -> Result<Value, ExecError> {
    let mut dimensions = Vec::with_capacity(subscripts.len());
    for subscript in subscripts {
        let Some(lower_value) = subscript.lower.as_ref() else {
            return Err(ExecError::DetailedError {
                message: "array slice subscript must provide both boundaries".into(),
                detail: Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                hint: None,
                sqlstate: "2202E",
            });
        };
        let Some(upper_value) = (if subscript.is_slice {
            subscript.upper.as_ref()
        } else {
            subscript.lower.as_ref()
        }) else {
            return Err(ExecError::DetailedError {
                message: "array slice subscript must provide both boundaries".into(),
                detail: Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                hint: None,
                sqlstate: "2202E",
            });
        };
        let lower = assignment_subscript_index(Some(lower_value))?
            .ok_or_else(|| assignment_null_subscript_error())?;
        let upper = assignment_subscript_index(Some(upper_value))?
            .ok_or_else(|| assignment_null_subscript_error())?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }
        dimensions.push(ArrayDimension {
            lower_bound: lower,
            length: checked_array_span_length(lower, upper)?,
        });
    }

    let target_items = dimensions
        .iter()
        .try_fold(1usize, |count, dim| count.checked_mul(dim.length))
        .ok_or_else(|| array_assignment_limit_error())
        .and_then(checked_array_item_count)?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(
            dimensions,
            source_array
                .elements
                .into_iter()
                .take(target_items)
                .collect(),
        ),
        source_array.element_type_oid,
    )))
}

fn assignment_current_array(current: Value) -> Result<ArrayValue, ExecError> {
    match current {
        Value::Null => Ok(ArrayValue::empty()),
        other => array_value_from_value(&other).ok_or(ExecError::TypeMismatch {
            op: "array assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn assignment_source_array(replacement: Value) -> Result<ArrayValue, ExecError> {
    array_value_from_value(&replacement).ok_or(ExecError::TypeMismatch {
        op: "array slice assignment",
        left: Value::Null,
        right: replacement,
    })
}

fn resolve_assignment_slice_bound(
    value: Option<&Value>,
    default: i32,
    is_slice: bool,
) -> Result<i32, ExecError> {
    match value {
        None if is_slice => Ok(default),
        None => assignment_subscript_index(None)?.ok_or_else(assignment_null_subscript_error),
        Some(_) => assignment_subscript_index(value)?.ok_or_else(assignment_null_subscript_error),
    }
}

fn assignment_null_subscript_error() -> ExecError {
    ExecError::InvalidStorageValue {
        column: "<array>".into(),
        details: "array subscript in assignment must not be null".into(),
    }
}

const MAX_ASSIGNMENT_ARRAY_ITEMS: usize = i32::MAX as usize;

fn checked_array_item_count(count: usize) -> Result<usize, ExecError> {
    if count > MAX_ASSIGNMENT_ARRAY_ITEMS {
        Err(array_assignment_limit_error())
    } else {
        Ok(count)
    }
}

fn checked_array_upper_bound(lower_bound: i32, length: usize) -> Result<i32, ExecError> {
    let length = i64::try_from(checked_array_item_count(length)?)
        .map_err(|_| array_assignment_limit_error())?;
    i32::try_from(
        i64::from(lower_bound)
            .checked_add(length)
            .and_then(|bound| bound.checked_sub(1))
            .ok_or_else(array_assignment_limit_error)?,
    )
    .map_err(|_| array_assignment_limit_error())
}

fn checked_array_span_length(lower: i32, upper: i32) -> Result<usize, ExecError> {
    let span = usize::try_from(
        i64::from(upper)
            .checked_sub(i64::from(lower))
            .and_then(|span| span.checked_add(1))
            .ok_or_else(array_assignment_limit_error)?,
    )
    .map_err(|_| array_assignment_limit_error())?;
    checked_array_item_count(span)
}

fn array_assignment_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "2202E",
    }
}

fn array_assignment_limit_error() -> ExecError {
    ExecError::DetailedError {
        message: "array size exceeds the maximum allowed".into(),
        detail: None,
        hint: None,
        sqlstate: "54000",
    }
}

fn array_with_element_type(mut array: ArrayValue, element_type_oid: Option<u32>) -> ArrayValue {
    array.element_type_oid = element_type_oid;
    array
}

fn linear_index_to_assignment_coords(
    mut offset: usize,
    lower_bounds: &[i32],
    lengths: &[usize],
) -> Vec<i32> {
    let mut coords = vec![0; lengths.len()];
    for dim_idx in 0..lengths.len() {
        let stride = lengths[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, length| product.saturating_mul(*length));
        let axis_offset = if stride == 0 { 0 } else { offset / stride };
        if stride != 0 {
            offset %= stride;
        }
        coords[dim_idx] = lower_bounds[dim_idx] + axis_offset as i32;
    }
    coords
}

fn assignment_coords_to_linear_index(coords: &[i32], dimensions: &[ArrayDimension]) -> usize {
    let mut offset = 0usize;
    for (dim_idx, coord) in coords.iter().enumerate() {
        let stride = dimensions[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, dim| product.saturating_mul(dim.length));
        offset += (*coord - dimensions[dim_idx].lower_bound) as usize * stride;
    }
    offset
}

fn assignment_top_level(current: Value) -> Result<(i32, Vec<Value>), ExecError> {
    match current {
        Value::Null => Ok((1, Vec::new())),
        Value::Array(items) => Ok((1, items)),
        Value::PgArray(array) => Ok((
            array.lower_bound(0).unwrap_or(1),
            assignment_top_level_items(&array),
        )),
        other => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn assignment_top_level_items(array: &ArrayValue) -> Vec<Value> {
    if array.dimensions.len() <= 1 {
        return array.elements.clone();
    }
    let child_dims = array.dimensions[1..].to_vec();
    let child_width = child_dims
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    let mut out = Vec::with_capacity(array.dimensions[0].length);
    for idx in 0..array.dimensions[0].length {
        let start = idx * child_width;
        out.push(Value::PgArray(ArrayValue::from_dimensions(
            child_dims.clone(),
            array.elements[start..start + child_width].to_vec(),
        )));
    }
    out
}

fn assignment_replacement_items(replacement: Value) -> Result<Vec<Value>, ExecError> {
    match replacement {
        Value::Array(items) => Ok(items),
        Value::PgArray(array) => Ok(assignment_top_level_items(&array)),
        other => Err(ExecError::TypeMismatch {
            op: "array slice assignment",
            left: Value::Null,
            right: other,
        }),
    }
}

fn extend_assignment_items(
    lower_bound: &mut i32,
    items: &mut Vec<Value>,
    start: i32,
    end: i32,
) -> Result<(), ExecError> {
    if items.is_empty() {
        *lower_bound = start;
    }
    if start < *lower_bound {
        let prepend = i64::from(*lower_bound)
            .checked_sub(i64::from(start))
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(array_assignment_limit_error)?;
        items.splice(0..0, std::iter::repeat_n(Value::Null, prepend));
        *lower_bound = start;
    }
    let upper_bound = i64::from(*lower_bound)
        .checked_add(i64::try_from(items.len()).map_err(|_| array_assignment_limit_error())?)
        .and_then(|bound| bound.checked_sub(1))
        .ok_or_else(array_assignment_limit_error)?;
    if i64::from(end) > upper_bound {
        let append = i64::from(end)
            .checked_sub(upper_bound)
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(array_assignment_limit_error)?;
        let new_len = items
            .len()
            .checked_add(append)
            .ok_or_else(array_assignment_limit_error)?;
        items.resize(checked_array_item_count(new_len)?, Value::Null);
    }
    Ok(())
}

fn build_assignment_array_value(lower_bound: i32, items: Vec<Value>) -> Result<Value, ExecError> {
    if items.is_empty() {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    let child_arrays = items
        .iter()
        .filter_map(|item| match item {
            Value::PgArray(array) => Some(Some(array.clone())),
            Value::Array(values) => {
                Some(ArrayValue::from_nested_values(values.clone(), vec![1]).ok())
            }
            Value::Null => Some(None),
            _ => None,
        })
        .collect::<Vec<_>>();
    if child_arrays.len() != items.len() {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    }
    let Some(template) = child_arrays.iter().find_map(|entry| entry.clone()) else {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    };
    let child_width = template.elements.len();
    let mut elements = Vec::with_capacity(items.len() * child_width);
    for entry in child_arrays {
        match entry {
            Some(array) => elements.extend(array.elements),
            None => elements.extend(std::iter::repeat_n(Value::Null, child_width)),
        }
    }
    let mut dimensions = vec![ArrayDimension {
        lower_bound,
        length: items.len(),
    }];
    dimensions.extend(template.dimensions);
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions, elements,
    )))
}

fn assignment_subscript_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn modified_attnums_for_update(assignments: &[BoundAssignment]) -> Vec<i16> {
    assignments
        .iter()
        .map(|assignment| assignment.column_index as i16 + 1)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn returning_result_columns(targets: &[TargetEntry]) -> Vec<QueryColumn> {
    targets
        .iter()
        .map(|target| QueryColumn {
            name: target.name.clone(),
            sql_type: target.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

fn project_returning_row(
    targets: &[TargetEntry],
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    project_returning_row_with_metadata(targets, row, None, None, ctx)
}

fn project_returning_row_with_metadata(
    targets: &[TargetEntry],
    row: &[Value],
    tid: Option<ItemPointerData>,
    table_oid: Option<u32>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut slot = TupleSlot::virtual_row_with_metadata(row.to_vec(), tid, table_oid);
    let mut values = targets
        .iter()
        .map(|target| eval_expr(&target.expr, &mut slot, ctx).map(|value| value.to_owned_value()))
        .collect::<Result<Vec<_>, _>>()?;
    Value::materialize_all(&mut values);
    Ok(values)
}

fn build_returning_result(columns: Vec<QueryColumn>, rows: Vec<Vec<Value>>) -> StatementResult {
    let column_names = columns.iter().map(|column| column.name.clone()).collect();
    StatementResult::Query {
        columns,
        column_names,
        rows,
    }
}

pub(crate) fn execute_insert_rows(
    relation_name: &str,
    relation_oid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &BoundRelationConstraints,
    rls_write_checks: &[RlsWriteCheck],
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let triggers = ctx
        .catalog
        .as_ref()
        .map(|catalog| {
            RuntimeTriggers::load(
                catalog,
                relation_oid,
                relation_name,
                desc,
                TriggerOperation::Insert,
                &[],
                ctx.session_replication_role,
            )
        })
        .transpose()?;
    if let Some(triggers) = &triggers {
        triggers.before_statement(ctx)?;
    }
    let mut transition_capture = triggers
        .as_ref()
        .map(|triggers| triggers.new_transition_capture());

    let mut inserted_rows = Vec::new();
    for values in rows {
        let Some(mut values) = (match &triggers {
            Some(triggers) => triggers.before_row_insert(values.clone(), ctx)?,
            None => Some(values.clone()),
        }) else {
            continue;
        };
        materialize_generated_columns(desc, &mut values, ctx)?;
        let heap_tid = write_insert_heap_row(
            relation_name,
            rel,
            toast,
            toast_index,
            desc,
            relation_constraints,
            rls_write_checks,
            &values,
            ctx,
            xid,
            cid,
        )?;
        maintain_indexes_for_row(rel, desc, indexes, &values, heap_tid, ctx)?;
        inserted_rows.push(values.clone());
        if let Some(triggers) = &triggers {
            if let Some(capture) = transition_capture.as_mut() {
                triggers.capture_insert_row(capture, &values);
            }
            triggers.after_row_insert(&values, ctx)?;
        }
    }

    if let Some(triggers) = &triggers {
        if let Some(capture) = transition_capture.as_ref() {
            triggers.after_transition_rows(capture, ctx)?;
            triggers.after_statement(Some(capture), ctx)?;
        } else {
            triggers.after_statement(None, ctx)?;
        }
    }

    Ok(inserted_rows)
}

pub(crate) fn execute_insert_values(
    relation_name: &str,
    relation_oid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &BoundRelationConstraints,
    rls_write_checks: &[RlsWriteCheck],
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<usize, ExecError> {
    if let Some(catalog) = ctx.catalog.clone() {
        return Ok(execute_insert_rows_with_routing(
            &catalog,
            relation_name,
            relation_oid,
            rel,
            toast,
            toast_index,
            desc,
            relation_constraints,
            rls_write_checks,
            indexes,
            rows,
            ctx,
            xid,
            cid,
        )?
        .len());
    }
    Ok(execute_insert_rows(
        relation_name,
        relation_oid,
        rel,
        toast,
        toast_index,
        desc,
        relation_constraints,
        rls_write_checks,
        indexes,
        rows,
        ctx,
        xid,
        cid,
    )?
    .len())
}

/// Execute a single-row insert from a prepared insert plan and parameter values.
/// This skips parsing, binding, and expression evaluation entirely.
pub fn execute_prepared_insert_row(
    prepared: &crate::backend::parser::PreparedInsert,
    params: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let triggers = ctx
        .catalog
        .as_ref()
        .map(|catalog| {
            RuntimeTriggers::load(
                catalog,
                prepared.relation_oid,
                &prepared.relation_name,
                &prepared.desc,
                TriggerOperation::Insert,
                &[],
                ctx.session_replication_role,
            )
        })
        .transpose()?;
    if let Some(triggers) = &triggers {
        triggers.before_statement(ctx)?;
    }
    let mut transition_capture = triggers
        .as_ref()
        .map(|triggers| triggers.new_transition_capture());

    let mut slot = TupleSlot::virtual_row(vec![Value::Null; prepared.desc.columns.len()]);
    let mut values = prepared
        .column_defaults
        .iter()
        .map(|expr| eval_expr(expr, &mut slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    for (column_index, param) in prepared.target_columns.iter().zip(params.iter()) {
        values[*column_index] = param.clone();
    }
    let Some(mut values) = (match &triggers {
        Some(triggers) => triggers.before_row_insert(values, ctx)?,
        None => Some(values),
    }) else {
        if let Some(triggers) = &triggers {
            if let Some(capture) = transition_capture.as_ref() {
                triggers.after_statement(Some(capture), ctx)?;
            } else {
                triggers.after_statement(None, ctx)?;
            }
        }
        return Ok(());
    };
    materialize_generated_columns(&prepared.desc, &mut values, ctx)?;
    let heap_tid = write_insert_heap_row(
        &prepared.relation_name,
        prepared.rel,
        prepared.toast,
        prepared.toast_index.as_ref(),
        &prepared.desc,
        &prepared.relation_constraints,
        &[],
        &values,
        ctx,
        xid,
        cid,
    )?;
    maintain_indexes_for_row(
        prepared.rel,
        &prepared.desc,
        &prepared.indexes,
        &values,
        heap_tid,
        ctx,
    )?;
    ctx.session_stats
        .write()
        .note_relation_insert(prepared.relation_oid);
    if let Some(triggers) = &triggers {
        if let Some(capture) = transition_capture.as_mut() {
            triggers.capture_insert_row(capture, &values);
        }
        triggers.after_row_insert(&values, ctx)?;
        if let Some(capture) = transition_capture.as_ref() {
            triggers.after_transition_rows(capture, ctx)?;
            triggers.after_statement(Some(capture), ctx)?;
        } else {
            triggers.after_statement(None, ctx)?;
        }
    }
    Ok(())
}

pub(crate) fn execute_update(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    execute_update_with_waiter(stmt, catalog, ctx, xid, cid, None)
}

pub fn execute_update_with_waiter(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_update(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        if stmt.input_plan.is_some() {
            return execute_update_from_joined_input(&stmt, ctx, xid, cid, waiter);
        }
        let mut affected_rows = 0;
        let mut returned_rows = Vec::new();

        for target in &stmt.targets {
            let modified_attnums = modified_attnums_for_update(&target.assignments);
            let triggers = ctx
                .catalog
                .as_ref()
                .map(|catalog| {
                    RuntimeTriggers::load(
                        catalog,
                        target.relation_oid,
                        &target.relation_name,
                        &target.desc,
                        TriggerOperation::Update,
                        &modified_attnums,
                        ctx.session_replication_role,
                    )
                })
                .transpose()?;
            if let Some(triggers) = &triggers {
                triggers.before_statement(ctx)?;
            }
            let mut transition_capture = triggers
                .as_ref()
                .map(|triggers| triggers.new_transition_capture());

            let desc = Rc::new(target.desc.clone());
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
            let qual = target
                .predicate
                .as_ref()
                .map(|p| compile_predicate_with_decoder(p, &decoder));
            let target_rows = match &target.row_source {
                BoundModifyRowSource::Heap => collect_matching_rows_heap(
                    target.rel,
                    &target.desc,
                    target.toast,
                    target.predicate.as_ref(),
                    ctx,
                )?,
                BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                    target.rel,
                    &target.desc,
                    target.toast,
                    index,
                    keys,
                    target.predicate.as_ref(),
                    ctx,
                )?,
            };

            for (tid, original_values) in target_rows {
                ctx.check_for_interrupts()?;
                let mut current_old_values = original_values.clone();
                let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
                let mut values = original_values;
                for assignment in &target.assignments {
                    let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                    apply_assignment_target(
                        &target.desc,
                        &mut values,
                        &BoundAssignmentTarget {
                            column_index: assignment.column_index,
                            subscripts: assignment.subscripts.clone(),
                            field_path: assignment.field_path.clone(),
                            target_sql_type: assignment.target_sql_type,
                        },
                        value,
                        &mut eval_slot,
                        ctx,
                    )?;
                }

                let mut current_tid = tid;
                let mut current_values = values;
                loop {
                    ctx.check_for_interrupts()?;
                    let Some(mut triggered_values) = (match &triggers {
                        Some(triggers) => triggers.before_row_update(
                            &current_old_values,
                            current_values.clone(),
                            ctx,
                        )?,
                        None => Some(current_values.clone()),
                    }) else {
                        break;
                    };
                    materialize_generated_columns(&target.desc, &mut triggered_values, ctx)?;
                    match write_updated_row(
                        &target.relation_name,
                        target.rel,
                        target.relation_oid,
                        target.toast,
                        target.toast_index.as_ref(),
                        &target.desc,
                        &target.relation_constraints,
                        &target.rls_write_checks,
                        &target.referenced_by_foreign_keys,
                        &target.indexes,
                        current_tid,
                        &current_old_values,
                        &triggered_values,
                        ctx,
                        xid,
                        cid,
                        waiter,
                    ) {
                        Ok(WriteUpdatedRowResult::Updated(_new_tid)) => {
                            ctx.session_stats
                                .write()
                                .note_relation_update(target.relation_oid);
                            if let Some(triggers) = &triggers {
                                if let Some(capture) = transition_capture.as_mut() {
                                    triggers.capture_update_row(
                                        capture,
                                        &current_old_values,
                                        &triggered_values,
                                    );
                                }
                                triggers.after_row_update(
                                    &current_old_values,
                                    &triggered_values,
                                    ctx,
                                )?;
                            }
                            if !stmt.returning.is_empty() {
                                returned_rows.push(project_returning_row(
                                    &stmt.returning,
                                    &triggered_values,
                                    ctx,
                                )?);
                            }
                            affected_rows += 1;
                            break;
                        }
                        Ok(WriteUpdatedRowResult::TupleUpdated(new_ctid)) => {
                            let new_tuple =
                                heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                            let mut new_slot = TupleSlot::from_heap_tuple(
                                Rc::clone(&desc),
                                Rc::clone(&attr_descs),
                                new_ctid,
                                new_tuple,
                            );
                            new_slot.toast = slot_toast_context(target.toast, ctx);
                            let passes = match &qual {
                                Some(q) => q(&mut new_slot, ctx)?,
                                None => true,
                            };
                            if !passes {
                                break;
                            }
                            let new_values_base = new_slot.into_values()?;
                            let mut eval_slot = TupleSlot::virtual_row(new_values_base.clone());
                            let mut new_values = new_values_base.clone();
                            for assignment in &target.assignments {
                                let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                                apply_assignment_target(
                                    &target.desc,
                                    &mut new_values,
                                    &BoundAssignmentTarget {
                                        column_index: assignment.column_index,
                                        subscripts: assignment.subscripts.clone(),
                                        field_path: assignment.field_path.clone(),
                                        target_sql_type: assignment.target_sql_type,
                                    },
                                    value,
                                    &mut eval_slot,
                                    ctx,
                                )?;
                            }
                            current_old_values = new_values_base;
                            current_values = new_values.clone();
                            current_tid = new_ctid;
                        }
                        Ok(WriteUpdatedRowResult::AlreadyModified) => {
                            break;
                        }
                        Err(err) => return Err(err),
                    }
                }
            }

            if let Some(triggers) = &triggers {
                if let Some(capture) = transition_capture.as_ref() {
                    triggers.after_transition_rows(capture, ctx)?;
                    triggers.after_statement(Some(capture), ctx)?;
                } else {
                    triggers.after_statement(None, ctx)?;
                }
            }
        }

        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_returning_result(
                returning_result_columns(&stmt.returning),
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

fn fetch_update_target_values(
    target: &BoundUpdateTarget,
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let desc = Rc::new(target.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let tuple = heap_fetch(&*ctx.pool, ctx.client_id, target.rel, tid)?;
    let mut slot = TupleSlot::from_heap_tuple(desc, attr_descs, tid, tuple);
    slot.toast = slot_toast_context(target.toast, ctx);
    slot.into_values()
}

fn project_update_target_visible_values(
    target: &BoundUpdateTarget,
    row_values: &[Value],
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut slot = TupleSlot::virtual_row_with_metadata(
        row_values.to_vec(),
        Some(tid),
        Some(target.relation_oid),
    );
    let mut values = target
        .parent_visible_exprs
        .iter()
        .map(|expr| eval_expr(expr, &mut slot, ctx).map(|value| value.to_owned_value()))
        .collect::<Result<Vec<_>, _>>()?;
    Value::materialize_all(&mut values);
    Ok(values)
}

fn build_update_from_eval_row(
    target: &BoundUpdateTarget,
    old_values: &[Value],
    source_values: &[Value],
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut values = project_update_target_visible_values(target, old_values, tid, ctx)?;
    values.extend(source_values.iter().cloned());
    Ok(values)
}

fn evaluate_update_from_assignments(
    target: &BoundUpdateTarget,
    old_values: &[Value],
    source_values: &[Value],
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let eval_row = build_update_from_eval_row(target, old_values, source_values, tid, ctx)?;
    let mut eval_slot =
        TupleSlot::virtual_row_with_metadata(eval_row, Some(tid), Some(target.relation_oid));
    let mut updated_values = old_values.to_vec();
    for assignment in &target.assignments {
        let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
        apply_assignment_target(
            &target.desc,
            &mut updated_values,
            &BoundAssignmentTarget {
                column_index: assignment.column_index,
                subscripts: assignment.subscripts.clone(),
                field_path: assignment.field_path.clone(),
                target_sql_type: assignment.target_sql_type,
            },
            value,
            &mut eval_slot,
            ctx,
        )?;
    }
    Ok(updated_values)
}

fn update_from_predicate_matches(
    target: &BoundUpdateTarget,
    old_values: &[Value],
    source_values: &[Value],
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(predicate) = &target.predicate else {
        return Ok(true);
    };
    let eval_row = build_update_from_eval_row(target, old_values, source_values, tid, ctx)?;
    let mut eval_slot =
        TupleSlot::virtual_row_with_metadata(eval_row, Some(tid), Some(target.relation_oid));
    Ok(matches!(
        eval_expr(predicate, &mut eval_slot, ctx)?,
        Value::Bool(true)
    ))
}

fn project_update_from_returning_row(
    stmt: &BoundUpdateStatement,
    target: &BoundUpdateTarget,
    new_values: &[Value],
    source_values: &[Value],
    tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut visible_values = project_update_target_visible_values(target, new_values, tid, ctx)?;
    visible_values.extend(source_values.iter().cloned());
    project_returning_row_with_metadata(
        &stmt.returning,
        &visible_values,
        Some(tid),
        Some(target.relation_oid),
        ctx,
    )
}

fn execute_update_from_joined_input(
    stmt: &BoundUpdateStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let input_plan = stmt.input_plan.as_ref().ok_or(ExecError::DetailedError {
        message: "UPDATE ... FROM is missing its input plan".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    let target_indexes = stmt
        .targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.relation_oid, index))
        .collect::<HashMap<_, _>>();
    let mut triggers = stmt
        .targets
        .iter()
        .map(|target| {
            let modified_attnums = modified_attnums_for_update(&target.assignments);
            ctx.catalog
                .as_ref()
                .map(|catalog| {
                    RuntimeTriggers::load(
                        catalog,
                        target.relation_oid,
                        &target.relation_name,
                        &target.desc,
                        TriggerOperation::Update,
                        &modified_attnums,
                        ctx.session_replication_role,
                    )
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    for trigger in triggers.iter().flatten() {
        trigger.before_statement(ctx)?;
    }
    let mut transition_captures = triggers
        .iter()
        .map(|trigger| {
            trigger
                .as_ref()
                .map(|trigger| trigger.new_transition_capture())
        })
        .collect::<Vec<_>>();

    let result = (|| {
        let mut state = executor_start(input_plan.plan_tree.clone());
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();

        while let Some(slot) = state.exec_proc_node(ctx)? {
            ctx.check_for_interrupts()?;
            let mut row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
            Value::materialize_all(&mut row_values);
            let target_tid = row_values
                .get(stmt.target_ctid_index)
                .ok_or(ExecError::DetailedError {
                    message: "update input row is missing target ctid marker".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
                .and_then(parse_tid_text)?
                .ok_or(ExecError::DetailedError {
                    message: "update input row is missing target ctid marker".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let target_tableoid = row_values
                .get(stmt.target_tableoid_index)
                .ok_or(ExecError::DetailedError {
                    message: "update input row is missing target tableoid marker".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
                .and_then(parse_update_tableoid)?;
            let target_index =
                *target_indexes
                    .get(&target_tableoid)
                    .ok_or(ExecError::DetailedError {
                        message: format!(
                            "update input row referenced unexpected target relation OID {target_tableoid}"
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "XX000",
                    })?;
            let target = &stmt.targets[target_index];
            let source_values =
                row_values[stmt.target_visible_count..stmt.visible_column_count].to_vec();
            let mut current_tid = target_tid;
            let mut current_old_values = fetch_update_target_values(target, current_tid, ctx)?;
            let mut current_values = evaluate_update_from_assignments(
                target,
                &current_old_values,
                &source_values,
                current_tid,
                ctx,
            )?;

            loop {
                ctx.check_for_interrupts()?;
                let Some(mut triggered_values) = (match triggers[target_index].as_ref() {
                    Some(trigger) => trigger.before_row_update(
                        &current_old_values,
                        current_values.clone(),
                        ctx,
                    )?,
                    None => Some(current_values.clone()),
                }) else {
                    break;
                };
                materialize_generated_columns(&target.desc, &mut triggered_values, ctx)?;
                match write_updated_row(
                    &target.relation_name,
                    target.rel,
                    target.relation_oid,
                    target.toast,
                    target.toast_index.as_ref(),
                    &target.desc,
                    &target.relation_constraints,
                    &target.rls_write_checks,
                    &target.referenced_by_foreign_keys,
                    &target.indexes,
                    current_tid,
                    &current_old_values,
                    &triggered_values,
                    ctx,
                    xid,
                    cid,
                    waiter,
                )? {
                    WriteUpdatedRowResult::Updated(new_tid) => {
                        ctx.session_stats
                            .write()
                            .note_relation_update(target.relation_oid);
                        if let Some(trigger) = triggers[target_index].as_ref() {
                            if let Some(capture) = transition_captures[target_index].as_mut() {
                                trigger.capture_update_row(
                                    capture,
                                    &current_old_values,
                                    &triggered_values,
                                );
                            }
                            trigger.after_row_update(
                                &current_old_values,
                                &triggered_values,
                                ctx,
                            )?;
                        }
                        if !stmt.returning.is_empty() {
                            returned_rows.push(project_update_from_returning_row(
                                stmt,
                                target,
                                &triggered_values,
                                &source_values,
                                new_tid,
                                ctx,
                            )?);
                        }
                        affected_rows += 1;
                        break;
                    }
                    WriteUpdatedRowResult::TupleUpdated(new_ctid) => {
                        let new_old_values = fetch_update_target_values(target, new_ctid, ctx)?;
                        if !update_from_predicate_matches(
                            target,
                            &new_old_values,
                            &source_values,
                            new_ctid,
                            ctx,
                        )? {
                            break;
                        }
                        current_values = evaluate_update_from_assignments(
                            target,
                            &new_old_values,
                            &source_values,
                            new_ctid,
                            ctx,
                        )?;
                        current_old_values = new_old_values;
                        current_tid = new_ctid;
                    }
                    WriteUpdatedRowResult::AlreadyModified => {
                        break;
                    }
                }
            }
        }

        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_returning_result(
                returning_result_columns(&stmt.returning),
                returned_rows,
            ))
        }
    })();

    if result.is_ok() {
        for (trigger, capture) in triggers.iter_mut().zip(transition_captures.iter()) {
            if let Some(trigger) = trigger {
                if let Some(capture) = capture.as_ref() {
                    trigger.after_transition_rows(capture, ctx)?;
                    trigger.after_statement(Some(capture), ctx)?;
                } else {
                    trigger.after_statement(None, ctx)?;
                }
            }
        }
    }
    result
}

pub(crate) fn execute_delete(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    execute_delete_with_waiter(stmt, catalog, ctx, xid, None)
}

pub fn execute_delete_with_waiter(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_delete(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0;
        let mut returned_rows = Vec::new();
        for target in &stmt.targets {
            let triggers = ctx
                .catalog
                .as_ref()
                .map(|catalog| {
                    RuntimeTriggers::load(
                        catalog,
                        target.relation_oid,
                        &target.relation_name,
                        &target.desc,
                        TriggerOperation::Delete,
                        &[],
                        ctx.session_replication_role,
                    )
                })
                .transpose()?;
            if let Some(triggers) = &triggers {
                triggers.before_statement(ctx)?;
            }
            let mut transition_capture = triggers
                .as_ref()
                .map(|triggers| triggers.new_transition_capture());

            let desc = Rc::new(target.desc.clone());
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
            let qual = target
                .predicate
                .as_ref()
                .map(|p| compile_predicate_with_decoder(p, &decoder));
            let targets = match &target.row_source {
                BoundModifyRowSource::Heap => collect_matching_rows_heap(
                    target.rel,
                    &target.desc,
                    target.toast,
                    target.predicate.as_ref(),
                    ctx,
                )?,
                BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                    target.rel,
                    &target.desc,
                    target.toast,
                    index,
                    keys,
                    target.predicate.as_ref(),
                    ctx,
                )?,
            };
            let snapshot = ctx.snapshot.clone();

            for (tid, values) in &targets {
                ctx.check_for_interrupts()?;
                let mut current_tid = *tid;
                let mut current_values = values.clone();
                loop {
                    ctx.check_for_interrupts()?;
                    if let Some(triggers) = &triggers {
                        if !triggers.before_row_delete(&current_values, ctx)? {
                            break;
                        }
                    }
                    let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_delete(
                        &target.relation_name,
                        &target.referenced_by_foreign_keys,
                        &current_values,
                        ctx,
                        xid,
                        waiter,
                    )?;
                    let old_tuple = if target.toast.is_some() {
                        Some(heap_fetch(
                            &*ctx.pool,
                            ctx.client_id,
                            target.rel,
                            current_tid,
                        )?)
                    } else {
                        None
                    };
                    match heap_delete_with_waiter(
                        &*ctx.pool,
                        ctx.client_id,
                        target.rel,
                        &ctx.txns,
                        xid,
                        current_tid,
                        &snapshot,
                        waiter,
                    ) {
                        Ok(()) => {
                            if let (Some(toast), Some(old_tuple)) =
                                (target.toast, old_tuple.as_ref())
                            {
                                delete_external_from_tuple(
                                    ctx,
                                    toast,
                                    &target.desc,
                                    old_tuple,
                                    xid,
                                )?;
                            }
                            validate_pending_set_default_rechecks(
                                pending_set_default_rechecks,
                                ctx,
                            )?;
                            ctx.session_stats
                                .write()
                                .note_relation_delete(target.relation_oid);
                            if let Some(triggers) = &triggers {
                                if let Some(capture) = transition_capture.as_mut() {
                                    triggers.capture_delete_row(capture, &current_values);
                                }
                                triggers.after_row_delete(&current_values, ctx)?;
                            }
                            if !stmt.returning.is_empty() {
                                returned_rows.push(project_returning_row(
                                    &stmt.returning,
                                    &current_values,
                                    ctx,
                                )?);
                            }
                            affected_rows += 1;
                            break;
                        }
                        Err(HeapError::TupleAlreadyModified(_)) => {
                            break;
                        }
                        Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                            let new_tuple =
                                heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                            let mut new_slot = TupleSlot::from_heap_tuple(
                                Rc::clone(&desc),
                                Rc::clone(&attr_descs),
                                new_ctid,
                                new_tuple,
                            );
                            new_slot.toast = slot_toast_context(target.toast, ctx);
                            let passes = match &qual {
                                Some(q) => q(&mut new_slot, ctx)?,
                                None => true,
                            };
                            if !passes {
                                break;
                            }
                            current_values = new_slot.into_values()?;
                            current_tid = new_ctid;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }

            if let Some(triggers) = &triggers {
                if let Some(capture) = transition_capture.as_ref() {
                    triggers.after_transition_rows(capture, ctx)?;
                    triggers.after_statement(Some(capture), ctx)?;
                } else {
                    triggers.after_statement(None, ctx)?;
                }
            }
        }

        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_returning_result(
                returning_result_columns(&stmt.returning),
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

#[derive(Debug, Clone)]
pub(crate) struct UpdateRowEvent {
    pub target: BoundUpdateTarget,
    pub tid: ItemPointerData,
    pub old_values: Vec<Value>,
    pub new_values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeleteRowEvent {
    pub target: BoundDeleteTarget,
    pub tid: ItemPointerData,
    pub old_values: Vec<Value>,
}

pub(crate) fn materialize_update_row_events(
    stmt: &BoundUpdateStatement,
    ctx: &mut ExecutorContext,
) -> Result<Vec<UpdateRowEvent>, ExecError> {
    if stmt.input_plan.is_some() {
        return materialize_update_from_joined_input_events(stmt, ctx);
    }
    let mut events = Vec::new();
    for target in &stmt.targets {
        let target_rows = match &target.row_source {
            BoundModifyRowSource::Heap => collect_matching_rows_heap(
                target.rel,
                &target.desc,
                target.toast,
                target.predicate.as_ref(),
                ctx,
            )?,
            BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                target.rel,
                &target.desc,
                target.toast,
                index,
                keys,
                target.predicate.as_ref(),
                ctx,
            )?,
        };

        for (tid, original_values) in target_rows {
            ctx.check_for_interrupts()?;
            let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
            let mut values = original_values.clone();
            for assignment in &target.assignments {
                let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                apply_assignment_target(
                    &target.desc,
                    &mut values,
                    &BoundAssignmentTarget {
                        column_index: assignment.column_index,
                        subscripts: assignment.subscripts.clone(),
                        field_path: assignment.field_path.clone(),
                        target_sql_type: assignment.target_sql_type,
                    },
                    value,
                    &mut eval_slot,
                    ctx,
                )?;
            }
            events.push(UpdateRowEvent {
                target: target.clone(),
                tid,
                old_values: original_values,
                new_values: values,
            });
        }
    }
    Ok(events)
}

fn materialize_update_from_joined_input_events(
    stmt: &BoundUpdateStatement,
    ctx: &mut ExecutorContext,
) -> Result<Vec<UpdateRowEvent>, ExecError> {
    let input_plan = stmt.input_plan.as_ref().ok_or(ExecError::DetailedError {
        message: "UPDATE ... FROM is missing its input plan".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    let target_indexes = stmt
        .targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.relation_oid, index))
        .collect::<HashMap<_, _>>();
    let mut state = executor_start(input_plan.plan_tree.clone());
    let mut events = Vec::new();
    while let Some(slot) = state.exec_proc_node(ctx)? {
        ctx.check_for_interrupts()?;
        let mut row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
        Value::materialize_all(&mut row_values);
        let tid = row_values
            .get(stmt.target_ctid_index)
            .ok_or(ExecError::DetailedError {
                message: "update input row is missing target ctid marker".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })
            .and_then(parse_tid_text)?
            .ok_or(ExecError::DetailedError {
                message: "update input row is missing target ctid marker".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let relation_oid = row_values
            .get(stmt.target_tableoid_index)
            .ok_or(ExecError::DetailedError {
                message: "update input row is missing target tableoid marker".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })
            .and_then(parse_update_tableoid)?;
        let target_index = *target_indexes
            .get(&relation_oid)
            .ok_or(ExecError::DetailedError {
                message: format!(
                    "update input row referenced unexpected target relation OID {relation_oid}"
                ),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let target = &stmt.targets[target_index];
        let source_values =
            row_values[stmt.target_visible_count..stmt.visible_column_count].to_vec();
        let old_values = fetch_update_target_values(target, tid, ctx)?;
        let new_values =
            evaluate_update_from_assignments(target, &old_values, &source_values, tid, ctx)?;
        events.push(UpdateRowEvent {
            target: target.clone(),
            tid,
            old_values,
            new_values,
        });
    }
    Ok(events)
}

pub(crate) fn apply_base_update_row(
    target: &BoundUpdateTarget,
    tid: ItemPointerData,
    old_values: Vec<Value>,
    new_values: Vec<Value>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<bool, ExecError> {
    let desc = Rc::new(target.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = target
        .predicate
        .as_ref()
        .map(|p| compile_predicate_with_decoder(p, &decoder));
    let mut current_tid = tid;
    let mut current_old_values = old_values;
    let mut current_values = new_values;
    loop {
        ctx.check_for_interrupts()?;
        materialize_generated_columns(&target.desc, &mut current_values, ctx)?;
        let old_tuple = heap_fetch(&*ctx.pool, ctx.client_id, target.rel, current_tid)?;
        crate::backend::executor::enforce_row_security_write_checks(
            &target.relation_name,
            &target.desc,
            &target.rls_write_checks,
            &current_values,
            ctx,
        )?;
        crate::backend::executor::enforce_relation_constraints(
            &target.relation_name,
            &target.desc,
            &target.relation_constraints,
            &current_values,
            ctx,
        )?;
        crate::backend::executor::enforce_outbound_foreign_keys(
            &target.relation_name,
            &target.relation_constraints.foreign_keys,
            Some(&current_old_values),
            &current_values,
            ctx,
        )?;
        let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_update(
            &target.relation_name,
            &target.referenced_by_foreign_keys,
            &current_old_values,
            &current_values,
            ctx,
            xid,
            cid,
            waiter,
        )?;
        let (current_replacement, toasted) = toast_tuple_for_write(
            &target.desc,
            &current_values,
            target.toast,
            target.toast_index.as_ref(),
            ctx,
            xid,
            cid,
        )?;
        match heap_update_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            target.rel,
            &ctx.txns,
            xid,
            cid,
            current_tid,
            &current_replacement,
            waiter,
        ) {
            Ok(new_tid) => {
                if let Some(toast) = target.toast {
                    delete_external_from_tuple(ctx, toast, &target.desc, &old_tuple, xid)?;
                }
                maintain_indexes_for_row(
                    target.rel,
                    &target.desc,
                    &target.indexes,
                    &current_values,
                    new_tid,
                    ctx,
                )?;
                validate_pending_set_default_rechecks(pending_set_default_rechecks, ctx)?;
                return Ok(true);
            }
            Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                let mut new_slot = TupleSlot::from_heap_tuple(
                    Rc::clone(&desc),
                    Rc::clone(&attr_descs),
                    new_ctid,
                    new_tuple,
                );
                new_slot.toast = slot_toast_context(target.toast, ctx);
                let passes = match &qual {
                    Some(q) => q(&mut new_slot, ctx)?,
                    None => true,
                };
                if !passes {
                    return Ok(false);
                }
                let new_values_base = new_slot.into_values()?;
                let mut eval_slot = TupleSlot::virtual_row(new_values_base.clone());
                let mut updated_values = new_values_base.clone();
                for assignment in &target.assignments {
                    let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                    apply_assignment_target(
                        &target.desc,
                        &mut updated_values,
                        &BoundAssignmentTarget {
                            column_index: assignment.column_index,
                            subscripts: assignment.subscripts.clone(),
                            field_path: assignment.field_path.clone(),
                            target_sql_type: assignment.target_sql_type,
                        },
                        value,
                        &mut eval_slot,
                        ctx,
                    )?;
                }
                current_old_values = new_values_base;
                current_values = updated_values;
                current_tid = new_ctid;
            }
            Err(HeapError::TupleAlreadyModified(_)) => {
                cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                return Ok(false);
            }
            Err(err) => {
                cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                return Err(err.into());
            }
        }
    }
}

pub(crate) fn materialize_delete_row_events(
    stmt: &BoundDeleteStatement,
    ctx: &mut ExecutorContext,
) -> Result<Vec<DeleteRowEvent>, ExecError> {
    let mut events = Vec::new();
    for target in &stmt.targets {
        let rows = match &target.row_source {
            BoundModifyRowSource::Heap => collect_matching_rows_heap(
                target.rel,
                &target.desc,
                target.toast,
                target.predicate.as_ref(),
                ctx,
            )?,
            BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                target.rel,
                &target.desc,
                target.toast,
                index,
                keys,
                target.predicate.as_ref(),
                ctx,
            )?,
        };
        for (tid, old_values) in rows {
            events.push(DeleteRowEvent {
                target: target.clone(),
                tid,
                old_values,
            });
        }
    }
    Ok(events)
}

pub(crate) fn apply_base_delete_row(
    target: &BoundDeleteTarget,
    tid: ItemPointerData,
    old_values: Vec<Value>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<bool, ExecError> {
    let desc = Rc::new(target.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = target
        .predicate
        .as_ref()
        .map(|p| compile_predicate_with_decoder(p, &decoder));
    let snapshot = ctx.snapshot.clone();
    let mut current_tid = tid;
    let mut current_values = old_values;
    loop {
        ctx.check_for_interrupts()?;
        let pending_set_default_rechecks = apply_inbound_foreign_key_actions_on_delete(
            &target.relation_name,
            &target.referenced_by_foreign_keys,
            &current_values,
            ctx,
            xid,
            waiter,
        )?;
        let old_tuple = if target.toast.is_some() {
            Some(heap_fetch(
                &*ctx.pool,
                ctx.client_id,
                target.rel,
                current_tid,
            )?)
        } else {
            None
        };
        match heap_delete_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            target.rel,
            &ctx.txns,
            xid,
            current_tid,
            &snapshot,
            waiter,
        ) {
            Ok(()) => {
                if let (Some(toast), Some(old_tuple)) = (target.toast, old_tuple.as_ref()) {
                    delete_external_from_tuple(ctx, toast, &target.desc, old_tuple, xid)?;
                }
                validate_pending_set_default_rechecks(pending_set_default_rechecks, ctx)?;
                return Ok(true);
            }
            Err(HeapError::TupleAlreadyModified(_)) => return Ok(false),
            Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                let mut new_slot = TupleSlot::from_heap_tuple(
                    Rc::clone(&desc),
                    Rc::clone(&attr_descs),
                    new_ctid,
                    new_tuple,
                );
                new_slot.toast = slot_toast_context(target.toast, ctx);
                let passes = match &qual {
                    Some(q) => q(&mut new_slot, ctx)?,
                    None => true,
                };
                if !passes {
                    return Ok(false);
                }
                current_values = new_slot.into_values()?;
                current_tid = new_ctid;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
