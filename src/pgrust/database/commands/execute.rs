use super::super::*;
use crate::backend::commands::rolecmds::PasswordSettings;
use crate::backend::executor::{
    ExecutorTransactionState, Expr, MaterializedCteTable, MaterializedRow,
    SharedExecutorTransactionState, TupleSlot, cast_value_with_source_type_catalog_and_config,
    eval_expr, execute_planned_stmt, execute_readonly_statement_with_config,
};
use crate::backend::parser::{
    BoundCte, BoundIndexRelation, BoundModifyingCte, BoundRelation, BoundWritableCte,
    CatalogLookup, CommonTableExpr, CteBody, DeleteStatement, FromItem, InsertSource,
    InsertStatement, ParseOptions, PreparedExternalParam, RuleEvent, SelectStatement,
    UpdateStatement, bind_delete_with_outer_scopes_and_ctes,
    bind_insert_with_outer_scopes_and_ctes, bind_scalar_expr_in_named_slot_scope,
    bind_update_with_outer_scopes_and_ctes, bound_cte_from_query_columns,
    cte_body_references_table, delete_statement_references_table,
    insert_statement_references_table, merge_statement_references_table,
    pg_plan_query_with_outer_scopes_and_ctes, pg_plan_values_query_with_outer_scopes_and_ctes,
    plan_merge_with_outer_ctes, resolve_raw_type_name, select_statement_references_table,
    update_statement_references_table, with_external_param_types,
};
use crate::backend::storage::lmgr::SerializableXactId;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::notices::push_warning_with_hint;
use crate::backend::utils::misc::stack_depth::StackDepthGuard;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::ReplicaIdentityKind;
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::primnodes::{QueryColumn, TargetEntry};
use crate::pl::plpgsql::execute_do_with_gucs;
use std::cell::RefCell;
use std::rc::Rc;

fn restrict_nonsystem_view_enabled(gucs: &std::collections::HashMap<String, String>) -> bool {
    gucs.get("restrict_nonsystem_relation_kind")
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().trim_matches('\'').eq_ignore_ascii_case("view"))
        })
        .unwrap_or(false)
}

fn normalize_direct_guc_name(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn parse_direct_bool_guc(value: &str) -> Option<bool> {
    match value
        .trim()
        .trim_matches('\'')
        .to_ascii_lowercase()
        .as_str()
    {
        "on" | "true" | "yes" | "1" | "t" => Some(true),
        "off" | "false" | "no" | "0" | "f" => Some(false),
        _ => None,
    }
}

fn direct_guc_default(name: &str) -> Option<&'static str> {
    match name {
        "enable_partitionwise_join" => Some("off"),
        "enable_partitionwise_aggregate" => Some("off"),
        "enable_seqscan"
        | "enable_indexscan"
        | "enable_indexonlyscan"
        | "enable_bitmapscan"
        | "enable_nestloop"
        | "enable_hashjoin"
        | "enable_mergejoin"
        | "enable_memoize"
        | "enable_material"
        | "enable_partition_pruning"
        | "enable_hashagg"
        | "enable_presorted_aggregate"
        | "enable_sort"
        | "enable_parallel_append"
        | "enable_parallel_hash"
        | "parallel_leader_participation" => Some("on"),
        "debug_parallel_query" => Some("off"),
        "lo_compat_privileges" => Some("off"),
        "max_parallel_workers" => Some("8"),
        "max_parallel_workers_per_gather" => Some("2"),
        "min_parallel_table_scan_size" => Some("8MB"),
        "min_parallel_index_scan_size" => Some("512kB"),
        "parallel_setup_cost" => Some("1000"),
        "parallel_tuple_cost" => Some("0.1"),
        "default_tablespace" => Some(""),
        _ => None,
    }
}

fn direct_bool_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: bool,
) -> bool {
    gucs.get(name)
        .and_then(|value| parse_direct_bool_guc(value))
        .unwrap_or(default)
}

fn direct_usize_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: usize,
) -> usize {
    gucs.get(name)
        .and_then(|value| value.trim().trim_matches('\'').parse::<usize>().ok())
        .unwrap_or(default)
}

fn direct_size_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: usize,
) -> usize {
    gucs.get(name)
        .and_then(|value| parse_direct_size_guc(value))
        .unwrap_or(default)
}

fn direct_f64_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: f64,
) -> f64 {
    gucs.get(name)
        .and_then(|value| value.trim().trim_matches('\'').parse::<f64>().ok())
        .unwrap_or(default)
}

fn parse_direct_size_guc(value: &str) -> Option<usize> {
    let value = value.trim().trim_matches('\'').trim();
    let split = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let number = value[..split].parse::<usize>().ok()?;
    let unit = value[split..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1,
        "kb" | "k" => 1024,
        "mb" | "m" => 1024 * 1024,
        "gb" | "g" => 1024 * 1024 * 1024,
        _ => return None,
    };
    number.checked_mul(multiplier)
}

fn direct_planner_config(gucs: &std::collections::HashMap<String, String>) -> PlannerConfig {
    PlannerConfig {
        enable_partitionwise_join: direct_bool_config(gucs, "enable_partitionwise_join", false),
        enable_partitionwise_aggregate: direct_bool_config(
            gucs,
            "enable_partitionwise_aggregate",
            false,
        ),
        enable_seqscan: direct_bool_config(gucs, "enable_seqscan", true),
        enable_indexscan: direct_bool_config(gucs, "enable_indexscan", true),
        enable_indexonlyscan: direct_bool_config(gucs, "enable_indexonlyscan", true),
        enable_bitmapscan: direct_bool_config(gucs, "enable_bitmapscan", true),
        enable_nestloop: direct_bool_config(gucs, "enable_nestloop", true),
        enable_hashjoin: direct_bool_config(gucs, "enable_hashjoin", true),
        enable_mergejoin: direct_bool_config(gucs, "enable_mergejoin", true),
        enable_memoize: direct_bool_config(gucs, "enable_memoize", true),
        enable_material: direct_bool_config(gucs, "enable_material", true),
        enable_partition_pruning: direct_bool_config(gucs, "enable_partition_pruning", true),
        constraint_exclusion_on: gucs
            .get("constraint_exclusion")
            .is_some_and(|value| value.eq_ignore_ascii_case("on")),
        constraint_exclusion_partition: gucs
            .get("constraint_exclusion")
            .map(|value| {
                value.eq_ignore_ascii_case("partition") || value.eq_ignore_ascii_case("on")
            })
            .unwrap_or(true),
        retain_partial_index_filters: false,
        enable_hashagg: direct_bool_config(gucs, "enable_hashagg", true),
        enable_presorted_aggregate: direct_bool_config(gucs, "enable_presorted_aggregate", true),
        enable_sort: direct_bool_config(gucs, "enable_sort", true),
        enable_parallel_append: direct_bool_config(gucs, "enable_parallel_append", true),
        enable_parallel_hash: direct_bool_config(gucs, "enable_parallel_hash", true),
        force_parallel_gather: direct_bool_config(gucs, "debug_parallel_query", false),
        max_parallel_workers: direct_usize_config(gucs, "max_parallel_workers", 8),
        max_parallel_workers_per_gather: direct_usize_config(
            gucs,
            "max_parallel_workers_per_gather",
            2,
        ),
        parallel_leader_participation: direct_bool_config(
            gucs,
            "parallel_leader_participation",
            true,
        ),
        min_parallel_table_scan_size: direct_size_config(
            gucs,
            "min_parallel_table_scan_size",
            8 * 1024 * 1024,
        ),
        min_parallel_index_scan_size: direct_size_config(
            gucs,
            "min_parallel_index_scan_size",
            512 * 1024,
        ),
        parallel_setup_cost: crate::include::nodes::plannodes::EstimateValue(direct_f64_config(
            gucs,
            "parallel_setup_cost",
            1000.0,
        )),
        parallel_tuple_cost: crate::include::nodes::plannodes::EstimateValue(direct_f64_config(
            gucs,
            "parallel_tuple_cost",
            0.1,
        )),
        fold_constants: true,
    }
}

fn relation_name_for_replica_identity_error(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn replica_identity_wrong_object(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42809",
    }
}

fn replica_identity_index_has_node_tree(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|sql| !sql.trim().is_empty())
}

fn validate_replica_identity_index(
    index_name: &str,
    index: &BoundIndexRelation,
    relation: &BoundRelation,
) -> Result<(), ExecError> {
    if !index.index_meta.indisunique {
        return Err(replica_identity_wrong_object(format!(
            "cannot use non-unique index \"{}\" as replica identity",
            index_name
        )));
    }
    if !index.index_meta.indimmediate || index.constraint_deferrable {
        return Err(replica_identity_wrong_object(format!(
            "cannot use non-immediate index \"{}\" as replica identity",
            index_name
        )));
    }
    if !index.index_exprs.is_empty()
        || replica_identity_index_has_node_tree(&index.index_meta.indexprs)
        || index.index_meta.indkey.iter().any(|attnum| *attnum <= 0)
    {
        return Err(replica_identity_wrong_object(format!(
            "cannot use expression index \"{}\" as replica identity",
            index_name
        )));
    }
    if index.index_predicate.is_some()
        || replica_identity_index_has_node_tree(&index.index_meta.indpred)
    {
        return Err(replica_identity_wrong_object(format!(
            "cannot use partial index \"{}\" as replica identity",
            index_name
        )));
    }

    let key_attnums = usize::try_from(index.index_meta.indnkeyatts.max(0)).unwrap_or(0);
    for attnum in index.index_meta.indkey.iter().take(key_attnums) {
        if *attnum <= 0 {
            return Err(replica_identity_wrong_object(format!(
                "cannot use expression index \"{}\" as replica identity",
                index_name
            )));
        }
        let column_index = usize::try_from(*attnum - 1).map_err(|_| {
            replica_identity_wrong_object(format!(
                "cannot use expression index \"{}\" as replica identity",
                index_name
            ))
        })?;
        let Some(column) = relation.desc.columns.get(column_index) else {
            return Err(replica_identity_wrong_object(format!(
                "cannot use expression index \"{}\" as replica identity",
                index_name
            )));
        };
        if column.dropped {
            return Err(replica_identity_wrong_object(format!(
                "cannot use expression index \"{}\" as replica identity",
                index_name
            )));
        }
        if column.storage.nullable {
            return Err(replica_identity_wrong_object(format!(
                "index \"{}\" cannot be used as replica identity because column \"{}\" is nullable",
                index_name, column.name
            )));
        }
    }

    Ok(())
}

fn replica_identity_kind_for_alter_table(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    table_name: &str,
    identity: &ReplicaIdentityKind,
) -> Result<(char, Option<u32>), ExecError> {
    match identity {
        ReplicaIdentityKind::Default => Ok(('d', None)),
        ReplicaIdentityKind::Full => Ok(('f', None)),
        ReplicaIdentityKind::Nothing => Ok(('n', None)),
        ReplicaIdentityKind::Index(index_name) => {
            let indexes = catalog.index_relations_for_heap(relation.relation_oid);
            if let Some(index) = indexes
                .into_iter()
                .find(|index| index.name.eq_ignore_ascii_case(index_name))
            {
                validate_replica_identity_index(index_name, &index, relation)?;
                return Ok(('i', Some(index.relation_oid)));
            }

            if catalog
                .lookup_any_relation(index_name)
                .is_some_and(|entry| matches!(entry.relkind, 'i' | 'I'))
            {
                return Err(replica_identity_wrong_object(format!(
                    "\"{}\" is not an index for table \"{}\"",
                    index_name,
                    relation_name_for_replica_identity_error(table_name)
                )));
            }

            Err(ExecError::Parse(
                crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "index on table",
                    actual: format!(
                        "index \"{}\" for table \"{}\" does not exist",
                        index_name,
                        relation_name_for_replica_identity_error(table_name)
                    ),
                },
            ))
        }
    }
}

fn refresh_autocommit_snapshot_after_lock_wait(
    db: &Database,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waited_for_lock: bool,
) -> Result<(), ExecError> {
    if waited_for_lock {
        ctx.snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct PreparedExternalBinding {
    paramid: usize,
    expr: Expr,
    ty: SqlType,
}

fn bind_prepared_external_params(
    params: &[PreparedExternalParam],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<PreparedExternalBinding>, ExecError> {
    params
        .iter()
        .map(|param| {
            let (expr, inferred) =
                bind_scalar_expr_in_named_slot_scope(&param.arg, &[], &[], catalog, &[])
                    .map_err(ExecError::Parse)?;
            let ty = match &param.type_name {
                Some(type_name) => {
                    resolve_raw_type_name(type_name, catalog).map_err(ExecError::Parse)?
                }
                None => inferred,
            };
            Ok(PreparedExternalBinding {
                paramid: param.paramid,
                expr,
                ty,
            })
        })
        .collect()
}

fn prepared_external_types(bindings: &[PreparedExternalBinding]) -> Vec<(usize, SqlType)> {
    bindings
        .iter()
        .map(|binding| (binding.paramid, binding.ty))
        .collect()
}

fn install_prepared_external_params(
    bindings: &[PreparedExternalBinding],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let mut slot = TupleSlot::empty(0);
    for binding in bindings {
        let value = eval_expr(&binding.expr, &mut slot, ctx)?;
        let value = cast_value_with_source_type_catalog_and_config(
            value,
            None,
            binding.ty,
            ctx.catalog.as_deref(),
            &ctx.datetime_config,
        )?;
        ctx.expr_bindings
            .external_params
            .insert(binding.paramid, value);
    }
    Ok(())
}

fn reject_restricted_view_access(name: &str, catalog: &dyn CatalogLookup) -> Result<(), ExecError> {
    let Some(entry) = catalog.lookup_any_relation(name) else {
        return Ok(());
    };
    if entry.relkind == 'v'
        && entry.namespace_oid != crate::include::catalog::PG_CATALOG_NAMESPACE_OID
    {
        return Err(ExecError::DetailedError {
            message: format!("access to non-system view \"{name}\" is restricted"),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    Ok(())
}

fn reject_restricted_views_in_select(
    select: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &select.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    if let Some(from) = &select.from {
        reject_restricted_views_in_from_item(from, catalog)?;
    }
    if let Some(set_op) = &select.set_operation {
        for input in &set_op.inputs {
            reject_restricted_views_in_select(input, catalog)?;
        }
    }
    Ok(())
}

fn reject_restricted_bound_view_refs_in_select(
    select: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    let mut relation_oids = std::collections::BTreeSet::new();
    let mut visible_ctes = Vec::new();
    collect_direct_relation_oids_from_select(
        select,
        catalog,
        &mut visible_ctes,
        &mut relation_oids,
    );
    for relation_oid in relation_oids {
        let Some(row) = catalog.class_row_by_oid(relation_oid) else {
            continue;
        };
        if row.relkind == 'v'
            && row.relnamespace != crate::include::catalog::PG_CATALOG_NAMESPACE_OID
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "access to non-system view \"{}\" is restricted",
                    row.relname
                ),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
    }
    Ok(())
}

fn reject_restricted_views_in_planned_stmt(
    planned_stmt: &crate::include::nodes::plannodes::PlannedStmt,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for requirement in &planned_stmt.relation_privileges {
        if requirement.relkind != 'v' {
            continue;
        }
        let Some(row) = catalog.class_row_by_oid(requirement.relation_oid) else {
            continue;
        };
        if row.relnamespace != crate::include::catalog::PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::DetailedError {
                message: format!(
                    "access to non-system view \"{}\" is restricted",
                    row.relname
                ),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
    }
    reject_restricted_views_in_plan(&planned_stmt.plan_tree, catalog)?;
    for subplan in &planned_stmt.subplans {
        reject_restricted_views_in_plan(subplan, catalog)?;
    }
    Ok(())
}

fn reject_restricted_view_oid(
    relation_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    let Some(row) = catalog.class_row_by_oid(relation_oid) else {
        return Ok(());
    };
    if row.relkind == 'v' && row.relnamespace != crate::include::catalog::PG_CATALOG_NAMESPACE_OID {
        return Err(ExecError::DetailedError {
            message: format!(
                "access to non-system view \"{}\" is restricted",
                row.relname
            ),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    Ok(())
}

fn reject_restricted_views_in_plan(
    plan: &crate::include::nodes::plannodes::Plan,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    use crate::include::nodes::plannodes::Plan;
    match plan {
        Plan::Result { .. } | Plan::WorkTableScan { .. } | Plan::FunctionScan { .. } => Ok(()),
        Plan::SeqScan { relation_oid, .. }
        | Plan::TidScan { relation_oid, .. }
        | Plan::TidRangeScan { relation_oid, .. }
        | Plan::IndexOnlyScan { relation_oid, .. }
        | Plan::IndexScan { relation_oid, .. }
        | Plan::BitmapIndexScan { relation_oid, .. } => {
            reject_restricted_view_oid(*relation_oid, catalog)
        }
        Plan::BitmapHeapScan {
            relation_oid,
            bitmapqual,
            ..
        } => {
            reject_restricted_view_oid(*relation_oid, catalog)?;
            reject_restricted_views_in_plan(bitmapqual, catalog)
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                reject_restricted_views_in_plan(child, catalog)?;
            }
            Ok(())
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => reject_restricted_views_in_plan(input, catalog),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            reject_restricted_views_in_plan(left, catalog)?;
            reject_restricted_views_in_plan(right, catalog)
        }
        Plan::CteScan { cte_plan, .. } => reject_restricted_views_in_plan(cte_plan, catalog),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            reject_restricted_views_in_plan(anchor, catalog)?;
            reject_restricted_views_in_plan(recursive, catalog)
        }
        Plan::Values { .. } => Ok(()),
    }
}

fn reject_restricted_views_in_cte_body(
    body: &CteBody,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    match body {
        CteBody::Select(select) => reject_restricted_views_in_select(select, catalog),
        CteBody::Values(_) => Ok(()),
        CteBody::Insert(insert) => reject_restricted_views_in_insert(insert, catalog),
        CteBody::Update(update) => reject_restricted_views_in_update(update, catalog),
        CteBody::Delete(delete) => reject_restricted_views_in_delete(delete, catalog),
        CteBody::Merge(merge) => reject_restricted_views_in_merge(merge, catalog),
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            reject_restricted_views_in_cte_body(anchor, catalog)?;
            reject_restricted_views_in_select(recursive, catalog)
        }
    }
}

fn reject_restricted_views_in_from_item(
    item: &FromItem,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    match item {
        FromItem::Table { name, .. } => reject_restricted_view_access(name, catalog),
        FromItem::DerivedTable(select) => reject_restricted_views_in_select(select, catalog),
        FromItem::Join { left, right, .. } => {
            reject_restricted_views_in_from_item(left, catalog)?;
            reject_restricted_views_in_from_item(right, catalog)
        }
        FromItem::Alias { source, .. }
        | FromItem::Lateral(source)
        | FromItem::TableSample { source, .. } => {
            reject_restricted_views_in_from_item(source, catalog)
        }
        FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::RowsFrom { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_) => Ok(()),
    }
}

fn reject_restricted_views_in_insert(
    insert: &InsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &insert.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    reject_restricted_view_access(&insert.table_name, catalog)?;
    if let InsertSource::Select(select) = &insert.source {
        reject_restricted_views_in_select(select, catalog)?;
    }
    Ok(())
}

fn reject_restricted_views_in_update(
    update: &UpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &update.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    reject_restricted_view_access(&update.table_name, catalog)?;
    if let Some(from) = &update.from {
        reject_restricted_views_in_from_item(from, catalog)?;
    }
    Ok(())
}

fn reject_restricted_views_in_delete(
    delete: &DeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &delete.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    reject_restricted_view_access(&delete.table_name, catalog)?;
    if let Some(using) = &delete.using {
        reject_restricted_views_in_from_item(using, catalog)?;
    }
    Ok(())
}

fn reject_restricted_views_in_merge(
    merge: &crate::backend::parser::MergeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &merge.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    reject_restricted_view_access(&merge.target_table, catalog)?;
    reject_restricted_views_in_from_item(&merge.source, catalog)
}

fn autocommit_datetime_config(config: &DateTimeConfig) -> DateTimeConfig {
    let statement_timestamp_usecs = config
        .statement_timestamp_usecs
        .unwrap_or_else(crate::backend::utils::time::datetime::current_postgres_timestamp_usecs);
    let transaction_timestamp_usecs = config
        .transaction_timestamp_usecs
        .unwrap_or(statement_timestamp_usecs);
    let mut config = config.clone();
    config.statement_timestamp_usecs = Some(statement_timestamp_usecs);
    config.transaction_timestamp_usecs = Some(transaction_timestamp_usecs);
    config
}

fn statement_timestamp_usecs(config: &DateTimeConfig) -> i64 {
    config
        .statement_timestamp_usecs
        .unwrap_or_else(crate::backend::utils::time::datetime::current_postgres_timestamp_usecs)
}

fn apply_writable_cte_column_aliases(
    cte: &CommonTableExpr,
    mut columns: Vec<QueryColumn>,
) -> Result<Vec<QueryColumn>, ExecError> {
    if cte.column_names.is_empty() {
        return Ok(columns);
    }
    if cte.column_names.len() != columns.len() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CTE column alias count matching query width",
            actual: format!(
                "CTE query has {} columns but {} column aliases were specified",
                columns.len(),
                cte.column_names.len()
            ),
        }));
    }
    for (column, name) in columns.iter_mut().zip(cte.column_names.iter()) {
        column.name = name.clone();
    }
    Ok(columns)
}

fn cte_body_has_writable(body: &CteBody) -> bool {
    match body {
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_) => true,
        CteBody::Select(select) => select_has_writable_ctes(select),
        CteBody::Values(values) => values
            .with
            .iter()
            .any(|cte| cte_body_has_writable(&cte.body)),
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            cte_body_has_writable(anchor)
                || recursive
                    .with
                    .iter()
                    .any(|cte| cte_body_has_writable(&cte.body))
        }
    }
}

fn select_has_writable_ctes(select: &SelectStatement) -> bool {
    select
        .with
        .iter()
        .any(|cte| cte_body_has_writable(&cte.body))
        || select
            .from
            .as_ref()
            .is_some_and(from_item_has_writable_ctes)
        || select
            .set_operation
            .as_ref()
            .is_some_and(|setop| setop.inputs.iter().any(select_has_writable_ctes))
}

fn select_has_non_top_level_writable_ctes(select: &SelectStatement) -> bool {
    select
        .from
        .as_ref()
        .is_some_and(from_item_has_writable_ctes)
        || select
            .set_operation
            .as_ref()
            .is_some_and(|setop| setop.inputs.iter().any(select_has_writable_ctes))
}

fn from_item_has_writable_ctes(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. }
        | FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::RowsFrom { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_) => false,
        FromItem::TableSample { source, .. }
        | FromItem::Lateral(source)
        | FromItem::Alias { source, .. } => from_item_has_writable_ctes(source),
        FromItem::DerivedTable(select) => select_has_writable_ctes(select),
        FromItem::Join { left, right, .. } => {
            from_item_has_writable_ctes(left) || from_item_has_writable_ctes(right)
        }
    }
}

fn cte_body_is_modifying(body: &CteBody) -> bool {
    matches!(
        body,
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_)
    )
}

fn modifying_cte_body_has_returning(body: &CteBody) -> bool {
    match body {
        CteBody::Insert(insert) => !insert.returning.is_empty(),
        CteBody::Update(update) => !update.returning.is_empty(),
        CteBody::Delete(delete) => !delete.returning.is_empty(),
        CteBody::Merge(merge) => !merge.returning.is_empty(),
        _ => false,
    }
}

fn modifying_cte_body_has_nested_modifying_ctes(body: &CteBody) -> bool {
    let nested = match body {
        CteBody::Insert(insert) => &insert.with,
        CteBody::Update(update) => &update.with,
        CteBody::Delete(delete) => &delete.with,
        CteBody::Merge(merge) => &merge.with,
        _ => return false,
    };
    nested.iter().any(|cte| cte_body_has_writable(&cte.body))
}

fn prepend_ctes_to_modifying_body(
    body: &CteBody,
    ctes: &[CommonTableExpr],
    with_recursive: bool,
) -> CteBody {
    match body {
        CteBody::Insert(insert) => {
            let mut insert = (**insert).clone();
            let mut with = ctes.to_vec();
            with.extend(insert.with);
            insert.with = with;
            insert.with_recursive = insert.with_recursive || (with_recursive && !ctes.is_empty());
            CteBody::Insert(Box::new(insert))
        }
        CteBody::Update(update) => {
            let mut update = (**update).clone();
            let mut with = ctes.to_vec();
            with.extend(update.with);
            update.with = with;
            update.with_recursive = update.with_recursive || (with_recursive && !ctes.is_empty());
            CteBody::Update(Box::new(update))
        }
        CteBody::Delete(delete) => {
            let mut delete = (**delete).clone();
            let mut with = ctes.to_vec();
            with.extend(delete.with);
            delete.with = with;
            delete.with_recursive = delete.with_recursive || (with_recursive && !ctes.is_empty());
            CteBody::Delete(Box::new(delete))
        }
        CteBody::Merge(merge) => {
            let mut merge = (**merge).clone();
            let mut with = ctes.to_vec();
            with.extend(merge.with);
            merge.with = with;
            merge.with_recursive = merge.with_recursive || (with_recursive && !ctes.is_empty());
            CteBody::Merge(Box::new(merge))
        }
        _ => body.clone(),
    }
}

fn modifying_cte_reference_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupportedMessage(format!(
        "WITH query \"{name}\" does not have a RETURNING clause"
    )))
}

fn recursive_modifying_cte_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::InvalidRecursion(format!(
        "recursive query \"{name}\" must not contain data-modifying statements"
    )))
}

fn nested_modifying_cte_error() -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupportedMessage(
        "WITH clause containing a data-modifying statement must be at the top level".into(),
    ))
}

fn cte_referenced_after<F>(
    ctes: &[CommonTableExpr],
    cte_index: usize,
    name: &str,
    with_recursive: bool,
    outer_references: &F,
) -> bool
where
    F: Fn(&str) -> bool,
{
    let referenced_by_ctes = if with_recursive {
        ctes.iter()
            .enumerate()
            .any(|(index, cte)| index != cte_index && cte_body_references_table(&cte.body, name))
    } else {
        ctes.iter()
            .skip(cte_index + 1)
            .any(|later| cte_body_references_table(&later.body, name))
    };
    referenced_by_ctes || outer_references(name)
}

fn modifying_cte_execution_order(
    ctes: &[CommonTableExpr],
    with_recursive: bool,
) -> Result<Vec<usize>, ExecError> {
    let modifying_indexes = ctes
        .iter()
        .enumerate()
        .filter_map(|(index, cte)| cte_body_is_modifying(&cte.body).then_some(index))
        .collect::<Vec<_>>();
    if !with_recursive {
        return Ok(modifying_indexes);
    }

    fn visit(
        ctes: &[CommonTableExpr],
        modifying_indexes: &[usize],
        index: usize,
        state: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<(), ExecError> {
        match state[index] {
            1 => return Err(recursive_modifying_cte_error(&ctes[index].name)),
            2 => return Ok(()),
            _ => {}
        }
        state[index] = 1;
        for &dependency in modifying_indexes {
            if dependency != index
                && cte_body_references_table(&ctes[index].body, &ctes[dependency].name)
            {
                visit(ctes, modifying_indexes, dependency, state, order)?;
            }
        }
        state[index] = 2;
        order.push(index);
        Ok(())
    }

    let mut state = vec![0; ctes.len()];
    let mut order = Vec::with_capacity(modifying_indexes.len());
    for &index in &modifying_indexes {
        visit(ctes, &modifying_indexes, index, &mut state, &mut order)?;
    }
    Ok(order)
}

fn visible_select_ctes_for_modifying_body(
    ctes: &[CommonTableExpr],
    cte_index: usize,
    with_recursive: bool,
    all_select_ctes: &[CommonTableExpr],
) -> Vec<CommonTableExpr> {
    if with_recursive {
        return all_select_ctes.to_vec();
    }
    ctes.iter()
        .take(cte_index)
        .filter(|cte| !cte_body_is_modifying(&cte.body))
        .cloned()
        .collect()
}

fn bound_returning_columns(targets: &[TargetEntry]) -> Vec<QueryColumn> {
    targets
        .iter()
        .map(|target| QueryColumn {
            name: target.name.clone(),
            sql_type: target.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

fn returning_columns_for_modifying_cte(
    cte: &CommonTableExpr,
    source: &BoundModifyingCte,
) -> Result<Vec<QueryColumn>, ExecError> {
    let targets = match source {
        BoundModifyingCte::Insert(stmt) => &stmt.returning,
        BoundModifyingCte::Update(stmt) => &stmt.returning,
        BoundModifyingCte::Delete(stmt) => &stmt.returning,
        BoundModifyingCte::Merge(stmt) => &stmt.returning,
    };
    apply_writable_cte_column_aliases(cte, bound_returning_columns(targets))
}

fn bind_modifying_cte_body(
    cte: &CommonTableExpr,
    catalog: &dyn CatalogLookup,
    materialized_ctes: &[BoundCte],
) -> Result<BoundModifyingCte, ExecError> {
    match &cte.body {
        CteBody::Insert(cte_insert) => Ok(BoundModifyingCte::Insert(
            bind_insert_with_outer_scopes_and_ctes(cte_insert, catalog, &[], materialized_ctes)?,
        )),
        CteBody::Update(cte_update) => Ok(BoundModifyingCte::Update(
            bind_update_with_outer_scopes_and_ctes(cte_update, catalog, &[], materialized_ctes)?,
        )),
        CteBody::Delete(cte_delete) => Ok(BoundModifyingCte::Delete(
            bind_delete_with_outer_scopes_and_ctes(cte_delete, catalog, &[], materialized_ctes)?,
        )),
        CteBody::Merge(cte_merge) => Ok(BoundModifyingCte::Merge(plan_merge_with_outer_ctes(
            cte_merge,
            catalog,
            materialized_ctes,
        )?)),
        _ => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "expected a data-modifying CTE".into(),
        ))),
    }
}

fn pin_bound_writable_cte_result(
    ctx: &mut ExecutorContext,
    bound: &BoundWritableCte,
    result: StatementResult,
) {
    if bound.cte.desc.columns.is_empty() {
        return;
    }
    let StatementResult::Query { rows, .. } = result else {
        return;
    };
    let rows = rows
        .into_iter()
        .map(|mut row| {
            Value::materialize_all(&mut row);
            MaterializedRow::new(TupleSlot::virtual_row(row), Vec::new())
        })
        .collect();
    ctx.pinned_cte_tables.insert(
        bound.cte.cte_id,
        Rc::new(RefCell::new(MaterializedCteTable { rows, eof: true })),
    );
}

fn oa_sql_tokens(sql: &str) -> Vec<String> {
    sql.split_whitespace()
        .map(oa_clean_sql_token)
        .filter(|token| !token.is_empty())
        .collect()
}

fn oa_clean_sql_token(token: &str) -> String {
    let trimmed = token.trim_matches(|ch: char| matches!(ch, ';' | ',' | '(' | ')'));
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
    }
    trimmed.to_string()
}

fn oa_token_after(tokens: &[String], pattern: &[&str]) -> Option<String> {
    tokens
        .windows(pattern.len().saturating_add(1))
        .find(|window| {
            pattern.iter().enumerate().all(|(idx, expected)| {
                window
                    .get(idx)
                    .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
            })
        })
        .and_then(|window| window.get(pattern.len()).cloned())
}

fn oa_first_token_after_prefix(sql: &str, prefix: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with(prefix) {
        return None;
    }
    trimmed
        .get(prefix.len()..)?
        .split_whitespace()
        .next()
        .map(oa_clean_sql_token)
}

fn oa_default_acl_objtype(name: &str) -> Result<char, ExecError> {
    match name.to_ascii_lowercase().as_str() {
        "table" | "tables" => Ok('r'),
        "sequence" | "sequences" => Ok('S'),
        "function" | "functions" | "routine" | "routines" => Ok('f'),
        "type" | "types" => Ok('T'),
        "schema" | "schemas" => Ok('n'),
        "large" | "large object" | "large objects" => Ok('L'),
        _ => Err(ExecError::DetailedError {
            message: format!("unrecognized default ACL object type \"{name}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn oa_unsupported_ddl(feature: &str, sql: &str) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupported(format!("{feature}: {sql}")))
}

impl Database {
    fn execute_bound_modifying_cte_autocommit(
        &self,
        client_id: ClientId,
        interrupts: &Arc<crate::backend::utils::misc::interrupts::InterruptState>,
        locked_rels: &mut Vec<crate::backend::storage::smgr::RelFileLocator>,
        source: &BoundModifyingCte,
        catalog: &dyn CatalogLookup,
        ctx: &mut ExecutorContext,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<StatementResult, ExecError> {
        match source {
            BoundModifyingCte::Insert(bound) => {
                let prepared =
                    super::rules::prepare_bound_insert_for_execution(bound.clone(), catalog)?;
                super::rules::enforce_modifying_cte_rule_restrictions(
                    prepared.stmt.relation_oid,
                    RuleEvent::Insert,
                    catalog,
                )?;
                let lock_requests = merge_table_lock_requests(
                    &insert_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                let waited_for_lock =
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                locked_rels.extend(table_lock_relations(&lock_requests));
                refresh_autocommit_snapshot_after_lock_wait(self, ctx, xid, cid, waited_for_lock)?;
                super::rules::execute_bound_insert_with_rules(prepared.stmt, catalog, ctx, xid, cid)
            }
            BoundModifyingCte::Update(bound) => {
                let prepared =
                    super::rules::prepare_bound_update_for_execution(bound.clone(), catalog)?;
                for target in &prepared.stmt.targets {
                    super::rules::enforce_modifying_cte_rule_restrictions(
                        target.relation_oid,
                        RuleEvent::Update,
                        catalog,
                    )?;
                }
                let lock_requests = merge_table_lock_requests(
                    &update_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                let waited_for_lock =
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                locked_rels.extend(table_lock_relations(&lock_requests));
                refresh_autocommit_snapshot_after_lock_wait(self, ctx, xid, cid, waited_for_lock)?;
                super::rules::execute_bound_update_with_rules(
                    prepared.stmt,
                    catalog,
                    ctx,
                    xid,
                    cid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                )
            }
            BoundModifyingCte::Delete(bound) => {
                let prepared =
                    super::rules::prepare_bound_delete_for_execution(bound.clone(), catalog)?;
                for target in &prepared.stmt.targets {
                    super::rules::enforce_modifying_cte_rule_restrictions(
                        target.relation_oid,
                        RuleEvent::Delete,
                        catalog,
                    )?;
                }
                let lock_requests = merge_table_lock_requests(
                    &delete_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                let waited_for_lock =
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                locked_rels.extend(table_lock_relations(&lock_requests));
                refresh_autocommit_snapshot_after_lock_wait(self, ctx, xid, cid, waited_for_lock)?;
                super::rules::execute_bound_delete_with_rules(
                    prepared.stmt,
                    catalog,
                    ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                )
            }
            BoundModifyingCte::Merge(bound) => crate::backend::commands::tablecmds::execute_merge(
                bound.clone(),
                catalog,
                ctx,
                xid,
                cid,
            ),
        }
    }

    fn execute_bound_modifying_ctes_autocommit(
        &self,
        client_id: ClientId,
        interrupts: &Arc<crate::backend::utils::misc::interrupts::InterruptState>,
        locked_rels: &mut Vec<crate::backend::storage::smgr::RelFileLocator>,
        bound_ctes: &[BoundWritableCte],
        catalog: &dyn CatalogLookup,
        ctx: &mut ExecutorContext,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<(), ExecError> {
        ctx.pinned_cte_tables.clear();
        for bound in bound_ctes {
            let result = self.execute_bound_modifying_cte_autocommit(
                client_id,
                interrupts,
                locked_rels,
                &bound.source,
                catalog,
                ctx,
                xid,
                cid,
            )?;
            pin_bound_writable_cte_result(ctx, bound, result);
        }
        Ok(())
    }

    fn bind_modifying_ctes_autocommit<F>(
        &self,
        ctes: &[CommonTableExpr],
        with_recursive: bool,
        catalog: &dyn CatalogLookup,
        outer_references: F,
    ) -> Result<(Vec<BoundWritableCte>, Vec<BoundCte>, Vec<CommonTableExpr>), ExecError>
    where
        F: Fn(&str) -> bool,
    {
        let mut bound_writable_ctes = Vec::new();
        let mut materialized_ctes = Vec::new();
        let mut remaining_ctes = Vec::new();

        for cte in ctes {
            if !cte_body_is_modifying(&cte.body) {
                if cte_body_has_writable(&cte.body) {
                    return Err(nested_modifying_cte_error());
                }
                remaining_ctes.push(cte.clone());
            }
        }

        let execution_order = modifying_cte_execution_order(ctes, with_recursive)?;
        for index in execution_order {
            let cte = &ctes[index];

            if cte_body_references_table(&cte.body, &cte.name) {
                return Err(recursive_modifying_cte_error(&cte.name));
            }
            if modifying_cte_body_has_nested_modifying_ctes(&cte.body) {
                return Err(nested_modifying_cte_error());
            }
            if !modifying_cte_body_has_returning(&cte.body)
                && cte_referenced_after(ctes, index, &cte.name, with_recursive, &outer_references)
            {
                return Err(modifying_cte_reference_error(&cte.name));
            }

            let mut executable = cte.clone();
            let visible_select_ctes = visible_select_ctes_for_modifying_body(
                ctes,
                index,
                with_recursive,
                &remaining_ctes,
            );
            executable.body =
                prepend_ctes_to_modifying_body(&cte.body, &visible_select_ctes, with_recursive);
            let source = bind_modifying_cte_body(&executable, catalog, &materialized_ctes)?;
            let columns = returning_columns_for_modifying_cte(cte, &source)?;
            let bound_cte = bound_cte_from_query_columns(cte.name.clone(), columns);
            if !bound_cte.desc.columns.is_empty() {
                materialized_ctes.push(bound_cte.clone());
            }
            bound_writable_ctes.push(BoundWritableCte {
                cte: bound_cte,
                source,
            });
        }

        Ok((bound_writable_ctes, materialized_ctes, remaining_ctes))
    }

    fn execute_object_address_unsupported_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::UnsupportedStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<Option<StatementResult>, ExecError> {
        match stmt.feature {
            "ALTER DEFAULT PRIVILEGES" => {
                self.execute_alter_default_privileges_for_object_address(
                    client_id,
                    &stmt.sql,
                    configured_search_path,
                )?;
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            "CREATE TRANSFORM" => {
                self.execute_create_transform_for_object_address(
                    client_id,
                    &stmt.sql,
                    configured_search_path,
                )?;
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            _ => Ok(None),
        }
    }

    fn execute_alter_default_privileges_for_object_address(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        // :HACK: This stores object-address identity plus the table ACL items
        // needed by new-relation creation. Full default privileges belong in
        // pg_default_acl-backed catalog state.
        if let Some(spec) = parse_large_object_default_privileges_sql(sql)? {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, 0)), configured_search_path);
            let role_oid = if let Some(role_name) = spec.role_name.as_deref() {
                catalog
                    .authid_rows()
                    .into_iter()
                    .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("role \"{role_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    })?
                    .oid
            } else {
                self.auth_state(client_id).current_user_oid()
            };
            let mut catalog_effects = Vec::new();
            let result = self
                .execute_alter_default_privileges_large_objects(
                    client_id,
                    role_oid,
                    &spec.grantee_names,
                    &spec.privilege_chars,
                    spec.with_grant_option,
                    spec.revoke,
                    xid,
                    0,
                    &mut catalog_effects,
                )
                .map(|()| StatementResult::AffectedRows(0));
            let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
            guard.disarm();
            result.map(|_| ())?;
            return Ok(());
        }
        let tokens = oa_sql_tokens(sql);
        let role_name = oa_token_after(&tokens, &["for", "role"]);
        let namespace_name = oa_token_after(&tokens, &["in", "schema"]);
        let object_kind = oa_token_after(&tokens, &["on"])
            .ok_or_else(|| oa_unsupported_ddl("ALTER DEFAULT PRIVILEGES", sql))?;
        let objtype = oa_default_acl_objtype(&object_kind)?;
        let is_grant = tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("grant"));
        let grantee_name = oa_token_after(&tokens, &[if is_grant { "to" } else { "from" }]);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let role = if let Some(role_name) = role_name {
            catalog
                .authid_rows()
                .into_iter()
                .find(|row| row.rolname.eq_ignore_ascii_case(&role_name))
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("role \"{role_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?
        } else {
            catalog
                .authid_rows()
                .into_iter()
                .find(|row| row.oid == self.auth_state(client_id).current_user_oid())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "role with OID {} does not exist",
                        self.auth_state(client_id).current_user_oid()
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?
        };
        let namespace = namespace_name
            .as_deref()
            .map(|name| {
                catalog
                    .namespace_rows()
                    .into_iter()
                    .find(|row| row.nspname.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })
            })
            .transpose()?;
        let namespace_oid = namespace.as_ref().map(|row| row.oid);
        let namespace_name = namespace.map(|row| row.nspname);
        let mut object_addresses = self.object_addresses.write();
        if is_grant
            && namespace_oid.is_none()
            && grantee_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(&role.rolname))
        {
            object_addresses.remove_default_acl(role.oid, namespace_oid, objtype);
        } else {
            let privilege_chars =
                crate::backend::catalog::object_address::default_acl_privilege_chars_from_tokens(
                    &tokens, objtype,
                );
            let acl_items =
                crate::backend::catalog::object_address::default_acl_items_for_object_address(
                    &role.rolname,
                    objtype,
                    is_grant,
                    grantee_name.as_deref(),
                    &privilege_chars,
                );
            object_addresses.upsert_default_acl(
                role.oid,
                role.rolname,
                namespace_oid,
                namespace_name,
                objtype,
                acl_items,
            );
        }
        Ok(())
    }

    fn execute_create_transform_for_object_address(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        // :HACK: Record transform identity only; transform execution is intentionally absent.
        let tokens = oa_sql_tokens(sql);
        let type_name = oa_token_after(&tokens, &["for"])
            .ok_or_else(|| oa_unsupported_ddl("CREATE TRANSFORM", sql))?;
        let language_name = oa_token_after(&tokens, &["language"])
            .ok_or_else(|| oa_unsupported_ddl("CREATE TRANSFORM", sql))?;
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let raw_type = crate::backend::parser::parse_type_name(&type_name).unwrap_or_else(|_| {
            crate::backend::parser::RawTypeName::Named {
                name: type_name.clone(),
                array_bounds: 0,
            }
        });
        let sql_type = crate::backend::parser::resolve_raw_type_name(&raw_type, &catalog)
            .map_err(ExecError::Parse)?;
        let type_oid = catalog
            .type_oid_for_sql_type(sql_type)
            .filter(|oid| *oid != 0)
            .unwrap_or(sql_type.type_oid);
        if type_oid == 0 {
            return Err(ExecError::Parse(ParseError::UnsupportedType(type_name)));
        }
        let language = catalog
            .language_row_by_name(&language_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("language \"{language_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        self.object_addresses
            .write()
            .upsert_transform(type_oid, language.oid);
        Ok(())
    }

    fn execute_create_subscription_for_object_address(&self, client_id: ClientId, sql: &str) {
        // :HACK: Store enough subscription identity for object-address regression coverage.
        if let Some(name) = oa_first_token_after_prefix(sql, "create subscription") {
            self.object_addresses
                .write()
                .upsert_subscription(name, self.auth_state(client_id).current_user_oid());
        }
        push_warning_with_hint(
            "subscription was created, but is not connected",
            "To initiate replication, you must manually create the replication slot, enable the subscription, and refresh the subscription.",
        );
    }

    fn execute_drop_subscription_for_object_address(&self, sql: &str) {
        if let Some(name) = oa_first_token_after_prefix(sql, "drop subscription") {
            self.object_addresses.write().drop_subscription(&name);
        }
    }

    pub(crate) fn execute_alter_table_replica_identity_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::AlterTableReplicaIdentityStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) =
            crate::pgrust::database::ddl::lookup_table_or_partitioned_table_for_alter_table(
                &catalog,
                &stmt.table_name,
                stmt.if_exists,
            )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let (identity, index_oid) = replica_identity_kind_for_alter_table(
            &catalog,
            &relation,
            &stmt.table_name,
            &stmt.identity,
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: None,
            interrupts,
        };
        let mut catalog_effects = Vec::new();
        let result = self
            .catalog
            .write()
            .set_replica_identity_mvcc(relation.relation_oid, identity, index_oid, &ctx)
            .map(|effect| {
                catalog_effects.push(effect);
                StatementResult::AffectedRows(0)
            })
            .map_err(map_catalog_error);
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_replica_identity_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::AlterTableReplicaIdentityStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) =
            crate::pgrust::database::ddl::lookup_table_or_partitioned_table_for_alter_table(
                &catalog,
                &stmt.table_name,
                stmt.if_exists,
            )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let (identity, index_oid) = replica_identity_kind_for_alter_table(
            &catalog,
            &relation,
            &stmt.table_name,
            &stmt.identity,
        )?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .set_replica_identity_mvcc(relation.relation_oid, identity, index_oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_truncate_table_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::TruncateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        ctx: &mut ExecutorContext,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        use crate::backend::commands::tablecmds::{
            check_truncate_relation_privileges, fire_after_truncate_triggers,
            fire_before_truncate_triggers, invalidate_relation_buffers_for_command,
            owned_sequence_oids_for_truncate, reinitialize_index_relation,
            resolve_explicit_truncate_relations, resolve_truncate_relations,
        };

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let targets = resolve_truncate_relations(stmt, &catalog, true)?;
        let privilege_targets = resolve_explicit_truncate_relations(stmt, &catalog)?;
        check_truncate_relation_privileges(&privilege_targets, ctx)?;
        let triggers = fire_before_truncate_triggers(&targets, &catalog, ctx)?;

        let mut rewrite_oids = Vec::new();
        let mut rewritten_index_oids = Vec::new();
        let mut truncated_relation_oids = Vec::new();
        for target in targets.iter().filter(|target| target.relkind == 'r') {
            // Transactional TRUNCATE swaps relfilenodes. Keep the pre-truncate
            // relfilenode durable before dropping its buffers so ROLLBACK can
            // restore the old table contents.
            if target.relpersistence == 't'
                && let Some(local) = self.existing_local_buffer_manager(client_id)
            {
                local.flush_relation(target.rel).map_err(|err| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Buffer(err))
                })?;
            }
            ctx.pool.flush_relation(target.rel).map_err(|err| {
                ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Buffer(err))
            })?;
            invalidate_relation_buffers_for_command(
                "TRUNCATE",
                pgrust_commands::truncate::relation_name_for_oid(&catalog, target.relation_oid),
                target,
                ctx,
            )?;
            if !truncated_relation_oids.contains(&target.relation_oid) {
                truncated_relation_oids.push(target.relation_oid);
            }

            if !rewrite_oids.contains(&target.relation_oid) {
                rewrite_oids.push(target.relation_oid);
            }
            for index in catalog.index_relations_for_heap(target.relation_oid) {
                if !rewrite_oids.contains(&index.relation_oid) {
                    rewrite_oids.push(index.relation_oid);
                }
                if !rewritten_index_oids.contains(&index.relation_oid) {
                    rewritten_index_oids.push(index.relation_oid);
                }
            }
            if let Some(toast) = target.toast {
                if !rewrite_oids.contains(&toast.relation_oid) {
                    rewrite_oids.push(toast.relation_oid);
                }
                for index in catalog.index_relations_for_heap(toast.relation_oid) {
                    if !rewrite_oids.contains(&index.relation_oid) {
                        rewrite_oids.push(index.relation_oid);
                    }
                    if !rewritten_index_oids.contains(&index.relation_oid) {
                        rewritten_index_oids.push(index.relation_oid);
                    }
                }
            }
        }

        if !rewrite_oids.is_empty() {
            let write_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .rewrite_relation_storage_mvcc(&rewrite_oids, &write_ctx)?;
            let rewrites = rewrite_oids
                .iter()
                .copied()
                .zip(effect.created_rels.iter().copied())
                .collect::<Vec<_>>();
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            self.record_truncate_temp_rewrites(client_id, &rewrites, temp_effects)?;
            {
                let stats_state = self.session_stats_state(client_id);
                let mut stats_state = stats_state.write();
                for relation_oid in truncated_relation_oids {
                    stats_state.note_relation_truncate(relation_oid);
                }
            }
            catalog_effects.push(effect);
            ctx.next_command_id = ctx.next_command_id.max(cid.saturating_add(1));
            ctx.snapshot.current_cid = ctx.snapshot.current_cid.max(ctx.next_command_id);
        }
        let refreshed_catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, cid.saturating_add(1))),
            configured_search_path,
        );
        for target in targets.iter().filter(|target| target.relkind == 'r') {
            for index in refreshed_catalog.index_relations_for_heap(target.relation_oid) {
                if rewritten_index_oids.contains(&index.relation_oid)
                    && index.index_meta.indisvalid
                    && index.index_meta.indisready
                {
                    reinitialize_index_relation(&index, ctx, xid)?;
                }
            }
            if let Some(toast) = target.toast {
                for index in refreshed_catalog.index_relations_for_heap(toast.relation_oid) {
                    if rewritten_index_oids.contains(&index.relation_oid)
                        && index.index_meta.indisvalid
                        && index.index_meta.indisready
                    {
                        reinitialize_index_relation(&index, ctx, xid)?;
                    }
                }
            }
        }
        if stmt.restart_identity {
            for sequence_oid in owned_sequence_oids_for_truncate(&targets, &catalog) {
                let persistent = catalog
                    .relation_by_oid(sequence_oid)
                    .is_some_and(|relation| relation.relpersistence != 't');
                let Some(mut data) = self.sequences.sequence_data(sequence_oid, persistent)? else {
                    continue;
                };
                data.state.last_value = data.options.start;
                data.state.log_cnt = 0;
                data.state.is_called = false;
                sequence_effects.push(self.sequences.apply_upsert(sequence_oid, data, persistent));
            }
        }
        ctx.catalog = Some(crate::backend::executor::executor_catalog(
            refreshed_catalog.clone(),
        ));
        fire_after_truncate_triggers(&triggers, ctx)?;
        Ok(StatementResult::AffectedRows(0))
    }

    fn record_truncate_temp_rewrites(
        &self,
        client_id: ClientId,
        rewrites: &[(u32, crate::backend::storage::smgr::RelFileLocator)],
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<(), ExecError> {
        for (relation_oid, new_rel) in rewrites {
            if let Ok(old_rel) = self.replace_temp_entry_rel(client_id, *relation_oid, *new_rel) {
                temp_effects.push(TempMutationEffect::ReplaceRel {
                    relation_oid: *relation_oid,
                    old_rel,
                    new_rel: *new_rel,
                });
            }
        }
        Ok(())
    }

    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            None,
            &DateTimeConfig::default(),
        )
    }

    fn direct_gucs_for_client(
        &self,
        client_id: ClientId,
    ) -> std::collections::HashMap<String, String> {
        self.session_guc_states
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_default()
    }

    fn apply_direct_guc_statement(
        &self,
        client_id: ClientId,
        stmt: &Statement,
    ) -> Result<Option<StatementResult>, ExecError> {
        match stmt {
            Statement::Set(set_stmt) => {
                let name = normalize_direct_guc_name(&set_stmt.name);
                if direct_guc_default(&name).is_some() {
                    let mut states = self.session_guc_states.write();
                    let gucs = states.entry(client_id).or_default();
                    if let Some(value) = set_stmt.value.as_ref() {
                        if name == "default_tablespace" {
                            gucs.insert(name, value.trim().trim_matches('\'').to_string());
                        } else if matches!(
                            name.as_str(),
                            "max_parallel_workers" | "max_parallel_workers_per_gather"
                        ) {
                            value
                                .trim()
                                .trim_matches('\'')
                                .parse::<usize>()
                                .map_err(|_| {
                                    ExecError::Parse(ParseError::UnrecognizedParameter(
                                        value.clone(),
                                    ))
                                })?;
                            gucs.insert(name, value.trim().trim_matches('\'').to_string());
                        } else if matches!(
                            name.as_str(),
                            "min_parallel_table_scan_size" | "min_parallel_index_scan_size"
                        ) {
                            parse_direct_size_guc(value).ok_or_else(|| {
                                ExecError::Parse(ParseError::UnrecognizedParameter(value.clone()))
                            })?;
                            gucs.insert(name, value.trim().trim_matches('\'').to_string());
                        } else if matches!(
                            name.as_str(),
                            "parallel_setup_cost" | "parallel_tuple_cost"
                        ) {
                            value
                                .trim()
                                .trim_matches('\'')
                                .parse::<f64>()
                                .map_err(|_| {
                                    ExecError::Parse(ParseError::UnrecognizedParameter(
                                        value.clone(),
                                    ))
                                })?;
                            gucs.insert(name, value.trim().trim_matches('\'').to_string());
                        } else if parse_direct_bool_guc(value).is_none() {
                            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                                value.clone(),
                            )));
                        } else {
                            gucs.insert(name, value.trim().trim_matches('\'').to_ascii_lowercase());
                        }
                    } else {
                        gucs.remove(&name);
                    }
                }
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            Statement::Reset(reset_stmt) => {
                let mut states = self.session_guc_states.write();
                if let Some(name) = reset_stmt.name.as_ref() {
                    let name = normalize_direct_guc_name(name);
                    if let Some(gucs) = states.get_mut(&client_id) {
                        gucs.remove(&name);
                    }
                } else {
                    states.remove(&client_id);
                }
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            Statement::Show(show_stmt) => {
                let name = normalize_direct_guc_name(&show_stmt.name);
                let Some(default) = direct_guc_default(&name) else {
                    return Ok(Some(StatementResult::AffectedRows(0)));
                };
                let gucs = self.direct_gucs_for_client(client_id);
                let value = gucs.get(&name).map(String::as_str).unwrap_or(default);
                Ok(Some(StatementResult::Query {
                    columns: vec![QueryColumn::text(show_stmt.name.clone())],
                    column_names: vec![show_stmt.name.clone()],
                    rows: vec![vec![Value::Text(value.into())]],
                }))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn execute_with_search_path(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(datetime_config.max_stack_depth_kb).run(|| {
                let stmt = self.plan_cache.get_statement_with_options(
                    sql,
                    ParseOptions {
                        max_stack_depth_kb: datetime_config.max_stack_depth_kb,
                        ..ParseOptions::default()
                    },
                )?;
                if let Some(result) = self.apply_direct_guc_statement(client_id, &stmt)? {
                    return Ok(result);
                }
                let gucs = self.direct_gucs_for_client(client_id);
                let planner_config = direct_planner_config(&gucs);
                self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
                    client_id,
                    stmt,
                    configured_search_path,
                    datetime_config,
                    &gucs,
                    planner_config,
                )
            })
        })
    }

    pub(crate) fn execute_statement_with_search_path(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_and_datetime_config(
            client_id,
            stmt,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_and_gucs(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_and_gucs(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            gucs,
            PlannerConfig::default(),
        )
    }

    pub(crate) fn execute_statement_with_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        planner_config: PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
            planner_config,
        )
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            gucs,
            planner_config,
            crate::backend::executor::PgPrngState::shared(),
        )
    }

    pub(crate) fn execute_prepared_statement_with_search_path_datetime_config_gucs_and_planner_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        external_params: &[PreparedExternalParam],
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        let datetime_config = autocommit_datetime_config(datetime_config);
        let statement_lock_scope_id = Some(self.allocate_statement_lock_scope_id());
        let stats_state = self.session_stats_state(client_id);
        stats_state.write().begin_top_level_xact();
        let advisory_locks = std::sync::Arc::clone(&self.advisory_locks);
        let row_locks = std::sync::Arc::clone(&self.row_locks);
        let result = self.execute_statement_with_search_path_inner(
            client_id,
            stmt,
            statement_lock_scope_id,
            configured_search_path,
            &datetime_config,
            gucs,
            planner_config,
            crate::backend::executor::PgPrngState::shared(),
            external_params,
        );
        if let Some(scope_id) = statement_lock_scope_id {
            advisory_locks.unlock_all_statement(client_id, scope_id);
            row_locks.unlock_all_statement(client_id, scope_id);
        }
        match &result {
            Ok(_) => stats_state.write().commit_top_level_xact(&self.stats),
            Err(_) => stats_state.write().rollback_top_level_xact(),
        }
        result
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    ) -> Result<StatementResult, ExecError> {
        let datetime_config = autocommit_datetime_config(datetime_config);
        let statement_lock_scope_id = Some(self.allocate_statement_lock_scope_id());
        let stats_state = self.session_stats_state(client_id);
        stats_state.write().begin_top_level_xact();
        let advisory_locks = std::sync::Arc::clone(&self.advisory_locks);
        let row_locks = std::sync::Arc::clone(&self.row_locks);
        let result = self.execute_statement_with_search_path_inner(
            client_id,
            stmt,
            statement_lock_scope_id,
            configured_search_path,
            &datetime_config,
            gucs,
            planner_config,
            random_state,
            &[],
        );
        if let Some(scope_id) = statement_lock_scope_id {
            advisory_locks.unlock_all_statement(client_id, scope_id);
            row_locks.unlock_all_statement(client_id, scope_id);
        }
        match &result {
            Ok(_) => stats_state.write().commit_top_level_xact(&self.stats),
            Err(_) => stats_state.write().rollback_top_level_xact(),
        }
        result
    }

    pub(crate) fn finish_txn_with_async_notifications(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
        temp_effects: &[TempMutationEffect],
        sequence_effects: &[SequenceMutationEffect],
        pending_async_notifications: Vec<PendingNotification>,
    ) -> Result<StatementResult, ExecError> {
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            catalog_effects,
            temp_effects,
            sequence_effects,
        );
        if result.is_ok() {
            self.async_notify_runtime
                .publish(client_id, &pending_async_notifications);
        }
        result
    }

    fn execute_notify_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::NotifyStatement,
    ) -> Result<StatementResult, ExecError> {
        let mut pending_async_notifications = Vec::new();
        queue_pending_notification(
            &mut pending_async_notifications,
            &stmt.channel,
            stmt.payload.as_deref().unwrap_or(""),
        )?;
        self.async_notify_runtime
            .publish(client_id, &pending_async_notifications);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_listen_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::ListenStatement,
    ) -> StatementResult {
        self.async_notify_runtime.listen(client_id, &stmt.channel);
        StatementResult::AffectedRows(0)
    }

    fn execute_unlisten_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::UnlistenStatement,
    ) -> StatementResult {
        self.async_notify_runtime
            .unlisten(client_id, stmt.channel.as_deref());
        StatementResult::AffectedRows(0)
    }

    fn execute_statement_with_search_path_inner(
        &self,
        client_id: ClientId,
        stmt: Statement,
        statement_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
        external_params: &[PreparedExternalParam],
    ) -> Result<StatementResult, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::commands::tablecmds::{
            check_planned_stmt_select_for_update_privileges, check_planned_stmt_select_privileges,
            resolve_truncate_relations,
        };
        let interrupts = self.interrupt_state(client_id);
        let session_replication_role = self.session_replication_role(client_id);

        match stmt {
            Statement::AlterTableMulti(ref statements) => {
                let parsed_statements = statements
                    .iter()
                    .map(|sql| crate::backend::parser::parse_statement(sql))
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(result) = self.try_execute_alter_table_batch_stmt_with_search_path(
                    client_id,
                    &parsed_statements,
                    configured_search_path,
                    datetime_config,
                )? {
                    return Ok(result);
                }
                for substmt in parsed_statements {
                    self.execute_statement_with_search_path_inner(
                        client_id,
                        substmt,
                        statement_lock_scope_id,
                        configured_search_path,
                        datetime_config,
                        gucs,
                        planner_config,
                        std::sync::Arc::clone(&random_state),
                        &[],
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Do(ref do_stmt) => execute_do_with_gucs(do_stmt, gucs),
            Statement::SetConstraints(_) => {
                crate::backend::utils::misc::notices::push_warning(
                    "SET CONSTRAINTS can only be used in transaction blocks",
                );
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Checkpoint(_) => {
                let auth = self.auth_state(client_id);
                let auth_catalog = self.auth_catalog(client_id, None).map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "authorization catalog",
                        actual: format!("{err:?}"),
                    })
                })?;
                if !auth.has_effective_membership(
                    crate::include::catalog::PG_CHECKPOINT_OID,
                    &auth_catalog,
                ) {
                    return Err(ExecError::DetailedError {
                        message: "permission denied to execute CHECKPOINT command".into(),
                        detail: Some(
                            "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
                                .into(),
                        ),
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                self.request_checkpoint(
                    crate::backend::access::transam::CheckpointRequestFlags::sql(),
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::LockTable(_) => Err(ExecError::DetailedError {
                message: "LOCK TABLE can only be used in transaction blocks".into(),
                detail: None,
                hint: None,
                sqlstate: "25P01",
            }),
            Statement::Notify(ref notify_stmt) => self.execute_notify_stmt(client_id, notify_stmt),
            Statement::Listen(ref listen_stmt) => {
                Ok(self.execute_listen_stmt(client_id, listen_stmt))
            }
            Statement::Unlisten(ref unlisten_stmt) => {
                Ok(self.execute_unlisten_stmt(client_id, unlisten_stmt))
            }
            Statement::Load(_) | Statement::Discard(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Analyze(ref analyze_stmt) => self.execute_analyze_stmt_with_search_path(
                client_id,
                analyze_stmt,
                configured_search_path,
            ),
            Statement::CreateIndex(ref create_stmt) => self
                .execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    Some(gucs),
                    65_536,
                ),
            Statement::ReindexIndex(ref reindex_stmt) => self
                .execute_reindex_index_stmt_with_search_path(
                    client_id,
                    reindex_stmt,
                    configured_search_path,
                ),
            Statement::CreateStatistics(ref create_stmt) => self
                .execute_create_statistics_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterStatistics(ref alter_stmt) => self
                .execute_alter_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearchDictionary(ref create_stmt) => self
                .execute_create_text_search_dictionary_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearchDictionary(ref alter_stmt) => self
                .execute_alter_text_search_dictionary_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearchConfiguration(ref create_stmt) => self
                .execute_create_text_search_configuration_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearchConfiguration(ref alter_stmt) => self
                .execute_alter_text_search_configuration_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::DropTextSearchConfiguration(ref drop_stmt) => self
                .execute_drop_text_search_configuration_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropTextSearch(ref drop_stmt) => self
                .execute_drop_text_search_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropStatistics(ref drop_stmt) => self
                .execute_drop_statistics_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableOwner(ref alter_stmt) => self
                .execute_alter_table_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterLargeObjectOwner(ref alter_stmt) => self
                .execute_alter_large_object_owner_stmt(
                    client_id,
                    alter_stmt.oid,
                    &alter_stmt.new_owner,
                ),
            Statement::AlterTableRename(ref rename_stmt) => self
                .execute_alter_table_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetSchema(ref alter_stmt) => self
                .execute_alter_table_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetTablespace(ref alter_stmt) => self
                .execute_alter_table_set_tablespace_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexSetTablespace(ref alter_stmt) => self
                .execute_alter_table_set_tablespace_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterMoveAllTablespace(ref alter_stmt) => {
                self.execute_alter_move_all_tablespace_stmt(client_id, alter_stmt)
            }
            Statement::AlterTableReset(ref alter_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let is_view = catalog
                    .lookup_any_relation(&alter_stmt.table_name)
                    .is_some_and(|relation| relation.relkind == 'v');
                drop(catalog);
                if is_view {
                    self.execute_alter_view_reset_options_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        configured_search_path,
                    )
                } else {
                    self.execute_alter_table_reset_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        configured_search_path,
                    )
                }
            }
            Statement::AlterTableSetPersistence(ref alter_stmt) => self
                .execute_alter_table_set_persistence_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetWithoutCluster(ref alter_stmt) => self
                .execute_alter_table_set_without_cluster_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexRename(ref rename_stmt) => self
                .execute_alter_index_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAttachPartition(ref attach_stmt) => self
                .execute_alter_index_attach_partition_stmt_with_search_path(
                    client_id,
                    attach_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewRename(ref rename_stmt) => self
                .execute_alter_view_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewRenameColumn(ref rename_stmt) => self
                .execute_alter_view_rename_column_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewSetSchema(ref alter_stmt) => self
                .execute_alter_view_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterMaterializedViewSetSchema(ref alter_stmt) => self
                .execute_alter_materialized_view_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterMaterializedViewSetAccessMethod(ref alter_stmt) => self
                .execute_alter_materialized_view_set_access_method_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_index_alter_column_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAlterColumnOptions(ref alter_stmt) => self
                .execute_alter_index_alter_column_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableCompound(ref compound_stmt) => {
                if let Some(result) = self.try_execute_alter_table_batch_stmt_with_search_path(
                    client_id,
                    &compound_stmt.actions,
                    configured_search_path,
                    datetime_config,
                )? {
                    return Ok(result);
                }
                for action in &compound_stmt.actions {
                    self.execute_statement_with_search_path_inner(
                        client_id,
                        action.clone(),
                        statement_lock_scope_id,
                        configured_search_path,
                        datetime_config,
                        gucs,
                        planner_config,
                        std::sync::Arc::clone(&random_state),
                        &[],
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::AlterViewOwner(ref alter_stmt) => self
                .execute_alter_view_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameColumn(ref rename_stmt) => self
                .execute_alter_table_rename_column_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddColumn(ref alter_stmt) => self
                .execute_alter_table_add_column_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddColumns(ref alter_stmt) => {
                let actions = [Statement::AlterTableAddColumns(alter_stmt.clone())];
                if let Some(result) = self.try_execute_alter_table_batch_stmt_with_search_path(
                    client_id,
                    &actions,
                    configured_search_path,
                    datetime_config,
                )? {
                    return Ok(result);
                }
                self.execute_alter_table_add_columns_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                )
            }
            Statement::AlterTableDropColumn(ref drop_stmt) => self
                .execute_alter_table_drop_column_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnType(ref alter_stmt) => self
                .execute_alter_table_alter_column_type_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                    datetime_config,
                ),
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => self
                .execute_alter_table_alter_column_default_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnExpression(ref alter_stmt) => {
                let actions = [Statement::AlterTableAlterColumnExpression(
                    alter_stmt.clone(),
                )];
                if let Some(result) = self.try_execute_alter_table_batch_stmt_with_search_path(
                    client_id,
                    &actions,
                    configured_search_path,
                    datetime_config,
                )? {
                    return Ok(result);
                }
                self.execute_alter_table_alter_column_expression_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                )
            }
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => self
                .execute_alter_table_alter_column_compression_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => self
                .execute_alter_table_alter_column_storage_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => self
                .execute_alter_table_alter_column_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_table_alter_column_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnIdentity(ref alter_stmt) => self
                .execute_alter_table_alter_column_identity_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddConstraint(ref alter_stmt) => self
                .execute_alter_table_add_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                    None,
                    None,
                ),
            Statement::AlterTableDropConstraint(ref alter_stmt) => self
                .execute_alter_table_drop_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterConstraint(ref alter_stmt) => self
                .execute_alter_table_alter_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameConstraint(ref alter_stmt) => self
                .execute_alter_table_rename_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetNotNull(ref alter_stmt) => self
                .execute_alter_table_set_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDropNotNull(ref alter_stmt) => self
                .execute_alter_table_drop_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableValidateConstraint(ref alter_stmt) => self
                .execute_alter_table_validate_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableInherit(ref alter_stmt) => self
                .execute_alter_table_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableNoInherit(ref alter_stmt) => self
                .execute_alter_table_no_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableOf(ref alter_stmt) => self
                .execute_alter_table_of_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableNotOf(ref alter_stmt) => self
                .execute_alter_table_not_of_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAttachPartition(ref alter_stmt) => self
                .execute_alter_table_attach_partition_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDetachPartition(ref alter_stmt) => self
                .execute_alter_table_detach_partition_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => self
                .execute_alter_table_set_row_security_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableReplicaIdentity(ref alter_stmt) => self
                .execute_alter_table_replica_identity_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPolicy(ref alter_stmt) => self
                .execute_alter_policy_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSet(ref alter_stmt) => self
                .execute_alter_table_set_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexSet(ref alter_stmt) => self
                .execute_alter_index_set_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Show(_)
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::Prepare(_)
            | Statement::Execute(_)
            | Statement::Deallocate(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CreateRole(ref create_stmt) => self.execute_create_role_stmt(
                client_id,
                create_stmt,
                None,
                PasswordSettings::default(),
            ),
            Statement::CreateDatabase(ref create_stmt) => {
                self.execute_create_database_stmt(client_id, create_stmt)
            }
            Statement::AlterDatabase(ref alter_stmt) => {
                self.execute_alter_database_stmt(client_id, alter_stmt)
            }
            Statement::AlterRole(ref alter_stmt) => {
                self.execute_alter_role_stmt(client_id, alter_stmt, PasswordSettings::default())
            }
            Statement::DropRole(ref drop_stmt) => self.execute_drop_role_stmt(client_id, drop_stmt),
            Statement::DropDatabase(ref drop_stmt) => {
                self.execute_drop_database_stmt(client_id, drop_stmt)
            }
            Statement::DropExtension(ref drop_stmt) => {
                self.execute_drop_extension_stmt(client_id, drop_stmt)
            }
            Statement::DropAccessMethod(ref drop_stmt) => {
                self.execute_drop_access_method_stmt(client_id, drop_stmt)
            }
            Statement::GrantObject(ref grant_stmt) => self
                .execute_grant_object_stmt_with_search_path(
                    client_id,
                    grant_stmt,
                    configured_search_path,
                ),
            Statement::AlterDefaultPrivileges(ref stmt) => self
                .execute_alter_default_privileges_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            Statement::RevokeObject(ref revoke_stmt) => self
                .execute_revoke_object_stmt_with_search_path(
                    client_id,
                    revoke_stmt,
                    configured_search_path,
                ),
            Statement::GrantRoleMembership(ref grant_stmt) => {
                self.execute_grant_role_membership_stmt(client_id, grant_stmt)
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                self.execute_revoke_role_membership_stmt(client_id, revoke_stmt)
            }
            Statement::DropOwned(ref drop_stmt) => {
                self.execute_drop_owned_stmt(client_id, drop_stmt)
            }
            Statement::ReassignOwned(ref reassign_stmt) => {
                self.execute_reassign_owned_stmt(client_id, reassign_stmt)
            }
            Statement::CommentOnDatabase(ref comment_stmt) => {
                self.execute_comment_on_database_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                self.execute_comment_on_role_stmt(client_id, comment_stmt)
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                self.execute_set_session_authorization_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                self.execute_reset_session_authorization_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::SetRole(ref set_stmt) => {
                if set_stmt.is_local {
                    return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                        "SET LOCAL ROLE",
                    )));
                }
                self.execute_set_role_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetRole(ref reset_stmt) => {
                self.execute_reset_role_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Unsupported(ref unsupported_stmt) => {
                if let Some(result) = self.execute_object_address_unsupported_stmt(
                    client_id,
                    unsupported_stmt,
                    configured_search_path,
                )? {
                    return Ok(result);
                }
                if unsupported_stmt.feature == "SECURITY LABEL" {
                    return Err(ExecError::Parse(
                        crate::backend::parser::security_label_provider_error(
                            &unsupported_stmt.sql,
                        ),
                    ));
                }
                if unsupported_stmt.feature == "ALTER TABLE form" {
                    let lower = unsupported_stmt.sql.to_ascii_lowercase();
                    if lower.contains(" set without oids") {
                        return Ok(StatementResult::AffectedRows(0));
                    }
                    if lower.contains(" set with oids") {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "valid ALTER TABLE form",
                            actual: "syntax error at or near \"WITH\"".into(),
                        }));
                    }
                }
                Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                    "{}: {}",
                    unsupported_stmt.feature, unsupported_stmt.sql
                ))))
            }
            Statement::Call(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "CALL execution".into(),
            ))),
            Statement::CopyFrom(_) | Statement::CopyTo(_) => {
                Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "COPY handled by session layer",
                    actual: "COPY".into(),
                }))
            }
            Statement::CreateFunction(ref create_stmt) => self
                .execute_create_function_stmt_with_search_path_and_gucs(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    Some(gucs),
                ),
            Statement::CreateProcedure(ref create_stmt) => self
                .execute_create_procedure_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateAggregate(ref create_stmt) => self
                .execute_create_aggregate_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterAggregateRename(ref rename_stmt) => self
                .execute_alter_aggregate_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::CreateCast(ref create_stmt) => self
                .execute_create_cast_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperator(ref create_stmt) => self
                .execute_create_operator_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperatorClass(ref create_stmt) => self
                .execute_create_operator_class_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperatorFamily(ref create_stmt) => self
                .execute_create_operator_family_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperatorFamily(ref alter_stmt) => self
                .execute_alter_operator_family_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperatorClass(ref alter_stmt) => self
                .execute_alter_operator_class_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::DropOperatorFamily(ref drop_stmt) => self
                .execute_drop_operator_family_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropOperatorClass(ref drop_stmt) => self
                .execute_drop_operator_class_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearch(ref create_stmt) => self
                .execute_create_text_search_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearch(ref alter_stmt) => self
                .execute_alter_text_search_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateSchema(ref create_stmt) => self
                .execute_create_schema_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTablespace(ref create_stmt) => {
                self.execute_create_tablespace_stmt(client_id, create_stmt, false)
            }
            Statement::DropTablespace(ref drop_stmt) => {
                self.execute_drop_tablespace_stmt(client_id, drop_stmt)
            }
            Statement::AlterTablespace(ref alter_stmt) => {
                self.execute_alter_tablespace_stmt(client_id, alter_stmt)
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => self
                .execute_alter_schema_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSchemaRename(ref alter_stmt) => self
                .execute_alter_schema_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPublication(ref alter_stmt) => self
                .execute_alter_publication_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSubscription(ref alter_stmt) => self
                .execute_alter_subscription_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperator(ref alter_stmt) => self
                .execute_alter_operator_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterConversion(ref alter_stmt) => self
                .execute_alter_conversion_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterProcedure(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "ALTER PROCEDURE".into(),
            ))),
            Statement::AlterRoutine(ref alter_stmt) => self
                .execute_alter_routine_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateSequence(ref create_stmt) => self
                .execute_create_sequence_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::Merge(ref merge_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                };
                let mut locked_rels = Vec::new();
                let result = (|| {
                    let has_writable_ctes = merge_stmt
                        .with
                        .iter()
                        .any(|cte| cte_body_has_writable(&cte.body));
                    if !has_writable_ctes {
                        let bound = crate::backend::parser::plan_merge(merge_stmt, &catalog)?;
                        return crate::backend::commands::tablecmds::execute_merge(
                            bound, &catalog, &mut ctx, xid, 0,
                        );
                    }

                    let mut outer_merge = merge_stmt.clone();
                    outer_merge.with.clear();
                    let refs_merge = outer_merge.clone();
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &merge_stmt.with,
                            merge_stmt.with_recursive,
                            &catalog,
                            |name| merge_statement_references_table(&refs_merge, name),
                        )?;
                    outer_merge.with = remaining_ctes;
                    let bound =
                        plan_merge_with_outer_ctes(&outer_merge, &catalog, &materialized_ctes)?;
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    crate::backend::commands::tablecmds::execute_merge(
                        bound, &catalog, &mut ctx, xid, 0,
                    )
                })();
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnSequence(ref comment_stmt) => self
                .execute_comment_on_sequence_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnColumn(ref comment_stmt) => self
                .execute_comment_on_column_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnView(ref comment_stmt) => self
                .execute_comment_on_view_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnIndex(ref comment_stmt) => self
                .execute_comment_on_index_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnType(ref comment_stmt) => self
                .execute_comment_on_type_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnAggregate(ref comment_stmt) => self
                .execute_comment_on_aggregate_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnFunction(ref comment_stmt) => self
                .execute_comment_on_function_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnOperator(ref comment_stmt) => self
                .execute_comment_on_operator_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnLargeObject(ref comment_stmt) => self
                .execute_comment_on_large_object_stmt(
                    client_id,
                    comment_stmt.oid,
                    comment_stmt.comment.as_deref(),
                ),
            Statement::CommentOnConstraint(ref comment_stmt) => self
                .execute_comment_on_constraint_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnRule(ref comment_stmt) => self
                .execute_comment_on_rule_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnTrigger(ref comment_stmt) => self
                .execute_comment_on_trigger_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnEventTrigger(ref comment_stmt) => self
                .execute_comment_on_event_trigger_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnDomain(ref comment_stmt) => self
                .execute_comment_on_domain_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnConversion(ref comment_stmt) => self
                .execute_comment_on_conversion_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnForeignDataWrapper(ref comment_stmt) => {
                self.execute_comment_on_foreign_data_wrapper_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnForeignServer(ref comment_stmt) => {
                self.execute_comment_on_foreign_server_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnPublication(ref comment_stmt) => self
                .execute_comment_on_publication_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnSubscription(ref comment_stmt) => self
                .execute_comment_on_subscription_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnStatistics(ref comment_stmt) => self
                .execute_comment_on_statistics_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CreateForeignDataWrapper(ref create_stmt) => self
                .execute_create_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateForeignServer(ref create_stmt) => {
                self.execute_create_foreign_server_stmt(client_id, create_stmt)
            }
            Statement::CreateLanguage(ref create_stmt) => {
                self.execute_create_language_stmt(client_id, create_stmt)
            }
            Statement::AlterLanguage(ref alter_stmt) => {
                self.execute_alter_language_stmt(client_id, alter_stmt)
            }
            Statement::DropLanguage(ref drop_stmt) => {
                self.execute_drop_language_stmt(client_id, drop_stmt)
            }
            Statement::CreateUserMapping(ref create_stmt) => {
                self.execute_create_user_mapping_stmt(client_id, create_stmt)
            }
            Statement::CreateForeignTable(ref create_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let result = self
                    .execute_create_foreign_table_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        0,
                        configured_search_path,
                        &mut catalog_effects,
                    );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::ImportForeignSchema(ref import_stmt) => {
                self.execute_import_foreign_schema_stmt(client_id, import_stmt)
            }
            Statement::AlterForeignDataWrapper(ref alter_stmt) => self
                .execute_alter_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterForeignDataWrapperOwner(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignDataWrapperRename(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_rename_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServer(ref alter_stmt) => {
                self.execute_alter_foreign_server_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServerOwner(ref alter_stmt) => {
                self.execute_alter_foreign_server_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServerRename(ref alter_stmt) => {
                self.execute_alter_foreign_server_rename_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignTableOptions(ref alter_stmt) => self
                .execute_alter_foreign_table_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterUserMapping(ref alter_stmt) => {
                self.execute_alter_user_mapping_stmt(client_id, alter_stmt)
            }
            Statement::DropForeignDataWrapper(ref drop_stmt) => {
                self.execute_drop_foreign_data_wrapper_stmt(client_id, drop_stmt)
            }
            Statement::DropForeignServer(ref drop_stmt) => {
                self.execute_drop_foreign_server_stmt(client_id, drop_stmt)
            }
            Statement::DropUserMapping(ref drop_stmt) => {
                self.execute_drop_user_mapping_stmt(client_id, drop_stmt)
            }
            Statement::Select(ref select_stmt) if select_has_writable_ctes(select_stmt) => {
                if select_has_non_top_level_writable_ctes(select_stmt) {
                    return Err(nested_modifying_cte_error());
                }
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let external_bindings = bind_prepared_external_params(external_params, &catalog)?;
                let external_types = prepared_external_types(&external_bindings);
                if restrict_nonsystem_view_enabled(gucs) {
                    reject_restricted_views_in_select(select_stmt, &catalog)?;
                    reject_restricted_bound_view_refs_in_select(select_stmt, &catalog)?;
                }

                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                install_prepared_external_params(&external_bindings, &mut ctx)?;
                let mut locked_rels = Vec::new();
                let result = with_external_param_types(&external_types, || {
                    let mut outer_select = select_stmt.clone();
                    outer_select.with.clear();
                    let refs_select = outer_select.clone();
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &select_stmt.with,
                            select_stmt.with_recursive,
                            &catalog,
                            |name| select_statement_references_table(&refs_select, name),
                        )?;
                    outer_select.with = remaining_ctes;
                    let planned = pg_plan_query_with_outer_scopes_and_ctes(
                        &outer_select,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    if select_stmt.locking_clause.is_some() {
                        check_planned_stmt_select_for_update_privileges(&planned, &ctx)?;
                    } else {
                        check_planned_stmt_select_privileges(&planned, &ctx)?;
                    }
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    let result = execute_planned_stmt(planned, &mut ctx);
                    ctx.pinned_cte_tables.clear();
                    result
                });
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Values(ref values_stmt)
                if values_stmt
                    .with
                    .iter()
                    .any(|cte| cte_body_has_writable(&cte.body)) =>
            {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let mut locked_rels = Vec::new();
                let result = (|| {
                    let mut outer_values = values_stmt.clone();
                    outer_values.with.clear();
                    let refs_body = CteBody::Values(outer_values.clone());
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &values_stmt.with,
                            values_stmt.with_recursive,
                            &catalog,
                            |name| cte_body_references_table(&refs_body, name),
                        )?;
                    outer_values.with = remaining_ctes;
                    let planned = pg_plan_values_query_with_outer_scopes_and_ctes(
                        &outer_values,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    let result = execute_planned_stmt(planned, &mut ctx);
                    ctx.pinned_cte_tables.clear();
                    result
                })();
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let visible_catalog =
                    self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let external_bindings =
                    bind_prepared_external_params(external_params, &visible_catalog)?;
                let external_types = prepared_external_types(&external_bindings);
                if restrict_nonsystem_view_enabled(gucs) {
                    match &stmt {
                        Statement::Select(select) => {
                            reject_restricted_views_in_select(select, &visible_catalog)?;
                            reject_restricted_bound_view_refs_in_select(select, &visible_catalog)?;
                        }
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                reject_restricted_views_in_select(select, &visible_catalog)?;
                                reject_restricted_bound_view_refs_in_select(
                                    select,
                                    &visible_catalog,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }
                let (stmt, planned_select, planned_select_for_update, rels) =
                    with_external_param_types(&external_types, || {
                        let mut rels = std::collections::BTreeSet::new();
                        let mut planned_select = None;
                        let mut planned_select_for_update = false;
                        match &stmt {
                            Statement::Select(select) if !select_has_writable_ctes(select) => {
                                let planned_stmt =
                                    crate::backend::rewrite::with_restrict_nonsystem_view_expansion(
                                        restrict_nonsystem_view_enabled(gucs),
                                        || crate::backend::parser::pg_plan_query_with_config(
                                        select,
                                        &visible_catalog,
                                        planner_config,
                                        ),
                                    )?;
                                collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                                planned_select_for_update = select.locking_clause.is_some();
                                planned_select = Some(planned_stmt);
                            }
                            Statement::Select(_) => {}
                            Statement::Values(_) => {}
                            Statement::Explain(explain) => {
                                if let Statement::Select(select) = explain.statement.as_ref()
                                    && !select_has_writable_ctes(select)
                                {
                                    let planned_stmt =
                                        crate::backend::rewrite::with_restrict_nonsystem_view_expansion(
                                            restrict_nonsystem_view_enabled(gucs),
                                            || crate::backend::parser::pg_plan_query_with_config(
                                            select,
                                            &visible_catalog,
                                            planner_config,
                                            ),
                                        )?;
                                    collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                                }
                            }
                            _ => unreachable!(),
                        }
                        Ok::<_, ExecError>((
                            stmt,
                            planned_select,
                            planned_select_for_update,
                            rels.into_iter().collect::<Vec<_>>(),
                        ))
                    })?;

                lock_relations_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let transaction_state: SharedExecutorTransactionState =
                    Arc::new(parking_lot::Mutex::new(ExecutorTransactionState {
                        xid: None,
                        cid: 0,
                        transaction_snapshot: None,
                        serializable_xact: None,
                    }));
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: Some(Arc::clone(&transaction_state)),
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(
                        visible_catalog.clone(),
                    )),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                install_prepared_external_params(&external_bindings, &mut ctx)?;
                let result = with_external_param_types(&external_types, || match planned_select {
                    Some(planned_stmt) => {
                        if restrict_nonsystem_view_enabled(gucs) {
                            reject_restricted_views_in_planned_stmt(
                                &planned_stmt,
                                &visible_catalog,
                            )?;
                        }
                        if planned_select_for_update {
                            check_planned_stmt_select_for_update_privileges(&planned_stmt, &ctx)?;
                        } else {
                            check_planned_stmt_select_privileges(&planned_stmt, &ctx)?;
                        }
                        execute_planned_stmt(planned_stmt, &mut ctx)
                    }
                    None => execute_readonly_statement_with_config(
                        stmt,
                        &visible_catalog,
                        &mut ctx,
                        planner_config,
                    ),
                });
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                let mut catalog_effects = std::mem::take(&mut ctx.catalog_effects);
                let temp_effects = std::mem::take(&mut ctx.temp_effects);
                let pending_catalog_effects = std::mem::take(&mut ctx.pending_catalog_effects);
                let pending_table_locks = std::mem::take(&mut ctx.pending_table_locks);
                catalog_effects.extend(pending_catalog_effects);
                drop(ctx);
                let xid = transaction_state.lock().xid;
                let result = if let Some(xid) = xid {
                    let validation_catalog =
                        self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                    let result = result.and_then(|result| {
                        crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                            self,
                            client_id,
                            &validation_catalog,
                            xid,
                            1,
                            Arc::clone(&interrupts),
                            datetime_config,
                            &deferred_foreign_keys,
                        )?;
                        Ok(result)
                    });
                    self.finish_txn_with_async_notifications(
                        client_id,
                        xid,
                        result,
                        &catalog_effects,
                        &temp_effects,
                        &[],
                        pending_async_notifications,
                    )
                } else {
                    if result.is_ok() {
                        self.async_notify_runtime
                            .publish(client_id, &pending_async_notifications);
                    }
                    result
                };

                unlock_relations(&self.table_locks, client_id, &pending_table_locks);
                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let external_bindings = bind_prepared_external_params(external_params, &catalog)?;
                let external_types = prepared_external_types(&external_bindings);
                if restrict_nonsystem_view_enabled(gucs) {
                    reject_restricted_views_in_insert(insert_stmt, &catalog)?;
                }
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                install_prepared_external_params(&external_bindings, &mut ctx)?;
                let mut locked_rels = Vec::new();
                let result = with_external_param_types(&external_types, || {
                    let has_writable_ctes = insert_stmt
                        .with
                        .iter()
                        .any(|cte| cte_body_has_writable(&cte.body));
                    if !has_writable_ctes {
                        let bound = bind_insert(insert_stmt, &catalog)?;
                        let prepared =
                            super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                        let lock_requests = merge_table_lock_requests(
                            &insert_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        let waited_for_lock =
                            crate::backend::storage::lmgr::lock_table_requests_interruptible(
                                &self.table_locks,
                                client_id,
                                &lock_requests,
                                interrupts.as_ref(),
                            )?;
                        locked_rels.extend(table_lock_relations(&lock_requests));
                        refresh_autocommit_snapshot_after_lock_wait(
                            self,
                            &mut ctx,
                            xid,
                            0,
                            waited_for_lock,
                        )?;
                        return super::rules::execute_bound_insert_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            0,
                        );
                    }

                    let mut outer_insert = insert_stmt.clone();
                    outer_insert.with.clear();
                    let refs_insert = outer_insert.clone();
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &insert_stmt.with,
                            insert_stmt.with_recursive,
                            &catalog,
                            |name| insert_statement_references_table(&refs_insert, name),
                        )?;
                    outer_insert.with = remaining_ctes;

                    let bound = bind_insert_with_outer_scopes_and_ctes(
                        &outer_insert,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    let prepared =
                        super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                    super::rules::enforce_modifying_cte_rule_restrictions(
                        prepared.stmt.relation_oid,
                        RuleEvent::Insert,
                        &catalog,
                    )?;
                    let lock_requests = merge_table_lock_requests(
                        &insert_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    let waited_for_lock =
                        crate::backend::storage::lmgr::lock_table_requests_interruptible(
                            &self.table_locks,
                            client_id,
                            &lock_requests,
                            interrupts.as_ref(),
                        )?;
                    locked_rels.extend(table_lock_relations(&lock_requests));
                    refresh_autocommit_snapshot_after_lock_wait(
                        self,
                        &mut ctx,
                        xid,
                        0,
                        waited_for_lock,
                    )?;
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    super::rules::execute_bound_insert_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )
                });
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let external_bindings = bind_prepared_external_params(external_params, &catalog)?;
                let external_types = prepared_external_types(&external_bindings);
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                install_prepared_external_params(&external_bindings, &mut ctx)?;
                let mut locked_rels = Vec::new();
                let result = with_external_param_types(&external_types, || {
                    let has_writable_ctes = update_stmt
                        .with
                        .iter()
                        .any(|cte| cte_body_has_writable(&cte.body));
                    if !has_writable_ctes {
                        let bound = bind_update(update_stmt, &catalog)?;
                        let prepared =
                            super::rules::prepare_bound_update_for_execution(bound, &catalog)?;
                        let lock_requests = merge_table_lock_requests(
                            &update_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        let waited_for_lock =
                            crate::backend::storage::lmgr::lock_table_requests_interruptible(
                                &self.table_locks,
                                client_id,
                                &lock_requests,
                                interrupts.as_ref(),
                            )?;
                        locked_rels.extend(table_lock_relations(&lock_requests));
                        refresh_autocommit_snapshot_after_lock_wait(
                            self,
                            &mut ctx,
                            xid,
                            0,
                            waited_for_lock,
                        )?;
                        return super::rules::execute_bound_update_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            0,
                            Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                        );
                    }

                    let mut outer_update = update_stmt.clone();
                    outer_update.with.clear();
                    let refs_update = outer_update.clone();
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &update_stmt.with,
                            update_stmt.with_recursive,
                            &catalog,
                            |name| update_statement_references_table(&refs_update, name),
                        )?;
                    outer_update.with = remaining_ctes;
                    let bound = bind_update_with_outer_scopes_and_ctes(
                        &outer_update,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    let prepared =
                        super::rules::prepare_bound_update_for_execution(bound, &catalog)?;
                    for target in &prepared.stmt.targets {
                        super::rules::enforce_modifying_cte_rule_restrictions(
                            target.relation_oid,
                            RuleEvent::Update,
                            &catalog,
                        )?;
                    }
                    let lock_requests = merge_table_lock_requests(
                        &update_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    let waited_for_lock =
                        crate::backend::storage::lmgr::lock_table_requests_interruptible(
                            &self.table_locks,
                            client_id,
                            &lock_requests,
                            interrupts.as_ref(),
                        )?;
                    locked_rels.extend(table_lock_relations(&lock_requests));
                    refresh_autocommit_snapshot_after_lock_wait(
                        self,
                        &mut ctx,
                        xid,
                        0,
                        waited_for_lock,
                    )?;
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    super::rules::execute_bound_update_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                        Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                    )
                });
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let mut locked_rels = Vec::new();
                let result = (|| {
                    let has_writable_ctes = delete_stmt
                        .with
                        .iter()
                        .any(|cte| cte_body_has_writable(&cte.body));
                    if !has_writable_ctes {
                        let bound = bind_delete(delete_stmt, &catalog)?;
                        let prepared =
                            super::rules::prepare_bound_delete_for_execution(bound, &catalog)?;
                        let lock_requests = merge_table_lock_requests(
                            &delete_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        let waited_for_lock =
                            crate::backend::storage::lmgr::lock_table_requests_interruptible(
                                &self.table_locks,
                                client_id,
                                &lock_requests,
                                interrupts.as_ref(),
                            )?;
                        locked_rels.extend(table_lock_relations(&lock_requests));
                        refresh_autocommit_snapshot_after_lock_wait(
                            self,
                            &mut ctx,
                            xid,
                            0,
                            waited_for_lock,
                        )?;
                        return super::rules::execute_bound_delete_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                        );
                    }

                    let mut outer_delete = delete_stmt.clone();
                    outer_delete.with.clear();
                    let refs_delete = outer_delete.clone();
                    let (bound_writable_ctes, materialized_ctes, remaining_ctes) = self
                        .bind_modifying_ctes_autocommit(
                            &delete_stmt.with,
                            delete_stmt.with_recursive,
                            &catalog,
                            |name| delete_statement_references_table(&refs_delete, name),
                        )?;
                    outer_delete.with = remaining_ctes;
                    let bound = bind_delete_with_outer_scopes_and_ctes(
                        &outer_delete,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    let prepared =
                        super::rules::prepare_bound_delete_for_execution(bound, &catalog)?;
                    for target in &prepared.stmt.targets {
                        super::rules::enforce_modifying_cte_rule_restrictions(
                            target.relation_oid,
                            RuleEvent::Delete,
                            &catalog,
                        )?;
                    }
                    let lock_requests = merge_table_lock_requests(
                        &delete_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    let waited_for_lock =
                        crate::backend::storage::lmgr::lock_table_requests_interruptible(
                            &self.table_locks,
                            client_id,
                            &lock_requests,
                            interrupts.as_ref(),
                        )?;
                    locked_rels.extend(table_lock_relations(&lock_requests));
                    refresh_autocommit_snapshot_after_lock_wait(
                        self,
                        &mut ctx,
                        xid,
                        0,
                        waited_for_lock,
                    )?;
                    self.execute_bound_modifying_ctes_autocommit(
                        client_id,
                        &interrupts,
                        &mut locked_rels,
                        &bound_writable_ctes,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )?;
                    super::rules::execute_bound_delete_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                    )
                })();
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::CreateTable(ref create_stmt) => self
                .execute_create_table_stmt_with_search_path_and_gucs(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    Some(gucs),
                ),
            Statement::CreateDomain(ref create_stmt) => self
                .execute_create_domain_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterDomain(ref alter_stmt) => self
                .execute_alter_domain_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateConversion(ref create_stmt) => self
                .execute_create_conversion_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateCollation(ref create_stmt) => self
                .execute_create_collation_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreatePublication(ref create_stmt) => self
                .execute_create_publication_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateSubscription(ref create_stmt) => self
                .execute_create_subscription_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTrigger(ref create_stmt) => self
                .execute_create_trigger_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateEventTrigger(ref create_stmt) => self
                .execute_create_event_trigger_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableTriggerState(ref alter_stmt) => self
                .execute_alter_table_trigger_state_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTrigger(ref alter_stmt) => self
                .execute_alter_event_trigger_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTriggerOwner(ref alter_stmt) => self
                .execute_alter_event_trigger_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTriggerRename(ref alter_stmt) => self
                .execute_alter_trigger_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTriggerRename(ref alter_stmt) => self
                .execute_alter_event_trigger_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreatePolicy(ref create_stmt) => self
                .execute_create_policy_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateType(ref create_stmt) => self
                .execute_create_type_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::DropCast(ref drop_stmt) => self.execute_drop_cast_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::AlterType(ref alter_stmt) => self.execute_alter_type_stmt_with_search_path(
                client_id,
                alter_stmt,
                configured_search_path,
            ),
            Statement::AlterTypeOwner(ref alter_stmt) => self
                .execute_alter_type_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateView(ref create_stmt) => self
                .execute_create_view_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateRule(ref create_stmt) => self
                .execute_create_rule_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterRuleRename(ref alter_stmt) => self
                .execute_alter_rule_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRuleState(ref alter_stmt) => self
                .execute_alter_table_rule_state_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateTableAs(ref create_stmt) => self
                .execute_create_table_as_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    None,
                    0,
                    configured_search_path,
                    planner_config,
                    Some(gucs),
                ),
            Statement::RefreshMaterializedView(ref refresh_stmt) => self
                .execute_refresh_materialized_view_stmt_with_search_path(
                    client_id,
                    refresh_stmt,
                    None,
                    0,
                    configured_search_path,
                ),
            Statement::Cluster(ref cluster_stmt) => self.execute_cluster_stmt_with_search_path(
                client_id,
                cluster_stmt,
                configured_search_path,
            ),
            Statement::DropTable(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropIndex(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_index_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropDomain(ref drop_stmt) => self.execute_drop_domain_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropFunction(ref drop_stmt) => self
                .execute_drop_function_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropProcedure(ref drop_stmt) => self
                .execute_drop_procedure_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropRoutine(ref drop_stmt) => self
                .execute_drop_routine_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropAggregate(ref drop_stmt) => self
                .execute_drop_aggregate_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropOperator(ref drop_stmt) => self
                .execute_drop_operator_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropConversion(ref drop_stmt) => self
                .execute_drop_conversion_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropCollation(ref drop_stmt) => self
                .execute_drop_collation_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPublication(ref drop_stmt) => self
                .execute_drop_publication_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropSubscription(ref drop_stmt) => self
                .execute_drop_subscription_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropTrigger(ref drop_stmt) => self
                .execute_drop_trigger_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropEventTrigger(ref drop_stmt) => self
                .execute_drop_event_trigger_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPolicy(ref drop_stmt) => self.execute_drop_policy_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropType(ref drop_stmt) => self.execute_drop_type_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropSequence(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let mut sequence_effects = Vec::new();
                let result = self.execute_drop_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                    &mut sequence_effects,
                );
                let result = self.finish_txn(
                    client_id,
                    xid,
                    result,
                    &catalog_effects,
                    &temp_effects,
                    &sequence_effects,
                );
                guard.disarm();
                result
            }
            Statement::DropView(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_view_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropMaterializedView(ref drop_stmt) => self
                .execute_drop_materialized_view_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    None,
                    0,
                    configured_search_path,
                ),
            Statement::DropRule(ref drop_stmt) => self.execute_drop_rule_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropSchema(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::AlterSequence(ref alter_stmt) => self
                .execute_alter_sequence_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceOwner(ref alter_stmt) => self
                .execute_alter_sequence_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceRename(ref rename_stmt) => self
                .execute_alter_sequence_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceSetSchema(ref alter_stmt) => self
                .execute_alter_sequence_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::TruncateTable(ref truncate_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 0)), configured_search_path);
                let relations = resolve_truncate_relations(truncate_stmt, &catalog, false)?;
                let rels = relations
                    .iter()
                    .map(|relation| relation.rel)
                    .collect::<Vec<_>>();
                lock_tables_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    TableLockMode::AccessExclusive,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    write_xid_override: None,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    active_grouping_refs: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    security_restricted: false,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    pending_portals: Vec::new(),
                    copy_freeze_relation_oids: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    proc_execute_acl_cache: std::collections::HashSet::new(),
                    srf_rows_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys),
                    trigger_depth: 0,
                };
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let mut sequence_effects = Vec::new();
                let result = self.execute_truncate_table_in_transaction_with_search_path(
                    client_id,
                    truncate_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut ctx,
                    &mut catalog_effects,
                    &mut temp_effects,
                    &mut sequence_effects,
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                catalog_effects.append(&mut std::mem::take(&mut ctx.catalog_effects));
                catalog_effects.extend(std::mem::take(&mut ctx.pending_catalog_effects));
                temp_effects.append(&mut std::mem::take(&mut ctx.temp_effects));
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &catalog_effects,
                    &temp_effects,
                    &sequence_effects,
                    pending_async_notifications,
                );
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                guard.disarm();
                result
            }
            Statement::Vacuum(ref vacuum_stmt) => self.execute_vacuum_stmt_with_search_path(
                client_id,
                vacuum_stmt,
                configured_search_path,
                Some(gucs),
            ),
            Statement::SetTransaction(_)
            | Statement::Begin(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::PrepareTransaction(_)
            | Statement::CommitPrepared(_)
            | Statement::RollbackPrepared(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::RollbackTo(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::DeclareCursor(_)
            | Statement::Fetch(_)
            | Statement::Move(_)
            | Statement::ClosePortal(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "session command handled by session layer",
                actual: "session command".into(),
            })),
        }
    }

    pub fn execute_streaming(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            None,
            None,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            None,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_config(
            client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
            None,
            None,
            PlannerConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_config(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        snapshot_override: Option<crate::backend::access::transam::xact::Snapshot>,
        serializable_xact: Option<SerializableXactId>,
        planner_config: PlannerConfig,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_config_and_random_state(
            client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            configured_search_path,
            datetime_config,
            gucs,
            snapshot_override,
            serializable_xact,
            planner_config,
            crate::backend::executor::PgPrngState::shared(),
        )
    }

    pub(crate) fn execute_streaming_with_config_and_random_state(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        snapshot_override: Option<crate::backend::access::transam::xact::Snapshot>,
        serializable_xact: Option<SerializableXactId>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    ) -> Result<SelectGuard, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::exec_expr::clear_subquery_eval_cache;
        use crate::backend::executor::executor_start;

        clear_subquery_eval_cache();
        let visible_catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let visible_catalog_snapshot = Some(crate::backend::executor::executor_catalog(
            visible_catalog.clone(),
        ));
        let (query_desc, rels) = {
            if restrict_nonsystem_view_enabled(gucs) {
                reject_restricted_views_in_select(select_stmt, &visible_catalog)?;
            }
            let query_desc = crate::backend::executor::create_query_desc(
                crate::backend::rewrite::with_restrict_nonsystem_view_expansion(
                    restrict_nonsystem_view_enabled(gucs),
                    || {
                        crate::backend::parser::pg_plan_query_with_config(
                            select_stmt,
                            &visible_catalog,
                            planner_config,
                        )
                    },
                )?,
                None,
            );
            if restrict_nonsystem_view_enabled(gucs) {
                reject_restricted_views_in_planned_stmt(
                    &query_desc.planned_stmt,
                    &visible_catalog,
                )?;
            }
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_planned_stmt(&query_desc.planned_stmt, &mut rels);
            (query_desc, rels.into_iter().collect::<Vec<_>>())
        };
        let privilege_planned_stmt = query_desc.planned_stmt.clone();

        let transaction_snapshot = snapshot_override.clone();
        let (snapshot, command_id) = match (snapshot_override, txn_ctx) {
            (Some(snapshot), Some((_xid, cid))) => (snapshot, cid),
            (Some(snapshot), None) => {
                let cid = snapshot.current_cid;
                (snapshot, cid)
            }
            (None, Some((xid, cid))) => (self.txns.read().snapshot_for_command(xid, cid)?, cid),
            (None, None) => (self.txns.read().snapshot(INVALID_TRANSACTION_ID)?, 0),
        };
        let transaction_state: SharedExecutorTransactionState =
            std::sync::Arc::new(parking_lot::Mutex::new(ExecutorTransactionState {
                xid: (snapshot.current_xid != INVALID_TRANSACTION_ID)
                    .then_some(snapshot.current_xid),
                cid: command_id,
                transaction_snapshot,
                serializable_xact,
            }));
        let columns = query_desc.columns();
        let column_names = query_desc.column_names();
        let state = executor_start(query_desc.planned_stmt.plan_tree);
        let interrupts = self.interrupt_state(client_id);
        let session_replication_role = self.session_replication_role(client_id);
        let ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            data_dir: Some(self.cluster.base_dir.clone()),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(std::sync::Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            row_locks: std::sync::Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: datetime_config.clone(),
            statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
            gucs: gucs.clone(),
            interrupts,
            stats: std::sync::Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            write_xid_override: None,
            transaction_state: Some(transaction_state),
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            next_command_id: command_id,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: query_desc.planned_stmt.subplans,
            timed: false,
            allow_side_effects: true,
            security_restricted: false,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
            copy_freeze_relation_oids: Vec::new(),
            catalog: visible_catalog_snapshot,
            scalar_function_cache: std::collections::HashMap::new(),
            proc_execute_acl_cache: std::collections::HashSet::new(),
            srf_rows_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: Some(
                crate::backend::executor::DeferredForeignKeyTracker::default(),
            ),
            trigger_depth: 0,
        };
        if select_stmt.locking_clause.is_some() {
            crate::backend::commands::tablecmds::check_planned_stmt_select_for_update_privileges(
                &privilege_planned_stmt,
                &ctx,
            )?;
        } else {
            crate::backend::commands::tablecmds::check_planned_stmt_select_privileges(
                &privilege_planned_stmt,
                &ctx,
            )?;
        }
        lock_relations_interruptible(&self.table_locks, client_id, &rels, ctx.interrupts.as_ref())?;

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: std::sync::Arc::clone(&self.table_locks),
            client_id,
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            row_locks: std::sync::Arc::clone(&self.row_locks),
            statement_lock_scope_id,
            interrupt_guard: None,
            catalog_effect_start: 0,
            base_command_id: command_id,
        })
    }
}

fn partitioned_truncate_targets(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    root_oid: u32,
) -> Vec<crate::backend::parser::BoundRelation> {
    catalog
        .find_all_inheritors(root_oid)
        .into_iter()
        .filter(|oid| *oid != root_oid)
        .filter_map(|oid| catalog.relation_by_oid(oid))
        .filter(|entry| entry.relkind == 'r')
        .collect()
}

fn inherited_truncate_targets(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    root_oid: u32,
) -> Vec<crate::backend::parser::BoundRelation> {
    catalog
        .find_all_inheritors(root_oid)
        .into_iter()
        .filter_map(|oid| catalog.relation_by_oid(oid))
        .filter(|entry| entry.relkind == 'r')
        .collect()
}
