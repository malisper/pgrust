use super::super::*;
use super::alter_table_work_queue::{
    build_alter_table_work_queue, has_inheritance_children, relation_name_for_alter_error,
};
use super::constraint::{find_constraint_row, validate_check_rows, validate_not_null_rows};
use super::create::{
    aggregate_signature_arg_oids, format_aggregate_signature, owned_sequence_dependency_kind,
    resolve_aggregate_proc_rows,
};
use super::foreign_data_wrapper::format_fdw_options;
use super::operator::{
    lookup_operator_row, operator_signature_display, resolve_operator_type_oid,
    unsupported_postfix_operator_error,
};
use super::tablespace::{
    ensure_non_global_relation_tablespace, ensure_tablespace_create_privilege,
    tablespace_oid_by_name,
};
use super::typed_table::reject_typed_table_ddl;
use crate::backend::access::heap::heapam::heap_update_with_waiter;
use crate::backend::commands::tablecmds::{
    collect_matching_rows_heap, evaluate_default_value, maintain_indexes_for_row,
};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::executor::value_io::{coerce_assignment_value, tuple_from_values};
use crate::backend::executor::{
    ExecutorContext, RelationDesc, enforce_domain_constraints_for_value_ref,
};
use crate::backend::parser::{
    AlterTableSetPersistenceStatement, AlterTableSetTablespaceStatement, BoundRelation,
    CatalogLookup, SqlType, TablePersistence, bind_scalar_expr_in_scope, parse_type_name,
    resolve_raw_type_name,
};
use crate::backend::storage::smgr::{ForkNumber, segment_path};
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CONSTRAINT_FOREIGN, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL,
    PG_CATALOG_NAMESPACE_OID, PG_CLASS_RELATION_OID, PG_TOAST_NAMESPACE_OID, relkind_has_storage,
    relkind_is_analyzable,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    AlterTableAddColumnsStatement, AlterTableAddConstraintStatement, ColumnConstraint,
    CommentOnAggregateStatement, CommentOnColumnStatement, CommentOnFunctionStatement,
    CommentOnIndexStatement, CommentOnOperatorStatement, CommentOnSequenceStatement,
    CommentOnViewStatement, MaintenanceTarget, TableConstraint, VacuumStatement,
};
use crate::include::nodes::primnodes::{
    BuiltinScalarFunction, Expr, ScalarFunctionImpl, user_attrno,
};
use crate::pgrust::auth::AuthState;
use crate::pgrust::autovacuum::{AutovacuumRelationInput, relation_needs_vacanalyze};
use crate::pgrust::database::ddl::{
    dependent_view_rewrites_for_relation, lookup_analyzable_relation_for_ddl,
    lookup_table_or_partitioned_table_for_alter_table,
};
use crate::{ClientId, RelFileLocator};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::Instant;

struct AddColumnTarget {
    relation: BoundRelation,
    column: crate::backend::executor::ColumnDesc,
    new_desc: RelationDesc,
    column_index: usize,
    append_column: bool,
    rewrite_existing_rows: bool,
    direct_parent_count: i16,
}

const AUTOVACUUM_CLIENT_ID_BASE: ClientId = 0xFF00_0000;

pub(super) fn copy_rewritten_relation_storage(
    base_dir: &std::path::Path,
    effect: &CatalogMutationEffect,
) -> Result<(), ExecError> {
    for (old_rel, new_rel) in effect
        .dropped_rels
        .iter()
        .copied()
        .zip(effect.created_rels.iter().copied())
    {
        copy_relation_storage_files(base_dir, old_rel, new_rel)?;
    }
    Ok(())
}

fn copy_relation_storage_files(
    base_dir: &std::path::Path,
    old_rel: RelFileLocator,
    new_rel: RelFileLocator,
) -> Result<(), ExecError> {
    for fork in [
        ForkNumber::Main,
        ForkNumber::Fsm,
        ForkNumber::VisibilityMap,
        ForkNumber::Init,
    ] {
        let mut segno = 0;
        loop {
            let source = segment_path(base_dir, old_rel, fork, segno);
            if !source.exists() {
                break;
            }
            let target = segment_path(base_dir, new_rel, fork, segno);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|err| ExecError::DetailedError {
                    message: format!("could not create tablespace directory: {err}"),
                    detail: None,
                    hint: None,
                    sqlstate: "58030",
                })?;
            }
            fs::copy(&source, &target).map_err(|err| ExecError::DetailedError {
                message: format!("could not move relation storage to tablespace: {err}"),
                detail: None,
                hint: None,
                sqlstate: "58030",
            })?;
            segno += 1;
        }
    }
    Ok(())
}

fn reject_composite_rowtype_non_null_default_dependency(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    column: &crate::include::nodes::parsenodes::ColumnDef,
) -> Result<(), ExecError> {
    if !column_default_is_non_null(column.default_expr.as_deref()) {
        return Ok(());
    }
    let Some(type_row) = catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typrelid == relation.relation_oid)
    else {
        return Ok(());
    };
    for class_row in catalog.class_rows() {
        if class_row.oid == relation.relation_oid || !relkind_is_analyzable(class_row.relkind) {
            continue;
        }
        let Some(dependent_relation) = catalog.lookup_relation_by_oid(class_row.oid) else {
            continue;
        };
        if let Some(dependent_column) = dependent_relation.desc.columns.iter().find(|candidate| {
            !candidate.dropped
                && (catalog.type_oid_for_sql_type(candidate.sql_type) == Some(type_row.oid)
                    || candidate.sql_type.type_oid == type_row.oid
                    || candidate.sql_type.typrelid == relation.relation_oid)
        }) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot alter table \"{}\" because column \"{}.{}\" uses its row type",
                    type_row.typname, class_row.relname, dependent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
    }
    Ok(())
}

fn column_default_is_non_null(default_expr: Option<&str>) -> bool {
    default_expr.is_some_and(|expr| !expr.trim().eq_ignore_ascii_case("null"))
}

fn duplicate_add_column_error(column_name: &str) -> ExecError {
    ExecError::Parse(ParseError::UnexpectedToken {
        expected: "new column name",
        actual: format!("column already exists: {column_name}"),
    })
}

fn reject_recursive_composite_column_type(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    column: &crate::include::nodes::parsenodes::ColumnDef,
) -> Result<(), ExecError> {
    let sql_type = match column.ty {
        crate::backend::parser::RawTypeName::Serial(_) => return Ok(()),
        _ => resolve_raw_type_name(&column.ty, catalog).map_err(ExecError::Parse)?,
    };
    if !sql_type_contains_relation_rowtype(catalog, sql_type, relation.relation_oid) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "composite type {} cannot be made a member of itself",
            relation_name_for_alter_error(catalog, relation.relation_oid)
        ),
        detail: None,
        hint: None,
        sqlstate: "42P16",
    })
}

fn sql_type_contains_relation_rowtype(
    catalog: &dyn CatalogLookup,
    sql_type: SqlType,
    relation_oid: u32,
) -> bool {
    sql_type_contains_relation_rowtype_inner(
        catalog,
        sql_type,
        relation_oid,
        &mut BTreeSet::new(),
        &mut BTreeSet::new(),
    )
}

fn sql_type_contains_relation_rowtype_inner(
    catalog: &dyn CatalogLookup,
    sql_type: SqlType,
    relation_oid: u32,
    seen_types: &mut BTreeSet<u32>,
    seen_relations: &mut BTreeSet<u32>,
) -> bool {
    if sql_type.typrelid == relation_oid {
        return true;
    }
    let mut pending = Vec::new();
    if sql_type.type_oid != 0 {
        pending.push(sql_type.type_oid);
    }
    if let Some(type_oid) = catalog.type_oid_for_sql_type(sql_type) {
        pending.push(type_oid);
    }
    while let Some(type_oid) = pending.pop() {
        if type_oid == 0 || !seen_types.insert(type_oid) {
            continue;
        }
        let Some(row) = catalog.type_by_oid(type_oid) else {
            continue;
        };
        if row.typrelid == relation_oid || row.sql_type.typrelid == relation_oid {
            return true;
        }
        if row.typrelid != 0
            && seen_relations.insert(row.typrelid)
            && let Some(relation) = catalog.lookup_relation_by_oid(row.typrelid)
            && relation.desc.columns.iter().any(|column| {
                !column.dropped
                    && sql_type_contains_relation_rowtype_inner(
                        catalog,
                        column.sql_type,
                        relation_oid,
                        seen_types,
                        seen_relations,
                    )
            })
        {
            return true;
        }
        if row.typbasetype != 0 {
            pending.push(row.typbasetype);
        }
        if row.typelem != 0 {
            pending.push(row.typelem);
        }
    }
    false
}

fn preflight_alter_table_add_columns(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    alter_stmt: &AlterTableAddColumnsStatement,
) -> Result<(), ExecError> {
    let table_name = relation_basename(&alter_stmt.table_name).to_string();
    let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
    let existing_columns = relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut added_columns = BTreeSet::new();

    for add_stmt in &alter_stmt.columns {
        let column_name = add_stmt.column.name.to_ascii_lowercase();
        if existing_columns.contains(&column_name) {
            if add_stmt.missing_ok {
                continue;
            }
            return Err(duplicate_add_column_error(&add_stmt.column.name));
        }
        if !added_columns.insert(column_name) {
            if add_stmt.missing_ok {
                continue;
            }
            return Err(duplicate_add_column_error(&add_stmt.column.name));
        }
        reject_recursive_composite_column_type(catalog, relation, &add_stmt.column)?;
        reject_composite_rowtype_non_null_default_dependency(catalog, relation, &add_stmt.column)?;
        validate_alter_table_add_column(
            &table_name,
            &relation.desc,
            &add_stmt.column,
            &existing_constraints,
            catalog,
        )?;
    }
    Ok(())
}

fn inline_added_column_table_constraints(
    alter_stmt: &AlterTableAddColumnStatement,
) -> Vec<AlterTableAddConstraintStatement> {
    let mut constraints = Vec::new();
    for constraint in &alter_stmt.column.constraints {
        let table_constraint = match constraint {
            ColumnConstraint::PrimaryKey {
                attributes,
                tablespace,
            } => TableConstraint::PrimaryKey {
                attributes: attributes.clone(),
                columns: vec![alter_stmt.column.name.clone()],
                include_columns: Vec::new(),
                without_overlaps: None,
                tablespace: tablespace.clone(),
            },
            ColumnConstraint::Unique {
                attributes,
                tablespace,
            } => TableConstraint::Unique {
                attributes: attributes.clone(),
                columns: vec![alter_stmt.column.name.clone()],
                include_columns: Vec::new(),
                without_overlaps: None,
                tablespace: tablespace.clone(),
            },
            ColumnConstraint::References {
                attributes,
                referenced_table,
                referenced_columns,
                match_type,
                on_delete,
                on_delete_set_columns,
                on_update,
            } => TableConstraint::ForeignKey {
                attributes: attributes.clone(),
                columns: vec![alter_stmt.column.name.clone()],
                period: None,
                referenced_table: referenced_table.clone(),
                referenced_columns: referenced_columns.clone(),
                referenced_period: None,
                match_type: *match_type,
                on_delete: *on_delete,
                on_delete_set_columns: on_delete_set_columns.clone(),
                on_update: *on_update,
            },
            ColumnConstraint::NotNull { .. } | ColumnConstraint::Check { .. } => continue,
        };
        constraints.push(AlterTableAddConstraintStatement {
            if_exists: alter_stmt.if_exists,
            only: alter_stmt.only,
            table_name: alter_stmt.table_name.clone(),
            constraint: table_constraint,
        });
    }
    constraints
}

fn add_column_is_plain_batch_compatible(alter_stmt: &AlterTableAddColumnStatement) -> bool {
    alter_stmt.fdw_options.is_none()
        && !matches!(
            alter_stmt.column.ty,
            crate::backend::parser::RawTypeName::Serial(_)
        )
        && alter_stmt.column.default_expr.is_none()
        && alter_stmt.column.generated.is_none()
        && alter_stmt.column.identity.is_none()
        && alter_stmt.column.constraints.is_empty()
}

#[derive(Debug, Clone)]
struct AutovacuumTarget {
    relation_oid: u32,
    rel: RelFileLocator,
    vacuum: bool,
    analyze: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct VacuumExecOptions {
    analyze: bool,
    full: bool,
    index_cleanup: Option<bool>,
    truncate: Option<bool>,
    default_truncate: bool,
    parallel_workers: Option<i32>,
    process_main: bool,
    process_toast: bool,
    only_database_stats: bool,
}

fn autovacuum_client_id(database_oid: u32) -> ClientId {
    AUTOVACUUM_CLIENT_ID_BASE | (database_oid & 0x0000_FFFF)
}

fn autovacuum_namespace_allowed(namespace_name: &str) -> bool {
    namespace_name != "pg_catalog"
        && namespace_name != "information_schema"
        && !namespace_name.starts_with("pg_toast")
        && !namespace_name.starts_with("pg_temp_")
        && !namespace_name.starts_with("pg_toast_temp_")
}

fn autovacuum_enabled(reloptions: Option<&[String]>) -> bool {
    reloptions
        .and_then(|options| {
            options.iter().find_map(|option| {
                let (name, value) = option.split_once('=')?;
                name.eq_ignore_ascii_case("autovacuum_enabled").then(|| {
                    !matches!(
                        value.to_ascii_lowercase().as_str(),
                        "false" | "off" | "no" | "0"
                    )
                })
            })
        })
        .unwrap_or(true)
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn replace_statistics_ext_data_rows_for_analyze(
    store: &mut crate::backend::catalog::store::CatalogStore,
    rows: &[crate::include::catalog::PgStatisticExtDataRow],
    ctx: &CatalogWriteContext,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<(), ExecError> {
    let mut rows_by_oid = BTreeMap::<u32, Vec<_>>::new();
    for row in rows.iter().cloned() {
        rows_by_oid.entry(row.stxoid).or_default().push(row);
    }
    for (statistics_oid, rows) in rows_by_oid {
        let effect = store
            .replace_statistics_data_rows_mvcc(statistics_oid, rows, ctx)
            .map_err(ExecError::from)?;
        catalog_effects.push(effect);
    }
    Ok(())
}

fn lookup_vacuum_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<crate::backend::parser::BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'm' | 'p') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or materialized view",
        })),
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

fn lookup_index_relation_for_comment(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<crate::backend::parser::BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'i' | 'I') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "index",
        })),
        None => Err(ExecError::DetailedError {
            message: format!("relation \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        }),
    }
}

fn vacuum_option_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

fn parse_vacuum_parallel_workers(stmt: &VacuumStatement) -> Result<Option<i32>, ExecError> {
    if !stmt.parallel_specified {
        return Ok(None);
    }
    let Some(raw) = stmt.parallel.as_deref() else {
        return Err(vacuum_option_error(
            "parallel option requires a value between 0 and 1024",
            "42601",
        ));
    };
    let workers = raw.parse::<i32>().map_err(|_| {
        vacuum_option_error(
            "parallel workers for vacuum must be between 0 and 1024",
            "42601",
        )
    })?;
    if !(0..=1024).contains(&workers) {
        return Err(vacuum_option_error(
            "parallel workers for vacuum must be between 0 and 1024",
            "42601",
        ));
    }
    Ok(Some(workers))
}

fn parse_vacuum_index_cleanup(raw: Option<&str>) -> Result<Option<bool>, ExecError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(None),
        "true" | "on" | "yes" | "1" => Ok(Some(true)),
        "false" | "off" | "no" | "0" => Ok(Some(false)),
        _ => Err(vacuum_option_error(
            "index_cleanup requires a Boolean value",
            "42601",
        )),
    }
}

fn parse_buffer_usage_limit_kb(raw: &str) -> Option<i64> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    let mut parts = normalized.split_whitespace();
    let number = parts.next()?.parse::<i64>().ok()?;
    match parts.next() {
        None | Some("kb") | Some("k") => Some(number),
        Some(_) => None,
    }
}

fn validate_buffer_usage_limit(raw: &str) -> Result<(), ExecError> {
    let Some(kb) = parse_buffer_usage_limit_kb(raw) else {
        return Err(vacuum_option_error(
            "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
            "22023",
        ));
    };
    if kb == 0 || (128..=16_777_216).contains(&kb) {
        return Ok(());
    }
    if raw.trim().split_whitespace().next().is_some_and(|number| {
        number
            .parse::<i64>()
            .ok()
            .is_some_and(|value| i32::try_from(value).is_err())
    }) {
        return Err(ExecError::DetailedError {
            message: "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB".into(),
            detail: None,
            hint: Some("Value exceeds integer range.".into()),
            sqlstate: "22023",
        });
    }
    Err(vacuum_option_error(
        "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
        "22023",
    ))
}

fn vacuum_exec_options(
    stmt: &VacuumStatement,
    gucs: Option<&std::collections::HashMap<String, String>>,
) -> Result<VacuumExecOptions, ExecError> {
    let parallel_workers = parse_vacuum_parallel_workers(stmt)?;
    let index_cleanup = parse_vacuum_index_cleanup(stmt.index_cleanup.as_deref())?;
    if let Some(raw) = &stmt.buffer_usage_limit {
        validate_buffer_usage_limit(raw)?;
    }
    if stmt.targets.iter().any(|target| !target.columns.is_empty()) && !stmt.analyze {
        return Err(vacuum_option_error(
            "ANALYZE option must be specified when a column list is provided",
            "0A000",
        ));
    }
    if stmt.full && parallel_workers.unwrap_or(0) > 0 {
        return Err(vacuum_option_error(
            "VACUUM FULL cannot be performed in parallel",
            "0A000",
        ));
    }
    if stmt.full && stmt.buffer_usage_limit.is_some() && !stmt.analyze {
        return Err(vacuum_option_error(
            "BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL",
            "0A000",
        ));
    }
    if stmt.full && stmt.disable_page_skipping {
        return Err(vacuum_option_error(
            "VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL",
            "0A000",
        ));
    }
    let process_toast = stmt.process_toast.unwrap_or(true);
    if stmt.full && !process_toast {
        return Err(vacuum_option_error(
            "PROCESS_TOAST required with VACUUM FULL",
            "0A000",
        ));
    }
    if stmt.only_database_stats {
        if !stmt.targets.is_empty() {
            return Err(vacuum_option_error(
                "ONLY_DATABASE_STATS cannot be specified with a list of tables",
                "0A000",
            ));
        }
        if stmt.analyze
            || stmt.full
            || stmt.freeze
            || stmt.disable_page_skipping
            || stmt.buffer_usage_limit.is_some()
            || stmt.parallel_specified
            || stmt.skip_database_stats
        {
            return Err(vacuum_option_error(
                "ONLY_DATABASE_STATS cannot be specified with other VACUUM options",
                "0A000",
            ));
        }
    }
    Ok(VacuumExecOptions {
        analyze: stmt.analyze,
        full: stmt.full,
        index_cleanup,
        truncate: stmt.truncate,
        default_truncate: gucs
            .and_then(|gucs| gucs.get("vacuum_truncate"))
            .map(|value| {
                !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "false" | "off" | "no" | "0"
                )
            })
            .unwrap_or(true),
        parallel_workers,
        process_main: stmt.process_main.unwrap_or(true),
        process_toast,
        only_database_stats: stmt.only_database_stats,
    })
}

fn lookup_table_or_partitioned_relation_for_comment(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<crate::backend::parser::BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::DetailedError {
            message: format!("relation \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        }),
    }
}

fn lookup_column_comment_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<crate::backend::parser::BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f' | 'c') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::DetailedError {
            message: format!("relation \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        }),
    }
}

fn lookup_sequence_relation_for_comment(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<crate::backend::parser::BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if entry.relkind == 'S' => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "sequence",
        })),
        None => Err(ExecError::DetailedError {
            message: format!("relation \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        }),
    }
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| part.parse::<u32>().ok())
        .collect()
}

fn ensure_function_owner(
    db: &Database,
    client_id: ClientId,
    owner_oid: u32,
    function_name: &str,
    txn_ctx: CatalogTxnContext,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.has_effective_membership(owner_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of function {function_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn resolve_exact_function_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    function_name: &str,
    arg_types: &[String],
) -> Result<crate::include::catalog::PgProcRow, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let desired_arg_oids = arg_types
        .iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.clone()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(ExecError::Parse)?;
    let schema_oid = match function_name.rsplit_once('.') {
        Some((schema_name, _)) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let base_name = function_name.rsplit('.').next().unwrap_or(function_name);
    let signature_arg_types = desired_arg_oids
        .iter()
        .map(|oid| format_type_text(*oid, None, &catalog))
        .collect::<Vec<_>>()
        .join(", ");
    let signature = format!("{function_name}({signature_arg_types})");
    let matches = catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .filter(|row| {
            row.prokind == 'f'
                && parse_proc_argtype_oids(&row.proargtypes) == Some(desired_arg_oids.clone())
                && schema_oid
                    .map(|schema_oid| row.pronamespace == schema_oid)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.clone()),
        [] => Err(ExecError::DetailedError {
            message: format!("function {signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        }),
        _ => Err(ExecError::DetailedError {
            message: format!("function name \"{signature}\" is not unique"),
            detail: None,
            hint: Some("Specify the argument list to select the function unambiguously.".into()),
            sqlstate: "42725",
        }),
    }
}

fn rewrite_heap_rows_for_added_serial_column(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    indexes: &[crate::backend::parser::BoundIndexRelation],
    sequence_oid: u32,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let target_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
    let new_column = new_desc
        .columns
        .last()
        .expect("serial add-column rewrite requires appended column");
    for (tid, mut values) in target_rows {
        ctx.check_for_interrupts()?;
        let next = db
            .sequences
            .allocate_value(sequence_oid, relation.relpersistence != 't')?;
        values.push(coerce_assignment_value(
            &Value::Int64(next),
            new_column.sql_type,
        )?);
        let replacement = tuple_from_values(new_desc, &values)?;
        let new_tid = heap_update_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            relation.rel,
            &ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
            None,
        )?;
        maintain_indexes_for_row(relation.rel, new_desc, indexes, &values, new_tid, ctx)?;
    }
    Ok(())
}

fn added_column_default_sql(
    column: &crate::backend::executor::ColumnDesc,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    if column.generated.is_some() || column.default_sequence_oid.is_some() {
        return None;
    }
    column.default_expr.clone().or_else(|| {
        catalog
            .type_oid_for_sql_type(column.sql_type)
            .and_then(|type_oid| catalog.type_default_sql(type_oid))
    })
}

fn bind_added_column_default_expr(
    column: &crate::backend::executor::ColumnDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ExecError> {
    let Some(default_sql) = added_column_default_sql(column, catalog) else {
        return Ok(None);
    };
    let parsed = crate::backend::parser::parse_expr(&default_sql).map_err(ExecError::Parse)?;
    let (bound, _) = bind_scalar_expr_in_scope(&parsed, &[], catalog).map_err(ExecError::Parse)?;
    Ok(Some(bound))
}

fn added_column_default_is_volatile(
    column: &crate::backend::executor::ColumnDesc,
    catalog: &dyn CatalogLookup,
) -> Result<bool, ExecError> {
    Ok(bind_added_column_default_expr(column, catalog)?
        .as_ref()
        .is_some_and(|expr| expr_contains_volatile_default(expr, catalog)))
}

fn proc_is_volatile_default(proc_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    catalog
        .proc_row_by_oid(proc_oid)
        .is_none_or(|row| row.provolatile == 'v')
}

fn builtin_scalar_function_is_volatile_default(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::Random
            | BuiltinScalarFunction::RandomNormal
            | BuiltinScalarFunction::ClockTimestamp
            | BuiltinScalarFunction::TimeOfDay
            | BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::IdentityNextVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::SetConfig
            | BuiltinScalarFunction::PgNotify
            | BuiltinScalarFunction::GenRandomUuid
            | BuiltinScalarFunction::UuidV7
    )
}

fn expr_contains_volatile_default(expr: &Expr, catalog: &dyn CatalogLookup) -> bool {
    match expr {
        Expr::Random => true,
        Expr::Func(func) => {
            match func.implementation {
                ScalarFunctionImpl::Builtin(func)
                    if builtin_scalar_function_is_volatile_default(func) =>
                {
                    return true;
                }
                ScalarFunctionImpl::UserDefined { proc_oid }
                    if proc_is_volatile_default(proc_oid, catalog) =>
                {
                    return true;
                }
                _ => {}
            }
            func.args
                .iter()
                .any(|arg| expr_contains_volatile_default(arg, catalog))
        }
        Expr::Op(op) => {
            let proc_oid = (op.opfuncid != 0)
                .then_some(op.opfuncid)
                .or_else(|| catalog.operator_by_oid(op.opno).map(|row| row.oprcode));
            proc_oid.is_some_and(|oid| proc_is_volatile_default(oid, catalog))
                || op
                    .args
                    .iter()
                    .any(|arg| expr_contains_volatile_default(arg, catalog))
        }
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_contains_volatile_default(arg, catalog)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|arg| expr_contains_volatile_default(arg, catalog))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_volatile_default(&arm.expr, catalog)
                        || expr_contains_volatile_default(&arm.result, catalog)
                })
                || expr_contains_volatile_default(&case_expr.defresult, catalog)
        }
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|arg| expr_contains_volatile_default(arg, catalog)),
        Expr::ScalarArrayOp(op) => {
            expr_contains_volatile_default(&op.left, catalog)
                || expr_contains_volatile_default(&op.right, catalog)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|arg| expr_contains_volatile_default(arg, catalog)),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_volatile_default(inner, catalog)
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
            expr_contains_volatile_default(expr, catalog)
                || expr_contains_volatile_default(pattern, catalog)
                || escape
                    .as_ref()
                    .is_some_and(|arg| expr_contains_volatile_default(arg, catalog))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::FieldSelect { expr: inner, .. } => {
            expr_contains_volatile_default(inner, catalog)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_volatile_default(left, catalog)
                || expr_contains_volatile_default(right, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|arg| expr_contains_volatile_default(arg, catalog)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, field)| expr_contains_volatile_default(field, catalog)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_volatile_default(array, catalog)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|arg| expr_contains_volatile_default(arg, catalog))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|arg| expr_contains_volatile_default(arg, catalog))
                })
        }
        Expr::Aggref(_)
        | Expr::GroupingKey(_)
        | Expr::GroupingFunc(_)
        | Expr::WindowFunc(_)
        | Expr::SetReturning(_)
        | Expr::SubLink(_)
        | Expr::SubPlan(_) => true,
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn evaluate_added_column_default_value(
    desc: &RelationDesc,
    column_index: usize,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = evaluate_default_value(desc, column_index, ctx)?;
    coerce_assignment_value(&value, desc.columns[column_index].sql_type)
}

fn rewrite_heap_rows_for_added_default_column(
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    column_index: usize,
    indexes: &[crate::backend::parser::BoundIndexRelation],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let target_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
    for (tid, mut values) in target_rows {
        ctx.check_for_interrupts()?;
        while values.len() < column_index {
            let column = &new_desc.columns[values.len()];
            values.push(crate::backend::executor::value_io::missing_column_value(
                column,
            ));
        }
        let value = evaluate_added_column_default_value(new_desc, column_index, ctx)?;
        values.push(value);
        let replacement = tuple_from_values(new_desc, &values)?;
        let new_tid = heap_update_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            relation.rel,
            &ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
            None,
        )?;
        maintain_indexes_for_row(relation.rel, new_desc, indexes, &values, new_tid, ctx)?;
    }
    Ok(())
}

fn add_column_validation_executor_context<C>(
    db: &Database,
    catalog: C,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<ExecutorContext, ExecError>
where
    C: CatalogLookup + 'static,
{
    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    Ok(ExecutorContext {
        pool: Arc::clone(&db.pool),
        data_dir: None,
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        lock_status_provider: Some(Arc::new(db.clone())),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        stats_import_runtime: None,
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: Arc::clone(&db.advisory_locks),
        row_locks: Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        statement_timestamp_usecs:
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: super::maintenance_safe_gucs(),
        interrupts,
        stats: Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot,
        write_xid_override: None,
        transaction_state: None,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: db.auth_state(client_id).session_user_oid(),
        current_user_oid: db.auth_state(client_id).current_user_oid(),
        active_role_oid: db.auth_state(client_id).active_role_oid(),
        session_replication_role: db.session_replication_role(client_id),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: cid,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        random_state: crate::backend::executor::PgPrngState::shared(),
        timed: false,
        allow_side_effects: false,
        security_restricted: false,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        active_grouping_refs: Vec::new(),
        subplans: Vec::new(),
        pending_async_notifications: Vec::new(),
        pending_portals: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: Some(db.clone()),
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        catalog: Some(crate::backend::executor::executor_catalog(catalog)),
        scalar_function_cache: std::collections::HashMap::new(),
        proc_execute_acl_cache: std::collections::HashSet::new(),
        srf_rows_cache: std::collections::HashMap::new(),
        plpgsql_function_cache: db.plpgsql_function_cache(client_id),
        pinned_cte_tables: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    })
}

fn validate_added_column_domain_constraints<C>(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    column_index: usize,
    catalog: C,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError>
where
    C: CatalogLookup + 'static,
{
    if matches!(relation.relkind, 'f' | 'p') {
        return Ok(());
    }
    let Some(column) = relation.desc.columns.get(column_index).cloned() else {
        return Ok(());
    };
    if catalog
        .domain_by_type_oid(column.sql_type.type_oid)
        .is_none()
    {
        return Ok(());
    }
    let mut ctx =
        add_column_validation_executor_context(db, catalog, client_id, xid, cid, interrupts)?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    for (_, values) in rows {
        ctx.check_for_interrupts()?;
        let value = match values.get(column_index) {
            Some(value) if !matches!(value, Value::Null) || column.default_expr.is_none() => {
                value.clone()
            }
            _ => evaluate_default_value(&relation.desc, column_index, &mut ctx)?,
        };
        enforce_domain_constraints_for_value_ref(&value, column.sql_type, &mut ctx)?;
    }
    Ok(())
}

fn current_database_owner_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == db.database_oid)
        .map(|row| row.datdba)
        .ok_or_else(|| ExecError::DetailedError {
            message: "current database does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "3D000",
        })
}

fn collect_catalog_analyze_targets(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    analyze_stmt: &AnalyzeStatement,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    if !analyze_stmt.targets.is_empty() {
        return Ok(analyze_stmt.targets.clone());
    }

    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let is_superuser = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper);
    let database_owner_oid = current_database_owner_oid(db, client_id, txn_ctx)?;
    let class_rows = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .class_rows();

    let mut targets = Vec::new();
    for class in class_rows {
        if !relkind_is_analyzable(class.relkind) {
            continue;
        }
        if db.other_session_temp_namespace_oid(client_id, class.relnamespace) {
            continue;
        }
        if !is_superuser
            && auth.current_user_oid() != database_owner_oid
            && !auth.has_effective_membership(class.relowner, &auth_catalog)
        {
            continue;
        }
        let Some(table_name) = crate::backend::utils::cache::lsyscache::relation_display_name(
            db,
            client_id,
            txn_ctx,
            configured_search_path,
            class.oid,
        ) else {
            continue;
        };
        targets.push(MaintenanceTarget {
            table_name,
            columns: Vec::new(),
            only: false,
        });
    }
    Ok(targets)
}

fn collect_catalog_vacuum_targets(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    vacuum_stmt: &VacuumStatement,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    if vacuum_stmt.only_database_stats {
        return Ok(Vec::new());
    }
    if !vacuum_stmt.targets.is_empty() {
        return Ok(vacuum_stmt.targets.clone());
    }

    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let is_superuser = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper);
    let database_owner_oid = current_database_owner_oid(db, client_id, txn_ctx)?;
    let class_rows = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .class_rows();

    let mut targets = Vec::new();
    for class in class_rows {
        if !matches!(class.relkind, 'r' | 'm') {
            continue;
        }
        if db.other_session_temp_namespace_oid(client_id, class.relnamespace) {
            continue;
        }
        if !is_superuser
            && auth.current_user_oid() != database_owner_oid
            && !auth.has_effective_membership(class.relowner, &auth_catalog)
        {
            continue;
        }
        let Some(table_name) = crate::backend::utils::cache::lsyscache::relation_display_name(
            db,
            client_id,
            txn_ctx,
            configured_search_path,
            class.oid,
        ) else {
            continue;
        };
        targets.push(MaintenanceTarget {
            table_name,
            columns: Vec::new(),
            only: false,
        });
    }
    Ok(targets)
}

fn relation_display_name_for_target(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| {
            catalog
                .namespace_row_by_oid(row.relnamespace)
                .map(|namespace| {
                    if matches!(namespace.nspname.as_str(), "public" | "pg_catalog")
                        || namespace.nspname.starts_with("pg_temp_")
                    {
                        row.relname.clone()
                    } else {
                        format!("{}.{}", namespace.nspname, row.relname)
                    }
                })
                .unwrap_or(row.relname)
        })
        .unwrap_or_else(|| relation_oid.to_string())
}

fn relation_warning_name_for_target(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    target: &MaintenanceTarget,
) -> String {
    catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_basename(&target.table_name).to_string())
}

fn validate_maintenance_columns(
    target: &MaintenanceTarget,
    relation: &BoundRelation,
) -> Result<(), ExecError> {
    let mut seen = BTreeSet::new();
    for column in &target.columns {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" appears more than once",
                    column,
                    relation_basename(&target.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42701",
            });
        }
        if !relation
            .desc
            .columns
            .iter()
            .any(|desc| !desc.dropped && desc.name.eq_ignore_ascii_case(column))
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    column,
                    relation_basename(&target.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        }
    }
    Ok(())
}

fn validate_maintenance_targets_for_vacuum(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
    analyze: bool,
) -> Result<(), ExecError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if matches!(entry.relkind, 'r' | 'm' | 'p') => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table or materialized view",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        if !analyze && !target.columns.is_empty() {
            return Err(vacuum_option_error(
                "ANALYZE option must be specified when a column list is provided",
                "0A000",
            ));
        }
        validate_maintenance_columns(target, &entry)?;
    }
    Ok(())
}

fn validate_maintenance_targets_for_analyze(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if relkind_is_analyzable(entry.relkind) => entry,
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
        validate_maintenance_columns(target, &entry)?;
    }
    Ok(())
}

fn expand_explicit_maintenance_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    let mut expanded = Vec::new();
    let mut seen = BTreeSet::new();
    for target in targets {
        let relation = lookup_vacuum_relation_for_ddl(catalog, &target.table_name)?;
        if target.only {
            if relation.relkind == 'p' {
                push_warning(format!(
                    "VACUUM ONLY of partitioned table \"{}\" has no effect",
                    relation_display_name_for_target(catalog, relation.relation_oid)
                ));
                continue;
            }
            if seen.insert(relation.relation_oid) {
                expanded.push(target.clone());
            }
            continue;
        }

        if seen.insert(relation.relation_oid) {
            expanded.push(target.clone());
        }
        for child_oid in catalog.find_all_inheritors(relation.relation_oid) {
            if child_oid == relation.relation_oid || !seen.insert(child_oid) {
                continue;
            }
            let Some(child) = catalog.relation_by_oid(child_oid) else {
                continue;
            };
            if !matches!(child.relkind, 'r' | 'm' | 'p') {
                continue;
            }
            expanded.push(MaintenanceTarget {
                table_name: relation_display_name_for_target(catalog, child.relation_oid),
                columns: target.columns.clone(),
                only: false,
            });
        }
    }
    Ok(expanded)
}

fn expand_explicit_analyze_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    let mut expanded = Vec::new();
    let mut seen = BTreeSet::new();
    for target in targets {
        let relation = lookup_analyzable_relation_for_ddl(catalog, &target.table_name)?;
        if seen.insert(relation.relation_oid) {
            expanded.push(target.clone());
        }
        if target.only {
            continue;
        }
        for child_oid in catalog.find_all_inheritors(relation.relation_oid) {
            if child_oid == relation.relation_oid || !seen.insert(child_oid) {
                continue;
            }
            let Some(child) = catalog.relation_by_oid(child_oid) else {
                continue;
            };
            if !relkind_is_analyzable(child.relkind) {
                continue;
            }
            expanded.push(MaintenanceTarget {
                table_name: relation_display_name_for_target(catalog, child.relation_oid),
                columns: target.columns.clone(),
                only: false,
            });
        }
    }
    Ok(expanded)
}

fn relation_is_maintenance_owner(
    relation: &BoundRelation,
    auth: &AuthState,
    auth_catalog: &crate::pgrust::auth::AuthCatalog,
    database_owner_oid: u32,
) -> bool {
    auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.current_user_oid() == database_owner_oid
        || auth.has_effective_membership(relation.owner_oid, auth_catalog)
}

fn filter_explicit_vacuum_targets_by_permission(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    targets: &[MaintenanceTarget],
    options: &VacuumExecOptions,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let database_owner_oid = current_database_owner_oid(db, client_id, txn_ctx)?;
    let mut allowed = Vec::with_capacity(targets.len());
    for target in targets {
        let relation = lookup_vacuum_relation_for_ddl(catalog, &target.table_name)?;
        if relation_is_maintenance_owner(&relation, &auth, &auth_catalog, database_owner_oid) {
            allowed.push(target.clone());
            continue;
        }
        let relname = relation_warning_name_for_target(catalog, &relation, target);
        let warned_vacuum = options.process_main || options.process_toast;
        if warned_vacuum {
            push_warning(format!(
                "permission denied to vacuum \"{}\", skipping it",
                relname
            ));
        } else if options.analyze {
            push_warning(format!(
                "permission denied to analyze \"{}\", skipping it",
                relname
            ));
        }
    }
    Ok(allowed)
}

fn filter_explicit_analyze_targets_by_permission(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    targets: &[MaintenanceTarget],
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let database_owner_oid = current_database_owner_oid(db, client_id, txn_ctx)?;
    let mut allowed = Vec::with_capacity(targets.len());
    for target in targets {
        let relation = lookup_analyzable_relation_for_ddl(catalog, &target.table_name)?;
        if relation_is_maintenance_owner(&relation, &auth, &auth_catalog, database_owner_oid) {
            allowed.push(target.clone());
            continue;
        }
        let relname = relation_warning_name_for_target(catalog, &relation, target);
        push_warning(format!(
            "permission denied to analyze \"{}\", skipping it",
            relname
        ));
    }
    Ok(allowed)
}

fn warn_parallel_vacuum_temp_tables(
    catalog: &dyn CatalogLookup,
    targets: &[MaintenanceTarget],
    options: &VacuumExecOptions,
) {
    if options.full || options.parallel_workers.unwrap_or(0) <= 0 {
        return;
    }
    for target in targets {
        if let Some(relation) = catalog.lookup_any_relation(&target.table_name)
            && relation.relpersistence == 't'
        {
            push_warning(format!(
                "disabling parallel option of vacuum on \"{}\" --- cannot vacuum temporary tables in parallel",
                relation_display_name_for_target(catalog, relation.relation_oid)
            ));
        }
    }
}

fn relation_name_for_add_column_notice(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn validate_persistence_foreign_keys(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    to_logged: bool,
) -> Result<(), ExecError> {
    let relation_name = relation_name_for_add_column_notice(catalog, relation_oid);
    let rows = if to_logged {
        catalog.constraint_rows_for_relation(relation_oid)
    } else {
        catalog.foreign_key_constraint_rows_referencing_relation(relation_oid)
    };
    for row in rows
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN)
    {
        let other_relation_oid = if to_logged {
            row.confrelid
        } else {
            row.conrelid
        };
        if other_relation_oid == 0 || other_relation_oid == relation_oid {
            continue;
        }
        let Some(other_relation) = catalog.relation_by_oid(other_relation_oid) else {
            continue;
        };
        let should_error = if to_logged {
            other_relation.relpersistence != 'p'
        } else {
            other_relation.relpersistence == 'p'
        };
        if should_error {
            let other_name = relation_name_for_add_column_notice(catalog, other_relation_oid);
            let target = if to_logged { "logged" } else { "unlogged" };
            let other_kind = if to_logged { "unlogged" } else { "logged" };
            return Err(ExecError::DetailedError {
                message: format!(
                    "could not change table \"{relation_name}\" to {target} because it references {other_kind} table \"{other_name}\""
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }
    Ok(())
}

fn collect_persistence_relation_oids(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    include_table_dependents: bool,
) -> Vec<u32> {
    let mut relation_oids = Vec::new();
    push_unique_relation_oid(&mut relation_oids, relation.relation_oid);
    if include_table_dependents {
        for index in catalog.index_relations_for_heap(relation.relation_oid) {
            push_unique_relation_oid(&mut relation_oids, index.relation_oid);
        }
        if let Some(toast) = relation.toast {
            push_unique_relation_oid(&mut relation_oids, toast.relation_oid);
            for index in catalog.index_relations_for_heap(toast.relation_oid) {
                push_unique_relation_oid(&mut relation_oids, index.relation_oid);
            }
        }
        for depend in
            catalog.depend_rows_referencing(PG_CLASS_RELATION_OID, relation.relation_oid, None)
        {
            if depend.classid == PG_CLASS_RELATION_OID
                && depend.objsubid == 0
                && depend.refobjsubid > 0
                && matches!(depend.deptype, DEPENDENCY_AUTO | DEPENDENCY_INTERNAL)
                && catalog
                    .class_row_by_oid(depend.objid)
                    .is_some_and(|row| row.relkind == 'S')
            {
                push_unique_relation_oid(&mut relation_oids, depend.objid);
            }
        }
        for column in &relation.desc.columns {
            if column.dropped {
                continue;
            }
            if let Some(sequence_oid) = column.default_sequence_oid
                && catalog
                    .class_row_by_oid(sequence_oid)
                    .is_some_and(|row| row.relkind == 'S')
            {
                push_unique_relation_oid(&mut relation_oids, sequence_oid);
            }
        }
    }
    relation_oids
}

fn push_unique_relation_oid(oids: &mut Vec<u32>, relation_oid: u32) {
    if !oids.contains(&relation_oid) {
        oids.push(relation_oid);
    }
}

fn collect_add_column_targets(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    base_column: &crate::backend::executor::ColumnDesc,
    only: bool,
) -> Result<Vec<AddColumnTarget>, ExecError> {
    if only && has_inheritance_children(catalog, relation.relation_oid) {
        return Err(ExecError::Parse(ParseError::InvalidTableDefinition(
            "column must be added to child tables too".into(),
        )));
    }

    let work_queue = build_alter_table_work_queue(catalog, relation, only)?;
    let mut targets = Vec::with_capacity(work_queue.len());

    for item in work_queue {
        let target_relation = item.relation;
        let direct_parent_count = item.expected_parents;
        if let Some((existing_index, existing)) = target_relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, existing)| {
                !existing.dropped && existing.name.eq_ignore_ascii_case(&base_column.name)
            })
        {
            if direct_parent_count == 0
                || existing.sql_type != base_column.sql_type
                || existing.default_expr != base_column.default_expr
                || existing.generated != base_column.generated
            {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "new or merge-compatible inherited column",
                    actual: format!("column already exists: {}", base_column.name),
                }));
            }
            push_notice(format!(
                "merging definition of column \"{}\" for child \"{}\"",
                base_column.name,
                relation_name_for_alter_error(catalog, target_relation.relation_oid)
            ));
            let mut column = existing.clone();
            column.attinhcount = direct_parent_count;
            column.attislocal = existing.attislocal;
            let mut new_desc = target_relation.desc.clone();
            new_desc.columns[existing_index] = column.clone();
            targets.push(AddColumnTarget {
                relation: target_relation,
                column,
                new_desc,
                column_index: existing_index,
                append_column: false,
                rewrite_existing_rows: false,
                direct_parent_count,
            });
        } else {
            let mut column = base_column.clone();
            if direct_parent_count > 0 {
                column.attinhcount = direct_parent_count;
                column.attislocal = false;
                if direct_parent_count > 1 {
                    push_notice(format!(
                        "merging definition of column \"{}\" for child \"{}\"",
                        column.name,
                        relation_name_for_alter_error(catalog, target_relation.relation_oid)
                    ));
                }
            }
            let mut new_desc = target_relation.desc.clone();
            let column_index = new_desc.columns.len();
            new_desc.columns.push(column.clone());
            targets.push(AddColumnTarget {
                relation: target_relation,
                column,
                new_desc,
                column_index,
                append_column: true,
                rewrite_existing_rows: false,
                direct_parent_count,
            });
        }
    }

    Ok(targets)
}

impl Database {
    pub(crate) fn execute_alter_table_set_persistence_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableSetPersistenceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_set_persistence_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_set_persistence_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableSetPersistenceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let table_name = alter_stmt.table_name.to_ascii_lowercase();
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{table_name}" does not exist, skipping"#
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("relation \"{table_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42P01",
            });
        };
        if relation.relkind == 'p' {
            let action = match alter_stmt.persistence {
                TablePersistence::Permanent => "SET LOGGED",
                TablePersistence::Unlogged => "SET UNLOGGED",
                TablePersistence::Temporary => "SET",
            };
            return Err(ExecError::DetailedError {
                message: format!(
                    "ALTER action {action} cannot be performed on relation \"{}\"",
                    table_name
                ),
                detail: Some("This operation is not supported for partitioned tables.".into()),
                hint: None,
                sqlstate: "42809",
            });
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot change logged status of table \"{}\" because it is temporary",
                    relation_name_for_add_column_notice(&catalog, relation.relation_oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        let new_relpersistence = match alter_stmt.persistence {
            TablePersistence::Permanent => 'p',
            TablePersistence::Unlogged => 'u',
            TablePersistence::Temporary => {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "ALTER TABLE SET TEMPORARY".into(),
                )));
            }
        };
        if relation.relpersistence == new_relpersistence {
            return Ok(StatementResult::AffectedRows(0));
        }
        if !matches!(relation.relkind, 'r' | 'S') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: table_name,
                expected: "table or sequence",
            }));
        }
        if relation.relkind == 'r' {
            validate_persistence_foreign_keys(
                &catalog,
                relation.relation_oid,
                new_relpersistence == 'p',
            )?;
        }
        let relation_oids =
            collect_persistence_relation_oids(&catalog, &relation, relation.relkind == 'r');
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        for relation_oid in relation_oids {
            let effect = self
                .catalog
                .write()
                .alter_relation_persistence_mvcc(relation_oid, new_relpersistence, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_set_tablespace_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableSetTablespaceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_set_tablespace_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_set_tablespace_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableSetTablespaceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let relation_name = alter_stmt.table_name.to_ascii_lowercase();
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&relation_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{relation_name}" does not exist, skipping"#
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("relation \"{relation_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42P01",
            });
        };
        if matches!(relation.relkind, 'S' | 'v' | 'c' | 'f') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: relation_name,
                expected: "relation with storage",
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let tablespace_oid = tablespace_oid_by_name(
            self,
            client_id,
            Some((xid, cid)),
            &alter_stmt.tablespace_name,
        )?;
        ensure_non_global_relation_tablespace(&alter_stmt.tablespace_name, tablespace_oid, false)?;
        ensure_tablespace_create_privilege(self, client_id, Some((xid, cid)), tablespace_oid)?;
        let current_tablespace_oid = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .class_by_oid(relation.relation_oid)
            .map(|row| row.reltablespace)
            .unwrap_or(relation.rel.spc_oid);
        if current_tablespace_oid == tablespace_oid {
            return Ok(StatementResult::AffectedRows(0));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .set_relation_tablespace_mvcc(relation.relation_oid, tablespace_oid, true, &ctx)
            .map_err(map_catalog_error)?;
        for old_rel in &effect.dropped_rels {
            self.pool
                .flush_relation(*old_rel)
                .map_err(|err| ExecError::DetailedError {
                    message: format!("could not flush relation before tablespace move: {err:?}"),
                    detail: None,
                    hint: None,
                    sqlstate: "58030",
                })?;
        }
        copy_rewritten_relation_storage(&self.cluster.base_dir, &effect)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        let stats_state = self.session_stats_state(client_id);
        let mut stats = stats_state.write();
        if relation.relpersistence == 't' {
            stats.note_io_write("client backend", "temp relation", "normal", 8192);
            stats.note_io_extend("client backend", "temp relation", "normal", 8192);
        } else {
            stats.note_io_write("client backend", "relation", "normal", 8192);
            stats.note_io_extend("client backend", "relation", "normal", 8192);
            stats.note_io_write("client backend", "wal", "normal", 8192);
            stats.note_io_fsync("client backend", "relation", "normal");
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn effective_analyze_targets_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        analyze_stmt: &AnalyzeStatement,
    ) -> Result<Vec<MaintenanceTarget>, ExecError> {
        let raw_targets = collect_catalog_analyze_targets(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            analyze_stmt,
        )?;
        if analyze_stmt.targets.is_empty() {
            return Ok(raw_targets);
        }
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        validate_maintenance_targets_for_analyze(&raw_targets, &catalog)?;
        let expanded = expand_explicit_analyze_targets(&raw_targets, &catalog)?;
        filter_explicit_analyze_targets_by_permission(self, client_id, txn_ctx, &catalog, &expanded)
    }

    pub(crate) fn effective_vacuum_targets_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        vacuum_stmt: &VacuumStatement,
    ) -> Result<Vec<MaintenanceTarget>, ExecError> {
        collect_catalog_vacuum_targets(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            vacuum_stmt,
        )
    }

    pub fn run_autovacuum_once(&self) -> Result<(), ExecError> {
        let client_id = autovacuum_client_id(self.database_oid);
        let stats_state = Arc::new(RwLock::new(SessionStatsState::default()));
        self.install_auth_state(client_id, AuthState::default());
        self.install_row_security_enabled(client_id, true);
        self.install_session_replication_role(client_id, SessionReplicationRole::Origin);
        self.install_temp_backend_id(client_id, client_id);
        self.install_stats_state(client_id, Arc::clone(&stats_state));

        let result = (|| {
            self.flush_inactive_session_stats();
            let targets = self.autovacuum_targets(client_id)?;
            for target in targets {
                if !self.table_locks.try_lock_table(
                    target.rel,
                    TableLockMode::ShareUpdateExclusive,
                    client_id,
                ) {
                    continue;
                }
                let result = self.execute_autovacuum_target(client_id, &target);
                self.table_locks.unlock_table(target.rel, client_id);
                result?;
            }
            Ok(())
        })();

        self.clear_stats_state(client_id);
        self.clear_temp_backend_id(client_id);
        self.clear_session_replication_role(client_id);
        self.clear_row_security_enabled(client_id);
        self.clear_auth_state(client_id);
        self.clear_interrupt_state(client_id);
        result
    }

    fn flush_inactive_session_stats(&self) {
        let states = self
            .session_stats_states
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for state in states {
            let mut state = state.write();
            if !state.xact_active {
                state.flush_pending(&self.stats);
            }
        }
    }

    fn autovacuum_targets(&self, client_id: ClientId) -> Result<Vec<AutovacuumTarget>, ExecError> {
        let catcache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        let namespace_names = catcache
            .namespace_rows()
            .into_iter()
            .map(|row| (row.oid, row.nspname))
            .collect::<BTreeMap<_, _>>();
        let class_rows = catcache.class_rows();
        let stats = self.stats.read().clone();
        let next_xid = self.txns.read().next_xid();
        let catalog = self.lazy_catalog_lookup(client_id, None, None);

        let mut targets = Vec::new();
        for class in class_rows {
            if class.relkind != 'r' || class.relpersistence != 'p' || class.relispartition {
                continue;
            }
            if !autovacuum_enabled(class.reloptions.as_deref()) {
                continue;
            }
            let Some(namespace_name) = namespace_names.get(&class.relnamespace) else {
                continue;
            };
            if !autovacuum_namespace_allowed(namespace_name) {
                continue;
            }
            let rel_stats = stats.relations.get(&class.oid).cloned().unwrap_or_default();
            let decision = relation_needs_vacanalyze(
                AutovacuumRelationInput {
                    reltuples: class.reltuples,
                    relpages: class.relpages,
                    relallfrozen: class.relallfrozen,
                    relfrozenxid: class.relfrozenxid,
                    next_xid,
                    dead_tuples: rel_stats.dead_tuples,
                    mod_since_analyze: rel_stats.mod_since_analyze,
                    ins_since_vacuum: rel_stats.ins_since_vacuum,
                },
                self.autovacuum_config,
            );
            if !decision.vacuum && !decision.analyze {
                continue;
            }
            let Some(relation) = catalog.relation_by_oid(class.oid) else {
                continue;
            };
            targets.push(AutovacuumTarget {
                relation_oid: class.oid,
                rel: relation.rel,
                vacuum: decision.vacuum,
                analyze: decision.analyze,
            });
        }
        Ok(targets)
    }

    fn execute_autovacuum_target(
        &self,
        client_id: ClientId,
        target: &AutovacuumTarget,
    ) -> Result<(), ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_autovacuum_target_in_transaction(
            client_id,
            target,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result.map(|_| ())
    }

    fn execute_autovacuum_target_in_transaction(
        &self,
        client_id: ClientId,
        target: &AutovacuumTarget,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), None);
        let Some(relation) = catalog.relation_by_oid(target.relation_oid) else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let Some(namespace) = catalog.namespace_row_by_oid(relation.namespace_oid) else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.rel != target.rel
            || relation.relkind != 'r'
            || relation.relpersistence != 'p'
            || relation.relispartition
            || !autovacuum_namespace_allowed(&namespace.nspname)
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        let relations = [relation];
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: super::maintenance_safe_gucs(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            write_xid_override: None,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            timed: false,
            allow_side_effects: false,
            security_restricted: false,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: Vec::new(),
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
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
        if !ctx.session_stats.read().xact_active {
            ctx.session_stats.write().flush_pending(&ctx.stats);
        }
        let vacuum_started = Instant::now();
        let vacuumed = if target.vacuum {
            crate::backend::commands::tablecmds::collect_vacuum_stats_for_relations(
                &relations, &catalog, &mut ctx,
            )?
        } else {
            Vec::new()
        };
        let vacuum_elapsed = vacuum_started.elapsed();
        let analyzed = if target.analyze {
            let analyze_started = Instant::now();
            crate::backend::commands::analyze::collect_analyze_stats_for_relations(
                &relations, &catalog, &mut ctx,
            )?
            .into_iter()
            .map(|result| (result, analyze_started.elapsed()))
            .collect()
        } else {
            Vec::new()
        };
        let session_stats = Arc::clone(&ctx.session_stats);
        drop(ctx);

        let write_ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: Arc::clone(&interrupts),
        };
        let mut store = self.catalog.write();
        let mut analyzed_by_oid = analyzed
            .into_iter()
            .map(|(result, elapsed)| (result.relation_oid, (result, elapsed)))
            .collect::<BTreeMap<_, _>>();
        for result in vacuumed {
            self.stats.write().report_relation_vacuum(
                result.relation_oid,
                true,
                vacuum_elapsed,
                result.removed_dead_tuples,
                result.remaining_dead_tuples,
            );
            let effect = if let Some((analyze_result, analyze_elapsed)) =
                analyzed_by_oid.remove(&result.relation_oid)
            {
                let effect = store
                    .set_relation_maintenance_stats_mvcc(
                        result.relation_oid,
                        analyze_result.relpages,
                        analyze_result.reltuples,
                        result.relallvisible,
                        result.relallfrozen,
                        result.relfrozenxid,
                        analyze_result.clear_relhassubclass,
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?;
                catalog_effects.push(effect);
                let effect = store
                    .replace_relation_statistics_mvcc(
                        analyze_result.relation_oid,
                        analyze_result.statistics.clone(),
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?;
                catalog_effects.push(effect);
                replace_statistics_ext_data_rows_for_analyze(
                    &mut store,
                    &analyze_result.statistics_ext_data,
                    &write_ctx,
                    catalog_effects,
                )?;
                session_stats.write().report_relation_analyze(
                    &self.stats,
                    analyze_result.relation_oid,
                    true,
                    analyze_elapsed,
                    analyze_result.reltuples,
                );
                continue;
            } else {
                store
                    .set_relation_vacuum_stats_mvcc(
                        result.relation_oid,
                        result.relpages,
                        result.relallvisible,
                        result.relallfrozen,
                        result.relfrozenxid,
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?
            };
            catalog_effects.push(effect);
        }
        for (result, analyze_elapsed) in analyzed_by_oid.into_values() {
            let effect = store
                .set_relation_analyze_stats_mvcc(
                    result.relation_oid,
                    result.relpages,
                    result.reltuples,
                    result.clear_relhassubclass,
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            let effect = store
                .replace_relation_statistics_mvcc(
                    result.relation_oid,
                    result.statistics.clone(),
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            replace_statistics_ext_data_rows_for_analyze(
                &mut store,
                &result.statistics_ext_data,
                &write_ctx,
                catalog_effects,
            )?;
            session_stats.write().report_relation_analyze(
                &self.stats,
                result.relation_oid,
                true,
                analyze_elapsed,
                result.reltuples,
            );
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, _, _) = self.normalize_domain_name_for_create(
            client_id,
            &comment_stmt.domain_name,
            configured_search_path,
        )?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&normalized) else {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                comment_stmt.domain_name.clone(),
            )));
        };
        domain.comment = comment_stmt.comment.clone();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_aggregate_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnAggregateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_function_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnFunctionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_function_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_operator_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnOperatorStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_operator_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_type_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnIndexStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_index_relation_for_comment(&catalog, &comment_stmt.index_name)?;
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_index_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnAggregateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let arg_oids = aggregate_signature_arg_oids(&catalog, &comment_stmt.signature)
            .map_err(ExecError::Parse)?;
        let schema_oid = match &comment_stmt.schema_name {
            Some(schema_name) => Some(
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?,
            ),
            None => None,
        };
        let matches = resolve_aggregate_proc_rows(
            &catalog,
            &comment_stmt.aggregate_name,
            schema_oid,
            &arg_oids,
        );
        let proc_row = match matches.as_slice() {
            [(row, _agg)] => row.clone(),
            [] => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "aggregate {} does not exist",
                        format_aggregate_signature(
                            &comment_stmt.aggregate_name,
                            &comment_stmt.signature,
                            &catalog
                        )?
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "aggregate name {} is ambiguous",
                        comment_stmt.aggregate_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42725",
                });
            }
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_proc_mvcc(proc_row.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_function_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let function_name = match &comment_stmt.schema_name {
            Some(schema_name) => format!("{schema_name}.{}", comment_stmt.function_name),
            None => comment_stmt.function_name.clone(),
        };
        let proc_row = resolve_exact_function_row(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            &function_name,
            &comment_stmt.arg_types,
        )?;
        ensure_function_owner(self, client_id, proc_row.proowner, &function_name, txn_ctx)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_proc_mvcc(proc_row.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_operator_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnOperatorStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if comment_stmt.right_arg.is_none() {
            return Err(unsupported_postfix_operator_error());
        }
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let left_type = resolve_operator_type_oid(&catalog, &comment_stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &comment_stmt.right_arg)?;
        let namespace_oid = comment_stmt
            .schema_name
            .as_deref()
            .map(|schema_name| {
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })
            })
            .transpose()?;
        let operator = lookup_operator_row(
            self,
            client_id,
            txn_ctx,
            namespace_oid,
            &comment_stmt.operator_name,
            left_type,
            right_type,
        )?
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "operator does not exist: {}",
                operator_signature_display(
                    &catalog,
                    &comment_stmt.operator_name,
                    left_type,
                    right_type
                )
            ),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
        if catalog.current_user_oid() != BOOTSTRAP_SUPERUSER_OID
            && catalog.current_user_oid() != operator.oprowner
        {
            return Err(ExecError::DetailedError {
                message: format!("must be owner of operator {}", comment_stmt.operator_name),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_operator_mvcc(operator.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_index_relation_for_comment(&catalog, &comment_stmt.index_name)?;
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.index_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = match catalog.lookup_any_relation(&comment_stmt.table_name) {
            Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f') => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: comment_stmt.table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::DetailedError {
                    message: format!("relation \"{}\" does not exist", comment_stmt.table_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                });
            }
        };
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_table_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_comment_on_sequence_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnSequenceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_sequence_relation_for_comment(&catalog, &comment_stmt.sequence_name)?;
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_sequence_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_comment_on_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_column_comment_relation(&catalog, &comment_stmt.table_name)?;
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_column_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_comment_on_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = match catalog.lookup_any_relation(&comment_stmt.view_name) {
            Some(entry) if entry.relkind == 'v' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: comment_stmt.view_name.clone(),
                    expected: "view",
                }));
            }
            None => {
                return Err(ExecError::DetailedError {
                    message: format!("relation \"{}\" does not exist", comment_stmt.view_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                });
            }
        };
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_view_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_comment_on_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let lock_tag = if comment_stmt.domain_name.is_none() {
            let interrupts = self.interrupt_state(client_id);
            let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            let relation = lookup_table_or_partitioned_relation_for_comment(
                &catalog,
                &comment_stmt.table_name,
            )?;
            let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
            self.table_locks.lock_table_interruptible(
                lock_tag,
                TableLockMode::AccessExclusive,
                client_id,
                interrupts.as_ref(),
            )?;
            Some(lock_tag)
        } else {
            None
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_constraint_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        if let Some(lock_tag) = lock_tag {
            self.table_locks.unlock_table(lock_tag, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_add_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let lock_queue = build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?;
        let lock_requests = lock_queue
            .iter()
            .map(|item| (item.relation.rel, TableLockMode::AccessExclusive))
            .collect::<Vec<_>>();
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            interrupts.as_ref(),
        )?;
        let locked_rels =
            crate::pgrust::database::foreign_keys::table_lock_relations(&lock_requests);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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
        for rel in locked_rels {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_add_columns_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let lock_queue = build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?;
        let lock_requests = lock_queue
            .iter()
            .map(|item| (item.relation.rel, TableLockMode::AccessExclusive))
            .collect::<Vec<_>>();
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            interrupts.as_ref(),
        )?;
        let locked_rels =
            crate::pgrust::database::foreign_keys::table_lock_relations(&lock_requests);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_alter_table_add_columns_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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
        for rel in locked_rels {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_analyze_stmt_with_search_path(
        &self,
        client_id: ClientId,
        analyze_stmt: &AnalyzeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let targets = self.effective_analyze_targets_with_search_path(
            client_id,
            None,
            configured_search_path,
            analyze_stmt,
        )?;
        let relation_names = targets
            .iter()
            .map(|target| target.table_name.clone())
            .collect::<Vec<_>>();
        let rels = relation_names
            .iter()
            .map(|name| lookup_analyzable_relation_for_ddl(&catalog, name))
            .collect::<Result<Vec<_>, _>>()?;
        let rel_locs = rels.iter().map(|rel| rel.rel).collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rel_locs,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_analyze_stmt_in_transaction_with_search_path(
            client_id,
            &targets,
            xid,
            0,
            configured_search_path,
            false,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for rel in rel_locs {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_vacuum_stmt_with_search_path(
        &self,
        client_id: ClientId,
        vacuum_stmt: &VacuumStatement,
        configured_search_path: Option<&[String]>,
        gucs: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<StatementResult, ExecError> {
        let options = vacuum_exec_options(vacuum_stmt, gucs)?;
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let raw_targets = self.effective_vacuum_targets_with_search_path(
            client_id,
            None,
            configured_search_path,
            vacuum_stmt,
        )?;
        validate_maintenance_targets_for_vacuum(&raw_targets, &catalog, options.analyze)?;
        let targets = if vacuum_stmt.targets.is_empty() {
            raw_targets
        } else {
            let expanded = expand_explicit_maintenance_targets(&raw_targets, &catalog)?;
            filter_explicit_vacuum_targets_by_permission(
                self, client_id, None, &catalog, &expanded, &options,
            )?
        };
        warn_parallel_vacuum_temp_tables(&catalog, &targets, &options);
        let relation_names = targets
            .iter()
            .map(|target| target.table_name.clone())
            .collect::<Vec<_>>();
        let rels = relation_names
            .iter()
            .map(|name| lookup_vacuum_relation_for_ddl(&catalog, name))
            .collect::<Result<Vec<_>, _>>()?;
        let rel_locs = rels.iter().map(|rel| rel.rel).collect::<Vec<_>>();
        let lock_mode = if options.full {
            TableLockMode::AccessExclusive
        } else {
            TableLockMode::ShareUpdateExclusive
        };
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rel_locs,
            lock_mode,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_vacuum_stmt_in_transaction_with_search_path(
            client_id,
            &targets,
            &options,
            xid,
            0,
            configured_search_path,
            false,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for rel in rel_locs {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_analyze_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        targets: &[MaintenanceTarget],
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        auto: bool,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: super::maintenance_safe_gucs(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            write_xid_override: None,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            timed: false,
            allow_side_effects: false,
            security_restricted: false,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: Vec::new(),
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
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
        if !ctx.session_stats.read().xact_active {
            ctx.session_stats.write().flush_pending(&ctx.stats);
        }
        let analyze_started = Instant::now();
        let analyzed = collect_analyze_stats(targets, &catalog, &mut ctx)?;
        let analyze_elapsed = analyze_started.elapsed();
        let session_stats = Arc::clone(&ctx.session_stats);
        drop(ctx);

        let write_ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: Arc::clone(&interrupts),
        };
        let mut store = self.catalog.write();
        for result in analyzed {
            let effect = store
                .set_relation_analyze_stats_mvcc(
                    result.relation_oid,
                    result.relpages,
                    result.reltuples,
                    result.clear_relhassubclass,
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            let effect = store
                .replace_relation_statistics_mvcc(
                    result.relation_oid,
                    result.statistics.clone(),
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            replace_statistics_ext_data_rows_for_analyze(
                &mut store,
                &result.statistics_ext_data,
                &write_ctx,
                catalog_effects,
            )?;
            session_stats.write().report_relation_analyze(
                &self.stats,
                result.relation_oid,
                auto,
                analyze_elapsed,
                result.reltuples,
            );
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_vacuum_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        targets: &[MaintenanceTarget],
        options: &VacuumExecOptions,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        auto: bool,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: super::maintenance_safe_gucs(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            write_xid_override: None,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            timed: false,
            allow_side_effects: false,
            security_restricted: false,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: Vec::new(),
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
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
        if !ctx.session_stats.read().xact_active {
            ctx.session_stats.write().flush_pending(&ctx.stats);
        }
        let vacuum_started = Instant::now();
        let vacuumed = if options.only_database_stats {
            Vec::new()
        } else if options.full {
            self.execute_vacuum_full_targets_with_search_path(
                client_id,
                targets,
                xid,
                cid,
                configured_search_path,
                options.process_main,
                &mut ctx,
                catalog_effects,
            )?
        } else if !options.process_main {
            crate::backend::commands::tablecmds::collect_vacuum_stats_with_options(
                targets,
                &catalog,
                &mut ctx,
                false,
                options.process_toast,
                options.index_cleanup,
                options.truncate,
                options.default_truncate,
            )?
        } else {
            crate::backend::commands::tablecmds::collect_vacuum_stats_with_options(
                targets,
                &catalog,
                &mut ctx,
                true,
                options.process_toast,
                options.index_cleanup,
                options.truncate,
                options.default_truncate,
            )?
        };
        let vacuum_elapsed = vacuum_started.elapsed();
        let analyzed = if options.analyze {
            let analyze_started = Instant::now();
            let analyze_results = if options.full {
                let analyze_cid = cid.saturating_add(1);
                ctx.snapshot = self.txns.read().snapshot_for_command(xid, analyze_cid)?;
                ctx.next_command_id = analyze_cid;
                let analyze_catalog = self.lazy_catalog_lookup(
                    client_id,
                    Some((xid, analyze_cid)),
                    configured_search_path,
                );
                ctx.catalog = Some(crate::backend::executor::executor_catalog(
                    analyze_catalog.clone(),
                ));
                crate::backend::commands::analyze::collect_analyze_stats(
                    targets,
                    &analyze_catalog,
                    &mut ctx,
                )?
            } else {
                crate::backend::commands::analyze::collect_analyze_stats(
                    targets, &catalog, &mut ctx,
                )?
            };
            analyze_results
                .into_iter()
                .map(|result| (result, analyze_started.elapsed()))
                .collect()
        } else {
            Vec::new()
        };
        let session_stats = Arc::clone(&ctx.session_stats);
        drop(ctx);

        let stats_cid = if options.full {
            cid.saturating_add(1)
        } else {
            cid
        };
        let write_ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            xid,
            cid: stats_cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: Arc::clone(&interrupts),
        };
        let mut store = self.catalog.write();
        let mut analyzed_by_oid = analyzed
            .into_iter()
            .map(|(result, elapsed)| (result.relation_oid, (result, elapsed)))
            .collect::<BTreeMap<_, _>>();
        for result in vacuumed {
            self.stats.write().report_relation_vacuum(
                result.relation_oid,
                auto,
                vacuum_elapsed,
                result.removed_dead_tuples,
                result.remaining_dead_tuples,
            );
            let effect = if let Some((analyze_result, analyze_elapsed)) =
                analyzed_by_oid.remove(&result.relation_oid)
            {
                let effect = store
                    .set_relation_maintenance_stats_mvcc(
                        result.relation_oid,
                        analyze_result.relpages,
                        analyze_result.reltuples,
                        result.relallvisible,
                        result.relallfrozen,
                        result.relfrozenxid,
                        analyze_result.clear_relhassubclass,
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?;
                catalog_effects.push(effect);
                let effect = store
                    .replace_relation_statistics_mvcc(
                        analyze_result.relation_oid,
                        analyze_result.statistics.clone(),
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?;
                catalog_effects.push(effect);
                replace_statistics_ext_data_rows_for_analyze(
                    &mut store,
                    &analyze_result.statistics_ext_data,
                    &write_ctx,
                    catalog_effects,
                )?;
                session_stats.write().report_relation_analyze(
                    &self.stats,
                    analyze_result.relation_oid,
                    auto,
                    analyze_elapsed,
                    analyze_result.reltuples,
                );
                continue;
            } else {
                store
                    .set_relation_vacuum_stats_mvcc(
                        result.relation_oid,
                        result.relpages,
                        result.relallvisible,
                        result.relallfrozen,
                        result.relfrozenxid,
                        &write_ctx,
                    )
                    .map_err(ExecError::from)?
            };
            catalog_effects.push(effect);
        }
        for (result, analyze_elapsed) in analyzed_by_oid.into_values() {
            let effect = store
                .set_relation_analyze_stats_mvcc(
                    result.relation_oid,
                    result.relpages,
                    result.reltuples,
                    result.clear_relhassubclass,
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            let effect = store
                .replace_relation_statistics_mvcc(
                    result.relation_oid,
                    result.statistics.clone(),
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            replace_statistics_ext_data_rows_for_analyze(
                &mut store,
                &result.statistics_ext_data,
                &write_ctx,
                catalog_effects,
            )?;
            session_stats.write().report_relation_analyze(
                &self.stats,
                result.relation_oid,
                auto,
                analyze_elapsed,
                result.reltuples,
            );
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_type_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let type_row = catalog
            .type_by_name(&comment_stmt.type_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("type \"{}\" does not exist", comment_stmt.type_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_type_mvcc(type_row.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation =
            lookup_table_or_partitioned_relation_for_comment(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON TABLE",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.table_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_sequence_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnSequenceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_sequence_relation_for_comment(&catalog, &comment_stmt.sequence_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent sequence for COMMENT ON SEQUENCE",
                actual: "temporary sequence".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.sequence_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_column_comment_relation(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON COLUMN",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.table_name)?;
        let column_index = relation
            .desc
            .columns
            .iter()
            .position(|column| {
                !column.dropped && column.name.eq_ignore_ascii_case(&comment_stmt.column_name)
            })
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    comment_stmt.column_name, comment_stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            })?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_column_mvcc(
                relation.relation_oid,
                i32::from(user_attrno(column_index)),
                comment_stmt.comment.as_deref(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match catalog.lookup_any_relation(&comment_stmt.view_name) {
            Some(entry) if entry.relkind == 'v' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: comment_stmt.view_name.clone(),
                    expected: "view",
                }));
            }
            None => {
                return Err(ExecError::DetailedError {
                    message: format!("relation \"{}\" does not exist", comment_stmt.view_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                });
            }
        };
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent view for COMMENT ON VIEW",
                actual: "temporary view".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.view_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        if let Some(domain_name) = &comment_stmt.domain_name {
            let (normalized, _, _) = self.normalize_domain_name_for_create(
                client_id,
                domain_name,
                configured_search_path,
            )?;
            let domain = self
                .domains
                .read()
                .get(&normalized)
                .cloned()
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(domain_name.clone()))
                })?;
            let constraint_oid = domain
                .constraints
                .iter()
                .find(|constraint| {
                    constraint
                        .name
                        .eq_ignore_ascii_case(&comment_stmt.constraint_name)
                })
                .map(|constraint| constraint.oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "constraint \"{}\" for domain \"{}\" does not exist",
                        comment_stmt.constraint_name, domain.name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .comment_constraint_mvcc(constraint_oid, comment_stmt.comment.as_deref(), &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            return Ok(StatementResult::AffectedRows(0));
        }
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation =
            lookup_table_or_partitioned_relation_for_comment(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON CONSTRAINT",
                actual: "temporary table".into(),
            }));
        }
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for COMMENT ON CONSTRAINT",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let row = find_constraint_row(&rows, &comment_stmt.constraint_name).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing table constraint",
                actual: format!(
                    "constraint \"{}\" for table \"{}\" does not exist",
                    comment_stmt.constraint_name,
                    relation_basename(&comment_stmt.table_name).to_ascii_lowercase()
                ),
            })
        })?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_constraint_mvcc(row.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_plain_alter_table_add_columns_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnsStatement,
        relation: &BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        reject_typed_table_ddl(relation, "add column to")?;
        ensure_relation_owner(self, client_id, relation, &alter_stmt.table_name)?;
        if relation.relispartition {
            return Err(ExecError::DetailedError {
                message: "cannot add column to a partition".into(),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        let _ = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;

        let relation_display_name = relation_basename(&alter_stmt.table_name);
        let mut planned_names = BTreeSet::new();
        let mut active_adds = Vec::new();
        for add_stmt in &alter_stmt.columns {
            let column_name = add_stmt.column.name.to_ascii_lowercase();
            let already_exists = relation.desc.columns.iter().any(|existing| {
                !existing.dropped && existing.name.eq_ignore_ascii_case(&add_stmt.column.name)
            }) || planned_names.contains(&column_name);
            if already_exists && add_stmt.missing_ok {
                push_notice(format!(
                    "column \"{}\" of relation \"{}\" already exists, skipping",
                    add_stmt.column.name, relation_display_name
                ));
                continue;
            }
            planned_names.insert(column_name);
            active_adds.push(add_stmt);
        }
        if active_adds.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let work_queue = build_alter_table_work_queue(&catalog, relation, alter_stmt.only)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        for item in work_queue {
            let mut target_relation = item.relation;
            let mut target_desc = target_relation.desc.clone();
            let mut target_columns = Vec::new();
            let table_name =
                relation_name_for_add_column_notice(&catalog, target_relation.relation_oid);
            let existing_constraints =
                catalog.constraint_rows_for_relation(target_relation.relation_oid);
            for add_stmt in &active_adds {
                let plan = validate_alter_table_add_column(
                    &table_name,
                    &target_desc,
                    &add_stmt.column,
                    &existing_constraints,
                    &catalog,
                )?;
                let crate::pgrust::database::ddl::AlterTableAddColumnPlan {
                    mut column,
                    owned_sequence,
                    not_null_action,
                    check_actions,
                } = plan;
                debug_assert!(owned_sequence.is_none());
                debug_assert!(not_null_action.is_none());
                debug_assert!(check_actions.is_empty());
                self.ensure_sql_type_usage_privilege(
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                    column.sql_type,
                )?;
                if item.expected_parents > 0 {
                    column.attinhcount = item.expected_parents;
                    column.attislocal = false;
                }
                target_desc.columns.push(column.clone());
                target_columns.push(column);
            }

            let effect = self
                .catalog
                .write()
                .alter_table_add_columns_mvcc(target_relation.relation_oid, target_columns, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            target_relation.desc = target_desc.clone();
            for column_index in target_relation
                .desc
                .columns
                .len()
                .saturating_sub(active_adds.len())
                ..target_relation.desc.columns.len()
            {
                validate_added_column_domain_constraints(
                    self,
                    &target_relation,
                    column_index,
                    catalog.clone(),
                    client_id,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                )?;
            }
            let (toast_namespace_oid, toast_namespace_name) =
                if target_relation.relpersistence == 't' {
                    let temp_backend_id = self.temp_backend_id(client_id);
                    (
                        Self::temp_toast_namespace_oid(temp_backend_id),
                        Self::temp_toast_namespace_name(temp_backend_id),
                    )
                } else {
                    (
                        PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE.to_string(),
                    )
                };
            if target_relation.relkind != 'f'
                && let Some(effect) = self
                    .catalog
                    .write()
                    .ensure_relation_toast_table_mvcc(
                        target_relation.relation_oid,
                        toast_namespace_oid,
                        &toast_namespace_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            {
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            if target_relation.relpersistence == 't' {
                self.replace_temp_entry_desc(client_id, target_relation.relation_oid, target_desc)?;
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_columns_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        preflight_alter_table_add_columns(&catalog, &relation, alter_stmt)?;
        if alter_stmt
            .columns
            .iter()
            .all(|add_stmt| add_column_is_plain_batch_compatible(add_stmt))
            && (relation.relkind == 'p'
                || has_inheritance_children(&catalog, relation.relation_oid))
        {
            return self
                .execute_plain_alter_table_add_columns_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    &relation,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                );
        }
        for add_stmt in &alter_stmt.columns {
            self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                client_id,
                add_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        reject_typed_table_ddl(&relation, "add column to")?;
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        if relation.relispartition {
            return Err(ExecError::DetailedError {
                message: "cannot add column to a partition".into(),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        if relation.desc.columns.iter().any(|existing| {
            !existing.dropped && existing.name.eq_ignore_ascii_case(&alter_stmt.column.name)
        }) {
            if alter_stmt.missing_ok {
                push_notice(format!(
                    "column \"{}\" of relation \"{}\" already exists, skipping",
                    alter_stmt.column.name,
                    relation_basename(&alter_stmt.table_name)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(duplicate_add_column_error(&alter_stmt.column.name));
        }
        let _ = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;
        reject_recursive_composite_column_type(&catalog, &relation, &alter_stmt.column)?;
        reject_composite_rowtype_non_null_default_dependency(
            &catalog,
            &relation,
            &alter_stmt.column,
        )?;
        let table_name = relation_basename(&alter_stmt.table_name).to_string();
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let plan = validate_alter_table_add_column(
            &table_name,
            &relation.desc,
            &alter_stmt.column,
            &existing_constraints,
            &catalog,
        )?;
        let crate::pgrust::database::ddl::AlterTableAddColumnPlan {
            mut column,
            owned_sequence,
            not_null_action,
            check_actions,
        } = plan;
        self.ensure_sql_type_usage_privilege(
            client_id,
            Some((xid, cid)),
            configured_search_path,
            column.sql_type,
        )?;
        if owned_sequence.as_ref().is_some_and(|sequence| {
            sequence.kind == crate::backend::parser::OwnedSequenceKind::Identity
        }) && relation.relkind != 'p'
            && has_inheritance_children(&catalog, relation.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: "cannot recursively add identity column to table that has child tables"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if let Some(fdw_options) = &alter_stmt.fdw_options {
            column.fdw_options = format_fdw_options(fdw_options)?;
        }
        let mut created_owned_sequence = None;
        if let Some(serial_column) = owned_sequence.as_ref() {
            let mut used_names = std::collections::BTreeSet::new();
            let created = self.create_owned_sequence_for_serial_column(
                client_id,
                &alter_stmt.table_name,
                relation.namespace_oid,
                match relation.relpersistence {
                    't' => TablePersistence::Temporary,
                    'u' => TablePersistence::Unlogged,
                    _ => TablePersistence::Permanent,
                },
                serial_column,
                xid,
                cid,
                &mut used_names,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?;
            column.default_expr = Some(format_nextval_default_oid(
                created.sequence_oid,
                serial_column.sql_type,
            ));
            column.default_sequence_oid = Some(created.sequence_oid);
            created_owned_sequence = Some(created);
        }
        let mut targets =
            collect_add_column_targets(&catalog, &relation, &column, alter_stmt.only)?;
        let indexes = targets
            .iter()
            .map(|target| {
                (
                    target.relation.relation_oid,
                    catalog.index_relations_for_heap(target.relation.relation_oid),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut default_ctx = None;
        for target in &mut targets {
            if target.relation.relkind == 'f'
                || !target.append_column
                || target.column.default_sequence_oid.is_some()
                || added_column_default_sql(&target.column, &catalog).is_none()
            {
                continue;
            }
            let ctx = match default_ctx.as_mut() {
                Some(ctx) => ctx,
                None => {
                    let mut ctx = add_column_validation_executor_context(
                        self,
                        catalog.clone(),
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                    ctx.allow_side_effects = true;
                    default_ctx.insert(ctx)
                }
            };
            if added_column_default_is_volatile(&target.column, &catalog)? {
                self.fire_table_rewrite_event_in_executor_context(
                    ctx,
                    "ALTER TABLE",
                    target.relation.relation_oid,
                    2,
                )?;
                if relkind_has_storage(target.relation.relkind) {
                    rewrite_heap_rows_for_added_default_column(
                        &target.relation,
                        &target.new_desc,
                        target.column_index,
                        indexes
                            .get(&target.relation.relation_oid)
                            .expect("indexes for add-column target"),
                        ctx,
                        xid,
                        cid,
                    )?;
                }
                target.rewrite_existing_rows = true;
                target.column.missing_default_value = None;
                for column in &mut target.new_desc.columns {
                    column.missing_default_value = None;
                }
            } else {
                let value = evaluate_added_column_default_value(
                    &target.new_desc,
                    target.column_index,
                    ctx,
                )?;
                if !matches!(value, Value::Null) {
                    target.column.missing_default_value = Some(value.clone());
                    target.new_desc.columns[target.column_index].missing_default_value =
                        Some(value);
                }
            }
        }
        if let Some(sequence_oid) = column.default_sequence_oid {
            let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&self.pool),
                data_dir: None,
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                lock_status_provider: Some(Arc::new(self.clone())),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                stats_import_runtime: None,
                async_notify_runtime: Some(self.async_notify_runtime.clone()),
                advisory_locks: Arc::clone(&self.advisory_locks),
                row_locks: Arc::clone(&self.row_locks),
                checkpoint_stats: self.checkpoint_stats_snapshot(),
                datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(
                ),
                statement_timestamp_usecs:
                    crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: super::maintenance_safe_gucs(),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                snapshot,
                write_xid_override: None,
                transaction_state: None,
                client_id,
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                active_role_oid: self.auth_state(client_id).active_role_oid(),
                session_replication_role: self.session_replication_role(client_id),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
                next_command_id: cid,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
                random_state: crate::backend::executor::PgPrngState::shared(),
                timed: false,
                allow_side_effects: true,
                security_restricted: false,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                active_grouping_refs: Vec::new(),
                subplans: Vec::new(),
                pending_async_notifications: Vec::new(),
                catalog_effects: Vec::new(),
                temp_effects: Vec::new(),
                database: Some(self.clone()),
                pending_catalog_effects: Vec::new(),
                pending_table_locks: Vec::new(),
                pending_portals: Vec::new(),
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
            for target in &targets {
                if target.relation.relkind == 'f' || !target.append_column {
                    continue;
                }
                rewrite_heap_rows_for_added_serial_column(
                    self,
                    &target.relation,
                    &target.new_desc,
                    indexes
                        .get(&target.relation.relation_oid)
                        .expect("indexes for add-column target"),
                    sequence_oid,
                    &mut ctx,
                    xid,
                    cid,
                )?;
            }
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        for target in &targets {
            let effect = if target.append_column {
                self.catalog
                    .write()
                    .alter_table_add_column_mvcc(
                        target.relation.relation_oid,
                        target.column.clone(),
                        target.rewrite_existing_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            } else {
                self.catalog
                    .write()
                    .update_relation_column_inheritance_mvcc(
                        target.relation.relation_oid,
                        &target.column.name,
                        target.column.attinhcount,
                        target.column.attislocal,
                        None,
                        None,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            };
            for old_rel in &effect.dropped_rels {
                self.pool
                    .flush_relation(*old_rel)
                    .map_err(|err| ExecError::DetailedError {
                        message: format!("could not flush relation before table rewrite: {err:?}"),
                        detail: None,
                        hint: None,
                        sqlstate: "58030",
                    })?;
            }
            copy_rewritten_relation_storage(&self.cluster.base_dir, &effect)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            if target.relation.relation_oid == relation.relation_oid
                && let Some(created_sequence) = created_owned_sequence.as_ref()
            {
                let sequence_dependency_ctx = CatalogWriteContext {
                    pool: ctx.pool.clone(),
                    txns: ctx.txns.clone(),
                    xid: ctx.xid,
                    cid: cid.saturating_add(1),
                    client_id: ctx.client_id,
                    waiter: ctx.waiter.clone(),
                    interrupts: ctx.interrupts.clone(),
                };
                let effect = self
                    .catalog
                    .write()
                    .set_sequence_owned_by_dependency_mvcc(
                        created_sequence.sequence_oid,
                        Some((
                            target.relation.relation_oid,
                            created_sequence.column_index.saturating_add(1) as i32,
                        )),
                        owned_sequence_dependency_kind(created_sequence.kind),
                        &sequence_dependency_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            if !target.append_column {
                if target.relation.relpersistence == 't' {
                    self.replace_temp_entry_desc(
                        client_id,
                        target.relation.relation_oid,
                        target.new_desc.clone(),
                    )?;
                }
                continue;
            }
            let (toast_namespace_oid, toast_namespace_name) =
                if target.relation.relpersistence == 't' {
                    let temp_backend_id = self.temp_backend_id(client_id);
                    (
                        Self::temp_toast_namespace_oid(temp_backend_id),
                        Self::temp_toast_namespace_name(temp_backend_id),
                    )
                } else {
                    (
                        PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE.to_string(),
                    )
                };
            let toast_ctx = CatalogWriteContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                xid: ctx.xid,
                cid: cid.saturating_add(1),
                client_id: ctx.client_id,
                waiter: ctx.waiter.clone(),
                interrupts: ctx.interrupts.clone(),
            };
            if target.relation.relkind != 'f' {
                if let Some(effect) = self
                    .catalog
                    .write()
                    .ensure_relation_toast_table_mvcc(
                        target.relation.relation_oid,
                        toast_namespace_oid,
                        &toast_namespace_name,
                        &toast_ctx,
                    )
                    .map_err(map_catalog_error)?
                {
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                }
            }
            if target.relation.relpersistence == 't' {
                self.replace_temp_entry_desc(
                    client_id,
                    target.relation.relation_oid,
                    target.new_desc.clone(),
                )?;
            }
        }
        for target in targets {
            let mut target_desc = target.new_desc.clone();
            let mut target_relation = target.relation.clone();
            target_relation.desc = target_desc.clone();
            let target_name =
                relation_name_for_add_column_notice(&catalog, target.relation.relation_oid);
            let new_column_index = target_desc
                .columns
                .get(target.column_index)
                .map(|_| target.column_index)
                .expect("add-column target column present");

            if let Some(action) = not_null_action.as_ref() {
                if !(target.direct_parent_count > 0 && action.no_inherit) {
                    if !action.not_valid {
                        validate_not_null_rows(
                            self,
                            &target_relation,
                            &target_name,
                            new_column_index,
                            &action.constraint_name,
                            &catalog,
                            client_id,
                            xid,
                            cid,
                            std::sync::Arc::clone(&interrupts),
                        )?;
                    }
                    let set_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: cid
                            .saturating_add(1)
                            .saturating_add(catalog_effects.len() as u32),
                        client_id,
                        waiter: None,
                        interrupts: std::sync::Arc::clone(&interrupts),
                    };
                    let (constraint_oid, effect) = self
                        .catalog
                        .write()
                        .set_column_not_null_mvcc(
                            target.relation.relation_oid,
                            &action.column,
                            action.constraint_name.clone(),
                            !action.not_valid,
                            action.no_inherit,
                            false,
                            &set_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    let inherited_not_null = target.direct_parent_count > 0;
                    let had_existing_not_null = !target.column.storage.nullable;
                    let not_null_is_local = if inherited_not_null {
                        had_existing_not_null && target.column.not_null_constraint_is_local
                    } else {
                        true
                    };
                    let not_null_inhcount = if inherited_not_null {
                        target
                            .column
                            .not_null_constraint_inhcount
                            .saturating_add(target.direct_parent_count)
                    } else {
                        0
                    };
                    let not_null_no_inherit = if inherited_not_null {
                        false
                    } else {
                        action.no_inherit
                    };
                    if inherited_not_null {
                        let inherit_ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: cid
                                .saturating_add(1)
                                .saturating_add(catalog_effects.len() as u32),
                            client_id,
                            waiter: None,
                            interrupts: std::sync::Arc::clone(&interrupts),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .alter_not_null_constraint_state_by_attnum_mvcc(
                                target.relation.relation_oid,
                                (new_column_index + 1) as i16,
                                constraint_oid,
                                &action.constraint_name,
                                None,
                                Some(false),
                                Some(not_null_is_local),
                                Some(not_null_inhcount),
                                &inherit_ctx,
                            )
                            .map_err(map_catalog_error)?;
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                    }
                    let column = target_desc
                        .columns
                        .get_mut(new_column_index)
                        .expect("new column present in target desc");
                    column.storage.nullable = false;
                    column.not_null_constraint_oid = Some(constraint_oid);
                    column.not_null_constraint_name = Some(action.constraint_name.clone());
                    column.not_null_constraint_validated = !action.not_valid;
                    column.not_null_constraint_no_inherit = not_null_no_inherit;
                    column.not_null_constraint_is_local = not_null_is_local;
                    column.not_null_constraint_inhcount = not_null_inhcount;
                    column.not_null_primary_key_owned = false;
                    target_relation.desc = target_desc.clone();
                }
            }

            validate_added_column_domain_constraints(
                self,
                &target_relation,
                new_column_index,
                catalog.clone(),
                client_id,
                xid,
                cid,
                std::sync::Arc::clone(&interrupts),
            )?;

            for action in &check_actions {
                if target.direct_parent_count > 1 && !action.no_inherit {
                    push_notice(format!(
                        "merging constraint \"{}\" with inherited definition",
                        action.constraint_name
                    ));
                }
                crate::backend::parser::bind_check_constraint_expr(
                    &action.expr_sql,
                    Some(&target_name),
                    &target_relation.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                if !action.not_valid {
                    validate_check_rows(
                        self,
                        &target_relation,
                        &target_name,
                        &action.constraint_name,
                        &action.expr_sql,
                        &catalog,
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid
                        .saturating_add(1)
                        .saturating_add(catalog_effects.len() as u32),
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .create_check_constraint_mvcc(
                        target.relation.relation_oid,
                        action.constraint_name.clone(),
                        action.enforced,
                        action.enforced && !action.not_valid,
                        action.no_inherit,
                        action.expr_sql.clone(),
                        action.parent_constraint_oid.unwrap_or(0),
                        action.is_local,
                        action.inhcount,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }

            if target.relation.relpersistence == 't' {
                self.replace_temp_entry_desc(client_id, target.relation.relation_oid, target_desc)?;
            }
        }
        for (index, constraint_stmt) in inline_added_column_table_constraints(alter_stmt)
            .into_iter()
            .enumerate()
        {
            self.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
                client_id,
                &constraint_stmt,
                xid,
                cid.saturating_add(50)
                    .saturating_add(catalog_effects.len() as u32)
                    .saturating_add(index as u32),
                configured_search_path,
                None,
                None,
                catalog_effects,
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
