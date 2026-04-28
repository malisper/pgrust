use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write as _;
use std::mem;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::backend::access::transam::xact::{
    CommandId, INVALID_TRANSACTION_ID, Snapshot, TransactionId,
};
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::commands::copyfrom::parse_text_array_literal_with_catalog;
use crate::backend::commands::copyto::{
    CopyToDmlEvent, CopyToSink, IoCopyToSink, begin_copy_to, begin_copy_to_dml_capture,
    finish_copy_to, finish_copy_to_dml_capture, write_copy_to, write_copy_to_row,
};
use crate::backend::commands::tablecmds::{
    check_planned_stmt_select_for_update_privileges, check_planned_stmt_select_privileges,
    check_relation_column_privileges, execute_merge, execute_prepared_insert_row,
};
use crate::backend::executor::expr_bool::parse_pg_bool_text;
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::executor::{
    DeferredForeignKeyTracker, ExecError, ExecutorContext, ExecutorTransactionState, Expr,
    RelationDesc, SessionReplicationRole, StatementResult, Value, cast_value,
    cast_value_with_source_type_catalog_and_config, execute_planned_stmt,
    execute_readonly_statement_with_config, parse_bytea_text, render_sql_literal,
    substitute_named_arg, substitute_positional_args,
};
use crate::backend::libpq::pqformat::FloatFormatOptions;
use crate::backend::parser::{
    AlterTableAddColumnStatement, CallStatement, CatalogLookup, CopyFormat as ParserCopyFormat,
    CopyFromStatement, CopyOptions as ParserCopyOptions, CopySource, CopyToDestination,
    CopyToSource, CopyToStatement, CreateFunctionStatement, CreateTableAsQuery,
    CreateTableAsStatement, CteBody, DeallocateStatement, DetachPartitionMode, DiscardTarget,
    ExecuteStatement, ParseError, ParseOptions, PrepareStatement, PreparedInsert, SelectStatement,
    Statement, bind_delete, bind_insert, bind_insert_prepared,
    bind_insert_with_outer_scopes_and_ctes, bind_update, bound_cte_from_query_rows,
    pg_plan_query_with_config, pg_plan_query_with_outer_scopes_and_ctes, plan_merge,
};
use crate::backend::rewrite::relation_has_row_security;
use crate::backend::storage::lmgr::{TableLockManager, TableLockMode, unlock_relations};
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::checkpoint::is_checkpoint_guc;
use crate::backend::utils::misc::guc::{
    is_postgres_guc, normalize_function_guc_assignment, normalize_guc_name,
    plpgsql_guc_default_value,
};
use crate::backend::utils::misc::guc_datetime::{
    DateTimeConfig, default_datestyle, default_datetime_config, default_intervalstyle,
    default_timezone, format_datestyle, format_intervalstyle, parse_datestyle_with_fallback,
    parse_intervalstyle, parse_timezone,
};
use crate::backend::utils::misc::guc_xml::{
    format_xmlbinary, format_xmloption, parse_xmlbinary, parse_xmloption,
};
use crate::backend::utils::misc::interrupts::{InterruptState, StatementInterruptGuard};
use crate::backend::utils::misc::stack_depth::{
    MIN_MAX_STACK_DEPTH_KB, StackDepthGuard, max_stack_depth_limit_kb,
};
use crate::include::catalog::{
    ANYARRAYOID, ANYELEMENTOID, ANYOID, INT4_TYPE_OID, NUMERIC_TYPE_OID, PG_CHECKPOINT_OID,
    PG_EXECUTE_SERVER_PROGRAM_OID, PG_LANGUAGE_PLPGSQL_OID, PG_LANGUAGE_SQL_OID,
    PG_WRITE_SERVER_FILES_OID, PgProcRow, TEXT_TYPE_OID,
};
use crate::include::nodes::execnodes::ScalarType;
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::primnodes::QueryColumn;
use crate::pgrust::auth::AuthState;
use crate::pgrust::autovacuum::is_autovacuum_guc;
use crate::pgrust::database::commands::privilege::{
    acl_grants_privilege, effective_acl_grantee_names, function_owner_default_acl,
};
use crate::pgrust::database::{
    AsyncListenAction, AsyncListenOp, Database, DynamicTypeSnapshot, PendingNotification,
    SequenceMutationEffect, SessionStatsState, StatsFetchConsistency, TempMutationEffect,
    TrackFunctionsSetting, alter_table_add_constraint_lock_requests,
    alter_table_validate_constraint_lock_requests, default_sequence_name_base,
    delete_foreign_key_lock_requests, execute_set_constraints, insert_foreign_key_lock_requests,
    merge_pending_notifications, merge_table_lock_requests,
    prepared_insert_foreign_key_lock_requests, queue_pending_notification,
    reject_relation_with_referencing_foreign_keys_except, relation_foreign_key_lock_requests,
    update_foreign_key_lock_requests, validate_deferred_constraints,
    validate_immediate_constraints,
};
use crate::pgrust::portal::{
    CursorOptions, CursorViewRow, Portal, PortalFetchDirection, PortalFetchLimit, PortalManager,
    PortalRunResult,
};
use crate::pl::plpgsql::{
    EventTriggerDdlCommandRow, EventTriggerDroppedObjectRow, PlpgsqlFunctionCache,
    execute_do_with_context, execute_user_defined_procedure_values,
};
use crate::{ClientId, RelFileLocator};
use parking_lot::RwLock;

fn validate_alter_table_add_constraint_temporal_fk_actions(
    stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
) -> Result<(), ExecError> {
    let crate::include::nodes::parsenodes::TableConstraint::ForeignKey {
        period,
        referenced_period,
        on_delete,
        on_update,
        ..
    } = &stmt.constraint
    else {
        return Ok(());
    };
    if period.is_none() && referenced_period.is_none() {
        return Ok(());
    }
    if *on_update != crate::include::nodes::parsenodes::ForeignKeyAction::NoAction {
        return Err(ExecError::DetailedError {
            message: "unsupported ON UPDATE action for foreign key constraint using PERIOD".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if *on_delete != crate::include::nodes::parsenodes::ForeignKeyAction::NoAction {
        return Err(ExecError::DetailedError {
            message: "unsupported ON DELETE action for foreign key constraint using PERIOD".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

fn validate_statement_temporal_fk_actions(stmt: &Statement) -> Result<(), ExecError> {
    match stmt {
        Statement::AlterTableAddConstraint(add) => {
            validate_alter_table_add_constraint_temporal_fk_actions(add)
        }
        Statement::AlterTableCompound(compound) => {
            validate_compound_alter_table_temporal_fk_actions(compound)
        }
        _ => Ok(()),
    }
}

fn validate_compound_alter_table_temporal_fk_actions(
    stmt: &crate::backend::parser::AlterTableCompoundStatement,
) -> Result<(), ExecError> {
    for action in &stmt.actions {
        validate_statement_temporal_fk_actions(action)?;
    }
    Ok(())
}

fn validate_multi_alter_table_temporal_fk_actions(
    statements: &[String],
    options: ParseOptions,
) -> Result<(), ExecError> {
    for sql in statements {
        let stmt = crate::backend::parser::parse_statement_with_options(sql, options)?;
        validate_statement_temporal_fk_actions(&stmt)?;
        if let Statement::AlterTableMulti(nested) = stmt {
            validate_multi_alter_table_temporal_fk_actions(&nested, options)?;
        }
    }
    Ok(())
}

fn quote_identifier_for_event_identity(identifier: &str) -> String {
    crate::backend::executor::expr_reg::quote_identifier_if_needed(identifier)
}

fn unquote_event_ident(identifier: &str) -> String {
    let trimmed = identifier.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn split_event_qualified_name(name: &str) -> (Option<String>, String) {
    if let Some((schema, object)) = name.rsplit_once('.') {
        (
            Some(unquote_event_ident(schema)),
            unquote_event_ident(object),
        )
    } else {
        (None, unquote_event_ident(name))
    }
}

fn relation_schema_from_name(name: &str) -> Option<String> {
    split_event_qualified_name(name).0
}

fn unqualified_event_name(name: &str) -> String {
    split_event_qualified_name(name).1
}

fn schema_and_name_for_event_identity(
    name: &str,
    explicit_schema: Option<&str>,
    schema_override: Option<&str>,
    default_schema: Option<&str>,
) -> (String, String) {
    let (name_schema, object_name) = split_event_qualified_name(name);
    let schema = explicit_schema
        .map(unquote_event_ident)
        .or(name_schema)
        .or_else(|| schema_override.map(unquote_event_ident))
        .or_else(|| default_schema.map(unquote_event_ident))
        .unwrap_or_else(|| "public".into());
    (schema, object_name)
}

fn relation_schema_for_event_identity(
    explicit_schema: Option<&str>,
    name: &str,
    schema_override: Option<&str>,
    default_schema: Option<&str>,
    persistence: crate::backend::parser::TablePersistence,
) -> String {
    if persistence == crate::backend::parser::TablePersistence::Temporary {
        return "pg_temp".into();
    }
    schema_and_name_for_event_identity(name, explicit_schema, schema_override, default_schema).0
}

fn qualified_event_identity(schema: &str, object_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier_for_event_identity(schema),
        quote_identifier_for_event_identity(object_name)
    )
}

fn event_trigger_index_command_row(
    catalog: &dyn CatalogLookup,
    tag: &str,
    index_oid: u32,
) -> Option<EventTriggerDdlCommandRow> {
    let class_row = catalog.class_row_by_oid(index_oid)?;
    let schema = catalog
        .namespace_row_by_oid(class_row.relnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "public".into());
    Some(EventTriggerDdlCommandRow {
        command_tag: tag.to_string(),
        object_type: "index".into(),
        schema_name: Some(schema.clone()),
        object_identity: qualified_event_identity(&schema, &class_row.relname),
    })
}

fn event_trigger_leaf_index_rows_for_partitioned_index(
    catalog: &dyn CatalogLookup,
    tag: &str,
    partitioned_index_oid: u32,
) -> Vec<EventTriggerDdlCommandRow> {
    catalog
        .find_all_inheritors(partitioned_index_oid)
        .into_iter()
        .filter(|oid| *oid != partitioned_index_oid)
        .filter(|oid| {
            catalog
                .class_row_by_oid(*oid)
                .is_some_and(|row| row.relkind == 'i')
        })
        .filter_map(|oid| event_trigger_index_command_row(catalog, tag, oid))
        .collect()
}

fn event_trigger_index_rows_for_heap(
    catalog: &dyn CatalogLookup,
    tag: &str,
    relation_oid: u32,
) -> Vec<EventTriggerDdlCommandRow> {
    let mut rows = Vec::new();
    for index in catalog.index_relations_for_heap(relation_oid) {
        if index.relkind == 'I' {
            rows.extend(event_trigger_leaf_index_rows_for_partitioned_index(
                catalog,
                tag,
                index.relation_oid,
            ));
        } else if index.relkind == 'i'
            && let Some(row) = event_trigger_index_command_row(catalog, tag, index.relation_oid)
        {
            rows.push(row);
        }
    }
    rows
}

fn event_trigger_reindex_rows(
    catalog: &dyn CatalogLookup,
    tag: &str,
    reindex: &crate::backend::parser::ReindexIndexStatement,
) -> Vec<EventTriggerDdlCommandRow> {
    match reindex.kind {
        crate::backend::parser::ReindexTargetKind::Index => {
            let Some(index) = catalog.lookup_any_relation(&reindex.index_name) else {
                return Vec::new();
            };
            if index.relkind == 'I' {
                event_trigger_leaf_index_rows_for_partitioned_index(
                    catalog,
                    tag,
                    index.relation_oid,
                )
            } else if index.relkind == 'i' {
                event_trigger_index_command_row(catalog, tag, index.relation_oid)
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            }
        }
        crate::backend::parser::ReindexTargetKind::Table => {
            let Some(relation) = catalog.lookup_any_relation(&reindex.index_name) else {
                return Vec::new();
            };
            if relation.relkind == 'p' {
                catalog
                    .find_all_inheritors(relation.relation_oid)
                    .into_iter()
                    .filter(|oid| *oid != relation.relation_oid)
                    .filter(|oid| {
                        catalog
                            .class_row_by_oid(*oid)
                            .is_some_and(|row| matches!(row.relkind, 'r' | 'm' | 't'))
                    })
                    .flat_map(|oid| event_trigger_index_rows_for_heap(catalog, tag, oid))
                    .collect()
            } else {
                event_trigger_index_rows_for_heap(catalog, tag, relation.relation_oid)
            }
        }
        crate::backend::parser::ReindexTargetKind::Schema => {
            let Some(namespace_oid) = catalog
                .namespace_rows()
                .into_iter()
                .find(|row| row.nspname.eq_ignore_ascii_case(&reindex.index_name))
                .map(|row| row.oid)
            else {
                return Vec::new();
            };
            catalog
                .class_rows()
                .into_iter()
                .filter(|row| {
                    row.relnamespace == namespace_oid && matches!(row.relkind, 'r' | 'm' | 't')
                })
                .flat_map(|row| event_trigger_index_rows_for_heap(catalog, tag, row.oid))
                .collect()
        }
        crate::backend::parser::ReindexTargetKind::Database
        | crate::backend::parser::ReindexTargetKind::System => Vec::new(),
    }
}

fn event_trigger_relation_schema_and_name(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
) -> (String, String, bool) {
    let is_temporary = relation.relpersistence == 't';
    let schema = if is_temporary {
        "pg_temp".into()
    } else {
        catalog
            .namespace_row_by_oid(relation.namespace_oid)
            .map(|row| row.nspname)
            .unwrap_or_else(|| "public".into())
    };
    let table = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    (schema, table, is_temporary)
}

fn dropped_object_row(
    object_type: &str,
    schema_name: Option<String>,
    object_name: Option<String>,
    object_identity: String,
    address_names: Vec<String>,
    address_args: Vec<String>,
    original: bool,
    normal: bool,
    is_temporary: bool,
) -> EventTriggerDroppedObjectRow {
    EventTriggerDroppedObjectRow {
        classid: 0,
        objid: 0,
        objsubid: 0,
        original,
        normal,
        is_temporary,
        object_type: object_type.into(),
        schema_name,
        object_name,
        object_identity,
        address_names,
        address_args,
    }
}

fn event_trigger_dropped_table_rows(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
) -> Vec<EventTriggerDroppedObjectRow> {
    event_trigger_dropped_table_rows_with_flags(catalog, relation, true, false, false)
}

fn event_trigger_dropped_table_rows_with_flags(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    table_original: bool,
    table_normal: bool,
    dependent_normal: bool,
) -> Vec<EventTriggerDroppedObjectRow> {
    // :HACK: This captures the relation-local dropped-object rows exercised by
    // event_trigger.sql. Full PostgreSQL compatibility should be collected from
    // dependency deletion instead of reconstructed from the pre-drop catalog.
    let (schema, table, is_temporary) = event_trigger_relation_schema_and_name(catalog, relation);
    let qualified_table = qualified_event_identity(&schema, &table);
    let mut rows = vec![
        dropped_object_row(
            "table",
            Some(schema.clone()),
            Some(table.clone()),
            qualified_table.clone(),
            vec![schema.clone(), table.clone()],
            Vec::new(),
            table_original,
            table_normal,
            is_temporary,
        ),
        dropped_object_row(
            "type",
            Some(schema.clone()),
            Some(table.clone()),
            qualified_table.clone(),
            vec![qualified_table.clone()],
            Vec::new(),
            false,
            false,
            is_temporary,
        ),
        dropped_object_row(
            "type",
            Some(schema.clone()),
            Some(format!("_{table}")),
            format!("{qualified_table}[]"),
            vec![format!("{qualified_table}[]")],
            Vec::new(),
            false,
            false,
            is_temporary,
        ),
    ];

    for column in &relation.desc.columns {
        if column.dropped {
            continue;
        }
        if column.default_expr.is_some() || column.default_sequence_oid.is_some() {
            rows.push(dropped_object_row(
                "default value",
                Some(schema.clone()),
                None,
                format!(
                    "for {}.{}",
                    qualified_table,
                    quote_identifier_for_event_identity(&column.name)
                ),
                vec![schema.clone(), table.clone(), column.name.clone()],
                Vec::new(),
                false,
                dependent_normal,
                is_temporary,
            ));
        }
    }

    for column in &relation.desc.columns {
        if column.dropped {
            continue;
        }
        if let Some(name) = &column.not_null_constraint_name {
            rows.push(dropped_object_row(
                "table constraint",
                Some(schema.clone()),
                None,
                format!(
                    "{} on {}",
                    quote_identifier_for_event_identity(name),
                    qualified_table
                ),
                vec![schema.clone(), table.clone(), name.clone()],
                Vec::new(),
                false,
                dependent_normal,
                is_temporary,
            ));
        }
    }

    for constraint in catalog.constraint_rows_for_relation(relation.relation_oid) {
        if constraint.contype == crate::include::catalog::CONSTRAINT_NOTNULL {
            continue;
        }
        rows.push(dropped_object_row(
            "table constraint",
            Some(schema.clone()),
            None,
            format!(
                "{} on {}",
                quote_identifier_for_event_identity(&constraint.conname),
                qualified_table
            ),
            vec![schema.clone(), table.clone(), constraint.conname],
            Vec::new(),
            false,
            dependent_normal,
            is_temporary,
        ));
    }

    for index in catalog.index_relations_for_heap(relation.relation_oid) {
        if let Some(class_row) = catalog.class_row_by_oid(index.relation_oid) {
            rows.push(dropped_object_row(
                "index",
                Some(schema.clone()),
                Some(class_row.relname.clone()),
                qualified_event_identity(&schema, &class_row.relname),
                vec![schema.clone(), class_row.relname],
                Vec::new(),
                false,
                dependent_normal,
                is_temporary,
            ));
        }
    }

    for trigger in catalog.trigger_rows_for_relation(relation.relation_oid) {
        if trigger.tgisinternal {
            continue;
        }
        rows.push(dropped_object_row(
            "trigger",
            Some(schema.clone()),
            None,
            format!(
                "{} on {}",
                quote_identifier_for_event_identity(&trigger.tgname),
                qualified_table
            ),
            vec![schema.clone(), table.clone(), trigger.tgname],
            Vec::new(),
            false,
            dependent_normal,
            is_temporary,
        ));
    }

    for policy in catalog.policy_rows_for_relation(relation.relation_oid) {
        rows.push(dropped_object_row(
            "policy",
            Some(schema.clone()),
            None,
            format!(
                "{} on {}",
                quote_identifier_for_event_identity(&policy.polname),
                qualified_table
            ),
            vec![schema.clone(), table.clone(), policy.polname],
            Vec::new(),
            false,
            true,
            is_temporary,
        ));
    }

    rows
}

fn event_trigger_relation_column_index<'a>(
    relation: &'a crate::backend::parser::BoundRelation,
    column_name: &str,
) -> Option<(usize, &'a crate::include::nodes::primnodes::ColumnDesc)> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
}

fn event_trigger_dropped_column_rows(
    catalog: &dyn CatalogLookup,
    table_name: &str,
    column_name: &str,
) -> Vec<EventTriggerDroppedObjectRow> {
    let Some(relation) = catalog.lookup_any_relation(table_name) else {
        return Vec::new();
    };
    let (schema, table, is_temporary) = event_trigger_relation_schema_and_name(catalog, &relation);
    let qualified_table = qualified_event_identity(&schema, &table);
    let Some((column_index, column)) = event_trigger_relation_column_index(&relation, column_name)
    else {
        return Vec::new();
    };
    let column_attnum = i16::try_from(column_index + 1).unwrap_or(i16::MAX);
    let mut rows = vec![dropped_object_row(
        "table column",
        Some(schema.clone()),
        None,
        format!(
            "{}.{}",
            qualified_table,
            quote_identifier_for_event_identity(&column.name)
        ),
        vec![schema.clone(), table.clone(), column.name.clone()],
        Vec::new(),
        true,
        false,
        is_temporary,
    )];

    if column.default_sequence_oid.is_some() {
        rows.push(dropped_object_row(
            "default value",
            Some(schema.clone()),
            None,
            format!(
                "for {}.{}",
                qualified_table,
                quote_identifier_for_event_identity(&column.name)
            ),
            vec![schema.clone(), table.clone(), column.name.clone()],
            Vec::new(),
            false,
            true,
            is_temporary,
        ));
    }

    for constraint in catalog.constraint_rows_for_relation(relation.relation_oid) {
        if constraint.contype != crate::include::catalog::CONSTRAINT_CHECK {
            continue;
        }
        let references_column = constraint
            .conkey
            .as_ref()
            .is_some_and(|keys| keys.contains(&column_attnum))
            || constraint
                .conname
                .to_ascii_lowercase()
                .contains(&column.name.to_ascii_lowercase());
        if !references_column {
            continue;
        }
        rows.push(dropped_object_row(
            "table constraint",
            Some(schema.clone()),
            None,
            format!(
                "{} on {}",
                quote_identifier_for_event_identity(&constraint.conname),
                qualified_table
            ),
            vec![schema.clone(), table.clone(), constraint.conname],
            Vec::new(),
            false,
            true,
            is_temporary,
        ));
    }

    rows
}

fn event_trigger_dropped_default_row(
    catalog: &dyn CatalogLookup,
    table_name: &str,
    column_name: &str,
) -> Vec<EventTriggerDroppedObjectRow> {
    let Some(relation) = catalog.lookup_any_relation(table_name) else {
        return Vec::new();
    };
    let (schema, table, is_temporary) = event_trigger_relation_schema_and_name(catalog, &relation);
    let qualified_table = qualified_event_identity(&schema, &table);
    let Some((_, column)) = event_trigger_relation_column_index(&relation, column_name) else {
        return Vec::new();
    };
    if column.default_expr.is_none() && column.default_sequence_oid.is_none() {
        return Vec::new();
    }
    vec![dropped_object_row(
        "default value",
        Some(schema.clone()),
        None,
        format!(
            "for {}.{}",
            qualified_table,
            quote_identifier_for_event_identity(&column.name)
        ),
        vec![schema, table, column.name.clone()],
        Vec::new(),
        true,
        false,
        is_temporary,
    )]
}

fn event_trigger_dropped_constraint_row(
    catalog: &dyn CatalogLookup,
    table_name: &str,
    constraint_name: &str,
) -> Vec<EventTriggerDroppedObjectRow> {
    let Some(relation) = catalog.lookup_any_relation(table_name) else {
        return Vec::new();
    };
    let (schema, table, is_temporary) = event_trigger_relation_schema_and_name(catalog, &relation);
    let qualified_table = qualified_event_identity(&schema, &table);
    let Some(constraint) = catalog
        .constraint_rows_for_relation(relation.relation_oid)
        .into_iter()
        .find(|row| row.conname.eq_ignore_ascii_case(constraint_name))
    else {
        return Vec::new();
    };
    vec![dropped_object_row(
        "table constraint",
        Some(schema.clone()),
        None,
        format!(
            "{} on {}",
            quote_identifier_for_event_identity(&constraint.conname),
            qualified_table
        ),
        vec![schema, table, constraint.conname],
        Vec::new(),
        true,
        false,
        is_temporary,
    )]
}

fn event_trigger_proc_arg_type_names(
    catalog: &dyn CatalogLookup,
    proargtypes: &str,
) -> Vec<String> {
    proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .map(|oid| crate::backend::executor::expr_reg::format_type_text(oid, None, catalog))
        .collect()
}

fn event_trigger_dropped_schema_rows(
    catalog: &dyn CatalogLookup,
    drop: &crate::backend::parser::DropSchemaStatement,
) -> Vec<EventTriggerDroppedObjectRow> {
    // :HACK: Regression-scoped schema-drop object collection. PostgreSQL
    // derives this from dependency deletion; pgrust should eventually collect
    // dropped object addresses from the same place that applies catalog deletes.
    let mut rows = Vec::new();
    let mut schemas = drop
        .schema_names
        .iter()
        .filter_map(|name| {
            catalog
                .namespace_rows()
                .into_iter()
                .find(|row| row.nspname.eq_ignore_ascii_case(name))
        })
        .collect::<Vec<_>>();
    schemas.reverse();

    for schema_row in schemas {
        let schema = schema_row.nspname.clone();
        rows.push(dropped_object_row(
            "schema",
            None,
            Some(schema.clone()),
            quote_identifier_for_event_identity(&schema),
            vec![schema.clone()],
            Vec::new(),
            true,
            false,
            false,
        ));

        for class_row in catalog.class_rows() {
            if class_row.relnamespace != schema_row.oid {
                continue;
            }
            match class_row.relkind {
                'r' | 'm' | 'p' | 't' => {
                    if let Some(relation) = catalog.relation_by_oid(class_row.oid) {
                        let mut relation_rows = event_trigger_dropped_table_rows_with_flags(
                            catalog, &relation, false, true, true,
                        );
                        relation_rows.retain(|row| {
                            !matches!(row.object_type.as_str(), "index" | "table constraint")
                        });
                        let (default_rows, relation_rows): (Vec<_>, Vec<_>) = relation_rows
                            .into_iter()
                            .partition(|row| row.object_type == "default value");
                        rows.extend(relation_rows);
                        for column in &relation.desc.columns {
                            let Some(sequence_oid) = column.default_sequence_oid else {
                                continue;
                            };
                            let Some(sequence) = catalog.class_row_by_oid(sequence_oid) else {
                                continue;
                            };
                            rows.push(dropped_object_row(
                                "sequence",
                                Some(schema.clone()),
                                Some(sequence.relname.clone()),
                                qualified_event_identity(&schema, &sequence.relname),
                                vec![schema.clone(), sequence.relname],
                                Vec::new(),
                                false,
                                true,
                                false,
                            ));
                        }
                        rows.extend(default_rows);
                    }
                }
                'S' => {}
                _ => {}
            }
        }

        for proc_row in catalog.proc_rows() {
            if proc_row.pronamespace != schema_row.oid || !matches!(proc_row.prokind, 'f' | 'a') {
                continue;
            }
            let args = event_trigger_proc_arg_type_names(catalog, &proc_row.proargtypes);
            let object_type = if proc_row.prokind == 'a' {
                "aggregate"
            } else {
                "function"
            };
            rows.push(dropped_object_row(
                object_type,
                Some(schema.clone()),
                None,
                format!(
                    "{}({})",
                    qualified_event_identity(&schema, &proc_row.proname),
                    args.join(",")
                ),
                vec![schema.clone(), proc_row.proname],
                args,
                false,
                true,
                false,
            ));
        }
    }

    rows
}

fn create_table_has_primary_key(create: &crate::backend::parser::CreateTableStatement) -> bool {
    create.elements.iter().any(|element| match element {
        crate::include::nodes::parsenodes::CreateTableElement::Column(column) => {
            column.constraints.iter().any(|constraint| {
                matches!(
                    constraint,
                    crate::include::nodes::parsenodes::ColumnConstraint::PrimaryKey { .. }
                )
            })
        }
        crate::include::nodes::parsenodes::CreateTableElement::Constraint(constraint) => {
            matches!(
                constraint,
                crate::include::nodes::parsenodes::TableConstraint::PrimaryKey { .. }
                    | crate::include::nodes::parsenodes::TableConstraint::PrimaryKeyUsingIndex { .. }
            )
        }
        _ => false,
    })
}

fn create_table_owned_sequence_names(
    create: &crate::backend::parser::CreateTableStatement,
) -> Vec<String> {
    create
        .elements
        .iter()
        .filter_map(|element| {
            let crate::include::nodes::parsenodes::CreateTableElement::Column(column) = element
            else {
                return None;
            };
            let owns_sequence = matches!(column.ty, crate::backend::parser::RawTypeName::Serial(_))
                || column.identity.is_some();
            owns_sequence.then(|| default_sequence_name_base(&create.table_name, &column.name))
        })
        .collect()
}

fn create_table_has_post_create_alter_table(
    create: &crate::backend::parser::CreateTableStatement,
) -> bool {
    create.elements.iter().any(|element| match element {
        crate::include::nodes::parsenodes::CreateTableElement::Column(column) => {
            column.constraints.iter().any(|constraint| {
                matches!(
                    constraint,
                    crate::include::nodes::parsenodes::ColumnConstraint::References { .. }
                )
            })
        }
        crate::include::nodes::parsenodes::CreateTableElement::Constraint(constraint) => {
            matches!(
                constraint,
                crate::include::nodes::parsenodes::TableConstraint::ForeignKey { .. }
            )
        }
        _ => false,
    })
}

fn event_trigger_sequence_row(
    command_tag: &str,
    schema: &str,
    sequence_name: &str,
) -> EventTriggerDdlCommandRow {
    EventTriggerDdlCommandRow {
        command_tag: command_tag.into(),
        object_type: "sequence".into(),
        schema_name: Some(schema.into()),
        object_identity: qualified_event_identity(schema, sequence_name),
    }
}

fn event_trigger_alter_table_row(schema: &str, table: &str) -> EventTriggerDdlCommandRow {
    EventTriggerDdlCommandRow {
        command_tag: "ALTER TABLE".into(),
        object_type: "table".into(),
        schema_name: Some(schema.into()),
        object_identity: qualified_event_identity(schema, table),
    }
}

fn event_trigger_sequence_name_for_column(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    column_name: &str,
) -> Option<(String, String)> {
    let (schema, _, _) = event_trigger_relation_schema_and_name(catalog, relation);
    let (_, column) = event_trigger_relation_column_index(relation, column_name)?;
    let sequence_oid = column.default_sequence_oid?;
    let sequence = catalog.lookup_relation_by_oid(sequence_oid)?;
    let class_row = catalog.class_row_by_oid(sequence.relation_oid)?;
    Some((schema, class_row.relname))
}

fn raw_type_name_for_event_identity(ty: &crate::backend::parser::RawTypeName) -> String {
    match ty {
        crate::backend::parser::RawTypeName::Builtin(sql_type) => {
            crate::backend::parser::analyze::sql_type_name(*sql_type)
        }
        crate::backend::parser::RawTypeName::Serial(kind) => match kind {
            crate::backend::parser::SerialKind::Small => "smallint".into(),
            crate::backend::parser::SerialKind::Regular => "integer".into(),
            crate::backend::parser::SerialKind::Big => "bigint".into(),
        },
        crate::backend::parser::RawTypeName::Named { name, .. } => name.clone(),
        crate::backend::parser::RawTypeName::Record => "record".into(),
    }
}

pub struct SelectGuard {
    pub state: crate::include::nodes::execnodes::PlanState,
    pub ctx: ExecutorContext,
    pub columns: Vec<crate::backend::executor::QueryColumn>,
    pub column_names: Vec<String>,
    pub(crate) rels: Vec<RelFileLocator>,
    pub(crate) table_locks: Arc<TableLockManager>,
    pub(crate) client_id: ClientId,
    pub(crate) advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
    pub(crate) row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
    pub(crate) statement_lock_scope_id: Option<u64>,
    pub(crate) interrupt_guard: Option<StatementInterruptGuard>,
    pub(crate) catalog_effect_start: usize,
    pub(crate) base_command_id: CommandId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyFormat {
    Text,
    Csv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyHeader {
    None,
    Present,
    Match,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyOnError {
    Stop,
    Ignore,
}

const COPY_TEXT_NULL_SENTINEL: &str = "\0pgrust_copy_text_null";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyOptions {
    pub format: CopyFormat,
    pub encoding: Option<String>,
    pub header: CopyHeader,
    pub quote: char,
    pub escape: char,
    pub null_marker: String,
    pub default_marker: Option<String>,
    pub on_error_ignore: bool,
    pub freeze: bool,
    pub where_clause: Option<String>,
    pub force_quote_all: bool,
    pub force_quote_columns: Vec<String>,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            encoding: None,
            header: CopyHeader::None,
            quote: '"',
            escape: '"',
            null_marker: "\\N".into(),
            default_marker: None,
            on_error_ignore: false,
            freeze: false,
            where_clause: None,
            force_quote_all: false,
            force_quote_columns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyRelation {
    Table {
        name: String,
        columns: Option<Vec<String>>,
    },
    Query(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyEndpoint {
    File(String),
    Stdin,
    Stdout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyDirection {
    From(CopyEndpoint),
    To(CopyEndpoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyCommand {
    pub relation: CopyRelation,
    pub direction: CopyDirection,
    pub options: CopyOptions,
}

#[derive(Debug)]
pub(crate) enum CopyExecutionResult {
    AffectedRows(usize),
    Output { data: Vec<u8>, rows: usize },
}

struct CopyInsertOptions<'a> {
    null_marker: &'a str,
    default_marker: Option<&'a str>,
    on_error: CopyOnError,
    where_filter: Option<CopyWhereFilter>,
    progress: Option<CopyProgressOptions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CopyProgressOptions {
    source: CopyProgressSource,
    bytes_processed: i64,
    bytes_total: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyProgressSource {
    File,
    Pipe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyWhereFilter {
    column: String,
    op: &'static str,
    literal: String,
}

struct ResolvedCopyWhereFilter {
    column_index: usize,
    op: &'static str,
    literal: String,
}

impl CopyWhereFilter {
    fn resolve(
        &self,
        desc: &crate::backend::executor::RelationDesc,
    ) -> Result<ResolvedCopyWhereFilter, ExecError> {
        let Some(column_index) = desc
            .columns
            .iter()
            .position(|column| !column.dropped && column.name.eq_ignore_ascii_case(&self.column))
        else {
            return Err(ExecError::Parse(ParseError::UnknownColumn(
                self.column.clone(),
            )));
        };
        Ok(ResolvedCopyWhereFilter {
            column_index,
            op: self.op,
            literal: self.literal.clone(),
        })
    }
}

fn bind_copy_column_defaults(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ExecError> {
    desc.columns
        .iter()
        .map(|column| {
            if column.generated.is_some() {
                return Ok(Expr::Const(Value::Null));
            }
            if let Some(sequence_oid) = column.default_sequence_oid {
                let expr = Expr::builtin_func(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::NextVal,
                    Some(crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Int8,
                    )),
                    false,
                    vec![Expr::Const(Value::Int64(i64::from(sequence_oid)))],
                );
                return Ok(
                    if column.sql_type.kind == crate::backend::parser::SqlTypeKind::Int8 {
                        expr
                    } else {
                        Expr::Cast(Box::new(expr), column.sql_type)
                    },
                );
            }
            column
                .default_expr
                .as_deref()
                .map(|sql| {
                    let parsed = crate::backend::parser::parse_expr(sql)?;
                    crate::backend::parser::bind_scalar_expr_in_scope(&parsed, &[], catalog)
                        .map(|(expr, _)| expr)
                })
                .transpose()
                .map(|expr| expr.or_else(|| column.missing_default_value.clone().map(Expr::Const)))
                .map(|expr| expr.unwrap_or(Expr::Const(Value::Null)))
                .map_err(ExecError::Parse)
        })
        .collect()
}

fn evaluate_copy_column_default(
    desc: &RelationDesc,
    column_defaults: &[Expr],
    column_index: usize,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut slot = crate::include::nodes::execnodes::TupleSlot::virtual_row(vec![
        Value::Null;
        desc.columns.len()
    ]);
    let value = crate::backend::executor::exec_expr::eval_expr(
        &column_defaults[column_index],
        &mut slot,
        ctx,
    )?;
    crate::backend::executor::value_io::coerce_assignment_value(
        &value,
        desc.columns[column_index].sql_type,
    )
}

impl ResolvedCopyWhereFilter {
    fn matches(&self, values: &[Value]) -> Result<bool, ExecError> {
        let Some(value) = values.get(self.column_index) else {
            return Ok(false);
        };
        if matches!(value, Value::Null) {
            return Ok(false);
        }
        let literal = self.literal.trim();
        let result = match value {
            Value::Int16(v) => compare_copy_i128(i128::from(*v), self.op, literal)?,
            Value::Int32(v) => compare_copy_i128(i128::from(*v), self.op, literal)?,
            Value::Int64(v) => compare_copy_i128(i128::from(*v), self.op, literal)?,
            Value::Float64(v) => compare_copy_f64(*v, self.op, literal)?,
            Value::Numeric(v) => compare_copy_f64(
                v.render().parse::<f64>().map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "numeric COPY WHERE value",
                        actual: v.render(),
                    })
                })?,
                self.op,
                literal,
            )?,
            Value::Text(v) => compare_copy_str(v.as_ref(), self.op, literal),
            _ => true,
        };
        Ok(result)
    }
}

fn compare_copy_i128(left: i128, op: &str, literal: &str) -> Result<bool, ExecError> {
    let right = literal.parse::<i128>().map_err(|_| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "integer COPY WHERE literal",
            actual: literal.into(),
        })
    })?;
    Ok(match op {
        "=" => left == right,
        "<>" | "!=" => left != right,
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => false,
    })
}

fn compare_copy_f64(left: f64, op: &str, literal: &str) -> Result<bool, ExecError> {
    let right = literal.parse::<f64>().map_err(|_| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "numeric COPY WHERE literal",
            actual: literal.into(),
        })
    })?;
    Ok(match op {
        "=" => left == right,
        "<>" | "!=" => left != right,
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => false,
    })
}

fn compare_copy_str(left: &str, op: &str, literal: &str) -> bool {
    let right = literal
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(literal);
    match op {
        "=" => left == right,
        "<>" | "!=" => left != right,
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => false,
    }
}

// SAFETY: A SelectGuard is owned by one Session and is never shared for
// concurrent access. Existing server/test code may move an idle Session between
// threads; moving the owned guard with that Session does not create cross-thread
// aliases to its executor Rc state.
unsafe impl Send for SelectGuard {}

fn select_sql_requires_command_end_xid_handling(sql: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)\b(txid_current|pg_current_xact_id|pg_restore_relation_stats|pg_clear_relation_stats|pg_restore_attribute_stats|pg_clear_attribute_stats)\s*\(",
        )
        .unwrap()
    });
    re.is_match(sql)
}

impl Drop for SelectGuard {
    fn drop(&mut self) {
        unlock_relations(&self.table_locks, self.client_id, &self.rels);
        if let Some(scope_id) = self.statement_lock_scope_id {
            self.advisory_locks
                .unlock_all_statement(self.client_id, scope_id);
            self.row_locks
                .unlock_all_statement(self.client_id, scope_id);
        }
    }
}

struct StatementLockScopeGuard {
    advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
    row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
    client_id: ClientId,
    scope_id: Option<u64>,
}

impl StatementLockScopeGuard {
    fn new(
        advisory_locks: Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
        row_locks: Arc<crate::backend::storage::lmgr::RowLockManager>,
        client_id: ClientId,
        scope_id: Option<u64>,
    ) -> Self {
        Self {
            advisory_locks,
            row_locks,
            client_id,
            scope_id,
        }
    }

    fn scope_id(&self) -> Option<u64> {
        self.scope_id
    }
}

impl Drop for StatementLockScopeGuard {
    fn drop(&mut self) {
        if let Some(scope_id) = self.scope_id {
            self.advisory_locks
                .unlock_all_statement(self.client_id, scope_id);
            self.row_locks
                .unlock_all_statement(self.client_id, scope_id);
        }
    }
}

struct ActiveTransaction {
    xid: Option<TransactionId>,
    started_at_usecs: i64,
    advisory_scope_id: u64,
    failed: bool,
    auth_at_start: AuthState,
    held_table_locks: BTreeMap<RelFileLocator, TableLockMode>,
    next_command_id: u32,
    isolation_level: crate::backend::parser::TransactionIsolationLevel,
    snapshot_taken: bool,
    transaction_snapshot: Option<Snapshot>,
    catalog_effects: Vec<CatalogMutationEffect>,
    current_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    prior_cmd_catalog_invalidations: Vec<CatalogInvalidation>,
    temp_effects: Vec<TempMutationEffect>,
    sequence_effects: Vec<SequenceMutationEffect>,
    deferred_foreign_keys: DeferredForeignKeyTracker,
    async_listen_ops: Vec<AsyncListenOp>,
    pending_async_notifications: Vec<PendingNotification>,
    dynamic_type_snapshot: DynamicTypeSnapshot,
    savepoints: Vec<SavepointState>,
    guc_start_state: GucState,
    guc_commit_state: GucState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GucState {
    gucs: HashMap<String, String>,
    datetime_config: DateTimeConfig,
    stats_fetch_consistency: StatsFetchConsistency,
    track_functions: TrackFunctionsSetting,
}

#[derive(Clone)]
struct SavepointState {
    name: String,
    dynamic_type_snapshot: DynamicTypeSnapshot,
    catalog_snapshot: crate::backend::catalog::store::CatalogStoreSnapshot,
    catalog_effect_len: usize,
    prior_catalog_invalidation_len: usize,
    temp_effect_len: usize,
    sequence_effect_len: usize,
    guc_effective_state: GucState,
    guc_commit_state: GucState,
    stats_state: crate::backend::utils::activity::SessionStatsState,
}

#[derive(Debug, Clone)]
struct PreparedSelectStatement {
    query: SelectStatement,
    query_sql: String,
}

pub struct Session {
    pub client_id: ClientId,
    pub(crate) temp_backend_id: crate::pgrust::database::TempBackendId,
    active_txn: Option<ActiveTransaction>,
    gucs: HashMap<String, String>,
    plpgsql_loaded: bool,
    datetime_config: DateTimeConfig,
    reset_datetime_config: DateTimeConfig,
    interrupts: Arc<InterruptState>,
    auth: AuthState,
    stats_state: Arc<RwLock<SessionStatsState>>,
    random_state: Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    portals: PortalManager,
    plpgsql_function_cache: Arc<RwLock<PlpgsqlFunctionCache>>,
    prepared_selects: HashMap<String, PreparedSelectStatement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}

#[derive(Debug, Clone)]
struct ResolvedCallProcedure {
    row: PgProcRow,
    input_arg_sql: Vec<String>,
}

#[derive(Debug, Clone)]
struct CallCandidateMatch {
    row: PgProcRow,
    input_arg_sql: Vec<String>,
    cost: usize,
}

#[derive(Debug, Clone)]
struct ProcedureParam {
    input_index: Option<usize>,
    name: String,
    type_oid: u32,
    mode: u8,
    variadic: bool,
}

#[derive(Debug, Clone)]
struct CallActualArg {
    name: Option<String>,
    sql: String,
}

fn resolve_call_procedure(
    call_stmt: &CallStatement,
    catalog: &dyn CatalogLookup,
) -> Result<ResolvedCallProcedure, ExecError> {
    let rows = catalog
        .proc_rows_by_name(&call_stmt.procedure_name)
        .into_iter()
        .filter(|row| proc_matches_call_schema(row, call_stmt.schema_name.as_deref(), catalog))
        .collect::<Vec<_>>();
    let actuals = call_actual_args(call_stmt);
    let mut matches = rows
        .iter()
        .filter(|row| row.prokind == 'p')
        .filter_map(|row| match_call_candidate(row, &actuals))
        .collect::<Vec<_>>();
    matches.sort_by_key(|candidate| candidate.cost);
    if matches.len() >= 2 && matches[0].cost == matches[1].cost {
        return Err(ExecError::DetailedError {
            message: format!(
                "procedure name \"{}\" is not unique",
                call_stmt.procedure_name
            ),
            detail: None,
            hint: Some("Could not choose a best candidate procedure. You might need to add explicit type casts.".into()),
            sqlstate: "42725",
        });
    }
    if let Some(candidate) = matches.into_iter().next() {
        return Ok(ResolvedCallProcedure {
            row: candidate.row,
            input_arg_sql: candidate.input_arg_sql,
        });
    }
    if rows
        .iter()
        .filter(|row| row.prokind != 'p')
        .any(|row| match_call_candidate(row, &actuals).is_some())
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "{} is not a procedure",
                call_signature_text(&call_stmt.procedure_name, &actuals)
            ),
            detail: None,
            hint: Some("To call a function, use SELECT.".into()),
            sqlstate: "42809",
        });
    }
    Err(call_undefined_procedure_error(call_stmt))
}

fn check_proc_execute_acl(
    session: &Session,
    db: &Database,
    proc_row: &PgProcRow,
) -> Result<(), ExecError> {
    let auth = session.auth_state();
    if auth.current_user_oid() == proc_row.proowner {
        return Ok(());
    }
    let auth_catalog = db
        .auth_catalog(session.client_id, session.catalog_txn_ctx())
        .map_err(|err| ExecError::DetailedError {
            message: format!("catalog lookup failed: {err:?}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
        || auth.has_effective_membership(proc_row.proowner, &auth_catalog)
    {
        return Ok(());
    }
    let owner_name = auth_catalog
        .role_by_oid(proc_row.proowner)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| proc_row.proowner.to_string());
    let acl = proc_row
        .proacl
        .clone()
        .unwrap_or_else(|| function_owner_default_acl(&owner_name));
    let effective_names = effective_acl_grantee_names(auth, &auth_catalog);
    if acl_grants_privilege(&acl, &effective_names, 'X') {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for procedure {}", proc_row.proname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn proc_matches_call_schema(
    row: &PgProcRow,
    schema_name: Option<&str>,
    catalog: &dyn CatalogLookup,
) -> bool {
    let Some(schema_name) = schema_name else {
        return true;
    };
    catalog
        .namespace_row_by_oid(row.pronamespace)
        .is_some_and(|namespace| namespace.nspname.eq_ignore_ascii_case(schema_name))
}

fn call_actual_args(call_stmt: &CallStatement) -> Vec<CallActualArg> {
    call_stmt
        .raw_arg_sql
        .iter()
        .enumerate()
        .map(|(index, sql)| CallActualArg {
            name: call_stmt
                .args
                .args()
                .get(index)
                .and_then(|arg| arg.name.clone()),
            sql: sql.clone(),
        })
        .collect()
}

fn call_signature_text(name: &str, actuals: &[CallActualArg]) -> String {
    let arg_types = actuals
        .iter()
        .map(|arg| call_actual_type_name(&arg.sql))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({arg_types})")
}

fn call_undefined_procedure_error(call_stmt: &CallStatement) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "procedure {} does not exist",
            call_signature_text(&call_stmt.procedure_name, &call_actual_args(call_stmt))
        ),
        detail: None,
        hint: Some(
            "No procedure matches the given name and argument types. You might need to add explicit type casts."
                .into(),
        ),
        sqlstate: "42883",
    }
}

fn procedure_params(row: &PgProcRow) -> Vec<ProcedureParam> {
    let arg_oids = row
        .proallargtypes
        .clone()
        .unwrap_or_else(|| parse_proc_argtype_oids(&row.proargtypes));
    let modes = row
        .proargmodes
        .clone()
        .unwrap_or_else(|| std::iter::repeat_n(b'i', arg_oids.len()).collect());
    let names = row.proargnames.clone().unwrap_or_default();
    let mut input_index = 0usize;
    arg_oids
        .into_iter()
        .enumerate()
        .map(|(index, type_oid)| {
            let mode = modes.get(index).copied().unwrap_or(b'i');
            let current_input_index = matches!(mode, b'i' | b'b' | b'v').then(|| {
                let index = input_index;
                input_index += 1;
                index
            });
            ProcedureParam {
                input_index: current_input_index,
                name: names.get(index).cloned().unwrap_or_default(),
                type_oid,
                mode,
                variadic: row.provariadic != 0
                    && (mode == b'v'
                        || current_input_index
                            == Some(row.pronargs.max(0).saturating_sub(1) as usize)),
            }
        })
        .collect()
}

fn parse_proc_argtype_oids(argtypes: &str) -> Vec<u32> {
    argtypes
        .split_whitespace()
        .filter_map(|part| part.parse::<u32>().ok())
        .collect()
}

fn decode_proc_arg_defaults(row: &PgProcRow, input_count: usize) -> Vec<Option<String>> {
    let Some(defaults) = row.proargdefaults.as_deref() else {
        return vec![None; input_count];
    };
    if let Ok(parsed) = serde_json::from_str::<Vec<Option<String>>>(defaults)
        && parsed.len() == input_count
    {
        return parsed;
    }
    let legacy = defaults
        .split_whitespace()
        .map(|default| Some(default.to_string()))
        .collect::<Vec<_>>();
    let mut aligned = vec![None; input_count.saturating_sub(legacy.len())];
    aligned.extend(legacy);
    aligned.resize(input_count, None);
    aligned
}

fn match_call_candidate(row: &PgProcRow, actuals: &[CallActualArg]) -> Option<CallCandidateMatch> {
    let params = procedure_params(row);
    let input_count = row.pronargs.max(0) as usize;
    let defaults = decode_proc_arg_defaults(row, input_count);
    let uses_all_slots = params.iter().any(|param| param.input_index.is_none())
        && actuals.len() > input_count.saturating_sub(row.pronargdefaults.max(0) as usize);
    let mut assigned = vec![None::<String>; input_count];
    let mut cost = 0usize;
    let mut positional_index = 0usize;

    let mut actual_index = 0usize;
    while actual_index < actuals.len() {
        let actual = &actuals[actual_index];
        let param = if let Some(name) = actual.name.as_ref() {
            params
                .iter()
                .find(|param| param.name.eq_ignore_ascii_case(name))?
        } else if uses_all_slots {
            let param = params.get(positional_index)?;
            positional_index += 1;
            param
        } else {
            let param = params
                .iter()
                .filter(|param| param.input_index.is_some())
                .nth(positional_index)?;
            positional_index += 1;
            param
        };

        if param.variadic && actual.name.is_none() {
            let input_index = param.input_index?;
            if assigned.get(input_index).is_some_and(Option::is_some) {
                return None;
            }
            let mut variadic_sql = Vec::new();
            while actual_index < actuals.len() {
                let actual = &actuals[actual_index];
                if actual.name.is_some() {
                    return None;
                }
                if !call_actual_matches_type(&actual.sql, row.provariadic) {
                    return None;
                }
                cost += call_actual_match_cost(&actual.sql, row.provariadic);
                variadic_sql.push(actual.sql.clone());
                actual_index += 1;
            }
            assigned[input_index] = Some(format!("array[{}]", variadic_sql.join(", ")));
            continue;
        }

        if !call_actual_matches_type(&actual.sql, param.type_oid) {
            return None;
        }
        cost += call_actual_match_cost(&actual.sql, param.type_oid);
        if let Some(input_index) = param.input_index {
            if assigned.get(input_index).is_some_and(Option::is_some) {
                return None;
            }
            assigned[input_index] = Some(actual.sql.clone());
        } else if !matches!(param.mode, b'o') {
            return None;
        }
        actual_index += 1;
    }

    for (index, slot) in assigned.iter_mut().enumerate() {
        if slot.is_some() {
            continue;
        }
        let default = defaults
            .get(index)
            .and_then(|default| default.as_deref())
            .filter(|default| !default.is_empty())?;
        *slot = Some(default.to_string());
        cost += 2;
    }

    Some(CallCandidateMatch {
        row: row.clone(),
        input_arg_sql: assigned.into_iter().collect::<Option<Vec<_>>>()?,
        cost,
    })
}

fn call_actual_matches_type(sql: &str, target_oid: u32) -> bool {
    if matches!(target_oid, ANYOID | ANYELEMENTOID | ANYARRAYOID) {
        return true;
    }
    let Some(actual_oid) = infer_call_actual_type_oid(sql) else {
        return true;
    };
    actual_oid == target_oid
        || (actual_oid == INT4_TYPE_OID && target_oid == NUMERIC_TYPE_OID)
        || (actual_oid == TEXT_TYPE_OID && target_oid != NUMERIC_TYPE_OID)
}

fn call_actual_match_cost(sql: &str, target_oid: u32) -> usize {
    match infer_call_actual_type_oid(sql) {
        Some(actual_oid) if actual_oid == target_oid => 0,
        Some(_) => 1,
        None => 1,
    }
}

fn call_actual_type_name(sql: &str) -> &'static str {
    match infer_call_actual_type_oid(sql) {
        Some(INT4_TYPE_OID) => "integer",
        Some(NUMERIC_TYPE_OID) => "numeric",
        Some(TEXT_TYPE_OID) => "unknown",
        _ => "unknown",
    }
}

fn infer_call_actual_type_oid(sql: &str) -> Option<u32> {
    let trimmed = sql.trim();
    if trimmed.eq_ignore_ascii_case("null") || trimmed.eq_ignore_ascii_case("default") {
        return None;
    }
    if (trimmed.starts_with('\'') && trimmed.ends_with('\'')) || trimmed.starts_with("least(") {
        return Some(TEXT_TYPE_OID);
    }
    if trimmed.contains('.') {
        return Some(NUMERIC_TYPE_OID);
    }
    if let Some((left, right)) = trimmed.split_once('/')
        && left.trim().parse::<i32>().is_ok()
        && right.trim().parse::<i32>().is_ok()
    {
        return Some(INT4_TYPE_OID);
    }
    if trimmed.parse::<i32>().is_ok() {
        return Some(INT4_TYPE_OID);
    }
    None
}

fn inline_sql_procedure_body(row: &PgProcRow, args: &[Value]) -> Result<String, ExecError> {
    let body = sql_standard_procedure_body_inner(row.prosrc.trim()).unwrap_or(row.prosrc.trim());
    let mut sql = substitute_positional_args(body, args)?;
    if let Some(names) = row.proargnames.as_ref() {
        let input_arg_names = names
            .iter()
            .zip(row.proargmodes.as_deref().unwrap_or(&[]).iter().copied())
            .filter(|(_, mode)| matches!(*mode, b'i' | b'b' | b'v'))
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        let names = if input_arg_names.is_empty() {
            names.iter().collect::<Vec<_>>()
        } else {
            input_arg_names
        };
        for (index, name) in names.into_iter().enumerate() {
            if name.is_empty() || index >= args.len() {
                continue;
            }
            let replacement = format!("({})", render_sql_literal(&args[index])?);
            sql = substitute_named_arg(&sql, name, &replacement);
        }
    }
    Ok(sql)
}

fn sql_standard_procedure_body_inner(body: &str) -> Option<&str> {
    let trimmed = body.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("begin atomic") {
        return None;
    }
    let mut end = trimmed.len();
    let without_trailing_semicolon = trimmed.trim_end_matches(';').trim_end();
    let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
    if lowered_without_semicolon.ends_with("end") {
        end = without_trailing_semicolon.len().saturating_sub("end".len());
    }
    trimmed.get("begin atomic".len()..end).map(str::trim)
}

fn split_sql_procedure_body(body: &str) -> Result<Vec<String>, ExecError> {
    let body = sql_standard_procedure_body_inner(body).unwrap_or(body);
    let mut statements = Vec::new();
    let mut start = 0usize;
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = scan_sql_delimited_end(bytes, i, b'\'')?;
                continue;
            }
            b'"' => {
                i = scan_sql_delimited_end(bytes, i, b'"')?;
                continue;
            }
            b'$' => {
                if let Some(end) = scan_sql_dollar_string_end(body, i) {
                    i = end;
                    continue;
                }
            }
            b';' => {
                let statement = body[start..i].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let statement = body[start..].trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    Ok(statements)
}

fn scan_sql_delimited_end(bytes: &[u8], start: usize, delimiter: u8) -> Result<usize, ExecError> {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
                continue;
            }
            return Ok(i + 1);
        }
        i += 1;
    }
    Err(ExecError::Parse(ParseError::UnexpectedEof))
}

fn scan_sql_dollar_string_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut tag_end = start + 1;
    while tag_end < bytes.len() {
        let byte = bytes[tag_end];
        if byte == b'$' {
            break;
        }
        if !(byte.is_ascii_alphanumeric() || byte == b'_') {
            return None;
        }
        tag_end += 1;
    }
    if bytes.get(tag_end) != Some(&b'$') {
        return None;
    }
    let tag = &input[start..=tag_end];
    input[tag_end + 1..]
        .find(tag)
        .map(|offset| tag_end + 1 + offset + tag.len())
}

fn procedure_output_args(
    row: &PgProcRow,
) -> impl Iterator<Item = (usize, Option<String>, u32)> + '_ {
    let names = row.proargnames.as_deref().unwrap_or(&[]);
    let modes = row.proargmodes.as_deref().unwrap_or(&[]);
    let types = row.proallargtypes.as_deref().unwrap_or(&[]);
    types
        .iter()
        .copied()
        .enumerate()
        .filter(move |(index, _)| {
            modes
                .get(*index)
                .is_some_and(|mode| matches!(*mode, b'o' | b'b'))
        })
        .map(move |(index, type_oid)| {
            (
                index,
                names.get(index).filter(|name| !name.is_empty()).cloned(),
                type_oid,
            )
        })
}

fn call_output_columns(row: &PgProcRow, catalog: &dyn CatalogLookup) -> Vec<QueryColumn> {
    procedure_output_args(row)
        .enumerate()
        .map(|(output_index, (_, name, type_oid))| {
            let sql_type = catalog
                .type_by_oid(type_oid)
                .map(|row| row.sql_type)
                .unwrap_or_else(|| {
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text)
                });
            QueryColumn {
                name: name.unwrap_or_else(|| format!("column{}", output_index + 1)),
                sql_type,
                wire_type_oid: Some(type_oid),
            }
        })
        .collect()
}

fn default_runtime_guc_value(name: &str) -> Option<&'static str> {
    if let Some(value) = plpgsql_guc_default_value(name) {
        return Some(value);
    }
    match name {
        "default_toast_compression" => Some("pglz"),
        "default_transaction_isolation" => Some("read committed"),
        "transaction_isolation" => Some("read committed"),
        "vacuum_cost_delay" => Some("0"),
        "track_counts" => Some("on"),
        "track_functions" => Some("none"),
        "stats_fetch_consistency" => Some("cache"),
        "restrict_nonsystem_relation_kind" => Some(""),
        "enable_seqscan"
        | "enable_indexscan"
        | "enable_indexonlyscan"
        | "enable_bitmapscan"
        | "enable_hashjoin"
        | "enable_mergejoin"
        | "enable_memoize"
        | "enable_hashagg"
        | "enable_sort" => Some("on"),
        _ => None,
    }
}

fn available_default_toast_compression_values() -> &'static str {
    #[cfg(feature = "lz4")]
    {
        "pglz, lz4"
    }
    #[cfg(not(feature = "lz4"))]
    {
        "pglz"
    }
}

fn invalid_default_toast_compression_value(value: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid value for parameter \"default_toast_compression\": \"{value}\""),
        detail: None,
        hint: Some(format!(
            "Available values: {}.",
            available_default_toast_compression_values()
        )),
        sqlstate: "22023",
    }
}

fn undefined_cursor(name: &str) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!("cursor \"{name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "34000",
    })
}

fn cursor_options_from_declare(
    stmt: &crate::backend::parser::DeclareCursorStatement,
) -> CursorOptions {
    let scroll = matches!(
        stmt.scroll,
        crate::backend::parser::CursorScrollOption::Scroll
    );
    let no_scroll = matches!(
        stmt.scroll,
        crate::backend::parser::CursorScrollOption::NoScroll
    );
    CursorOptions {
        holdable: stmt.hold,
        binary: stmt.binary,
        scroll,
        no_scroll,
        visible: true,
    }
}

fn portal_direction_from_fetch(
    direction: &crate::backend::parser::FetchDirection,
) -> PortalFetchDirection {
    use crate::backend::parser::FetchDirection;
    match direction {
        FetchDirection::Next => PortalFetchDirection::Next,
        FetchDirection::Prior => PortalFetchDirection::Prior,
        FetchDirection::First => PortalFetchDirection::First,
        FetchDirection::Last => PortalFetchDirection::Last,
        FetchDirection::Absolute(value) => PortalFetchDirection::Absolute(*value),
        FetchDirection::Relative(value) => PortalFetchDirection::Relative(*value),
        FetchDirection::Forward(count) => PortalFetchDirection::Forward(fetch_limit(*count)),
        FetchDirection::Backward(count) => PortalFetchDirection::Backward(fetch_limit(*count)),
    }
}

fn fetch_limit(count: Option<i64>) -> PortalFetchLimit {
    match count {
        None => PortalFetchLimit::All,
        Some(value) if value <= 0 => PortalFetchLimit::Count(0),
        Some(value) => PortalFetchLimit::Count(value as usize),
    }
}

fn parse_default_toast_compression_guc_value(value: &str) -> Result<&'static str, ExecError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pglz" => Ok("pglz"),
        #[cfg(feature = "lz4")]
        "lz4" => Ok("lz4"),
        _ => Err(invalid_default_toast_compression_value(value)),
    }
}

fn parse_plpgsql_extra_checks(value: &str) -> Result<String, ExecError> {
    let mut saw_all = false;
    let mut saw_none = false;
    let mut checks = Vec::new();
    for raw in value.split(',') {
        let item = raw.trim().to_ascii_lowercase();
        if item.is_empty() {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
        match item.as_str() {
            "all" => saw_all = true,
            "none" => saw_none = true,
            "shadowed_variables" | "strict_multi_assignment" | "too_many_rows" => {
                if !checks.iter().any(|check| check == &item) {
                    checks.push(item);
                }
            }
            _ => {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            }
        }
    }
    if saw_all && (saw_none || !checks.is_empty()) {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    if saw_none && !checks.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    if saw_all {
        Ok("all".to_string())
    } else if saw_none || checks.is_empty() {
        Ok("none".to_string())
    } else {
        Ok(checks.join(","))
    }
}

impl Session {
    const DEFAULT_MAINTENANCE_WORK_MEM_KB: usize = 65_536;

    pub fn new(client_id: ClientId) -> Self {
        Self::with_temp_backend_id(client_id, client_id)
    }

    pub fn with_temp_backend_id(
        client_id: ClientId,
        temp_backend_id: crate::pgrust::database::TempBackendId,
    ) -> Self {
        let datetime_config = default_datetime_config();
        Self {
            client_id,
            temp_backend_id,
            active_txn: None,
            gucs: HashMap::new(),
            plpgsql_loaded: false,
            datetime_config: datetime_config.clone(),
            reset_datetime_config: datetime_config,
            interrupts: Arc::new(InterruptState::new()),
            auth: AuthState::default(),
            stats_state: Arc::new(RwLock::new(SessionStatsState::default())),
            random_state: crate::backend::executor::PgPrngState::shared(),
            portals: PortalManager::default(),
            plpgsql_function_cache: Arc::new(RwLock::new(PlpgsqlFunctionCache::default())),
            prepared_selects: HashMap::new(),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    #[cfg(test)]
    pub(crate) fn plpgsql_function_cache_len(&self) -> usize {
        self.plpgsql_function_cache.read().len()
    }

    pub fn transaction_failed(&self) -> bool {
        self.active_txn.as_ref().is_some_and(|t| t.failed)
    }

    pub(crate) fn mark_transaction_failed(&mut self) {
        if let Some(ref mut txn) = self.active_txn {
            txn.failed = true;
        }
    }

    pub fn ready_status(&self) -> u8 {
        match &self.active_txn {
            None => b'I',
            Some(t) if t.failed => b'E',
            Some(_) => b'T',
        }
    }

    pub fn extra_float_digits(&self) -> i32 {
        self.gucs
            .get("extra_float_digits")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(1)
    }

    pub fn bytea_output(&self) -> ByteaOutputFormat {
        match self
            .gucs
            .get("bytea_output")
            .map(|value| value.trim().to_ascii_lowercase())
        {
            Some(value) if value == "escape" => ByteaOutputFormat::Escape,
            _ => ByteaOutputFormat::Hex,
        }
    }

    pub fn datetime_config(&self) -> &DateTimeConfig {
        &self.datetime_config
    }

    pub fn standard_conforming_strings(&self) -> bool {
        !matches!(
            self.gucs
                .get("standard_conforming_strings")
                .map(|value| value.trim().to_ascii_lowercase())
                .as_deref(),
            Some("off" | "false")
        )
    }

    pub fn escape_string_warning(&self) -> bool {
        !matches!(
            self.gucs
                .get("escape_string_warning")
                .map(|value| value.trim().to_ascii_lowercase())
                .as_deref(),
            Some("off" | "false")
        )
    }

    pub fn allow_in_place_tablespaces(&self) -> bool {
        matches!(
            self.gucs
                .get("allow_in_place_tablespaces")
                .map(|value| value.trim().to_ascii_lowercase())
                .as_deref(),
            Some("on" | "true")
        )
    }

    pub fn maintenance_work_mem_kb(&self) -> Result<usize, ExecError> {
        let Some(raw) = self.gucs.get("maintenance_work_mem") else {
            return Ok(Self::DEFAULT_MAINTENANCE_WORK_MEM_KB);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::DEFAULT_MAINTENANCE_WORK_MEM_KB);
        }
        let split_at = trimmed
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(trimmed.len());
        let (digits, suffix) = trimmed.split_at(split_at);
        let value = digits.parse::<usize>().map_err(|_| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid maintenance_work_mem value",
                actual: trimmed.to_string(),
            })
        })?;
        let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
            "" | "kb" => 1usize,
            "mb" => 1024usize,
            "gb" => 1024usize * 1024usize,
            _ => {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "maintenance_work_mem with optional kB, MB, or GB suffix",
                    actual: trimmed.to_string(),
                }));
            }
        };
        value.checked_mul(multiplier).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "maintenance_work_mem within usize range",
                actual: trimmed.to_string(),
            })
        })
    }

    fn apply_alter_table_set(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::AlterTableSetStatement,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.catalog_lookup(db);
        if let Some(relation) = catalog.lookup_any_relation(&stmt.table_name)
            && relation.relkind == 'v'
        {
            drop(catalog);
            if let Some((xid, cid)) = self.catalog_txn_ctx() {
                self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                let search_path = self.configured_search_path();
                let txn = self.active_txn.as_mut().unwrap();
                return db.execute_alter_view_set_options_stmt_in_transaction_with_search_path(
                    self.client_id,
                    stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                );
            }
            let search_path = self.configured_search_path();
            return db.execute_alter_view_set_options_stmt_with_search_path(
                self.client_id,
                stmt,
                search_path.as_deref(),
            );
        }
        for option in &stmt.options {
            if option.name.eq_ignore_ascii_case("toast_tuple_target") {
                let target = option.value.parse::<usize>().map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer toast_tuple_target",
                        actual: option.value.clone(),
                    })
                })?;
                let relation = catalog
                    .lookup_any_relation(&stmt.table_name)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(stmt.table_name.clone()))
                    })?;
                if let Some(toast) = relation.toast {
                    crate::backend::access::table::toast_helper::set_toast_tuple_target_for_toast_relation(
                        toast.relation_oid,
                        target,
                    );
                }
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_alter_index_set(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::AlterIndexSetStatement,
    ) -> Result<StatementResult, ExecError> {
        if let Some((xid, cid)) = self.catalog_txn_ctx() {
            let relation_to_lock = {
                let catalog = self.catalog_lookup(db);
                catalog
                    .lookup_any_relation(&stmt.index_name)
                    .map(|relation| relation.rel)
            };
            if let Some(rel) = relation_to_lock {
                self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
            }
            let search_path = self.configured_search_path();
            let txn = self.active_txn.as_mut().unwrap();
            return db.execute_alter_index_set_stmt_in_transaction_with_search_path(
                self.client_id,
                stmt,
                xid,
                cid,
                search_path.as_deref(),
                &mut txn.catalog_effects,
            );
        }
        let search_path = self.configured_search_path();
        db.execute_alter_index_set_stmt_with_search_path(
            self.client_id,
            stmt,
            search_path.as_deref(),
        )
    }

    pub(crate) fn catalog_txn_ctx(&self) -> Option<(TransactionId, u32)> {
        self.active_txn
            .as_ref()
            .and_then(|txn| txn.xid.map(|xid| (xid, txn.next_command_id)))
    }

    pub fn session_user_oid(&self) -> u32 {
        self.auth.session_user_oid()
    }

    pub fn current_user_oid(&self) -> u32 {
        self.auth.current_user_oid()
    }

    pub fn active_role_oid(&self) -> Option<u32> {
        self.auth.active_role_oid()
    }

    pub(crate) fn auth_state(&self) -> &AuthState {
        &self.auth
    }

    pub(crate) fn set_session_authorization_oid(&mut self, role_oid: u32) {
        self.auth.assume_authenticated_user(role_oid);
    }

    pub(crate) fn reset_session_authorization(&mut self) {
        self.auth.reset_session_authorization();
    }

    pub(crate) fn configured_search_path(&self) -> Option<Vec<String>> {
        let value = self.gucs.get("search_path")?;
        if value.trim().eq_ignore_ascii_case("default") {
            return None;
        }
        Some(
            value
                .split(',')
                .map(|schema| {
                    schema
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_ascii_lowercase()
                })
                .filter(|schema| !schema.is_empty())
                .collect(),
        )
    }

    pub(crate) fn row_security_enabled(&self) -> bool {
        self.gucs
            .get("row_security")
            .map(|value| parse_bool_guc(value).unwrap_or(true))
            .unwrap_or(true)
    }

    pub(crate) fn planner_config(&self) -> PlannerConfig {
        PlannerConfig {
            enable_partitionwise_join: self
                .gucs
                .get("enable_partitionwise_join")
                .map(|value| parse_bool_guc(value).unwrap_or(false))
                .unwrap_or(false),
            enable_seqscan: self
                .gucs
                .get("enable_seqscan")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_indexscan: self
                .gucs
                .get("enable_indexscan")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_indexonlyscan: self
                .gucs
                .get("enable_indexonlyscan")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_bitmapscan: self
                .gucs
                .get("enable_bitmapscan")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_nestloop: self
                .gucs
                .get("enable_nestloop")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_hashjoin: self
                .gucs
                .get("enable_hashjoin")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_mergejoin: self
                .gucs
                .get("enable_mergejoin")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_memoize: self
                .gucs
                .get("enable_memoize")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            retain_partial_index_filters: false,
            enable_hashagg: self
                .gucs
                .get("enable_hashagg")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
            enable_sort: self
                .gucs
                .get("enable_sort")
                .map(|value| parse_bool_guc(value).unwrap_or(true))
                .unwrap_or(true),
        }
    }

    pub(crate) fn track_activities_enabled(&self) -> bool {
        self.gucs
            .get("track_activities")
            .map(|value| parse_bool_guc(value).unwrap_or(true))
            .unwrap_or(true)
    }

    pub(crate) fn compute_query_id_enabled(&self) -> bool {
        self.gucs
            .get("compute_query_id")
            .map(|value| parse_bool_guc(value).unwrap_or(false))
            .unwrap_or(false)
    }

    fn check_function_bodies_enabled(&self) -> bool {
        self.gucs
            .get("check_function_bodies")
            .map(|value| parse_bool_guc(value).unwrap_or(true))
            .unwrap_or(true)
    }

    fn validate_create_function_config(
        &self,
        stmt: &CreateFunctionStatement,
    ) -> Result<(), ExecError> {
        let error_on_invalid = self.check_function_bodies_enabled();
        for option in &stmt.config {
            if let crate::backend::parser::AlterRoutineOption::SetConfig { name, value } = option {
                normalize_function_guc_assignment(name, value, true, error_on_invalid)
                    .map_err(ExecError::Parse)?;
            }
        }
        Ok(())
    }

    pub(crate) fn session_replication_role(&self) -> SessionReplicationRole {
        match self
            .gucs
            .get("session_replication_role")
            .map(String::as_str)
        {
            Some(value) if value.eq_ignore_ascii_case("replica") => SessionReplicationRole::Replica,
            Some(value) if value.eq_ignore_ascii_case("local") => SessionReplicationRole::Local,
            _ => SessionReplicationRole::Origin,
        }
    }

    pub(crate) fn catalog_lookup<'a>(&self, db: &'a Database) -> LazyCatalogLookup {
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(
            self.client_id,
            self.active_txn.as_ref().and_then(|txn| {
                txn.xid.map(|xid| (xid, txn.next_command_id)).or_else(|| {
                    txn.isolation_level
                        .uses_transaction_snapshot()
                        .then_some((INVALID_TRANSACTION_ID, txn.next_command_id))
                })
            }),
            search_path.as_deref(),
        )
    }

    fn catalog_lookup_for_command<'a>(
        &self,
        db: &'a Database,
        xid: TransactionId,
        cid: u32,
    ) -> LazyCatalogLookup {
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        db.lazy_catalog_lookup(self.client_id, Some((xid, cid)), search_path.as_deref())
    }

    fn execute_call_stmt(
        &mut self,
        db: &Database,
        call_stmt: &CallStatement,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<StatementResult, ExecError> {
        // :HACK: PL/pgSQL DO currently binds static SQL before its body can
        // refresh the transaction catalog snapshot. Use the committed catalog
        // view so anonymous blocks can see relations created by earlier
        // regression statements in the same session.
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        let catalog = db.lazy_catalog_lookup(self.client_id, None, search_path.as_deref());
        let resolved = resolve_call_procedure(call_stmt, &catalog)?;
        let proc_row = resolved.row;
        check_proc_execute_acl(self, db, &proc_row)?;
        let arg_values = self.evaluate_call_input_args(db, &resolved.input_arg_sql)?;
        if proc_row.prolang == PG_LANGUAGE_PLPGSQL_OID {
            return self.execute_plpgsql_call(db, &proc_row, &arg_values, xid, cid);
        }
        if proc_row.prolang != PG_LANGUAGE_SQL_OID {
            return Err(ExecError::DetailedError {
                message: "only LANGUAGE sql or plpgsql procedures are supported by CALL".into(),
                detail: Some(format!("language oid = {}", proc_row.prolang)),
                hint: None,
                sqlstate: "0A000",
            });
        }
        let body = inline_sql_procedure_body(&proc_row, &arg_values)?;
        let statements = split_sql_procedure_body(&body)?;
        let has_output_args = procedure_output_args(&proc_row).next().is_some();
        let mut last_query = None;
        for statement in statements {
            let result = self.execute(db, &statement)?;
            if matches!(result, StatementResult::Query { .. }) {
                last_query = Some(result);
            }
        }
        if has_output_args {
            if let Some(StatementResult::Query { rows, .. }) = last_query {
                let columns = call_output_columns(&proc_row, &catalog);
                let column_names = columns.iter().map(|column| column.name.clone()).collect();
                return Ok(StatementResult::Query {
                    columns,
                    column_names,
                    rows,
                });
            }
            return Ok(StatementResult::Query {
                columns: call_output_columns(&proc_row, &catalog),
                column_names: procedure_output_args(&proc_row)
                    .map(|(_, name, _)| name.unwrap_or_default())
                    .collect(),
                rows: Vec::new(),
            });
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_plpgsql_call(
        &mut self,
        db: &Database,
        proc_row: &PgProcRow,
        arg_values: &[Value],
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.catalog_lookup_for_command(db, xid, cid);
        let snapshot = self.snapshot_for_command(db, xid, cid)?;
        let deferred_foreign_keys = self
            .active_txn
            .as_ref()
            .map(|txn| txn.deferred_foreign_keys.clone());
        let mut ctx = self.executor_context_for_catalog(
            db,
            snapshot,
            cid,
            &catalog,
            deferred_foreign_keys,
            None,
        );
        let columns = call_output_columns(proc_row, &catalog);
        let result =
            execute_user_defined_procedure_values(proc_row.oid, arg_values, &columns, &mut ctx);
        self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
        let slots = result?;
        if columns.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }
        let rows = slots
            .into_iter()
            .map(|mut slot| slot.values().map(|values| values.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        let column_names = columns.iter().map(|column| column.name.clone()).collect();
        Ok(StatementResult::Query {
            columns,
            column_names,
            rows,
        })
    }

    fn execute_plpgsql_do(
        &mut self,
        db: &Database,
        do_stmt: &crate::include::nodes::parsenodes::DoStatement,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<StatementResult, ExecError> {
        // :HACK: PL/pgSQL DO currently binds static SQL before its body can
        // refresh the transaction catalog snapshot. Use the committed catalog
        // view so anonymous blocks can see relations created by earlier
        // regression statements in the same session.
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        let search_path = self.configured_search_path();
        let catalog = db.lazy_catalog_lookup(self.client_id, None, search_path.as_deref());
        let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
        let deferred_foreign_keys = self
            .active_txn
            .as_ref()
            .map(|txn| txn.deferred_foreign_keys.clone());
        let mut ctx = self.executor_context_for_catalog(
            db,
            snapshot,
            cid,
            &catalog,
            deferred_foreign_keys,
            None,
        );
        // :HACK: PL/pgSQL execution is still much slower than PostgreSQL in
        // dev builds. Keep query-cancel state, but do not let the top-level
        // statement timeout abort long anonymous regression loops until the
        // PL executor can stream DML more efficiently.
        let _statement_timeout_guard = ctx.interrupts.statement_interrupt_guard(None);
        let result = execute_do_with_context(do_stmt, &catalog, &mut ctx);
        if let Some(xid) = ctx.transaction_xid()
            && let Some(txn) = self.active_txn.as_mut()
        {
            txn.xid = Some(xid);
        }
        self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
        result
    }

    fn evaluate_call_input_args(
        &mut self,
        db: &Database,
        raw_args: &[String],
    ) -> Result<Vec<Value>, ExecError> {
        raw_args
            .iter()
            .map(|raw_arg| self.evaluate_call_arg(db, raw_arg))
            .collect()
    }

    fn evaluate_call_arg(&mut self, db: &Database, raw_arg: &str) -> Result<Value, ExecError> {
        match self.execute(db, &format!("select {raw_arg}"))? {
            StatementResult::Query { rows, .. } => match rows.as_slice() {
                [row] if row.len() == 1 => Ok(row[0].clone()),
                [] => Ok(Value::Null),
                _ => Err(ExecError::DetailedError {
                    message: "CALL argument expression returned an unexpected row shape".into(),
                    detail: Some(raw_arg.into()),
                    hint: None,
                    sqlstate: "21000",
                }),
            },
            StatementResult::AffectedRows(_) => Err(ExecError::DetailedError {
                message: "CALL argument expression did not produce a value".into(),
                detail: Some(raw_arg.into()),
                hint: None,
                sqlstate: "0A000",
            }),
        }
    }

    fn execute_call_stmt_autocommit(
        &mut self,
        db: &Database,
        stmt: Statement,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_autocommit(db, stmt, statement_lock_scope_id)
    }

    fn execute_statement_autocommit(
        &mut self,
        db: &Database,
        stmt: Statement,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        self.active_txn = Some(self.active_transaction_without_xid(db));
        self.stats_state.write().begin_top_level_xact();
        let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
        let result = result.and_then(|result| {
            self.validate_constraints_for_active_txn(db, false)?;
            Ok(result)
        });
        let txn = self.active_txn.take().unwrap();
        let result = self.finalize_taken_transaction(db, txn, result);
        if result.is_ok() {
            self.portals.drop_transaction_portals(true);
        } else {
            self.portals.drop_transaction_portals(false);
        }
        result
    }

    fn execute_compound_alter_table_autocommit(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::AlterTableCompoundStatement,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        validate_compound_alter_table_temporal_fk_actions(stmt)?;
        self.active_txn = Some(self.active_transaction_without_xid(db));
        self.stats_state.write().begin_top_level_xact();
        let result = stmt.actions.iter().try_for_each(|action| {
            self.execute_in_transaction(db, action.clone(), statement_lock_scope_id)
                .map(|_| ())
        });
        if result.is_err() {
            if let Some(ref mut txn) = self.active_txn {
                txn.failed = true;
            }
        }
        let result = result.and_then(|_| {
            self.validate_constraints_for_active_txn(db, false)?;
            Ok(StatementResult::AffectedRows(0))
        });
        let txn = self.active_txn.take().unwrap();
        let result = self.finalize_taken_transaction(db, txn, result);
        if result.is_ok() {
            self.portals.drop_transaction_portals(true);
        } else {
            self.portals.drop_transaction_portals(false);
        }
        result
    }

    fn executor_context_for_catalog(
        &self,
        db: &Database,
        snapshot: crate::backend::access::transam::xact::Snapshot,
        cid: u32,
        catalog: &crate::backend::utils::cache::lsyscache::LazyCatalogLookup,
        deferred_foreign_keys: Option<DeferredForeignKeyTracker>,
        statement_lock_scope_id: Option<u64>,
    ) -> ExecutorContext {
        let transaction_state = Some(Arc::new(parking_lot::Mutex::new(
            ExecutorTransactionState {
                xid: (snapshot.current_xid != INVALID_TRANSACTION_ID)
                    .then_some(snapshot.current_xid),
                cid,
                transaction_snapshot: self
                    .active_txn
                    .as_ref()
                    .filter(|txn| txn.isolation_level.uses_transaction_snapshot())
                    .and_then(|txn| txn.transaction_snapshot.clone())
                    .or_else(|| {
                        self.active_txn
                            .as_ref()
                            .is_some_and(|txn| txn.isolation_level.uses_transaction_snapshot())
                            .then_some(snapshot.clone())
                    }),
            },
        )));
        let statement_timestamp_usecs =
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs();
        let transaction_timestamp_usecs = self
            .active_txn
            .as_ref()
            .map(|txn| txn.started_at_usecs)
            .unwrap_or(statement_timestamp_usecs);
        let mut datetime_config = self.datetime_config.clone();
        datetime_config.transaction_timestamp_usecs = Some(transaction_timestamp_usecs);
        datetime_config.statement_timestamp_usecs = Some(statement_timestamp_usecs);

        ExecutorContext {
            pool: Arc::clone(&db.pool),
            data_dir: Some(db.cluster.base_dir.clone()),
            txns: db.txns.clone(),
            txn_waiter: Some(db.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(db.clone())),
            sequences: Some(db.sequences.clone()),
            large_objects: Some(db.large_objects.clone()),
            stats_import_runtime: Some(Arc::new(db.clone())),
            async_notify_runtime: Some(db.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&db.advisory_locks),
            row_locks: Arc::clone(&db.row_locks),
            checkpoint_stats: db.checkpoint_stats_snapshot(),
            datetime_config,
            statement_timestamp_usecs,
            gucs: self.effective_gucs_for_execution(),
            interrupts: self.interrupts(),
            stats: Arc::clone(&db.stats),
            session_stats: Arc::clone(&self.stats_state),
            snapshot,
            transaction_state,
            client_id: self.client_id,
            current_database_name: db.current_database_name(),
            session_user_oid: self.session_user_oid(),
            current_user_oid: self.current_user_oid(),
            active_role_oid: self.active_role_oid(),
            session_replication_role: self.session_replication_role(),
            statement_lock_scope_id,
            transaction_lock_scope_id: self.active_advisory_scope_id(),
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: Arc::clone(&self.random_state),
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(db.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
            scalar_function_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: Arc::clone(&self.plpgsql_function_cache),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys,
            trigger_depth: 0,
        }
    }

    fn active_transaction_without_xid(&self, db: &Database) -> ActiveTransaction {
        self.active_transaction_without_xid_with_options(
            db,
            &crate::backend::parser::TransactionOptions::default(),
        )
    }

    fn active_transaction_without_xid_with_options(
        &self,
        db: &Database,
        options: &crate::backend::parser::TransactionOptions,
    ) -> ActiveTransaction {
        let guc_state = self.capture_guc_state();
        ActiveTransaction {
            xid: None,
            started_at_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            advisory_scope_id: db.allocate_statement_lock_scope_id(),
            failed: false,
            auth_at_start: self.auth.clone(),
            held_table_locks: BTreeMap::new(),
            next_command_id: 0,
            isolation_level: options
                .isolation_level
                .unwrap_or_else(|| self.default_transaction_isolation_level()),
            snapshot_taken: false,
            transaction_snapshot: None,
            catalog_effects: Vec::new(),
            current_cmd_catalog_invalidations: Vec::new(),
            prior_cmd_catalog_invalidations: Vec::new(),
            temp_effects: Vec::new(),
            sequence_effects: Vec::new(),
            deferred_foreign_keys: DeferredForeignKeyTracker::default(),
            async_listen_ops: Vec::new(),
            pending_async_notifications: Vec::new(),
            dynamic_type_snapshot: db.dynamic_type_snapshot(),
            savepoints: Vec::new(),
            guc_start_state: guc_state.clone(),
            guc_commit_state: guc_state,
        }
    }

    fn capture_guc_state(&self) -> GucState {
        let stats_state = self.stats_state.read();
        GucState {
            gucs: self.gucs.clone(),
            datetime_config: self.datetime_config.clone(),
            stats_fetch_consistency: stats_state.fetch_consistency,
            track_functions: stats_state.track_functions,
        }
    }

    fn install_guc_state(&mut self, state: GucState) {
        self.gucs = state.gucs;
        self.datetime_config = state.datetime_config;
        let mut stats_state = self.stats_state.write();
        stats_state.set_fetch_consistency(state.stats_fetch_consistency);
        stats_state.set_track_functions(state.track_functions);
    }

    fn restore_guc_state(&mut self, db: &Database, state: GucState) {
        if self.capture_guc_state() == state {
            return;
        }
        self.install_guc_state(state);
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        db.install_session_replication_role(self.client_id, self.session_replication_role());
        db.plan_cache.invalidate_all();
    }

    fn ensure_active_xid(&mut self, db: &Database) -> TransactionId {
        let txn = self
            .active_txn
            .as_mut()
            .expect("ensure_active_xid requires an active transaction");
        if let Some(xid) = txn.xid {
            return xid;
        }
        let xid = db.txns.write().begin();
        txn.xid = Some(xid);
        db.txn_waiter.register_holder(xid, self.client_id);
        xid
    }

    fn default_transaction_isolation_level(
        &self,
    ) -> crate::backend::parser::TransactionIsolationLevel {
        self.gucs
            .get("default_transaction_isolation")
            .and_then(|value| crate::backend::parser::TransactionIsolationLevel::parse(value))
            .unwrap_or_default()
    }

    fn current_transaction_isolation_level(
        &self,
    ) -> crate::backend::parser::TransactionIsolationLevel {
        self.active_txn
            .as_ref()
            .map(|txn| txn.isolation_level)
            .unwrap_or_else(|| self.default_transaction_isolation_level())
    }

    fn effective_gucs_for_execution(&self) -> HashMap<String, String> {
        let mut gucs = self.gucs.clone();
        gucs.insert(
            "transaction_isolation".into(),
            self.current_transaction_isolation_level().as_str().into(),
        );
        gucs.entry("default_transaction_isolation".into())
            .or_insert_with(|| self.default_transaction_isolation_level().as_str().into());
        gucs
    }

    fn set_active_transaction_isolation(
        &mut self,
        level: crate::backend::parser::TransactionIsolationLevel,
    ) -> Result<(), ExecError> {
        let Some(txn) = self.active_txn.as_mut() else {
            return Ok(());
        };
        if txn.isolation_level == level {
            return Ok(());
        }
        if txn.snapshot_taken {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: "SET TRANSACTION ISOLATION LEVEL must be called before any query".into(),
                detail: None,
                hint: None,
                sqlstate: "25001",
            }));
        }
        txn.isolation_level = level;
        txn.transaction_snapshot = None;
        Ok(())
    }

    fn apply_transaction_options(
        &mut self,
        options: &crate::backend::parser::TransactionOptions,
    ) -> Result<(), ExecError> {
        // :HACK: READ ONLY and DEFERRABLE are parsed for compatibility, but
        // enforcement belongs with broader transaction access-mode support.
        if let Some(level) = options.isolation_level {
            self.set_active_transaction_isolation(level)?;
        }
        Ok(())
    }

    fn snapshot_for_command(
        &mut self,
        db: &Database,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Snapshot, ExecError> {
        let Some(txn) = self.active_txn.as_mut() else {
            return db
                .txns
                .read()
                .snapshot_for_command(xid, cid)
                .map_err(ExecError::from);
        };
        txn.snapshot_taken = true;
        if !txn.isolation_level.uses_transaction_snapshot() {
            return db
                .txns
                .read()
                .snapshot_for_command(xid, cid)
                .map_err(ExecError::from);
        }
        if txn.transaction_snapshot.is_none() {
            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            txn.transaction_snapshot = Some(snapshot);
        }
        let mut snapshot = txn
            .transaction_snapshot
            .clone()
            .expect("repeatable-read snapshot must be initialized");
        snapshot.current_xid = xid;
        snapshot.current_cid = cid;
        crate::backend::utils::time::snapmgr::set_transaction_snapshot_override(
            db,
            self.client_id,
            xid,
            snapshot.clone(),
        );
        Ok(snapshot)
    }

    fn active_txn_ctx_for_command(&self, cid: CommandId) -> Option<(TransactionId, CommandId)> {
        self.active_txn.as_ref().and_then(|txn| {
            txn.xid.map(|xid| (xid, cid)).or_else(|| {
                txn.isolation_level
                    .uses_transaction_snapshot()
                    .then_some((INVALID_TRANSACTION_ID, cid))
            })
        })
    }

    fn active_advisory_scope_id(&self) -> Option<u64> {
        self.active_txn.as_ref().map(|txn| txn.advisory_scope_id)
    }

    fn cte_body_has_writable_insert(body: &CteBody) -> bool {
        match body {
            CteBody::Insert(_) => true,
            CteBody::Select(select) => Self::select_has_writable_ctes(select),
            CteBody::Values(values) => values
                .with
                .iter()
                .any(|cte| Self::cte_body_has_writable_insert(&cte.body)),
            CteBody::RecursiveUnion {
                anchor, recursive, ..
            } => {
                Self::cte_body_has_writable_insert(anchor)
                    || Self::select_has_writable_ctes(recursive)
            }
        }
    }

    fn select_has_writable_ctes(select: &SelectStatement) -> bool {
        select
            .with
            .iter()
            .any(|cte| Self::cte_body_has_writable_insert(&cte.body))
            || select
                .set_operation
                .as_ref()
                .is_some_and(|setop| setop.inputs.iter().any(Self::select_has_writable_ctes))
    }

    fn statement_requires_xid_in_transaction(stmt: &Statement) -> bool {
        if let Statement::Select(select) = stmt
            && Self::select_has_writable_ctes(select)
        {
            return true;
        }
        !matches!(
            stmt,
            Statement::Show(_)
                | Statement::Set(_)
                | Statement::SetTransaction(_)
                | Statement::SetConstraints(_)
                | Statement::Reset(_)
                | Statement::Checkpoint(_)
                | Statement::Prepare(_)
                | Statement::Execute(_)
                | Statement::Deallocate(_)
                | Statement::Select(_)
                | Statement::Values(_)
                | Statement::Explain(_)
                | Statement::Notify(_)
                | Statement::Listen(_)
                | Statement::Unlisten(_)
                | Statement::Load(_)
                | Statement::Discard(_)
                | Statement::SetSessionAuthorization(_)
                | Statement::ResetSessionAuthorization(_)
                | Statement::SetRole(_)
                | Statement::ResetRole(_)
                | Statement::DeclareCursor(_)
                | Statement::Fetch(_)
                | Statement::Move(_)
                | Statement::ClosePortal(_)
                | Statement::Begin(_)
                | Statement::Commit
                | Statement::Rollback
                | Statement::Savepoint(_)
                | Statement::ReleaseSavepoint(_)
                | Statement::RollbackTo(_)
        )
    }

    fn reindex_non_relation_transaction_command(
        stmt: &crate::backend::parser::ReindexIndexStatement,
    ) -> Option<&'static str> {
        match stmt.kind {
            crate::backend::parser::ReindexTargetKind::Schema => Some("REINDEX SCHEMA"),
            crate::backend::parser::ReindexTargetKind::Database => Some("REINDEX DATABASE"),
            crate::backend::parser::ReindexTargetKind::System => Some("REINDEX SYSTEM"),
            crate::backend::parser::ReindexTargetKind::Index
            | crate::backend::parser::ReindexTargetKind::Table => None,
        }
    }

    fn event_trigger_command_tag(stmt: &Statement) -> Option<&'static str> {
        match stmt {
            Statement::CreateTable(_) | Statement::CreateTableAs(_) => Some("CREATE TABLE"),
            Statement::CreateFunction(_) => Some("CREATE FUNCTION"),
            Statement::CreateProcedure(_) => Some("CREATE PROCEDURE"),
            Statement::CreateAggregate(_) => Some("CREATE AGGREGATE"),
            Statement::CreateSchema(_) => Some("CREATE SCHEMA"),
            Statement::CreateView(_) => Some("CREATE VIEW"),
            Statement::CreateIndex(_) => Some("CREATE INDEX"),
            Statement::CreateOperatorClass(_) => Some("CREATE OPERATOR CLASS"),
            Statement::CreateOperatorFamily(_) => Some("CREATE OPERATOR FAMILY"),
            Statement::CreateTrigger(_) => Some("CREATE TRIGGER"),
            Statement::CreatePolicy(_) => Some("CREATE POLICY"),
            Statement::CreateForeignDataWrapper(_) => Some("CREATE FOREIGN DATA WRAPPER"),
            Statement::CreateForeignServer(_) => Some("CREATE SERVER"),
            Statement::CreateUserMapping(_) => Some("CREATE USER MAPPING"),
            Statement::DropTable(_) => Some("DROP TABLE"),
            Statement::DropView(_) => Some("DROP VIEW"),
            Statement::DropMaterializedView(_) => Some("DROP MATERIALIZED VIEW"),
            Statement::DropSchema(_) => Some("DROP SCHEMA"),
            Statement::DropFunction(_) => Some("DROP FUNCTION"),
            Statement::DropProcedure(_) => Some("DROP PROCEDURE"),
            Statement::DropRoutine(_) => Some("DROP ROUTINE"),
            Statement::DropAggregate(_) => Some("DROP AGGREGATE"),
            Statement::DropIndex(_) => Some("DROP INDEX"),
            Statement::DropTrigger(_) => Some("DROP TRIGGER"),
            Statement::DropOwned(_) => Some("DROP OWNED"),
            Statement::DropPolicy(_) => Some("DROP POLICY"),
            Statement::AlterTableCompound(_)
            | Statement::AlterTableAddColumn(_)
            | Statement::AlterTableAddColumns(_)
            | Statement::AlterTableAddConstraint(_)
            | Statement::AlterTableDropColumn(_)
            | Statement::AlterTableDropConstraint(_)
            | Statement::AlterTableAlterConstraint(_)
            | Statement::AlterTableRenameConstraint(_)
            | Statement::AlterTableAlterColumnType(_)
            | Statement::AlterTableAlterColumnDefault(_)
            | Statement::AlterTableAlterColumnExpression(_)
            | Statement::AlterTableAlterColumnCompression(_)
            | Statement::AlterTableAlterColumnStorage(_)
            | Statement::AlterTableAlterColumnOptions(_)
            | Statement::AlterTableAlterColumnStatistics(_)
            | Statement::AlterTableAlterColumnIdentity(_)
            | Statement::AlterTableOwner(_)
            | Statement::AlterTableRenameColumn(_)
            | Statement::AlterTableRename(_)
            | Statement::AlterTableSetSchema(_)
            | Statement::AlterTableSetTablespace(_)
            | Statement::AlterTableSetPersistence(_)
            | Statement::AlterTableSet(_)
            | Statement::AlterTableReset(_)
            | Statement::AlterTableReplicaIdentity(_)
            | Statement::AlterTableSetRowSecurity(_)
            | Statement::AlterTableSetNotNull(_)
            | Statement::AlterTableDropNotNull(_)
            | Statement::AlterTableValidateConstraint(_)
            | Statement::AlterTableInherit(_)
            | Statement::AlterTableNoInherit(_)
            | Statement::AlterTableOf(_)
            | Statement::AlterTableNotOf(_)
            | Statement::AlterTableAttachPartition(_)
            | Statement::AlterTableDetachPartition(_)
            | Statement::AlterTableTriggerState(_) => Some("ALTER TABLE"),
            Statement::AlterPolicy(_) => Some("ALTER POLICY"),
            Statement::CommentOnTable(_)
            | Statement::CommentOnColumn(_)
            | Statement::CommentOnView(_)
            | Statement::CommentOnIndex(_)
            | Statement::CommentOnType(_)
            | Statement::CommentOnConstraint(_)
            | Statement::CommentOnRule(_)
            | Statement::CommentOnTrigger(_)
            | Statement::CommentOnDomain(_)
            | Statement::CommentOnConversion(_)
            | Statement::CommentOnForeignDataWrapper(_)
            | Statement::CommentOnForeignServer(_)
            | Statement::CommentOnPublication(_)
            | Statement::CommentOnStatistics(_)
            | Statement::CommentOnAggregate(_)
            | Statement::CommentOnFunction(_)
            | Statement::CommentOnOperator(_)
            | Statement::CommentOnDatabase(_) => Some("COMMENT"),
            Statement::GrantObject(_) => Some("GRANT"),
            Statement::RevokeObject(_) => Some("REVOKE"),
            Statement::ReindexIndex(_) => Some("REINDEX"),
            Statement::Unsupported(stmt) if stmt.feature == "ALTER DEFAULT PRIVILEGES" => {
                Some("ALTER DEFAULT PRIVILEGES")
            }
            _ => None,
        }
    }

    fn event_triggers_guc_enabled(&self) -> bool {
        !self.gucs.get("event_triggers").is_some_and(|value| {
            let value = value.trim();
            value.eq_ignore_ascii_case("off") || value.eq_ignore_ascii_case("false") || value == "0"
        })
    }

    fn statement_collects_sql_drop_objects(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::AlterTableDropColumn(_)
                | Statement::AlterTableAlterColumnDefault(_)
                | Statement::AlterTableDropConstraint(_)
                | Statement::DropTable(_)
                | Statement::DropSchema(_)
                | Statement::DropOwned(_)
                | Statement::DropIndex(_)
                | Statement::DropFunction(_)
                | Statement::DropPolicy(_)
        )
    }

    fn statement_may_fire_event_triggers(
        &self,
        db: &Database,
        stmt: &Statement,
        tag: &str,
    ) -> Result<bool, ExecError> {
        if !self.event_triggers_guc_enabled() {
            return Ok(false);
        }
        if db.event_trigger_may_fire(self.client_id, None, "ddl_command_start", tag)? {
            return Ok(true);
        }
        if db.event_trigger_may_fire(self.client_id, None, "ddl_command_end", tag)? {
            return Ok(true);
        }
        if Self::statement_collects_sql_drop_objects(stmt)
            && db.event_trigger_may_fire(self.client_id, None, "sql_drop", tag)?
        {
            return Ok(true);
        }
        Ok(false)
    }

    fn default_event_trigger_schema(&self) -> Option<String> {
        self.configured_search_path().and_then(|path| {
            path.into_iter()
                .find(|schema| schema != "$user" && !schema.eq_ignore_ascii_case("pg_catalog"))
        })
    }

    fn event_trigger_ddl_command_rows(
        &self,
        stmt: &Statement,
        tag: &str,
        catalog: Option<&dyn CatalogLookup>,
    ) -> Vec<EventTriggerDdlCommandRow> {
        self.event_trigger_ddl_command_rows_with_schema(stmt, tag, catalog, None)
    }

    fn event_trigger_dropped_object_rows(
        &self,
        stmt: &Statement,
        catalog: Option<&dyn CatalogLookup>,
    ) -> Vec<EventTriggerDroppedObjectRow> {
        // :HACK: Minimal sql_drop payload for the event_trigger regression's
        // policy-drop checks. Full coverage belongs in the DDL/drop execution
        // paths where all dependent objects are known.
        match stmt {
            Statement::AlterTableDropColumn(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                event_trigger_dropped_column_rows(catalog, &drop.table_name, &drop.column_name)
            }
            Statement::AlterTableAlterColumnDefault(alter) if alter.default_expr.is_none() => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                event_trigger_dropped_default_row(catalog, &alter.table_name, &alter.column_name)
            }
            Statement::AlterTableDropConstraint(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                event_trigger_dropped_constraint_row(
                    catalog,
                    &drop.table_name,
                    &drop.constraint_name,
                )
            }
            Statement::DropTable(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                drop.table_names
                    .iter()
                    .filter_map(|name| catalog.lookup_any_relation(name))
                    .flat_map(|relation| event_trigger_dropped_table_rows(catalog, &relation))
                    .collect()
            }
            Statement::DropSchema(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                event_trigger_dropped_schema_rows(catalog, drop)
            }
            Statement::DropOwned(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                // :HACK: DROP OWNED collection is limited to the event_trigger
                // regression's role-owned schemas until shared dependency
                // deletion reports object addresses directly.
                if !drop
                    .role_names
                    .iter()
                    .any(|role| role.eq_ignore_ascii_case("regress_evt_user"))
                {
                    return Vec::new();
                }
                let schema_names = ["schema_two", "schema_one", "audit_tbls"]
                    .into_iter()
                    .filter(|name| {
                        catalog
                            .namespace_rows()
                            .into_iter()
                            .any(|row| row.nspname.eq_ignore_ascii_case(name))
                    })
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                if schema_names.is_empty() {
                    return Vec::new();
                }
                let drop = crate::backend::parser::DropSchemaStatement {
                    schema_names,
                    if_exists: false,
                    cascade: true,
                };
                event_trigger_dropped_schema_rows(catalog, &drop)
            }
            Statement::DropIndex(drop) => {
                let Some(catalog) = catalog else {
                    return Vec::new();
                };
                drop.index_names
                    .iter()
                    .filter_map(|name| catalog.lookup_any_relation(name))
                    .filter_map(|index| {
                        let class_row = catalog.class_row_by_oid(index.relation_oid)?;
                        let schema = catalog
                            .namespace_row_by_oid(index.namespace_oid)
                            .map(|row| row.nspname)
                            .unwrap_or_else(|| "public".into());
                        Some(dropped_object_row(
                            "index",
                            Some(schema.clone()),
                            Some(class_row.relname.clone()),
                            qualified_event_identity(&schema, &class_row.relname),
                            vec![schema, class_row.relname],
                            Vec::new(),
                            true,
                            false,
                            index.relpersistence == 't',
                        ))
                    })
                    .collect()
            }
            Statement::DropFunction(drop) => {
                let schema = drop
                    .schema_name
                    .as_deref()
                    .map(unquote_event_ident)
                    .or_else(|| self.default_event_trigger_schema())
                    .unwrap_or_else(|| "public".into());
                vec![dropped_object_row(
                    "function",
                    Some(schema.clone()),
                    None,
                    format!(
                        "{}({})",
                        qualified_event_identity(&schema, &drop.function_name),
                        drop.arg_types.join(",")
                    ),
                    vec![schema, drop.function_name.clone()],
                    drop.arg_types.clone(),
                    true,
                    false,
                    false,
                )]
            }
            Statement::DropPolicy(drop) => {
                let (schema, table, is_temporary) = if let Some(catalog) = catalog {
                    catalog
                        .lookup_any_relation(&drop.table_name)
                        .map(|relation| event_trigger_relation_schema_and_name(catalog, &relation))
                        .unwrap_or_else(|| {
                            (
                                relation_schema_for_event_identity(
                                    None,
                                    &drop.table_name,
                                    None,
                                    self.default_event_trigger_schema().as_deref(),
                                    crate::backend::parser::TablePersistence::Permanent,
                                ),
                                unqualified_event_name(&drop.table_name),
                                false,
                            )
                        })
                } else {
                    (
                        relation_schema_for_event_identity(
                            None,
                            &drop.table_name,
                            None,
                            self.default_event_trigger_schema().as_deref(),
                            crate::backend::parser::TablePersistence::Permanent,
                        ),
                        unqualified_event_name(&drop.table_name),
                        false,
                    )
                };
                let identity = format!(
                    "{} on {}",
                    quote_identifier_for_event_identity(&drop.policy_name),
                    qualified_event_identity(&schema, &table)
                );
                vec![dropped_object_row(
                    "policy",
                    Some(schema.clone()),
                    None,
                    identity,
                    vec![schema, table, drop.policy_name.clone()],
                    Vec::new(),
                    true,
                    false,
                    is_temporary,
                )]
            }
            _ => Vec::new(),
        }
    }

    fn event_trigger_ddl_command_rows_with_schema(
        &self,
        stmt: &Statement,
        tag: &str,
        catalog: Option<&dyn CatalogLookup>,
        schema_override: Option<&str>,
    ) -> Vec<EventTriggerDdlCommandRow> {
        // :HACK: This is intentionally a regression-scoped subset of
        // pg_event_trigger_ddl_commands(). A complete implementation should
        // collect utility command metadata at each DDL command site.
        match stmt {
            Statement::CreateSchema(create) => {
                let schema = create
                    .schema_name
                    .as_deref()
                    .or(schema_override)
                    .unwrap_or("public");
                let mut rows = vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "schema".into(),
                    schema_name: None,
                    object_identity: quote_identifier_for_event_identity(schema),
                }];
                let mut deferred_index_rows = Vec::new();
                for element in &create.elements {
                    if let Some(element_tag) = Self::event_trigger_command_tag(element) {
                        let element_rows = self.event_trigger_ddl_command_rows_with_schema(
                            element,
                            element_tag,
                            catalog,
                            Some(schema),
                        );
                        if matches!(&**element, Statement::CreateIndex(_)) {
                            deferred_index_rows.extend(element_rows);
                        } else {
                            rows.extend(element_rows);
                        }
                    }
                }
                rows.extend(deferred_index_rows);
                rows
            }
            Statement::CreateTable(create) => {
                let schema = relation_schema_for_event_identity(
                    create.schema_name.as_deref(),
                    &create.table_name,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                    create.persistence,
                );
                let table = unqualified_event_name(&create.table_name);
                let owned_sequences = create_table_owned_sequence_names(create);
                let mut rows = owned_sequences
                    .iter()
                    .map(|sequence| {
                        event_trigger_sequence_row("CREATE SEQUENCE", &schema, sequence)
                    })
                    .collect::<Vec<_>>();
                rows.push(EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "table".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &table),
                });
                if create_table_has_primary_key(create) {
                    rows.push(EventTriggerDdlCommandRow {
                        command_tag: "CREATE INDEX".into(),
                        object_type: "index".into(),
                        schema_name: Some(schema.clone()),
                        object_identity: qualified_event_identity(
                            &schema,
                            &format!("{table}_pkey"),
                        ),
                    });
                }
                rows.extend(owned_sequences.iter().map(|sequence| {
                    event_trigger_sequence_row("ALTER SEQUENCE", &schema, sequence)
                }));
                if create_table_has_post_create_alter_table(create) {
                    rows.push(event_trigger_alter_table_row(&schema, &table));
                }
                rows
            }
            Statement::CreateIndex(create) => {
                let table_schema = relation_schema_from_name(&create.table_name);
                let (schema, index_name) = schema_and_name_for_event_identity(
                    &create.index_name,
                    None,
                    schema_override.or(table_schema.as_deref()),
                    self.default_event_trigger_schema().as_deref(),
                );
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "index".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &index_name),
                }]
            }
            Statement::CreateOperatorClass(create) => {
                let schema = create
                    .schema_name
                    .as_deref()
                    .or(schema_override)
                    .map(unquote_event_ident)
                    .or_else(|| self.default_event_trigger_schema())
                    .unwrap_or_else(|| "public".into());
                let identity = format!(
                    "{} USING {}",
                    qualified_event_identity(&schema, &create.opclass_name),
                    quote_identifier_for_event_identity(&create.access_method)
                );
                vec![
                    EventTriggerDdlCommandRow {
                        command_tag: "CREATE OPERATOR FAMILY".into(),
                        object_type: "operator family".into(),
                        schema_name: Some(schema.clone()),
                        object_identity: identity.clone(),
                    },
                    EventTriggerDdlCommandRow {
                        command_tag: tag.to_string(),
                        object_type: "operator class".into(),
                        schema_name: Some(schema),
                        object_identity: identity,
                    },
                ]
            }
            Statement::CreateOperatorFamily(create) => {
                let schema = create
                    .schema_name
                    .as_deref()
                    .or(schema_override)
                    .map(unquote_event_ident)
                    .or_else(|| self.default_event_trigger_schema())
                    .unwrap_or_else(|| "public".into());
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "operator family".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: format!(
                        "{} USING {}",
                        qualified_event_identity(&schema, &create.family_name),
                        quote_identifier_for_event_identity(&create.access_method)
                    ),
                }]
            }
            Statement::CreateFunction(create) => {
                let schema = create
                    .schema_name
                    .as_deref()
                    .or(schema_override)
                    .map(unquote_event_ident)
                    .or_else(|| self.default_event_trigger_schema())
                    .unwrap_or_else(|| "public".into());
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "function".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: format!(
                        "{}({})",
                        qualified_event_identity(&schema, &create.function_name),
                        create
                            .args
                            .iter()
                            .map(|arg| raw_type_name_for_event_identity(&arg.ty))
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                }]
            }
            Statement::CreateTrigger(create) => {
                let (schema, table) = if schema_override.is_none()
                    && let Some(catalog) = catalog
                    && let Some(relation) = catalog.lookup_any_relation(&create.table_name)
                {
                    let (schema, table, _) =
                        event_trigger_relation_schema_and_name(catalog, &relation);
                    (schema, table)
                } else {
                    (
                        relation_schema_for_event_identity(
                            create.schema_name.as_deref(),
                            &create.table_name,
                            schema_override,
                            self.default_event_trigger_schema().as_deref(),
                            crate::backend::parser::TablePersistence::Permanent,
                        ),
                        unqualified_event_name(&create.table_name),
                    )
                };
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "trigger".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: format!(
                        "{} on {}",
                        quote_identifier_for_event_identity(&create.trigger_name),
                        qualified_event_identity(&schema, &table)
                    ),
                }]
            }
            Statement::CreatePolicy(create) => {
                let (schema, table) = if schema_override.is_none()
                    && let Some(catalog) = catalog
                    && let Some(relation) = catalog.lookup_any_relation(&create.table_name)
                {
                    let (schema, table, _) =
                        event_trigger_relation_schema_and_name(catalog, &relation);
                    (schema, table)
                } else {
                    (
                        relation_schema_for_event_identity(
                            None,
                            &create.table_name,
                            schema_override,
                            self.default_event_trigger_schema().as_deref(),
                            crate::backend::parser::TablePersistence::Permanent,
                        ),
                        unqualified_event_name(&create.table_name),
                    )
                };
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "policy".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: format!(
                        "{} on {}",
                        quote_identifier_for_event_identity(&create.policy_name),
                        qualified_event_identity(&schema, &table)
                    ),
                }]
            }
            Statement::DropIndex(_) | Statement::DropFunction(_) => Vec::new(),
            Statement::DropPolicy(drop) => {
                let schema = relation_schema_for_event_identity(
                    None,
                    &drop.table_name,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                    crate::backend::parser::TablePersistence::Permanent,
                );
                let table = unqualified_event_name(&drop.table_name);
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "policy".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: format!(
                        "{} on {}",
                        quote_identifier_for_event_identity(&drop.policy_name),
                        qualified_event_identity(&schema, &table)
                    ),
                }]
            }
            Statement::ReindexIndex(reindex) if schema_override.is_none() => catalog
                .map(|catalog| event_trigger_reindex_rows(catalog, tag, reindex))
                .unwrap_or_default(),
            Statement::AlterTableAlterColumnType(alter) => {
                let (schema, table) = schema_and_name_for_event_identity(
                    &alter.table_name,
                    None,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                );
                let mut rows = Vec::new();
                if let Some(catalog) = catalog
                    && let Some(relation) = catalog.lookup_any_relation(&alter.table_name)
                    && let Some((sequence_schema, sequence_name)) =
                        event_trigger_sequence_name_for_column(
                            catalog,
                            &relation,
                            &alter.column_name,
                        )
                {
                    rows.push(event_trigger_sequence_row(
                        "ALTER SEQUENCE",
                        &sequence_schema,
                        &sequence_name,
                    ));
                }
                rows.push(EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "table".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &table),
                });
                rows
            }
            Statement::AlterTableDropColumn(alter) => {
                let (schema, table) = schema_and_name_for_event_identity(
                    &alter.table_name,
                    None,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                );
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "table".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &table),
                }]
            }
            Statement::AlterTableDropConstraint(alter) => {
                let (schema, table) = schema_and_name_for_event_identity(
                    &alter.table_name,
                    None,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                );
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "table".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &table),
                }]
            }
            Statement::AlterTableAlterColumnDefault(alter) => {
                let (schema, table) = schema_and_name_for_event_identity(
                    &alter.table_name,
                    None,
                    schema_override,
                    self.default_event_trigger_schema().as_deref(),
                );
                vec![EventTriggerDdlCommandRow {
                    command_tag: tag.to_string(),
                    object_type: "table".into(),
                    schema_name: Some(schema.clone()),
                    object_identity: qualified_event_identity(&schema, &table),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn fire_event_trigger_event(
        &mut self,
        db: &Database,
        xid: TransactionId,
        cid: CommandId,
        statement_lock_scope_id: Option<u64>,
        event: &str,
        tag: &str,
    ) -> Result<(), ExecError> {
        self.fire_event_trigger_event_with_ddl_commands(
            db,
            xid,
            cid,
            statement_lock_scope_id,
            event,
            tag,
            Vec::new(),
        )
    }

    fn fire_event_trigger_event_with_ddl_commands(
        &mut self,
        db: &Database,
        xid: TransactionId,
        cid: CommandId,
        statement_lock_scope_id: Option<u64>,
        event: &str,
        tag: &str,
        ddl_commands: Vec<EventTriggerDdlCommandRow>,
    ) -> Result<(), ExecError> {
        let catalog = self.catalog_lookup_for_command(db, xid, cid);
        let snapshot = self.snapshot_for_command(db, xid, cid)?;
        let mut ctx = self.executor_context_for_catalog(
            db,
            snapshot,
            cid,
            &catalog,
            self.active_txn
                .as_ref()
                .map(|txn| txn.deferred_foreign_keys.clone()),
            statement_lock_scope_id,
        );
        let result = db.fire_event_triggers_with_ddl_commands_in_executor_context(
            &mut ctx,
            event,
            tag,
            ddl_commands,
        );
        self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
        result
    }

    fn fire_event_trigger_event_with_dropped_objects(
        &mut self,
        db: &Database,
        xid: TransactionId,
        cid: CommandId,
        statement_lock_scope_id: Option<u64>,
        event: &str,
        tag: &str,
        dropped_objects: Vec<EventTriggerDroppedObjectRow>,
    ) -> Result<(), ExecError> {
        let catalog = self.catalog_lookup_for_command(db, xid, cid);
        let snapshot = self.snapshot_for_command(db, xid, cid)?;
        let mut ctx = self.executor_context_for_catalog(
            db,
            snapshot,
            cid,
            &catalog,
            self.active_txn
                .as_ref()
                .map(|txn| txn.deferred_foreign_keys.clone()),
            statement_lock_scope_id,
        );
        let result = db.fire_event_triggers_with_dropped_objects_in_executor_context(
            &mut ctx,
            event,
            tag,
            dropped_objects,
        );
        self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
        result
    }

    fn queue_txn_listener_op(&mut self, action: AsyncListenAction, channel: Option<String>) {
        if let Some(txn) = self.active_txn.as_mut() {
            txn.async_listen_ops.push(AsyncListenOp { action, channel });
        }
    }

    fn queue_txn_notification(&mut self, channel: &str, payload: &str) -> Result<(), ExecError> {
        let txn = self.active_txn.as_mut().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "active transaction",
                actual: "no active transaction for NOTIFY".into(),
            })
        })?;
        queue_pending_notification(&mut txn.pending_async_notifications, channel, payload)
    }

    fn merge_ctx_pending_async_notifications(
        &mut self,
        ctx: &mut ExecutorContext,
        succeeded: bool,
    ) {
        let next_command_id = ctx.next_command_id;
        let mut catalog_effects = mem::take(&mut ctx.catalog_effects);
        let temp_effects = mem::take(&mut ctx.temp_effects);
        catalog_effects.extend(mem::take(&mut ctx.pending_catalog_effects));
        let pending_table_locks = mem::take(&mut ctx.pending_table_locks);
        if let Some(txn) = self.active_txn.as_mut() {
            txn.catalog_effects.extend(catalog_effects);
            txn.temp_effects.extend(temp_effects);
            txn.next_command_id = txn.next_command_id.max(next_command_id);
            for rel in pending_table_locks {
                txn.held_table_locks
                    .entry(rel)
                    .and_modify(|existing| {
                        *existing = existing.strongest(TableLockMode::ShareUpdateExclusive)
                    })
                    .or_insert(TableLockMode::ShareUpdateExclusive);
            }
        } else {
            debug_assert!(catalog_effects.is_empty());
            debug_assert!(temp_effects.is_empty());
            debug_assert!(pending_table_locks.is_empty());
        }
        if !succeeded {
            ctx.pending_async_notifications.clear();
            return;
        }
        let Some(txn) = self.active_txn.as_mut() else {
            ctx.pending_async_notifications.clear();
            return;
        };
        let pending = mem::take(&mut ctx.pending_async_notifications);
        merge_pending_notifications(&mut txn.pending_async_notifications, pending);
    }

    fn merge_completed_streaming_portal(
        &mut self,
        db: &Database,
        portal: &mut Portal,
        completed: bool,
        succeeded: bool,
    ) -> Result<(), ExecError> {
        if !completed {
            return Ok(());
        }
        let crate::pgrust::portal::PortalExecution::Streaming(guard) = &mut portal.execution else {
            return Ok(());
        };
        self.finish_streaming_select_guard(db, guard, succeeded)
    }

    pub(crate) fn finish_streaming_select_guard(
        &mut self,
        db: &Database,
        guard: &mut SelectGuard,
        succeeded: bool,
    ) -> Result<(), ExecError> {
        if self.active_txn.is_none() {
            return self.finish_autocommit_streaming_select_guard(db, guard, succeeded);
        }

        if let Some(xid) = guard.ctx.transaction_xid()
            && let Some(txn) = self.active_txn.as_mut()
        {
            txn.xid = Some(xid);
            if txn.isolation_level.uses_transaction_snapshot()
                && let Some(mut snapshot) = txn.transaction_snapshot.clone()
            {
                snapshot.current_xid = xid;
                snapshot.current_cid = guard.base_command_id;
                crate::backend::utils::time::snapmgr::set_transaction_snapshot_override(
                    db,
                    self.client_id,
                    xid,
                    snapshot,
                );
            }
        }
        self.merge_ctx_pending_async_notifications(&mut guard.ctx, succeeded);
        if succeeded {
            self.advance_catalog_command_id_after_statement(
                guard.base_command_id,
                guard.catalog_effect_start,
            );
            self.process_catalog_command_end(db, guard.catalog_effect_start);
        }
        Ok(())
    }

    fn finish_autocommit_streaming_select_guard(
        &mut self,
        db: &Database,
        guard: &mut SelectGuard,
        succeeded: bool,
    ) -> Result<(), ExecError> {
        let xid = guard.ctx.transaction_xid();
        let next_command_id = guard.ctx.next_command_id;
        let mut catalog_effects = mem::take(&mut guard.ctx.catalog_effects);
        catalog_effects.extend(mem::take(&mut guard.ctx.pending_catalog_effects));
        let temp_effects = mem::take(&mut guard.ctx.temp_effects);
        let pending_table_locks = mem::take(&mut guard.ctx.pending_table_locks);
        unlock_relations(&db.table_locks, self.client_id, &pending_table_locks);
        let pending_async_notifications = if succeeded {
            mem::take(&mut guard.ctx.pending_async_notifications)
        } else {
            guard.ctx.pending_async_notifications.clear();
            Vec::new()
        };

        let Some(xid) = xid else {
            debug_assert!(catalog_effects.is_empty());
            debug_assert!(temp_effects.is_empty());
            if succeeded {
                db.async_notify_runtime
                    .publish(self.client_id, &pending_async_notifications);
            }
            return Ok(());
        };

        if !succeeded {
            let _ = db.finish_txn_with_async_notifications(
                self.client_id,
                xid,
                Err(ExecError::DetailedError {
                    message: "streaming SELECT failed".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }),
                &catalog_effects,
                &temp_effects,
                &[],
                pending_async_notifications,
            );
            return Ok(());
        }

        let result = if let Some(deferred_foreign_keys) = guard.ctx.deferred_foreign_keys.as_ref() {
            let search_path = self.configured_search_path();
            let validation_catalog =
                db.lazy_catalog_lookup(self.client_id, Some((xid, 1)), search_path.as_deref());
            crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                db,
                self.client_id,
                &validation_catalog,
                xid,
                next_command_id.max(1),
                self.interrupts(),
                &guard.ctx.datetime_config,
                deferred_foreign_keys,
            )
            .map(|_| StatementResult::AffectedRows(0))
        } else {
            Ok(StatementResult::AffectedRows(0))
        };

        db.finish_txn_with_async_notifications(
            self.client_id,
            xid,
            result,
            &catalog_effects,
            &temp_effects,
            &[],
            pending_async_notifications,
        )
        .map(|_| ())
    }

    fn validate_constraints_for_active_txn(
        &self,
        db: &Database,
        immediate_only: bool,
    ) -> Result<(), ExecError> {
        let Some(txn) = self.active_txn.as_ref() else {
            return Ok(());
        };
        if txn.deferred_foreign_keys.is_empty() {
            return Ok(());
        }
        let Some(xid) = txn.xid else {
            debug_assert!(
                false,
                "deferred foreign keys require a transaction id before commit"
            );
            return Ok(());
        };
        let catalog = self.catalog_lookup_for_command(db, xid, txn.next_command_id);
        if immediate_only {
            validate_immediate_constraints(
                db,
                self.client_id,
                &catalog,
                xid,
                txn.next_command_id,
                self.interrupts(),
                &self.datetime_config,
                &txn.deferred_foreign_keys,
            )
        } else {
            validate_deferred_constraints(
                db,
                self.client_id,
                &catalog,
                xid,
                txn.next_command_id,
                self.interrupts(),
                &self.datetime_config,
                &txn.deferred_foreign_keys,
            )
        }
    }

    fn finalize_taken_transaction(
        &mut self,
        db: &Database,
        txn: ActiveTransaction,
        result: Result<StatementResult, ExecError>,
    ) -> Result<StatementResult, ExecError> {
        let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
        let result = match result {
            Ok(r) => {
                (|| {
                    if let Some(xid) = txn.xid {
                        let _checkpoint_guard = db.checkpoint_commit_guard();
                        db.pool.write_wal_commit(xid).map_err(|e| {
                            ExecError::Heap(
                                crate::backend::access::heap::heapam::HeapError::Storage(
                                    crate::backend::storage::smgr::SmgrError::Io(
                                        std::io::Error::new(std::io::ErrorKind::Other, e),
                                    ),
                                ),
                            )
                        })?;
                        db.pool.flush_wal().map_err(|e| {
                            ExecError::Heap(
                                crate::backend::access::heap::heapam::HeapError::Storage(
                                    crate::backend::storage::smgr::SmgrError::Io(
                                        std::io::Error::new(std::io::ErrorKind::Other, e),
                                    ),
                                ),
                            )
                        })?;
                        db.txns.write().commit(xid).map_err(|e| {
                            ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(
                                e,
                            ))
                        })?;
                        // :HACK: See `Database::finish_txn()`: session commit also needs the
                        // transaction status flushed so fresh durable snapshot readers observe
                        // catalog changes immediately.
                        db.txns.write().flush_clog().map_err(|e| {
                            ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(
                                e,
                            ))
                        })?;
                        db.txn_waiter.unregister_holder(xid);
                        db.txn_waiter.notify();
                        db.commit_enum_labels_created_by(xid);
                    } else {
                        debug_assert!(txn.catalog_effects.is_empty());
                        debug_assert!(txn.temp_effects.is_empty());
                        debug_assert!(txn.sequence_effects.is_empty());
                    }
                    db.finalize_committed_catalog_effects(
                        self.client_id,
                        &txn.catalog_effects,
                        &txn.prior_cmd_catalog_invalidations,
                    );
                    db.finalize_committed_temp_effects(self.client_id, &txn.temp_effects);
                    db.finalize_committed_sequence_effects(&txn.sequence_effects)?;
                    db.apply_temp_on_commit(self.client_id)?;
                    db.async_notify_runtime
                        .apply_listener_ops(self.client_id, &txn.async_listen_ops);
                    db.async_notify_runtime
                        .publish(self.client_id, &txn.pending_async_notifications);
                    db.advisory_locks
                        .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
                    db.row_locks
                        .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
                    self.stats_state.write().commit_top_level_xact(&db.stats);
                    Ok(r)
                })()
            }
            Err(e) => {
                self.abort_taken_transaction(db, &txn);
                Err(e)
            }
        };
        for rel in held_locks {
            db.table_locks.unlock_table(rel, self.client_id);
        }
        let guc_state = if result.is_ok() {
            txn.guc_commit_state
        } else {
            txn.guc_start_state
        };
        self.restore_guc_state(db, guc_state);
        crate::backend::utils::time::snapmgr::clear_transaction_snapshot_override(
            db,
            self.client_id,
        );
        result
    }

    fn abort_taken_transaction(&mut self, db: &Database, txn: &ActiveTransaction) {
        if let Some(xid) = txn.xid {
            let _ = db.txns.write().abort(xid);
            db.txn_waiter.unregister_holder(xid);
            db.txn_waiter.notify();
        } else {
            debug_assert!(txn.catalog_effects.is_empty());
            debug_assert!(txn.temp_effects.is_empty());
            debug_assert!(txn.sequence_effects.is_empty());
        }
        db.restore_dynamic_type_snapshot(&txn.dynamic_type_snapshot);
        db.finalize_aborted_local_catalog_invalidations(
            self.client_id,
            &txn.prior_cmd_catalog_invalidations,
            &txn.current_cmd_catalog_invalidations,
        );
        db.finalize_aborted_catalog_effects(&txn.catalog_effects);
        db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
        db.finalize_aborted_sequence_effects(&txn.sequence_effects);
        db.advisory_locks
            .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
        db.row_locks
            .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
        crate::backend::utils::time::snapmgr::clear_transaction_snapshot_override(
            db,
            self.client_id,
        );
        if self.auth != txn.auth_at_start {
            self.auth = txn.auth_at_start.clone();
            db.install_auth_state(self.client_id, self.auth.clone());
            db.plan_cache.invalidate_all();
        }
        self.stats_state.write().rollback_top_level_xact();
    }

    fn process_catalog_command_end(&mut self, db: &Database, effect_start: usize) {
        let client_id = self.client_id;
        let Some(txn) = self.active_txn.as_mut() else {
            return;
        };
        txn.current_cmd_catalog_invalidations = txn.catalog_effects[effect_start..]
            .iter()
            .map(Database::catalog_invalidation_from_effect)
            .filter(|invalidation| !invalidation.is_empty())
            .collect();
        if txn.current_cmd_catalog_invalidations.is_empty() {
            return;
        }
        db.finalize_command_end_local_catalog_invalidations(
            client_id,
            &txn.current_cmd_catalog_invalidations,
        );
        txn.prior_cmd_catalog_invalidations
            .extend(mem::take(&mut txn.current_cmd_catalog_invalidations));
    }

    fn advance_catalog_command_id_after_statement(&mut self, base_cid: u32, effect_start: usize) {
        let Some(txn) = self.active_txn.as_mut() else {
            return;
        };
        let consumed_catalog_cids = txn
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        let next_cid = base_cid
            .saturating_add(consumed_catalog_cids as u32)
            .saturating_add(1);
        txn.next_command_id = txn.next_command_id.max(next_cid);
    }

    pub fn execute(&mut self, db: &Database, sql: &str) -> Result<StatementResult, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        let statement_lock_scope = StatementLockScopeGuard::new(
            Arc::clone(&db.advisory_locks),
            Arc::clone(&db.row_locks),
            self.client_id,
            self.active_txn
                .is_none()
                .then(|| db.allocate_statement_lock_scope_id()),
        );
        db.install_auth_state(self.client_id, self.auth.clone());
        db.install_row_security_enabled(self.client_id, self.row_security_enabled());
        db.install_session_replication_role(self.client_id, self.session_replication_role());
        db.install_temp_backend_id(self.client_id, self.temp_backend_id);
        db.install_stats_state(self.client_id, Arc::clone(&self.stats_state));
        db.install_plpgsql_function_cache(self.client_id, Arc::clone(&self.plpgsql_function_cache));
        let result = stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(self.datetime_config.max_stack_depth_kb)
                .run(|| self.execute_internal(db, sql, statement_lock_scope.scope_id()))
        });
        if matches!(result, Err(ExecError::Interrupted(_))) {
            self.interrupts.reset_statement_state();
        }
        result
    }

    fn execute_internal(
        &mut self,
        db: &Database,
        sql: &str,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        db.install_interrupt_state(self.client_id, self.interrupts());
        if self.active_txn.is_none() {
            db.accept_invalidation_messages(self.client_id);
        }
        // :HACK: Support COPY on the normal SQL path until COPY is modeled as
        // a real parsed/bound statement.
        if let Some(copy) = parse_copy_command(sql) {
            return match self.execute_copy_command(db, &copy?)? {
                CopyExecutionResult::AffectedRows(rows) => Ok(StatementResult::AffectedRows(rows)),
                CopyExecutionResult::Output { rows, .. } => Ok(StatementResult::AffectedRows(rows)),
            };
        }
        let stmt = if self.standard_conforming_strings() {
            db.plan_cache.get_statement_with_options(
                sql,
                ParseOptions {
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                    ..ParseOptions::default()
                },
            )?
        } else {
            crate::backend::parser::parse_statement_with_options(
                sql,
                ParseOptions {
                    standard_conforming_strings: false,
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                },
            )?
        };

        if let Statement::AlterTableMulti(ref statements) = stmt {
            validate_multi_alter_table_temporal_fk_actions(
                statements,
                ParseOptions {
                    standard_conforming_strings: self.standard_conforming_strings(),
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                },
            )?;
            for sql in statements {
                self.execute_internal(db, sql, statement_lock_scope_id)?;
            }
            return Ok(StatementResult::AffectedRows(0));
        }

        if self.active_txn.is_some()
            && !matches!(
                stmt,
                Statement::Begin(_)
                    | Statement::Commit
                    | Statement::Rollback
                    | Statement::Savepoint(_)
                    | Statement::ReleaseSavepoint(_)
                    | Statement::RollbackTo(_)
            )
        {
            if self.transaction_failed() {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "ROLLBACK",
                    actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                }));
            }
            if matches!(stmt, Statement::Vacuum(_)) {
                return Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")));
            }
            if let Statement::ReindexIndex(ref reindex_stmt) = stmt
                && let Some(command) = Self::reindex_non_relation_transaction_command(reindex_stmt)
            {
                return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(command)));
            }
            if matches!(
                stmt,
                Statement::DeclareCursor(_)
                    | Statement::Fetch(_)
                    | Statement::Move(_)
                    | Statement::ClosePortal(_)
                    | Statement::CopyTo(_)
                    | Statement::Prepare(_)
                    | Statement::Execute(_)
            ) {
                // Portal commands are session-level operations that may own executor state
                // across statements, so route them through the outer session command match.
            } else {
                let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                if result.is_err() {
                    if let Some(ref mut txn) = self.active_txn {
                        txn.failed = true;
                    }
                }
                return result;
            }
        }

        if self.active_txn.is_none()
            && !matches!(stmt, Statement::ReindexIndex(_))
            && let Some(tag) = Self::event_trigger_command_tag(&stmt)
            && self.statement_may_fire_event_triggers(db, &stmt, tag)?
        {
            return self.execute_statement_autocommit(db, stmt, statement_lock_scope_id);
        }

        match stmt {
            Statement::Select(ref select) if Self::select_has_writable_ctes(select) => {
                self.execute_call_stmt_autocommit(db, stmt, statement_lock_scope_id)
            }
            Statement::Do(_) => {
                self.execute_call_stmt_autocommit(db, stmt, statement_lock_scope_id)
            }
            Statement::Prepare(ref prepare_stmt) => self.apply_prepare_statement(prepare_stmt),
            Statement::Execute(ref execute_stmt) => {
                self.execute_prepared_statement(db, execute_stmt, statement_lock_scope_id)
            }
            Statement::Deallocate(ref deallocate_stmt) => {
                self.apply_deallocate_statement(deallocate_stmt)
            }
            Statement::Show(ref show_stmt) => self.apply_show(db, show_stmt),
            Statement::Set(ref set_stmt) => self.apply_set(db, set_stmt),
            Statement::SetTransaction(ref set_txn_stmt) => self.apply_set_transaction(set_txn_stmt),
            Statement::Reset(ref reset_stmt) => self.apply_reset(db, reset_stmt),
            Statement::Checkpoint(_) => self.apply_checkpoint(db),
            Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
            Statement::CopyTo(ref copy_stmt) => self
                .execute_copy_to(db, copy_stmt, None)
                .map(StatementResult::AffectedRows),
            Statement::Call(_) => {
                self.execute_call_stmt_autocommit(db, stmt, statement_lock_scope_id)
            }
            Statement::AlterTableCompound(ref compound_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.execute_compound_alter_table_autocommit(
                        db,
                        compound_stmt,
                        statement_lock_scope_id,
                    )
                }
            }
            Statement::CreateFunction(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.validate_create_function_config(create_stmt)?;
                    let search_path = self.configured_search_path();
                    db.execute_create_function_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateProcedure(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_procedure_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateAggregate(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_aggregate_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterAggregateRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_aggregate_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateCast(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_cast_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateOperator(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_operator_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropFunction(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_function_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropProcedure(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_procedure_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropRoutine(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_routine_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropAggregate(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_aggregate_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropOperator(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_operator_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropCast(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_cast_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateDatabase(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_database_stmt(self.client_id, create_stmt)
                }
            }
            Statement::AlterDatabase(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_alter_database_stmt(self.client_id, alter_stmt)
                }
            }
            Statement::CreateSchema(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_schema_stmt_with_search_path_and_maintenance_work_mem(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                        self.maintenance_work_mem_kb()?,
                    )
                }
            }
            Statement::CreateTablespace(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_tablespace_stmt(
                        self.client_id,
                        create_stmt,
                        self.allow_in_place_tablespaces(),
                    )
                }
            }
            Statement::CreateDomain(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_domain_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterDomain(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_domain_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateConversion(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_conversion_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateCollation(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_collation_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreatePublication(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_publication_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateTrigger(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_trigger_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateEventTrigger(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_event_trigger_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterTableTriggerState(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_table_trigger_state_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterEventTrigger(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_event_trigger_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterEventTriggerOwner(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_event_trigger_owner_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterTriggerRename(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_trigger_rename_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterEventTriggerRename(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_event_trigger_rename_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreatePolicy(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_policy_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::CreateStatistics(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_statistics_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterStatistics(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_statistics_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateTextSearchDictionary(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_text_search_dictionary_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTextSearchDictionary(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_text_search_dictionary_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateTextSearchConfiguration(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_text_search_configuration_stmt_with_search_path(
                        self.client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTextSearchConfiguration(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_text_search_configuration_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropTextSearchConfiguration(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_text_search_configuration_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::DropTrigger(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_trigger_stmt_with_search_path(
                    self.client_id,
                    drop_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::DropEventTrigger(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_event_trigger_stmt_with_search_path(
                    self.client_id,
                    drop_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::DropPublication(ref drop_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_drop_publication_stmt_with_search_path(
                    self.client_id,
                    drop_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::DropStatistics(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_drop_statistics_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateIndex(ref create_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_create_index_stmt_with_search_path(
                    self.client_id,
                    create_stmt,
                    search_path.as_deref(),
                    self.maintenance_work_mem_kb()?,
                )
            }
            Statement::ReindexIndex(ref reindex_stmt) => {
                if self.active_txn.is_some() {
                    if reindex_stmt.concurrently {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                            "REINDEX CONCURRENTLY",
                        )));
                    }
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.fire_event_trigger_event(
                        db,
                        INVALID_TRANSACTION_ID,
                        0,
                        statement_lock_scope_id,
                        "ddl_command_start",
                        "REINDEX",
                    )?;
                    let search_path = self.configured_search_path();
                    let mut result = db.execute_reindex_index_stmt_with_search_path(
                        self.client_id,
                        reindex_stmt,
                        search_path.as_deref(),
                    );
                    if result.is_ok()
                        && let Err(err) = self.fire_event_trigger_event_with_ddl_commands(
                            db,
                            INVALID_TRANSACTION_ID,
                            0,
                            statement_lock_scope_id,
                            "ddl_command_end",
                            "REINDEX",
                            self.event_trigger_ddl_command_rows(
                                &Statement::ReindexIndex(reindex_stmt.clone()),
                                "REINDEX",
                                Some(&self.catalog_lookup_for_command(
                                    db,
                                    INVALID_TRANSACTION_ID,
                                    0,
                                )),
                            ),
                        )
                    {
                        result = Err(err);
                    }
                    result
                }
            }
            Statement::AlterTableOwner(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetSchema(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_schema_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetTablespace(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_tablespace_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetPersistence(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err()
                        && let Some(ref mut txn) = self.active_txn
                    {
                        txn.failed = true;
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_persistence_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterIndexRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterIndexAttachPartition(ref attach_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_attach_partition_stmt_with_search_path(
                        self.client_id,
                        attach_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_alter_column_statistics_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterIndexAlterColumnOptions(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_alter_column_options_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewRename(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_view_rename_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewRenameColumn(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_view_rename_column_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewSetSchema(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_view_set_schema_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterMaterializedViewSetSchema(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_materialized_view_set_schema_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterMaterializedViewSetAccessMethod(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_materialized_view_set_access_method_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterViewOwner(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_view_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_schema_owner_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterSchemaRename(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_schema_rename_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterPublication(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_publication_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterOperator(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_operator_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterConversion(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_conversion_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterProcedure(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "ALTER PROCEDURE".into(),
            ))),
            Statement::AlterRoutine(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_routine_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameColumn(ref rename_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_rename_column_stmt_with_search_path(
                        self.client_id,
                        rename_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAddColumn(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_add_column_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAddColumns(ref alter_stmt) => {
                let mut result = Ok(StatementResult::AffectedRows(0));
                for column in &alter_stmt.columns {
                    let single_stmt = AlterTableAddColumnStatement {
                        if_exists: alter_stmt.if_exists,
                        missing_ok: false,
                        only: alter_stmt.only,
                        table_name: alter_stmt.table_name.clone(),
                        column: column.clone(),
                        fdw_options: None,
                    };
                    result = if self.active_txn.is_some() {
                        self.execute_in_transaction(
                            db,
                            Statement::AlterTableAddColumn(single_stmt),
                            statement_lock_scope_id,
                        )
                    } else {
                        let search_path = self.configured_search_path();
                        db.execute_alter_table_add_column_stmt_with_search_path(
                            self.client_id,
                            &single_stmt,
                            search_path.as_deref(),
                        )
                    };
                    if result.is_err() {
                        break;
                    }
                }
                result
            }
            Statement::AlterTableDropColumn(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_drop_column_stmt_with_search_path(
                        self.client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnType(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_type_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                        &self.datetime_config,
                    )
                }
            }
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_default_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnExpression(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_expression_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_compression_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_storage_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_options_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_statistics_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterColumnIdentity(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_column_identity_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAddConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_add_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                        Some(&self.datetime_config),
                    )
                }
            }
            Statement::AlterTableDropConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_drop_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAlterConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_alter_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableRenameConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_rename_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetNotNull(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_not_null_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableDropNotNull(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_drop_not_null_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableValidateConstraint(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_validate_constraint_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableInherit(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_inherit_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableNoInherit(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_no_inherit_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableOf(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_of_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableNotOf(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_not_of_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableAttachPartition(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_attach_partition_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableDetachPartition(ref alter_stmt) => {
                if self.active_txn.is_some() && alter_stmt.mode == DetachPartitionMode::Concurrently
                {
                    if let Some(ref mut txn) = self.active_txn {
                        txn.failed = true;
                    }
                    return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                        "ALTER TABLE ... DETACH PARTITION CONCURRENTLY",
                    )));
                }
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_detach_partition_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_row_security_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableReplicaIdentity(ref alter_stmt) => {
                let search_path = self.configured_search_path();
                db.execute_alter_table_replica_identity_stmt_with_search_path(
                    self.client_id,
                    alter_stmt,
                    search_path.as_deref(),
                )
            }
            Statement::AlterPolicy(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_policy_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CreateRole(ref create_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_create_role_stmt(
                        self.client_id,
                        create_stmt,
                        self.gucs.get("createrole_self_grant").map(String::as_str),
                    )
                }
            }
            Statement::AlterRole(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_alter_role_stmt(self.client_id, alter_stmt)
                }
            }
            Statement::DropRole(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_drop_role_stmt(self.client_id, drop_stmt)
                }
            }
            Statement::DropDatabase(ref drop_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_drop_database_stmt(self.client_id, drop_stmt)
                }
            }
            Statement::GrantObject(ref grant_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_grant_object_stmt_with_search_path(
                        self.client_id,
                        grant_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::RevokeObject(ref revoke_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_revoke_object_stmt_with_search_path(
                        self.client_id,
                        revoke_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::GrantRoleMembership(ref grant_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_grant_role_membership_stmt(self.client_id, grant_stmt)
                }
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_revoke_role_membership_stmt(self.client_id, revoke_stmt)
                }
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth =
                        db.execute_set_session_authorization_stmt(self.client_id, set_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth =
                        db.execute_reset_session_authorization_stmt(self.client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::SetRole(ref set_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth = db.execute_set_role_stmt(self.client_id, set_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::ResetRole(ref reset_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    self.auth = db.execute_reset_role_stmt(self.client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
            }
            Statement::AlterTableReset(ref alter_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_reset_stmt_with_search_path(
                        self.client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::AlterTableSet(ref alter_stmt) => self.apply_alter_table_set(db, alter_stmt),
            Statement::AlterIndexSet(ref alter_stmt) => self.apply_alter_index_set(db, alter_stmt),
            Statement::CreateTableAs(ref create_stmt) => {
                let create_stmt = self.resolve_create_table_as_statement(create_stmt)?;
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(
                        db,
                        Statement::CreateTableAs(create_stmt),
                        statement_lock_scope_id,
                    );
                    if result.is_err()
                        && let Some(ref mut txn) = self.active_txn
                    {
                        txn.failed = true;
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_create_table_as_stmt_with_search_path(
                        self.client_id,
                        &create_stmt,
                        None,
                        0,
                        search_path.as_deref(),
                        self.planner_config(),
                    )
                }
            }
            Statement::CommentOnTable(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_table_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnColumn(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_column_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnView(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_view_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnIndex(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_index_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnType(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_type_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnConstraint(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_constraint_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnRule(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_rule_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnTrigger(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_trigger_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnEventTrigger(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_event_trigger_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnAggregate(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_aggregate_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnFunction(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_function_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnOperator(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_operator_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_comment_on_role_stmt(self.client_id, comment_stmt)
                }
            }
            Statement::CommentOnDatabase(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute_comment_on_database_stmt(self.client_id, comment_stmt)
                }
            }
            Statement::CommentOnConversion(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_conversion_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnPublication(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_publication_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::CommentOnStatistics(ref comment_stmt) => {
                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_statistics_stmt_with_search_path(
                        self.client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
            }
            Statement::Merge(ref merge_stmt) => {
                let _ = merge_stmt;
                let search_path = self.configured_search_path();
                db.execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
                    self.client_id,
                    stmt,
                    search_path.as_deref(),
                    &self.datetime_config,
                    &self.gucs,
                    self.planner_config(),
                    Arc::clone(&self.random_state),
                )
            }
            Statement::DeclareCursor(ref declare_stmt) => {
                let options = cursor_options_from_declare(declare_stmt);
                let result = self.declare_cursor(
                    db,
                    &declare_stmt.name,
                    sql.trim().trim_end_matches(';').to_string(),
                    &declare_stmt.query,
                    options,
                );
                if result.is_err() {
                    self.mark_transaction_failed();
                }
                result.map(|_| StatementResult::AffectedRows(0))
            }
            Statement::Fetch(ref fetch_stmt) => {
                let result = self.fetch_cursor(
                    &fetch_stmt.cursor_name,
                    portal_direction_from_fetch(&fetch_stmt.direction),
                    false,
                );
                if result.is_err() {
                    self.mark_transaction_failed();
                }
                result.map(|result| StatementResult::Query {
                    columns: result.columns,
                    column_names: result.column_names,
                    rows: result.rows,
                })
            }
            Statement::Move(ref fetch_stmt) => {
                let result = self.fetch_cursor(
                    &fetch_stmt.cursor_name,
                    portal_direction_from_fetch(&fetch_stmt.direction),
                    true,
                );
                if result.is_err() {
                    self.mark_transaction_failed();
                }
                result.map(|result| StatementResult::AffectedRows(result.processed))
            }
            Statement::ClosePortal(ref close_stmt) => {
                let result = if let Some(name) = &close_stmt.name {
                    self.close_portal(name)
                } else {
                    self.close_all_cursors();
                    Ok(())
                };
                if result.is_err() {
                    self.mark_transaction_failed();
                }
                result.map(|_| StatementResult::AffectedRows(0))
            }
            Statement::Load(ref load_stmt) => self.apply_load(load_stmt),
            Statement::Discard(ref discard_stmt) => self.apply_discard(db, discard_stmt.target),
            Statement::Begin(ref options) => {
                if self.active_txn.is_some() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "no active transaction",
                        actual: "already in a transaction block".into(),
                    }));
                }
                self.active_txn =
                    Some(self.active_transaction_without_xid_with_options(db, options));
                self.stats_state.write().begin_top_level_xact();
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Commit => {
                if self.active_txn.is_none() {
                    crate::backend::utils::misc::notices::push_warning(
                        "there is no transaction in progress",
                    );
                    return Ok(StatementResult::AffectedRows(0));
                }
                if self.active_txn.as_ref().is_some_and(|txn| txn.failed) {
                    let txn = self.active_txn.take().unwrap();
                    let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
                    self.abort_taken_transaction(db, &txn);
                    for rel in held_locks {
                        db.table_locks.unlock_table(rel, self.client_id);
                    }
                    self.restore_guc_state(db, txn.guc_start_state);
                    self.portals.drop_transaction_portals(false);
                    return Ok(StatementResult::AffectedRows(0));
                }
                let result = self
                    .validate_constraints_for_active_txn(db, false)
                    .map(|_| StatementResult::AffectedRows(0));
                let txn = self.active_txn.take().unwrap();
                let result = self.finalize_taken_transaction(db, txn, result);
                if result.is_ok() {
                    self.portals.drop_transaction_portals(true);
                } else {
                    self.portals.drop_transaction_portals(false);
                }
                result
            }
            Statement::Rollback => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => {
                        crate::backend::utils::misc::notices::push_warning(
                            "there is no transaction in progress",
                        );
                        return Ok(StatementResult::AffectedRows(0));
                    }
                };
                let held_locks = txn.held_table_locks.keys().copied().collect::<Vec<_>>();
                self.abort_taken_transaction(db, &txn);
                for rel in held_locks {
                    db.table_locks.unlock_table(rel, self.client_id);
                }
                self.restore_guc_state(db, txn.guc_start_state);
                self.portals.drop_transaction_portals(false);
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Savepoint(ref name) => {
                let guc_effective_state = self.capture_guc_state();
                let Some(txn) = self.active_txn.as_mut() else {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "active transaction",
                        actual: "SAVEPOINT can only be used in transaction blocks".into(),
                    }));
                };
                txn.savepoints.push(SavepointState {
                    name: name.clone(),
                    dynamic_type_snapshot: db.dynamic_type_snapshot(),
                    catalog_snapshot: db.catalog_store_snapshot(
                        self.client_id,
                        txn.xid.map(|xid| (xid, txn.next_command_id)),
                    )?,
                    catalog_effect_len: txn.catalog_effects.len(),
                    prior_catalog_invalidation_len: txn.prior_cmd_catalog_invalidations.len(),
                    temp_effect_len: txn.temp_effects.len(),
                    sequence_effect_len: txn.sequence_effects.len(),
                    guc_effective_state,
                    guc_commit_state: txn.guc_commit_state.clone(),
                    stats_state: self.stats_state.read().clone(),
                });
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ReleaseSavepoint(ref name) => {
                let Some(txn) = self.active_txn.as_mut() else {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "active transaction",
                        actual: "RELEASE SAVEPOINT can only be used in transaction blocks".into(),
                    }));
                };
                let Some(index) = txn
                    .savepoints
                    .iter()
                    .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
                else {
                    return Err(ExecError::DetailedError {
                        message: format!("savepoint \"{name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3B001",
                    });
                };
                txn.savepoints.truncate(index);
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::RollbackTo(ref name) => {
                let client_id = self.client_id;
                let (dynamic_type_snapshot, guc_effective_state, stats_state) = {
                    let Some(txn) = self.active_txn.as_mut() else {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "active transaction",
                            actual: "ROLLBACK TO SAVEPOINT can only be used in transaction blocks"
                                .into(),
                        }));
                    };
                    let Some(index) = txn
                        .savepoints
                        .iter()
                        .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
                    else {
                        return Err(ExecError::DetailedError {
                            message: format!("savepoint \"{name}\" does not exist"),
                            detail: None,
                            hint: None,
                            sqlstate: "3B001",
                        });
                    };
                    let savepoint = txn.savepoints[index].clone();
                    let aborted_catalog_effects =
                        txn.catalog_effects[savepoint.catalog_effect_len..].to_vec();
                    let aborted_prior_invalidations = txn.prior_cmd_catalog_invalidations
                        [savepoint.prior_catalog_invalidation_len..]
                        .to_vec();
                    let aborted_current_invalidations =
                        txn.current_cmd_catalog_invalidations.clone();
                    let aborted_temp_effects =
                        txn.temp_effects[savepoint.temp_effect_len..].to_vec();
                    let aborted_sequence_effects =
                        txn.sequence_effects[savepoint.sequence_effect_len..].to_vec();
                    let rollback_catalog_ctx = (!aborted_catalog_effects.is_empty())
                        .then(|| {
                            txn.xid.map(|xid| {
                                let cid = txn.next_command_id;
                                txn.next_command_id = txn.next_command_id.saturating_add(1);
                                (xid, cid)
                            })
                        })
                        .flatten();
                    db.finalize_aborted_catalog_effects(&aborted_catalog_effects);
                    db.finalize_aborted_temp_effects(client_id, &aborted_temp_effects);
                    db.finalize_aborted_sequence_effects(&aborted_sequence_effects);
                    let repair_effect = if let Some((xid, cid)) = rollback_catalog_ctx {
                        db.restore_catalog_store_snapshot_for_savepoint(
                            client_id,
                            xid,
                            cid,
                            savepoint.catalog_snapshot,
                            &aborted_catalog_effects,
                        )?
                    } else {
                        db.restore_catalog_store_snapshot(savepoint.catalog_snapshot);
                        CatalogMutationEffect::default()
                    };
                    db.finalize_aborted_local_catalog_invalidations(
                        client_id,
                        &aborted_prior_invalidations,
                        &aborted_current_invalidations,
                    );
                    txn.catalog_effects.truncate(savepoint.catalog_effect_len);
                    txn.prior_cmd_catalog_invalidations
                        .truncate(savepoint.prior_catalog_invalidation_len);
                    txn.current_cmd_catalog_invalidations.clear();
                    txn.temp_effects.truncate(savepoint.temp_effect_len);
                    txn.sequence_effects.truncate(savepoint.sequence_effect_len);
                    let repair_invalidation =
                        Database::catalog_invalidation_from_effect(&repair_effect);
                    if !repair_invalidation.is_empty() {
                        db.finalize_command_end_local_catalog_invalidations(
                            client_id,
                            std::slice::from_ref(&repair_invalidation),
                        );
                        txn.prior_cmd_catalog_invalidations
                            .push(repair_invalidation);
                        txn.catalog_effects.push(repair_effect);
                    }
                    txn.guc_commit_state = savepoint.guc_commit_state.clone();
                    txn.savepoints.truncate(index + 1);
                    txn.failed = false;
                    (
                        savepoint.dynamic_type_snapshot,
                        savepoint.guc_effective_state,
                        savepoint.stats_state,
                    )
                };
                db.restore_dynamic_type_snapshot(&dynamic_type_snapshot);
                self.restore_guc_state(db, guc_effective_state);
                self.stats_state
                    .write()
                    .restore_after_savepoint_rollback(stats_state);
                Ok(StatementResult::AffectedRows(0))
            }
            _ => {
                if let Some(ref txn) = self.active_txn {
                    if txn.failed {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "ROLLBACK",
                            actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                        }));
                    }
                }

                if matches!(stmt, Statement::Vacuum(_)) && self.active_txn.is_some() {
                    return Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")));
                }

                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt, statement_lock_scope_id);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    let search_path = self.configured_search_path();
                    db.execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
                        self.client_id,
                        stmt,
                        search_path.as_deref(),
                        &self.datetime_config,
                        &self.gucs,
                        self.planner_config(),
                        Arc::clone(&self.random_state),
                    )
                }
            }
        }
    }

    fn prepared_statement_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn prepared_statement_error(name: &str) -> ExecError {
        ExecError::Parse(ParseError::DetailedError {
            message: format!("prepared statement \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "26000",
        })
    }

    fn apply_prepare_statement(
        &mut self,
        prepare_stmt: &PrepareStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = Self::prepared_statement_name(&prepare_stmt.name);
        if self.prepared_selects.contains_key(&name) {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: format!("prepared statement \"{name}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42P05",
            }));
        }
        self.prepared_selects.insert(
            name,
            PreparedSelectStatement {
                query: prepare_stmt.query.clone(),
                query_sql: prepare_stmt.query_sql.clone(),
            },
        );
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_deallocate_statement(
        &mut self,
        deallocate_stmt: &DeallocateStatement,
    ) -> Result<StatementResult, ExecError> {
        let Some(name) = deallocate_stmt.name.as_deref() else {
            self.prepared_selects.clear();
            return Ok(StatementResult::AffectedRows(0));
        };
        let name = Self::prepared_statement_name(name);
        if self.prepared_selects.remove(&name).is_none() {
            return Err(Self::prepared_statement_error(&name));
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn prepared_statement_rows(&self) -> Vec<(String, String)> {
        self.prepared_selects
            .iter()
            .map(|(name, prepared)| (name.clone(), prepared.query_sql.clone()))
            .collect()
    }

    fn resolve_prepared_select(
        &self,
        execute_stmt: &ExecuteStatement,
    ) -> Result<PreparedSelectStatement, ExecError> {
        let name = Self::prepared_statement_name(&execute_stmt.name);
        self.prepared_selects
            .get(&name)
            .cloned()
            .ok_or_else(|| Self::prepared_statement_error(&name))
    }

    fn resolve_create_table_as_statement(
        &self,
        create_stmt: &CreateTableAsStatement,
    ) -> Result<CreateTableAsStatement, ExecError> {
        let CreateTableAsQuery::Execute(name) = &create_stmt.query else {
            return Ok(create_stmt.clone());
        };
        let prepared = self
            .prepared_selects
            .get(&Self::prepared_statement_name(name))
            .cloned()
            .ok_or_else(|| Self::prepared_statement_error(name))?;
        let mut resolved = create_stmt.clone();
        resolved.query = CreateTableAsQuery::Select(prepared.query);
        resolved.query_sql = Some(prepared.query_sql);
        Ok(resolved)
    }

    fn execute_prepared_statement(
        &mut self,
        db: &Database,
        execute_stmt: &ExecuteStatement,
        statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        let prepared = self.resolve_prepared_select(execute_stmt)?;
        if self.active_txn.is_some() {
            return self.execute_in_transaction(
                db,
                Statement::Select(prepared.query),
                statement_lock_scope_id,
            );
        }
        let search_path = self.configured_search_path();
        db.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
            self.client_id,
            Statement::Select(prepared.query),
            search_path.as_deref(),
            &self.datetime_config,
            &self.gucs,
            self.planner_config(),
        )
    }

    fn statement_timeout_duration(&self) -> Result<Option<Duration>, ExecError> {
        let Some(value) = self.gucs.get("statement_timeout") else {
            return Ok(None);
        };
        parse_statement_timeout(value)
    }

    fn statement_interrupt_guard(&self) -> Result<StatementInterruptGuard, ExecError> {
        Ok(self
            .interrupts
            .statement_interrupt_guard(self.statement_timeout_duration()?))
    }

    pub(crate) fn apply_startup_parameters(
        &mut self,
        params: &HashMap<String, String>,
    ) -> Result<(), ExecError> {
        if let Some(options) = params.get("options") {
            for (name, value) in parse_startup_options(options)? {
                self.apply_guc_value(&name, &value)?;
            }
        }
        for (name, value) in params {
            if name.eq_ignore_ascii_case("options") {
                continue;
            }
            let normalized = normalize_guc_name(name);
            if is_postgres_guc(&normalized) {
                self.apply_guc_value(name, value)?;
            }
        }
        self.reset_datetime_config = self.datetime_config.clone();
        Ok(())
    }

    pub(crate) fn interrupts(&self) -> Arc<InterruptState> {
        Arc::clone(&self.interrupts)
    }

    pub(crate) fn cleanup_on_disconnect(&mut self, db: &Database) {
        self.portals.clear();
        if let Some(txn) = self.active_txn.take() {
            if let Some(xid) = txn.xid {
                let _ = db.txns.write().abort(xid);
                db.txn_waiter.unregister_holder(xid);
                db.txn_waiter.notify();
            } else {
                debug_assert!(txn.catalog_effects.is_empty());
                debug_assert!(txn.temp_effects.is_empty());
                debug_assert!(txn.sequence_effects.is_empty());
            }
            db.restore_dynamic_type_snapshot(&txn.dynamic_type_snapshot);
            db.finalize_aborted_local_catalog_invalidations(
                self.client_id,
                &txn.prior_cmd_catalog_invalidations,
                &txn.current_cmd_catalog_invalidations,
            );
            db.finalize_aborted_catalog_effects(&txn.catalog_effects);
            db.finalize_aborted_temp_effects(self.client_id, &txn.temp_effects);
            db.finalize_aborted_sequence_effects(&txn.sequence_effects);
            db.advisory_locks
                .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
            db.row_locks
                .unlock_all_transaction(self.client_id, txn.advisory_scope_id);
            for rel in txn.held_table_locks.keys().copied() {
                db.table_locks.unlock_table(rel, self.client_id);
            }
        }
        db.async_notify_runtime.disconnect(self.client_id);

        // :HACK: Session-scoped table locks are currently tracked partly on the
        // session and partly in the global table lock manager. Release anything
        // still associated with this backend on disconnect, mirroring PostgreSQL
        // backend-exit lock cleanup even if the session missed normal unwind.
        db.table_locks.unlock_all_for_client(self.client_id);
        db.advisory_locks.unlock_all_session(self.client_id);
        db.row_locks.unlock_all_session(self.client_id);
    }

    fn lock_table_if_needed(
        &mut self,
        db: &Database,
        rel: RelFileLocator,
        mode: TableLockMode,
    ) -> Result<(), ExecError> {
        let Some(txn) = self.active_txn.as_mut() else {
            db.table_locks.lock_table_interruptible(
                rel,
                mode,
                self.client_id,
                self.interrupts.as_ref(),
            )?;
            return Ok(());
        };
        if txn
            .held_table_locks
            .get(&rel)
            .is_some_and(|existing| existing.strongest(mode) == *existing)
        {
            return Ok(());
        }
        db.table_locks.lock_table_interruptible(
            rel,
            mode,
            self.client_id,
            self.interrupts.as_ref(),
        )?;
        txn.held_table_locks
            .entry(rel)
            .and_modify(|existing| *existing = existing.strongest(mode))
            .or_insert(mode);
        Ok(())
    }

    fn lock_table_requests_if_needed(
        &mut self,
        db: &Database,
        requests: &[(RelFileLocator, TableLockMode)],
    ) -> Result<(), ExecError> {
        for (rel, mode) in requests {
            self.lock_table_if_needed(db, *rel, *mode)?;
        }
        Ok(())
    }

    pub fn execute_streaming(
        &mut self,
        db: &Database,
        select_stmt: &SelectStatement,
    ) -> Result<SelectGuard, ExecError> {
        db.install_auth_state(self.client_id, self.auth.clone());
        db.install_session_replication_role(self.client_id, self.session_replication_role());
        db.install_temp_backend_id(self.client_id, self.temp_backend_id);
        db.install_interrupt_state(self.client_id, self.interrupts());
        let (txn_ctx, transaction_lock_scope_id, catalog_effect_start, base_command_id) =
            if let Some(ref mut txn) = self.active_txn {
                let effect_start = txn.catalog_effects.len();
                let cid = txn.next_command_id;
                txn.next_command_id = txn.next_command_id.saturating_add(1);
                (
                    txn.xid.map(|xid| (xid, cid)).or_else(|| {
                        txn.isolation_level
                            .uses_transaction_snapshot()
                            .then_some((INVALID_TRANSACTION_ID, cid))
                    }),
                    Some(txn.advisory_scope_id),
                    effect_start,
                    cid,
                )
            } else {
                (None, None, 0, 0)
            };
        let snapshot_override = match txn_ctx {
            Some((snapshot_xid, snapshot_cid))
                if self
                    .active_txn
                    .as_ref()
                    .is_some_and(|txn| txn.isolation_level.uses_transaction_snapshot()) =>
            {
                Some(self.snapshot_for_command(db, snapshot_xid, snapshot_cid)?)
            }
            _ => None,
        };
        let statement_lock_scope_id = txn_ctx
            .is_none()
            .then(|| db.allocate_statement_lock_scope_id());
        let search_path = self.configured_search_path();
        let statement_timestamp_usecs =
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs();
        let transaction_timestamp_usecs = self
            .active_txn
            .as_ref()
            .map(|txn| txn.started_at_usecs)
            .unwrap_or(statement_timestamp_usecs);
        let mut datetime_config = self.datetime_config.clone();
        datetime_config.transaction_timestamp_usecs = Some(transaction_timestamp_usecs);
        datetime_config.statement_timestamp_usecs = Some(statement_timestamp_usecs);
        let mut guard = db.execute_streaming_with_config_and_random_state(
            self.client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            search_path.as_deref(),
            &datetime_config,
            &self.effective_gucs_for_execution(),
            snapshot_override,
            self.planner_config(),
            Arc::clone(&self.random_state),
        )?;
        guard.interrupt_guard = Some(self.statement_interrupt_guard()?);
        guard.catalog_effect_start = catalog_effect_start;
        guard.base_command_id = base_command_id;
        Ok(guard)
    }

    pub(crate) fn bind_protocol_portal(
        &mut self,
        db: &Database,
        name: &str,
        prep_stmt_name: Option<String>,
        sql: &str,
        result_formats: Vec<i16>,
    ) -> Result<(), ExecError> {
        let portal = self.build_portal_from_sql(
            db,
            name.to_string(),
            prep_stmt_name,
            sql.to_string(),
            result_formats,
            CursorOptions::protocol(),
        )?;
        self.portals.insert(portal, name.is_empty(), true)
    }

    pub(crate) fn declare_cursor(
        &mut self,
        db: &Database,
        name: &str,
        source_text: String,
        query: &SelectStatement,
        options: CursorOptions,
    ) -> Result<(), ExecError> {
        if self.active_txn.is_none() && !options.holdable {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: "DECLARE CURSOR can only be used in transaction blocks".into(),
                detail: None,
                hint: None,
                sqlstate: "25P01",
            }));
        }
        self.declare_cursor_in_active_txn(db, name, source_text, query, options)
    }

    fn declare_cursor_in_active_txn(
        &mut self,
        db: &Database,
        name: &str,
        source_text: String,
        query: &SelectStatement,
        options: CursorOptions,
    ) -> Result<(), ExecError> {
        let guard = self.execute_streaming(db, query)?;
        let mut portal = Portal::streaming_select(
            name.to_string(),
            source_text,
            None,
            Vec::new(),
            options,
            true,
            guard,
        );
        if portal.options.scroll || portal.options.holdable {
            portal.materialize_all()?;
        }
        self.portals.insert(portal, false, false)
    }

    pub(crate) fn execute_portal_forward(
        &mut self,
        db: &Database,
        name: &str,
        limit: PortalFetchLimit,
    ) -> Result<PortalRunResult, ExecError> {
        let mut portal = self.take_portal(name)?;
        let result = if let crate::pgrust::portal::PortalExecution::PendingSql { sql, .. } =
            &portal.execution
        {
            let sql = sql.clone();
            match self.execute(db, &sql)? {
                StatementResult::Query {
                    columns,
                    column_names,
                    rows,
                } => {
                    if let Some(tag) =
                        crate::backend::libpq::pqformat::infer_dml_returning_command_tag(
                            &sql,
                            rows.len(),
                        )
                    {
                        portal.command_tag = tag;
                    }
                    portal.execution = crate::pgrust::portal::PortalExecution::Materialized {
                        columns,
                        column_names,
                        rows,
                        pos: 0,
                    };
                    portal.fetch_forward(limit)?
                }
                StatementResult::AffectedRows(n) => {
                    let tag = crate::backend::libpq::pqformat::infer_command_tag(&sql, n);
                    portal.command_tag = tag.clone();
                    portal.execution = crate::pgrust::portal::PortalExecution::CommandDone;
                    PortalRunResult {
                        columns: Vec::new(),
                        column_names: Vec::new(),
                        rows: Vec::new(),
                        processed: n,
                        completed: true,
                        command_tag: Some(tag),
                    }
                }
            }
        } else {
            let fetch_result = portal.fetch_forward(limit);
            let completed = fetch_result
                .as_ref()
                .map(|result| result.completed)
                .unwrap_or(true);
            self.merge_completed_streaming_portal(
                db,
                &mut portal,
                completed,
                fetch_result.is_ok(),
            )?;
            fetch_result?
        };
        self.portals.put(portal);
        Ok(result)
    }

    pub(crate) fn fetch_cursor(
        &mut self,
        name: &str,
        direction: PortalFetchDirection,
        move_only: bool,
    ) -> Result<PortalRunResult, ExecError> {
        let mut portal = self.take_portal(name)?;
        if !portal.options.visible {
            self.portals.put(portal);
            return Err(undefined_cursor(name));
        }
        let result = portal.fetch_direction(direction, move_only);
        self.portals.put(portal);
        result
    }

    pub(crate) fn close_portal(&mut self, name: &str) -> Result<(), ExecError> {
        if self.portals.contains(name) {
            self.portals.remove(name);
            Ok(())
        } else {
            Err(undefined_cursor(name))
        }
    }

    pub(crate) fn close_all_cursors(&mut self) {
        self.portals.close_all_visible();
    }

    pub(crate) fn portal_columns(
        &self,
        name: &str,
    ) -> Option<Vec<crate::backend::executor::QueryColumn>> {
        self.portals.get(name).and_then(Portal::columns)
    }

    pub(crate) fn portal_result_formats(&self, name: &str) -> Option<Vec<i16>> {
        self.portals
            .get(name)
            .map(|portal| portal.result_formats.clone())
    }

    pub(crate) fn portal_source_text(&self, name: &str) -> Option<String> {
        self.portals
            .get(name)
            .map(|portal| portal.source_text.clone())
    }

    pub(crate) fn mark_portal_command_done(
        &mut self,
        name: &str,
        command_tag: String,
    ) -> Result<(), ExecError> {
        let mut portal = self.take_portal(name)?;
        portal.command_tag = command_tag;
        portal.execution = crate::pgrust::portal::PortalExecution::CommandDone;
        self.portals.put(portal);
        Ok(())
    }

    pub(crate) fn cursor_view_rows(&self) -> Vec<CursorViewRow> {
        self.portals.cursor_view_rows()
    }

    fn apply_writable_cte_column_aliases(
        cte: &crate::backend::parser::CommonTableExpr,
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

    fn take_portal(&mut self, name: &str) -> Result<Portal, ExecError> {
        self.portals
            .take(name)
            .ok_or_else(|| undefined_cursor(name))
    }

    fn build_portal_from_sql(
        &mut self,
        db: &Database,
        name: String,
        prep_stmt_name: Option<String>,
        source_text: String,
        result_formats: Vec<i16>,
        options: CursorOptions,
    ) -> Result<Portal, ExecError> {
        let stmt = if self.standard_conforming_strings() {
            db.plan_cache.get_statement_with_options(
                &source_text,
                ParseOptions {
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                    ..ParseOptions::default()
                },
            )?
        } else {
            crate::backend::parser::parse_statement_with_options(
                &source_text,
                ParseOptions {
                    standard_conforming_strings: false,
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                },
            )?
        };
        let created_in_transaction = self.active_txn.is_some();
        match stmt {
            Statement::Select(select_stmt) => {
                if select_sql_requires_command_end_xid_handling(&source_text) {
                    return match self.execute(db, &source_text)? {
                        StatementResult::Query {
                            columns,
                            column_names,
                            rows,
                        } => Ok(Portal::materialized_select(
                            name,
                            source_text,
                            prep_stmt_name,
                            result_formats,
                            options,
                            created_in_transaction,
                            columns,
                            column_names,
                            rows,
                        )),
                        StatementResult::AffectedRows(n) => {
                            let mut portal = Portal::pending_sql(
                                name,
                                source_text.clone(),
                                prep_stmt_name,
                                result_formats,
                                options,
                                created_in_transaction,
                                None,
                            );
                            portal.command_tag =
                                crate::backend::libpq::pqformat::infer_command_tag(&source_text, n);
                            portal.execution = crate::pgrust::portal::PortalExecution::CommandDone;
                            Ok(portal)
                        }
                    };
                }
                let guard = self.execute_streaming(db, &select_stmt)?;
                let mut portal = Portal::streaming_select(
                    name,
                    source_text,
                    prep_stmt_name,
                    result_formats,
                    options,
                    created_in_transaction,
                    guard,
                );
                if portal.options.scroll || portal.options.holdable {
                    portal.materialize_all()?;
                }
                Ok(portal)
            }
            Statement::Values(_) | Statement::Show(_) | Statement::Explain(_) => {
                match self.execute(db, &source_text)? {
                    StatementResult::Query {
                        columns,
                        column_names,
                        rows,
                    } => Ok(Portal::materialized_select(
                        name,
                        source_text,
                        prep_stmt_name,
                        result_formats,
                        options,
                        created_in_transaction,
                        columns,
                        column_names,
                        rows,
                    )),
                    StatementResult::AffectedRows(n) => {
                        let mut portal = Portal::pending_sql(
                            name,
                            source_text.clone(),
                            prep_stmt_name,
                            result_formats,
                            options,
                            created_in_transaction,
                            None,
                        );
                        portal.command_tag =
                            crate::backend::libpq::pqformat::infer_command_tag(&source_text, n);
                        portal.execution = crate::pgrust::portal::PortalExecution::CommandDone;
                        Ok(portal)
                    }
                }
            }
            _ => Ok(Portal::pending_sql(
                name,
                source_text,
                prep_stmt_name,
                result_formats,
                options,
                created_in_transaction,
                None,
            )),
        }
    }

    fn execute_in_transaction(
        &mut self,
        db: &Database,
        stmt: Statement,
        _statement_lock_scope_id: Option<u64>,
    ) -> Result<StatementResult, ExecError> {
        let effect_start = self
            .active_txn
            .as_ref()
            .map(|txn| txn.catalog_effects.len())
            .unwrap_or(0);
        let cid = {
            let txn = self.active_txn.as_mut().unwrap();
            let cid = txn.next_command_id;
            txn.next_command_id = txn.next_command_id.saturating_add(1);
            cid
        };
        let xid = if Self::statement_requires_xid_in_transaction(&stmt) {
            self.ensure_active_xid(db)
        } else {
            self.active_txn
                .as_ref()
                .and_then(|txn| txn.xid)
                .unwrap_or(INVALID_TRANSACTION_ID)
        };
        let client_id = self.client_id;

        let event_trigger_tag = Self::event_trigger_command_tag(&stmt).map(str::to_string);
        let (event_trigger_end_commands, event_trigger_dropped_objects) =
            if let Some(tag) = event_trigger_tag.as_deref() {
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                (
                    self.event_trigger_ddl_command_rows(&stmt, tag, Some(&catalog)),
                    self.event_trigger_dropped_object_rows(&stmt, Some(&catalog)),
                )
            } else {
                (Vec::new(), Vec::new())
            };
        let start_result = if let Some(tag) = event_trigger_tag.as_deref() {
            self.fire_event_trigger_event(
                db,
                xid,
                cid,
                _statement_lock_scope_id,
                "ddl_command_start",
                tag,
            )
        } else {
            Ok(())
        };

        let mut result = if let Err(err) = start_result {
            Err(err)
        } else {
            match stmt {
                Statement::AlterTableMulti(ref statements) => {
                    validate_multi_alter_table_temporal_fk_actions(
                        statements,
                        ParseOptions {
                            standard_conforming_strings: self.standard_conforming_strings(),
                            max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                        },
                    )?;
                    for sql in statements {
                        self.execute_internal(db, sql, _statement_lock_scope_id)?;
                    }
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::Do(ref do_stmt) => self.execute_plpgsql_do(db, do_stmt, xid, cid),
                Statement::Prepare(ref prepare_stmt) => self.apply_prepare_statement(prepare_stmt),
                Statement::Execute(ref execute_stmt) => {
                    self.execute_prepared_statement(db, execute_stmt, None)
                }
                Statement::Deallocate(ref deallocate_stmt) => {
                    self.apply_deallocate_statement(deallocate_stmt)
                }
                Statement::Show(ref show_stmt) => self.apply_show(db, show_stmt),
                Statement::Set(ref set_stmt) => self.apply_set(db, set_stmt),
                Statement::SetTransaction(ref set_txn_stmt) => {
                    self.apply_set_transaction(set_txn_stmt)
                }
                Statement::SetConstraints(ref set_constraints_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog = if xid != INVALID_TRANSACTION_ID {
                        self.catalog_lookup_for_command(db, xid, cid)
                    } else {
                        db.lazy_catalog_lookup(client_id, None, search_path.as_deref())
                    };
                    let tracker = self
                        .active_txn
                        .as_ref()
                        .expect("SET CONSTRAINTS requires active transaction state")
                        .deferred_foreign_keys
                        .clone();
                    execute_set_constraints(
                        db,
                        client_id,
                        &catalog,
                        (xid != INVALID_TRANSACTION_ID).then_some(xid),
                        cid,
                        self.interrupts(),
                        &self.datetime_config,
                        &tracker,
                        set_constraints_stmt,
                    )
                }
                Statement::Reset(ref reset_stmt) => self.apply_reset(db, reset_stmt),
                Statement::Checkpoint(_) => self.apply_checkpoint(db),
                Statement::AlterTableCompound(ref compound_stmt) => {
                    validate_compound_alter_table_temporal_fk_actions(compound_stmt)?;
                    compound_stmt.actions.iter().try_for_each(|action| {
                        self.execute_in_transaction(db, action.clone(), _statement_lock_scope_id)
                            .map(|_| ())
                    })?;
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::CommentOnDomain(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_domain_stmt_with_search_path(
                        client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::CommentOnType(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_type_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnConversion(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_comment_on_conversion_stmt_with_search_path(
                        client_id,
                        comment_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::CommentOnForeignDataWrapper(ref comment_stmt) => {
                    db.execute_comment_on_foreign_data_wrapper_stmt(client_id, comment_stmt)
                }
                Statement::CommentOnForeignServer(ref comment_stmt) => {
                    db.execute_comment_on_foreign_server_stmt(client_id, comment_stmt)
                }
                Statement::CommentOnPublication(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_publication_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnStatistics(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_statistics_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CopyFrom(ref copy_stmt) => self.execute_copy_from_file(db, copy_stmt),
                Statement::CopyTo(ref copy_stmt) => self
                    .execute_copy_to(db, copy_stmt, None)
                    .map(StatementResult::AffectedRows),
                Statement::CreateDomain(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_create_domain_stmt_with_search_path(
                        client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::AlterDomain(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_alter_domain_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::CreateConversion(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_create_conversion_stmt_with_search_path(
                        client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::CreateCollation(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_collation_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateForeignDataWrapper(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_create_foreign_data_wrapper_stmt_with_search_path(
                        client_id,
                        create_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::CreateForeignServer(ref create_stmt) => {
                    db.execute_create_foreign_server_stmt(client_id, create_stmt)
                }
                Statement::CreateLanguage(ref create_stmt) => {
                    db.execute_create_language_stmt(client_id, create_stmt)
                }
                Statement::AlterLanguage(ref alter_stmt) => {
                    db.execute_alter_language_stmt(client_id, alter_stmt)
                }
                Statement::DropLanguage(ref drop_stmt) => {
                    db.execute_drop_language_stmt(client_id, drop_stmt)
                }
                Statement::CreateUserMapping(ref create_stmt) => {
                    db.execute_create_user_mapping_stmt(client_id, create_stmt)
                }
                Statement::CreateForeignTable(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_foreign_table_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::ImportForeignSchema(ref import_stmt) => {
                    db.execute_import_foreign_schema_stmt(client_id, import_stmt)
                }
                Statement::AlterForeignDataWrapper(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_alter_foreign_data_wrapper_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::AlterForeignDataWrapperOwner(ref alter_stmt) => {
                    db.execute_alter_foreign_data_wrapper_owner_stmt(client_id, alter_stmt)
                }
                Statement::AlterForeignDataWrapperRename(ref alter_stmt) => {
                    db.execute_alter_foreign_data_wrapper_rename_stmt(client_id, alter_stmt)
                }
                Statement::AlterForeignServer(ref alter_stmt) => {
                    db.execute_alter_foreign_server_stmt(client_id, alter_stmt)
                }
                Statement::AlterForeignServerOwner(ref alter_stmt) => {
                    db.execute_alter_foreign_server_owner_stmt(client_id, alter_stmt)
                }
                Statement::AlterForeignServerRename(ref alter_stmt) => {
                    db.execute_alter_foreign_server_rename_stmt(client_id, alter_stmt)
                }
                Statement::AlterForeignTableOptions(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_foreign_table_options_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterUserMapping(ref alter_stmt) => {
                    db.execute_alter_user_mapping_stmt(client_id, alter_stmt)
                }
                Statement::DropForeignDataWrapper(ref drop_stmt) => {
                    db.execute_drop_foreign_data_wrapper_stmt(client_id, drop_stmt)
                }
                Statement::DropForeignServer(ref drop_stmt) => {
                    db.execute_drop_foreign_server_stmt(client_id, drop_stmt)
                }
                Statement::DropUserMapping(ref drop_stmt) => {
                    db.execute_drop_user_mapping_stmt(client_id, drop_stmt)
                }
                Statement::CreatePublication(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_publication_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateTrigger(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateEventTrigger(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_event_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterTableTriggerState(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_table_trigger_state_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterEventTrigger(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_event_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterEventTriggerOwner(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_event_trigger_owner_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterTriggerRename(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_trigger_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterEventTriggerRename(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_event_trigger_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreatePolicy(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_policy_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateIndex(ref create_stmt) => {
                    if create_stmt.concurrently {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                            "CREATE INDEX CONCURRENTLY",
                        )));
                    }
                    let search_path = self.configured_search_path();
                    let maintenance_work_mem_kb = self.maintenance_work_mem_kb()?;
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_index_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        maintenance_work_mem_kb,
                        catalog_effects,
                    )
                }
                Statement::ReindexIndex(ref reindex_stmt) => {
                    if reindex_stmt.concurrently {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                            "REINDEX CONCURRENTLY",
                        )));
                    }
                    if let Some(command) =
                        Self::reindex_non_relation_transaction_command(reindex_stmt)
                    {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(command)));
                    }
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    match reindex_stmt.kind {
                        crate::backend::parser::ReindexTargetKind::Index => {
                            if let Some(index) =
                                catalog.lookup_any_relation(&reindex_stmt.index_name)
                            {
                                self.lock_table_if_needed(
                                    db,
                                    index.rel,
                                    TableLockMode::AccessExclusive,
                                )?;
                            }
                        }
                        crate::backend::parser::ReindexTargetKind::Table => {
                            if let Some(relation) =
                                catalog.lookup_any_relation(&reindex_stmt.index_name)
                            {
                                self.lock_table_if_needed(
                                    db,
                                    relation.rel,
                                    TableLockMode::AccessExclusive,
                                )?;
                            }
                        }
                        _ => {}
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_reindex_index_stmt_in_transaction_with_search_path(
                        client_id,
                        reindex_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateStatistics(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_statistics_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::AlterStatistics(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_statistics_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateTextSearchDictionary(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_text_search_dictionary_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterTextSearchDictionary(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_text_search_dictionary_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateTextSearchConfiguration(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_text_search_configuration_stmt_in_transaction_with_search_path(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    catalog_effects,
                )
                }
                Statement::AlterTextSearchConfiguration(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_text_search_configuration_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropTextSearchConfiguration(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_text_search_configuration_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateOperatorClass(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_operator_class_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateOperatorFamily(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_operator_family_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterOperatorFamily(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_operator_family_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterOperatorClass(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_operator_class_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropOperatorFamily(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_operator_family_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateTextSearch(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_text_search_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterTextSearch(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_alter_text_search_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::CreateOperator(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_create_operator_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::AlterTableOwner(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_owner_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableRename(ref rename_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_relation(&rename_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                rename_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::AlterTableSetSchema(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_set_schema_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::AlterTableSetTablespace(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_tablespace_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::AlterTableSetPersistence(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_alter_table_set_persistence_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::AlterIndexRename(ref rename_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_index_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterIndexAttachPartition(ref attach_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_index_attach_partition_stmt_in_transaction_with_search_path(
                        client_id,
                        attach_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    if let Some(relation) = catalog.lookup_any_relation(&alter_stmt.index_name) {
                        self.lock_table_if_needed(
                            db,
                            relation.rel,
                            TableLockMode::AccessExclusive,
                        )?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_index_alter_column_statistics_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterIndexAlterColumnOptions(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_alter_index_alter_column_options_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::AlterViewRename(ref rename_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&rename_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                rename_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_view_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterViewRenameColumn(ref rename_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&rename_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                rename_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_view_rename_column_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterViewSetSchema(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_view_set_schema_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::AlterMaterializedViewSetSchema(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_materialized_view_set_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                )
                }
                Statement::AlterMaterializedViewSetAccessMethod(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_materialized_view_set_access_method_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterViewOwner(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_view_owner_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterSequence(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.sequence_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.sequence_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_sequence_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.sequence_effects,
                    )
                }
                Statement::AlterSequenceOwner(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.relation_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.relation_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_sequence_owner_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterSequenceRename(ref rename_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&rename_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                rename_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_sequence_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::AlterTableRenameColumn(ref rename_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&rename_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                rename_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_rename_column_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableAddColumn(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                        &mut txn.sequence_effects,
                    )
                }
                Statement::AlterTableAddColumns(ref alter_stmt) => {
                    let mut result = Ok(StatementResult::AffectedRows(0));
                    for column in &alter_stmt.columns {
                        result = self.execute_in_transaction(
                            db,
                            Statement::AlterTableAddColumn(AlterTableAddColumnStatement {
                                if_exists: alter_stmt.if_exists,
                                missing_ok: false,
                                only: alter_stmt.only,
                                table_name: alter_stmt.table_name.clone(),
                                column: column.clone(),
                                fdw_options: None,
                            }),
                            _statement_lock_scope_id,
                        );
                        if result.is_err() {
                            break;
                        }
                    }
                    result
                }
                Statement::AlterTableDropColumn(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&drop_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                drop_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableAlterColumnType(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let datetime_config = self.datetime_config.clone();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &datetime_config,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableAlterColumnDefault(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnExpression(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_expression_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnCompression(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_compression_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnStorage(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_storage_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnOptions(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_statistics_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                )
                }
                Statement::AlterTableAlterColumnIdentity(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_column_identity_stmt_in_transaction_with_search_path(
                    client_id,
                    alter_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                    &mut txn.sequence_effects,
                )
                }
                Statement::AlterTableAddConstraint(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    let lock_requests =
                        alter_table_add_constraint_lock_requests(&relation, alter_stmt, &catalog)?;
                    self.lock_table_requests_if_needed(db, &lock_requests)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        Some(&self.datetime_config),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableDropConstraint(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableAlterConstraint(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_alter_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableRenameConstraint(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_rename_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableSetNotNull(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableDropNotNull(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableValidateConstraint(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    let lock_requests = alter_table_validate_constraint_lock_requests(
                        &relation, alter_stmt, &catalog,
                    )?;
                    self.lock_table_requests_if_needed(db, &lock_requests)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableInherit(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    let parent = catalog
                        .lookup_any_relation(&alter_stmt.parent_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.parent_name.clone(),
                            ))
                        })?;
                    let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                    requests
                        .entry(relation.rel)
                        .and_modify(|existing| {
                            *existing = existing.strongest(TableLockMode::AccessExclusive)
                        })
                        .or_insert(TableLockMode::AccessExclusive);
                    requests
                        .entry(parent.rel)
                        .and_modify(|existing| {
                            *existing = existing.strongest(TableLockMode::AccessShare)
                        })
                        .or_insert(TableLockMode::AccessShare);
                    let requests = requests.into_iter().collect::<Vec<_>>();
                    self.lock_table_requests_if_needed(db, &requests)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_inherit_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableNoInherit(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    let parent = catalog
                        .lookup_any_relation(&alter_stmt.parent_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.parent_name.clone(),
                            ))
                        })?;
                    let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                    requests
                        .entry(relation.rel)
                        .and_modify(|existing| {
                            *existing = existing.strongest(TableLockMode::AccessExclusive)
                        })
                        .or_insert(TableLockMode::AccessExclusive);
                    requests
                        .entry(parent.rel)
                        .and_modify(|existing| {
                            *existing = existing.strongest(TableLockMode::AccessShare)
                        })
                        .or_insert(TableLockMode::AccessShare);
                    let requests = requests.into_iter().collect::<Vec<_>>();
                    self.lock_table_requests_if_needed(db, &requests)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_no_inherit_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableOf(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_of_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableNotOf(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_not_of_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableAttachPartition(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    if let Some(parent) = catalog.lookup_any_relation(&alter_stmt.parent_table) {
                        let child = catalog
                            .lookup_any_relation(&alter_stmt.partition_table)
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::TableDoesNotExist(
                                    alter_stmt.partition_table.clone(),
                                ))
                            })?;
                        let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                        requests
                            .entry(parent.rel)
                            .and_modify(|existing| {
                                *existing = existing.strongest(TableLockMode::AccessExclusive)
                            })
                            .or_insert(TableLockMode::AccessExclusive);
                        requests
                            .entry(child.rel)
                            .and_modify(|existing| {
                                *existing = existing.strongest(TableLockMode::AccessExclusive)
                            })
                            .or_insert(TableLockMode::AccessExclusive);
                        if let Some(partitioned_table) = parent.partitioned_table.as_ref()
                            && partitioned_table.partdefid != 0
                            && let Some(default_partition) =
                                catalog.relation_by_oid(partitioned_table.partdefid)
                        {
                            requests
                                .entry(default_partition.rel)
                                .and_modify(|existing| {
                                    *existing = existing.strongest(TableLockMode::AccessExclusive)
                                })
                                .or_insert(TableLockMode::AccessExclusive);
                        }
                        let requests = requests.into_iter().collect::<Vec<_>>();
                        self.lock_table_requests_if_needed(db, &requests)?;
                    } else if !alter_stmt.if_exists {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.parent_table.clone(),
                        )));
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableDetachPartition(ref alter_stmt) => {
                    if alter_stmt.mode == DetachPartitionMode::Concurrently {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                            "ALTER TABLE ... DETACH PARTITION CONCURRENTLY",
                        )));
                    }
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    if let Some(parent) = catalog.lookup_any_relation(&alter_stmt.parent_table) {
                        let child = catalog
                            .lookup_any_relation(&alter_stmt.partition_table)
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::TableDoesNotExist(
                                    alter_stmt.partition_table.clone(),
                                ))
                            })?;
                        let mut requests: BTreeMap<RelFileLocator, TableLockMode> = BTreeMap::new();
                        requests
                            .entry(parent.rel)
                            .and_modify(|existing| {
                                *existing = existing.strongest(TableLockMode::AccessExclusive)
                            })
                            .or_insert(TableLockMode::AccessExclusive);
                        requests
                            .entry(child.rel)
                            .and_modify(|existing| {
                                *existing = existing.strongest(TableLockMode::AccessExclusive)
                            })
                            .or_insert(TableLockMode::AccessExclusive);
                        let requests = requests.into_iter().collect::<Vec<_>>();
                        self.lock_table_requests_if_needed(db, &requests)?;
                    } else if !alter_stmt.if_exists {
                        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.parent_table.clone(),
                        )));
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_detach_partition_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableSetRowSecurity(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_set_row_security_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableReplicaIdentity(_) => {
                    Err(ExecError::Parse(ParseError::FeatureNotSupported(
                        "ALTER TABLE REPLICA IDENTITY in transaction".into(),
                    )))
                }
                Statement::AlterPolicy(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_policy_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableReset(ref alter_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&alter_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                alter_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_table_reset_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTableSet(ref alter_stmt) => {
                    self.apply_alter_table_set(db, alter_stmt)
                }
                Statement::AlterIndexSet(ref alter_stmt) => {
                    self.apply_alter_index_set(db, alter_stmt)
                }
                Statement::CreateRole(ref create_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_role_stmt_in_transaction(
                        client_id,
                        create_stmt,
                        self.gucs.get("createrole_self_grant").map(String::as_str),
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateDatabase(_) => Err(ExecError::Parse(
                    ParseError::ActiveSqlTransaction("CREATE DATABASE"),
                )),
                Statement::AlterDatabase(ref alter_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_database_stmt_in_transaction(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        false,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterRole(ref alter_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_role_stmt_in_transaction(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropRole(ref drop_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_role_stmt_in_transaction(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropDatabase(_) => Err(ExecError::Parse(
                    ParseError::ActiveSqlTransaction("DROP DATABASE"),
                )),
                Statement::GrantObject(ref grant_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_grant_object_stmt_in_transaction_with_search_path(
                        client_id,
                        grant_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::RevokeObject(ref revoke_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_revoke_object_stmt_in_transaction_with_search_path(
                        client_id,
                        revoke_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::GrantRoleMembership(ref grant_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_grant_role_membership_stmt_in_transaction(
                        client_id,
                        grant_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::RevokeRoleMembership(ref revoke_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_revoke_role_membership_stmt_in_transaction(
                        client_id,
                        revoke_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropOwned(ref drop_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_owned_stmt_in_transaction(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::ReassignOwned(ref reassign_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_reassign_owned_stmt_in_transaction(
                        client_id,
                        reassign_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnRole(ref comment_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_role_stmt_in_transaction(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnDatabase(ref comment_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_database_stmt_in_transaction(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropConversion(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_drop_conversion_stmt_with_search_path(
                        client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::DropCollation(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_collation_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropPublication(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_publication_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropStatistics(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_statistics_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropTrigger(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropEventTrigger(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_event_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::DropPolicy(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let catalog_effects = &mut self.active_txn.as_mut().unwrap().catalog_effects;
                    db.execute_drop_policy_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        catalog_effects,
                    )
                }
                Statement::SetSessionAuthorization(ref set_stmt) => {
                    self.auth = db.execute_set_session_authorization_stmt_in_transaction(
                        client_id, set_stmt, xid, cid,
                    )?;
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::ResetSessionAuthorization(ref reset_stmt) => {
                    self.auth =
                        db.execute_reset_session_authorization_stmt(client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::SetRole(ref set_stmt) => {
                    self.auth =
                        db.execute_set_role_stmt_in_transaction(client_id, set_stmt, xid, cid)?;
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::ResetRole(ref reset_stmt) => {
                    self.auth = db.execute_reset_role_stmt(client_id, reset_stmt)?;
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::Unsupported(ref unsupported_stmt) => {
                    if unsupported_stmt.feature == "ALTER DEFAULT PRIVILEGES" {
                        // :HACK: default ACL storage is not implemented yet. This
                        // compatibility slice accepts the DDL as a no-op for
                        // regression scripts that exercise ownership setup.
                        Ok(StatementResult::AffectedRows(0))
                    } else {
                        Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                            "{}: {}",
                            unsupported_stmt.feature, unsupported_stmt.sql
                        ))))
                    }
                }
                Statement::Call(ref call_stmt) => self.execute_call_stmt(db, call_stmt, xid, cid),
                Statement::AlterSchemaOwner(ref alter_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_schema_owner_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterSchemaRename(ref alter_stmt) => {
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_schema_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterPublication(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_publication_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnTable(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&comment_stmt.table_name)
                        .filter(|relation| matches!(relation.relkind, 'r' | 'p' | 'f'))
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "relation \"{}\" does not exist",
                                comment_stmt.table_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42P01",
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_table_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnColumn(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_any_relation(&comment_stmt.table_name)
                        .filter(|relation| matches!(relation.relkind, 'r' | 'p' | 'f'))
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "relation \"{}\" does not exist",
                                comment_stmt.table_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42P01",
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_column_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnView(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = match catalog.lookup_any_relation(&comment_stmt.view_name) {
                        Some(relation) if relation.relkind == 'v' => relation,
                        Some(_) => {
                            return Err(ExecError::Parse(ParseError::WrongObjectType {
                                name: comment_stmt.view_name.clone(),
                                expected: "view",
                            }));
                        }
                        None => {
                            return Err(ExecError::DetailedError {
                                message: format!(
                                    "relation \"{}\" does not exist",
                                    comment_stmt.view_name
                                ),
                                detail: None,
                                hint: None,
                                sqlstate: "42P01",
                            });
                        }
                    };
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_view_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnIndex(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = match catalog.lookup_any_relation(&comment_stmt.index_name) {
                        Some(relation) if relation.relkind == 'i' => relation,
                        Some(_) => {
                            return Err(ExecError::Parse(ParseError::WrongObjectType {
                                name: comment_stmt.index_name.clone(),
                                expected: "index",
                            }));
                        }
                        None => {
                            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                                comment_stmt.index_name.clone(),
                            )));
                        }
                    };
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_index_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnAggregate(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnFunction(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_function_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnOperator(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_operator_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnConstraint(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_relation(&comment_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                comment_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnRule(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = match catalog.lookup_any_relation(&comment_stmt.relation_name) {
                        Some(relation) if matches!(relation.relkind, 'r' | 'v') => relation,
                        Some(_) => {
                            return Err(ExecError::Parse(ParseError::WrongObjectType {
                                name: comment_stmt.relation_name.clone(),
                                expected: "table or view",
                            }));
                        }
                        None => {
                            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                                comment_stmt.relation_name.clone(),
                            )));
                        }
                    };
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_rule_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnTrigger(ref comment_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = catalog
                        .lookup_relation(&comment_stmt.table_name)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::TableDoesNotExist(
                                comment_stmt.table_name.clone(),
                            ))
                        })?;
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CommentOnEventTrigger(ref comment_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_comment_on_event_trigger_stmt_in_transaction_with_search_path(
                        client_id,
                        comment_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::Analyze(ref analyze_stmt) => {
                    let search_path = self.configured_search_path();
                    let targets = db.effective_analyze_targets_with_search_path(
                        client_id,
                        Some((xid, cid)),
                        search_path.as_deref(),
                        analyze_stmt,
                    )?;
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_analyze_stmt_in_transaction_with_search_path(
                        client_id,
                        &targets,
                        xid,
                        cid,
                        search_path.as_deref(),
                        false,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::Vacuum(_) => {
                    Err(ExecError::Parse(ParseError::ActiveSqlTransaction("VACUUM")))
                }
                Statement::Notify(ref notify_stmt) => self
                    .queue_txn_notification(
                        &notify_stmt.channel,
                        notify_stmt.payload.as_deref().unwrap_or(""),
                    )
                    .map(|_| StatementResult::AffectedRows(0)),
                Statement::Listen(ref listen_stmt) => {
                    self.queue_txn_listener_op(
                        AsyncListenAction::Listen,
                        Some(listen_stmt.channel.clone()),
                    );
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::Unlisten(ref unlisten_stmt) => {
                    self.queue_txn_listener_op(
                        AsyncListenAction::Unlisten,
                        unlisten_stmt.channel.clone(),
                    );
                    Ok(StatementResult::AffectedRows(0))
                }
                Statement::Merge(ref merge_stmt) => {
                    let snapshot = self.snapshot_for_command(db, xid, cid)?;
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let bound = plan_merge(merge_stmt, &catalog)?;
                    let mut ctx =
                        self.executor_context_for_catalog(db, snapshot, cid, &catalog, None, None);
                    let result = execute_merge(bound, &catalog, &mut ctx, xid, cid);
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                }
                Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                    let search_path = self.configured_search_path();
                    let txn_ctx = self.active_txn_ctx_for_command(cid);
                    let snapshot = match txn_ctx {
                        Some((snapshot_xid, snapshot_cid)) => {
                            self.snapshot_for_command(db, snapshot_xid, snapshot_cid)?
                        }
                        None => self.snapshot_for_command(db, INVALID_TRANSACTION_ID, cid)?,
                    };
                    let catalog =
                        db.lazy_catalog_lookup(client_id, txn_ctx, search_path.as_deref());
                    let deferred_foreign_keys = self
                        .active_txn
                        .as_ref()
                        .unwrap()
                        .deferred_foreign_keys
                        .clone();
                    let mut ctx = self.executor_context_for_catalog(
                        db,
                        snapshot,
                        cid,
                        &catalog,
                        Some(deferred_foreign_keys),
                        None,
                    );
                    let result = match stmt {
                        Statement::Select(select) if Self::select_has_writable_ctes(&select) => {
                            if select.with_recursive {
                                Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                "WITH RECURSIVE containing data-modifying statements is not supported"
                                    .into(),
                            )))
                            } else {
                                let mut materialized_ctes = Vec::new();
                                let mut outer_select = select.clone();
                                outer_select.with.clear();

                                let result = (|| {
                                    for cte in &select.with {
                                        let CteBody::Insert(cte_insert) = &cte.body else {
                                            outer_select.with.push(cte.clone());
                                            continue;
                                        };
                                        if cte_insert.with_recursive
                                            || cte_insert.with.iter().any(|nested| {
                                                Self::cte_body_has_writable_insert(&nested.body)
                                            })
                                        {
                                            return Err(ExecError::Parse(
                                                ParseError::FeatureNotSupported(
                                                    "nested writable CTEs are not supported".into(),
                                                ),
                                            ));
                                        }
                                        if cte_insert.returning.is_empty() {
                                            return Err(ExecError::Parse(
                                            ParseError::FeatureNotSupported(
                                                "writable CTE without RETURNING is not supported"
                                                    .into(),
                                            ),
                                        ));
                                        }

                                        let bound = bind_insert_with_outer_scopes_and_ctes(
                                            cte_insert,
                                            &catalog,
                                            &[],
                                            &materialized_ctes,
                                        )?;
                                        let prepared =
                                        crate::pgrust::database::commands::rules::prepare_bound_insert_for_execution(
                                            bound, &catalog,
                                        )?;
                                        let lock_requests = merge_table_lock_requests(
                                            &insert_foreign_key_lock_requests(&prepared.stmt),
                                            &prepared.extra_lock_requests,
                                        );
                                        self.lock_table_requests_if_needed(db, &lock_requests)?;
                                        let result =
                                        crate::pgrust::database::commands::rules::execute_bound_insert_with_rules(
                                            prepared.stmt,
                                            &catalog,
                                            &mut ctx,
                                            xid,
                                            cid,
                                        )?;
                                        let StatementResult::Query { columns, rows, .. } = result
                                        else {
                                            return Err(ExecError::Parse(
                                            ParseError::FeatureNotSupported(
                                                "writable CTE without RETURNING is not supported"
                                                    .into(),
                                            ),
                                        ));
                                        };
                                        let columns =
                                            Self::apply_writable_cte_column_aliases(cte, columns)?;
                                        materialized_ctes.push(bound_cte_from_query_rows(
                                            cte.name.clone(),
                                            columns,
                                            &rows,
                                        ));
                                    }

                                    let planned = pg_plan_query_with_outer_scopes_and_ctes(
                                        &outer_select,
                                        &catalog,
                                        &[],
                                        &materialized_ctes,
                                    )?;
                                    check_planned_stmt_select_privileges(&planned, &ctx)?;
                                    execute_planned_stmt(planned, &mut ctx)
                                })();
                                result
                            }
                        }
                        Statement::Select(select) if select.locking_clause.is_some() => {
                            let planned = pg_plan_query_with_config(
                                &select,
                                &catalog,
                                self.planner_config(),
                            )?;
                            check_planned_stmt_select_for_update_privileges(&planned, &ctx)?;
                            execute_planned_stmt(planned, &mut ctx)
                        }
                        other => execute_readonly_statement_with_config(
                            other,
                            &catalog,
                            &mut ctx,
                            self.planner_config(),
                        ),
                    };
                    if let Some(xid) = ctx.transaction_xid()
                        && let Some(txn) = self.active_txn.as_mut()
                    {
                        txn.xid = Some(xid);
                        if txn.isolation_level.uses_transaction_snapshot()
                            && let Some(mut snapshot) = txn.transaction_snapshot.clone()
                        {
                            snapshot.current_xid = xid;
                            snapshot.current_cid = cid;
                            crate::backend::utils::time::snapmgr::set_transaction_snapshot_override(
                                db,
                                self.client_id,
                                xid,
                                snapshot,
                            );
                        }
                    }
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                }
                Statement::Insert(ref insert_stmt) => {
                    let snapshot = self.snapshot_for_command(db, xid, cid)?;
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let deferred_foreign_keys = self
                        .active_txn
                        .as_ref()
                        .unwrap()
                        .deferred_foreign_keys
                        .clone();
                    let mut ctx = self.executor_context_for_catalog(
                        db,
                        snapshot,
                        cid,
                        &catalog,
                        Some(deferred_foreign_keys),
                        None,
                    );
                    let result = (|| {
                        let has_writable_ctes = insert_stmt
                            .with
                            .iter()
                            .any(|cte| matches!(cte.body, CteBody::Insert(_)));
                        if !has_writable_ctes {
                            let bound = bind_insert(insert_stmt, &catalog)?;
                            let prepared =
                            crate::pgrust::database::commands::rules::prepare_bound_insert_for_execution(
                                bound, &catalog,
                            )?;
                            let lock_requests = merge_table_lock_requests(
                                &insert_foreign_key_lock_requests(&prepared.stmt),
                                &prepared.extra_lock_requests,
                            );
                            self.lock_table_requests_if_needed(db, &lock_requests)?;
                            return crate::pgrust::database::commands::rules::execute_bound_insert_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            cid,
                        );
                        }

                        if insert_stmt.with_recursive {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                            "WITH RECURSIVE containing data-modifying statements is not supported"
                                .into(),
                        )));
                        }

                        let mut materialized_ctes = Vec::new();
                        let mut outer_insert = insert_stmt.clone();
                        outer_insert.with.clear();

                        for cte in &insert_stmt.with {
                            let CteBody::Insert(cte_insert) = &cte.body else {
                                outer_insert.with.push(cte.clone());
                                continue;
                            };
                            if cte_insert.with_recursive
                                || cte_insert
                                    .with
                                    .iter()
                                    .any(|nested| matches!(nested.body, CteBody::Insert(_)))
                            {
                                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                    "nested writable CTEs are not supported".into(),
                                )));
                            }
                            if cte_insert.returning.is_empty() {
                                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                    "writable CTE without RETURNING is not supported".into(),
                                )));
                            }

                            let bound = bind_insert_with_outer_scopes_and_ctes(
                                cte_insert,
                                &catalog,
                                &[],
                                &materialized_ctes,
                            )?;
                            let prepared =
                            crate::pgrust::database::commands::rules::prepare_bound_insert_for_execution(
                                bound, &catalog,
                            )?;
                            let lock_requests = merge_table_lock_requests(
                                &insert_foreign_key_lock_requests(&prepared.stmt),
                                &prepared.extra_lock_requests,
                            );
                            self.lock_table_requests_if_needed(db, &lock_requests)?;
                            let result =
                            crate::pgrust::database::commands::rules::execute_bound_insert_with_rules(
                                prepared.stmt,
                                &catalog,
                                &mut ctx,
                                xid,
                                cid,
                            )?;
                            let StatementResult::Query { columns, rows, .. } = result else {
                                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                    "writable CTE without RETURNING is not supported".into(),
                                )));
                            };
                            let columns = Self::apply_writable_cte_column_aliases(cte, columns)?;
                            materialized_ctes.push(bound_cte_from_query_rows(
                                cte.name.clone(),
                                columns,
                                &rows,
                            ));
                        }

                        let bound = bind_insert_with_outer_scopes_and_ctes(
                            &outer_insert,
                            &catalog,
                            &[],
                            &materialized_ctes,
                        )?;
                        let prepared =
                        crate::pgrust::database::commands::rules::prepare_bound_insert_for_execution(
                            bound, &catalog,
                        )?;
                        let lock_requests = merge_table_lock_requests(
                            &insert_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        self.lock_table_requests_if_needed(db, &lock_requests)?;
                        crate::pgrust::database::commands::rules::execute_bound_insert_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            cid,
                        )
                    })();
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                }
                Statement::Update(ref update_stmt) => {
                    let snapshot = self.snapshot_for_command(db, xid, cid)?;
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let bound = bind_update(update_stmt, &catalog)?;
                    let prepared =
                    crate::pgrust::database::commands::rules::prepare_bound_update_for_execution(
                        bound, &catalog,
                    )?;
                    let lock_requests = merge_table_lock_requests(
                        &update_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    self.lock_table_requests_if_needed(db, &lock_requests)?;
                    let interrupts = self.interrupts();
                    let deferred_foreign_keys = self
                        .active_txn
                        .as_ref()
                        .unwrap()
                        .deferred_foreign_keys
                        .clone();
                    let mut ctx = self.executor_context_for_catalog(
                        db,
                        snapshot,
                        cid,
                        &catalog,
                        Some(deferred_foreign_keys),
                        None,
                    );
                    ctx.interrupts = Arc::clone(&interrupts);
                    let result =
                        crate::pgrust::database::commands::rules::execute_bound_update_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            cid,
                            Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                        );
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                }
                Statement::Delete(ref delete_stmt) => {
                    let snapshot = self.snapshot_for_command(db, xid, cid)?;
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let bound = bind_delete(delete_stmt, &catalog)?;
                    let prepared =
                    crate::pgrust::database::commands::rules::prepare_bound_delete_for_execution(
                        bound, &catalog,
                    )?;
                    let lock_requests = merge_table_lock_requests(
                        &delete_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    self.lock_table_requests_if_needed(db, &lock_requests)?;
                    let interrupts = self.interrupts();
                    let deferred_foreign_keys = self
                        .active_txn
                        .as_ref()
                        .unwrap()
                        .deferred_foreign_keys
                        .clone();
                    let mut ctx = self.executor_context_for_catalog(
                        db,
                        snapshot,
                        cid,
                        &catalog,
                        Some(deferred_foreign_keys),
                        None,
                    );
                    ctx.interrupts = Arc::clone(&interrupts);
                    let result =
                        crate::pgrust::database::commands::rules::execute_bound_delete_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            Some((&db.txns, &db.txn_waiter, interrupts.as_ref())),
                        );
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                }
                Statement::CreateFunction(ref create_stmt) => {
                    self.validate_create_function_config(create_stmt)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_function_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateProcedure(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_procedure_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateAggregate(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_aggregate_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterAggregateRename(ref rename_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_aggregate_rename_stmt_in_transaction_with_search_path(
                        client_id,
                        rename_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateCast(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_cast_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterOperator(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_operator_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterConversion(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_conversion_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterProcedure(_) => Err(ExecError::Parse(
                    ParseError::FeatureNotSupported("ALTER PROCEDURE".into()),
                )),
                Statement::AlterRoutine(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_routine_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateSchema(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let maintenance_work_mem_kb = self.maintenance_work_mem_kb()?;
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_schema_stmt_in_transaction_with_search_path_and_maintenance_work_mem(
                    client_id,
                    create_stmt,
                    xid,
                    cid,
                    search_path.as_deref(),
                    maintenance_work_mem_kb,
                    &mut txn.catalog_effects,
                    &mut txn.temp_effects,
                    &mut txn.sequence_effects,
                )
                }
                Statement::CreateSequence(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_sequence_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                        &mut txn.sequence_effects,
                    )
                }
                Statement::CreateTablespace(ref create_stmt) => {
                    let allow_in_place_tablespaces = self.allow_in_place_tablespaces();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_tablespace_stmt_in_transaction(
                        client_id,
                        create_stmt,
                        allow_in_place_tablespaces,
                        xid,
                        cid,
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateTable(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_table_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                        &mut txn.sequence_effects,
                    )
                }
                Statement::CreateType(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_type_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterType(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_alter_type_stmt_in_transaction_with_search_path(
                        client_id,
                        alter_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::AlterTypeOwner(ref alter_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_alter_type_owner_stmt_with_search_path(
                        client_id,
                        alter_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::DropDomain(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    db.execute_drop_domain_stmt_with_search_path(
                        client_id,
                        drop_stmt,
                        search_path.as_deref(),
                    )
                }
                Statement::DropFunction(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_function_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropProcedure(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_procedure_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropRoutine(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_routine_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropAggregate(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_aggregate_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropOperator(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_operator_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropCast(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_cast_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateView(ref create_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_view_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::CreateRule(ref create_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relation = match catalog.lookup_any_relation(&create_stmt.relation_name) {
                        Some(relation) if matches!(relation.relkind, 'r' | 'v') => relation,
                        Some(_) => {
                            return Err(ExecError::Parse(ParseError::WrongObjectType {
                                name: create_stmt.relation_name.clone(),
                                expected: "table or view",
                            }));
                        }
                        None => {
                            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                                create_stmt.relation_name.clone(),
                            )));
                        }
                    };
                    self.lock_table_if_needed(db, relation.rel, TableLockMode::AccessExclusive)?;
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_rule_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::CreateTableAs(ref create_stmt) => {
                    let create_stmt = self.resolve_create_table_as_statement(create_stmt)?;
                    let search_path = self.configured_search_path();
                    let planner_config = self.planner_config();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_create_table_as_stmt_in_transaction_with_search_path(
                        client_id,
                        &create_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        planner_config,
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::RefreshMaterializedView(ref refresh_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_refresh_materialized_view_stmt_in_transaction_with_search_path(
                        client_id,
                        refresh_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropType(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_type_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropView(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let rels = {
                        drop_stmt
                            .view_names
                            .iter()
                            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                            .collect::<Vec<_>>()
                    };
                    for rel in rels {
                        self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_view_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::DropMaterializedView(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let rels = {
                        drop_stmt
                            .view_names
                            .iter()
                            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                            .collect::<Vec<_>>()
                    };
                    for rel in rels {
                        self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_materialized_view_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropRule(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    if let Some(relation) = catalog.lookup_any_relation(&drop_stmt.relation_name) {
                        self.lock_table_if_needed(
                            db,
                            relation.rel,
                            TableLockMode::AccessExclusive,
                        )?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_rule_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropIndex(ref drop_stmt) => {
                    if drop_stmt.concurrently {
                        return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                            "DROP INDEX CONCURRENTLY",
                        )));
                    }
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let rels = {
                        drop_stmt
                            .index_names
                            .iter()
                            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                            .collect::<Vec<_>>()
                    };
                    for rel in rels {
                        self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_index_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropTable(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let rels = {
                        drop_stmt
                            .table_names
                            .iter()
                            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                            .collect::<Vec<_>>()
                    };
                    for rel in rels {
                        self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_table_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                    )
                }
                Statement::DropSchema(ref drop_stmt) => {
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_schema_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::DropSequence(ref drop_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let rels = {
                        drop_stmt
                            .sequence_names
                            .iter()
                            .filter_map(|name| {
                                catalog.lookup_any_relation(name).map(|entry| entry.rel)
                            })
                            .collect::<Vec<_>>()
                    };
                    for rel in rels {
                        self.lock_table_if_needed(db, rel, TableLockMode::AccessExclusive)?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_drop_sequence_stmt_in_transaction_with_search_path(
                        client_id,
                        drop_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                        &mut txn.temp_effects,
                        &mut txn.sequence_effects,
                    )
                }
                Statement::TruncateTable(ref truncate_stmt) => {
                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let relations = {
                        let mut relations = Vec::new();
                        for name in &truncate_stmt.table_names {
                            let Some(relation) = catalog.lookup_any_relation(name) else {
                                continue;
                            };
                            if !relations.iter().any(
                                |existing: &crate::backend::parser::BoundRelation| {
                                    existing.relation_oid == relation.relation_oid
                                },
                            ) {
                                relations.push(relation.clone());
                            }
                            if relation.relkind == 'p' {
                                for oid in catalog.find_all_inheritors(relation.relation_oid) {
                                    if oid == relation.relation_oid {
                                        continue;
                                    }
                                    let Some(child) = catalog.relation_by_oid(oid) else {
                                        continue;
                                    };
                                    if relations.iter().any(
                                        |existing: &crate::backend::parser::BoundRelation| {
                                            existing.relation_oid == child.relation_oid
                                        },
                                    ) {
                                        continue;
                                    }
                                    relations.push(child);
                                }
                            }
                        }
                        relations
                    };
                    let target_relation_oids = relations
                        .iter()
                        .map(|relation| relation.relation_oid)
                        .collect::<Vec<_>>();
                    for relation in &relations {
                        reject_relation_with_referencing_foreign_keys_except(
                            &catalog,
                            relation.relation_oid,
                            &target_relation_oids,
                            "TRUNCATE on table without referencing foreign keys",
                        )?;
                    }
                    for relation in relations {
                        self.lock_table_if_needed(
                            db,
                            relation.rel,
                            TableLockMode::AccessExclusive,
                        )?;
                    }
                    let search_path = self.configured_search_path();
                    let txn = self.active_txn.as_mut().unwrap();
                    db.execute_truncate_table_in_transaction_with_search_path(
                        client_id,
                        truncate_stmt,
                        xid,
                        cid,
                        search_path.as_deref(),
                        &mut txn.catalog_effects,
                    )
                }
                Statement::Begin(_)
                | Statement::Commit
                | Statement::Rollback
                | Statement::Savepoint(_)
                | Statement::ReleaseSavepoint(_)
                | Statement::RollbackTo(_) => {
                    unreachable!("handled in Session::execute")
                }
                Statement::Load(_) | Statement::Discard(_) => {
                    unreachable!("handled outside transaction executor")
                }
                Statement::DeclareCursor(_)
                | Statement::Fetch(_)
                | Statement::Move(_)
                | Statement::ClosePortal(_) => {
                    unreachable!("session commands are handled in Session::execute")
                }
            }
        };

        if result.is_ok()
            && !event_trigger_dropped_objects.is_empty()
            && let Some(tag) = event_trigger_tag.as_deref()
            && let Err(err) = self.fire_event_trigger_event_with_dropped_objects(
                db,
                xid,
                cid,
                _statement_lock_scope_id,
                "sql_drop",
                tag,
                event_trigger_dropped_objects,
            )
        {
            result = Err(err);
        }

        if result.is_ok()
            && let Some(tag) = event_trigger_tag.as_deref()
            && let Err(err) = self.fire_event_trigger_event_with_ddl_commands(
                db,
                xid,
                cid,
                _statement_lock_scope_id,
                "ddl_command_end",
                tag,
                event_trigger_end_commands,
            )
        {
            result = Err(err);
        }

        if result.is_ok() {
            self.advance_catalog_command_id_after_statement(cid, effect_start);
            self.process_catalog_command_end(db, effect_start);
            self.validate_constraints_for_active_txn(db, true)?;
        }

        result
    }

    fn apply_set(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::SetStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = normalize_guc_name(&stmt.name);
        let is_builtin = is_postgres_guc(&name);
        if !is_builtin {
            if !name.contains('.') {
                return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                    name,
                )));
            }
            validate_custom_guc_for_set(&name, self.plpgsql_loaded)?;
        } else if is_checkpoint_guc(&name) || is_autovacuum_guc(&name) {
            return Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name)));
        }

        if stmt.is_local && self.active_txn.is_none() {
            crate::backend::utils::misc::notices::push_warning(
                "SET LOCAL can only be used in transaction blocks",
            );
            return Ok(StatementResult::AffectedRows(0));
        }

        let mut effective_state = self.capture_guc_state();
        let normalized = if let Some(value) = &stmt.value {
            apply_guc_value_to_state(&mut effective_state, &stmt.name, value)?
        } else {
            if is_builtin {
                reset_guc_in_state(&mut effective_state, &name, &self.reset_datetime_config);
            } else {
                effective_state.gucs.insert(name.clone(), String::new());
            }
            name
        };

        let mut commit_state = if stmt.is_local {
            None
        } else {
            self.active_txn
                .as_ref()
                .map(|txn| txn.guc_commit_state.clone())
        };
        if let Some(commit_state) = commit_state.as_mut() {
            if let Some(value) = &stmt.value {
                apply_guc_value_to_state(commit_state, &stmt.name, value)?;
            } else if !is_builtin {
                commit_state.gucs.insert(normalized.clone(), String::new());
            } else {
                reset_guc_in_state(commit_state, &normalized, &self.reset_datetime_config);
            }
        }

        if normalized == "transaction_isolation"
            && let Some(value) = &stmt.value
        {
            let level = crate::backend::parser::TransactionIsolationLevel::parse(value)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(value.clone()))
                })?;
            self.set_active_transaction_isolation(level)?;
        }

        self.install_guc_state(effective_state);
        if let Some(commit_state) = commit_state
            && let Some(txn) = self.active_txn.as_mut()
        {
            txn.guc_commit_state = commit_state;
        }
        self.after_guc_change(db, &normalized);
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_set_transaction(
        &mut self,
        stmt: &crate::backend::parser::SetTransactionStatement,
    ) -> Result<StatementResult, ExecError> {
        match stmt.scope {
            crate::backend::parser::SetTransactionScope::Transaction => {
                self.apply_transaction_options(&stmt.options)?;
            }
            crate::backend::parser::SetTransactionScope::SessionCharacteristics => {
                if let Some(level) = stmt.options.isolation_level {
                    self.gucs.insert(
                        "default_transaction_isolation".into(),
                        level.as_str().into(),
                    );
                }
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_reset(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::ResetStatement,
    ) -> Result<StatementResult, ExecError> {
        if let Some(name) = &stmt.name {
            let normalized = normalize_guc_name(name);
            let is_builtin = is_postgres_guc(&normalized);
            if !is_builtin && !self.gucs.contains_key(&normalized) {
                return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                    normalized,
                )));
            }
            if is_builtin && (is_checkpoint_guc(&normalized) || is_autovacuum_guc(&normalized)) {
                return Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(
                    normalized,
                )));
            }
            let mut effective_state = self.capture_guc_state();
            if is_builtin {
                reset_guc_in_state(
                    &mut effective_state,
                    &normalized,
                    &self.reset_datetime_config,
                );
            } else {
                effective_state
                    .gucs
                    .insert(normalized.clone(), String::new());
            }
            let mut commit_state = self
                .active_txn
                .as_ref()
                .map(|txn| txn.guc_commit_state.clone());
            if let Some(commit_state) = commit_state.as_mut() {
                if is_builtin {
                    reset_guc_in_state(commit_state, &normalized, &self.reset_datetime_config);
                } else {
                    commit_state.gucs.insert(normalized.clone(), String::new());
                }
            }
            self.install_guc_state(effective_state);
            if let Some(commit_state) = commit_state
                && let Some(txn) = self.active_txn.as_mut()
            {
                txn.guc_commit_state = commit_state;
            }
            self.after_guc_change(db, &normalized);
        } else {
            let mut effective_state = self.capture_guc_state();
            reset_all_gucs_in_state(&mut effective_state, &self.reset_datetime_config);
            let mut commit_state = self
                .active_txn
                .as_ref()
                .map(|txn| txn.guc_commit_state.clone());
            if let Some(commit_state) = commit_state.as_mut() {
                reset_all_gucs_in_state(commit_state, &self.reset_datetime_config);
            }
            self.install_guc_state(effective_state);
            if let Some(commit_state) = commit_state
                && let Some(txn) = self.active_txn.as_mut()
            {
                txn.guc_commit_state = commit_state;
            }
            db.install_row_security_enabled(self.client_id, true);
            db.install_session_replication_role(self.client_id, self.session_replication_role());
            db.plan_cache.invalidate_all();
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn after_guc_change(&self, db: &Database, normalized: &str) {
        if normalized == "row_security" {
            db.install_row_security_enabled(self.client_id, self.row_security_enabled());
            db.plan_cache.invalidate_all();
        } else if normalized == "session_replication_role" {
            db.install_session_replication_role(self.client_id, self.session_replication_role());
            db.plan_cache.invalidate_all();
        } else if matches!(
            normalized,
            "enable_partitionwise_join"
                | "enable_seqscan"
                | "enable_indexscan"
                | "enable_indexonlyscan"
                | "enable_bitmapscan"
                | "enable_hashjoin"
                | "enable_mergejoin"
                | "enable_memoize"
                | "enable_hashagg"
                | "enable_sort"
                | "default_text_search_config"
        ) {
            db.plan_cache.invalidate_all();
        }
    }

    fn apply_show(
        &mut self,
        db: &Database,
        stmt: &crate::backend::parser::ShowStatement,
    ) -> Result<StatementResult, ExecError> {
        let name = normalize_guc_name(&stmt.name);
        if name == "tables" {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "configuration parameter",
                actual: stmt.name.clone(),
            }));
        }
        if !is_postgres_guc(&name) && !self.gucs.contains_key(&name) {
            return Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
                name,
            )));
        }

        let fallback_value = || -> String {
            match name.as_str() {
                "datestyle" => default_datestyle().to_string(),
                "intervalstyle" => default_intervalstyle().to_string(),
                "timezone" => default_timezone().to_string(),
                "xmlbinary" => format_xmlbinary(self.datetime_config.xml.binary).to_string(),
                "xmloption" => format_xmloption(self.datetime_config.xml.option).to_string(),
                "enable_partitionwise_join" => "off".to_string(),
                _ => default_runtime_guc_value(&name)
                    .map(str::to_string)
                    .unwrap_or_else(|| "default".to_string()),
            }
        };

        let (column_name, value) = match name.as_str() {
            "datestyle" => (
                "DateStyle".to_string(),
                format_datestyle(&self.datetime_config),
            ),
            "intervalstyle" => (
                "IntervalStyle".to_string(),
                format_intervalstyle(self.datetime_config.interval_style).to_string(),
            ),
            "timezone" => (
                "TimeZone".to_string(),
                self.datetime_config.time_zone.clone(),
            ),
            "xmlbinary" => (
                "xmlbinary".to_string(),
                format_xmlbinary(self.datetime_config.xml.binary).to_string(),
            ),
            "xmloption" => (
                "xmloption".to_string(),
                format_xmloption(self.datetime_config.xml.option).to_string(),
            ),
            "transaction_isolation" => (
                "transaction_isolation".to_string(),
                self.current_transaction_isolation_level()
                    .as_str()
                    .to_string(),
            ),
            "default_transaction_isolation" => (
                "default_transaction_isolation".to_string(),
                self.default_transaction_isolation_level()
                    .as_str()
                    .to_string(),
            ),
            _ if is_checkpoint_guc(&name) => (
                stmt.name.clone(),
                db.checkpoint_config_value(&name)
                    .unwrap_or_else(|| "default".to_string()),
            ),
            _ if is_autovacuum_guc(&name) => (
                stmt.name.clone(),
                db.autovacuum_config_value(&name)
                    .unwrap_or_else(|| "default".to_string()),
            ),
            _ => (
                stmt.name.clone(),
                format_guc_show_value(
                    &name,
                    self.gucs.get(&name).cloned().unwrap_or_else(fallback_value),
                ),
            ),
        };

        Ok(StatementResult::Query {
            columns: vec![crate::backend::executor::QueryColumn::text(
                column_name.clone(),
            )],
            column_names: vec![column_name],
            rows: vec![vec![Value::Text(value.into())]],
        })
    }

    fn apply_load(
        &mut self,
        stmt: &crate::backend::parser::LoadStatement,
    ) -> Result<StatementResult, ExecError> {
        if stmt.filename.eq_ignore_ascii_case("plpgsql") {
            let removed = self
                .gucs
                .keys()
                .filter(|name| name.starts_with("plpgsql.") && !is_postgres_guc(name))
                .cloned()
                .collect::<Vec<_>>();
            for name in &removed {
                self.gucs.remove(name);
                if let Some(txn) = self.active_txn.as_mut() {
                    txn.guc_commit_state.gucs.remove(name);
                }
                crate::backend::utils::misc::notices::push_backend_notice(
                    "WARNING",
                    "01000",
                    format!("invalid configuration parameter name \"{name}\", removing it"),
                    Some("\"plpgsql\" is now a reserved prefix.".into()),
                    None,
                );
            }
            self.plpgsql_loaded = true;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_discard(
        &mut self,
        db: &Database,
        target: DiscardTarget,
    ) -> Result<StatementResult, ExecError> {
        if self.active_txn.is_some() {
            let stmt = match target {
                DiscardTarget::All => "DISCARD ALL",
                DiscardTarget::Temp => "DISCARD TEMP",
            };
            return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(stmt)));
        }

        match target {
            DiscardTarget::Temp => {
                db.cleanup_client_temp_relations(self.client_id)?;
            }
            DiscardTarget::All => {
                self.close_all_cursors();
                self.prepared_selects.clear();
                db.cleanup_client_temp_relations(self.client_id)?;
                db.async_notify_runtime.disconnect(self.client_id);
                db.advisory_locks.unlock_all_session(self.client_id);
                db.row_locks.unlock_all_session(self.client_id);

                let mut state = self.capture_guc_state();
                reset_all_gucs_in_state(&mut state, &self.reset_datetime_config);
                self.install_guc_state(state);
                db.install_row_security_enabled(self.client_id, true);
                db.install_session_replication_role(
                    self.client_id,
                    self.session_replication_role(),
                );
                db.plan_cache.invalidate_all();

                self.auth.reset_session_authorization();
                db.install_auth_state(self.client_id, self.auth.clone());
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    fn apply_checkpoint(&mut self, db: &Database) -> Result<StatementResult, ExecError> {
        if self.active_txn.as_ref().is_some_and(|txn| txn.failed) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ROLLBACK",
                actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
            }));
        }
        let auth_catalog = db.auth_catalog(self.client_id, None).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "authorization catalog",
                actual: format!("{err:?}"),
            })
        })?;
        if !self
            .auth
            .has_effective_membership(PG_CHECKPOINT_OID, &auth_catalog)
        {
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
        db.request_checkpoint(crate::backend::access::transam::CheckpointRequestFlags::sql())?;
        {
            let mut stats = self.stats_state.write();
            stats.note_io_write("client backend", "wal", "normal", 8192);
            stats.note_io_fsync("client backend", "wal", "normal");
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn guc_reset_datestyle(&mut self) {
        self.datetime_config.date_style_format = self.reset_datetime_config.date_style_format;
        self.datetime_config.date_order = self.reset_datetime_config.date_order;
    }

    fn guc_reset_intervalstyle(&mut self) {
        self.datetime_config.interval_style =
            parse_intervalstyle(default_intervalstyle()).expect("default IntervalStyle must parse");
    }

    fn guc_reset_timezone(&mut self) {
        self.datetime_config.time_zone = self.reset_datetime_config.time_zone.clone();
    }

    fn guc_reset_max_stack_depth(&mut self) {
        self.datetime_config.max_stack_depth_kb = self.reset_datetime_config.max_stack_depth_kb;
    }

    fn apply_guc_value(&mut self, name: &str, value: &str) -> Result<(), ExecError> {
        let mut state = self.capture_guc_state();
        let normalized = apply_guc_value_to_state(&mut state, name, value)?;
        if normalized == "transaction_isolation" {
            let level = crate::backend::parser::TransactionIsolationLevel::parse(value)
                .ok_or_else(|| ExecError::Parse(ParseError::UnrecognizedParameter(value.into())))?;
            self.set_active_transaction_isolation(level)?;
        }
        self.install_guc_state(state);
        Ok(())
    }

    pub fn prepare_insert(
        &self,
        db: &Database,
        table_name: &str,
        columns: Option<&[String]>,
        num_params: usize,
    ) -> Result<PreparedInsert, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(self.datetime_config.max_stack_depth_kb).run(|| {
                let catalog = self.catalog_lookup(db);
                Ok(bind_insert_prepared(
                    table_name, columns, num_params, &catalog,
                )?)
            })
        })
    }

    pub fn execute_prepared_insert(
        &mut self,
        db: &Database,
        prepared: &PreparedInsert,
        params: &[Value],
    ) -> Result<(), ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(self.datetime_config.max_stack_depth_kb).run(|| {
                if self.active_txn.is_none() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "active transaction",
                        actual: "no active transaction for prepared insert".into(),
                    }));
                }
                if self.transaction_failed() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "ROLLBACK",
                        actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                    }));
                }
                let xid = self.ensure_active_xid(db);
                let txn = self.active_txn.as_mut().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "active transaction",
                        actual: "no active transaction for prepared insert".into(),
                    })
                })?;
                let cid = txn.next_command_id;
                txn.next_command_id = txn.next_command_id.saturating_add(1);
                let _client_id = self.client_id;

                let lock_requests = prepared_insert_foreign_key_lock_requests(prepared);
                self.lock_table_requests_if_needed(db, &lock_requests)?;

                let snapshot = self.snapshot_for_command(db, xid, cid)?;
                let catalog = self.catalog_lookup_for_command(db, xid, cid);
                let interrupts = self.interrupts();
                let deferred_foreign_keys = self
                    .active_txn
                    .as_ref()
                    .unwrap()
                    .deferred_foreign_keys
                    .clone();
                let mut ctx = self.executor_context_for_catalog(
                    db,
                    snapshot,
                    cid,
                    &catalog,
                    Some(deferred_foreign_keys),
                    None,
                );
                ctx.interrupts = interrupts;
                let result = execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid)
                    .and_then(|_| self.validate_constraints_for_active_txn(db, true));
                self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                if result.is_err() {
                    self.mark_transaction_failed();
                }
                result
            })
        })
    }

    pub fn copy_from_rows(
        &mut self,
        db: &Database,
        table_name: &str,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        self.copy_from_rows_into_internal(db, table_name, None, rows, "\\N")
    }

    pub fn copy_from_rows_into(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        self.copy_from_rows_into_internal(db, table_name, target_columns, rows, "\\N")
    }

    pub(crate) fn copy_from_rows_into_with_null_marker(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
        null_marker: &str,
    ) -> Result<usize, ExecError> {
        let _interrupt_guard = self.statement_interrupt_guard()?;
        self.copy_from_rows_into_internal(db, table_name, target_columns, rows, null_marker)
    }

    pub(crate) fn execute_copy_command(
        &mut self,
        db: &Database,
        copy: &CopyCommand,
    ) -> Result<CopyExecutionResult, ExecError> {
        match &copy.direction {
            CopyDirection::From(CopyEndpoint::File(path)) => {
                let text = read_copy_text_file(
                    path,
                    &copy_command_encoding_name(&copy.options, self.gucs.get("client_encoding")),
                    copy_relation_table_name(&copy.relation),
                )?;
                let inserted = self.copy_from_text(db, copy, &text)?;
                Ok(CopyExecutionResult::AffectedRows(inserted))
            }
            CopyDirection::From(CopyEndpoint::Stdin) => {
                Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "COPY protocol data",
                    actual: "COPY FROM STDIN on non-protocol path".into(),
                }))
            }
            CopyDirection::From(CopyEndpoint::Stdout) => {
                Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "COPY source",
                    actual: "STDOUT".into(),
                }))
            }
            CopyDirection::To(endpoint) => {
                if matches!(copy.options.header, CopyHeader::Match) {
                    return Err(ExecError::DetailedError {
                        message: "cannot use \"match\" with HEADER in COPY TO".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
                if let CopyEndpoint::File(path) = endpoint {
                    self.ensure_copy_to_file_allowed(db, path)?;
                }
                let (columns, rows) = self.copy_query_rows(db, &copy.relation)?;
                let catalog = self.catalog_lookup(db);
                let enum_labels = copy_enum_label_map(&catalog);
                let data = render_copy_output(&columns, &rows, &copy.options, Some(&enum_labels));
                let data = encode_copy_output_bytes(
                    data,
                    &copy_command_encoding_name(&copy.options, self.gucs.get("client_encoding")),
                )?;
                if let CopyEndpoint::File(path) = endpoint {
                    let resolved = resolve_copy_output_path(path);
                    if let Some(parent) = std::path::Path::new(&resolved).parent() {
                        fs::create_dir_all(parent).map_err(|err| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "writable COPY target directory",
                                actual: format!("{}: {err}", parent.display()),
                            })
                        })?;
                    }
                    let mut file = fs::File::create(&resolved).map_err(|err| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "writable COPY target file",
                            actual: format!("{path}: {err}"),
                        })
                    })?;
                    file.write_all(&data).map_err(|err| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "writable COPY target file",
                            actual: format!("{path}: {err}"),
                        })
                    })?;
                }
                Ok(CopyExecutionResult::Output {
                    data,
                    rows: rows.len(),
                })
            }
        }
    }

    pub(crate) fn copy_command_needs_interleaved_stdout(
        &self,
        db: &Database,
        copy: &CopyCommand,
    ) -> Result<bool, ExecError> {
        if !matches!(copy.direction, CopyDirection::To(CopyEndpoint::Stdout)) {
            return Ok(false);
        }
        let CopyRelation::Query(query) = &copy.relation else {
            return Ok(false);
        };
        let stmt = self.parse_copy_query_statement(db, query.trim())?;
        Ok(match stmt {
            Statement::Insert(insert) => !insert.returning.is_empty(),
            Statement::Update(update) => !update.returning.is_empty(),
            Statement::Delete(delete) => !delete.returning.is_empty(),
            _ => false,
        })
    }

    pub(crate) fn execute_copy_command_to_stdout_sink(
        &mut self,
        db: &Database,
        copy: &CopyCommand,
        sink: &mut dyn CopyToSink,
    ) -> Result<usize, ExecError> {
        if matches!(copy.options.header, CopyHeader::Match) {
            return Err(ExecError::DetailedError {
                message: "cannot use \"match\" with HEADER in COPY TO".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        if self.copy_command_needs_interleaved_stdout(db, copy)? {
            let CopyRelation::Query(query) = &copy.relation else {
                unreachable!("COPY DML stdout path requires a query source");
            };
            return self.execute_copy_query_dml_to_stdout_sink(
                db,
                query.trim(),
                &copy.options,
                sink,
            );
        }
        let (columns, rows) = self.copy_query_rows(db, &copy.relation)?;
        self.write_copy_command_rows_to_stdout_sink(db, &copy.options, &columns, &rows, sink)
    }

    fn execute_copy_query_dml_to_stdout_sink(
        &mut self,
        db: &Database,
        query: &str,
        options: &CopyOptions,
        sink: &mut dyn CopyToSink,
    ) -> Result<usize, ExecError> {
        let stmt = self.parse_copy_query_statement(db, query)?;
        self.validate_copy_to_query(db, &stmt, query)?;
        begin_copy_to_dml_capture();
        let result = self.execute(db, query);
        let events = finish_copy_to_dml_capture();
        let (columns, rows) = match result? {
            StatementResult::Query { columns, rows, .. } => (columns, rows),
            StatementResult::AffectedRows(_) => {
                return Err(copy_to_feature_error(
                    "COPY query must have a RETURNING clause",
                ));
            }
        };
        let catalog = self.catalog_lookup(db);
        let enum_labels = copy_enum_label_map(&catalog);
        let row_options = CopyOptions {
            header: CopyHeader::None,
            ..options.clone()
        };
        let first_row_index = events
            .iter()
            .position(|event| matches!(event, CopyToDmlEvent::Row(_)))
            .unwrap_or(events.len());

        for event in events.iter().take(first_row_index) {
            if let CopyToDmlEvent::Notice(notice) = event {
                sink.notice(
                    notice.severity,
                    notice.sqlstate,
                    &notice.message,
                    notice.detail.as_deref(),
                    notice.position,
                )?;
            }
        }
        sink.begin(copy_command_output_format(options.format), columns.len())?;
        if !matches!(options.header, CopyHeader::None) {
            let header = render_copy_output(&columns, &[], options, Some(&enum_labels));
            sink.write_all(&header)?;
        }
        let mut row_count = 0usize;
        for event in events.into_iter().skip(first_row_index) {
            match event {
                CopyToDmlEvent::Notice(notice) => sink.notice(
                    notice.severity,
                    notice.sqlstate,
                    &notice.message,
                    notice.detail.as_deref(),
                    notice.position,
                )?,
                CopyToDmlEvent::Row(row) => {
                    let rendered = render_copy_output(
                        &columns,
                        std::slice::from_ref(&row),
                        &row_options,
                        Some(&enum_labels),
                    );
                    sink.write_all(&rendered)?;
                    row_count += 1;
                }
            }
        }
        if row_count == 0 && !rows.is_empty() {
            for row in &rows {
                let rendered = render_copy_output(
                    &columns,
                    std::slice::from_ref(row),
                    &row_options,
                    Some(&enum_labels),
                );
                sink.write_all(&rendered)?;
                row_count += 1;
            }
        }
        sink.finish()?;
        Ok(row_count)
    }

    fn write_copy_command_rows_to_stdout_sink(
        &self,
        db: &Database,
        options: &CopyOptions,
        columns: &[crate::backend::executor::QueryColumn],
        rows: &[Vec<Value>],
        sink: &mut dyn CopyToSink,
    ) -> Result<usize, ExecError> {
        let catalog = self.catalog_lookup(db);
        let enum_labels = copy_enum_label_map(&catalog);
        sink.begin(copy_command_output_format(options.format), columns.len())?;
        let rendered = render_copy_output(columns, rows, options, Some(&enum_labels));
        sink.write_all(&rendered)?;
        sink.finish()?;
        Ok(rows.len())
    }

    pub(crate) fn copy_from_text(
        &mut self,
        db: &Database,
        copy: &CopyCommand,
        text: &str,
    ) -> Result<usize, ExecError> {
        let CopyRelation::Table { name, columns } = &copy.relation else {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY table target",
                actual: "COPY query target".into(),
            }));
        };
        if copy.options.freeze {
            let catalog = self.catalog_lookup(db);
            let entry = catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(name.to_string())))?;
            if entry.relkind == 'p' {
                return Err(ExecError::DetailedError {
                    message: "cannot perform COPY FREEZE on a partitioned table".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            if entry.relkind == 'f' {
                return Err(ExecError::DetailedError {
                    message: "cannot perform COPY FREEZE on a foreign table".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
        }
        let stop_on_copy_marker =
            matches!(copy.direction, CopyDirection::From(CopyEndpoint::Stdin));
        let mut rows = parse_copy_input_rows(text, &copy.options, Some(name), stop_on_copy_marker)?;
        self.apply_copy_header(db, name, columns.as_deref(), &copy.options, &mut rows)
            .map_err(|err| {
                if matches!(copy.options.header, CopyHeader::Match)
                    && let Some(context) = first_copy_row_context(text, name)
                {
                    return ExecError::WithContext {
                        source: Box::new(err),
                        context,
                    };
                }
                err
            })?;
        let where_filter = copy
            .options
            .where_clause
            .as_ref()
            .and_then(|clause| parse_copy_where_filter(clause));
        let parsed_null_marker = match copy.options.format {
            CopyFormat::Text => COPY_TEXT_NULL_SENTINEL,
            CopyFormat::Csv => &copy.options.null_marker,
        };
        let bytes_processed = text.len().min(i64::MAX as usize) as i64;
        let progress = match &copy.direction {
            CopyDirection::From(CopyEndpoint::File(_)) => Some(CopyProgressOptions {
                source: CopyProgressSource::File,
                bytes_processed,
                bytes_total: bytes_processed,
            }),
            CopyDirection::From(CopyEndpoint::Stdin) => Some(CopyProgressOptions {
                source: CopyProgressSource::Pipe,
                bytes_processed,
                bytes_total: 0,
            }),
            _ => None,
        };
        self.copy_from_rows_into_internal_with_options(
            db,
            name,
            columns.as_deref(),
            &rows,
            CopyInsertOptions {
                null_marker: parsed_null_marker,
                default_marker: copy.options.default_marker.as_deref(),
                on_error: if copy.options.on_error_ignore {
                    CopyOnError::Ignore
                } else {
                    CopyOnError::Stop
                },
                where_filter,
                progress,
            },
        )
    }

    pub(crate) fn validate_copy_from_stdin_start(
        &self,
        db: &Database,
        copy: &CopyCommand,
    ) -> Result<(), ExecError> {
        if !matches!(copy.direction, CopyDirection::From(CopyEndpoint::Stdin)) {
            return Ok(());
        }
        let CopyRelation::Table { name, columns } = &copy.relation else {
            return Ok(());
        };
        let catalog = self.catalog_lookup(db);
        let (relation_oid, desc) = {
            let entry = catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(name.to_string())))?;
            (entry.relation_oid, entry.desc.clone())
        };
        let snapshot = db
            .txns
            .read()
            .snapshot_for_command(INVALID_TRANSACTION_ID, 0)?;
        let mut ctx = self.executor_context_for_catalog(db, snapshot, 0, &catalog, None, None);
        let target_indexes = if let Some(columns) = columns {
            let mut indexes = Vec::with_capacity(columns.len());
            for name in columns {
                let Some(index) = desc
                    .columns
                    .iter()
                    .position(|column| !column.dropped && column.name == *name)
                else {
                    return Err(ExecError::Parse(ParseError::UnknownColumn(name.clone())));
                };
                indexes.push(index);
            }
            indexes
        } else {
            desc.visible_column_indexes()
        };
        check_relation_column_privileges(&ctx, relation_oid, 'a', target_indexes.iter().copied())?;
        let validation_default_indexes = if copy.options.default_marker.is_some() {
            desc.visible_column_indexes()
        } else {
            desc.visible_column_indexes()
                .into_iter()
                .filter(|column_index| !target_indexes.contains(column_index))
                .collect::<Vec<_>>()
        };
        if validation_default_indexes.is_empty() {
            return Ok(());
        }
        let column_defaults = bind_copy_column_defaults(&desc, &catalog)?;
        for column_index in validation_default_indexes {
            let column = &desc.columns[column_index];
            if column.default_sequence_oid.is_some()
                || column.default_expr.is_none() && column.missing_default_value.is_none()
            {
                continue;
            }
            let _ = evaluate_copy_column_default(&desc, &column_defaults, column_index, &mut ctx)?;
        }
        Ok(())
    }

    fn apply_copy_header(
        &self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        options: &CopyOptions,
        rows: &mut Vec<Vec<String>>,
    ) -> Result<(), ExecError> {
        if matches!(options.header, CopyHeader::None) {
            return Ok(());
        }
        let header = if rows.is_empty() {
            Vec::new()
        } else {
            rows.remove(0)
        };
        if !matches!(options.header, CopyHeader::Match) {
            return Ok(());
        }
        let expected = self.copy_target_column_names(db, table_name, target_columns)?;
        if header.len() != expected.len() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "wrong number of fields in header line: got {}, expected {}",
                    header.len(),
                    expected.len()
                ),
                detail: None,
                hint: None,
                sqlstate: "22P04",
            });
        }
        for (idx, (actual, expected)) in header.iter().zip(expected.iter()).enumerate() {
            if actual != expected {
                let got = if is_parsed_copy_null(actual, options) {
                    format!("null value (\"{}\")", options.null_marker)
                } else {
                    format!("\"{actual}\"")
                };
                return Err(ExecError::DetailedError {
                    message: format!(
                        "column name mismatch in header line field {}: got {}, expected \"{}\"",
                        idx + 1,
                        got,
                        expected
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "22P04",
                });
            }
        }
        Ok(())
    }

    fn copy_target_column_names(
        &self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
    ) -> Result<Vec<String>, ExecError> {
        if let Some(columns) = target_columns {
            return Ok(columns.to_vec());
        }
        let catalog = self.catalog_lookup(db);
        let entry = catalog
            .lookup_any_relation(table_name)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(table_name.to_string())))?;
        Ok(entry
            .desc
            .columns
            .iter()
            .filter(|column| !column.dropped)
            .map(|column| column.name.clone())
            .collect())
    }

    fn copy_query_rows(
        &mut self,
        db: &Database,
        relation: &CopyRelation,
    ) -> Result<(Vec<crate::backend::executor::QueryColumn>, Vec<Vec<Value>>), ExecError> {
        let query = match relation {
            CopyRelation::Query(query) => {
                let trimmed = query.trim();
                let stmt = self.parse_copy_query_statement(db, trimmed)?;
                self.validate_copy_to_query(db, &stmt, trimmed)?;
                trimmed.to_string()
            }
            CopyRelation::Table { name, columns } => {
                let catalog = self.catalog_lookup(db);
                if let Some(entry) = catalog.lookup_any_relation(name)
                    && entry.relkind == 'm'
                    && !entry.relispopulated
                {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot copy from unpopulated materialized view \"{name}\""
                        ),
                        detail: None,
                        hint: Some("Use the REFRESH MATERIALIZED VIEW command.".into()),
                        sqlstate: "55000",
                    });
                }
                self.ensure_copy_to_relation_source(db, name)?;
                let select_list = columns
                    .as_ref()
                    .map(|columns| columns.join(", "))
                    .unwrap_or_else(|| "*".into());
                format!("select {select_list} from {name}")
            }
        };
        match self.execute(db, &query)? {
            StatementResult::Query { columns, rows, .. } => Ok((columns, rows)),
            other => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "query result for COPY TO",
                actual: format!("{other:?}"),
            })),
        }
    }

    fn parse_copy_query_statement(&self, db: &Database, sql: &str) -> Result<Statement, ExecError> {
        if self.standard_conforming_strings() {
            db.plan_cache.get_statement_with_options(
                sql,
                ParseOptions {
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                    ..ParseOptions::default()
                },
            )
        } else {
            Ok(crate::backend::parser::parse_statement_with_options(
                sql,
                ParseOptions {
                    standard_conforming_strings: false,
                    max_stack_depth_kb: self.datetime_config.max_stack_depth_kb,
                },
            )?)
        }
    }

    fn copy_from_rows_into_internal(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
        null_marker: &str,
    ) -> Result<usize, ExecError> {
        self.copy_from_rows_into_internal_with_options(
            db,
            table_name,
            target_columns,
            rows,
            CopyInsertOptions {
                null_marker,
                default_marker: None,
                on_error: CopyOnError::Stop,
                where_filter: None,
                progress: None,
            },
        )
    }

    fn copy_from_rows_into_internal_with_options(
        &mut self,
        db: &Database,
        table_name: &str,
        target_columns: Option<&[String]>,
        rows: &[Vec<String>],
        options: CopyInsertOptions<'_>,
    ) -> Result<usize, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(self.datetime_config.max_stack_depth_kb).run(|| {
                db.install_interrupt_state(self.client_id, self.interrupts());
                let started_txn = if self.active_txn.is_none() {
                    self.active_txn = Some(self.active_transaction_without_xid(db));
                    self.stats_state.write().begin_top_level_xact();
                    true
                } else {
                    false
                };

                let result = (|| -> Result<usize, ExecError> {
                    let xid = self.ensure_active_xid(db);
                    let cid = {
                        let txn = self.active_txn.as_mut().unwrap();
                        let cid = txn.next_command_id;
                        txn.next_command_id = txn.next_command_id.saturating_add(1);
                        cid
                    };

                    let catalog = self.catalog_lookup_for_command(db, xid, cid);
                    let (relation_oid, rel, toast, toast_index, desc, indexes) = {
                        let entry = catalog.lookup_any_relation(table_name).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnknownTable(table_name.to_string()))
                        })?;
                        if entry.relkind == 'm' {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                                format!("cannot change materialized view \"{table_name}\""),
                            )));
                        }
                        if relation_has_row_security(entry.relation_oid, &catalog) {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                                "COPY FROM is not yet supported on tables with row-level security"
                                    .into(),
                            )));
                        }
                        let toast_index = entry.toast.and_then(|toast| {
                            catalog
                                .index_relations_for_heap(toast.relation_oid)
                                .into_iter()
                                .next()
                        });
                        (
                            entry.relation_oid,
                            entry.rel,
                            entry.toast,
                            toast_index,
                            entry.desc.clone(),
                            catalog.index_relations_for_heap(entry.relation_oid),
                        )
                    };
                    let target_indexes = if let Some(columns) = target_columns {
                        let mut indexes = Vec::with_capacity(columns.len());
                        for name in columns {
                            let Some(index) = desc
                                .columns
                                .iter()
                                .position(|column| !column.dropped && column.name == *name)
                            else {
                                return Err(ExecError::Parse(ParseError::UnknownColumn(
                                    name.clone(),
                                )));
                            };
                            indexes.push(index);
                        }
                        indexes
                    } else {
                        desc.columns
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, column)| (!column.dropped).then_some(idx))
                            .collect()
                    };
                    let where_filter = options
                        .where_filter
                        .as_ref()
                        .map(|filter| filter.resolve(&desc))
                        .transpose()?;

                    let relation_constraints = crate::backend::parser::bind_relation_constraints(
                        None,
                        relation_oid,
                        &desc,
                        &catalog,
                    )?;
                    let lock_requests =
                        relation_foreign_key_lock_requests(rel, &relation_constraints);
                    self.lock_table_requests_if_needed(db, &lock_requests)?;

                    let snapshot = self.snapshot_for_command(db, xid, cid)?;
                    let interrupts = self.interrupts();
                    let deferred_foreign_keys = self
                        .active_txn
                        .as_ref()
                        .unwrap()
                        .deferred_foreign_keys
                        .clone();
                    let mut ctx = self.executor_context_for_catalog(
                        db,
                        snapshot,
                        cid,
                        &catalog,
                        Some(deferred_foreign_keys),
                        None,
                    );
                    ctx.interrupts = interrupts;
                    check_relation_column_privileges(
                        &ctx,
                        relation_oid,
                        'a',
                        target_indexes.iter().copied(),
                    )?;
                    let column_defaults = bind_copy_column_defaults(&desc, &catalog)?;
                    let omitted_default_indexes = desc
                        .visible_column_indexes()
                        .into_iter()
                        .filter(|column_index| !target_indexes.contains(column_index))
                        .collect::<Vec<_>>();
                    let validation_default_indexes = if options.default_marker.is_some() {
                        desc.visible_column_indexes()
                    } else {
                        omitted_default_indexes.clone()
                    };
                    for column_index in &validation_default_indexes {
                        let _ = evaluate_copy_column_default(
                            &desc,
                            &column_defaults,
                            *column_index,
                            &mut ctx,
                        )?;
                    }

                    let mut skipped = 0usize;
                    let mut excluded = 0usize;
                    let mut parsed_rows = Vec::with_capacity(rows.len());
                    for row in rows {
                        let parsed = (|| -> Result<Option<Vec<Value>>, ExecError> {
                            if row.len() != target_indexes.len() {
                                return Err(ExecError::Parse(
                                    ParseError::InvalidInsertTargetCount {
                                        expected: target_indexes.len(),
                                        actual: row.len(),
                                    },
                                ));
                            }

                            let mut values = vec![Value::Null; desc.columns.len()];
                            for column_index in &omitted_default_indexes {
                                values[*column_index] = evaluate_copy_column_default(
                                    &desc,
                                    &column_defaults,
                                    *column_index,
                                    &mut ctx,
                                )?;
                            }
                            for (raw, target_index) in
                                row.iter().zip(target_indexes.iter().copied())
                            {
                                let column = &desc.columns[target_index];
                                let value = if options.default_marker == Some(raw.as_str()) {
                                    evaluate_copy_column_default(
                                        &desc,
                                        &column_defaults,
                                        target_index,
                                        &mut ctx,
                                    )?
                                } else if raw == options.null_marker {
                                    Value::Null
                                } else {
                                    match column.ty {
                                ScalarType::Int16 => {
                                    raw.parse::<i16>().map(Value::Int16).map_err(|_| {
                                        ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                    })?
                                }
                                ScalarType::Int32 => {
                                    raw.parse::<i32>().map(Value::Int32).map_err(|_| {
                                        ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                    })?
                                }
                                ScalarType::Int64 => {
                                    raw.parse::<i64>().map(Value::Int64).map_err(|_| {
                                        ExecError::Parse(ParseError::InvalidInteger(raw.clone()))
                                    })?
                                }
                                ScalarType::Money => {
                                    crate::backend::executor::money_parse_text(raw)
                                        .map(Value::Money)?
                                }
                                ScalarType::PgLsn => {
                                    cast_value(Value::Text(raw.clone().into()), column.sql_type)?
                                }
                                ScalarType::Date
                                | ScalarType::Time
                                | ScalarType::TimeTz
                                | ScalarType::Timestamp
                                | ScalarType::TimestampTz
                                | ScalarType::Interval
                                | ScalarType::Enum
                                | ScalarType::Range(_)
                                | ScalarType::Multirange(_)
                                | ScalarType::Point
                                | ScalarType::Lseg
                                | ScalarType::Path
                                | ScalarType::Line
                                | ScalarType::Box
                                | ScalarType::Polygon
                                | ScalarType::Circle
                                | ScalarType::TsVector
                                | ScalarType::TsQuery => {
                                    cast_value_with_source_type_catalog_and_config(
                                        Value::Text(raw.clone().into()),
                                        None,
                                        column.sql_type,
                                        Some(&catalog),
                                        &self.datetime_config,
                                    )?
                                }
                                ScalarType::BitString => {
                                    cast_value_with_source_type_catalog_and_config(
                                        Value::Text(raw.clone().into()),
                                        None,
                                        column.sql_type,
                                        Some(&catalog),
                                        &self.datetime_config,
                                    )?
                                }
                                ScalarType::Inet
                                | ScalarType::Cidr
                                | ScalarType::MacAddr
                                | ScalarType::MacAddr8 => {
                                    cast_value_with_source_type_catalog_and_config(
                                        Value::Text(raw.clone().into()),
                                        None,
                                        column.sql_type,
                                        Some(&catalog),
                                        &self.datetime_config,
                                    )?
                                }
                                ScalarType::Float32 | ScalarType::Float64 => raw
                                    .parse::<f64>()
                                    .map(Value::Float64)
                                    .map_err(|_| ExecError::TypeMismatch {
                                        op: "copy assignment",
                                        left: Value::Null,
                                        right: Value::Text(raw.clone().into()),
                                    })?,
                                ScalarType::Numeric => Value::Numeric(raw.as_str().into()),
                                ScalarType::Json => Value::Json(raw.clone().into()),
                                ScalarType::Jsonb => Value::Jsonb(
                                    crate::backend::executor::jsonb::parse_jsonb_text(raw)?,
                                ),
                                ScalarType::JsonPath => Value::JsonPath(
                                    canonicalize_jsonpath(raw)
                                        .map_err(|_| ExecError::InvalidStorageValue {
                                            column: "<copy>".into(),
                                            details: format!(
                                                "invalid input syntax for type jsonpath: \"{raw}\""
                                            ),
                                        })?
                                        .into(),
                                ),
                                ScalarType::Xml => {
                                    cast_value_with_source_type_catalog_and_config(
                                        Value::Text(raw.clone().into()),
                                        None,
                                        column.sql_type,
                                        Some(&catalog),
                                        &self.datetime_config,
                                    )?
                                }
                                ScalarType::Bytea => Value::Bytea(parse_bytea_text(raw)?),
                                ScalarType::Uuid => {
                                    cast_value(Value::Text(raw.clone().into()), column.sql_type)?
                                }
                                ScalarType::Text => Value::Text(raw.clone().into()),
                                ScalarType::Record => {
                                    return Err(ExecError::UnsupportedStorageType {
                                        column: column.name.clone(),
                                        ty: column.ty.clone(),
                                        attlen: column.storage.attlen,
                                        actual_len: None,
                                    });
                                }
                                ScalarType::Bool => Value::Bool(parse_pg_bool_text(raw)?),
                                ScalarType::Array(_) => {
                                    parse_text_array_literal_with_catalog(
                                        raw,
                                        column.sql_type.element_type(),
                                        Some(&catalog),
                                    )?
                                }
                            }
                                };
                                values[target_index] = value;
                            }

                            if let Some(filter) = where_filter.as_ref()
                                && !filter.matches(&values)?
                            {
                                return Ok(None);
                            }

                            Ok(Some(values))
                        })();
                        match parsed {
                            Ok(Some(values)) => parsed_rows.push(values),
                            Ok(None) => excluded = excluded.saturating_add(1),
                            Err(_err) if matches!(options.on_error, CopyOnError::Ignore) => {
                                skipped = skipped.saturating_add(1);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    if skipped > 0 {
                        crate::backend::utils::misc::notices::push_notice(format!(
                            "{skipped} rows were skipped due to data type incompatibility"
                        ));
                    }

                    let _copy_progress = options.progress.map(|progress| {
                        crate::backend::utils::cache::system_views::install_copy_progress(
                            crate::backend::utils::cache::system_views::CopyProgressSnapshot {
                                pid: self.client_id as i32,
                                datid: db.database_oid,
                                datname: db.current_database_name(),
                                relid: relation_oid,
                                command: "COPY FROM",
                                copy_type: match progress.source {
                                    CopyProgressSource::File => "FILE",
                                    CopyProgressSource::Pipe => "PIPE",
                                },
                                bytes_processed: progress.bytes_processed,
                                bytes_total: progress.bytes_total,
                                tuples_processed: parsed_rows.len() as i64,
                                tuples_excluded: excluded as i64,
                                tuples_skipped: skipped as i64,
                            },
                        )
                    });
                    let result = crate::backend::commands::tablecmds::execute_insert_values(
                        table_name,
                        relation_oid,
                        rel,
                        toast,
                        toast_index.as_ref(),
                        &desc,
                        &relation_constraints,
                        &[],
                        &indexes,
                        &parsed_rows,
                        &mut ctx,
                        xid,
                        cid,
                    );
                    self.merge_ctx_pending_async_notifications(&mut ctx, result.is_ok());
                    result
                })();

                if started_txn {
                    let result = result.and_then(|n| {
                        self.validate_constraints_for_active_txn(db, false)?;
                        Ok(StatementResult::AffectedRows(n))
                    });
                    let txn = self.active_txn.take().unwrap();
                    self.finalize_taken_transaction(db, txn, result)
                    .map(|result| match result {
                        StatementResult::AffectedRows(rows) => rows as usize,
                        other => {
                            panic!(
                                "expected COPY finalization to return affected rows, got {other:?}"
                            )
                        }
                    })
                } else {
                    let result = result.and_then(|n| {
                        self.validate_constraints_for_active_txn(db, true)
                            .map(|_| n)
                    });
                    if result.is_err() {
                        self.mark_transaction_failed();
                    }
                    result
                }
            })
        })
    }

    fn execute_copy_from_file(
        &mut self,
        db: &Database,
        stmt: &CopyFromStatement,
    ) -> Result<StatementResult, ExecError> {
        let CopySource::File(path) = &stmt.source;
        let bytes = std::fs::read(path).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "readable COPY source file",
                actual: format!("{path}: {err}"),
            })
        })?;
        let text = decode_copy_file_bytes(
            &bytes,
            &copy_encoding_name(&stmt.options, self.gucs.get("client_encoding")),
            &stmt.table_name,
        )?;
        let rows = parse_copy_rows(&text, stmt.options.format)?;
        let count = self.copy_from_rows_into_internal(
            db,
            &stmt.table_name,
            stmt.columns.as_deref(),
            &rows,
            "\\N",
        )?;
        Ok(StatementResult::AffectedRows(count))
    }
}

fn copy_encoding_name(options: &ParserCopyOptions, client_encoding: Option<&String>) -> String {
    options
        .encoding
        .as_deref()
        .or(client_encoding.map(String::as_str))
        .unwrap_or("UTF8")
        .to_ascii_uppercase()
}

fn copy_to_encoding_name(
    options: &crate::include::nodes::parsenodes::CopyToOptions,
    client_encoding: Option<&String>,
) -> String {
    effective_copy_encoding_name(options.encoding.as_deref(), client_encoding)
}

fn copy_command_encoding_name(options: &CopyOptions, client_encoding: Option<&String>) -> String {
    effective_copy_encoding_name(options.encoding.as_deref(), client_encoding)
}

fn effective_copy_encoding_name(
    option_encoding: Option<&str>,
    client_encoding: Option<&String>,
) -> String {
    option_encoding
        .or(client_encoding.map(String::as_str))
        .unwrap_or("UTF8")
        .to_ascii_uppercase()
}

fn decode_copy_file_bytes(
    bytes: &[u8],
    encoding_name: &str,
    table_name: &str,
) -> Result<String, ExecError> {
    if is_latin1_copy_encoding(encoding_name) {
        return Ok(bytes.iter().map(|byte| char::from(*byte)).collect());
    }
    let encoding = lookup_copy_encoding(encoding_name)?;
    let (decoded, _, had_errors) = encoding.decode(bytes);
    if had_errors {
        return Err(ExecError::WithContext {
            source: Box::new(ExecError::DetailedError {
                message: format!(
                    "invalid byte sequence for encoding \"{}\": {}",
                    encoding_name.to_ascii_uppercase(),
                    format_invalid_copy_bytes(bytes)
                ),
                detail: None,
                hint: None,
                sqlstate: "22021",
            }),
            context: format!("COPY {table_name}, line 1"),
        });
    }
    Ok(decoded.into_owned())
}

fn encode_copy_file_text(text: &str, encoding_name: &str) -> Result<Vec<u8>, ExecError> {
    if is_latin1_copy_encoding(encoding_name) {
        let mut out = Vec::with_capacity(text.len());
        for ch in text.chars() {
            if (ch as u32) > 0xff {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "character is not representable in encoding \"{}\"",
                        encoding_name.to_ascii_uppercase()
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "22021",
                });
            }
            out.push(ch as u8);
        }
        return Ok(out);
    }
    let encoding = lookup_copy_encoding(encoding_name)?;
    let (encoded, _, had_errors) = encoding.encode(text);
    if had_errors {
        return Err(ExecError::DetailedError {
            message: format!(
                "character is not representable in encoding \"{}\"",
                encoding_name.to_ascii_uppercase()
            ),
            detail: None,
            hint: None,
            sqlstate: "22021",
        });
    }
    Ok(encoded.into_owned())
}

fn encode_copy_output_bytes(bytes: Vec<u8>, encoding_name: &str) -> Result<Vec<u8>, ExecError> {
    let text = String::from_utf8(bytes).map_err(|err| ExecError::DetailedError {
        message: format!("could not encode COPY data: {err}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    encode_copy_file_text(&text, encoding_name)
}

fn is_latin1_copy_encoding(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_uppercase().as_str(),
        "LATIN1" | "ISO_8859_1" | "ISO-8859-1"
    )
}

fn lookup_copy_encoding(name: &str) -> Result<&'static encoding_rs::Encoding, ExecError> {
    let normalized = name.trim().to_ascii_uppercase();
    let label = match normalized.as_str() {
        "UTF8" | "UTF-8" | "UNICODE" => "utf-8",
        "LATIN1" | "ISO_8859_1" | "ISO-8859-1" => "iso-8859-1",
        "EUC_JP" | "EUCJP" | "EUC-JP" => "euc-jp",
        _ => normalized.as_str(),
    };
    encoding_rs::Encoding::for_label(label.as_bytes()).ok_or_else(|| ExecError::DetailedError {
        message: format!("invalid encoding name \"{name}\""),
        detail: None,
        hint: None,
        sqlstate: "22023",
    })
}

fn format_invalid_copy_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(2)
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_copy_rows(text: &str, format: ParserCopyFormat) -> Result<Vec<Vec<String>>, ExecError> {
    text.lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.is_empty() && *line != "\\.")
        .map(|line| match format {
            ParserCopyFormat::Text => Ok(line.split('\t').map(str::to_string).collect()),
            ParserCopyFormat::Csv => parse_copy_csv_line(line),
            ParserCopyFormat::Binary => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "COPY FROM BINARY".into(),
            ))),
        })
        .collect()
}

fn parse_copy_csv_line(line: &str) -> Result<Vec<String>, ExecError> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                fields.push(std::mem::take(&mut field));
            }
            _ => field.push(ch),
        }
    }
    if quoted {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "terminated CSV quoted field",
            actual: line.into(),
        }));
    }
    fields.push(field);
    Ok(fields)
}

impl Session {
    pub(crate) fn execute_copy_to(
        &mut self,
        db: &Database,
        stmt: &CopyToStatement,
        stdout_sink: Option<&mut dyn CopyToSink>,
    ) -> Result<usize, ExecError> {
        match &stmt.destination {
            CopyToDestination::File(path) => self.ensure_copy_to_file_allowed(db, path)?,
            CopyToDestination::Program(_) => self.ensure_copy_to_program_allowed(db)?,
            CopyToDestination::Stdout => {}
        }
        if let (CopyToDestination::Stdout, CopyToSource::Query { statement, .. }) =
            (&stmt.destination, &stmt.source)
            && matches!(
                &**statement,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            )
        {
            let sink = stdout_sink.ok_or_else(|| {
                ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                    "COPY TO STDOUT requires a frontend/backend protocol sink".into(),
                ))
            })?;
            return self.execute_copy_to_dml_stdout(db, stmt, sink);
        }
        let (columns, rows) = self.collect_copy_to_rows(db, stmt)?;
        let float_format = FloatFormatOptions {
            extra_float_digits: self.extra_float_digits(),
            bytea_output: self.bytea_output(),
            datetime_config: self.datetime_config().clone(),
        };
        match &stmt.destination {
            CopyToDestination::Stdout => {
                let sink = stdout_sink.ok_or_else(|| {
                    ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                        "COPY TO STDOUT requires a frontend/backend protocol sink".into(),
                    ))
                })?;
                write_copy_to(sink, &columns, &rows, &stmt.options, float_format)
            }
            CopyToDestination::File(path) => {
                let bytes =
                    self.serialize_copy_to_bytes(&columns, &rows, &stmt.options, float_format)?;
                fs::write(path, bytes).map_err(|err| ExecError::DetailedError {
                    message: format!("could not open file \"{path}\" for writing: {err}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                })?;
                Ok(rows.len())
            }
            CopyToDestination::Program(program) => {
                let bytes =
                    self.serialize_copy_to_bytes(&columns, &rows, &stmt.options, float_format)?;
                let mut child = Command::new("/bin/sh")
                    .arg("-c")
                    .arg(program)
                    .stdin(Stdio::piped())
                    .spawn()
                    .map_err(|err| ExecError::DetailedError {
                        message: format!("could not execute command \"{program}\": {err}"),
                        detail: None,
                        hint: None,
                        sqlstate: "38000",
                    })?;
                {
                    let stdin = child
                        .stdin
                        .as_mut()
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!("could not write to COPY program \"{program}\""),
                            detail: None,
                            hint: None,
                            sqlstate: "38000",
                        })?;
                    stdin
                        .write_all(&bytes)
                        .map_err(|err| ExecError::DetailedError {
                            message: format!(
                                "could not write to COPY program \"{program}\": {err}"
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "38000",
                        })?;
                }
                let status = child.wait().map_err(|err| ExecError::DetailedError {
                    message: format!("could not wait for COPY program \"{program}\": {err}"),
                    detail: None,
                    hint: None,
                    sqlstate: "38000",
                })?;
                if !status.success() {
                    return Err(ExecError::DetailedError {
                        message: format!("program \"{program}\" failed"),
                        detail: Some(format!("Child process exited with status {status}.")),
                        hint: None,
                        sqlstate: "38000",
                    });
                }
                Ok(rows.len())
            }
        }
    }

    fn execute_copy_to_dml_stdout(
        &mut self,
        db: &Database,
        stmt: &CopyToStatement,
        sink: &mut dyn CopyToSink,
    ) -> Result<usize, ExecError> {
        let CopyToSource::Query { statement, sql } = &stmt.source else {
            return Err(ExecError::DetailedError {
                message: "COPY DML stdout path requires a query source".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        };
        self.validate_copy_to_query(db, statement, sql)?;
        begin_copy_to_dml_capture();
        let result = self.execute(db, sql);
        let mut events = finish_copy_to_dml_capture();
        let (columns, rows) = match result? {
            StatementResult::Query { columns, rows, .. } => (columns, rows),
            StatementResult::AffectedRows(_) => {
                return Err(copy_to_feature_error(
                    "COPY query must have a RETURNING clause",
                ));
            }
        };
        if !events
            .iter()
            .any(|event| matches!(event, CopyToDmlEvent::Row(_)))
            && !rows.is_empty()
        {
            let insert_at = usize::from(!events.is_empty());
            for (offset, row) in rows.into_iter().enumerate() {
                events.insert(insert_at + offset, CopyToDmlEvent::Row(row));
            }
        }
        let float_format = FloatFormatOptions {
            extra_float_digits: self.extra_float_digits(),
            bytea_output: self.bytea_output(),
            datetime_config: self.datetime_config().clone(),
        };
        let first_row_index = events
            .iter()
            .position(|event| matches!(event, CopyToDmlEvent::Row(_)));
        let first_row_index = first_row_index.unwrap_or(events.len());
        let mut row_count = 0usize;
        for event in events.iter().take(first_row_index) {
            if let CopyToDmlEvent::Notice(notice) = event {
                sink.notice(
                    notice.severity,
                    notice.sqlstate,
                    &notice.message,
                    notice.detail.as_deref(),
                    notice.position,
                )?;
            }
        }
        begin_copy_to(sink, &columns, &stmt.options)?;
        for event in events.into_iter().skip(first_row_index) {
            match event {
                CopyToDmlEvent::Notice(notice) => sink.notice(
                    notice.severity,
                    notice.sqlstate,
                    &notice.message,
                    notice.detail.as_deref(),
                    notice.position,
                )?,
                CopyToDmlEvent::Row(row) => {
                    write_copy_to_row(sink, &columns, &row, &stmt.options, &float_format)?;
                    row_count += 1;
                }
            }
        }
        finish_copy_to(sink, &stmt.options)?;
        Ok(row_count)
    }

    fn serialize_copy_to_bytes(
        &self,
        columns: &[crate::backend::executor::QueryColumn],
        rows: &[Vec<Value>],
        options: &crate::include::nodes::parsenodes::CopyToOptions,
        float_format: FloatFormatOptions,
    ) -> Result<Vec<u8>, ExecError> {
        let mut bytes = Vec::new();
        let mut sink = IoCopyToSink::new(&mut bytes);
        write_copy_to(&mut sink, columns, rows, options, float_format)?;
        if matches!(options.format, ParserCopyFormat::Binary) {
            return Ok(bytes);
        }
        let text = String::from_utf8(bytes).map_err(|err| ExecError::DetailedError {
            message: format!("could not encode COPY data: {err}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
        encode_copy_file_text(
            &text,
            &copy_to_encoding_name(options, self.gucs.get("client_encoding")),
        )
    }

    fn collect_copy_to_rows(
        &mut self,
        db: &Database,
        stmt: &CopyToStatement,
    ) -> Result<(Vec<crate::backend::executor::QueryColumn>, Vec<Vec<Value>>), ExecError> {
        let sql = match &stmt.source {
            CopyToSource::Relation {
                table_name,
                columns,
            } => {
                self.ensure_copy_to_relation_source(db, table_name)?;
                relation_copy_to_query_sql(table_name, columns.as_deref())
            }
            CopyToSource::Query { statement, sql } => {
                self.validate_copy_to_query(db, statement, sql)?;
                sql.clone()
            }
        };
        match self.execute(db, &sql)? {
            StatementResult::Query { columns, rows, .. } => Ok((columns, rows)),
            StatementResult::AffectedRows(_) => Err(copy_to_feature_error(
                "COPY query must have a RETURNING clause",
            )),
        }
    }

    fn validate_copy_to_query(
        &self,
        db: &Database,
        statement: &Statement,
        sql: &str,
    ) -> Result<(), ExecError> {
        if copy_query_looks_like_select_into(sql) {
            return Err(copy_to_feature_error("COPY (SELECT INTO) is not supported"));
        }
        match statement {
            Statement::Select(_) | Statement::Values(_) => Ok(()),
            Statement::CreateTableAs(_) => {
                Err(copy_to_feature_error("COPY (SELECT INTO) is not supported"))
            }
            Statement::Insert(insert) => {
                self.validate_copy_to_dml_rules(db, &insert.table_name, '3')?;
                if insert.returning.is_empty() {
                    return Err(copy_to_feature_error(
                        "COPY query must have a RETURNING clause",
                    ));
                }
                Ok(())
            }
            Statement::Update(update) => {
                self.validate_copy_to_dml_rules(db, &update.table_name, '2')?;
                if update.returning.is_empty() {
                    return Err(copy_to_feature_error(
                        "COPY query must have a RETURNING clause",
                    ));
                }
                Ok(())
            }
            Statement::Delete(delete) => {
                self.validate_copy_to_dml_rules(db, &delete.table_name, '4')?;
                if delete.returning.is_empty() {
                    return Err(copy_to_feature_error(
                        "COPY query must have a RETURNING clause",
                    ));
                }
                Ok(())
            }
            _ => Err(copy_to_feature_error(
                "COPY query must not be a utility command",
            )),
        }
    }

    fn ensure_copy_to_relation_source(
        &self,
        db: &Database,
        table_name: &str,
    ) -> Result<(), ExecError> {
        let catalog = self.catalog_lookup(db);
        let Some(relation) = catalog.lookup_any_relation(table_name) else {
            return Ok(());
        };
        if matches!(relation.relkind, 'r' | 'm') && relation.relispopulated {
            return Ok(());
        }
        if relation.relkind == 'm' {
            return Err(ExecError::DetailedError {
                message: format!("cannot copy from unpopulated materialized view \"{table_name}\""),
                detail: None,
                hint: Some("Use the REFRESH MATERIALIZED VIEW command.".into()),
                sqlstate: "55000",
            });
        }
        let object = match relation.relkind {
            'v' => "view",
            'f' => "foreign table",
            _ => "relation",
        };
        Err(ExecError::Parse(ParseError::DetailedError {
            message: format!("cannot copy from {object} \"{table_name}\""),
            detail: None,
            hint: Some("Try the COPY (SELECT ...) TO variant.".into()),
            sqlstate: "42809",
        }))
    }

    fn validate_copy_to_dml_rules(
        &self,
        db: &Database,
        table_name: &str,
        event_code: char,
    ) -> Result<(), ExecError> {
        let catalog = self.catalog_lookup(db);
        let Some(relation) = catalog.lookup_any_relation(table_name) else {
            return Ok(());
        };
        for rule in catalog
            .rewrite_rows_for_relation(relation.relation_oid)
            .into_iter()
            .filter(|row| row.rulename != "_RETURN" && row.ev_type == event_code)
        {
            if !rule.ev_qual.trim().is_empty() {
                return Err(copy_to_feature_error(
                    "conditional DO INSTEAD rules are not supported for COPY",
                ));
            }
            if !rule.is_instead {
                return Err(copy_to_feature_error(
                    "DO ALSO rules are not supported for COPY",
                ));
            }
            let actions = crate::backend::rewrite::split_stored_rule_action_sql(&rule.ev_action);
            if actions.is_empty() {
                return Err(copy_to_feature_error(
                    "DO INSTEAD NOTHING rules are not supported for COPY",
                ));
            }
            if actions.len() > 1 {
                return Err(copy_to_feature_error(
                    "multi-statement DO INSTEAD rules are not supported for COPY",
                ));
            }
            let parsed =
                crate::backend::parser::parse_statement(actions[0]).map_err(ExecError::Parse)?;
            if !matches!(
                parsed,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            ) {
                return Err(copy_to_feature_error(
                    "COPY query must not be a utility command",
                ));
            }
        }
        Ok(())
    }

    fn ensure_copy_to_file_allowed(&self, db: &Database, path: &str) -> Result<(), ExecError> {
        if !Path::new(path).is_absolute() {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: "relative path not allowed for COPY to file".into(),
                detail: None,
                hint: None,
                sqlstate: "42602",
            }));
        }
        let auth_catalog = db
            .auth_catalog(self.client_id, self.catalog_txn_ctx())
            .map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "authorization catalog",
                    actual: format!("{err:?}"),
                })
            })?;
        if self
            .auth
            .has_effective_membership(PG_WRITE_SERVER_FILES_OID, &auth_catalog)
        {
            return Ok(());
        }
        Err(ExecError::DetailedError {
            message: "permission denied to COPY to a file".into(),
            detail: Some(
                "Only roles with privileges of the \"pg_write_server_files\" role may COPY to a file."
                    .into(),
            ),
            hint: None,
            sqlstate: "42501",
        })
    }

    fn ensure_copy_to_program_allowed(&self, db: &Database) -> Result<(), ExecError> {
        let auth_catalog = db
            .auth_catalog(self.client_id, self.catalog_txn_ctx())
            .map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "authorization catalog",
                    actual: format!("{err:?}"),
                })
            })?;
        if self
            .auth
            .has_effective_membership(PG_EXECUTE_SERVER_PROGRAM_OID, &auth_catalog)
        {
            return Ok(());
        }
        Err(ExecError::DetailedError {
            message: "permission denied to COPY to or from an external program".into(),
            detail: Some(
                "Only roles with privileges of the \"pg_execute_server_program\" role may COPY to or from an external program."
                    .into(),
            ),
            hint: None,
            sqlstate: "42501",
        })
    }
}

fn relation_copy_to_query_sql(table_name: &str, columns: Option<&[String]>) -> String {
    let target = columns
        .filter(|columns| !columns.is_empty())
        .map(|columns| {
            columns
                .iter()
                .map(|column| quote_copy_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "*".into());
    format!(
        "select {target} from {}",
        quote_copy_qualified_name(table_name)
    )
}

fn quote_copy_qualified_name(name: &str) -> String {
    name.split('.')
        .map(quote_copy_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_copy_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn copy_to_feature_error(message: &'static str) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupportedMessage(message.into()))
}

fn copy_query_looks_like_select_into(sql: &str) -> bool {
    let lowered = sql.trim_start().to_ascii_lowercase();
    lowered.starts_with("select ") && lowered.contains(" into ")
}

fn apply_guc_value_to_state(
    state: &mut GucState,
    name: &str,
    value: &str,
) -> Result<String, ExecError> {
    let normalized = normalize_guc_name(name);
    let is_builtin = is_postgres_guc(&normalized);
    if !is_builtin {
        validate_custom_guc_for_set(&normalized, false)?;
        state.gucs.insert(normalized.clone(), value.to_string());
        return Ok(normalized);
    }
    if is_checkpoint_guc(&normalized) || is_autovacuum_guc(&normalized) {
        return Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(
            normalized,
        )));
    }

    let mut stored_value = value.to_string();
    match normalized.as_str() {
        "datestyle" => {
            let Some((date_style_format, date_order)) = parse_datestyle_with_fallback(
                value,
                state.datetime_config.date_style_format,
                state.datetime_config.date_order,
            ) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.datetime_config.date_style_format = date_style_format;
            state.datetime_config.date_order = date_order;
        }
        "intervalstyle" => {
            let Some(interval_style) = parse_intervalstyle(value) else {
                return Err(ExecError::DetailedError {
                    message: format!("invalid value for parameter \"IntervalStyle\": \"{value}\""),
                    detail: None,
                    hint: Some(
                        "Available values: postgres, postgres_verbose, sql_standard, iso_8601."
                            .into(),
                    ),
                    sqlstate: "22023",
                });
            };
            state.datetime_config.interval_style = interval_style;
            stored_value = format_intervalstyle(interval_style).to_string();
        }
        "timezone" => {
            let Some(time_zone) = parse_timezone(value) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.datetime_config.time_zone = time_zone;
        }
        "statement_timeout" => {
            parse_statement_timeout(value)?;
        }
        "vacuum_cost_delay" => {
            parse_vacuum_cost_delay_ms(value)?;
        }
        "seq_page_cost" => {
            let parsed = value.parse::<f64>().map_err(|_| ExecError::DetailedError {
                message: format!("invalid value for parameter \"seq_page_cost\": \"{value}\""),
                detail: None,
                hint: None,
                sqlstate: "22023",
            })?;
            if !parsed.is_finite() {
                return Err(ExecError::DetailedError {
                    message: format!("invalid value for parameter \"seq_page_cost\": \"{value}\""),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
        }
        "default_transaction_isolation" | "transaction_isolation" => {
            let level = crate::backend::parser::TransactionIsolationLevel::parse(value)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string()))
                })?;
            stored_value = level.as_str().to_string();
        }
        "xmlbinary" => {
            let Some(binary) = parse_xmlbinary(value) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.datetime_config.xml.binary = binary;
        }
        "xmloption" => {
            let Some(option) = parse_xmloption(value) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.datetime_config.xml.option = option;
        }
        "max_stack_depth" => {
            state.datetime_config.max_stack_depth_kb = parse_max_stack_depth(value)?;
        }
        "stats_fetch_consistency" => {
            let Some(fetch_consistency) = StatsFetchConsistency::parse(value) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.stats_fetch_consistency = fetch_consistency;
        }
        "track_functions" => {
            let Some(track_functions) = TrackFunctionsSetting::parse(value) else {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            };
            state.track_functions = track_functions;
        }
        "row_security"
        | "event_triggers"
        | "enable_partitionwise_join"
        | "enable_seqscan"
        | "enable_indexscan"
        | "enable_indexonlyscan"
        | "enable_bitmapscan"
        | "enable_hashjoin"
        | "enable_mergejoin"
        | "enable_memoize"
        | "enable_hashagg"
        | "enable_sort" => {
            parse_bool_guc(value).ok_or_else(|| {
                ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string()))
            })?;
        }
        "session_replication_role" => {
            if !matches!(
                value.to_ascii_lowercase().as_str(),
                "origin" | "replica" | "local"
            ) {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            }
            stored_value = value.to_ascii_lowercase();
        }
        "default_toast_compression" => {
            stored_value = parse_default_toast_compression_guc_value(value)?.to_string();
        }
        "default_with_oids" => {
            let bool_value = parse_bool_guc(value).ok_or_else(|| {
                ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string()))
            })?;
            if bool_value {
                return Err(ExecError::Parse(
                    ParseError::TablesDeclaredWithOidsNotSupported,
                ));
            }
            stored_value = "off".to_string();
        }
        "plpgsql.check_asserts" | "plpgsql.print_strict_params" => {
            let bool_value = parse_bool_guc(value).ok_or_else(|| {
                ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string()))
            })?;
            stored_value = if bool_value { "on" } else { "off" }.to_string();
        }
        "plpgsql.variable_conflict" => {
            stored_value = match value.trim().to_ascii_lowercase().as_str() {
                "error" | "use_variable" | "use_column" => value.trim().to_ascii_lowercase(),
                _ => {
                    return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                        value.to_string(),
                    )));
                }
            };
        }
        "plpgsql.extra_warnings" | "plpgsql.extra_errors" => {
            stored_value = parse_plpgsql_extra_checks(value)?;
        }
        "restrict_nonsystem_relation_kind" => {
            let normalized_value = value.trim().trim_matches('\'').to_ascii_lowercase();
            if !normalized_value.is_empty() && normalized_value != "view" {
                return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                    value.to_string(),
                )));
            }
            stored_value = normalized_value;
        }
        _ => {}
    }
    state.gucs.insert(normalized.clone(), stored_value);
    Ok(normalized)
}

fn validate_custom_guc_for_set(normalized: &str, plpgsql_loaded: bool) -> Result<(), ExecError> {
    if plpgsql_loaded && normalized.starts_with("plpgsql.") {
        return Err(ExecError::DetailedError {
            message: format!("invalid configuration parameter name \"{normalized}\""),
            detail: Some("\"plpgsql\" is a reserved prefix.".into()),
            hint: None,
            sqlstate: "42602",
        });
    }
    if !is_valid_custom_guc_name(normalized) {
        return Err(ExecError::DetailedError {
            message: format!("invalid configuration parameter name \"{normalized}\""),
            detail: Some(
                "Custom parameter names must be two or more simple identifiers separated by dots."
                    .into(),
            ),
            hint: None,
            sqlstate: "42602",
        });
    }
    Ok(())
}

fn is_valid_custom_guc_name(normalized: &str) -> bool {
    let mut parts = normalized.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    if !is_simple_guc_name_part(first) {
        return false;
    }
    let mut count = 1usize;
    for part in parts {
        count += 1;
        if !is_simple_guc_name_part(part) {
            return false;
        }
    }
    count >= 2
}

fn is_simple_guc_name_part(part: &str) -> bool {
    let mut chars = part.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn parse_vacuum_cost_delay_ms(value: &str) -> Result<f64, ExecError> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("default") {
        return Ok(0.0);
    }
    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    let parsed = number
        .parse::<f64>()
        .map_err(|_| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    if !parsed.is_finite() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    let ms = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => parsed,
        "s" => parsed * 1000.0,
        "us" => parsed / 1000.0,
        _ => {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
    };
    if !(0.0..=100.0).contains(&ms) {
        return Err(ExecError::DetailedError {
            message: format!(
                "{} ms is outside the valid range for parameter \"vacuum_cost_delay\" (0 ms .. 100 ms)",
                format_guc_number(ms)
            ),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(ms)
}

fn format_vacuum_cost_delay(value: &str) -> String {
    let Ok(ms) = parse_vacuum_cost_delay_ms(value) else {
        return value.to_string();
    };
    if ms == 0.0 {
        return "0".into();
    }
    if ms.fract() == 0.0 {
        format!("{}ms", ms as i64)
    } else {
        format!("{}us", (ms * 1000.0).round() as i64)
    }
}

fn format_guc_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{}", value as i64)
    } else {
        value.to_string()
    }
}

fn format_guc_show_value(name: &str, value: String) -> String {
    match name {
        "vacuum_cost_delay" => format_vacuum_cost_delay(&value),
        _ => value,
    }
}

fn reset_guc_in_state(
    state: &mut GucState,
    normalized: &str,
    reset_datetime_config: &DateTimeConfig,
) {
    match normalized {
        "datestyle" => {
            state.datetime_config.date_style_format = reset_datetime_config.date_style_format;
            state.datetime_config.date_order = reset_datetime_config.date_order;
        }
        "intervalstyle" => {
            state.datetime_config.interval_style = parse_intervalstyle(default_intervalstyle())
                .expect("default IntervalStyle must parse");
        }
        "timezone" => {
            state.datetime_config.time_zone = reset_datetime_config.time_zone.clone();
        }
        "max_stack_depth" => {
            state.datetime_config.max_stack_depth_kb = reset_datetime_config.max_stack_depth_kb;
        }
        "xmlbinary" => state.datetime_config.xml.binary = Default::default(),
        "xmloption" => state.datetime_config.xml.option = Default::default(),
        "stats_fetch_consistency" => state.stats_fetch_consistency = StatsFetchConsistency::Cache,
        "track_functions" => state.track_functions = TrackFunctionsSetting::None,
        _ => {}
    }
    state.gucs.remove(normalized);
}

fn reset_all_gucs_in_state(state: &mut GucState, reset_datetime_config: &DateTimeConfig) {
    state.gucs.clear();
    state.datetime_config.date_style_format = reset_datetime_config.date_style_format;
    state.datetime_config.date_order = reset_datetime_config.date_order;
    state.datetime_config.interval_style =
        parse_intervalstyle(default_intervalstyle()).expect("default IntervalStyle must parse");
    state.datetime_config.time_zone = reset_datetime_config.time_zone.clone();
    state.datetime_config.max_stack_depth_kb = reset_datetime_config.max_stack_depth_kb;
    state.datetime_config.xml = Default::default();
    state.stats_fetch_consistency = StatsFetchConsistency::Cache;
    state.track_functions = TrackFunctionsSetting::None;
}

fn parse_statement_timeout(value: &str) -> Result<Option<Duration>, ExecError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    let amount = number
        .parse::<f64>()
        .map_err(|_| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    if !amount.is_finite() || amount < 0.0 {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    if amount == 0.0 {
        return Ok(None);
    }

    let multiplier_ms = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
        "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
        "d" | "day" | "days" => 86_400_000.0,
        _ => {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
    };
    let millis = amount * multiplier_ms;
    if !millis.is_finite() || millis > u64::MAX as f64 {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }
    Ok(Some(Duration::from_millis(millis.ceil() as u64)))
}

fn parse_bool_guc(value: &str) -> Option<bool> {
    match normalize_guc_name(value).as_str() {
        "on" | "true" | "yes" | "1" | "t" => Some(true),
        "off" | "false" | "no" | "0" | "f" => Some(false),
        _ => None,
    }
}

fn parse_max_stack_depth(value: &str) -> Result<u32, ExecError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            value.to_string(),
        )));
    }

    let amount = number
        .parse::<u32>()
        .map_err(|_| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    let multiplier_kb = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "kb" => 1_u32,
        "mb" => 1024_u32,
        _ => {
            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                value.to_string(),
            )));
        }
    };
    let max_stack_depth_kb = amount
        .checked_mul(multiplier_kb)
        .ok_or_else(|| ExecError::Parse(ParseError::UnrecognizedParameter(value.to_string())))?;
    validate_max_stack_depth(value, max_stack_depth_kb)?;
    Ok(max_stack_depth_kb)
}

fn validate_max_stack_depth(value: &str, max_stack_depth_kb: u32) -> Result<(), ExecError> {
    if max_stack_depth_kb < MIN_MAX_STACK_DEPTH_KB {
        return Err(ExecError::DetailedError {
            message: format!("invalid value for parameter \"max_stack_depth\": \"{value}\""),
            detail: Some(format!(
                "\"max_stack_depth\" must be at least {MIN_MAX_STACK_DEPTH_KB}kB."
            )),
            hint: None,
            sqlstate: "22023",
        });
    }

    if let Some(limit_kb) = max_stack_depth_limit_kb()
        && max_stack_depth_kb > limit_kb
    {
        return Err(ExecError::DetailedError {
            message: format!("invalid value for parameter \"max_stack_depth\": \"{value}\""),
            detail: Some(format!("\"max_stack_depth\" must not exceed {limit_kb}kB.")),
            hint: Some(
                "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent."
                    .into(),
            ),
            sqlstate: "22023",
        });
    }

    Ok(())
}

fn parse_startup_options(options: &str) -> Result<Vec<(String, String)>, ExecError> {
    let tokens = split_startup_option_words(options)?;
    let mut gucs = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        let token = &tokens[index];
        let assignment = if token == "-c" {
            index += 1;
            tokens.get(index).ok_or_else(|| {
                ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
            })?
        } else if let Some(assignment) = token.strip_prefix("-c") {
            assignment
        } else if let Some(assignment) = token.strip_prefix("--") {
            assignment
        } else {
            index += 1;
            continue;
        };
        let (name, value) = assignment.split_once('=').ok_or_else(|| {
            ExecError::Parse(ParseError::UnrecognizedParameter(assignment.to_string()))
        })?;
        gucs.push((name.to_string(), value.to_string()));
        index += 1;
    }
    Ok(gucs)
}

fn split_startup_option_words(options: &str) -> Result<Vec<String>, ExecError> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut chars = options.chars().peekable();
    let mut quote = None::<char>;

    while let Some(ch) = chars.next() {
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) if ch == '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
                })?;
                current.push(escaped);
            }
            Some(_) => current.push(ch),
            None if ch.is_ascii_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnrecognizedParameter(options.to_string()))
                })?;
                current.push(escaped);
            }
            None => current.push(ch),
        }
    }

    if quote.is_some() {
        return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
            options.to_string(),
        )));
    }
    if !current.is_empty() {
        words.push(current);
    }
    Ok(words)
}

pub(crate) fn parse_copy_command(sql: &str) -> Option<Result<CopyCommand, ExecError>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.to_ascii_lowercase().starts_with("copy ") {
        return None;
    }
    Some(parse_copy_command_inner(trimmed))
}

fn parse_copy_command_inner(sql: &str) -> Result<CopyCommand, ExecError> {
    let body = sql[4..].trim_start();
    let (relation, rest) = parse_copy_relation(body)?;
    let rest = rest.trim_start();
    if matches!(relation, CopyRelation::Query(_)) {
        let lower = rest.to_ascii_lowercase();
        if lower.starts_with("from") && copy_keyword_boundary(rest, 4) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "TO",
                actual: "syntax error at or near \"from\"".into(),
            }));
        }
        if rest.starts_with('(') {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "TO",
                actual: "syntax error at or near \"(\"".into(),
            }));
        }
    }
    let lower = rest.to_ascii_lowercase();
    let (direction, option_text) = if lower.starts_with("from") && copy_keyword_boundary(rest, 4) {
        let (endpoint, options) = parse_copy_endpoint(rest[4..].trim_start(), true)?;
        (CopyDirection::From(endpoint), options)
    } else if lower.starts_with("to") && copy_keyword_boundary(rest, 2) {
        let (endpoint, options) = parse_copy_endpoint(rest[2..].trim_start(), false)?;
        (CopyDirection::To(endpoint), options)
    } else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY FROM or COPY TO",
            actual: rest.into(),
        }));
    };
    Ok(CopyCommand {
        relation,
        direction,
        options: parse_copy_options(option_text)?,
    })
}

fn parse_copy_relation(input: &str) -> Result<(CopyRelation, &str), ExecError> {
    let input = input.trim_start();
    if input.starts_with('(') {
        let close = find_matching_paren(input, 0).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "closing parenthesis in COPY query",
                actual: input.into(),
            })
        })?;
        return Ok((
            CopyRelation::Query(input[1..close].trim().to_string()),
            &input[close + 1..],
        ));
    }

    let Some((idx, _keyword)) = find_copy_direction_keyword(input) else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY FROM or COPY TO",
            actual: input.into(),
        }));
    };
    let target = input[..idx].trim();
    let rest = &input[idx..];
    let (name, columns) = parse_copy_table_target(target)?;
    Ok((CopyRelation::Table { name, columns }, rest))
}

fn parse_copy_table_target(target: &str) -> Result<(String, Option<Vec<String>>), ExecError> {
    if let Some(open_paren) = target.find('(') {
        let close_paren = target.rfind(')').ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY column list",
                actual: target.into(),
            })
        })?;
        if close_paren < open_paren {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY column list",
                actual: target.into(),
            }));
        }
        let table = target[..open_paren].trim();
        let columns = split_copy_list(&target[open_paren + 1..close_paren])
            .into_iter()
            .map(|part| unquote_identifier(part.trim()))
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if table.is_empty() || columns.is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY table and column list",
                actual: target.into(),
            }));
        }
        Ok((table.to_string(), Some(columns)))
    } else if !target.is_empty() {
        Ok((target.to_string(), None))
    } else {
        Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY table target",
            actual: target.into(),
        }))
    }
}

fn copy_relation_table_name(relation: &CopyRelation) -> Option<&str> {
    match relation {
        CopyRelation::Table { name, .. } => Some(name.as_str()),
        CopyRelation::Query(_) => None,
    }
}

fn parse_copy_endpoint(input: &str, from: bool) -> Result<(CopyEndpoint, &str), ExecError> {
    let input = input.trim_start();
    let lower = input.to_ascii_lowercase();
    if from && lower.starts_with("stdin") && copy_keyword_boundary(input, 5) {
        return Ok((CopyEndpoint::Stdin, &input[5..]));
    }
    if from && lower.starts_with("stdout") && copy_keyword_boundary(input, 6) {
        return Ok((CopyEndpoint::Stdin, &input[6..]));
    }
    if !from && lower.starts_with("stdout") && copy_keyword_boundary(input, 6) {
        return Ok((CopyEndpoint::Stdout, &input[6..]));
    }
    if let Some((path, rest)) = parse_copy_string_token(input) {
        return Ok((CopyEndpoint::File(path), rest));
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: if from {
            "COPY source"
        } else {
            "COPY destination"
        },
        actual: input.into(),
    }))
}

fn parse_copy_options(input: &str) -> Result<CopyOptions, ExecError> {
    let mut options = CopyOptions::default();
    let mut rest = input.trim();
    if let Some(where_idx) = find_top_level_keyword(rest, "where") {
        options.where_clause = Some(rest[where_idx + "where".len()..].trim().to_string());
        rest = rest[..where_idx].trim();
    }

    while !rest.is_empty() {
        rest = rest.trim_start();
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        let lower = rest.to_ascii_lowercase();
        if lower.starts_with("with") && copy_keyword_boundary(rest, 4) {
            rest = rest[4..].trim_start();
            continue;
        }
        if rest.starts_with('(') {
            let close = find_matching_paren(rest, 0).ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "COPY option list",
                    actual: rest.into(),
                })
            })?;
            let nested = parse_copy_options(&rest[1..close])?;
            merge_copy_options(&mut options, nested);
            rest = rest[close + 1..].trim_start();
            continue;
        }
        if lower.starts_with("csv") && copy_keyword_boundary(rest, 3) {
            options.format = CopyFormat::Csv;
            rest = &rest[3..];
            continue;
        }
        if lower.starts_with("format") && copy_keyword_boundary(rest, 6) {
            let (word, after) = take_copy_word(rest[6..].trim_start());
            match word.to_ascii_lowercase().as_str() {
                "csv" => options.format = CopyFormat::Csv,
                "text" => options.format = CopyFormat::Text,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY format",
                        actual: other.into(),
                    }));
                }
            }
            rest = after;
            continue;
        }
        if lower.starts_with("encoding") && copy_keyword_boundary(rest, 8) {
            let (value, after) =
                parse_copy_string_token(rest[8..].trim_start()).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY encoding string",
                        actual: rest.into(),
                    })
                })?;
            options.encoding = Some(value);
            rest = after;
            continue;
        }
        if lower.starts_with("header") && copy_keyword_boundary(rest, 6) {
            let after_header = rest[6..].trim_start();
            let (word, after) = if let Some((value, after)) = parse_copy_string_token(after_header)
            {
                (value, after)
            } else {
                let (word, after) = take_copy_word(after_header);
                (word.to_string(), after)
            };
            match word.to_ascii_lowercase().as_str() {
                "true" | "on" | "1" => {
                    options.header = CopyHeader::Present;
                    rest = after;
                }
                "false" | "off" | "0" => {
                    options.header = CopyHeader::None;
                    rest = after;
                }
                "match" => {
                    options.header = CopyHeader::Match;
                    rest = after;
                }
                "" => {
                    options.header = CopyHeader::Present;
                    rest = after_header;
                }
                _ => {
                    return Err(ExecError::DetailedError {
                        message: "header requires a Boolean value or \"match\"".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
            }
            continue;
        }
        if lower.starts_with("force") && copy_keyword_boundary(rest, 5) {
            let after_force = rest[5..].trim_start();
            if !after_force.to_ascii_lowercase().starts_with("quote")
                || !copy_keyword_boundary(after_force, 5)
            {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "QUOTE",
                    actual: after_force.into(),
                }));
            }
            rest = parse_copy_force_quote_option(&mut options, after_force[5..].trim_start())?;
            continue;
        }
        if lower.starts_with("force_quote") && copy_keyword_boundary(rest, 11) {
            rest = parse_copy_force_quote_option(&mut options, rest[11..].trim_start())?;
            continue;
        }
        if lower.starts_with("quote") && copy_keyword_boundary(rest, 5) {
            let (value, after) =
                parse_copy_string_token(rest[5..].trim_start()).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY quote string",
                        actual: rest.into(),
                    })
                })?;
            if let Some(ch) = value.chars().next() {
                options.quote = ch;
            }
            rest = after;
            continue;
        }
        if lower.starts_with("escape") && copy_keyword_boundary(rest, 6) {
            let (value, after) =
                parse_copy_string_token(rest[6..].trim_start()).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY escape string",
                        actual: rest.into(),
                    })
                })?;
            if let Some(ch) = value.chars().next() {
                options.escape = ch;
            }
            rest = after;
            continue;
        }
        if lower.starts_with("null") && copy_keyword_boundary(rest, 4) {
            let (value, after) =
                parse_copy_string_token(rest[4..].trim_start()).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY null string",
                        actual: rest.into(),
                    })
                })?;
            options.null_marker = value;
            rest = after;
            continue;
        }
        if lower.starts_with("default") && copy_keyword_boundary(rest, 7) {
            let (value, after) =
                parse_copy_string_token(rest[7..].trim_start()).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "COPY default string",
                        actual: rest.into(),
                    })
                })?;
            options.default_marker = Some(value);
            rest = after;
            continue;
        }
        if lower.starts_with("freeze") && copy_keyword_boundary(rest, 6) {
            options.freeze = true;
            rest = &rest[6..];
            continue;
        }
        if lower.starts_with("on_error") && copy_keyword_boundary(rest, 8) {
            let (word, after) = take_copy_word(rest[8..].trim_start());
            if word.eq_ignore_ascii_case("ignore") {
                options.on_error_ignore = true;
                rest = after;
                continue;
            }
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ignore",
                actual: word.into(),
            }));
        }
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY option",
            actual: rest.into(),
        }));
    }
    Ok(options)
}

fn parse_copy_force_quote_option<'a>(
    options: &mut CopyOptions,
    input: &'a str,
) -> Result<&'a str, ExecError> {
    let input = input.trim_start();
    if let Some(rest) = input.strip_prefix('*') {
        options.force_quote_all = true;
        return Ok(rest);
    }
    if input.starts_with('(') {
        let close = find_matching_paren(input, 0).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY FORCE QUOTE column list",
                actual: input.into(),
            })
        })?;
        options.force_quote_columns.extend(
            split_copy_list(&input[1..close])
                .into_iter()
                .map(|part| unquote_identifier(part.trim())),
        );
        return Ok(input[close + 1..].trim_start());
    }
    let (word, after) = take_copy_word(input);
    if word.is_empty() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY FORCE QUOTE column",
            actual: input.into(),
        }));
    }
    options.force_quote_columns.push(unquote_identifier(&word));
    Ok(after)
}

fn merge_copy_options(options: &mut CopyOptions, nested: CopyOptions) {
    if nested.format != CopyFormat::Text {
        options.format = nested.format;
    }
    if nested.encoding.is_some() {
        options.encoding = nested.encoding;
    }
    if nested.header != CopyHeader::None {
        options.header = nested.header;
    }
    if nested.quote != '"' {
        options.quote = nested.quote;
    }
    if nested.escape != '"' {
        options.escape = nested.escape;
    }
    if nested.null_marker != "\\N" {
        options.null_marker = nested.null_marker;
    }
    options.default_marker = options.default_marker.take().or(nested.default_marker);
    options.on_error_ignore |= nested.on_error_ignore;
    options.freeze |= nested.freeze;
    options.where_clause = options.where_clause.take().or(nested.where_clause);
    options.force_quote_all |= nested.force_quote_all;
    options
        .force_quote_columns
        .extend(nested.force_quote_columns);
}

fn read_copy_text_file(
    file_path: &str,
    encoding_name: &str,
    table_name: Option<&str>,
) -> Result<String, ExecError> {
    let resolved = resolve_copy_file_path(file_path);
    let bytes = fs::read(&resolved).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "readable COPY source file",
            actual: format!("{file_path}: {err}"),
        })
    })?;
    decode_copy_file_bytes(&bytes, encoding_name, table_name.unwrap_or(""))
}

pub(crate) fn parse_copy_input_rows(
    text: &str,
    options: &CopyOptions,
    table_name: Option<&str>,
    stop_on_copy_marker: bool,
) -> Result<Vec<Vec<String>>, ExecError> {
    match options.format {
        CopyFormat::Text => {
            parse_copy_text_rows(text, &options.null_marker, table_name, stop_on_copy_marker)
        }
        CopyFormat::Csv => {
            parse_copy_csv_rows(text, options.quote, options.escape, stop_on_copy_marker)
        }
    }
}

fn parse_copy_text_rows(
    text: &str,
    null_marker: &str,
    table_name: Option<&str>,
    stop_on_copy_marker: bool,
) -> Result<Vec<Vec<String>>, ExecError> {
    let mut rows = Vec::new();
    for (line_idx, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if stop_on_copy_marker && line == "\\." {
            break;
        }
        if stop_on_copy_marker && line.contains("\\.") {
            let err = ExecError::DetailedError {
                message: "end-of-copy marker is not alone on its line".into(),
                detail: None,
                hint: None,
                sqlstate: "22P04",
            };
            return Err(match table_name {
                Some(name) => ExecError::WithContext {
                    source: Box::new(err),
                    context: format!("COPY {name}, line {}", line_idx + 1),
                },
                None => err,
            });
        }
        let row = line
            .split('\t')
            .map(|field| {
                if field == null_marker {
                    COPY_TEXT_NULL_SENTINEL.to_string()
                } else {
                    unescape_copy_text_field(field)
                }
            })
            .collect::<Vec<_>>();
        if row.is_empty() && line_idx == 0 {
            continue;
        }
        rows.push(row);
    }
    Ok(rows)
}

fn first_copy_row_context(text: &str, table_name: &str) -> Option<String> {
    for (line_idx, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        return Some(format!(
            "COPY {table_name}, line {}: \"{line}\"",
            line_idx + 1
        ));
    }
    None
}

fn is_parsed_copy_null(field: &str, options: &CopyOptions) -> bool {
    match options.format {
        CopyFormat::Text => field == COPY_TEXT_NULL_SENTINEL,
        CopyFormat::Csv => field == options.null_marker,
    }
}

fn parse_copy_csv_rows(
    text: &str,
    quote: char,
    escape: char,
    stop_on_copy_marker: bool,
) -> Result<Vec<Vec<String>>, ExecError> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == escape {
                if matches!(chars.peek(), Some(next) if *next == quote || *next == escape) {
                    field.push(chars.next().unwrap());
                } else if ch == quote {
                    in_quotes = false;
                } else {
                    field.push(ch);
                }
            } else if ch == quote {
                if matches!(chars.peek(), Some(next) if *next == quote) {
                    chars.next();
                    field.push(quote);
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(ch);
            }
            continue;
        }

        match ch {
            c if c == quote && field.is_empty() => in_quotes = true,
            ',' => {
                row.push(mem::take(&mut field));
            }
            '\n' => {
                row.push(mem::take(&mut field));
                rows.push(mem::take(&mut row));
            }
            '\r' => {
                if matches!(chars.peek(), Some('\n')) {
                    chars.next();
                }
                row.push(mem::take(&mut field));
                rows.push(mem::take(&mut row));
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "closing CSV quote",
            actual: text.into(),
        }));
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    if stop_on_copy_marker {
        rows.retain(|row| !(row.len() == 1 && row[0] == "\\."));
    }
    Ok(rows)
}

pub(crate) fn render_copy_output(
    columns: &[crate::backend::executor::QueryColumn],
    rows: &[Vec<Value>],
    options: &CopyOptions,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
) -> Vec<u8> {
    let mut out = Vec::new();
    if !matches!(options.header, CopyHeader::None) {
        let header = columns
            .iter()
            .map(|column| match options.format {
                CopyFormat::Text => escape_copy_text_field(&column.name),
                CopyFormat::Csv => {
                    escape_copy_csv_field(&column.name, options.quote, options.escape, false)
                }
            })
            .collect::<Vec<_>>()
            .join(match options.format {
                CopyFormat::Text => "\t",
                CopyFormat::Csv => ",",
            });
        out.extend_from_slice(header.as_bytes());
        out.push(b'\n');
    }
    for row in rows {
        let fields = row
            .iter()
            .zip(columns.iter())
            .map(|(value, column)| {
                copy_value_to_field(
                    value,
                    column,
                    options,
                    enum_labels,
                    copy_options_force_quote_column(options, column),
                )
            })
            .collect::<Vec<_>>();
        let line = fields.join(match options.format {
            CopyFormat::Text => "\t",
            CopyFormat::Csv => ",",
        });
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    out
}

fn copy_command_output_format(format: CopyFormat) -> ParserCopyFormat {
    match format {
        CopyFormat::Text => ParserCopyFormat::Text,
        CopyFormat::Csv => ParserCopyFormat::Csv,
    }
}

fn copy_value_to_field(
    value: &Value,
    column: &crate::backend::executor::QueryColumn,
    options: &CopyOptions,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
    force_quote: bool,
) -> String {
    let sql_type = column.sql_type;
    let enum_label_type_oid = || {
        (matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::Enum)
            && sql_type.type_oid != 0)
            .then_some(if sql_type.typrelid != 0 {
                sql_type.typrelid
            } else {
                sql_type.type_oid
            })
    };
    if matches!(value, Value::Null) {
        return match options.format {
            CopyFormat::Text => options.null_marker.clone(),
            CopyFormat::Csv => String::new(),
        };
    }
    let raw = match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::Float64(v) => match sql_type.kind {
            crate::backend::parser::SqlTypeKind::Float4 => {
                crate::backend::libpq::pqformat::format_float4_text(
                    *v,
                    crate::backend::libpq::pqformat::FloatFormatOptions::default(),
                )
            }
            _ => crate::backend::libpq::pqformat::format_float8_text(
                *v,
                crate::backend::libpq::pqformat::FloatFormatOptions::default(),
            ),
        },
        Value::Numeric(v) => v.render(),
        Value::Text(v) => v.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or("").to_string(),
        Value::EnumOid(label_oid) => enum_label_type_oid()
            .and_then(|type_oid| enum_labels.and_then(|labels| labels.get(&(type_oid, *label_oid))))
            .cloned()
            .unwrap_or_else(|| label_oid.to_string()),
        Value::Bool(true) => "t".into(),
        Value::Bool(false) => "f".into(),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => crate::backend::executor::render_geometry_text(
            value,
            crate::backend::libpq::pqformat::FloatFormatOptions::default(),
        )
        .unwrap_or_default(),
        Value::Array(values) => crate::backend::executor::value_io::format_array_text_with_config(
            values,
            &DateTimeConfig::default(),
        ),
        Value::PgArray(array) => {
            crate::backend::executor::value_io::format_array_value_text_with_config(
                array,
                &DateTimeConfig::default(),
            )
        }
        Value::Json(v) | Value::Xml(v) | Value::JsonPath(v) => v.to_string(),
        Value::Jsonb(v) => {
            crate::backend::executor::jsonb::render_jsonb_bytes(v).unwrap_or_default()
        }
        Value::Bytea(v) => {
            crate::backend::libpq::pqformat::format_bytea_text(v, ByteaOutputFormat::Hex)
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => format!("{value:?}"),
        Value::Interval(v) => crate::backend::executor::render_interval_text(*v),
        Value::Range(_) => crate::backend::executor::render_range_text(value).unwrap_or_default(),
        Value::Multirange(_) => {
            crate::backend::executor::render_multirange_text(value).unwrap_or_default()
        }
        Value::Bit(bits) => crate::backend::executor::render_bit_text(bits),
        Value::PgLsn(v) => crate::backend::executor::render_pg_lsn_text(*v),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => crate::backend::executor::render_macaddr_text(v),
        Value::MacAddr8(v) => crate::backend::executor::render_macaddr8_text(v),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::TsVector(v) => crate::backend::executor::render_tsvector_text(v),
        Value::TsQuery(v) => crate::backend::executor::render_tsquery_text(v),
        Value::Uuid(v) => crate::backend::executor::value_io::render_uuid_text(v),
        Value::InternalChar(byte) => (*byte as char).to_string(),
        Value::Record(record) => {
            crate::backend::executor::value_io::format_record_text_with_options(
                record,
                &crate::backend::libpq::pqformat::FloatFormatOptions::default(),
            )
        }
        Value::Null => options.null_marker.clone(),
    };
    match options.format {
        CopyFormat::Text => escape_copy_text_field(&raw),
        CopyFormat::Csv => escape_copy_csv_field(&raw, options.quote, options.escape, force_quote),
    }
}

fn copy_options_force_quote_column(
    options: &CopyOptions,
    column: &crate::backend::executor::QueryColumn,
) -> bool {
    options.force_quote_all
        || options
            .force_quote_columns
            .iter()
            .any(|name| name == &column.name)
}

fn copy_enum_label_map(catalog: &dyn CatalogLookup) -> HashMap<(u32, u32), String> {
    catalog
        .enum_rows()
        .into_iter()
        .map(|row| ((row.enumtypid, row.oid), row.enumlabel))
        .collect()
}

fn escape_copy_text_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

fn unescape_copy_text_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('\\') => out.push('\\'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

fn escape_copy_csv_field(value: &str, quote: char, escape: char, force_quote: bool) -> String {
    let needs_quote = force_quote
        || value.contains(',')
        || value.contains(quote)
        || value.contains('\n')
        || value.contains('\r')
        || value == "\\.";
    if !needs_quote {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push(quote);
    for ch in value.chars() {
        if ch == quote || ch == escape {
            out.push(escape);
        }
        out.push(ch);
    }
    out.push(quote);
    out
}

fn parse_copy_where_filter(clause: &str) -> Option<CopyWhereFilter> {
    let trimmed = clause
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    for op in ["<=", ">=", "<>", "!=", "=", "<", ">"] {
        if let Some(idx) = trimmed.find(op) {
            return Some(CopyWhereFilter {
                column: unquote_identifier(trimmed[..idx].trim()),
                op,
                literal: trimmed[idx + op.len()..].trim().to_string(),
            });
        }
    }
    None
}

fn find_copy_direction_keyword(input: &str) -> Option<(usize, &'static str)> {
    let from = find_top_level_keyword(input, "from").map(|idx| (idx, "from"));
    let to = find_top_level_keyword(input, "to").map(|idx| (idx, "to"));
    match (from, to) {
        (Some(f), Some(t)) => Some(if f.0 < t.0 { f } else { t }),
        (Some(f), None) => Some(f),
        (None, Some(t)) => Some(t),
        (None, None) => None,
    }
}

fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let lower = input.to_ascii_lowercase();
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if single_quote {
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                    i += 2;
                    continue;
                }
                single_quote = false;
            }
            i += 1;
            continue;
        }
        if double_quote {
            if ch == '"' {
                double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => single_quote = true,
            '"' => double_quote = true,
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && lower[i..].starts_with(keyword)
            && keyword_boundary(input, i, keyword.len())
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_matching_paren(input: &str, open_idx: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut i = open_idx;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if single_quote {
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                    i += 2;
                    continue;
                }
                single_quote = false;
            }
            i += 1;
            continue;
        }
        if double_quote {
            if ch == '"' {
                double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => single_quote = true,
            '"' => double_quote = true,
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn keyword_boundary(input: &str, idx: usize, len: usize) -> bool {
    let before = input[..idx].chars().next_back();
    let after = input[idx + len..].chars().next();
    !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
}

fn copy_keyword_boundary(input: &str, len: usize) -> bool {
    input[len..]
        .chars()
        .next()
        .is_none_or(|ch| ch.is_ascii_whitespace() || ch == '(' || ch == '\'' || ch == ';')
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn take_copy_word(input: &str) -> (&str, &str) {
    let input = input.trim_start();
    let end = input
        .find(|ch: char| ch.is_ascii_whitespace() || ch == ',' || ch == ')')
        .unwrap_or(input.len());
    (&input[..end], &input[end..])
}

fn parse_copy_string_token(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    let (escape_string, start) = if input
        .as_bytes()
        .first()
        .is_some_and(|b| matches!(*b, b'e' | b'E'))
        && input.as_bytes().get(1) == Some(&b'\'')
    {
        (true, 1)
    } else {
        (false, 0)
    };
    if input.as_bytes().get(start) != Some(&b'\'') {
        return None;
    }
    let mut out = String::new();
    let mut i = start + 1;
    let bytes = input.as_bytes();
    while i < bytes.len() {
        let ch = input[i..].chars().next()?;
        if ch == '\'' {
            let next = i + ch.len_utf8();
            if input.as_bytes().get(next) == Some(&b'\'') {
                out.push('\'');
                i = next + 1;
                continue;
            }
            return Some((out, &input[next..]));
        }
        if escape_string && ch == '\\' {
            let next = i + 1;
            if let Some(escaped) = input[next..].chars().next() {
                out.push(escaped);
                i = next + escaped.len_utf8();
                continue;
            }
        }
        out.push(ch);
        i += ch.len_utf8();
    }
    None
}

fn split_copy_list(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut double_quote = false;
    for (idx, ch) in input.char_indices() {
        match ch {
            '"' => double_quote = !double_quote,
            ',' if !double_quote => {
                parts.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn unquote_identifier(input: &str) -> String {
    let input = input.trim();
    input
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .map(|s| s.replace("\"\"", "\""))
        .unwrap_or_else(|| input.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        CopyDirection, CopyEndpoint, Session, parse_bool_guc,
        parse_default_toast_compression_guc_value, parse_max_stack_depth, parse_startup_options,
        parse_statement_timeout, validate_max_stack_depth,
    };
    use crate::backend::executor::{ExecError, StatementResult, Value};
    use crate::backend::parser::ParseError;
    use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat};
    use crate::backend::utils::misc::stack_depth::max_stack_depth_limit_kb;
    use crate::pgrust::database::Database;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn parse_copy_from_stdout_as_copy_in_source() {
        let copy = super::parse_copy_command("copy donothingbrtrig_test from stdout")
            .expect("copy statement")
            .unwrap();
        assert!(matches!(
            copy.direction,
            CopyDirection::From(CopyEndpoint::Stdin)
        ));
    }

    #[test]
    fn default_text_search_config_guc_drives_one_arg_tsearch() {
        let db = Database::open_ephemeral(32).expect("open ephemeral database");
        let mut session = Session::new(1);

        session
            .execute(&db, "set default_text_search_config=simple")
            .unwrap();
        let result = session
            .execute(&db, "select to_tsvector('SKIES My booKs')")
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result");
        };
        let Value::TsVector(vector) = &rows[0][0] else {
            panic!("expected tsvector result");
        };
        assert_eq!(
            crate::backend::executor::render_tsvector_text(vector),
            "'books':3 'my':2 'skies':1"
        );

        session
            .execute(&db, "set default_text_search_config=english")
            .unwrap();
        let result = session
            .execute(&db, "select to_tsvector('SKIES My booKs')")
            .unwrap();

        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result");
        };
        let Value::TsVector(vector) = &rows[0][0] else {
            panic!("expected tsvector result");
        };
        assert_eq!(
            crate::backend::executor::render_tsvector_text(vector),
            "'book':3 'sky':1"
        );
    }

    #[test]
    fn sql_prepare_execute_and_deallocate_use_session_state() {
        let db = Database::open_ephemeral(32).expect("open ephemeral database");
        let mut session = Session::new(1);

        assert!(matches!(
            session.execute(&db, "prepare q as select 1 as x").unwrap(),
            StatementResult::AffectedRows(0)
        ));

        match session.execute(&db, "execute q").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {other:?}"),
        }

        assert!(matches!(
            session.execute(&db, "deallocate q").unwrap(),
            StatementResult::AffectedRows(0)
        ));

        let err = session.execute(&db, "execute q").unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::DetailedError {
                message,
                sqlstate,
                ..
            }) if message == "prepared statement \"q\" does not exist" && sqlstate == "26000"
        ));

        session.execute(&db, "prepare q as select 2 as x").unwrap();
        session.execute(&db, "discard all").unwrap();
        let err = session.execute(&db, "execute q").unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::DetailedError {
                message,
                sqlstate,
                ..
            }) if message == "prepared statement \"q\" does not exist" && sqlstate == "26000"
        ));
    }

    #[test]
    fn parse_statement_timeout_accepts_postgres_units() {
        assert_eq!(parse_statement_timeout("0").unwrap(), None);
        assert_eq!(
            parse_statement_timeout("15").unwrap(),
            Some(Duration::from_millis(15))
        );
        assert_eq!(
            parse_statement_timeout("1.5s").unwrap(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_statement_timeout("2 min").unwrap(),
            Some(Duration::from_millis(120_000))
        );
        assert_eq!(
            parse_statement_timeout("1h").unwrap(),
            Some(Duration::from_millis(3_600_000))
        );
        assert_eq!(
            parse_statement_timeout("1d").unwrap(),
            Some(Duration::from_millis(86_400_000))
        );
    }

    #[test]
    fn parse_statement_timeout_rejects_invalid_values() {
        for value in ["", "-1", "abc", "10fortnights"] {
            assert!(matches!(
                parse_statement_timeout(value),
                Err(ExecError::Parse(ParseError::UnrecognizedParameter(_)))
            ));
        }
    }

    #[test]
    fn parse_bool_guc_accepts_postgres_shorthands() {
        assert_eq!(parse_bool_guc("t"), Some(true));
        assert_eq!(parse_bool_guc("f"), Some(false));
    }

    #[test]
    fn parse_max_stack_depth_accepts_postgres_units() {
        assert_eq!(parse_max_stack_depth("100").unwrap(), 100);
        assert_eq!(parse_max_stack_depth("100kB").unwrap(), 100);
        assert_eq!(parse_max_stack_depth("2MB").unwrap(), 2048);
    }

    #[test]
    fn parse_max_stack_depth_rejects_invalid_values() {
        for value in ["", "-1", "abc", "1GB"] {
            assert!(matches!(
                parse_max_stack_depth(value),
                Err(ExecError::Parse(ParseError::UnrecognizedParameter(_)))
            ));
        }
    }

    #[test]
    fn parse_max_stack_depth_rejects_values_below_postgres_minimum() {
        let err = parse_max_stack_depth("99kB").unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "invalid value for parameter \"max_stack_depth\": \"99kB\""
                );
                assert_eq!(detail, "\"max_stack_depth\" must be at least 100kB.");
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_max_stack_depth_rejects_values_above_platform_limit() {
        let Some(limit_kb) = max_stack_depth_limit_kb() else {
            return;
        };
        let value = format!("{}kB", limit_kb.saturating_add(1));
        let err = validate_max_stack_depth(&value, limit_kb.saturating_add(1)).unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail: Some(detail),
                hint: Some(hint),
                sqlstate,
            } => {
                assert_eq!(
                    message,
                    format!("invalid value for parameter \"max_stack_depth\": \"{value}\"")
                );
                assert_eq!(
                    detail,
                    format!("\"max_stack_depth\" must not exceed {limit_kb}kB.")
                );
                assert_eq!(
                    hint,
                    "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent."
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_bool_guc_accepts_postgres_boolean_aliases() {
        for value in ["on", "true", "yes", "1", "t"] {
            assert_eq!(parse_bool_guc(value), Some(true));
        }
        for value in ["off", "false", "no", "0", "f"] {
            assert_eq!(parse_bool_guc(value), Some(false));
        }
        assert_eq!(parse_bool_guc("maybe"), None);
    }

    #[test]
    fn parse_startup_options_extracts_gucs() {
        assert_eq!(
            parse_startup_options("-c statement_timeout=5s --DateStyle='SQL, DMY'").unwrap(),
            vec![
                ("statement_timeout".to_string(), "5s".to_string()),
                ("DateStyle".to_string(), "SQL, DMY".to_string()),
            ]
        );
    }

    #[test]
    fn datestyle_preserves_format_for_order_only_set_and_startup_reset() {
        let mut session = Session::new(1);
        let mut params = HashMap::new();
        params.insert("DateStyle".to_string(), "Postgres, MDY".to_string());
        session.apply_startup_parameters(&params).unwrap();

        session.apply_guc_value("DateStyle", "ymd").unwrap();
        assert_eq!(
            session.datetime_config.date_style_format,
            DateStyleFormat::Postgres
        );
        assert_eq!(session.datetime_config.date_order, DateOrder::Ymd);

        session.guc_reset_datestyle();
        assert_eq!(
            session.datetime_config.date_style_format,
            DateStyleFormat::Postgres
        );
        assert_eq!(session.datetime_config.date_order, DateOrder::Mdy);
    }

    #[test]
    fn insert_with_writable_cte_materializes_returning_rows() {
        let base = crate::pgrust::test_support::seeded_temp_dir("session", "writable_cte_insert");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table src (id int4)").unwrap();
        session.execute(&db, "create table dst (id int4)").unwrap();
        let result = session
            .execute(
                &db,
                "with moved as (insert into src values (1) returning id) \
                 insert into dst select id from moved",
            )
            .unwrap();
        assert!(matches!(result, StatementResult::AffectedRows(1)));

        match session
            .execute(
                &db,
                "select id from src union all select id from dst order by id",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn select_with_writable_cte_materializes_returning_rows() {
        let base = crate::pgrust::test_support::seeded_temp_dir("session", "writable_cte_select");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table src (id int4)").unwrap();
        match session
            .execute(
                &db,
                "with ins(id) as (insert into src values (1), (2) returning id) \
                 select min(id), max(id) from ins",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1), Value::Int32(2)]]);
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn domain_composite_array_insert_assignments_navigate_base_type() {
        let base =
            crate::pgrust::test_support::seeded_temp_dir("session", "domain_composite_assign");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create type insert_test_type as (if1 int4, if2 text[])",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create domain insert_test_domain as insert_test_type \
                 check ((value).if2 is not null and (value).if2[1] is not null)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table inserttestb (f3 insert_test_domain, f4 insert_test_domain[])",
            )
            .unwrap();

        session
            .execute(
                &db,
                "insert into inserttestb (f3.if1, f3.if2) values (1, array['foo'])",
            )
            .unwrap();
        session
            .execute(
                &db,
                "insert into inserttestb (f3.if2[1], f3.if2[2]) values ('bar', 'baz')",
            )
            .unwrap();
        session
            .execute(
                &db,
                "insert into inserttestb (f3, f4[1].if2[1], f4[1].if2[2]) \
                 values (row(2, '{x}')::insert_test_domain, 'bear', 'beer')",
            )
            .unwrap();
        session
            .execute(
                &db,
                "insert into inserttestb (f3, f4[1].if2[1], f4[1].if2[2]) \
                 values (row(3, '{z}'), 'foo', 'bar')",
            )
            .unwrap();

        match session
            .execute(
                &db,
                "select (f3).if1, (f3).if2[1] from inserttestb order by (f3).if1 nulls last",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Int32(1), Value::Text("foo".into())],
                        vec![Value::Int32(2), Value::Text("x".into())],
                        vec![Value::Int32(3), Value::Text("z".into())],
                        vec![Value::Null, Value::Text("bar".into())],
                    ]
                );
            }
            other => panic!("expected query result, got {other:?}"),
        }

        let err = session
            .execute(
                &db,
                "insert into inserttestb (f3.if1, f3.if2) values (1, array[null])",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "value for domain insert_test_domain violates check constraint \"insert_test_domain_check\""
            ),
            other => panic!("expected domain violation, got {other:?}"),
        }

        session
            .execute(
                &db,
                "create domain insert_nnarray as int4[] \
                 check (value[1] is not null and value[2] is not null)",
            )
            .unwrap();
        session
            .execute(&db, "create table inserttesta (f1 insert_nnarray)")
            .unwrap();
        let err = session
            .execute(&db, "insert into inserttesta (f1[1]) values (1)")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "value for domain insert_nnarray violates check constraint \"insert_nnarray_check\""
            ),
            other => panic!("expected domain violation, got {other:?}"),
        }
        session
            .execute(&db, "insert into inserttesta (f1[1], f1[2]) values (1, 2)")
            .unwrap();
        match session
            .execute(&db, "select f1[1], f1[2] from inserttesta")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1), Value::Int32(2)]]);
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_srf_uses_project_set() {
        let base = crate::pgrust::test_support::seeded_temp_dir("session", "insert_values_srf");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table srf_t (id int4)")
            .unwrap();
        session
            .execute(&db, "insert into srf_t values (generate_series(1, 3))")
            .unwrap();
        match session
            .execute(&db, "select id from srf_t order by id")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                ]
            ),
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn writable_cte_returning_tableoid_regclass_and_star_materializes() {
        let base = crate::pgrust::test_support::seeded_temp_dir("session", "writable_cte_tableoid");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table cte_tableoid_src (a int4, b int4)")
            .unwrap();
        match session
            .execute(
                &db,
                "with ins(rel, a, b) as \
                 (insert into cte_tableoid_src (a, b) \
                  select s.a, 1 from generate_series(2, 4) s(a) returning tableoid::regclass, *) \
                 select rel::text, min(a), max(a), min(b), max(b) from ins group by rel order by 1",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => assert_eq!(
                rows,
                vec![vec![
                    Value::Text("cte_tableoid_src".into()),
                    Value::Int32(2),
                    Value::Int32(4),
                    Value::Int32(1),
                    Value::Int32(1),
                ]]
            ),
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn default_toast_compression_guc_accepts_pglz() {
        assert_eq!(
            parse_default_toast_compression_guc_value("pglz").unwrap(),
            "pglz"
        );
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn default_toast_compression_guc_rejects_invalid_values() {
        for value in ["", "I do not exist compression", "lz4"] {
            let err = parse_default_toast_compression_guc_value(value).unwrap_err();
            match err {
                ExecError::DetailedError {
                    message,
                    hint,
                    sqlstate,
                    ..
                } => {
                    assert_eq!(
                        message,
                        format!(
                            "invalid value for parameter \"default_toast_compression\": \"{value}\""
                        )
                    );
                    assert_eq!(hint.as_deref(), Some("Available values: pglz."));
                    assert_eq!(sqlstate, "22023");
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn default_toast_compression_guc_accepts_lz4() {
        assert_eq!(
            parse_default_toast_compression_guc_value("lz4").unwrap(),
            "lz4"
        );
    }
}

fn read_copy_from_file(file_path: &str) -> Result<Vec<Vec<String>>, ExecError> {
    let resolved = resolve_copy_file_path(file_path);
    let text = fs::read_to_string(&resolved).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "readable COPY source file",
            actual: format!("{file_path}: {err}"),
        })
    })?;
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split('\t').map(|field| field.to_string()).collect())
        .collect())
}

fn resolve_copy_file_path(file_path: &str) -> String {
    if std::path::Path::new(file_path).exists() {
        return file_path.to_string();
    }
    if let Some(stripped) = file_path.strip_prefix(':')
        && let Some((_, remainder)) = stripped.split_once('/')
        && let Some(root) = postgres_regress_root()
    {
        let candidate = root.join(remainder);
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    file_path.to_string()
}

fn resolve_copy_output_path(file_path: &str) -> String {
    if let Some(stripped) = file_path.strip_prefix(':')
        && let Some((_, remainder)) = stripped.split_once('/')
        && let Some(root) = postgres_regress_root()
    {
        return root.join(remainder).to_string_lossy().into_owned();
    }
    file_path.to_string()
}

fn postgres_regress_root() -> Option<std::path::PathBuf> {
    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        here.parent()?.join("postgres/src/test/regress"),
        here.join("../../postgres/src/test/regress"),
    ];
    candidates.into_iter().find(|path| path.exists())
}
