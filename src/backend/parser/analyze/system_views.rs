use super::query::AnalyzedFrom;
use super::*;
use crate::backend::rewrite::{ViewDmlEvent, load_view_return_query, load_view_return_select};
use crate::backend::utils::cache::system_view_registry::{
    SyntheticSystemViewKind, synthetic_system_view,
};
use crate::backend::utils::trigger::format_trigger_definition;
use crate::include::catalog::{PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID};
use crate::include::nodes::parsenodes::{JoinTreeNode, RangeTblEntryKind};
use crate::include::nodes::primnodes::{
    SetReturningCall, attrno_index, is_system_attr, set_returning_call_exprs,
};

const INFO_SCHEMA_NAME: &str = "information_schema";
const REGRESSION_DATABASE_NAME: &str = "regression";

#[derive(Debug, Clone, Copy, Default)]
struct ViewColumnUpdatability {
    insertable: bool,
    updatable: bool,
}

#[derive(Debug, Clone, Default)]
struct ViewUpdatability {
    insertable: bool,
    updatable: bool,
    deletable: bool,
    trigger_insertable: bool,
    trigger_updatable: bool,
    trigger_deletable: bool,
    columns: Vec<ViewColumnUpdatability>,
}

#[derive(Debug, Clone)]
struct ViewMetadataRow {
    schema_name: String,
    table_name: String,
    relation_oid: u32,
    relation_desc: RelationDesc,
    view_definition: String,
    check_option: &'static str,
    updatability: ViewUpdatability,
}

#[derive(Debug, Clone)]
struct InformationSchemaTriggerRow {
    trigger_schema: String,
    trigger_name: String,
    event_manipulation: &'static str,
    event_object_schema: String,
    event_object_table: String,
    action_order: i32,
    action_condition: Option<String>,
    action_statement: String,
    action_orientation: &'static str,
    action_timing: &'static str,
    action_reference_old_table: Option<String>,
    action_reference_new_table: Option<String>,
}

fn build_values_view(
    name: &str,
    output_columns: Vec<QueryColumn>,
    rows: Vec<Vec<Value>>,
) -> Option<(AnalyzedFrom, BoundScope)> {
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let rows = rows
        .into_iter()
        .map(|row| row.into_iter().map(Expr::Const).collect())
        .collect();
    Some((
        AnalyzedFrom::values(rows, output_columns),
        scope_for_relation(Some(name), &desc),
    ))
}

fn build_function_view(
    name: &str,
    output_columns: Vec<QueryColumn>,
    call: SetReturningCall,
) -> Option<(AnalyzedFrom, BoundScope)> {
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    Some((
        AnalyzedFrom::function(call),
        scope_for_relation(Some(name), &desc),
    ))
}

pub(super) fn bind_builtin_system_view(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Option<(AnalyzedFrom, BoundScope)> {
    let view = synthetic_system_view(name)?;
    if matches!(view.kind, SyntheticSystemViewKind::PgLocks) {
        let output_columns = view.output_columns();
        return build_function_view(
            name,
            output_columns.clone(),
            SetReturningCall::PgLockStatus {
                func_oid: 1371,
                func_variadic: false,
                output_columns,
                with_ordinality: false,
            },
        );
    }
    let rows = match view.kind {
        SyntheticSystemViewKind::PgEnum => catalog
            .enum_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Value::Int64(i64::from(row.oid)),
                    Value::Int64(i64::from(row.enumtypid)),
                    Value::Float64(row.enumsortorder),
                    Value::Text(row.enumlabel.into()),
                ]
            })
            .collect(),
        SyntheticSystemViewKind::PgType => catalog
            .type_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Value::Int64(i64::from(row.oid)),
                    Value::Text(row.typname.into()),
                    Value::Int64(i64::from(row.typnamespace)),
                    Value::Int64(i64::from(row.typowner)),
                    Value::Int16(row.typlen),
                    Value::Bool(row.typbyval),
                    Value::InternalChar(row.typtype as u8),
                    Value::Bool(row.typisdefined),
                    Value::InternalChar(row.typalign.as_char() as u8),
                    Value::InternalChar(row.typstorage.as_char() as u8),
                    Value::Int64(i64::from(row.typrelid)),
                    Value::Int64(i64::from(row.typsubscript)),
                    Value::Int64(i64::from(row.typelem)),
                    Value::Int64(i64::from(row.typarray)),
                    Value::Int64(i64::from(row.typinput)),
                    Value::Int64(i64::from(row.typoutput)),
                    Value::Int64(i64::from(row.typreceive)),
                    Value::Int64(i64::from(row.typsend)),
                    Value::Int64(i64::from(row.typmodin)),
                    Value::Int64(i64::from(row.typmodout)),
                    Value::InternalChar(row.typdelim as u8),
                    Value::Int64(i64::from(row.typanalyze)),
                    Value::Int64(i64::from(row.typbasetype)),
                    Value::Int64(i64::from(row.typcollation)),
                    match row.typacl {
                        Some(values) => Value::Array(
                            values
                                .into_iter()
                                .map(|value| Value::Text(value.into()))
                                .collect(),
                        ),
                        None => Value::Null,
                    },
                ]
            })
            .collect(),
        SyntheticSystemViewKind::PgRange => catalog
            .range_rows()
            .into_iter()
            .map(|row| {
                let rngcanonical = row
                    .rngcanonical
                    .as_deref()
                    .and_then(|name| catalog.proc_rows_by_name(name).first().map(|proc| proc.oid))
                    .unwrap_or(0);
                let rngsubdiff = row
                    .rngsubdiff
                    .as_deref()
                    .and_then(|name| catalog.proc_rows_by_name(name).first().map(|proc| proc.oid))
                    .unwrap_or(0);
                vec![
                    Value::Int64(i64::from(row.rngtypid)),
                    Value::Int64(i64::from(row.rngsubtype)),
                    Value::Int64(i64::from(row.rngmultitypid)),
                    Value::Int64(i64::from(row.rngcollation)),
                    Value::Int64(i64::from(row.rngsubopc)),
                    Value::Int64(i64::from(rngcanonical)),
                    Value::Int64(i64::from(rngsubdiff)),
                ]
            })
            .collect(),
        SyntheticSystemViewKind::PgViews => catalog.pg_views_rows(),
        SyntheticSystemViewKind::PgMatviews => catalog.pg_matviews_rows(),
        SyntheticSystemViewKind::PgIndexes => catalog.pg_indexes_rows(),
        SyntheticSystemViewKind::PgPolicies => catalog.pg_policies_rows(),
        SyntheticSystemViewKind::PgRules => catalog.pg_rules_rows(),
        SyntheticSystemViewKind::PgStats => catalog.pg_stats_rows(),
        SyntheticSystemViewKind::PgSettings => catalog.pg_settings_rows(),
        SyntheticSystemViewKind::PgUserMappings => catalog.pg_user_mappings_rows(),
        SyntheticSystemViewKind::PgRoles => catalog
            .authid_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Value::Text(row.rolname.into()),
                    Value::Int64(i64::from(row.oid)),
                ]
            })
            .collect(),
        SyntheticSystemViewKind::PgStatActivity => catalog.pg_stat_activity_rows(),
        SyntheticSystemViewKind::PgStatAllTables => catalog.pg_stat_all_tables_rows(),
        SyntheticSystemViewKind::PgStatUserTables => catalog.pg_stat_user_tables_rows(),
        SyntheticSystemViewKind::PgStatioUserTables => catalog.pg_statio_user_tables_rows(),
        SyntheticSystemViewKind::PgStatUserFunctions => catalog.pg_stat_user_functions_rows(),
        SyntheticSystemViewKind::PgStatIo => catalog.pg_stat_io_rows(),
        SyntheticSystemViewKind::PgStatProgressCopy => catalog.pg_stat_progress_copy_rows(),
        SyntheticSystemViewKind::PgLocks => unreachable!("pg_locks is bound as pg_lock_status()"),
        SyntheticSystemViewKind::InformationSchemaTables => information_schema_table_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaViews => information_schema_view_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaColumns => {
            information_schema_column_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaColumnColumnUsage => {
            information_schema_column_column_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaTriggers => {
            information_schema_trigger_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaForeignDataWrappers => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrappers_rows(
                catalog.authid_rows(),
                catalog.foreign_data_wrapper_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignDataWrapperOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrapper_options_rows(
                catalog.foreign_data_wrapper_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignServers => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_servers_rows(
                catalog.authid_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.foreign_server_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignServerOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_server_options_rows(
                catalog.foreign_server_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUserMappings => {
            crate::backend::utils::cache::system_views::build_information_schema_user_mappings_rows(
                catalog.authid_rows(),
                catalog.foreign_server_rows(),
                catalog.user_mapping_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUserMappingOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_user_mapping_options_rows(
                catalog.authid_rows(),
                catalog.foreign_server_rows(),
                catalog.user_mapping_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUsagePrivileges
        | SyntheticSystemViewKind::InformationSchemaRoleUsageGrants => {
            crate::backend::utils::cache::system_views::build_information_schema_usage_privileges_rows(
                catalog.authid_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.foreign_server_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignTables => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_tables_rows(
                catalog.namespace_rows(),
                catalog.class_rows(),
                catalog.foreign_server_rows(),
                catalog.foreign_table_rows(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignTableOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_table_options_rows(
                catalog.namespace_rows(),
                catalog.class_rows(),
                catalog.foreign_table_rows(),
            )
        }
    };
    build_values_view(name, view.output_columns(), rows)
}

fn information_schema_table_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    information_schema_view_metadata(catalog)
        .into_iter()
        .map(|view| {
            vec![
                Value::Text(view.table_name.into()),
                yes_or_no(view.updatability.insertable),
            ]
        })
        .collect()
}

fn information_schema_view_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    information_schema_view_metadata(catalog)
        .into_iter()
        .map(|view| {
            vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(view.schema_name.into()),
                Value::Text(view.table_name.into()),
                Value::Text(view.view_definition.into()),
                Value::Text(view.check_option.into()),
                yes_or_no(view.updatability.updatable && view.updatability.deletable),
                yes_or_no(view.updatability.insertable),
                yes_or_no(view.updatability.trigger_updatable),
                yes_or_no(view.updatability.trigger_deletable),
                yes_or_no(view.updatability.trigger_insertable),
            ]
        })
        .collect()
}

fn information_schema_relation_rows(
    catalog: &dyn CatalogLookup,
    relkinds: &[char],
) -> Vec<(String, String, BoundRelation)> {
    let mut seen_relation_oids = std::collections::BTreeSet::new();
    let mut rows = Vec::new();
    for class_row in catalog.class_rows() {
        if (!relkinds.is_empty() && !relkinds.contains(&class_row.relkind))
            || !seen_relation_oids.insert(class_row.oid)
        {
            continue;
        }
        let Some(namespace) = catalog.namespace_row_by_oid(class_row.relnamespace) else {
            continue;
        };
        if namespace.nspname.eq_ignore_ascii_case("pg_catalog")
            || namespace.nspname.eq_ignore_ascii_case(INFO_SCHEMA_NAME)
        {
            continue;
        }
        let Some(relation) = catalog.relation_by_oid(class_row.oid) else {
            continue;
        };
        rows.push((namespace.nspname, class_row.relname, relation));
    }
    rows
}

fn information_schema_column_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let view_metadata = information_schema_view_metadata(catalog)
        .into_iter()
        .map(|view| (view.relation_oid, view.updatability))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for (schema_name, table_name, relation) in
        information_schema_relation_rows(catalog, &['r', 'p', 'v', 'f'])
    {
        for (index, column) in relation.desc.columns.iter().enumerate() {
            if column.dropped {
                continue;
            }
            let is_generated = column.generated.is_some();
            let is_updatable = match relation.relkind {
                'r' | 'p' => true,
                'v' | 'f' => view_metadata
                    .get(&relation.relation_oid)
                    .and_then(|updatability| updatability.columns.get(index))
                    .is_some_and(|entry| entry.insertable || entry.updatable),
                _ => false,
            };
            let type_oid = catalog.type_oid_for_sql_type(column.sql_type).unwrap_or(0);
            let (udt_schema, udt_name) = catalog
                .type_by_oid(type_oid)
                .and_then(|row| {
                    let namespace = catalog.namespace_row_by_oid(row.typnamespace)?;
                    Some((namespace.nspname, row.typname))
                })
                .unwrap_or_else(|| ("pg_catalog".into(), sql_type_name(column.sql_type)));
            rows.push(vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(schema_name.clone().into()),
                Value::Text(table_name.clone().into()),
                Value::Text(column.name.clone().into()),
                Value::Int32((index + 1) as i32),
                if is_generated {
                    Value::Null
                } else {
                    column
                        .default_expr
                        .as_ref()
                        .map(|sql| Value::Text(sql.clone().into()))
                        .unwrap_or(Value::Null)
                },
                yes_or_no(column.storage.nullable),
                Value::Text(information_schema_data_type(column.sql_type, catalog).into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(udt_schema.into()),
                Value::Text(udt_name.into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Text((index + 1).to_string().into()),
                Value::Text("NO".into()),
                yes_or_no(column.identity.is_some()),
                column
                    .identity
                    .map(|kind| match kind {
                        crate::include::nodes::parsenodes::ColumnIdentityKind::Always => {
                            Value::Text("ALWAYS".into())
                        }
                        crate::include::nodes::parsenodes::ColumnIdentityKind::ByDefault => {
                            Value::Text("BY DEFAULT".into())
                        }
                    })
                    .unwrap_or(Value::Null),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                column
                    .identity
                    .map(|_| yes_or_no(false))
                    .unwrap_or(Value::Null),
                Value::Text(if is_generated { "ALWAYS" } else { "NEVER" }.into()),
                if is_generated {
                    column
                        .default_expr
                        .as_ref()
                        .map(|sql| Value::Text(format_generation_expression_sql(sql).into()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                },
                yes_or_no(is_updatable),
            ]);
        }
    }
    rows.sort_by(|left, right| {
        value_text(left.get(1))
            .cmp(value_text(right.get(1)))
            .then_with(|| value_text(left.get(2)).cmp(value_text(right.get(2))))
            .then_with(|| value_int32(left.get(4)).cmp(&value_int32(right.get(4))))
    });
    rows
}

fn information_schema_column_column_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let depend_rows = catalog.depend_rows();
    let mut rows = std::collections::BTreeSet::new();
    for (schema_name, table_name, relation) in
        information_schema_relation_rows(catalog, &['r', 'p'])
    {
        for (dependent_index, dependent_column) in relation.desc.columns.iter().enumerate() {
            if dependent_column.dropped || dependent_column.generated.is_none() {
                continue;
            }
            let Some(attrdef_oid) = dependent_column.attrdef_oid else {
                continue;
            };
            let dependent_attnum = (dependent_index + 1) as i32;
            for depend in depend_rows.iter().filter(|row| {
                row.classid == PG_ATTRDEF_RELATION_OID
                    && row.objid == attrdef_oid
                    && row.refclassid == PG_CLASS_RELATION_OID
                    && row.refobjid == relation.relation_oid
                    && row.refobjsubid > 0
                    && row.refobjsubid != dependent_attnum
            }) {
                let Some(column_index) = depend
                    .refobjsubid
                    .checked_sub(1)
                    .and_then(|index| usize::try_from(index).ok())
                else {
                    continue;
                };
                let Some(source_column) = relation.desc.columns.get(column_index) else {
                    continue;
                };
                if source_column.dropped {
                    continue;
                }
                rows.insert((
                    schema_name.clone(),
                    table_name.clone(),
                    source_column.name.clone(),
                    dependent_column.name.clone(),
                ));
            }
        }
    }
    rows.into_iter()
        .map(|(schema_name, table_name, column_name, dependent_column)| {
            vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(schema_name.into()),
                Value::Text(table_name.into()),
                Value::Text(column_name.into()),
                Value::Text(dependent_column.into()),
            ]
        })
        .collect()
}

fn information_schema_data_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> String {
    if sql_type.is_array {
        return "ARRAY".into();
    }
    let Some(type_oid) = catalog.type_oid_for_sql_type(sql_type) else {
        return sql_type_name(sql_type);
    };
    let Some(row) = catalog.type_by_oid(type_oid) else {
        return sql_type_name(sql_type);
    };
    let schema_name = catalog
        .namespace_row_by_oid(row.typnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "pg_catalog".into());
    if schema_name == "pg_catalog" {
        sql_type_name(sql_type)
    } else {
        "USER-DEFINED".into()
    }
}

fn format_generation_expression_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.is_empty() || trimmed.starts_with('(') {
        return trimmed.to_string();
    }
    match crate::backend::parser::parse_expr(trimmed) {
        Ok(SqlExpr::Column(_))
        | Ok(SqlExpr::Const(_))
        | Ok(SqlExpr::IntegerLiteral(_))
        | Ok(SqlExpr::NumericLiteral(_))
        | Ok(SqlExpr::FuncCall { .. }) => trimmed.to_string(),
        Ok(_) => format!("({trimmed})"),
        Err(_) => trimmed.to_string(),
    }
}

fn value_text(value: Option<&Value>) -> &str {
    match value {
        Some(Value::Text(value)) => value.as_str(),
        _ => "",
    }
}

fn value_int32(value: Option<&Value>) -> i32 {
    match value {
        Some(Value::Int32(value)) => *value,
        _ => 0,
    }
}

fn information_schema_trigger_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for trigger in catalog.trigger_rows() {
        if trigger.tgisinternal {
            continue;
        }
        let Some(relation) = catalog.relation_by_oid(trigger.tgrelid) else {
            continue;
        };
        let Some(class_row) = catalog.class_row_by_oid(trigger.tgrelid) else {
            continue;
        };
        let Some(namespace) = catalog.namespace_row_by_oid(relation.namespace_oid) else {
            continue;
        };
        if namespace.nspname.eq_ignore_ascii_case("pg_catalog")
            || namespace.nspname.eq_ignore_ascii_case(INFO_SCHEMA_NAME)
        {
            continue;
        }
        let Some(formatted) = format_trigger_definition(catalog, &trigger, false) else {
            continue;
        };
        for event_manipulation in formatted.event_manipulations {
            rows.push(InformationSchemaTriggerRow {
                trigger_schema: namespace.nspname.clone(),
                trigger_name: trigger.tgname.clone(),
                event_manipulation,
                event_object_schema: namespace.nspname.clone(),
                event_object_table: class_row.relname.clone(),
                action_order: 0,
                action_condition: formatted.action_condition.clone(),
                action_statement: formatted.action_statement.clone(),
                action_orientation: formatted.action_orientation,
                action_timing: formatted.action_timing,
                action_reference_old_table: formatted.action_reference_old_table.clone(),
                action_reference_new_table: formatted.action_reference_new_table.clone(),
            });
        }
    }

    rows.sort_by(|left, right| {
        left.trigger_schema
            .cmp(&right.trigger_schema)
            .then_with(|| left.event_object_table.cmp(&right.event_object_table))
            .then_with(|| left.event_manipulation.cmp(right.event_manipulation))
            .then_with(|| left.action_orientation.cmp(right.action_orientation))
            .then_with(|| left.action_timing.cmp(right.action_timing))
            .then_with(|| left.trigger_name.cmp(&right.trigger_name))
    });

    let mut last_partition: Option<(String, String, &'static str, &'static str, &'static str)> =
        None;
    let mut next_order = 0_i32;
    for row in &mut rows {
        let partition = (
            row.event_object_schema.clone(),
            row.event_object_table.clone(),
            row.event_manipulation,
            row.action_orientation,
            row.action_timing,
        );
        if last_partition.as_ref() != Some(&partition) {
            next_order = 1;
            last_partition = Some(partition);
        } else {
            next_order += 1;
        }
        row.action_order = next_order;
    }

    rows.into_iter()
        .map(|row| {
            vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(row.trigger_schema.clone().into()),
                Value::Text(row.trigger_name.into()),
                Value::Text(row.event_manipulation.into()),
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(row.event_object_schema.into()),
                Value::Text(row.event_object_table.into()),
                Value::Int32(row.action_order),
                row.action_condition
                    .map(|value| Value::Text(value.into()))
                    .unwrap_or(Value::Null),
                Value::Text(row.action_statement.into()),
                Value::Text(row.action_orientation.into()),
                Value::Text(row.action_timing.into()),
                row.action_reference_old_table
                    .map(|value| Value::Text(value.into()))
                    .unwrap_or(Value::Null),
                row.action_reference_new_table
                    .map(|value| Value::Text(value.into()))
                    .unwrap_or(Value::Null),
                Value::Null,
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

fn information_schema_view_metadata(catalog: &dyn CatalogLookup) -> Vec<ViewMetadataRow> {
    let mut rows = information_schema_relation_rows(catalog, &['v'])
        .into_iter()
        .map(|(schema_name, table_name, relation)| {
            let (view_definition, check_option) =
                view_definition_and_check_option(catalog, relation.relation_oid);
            ViewMetadataRow {
                schema_name,
                table_name,
                relation_oid: relation.relation_oid,
                relation_desc: relation.desc.clone(),
                view_definition,
                check_option,
                updatability: describe_view_updatability(
                    relation.relation_oid,
                    &relation.desc,
                    catalog,
                ),
            }
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| left.table_name.cmp(&right.table_name))
            .then_with(|| left.relation_oid.cmp(&right.relation_oid))
    });
    rows
}

fn view_definition_and_check_option(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> (String, &'static str) {
    let sql = catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.rulename == "_RETURN")
        .map(|row| row.ev_action)
        .unwrap_or_default();
    let (definition, check_option) =
        crate::backend::rewrite::split_stored_view_definition_sql(&sql);
    (
        definition.to_string(),
        match check_option {
            crate::include::nodes::parsenodes::ViewCheckOption::None => "NONE",
            crate::include::nodes::parsenodes::ViewCheckOption::Local => "LOCAL",
            crate::include::nodes::parsenodes::ViewCheckOption::Cascaded => "CASCADED",
        },
    )
}

fn describe_view_updatability(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> ViewUpdatability {
    describe_view_updatability_inner(relation_oid, relation_desc, catalog, &[]).unwrap_or_else(
        |_| ViewUpdatability {
            trigger_insertable: has_instead_trigger(relation_oid, ViewDmlEvent::Insert, catalog),
            trigger_updatable: has_instead_trigger(relation_oid, ViewDmlEvent::Update, catalog),
            trigger_deletable: has_instead_trigger(relation_oid, ViewDmlEvent::Delete, catalog),
            columns: vec![ViewColumnUpdatability::default(); relation_desc.columns.len()],
            ..ViewUpdatability::default()
        },
    )
}

fn describe_view_updatability_inner(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<ViewUpdatability, ParseError> {
    let trigger_insertable = has_instead_trigger(relation_oid, ViewDmlEvent::Insert, catalog);
    let trigger_updatable = has_instead_trigger(relation_oid, ViewDmlEvent::Update, catalog);
    let trigger_deletable = has_instead_trigger(relation_oid, ViewDmlEvent::Delete, catalog);
    if expanded_views.contains(&relation_oid) {
        return Ok(ViewUpdatability {
            trigger_insertable,
            trigger_updatable,
            trigger_deletable,
            columns: vec![ViewColumnUpdatability::default(); relation_desc.columns.len()],
            ..ViewUpdatability::default()
        });
    }

    let raw_select = load_view_return_select(relation_oid, None, catalog, expanded_views)?;
    let query = load_view_return_query(relation_oid, relation_desc, None, catalog, expanded_views)?;
    let auto = describe_auto_updatable_view_shape(
        &raw_select,
        &query,
        relation_desc,
        catalog,
        expanded_views,
    );

    Ok(ViewUpdatability {
        insertable: auto.insertable
            || has_unconditional_instead_rule(relation_oid, ViewDmlEvent::Insert, catalog),
        updatable: auto.updatable
            || has_unconditional_instead_rule(relation_oid, ViewDmlEvent::Update, catalog),
        deletable: auto.deletable
            || has_unconditional_instead_rule(relation_oid, ViewDmlEvent::Delete, catalog),
        trigger_insertable,
        trigger_updatable,
        trigger_deletable,
        columns: auto.columns,
    })
}

fn describe_auto_updatable_view_shape(
    raw_select: &SelectStatement,
    query: &Query,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> ViewUpdatability {
    let mut result = ViewUpdatability {
        columns: vec![ViewColumnUpdatability::default(); relation_desc.columns.len()],
        ..ViewUpdatability::default()
    };

    if raw_select.distinct
        || !query.group_by.is_empty()
        || query.having_qual.is_some()
        || raw_select.set_operation.is_some()
        || query.recursive_union.is_some()
        || !raw_select.with.is_empty()
        || query.limit_count.is_some()
        || query.limit_offset != 0
        || !query.accumulators.is_empty()
        || !query.window_clauses.is_empty()
        || query.has_target_srfs
        || query.where_qual.as_ref().is_some_and(expr_contains_sublink)
    {
        return result;
    }

    let Some(JoinTreeNode::RangeTblRef(base_rtindex)) = query.jointree.as_ref() else {
        return result;
    };
    let Some(base_rte) = query.rtable.get(base_rtindex - 1) else {
        return result;
    };
    let RangeTblEntryKind::Relation {
        relation_oid,
        relkind,
        ..
    } = &base_rte.kind
    else {
        return result;
    };

    let mut any_insertable = false;
    let mut any_updatable = false;
    let nested = if *relkind == 'v' {
        let Some(base_relation) = catalog
            .lookup_relation_by_oid(*relation_oid)
            .or_else(|| catalog.relation_by_oid(*relation_oid))
        else {
            return result;
        };
        let mut next_expanded = expanded_views.to_vec();
        next_expanded.push(*relation_oid);
        Some(
            describe_view_updatability_inner(
                *relation_oid,
                &base_relation.desc,
                catalog,
                &next_expanded,
            )
            .unwrap_or_default(),
        )
    } else {
        None
    };

    for (index, target) in query.target_list.iter().enumerate() {
        if index >= relation_desc.columns.len()
            || target.resjunk
            || expr_contains_sublink(&target.expr)
        {
            continue;
        }
        let Expr::Var(var) = &target.expr else {
            continue;
        };
        if var.varlevelsup != 0 || var.varno != *base_rtindex {
            continue;
        }
        let Some(column_index) = attrno_index(var.varattno) else {
            continue;
        };

        result.columns[index] = match *relkind {
            'r' if !is_system_attr(var.varattno) => ViewColumnUpdatability {
                insertable: true,
                updatable: true,
            },
            'v' => nested
                .as_ref()
                .and_then(|entry| entry.columns.get(column_index))
                .copied()
                .unwrap_or_default(),
            _ => ViewColumnUpdatability::default(),
        };

        any_insertable |= result.columns[index].insertable;
        any_updatable |= result.columns[index].updatable;
    }

    match *relkind {
        'r' => {
            result.insertable = any_insertable;
            result.updatable = any_updatable;
            result.deletable = true;
        }
        'v' => {
            let Some(nested) = nested else {
                return result;
            };
            result.insertable = any_insertable && nested.insertable;
            result.updatable = any_updatable && nested.updatable;
            result.deletable = nested.deletable;
        }
        _ => {}
    }

    result
}

fn has_unconditional_instead_rule(
    relation_oid: u32,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
) -> bool {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| {
            row.rulename != "_RETURN"
                && row.ev_type == view_event_rule_code(event)
                && row.is_instead
                && row.ev_qual.trim().is_empty()
        })
}

fn has_instead_trigger(
    relation_oid: u32,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
) -> bool {
    let required_bits = match event {
        ViewDmlEvent::Update => 81,
        ViewDmlEvent::Delete => 73,
        ViewDmlEvent::Insert => 69,
    };
    catalog
        .trigger_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| row.tgtype & required_bits == required_bits)
}

fn yes_or_no(value: bool) -> Value {
    Value::Text(if value { "YES" } else { "NO" }.into())
}

fn view_event_rule_code(event: ViewDmlEvent) -> char {
    match event {
        ViewDmlEvent::Update => '2',
        ViewDmlEvent::Insert => '3',
        ViewDmlEvent::Delete => '4',
    }
}

fn expr_contains_sublink(expr: &Expr) -> bool {
    match expr {
        Expr::SubLink(_) | Expr::SubPlan(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_sublink),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_sublink),
        Expr::Func(func) => func.args.iter().any(expr_contains_sublink),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_contains_sublink)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_sublink),
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_sublink)
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_sublink(&item.expr))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_sublink(expr))
        }
        Expr::WindowFunc(window) => window.args.iter().any(expr_contains_sublink),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_sublink(&saop.left) || expr_contains_sublink(&saop.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_sublink),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => expr_contains_sublink(inner),
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
            expr_contains_sublink(expr)
                || expr_contains_sublink(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_sublink(expr))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_sublink(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_sublink(left) || expr_contains_sublink(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_sublink),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_sublink(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_sublink(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_sublink(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_contains_sublink(expr))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_contains_sublink(expr))
                })
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_sublink(expr))
                || case_expr.args.iter().any(|when| {
                    expr_contains_sublink(&when.expr) || expr_contains_sublink(&when.result)
                })
                || expr_contains_sublink(&case_expr.defresult)
        }
        Expr::Param(_)
        | Expr::Var(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}
