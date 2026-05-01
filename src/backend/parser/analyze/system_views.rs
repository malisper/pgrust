use super::query::AnalyzedFrom;
use super::*;
use crate::backend::rewrite::{
    ViewDmlEvent, load_view_return_query, load_view_return_select,
    render_relation_expr_sql_for_information_schema,
};
use crate::backend::utils::cache::system_view_registry::{
    SyntheticSystemViewKind, synthetic_system_view,
};
use crate::backend::utils::trigger::format_trigger_definition;
use crate::include::catalog::{
    CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID,
    PG_PROC_RELATION_OID, TEXT_TYPE_OID,
};
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
    let width = output_columns.len();
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let rows = rows
        .into_iter()
        // Keep malformed synthetic rows from turning into later executor
        // tuple-width failures when a query projects a missing column.
        .filter(|row| row.len() == width)
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
    if let Some(function) = view.set_returning_function() {
        let output_columns = view.output_columns();
        return build_function_view(
            name,
            output_columns.clone(),
            SetReturningCall::UserDefined {
                proc_oid: function.proc_oid,
                function_name: function.function_name.into(),
                func_variadic: false,
                args: Vec::new(),
                inlined_expr: None,
                output_columns,
                with_ordinality: false,
            },
        );
    }
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
    if matches!(view.kind, SyntheticSystemViewKind::PgStatProgressCopy) {
        let output_columns = view.output_columns();
        return build_function_view(
            name,
            output_columns.clone(),
            SetReturningCall::PgStatProgressCopy {
                output_columns,
                with_ordinality: false,
            },
        );
    }
    if matches!(view.kind, SyntheticSystemViewKind::PgSequences) {
        let output_columns = view.output_columns();
        return build_function_view(
            name,
            output_columns.clone(),
            SetReturningCall::PgSequences {
                output_columns,
                with_ordinality: false,
            },
        );
    }
    if matches!(
        view.kind,
        SyntheticSystemViewKind::InformationSchemaSequences
    ) {
        let output_columns = view.output_columns();
        return build_function_view(
            name,
            output_columns.clone(),
            SetReturningCall::InformationSchemaSequences {
                output_columns,
                with_ordinality: false,
            },
        );
    }
    let rows = match view.kind {
        SyntheticSystemViewKind::PgShmemAllocationsNuma => Vec::new(),
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
                let domain = catalog.domain_by_type_oid(row.oid);
                let typnotnull = domain.as_ref().is_some_and(|domain| domain.not_null);
                let typdefault = domain
                    .as_ref()
                    .and_then(|domain| domain.default.clone())
                    .or_else(|| catalog.type_default_sql(row.oid));
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
                    Value::Int32(row.sql_type.typmod),
                    Value::Int64(i64::from(row.typcollation)),
                    Value::Bool(typnotnull),
                    typdefault
                        .map(|value| Value::Text(value.into()))
                        .unwrap_or(Value::Null),
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
        SyntheticSystemViewKind::PgConstraint => catalog
            .constraint_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Value::Int64(i64::from(row.oid)),
                    Value::Text(row.conname.into()),
                    Value::Int64(i64::from(row.connamespace)),
                    Value::Text(row.contype.to_string().into()),
                    Value::Bool(row.condeferrable),
                    Value::Bool(row.condeferred),
                    Value::Bool(row.conenforced),
                    Value::Bool(row.convalidated),
                    Value::Int64(i64::from(row.conrelid)),
                    Value::Int64(i64::from(row.contypid)),
                    Value::Int64(i64::from(row.conindid)),
                    Value::Int64(i64::from(row.conparentid)),
                    Value::Int64(i64::from(row.confrelid)),
                    Value::Text(row.confupdtype.to_string().into()),
                    Value::Text(row.confdeltype.to_string().into()),
                    Value::Text(row.confmatchtype.to_string().into()),
                    nullable_int2_array(row.conkey),
                    nullable_int2_array(row.confkey),
                    nullable_oid_array(row.conpfeqop),
                    nullable_oid_array(row.conppeqop),
                    nullable_oid_array(row.conffeqop),
                    nullable_int2_array(row.confdelsetcols),
                    nullable_oid_array(row.conexclop),
                    row.conbin
                        .map(|value| Value::Text(value.into()))
                        .unwrap_or(Value::Null),
                    Value::Bool(row.conislocal),
                    Value::Int16(row.coninhcount),
                    Value::Bool(row.connoinherit),
                    Value::Bool(row.conperiod),
                ]
            })
            .collect(),
        SyntheticSystemViewKind::PgInitPrivs => {
            // :HACK: Model the initdb-populated catalog just enough for pg_dump
            // and regression visibility checks until bootstrap records initial ACLs.
            vec![vec![
                Value::Int64(i64::from(PG_PROC_RELATION_OID)),
                Value::Int64(i64::from(PG_CLASS_RELATION_OID)),
                Value::Int32(0),
                Value::InternalChar(b'i'),
                Value::Array(vec![Value::Text("postgres=arwdDxt/postgres".into())]),
            ]]
        }
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
        SyntheticSystemViewKind::PgTables => catalog.pg_tables_rows(),
        SyntheticSystemViewKind::PgViews => catalog.pg_views_rows(),
        SyntheticSystemViewKind::PgMatviews => catalog.pg_matviews_rows(),
        SyntheticSystemViewKind::PgIndexes => catalog.pg_indexes_rows(),
        SyntheticSystemViewKind::PgPolicies => catalog.pg_policies_rows(),
        SyntheticSystemViewKind::PgPublicationTables => {
            crate::backend::utils::cache::system_views::build_pg_publication_tables_rows(
                catalog.publication_rows(),
                catalog.publication_rel_rows(),
                catalog.publication_namespace_rows(),
                catalog.namespace_rows(),
                catalog.class_rows(),
                catalog.attribute_rows(),
                catalog.inheritance_rows(),
            )
        }
        SyntheticSystemViewKind::PgSequences => {
            unreachable!("pg_sequences is bound as a function view")
        }
        SyntheticSystemViewKind::PgRules => catalog.pg_rules_rows(),
        SyntheticSystemViewKind::PgStats => catalog.pg_stats_rows(),
        SyntheticSystemViewKind::PgStatsExt => catalog.pg_stats_ext_rows(),
        SyntheticSystemViewKind::PgStatsExtExprs => catalog.pg_stats_ext_exprs_rows(),
        SyntheticSystemViewKind::PgAvailableExtensions
        | SyntheticSystemViewKind::PgAvailableExtensionVersions
        | SyntheticSystemViewKind::PgBackendMemoryContexts
        | SyntheticSystemViewKind::PgConfig
        | SyntheticSystemViewKind::PgCursors
        | SyntheticSystemViewKind::PgFileSettings
        | SyntheticSystemViewKind::PgHbaFileRules
        | SyntheticSystemViewKind::PgIdentFileMappings
        | SyntheticSystemViewKind::PgPreparedXacts
        | SyntheticSystemViewKind::PgPreparedStatements
        | SyntheticSystemViewKind::PgSettings
        | SyntheticSystemViewKind::PgStatWalReceiver
        | SyntheticSystemViewKind::PgWaitEvents
        | SyntheticSystemViewKind::PgTimezoneNames
        | SyntheticSystemViewKind::PgTimezoneAbbrevs => {
            unreachable!("SRF-backed system views are bound before value-row expansion")
        }
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
        SyntheticSystemViewKind::PgStatDatabase => catalog.pg_stat_database_rows(),
        SyntheticSystemViewKind::PgStatCheckpointer => catalog.pg_stat_checkpointer_rows(),
        SyntheticSystemViewKind::PgStatWal => catalog.pg_stat_wal_rows(),
        SyntheticSystemViewKind::PgStatSlru => catalog.pg_stat_slru_rows(),
        SyntheticSystemViewKind::PgStatArchiver => catalog.pg_stat_archiver_rows(),
        SyntheticSystemViewKind::PgStatBgwriter => catalog.pg_stat_bgwriter_rows(),
        SyntheticSystemViewKind::PgStatRecoveryPrefetch => {
            catalog.pg_stat_recovery_prefetch_rows()
        }
        SyntheticSystemViewKind::PgStatSubscriptionStats => catalog.pg_stat_subscription_stats_rows(),
        SyntheticSystemViewKind::PgStatAllTables => catalog.pg_stat_all_tables_rows(),
        SyntheticSystemViewKind::PgStatUserTables => catalog.pg_stat_user_tables_rows(),
        SyntheticSystemViewKind::PgStatioUserTables => catalog.pg_statio_user_tables_rows(),
        SyntheticSystemViewKind::PgStatUserFunctions => catalog.pg_stat_user_functions_rows(),
        SyntheticSystemViewKind::PgStatIo => catalog.pg_stat_io_rows(),
        SyntheticSystemViewKind::PgStatProgressCopy => {
            unreachable!("pg_stat_progress_copy is bound as a runtime SRF")
        }
        SyntheticSystemViewKind::PgLocks => unreachable!("pg_locks is bound as pg_lock_status()"),
        SyntheticSystemViewKind::InformationSchemaTables => information_schema_table_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaViews => information_schema_view_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaSequences => {
            unreachable!("information_schema.sequences is bound as a function view")
        }
        SyntheticSystemViewKind::InformationSchemaColumns => {
            information_schema_column_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaRoutines => information_schema_routine_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaParameters => {
            information_schema_parameter_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaRoutineRoutineUsage => {
            information_schema_routine_routine_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaRoutineSequenceUsage => {
            information_schema_routine_sequence_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaRoutineColumnUsage => {
            information_schema_routine_column_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaRoutineTableUsage => {
            information_schema_routine_table_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaColumnColumnUsage => {
            information_schema_column_column_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaColumnDomainUsage => {
            information_schema_column_domain_usage_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaDomainConstraints => {
            information_schema_domain_constraints_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaDomains => information_schema_domain_rows(catalog),
        SyntheticSystemViewKind::InformationSchemaCheckConstraints => {
            information_schema_check_constraint_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaTriggers => {
            information_schema_trigger_rows(catalog)
        }
        SyntheticSystemViewKind::InformationSchemaForeignDataWrappers => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrappers_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignDataWrapperOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrapper_options_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignServers => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_servers_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.foreign_server_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaForeignServerOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_foreign_server_options_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_server_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUserMappings => {
            crate::backend::utils::cache::system_views::build_information_schema_user_mappings_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_server_rows(),
                catalog.user_mapping_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUserMappingOptions => {
            crate::backend::utils::cache::system_views::build_information_schema_user_mapping_options_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_server_rows(),
                catalog.user_mapping_rows(),
                catalog.current_user_oid(),
            )
        }
        SyntheticSystemViewKind::InformationSchemaUsagePrivileges
        | SyntheticSystemViewKind::InformationSchemaRoleUsageGrants => {
            crate::backend::utils::cache::system_views::build_information_schema_usage_privileges_rows(
                catalog.authid_rows(),
                catalog.auth_members_rows(),
                catalog.foreign_data_wrapper_rows(),
                catalog.foreign_server_rows(),
                catalog.current_user_oid(),
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

fn nullable_int2_array(values: Option<Vec<i16>>) -> Value {
    values
        .map(|values| {
            Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(
                    values.into_iter().map(Value::Int16).collect(),
                )
                .with_element_type_oid(crate::include::catalog::INT2_TYPE_OID),
            )
        })
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MalformedPgIndexesCatalog;

    impl CatalogLookup for MalformedPgIndexesCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }

        fn pg_indexes_rows(&self) -> Vec<Vec<Value>> {
            vec![vec![
                Value::Text("pg_catalog".into()),
                Value::Text("pg_settings".into()),
                Value::Text("pg_settings_u".into()),
                Value::Text("CREATE RULE pg_settings_u AS ...".into()),
            ]]
        }
    }

    #[test]
    fn synthetic_system_view_drops_malformed_rows_before_planning() {
        let (from, _) = bind_builtin_system_view("pg_indexes", &MalformedPgIndexesCatalog).unwrap();
        assert_eq!(from.output_columns.len(), 5);

        let RangeTblEntryKind::Values { rows, .. } = &from.rtable[0].kind else {
            panic!("expected pg_indexes to bind as a values view");
        };
        assert!(rows.is_empty());
    }
}

fn nullable_oid_array(values: Option<Vec<u32>>) -> Value {
    values
        .map(|values| {
            Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(
                    values
                        .into_iter()
                        .map(|value| Value::Int32(value as i32))
                        .collect(),
                )
                .with_element_type_oid(crate::include::catalog::OID_TYPE_OID),
            )
        })
        .unwrap_or(Value::Null)
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
    let sequence_rows = catalog
        .sequence_rows()
        .into_iter()
        .map(|row| (row.seqrelid, row))
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
            let identity_sequence = column
                .identity
                .and(column.default_sequence_oid)
                .and_then(|sequence_oid| sequence_rows.get(&sequence_oid));
            rows.push(vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(schema_name.clone().into()),
                Value::Text(table_name.clone().into()),
                Value::Text(column.name.clone().into()),
                Value::Int32((index + 1) as i32),
                if is_generated || column.identity.is_some() {
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
                identity_sequence
                    .map(|row| Value::Text(row.seqstart.to_string().into()))
                    .unwrap_or(Value::Null),
                identity_sequence
                    .map(|row| Value::Text(row.seqincrement.to_string().into()))
                    .unwrap_or(Value::Null),
                identity_sequence
                    .map(|row| Value::Text(row.seqmax.to_string().into()))
                    .unwrap_or(Value::Null),
                identity_sequence
                    .map(|row| Value::Text(row.seqmin.to_string().into()))
                    .unwrap_or(Value::Null),
                column
                    .identity
                    .map(|_| yes_or_no(identity_sequence.is_some_and(|row| row.seqcycle)))
                    .unwrap_or_else(|| yes_or_no(false)),
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

fn information_schema_routine_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    catalog
        .proc_rows()
        .into_iter()
        .filter(|row| row.prokind == 'f')
        .filter_map(|row| {
            let schema = routine_schema_name(catalog, row.pronamespace)?;
            Some(vec![
                Value::Text(schema.clone().into()),
                Value::Text(row.proname.clone().into()),
                Value::Text(schema.into()),
                Value::Text(row.proname.into()),
            ])
        })
        .collect()
}

fn information_schema_parameter_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for proc_row in catalog
        .proc_rows()
        .into_iter()
        .filter(|row| row.prokind == 'f')
    {
        let Some(schema) = routine_schema_name(catalog, proc_row.pronamespace) else {
            continue;
        };
        let defaults = decode_information_schema_proc_arg_defaults(&proc_row);
        let names = proc_row.proargnames.clone().unwrap_or_default();
        if let (Some(all_argtypes), Some(modes)) = (
            proc_row.proallargtypes.as_ref(),
            proc_row.proargmodes.as_ref(),
        ) {
            let mut input_index = 0usize;
            for (index, (type_oid, mode)) in all_argtypes.iter().zip(modes.iter()).enumerate() {
                let is_input = matches!(*mode, b'i' | b'b' | b'v');
                let default = if is_input {
                    let value = defaults.get(input_index).cloned().flatten();
                    input_index += 1;
                    value
                } else {
                    None
                };
                rows.push(information_schema_parameter_row(
                    &schema,
                    &proc_row.proname,
                    index,
                    names.get(index).cloned().unwrap_or_default(),
                    default,
                    *type_oid,
                ));
            }
            continue;
        }
        let arg_oids = parse_information_schema_proc_argtype_oids(&proc_row.proargtypes);
        for (index, type_oid) in arg_oids.iter().copied().enumerate() {
            rows.push(information_schema_parameter_row(
                &schema,
                &proc_row.proname,
                index,
                names.get(index).cloned().unwrap_or_default(),
                defaults.get(index).cloned().flatten(),
                type_oid,
            ));
        }
    }
    rows
}

fn information_schema_parameter_row(
    schema: &str,
    routine_name: &str,
    index: usize,
    parameter_name: String,
    default: Option<String>,
    type_oid: u32,
) -> Vec<Value> {
    vec![
        Value::Text(schema.into()),
        Value::Text(routine_name.into()),
        Value::Int32((index + 1) as i32),
        if parameter_name.is_empty() {
            Value::Null
        } else {
            Value::Text(parameter_name.into())
        },
        default
            .map(|value| Value::Text(format_parameter_default(value, type_oid).into()))
            .unwrap_or(Value::Null),
    ]
}

fn information_schema_routine_routine_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    routine_depend_rows(catalog, PG_PROC_RELATION_OID)
        .into_iter()
        .filter_map(|(routine, depend)| {
            let referenced = catalog.proc_row_by_oid(depend.refobjid)?;
            routine_schema_name(catalog, referenced.pronamespace)?;
            Some(vec![
                Value::Text(routine.proname.into()),
                Value::Text(referenced.proname.into()),
            ])
        })
        .collect()
}

fn information_schema_routine_sequence_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    routine_depend_rows(catalog, PG_CLASS_RELATION_OID)
        .into_iter()
        .filter_map(|(routine, depend)| {
            let class = catalog.class_row_by_oid(depend.refobjid)?;
            (class.relkind == 'S').then_some(())?;
            let schema = routine_schema_name(catalog, routine.pronamespace)?;
            Some(vec![
                Value::Text(schema.into()),
                Value::Text(routine.proname.into()),
                Value::Text(class.relname.into()),
            ])
        })
        .collect()
}

fn information_schema_routine_column_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    routine_depend_rows(catalog, PG_CLASS_RELATION_OID)
        .into_iter()
        .filter(|(_, depend)| depend.refobjsubid > 0)
        .filter_map(|(routine, depend)| {
            let schema = routine_schema_name(catalog, routine.pronamespace)?;
            let relation = catalog.lookup_relation_by_oid(depend.refobjid)?;
            let class = catalog.class_row_by_oid(depend.refobjid)?;
            (!matches!(class.relkind, 'S')).then_some(())?;
            let column = depend
                .refobjsubid
                .checked_sub(1)
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| relation.desc.columns.get(index))?;
            (!column.dropped).then_some(())?;
            Some(vec![
                Value::Text(schema.into()),
                Value::Text(routine.proname.into()),
                Value::Text(class.relname.into()),
                Value::Text(column.name.clone().into()),
            ])
        })
        .collect()
}

fn information_schema_routine_table_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut seen = std::collections::BTreeSet::new();
    routine_depend_rows(catalog, PG_CLASS_RELATION_OID)
        .into_iter()
        .filter_map(|(routine, depend)| {
            let schema = routine_schema_name(catalog, routine.pronamespace)?;
            let class = catalog.class_row_by_oid(depend.refobjid)?;
            matches!(class.relkind, 'r' | 'p' | 'v').then_some(())?;
            seen.insert((routine.oid, class.oid)).then_some(vec![
                Value::Text(schema.into()),
                Value::Text(routine.proname.into()),
                Value::Text(class.relname.into()),
            ])
        })
        .collect()
}

fn routine_depend_rows(
    catalog: &dyn CatalogLookup,
    refclassid: u32,
) -> Vec<(
    crate::include::catalog::PgProcRow,
    crate::include::catalog::PgDependRow,
)> {
    let visible_routine_oids = user_visible_routines(catalog)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<std::collections::BTreeMap<_, _>>();
    catalog
        .depend_rows()
        .into_iter()
        .filter(|row| {
            row.classid == PG_PROC_RELATION_OID
                && row.objsubid == 0
                && row.refclassid == refclassid
                && visible_routine_oids.contains_key(&row.objid)
        })
        .filter_map(|row| {
            visible_routine_oids
                .get(&row.objid)
                .cloned()
                .map(|routine| (routine, row))
        })
        .collect()
}

fn user_visible_routines(catalog: &dyn CatalogLookup) -> Vec<crate::include::catalog::PgProcRow> {
    catalog
        .proc_rows()
        .into_iter()
        .filter(|row| row.prokind == 'f')
        .filter(|row| routine_schema_name(catalog, row.pronamespace).is_some())
        .collect()
}

fn routine_schema_name(catalog: &dyn CatalogLookup, namespace_oid: u32) -> Option<String> {
    let namespace = catalog.namespace_row_by_oid(namespace_oid)?;
    (!namespace.nspname.eq_ignore_ascii_case("pg_catalog")
        && !namespace.nspname.eq_ignore_ascii_case(INFO_SCHEMA_NAME))
    .then_some(namespace.nspname)
}

fn parse_information_schema_proc_argtype_oids(argtypes: &str) -> Vec<u32> {
    argtypes
        .split_whitespace()
        .filter_map(|item| item.parse::<u32>().ok())
        .collect()
}

fn decode_information_schema_proc_arg_defaults(
    row: &crate::include::catalog::PgProcRow,
) -> Vec<Option<String>> {
    let input_count = row.pronargs.max(0) as usize;
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

fn format_parameter_default(default: String, type_oid: u32) -> String {
    if type_oid == TEXT_TYPE_OID && default.starts_with('\'') && !default.contains("::") {
        format!("{default}::text")
    } else {
        default
    }
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

fn information_schema_column_domain_usage_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (schema_name, table_name, relation) in
        information_schema_relation_rows(catalog, &['r', 'p', 'v', 'f'])
    {
        for column in relation
            .desc
            .columns
            .iter()
            .filter(|column| !column.dropped)
        {
            let Some(type_oid) = catalog.type_oid_for_sql_type(column.sql_type) else {
                continue;
            };
            let Some(type_row) = catalog.type_by_oid(type_oid) else {
                continue;
            };
            if type_row.typtype != 'd' {
                continue;
            }
            let Some(domain_schema) = catalog
                .namespace_row_by_oid(type_row.typnamespace)
                .map(|row| row.nspname)
            else {
                continue;
            };
            rows.push(vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(domain_schema.into()),
                Value::Text(type_row.typname.into()),
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(schema_name.clone().into()),
                Value::Text(table_name.clone().into()),
                Value::Text(column.name.clone().into()),
            ]);
        }
    }
    rows.sort_by(|left, right| {
        value_text(left.get(2))
            .cmp(value_text(right.get(2)))
            .then_with(|| value_text(left.get(5)).cmp(value_text(right.get(5))))
            .then_with(|| value_text(left.get(6)).cmp(value_text(right.get(6))))
    });
    rows
}

fn information_schema_domain_constraints_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for constraint in catalog.constraint_rows() {
        if constraint.contypid == 0
            || !matches!(constraint.contype, CONSTRAINT_CHECK | CONSTRAINT_NOTNULL)
        {
            continue;
        }
        let Some(domain_row) = catalog.type_by_oid(constraint.contypid) else {
            continue;
        };
        if domain_row.typtype != 'd' {
            continue;
        }
        let Some(constraint_schema) = catalog
            .namespace_row_by_oid(constraint.connamespace)
            .map(|row| row.nspname)
        else {
            continue;
        };
        let Some(domain_schema) = catalog
            .namespace_row_by_oid(domain_row.typnamespace)
            .map(|row| row.nspname)
        else {
            continue;
        };
        rows.push(vec![
            Value::Text(REGRESSION_DATABASE_NAME.into()),
            Value::Text(constraint_schema.into()),
            Value::Text(constraint.conname.into()),
            Value::Text(REGRESSION_DATABASE_NAME.into()),
            Value::Text(domain_schema.into()),
            Value::Text(domain_row.typname.into()),
            yes_or_no(constraint.condeferrable),
            yes_or_no(constraint.condeferred),
        ]);
    }
    rows.sort_by(|left, right| value_text(left.get(2)).cmp(value_text(right.get(2))));
    rows
}

fn information_schema_domain_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for domain_row in catalog
        .type_rows()
        .into_iter()
        .filter(|row| row.typtype == 'd')
    {
        let Some(domain_schema) = catalog
            .namespace_row_by_oid(domain_row.typnamespace)
            .map(|row| row.nspname)
        else {
            continue;
        };
        let base_row = information_schema_domain_base_type_row(catalog, domain_row.typbasetype);
        let base_sql_type = base_row
            .as_ref()
            .map(|row| row.sql_type)
            .unwrap_or(domain_row.sql_type);
        let (character_maximum_length, character_octet_length) =
            information_schema_character_metadata(base_sql_type);
        let (numeric_precision, numeric_precision_radix, numeric_scale) =
            information_schema_numeric_metadata(base_sql_type);
        let (udt_schema, udt_name) = base_row
            .and_then(|row| {
                let namespace = catalog.namespace_row_by_oid(row.typnamespace)?;
                Some((namespace.nspname, row.typname))
            })
            .unwrap_or_else(|| ("pg_catalog".into(), sql_type_name(base_sql_type)));
        let domain_default = catalog
            .domain_by_type_oid(domain_row.oid)
            .and_then(|domain| domain.default)
            .or_else(|| catalog.type_default_sql(domain_row.oid));
        rows.push(vec![
            Value::Text(REGRESSION_DATABASE_NAME.into()),
            Value::Text(domain_schema.into()),
            Value::Text(domain_row.typname.into()),
            Value::Text(information_schema_data_type(base_sql_type, catalog).into()),
            character_maximum_length,
            character_octet_length,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            numeric_precision,
            numeric_precision_radix,
            numeric_scale,
            Value::Null,
            Value::Null,
            Value::Null,
            domain_default
                .map(|value| Value::Text(value.into()))
                .unwrap_or(Value::Null),
            Value::Text(REGRESSION_DATABASE_NAME.into()),
            Value::Text(udt_schema.into()),
            Value::Text(udt_name.into()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Text("1".into()),
        ]);
    }
    rows.sort_by(|left, right| value_text(left.get(2)).cmp(value_text(right.get(2))));
    rows
}

fn information_schema_check_constraint_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for constraint in catalog.constraint_rows() {
        if constraint.contypid != 0 {
            if !matches!(constraint.contype, CONSTRAINT_CHECK | CONSTRAINT_NOTNULL) {
                continue;
            }
            let Some(domain_row) = catalog.type_by_oid(constraint.contypid) else {
                continue;
            };
            if domain_row.typtype != 'd' {
                continue;
            }
            let Some(constraint_schema) = catalog
                .namespace_row_by_oid(constraint.connamespace)
                .map(|row| row.nspname)
            else {
                continue;
            };
            rows.push(vec![
                Value::Text(REGRESSION_DATABASE_NAME.into()),
                Value::Text(constraint_schema.into()),
                Value::Text(constraint.conname.clone().into()),
                Value::Text(information_schema_domain_check_clause(&constraint).into()),
            ]);
            continue;
        }
        if constraint.contype != CONSTRAINT_CHECK {
            continue;
        }
        let Some(constraint_schema) = catalog
            .namespace_row_by_oid(constraint.connamespace)
            .map(|row| row.nspname)
        else {
            continue;
        };
        let Some(check_clause) = information_schema_relation_check_clause(catalog, &constraint)
        else {
            continue;
        };
        rows.push(vec![
            Value::Text(REGRESSION_DATABASE_NAME.into()),
            Value::Text(constraint_schema.into()),
            Value::Text(constraint.conname.into()),
            Value::Text(check_clause.into()),
        ]);
    }
    rows.sort_by(|left, right| {
        value_text(left.get(1))
            .cmp(value_text(right.get(1)))
            .then_with(|| value_text(left.get(2)).cmp(value_text(right.get(2))))
    });
    rows
}

fn information_schema_domain_base_type_row(
    catalog: &dyn CatalogLookup,
    type_oid: u32,
) -> Option<crate::include::catalog::PgTypeRow> {
    let mut current_oid = type_oid;
    for _ in 0..64 {
        let row = catalog.type_by_oid(current_oid)?;
        if row.typtype != 'd' || row.typbasetype == 0 {
            return Some(row);
        }
        current_oid = row.typbasetype;
    }
    None
}

fn information_schema_character_metadata(sql_type: SqlType) -> (Value, Value) {
    match sql_type.kind {
        SqlTypeKind::Varchar | SqlTypeKind::Char => match sql_type.char_len() {
            Some(length) => (Value::Int32(length), Value::Int32(length.saturating_mul(4))),
            None => (Value::Null, Value::Null),
        },
        _ => (Value::Null, Value::Null),
    }
}

fn information_schema_numeric_metadata(sql_type: SqlType) -> (Value, Value, Value) {
    match sql_type.kind {
        SqlTypeKind::Int2 => (Value::Int32(16), Value::Int32(2), Value::Int32(0)),
        SqlTypeKind::Int4 | SqlTypeKind::Oid => {
            (Value::Int32(32), Value::Int32(2), Value::Int32(0))
        }
        SqlTypeKind::Int8 => (Value::Int32(64), Value::Int32(2), Value::Int32(0)),
        SqlTypeKind::Float4 => (Value::Int32(24), Value::Int32(2), Value::Null),
        SqlTypeKind::Float8 => (Value::Int32(53), Value::Int32(2), Value::Null),
        SqlTypeKind::Numeric => match sql_type.numeric_precision_scale() {
            Some((precision, scale)) => (
                Value::Int32(precision),
                Value::Int32(10),
                Value::Int32(scale),
            ),
            None => (Value::Null, Value::Int32(10), Value::Null),
        },
        _ => (Value::Null, Value::Null, Value::Null),
    }
}

fn information_schema_relation_check_clause(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let raw = row.conbin.as_deref()?;
    let rendered = canonicalize_check_clause_sql(catalog, row).unwrap_or_else(|| raw.to_string());
    let trimmed = rendered.trim();
    if trimmed.starts_with("JSON_EXISTS(") || trimmed.starts_with('(') {
        Some(trimmed.to_string())
    } else {
        Some(format!("({trimmed})"))
    }
}

fn canonicalize_check_clause_sql(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let expr_sql = row.conbin.as_deref()?;
    if !contains_sql_json_query_function(expr_sql) {
        return None;
    }
    let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
    let relation_name = catalog
        .class_row_by_oid(row.conrelid)
        .map(|row| row.relname);
    let bound =
        bind_relation_expr(expr_sql, relation_name.as_deref(), &relation.desc, catalog).ok()?;
    Some(render_relation_expr_sql_for_information_schema(
        &bound,
        relation_name.as_deref(),
        &relation.desc,
        catalog,
    ))
}

fn contains_sql_json_query_function(expr_sql: &str) -> bool {
    let upper = expr_sql.to_ascii_uppercase();
    upper.contains("JSON_QUERY(") || upper.contains("JSON_VALUE(") || upper.contains("JSON_EXISTS(")
}

fn information_schema_domain_check_clause(
    constraint: &crate::include::catalog::PgConstraintRow,
) -> String {
    if constraint.contype == CONSTRAINT_NOTNULL {
        return "VALUE IS NOT NULL".into();
    }
    let expr = constraint.conbin.as_deref().unwrap_or_default().trim();
    if expr.starts_with('(') {
        information_schema_domain_value_name(expr)
    } else {
        format!("({})", information_schema_domain_value_name(expr))
    }
}

fn information_schema_domain_value_name(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut token = String::new();
    for ch in expr.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch);
            continue;
        }
        if !token.is_empty() {
            if token.eq_ignore_ascii_case("value") {
                out.push_str("VALUE");
            } else {
                out.push_str(&token);
            }
            token.clear();
        }
        out.push(ch);
    }
    if !token.is_empty() {
        if token.eq_ignore_ascii_case("value") {
            out.push_str("VALUE");
        } else {
            out.push_str(&token);
        }
    }
    out
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
    if let Some(relation) = catalog.relation_by_oid(relation_oid)
        && let Ok(definition) =
            crate::backend::rewrite::format_view_definition(relation_oid, &relation.desc, catalog)
    {
        let sql = catalog
            .rewrite_rows_for_relation(relation_oid)
            .into_iter()
            .find(|row| row.rulename == "_RETURN")
            .map(|row| row.ev_action)
            .unwrap_or_default();
        let (_, check_option) = crate::backend::rewrite::split_stored_view_definition_sql(&sql);
        return (
            definition,
            match check_option {
                crate::include::nodes::parsenodes::ViewCheckOption::None => "NONE",
                crate::include::nodes::parsenodes::ViewCheckOption::Local => "LOCAL",
                crate::include::nodes::parsenodes::ViewCheckOption::Cascaded => "CASCADED",
            },
        );
    }
    let sql = catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.rulename == "_RETURN")
        .map(|row| row.ev_action)
        .unwrap_or_default();
    let (definition, check_option) =
        crate::backend::rewrite::split_stored_view_definition_sql(&sql);
    (
        normalize_stored_view_definition_for_information_schema(definition),
        match check_option {
            crate::include::nodes::parsenodes::ViewCheckOption::None => "NONE",
            crate::include::nodes::parsenodes::ViewCheckOption::Local => "LOCAL",
            crate::include::nodes::parsenodes::ViewCheckOption::Cascaded => "CASCADED",
        },
    )
}

fn normalize_stored_view_definition_for_information_schema(definition: &str) -> String {
    let trimmed = definition.trim();
    let lower = trimmed.to_ascii_lowercase();
    let mut rendered = if lower.starts_with("select ") {
        format!(" SELECT {}", &trimmed["select ".len()..])
    } else {
        trimmed.to_string()
    };
    rendered = replace_keyword_outside_single_quotes(&rendered, " as ", " AS ");
    if !rendered.ends_with(';') {
        rendered.push(';');
    }
    rendered
}

fn replace_keyword_outside_single_quotes(sql: &str, needle: &str, replacement: &str) -> String {
    let mut rendered = String::with_capacity(sql.len());
    let mut index = 0usize;
    let mut in_string = false;
    while index < sql.len() {
        let rest = &sql[index..];
        if rest.starts_with('\'') {
            rendered.push('\'');
            index += 1;
            if in_string && sql[index..].starts_with('\'') {
                rendered.push('\'');
                index += 1;
            } else {
                in_string = !in_string;
            }
            continue;
        }
        if !in_string && rest.to_ascii_lowercase().starts_with(needle) {
            rendered.push_str(replacement);
            index += needle.len();
            continue;
        }
        let ch = rest.chars().next().expect("nonempty SQL slice");
        rendered.push(ch);
        index += ch.len_utf8();
    }
    rendered
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
        || query.limit_offset.is_some()
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
        Expr::GroupingKey(grouping_key) => expr_contains_sublink(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_contains_sublink),
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
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}
