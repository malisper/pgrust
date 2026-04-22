use super::query::AnalyzedFrom;
use super::*;
use crate::backend::rewrite::{ViewDmlEvent, load_view_return_query, load_view_return_select};
use crate::include::nodes::parsenodes::{JoinTreeNode, RangeTblEntryKind};
use crate::include::nodes::primnodes::{attrno_index, is_system_attr};

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

fn is_pg_views_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_views") || name.eq_ignore_ascii_case("pg_catalog.pg_views")
}

fn is_pg_rules_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_rules") || name.eq_ignore_ascii_case("pg_catalog.pg_rules")
}

fn is_pg_stats_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stats") || name.eq_ignore_ascii_case("pg_catalog.pg_stats")
}

fn is_pg_stat_activity_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stat_activity")
        || name.eq_ignore_ascii_case("pg_catalog.pg_stat_activity")
}

fn is_pg_stat_user_tables_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stat_user_tables")
        || name.eq_ignore_ascii_case("pg_catalog.pg_stat_user_tables")
}

fn is_pg_statio_user_tables_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_statio_user_tables")
        || name.eq_ignore_ascii_case("pg_catalog.pg_statio_user_tables")
}

fn is_pg_stat_user_functions_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stat_user_functions")
        || name.eq_ignore_ascii_case("pg_catalog.pg_stat_user_functions")
}

fn is_pg_stat_io_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stat_io") || name.eq_ignore_ascii_case("pg_catalog.pg_stat_io")
}

fn is_information_schema_tables_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("information_schema.tables")
}

fn is_information_schema_views_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("information_schema.views")
}

fn is_information_schema_columns_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("information_schema.columns")
}

fn is_pg_locks_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_locks") || name.eq_ignore_ascii_case("pg_catalog.pg_locks")
}

pub(super) fn bind_builtin_system_view(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Option<(AnalyzedFrom, BoundScope)> {
    let build_values_view =
        |name: &str, output_columns: Vec<QueryColumn>, rows: Vec<Vec<Value>>| -> Option<_> {
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
        };

    if is_pg_views_name(name) {
        let output_columns = vec![
            QueryColumn::text("schemaname"),
            QueryColumn::text("viewname"),
            QueryColumn::text("viewowner"),
            QueryColumn::text("definition"),
        ];
        return build_values_view(name, output_columns, catalog.pg_views_rows());
    }

    if is_pg_rules_name(name) {
        let output_columns = vec![
            QueryColumn::text("schemaname"),
            QueryColumn::text("tablename"),
            QueryColumn::text("rulename"),
            QueryColumn::text("definition"),
        ];
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                .collect(),
        };
        let rows = catalog
            .pg_rules_rows()
            .into_iter()
            .map(|row| row.into_iter().map(Expr::Const).collect())
            .collect();

        return Some((
            AnalyzedFrom::values(rows, output_columns),
            scope_for_relation(Some(name), &desc),
        ));
    }

    if is_information_schema_tables_name(name) {
        let output_columns = vec![
            QueryColumn::text("table_name"),
            QueryColumn::text("is_insertable_into"),
        ];
        return build_values_view(name, output_columns, information_schema_table_rows(catalog));
    }

    if is_information_schema_views_name(name) {
        let output_columns = vec![
            QueryColumn::text("table_catalog"),
            QueryColumn::text("table_schema"),
            QueryColumn::text("table_name"),
            QueryColumn::text("view_definition"),
            QueryColumn::text("check_option"),
            QueryColumn::text("is_updatable"),
            QueryColumn::text("is_insertable_into"),
            QueryColumn::text("is_trigger_updatable"),
            QueryColumn::text("is_trigger_deletable"),
            QueryColumn::text("is_trigger_insertable_into"),
        ];
        return build_values_view(name, output_columns, information_schema_view_rows(catalog));
    }

    if is_information_schema_columns_name(name) {
        let output_columns = vec![
            QueryColumn::text("table_name"),
            QueryColumn::text("column_name"),
            QueryColumn {
                name: "ordinal_position".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn::text("is_updatable"),
        ];
        return build_values_view(
            name,
            output_columns,
            information_schema_column_rows(catalog),
        );
    }

    if is_pg_stat_activity_name(name) {
        let output_columns = vec![
            QueryColumn {
                name: "pid".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn::text("datname"),
            QueryColumn::text("usename"),
            QueryColumn::text("state"),
            QueryColumn::text("query"),
        ];
        return build_values_view(name, output_columns, catalog.pg_stat_activity_rows());
    }

    if is_pg_locks_name(name) {
        let output_columns = vec![
            QueryColumn::text("locktype"),
            QueryColumn {
                name: "database".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relation".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "page".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "tuple".into(),
                sql_type: SqlType::new(SqlTypeKind::Int2),
                wire_type_oid: None,
            },
            QueryColumn::text("virtualxid"),
            QueryColumn {
                name: "transactionid".into(),
                sql_type: SqlType::new(SqlTypeKind::Xid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "classid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "objid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "objsubid".into(),
                sql_type: SqlType::new(SqlTypeKind::Int2),
                wire_type_oid: None,
            },
            QueryColumn::text("virtualtransaction"),
            QueryColumn {
                name: "pid".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn::text("mode"),
            QueryColumn {
                name: "granted".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "fastpath".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "waitstart".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
        ];
        return build_values_view(name, output_columns, catalog.pg_locks_rows());
    }

    if is_pg_stat_user_tables_name(name) {
        let output_columns = vec![
            QueryColumn {
                name: "relid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn::text("schemaname"),
            QueryColumn::text("relname"),
            QueryColumn {
                name: "seq_scan".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_seq_scan".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "seq_tup_read".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "idx_scan".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_idx_scan".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "idx_tup_fetch".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_tup_ins".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_tup_upd".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_tup_del".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_tup_hot_upd".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_tup_newpage_upd".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_live_tup".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_dead_tup".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_mod_since_analyze".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "n_ins_since_vacuum".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_vacuum".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_autovacuum".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_analyze".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "last_autoanalyze".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "vacuum_count".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "autovacuum_count".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "analyze_count".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "autoanalyze_count".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "total_vacuum_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "total_autovacuum_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "total_analyze_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "total_autoanalyze_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
        ];
        return build_values_view(name, output_columns, catalog.pg_stat_user_tables_rows());
    }

    if is_pg_statio_user_tables_name(name) {
        let output_columns = vec![
            QueryColumn {
                name: "relid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn::text("schemaname"),
            QueryColumn::text("relname"),
            QueryColumn {
                name: "heap_blks_read".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "heap_blks_hit".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "idx_blks_read".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "idx_blks_hit".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "toast_blks_read".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "toast_blks_hit".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "tidx_blks_read".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "tidx_blks_hit".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
        ];
        return build_values_view(name, output_columns, catalog.pg_statio_user_tables_rows());
    }

    if is_pg_stat_user_functions_name(name) {
        let output_columns = vec![
            QueryColumn {
                name: "funcid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn::text("schemaname"),
            QueryColumn::text("funcname"),
            QueryColumn {
                name: "calls".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "total_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "self_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
        ];
        return build_values_view(name, output_columns, catalog.pg_stat_user_functions_rows());
    }

    if is_pg_stat_io_name(name) {
        let output_columns = vec![
            QueryColumn::text("backend_type"),
            QueryColumn::text("object"),
            QueryColumn::text("context"),
            QueryColumn {
                name: "reads".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "read_bytes".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "read_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "writes".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "write_bytes".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "write_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "writebacks".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "writeback_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "extends".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "extend_bytes".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "extend_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "hits".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "evictions".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "reuses".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "fsyncs".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "fsync_time".into(),
                sql_type: SqlType::new(SqlTypeKind::Float8),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "stats_reset".into(),
                sql_type: SqlType::new(SqlTypeKind::TimestampTz),
                wire_type_oid: None,
            },
        ];
        return build_values_view(name, output_columns, catalog.pg_stat_io_rows());
    }

    if !is_pg_stats_name(name) {
        return None;
    }

    let output_columns = vec![
        QueryColumn::text("schemaname"),
        QueryColumn::text("tablename"),
        QueryColumn::text("attname"),
        QueryColumn {
            name: "inherited".into(),
            sql_type: SqlType::new(SqlTypeKind::Bool),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "null_frac".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "avg_width".into(),
            sql_type: SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "n_distinct".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "most_common_vals".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "most_common_freqs".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "histogram_bounds".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "correlation".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "most_common_elems".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "most_common_elem_freqs".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "elem_count_histogram".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "range_length_histogram".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "range_empty_frac".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "range_bounds_histogram".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
            wire_type_oid: None,
        },
    ];
    build_values_view(name, output_columns, catalog.pg_stats_rows())
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

fn information_schema_column_rows(catalog: &dyn CatalogLookup) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for view in information_schema_view_metadata(catalog) {
        for (index, column) in view.relation_desc.columns.iter().enumerate() {
            let updatable = view
                .updatability
                .columns
                .get(index)
                .is_some_and(|entry| entry.insertable || entry.updatable);
            rows.push(vec![
                Value::Text(view.table_name.clone().into()),
                Value::Text(column.name.clone().into()),
                Value::Int32((index + 1) as i32),
                yes_or_no(updatable),
            ]);
        }
    }
    rows
}

fn information_schema_view_metadata(catalog: &dyn CatalogLookup) -> Vec<ViewMetadataRow> {
    let Some(visible) = catalog.materialize_visible_catalog() else {
        return Vec::new();
    };

    let mut seen_relation_oids = std::collections::BTreeSet::new();
    let mut rows = visible
        .relcache()
        .entries()
        .filter_map(|(name, entry)| {
            if entry.relkind != 'v' || !seen_relation_oids.insert(entry.relation_oid) {
                return None;
            }

            let (schema_name, table_name) = split_qualified_relation_name(name);
            if schema_name.eq_ignore_ascii_case("pg_catalog")
                || schema_name.eq_ignore_ascii_case(INFO_SCHEMA_NAME)
            {
                return None;
            }

            let (view_definition, check_option) = view_definition_and_check_option(catalog, entry);
            Some(ViewMetadataRow {
                schema_name,
                table_name,
                relation_oid: entry.relation_oid,
                relation_desc: entry.desc.clone(),
                view_definition,
                check_option,
                updatability: describe_view_updatability(entry.relation_oid, &entry.desc, catalog),
            })
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

fn split_qualified_relation_name(name: &str) -> (String, String) {
    match name.split_once('.') {
        Some((schema, relation_name)) => (schema.to_string(), relation_name.to_string()),
        None => ("public".to_string(), name.to_string()),
    }
}

fn view_definition_and_check_option(
    catalog: &dyn CatalogLookup,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
) -> (String, &'static str) {
    let sql = catalog
        .rewrite_rows_for_relation(entry.relation_oid)
        .into_iter()
        .find(|row| row.rulename == "_RETURN")
        .map(|row| row.ev_action)
        .unwrap_or_default();
    let (definition, check_option) = crate::backend::rewrite::split_stored_view_definition_sql(&sql);
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
        || query.project_set.is_some()
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
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}
