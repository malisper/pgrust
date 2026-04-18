use super::query::AnalyzedFrom;
use super::*;

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
