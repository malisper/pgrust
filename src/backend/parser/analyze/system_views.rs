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

pub(super) fn bind_builtin_system_view(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Option<(AnalyzedFrom, BoundScope)> {
    if is_pg_views_name(name) {
        let output_columns = vec![
            QueryColumn::text("schemaname"),
            QueryColumn::text("viewname"),
            QueryColumn::text("viewowner"),
            QueryColumn::text("definition"),
        ];
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                .collect(),
        };
        let rows = catalog
            .pg_views_rows()
            .into_iter()
            .map(|row| row.into_iter().map(Expr::Const).collect())
            .collect();

        return Some((
            AnalyzedFrom::values(rows, output_columns),
            scope_for_relation(Some(name), &desc),
        ));
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
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                .collect(),
        };
        let rows = catalog
            .pg_stat_activity_rows()
            .into_iter()
            .map(|row| row.into_iter().map(Expr::Const).collect())
            .collect();

        return Some((
            AnalyzedFrom::values(rows, output_columns),
            scope_for_relation(Some(name), &desc),
        ));
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
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let rows = catalog
        .pg_stats_rows()
        .into_iter()
        .map(|row| row.into_iter().map(Expr::Const).collect())
        .collect();

    Some((
        AnalyzedFrom::values(rows, output_columns),
        scope_for_relation(Some(name), &desc),
    ))
}
