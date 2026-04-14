use super::query::AnalyzedFrom;
use super::*;

fn is_pg_views_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_views") || name.eq_ignore_ascii_case("pg_catalog.pg_views")
}

fn is_pg_stats_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_stats") || name.eq_ignore_ascii_case("pg_catalog.pg_stats")
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
        },
        QueryColumn {
            name: "null_frac".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
        },
        QueryColumn {
            name: "avg_width".into(),
            sql_type: SqlType::new(SqlTypeKind::Int4),
        },
        QueryColumn {
            name: "n_distinct".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
        },
        QueryColumn {
            name: "most_common_vals".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
        },
        QueryColumn {
            name: "most_common_freqs".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
        },
        QueryColumn {
            name: "histogram_bounds".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
        },
        QueryColumn {
            name: "correlation".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
        },
        QueryColumn {
            name: "most_common_elems".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
        },
        QueryColumn {
            name: "most_common_elem_freqs".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
        },
        QueryColumn {
            name: "elem_count_histogram".into(),
            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
        },
        QueryColumn {
            name: "range_length_histogram".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
        },
        QueryColumn {
            name: "range_empty_frac".into(),
            sql_type: SqlType::new(SqlTypeKind::Float4),
        },
        QueryColumn {
            name: "range_bounds_histogram".into(),
            sql_type: SqlType::new(SqlTypeKind::AnyArray),
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
