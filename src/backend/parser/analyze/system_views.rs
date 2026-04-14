use super::*;

fn is_pg_views_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("pg_views") || name.eq_ignore_ascii_case("pg_catalog.pg_views")
}

pub(super) fn bind_builtin_system_view(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Option<(Plan, BoundScope)> {
    if !is_pg_views_name(name) {
        return None;
    }

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

    Some((
        Plan::Values {
            rows,
            output_columns,
        },
        scope_for_relation(Some(name), &desc),
    ))
}
