use super::*;

pub(crate) fn bind_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<TargetEntry>, ParseError> {
    let mut entries = Vec::new();
    for item in targets {
        if let SqlExpr::Column(name) = &item.expr {
            if name == "*" {
                entries.extend(expand_star_targets(scope, None)?);
                continue;
            }
            if let Some(relation) = name.strip_suffix(".*") {
                entries.extend(expand_star_targets(scope, Some(relation))?);
                continue;
            }
        }

        entries.push(TargetEntry {
            name: item.output_name.clone(),
            expr: bind_expr_with_outer_and_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            sql_type: infer_sql_expr_type_with_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        });
    }
    Ok(entries)
}

fn expand_star_targets(
    scope: &BoundScope,
    relation: Option<&str>,
) -> Result<Vec<TargetEntry>, ParseError> {
    let entries = scope
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            relation.is_none_or(|relation_name| {
                column
                    .relation_name
                    .as_deref()
                    .is_some_and(|visible| visible.eq_ignore_ascii_case(relation_name))
            })
        })
        .map(|(index, column)| TargetEntry {
            name: column.output_name.clone(),
            expr: Expr::Column(index),
            sql_type: scope.desc.columns[index].sql_type,
        })
        .collect::<Vec<_>>();

    if entries.is_empty() {
        return Err(ParseError::UnknownColumn(
            relation
                .map(|name| format!("{name}.*"))
                .unwrap_or_else(|| "*".to_string()),
        ));
    }
    Ok(entries)
}
