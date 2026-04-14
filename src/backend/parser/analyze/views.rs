use super::*;
use super::query::AnalyzedFrom;

const RETURN_RULE_NAME: &str = "_RETURN";

fn view_display_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn return_rule_sql(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    display_name: &str,
) -> Result<String, ParseError> {
    let mut rows = catalog.rewrite_rows_for_relation(relation.relation_oid);
    rows.retain(|row| row.rulename == RETURN_RULE_NAME);
    match rows.as_slice() {
        [row] => Ok(row.ev_action.clone()),
        [] => Err(ParseError::UnexpectedToken {
            expected: "view _RETURN rule",
            actual: format!("missing rewrite rule for view {display_name}"),
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "single view _RETURN rule",
            actual: format!("multiple rewrite rules for view {display_name}"),
        }),
    }
}

fn validate_view_shape(
    plan: &Query,
    relation: &BoundRelation,
    display_name: &str,
) -> Result<(), ParseError> {
    let actual_columns = plan.columns();
    if actual_columns.len() != relation.desc.columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "view query width matching stored view columns",
            actual: format!("stale view definition for {display_name}"),
        });
    }
    for (actual_column, stored_column) in
        actual_columns.into_iter().zip(relation.desc.columns.iter())
    {
        if !actual_column.name.eq_ignore_ascii_case(&stored_column.name)
            || actual_column.sql_type != stored_column.sql_type
        {
            return Err(ParseError::UnexpectedToken {
                expected: "view query columns matching stored view descriptor",
                actual: format!("stale view definition for {display_name}"),
            });
        }
    }
    Ok(())
}

pub(super) fn bind_view_reference(
    relation_name: &str,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let display_name = view_display_name(relation_name);
    if expanded_views.contains(&relation.relation_oid) {
        return Err(ParseError::RecursiveView(display_name));
    }
    let sql = return_rule_sql(catalog, relation, &display_name)?;
    let stmt = crate::backend::parser::parse_statement(&sql)?;
    let Statement::Select(select) = stmt else {
        return Err(ParseError::UnexpectedToken {
            expected: "SELECT view definition",
            actual: sql,
        });
    };
    let mut next_views = expanded_views.to_vec();
    next_views.push(relation.relation_oid);
    let (plan, _) = analyze_select_query_with_outer(
        &select,
        catalog,
        outer_scopes,
        grouped_outer.cloned(),
        ctes,
        &next_views,
    )?;
    validate_view_shape(&plan, relation, &display_name)?;
    Ok((
        AnalyzedFrom::subquery(plan),
        scope_for_relation(Some(relation_name), &relation.desc),
    ))
}
