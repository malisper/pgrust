use super::*;
use crate::include::nodes::primnodes::{INNER_VAR, OUTER_VAR, Var, user_attrno};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BoundRuleAction {
    Insert(BoundInsertStatement),
    Update(BoundUpdateStatement),
    Delete(BoundDeleteStatement),
}

fn scope_for_special_rule_tuple(
    relation_name: Option<&str>,
    desc: &RelationDesc,
    varno: usize,
    qualified_only: bool,
) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        output_exprs: desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                })
            })
            .collect(),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                qualified_only,
                relation_names: relation_name.into_iter().map(str::to_string).collect(),
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
            })
            .collect(),
        relations: relation_name
            .map(|name| {
                vec![ScopeRelation {
                    relation_names: vec![name.to_string()],
                    hidden_invalid_relation_names: vec![],
                    hidden_missing_relation_names: vec![],
                    system_varno: Some(varno),
                }]
            })
            .unwrap_or_default(),
    }
}

pub(crate) fn bind_rule_qual(
    expr: &SqlExpr,
    relation_desc: &RelationDesc,
    event: RuleEvent,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    let default_varno = match event {
        RuleEvent::Delete => OUTER_VAR,
        RuleEvent::Insert | RuleEvent::Update | RuleEvent::Select => INNER_VAR,
    };
    let local_scope = scope_for_special_rule_tuple(None, relation_desc, default_varno, false);
    let outer_scopes = vec![
        scope_for_special_rule_tuple(Some("old"), relation_desc, OUTER_VAR, true),
        scope_for_special_rule_tuple(Some("new"), relation_desc, INNER_VAR, true),
    ];
    bind_expr_with_outer_and_ctes(expr, &local_scope, catalog, &outer_scopes, None, &[])
}

pub(crate) fn bind_rule_action_statement(
    statement: &Statement,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<BoundRuleAction, ParseError> {
    let outer_scopes = vec![
        scope_for_special_rule_tuple(Some("old"), relation_desc, OUTER_VAR, true),
        scope_for_special_rule_tuple(Some("new"), relation_desc, INNER_VAR, true),
    ];
    match statement {
        Statement::Insert(stmt) => Ok(BoundRuleAction::Insert(bind_insert_with_outer_scopes(
            stmt,
            catalog,
            &outer_scopes,
        )?)),
        Statement::Update(stmt) => Ok(BoundRuleAction::Update(bind_update_with_outer_scopes(
            stmt,
            catalog,
            &outer_scopes,
        )?)),
        Statement::Delete(stmt) => Ok(BoundRuleAction::Delete(bind_delete_with_outer_scopes(
            stmt,
            catalog,
            &outer_scopes,
        )?)),
        _ => Err(ParseError::FeatureNotSupported(
            "rule action statement".into(),
        )),
    }
}

pub(crate) fn validate_rule_definition(
    stmt: &CreateRuleStatement,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if let Some(expr) = &stmt.where_clause {
        let _ = bind_rule_qual(expr, relation_desc, stmt.event, catalog)?;
    }
    for action in &stmt.actions {
        let _ = bind_rule_action_statement(&action.statement, relation_desc, catalog)?;
    }
    Ok(())
}
