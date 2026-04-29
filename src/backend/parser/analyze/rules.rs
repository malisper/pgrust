use super::*;
use crate::include::nodes::primnodes::{INNER_VAR, OUTER_VAR, Var, user_attrno};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BoundRuleAction {
    Insert(BoundInsertStatement),
    Update(BoundUpdateStatement),
    Delete(BoundDeleteStatement),
    Select(PlannedStmt),
    Notify(NotifyStatement),
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
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            })
            .collect(),
        relations: relation_name
            .map(|name| {
                vec![ScopeRelation {
                    relation_names: vec![name.to_string()],
                    hidden_invalid_relation_names: vec![],
                    hidden_missing_relation_names: vec![],
                    system_varno: Some(varno),
                    relation_oid: None,
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
        Statement::Select(stmt) => Ok(BoundRuleAction::Select(
            pg_plan_query_with_outer_scopes_and_ctes(stmt, catalog, &outer_scopes, &[])?,
        )),
        Statement::Notify(stmt) => Ok(BoundRuleAction::Notify(stmt.clone())),
        _ => Err(ParseError::FeatureNotSupported(
            "rule action statement".into(),
        )),
    }
}

fn rule_action_returning_targets(action: &BoundRuleAction) -> &[TargetEntry] {
    match action {
        BoundRuleAction::Insert(stmt) => &stmt.returning,
        BoundRuleAction::Update(stmt) => &stmt.returning,
        BoundRuleAction::Delete(stmt) => &stmt.returning,
        BoundRuleAction::Select(_) | BoundRuleAction::Notify(_) => &[],
    }
}

fn validate_rule_action_returning_targets(
    targets: &[TargetEntry],
    relation_desc: &RelationDesc,
) -> Result<(), ParseError> {
    if targets.len() > relation_desc.columns.len() {
        return Err(ParseError::FeatureNotSupported(
            "RETURNING list has too many entries".into(),
        ));
    }
    if targets.len() < relation_desc.columns.len() {
        return Err(ParseError::FeatureNotSupported(
            "RETURNING list has too few entries".into(),
        ));
    }

    for (index, (target, column)) in targets.iter().zip(&relation_desc.columns).enumerate() {
        if target.sql_type != column.sql_type {
            return Err(ParseError::FeatureNotSupported(format!(
                "RETURNING list's entry {} has different type from column \"{}\"",
                index + 1,
                column.name
            )));
        }
    }

    Ok(())
}

pub(crate) fn validate_rule_definition(
    stmt: &CreateRuleStatement,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if let Some(expr) = &stmt.where_clause {
        let _ = bind_rule_qual(expr, relation_desc, stmt.event, catalog)?;
    }

    let mut returning_count = 0usize;
    for action in &stmt.actions {
        if !matches!(
            action.statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        ) {
            continue;
        }
        let bound = bind_rule_action_statement(&action.statement, relation_desc, catalog)?;
        let returning = rule_action_returning_targets(&bound);
        if returning.is_empty() {
            continue;
        }

        returning_count += 1;
        if returning_count > 1 {
            return Err(ParseError::FeatureNotSupported(
                "cannot have multiple RETURNING lists in a rule".into(),
            ));
        }
        if stmt.where_clause.is_some() {
            return Err(ParseError::FeatureNotSupported(
                "RETURNING lists are not supported in conditional rules".into(),
            ));
        }
        if stmt.do_kind != RuleDoKind::Instead {
            return Err(ParseError::FeatureNotSupported(
                "RETURNING lists are not supported in non-INSTEAD rules".into(),
            ));
        }
        validate_rule_action_returning_targets(returning, relation_desc)?;
    }
    Ok(())
}
