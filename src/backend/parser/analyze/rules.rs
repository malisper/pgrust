use super::*;
use crate::include::nodes::primnodes::{INNER_VAR, OUTER_VAR, Var, user_attrno};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BoundRuleAction {
    Insert(BoundInsertStatement),
    Update(BoundUpdateStatement),
    Delete(BoundDeleteStatement),
    Select(PlannedStmt),
    Notify(NotifyStatement),
    Sequence(Vec<BoundRuleAction>),
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
    reject_old_new_in_rule_action_ctes(statement)?;
    let outer_scopes = vec![
        scope_for_special_rule_tuple(Some("old"), relation_desc, OUTER_VAR, true),
        scope_for_special_rule_tuple(Some("new"), relation_desc, INNER_VAR, true),
    ];
    if statement_has_modifying_ctes(statement) {
        return bind_rule_action_statement_with_modifying_ctes(statement, catalog, &outer_scopes);
    }
    bind_rule_action_statement_inner(statement, catalog, &outer_scopes)
}

fn bind_rule_action_statement_inner(
    statement: &Statement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundRuleAction, ParseError> {
    match statement {
        Statement::Insert(stmt) => Ok(BoundRuleAction::Insert(bind_insert_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        Statement::Update(stmt) => Ok(BoundRuleAction::Update(bind_update_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        Statement::Delete(stmt) => Ok(BoundRuleAction::Delete(bind_delete_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        Statement::Select(stmt) => Ok(BoundRuleAction::Select(
            pg_plan_query_with_outer_scopes_and_ctes(stmt, catalog, outer_scopes, &[])?,
        )),
        Statement::Notify(stmt) => Ok(BoundRuleAction::Notify(stmt.clone())),
        _ => Err(ParseError::FeatureNotSupported(
            "rule action statement".into(),
        )),
    }
}

fn bind_rule_action_statement_with_modifying_ctes(
    statement: &Statement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundRuleAction, ParseError> {
    let mut actions = Vec::new();
    let main_statement = match statement {
        Statement::Insert(stmt) => {
            let mut main = stmt.clone();
            main.with.clear();
            for cte in &stmt.with {
                if super::cte_body_is_modifying(&cte.body) {
                    actions.push(bind_rule_modifying_cte_body(
                        &cte.body,
                        catalog,
                        outer_scopes,
                    )?);
                } else {
                    main.with.push(cte.clone());
                }
            }
            Statement::Insert(main)
        }
        Statement::Update(stmt) => {
            let mut main = stmt.clone();
            main.with.clear();
            for cte in &stmt.with {
                if super::cte_body_is_modifying(&cte.body) {
                    actions.push(bind_rule_modifying_cte_body(
                        &cte.body,
                        catalog,
                        outer_scopes,
                    )?);
                } else {
                    main.with.push(cte.clone());
                }
            }
            Statement::Update(main)
        }
        Statement::Delete(stmt) => {
            let mut main = stmt.clone();
            main.with.clear();
            for cte in &stmt.with {
                if super::cte_body_is_modifying(&cte.body) {
                    actions.push(bind_rule_modifying_cte_body(
                        &cte.body,
                        catalog,
                        outer_scopes,
                    )?);
                } else {
                    main.with.push(cte.clone());
                }
            }
            Statement::Delete(main)
        }
        Statement::Select(stmt) => {
            let mut main = stmt.clone();
            main.with.clear();
            for cte in &stmt.with {
                if super::cte_body_is_modifying(&cte.body) {
                    actions.push(bind_rule_modifying_cte_body(
                        &cte.body,
                        catalog,
                        outer_scopes,
                    )?);
                } else {
                    main.with.push(cte.clone());
                }
            }
            Statement::Select(main)
        }
        _ => {
            return Err(ParseError::FeatureNotSupported(
                "rule action statement with data-modifying WITH".into(),
            ));
        }
    };
    actions.push(bind_rule_action_statement_inner(
        &main_statement,
        catalog,
        outer_scopes,
    )?);
    Ok(BoundRuleAction::Sequence(actions))
}

fn bind_rule_modifying_cte_body(
    body: &CteBody,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundRuleAction, ParseError> {
    match body {
        CteBody::Insert(stmt) => Ok(BoundRuleAction::Insert(bind_insert_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        CteBody::Update(stmt) => Ok(BoundRuleAction::Update(bind_update_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        CteBody::Delete(stmt) => Ok(BoundRuleAction::Delete(bind_delete_with_outer_scopes(
            stmt,
            catalog,
            outer_scopes,
        )?)),
        CteBody::Merge(_) => Err(ParseError::FeatureNotSupported(
            "MERGE rule action CTE".into(),
        )),
        _ => Err(ParseError::FeatureNotSupported("rule action CTE".into())),
    }
}

fn statement_has_modifying_ctes(statement: &Statement) -> bool {
    let ctes = match statement {
        Statement::Select(stmt) => &stmt.with,
        Statement::Insert(stmt) => &stmt.with,
        Statement::Update(stmt) => &stmt.with,
        Statement::Delete(stmt) => &stmt.with,
        Statement::Merge(stmt) => &stmt.with,
        Statement::Values(stmt) => &stmt.with,
        _ => return false,
    };
    ctes.iter()
        .any(|cte| super::cte_body_is_modifying(&cte.body))
}

fn reject_old_new_in_rule_action_ctes(statement: &Statement) -> Result<(), ParseError> {
    let ctes = match statement {
        Statement::Select(stmt) => &stmt.with,
        Statement::Insert(stmt) => &stmt.with,
        Statement::Update(stmt) => &stmt.with,
        Statement::Delete(stmt) => &stmt.with,
        Statement::Merge(stmt) => &stmt.with,
        Statement::Values(stmt) => &stmt.with,
        _ => return Ok(()),
    };
    for cte in ctes {
        if super::cte_body_references_table(&cte.body, "old") {
            return Err(ParseError::FeatureNotSupportedMessage(
                "cannot refer to OLD within WITH query".into(),
            ));
        }
        if super::cte_body_references_table(&cte.body, "new") {
            return Err(ParseError::FeatureNotSupportedMessage(
                "cannot refer to NEW within WITH query".into(),
            ));
        }
    }
    Ok(())
}

fn rule_action_returning_targets(action: &BoundRuleAction) -> &[TargetEntry] {
    match action {
        BoundRuleAction::Insert(stmt) => &stmt.returning,
        BoundRuleAction::Update(stmt) => &stmt.returning,
        BoundRuleAction::Delete(stmt) => &stmt.returning,
        BoundRuleAction::Select(_) | BoundRuleAction::Notify(_) => &[],
        BoundRuleAction::Sequence(actions) => actions
            .last()
            .map(rule_action_returning_targets)
            .unwrap_or(&[]),
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
