#![allow(dead_code)]

use super::rewrite_policy_expr;
use crate::backend::catalog::role_memberships::has_effective_membership;
use crate::backend::catalog::roles::{has_bypassrls_privilege, policy_applies_to_role};
use crate::backend::parser::{
    BoundScope, CatalogLookup, ParseError, bind_expr_with_outer_and_ctes, parse_select,
    scope_for_relation, shift_scope_rtindexes,
};
use crate::include::catalog::{PgClassRow, PgPolicyRow, PolicyCommand};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{Query, RangeTblEntryKind};
use crate::include::nodes::primnodes::{BoolExprType, Expr, RelationDesc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RlsWriteCheckSource {
    Insert,
    Update,
    SelectVisibility,
    ViewCheckOption(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RlsWriteCheck {
    pub expr: Expr,
    pub policy_name: Option<String>,
    pub source: RlsWriteCheckSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetRlsState {
    pub visibility_qual: Option<Expr>,
    pub write_checks: Vec<RlsWriteCheck>,
    pub depends_on_row_security: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RlsStatus {
    None,
    NoneEnv,
    Enabled,
}

pub(crate) fn relation_has_row_security(relation_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    catalog
        .class_row_by_oid(relation_oid)
        .is_some_and(|row| row.relrowsecurity)
}

pub(crate) fn apply_query_row_security(
    query: &mut Query,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    let mut active_policy_relations = Vec::new();
    apply_query_row_security_with_active_relations(query, catalog, &mut active_policy_relations)
}

pub(super) fn apply_query_row_security_with_active_relations(
    query: &mut Query,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<(), ParseError> {
    if let Some(recursive_union) = query.recursive_union.as_mut() {
        apply_query_row_security_with_active_relations(
            &mut recursive_union.anchor,
            catalog,
            active_policy_relations,
        )?;
        apply_query_row_security_with_active_relations(
            &mut recursive_union.recursive,
            catalog,
            active_policy_relations,
        )?;
        query.depends_on_row_security |= recursive_union.anchor.depends_on_row_security
            || recursive_union.recursive.depends_on_row_security;
    }
    if let Some(set_operation) = query.set_operation.as_mut() {
        for input in &mut set_operation.inputs {
            apply_query_row_security_with_active_relations(
                input,
                catalog,
                active_policy_relations,
            )?;
            query.depends_on_row_security |= input.depends_on_row_security;
        }
    }

    for (index, rte) in query.rtable.iter_mut().enumerate() {
        match &mut rte.kind {
            RangeTblEntryKind::Subquery { query: subquery }
            | RangeTblEntryKind::Cte {
                query: subquery, ..
            } => {
                apply_query_row_security_with_active_relations(
                    subquery,
                    catalog,
                    active_policy_relations,
                )?;
                query.depends_on_row_security |= subquery.depends_on_row_security;
            }
            RangeTblEntryKind::Relation { relation_oid, .. } => {
                let Some(class_row) = catalog.class_row_by_oid(*relation_oid) else {
                    continue;
                };
                let status = check_enable_rls(&class_row, catalog)?;
                if matches!(status, RlsStatus::None) {
                    continue;
                }
                query.depends_on_row_security = true;
                if matches!(status, RlsStatus::NoneEnv) {
                    continue;
                }

                let mut quals = with_active_policy_relation(
                    *relation_oid,
                    &class_row.relname,
                    active_policy_relations,
                    |active_policy_relations| {
                        visibility_policy_clauses(
                            *relation_oid,
                            &class_row.relname,
                            &rte.desc,
                            PolicyCommand::Select,
                            index + 1,
                            catalog,
                            active_policy_relations,
                        )
                    },
                )?;
                if !quals.is_empty() {
                    quals.extend(std::mem::take(&mut rte.security_quals));
                    rte.security_quals = quals;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub(crate) fn build_target_relation_row_security(
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    command: PolicyCommand,
    include_select_visibility: bool,
    include_select_check: bool,
    catalog: &dyn CatalogLookup,
) -> Result<TargetRlsState, ParseError> {
    let mut active_policy_relations = Vec::new();
    build_target_relation_row_security_inner(
        relation_name,
        relation_oid,
        desc,
        command,
        include_select_visibility,
        include_select_check,
        catalog,
        &mut active_policy_relations,
    )
}

fn build_target_relation_row_security_inner(
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    command: PolicyCommand,
    include_select_visibility: bool,
    include_select_check: bool,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<TargetRlsState, ParseError> {
    let Some(class_row) = catalog.class_row_by_oid(relation_oid) else {
        return Ok(TargetRlsState {
            visibility_qual: None,
            write_checks: Vec::new(),
            depends_on_row_security: false,
        });
    };
    let status = check_enable_rls(&class_row, catalog)?;
    let depends_on_row_security = !matches!(status, RlsStatus::None);
    if matches!(status, RlsStatus::None | RlsStatus::NoneEnv) {
        return Ok(TargetRlsState {
            visibility_qual: None,
            write_checks: Vec::new(),
            depends_on_row_security,
        });
    }

    let (visibility_clauses, write_checks) = with_active_policy_relation(
        relation_oid,
        &class_row.relname,
        active_policy_relations,
        |active_policy_relations| {
            let mut visibility_clauses = match command {
                PolicyCommand::Update | PolicyCommand::Delete => visibility_policy_clauses(
                    relation_oid,
                    relation_name,
                    desc,
                    command,
                    1,
                    catalog,
                    active_policy_relations,
                )?,
                _ => Vec::new(),
            };
            if include_select_visibility {
                visibility_clauses.extend(visibility_policy_clauses(
                    relation_oid,
                    relation_name,
                    desc,
                    PolicyCommand::Select,
                    1,
                    catalog,
                    active_policy_relations,
                )?);
            }

            let mut write_checks = match command {
                PolicyCommand::Insert => write_policy_checks(
                    relation_oid,
                    relation_name,
                    desc,
                    command,
                    RlsWriteCheckSource::Insert,
                    false,
                    catalog,
                    active_policy_relations,
                )?,
                PolicyCommand::Update => write_policy_checks(
                    relation_oid,
                    relation_name,
                    desc,
                    command,
                    RlsWriteCheckSource::Update,
                    false,
                    catalog,
                    active_policy_relations,
                )?,
                _ => Vec::new(),
            };
            if include_select_check {
                write_checks.extend(write_policy_checks(
                    relation_oid,
                    relation_name,
                    desc,
                    PolicyCommand::Select,
                    RlsWriteCheckSource::SelectVisibility,
                    true,
                    catalog,
                    active_policy_relations,
                )?);
            }
            Ok((visibility_clauses, write_checks))
        },
    )?;

    Ok(TargetRlsState {
        visibility_qual: and_exprs(visibility_clauses),
        write_checks,
        depends_on_row_security,
    })
}

fn check_enable_rls(
    class_row: &PgClassRow,
    catalog: &dyn CatalogLookup,
) -> Result<RlsStatus, ParseError> {
    if !class_row.relrowsecurity {
        return Ok(RlsStatus::None);
    }

    let current_user_oid = catalog.current_user_oid();
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if has_bypassrls_privilege(current_user_oid, &authid_rows)
        || (!class_row.relforcerowsecurity
            && has_effective_membership(
                current_user_oid,
                class_row.relowner,
                &authid_rows,
                &auth_members_rows,
            ))
    {
        return Ok(RlsStatus::NoneEnv);
    }

    if !catalog.row_security_enabled() {
        return Err(ParseError::DetailedError {
            message: format!(
                "query would be affected by row-level security policy for table \"{}\"",
                class_row.relname
            ),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    }

    Ok(RlsStatus::Enabled)
}

fn visibility_policy_clauses(
    relation_oid: u32,
    relation_name: &str,
    desc: &RelationDesc,
    command: PolicyCommand,
    scope_rtindex: usize,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Vec<Expr>, ParseError> {
    let (permissive, restrictive) = applicable_policies(relation_oid, command, catalog);
    let permissive_qual = combined_permissive_expr(
        &permissive,
        relation_name,
        desc,
        scope_rtindex,
        false,
        catalog,
        active_policy_relations,
    )?
    .unwrap_or_else(|| Expr::Const(Value::Bool(false)));
    let mut quals = vec![permissive_qual];
    for policy in restrictive {
        let expr = bound_policy_expr(
            policy.polqual.as_deref(),
            relation_name,
            desc,
            scope_rtindex,
            catalog,
            active_policy_relations,
        )?;
        if !expr_is_true(&expr) {
            quals.push(expr);
        }
    }
    Ok(quals)
}

fn write_policy_checks(
    relation_oid: u32,
    relation_name: &str,
    desc: &RelationDesc,
    command: PolicyCommand,
    source: RlsWriteCheckSource,
    force_using: bool,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Vec<RlsWriteCheck>, ParseError> {
    let (permissive, restrictive) = applicable_policies(relation_oid, command, catalog);
    let mut checks = Vec::new();

    let permissive_expr = combined_permissive_expr(
        &permissive,
        relation_name,
        desc,
        1,
        force_using,
        catalog,
        active_policy_relations,
    )?
    .unwrap_or_else(|| Expr::Const(Value::Bool(false)));
    if !expr_is_true(&permissive_expr) {
        checks.push(RlsWriteCheck {
            expr: permissive_expr,
            policy_name: None,
            source: source.clone(),
        });
    }

    for policy in restrictive {
        let expr = if force_using {
            bound_policy_expr(
                policy.polqual.as_deref(),
                relation_name,
                desc,
                1,
                catalog,
                active_policy_relations,
            )?
        } else {
            bound_policy_expr(
                policy.polwithcheck.as_deref().or(policy.polqual.as_deref()),
                relation_name,
                desc,
                1,
                catalog,
                active_policy_relations,
            )?
        };
        if expr_is_true(&expr) {
            continue;
        }
        checks.push(RlsWriteCheck {
            expr,
            policy_name: Some(policy.polname.clone()),
            source: source.clone(),
        });
    }

    Ok(checks)
}

fn applicable_policies(
    relation_oid: u32,
    command: PolicyCommand,
    catalog: &dyn CatalogLookup,
) -> (Vec<PgPolicyRow>, Vec<PgPolicyRow>) {
    let current_user_oid = catalog.current_user_oid();
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    let mut permissive = Vec::new();
    let mut restrictive = Vec::new();
    for policy in catalog.policy_rows_for_relation(relation_oid) {
        if policy.polcmd != PolicyCommand::All && policy.polcmd != command {
            continue;
        }
        if !policy_applies_to_role(
            &policy.polroles,
            current_user_oid,
            &authid_rows,
            &auth_members_rows,
        ) {
            continue;
        }
        if policy.polpermissive {
            permissive.push(policy);
        } else {
            restrictive.push(policy);
        }
    }
    permissive.sort_by(|left, right| left.polname.cmp(&right.polname));
    restrictive.sort_by(|left, right| left.polname.cmp(&right.polname));
    (permissive, restrictive)
}

fn combined_permissive_expr(
    policies: &[PgPolicyRow],
    relation_name: &str,
    desc: &RelationDesc,
    scope_rtindex: usize,
    force_using: bool,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Option<Expr>, ParseError> {
    let mut exprs = Vec::new();
    for policy in policies {
        let expr = if force_using {
            bound_policy_expr(
                policy.polqual.as_deref(),
                relation_name,
                desc,
                scope_rtindex,
                catalog,
                active_policy_relations,
            )?
        } else {
            bound_policy_expr(
                policy.polwithcheck.as_deref().or(policy.polqual.as_deref()),
                relation_name,
                desc,
                scope_rtindex,
                catalog,
                active_policy_relations,
            )?
        };
        if expr_is_true(&expr) {
            return Ok(Some(expr));
        }
        exprs.push(expr);
    }
    Ok(or_exprs(exprs))
}

fn bound_policy_expr(
    expr_sql: Option<&str>,
    relation_name: &str,
    desc: &RelationDesc,
    scope_rtindex: usize,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Expr, ParseError> {
    let Some(expr_sql) = expr_sql else {
        return Ok(Expr::Const(Value::Bool(true)));
    };
    let parsed = parse_policy_expr(expr_sql)?;
    let scope = policy_scope(relation_name, desc, scope_rtindex);
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
        let expr = bind_expr_with_outer_and_ctes(&parsed, &scope, catalog, &[], None, &[])?;
        rewrite_policy_expr(expr, catalog, active_policy_relations)
    })
}

fn policy_scope(relation_name: &str, desc: &RelationDesc, scope_rtindex: usize) -> BoundScope {
    shift_scope_rtindexes(
        scope_for_relation(Some(relation_name), desc),
        scope_rtindex - 1,
    )
}

fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(Expr::bool_expr(BoolExprType::And, exprs)),
    }
}

fn or_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(Expr::bool_expr(BoolExprType::Or, exprs)),
    }
}

fn expr_is_true(expr: &Expr) -> bool {
    matches!(expr, Expr::Const(Value::Bool(true)))
}

fn parse_policy_expr(
    expr_sql: &str,
) -> Result<crate::include::nodes::parsenodes::SqlExpr, ParseError> {
    let stmt = parse_select(&format!("select {expr_sql}"))?;
    if stmt.targets.len() != 1 {
        return Err(ParseError::UnexpectedToken {
            expected: "single policy expression",
            actual: format!("{} select targets", stmt.targets.len()),
        });
    }
    Ok(stmt
        .targets
        .into_iter()
        .next()
        .expect("single-target policy expression")
        .expr)
}

fn with_active_policy_relation<T>(
    relation_oid: u32,
    relation_name: &str,
    active_policy_relations: &mut Vec<u32>,
    f: impl FnOnce(&mut Vec<u32>) -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    if active_policy_relations.contains(&relation_oid) {
        return Err(ParseError::DetailedError {
            message: format!(
                "infinite recursion detected in policy for relation \"{}\"",
                relation_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }

    active_policy_relations.push(relation_oid);
    let result = f(active_policy_relations);
    active_policy_relations.pop();
    result
}
