use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogMutationEffect;
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::commands::tablecmds::{
    apply_base_delete_row, apply_base_update_row, execute_delete_with_waiter, execute_insert,
    execute_insert_values, execute_update_with_waiter, finalize_bound_delete_stmt,
    finalize_bound_insert_stmt, finalize_bound_update_stmt, materialize_delete_row_events,
    materialize_insert_rows, materialize_update_row_events,
};
use crate::backend::commands::trigger::{RuntimeTriggers, relation_has_instead_row_trigger};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, TupleSlot, Value, eval_expr,
};
use crate::backend::parser::{
    CatalogLookup, CommentOnRuleStatement, CreateRuleStatement, DropRuleStatement, FromItem,
    ParseError, RuleDoKind, RuleEvent, SelectItem, SelectStatement, Statement,
    bind_rule_action_statement, bind_rule_qual, rewrite_bound_delete_auto_view_target,
    rewrite_bound_insert_auto_view_target, rewrite_bound_update_auto_view_target,
    validate_rule_definition,
};
use crate::backend::rewrite::split_stored_rule_action_sql;
use crate::backend::rewrite::{ViewDmlEvent, ViewDmlRewriteError};
use crate::backend::storage::lmgr::TableLockMode;
use crate::include::catalog::PgRewriteRow;
use crate::include::nodes::primnodes::{QueryColumn, RelationDesc, TargetEntry};
use crate::pgrust::database::TransactionWaiter;
use crate::pgrust::database::ddl::map_catalog_error;
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_rule_relation_for_ddl};
use crate::pgrust::database::foreign_keys::TableLockRequest;
use crate::pgrust::database::{AutoCommitGuard, Database};
use crate::pl::plpgsql::TriggerOperation;

impl Database {
    pub(crate) fn execute_create_rule_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateRuleStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &create_stmt.relation_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_rule_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_create_rule_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateRuleStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &create_stmt.relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &create_stmt.relation_name)?;
        validate_create_rule_stmt(create_stmt, &relation, &catalog)?;
        validate_rule_definition(create_stmt, &relation.desc, &catalog)
            .map_err(ExecError::Parse)?;

        let referenced_relation_oids =
            referenced_relation_oids_for_rule(create_stmt, relation.relation_oid, &catalog)?;
        let ev_action = create_stmt
            .actions
            .iter()
            .map(|action| action.sql.as_str())
            .collect::<Vec<_>>()
            .join(";\n");
        let ev_qual = create_stmt.where_sql.clone().unwrap_or_default();

        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: Arc::clone(&self.txns),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let effect = catalog_guard
            .create_rule_mvcc(
                relation.relation_oid,
                create_stmt.rule_name.clone(),
                rule_event_code(create_stmt.event),
                create_stmt.do_kind == RuleDoKind::Instead,
                ev_qual,
                ev_action,
                &referenced_relation_oids,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_rule_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnRuleStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &comment_stmt.relation_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_rule_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_comment_on_rule_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnRuleStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &comment_stmt.relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.relation_name)?;
        let rewrite = lookup_rule_row(&catalog, relation.relation_oid, &comment_stmt.rule_name)
            .ok_or_else(|| {
                missing_rule_error(&comment_stmt.rule_name, &comment_stmt.relation_name)
            })?;

        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: Arc::clone(&self.txns),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let effect = catalog_guard
            .comment_rule_mvcc(rewrite.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_rule_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropRuleStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let maybe_relation = {
            let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            catalog.lookup_any_relation(&drop_stmt.relation_name)
        };
        if let Some(relation) = maybe_relation {
            let interrupts = self.interrupt_state(client_id);
            self.table_locks.lock_table_interruptible(
                relation.rel,
                TableLockMode::AccessExclusive,
                client_id,
                interrupts.as_ref(),
            )?;
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let result = self.execute_drop_rule_stmt_in_transaction_with_search_path(
                client_id,
                drop_stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
            let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
            guard.disarm();
            self.table_locks.unlock_table(relation.rel, client_id);
            result
        } else if drop_stmt.if_exists {
            Ok(StatementResult::AffectedRows(0))
        } else {
            Err(ExecError::Parse(ParseError::UnknownTable(
                drop_stmt.relation_name.clone(),
            )))
        }
    }

    pub(crate) fn execute_drop_rule_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropRuleStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match lookup_rule_relation_for_ddl(&catalog, &drop_stmt.relation_name) {
            Ok(relation) => relation,
            Err(_err) if drop_stmt.if_exists => return Ok(StatementResult::AffectedRows(0)),
            Err(ExecError::Parse(ParseError::TableDoesNotExist(_))) => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    drop_stmt.relation_name.clone(),
                )));
            }
            Err(err) => return Err(err),
        };
        ensure_relation_owner(self, client_id, &relation, &drop_stmt.relation_name)?;
        let Some(rewrite) = lookup_rule_row(&catalog, relation.relation_oid, &drop_stmt.rule_name)
        else {
            return if drop_stmt.if_exists {
                Ok(StatementResult::AffectedRows(0))
            } else {
                Err(missing_rule_error(
                    &drop_stmt.rule_name,
                    &drop_stmt.relation_name,
                ))
            };
        };
        if rewrite.rulename.eq_ignore_ascii_case("_RETURN") {
            return Err(ExecError::DetailedError {
                message: format!(
                    "rule \"{}\" for relation \"{}\" cannot be dropped",
                    drop_stmt.rule_name, drop_stmt.relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "2BP01",
            });
        }

        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: Arc::clone(&self.txns),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let effect = catalog_guard
            .drop_rule_mvcc(rewrite.oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

fn validate_create_rule_stmt(
    create_stmt: &CreateRuleStatement,
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if create_stmt.rule_name.eq_ignore_ascii_case("_RETURN") {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "_RETURN rules".into(),
        )));
    }
    if create_stmt.event == RuleEvent::Select {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CREATE RULE ... ON SELECT".into(),
        )));
    }
    if create_stmt.actions.is_empty() && create_stmt.do_kind != RuleDoKind::Instead {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "DO ALSO NOTHING".into(),
        )));
    }
    if lookup_rule_row(catalog, relation.relation_oid, &create_stmt.rule_name).is_some() {
        return Err(ExecError::DetailedError {
            message: format!(
                "rule \"{}\" for relation \"{}\" already exists",
                create_stmt.rule_name, create_stmt.relation_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42710",
        });
    }
    Ok(())
}

fn referenced_relation_oids_for_rule(
    create_stmt: &CreateRuleStatement,
    owner_relation_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<u32>, ExecError> {
    let mut referenced = BTreeSet::new();
    for action in &create_stmt.actions {
        let maybe_name = match &action.statement {
            Statement::Insert(stmt) => Some(stmt.table_name.as_str()),
            Statement::Update(stmt) => Some(stmt.table_name.as_str()),
            Statement::Delete(stmt) => Some(stmt.table_name.as_str()),
            _ => None,
        };
        if let Some(name) = maybe_name {
            let relation = catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(name.to_string())))?;
            if relation.relation_oid != owner_relation_oid {
                referenced.insert(relation.relation_oid);
            }
        }
    }
    Ok(referenced.into_iter().collect())
}

fn lookup_rule_row(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    rule_name: &str,
) -> Option<PgRewriteRow> {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.rulename.eq_ignore_ascii_case(rule_name))
}

fn rule_event_code(event: RuleEvent) -> char {
    match event {
        RuleEvent::Select => '1',
        RuleEvent::Update => '2',
        RuleEvent::Insert => '3',
        RuleEvent::Delete => '4',
    }
}

fn missing_rule_error(rule_name: &str, relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "rule \"{}\" for relation \"{}\" does not exist",
            rule_name, relation_name
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

#[derive(Clone)]
struct PreparedRule {
    is_instead: bool,
    qual: Option<crate::backend::executor::Expr>,
    actions: Vec<crate::backend::parser::BoundRuleAction>,
}

#[derive(Default)]
struct RuleMatchOutcome {
    matched_instead: bool,
    matched_actions: bool,
    returning_seen: bool,
    returning_rows: Vec<Vec<Value>>,
}

pub(crate) struct PreparedBoundStatement<T> {
    pub(crate) stmt: T,
    pub(crate) extra_lock_requests: Vec<TableLockRequest>,
}

pub(crate) fn prepare_bound_insert_for_execution(
    stmt: crate::backend::parser::BoundInsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PreparedBoundStatement<crate::backend::parser::BoundInsertStatement>, ExecError> {
    if stmt.relkind != 'v'
        || relation_has_user_rules_for_event(stmt.relation_oid, RuleEvent::Insert, catalog)
    {
        return Ok(PreparedBoundStatement {
            stmt,
            extra_lock_requests: Vec::new(),
        });
    }
    if relation_has_instead_row_trigger(catalog, stmt.relation_oid, TriggerOperation::Insert) {
        return Ok(PreparedBoundStatement {
            extra_lock_requests: vec![(stmt.rel, TableLockMode::RowExclusive)],
            stmt,
        });
    }

    let view_name = stmt.relation_name.clone();
    let view_rel = stmt.rel;
    let stmt = rewrite_bound_insert_auto_view_target(stmt, catalog)
        .map_err(|err| auto_view_prepare_error(&view_name, ViewDmlEvent::Insert, err))?;
    Ok(PreparedBoundStatement {
        stmt,
        extra_lock_requests: vec![(view_rel, TableLockMode::RowExclusive)],
    })
}

pub(crate) fn prepare_bound_update_for_execution(
    stmt: crate::backend::parser::BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PreparedBoundStatement<crate::backend::parser::BoundUpdateStatement>, ExecError> {
    let Some(view_target) = stmt.targets.iter().find(|target| target.relkind == 'v') else {
        return Ok(PreparedBoundStatement {
            stmt,
            extra_lock_requests: Vec::new(),
        });
    };
    if relation_has_user_rules_for_event(view_target.relation_oid, RuleEvent::Update, catalog) {
        return Ok(PreparedBoundStatement {
            stmt,
            extra_lock_requests: Vec::new(),
        });
    }
    if relation_has_instead_row_trigger(catalog, view_target.relation_oid, TriggerOperation::Update)
    {
        return Ok(PreparedBoundStatement {
            extra_lock_requests: vec![(view_target.rel, TableLockMode::RowExclusive)],
            stmt,
        });
    }

    let view_name = view_target.relation_name.clone();
    let view_rel = view_target.rel;
    let stmt = rewrite_bound_update_auto_view_target(stmt, catalog)
        .map_err(|err| auto_view_prepare_error(&view_name, ViewDmlEvent::Update, err))?;
    Ok(PreparedBoundStatement {
        stmt,
        extra_lock_requests: vec![(view_rel, TableLockMode::RowExclusive)],
    })
}

pub(crate) fn prepare_bound_delete_for_execution(
    stmt: crate::backend::parser::BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PreparedBoundStatement<crate::backend::parser::BoundDeleteStatement>, ExecError> {
    let Some(view_target) = stmt.targets.iter().find(|target| target.relkind == 'v') else {
        return Ok(PreparedBoundStatement {
            stmt,
            extra_lock_requests: Vec::new(),
        });
    };
    if relation_has_user_rules_for_event(view_target.relation_oid, RuleEvent::Delete, catalog) {
        return Ok(PreparedBoundStatement {
            stmt,
            extra_lock_requests: Vec::new(),
        });
    }
    if relation_has_instead_row_trigger(catalog, view_target.relation_oid, TriggerOperation::Delete)
    {
        return Ok(PreparedBoundStatement {
            extra_lock_requests: vec![(view_target.rel, TableLockMode::RowExclusive)],
            stmt,
        });
    }

    let view_name = view_target.relation_name.clone();
    let view_rel = view_target.rel;
    let stmt = rewrite_bound_delete_auto_view_target(stmt, catalog)
        .map_err(|err| auto_view_prepare_error(&view_name, ViewDmlEvent::Delete, err))?;
    Ok(PreparedBoundStatement {
        stmt,
        extra_lock_requests: vec![(view_rel, TableLockMode::RowExclusive)],
    })
}

pub(crate) fn execute_bound_insert_with_rules(
    stmt: crate::backend::parser::BoundInsertStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let has_user_rules =
        relation_has_user_rules_for_event(stmt.relation_oid, RuleEvent::Insert, catalog);
    if matches!(stmt.relkind, 'r' | 'p') && !has_user_rules {
        return execute_insert(stmt, catalog, ctx, xid, cid);
    }
    if stmt.relkind == 'v'
        && !has_user_rules
        && relation_has_instead_row_trigger(catalog, stmt.relation_oid, TriggerOperation::Insert)
    {
        return execute_view_insert_with_triggers(stmt, catalog, ctx);
    }

    let stmt = finalize_bound_insert_stmt(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let rules = load_prepared_rules(stmt.relation_oid, RuleEvent::Insert, &stmt.desc, catalog)?;
        let rows = materialize_insert_rows(&stmt, catalog, ctx)?;
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        let null_old = vec![Value::Null; stmt.desc.columns.len()];

        for row in rows {
            let outcome = execute_matching_rules(
                &rules,
                &null_old,
                &row,
                catalog,
                ctx,
                xid,
                cid,
                None,
                !stmt.returning.is_empty(),
            )?;
            if outcome.matched_instead {
                if !stmt.returning.is_empty() {
                    if !outcome.returning_seen {
                        return Err(missing_rule_returning_error(
                            RuleEvent::Insert,
                            &stmt.relation_name,
                        ));
                    }
                    returned_rows.extend(project_statement_returning_rows(
                        &stmt.returning,
                        &outcome.returning_rows,
                        ctx,
                    )?);
                }
                if outcome.matched_actions {
                    affected_rows += 1;
                }
                continue;
            }
            if !matches!(stmt.relkind, 'r' | 'p') {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: stmt.relation_name.clone(),
                    expected: "table",
                }));
            }
            if stmt.returning.is_empty() {
                execute_insert_values(
                    &stmt.relation_name,
                    stmt.relation_oid,
                    stmt.rel,
                    stmt.toast,
                    stmt.toast_index.as_ref(),
                    &stmt.desc,
                    &stmt.relation_constraints,
                    &stmt.rls_write_checks,
                    &stmt.indexes,
                    std::slice::from_ref(&row),
                    ctx,
                    xid,
                    cid,
                )?;
            } else {
                returned_rows.extend(project_statement_returning_rows(
                    &stmt.returning,
                    &crate::backend::commands::tablecmds::execute_insert_rows(
                        &stmt.relation_name,
                        &stmt.relation_name,
                        stmt.relation_oid,
                        stmt.rel,
                        stmt.toast,
                        stmt.toast_index.as_ref(),
                        &stmt.desc,
                        &stmt.relation_constraints,
                        &stmt.rls_write_checks,
                        &stmt.indexes,
                        std::slice::from_ref(&row),
                        None,
                        ctx,
                        xid,
                        cid,
                    )?,
                    ctx,
                )?);
            }
            affected_rows += 1;
        }
        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_statement_returning_result(
                &stmt.returning,
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

pub(crate) fn execute_bound_update_with_rules(
    stmt: crate::backend::parser::BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<crate::backend::access::transam::xact::TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let has_rule_target = stmt.targets.iter().any(|target| {
        target.relkind == 'v'
            || catalog
                .rewrite_rows_for_relation(target.relation_oid)
                .into_iter()
                .any(|row| {
                    row.ev_type == rule_event_code(RuleEvent::Update) && row.rulename != "_RETURN"
                })
    });
    if !has_rule_target {
        return execute_update_with_waiter(stmt, catalog, ctx, xid, cid, waiter);
    }

    let stmt = finalize_bound_update_stmt(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        let joined_update_events = if stmt.input_plan.is_some() {
            Some(materialize_update_row_events(&stmt, ctx)?)
        } else {
            None
        };
        for target in &stmt.targets {
            let view_has_user_rules =
                relation_has_user_rules_for_event(target.relation_oid, RuleEvent::Update, catalog);
            if target.relkind == 'v'
                && !view_has_user_rules
                && relation_has_instead_row_trigger(
                    catalog,
                    target.relation_oid,
                    TriggerOperation::Update,
                )
            {
                let (target_affected_rows, mut target_returned_rows) =
                    execute_view_update_with_triggers(target, &stmt.returning, catalog, ctx)?;
                affected_rows += target_affected_rows;
                returned_rows.append(&mut target_returned_rows);
                continue;
            }
            let rules = load_prepared_rules(
                target.relation_oid,
                RuleEvent::Update,
                &target.desc,
                catalog,
            )?;
            if target.relkind == 'v' {
                if !view_has_user_rules {
                    return Err(missing_view_instead_trigger_error(
                        &target.relation_name,
                        ViewDmlEvent::Update,
                    ));
                }
                for (old_values, new_values) in
                    materialize_view_update_events(target, catalog, ctx)?
                {
                    let outcome = execute_matching_rules(
                        &rules,
                        &old_values,
                        &new_values,
                        catalog,
                        ctx,
                        xid,
                        cid,
                        waiter,
                        !stmt.returning.is_empty(),
                    )?;
                    if !outcome.matched_instead {
                        return Err(ExecError::Parse(ParseError::WrongObjectType {
                            name: target.relation_name.clone(),
                            expected: "table",
                        }));
                    }
                    if !stmt.returning.is_empty() {
                        if !outcome.returning_seen {
                            return Err(missing_rule_returning_error(
                                RuleEvent::Update,
                                &target.relation_name,
                            ));
                        }
                        returned_rows.extend(project_statement_returning_rows(
                            &stmt.returning,
                            &outcome.returning_rows,
                            ctx,
                        )?);
                    }
                    if outcome.matched_actions {
                        affected_rows += 1;
                    }
                }
                continue;
            }

            let target_events = if let Some(events) = &joined_update_events {
                events
                    .iter()
                    .filter(|event| event.target.relation_oid == target.relation_oid)
                    .cloned()
                    .collect()
            } else {
                materialize_update_row_events(
                    &crate::backend::parser::BoundUpdateStatement {
                        target_relation_name: target.relation_name.clone(),
                        explain_target_name: target.relation_name.clone(),
                        targets: vec![target.clone()],
                        returning: Vec::new(),
                        input_plan: None,
                        target_visible_count: target.desc.columns.len(),
                        visible_column_count: target.desc.columns.len(),
                        target_ctid_index: target.desc.columns.len(),
                        target_tableoid_index: target.desc.columns.len() + 1,
                        required_privileges: Vec::new(),
                        subplans: Vec::new(),
                    },
                    ctx,
                )?
            };

            for event in target_events {
                let outcome = execute_matching_rules(
                    &rules,
                    &event.old_values,
                    &event.new_values,
                    catalog,
                    ctx,
                    xid,
                    cid,
                    waiter,
                    !stmt.returning.is_empty(),
                )?;
                if outcome.matched_instead {
                    if !stmt.returning.is_empty() {
                        if !outcome.returning_seen {
                            return Err(missing_rule_returning_error(
                                RuleEvent::Update,
                                &event.target.relation_name,
                            ));
                        }
                        returned_rows.extend(project_statement_returning_rows(
                            &stmt.returning,
                            &outcome.returning_rows,
                            ctx,
                        )?);
                    }
                    if outcome.matched_actions {
                        affected_rows += 1;
                    }
                    continue;
                }
                let returned_row = (!stmt.returning.is_empty()).then(|| event.new_values.clone());
                if apply_base_update_row(
                    &event.target,
                    event.tid,
                    event.old_values,
                    event.new_values,
                    ctx,
                    xid,
                    cid,
                    waiter,
                )? {
                    if let Some(returned_row) = returned_row {
                        returned_rows.push(project_statement_returning_row(
                            &stmt.returning,
                            &returned_row,
                            ctx,
                        )?);
                    }
                    affected_rows += 1;
                }
            }
        }
        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_statement_returning_result(
                &stmt.returning,
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

pub(crate) fn execute_bound_delete_with_rules(
    stmt: crate::backend::parser::BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(
        &RwLock<crate::backend::access::transam::xact::TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let has_rule_target = stmt.targets.iter().any(|target| {
        target.relkind == 'v'
            || catalog
                .rewrite_rows_for_relation(target.relation_oid)
                .into_iter()
                .any(|row| {
                    row.ev_type == rule_event_code(RuleEvent::Delete) && row.rulename != "_RETURN"
                })
    });
    if !has_rule_target {
        return execute_delete_with_waiter(stmt, catalog, ctx, xid, waiter);
    }

    let stmt = finalize_bound_delete_stmt(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        for target in &stmt.targets {
            let view_has_user_rules =
                relation_has_user_rules_for_event(target.relation_oid, RuleEvent::Delete, catalog);
            if target.relkind == 'v'
                && !view_has_user_rules
                && relation_has_instead_row_trigger(
                    catalog,
                    target.relation_oid,
                    TriggerOperation::Delete,
                )
            {
                let (target_affected_rows, mut target_returned_rows) =
                    execute_view_delete_with_triggers(target, &stmt.returning, catalog, ctx)?;
                affected_rows += target_affected_rows;
                returned_rows.append(&mut target_returned_rows);
                continue;
            }
            let rules = load_prepared_rules(
                target.relation_oid,
                RuleEvent::Delete,
                &target.desc,
                catalog,
            )?;
            if target.relkind == 'v' {
                if !view_has_user_rules {
                    return Err(missing_view_instead_trigger_error(
                        &target.relation_name,
                        ViewDmlEvent::Delete,
                    ));
                }
                for old_values in materialize_view_delete_events(target, catalog, ctx)? {
                    let outcome = execute_matching_rules(
                        &rules,
                        &old_values,
                        &vec![Value::Null; target.desc.columns.len()],
                        catalog,
                        ctx,
                        xid,
                        0,
                        waiter,
                        !stmt.returning.is_empty(),
                    )?;
                    if !outcome.matched_instead {
                        return Err(ExecError::Parse(ParseError::WrongObjectType {
                            name: target.relation_name.clone(),
                            expected: "table",
                        }));
                    }
                    if !stmt.returning.is_empty() {
                        if !outcome.returning_seen {
                            return Err(missing_rule_returning_error(
                                RuleEvent::Delete,
                                &target.relation_name,
                            ));
                        }
                        returned_rows.extend(project_statement_returning_rows(
                            &stmt.returning,
                            &outcome.returning_rows,
                            ctx,
                        )?);
                    }
                    if outcome.matched_actions {
                        affected_rows += 1;
                    }
                }
                continue;
            }

            for event in materialize_delete_row_events(
                &crate::backend::parser::BoundDeleteStatement {
                    targets: vec![target.clone()],
                    returning: Vec::new(),
                    input_plan: None,
                    target_visible_count: target.desc.columns.len(),
                    visible_column_count: target.desc.columns.len(),
                    target_ctid_index: target.desc.columns.len(),
                    target_tableoid_index: target.desc.columns.len() + 1,
                    required_privileges: Vec::new(),
                    subplans: Vec::new(),
                },
                ctx,
            )? {
                let null_new = vec![Value::Null; event.target.desc.columns.len()];
                let outcome = execute_matching_rules(
                    &rules,
                    &event.old_values,
                    &null_new,
                    catalog,
                    ctx,
                    xid,
                    0,
                    waiter,
                    !stmt.returning.is_empty(),
                )?;
                if outcome.matched_instead {
                    if !stmt.returning.is_empty() {
                        if !outcome.returning_seen {
                            return Err(missing_rule_returning_error(
                                RuleEvent::Delete,
                                &event.target.relation_name,
                            ));
                        }
                        returned_rows.extend(project_statement_returning_rows(
                            &stmt.returning,
                            &outcome.returning_rows,
                            ctx,
                        )?);
                    }
                    if outcome.matched_actions {
                        affected_rows += 1;
                    }
                    continue;
                }
                let returned_row = (!stmt.returning.is_empty()).then(|| event.old_values.clone());
                if apply_base_delete_row(
                    &event.target,
                    event.tid,
                    event.old_values,
                    ctx,
                    xid,
                    waiter,
                )? {
                    if let Some(returned_row) = returned_row {
                        returned_rows.push(project_statement_returning_row(
                            &stmt.returning,
                            &returned_row,
                            ctx,
                        )?);
                    }
                    affected_rows += 1;
                }
            }
        }
        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_statement_returning_result(
                &stmt.returning,
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

fn execute_matching_rules(
    rules: &[PreparedRule],
    old_values: &[Value],
    new_values: &[Value],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<crate::backend::access::transam::xact::TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
    capture_returning: bool,
) -> Result<RuleMatchOutcome, ExecError> {
    let mut outcome = RuleMatchOutcome::default();
    for rule in rules {
        if let Some(qual) = &rule.qual
            && !evaluate_rule_qual(qual, old_values, new_values, ctx)?
        {
            continue;
        }
        outcome.matched_instead |= rule.is_instead;
        if !rule.actions.is_empty() {
            outcome.matched_actions = true;
        }
        for action in &rule.actions {
            let result = with_rule_bindings(ctx, old_values, new_values, |ctx| {
                execute_rule_action(action, catalog, ctx, xid, cid, waiter)
            })?;
            if capture_returning && rule_action_has_returning(action) {
                if outcome.returning_seen {
                    return Err(multiple_rule_returning_error());
                }
                outcome.returning_seen = true;
                outcome.returning_rows = extract_rule_action_returning_rows(result)?;
            }
        }
    }
    Ok(outcome)
}

fn execute_view_insert_with_triggers(
    stmt: crate::backend::parser::BoundInsertStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_insert_stmt(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let triggers = load_view_runtime_triggers(
            &stmt.relation_name,
            stmt.relation_oid,
            &stmt.desc,
            TriggerOperation::Insert,
            &[],
            ctx,
        )?;
        if !triggers.has_instead_row_triggers() {
            return Err(missing_view_instead_trigger_error(
                &stmt.relation_name,
                ViewDmlEvent::Insert,
            ));
        }
        triggers.before_statement(ctx)?;
        let rows = materialize_insert_rows(&stmt, catalog, ctx)?;
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        for row in rows {
            let Some(returned_row) = triggers.instead_row_insert(row, ctx)? else {
                continue;
            };
            if !stmt.returning.is_empty() {
                returned_rows.push(project_statement_returning_row(
                    &stmt.returning,
                    &returned_row,
                    ctx,
                )?);
            }
            affected_rows += 1;
        }
        triggers.after_statement(None, ctx)?;
        if stmt.returning.is_empty() {
            Ok(StatementResult::AffectedRows(affected_rows))
        } else {
            Ok(build_statement_returning_result(
                &stmt.returning,
                returned_rows,
            ))
        }
    })();
    ctx.subplans = saved_subplans;
    result
}

fn execute_view_update_with_triggers(
    target: &crate::backend::parser::BoundUpdateTarget,
    returning: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<(usize, Vec<Vec<Value>>), ExecError> {
    let modified_attnums = modified_attnums_for_assignments(&target.assignments);
    let triggers = load_view_runtime_triggers(
        &target.relation_name,
        target.relation_oid,
        &target.desc,
        TriggerOperation::Update,
        &modified_attnums,
        ctx,
    )?;
    if !triggers.has_instead_row_triggers() {
        return Err(missing_view_instead_trigger_error(
            &target.relation_name,
            ViewDmlEvent::Update,
        ));
    }
    triggers.before_statement(ctx)?;
    let mut affected_rows = 0usize;
    let mut returned_rows = Vec::new();
    for (old_values, new_values) in materialize_view_update_events(target, catalog, ctx)? {
        let Some(returned_row) = triggers.instead_row_update(&old_values, new_values, ctx)? else {
            continue;
        };
        if !returning.is_empty() {
            returned_rows.push(project_statement_returning_row(
                returning,
                &returned_row,
                ctx,
            )?);
        }
        affected_rows += 1;
    }
    triggers.after_statement(None, ctx)?;
    Ok((affected_rows, returned_rows))
}

fn execute_view_delete_with_triggers(
    target: &crate::backend::parser::BoundDeleteTarget,
    returning: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<(usize, Vec<Vec<Value>>), ExecError> {
    let triggers = load_view_runtime_triggers(
        &target.relation_name,
        target.relation_oid,
        &target.desc,
        TriggerOperation::Delete,
        &[],
        ctx,
    )?;
    if !triggers.has_instead_row_triggers() {
        return Err(missing_view_instead_trigger_error(
            &target.relation_name,
            ViewDmlEvent::Delete,
        ));
    }
    triggers.before_statement(ctx)?;
    let mut affected_rows = 0usize;
    let mut returned_rows = Vec::new();
    for old_values in materialize_view_delete_events(target, catalog, ctx)? {
        let Some(returned_row) = triggers.instead_row_delete(old_values, ctx)? else {
            continue;
        };
        if !returning.is_empty() {
            returned_rows.push(project_statement_returning_row(
                returning,
                &returned_row,
                ctx,
            )?);
        }
        affected_rows += 1;
    }
    triggers.after_statement(None, ctx)?;
    Ok((affected_rows, returned_rows))
}

fn rule_action_has_returning(action: &crate::backend::parser::BoundRuleAction) -> bool {
    match action {
        crate::backend::parser::BoundRuleAction::Insert(stmt) => !stmt.returning.is_empty(),
        crate::backend::parser::BoundRuleAction::Update(stmt) => !stmt.returning.is_empty(),
        crate::backend::parser::BoundRuleAction::Delete(stmt) => !stmt.returning.is_empty(),
    }
}

fn extract_rule_action_returning_rows(
    result: StatementResult,
) -> Result<Vec<Vec<Value>>, ExecError> {
    match result {
        StatementResult::Query { rows, .. } => Ok(rows),
        _ => Err(ExecError::DetailedError {
            message: "rule action RETURNING did not produce rows".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn missing_rule_returning_error(event: RuleEvent, relation_name: &str) -> ExecError {
    let (message, hint) = match event {
        RuleEvent::Insert => (
            format!("cannot perform INSERT RETURNING on relation \"{relation_name}\""),
            "You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.",
        ),
        RuleEvent::Update => (
            format!("cannot perform UPDATE RETURNING on relation \"{relation_name}\""),
            "You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.",
        ),
        RuleEvent::Delete => (
            format!("cannot perform DELETE RETURNING on relation \"{relation_name}\""),
            "You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.",
        ),
        RuleEvent::Select => unreachable!("SELECT rules are rejected before execution"),
    };
    ExecError::DetailedError {
        message,
        detail: None,
        hint: Some(hint.into()),
        sqlstate: "0A000",
    }
}

fn multiple_rule_returning_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot have RETURNING lists in multiple rules".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn statement_returning_columns(targets: &[TargetEntry]) -> Vec<QueryColumn> {
    targets
        .iter()
        .map(|target| QueryColumn {
            name: target.name.clone(),
            sql_type: target.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

fn project_statement_returning_row(
    targets: &[TargetEntry],
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut slot = TupleSlot::virtual_row(row.to_vec());
    let mut values = targets
        .iter()
        .map(|target| eval_expr(&target.expr, &mut slot, ctx).map(|value| value.to_owned_value()))
        .collect::<Result<Vec<_>, _>>()?;
    Value::materialize_all(&mut values);
    Ok(values)
}

fn project_statement_returning_rows(
    targets: &[TargetEntry],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
) -> Result<Vec<Vec<Value>>, ExecError> {
    rows.iter()
        .map(|row| project_statement_returning_row(targets, row, ctx))
        .collect()
}

fn build_statement_returning_result(
    targets: &[TargetEntry],
    rows: Vec<Vec<Value>>,
) -> StatementResult {
    let columns = statement_returning_columns(targets);
    let column_names = columns.iter().map(|column| column.name.clone()).collect();
    StatementResult::Query {
        columns,
        column_names,
        rows,
    }
}

fn execute_rule_action(
    action: &crate::backend::parser::BoundRuleAction,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<crate::backend::access::transam::xact::TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    match action {
        crate::backend::parser::BoundRuleAction::Insert(stmt) => {
            execute_bound_insert_with_rules(stmt.clone(), catalog, ctx, xid, cid)
        }
        crate::backend::parser::BoundRuleAction::Update(stmt) => {
            execute_bound_update_with_rules(stmt.clone(), catalog, ctx, xid, cid, waiter)
        }
        crate::backend::parser::BoundRuleAction::Delete(stmt) => {
            execute_bound_delete_with_rules(stmt.clone(), catalog, ctx, xid, waiter)
        }
    }
}

fn evaluate_rule_qual(
    qual: &crate::backend::executor::Expr,
    old_values: &[Value],
    new_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    with_rule_bindings(ctx, old_values, new_values, |ctx| {
        let mut slot = TupleSlot::virtual_row(Vec::new());
        match eval_expr(qual, &mut slot, ctx)? {
            Value::Bool(value) => Ok(value),
            Value::Null => Ok(false),
            other => Err(ExecError::NonBoolQual(other)),
        }
    })
}

fn with_rule_bindings<T>(
    ctx: &mut ExecutorContext,
    old_values: &[Value],
    new_values: &[Value],
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let saved_outer = ctx.expr_bindings.outer_tuple.clone();
    let saved_inner = ctx.expr_bindings.inner_tuple.clone();
    ctx.expr_bindings.outer_tuple = Some(old_values.to_vec());
    ctx.expr_bindings.inner_tuple = Some(new_values.to_vec());
    let result = f(ctx);
    ctx.expr_bindings.outer_tuple = saved_outer;
    ctx.expr_bindings.inner_tuple = saved_inner;
    result
}

fn load_prepared_rules(
    relation_oid: u32,
    event: RuleEvent,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<PreparedRule>, ExecError> {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| row.rulename != "_RETURN" && row.ev_type == rule_event_code(event))
        .map(|row| {
            let qual = if row.ev_qual.is_empty() {
                None
            } else {
                let parsed = crate::backend::parser::parse_expr(&row.ev_qual)?;
                Some(bind_rule_qual(&parsed, relation_desc, event, catalog)?)
            };
            let mut actions = Vec::new();
            for sql in split_stored_rule_action_sql(&row.ev_action) {
                let statement = crate::backend::parser::parse_statement(sql)?;
                actions.push(bind_rule_action_statement(
                    &statement,
                    relation_desc,
                    catalog,
                )?);
            }
            Ok(PreparedRule {
                is_instead: row.is_instead,
                qual,
                actions,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()
        .map_err(ExecError::Parse)
}

fn materialize_view_update_events(
    target: &crate::backend::parser::BoundUpdateTarget,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(Vec<Value>, Vec<Value>)>, ExecError> {
    let rows = materialize_view_rows(&target.relation_name, &target.desc, catalog, ctx)?;
    let mut out = Vec::new();
    for row in rows {
        if !row_passes_predicate(target.predicate.as_ref(), &row, ctx)? {
            continue;
        }
        let mut eval_slot = TupleSlot::virtual_row(row.clone());
        let mut new_values = row.clone();
        for assignment in &target.assignments {
            let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
            new_values[assignment.column_index] = value;
        }
        out.push((row, new_values));
    }
    Ok(out)
}

fn materialize_view_delete_events(
    target: &crate::backend::parser::BoundDeleteTarget,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let rows = materialize_view_rows(&target.relation_name, &target.desc, catalog, ctx)?;
    rows.into_iter()
        .filter_map(
            |row| match row_passes_predicate(target.predicate.as_ref(), &row, ctx) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            },
        )
        .collect()
}

fn materialize_view_rows(
    relation_name: &str,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let visible_columns = desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| SelectItem {
            output_name: column.name.clone(),
            expr: crate::backend::parser::SqlExpr::Column(column.name.clone()),
        })
        .collect();
    let select = SelectStatement {
        with_recursive: false,
        with: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        from: Some(FromItem::Table {
            name: relation_name.to_string(),
            only: false,
        }),
        targets: visible_columns,
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        window_clauses: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        locking_clause: None,
        locking_targets: Vec::new(),
        set_operation: None,
    };
    let planned = crate::backend::parser::pg_plan_query(&select, catalog)?;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, planned.subplans.clone());
    let result: Result<Vec<Vec<Value>>, ExecError> = (|| {
        let mut state = crate::backend::executor::executor_start(planned.plan_tree);
        let mut rows = Vec::new();
        while let Some(slot) = crate::backend::executor::exec_next(&mut state, ctx)? {
            rows.push(slot.values()?.to_vec());
        }
        Ok(rows)
    })();
    ctx.subplans = saved_subplans;
    result
}

fn row_passes_predicate(
    predicate: Option<&crate::backend::executor::Expr>,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let mut slot = TupleSlot::virtual_row(row.to_vec());
    match eval_expr(predicate, &mut slot, ctx)? {
        Value::Bool(value) => Ok(value),
        Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn relation_has_user_rules_for_event(
    relation_oid: u32,
    event: RuleEvent,
    catalog: &dyn CatalogLookup,
) -> bool {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| row.rulename != "_RETURN" && row.ev_type == rule_event_code(event))
}

fn load_view_runtime_triggers(
    relation_name: &str,
    relation_oid: u32,
    relation_desc: &RelationDesc,
    event: TriggerOperation,
    modified_attnums: &[i16],
    ctx: &mut ExecutorContext,
) -> Result<RuntimeTriggers, ExecError> {
    let visible_catalog = ctx.catalog.as_deref().ok_or(ExecError::DetailedError {
        message: "view trigger execution requires executor catalog context".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    })?;
    RuntimeTriggers::load(
        visible_catalog,
        relation_oid,
        relation_name,
        relation_desc,
        event,
        modified_attnums,
        ctx.session_replication_role,
    )
}

fn modified_attnums_for_assignments(
    assignments: &[crate::backend::parser::BoundAssignment],
) -> Vec<i16> {
    assignments
        .iter()
        .map(|assignment| (assignment.column_index + 1) as i16)
        .collect()
}

fn auto_view_prepare_error(
    relation_name: &str,
    event: ViewDmlEvent,
    err: ViewDmlRewriteError,
) -> ExecError {
    match err {
        ViewDmlRewriteError::DeferredFeature(detail) => {
            ExecError::Parse(ParseError::FeatureNotSupported(detail))
        }
        ViewDmlRewriteError::NonUpdatableColumn {
            column_name,
            reason,
        } => ExecError::DetailedError {
            message: format!(
                "cannot {} column \"{}\" of view \"{}\"",
                event_verb(event),
                column_name,
                relation_name
            ),
            detail: Some(reason.detail().into()),
            hint: None,
            sqlstate: "55000",
        },
        ViewDmlRewriteError::MultipleAssignments(column_name) => ExecError::DetailedError {
            message: format!("multiple assignments to same column \"{}\"", column_name),
            detail: None,
            hint: None,
            sqlstate: "42601",
        },
        other => ExecError::DetailedError {
            message: format!("cannot {} view \"{}\"", event_verb(event), relation_name),
            detail: Some(other.detail()),
            hint: Some(format!(
                "To enable {} the view, provide an INSTEAD OF {} trigger or an unconditional ON {} DO INSTEAD rule.",
                event_gerund(event),
                event_name(event),
                event_name(event),
            )),
            sqlstate: "55000",
        },
    }
}

fn missing_view_instead_trigger_error(relation_name: &str, event: ViewDmlEvent) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot {} view \"{}\"", event_verb(event), relation_name),
        detail: Some(format!(
            "View \"{}\" needs an enabled INSTEAD OF {} trigger or an unconditional ON {} DO INSTEAD rule.",
            relation_name,
            event_name(event),
            event_name(event),
        )),
        hint: Some(format!(
            "To enable {} the view, provide an INSTEAD OF {} trigger or an unconditional ON {} DO INSTEAD rule.",
            event_gerund(event),
            event_name(event),
            event_name(event),
        )),
        sqlstate: "55000",
    }
}

fn event_name(event: ViewDmlEvent) -> &'static str {
    match event {
        ViewDmlEvent::Insert => "INSERT",
        ViewDmlEvent::Update => "UPDATE",
        ViewDmlEvent::Delete => "DELETE",
    }
}

fn event_verb(event: ViewDmlEvent) -> &'static str {
    match event {
        ViewDmlEvent::Insert => "insert into",
        ViewDmlEvent::Update => "update",
        ViewDmlEvent::Delete => "delete from",
    }
}

fn event_gerund(event: ViewDmlEvent) -> &'static str {
    match event {
        ViewDmlEvent::Insert => "inserting into",
        ViewDmlEvent::Update => "updating",
        ViewDmlEvent::Delete => "deleting from",
    }
}
