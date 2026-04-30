use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogMutationEffect;
use crate::backend::catalog::store::{CatalogWriteContext, RuleDependencies, RuleOwnerDependency};
use crate::backend::commands::tablecmds::{
    apply_assignment_target, apply_base_delete_row, apply_base_update_row,
    check_planned_stmt_select_privileges, check_relation_privilege_requirements,
    execute_delete_with_waiter, execute_insert, execute_insert_values, execute_update_with_waiter,
    finalize_bound_delete_stmt, finalize_bound_insert_stmt, finalize_bound_update_stmt,
    materialize_delete_row_events, materialize_insert_rows, materialize_update_row_events,
};
use crate::backend::commands::trigger::{RuntimeTriggers, relation_has_instead_row_trigger};
use crate::backend::executor::{
    ExecError, ExecutorContext, SessionReplicationRole, StatementResult, TupleSlot, Value,
    eval_expr, execute_planned_stmt,
};
use crate::backend::optimizer::finalize_expr_subqueries;
use crate::backend::parser::{
    AlterRuleRenameStatement, AlterTableRuleStateStatement, AlterTableTriggerMode,
    BoundAssignmentTarget, CatalogLookup, CommentOnRuleStatement, CreateRuleStatement, CteBody,
    DropRuleStatement, FromItem, ParseError, RuleDoKind, RuleEvent, SelectItem, SelectStatement,
    Statement, bind_rule_action_statement, bind_rule_qual, cte_body_references_table,
    delete_statement_references_table, insert_statement_references_table,
    merge_statement_references_table, rewrite_bound_delete_auto_view_target,
    rewrite_bound_insert_auto_view_target, rewrite_bound_update_auto_view_target,
    select_statement_references_table, update_statement_references_table, validate_rule_definition,
};
use crate::backend::rewrite::split_stored_rule_action_sql;
use crate::backend::rewrite::{ViewDmlEvent, ViewDmlRewriteError};
use crate::backend::storage::lmgr::TableLockMode;
use crate::include::catalog::PgRewriteRow;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, TableSampleClause,
};
use crate::include::nodes::plannodes::{
    ExecParamSource, IndexScanKey, IndexScanKeyArgument, PartitionPrunePlan, Plan, PlannedStmt,
};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, CaseExpr, Expr, ExprArraySubscript, FuncExpr, OpExpr, OrderByEntry,
    ProjectSetTarget, QueryColumn, RULE_NEW_VAR, RULE_OLD_VAR, RelationDesc, ScalarArrayOpExpr,
    SetReturningExpr, SubLink, SubPlan, TargetEntry, WindowClause, WindowFrame, WindowFrameBound,
    WindowFuncExpr, WindowFuncKind, XmlExpr, attrno_index,
};
use crate::pgrust::database::TransactionWaiter;
use crate::pgrust::database::ddl::map_catalog_error;
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_rule_relation_for_ddl};
use crate::pgrust::database::foreign_keys::TableLockRequest;
use crate::pgrust::database::{AutoCommitGuard, Database, queue_pending_notification};
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

        let existing_rule =
            lookup_rule_row(&catalog, relation.relation_oid, &create_stmt.rule_name);
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
        let effect = if let Some(existing_rule) = existing_rule {
            catalog_guard
                .replace_rule_mvcc_with_dependencies(
                    existing_rule.oid,
                    relation.relation_oid,
                    create_stmt.rule_name.clone(),
                    rule_event_code(create_stmt.event),
                    create_stmt.do_kind == RuleDoKind::Instead,
                    ev_qual,
                    ev_action,
                    RuleDependencies::from_relation_oids(&referenced_relation_oids),
                    RuleOwnerDependency::Auto,
                    &ctx,
                )
                .map_err(map_catalog_error)?
        } else {
            catalog_guard
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
                .map_err(map_catalog_error)?
        };
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_rule_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRuleRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &alter_stmt.relation_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_rule_rename_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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

    pub(crate) fn execute_alter_rule_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRuleRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &alter_stmt.relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.relation_name)?;
        let mut rewrite = lookup_rule_row(&catalog, relation.relation_oid, &alter_stmt.rule_name)
            .ok_or_else(|| {
            missing_rule_error(&alter_stmt.rule_name, &alter_stmt.relation_name)
        })?;
        if rewrite.rulename == "_RETURN" {
            return Err(ExecError::DetailedError {
                message: "renaming an ON SELECT rule is not allowed".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if let Some(existing) =
            lookup_rule_row(&catalog, relation.relation_oid, &alter_stmt.new_rule_name)
            && existing.oid != rewrite.oid
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "rule \"{}\" for relation \"{}\" already exists",
                    alter_stmt.new_rule_name, alter_stmt.relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        rewrite.rulename = alter_stmt.new_rule_name.clone();

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
            .replace_rule_row_mvcc(rewrite, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_rule_state_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableRuleStateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
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
        let result = self.execute_alter_table_rule_state_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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

    pub(crate) fn execute_alter_table_rule_state_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableRuleStateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_rule_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let mut rewrite =
            lookup_rule_row(&catalog, relation.relation_oid, &alter_stmt.rule_name)
                .ok_or_else(|| missing_rule_error(&alter_stmt.rule_name, &alter_stmt.table_name))?;
        rewrite.ev_enabled = rule_enabled_char(alter_stmt.mode);

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
            .replace_rule_row_mvcc(rewrite, &ctx)
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
            let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            push_missing_rule_relation_notice(&catalog, &drop_stmt.relation_name);
            Ok(StatementResult::AffectedRows(0))
        } else {
            let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            Err(missing_rule_relation_error(
                &catalog,
                &drop_stmt.relation_name,
            ))
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
            Err(_err) if drop_stmt.if_exists => {
                push_missing_rule_relation_notice(&catalog, &drop_stmt.relation_name);
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(ExecError::Parse(
                ParseError::TableDoesNotExist(_) | ParseError::UnknownTable(_),
            )) => {
                return Err(missing_rule_relation_error(
                    &catalog,
                    &drop_stmt.relation_name,
                ));
            }
            Err(err) => return Err(err),
        };
        ensure_relation_owner(self, client_id, &relation, &drop_stmt.relation_name)?;
        let Some(rewrite) = lookup_rule_row(&catalog, relation.relation_oid, &drop_stmt.rule_name)
        else {
            return if drop_stmt.if_exists {
                crate::backend::utils::misc::notices::push_notice(format!(
                    "rule \"{}\" for relation \"{}\" does not exist, skipping",
                    drop_stmt.rule_name, drop_stmt.relation_name
                ));
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
                    "cannot drop rule _RETURN on view {} because view {} requires it",
                    drop_stmt.relation_name, drop_stmt.relation_name
                ),
                detail: None,
                hint: Some(format!(
                    "You can drop view {} instead.",
                    drop_stmt.relation_name
                )),
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
        let detail = match relation.relkind {
            'p' => "This operation is not supported for partitioned tables.",
            'r' => "This operation is not supported for tables.",
            _ => "This operation is not supported for this relation.",
        };
        return Err(ExecError::DetailedError {
            message: format!(
                "relation \"{}\" cannot have ON SELECT rules",
                create_stmt.relation_name
            ),
            detail: Some(detail.into()),
            hint: None,
            sqlstate: "42809",
        });
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
    if lookup_rule_row(catalog, relation.relation_oid, &create_stmt.rule_name).is_some()
        && !create_stmt.replace_existing
    {
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

pub(crate) fn enforce_modifying_cte_rule_restrictions(
    relation_oid: u32,
    event: RuleEvent,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for row in catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| row.rulename != "_RETURN" && row.ev_type == rule_event_code(event))
    {
        if !row.ev_qual.is_empty() && row.is_instead {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH"
                    .into(),
            )));
        }
        if !row.is_instead {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "DO ALSO rules are not supported for data-modifying statements in WITH".into(),
            )));
        }

        let action_sql = split_stored_rule_action_sql(&row.ev_action);
        if action_sql.is_empty() {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH"
                    .into(),
            )));
        }
        if action_sql.len() > 1 {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH"
                    .into(),
            )));
        }

        let statement = crate::backend::parser::parse_statement(action_sql[0])?;
        match statement {
            Statement::Notify(_) => {
                return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                    "DO INSTEAD NOTIFY rules are not supported for data-modifying statements in WITH"
                        .into(),
                )));
            }
            Statement::Insert(insert)
                if matches!(
                    insert.source,
                    crate::backend::parser::InsertSource::Select(_)
                ) =>
            {
                return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                    "INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH"
                        .into(),
                )));
            }
            _ => {}
        }
    }
    Ok(())
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

fn rule_enabled_char(mode: AlterTableTriggerMode) -> char {
    match mode {
        AlterTableTriggerMode::Disable => 'D',
        AlterTableTriggerMode::EnableOrigin => 'O',
        AlterTableTriggerMode::EnableReplica => 'R',
        AlterTableTriggerMode::EnableAlways => 'A',
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

fn push_missing_rule_relation_notice(catalog: &dyn CatalogLookup, relation_name: &str) {
    if let Some((schema_name, _)) = relation_name.split_once('.')
        && !catalog
            .namespace_rows()
            .into_iter()
            .any(|row| row.nspname.eq_ignore_ascii_case(schema_name))
    {
        crate::backend::utils::misc::notices::push_notice(format!(
            "schema \"{schema_name}\" does not exist, skipping"
        ));
        return;
    }
    crate::backend::utils::misc::notices::push_notice(format!(
        "relation \"{}\" does not exist, skipping",
        relation_name.rsplit('.').next().unwrap_or(relation_name)
    ));
}

fn missing_rule_relation_error(catalog: &dyn CatalogLookup, relation_name: &str) -> ExecError {
    if let Some((schema_name, _)) = relation_name.split_once('.')
        && !catalog
            .namespace_rows()
            .into_iter()
            .any(|row| row.nspname.eq_ignore_ascii_case(schema_name))
    {
        return ExecError::DetailedError {
            message: format!("schema \"{schema_name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "3F000",
        };
    }
    ExecError::Parse(ParseError::UnknownTable(relation_name.to_string()))
}

#[derive(Clone)]
struct PreparedRule {
    is_instead: bool,
    owner_oid: u32,
    qual: Option<crate::backend::executor::Expr>,
    qual_subplans: Vec<Plan>,
    actions: Vec<crate::backend::parser::BoundRuleAction>,
    actions_reference_old_new: bool,
}

#[derive(Default)]
struct RuleMatchOutcome {
    matched_instead: bool,
    matched_actions: bool,
    returning_seen: bool,
    returning_rows: Vec<Vec<Value>>,
    query_columns: Option<Vec<QueryColumn>>,
    query_column_names: Vec<String>,
    query_rows: Vec<Vec<Value>>,
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
    if (stmt.on_conflict.is_some() || stmt.raw_on_conflict.is_some())
        && (relation_has_active_user_rules_for_event(
            stmt.relation_oid,
            RuleEvent::Insert,
            catalog,
            ctx.session_replication_role,
        ) || relation_has_active_user_rules_for_event(
            stmt.relation_oid,
            RuleEvent::Update,
            catalog,
            ctx.session_replication_role,
        ))
    {
        return Err(ExecError::DetailedError {
            message:
                "INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules"
                    .into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let has_user_rules = relation_has_active_user_rules_for_event(
        stmt.relation_oid,
        RuleEvent::Insert,
        catalog,
        ctx.session_replication_role,
    );
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
    check_relation_privilege_requirements(ctx, &stmt.required_privileges)?;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let rules = load_prepared_rules(
            stmt.relation_oid,
            RuleEvent::Insert,
            &stmt.desc,
            catalog,
            ctx.session_replication_role,
        )?;
        let rows = materialize_insert_rows(&stmt, catalog, ctx)?;
        if stmt.relkind == 'v' && rules.iter().all(|rule| !rule.is_instead) {
            let view_name = stmt.relation_name.clone();
            let auto_stmt = rewrite_bound_insert_auto_view_target(stmt.clone(), catalog)
                .map_err(|err| auto_view_prepare_error(&view_name, ViewDmlEvent::Insert, err))?;
            let auto_result = execute_bound_insert_with_rules(auto_stmt, catalog, ctx, xid, cid)?;
            let null_old = vec![Value::Null; stmt.desc.columns.len()];
            let mut query_columns = None;
            let mut query_column_names = Vec::new();
            let mut query_rows = Vec::new();
            ctx.snapshot.current_cid = ctx.snapshot.current_cid.max(cid.saturating_add(1));
            for row in &rows {
                let mut outcome = execute_matching_rules(
                    &rules,
                    &null_old,
                    row,
                    catalog,
                    ctx,
                    xid,
                    cid.saturating_add(1),
                    None,
                    false,
                )?;
                append_rule_query_output(
                    &mut query_columns,
                    &mut query_column_names,
                    &mut query_rows,
                    &mut outcome,
                );
            }
            if let Some(columns) = query_columns {
                return Ok(StatementResult::Query {
                    columns,
                    column_names: query_column_names,
                    rows: query_rows,
                });
            }
            return Ok(auto_result);
        }
        // :HACK: PostgreSQL's rule rewriter duplicates the original INSERT
        // expressions into DO ALSO rule actions. Until pgrust stores and
        // executes rewritten query trees, re-evaluate the INSERT source for
        // table DO ALSO rules so volatile defaults such as serial nextval()
        // behave like the duplicated rule expansion.
        let rule_new_rows =
            if matches!(stmt.relkind, 'r' | 'p') && rules.iter().any(|rule| !rule.is_instead) {
                materialize_insert_rows(&stmt, catalog, ctx)?
            } else {
                rows.clone()
            };
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        let mut query_columns = None;
        let mut query_column_names = Vec::new();
        let mut query_rows = Vec::new();
        let null_old = vec![Value::Null; stmt.desc.columns.len()];

        for (row, rule_new_row) in rows.into_iter().zip(rule_new_rows.into_iter()) {
            let mut outcome = execute_matching_rules(
                &rules,
                &null_old,
                &rule_new_row,
                catalog,
                ctx,
                xid,
                cid,
                None,
                !stmt.returning.is_empty(),
            )?;
            append_rule_query_output(
                &mut query_columns,
                &mut query_column_names,
                &mut query_rows,
                &mut outcome,
            );
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
                    None,
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
            if let Some(columns) = query_columns {
                return Ok(StatementResult::Query {
                    columns,
                    column_names: query_column_names,
                    rows: query_rows,
                });
            }
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
                    row.ev_type == rule_event_code(RuleEvent::Update)
                        && row.rulename != "_RETURN"
                        && rule_enabled_for_session(&row, ctx.session_replication_role)
                })
    });
    if !has_rule_target {
        return execute_update_with_waiter(stmt, catalog, ctx, xid, cid, waiter);
    }

    let stmt = finalize_bound_update_stmt(stmt, catalog);
    check_relation_privilege_requirements(ctx, &stmt.required_privileges)?;
    if let Some(input_plan) = &stmt.input_plan {
        check_planned_stmt_select_privileges(input_plan, ctx)?;
    }
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        let mut query_columns = None;
        let mut query_column_names = Vec::new();
        let mut query_rows = Vec::new();
        let joined_update_events = if stmt.input_plan.is_some()
            && stmt.targets.iter().any(|target| target.relkind != 'v')
        {
            Some(materialize_update_row_events(&stmt, ctx)?)
        } else {
            None
        };
        for target in &stmt.targets {
            let view_has_user_rules = relation_has_active_user_rules_for_event(
                target.relation_oid,
                RuleEvent::Update,
                catalog,
                ctx.session_replication_role,
            );
            if target.relkind == 'v'
                && !view_has_user_rules
                && relation_has_instead_row_trigger(
                    catalog,
                    target.relation_oid,
                    TriggerOperation::Update,
                )
            {
                let (target_affected_rows, mut target_returned_rows) =
                    execute_view_update_with_triggers(
                        &stmt,
                        target,
                        &stmt.returning,
                        catalog,
                        ctx,
                    )?;
                affected_rows += target_affected_rows;
                returned_rows.append(&mut target_returned_rows);
                continue;
            }
            let rules = load_prepared_rules(
                target.relation_oid,
                RuleEvent::Update,
                &target.desc,
                catalog,
                ctx.session_replication_role,
            )?;
            if target.relkind == 'v' {
                if !view_has_user_rules {
                    return Err(missing_view_instead_trigger_error(
                        &target.relation_name,
                        ViewDmlEvent::Update,
                    ));
                }
                if rules.iter().all(|rule| !rule.is_instead) {
                    let events =
                        materialize_view_update_events_for_stmt(&stmt, target, catalog, ctx)?;
                    let view_name = target.relation_name.clone();
                    let auto_stmt = rewrite_bound_update_auto_view_target(stmt.clone(), catalog)
                        .map_err(|err| {
                            auto_view_prepare_error(&view_name, ViewDmlEvent::Update, err)
                        })?;
                    match execute_bound_update_with_rules(
                        auto_stmt, catalog, ctx, xid, cid, waiter,
                    )? {
                        StatementResult::AffectedRows(rows) => affected_rows += rows,
                        StatementResult::Query { rows, .. } => returned_rows.extend(rows),
                    }
                    ctx.snapshot.current_cid = ctx.snapshot.current_cid.max(cid.saturating_add(1));
                    for event in events {
                        let mut outcome = execute_matching_rules(
                            &rules,
                            &event.old_values,
                            &event.new_values,
                            catalog,
                            ctx,
                            xid,
                            cid.saturating_add(1),
                            waiter,
                            false,
                        )?;
                        append_rule_query_output(
                            &mut query_columns,
                            &mut query_column_names,
                            &mut query_rows,
                            &mut outcome,
                        );
                    }
                    continue;
                }
                for event in materialize_view_update_events_for_stmt(&stmt, target, catalog, ctx)? {
                    let mut outcome = execute_matching_rules(
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
                    append_rule_query_output(
                        &mut query_columns,
                        &mut query_column_names,
                        &mut query_rows,
                        &mut outcome,
                    );
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
                        let returning_rows =
                            append_update_from_source_rows(&outcome.returning_rows, &event);
                        returned_rows.extend(project_statement_returning_rows(
                            &stmt.returning,
                            &returning_rows,
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
                        current_of: None,
                    },
                    ctx,
                )?
            };

            for event in target_events {
                let mut outcome = execute_matching_rules(
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
                append_rule_query_output(
                    &mut query_columns,
                    &mut query_column_names,
                    &mut query_rows,
                    &mut outcome,
                );
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
            if let Some(columns) = query_columns {
                return Ok(StatementResult::Query {
                    columns,
                    column_names: query_column_names,
                    rows: query_rows,
                });
            }
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
                    row.ev_type == rule_event_code(RuleEvent::Delete)
                        && row.rulename != "_RETURN"
                        && rule_enabled_for_session(&row, ctx.session_replication_role)
                })
    });
    if !has_rule_target {
        return execute_delete_with_waiter(stmt, catalog, ctx, xid, waiter);
    }

    let stmt = finalize_bound_delete_stmt(stmt, catalog);
    check_relation_privilege_requirements(ctx, &stmt.required_privileges)?;
    if let Some(input_plan) = &stmt.input_plan {
        check_planned_stmt_select_privileges(input_plan, ctx)?;
    }
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0usize;
        let mut returned_rows = Vec::new();
        let mut query_columns = None;
        let mut query_column_names = Vec::new();
        let mut query_rows = Vec::new();
        if stmt.input_plan.is_some() && stmt.targets.iter().all(|target| target.relkind != 'v') {
            for event in materialize_delete_row_events(&stmt, ctx)? {
                let target = event.target;
                let rules = load_prepared_rules(
                    target.relation_oid,
                    RuleEvent::Delete,
                    &target.desc,
                    catalog,
                    ctx.session_replication_role,
                )?;
                let null_new = vec![Value::Null; target.desc.columns.len()];
                let mut outcome = execute_matching_rules(
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
                append_rule_query_output(
                    &mut query_columns,
                    &mut query_column_names,
                    &mut query_rows,
                    &mut outcome,
                );
                if outcome.matched_instead {
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
                    continue;
                }
                if apply_base_delete_row(&target, event.tid, event.old_values, ctx, xid, waiter)? {
                    if !stmt.returning.is_empty() {
                        returned_rows.push(project_statement_returning_row(
                            &stmt.returning,
                            &event.returning_values,
                            ctx,
                        )?);
                    }
                    affected_rows += 1;
                }
            }
            return if stmt.returning.is_empty() {
                if let Some(columns) = query_columns {
                    return Ok(StatementResult::Query {
                        columns,
                        column_names: query_column_names,
                        rows: query_rows,
                    });
                }
                Ok(StatementResult::AffectedRows(affected_rows))
            } else {
                Ok(build_statement_returning_result(
                    &stmt.returning,
                    returned_rows,
                ))
            };
        }
        for target in &stmt.targets {
            let view_has_user_rules = relation_has_active_user_rules_for_event(
                target.relation_oid,
                RuleEvent::Delete,
                catalog,
                ctx.session_replication_role,
            );
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
                ctx.session_replication_role,
            )?;
            if let Some(outcome) = execute_statement_level_instead_rules(
                &rules,
                target.desc.columns.len(),
                catalog,
                ctx,
                xid,
                0,
                waiter,
                !stmt.returning.is_empty(),
            )? {
                affected_rows += append_statement_level_instead_result(
                    RuleEvent::Delete,
                    &target.relation_name,
                    &stmt.returning,
                    outcome,
                    ctx,
                    &mut returned_rows,
                )?;
                continue;
            }
            if target.relkind == 'v' {
                if !view_has_user_rules {
                    return Err(missing_view_instead_trigger_error(
                        &target.relation_name,
                        ViewDmlEvent::Delete,
                    ));
                }
                for old_values in materialize_view_delete_events(target, catalog, ctx)? {
                    let mut outcome = execute_matching_rules(
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
                    append_rule_query_output(
                        &mut query_columns,
                        &mut query_column_names,
                        &mut query_rows,
                        &mut outcome,
                    );
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
                    current_of: None,
                },
                ctx,
            )? {
                let null_new = vec![Value::Null; event.target.desc.columns.len()];
                let mut outcome = execute_matching_rules(
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
                append_rule_query_output(
                    &mut query_columns,
                    &mut query_column_names,
                    &mut query_rows,
                    &mut outcome,
                );
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
            if let Some(columns) = query_columns {
                return Ok(StatementResult::Query {
                    columns,
                    column_names: query_column_names,
                    rows: query_rows,
                });
            }
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
            && !evaluate_rule_qual(qual, old_values, new_values, rule.owner_oid, catalog, ctx)?
        {
            continue;
        }
        outcome.matched_instead |= rule.is_instead;
        if !rule.actions.is_empty() {
            outcome.matched_actions = true;
        }
        for action in &rule.actions {
            let action = substitute_rule_action(action.clone(), old_values, new_values);
            let result = with_rule_owner(ctx, rule.owner_oid, |ctx| {
                with_rule_bindings(ctx, old_values, new_values, |ctx| {
                    execute_rule_action(&action, catalog, ctx, xid, cid, waiter)
                })
            })?;
            if capture_returning && rule_action_has_returning(&action) {
                if outcome.returning_seen {
                    return Err(multiple_rule_returning_error());
                }
                outcome.returning_seen = true;
                outcome.returning_rows = extract_rule_action_returning_rows(result)?;
            } else if rule_action_returns_query_output(&action) {
                capture_rule_action_query_output(result, &mut outcome);
            }
        }
    }
    Ok(outcome)
}

fn rule_action_returns_query_output(action: &crate::backend::parser::BoundRuleAction) -> bool {
    matches!(
        action,
        crate::backend::parser::BoundRuleAction::Select(_)
            | crate::backend::parser::BoundRuleAction::Values(_)
    )
}

fn capture_rule_action_query_output(result: StatementResult, outcome: &mut RuleMatchOutcome) {
    let StatementResult::Query {
        columns,
        column_names,
        mut rows,
    } = result
    else {
        return;
    };
    if outcome.query_columns.is_none() {
        outcome.query_columns = Some(columns);
        outcome.query_column_names = column_names;
    }
    outcome.query_rows.append(&mut rows);
}

fn append_rule_query_output(
    columns: &mut Option<Vec<QueryColumn>>,
    column_names: &mut Vec<String>,
    rows: &mut Vec<Vec<Value>>,
    outcome: &mut RuleMatchOutcome,
) {
    if columns.is_none() {
        *columns = outcome.query_columns.take();
        if columns.is_some() {
            *column_names = std::mem::take(&mut outcome.query_column_names);
        }
    }
    rows.append(&mut outcome.query_rows);
}

fn execute_statement_level_instead_rules(
    rules: &[PreparedRule],
    row_width: usize,
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
) -> Result<Option<RuleMatchOutcome>, ExecError> {
    if rules.is_empty()
        || !rules
            .iter()
            .all(|rule| rule.is_instead && rule.qual.is_none() && !rule.actions_reference_old_new)
    {
        return Ok(None);
    }
    let null_tuple = vec![Value::Null; row_width];
    execute_matching_rules(
        rules,
        &null_tuple,
        &null_tuple,
        catalog,
        ctx,
        xid,
        cid,
        waiter,
        capture_returning,
    )
    .map(Some)
}

fn append_statement_level_instead_result(
    event: RuleEvent,
    relation_name: &str,
    returning: &[TargetEntry],
    outcome: RuleMatchOutcome,
    ctx: &mut ExecutorContext,
    returned_rows: &mut Vec<Vec<Value>>,
) -> Result<usize, ExecError> {
    if !returning.is_empty() {
        if !outcome.returning_seen {
            return Err(missing_rule_returning_error(event, relation_name));
        }
        returned_rows.extend(project_statement_returning_rows(
            returning,
            &outcome.returning_rows,
            ctx,
        )?);
    }
    Ok(usize::from(outcome.matched_actions))
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
    stmt: &crate::backend::parser::BoundUpdateStatement,
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
    for event in materialize_view_update_events_for_stmt(stmt, target, catalog, ctx)? {
        let Some(returned_row) =
            triggers.instead_row_update(&event.old_values, event.new_values, ctx)?
        else {
            continue;
        };
        if !returning.is_empty() {
            let mut returned_row = returned_row;
            returned_row.extend(event.source_values.iter().cloned());
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
        crate::backend::parser::BoundRuleAction::Select(_)
        | crate::backend::parser::BoundRuleAction::Values(_)
        | crate::backend::parser::BoundRuleAction::Notify(_) => false,
        crate::backend::parser::BoundRuleAction::Sequence(actions) => {
            actions.last().is_some_and(rule_action_has_returning)
        }
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
            let prepared = prepare_bound_insert_for_execution(stmt.clone(), catalog)?;
            execute_bound_insert_with_rules(prepared.stmt, catalog, ctx, xid, cid)
        }
        crate::backend::parser::BoundRuleAction::Update(stmt) => {
            let prepared = prepare_bound_update_for_execution(stmt.clone(), catalog)?;
            execute_bound_update_with_rules(prepared.stmt, catalog, ctx, xid, cid, waiter)
        }
        crate::backend::parser::BoundRuleAction::Delete(stmt) => {
            let prepared = prepare_bound_delete_for_execution(stmt.clone(), catalog)?;
            execute_bound_delete_with_rules(prepared.stmt, catalog, ctx, xid, waiter)
        }
        crate::backend::parser::BoundRuleAction::Sequence(actions) => {
            let mut result = StatementResult::AffectedRows(0);
            for action in actions {
                result = execute_rule_action(action, catalog, ctx, xid, cid, waiter)?;
            }
            Ok(result)
        }
        crate::backend::parser::BoundRuleAction::Select(planned)
        | crate::backend::parser::BoundRuleAction::Values(planned) => {
            execute_planned_stmt(planned.clone(), ctx)
        }
        crate::backend::parser::BoundRuleAction::Notify(stmt) => {
            queue_pending_notification(
                &mut ctx.pending_async_notifications,
                &stmt.channel,
                stmt.payload.as_deref().unwrap_or(""),
            )?;
            Ok(StatementResult::AffectedRows(0))
        }
    }
}

fn evaluate_rule_qual(
    qual: &crate::backend::executor::Expr,
    old_values: &[Value],
    new_values: &[Value],
    owner_oid: u32,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut subplans = Vec::new();
    let qual = finalize_expr_subqueries(
        substitute_rule_expr(qual.clone(), old_values, new_values),
        catalog,
        &mut subplans,
    );
    let saved_subplans = std::mem::replace(&mut ctx.subplans, subplans);
    let result = with_rule_owner(ctx, owner_oid, |ctx| {
        with_rule_bindings(ctx, old_values, new_values, |ctx| {
            let mut slot = TupleSlot::virtual_row(Vec::new());
            match eval_expr(&qual, &mut slot, ctx)? {
                Value::Bool(value) => Ok(value),
                Value::Null => Ok(false),
                other => Err(ExecError::NonBoolQual(other)),
            }
        })
    });
    ctx.subplans = saved_subplans;
    result
}

fn with_rule_bindings<T>(
    ctx: &mut ExecutorContext,
    old_values: &[Value],
    new_values: &[Value],
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let saved_old = ctx.expr_bindings.rule_old_tuple.clone();
    let saved_new = ctx.expr_bindings.rule_new_tuple.clone();
    ctx.expr_bindings.rule_old_tuple = Some(old_values.to_vec());
    ctx.expr_bindings.rule_new_tuple = Some(new_values.to_vec());
    let result = f(ctx);
    ctx.expr_bindings.rule_old_tuple = saved_old;
    ctx.expr_bindings.rule_new_tuple = saved_new;
    result
}

fn with_rule_owner<T>(
    ctx: &mut ExecutorContext,
    owner_oid: u32,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let saved_user = ctx.current_user_oid;
    ctx.current_user_oid = owner_oid;
    let result = f(ctx);
    ctx.current_user_oid = saved_user;
    result
}

fn substitute_rule_tuple_var(
    varno: usize,
    varattno: i32,
    old_values: &[Value],
    new_values: &[Value],
) -> Option<Value> {
    let index = attrno_index(varattno)?;
    match varno {
        RULE_OLD_VAR => Some(old_values.get(index).cloned().unwrap_or(Value::Null)),
        RULE_NEW_VAR => Some(new_values.get(index).cloned().unwrap_or(Value::Null)),
        _ => None,
    }
}

fn substitute_rule_expr(expr: Expr, old_values: &[Value], new_values: &[Value]) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => {
            substitute_rule_tuple_var(var.varno, var.varattno, old_values, new_values)
                .map(Expr::Const)
                .unwrap_or(Expr::Var(var))
        }
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(substitute_rule_aggref(
            *aggref, old_values, new_values,
        ))),
        Expr::WindowFunc(func) => Expr::WindowFunc(Box::new(substitute_rule_window_func(
            *func, old_values, new_values,
        ))),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(substitute_rule_expr(*arg, old_values, new_values))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: substitute_rule_expr(arm.expr, old_values, new_values),
                    result: substitute_rule_expr(arm.result, old_values, new_values),
                })
                .collect(),
            defresult: Box::new(substitute_rule_expr(
                *case_expr.defresult,
                old_values,
                new_values,
            )),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => Expr::SqlJsonQueryFunction(Box::new(
            (*func).map_exprs(|expr| substitute_rule_expr(expr, old_values, new_values)),
        )),
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: srf
                .call
                .map_exprs(|expr| substitute_rule_expr(expr, old_values, new_values)),
            ..*srf
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(substitute_rule_expr(*expr, old_values, new_values))),
            subselect: Box::new(substitute_rule_query(
                *sublink.subselect,
                old_values,
                new_values,
            )),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(substitute_rule_expr(*expr, old_values, new_values))),
            args: subplan
                .args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            ..*subplan
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(substitute_rule_expr(*saop.left, old_values, new_values)),
            right: Box::new(substitute_rule_expr(*saop.right, old_values, new_values)),
            ..*saop
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| substitute_rule_expr(arg, old_values, new_values))
                .collect(),
            ..*xml
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(substitute_rule_expr(*inner, old_values, new_values)),
            ty,
        ),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(substitute_rule_expr(*expr, old_values, new_values)),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(substitute_rule_expr(*expr, old_values, new_values)),
            pattern: Box::new(substitute_rule_expr(*pattern, old_values, new_values)),
            escape: escape
                .map(|expr| Box::new(substitute_rule_expr(*expr, old_values, new_values))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(substitute_rule_expr(*expr, old_values, new_values)),
            pattern: Box::new(substitute_rule_expr(*pattern, old_values, new_values)),
            escape: escape
                .map(|expr| Box::new(substitute_rule_expr(*expr, old_values, new_values))),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_rule_expr(
            *inner, old_values, new_values,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(substitute_rule_expr(
            *inner, old_values, new_values,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(substitute_rule_expr(*left, old_values, new_values)),
            Box::new(substitute_rule_expr(*right, old_values, new_values)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(substitute_rule_expr(*left, old_values, new_values)),
            Box::new(substitute_rule_expr(*right, old_values, new_values)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, substitute_rule_expr(expr, old_values, new_values)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(substitute_rule_expr(*expr, old_values, new_values)),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(substitute_rule_expr(*left, old_values, new_values)),
            Box::new(substitute_rule_expr(*right, old_values, new_values)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(substitute_rule_expr(*array, old_values, new_values)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    substitute_rule_expr_array_subscript(subscript, old_values, new_values)
                })
                .collect(),
        },
        other => other,
    }
}

fn substitute_rule_expr_array_subscript(
    subscript: ExprArraySubscript,
    old_values: &[Value],
    new_values: &[Value],
) -> ExprArraySubscript {
    ExprArraySubscript {
        lower: subscript
            .lower
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        upper: subscript
            .upper
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        is_slice: subscript.is_slice,
    }
}

fn substitute_rule_order_by(
    item: OrderByEntry,
    old_values: &[Value],
    new_values: &[Value],
) -> OrderByEntry {
    OrderByEntry {
        expr: substitute_rule_expr(item.expr, old_values, new_values),
        ..item
    }
}

fn substitute_rule_sort_group(
    item: crate::include::nodes::primnodes::SortGroupClause,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::include::nodes::primnodes::SortGroupClause {
    crate::include::nodes::primnodes::SortGroupClause {
        expr: substitute_rule_expr(item.expr, old_values, new_values),
        ..item
    }
}

fn substitute_rule_target(
    target: TargetEntry,
    old_values: &[Value],
    new_values: &[Value],
) -> TargetEntry {
    TargetEntry {
        expr: substitute_rule_expr(target.expr, old_values, new_values),
        ..target
    }
}

fn substitute_rule_agg_accum(
    accum: AggAccum,
    old_values: &[Value],
    new_values: &[Value],
) -> AggAccum {
    AggAccum {
        direct_args: accum
            .direct_args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        args: accum
            .args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| substitute_rule_order_by(item, old_values, new_values))
            .collect(),
        filter: accum
            .filter
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        ..accum
    }
}

fn substitute_rule_aggref(aggref: Aggref, old_values: &[Value], new_values: &[Value]) -> Aggref {
    Aggref {
        direct_args: aggref
            .direct_args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        args: aggref
            .args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        aggorder: aggref
            .aggorder
            .into_iter()
            .map(|item| substitute_rule_order_by(item, old_values, new_values))
            .collect(),
        aggfilter: aggref
            .aggfilter
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        ..aggref
    }
}

fn substitute_rule_window_func(
    func: WindowFuncExpr,
    old_values: &[Value],
    new_values: &[Value],
) -> WindowFuncExpr {
    WindowFuncExpr {
        kind: match func.kind {
            WindowFuncKind::Aggregate(aggref) => {
                WindowFuncKind::Aggregate(substitute_rule_aggref(aggref, old_values, new_values))
            }
            kind => kind,
        },
        args: func
            .args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        ..func
    }
}

fn substitute_rule_window_frame_bound(
    bound: WindowFrameBound,
    old_values: &[Value],
    new_values: &[Value],
) -> WindowFrameBound {
    match bound {
        WindowFrameBound::OffsetPreceding(offset) => {
            let expr = substitute_rule_expr(offset.expr.clone(), old_values, new_values);
            WindowFrameBound::OffsetPreceding(offset.with_expr(expr))
        }
        WindowFrameBound::OffsetFollowing(offset) => {
            let expr = substitute_rule_expr(offset.expr.clone(), old_values, new_values);
            WindowFrameBound::OffsetFollowing(offset.with_expr(expr))
        }
        other => other,
    }
}

fn substitute_rule_window_clause(
    clause: WindowClause,
    old_values: &[Value],
    new_values: &[Value],
) -> WindowClause {
    WindowClause {
        spec: crate::include::nodes::primnodes::WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| substitute_rule_order_by(item, old_values, new_values))
                .collect(),
            frame: WindowFrame {
                start_bound: substitute_rule_window_frame_bound(
                    clause.spec.frame.start_bound,
                    old_values,
                    new_values,
                ),
                end_bound: substitute_rule_window_frame_bound(
                    clause.spec.frame.end_bound,
                    old_values,
                    new_values,
                ),
                ..clause.spec.frame
            },
        },
        functions: clause
            .functions
            .into_iter()
            .map(|func| substitute_rule_window_func(func, old_values, new_values))
            .collect(),
    }
}

fn substitute_rule_table_sample(
    sample: TableSampleClause,
    old_values: &[Value],
    new_values: &[Value],
) -> TableSampleClause {
    TableSampleClause {
        args: sample
            .args
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        repeatable: sample
            .repeatable
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        ..sample
    }
}

fn substitute_rule_rte(
    rte: RangeTblEntry,
    old_values: &[Value],
    new_values: &[Value],
) -> RangeTblEntry {
    let kind = match rte.kind {
        RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample,
        } => RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample: tablesample
                .map(|sample| substitute_rule_table_sample(sample, old_values, new_values)),
        },
        RangeTblEntryKind::Join {
            from_list,
            jointype,
            joinmergedcols,
            joinaliasvars,
            joinleftcols,
            joinrightcols,
        } => RangeTblEntryKind::Join {
            from_list,
            jointype,
            joinmergedcols,
            joinaliasvars: joinaliasvars
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            joinleftcols,
            joinrightcols,
        },
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => RangeTblEntryKind::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
            call: call.map_exprs(|expr| substitute_rule_expr(expr, old_values, new_values)),
        },
        RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
            cte_id,
            query: Box::new(substitute_rule_query(*query, old_values, new_values)),
        },
        RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
            query: Box::new(substitute_rule_query(*query, old_values, new_values)),
        },
        other => other,
    };
    RangeTblEntry {
        security_quals: rte
            .security_quals
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        kind,
        ..rte
    }
}

fn substitute_rule_jointree(
    node: JoinTreeNode,
    old_values: &[Value],
    new_values: &[Value],
) -> JoinTreeNode {
    match node {
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => JoinTreeNode::JoinExpr {
            left: Box::new(substitute_rule_jointree(*left, old_values, new_values)),
            right: Box::new(substitute_rule_jointree(*right, old_values, new_values)),
            kind,
            quals: substitute_rule_expr(quals, old_values, new_values),
            rtindex,
        },
        other => other,
    }
}

fn substitute_rule_query(mut query: Query, old_values: &[Value], new_values: &[Value]) -> Query {
    query.rtable = query
        .rtable
        .into_iter()
        .map(|rte| substitute_rule_rte(rte, old_values, new_values))
        .collect();
    query.jointree = query
        .jointree
        .map(|node| substitute_rule_jointree(node, old_values, new_values));
    query.target_list = query
        .target_list
        .into_iter()
        .map(|target| substitute_rule_target(target, old_values, new_values))
        .collect();
    query.where_qual = query
        .where_qual
        .map(|expr| substitute_rule_expr(expr, old_values, new_values));
    query.group_by = query
        .group_by
        .into_iter()
        .map(|expr| substitute_rule_expr(expr, old_values, new_values))
        .collect();
    query.accumulators = query
        .accumulators
        .into_iter()
        .map(|accum| substitute_rule_agg_accum(accum, old_values, new_values))
        .collect();
    query.window_clauses = query
        .window_clauses
        .into_iter()
        .map(|clause| substitute_rule_window_clause(clause, old_values, new_values))
        .collect();
    query.having_qual = query
        .having_qual
        .map(|expr| substitute_rule_expr(expr, old_values, new_values));
    query.sort_clause = query
        .sort_clause
        .into_iter()
        .map(|item| substitute_rule_sort_group(item, old_values, new_values))
        .collect();
    query.recursive_union = query.recursive_union.map(|union| {
        Box::new(crate::include::nodes::parsenodes::RecursiveUnionQuery {
            anchor: substitute_rule_query(union.anchor, old_values, new_values),
            recursive: substitute_rule_query(union.recursive, old_values, new_values),
            ..*union
        })
    });
    query.set_operation = query.set_operation.map(|setop| {
        Box::new(crate::include::nodes::parsenodes::SetOperationQuery {
            inputs: setop
                .inputs
                .into_iter()
                .map(|input| substitute_rule_query(input, old_values, new_values))
                .collect(),
            ..*setop
        })
    });
    query
}

fn substitute_rule_exec_param(
    param: ExecParamSource,
    old_values: &[Value],
    new_values: &[Value],
) -> ExecParamSource {
    ExecParamSource {
        expr: substitute_rule_expr(param.expr, old_values, new_values),
        ..param
    }
}

fn substitute_rule_index_key(
    key: IndexScanKey,
    old_values: &[Value],
    new_values: &[Value],
) -> IndexScanKey {
    IndexScanKey {
        argument: match key.argument {
            IndexScanKeyArgument::Runtime(expr) => {
                IndexScanKeyArgument::Runtime(substitute_rule_expr(expr, old_values, new_values))
            }
            other => other,
        },
        display_expr: key
            .display_expr
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        ..key
    }
}

fn substitute_rule_partition_prune(
    info: PartitionPrunePlan,
    old_values: &[Value],
    new_values: &[Value],
) -> PartitionPrunePlan {
    PartitionPrunePlan {
        filter: substitute_rule_expr(info.filter, old_values, new_values),
        ..info
    }
}

fn substitute_rule_project_set_target(
    target: ProjectSetTarget,
    old_values: &[Value],
    new_values: &[Value],
) -> ProjectSetTarget {
    match target {
        ProjectSetTarget::Scalar(target) => {
            ProjectSetTarget::Scalar(substitute_rule_target(target, old_values, new_values))
        }
        ProjectSetTarget::Set {
            name,
            source_expr,
            call,
            sql_type,
            column_index,
            ressortgroupref,
        } => ProjectSetTarget::Set {
            name,
            source_expr: substitute_rule_expr(source_expr, old_values, new_values),
            call: call.map_exprs(|expr| substitute_rule_expr(expr, old_values, new_values)),
            sql_type,
            column_index,
            ressortgroupref,
        },
    }
}

fn substitute_rule_plan(plan: Plan, old_values: &[Value], new_values: &[Value]) -> Plan {
    match plan {
        Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune: partition_prune
                .map(|info| substitute_rule_partition_prune(info, old_values, new_values)),
            children: children
                .into_iter()
                .map(|child| substitute_rule_plan(child, old_values, new_values))
                .collect(),
        },
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items: items
                .into_iter()
                .map(|item| substitute_rule_order_by(item, old_values, new_values))
                .collect(),
            partition_prune: partition_prune
                .map(|info| substitute_rule_partition_prune(info, old_values, new_values)),
            children: children
                .into_iter()
                .map(|child| substitute_rule_plan(child, old_values, new_values))
                .collect(),
        },
        Plan::Unique {
            plan_info,
            key_indices,
            input,
        } => Plan::Unique {
            plan_info,
            key_indices,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
        },
        Plan::IndexOnlyScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
        } => Plan::IndexOnlyScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys: keys
                .into_iter()
                .map(|key| substitute_rule_index_key(key, old_values, new_values))
                .collect(),
            order_by_keys: order_by_keys
                .into_iter()
                .map(|key| substitute_rule_index_key(key, old_values, new_values))
                .collect(),
            direction,
        },
        Plan::IndexScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
            index_only,
        } => Plan::IndexScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys: keys
                .into_iter()
                .map(|key| substitute_rule_index_key(key, old_values, new_values))
                .collect(),
            order_by_keys: order_by_keys
                .into_iter()
                .map(|key| substitute_rule_index_key(key, old_values, new_values))
                .collect(),
            direction,
            index_only,
        },
        Plan::BitmapIndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
        } => Plan::BitmapIndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys: keys
                .into_iter()
                .map(|key| substitute_rule_index_key(key, old_values, new_values))
                .collect(),
            index_quals: index_quals
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::BitmapOr {
            plan_info,
            children,
        } => Plan::BitmapOr {
            plan_info,
            children: children
                .into_iter()
                .map(|child| substitute_rule_plan(child, old_values, new_values))
                .collect(),
        },
        Plan::BitmapAnd {
            plan_info,
            children,
        } => Plan::BitmapAnd {
            plan_info,
            children: children
                .into_iter()
                .map(|child| substitute_rule_plan(child, old_values, new_values))
                .collect(),
        },
        Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
        } => Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual: Box::new(substitute_rule_plan(*bitmapqual, old_values, new_values)),
            recheck_qual: recheck_qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            filter_qual: filter_qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::Materialize { plan_info, input } => Plan::Materialize {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
        },
        Plan::Memoize {
            plan_info,
            input,
            cache_keys,
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        } => Plan::Memoize {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            cache_keys: cache_keys
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        },
        Plan::Gather {
            plan_info,
            input,
            workers_planned,
            single_copy,
        } => Plan::Gather {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            workers_planned,
            single_copy,
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            nest_params,
            join_qual,
            qual,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(substitute_rule_plan(*left, old_values, new_values)),
            right: Box::new(substitute_rule_plan(*right, old_values, new_values)),
            kind,
            nest_params: nest_params
                .into_iter()
                .map(|param| substitute_rule_exec_param(param, old_values, new_values))
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(substitute_rule_plan(*left, old_values, new_values)),
            right: Box::new(substitute_rule_plan(*right, old_values, new_values)),
            kind,
            hash_clauses: hash_clauses
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            merge_key_descending,
            join_qual,
            qual,
        } => Plan::MergeJoin {
            plan_info,
            left: Box::new(substitute_rule_plan(*left, old_values, new_values)),
            right: Box::new(substitute_rule_plan(*right, old_values, new_values)),
            kind,
            merge_clauses: merge_clauses
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            outer_merge_keys: outer_merge_keys
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            inner_merge_keys: inner_merge_keys
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            merge_key_descending,
            join_qual: join_qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            predicate: substitute_rule_expr(predicate, old_values, new_values),
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
            display_items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            items: items
                .into_iter()
                .map(|item| substitute_rule_order_by(item, old_values, new_values))
                .collect(),
            display_items,
        },
        Plan::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        } => Plan::IncrementalSort {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            items: items
                .into_iter()
                .map(|item| substitute_rule_order_by(item, old_values, new_values))
                .collect(),
            presorted_count,
            display_items,
            presorted_display_items,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            targets: targets
                .into_iter()
                .map(|target| substitute_rule_target(target, old_values, new_values))
                .collect(),
        },
        Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input,
            group_by,
            group_by_refs,
            grouping_sets,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            semantic_output_names,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            group_by: group_by
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            group_by_refs,
            grouping_sets,
            passthrough_exprs: passthrough_exprs
                .into_iter()
                .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| substitute_rule_agg_accum(accum, old_values, new_values))
                .collect(),
            semantic_accumulators: semantic_accumulators.map(|accums| {
                accums
                    .into_iter()
                    .map(|accum| substitute_rule_agg_accum(accum, old_values, new_values))
                    .collect()
            }),
            semantic_output_names,
            having: having.map(|expr| substitute_rule_expr(expr, old_values, new_values)),
            output_columns,
        },
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            output_columns,
        } => Plan::WindowAgg {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            clause: substitute_rule_window_clause(clause, old_values, new_values),
            output_columns,
        },
        Plan::FunctionScan {
            plan_info,
            call,
            table_alias,
        } => Plan::FunctionScan {
            plan_info,
            call: call.map_exprs(|expr| substitute_rule_expr(expr, old_values, new_values)),
            table_alias,
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            scan_name,
            filter,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            scan_name,
            filter: filter.map(|expr| substitute_rule_expr(expr, old_values, new_values)),
            output_columns,
        },
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
            cte_plan,
            output_columns,
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
            cte_plan: Box::new(substitute_rule_plan(*cte_plan, old_values, new_values)),
            output_columns,
        },
        Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
            output_columns,
            anchor,
            recursive,
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
            output_columns,
            anchor: Box::new(substitute_rule_plan(*anchor, old_values, new_values)),
            recursive: Box::new(substitute_rule_plan(*recursive, old_values, new_values)),
        },
        Plan::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            children,
        } => Plan::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            children: children
                .into_iter()
                .map(|child| substitute_rule_plan(child, old_values, new_values))
                .collect(),
        },
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            targets: targets
                .into_iter()
                .map(|target| substitute_rule_project_set_target(target, old_values, new_values))
                .collect(),
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            limit,
            offset,
        },
        Plan::LockRows {
            plan_info,
            input,
            row_marks,
        } => Plan::LockRows {
            plan_info,
            input: Box::new(substitute_rule_plan(*input, old_values, new_values)),
            row_marks,
        },
        Plan::TidScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            desc,
            tid_cond,
            filter,
        } => Plan::TidScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            desc,
            tid_cond: crate::include::nodes::plannodes::TidScanCond {
                sources: tid_cond
                    .sources
                    .into_iter()
                    .map(|source| match source {
                        crate::include::nodes::plannodes::TidScanSource::Scalar(expr) => {
                            crate::include::nodes::plannodes::TidScanSource::Scalar(
                                substitute_rule_expr(expr, old_values, new_values),
                            )
                        }
                        crate::include::nodes::plannodes::TidScanSource::Array(expr) => {
                            crate::include::nodes::plannodes::TidScanSource::Array(
                                substitute_rule_expr(expr, old_values, new_values),
                            )
                        }
                    })
                    .collect(),
                display_expr: substitute_rule_expr(tid_cond.display_expr, old_values, new_values),
            },
            filter: filter.map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        },
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::WorkTableScan { .. } => plan,
    }
}

fn substitute_rule_planned_stmt(
    planned: PlannedStmt,
    old_values: &[Value],
    new_values: &[Value],
) -> PlannedStmt {
    PlannedStmt {
        plan_tree: substitute_rule_plan(planned.plan_tree, old_values, new_values),
        subplans: planned
            .subplans
            .into_iter()
            .map(|plan| substitute_rule_plan(plan, old_values, new_values))
            .collect(),
        ext_params: planned
            .ext_params
            .into_iter()
            .map(|param| substitute_rule_exec_param(param, old_values, new_values))
            .collect(),
        ..planned
    }
}

fn substitute_rule_bound_array_subscript(
    subscript: crate::backend::parser::BoundArraySubscript,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundArraySubscript {
    crate::backend::parser::BoundArraySubscript {
        lower: subscript
            .lower
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        upper: subscript
            .upper
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        is_slice: subscript.is_slice,
    }
}

fn substitute_rule_assignment(
    assignment: crate::backend::parser::BoundAssignment,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundAssignment {
    crate::backend::parser::BoundAssignment {
        subscripts: assignment
            .subscripts
            .into_iter()
            .map(|subscript| {
                substitute_rule_bound_array_subscript(subscript, old_values, new_values)
            })
            .collect(),
        expr: substitute_rule_expr(assignment.expr, old_values, new_values),
        ..assignment
    }
}

fn substitute_rule_assignment_target(
    target: BoundAssignmentTarget,
    old_values: &[Value],
    new_values: &[Value],
) -> BoundAssignmentTarget {
    BoundAssignmentTarget {
        subscripts: target
            .subscripts
            .into_iter()
            .map(|subscript| {
                substitute_rule_bound_array_subscript(subscript, old_values, new_values)
            })
            .collect(),
        ..target
    }
}

fn substitute_rule_rls_check(
    check: crate::backend::rewrite::RlsWriteCheck,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::rewrite::RlsWriteCheck {
    crate::backend::rewrite::RlsWriteCheck {
        expr: substitute_rule_expr(check.expr, old_values, new_values),
        ..check
    }
}

fn substitute_rule_on_conflict(
    clause: crate::backend::parser::BoundOnConflictClause,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundOnConflictClause {
    crate::backend::parser::BoundOnConflictClause {
        action: match clause.action {
            crate::backend::parser::BoundOnConflictAction::Update {
                assignments,
                predicate,
                conflict_visibility_checks,
                update_write_checks,
            } => crate::backend::parser::BoundOnConflictAction::Update {
                assignments: assignments
                    .into_iter()
                    .map(|assignment| {
                        substitute_rule_assignment(assignment, old_values, new_values)
                    })
                    .collect(),
                predicate: predicate.map(|expr| substitute_rule_expr(expr, old_values, new_values)),
                conflict_visibility_checks: conflict_visibility_checks
                    .into_iter()
                    .map(|check| substitute_rule_rls_check(check, old_values, new_values))
                    .collect(),
                update_write_checks: update_write_checks
                    .into_iter()
                    .map(|check| substitute_rule_rls_check(check, old_values, new_values))
                    .collect(),
            },
            other => other,
        },
        ..clause
    }
}

fn substitute_rule_insert_source(
    source: crate::backend::parser::BoundInsertSource,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundInsertSource {
    match source {
        crate::backend::parser::BoundInsertSource::Values(rows) => {
            crate::backend::parser::BoundInsertSource::Values(
                rows.into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                            .collect()
                    })
                    .collect(),
            )
        }
        crate::backend::parser::BoundInsertSource::ProjectSetValues(rows) => {
            crate::backend::parser::BoundInsertSource::ProjectSetValues(
                rows.into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                            .collect()
                    })
                    .collect(),
            )
        }
        crate::backend::parser::BoundInsertSource::DefaultValues(defaults) => {
            crate::backend::parser::BoundInsertSource::DefaultValues(
                defaults
                    .into_iter()
                    .map(|expr| substitute_rule_expr(expr, old_values, new_values))
                    .collect(),
            )
        }
        crate::backend::parser::BoundInsertSource::Select(query) => {
            crate::backend::parser::BoundInsertSource::Select(Box::new(substitute_rule_query(
                *query, old_values, new_values,
            )))
        }
    }
}

fn substitute_rule_insert_stmt(
    stmt: crate::backend::parser::BoundInsertStatement,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundInsertStatement {
    crate::backend::parser::BoundInsertStatement {
        column_defaults: stmt
            .column_defaults
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        target_columns: stmt
            .target_columns
            .into_iter()
            .map(|target| substitute_rule_assignment_target(target, old_values, new_values))
            .collect(),
        source: substitute_rule_insert_source(stmt.source, old_values, new_values),
        on_conflict: stmt
            .on_conflict
            .map(|clause| substitute_rule_on_conflict(clause, old_values, new_values)),
        returning: stmt
            .returning
            .into_iter()
            .map(|target| substitute_rule_target(target, old_values, new_values))
            .collect(),
        rls_write_checks: stmt
            .rls_write_checks
            .into_iter()
            .map(|check| substitute_rule_rls_check(check, old_values, new_values))
            .collect(),
        subplans: stmt
            .subplans
            .into_iter()
            .map(|plan| substitute_rule_plan(plan, old_values, new_values))
            .collect(),
        ..stmt
    }
}

fn substitute_rule_update_target(
    target: crate::backend::parser::BoundUpdateTarget,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundUpdateTarget {
    crate::backend::parser::BoundUpdateTarget {
        assignments: target
            .assignments
            .into_iter()
            .map(|assignment| substitute_rule_assignment(assignment, old_values, new_values))
            .collect(),
        parent_visible_exprs: target
            .parent_visible_exprs
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        predicate: target
            .predicate
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        rls_write_checks: target
            .rls_write_checks
            .into_iter()
            .map(|check| substitute_rule_rls_check(check, old_values, new_values))
            .collect(),
        ..target
    }
}

fn substitute_rule_update_stmt(
    stmt: crate::backend::parser::BoundUpdateStatement,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundUpdateStatement {
    crate::backend::parser::BoundUpdateStatement {
        targets: stmt
            .targets
            .into_iter()
            .map(|target| substitute_rule_update_target(target, old_values, new_values))
            .collect(),
        returning: stmt
            .returning
            .into_iter()
            .map(|target| substitute_rule_target(target, old_values, new_values))
            .collect(),
        input_plan: stmt
            .input_plan
            .map(|planned| substitute_rule_planned_stmt(planned, old_values, new_values)),
        subplans: stmt
            .subplans
            .into_iter()
            .map(|plan| substitute_rule_plan(plan, old_values, new_values))
            .collect(),
        ..stmt
    }
}

fn substitute_rule_delete_target(
    target: crate::backend::parser::BoundDeleteTarget,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundDeleteTarget {
    crate::backend::parser::BoundDeleteTarget {
        parent_visible_exprs: target
            .parent_visible_exprs
            .into_iter()
            .map(|expr| substitute_rule_expr(expr, old_values, new_values))
            .collect(),
        predicate: target
            .predicate
            .map(|expr| substitute_rule_expr(expr, old_values, new_values)),
        ..target
    }
}

fn substitute_rule_delete_stmt(
    stmt: crate::backend::parser::BoundDeleteStatement,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundDeleteStatement {
    crate::backend::parser::BoundDeleteStatement {
        targets: stmt
            .targets
            .into_iter()
            .map(|target| substitute_rule_delete_target(target, old_values, new_values))
            .collect(),
        returning: stmt
            .returning
            .into_iter()
            .map(|target| substitute_rule_target(target, old_values, new_values))
            .collect(),
        input_plan: stmt
            .input_plan
            .map(|planned| substitute_rule_planned_stmt(planned, old_values, new_values)),
        subplans: stmt
            .subplans
            .into_iter()
            .map(|plan| substitute_rule_plan(plan, old_values, new_values))
            .collect(),
        ..stmt
    }
}

fn substitute_rule_action(
    action: crate::backend::parser::BoundRuleAction,
    old_values: &[Value],
    new_values: &[Value],
) -> crate::backend::parser::BoundRuleAction {
    match action {
        crate::backend::parser::BoundRuleAction::Insert(stmt) => {
            crate::backend::parser::BoundRuleAction::Insert(substitute_rule_insert_stmt(
                stmt, old_values, new_values,
            ))
        }
        crate::backend::parser::BoundRuleAction::Update(stmt) => {
            crate::backend::parser::BoundRuleAction::Update(stmt)
        }
        crate::backend::parser::BoundRuleAction::Delete(stmt) => {
            crate::backend::parser::BoundRuleAction::Delete(stmt)
        }
        crate::backend::parser::BoundRuleAction::Select(planned) => {
            crate::backend::parser::BoundRuleAction::Select(substitute_rule_planned_stmt(
                planned, old_values, new_values,
            ))
        }
        crate::backend::parser::BoundRuleAction::Values(planned) => {
            crate::backend::parser::BoundRuleAction::Values(substitute_rule_planned_stmt(
                planned, old_values, new_values,
            ))
        }
        other => other,
    }
}

fn load_prepared_rules(
    relation_oid: u32,
    event: RuleEvent,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    session_replication_role: SessionReplicationRole,
) -> Result<Vec<PreparedRule>, ExecError> {
    let owner_oid = catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relowner)
        .unwrap_or_else(|| catalog.current_user_oid());
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| {
            row.rulename != "_RETURN"
                && row.ev_type == rule_event_code(event)
                && rule_enabled_for_session(&row, session_replication_role)
        })
        .map(|row| {
            let qual = if row.ev_qual.is_empty() {
                None
            } else {
                let parsed = crate::backend::parser::parse_expr(&row.ev_qual)?;
                Some(bind_rule_qual(&parsed, relation_desc, event, catalog)?)
            };
            let mut actions = Vec::new();
            let mut actions_reference_old_new = false;
            for sql in split_stored_rule_action_sql(&row.ev_action) {
                let statement = crate::backend::parser::parse_statement(sql)?;
                actions_reference_old_new |= statement_references_rule_tuple(&statement);
                actions.push(bind_rule_action_statement(
                    &statement,
                    relation_desc,
                    catalog,
                )?);
            }
            Ok(PreparedRule {
                is_instead: row.is_instead,
                owner_oid,
                qual,
                qual_subplans: Vec::new(),
                actions,
                actions_reference_old_new,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()
        .map_err(ExecError::Parse)
}

fn statement_references_rule_tuple(statement: &Statement) -> bool {
    statement_references_table(statement, "old") || statement_references_table(statement, "new")
}

fn statement_references_table(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Select(stmt) => select_statement_references_table(stmt, table_name),
        Statement::Values(stmt) => {
            cte_body_references_table(&CteBody::Values(stmt.clone()), table_name)
        }
        Statement::Insert(stmt) => insert_statement_references_table(stmt, table_name),
        Statement::Update(stmt) => update_statement_references_table(stmt, table_name),
        Statement::Delete(stmt) => delete_statement_references_table(stmt, table_name),
        Statement::Merge(stmt) => merge_statement_references_table(stmt, table_name),
        _ => false,
    }
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

fn materialize_view_update_events_for_stmt(
    stmt: &crate::backend::parser::BoundUpdateStatement,
    target: &crate::backend::parser::BoundUpdateTarget,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<ViewUpdateEvent>, ExecError> {
    if stmt.input_plan.is_some() {
        materialize_joined_view_update_events(stmt, target, ctx)
    } else {
        materialize_view_update_events(target, catalog, ctx).map(|events| {
            events
                .into_iter()
                .map(|(old_values, new_values)| ViewUpdateEvent {
                    old_values,
                    new_values,
                    source_values: Vec::new(),
                })
                .collect()
        })
    }
}

struct ViewUpdateEvent {
    old_values: Vec<Value>,
    new_values: Vec<Value>,
    source_values: Vec<Value>,
}

fn materialize_joined_view_update_events(
    stmt: &crate::backend::parser::BoundUpdateStatement,
    target: &crate::backend::parser::BoundUpdateTarget,
    ctx: &mut ExecutorContext,
) -> Result<Vec<ViewUpdateEvent>, ExecError> {
    let input_plan = stmt.input_plan.as_ref().ok_or(ExecError::DetailedError {
        message: "UPDATE ... FROM is missing its input plan".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    let mut state = crate::backend::executor::executor_start(input_plan.plan_tree.clone());
    let mut out = Vec::new();
    while let Some(slot) = state.exec_proc_node(ctx)? {
        ctx.check_for_interrupts()?;
        let mut row_values = slot.values()?.to_vec();
        Value::materialize_all(&mut row_values);
        if row_values.len() < stmt.visible_column_count {
            return Err(ExecError::DetailedError {
                message: "update input row is missing visible columns".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
        let old_values = row_values[..stmt.target_visible_count].to_vec();
        let source_values = &row_values[stmt.target_visible_count..stmt.visible_column_count];
        let mut eval_row = old_values.clone();
        eval_row.extend(source_values.iter().cloned());
        let mut eval_slot = TupleSlot::virtual_row(eval_row);
        let mut new_values = old_values.clone();
        for assignment in &target.assignments {
            let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
            apply_assignment_target(
                &target.desc,
                &mut new_values,
                &BoundAssignmentTarget {
                    column_index: assignment.column_index,
                    subscripts: assignment.subscripts.clone(),
                    field_path: assignment.field_path.clone(),
                    indirection: assignment.indirection.clone(),
                    target_sql_type: assignment.target_sql_type,
                },
                value,
                &mut eval_slot,
                ctx,
            )?;
        }
        out.push(ViewUpdateEvent {
            old_values,
            new_values,
            source_values: source_values.to_vec(),
        });
    }
    Ok(out)
}

fn append_update_from_source_rows(rows: &[Vec<Value>], event: &ViewUpdateEvent) -> Vec<Vec<Value>> {
    if event.source_values.is_empty() {
        return rows.to_vec();
    }
    rows.iter()
        .map(|row| {
            let mut values = row.clone();
            values.extend(event.source_values.iter().cloned());
            values
        })
        .collect()
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
        group_by_distinct: false,
        having: None,
        window_clauses: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        locking_clause: None,
        locking_targets: Vec::new(),
        locking_nowait: false,
        set_operation: None,
    };
    let planned = crate::backend::parser::pg_plan_query(&select, catalog)?;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, planned.subplans.clone());
    let result: Result<Vec<Vec<Value>>, ExecError> = (|| {
        let mut state = crate::backend::executor::executor_start(planned.plan_tree);
        let mut rows = Vec::new();
        while let Some(slot) = crate::backend::executor::exec_next(&mut state, ctx)? {
            let mut values = slot.values()?.to_vec();
            Value::materialize_all(&mut values);
            rows.push(values);
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

fn relation_has_active_user_rules_for_event(
    relation_oid: u32,
    event: RuleEvent,
    catalog: &dyn CatalogLookup,
    session_replication_role: SessionReplicationRole,
) -> bool {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| {
            row.rulename != "_RETURN"
                && row.ev_type == rule_event_code(event)
                && rule_enabled_for_session(&row, session_replication_role)
        })
}

fn rule_enabled_for_session(
    row: &PgRewriteRow,
    session_replication_role: SessionReplicationRole,
) -> bool {
    match row.ev_enabled {
        'D' => false,
        'A' => true,
        'R' => session_replication_role == SessionReplicationRole::Replica,
        'O' => session_replication_role != SessionReplicationRole::Replica,
        _ => true,
    }
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
