use std::collections::BTreeSet;
use std::sync::Arc;

use super::super::*;
use crate::backend::parser::{
    AlterTableTriggerMode, AlterTableTriggerStateStatement, AlterTableTriggerTarget,
    AlterTriggerRenameStatement, CatalogLookup, CommentOnTriggerStatement, CreateTriggerStatement,
    DropTriggerStatement, JsonTableBehavior, ParseError, RawWindowFrameBound, SqlCallArgs, SqlExpr,
    SqlType, SqlTypeKind, TriggerEvent, TriggerLevel, TriggerTiming,
    bind_scalar_expr_in_named_relation_scope, parse_expr,
};
use crate::include::catalog::{
    PG_LANGUAGE_INTERNAL_OID, PG_LANGUAGE_PLPGSQL_OID, PgConstraintRow, PgTriggerRow,
    RI_FKEY_CASCADE_DEL_PROC_OID, RI_FKEY_CASCADE_UPD_PROC_OID, RI_FKEY_CHECK_INS_PROC_OID,
    RI_FKEY_CHECK_UPD_PROC_OID, RI_FKEY_NOACTION_DEL_PROC_OID, RI_FKEY_NOACTION_UPD_PROC_OID,
    RI_FKEY_RESTRICT_DEL_PROC_OID, RI_FKEY_RESTRICT_UPD_PROC_OID, RI_FKEY_SETDEFAULT_DEL_PROC_OID,
    RI_FKEY_SETDEFAULT_UPD_PROC_OID, RI_FKEY_SETNULL_DEL_PROC_OID, RI_FKEY_SETNULL_UPD_PROC_OID,
};
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_trigger_relation_for_ddl};

const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

const TRIGGER_DISABLED: char = 'D';
const TRIGGER_ENABLED_ORIGIN: char = 'O';
const TRIGGER_ENABLED_REPLICA: char = 'R';
const TRIGGER_ENABLED_ALWAYS: char = 'A';

const TRIGGER_NEW_TABLEOID_COLUMN: &str = "__trigger_new_tableoid";
const TRIGGER_OLD_TABLEOID_COLUMN: &str = "__trigger_old_tableoid";
const TRIGGER_NEW_CTID_COLUMN: &str = "__trigger_new_ctid";
const TRIGGER_OLD_CTID_COLUMN: &str = "__trigger_old_ctid";

impl Database {
    pub(super) fn create_foreign_key_triggers_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        constraint: &PgConstraintRow,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        self.create_foreign_key_trigger_rows_in_transaction(
            client_id,
            xid,
            cid,
            foreign_key_trigger_rows(constraint),
            catalog_effects,
        )
    }

    pub(super) fn create_foreign_key_check_triggers_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        constraint: &PgConstraintRow,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        self.create_foreign_key_trigger_rows_in_transaction(
            client_id,
            xid,
            cid,
            foreign_key_check_trigger_rows(constraint),
            catalog_effects,
        )
    }

    pub(super) fn create_foreign_key_action_triggers_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        constraint: &PgConstraintRow,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        self.create_foreign_key_trigger_rows_in_transaction(
            client_id,
            xid,
            cid,
            foreign_key_action_trigger_rows(constraint),
            catalog_effects,
        )
    }

    fn create_foreign_key_trigger_rows_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        rows: Vec<PgTriggerRow>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let next_cid = cid.saturating_add(rows.len() as u32);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .create_triggers_mvcc(rows, &ctx)
            .map_err(map_catalog_error)?
            .1;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        self.plan_cache.invalidate_all();
        Ok(next_cid)
    }

    pub(super) fn drop_foreign_key_triggers_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        constraint: &PgConstraintRow,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let mut rows = catalog.trigger_rows_for_relation(constraint.conrelid);
        if constraint.confrelid != constraint.conrelid {
            rows.extend(catalog.trigger_rows_for_relation(constraint.confrelid));
        }
        rows.retain(|row| row.tgisinternal && row.tgconstraint == constraint.oid);
        let interrupts = self.interrupt_state(client_id);
        for (index, row) in rows.into_iter().enumerate() {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(index as u32),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .drop_trigger_mvcc(row.tgrelid, &row.tgname, &ctx)
                .map_err(map_catalog_error)?
                .1;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        if !catalog_effects.is_empty() {
            self.plan_cache.invalidate_all();
        }
        Ok(())
    }

    pub(super) fn alter_foreign_key_trigger_deferrability_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        constraint: &PgConstraintRow,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let mut rows = catalog.trigger_rows_for_relation(constraint.conrelid);
        if constraint.confrelid != constraint.conrelid {
            rows.extend(catalog.trigger_rows_for_relation(constraint.confrelid));
        }
        rows.retain(|row| {
            row.tgisinternal
                && row.tgconstraint == constraint.oid
                && foreign_key_trigger_deferrability_follows_constraint(row.tgfoid)
                && (row.tgdeferrable != constraint.condeferrable
                    || row.tginitdeferred != constraint.condeferred)
        });
        let interrupts = self.interrupt_state(client_id);
        for (index, row) in rows.into_iter().enumerate() {
            let mut replacement = row.clone();
            replacement.tgdeferrable = constraint.condeferrable;
            replacement.tginitdeferred = constraint.condeferred;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(index as u32),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .replace_trigger_mvcc(&row, replacement, &ctx)
                .map_err(map_catalog_error)?
                .1;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        if !catalog_effects.is_empty() {
            self.plan_cache.invalidate_all();
        }
        Ok(())
    }

    pub(crate) fn execute_create_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_trigger_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation_name = format_trigger_relation_name(stmt);
        let relation = lookup_trigger_relation_for_ddl(&catalog, &relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &relation_name)?;

        let trigger_name = stmt.trigger_name.to_ascii_lowercase();
        let trigger_function = resolve_trigger_function(
            self,
            client_id,
            Some((xid, cid)),
            &catalog,
            stmt,
            configured_search_path,
        )?;
        validate_trigger_stmt(stmt, &relation, &relation_name, &catalog)?;

        let tgattr = trigger_update_attnums(stmt, &relation.desc)?;
        let tgtype = trigger_type_bits(stmt);
        let (tgoldtable, tgnewtable) = trigger_transition_table_names(stmt);
        let trigger_row = PgTriggerRow {
            oid: 0,
            tgrelid: relation.relation_oid,
            tgparentid: 0,
            tgname: trigger_name.clone(),
            tgfoid: trigger_function.oid,
            tgtype,
            tgenabled: 'O',
            tgisinternal: false,
            tgconstrrelid: 0,
            tgconstrindid: 0,
            tgconstraint: 0,
            tgdeferrable: false,
            tginitdeferred: false,
            tgnargs: stmt.func_args.len() as i16,
            tgattr,
            tgargs: stmt.func_args.clone(),
            tgqual: stmt.when_clause_sql.clone(),
            tgoldtable,
            tgnewtable,
        };

        let existing = catalog
            .trigger_rows_for_relation(relation.relation_oid)
            .into_iter()
            .find(|row| row.tgname.eq_ignore_ascii_case(&trigger_name));
        if existing.is_some() && !stmt.replace_existing {
            return Err(ExecError::DetailedError {
                message: format!(
                    "trigger \"{}\" for relation \"{}\" already exists",
                    trigger_name, relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let (created_trigger, mut effects) = {
            let mut catalog_store = self.catalog.write();
            if let Some(existing) = existing {
                let (oid, effect) = catalog_store
                    .replace_trigger_mvcc(&existing, trigger_row.clone(), &ctx)
                    .map_err(map_catalog_error)?;
                let mut row = trigger_row.clone();
                row.oid = oid;
                (row, vec![effect])
            } else {
                let (oid, effect) = catalog_store
                    .create_trigger_mvcc(trigger_row.clone(), &ctx)
                    .map_err(map_catalog_error)?;
                let mut row = trigger_row.clone();
                row.oid = oid;
                (row, vec![effect])
            }
        };
        if relation.relkind == 'p' && trigger_row_is_row(&created_trigger) {
            let clone_effects = self.create_partition_trigger_clones_in_transaction(
                client_id,
                xid,
                cid.saturating_add(1),
                &created_trigger,
                configured_search_path,
            )?;
            effects.extend(clone_effects);
        }
        for effect in &effects {
            self.apply_catalog_mutation_effect_immediate(effect)?;
        }
        self.plan_cache.invalidate_all();
        catalog_effects.extend(effects);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_trigger_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation_name = format_trigger_drop_relation_name(stmt);
        let relation = match catalog.lookup_any_relation(&relation_name) {
            Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f' | 'v') => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: relation_name.clone(),
                    expected: "table or view",
                }));
            }
            None if stmt.if_exists => {
                push_missing_trigger_relation_notice(&catalog, &relation_name);
                return Ok(StatementResult::AffectedRows(0));
            }
            None => return Err(missing_trigger_relation_error(&catalog, &relation_name)),
        };
        ensure_relation_owner(self, client_id, &relation, &relation_name)?;

        let Some(existing) = catalog
            .trigger_rows_for_relation(relation.relation_oid)
            .into_iter()
            .find(|row| row.tgname.eq_ignore_ascii_case(&stmt.trigger_name))
        else {
            if stmt.if_exists {
                crate::backend::utils::misc::notices::push_notice(format!(
                    "trigger \"{}\" for relation \"{}\" does not exist, skipping",
                    stmt.trigger_name, relation_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "trigger \"{}\" for table \"{}\" does not exist",
                    stmt.trigger_name, relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        if existing.tgparentid != 0 {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop trigger {} on table {} because trigger {} on a partitioned table requires it",
                    stmt.trigger_name, relation_name, stmt.trigger_name
                ),
                detail: None,
                hint: Some(format!(
                    "You can drop trigger {} on the partitioned table instead.",
                    stmt.trigger_name
                )),
                sqlstate: "2BP01",
            });
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let mut effects = Vec::new();
        let inherited = inherited_trigger_descendants(&catalog, existing.tgrelid, existing.oid);
        {
            let mut catalog_store = self.catalog.write();
            for row in inherited.iter().rev() {
                let (_removed, effect) = catalog_store
                    .drop_trigger_mvcc(row.tgrelid, &row.tgname, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
            let (_removed, effect) = catalog_store
                .drop_trigger_mvcc(relation.relation_oid, &stmt.trigger_name, &ctx)
                .map_err(map_catalog_error)?;
            effects.push(effect);
        }
        for effect in &effects {
            self.apply_catalog_mutation_effect_immediate(effect)?;
        }
        self.plan_cache.invalidate_all();
        catalog_effects.extend(effects);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn clone_parent_row_triggers_to_partition_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let parent_triggers = catalog
            .trigger_rows_for_relation(parent_oid)
            .into_iter()
            .filter(trigger_row_is_row)
            .filter(|row| !internal_constraint_trigger_row(row))
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for parent_trigger in parent_triggers {
            effects.extend(self.create_partition_trigger_clone_tree_in_transaction(
                client_id,
                xid,
                cid.saturating_add(1),
                &catalog,
                &parent_trigger,
                child_oid,
            )?);
        }
        for effect in &effects {
            self.apply_catalog_mutation_effect_immediate(effect)?;
        }
        catalog_effects.extend(effects);
        Ok(())
    }

    pub(crate) fn drop_cloned_parent_row_triggers_from_partition_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let parent_trigger_oids = catalog
            .trigger_rows_for_relation(parent_oid)
            .into_iter()
            .filter(trigger_row_is_row)
            .filter(|row| !internal_constraint_trigger_row(row))
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        if parent_trigger_oids.is_empty() {
            return Ok(cid);
        }
        let child_triggers = catalog
            .trigger_rows_for_relation(child_oid)
            .into_iter()
            .filter(|row| row.tgparentid != 0 && parent_trigger_oids.contains(&row.tgparentid))
            .collect::<Vec<_>>();
        if child_triggers.is_empty() {
            return Ok(cid);
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let mut effects = Vec::new();
        {
            let mut catalog_store = self.catalog.write();
            for child_trigger in child_triggers {
                let descendants =
                    inherited_trigger_descendants(&catalog, child_oid, child_trigger.oid);
                for row in descendants.iter().rev() {
                    let (_removed, effect) = catalog_store
                        .drop_trigger_mvcc(row.tgrelid, &row.tgname, &ctx)
                        .map_err(map_catalog_error)?;
                    effects.push(effect);
                }
                let (_removed, effect) = catalog_store
                    .drop_trigger_mvcc(child_trigger.tgrelid, &child_trigger.tgname, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
        }
        for effect in &effects {
            self.apply_catalog_mutation_effect_immediate(effect)?;
        }
        self.plan_cache.invalidate_all();
        catalog_effects.extend(effects);
        Ok(cid.saturating_add(1))
    }

    fn create_partition_trigger_clones_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_trigger: &PgTriggerRow,
        configured_search_path: Option<&[String]>,
    ) -> Result<Vec<CatalogMutationEffect>, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut effects = Vec::new();
        for child in direct_partition_child_oids(&catalog, parent_trigger.tgrelid) {
            effects.extend(self.create_partition_trigger_clone_tree_in_transaction(
                client_id,
                xid,
                cid,
                &catalog,
                parent_trigger,
                child,
            )?);
        }
        Ok(effects)
    }

    fn create_partition_trigger_clone_tree_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        catalog: &dyn CatalogLookup,
        parent_trigger: &PgTriggerRow,
        child_oid: u32,
    ) -> Result<Vec<CatalogMutationEffect>, ExecError> {
        let mut child_trigger = cloned_partition_trigger_row(parent_trigger, child_oid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (oid, effect) = self
            .catalog
            .write()
            .create_trigger_mvcc(child_trigger.clone(), &ctx)
            .map_err(map_catalog_error)?;
        child_trigger.oid = oid;
        let mut effects = vec![effect];
        for grandchild_oid in direct_partition_child_oids(catalog, child_oid) {
            effects.extend(self.create_partition_trigger_clone_tree_in_transaction(
                client_id,
                xid,
                cid,
                catalog,
                &child_trigger,
                grandchild_oid,
            )?);
        }
        Ok(effects)
    }

    pub(crate) fn execute_comment_on_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_trigger_relation_for_ddl(&catalog, &stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_trigger_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
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

    pub(crate) fn execute_comment_on_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_trigger_relation_for_ddl(&catalog, &stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        let trigger = lookup_trigger_row(&catalog, relation.relation_oid, &stmt.trigger_name)
            .ok_or_else(|| missing_trigger_error(&stmt.trigger_name, &stmt.table_name))?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_trigger_mvcc(trigger.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_trigger_state_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableTriggerStateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_trigger_state_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_trigger_state_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableTriggerStateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_trigger_relation_for_ddl(&catalog, &stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;

        let all_triggers = catalog.trigger_rows_for_relation(relation.relation_oid);
        let mut selected = match &stmt.target {
            AlterTableTriggerTarget::Named(name) => {
                let Some(row) = lookup_trigger_row(&catalog, relation.relation_oid, name) else {
                    return Err(missing_trigger_error(name, &stmt.table_name));
                };
                vec![row]
            }
            AlterTableTriggerTarget::All => all_triggers,
            AlterTableTriggerTarget::User => all_triggers
                .into_iter()
                .filter(|row| !row.tgisinternal)
                .collect(),
        };
        if !stmt.only && relation.relkind == 'p' {
            let mut inherited = Vec::new();
            for row in &selected {
                if trigger_row_is_row(row) {
                    inherited.extend(inherited_trigger_descendants(
                        &catalog,
                        relation.relation_oid,
                        row.oid,
                    ));
                }
            }
            selected.extend(inherited);
        }
        if selected.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let new_state = trigger_state_char(stmt.mode);
        let interrupts = self.interrupt_state(client_id);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        for row in selected {
            let mut updated = row.clone();
            updated.tgenabled = new_state;
            let effect = self
                .catalog
                .write()
                .replace_trigger_mvcc(&row, updated, &ctx)
                .map_err(map_catalog_error)?
                .1;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_trigger_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTriggerRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_trigger_rename_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_trigger_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTriggerRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let relation_name = qualified_relation_name(stmt.schema_name.as_deref(), &stmt.table_name);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_trigger_relation_for_ddl(&catalog, &relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &relation_name)?;
        let existing = lookup_trigger_row(&catalog, relation.relation_oid, &stmt.trigger_name)
            .ok_or_else(|| missing_trigger_error(&stmt.trigger_name, &relation_name))?;

        let interrupts = self.interrupt_state(client_id);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let mut updated = existing.clone();
        updated.tgname = stmt.new_trigger_name.to_ascii_lowercase();
        let effect = self
            .catalog
            .write()
            .replace_trigger_mvcc(&existing, updated, &ctx)
            .map_err(map_catalog_error)?
            .1;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

fn format_trigger_relation_name(stmt: &CreateTriggerStatement) -> String {
    qualified_relation_name(stmt.schema_name.as_deref(), &stmt.table_name)
}

fn format_trigger_drop_relation_name(stmt: &DropTriggerStatement) -> String {
    qualified_relation_name(stmt.schema_name.as_deref(), &stmt.table_name)
}

fn qualified_relation_name(schema_name: Option<&str>, relation_name: &str) -> String {
    schema_name
        .map(|schema| format!("{schema}.{relation_name}"))
        .unwrap_or_else(|| relation_name.to_string())
}

fn push_missing_trigger_relation_notice(catalog: &dyn CatalogLookup, relation_name: &str) {
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

fn missing_trigger_relation_error(catalog: &dyn CatalogLookup, relation_name: &str) -> ExecError {
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
    ExecError::DetailedError {
        message: format!(
            "relation \"{}\" does not exist",
            relation_name.rsplit('.').next().unwrap_or(relation_name)
        ),
        detail: None,
        hint: None,
        sqlstate: "42P01",
    }
}

fn lookup_trigger_row(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    trigger_name: &str,
) -> Option<PgTriggerRow> {
    catalog
        .trigger_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.tgname.eq_ignore_ascii_case(trigger_name))
}

fn missing_trigger_error(trigger_name: &str, table_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "trigger \"{}\" for table \"{}\" does not exist",
            trigger_name, table_name
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn resolve_trigger_function(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    stmt: &CreateTriggerStatement,
    configured_search_path: Option<&[String]>,
) -> Result<crate::include::catalog::PgProcRow, ExecError> {
    let proname = stmt.function_name.to_ascii_lowercase();
    let namespace_candidates = if let Some(schema_name) = stmt.function_schema_name.as_deref() {
        vec![resolve_function_schema_oid(
            db,
            client_id,
            txn_ctx,
            schema_name,
        )?]
    } else {
        function_search_path_namespace_oids(db, client_id, txn_ctx, configured_search_path)
    };
    let function = namespace_candidates
        .iter()
        .find_map(|namespace_oid| {
            catalog.proc_rows_by_name(&proname).into_iter().find(|row| {
                row.pronamespace == *namespace_oid
                    && row.pronargs == 0
                    && row.proargtypes.trim().is_empty()
            })
        })
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function {}() does not exist", proname),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    if function.prokind != 'f' {
        return Err(ExecError::DetailedError {
            message: format!("{} is not a function", proname),
            detail: None,
            hint: None,
            sqlstate: "42883",
        });
    }
    let supported_trigger_language = if function.prolang == PG_LANGUAGE_PLPGSQL_OID {
        catalog
            .language_row_by_oid(function.prolang)
            .is_some_and(|row| row.oid == PG_LANGUAGE_PLPGSQL_OID)
    } else {
        function.prolang == PG_LANGUAGE_INTERNAL_OID
    };
    if !supported_trigger_language {
        return Err(ExecError::DetailedError {
            message: format!(
                "trigger function {} must be written in plpgsql or be an internal function",
                proname
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let return_type = catalog
        .type_by_oid(function.prorettype)
        .map(|row| row.sql_type)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnsupportedType(function.prorettype.to_string()))
        })?;
    if return_type.kind != SqlTypeKind::Trigger {
        return Err(ExecError::DetailedError {
            message: format!("function {} must return trigger", proname),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if function.pronargs != 0 {
        return Err(ExecError::DetailedError {
            message: format!("function {} must not accept any arguments", proname),
            detail: None,
            hint: None,
            sqlstate: "42P13",
        });
    }
    Ok(function)
}

fn function_search_path_namespace_oids(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
) -> Vec<u32> {
    let mut out = vec![crate::include::catalog::PG_CATALOG_NAMESPACE_OID];
    for item in db.effective_search_path(client_id, configured_search_path) {
        match item.as_str() {
            "" | "$user" | "pg_catalog" => {}
            _ => {
                if let Some(namespace_oid) =
                    db.visible_namespace_oid_by_name(client_id, txn_ctx, &item)
                {
                    out.push(namespace_oid);
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn resolve_function_schema_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<u32, ExecError> {
    let normalized = name.to_ascii_lowercase();
    if normalized == "pg_catalog" {
        return Ok(crate::include::catalog::PG_CATALOG_NAMESPACE_OID);
    }
    db.visible_namespace_oid_by_name(client_id, txn_ctx, &normalized)
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedQualifiedName(name.to_string())))
}

fn validate_trigger_stmt(
    stmt: &CreateTriggerStatement,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    validate_trigger_referencing_usage(stmt, relation, relation_name, catalog)?;
    validate_trigger_relation_kind(stmt, relation.relkind, relation_name)?;

    for event in &stmt.events {
        if event.event == TriggerEvent::Update {
            let mut seen = BTreeSet::new();
            for column in &event.update_columns {
                let attnum = relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, candidate)| {
                        (!candidate.dropped && candidate.name.eq_ignore_ascii_case(column))
                            .then_some(index as i16 + 1)
                    })
                    .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.clone())))?;
                if !seen.insert(attnum) {
                    return Err(ExecError::DetailedError {
                        message: format!("column \"{}\" specified more than once", column),
                        detail: None,
                        hint: None,
                        sqlstate: "42701",
                    });
                }
            }
        }
    }

    if let Some(when_clause_sql) = stmt.when_clause_sql.as_deref() {
        let mut parsed = parse_expr(when_clause_sql).map_err(ExecError::Parse)?;
        rewrite_trigger_system_column_refs(&mut parsed);
        for event in &stmt.events {
            validate_trigger_when_usage(stmt, event.event, when_clause_sql)?;
            let mut relation_scopes = Vec::new();
            if stmt.level == TriggerLevel::Row {
                match event.event {
                    TriggerEvent::Insert => relation_scopes.push(("new", &relation.desc)),
                    TriggerEvent::Update => {
                        relation_scopes.push(("new", &relation.desc));
                        relation_scopes.push(("old", &relation.desc));
                    }
                    TriggerEvent::Delete => relation_scopes.push(("old", &relation.desc)),
                    TriggerEvent::Truncate => {}
                }
            }
            let local_columns = trigger_when_local_columns(event.event);
            let (_, when_type) = bind_scalar_expr_in_named_relation_scope(
                &parsed,
                &relation_scopes,
                &local_columns,
                catalog,
            )
            .map_err(ExecError::Parse)?;
            if when_type.kind != SqlTypeKind::Bool {
                return Err(ExecError::DetailedError {
                    message: "trigger WHEN condition must return type boolean".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42804",
                });
            }
        }
    }
    Ok(())
}

fn trigger_row_is_row(row: &PgTriggerRow) -> bool {
    row.tgtype & TRIGGER_TYPE_ROW != 0
}

fn direct_partition_child_oids(catalog: &dyn CatalogLookup, parent_oid: u32) -> Vec<u32> {
    let mut children = catalog
        .inheritance_children(parent_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| {
            catalog
                .relation_by_oid(row.inhrelid)
                .filter(|relation| relation.relispartition)
                .map(|_| (row.inhseqno, row.inhrelid))
        })
        .collect::<Vec<_>>();
    children.sort_unstable();
    children.into_iter().map(|(_, oid)| oid).collect()
}

fn cloned_partition_trigger_row(parent: &PgTriggerRow, child_oid: u32) -> PgTriggerRow {
    PgTriggerRow {
        oid: 0,
        tgrelid: child_oid,
        tgparentid: parent.oid,
        tgname: parent.tgname.clone(),
        tgfoid: parent.tgfoid,
        tgtype: parent.tgtype,
        tgenabled: parent.tgenabled,
        tgisinternal: parent.tgisinternal,
        tgconstrrelid: parent.tgconstrrelid,
        tgconstrindid: parent.tgconstrindid,
        tgconstraint: parent.tgconstraint,
        tgdeferrable: parent.tgdeferrable,
        tginitdeferred: parent.tginitdeferred,
        tgnargs: parent.tgnargs,
        tgattr: parent.tgattr.clone(),
        tgargs: parent.tgargs.clone(),
        tgqual: parent.tgqual.clone(),
        tgoldtable: parent.tgoldtable.clone(),
        tgnewtable: parent.tgnewtable.clone(),
    }
}

fn internal_constraint_trigger_row(row: &PgTriggerRow) -> bool {
    row.tgisinternal && row.tgconstraint != 0
}

fn foreign_key_trigger_rows(constraint: &PgConstraintRow) -> Vec<PgTriggerRow> {
    let mut rows = foreign_key_check_trigger_rows(constraint);
    rows.extend(foreign_key_action_trigger_rows(constraint));
    rows
}

fn foreign_key_action_trigger_rows(constraint: &PgConstraintRow) -> Vec<PgTriggerRow> {
    vec![
        foreign_key_trigger_row(
            constraint,
            constraint.confrelid,
            constraint.conrelid,
            foreign_key_delete_proc_oid(constraint.confdeltype),
            TRIGGER_TYPE_ROW | TRIGGER_TYPE_DELETE,
            "a",
            3,
        ),
        foreign_key_trigger_row(
            constraint,
            constraint.confrelid,
            constraint.conrelid,
            foreign_key_update_proc_oid(constraint.confupdtype),
            TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE,
            "a",
            4,
        ),
    ]
}

fn foreign_key_check_trigger_rows(constraint: &PgConstraintRow) -> Vec<PgTriggerRow> {
    vec![
        foreign_key_trigger_row(
            constraint,
            constraint.conrelid,
            constraint.confrelid,
            RI_FKEY_CHECK_INS_PROC_OID,
            TRIGGER_TYPE_ROW | TRIGGER_TYPE_INSERT,
            "c",
            1,
        ),
        foreign_key_trigger_row(
            constraint,
            constraint.conrelid,
            constraint.confrelid,
            RI_FKEY_CHECK_UPD_PROC_OID,
            TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE,
            "c",
            2,
        ),
    ]
}

fn foreign_key_trigger_row(
    constraint: &PgConstraintRow,
    tgrelid: u32,
    tgconstrrelid: u32,
    tgfoid: u32,
    tgtype: i16,
    prefix: &str,
    offset: u32,
) -> PgTriggerRow {
    let deferrability_follows_constraint =
        foreign_key_trigger_deferrability_follows_constraint(tgfoid);
    PgTriggerRow {
        oid: 0,
        tgrelid,
        tgparentid: 0,
        tgname: format!(
            "RI_ConstraintTrigger_{}_{}",
            prefix,
            u64::from(constraint.oid) * 10 + u64::from(offset)
        ),
        tgfoid,
        tgtype,
        tgenabled: TRIGGER_ENABLED_ORIGIN,
        tgisinternal: true,
        tgconstrrelid,
        tgconstrindid: constraint.conindid,
        tgconstraint: constraint.oid,
        tgdeferrable: deferrability_follows_constraint && constraint.condeferrable,
        tginitdeferred: deferrability_follows_constraint && constraint.condeferred,
        tgnargs: 0,
        tgattr: Vec::new(),
        tgargs: Vec::new(),
        tgqual: None,
        tgoldtable: None,
        tgnewtable: None,
    }
}

fn foreign_key_trigger_deferrability_follows_constraint(tgfoid: u32) -> bool {
    matches!(
        tgfoid,
        RI_FKEY_CHECK_INS_PROC_OID
            | RI_FKEY_CHECK_UPD_PROC_OID
            | RI_FKEY_NOACTION_DEL_PROC_OID
            | RI_FKEY_NOACTION_UPD_PROC_OID
    )
}

fn foreign_key_delete_proc_oid(action: char) -> u32 {
    match action {
        'c' => RI_FKEY_CASCADE_DEL_PROC_OID,
        'r' => RI_FKEY_RESTRICT_DEL_PROC_OID,
        'n' => RI_FKEY_SETNULL_DEL_PROC_OID,
        'd' => RI_FKEY_SETDEFAULT_DEL_PROC_OID,
        _ => RI_FKEY_NOACTION_DEL_PROC_OID,
    }
}

fn foreign_key_update_proc_oid(action: char) -> u32 {
    match action {
        'c' => RI_FKEY_CASCADE_UPD_PROC_OID,
        'r' => RI_FKEY_RESTRICT_UPD_PROC_OID,
        'n' => RI_FKEY_SETNULL_UPD_PROC_OID,
        'd' => RI_FKEY_SETDEFAULT_UPD_PROC_OID,
        _ => RI_FKEY_NOACTION_UPD_PROC_OID,
    }
}

fn inherited_trigger_descendants(
    catalog: &dyn CatalogLookup,
    root_relation_oid: u32,
    root_trigger_oid: u32,
) -> Vec<PgTriggerRow> {
    let mut descendants = Vec::new();
    let mut pending = vec![root_trigger_oid];
    let relation_oids = catalog.find_all_inheritors(root_relation_oid);
    while let Some(parent_oid) = pending.pop() {
        let children = relation_oids
            .iter()
            .flat_map(|relation_oid| catalog.trigger_rows_for_relation(*relation_oid))
            .filter(|row| row.tgparentid == parent_oid)
            .collect::<Vec<_>>();
        for child in children {
            pending.push(child.oid);
            descendants.push(child);
        }
    }
    descendants
}

fn trigger_update_attnums(
    stmt: &CreateTriggerStatement,
    desc: &crate::backend::executor::RelationDesc,
) -> Result<Vec<i16>, ExecError> {
    let mut attnums = BTreeSet::new();
    for event in &stmt.events {
        if event.event != TriggerEvent::Update {
            continue;
        }
        for column in &event.update_columns {
            let attnum = desc
                .columns
                .iter()
                .enumerate()
                .find_map(|(index, candidate)| {
                    (!candidate.dropped && candidate.name.eq_ignore_ascii_case(column))
                        .then_some(index as i16 + 1)
                })
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.clone())))?;
            attnums.insert(attnum);
        }
    }
    Ok(attnums.into_iter().collect())
}

fn trigger_type_bits(stmt: &CreateTriggerStatement) -> i16 {
    let mut bits = 0;
    if stmt.level == TriggerLevel::Row {
        bits |= TRIGGER_TYPE_ROW;
    }
    match stmt.timing {
        TriggerTiming::Before => bits |= TRIGGER_TYPE_BEFORE,
        TriggerTiming::Instead => bits |= TRIGGER_TYPE_INSTEAD,
        TriggerTiming::After => {}
    }
    for event in &stmt.events {
        bits |= match event.event {
            TriggerEvent::Insert => TRIGGER_TYPE_INSERT,
            TriggerEvent::Delete => TRIGGER_TYPE_DELETE,
            TriggerEvent::Update => TRIGGER_TYPE_UPDATE,
            TriggerEvent::Truncate => TRIGGER_TYPE_TRUNCATE,
        };
    }
    bits
}

fn validate_trigger_relation_kind(
    stmt: &CreateTriggerStatement,
    relkind: char,
    relation_name: &str,
) -> Result<(), ExecError> {
    match relkind {
        'v' => validate_view_trigger_stmt(stmt, relation_name),
        'f' => validate_foreign_table_trigger_stmt(stmt, relation_name),
        _ => validate_table_trigger_stmt(stmt, relation_name),
    }
}

fn validate_table_trigger_stmt(
    stmt: &CreateTriggerStatement,
    relation_name: &str,
) -> Result<(), ExecError> {
    if stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Truncate)
    {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "TRUNCATE triggers are not supported".into(),
        )));
    }
    if stmt.is_constraint {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CONSTRAINT TRIGGER is not supported".into(),
        )));
    }
    if stmt.timing == TriggerTiming::Instead {
        return Err(ExecError::DetailedError {
            message: format!("\"{}\" is a table", relation_name),
            detail: Some("Tables cannot have INSTEAD OF triggers.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn validate_foreign_table_trigger_stmt(
    stmt: &CreateTriggerStatement,
    relation_name: &str,
) -> Result<(), ExecError> {
    let relation_basename = relation_name.rsplit('.').next().unwrap_or(relation_name);
    if stmt.is_constraint {
        return Err(wrong_object_type_error(
            relation_basename,
            "foreign table",
            "Foreign tables cannot have constraint triggers.",
        ));
    }
    if stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Truncate)
    {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "TRUNCATE triggers are not supported".into(),
        )));
    }
    if stmt.timing == TriggerTiming::Instead {
        return Err(ExecError::DetailedError {
            message: format!("\"{}\" is a foreign table", relation_basename),
            detail: Some("Foreign tables cannot have INSTEAD OF triggers.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn validate_view_trigger_stmt(
    stmt: &CreateTriggerStatement,
    relation_name: &str,
) -> Result<(), ExecError> {
    if stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Truncate)
    {
        return Err(ExecError::DetailedError {
            message: format!("\"{}\" is a view", relation_name),
            detail: Some("Views cannot have TRUNCATE triggers.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    if stmt.timing == TriggerTiming::Instead {
        if stmt.when_clause_sql.is_some() {
            return Err(ExecError::DetailedError {
                message: "INSTEAD OF triggers cannot have WHEN conditions".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if stmt
            .events
            .iter()
            .any(|event| !event.update_columns.is_empty())
        {
            return Err(ExecError::DetailedError {
                message: "INSTEAD OF triggers cannot have column lists".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if stmt.level != TriggerLevel::Row {
            return Err(ExecError::DetailedError {
                message: "INSTEAD OF triggers must be FOR EACH ROW".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        return Ok(());
    }

    if stmt.level == TriggerLevel::Row {
        return Err(ExecError::DetailedError {
            message: format!("\"{}\" is a view", relation_name),
            detail: Some("Views cannot have row-level BEFORE or AFTER triggers.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn validate_trigger_referencing_usage(
    stmt: &CreateTriggerStatement,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if stmt.referencing.is_empty() {
        return Ok(());
    }
    if stmt.referencing.iter().any(|spec| !spec.is_table) {
        return Err(ExecError::DetailedError {
            message: "ROW variable naming in the REFERENCING clause is not supported".into(),
            detail: None,
            hint: Some("Use OLD TABLE or NEW TABLE for naming transition tables.".into()),
            sqlstate: "0A000",
        });
    }
    if relation.relkind == 'v' {
        return Err(wrong_object_type_error(
            relation_name,
            "view",
            "Triggers on views cannot have transition tables.",
        ));
    }
    if relation.relkind == 'f' {
        let relation_basename = relation_name.rsplit('.').next().unwrap_or(relation_name);
        return Err(wrong_object_type_error(
            relation_basename,
            "foreign table",
            "Triggers on foreign tables cannot have transition tables.",
        ));
    }
    if stmt.level == TriggerLevel::Row && relation.relkind == 'p' {
        return Err(wrong_object_type_error(
            relation_name,
            "partitioned table",
            "ROW triggers with transition tables are not supported on partitioned tables.",
        ));
    }
    if stmt.level == TriggerLevel::Row
        && !catalog
            .inheritance_parents(relation.relation_oid)
            .is_empty()
    {
        if relation.relispartition {
            return Err(feature_not_supported_error(
                "ROW triggers with transition tables are not supported on partitions",
            ));
        }
        return Err(feature_not_supported_error(
            "ROW triggers with transition tables are not supported on inheritance children",
        ));
    }
    if stmt.timing != TriggerTiming::After {
        return Err(ExecError::DetailedError {
            message: "transition table name can only be specified for an AFTER trigger".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Truncate)
    {
        return Err(feature_not_supported_error(
            "TRUNCATE triggers with transition tables are not supported",
        ));
    }
    if stmt.events.len() != 1 {
        return Err(feature_not_supported_error(
            "transition tables cannot be specified for triggers with more than one event",
        ));
    }
    if stmt
        .events
        .iter()
        .any(|event| !event.update_columns.is_empty())
    {
        return Err(feature_not_supported_error(
            "transition tables cannot be specified for triggers with column lists",
        ));
    }
    let has_insert = stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Insert);
    let has_update = stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Update);
    let has_delete = stmt
        .events
        .iter()
        .any(|event| event.event == TriggerEvent::Delete);
    let mut old_name = None::<String>;
    let mut new_name = None::<String>;
    for spec in &stmt.referencing {
        if spec.is_new {
            if !(has_insert || has_update) {
                return Err(ExecError::DetailedError {
                    message: "NEW TABLE can only be specified for an INSERT or UPDATE trigger"
                        .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P17",
                });
            }
            if new_name.is_some() {
                return Err(ExecError::DetailedError {
                    message: "NEW TABLE cannot be specified multiple times".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P17",
                });
            }
            new_name = Some(spec.name.clone());
        } else {
            if !(has_delete || has_update) {
                return Err(ExecError::DetailedError {
                    message: "OLD TABLE can only be specified for a DELETE or UPDATE trigger"
                        .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P17",
                });
            }
            if old_name.is_some() {
                return Err(ExecError::DetailedError {
                    message: "OLD TABLE cannot be specified multiple times".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P17",
                });
            }
            old_name = Some(spec.name.clone());
        }
    }
    if old_name
        .as_ref()
        .zip(new_name.as_ref())
        .is_some_and(|(old, new)| old.eq_ignore_ascii_case(new))
    {
        return Err(ExecError::DetailedError {
            message: "OLD TABLE name and NEW TABLE name cannot be the same".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

fn trigger_transition_table_names(
    stmt: &CreateTriggerStatement,
) -> (Option<String>, Option<String>) {
    let mut old_name = None;
    let mut new_name = None;
    for spec in &stmt.referencing {
        if spec.is_new {
            new_name = Some(spec.name.clone());
        } else {
            old_name = Some(spec.name.clone());
        }
    }
    (old_name, new_name)
}

fn wrong_object_type_error(relation_name: &str, kind: &str, detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("\"{relation_name}\" is a {kind}"),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "42809",
    }
}

fn feature_not_supported_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn validate_trigger_when_usage(
    stmt: &CreateTriggerStatement,
    event: TriggerEvent,
    when_clause_sql: &str,
) -> Result<(), ExecError> {
    let lowered = when_clause_sql.to_ascii_lowercase();
    let references_old = lowered.contains("old.");
    let references_new = lowered.contains("new.");
    let references_new_system = [
        "new.tableoid",
        "new.ctid",
        "new.xmin",
        "new.xmax",
        "new.cmin",
        "new.cmax",
    ]
    .iter()
    .any(|needle| lowered.contains(needle));

    if matches!(event, TriggerEvent::Insert) && references_old {
        return Err(ExecError::DetailedError {
            message: "INSERT trigger's WHEN condition cannot reference OLD values".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if matches!(event, TriggerEvent::Delete) && references_new {
        return Err(ExecError::DetailedError {
            message: "DELETE trigger's WHEN condition cannot reference NEW values".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if matches!(event, TriggerEvent::Truncate) && (references_old || references_new) {
        return Err(ExecError::DetailedError {
            message: "TRUNCATE trigger's WHEN condition cannot reference column values".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if stmt.level == TriggerLevel::Statement && (references_old || references_new) {
        return Err(ExecError::DetailedError {
            message: "statement trigger's WHEN condition cannot reference column values".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if stmt.level == TriggerLevel::Row
        && stmt.timing == TriggerTiming::Before
        && references_new_system
    {
        return Err(ExecError::DetailedError {
            message: "BEFORE trigger's WHEN condition cannot reference NEW system columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

fn trigger_when_local_columns(event: TriggerEvent) -> Vec<(String, SqlType)> {
    match event {
        TriggerEvent::Insert => vec![
            (
                TRIGGER_NEW_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_NEW_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
        TriggerEvent::Update => vec![
            (
                TRIGGER_NEW_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_NEW_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
            (
                TRIGGER_OLD_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_OLD_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
        TriggerEvent::Delete => vec![
            (
                TRIGGER_OLD_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_OLD_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
        TriggerEvent::Truncate => Vec::new(),
    }
}

fn rewrite_trigger_system_column_refs(expr: &mut SqlExpr) {
    match expr {
        SqlExpr::Column(name) => {
            let lowered = name.to_ascii_lowercase();
            if lowered == "new.tableoid" {
                *name = TRIGGER_NEW_TABLEOID_COLUMN.into();
            } else if lowered == "old.tableoid" {
                *name = TRIGGER_OLD_TABLEOID_COLUMN.into();
            } else if lowered == "new.ctid" {
                *name = TRIGGER_NEW_CTID_COLUMN.into();
            } else if lowered == "old.ctid" {
                *name = TRIGGER_OLD_CTID_COLUMN.into();
            }
        }
        SqlExpr::Parameter(_) => {}
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        } => {
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(right);
        }
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. } => {
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(right);
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::FieldSelect { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. }
        | SqlExpr::Collate { expr: inner, .. } => rewrite_trigger_system_column_refs(inner),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            rewrite_trigger_system_column_refs(expr);
            rewrite_trigger_system_column_refs(pattern);
            if let Some(escape) = escape {
                rewrite_trigger_system_column_refs(escape);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                rewrite_trigger_system_column_refs(arg);
            }
            for when in args {
                rewrite_trigger_system_column_refs(&mut when.expr);
                rewrite_trigger_system_column_refs(&mut when.result);
            }
            if let Some(defresult) = defresult {
                rewrite_trigger_system_column_refs(defresult);
            }
        }
        SqlExpr::ArrayLiteral(values) | SqlExpr::Row(values) => {
            for value in values {
                rewrite_trigger_system_column_refs(value);
            }
        }
        SqlExpr::InSubquery { expr, .. } => rewrite_trigger_system_column_refs(expr),
        SqlExpr::QuantifiedSubquery { left, .. } => rewrite_trigger_system_column_refs(left),
        SqlExpr::QuantifiedArray { left, array, .. } => {
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(array);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            rewrite_trigger_system_column_refs(array);
            for subscript in subscripts {
                if let Some(lower) = &mut subscript.lower {
                    rewrite_trigger_system_column_refs(lower);
                }
                if let Some(upper) = &mut subscript.upper {
                    rewrite_trigger_system_column_refs(upper);
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for arg in &mut xml.named_args {
                rewrite_trigger_system_column_refs(arg);
            }
            for arg in &mut xml.args {
                rewrite_trigger_system_column_refs(arg);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            rewrite_trigger_system_column_refs(&mut func.context);
            rewrite_trigger_system_column_refs(&mut func.path);
            for arg in &mut func.passing {
                rewrite_trigger_system_column_refs(&mut arg.expr);
            }
            if let Some(JsonTableBehavior::Default(expr)) = &mut func.on_empty {
                rewrite_trigger_system_column_refs(expr);
            }
            if let Some(JsonTableBehavior::Default(expr)) = &mut func.on_error {
                rewrite_trigger_system_column_refs(expr);
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            if let SqlCallArgs::Args(args) = args {
                for arg in args {
                    rewrite_trigger_system_column_refs(&mut arg.value);
                }
            }
            for item in order_by {
                rewrite_trigger_system_column_refs(&mut item.expr);
            }
            if let Some(filter) = filter {
                rewrite_trigger_system_column_refs(filter);
            }
            if let Some(over) = over {
                for expr in &mut over.partition_by {
                    rewrite_trigger_system_column_refs(expr);
                }
                for item in &mut over.order_by {
                    rewrite_trigger_system_column_refs(&mut item.expr);
                }
                if let Some(frame) = &mut over.frame {
                    rewrite_trigger_window_bound(&mut frame.start_bound);
                    rewrite_trigger_window_bound(&mut frame.end_bound);
                }
            }
        }
        SqlExpr::Const(_)
        | SqlExpr::Default
        | SqlExpr::ParamRef(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
    }
}

fn rewrite_trigger_window_bound(bound: &mut RawWindowFrameBound) {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) | RawWindowFrameBound::OffsetFollowing(expr) => {
            rewrite_trigger_system_column_refs(expr);
        }
        RawWindowFrameBound::UnboundedPreceding
        | RawWindowFrameBound::CurrentRow
        | RawWindowFrameBound::UnboundedFollowing => {}
    }
}

fn trigger_state_char(mode: AlterTableTriggerMode) -> char {
    match mode {
        AlterTableTriggerMode::Disable => TRIGGER_DISABLED,
        AlterTableTriggerMode::EnableOrigin => TRIGGER_ENABLED_ORIGIN,
        AlterTableTriggerMode::EnableReplica => TRIGGER_ENABLED_REPLICA,
        AlterTableTriggerMode::EnableAlways => TRIGGER_ENABLED_ALWAYS,
    }
}
