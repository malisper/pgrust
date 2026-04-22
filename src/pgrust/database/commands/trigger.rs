use std::collections::BTreeSet;
use std::sync::Arc;

use super::super::*;
use crate::backend::parser::{
    CatalogLookup, CommentOnTriggerStatement, CreateTriggerStatement, DropTriggerStatement,
    ParseError, SqlTypeKind, TriggerEvent, TriggerTiming, bind_scalar_expr_in_named_relation_scope,
    parse_expr,
};
use crate::include::catalog::{PG_LANGUAGE_PLPGSQL_OID, PgTriggerRow};
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_heap_relation_for_ddl};

const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;

impl Database {
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &relation_name)?;
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
        validate_trigger_stmt(stmt, &relation.desc, &catalog)?;

        let tgattr = trigger_update_attnums(stmt, &relation.desc)?;
        let tgtype = trigger_type_bits(stmt);
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
            tgoldtable: None,
            tgnewtable: None,
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
        let effect = {
            let mut catalog_store = self.catalog.write();
            if let Some(existing) = existing {
                catalog_store
                    .replace_trigger_mvcc(&existing, trigger_row, &ctx)
                    .map_err(map_catalog_error)?
                    .1
            } else {
                catalog_store
                    .create_trigger_mvcc(trigger_row, &ctx)
                    .map_err(map_catalog_error)?
                    .1
            }
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &relation_name)?;
        ensure_relation_owner(self, client_id, &relation, &relation_name)?;

        let Some(_existing) = catalog
            .trigger_rows_for_relation(relation.relation_oid)
            .into_iter()
            .find(|row| row.tgname.eq_ignore_ascii_case(&stmt.trigger_name))
        else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "trigger \"{}\" for relation \"{}\" does not exist",
                    stmt.trigger_name, relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (_removed, effect) = self
            .catalog
            .write()
            .drop_trigger_mvcc(relation.relation_oid, &stmt.trigger_name, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &stmt.table_name)?;
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &stmt.table_name)?;
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
}

fn format_trigger_relation_name(stmt: &CreateTriggerStatement) -> String {
    stmt.schema_name
        .as_deref()
        .map(|schema| format!("{schema}.{}", stmt.table_name))
        .unwrap_or_else(|| stmt.table_name.clone())
}

fn format_trigger_drop_relation_name(stmt: &DropTriggerStatement) -> String {
    stmt.schema_name
        .as_deref()
        .map(|schema| format!("{schema}.{}", stmt.table_name))
        .unwrap_or_else(|| stmt.table_name.clone())
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
    if function.prolang != PG_LANGUAGE_PLPGSQL_OID
        || catalog
            .language_row_by_oid(function.prolang)
            .is_none_or(|row| row.oid != PG_LANGUAGE_PLPGSQL_OID)
    {
        return Err(ExecError::DetailedError {
            message: format!("trigger function {} must be written in plpgsql", proname),
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
    desc: &crate::backend::executor::RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for event in &stmt.events {
        if event.event == TriggerEvent::Update {
            let mut seen = BTreeSet::new();
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
        let parsed = parse_expr(when_clause_sql).map_err(ExecError::Parse)?;
        for event in &stmt.events {
            let mut relation_scopes = Vec::new();
            if stmt.level == crate::backend::parser::TriggerLevel::Row {
                match event.event {
                    TriggerEvent::Insert => relation_scopes.push(("new", desc)),
                    TriggerEvent::Update => {
                        relation_scopes.push(("new", desc));
                        relation_scopes.push(("old", desc));
                    }
                    TriggerEvent::Delete => relation_scopes.push(("old", desc)),
                }
            }
            let (_, when_type) =
                bind_scalar_expr_in_named_relation_scope(&parsed, &relation_scopes, &[], catalog)
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
    if stmt.level == crate::backend::parser::TriggerLevel::Row {
        bits |= TRIGGER_TYPE_ROW;
    }
    if stmt.timing == TriggerTiming::Before {
        bits |= TRIGGER_TYPE_BEFORE;
    }
    for event in &stmt.events {
        bits |= match event.event {
            TriggerEvent::Insert => TRIGGER_TYPE_INSERT,
            TriggerEvent::Delete => TRIGGER_TYPE_DELETE,
            TriggerEvent::Update => TRIGGER_TYPE_UPDATE,
        };
    }
    bits
}
