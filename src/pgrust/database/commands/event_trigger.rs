use std::sync::Arc;

use super::super::*;
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::parser::{
    AlterEventTriggerOwnerStatement, AlterEventTriggerRenameStatement, AlterEventTriggerStatement,
    AlterTableTriggerMode, CatalogLookup, CommentOnEventTriggerStatement,
    CreateEventTriggerStatement, DropEventTriggerStatement,
};
use crate::backend::utils::cache::evtcache::event_trigger_cache;
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::{EVENT_TRIGGER_TYPE_OID, PG_LANGUAGE_SQL_OID, PgEventTriggerRow};
use crate::pl::plpgsql::{EventTriggerCallContext, execute_user_defined_event_trigger_function};

const EVENT_TRIGGER_DISABLED: char = 'D';
const EVENT_TRIGGER_ENABLED_ORIGIN: char = 'O';
const EVENT_TRIGGER_ENABLED_REPLICA: char = 'R';
const EVENT_TRIGGER_ENABLED_ALWAYS: char = 'A';

impl Database {
    pub(crate) fn fire_event_triggers_in_executor_context(
        &self,
        ctx: &mut ExecutorContext,
        event: &str,
        tag: &str,
    ) -> Result<(), ExecError> {
        self.fire_event_triggers_with_context(ctx, event, tag, Vec::new(), Vec::new(), None)
    }

    pub(crate) fn fire_event_triggers_with_ddl_commands_in_executor_context(
        &self,
        ctx: &mut ExecutorContext,
        event: &str,
        tag: &str,
        ddl_commands: Vec<crate::pl::plpgsql::EventTriggerDdlCommandRow>,
    ) -> Result<(), ExecError> {
        self.fire_event_triggers_with_context(ctx, event, tag, ddl_commands, Vec::new(), None)
    }

    pub(crate) fn fire_event_triggers_with_dropped_objects_in_executor_context(
        &self,
        ctx: &mut ExecutorContext,
        event: &str,
        tag: &str,
        dropped_objects: Vec<crate::pl::plpgsql::EventTriggerDroppedObjectRow>,
    ) -> Result<(), ExecError> {
        self.fire_event_triggers_with_context(ctx, event, tag, Vec::new(), dropped_objects, None)
    }

    pub(crate) fn fire_table_rewrite_event_in_executor_context(
        &self,
        ctx: &mut ExecutorContext,
        tag: &str,
        relation_oid: u32,
        reason: i32,
    ) -> Result<(), ExecError> {
        self.fire_event_triggers_with_context(
            ctx,
            "table_rewrite",
            tag,
            Vec::new(),
            Vec::new(),
            Some((relation_oid, reason)),
        )
    }

    pub(crate) fn event_trigger_may_fire(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        event: &str,
        tag: &str,
    ) -> Result<bool, ExecError> {
        let cache = event_trigger_cache(self, client_id, txn_ctx).map_err(ExecError::from)?;
        Ok(cache.may_fire(event, tag, self.session_replication_role(client_id)))
    }

    pub(crate) fn table_rewrite_event_trigger_may_fire(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        tag: &str,
    ) -> Result<bool, ExecError> {
        self.event_trigger_may_fire(client_id, txn_ctx, "table_rewrite", tag)
    }

    pub(crate) fn current_database_has_login_event_triggers(&self) -> Result<bool, ExecError> {
        Ok(self.current_database_row()?.dathasloginevt)
    }

    fn fire_event_triggers_with_context(
        &self,
        ctx: &mut ExecutorContext,
        event: &str,
        tag: &str,
        ddl_commands: Vec<crate::pl::plpgsql::EventTriggerDdlCommandRow>,
        dropped_objects: Vec<crate::pl::plpgsql::EventTriggerDroppedObjectRow>,
        table_rewrite: Option<(u32, i32)>,
    ) -> Result<(), ExecError> {
        if !event_triggers_guc_enabled(&ctx.gucs) {
            return Ok(());
        }
        let tag = tag.to_ascii_uppercase();
        let txn_ctx = (ctx.snapshot.current_xid != INVALID_TRANSACTION_ID)
            .then_some((ctx.snapshot.current_xid, ctx.next_command_id));
        let cache = event_trigger_cache(self, ctx.client_id, txn_ctx).map_err(ExecError::from)?;
        let rows = cache.matching_rows(event, &tag, ctx.session_replication_role);
        if rows.is_empty() {
            return Ok(());
        }
        let table_rewrite_relation_name = match table_rewrite {
            Some((oid, _)) => self
                .backend_catcache(ctx.client_id, txn_ctx)
                .map_err(ExecError::from)?
                .class_by_oid(oid)
                .map(|row| row.relname.clone()),
            None => None,
        };

        let call = EventTriggerCallContext {
            event: event.to_ascii_lowercase(),
            tag,
            ddl_commands,
            dropped_objects,
            table_rewrite_relation_oid: table_rewrite.map(|(oid, _)| oid),
            table_rewrite_relation_name,
            table_rewrite_reason: table_rewrite.map(|(_, reason)| reason),
        };
        for row in rows {
            execute_user_defined_event_trigger_function(row.evtfoid, &call, ctx)?;
        }
        Ok(())
    }

    pub(crate) fn execute_create_event_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateEventTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_event_trigger_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_event_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateEventTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let current_role = ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::Create(&stmt.trigger_name),
        )?;
        validate_event_trigger_event(&stmt.event_name)?;
        let tags = normalize_event_trigger_when_clauses(stmt)?;
        let event_name = stmt.event_name.to_ascii_lowercase();
        let function = resolve_event_trigger_function(
            self,
            client_id,
            Some((xid, cid)),
            stmt,
            configured_search_path,
        )?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache
            .event_trigger_row_by_name(&stmt.trigger_name)
            .is_some()
        {
            return Err(ExecError::DetailedError {
                message: format!("event trigger \"{}\" already exists", stmt.trigger_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let row = PgEventTriggerRow {
            oid: 0,
            evtname: stmt.trigger_name.to_ascii_lowercase(),
            evtevent: event_name.clone(),
            evtowner: current_role.oid,
            evtfoid: function.oid,
            evtenabled: EVENT_TRIGGER_ENABLED_ORIGIN,
            evttags: tags,
        };
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let (_oid, effect) = self
            .catalog
            .write()
            .create_event_trigger_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        if event_name == "login" {
            self.set_current_database_has_login_event_triggers(
                client_id,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_event_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_event_trigger_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_event_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::Alter(&stmt.trigger_name),
        )?;
        let mut row =
            lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.trigger_name)?;
        row.evtenabled = event_trigger_enabled_char(stmt.mode);
        let enables_login =
            row.evtevent.eq_ignore_ascii_case("login") && row.evtenabled != EVENT_TRIGGER_DISABLED;
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_event_trigger_mvcc(&stmt.trigger_name, row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        if enables_login {
            self.set_current_database_has_login_event_triggers(
                client_id,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_event_trigger_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerOwnerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_event_trigger_owner_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_event_trigger_owner_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerOwnerStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::ChangeOwner(&stmt.trigger_name),
        )?;
        let mut row =
            lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.trigger_name)?;
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let new_owner = auth_catalog
            .role_by_name(&stmt.new_owner.to_ascii_lowercase())
            .ok_or_else(|| role_not_found_error(&stmt.new_owner))?;
        if !new_owner.rolsuper {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to change owner of event trigger \"{}\"",
                    stmt.trigger_name
                ),
                detail: None,
                hint: Some("The owner of an event trigger must be a superuser.".into()),
                sqlstate: "42501",
            });
        }
        row.evtowner = new_owner.oid;
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_event_trigger_mvcc(&stmt.trigger_name, row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_event_trigger_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_event_trigger_rename_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_event_trigger_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterEventTriggerRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::Alter(&stmt.trigger_name),
        )?;
        let mut row =
            lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.trigger_name)?;
        if lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.new_trigger_name)
            .is_ok()
        {
            return Err(ExecError::DetailedError {
                message: format!("event trigger \"{}\" already exists", stmt.new_trigger_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        row.evtname = stmt.new_trigger_name.to_ascii_lowercase();
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_event_trigger_mvcc(&stmt.trigger_name, row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_event_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropEventTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_event_trigger_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_event_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropEventTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::Alter(&stmt.trigger_name),
        )?;
        if lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.trigger_name).is_err()
        {
            if stmt.if_exists {
                push_notice(format!(
                    "event trigger \"{}\" does not exist, skipping",
                    stmt.trigger_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(missing_event_trigger_error(&stmt.trigger_name));
        }
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let (_row, effect) = self
            .catalog
            .write()
            .drop_event_trigger_mvcc(&stmt.trigger_name, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_event_trigger_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnEventTriggerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_event_trigger_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_comment_on_event_trigger_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnEventTriggerStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_event_trigger_superuser(
            self,
            client_id,
            Some((xid, cid)),
            EventTriggerSuperuserAction::Alter(&stmt.trigger_name),
        )?;
        let row = lookup_event_trigger_row(self, client_id, Some((xid, cid)), &stmt.trigger_name)?;
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let effect = self
            .catalog
            .write()
            .comment_event_trigger_mvcc(row.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

impl Database {
    fn current_database_row(&self) -> Result<crate::include::catalog::PgDatabaseRow, ExecError> {
        self.shared_catalog
            .read()
            .catcache()
            .map_err(map_catalog_error)?
            .database_rows()
            .into_iter()
            .find(|row| row.oid == self.database_oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("database with oid {} does not exist", self.database_oid),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            })
    }

    fn set_current_database_has_login_event_triggers(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let mut row = self.current_database_row()?;
        if row.dathasloginevt {
            return Ok(());
        }
        row.dathasloginevt = true;
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let effect = self
            .shared_catalog
            .write()
            .replace_database_row_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(())
    }
}

fn catalog_write_context(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
) -> CatalogWriteContext {
    CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid,
        client_id,
        waiter: None,
        interrupts: Arc::clone(&db.interrupt_state(client_id)),
    }
}

enum EventTriggerSuperuserAction<'a> {
    Create(&'a str),
    Alter(&'a str),
    ChangeOwner(&'a str),
}

fn ensure_event_trigger_superuser(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    action: EventTriggerSuperuserAction<'_>,
) -> Result<crate::include::catalog::PgAuthIdRow, ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let current = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .ok_or_else(current_role_missing_error)?;
    if current.rolsuper {
        Ok(current.clone())
    } else {
        let (message, hint) = match action {
            EventTriggerSuperuserAction::Create(name) => (
                format!("permission denied to create event trigger \"{name}\""),
                "Must be superuser to create an event trigger.",
            ),
            EventTriggerSuperuserAction::Alter(name) => (
                format!("permission denied to alter event trigger \"{name}\""),
                "Must be superuser to alter an event trigger.",
            ),
            EventTriggerSuperuserAction::ChangeOwner(name) => (
                format!("permission denied to change owner of event trigger \"{name}\""),
                "The owner of an event trigger must be a superuser.",
            ),
        };
        Err(ExecError::DetailedError {
            message,
            detail: None,
            hint: Some(hint.into()),
            sqlstate: "42501",
        })
    }
}

fn lookup_event_trigger_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<PgEventTriggerRow, ExecError> {
    let catcache = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    catcache
        .event_trigger_row_by_name(name)
        .cloned()
        .ok_or_else(|| missing_event_trigger_error(name))
}

fn missing_event_trigger_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("event trigger \"{name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn role_not_found_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn current_role_missing_error() -> ExecError {
    ExecError::DetailedError {
        message: "current user role does not exist".into(),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn event_trigger_enabled_char(mode: AlterTableTriggerMode) -> char {
    match mode {
        AlterTableTriggerMode::Disable => EVENT_TRIGGER_DISABLED,
        AlterTableTriggerMode::EnableOrigin => EVENT_TRIGGER_ENABLED_ORIGIN,
        AlterTableTriggerMode::EnableReplica => EVENT_TRIGGER_ENABLED_REPLICA,
        AlterTableTriggerMode::EnableAlways => EVENT_TRIGGER_ENABLED_ALWAYS,
    }
}

fn event_triggers_guc_enabled(gucs: &std::collections::HashMap<String, String>) -> bool {
    !gucs.get("event_triggers").is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "off" | "false" | "0" | "no"
        )
    })
}

fn validate_event_trigger_event(event_name: &str) -> Result<(), ExecError> {
    match event_name.to_ascii_lowercase().as_str() {
        "ddl_command_start" | "ddl_command_end" | "sql_drop" | "login" | "table_rewrite" => Ok(()),
        _ => Err(ExecError::DetailedError {
            message: format!("unrecognized event name \"{}\"", event_name),
            detail: None,
            hint: None,
            sqlstate: "42601",
        }),
    }
}

fn normalize_event_trigger_when_clauses(
    stmt: &CreateEventTriggerStatement,
) -> Result<Option<Vec<String>>, ExecError> {
    let is_login_event = stmt.event_name.eq_ignore_ascii_case("login");
    let mut saw_tag = false;
    let mut tags = Vec::new();
    for clause in &stmt.when_clauses {
        if !clause.variable.eq_ignore_ascii_case("tag") {
            return Err(ExecError::DetailedError {
                message: format!("unrecognized filter variable \"{}\"", clause.variable),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        if saw_tag {
            return Err(ExecError::DetailedError {
                message: "filter variable \"tag\" specified more than once".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        saw_tag = true;
        for value in &clause.values {
            if !is_login_event {
                validate_event_trigger_tag(value)?;
            }
            tags.push(value.to_ascii_uppercase());
        }
    }
    if is_login_event && saw_tag {
        return Err(ExecError::DetailedError {
            message: "tag filtering is not supported for login event triggers".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if tags.is_empty() {
        Ok(None)
    } else {
        tags.sort();
        tags.dedup();
        Ok(Some(tags))
    }
}

fn validate_event_trigger_tag(tag: &str) -> Result<(), ExecError> {
    let normalized = tag.to_ascii_uppercase();
    if matches!(
        normalized.as_str(),
        "CREATE EVENT TRIGGER"
            | "ALTER EVENT TRIGGER"
            | "DROP EVENT TRIGGER"
            | "CREATE DATABASE"
            | "DROP DATABASE"
            | "CREATE TABLESPACE"
            | "DROP TABLESPACE"
            | "CREATE ROLE"
            | "ALTER ROLE"
            | "DROP ROLE"
    ) {
        return Err(ExecError::DetailedError {
            message: format!("event triggers are not supported for {}", tag),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if !event_trigger_tag_is_known(&normalized) {
        return Err(ExecError::DetailedError {
            message: format!(
                "filter value \"{}\" not recognized for filter variable \"tag\"",
                tag
            ),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    Ok(())
}

fn event_trigger_tag_is_known(tag: &str) -> bool {
    matches!(
        tag,
        "ALTER DEFAULT PRIVILEGES"
            | "ALTER POLICY"
            | "ALTER TABLE"
            | "COMMENT"
            | "CREATE AGGREGATE"
            | "CREATE FOREIGN DATA WRAPPER"
            | "CREATE FUNCTION"
            | "CREATE INDEX"
            | "CREATE MATERIALIZED VIEW"
            | "CREATE OPERATOR CLASS"
            | "CREATE OPERATOR FAMILY"
            | "CREATE POLICY"
            | "CREATE PROCEDURE"
            | "CREATE SCHEMA"
            | "CREATE SERVER"
            | "CREATE TABLE"
            | "CREATE TYPE"
            | "CREATE USER MAPPING"
            | "CREATE VIEW"
            | "DROP AGGREGATE"
            | "DROP FUNCTION"
            | "DROP INDEX"
            | "DROP MATERIALIZED VIEW"
            | "DROP OWNED"
            | "DROP POLICY"
            | "DROP PROCEDURE"
            | "DROP ROUTINE"
            | "DROP SCHEMA"
            | "DROP TABLE"
            | "DROP VIEW"
            | "GRANT"
            | "REINDEX"
            | "REVOKE"
    )
}

fn resolve_event_trigger_function(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    stmt: &CreateEventTriggerStatement,
    configured_search_path: Option<&[String]>,
) -> Result<crate::include::catalog::PgProcRow, ExecError> {
    let proname = stmt.function_name.to_ascii_lowercase();
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
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
    if function.prolang == PG_LANGUAGE_SQL_OID {
        return Err(ExecError::DetailedError {
            message: "SQL functions cannot return type event_trigger".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if function.prorettype != EVENT_TRIGGER_TYPE_OID {
        return Err(ExecError::DetailedError {
            message: format!("function {} must return type event_trigger", proname),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if function.pronargs != 0 {
        return Err(ExecError::DetailedError {
            message: "event trigger functions cannot have declared arguments".into(),
            detail: None,
            hint: None,
            sqlstate: "42P13",
        });
    }
    Ok(function)
}

fn resolve_function_schema_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: &str,
) -> Result<u32, ExecError> {
    db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("schema \"{schema_name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "3F000",
        })
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
    out
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::{ExecError, StatementResult, Value};
    use crate::pgrust::database::Database;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pgrust_event_trigger_{label}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn create_login_event_trigger_function(db: &Database, name: &str) {
        db.execute(
            1,
            &format!(
                "create function {name}() returns event_trigger as $$ begin end; $$ language plpgsql"
            ),
        )
        .unwrap();
    }

    fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(1, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn login_event_trigger_create_sets_event_and_database_flag() {
        let db = Database::open(temp_dir("login_create_sets_flag"), 16).unwrap();
        create_login_event_trigger_function(&db, "login_proc");

        db.execute(
            1,
            "create event trigger login_et on login execute procedure login_proc()",
        )
        .unwrap();

        assert_eq!(
            query_rows(
                &db,
                "select evtevent from pg_event_trigger where evtname = 'login_et'"
            ),
            vec![vec![Value::Text("login".into())]]
        );
        assert_eq!(
            query_rows(
                &db,
                "select dathasloginevt from pg_database where datname = 'postgres'"
            ),
            vec![vec![Value::Bool(true)]]
        );
    }

    #[test]
    fn login_event_trigger_tag_filter_is_not_supported() {
        let db = Database::open(temp_dir("login_tag_filter"), 16).unwrap();
        create_login_event_trigger_function(&db, "login_tag_proc");

        let err = db
            .execute(
                1,
                "create event trigger login_tag_et on login when tag in ('create table') execute procedure login_tag_proc()",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "tag filtering is not supported for login event triggers"
                );
                assert_eq!(sqlstate, "0A000");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn login_event_trigger_enable_sets_database_flag() {
        let db = Database::open(temp_dir("login_enable_sets_flag"), 16).unwrap();
        create_login_event_trigger_function(&db, "login_enable_proc");

        db.execute(
            1,
            "create event trigger login_enable_et on login execute procedure login_enable_proc()",
        )
        .unwrap();
        db.execute(1, "alter event trigger login_enable_et disable")
            .unwrap();
        db.execute(
            1,
            "update pg_database set dathasloginevt = false where datname = 'postgres'",
        )
        .unwrap();
        db.execute(1, "alter event trigger login_enable_et enable always")
            .unwrap();

        assert_eq!(
            query_rows(
                &db,
                "select dathasloginevt from pg_database where datname = 'postgres'"
            ),
            vec![vec![Value::Bool(true)]]
        );
    }
}
