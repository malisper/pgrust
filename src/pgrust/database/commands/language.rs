use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterLanguageAction, AlterLanguageStatement, CreateLanguageStatement, DropLanguageStatement,
};
use crate::include::catalog::PgLanguageRow;
use crate::pgrust::database::ddl::ensure_can_set_role;

fn normalize_language_name(name: &str) -> String {
    name.trim_matches('"').to_ascii_lowercase()
}

fn lookup_language(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<Option<PgLanguageRow>, ExecError> {
    let normalized = normalize_language_name(name);
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .language_rows()
        .into_iter()
        .find(|row| row.lanname.eq_ignore_ascii_case(&normalized)))
}

fn language_owner_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of language {name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn language_duplicate_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("language \"{name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn ensure_language_owner(
    db: &Database,
    client_id: ClientId,
    row: &PgLanguageRow,
    name: &str,
    txn_ctx: CatalogTxnContext,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(row.lanowner, &auth_catalog) {
        return Ok(());
    }
    Err(language_owner_error(name))
}

fn commit_language_effect(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    effect: CatalogMutationEffect,
) -> Result<StatementResult, ExecError> {
    db.finish_txn(
        client_id,
        xid,
        Ok(StatementResult::AffectedRows(0)),
        &[effect],
        &[],
        &[],
    )
}

impl Database {
    pub(crate) fn execute_create_language_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateLanguageStatement,
    ) -> Result<StatementResult, ExecError> {
        let language_name = normalize_language_name(&stmt.language_name);
        if lookup_language(self, client_id, None, &language_name)?.is_some() {
            return Err(language_duplicate_error(&stmt.language_name));
        }
        let catalog = self.lazy_catalog_lookup(client_id, None, None);
        let handler_oid = catalog
            .proc_rows_by_name(&normalize_language_name(&stmt.handler_name))
            .into_iter()
            .find(|row| row.pronargs == 0)
            .map(|row| row.oid)
            .unwrap_or(0);
        let row = PgLanguageRow {
            oid: 0,
            lanname: language_name,
            lanowner: self.auth_state(client_id).current_user_oid(),
            lanispl: true,
            lanpltrusted: true,
            lanplcallfoid: handler_oid,
            laninline: 0,
            lanvalidator: 0,
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_, effect) = self
            .catalog
            .write()
            .create_language_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        let result = commit_language_effect(self, client_id, xid, effect);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_language_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterLanguageStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing =
            lookup_language(self, client_id, None, &stmt.language_name)?.ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("language \"{}\" does not exist", stmt.language_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        ensure_language_owner(self, client_id, &existing, &stmt.language_name, None)?;
        let mut replacement = existing.clone();
        match &stmt.action {
            AlterLanguageAction::Rename { new_name } => {
                let normalized = normalize_language_name(new_name);
                if lookup_language(self, client_id, None, &normalized)?.is_some() {
                    return Err(language_duplicate_error(new_name));
                }
                replacement.lanname = normalized;
            }
            AlterLanguageAction::OwnerTo { new_owner } => {
                let auth_catalog = self
                    .auth_catalog(client_id, None)
                    .map_err(map_catalog_error)?;
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("role \"{new_owner}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                replacement.lanowner = role.oid;
            }
        }

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_, effect) = self
            .catalog
            .write()
            .replace_language_mvcc(&existing, replacement, &ctx)
            .map_err(map_catalog_error)?;
        let result = commit_language_effect(self, client_id, xid, effect);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_language_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropLanguageStatement,
    ) -> Result<StatementResult, ExecError> {
        let Some(existing) = lookup_language(self, client_id, None, &stmt.language_name)? else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("language \"{}\" does not exist", stmt.language_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .drop_language_mvcc(&existing, &ctx)
            .map_err(map_catalog_error)?;
        let result = commit_language_effect(self, client_id, xid, effect);
        guard.disarm();
        result
    }
}
