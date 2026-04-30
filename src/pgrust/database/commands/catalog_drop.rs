use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{DropAccessMethodStatement, DropExtensionStatement};
use crate::backend::utils::misc::notices::push_notice;

fn ensure_current_user_superuser(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    object_kind: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be superuser to drop {object_kind}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

impl Database {
    pub(crate) fn execute_drop_extension_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropExtensionStatement,
    ) -> Result<StatementResult, ExecError> {
        for extension_name in &stmt.extension_names {
            if stmt.if_exists {
                push_notice(format!(
                    "extension \"{extension_name}\" does not exist, skipping"
                ));
                continue;
            }
            return Err(ExecError::DetailedError {
                message: format!("extension \"{extension_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        }
        let _ = client_id;
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_access_method_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropAccessMethodStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_access_method_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_access_method_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &DropAccessMethodStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        ensure_current_user_superuser(self, client_id, txn_ctx, "access method")?;
        let catcache = self
            .backend_catcache(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        let am_rows = catcache.am_rows();
        let class_rows = catcache.class_rows();
        let opfamily_rows = catcache.opfamily_rows();
        let opclass_rows = catcache.opclass_rows();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };

        for access_method_name in &stmt.access_method_names {
            let Some(row) = am_rows
                .iter()
                .find(|row| row.amname.eq_ignore_ascii_case(access_method_name))
                .cloned()
            else {
                if stmt.if_exists {
                    push_notice(format!(
                        "access method \"{access_method_name}\" does not exist, skipping"
                    ));
                    continue;
                }
                return Err(ExecError::DetailedError {
                    message: format!("access method \"{access_method_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                });
            };

            if let Some(class_row) = class_rows
                .iter()
                .find(|class_row| class_row.relam == row.oid)
            {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop access method {} because other objects depend on it",
                        row.amname
                    ),
                    detail: Some(format!(
                        "relation {} depends on access method {}",
                        class_row.relname, row.amname
                    )),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }
            if let Some(opfamily_row) = opfamily_rows
                .iter()
                .find(|opfamily_row| opfamily_row.opfmethod == row.oid)
            {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop access method {} because other objects depend on it",
                        row.amname
                    ),
                    detail: Some(format!(
                        "operator family {} depends on access method {}",
                        opfamily_row.opfname, row.amname
                    )),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }
            if let Some(opclass_row) = opclass_rows
                .iter()
                .find(|opclass_row| opclass_row.opcmethod == row.oid)
            {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop access method {} because other objects depend on it",
                        row.amname
                    ),
                    detail: Some(format!(
                        "operator class {} depends on access method {}",
                        opclass_row.opcname, row.amname
                    )),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }

            let effect = self
                .catalog
                .write()
                .drop_access_method_mvcc(row, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }

        if !catalog_effects.is_empty() {
            self.plan_cache.invalidate_all();
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
