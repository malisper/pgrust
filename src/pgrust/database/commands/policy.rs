use super::super::*;
use crate::backend::parser::{
    AlterPolicyAction, AlterPolicyStatement, CatalogLookup, CreatePolicyStatement,
    DropPolicyStatement, ParseError, SqlTypeKind, bind_scalar_expr_in_named_relation_scope,
};
use crate::include::catalog::PgPolicyRow;
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_heap_relation_for_ddl};

const PUBLIC_ROLE_OID: u32 = 0;

impl Database {
    pub(crate) fn execute_create_policy_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePolicyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_policy_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_policy_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePolicyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        validate_policy_stmt(
            &catalog,
            &relation.desc,
            &stmt.table_name,
            stmt.using_expr.as_ref(),
            stmt.with_check_expr.as_ref(),
        )?;

        if catalog
            .policy_rows_for_relation(relation.relation_oid)
            .into_iter()
            .any(|row| row.polname.eq_ignore_ascii_case(&stmt.policy_name))
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "policy \"{}\" for table \"{}\" already exists",
                    stmt.policy_name, stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let policy_row = PgPolicyRow {
            oid: 0,
            polname: stmt.policy_name.to_ascii_lowercase(),
            polrelid: relation.relation_oid,
            polcmd: stmt.command,
            polpermissive: stmt.permissive,
            polroles: resolve_policy_roles(self, client_id, &stmt.role_names)?,
            polqual: stmt.using_sql.clone(),
            polwithcheck: stmt.with_check_sql.clone(),
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
        let (_oid, effect) = self
            .catalog
            .write()
            .create_policy_mvcc(policy_row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_policy_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPolicyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_policy_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_policy_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPolicyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        let existing = catalog
            .policy_rows_for_relation(relation.relation_oid)
            .into_iter()
            .find(|row| row.polname.eq_ignore_ascii_case(&stmt.policy_name))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "policy \"{}\" for table \"{}\" does not exist",
                    stmt.policy_name, stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;

        let updated = match &stmt.action {
            AlterPolicyAction::Rename { new_name } => PgPolicyRow {
                polname: new_name.to_ascii_lowercase(),
                ..existing.clone()
            },
            AlterPolicyAction::Update {
                role_names,
                using_expr,
                using_sql,
                with_check_expr,
                with_check_sql,
            } => {
                validate_policy_stmt(
                    &catalog,
                    &relation.desc,
                    &stmt.table_name,
                    using_expr.as_ref(),
                    with_check_expr.as_ref(),
                )?;
                PgPolicyRow {
                    polroles: role_names
                        .as_ref()
                        .map(|names| resolve_policy_roles(self, client_id, names))
                        .transpose()?
                        .unwrap_or_else(|| existing.polroles.clone()),
                    polqual: using_sql.clone().or_else(|| existing.polqual.clone()),
                    polwithcheck: with_check_sql
                        .clone()
                        .or_else(|| existing.polwithcheck.clone()),
                    ..existing.clone()
                }
            }
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
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_policy_mvcc(&existing, updated, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_policy_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPolicyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_policy_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_policy_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPolicyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        let Some(_existing) = catalog
            .policy_rows_for_relation(relation.relation_oid)
            .into_iter()
            .find(|row| row.polname.eq_ignore_ascii_case(&stmt.policy_name))
        else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "policy \"{}\" for table \"{}\" does not exist",
                    stmt.policy_name, stmt.table_name
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
            .drop_policy_mvcc(relation.relation_oid, &stmt.policy_name, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

fn resolve_policy_roles(
    db: &Database,
    client_id: ClientId,
    role_names: &[String],
) -> Result<Vec<u32>, ExecError> {
    let auth_catalog = db.auth_catalog(client_id, None).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "authorization catalog",
            actual: format!("{err:?}"),
        })
    })?;
    role_names
        .iter()
        .map(|role_name| {
            if role_name.eq_ignore_ascii_case("public") {
                return Ok(PUBLIC_ROLE_OID);
            }
            auth_catalog
                .role_by_name(role_name)
                .map(|row| row.oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("role \"{role_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })
        })
        .collect()
}

fn validate_policy_stmt(
    catalog: &dyn CatalogLookup,
    desc: &crate::backend::executor::RelationDesc,
    relation_name: &str,
    using_expr: Option<&crate::backend::parser::SqlExpr>,
    with_check_expr: Option<&crate::backend::parser::SqlExpr>,
) -> Result<(), ExecError> {
    for expr in [using_expr, with_check_expr].into_iter().flatten() {
        let (_, expr_type) =
            bind_scalar_expr_in_named_relation_scope(expr, &[(relation_name, desc)], &[], catalog)
                .map_err(ExecError::Parse)?;
        if expr_type.kind != SqlTypeKind::Bool {
            return Err(ExecError::DetailedError {
                message: "policy expression must return type boolean".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    Ok(())
}
