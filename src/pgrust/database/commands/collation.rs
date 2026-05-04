use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    CreateCollationKind, CreateCollationStatement, DropCollationStatement,
};
use crate::include::catalog::{PG_CATALOG_NAMESPACE_OID, PgCollationRow};
use pgrust_commands::collation::{
    collation_row_by_name_namespace, create_collation_row_from_options, split_schema_qualified_name,
};

fn collation_error_to_exec(error: pgrust_commands::collation::CollationError) -> ExecError {
    match error {
        pgrust_commands::collation::CollationError::Detailed {
            message,
            detail,
            hint,
            sqlstate,
        } => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
    }
}

impl Database {
    pub(crate) fn execute_create_collation_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateCollationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_collation_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_collation_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateCollationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let ((schema_name, object_name), namespace_oid) = resolve_collation_create_name(
            self,
            client_id,
            Some((xid, cid)),
            &stmt.collation_name,
            configured_search_path,
        )?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if collation_row_by_name_namespace(&catcache.collation_rows(), namespace_oid, &object_name)
            .is_some()
        {
            return Err(ExecError::DetailedError {
                message: format!("collation \"{}\" already exists", object_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let current_user_oid = self.auth_state(client_id).current_user_oid();
        let row = match &stmt.kind {
            CreateCollationKind::From { source_collation } => {
                if source_collation.eq_ignore_ascii_case("default")
                    || source_collation.eq_ignore_ascii_case("pg_catalog.default")
                {
                    return Err(ExecError::DetailedError {
                        message: "collation \"default\" cannot be copied".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                let source = resolve_collation_row(
                    self,
                    client_id,
                    Some((xid, cid)),
                    source_collation,
                    configured_search_path,
                )?
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "collation \"{}\" for encoding \"UTF8\" does not exist",
                        source_collation
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
                PgCollationRow {
                    oid: 0,
                    collname: object_name,
                    collnamespace: namespace_oid,
                    collowner: current_user_oid,
                    collprovider: source.collprovider,
                    collisdeterministic: source.collisdeterministic,
                    collencoding: source.collencoding,
                    collcollate: source.collcollate,
                    collctype: source.collctype,
                    colllocale: source.colllocale,
                    collicurules: source.collicurules,
                    collversion: source.collversion,
                }
            }
            CreateCollationKind::Options { options } => create_collation_row_from_options(
                object_name,
                namespace_oid,
                current_user_oid,
                options,
            )
            .map_err(collation_error_to_exec)?,
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_oid, effect) = self
            .catalog
            .write()
            .create_collation_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        let _ = schema_name;
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_collation_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropCollationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_collation_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_collation_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropCollationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let Some(row) = resolve_collation_row(
            self,
            client_id,
            Some((xid, cid)),
            &stmt.collation_name,
            configured_search_path,
        )?
        else {
            if stmt.if_exists {
                if let Some((schema_name, _)) = stmt.collation_name.split_once('.')
                    && self
                        .visible_namespace_oid_by_name(client_id, Some((xid, cid)), schema_name)
                        .is_none()
                {
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "schema \"{schema_name}\" does not exist, skipping"
                    ));
                } else {
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "collation \"{}\" does not exist, skipping",
                        stmt.collation_name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&stmt.collation_name)
                    ));
                }
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("collation \"{}\" does not exist", stmt.collation_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        if row.collnamespace == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::DetailedError {
                message: format!("permission denied to drop collation \"{}\"", row.collname),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        ensure_collation_owner(self, client_id, Some((xid, cid)), &row)?;
        if !stmt.cascade
            && let Some((relation_name, column_name)) =
                find_column_depending_on_collation(self, client_id, Some((xid, cid)), row.oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop collation {} because other objects depend on it",
                    row.collname
                ),
                detail: Some(format!(
                    "column {} of table {} depends on collation {}",
                    column_name, relation_name, row.collname
                )),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .drop_collation_mvcc(&row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }
}

fn find_column_depending_on_collation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    collation_oid: u32,
) -> Option<(String, String)> {
    let catcache = db.backend_catcache(client_id, txn_ctx).ok()?;
    let classes = catcache.class_rows();
    catcache
        .attribute_rows()
        .into_iter()
        .filter(|attr| attr.attnum > 0 && !attr.attisdropped && attr.attcollation == collation_oid)
        .find_map(|attr| {
            classes
                .iter()
                .find(|class| class.oid == attr.attrelid)
                .map(|class| (class.relname.clone(), attr.attname))
        })
}

fn resolve_collation_create_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<((String, String), u32), ExecError> {
    let (schema_name, object_name) = split_schema_qualified_name(raw_name);
    if let Some(schema_name) = schema_name {
        let namespace_oid = db
            .visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{}\" does not exist", schema_name),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        return Ok(((schema_name, object_name), namespace_oid));
    }
    for schema_name in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema_name.as_str(), "" | "$user" | "pg_catalog") {
            continue;
        }
        if let Some(namespace_oid) =
            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name)
        {
            return Ok(((schema_name, object_name), namespace_oid));
        }
    }
    Ok((
        ("public".into(), object_name),
        crate::include::catalog::PUBLIC_NAMESPACE_OID,
    ))
}

fn resolve_collation_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<Option<PgCollationRow>, ExecError> {
    let rows = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .collation_rows();
    let (schema_name, object_name) = split_schema_qualified_name(raw_name);
    if let Some(schema_name) = schema_name {
        let Some(namespace_oid) =
            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name)
        else {
            return Ok(None);
        };
        return Ok(collation_row_by_name_namespace(
            &rows,
            namespace_oid,
            &object_name,
        ));
    }
    for schema_name in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema_name.as_str(), "" | "$user") {
            continue;
        }
        let Some(namespace_oid) =
            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name)
        else {
            continue;
        };
        if let Some(row) = collation_row_by_name_namespace(&rows, namespace_oid, &object_name) {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn ensure_collation_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    row: &PgCollationRow,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(row.collowner, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of collation {}", row.collname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}
