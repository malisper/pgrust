use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterConversionAction, AlterConversionStatement, CommentOnConversionStatement,
    CreateConversionStatement, DropConversionStatement,
};
use crate::pgrust::database::ddl::ensure_can_set_role;
use pgrust_commands::conversion::{conversion_object_name, conversion_row_from_entry};

impl Database {
    pub(crate) fn execute_create_conversion_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_conversion_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_conversion_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateConversionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, object_name, namespace_oid, _) = resolve_conversion_create_name(
            self,
            client_id,
            Some((xid, cid)),
            &stmt.conversion_name,
            configured_search_path,
        )?;
        let current_user_oid = self.auth_state(client_id).current_user_oid();
        let mut conversions = self.conversions.write();
        if conversions.contains_key(&normalized) {
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" already exists", object_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let for_encoding = stmt.for_encoding.to_ascii_uppercase();
        let to_encoding = stmt.to_encoding.to_ascii_uppercase();
        if stmt.is_default
            && conversions.values().any(|existing| {
                existing.namespace_oid == namespace_oid
                    && existing.is_default
                    && existing.for_encoding.eq_ignore_ascii_case(&for_encoding)
                    && existing.to_encoding.eq_ignore_ascii_case(&to_encoding)
            })
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "default conversion for {} to {} already exists",
                    for_encoding, to_encoding
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let mut entry = ConversionEntry {
            oid: 0,
            name: object_name,
            namespace_oid,
            for_encoding,
            to_encoding,
            function_name: stmt.function_name.to_ascii_lowercase(),
            is_default: stmt.is_default,
            owner_oid: current_user_oid,
            comment: None,
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
        let (oid, effect) = self
            .catalog
            .write()
            .create_conversion_mvcc(conversion_row_from_entry(&entry), &ctx)
            .map_err(map_catalog_error)?;
        entry.oid = oid;
        catalog_effects.push(effect);
        conversions.insert(normalized, entry);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_conversion_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_conversion_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_conversion_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropConversionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let mut conversions = self.conversions.write();
        let Some((storage_key, _object_name)) = conversion_lookup_storage_key(
            self,
            client_id,
            Some((xid, cid)),
            &conversions,
            &stmt.conversion_name,
            configured_search_path,
        )?
        .map(|(key, entry, _)| (key, entry.name.clone())) else {
            if stmt.if_exists {
                if let Some((schema_name, _)) = stmt.conversion_name.split_once('.')
                    && self
                        .visible_namespace_oid_by_name(client_id, Some((xid, cid)), schema_name)
                        .is_none()
                {
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "schema \"{schema_name}\" does not exist, skipping"
                    ));
                } else {
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "conversion \"{}\" does not exist, skipping",
                        conversion_object_name(&stmt.conversion_name)
                    ));
                }
                return Ok(StatementResult::AffectedRows(0));
            }
            let object_name = conversion_object_name(&stmt.conversion_name);
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" does not exist", object_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        let entry = conversions
            .remove(&storage_key)
            .expect("lookup returned key");
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
            .drop_conversion_mvcc(&conversion_row_from_entry(&entry), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_conversion_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let mut conversions = self.conversions.write();
        let Some((storage_key, entry, _)) = conversion_lookup_storage_key(
            self,
            client_id,
            None,
            &conversions,
            &stmt.conversion_name,
            configured_search_path,
        )?
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "conversion \"{}\" does not exist",
                    conversion_object_name(&stmt.conversion_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        let Some(conversion) = conversions.get_mut(&storage_key) else {
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" does not exist", entry.name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        conversion.comment = stmt.comment.clone();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_conversion_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_conversion_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_conversion_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterConversionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let mut conversions = self.conversions.write();
        let Some((old_key, old_entry, _)) = conversion_lookup_storage_key(
            self,
            client_id,
            Some((xid, cid)),
            &conversions,
            &stmt.conversion_name,
            configured_search_path,
        )?
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "conversion \"{}\" does not exist",
                    conversion_object_name(&stmt.conversion_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        ensure_conversion_owner(self, client_id, Some((xid, cid)), &old_entry)?;
        let mut updated = old_entry.clone();
        let mut new_key = old_key.clone();
        match &stmt.action {
            AlterConversionAction::Rename { new_name } => {
                updated.name = new_name.to_ascii_lowercase();
                new_key = format!(
                    "{}.{}",
                    namespace_name(self, client_id, Some((xid, cid)), updated.namespace_oid)?,
                    updated.name
                );
            }
            AlterConversionAction::OwnerTo { new_owner } => {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{new_owner}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                updated.owner_oid = role.oid;
            }
            AlterConversionAction::SetSchema { new_schema } => {
                updated.namespace_oid = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                new_key = format!("{}.{}", new_schema.to_ascii_lowercase(), updated.name);
            }
        }
        if new_key != old_key && conversions.contains_key(&new_key) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "conversion \"{}\" already exists in schema \"{}\"",
                    updated.name,
                    namespace_name(self, client_id, Some((xid, cid)), updated.namespace_oid)?
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
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_conversion_mvcc(
                &conversion_row_from_entry(&old_entry),
                conversion_row_from_entry(&updated),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        conversions.remove(&old_key);
        conversions.insert(new_key, updated);
        catalog_effects.push(effect);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }
}

fn conversion_lookup_storage_key(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    conversions: &std::collections::BTreeMap<String, ConversionEntry>,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<Option<(String, ConversionEntry, u32)>, ExecError> {
    let lowered = raw_name.to_ascii_lowercase();
    if lowered.contains('.') {
        if let Some(entry) = conversions.get(&lowered) {
            return Ok(Some((lowered, entry.clone(), entry.namespace_oid)));
        }
        return Ok(None);
    }
    for schema_name in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema_name.as_str(), "" | "$user" | "pg_catalog") {
            continue;
        }
        let Some(namespace_oid) =
            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name)
        else {
            continue;
        };
        let key = format!("{}.{}", schema_name.to_ascii_lowercase(), lowered);
        if let Some(entry) = conversions.get(&key) {
            return Ok(Some((key, entry.clone(), namespace_oid)));
        }
        if let Some(entry) = conversions
            .values()
            .find(|entry| entry.namespace_oid == namespace_oid && entry.name == lowered)
        {
            return Ok(Some((
                format!("{}.{}", schema_name.to_ascii_lowercase(), lowered),
                entry.clone(),
                namespace_oid,
            )));
        }
    }
    Ok(None)
}

fn resolve_conversion_create_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<(String, String, u32, String), ExecError> {
    let lowered = raw_name.to_ascii_lowercase();
    if let Some((schema, object)) = lowered.split_once('.') {
        let namespace_oid = db
            .visible_namespace_oid_by_name(client_id, txn_ctx, schema)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        return Ok((
            format!("{schema}.{object}"),
            object.to_string(),
            namespace_oid,
            schema.to_string(),
        ));
    }
    for schema in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema.as_str(), "" | "$user" | "pg_catalog") {
            continue;
        }
        if let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema) {
            return Ok((
                format!("{}.{}", schema.to_ascii_lowercase(), lowered),
                lowered,
                namespace_oid,
                schema,
            ));
        }
    }
    Ok((
        format!("public.{lowered}"),
        lowered,
        crate::include::catalog::PUBLIC_NAMESPACE_OID,
        "public".into(),
    ))
}

fn ensure_conversion_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    entry: &ConversionEntry,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(entry.owner_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of conversion {}", entry.name),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_oid: u32,
) -> Result<String, ExecError> {
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| namespace_oid.to_string()))
}
