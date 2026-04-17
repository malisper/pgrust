use super::super::*;
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{ColumnDesc, RelationDesc, StatementResult};
use crate::backend::parser::{
    CatalogLookup, CreateCompositeTypeStatement, CreateEnumTypeStatement, CreateTypeStatement,
    DropTypeStatement, ParseError, resolve_raw_type_name,
};
use crate::pgrust::database::EnumTypeEntry;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, is_system_column_name, map_catalog_error, reject_type_with_dependents,
};

enum ResolvedDropTypeTarget {
    Composite {
        relation_oid: u32,
        type_oid: u32,
        display_name: String,
    },
    Enum {
        type_oid: u32,
        normalized_name: String,
        display_name: String,
    },
    Other,
}

impl Database {
    pub(crate) fn execute_create_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_type_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_type_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        match create_stmt {
            CreateTypeStatement::Composite(stmt) => {
                let interrupts = self.interrupt_state(client_id);
                let (type_name, namespace_oid) = self.normalize_create_type_name_with_search_path(
                    client_id,
                    Some((xid, cid)),
                    stmt.schema_name.as_deref(),
                    &stmt.type_name,
                    configured_search_path,
                )?;
                let catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
                let desc = lower_create_composite_type_desc(stmt, &catalog)?;
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts,
                };
                match self.catalog.write().create_composite_type_mvcc(
                    type_name,
                    desc,
                    namespace_oid,
                    self.auth_state(client_id).current_user_oid(),
                    &ctx,
                ) {
                    Ok((_entry, effect)) => {
                        catalog_effects.push(effect);
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(CatalogError::TableAlreadyExists(_)) => {
                        Err(type_already_exists_error(&composite_type_display_name(stmt)))
                    }
                    Err(err) => Err(map_catalog_error(err)),
                }
            }
            CreateTypeStatement::Enum(stmt) => self.execute_create_enum_type_stmt(
                client_id,
                stmt,
                xid,
                cid,
                configured_search_path,
            ),
        }
    }

    pub(crate) fn execute_drop_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_type_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_type_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if drop_stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "DROP TYPE CASCADE is not supported yet".into(),
            )));
        }

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let interrupts = self.interrupt_state(client_id);
        let mut dropped = 0usize;

        for type_name in &drop_stmt.type_names {
            match self.resolve_drop_type_target(
                client_id,
                Some((xid, cid)),
                configured_search_path,
                type_name,
            )? {
                Some(ResolvedDropTypeTarget::Composite {
                    relation_oid,
                    type_oid,
                    display_name,
                }) => {
                    let relation =
                        catalog
                            .lookup_relation_by_oid(relation_oid)
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::UnexpectedToken {
                                    expected: "composite type relation",
                                    actual: display_name.clone(),
                                })
                            })?;
                    ensure_relation_owner(self, client_id, &relation, &display_name)?;
                    reject_type_with_dependents(
                        self,
                        client_id,
                        Some((xid, cid)),
                        type_oid,
                        &display_name,
                    )?;
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid,
                        client_id,
                        waiter: Some(self.txn_waiter.clone()),
                        interrupts: Arc::clone(&interrupts),
                    };
                    match self
                        .catalog
                        .write()
                        .drop_composite_type_by_oid_mvcc(relation_oid, &ctx)
                    {
                        Ok((_entry, effect)) => {
                            catalog_effects.push(effect);
                            dropped += 1;
                        }
                        Err(CatalogError::UnknownTable(_)) if drop_stmt.if_exists => {}
                        Err(CatalogError::UnknownTable(_)) => {
                            return Err(type_does_not_exist_error(type_name));
                        }
                        Err(err) => return Err(map_catalog_error(err)),
                    }
                }
                Some(ResolvedDropTypeTarget::Other) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: type_name.clone(),
                        expected: "type",
                    }));
                }
                Some(ResolvedDropTypeTarget::Enum {
                    type_oid,
                    normalized_name,
                    display_name,
                }) => {
                    reject_type_with_dependents(
                        self,
                        client_id,
                        Some((xid, cid)),
                        type_oid,
                        &display_name,
                    )?;
                    let removed = self.enum_types.write().remove(&normalized_name);
                    if removed.is_some() {
                        self.plan_cache.invalidate_all();
                        dropped += 1;
                    } else if !drop_stmt.if_exists {
                        return Err(type_does_not_exist_error(type_name));
                    }
                }
                None if drop_stmt.if_exists => {}
                None => return Err(type_does_not_exist_error(type_name)),
            }
        }

        Ok(StatementResult::AffectedRows(dropped))
    }

    fn resolve_drop_type_target(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        type_name: &str,
    ) -> Result<Option<ResolvedDropTypeTarget>, ExecError> {
        let catcache = self.backend_catcache(client_id, txn_ctx).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "catalog lookup",
                actual: format!("{err:?}"),
            })
        })?;
        let types = catcache.type_rows();
        let resolve_namespace_oid = |schema_name: &str| {
            catcache
                .namespace_by_name(schema_name)
                .filter(|row| !self.other_session_temp_namespace_oid(client_id, row.oid))
                .map(|row| row.oid)
        };
        let format_name = |namespace_oid: u32, object_name: &str| {
            let schema_name = catcache
                .namespace_by_oid(namespace_oid)
                .map(|row| row.nspname.clone())
                .unwrap_or_else(|| "public".to_string());
            match schema_name.as_str() {
                "public" | "pg_catalog" => object_name.to_string(),
                _ => format!("{schema_name}.{object_name}"),
            }
        };
        let search_path = self.effective_search_path(client_id, configured_search_path);

        let matched = if let Some((schema_name, object_name)) = type_name.split_once('.') {
            let Some(namespace_oid) = resolve_namespace_oid(schema_name) else {
                return Ok(None);
            };
            types.into_iter().find(|row| {
                row.typnamespace == namespace_oid && row.typname.eq_ignore_ascii_case(object_name)
            })
        } else {
            let mut matched = None;
            for schema_name in &search_path {
                if schema_name.is_empty() || schema_name == "$user" {
                    continue;
                }
                let Some(namespace_oid) = resolve_namespace_oid(schema_name) else {
                    continue;
                };
                if let Some(row) = types.iter().find(|row| {
                    row.typnamespace == namespace_oid && row.typname.eq_ignore_ascii_case(type_name)
                }) {
                    matched = Some(row.clone());
                    break;
                }
            }
            matched
        };

        let Some(type_row) = matched else {
            let enum_row = if let Some((schema_name, object_name)) = type_name.split_once('.') {
                let Some(namespace_oid) = resolve_namespace_oid(schema_name) else {
                    return Ok(None);
                };
                self.enum_type_rows_for_search_path(&search_path)
                    .into_iter()
                    .find(|row| {
                        row.typnamespace == namespace_oid
                            && row.typelem == 0
                            && row.typname.eq_ignore_ascii_case(object_name)
                    })
            } else {
                self.enum_type_rows_for_search_path(&search_path)
                    .into_iter()
                    .find(|row| row.typelem == 0 && row.typname.eq_ignore_ascii_case(type_name))
            };
            if let Some(row) = enum_row {
                let normalized_name = format_name(row.typnamespace, &row.typname);
                return Ok(Some(ResolvedDropTypeTarget::Enum {
                    type_oid: row.oid,
                    normalized_name,
                    display_name: format_name(row.typnamespace, &row.typname),
                }));
            }
            return Ok(None);
        };
        let Some(class_row) = catcache.class_by_oid(type_row.typrelid) else {
            return Ok(Some(ResolvedDropTypeTarget::Other));
        };
        if class_row.relkind != 'c' {
            return Ok(Some(ResolvedDropTypeTarget::Other));
        }

        Ok(Some(ResolvedDropTypeTarget::Composite {
            relation_oid: class_row.oid,
            type_oid: type_row.oid,
            display_name: format_name(type_row.typnamespace, &type_row.typname),
        }))
    }
}

fn lower_create_composite_type_desc(
    stmt: &CreateCompositeTypeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<RelationDesc, ExecError> {
    let mut columns = Vec::with_capacity(stmt.attributes.len());
    for attribute in &stmt.attributes {
        if is_system_column_name(&attribute.name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "non-system attribute name",
                actual: attribute.name.clone(),
            }));
        }
        if columns
            .iter()
            .any(|column: &ColumnDesc| column.name.eq_ignore_ascii_case(&attribute.name))
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "new attribute name",
                actual: format!("attribute already exists: {}", attribute.name),
            }));
        }

        let sql_type = resolve_raw_type_name(&attribute.ty, catalog).map_err(ExecError::Parse)?;
        columns.push(column_desc(attribute.name.clone(), sql_type, true));
    }
    Ok(RelationDesc { columns })
}

impl Database {
    fn execute_create_enum_type_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateEnumTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.labels.is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "one or more enum labels",
                actual: "()".into(),
            }));
        }
        let (normalized, object_name, namespace_oid) = self.normalize_enum_type_name_for_create(
            client_id,
            Some((xid, cid)),
            stmt,
            configured_search_path,
        )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        if catalog.type_rows().into_iter().any(|row| {
            row.typelem == 0
                && row.typnamespace == namespace_oid
                && row.typname.eq_ignore_ascii_case(&object_name)
        }) {
            return Err(type_already_exists_error(&enum_type_display_name(stmt)));
        }
        let mut enum_types = self.enum_types.write();
        if enum_types.contains_key(&normalized) {
            return Err(type_already_exists_error(&enum_type_display_name(stmt)));
        }
        if enum_types.values().any(|entry| {
            entry.namespace_oid == namespace_oid && entry.name.eq_ignore_ascii_case(&object_name)
        }) {
            return Err(type_already_exists_error(&enum_type_display_name(stmt)));
        }

        let next_catalog_oid = {
            let catalog = self.catalog.write();
            let snapshot = catalog.catalog_snapshot().map_err(map_catalog_error)?;
            snapshot.next_oid()
        };
        let next_dynamic_oid = self
            .domains
            .read()
            .values()
            .map(|domain| domain.oid.saturating_add(1))
            .chain(
                enum_types
                    .values()
                    .map(|entry| entry.array_oid.saturating_add(1)),
            )
            .max()
            .unwrap_or(next_catalog_oid)
            .max(next_catalog_oid);
        let oid = next_dynamic_oid;
        let array_oid = oid.saturating_add(1);
        enum_types.insert(
            normalized,
            EnumTypeEntry {
                oid,
                array_oid,
                name: object_name,
                namespace_oid,
                labels: stmt.labels.clone(),
                comment: None,
            },
        );
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn normalize_enum_type_name_for_create(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateEnumTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, String, u32), ExecError> {
        let normalized = self
            .normalize_create_type_name_with_search_path(
                client_id,
                txn_ctx,
                stmt.schema_name.as_deref(),
                &stmt.type_name,
                configured_search_path,
            )
            .map_err(ExecError::Parse)?;
        let object_name = normalized
            .0
            .rsplit('.')
            .next()
            .unwrap_or(&normalized.0)
            .to_string();
        Ok((normalized.0, object_name, normalized.1))
    }
}

fn composite_type_display_name(stmt: &CreateCompositeTypeStatement) -> String {
    match &stmt.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", stmt.type_name),
        None => stmt.type_name.clone(),
    }
}

fn enum_type_display_name(stmt: &CreateEnumTypeStatement) -> String {
    match &stmt.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", stmt.type_name),
        None => stmt.type_name.clone(),
    }
}

fn type_already_exists_error(type_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("type \"{type_name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn type_does_not_exist_error(type_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("type \"{type_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}
