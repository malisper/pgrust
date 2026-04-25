use super::super::*;
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{ColumnDesc, RelationDesc, StatementResult};
use crate::backend::parser::{
    CatalogLookup, CreateCompositeTypeStatement, CreateEnumTypeStatement, CreateRangeTypeStatement,
    CreateTypeStatement, DropTypeStatement, ParseError, resolve_raw_type_name,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::FLOAT8_TYPE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, format_sql_type_name, is_system_column_name, map_catalog_error,
    reject_type_with_dependents,
};
use crate::pgrust::database::{EnumTypeEntry, RangeTypeEntry, save_range_type_entries};

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
    Range {
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
                    Err(CatalogError::TableAlreadyExists(_)) => Err(type_already_exists_error(
                        &composite_type_display_name(stmt),
                    )),
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
            CreateTypeStatement::Range(stmt) => self.execute_create_range_type_stmt(
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
                    let dependent_ranges = self.dependent_range_types_for_type_oid(type_oid);
                    if !dependent_ranges.is_empty() && !drop_stmt.cascade {
                        return Err(type_has_range_dependents_error(
                            &display_name,
                            &dependent_ranges[0].1,
                        ));
                    }
                    if drop_stmt.cascade {
                        self.drop_dependent_range_types(
                            client_id,
                            configured_search_path,
                            &dependent_ranges,
                        )?;
                    } else {
                        reject_type_with_dependents(
                            self,
                            client_id,
                            Some((xid, cid)),
                            type_oid,
                            &display_name,
                        )?;
                    }
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
                    let dependent_ranges = self.dependent_range_types_for_type_oid(type_oid);
                    if !dependent_ranges.is_empty() && !drop_stmt.cascade {
                        return Err(type_has_range_dependents_error(
                            &display_name,
                            &dependent_ranges[0].1,
                        ));
                    }
                    if drop_stmt.cascade {
                        self.drop_dependent_range_types(
                            client_id,
                            configured_search_path,
                            &dependent_ranges,
                        )?;
                    } else {
                        reject_type_with_dependents(
                            self,
                            client_id,
                            Some((xid, cid)),
                            type_oid,
                            &display_name,
                        )?;
                    }
                    let removed = self.enum_types.write().remove(&normalized_name);
                    if removed.is_some() {
                        self.refresh_catalog_store_dynamic_type_rows(
                            client_id,
                            configured_search_path,
                        );
                        self.invalidate_backend_cache_state(client_id);
                        self.plan_cache.invalidate_all();
                        dropped += 1;
                    } else if !drop_stmt.if_exists {
                        return Err(type_does_not_exist_error(type_name));
                    }
                }
                Some(ResolvedDropTypeTarget::Range {
                    type_oid,
                    normalized_name,
                    display_name,
                }) => {
                    let dependent_ranges = self.dependent_range_types_for_type_oid(type_oid);
                    if !dependent_ranges.is_empty() && !drop_stmt.cascade {
                        return Err(type_has_range_dependents_error(
                            &display_name,
                            &dependent_ranges[0].1,
                        ));
                    }
                    if drop_stmt.cascade {
                        self.drop_dependent_range_types(
                            client_id,
                            configured_search_path,
                            &dependent_ranges,
                        )?;
                    } else {
                        reject_type_with_dependents(
                            self,
                            client_id,
                            Some((xid, cid)),
                            type_oid,
                            &display_name,
                        )?;
                    }
                    let removed = self.range_types.read().get(&normalized_name).cloned();
                    if removed.is_some() {
                        {
                            let mut range_types = self.range_types.write();
                            range_types.remove(&normalized_name);
                            save_range_type_entries(
                                &self.cluster.base_dir,
                                self.database_oid,
                                &range_types,
                            )?;
                        }
                        self.refresh_catalog_store_dynamic_type_rows(
                            client_id,
                            configured_search_path,
                        );
                        self.invalidate_backend_cache_state(client_id);
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

    fn dependent_range_types_for_type_oid(&self, type_oid: u32) -> Vec<(String, String)> {
        self.range_types
            .read()
            .iter()
            .filter(|(_, entry)| {
                entry.oid != type_oid
                    && (entry.subtype_dependency_oid == Some(type_oid)
                        || entry.subtype.type_oid == type_oid)
            })
            .map(|(key, entry)| (key.clone(), entry.name.clone()))
            .collect()
    }

    fn drop_dependent_range_types(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
        dependent_ranges: &[(String, String)],
    ) -> Result<(), ExecError> {
        if dependent_ranges.is_empty() {
            return Ok(());
        }
        {
            let mut range_types = self.range_types.write();
            for (key, name) in dependent_ranges {
                if range_types.remove(key).is_some() {
                    push_notice(format!("drop cascades to type {name}"));
                }
            }
            save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
        }
        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(())
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
            let range_row = if let Some((schema_name, object_name)) = type_name.split_once('.') {
                let Some(namespace_oid) = resolve_namespace_oid(schema_name) else {
                    return Ok(None);
                };
                self.range_type_rows_for_search_path(&search_path)
                    .into_iter()
                    .find(|row| {
                        row.typnamespace == namespace_oid
                            && row.typelem == 0
                            && row.typname.eq_ignore_ascii_case(object_name)
                    })
            } else {
                self.range_type_rows_for_search_path(&search_path)
                    .into_iter()
                    .find(|row| row.typelem == 0 && row.typname.eq_ignore_ascii_case(type_name))
            };
            if let Some(row) = range_row {
                let normalized_name = format_name(row.typnamespace, &row.typname);
                return Ok(Some(ResolvedDropTypeTarget::Range {
                    type_oid: row.oid,
                    normalized_name,
                    display_name: format_name(row.typnamespace, &row.typname),
                }));
            }
            return Ok(None);
        };
        if type_row.typelem == 0 {
            let normalized_name = format_name(type_row.typnamespace, &type_row.typname);
            if self
                .enum_types
                .read()
                .values()
                .any(|entry| entry.oid == type_row.oid)
            {
                return Ok(Some(ResolvedDropTypeTarget::Enum {
                    type_oid: type_row.oid,
                    normalized_name: normalized_name.clone(),
                    display_name: normalized_name,
                }));
            }
            if self
                .range_types
                .read()
                .values()
                .any(|entry| entry.oid == type_row.oid)
            {
                return Ok(Some(ResolvedDropTypeTarget::Range {
                    type_oid: type_row.oid,
                    normalized_name: normalized_name.clone(),
                    display_name: normalized_name,
                }));
            }
        }
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

fn type_has_range_dependents_error(type_name: &str, dependent_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot drop type {type_name} because other objects depend on it"),
        detail: Some(format!("type {dependent_name} depends on type {type_name}")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
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
        let oid = self.allocate_dynamic_type_oids(2, Some(&enum_types), None)?;
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
        drop(enum_types);
        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_create_range_type_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateRangeTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, object_name, namespace_oid) = self.normalize_range_type_name_for_create(
            client_id,
            Some((xid, cid)),
            stmt,
            configured_search_path,
        )?;
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let base_type_rows = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "catalog lookup",
                    actual: format!("{err:?}"),
                })
            })?
            .type_rows();
        let enum_type_rows = self.enum_type_rows_for_search_path(&search_path);
        let range_type_snapshot = self.range_types.read().clone();
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let resolved_subtype =
            resolve_raw_type_name(&stmt.subtype, &catalog).map_err(ExecError::Parse)?;
        let subtype_oid = catalog
            .type_oid_for_sql_type(resolved_subtype)
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(stmt.type_name.clone())))?;
        let domain_subtype = self
            .domains
            .read()
            .values()
            .find(|domain| domain.oid == subtype_oid)
            .cloned();
        let (subtype, subtype_dependency_oid) = if let Some(domain) = domain_subtype {
            let base_oid = catalog
                .type_oid_for_sql_type(domain.sql_type)
                .unwrap_or(domain.sql_type.type_oid);
            (
                domain
                    .sql_type
                    .with_identity(base_oid, domain.sql_type.typrelid),
                Some(domain.oid),
            )
        } else {
            (
                resolved_subtype.with_identity(subtype_oid, resolved_subtype.typrelid),
                None,
            )
        };
        if let Some(subtype_diff) = stmt.subtype_diff.as_deref() {
            validate_range_subtype_diff_function(
                self,
                client_id,
                Some((xid, cid)),
                &catalog,
                subtype_diff,
                subtype,
                subtype_oid,
            )?;
        }
        drop(catalog);
        if type_name_exists_in_rows(&base_type_rows, namespace_oid, &object_name)
            || type_name_exists_in_rows(&enum_type_rows, namespace_oid, &object_name)
            || range_type_name_exists_in_snapshot(&range_type_snapshot, namespace_oid, &object_name)
        {
            return Err(type_already_exists_error(&range_type_display_name(stmt)));
        }
        let multirange_name = stmt
            .multirange_type_name
            .clone()
            .unwrap_or_else(|| default_multirange_type_name(&object_name));
        if type_name_exists_in_rows(&base_type_rows, namespace_oid, &multirange_name)
            || type_name_exists_in_rows(&enum_type_rows, namespace_oid, &multirange_name)
            || range_type_or_multirange_name_exists_in_snapshot(
                &range_type_snapshot,
                namespace_oid,
                &multirange_name,
            )
        {
            return Err(multirange_type_already_exists_error(
                &range_type_display_name(stmt),
                &multirange_name,
            ));
        }
        let oid = self.allocate_dynamic_type_oids(4, None, Some(&range_type_snapshot))?;

        let mut range_types = self.range_types.write();
        if range_types.contains_key(&normalized) {
            return Err(type_already_exists_error(&range_type_display_name(stmt)));
        }
        if range_types.values().any(|entry| {
            entry.namespace_oid == namespace_oid
                && (entry.name.eq_ignore_ascii_case(&object_name)
                    || entry.name.eq_ignore_ascii_case(&multirange_name)
                    || entry.multirange_name.eq_ignore_ascii_case(&multirange_name))
        }) {
            return Err(multirange_type_already_exists_error(
                &range_type_display_name(stmt),
                &multirange_name,
            ));
        }
        let array_oid = oid.saturating_add(1);
        let multirange_oid = oid.saturating_add(2);
        let multirange_array_oid = oid.saturating_add(3);
        let entry = RangeTypeEntry {
            oid,
            array_oid,
            multirange_oid,
            multirange_array_oid,
            name: object_name,
            multirange_name,
            namespace_oid,
            subtype,
            subtype_dependency_oid,
            subtype_opclass: stmt.subtype_opclass.clone(),
            subtype_diff: stmt.subtype_diff.clone(),
            collation: stmt.collation.clone(),
            comment: None,
        };
        range_types.insert(normalized, entry);
        save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
        drop(range_types);
        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
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

    fn normalize_range_type_name_for_create(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateRangeTypeStatement,
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

    pub(crate) fn allocate_dynamic_type_oids(
        &self,
        count: u32,
        existing_enum_types: Option<&std::collections::BTreeMap<String, EnumTypeEntry>>,
        existing_range_types: Option<&std::collections::BTreeMap<String, RangeTypeEntry>>,
    ) -> Result<u32, ExecError> {
        let dynamic_floor = self
            .domains
            .read()
            .values()
            .map(|domain| domain.oid.saturating_add(1))
            .chain(
                existing_enum_types
                    .into_iter()
                    .flat_map(|enum_types| enum_types.values())
                    .map(|entry| entry.array_oid.saturating_add(1)),
            )
            .chain(
                existing_enum_types
                    .is_none()
                    .then(|| self.enum_types.read())
                    .into_iter()
                    .flat_map(|enum_types| {
                        enum_types
                            .values()
                            .map(|entry| entry.array_oid.saturating_add(1))
                            .collect::<Vec<_>>()
                    }),
            )
            .chain(
                existing_range_types
                    .into_iter()
                    .flat_map(|range_types| range_types.values())
                    .map(|entry| entry.multirange_array_oid.saturating_add(1)),
            )
            .chain(
                existing_range_types
                    .is_none()
                    .then(|| self.range_types.read())
                    .into_iter()
                    .flat_map(|range_types| {
                        range_types
                            .values()
                            .map(|entry| entry.multirange_array_oid.saturating_add(1))
                            .collect::<Vec<_>>()
                    }),
            )
            .max()
            .unwrap_or(crate::backend::catalog::store::DEFAULT_FIRST_USER_OID);
        self.catalog
            .write()
            .allocate_oid_block(count, dynamic_floor)
            .map_err(map_catalog_error)
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

fn range_type_display_name(stmt: &CreateRangeTypeStatement) -> String {
    match &stmt.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", stmt.type_name),
        None => stmt.type_name.clone(),
    }
}

fn default_multirange_type_name(range_type_name: &str) -> String {
    let lower = range_type_name.to_ascii_lowercase();
    if let Some(start) = lower.find("range") {
        let end = start + "range".len();
        format!(
            "{}multirange{}",
            &range_type_name[..start],
            &range_type_name[end..]
        )
    } else {
        format!("{range_type_name}_multirange")
    }
}

fn split_range_function_name(name: &str) -> (Option<&str>, &str) {
    name.rsplit_once('.')
        .map(|(schema_name, function_name)| (Some(schema_name), function_name))
        .unwrap_or((None, name))
}

fn validate_range_subtype_diff_function(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    function_name: &str,
    subtype: crate::backend::parser::SqlType,
    subtype_oid: u32,
) -> Result<(), ExecError> {
    let (schema_name, base_name) = split_range_function_name(function_name);
    let namespace_oid = match schema_name {
        Some(schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let expected_argtypes = format!("{subtype_oid} {subtype_oid}");
    let found = catalog.proc_rows_by_name(base_name).into_iter().any(|row| {
        row.prokind == 'f'
            && row.prorettype == FLOAT8_TYPE_OID
            && row.proargtypes == expected_argtypes
            && namespace_oid
                .map(|namespace_oid| row.pronamespace == namespace_oid)
                .unwrap_or(true)
    });
    if found {
        return Ok(());
    }
    let type_name = match subtype_oid {
        FLOAT8_TYPE_OID => "double precision".to_string(),
        _ => format_sql_type_name(subtype),
    };
    Err(ExecError::DetailedError {
        message: format!("function {function_name}({type_name}, {type_name}) does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42883",
    })
}

fn type_name_exists_in_rows(
    rows: &[crate::include::catalog::PgTypeRow],
    namespace_oid: u32,
    name: &str,
) -> bool {
    rows.iter().any(|row| {
        row.typelem == 0
            && row.typnamespace == namespace_oid
            && row.typname.eq_ignore_ascii_case(name)
    })
}

fn range_type_name_exists_in_snapshot(
    range_types: &std::collections::BTreeMap<String, RangeTypeEntry>,
    namespace_oid: u32,
    name: &str,
) -> bool {
    range_types
        .values()
        .any(|entry| entry.namespace_oid == namespace_oid && entry.name.eq_ignore_ascii_case(name))
}

fn range_type_or_multirange_name_exists_in_snapshot(
    range_types: &std::collections::BTreeMap<String, RangeTypeEntry>,
    namespace_oid: u32,
    name: &str,
) -> bool {
    range_types.values().any(|entry| {
        entry.namespace_oid == namespace_oid
            && (entry.name.eq_ignore_ascii_case(name)
                || entry.multirange_name.eq_ignore_ascii_case(name))
    })
}

fn type_already_exists_error(type_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("type \"{type_name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn multirange_type_already_exists_error(range_name: &str, multirange_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("type \"{multirange_name}\" already exists"),
        detail: Some(format!(
            "Automatic creation of multirange type for range type \"{range_name}\" failed."
        )),
        hint: Some(
            "Choose a different type name, or supply a multirange type name with multirange_type_name."
                .into(),
        ),
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
