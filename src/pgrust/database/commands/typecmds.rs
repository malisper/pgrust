use super::super::*;
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::executor::{ColumnDesc, RelationDesc, StatementResult};
use crate::backend::parser::{
    AlterEnumValuePosition, AlterTypeAddEnumValueStatement, AlterTypeOwnerStatement,
    AlterTypeRenameEnumValueStatement, AlterTypeRenameTypeStatement, AlterTypeStatement,
    CatalogLookup, CreateCompositeTypeStatement, CreateEnumTypeStatement, CreateRangeTypeStatement,
    CreateTypeStatement, DropTypeStatement, ParseError, parse_type_name, resolve_raw_type_name,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, is_system_column_name, map_catalog_error, reject_type_with_dependents,
};
use crate::pgrust::database::{EnumLabelEntry, EnumTypeEntry, RangeTypeEntry};

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

    pub(crate) fn execute_alter_type_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTypeOwnerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let new_owner = find_role_by_name(auth_catalog.roles(), &stmt.new_owner)
            .ok_or_else(|| role_does_not_exist_error(&stmt.new_owner))?;
        let mut range_types = self.range_types.write();
        let range_key = range_types
            .iter()
            .find(|(_, entry)| {
                entry.name.eq_ignore_ascii_case(stmt.type_name.as_str())
                    && namespace_visible_in_search_path(entry.namespace_oid, &search_path)
            })
            .map(|(key, _)| key.clone());
        let Some(range_key) = range_key else {
            return Err(range_types
                .values()
                .find(|entry| {
                    entry
                        .multirange_name
                        .eq_ignore_ascii_case(stmt.type_name.as_str())
                        && namespace_visible_in_search_path(entry.namespace_oid, &search_path)
                })
                .map(|entry| {
                    cannot_alter_multirange_type_error(&entry.multirange_name, &entry.name)
                })
                .unwrap_or_else(|| type_does_not_exist_error(&stmt.type_name)));
        };
        let entry = range_types
            .get_mut(&range_key)
            .expect("range key found in snapshot");
        entry.owner_oid = new_owner.oid;
        entry.owner_usage = true;
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
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
                    if drop_stmt.cascade {
                        let dependent_ranges = self
                            .range_types
                            .read()
                            .iter()
                            .filter(|(_, entry)| entry.subtype.type_oid == type_oid)
                            .map(|(key, entry)| (key.clone(), entry.name.clone()))
                            .collect::<Vec<_>>();
                        if !dependent_ranges.is_empty() {
                            let mut range_types = self.range_types.write();
                            for (key, name) in dependent_ranges {
                                push_notice(format!("drop cascades to type {name}"));
                                range_types.remove(&key);
                            }
                        }
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
                Some(ResolvedDropTypeTarget::Range {
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
                    let removed = self.range_types.write().remove(&normalized_name);
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

    pub(crate) fn execute_alter_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let result = self.execute_alter_type_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
        );
        let result = self.finish_txn(client_id, xid, result, &[], &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_type_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match alter_stmt {
            AlterTypeStatement::AddEnumValue(stmt) => self.execute_alter_type_add_enum_value_stmt(
                client_id,
                stmt,
                xid,
                cid,
                configured_search_path,
            ),
            AlterTypeStatement::RenameEnumValue(stmt) => self
                .execute_alter_type_rename_enum_value_stmt(
                    client_id,
                    stmt,
                    xid,
                    cid,
                    configured_search_path,
                ),
            AlterTypeStatement::RenameType(stmt) => self.execute_alter_type_rename_type_stmt(
                client_id,
                stmt,
                xid,
                cid,
                configured_search_path,
            ),
        }
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
        let oid = self.next_dynamic_type_oid(Some(&enum_types), None)?;
        let array_oid = oid.saturating_add(1);
        let labels = stmt
            .labels
            .iter()
            .enumerate()
            .map(|(index, label)| EnumLabelEntry {
                oid: oid.saturating_add(2 + index as u32),
                label: label.clone(),
                sort_order: (index as f64) + 1.0,
                committed: true,
                creating_xid: None,
            })
            .collect();
        enum_types.insert(
            normalized,
            EnumTypeEntry {
                oid,
                array_oid,
                name: object_name,
                namespace_oid,
                labels,
                creating_xid: Some(xid),
                typacl: None,
                comment: None,
            },
        );
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_type_add_enum_value_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterTypeAddEnumValueStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        validate_enum_label_len(&stmt.label)?;
        let type_oid = self.resolve_enum_type_oid(
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.type_name,
            configured_search_path,
        )?;
        let mut enum_types = self.enum_types.write();
        let next_label_oid = enum_types
            .values()
            .flat_map(|entry| {
                std::iter::once(entry.array_oid)
                    .chain(std::iter::once(entry.oid))
                    .chain(entry.labels.iter().map(|label| label.oid))
            })
            .max()
            .unwrap_or(type_oid)
            .saturating_add(1);
        let entry = enum_types
            .values_mut()
            .find(|entry| entry.oid == type_oid)
            .ok_or_else(|| type_does_not_exist_error(&stmt.type_name))?;
        if entry.labels.iter().any(|label| label.label == stmt.label) {
            if stmt.if_not_exists {
                push_notice(format!(
                    "enum label \"{}\" already exists, skipping",
                    stmt.label
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("enum label \"{}\" already exists", stmt.label),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let sort_order = match &stmt.position {
            None => {
                entry
                    .labels
                    .iter()
                    .map(|label| label.sort_order)
                    .max_by(f64::total_cmp)
                    .unwrap_or(0.0)
                    + 1.0
            }
            Some(AlterEnumValuePosition::Before(neighbor)) => {
                let index = entry
                    .labels
                    .iter()
                    .position(|label| label.label == *neighbor)
                    .ok_or_else(|| enum_neighbor_missing_error(&entry.name, neighbor))?;
                let previous = index
                    .checked_sub(1)
                    .and_then(|prev| entry.labels.get(prev))
                    .map(|label| label.sort_order)
                    .unwrap_or(0.0);
                let current = entry.labels[index].sort_order;
                if index == 0 {
                    current - 1.0
                } else {
                    midpoint_or_renumber(&mut entry.labels, previous, current)
                }
            }
            Some(AlterEnumValuePosition::After(neighbor)) => {
                let index = entry
                    .labels
                    .iter()
                    .position(|label| label.label == *neighbor)
                    .ok_or_else(|| enum_neighbor_missing_error(&entry.name, neighbor))?;
                let current = entry.labels[index].sort_order;
                let next = entry
                    .labels
                    .get(index + 1)
                    .map(|label| label.sort_order)
                    .unwrap_or(current + 2.0);
                midpoint_or_renumber(&mut entry.labels, current, next)
            }
        };
        let immediately_usable = entry.creating_xid == Some(xid);
        entry.labels.push(EnumLabelEntry {
            oid: next_label_oid,
            label: stmt.label.clone(),
            sort_order,
            committed: immediately_usable,
            creating_xid: (!immediately_usable).then_some(xid),
        });
        entry
            .labels
            .sort_by(|left, right| left.sort_order.total_cmp(&right.sort_order));
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_type_rename_enum_value_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterTypeRenameEnumValueStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        validate_enum_label_len(&stmt.new_label)?;
        let type_oid = self.resolve_enum_type_oid(
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.type_name,
            configured_search_path,
        )?;
        let mut enum_types = self.enum_types.write();
        let entry = enum_types
            .values_mut()
            .find(|entry| entry.oid == type_oid)
            .ok_or_else(|| type_does_not_exist_error(&stmt.type_name))?;
        let label_index = entry
            .labels
            .iter()
            .position(|label| label.label == stmt.old_label)
            .ok_or_else(|| enum_neighbor_missing_error(&entry.name, &stmt.old_label))?;
        if entry
            .labels
            .iter()
            .any(|label| label.label == stmt.new_label)
        {
            return Err(ExecError::DetailedError {
                message: format!("enum label \"{}\" already exists", stmt.new_label),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        entry.labels[label_index].label = stmt.new_label.clone();
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_type_rename_type_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterTypeRenameTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.new_type_name.contains('.') {
            return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
                stmt.new_type_name.clone(),
            )));
        }
        let type_oid = self.resolve_enum_type_oid(
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.type_name,
            configured_search_path,
        )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let visible_type_rows = catalog.type_rows();
        let mut enum_types = self.enum_types.write();
        let old_key = enum_types
            .iter()
            .find_map(|(key, entry)| (entry.oid == type_oid).then_some(key.clone()))
            .ok_or_else(|| type_does_not_exist_error(&stmt.type_name))?;
        let mut entry = enum_types
            .remove(&old_key)
            .ok_or_else(|| type_does_not_exist_error(&stmt.type_name))?;
        let new_name = stmt.new_type_name.to_ascii_lowercase();
        let new_key = match stmt.schema_name.as_deref() {
            Some(schema_name) => format!("{}.{}", schema_name.to_ascii_lowercase(), new_name),
            None => format!("public.{new_name}"),
        };
        if visible_type_rows.into_iter().any(|row| {
            row.oid != entry.oid
                && row.typelem == 0
                && row.typnamespace == entry.namespace_oid
                && row.typname.eq_ignore_ascii_case(&new_name)
        }) || enum_types.values().any(|existing| {
            existing.namespace_oid == entry.namespace_oid
                && existing.name.eq_ignore_ascii_case(&new_name)
        }) {
            enum_types.insert(old_key, entry);
            return Err(type_already_exists_error(&new_name));
        }
        entry.name = new_name;
        enum_types.insert(new_key, entry);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn resolve_enum_type_oid(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        schema_name: Option<&str>,
        type_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<u32, ExecError> {
        let lookup_name = schema_name
            .map(|schema| format!("{schema}.{type_name}"))
            .unwrap_or_else(|| type_name.to_string());
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let row = catalog
            .type_by_name(&lookup_name)
            .ok_or_else(|| type_does_not_exist_error(&lookup_name))?;
        if !matches!(row.sql_type.kind, SqlTypeKind::Enum) {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: lookup_name,
                expected: "enum type",
            }));
        }
        Ok(row.oid)
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
        let subtype = resolve_raw_type_name(&stmt.subtype, &catalog).map_err(ExecError::Parse)?;
        let subtype_oid = catalog
            .type_oid_for_sql_type(subtype)
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(stmt.type_name.clone())))?;
        let subtype = subtype.with_identity(subtype_oid, subtype.typrelid);
        if type_name_exists_in_rows(&base_type_rows, namespace_oid, &object_name)
            || type_name_exists_in_rows(&enum_type_rows, namespace_oid, &object_name)
            || range_type_name_exists_in_snapshot(&range_type_snapshot, namespace_oid, &object_name)
        {
            return Err(type_already_exists_error(&range_type_display_name(stmt)));
        }
        let manual_multirange_name = stmt.multirange_type_name.is_some();
        let multirange_name = stmt
            .multirange_type_name
            .clone()
            .unwrap_or_else(|| default_multirange_type_name(&object_name));
        let multirange_conflict = type_conflict_name_in_rows(
            &base_type_rows,
            namespace_oid,
            &multirange_name,
            Some(&catalog),
        )
        .or_else(|| {
            type_conflict_name_in_rows(&enum_type_rows, namespace_oid, &multirange_name, None)
        })
        .or_else(|| {
            range_type_or_multirange_conflict_name_in_snapshot(
                &range_type_snapshot,
                namespace_oid,
                &multirange_name,
            )
        });
        if let Some(conflict_name) = multirange_conflict {
            return if manual_multirange_name {
                Err(type_already_exists_error(&conflict_name))
            } else {
                Err(multirange_type_already_exists_error(
                    &range_type_display_name(stmt),
                    &conflict_name,
                ))
            };
        }
        drop(catalog);
        let oid = self.next_dynamic_type_oid(None, Some(&range_type_snapshot))?;

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
            return if manual_multirange_name {
                Err(type_already_exists_error(&multirange_name))
            } else {
                Err(multirange_type_already_exists_error(
                    &range_type_display_name(stmt),
                    &multirange_name,
                ))
            };
        }
        let array_oid = oid.saturating_add(1);
        let multirange_oid = oid.saturating_add(2);
        let multirange_array_oid = oid.saturating_add(3);
        range_types.insert(
            normalized,
            RangeTypeEntry {
                oid,
                array_oid,
                multirange_oid,
                multirange_array_oid,
                name: object_name,
                multirange_name,
                namespace_oid,
                owner_oid: self.auth_state(client_id).current_user_oid(),
                public_usage: true,
                owner_usage: true,
                subtype,
                subtype_diff: stmt.subtype_diff.clone(),
                collation: stmt.collation.clone(),
                typacl: None,
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

    fn next_dynamic_type_oid(
        &self,
        existing_enum_types: Option<&std::collections::BTreeMap<String, EnumTypeEntry>>,
        existing_range_types: Option<&std::collections::BTreeMap<String, RangeTypeEntry>>,
    ) -> Result<u32, ExecError> {
        let next_catalog_oid = self.catalog.read().next_oid();
        let next_dynamic_oid = self
            .domains
            .read()
            .values()
            .map(|domain| domain.oid.saturating_add(1))
            .chain(
                existing_enum_types
                    .into_iter()
                    .flat_map(|enum_types| enum_types.values())
                    .map(|entry| {
                        entry
                            .labels
                            .iter()
                            .map(|label| label.oid)
                            .max()
                            .unwrap_or(entry.array_oid)
                            .max(entry.array_oid)
                            .saturating_add(1)
                    }),
            )
            .chain(
                existing_enum_types
                    .is_none()
                    .then(|| self.enum_types.read())
                    .into_iter()
                    .flat_map(|enum_types| {
                        enum_types
                            .values()
                            .map(|entry| {
                                entry
                                    .labels
                                    .iter()
                                    .map(|label| label.oid)
                                    .max()
                                    .unwrap_or(entry.array_oid)
                                    .max(entry.array_oid)
                                    .saturating_add(1)
                            })
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
            .unwrap_or(next_catalog_oid)
            .max(next_catalog_oid);
        Ok(next_dynamic_oid)
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

fn validate_enum_label_len(label: &str) -> Result<(), ExecError> {
    if label.len() > 63 {
        return Err(ExecError::DetailedError {
            message: format!("invalid enum label \"{label}\""),
            detail: Some("Labels must be 63 bytes or less.".into()),
            hint: None,
            sqlstate: "42622",
        });
    }
    Ok(())
}

fn enum_neighbor_missing_error(type_name: &str, label: &str) -> ExecError {
    let _ = type_name;
    ExecError::DetailedError {
        message: format!("\"{label}\" is not an existing enum label"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn midpoint_or_renumber(labels: &mut [EnumLabelEntry], low: f64, high: f64) -> f64 {
    let midpoint = ((low as f32 + high as f32) / 2.0) as f64;
    if midpoint > low && midpoint < high {
        return midpoint;
    }
    labels.sort_by(|left, right| left.sort_order.total_cmp(&right.sort_order));
    let high_index = labels
        .iter()
        .position(|label| label.sort_order >= high)
        .unwrap_or(labels.len());
    for (idx, label) in labels.iter_mut().enumerate() {
        label.sort_order = (idx as f64) + 1.0;
    }
    let renumbered_low = high_index
        .checked_sub(1)
        .and_then(|idx| labels.get(idx))
        .map(|label| label.sort_order)
        .unwrap_or(0.0);
    let renumbered_high = labels
        .get(high_index)
        .map(|label| label.sort_order)
        .unwrap_or(renumbered_low + 2.0);
    ((renumbered_low as f32 + renumbered_high as f32) / 2.0) as f64
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

fn type_conflict_name_in_rows(
    rows: &[crate::include::catalog::PgTypeRow],
    namespace_oid: u32,
    name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    rows.iter()
        .find(|row| {
            row.typelem == 0
                && (row.typnamespace == namespace_oid || catalog.is_some())
                && row.typname.eq_ignore_ascii_case(name)
        })
        .map(|row| row.typname.clone())
        .or_else(|| {
            let catalog = catalog?;
            let raw_type = parse_type_name(name).ok()?;
            let sql_type = resolve_raw_type_name(&raw_type, catalog).ok()?;
            let type_oid = catalog.type_oid_for_sql_type(sql_type)?;
            rows.iter()
                .find(|row| row.typelem == 0 && row.oid == type_oid)
                .map(|row| row.typname.clone())
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

fn range_type_or_multirange_conflict_name_in_snapshot(
    range_types: &std::collections::BTreeMap<String, RangeTypeEntry>,
    namespace_oid: u32,
    name: &str,
) -> Option<String> {
    range_types.values().find_map(|entry| {
        if entry.namespace_oid != namespace_oid {
            return None;
        }
        if entry.name.eq_ignore_ascii_case(name) {
            Some(entry.name.clone())
        } else if entry.multirange_name.eq_ignore_ascii_case(name) {
            Some(entry.multirange_name.clone())
        } else {
            None
        }
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
            "Failed while creating a multirange type for type \"{range_name}\"."
        )),
        hint: Some(
            "You can manually specify a multirange type name using the \"multirange_type_name\" attribute."
                .into(),
        ),
        sqlstate: "42710",
    }
}

fn namespace_visible_in_search_path(namespace_oid: u32, search_path: &[String]) -> bool {
    search_path.iter().any(|schema| {
        (schema == "public" && namespace_oid == crate::include::catalog::PUBLIC_NAMESPACE_OID)
            || (schema == "pg_catalog"
                && namespace_oid == crate::include::catalog::PG_CATALOG_NAMESPACE_OID)
    })
}

fn cannot_alter_multirange_type_error(multirange_name: &str, range_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot alter multirange type {multirange_name}"),
        detail: None,
        hint: Some(format!(
            "You can alter type {range_name}, which will alter the multirange type as well."
        )),
        sqlstate: "42809",
    }
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{role_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
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
