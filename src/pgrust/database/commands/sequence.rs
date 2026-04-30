use super::super::*;
use crate::backend::parser::{BoundRelation, CatalogLookup, SequenceOwnedByClause};
use crate::pgrust::database::ddl::{ensure_relation_owner, relation_kind_name};
use crate::pgrust::database::sequences::{
    apply_sequence_option_patch, initial_sequence_state, pg_sequence_row,
    resolve_sequence_options_spec, sequence_type_oid_for_raw_type,
};
use std::collections::BTreeMap;

fn lookup_sequence_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if entry.relkind == 'S' => Ok(entry),
        Some(entry) => Err(ExecError::DetailedError {
            message: format!("cannot open relation \"{}\"", name.to_ascii_lowercase()),
            detail: Some(format!(
                "This operation is not supported for {}s.",
                relation_kind_name(entry.relkind)
            )),
            hint: None,
            sqlstate: "42809",
        }),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

fn resolve_owned_by_clause(
    catalog: &dyn CatalogLookup,
    sequence_namespace_oid: u32,
    clause: Option<&SequenceOwnedByClause>,
) -> Result<Option<SequenceOwnedByRef>, ExecError> {
    match clause {
        None | Some(SequenceOwnedByClause::None) => Ok(None),
        Some(SequenceOwnedByClause::Column {
            table_name,
            column_name,
        }) => {
            let relation = catalog.lookup_any_relation(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(table_name.clone()))
            })?;
            if relation.relkind != 'r' {
                let object_kind_plural = if relation.relkind == 'i' {
                    "indexes".to_string()
                } else {
                    format!("{}s", relation_kind_name(relation.relkind))
                };
                return Err(ExecError::DetailedError {
                    message: format!(r#"sequence cannot be owned by relation "{}""#, table_name),
                    detail: Some(format!(
                        "This operation is not supported for {object_kind_plural}."
                    )),
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            if relation.namespace_oid != sequence_namespace_oid {
                return Err(ExecError::DetailedError {
                    message: "sequence must be in same schema as table it is linked to".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "55000",
                });
            }
            let attnum = relation
                .desc
                .columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some((index + 1) as i32)
                })
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        r#"column "{}" of relation "{}" does not exist"#,
                        column_name, table_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42703",
                })?;
            Ok(Some(SequenceOwnedByRef {
                relation_oid: relation.relation_oid,
                attnum,
            }))
        }
    }
}

fn find_sequence_default_refs(
    catalog: &dyn CatalogLookup,
    sequence_oid: u32,
) -> Vec<(u32, String)> {
    let mut refs = Vec::new();
    for class in catalog.class_rows() {
        let Some(entry) = catalog.relation_by_oid(class.oid) else {
            continue;
        };
        if entry.relkind != 'r' {
            continue;
        }
        for column in &entry.desc.columns {
            if !column.dropped && column.default_sequence_oid == Some(sequence_oid) {
                refs.push((entry.relation_oid, column.name.clone()));
            }
        }
    }
    refs
}

fn find_sequence_type_relation_refs(
    catalog: &dyn CatalogLookup,
    sequence_oid: u32,
) -> Vec<(u32, String)> {
    let Some(class_row) = catalog.class_row_by_oid(sequence_oid) else {
        return Vec::new();
    };
    if class_row.reltype == 0 {
        return Vec::new();
    }

    let mut referenced_type_oids = vec![class_row.reltype];
    if let Some(row_type) = catalog.type_by_oid(class_row.reltype)
        && row_type.typarray != 0
    {
        referenced_type_oids.push(row_type.typarray);
    }

    let mut refs = BTreeMap::new();
    for class in catalog.class_rows() {
        let Some(entry) = catalog.relation_by_oid(class.oid) else {
            continue;
        };
        if entry.relation_oid == sequence_oid {
            continue;
        }
        if !matches!(entry.relkind, 'r' | 'S' | 'i' | 't' | 'v' | 'c') {
            continue;
        }
        let uses_sequence_type = entry.desc.columns.iter().any(|column| {
            CatalogLookup::type_oid_for_sql_type(catalog, column.sql_type)
                .is_some_and(|oid| referenced_type_oids.contains(&oid))
                || (column.sql_type.type_oid != 0
                    && referenced_type_oids.contains(&column.sql_type.type_oid))
        });
        if !uses_sequence_type {
            continue;
        }
        let display_name = class.relname;
        refs.entry(entry.relation_oid)
            .and_modify(|existing: &mut String| {
                if !existing.contains('.') && display_name.contains('.') {
                    *existing = display_name.clone();
                }
            })
            .or_insert(display_name);
    }
    refs.into_iter().collect()
}

impl Database {
    pub(crate) fn execute_create_sequence_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateSequenceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_create_sequence_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
            &mut sequence_effects,
        );
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            &catalog_effects,
            &temp_effects,
            &sequence_effects,
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_sequence_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateSequenceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (sequence_name, namespace_oid, persistence) = self
            .normalize_create_sequence_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let type_oid = match create_stmt.options.as_type.as_ref() {
            Some(type_name) => {
                sequence_type_oid_for_raw_type(type_name).map_err(ExecError::Parse)?
            }
            None => crate::include::catalog::INT8_TYPE_OID,
        };
        let mut options = resolve_sequence_options_spec(&create_stmt.options, type_oid)
            .map_err(ExecError::Parse)?;
        options.owned_by = resolve_owned_by_clause(
            &catalog,
            namespace_oid,
            create_stmt.options.owned_by.as_ref(),
        )?;
        let data = SequenceData {
            state: initial_sequence_state(&options),
            options,
        };

        match persistence {
            TablePersistence::Permanent | TablePersistence::Unlogged => {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let relpersistence = match persistence {
                    TablePersistence::Permanent => 'p',
                    TablePersistence::Unlogged => 'u',
                    TablePersistence::Temporary => 't',
                };
                let result = self.catalog.write().create_relation_mvcc_with_relkind(
                    sequence_name.clone(),
                    SequenceRuntime::sequence_relation_desc(),
                    namespace_oid,
                    1,
                    relpersistence,
                    'S',
                    self.auth_state(client_id).current_user_oid(),
                    None,
                    &ctx,
                );
                match result {
                    Err(CatalogError::TableAlreadyExists(_)) if create_stmt.if_not_exists => {
                        crate::backend::utils::misc::notices::push_notice(format!(
                            r#"relation "{sequence_name}" already exists, skipping"#
                        ));
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(err) => Err(map_catalog_error(err)),
                    Ok((entry, effect)) => {
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        let pg_sequence_effect = self
                            .catalog
                            .write()
                            .upsert_sequence_row_mvcc(
                                pg_sequence_row(entry.relation_oid, &data),
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        self.apply_catalog_mutation_effect_immediate(&pg_sequence_effect)?;
                        catalog_effects.push(pg_sequence_effect);
                        if let Some(owned_by) = data.options.owned_by {
                            let effect = self
                                .catalog
                                .write()
                                .set_sequence_owned_by_dependency_mvcc(
                                    entry.relation_oid,
                                    Some((owned_by.relation_oid, owned_by.attnum)),
                                    &ctx,
                                )
                                .map_err(map_catalog_error)?;
                            self.apply_catalog_mutation_effect_immediate(&effect)?;
                            catalog_effects.push(effect);
                        }
                        sequence_effects.push(self.sequences.apply_upsert(
                            entry.relation_oid,
                            data,
                            true,
                        ));
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_with_relkind_in_transaction(
                    client_id,
                    sequence_name,
                    SequenceRuntime::sequence_relation_desc(),
                    OnCommitAction::PreserveRows,
                    xid,
                    cid,
                    'S',
                    0,
                    None,
                    catalog_effects,
                    temp_effects,
                )?;
                if let Some(owned_by) = data.options.owned_by {
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid,
                        client_id,
                        waiter: None,
                        interrupts: Arc::clone(&interrupts),
                    };
                    let effect = self
                        .catalog
                        .write()
                        .set_sequence_owned_by_dependency_mvcc(
                            created.entry.relation_oid,
                            Some((owned_by.relation_oid, owned_by.attnum)),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                }
                sequence_effects.push(self.sequences.apply_upsert(
                    created.entry.relation_oid,
                    data,
                    false,
                ));
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_alter_sequence_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterSequenceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = match lookup_sequence_relation_for_ddl(&catalog, &alter_stmt.sequence_name) {
            Ok(relation) => relation,
            Err(ExecError::Parse(ParseError::TableDoesNotExist(_))) if alter_stmt.if_exists => {
                crate::backend::utils::misc::notices::push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.sequence_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(err) => return Err(err),
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_alter_sequence_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut sequence_effects,
        );
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            &catalog_effects,
            &[],
            &sequence_effects,
        );
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_sequence_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterSequenceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match lookup_sequence_relation_for_ddl(&catalog, &alter_stmt.sequence_name) {
            Ok(relation) => relation,
            Err(ExecError::Parse(ParseError::TableDoesNotExist(_))) if alter_stmt.if_exists => {
                crate::backend::utils::misc::notices::push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.sequence_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(err) => return Err(err),
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.sequence_name)?;
        let current = self
            .sequences
            .sequence_data(relation.relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    alter_stmt.sequence_name.clone(),
                ))
            })?;
        let (mut options, restart) =
            apply_sequence_option_patch(&current.options, &alter_stmt.options)
                .map_err(ExecError::Parse)?;
        if matches!(
            alter_stmt.options.owned_by,
            Some(SequenceOwnedByClause::Column { .. })
        ) || matches!(
            alter_stmt.options.owned_by,
            Some(SequenceOwnedByClause::None)
        ) {
            options.owned_by = resolve_owned_by_clause(
                &catalog,
                relation.namespace_oid,
                alter_stmt.options.owned_by.as_ref(),
            )?;
        }
        let mut next = current;
        next.options = options;
        if let Some(state) = restart {
            next.state = state;
        }
        if relation.relpersistence != 't' {
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
                .upsert_sequence_row_mvcc(pg_sequence_row(relation.relation_oid, &next), &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            if matches!(
                alter_stmt.options.owned_by,
                Some(SequenceOwnedByClause::Column { .. }) | Some(SequenceOwnedByClause::None)
            ) {
                let effect = self
                    .catalog
                    .write()
                    .set_sequence_owned_by_dependency_mvcc(
                        relation.relation_oid,
                        next.options
                            .owned_by
                            .map(|owned_by| (owned_by.relation_oid, owned_by.attnum)),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            if let Some(persistence) = alter_stmt.options.persistence {
                let relpersistence = match persistence {
                    TablePersistence::Permanent => 'p',
                    TablePersistence::Unlogged => 'u',
                    TablePersistence::Temporary => relation.relpersistence,
                };
                if relpersistence != relation.relpersistence {
                    let effect = self
                        .catalog
                        .write()
                        .alter_relation_persistence_mvcc(
                            relation.relation_oid,
                            relpersistence,
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                }
            }
        }
        sequence_effects.push(self.sequences.apply_upsert(
            relation.relation_oid,
            next,
            relation.relpersistence != 't',
        ));
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_sequence_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropSequenceStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .sequence_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|entry| entry.rel))
            .collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for sequence_name in &drop_stmt.sequence_names {
            let maybe_relation = catalog.lookup_any_relation(sequence_name);
            let relation = match maybe_relation {
                Some(relation) if relation.relkind == 'S' => relation,
                Some(_) => {
                    result = Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: sequence_name.clone(),
                        expected: relation_kind_name('S'),
                    }));
                    break;
                }
                None if drop_stmt.if_exists => continue,
                None => {
                    result = Err(ExecError::DetailedError {
                        message: format!(r#"sequence "{sequence_name}" does not exist"#).into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P01",
                    });
                    break;
                }
            };
            ensure_relation_owner(self, client_id, &relation, sequence_name)?;

            let refs = find_sequence_default_refs(&catalog, relation.relation_oid);
            let type_refs = find_sequence_type_relation_refs(&catalog, relation.relation_oid);
            if (!refs.is_empty() || !type_refs.is_empty()) && !drop_stmt.cascade {
                let mut dependents = refs
                    .iter()
                    .map(|(relation_oid, column_name)| {
                        let table_name = catalog
                            .class_row_by_oid(*relation_oid)
                            .map(|row| row.relname)
                            .unwrap_or_else(|| relation_oid.to_string());
                        format!(
                            "default value for column {column_name} of table {table_name} depends on sequence {sequence_name}"
                        )
                    })
                    .collect::<Vec<_>>();
                dependents.extend(type_refs.iter().map(|(_, name)| name.clone()));
                result = Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop sequence {} because other objects depend on it",
                        sequence_name
                    )
                    .into(),
                    detail: Some(dependents.join("\n").into()),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
                break;
            }
            if drop_stmt.cascade {
                let interrupts = self.interrupt_state(client_id);
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                for (table_oid, column_name) in refs {
                    let effect = self
                        .catalog
                        .write()
                        .alter_table_set_column_default_mvcc(
                            table_oid,
                            &column_name,
                            None,
                            None,
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    catalog_effects.push(effect);
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
                for (relation_oid, _) in type_refs {
                    let effect = match self
                        .catalog
                        .write()
                        .drop_relation_by_oid_mvcc(relation_oid, &ctx)
                    {
                        Ok((_, effect)) => effect,
                        Err(CatalogError::UnknownTable(_)) => continue,
                        Err(err) => {
                            result = Err(map_catalog_error(err));
                            break;
                        }
                    };
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                }
                if result.is_err() {
                    break;
                }
            }

            if relation.relpersistence == 't' {
                let _ = self.drop_temp_relation_in_transaction(
                    client_id,
                    sequence_name,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?;
            } else {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(relation.relation_oid, &ctx)
                    .map_err(map_catalog_error)?
                    .1;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            sequence_effects.push(
                self.sequences
                    .queue_drop(relation.relation_oid, relation.relpersistence != 't'),
            );
            dropped += 1;
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        result.map(|_| StatementResult::AffectedRows(dropped))
    }

    pub(crate) fn execute_alter_sequence_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_sequence_relation_for_ddl(&catalog, &rename_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_alter_sequence_rename_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_sequence_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_sequence_relation_for_ddl(&catalog, &rename_stmt.table_name)?;
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
        let new_name = rename_stmt.new_table_name.to_ascii_lowercase();
        let visible_type_rows = catalog.type_rows();
        if relation.relpersistence == 't' {
            let _ = self.rename_temp_relation_in_transaction(
                client_id,
                relation.relation_oid,
                &new_name,
                xid,
                cid,
                catalog_effects,
                temp_effects,
            )?;
        } else {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: None,
                interrupts,
            };
            let effect = self
                .catalog
                .write()
                .rename_relation_mvcc(relation.relation_oid, &new_name, &visible_type_rows, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
