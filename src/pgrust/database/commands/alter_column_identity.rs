use super::super::*;
use crate::backend::parser::{
    AlterColumnIdentityAction, AlterTableAlterColumnIdentityStatement, BoundRelation,
    CatalogLookup, ColumnIdentityKind, OwnedSequenceSpec, SequenceOptionsPatchSpec,
    SequenceOptionsSpec, SerialKind, SqlType, SqlTypeKind,
};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table, map_catalog_error,
};
use crate::pgrust::database::sequences::{apply_sequence_option_patch, pg_sequence_row};

fn identity_column_index(relation: &BoundRelation, column_name: &str) -> Result<usize, ExecError> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.into())))
}

fn serial_kind_for_identity_sql_type(sql_type: SqlType) -> Result<SerialKind, ParseError> {
    match sql_type.kind {
        SqlTypeKind::Int2 if !sql_type.is_array => Ok(SerialKind::Small),
        SqlTypeKind::Int4 if !sql_type.is_array => Ok(SerialKind::Regular),
        SqlTypeKind::Int8 if !sql_type.is_array => Ok(SerialKind::Big),
        _ => Err(ParseError::UnexpectedToken {
            expected: "smallint, integer, or bigint identity column",
            actual: crate::pgrust::database::ddl::format_sql_type_name(sql_type),
        }),
    }
}

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> Option<String> {
    let class = catalog.class_row_by_oid(relation_oid)?;
    let namespace = catalog.namespace_row_by_oid(class.relnamespace)?;
    Some(format!("{}.{}", namespace.nspname, class.relname))
}

fn ensure_identity_add_allowed(
    relation: &BoundRelation,
    relation_name: &str,
    column_index: usize,
) -> Result<SerialKind, ExecError> {
    let column = &relation.desc.columns[column_index];
    if column.identity.is_some() {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{}\" of relation \"{}\" is already an identity column",
                column.name, relation_name
            )
            .into(),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    if column.storage.nullable {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{}\" of relation \"{}\" must be declared NOT NULL before identity can be added",
                column.name, relation_name
            )
            .into(),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    if column.default_expr.is_some() || column.default_sequence_oid.is_some() {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{}\" of relation \"{}\" already has a default value",
                column.name, relation_name
            )
            .into(),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    serial_kind_for_identity_sql_type(column.sql_type).map_err(ExecError::Parse)
}

impl Database {
    pub(crate) fn execute_alter_table_alter_column_identity_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAlterColumnIdentityStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
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
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self
            .execute_alter_table_alter_column_identity_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
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
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_alter_table_alter_column_identity_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAlterColumnIdentityStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN IDENTITY",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let column_index = identity_column_index(&relation, &alter_stmt.column_name)?;

        match &alter_stmt.action {
            AlterColumnIdentityAction::Add(identity) => self.alter_column_add_identity(
                client_id,
                &relation,
                &alter_stmt.table_name,
                column_index,
                identity.kind,
                &identity.options,
                xid,
                cid,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?,
            AlterColumnIdentityAction::Drop { missing_ok } => self.alter_column_drop_identity(
                client_id,
                &catalog,
                &relation,
                &alter_stmt.table_name,
                column_index,
                *missing_ok,
                xid,
                cid,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?,
            AlterColumnIdentityAction::Set {
                generation,
                options,
            } => self.alter_column_set_identity(
                client_id,
                &relation,
                &alter_stmt.table_name,
                column_index,
                *generation,
                options,
                xid,
                cid,
                catalog_effects,
                sequence_effects,
            )?,
        }
        Ok(StatementResult::AffectedRows(0))
    }

    #[allow(clippy::too_many_arguments)]
    fn alter_column_add_identity(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        relation_name: &str,
        column_index: usize,
        kind: ColumnIdentityKind,
        options: &SequenceOptionsSpec,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<(), ExecError> {
        let serial_kind = ensure_identity_add_allowed(relation, relation_name, column_index)?;
        let column = &relation.desc.columns[column_index];
        let persistence = match relation.relpersistence {
            't' => TablePersistence::Temporary,
            _ => TablePersistence::Permanent,
        };
        let mut used_names = std::collections::BTreeSet::new();
        let created = self.create_owned_sequence_for_serial_column(
            client_id,
            relation_name,
            relation.namespace_oid,
            persistence,
            &OwnedSequenceSpec {
                column_index,
                column_name: column.name.clone(),
                serial_kind,
                sql_type: column.sql_type,
                options: options.clone(),
            },
            xid,
            cid,
            &mut used_names,
            catalog_effects,
            temp_effects,
            sequence_effects,
        )?;
        let default_expr = Some(format_nextval_default_oid(
            created.sequence_oid,
            column.sql_type,
        ));
        self.set_column_identity_catalog(
            client_id,
            relation,
            &column.name,
            Some(kind),
            default_expr,
            Some(created.sequence_oid),
            xid,
            cid,
            catalog_effects,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn alter_column_drop_identity(
        &self,
        client_id: ClientId,
        catalog: &dyn CatalogLookup,
        relation: &BoundRelation,
        relation_name: &str,
        column_index: usize,
        missing_ok: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<(), ExecError> {
        let column = &relation.desc.columns[column_index];
        let Some(_identity) = column.identity else {
            if missing_ok {
                return Ok(());
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    column.name, relation_name
                )
                .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        };
        let sequence_oid = column.default_sequence_oid;
        let sequence_name = sequence_oid.and_then(|oid| relation_name_for_oid(catalog, oid));
        self.set_column_identity_catalog(
            client_id,
            relation,
            &column.name,
            None,
            None,
            None,
            xid,
            cid,
            catalog_effects,
        )?;
        if let Some(sequence_oid) = sequence_oid {
            if relation.relpersistence == 't' {
                if let Some(sequence_name) = sequence_name {
                    let _ = self.drop_temp_relation_in_transaction(
                        client_id,
                        &sequence_name,
                        xid,
                        cid,
                        catalog_effects,
                        temp_effects,
                    )?;
                }
            } else {
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
                    .drop_relation_by_oid_mvcc(sequence_oid, &ctx)
                    .map_err(map_catalog_error)?
                    .1;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            sequence_effects.push(
                self.sequences
                    .queue_drop(sequence_oid, relation.relpersistence != 't'),
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn alter_column_set_identity(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        relation_name: &str,
        column_index: usize,
        generation: Option<ColumnIdentityKind>,
        options: &SequenceOptionsPatchSpec,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<(), ExecError> {
        let column = &relation.desc.columns[column_index];
        let Some(current_identity) = column.identity else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    column.name, relation_name
                )
                .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        };
        let sequence_oid = column
            .default_sequence_oid
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "identity column \"{}\" of relation \"{}\" has no sequence",
                    column.name, relation_name
                )
                .into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let current = self.sequences.sequence_data(sequence_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(sequence_oid.to_string()))
        })?;
        let (next_options, restart) =
            apply_sequence_option_patch(&current.options, options).map_err(ExecError::Parse)?;
        let mut next = current;
        next.options = next_options;
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
                .upsert_sequence_row_mvcc(pg_sequence_row(sequence_oid, &next), &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        sequence_effects.push(self.sequences.apply_upsert(
            sequence_oid,
            next,
            relation.relpersistence != 't',
        ));
        if let Some(generation) = generation
            && generation != current_identity
        {
            self.set_column_identity_catalog(
                client_id,
                relation,
                &column.name,
                Some(generation),
                column.default_expr.clone(),
                column.default_sequence_oid,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn set_column_identity_catalog(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        column_name: &str,
        identity: Option<ColumnIdentityKind>,
        default_expr: Option<String>,
        default_sequence_oid: Option<u32>,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
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
            .alter_table_set_column_identity_mvcc(
                relation.relation_oid,
                column_name,
                identity,
                default_expr.clone(),
                default_sequence_oid,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        if relation.relpersistence == 't' {
            let mut temp_desc = relation.desc.clone();
            let column = temp_desc
                .columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.into())))?;
            column.identity = identity;
            column.generated = None;
            column.default_expr = default_expr;
            column.default_sequence_oid = default_sequence_oid;
            if column.default_expr.is_none() {
                column.attrdef_oid = None;
                column.missing_default_value = None;
            }
            self.replace_temp_entry_desc(client_id, relation.relation_oid, temp_desc)?;
        }
        catalog_effects.push(effect);
        Ok(())
    }
}
