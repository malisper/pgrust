use super::super::*;
use crate::backend::commands::partition::validate_new_partition_bound;
use crate::backend::parser::{
    AggregateArgType, AggregateSignatureKind, CreateAggregateStatement, CreateFunctionReturnSpec,
    CreateFunctionStatement, FunctionArgMode, FunctionParallel, FunctionVolatility,
    OwnedSequenceSpec, PartitionBoundSpec, SequenceOptionsSpec, SqlTypeKind,
    pg_partitioned_table_row, resolve_raw_type_name, serialize_partition_bound,
};
use crate::include::catalog::{
    ANYOID, BOOTSTRAP_SUPERUSER_OID, BYTEA_TYPE_OID, INTERNAL_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PG_LANGUAGE_INTERNAL_OID, PG_LANGUAGE_PLPGSQL_OID, PG_LANGUAGE_SQL_OID, PgAggregateRow,
    PgProcRow, RECORD_TYPE_OID,
};
use crate::include::nodes::parsenodes::{ForeignKeyAction, ForeignKeyMatchType};
use crate::include::nodes::primnodes::QueryColumn;
use crate::pgrust::database::{
    SequenceData, SequenceRuntime, default_sequence_name_base, format_nextval_default_oid,
    initial_sequence_state, resolve_sequence_options_spec, sequence_type_oid_for_serial_kind,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct CreatedOwnedSequence {
    pub(super) column_index: usize,
    pub(super) sequence_oid: u32,
}

fn relation_exists_in_namespace(
    catalog: &dyn CatalogLookup,
    name: &str,
    namespace_oid: u32,
) -> bool {
    catalog
        .lookup_any_relation(name)
        .is_some_and(|relation| relation.namespace_oid == namespace_oid)
}

fn created_relkind(lowered: &crate::backend::parser::LoweredCreateTable) -> char {
    if lowered.partition_spec.is_some() {
        'p'
    } else {
        'r'
    }
}

fn validate_partitioned_table_ddl(
    table_name: &str,
    lowered: &crate::backend::parser::LoweredCreateTable,
) -> Result<(), ExecError> {
    if lowered.partition_spec.is_some() && !lowered.foreign_key_actions.is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
            "foreign keys on partitioned table \"{table_name}\""
        ))));
    }
    Ok(())
}

fn existing_view_prefix_matches(
    old_desc: &crate::backend::executor::RelationDesc,
    new_desc: &crate::backend::executor::RelationDesc,
) -> bool {
    old_desc.columns.len() <= new_desc.columns.len()
        && old_desc
            .columns
            .iter()
            .zip(new_desc.columns.iter())
            .all(|(old_column, new_column)| {
                old_column.name.eq_ignore_ascii_case(&new_column.name)
                    && old_column.sql_type == new_column.sql_type
            })
}

pub(super) fn normalize_create_proc_name_for_search_path(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    proc_name: &str,
    object_kind: &'static str,
    configured_search_path: Option<&[String]>,
) -> Result<(String, u32), ParseError> {
    let normalized = proc_name.to_ascii_lowercase();
    match schema_name.map(str::to_ascii_lowercase) {
        Some(schema) if schema == "pg_catalog" => Ok((normalized, PG_CATALOG_NAMESPACE_OID)),
        Some(schema) if schema == "pg_temp" => Err(ParseError::UnexpectedToken {
            expected: "permanent database object",
            actual: format!("temporary {object_kind}"),
        }),
        Some(schema) => db
            .visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
            .map(|namespace_oid| (normalized.clone(), namespace_oid))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "existing schema",
                actual: format!("schema \"{schema}\" does not exist"),
            }),
        None => {
            let search_path = db.effective_search_path(client_id, configured_search_path);
            for schema in search_path {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" => continue,
                    "pg_catalog" => continue,
                    _ => {
                        if let Some(namespace_oid) =
                            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
                        {
                            return Ok((normalized.clone(), namespace_oid));
                        }
                    }
                }
            }
            Err(ParseError::NoSchemaSelectedForCreate)
        }
    }
}

fn proc_parallel_code(parallel: FunctionParallel) -> char {
    match parallel {
        FunctionParallel::Unsafe => 'u',
        FunctionParallel::Restricted => 'r',
        FunctionParallel::Safe => 's',
    }
}

pub(super) fn aggregate_signature_arg_oids(
    catalog: &dyn CatalogLookup,
    signature: &AggregateSignatureKind,
) -> Result<Vec<u32>, ParseError> {
    match signature {
        AggregateSignatureKind::Star => Ok(Vec::new()),
        AggregateSignatureKind::Args(args) => args
            .iter()
            .map(|arg| match arg {
                AggregateArgType::AnyPseudo => Ok(ANYOID),
                AggregateArgType::Type(raw_type) => {
                    let sql_type = resolve_raw_type_name(raw_type, catalog)?;
                    catalog
                        .type_oid_for_sql_type(sql_type)
                        .ok_or_else(|| ParseError::UnsupportedType(format!("{sql_type:?}")))
                }
            })
            .collect(),
    }
}

fn split_proc_name(name: &str) -> (Option<&str>, &str) {
    name.rsplit_once('.')
        .map(|(schema, proc_name)| (Some(schema), proc_name))
        .unwrap_or((None, name))
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| part.parse::<u32>().ok())
        .collect()
}

fn resolve_exact_proc_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    arg_oids: &[u32],
    expected_kind: char,
) -> Result<PgProcRow, ExecError> {
    let (schema_name, base_name) = split_proc_name(proc_name);
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
    let matches = catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .filter(|row| {
            row.prokind == expected_kind
                && parse_proc_argtype_oids(&row.proargtypes)
                    .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
                && namespace_oid
                    .map(|namespace_oid| row.pronamespace == namespace_oid)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.clone()),
        [] => Err(ExecError::DetailedError {
            message: format!("function {proc_name} does not exist"),
            detail: Some(format!("expected argument OIDs {arg_oids:?}")),
            hint: None,
            sqlstate: "42883",
        }),
        _ => Err(ExecError::DetailedError {
            message: format!("function name {proc_name} is ambiguous"),
            detail: Some(format!("expected argument OIDs {arg_oids:?}")),
            hint: None,
            sqlstate: "42725",
        }),
    }
}

pub(super) fn resolve_aggregate_proc_rows(
    catalog: &dyn CatalogLookup,
    aggregate_name: &str,
    namespace_oid: Option<u32>,
    arg_oids: &[u32],
) -> Vec<(PgProcRow, PgAggregateRow)> {
    catalog
        .proc_rows_by_name(aggregate_name)
        .into_iter()
        .filter(|row| {
            row.prokind == 'a'
                && namespace_oid
                    .map(|namespace_oid| row.pronamespace == namespace_oid)
                    .unwrap_or(true)
                && parse_proc_argtype_oids(&row.proargtypes)
                    .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
        })
        .filter_map(|row| {
            catalog
                .aggregate_by_fnoid(row.oid)
                .map(|aggregate_row| (row, aggregate_row))
        })
        .collect()
}

fn proc_arg_mode(mode: FunctionArgMode) -> u8 {
    match mode {
        FunctionArgMode::In => b'i',
        FunctionArgMode::Out => b'o',
        FunctionArgMode::InOut => b'b',
    }
}

fn foreign_key_action_code(action: ForeignKeyAction) -> char {
    match action {
        ForeignKeyAction::NoAction => 'a',
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
        ForeignKeyAction::SetDefault => 'd',
    }
}

fn foreign_key_match_code(match_type: ForeignKeyMatchType) -> char {
    match match_type {
        ForeignKeyMatchType::Simple => 's',
        ForeignKeyMatchType::Full => 'f',
        ForeignKeyMatchType::Partial => 'p',
    }
}

fn create_table_like_statistics_name_base(
    relation_name: &str,
    source_row: &crate::include::catalog::PgStatisticExtRow,
    relation: &crate::backend::parser::BoundRelation,
) -> String {
    let target_name = source_row
        .stxkeys
        .iter()
        .find_map(|attnum| {
            usize::try_from(*attnum)
                .ok()
                .and_then(|index| index.checked_sub(1))
                .and_then(|index| relation.desc.columns.get(index))
                .map(|column| column.name.as_str())
        })
        .unwrap_or("expr");
    format!("{relation_name}_{target_name}_stat")
}

fn column_attnums_for_names(
    desc: &crate::backend::executor::RelationDesc,
    columns: &[String],
) -> Vec<i16> {
    columns
        .iter()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index as i16 + 1)
                })
                .unwrap_or_else(|| panic!("missing column for foreign key: {column_name}"))
        })
        .collect()
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    fn install_create_table_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        table_cid: CommandId,
        table_name: &str,
        relation: &crate::backend::parser::BoundRelation,
        lowered: &crate::backend::parser::LoweredCreateTable,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = table_cid.saturating_add(1);
        if relation.relkind == 'p' || relation.relispartition {
            next_cid = self.install_partitioned_index_backed_constraints_in_transaction(
                client_id,
                xid,
                next_cid,
                relation,
                &lowered.constraint_actions,
                configured_search_path,
                catalog_effects,
            )?;
        } else {
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
            for action in &lowered.constraint_actions {
                let action_cid = next_cid;
                next_cid = next_cid.saturating_add(3);
                let constraint_name = action
                    .constraint_name
                    .clone()
                    .expect("normalized key constraint name");
                let index_name = self.choose_available_relation_name(
                    client_id,
                    xid,
                    action_cid,
                    relation.namespace_oid,
                    &constraint_name,
                )?;
                let index_columns = action
                    .columns
                    .iter()
                    .cloned()
                    .map(crate::backend::parser::IndexColumnDef::from)
                    .collect::<Vec<_>>();
                let (access_method_oid, access_method_handler, build_options) =
                    if action.without_overlaps.is_some() {
                        self.resolve_temporal_index_build_options(
                            client_id,
                            Some((xid, action_cid)),
                            relation,
                            &index_columns,
                        )?
                    } else {
                        self.resolve_simple_index_build_options(
                            client_id,
                            Some((xid, action_cid)),
                            "btree",
                            relation,
                            &index_columns,
                            &[],
                        )?
                    };
                let build_options = crate::backend::catalog::CatalogIndexBuildOptions {
                    indimmediate: !action.deferrable,
                    ..build_options
                };
                let index_entry = self.build_simple_index_in_transaction(
                    client_id,
                    relation,
                    &index_name,
                    catalog.materialize_visible_catalog(),
                    &index_columns,
                    None,
                    true,
                    action.primary,
                    action.nulls_not_distinct,
                    xid,
                    action_cid,
                    access_method_oid,
                    access_method_handler,
                    &build_options,
                    65_536,
                    catalog_effects,
                )?;
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: action_cid.saturating_add(2),
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let primary_key_owned_not_null_oids = if action.primary {
                    action
                        .columns
                        .iter()
                        .filter_map(|column_name| {
                            relation.desc.columns.iter().find_map(|column| {
                                (column.name.eq_ignore_ascii_case(column_name)
                                    && column.not_null_primary_key_owned)
                                    .then_some(column.not_null_constraint_oid)
                                    .flatten()
                            })
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let conexclop = if action.without_overlaps.is_some() {
                    Some(self.temporal_constraint_operator_oids_for_relation(
                        relation.relation_oid,
                        &action.columns,
                        action.without_overlaps.as_deref(),
                        &catalog,
                    )?)
                } else {
                    None
                };
                let constraint_effect = self
                    .catalog
                    .write()
                    .create_index_backed_constraint_mvcc_with_period(
                        relation.relation_oid,
                        index_entry.relation_oid,
                        constraint_name,
                        if action.primary {
                            crate::include::catalog::CONSTRAINT_PRIMARY
                        } else {
                            crate::include::catalog::CONSTRAINT_UNIQUE
                        },
                        &primary_key_owned_not_null_oids,
                        action.without_overlaps.is_some(),
                        conexclop,
                        action.deferrable,
                        action.initially_deferred,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
                catalog_effects.push(constraint_effect);
            }
        }

        let check_base_cid = next_cid;
        for (index, action) in lowered.check_actions.iter().enumerate() {
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, check_base_cid)),
                configured_search_path,
            );
            crate::backend::parser::bind_check_constraint_expr(
                &action.expr_sql,
                Some(table_name),
                &relation.desc,
                &catalog,
            )?;
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: check_base_cid.saturating_add(index as u32),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let constraint_effect = self
                .catalog
                .write()
                .create_check_constraint_mvcc(
                    relation.relation_oid,
                    action.constraint_name.clone(),
                    action.enforced,
                    action.enforced && !action.not_valid,
                    action.no_inherit,
                    action.expr_sql.clone(),
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
        }

        let foreign_key_base_cid =
            check_base_cid.saturating_add(lowered.check_actions.len() as u32);
        for (index, action) in lowered.foreign_key_actions.iter().enumerate() {
            let constraint_cid = foreign_key_base_cid.saturating_add(index as u32);
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, constraint_cid)),
                configured_search_path,
            );
            let (referenced_relation, referenced_index_oid) = if action.self_referential {
                let referenced_relation = catalog
                    .lookup_relation_by_oid(relation.relation_oid)
                    .unwrap_or_else(|| relation.clone());
                let referenced_attnums =
                    column_attnums_for_names(&referenced_relation.desc, &action.referenced_columns);
                let referenced_index_oid = catalog
                    .index_relations_for_heap(referenced_relation.relation_oid)
                    .into_iter()
                    .find(|index| {
                        index.index_meta.indisunique
                            && index.index_meta.indkey == referenced_attnums
                    })
                    .map(|index| index.relation_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "referenced UNIQUE or PRIMARY KEY index",
                            actual: format!(
                                "table \"{table_name}\" lacks an exact matching unique key"
                            ),
                        })
                    })?;
                (referenced_relation, referenced_index_oid)
            } else {
                let referenced_relation = catalog
                    .lookup_relation_by_oid(action.referenced_relation_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
                    })?;
                (referenced_relation, action.referenced_index_oid)
            };
            let local_attnums = column_attnums_for_names(&relation.desc, &action.columns);
            let referenced_attnums =
                column_attnums_for_names(&referenced_relation.desc, &action.referenced_columns);
            let delete_set_attnums = action
                .on_delete_set_columns
                .as_deref()
                .map(|columns| column_attnums_for_names(&relation.desc, columns));
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: constraint_cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let constraint_effect = self
                .catalog
                .write()
                .create_foreign_key_constraint_mvcc(
                    relation.relation_oid,
                    action.constraint_name.clone(),
                    action.deferrable,
                    action.initially_deferred,
                    action.enforced,
                    action.enforced && !action.not_valid,
                    &local_attnums,
                    referenced_relation.relation_oid,
                    referenced_index_oid,
                    &referenced_attnums,
                    foreign_key_action_code(action.on_update),
                    foreign_key_action_code(action.on_delete),
                    foreign_key_match_code(action.match_type),
                    delete_set_attnums.as_deref(),
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
        }

        Ok(())
    }

    pub(super) fn refresh_partitioned_relation_metadata(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::parser::BoundRelation, ExecError> {
        self.invalidate_backend_cache_state(client_id);
        let visible_cid = cid.saturating_add(1);
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, visible_cid)), configured_search_path);
        catalog.relation_by_oid(relation_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replace_relation_partition_metadata_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        relispartition: bool,
        relpartbound: Option<String>,
        partitioned_table: Option<crate::include::catalog::PgPartitionedTableRow>,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::parser::BoundRelation, ExecError> {
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
            .replace_relation_partitioning_mvcc(
                relation_oid,
                relispartition,
                relpartbound.clone(),
                partitioned_table.clone(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        let relation = self.refresh_partitioned_relation_metadata(
            client_id,
            relation_oid,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        if relation.relpersistence == 't' {
            self.replace_temp_entry_partition_metadata(
                client_id,
                relation_oid,
                relation.relkind,
                relispartition,
                relpartbound,
                partitioned_table,
            )?;
        }
        Ok(relation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn update_partitioned_table_default_partition_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        partdefid: u32,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let relation = self.refresh_partitioned_relation_metadata(
            client_id,
            relation_oid,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        let Some(partitioned_table) = relation.partitioned_table.clone() else {
            return Ok(());
        };
        let updated = crate::include::catalog::PgPartitionedTableRow {
            partdefid,
            ..partitioned_table
        };
        let _ = self.replace_relation_partition_metadata_in_transaction(
            client_id,
            relation_oid,
            relation.relispartition,
            relation.relpartbound.clone(),
            Some(updated),
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_owned_sequence_for_serial_column(
        &self,
        client_id: ClientId,
        table_name: &str,
        namespace_oid: u32,
        persistence: TablePersistence,
        column: &OwnedSequenceSpec,
        xid: TransactionId,
        cid: CommandId,
        used_names: &mut std::collections::BTreeSet<String>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<CreatedOwnedSequence, ExecError> {
        let base_name = default_sequence_name_base(table_name, &column.column_name);
        let mut sequence_name =
            self.choose_available_relation_name(client_id, xid, cid, namespace_oid, &base_name)?;
        if !used_names.insert(sequence_name.to_ascii_lowercase()) {
            for suffix in 1.. {
                let candidate = format!("{base_name}{suffix}");
                if used_names.insert(candidate.to_ascii_lowercase()) {
                    sequence_name = candidate;
                    break;
                }
            }
        }

        let options = resolve_sequence_options_spec(
            &SequenceOptionsSpec::default(),
            sequence_type_oid_for_serial_kind(column.serial_kind),
        )
        .map_err(ExecError::Parse)?;
        let data = SequenceData {
            state: initial_sequence_state(&options),
            options,
        };

        let sequence_oid = match persistence {
            TablePersistence::Permanent => {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let (entry, effect) = self
                    .catalog
                    .write()
                    .create_relation_mvcc_with_relkind(
                        sequence_name,
                        SequenceRuntime::sequence_relation_desc(),
                        namespace_oid,
                        1,
                        'p',
                        'S',
                        self.auth_state(client_id).current_user_oid(),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                sequence_effects.push(self.sequences.apply_upsert(entry.relation_oid, data, true));
                entry.relation_oid
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
                    catalog_effects,
                    temp_effects,
                )?;
                sequence_effects.push(self.sequences.apply_upsert(
                    created.entry.relation_oid,
                    data,
                    false,
                ));
                created.entry.relation_oid
            }
        };

        Ok(CreatedOwnedSequence {
            column_index: column.column_index,
            sequence_oid,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_create_table_like_post_create_actions(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        lowered: &crate::backend::parser::LoweredCreateTable,
        xid: TransactionId,
        mut cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        for action in &lowered.like_post_create_actions {
            if action.include_comments {
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
                    .copy_relation_column_comments_mvcc(
                        action.source_relation_oid,
                        relation.relation_oid,
                        i32::try_from(relation.desc.columns.len()).unwrap_or(i32::MAX),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                cid = cid.saturating_add(1);
            }

            if action.include_statistics {
                cid = self.copy_create_table_like_statistics(
                    client_id,
                    action.source_relation_oid,
                    relation,
                    action.include_comments,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
        }
        Ok(cid)
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_create_table_like_statistics(
        &self,
        client_id: ClientId,
        source_relation_oid: u32,
        relation: &crate::backend::parser::BoundRelation,
        include_comments: bool,
        xid: TransactionId,
        mut cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let source_statistics = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .statistic_ext_rows_for_relation(source_relation_oid);
        if source_statistics.is_empty() {
            return Ok(cid);
        }

        let relation_name = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .class_by_oid(relation.relation_oid)
            .map(|row| row.relname.to_ascii_lowercase())
            .unwrap_or_else(|| relation.relation_oid.to_string());
        let mut used_names = std::collections::BTreeSet::new();
        for source_row in source_statistics {
            let base_name =
                create_table_like_statistics_name_base(&relation_name, &source_row, relation);
            let mut candidate = base_name.clone();
            let mut suffix = 1usize;
            loop {
                let catalog = self
                    .backend_catcache(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                if !used_names.contains(&candidate)
                    && catalog
                        .statistic_ext_row_by_name_namespace(&candidate, relation.namespace_oid)
                        .is_none()
                {
                    break;
                }
                candidate = format!("{base_name}{suffix}");
                suffix = suffix.saturating_add(1);
            }
            used_names.insert(candidate.clone());

            let row = crate::include::catalog::PgStatisticExtRow {
                oid: 0,
                stxrelid: relation.relation_oid,
                stxname: candidate,
                stxnamespace: relation.namespace_oid,
                stxowner: self.auth_state(client_id).current_user_oid(),
                stxkeys: source_row.stxkeys.clone(),
                stxstattarget: source_row.stxstattarget,
                stxkind: source_row.stxkind.clone(),
                stxexprs: source_row.stxexprs.clone(),
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
            let (created_oid, effect) = self
                .catalog
                .write()
                .create_statistics_mvcc(row, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);

            if include_comments {
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
                    .copy_object_comment_mvcc(
                        source_row.oid,
                        crate::include::catalog::PG_STATISTIC_EXT_RELATION_OID,
                        created_oid,
                        crate::include::catalog::PG_STATISTIC_EXT_RELATION_OID,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                cid = cid.saturating_add(1);
            }
        }

        self.plan_cache.invalidate_all();
        Ok(cid)
    }

    pub(crate) fn execute_create_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let sql_type = crate::backend::parser::resolve_raw_type_name(&create_stmt.ty, &catalog)
            .map_err(ExecError::Parse)?;
        let (normalized, object_name, namespace_oid) = self
            .normalize_domain_name_for_create(&create_stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        if domains.contains_key(&normalized) {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                create_stmt.domain_name.clone(),
            )));
        }
        let oid = {
            let catalog = self.catalog.write();
            let snapshot = catalog.catalog_snapshot().map_err(map_catalog_error)?;
            let next_catalog_oid = snapshot.next_oid();
            domains
                .values()
                .map(|domain| domain.oid.saturating_add(1))
                .max()
                .unwrap_or(next_catalog_oid)
                .max(next_catalog_oid)
        };
        domains.insert(
            normalized,
            DomainEntry {
                oid,
                name: object_name,
                namespace_oid,
                sql_type,
                comment: None,
            },
        );
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_function_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_function_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_function_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let (function_name, namespace_oid) = normalize_create_proc_name_for_search_path(
            self,
            client_id,
            Some((xid, cid)),
            create_stmt.schema_name.as_deref(),
            &create_stmt.function_name,
            "function",
            configured_search_path,
        )?;

        let language_row = catalog
            .language_row_by_name(&create_stmt.language)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "LANGUAGE plpgsql, sql, or internal",
                    actual: format!("LANGUAGE {}", create_stmt.language),
                })
            })?;
        if !matches!(
            language_row.oid,
            PG_LANGUAGE_PLPGSQL_OID | PG_LANGUAGE_SQL_OID | PG_LANGUAGE_INTERNAL_OID
        ) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "LANGUAGE plpgsql, sql, or internal",
                actual: format!("LANGUAGE {}", create_stmt.language),
            }));
        }

        let mut callable_arg_oids = Vec::new();
        let mut all_arg_oids = Vec::new();
        let mut all_arg_modes = Vec::new();
        let mut all_arg_names = Vec::new();
        let mut output_args = Vec::new();

        for arg in &create_stmt.args {
            let sql_type = resolve_raw_type_name(&arg.ty, &catalog).map_err(ExecError::Parse)?;
            let type_oid = catalog
                .type_oid_for_sql_type(sql_type)
                .or_else(|| matches!(sql_type.kind, SqlTypeKind::Record).then_some(RECORD_TYPE_OID))
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(
                        arg.name.clone().unwrap_or_else(|| format!("{:?}", arg.ty)),
                    ))
                })?;

            if matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut) {
                callable_arg_oids.push(type_oid);
            }
            if matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut) {
                output_args.push(QueryColumn {
                    name: arg.name.clone().unwrap_or_default(),
                    sql_type,
                    wire_type_oid: None,
                });
            }
            all_arg_oids.push(type_oid);
            all_arg_modes.push(proc_arg_mode(arg.mode));
            all_arg_names.push(arg.name.clone().unwrap_or_default());
        }

        let mut proretset = false;
        let prorettype: u32;
        let mut proallargtypes = None;
        let mut proargmodes = None;
        let mut proargnames = all_arg_names
            .iter()
            .any(|name| !name.is_empty())
            .then_some(all_arg_names.clone());

        match &create_stmt.return_spec {
            CreateFunctionReturnSpec::Type { ty, setof } => {
                let sql_type = resolve_raw_type_name(ty, &catalog).map_err(ExecError::Parse)?;
                proretset = *setof;
                prorettype = if matches!(sql_type.kind, SqlTypeKind::Record) {
                    RECORD_TYPE_OID
                } else {
                    catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnsupportedType(format!("{sql_type:?}")))
                    })?
                };
                if !output_args.is_empty() {
                    let required_rettype = if output_args.len() == 1 {
                        catalog
                            .type_oid_for_sql_type(output_args[0].sql_type)
                            .or_else(|| {
                                matches!(output_args[0].sql_type.kind, SqlTypeKind::Record)
                                    .then_some(RECORD_TYPE_OID)
                            })
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::UnsupportedType(
                                    output_args[0].name.clone(),
                                ))
                            })?
                    } else {
                        RECORD_TYPE_OID
                    };
                    if prorettype != required_rettype {
                        return Err(ExecError::DetailedError {
                            message: "function result type must match OUT arguments".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42P13",
                        });
                    }
                    proallargtypes = Some(all_arg_oids.clone());
                    proargmodes = Some(all_arg_modes.clone());
                    proargnames = all_arg_names
                        .iter()
                        .any(|name| !name.is_empty())
                        .then_some(all_arg_names.clone());
                }
            }
            CreateFunctionReturnSpec::Table(columns) => {
                proretset = true;
                prorettype = RECORD_TYPE_OID;
                let mut table_oids = Vec::with_capacity(columns.len());
                let mut table_names = Vec::with_capacity(columns.len());
                for column in columns {
                    let sql_type =
                        resolve_raw_type_name(&column.ty, &catalog).map_err(ExecError::Parse)?;
                    if matches!(sql_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
                        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                            "record and composite RETURNS TABLE columns are not supported yet"
                                .into(),
                        )));
                    }
                    table_oids.push(catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                    })?);
                    table_names.push(column.name.clone());
                }
                proallargtypes = Some(
                    callable_arg_oids
                        .iter()
                        .copied()
                        .chain(table_oids.iter().copied())
                        .collect(),
                );
                proargmodes = Some(
                    create_stmt
                        .args
                        .iter()
                        .map(|arg| proc_arg_mode(arg.mode))
                        .filter(|mode| matches!(*mode, b'i' | b'b'))
                        .chain(std::iter::repeat_n(b't', table_oids.len()))
                        .collect(),
                );
                let mut names = create_stmt
                    .args
                    .iter()
                    .filter(|arg| matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut))
                    .map(|arg| arg.name.clone().unwrap_or_default())
                    .collect::<Vec<_>>();
                names.extend(table_names);
                proargnames = Some(names);
            }
            CreateFunctionReturnSpec::DerivedFromOutArgs { setof_record } => {
                if output_args.is_empty() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "OUT or INOUT arguments",
                        actual: create_stmt.function_name.clone(),
                    }));
                }
                proallargtypes = Some(all_arg_oids.clone());
                proargmodes = Some(all_arg_modes.clone());
                proargnames = all_arg_names
                    .iter()
                    .any(|name| !name.is_empty())
                    .then_some(all_arg_names.clone());
                if *setof_record {
                    proretset = true;
                    prorettype = RECORD_TYPE_OID;
                } else if output_args.len() == 1 {
                    prorettype = catalog
                        .type_oid_for_sql_type(output_args[0].sql_type)
                        .or_else(|| {
                            matches!(output_args[0].sql_type.kind, SqlTypeKind::Record)
                                .then_some(RECORD_TYPE_OID)
                        })
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnsupportedType(
                                output_args[0].name.clone(),
                            ))
                        })?;
                } else {
                    prorettype = RECORD_TYPE_OID;
                }
            }
        }

        let proargtypes = callable_arg_oids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let existing_proc = catalog
            .proc_rows_by_name(&function_name)
            .into_iter()
            .find(|row| row.pronamespace == namespace_oid && row.proargtypes == proargtypes);
        if existing_proc.is_some() && !create_stmt.replace_existing {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unique function signature",
                actual: format!("function {}({}) already exists", function_name, proargtypes),
            }));
        }

        let proc_row = PgProcRow {
            oid: 0,
            proname: function_name.clone(),
            pronamespace: namespace_oid,
            proowner: BOOTSTRAP_SUPERUSER_OID,
            prolang: language_row.oid,
            procost: create_stmt
                .cost
                .as_deref()
                .map(|cost| {
                    cost.parse::<f64>()
                        .expect("validated CREATE FUNCTION COST must parse")
                })
                .unwrap_or(100.0),
            prorows: if proretset { 1000.0 } else { 0.0 },
            provariadic: 0,
            prosupport: 0,
            prokind: 'f',
            prosecdef: false,
            proleakproof: create_stmt.leakproof,
            proisstrict: create_stmt.strict,
            proretset,
            provolatile: match create_stmt.volatility {
                FunctionVolatility::Volatile => 'v',
                FunctionVolatility::Stable => 's',
                FunctionVolatility::Immutable => 'i',
            },
            proparallel: proc_parallel_code(create_stmt.parallel),
            pronargs: callable_arg_oids.len() as i16,
            pronargdefaults: 0,
            prorettype,
            proargtypes,
            proallargtypes,
            proargmodes,
            proargnames,
            prosrc: create_stmt.body.clone(),
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            let (_oid, effect) = if let Some(existing) = existing_proc {
                catalog_store
                    .replace_proc_mvcc(&existing, proc_row, &ctx)
                    .map_err(map_catalog_error)?
            } else {
                catalog_store
                    .create_proc_mvcc(proc_row, &ctx)
                    .map_err(map_catalog_error)?
            };
            effect
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_aggregate_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateAggregateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_aggregate_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_aggregate_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateAggregateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let (aggregate_name, namespace_oid) = normalize_create_proc_name_for_search_path(
            self,
            client_id,
            txn_ctx,
            create_stmt.schema_name.as_deref(),
            &create_stmt.aggregate_name,
            "aggregate",
            configured_search_path,
        )?;
        let arg_oids = aggregate_signature_arg_oids(&catalog, &create_stmt.signature)?;
        let stype =
            resolve_raw_type_name(&create_stmt.stype, &catalog).map_err(ExecError::Parse)?;
        let stype_oid = catalog
            .type_oid_for_sql_type(stype)
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("{stype:?}"))))?;
        let mut trans_arg_oids = Vec::with_capacity(arg_oids.len() + 1);
        trans_arg_oids.push(stype_oid);
        trans_arg_oids.extend(arg_oids.iter().copied());
        let transfn_row = resolve_exact_proc_row(
            self,
            client_id,
            txn_ctx,
            &catalog,
            &create_stmt.sfunc_name,
            &trans_arg_oids,
            'f',
        )?;
        let finalfn_row = create_stmt
            .finalfunc_name
            .as_deref()
            .map(|name| {
                resolve_exact_proc_row(self, client_id, txn_ctx, &catalog, name, &[stype_oid], 'f')
            })
            .transpose()?;
        if create_stmt.serialfunc_name.is_some() != create_stmt.deserialfunc_name.is_some() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "both SERIALFUNC and DESERIALFUNC",
                actual: aggregate_name.clone(),
            }));
        }
        let combinefn_row = create_stmt
            .combinefunc_name
            .as_deref()
            .map(|name| {
                resolve_exact_proc_row(
                    self,
                    client_id,
                    txn_ctx,
                    &catalog,
                    name,
                    &[stype_oid, stype_oid],
                    'f',
                )
            })
            .transpose()?;
        let serialfn_row = create_stmt
            .serialfunc_name
            .as_deref()
            .map(|name| {
                resolve_exact_proc_row(self, client_id, txn_ctx, &catalog, name, &[stype_oid], 'f')
            })
            .transpose()?;
        let deserialfn_row = create_stmt
            .deserialfunc_name
            .as_deref()
            .map(|name| {
                resolve_exact_proc_row(
                    self,
                    client_id,
                    txn_ctx,
                    &catalog,
                    name,
                    &[BYTEA_TYPE_OID, INTERNAL_TYPE_OID],
                    'f',
                )
            })
            .transpose()?;
        let mstype_oid = create_stmt
            .mstype
            .as_ref()
            .map(|mtype| {
                resolve_raw_type_name(mtype, &catalog)
                    .map_err(ExecError::Parse)
                    .and_then(|sql_type| {
                        catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnsupportedType(format!("{sql_type:?}")))
                        })
                    })
            })
            .transpose()?;
        let msfunc_row = create_stmt
            .msfunc_name
            .as_deref()
            .map(|name| {
                let mstype_oid = mstype_oid.unwrap_or(stype_oid);
                let mut args = Vec::with_capacity(arg_oids.len() + 1);
                args.push(mstype_oid);
                args.extend(arg_oids.iter().copied());
                resolve_exact_proc_row(self, client_id, txn_ctx, &catalog, name, &args, 'f')
            })
            .transpose()?;
        let minvfunc_row = create_stmt
            .minvfunc_name
            .as_deref()
            .map(|name| {
                let mstype_oid = mstype_oid.unwrap_or(stype_oid);
                let mut args = Vec::with_capacity(arg_oids.len() + 1);
                args.push(mstype_oid);
                args.extend(arg_oids.iter().copied());
                resolve_exact_proc_row(self, client_id, txn_ctx, &catalog, name, &args, 'f')
            })
            .transpose()?;
        let mfinalfn_row = create_stmt
            .mfinalfunc_name
            .as_deref()
            .map(|name| {
                resolve_exact_proc_row(
                    self,
                    client_id,
                    txn_ctx,
                    &catalog,
                    name,
                    &[mstype_oid.unwrap_or(stype_oid)],
                    'f',
                )
            })
            .transpose()?;
        let result_type_oid = finalfn_row
            .as_ref()
            .map(|row| row.prorettype)
            .unwrap_or(stype_oid);
        let proargtypes = arg_oids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let conflicting_non_aggregate = catalog
            .proc_rows_by_name(&aggregate_name)
            .into_iter()
            .find(|row| {
                row.pronamespace == namespace_oid
                    && parse_proc_argtype_oids(&row.proargtypes)
                        .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
                    && row.prokind != 'a'
            });
        if conflicting_non_aggregate.is_some() {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: aggregate_name.clone(),
                expected: "aggregate",
            }));
        }
        let existing =
            resolve_aggregate_proc_rows(&catalog, &aggregate_name, Some(namespace_oid), &arg_oids);
        if !existing.is_empty() && !create_stmt.replace_existing {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unique aggregate signature",
                actual: format!("aggregate {aggregate_name}({proargtypes}) already exists"),
            }));
        }

        let proc_row = PgProcRow {
            oid: 0,
            proname: aggregate_name.clone(),
            pronamespace: namespace_oid,
            proowner: BOOTSTRAP_SUPERUSER_OID,
            prolang: PG_LANGUAGE_INTERNAL_OID,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind: 'a',
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'i',
            proparallel: create_stmt.parallel.map(proc_parallel_code).unwrap_or('u'),
            pronargs: arg_oids.len() as i16,
            pronargdefaults: 0,
            prorettype: result_type_oid,
            proargtypes,
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
            prosrc: aggregate_name.clone(),
        };
        let aggregate_row = PgAggregateRow {
            aggfnoid: 0,
            aggkind: 'n',
            aggnumdirectargs: 0,
            aggtransfn: transfn_row.oid,
            aggfinalfn: finalfn_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggcombinefn: combinefn_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggserialfn: serialfn_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggdeserialfn: deserialfn_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggmtransfn: msfunc_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggminvtransfn: minvfunc_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggmfinalfn: mfinalfn_row.as_ref().map(|row| row.oid).unwrap_or(0),
            aggfinalextra: create_stmt.finalfunc_extra,
            aggmfinalextra: create_stmt.mfinalfunc_extra,
            aggfinalmodify: create_stmt.finalfunc_modify,
            aggmfinalmodify: create_stmt.mfinalfunc_modify,
            aggsortop: 0,
            aggtranstype: stype_oid,
            aggtransspace: create_stmt.transspace,
            aggmtranstype: mstype_oid.unwrap_or(0),
            aggmtransspace: create_stmt.mtransspace,
            agginitval: create_stmt.initcond.clone(),
            aggminitval: create_stmt.minitcond.clone(),
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            let (_oid, effect) = if let Some((old_proc_row, old_aggregate_row)) = existing.first() {
                catalog_store
                    .replace_aggregate_mvcc(
                        old_proc_row,
                        old_aggregate_row,
                        proc_row,
                        aggregate_row,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            } else {
                catalog_store
                    .create_aggregate_mvcc(proc_row, aggregate_row, &ctx)
                    .map_err(map_catalog_error)?
            };
            effect
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_create_table_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_view_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (table_name, namespace_oid, persistence) = self
            .normalize_create_table_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let lowered = lower_create_table_with_catalog(create_stmt, &catalog, persistence)?;
        if create_stmt.if_not_exists
            && relation_exists_in_namespace(&catalog, &table_name, namespace_oid)
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        validate_partitioned_table_ddl(&table_name, &lowered)?;
        if let Some(parent_oid) = lowered.partition_parent_oid
            && let Some(bound) = lowered.partition_bound.as_ref()
        {
            let parent = catalog.relation_by_oid(parent_oid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(parent_oid.to_string()))
            })?;
            validate_new_partition_bound(&catalog, &parent, &table_name, bound, None)?;
        }

        let mut desc = lowered.relation_desc.clone();
        let mut used_sequence_names = std::collections::BTreeSet::new();
        let mut created_sequences = Vec::with_capacity(lowered.owned_sequences.len());
        for serial_column in &lowered.owned_sequences {
            created_sequences.push(self.create_owned_sequence_for_serial_column(
                client_id,
                &table_name,
                namespace_oid,
                persistence,
                serial_column,
                xid,
                cid,
                &mut used_sequence_names,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?);
        }
        for created in created_sequences {
            let column = desc
                .columns
                .get_mut(created.column_index)
                .expect("serial column index must exist");
            column.default_expr = Some(format_nextval_default_oid(
                created.sequence_oid,
                column.sql_type,
            ));
            column.default_sequence_oid = Some(created.sequence_oid);
            column.missing_default_value = None;
        }

        let table_cid = cid;
        let relation_relkind = created_relkind(&lowered);
        match persistence {
            TablePersistence::Permanent => {
                let mut catalog_guard = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: table_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let result = if relation_relkind == 'r' {
                    catalog_guard.create_table_mvcc_with_options(
                        table_name.clone(),
                        desc.clone(),
                        namespace_oid,
                        self.database_oid,
                        'p',
                        crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                        self.auth_state(client_id).current_user_oid(),
                        &ctx,
                    )
                } else {
                    catalog_guard
                        .create_relation_mvcc_with_relkind(
                            table_name.clone(),
                            desc.clone(),
                            namespace_oid,
                            self.database_oid,
                            'p',
                            relation_relkind,
                            self.auth_state(client_id).current_user_oid(),
                            &ctx,
                        )
                        .map(|(entry, effect)| {
                            (
                                crate::backend::catalog::store::CreateTableResult {
                                    entry,
                                    toast: None,
                                },
                                effect,
                            )
                        })
                };
                match result {
                    Err(CatalogError::TableAlreadyExists(_name)) if create_stmt.if_not_exists => {
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(err) => Err(map_catalog_error(err)),
                    Ok((created, effect)) => {
                        drop(catalog_guard);
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        if !lowered.parent_oids.is_empty() {
                            let inherit_ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: table_cid.saturating_add(1),
                                client_id,
                                waiter: None,
                                interrupts: Arc::clone(&interrupts),
                            };
                            let inherit_effect = self
                                .catalog
                                .write()
                                .create_relation_inheritance_mvcc(
                                    created.entry.relation_oid,
                                    &lowered.parent_oids,
                                    &inherit_ctx,
                                )
                                .map_err(map_catalog_error)?;
                            self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
                            catalog_effects.push(inherit_effect);
                        }
                        let relpartbound = lowered
                            .partition_bound
                            .as_ref()
                            .map(serialize_partition_bound)
                            .transpose()
                            .map_err(ExecError::Parse)?;
                        let partitioned_table = lowered.partition_spec.as_ref().map(|spec| {
                            pg_partitioned_table_row(created.entry.relation_oid, spec, 0)
                        });
                        let relation = if lowered.partition_parent_oid.is_some()
                            || lowered.partition_spec.is_some()
                        {
                            let relation = self
                                .replace_relation_partition_metadata_in_transaction(
                                    client_id,
                                    created.entry.relation_oid,
                                    lowered.partition_parent_oid.is_some(),
                                    relpartbound,
                                    partitioned_table,
                                    xid,
                                    table_cid.saturating_add(2),
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            if lowered
                                .partition_bound
                                .as_ref()
                                .is_some_and(PartitionBoundSpec::is_default)
                                && let Some(parent_oid) = lowered.partition_parent_oid
                            {
                                self.update_partitioned_table_default_partition_in_transaction(
                                    client_id,
                                    parent_oid,
                                    created.entry.relation_oid,
                                    xid,
                                    table_cid.saturating_add(3),
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            }
                            relation
                        } else {
                            crate::backend::parser::BoundRelation {
                                rel: created.entry.rel,
                                relation_oid: created.entry.relation_oid,
                                toast: created.toast.as_ref().map(|toast| {
                                    crate::include::nodes::primnodes::ToastRelationRef {
                                        rel: toast.toast_entry.rel,
                                        relation_oid: toast.toast_entry.relation_oid,
                                    }
                                }),
                                namespace_oid: created.entry.namespace_oid,
                                owner_oid: created.entry.owner_oid,
                                relpersistence: created.entry.relpersistence,
                                relkind: created.entry.relkind,
                                relispopulated: created.entry.relispopulated,
                                relispartition: created.entry.relispartition,
                                relpartbound: created.entry.relpartbound.clone(),
                                desc: created.entry.desc.clone(),
                                partitioned_table: created.entry.partitioned_table.clone(),
                            }
                        };
                        let mut constraint_cid_base = table_cid.saturating_add(1);
                        if !lowered.parent_oids.is_empty() {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(2));
                        }
                        if lowered.partition_spec.is_some()
                            || lowered.partition_parent_oid.is_some()
                        {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(3));
                        }
                        if lowered
                            .partition_bound
                            .as_ref()
                            .is_some_and(PartitionBoundSpec::is_default)
                        {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(4));
                        }
                        self.install_create_table_constraints_in_transaction(
                            client_id,
                            xid,
                            constraint_cid_base,
                            &table_name,
                            &relation,
                            &lowered,
                            configured_search_path,
                            catalog_effects,
                        )?;
                        let next_cid = self.apply_create_table_like_post_create_actions(
                            client_id,
                            &relation,
                            &lowered,
                            xid,
                            constraint_cid_base.saturating_add(1),
                            catalog_effects,
                        )?;
                        if let Some(parent_oid) = lowered.partition_parent_oid {
                            let next_cid = self
                                .reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
                                    client_id,
                                    xid,
                                    next_cid,
                                    parent_oid,
                                    relation.relation_oid,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            self.clone_parent_row_triggers_to_partition_in_transaction(
                                client_id,
                                xid,
                                next_cid,
                                parent_oid,
                                relation.relation_oid,
                                configured_search_path,
                                catalog_effects,
                            )?;
                        }
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_with_relkind_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc,
                    create_stmt.on_commit,
                    xid,
                    table_cid,
                    relation_relkind,
                    catalog_effects,
                    temp_effects,
                )?;
                if !lowered.parent_oids.is_empty() {
                    let inherit_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: table_cid.saturating_add(1),
                        client_id,
                        waiter: None,
                        interrupts,
                    };
                    let inherit_effect = self
                        .catalog
                        .write()
                        .create_relation_inheritance_mvcc(
                            created.entry.relation_oid,
                            &lowered.parent_oids,
                            &inherit_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
                    catalog_effects.push(inherit_effect);
                }
                let relpartbound = lowered
                    .partition_bound
                    .as_ref()
                    .map(serialize_partition_bound)
                    .transpose()
                    .map_err(ExecError::Parse)?;
                let partitioned_table = lowered
                    .partition_spec
                    .as_ref()
                    .map(|spec| pg_partitioned_table_row(created.entry.relation_oid, spec, 0));
                let relation =
                    if lowered.partition_parent_oid.is_some() || lowered.partition_spec.is_some() {
                        let relation = self.replace_relation_partition_metadata_in_transaction(
                            client_id,
                            created.entry.relation_oid,
                            lowered.partition_parent_oid.is_some(),
                            relpartbound,
                            partitioned_table,
                            xid,
                            table_cid.saturating_add(2),
                            configured_search_path,
                            catalog_effects,
                        )?;
                        if lowered
                            .partition_bound
                            .as_ref()
                            .is_some_and(PartitionBoundSpec::is_default)
                            && let Some(parent_oid) = lowered.partition_parent_oid
                        {
                            self.update_partitioned_table_default_partition_in_transaction(
                                client_id,
                                parent_oid,
                                created.entry.relation_oid,
                                xid,
                                table_cid.saturating_add(3),
                                configured_search_path,
                                catalog_effects,
                            )?;
                        }
                        relation
                    } else {
                        crate::backend::parser::BoundRelation {
                            rel: created.entry.rel,
                            relation_oid: created.entry.relation_oid,
                            toast: created.toast.as_ref().map(|toast| {
                                crate::include::nodes::primnodes::ToastRelationRef {
                                    rel: toast.toast_entry.rel,
                                    relation_oid: toast.toast_entry.relation_oid,
                                }
                            }),
                            namespace_oid: created.entry.namespace_oid,
                            owner_oid: created.entry.owner_oid,
                            relpersistence: created.entry.relpersistence,
                            relkind: created.entry.relkind,
                            relispopulated: created.entry.relispopulated,
                            relispartition: created.entry.relispartition,
                            relpartbound: created.entry.relpartbound.clone(),
                            desc: created.entry.desc.clone(),
                            partitioned_table: created.entry.partitioned_table.clone(),
                        }
                    };
                let mut constraint_cid_base = table_cid.saturating_add(1);
                if !lowered.parent_oids.is_empty() {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(2));
                }
                if lowered.partition_spec.is_some() || lowered.partition_parent_oid.is_some() {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(3));
                }
                if lowered
                    .partition_bound
                    .as_ref()
                    .is_some_and(PartitionBoundSpec::is_default)
                {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(4));
                }
                self.install_create_table_constraints_in_transaction(
                    client_id,
                    xid,
                    constraint_cid_base,
                    &table_name,
                    &relation,
                    &lowered,
                    configured_search_path,
                    catalog_effects,
                )?;
                if let Some(parent_oid) = lowered.partition_parent_oid {
                    let next_cid = self
                        .reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
                            client_id,
                            xid,
                            constraint_cid_base.saturating_add(1),
                            parent_oid,
                            relation.relation_oid,
                            configured_search_path,
                            catalog_effects,
                        )?;
                    self.clone_parent_row_triggers_to_partition_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        parent_oid,
                        relation.relation_oid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_create_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (view_name, namespace_oid) = self.normalize_create_view_stmt_with_search_path(
            client_id,
            Some((xid, cid)),
            create_stmt,
            configured_search_path,
        )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let plan = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?.plan_tree;
        let desc = crate::backend::executor::RelationDesc {
            columns: plan
                .column_names()
                .into_iter()
                .zip(plan.columns())
                .map(|(name, column)| column_desc(name, column.sql_type, true))
                .collect(),
        };
        let mut referenced_relation_oids = std::collections::BTreeSet::new();
        collect_direct_relation_oids_from_select(
            &create_stmt.query,
            &catalog,
            &mut Vec::new(),
            &mut referenced_relation_oids,
        );
        let existing_relation = catalog
            .lookup_any_relation(&view_name)
            .filter(|relation| relation.namespace_oid == namespace_oid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let is_new_relation = existing_relation.is_none();
        let relation_oid = if let Some(existing_relation) = existing_relation {
            if !create_stmt.or_replace {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(view_name)));
            }
            if existing_relation.relkind != 'v' {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: create_stmt.view_name.clone(),
                    expected: "view",
                }));
            }
            if !existing_view_prefix_matches(&existing_relation.desc, &desc) {
                return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                    "CREATE OR REPLACE VIEW can only add new columns at the end of the view".into(),
                )));
            }
            let replace_effect = self
                .catalog
                .write()
                .alter_view_relation_desc_mvcc(existing_relation.relation_oid, desc, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(replace_effect);
            existing_relation.relation_oid
        } else {
            match create_stmt.persistence {
                TablePersistence::Permanent => {
                    let (entry, create_effect) = self
                        .catalog
                        .write()
                        .create_view_relation_mvcc(
                            view_name.clone(),
                            desc,
                            namespace_oid,
                            self.auth_state(client_id).current_user_oid(),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    catalog_effects.push(create_effect);
                    entry.relation_oid
                }
                TablePersistence::Temporary => {
                    let created = self.create_temp_relation_with_relkind_in_transaction(
                        client_id,
                        view_name.clone(),
                        desc,
                        OnCommitAction::PreserveRows,
                        xid,
                        cid,
                        'v',
                        catalog_effects,
                        temp_effects,
                    )?;
                    created.entry.relation_oid
                }
            }
        };

        let rule_drop_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
            interrupts: Arc::clone(&ctx.interrupts),
        };
        if create_stmt.or_replace
            && let Some(rewrite_oid) = catalog
                .rewrite_rows_for_relation(relation_oid)
                .into_iter()
                .find(|row| row.rulename == "_RETURN")
                .map(|row| row.oid)
        {
            let drop_effect = self
                .catalog
                .write()
                .drop_rule_mvcc(rewrite_oid, &rule_drop_ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(drop_effect);
        }
        let rule_ctx = CatalogWriteContext {
            cid: cid.saturating_add(2),
            ..rule_drop_ctx
        };
        let rule_effect = self
            .catalog
            .write()
            .create_rule_mvcc_with_owner_dependency(
                relation_oid,
                "_RETURN",
                '1',
                true,
                String::new(),
                create_stmt.query_sql.clone(),
                &referenced_relation_oids.into_iter().collect::<Vec<_>>(),
                crate::backend::catalog::store::RuleOwnerDependency::Internal,
                &rule_ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(rule_effect);
        if is_new_relation {
            // :HACK: CREATE VIEW reserves an intermediate command id between creating
            // the relation row and publishing the _RETURN rule. Session command-end
            // bookkeeping advances by catalog effect count, so pad the effect list to
            // keep the next statement's cid beyond the reserved internal cids.
            catalog_effects.push(CatalogMutationEffect::default());
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_as_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if create_stmt.object_type
            == crate::include::nodes::parsenodes::TableAsObjectType::MaterializedView
        {
            return self.execute_create_materialized_view_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
            );
        }
        let interrupts = self.interrupt_state(client_id);
        let (table_name, namespace_oid, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let planned_stmt = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?;
        let mut rels = std::collections::BTreeSet::new();
        collect_rels_from_planned_stmt(&planned_stmt, &mut rels);

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: false,
            pending_async_notifications: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        let query_result = execute_readonly_statement(
            Statement::Select(create_stmt.query.clone()),
            &catalog,
            &mut ctx,
        );
        let StatementResult::Query {
            columns,
            column_names,
            rows,
        } = query_result?
        else {
            unreachable!("ctas query should return rows");
        };

        let desc = crate::backend::executor::RelationDesc {
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let name = create_stmt
                        .column_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| column_names[index].clone());
                    column_desc(name, column.sql_type, true)
                })
                .collect(),
        };

        let (relation_oid, rel, toast, toast_index) = match persistence {
            TablePersistence::Permanent => {
                let stmt = CreateTableStatement {
                    schema_name: None,
                    table_name: table_name.clone(),
                    persistence,
                    on_commit: create_stmt.on_commit,
                    elements: desc
                        .columns
                        .iter()
                        .map(|column| {
                            crate::backend::parser::CreateTableElement::Column(
                                crate::backend::parser::ColumnDef {
                                    name: column.name.clone(),
                                    ty: crate::backend::parser::RawTypeName::Builtin(
                                        column.sql_type,
                                    ),
                                    collation: None,
                                    default_expr: None,
                                    generated: None,
                                    identity: None,
                                    storage: None,
                                    compression: None,
                                    constraints: vec![],
                                },
                            )
                        })
                        .collect(),
                    inherits: Vec::new(),
                    partition_spec: None,
                    partition_of: None,
                    partition_bound: None,
                    if_not_exists: create_stmt.if_not_exists,
                };
                let mut catalog_guard = self.catalog.write();
                let write_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (created, effect) = catalog_guard
                    .create_table_mvcc_with_options(
                        table_name.clone(),
                        create_relation_desc(&stmt, &catalog)?,
                        namespace_oid,
                        self.database_oid,
                        'p',
                        crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                        self.auth_state(client_id).current_user_oid(),
                        &write_ctx,
                    )
                    .map_err(map_catalog_error)?;
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let (toast, toast_index) = toast_bindings_from_create_result(&created);
                (
                    created.entry.relation_oid,
                    created.entry.rel,
                    toast,
                    toast_index,
                )
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?;
                let (toast, toast_index) = toast_bindings_from_temp_relation(&created);
                (
                    created.entry.relation_oid,
                    created.entry.rel,
                    toast,
                    toast_index,
                )
            }
        };
        if rows.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut insert_ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        let inserted = crate::backend::commands::tablecmds::execute_insert_values(
            &table_name,
            relation_oid,
            rel,
            toast,
            toast_index.as_ref(),
            &desc,
            &crate::backend::parser::BoundRelationConstraints::default(),
            &[],
            &[],
            &rows,
            &mut insert_ctx,
            xid,
            cid,
        )?;
        Ok(StatementResult::AffectedRows(inserted))
    }

    pub(crate) fn execute_create_table_as_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            return self.execute_create_table_as_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
                &mut temp_effects,
            );
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_as_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }
}
