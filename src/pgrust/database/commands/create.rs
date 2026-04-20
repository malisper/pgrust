use super::super::*;
use crate::backend::parser::{
    CreateFunctionReturnSpec, CreateFunctionStatement, FunctionArgMode, FunctionParallel,
    FunctionVolatility, OwnedSequenceSpec, SequenceOptionsSpec, SqlTypeKind, resolve_raw_type_name,
};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_PLPGSQL_OID,
    PG_LANGUAGE_SQL_OID, PUBLIC_NAMESPACE_OID, PgProcRow, RECORD_TYPE_OID,
};
use crate::include::nodes::parsenodes::{ForeignKeyAction, ForeignKeyMatchType};
use crate::include::nodes::primnodes::{QueryColumn, ToastRelationRef};
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

fn normalize_create_function_name_for_search_path(
    stmt: &CreateFunctionStatement,
    configured_search_path: Option<&[String]>,
) -> Result<(String, u32), ParseError> {
    let normalized = stmt.function_name.to_ascii_lowercase();
    match stmt.schema_name.as_deref().map(str::to_ascii_lowercase) {
        Some(schema) if schema == "public" => Ok((normalized, PUBLIC_NAMESPACE_OID)),
        Some(schema) if schema == "pg_catalog" => Ok((normalized, PG_CATALOG_NAMESPACE_OID)),
        Some(schema) if schema == "pg_temp" => Err(ParseError::UnexpectedToken {
            expected: "permanent function",
            actual: "temporary function".into(),
        }),
        Some(schema) => Err(ParseError::UnsupportedQualifiedName(format!(
            "{schema}.{}",
            stmt.function_name
        ))),
        None => {
            let search_path = configured_search_path
                .map(|path| {
                    path.iter()
                        .map(|s| s.trim().to_ascii_lowercase())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec!["public".into()]);
            for schema in search_path {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" => continue,
                    "pg_catalog" => continue,
                    "public" => return Ok((normalized, PUBLIC_NAMESPACE_OID)),
                    _ => continue,
                }
            }
            Err(ParseError::NoSchemaSelectedForCreate)
        }
    }
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
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
        for (index, action) in lowered.constraint_actions.iter().enumerate() {
            let action_cid = table_cid
                .saturating_add(1)
                .saturating_add((index as u32).saturating_mul(3));
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
            let build_options = self.resolve_simple_btree_build_options(
                client_id,
                Some((xid, action_cid)),
                relation,
                &index_columns,
            )?;
            let index_entry = self.build_simple_btree_index_in_transaction(
                client_id,
                relation,
                &index_name,
                catalog.materialize_visible_catalog(),
                &index_columns,
                true,
                action.primary,
                xid,
                action_cid,
                build_options.0,
                build_options.1,
                &build_options.2,
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
            let constraint_effect = self
                .catalog
                .write()
                .create_index_backed_constraint_mvcc(
                    relation.relation_oid,
                    index_entry.relation_oid,
                    constraint_name,
                    if action.primary {
                        crate::include::catalog::CONSTRAINT_PRIMARY
                    } else {
                        crate::include::catalog::CONSTRAINT_UNIQUE
                    },
                    &primary_key_owned_not_null_oids,
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
        }

        let check_base_cid = table_cid
            .saturating_add(1)
            .saturating_add((lowered.constraint_actions.len() as u32).saturating_mul(3));
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
                    !action.not_valid,
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
                    !action.not_valid,
                    &local_attnums,
                    referenced_relation.relation_oid,
                    referenced_index_oid,
                    &referenced_attnums,
                    foreign_key_action_code(action.on_update),
                    foreign_key_action_code(action.on_delete),
                    foreign_key_match_code(action.match_type),
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
        }

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
        let (function_name, namespace_oid) =
            normalize_create_function_name_for_search_path(create_stmt, configured_search_path)?;

        let language_row = catalog
            .language_row_by_name(&create_stmt.language)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "LANGUAGE plpgsql or sql",
                    actual: format!("LANGUAGE {}", create_stmt.language),
                })
            })?;
        if !matches!(
            language_row.oid,
            PG_LANGUAGE_PLPGSQL_OID | PG_LANGUAGE_SQL_OID
        ) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "LANGUAGE plpgsql or sql",
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
            if matches!(sql_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "record and composite function arguments are not supported yet".into(),
                )));
            }
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
        let mut prorettype = 0u32;
        let mut proallargtypes = None;
        let mut proargmodes = None;
        let mut proargnames = all_arg_names
            .iter()
            .any(|name| !name.is_empty())
            .then_some(all_arg_names.clone());

        match &create_stmt.return_spec {
            CreateFunctionReturnSpec::Type { ty, setof } => {
                let sql_type = resolve_raw_type_name(ty, &catalog).map_err(ExecError::Parse)?;
                if matches!(sql_type.kind, SqlTypeKind::Record) && !setof {
                    return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                        "non-set RETURNS record is not supported yet".into(),
                    )));
                }
                if matches!(sql_type.kind, SqlTypeKind::Composite) && !setof {
                    return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                        "non-set RETURNS named composite is not supported yet".into(),
                    )));
                }
                if !output_args.is_empty() {
                    return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                        "explicit RETURNS with OUT/INOUT arguments is not supported unless RETURNS SETOF record".into(),
                    )));
                }
                proretset = *setof;
                prorettype = if matches!(sql_type.kind, SqlTypeKind::Record) {
                    RECORD_TYPE_OID
                } else {
                    catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnsupportedType(format!("{sql_type:?}")))
                    })?
                };
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
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnsupportedType(
                                output_args[0].name.clone(),
                            ))
                        })?;
                } else {
                    return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                        "multi-OUT non-set functions are not supported yet".into(),
                    )));
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
            procost: 100.0,
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
            proparallel: match create_stmt.parallel {
                FunctionParallel::Unsafe => 'u',
                FunctionParallel::Restricted => 'r',
                FunctionParallel::Safe => 's',
            },
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
        let result = self.execute_create_view_stmt_in_transaction_with_search_path(
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
                let result = catalog_guard.create_table_mvcc_with_options(
                    table_name.clone(),
                    desc.clone(),
                    namespace_oid,
                    self.database_oid,
                    'p',
                    crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                    crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                    self.auth_state(client_id).current_user_oid(),
                    &ctx,
                );
                match result {
                    Err(CatalogError::TableAlreadyExists(name)) if create_stmt.if_not_exists => {
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
                        let relation = crate::backend::parser::BoundRelation {
                            rel: created.entry.rel,
                            relation_oid: created.entry.relation_oid,
                            namespace_oid: created.entry.namespace_oid,
                            owner_oid: created.entry.owner_oid,
                            relpersistence: created.entry.relpersistence,
                            relkind: created.entry.relkind,
                            toast: created.toast.as_ref().map(|toast| ToastRelationRef {
                                rel: toast.toast_entry.rel,
                                relation_oid: toast.toast_entry.relation_oid,
                            }),
                            desc: created.entry.desc.clone(),
                        };
                        let constraint_cid_base =
                            table_cid.saturating_add(u32::from(!lowered.parent_oids.is_empty()));
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
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc,
                    create_stmt.on_commit,
                    xid,
                    table_cid,
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
                let relation = crate::backend::parser::BoundRelation {
                    rel: created.entry.rel,
                    relation_oid: created.entry.relation_oid,
                    namespace_oid: created.entry.namespace_oid,
                    owner_oid: created.entry.owner_oid,
                    relpersistence: created.entry.relpersistence,
                    relkind: created.entry.relkind,
                    toast: created.toast.as_ref().map(|toast| ToastRelationRef {
                        rel: toast.toast_entry.rel,
                        relation_oid: toast.toast_entry.relation_oid,
                    }),
                    desc: created.entry.desc.clone(),
                };
                let constraint_cid_base =
                    table_cid.saturating_add(u32::from(!lowered.parent_oids.is_empty()));
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
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
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

        let rule_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
            interrupts: Arc::clone(&ctx.interrupts),
        };
        let rule_effect = self
            .catalog
            .write()
            .create_rule_mvcc_with_owner_dependency(
                entry.relation_oid,
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
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            next_command_id: cid,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: false,
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
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
                                    default_expr: None,
                                    constraints: vec![],
                                },
                            )
                        })
                        .collect(),
                    inherits: Vec::new(),
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
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            next_command_id: cid,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
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
