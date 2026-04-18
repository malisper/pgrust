use super::super::*;
use crate::backend::commands::tablecmds::{collect_matching_rows_heap, index_key_values_for_row};
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::access::amapi::{IndexBuildEmptyContext, IndexInsertContext, IndexUniqueCheck};
use crate::include::catalog::range_type_ref_for_sql_type;
use std::collections::BTreeSet;

impl Database {
    fn relcache_index_meta_from_catalog(
        meta: &crate::backend::catalog::CatalogIndexMeta,
        am_oid: u32,
        am_handler_oid: u32,
    ) -> crate::backend::utils::cache::relcache::IndexRelCacheEntry {
        crate::backend::utils::cache::relcache::IndexRelCacheEntry {
            indrelid: meta.indrelid,
            indnatts: meta.indkey.len() as i16,
            indnkeyatts: meta.indkey.len() as i16,
            indisunique: meta.indisunique,
            indnullsnotdistinct: false,
            indisprimary: meta.indisprimary,
            indisexclusion: false,
            indimmediate: false,
            indisclustered: false,
            indisvalid: meta.indisvalid,
            indcheckxmin: false,
            indisready: meta.indisready,
            indislive: meta.indislive,
            indisreplident: false,
            am_oid,
            am_handler_oid: Some(am_handler_oid),
            indkey: meta.indkey.clone(),
            indclass: meta.indclass.clone(),
            indcollation: meta.indcollation.clone(),
            indoption: meta.indoption.clone(),
            opfamily_oids: Vec::new(),
            opcintype_oids: Vec::new(),
            indexprs: meta.indexprs.clone(),
            indpred: meta.indpred.clone(),
        }
    }

    fn default_index_base_name(
        relation_name: &str,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> String {
        let first_column = columns
            .first()
            .map(|column| {
                if column.expr_sql.is_some() {
                    "expr"
                } else {
                    column.name.as_str()
                }
            })
            .unwrap_or("idx");
        format!("{relation_name}_{first_column}_idx")
    }

    pub(super) fn resolve_simple_btree_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self, client_id, txn_ctx, "btree",
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !access_method.amname.eq_ignore_ascii_case("btree") {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            }));
        }

        let type_rows =
            crate::backend::utils::cache::syscache::ensure_type_rows(self, client_id, txn_ctx);
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column in columns {
            let sql_type = if column.expr_sql.is_some() {
                column.expr_type.ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "inferred expression index type",
                        actual: "missing expression index type".into(),
                    })
                })?
            } else {
                relation
                    .desc
                    .columns
                    .iter()
                    .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(column.name.clone()))
                    })?
                    .sql_type
            };
            let type_oid = range_type_ref_for_sql_type(sql_type)
                .map(|range_type| range_type.type_oid())
                .or_else(|| {
                    type_rows
                        .iter()
                        .find(|row| row.sql_type == sql_type)
                        .map(|row| row.oid)
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(
                        column
                            .expr_sql
                            .clone()
                            .unwrap_or_else(|| column.name.clone()),
                    ))
                })?;
            let opclass = crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
                self,
                client_id,
                txn_ctx,
                access_method.oid,
                type_oid,
            )
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnsupportedType(
                    column
                        .expr_sql
                        .clone()
                        .unwrap_or_else(|| column.name.clone()),
                ))
            })?;
            indclass.push(opclass.oid);
            indcollation.push(0);
            let mut option = 0i16;
            if column.descending {
                option |= 0x0001;
            }
            if column.nulls_first.unwrap_or(false) {
                option |= 0x0002;
            }
            indoption.push(option);
        }

        Ok((
            access_method.oid,
            access_method.amhandler,
            CatalogIndexBuildOptions {
                am_oid: access_method.oid,
                indclass,
                indcollation,
                indoption,
            },
        ))
    }

    pub(super) fn build_simple_btree_index_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        index_name: &str,
        visible_catalog: Option<crate::backend::utils::cache::visible_catalog::VisibleCatalog>,
        columns: &[crate::backend::parser::IndexColumnDef],
        unique: bool,
        primary: bool,
        xid: TransactionId,
        cid: CommandId,
        access_method_oid: u32,
        access_method_handler: u32,
        build_options: &CatalogIndexBuildOptions,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::catalog::CatalogEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let (index_entry, effect) = catalog_guard
            .create_index_for_relation_mvcc_with_options(
                index_name.to_string(),
                relation.relation_oid,
                unique,
                primary,
                columns,
                build_options,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        if index_entry
            .index_meta
            .as_ref()
            .and_then(|meta| meta.indexprs.as_ref())
            .is_some()
        {
            self.build_expression_index_rows_in_transaction(
                client_id,
                relation,
                &index_entry,
                index_name,
                visible_catalog,
                xid,
                cid,
                access_method_handler,
                maintenance_work_mem_kb,
            )?;
            let mut catalog_guard = self.catalog.write();
            let readiness_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(1),
                client_id,
                waiter: None,
                interrupts,
            };
            let ready_effect = catalog_guard
                .set_index_ready_valid_mvcc(index_entry.relation_oid, true, true, &readiness_ctx)
                .map_err(|err| match err {
                    CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                    _ => ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index catalog readiness update",
                        actual: "index readiness update failed".into(),
                    }),
                })?;
            drop(catalog_guard);
            self.apply_catalog_mutation_effect_immediate(&ready_effect)?;
            catalog_effects.push(ready_effect);
            return Ok(index_entry);
        }

        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index build snapshot",
                    actual: "snapshot creation failed".into(),
                })
            })?;
        let index_meta = index_entry.index_meta.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index metadata",
                actual: "missing index metadata".into(),
            })
        })?;
        let build_ctx = crate::include::access::amapi::IndexBuildContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            client_id,
            interrupts: Arc::clone(&interrupts),
            snapshot,
            heap_relation: relation.rel,
            heap_desc: relation.desc.clone(),
            index_relation: index_entry.rel,
            index_name: index_name.to_string(),
            index_desc: index_entry.desc.clone(),
            index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry {
                indrelid: index_meta.indrelid,
                indnatts: index_meta.indkey.len() as i16,
                indnkeyatts: index_meta.indkey.len() as i16,
                indisunique: index_meta.indisunique,
                indnullsnotdistinct: false,
                indisprimary: index_meta.indisprimary,
                indisexclusion: false,
                indimmediate: false,
                indisclustered: false,
                indisvalid: index_meta.indisvalid,
                indcheckxmin: false,
                indisready: index_meta.indisready,
                indislive: index_meta.indislive,
                indisreplident: false,
                am_oid: access_method_oid,
                am_handler_oid: Some(access_method_handler),
                indkey: index_meta.indkey.clone(),
                indclass: index_meta.indclass.clone(),
                indcollation: index_meta.indcollation.clone(),
                indoption: index_meta.indoption.clone(),
                opfamily_oids: Vec::new(),
                opcintype_oids: Vec::new(),
                indexprs: index_meta.indexprs.clone(),
                indpred: index_meta.indpred.clone(),
            },
            maintenance_work_mem_kb,
        };
        crate::backend::access::index::indexam::index_build_stub(&build_ctx, access_method_oid)
            .map_err(|err| match err {
                CatalogError::UniqueViolation(constraint) => {
                    ExecError::UniqueViolation { constraint }
                }
                CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method build",
                    actual: "index build failed".into(),
                }),
            })?;

        let mut catalog_guard = self.catalog.write();
        let readiness_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
            interrupts,
        };
        let ready_effect = catalog_guard
            .set_index_ready_valid_mvcc(index_entry.relation_oid, true, true, &readiness_ctx)
            .map_err(|err| match err {
                CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index catalog readiness update",
                    actual: "index readiness update failed".into(),
                }),
            })?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&ready_effect)?;
        catalog_effects.push(ready_effect);
        Ok(index_entry)
    }

    fn build_expression_index_rows_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        index_entry: &crate::backend::catalog::CatalogEntry,
        index_name: &str,
        visible_catalog: Option<crate::backend::utils::cache::visible_catalog::VisibleCatalog>,
        xid: TransactionId,
        cid: CommandId,
        access_method_handler: u32,
        maintenance_work_mem_kb: usize,
    ) -> Result<(), ExecError> {
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
            let interrupts = self.interrupt_state(client_id);
            crate::backend::access::index::indexam::index_build_empty_stub(
                &IndexBuildEmptyContext {
                    pool: self.pool.clone(),
                    client_id,
                    xid,
                    index_relation: index_entry.rel,
                },
                crate::include::catalog::BTREE_AM_OID,
            )
            .map_err(map_catalog_error)?;

            let mut ctx = ExecutorContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                checkpoint_stats: CheckpointStatsSnapshot::default(),
                datetime_config: DateTimeConfig::default(),
                interrupts,
                snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
                client_id,
                next_command_id: cid,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                catalog: visible_catalog,
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
            };
            let rows = collect_matching_rows_heap(
                relation.rel,
                &relation.desc,
                relation.toast,
                None,
                &mut ctx,
            )?;
            let catalog_index_meta = index_entry.index_meta.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index metadata",
                    actual: "missing index metadata".into(),
                })
            })?;
            let bound_index = crate::backend::parser::BoundIndexRelation {
                name: index_name.to_string(),
                rel: index_entry.rel,
                relation_oid: index_entry.relation_oid,
                desc: index_entry.desc.clone(),
                index_meta: Self::relcache_index_meta_from_catalog(
                    &catalog_index_meta,
                    crate::include::catalog::BTREE_AM_OID,
                    access_method_handler,
                ),
                index_exprs: crate::backend::parser::bind_index_exprs(
                    &Self::relcache_index_meta_from_catalog(
                        &catalog_index_meta,
                        crate::include::catalog::BTREE_AM_OID,
                        access_method_handler,
                    ),
                    &relation.desc,
                    &ctx.catalog
                        .clone()
                        .expect("visible catalog for expression index build"),
                )
                .map_err(ExecError::Parse)?,
            };
            let mut index_meta = bound_index.index_meta.clone();
            index_meta.indkey = (1..=index_meta.indkey.len())
                .map(|attnum| attnum as i16)
                .collect::<Vec<_>>();
            index_meta.indexprs = None;
            for (heap_tid, values) in rows {
                let key_values =
                    index_key_values_for_row(&bound_index, &relation.desc, &values, &mut ctx)?;
                crate::backend::access::index::indexam::index_insert_stub(
                    &IndexInsertContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        txn_waiter: Some(self.txn_waiter.clone()),
                        client_id,
                        interrupts: self.interrupt_state(client_id),
                        snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
                        heap_relation: relation.rel,
                        heap_desc: index_entry.desc.clone(),
                        index_relation: index_entry.rel,
                        index_name: index_name.to_string(),
                        index_desc: index_entry.desc.clone(),
                        index_meta: index_meta.clone(),
                        heap_tid,
                        values: key_values,
                        unique_check: if index_meta.indisunique {
                            IndexUniqueCheck::Yes
                        } else {
                            IndexUniqueCheck::No
                        },
                    },
                    crate::include::catalog::BTREE_AM_OID,
                )
                .map_err(|err| match err {
                    CatalogError::UniqueViolation(constraint) => {
                        ExecError::UniqueViolation { constraint }
                    }
                    _ => map_catalog_error(err),
                })?;
            }
            let _ = maintenance_work_mem_kb;
            Ok(())
        })
    }

    pub(super) fn choose_available_relation_name(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        namespace_oid: u32,
        base: &str,
    ) -> Result<String, ExecError> {
        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "constraint name lookup snapshot",
                    actual: "snapshot creation failed".into(),
                })
            })?;
        let catalog = self.catalog.read();
        let txns = self.txns.read();
        let existing = crate::backend::catalog::loader::load_visible_class_rows(
            catalog.base_dir(),
            &self.pool,
            &txns,
            &snapshot,
            client_id,
        )
        .map_err(map_catalog_error)?
        .into_iter()
        .filter(|row| row.relnamespace == namespace_oid)
        .map(|row| row.relname.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
        if !existing.contains(&base.to_ascii_lowercase()) {
            return Ok(base.to_string());
        }
        for suffix in 1.. {
            let candidate = format!("{base}{suffix}");
            if !existing.contains(&candidate.to_ascii_lowercase()) {
                return Ok(candidate);
            }
        }
        unreachable!("numeric suffix search should always find a free index name")
    }

    pub(crate) fn execute_create_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_index_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            maintenance_work_mem_kb,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let entry = catalog
            .lookup_any_relation(&create_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    create_stmt.table_name.clone(),
                ))
            })?;

        if entry.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for CREATE INDEX",
                actual: "temporary table".into(),
            }));
        }
        if entry.relkind != 'r' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: create_stmt.table_name.clone(),
                expected: "table",
            }));
        }
        ensure_relation_owner(self, client_id, &entry, &create_stmt.table_name)?;
        if create_stmt
            .using_method
            .as_deref()
            .is_some_and(|method| !method.eq_ignore_ascii_case("btree"))
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            }));
        }
        if !create_stmt.include_columns.is_empty()
            || !create_stmt.options.is_empty()
            || create_stmt.predicate.is_some()
            || create_stmt
                .columns
                .iter()
                .any(|column| column.descending || column.nulls_first.is_some())
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "simple btree column index",
                actual: "unsupported CREATE INDEX feature".into(),
            }));
        }
        let mut index_columns = create_stmt.columns.clone();
        for column in &mut index_columns {
            if let Some(expr_sql) = column.expr_sql.as_deref() {
                column.expr_type = Some(
                    crate::backend::parser::infer_relation_expr_sql_type(
                        expr_sql,
                        Some(
                            create_stmt
                                .table_name
                                .rsplit('.')
                                .next()
                                .unwrap_or(&create_stmt.table_name),
                        ),
                        &entry.desc,
                        &catalog,
                    )
                    .map_err(ExecError::Parse)?,
                );
            }
        }
        let (access_method_oid, access_method_handler, build_options) = self
            .resolve_simple_btree_build_options(
                client_id,
                Some((xid, cid)),
                &entry,
                &index_columns,
            )?;
        let index_name = if create_stmt.index_name.is_empty() {
            self.choose_available_relation_name(
                client_id,
                xid,
                cid,
                entry.namespace_oid,
                &Self::default_index_base_name(&create_stmt.table_name, &index_columns),
            )?
        } else {
            create_stmt.index_name.clone()
        };
        match self.build_simple_btree_index_in_transaction(
            client_id,
            &entry,
            &index_name,
            catalog.materialize_visible_catalog(),
            &index_columns,
            create_stmt.unique,
            false,
            xid,
            cid,
            access_method_oid,
            access_method_handler,
            &build_options,
            maintenance_work_mem_kb,
            catalog_effects,
        ) {
            Ok(_) => {}
            Err(ExecError::Parse(ParseError::TableAlreadyExists(_)))
                if create_stmt.if_not_exists =>
            {
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(err) => return Err(err),
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
