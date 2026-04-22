use super::super::*;
use crate::backend::commands::tablecmds::{collect_matching_rows_heap, index_key_values_for_row};
use crate::backend::utils::cache::relcache::{IndexAmOpEntry, IndexAmProcEntry};
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::access::amapi::{
    IndexBuildEmptyContext, IndexBuildExprContext, IndexInsertContext, IndexUniqueCheck,
};
use crate::include::catalog::{
    GIST_AM_OID, GIST_RANGE_FAMILY_OID, builtin_range_rows, range_type_ref_for_sql_type,
};
use std::collections::BTreeSet;

struct ResolvedIndexSupportMetadata {
    opfamily_oids: Vec<u32>,
    opcintype_oids: Vec<u32>,
    opckeytype_oids: Vec<u32>,
    amop_entries: Vec<Vec<IndexAmOpEntry>>,
    amproc_entries: Vec<Vec<IndexAmProcEntry>>,
}

impl Database {
    fn relcache_index_meta_from_catalog(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        indexrelid: u32,
        meta: &crate::backend::catalog::CatalogIndexMeta,
        am_oid: u32,
        am_handler_oid: u32,
    ) -> Result<crate::backend::utils::cache::relcache::IndexRelCacheEntry, ExecError> {
        let support = self.resolve_index_support_metadata(client_id, txn_ctx, &meta.indclass)?;
        Ok(crate::backend::utils::cache::relcache::IndexRelCacheEntry {
            indexrelid,
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
            opfamily_oids: support.opfamily_oids,
            opcintype_oids: support.opcintype_oids,
            opckeytype_oids: support.opckeytype_oids,
            amop_entries: support.amop_entries,
            amproc_entries: support.amproc_entries,
            indexprs: meta.indexprs.clone(),
            indpred: meta.indpred.clone(),
        })
    }

    fn resolve_index_support_metadata(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        indclass: &[u32],
    ) -> Result<ResolvedIndexSupportMetadata, ExecError> {
        let opclass_rows =
            crate::backend::utils::cache::syscache::ensure_opclass_rows(self, client_id, txn_ctx);
        let amop_rows =
            crate::backend::utils::cache::syscache::ensure_amop_rows(self, client_id, txn_ctx);
        let amproc_rows =
            crate::backend::utils::cache::syscache::ensure_amproc_rows(self, client_id, txn_ctx);
        let operator_rows = crate::include::catalog::bootstrap_pg_operator_rows();

        let resolved_opclasses = indclass
            .iter()
            .map(|oid| {
                opclass_rows
                    .iter()
                    .find(|row| row.oid == *oid)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "valid index operator class",
                            actual: format!("unknown operator class oid {oid}"),
                        })
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let opfamily_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcfamily)
            .collect::<Vec<_>>();
        let opcintype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcintype)
            .collect::<Vec<_>>();
        let opckeytype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opckeytype)
            .collect::<Vec<_>>();
        let amop_entries = opfamily_oids
            .iter()
            .map(|family_oid| {
                amop_rows
                    .iter()
                    .filter(|row| row.amopfamily == *family_oid)
                    .map(|row| IndexAmOpEntry {
                        strategy: row.amopstrategy,
                        purpose: row.amoppurpose,
                        lefttype: row.amoplefttype,
                        righttype: row.amoprighttype,
                        operator_oid: row.amopopr,
                        operator_proc_oid: operator_rows
                            .iter()
                            .find(|operator| operator.oid == row.amopopr)
                            .map(|operator| operator.oprcode)
                            .unwrap_or(0),
                        sortfamily_oid: row.amopsortfamily,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let amproc_entries = opfamily_oids
            .iter()
            .map(|family_oid| {
                amproc_rows
                    .iter()
                    .filter(|row| row.amprocfamily == *family_oid)
                    .map(|row| IndexAmProcEntry {
                        procnum: row.amprocnum,
                        lefttype: row.amproclefttype,
                        righttype: row.amprocrighttype,
                        proc_oid: row.amproc,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(ResolvedIndexSupportMetadata {
            opfamily_oids,
            opcintype_oids,
            opckeytype_oids,
            amop_entries,
            amproc_entries,
        })
    }

    fn default_index_base_name(
        relation_name: &str,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> String {
        let column_part = columns
            .iter()
            .map(|column| {
                if column.expr_sql.is_some() {
                    "expr"
                } else {
                    column.name.as_str()
                }
            })
            .collect::<Vec<_>>()
            .join("_");
        let column_part = if column_part.is_empty() {
            "idx".to_string()
        } else {
            column_part
        };
        format!("{relation_name}_{column_part}_idx")
    }

    pub(super) fn resolve_simple_index_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        access_method_name: &str,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self,
            client_id,
            txn_ctx,
            access_method_name,
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !access_method
            .amname
            .eq_ignore_ascii_case(access_method_name)
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            }));
        }

        let type_rows =
            crate::backend::utils::cache::syscache::ensure_type_rows(self, client_id, txn_ctx);
        let opclass_rows =
            crate::backend::utils::cache::syscache::ensure_opclass_rows(self, client_id, txn_ctx);
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
                        .or_else(|| {
                            type_rows
                                .iter()
                                .find(|row| {
                                    row.sql_type.kind == sql_type.kind
                                        && row.sql_type.is_array == sql_type.is_array
                                        && row.typrelid == 0
                                })
                                .map(|row| row.oid)
                        })
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(
                        column
                            .expr_sql
                            .clone()
                            .unwrap_or_else(|| column.name.clone()),
                    ))
                })?;
            let opclass = if let Some(opclass_name) = column.opclass.as_deref() {
                let is_range_type = builtin_range_rows()
                    .iter()
                    .any(|row| row.rngtypid == type_oid);
                opclass_rows
                    .iter()
                    .find(|row| {
                        row.opcmethod == access_method.oid
                            && row.opcname.eq_ignore_ascii_case(opclass_name)
                            && (row.opcintype == type_oid
                                || (is_range_type && row.opcfamily == GIST_RANGE_FAMILY_OID))
                    })
                    .cloned()
            } else {
                crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
                    self,
                    client_id,
                    txn_ctx,
                    access_method.oid,
                    type_oid,
                )
            }
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

    pub(super) fn build_simple_index_in_transaction(
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

        let has_expression_keys = index_entry
            .index_meta
            .as_ref()
            .and_then(|meta| meta.indexprs.as_ref())
            .is_some();
        if has_expression_keys && access_method_oid != GIST_AM_OID {
            self.build_expression_index_rows_in_transaction(
                client_id,
                relation,
                &index_entry,
                index_name,
                visible_catalog,
                xid,
                cid,
                access_method_oid,
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
        let current_xid = snapshot.current_xid;
        let index_meta = index_entry.index_meta.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index metadata",
                actual: "missing index metadata".into(),
            })
        })?;
        let relcache_index_meta = self.relcache_index_meta_from_catalog(
            client_id,
            Some((xid, cid)),
            index_entry.relation_oid,
            &index_meta,
            access_method_oid,
            access_method_handler,
        )?;
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
            index_meta: relcache_index_meta,
            maintenance_work_mem_kb,
            expr_eval: has_expression_keys.then_some(IndexBuildExprContext {
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                datetime_config: DateTimeConfig::default(),
                stats: std::sync::Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                current_xid,
                statement_lock_scope_id: None,
                visible_catalog: visible_catalog.clone(),
            }),
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
        if relation.relpersistence == 't' {
            let mut temp_index_meta = index_meta.clone();
            temp_index_meta.indisready = true;
            temp_index_meta.indisvalid = true;
            self.install_temp_entry(
                client_id,
                index_name,
                crate::backend::utils::cache::relcache::RelCacheEntry {
                    rel: index_entry.rel,
                    relation_oid: index_entry.relation_oid,
                    namespace_oid: index_entry.namespace_oid,
                    owner_oid: index_entry.owner_oid,
                    row_type_oid: index_entry.row_type_oid,
                    array_type_oid: index_entry.array_type_oid,
                    reltoastrelid: index_entry.reltoastrelid,
                    relpersistence: index_entry.relpersistence,
                    relkind: index_entry.relkind,
                    relhastriggers: index_entry.relhastriggers,
                    relispartition: index_entry.relispartition,
                    relpartbound: index_entry.relpartbound.clone(),
                    relrowsecurity: index_entry.relrowsecurity,
                    relforcerowsecurity: index_entry.relforcerowsecurity,
                    desc: index_entry.desc.clone(),
                    partitioned_table: index_entry.partitioned_table.clone(),
                    index: Some(self.relcache_index_meta_from_catalog(
                        client_id,
                        Some((xid, cid)),
                        index_entry.relation_oid,
                        &temp_index_meta,
                        access_method_oid,
                        access_method_handler,
                    )?),
                },
                self.temp_entry_on_commit(client_id, relation.relation_oid)
                    .unwrap_or(OnCommitAction::PreserveRows),
            )?;
        }
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
        access_method_oid: u32,
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
                access_method_oid,
            )
            .map_err(map_catalog_error)?;

            let mut ctx = ExecutorContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                advisory_locks: Arc::clone(&self.advisory_locks),
                checkpoint_stats: CheckpointStatsSnapshot::default(),
                datetime_config: DateTimeConfig::default(),
                interrupts,
                stats: std::sync::Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
                client_id,
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                active_role_oid: self.auth_state(client_id).active_role_oid(),
                statement_lock_scope_id: None,
                next_command_id: cid,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
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
                deferred_foreign_keys: None,
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
            let relcache_index_meta = self.relcache_index_meta_from_catalog(
                client_id,
                Some((xid, cid)),
                index_entry.relation_oid,
                &catalog_index_meta,
                access_method_oid,
                access_method_handler,
            )?;
            let bound_index = crate::backend::parser::BoundIndexRelation {
                name: index_name.to_string(),
                rel: index_entry.rel,
                relation_oid: index_entry.relation_oid,
                desc: index_entry.desc.clone(),
                index_meta: relcache_index_meta.clone(),
                index_exprs: crate::backend::parser::bind_index_exprs(
                    &relcache_index_meta,
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
                        heap_desc: relation.desc.clone(),
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
                    access_method_oid,
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

        if entry.relkind != 'r' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: create_stmt.table_name.clone(),
                expected: "table",
            }));
        }
        ensure_relation_owner(self, client_id, &entry, &create_stmt.table_name)?;
        if !create_stmt.include_columns.is_empty()
            || !create_stmt.options.is_empty()
            || create_stmt.predicate.is_some()
            || create_stmt
                .columns
                .iter()
                .any(|column| column.descending || column.nulls_first.is_some())
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "simple index definition",
                actual: "unsupported CREATE INDEX feature".into(),
            }));
        }
        let access_method_name = create_stmt.using_method.as_deref().unwrap_or("btree");
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
            .resolve_simple_index_build_options(
                client_id,
                Some((xid, cid)),
                access_method_name,
                &entry,
                &index_columns,
            )?;
        let am_routine = crate::backend::access::index::amapi::index_am_handler(access_method_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "supported index access method",
                    actual: format!("unknown access method oid {access_method_oid}"),
                })
            })?;
        if create_stmt.unique && !am_routine.amcanunique {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "access method \"{}\" does not support unique indexes",
                access_method_name
            ))));
        }
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
        match self.build_simple_index_in_transaction(
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
