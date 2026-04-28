use std::collections::{BTreeMap, BTreeSet};

use super::super::*;
use super::alter_table_work_queue::{
    AlterTableWorkItem, build_alter_table_work_queue, direct_inheritance_children,
    has_inheritance_children,
};
use super::typed_table::reject_typed_table_ddl;
use crate::backend::parser::{BoundRelation, bind_generated_expr, expr_references_column};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    is_system_column_name, lookup_table_or_partitioned_table_for_alter_table,
    reject_column_with_publication_dependencies, reject_column_with_rule_dependencies,
    reject_column_with_trigger_dependencies,
};

fn display_relation_name(catalog: &dyn CatalogLookup, relation: &BoundRelation) -> String {
    let relname = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let Some(schema) = catalog
        .namespace_row_by_oid(relation.namespace_oid)
        .map(|row| row.nspname)
    else {
        return relname;
    };
    if matches!(schema.as_str(), "public" | "pg_catalog")
        || catalog
            .search_path()
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(&schema))
    {
        relname
    } else {
        format!("{schema}.{relname}")
    }
}

fn generated_columns_referencing_any(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    source_indices: &BTreeSet<usize>,
) -> Result<Vec<usize>, ExecError> {
    let mut dependent_indices = Vec::new();
    for (generated_index, generated_column) in relation.desc.columns.iter().enumerate() {
        if source_indices.contains(&generated_index) || generated_column.generated.is_none() {
            continue;
        }
        let Some(expr) = bind_generated_expr(&relation.desc, generated_index, catalog)
            .map_err(ExecError::Parse)?
        else {
            continue;
        };
        if source_indices
            .iter()
            .any(|source_index| expr_references_column(&expr, *source_index))
        {
            dependent_indices.push(generated_index);
        }
    }
    Ok(dependent_indices)
}

fn generated_columns_to_drop_for_column(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    column_index: usize,
) -> Result<Vec<usize>, ExecError> {
    let mut source_indices = BTreeSet::from([column_index]);
    let mut dependent_indices = BTreeSet::new();
    loop {
        let mut changed = false;
        for dependent_index in
            generated_columns_referencing_any(catalog, relation, &source_indices)?
        {
            if dependent_indices.insert(dependent_index) {
                source_indices.insert(dependent_index);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(dependent_indices.into_iter().collect())
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn visible_column_index(relation: &BoundRelation, column_name: &str) -> Option<usize> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
}

fn cannot_drop_inherited_column_error(column_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot drop inherited column \"{column_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42P16",
    }
}

fn relation_column_by_name<'a>(
    relation: &'a BoundRelation,
    column_name: &str,
) -> Option<&'a crate::backend::executor::ColumnDesc> {
    visible_column_index(relation, column_name).map(|index| &relation.desc.columns[index])
}

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn foreign_key_column_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnum: i16,
) -> Vec<PgConstraintRow> {
    let mut dependencies = BTreeMap::new();
    for row in catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_FOREIGN)
    {
        if row
            .conkey
            .as_ref()
            .is_some_and(|attnums| attnums.contains(&attnum))
        {
            dependencies.insert(row.oid, row);
        }
    }
    for row in catalog.foreign_key_constraint_rows_referencing_relation(relation_oid) {
        if row
            .confkey
            .as_ref()
            .is_some_and(|attnums| attnums.contains(&attnum))
        {
            dependencies.insert(row.oid, row);
        }
    }
    let mut dependencies = dependencies.into_values().collect::<Vec<_>>();
    dependencies.sort_by(|left, right| {
        (
            relation_name_for_oid(catalog, left.conrelid),
            left.conname.clone(),
        )
            .cmp(&(
                relation_name_for_oid(catalog, right.conrelid),
                right.conname.clone(),
            ))
    });
    dependencies
}

fn foreign_key_dependency_detail(
    catalog: &dyn CatalogLookup,
    relation_display_name: &str,
    column_name: &str,
    row: &PgConstraintRow,
) -> String {
    format!(
        "constraint {} on table {} depends on column {} of table {}",
        row.conname,
        relation_name_for_oid(catalog, row.conrelid),
        column_name,
        relation_display_name
    )
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    fn drop_foreign_key_dependencies_for_column_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        catalog: &dyn CatalogLookup,
        dependencies: &[PgConstraintRow],
        interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let dependency_oids = dependencies
            .iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let mut next_cid = cid;
        for row in dependencies {
            if row.conparentid != 0 && dependency_oids.contains(&row.conparentid) {
                continue;
            }
            let constraint_relation = catalog.relation_by_oid(row.conrelid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(row.conrelid.to_string()))
            })?;
            push_notice(format!(
                "drop cascades to constraint {} on table {}",
                row.conname,
                relation_name_for_oid(catalog, row.conrelid)
            ));
            if constraint_relation.relkind == 'p' {
                next_cid = self.drop_partition_child_foreign_key_constraints_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    row,
                    catalog,
                    catalog_effects,
                )?;
            }
            let effects_before_triggers = catalog_effects.len();
            self.drop_foreign_key_triggers_in_transaction(
                client_id,
                xid,
                next_cid,
                row,
                catalog,
                catalog_effects,
            )?;
            next_cid = next_cid.saturating_add(
                catalog_effects
                    .len()
                    .saturating_sub(effects_before_triggers) as u32,
            );
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: None,
                interrupts: std::sync::Arc::clone(&interrupts),
            };
            let (_removed, effect) = self
                .catalog
                .write()
                .drop_relation_constraint_mvcc(row.conrelid, &row.conname, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        Ok(next_cid)
    }

    pub(crate) fn execute_alter_table_drop_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &AlterTableDropColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let lock_queue = build_alter_table_work_queue(&catalog, &relation, drop_stmt.only)?;
        let lock_requests = lock_queue
            .iter()
            .map(|item| (item.relation.rel, TableLockMode::AccessExclusive))
            .collect::<Vec<_>>();
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            self.interrupt_state(client_id).as_ref(),
        )?;
        let locked_rels =
            crate::pgrust::database::foreign_keys::table_lock_relations(&lock_requests);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for rel in locked_rels {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &AlterTableDropColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE DROP COLUMN",
                actual: "system catalog".into(),
            }));
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE DROP COLUMN",
                actual: "temporary table".into(),
            }));
        }
        reject_typed_table_ddl(&relation, "drop column from")?;
        ensure_relation_owner(self, client_id, &relation, &drop_stmt.table_name)?;
        if is_system_column_name(&drop_stmt.column_name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column name for DROP COLUMN",
                actual: drop_stmt.column_name.clone(),
            }));
        }
        let work_queue = build_alter_table_work_queue(&catalog, &relation, drop_stmt.only)?;
        let column_index = match visible_column_index(&relation, &drop_stmt.column_name) {
            Some(index) => index,
            None if drop_stmt.missing_ok => {
                push_notice(format!(
                    "column \"{}\" of relation \"{}\" does not exist, skipping",
                    drop_stmt.column_name,
                    relation_basename(&drop_stmt.table_name)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownColumn(
                    drop_stmt.column_name.clone(),
                )));
            }
        };
        if relation.desc.columns[column_index].attinhcount > 0 {
            return Err(cannot_drop_inherited_column_error(&drop_stmt.column_name));
        }
        if drop_stmt.only
            && relation.relkind == 'p'
            && has_inheritance_children(&catalog, relation.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: "cannot drop column from only the partitioned table when partitions exist"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        let dependent_generated_indices =
            generated_columns_to_drop_for_column(&catalog, &relation, column_index)?;
        let relation_display_name = display_relation_name(&catalog, &relation);
        let column_name = relation.desc.columns[column_index].name.clone();
        let target_attnum = (column_index + 1) as i16;
        if !drop_stmt.cascade
            && let Some(dependent_index) = dependent_generated_indices.first()
        {
            let referenced = &relation.desc.columns[column_index];
            let dependent = &relation.desc.columns[*dependent_index];
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop column {} of table {} because other objects depend on it",
                    referenced.name, relation_display_name
                ),
                detail: Some(format!(
                    "column {} of table {} depends on column {} of table {}",
                    dependent.name, relation_display_name, referenced.name, relation_display_name
                )),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
        let foreign_key_dependencies =
            foreign_key_column_dependencies(&catalog, relation.relation_oid, target_attnum);
        if !foreign_key_dependencies.is_empty() && !drop_stmt.cascade {
            let row = &foreign_key_dependencies[0];
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop column {} of table {} because other objects depend on it",
                    column_name, relation_display_name
                ),
                detail: Some(foreign_key_dependency_detail(
                    &catalog,
                    &relation_display_name,
                    &column_name,
                    row,
                )),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
        reject_column_with_rule_dependencies(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            &column_name,
            target_attnum,
        )?;
        let mut next_cid = cid;
        if drop_stmt.cascade {
            next_cid = self.drop_foreign_key_dependencies_for_column_in_transaction(
                client_id,
                xid,
                next_cid,
                &catalog,
                &foreign_key_dependencies,
                interrupts.clone(),
                catalog_effects,
            )?;
        }
        reject_column_with_trigger_dependencies(
            &catalog,
            relation.relation_oid,
            &column_name,
            target_attnum,
        )?;
        reject_column_with_publication_dependencies(
            &catalog,
            relation.relation_oid,
            &column_name,
            target_attnum,
        )?;
        let dependent_indexes = catalog
            .index_relations_for_heap(relation.relation_oid)
            .into_iter()
            .filter(|index| index.index_meta.indkey.contains(&target_attnum))
            .collect::<Vec<_>>();
        self.drop_statistics_for_column_in_transaction(
            client_id,
            relation.relation_oid,
            target_attnum,
            xid,
            &mut next_cid,
            catalog_effects,
        )?;
        for dependent_index in dependent_generated_indices.into_iter().rev() {
            let dependent_column_name = relation.desc.columns[dependent_index].name.clone();
            push_notice(format!(
                "drop cascades to column {} of table {}",
                dependent_column_name, relation_display_name
            ));
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .alter_table_drop_column_mvcc(relation.relation_oid, &dependent_column_name, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        for index in dependent_indexes {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .drop_relation_entry_by_oid_mvcc(index.relation_oid, &ctx)
                .map_err(|err| match err {
                    CatalogError::UnknownTable(_) => ExecError::Parse(
                        ParseError::TableDoesNotExist(index.relation_oid.to_string()),
                    ),
                    other => ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "dependent index for DROP COLUMN",
                        actual: format!("{other:?}"),
                    }),
                })?
                .1;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        let inheritance_targets = if drop_stmt.only {
            direct_inheritance_children(&catalog, relation.relation_oid)
                .into_iter()
                .filter_map(|relation_oid| catalog.lookup_relation_by_oid(relation_oid))
                .map(|relation| AlterTableWorkItem {
                    relation,
                    recursing: true,
                    expected_parents: 1,
                })
                .collect::<Vec<_>>()
        } else {
            work_queue
                .into_iter()
                .filter(|item| item.recursing)
                .collect::<Vec<_>>()
        };
        let mut dropped_inheritance_oids = BTreeSet::from([relation.relation_oid]);
        for item in inheritance_targets {
            let Some(column) = relation_column_by_name(&item.relation, &drop_stmt.column_name)
            else {
                continue;
            };
            let expected_parents = if drop_stmt.only {
                item.expected_parents
            } else {
                catalog
                    .inheritance_parents(item.relation.relation_oid)
                    .into_iter()
                    .filter(|parent| {
                        !parent.inhdetachpending
                            && dropped_inheritance_oids.contains(&parent.inhparent)
                    })
                    .count()
                    .min(i16::MAX as usize) as i16
            };
            if expected_parents == 0 {
                continue;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = if drop_stmt.only {
                let new_attinhcount = column.attinhcount.saturating_sub(expected_parents);
                let not_null_inhcount = column.not_null_constraint_oid.map(|_| {
                    column
                        .not_null_constraint_inhcount
                        .saturating_sub(expected_parents)
                });
                self.catalog
                    .write()
                    .update_relation_column_inheritance_mvcc(
                        item.relation.relation_oid,
                        &drop_stmt.column_name,
                        new_attinhcount,
                        true,
                        not_null_inhcount,
                        column.not_null_constraint_oid.map(|_| true),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            } else if column.attinhcount <= expected_parents && !column.attislocal {
                self.catalog
                    .write()
                    .alter_table_drop_column_mvcc(
                        item.relation.relation_oid,
                        &drop_stmt.column_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            } else {
                let new_attinhcount = column.attinhcount.saturating_sub(expected_parents);
                let new_attislocal = column.attislocal || new_attinhcount == 0;
                let not_null_inhcount = column.not_null_constraint_oid.map(|_| {
                    column
                        .not_null_constraint_inhcount
                        .saturating_sub(expected_parents)
                });
                let not_null_is_local = column
                    .not_null_constraint_oid
                    .map(|_| column.not_null_constraint_is_local || not_null_inhcount == Some(0));
                self.catalog
                    .write()
                    .update_relation_column_inheritance_mvcc(
                        item.relation.relation_oid,
                        &drop_stmt.column_name,
                        new_attinhcount,
                        new_attislocal,
                        not_null_inhcount,
                        not_null_is_local,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            };
            if !drop_stmt.only && column.attinhcount <= expected_parents && !column.attislocal {
                dropped_inheritance_oids.insert(item.relation.relation_oid);
            }
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: next_cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .alter_table_drop_column_mvcc(relation.relation_oid, &drop_stmt.column_name, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
