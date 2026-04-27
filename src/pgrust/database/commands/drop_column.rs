use std::collections::BTreeSet;

use super::super::*;
use super::typed_table::reject_typed_table_ddl;
use crate::backend::parser::{BoundRelation, bind_generated_expr, expr_references_column};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    is_system_column_name, lookup_heap_relation_for_alter_table,
    reject_column_with_rule_dependencies, reject_column_with_trigger_dependencies,
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

impl Database {
    pub(crate) fn execute_alter_table_drop_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &AlterTableDropColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
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
        self.table_locks.unlock_table(relation.rel, client_id);
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
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
        reject_inheritance_tree_ddl(
            &catalog,
            relation.relation_oid,
            "ALTER TABLE DROP COLUMN on inheritance tree members is not supported yet",
        )?;
        if is_system_column_name(&drop_stmt.column_name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column name for DROP COLUMN",
                actual: drop_stmt.column_name.clone(),
            }));
        }
        let column_index =
            match relation
                .desc
                .columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(&drop_stmt.column_name))
                        .then_some(index)
                }) {
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
        let dependent_generated_indices =
            generated_columns_to_drop_for_column(&catalog, &relation, column_index)?;
        let relation_display_name = display_relation_name(&catalog, &relation);
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
        reject_column_with_rule_dependencies(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            &relation.desc.columns[column_index].name,
            (column_index + 1) as i16,
        )?;
        reject_column_with_foreign_key_dependencies(
            &catalog,
            relation.relation_oid,
            &relation.desc.columns[column_index].name,
            (column_index + 1) as i16,
            "ALTER TABLE DROP COLUMN on column without foreign key dependencies",
        )?;
        reject_column_with_trigger_dependencies(
            &catalog,
            relation.relation_oid,
            &relation.desc.columns[column_index].name,
            (column_index + 1) as i16,
        )?;
        let target_attnum = (column_index + 1) as i16;
        let dependent_indexes = catalog
            .index_relations_for_heap(relation.relation_oid)
            .into_iter()
            .filter(|index| index.index_meta.indkey.contains(&target_attnum))
            .collect::<Vec<_>>();
        let mut next_cid = cid;
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
