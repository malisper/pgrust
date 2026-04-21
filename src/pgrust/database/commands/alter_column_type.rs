use super::super::*;
use crate::backend::access::heap::heapam::heap_update_with_waiter;
use crate::backend::commands::tablecmds::{
    collect_matching_rows_heap, insert_index_entry_for_row, reinitialize_index_relation,
};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::backend::executor::{ExecutorContext, RelationDesc, TupleSlot, eval_expr};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::catalog::{BTREE_AM_OID, PG_CATALOG_NAMESPACE_OID, default_btree_opclass_oid};
use crate::pgrust::database::ddl::{
    lookup_heap_relation_for_alter_table, validate_alter_table_alter_column_type,
};
use std::collections::BTreeSet;

struct AlterColumnTypeTarget {
    relation: crate::backend::parser::BoundRelation,
    new_desc: RelationDesc,
    rewrite_expr: crate::backend::executor::Expr,
    column_index: usize,
    indexes: Vec<crate::backend::parser::BoundIndexRelation>,
}

fn reject_unsupported_alter_column_type_indexes(
    indexes: &[crate::backend::parser::BoundIndexRelation],
    column_index: usize,
) -> Result<(), ExecError> {
    let target_attnum = (column_index + 1) as i16;
    let has_unsupported_dependency = indexes.iter().any(|index| {
        if index
            .index_meta
            .indpred
            .as_deref()
            .is_some_and(|pred| !pred.is_empty())
            || index
                .index_meta
                .indexprs
                .as_deref()
                .is_some_and(|exprs| !exprs.is_empty())
        {
            return true;
        }
        if !index.index_meta.indkey.contains(&target_attnum) {
            return false;
        }
        // :HACK: Plain primary/unique key indexes survive the current heap
        // rewrite path well enough for the regression ALTER TYPE cases, but
        // general secondary-index metadata rewrites still need real support.
        !(index.index_meta.indisprimary || index.index_meta.indisunique)
    });
    if has_unsupported_dependency {
        // :HACK: First-pass ALTER COLUMN TYPE rewrites heap rows in place and
        // only keeps plain primary/unique indexes in sync. Secondary target-
        // column indexes and expression/partial indexes still need proper
        // index metadata rewrites.
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "ALTER TABLE ALTER COLUMN TYPE with dependent indexes".into(),
        )));
    }
    Ok(())
}

fn rewrite_bound_indexes_for_alter_column_type(
    indexes: Vec<crate::backend::parser::BoundIndexRelation>,
    column_index: usize,
    new_column: &crate::backend::executor::ColumnDesc,
) -> Vec<crate::backend::parser::BoundIndexRelation> {
    let target_attnum = (column_index + 1) as i16;
    let new_type_oid = sql_type_oid(new_column.sql_type);
    indexes
        .into_iter()
        .map(|mut index| {
            for (index_column_index, attnum) in index.index_meta.indkey.iter().enumerate() {
                if *attnum != target_attnum {
                    continue;
                }
                index.desc.columns[index_column_index] = new_column.clone();
                if index.index_meta.am_oid == BTREE_AM_OID
                    && let Some(opclass_oid) = default_btree_opclass_oid(new_type_oid)
                {
                    index.index_meta.indclass[index_column_index] = opclass_oid;
                }
            }
            index
        })
        .collect()
}

fn rewrite_heap_rows_for_alter_column_type(
    _db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    column_index: usize,
    rewrite_expr: &crate::backend::executor::Expr,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    let target_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
    let mut rewritten_rows = Vec::with_capacity(target_rows.len());
    for (tid, original_values) in target_rows {
        ctx.check_for_interrupts()?;
        let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
        let mut values = original_values;
        values[column_index] = eval_expr(rewrite_expr, &mut eval_slot, ctx)?;
        let replacement = tuple_from_values(new_desc, &values)?;
        let new_tid = heap_update_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            relation.rel,
            &ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
            None,
        )?;
        rewritten_rows.push((new_tid, values));
    }
    Ok(rewritten_rows)
}

fn rebuild_relation_indexes_for_alter_column_type(
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    indexes: &[crate::backend::parser::BoundIndexRelation],
    rewritten_rows: &[(ItemPointerData, Vec<Value>)],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    for index in indexes
        .iter()
        .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
    {
        reinitialize_index_relation(index, ctx, xid)?;
        for (tid, values) in rewritten_rows {
            insert_index_entry_for_row(relation.rel, new_desc, index, values, *tid, ctx)?;
        }
    }
    Ok(())
}

fn relation_name_for_error(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn reject_inherited_type_change_conflicts(
    catalog: &dyn CatalogLookup,
    target_relation_oids: &BTreeSet<u32>,
    relation: &crate::backend::parser::BoundRelation,
    column_name: &str,
    new_sql_type: crate::backend::parser::SqlType,
) -> Result<(), ExecError> {
    for parent in catalog.inheritance_parents(relation.relation_oid) {
        if target_relation_oids.contains(&parent.inhparent) {
            continue;
        }
        let Some(parent_relation) = catalog.lookup_relation_by_oid(parent.inhparent) else {
            continue;
        };
        let Some(parent_column) = parent_relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        else {
            continue;
        };
        if parent_column.sql_type != new_sql_type {
            let relation_name = relation_name_for_error(catalog, relation.relation_oid);
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot alter inherited column \"{column_name}\" of relation \"{relation_name}\""
                ),
                detail: Some(format!(
                    "child table \"{relation_name}\" has conflicting inherited definition for column \"{column_name}\""
                )),
                hint: None,
                sqlstate: "0A000",
            });
        }
    }
    Ok(())
}

fn collect_alter_column_type_targets(
    db: &Database,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    relation: &crate::backend::parser::BoundRelation,
    alter_stmt: &crate::backend::parser::AlterTableAlterColumnTypeStatement,
) -> Result<Vec<AlterColumnTypeTarget>, ExecError> {
    let target_relation_oids = catalog
        .find_all_inheritors(relation.relation_oid)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut targets = Vec::with_capacity(target_relation_oids.len());

    for relation_oid in &target_relation_oids {
        let target_relation = if *relation_oid == relation.relation_oid {
            relation.clone()
        } else {
            catalog
                .lookup_relation_by_oid(*relation_oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownTable(relation_oid.to_string()))
                })?
        };
        if target_relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN TYPE",
                actual: "system catalog".into(),
            }));
        }
        reject_relation_with_dependent_views(
            db,
            client_id,
            Some((xid, cid)),
            target_relation.relation_oid,
            "ALTER TABLE ALTER COLUMN TYPE on relation without dependent views",
        )?;
        let plan = validate_alter_table_alter_column_type(
            catalog,
            &target_relation.desc,
            &alter_stmt.column_name,
            &alter_stmt.ty,
            if *relation_oid == relation.relation_oid {
                alter_stmt.using_expr.as_ref()
            } else {
                None
            },
        )?;
        reject_inherited_type_change_conflicts(
            catalog,
            &target_relation_oids,
            &target_relation,
            &alter_stmt.column_name,
            plan.new_column.sql_type,
        )?;
        let indexes = catalog.index_relations_for_heap(target_relation.relation_oid);
        reject_unsupported_alter_column_type_indexes(&indexes, plan.column_index)?;
        let mut new_desc = target_relation.desc.clone();
        new_desc.columns[plan.column_index] = plan.new_column;
        let indexes = rewrite_bound_indexes_for_alter_column_type(
            indexes,
            plan.column_index,
            &new_desc.columns[plan.column_index],
        );
        targets.push(AlterColumnTypeTarget {
            relation: target_relation,
            new_desc,
            rewrite_expr: plan.rewrite_expr,
            column_index: plan.column_index,
            indexes,
        });
    }

    Ok(targets)
}

impl Database {
    pub(crate) fn execute_alter_table_alter_column_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnTypeStatement,
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
        let result = self
            .execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
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

    pub(crate) fn execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnTypeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
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
                expected: "user table for ALTER TABLE ALTER COLUMN TYPE",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let targets = collect_alter_column_type_targets(
            self, &catalog, client_id, xid, cid, &relation, alter_stmt,
        )?;

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: std::sync::Arc::clone(&interrupts),
            stats: std::sync::Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
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
        for target in &targets {
            let rewritten_rows = rewrite_heap_rows_for_alter_column_type(
                self,
                &target.relation,
                &target.new_desc,
                target.column_index,
                &target.rewrite_expr,
                &mut ctx,
                xid,
                cid,
            )?;
            rebuild_relation_indexes_for_alter_column_type(
                &target.relation,
                &target.new_desc,
                &target.indexes,
                &rewritten_rows,
                &mut ctx,
                xid,
            )?;
        }
        drop(ctx);

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        for target in targets {
            let effect = self
                .catalog
                .write()
                .alter_table_alter_column_type_mvcc(
                    target.relation.relation_oid,
                    &alter_stmt.column_name,
                    target.new_desc.columns[target.column_index].clone(),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            if target.relation.relpersistence == 't' {
                self.replace_temp_entry_desc(
                    client_id,
                    target.relation.relation_oid,
                    target.new_desc,
                )?;
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
