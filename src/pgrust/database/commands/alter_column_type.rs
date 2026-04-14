use super::super::*;
use crate::backend::access::heap::heapam::heap_update_with_waiter;
use crate::backend::commands::tablecmds::{collect_matching_rows_heap, maintain_indexes_for_row};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::executor::{ExecutorContext, RelationDesc, TupleSlot, eval_expr};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::validate_alter_table_alter_column_type;

fn reject_unsupported_alter_column_type_indexes(
    indexes: &[crate::backend::parser::BoundIndexRelation],
    column_index: usize,
) -> Result<(), ExecError> {
    let target_attnum = (column_index + 1) as i16;
    let has_unsupported_dependency = indexes.iter().any(|index| {
        index.index_meta.indkey.contains(&target_attnum)
            || index
                .index_meta
                .indpred
                .as_deref()
                .is_some_and(|pred| !pred.is_empty())
            || index
                .index_meta
                .indexprs
                .as_deref()
                .is_some_and(|exprs| !exprs.is_empty())
    });
    if has_unsupported_dependency {
        // :HACK: First-pass ALTER COLUMN TYPE rewrites heap rows in place and
        // only keeps unrelated indexes in sync. Target-column indexes and
        // expression/partial indexes need proper index metadata rewrites.
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "ALTER TABLE ALTER COLUMN TYPE with dependent indexes".into(),
        )));
    }
    Ok(())
}

fn rewrite_heap_rows_for_alter_column_type(
    _db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    indexes: &[crate::backend::parser::BoundIndexRelation],
    column_index: usize,
    rewrite_expr: &crate::backend::executor::Expr,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let target_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
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
        maintain_indexes_for_row(relation.rel, new_desc, indexes, &values, new_tid, ctx)?;
    }
    Ok(())
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
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
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN TYPE",
                actual: "system catalog".into(),
            }));
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE ALTER COLUMN TYPE",
                actual: "temporary table".into(),
            }));
        }
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE ALTER COLUMN TYPE on relation without dependent views",
        )?;
        let plan = validate_alter_table_alter_column_type(
            &catalog,
            &relation.desc,
            &alter_stmt.column_name,
            &alter_stmt.ty,
            alter_stmt.using_expr.as_ref(),
        )?;
        let indexes = catalog.index_relations_for_heap(relation.relation_oid);
        reject_unsupported_alter_column_type_indexes(&indexes, plan.column_index)?;

        let mut new_desc = relation.desc.clone();
        new_desc.columns[plan.column_index] = plan.new_column.clone();
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            interrupts: std::sync::Arc::clone(&interrupts),
            snapshot,
            client_id,
            next_command_id: cid,
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            outer_rows: Vec::new(),
            subplans: Vec::new(),
            timed: false,
        };
        rewrite_heap_rows_for_alter_column_type(
            self,
            &relation,
            &new_desc,
            &indexes,
            plan.column_index,
            &plan.rewrite_expr,
            &mut ctx,
            xid,
            cid,
        )?;
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
        let effect = self
            .catalog
            .write()
            .alter_table_alter_column_type_mvcc(
                relation.relation_oid,
                &alter_stmt.column_name,
                plan.new_column,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
