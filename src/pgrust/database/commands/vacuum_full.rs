use super::super::*;
use crate::backend::access::heap::heapam::heap_insert_mvcc_with_cid;
use crate::backend::access::heap::vacuumlazy::VacuumRelationStats;
use crate::backend::commands::tablecmds::{
    collect_matching_rows_heap, collect_vacuum_stats_for_relations, maintain_indexes_for_row,
    reinitialize_index_relation, toast_tuple_for_write,
};
use crate::backend::executor::{ExecError, ExecutorContext, Value};
use crate::backend::parser::{BoundIndexRelation, BoundRelation, CatalogLookup, ParseError};
use crate::include::nodes::parsenodes::MaintenanceTarget;

impl Database {
    pub(crate) fn execute_vacuum_full_targets_with_search_path(
        &self,
        client_id: ClientId,
        targets: &[MaintenanceTarget],
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        process_main: bool,
        ctx: &mut ExecutorContext,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<Vec<VacuumRelationStats>, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut stats_targets = Vec::new();

        for target in targets {
            let Some(relation) = catalog.lookup_any_relation(&target.table_name) else {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            };
            if process_main {
                if !matches!(relation.relkind, 'r' | 'm') {
                    continue;
                }
                self.vacuum_full_main_relation(
                    client_id,
                    &relation,
                    xid,
                    cid,
                    configured_search_path,
                    ctx,
                    catalog_effects,
                )?;
                let refreshed = self.lookup_vacuum_full_relation(
                    client_id,
                    xid,
                    cid.saturating_add(1),
                    configured_search_path,
                    relation.relation_oid,
                )?;
                stats_targets.push(refreshed);
            } else if let Some(toast) = relation.toast {
                let Some(toast_relation) = catalog.relation_by_oid(toast.relation_oid) else {
                    continue;
                };
                self.vacuum_full_storage_relation(
                    client_id,
                    &toast_relation,
                    xid,
                    cid,
                    configured_search_path,
                    ctx,
                    catalog_effects,
                    false,
                )?;
            }
        }

        if stats_targets.is_empty() {
            return Ok(Vec::new());
        }
        let refreshed_catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        collect_vacuum_stats_for_relations(&stats_targets, &refreshed_catalog, ctx)
    }

    fn vacuum_full_main_relation(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        ctx: &mut ExecutorContext,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        self.vacuum_full_storage_relation(
            client_id,
            relation,
            xid,
            cid,
            configured_search_path,
            ctx,
            catalog_effects,
            true,
        )
    }

    fn vacuum_full_storage_relation(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        ctx: &mut ExecutorContext,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        rewrite_toast: bool,
    ) -> Result<(), ExecError> {
        let rows =
            collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?
                .into_iter()
                .map(|(_, values)| values)
                .collect::<Vec<_>>();

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut rewrite_oids = vec![relation.relation_oid];
        for index in catalog.index_relations_for_heap(relation.relation_oid) {
            push_unique_oid(&mut rewrite_oids, index.relation_oid);
        }
        if rewrite_toast && let Some(toast) = relation.toast {
            push_unique_oid(&mut rewrite_oids, toast.relation_oid);
            for index in catalog.index_relations_for_heap(toast.relation_oid) {
                push_unique_oid(&mut rewrite_oids, index.relation_oid);
            }
        }

        self.rewrite_vacuum_full_storage(
            client_id,
            xid,
            cid,
            configured_search_path,
            &rewrite_oids,
            ctx,
            catalog_effects,
        )?;

        let post_rewrite_cid = cid.saturating_add(1);
        let refreshed = self.lookup_vacuum_full_relation(
            client_id,
            xid,
            post_rewrite_cid,
            configured_search_path,
            relation.relation_oid,
        )?;
        self.reinsert_vacuum_full_rows(
            client_id,
            xid,
            post_rewrite_cid,
            cid,
            configured_search_path,
            refreshed,
            rows,
            ctx,
        )
    }

    fn rewrite_vacuum_full_storage(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        rewrite_oids: &[u32],
        ctx: &mut ExecutorContext,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let write_ctx = CatalogWriteContext {
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
            .rewrite_relation_storage_mvcc(rewrite_oids, &write_ctx)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let refreshed_catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, cid.saturating_add(1))),
            configured_search_path,
        );
        ctx.catalog = refreshed_catalog.materialize_visible_catalog();
        Ok(())
    }

    fn lookup_vacuum_full_relation(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        relation_oid: u32,
    ) -> Result<BoundRelation, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        catalog
            .relation_by_oid(relation_oid)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(relation_oid.to_string())))
    }

    fn reinsert_vacuum_full_rows(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        catalog_cid: CommandId,
        insert_cid: CommandId,
        configured_search_path: Option<&[String]>,
        relation: BoundRelation,
        rows: Vec<Vec<Value>>,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, catalog_cid)), configured_search_path);
        let indexes = catalog.index_relations_for_heap(relation.relation_oid);
        let toast_index = relation.toast.and_then(|toast| {
            catalog
                .index_relations_for_heap(toast.relation_oid)
                .into_iter()
                .next()
        });
        reinitialize_indexes(&indexes, ctx, xid)?;
        if let Some(toast) = relation.toast {
            let toast_indexes = catalog.index_relations_for_heap(toast.relation_oid);
            reinitialize_indexes(&toast_indexes, ctx, xid)?;
        }
        for values in rows {
            let (tuple, _toasted) = toast_tuple_for_write(
                &relation.desc,
                &values,
                relation.toast,
                toast_index.as_ref(),
                ctx,
                xid,
                insert_cid,
            )?;
            let tid = heap_insert_mvcc_with_cid(
                &*ctx.pool,
                ctx.client_id,
                relation.rel,
                xid,
                insert_cid,
                &tuple,
            )?;
            maintain_indexes_for_row(relation.rel, &relation.desc, &indexes, &values, tid, ctx)?;
        }
        Ok(())
    }
}

fn push_unique_oid(oids: &mut Vec<u32>, oid: u32) {
    if !oids.contains(&oid) {
        oids.push(oid);
    }
}

fn reinitialize_indexes(
    indexes: &[BoundIndexRelation],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    for index in indexes {
        if index.index_meta.indisvalid && index.index_meta.indisready {
            reinitialize_index_relation(index, ctx, xid)?;
        }
    }
    Ok(())
}
