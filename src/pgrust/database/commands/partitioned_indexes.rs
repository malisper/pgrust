use std::sync::Arc;

use super::super::*;
use crate::backend::catalog::{CatalogEntry, CatalogIndexBuildOptions};
use crate::backend::parser::{
    AlterIndexAttachPartitionStatement, BoundIndexRelation, BoundRelation, CatalogLookup,
    IndexBackedConstraintAction, IndexColumnDef, OnCommitAction, ParseError,
};
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::pgrust::database::ddl::ensure_relation_owner;

#[derive(Debug, Clone)]
pub(super) struct PartitionedIndexSpec {
    pub columns: Vec<IndexColumnDef>,
    pub predicate_sql: Option<String>,
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub access_method_oid: u32,
    pub access_method_handler: u32,
    pub build_options: CatalogIndexBuildOptions,
}

struct PartitionedIndexInstaller<'a> {
    db: &'a Database,
    client_id: ClientId,
    xid: TransactionId,
    next_cid: &'a mut CommandId,
    configured_search_path: Option<&'a [String]>,
    catalog_effects: &'a mut Vec<CatalogMutationEffect>,
    maintenance_work_mem_kb: usize,
    interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
}

impl<'a> PartitionedIndexInstaller<'a> {
    fn visible_cid(&self) -> CommandId {
        *self.next_cid
    }

    fn take_cid(&mut self) -> CommandId {
        let cid = *self.next_cid;
        *self.next_cid = (*self.next_cid).saturating_add(1);
        cid
    }

    fn take_cid_span(&mut self, width: u32) -> CommandId {
        let cid = *self.next_cid;
        *self.next_cid = (*self.next_cid).saturating_add(width);
        cid
    }

    fn catalog(&self) -> impl CatalogLookup + '_ {
        self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        )
    }

    fn write_ctx(&self, cid: CommandId) -> CatalogWriteContext {
        CatalogWriteContext {
            pool: self.db.pool.clone(),
            txns: self.db.txns.clone(),
            xid: self.xid,
            cid,
            client_id: self.client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupts),
        }
    }

    fn apply_effect(&mut self, effect: CatalogMutationEffect) -> Result<(), ExecError> {
        self.db.apply_catalog_mutation_effect_immediate(&effect)?;
        self.catalog_effects.push(effect);
        Ok(())
    }

    fn current_relation(&self, relation_oid: u32) -> Result<BoundRelation, ExecError> {
        self.catalog().relation_by_oid(relation_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })
    }

    fn relation_name(&self, relation_oid: u32) -> Result<String, ExecError> {
        self.catalog()
            .class_row_by_oid(relation_oid)
            .map(|row| row.relname)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })
    }

    fn index_relation(&self, index_oid: u32) -> Result<BoundRelation, ExecError> {
        self.catalog()
            .relation_by_oid(index_oid)
            .filter(|relation| matches!(relation.relkind, 'i' | 'I'))
            .ok_or_else(|| {
                ExecError::Parse(ParseError::WrongObjectType {
                    name: index_oid.to_string(),
                    expected: "index",
                })
            })
    }

    fn index_by_oid(&self, index_oid: u32) -> Result<BoundIndexRelation, ExecError> {
        let heap_oid = self.index_heap_oid(index_oid)?;
        self.catalog()
            .index_relations_for_heap(heap_oid)
            .into_iter()
            .find(|index| index.relation_oid == index_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index metadata",
                    actual: format!("missing index metadata for {index_oid}"),
                })
            })
    }

    fn index_heap_oid(&self, index_oid: u32) -> Result<u32, ExecError> {
        crate::backend::utils::cache::lsyscache::index_row_by_indexrelid(
            self.db,
            self.client_id,
            Some((self.xid, self.visible_cid())),
            index_oid,
        )
        .map(|row| row.indrelid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index metadata",
                actual: format!("missing index metadata for {index_oid}"),
            })
        })
    }

    fn direct_partition_children(
        &self,
        relation_oid: u32,
    ) -> Result<Vec<BoundRelation>, ExecError> {
        let catalog = self.catalog();
        let mut inherits = catalog.inheritance_children(relation_oid);
        inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        inherits
            .into_iter()
            .filter(|row| !row.inhdetachpending)
            .filter_map(|row| {
                catalog
                    .relation_by_oid(row.inhrelid)
                    .map(|child| (child.relispartition, child))
            })
            .filter(|(is_partition, _)| *is_partition)
            .map(|(_, child)| Ok(child))
            .collect()
    }

    fn direct_index_children(&self, index_oid: u32) -> Result<Vec<BoundIndexRelation>, ExecError> {
        let catalog = self.catalog();
        let mut inherits = catalog.inheritance_children(index_oid);
        inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        inherits
            .into_iter()
            .filter(|row| !row.inhdetachpending)
            .map(|row| self.index_by_oid(row.inhrelid))
            .collect()
    }

    fn index_is_constraint_backed(&self, relation_oid: u32, index_oid: u32) -> bool {
        self.catalog()
            .constraint_rows_for_relation(relation_oid)
            .into_iter()
            .any(|row| row.conindid == index_oid)
    }

    fn column_attnums_for_index_columns(
        desc: &crate::backend::executor::RelationDesc,
        columns: &[IndexColumnDef],
    ) -> Result<Vec<i16>, ExecError> {
        columns
            .iter()
            .map(|column| {
                if column.expr_sql.is_some() {
                    return Ok(0);
                }
                desc.columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, desc)| {
                        (!desc.dropped && desc.name.eq_ignore_ascii_case(&column.name))
                            .then_some(index as i16 + 1)
                    })
                    .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.name.clone())))
            })
            .collect()
    }

    fn serialized_index_exprs(columns: &[IndexColumnDef]) -> Result<Option<String>, ExecError> {
        let exprs = columns
            .iter()
            .filter_map(|column| column.expr_sql.as_ref().map(|sql| sql.trim().to_string()))
            .collect::<Vec<_>>();
        if exprs.is_empty() {
            Ok(None)
        } else {
            serde_json::to_string(&exprs).map(Some).map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "serialized index expressions",
                    actual: "invalid index expression metadata".into(),
                })
            })
        }
    }

    fn normalized_predicate(predicate: Option<&str>) -> Option<String> {
        predicate
            .map(str::trim)
            .filter(|sql| !sql.is_empty())
            .map(str::to_string)
    }

    fn index_matches_relation(
        &self,
        index: &BoundIndexRelation,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
        expected_relkind: char,
        require_valid: bool,
        require_unattached: bool,
    ) -> Result<bool, ExecError> {
        let catalog = self.catalog();
        let Some(index_relation) = catalog.relation_by_oid(index.relation_oid) else {
            return Ok(false);
        };
        if index_relation.relkind != expected_relkind {
            return Ok(false);
        }
        if require_valid && !index.index_meta.indisvalid {
            return Ok(false);
        }
        if require_unattached && !catalog.inheritance_parents(index.relation_oid).is_empty() {
            return Ok(false);
        }
        if self.index_is_constraint_backed(relation.relation_oid, index.relation_oid) {
            return Ok(false);
        }
        let Some(class_row) = catalog.class_row_by_oid(index.relation_oid) else {
            return Ok(false);
        };
        let expected_attnums =
            Self::column_attnums_for_index_columns(&relation.desc, &spec.columns)?;
        let expected_indexprs = Self::serialized_index_exprs(&spec.columns)?;
        let expected_predicate = Self::normalized_predicate(spec.predicate_sql.as_deref());
        let actual_predicate = Self::normalized_predicate(index.index_meta.indpred.as_deref());
        Ok(class_row.relam == spec.build_options.am_oid
            && index.index_meta.indisunique == spec.unique
            && index.index_meta.indnullsnotdistinct == spec.nulls_not_distinct
            && !index.index_meta.indisprimary
            && index.index_meta.indkey == expected_attnums
            && index.index_meta.indexprs == expected_indexprs
            && actual_predicate == expected_predicate
            && index.index_meta.indclass == spec.build_options.indclass
            && index.index_meta.indcollation == spec.build_options.indcollation
            && index.index_meta.indoption == spec.build_options.indoption
            && index.index_meta.brin_options == spec.build_options.brin_options)
    }

    fn find_attached_child_index(
        &self,
        parent_index_oid: u32,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
    ) -> Result<Option<BoundIndexRelation>, ExecError> {
        let expected_relkind = if relation.relkind == 'p' { 'I' } else { 'i' };
        for child_index in self.direct_index_children(parent_index_oid)? {
            if child_index.index_meta.indrelid != relation.relation_oid {
                continue;
            }
            if self.index_matches_relation(
                &child_index,
                relation,
                spec,
                expected_relkind,
                false,
                false,
            )? {
                return Ok(Some(child_index));
            }
        }
        Ok(None)
    }

    fn find_matching_unattached_index(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
        require_valid: bool,
    ) -> Result<Option<BoundIndexRelation>, ExecError> {
        let expected_relkind = if relation.relkind == 'p' { 'I' } else { 'i' };
        for index in self
            .catalog()
            .index_relations_for_heap(relation.relation_oid)
        {
            if self.index_matches_relation(
                &index,
                relation,
                spec,
                expected_relkind,
                require_valid,
                true,
            )? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn find_matching_unattached_index_named(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
        index_name: &str,
        require_valid: bool,
    ) -> Result<Option<BoundIndexRelation>, ExecError> {
        for index in self
            .catalog()
            .index_relations_for_heap(relation.relation_oid)
        {
            let Some(class_row) = self.catalog().class_row_by_oid(index.relation_oid) else {
                continue;
            };
            if !class_row.relname.eq_ignore_ascii_case(index_name) {
                continue;
            }
            if self.index_matches_relation(&index, relation, spec, 'I', require_valid, true)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn create_index_inheritance(
        &mut self,
        child_index_oid: u32,
        parent_index_oid: u32,
    ) -> Result<(), ExecError> {
        let existing_parents = self.catalog().inheritance_parents(child_index_oid);
        if existing_parents
            .iter()
            .any(|row| row.inhparent == parent_index_oid)
        {
            return Ok(());
        }
        if !existing_parents.is_empty() {
            return Err(ExecError::DetailedError {
                message: format!("index {child_index_oid} already has an inheritance parent"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let effect = self
            .db
            .catalog
            .write()
            .create_relation_inheritance_mvcc(child_index_oid, &[parent_index_oid], &ctx)
            .map_err(map_catalog_error)?;
        self.apply_effect(effect)
    }

    fn set_index_valid(&mut self, index_oid: u32, valid: bool) -> Result<(), ExecError> {
        let index = self.index_by_oid(index_oid)?;
        if index.index_meta.indisready && index.index_meta.indisvalid == valid {
            return Ok(());
        }
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let effect = self
            .db
            .catalog
            .write()
            .set_index_ready_valid_mvcc(index_oid, true, valid, &ctx)
            .map_err(|err| match err {
                CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index catalog validity update",
                    actual: "index validity update failed".into(),
                }),
            })?;
        self.apply_effect(effect)
    }

    fn create_partitioned_index(
        &mut self,
        relation: &BoundRelation,
        index_name: &str,
        spec: &PartitionedIndexSpec,
        valid: bool,
    ) -> Result<CatalogEntry, ExecError> {
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let (mut index_entry, effect) = self
            .db
            .catalog
            .write()
            .create_partitioned_index_for_relation_mvcc_with_options(
                index_name.to_string(),
                relation.relation_oid,
                spec.unique,
                false,
                &spec.columns,
                &spec.build_options,
                spec.predicate_sql.as_deref(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_effect(effect)?;
        if !valid {
            self.set_index_valid(index_entry.relation_oid, false)?;
            if let Some(meta) = index_entry.index_meta.as_mut() {
                meta.indisvalid = false;
            }
        }
        if relation.relpersistence == 't' {
            let temp_index_meta = index_entry.index_meta.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "partitioned index metadata",
                    actual: "missing partitioned index metadata".into(),
                })
            })?;
            self.db.install_temp_entry(
                self.client_id,
                index_name,
                RelCacheEntry {
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
                    index: Some(self.db.relcache_index_meta_from_catalog(
                        self.client_id,
                        Some((self.xid, self.visible_cid())),
                        index_entry.relation_oid,
                        &temp_index_meta,
                        spec.access_method_oid,
                        spec.access_method_handler,
                    )?),
                },
                self.db
                    .temp_entry_on_commit(self.client_id, relation.relation_oid)
                    .unwrap_or(OnCommitAction::PreserveRows),
            )?;
        }
        Ok(index_entry)
    }

    fn create_physical_index(
        &mut self,
        relation: &BoundRelation,
        index_name: &str,
        spec: &PartitionedIndexSpec,
    ) -> Result<CatalogEntry, ExecError> {
        let index_cid = self.take_cid_span(2);
        let visible_catalog = self
            .db
            .lazy_catalog_lookup(
                self.client_id,
                Some((self.xid, self.visible_cid())),
                self.configured_search_path,
            )
            .materialize_visible_catalog();
        self.db.build_simple_index_in_transaction(
            self.client_id,
            relation,
            index_name,
            visible_catalog,
            &spec.columns,
            spec.predicate_sql.as_deref(),
            spec.unique,
            false,
            spec.nulls_not_distinct,
            self.xid,
            index_cid,
            spec.access_method_oid,
            spec.access_method_handler,
            &spec.build_options,
            self.maintenance_work_mem_kb,
            self.catalog_effects,
        )
    }

    fn child_index_base_name(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
    ) -> Result<String, ExecError> {
        let relation_name = self.relation_name(relation.relation_oid)?;
        Ok(Database::default_index_base_name(
            &relation_name,
            &spec.columns,
        ))
    }

    fn child_index_name(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedIndexSpec,
    ) -> Result<String, ExecError> {
        let base = self.child_index_base_name(relation, spec)?;
        self.db.choose_available_relation_name(
            self.client_id,
            self.xid,
            self.visible_cid(),
            relation.namespace_oid,
            &base,
        )
    }

    fn reconcile_relation_index_tree(
        &mut self,
        relation: BoundRelation,
        parent_index_oid: u32,
        spec: &PartitionedIndexSpec,
    ) -> Result<(u32, bool), ExecError> {
        let relation = self.current_relation(relation.relation_oid)?;
        if let Some(attached) = self.find_attached_child_index(parent_index_oid, &relation, spec)? {
            return Ok((attached.relation_oid, attached.index_meta.indisvalid));
        }
        if let Some(existing) = self.find_matching_unattached_index(&relation, spec, true)? {
            self.create_index_inheritance(existing.relation_oid, parent_index_oid)?;
            return Ok((existing.relation_oid, existing.index_meta.indisvalid));
        }

        if relation.relkind == 'p' {
            let base_name = self.child_index_base_name(&relation, spec)?;
            if let Some(existing) =
                self.find_matching_unattached_index_named(&relation, spec, &base_name, false)?
            {
                self.create_index_inheritance(existing.relation_oid, parent_index_oid)?;
                return Ok((existing.relation_oid, existing.index_meta.indisvalid));
            }
            let index_name = self.child_index_name(&relation, spec)?;
            let index_entry = self.create_partitioned_index(&relation, &index_name, spec, true)?;
            self.create_index_inheritance(index_entry.relation_oid, parent_index_oid)?;
            let valid = self.reconcile_existing_partitioned_index_children(
                &relation,
                index_entry.relation_oid,
                spec,
            )?;
            Ok((index_entry.relation_oid, valid))
        } else {
            let index_name = self.child_index_name(&relation, spec)?;
            let index_entry = self.create_physical_index(&relation, &index_name, spec)?;
            self.create_index_inheritance(index_entry.relation_oid, parent_index_oid)?;
            Ok((index_entry.relation_oid, true))
        }
    }

    fn reconcile_existing_partitioned_index_children(
        &mut self,
        relation: &BoundRelation,
        index_oid: u32,
        spec: &PartitionedIndexSpec,
    ) -> Result<bool, ExecError> {
        let mut valid = true;
        for child in self.direct_partition_children(relation.relation_oid)? {
            let (_, child_valid) = self.reconcile_relation_index_tree(child, index_oid, spec)?;
            if !child_valid {
                valid = false;
            }
        }
        self.set_index_valid(index_oid, valid)?;
        Ok(valid)
    }

    fn validate_partitioned_index_upward(&mut self, index_oid: u32) -> Result<bool, ExecError> {
        let index = self.index_by_oid(index_oid)?;
        let relation = self.current_relation(index.index_meta.indrelid)?;
        if relation.relkind != 'p' {
            return Ok(index.index_meta.indisvalid);
        }
        let direct_partitions = self.direct_partition_children(relation.relation_oid)?;
        let attached_indexes = self.direct_index_children(index_oid)?;
        let valid = direct_partitions.iter().all(|partition| {
            attached_indexes.iter().any(|child_index| {
                child_index.index_meta.indrelid == partition.relation_oid
                    && child_index.index_meta.indisvalid
            })
        });
        self.set_index_valid(index_oid, valid)?;
        if valid {
            let parent_oids = self
                .catalog()
                .inheritance_parents(index_oid)
                .into_iter()
                .map(|row| row.inhparent)
                .collect::<Vec<_>>();
            for parent_oid in parent_oids {
                let _ = self.validate_partitioned_index_upward(parent_oid)?;
            }
        }
        Ok(valid)
    }

    fn spec_from_index(
        &self,
        index: &BoundIndexRelation,
        relation: &BoundRelation,
    ) -> Result<PartitionedIndexSpec, ExecError> {
        let mut expr_sqls = index
            .index_meta
            .indexprs
            .as_deref()
            .map(|json| {
                serde_json::from_str::<Vec<String>>(json).map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "serialized index expressions",
                        actual: "invalid index expression metadata".into(),
                    })
                })
            })
            .transpose()?
            .unwrap_or_default()
            .into_iter();
        let mut columns = Vec::with_capacity(index.index_meta.indkey.len());
        for (position, attnum) in index.index_meta.indkey.iter().copied().enumerate() {
            let indoption = index
                .index_meta
                .indoption
                .get(position)
                .copied()
                .unwrap_or(0);
            if attnum == 0 {
                let expr_sql = expr_sqls.next().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index expression SQL",
                        actual: "missing expression index metadata".into(),
                    })
                })?;
                columns.push(IndexColumnDef {
                    name: String::new(),
                    expr_sql: Some(expr_sql),
                    expr_type: index
                        .desc
                        .columns
                        .get(position)
                        .map(|column| column.sql_type),
                    collation: None,
                    opclass: None,
                    descending: indoption & 0x0001 != 0,
                    nulls_first: (indoption & 0x0002 != 0).then_some(true),
                });
                continue;
            }
            let column = relation
                .desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .filter(|column| !column.dropped)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index column",
                        actual: format!("invalid attnum {attnum}"),
                    })
                })?;
            columns.push(IndexColumnDef {
                name: column.name.clone(),
                expr_sql: None,
                expr_type: None,
                collation: None,
                opclass: None,
                descending: indoption & 0x0001 != 0,
                nulls_first: (indoption & 0x0002 != 0).then_some(true),
            });
        }
        let access_method_oid = index.index_meta.am_oid;
        let access_method_handler = index
            .index_meta
            .am_handler_oid
            .or_else(|| {
                crate::backend::utils::cache::lsyscache::access_method_row_by_oid(
                    self.db,
                    self.client_id,
                    Some((self.xid, self.visible_cid())),
                    access_method_oid,
                )
                .map(|row| row.amhandler)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method handler",
                    actual: format!("missing access method {access_method_oid}"),
                })
            })?;
        Ok(PartitionedIndexSpec {
            columns,
            predicate_sql: index.index_meta.indpred.clone(),
            unique: index.index_meta.indisunique,
            nulls_not_distinct: index.index_meta.indnullsnotdistinct,
            access_method_oid,
            access_method_handler,
            build_options: CatalogIndexBuildOptions {
                am_oid: access_method_oid,
                indclass: index.index_meta.indclass.clone(),
                indcollation: index.index_meta.indcollation.clone(),
                indoption: index.index_meta.indoption.clone(),
                indnullsnotdistinct: index.index_meta.indnullsnotdistinct,
                indisexclusion: index.index_meta.indisexclusion,
                brin_options: index.index_meta.brin_options.clone(),
                gin_options: index.index_meta.gin_options.clone(),
            },
        })
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_partitioned_index_in_transaction(
        &self,
        client_id: ClientId,
        relation: &BoundRelation,
        index_name: &str,
        columns: &[IndexColumnDef],
        predicate_sql: Option<&str>,
        unique: bool,
        nulls_not_distinct: bool,
        only: bool,
        xid: TransactionId,
        start_cid: CommandId,
        access_method_oid: u32,
        access_method_handler: u32,
        build_options: &CatalogIndexBuildOptions,
        maintenance_work_mem_kb: usize,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CatalogEntry, ExecError> {
        if unique {
            let partition_spec = crate::backend::parser::relation_partition_spec(relation)
                .map_err(ExecError::Parse)?;
            let key_columns = columns
                .iter()
                .filter(|column| column.expr_sql.is_none())
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let relation_name = self
                .lazy_catalog_lookup(client_id, Some((xid, start_cid)), configured_search_path)
                .class_row_by_oid(relation.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| relation.relation_oid.to_string());
            crate::backend::parser::validate_partitioned_index_backed_constraints(
                &relation_name,
                Some(&partition_spec),
                &[IndexBackedConstraintAction {
                    constraint_name: Some(index_name.to_string()),
                    columns: key_columns,
                    primary: false,
                    nulls_not_distinct,
                    without_overlaps: None,
                }],
            )
            .map_err(ExecError::Parse)?;
        }

        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = start_cid;
        let mut installer = PartitionedIndexInstaller {
            db: self,
            client_id,
            xid,
            next_cid: &mut next_cid,
            configured_search_path,
            catalog_effects,
            maintenance_work_mem_kb,
            interrupts,
        };
        let spec = PartitionedIndexSpec {
            columns: columns.to_vec(),
            predicate_sql: predicate_sql.map(str::to_string),
            unique,
            nulls_not_distinct,
            access_method_oid,
            access_method_handler,
            build_options: CatalogIndexBuildOptions {
                indnullsnotdistinct: nulls_not_distinct,
                ..build_options.clone()
            },
        };
        let mut root = installer.create_partitioned_index(relation, index_name, &spec, true)?;
        if only {
            if !installer
                .direct_partition_children(relation.relation_oid)?
                .is_empty()
            {
                installer.set_index_valid(root.relation_oid, false)?;
                if let Some(meta) = root.index_meta.as_mut() {
                    meta.indisvalid = false;
                }
            }
            return Ok(root);
        }

        let valid = installer.reconcile_existing_partitioned_index_children(
            relation,
            root.relation_oid,
            &spec,
        )?;
        if let Some(meta) = root.index_meta.as_mut() {
            meta.indisvalid = valid;
        }
        Ok(root)
    }

    pub(super) fn reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        start_cid: CommandId,
        parent_relation_oid: u32,
        child_relation_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = start_cid;
        let mut installer = PartitionedIndexInstaller {
            db: self,
            client_id,
            xid,
            next_cid: &mut next_cid,
            configured_search_path,
            catalog_effects,
            maintenance_work_mem_kb: 65_536,
            interrupts,
        };
        let parent = installer.current_relation(parent_relation_oid)?;
        let child = installer.current_relation(child_relation_oid)?;
        let catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, installer.visible_cid())),
            configured_search_path,
        );
        let constrained_indexes = catalog
            .constraint_rows_for_relation(parent_relation_oid)
            .into_iter()
            .map(|row| row.conindid)
            .collect::<Vec<_>>();
        let parent_indexes = catalog.index_relations_for_heap(parent_relation_oid);
        for parent_index in parent_indexes {
            let Some(parent_index_relation) = catalog.relation_by_oid(parent_index.relation_oid)
            else {
                continue;
            };
            if parent_index_relation.relkind != 'I'
                || constrained_indexes.contains(&parent_index.relation_oid)
            {
                continue;
            }
            let spec = installer.spec_from_index(&parent_index, &parent)?;
            let _ = installer.reconcile_relation_index_tree(
                child.clone(),
                parent_index.relation_oid,
                &spec,
            )?;
            let _ = installer.validate_partitioned_index_upward(parent_index.relation_oid)?;
        }
        Ok(next_cid)
    }

    pub(crate) fn execute_alter_index_attach_partition_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterIndexAttachPartitionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_index_attach_partition_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_index_attach_partition_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterIndexAttachPartitionStatement,
        xid: TransactionId,
        start_cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, start_cid)), configured_search_path);
        let parent_relation = catalog
            .lookup_any_relation(&stmt.parent_index_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(stmt.parent_index_name.clone()))
            })?;
        let child_relation = catalog
            .lookup_any_relation(&stmt.child_index_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(stmt.child_index_name.clone()))
            })?;
        if parent_relation.relkind != 'I' {
            let message = if parent_relation.relkind == 'i' {
                format!("\"{}\" is not a partitioned index", stmt.parent_index_name)
            } else {
                format!("\"{}\" is not an index", stmt.parent_index_name)
            };
            return Err(ExecError::DetailedError {
                message,
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        if !matches!(child_relation.relkind, 'i' | 'I') {
            return Err(ExecError::DetailedError {
                message: format!("\"{}\" is not an index", stmt.child_index_name),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        ensure_relation_owner(self, client_id, &parent_relation, &stmt.parent_index_name)?;
        ensure_relation_owner(self, client_id, &child_relation, &stmt.child_index_name)?;

        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = start_cid;
        let mut installer = PartitionedIndexInstaller {
            db: self,
            client_id,
            xid,
            next_cid: &mut next_cid,
            configured_search_path,
            catalog_effects,
            maintenance_work_mem_kb: 65_536,
            interrupts,
        };
        let parent_index = installer.index_by_oid(parent_relation.relation_oid)?;
        let child_index = installer.index_by_oid(child_relation.relation_oid)?;
        let parent_table = installer.current_relation(parent_index.index_meta.indrelid)?;
        let child_table = installer.current_relation(child_index.index_meta.indrelid)?;
        if !installer
            .catalog()
            .inheritance_parents(child_table.relation_oid)
            .iter()
            .any(|row| row.inhparent == parent_table.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    stmt.child_index_name, stmt.parent_index_name
                ),
                detail: Some(
                    "The child index is not on a partition of the parent index's table.".into(),
                ),
                hint: None,
                sqlstate: "42809",
            });
        }
        let existing_parents = installer
            .catalog()
            .inheritance_parents(child_index.relation_oid);
        if existing_parents
            .iter()
            .any(|row| row.inhparent == parent_index.relation_oid)
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        if !existing_parents.is_empty() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "index \"{}\" is already attached to another index",
                    stmt.child_index_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        if installer
            .direct_index_children(parent_index.relation_oid)?
            .into_iter()
            .any(|index| index.index_meta.indrelid == child_table.relation_oid)
        {
            let child_table_name = installer
                .catalog()
                .class_row_by_oid(child_table.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| stmt.child_index_name.clone());
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    stmt.child_index_name, stmt.parent_index_name
                ),
                detail: Some(format!(
                    "Another index is already attached for partition \"{child_table_name}\"."
                )),
                hint: None,
                sqlstate: "42710",
            });
        }
        if installer.index_is_constraint_backed(child_table.relation_oid, child_index.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot attach constraint index \"{}\"",
                    stmt.child_index_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        let spec = installer.spec_from_index(&parent_index, &parent_table)?;
        let expected_relkind = if child_table.relkind == 'p' { 'I' } else { 'i' };
        if !installer.index_matches_relation(
            &child_index,
            &child_table,
            &spec,
            expected_relkind,
            false,
            true,
        )? {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    stmt.child_index_name, stmt.parent_index_name
                ),
                detail: Some("The index definitions do not match.".into()),
                hint: None,
                sqlstate: "42809",
            });
        }
        installer.create_index_inheritance(child_index.relation_oid, parent_index.relation_oid)?;
        let _ = installer.validate_partitioned_index_upward(parent_index.relation_oid)?;
        Ok(StatementResult::AffectedRows(0))
    }
}
