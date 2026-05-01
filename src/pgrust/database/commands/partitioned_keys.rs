use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use super::super::*;
use super::constraint::{validate_not_null_rows, verify_not_null_pk_compatible};
use super::tablespace::resolve_relation_tablespace_oid;
use crate::backend::catalog::pg_constraint::sort_pg_constraint_rows;
use crate::backend::catalog::{CatalogEntry, CatalogIndexBuildOptions};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    BoundRelation, CatalogLookup, IndexBackedConstraintAction, IndexColumnDef, OnCommitAction,
    ParseError,
};
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::include::catalog::{
    CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, PgConstraintRow,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionedKeySpec {
    columns: Vec<String>,
    primary: bool,
    nulls_not_distinct: bool,
    without_overlaps: Option<String>,
    tablespace: Option<String>,
    deferrable: bool,
    initially_deferred: bool,
}

impl PartitionedKeySpec {
    fn from_action(action: &IndexBackedConstraintAction) -> Self {
        Self {
            columns: action.columns.clone(),
            primary: action.primary,
            nulls_not_distinct: action.nulls_not_distinct,
            without_overlaps: action.without_overlaps.clone(),
            tablespace: action.tablespace.clone(),
            deferrable: action.deferrable,
            initially_deferred: action.initially_deferred,
        }
    }

    fn constraint_type(&self) -> char {
        if self.primary {
            CONSTRAINT_PRIMARY
        } else {
            CONSTRAINT_UNIQUE
        }
    }

    fn index_columns(&self) -> Vec<IndexColumnDef> {
        self.columns
            .iter()
            .cloned()
            .map(IndexColumnDef::from)
            .collect()
    }

    fn default_constraint_name(&self, relation_name: &str) -> String {
        if self.primary {
            format!("{relation_name}_pkey")
        } else {
            format!("{relation_name}_{}_key", self.columns.join("_"))
        }
    }
}

#[derive(Debug, Clone)]
struct AttachedKey {
    constraint: PgConstraintRow,
    index_oid: u32,
}

struct PartitionedKeyInstaller<'a> {
    db: &'a Database,
    client_id: ClientId,
    xid: TransactionId,
    next_cid: &'a mut CommandId,
    configured_search_path: Option<&'a [String]>,
    gucs: Option<&'a HashMap<String, String>>,
    catalog_effects: &'a mut Vec<CatalogMutationEffect>,
    interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
    relation_cache: RefCell<BTreeMap<u32, BoundRelation>>,
    relation_name_cache: RefCell<BTreeMap<u32, String>>,
    direct_partition_children_cache: RefCell<BTreeMap<u32, Vec<BoundRelation>>>,
}

impl<'a> PartitionedKeyInstaller<'a> {
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

    fn current_relation(&self, relation_oid: u32) -> Result<BoundRelation, ExecError> {
        if let Some(relation) = self.relation_cache.borrow().get(&relation_oid).cloned() {
            return Ok(relation);
        }
        let relation = self
            .db
            .lazy_catalog_lookup(
                self.client_id,
                Some((self.xid, self.visible_cid())),
                self.configured_search_path,
            )
            .relation_by_oid(relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        self.relation_cache
            .borrow_mut()
            .insert(relation_oid, relation.clone());
        Ok(relation)
    }

    fn relation_name(&self, relation_oid: u32) -> Result<String, ExecError> {
        if let Some(name) = self
            .relation_name_cache
            .borrow()
            .get(&relation_oid)
            .cloned()
        {
            return Ok(name);
        }
        let name = self
            .db
            .lazy_catalog_lookup(
                self.client_id,
                Some((self.xid, self.visible_cid())),
                self.configured_search_path,
            )
            .class_row_by_oid(relation_oid)
            .map(|row| row.relname)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        self.relation_name_cache
            .borrow_mut()
            .insert(relation_oid, name.clone());
        Ok(name)
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

    fn direct_partition_parent(
        &self,
        relation: &BoundRelation,
    ) -> Result<Option<BoundRelation>, ExecError> {
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let parent_oid = catalog
            .inheritance_parents(relation.relation_oid)
            .into_iter()
            .filter(|row| !row.inhdetachpending)
            .find_map(|row| {
                catalog
                    .relation_by_oid(row.inhparent)
                    .filter(|parent| parent.partitioned_table.is_some())
                    .map(|parent| parent.relation_oid)
            });
        parent_oid.map(|oid| self.current_relation(oid)).transpose()
    }

    fn direct_partition_children(
        &self,
        relation_oid: u32,
    ) -> Result<Vec<BoundRelation>, ExecError> {
        if let Some(children) = self
            .direct_partition_children_cache
            .borrow()
            .get(&relation_oid)
            .cloned()
        {
            return Ok(children);
        }
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let mut inherits = catalog.inheritance_children(relation_oid);
        inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        let children = inherits
            .into_iter()
            .filter(|row| !row.inhdetachpending)
            .filter_map(|row| {
                catalog
                    .relation_by_oid(row.inhrelid)
                    .map(|child| (child.relispartition, child))
            })
            .filter(|(is_partition, _)| *is_partition)
            .map(|(_, child)| child)
            .collect::<Vec<_>>();
        self.direct_partition_children_cache
            .borrow_mut()
            .insert(relation_oid, children.clone());
        Ok(children)
    }

    fn direct_partition_children_include_foreign_table(
        &self,
        relation_oid: u32,
    ) -> Result<bool, ExecError> {
        Ok(self
            .direct_partition_children(relation_oid)?
            .into_iter()
            .any(|child| child.relkind == 'f'))
    }

    fn column_attnums_for_names(
        desc: &RelationDesc,
        columns: &[String],
    ) -> Result<Vec<i16>, ExecError> {
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
                    .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
            })
            .collect()
    }

    fn column_names_for_attnums(
        desc: &RelationDesc,
        attnums: &[i16],
    ) -> Result<Vec<String>, ExecError> {
        attnums
            .iter()
            .map(|attnum| {
                usize::try_from(attnum.saturating_sub(1))
                    .ok()
                    .and_then(|index| desc.columns.get(index))
                    .filter(|column| !column.dropped)
                    .map(|column| column.name.clone())
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "constraint column",
                            actual: format!("invalid attnum {attnum}"),
                        })
                    })
            })
            .collect()
    }

    fn choose_available_constraint_name(base: &str, used_names: &mut BTreeSet<String>) -> String {
        if used_names.insert(base.to_ascii_lowercase()) {
            return base.to_string();
        }
        for suffix in 1.. {
            let candidate = format!("{base}{suffix}");
            if used_names.insert(candidate.to_ascii_lowercase()) {
                return candidate;
            }
        }
        unreachable!("constraint name suffix space exhausted")
    }

    fn generated_constraint_name(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
    ) -> Result<String, ExecError> {
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let relation_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let mut used_names = relation_constraints
            .iter()
            .map(|row| row.conname.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let relation_name = self.relation_name(relation.relation_oid)?;
        Ok(Self::choose_available_constraint_name(
            &spec.default_constraint_name(&relation_name),
            &mut used_names,
        ))
    }

    fn resolve_build_options(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
    ) -> Result<(u32, u32, CatalogIndexBuildOptions, Vec<IndexColumnDef>), ExecError> {
        let index_columns = spec.index_columns();
        let (access_method_oid, access_method_handler, build_options) =
            if spec.without_overlaps.is_some() {
                self.db.resolve_temporal_index_build_options(
                    self.client_id,
                    Some((self.xid, self.visible_cid())),
                    relation,
                    &index_columns,
                )?
            } else {
                self.db.resolve_simple_index_build_options(
                    self.client_id,
                    Some((self.xid, self.visible_cid())),
                    "btree",
                    relation,
                    &index_columns,
                    &[],
                )?
            };
        Ok((
            access_method_oid,
            access_method_handler,
            CatalogIndexBuildOptions {
                indimmediate: !spec.deferrable,
                ..build_options
            },
            index_columns,
        ))
    }

    fn matching_key_on_relation(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
        build_options: &CatalogIndexBuildOptions,
        expected_index_relkind: char,
        parent_constraint_oid: Option<u32>,
        local_only: bool,
    ) -> Result<Option<AttachedKey>, ExecError> {
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let expected_attnums = Self::column_attnums_for_names(&relation.desc, &spec.columns)?;
        let mut rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        sort_pg_constraint_rows(&mut rows);
        for row in rows {
            if row.contype != spec.constraint_type()
                || row.conindid == 0
                || row.conkey.as_deref() != Some(expected_attnums.as_slice())
            {
                continue;
            }
            if local_only && (row.conparentid != 0 || row.coninhcount != 0) {
                continue;
            }
            if let Some(parent_constraint_oid) = parent_constraint_oid
                && row.conparentid != parent_constraint_oid
            {
                continue;
            }
            let Some(index_relation) = catalog.relation_by_oid(row.conindid) else {
                continue;
            };
            if index_relation.relkind != expected_index_relkind {
                continue;
            }
            let Some(index) = catalog
                .index_relations_for_heap(relation.relation_oid)
                .into_iter()
                .find(|index| index.relation_oid == row.conindid)
            else {
                continue;
            };
            let Some(class_row) = catalog.class_row_by_oid(row.conindid) else {
                continue;
            };
            if class_row.relam != build_options.am_oid
                || index.index_meta.indisprimary != spec.primary
                || !index.index_meta.indisunique
                || index.index_meta.indimmediate != !spec.deferrable
                || index.index_meta.indisexclusion != spec.without_overlaps.is_some()
                || index.index_meta.indnullsnotdistinct != spec.nulls_not_distinct
                || index.index_meta.indkey != expected_attnums
                || index.index_meta.indexprs.is_some()
                || index.index_meta.indpred.is_some()
                || index.index_meta.indclass != build_options.indclass
                || index.index_meta.indcollation != build_options.indcollation
                || index.index_meta.indoption != build_options.indoption
                || row.condeferrable != spec.deferrable
                || row.condeferred != spec.initially_deferred
                || row.conperiod != spec.without_overlaps.is_some()
            {
                continue;
            }
            return Ok(Some(AttachedKey {
                constraint: row,
                index_oid: index.relation_oid,
            }));
        }
        Ok(None)
    }

    fn find_existing_key(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
        parent_constraint_oid: Option<u32>,
    ) -> Result<Option<AttachedKey>, ExecError> {
        let (_, _, build_options, _) = self.resolve_build_options(relation, spec)?;
        self.matching_key_on_relation(
            relation,
            spec,
            &build_options,
            if relation.relkind == 'p' { 'I' } else { 'i' },
            parent_constraint_oid,
            false,
        )
    }

    fn find_local_key(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
    ) -> Result<Option<AttachedKey>, ExecError> {
        let (_, _, build_options, _) = self.resolve_build_options(relation, spec)?;
        self.matching_key_on_relation(
            relation,
            spec,
            &build_options,
            if relation.relkind == 'p' { 'I' } else { 'i' },
            None,
            true,
        )
    }

    fn ensure_primary_key_not_nulls(
        &mut self,
        relation: &BoundRelation,
        relation_name: &str,
        spec: &PartitionedKeySpec,
    ) -> Result<Vec<u32>, ExecError> {
        if !spec.primary {
            return Ok(Vec::new());
        }
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let relation_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let mut used_names = relation_constraints
            .iter()
            .map(|row| row.conname.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let mut primary_key_owned_not_null_oids = Vec::new();
        for column_name in &spec.columns {
            let Some(column_index) =
                relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                            .then_some(index)
                    })
            else {
                return Err(ExecError::Parse(ParseError::UnknownColumn(
                    column_name.clone(),
                )));
            };
            if !relation.desc.columns[column_index].storage.nullable {
                if let Some(row) = relation_constraints.iter().find(|row| {
                    row.contype == CONSTRAINT_NOTNULL
                        && row
                            .conkey
                            .as_ref()
                            .is_some_and(|keys| keys.contains(&((column_index + 1) as i16)))
                }) {
                    verify_not_null_pk_compatible(
                        row,
                        &relation.desc.columns[column_index].name,
                        relation_name,
                    )?;
                }
                continue;
            }
            let not_null_name = Self::choose_available_constraint_name(
                &format!("{relation_name}_{column_name}_not_null"),
                &mut used_names,
            );
            if relation.relkind == 'r' {
                validate_not_null_rows(
                    self.db,
                    relation,
                    relation_name,
                    column_index,
                    &not_null_name,
                    &catalog,
                    self.client_id,
                    self.xid,
                    self.visible_cid(),
                    Arc::clone(&self.interrupts),
                )?;
            }
            let cid = self.take_cid();
            let ctx = self.write_ctx(cid);
            let (constraint_oid, effect) = self
                .db
                .catalog
                .write()
                .set_column_not_null_mvcc(
                    relation.relation_oid,
                    column_name,
                    not_null_name,
                    true,
                    false,
                    true,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_effect(effect)?;
            primary_key_owned_not_null_oids.push(constraint_oid);
        }
        Ok(primary_key_owned_not_null_oids)
    }

    fn create_partitioned_index(
        &mut self,
        relation: &BoundRelation,
        index_name: &str,
        spec: &PartitionedKeySpec,
        access_method_oid: u32,
        access_method_handler: u32,
        build_options: &CatalogIndexBuildOptions,
        index_columns: &[IndexColumnDef],
    ) -> Result<CatalogEntry, ExecError> {
        let default_tablespace = self
            .gucs
            .and_then(|gucs| gucs.get("default_tablespace"))
            .map(String::as_str)
            .filter(|name| !name.trim().is_empty());
        if spec
            .tablespace
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case("pg_default"))
            || (spec.tablespace.is_none()
                && default_tablespace.is_some_and(|name| name.eq_ignore_ascii_case("pg_default")))
        {
            return Err(ExecError::DetailedError {
                message: "cannot specify default tablespace for partitioned relations".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        let tablespace_oid = if relation.relpersistence == 't' {
            0
        } else {
            resolve_relation_tablespace_oid(
                self.db,
                self.client_id,
                Some((self.xid, self.visible_cid())),
                spec.tablespace.as_deref(),
                self.gucs,
            )?
        };
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let (mut index_entry, effect) = self
            .db
            .catalog
            .write()
            .create_partitioned_index_for_relation_mvcc_with_options(
                index_name.to_string(),
                relation.relation_oid,
                true,
                spec.primary,
                index_columns,
                build_options,
                None,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_effect(effect)?;
        if tablespace_oid != index_entry.rel.spc_oid {
            let cid = self.take_cid();
            let ctx = self.write_ctx(cid);
            let effect = self
                .db
                .catalog
                .write()
                .set_relation_tablespace_mvcc(index_entry.relation_oid, tablespace_oid, false, &ctx)
                .map_err(map_catalog_error)?;
            index_entry.rel = effect.created_rels.first().copied().unwrap_or_else(|| {
                let mut rel = index_entry.rel;
                rel.spc_oid = tablespace_oid;
                rel
            });
            self.apply_effect(effect)?;
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
                    of_type_oid: index_entry.of_type_oid,
                    row_type_oid: index_entry.row_type_oid,
                    array_type_oid: index_entry.array_type_oid,
                    reltoastrelid: index_entry.reltoastrelid,
                    relhasindex: false,
                    relpersistence: index_entry.relpersistence,
                    relkind: index_entry.relkind,
                    relispopulated: index_entry.relispopulated,
                    relhastriggers: index_entry.relhastriggers,
                    relispartition: index_entry.relispartition,
                    relpartbound: index_entry.relpartbound.clone(),
                    relrowsecurity: index_entry.relrowsecurity,
                    relforcerowsecurity: index_entry.relforcerowsecurity,
                    desc: index_entry.desc.clone(),
                    partitioned_table: index_entry.partitioned_table.clone(),
                    partition_spec: None,
                    index: Some(self.db.relcache_index_meta_from_catalog(
                        self.client_id,
                        Some((self.xid, cid)),
                        index_entry.relation_oid,
                        &temp_index_meta,
                        access_method_oid,
                        access_method_handler,
                    )?),
                },
                self.db
                    .temp_entry_on_commit(self.client_id, relation.relation_oid)
                    .unwrap_or(OnCommitAction::PreserveRows),
            )?;
        }
        Ok(index_entry)
    }

    fn create_index_inheritance(
        &mut self,
        child_index_oid: u32,
        parent_index_oid: u32,
    ) -> Result<(), ExecError> {
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let existing_parents = catalog.inheritance_parents(child_index_oid);
        if existing_parents
            .iter()
            .any(|row| row.inhparent == parent_index_oid)
        {
            return Ok(());
        }
        if !existing_parents.is_empty() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "index {} already has an inheritance parent",
                    child_index_oid
                ),
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
        let index = crate::backend::utils::cache::lsyscache::index_row_by_indexrelid(
            self.db,
            self.client_id,
            Some((self.xid, self.visible_cid())),
            index_oid,
        )
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index catalog row",
                actual: format!("missing pg_index row for {index_oid}"),
            })
        })?;
        if index.indisready && index.indisvalid == valid {
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

    fn validate_direct_partition_primary_key_not_nulls(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
    ) -> Result<(), ExecError> {
        if !spec.primary {
            return Ok(());
        }
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        for child in self.direct_partition_children(relation.relation_oid)? {
            let child_name = self.relation_name(child.relation_oid)?;
            let child_constraints = catalog.constraint_rows_for_relation(child.relation_oid);
            for column_name in &spec.columns {
                let Some((column_index, column)) =
                    child.desc.columns.iter().enumerate().find(|(_, column)| {
                        !column.dropped && column.name.eq_ignore_ascii_case(column_name)
                    })
                else {
                    continue;
                };
                if column.storage.nullable {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "column \"{}\" of table \"{}\" is not marked NOT NULL",
                            column.name, child_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if let Some(row) = child_constraints.iter().find(|row| {
                    row.contype == CONSTRAINT_NOTNULL
                        && row
                            .conkey
                            .as_ref()
                            .is_some_and(|key| key.as_slice() == [(column_index + 1) as i16])
                }) {
                    verify_not_null_pk_compatible(row, &column.name, &child_name)?;
                }
            }
        }
        Ok(())
    }

    fn relation_has_primary_key(&self, relation_oid: u32) -> bool {
        self.db
            .lazy_catalog_lookup(
                self.client_id,
                Some((self.xid, self.visible_cid())),
                self.configured_search_path,
            )
            .constraint_rows_for_relation(relation_oid)
            .into_iter()
            .any(|row| row.contype == CONSTRAINT_PRIMARY && row.conindid != 0)
    }

    fn multiple_primary_keys_error(&self, relation: &BoundRelation) -> ExecError {
        let relation_name = self
            .relation_name(relation.relation_oid)
            .unwrap_or_else(|_| relation.relation_oid.to_string());
        ExecError::Parse(ParseError::DetailedError {
            message: format!("multiple primary keys for table \"{relation_name}\" are not allowed"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn create_key_constraint(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        constraint_name: &str,
        spec: &PartitionedKeySpec,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
    ) -> Result<PgConstraintRow, ExecError> {
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let conexclop = if spec.without_overlaps.is_some() {
            let catalog = self.db.lazy_catalog_lookup(
                self.client_id,
                Some((self.xid, self.visible_cid())),
                self.configured_search_path,
            );
            Some(self.db.temporal_constraint_operator_oids_for_relation(
                relation_oid,
                &spec.columns,
                spec.without_overlaps.as_deref(),
                &catalog,
            )?)
        } else {
            None
        };
        let (constraint, effect) = self
            .db
            .catalog
            .write()
            .create_index_backed_constraint_mvcc_with_inheritance_and_period(
                relation_oid,
                index_oid,
                constraint_name.to_string(),
                spec.constraint_type(),
                primary_key_owned_not_null_oids,
                conparentid,
                conislocal,
                coninhcount,
                connoinherit,
                spec.without_overlaps.is_some(),
                conexclop,
                spec.deferrable,
                spec.initially_deferred,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_effect(effect)?;
        Ok(constraint)
    }

    fn update_key_constraint_inheritance(
        &mut self,
        relation_oid: u32,
        constraint_oid: u32,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
    ) -> Result<(), ExecError> {
        let cid = self.take_cid();
        let ctx = self.write_ctx(cid);
        let effect = self
            .db
            .catalog
            .write()
            .update_index_backed_constraint_inheritance_mvcc(
                relation_oid,
                constraint_oid,
                conparentid,
                conislocal,
                coninhcount,
                connoinherit,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_effect(effect)
    }

    fn parent_key_for_spec(
        &self,
        relation: &BoundRelation,
        spec: &PartitionedKeySpec,
    ) -> Result<Option<AttachedKey>, ExecError> {
        let Some(parent) = self.direct_partition_parent(relation)? else {
            return Ok(None);
        };
        self.find_existing_key(&parent, spec, None)
    }

    fn spec_from_constraint(
        &self,
        relation: &BoundRelation,
        constraint: &PgConstraintRow,
    ) -> Result<PartitionedKeySpec, ExecError> {
        let attnums = constraint.conkey.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "constraint columns",
                actual: format!("missing conkey for constraint {}", constraint.conname),
            })
        })?;
        let catalog = self.db.lazy_catalog_lookup(
            self.client_id,
            Some((self.xid, self.visible_cid())),
            self.configured_search_path,
        );
        let index = catalog
            .index_relations_for_heap(relation.relation_oid)
            .into_iter()
            .find(|index| index.relation_oid == constraint.conindid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "matching key index",
                    actual: format!("missing index {}", constraint.conindid),
                })
            })?;
        Ok(PartitionedKeySpec {
            columns: Self::column_names_for_attnums(&relation.desc, &attnums)?,
            primary: constraint.contype == CONSTRAINT_PRIMARY,
            nulls_not_distinct: index.index_meta.indnullsnotdistinct,
            without_overlaps: constraint
                .conperiod
                .then(|| Self::column_names_for_attnums(&relation.desc, &attnums).ok())
                .flatten()
                .and_then(|columns| columns.last().cloned()),
            tablespace: None,
            deferrable: constraint.condeferrable,
            initially_deferred: constraint.condeferred,
        })
    }

    fn reconcile_relation_key_tree(
        &mut self,
        relation: BoundRelation,
        parent: Option<&AttachedKey>,
        spec: &PartitionedKeySpec,
        desired_constraint_name: Option<&str>,
        recurse: bool,
    ) -> Result<AttachedKey, ExecError> {
        let relation = self.current_relation(relation.relation_oid)?;
        if parent.is_none() && relation.relkind == 'p' && !recurse {
            self.validate_direct_partition_primary_key_not_nulls(&relation, spec)?;
        }
        if relation.relkind == 'p' {
            let partition_spec = crate::backend::parser::relation_partition_spec(&relation)
                .map_err(ExecError::Parse)?;
            crate::backend::parser::validate_partitioned_index_backed_constraints(
                &self.relation_name(relation.relation_oid)?,
                Some(&partition_spec),
                &[IndexBackedConstraintAction {
                    constraint_name: desired_constraint_name.map(str::to_string),
                    existing_index_name: None,
                    columns: spec.columns.clone(),
                    index_columns: spec
                        .columns
                        .iter()
                        .cloned()
                        .map(crate::backend::parser::IndexColumnDef::from)
                        .collect(),
                    include_columns: Vec::new(),
                    primary: spec.primary,
                    exclusion: false,
                    nulls_not_distinct: spec.nulls_not_distinct,
                    without_overlaps: spec.without_overlaps.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    tablespace: None,
                    deferrable: spec.deferrable,
                    initially_deferred: spec.initially_deferred,
                }],
            )
            .map_err(ExecError::Parse)?;
        }
        if let Some(parent) = parent
            && let Some(existing) =
                self.find_existing_key(&relation, spec, Some(parent.constraint.oid))?
        {
            if relation.relkind == 'p' {
                for child in self.direct_partition_children(relation.relation_oid)? {
                    let _ =
                        self.reconcile_relation_key_tree(child, Some(&existing), spec, None, true)?;
                }
            }
            return Ok(existing);
        }

        if let Some(parent) = parent
            && let Some(existing) = self.find_local_key(&relation, spec)?
        {
            self.create_index_inheritance(existing.index_oid, parent.index_oid)?;
            self.update_key_constraint_inheritance(
                relation.relation_oid,
                existing.constraint.oid,
                parent.constraint.oid,
                false,
                1,
                true,
            )?;
            let attached = AttachedKey {
                constraint: PgConstraintRow {
                    conparentid: parent.constraint.oid,
                    conislocal: false,
                    coninhcount: 1,
                    connoinherit: true,
                    ..existing.constraint
                },
                index_oid: existing.index_oid,
            };
            if relation.relkind == 'p' {
                for child in self.direct_partition_children(relation.relation_oid)? {
                    let _ =
                        self.reconcile_relation_key_tree(child, Some(&attached), spec, None, true)?;
                }
            }
            return Ok(attached);
        }

        if parent.is_none()
            && let Some(existing) = self.find_existing_key(&relation, spec, None)?
        {
            if relation.relkind == 'p' && recurse {
                for child in self.direct_partition_children(relation.relation_oid)? {
                    let _ =
                        self.reconcile_relation_key_tree(child, Some(&existing), spec, None, true)?;
                }
            }
            return Ok(existing);
        }

        if parent.is_some() && spec.primary && self.relation_has_primary_key(relation.relation_oid)
        {
            return Err(self.multiple_primary_keys_error(&relation));
        }

        let relation_name = self.relation_name(relation.relation_oid)?;
        let primary_key_owned_not_null_oids =
            self.ensure_primary_key_not_nulls(&relation, &relation_name, spec)?;
        let (access_method_oid, access_method_handler, build_options, index_columns) =
            self.resolve_build_options(&relation, spec)?;
        let constraint_name = match desired_constraint_name {
            Some(name) => name.to_string(),
            None => self.generated_constraint_name(&relation, spec)?,
        };
        let index_name = self.db.choose_available_relation_name(
            self.client_id,
            self.xid,
            self.visible_cid(),
            relation.namespace_oid,
            &constraint_name,
        )?;

        let attached = if relation.relkind == 'p' {
            if self.direct_partition_children_include_foreign_table(relation.relation_oid)? {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot create unique index on partitioned table \"{}\"",
                        relation_name
                    ),
                    detail: Some(format!(
                        "Table \"{}\" contains partitions that are foreign tables.",
                        relation_name
                    )),
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            let index_entry = self.create_partitioned_index(
                &relation,
                &index_name,
                spec,
                access_method_oid,
                access_method_handler,
                &build_options,
                &index_columns,
            )?;
            let (conparentid, conislocal, coninhcount, connoinherit) = if let Some(parent) = parent
            {
                self.create_index_inheritance(index_entry.relation_oid, parent.index_oid)?;
                (parent.constraint.oid, false, 1, false)
            } else {
                (0, true, 0, true)
            };
            let constraint = self.create_key_constraint(
                relation.relation_oid,
                index_entry.relation_oid,
                &constraint_name,
                spec,
                &primary_key_owned_not_null_oids,
                conparentid,
                conislocal,
                coninhcount,
                connoinherit,
            )?;
            AttachedKey {
                constraint,
                index_oid: index_entry.relation_oid,
            }
        } else {
            let index_cid = self.take_cid_span(2);
            let visible_catalog = Some(crate::backend::executor::executor_catalog(
                self.db.lazy_catalog_lookup(
                    self.client_id,
                    Some((self.xid, self.visible_cid())),
                    self.configured_search_path,
                ),
            ));
            let index_entry = self.db.build_simple_index_in_transaction(
                self.client_id,
                &relation,
                &index_name,
                visible_catalog,
                &index_columns,
                None,
                true,
                spec.primary,
                spec.nulls_not_distinct,
                if relation.relpersistence == 't' {
                    None
                } else {
                    Some(resolve_relation_tablespace_oid(
                        self.db,
                        self.client_id,
                        Some((self.xid, index_cid)),
                        spec.tablespace.as_deref(),
                        self.gucs,
                    )?)
                },
                self.xid,
                index_cid,
                access_method_oid,
                access_method_handler,
                &build_options,
                65_536,
                false,
                true,
                self.catalog_effects,
            )?;
            let (conparentid, conislocal, coninhcount, connoinherit) = if let Some(parent) = parent
            {
                self.create_index_inheritance(index_entry.relation_oid, parent.index_oid)?;
                (parent.constraint.oid, false, 1, false)
            } else {
                (0, true, 0, true)
            };
            let constraint = self.create_key_constraint(
                relation.relation_oid,
                index_entry.relation_oid,
                &constraint_name,
                spec,
                &primary_key_owned_not_null_oids,
                conparentid,
                conislocal,
                coninhcount,
                connoinherit,
            )?;
            AttachedKey {
                constraint,
                index_oid: index_entry.relation_oid,
            }
        };

        if relation.relkind == 'p' {
            if recurse {
                for child in self.direct_partition_children(relation.relation_oid)? {
                    let _ =
                        self.reconcile_relation_key_tree(child, Some(&attached), spec, None, true)?;
                }
            } else if !self
                .direct_partition_children(relation.relation_oid)?
                .is_empty()
            {
                self.set_index_valid(attached.index_oid, false)?;
            }
        }
        Ok(attached)
    }
}

impl Database {
    pub(super) fn install_partitioned_index_backed_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        start_cid: CommandId,
        relation: &BoundRelation,
        actions: &[IndexBackedConstraintAction],
        recurse: bool,
        configured_search_path: Option<&[String]>,
        gucs: Option<&HashMap<String, String>>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = start_cid;
        let mut installer = PartitionedKeyInstaller {
            db: self,
            client_id,
            xid,
            next_cid: &mut next_cid,
            configured_search_path,
            gucs,
            catalog_effects,
            interrupts,
            relation_cache: RefCell::new(BTreeMap::new()),
            relation_name_cache: RefCell::new(BTreeMap::new()),
            direct_partition_children_cache: RefCell::new(BTreeMap::new()),
        };
        for action in actions {
            let relation = installer.current_relation(relation.relation_oid)?;
            if relation.relkind == 'p' {
                let partition_spec = crate::backend::parser::relation_partition_spec(&relation)
                    .map_err(ExecError::Parse)?;
                crate::backend::parser::validate_partitioned_index_backed_constraints(
                    &installer.relation_name(relation.relation_oid)?,
                    Some(&partition_spec),
                    std::slice::from_ref(action),
                )
                .map_err(ExecError::Parse)?;
            }
            let spec = PartitionedKeySpec::from_action(action);
            let parent = installer.parent_key_for_spec(&relation, &spec)?;
            let _ = installer.reconcile_relation_key_tree(
                relation,
                parent.as_ref(),
                &spec,
                action.constraint_name.as_deref(),
                recurse,
            )?;
        }
        Ok(next_cid)
    }

    pub(super) fn reconcile_partitioned_parent_keys_for_attached_child_in_transaction(
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
        let mut installer = PartitionedKeyInstaller {
            db: self,
            client_id,
            xid,
            next_cid: &mut next_cid,
            configured_search_path,
            gucs: None,
            catalog_effects,
            interrupts,
            relation_cache: RefCell::new(BTreeMap::new()),
            relation_name_cache: RefCell::new(BTreeMap::new()),
            direct_partition_children_cache: RefCell::new(BTreeMap::new()),
        };
        let parent = installer.current_relation(parent_relation_oid)?;
        let child = installer.current_relation(child_relation_oid)?;
        let catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, installer.visible_cid())),
            configured_search_path,
        );
        let mut parent_constraints = catalog
            .constraint_rows_for_relation(parent_relation_oid)
            .into_iter()
            .filter(|row| {
                matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE) && row.conindid != 0
            })
            .collect::<Vec<_>>();
        sort_pg_constraint_rows(&mut parent_constraints);
        for constraint in parent_constraints {
            let spec = installer.spec_from_constraint(&parent, &constraint)?;
            let index_oid = constraint.conindid;
            let parent_key = AttachedKey {
                constraint,
                index_oid,
            };
            let _ = installer.reconcile_relation_key_tree(
                child.clone(),
                Some(&parent_key),
                &spec,
                None,
                true,
            )?;
        }
        Ok(next_cid)
    }
}
