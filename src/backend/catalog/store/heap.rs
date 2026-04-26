use std::collections::{BTreeMap, BTreeSet};

use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, CatalogIndexMeta,
    allocate_relation_object_oids,
};
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible_in_db;
use crate::backend::catalog::persistence::{
    delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
};
use crate::backend::catalog::pg_constraint::{derived_pg_constraint_rows, sort_pg_constraint_rows};
use crate::backend::catalog::pg_depend::{
    aggregate_depend_rows, derived_pg_depend_rows, foreign_data_wrapper_depend_rows,
    foreign_key_constraint_depend_rows, index_backed_constraint_depend_rows,
    inheritance_depend_rows, operator_depend_rows, primary_key_owned_not_null_depend_rows,
    proc_depend_rows, publication_namespace_depend_rows, publication_rel_depend_rows,
    relation_constraint_depend_rows, relation_rule_depend_rows, sort_pg_depend_rows,
    statistic_ext_depend_rows, trigger_depend_rows, view_rewrite_depend_rows,
};
use crate::backend::catalog::rowcodec::{pg_cast_row_from_values, pg_description_row_from_values};
use crate::backend::catalog::rows::{
    PhysicalCatalogRows, create_composite_type_sync_kinds, create_index_sync_kinds,
    create_table_sync_kinds, create_view_sync_kinds, drop_relation_delete_kinds,
    drop_relation_sync_kinds, extend_physical_catalog_rows,
    physical_catalog_rows_for_catalog_entry,
};
use crate::backend::catalog::state::validate_builtin_type_rows;
use crate::backend::catalog::toasting::{
    PG_TOAST_NAMESPACE, ToastCatalogChanges, new_relation_create_toast_table,
    relation_needs_toast_table, toast_index_name, toast_relation_desc, toast_relation_name,
};
use crate::backend::executor::{ColumnDesc, RelationDesc};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name, sql_type_oid};
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::utils::cache::syscache::{SysCacheId, SysCacheTuple};
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    BootstrapCatalogKind, CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY,
    CONSTRAINT_UNIQUE, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_AM_RELATION_OID,
    PG_AMOP_RELATION_OID, PG_AMPROC_RELATION_OID, PG_ATTRDEF_RELATION_OID, PG_AUTHID_RELATION_OID,
    PG_CAST_RELATION_OID, PG_CLASS_RELATION_OID, PG_CONSTRAINT_RELATION_OID,
    PG_FOREIGN_DATA_WRAPPER_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_OPCLASS_RELATION_OID,
    PG_OPERATOR_RELATION_OID, PG_OPFAMILY_RELATION_OID, PG_POLICY_RELATION_OID,
    PG_PROC_RELATION_OID, PG_PUBLICATION_NAMESPACE_RELATION_OID, PG_PUBLICATION_REL_RELATION_OID,
    PG_PUBLICATION_RELATION_OID, PG_REWRITE_RELATION_OID, PG_STATISTIC_EXT_RELATION_OID,
    PG_TRIGGER_RELATION_OID, PG_TYPE_RELATION_OID, PgAggregateRow, PgAmopRow, PgAmprocRow,
    PgAttrdefRow, PgAttributeRow, PgCastRow, PgClassRow, PgConstraintRow, PgDatabaseRow,
    PgDependRow, PgDescriptionRow, PgForeignDataWrapperRow, PgInheritsRow, PgNamespaceRow,
    PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow,
    PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow, PgRewriteRow,
    PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTablespaceRow, PgTypeRow,
    relkind_has_storage,
};
use crate::include::nodes::datum::Value;

use super::{
    CatalogControl, CatalogMutationEffect, CatalogStore, CatalogStoreMode, CatalogWriteContext,
    CreateTableResult, RuleOwnerDependency,
};

const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
const PG_CAST_OID_INDEX_OID: u32 = 2660;
const PG_CAST_SOURCE_TARGET_INDEX_OID: u32 = 2661;
const PG_NAMESPACE_OID_INDEX_OID: u32 = 2685;
const PG_STATISTIC_RELID_ATT_INH_INDEX_OID: u32 = 2696;

impl CatalogStore {
    pub fn create_relation_mvcc_with_relkind(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        relkind: char,
        owner_oid: u32,
        reloptions: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        if self
            .get_relname_relid(ctx, &syscache_relname(&name), namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let mut entry = build_relation_entry(
            name.clone(),
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            relkind,
            owner_oid,
            0,
            &mut control,
        )?;
        entry.reloptions = reloptions;
        let kinds = create_table_sync_kinds(&entry);
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            rows_for_new_relation_entry(&type_lookup, &name, &entry)?
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.created_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
    }

    #[cfg(test)]
    pub fn create_database_row_direct(
        &mut self,
        mut row: PgDatabaseRow,
    ) -> Result<PgDatabaseRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut control = self.control_state()?;
            if self
                .catalog
                .databases
                .iter()
                .any(|existing| existing.datname.eq_ignore_ascii_case(&row.datname))
            {
                return Err(CatalogError::UniqueViolation(
                    "pg_database_datname_index".into(),
                ));
            }
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows::default(),
                &PhysicalCatalogRows {
                    databases: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgDatabase],
            )?;
            self.catalog.databases.push(row.clone());
            self.catalog
                .databases
                .sort_by_key(|existing| (existing.oid, existing.datname.clone()));
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        if catalog
            .databases
            .iter()
            .any(|existing| existing.datname.eq_ignore_ascii_case(&row.datname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_database_datname_index".into(),
            ));
        }
        if row.oid == 0 {
            row.oid = catalog.next_oid();
        }
        catalog.next_oid = catalog.next_oid.max(row.oid.saturating_add(1));
        catalog.databases.push(row.clone());
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &PhysicalCatalogRows {
                databases: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgDatabase],
        )?;
        self.catalog = catalog.clone();
        self.control.next_oid = catalog.next_oid;
        self.control.next_rel_number = catalog.next_rel_number;
        Ok(row)
    }

    #[cfg(test)]
    pub fn drop_database_row_direct(&mut self, name: &str) -> Result<PgDatabaseRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut databases = self.catalog.databases.clone();
            let control = self.control_state()?;
            let position = databases
                .iter()
                .position(|row| row.datname.eq_ignore_ascii_case(name))
                .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
            let row = databases.remove(position);
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    databases: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows::default(),
                &[BootstrapCatalogKind::PgDatabase],
            )?;
            self.catalog.databases = databases;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let position = catalog
            .databases
            .iter()
            .position(|row| row.datname.eq_ignore_ascii_case(name))
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        let row = catalog.databases.remove(position);
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                databases: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows::default(),
            &[BootstrapCatalogKind::PgDatabase],
        )?;
        self.catalog = catalog.clone();
        self.control.next_oid = catalog.next_oid;
        self.control.next_rel_number = catalog.next_rel_number;
        Ok(row)
    }

    pub fn create_database_row_mvcc(
        &mut self,
        mut row: PgDatabaseRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgDatabaseRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        if catcache
            .database_rows()
            .iter()
            .any(|existing| existing.datname.eq_ignore_ascii_case(&row.datname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_database_datname_index".into(),
            ));
        }
        row.oid = self.allocate_next_oid(row.oid)?;
        let kinds = [BootstrapCatalogKind::PgDatabase];
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                databases: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((row, effect))
    }

    pub fn drop_database_row_mvcc(
        &mut self,
        name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgDatabaseRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let row = catcache
            .database_rows()
            .into_iter()
            .find(|row| row.datname.eq_ignore_ascii_case(name))
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        let kinds = [BootstrapCatalogKind::PgDatabase];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                databases: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((row, effect))
    }

    pub fn replace_database_row_mvcc(
        &mut self,
        row: PgDatabaseRow,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let existing = catcache
            .database_rows()
            .into_iter()
            .find(|existing| existing.oid == row.oid)
            .ok_or_else(|| CatalogError::UnknownTable(row.oid.to_string()))?;
        if existing.datname != row.datname
            && catcache.database_rows().into_iter().any(|candidate| {
                candidate.oid != row.oid && candidate.datname.eq_ignore_ascii_case(&row.datname)
            })
        {
            return Err(CatalogError::UniqueViolation(
                "pg_database_datname_index".into(),
            ));
        }

        let kinds = [BootstrapCatalogKind::PgDatabase];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                databases: vec![existing],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                databases: vec![row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok(effect)
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into();
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let catcache = self.catcache()?;
            let relcache = self.relcache()?;
            if relcache.get_by_name(&name).is_some() {
                return Err(CatalogError::TableAlreadyExists(
                    normalize_catalog_name(&name).to_ascii_lowercase(),
                ));
            }

            let mut control = self.control_state()?;
            let entry = build_relation_entry(
                name.clone(),
                desc,
                crate::include::catalog::PUBLIC_NAMESPACE_OID,
                self.scope_db_oid(),
                'p',
                'r',
                crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                0,
                &mut control,
            )?;
            let toast = build_toast_catalog_changes(
                &name,
                &entry,
                PG_TOAST_NAMESPACE,
                crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                &mut control,
            )?;
            let entry = toast
                .as_ref()
                .map(|changes| changes.new_parent.clone())
                .unwrap_or(entry);
            let mut kinds = create_table_sync_kinds(&entry);
            let mut rows_to_insert = rows_for_new_relation_entry(&catcache, &name, &entry)?;
            if let Some(toast) = &toast {
                merge_catalog_kinds(&mut kinds, &create_table_sync_kinds(&toast.toast_entry));
                merge_catalog_kinds(&mut kinds, &create_index_sync_kinds());
                extend_physical_catalog_rows(
                    &mut rows_to_insert,
                    rows_for_new_relation_entry(&catcache, &toast.toast_name, &toast.toast_entry)?,
                );
                extend_physical_catalog_rows(
                    &mut rows_to_insert,
                    rows_for_new_relation_entry(&catcache, &toast.index_name, &toast.index_entry)?,
                );
                rows_to_insert.depends.push(PgDependRow {
                    classid: PG_CLASS_RELATION_OID,
                    objid: toast.toast_entry.relation_oid,
                    objsubid: 0,
                    refclassid: PG_CLASS_RELATION_OID,
                    refobjid: entry.relation_oid,
                    refobjsubid: 0,
                    deptype: crate::include::catalog::DEPENDENCY_INTERNAL,
                });
                sort_pg_depend_rows(&mut rows_to_insert.depends);
            }
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows::default(),
                &rows_to_insert,
                &kinds,
            )?;
            self.control = control;
            return Ok(entry);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.create_table_with_options(
            name.clone(),
            desc,
            crate::include::catalog::PUBLIC_NAMESPACE_OID,
            self.scope_db_oid(),
            'p',
            crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        )?;
        let toast = new_relation_create_toast_table(
            &mut catalog,
            entry.relation_oid,
            crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
            crate::include::catalog::PG_TOAST_NAMESPACE_OID,
        )?;
        let entry = toast
            .as_ref()
            .map(|changes| changes.new_parent.clone())
            .unwrap_or(entry);
        let mut kinds = create_table_sync_kinds(&entry);
        let mut rows_to_insert = physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry);
        if let Some(toast) = &toast {
            merge_catalog_kinds(&mut kinds, &create_table_sync_kinds(&toast.toast_entry));
            merge_catalog_kinds(&mut kinds, &create_index_sync_kinds());
            add_catalog_entry_rows(
                &mut rows_to_insert,
                &catalog,
                &toast.toast_name,
                &toast.toast_entry,
            );
            add_catalog_entry_rows(
                &mut rows_to_insert,
                &catalog,
                &toast.index_name,
                &toast.index_entry,
            );
        }
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &rows_to_insert,
            &kinds,
        )?;
        Ok(entry)
    }

    pub fn create_index(
        &mut self,
        index_name: impl Into<String>,
        table_name: &str,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_index_with_flags(index_name, table_name, unique, false, columns)
    }

    pub fn create_index_with_flags(
        &mut self,
        index_name: impl Into<String>,
        table_name: &str,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        let index_name = index_name.into();
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let relation = self
                .relation(table_name)?
                .ok_or_else(|| CatalogError::UnknownTable(table_name.to_string()))?;
            if primary {
                return self.create_index_for_relation_with_flags(
                    index_name,
                    relation.relation_oid,
                    unique,
                    true,
                    columns,
                );
            }
            return self.create_index_for_relation_with_flags(
                index_name,
                relation.relation_oid,
                unique,
                false,
                columns,
            );
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = if primary {
            let table = catalog
                .get(table_name)
                .ok_or_else(|| CatalogError::UnknownTable(table_name.to_string()))?;
            catalog.create_index_for_relation_with_flags(
                index_name.clone(),
                table.relation_oid,
                unique,
                true,
                columns,
            )?
        } else {
            catalog.create_index(index_name.clone(), table_name, unique, columns)?
        };
        let kinds = create_index_sync_kinds();
        let rows_to_insert = physical_catalog_rows_for_catalog_entry(&catalog, &index_name, &entry);
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &rows_to_insert,
            &kinds,
        )?;
        Ok(entry)
    }

    pub fn create_index_for_relation(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_index_for_relation_with_flags(index_name, relation_oid, unique, false, columns)
    }

    pub fn create_index_for_relation_with_flags(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        let options = CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass: Vec::new(),
            indcollation: Vec::new(),
            indoption: Vec::new(),
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            brin_options: None,
            gin_options: None,
            hash_options: None,
        };
        self.create_index_for_relation_with_options(
            index_name,
            relation_oid,
            unique,
            primary,
            columns,
            &options,
        )
    }

    pub fn create_index_for_relation_with_options(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
    ) -> Result<CatalogEntry, CatalogError> {
        let index_name = index_name.into();
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let catcache = self.catcache()?;
            let relcache = self.relcache()?;
            if relcache.get_by_name(&index_name).is_some() {
                return Err(CatalogError::TableAlreadyExists(
                    normalize_catalog_name(&index_name).to_ascii_lowercase(),
                ));
            }
            let table = relcache
                .get_by_oid(relation_oid)
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            let table_entry = catalog_entry_from_visible_relation(&catcache, table)?;
            let mut control = self.control_state()?;
            let entry = build_index_entry(
                &catcache,
                index_name.clone(),
                &table_entry,
                unique,
                primary,
                columns,
                options,
                None,
                &mut control,
            )?;
            let kinds = create_index_sync_kinds();
            let rows_to_insert = rows_for_new_relation_entry(&catcache, &index_name, &entry)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows::default(),
                &rows_to_insert,
                &kinds,
            )?;
            self.control = control;
            return Ok(entry);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = if options.indclass.is_empty()
            && options.indcollation.is_empty()
            && options.indoption.is_empty()
        {
            catalog.create_index_for_relation_with_flags(
                index_name.clone(),
                relation_oid,
                unique,
                primary,
                columns,
            )?
        } else {
            catalog.create_index_for_relation_with_options_and_flags(
                index_name.clone(),
                relation_oid,
                unique,
                primary,
                columns,
                options,
                None,
            )?
        };
        let kinds = create_index_sync_kinds();
        let rows_to_insert = physical_catalog_rows_for_catalog_entry(&catalog, &index_name, &entry);
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &rows_to_insert,
            &kinds,
        )?;
        Ok(entry)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<Vec<CatalogEntry>, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let relcache = self.relcache()?;
            let entry = relcache
                .get_by_name(name)
                .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
            if !relkind_is_droppable_table(entry.relkind) {
                return Err(CatalogError::UnknownTable(name.to_string()));
            }
            return self.drop_relation_by_oid(entry.relation_oid);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog
            .get(name)
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        if !relkind_is_droppable_table(entry.relkind) {
            return Err(CatalogError::UnknownTable(name.to_string()));
        }
        let relation_oid = entry.relation_oid;
        self.drop_relation_entries(&mut catalog, relation_oid)
    }

    pub fn drop_relation_by_oid(
        &mut self,
        relation_oid: u32,
    ) -> Result<Vec<CatalogEntry>, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let catcache = self.catcache()?;
            let relcache = self.relcache()?;
            if has_nonpartition_inherited_children_visible(&catcache, relation_oid) {
                return Err(CatalogError::Corrupt(
                    "DROP TABLE with inherited children requires CASCADE, which is not supported yet",
                ));
            }
            let control = self.control_state()?;
            let (rows_to_delete, rows_to_insert, dropped, _affected_parent_oids) =
                drop_relation_entries_visible(&catcache, &relcache, relation_oid)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &rows_to_delete,
                &rows_to_insert,
                &drop_relation_sync_kinds(),
            )?;
            self.control = control;
            return Ok(dropped);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        self.drop_relation_entries(&mut catalog, relation_oid)
    }

    fn drop_relation_entries(
        &mut self,
        catalog: &mut Catalog,
        relation_oid: u32,
    ) -> Result<Vec<CatalogEntry>, CatalogError> {
        if has_nonpartition_inherited_children(catalog, relation_oid) {
            return Err(CatalogError::Corrupt(
                "DROP TABLE with inherited children requires CASCADE, which is not supported yet",
            ));
        }
        let oids = drop_relation_oids_by_oid(catalog, relation_oid)?;
        let dropped_entries = oids
            .iter()
            .copied()
            .map(|oid| {
                let name = catalog
                    .relation_name_by_oid(oid)
                    .ok_or_else(|| CatalogError::UnknownTable(oid.to_string()))?
                    .to_string();
                let entry = catalog
                    .get_by_oid(oid)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownTable(oid.to_string()))?;
                Ok::<_, CatalogError>((name, entry))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let dropped_oids = dropped_entries
            .iter()
            .map(|(_, entry)| entry.relation_oid)
            .collect::<BTreeSet<_>>();
        let affected_parent_oids = dropped_entries
            .iter()
            .flat_map(|(_, entry)| catalog.inheritance_parents(entry.relation_oid))
            .map(|row| row.inhparent)
            .filter(|parent_oid| !dropped_oids.contains(parent_oid))
            .collect::<BTreeSet<_>>();
        let affected_parent_entries = affected_parent_oids
            .iter()
            .copied()
            .map(|parent_oid| {
                let name = catalog
                    .relation_name_by_oid(parent_oid)
                    .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?
                    .to_string();
                let entry = catalog
                    .get_by_oid(parent_oid)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
                Ok::<_, CatalogError>((name, entry))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows_to_delete = PhysicalCatalogRows::default();
        for (name, entry) in &affected_parent_entries {
            add_catalog_entry_rows(&mut rows_to_delete, catalog, name, entry);
        }
        for (name, entry) in &dropped_entries {
            rows_to_delete
                .inherits
                .extend(catalog.inheritance_parents(entry.relation_oid));
            let mut entry_rows = physical_catalog_rows_for_catalog_entry(catalog, name, entry);
            entry_rows.inherits.clear();
            extend_physical_catalog_rows(&mut rows_to_delete, entry_rows);
        }

        for (_, entry) in &dropped_entries {
            let _ = catalog.detach_inheritance(entry.relation_oid);
        }
        let dropped = dropped_entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        for (_, entry) in &dropped_entries {
            let _ = catalog.remove_by_oid(entry.relation_oid);
        }

        let mut rows_to_insert = PhysicalCatalogRows::default();
        for (name, _) in &affected_parent_entries {
            let Some(entry) = catalog.get(name) else {
                continue;
            };
            add_catalog_entry_rows(&mut rows_to_insert, catalog, name, entry);
        }

        self.persist_catalog_row_changes(
            catalog,
            &rows_to_delete,
            &rows_to_insert,
            &drop_relation_sync_kinds(),
        )?;
        Ok(dropped)
    }

    pub fn create_table_mvcc(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CreateTableResult, CatalogMutationEffect), CatalogError> {
        self.create_table_mvcc_with_options(
            name,
            desc,
            crate::include::catalog::PUBLIC_NAMESPACE_OID,
            self.scope_db_oid(),
            'p',
            crate::include::catalog::PG_TOAST_NAMESPACE_OID,
            crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
            owner_oid,
            ctx,
        )
    }

    pub fn create_table_mvcc_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        toast_namespace_oid: u32,
        toast_namespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CreateTableResult, CatalogMutationEffect), CatalogError> {
        self.create_storage_relation_mvcc_with_options(
            name,
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            'r',
            toast_namespace_oid,
            toast_namespace_name,
            owner_oid,
            0,
            ctx,
        )
    }

    pub fn create_typed_table_mvcc_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        toast_namespace_oid: u32,
        toast_namespace_name: &str,
        owner_oid: u32,
        of_type_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CreateTableResult, CatalogMutationEffect), CatalogError> {
        self.create_storage_relation_mvcc_with_options(
            name,
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            'r',
            toast_namespace_oid,
            toast_namespace_name,
            owner_oid,
            of_type_oid,
            ctx,
        )
    }

    pub fn create_materialized_view_mvcc_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        toast_namespace_oid: u32,
        toast_namespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CreateTableResult, CatalogMutationEffect), CatalogError> {
        self.create_storage_relation_mvcc_with_options(
            name,
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            'm',
            toast_namespace_oid,
            toast_namespace_name,
            owner_oid,
            0,
            ctx,
        )
    }

    fn create_storage_relation_mvcc_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        relkind: char,
        toast_namespace_oid: u32,
        toast_namespace_name: &str,
        owner_oid: u32,
        of_type_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CreateTableResult, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        if self
            .get_relname_relid(ctx, &syscache_relname(&name), namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            name.clone(),
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            relkind,
            owner_oid,
            of_type_oid,
            &mut control,
        )?;
        let toast = build_toast_catalog_changes(
            &name,
            &entry,
            toast_namespace_name,
            toast_namespace_oid,
            &mut control,
        )?;
        let entry = toast
            .as_ref()
            .map(|changes| changes.new_parent.clone())
            .unwrap_or(entry);
        let mut kinds = create_table_sync_kinds(&entry);
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let mut rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            let mut rows = rows_for_new_relation_entry(&type_lookup, &name, &entry)?;
            if let Some(toast) = &toast {
                extend_physical_catalog_rows(
                    &mut rows,
                    rows_for_new_relation_entry(
                        &type_lookup,
                        &toast.toast_name,
                        &toast.toast_entry,
                    )?,
                );
                extend_physical_catalog_rows(
                    &mut rows,
                    rows_for_new_relation_entry(
                        &type_lookup,
                        &toast.index_name,
                        &toast.index_entry,
                    )?,
                );
            }
            rows
        };
        if let Some(toast) = &toast {
            rows.depends.push(PgDependRow {
                classid: PG_CLASS_RELATION_OID,
                objid: toast.toast_entry.relation_oid,
                objsubid: 0,
                refclassid: PG_CLASS_RELATION_OID,
                refobjid: entry.relation_oid,
                refobjsubid: 0,
                deptype: crate::include::catalog::DEPENDENCY_INTERNAL,
            });
            sort_pg_depend_rows(&mut rows.depends);
            merge_catalog_kinds(&mut kinds, &create_table_sync_kinds(&toast.toast_entry));
            merge_catalog_kinds(&mut kinds, &create_index_sync_kinds());
        }
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.created_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        if let Some(ref toast) = toast {
            record_toast_effects(&mut effect, toast);
        }
        Ok((CreateTableResult { entry, toast }, effect))
    }

    pub fn create_namespace_mvcc(
        &mut self,
        namespace_oid: u32,
        namespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let temp_namespace =
            namespace_name.starts_with("pg_temp_") || namespace_name.starts_with("pg_toast_temp_");
        let namespace_oid = if temp_namespace && namespace_oid != 0 {
            namespace_oid
        } else {
            self.allocate_next_oid(namespace_oid)?
        };
        let kinds = [BootstrapCatalogKind::PgNamespace];
        if !temp_namespace {
            self.invalidate_relcache_init_for_kinds(&kinds);
        }
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
                nspacl: None,
            }],
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn create_tablespace_mvcc(
        &mut self,
        tablespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let oid = self.allocate_next_oid(0)?;
        let kinds = [BootstrapCatalogKind::PgTablespace];

        let rows = PhysicalCatalogRows {
            tablespaces: vec![PgTablespaceRow {
                oid,
                spcname: tablespace_name.to_string(),
                spcowner: owner_oid,
            }],
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((oid, effect))
    }

    pub fn create_proc_mvcc(
        &mut self,
        mut row: PgProcRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        row.oid = self.allocate_next_oid(row.oid)?;
        let mut referenced_type_oids = parse_proc_argtype_oids(&row.proargtypes);
        if let Some(all_arg_types) = &row.proallargtypes {
            referenced_type_oids.extend(all_arg_types.iter().copied());
        }
        let kinds = [BootstrapCatalogKind::PgProc, BootstrapCatalogKind::PgDepend];
        let rows = PhysicalCatalogRows {
            procs: vec![row.clone()],
            depends: proc_depend_rows(
                row.oid,
                row.pronamespace,
                row.prorettype,
                &referenced_type_oids,
            ),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((row.oid, effect))
    }

    pub fn create_foreign_data_wrapper_mvcc(
        &mut self,
        mut row: PgForeignDataWrapperRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        row.oid = self.allocate_next_oid(row.oid)?;
        let kinds = [
            BootstrapCatalogKind::PgForeignDataWrapper,
            BootstrapCatalogKind::PgDepend,
        ];
        let rows = PhysicalCatalogRows {
            foreign_data_wrappers: vec![row.clone()],
            depends: foreign_data_wrapper_depend_rows(row.oid, row.fdwhandler, row.fdwvalidator),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok((row.oid, effect))
    }

    pub fn replace_foreign_data_wrapper_mvcc(
        &mut self,
        old_row: &PgForeignDataWrapperRow,
        mut row: PgForeignDataWrapperRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let kinds = [
            BootstrapCatalogKind::PgForeignDataWrapper,
            BootstrapCatalogKind::PgDepend,
        ];
        let old_rows = PhysicalCatalogRows {
            foreign_data_wrappers: vec![old_row.clone()],
            depends: foreign_data_wrapper_depend_rows(
                old_row.oid,
                old_row.fdwhandler,
                old_row.fdwvalidator,
            ),
            ..PhysicalCatalogRows::default()
        };
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;

        row.oid = old_row.oid;
        let new_rows = PhysicalCatalogRows {
            foreign_data_wrappers: vec![row.clone()],
            depends: foreign_data_wrapper_depend_rows(row.oid, row.fdwhandler, row.fdwvalidator),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok((row.oid, effect))
    }

    pub fn drop_foreign_data_wrapper_mvcc(
        &mut self,
        row: &PgForeignDataWrapperRow,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut kinds = vec![
            BootstrapCatalogKind::PgForeignDataWrapper,
            BootstrapCatalogKind::PgDepend,
        ];
        let description_rows = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &ctx.txns
                .read()
                .snapshot_for_command(ctx.xid, ctx.cid)
                .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?,
            ctx.client_id,
            self.scope_db_oid(),
            PG_DESCRIPTION_O_C_O_INDEX_OID,
            vec![
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 1,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(row.oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(PG_FOREIGN_DATA_WRAPPER_RELATION_OID)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 3,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int32(0),
                },
            ],
        )?
        .into_iter()
        .map(pg_description_row_from_values)
        .collect::<Result<Vec<_>, _>>()?;
        if !description_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDescription);
        }
        let rows = PhysicalCatalogRows {
            foreign_data_wrappers: vec![row.clone()],
            depends: foreign_data_wrapper_depend_rows(row.oid, row.fdwhandler, row.fdwvalidator),
            descriptions: description_rows,
            ..PhysicalCatalogRows::default()
        };
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok(effect)
    }

    pub fn comment_foreign_data_wrapper_mvcc(
        &mut self,
        fdw_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(fdw_oid, PG_FOREIGN_DATA_WRAPPER_RELATION_OID, comment, ctx)
    }

    pub fn comment_proc_mvcc(
        &mut self,
        proc_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(proc_oid, PG_PROC_RELATION_OID, comment, ctx)
    }

    pub fn comment_operator_mvcc(
        &mut self,
        operator_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(operator_oid, PG_OPERATOR_RELATION_OID, comment, ctx)
    }

    pub fn replace_proc_mvcc(
        &mut self,
        old_row: &PgProcRow,
        mut row: PgProcRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut old_referenced_type_oids = parse_proc_argtype_oids(&old_row.proargtypes);
        if let Some(all_arg_types) = &old_row.proallargtypes {
            old_referenced_type_oids.extend(all_arg_types.iter().copied());
        }
        let old_rows = PhysicalCatalogRows {
            procs: vec![old_row.clone()],
            depends: proc_depend_rows(
                old_row.oid,
                old_row.pronamespace,
                old_row.prorettype,
                &old_referenced_type_oids,
            ),
            ..PhysicalCatalogRows::default()
        };
        let kinds = [BootstrapCatalogKind::PgProc, BootstrapCatalogKind::PgDepend];
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;

        row.oid = old_row.oid;
        let mut referenced_type_oids = parse_proc_argtype_oids(&row.proargtypes);
        if let Some(all_arg_types) = &row.proallargtypes {
            referenced_type_oids.extend(all_arg_types.iter().copied());
        }
        let new_rows = PhysicalCatalogRows {
            procs: vec![row.clone()],
            depends: proc_depend_rows(
                row.oid,
                row.pronamespace,
                row.prorettype,
                &referenced_type_oids,
            ),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((row.oid, effect))
    }

    pub fn create_operator_mvcc(
        &mut self,
        mut row: PgOperatorRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values_without_relcache_invalidation(
            control.next_oid,
            control.next_rel_number,
        )?;
        self.control = control;

        let rows = PhysicalCatalogRows {
            operators: vec![row.clone()],
            depends: operator_depend_rows(&row),
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgOperator,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok((row.oid, effect))
    }

    pub fn replace_operator_mvcc(
        &mut self,
        old_row: &PgOperatorRow,
        mut row: PgOperatorRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let old_rows = PhysicalCatalogRows {
            operators: vec![old_row.clone()],
            depends: operator_depend_rows(old_row),
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgOperator,
            BootstrapCatalogKind::PgDepend,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;

        row.oid = old_row.oid;
        let new_rows = PhysicalCatalogRows {
            operators: vec![row.clone()],
            depends: operator_depend_rows(&row),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok((row.oid, effect))
    }

    pub fn drop_operator_by_oid_mvcc(
        &mut self,
        operator_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgOperatorRow, CatalogMutationEffect), CatalogError> {
        let row = operator_row_by_oid_mvcc(self, ctx, operator_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(operator_oid.to_string()))?;
        let rows = PhysicalCatalogRows {
            operators: vec![row.clone()],
            depends: operator_depend_rows(&row),
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgOperator,
            BootstrapCatalogKind::PgDepend,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.oid);
        Ok((row, effect))
    }

    pub fn create_aggregate_mvcc(
        &mut self,
        mut proc_row: PgProcRow,
        aggregate_row: PgAggregateRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        proc_row.oid = self.allocate_next_oid(proc_row.oid)?;
        let arg_type_oids = parse_proc_argtype_oids(&proc_row.proargtypes);
        let kinds = [
            BootstrapCatalogKind::PgProc,
            BootstrapCatalogKind::PgAggregate,
            BootstrapCatalogKind::PgDepend,
        ];
        let aggregate_row = PgAggregateRow {
            aggfnoid: proc_row.oid,
            ..aggregate_row
        };
        let rows = PhysicalCatalogRows {
            procs: vec![proc_row.clone()],
            aggregates: vec![aggregate_row.clone()],
            depends: aggregate_depend_rows(
                proc_row.oid,
                proc_row.pronamespace,
                proc_row.prorettype,
                &arg_type_oids,
                &aggregate_row,
            ),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((proc_row.oid, effect))
    }

    pub fn replace_aggregate_mvcc(
        &mut self,
        old_proc_row: &PgProcRow,
        old_aggregate_row: &PgAggregateRow,
        mut proc_row: PgProcRow,
        aggregate_row: PgAggregateRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let old_arg_type_oids = parse_proc_argtype_oids(&old_proc_row.proargtypes);
        let old_rows = PhysicalCatalogRows {
            procs: vec![old_proc_row.clone()],
            aggregates: vec![old_aggregate_row.clone()],
            depends: aggregate_depend_rows(
                old_proc_row.oid,
                old_proc_row.pronamespace,
                old_proc_row.prorettype,
                &old_arg_type_oids,
                old_aggregate_row,
            ),
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgProc,
            BootstrapCatalogKind::PgAggregate,
            BootstrapCatalogKind::PgDepend,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;

        proc_row.oid = old_proc_row.oid;
        let arg_type_oids = parse_proc_argtype_oids(&proc_row.proargtypes);
        let aggregate_row = PgAggregateRow {
            aggfnoid: proc_row.oid,
            ..aggregate_row
        };
        let new_rows = PhysicalCatalogRows {
            procs: vec![proc_row.clone()],
            aggregates: vec![aggregate_row.clone()],
            depends: aggregate_depend_rows(
                proc_row.oid,
                proc_row.pronamespace,
                proc_row.prorettype,
                &arg_type_oids,
                &aggregate_row,
            ),
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((proc_row.oid, effect))
    }

    pub fn drop_proc_by_oid_mvcc(
        &mut self,
        proc_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgProcRow, CatalogMutationEffect), CatalogError> {
        let proc_row = proc_row_by_oid_mvcc(self, ctx, proc_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(proc_oid.to_string()))?;
        let mut referenced_type_oids = parse_proc_argtype_oids(&proc_row.proargtypes);
        if let Some(all_arg_types) = &proc_row.proallargtypes {
            referenced_type_oids.extend(all_arg_types.iter().copied());
        }
        let aggregate_row = aggregate_row_by_fnoid_mvcc(self, ctx, proc_oid)?;
        let mut kinds = vec![BootstrapCatalogKind::PgProc, BootstrapCatalogKind::PgDepend];
        if aggregate_row.is_some() {
            kinds.push(BootstrapCatalogKind::PgAggregate);
        }
        let description_rows =
            description_rows_for_object_mvcc(self, ctx, proc_row.oid, PG_PROC_RELATION_OID, 0)?;
        if !description_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDescription);
        }
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                procs: vec![proc_row.clone()],
                aggregates: aggregate_row.clone().into_iter().collect(),
                depends: aggregate_row
                    .as_ref()
                    .map(|agg| {
                        aggregate_depend_rows(
                            proc_row.oid,
                            proc_row.pronamespace,
                            proc_row.prorettype,
                            &referenced_type_oids,
                            agg,
                        )
                    })
                    .unwrap_or_else(|| {
                        proc_depend_rows(
                            proc_row.oid,
                            proc_row.pronamespace,
                            proc_row.prorettype,
                            &referenced_type_oids,
                        )
                    }),
                descriptions: description_rows,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((proc_row, effect))
    }

    pub fn create_cast_mvcc(
        &mut self,
        mut row: PgCastRow,
        mut depends: Vec<PgDependRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values_without_relcache_invalidation(
            control.next_oid,
            control.next_rel_number,
        )?;
        self.control = control;

        for depend in &mut depends {
            depend.classid = PG_CAST_RELATION_OID;
            depend.objid = row.oid;
            depend.objsubid = 0;
        }
        sort_pg_depend_rows(&mut depends);
        let rows = PhysicalCatalogRows {
            casts: vec![row.clone()],
            depends,
            ..PhysicalCatalogRows::default()
        };
        let kinds = [BootstrapCatalogKind::PgCast, BootstrapCatalogKind::PgDepend];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((row.oid, effect))
    }

    pub fn drop_cast_by_oid_mvcc(
        &mut self,
        cast_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgCastRow, CatalogMutationEffect), CatalogError> {
        let cast_row = cast_row_by_oid_mvcc(self, ctx, cast_oid)?
            .ok_or_else(|| CatalogError::UnknownType(cast_oid.to_string()))?;
        let depends = depend_rows_for_object_mvcc(self, ctx, PG_CAST_RELATION_OID, cast_oid)?;
        let description_rows =
            description_rows_for_object_mvcc(self, ctx, cast_oid, PG_CAST_RELATION_OID, 0)?;
        let rows = PhysicalCatalogRows {
            casts: vec![cast_row.clone()],
            depends,
            descriptions: description_rows,
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgCast,
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgDescription,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((cast_row, effect))
    }

    pub fn create_operator_class_mvcc(
        &mut self,
        mut opfamily_row: PgOpfamilyRow,
        mut opclass_row: PgOpclassRow,
        mut amop_rows: Vec<PgAmopRow>,
        mut amproc_rows: Vec<PgAmprocRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut control = self.control_state()?;
        if opfamily_row.oid == 0 {
            opfamily_row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(opfamily_row.oid.saturating_add(1));
        if opclass_row.oid == 0 {
            opclass_row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(opclass_row.oid.saturating_add(1));
        opclass_row.opcfamily = opfamily_row.oid;
        for row in &mut amop_rows {
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
            row.amopfamily = opfamily_row.oid;
        }
        for row in &mut amproc_rows {
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
            row.amprocfamily = opfamily_row.oid;
        }
        self.persist_control_values_without_relcache_invalidation(
            control.next_oid,
            control.next_rel_number,
        )?;
        self.control = control;

        let mut depends = vec![
            PgDependRow {
                classid: PG_OPFAMILY_RELATION_OID,
                objid: opfamily_row.oid,
                objsubid: 0,
                refclassid: PG_NAMESPACE_RELATION_OID,
                refobjid: opfamily_row.opfnamespace,
                refobjsubid: 0,
                deptype: 'n',
            },
            PgDependRow {
                classid: PG_OPFAMILY_RELATION_OID,
                objid: opfamily_row.oid,
                objsubid: 0,
                refclassid: PG_AM_RELATION_OID,
                refobjid: opfamily_row.opfmethod,
                refobjsubid: 0,
                deptype: 'n',
            },
            PgDependRow {
                classid: PG_OPCLASS_RELATION_OID,
                objid: opclass_row.oid,
                objsubid: 0,
                refclassid: PG_OPFAMILY_RELATION_OID,
                refobjid: opfamily_row.oid,
                refobjsubid: 0,
                deptype: 'n',
            },
            PgDependRow {
                classid: PG_OPCLASS_RELATION_OID,
                objid: opclass_row.oid,
                objsubid: 0,
                refclassid: PG_NAMESPACE_RELATION_OID,
                refobjid: opclass_row.opcnamespace,
                refobjsubid: 0,
                deptype: 'n',
            },
            PgDependRow {
                classid: PG_OPCLASS_RELATION_OID,
                objid: opclass_row.oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: opclass_row.opcintype,
                refobjsubid: 0,
                deptype: 'n',
            },
            PgDependRow {
                classid: PG_OPCLASS_RELATION_OID,
                objid: opclass_row.oid,
                objsubid: 0,
                refclassid: PG_AM_RELATION_OID,
                refobjid: opclass_row.opcmethod,
                refobjsubid: 0,
                deptype: 'n',
            },
        ];
        depends.extend(amop_rows.iter().flat_map(|row| {
            [
                PgDependRow {
                    classid: PG_AMOP_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                    refclassid: PG_OPFAMILY_RELATION_OID,
                    refobjid: row.amopfamily,
                    refobjsubid: 0,
                    deptype: 'n',
                },
                PgDependRow {
                    classid: PG_AMOP_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                    refclassid: PG_OPERATOR_RELATION_OID,
                    refobjid: row.amopopr,
                    refobjsubid: 0,
                    deptype: 'n',
                },
            ]
        }));
        depends.extend(amproc_rows.iter().flat_map(|row| {
            [
                PgDependRow {
                    classid: PG_AMPROC_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                    refclassid: PG_OPFAMILY_RELATION_OID,
                    refobjid: row.amprocfamily,
                    refobjsubid: 0,
                    deptype: 'n',
                },
                PgDependRow {
                    classid: PG_AMPROC_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                    refclassid: PG_PROC_RELATION_OID,
                    refobjid: row.amproc,
                    refobjsubid: 0,
                    deptype: 'n',
                },
            ]
        }));
        sort_pg_depend_rows(&mut depends);

        let kinds = [
            BootstrapCatalogKind::PgOpfamily,
            BootstrapCatalogKind::PgOpclass,
            BootstrapCatalogKind::PgAmop,
            BootstrapCatalogKind::PgAmproc,
            BootstrapCatalogKind::PgDepend,
        ];
        let rows = PhysicalCatalogRows {
            opfamilies: vec![opfamily_row],
            opclasses: vec![opclass_row.clone()],
            amops: amop_rows,
            amprocs: amproc_rows,
            depends,
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((opclass_row.oid, effect))
    }

    pub fn create_trigger_mvcc(
        &mut self,
        mut row: crate::include::catalog::PgTriggerRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        if relation_triggers_mvcc(self, ctx, row.tgrelid)?
            .into_iter()
            .any(|existing| existing.tgname.eq_ignore_ascii_case(&row.tgname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_trigger_tgrelid_tgname_index".into(),
            ));
        }
        let old_class = class_row_by_oid_mvcc(self, ctx, row.tgrelid)?
            .ok_or_else(|| CatalogError::UnknownTable(row.tgrelid.to_string()))?;
        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut insert_rows = PhysicalCatalogRows {
            triggers: vec![row.clone()],
            depends: trigger_depend_rows(row.oid, row.tgrelid, row.tgfoid, &row.tgattr),
            ..PhysicalCatalogRows::default()
        };
        let mut kinds = vec![
            BootstrapCatalogKind::PgTrigger,
            BootstrapCatalogKind::PgDepend,
        ];
        if !old_class.relhastriggers {
            insert_rows.classes.push(PgClassRow {
                relhastriggers: true,
                ..old_class.clone()
            });
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    classes: vec![old_class],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &[BootstrapCatalogKind::PgClass],
            )?;
            kinds.push(BootstrapCatalogKind::PgClass);
        }
        insert_catalog_rows_subset_mvcc(ctx, &insert_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.tgrelid);
        Ok((row.oid, effect))
    }

    pub fn replace_trigger_mvcc(
        &mut self,
        old_row: &crate::include::catalog::PgTriggerRow,
        mut row: crate::include::catalog::PgTriggerRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let old_visible = trigger_row_mvcc(self, ctx, old_row.tgrelid, &old_row.tgname)?;
        if relation_triggers_mvcc(self, ctx, row.tgrelid)?
            .iter()
            .any(|existing| {
                existing.oid != old_visible.oid && existing.tgname.eq_ignore_ascii_case(&row.tgname)
            })
        {
            return Err(CatalogError::UniqueViolation(
                "pg_trigger_tgrelid_tgname_index".into(),
            ));
        }
        row.oid = old_visible.oid;
        let old_depends = trigger_depend_rows(
            old_visible.oid,
            old_visible.tgrelid,
            old_visible.tgfoid,
            &old_visible.tgattr,
        );
        let new_depends = trigger_depend_rows(row.oid, row.tgrelid, row.tgfoid, &row.tgattr);

        let old_class = class_row_by_oid_mvcc(self, ctx, old_visible.tgrelid)?
            .ok_or_else(|| CatalogError::UnknownTable(old_visible.tgrelid.to_string()))?;
        let new_class = class_row_by_oid_mvcc(self, ctx, row.tgrelid)?
            .ok_or_else(|| CatalogError::UnknownTable(row.tgrelid.to_string()))?;

        let old_has_remaining = relation_triggers_mvcc(self, ctx, old_visible.tgrelid)?
            .into_iter()
            .any(|trigger| trigger.oid != old_visible.oid)
            || row.tgrelid == old_visible.tgrelid;
        let new_has_triggers = true;

        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                triggers: vec![old_visible.clone()],
                depends: old_depends,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[
                BootstrapCatalogKind::PgTrigger,
                BootstrapCatalogKind::PgDepend,
            ],
        )?;

        let mut kinds = vec![
            BootstrapCatalogKind::PgTrigger,
            BootstrapCatalogKind::PgDepend,
        ];

        if old_class.relhastriggers != old_has_remaining {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    classes: vec![old_class.clone()],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &[BootstrapCatalogKind::PgClass],
            )?;
        }

        let mut insert_rows = PhysicalCatalogRows {
            triggers: vec![row.clone()],
            depends: new_depends,
            ..PhysicalCatalogRows::default()
        };
        let same_relation = old_visible.tgrelid == row.tgrelid;
        if old_class.relhastriggers != old_has_remaining {
            insert_rows.classes.push(PgClassRow {
                relhastriggers: old_has_remaining,
                ..old_class
            });
        }
        if !same_relation && new_class.relhastriggers != new_has_triggers {
            insert_rows.classes.push(PgClassRow {
                relhastriggers: new_has_triggers,
                ..new_class
            });
        }
        if !insert_rows.classes.is_empty() {
            kinds.push(BootstrapCatalogKind::PgClass);
        }

        self.invalidate_relcache_init_for_kinds(&kinds);
        insert_catalog_rows_subset_mvcc(ctx, &insert_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.tgrelid);
        Ok((row.oid, effect))
    }

    pub fn drop_trigger_mvcc(
        &mut self,
        relation_oid: u32,
        trigger_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(crate::include::catalog::PgTriggerRow, CatalogMutationEffect), CatalogError> {
        let old_trigger = trigger_row_mvcc(self, ctx, relation_oid, trigger_name)?;
        let old_class = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let has_remaining = relation_triggers_mvcc(self, ctx, relation_oid)?
            .into_iter()
            .any(|trigger| trigger.oid != old_trigger.oid);

        let mut kinds = vec![
            BootstrapCatalogKind::PgTrigger,
            BootstrapCatalogKind::PgDepend,
        ];
        let mut delete_rows = PhysicalCatalogRows {
            triggers: vec![old_trigger.clone()],
            depends: trigger_depend_rows(
                old_trigger.oid,
                old_trigger.tgrelid,
                old_trigger.tgfoid,
                &old_trigger.tgattr,
            ),
            ..PhysicalCatalogRows::default()
        };
        if old_class.relhastriggers != has_remaining {
            delete_rows.classes.push(old_class.clone());
            kinds.push(BootstrapCatalogKind::PgClass);
        }
        self.invalidate_relcache_init_for_kinds(&kinds);
        delete_catalog_rows_subset_mvcc(ctx, &delete_rows, self.scope_db_oid(), &kinds)?;

        if old_class.relhastriggers != has_remaining {
            insert_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    classes: vec![PgClassRow {
                        relhastriggers: has_remaining,
                        ..old_class
                    }],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &[BootstrapCatalogKind::PgClass],
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok((old_trigger, effect))
    }

    pub fn create_publication_mvcc(
        &mut self,
        mut publication: PgPublicationRow,
        mut publication_rels: Vec<PgPublicationRelRow>,
        mut publication_namespaces: Vec<PgPublicationNamespaceRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        if publication_row_by_name_mvcc(self, ctx, &publication.pubname)?.is_some() {
            return Err(CatalogError::UniqueViolation(
                "pg_publication_pubname_index".into(),
            ));
        }

        let mut control = self.control_state()?;
        if publication.oid == 0 {
            publication.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(publication.oid.saturating_add(1));
        for row in &mut publication_rels {
            row.prpubid = publication.oid;
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        }
        for row in &mut publication_namespaces {
            row.pnpubid = publication.oid;
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        }
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut rows = PhysicalCatalogRows {
            publications: vec![publication.clone()],
            publication_rels: publication_rels.clone(),
            publication_namespaces: publication_namespaces.clone(),
            ..PhysicalCatalogRows::default()
        };
        for row in &publication_rels {
            rows.depends.extend(publication_rel_depend_rows(
                row.oid,
                publication.oid,
                row.prrelid,
            ));
        }
        for row in &publication_namespaces {
            rows.depends.extend(publication_namespace_depend_rows(
                row.oid,
                publication.oid,
                row.pnnspid,
            ));
        }
        sort_pg_depend_rows(&mut rows.depends);

        let mut kinds = vec![BootstrapCatalogKind::PgPublication];
        if !rows.publication_rels.is_empty() {
            kinds.push(BootstrapCatalogKind::PgPublicationRel);
        }
        if !rows.publication_namespaces.is_empty() {
            kinds.push(BootstrapCatalogKind::PgPublicationNamespace);
        }
        if !rows.depends.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDepend);
        }
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((publication.oid, effect))
    }

    pub fn create_statistics_mvcc(
        &mut self,
        mut row: PgStatisticExtRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        if statistic_ext_row_by_name_namespace_mvcc(self, ctx, &row.stxname, row.stxnamespace)?
            .is_some()
        {
            return Err(CatalogError::UniqueViolation(
                "pg_statistic_ext_name_index".into(),
            ));
        }

        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut rows = PhysicalCatalogRows {
            statistics_ext: vec![row.clone()],
            depends: statistic_ext_depend_rows(&row),
            ..PhysicalCatalogRows::default()
        };
        sort_pg_depend_rows(&mut rows.depends);

        let mut kinds = vec![BootstrapCatalogKind::PgStatisticExt];
        if !rows.depends.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDepend);
        }
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.stxrelid);
        Ok((row.oid, effect))
    }

    pub fn create_policy_mvcc(
        &mut self,
        mut row: PgPolicyRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        if relation_policies_mvcc(self, ctx, row.polrelid)?
            .into_iter()
            .any(|existing| existing.polname.eq_ignore_ascii_case(&row.polname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_policy_polrelid_polname_index".into(),
            ));
        }
        if class_row_by_oid_mvcc(self, ctx, row.polrelid)?.is_none() {
            return Err(CatalogError::UnknownTable(row.polrelid.to_string()));
        }
        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                policies: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgPolicy],
        )?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgPolicy]);
        effect_record_oid(&mut effect.relation_oids, row.polrelid);
        Ok((row.oid, effect))
    }

    pub fn replace_policy_mvcc(
        &mut self,
        old_row: &PgPolicyRow,
        mut row: PgPolicyRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let old_visible = policy_row_mvcc(self, ctx, old_row.polrelid, &old_row.polname)?;
        if relation_policies_mvcc(self, ctx, row.polrelid)?
            .into_iter()
            .any(|existing| {
                existing.oid != old_visible.oid
                    && existing.polname.eq_ignore_ascii_case(&row.polname)
            })
        {
            return Err(CatalogError::UniqueViolation(
                "pg_policy_polrelid_polname_index".into(),
            ));
        }
        row.oid = old_visible.oid;
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                policies: vec![old_visible.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgPolicy],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                policies: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgPolicy],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgPolicy]);
        effect_record_oid(&mut effect.relation_oids, row.polrelid);
        Ok((row.oid, effect))
    }

    pub fn drop_policy_mvcc(
        &mut self,
        relation_oid: u32,
        policy_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgPolicyRow, CatalogMutationEffect), CatalogError> {
        let old_policy = policy_row_mvcc(self, ctx, relation_oid, policy_name)?;
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                policies: vec![old_policy.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgPolicy],
        )?;
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgPolicy]);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok((old_policy, effect))
    }

    pub fn replace_publication_row_mvcc(
        &mut self,
        publication: PgPublicationRow,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = publication_row_by_oid_mvcc(self, ctx, publication.oid)?
            .ok_or_else(|| CatalogError::UnknownTable(publication.oid.to_string()))?;
        if existing.pubname != publication.pubname
            && publication_row_by_name_mvcc(self, ctx, &publication.pubname)?.is_some()
        {
            return Err(CatalogError::UniqueViolation(
                "pg_publication_pubname_index".into(),
            ));
        }

        let kinds = [BootstrapCatalogKind::PgPublication];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                publications: vec![existing],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                publications: vec![publication],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok(effect)
    }

    pub fn replace_statistics_row_mvcc(
        &mut self,
        row: PgStatisticExtRow,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let description_rows =
            description_rows_for_object_mvcc(self, ctx, row.oid, PG_STATISTIC_EXT_RELATION_OID, 0)?;
        let existing = statistic_ext_row_by_oid_mvcc(self, ctx, row.oid)?
            .ok_or_else(|| CatalogError::UnknownTable(row.oid.to_string()))?;
        if statistic_ext_row_by_name_namespace_mvcc(self, ctx, &row.stxname, row.stxnamespace)?
            .is_some_and(|found| found.oid != row.oid)
        {
            return Err(CatalogError::UniqueViolation(
                "pg_statistic_ext_name_index".into(),
            ));
        }

        let old_depends = statistic_ext_depend_rows(&existing);
        let old_rows = PhysicalCatalogRows {
            statistics_ext: vec![existing],
            depends: old_depends,
            descriptions: description_rows.clone(),
            ..PhysicalCatalogRows::default()
        };
        let new_rows = PhysicalCatalogRows {
            statistics_ext: vec![row.clone()],
            depends: statistic_ext_depend_rows(&row),
            descriptions: description_rows.clone(),
            ..PhysicalCatalogRows::default()
        };
        let mut delete_kinds = Vec::new();
        let mut insert_kinds = vec![
            BootstrapCatalogKind::PgStatisticExt,
            BootstrapCatalogKind::PgDepend,
        ];
        if !description_rows.is_empty() {
            delete_kinds.push(BootstrapCatalogKind::PgDescription);
            insert_kinds.push(BootstrapCatalogKind::PgDescription);
        }
        delete_kinds.push(BootstrapCatalogKind::PgDepend);
        delete_kinds.push(BootstrapCatalogKind::PgStatisticExt);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &delete_kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &insert_kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &delete_kinds);
        effect_record_catalog_kinds(&mut effect, &insert_kinds);
        effect_record_oid(&mut effect.relation_oids, row.stxrelid);
        Ok(effect)
    }

    pub fn replace_publication_memberships_mvcc(
        &mut self,
        publication_oid: u32,
        mut publication_rels: Vec<PgPublicationRelRow>,
        mut publication_namespaces: Vec<PgPublicationNamespaceRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        if publication_row_by_oid_mvcc(self, ctx, publication_oid)?.is_none() {
            return Err(CatalogError::UnknownTable(publication_oid.to_string()));
        }

        let old_publication_rels =
            publication_rel_rows_for_publication_mvcc(self, ctx, publication_oid)?;
        let old_publication_namespaces =
            publication_namespace_rows_for_publication_mvcc(self, ctx, publication_oid)?;
        let old_rel_row_oids = old_publication_rels
            .iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let old_namespace_row_oids = old_publication_namespaces
            .iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let old_depends = depend_rows_referencing_object_mvcc(
            self,
            ctx,
            PG_PUBLICATION_RELATION_OID,
            publication_oid,
        )?
        .into_iter()
        .filter(|row| {
            (row.classid == PG_PUBLICATION_REL_RELATION_OID
                && old_rel_row_oids.contains(&row.objid))
                || (row.classid == PG_PUBLICATION_NAMESPACE_RELATION_OID
                    && old_namespace_row_oids.contains(&row.objid))
        })
        .collect::<Vec<_>>();

        let delete_rows = PhysicalCatalogRows {
            publication_rels: old_publication_rels,
            publication_namespaces: old_publication_namespaces,
            depends: old_depends,
            ..PhysicalCatalogRows::default()
        };
        let mut delete_kinds = Vec::new();
        if !delete_rows.publication_rels.is_empty() {
            delete_kinds.push(BootstrapCatalogKind::PgPublicationRel);
        }
        if !delete_rows.publication_namespaces.is_empty() {
            delete_kinds.push(BootstrapCatalogKind::PgPublicationNamespace);
        }
        if !delete_rows.depends.is_empty() {
            delete_kinds.push(BootstrapCatalogKind::PgDepend);
        }
        if !delete_kinds.is_empty() {
            delete_catalog_rows_subset_mvcc(ctx, &delete_rows, self.scope_db_oid(), &delete_kinds)?;
        }

        let mut control = self.control_state()?;
        for row in &mut publication_rels {
            row.prpubid = publication_oid;
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        }
        for row in &mut publication_namespaces {
            row.pnpubid = publication_oid;
            if row.oid == 0 {
                row.oid = control.next_oid;
            }
            control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        }
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut insert_rows = PhysicalCatalogRows {
            publication_rels: publication_rels.clone(),
            publication_namespaces: publication_namespaces.clone(),
            ..PhysicalCatalogRows::default()
        };
        for row in &publication_rels {
            insert_rows.depends.extend(publication_rel_depend_rows(
                row.oid,
                publication_oid,
                row.prrelid,
            ));
        }
        for row in &publication_namespaces {
            insert_rows
                .depends
                .extend(publication_namespace_depend_rows(
                    row.oid,
                    publication_oid,
                    row.pnnspid,
                ));
        }
        sort_pg_depend_rows(&mut insert_rows.depends);

        let mut insert_kinds = Vec::new();
        if !insert_rows.publication_rels.is_empty() {
            insert_kinds.push(BootstrapCatalogKind::PgPublicationRel);
        }
        if !insert_rows.publication_namespaces.is_empty() {
            insert_kinds.push(BootstrapCatalogKind::PgPublicationNamespace);
        }
        if !insert_rows.depends.is_empty() {
            insert_kinds.push(BootstrapCatalogKind::PgDepend);
        }
        if !insert_kinds.is_empty() {
            insert_catalog_rows_subset_mvcc(ctx, &insert_rows, self.scope_db_oid(), &insert_kinds)?;
        }
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &delete_kinds);
        effect_record_catalog_kinds(&mut effect, &insert_kinds);
        Ok(effect)
    }

    pub fn replace_statistics_data_rows_mvcc(
        &mut self,
        statistics_oid: u32,
        mut rows: Vec<PgStatisticExtDataRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_rows = statistic_ext_data_rows_mvcc(self, ctx, statistics_oid)?;
        let delete_kinds = if old_rows.is_empty() {
            Vec::new()
        } else {
            vec![BootstrapCatalogKind::PgStatisticExtData]
        };
        if !old_rows.is_empty() {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics_ext_data: old_rows,
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &delete_kinds,
            )?;
        }

        for row in &mut rows {
            row.stxoid = statistics_oid;
        }
        let insert_kinds = if rows.is_empty() {
            Vec::new()
        } else {
            vec![BootstrapCatalogKind::PgStatisticExtData]
        };
        if !rows.is_empty() {
            insert_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics_ext_data: rows,
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &insert_kinds,
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &delete_kinds);
        effect_record_catalog_kinds(&mut effect, &insert_kinds);
        Ok(effect)
    }

    pub fn drop_publication_mvcc(
        &mut self,
        publication_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgPublicationRow, CatalogMutationEffect), CatalogError> {
        let publication = publication_row_by_oid_mvcc(self, ctx, publication_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(publication_oid.to_string()))?;

        let membership_effect = self.replace_publication_memberships_mvcc(
            publication_oid,
            Vec::new(),
            Vec::new(),
            ctx,
        )?;
        let comment_effect = self.comment_publication_mvcc(publication_oid, None, ctx)?;

        let kinds = [BootstrapCatalogKind::PgPublication];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                publications: vec![publication.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &membership_effect.touched_catalogs);
        effect_record_catalog_kinds(&mut effect, &comment_effect.touched_catalogs);
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((publication, effect))
    }

    pub fn drop_statistics_mvcc(
        &mut self,
        statistics_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgStatisticExtRow, CatalogMutationEffect), CatalogError> {
        let statistics = statistic_ext_row_by_oid_mvcc(self, ctx, statistics_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(statistics_oid.to_string()))?;
        let depends =
            depend_rows_for_object_mvcc(self, ctx, PG_STATISTIC_EXT_RELATION_OID, statistics_oid)?;
        let data_effect =
            self.replace_statistics_data_rows_mvcc(statistics_oid, Vec::new(), ctx)?;
        let comment_effect = self.comment_statistics_mvcc(statistics_oid, None, ctx)?;

        let mut rows = PhysicalCatalogRows {
            statistics_ext: vec![statistics.clone()],
            depends,
            ..PhysicalCatalogRows::default()
        };
        let mut kinds = vec![BootstrapCatalogKind::PgStatisticExt];
        if !rows.depends.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDepend);
            sort_pg_depend_rows(&mut rows.depends);
        }
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &data_effect.touched_catalogs);
        effect_record_catalog_kinds(&mut effect, &comment_effect.touched_catalogs);
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, statistics.stxrelid);
        Ok((statistics, effect))
    }

    pub fn create_relation_inheritance_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        if parent_oids.is_empty() {
            return Ok(CatalogMutationEffect::default());
        }

        let child_relation = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let child_class = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let child_entry = catalog_entry_from_relation_row(&child_class, &child_relation);
        let old_child_rows = rows_for_existing_relation_mvcc(self, ctx, &child_entry)?;

        let mut rows_to_delete = PhysicalCatalogRows {
            depends: old_child_rows.depends.clone(),
            inherits: old_child_rows.inherits.clone(),
            ..PhysicalCatalogRows::default()
        };
        let mut rows_to_insert = PhysicalCatalogRows::default();
        let mut child_depends = Vec::new();
        let preserved_object_oids = old_child_rows
            .constraints
            .iter()
            .map(|row| row.oid)
            .chain(old_child_rows.rewrites.iter().map(|row| row.oid))
            .chain(old_child_rows.triggers.iter().map(|row| row.oid))
            .collect::<BTreeSet<_>>();
        child_depends.extend(
            old_child_rows
                .depends
                .iter()
                .filter(|row| preserved_object_oids.contains(&row.objid))
                .cloned(),
        );
        child_depends.extend(derived_pg_depend_rows(&child_entry));
        child_depends.extend(inheritance_depend_rows(relation_oid, parent_oids));
        sort_pg_depend_rows(&mut child_depends);
        rows_to_insert.depends = child_depends;
        rows_to_insert.inherits = parent_oids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, parent_oid)| PgInheritsRow {
                inhrelid: relation_oid,
                inhparent: parent_oid,
                inhseqno: index.saturating_add(1) as i32,
                inhdetachpending: false,
            })
            .collect();

        for &parent_oid in parent_oids {
            let old_parent = class_row_by_oid_mvcc(self, ctx, parent_oid)?
                .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
            if old_parent.relhassubclass {
                continue;
            }
            rows_to_delete.classes.push(old_parent.clone());
            rows_to_insert.classes.push(PgClassRow {
                relhassubclass: true,
                ..old_parent
            });
        }

        let mut kinds = vec![
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgInherits,
        ];
        if !rows_to_delete.classes.is_empty() {
            kinds.push(BootstrapCatalogKind::PgClass);
        }
        self.invalidate_relcache_init_for_kinds(&kinds);
        delete_catalog_rows_subset_mvcc(ctx, &rows_to_delete, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows_to_insert, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        for parent_oid in parent_oids {
            effect_record_oid(&mut effect.relation_oids, *parent_oid);
        }
        Ok(effect)
    }

    pub fn mark_relation_inheritance_detached_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut current_inherits = relation_inherits_mvcc(self, ctx, relation_oid)?;
        crate::include::catalog::sort_pg_inherits_rows(&mut current_inherits);
        let old_inherit = current_inherits
            .iter()
            .find(|row| row.inhparent == parent_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
        if old_inherit.inhdetachpending {
            return Ok(CatalogMutationEffect::default());
        }
        let new_inherit = PgInheritsRow {
            inhdetachpending: true,
            ..old_inherit.clone()
        };
        let rows_to_delete = PhysicalCatalogRows {
            inherits: vec![old_inherit],
            ..PhysicalCatalogRows::default()
        };
        let rows_to_insert = PhysicalCatalogRows {
            inherits: vec![new_inherit],
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![BootstrapCatalogKind::PgInherits];
        self.invalidate_relcache_init_for_kinds(&kinds);
        delete_catalog_rows_subset_mvcc(ctx, &rows_to_delete, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows_to_insert, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, parent_oid);
        Ok(effect)
    }

    pub fn drop_partition_inheritance_parent_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oid: u32,
        expect_detach_pending: Option<bool>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let current_inherits = relation_inherits_mvcc(self, ctx, relation_oid)?;
        let removed_inherit = current_inherits
            .iter()
            .find(|row| row.inhparent == parent_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
        if let Some(expected) = expect_detach_pending
            && removed_inherit.inhdetachpending != expected
        {
            return Err(CatalogError::UnknownTable(parent_oid.to_string()));
        }
        let removed_depends =
            depend_rows_for_object_mvcc(self, ctx, PG_CLASS_RELATION_OID, relation_oid)?
                .into_iter()
                .filter(|row| {
                    row.classid == PG_CLASS_RELATION_OID
                        && row.objid == relation_oid
                        && row.refclassid == PG_CLASS_RELATION_OID
                        && row.refobjid == parent_oid
                        && row.refobjsubid == 0
                        && row.deptype == DEPENDENCY_NORMAL
                })
                .collect::<Vec<_>>();

        let mut rows_to_delete = PhysicalCatalogRows {
            inherits: vec![removed_inherit],
            depends: removed_depends,
            ..PhysicalCatalogRows::default()
        };
        let mut rows_to_insert = PhysicalCatalogRows::default();
        if let Some(old_parent) = class_row_by_oid_mvcc(self, ctx, parent_oid)? {
            let has_remaining_children = relation_inherited_by_mvcc(self, ctx, parent_oid)?
                .into_iter()
                .any(|row| row.inhrelid != relation_oid);
            if old_parent.relhassubclass != has_remaining_children {
                rows_to_delete.classes.push(old_parent.clone());
                rows_to_insert.classes.push(PgClassRow {
                    relhassubclass: has_remaining_children,
                    ..old_parent
                });
            }
        }

        let mut kinds = vec![
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgInherits,
        ];
        if !rows_to_delete.classes.is_empty() {
            kinds.push(BootstrapCatalogKind::PgClass);
        }
        self.invalidate_relcache_init_for_kinds(&kinds);
        delete_catalog_rows_subset_mvcc(ctx, &rows_to_delete, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows_to_insert, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, parent_oid);
        Ok(effect)
    }

    pub fn drop_relation_inheritance_parent_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let child_relation = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if child_relation.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let parent_relation = self
            .relation_id_get_relation(ctx, parent_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
        if parent_relation.relkind != 'r' {
            return Err(CatalogError::UnknownTable(parent_oid.to_string()));
        }

        let mut current_inherits = relation_inherits_mvcc(self, ctx, relation_oid)?;
        crate::include::catalog::sort_pg_inherits_rows(&mut current_inherits);
        let removed_inherit = current_inherits
            .iter()
            .find(|row| row.inhparent == parent_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
        let remaining_inherits = current_inherits
            .iter()
            .filter(|row| row.inhparent != parent_oid)
            .cloned()
            .collect::<Vec<_>>();
        let current_parent_relations = current_inherits
            .iter()
            .map(|row| {
                self.relation_id_get_relation(ctx, row.inhparent)?
                    .ok_or_else(|| CatalogError::UnknownTable(row.inhparent.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let remaining_parent_relations = remaining_inherits
            .iter()
            .map(|row| {
                self.relation_id_get_relation(ctx, row.inhparent)?
                    .ok_or_else(|| CatalogError::UnknownTable(row.inhparent.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let child_class = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let child_name = child_class.relname.clone();
        let old_child_entry = catalog_entry_from_relation_row(&child_class, &child_relation);
        let mut new_child_entry = old_child_entry.clone();

        for column in &mut new_child_entry.desc.columns {
            if column.dropped {
                continue;
            }

            let current_parent_match_count =
                inherited_parent_column_match_count(&current_parent_relations, &column.name);
            let remaining_parent_match_count =
                inherited_parent_column_match_count(&remaining_parent_relations, &column.name);
            let had_local_column_definition =
                column.attislocal && column.attinhcount == current_parent_match_count as i16;
            column.attinhcount = remaining_parent_match_count as i16;
            column.attislocal = had_local_column_definition || remaining_parent_match_count == 0;

            if column.storage.nullable {
                continue;
            }

            let current_parent_not_null_count =
                inherited_parent_not_null_match_count(&current_parent_relations, &column.name);
            let remaining_parent_not_null_count =
                inherited_parent_not_null_match_count(&remaining_parent_relations, &column.name);
            let had_local_not_null_definition = column.not_null_constraint_is_local
                && column.not_null_constraint_inhcount == current_parent_not_null_count as i16;
            column.not_null_constraint_is_local =
                had_local_not_null_definition || remaining_parent_not_null_count == 0;
            column.not_null_constraint_inhcount = remaining_parent_not_null_count as i16;
            if !had_local_not_null_definition {
                column.not_null_constraint_no_inherit = false;
            }
        }

        let mut preserved_constraints = relation_constraints_mvcc(self, ctx, relation_oid)?
            .into_iter()
            .filter(|row| row.contype != CONSTRAINT_NOTNULL)
            .map(|mut row| {
                if row.contype == CONSTRAINT_CHECK {
                    let current_parent_match_count = inherited_parent_check_match_count_mvcc(
                        self,
                        ctx,
                        &current_parent_relations,
                        &row,
                    )?;
                    let remaining_parent_match_count = inherited_parent_check_match_count_mvcc(
                        self,
                        ctx,
                        &remaining_parent_relations,
                        &row,
                    )?;
                    let had_local_definition =
                        row.conislocal && row.coninhcount == current_parent_match_count as i16;
                    row.coninhcount = remaining_parent_match_count as i16;
                    row.conislocal = had_local_definition || remaining_parent_match_count == 0;
                    if !had_local_definition {
                        row.connoinherit = false;
                    }
                }
                Ok(row)
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;
        sort_pg_constraint_rows(&mut preserved_constraints);

        let mut new_constraints = derived_pg_constraint_rows(
            relation_oid,
            relation_object_name(&child_name),
            child_relation.namespace_oid,
            &new_child_entry.desc,
        );
        new_constraints.extend(preserved_constraints);
        sort_pg_constraint_rows(&mut new_constraints);

        let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
        let new_attributes =
            rows_for_new_relation_entry(&type_lookup, &child_name, &new_child_entry)?.attributes;
        let old_attributes = relation_attributes_mvcc(self, ctx, relation_oid)?;
        let old_constraints = relation_constraints_mvcc(self, ctx, relation_oid)?;
        let removed_depends =
            depend_rows_for_object_mvcc(self, ctx, PG_CLASS_RELATION_OID, relation_oid)?
                .into_iter()
                .filter(|row| {
                    row.classid == PG_CLASS_RELATION_OID
                        && row.objid == relation_oid
                        && row.refclassid == PG_CLASS_RELATION_OID
                        && row.refobjid == parent_oid
                        && row.refobjsubid == 0
                        && row.deptype == DEPENDENCY_NORMAL
                })
                .collect::<Vec<_>>();

        let mut rows_to_delete = PhysicalCatalogRows {
            attributes: old_attributes,
            constraints: old_constraints,
            inherits: vec![removed_inherit],
            depends: removed_depends,
            ..PhysicalCatalogRows::default()
        };
        let mut rows_to_insert = PhysicalCatalogRows {
            attributes: new_attributes,
            constraints: new_constraints,
            ..PhysicalCatalogRows::default()
        };

        if let Some(old_parent) = class_row_by_oid_mvcc(self, ctx, parent_oid)? {
            let has_remaining_children = relation_inherited_by_mvcc(self, ctx, parent_oid)?
                .into_iter()
                .any(|row| row.inhparent == parent_oid && row.inhrelid != relation_oid);
            if old_parent.relhassubclass != has_remaining_children {
                rows_to_delete.classes.push(old_parent.clone());
                rows_to_insert.classes.push(PgClassRow {
                    relhassubclass: has_remaining_children,
                    ..old_parent
                });
            }
        }

        let mut kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgInherits,
        ];
        if !rows_to_delete.classes.is_empty() {
            kinds.push(BootstrapCatalogKind::PgClass);
        }

        self.invalidate_relcache_init_for_kinds(&kinds);
        delete_catalog_rows_subset_mvcc(ctx, &rows_to_delete, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows_to_insert, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, parent_oid);
        Ok((new_child_entry, effect))
    }

    pub fn drop_namespace_mvcc(
        &mut self,
        namespace_oid: u32,
        namespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let publication_namespaces =
            publication_namespace_rows_for_namespace_mvcc(self, ctx, namespace_oid)?;
        let mut depends = Vec::new();
        for row in &publication_namespaces {
            depends.extend(depend_rows_for_object_mvcc(
                self,
                ctx,
                PG_PUBLICATION_NAMESPACE_RELATION_OID,
                row.oid,
            )?);
        }
        sort_pg_depend_rows(&mut depends);
        if !namespace_name.starts_with("pg_temp_") && !namespace_name.starts_with("pg_toast_temp_")
        {
            self.invalidate_relcache_init_for_kinds(&[BootstrapCatalogKind::PgNamespace]);
        }
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
                nspacl: None,
            }],
            publication_namespaces,
            depends,
            ..PhysicalCatalogRows::default()
        };
        let mut kinds = vec![BootstrapCatalogKind::PgNamespace];
        if !rows.publication_namespaces.is_empty() {
            kinds.push(BootstrapCatalogKind::PgPublicationNamespace);
            kinds.push(BootstrapCatalogKind::PgDepend);
        }
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn create_view_relation_mvcc(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        owner_oid: u32,
        reloptions: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        if self
            .get_relname_relid(ctx, &syscache_relname(&name), namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let mut entry = build_relation_entry(
            name.clone(),
            desc,
            namespace_oid,
            self.scope_db_oid(),
            'p',
            'v',
            owner_oid,
            0,
            &mut control,
        )?;
        entry.reloptions = reloptions;
        let kinds = create_view_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            rows_for_new_relation_entry(&type_lookup, &name, &entry)?
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
    }

    pub fn create_rule_mvcc(
        &mut self,
        relation_oid: u32,
        rule_name: impl Into<String>,
        ev_type: char,
        is_instead: bool,
        ev_qual: String,
        ev_action: String,
        referenced_relation_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.create_rule_mvcc_with_owner_dependency(
            relation_oid,
            rule_name,
            ev_type,
            is_instead,
            ev_qual,
            ev_action,
            referenced_relation_oids,
            RuleOwnerDependency::Auto,
            ctx,
        )
    }

    pub fn create_rule_mvcc_with_owner_dependency(
        &mut self,
        relation_oid: u32,
        rule_name: impl Into<String>,
        ev_type: char,
        is_instead: bool,
        ev_qual: String,
        ev_action: String,
        referenced_relation_oids: &[u32],
        owner_dependency: RuleOwnerDependency,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let rule_name = rule_name.into();
        self.relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let mut control = self.control_state()?;
        let rewrite_row = PgRewriteRow {
            oid: control.next_oid,
            rulename: rule_name,
            ev_class: relation_oid,
            ev_type,
            ev_enabled: 'O',
            is_instead,
            ev_qual,
            ev_action,
        };
        control.next_oid = control.next_oid.saturating_add(1);
        let mut referenced = referenced_relation_oids.to_vec();
        referenced.sort_unstable();
        referenced.dedup();

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            rewrites: vec![rewrite_row.clone()],
            depends: match owner_dependency {
                RuleOwnerDependency::Auto => {
                    relation_rule_depend_rows(rewrite_row.oid, relation_oid, &referenced)
                }
                RuleOwnerDependency::Internal => {
                    view_rewrite_depend_rows(rewrite_row.oid, relation_oid, &referenced)
                }
            },
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgRewrite,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn create_composite_type_mvcc(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        if self
            .get_relname_relid(ctx, &syscache_relname(&name), namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            name.clone(),
            desc,
            namespace_oid,
            self.scope_db_oid(),
            'p',
            'c',
            owner_oid,
            0,
            &mut control,
        )?;

        let kinds = create_composite_type_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            rows_for_new_relation_entry(&type_lookup, &name, &entry)?
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
    }

    pub fn create_shell_type_mvcc(
        &mut self,
        name: impl Into<String>,
        namespace_oid: u32,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        let object_name = relation_object_name(&name).to_ascii_lowercase();
        if type_row_by_name_namespace_mvcc(self, ctx, &object_name, namespace_oid)?.is_some() {
            return Err(CatalogError::TableAlreadyExists(object_name));
        }
        let oid = self.allocate_next_oid(0)?;
        let row = PgTypeRow {
            oid,
            typname: object_name,
            typnamespace: namespace_oid,
            typowner: owner_oid,
            typacl: None,
            typlen: -1,
            typbyval: false,
            typtype: 'p',
            typisdefined: false,
            typalign: AttributeAlign::Int,
            typstorage: AttributeStorage::Plain,
            typrelid: 0,
            typsubscript: 0,
            typelem: 0,
            typarray: 0,
            typinput: 0,
            typoutput: 0,
            typreceive: 0,
            typsend: 0,
            typmodin: 0,
            typmodout: 0,
            typdelim: ',',
            typanalyze: 0,
            typbasetype: 0,
            typcollation: 0,
            sql_type: SqlType::new(SqlTypeKind::Shell).with_identity(oid, 0),
        };
        let kinds = [BootstrapCatalogKind::PgType];
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        effect_record_oid(&mut effect.type_oids, oid);
        Ok((oid, effect))
    }

    pub fn complete_shell_base_type_mvcc(
        &mut self,
        type_oid: u32,
        typlen: i16,
        typalign: AttributeAlign,
        typstorage: AttributeStorage,
        typelem: u32,
        support_proc_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let old_row = type_row_by_oid_mvcc(self, ctx, type_oid)?
            .ok_or_else(|| CatalogError::UnknownType(type_oid.to_string()))?;
        if !matches!(old_row.sql_type.kind, SqlTypeKind::Shell) {
            return Err(CatalogError::TableAlreadyExists(old_row.typname.clone()));
        }

        let array_name = format!("_{}", old_row.typname);
        if type_row_by_name_namespace_mvcc(self, ctx, &array_name, old_row.typnamespace)?.is_some()
        {
            return Err(CatalogError::TableAlreadyExists(array_name));
        }
        let array_oid = self.allocate_next_oid(0)?;
        // :HACK: User-defined base types reuse text storage until registered
        // input/output functions are wired into value I/O.
        let base_sql_type = SqlType::new(SqlTypeKind::Text).with_identity(type_oid, 0);
        let base_row = PgTypeRow {
            oid: type_oid,
            typname: old_row.typname.clone(),
            typnamespace: old_row.typnamespace,
            typowner: old_row.typowner,
            typacl: old_row.typacl.clone(),
            typlen,
            typbyval: matches!(typlen, 1 | 2 | 4 | 8),
            typtype: 'b',
            typisdefined: true,
            typalign,
            typstorage,
            typrelid: 0,
            typsubscript: support_proc_oids.get(7).copied().unwrap_or(0),
            typelem,
            typarray: array_oid,
            typinput: support_proc_oids.first().copied().unwrap_or(0),
            typoutput: support_proc_oids.get(1).copied().unwrap_or(0),
            typreceive: support_proc_oids.get(2).copied().unwrap_or(0),
            typsend: support_proc_oids.get(3).copied().unwrap_or(0),
            typmodin: support_proc_oids.get(4).copied().unwrap_or(0),
            typmodout: support_proc_oids.get(5).copied().unwrap_or(0),
            typdelim: ',',
            typanalyze: support_proc_oids.get(6).copied().unwrap_or(0),
            typbasetype: 0,
            typcollation: 0,
            sql_type: base_sql_type,
        };
        let array_row = PgTypeRow {
            oid: array_oid,
            typname: array_name,
            typnamespace: old_row.typnamespace,
            typowner: old_row.typowner,
            typacl: old_row.typacl.clone(),
            typlen: -1,
            typbyval: false,
            typtype: 'b',
            typisdefined: true,
            typalign: AttributeAlign::Int,
            typstorage: AttributeStorage::Extended,
            typrelid: 0,
            typsubscript: 6179,
            typelem: type_oid,
            typarray: 0,
            typinput: 750,
            typoutput: 751,
            typreceive: 2400,
            typsend: 2401,
            typmodin: 0,
            typmodout: 0,
            typdelim: ',',
            typanalyze: 3816,
            typbasetype: 0,
            typcollation: 0,
            sql_type: SqlType::array_of(base_sql_type),
        };
        let mut depends = vec![
            PgDependRow {
                classid: PG_TYPE_RELATION_OID,
                objid: type_oid,
                objsubid: 0,
                refclassid: PG_NAMESPACE_RELATION_OID,
                refobjid: old_row.typnamespace,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            },
            PgDependRow {
                classid: PG_TYPE_RELATION_OID,
                objid: array_oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: type_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_INTERNAL,
            },
        ];
        depends.extend(
            support_proc_oids
                .iter()
                .copied()
                .filter(|proc_oid| *proc_oid != 0)
                .map(|proc_oid| PgDependRow {
                    classid: PG_TYPE_RELATION_OID,
                    objid: type_oid,
                    objsubid: 0,
                    refclassid: PG_PROC_RELATION_OID,
                    refobjid: proc_oid,
                    refobjsubid: 0,
                    deptype: DEPENDENCY_NORMAL,
                }),
        );
        sort_pg_depend_rows(&mut depends);

        let kinds = [BootstrapCatalogKind::PgType, BootstrapCatalogKind::PgDepend];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![old_row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgType],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![base_row, array_row],
                depends,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, old_row.typnamespace);
        effect_record_oid(&mut effect.type_oids, type_oid);
        effect_record_oid(&mut effect.type_oids, array_oid);
        Ok((array_oid, effect))
    }

    pub fn create_index_for_relation_mvcc(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        self.create_index_for_relation_mvcc_with_flags(
            index_name,
            relation_oid,
            unique,
            false,
            columns,
            ctx,
        )
    }

    pub fn create_index_for_relation_mvcc_with_flags(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let options = CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass: Vec::new(),
            indcollation: Vec::new(),
            indoption: Vec::new(),
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            brin_options: None,
            gin_options: None,
            hash_options: None,
        };
        self.create_index_for_relation_mvcc_with_options(
            index_name,
            relation_oid,
            unique,
            primary,
            columns,
            &options,
            None,
            ctx,
        )
    }

    pub fn create_index_for_relation_mvcc_with_options(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let index_name = index_name.into();
        let table = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let table_entry = catalog_entry_from_relation(&table);
        self.create_index_for_catalog_entry_mvcc_with_options(
            index_name,
            &table_entry,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            ctx,
        )
    }

    pub fn create_index_for_catalog_entry_mvcc_with_options(
        &mut self,
        index_name: impl Into<String>,
        table: &CatalogEntry,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let index_name = index_name.into();
        if self
            .get_relname_relid(ctx, &syscache_relname(&index_name), table.namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&index_name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
        let entry = build_index_entry(
            &type_lookup,
            index_name.clone(),
            table,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            &mut control,
        )?;
        let kinds = create_index_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&type_lookup, &index_name, &entry)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.created_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        effect_record_oid(&mut effect.relation_oids, table.relation_oid);
        Ok((entry, effect))
    }

    pub fn create_index_for_entry_mvcc_with_options(
        &mut self,
        index_name: impl Into<String>,
        table_entry: CatalogEntry,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let index_name = index_name.into();
        if self
            .get_relname_relid(
                ctx,
                &syscache_relname(&index_name),
                table_entry.namespace_oid,
            )?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&index_name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
        let entry = build_index_entry(
            &type_lookup,
            index_name.clone(),
            &table_entry,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            &mut control,
        )?;
        let kinds = create_index_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&type_lookup, &index_name, &entry)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.created_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        effect_record_oid(&mut effect.relation_oids, table_entry.relation_oid);
        Ok((entry, effect))
    }

    pub fn create_partitioned_index_for_relation_mvcc_with_options(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let index_name = index_name.into();
        let table = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if self
            .get_relname_relid(ctx, &syscache_relname(&index_name), table.namespace_oid)?
            .is_some()
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&index_name).to_ascii_lowercase(),
            ));
        }
        let table_entry = catalog_entry_from_relation(&table);
        let mut control = self.control_state()?;
        let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
        let entry = build_partitioned_index_entry(
            &type_lookup,
            index_name.clone(),
            &table_entry,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            &mut control,
        )?;
        let kinds = create_index_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&type_lookup, &index_name, &entry)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok((entry, effect))
    }

    pub fn create_index_backed_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.create_index_backed_constraint_mvcc_with_period(
            relation_oid,
            index_oid,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            false,
            None,
            false,
            false,
            ctx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_mvcc_with_period(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conperiod: bool,
        conexclop: Option<Vec<u32>>,
        deferrable: bool,
        initially_deferred: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.create_index_backed_constraint_mvcc_with_inheritance_and_period(
            relation_oid,
            index_oid,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            0,
            true,
            0,
            false,
            conperiod,
            conexclop,
            deferrable,
            initially_deferred,
            ctx,
        )
        .map(|(_, effect)| effect)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_for_entries_mvcc_with_period(
        &mut self,
        table: &CatalogEntry,
        index: &CatalogEntry,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conperiod: bool,
        conexclop: Option<Vec<u32>>,
        deferrable: bool,
        initially_deferred: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.create_index_backed_constraint_for_entries_mvcc_with_inheritance_and_period(
            table,
            index,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            0,
            true,
            0,
            false,
            conperiod,
            conexclop,
            deferrable,
            initially_deferred,
            ctx,
        )
        .map(|(_, effect)| effect)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_mvcc_with_inheritance(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        self.create_index_backed_constraint_mvcc_with_inheritance_and_period(
            relation_oid,
            index_oid,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            conparentid,
            conislocal,
            coninhcount,
            connoinherit,
            false,
            None,
            false,
            false,
            ctx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_mvcc_with_inheritance_and_period(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        conperiod: bool,
        conexclop: Option<Vec<u32>>,
        deferrable: bool,
        initially_deferred: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let conname = conname.into();
        let table = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' && table.relkind != 'p' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let index = self
            .relation_id_get_relation(ctx, index_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(index_oid.to_string()))?;
        if index.relkind != 'i' && index.relkind != 'I' {
            return Err(CatalogError::UnknownTable(index_oid.to_string()));
        }
        if self
            .search_sys_cache_list1(
                ctx,
                SysCacheId::ConstraintRelId,
                Value::Int64(i64::from(relation_oid)),
            )?
            .into_iter()
            .any(|tuple| match tuple {
                SysCacheTuple::Constraint(row) => {
                    row.contype == contype && row.conname.eq_ignore_ascii_case(&conname)
                }
                _ => false,
            })
        {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced: true,
            convalidated: true,
            conrelid: relation_oid,
            contypid: 0,
            conindid: index_oid,
            conparentid,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: index.index.as_ref().map(|meta| {
                meta.indkey
                    .iter()
                    .take(meta.indclass.len())
                    .copied()
                    .collect()
            }),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop,
            conbin: None,
            conislocal,
            coninhcount,
            connoinherit,
            conperiod,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        let mut depends =
            index_backed_constraint_depend_rows(constraint.oid, relation_oid, index_oid);
        if contype == CONSTRAINT_PRIMARY {
            for &not_null_constraint_oid in primary_key_owned_not_null_oids {
                depends.extend(primary_key_owned_not_null_depend_rows(
                    not_null_constraint_oid,
                    constraint.oid,
                ));
            }
            sort_pg_depend_rows(&mut depends);
        }

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends,
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, index_oid);
        Ok((constraint, effect))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_for_entries_mvcc_with_inheritance_and_period(
        &mut self,
        table: &CatalogEntry,
        index: &CatalogEntry,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        conperiod: bool,
        conexclop: Option<Vec<u32>>,
        deferrable: bool,
        initially_deferred: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let conname = conname.into();
        if table.relkind != 'r' && table.relkind != 'p' {
            return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
        }
        if index.relkind != 'i' && index.relkind != 'I' {
            return Err(CatalogError::UnknownTable(index.relation_oid.to_string()));
        }
        if self
            .search_sys_cache_list1(
                ctx,
                SysCacheId::ConstraintRelId,
                Value::Int64(i64::from(table.relation_oid)),
            )?
            .into_iter()
            .any(|tuple| match tuple {
                SysCacheTuple::Constraint(row) => {
                    row.contype == contype && row.conname.eq_ignore_ascii_case(&conname)
                }
                _ => false,
            })
        {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced: true,
            convalidated: true,
            conrelid: table.relation_oid,
            contypid: 0,
            conindid: index.relation_oid,
            conparentid,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: index.index_meta.as_ref().map(index_constraint_key_attnums),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop,
            conbin: None,
            conislocal,
            coninhcount,
            connoinherit,
            conperiod,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        let mut depends = index_backed_constraint_depend_rows(
            constraint.oid,
            table.relation_oid,
            index.relation_oid,
        );
        if contype == CONSTRAINT_PRIMARY {
            for &not_null_constraint_oid in primary_key_owned_not_null_oids {
                depends.extend(primary_key_owned_not_null_depend_rows(
                    not_null_constraint_oid,
                    constraint.oid,
                ));
            }
            sort_pg_depend_rows(&mut depends);
        }

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends,
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, table.relation_oid);
        effect_record_oid(&mut effect.relation_oids, index.relation_oid);
        Ok((constraint, effect))
    }

    pub fn update_index_backed_constraint_inheritance_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_oid: u32,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_constraint = relation_constraint_row_by_oid_mvcc(self, ctx, constraint_oid)?
            .filter(|row| row.conrelid == relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(constraint_oid.to_string()))?;
        if !matches!(
            old_constraint.contype,
            CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE
        ) || old_constraint.conindid == 0
        {
            return Err(CatalogError::UnknownTable(constraint_oid.to_string()));
        }
        let primary_key_owned_not_null_oids = depend_rows_referencing_object_mvcc(
            self,
            ctx,
            PG_CONSTRAINT_RELATION_OID,
            constraint_oid,
        )?
        .into_iter()
        .filter(|depend| {
            depend.classid == PG_CONSTRAINT_RELATION_OID
                && depend.refclassid == PG_CONSTRAINT_RELATION_OID
                && depend.refobjid == constraint_oid
                && depend.deptype == DEPENDENCY_INTERNAL
        })
        .map(|depend| depend.objid)
        .collect::<Vec<_>>();
        let mut new_constraint = old_constraint.clone();
        new_constraint.conparentid = conparentid;
        new_constraint.conislocal = conislocal;
        new_constraint.coninhcount = coninhcount;
        new_constraint.connoinherit = connoinherit;
        let mut depends = index_backed_constraint_depend_rows(
            new_constraint.oid,
            new_constraint.conrelid,
            new_constraint.conindid,
        );
        if new_constraint.contype == CONSTRAINT_PRIMARY {
            for not_null_constraint_oid in primary_key_owned_not_null_oids {
                depends.extend(primary_key_owned_not_null_depend_rows(
                    not_null_constraint_oid,
                    new_constraint.oid,
                ));
            }
            sort_pg_depend_rows(&mut depends);
        }
        let old_depends = constraint_depend_rows_mvcc(self, ctx, old_constraint.oid)?;
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_constraint],
                depends: old_depends,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_constraint.clone()],
                depends,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, new_constraint.conindid);
        Ok(effect)
    }

    pub fn create_check_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        conenforced: bool,
        convalidated: bool,
        connoinherit: bool,
        conbin: impl Into<String>,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.create_check_constraint_mvcc_with_row(
            relation_oid,
            conname,
            conenforced,
            convalidated,
            connoinherit,
            conbin,
            conparentid,
            conislocal,
            coninhcount,
            ctx,
        )
        .map(|(_, effect)| effect)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_check_constraint_mvcc_with_row(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        conenforced: bool,
        convalidated: bool,
        connoinherit: bool,
        conbin: impl Into<String>,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let conname = conname.into();
        let conbin = conbin.into();
        let table = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(table.relkind, 'r' | 'p') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if relation_constraint_exists_mvcc(self, ctx, relation_oid, &conname, None)? {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype: crate::include::catalog::CONSTRAINT_CHECK,
            condeferrable: false,
            condeferred: false,
            conenforced,
            convalidated,
            conrelid: relation_oid,
            contypid: 0,
            conindid: 0,
            conparentid,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: None,
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: Some(conbin),
            conislocal,
            coninhcount,
            connoinherit,
            conperiod: false,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: relation_constraint_depend_rows(constraint.oid, relation_oid),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok((constraint, effect))
    }

    pub fn update_check_constraint_inheritance_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_oid: u32,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_row = relation_constraint_row_by_oid_mvcc(self, ctx, constraint_oid)?
            .filter(|row| row.conrelid == relation_oid && row.contype == CONSTRAINT_CHECK)
            .ok_or_else(|| CatalogError::UnknownTable(constraint_oid.to_string()))?;
        let mut new_row = old_row.clone();
        new_row.conparentid = conparentid;
        new_row.conislocal = conislocal;
        new_row.coninhcount = coninhcount;
        new_row.connoinherit = connoinherit;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn create_foreign_key_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        deferrable: bool,
        initially_deferred: bool,
        conenforced: bool,
        convalidated: bool,
        local_attnums: &[i16],
        referenced_relation_oid: u32,
        referenced_index_oid: u32,
        referenced_attnums: &[i16],
        confupdtype: char,
        confdeltype: char,
        confmatchtype: char,
        confdelsetcols: Option<&[i16]>,
        conperiod: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let conname = conname.into();
        let table = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let referenced_table = self
            .relation_id_get_relation(ctx, referenced_relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(referenced_relation_oid.to_string()))?;
        if referenced_table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(
                referenced_relation_oid.to_string(),
            ));
        }
        let referenced_index = self
            .relation_id_get_relation(ctx, referenced_index_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(referenced_index_oid.to_string()))?;
        if referenced_index.relkind != 'i' {
            return Err(CatalogError::UnknownTable(referenced_index_oid.to_string()));
        }
        if relation_constraint_exists_mvcc(self, ctx, relation_oid, &conname, None)? {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let equality_ops = referenced_index
            .index
            .as_ref()
            .and_then(|meta| foreign_key_equality_operators_visible(&meta.indclass));
        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype: crate::include::catalog::CONSTRAINT_FOREIGN,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced,
            convalidated,
            conrelid: relation_oid,
            contypid: 0,
            conindid: referenced_index_oid,
            conparentid: 0,
            confrelid: referenced_relation_oid,
            confupdtype,
            confdeltype,
            confmatchtype,
            conkey: Some(local_attnums.to_vec()),
            confkey: Some(referenced_attnums.to_vec()),
            conpfeqop: equality_ops.clone(),
            conppeqop: equality_ops.clone(),
            conffeqop: equality_ops,
            confdelsetcols: confdelsetcols.map(<[i16]>::to_vec),
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: foreign_key_constraint_depend_rows(
                constraint.oid,
                relation_oid,
                referenced_relation_oid,
                referenced_index_oid,
            ),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, referenced_relation_oid);
        effect_record_oid(&mut effect.relation_oids, referenced_index_oid);
        Ok((constraint, effect))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_foreign_key_constraint_for_entries_mvcc(
        &mut self,
        table: &CatalogEntry,
        conname: impl Into<String>,
        deferrable: bool,
        initially_deferred: bool,
        conenforced: bool,
        convalidated: bool,
        local_attnums: &[i16],
        referenced_table: &CatalogEntry,
        referenced_index: &CatalogEntry,
        referenced_attnums: &[i16],
        confupdtype: char,
        confdeltype: char,
        confmatchtype: char,
        confdelsetcols: Option<&[i16]>,
        conperiod: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let conname = conname.into();
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
        }
        if referenced_table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(
                referenced_table.relation_oid.to_string(),
            ));
        }
        if referenced_index.relkind != 'i' {
            return Err(CatalogError::UnknownTable(
                referenced_index.relation_oid.to_string(),
            ));
        }
        if relation_constraint_exists_mvcc(self, ctx, table.relation_oid, &conname, None)? {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let equality_ops = referenced_index
            .index_meta
            .as_ref()
            .and_then(|meta| foreign_key_equality_operators_visible(&meta.indclass));
        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype: crate::include::catalog::CONSTRAINT_FOREIGN,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced,
            convalidated,
            conrelid: table.relation_oid,
            contypid: 0,
            conindid: referenced_index.relation_oid,
            conparentid: 0,
            confrelid: referenced_table.relation_oid,
            confupdtype,
            confdeltype,
            confmatchtype,
            conkey: Some(local_attnums.to_vec()),
            confkey: Some(referenced_attnums.to_vec()),
            conpfeqop: equality_ops.clone(),
            conppeqop: equality_ops.clone(),
            conffeqop: equality_ops,
            confdelsetcols: confdelsetcols.map(<[i16]>::to_vec),
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: foreign_key_constraint_depend_rows(
                constraint.oid,
                table.relation_oid,
                referenced_table.relation_oid,
                referenced_index.relation_oid,
            ),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, table.relation_oid);
        effect_record_oid(&mut effect.relation_oids, referenced_table.relation_oid);
        effect_record_oid(&mut effect.relation_oids, referenced_index.relation_oid);
        Ok((constraint, effect))
    }

    pub fn drop_relation_entry_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
        let old_rows = rows_for_drop_relation_entry_mvcc(self, ctx, &entry)?;
        let kinds = drop_relation_delete_kinds();
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.dropped_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        if entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        }
        Ok((entry, effect))
    }

    pub fn drop_relation_entry_mvcc(
        &mut self,
        entry: CatalogEntry,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_rows = rows_for_drop_relation_entry_mvcc(self, ctx, &entry)?;
        let kinds = drop_relation_delete_kinds();
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.dropped_rels, entry.rel);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        if entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        }
        Ok(effect)
    }

    pub fn set_column_not_null_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        constraint_name: impl Into<String>,
        validated: bool,
        no_inherit: bool,
        primary_key_owned: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let constraint_name = constraint_name.into();
        let (old_entry, _new_entry, constraint_oid, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.storage.nullable = false;
                if column.not_null_constraint_oid.is_none() {
                    column.not_null_constraint_oid = Some(control.next_oid);
                    control.next_oid = control.next_oid.saturating_add(1);
                }
                let constraint_oid = column
                    .not_null_constraint_oid
                    .expect("not-null constraint oid");
                column.not_null_constraint_name = Some(constraint_name);
                column.not_null_constraint_validated = validated;
                column.not_null_constraint_no_inherit = no_inherit;
                column.not_null_primary_key_owned = primary_key_owned;
                Ok((
                    constraint_oid,
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, old_entry.row_type_oid);
        Ok((constraint_oid, effect))
    }

    pub fn drop_column_not_null_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.storage.nullable = true;
                column.not_null_constraint_oid = None;
                column.not_null_constraint_name = None;
                column.not_null_constraint_validated = false;
                column.not_null_constraint_no_inherit = false;
                column.not_null_primary_key_owned = false;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn validate_not_null_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index =
                    not_null_constraint_column_index_visible(&entry.desc, constraint_name)?;
                entry.desc.columns[column_index].not_null_constraint_validated = true;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn validate_check_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_row = relation_constraint_row_mvcc(
            self,
            ctx,
            relation_oid,
            constraint_name,
            Some(crate::include::catalog::CONSTRAINT_CHECK),
        )?;
        let mut new_row = old_row.clone();
        new_row.convalidated = true;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn validate_foreign_key_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_row = relation_constraint_row_mvcc(
            self,
            ctx,
            relation_oid,
            constraint_name,
            Some(crate::include::catalog::CONSTRAINT_FOREIGN),
        )?;
        let mut new_row = old_row.clone();
        new_row.convalidated = true;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn alter_foreign_key_constraint_attributes_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        deferrable: bool,
        initially_deferred: bool,
        enforced: bool,
        validated: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_row = relation_constraint_row_mvcc(
            self,
            ctx,
            relation_oid,
            constraint_name,
            Some(crate::include::catalog::CONSTRAINT_FOREIGN),
        )?;
        let mut new_row = old_row.clone();
        new_row.condeferrable = deferrable;
        new_row.condeferred = initially_deferred;
        new_row.conenforced = enforced;
        new_row.convalidated = validated;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, new_row.confrelid);
        if new_row.conindid != 0 {
            effect_record_oid(&mut effect.relation_oids, new_row.conindid);
        }
        Ok(effect)
    }

    pub fn rename_relation_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        new_constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let new_constraint_name = new_constraint_name.to_ascii_lowercase();
        if relation_constraint_exists_mvcc(self, ctx, relation_oid, &new_constraint_name, None)? {
            return Err(CatalogError::TableAlreadyExists(new_constraint_name));
        }
        let old_constraint =
            relation_constraint_row_mvcc(self, ctx, relation_oid, constraint_name, None)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_oid(&mut effect.relation_oids, relation_oid);

        if old_constraint.contype == crate::include::catalog::CONSTRAINT_NOTNULL {
            let (_old_entry, _new_entry, _, kinds) =
                mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                    if entry.relkind != 'r' {
                        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                    }
                    let column_index =
                        not_null_constraint_column_index_visible(&entry.desc, constraint_name)?;
                    entry.desc.columns[column_index].not_null_constraint_name =
                        Some(new_constraint_name.clone());
                    Ok(((), vec![BootstrapCatalogKind::PgConstraint]))
                })?;
            effect_record_catalog_kinds(&mut effect, &kinds);
            return Ok(effect);
        }

        let mut new_constraint = old_constraint.clone();
        new_constraint.conname = new_constraint_name.clone();
        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_constraint.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_constraint.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        effect_record_catalog_kinds(&mut effect, &kinds);

        if new_constraint.conindid != 0 {
            let index_effect =
                self.rename_relation_mvcc(new_constraint.conindid, &new_constraint_name, &[], ctx)?;
            effect_record_catalog_kinds(&mut effect, &index_effect.touched_catalogs);
            for rel in index_effect.created_rels {
                effect_record_rel(&mut effect.created_rels, rel);
            }
            for rel in index_effect.dropped_rels {
                effect_record_rel(&mut effect.dropped_rels, rel);
            }
            for oid in index_effect.relation_oids {
                effect_record_oid(&mut effect.relation_oids, oid);
            }
            for oid in index_effect.namespace_oids {
                effect_record_oid(&mut effect.namespace_oids, oid);
            }
            for oid in index_effect.type_oids {
                effect_record_oid(&mut effect.type_oids, oid);
            }
        }

        Ok(effect)
    }

    pub fn drop_relation_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let removed = relation_constraint_row_mvcc(self, ctx, relation_oid, constraint_name, None)?;
        let removed_depends = constraint_depend_rows_mvcc(self, ctx, removed.oid)?;
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![removed.clone()],
                depends: removed_depends,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if removed.conindid != 0 {
            effect_record_oid(&mut effect.relation_oids, removed.conindid);
        }
        Ok((removed, effect))
    }

    pub fn drop_relation_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(Vec<CatalogEntry>, CatalogMutationEffect), CatalogError> {
        self.drop_relation_by_oid_mvcc_with_extra_type_rows(relation_oid, ctx, &[])
    }

    pub fn drop_relation_by_oid_mvcc_with_extra_type_rows(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
        extra_type_rows: &[PgTypeRow],
    ) -> Result<(Vec<CatalogEntry>, CatalogMutationEffect), CatalogError> {
        if has_nonpartition_inherited_children_mvcc(self, ctx, relation_oid)? {
            return Err(CatalogError::Corrupt(
                "DROP TABLE with inherited children requires CASCADE, which is not supported yet",
            ));
        }
        let (rows_to_delete, parent_rows_to_insert, dropped, affected_parent_oids) =
            drop_relation_entries_mvcc(self, ctx, relation_oid, extra_type_rows)?;
        let kinds = drop_relation_delete_kinds();
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &rows_to_delete, self.scope_db_oid(), &kinds)?;
        if !parent_rows_to_insert.classes.is_empty() {
            let parent_kinds = vec![BootstrapCatalogKind::PgClass];
            insert_catalog_rows_subset_mvcc(
                ctx,
                &parent_rows_to_insert,
                self.scope_db_oid(),
                &parent_kinds,
            )?;
        }
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        if !parent_rows_to_insert.classes.is_empty() {
            effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgClass]);
        }
        for entry in &dropped {
            let comment_effect = self.comment_relation_mvcc(entry.relation_oid, None, ctx)?;
            effect_record_catalog_kinds(&mut effect, &comment_effect.touched_catalogs);
            effect_record_rel(&mut effect.dropped_rels, entry.rel);
            effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
            effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
            effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        }
        for parent_oid in &affected_parent_oids {
            effect_record_oid(&mut effect.relation_oids, *parent_oid);
        }
        Ok((dropped, effect))
    }

    pub fn drop_view_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
        if entry.relkind != 'v' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let rows = rows_for_drop_relation_entry_mvcc(self, ctx, &entry)?;
        let kinds = drop_relation_delete_kinds();
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
    }

    pub fn drop_composite_type_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
        if entry.relkind != 'c' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let rows = rows_for_drop_relation_entry_mvcc(self, ctx, &entry)?;
        let kinds = drop_relation_delete_kinds();
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
    }

    pub fn drop_shell_type_by_oid_mvcc(
        &mut self,
        type_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let type_row = type_row_by_oid_mvcc(self, ctx, type_oid)?
            .ok_or_else(|| CatalogError::UnknownType(type_oid.to_string()))?;
        if !matches!(type_row.sql_type.kind, SqlTypeKind::Shell) {
            return Err(CatalogError::UnknownType(type_oid.to_string()));
        }
        let description_rows =
            description_rows_for_object_mvcc(self, ctx, type_oid, PG_TYPE_RELATION_OID, 0)?;
        let depend_rows = depend_rows_for_object_mvcc(self, ctx, PG_TYPE_RELATION_OID, type_oid)?;
        let mut kinds = vec![BootstrapCatalogKind::PgType];
        if !description_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDescription);
        }
        if !depend_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDepend);
        }
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![type_row.clone()],
                descriptions: description_rows,
                depends: depend_rows,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, type_row.typnamespace);
        effect_record_oid(&mut effect.type_oids, type_row.oid);
        Ok(effect)
    }

    pub fn drop_base_type_by_oid_mvcc(
        &mut self,
        type_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let type_row = type_row_by_oid_mvcc(self, ctx, type_oid)?
            .ok_or_else(|| CatalogError::UnknownType(type_oid.to_string()))?;
        if matches!(type_row.sql_type.kind, SqlTypeKind::Shell) {
            return Err(CatalogError::UnknownType(type_oid.to_string()));
        }
        let mut type_rows = vec![type_row.clone()];
        if type_row.typarray != 0
            && let Some(array_row) = type_row_by_oid_mvcc(self, ctx, type_row.typarray)?
        {
            type_rows.push(array_row);
        }
        let mut description_rows = Vec::new();
        let mut depend_rows = Vec::new();
        for row in &type_rows {
            description_rows.extend(description_rows_for_object_mvcc(
                self,
                ctx,
                row.oid,
                PG_TYPE_RELATION_OID,
                0,
            )?);
            depend_rows.extend(depend_rows_for_object_mvcc(
                self,
                ctx,
                PG_TYPE_RELATION_OID,
                row.oid,
            )?);
            depend_rows.extend(depend_rows_referencing_object_mvcc(
                self,
                ctx,
                PG_TYPE_RELATION_OID,
                row.oid,
            )?);
        }
        sort_pg_depend_rows(&mut depend_rows);
        depend_rows.dedup();

        let mut kinds = vec![BootstrapCatalogKind::PgType];
        if !description_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDescription);
        }
        if !depend_rows.is_empty() {
            kinds.push(BootstrapCatalogKind::PgDepend);
        }
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: type_rows.clone(),
                descriptions: description_rows,
                depends: depend_rows,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.namespace_oids, type_row.typnamespace);
        for row in type_rows {
            effect_record_oid(&mut effect.type_oids, row.oid);
        }
        Ok(effect)
    }

    pub fn set_index_ready_valid_mvcc(
        &mut self,
        relation_oid: u32,
        indisready: bool,
        indisvalid: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let class_row = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(class_row.relkind, 'i' | 'I') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let old_index = self
            .search_sys_cache1(ctx, SysCacheId::IndexRelId, oid_key(relation_oid))?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Index(row) => Some(row),
                _ => None,
            })
            .ok_or(CatalogError::Corrupt(
                "index relation missing index metadata",
            ))?;
        let mut new_index = old_index.clone();
        new_index.indisready = indisready;
        new_index.indisvalid = indisvalid;

        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let kinds = vec![BootstrapCatalogKind::PgIndex];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: vec![old_index.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: vec![new_index],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, old_index.indrelid);
        Ok(effect)
    }

    pub fn set_index_entry_ready_valid_mvcc(
        &mut self,
        old_entry: &CatalogEntry,
        indisready: bool,
        indisvalid: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        if !matches!(old_entry.relkind, 'i' | 'I') {
            return Err(CatalogError::UnknownTable(
                old_entry.relation_oid.to_string(),
            ));
        }
        let mut new_entry = old_entry.clone();
        let index_meta = new_entry.index_meta.as_mut().ok_or(CatalogError::Corrupt(
            "index relation missing index metadata",
        ))?;
        index_meta.indisready = indisready;
        index_meta.indisvalid = indisvalid;

        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let kinds = vec![BootstrapCatalogKind::PgIndex];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: vec![index_row_for_entry(old_entry).ok_or(CatalogError::Corrupt(
                    "index relation missing index metadata",
                ))?],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: vec![index_row_for_entry(&new_entry).ok_or(CatalogError::Corrupt(
                    "index relation missing index metadata",
                ))?],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, old_entry.relation_oid);
        if let Some(index_meta) = &new_entry.index_meta {
            effect_record_oid(&mut effect.relation_oids, index_meta.indrelid);
        }
        Ok(effect)
    }

    pub fn set_replica_identity_index_mvcc(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_indexes = index_rows_for_relation_mvcc(self, ctx, relation_oid)?;
        let target = old_indexes
            .iter()
            .find(|row| row.indexrelid == index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(index_oid.to_string()))?;
        if !target.indisunique {
            return Err(CatalogError::Corrupt(
                "replica identity index must be unique",
            ));
        }
        let mut new_indexes = old_indexes.clone();
        for row in &mut new_indexes {
            row.indisreplident = row.indexrelid == index_oid;
        }

        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let kinds = vec![BootstrapCatalogKind::PgIndex];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: old_indexes,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                indexes: new_indexes,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.relation_oids, index_oid);
        Ok(effect)
    }

    pub fn alter_table_add_column_mvcc(
        &mut self,
        relation_oid: u32,
        column: ColumnDesc,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, move |entry, control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                if entry
                    .desc
                    .columns
                    .iter()
                    .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
                {
                    return Err(CatalogError::TableAlreadyExists(column.name.clone()));
                }
                entry.desc.columns.push(column);
                allocate_relation_object_oids(&mut entry.desc, &mut control.next_oid);
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn ensure_relation_toast_table_mvcc(
        &mut self,
        relation_oid: u32,
        toast_namespace_oid: u32,
        toast_namespace_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<Option<CatalogMutationEffect>, CatalogError> {
        let class_row = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let relation_name = class_row.relname.clone();
        let relation = self
            .relation_id_get_relation(ctx, relation_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = catalog_entry_from_relation_row(&class_row, &relation);

        let mut control = self.control_state()?;
        let Some(toast) = build_toast_catalog_changes(
            &relation_name,
            &old_entry,
            toast_namespace_name,
            toast_namespace_oid,
            &mut control,
        )?
        else {
            return Ok(None);
        };

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let mut kinds = create_table_sync_kinds(&toast.new_parent);
        merge_catalog_kinds(&mut kinds, &create_table_sync_kinds(&toast.toast_entry));
        merge_catalog_kinds(&mut kinds, &create_index_sync_kinds());
        kinds.retain(|kind| *kind != BootstrapCatalogKind::PgInherits);

        let old_rows = rows_for_existing_relation_mvcc(self, ctx, &old_entry)?;
        let mut new_rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            let mut rows =
                rows_for_new_relation_entry(&type_lookup, &relation_name, &toast.new_parent)?;
            extend_physical_catalog_rows(
                &mut rows,
                rows_for_new_relation_entry(&type_lookup, &toast.toast_name, &toast.toast_entry)?,
            );
            extend_physical_catalog_rows(
                &mut rows,
                rows_for_new_relation_entry(&type_lookup, &toast.index_name, &toast.index_entry)?,
            );
            rows
        };
        new_rows.depends.push(PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: toast.toast_entry.relation_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: crate::include::catalog::DEPENDENCY_INTERNAL,
        });
        sort_pg_depend_rows(&mut new_rows.depends);
        preserve_non_derived_relation_rows_mvcc(self, ctx, &old_entry, &kinds, &mut new_rows)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, toast.new_parent.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, toast.new_parent.namespace_oid);
        effect_record_oid(&mut effect.type_oids, toast.new_parent.row_type_oid);
        record_toast_effects(&mut effect, &toast);
        Ok(Some(effect))
    }

    pub fn alter_table_drop_column_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !matches!(entry.relkind, 'r' | 'v') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let attnum = column_index + 1;
                let column = &mut entry.desc.columns[column_index];
                column.name = dropped_column_name_visible(attnum);
                column.storage.name = column.name.clone();
                column.storage.nullable = true;
                column.dropped = true;
                column.attstattarget = -1;
                column.not_null_constraint_oid = None;
                column.not_null_constraint_name = None;
                column.not_null_constraint_validated = false;
                column.not_null_primary_key_owned = false;
                column.attrdef_oid = None;
                column.default_expr = None;
                column.generated = None;
                column.missing_default_value = None;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_default_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        default_expr: Option<String>,
        default_sequence_oid: Option<u32>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.default_expr = default_expr;
                column.default_sequence_oid = default_sequence_oid;
                if column.default_expr.is_some() {
                    if column.attrdef_oid.is_none() {
                        column.attrdef_oid = Some(control.next_oid);
                        control.next_oid = control.next_oid.saturating_add(1);
                    }
                } else {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_generation_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        default_expr: Option<String>,
        generated: Option<crate::include::nodes::parsenodes::ColumnGeneratedKind>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, move |entry, control| {
                if entry.relkind != 'r' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.default_expr = default_expr;
                column.default_sequence_oid = None;
                column.generated = generated;
                if column.default_expr.is_some() {
                    if column.attrdef_oid.is_none() {
                        column.attrdef_oid = Some(control.next_oid);
                        control.next_oid = control.next_oid.saturating_add(1);
                    }
                } else {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_identity_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        identity: Option<crate::include::nodes::parsenodes::ColumnIdentityKind>,
        default_expr: Option<String>,
        default_sequence_oid: Option<u32>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, move |entry, control| {
                if !matches!(entry.relkind, 'r' | 'p') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.identity = identity;
                column.generated = None;
                column.default_expr = default_expr;
                column.default_sequence_oid = default_sequence_oid;
                if column.default_expr.is_some() {
                    if column.attrdef_oid.is_none() {
                        column.attrdef_oid = Some(control.next_oid);
                        control.next_oid = control.next_oid.saturating_add(1);
                    }
                } else {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.set_relation_column_statistics_mvcc(
            relation_oid,
            column_name,
            statistics_target,
            ctx,
            &['r'],
        )
    }

    pub fn alter_index_set_column_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.set_relation_column_statistics_mvcc(
            relation_oid,
            column_name,
            statistics_target,
            ctx,
            &['i'],
        )
    }

    fn set_relation_column_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
        ctx: &CatalogWriteContext,
        allowed_relkinds: &[char],
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !allowed_relkinds.contains(&entry.relkind) {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                entry.desc.columns[column_index].attstattarget = statistics_target;
                Ok(((), vec![BootstrapCatalogKind::PgAttribute]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_storage_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        storage: crate::include::access::htup::AttributeStorage,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !matches!(entry.relkind, 'r' | 'v') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                entry.desc.columns[column_index].storage.attstorage = storage;
                Ok(((), vec![BootstrapCatalogKind::PgAttribute]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_set_column_compression_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        compression: crate::include::access::htup::AttributeCompression,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if !matches!(entry.relkind, 'r' | 'v') {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                entry.desc.columns[column_index].storage.attcompression = compression;
                Ok(((), vec![BootstrapCatalogKind::PgAttribute]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_alter_column_type_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        new_column: ColumnDesc,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, move |entry, _control| {
                if entry.relkind != 'r' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                entry.desc.columns[column_index] = new_column;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_table_rename_column_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        new_column_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'r' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                if entry.desc.columns.iter().any(|column| {
                    !column.dropped
                        && !column.name.eq_ignore_ascii_case(column_name)
                        && column.name.eq_ignore_ascii_case(new_column_name)
                }) {
                    return Err(CatalogError::TableAlreadyExists(
                        new_column_name.to_string(),
                    ));
                }
                let column_index = relation_column_index_visible(&entry.desc, column_name)?;
                let column = &mut entry.desc.columns[column_index];
                column.name = new_column_name.to_string();
                column.storage.name = column.name.clone();
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_relation_of_type_mvcc(
        &mut self,
        relation_oid: u32,
        of_type_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'r' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                entry.of_type_oid = of_type_oid;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgClass,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if old_entry.of_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, old_entry.of_type_oid);
        }
        if new_entry.of_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, new_entry.of_type_oid);
        }
        Ok(effect)
    }

    pub fn alter_relation_desc_mvcc(
        &mut self,
        relation_oid: u32,
        desc: RelationDesc,
        allowed_relkinds: &[char],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, move |entry, control| {
                if !allowed_relkinds.contains(&entry.relkind) {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                entry.desc = desc;
                if matches!(entry.relkind, 'r' | 'p') {
                    allocate_relation_object_oids(&mut entry.desc, &mut control.next_oid);
                }
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgType,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                        BootstrapCatalogKind::PgAttrdef,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if old_entry.row_type_oid != 0 || new_entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        }
        Ok(effect)
    }

    pub fn rename_relation_mvcc(
        &mut self,
        relation_oid: u32,
        new_name: &str,
        extra_visible_type_rows: &[PgTypeRow],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
        if !matches!(entry.relkind, 'r' | 'p' | 'S' | 'i' | 'I' | 'v') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if self
            .get_relname_relid(ctx, &syscache_relname(new_name), entry.namespace_oid)?
            .is_some_and(|oid| oid != relation_oid)
        {
            return Err(CatalogError::TableAlreadyExists(
                new_name.to_ascii_lowercase(),
            ));
        }
        let mut old_rows = rows_for_existing_relation_mvcc(self, ctx, &entry)?;
        let mut new_rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            rows_for_new_relation_entry(&type_lookup, new_name, &entry)?
        };
        let mut modified_type_oids = Vec::new();
        if entry.row_type_oid != 0 {
            let mut visible_type_rows = BTreeMap::new();
            for row in extra_visible_type_rows {
                visible_type_rows.insert(row.oid, row.clone());
            }
            let mut old_type_rows = BTreeMap::new();
            let mut new_type_rows = BTreeMap::new();
            rename_type_row_mvcc(
                self,
                ctx,
                entry.row_type_oid,
                new_name,
                entry.namespace_oid,
                &mut visible_type_rows,
                &mut old_type_rows,
                &mut new_type_rows,
            )?;
            if !old_type_rows.is_empty() {
                modified_type_oids.extend(new_type_rows.keys().copied());
                old_rows.types = old_type_rows.into_values().collect();
                new_rows.types = new_type_rows.into_values().collect();
            }
        }
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut kinds = vec![
            BootstrapCatalogKind::PgClass,
            BootstrapCatalogKind::PgConstraint,
        ];
        if !old_rows.types.is_empty() || !new_rows.types.is_empty() {
            kinds.insert(1, BootstrapCatalogKind::PgType);
        }
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        for oid in modified_type_oids {
            effect_record_oid(&mut effect.type_oids, oid);
        }
        Ok(effect)
    }

    pub fn move_relation_to_namespace_mvcc(
        &mut self,
        relation_oid: u32,
        namespace_oid: u32,
        _extra_visible_type_rows: &[PgTypeRow],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let current_name = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .map(|row| row.relname)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if self
            .get_relname_relid(ctx, &syscache_relname(&current_name), namespace_oid)?
            .is_some_and(|oid| oid != relation_oid)
        {
            return Err(CatalogError::TableAlreadyExists(current_name.to_string()));
        }
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.namespace_oid = namespace_oid;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgClass,
                        BootstrapCatalogKind::PgType,
                        BootstrapCatalogKind::PgConstraint,
                        BootstrapCatalogKind::PgDepend,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.array_type_oid);
        Ok(effect)
    }

    pub fn rewrite_relation_storage_mvcc(
        &mut self,
        relation_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut control = self.control_state()?;
        let kinds = vec![BootstrapCatalogKind::PgClass];
        let mut old_rows = PhysicalCatalogRows::default();
        let mut new_rows = PhysicalCatalogRows::default();
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);

        for &relation_oid in relation_oids {
            let old_entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
            if !matches!(old_entry.relkind, 'r' | 't' | 'i') {
                return Err(CatalogError::UnknownTable(relation_oid.to_string()));
            }
            let relation_name = class_row_by_oid_mvcc(self, ctx, relation_oid)?
                .map(|row| row.relname)
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            let mut new_entry = old_entry.clone();
            new_entry.rel.rel_number = control.next_rel_number;
            control.next_rel_number = control.next_rel_number.saturating_add(1);
            old_rows
                .classes
                .push(class_row_for_relation_name(&relation_name, &old_entry));
            new_rows
                .classes
                .push(class_row_for_relation_name(&relation_name, &new_entry));
            effect_record_oid(&mut effect.relation_oids, old_entry.relation_oid);
            effect_record_rel(&mut effect.dropped_rels, old_entry.rel);
            effect_record_rel(&mut effect.created_rels, new_entry.rel);
        }

        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;
        self.control = control;
        Ok(effect)
    }

    pub fn alter_relation_owner_mvcc(
        &mut self,
        relation_oid: u32,
        new_owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let old_entry = catalog_entry_by_oid_mvcc(self, ctx, relation_oid)?;
        let relation_name = class_row_by_oid_mvcc(self, ctx, relation_oid)?
            .map(|row| row.relname)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let mut new_entry = old_entry.clone();
        new_entry.owner_oid = new_owner_oid;

        let mut kinds = vec![BootstrapCatalogKind::PgClass];
        if old_entry.row_type_oid != 0 || new_entry.row_type_oid != 0 {
            kinds.push(BootstrapCatalogKind::PgType);
        }
        let old_rows = rows_for_existing_relation_mvcc(self, ctx, &old_entry)?;
        let new_rows = {
            let type_lookup = CatalogStoreTypeLookup { store: &*self, ctx };
            rows_for_new_relation_entry(&type_lookup, &relation_name, &new_entry)?
        };
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if new_entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        }
        Ok(effect)
    }

    pub fn alter_relation_acl_mvcc(
        &mut self,
        relation_oid: u32,
        relacl: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.relacl = relacl;
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn alter_attribute_acl_mvcc(
        &mut self,
        relation_oid: u32,
        attnum: i16,
        attacl: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                let Some(column) = entry
                    .desc
                    .columns
                    .get_mut(attnum.saturating_sub(1) as usize)
                else {
                    return Err(CatalogError::Corrupt("unknown attribute"));
                };
                column.attacl = attacl;
                Ok(((), vec![BootstrapCatalogKind::PgAttribute]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn alter_relation_row_security_mvcc(
        &mut self,
        relation_oid: u32,
        relrowsecurity: Option<bool>,
        relforcerowsecurity: Option<bool>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if let Some(value) = relrowsecurity {
                    entry.relrowsecurity = value;
                }
                if let Some(value) = relforcerowsecurity {
                    entry.relforcerowsecurity = value;
                }
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn set_matview_populated_mvcc(
        &mut self,
        relation_oid: u32,
        relispopulated: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'm' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                entry.relispopulated = relispopulated;
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn alter_view_relation_desc_mvcc(
        &mut self,
        relation_oid: u32,
        desc: RelationDesc,
        reloptions: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'v' {
                    return Err(CatalogError::UnknownTable(relation_oid.to_string()));
                }
                entry.desc = desc;
                entry.reloptions = reloptions;
                Ok((
                    (),
                    vec![
                        BootstrapCatalogKind::PgClass,
                        BootstrapCatalogKind::PgAttribute,
                        BootstrapCatalogKind::PgType,
                    ],
                ))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if old_entry.row_type_oid != 0 || new_entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        }
        Ok(effect)
    }

    pub fn replace_relation_partitioning_mvcc(
        &mut self,
        relation_oid: u32,
        relispartition: bool,
        relpartbound: Option<String>,
        partitioned_table: Option<PgPartitionedTableRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.relispartition = relispartition;
                entry.relpartbound = relpartbound;
                entry.partitioned_table = partitioned_table;
                let mut kinds = vec![BootstrapCatalogKind::PgClass];
                if entry.partitioned_table.is_some() {
                    kinds.push(BootstrapCatalogKind::PgPartitionedTable);
                }
                Ok(((), kinds))
            })?;

        let mut effect = CatalogMutationEffect::default();
        let mut touched_kinds = kinds;
        if old_entry.partitioned_table.is_some() || new_entry.partitioned_table.is_some() {
            merge_catalog_kinds(
                &mut touched_kinds,
                &[BootstrapCatalogKind::PgPartitionedTable],
            );
        }
        effect_record_catalog_kinds(&mut effect, &touched_kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn set_relation_analyze_stats_mvcc(
        &mut self,
        relation_oid: u32,
        relpages: i32,
        reltuples: f64,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.relpages = relpages;
                entry.reltuples = reltuples;
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn set_relation_vacuum_stats_mvcc(
        &mut self,
        relation_oid: u32,
        relpages: i32,
        relallvisible: i32,
        relallfrozen: i32,
        relfrozenxid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.relpages = relpages;
                entry.relallvisible = relallvisible;
                entry.relallfrozen = relallfrozen;
                entry.relfrozenxid = relfrozenxid;
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn set_relation_maintenance_stats_mvcc(
        &mut self,
        relation_oid: u32,
        relpages: i32,
        reltuples: f64,
        relallvisible: i32,
        relallfrozen: i32,
        relfrozenxid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                entry.relpages = relpages;
                entry.reltuples = reltuples;
                entry.relallvisible = relallvisible;
                entry.relallfrozen = relallfrozen;
                entry.relfrozenxid = relfrozenxid;
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn set_relation_import_stats_mvcc(
        &mut self,
        relation_oid: u32,
        relpages: Option<i32>,
        reltuples: Option<f64>,
        relallvisible: Option<i32>,
        relallfrozen: Option<i32>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, _new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if let Some(relpages) = relpages {
                    entry.relpages = relpages;
                }
                if let Some(reltuples) = reltuples {
                    entry.reltuples = reltuples;
                }
                if let Some(relallvisible) = relallvisible {
                    entry.relallvisible = relallvisible;
                }
                if let Some(relallfrozen) = relallfrozen {
                    entry.relallfrozen = relallfrozen;
                }
                Ok(((), vec![BootstrapCatalogKind::PgClass]))
            })?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn replace_relation_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        statistics: Vec<PgStatisticRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = relation_statistics_mvcc(self, ctx, relation_oid)?;

        let kinds = vec![BootstrapCatalogKind::PgStatistic];
        if !existing.is_empty() {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics: existing,
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &kinds,
            )?;
        }
        if !statistics.is_empty() {
            insert_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics,
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &kinds,
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn upsert_relation_statistic_mvcc(
        &mut self,
        row: PgStatisticRow,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = relation_statistics_mvcc(self, ctx, row.starelid)?
            .into_iter()
            .find(|candidate| {
                candidate.staattnum == row.staattnum && candidate.stainherit == row.stainherit
            });

        let kinds = vec![BootstrapCatalogKind::PgStatistic];
        if let Some(existing) = existing {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics: vec![existing],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &kinds,
            )?;
        }
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                statistics: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, row.starelid);
        Ok(effect)
    }

    pub fn delete_relation_statistic_mvcc(
        &mut self,
        relation_oid: u32,
        attnum: i16,
        inherited: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = relation_statistics_mvcc(self, ctx, relation_oid)?
            .into_iter()
            .find(|candidate| candidate.staattnum == attnum && candidate.stainherit == inherited);
        let kinds = vec![BootstrapCatalogKind::PgStatistic];
        if let Some(existing) = existing {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    statistics: vec![existing],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &kinds,
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn comment_relation_mvcc(
        &mut self,
        relation_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(relation_oid, PG_CLASS_RELATION_OID, comment, ctx)
    }

    pub fn comment_type_mvcc(
        &mut self,
        type_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(type_oid, PG_TYPE_RELATION_OID, comment, ctx)
    }

    pub fn comment_column_mvcc(
        &mut self,
        relation_oid: u32,
        attnum: i32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_object_subid_mvcc(relation_oid, PG_CLASS_RELATION_OID, attnum, comment, ctx)
    }

    pub fn copy_relation_column_comments_mvcc(
        &mut self,
        source_relation_oid: u32,
        target_relation_oid: u32,
        max_target_attnum: i32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let source_rows = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            PG_DESCRIPTION_O_C_O_INDEX_OID,
            vec![
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 1,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(source_relation_oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(PG_CLASS_RELATION_OID)),
                },
            ],
        )?
        .into_iter()
        .map(pg_description_row_from_values)
        .collect::<Result<Vec<_>, _>>()?;

        let copied_rows = source_rows
            .into_iter()
            .filter(|row| row.objsubid > 0 && row.objsubid <= max_target_attnum)
            .map(|row| PgDescriptionRow {
                objoid: target_relation_oid,
                classoid: PG_CLASS_RELATION_OID,
                objsubid: row.objsubid,
                description: row.description,
            })
            .collect::<Vec<_>>();
        if copied_rows.is_empty() {
            return Ok(CatalogMutationEffect::default());
        }

        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                descriptions: copied_rows,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgDescription],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgDescription]);
        effect_record_oid(&mut effect.relation_oids, target_relation_oid);
        Ok(effect)
    }

    pub fn copy_object_comment_mvcc(
        &mut self,
        source_object_oid: u32,
        source_classoid: u32,
        target_object_oid: u32,
        target_classoid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let source_rows = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            PG_DESCRIPTION_O_C_O_INDEX_OID,
            vec![
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 1,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(source_object_oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(source_classoid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 3,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int32(0),
                },
            ],
        )?
        .into_iter()
        .map(pg_description_row_from_values)
        .collect::<Result<Vec<_>, _>>()?;
        let Some(source_row) = source_rows.first() else {
            return Ok(CatalogMutationEffect::default());
        };

        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                descriptions: vec![PgDescriptionRow {
                    objoid: target_object_oid,
                    classoid: target_classoid,
                    objsubid: 0,
                    description: source_row.description.clone(),
                }],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgDescription],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgDescription]);
        effect_record_oid(&mut effect.relation_oids, target_object_oid);
        Ok(effect)
    }

    pub fn comment_role_mvcc(
        &mut self,
        role_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(role_oid, PG_AUTHID_RELATION_OID, comment, ctx)
    }

    pub fn comment_rule_mvcc(
        &mut self,
        rewrite_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(rewrite_oid, PG_REWRITE_RELATION_OID, comment, ctx)
    }

    pub fn comment_trigger_mvcc(
        &mut self,
        trigger_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(trigger_oid, PG_TRIGGER_RELATION_OID, comment, ctx)
    }

    pub fn comment_constraint_mvcc(
        &mut self,
        constraint_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(constraint_oid, PG_CONSTRAINT_RELATION_OID, comment, ctx)
    }

    pub fn comment_publication_mvcc(
        &mut self,
        publication_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(publication_oid, PG_PUBLICATION_RELATION_OID, comment, ctx)
    }

    pub fn comment_statistics_mvcc(
        &mut self,
        statistics_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(statistics_oid, PG_STATISTIC_EXT_RELATION_OID, comment, ctx)
    }

    pub fn drop_rule_mvcc(
        &mut self,
        rewrite_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let description_rows =
            description_rows_for_object_mvcc(self, ctx, rewrite_oid, PG_REWRITE_RELATION_OID, 0)?;
        let removed_rewrite = rewrite_row_by_oid_mvcc(self, ctx, rewrite_oid)?;
        let removed_depends =
            depend_rows_for_object_mvcc(self, ctx, PG_REWRITE_RELATION_OID, rewrite_oid)?;

        let rows = PhysicalCatalogRows {
            rewrites: vec![removed_rewrite.clone()],
            depends: removed_depends,
            descriptions: description_rows,
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgDescription,
            BootstrapCatalogKind::PgRewrite,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, removed_rewrite.ev_class);
        Ok(effect)
    }

    fn comment_shared_object_mvcc(
        &mut self,
        object_oid: u32,
        classoid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_object_subid_mvcc(object_oid, classoid, 0, comment, ctx)
    }

    fn comment_object_subid_mvcc(
        &mut self,
        object_oid: u32,
        classoid: u32,
        objsubid: i32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = description_rows_for_object_mvcc(self, ctx, object_oid, classoid, objsubid)?;

        let normalized = comment.and_then(|text| (!text.is_empty()).then_some(text));
        if let Some(existing_row) = existing.first() {
            delete_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    descriptions: vec![existing_row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &[BootstrapCatalogKind::PgDescription],
            )?;
            if let Some(text) = normalized {
                insert_catalog_rows_subset_mvcc(
                    ctx,
                    &PhysicalCatalogRows {
                        descriptions: vec![PgDescriptionRow {
                            objoid: object_oid,
                            classoid,
                            objsubid,
                            description: text.to_string(),
                        }],
                        ..PhysicalCatalogRows::default()
                    },
                    self.scope_db_oid(),
                    &[BootstrapCatalogKind::PgDescription],
                )?;
            }
        } else if let Some(text) = normalized {
            insert_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    descriptions: vec![PgDescriptionRow {
                        objoid: object_oid,
                        classoid,
                        objsubid,
                        description: text.to_string(),
                    }],
                    ..PhysicalCatalogRows::default()
                },
                self.scope_db_oid(),
                &[BootstrapCatalogKind::PgDescription],
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgDescription]);
        effect_record_oid(&mut effect.relation_oids, object_oid);
        Ok(effect)
    }

    pub fn alter_namespace_owner_mvcc(
        &mut self,
        namespace_oid: u32,
        new_owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let existing = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            PG_NAMESPACE_OID_INDEX_OID,
            vec![crate::include::access::scankey::ScanKeyData {
                attribute_number: 1,
                strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int64(i64::from(namespace_oid)),
            }],
        )?
        .into_iter()
        .map(crate::backend::catalog::rowcodec::namespace_row_from_values)
        .collect::<Result<Vec<_>, _>>()?;
        let existing_row = existing
            .first()
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(namespace_oid.to_string()))?;

        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                namespaces: vec![existing_row.clone()],
                ..PhysicalCatalogRows::default()
            },
            1,
            &[BootstrapCatalogKind::PgNamespace],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                namespaces: vec![PgNamespaceRow {
                    nspowner: new_owner_oid,
                    ..existing_row
                }],
                ..PhysicalCatalogRows::default()
            },
            1,
            &[BootstrapCatalogKind::PgNamespace],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgNamespace]);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn alter_namespace_acl_mvcc(
        &mut self,
        namespace_oid: u32,
        nspacl: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let existing_row = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            PG_NAMESPACE_OID_INDEX_OID,
            vec![crate::include::access::scankey::ScanKeyData {
                attribute_number: 1,
                strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int64(i64::from(namespace_oid)),
            }],
        )?
        .into_iter()
        .map(crate::backend::catalog::rowcodec::namespace_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .next()
        .ok_or_else(|| CatalogError::UnknownTable(namespace_oid.to_string()))?;

        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                namespaces: vec![existing_row.clone()],
                ..PhysicalCatalogRows::default()
            },
            1,
            &[BootstrapCatalogKind::PgNamespace],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                namespaces: vec![PgNamespaceRow {
                    nspacl,
                    ..existing_row
                }],
                ..PhysicalCatalogRows::default()
            },
            1,
            &[BootstrapCatalogKind::PgNamespace],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgNamespace]);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn alter_proc_acl_mvcc(
        &mut self,
        proc_oid: u32,
        proacl: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = proc_row_by_oid_mvcc(self, ctx, proc_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(proc_oid.to_string()))?;
        let mut updated = existing.clone();
        updated.proacl = proacl;
        let (_, effect) = self.replace_proc_mvcc(&existing, updated, ctx)?;
        Ok(effect)
    }

    pub fn alter_type_acl_mvcc(
        &mut self,
        type_oid: u32,
        typacl: Option<Vec<String>>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let existing = type_row_by_oid_mvcc(self, ctx, type_oid)?
            .ok_or_else(|| CatalogError::UnknownTable(type_oid.to_string()))?;
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![existing.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgType],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: vec![PgTypeRow { typacl, ..existing }],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgType],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgType]);
        effect_record_oid(&mut effect.type_oids, type_oid);
        Ok(effect)
    }

    pub fn replace_type_rows_mvcc(
        &mut self,
        rows: Vec<PgTypeRow>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut old_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            old_rows.push(
                type_row_by_oid_mvcc(self, ctx, row.oid)?
                    .ok_or_else(|| CatalogError::UnknownType(row.oid.to_string()))?,
            );
        }
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: old_rows,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgType],
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                types: rows.clone(),
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgType],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgType]);
        for row in rows {
            effect_record_oid(&mut effect.type_oids, row.oid);
        }
        Ok(effect)
    }
}

fn visible_catalog_caches_for_ctx(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
) -> Result<(CatCache, RelCache), CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
    let txns = ctx.txns.read();
    let catcache = store.catcache_with_snapshot(&ctx.pool, &txns, &snapshot, ctx.client_id)?;
    let relcache = RelCache::from_catcache_in_db(&catcache, store.scope_db_oid())?;
    Ok((catcache, relcache))
}

trait PgTypeLookup {
    fn type_by_oid(&self, oid: u32) -> Result<Option<PgTypeRow>, CatalogError>;
}

impl PgTypeLookup for CatCache {
    fn type_by_oid(&self, oid: u32) -> Result<Option<PgTypeRow>, CatalogError> {
        Ok(CatCache::type_by_oid(self, oid).cloned())
    }
}

struct CatalogStoreTypeLookup<'a> {
    store: &'a CatalogStore,
    ctx: &'a CatalogWriteContext,
}

impl PgTypeLookup for CatalogStoreTypeLookup<'_> {
    fn type_by_oid(&self, oid: u32) -> Result<Option<PgTypeRow>, CatalogError> {
        Ok(self
            .store
            .search_sys_cache1(self.ctx, SysCacheId::TypeOid, Value::Int64(i64::from(oid)))?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Type(row) => Some(row),
                _ => None,
            }))
    }
}

fn syscache_relname(name: &str) -> String {
    normalize_catalog_name(name)
        .rsplit_once('.')
        .map(|(_, relname)| relname)
        .unwrap_or_else(|| normalize_catalog_name(name))
        .to_ascii_lowercase()
}

fn build_relation_entry(
    _name: String,
    mut desc: RelationDesc,
    namespace_oid: u32,
    db_oid: u32,
    relpersistence: char,
    relkind: char,
    owner_oid: u32,
    of_type_oid: u32,
    control: &mut CatalogControl,
) -> Result<CatalogEntry, CatalogError> {
    validate_builtin_type_rows(&desc)?;

    let relation_oid = control.next_oid;
    let row_type_oid = relation_oid.saturating_add(1);
    let array_type_oid = if row_type_oid != 0 {
        row_type_oid.saturating_add(1)
    } else {
        0
    };
    let mut next_oid = array_type_oid.saturating_add(1);
    if matches!(relkind, 'r' | 'p') {
        allocate_relation_object_oids(&mut desc, &mut next_oid);
    }
    let rel_number = if relkind_has_storage(relkind) {
        control.next_rel_number
    } else {
        0
    };
    let (relpages, reltuples) = if relkind == 'S' {
        (1, 1.0)
    } else if relkind_has_storage(relkind) {
        (0, -1.0)
    } else {
        (0, 0.0)
    };

    let entry = CatalogEntry {
        rel: crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number,
        },
        relation_oid,
        namespace_oid,
        owner_oid,
        relacl: None,
        reloptions: None,
        of_type_oid,
        row_type_oid,
        array_type_oid,
        reltoastrelid: 0,
        relpersistence,
        relkind,
        am_oid: crate::include::catalog::relam_for_relkind(relkind),
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages,
        reltuples,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc,
        partitioned_table: None,
        index_meta: None,
    };
    if relkind_has_storage(relkind) {
        control.next_rel_number = control.next_rel_number.saturating_add(1);
    }
    control.next_oid = next_oid;
    Ok(entry)
}

fn build_index_entry(
    type_lookup: &impl PgTypeLookup,
    index_name: String,
    table: &CatalogEntry,
    unique: bool,
    primary: bool,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    options: &CatalogIndexBuildOptions,
    predicate_sql: Option<&str>,
    control: &mut CatalogControl,
) -> Result<CatalogEntry, CatalogError> {
    build_index_entry_with_relkind(
        type_lookup,
        index_name,
        table,
        unique,
        primary,
        columns,
        options,
        predicate_sql,
        control,
        'i',
        false,
        false,
    )
}

fn build_partitioned_index_entry(
    type_lookup: &impl PgTypeLookup,
    index_name: String,
    table: &CatalogEntry,
    unique: bool,
    primary: bool,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    options: &CatalogIndexBuildOptions,
    predicate_sql: Option<&str>,
    control: &mut CatalogControl,
) -> Result<CatalogEntry, CatalogError> {
    build_index_entry_with_relkind(
        type_lookup,
        index_name,
        table,
        unique,
        primary,
        columns,
        options,
        predicate_sql,
        control,
        'I',
        true,
        true,
    )
}

fn build_index_entry_with_relkind(
    type_lookup: &impl PgTypeLookup,
    index_name: String,
    table: &CatalogEntry,
    unique: bool,
    primary: bool,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    options: &CatalogIndexBuildOptions,
    predicate_sql: Option<&str>,
    control: &mut CatalogControl,
    relkind: char,
    indisready: bool,
    indisvalid: bool,
) -> Result<CatalogEntry, CatalogError> {
    let _ = index_name;
    if relkind == 'i' {
        if !matches!(table.relkind, 'r' | 't' | 'm') {
            return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
        }
    } else if relkind == 'I' {
        if table.relkind != 'p' {
            return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
        }
    } else {
        return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
    }
    let resolved_options = if options.indclass.is_empty()
        && options.indcollation.is_empty()
        && options.indoption.is_empty()
    {
        default_index_build_options_for_relation(type_lookup, table, columns)?
    } else {
        options.clone()
    };

    let mut indkey = Vec::with_capacity(columns.len());
    let mut index_columns = Vec::with_capacity(columns.len());
    let mut used_index_column_names = BTreeSet::new();
    let mut expr_sqls = Vec::new();
    for (position, column_name) in columns.iter().enumerate() {
        if let Some(expr_sql) = column_name.expr_sql.as_deref() {
            indkey.push(0);
            expr_sqls.push(expr_sql.to_string());
            let expr_type = column_name
                .expr_type
                .ok_or(CatalogError::Corrupt("missing expression index sql type"))?;
            let index_type =
                index_column_opckey_sql_type(position, &resolved_options).unwrap_or(expr_type);
            push_unique_index_column(
                &mut index_columns,
                &mut used_index_column_names,
                crate::backend::catalog::catalog::column_desc("expr", index_type, true),
            );
            continue;
        }

        let (attnum, column) = table
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name.eq_ignore_ascii_case(&column_name.name))
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.name.clone()))?;
        indkey.push(attnum.saturating_add(1) as i16);
        let mut column = column.clone();
        column.not_null_constraint_oid = None;
        column.not_null_constraint_name = None;
        column.not_null_constraint_validated = false;
        column.not_null_primary_key_owned = false;
        column.attrdef_oid = None;
        column.default_expr = None;
        if let Some(index_type) = index_column_opckey_sql_type(position, &resolved_options) {
            let nullable = column.storage.nullable;
            column = crate::backend::catalog::catalog::column_desc(
                column.name.clone(),
                index_type,
                nullable,
            );
        }
        push_unique_index_column(&mut index_columns, &mut used_index_column_names, column);
    }

    let key_count = resolved_options.indclass.len();
    if key_count > columns.len()
        || resolved_options.indcollation.len() != key_count
        || resolved_options.indoption.len() != key_count
    {
        return Err(CatalogError::Corrupt("index build options length mismatch"));
    }

    let entry = CatalogEntry {
        rel: crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid: table.rel.db_oid,
            rel_number: if relkind_has_storage(relkind) {
                control.next_rel_number
            } else {
                0
            },
        },
        relation_oid: control.next_oid,
        namespace_oid: table.namespace_oid,
        owner_oid: table.owner_oid,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: table.relpersistence,
        relkind,
        am_oid: resolved_options.am_oid,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: if relkind_has_storage(relkind) { 1 } else { 0 },
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: RelationDesc {
            columns: index_columns,
        },
        partitioned_table: None,
        index_meta: Some(CatalogIndexMeta {
            indrelid: table.relation_oid,
            indkey,
            indisunique: unique,
            indnullsnotdistinct: resolved_options.indnullsnotdistinct,
            indisprimary: primary,
            indisexclusion: resolved_options.indisexclusion,
            indimmediate: resolved_options.indimmediate,
            indisvalid,
            indisready,
            indislive: true,
            indclass: resolved_options.indclass,
            indcollation: resolved_options.indcollation,
            indoption: resolved_options.indoption,
            indexprs: (!expr_sqls.is_empty())
                .then(|| serde_json::to_string(&expr_sqls))
                .transpose()
                .map_err(|_| CatalogError::Corrupt("invalid index expression metadata"))?,
            indpred: predicate_sql
                .map(str::trim)
                .filter(|pred| !pred.is_empty())
                .map(str::to_string),
            brin_options: resolved_options.brin_options.clone(),
            gin_options: resolved_options.gin_options.clone(),
            hash_options: resolved_options.hash_options,
        }),
    };
    control.next_rel_number = control
        .next_rel_number
        .saturating_add(u32::from(relkind_has_storage(relkind)));
    control.next_oid = control.next_oid.saturating_add(1);
    Ok(entry)
}

fn index_column_opckey_sql_type(
    position: usize,
    options: &CatalogIndexBuildOptions,
) -> Option<SqlType> {
    let opclass_oid = *options.indclass.get(position)?;
    let opclass = crate::include::catalog::bootstrap_pg_opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    if opclass.opcmethod != crate::include::catalog::GIST_AM_OID || opclass.opckeytype == 0 {
        return None;
    }
    crate::include::catalog::builtin_type_row_by_oid(opclass.opckeytype).map(|row| row.sql_type)
}

fn push_unique_index_column(
    columns: &mut Vec<ColumnDesc>,
    used_names: &mut BTreeSet<String>,
    mut column: ColumnDesc,
) {
    let base = column.name.clone();
    let mut candidate = base.clone();
    let mut suffix = 1usize;
    while !used_names.insert(candidate.to_ascii_lowercase()) {
        candidate = format!("{base}{suffix}");
        suffix = suffix.saturating_add(1);
    }
    column.name = candidate;
    columns.push(column);
}

fn build_toast_catalog_changes(
    parent_name: &str,
    parent: &CatalogEntry,
    toast_namespace_name: &str,
    toast_namespace_oid: u32,
    control: &mut CatalogControl,
) -> Result<Option<ToastCatalogChanges>, CatalogError> {
    if parent.relkind != 'r'
        || parent.reltoastrelid != 0
        || !relation_needs_toast_table(&parent.desc)
    {
        return Ok(None);
    }

    let old_parent = parent.clone();
    let mut new_parent = parent.clone();
    let toast_name = format!(
        "{toast_namespace_name}.{}",
        toast_relation_name(parent.relation_oid)
    );
    let toast_entry = build_relation_entry(
        toast_name.clone(),
        toast_relation_desc(),
        toast_namespace_oid,
        parent.rel.db_oid,
        parent.relpersistence,
        't',
        parent.owner_oid,
        0,
        control,
    )?;
    new_parent.reltoastrelid = toast_entry.relation_oid;

    let index_name = format!(
        "{toast_namespace_name}.{}",
        toast_index_name(parent.relation_oid)
    );
    let catcache = CatCache::default();
    let mut index_entry = build_index_entry(
        &catcache,
        index_name.clone(),
        &toast_entry,
        true,
        false,
        &[
            crate::include::nodes::parsenodes::IndexColumnDef::from("chunk_id"),
            crate::include::nodes::parsenodes::IndexColumnDef::from("chunk_seq"),
        ],
        &CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass: vec![
                crate::include::catalog::OID_BTREE_OPCLASS_OID,
                crate::include::catalog::INT4_BTREE_OPCLASS_OID,
            ],
            indcollation: vec![0, 0],
            indoption: vec![0, 0],
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            brin_options: None,
            gin_options: None,
            hash_options: None,
        },
        None,
        control,
    )?;
    if let Some(index_meta) = index_entry.index_meta.as_mut() {
        index_meta.indisready = true;
        index_meta.indisvalid = true;
    }

    Ok(Some(ToastCatalogChanges {
        parent_name: parent_name.to_string(),
        old_parent,
        new_parent,
        toast_name,
        toast_entry,
        index_name,
        index_entry,
    }))
}

fn index_constraint_key_attnums(meta: &CatalogIndexMeta) -> Vec<i16> {
    meta.indkey
        .iter()
        .take(meta.indclass.len())
        .copied()
        .collect()
}

fn default_index_build_options_for_relation(
    type_lookup: &impl PgTypeLookup,
    table: &CatalogEntry,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
) -> Result<CatalogIndexBuildOptions, CatalogError> {
    let mut indclass = Vec::with_capacity(columns.len());
    let mut indcollation = Vec::with_capacity(columns.len());
    let mut indoption = Vec::with_capacity(columns.len());
    for column_name in columns {
        let column = table
            .desc
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name.name))
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.name.clone()))?;
        let type_oid = resolved_sql_type_oid(type_lookup, table, column.sql_type)?;
        let opclass_oid = if matches!(
            column.sql_type.element_type().kind,
            crate::backend::parser::SqlTypeKind::Enum
        ) {
            crate::include::catalog::ENUM_BTREE_OPCLASS_OID
        } else {
            crate::include::catalog::default_btree_opclass_oid(type_oid)
                .ok_or_else(|| CatalogError::UnknownType("index column type".into()))?
        };
        indclass.push(opclass_oid);
        indcollation.push(0);
        let mut option = 0i16;
        if column_name.descending {
            option |= 0x0001;
        }
        if column_name.nulls_first.unwrap_or(false) {
            option |= 0x0002;
        }
        indoption.push(option);
    }
    Ok(CatalogIndexBuildOptions {
        am_oid: crate::include::catalog::BTREE_AM_OID,
        indclass,
        indcollation,
        indoption,
        indnullsnotdistinct: false,
        indisexclusion: false,
        indimmediate: true,
        brin_options: None,
        gin_options: None,
        hash_options: None,
    })
}

fn mutate_visible_relation_entry_mvcc<T, F>(
    store: &mut CatalogStore,
    relation_oid: u32,
    ctx: &CatalogWriteContext,
    mutator: F,
) -> Result<(CatalogEntry, CatalogEntry, T, Vec<BootstrapCatalogKind>), CatalogError>
where
    F: FnOnce(
        &mut CatalogEntry,
        &mut CatalogControl,
    ) -> Result<(T, Vec<BootstrapCatalogKind>), CatalogError>,
{
    let relation = store
        .relation_id_get_relation(ctx, relation_oid)?
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    let class_row = class_row_by_oid_mvcc(store, ctx, relation_oid)?
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    let relation_name = class_row.relname.clone();
    let old_entry = catalog_entry_from_relation_row(&class_row, &relation);
    let mut new_entry = old_entry.clone();
    let mut control = store.control_state()?;
    let (extra, kinds) = mutator(&mut new_entry, &mut control)?;
    store.persist_control_values(control.next_oid, control.next_rel_number)?;
    let old_rows = rows_for_existing_relation_mvcc(store, ctx, &old_entry)?;
    let mut new_rows = {
        let type_lookup = CatalogStoreTypeLookup {
            store: &*store,
            ctx,
        };
        rows_for_new_relation_entry(&type_lookup, &relation_name, &new_entry)?
    };
    preserve_non_derived_relation_rows_mvcc(store, ctx, &old_entry, &kinds, &mut new_rows)?;
    delete_catalog_rows_subset_mvcc(ctx, &old_rows, store.scope_db_oid(), &kinds)?;
    insert_catalog_rows_subset_mvcc(ctx, &new_rows, store.scope_db_oid(), &kinds)?;
    store.control = control;
    Ok((old_entry, new_entry, extra, kinds))
}

fn relation_column_index_visible(
    desc: &RelationDesc,
    column_name: &str,
) -> Result<usize, CatalogError> {
    desc.columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| CatalogError::UnknownColumn(column_name.to_string()))
}

fn not_null_constraint_column_index_visible(
    desc: &RelationDesc,
    constraint_name: &str,
) -> Result<usize, CatalogError> {
    desc.columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped
                && column
                    .not_null_constraint_name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(constraint_name)))
            .then_some(index)
        })
        .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))
}

fn inherited_parent_column_match_count(parents: &[RelCacheEntry], column_name: &str) -> usize {
    parents
        .iter()
        .filter(|parent| {
            parent
                .desc
                .columns
                .iter()
                .any(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        })
        .count()
}

fn inherited_parent_not_null_match_count(parents: &[RelCacheEntry], column_name: &str) -> usize {
    parents
        .iter()
        .filter(|parent| {
            parent.desc.columns.iter().any(|column| {
                !column.dropped
                    && column.name.eq_ignore_ascii_case(column_name)
                    && !column.storage.nullable
                    && !column.not_null_constraint_no_inherit
            })
        })
        .count()
}

fn inherited_parent_check_match_count(
    catcache: &CatCache,
    parents: &[RelCacheEntry],
    child_constraint: &PgConstraintRow,
) -> usize {
    parents
        .iter()
        .filter(|parent| {
            catcache
                .constraint_rows_for_relation(parent.relation_oid)
                .into_iter()
                .any(|parent_constraint| {
                    parent_constraint.contype == CONSTRAINT_CHECK
                        && !parent_constraint.connoinherit
                        && parent_constraint
                            .conname
                            .eq_ignore_ascii_case(&child_constraint.conname)
                        && parent_constraint.conbin == child_constraint.conbin
                })
        })
        .count()
}

fn inherited_parent_check_match_count_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    parents: &[RelCacheEntry],
    child_constraint: &PgConstraintRow,
) -> Result<usize, CatalogError> {
    let mut count = 0;
    for parent in parents {
        if relation_constraints_mvcc(store, ctx, parent.relation_oid)?
            .into_iter()
            .any(|parent_constraint| {
                parent_constraint.contype == CONSTRAINT_CHECK
                    && !parent_constraint.connoinherit
                    && parent_constraint
                        .conname
                        .eq_ignore_ascii_case(&child_constraint.conname)
                    && parent_constraint.conbin == child_constraint.conbin
            })
        {
            count += 1;
        }
    }
    Ok(count)
}

fn rewrite_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    rewrite_oid: u32,
) -> Result<PgRewriteRow, CatalogError> {
    store
        .search_sys_cache1(ctx, SysCacheId::RewriteOid, oid_key(rewrite_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Rewrite(row) => Some(row),
            _ => None,
        })
        .ok_or_else(|| CatalogError::UnknownTable(rewrite_oid.to_string()))
}

fn policy_row_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    policy_name: &str,
) -> Result<PgPolicyRow, CatalogError> {
    store
        .search_sys_cache2(
            ctx,
            SysCacheId::PolicyPolrelidPolname,
            oid_key(relation_oid),
            Value::Text(policy_name.to_ascii_lowercase().into()),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Policy(row) => Some(row),
            _ => None,
        })
        .ok_or_else(|| CatalogError::UnknownTable(policy_name.to_string()))
}

fn foreign_key_equality_operators_visible(indclass: &[u32]) -> Option<Vec<u32>> {
    let opclasses = crate::include::catalog::bootstrap_pg_opclass_rows();
    let amops = crate::include::catalog::bootstrap_pg_amop_rows();
    indclass
        .iter()
        .map(|opclass_oid| {
            let family = opclasses
                .iter()
                .find(|row| row.oid == *opclass_oid)?
                .opcfamily;
            amops
                .iter()
                .find(|row| row.amopfamily == family && row.amopstrategy == 3)
                .map(|row| row.amopopr)
        })
        .collect()
}

fn preserve_non_derived_relation_rows(
    catcache: &CatCache,
    entry: &CatalogEntry,
    kinds: &[BootstrapCatalogKind],
    new_rows: &mut PhysicalCatalogRows,
) {
    if !matches!(entry.relkind, 'r' | 'p') {
        return;
    }

    let preserved_constraints = catcache
        .constraint_rows_for_relation(entry.relation_oid)
        .into_iter()
        .filter(|row| row.contype != crate::include::catalog::CONSTRAINT_NOTNULL)
        .collect::<Vec<_>>();

    if kinds.contains(&BootstrapCatalogKind::PgConstraint) {
        new_rows.constraints.extend(preserved_constraints.clone());
    }
    if kinds.contains(&BootstrapCatalogKind::PgDepend) {
        let preserved_constraint_oids = preserved_constraints
            .iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        new_rows.depends.extend(
            catcache
                .depend_rows()
                .into_iter()
                .filter(|row| preserved_constraint_oids.contains(&row.objid)),
        );
        sort_pg_depend_rows(&mut new_rows.depends);
    }
}

fn preserve_non_derived_relation_rows_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    entry: &CatalogEntry,
    kinds: &[BootstrapCatalogKind],
    new_rows: &mut PhysicalCatalogRows,
) -> Result<(), CatalogError> {
    if !matches!(entry.relkind, 'r' | 'p') {
        return Ok(());
    }

    let preserved_constraints = relation_constraints_mvcc(store, ctx, entry.relation_oid)?
        .into_iter()
        .filter(|row| row.contype != crate::include::catalog::CONSTRAINT_NOTNULL)
        .collect::<Vec<_>>();

    if kinds.contains(&BootstrapCatalogKind::PgConstraint) {
        new_rows.constraints.extend(preserved_constraints.clone());
    }
    if kinds.contains(&BootstrapCatalogKind::PgDepend) {
        for row in &preserved_constraints {
            new_rows.depends.extend(depend_rows_for_object_mvcc(
                store,
                ctx,
                PG_CONSTRAINT_RELATION_OID,
                row.oid,
            )?);
        }
        sort_pg_depend_rows(&mut new_rows.depends);
    }

    Ok(())
}

fn dropped_column_name_visible(attnum: usize) -> String {
    format!("........pg.dropped.{attnum}........")
}

fn rows_for_new_relation_entry(
    type_lookup: &impl PgTypeLookup,
    relation_name: &str,
    entry: &CatalogEntry,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut rows = PhysicalCatalogRows::default();
    rows.classes
        .push(class_row_for_relation_name(relation_name, entry));
    rows.types
        .extend(type_rows_for_relation_name(relation_name, entry));

    rows.attributes.extend(
        entry
            .desc
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let atttypid = resolved_sql_type_oid(type_lookup, entry, column.sql_type)?;
                Ok(PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid,
                    attlen: column.storage.attlen,
                    attnum: idx.saturating_add(1) as i16,
                    attnotnull: !column.storage.nullable,
                    attisdropped: column.dropped,
                    atttypmod: column.sql_type.typmod,
                    attalign: column.storage.attalign,
                    attstorage: column.storage.attstorage,
                    attcompression: column.storage.attcompression,
                    attstattarget: column.attstattarget,
                    attinhcount: column.attinhcount,
                    attislocal: column.attislocal,
                    attidentity: column
                        .identity
                        .map(|kind| kind.catalog_char())
                        .unwrap_or('\0'),
                    attgenerated: column
                        .generated
                        .map(|kind| kind.catalog_char())
                        .unwrap_or('\0'),
                    attcollation: crate::backend::catalog::catalog::catalog_attribute_collation_oid(
                        entry.relation_oid,
                        column.collation_oid,
                    ),
                    attacl: column.attacl.clone(),
                    attoptions: None,
                    attfdwoptions: None,
                    attmissingval: None,
                    attbyval: crate::include::catalog::builtin_type_row_by_oid(atttypid)
                        .is_some_and(|row| row.typbyval),
                    sql_type: column.sql_type,
                })
            })
            .collect::<Result<Vec<_>, CatalogError>>()?,
    );
    rows.attrdefs.extend(
        entry
            .desc
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                Some(PgAttrdefRow {
                    oid: column.attrdef_oid?,
                    adrelid: entry.relation_oid,
                    adnum: idx.saturating_add(1) as i16,
                    adbin: column.default_expr.clone()?,
                })
            }),
    );
    rows.constraints
        .extend(constraint_rows_for_relation_name(relation_name, entry));
    rows.depends.extend(derived_pg_depend_rows(entry));
    if let Some(row) = &entry.partitioned_table {
        rows.partitioned_tables.push(row.clone());
    }
    if let Some(index_row) = index_row_for_entry(entry) {
        rows.indexes.push(index_row);
    }
    sort_pg_depend_rows(&mut rows.depends);
    Ok(rows)
}

fn rename_visible_type_row(
    type_oid: u32,
    new_type_name: &str,
    namespace_oid: u32,
    visible_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    old_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    new_type_rows: &mut BTreeMap<u32, PgTypeRow>,
) -> Result<(), CatalogError> {
    let Some(current_row) = visible_type_rows.get(&type_oid).cloned() else {
        return Err(CatalogError::UnknownType(type_oid.to_string()));
    };
    if current_row.typnamespace != namespace_oid {
        return Err(CatalogError::Corrupt(
            "type namespace mismatch during relation rename",
        ));
    }
    if current_row.typname.eq_ignore_ascii_case(new_type_name) {
        return Ok(());
    }

    let array_oid = current_row.typarray;
    let mut moved_own_array_type = false;
    while let Some(conflicting_type_oid) =
        conflicting_type_oid_for_name(visible_type_rows, type_oid, new_type_name, namespace_oid)
    {
        if autogenerated_array_type(visible_type_rows, conflicting_type_oid) {
            let moved_name =
                available_array_type_name(visible_type_rows, new_type_name, namespace_oid);
            if conflicting_type_oid == array_oid {
                moved_own_array_type = true;
            }
            rename_visible_type_row(
                conflicting_type_oid,
                &moved_name,
                namespace_oid,
                visible_type_rows,
                old_type_rows,
                new_type_rows,
            )?;
        } else {
            return Err(CatalogError::TypeAlreadyExists(
                new_type_name.to_ascii_lowercase(),
            ));
        }
    }

    stage_visible_type_row_rename(
        type_oid,
        new_type_name,
        visible_type_rows,
        old_type_rows,
        new_type_rows,
    )?;

    if array_oid != 0 && !moved_own_array_type {
        let array_name = available_array_type_name(visible_type_rows, new_type_name, namespace_oid);
        rename_visible_type_row(
            array_oid,
            &array_name,
            namespace_oid,
            visible_type_rows,
            old_type_rows,
            new_type_rows,
        )?;
    }

    Ok(())
}

fn conflicting_type_oid_for_name(
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    type_oid: u32,
    type_name: &str,
    namespace_oid: u32,
) -> Option<u32> {
    visible_type_rows.values().find_map(|row| {
        (row.oid != type_oid
            && row.typnamespace == namespace_oid
            && row.typname.eq_ignore_ascii_case(type_name))
        .then_some(row.oid)
    })
}

fn autogenerated_array_type(visible_type_rows: &BTreeMap<u32, PgTypeRow>, type_oid: u32) -> bool {
    let Some(array_row) = visible_type_rows.get(&type_oid) else {
        return false;
    };
    if array_row.typelem == 0 {
        return false;
    }
    visible_type_rows
        .get(&array_row.typelem)
        .is_some_and(|element_row| element_row.typarray == array_row.oid)
}

fn available_array_type_name(
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    base_type_name: &str,
    namespace_oid: u32,
) -> String {
    let first_choice = format!("_{base_type_name}");
    if conflicting_type_oid_for_name(visible_type_rows, 0, &first_choice, namespace_oid).is_none() {
        return first_choice;
    }
    for suffix in 1.. {
        let candidate = format!("_{base_type_name}_{suffix}");
        if conflicting_type_oid_for_name(visible_type_rows, 0, &candidate, namespace_oid).is_none()
        {
            return candidate;
        }
    }
    unreachable!("array type name search should always find a free name")
}

fn rename_type_row_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    type_oid: u32,
    new_type_name: &str,
    namespace_oid: u32,
    visible_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    old_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    new_type_rows: &mut BTreeMap<u32, PgTypeRow>,
) -> Result<(), CatalogError> {
    let Some(current_row) =
        type_row_from_visible_or_store_mvcc(store, ctx, visible_type_rows, type_oid)?
    else {
        return Err(CatalogError::UnknownType(type_oid.to_string()));
    };
    if current_row.typnamespace != namespace_oid {
        return Err(CatalogError::Corrupt(
            "type namespace mismatch during relation rename",
        ));
    }
    if current_row.typname.eq_ignore_ascii_case(new_type_name) {
        return Ok(());
    }

    visible_type_rows.insert(type_oid, current_row.clone());
    let array_oid = current_row.typarray;
    let mut moved_own_array_type = false;
    while let Some(conflicting_type_oid) = conflicting_type_oid_for_name_mvcc(
        store,
        ctx,
        visible_type_rows,
        type_oid,
        new_type_name,
        namespace_oid,
    )? {
        if autogenerated_array_type_mvcc(store, ctx, visible_type_rows, conflicting_type_oid)? {
            let moved_name = available_array_type_name_mvcc(
                store,
                ctx,
                visible_type_rows,
                new_type_name,
                namespace_oid,
            )?;
            if conflicting_type_oid == array_oid {
                moved_own_array_type = true;
            }
            rename_type_row_mvcc(
                store,
                ctx,
                conflicting_type_oid,
                &moved_name,
                namespace_oid,
                visible_type_rows,
                old_type_rows,
                new_type_rows,
            )?;
        } else {
            return Err(CatalogError::TypeAlreadyExists(
                new_type_name.to_ascii_lowercase(),
            ));
        }
    }

    stage_visible_type_row_rename(
        type_oid,
        new_type_name,
        visible_type_rows,
        old_type_rows,
        new_type_rows,
    )?;

    if array_oid != 0 && !moved_own_array_type {
        let array_name = available_array_type_name_mvcc(
            store,
            ctx,
            visible_type_rows,
            new_type_name,
            namespace_oid,
        )?;
        rename_type_row_mvcc(
            store,
            ctx,
            array_oid,
            &array_name,
            namespace_oid,
            visible_type_rows,
            old_type_rows,
            new_type_rows,
        )?;
    }

    Ok(())
}

fn conflicting_type_oid_for_name_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    type_oid: u32,
    type_name: &str,
    namespace_oid: u32,
) -> Result<Option<u32>, CatalogError> {
    if let Some(oid) =
        conflicting_type_oid_for_name(visible_type_rows, type_oid, type_name, namespace_oid)
    {
        return Ok(Some(oid));
    }
    Ok(
        type_row_by_name_namespace_mvcc(store, ctx, type_name, namespace_oid)?
            .filter(|row| row.oid != type_oid && !visible_type_rows.contains_key(&row.oid))
            .map(|row| row.oid),
    )
}

fn autogenerated_array_type_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    type_oid: u32,
) -> Result<bool, CatalogError> {
    let array_row = type_row_from_visible_or_store_mvcc(store, ctx, visible_type_rows, type_oid)?;
    let Some(array_row) = array_row else {
        return Ok(false);
    };
    if array_row.typelem == 0 {
        return Ok(false);
    }
    let element_row =
        type_row_from_visible_or_store_mvcc(store, ctx, visible_type_rows, array_row.typelem)?;
    Ok(element_row.is_some_and(|element_row| element_row.typarray == array_row.oid))
}

fn type_row_from_visible_or_store_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    type_oid: u32,
) -> Result<Option<PgTypeRow>, CatalogError> {
    if let Some(row) = visible_type_rows.get(&type_oid) {
        return Ok(Some(row.clone()));
    }
    type_row_by_oid_mvcc(store, ctx, type_oid)
}

fn available_array_type_name_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    visible_type_rows: &BTreeMap<u32, PgTypeRow>,
    base_type_name: &str,
    namespace_oid: u32,
) -> Result<String, CatalogError> {
    let first_choice = format!("_{base_type_name}");
    if conflicting_type_oid_for_name_mvcc(
        store,
        ctx,
        visible_type_rows,
        0,
        &first_choice,
        namespace_oid,
    )?
    .is_none()
    {
        return Ok(first_choice);
    }
    for suffix in 1.. {
        let candidate = format!("_{base_type_name}_{suffix}");
        if conflicting_type_oid_for_name_mvcc(
            store,
            ctx,
            visible_type_rows,
            0,
            &candidate,
            namespace_oid,
        )?
        .is_none()
        {
            return Ok(candidate);
        }
    }
    unreachable!("array type name search should always find a free name")
}

fn stage_visible_type_row_rename(
    type_oid: u32,
    new_type_name: &str,
    visible_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    old_type_rows: &mut BTreeMap<u32, PgTypeRow>,
    new_type_rows: &mut BTreeMap<u32, PgTypeRow>,
) -> Result<(), CatalogError> {
    let Some(current_row) = visible_type_rows.get(&type_oid).cloned() else {
        return Err(CatalogError::UnknownType(type_oid.to_string()));
    };
    old_type_rows
        .entry(type_oid)
        .or_insert_with(|| current_row.clone());
    let mut renamed_row = current_row;
    renamed_row.typname = new_type_name.to_ascii_lowercase();
    visible_type_rows.insert(type_oid, renamed_row.clone());
    new_type_rows.insert(type_oid, renamed_row);
    Ok(())
}

fn relation_object_name(relation_name: &str) -> &str {
    relation_name
        .rsplit_once('.')
        .map(|(_, object)| object)
        .unwrap_or(relation_name)
}

fn class_row_for_relation_name(relation_name: &str, entry: &CatalogEntry) -> PgClassRow {
    PgClassRow {
        oid: entry.relation_oid,
        relname: relation_object_name(relation_name).to_string(),
        relnamespace: entry.namespace_oid,
        reltype: entry.row_type_oid,
        relowner: entry.owner_oid,
        relam: entry.am_oid,
        relfilenode: entry.rel.rel_number,
        reltablespace: 0,
        relpages: entry.relpages,
        reltuples: entry.reltuples,
        relallvisible: entry.relallvisible,
        relallfrozen: entry.relallfrozen,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relnatts: entry.desc.columns.len() as i16,
        relhassubclass: entry.relhassubclass,
        relhastriggers: entry.relhastriggers,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        relispopulated: entry.relispopulated,
        relispartition: entry.relispartition,
        relfrozenxid: entry.relfrozenxid,
        relpartbound: entry.relpartbound.clone(),
        reloptions: entry.reloptions.clone(),
        relacl: entry.relacl.clone(),
        relreplident: 'd',
        reloftype: entry.of_type_oid,
    }
}

fn type_rows_for_relation_name(
    relation_name: &str,
    entry: &CatalogEntry,
) -> Vec<crate::include::catalog::PgTypeRow> {
    let relname = relation_object_name(relation_name);
    let mut rows = Vec::new();
    if entry.row_type_oid != 0 {
        rows.push(crate::include::catalog::composite_type_row(
            relname,
            entry.row_type_oid,
            entry.namespace_oid,
            entry.relation_oid,
            entry.array_type_oid,
        ));
    }
    if entry.array_type_oid != 0 {
        rows.push(crate::include::catalog::composite_array_type_row(
            relname,
            entry.array_type_oid,
            entry.namespace_oid,
            entry.row_type_oid,
            entry.relation_oid,
        ));
    }
    rows
}

fn constraint_rows_for_relation_name(
    relation_name: &str,
    entry: &CatalogEntry,
) -> Vec<PgConstraintRow> {
    if matches!(entry.relkind, 'r' | 'p') {
        return derived_pg_constraint_rows(
            entry.relation_oid,
            relation_object_name(relation_name),
            entry.namespace_oid,
            &entry.desc,
        );
    }
    Vec::new()
}

fn index_row_for_entry(entry: &CatalogEntry) -> Option<crate::include::catalog::PgIndexRow> {
    let index_meta = entry.index_meta.as_ref()?;
    Some(crate::include::catalog::PgIndexRow {
        indexrelid: entry.relation_oid,
        indrelid: index_meta.indrelid,
        indnatts: index_meta.indkey.len() as i16,
        indnkeyatts: index_meta.indclass.len() as i16,
        indisunique: index_meta.indisunique,
        indnullsnotdistinct: index_meta.indnullsnotdistinct,
        indisprimary: index_meta.indisprimary,
        indisexclusion: index_meta.indisexclusion,
        indimmediate: index_meta.indimmediate,
        indisclustered: false,
        indisvalid: index_meta.indisvalid,
        indcheckxmin: false,
        indisready: index_meta.indisready,
        indislive: index_meta.indislive,
        indisreplident: false,
        indkey: index_meta.indkey.clone(),
        indcollation: index_meta.indcollation.clone(),
        indclass: index_meta.indclass.clone(),
        indoption: index_meta.indoption.clone(),
        indexprs: index_meta.indexprs.clone(),
        indpred: index_meta.indpred.clone(),
    })
}

fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn class_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Option<PgClassRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::RelOid, oid_key(relation_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Class(row) => Some(row),
            _ => None,
        }))
}

fn proc_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    proc_oid: u32,
) -> Result<Option<PgProcRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::ProcOid, oid_key(proc_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Proc(row) => Some(row),
            _ => None,
        }))
}

fn aggregate_row_by_fnoid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    proc_oid: u32,
) -> Result<Option<PgAggregateRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::AggFnoid, oid_key(proc_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Aggregate(row) => Some(row),
            _ => None,
        }))
}

fn operator_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    operator_oid: u32,
) -> Result<Option<PgOperatorRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::OperOid, oid_key(operator_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Operator(row) => Some(row),
            _ => None,
        }))
}

fn cast_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    cast_oid: u32,
) -> Result<Option<PgCastRow>, CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
    Ok(probe_system_catalog_rows_visible_in_db(
        &ctx.pool,
        &ctx.txns,
        &snapshot,
        ctx.client_id,
        store.scope_db_oid(),
        PG_CAST_OID_INDEX_OID,
        vec![ScanKeyData {
            attribute_number: 1,
            strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
            argument: Value::Int64(i64::from(cast_oid)),
        }],
    )?
    .into_iter()
    .filter_map(|values| pg_cast_row_from_values(values).ok())
    .next())
}

fn cast_row_by_source_target_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    source_oid: u32,
    target_oid: u32,
) -> Result<Option<PgCastRow>, CatalogError> {
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
    Ok(probe_system_catalog_rows_visible_in_db(
        &ctx.pool,
        &ctx.txns,
        &snapshot,
        ctx.client_id,
        store.scope_db_oid(),
        PG_CAST_SOURCE_TARGET_INDEX_OID,
        vec![
            ScanKeyData {
                attribute_number: 1,
                strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int64(i64::from(source_oid)),
            },
            ScanKeyData {
                attribute_number: 2,
                strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int64(i64::from(target_oid)),
            },
        ],
    )?
    .into_iter()
    .filter_map(|values| pg_cast_row_from_values(values).ok())
    .next())
}

fn type_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    type_oid: u32,
) -> Result<Option<PgTypeRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::TypeOid, oid_key(type_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row),
            _ => None,
        }))
}

fn type_row_by_name_namespace_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    type_name: &str,
    namespace_oid: u32,
) -> Result<Option<PgTypeRow>, CatalogError> {
    Ok(store
        .search_sys_cache2(
            ctx,
            SysCacheId::TypeNameNsp,
            Value::Text(type_name.to_ascii_lowercase().into()),
            oid_key(namespace_oid),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row),
            _ => None,
        }))
}

fn relation_constraints_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgConstraintRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::ConstraintRelId, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Constraint(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn relation_constraint_exists_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    constraint_name: &str,
    contype: Option<char>,
) -> Result<bool, CatalogError> {
    Ok(relation_constraints_mvcc(store, ctx, relation_oid)?
        .into_iter()
        .any(|row| {
            contype.is_none_or(|expected| row.contype == expected)
                && row.conname.eq_ignore_ascii_case(constraint_name)
        }))
}

fn relation_constraint_row_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    constraint_name: &str,
    contype: Option<char>,
) -> Result<PgConstraintRow, CatalogError> {
    relation_constraints_mvcc(store, ctx, relation_oid)?
        .into_iter()
        .find(|row| {
            contype.is_none_or(|expected| row.contype == expected)
                && row.conname.eq_ignore_ascii_case(constraint_name)
        })
        .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))
}

fn relation_constraint_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    constraint_oid: u32,
) -> Result<Option<PgConstraintRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::ConstraintOid, oid_key(constraint_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Constraint(row) => Some(row),
            _ => None,
        }))
}

fn relation_attributes_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgAttributeRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(ctx, SysCacheId::AttrNum, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Attribute(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.attnum);
    Ok(rows)
}

fn relation_attrdefs_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgAttrdefRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(ctx, SysCacheId::AttrDefault, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Attrdef(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.adnum);
    Ok(rows)
}

fn relation_inherits_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgInheritsRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(ctx, SysCacheId::InheritsRelIdSeqNo, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Inherits(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    crate::include::catalog::sort_pg_inherits_rows(&mut rows);
    Ok(rows)
}

fn relation_inherited_by_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgInheritsRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(ctx, SysCacheId::InheritsParent, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Inherits(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    Ok(rows)
}

fn relation_policies_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgPolicyRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(
            ctx,
            SysCacheId::PolicyPolrelidPolname,
            oid_key(relation_oid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Policy(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.polrelid, row.polname.clone(), row.oid));
    Ok(rows)
}

fn relation_rewrites_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgRewriteRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::RuleRelName, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Rewrite(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn relation_triggers_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<crate::include::catalog::PgTriggerRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::TriggerRelidName, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Trigger(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn trigger_row_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    trigger_name: &str,
) -> Result<crate::include::catalog::PgTriggerRow, CatalogError> {
    relation_triggers_mvcc(store, ctx, relation_oid)?
        .into_iter()
        .find(|row| row.tgname.eq_ignore_ascii_case(trigger_name))
        .ok_or_else(|| CatalogError::UnknownTable(trigger_name.to_string()))
}

fn partitioned_table_row_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Option<PgPartitionedTableRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::PartRelId, oid_key(relation_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::PartitionedTable(row) => Some(row),
            _ => None,
        }))
}

fn relation_statistics_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgStatisticRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::StatRelAttInh, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Statistic(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn relation_statistic_ext_rows_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgStatisticExtRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::StatisticExtRelId, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::StatisticExt(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn statistic_ext_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    statistics_oid: u32,
) -> Result<Option<PgStatisticExtRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::StatExtOid, oid_key(statistics_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::StatisticExt(row) => Some(row),
            _ => None,
        }))
}

fn statistic_ext_row_by_name_namespace_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    name: &str,
    namespace_oid: u32,
) -> Result<Option<PgStatisticExtRow>, CatalogError> {
    Ok(store
        .search_sys_cache2(
            ctx,
            SysCacheId::StatExtNameNsp,
            Value::Text(name.to_ascii_lowercase().into()),
            oid_key(namespace_oid),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::StatisticExt(row) => Some(row),
            _ => None,
        }))
}

fn statistic_ext_data_rows_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    statistics_oid: u32,
) -> Result<Vec<PgStatisticExtDataRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(
            ctx,
            SysCacheId::StatisticExtDataStxoidInh,
            oid_key(statistics_oid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::StatisticExtData(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn index_rows_for_relation_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<crate::include::catalog::PgIndexRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::IndexIndRelId, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Index(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn publication_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    publication_oid: u32,
) -> Result<Option<PgPublicationRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::PublicationOid, oid_key(publication_oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Publication(row) => Some(row),
            _ => None,
        }))
}

fn publication_row_by_name_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    name: &str,
) -> Result<Option<PgPublicationRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(
            ctx,
            SysCacheId::PublicationName,
            Value::Text(name.to_ascii_lowercase().into()),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Publication(row) => Some(row),
            _ => None,
        }))
}

fn publication_rel_rows_for_relation_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<Vec<PgPublicationRelRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(ctx, SysCacheId::PublicationRelMap, oid_key(relation_oid))?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::PublicationRel(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn publication_rel_rows_for_publication_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    publication_oid: u32,
) -> Result<Vec<PgPublicationRelRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list1(
            ctx,
            SysCacheId::PublicationRelPrpubid,
            oid_key(publication_oid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::PublicationRel(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn publication_namespace_row_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    oid: u32,
) -> Result<Option<PgPublicationNamespaceRow>, CatalogError> {
    Ok(store
        .search_sys_cache1(ctx, SysCacheId::PublicationNamespace, oid_key(oid))?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::PublicationNamespace(row) => Some(row),
            _ => None,
        }))
}

fn publication_namespace_rows_for_publication_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    publication_oid: u32,
) -> Result<Vec<PgPublicationNamespaceRow>, CatalogError> {
    let mut rows = Vec::new();
    for depend in depend_rows_referencing_object_mvcc(
        store,
        ctx,
        PG_PUBLICATION_RELATION_OID,
        publication_oid,
    )? {
        if depend.classid != PG_PUBLICATION_NAMESPACE_RELATION_OID || depend.objsubid != 0 {
            continue;
        }
        if let Some(row) = publication_namespace_row_by_oid_mvcc(store, ctx, depend.objid)? {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| (row.pnpubid, row.pnnspid, row.oid));
    rows.dedup_by_key(|row| row.oid);
    Ok(rows)
}

fn publication_namespace_rows_for_namespace_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    namespace_oid: u32,
) -> Result<Vec<PgPublicationNamespaceRow>, CatalogError> {
    let mut rows = store
        .search_sys_cache_list1(
            ctx,
            SysCacheId::PublicationNamespaceMap,
            oid_key(namespace_oid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::PublicationNamespace(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.pnnspid, row.pnpubid, row.oid));
    Ok(rows)
}

fn depend_rows_for_object_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    classid: u32,
    objid: u32,
) -> Result<Vec<PgDependRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list2(
            ctx,
            SysCacheId::DependDepender,
            oid_key(classid),
            oid_key(objid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Depend(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn depend_rows_referencing_object_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    refclassid: u32,
    refobjid: u32,
) -> Result<Vec<PgDependRow>, CatalogError> {
    Ok(store
        .search_sys_cache_list2(
            ctx,
            SysCacheId::DependReference,
            oid_key(refclassid),
            oid_key(refobjid),
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Depend(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn constraint_depend_rows_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    constraint_oid: u32,
) -> Result<Vec<PgDependRow>, CatalogError> {
    let mut rows =
        depend_rows_for_object_mvcc(store, ctx, PG_CONSTRAINT_RELATION_OID, constraint_oid)?;
    rows.extend(depend_rows_referencing_object_mvcc(
        store,
        ctx,
        PG_CONSTRAINT_RELATION_OID,
        constraint_oid,
    )?);
    sort_pg_depend_rows(&mut rows);
    Ok(rows)
}

fn description_rows_for_object_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    objoid: u32,
    classoid: u32,
    objsubid: i32,
) -> Result<Vec<PgDescriptionRow>, CatalogError> {
    Ok(store
        .search_sys_cache(
            ctx,
            SysCacheId::DescriptionObj,
            vec![oid_key(objoid), oid_key(classoid), Value::Int32(objsubid)],
        )?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Description(row) => Some(row),
            _ => None,
        })
        .collect())
}

fn collect_depend_rows_for_object_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    classid: u32,
    objid: u32,
    rows: &mut Vec<PgDependRow>,
) -> Result<(), CatalogError> {
    rows.extend(depend_rows_for_object_mvcc(store, ctx, classid, objid)?);
    Ok(())
}

fn catalog_entry_from_relation_row(
    class_row: &PgClassRow,
    relation: &RelCacheEntry,
) -> CatalogEntry {
    CatalogEntry {
        rel: relation.rel,
        relation_oid: relation.relation_oid,
        namespace_oid: relation.namespace_oid,
        owner_oid: relation.owner_oid,
        relacl: class_row.relacl.clone(),
        reloptions: class_row.reloptions.clone(),
        of_type_oid: class_row.reloftype,
        row_type_oid: relation.row_type_oid,
        array_type_oid: relation.array_type_oid,
        reltoastrelid: relation.reltoastrelid,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: class_row.relam,
        relhassubclass: class_row.relhassubclass,
        relhastriggers: relation.relhastriggers,
        relispartition: class_row.relispartition,
        relispopulated: class_row.relispopulated,
        relpartbound: class_row.relpartbound.clone(),
        relrowsecurity: class_row.relrowsecurity,
        relforcerowsecurity: class_row.relforcerowsecurity,
        relpages: class_row.relpages,
        reltuples: class_row.reltuples,
        relallvisible: class_row.relallvisible,
        relallfrozen: class_row.relallfrozen,
        relfrozenxid: class_row.relfrozenxid,
        desc: relation.desc.clone(),
        partitioned_table: relation.partitioned_table.clone(),
        index_meta: relation.index.as_ref().map(|index| CatalogIndexMeta {
            indrelid: index.indrelid,
            indkey: index.indkey.clone(),
            indisunique: index.indisunique,
            indnullsnotdistinct: index.indnullsnotdistinct,
            indisprimary: index.indisprimary,
            indisexclusion: index.indisexclusion,
            indimmediate: index.indimmediate,
            indisvalid: index.indisvalid,
            indisready: index.indisready,
            indislive: index.indislive,
            indclass: index.indclass.clone(),
            indcollation: index.indcollation.clone(),
            indoption: index.indoption.clone(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
            brin_options: index.brin_options.clone(),
            gin_options: index.gin_options.clone(),
            hash_options: index.hash_options,
        }),
    }
}

fn catalog_entry_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<CatalogEntry, CatalogError> {
    catalog_entry_by_oid_mvcc_with_extra_type_rows(store, ctx, relation_oid, &[])
}

fn catalog_entry_by_oid_mvcc_with_extra_type_rows(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    extra_type_rows: &[PgTypeRow],
) -> Result<CatalogEntry, CatalogError> {
    let relation = store
        .relation_id_get_relation_with_extra_type_rows(ctx, relation_oid, extra_type_rows)?
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    let class_row = class_row_by_oid_mvcc(store, ctx, relation_oid)?
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    Ok(catalog_entry_from_relation_row(&class_row, &relation))
}

fn rows_for_existing_relation_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    entry: &CatalogEntry,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let class_row = class_row_by_oid_mvcc(store, ctx, entry.relation_oid)?
        .ok_or_else(|| CatalogError::UnknownTable(entry.relation_oid.to_string()))?;
    let attributes = relation_attributes_mvcc(store, ctx, entry.relation_oid)?;
    let attrdefs = relation_attrdefs_mvcc(store, ctx, entry.relation_oid)?;
    let rewrites = relation_rewrites_mvcc(store, ctx, entry.relation_oid)?;
    let triggers = relation_triggers_mvcc(store, ctx, entry.relation_oid)?;
    let inherits = relation_inherits_mvcc(store, ctx, entry.relation_oid)?;
    let constraints = if matches!(entry.relkind, 'r' | 'p') {
        relation_constraints_mvcc(store, ctx, entry.relation_oid)?
    } else {
        Vec::new()
    };

    let mut rows = PhysicalCatalogRows {
        classes: vec![class_row],
        attributes,
        attrdefs,
        rewrites,
        triggers,
        inherits,
        partitioned_tables: partitioned_table_row_mvcc(store, ctx, entry.relation_oid)?
            .into_iter()
            .collect(),
        constraints,
        ..PhysicalCatalogRows::default()
    };
    if entry.row_type_oid != 0
        && let Some(row) = type_row_by_oid_mvcc(store, ctx, entry.row_type_oid)?
    {
        rows.types.push(row);
    }
    if entry.array_type_oid != 0
        && let Some(row) = type_row_by_oid_mvcc(store, ctx, entry.array_type_oid)?
    {
        rows.types.push(row);
    }
    if matches!(entry.relkind, 'i' | 'I') {
        rows.indexes.extend(index_rows_for_relation_mvcc(
            store,
            ctx,
            entry.relation_oid,
        )?);
    }

    collect_depend_rows_for_object_mvcc(
        store,
        ctx,
        PG_CLASS_RELATION_OID,
        entry.relation_oid,
        &mut rows.depends,
    )?;
    if entry.row_type_oid != 0 {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_TYPE_RELATION_OID,
            entry.row_type_oid,
            &mut rows.depends,
        )?;
    }
    if entry.array_type_oid != 0 {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_TYPE_RELATION_OID,
            entry.array_type_oid,
            &mut rows.depends,
        )?;
    }
    for row in &rows.attrdefs {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_ATTRDEF_RELATION_OID,
            row.oid,
            &mut rows.depends,
        )?;
    }
    for row in &rows.rewrites {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_REWRITE_RELATION_OID,
            row.oid,
            &mut rows.depends,
        )?;
    }
    for row in &rows.triggers {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_TRIGGER_RELATION_OID,
            row.oid,
            &mut rows.depends,
        )?;
    }
    for row in &rows.constraints {
        collect_depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_CONSTRAINT_RELATION_OID,
            row.oid,
            &mut rows.depends,
        )?;
    }
    sort_pg_depend_rows(&mut rows.depends);
    rows.depends.dedup();
    Ok(rows)
}

fn rows_for_drop_relation_entry_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    entry: &CatalogEntry,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut rows = rows_for_existing_relation_mvcc(store, ctx, entry)?;

    rows.policies
        .extend(relation_policies_mvcc(store, ctx, entry.relation_oid)?);
    rows.statistics
        .extend(relation_statistics_mvcc(store, ctx, entry.relation_oid)?);

    for statistic_ext in relation_statistic_ext_rows_mvcc(store, ctx, entry.relation_oid)? {
        rows.depends.extend(depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_STATISTIC_EXT_RELATION_OID,
            statistic_ext.oid,
        )?);
        rows.statistics_ext_data
            .extend(statistic_ext_data_rows_mvcc(store, ctx, statistic_ext.oid)?);
        rows.descriptions.extend(description_rows_for_object_mvcc(
            store,
            ctx,
            statistic_ext.oid,
            PG_STATISTIC_EXT_RELATION_OID,
            0,
        )?);
        rows.statistics_ext.push(statistic_ext);
    }

    collect_relation_description_rows_mvcc(store, ctx, &mut rows)?;
    sort_pg_depend_rows(&mut rows.depends);
    rows.depends.dedup();
    dedup_description_rows(&mut rows.descriptions);
    Ok(rows)
}

fn collect_relation_description_rows_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    rows: &mut PhysicalCatalogRows,
) -> Result<(), CatalogError> {
    let mut object_keys = Vec::new();
    object_keys.extend(
        rows.classes
            .iter()
            .map(|row| (row.oid, PG_CLASS_RELATION_OID)),
    );
    object_keys.extend(rows.types.iter().map(|row| (row.oid, PG_TYPE_RELATION_OID)));
    object_keys.extend(
        rows.attrdefs
            .iter()
            .map(|row| (row.oid, PG_ATTRDEF_RELATION_OID)),
    );
    object_keys.extend(
        rows.constraints
            .iter()
            .map(|row| (row.oid, PG_CONSTRAINT_RELATION_OID)),
    );
    object_keys.extend(
        rows.rewrites
            .iter()
            .map(|row| (row.oid, PG_REWRITE_RELATION_OID)),
    );
    object_keys.extend(
        rows.triggers
            .iter()
            .map(|row| (row.oid, PG_TRIGGER_RELATION_OID)),
    );
    object_keys.extend(
        rows.policies
            .iter()
            .map(|row| (row.oid, PG_POLICY_RELATION_OID)),
    );
    object_keys.extend(
        rows.statistics_ext
            .iter()
            .map(|row| (row.oid, PG_STATISTIC_EXT_RELATION_OID)),
    );

    for (objoid, classoid) in object_keys {
        rows.descriptions.extend(description_rows_for_object_mvcc(
            store, ctx, objoid, classoid, 0,
        )?);
    }
    Ok(())
}

fn dedup_description_rows(rows: &mut Vec<PgDescriptionRow>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| seen.insert((row.objoid, row.classoid, row.objsubid)));
}

fn rows_for_existing_relation(
    catcache: &CatCache,
    entry: &CatalogEntry,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let class_row = catcache
        .class_by_oid(entry.relation_oid)
        .cloned()
        .ok_or_else(|| CatalogError::UnknownTable(entry.relation_oid.to_string()))?;
    let attributes = catcache
        .attributes_by_relid(entry.relation_oid)
        .unwrap_or(&[])
        .to_vec();
    let attrdefs = attributes
        .iter()
        .filter_map(|attribute| {
            catcache
                .attrdef_by_relid_attnum(entry.relation_oid, attribute.attnum)
                .cloned()
        })
        .collect::<Vec<_>>();
    let rewrites = catcache.rewrite_rows_for_relation(entry.relation_oid);
    let triggers = catcache.trigger_rows_for_relation(entry.relation_oid);
    let inherits = catcache
        .inherit_rows()
        .into_iter()
        .filter(|row| row.inhrelid == entry.relation_oid)
        .collect::<Vec<_>>();
    let constraints = if matches!(entry.relkind, 'r' | 'p') {
        catcache.constraint_rows_for_relation(entry.relation_oid)
    } else {
        Vec::new()
    };
    let mut object_oids = BTreeSet::from([entry.relation_oid]);
    if entry.row_type_oid != 0 {
        object_oids.insert(entry.row_type_oid);
    }
    if entry.array_type_oid != 0 {
        object_oids.insert(entry.array_type_oid);
    }
    object_oids.extend(attrdefs.iter().map(|row| row.oid));
    object_oids.extend(rewrites.iter().map(|row| row.oid));
    object_oids.extend(triggers.iter().map(|row| row.oid));
    let constraint_oids = constraints
        .iter()
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();

    let mut rows = PhysicalCatalogRows {
        classes: vec![class_row],
        attributes,
        attrdefs,
        rewrites,
        triggers,
        inherits,
        partitioned_tables: catcache
            .partitioned_table_row(entry.relation_oid)
            .cloned()
            .into_iter()
            .collect(),
        constraints,
        ..PhysicalCatalogRows::default()
    };
    if entry.row_type_oid != 0
        && let Some(row) = catcache.type_by_oid(entry.row_type_oid).cloned()
    {
        rows.types.push(row);
    }
    if entry.array_type_oid != 0
        && let Some(row) = catcache.type_by_oid(entry.array_type_oid).cloned()
    {
        rows.types.push(row);
    }
    if matches!(entry.relkind, 'i' | 'I') {
        rows.indexes.extend(
            catcache
                .index_rows()
                .into_iter()
                .filter(|row| row.indexrelid == entry.relation_oid),
        );
    }
    rows.depends.extend(
        catcache
            .depend_rows()
            .into_iter()
            .filter(|row| object_oids.contains(&row.objid) || constraint_oids.contains(&row.objid)),
    );
    sort_pg_depend_rows(&mut rows.depends);
    Ok(rows)
}

fn catalog_entry_from_visible_relation(
    catcache: &CatCache,
    relation: &RelCacheEntry,
) -> Result<CatalogEntry, CatalogError> {
    let class_row = catcache
        .class_by_oid(relation.relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation.relation_oid.to_string()))?;
    Ok(CatalogEntry {
        rel: relation.rel,
        relation_oid: relation.relation_oid,
        namespace_oid: relation.namespace_oid,
        owner_oid: relation.owner_oid,
        relacl: class_row.relacl.clone(),
        reloptions: class_row.reloptions.clone(),
        of_type_oid: class_row.reloftype,
        row_type_oid: relation.row_type_oid,
        array_type_oid: relation.array_type_oid,
        reltoastrelid: relation.reltoastrelid,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: class_row.relam,
        relhassubclass: class_row.relhassubclass,
        relhastriggers: relation.relhastriggers,
        relispartition: class_row.relispartition,
        relispopulated: class_row.relispopulated,
        relpartbound: class_row.relpartbound.clone(),
        relrowsecurity: class_row.relrowsecurity,
        relforcerowsecurity: class_row.relforcerowsecurity,
        relpages: class_row.relpages,
        reltuples: class_row.reltuples,
        relallvisible: class_row.relallvisible,
        relallfrozen: class_row.relallfrozen,
        relfrozenxid: class_row.relfrozenxid,
        desc: relation.desc.clone(),
        partitioned_table: relation.partitioned_table.clone(),
        index_meta: relation.index.as_ref().map(|index| CatalogIndexMeta {
            indrelid: index.indrelid,
            indkey: index.indkey.clone(),
            indisunique: index.indisunique,
            indnullsnotdistinct: index.indnullsnotdistinct,
            indisprimary: index.indisprimary,
            indisexclusion: index.indisexclusion,
            indimmediate: index.indimmediate,
            indisvalid: index.indisvalid,
            indisready: index.indisready,
            indislive: index.indislive,
            indclass: index.indclass.clone(),
            indcollation: index.indcollation.clone(),
            indoption: index.indoption.clone(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
            brin_options: index.brin_options.clone(),
            gin_options: index.gin_options.clone(),
            hash_options: index.hash_options,
        }),
    })
}

fn catalog_entry_from_relation(relation: &RelCacheEntry) -> CatalogEntry {
    CatalogEntry {
        rel: relation.rel,
        relation_oid: relation.relation_oid,
        namespace_oid: relation.namespace_oid,
        owner_oid: relation.owner_oid,
        relacl: None,
        reloptions: None,
        of_type_oid: relation.of_type_oid,
        row_type_oid: relation.row_type_oid,
        array_type_oid: relation.array_type_oid,
        reltoastrelid: relation.reltoastrelid,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: relation
            .index
            .as_ref()
            .map(|index| index.am_oid)
            .unwrap_or_else(|| crate::include::catalog::relam_for_relkind(relation.relkind)),
        relhassubclass: false,
        relhastriggers: relation.relhastriggers,
        relispartition: relation.relispartition,
        relispopulated: relation.relispopulated,
        relpartbound: relation.relpartbound.clone(),
        relrowsecurity: relation.relrowsecurity,
        relforcerowsecurity: relation.relforcerowsecurity,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: relation.desc.clone(),
        partitioned_table: relation.partitioned_table.clone(),
        index_meta: relation.index.as_ref().map(|index| CatalogIndexMeta {
            indrelid: index.indrelid,
            indkey: index.indkey.clone(),
            indisunique: index.indisunique,
            indnullsnotdistinct: index.indnullsnotdistinct,
            indisprimary: index.indisprimary,
            indisexclusion: index.indisexclusion,
            indimmediate: index.indimmediate,
            indisvalid: index.indisvalid,
            indisready: index.indisready,
            indislive: index.indislive,
            indclass: index.indclass.clone(),
            indcollation: index.indcollation.clone(),
            indoption: index.indoption.clone(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
            brin_options: index.brin_options.clone(),
            gin_options: index.gin_options.clone(),
            hash_options: index.hash_options,
        }),
    }
}

fn resolved_sql_type_oid(
    type_lookup: &impl PgTypeLookup,
    entry: &CatalogEntry,
    sql_type: crate::backend::parser::SqlType,
) -> Result<u32, CatalogError> {
    if sql_type.is_array
        && matches!(
            sql_type.kind,
            crate::backend::parser::SqlTypeKind::Composite
                | crate::backend::parser::SqlTypeKind::Record
        )
        && sql_type.type_oid != 0
    {
        if sql_type.type_oid == entry.row_type_oid && entry.array_type_oid != 0 {
            return Ok(entry.array_type_oid);
        }
        if let Some(row) = type_lookup.type_by_oid(sql_type.type_oid)?
            && row.typarray != 0
        {
            return Ok(row.typarray);
        }
    }
    if sql_type.is_array
        && sql_type.type_oid != 0
        && let Some(row) = type_lookup.type_by_oid(sql_type.type_oid)?
        && row.typarray != 0
    {
        return Ok(row.typarray);
    }
    Ok(sql_type_oid(sql_type))
}

fn relkind_is_droppable_table(relkind: char) -> bool {
    matches!(relkind, 'r' | 'p' | 'm')
}

fn has_nonpartition_inherited_children_visible(catcache: &CatCache, relation_oid: u32) -> bool {
    catcache.inherit_rows().iter().any(|row| {
        row.inhparent == relation_oid
            && match catcache.class_by_oid(row.inhrelid) {
                Some(child) => !child.relispartition,
                None => true,
            }
    })
}

fn has_nonpartition_inherited_children_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<bool, CatalogError> {
    for row in relation_inherited_by_mvcc(store, ctx, relation_oid)? {
        let Some(child) = class_row_by_oid_mvcc(store, ctx, row.inhrelid)? else {
            return Ok(true);
        };
        if !child.relispartition {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_nonpartition_inherited_children(catalog: &Catalog, relation_oid: u32) -> bool {
    catalog.inherit_rows().iter().any(|row| {
        row.inhparent == relation_oid
            && match catalog.get_by_oid(row.inhrelid) {
                Some(child) => !child.relispartition,
                None => true,
            }
    })
}

fn drop_relation_entries_visible(
    catcache: &CatCache,
    relcache: &RelCache,
    relation_oid: u32,
) -> Result<
    (
        PhysicalCatalogRows,
        PhysicalCatalogRows,
        Vec<CatalogEntry>,
        Vec<u32>,
    ),
    CatalogError,
> {
    let oids = drop_relation_oids_by_oid_visible(relcache, &catcache.depend_rows(), relation_oid)?;
    let dropped = oids
        .iter()
        .copied()
        .map(|oid| {
            let relation = relcache
                .get_by_oid(oid)
                .ok_or_else(|| CatalogError::UnknownTable(oid.to_string()))?;
            catalog_entry_from_visible_relation(catcache, relation)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let dropped_oids = dropped
        .iter()
        .map(|entry| entry.relation_oid)
        .collect::<BTreeSet<_>>();
    let affected_parent_oids = dropped
        .iter()
        .flat_map(|entry| {
            catcache
                .inherit_rows()
                .into_iter()
                .filter(move |row| row.inhrelid == entry.relation_oid)
                .map(|row| row.inhparent)
        })
        .filter(|parent_oid| !dropped_oids.contains(parent_oid))
        .collect::<BTreeSet<_>>();

    let mut rows_to_delete = PhysicalCatalogRows::default();
    for entry in &dropped {
        extend_physical_catalog_rows(
            &mut rows_to_delete,
            rows_for_existing_relation(catcache, entry)?,
        );
        extend_physical_catalog_rows(
            &mut rows_to_delete,
            publication_rows_for_relation(catcache, entry.relation_oid),
        );
    }

    let mut rows_to_insert = PhysicalCatalogRows::default();
    for parent_oid in &affected_parent_oids {
        let Some(old_parent) = catcache.class_by_oid(*parent_oid).cloned() else {
            continue;
        };
        let has_remaining = catcache
            .inherit_rows()
            .into_iter()
            .any(|row| row.inhparent == *parent_oid && !dropped_oids.contains(&row.inhrelid));
        if old_parent.relhassubclass == has_remaining {
            continue;
        }
        rows_to_delete.classes.push(old_parent.clone());
        rows_to_insert.classes.push(PgClassRow {
            relhassubclass: has_remaining,
            ..old_parent
        });
    }

    Ok((
        rows_to_delete,
        rows_to_insert,
        dropped,
        affected_parent_oids.into_iter().collect(),
    ))
}

fn drop_relation_entries_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    extra_type_rows: &[PgTypeRow],
) -> Result<
    (
        PhysicalCatalogRows,
        PhysicalCatalogRows,
        Vec<CatalogEntry>,
        Vec<u32>,
    ),
    CatalogError,
> {
    let oids = drop_relation_oids_by_oid_mvcc(store, ctx, relation_oid, extra_type_rows)?;
    let dropped = oids
        .iter()
        .copied()
        .map(|oid| catalog_entry_by_oid_mvcc_with_extra_type_rows(store, ctx, oid, extra_type_rows))
        .collect::<Result<Vec<_>, _>>()?;
    let dropped_oids = dropped
        .iter()
        .map(|entry| entry.relation_oid)
        .collect::<BTreeSet<_>>();
    let mut affected_parent_oids = BTreeSet::new();
    for entry in &dropped {
        affected_parent_oids.extend(
            relation_inherits_mvcc(store, ctx, entry.relation_oid)?
                .into_iter()
                .map(|row| row.inhparent)
                .filter(|parent_oid| !dropped_oids.contains(parent_oid)),
        );
    }

    let mut rows_to_delete = PhysicalCatalogRows::default();
    for entry in &dropped {
        extend_physical_catalog_rows(
            &mut rows_to_delete,
            rows_for_drop_relation_entry_mvcc(store, ctx, entry)?,
        );
        extend_physical_catalog_rows(
            &mut rows_to_delete,
            publication_rows_for_relation_mvcc(store, ctx, entry.relation_oid)?,
        );
    }

    let mut rows_to_insert = PhysicalCatalogRows::default();
    for parent_oid in &affected_parent_oids {
        let Some(old_parent) = class_row_by_oid_mvcc(store, ctx, *parent_oid)? else {
            continue;
        };
        let has_remaining = relation_inherited_by_mvcc(store, ctx, *parent_oid)?
            .into_iter()
            .any(|row| !dropped_oids.contains(&row.inhrelid));
        if old_parent.relhassubclass == has_remaining {
            continue;
        }
        rows_to_delete.classes.push(old_parent.clone());
        rows_to_insert.classes.push(PgClassRow {
            relhassubclass: has_remaining,
            ..old_parent
        });
    }

    Ok((
        rows_to_delete,
        rows_to_insert,
        dropped,
        affected_parent_oids.into_iter().collect(),
    ))
}

fn publication_rows_for_relation(catcache: &CatCache, relation_oid: u32) -> PhysicalCatalogRows {
    let publication_rels = catcache.publication_rel_rows_for_relation(relation_oid);
    if publication_rels.is_empty() {
        return PhysicalCatalogRows::default();
    }
    let publication_rel_oids = publication_rels
        .iter()
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    let mut rows = PhysicalCatalogRows {
        publication_rels,
        depends: catcache
            .depend_rows()
            .into_iter()
            .filter(|row| publication_rel_oids.contains(&row.objid))
            .collect(),
        ..PhysicalCatalogRows::default()
    };
    sort_pg_depend_rows(&mut rows.depends);
    rows
}

fn publication_rows_for_relation_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let publication_rels = publication_rel_rows_for_relation_mvcc(store, ctx, relation_oid)?;
    if publication_rels.is_empty() {
        return Ok(PhysicalCatalogRows::default());
    }

    let mut rows = PhysicalCatalogRows {
        publication_rels,
        ..PhysicalCatalogRows::default()
    };
    for row in &rows.publication_rels {
        rows.depends.extend(depend_rows_for_object_mvcc(
            store,
            ctx,
            PG_PUBLICATION_REL_RELATION_OID,
            row.oid,
        )?);
    }
    sort_pg_depend_rows(&mut rows.depends);
    Ok(rows)
}

fn drop_relation_oids_by_oid_visible(
    relcache: &RelCache,
    depend_rows: &[PgDependRow],
    relation_oid: u32,
) -> Result<Vec<u32>, CatalogError> {
    let entry = relcache
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if !(relkind_is_droppable_table(entry.relkind) || entry.relkind == 'S') {
        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
    }
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    collect_relation_drop_oids_visible(relcache, depend_rows, relation_oid, &mut seen, &mut order);
    Ok(order)
}

fn drop_relation_oids_by_oid_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    extra_type_rows: &[PgTypeRow],
) -> Result<Vec<u32>, CatalogError> {
    let entry = store
        .relation_id_get_relation_with_extra_type_rows(ctx, relation_oid, extra_type_rows)?
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if !(relkind_is_droppable_table(entry.relkind) || entry.relkind == 'S') {
        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
    }
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    collect_relation_drop_oids_mvcc(
        store,
        ctx,
        relation_oid,
        extra_type_rows,
        &mut seen,
        &mut order,
    )?;
    Ok(order)
}

fn collect_relation_drop_oids_visible(
    relcache: &RelCache,
    depend_rows: &[PgDependRow],
    relation_oid: u32,
    seen: &mut BTreeSet<u32>,
    order: &mut Vec<u32>,
) {
    if !seen.insert(relation_oid) {
        return;
    }

    for row in depend_rows {
        if row.refclassid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.refobjid != relation_oid
            || row.classid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.objsubid != 0
        {
            continue;
        }
        if let Some(dependent) = relcache.get_by_oid(row.objid) {
            if !matches!(dependent.relkind, 'r' | 'i' | 'I' | 't' | 'S') {
                continue;
            }
            collect_relation_drop_oids_visible(
                relcache,
                depend_rows,
                dependent.relation_oid,
                seen,
                order,
            );
        }
    }

    order.push(relation_oid);
}

fn collect_relation_drop_oids_mvcc(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
    relation_oid: u32,
    extra_type_rows: &[PgTypeRow],
    seen: &mut BTreeSet<u32>,
    order: &mut Vec<u32>,
) -> Result<(), CatalogError> {
    if !seen.insert(relation_oid) {
        return Ok(());
    }

    for row in depend_rows_referencing_object_mvcc(store, ctx, PG_CLASS_RELATION_OID, relation_oid)?
    {
        if row.classid != PG_CLASS_RELATION_OID || row.objsubid != 0 {
            continue;
        }
        if let Some(dependent) =
            store.relation_id_get_relation_with_extra_type_rows(ctx, row.objid, extra_type_rows)?
        {
            if !matches!(dependent.relkind, 'r' | 'i' | 'I' | 't' | 'S') {
                continue;
            }
            collect_relation_drop_oids_mvcc(
                store,
                ctx,
                dependent.relation_oid,
                extra_type_rows,
                seen,
                order,
            )?;
        }
    }

    order.push(relation_oid);
    Ok(())
}

fn drop_relation_oids_by_oid(
    catalog: &Catalog,
    relation_oid: u32,
) -> Result<Vec<u32>, CatalogError> {
    let entry = catalog
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if !(relkind_is_droppable_table(entry.relkind) || entry.relkind == 'S') {
        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
    }
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    collect_relation_drop_oids(
        catalog,
        catalog.depend_rows(),
        relation_oid,
        &mut seen,
        &mut order,
    );
    Ok(order)
}

fn collect_relation_drop_oids(
    catalog: &Catalog,
    depend_rows: &[PgDependRow],
    relation_oid: u32,
    seen: &mut BTreeSet<u32>,
    order: &mut Vec<u32>,
) {
    if !seen.insert(relation_oid) {
        return;
    }

    for row in depend_rows {
        if row.refclassid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.refobjid != relation_oid
            || row.classid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.objsubid != 0
        {
            continue;
        }
        if let Some(dependent) = catalog.get_by_oid(row.objid) {
            if !matches!(dependent.relkind, 'r' | 'i' | 'I' | 't' | 'S') {
                continue;
            }
            collect_relation_drop_oids(catalog, depend_rows, dependent.relation_oid, seen, order);
        }
    }

    order.push(relation_oid);
}

fn parse_proc_argtype_oids(argtypes: &str) -> Vec<u32> {
    argtypes
        .split_ascii_whitespace()
        .filter_map(|part| part.parse::<u32>().ok())
        .collect()
}

fn effect_record_catalog_kinds(effect: &mut CatalogMutationEffect, kinds: &[BootstrapCatalogKind]) {
    for &kind in kinds {
        if !effect.touched_catalogs.contains(&kind) {
            effect.touched_catalogs.push(kind);
        }
    }
}

fn effect_record_rel(
    rels: &mut Vec<crate::backend::storage::smgr::RelFileLocator>,
    rel: crate::backend::storage::smgr::RelFileLocator,
) {
    if !rels.contains(&rel) {
        rels.push(rel);
    }
}

fn effect_record_oid(oids: &mut Vec<u32>, oid: u32) {
    if !oids.contains(&oid) {
        oids.push(oid);
    }
}

fn add_catalog_entry_rows(
    target: &mut PhysicalCatalogRows,
    catalog: &Catalog,
    relation_name: &str,
    entry: &CatalogEntry,
) {
    extend_physical_catalog_rows(
        target,
        physical_catalog_rows_for_catalog_entry(catalog, relation_name, entry),
    );
}

fn merge_catalog_kinds(target: &mut Vec<BootstrapCatalogKind>, kinds: &[BootstrapCatalogKind]) {
    for &kind in kinds {
        if !target.contains(&kind) {
            target.push(kind);
        }
    }
}

fn record_toast_effects(effect: &mut CatalogMutationEffect, toast: &ToastCatalogChanges) {
    effect_record_rel(&mut effect.created_rels, toast.toast_entry.rel);
    effect_record_oid(&mut effect.relation_oids, toast.toast_entry.relation_oid);
    effect_record_oid(&mut effect.namespace_oids, toast.toast_entry.namespace_oid);
    effect_record_oid(&mut effect.type_oids, toast.toast_entry.row_type_oid);
    effect_record_rel(&mut effect.created_rels, toast.index_entry.rel);
    effect_record_oid(&mut effect.relation_oids, toast.index_entry.relation_oid);
    effect_record_oid(&mut effect.namespace_oids, toast.index_entry.namespace_oid);
}
