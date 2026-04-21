use std::collections::BTreeSet;

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
    derived_pg_depend_rows, foreign_data_wrapper_depend_rows, foreign_key_constraint_depend_rows,
    index_backed_constraint_depend_rows, inheritance_depend_rows,
    primary_key_owned_not_null_depend_rows, proc_depend_rows, relation_constraint_depend_rows,
    relation_rule_depend_rows, sort_pg_depend_rows, trigger_depend_rows, view_rewrite_depend_rows,
};
use crate::backend::catalog::rowcodec::{
    pg_description_row_from_values, pg_statistic_row_from_values,
};
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
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name, sql_type_oid};
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::include::catalog::{
    BootstrapCatalogKind, CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, DEPENDENCY_NORMAL,
    PG_AM_RELATION_OID, PG_AMOP_RELATION_OID, PG_AMPROC_RELATION_OID, PG_AUTHID_RELATION_OID,
    PG_CLASS_RELATION_OID, PG_FOREIGN_DATA_WRAPPER_RELATION_OID, PG_NAMESPACE_RELATION_OID,
    PG_OPCLASS_RELATION_OID, PG_OPERATOR_RELATION_OID, PG_OPFAMILY_RELATION_OID,
    PG_PROC_RELATION_OID, PG_REWRITE_RELATION_OID, PG_TYPE_RELATION_OID, PgAmopRow, PgAmprocRow,
    PgAttrdefRow, PgAttributeRow, PgClassRow, PgConstraintRow, PgDatabaseRow, PgDependRow,
    PgDescriptionRow, PgForeignDataWrapperRow, PgInheritsRow, PgNamespaceRow, PgOpclassRow,
    PgOpfamilyRow, PgProcRow, PgRewriteRow, PgStatisticRow, PgTablespaceRow, relkind_has_storage,
};
use crate::include::nodes::datum::Value;

use super::{
    CatalogControl, CatalogMutationEffect, CatalogStore, CatalogStoreMode, CatalogWriteContext,
    CreateTableResult, RuleOwnerDependency,
};

const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
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
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        if relcache.get_by_name(&name).is_some() {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            &catcache,
            name.clone(),
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            relkind,
            owner_oid,
            &mut control,
        )?;
        let kinds = create_table_sync_kinds(&entry);
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&catcache, &name, &entry)?;
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
                &catcache,
                name.clone(),
                desc,
                crate::include::catalog::PUBLIC_NAMESPACE_OID,
                self.scope_db_oid(),
                'p',
                'r',
                crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                &mut control,
            )?;
            let toast = build_toast_catalog_changes(
                &catcache,
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
            if entry.relkind != 'r' {
                return Err(CatalogError::UnknownTable(name.to_string()));
            }
            return self.drop_relation_by_oid(entry.relation_oid);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog
            .get(name)
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        if entry.relkind != 'r' {
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
        let name = name.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        if relcache.get_by_name(&name).is_some() {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            &catcache,
            name.clone(),
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            'r',
            owner_oid,
            &mut control,
        )?;
        let toast = build_toast_catalog_changes(
            &catcache,
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
        let mut rows = rows_for_new_relation_entry(&catcache, &name, &entry)?;
        if let Some(toast) = &toast {
            extend_physical_catalog_rows(
                &mut rows,
                rows_for_new_relation_entry(&catcache, &toast.toast_name, &toast.toast_entry)?,
            );
            extend_physical_catalog_rows(
                &mut rows,
                rows_for_new_relation_entry(&catcache, &toast.index_name, &toast.index_entry)?,
            );
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
        let namespace_oid = self.allocate_next_oid(namespace_oid)?;
        let kinds = [BootstrapCatalogKind::PgNamespace];
        if !namespace_name.starts_with("pg_temp_") && !namespace_name.starts_with("pg_toast_temp_")
        {
            self.invalidate_relcache_init_for_kinds(&kinds);
        }
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
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

    pub fn drop_proc_by_oid_mvcc(
        &mut self,
        proc_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgProcRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let proc_row = catcache
            .proc_by_oid(proc_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(proc_oid.to_string()))?;
        let mut referenced_type_oids = parse_proc_argtype_oids(&proc_row.proargtypes);
        if let Some(all_arg_types) = &proc_row.proallargtypes {
            referenced_type_oids.extend(all_arg_types.iter().copied());
        }
        let kinds = [BootstrapCatalogKind::PgProc, BootstrapCatalogKind::PgDepend];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                procs: vec![proc_row.clone()],
                depends: proc_depend_rows(
                    proc_row.oid,
                    proc_row.pronamespace,
                    proc_row.prorettype,
                    &referenced_type_oids,
                ),
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        Ok((proc_row, effect))
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let existing_rel_triggers = catcache.trigger_rows_for_relation(row.tgrelid);
        if existing_rel_triggers
            .iter()
            .any(|existing| existing.tgname.eq_ignore_ascii_case(&row.tgname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_trigger_tgrelid_tgname_index".into(),
            ));
        }
        let old_class = catcache
            .class_by_oid(row.tgrelid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(row.tgrelid.to_string()))?;
        let mut control = self.control_state()?;
        if row.oid == 0 {
            row.oid = control.next_oid;
        }
        control.next_oid = control.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let mut insert_rows = PhysicalCatalogRows {
            triggers: vec![row.clone()],
            depends: trigger_depend_rows(row.oid, row.tgrelid, row.tgfoid),
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_visible = trigger_row_visible(&catcache, old_row.tgrelid, &old_row.tgname)?;
        let new_relation_triggers = catcache.trigger_rows_for_relation(row.tgrelid);
        if new_relation_triggers.iter().any(|existing| {
            existing.oid != old_visible.oid && existing.tgname.eq_ignore_ascii_case(&row.tgname)
        }) {
            return Err(CatalogError::UniqueViolation(
                "pg_trigger_tgrelid_tgname_index".into(),
            ));
        }
        row.oid = old_visible.oid;
        let old_depends =
            trigger_depend_rows(old_visible.oid, old_visible.tgrelid, old_visible.tgfoid);
        let new_depends = trigger_depend_rows(row.oid, row.tgrelid, row.tgfoid);

        let old_class = catcache
            .class_by_oid(old_visible.tgrelid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(old_visible.tgrelid.to_string()))?;
        let new_class = catcache
            .class_by_oid(row.tgrelid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(row.tgrelid.to_string()))?;

        let old_has_remaining = catcache
            .trigger_rows_for_relation(old_visible.tgrelid)
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_trigger = trigger_row_visible(&catcache, relation_oid, trigger_name)?;
        let old_class = catcache
            .class_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let has_remaining = catcache
            .trigger_rows_for_relation(relation_oid)
            .into_iter()
            .any(|trigger| trigger.oid != old_trigger.oid);

        let mut kinds = vec![
            BootstrapCatalogKind::PgTrigger,
            BootstrapCatalogKind::PgDepend,
        ];
        let mut delete_rows = PhysicalCatalogRows {
            triggers: vec![old_trigger.clone()],
            depends: trigger_depend_rows(old_trigger.oid, old_trigger.tgrelid, old_trigger.tgfoid),
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

    pub fn create_policy_mvcc(
        &mut self,
        mut row: crate::include::catalog::PgPolicyRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        if catcache
            .policy_rows_for_relation(row.polrelid)
            .iter()
            .any(|existing| existing.polname.eq_ignore_ascii_case(&row.polname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_policy_polrelid_polname_index".into(),
            ));
        }
        if catcache.class_by_oid(row.polrelid).is_none() {
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
        old_row: &crate::include::catalog::PgPolicyRow,
        mut row: crate::include::catalog::PgPolicyRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_visible = policy_row_visible(&catcache, old_row.polrelid, &old_row.polname)?;
        if catcache
            .policy_rows_for_relation(row.polrelid)
            .iter()
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
    ) -> Result<(crate::include::catalog::PgPolicyRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_policy = policy_row_visible(&catcache, relation_oid, policy_name)?;
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

    pub fn create_relation_inheritance_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        if parent_oids.is_empty() {
            return Ok(CatalogMutationEffect::default());
        }

        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let child_relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let child_entry = catalog_entry_from_visible_relation(&catcache, child_relation)?;
        let old_child_rows = rows_for_existing_relation(&catcache, &child_entry)?;

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
            let old_parent = catcache
                .class_by_oid(parent_oid)
                .cloned()
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

    pub fn drop_relation_inheritance_parent_mvcc(
        &mut self,
        relation_oid: u32,
        parent_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let child_relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if child_relation.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let parent_relation = relcache
            .get_by_oid(parent_oid)
            .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?;
        if parent_relation.relkind != 'r' {
            return Err(CatalogError::UnknownTable(parent_oid.to_string()));
        }

        let mut current_inherits = catcache
            .inherit_rows()
            .into_iter()
            .filter(|row| row.inhrelid == relation_oid)
            .collect::<Vec<_>>();
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
                relcache
                    .get_by_oid(row.inhparent)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownTable(row.inhparent.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let remaining_parent_relations = remaining_inherits
            .iter()
            .map(|row| {
                relcache
                    .get_by_oid(row.inhparent)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownTable(row.inhparent.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let child_name = catcache
            .class_by_oid(relation_oid)
            .map(|row| row.relname.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_child_entry = catalog_entry_from_visible_relation(&catcache, child_relation)?;
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

        let mut preserved_constraints = catcache
            .constraint_rows_for_relation(relation_oid)
            .into_iter()
            .filter(|row| row.contype != CONSTRAINT_NOTNULL)
            .map(|mut row| {
                if row.contype == CONSTRAINT_CHECK {
                    let current_parent_match_count = inherited_parent_check_match_count(
                        &catcache,
                        &current_parent_relations,
                        &row,
                    );
                    let remaining_parent_match_count = inherited_parent_check_match_count(
                        &catcache,
                        &remaining_parent_relations,
                        &row,
                    );
                    let had_local_definition =
                        row.conislocal && row.coninhcount == current_parent_match_count as i16;
                    row.coninhcount = remaining_parent_match_count as i16;
                    row.conislocal = had_local_definition || remaining_parent_match_count == 0;
                    if !had_local_definition {
                        row.connoinherit = false;
                    }
                }
                row
            })
            .collect::<Vec<_>>();
        sort_pg_constraint_rows(&mut preserved_constraints);

        let mut new_constraints = derived_pg_constraint_rows(
            relation_oid,
            relation_object_name(&child_name),
            child_relation.namespace_oid,
            &new_child_entry.desc,
        );
        new_constraints.extend(preserved_constraints);
        sort_pg_constraint_rows(&mut new_constraints);

        let new_attributes =
            rows_for_new_relation_entry(&catcache, &child_name, &new_child_entry)?.attributes;
        let old_attributes = catcache
            .attributes_by_relid(relation_oid)
            .unwrap_or(&[])
            .to_vec();
        let old_constraints = catcache.constraint_rows_for_relation(relation_oid);
        let removed_depends = catcache
            .depend_rows()
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

        if let Some(old_parent) = catcache.class_by_oid(parent_oid).cloned() {
            let has_remaining_children = catcache
                .inherit_rows()
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
        if !namespace_name.starts_with("pg_temp_") && !namespace_name.starts_with("pg_toast_temp_")
        {
            self.invalidate_relcache_init_for_kinds(&[BootstrapCatalogKind::PgNamespace]);
        }
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
            }],
            ..PhysicalCatalogRows::default()
        };
        delete_catalog_rows_subset_mvcc(
            ctx,
            &rows,
            self.scope_db_oid(),
            &[BootstrapCatalogKind::PgNamespace],
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgNamespace]);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn create_view_relation_mvcc(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        if catcache
            .class_rows()
            .iter()
            .any(|row| row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(&name))
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            &catcache,
            name.clone(),
            desc,
            namespace_oid,
            self.scope_db_oid(),
            'p',
            'v',
            owner_oid,
            &mut control,
        )?;
        let kinds = create_view_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&catcache, &name, &entry)?;
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
        let relcache = visible_catalog_caches_for_ctx(self, ctx)?.1;
        relcache
            .get_by_oid(relation_oid)
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        if catcache
            .class_rows()
            .iter()
            .any(|row| row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(&name))
        {
            return Err(CatalogError::TableAlreadyExists(
                normalize_catalog_name(&name).to_ascii_lowercase(),
            ));
        }
        let mut control = self.control_state()?;
        let entry = build_relation_entry(
            &catcache,
            name.clone(),
            desc,
            namespace_oid,
            self.scope_db_oid(),
            'p',
            'c',
            owner_oid,
            &mut control,
        )?;

        let kinds = create_composite_type_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&catcache, &name, &entry)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok((entry, effect))
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
        };
        self.create_index_for_relation_mvcc_with_options(
            index_name,
            relation_oid,
            unique,
            primary,
            columns,
            &options,
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
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let index_name = index_name.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
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
            &mut control,
        )?;
        let kinds = create_index_sync_kinds();
        self.persist_control_values(control.next_oid, control.next_rel_number)?;
        let rows = rows_for_new_relation_entry(&catcache, &index_name, &entry)?;
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.created_rels, entry.rel);
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
        let conname = conname.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let table = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let index = relcache
            .get_by_oid(index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(index_oid.to_string()))?;
        if index.relkind != 'i' {
            return Err(CatalogError::UnknownTable(index_oid.to_string()));
        }
        if catcache.constraint_rows().into_iter().any(|row| {
            row.conrelid == relation_oid
                && row.contype == contype
                && row.conname.eq_ignore_ascii_case(&conname)
        }) {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let mut control = self.control_state()?;
        let constraint = PgConstraintRow {
            oid: control.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype,
            condeferrable: false,
            condeferred: false,
            conenforced: true,
            convalidated: true,
            conrelid: relation_oid,
            contypid: 0,
            conindid: index_oid,
            conparentid: 0,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: index.index.as_ref().map(|meta| meta.indkey.clone()),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        };
        control.next_oid = control.next_oid.saturating_add(1);

        let mut depends =
            index_backed_constraint_depend_rows(constraint.oid, relation_oid, index_oid);
        if contype == crate::include::catalog::CONSTRAINT_PRIMARY {
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
        Ok(effect)
    }

    pub fn create_check_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        convalidated: bool,
        connoinherit: bool,
        conbin: impl Into<String>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let conname = conname.into();
        let conbin = conbin.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let table = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if relation_constraint_exists_visible(&catcache, relation_oid, &conname, None) {
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
            conenforced: true,
            convalidated,
            conrelid: relation_oid,
            contypid: 0,
            conindid: 0,
            conparentid: 0,
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
            conislocal: true,
            coninhcount: 0,
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
        Ok(effect)
    }

    pub fn create_foreign_key_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
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
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let conname = conname.into();
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let table = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let referenced_table = relcache
            .get_by_oid(referenced_relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(referenced_relation_oid.to_string()))?;
        if referenced_table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(
                referenced_relation_oid.to_string(),
            ));
        }
        let referenced_index = relcache
            .get_by_oid(referenced_index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(referenced_index_oid.to_string()))?;
        if referenced_index.relkind != 'i' {
            return Err(CatalogError::UnknownTable(referenced_index_oid.to_string()));
        }
        if relation_constraint_exists_visible(&catcache, relation_oid, &conname, None) {
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
            condeferrable: false,
            condeferred: false,
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
            conperiod: false,
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
        Ok(effect)
    }

    pub fn drop_relation_entry_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        let old_rows = rows_for_existing_relation(&catcache, &entry)?;
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
                if entry.relkind != 'r' {
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
                if entry.relkind != 'r' {
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
                if entry.relkind != 'r' {
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_row = relation_constraint_row_visible(
            &catcache,
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_row = relation_constraint_row_visible(
            &catcache,
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let old_row = relation_constraint_row_visible(
            &catcache,
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        if relation_constraint_exists_visible(&catcache, relation_oid, &new_constraint_name, None) {
            return Err(CatalogError::TableAlreadyExists(new_constraint_name));
        }
        let old_constraint =
            relation_constraint_row_visible(&catcache, relation_oid, constraint_name, None)?;

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
                self.rename_relation_mvcc(new_constraint.conindid, &new_constraint_name, ctx)?;
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
        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let removed =
            relation_constraint_row_visible(&catcache, relation_oid, constraint_name, None)?;
        let removed_depends = constraint_depend_rows_visible(&catcache, removed.oid);
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
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        if catcache
            .inherit_rows()
            .iter()
            .any(|row| row.inhparent == relation_oid)
        {
            return Err(CatalogError::Corrupt(
                "DROP TABLE with inherited children requires CASCADE, which is not supported yet",
            ));
        }
        let (rows_to_delete, parent_rows_to_insert, dropped, affected_parent_oids) =
            drop_relation_entries_visible(&catcache, &relcache, relation_oid)?;
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
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if relation.relkind != 'v' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        let rows = rows_for_existing_relation(&catcache, &entry)?;
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
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if relation.relkind != 'c' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        let rows = rows_for_existing_relation(&catcache, &entry)?;
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

    pub fn set_index_ready_valid_mvcc(
        &mut self,
        relation_oid: u32,
        indisready: bool,
        indisvalid: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        if old_entry.relkind != 'i' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
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
                indexes: vec![index_row_for_entry(&old_entry).ok_or(CatalogError::Corrupt(
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
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if let Some(index_meta) = &new_entry.index_meta {
            effect_record_oid(&mut effect.relation_oids, index_meta.indrelid);
        }
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
                if entry.relkind != 'r' {
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

    pub fn alter_table_drop_column_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'r' {
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
                if entry.relkind != 'r' {
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

    pub fn alter_table_set_column_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (_old_entry, new_entry, _, kinds) =
            mutate_visible_relation_entry_mvcc(self, relation_oid, ctx, |entry, _control| {
                if entry.relkind != 'r' {
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
                if entry.relkind != 'r' {
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

    pub fn rename_relation_mvcc(
        &mut self,
        relation_oid: u32,
        new_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(relation.relkind, 'r' | 'S' | 'i') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if catcache.class_rows().iter().any(|row| {
            row.oid != relation_oid
                && row.relnamespace == relation.namespace_oid
                && row.relname.eq_ignore_ascii_case(new_name)
        }) {
            return Err(CatalogError::TableAlreadyExists(
                new_name.to_ascii_lowercase(),
            ));
        }
        let entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        let old_rows = rows_for_existing_relation(&catcache, &entry)?;
        let new_rows = rows_for_new_relation_entry(&catcache, new_name, &entry)?;
        let control = self.control_state()?;
        self.persist_control_values(control.next_oid, control.next_rel_number)?;

        let kinds = vec![
            BootstrapCatalogKind::PgClass,
            BootstrapCatalogKind::PgType,
            BootstrapCatalogKind::PgConstraint,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, self.scope_db_oid(), &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, self.scope_db_oid(), &kinds)?;
        self.control = control;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
        Ok(effect)
    }

    pub fn rewrite_relation_storage_mvcc(
        &mut self,
        relation_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let mut control = self.control_state()?;
        let kinds = vec![BootstrapCatalogKind::PgClass];
        let mut old_rows = PhysicalCatalogRows::default();
        let mut new_rows = PhysicalCatalogRows::default();
        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);

        for &relation_oid in relation_oids {
            let relation = relcache
                .get_by_oid(relation_oid)
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            if !matches!(relation.relkind, 'r' | 't' | 'i') {
                return Err(CatalogError::UnknownTable(relation_oid.to_string()));
            }
            let old_entry = catalog_entry_from_visible_relation(&catcache, relation)?;
            let relation_name = catcache
                .class_by_oid(relation_oid)
                .map(|row| row.relname.clone())
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
        let (catcache, relcache) = visible_catalog_caches_for_ctx(self, ctx)?;
        let relation = relcache
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = catalog_entry_from_visible_relation(&catcache, relation)?;
        let relation_name = catcache
            .class_by_oid(relation_oid)
            .map(|row| row.relname.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let mut new_entry = old_entry.clone();
        new_entry.owner_oid = new_owner_oid;

        let mut kinds = vec![BootstrapCatalogKind::PgClass];
        if old_entry.row_type_oid != 0 || new_entry.row_type_oid != 0 {
            kinds.push(BootstrapCatalogKind::PgType);
        }
        let old_rows = rows_for_existing_relation(&catcache, &old_entry)?;
        let new_rows = rows_for_new_relation_entry(&catcache, &relation_name, &new_entry)?;
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

    pub fn replace_relation_statistics_mvcc(
        &mut self,
        relation_oid: u32,
        statistics: Vec<PgStatisticRow>,
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
            PG_STATISTIC_RELID_ATT_INH_INDEX_OID,
            vec![crate::include::access::scankey::ScanKeyData {
                attribute_number: 1,
                strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int64(i64::from(relation_oid)),
            }],
        )?
        .into_iter()
        .map(pg_statistic_row_from_values)
        .collect::<Result<Vec<_>, _>>()?;

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

    pub fn comment_relation_mvcc(
        &mut self,
        relation_oid: u32,
        comment: Option<&str>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        self.comment_shared_object_mvcc(relation_oid, PG_CLASS_RELATION_OID, comment, ctx)
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

    pub fn drop_rule_mvcc(
        &mut self,
        rewrite_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let description_rows = probe_system_catalog_rows_visible_in_db(
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
                    argument: Value::Int64(i64::from(rewrite_oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(PG_REWRITE_RELATION_OID)),
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

        let catcache = visible_catalog_caches_for_ctx(self, ctx)?.0;
        let removed_rewrite = rewrite_row_visible(&catcache, rewrite_oid)?;
        let removed_depends = catcache
            .depend_rows()
            .into_iter()
            .filter(|row| row.objid == rewrite_oid)
            .collect::<Vec<_>>();

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
            PG_DESCRIPTION_O_C_O_INDEX_OID,
            vec![
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 1,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(object_oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(classoid)),
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
                            objsubid: 0,
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
                        objsubid: 0,
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

fn build_relation_entry(
    _catcache: &CatCache,
    _name: String,
    mut desc: RelationDesc,
    namespace_oid: u32,
    db_oid: u32,
    relpersistence: char,
    relkind: char,
    owner_oid: u32,
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
    if relkind == 'r' {
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
        row_type_oid,
        array_type_oid,
        reltoastrelid: 0,
        relpersistence,
        relkind,
        am_oid: crate::include::catalog::relam_for_relkind(relkind),
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages,
        reltuples,
        desc,
        index_meta: None,
    };
    if relkind_has_storage(relkind) {
        control.next_rel_number = control.next_rel_number.saturating_add(1);
    }
    control.next_oid = next_oid;
    Ok(entry)
}

fn build_index_entry(
    catcache: &CatCache,
    index_name: String,
    table: &CatalogEntry,
    unique: bool,
    primary: bool,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    options: &CatalogIndexBuildOptions,
    control: &mut CatalogControl,
) -> Result<CatalogEntry, CatalogError> {
    let _ = index_name;
    if table.relkind != 'r' && table.relkind != 't' {
        return Err(CatalogError::UnknownTable(table.relation_oid.to_string()));
    }
    let resolved_options = if options.indclass.is_empty()
        && options.indcollation.is_empty()
        && options.indoption.is_empty()
    {
        default_index_build_options_for_relation(catcache, table, columns)?
    } else {
        options.clone()
    };

    let mut indkey = Vec::with_capacity(columns.len());
    let mut index_columns = Vec::with_capacity(columns.len());
    let mut expr_sqls = Vec::new();
    for (position, column_name) in columns.iter().enumerate() {
        if let Some(expr_sql) = column_name.expr_sql.as_deref() {
            indkey.push(0);
            expr_sqls.push(expr_sql.to_string());
            let expr_type = column_name
                .expr_type
                .ok_or(CatalogError::Corrupt("missing expression index sql type"))?;
            index_columns.push(crate::backend::catalog::catalog::column_desc(
                format!("expr{}", position + 1),
                expr_type,
                true,
            ));
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
        index_columns.push(column);
    }

    if resolved_options.indclass.len() != columns.len()
        || resolved_options.indcollation.len() != columns.len()
        || resolved_options.indoption.len() != columns.len()
    {
        return Err(CatalogError::Corrupt("index build options length mismatch"));
    }

    let entry = CatalogEntry {
        rel: crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid: table.rel.db_oid,
            rel_number: control.next_rel_number,
        },
        relation_oid: control.next_oid,
        namespace_oid: table.namespace_oid,
        owner_oid: table.owner_oid,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: table.relpersistence,
        relkind: 'i',
        am_oid: resolved_options.am_oid,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: -1.0,
        desc: RelationDesc {
            columns: index_columns,
        },
        index_meta: Some(CatalogIndexMeta {
            indrelid: table.relation_oid,
            indkey,
            indisunique: unique,
            indisprimary: primary,
            indisvalid: false,
            indisready: false,
            indislive: true,
            indclass: resolved_options.indclass,
            indcollation: resolved_options.indcollation,
            indoption: resolved_options.indoption,
            indexprs: (!expr_sqls.is_empty())
                .then(|| serde_json::to_string(&expr_sqls))
                .transpose()
                .map_err(|_| CatalogError::Corrupt("invalid index expression metadata"))?,
            indpred: None,
        }),
    };
    control.next_rel_number = control.next_rel_number.saturating_add(1);
    control.next_oid = control.next_oid.saturating_add(1);
    Ok(entry)
}

fn build_toast_catalog_changes(
    catcache: &CatCache,
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
        catcache,
        toast_name.clone(),
        toast_relation_desc(),
        toast_namespace_oid,
        parent.rel.db_oid,
        parent.relpersistence,
        't',
        parent.owner_oid,
        control,
    )?;
    new_parent.reltoastrelid = toast_entry.relation_oid;

    let index_name = format!(
        "{toast_namespace_name}.{}",
        toast_index_name(parent.relation_oid)
    );
    let mut index_entry = build_index_entry(
        catcache,
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
        },
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

fn default_index_build_options_for_relation(
    catcache: &CatCache,
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
        let type_oid = resolved_sql_type_oid(catcache, table, column.sql_type);
        let opclass_oid = crate::include::catalog::default_btree_opclass_oid(type_oid)
            .ok_or_else(|| CatalogError::UnknownType("index column type".into()))?;
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
    let (catcache, relcache) = visible_catalog_caches_for_ctx(store, ctx)?;
    let relation = relcache
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    let relation_name = catcache
        .class_by_oid(relation_oid)
        .map(|row| row.relname.clone())
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    let old_entry = catalog_entry_from_visible_relation(&catcache, relation)?;
    let mut new_entry = old_entry.clone();
    let mut control = store.control_state()?;
    let (extra, kinds) = mutator(&mut new_entry, &mut control)?;
    store.persist_control_values(control.next_oid, control.next_rel_number)?;
    let old_rows = rows_for_existing_relation(&catcache, &old_entry)?;
    let mut new_rows = rows_for_new_relation_entry(&catcache, &relation_name, &new_entry)?;
    preserve_non_derived_relation_rows(&catcache, &old_entry, &kinds, &mut new_rows);
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

fn relation_constraint_exists_visible(
    catcache: &CatCache,
    relation_oid: u32,
    constraint_name: &str,
    contype: Option<char>,
) -> bool {
    catcache
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| {
            contype.is_none_or(|expected| row.contype == expected)
                && row.conname.eq_ignore_ascii_case(constraint_name)
        })
}

fn relation_constraint_row_visible(
    catcache: &CatCache,
    relation_oid: u32,
    constraint_name: &str,
    contype: Option<char>,
) -> Result<PgConstraintRow, CatalogError> {
    catcache
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| {
            contype.is_none_or(|expected| row.contype == expected)
                && row.conname.eq_ignore_ascii_case(constraint_name)
        })
        .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))
}

fn constraint_depend_rows_visible(catcache: &CatCache, constraint_oid: u32) -> Vec<PgDependRow> {
    let mut rows = catcache
        .depend_rows()
        .into_iter()
        .filter(|row| row.objid == constraint_oid || row.refobjid == constraint_oid)
        .collect::<Vec<_>>();
    sort_pg_depend_rows(&mut rows);
    rows
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

fn trigger_row_visible(
    catcache: &CatCache,
    relation_oid: u32,
    trigger_name: &str,
) -> Result<crate::include::catalog::PgTriggerRow, CatalogError> {
    catcache
        .trigger_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.tgname.eq_ignore_ascii_case(trigger_name))
        .ok_or_else(|| CatalogError::UnknownTable(trigger_name.to_string()))
}

fn policy_row_visible(
    catcache: &CatCache,
    relation_oid: u32,
    policy_name: &str,
) -> Result<crate::include::catalog::PgPolicyRow, CatalogError> {
    catcache
        .policy_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.polname.eq_ignore_ascii_case(policy_name))
        .ok_or_else(|| CatalogError::UnknownTable(policy_name.to_string()))
}
fn rewrite_row_visible(
    catcache: &CatCache,
    rewrite_oid: u32,
) -> Result<PgRewriteRow, CatalogError> {
    catcache
        .rewrite_rows()
        .into_iter()
        .find(|row| row.oid == rewrite_oid)
        .ok_or_else(|| CatalogError::UnknownTable(rewrite_oid.to_string()))
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
    if entry.relkind != 'r' {
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

fn dropped_column_name_visible(attnum: usize) -> String {
    format!("........pg.dropped.{attnum}........")
}

fn rows_for_new_relation_entry(
    catcache: &CatCache,
    relation_name: &str,
    entry: &CatalogEntry,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut rows = PhysicalCatalogRows::default();
    rows.classes
        .push(class_row_for_relation_name(relation_name, entry));
    rows.types
        .extend(type_rows_for_relation_name(relation_name, entry));

    rows.attributes
        .extend(
            entry
                .desc
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid: resolved_sql_type_oid(catcache, entry, column.sql_type),
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
                    sql_type: column.sql_type,
                }),
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
    if let Some(index_row) = index_row_for_entry(entry) {
        rows.indexes.push(index_row);
    }
    sort_pg_depend_rows(&mut rows.depends);
    Ok(rows)
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
        reltablespace: 0,
        relfilenode: entry.rel.rel_number,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relhassubclass: entry.relhassubclass,
        relhastriggers: entry.relhastriggers,
        relispartition: entry.relispartition,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        relnatts: entry.desc.columns.len() as i16,
        relpages: entry.relpages,
        reltuples: entry.reltuples,
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
    if entry.relkind == 'r' {
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
        indnkeyatts: index_meta.indkey.len() as i16,
        indisunique: index_meta.indisunique,
        indnullsnotdistinct: false,
        indisprimary: index_meta.indisprimary,
        indisexclusion: false,
        indimmediate: true,
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
    let constraints = if entry.relkind == 'r' {
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
    if entry.relkind == 'i' {
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
        row_type_oid: relation.row_type_oid,
        array_type_oid: relation.array_type_oid,
        reltoastrelid: relation.reltoastrelid,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: class_row.relam,
        relhassubclass: class_row.relhassubclass,
        relhastriggers: relation.relhastriggers,
        relispartition: class_row.relispartition,
        relrowsecurity: class_row.relrowsecurity,
        relforcerowsecurity: class_row.relforcerowsecurity,
        relpages: class_row.relpages,
        reltuples: class_row.reltuples,
        desc: relation.desc.clone(),
        index_meta: relation.index.as_ref().map(|index| CatalogIndexMeta {
            indrelid: index.indrelid,
            indkey: index.indkey.clone(),
            indisunique: index.indisunique,
            indisprimary: index.indisprimary,
            indisvalid: index.indisvalid,
            indisready: index.indisready,
            indislive: index.indislive,
            indclass: index.indclass.clone(),
            indcollation: index.indcollation.clone(),
            indoption: index.indoption.clone(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
        }),
    })
}

fn resolved_sql_type_oid(
    catcache: &CatCache,
    entry: &CatalogEntry,
    sql_type: crate::backend::parser::SqlType,
) -> u32 {
    if sql_type.is_array
        && matches!(
            sql_type.kind,
            crate::backend::parser::SqlTypeKind::Composite
                | crate::backend::parser::SqlTypeKind::Record
        )
        && sql_type.type_oid != 0
    {
        if sql_type.type_oid == entry.row_type_oid && entry.array_type_oid != 0 {
            return entry.array_type_oid;
        }
        if let Some(row) = catcache.type_by_oid(sql_type.type_oid)
            && row.typarray != 0
        {
            return row.typarray;
        }
    }
    sql_type_oid(sql_type)
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

fn drop_relation_oids_by_oid_visible(
    relcache: &RelCache,
    depend_rows: &[PgDependRow],
    relation_oid: u32,
) -> Result<Vec<u32>, CatalogError> {
    let entry = relcache
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if !matches!(entry.relkind, 'r' | 'S') {
        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
    }
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    collect_relation_drop_oids_visible(relcache, depend_rows, relation_oid, &mut seen, &mut order);
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
            if !matches!(dependent.relkind, 'r' | 'i' | 't' | 'S') {
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

fn drop_relation_oids_by_oid(
    catalog: &Catalog,
    relation_oid: u32,
) -> Result<Vec<u32>, CatalogError> {
    let entry = catalog
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if !matches!(entry.relkind, 'r' | 'S') {
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
            if !matches!(dependent.relkind, 'r' | 'i' | 't' | 'S') {
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
