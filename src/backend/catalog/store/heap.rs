use std::collections::BTreeSet;

use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions,
};
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible;
use crate::backend::catalog::persistence::{
    append_catalog_entry_rows, delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
};
use crate::backend::catalog::pg_depend::view_rewrite_depend_rows;
use crate::backend::catalog::rowcodec::{
    pg_description_row_from_values, pg_statistic_row_from_values,
};
use crate::backend::catalog::rows::{
    PhysicalCatalogRows, create_index_sync_kinds, create_table_sync_kinds, create_view_sync_kinds,
    drop_relation_delete_kinds, drop_relation_sync_kinds, extend_physical_catalog_rows,
    physical_catalog_rows_for_catalog_entry,
};
use crate::backend::catalog::toasting::{ToastCatalogChanges, new_relation_create_toast_table};
use crate::backend::executor::{ColumnDesc, RelationDesc};
use crate::include::catalog::{
    BootstrapCatalogKind, PG_CLASS_RELATION_OID, PgConstraintRow, PgDependRow, PgDescriptionRow,
    PgNamespaceRow, PgRewriteRow, PgStatisticRow,
};
use crate::include::nodes::datum::Value;

use super::{CatalogMutationEffect, CatalogStore, CatalogWriteContext, CreateTableResult};

const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
const PG_STATISTIC_RELID_ATT_INH_INDEX_OID: u32 = 2696;

impl CatalogStore {
    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into();
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.create_table(name.clone(), desc)?;
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
        let kinds = create_table_sync_kinds(&entry);
        self.persist_control_state(&catalog)?;
        if let crate::backend::catalog::store::CatalogStoreMode::Durable { base_dir, .. } =
            &self.mode
        {
            append_catalog_entry_rows(base_dir, &catalog, &name, &entry, &kinds)?;
            if let Some(toast) = toast {
                append_catalog_entry_rows(
                    base_dir,
                    &catalog,
                    &toast.toast_name,
                    &toast.toast_entry,
                    &create_table_sync_kinds(&toast.toast_entry),
                )?;
                append_catalog_entry_rows(
                    base_dir,
                    &catalog,
                    &toast.index_name,
                    &toast.index_entry,
                    &create_index_sync_kinds(),
                )?;
            }
        }
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
        self.persist_control_state(&catalog)?;
        if let crate::backend::catalog::store::CatalogStoreMode::Durable { base_dir, .. } =
            &self.mode
        {
            append_catalog_entry_rows(base_dir, &catalog, &index_name, &entry, &kinds)?;
        }
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
        self.persist_control_state(&catalog)?;
        if let crate::backend::catalog::store::CatalogStoreMode::Durable { base_dir, .. } =
            &self.mode
        {
            append_catalog_entry_rows(base_dir, &catalog, &index_name, &entry, &kinds)?;
        }
        Ok(entry)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<Vec<CatalogEntry>, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog
            .get(name)
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        if entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(name.to_string()));
        }
        let oids = drop_relation_oids_by_oid(&catalog, entry.relation_oid)?;
        let mut dropped = Vec::with_capacity(oids.len());
        for oid in oids {
            if let Some((_name, entry)) = catalog.remove_by_oid(oid) {
                dropped.push(entry);
            }
        }
        self.persist_catalog_kinds(&catalog, &drop_relation_sync_kinds())?;
        Ok(dropped)
    }

    pub fn drop_relation_by_oid(
        &mut self,
        relation_oid: u32,
    ) -> Result<Vec<CatalogEntry>, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let oids = drop_relation_oids_by_oid(&catalog, relation_oid)?;
        let mut dropped = Vec::with_capacity(oids.len());
        for oid in oids {
            if let Some((_name, entry)) = catalog.remove_by_oid(oid) {
                dropped.push(entry);
            }
        }
        self.persist_catalog_kinds(&catalog, &drop_relation_sync_kinds())?;
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
            1,
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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let entry = catalog.create_table_with_options(
            name.clone(),
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            owner_oid,
        )?;
        let toast = new_relation_create_toast_table(
            &mut catalog,
            entry.relation_oid,
            toast_namespace_name,
            toast_namespace_oid,
        )?;
        let entry = toast
            .as_ref()
            .map(|changes| changes.new_parent.clone())
            .unwrap_or(entry);
        let mut kinds = create_table_sync_kinds(&entry);
        self.persist_control_state(&catalog)?;
        let mut rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry);
        if let Some(toast) = &toast {
            add_catalog_entry_rows(&mut rows, &catalog, &toast.toast_name, &toast.toast_entry);
            add_catalog_entry_rows(&mut rows, &catalog, &toast.index_name, &toast.index_entry);
            merge_catalog_kinds(&mut kinds, &create_table_sync_kinds(&toast.toast_entry));
            merge_catalog_kinds(&mut kinds, &create_index_sync_kinds());
        }
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

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
        let namespace_oid = if namespace_oid == 0 {
            let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
            let oid = catalog.next_oid();
            self.persist_control_state(&catalog)?;
            oid
        } else {
            namespace_oid
        };
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
            }],
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &[BootstrapCatalogKind::PgNamespace])?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgNamespace]);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn create_proc_mvcc(
        &mut self,
        mut row: crate::include::catalog::PgProcRow,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        if row.oid == 0 {
            row.oid = catalog.next_oid();
        }
        catalog.next_oid = catalog.next_oid.max(row.oid.saturating_add(1));
        self.persist_control_state(&catalog)?;

        let rows = PhysicalCatalogRows {
            procs: vec![row.clone()],
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &[BootstrapCatalogKind::PgProc])?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgProc]);
        Ok((row.oid, effect))
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

        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let child_name = catalog
            .relation_name_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?
            .to_string();
        let child_entry = catalog
            .get_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let parent_entries = parent_oids
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

        let old_child_rows =
            physical_catalog_rows_for_catalog_entry(&catalog, &child_name, &child_entry);
        let mut old_parent_rows = PhysicalCatalogRows::default();
        for (parent_name, parent_entry) in &parent_entries {
            add_catalog_entry_rows(&mut old_parent_rows, &catalog, parent_name, parent_entry);
        }

        catalog.attach_inheritance(relation_oid, parent_oids)?;
        self.persist_control_state(&catalog)?;

        let child_entry = catalog
            .get_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let new_child_rows =
            physical_catalog_rows_for_catalog_entry(&catalog, &child_name, &child_entry);
        let mut new_parent_rows = PhysicalCatalogRows::default();
        for (parent_name, _) in &parent_entries {
            let parent_entry = catalog
                .get(parent_name)
                .cloned()
                .ok_or_else(|| CatalogError::UnknownTable(parent_name.clone()))?;
            add_catalog_entry_rows(&mut new_parent_rows, &catalog, parent_name, &parent_entry);
        }

        let parent_kinds = vec![BootstrapCatalogKind::PgClass];
        let child_kinds = vec![
            BootstrapCatalogKind::PgDepend,
            BootstrapCatalogKind::PgInherits,
        ];
        delete_catalog_rows_subset_mvcc(ctx, &old_parent_rows, 1, &parent_kinds)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_child_rows, 1, &child_kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_parent_rows, 1, &parent_kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_child_rows, 1, &child_kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &parent_kinds);
        effect_record_catalog_kinds(&mut effect, &child_kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        for parent_oid in parent_oids {
            effect_record_oid(&mut effect.relation_oids, *parent_oid);
        }
        Ok(effect)
    }

    pub fn drop_namespace_mvcc(
        &mut self,
        namespace_oid: u32,
        namespace_name: &str,
        owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let rows = PhysicalCatalogRows {
            namespaces: vec![PgNamespaceRow {
                oid: namespace_oid,
                nspname: namespace_name.to_string(),
                nspowner: owner_oid,
            }],
            ..PhysicalCatalogRows::default()
        };
        delete_catalog_rows_subset_mvcc(ctx, &rows, 1, &[BootstrapCatalogKind::PgNamespace])?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgNamespace]);
        effect_record_oid(&mut effect.namespace_oids, namespace_oid);
        Ok(effect)
    }

    pub fn create_view_mvcc(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        owner_oid: u32,
        definition: String,
        referenced_relation_oids: &[u32],
        ctx: &CatalogWriteContext,
    ) -> Result<(CatalogEntry, CatalogMutationEffect), CatalogError> {
        let name = name.into();
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let entry = catalog.create_table_with_relkind(
            name.clone(),
            desc,
            namespace_oid,
            1,
            'p',
            'v',
            owner_oid,
        )?;
        let rewrite_row = PgRewriteRow {
            oid: catalog.next_oid(),
            rulename: "_RETURN".to_string(),
            ev_class: entry.relation_oid,
            ev_type: '1',
            ev_enabled: 'O',
            is_instead: true,
            ev_qual: String::new(),
            ev_action: definition,
        };
        catalog.add_rewrite_row(rewrite_row.clone());
        let mut referenced = referenced_relation_oids.to_vec();
        referenced.sort_unstable();
        referenced.dedup();
        for row in view_rewrite_depend_rows(rewrite_row.oid, entry.relation_oid, &referenced) {
            catalog.add_depend_row(row);
        }

        let kinds = create_view_sync_kinds();
        self.persist_control_state(&catalog)?;
        let rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry);
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
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
        self.persist_control_state(&catalog)?;
        let rows = physical_catalog_rows_for_catalog_entry(&catalog, &index_name, &entry);
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let constraint = catalog.create_index_backed_constraint(
            relation_oid,
            index_oid,
            conname.into(),
            contype,
            primary_key_owned_not_null_oids,
        )?;
        self.persist_control_state(&catalog)?;

        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: catalog
                .depend_rows()
                .iter()
                .filter(|row| row.objid == constraint.oid || row.refobjid == constraint.oid)
                .cloned()
                .collect(),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

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
        conbin: impl Into<String>,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let constraint = catalog.create_check_constraint(
            relation_oid,
            conname.into(),
            convalidated,
            conbin.into(),
        )?;
        self.persist_control_state(&catalog)?;

        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: catalog
                .depend_rows()
                .iter()
                .filter(|row| row.objid == constraint.oid)
                .cloned()
                .collect(),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn create_foreign_key_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        convalidated: bool,
        local_attnums: &[i16],
        referenced_relation_oid: u32,
        referenced_index_oid: u32,
        referenced_attnums: &[i16],
        confupdtype: char,
        confdeltype: char,
        confmatchtype: char,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let constraint = catalog.create_foreign_key_constraint(
            relation_oid,
            conname.into(),
            convalidated,
            local_attnums,
            referenced_relation_oid,
            referenced_index_oid,
            referenced_attnums,
            confupdtype,
            confdeltype,
            confmatchtype,
        )?;
        self.persist_control_state(&catalog)?;

        let rows = PhysicalCatalogRows {
            constraints: vec![constraint.clone()],
            depends: catalog
                .depend_rows()
                .iter()
                .filter(|row| row.objid == constraint.oid || row.refobjid == constraint.oid)
                .cloned()
                .collect(),
            ..PhysicalCatalogRows::default()
        };
        let kinds = vec![
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        insert_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let old_catalog = catalog.clone();
        let (name, old_entry) = old_catalog
            .entries()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, entry)| (name.to_string(), entry.clone()))
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_rows = physical_catalog_rows_for_catalog_entry(&old_catalog, &name, &old_entry);
        let kinds = drop_relation_delete_kinds();
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        let (_removed_name, removed_entry) = catalog.drop_relation_entry_by_oid(relation_oid)?;
        self.persist_control_state(&catalog)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_rel(&mut effect.dropped_rels, removed_entry.rel);
        effect_record_oid(&mut effect.relation_oids, removed_entry.relation_oid);
        effect_record_oid(&mut effect.namespace_oids, removed_entry.namespace_oid);
        if removed_entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, removed_entry.row_type_oid);
        }
        Ok((removed_entry, effect))
    }

    pub fn set_column_not_null_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        constraint_name: impl Into<String>,
        validated: bool,
        primary_key_owned: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let old_catalog = catalog.clone();
        let (constraint_oid, name, old_entry, new_entry) = catalog.set_column_not_null(
            relation_oid,
            column_name,
            constraint_name.into(),
            validated,
            primary_key_owned,
        )?;

        let kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        let old_rows = physical_catalog_rows_for_catalog_entry(&old_catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        self.persist_control_state(&catalog)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok((constraint_oid, effect))
    }

    pub fn drop_column_not_null_mvcc(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let old_catalog = catalog.clone();
        let (name, old_entry, new_entry) =
            catalog.drop_column_not_null(relation_oid, column_name)?;

        let kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        let old_rows = physical_catalog_rows_for_catalog_entry(&old_catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        self.persist_control_state(&catalog)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let old_catalog = catalog.clone();
        let (name, old_entry, new_entry) =
            catalog.validate_not_null_constraint(relation_oid, constraint_name)?;

        let kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        let old_rows = physical_catalog_rows_for_catalog_entry(&old_catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        self.persist_control_state(&catalog)?;
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (old_row, new_row) =
            catalog.validate_check_constraint(relation_oid, constraint_name)?;
        self.persist_control_state(&catalog)?;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            1,
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row],
                ..PhysicalCatalogRows::default()
            },
            1,
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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (old_row, new_row) =
            catalog.validate_foreign_key_constraint(relation_oid, constraint_name)?;
        self.persist_control_state(&catalog)?;

        let kinds = vec![BootstrapCatalogKind::PgConstraint];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            1,
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                constraints: vec![new_row],
                ..PhysicalCatalogRows::default()
            },
            1,
            &kinds,
        )?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }

    pub fn drop_relation_constraint_mvcc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgConstraintRow, CatalogMutationEffect), CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let old_catalog = catalog.clone();
        let removed = catalog.drop_relation_constraint(relation_oid, constraint_name)?;
        self.persist_control_state(&catalog)?;

        let removed_depends = old_catalog
            .depend_rows()
            .iter()
            .filter(|row| row.objid == removed.oid || row.refobjid == removed.oid)
            .cloned()
            .collect::<Vec<_>>();
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
            1,
            &kinds,
        )?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        if catalog.has_subclass(relation_oid) {
            return Err(CatalogError::Corrupt(
                "DROP TABLE with inherited children requires CASCADE, which is not supported yet",
            ));
        }

        let affected_parent_oids = catalog
            .inheritance_parents(relation_oid)
            .into_iter()
            .map(|row| row.inhparent)
            .collect::<Vec<_>>();
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
        let oids = drop_relation_oids_by_oid(&catalog, relation_oid)?;
        let mut dropped = Vec::with_capacity(oids.len());
        let mut rows = PhysicalCatalogRows::default();
        let mut inherit_rows = PhysicalCatalogRows::default();
        for oid in oids {
            let Some((name, entry)) = catalog
                .entries()
                .find(|(_, entry)| entry.relation_oid == oid)
                .map(|(name, entry)| (name.to_string(), entry.clone()))
            else {
                continue;
            };
            inherit_rows
                .inherits
                .extend(catalog.inheritance_parents(entry.relation_oid));
            let mut entry_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry);
            entry_rows.inherits.clear();
            extend_physical_catalog_rows(&mut rows, entry_rows);
            dropped.push(entry);
        }

        let mut old_parent_rows = PhysicalCatalogRows::default();
        for (name, entry) in &affected_parent_entries {
            add_catalog_entry_rows(&mut old_parent_rows, &catalog, name, entry);
        }
        let inherit_kinds = vec![BootstrapCatalogKind::PgInherits];
        if !inherit_rows.inherits.is_empty() {
            delete_catalog_rows_subset_mvcc(ctx, &inherit_rows, 1, &inherit_kinds)?;
        }
        for entry in &dropped {
            let _ = catalog.detach_inheritance(entry.relation_oid);
        }

        let kinds = drop_relation_delete_kinds()
            .into_iter()
            .filter(|kind| *kind != BootstrapCatalogKind::PgInherits)
            .collect::<Vec<_>>();
        delete_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        let parent_kinds = vec![BootstrapCatalogKind::PgClass];
        let mut new_parent_rows = PhysicalCatalogRows::default();
        for (name, _) in &affected_parent_entries {
            let Some(entry) = catalog.get(name) else {
                continue;
            };
            add_catalog_entry_rows(&mut new_parent_rows, &catalog, name, entry);
        }
        if !affected_parent_entries.is_empty() {
            delete_catalog_rows_subset_mvcc(ctx, &old_parent_rows, 1, &parent_kinds)?;
            insert_catalog_rows_subset_mvcc(ctx, &new_parent_rows, 1, &parent_kinds)?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &inherit_kinds);
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_catalog_kinds(&mut effect, &parent_kinds);
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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, entry) = catalog
            .entries()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, entry)| (name.to_string(), entry.clone()))
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if entry.relkind != 'v' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry);
        let kinds = drop_relation_delete_kinds();
        delete_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;
        let _ = catalog.remove_by_oid(relation_oid);

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.set_index_ready_valid(relation_oid, indisready, indisvalid)?;
        self.persist_control_state(&catalog)?;

        let kinds = vec![BootstrapCatalogKind::PgIndex];
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) = catalog.alter_table_add_column(relation_oid, column)?;
        self.persist_control_state(&catalog)?;

        let mut kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgDepend,
        ];
        if new_entry
            .desc
            .columns
            .iter()
            .any(|column| column.attrdef_oid.is_some())
        {
            kinds.push(BootstrapCatalogKind::PgAttrdef);
        }
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.alter_table_drop_column(relation_oid, column_name)?;

        let mut kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        if old_entry
            .desc
            .columns
            .iter()
            .any(|column| column.attrdef_oid.is_some())
            || new_entry
                .desc
                .columns
                .iter()
                .any(|column| column.attrdef_oid.is_some())
        {
            kinds.push(BootstrapCatalogKind::PgAttrdef);
        }
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.alter_table_alter_column_type(relation_oid, column_name, new_column)?;
        self.persist_control_state(&catalog)?;

        let mut kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgDepend,
        ];
        if old_entry
            .desc
            .columns
            .iter()
            .any(|column| column.attrdef_oid.is_some())
            || new_entry
                .desc
                .columns
                .iter()
                .any(|column| column.attrdef_oid.is_some())
        {
            kinds.push(BootstrapCatalogKind::PgAttrdef);
        }
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.alter_table_rename_column(relation_oid, column_name, new_column_name)?;

        let kinds = vec![
            BootstrapCatalogKind::PgAttribute,
            BootstrapCatalogKind::PgConstraint,
            BootstrapCatalogKind::PgDepend,
        ];
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (old_name, old_entry, new_name, new_entry) =
            catalog.rename_relation(relation_oid, new_name)?;

        let kinds = vec![
            BootstrapCatalogKind::PgClass,
            BootstrapCatalogKind::PgType,
            BootstrapCatalogKind::PgConstraint,
        ];
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &old_name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &new_name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        effect_record_oid(&mut effect.namespace_oids, new_entry.namespace_oid);
        effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        Ok(effect)
    }

    pub fn alter_relation_owner_mvcc(
        &mut self,
        relation_oid: u32,
        new_owner_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.alter_relation_owner(relation_oid, new_owner_oid)?;

        let mut kinds = vec![BootstrapCatalogKind::PgClass];
        if old_entry.row_type_oid != 0 || new_entry.row_type_oid != 0 {
            kinds.push(BootstrapCatalogKind::PgType);
        }
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        if new_entry.row_type_oid != 0 {
            effect_record_oid(&mut effect.type_oids, new_entry.row_type_oid);
        }
        Ok(effect)
    }

    pub fn set_relation_analyze_stats_mvcc(
        &mut self,
        relation_oid: u32,
        relpages: i32,
        reltuples: f64,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (name, old_entry, new_entry) =
            catalog.set_relation_stats(relation_oid, relpages, reltuples)?;
        let kinds = vec![BootstrapCatalogKind::PgClass];
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &name, &new_entry);
        delete_catalog_rows_subset_mvcc(ctx, &old_rows, 1, &kinds)?;
        insert_catalog_rows_subset_mvcc(ctx, &new_rows, 1, &kinds)?;

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
        let existing = probe_system_catalog_rows_visible(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
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
                1,
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
                1,
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
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let existing = probe_system_catalog_rows_visible(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            PG_DESCRIPTION_O_C_O_INDEX_OID,
            vec![
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 1,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(relation_oid)),
                },
                crate::include::access::scankey::ScanKeyData {
                    attribute_number: 2,
                    strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                    argument: Value::Int64(i64::from(PG_CLASS_RELATION_OID)),
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
                1,
                &[BootstrapCatalogKind::PgDescription],
            )?;
            if let Some(text) = normalized {
                insert_catalog_rows_subset_mvcc(
                    ctx,
                    &PhysicalCatalogRows {
                        descriptions: vec![PgDescriptionRow {
                            objoid: relation_oid,
                            classoid: PG_CLASS_RELATION_OID,
                            objsubid: 0,
                            description: text.to_string(),
                        }],
                        ..PhysicalCatalogRows::default()
                    },
                    1,
                    &[BootstrapCatalogKind::PgDescription],
                )?;
            }
        } else if let Some(text) = normalized {
            insert_catalog_rows_subset_mvcc(
                ctx,
                &PhysicalCatalogRows {
                    descriptions: vec![PgDescriptionRow {
                        objoid: relation_oid,
                        classoid: PG_CLASS_RELATION_OID,
                        objsubid: 0,
                        description: text.to_string(),
                    }],
                    ..PhysicalCatalogRows::default()
                },
                1,
                &[BootstrapCatalogKind::PgDescription],
            )?;
        }

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &[BootstrapCatalogKind::PgDescription]);
        effect_record_oid(&mut effect.relation_oids, relation_oid);
        Ok(effect)
    }
}

fn drop_relation_oids_by_oid(
    catalog: &Catalog,
    relation_oid: u32,
) -> Result<Vec<u32>, CatalogError> {
    let entry = catalog
        .get_by_oid(relation_oid)
        .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
    if entry.relkind != 'r' {
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
            if dependent.relkind != 'r' && dependent.relkind != 'i' && dependent.relkind != 't' {
                continue;
            }
            collect_relation_drop_oids(catalog, depend_rows, dependent.relation_oid, seen, order);
        }
    }

    order.push(relation_oid);
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
