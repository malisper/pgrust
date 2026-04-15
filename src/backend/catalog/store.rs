use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::access::transam::xact::{
    CommandId, Snapshot, TransactionId, TransactionManager,
};
use crate::backend::catalog::bootstrap::bootstrap_catalog_entry;
use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions,
};
use crate::backend::catalog::indexing::{
    insert_bootstrap_system_indexes, probe_system_catalog_rows_visible,
    rebuild_system_catalog_indexes,
};
use crate::backend::catalog::loader::{
    load_catalog_from_physical, load_catalog_from_visible_physical,
    load_physical_catalog_rows_visible,
};
use crate::backend::catalog::persistence::{
    append_catalog_entry_rows, delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
    sync_catalog_rows_subset,
};
use crate::backend::catalog::pg_depend::view_rewrite_depend_rows;
use crate::backend::catalog::role_memberships::{
    NewRoleMembership, grant_membership as grant_role_membership_row,
    revoke_role_membership_option as update_role_membership_row,
};
use crate::backend::catalog::roles::{
    RoleAttributes, alter_role_attributes as alter_role_row, create_role as create_role_row,
    drop_roles as drop_role_rows, rename_role as rename_role_row,
};
use crate::backend::catalog::rowcodec::{
    pg_description_row_from_values, pg_statistic_row_from_values,
};
use crate::backend::catalog::rows::{
    PhysicalCatalogRows, create_index_sync_kinds, create_table_sync_kinds, create_view_sync_kinds,
    drop_relation_delete_kinds, drop_relation_sync_kinds, extend_physical_catalog_rows,
    physical_catalog_rows_for_catalog_entry, physical_catalog_rows_from_catcache,
};
use crate::backend::catalog::toasting::{ToastCatalogChanges, new_relation_create_toast_table};
use crate::backend::executor::RelationDesc;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::TransactionWaiter;
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::utils::misc::interrupts::{InterruptState, check_for_interrupts};
use crate::include::catalog::{
    BootstrapCatalogKind, PG_CLASS_RELATION_OID, PgAuthIdRow, PgAuthMembersRow, PgDependRow,
    PgDescriptionRow, PgNamespaceRow, PgRewriteRow, PgStatisticRow, bootstrap_catalog_kinds,
};
use crate::include::nodes::datum::Value;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;
pub(crate) const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub(crate) const DEFAULT_FIRST_USER_OID: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStore {
    base_dir: PathBuf,
    control_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogMutationEffect {
    pub touched_catalogs: Vec<BootstrapCatalogKind>,
    pub created_rels: Vec<RelFileLocator>,
    pub dropped_rels: Vec<RelFileLocator>,
    pub relation_oids: Vec<u32>,
    pub namespace_oids: Vec<u32>,
    pub type_oids: Vec<u32>,
    pub full_reset: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableResult {
    pub entry: CatalogEntry,
    pub toast: Option<ToastCatalogChanges>,
}

pub struct CatalogWriteContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<RwLock<TransactionManager>>,
    pub xid: TransactionId,
    pub cid: CommandId,
    pub client_id: crate::ClientId,
    pub waiter: Option<std::sync::Arc<TransactionWaiter>>,
    pub interrupts: std::sync::Arc<InterruptState>,
}

impl CatalogWriteContext {
    pub fn check_for_interrupts(&self) -> Result<(), CatalogError> {
        check_for_interrupts(&self.interrupts).map_err(CatalogError::Interrupted)
    }
}

const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
const PG_STATISTIC_RELID_ATT_INH_INDEX_OID: u32 = 2696;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogControl {
    next_oid: u32,
    next_rel_number: u32,
    bootstrap_complete: bool,
}

impl CatalogStore {
    pub fn load(base_dir: impl Into<PathBuf>) -> Result<Self, CatalogError> {
        let base_dir = base_dir.into();
        let global_dir = base_dir.join("global");
        let control_path = global_dir.join("pg_control");
        if let Some(parent) = control_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }

        let (mut catalog, control) = if control_path.exists() {
            let control = load_control_file(&control_path)?;
            let mut catalog = load_catalog_from_visible_physical_startup(&base_dir)?;
            insert_missing_bootstrap_relations(&mut catalog);
            insert_bootstrap_system_indexes(&mut catalog);
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
            (catalog, control)
        } else {
            let catalog = Catalog::default();
            let control = CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            };
            persist_control_file(&control_path, &control)?;
            (catalog, control)
        };

        catalog.next_oid = catalog.next_oid.max(control.next_oid);
        catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        persist_control_file(
            &control_path,
            &CatalogControl {
                next_rel_number: catalog.next_rel_number,
                next_oid: catalog.next_oid.max(control.next_oid),
                bootstrap_complete: control.bootstrap_complete,
            },
        )?;
        sync_physical_catalogs(&base_dir, &catalog)?;

        Ok(Self {
            base_dir,
            control_path,
        })
    }

    pub fn catalog_snapshot(&self) -> Result<Catalog, CatalogError> {
        self.catalog_snapshot_with_control()
    }

    pub fn relcache(&self) -> Result<RelCache, CatalogError> {
        RelCache::from_physical(&self.base_dir)
    }

    pub fn catcache(&self) -> Result<CatCache, CatalogError> {
        CatCache::from_physical(&self.base_dir)
    }

    pub fn catcache_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<CatCache, CatalogError> {
        let rows =
            load_physical_catalog_rows_visible(&self.base_dir, pool, txns, snapshot, client_id)?;
        Ok(CatCache::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.indexes,
            rows.rewrites,
            rows.ams,
            rows.authids,
            rows.auth_members,
            rows.languages,
            rows.ts_parsers,
            rows.ts_templates,
            rows.ts_dicts,
            rows.ts_configs,
            rows.ts_config_maps,
            rows.constraints,
            rows.operators,
            rows.procs,
            rows.casts,
            rows.collations,
            rows.databases,
            rows.tablespaces,
            rows.statistics,
            rows.types,
        ))
    }

    pub fn relation(&self, name: &str) -> Result<Option<RelCacheEntry>, CatalogError> {
        Ok(self.relcache()?.get_by_name(name).cloned())
    }

    pub fn relcache_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<RelCache, CatalogError> {
        let catcache = self.catcache_with_snapshot(pool, txns, snapshot, client_id)?;
        RelCache::from_catcache(&catcache)
    }

    pub fn visible_table_names(&self) -> Result<Vec<String>, CatalogError> {
        let mut names = self
            .relcache()?
            .entries()
            .filter(|(_, entry)| {
                entry.relkind == 'r'
                    && entry.namespace_oid != crate::include::catalog::PG_CATALOG_NAMESPACE_OID
            })
            .map(|(name, _)| name.to_string())
            .filter(|name| !name.contains('.'))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

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
        append_catalog_entry_rows(&self.base_dir, &catalog, &name, &entry, &kinds)?;
        if let Some(toast) = toast {
            append_catalog_entry_rows(
                &self.base_dir,
                &catalog,
                &toast.toast_name,
                &toast.toast_entry,
                &create_table_sync_kinds(&toast.toast_entry),
            )?;
            append_catalog_entry_rows(
                &self.base_dir,
                &catalog,
                &toast.index_name,
                &toast.index_entry,
                &create_index_sync_kinds(),
            )?;
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
        append_catalog_entry_rows(&self.base_dir, &catalog, &index_name, &entry, &kinds)?;
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
        append_catalog_entry_rows(&self.base_dir, &catalog, &index_name, &entry, &kinds)?;
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
            record_toast_effects(&mut effect, &toast);
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

    pub fn create_role(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = create_role_row(
            &mut catalog.authids,
            &mut catalog.next_oid,
            role_name,
            attrs,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        Ok(row)
    }

    pub fn rename_role(
        &mut self,
        role_name: &str,
        new_name: &str,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = rename_role_row(&mut catalog.authids, role_name, new_name)?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        Ok(row)
    }

    pub fn alter_role_attributes(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = alter_role_row(&mut catalog.authids, role_name, attrs)?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        Ok(row)
    }

    pub fn drop_role(&mut self, role_name: &str) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let removed = drop_role_rows(&mut catalog.authids, &[role_name.to_string()])?;
        let removed_row = removed
            .into_iter()
            .next()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        catalog.auth_members.retain(|row| {
            row.roleid != removed_row.oid
                && row.member != removed_row.oid
                && row.grantor != removed_row.oid
        });
        self.persist_catalog_kinds(
            &catalog,
            &[
                BootstrapCatalogKind::PgAuthId,
                BootstrapCatalogKind::PgAuthMembers,
            ],
        )?;
        Ok(removed_row)
    }

    pub fn grant_role_membership(
        &mut self,
        membership: &NewRoleMembership,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = grant_role_membership_row(
            &mut catalog.auth_members,
            &mut catalog.next_oid,
            membership,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthMembers])?;
        Ok(row)
    }

    pub fn update_role_membership_options(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
        admin_option: bool,
        inherit_option: bool,
        set_option: bool,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = update_role_membership_row(
            &mut catalog.auth_members,
            roleid,
            member,
            grantor,
            admin_option,
            inherit_option,
            set_option,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthMembers])?;
        Ok(row)
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
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let constraint = catalog.create_index_backed_constraint(
            relation_oid,
            index_oid,
            conname.into(),
            contype,
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

    pub fn drop_relation_by_oid_mvcc(
        &mut self,
        relation_oid: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(Vec<CatalogEntry>, CatalogMutationEffect), CatalogError> {
        let catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let oids = drop_relation_oids_by_oid(&catalog, relation_oid)?;
        let mut dropped = Vec::with_capacity(oids.len());
        let mut rows = PhysicalCatalogRows::default();
        for oid in oids {
            let Some((name, entry)) = catalog
                .entries()
                .find(|(_, entry)| entry.relation_oid == oid)
                .map(|(name, entry)| (name.to_string(), entry.clone()))
            else {
                continue;
            };
            extend_physical_catalog_rows(
                &mut rows,
                physical_catalog_rows_for_catalog_entry(&catalog, &name, &entry),
            );
            dropped.push(entry);
        }

        let kinds = drop_relation_delete_kinds();
        delete_catalog_rows_subset_mvcc(ctx, &rows, 1, &kinds)?;

        let mut effect = CatalogMutationEffect::default();
        effect_record_catalog_kinds(&mut effect, &kinds);
        for entry in &dropped {
            let comment_effect = self.comment_relation_mvcc(entry.relation_oid, None, ctx)?;
            effect_record_catalog_kinds(&mut effect, &comment_effect.touched_catalogs);
            effect_record_rel(&mut effect.dropped_rels, entry.rel);
            effect_record_oid(&mut effect.relation_oids, entry.relation_oid);
            effect_record_oid(&mut effect.namespace_oids, entry.namespace_oid);
            effect_record_oid(&mut effect.type_oids, entry.row_type_oid);
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
        column: crate::backend::executor::ColumnDesc,
        ctx: &CatalogWriteContext,
    ) -> Result<CatalogMutationEffect, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control_for_snapshot(ctx)?;
        let (_name, old_entry, new_entry) = catalog.alter_table_add_column(relation_oid, column)?;
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
        let old_rows = physical_catalog_rows_for_catalog_entry(&catalog, &_name, &old_entry);
        let new_rows = physical_catalog_rows_for_catalog_entry(&catalog, &_name, &new_entry);
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
        new_column: crate::backend::executor::ColumnDesc,
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

    fn persist_catalog_kinds(
        &self,
        catalog: &Catalog,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), CatalogError> {
        self.persist_control_state(catalog)?;
        sync_physical_catalogs_kinds(&self.base_dir, catalog, kinds)
    }

    fn persist_control_state(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        persist_control_file(
            &self.control_path,
            &CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            },
        )
    }
}

fn insert_missing_bootstrap_relations(catalog: &mut Catalog) {
    for kind in bootstrap_catalog_kinds() {
        if catalog.get_by_oid(kind.relation_oid()).is_none() {
            catalog.insert(kind.relation_name(), bootstrap_catalog_entry(kind));
        }
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

impl CatalogStore {
    fn catalog_snapshot_with_control(&self) -> Result<Catalog, CatalogError> {
        let mut catalog = load_catalog_from_physical(&self.base_dir)?;
        if self.control_path.exists() {
            let control = load_control_file(&self.control_path)?;
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        }
        Ok(catalog)
    }

    fn catalog_snapshot_with_control_for_snapshot(
        &self,
        ctx: &CatalogWriteContext,
    ) -> Result<Catalog, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let txns = ctx.txns.read();
        let mut catalog = load_catalog_from_visible_physical(
            &self.base_dir,
            &ctx.pool,
            &txns,
            &snapshot,
            ctx.client_id,
        )?;
        if self.control_path.exists() {
            let control = load_control_file(&self.control_path)?;
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        }
        Ok(catalog)
    }
}

#[cfg(test)]
pub(crate) fn sync_catalog_heaps_for_tests(
    base_dir: &Path,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    crate::backend::catalog::persistence::sync_catalog_rows(base_dir, &rows, 1)
}

impl CatalogStore {
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

fn load_catalog_from_visible_physical_startup(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let txns = TransactionManager::new_durable(base_dir.to_path_buf())
        .map_err(|e| CatalogError::Io(format!("transaction status load failed: {e:?}")))?;
    let snapshot = txns
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|e| CatalogError::Io(format!("startup catalog snapshot failed: {e:?}")))?;
    let pool = BufferPool::new(SmgrStorageBackend::new(MdStorageManager::new(base_dir)), 64);
    load_catalog_from_visible_physical(base_dir, &pool, &txns, &snapshot, 0)
}

fn sync_physical_catalogs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    sync_physical_catalogs_kinds(base_dir, catalog, &bootstrap_catalog_kinds())
}

fn sync_physical_catalogs_kinds(
    base_dir: &Path,
    catalog: &Catalog,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let mut rows = physical_catalog_rows_from_catcache(&catcache);
    if kinds.contains(&BootstrapCatalogKind::PgDescription) {
        let pool = std::sync::Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(base_dir)),
            64,
        ));
        let txns = TransactionManager::new_durable(base_dir.to_path_buf()).unwrap_or_default();
        if let Ok(snapshot) = txns.snapshot(INVALID_TRANSACTION_ID) {
            if let Ok(existing_rows) =
                load_physical_catalog_rows_visible(base_dir, &pool, &txns, &snapshot, 0)
            {
                rows.descriptions = existing_rows.descriptions;
            }
        }
    }
    sync_catalog_rows_subset(base_dir, &rows, 1, kinds)?;
    rebuild_system_catalog_indexes(base_dir)
}

fn effect_record_catalog_kinds(effect: &mut CatalogMutationEffect, kinds: &[BootstrapCatalogKind]) {
    for &kind in kinds {
        if !effect.touched_catalogs.contains(&kind) {
            effect.touched_catalogs.push(kind);
        }
    }
}

fn effect_record_rel(rels: &mut Vec<RelFileLocator>, rel: RelFileLocator) {
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

fn persist_control_file(path: &Path, control: &CatalogControl) -> Result<(), CatalogError> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.next_oid.to_le_bytes());
    bytes.extend_from_slice(&control.next_rel_number.to_le_bytes());
    bytes.extend_from_slice(&(u32::from(control.bootstrap_complete)).to_le_bytes());
    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_control_file(path: &Path) -> Result<CatalogControl, CatalogError> {
    let bytes = fs::read(path).map_err(|e| CatalogError::Io(e.to_string()))?;
    if bytes.len() != 16 {
        return Err(CatalogError::Corrupt("invalid control file size"));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != CONTROL_FILE_MAGIC {
        return Err(CatalogError::Corrupt("invalid control magic"));
    }
    let next_oid = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let next_rel_number = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let bootstrap_complete = match u32::from_le_bytes(bytes[12..16].try_into().unwrap()) {
        0 => false,
        1 => true,
        _ => return Err(CatalogError::Corrupt("invalid bootstrap flag")),
    };

    Ok(CatalogControl {
        next_oid,
        next_rel_number,
        bootstrap_complete,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::column_desc;
    use crate::backend::catalog::loader::load_physical_catalog_rows;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::backend::storage::smgr::ForkNumber;
    use crate::backend::storage::smgr::segment_path;
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_NAME, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, C_COLLATION_OID,
        CURRENT_DATABASE_NAME, DEFAULT_COLLATION_OID, DEFAULT_TABLESPACE_OID, DEPENDENCY_AUTO,
        DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, HEAP_TABLE_AM_OID, INT4_TYPE_OID, INT8_TYPE_OID,
        JSON_TYPE_OID, OID_TYPE_OID, PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID,
        PG_CONSTRAINT_RELATION_OID, PG_LANGUAGE_INTERNAL_OID, PG_NAMESPACE_RELATION_OID,
        PG_TOAST_NAMESPACE_OID, PG_TYPE_RELATION_OID, POSIX_COLLATION_OID, PUBLIC_NAMESPACE_OID,
        TEXT_TYPE_OID, VARCHAR_TYPE_OID,
    };
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pgrust_catalog_{label}_{nanos}"))
    }

    #[test]
    fn catalog_store_roundtrips() {
        let base = temp_dir("roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        assert!(store.catalog_snapshot().unwrap().get("pg_class").is_some());
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        assert_eq!(entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert!(entry.relation_oid >= DEFAULT_FIRST_USER_OID);

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_entry = reopened_catalog.get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
    }

    #[test]
    fn catalog_store_persists_column_defaults() {
        let base = temp_dir("defaults_roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("b1", SqlType::with_bit_len(SqlTypeKind::Bit, 4), false),
                column_desc("b2", SqlType::with_bit_len(SqlTypeKind::VarBit, 5), true),
            ],
        };
        desc.columns[0].default_expr = Some("'1001'".into());
        desc.columns[1].default_expr = Some("B'0101'".into());
        store.create_table("bit_defaults", desc).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let entry = relcache.get_by_name("bit_defaults").unwrap();
        assert_eq!(
            entry.desc.columns[0].default_expr.as_deref(),
            Some("'1001'")
        );
        assert_eq!(
            entry.desc.columns[1].default_expr.as_deref(),
            Some("B'0101'")
        );
    }

    #[test]
    fn catalog_store_persists_pg_attrdef_rows() {
        let base = temp_dir("attrdef_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'hello'");
        assert!(attrdef.oid >= DEFAULT_FIRST_USER_OID);
    }

    #[test]
    fn catalog_store_persists_pg_depend_rows() {
        let base = temp_dir("depend_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();
        let attrdef_oid = entry.desc.columns[1].attrdef_oid.unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_NORMAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_TYPE_RELATION_OID
                && row.objid == entry.row_type_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_INTERNAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_ATTRDEF_RELATION_OID
                && row.objid == attrdef_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CONSTRAINT_RELATION_OID
                && row.objid == constraint_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.depend_rows().iter().any(|row| {
            row.classid == PG_CONSTRAINT_RELATION_OID
                && row.objid == constraint_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));
    }

    #[test]
    fn catalog_store_persists_pg_index_rows() {
        let base = temp_dir("index_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let index = store
            .create_index(
                "people_name_idx",
                "people",
                true,
                &["id".into(), "name".into()],
            )
            .unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let index_row = rows
            .indexes
            .iter()
            .find(|row| row.indexrelid == index.relation_oid)
            .unwrap();
        assert_eq!(index_row.indrelid, table.relation_oid);
        assert_eq!(index_row.indnatts, 2);
        assert_eq!(index_row.indnkeyatts, 2);
        assert!(index_row.indisunique);
        assert_eq!(index_row.indkey, vec![1, 2]);

        let class_row = rows
            .classes
            .iter()
            .find(|row| row.oid == index.relation_oid)
            .unwrap();
        assert_eq!(class_row.relkind, 'i');
        assert_eq!(class_row.relam, BTREE_AM_OID);
        assert_eq!(class_row.relpersistence, 'p');
        assert_eq!(class_row.relnamespace, PUBLIC_NAMESPACE_OID);
        assert_eq!(class_row.reltype, 0);

        let table_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.relation_oid)
            .unwrap();
        assert_eq!(table_row.relam, HEAP_TABLE_AM_OID);
        assert_eq!(table_row.relpersistence, 'p');

        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_index = reopened_catalog.get("people_name_idx").unwrap();
        assert_eq!(reopened_index.relkind, 'i');
        assert_eq!(
            reopened_index.index_meta.as_ref().map(|meta| (
                meta.indrelid,
                meta.indkey.clone(),
                meta.indisunique
            )),
            Some((table.relation_oid, vec![1, 2], true))
        );
    }

    #[test]
    fn catalog_store_creates_toast_table_and_index() {
        let base = temp_dir("toast_create");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        assert_ne!(table.reltoastrelid, 0);

        let rows = load_physical_catalog_rows(&base).unwrap();
        let table_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.relation_oid)
            .unwrap();
        assert_eq!(table_row.reltoastrelid, table.reltoastrelid);

        let toast_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.reltoastrelid)
            .unwrap();
        assert_eq!(toast_row.relkind, 't');
        assert_eq!(toast_row.relnamespace, PG_TOAST_NAMESPACE_OID);

        let toast_index = rows
            .indexes
            .iter()
            .find(|row| row.indrelid == toast_row.oid)
            .unwrap();
        assert!(toast_index.indisunique);
        assert_eq!(toast_index.indkey, vec![1, 2]);

        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == toast_row.oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.deptype == DEPENDENCY_INTERNAL
        }));
    }

    #[test]
    fn catalog_store_persists_pg_am_rows() {
        let base = temp_dir("am_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.ams.iter().any(|row| {
            row.oid == HEAP_TABLE_AM_OID
                && row.amname == "heap"
                && row.amhandler == 3
                && row.amtype == 't'
        }));
        assert!(rows.ams.iter().any(|row| {
            row.oid == BTREE_AM_OID
                && row.amname == "btree"
                && row.amhandler == 330
                && row.amtype == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_authid_rows() {
        let base = temp_dir("authid_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.authids.iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID
                && row.rolname == BOOTSTRAP_SUPERUSER_NAME
                && row.rolsuper
                && row.rolcreatedb
                && row.rolcanlogin
                && row.rolconnlimit == -1
        }));
    }

    #[test]
    fn catalog_store_persists_pg_auth_members_rows() {
        let base = temp_dir("auth_members_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.auth_members.is_empty());
    }

    #[test]
    fn catalog_store_persists_pg_language_rows() {
        let base = temp_dir("language_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.languages.iter().any(|row| {
            row.oid == PG_LANGUAGE_INTERNAL_OID
                && row.lanname == "internal"
                && row.lanowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(
            rows.languages
                .iter()
                .any(|row| row.lanname == "sql" && row.lanpltrusted)
        );
    }

    #[test]
    fn catalog_store_persists_created_role_rows() {
        let base = temp_dir("create_role_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let created = store
            .create_role(
                "app_user",
                &crate::backend::catalog::roles::RoleAttributes {
                    rolcanlogin: true,
                    ..crate::backend::catalog::roles::RoleAttributes::default()
                },
            )
            .unwrap();
        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(
            rows.authids
                .iter()
                .any(|row| row.oid == created.oid && row.rolname == "app_user" && row.rolcanlogin)
        );
    }

    #[test]
    fn catalog_store_renames_and_drops_role_rows() {
        let base = temp_dir("rename_drop_role_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_role(
                "app_user",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let renamed = store.rename_role("app_user", "app_owner").unwrap();
        assert_eq!(renamed.rolname, "app_owner");
        let dropped = store.drop_role("app_owner").unwrap();
        assert_eq!(dropped.rolname, "app_owner");

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(
            !rows
                .authids
                .iter()
                .any(|row| row.rolname == "app_user" || row.rolname == "app_owner")
        );
    }

    #[test]
    fn catalog_store_persists_role_memberships_and_option_updates() {
        let base = temp_dir("auth_membership_mutations");
        let mut store = CatalogStore::load(&base).unwrap();
        let parent = store
            .create_role(
                "parent_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let member = store
            .create_role(
                "member_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let created = store
            .grant_role_membership(
                &crate::backend::catalog::role_memberships::NewRoleMembership {
                    roleid: parent.oid,
                    member: member.oid,
                    grantor: BOOTSTRAP_SUPERUSER_OID,
                    admin_option: false,
                    inherit_option: true,
                    set_option: true,
                },
            )
            .unwrap();
        let updated = store
            .update_role_membership_options(
                parent.oid,
                member.oid,
                BOOTSTRAP_SUPERUSER_OID,
                true,
                false,
                false,
            )
            .unwrap();
        assert_eq!(created.oid, updated.oid);
        assert!(updated.admin_option);

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(rows.auth_members.iter().any(|row| {
            row.oid == created.oid
                && row.roleid == parent.oid
                && row.member == member.oid
                && row.admin_option
                && !row.inherit_option
                && !row.set_option
        }));
    }

    #[test]
    fn catalog_store_persists_pg_operator_rows() {
        let base = temp_dir("operator_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.operators.iter().any(|row| {
            row.oid == 91
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BOOL_TYPE_OID
                && row.oprright == crate::include::catalog::BOOL_TYPE_OID
                && row.oprcode == crate::include::catalog::BOOL_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 96
                && row.oprname == "="
                && row.oprleft == INT4_TYPE_OID
                && row.oprright == INT4_TYPE_OID
                && row.oprcode == crate::include::catalog::INT4_CMP_EQ_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 3877
                && row.oprname == "^@"
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_STARTS_WITH_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 664
                && row.oprname == "<"
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 667
                && row.oprname == ">="
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_CMP_GE_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1784
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BIT_TYPE_OID
                && row.oprright == crate::include::catalog::BIT_TYPE_OID
                && row.oprcode == crate::include::catalog::BIT_CMP_EQ_PROC_OID
                && row.oprcanmerge
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1806
                && row.oprname == "<"
                && row.oprleft == crate::include::catalog::VARBIT_TYPE_OID
                && row.oprright == crate::include::catalog::VARBIT_TYPE_OID
                && row.oprcode == crate::include::catalog::VARBIT_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1955
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprcode == crate::include::catalog::BYTEA_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1957
                && row.oprname == "<"
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprcode == crate::include::catalog::BYTEA_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 3240
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::JSONB_TYPE_OID
                && row.oprright == crate::include::catalog::JSONB_TYPE_OID
                && row.oprcode == crate::include::catalog::JSONB_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
    }

    #[test]
    fn catalog_store_persists_pg_constraint_rows() {
        let base = temp_dir("constraint_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid
                && row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
                && row.connamespace == PUBLIC_NAMESPACE_OID
                && row.convalidated
        }));
    }

    #[test]
    fn catalog_store_loads_not_null_constraint_oids_from_pg_constraint() {
        let base = temp_dir("constraint_oid_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_entry = reopened_catalog.get("people").unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert_eq!(
            reopened_entry.desc.columns[0].not_null_constraint_oid,
            Some(constraint_oid)
        );
        assert!(reopened_catalog.next_oid() > constraint_oid);
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid
                && row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
                && row.connamespace == PUBLIC_NAMESPACE_OID
                && row.convalidated
        }));
    }

    #[test]
    fn physical_catalog_rows_for_entry_use_first_class_constraint_and_depend_rows() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let constraint = catalog
            .constraints
            .iter_mut()
            .find(|row| row.oid == constraint_oid)
            .unwrap();
        constraint.conname = "people_id_custom_not_null".into();

        let depend = catalog
            .depends
            .iter_mut()
            .find(|row| row.objid == constraint_oid)
            .unwrap();
        depend.deptype = DEPENDENCY_INTERNAL;

        let rows = physical_catalog_rows_for_catalog_entry(&catalog, "people", &entry);
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid && row.conname == "people_id_custom_not_null"
        }));
        assert!(
            rows.constraints
                .iter()
                .all(|row| row.oid != constraint_oid || row.conname != "people_id_not_null")
        );
        assert!(
            rows.depends
                .iter()
                .any(|row| row.objid == constraint_oid && row.deptype == DEPENDENCY_INTERNAL)
        );
    }

    #[test]
    fn catalog_store_persists_pg_proc_rows() {
        let base = temp_dir("proc_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.procs.iter().any(|row| {
            row.proname == "lower"
                && row.pronargs == 1
                && row.prorettype == TEXT_TYPE_OID
                && row.prokind == 'f'
                && row.prosrc == "lower"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "count"
                && row.pronargs == 1
                && row.prorettype == INT8_TYPE_OID
                && row.prokind == 'a'
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "numeric"
                && row.proargtypes == INT4_TYPE_OID.to_string()
                && row.prorettype == crate::include::catalog::NUMERIC_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "biteq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BIT_TYPE_OID,
                        crate::include::catalog::BIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "biteq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "varbitlt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::VARBIT_TYPE_OID,
                        crate::include::catalog::VARBIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "varbitlt"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "byteaeq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "byteaeq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "bytealt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "bytealt"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "jsonb_eq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::JSONB_TYPE_OID,
                        crate::include::catalog::JSONB_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prokind == 'f'
                && row.prosrc == "jsonb_eq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "json_array_elements" && row.proretset && row.prorettype == JSON_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_LT_PROC_OID
                && row.proname == "text_lt"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_GE_PROC_OID
                && row.proname == "text_ge"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
    }

    #[test]
    fn catalog_store_persists_pg_collation_rows() {
        let base = temp_dir("collation_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert_eq!(
            rows.collations
                .iter()
                .map(|row| (
                    row.oid,
                    row.collname.as_str(),
                    row.collprovider,
                    row.collowner
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    DEFAULT_COLLATION_OID,
                    "default",
                    'd',
                    BOOTSTRAP_SUPERUSER_OID,
                ),
                (C_COLLATION_OID, "C", 'c', BOOTSTRAP_SUPERUSER_OID),
                (POSIX_COLLATION_OID, "POSIX", 'c', BOOTSTRAP_SUPERUSER_OID),
            ]
        );
    }

    #[test]
    fn catalog_store_persists_pg_cast_rows() {
        let base = temp_dir("cast_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == OID_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == crate::include::catalog::NUMERIC_TYPE_OID
                && row.castfunc != 0
                && row.castcontext == 'i'
                && row.castmethod == 'f'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == VARCHAR_TYPE_OID
                && row.casttarget == TEXT_TYPE_OID
                && row.castcontext == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONPATH_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::VARBIT_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::INT4_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_database_rows() {
        let base = temp_dir("database_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.databases.iter().any(|row| {
            row.oid == 1
                && row.datname == CURRENT_DATABASE_NAME
                && row.datdba == BOOTSTRAP_SUPERUSER_OID
                && row.dattablespace == DEFAULT_TABLESPACE_OID
                && !row.datistemplate
                && row.datallowconn
        }));
    }

    #[test]
    fn catalog_store_persists_pg_tablespace_rows() {
        let base = temp_dir("tablespace_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == crate::include::catalog::GLOBAL_TABLESPACE_OID
                && row.spcname == "pg_global"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
    }

    #[test]
    fn catalog_store_drop_table_cascades_indexes() {
        let base = temp_dir("drop_index_cascade");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let index = store
            .create_index("people_name_idx", "people", false, &["name".into()])
            .unwrap();

        let dropped = store.drop_table("people").unwrap();
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == index.relation_oid)
        );
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.relation_oid)
        );
        assert!(dropped.iter().any(|entry| entry.relkind == 't'));
        assert!(dropped.iter().any(|entry| {
            entry.relkind == 'i'
                && entry
                    .index_meta
                    .as_ref()
                    .is_some_and(|meta| meta.indrelid == table.reltoastrelid)
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_none());
        assert!(reopened_catalog.get("people_name_idx").is_none());

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(!rows.classes.iter().any(|row| row.oid == table.relation_oid));
        assert!(!rows.classes.iter().any(|row| row.oid == index.relation_oid));
        assert!(
            !rows
                .indexes
                .iter()
                .any(|row| row.indexrelid == index.relation_oid)
        );
        assert!(
            !rows
                .depends
                .iter()
                .any(|row| row.objid == index.relation_oid)
        );
    }

    #[test]
    fn catalog_store_drop_table_cascades_toast_relations() {
        let base = temp_dir("drop_toast_cascade");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let dropped = store.drop_table("docs").unwrap();
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.relation_oid)
        );
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.reltoastrelid && entry.relkind == 't')
        );
        assert!(dropped.iter().any(|entry| {
            entry.relkind == 'i'
                && entry
                    .index_meta
                    .as_ref()
                    .is_some_and(|meta| meta.indrelid == table.reltoastrelid)
        }));
    }

    #[test]
    fn catalog_store_drop_table_removes_constraint_and_depend_rows() {
        let base = temp_dir("drop_constraint_depend_cleanup");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();
        let attrdef_oid = entry.desc.columns[1].attrdef_oid.unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let dropped = store.drop_table("notes").unwrap();
        assert!(
            dropped
                .iter()
                .any(|dropped| dropped.relation_oid == entry.relation_oid)
        );
        assert!(dropped.iter().any(|dropped| dropped.relkind == 't'));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("notes").is_none());
        assert!(
            reopened_catalog
                .constraint_rows()
                .iter()
                .all(|row| row.conrelid != entry.relation_oid)
        );
        assert!(reopened_catalog.depend_rows().iter().all(|row| {
            row.objid != entry.relation_oid
                && row.refobjid != entry.relation_oid
                && row.objid != attrdef_oid
                && row.objid != constraint_oid
        }));

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.constraints
                .iter()
                .all(|row| row.conrelid != entry.relation_oid)
        );
        assert!(rows.depends.iter().all(|row| {
            row.objid != entry.relation_oid
                && row.refobjid != entry.relation_oid
                && row.objid != attrdef_oid
                && row.objid != constraint_oid
        }));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_create_table_appends_to_touched_catalog_relations() {
        let base = temp_dir("selective_catalog_sync_create_table");
        let mut store = CatalogStore::load(&base).unwrap();
        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let proc_meta_before = fs::metadata(&proc_path).unwrap();
        let class_meta_before = fs::metadata(&class_path).unwrap();

        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let proc_meta_after = fs::metadata(&proc_path).unwrap();
        let class_meta_after = fs::metadata(&class_path).unwrap();
        assert_eq!(proc_meta_before.ino(), proc_meta_after.ino());
        assert_eq!(
            proc_meta_before.modified().unwrap(),
            proc_meta_after.modified().unwrap()
        );
        assert_eq!(class_meta_before.ino(), class_meta_after.ino());
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_create_index_appends_to_touched_catalog_relations() {
        let base = temp_dir("selective_catalog_sync_create_index");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let index_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let proc_meta_before = fs::metadata(&proc_path).unwrap();
        let class_meta_before = fs::metadata(&class_path).unwrap();
        let index_meta_before = fs::metadata(&index_path).unwrap();

        store
            .create_index("people_id_idx", "people", false, &["id".into()])
            .unwrap();

        let proc_meta_after = fs::metadata(&proc_path).unwrap();
        let class_meta_after = fs::metadata(&class_path).unwrap();
        let index_meta_after = fs::metadata(&index_path).unwrap();
        assert_eq!(proc_meta_before.ino(), proc_meta_after.ino());
        assert_eq!(
            proc_meta_before.modified().unwrap(),
            proc_meta_after.modified().unwrap()
        );
        assert_eq!(class_meta_before.ino(), class_meta_after.ino());
        assert_eq!(index_meta_before.ino(), index_meta_after.ino());
    }

    #[test]
    fn catalog_store_bootstraps_physical_core_catalog_relfiles() {
        let base = temp_dir("physical_bootstrap");
        let store = CatalogStore::load(&base).unwrap();
        let catalog = store.catalog_snapshot().unwrap();
        for name in ["pg_namespace", "pg_type", "pg_attribute", "pg_class"] {
            let entry = catalog.get(name).unwrap();
            let path = segment_path(&base, entry.rel, ForkNumber::Main, 0);
            let meta = fs::metadata(path).unwrap();
            assert!(meta.len() > 0, "{name} should have heap data");
        }

        let attrdef = catalog.get("pg_attrdef").unwrap();
        let attrdef_path = segment_path(&base, attrdef.rel, ForkNumber::Main, 0);
        assert!(attrdef_path.exists(), "pg_attrdef relfile should exist");
        let depend = catalog.get("pg_depend").unwrap();
        let depend_path = segment_path(&base, depend.rel, ForkNumber::Main, 0);
        assert!(depend_path.exists(), "pg_depend relfile should exist");
        let index = catalog.get("pg_index").unwrap();
        let index_path = segment_path(&base, index.rel, ForkNumber::Main, 0);
        assert!(index_path.exists(), "pg_index relfile should exist");
        let database = catalog.get("pg_database").unwrap();
        let database_path = segment_path(&base, database.rel, ForkNumber::Main, 0);
        assert!(database_path.exists(), "pg_database relfile should exist");
        let authid = catalog.get("pg_authid").unwrap();
        let authid_path = segment_path(&base, authid.rel, ForkNumber::Main, 0);
        assert!(authid_path.exists(), "pg_authid relfile should exist");
        let auth_members = catalog.get("pg_auth_members").unwrap();
        let auth_members_path = segment_path(&base, auth_members.rel, ForkNumber::Main, 0);
        assert!(
            auth_members_path.exists(),
            "pg_auth_members relfile should exist"
        );
        let collation = catalog.get("pg_collation").unwrap();
        let collation_path = segment_path(&base, collation.rel, ForkNumber::Main, 0);
        assert!(collation_path.exists(), "pg_collation relfile should exist");
        let language = catalog.get("pg_language").unwrap();
        let language_path = segment_path(&base, language.rel, ForkNumber::Main, 0);
        assert!(language_path.exists(), "pg_language relfile should exist");
        let operator = catalog.get("pg_operator").unwrap();
        let operator_path = segment_path(&base, operator.rel, ForkNumber::Main, 0);
        assert!(operator_path.exists(), "pg_operator relfile should exist");
        let proc = catalog.get("pg_proc").unwrap();
        let proc_path = segment_path(&base, proc.rel, ForkNumber::Main, 0);
        assert!(proc_path.exists(), "pg_proc relfile should exist");
        let cast = catalog.get("pg_cast").unwrap();
        let cast_path = segment_path(&base, cast.rel, ForkNumber::Main, 0);
        assert!(cast_path.exists(), "pg_cast relfile should exist");
        let constraint = catalog.get("pg_constraint").unwrap();
        let constraint_path = segment_path(&base, constraint.rel, ForkNumber::Main, 0);
        assert!(
            constraint_path.exists(),
            "pg_constraint relfile should exist"
        );
        let am = catalog.get("pg_am").unwrap();
        let am_path = segment_path(&base, am.rel, ForkNumber::Main, 0);
        assert!(am_path.exists(), "pg_am relfile should exist");
        let tablespace = catalog.get("pg_tablespace").unwrap();
        let tablespace_path = segment_path(&base, tablespace.rel, ForkNumber::Main, 0);
        assert!(
            tablespace_path.exists(),
            "pg_tablespace relfile should exist"
        );
    }

    #[test]
    fn catalog_store_loads_from_physical_catalogs_without_schema_file() {
        let base = temp_dir("physical_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "shipments",
                RelationDesc {
                    columns: vec![column_desc(
                        "tags",
                        SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                        true,
                    )],
                },
            )
            .unwrap();
        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("shipments").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
        );
    }

    #[test]
    fn catalog_store_roundtrips_zero_column_tables() {
        let base = temp_dir("zero_columns");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "zerocol",
                RelationDesc {
                    columns: Vec::new(),
                },
            )
            .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("zerocol").unwrap();
        assert!(entry.desc.columns.is_empty());
    }

    #[test]
    fn catalog_store_preserves_relation_allocators_across_drop_and_reload() {
        let base = temp_dir("allocator_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let first = store
            .create_table(
                "first",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        store.drop_table("first").unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let mut reopened = reopened;
        let second = reopened
            .create_table(
                "second",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        assert!(second.rel.rel_number > first.rel.rel_number);
        assert!(second.relation_oid > first.relation_oid);
        assert!(second.row_type_oid > first.row_type_oid);
    }

    #[test]
    fn catalog_store_migrates_legacy_defaults_json_into_pg_attrdef() {
        let base = temp_dir("legacy_defaults_migration");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'legacy'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let attrdef_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAttrdef.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&attrdef_path).unwrap();
        let legacy_dir = base.join("catalog");
        fs::create_dir_all(&legacy_dir).unwrap();
        fs::write(
            legacy_dir.join("defaults.json"),
            format!(
                r#"[{{"relation_oid":{},"attnum":2,"expr":"'legacy'"}}]"#,
                entry.relation_oid
            ),
        )
        .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let migrated = relcache.get_by_name("notes").unwrap();
        assert_eq!(
            migrated.desc.columns[1].default_expr.as_deref(),
            Some("'legacy'")
        );
        assert!(migrated.desc.columns[1].attrdef_oid.is_some());

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'legacy'");
        assert!(attrdef.oid > entry.row_type_oid);
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_depend_relation() {
        let base = temp_dir("missing_depend_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let depend_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgDepend.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&depend_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
        }));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_index_relation() {
        let base = temp_dir("missing_index_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let index_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&index_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(index_path.exists(), "pg_index relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.indexes.is_empty());
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_am_relation() {
        let base = temp_dir("missing_am_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let am_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAm.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&am_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(am_path.exists(), "pg_am relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.ams
                .iter()
                .any(|row| row.oid == HEAP_TABLE_AM_OID && row.amname == "heap")
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_database_relation() {
        let base = temp_dir("missing_database_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let database_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgDatabase.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&database_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            database_path.exists(),
            "pg_database relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.databases
                .iter()
                .any(|row| row.datname == CURRENT_DATABASE_NAME)
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_authid_relation() {
        let base = temp_dir("missing_authid_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let authid_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAuthId.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&authid_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            authid_path.exists(),
            "pg_authid relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.authids.iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID && row.rolname == BOOTSTRAP_SUPERUSER_NAME
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_auth_members_relation() {
        let base = temp_dir("missing_auth_members_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let auth_members_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAuthMembers.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&auth_members_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            auth_members_path.exists(),
            "pg_auth_members relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.auth_members.is_empty());
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_collation_relation() {
        let base = temp_dir("missing_collation_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let collation_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgCollation.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&collation_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            collation_path.exists(),
            "pg_collation relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.collations
                .iter()
                .any(|row| row.oid == DEFAULT_COLLATION_OID && row.collname == "default")
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_cast_relation() {
        let base = temp_dir("missing_cast_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cast_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgCast.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&cast_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(cast_path.exists(), "pg_cast relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.casts
                .iter()
                .any(|row| { row.castsource == INT4_TYPE_OID && row.casttarget == OID_TYPE_OID })
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_proc_relation() {
        let base = temp_dir("missing_proc_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&proc_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(proc_path.exists(), "pg_proc relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.procs
                .iter()
                .any(|row| row.proname == "lower" && row.prorettype == TEXT_TYPE_OID)
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_language_relation() {
        let base = temp_dir("missing_language_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let language_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgLanguage.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&language_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            language_path.exists(),
            "pg_language relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(
            rows.languages
                .iter()
                .any(|row| row.oid == PG_LANGUAGE_INTERNAL_OID && row.lanname == "internal")
        );
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_operator_relation() {
        let base = temp_dir("missing_operator_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let operator_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgOperator.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&operator_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            operator_path.exists(),
            "pg_operator relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.operators.iter().any(|row| {
            row.oid == 96
                && row.oprname == "="
                && row.oprleft == INT4_TYPE_OID
                && row.oprright == INT4_TYPE_OID
                && row.oprcode == crate::include::catalog::INT4_CMP_EQ_PROC_OID
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_constraint_relation() {
        let base = temp_dir("missing_constraint_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let constraint_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgConstraint.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&constraint_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            constraint_path.exists(),
            "pg_constraint relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.constraints.iter().any(|row| {
            row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_tablespace_relation() {
        let base = temp_dir("missing_tablespace_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let tablespace_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgTablespace.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&tablespace_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            tablespace_path.exists(),
            "pg_tablespace relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }
}
