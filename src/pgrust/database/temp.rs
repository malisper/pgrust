use super::*;
use crate::backend::catalog::persistence::delete_catalog_rows_subset_mvcc;
use crate::backend::catalog::rows::{PhysicalCatalogRows, drop_relation_delete_kinds};
use crate::backend::utils::cache::catcache::CatCache;
use crate::include::catalog::{
    BootstrapCatalogKind, PG_ATTRDEF_RELATION_OID, PG_CAST_RELATION_OID, PG_CLASS_RELATION_OID,
    PG_CONSTRAINT_RELATION_OID, PG_OPERATOR_RELATION_OID, PG_POLICY_RELATION_OID,
    PG_PROC_RELATION_OID, PG_PUBLICATION_REL_RELATION_OID, PG_REWRITE_RELATION_OID,
    PG_STATISTIC_EXT_RELATION_OID, PG_TRIGGER_RELATION_OID, PG_TYPE_RELATION_OID,
    relkind_has_storage,
};

fn normalize_temp_lookup_name(table_name: &str) -> String {
    table_name
        .strip_prefix("pg_temp.")
        .unwrap_or(table_name)
        .to_ascii_lowercase()
}

fn push_unique<T: PartialEq>(target: &mut Vec<T>, row: T) {
    if !target.contains(&row) {
        target.push(row);
    }
}

fn push_oid(target: &mut Vec<u32>, oid: u32) {
    if oid != 0 && !target.contains(&oid) {
        target.push(oid);
    }
}

fn proc_argtype_oids(argtypes: &str) -> impl Iterator<Item = u32> + '_ {
    argtypes
        .split_whitespace()
        .filter_map(|part| part.parse::<u32>().ok())
}

fn collect_temp_on_commit_drop_names(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    namespace: &TempNamespace,
    relation_oid: u32,
    seen: &mut BTreeSet<u32>,
    names: &mut Vec<String>,
) {
    if !seen.insert(relation_oid) {
        return;
    }
    let mut children = catalog.inheritance_children(relation_oid);
    children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    for child in children.into_iter().filter(|row| !row.inhdetachpending) {
        collect_temp_on_commit_drop_names(catalog, namespace, child.inhrelid, seen, names);
    }
    if let Some(name) = namespace
        .tables
        .iter()
        .find_map(|(name, entry)| (entry.entry.relation_oid == relation_oid).then(|| name.clone()))
    {
        names.push(name);
    }
}

fn temp_namespace_catalog_rows(
    catcache: &CatCache,
    namespace_oids: &[u32],
    temp_db_oid: u32,
) -> (PhysicalCatalogRows, CatalogMutationEffect) {
    let namespace_set = namespace_oids.iter().copied().collect::<BTreeSet<_>>();
    let classes = catcache
        .class_rows()
        .into_iter()
        .filter(|row| row.relpersistence == 't' && namespace_set.contains(&row.relnamespace))
        .collect::<Vec<_>>();
    let relation_oids = classes.iter().map(|row| row.oid).collect::<BTreeSet<_>>();
    let mut type_oids = classes
        .iter()
        .filter_map(|row| (row.reltype != 0).then_some(row.reltype))
        .collect::<BTreeSet<_>>();

    let mut rows = PhysicalCatalogRows::default();
    for row in classes {
        push_unique(&mut rows.classes, row);
    }
    for row in catcache.type_rows() {
        if relation_oids.contains(&row.typrelid)
            || type_oids.contains(&row.oid)
            || namespace_set.contains(&row.typnamespace)
        {
            type_oids.insert(row.oid);
            if row.typarray != 0 {
                type_oids.insert(row.typarray);
            }
            push_unique(&mut rows.types, row);
        }
    }
    for row in catcache.type_rows() {
        if type_oids.contains(&row.oid) {
            push_unique(&mut rows.types, row);
        }
    }
    for row in catcache.attribute_rows() {
        if relation_oids.contains(&row.attrelid) {
            push_unique(&mut rows.attributes, row);
        }
    }
    for row in catcache.attrdef_rows() {
        if relation_oids.contains(&row.adrelid) {
            push_unique(&mut rows.attrdefs, row);
        }
    }
    for row in catcache.index_rows() {
        if relation_oids.contains(&row.indexrelid) || relation_oids.contains(&row.indrelid) {
            push_unique(&mut rows.indexes, row);
        }
    }
    for row in catcache.inherit_rows() {
        if relation_oids.contains(&row.inhrelid) || relation_oids.contains(&row.inhparent) {
            push_unique(&mut rows.inherits, row);
        }
    }
    for row in catcache.partitioned_table_rows() {
        if relation_oids.contains(&row.partrelid) {
            push_unique(&mut rows.partitioned_tables, row);
        }
    }
    for row in catcache.constraint_rows() {
        if relation_oids.contains(&row.conrelid)
            || relation_oids.contains(&row.conindid)
            || relation_oids.contains(&row.confrelid)
            || type_oids.contains(&row.contypid)
            || namespace_set.contains(&row.connamespace)
        {
            push_unique(&mut rows.constraints, row);
        }
    }
    for row in catcache.rewrite_rows() {
        if relation_oids.contains(&row.ev_class) {
            push_unique(&mut rows.rewrites, row);
        }
    }
    for row in catcache.trigger_rows() {
        if relation_oids.contains(&row.tgrelid)
            || relation_oids.contains(&row.tgconstrrelid)
            || relation_oids.contains(&row.tgconstrindid)
        {
            push_unique(&mut rows.triggers, row);
        }
    }
    for row in catcache.policy_rows() {
        if relation_oids.contains(&row.polrelid) {
            push_unique(&mut rows.policies, row);
        }
    }
    for row in catcache.publication_rel_rows() {
        if relation_oids.contains(&row.prrelid) {
            push_unique(&mut rows.publication_rels, row);
        }
    }
    for row in catcache.statistic_rows() {
        if relation_oids.contains(&row.starelid) {
            push_unique(&mut rows.statistics, row);
        }
    }
    for row in catcache.statistic_ext_rows() {
        if relation_oids.contains(&row.stxrelid) || namespace_set.contains(&row.stxnamespace) {
            push_unique(&mut rows.statistics_ext, row);
        }
    }
    let statistic_ext_oids = rows
        .statistics_ext
        .iter()
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    for row in catcache.statistic_ext_data_rows() {
        if statistic_ext_oids.contains(&row.stxoid) {
            push_unique(&mut rows.statistics_ext_data, row);
        }
    }
    for row in catcache.proc_rows() {
        if namespace_set.contains(&row.pronamespace)
            || type_oids.contains(&row.prorettype)
            || proc_argtype_oids(&row.proargtypes).any(|oid| type_oids.contains(&oid))
        {
            push_unique(&mut rows.procs, row);
        }
    }
    let proc_oids = rows
        .procs
        .iter()
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    for row in catcache.operator_rows() {
        if namespace_set.contains(&row.oprnamespace)
            || type_oids.contains(&row.oprleft)
            || type_oids.contains(&row.oprright)
            || proc_oids.contains(&row.oprcode)
            || proc_oids.contains(&row.oprrest)
            || proc_oids.contains(&row.oprjoin)
        {
            push_unique(&mut rows.operators, row);
        }
    }
    for row in catcache.cast_rows() {
        if type_oids.contains(&row.castsource)
            || type_oids.contains(&row.casttarget)
            || proc_oids.contains(&row.castfunc)
        {
            push_unique(&mut rows.casts, row);
        }
    }

    let mut object_keys = rows
        .classes
        .iter()
        .map(|row| (PG_CLASS_RELATION_OID, row.oid))
        .collect::<BTreeSet<_>>();
    object_keys.extend(rows.types.iter().map(|row| (PG_TYPE_RELATION_OID, row.oid)));
    object_keys.extend(
        rows.attrdefs
            .iter()
            .map(|row| (PG_ATTRDEF_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.constraints
            .iter()
            .map(|row| (PG_CONSTRAINT_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.rewrites
            .iter()
            .map(|row| (PG_REWRITE_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.triggers
            .iter()
            .map(|row| (PG_TRIGGER_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.policies
            .iter()
            .map(|row| (PG_POLICY_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.publication_rels
            .iter()
            .map(|row| (PG_PUBLICATION_REL_RELATION_OID, row.oid)),
    );
    object_keys.extend(
        rows.statistics_ext
            .iter()
            .map(|row| (PG_STATISTIC_EXT_RELATION_OID, row.oid)),
    );
    object_keys.extend(rows.procs.iter().map(|row| (PG_PROC_RELATION_OID, row.oid)));
    object_keys.extend(
        rows.operators
            .iter()
            .map(|row| (PG_OPERATOR_RELATION_OID, row.oid)),
    );
    object_keys.extend(rows.casts.iter().map(|row| (PG_CAST_RELATION_OID, row.oid)));

    for row in catcache.depend_rows() {
        if object_keys.contains(&(row.classid, row.objid))
            || object_keys.contains(&(row.refclassid, row.refobjid))
        {
            push_unique(&mut rows.depends, row);
        }
    }

    let mut effect = CatalogMutationEffect {
        touched_catalogs: temp_namespace_delete_kinds(),
        ..CatalogMutationEffect::default()
    };
    for row in &rows.classes {
        push_oid(&mut effect.relation_oids, row.oid);
        push_oid(&mut effect.namespace_oids, row.relnamespace);
        push_oid(&mut effect.type_oids, row.reltype);
        if row.relfilenode != 0 && relkind_has_storage(row.relkind) {
            push_unique(
                &mut effect.dropped_rels,
                RelFileLocator {
                    spc_oid: row.reltablespace,
                    db_oid: temp_db_oid,
                    rel_number: row.relfilenode,
                },
            );
        }
    }
    for row in &rows.types {
        push_oid(&mut effect.type_oids, row.oid);
        push_oid(&mut effect.namespace_oids, row.typnamespace);
    }
    for oid in namespace_oids {
        push_oid(&mut effect.namespace_oids, *oid);
    }

    (rows, effect)
}

fn temp_namespace_delete_kinds() -> Vec<BootstrapCatalogKind> {
    let mut kinds = drop_relation_delete_kinds();
    for kind in [
        BootstrapCatalogKind::PgProc,
        BootstrapCatalogKind::PgOperator,
        BootstrapCatalogKind::PgCast,
    ] {
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    kinds
}

impl Database {
    #[cfg(test)]
    pub(crate) fn temp_entry(
        &self,
        client_id: ClientId,
        table_name: &str,
    ) -> Option<RelCacheEntry> {
        let normalized = normalize_temp_lookup_name(table_name);
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| ns.tables.get(&normalized).map(|entry| entry.entry.clone()))
    }

    pub(super) fn temp_entry_on_commit(
        &self,
        client_id: ClientId,
        relation_oid: u32,
    ) -> Option<OnCommitAction> {
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| {
                ns.tables
                    .values()
                    .find(|entry| entry.entry.relation_oid == relation_oid)
                    .map(|entry| entry.on_commit)
            })
    }

    pub(super) fn install_temp_entry(
        &self,
        client_id: ClientId,
        table_name: &str,
        entry: RelCacheEntry,
        on_commit: OnCommitAction,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces
            .get_mut(&temp_backend_id)
            .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone())))?;
        namespace
            .tables
            .insert(normalized, TempCatalogEntry { entry, on_commit });
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn replace_temp_entry_desc(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        desc: crate::backend::executor::RelationDesc,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        entry.entry.desc = desc;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn replace_temp_entry_partition_metadata(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        relkind: char,
        relispartition: bool,
        relpartbound: Option<String>,
        partitioned_table: Option<crate::include::catalog::PgPartitionedTableRow>,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        entry.entry.relkind = relkind;
        entry.entry.relispartition = relispartition;
        entry.entry.relpartbound = relpartbound;
        entry.entry.partitioned_table = partitioned_table;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn replace_temp_entry_rel(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        rel: crate::backend::storage::smgr::RelFileLocator,
    ) -> Result<crate::backend::storage::smgr::RelFileLocator, ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        let old_rel = entry.entry.rel;
        entry.entry.rel = rel;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(old_rel)
    }

    pub(super) fn replace_temp_entry_index_readiness(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        indisready: bool,
        indisvalid: bool,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        let index = entry.entry.index.as_mut().ok_or_else(|| {
            ExecError::Parse(ParseError::WrongObjectType {
                name: relation_oid.to_string(),
                expected: "index",
            })
        })?;
        index.indisready = indisready;
        index.indisvalid = indisvalid;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn temp_relation_name_for_oid(
        &self,
        client_id: ClientId,
        relation_oid: u32,
    ) -> Option<String> {
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| {
                ns.tables.iter().find_map(|(name, entry)| {
                    (entry.entry.relation_oid == relation_oid).then(|| name.clone())
                })
            })
    }

    fn cleanup_stale_temp_relations_in_transaction(
        &self,
        client_id: ClientId,
        temp_backend_id: TempBackendId,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let namespace_oids = [
            Self::temp_namespace_oid(temp_backend_id),
            Self::temp_toast_namespace_oid(temp_backend_id),
        ];
        if self.remove_temp_dynamic_entries(namespace_oids[0]) {
            self.refresh_catalog_store_dynamic_type_rows(client_id, None);
        }
        let catcache = self
            .txn_backend_catcache(client_id, xid, *cid)
            .map_err(map_catalog_error)?;
        let (rows, effect) = temp_namespace_catalog_rows(
            &catcache,
            &namespace_oids,
            Self::temp_db_oid(temp_backend_id),
        );
        if rows.classes.is_empty()
            && rows.types.is_empty()
            && rows.procs.is_empty()
            && rows.operators.is_empty()
            && rows.casts.is_empty()
            && rows.depends.is_empty()
        {
            return Ok(());
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: *cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        delete_catalog_rows_subset_mvcc(
            &ctx,
            &rows,
            self.database_oid,
            &temp_namespace_delete_kinds(),
        )
        .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        *cid = (*cid).saturating_add(1);
        Ok(())
    }

    fn remove_temp_dynamic_entries(&self, namespace_oid: u32) -> bool {
        let mut removed_type_oids = BTreeSet::new();
        let mut changed = false;
        {
            let mut domains = self.domains.write();
            let before = domains.len();
            domains.retain(|_, domain| {
                if domain.namespace_oid == namespace_oid {
                    removed_type_oids.insert(domain.oid);
                    removed_type_oids.insert(domain.array_oid);
                    return false;
                }
                true
            });
            changed |= domains.len() != before;
        }
        {
            let mut enum_types = self.enum_types.write();
            let before = enum_types.len();
            enum_types.retain(|_, entry| {
                if entry.namespace_oid == namespace_oid {
                    removed_type_oids.insert(entry.oid);
                    removed_type_oids.insert(entry.array_oid);
                    return false;
                }
                true
            });
            changed |= enum_types.len() != before;
        }
        {
            let mut range_types = self.range_types.write();
            let before = range_types.len();
            range_types.retain(|_, entry| {
                if entry.namespace_oid == namespace_oid {
                    removed_type_oids.insert(entry.oid);
                    removed_type_oids.insert(entry.array_oid);
                    removed_type_oids.insert(entry.multirange_oid);
                    removed_type_oids.insert(entry.multirange_array_oid);
                    return false;
                }
                true
            });
            changed |= range_types.len() != before;
        }
        if !removed_type_oids.is_empty() {
            let mut base_types = self.base_types.write();
            let before = base_types.len();
            base_types.retain(|oid, entry| {
                !removed_type_oids.contains(oid) && !removed_type_oids.contains(&entry.array_oid)
            });
            changed |= base_types.len() != before;
        }
        {
            let mut conversions = self.conversions.write();
            let before = conversions.len();
            conversions.retain(|_, entry| entry.namespace_oid != namespace_oid);
            changed |= conversions.len() != before;
        }
        {
            let mut statistics_objects = self.statistics_objects.write();
            let before = statistics_objects.len();
            statistics_objects.retain(|_, entry| entry.namespace_oid != namespace_oid);
            changed |= statistics_objects.len() != before;
        }
        changed
    }

    pub(crate) fn ensure_temp_namespace(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<TempNamespace, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let temp_backend_id = self.temp_backend_id(client_id);
        if let Some(namespace) = self.owned_temp_namespace(client_id) {
            temp_effects.push(TempMutationEffect::TouchNamespace {
                name: namespace.name.clone(),
            });
            return Ok(namespace);
        }

        let temp_oid = Self::temp_namespace_oid(temp_backend_id);
        let temp_name = Self::temp_namespace_name(temp_backend_id);
        let temp_toast_oid = Self::temp_toast_namespace_oid(temp_backend_id);
        let temp_toast_name = Self::temp_toast_namespace_name(temp_backend_id);
        let catcache = self
            .txn_backend_catcache(client_id, xid, *cid)
            .map_err(map_catalog_error)?;
        let existing_namespace = catcache.namespace_by_oid(temp_oid).cloned();
        let existing_toast_namespace = catcache.namespace_by_oid(temp_toast_oid).cloned();

        if existing_namespace.is_some() || existing_toast_namespace.is_some() {
            self.cleanup_stale_temp_relations_in_transaction(
                client_id,
                temp_backend_id,
                xid,
                cid,
                catalog_effects,
            )?;
        }

        let namespace = TempNamespace {
            oid: temp_oid,
            name: temp_name,
            owner_oid: existing_namespace
                .as_ref()
                .or(existing_toast_namespace.as_ref())
                .map(|row| row.nspowner)
                .unwrap_or_else(|| self.auth_state(client_id).current_user_oid()),
            toast_oid: temp_toast_oid,
            toast_name: temp_toast_name,
            tables: BTreeMap::new(),
            generation: 0,
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: *cid,
            client_id,
            waiter: None,
            interrupts,
        };
        if existing_namespace.is_none() {
            let effect = self
                .catalog
                .write()
                .create_namespace_mvcc(namespace.oid, &namespace.name, namespace.owner_oid, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        if existing_toast_namespace.is_none() {
            let effect = self
                .catalog
                .write()
                .create_namespace_mvcc(
                    namespace.toast_oid,
                    &namespace.toast_name,
                    namespace.owner_oid,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        {
            let mut namespaces = self.temp_relations.write();
            namespaces.insert(temp_backend_id, namespace.clone());
        }
        temp_effects.push(TempMutationEffect::Create {
            name: namespace.name.clone(),
            entry: RelCacheEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: crate::include::catalog::BootstrapCatalogKind::PgNamespace
                        .relation_oid(),
                },
                relation_oid: namespace.oid,
                namespace_oid: namespace.oid,
                owner_oid: namespace.owner_oid,
                of_type_oid: 0,
                row_type_oid: 0,
                array_type_oid: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relpersistence: 't',
                relkind: 'n',
                relispopulated: true,
                relhastriggers: false,
                relispartition: false,
                relpartbound: None,
                relrowsecurity: false,
                relforcerowsecurity: false,
                desc: crate::backend::executor::RelationDesc {
                    columns: Vec::new(),
                },
                partitioned_table: None,
                partition_spec: None,
                index: None,
            },
            on_commit: OnCommitAction::PreserveRows,
            namespace_created: true,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(namespace)
    }

    pub(crate) fn record_temp_namespace_touch(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) {
        if let Some(namespace) = self.owned_temp_namespace(client_id)
            && namespace.oid == namespace_oid
        {
            temp_effects.push(TempMutationEffect::TouchNamespace {
                name: namespace.name,
            });
        }
    }

    pub(super) fn create_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: String,
        desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<CreatedTempRelation, ExecError> {
        self.create_temp_relation_with_relkind_in_transaction(
            client_id,
            table_name,
            desc,
            on_commit,
            xid,
            cid,
            'r',
            0,
            None,
            catalog_effects,
            temp_effects,
        )
    }

    pub(super) fn create_temp_relation_with_relkind_in_transaction(
        &self,
        client_id: ClientId,
        table_name: String,
        desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
        xid: TransactionId,
        mut cid: CommandId,
        relkind: char,
        of_type_oid: u32,
        reloptions: Option<Vec<String>>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<CreatedTempRelation, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(&table_name);
        let namespace =
            self.ensure_temp_namespace(client_id, xid, &mut cid, catalog_effects, temp_effects)?;
        if namespace.tables.contains_key(&normalized) {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(normalized)));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let (created, effect) = if relkind == 'r' {
            self.catalog
                .write()
                .create_typed_table_mvcc_with_options(
                    format!("{}.{}", namespace.name, normalized),
                    desc,
                    namespace.oid,
                    Self::temp_db_oid(temp_backend_id),
                    't',
                    namespace.toast_oid,
                    &namespace.toast_name,
                    self.auth_state(client_id).current_user_oid(),
                    of_type_oid,
                    reloptions.clone(),
                    &ctx,
                )
                .map_err(map_catalog_error)?
        } else {
            let (entry, effect) = self
                .catalog
                .write()
                .create_relation_mvcc_with_relkind(
                    format!("{}.{}", namespace.name, normalized),
                    desc,
                    namespace.oid,
                    Self::temp_db_oid(temp_backend_id),
                    't',
                    relkind,
                    self.auth_state(client_id).current_user_oid(),
                    reloptions,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            (
                crate::backend::catalog::store::CreateTableResult { entry, toast: None },
                effect,
            )
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let rel_entry = RelCacheEntry {
            rel: created.entry.rel,
            relation_oid: created.entry.relation_oid,
            namespace_oid: created.entry.namespace_oid,
            owner_oid: created.entry.owner_oid,
            of_type_oid: created.entry.of_type_oid,
            row_type_oid: created.entry.row_type_oid,
            array_type_oid: created.entry.array_type_oid,
            reltoastrelid: created.entry.reltoastrelid,
            relhasindex: false,
            relpersistence: created.entry.relpersistence,
            relkind: created.entry.relkind,
            relispopulated: created.entry.relispopulated,
            relhastriggers: created.entry.relhastriggers,
            relispartition: created.entry.relispartition,
            relpartbound: created.entry.relpartbound.clone(),
            relrowsecurity: created.entry.relrowsecurity,
            relforcerowsecurity: created.entry.relforcerowsecurity,
            desc: created.entry.desc.clone(),
            partitioned_table: created.entry.partitioned_table.clone(),
            partition_spec: None,
            index: None,
        };
        {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.tables.insert(
                normalized.clone(),
                TempCatalogEntry {
                    entry: rel_entry.clone(),
                    on_commit,
                },
            );
            namespace.generation = namespace.generation.saturating_add(1);
        }
        temp_effects.push(TempMutationEffect::Create {
            name: normalized,
            entry: rel_entry.clone(),
            on_commit,
            namespace_created: false,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(CreatedTempRelation {
            entry: rel_entry,
            toast: created.toast,
        })
    }

    pub(crate) fn drop_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let removed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            let removed = namespace.tables.remove(&normalized).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.generation = namespace.generation.saturating_add(1);
            removed
        };
        let mut next_cid = cid;
        self.drop_statistics_for_relation_in_transaction(
            client_id,
            removed.entry.relation_oid,
            xid,
            &mut next_cid,
            catalog_effects,
        )?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: next_cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts,
        };
        let visible_type_rows = self
            .lazy_catalog_lookup(client_id, Some((xid, next_cid)), None)
            .type_rows();
        let (dropped_entries, effect) = self
            .catalog
            .write()
            .drop_relation_by_oid_mvcc_with_extra_type_rows(
                removed.entry.relation_oid,
                &ctx,
                &visible_type_rows,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        temp_effects.push(TempMutationEffect::Drop {
            name: normalized,
            entry: removed.entry.clone(),
            on_commit: removed.on_commit,
        });
        let mut cascaded_temp_drops = Vec::new();
        {
            let mut namespaces = self.temp_relations.write();
            if let Some(namespace) = namespaces.get_mut(&temp_backend_id) {
                for dropped_entry in &dropped_entries {
                    if dropped_entry.relation_oid == removed.entry.relation_oid {
                        continue;
                    }
                    let Some(name) = namespace.tables.iter().find_map(|(name, temp_entry)| {
                        (temp_entry.entry.relation_oid == dropped_entry.relation_oid)
                            .then(|| name.clone())
                    }) else {
                        continue;
                    };
                    if let Some(temp_entry) = namespace.tables.remove(&name) {
                        cascaded_temp_drops.push((name, temp_entry));
                    }
                }
                if !cascaded_temp_drops.is_empty() {
                    namespace.generation = namespace.generation.saturating_add(1);
                }
            }
        }
        temp_effects.extend(cascaded_temp_drops.into_iter().map(|(name, temp_entry)| {
            TempMutationEffect::Drop {
                name,
                entry: temp_entry.entry,
                on_commit: temp_entry.on_commit,
            }
        }));
        self.invalidate_backend_cache_state(client_id);
        Ok(removed.entry)
    }

    pub(super) fn remove_temp_entry_after_catalog_drop(
        &self,
        client_id: ClientId,
        table_name: &str,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let removed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            let removed = namespace.tables.remove(&normalized).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.generation = namespace.generation.saturating_add(1);
            removed
        };
        temp_effects.push(TempMutationEffect::Drop {
            name: normalized,
            entry: removed.entry.clone(),
            on_commit: removed.on_commit,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(removed.entry)
    }

    pub(crate) fn rename_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        new_table_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let normalized_new = normalize_temp_lookup_name(new_table_name);
        let temp_backend_id = self.temp_backend_id(client_id);
        let old_name = {
            let namespaces = self.temp_relations.read();
            let namespace = namespaces.get(&temp_backend_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized_new.clone()))
            })?;
            namespace
                .tables
                .iter()
                .find_map(|(name, entry)| {
                    (entry.entry.relation_oid == relation_oid).then(|| name.clone())
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
                })?
        };

        if old_name != normalized_new {
            let namespaces = self.temp_relations.read();
            let namespace = namespaces
                .get(&temp_backend_id)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            if namespace.tables.contains_key(&normalized_new) {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(
                    normalized_new.clone(),
                )));
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let visible_type_rows = self
            .lazy_catalog_lookup(client_id, Some((xid, cid)), None)
            .type_rows();
        let effect = self
            .catalog
            .write()
            .rename_relation_mvcc(relation_oid, &normalized_new, &visible_type_rows, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);

        let renamed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces
                .get_mut(&temp_backend_id)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            let entry = namespace
                .tables
                .remove(&old_name)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            let rel_entry = entry.entry.clone();
            namespace.tables.insert(normalized_new.clone(), entry);
            namespace.generation = namespace.generation.saturating_add(1);
            rel_entry
        };
        temp_effects.push(TempMutationEffect::Rename {
            old_name,
            new_name: normalized_new,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(renamed)
    }

    pub(crate) fn apply_temp_on_commit(&self, client_id: ClientId) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut to_delete = Vec::new();
        let mut to_drop = Vec::new();
        {
            let namespaces = self.temp_relations.read();
            if let Some(namespace) = namespaces.get(&temp_backend_id) {
                let catalog = self.lazy_catalog_lookup(client_id, None, None);
                let mut drop_seen = BTreeSet::new();
                for entry in namespace.tables.values() {
                    match entry.on_commit {
                        OnCommitAction::PreserveRows => {}
                        OnCommitAction::DeleteRows => {
                            if !matches!(entry.entry.relkind, 'p' | 'I') {
                                to_delete.push(entry.entry.rel);
                            }
                        }
                        OnCommitAction::Drop => collect_temp_on_commit_drop_names(
                            &catalog,
                            namespace,
                            entry.entry.relation_oid,
                            &mut drop_seen,
                            &mut to_drop,
                        ),
                    }
                }
            }
        }

        for rel in to_delete {
            let _ = self.pool.invalidate_relation(rel);
            self.pool
                .with_storage_mut(|s| {
                    s.smgr
                        .truncate(rel, crate::backend::storage::smgr::ForkNumber::Main, 0)?;
                    if s.smgr.exists(
                        rel,
                        crate::backend::storage::smgr::ForkNumber::VisibilityMap,
                    ) {
                        s.smgr.truncate(
                            rel,
                            crate::backend::storage::smgr::ForkNumber::VisibilityMap,
                            0,
                        )?;
                    }
                    Ok(())
                })
                .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
        }

        for name in to_drop {
            let normalized = normalize_temp_lookup_name(&name);
            let still_tracked = self
                .temp_relations
                .read()
                .get(&self.temp_backend_id(client_id))
                .is_some_and(|namespace| namespace.tables.contains_key(&normalized));
            if !still_tracked {
                continue;
            }
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            let result = self.drop_temp_relation_in_transaction(
                client_id,
                &name,
                xid,
                0,
                &mut catalog_effects,
                &mut temp_effects,
            );
            let result = self.finish_txn(
                client_id,
                xid,
                result.map(|_| StatementResult::AffectedRows(0)),
                &catalog_effects,
                &temp_effects,
                &[],
            );
            guard.disarm();
            match result {
                Ok(_) => {}
                Err(ExecError::Parse(ParseError::TableDoesNotExist(_))) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn cleanup_client_temp_relations_once(
        &self,
        client_id: ClientId,
        temp_backend_id: TempBackendId,
    ) -> Result<(), ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut cid = 0;
        let mut effects = Vec::new();
        let result = self.cleanup_stale_temp_relations_in_transaction(
            client_id,
            temp_backend_id,
            xid,
            &mut cid,
            &mut effects,
        );
        let result = self.finish_txn(
            client_id,
            xid,
            result.map(|_| StatementResult::AffectedRows(0)),
            &effects,
            &[],
            &[],
        );
        guard.disarm();
        result.map(|_| ())
    }

    pub(crate) fn cleanup_client_temp_relations(
        &self,
        client_id: ClientId,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let Some(_namespace) = self.owned_temp_namespace(client_id) else {
            self.temp_relations.write().remove(&temp_backend_id);
            return Ok(());
        };
        let result = self.cleanup_client_temp_relations_once(client_id, temp_backend_id);
        self.temp_relations.write().remove(&temp_backend_id);
        self.invalidate_backend_cache_state(client_id);
        result
    }
}
