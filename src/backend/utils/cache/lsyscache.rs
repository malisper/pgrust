use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::cache::catcache::normalize_catalog_name;
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    backend_catcache, backend_relcache, ensure_attribute_rows, ensure_class_rows,
    ensure_constraint_rows, ensure_inherit_rows, ensure_namespace_rows, ensure_rewrite_rows,
    ensure_statistic_rows, ensure_type_rows,
};
use crate::backend::utils::cache::system_views::{build_pg_stats_rows, build_pg_views_rows};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    PgAmRow, PgAmopRow, PgAmprocRow, PgClassRow, PgCollationRow, PgConstraintRow, PgIndexRow,
    PgInheritsRow, PgLanguageRow, PgOpclassRow, PgOpfamilyRow, PgProcRow, PgRewriteRow,
    PgStatisticRow, PgTypeRow,
};
use crate::include::nodes::datum::Value;
use crate::pgrust::database::{Database, TempNamespace};

fn namespace_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .namespace_by_name(name)
        .cloned()
}

fn namespace_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .namespace_by_oid(oid)
        .cloned()
}

fn class_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .class_by_oid(oid)
        .cloned()
}

fn class_row_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relname: &str,
    namespace_oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .class_rows()
        .into_iter()
        .find(|row| {
            row.relnamespace == namespace_oid
                && row.relname.eq_ignore_ascii_case(relname)
                && !db.other_session_temp_namespace_oid(client_id, row.relnamespace)
        })
}

fn attribute_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttributeRow> {
    let mut rows = backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .attributes_by_relid(relation_oid)
                .unwrap_or(&[])
                .to_vec()
        })
        .unwrap_or_default();
    rows.sort_by_key(|row| row.attnum);
    rows
}

fn attrdef_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttrdefRow> {
    let mut rows: Vec<_> = backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .attrdef_rows()
                .into_iter()
                .filter(|row| row.adrelid == relation_oid)
                .collect()
        })
        .unwrap_or_default();
    rows.sort_by_key(|row| row.adnum);
    rows
}

fn type_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .type_by_oid(oid)
        .cloned()
}

fn visible_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Option<crate::backend::utils::cache::catcache::CatCache> {
    backend_catcache(db, client_id, txn_ctx).ok()
}

fn proc_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgProcRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .proc_by_oid(oid)
        .cloned()
}

fn proc_rows_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Vec<PgProcRow> {
    visible_catcache(db, client_id, txn_ctx)
        .into_iter()
        .flat_map(|catcache| {
            catcache
                .proc_rows_by_name(name)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn language_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgLanguageRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .language_rows()
        .into_iter()
        .find(|row| row.oid == oid)
}

fn language_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgLanguageRow> {
    visible_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.language_rows())
        .unwrap_or_default()
}

fn language_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<PgLanguageRow> {
    let normalized = normalize_catalog_name(name);
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .language_rows()
        .into_iter()
        .find(|row| row.lanname.eq_ignore_ascii_case(normalized))
}

fn opclass_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgOpclassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == oid)
}

fn opclass_rows_for_am(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Vec<PgOpclassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .opclass_rows()
                .into_iter()
                .filter(|row| row.opcmethod == am_oid)
                .collect()
        })
        .unwrap_or_default()
}

pub struct LazyCatalogLookup<'a> {
    pub db: &'a Database,
    pub client_id: ClientId,
    pub txn_ctx: Option<(TransactionId, CommandId)>,
    pub search_path: Vec<String>,
}

fn owned_temp_namespace(db: &Database, client_id: ClientId) -> Option<TempNamespace> {
    db.temp_relations.read().get(&client_id).cloned()
}

fn namespace_oid_for_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<u32> {
    namespace_row_by_name(db, client_id, txn_ctx, name).map(|row| row.oid)
}

fn type_for_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    type_row_by_oid(db, client_id, txn_ctx, oid)
}

pub fn access_method_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    amname: &str,
) -> Option<PgAmRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .am_rows()
        .into_iter()
        .find(|row| row.amname.eq_ignore_ascii_case(amname))
}

pub fn access_method_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Option<PgAmRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .am_rows()
        .into_iter()
        .find(|row| row.oid == am_oid)
}

pub fn default_opclass_for_am_and_type(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
    input_type_oid: u32,
) -> Option<PgOpclassRow> {
    opclass_rows_for_am(db, client_id, txn_ctx, am_oid)
        .into_iter()
        .find(|row| row.opcmethod == am_oid && row.opcdefault && row.opcintype == input_type_oid)
}

pub fn opfamily_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Option<PgOpfamilyRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .opfamily_rows()
        .into_iter()
        .find(|row| row.oid == family_oid)
}

pub fn collation_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    collation_oid: u32,
) -> Option<PgCollationRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == collation_oid)
}

pub fn amop_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmopRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .amop_rows()
                .into_iter()
                .filter(|row| row.amopfamily == family_oid)
                .collect()
        })
        .unwrap_or_default()
}

pub fn amproc_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmprocRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .amproc_rows()
                .into_iter()
                .filter(|row| row.amprocfamily == family_oid)
                .collect()
        })
        .unwrap_or_default()
}

pub fn index_row_by_indexrelid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<PgIndexRow> {
    backend_catcache(db, client_id, txn_ctx)
        .ok()?
        .index_rows()
        .into_iter()
        .find(|row| row.indexrelid == relation_oid)
}

pub fn index_relation_oids_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| {
            catcache
                .index_rows()
                .into_iter()
                .filter(|row| row.indrelid == relation_oid)
                .map(|row| row.indexrelid)
                .collect()
        })
        .unwrap_or_default()
}

pub fn relation_entry_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<RelCacheEntry> {
    if let Some(entry) = db
        .temp_relations
        .read()
        .get(&client_id)
        .and_then(|namespace| {
            namespace
                .tables
                .values()
                .find(|temp| temp.entry.relation_oid == relation_oid)
                .map(|temp| temp.entry.clone())
        })
    {
        return Some(entry);
    }

    let entry = backend_relcache(db, client_id, txn_ctx)
        .ok()?
        .get_by_oid(relation_oid)
        .cloned()?;
    (!db.other_session_temp_namespace_oid(client_id, entry.namespace_oid)).then_some(entry)
}

fn toast_relation_from_entry(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    entry: &RelCacheEntry,
) -> Option<crate::include::nodes::primnodes::ToastRelationRef> {
    let toast_oid = entry.reltoastrelid;
    (toast_oid != 0)
        .then(|| relation_entry_by_oid(db, client_id, txn_ctx, toast_oid))
        .flatten()
        .map(|toast| crate::include::nodes::primnodes::ToastRelationRef {
            rel: toast.rel,
            relation_oid: toast.relation_oid,
        })
}

pub fn lookup_any_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> Option<BoundRelation> {
    let normalized = normalize_catalog_name(name).to_ascii_lowercase();
    if let Some((schema, relname)) = normalized.split_once('.') {
        let schema_name = if schema == "pg_temp" {
            owned_temp_namespace(db, client_id)?.name
        } else {
            schema.to_string()
        };
        let mut relcache = backend_relcache(db, client_id, txn_ctx).ok()?;
        if let Some(temp_namespace) = owned_temp_namespace(db, client_id) {
            for (temp_name, entry) in temp_namespace.tables {
                relcache.insert(temp_name.clone(), entry.entry.clone());
                relcache.insert(
                    format!("{}.{}", temp_namespace.name, temp_name),
                    entry.entry,
                );
            }
        }
        let entry = relcache
            .get_by_name(&format!("{schema_name}.{relname}"))
            .filter(|entry| !db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))?
            .clone();
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        });
    }

    if let Some(temp) = db
        .temp_relations
        .read()
        .get(&client_id)
        .and_then(|namespace| {
            namespace
                .tables
                .get(&normalized)
                .map(|entry| entry.entry.clone())
        })
    {
        return Some(BoundRelation {
            rel: temp.rel,
            relation_oid: temp.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &temp),
            namespace_oid: temp.namespace_oid,
            owner_oid: temp.owner_oid,
            relpersistence: temp.relpersistence,
            relkind: temp.relkind,
            desc: temp.desc.clone(),
        });
    }

    let mut relcache = backend_relcache(db, client_id, txn_ctx).ok()?;
    if let Some(temp_namespace) = owned_temp_namespace(db, client_id) {
        for (temp_name, entry) in temp_namespace.tables {
            relcache.insert(temp_name.clone(), entry.entry.clone());
            relcache.insert(
                format!("{}.{}", temp_namespace.name, temp_name),
                entry.entry,
            );
        }
    }
    let relcache = relcache.with_search_path(search_path);
    if let Some(entry) = relcache
        .get_by_name(&normalized)
        .filter(|entry| !db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))
    {
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        });
    }

    None
}

pub fn describe_relation_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<RelCacheEntry> {
    relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)
}

pub fn relation_namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<String> {
    let entry = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)?;
    namespace_row_by_oid(db, client_id, txn_ctx, entry.namespace_oid).map(|row| row.nspname)
}

pub fn relation_display_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    configured_search_path: Option<&[String]>,
    relation_oid: u32,
) -> Option<String> {
    let entry = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)?;
    let class = class_row_by_oid(db, client_id, txn_ctx, relation_oid)?;
    let namespace = relation_namespace_name(db, client_id, txn_ctx, relation_oid)?;
    if namespace.starts_with("pg_temp_") {
        return Some(format!("{namespace}.{}", class.relname));
    }
    let search_path = db.effective_search_path(client_id, configured_search_path);
    let first_match = search_path
        .iter()
        .find_map(|schema| {
            let namespace_oid = namespace_oid_for_name(db, client_id, txn_ctx, schema)?;
            let row =
                class_row_by_name_namespace(db, client_id, txn_ctx, &class.relname, namespace_oid)?;
            let visible_entry = relation_entry_by_oid(db, client_id, txn_ctx, row.oid)?;
            Some((row, visible_entry))
        })
        .and_then(|(row, visible_entry)| {
            visible_entry
                .relkind
                .eq(&entry.relkind)
                .then_some(())
                .map(|_| row.relnamespace)
        });
    if let Some(visible_namespace_oid) = first_match
        && visible_namespace_oid == entry.namespace_oid
    {
        Some(class.relname)
    } else {
        Some(format!("{namespace}.{}", class.relname))
    }
}

pub fn has_index_on_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> bool {
    !index_relation_oids_for_heap(db, client_id, txn_ctx, relation_oid).is_empty()
}

pub fn access_method_name_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<String> {
    let class = class_row_by_oid(db, client_id, txn_ctx, relation_oid)?;
    access_method_row_by_oid(db, client_id, txn_ctx, class.relam)
        .map(|row| row.amname)
        .or_else(|| match class.relkind {
            'r' => Some("heap".to_string()),
            'i' => Some("btree".to_string()),
            _ => None,
        })
}

pub fn constraint_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgConstraintRow> {
    if let Ok(catcache) = backend_catcache(db, client_id, txn_ctx) {
        let rows = catcache.constraint_rows_for_relation(relation_oid);
        if !rows.is_empty() {
            return rows;
        }
    }
    if let Ok(relcache) = backend_relcache(db, client_id, txn_ctx)
        && let Some(entry) = relcache.get_by_oid(relation_oid)
        && let Some(class) = class_row_by_oid(db, client_id, txn_ctx, relation_oid)
    {
        return derived_pg_constraint_rows(
            relation_oid,
            &class.relname,
            entry.namespace_oid,
            &entry.desc,
        );
    }
    let constraint_rows = ensure_constraint_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| row.conrelid == relation_oid)
        .collect::<Vec<_>>();
    if !constraint_rows.is_empty() {
        return constraint_rows;
    }
    let Some(class) = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == relation_oid)
    else {
        return Vec::new();
    };
    let Some(entry) = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid) else {
        return Vec::new();
    };
    derived_pg_constraint_rows(
        relation_oid,
        &class.relname,
        entry.namespace_oid,
        &entry.desc,
    )
}

impl CatalogLookup for LazyCatalogLookup<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        lookup_any_relation(
            self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        )
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.relation_by_oid(relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        let entry = relation_entry_by_oid(self.db, self.client_id, self.txn_ctx, relation_oid)?;
        Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(self.db, self.client_id, self.txn_ctx, &entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        })
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        constraint_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        ensure_constraint_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        proc_rows_by_name(self.db, self.client_id, self.txn_ctx, name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        proc_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = ensure_type_rows(self.db, self.client_id, self.txn_ctx);
        rows.extend(self.db.domain_type_rows_for_search_path(&self.search_path));
        rows
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        language_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        language_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        language_row_by_name(self.db, self.client_id, self.txn_ctx, name)
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        ensure_rewrite_rows(self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|row| row.ev_class == relation_oid)
            .collect()
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        class_row_by_oid(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        ensure_inherit_rows(self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|row| row.inhrelid == relation_oid)
            .collect()
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        ensure_inherit_rows(self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|row| row.inhparent == relation_oid)
            .collect()
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        ensure_statistic_rows(self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|row| row.starelid == relation_oid)
            .collect()
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_views_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stats_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_attribute_rows(self.db, self.client_id, self.txn_ctx),
            ensure_statistic_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        self.db.pg_stat_activity_rows()
    }

    fn index_relations_for_heap(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::backend::parser::BoundIndexRelation> {
        index_relation_oids_for_heap(self.db, self.client_id, self.txn_ctx, relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry =
                    relation_entry_by_oid(self.db, self.client_id, self.txn_ctx, index_oid)?;
                let index_meta = entry.index.as_ref()?.clone();
                let class =
                    class_row_by_oid(self.db, self.client_id, self.txn_ctx, entry.relation_oid)?;
                Some(crate::backend::parser::BoundIndexRelation {
                    name: class.relname,
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc,
                    index_meta,
                })
            })
            .collect()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        let catcache = visible_catcache(self.db, self.client_id, self.txn_ctx)?;
        let mut relcache = backend_relcache(self.db, self.client_id, self.txn_ctx).ok()?;
        if let Some(temp_namespace) = owned_temp_namespace(self.db, self.client_id) {
            for (name, entry) in temp_namespace.tables {
                relcache.insert(name.clone(), entry.entry.clone());
                relcache.insert(format!("{}.{}", temp_namespace.name, name), entry.entry);
            }
        }
        Some(VisibleCatalog::new(
            relcache.with_search_path(&self.search_path),
            Some(catcache),
        ))
    }
}
