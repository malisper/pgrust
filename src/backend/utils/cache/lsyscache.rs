use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup, SqlType};
use crate::backend::utils::cache::relcache::{IndexRelCacheEntry, RelCacheEntry};
use crate::backend::utils::cache::syscache::{
    ensure_am_rows, ensure_amop_rows, ensure_amproc_rows, ensure_attrdef_rows,
    ensure_attribute_rows, ensure_class_rows, ensure_collation_rows, ensure_index_rows,
    ensure_namespace_rows, ensure_opclass_rows, ensure_opfamily_rows, ensure_type_rows,
};
use crate::include::catalog::{
    PgAmRow, PgAmopRow, PgAmprocRow, PgCollationRow, PgConstraintRow, PgIndexRow, PgOpclassRow,
    PgOpfamilyRow, PgTypeRow,
};
use crate::pgrust::database::{Database, TempNamespace};
use crate::{RelFileLocator, backend::utils::cache::catcache::normalize_catalog_name};

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
    let normalized = name.to_ascii_lowercase();
    ensure_namespace_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.nspname.eq_ignore_ascii_case(&normalized))
        .map(|row| row.oid)
}

fn type_for_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    ensure_type_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == oid)
}

pub fn access_method_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    amname: &str,
) -> Option<PgAmRow> {
    ensure_am_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.amname.eq_ignore_ascii_case(amname))
}

pub fn access_method_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Option<PgAmRow> {
    ensure_am_rows(db, client_id, txn_ctx)
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
    ensure_opclass_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.opcmethod == am_oid && row.opcdefault && row.opcintype == input_type_oid)
}

pub fn opfamily_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Option<PgOpfamilyRow> {
    ensure_opfamily_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == family_oid)
}

pub fn collation_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    collation_oid: u32,
) -> Option<PgCollationRow> {
    ensure_collation_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == collation_oid)
}

pub fn amop_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmopRow> {
    ensure_amop_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| row.amopfamily == family_oid)
        .collect()
}

pub fn amproc_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmprocRow> {
    ensure_amproc_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| row.amprocfamily == family_oid)
        .collect()
}

pub fn index_row_by_indexrelid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<PgIndexRow> {
    ensure_index_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.indexrelid == relation_oid)
}

pub fn index_relation_oids_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    ensure_index_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| row.indrelid == relation_oid)
        .map(|row| row.indexrelid)
        .collect()
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

    if let Some(entry) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.relation_entries_by_oid.get(&relation_oid).cloned())
    {
        return Some(entry);
    }

    let class = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == relation_oid)?;
    if db.other_session_temp_namespace_oid(client_id, class.relnamespace) {
        return None;
    }

    let attrdefs = ensure_attrdef_rows(db, client_id, txn_ctx);
    let columns = ensure_attribute_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|attr| attr.attrelid == relation_oid)
        .map(|attr| {
            let sql_type = type_for_oid(db, client_id, txn_ctx, attr.atttypid)?.sql_type;
            let mut desc = column_desc(
                attr.attname.clone(),
                SqlType {
                    typmod: attr.atttypmod,
                    ..sql_type
                },
                !attr.attnotnull,
            );
            if let Some(attrdef) = attrdefs
                .iter()
                .find(|attrdef| attrdef.adrelid == relation_oid && attrdef.adnum == attr.attnum)
            {
                desc.attrdef_oid = Some(attrdef.oid);
                desc.default_expr = Some(attrdef.adbin.clone());
            }
            Some(desc)
        })
        .collect::<Option<Vec<_>>>()?;

    let index = if class.relkind == 'i' {
        index_row_by_indexrelid(db, client_id, txn_ctx, relation_oid).map(|index_row| {
            let am_row = access_method_row_by_oid(db, client_id, txn_ctx, class.relam);
            let opclass_rows = ensure_opclass_rows(db, client_id, txn_ctx);
            let indclass = index_row.indclass.clone();
            let resolved_opclasses = indclass
                .iter()
                .filter_map(|oid| opclass_rows.iter().find(|row| row.oid == *oid))
                .collect::<Vec<_>>();
            IndexRelCacheEntry {
                indrelid: index_row.indrelid,
                indnatts: index_row.indnatts,
                indnkeyatts: index_row.indnkeyatts,
                indisunique: index_row.indisunique,
                indnullsnotdistinct: index_row.indnullsnotdistinct,
                indisprimary: index_row.indisprimary,
                indisexclusion: index_row.indisexclusion,
                indimmediate: index_row.indimmediate,
                indisclustered: index_row.indisclustered,
                indisvalid: index_row.indisvalid,
                indcheckxmin: index_row.indcheckxmin,
                indisready: index_row.indisready,
                indislive: index_row.indislive,
                indisreplident: index_row.indisreplident,
                am_oid: class.relam,
                am_handler_oid: am_row.as_ref().map(|row| row.amhandler),
                indkey: index_row.indkey.clone(),
                indclass,
                indcollation: index_row.indcollation.clone(),
                indoption: index_row.indoption.clone(),
                opfamily_oids: resolved_opclasses.iter().map(|row| row.opcfamily).collect(),
                opcintype_oids: resolved_opclasses.iter().map(|row| row.opcintype).collect(),
                indexprs: index_row.indexprs.clone(),
                indpred: index_row.indpred.clone(),
            }
        })
    } else {
        None
    };

    let entry = RelCacheEntry {
        rel: RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: class.relfilenode,
        },
        relation_oid: class.oid,
        namespace_oid: class.relnamespace,
        row_type_oid: class.reltype,
        relpersistence: class.relpersistence,
        relkind: class.relkind,
        desc: crate::backend::executor::RelationDesc { columns },
        index,
    };

    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .relation_entries_by_oid
        .insert(relation_oid, entry.clone());
    Some(entry)
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
        let namespace_oid = namespace_oid_for_name(db, client_id, txn_ctx, &schema_name)?;
        let class = ensure_class_rows(db, client_id, txn_ctx)
            .into_iter()
            .find(|row| {
                row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(relname)
            })?;
        let entry = relation_entry_by_oid(db, client_id, txn_ctx, class.oid)?;
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
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
            namespace_oid: temp.namespace_oid,
            relpersistence: temp.relpersistence,
            relkind: temp.relkind,
            desc: temp.desc.clone(),
        });
    }

    for schema in search_path {
        let Some(namespace_oid) = namespace_oid_for_name(db, client_id, txn_ctx, schema) else {
            continue;
        };
        let Some(class) = ensure_class_rows(db, client_id, txn_ctx)
            .into_iter()
            .find(|row| {
                row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(&normalized)
            })
        else {
            continue;
        };
        let Some(entry) = relation_entry_by_oid(db, client_id, txn_ctx, class.oid) else {
            continue;
        };
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
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
    ensure_namespace_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == entry.namespace_oid)
        .map(|row| row.nspname)
}

pub fn relation_display_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    configured_search_path: Option<&[String]>,
    relation_oid: u32,
) -> Option<String> {
    let entry = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)?;
    let class = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == relation_oid)?;
    let namespace = relation_namespace_name(db, client_id, txn_ctx, relation_oid)?;
    let search_path = db.effective_search_path(client_id, configured_search_path);
    let first_match = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| row.relname.eq_ignore_ascii_case(&class.relname))
        .filter_map(|row| relation_entry_by_oid(db, client_id, txn_ctx, row.oid).map(|e| (row, e)))
        .find_map(|(row, visible_entry)| {
            visible_entry
                .relkind
                .eq(&entry.relkind)
                .then_some(())
                .and_then(|_| {
                    search_path.iter().position(|schema| {
                        namespace_oid_for_name(db, client_id, txn_ctx, schema)
                            == Some(visible_entry.namespace_oid)
                    })
                })
                .map(|position| (position, row.relnamespace))
        });
    if let Some((_, visible_namespace_oid)) = first_match
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
    ensure_index_rows(db, client_id, txn_ctx)
        .into_iter()
        .any(|row| row.indrelid == relation_oid)
}

pub fn access_method_name_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<String> {
    let class = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == relation_oid)?;
    ensure_am_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.oid == class.relam)
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

    fn type_rows(&self) -> Vec<PgTypeRow> {
        ensure_type_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<crate::backend::parser::BoundIndexRelation> {
        index_relation_oids_for_heap(self.db, self.client_id, self.txn_ctx, relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry = relation_entry_by_oid(self.db, self.client_id, self.txn_ctx, index_oid)?;
                let index_meta = entry.index.as_ref()?.clone();
                Some(crate::backend::parser::BoundIndexRelation {
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc,
                    index_meta,
                })
            })
            .collect()
    }
}
