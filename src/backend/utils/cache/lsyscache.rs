use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible;
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::catalog::rowcodec::{
    namespace_row_from_values, pg_am_row_from_values, pg_amop_row_from_values,
    pg_amproc_row_from_values, pg_attrdef_row_from_values, pg_attribute_row_from_values,
    pg_class_row_from_values, pg_collation_row_from_values, pg_index_row_from_values,
    pg_opclass_row_from_values, pg_opfamily_row_from_values, pg_type_row_from_values,
};
use crate::backend::parser::{BoundRelation, CatalogLookup, SqlType};
use crate::backend::utils::cache::relcache::{IndexRelCacheEntry, RelCacheEntry};
use crate::backend::utils::cache::syscache::{
    catalog_snapshot_for_lookup, ensure_attribute_rows, ensure_class_rows, ensure_constraint_rows,
    ensure_namespace_rows, ensure_rewrite_rows, ensure_statistic_rows, ensure_type_rows,
};
use crate::backend::utils::cache::system_views::{build_pg_stats_rows, build_pg_views_rows};
use crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    PgAmRow, PgAmopRow, PgAmprocRow, PgClassRow, PgCollationRow, PgConstraintRow, PgIndexRow,
    PgOpclassRow, PgOpfamilyRow, PgRewriteRow, PgStatisticRow, PgTypeRow,
};
use crate::include::nodes::datum::Value;
use crate::pgrust::database::{Database, TempNamespace};
use crate::{RelFileLocator, backend::utils::cache::catcache::normalize_catalog_name};

const PG_NAMESPACE_NSPNAME_INDEX_OID: u32 = 2684;
const PG_NAMESPACE_OID_INDEX_OID: u32 = 2685;
const PG_CLASS_OID_INDEX_OID: u32 = 2662;
const PG_CLASS_RELNAME_NSP_INDEX_OID: u32 = 2663;
const PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID: u32 = 2659;
const PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID: u32 = 2656;
const PG_TYPE_OID_INDEX_OID: u32 = 2703;
const PG_INDEX_INDRELID_INDEX_OID: u32 = 2678;
const PG_INDEX_INDEXRELID_INDEX_OID: u32 = 2679;
const PG_AM_NAME_INDEX_OID: u32 = 2651;
const PG_AM_OID_INDEX_OID: u32 = 2652;
const PG_AMOP_FAM_STRAT_INDEX_OID: u32 = 2653;
const PG_AMPROC_FAM_PROC_INDEX_OID: u32 = 2655;
const PG_OPCLASS_AM_NAME_NSP_INDEX_OID: u32 = 2686;
const PG_OPCLASS_OID_INDEX_OID: u32 = 2687;
const PG_OPFAMILY_OID_INDEX_OID: u32 = 2755;
const PG_COLLATION_OID_INDEX_OID: u32 = 3085;

fn eq_key(attribute_number: i16, argument: Value) -> ScanKeyData {
    ScanKeyData {
        attribute_number,
        strategy: BT_EQUAL_STRATEGY_NUMBER,
        argument,
    }
}

fn oid_value(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn probe_rows<T>(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
    decode: fn(Vec<Value>) -> Result<T, CatalogError>,
) -> Vec<T> {
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    probe_system_catalog_rows_visible(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        index_relation_oid,
        key_data,
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|values| decode(values).ok())
    .collect()
}

fn probe_first_row<T>(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    index_relation_oid: u32,
    key_data: Vec<ScanKeyData>,
    decode: fn(Vec<Value>) -> Result<T, CatalogError>,
) -> Option<T> {
    probe_rows(db, client_id, txn_ctx, index_relation_oid, key_data, decode)
        .into_iter()
        .next()
}

fn namespace_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_NAMESPACE_NSPNAME_INDEX_OID,
        vec![eq_key(1, Value::Text(name.to_ascii_lowercase().into()))],
        namespace_row_from_values,
    )
}

fn namespace_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_NAMESPACE_OID_INDEX_OID,
        vec![eq_key(1, oid_value(oid))],
        namespace_row_from_values,
    )
}

fn class_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_CLASS_OID_INDEX_OID,
        vec![eq_key(1, oid_value(oid))],
        pg_class_row_from_values,
    )
}

fn class_row_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relname: &str,
    namespace_oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_CLASS_RELNAME_NSP_INDEX_OID,
        vec![
            eq_key(1, Value::Text(relname.to_ascii_lowercase().into())),
            eq_key(2, oid_value(namespace_oid)),
        ],
        pg_class_row_from_values,
    )
}

fn attribute_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttributeRow> {
    let mut rows = probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID,
        vec![eq_key(1, oid_value(relation_oid))],
        pg_attribute_row_from_values,
    );
    rows.sort_by_key(|row| row.attnum);
    rows
}

fn attrdef_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttrdefRow> {
    let mut rows = probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID,
        vec![eq_key(1, oid_value(relation_oid))],
        pg_attrdef_row_from_values,
    );
    rows.sort_by_key(|row| row.adnum);
    rows
}

fn type_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_TYPE_OID_INDEX_OID,
        vec![eq_key(1, oid_value(oid))],
        pg_type_row_from_values,
    )
}

fn opclass_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgOpclassRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_OPCLASS_OID_INDEX_OID,
        vec![eq_key(1, oid_value(oid))],
        pg_opclass_row_from_values,
    )
}

fn opclass_rows_for_am(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Vec<PgOpclassRow> {
    probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_OPCLASS_AM_NAME_NSP_INDEX_OID,
        vec![eq_key(1, oid_value(am_oid))],
        pg_opclass_row_from_values,
    )
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
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_AM_NAME_INDEX_OID,
        vec![eq_key(1, Value::Text(amname.to_ascii_lowercase().into()))],
        pg_am_row_from_values,
    )
}

pub fn access_method_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Option<PgAmRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_AM_OID_INDEX_OID,
        vec![eq_key(1, oid_value(am_oid))],
        pg_am_row_from_values,
    )
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
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_OPFAMILY_OID_INDEX_OID,
        vec![eq_key(1, oid_value(family_oid))],
        pg_opfamily_row_from_values,
    )
}

pub fn collation_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    collation_oid: u32,
) -> Option<PgCollationRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_COLLATION_OID_INDEX_OID,
        vec![eq_key(1, oid_value(collation_oid))],
        pg_collation_row_from_values,
    )
}

pub fn amop_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmopRow> {
    probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_AMOP_FAM_STRAT_INDEX_OID,
        vec![eq_key(1, oid_value(family_oid))],
        pg_amop_row_from_values,
    )
}

pub fn amproc_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmprocRow> {
    probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_AMPROC_FAM_PROC_INDEX_OID,
        vec![eq_key(1, oid_value(family_oid))],
        pg_amproc_row_from_values,
    )
}

pub fn index_row_by_indexrelid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<PgIndexRow> {
    probe_first_row(
        db,
        client_id,
        txn_ctx,
        PG_INDEX_INDEXRELID_INDEX_OID,
        vec![eq_key(1, oid_value(relation_oid))],
        pg_index_row_from_values,
    )
}

pub fn index_relation_oids_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    probe_rows(
        db,
        client_id,
        txn_ctx,
        PG_INDEX_INDRELID_INDEX_OID,
        vec![eq_key(1, oid_value(relation_oid))],
        pg_index_row_from_values,
    )
    .into_iter()
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

    let class = class_row_by_oid(db, client_id, txn_ctx, relation_oid)?;
    if db.other_session_temp_namespace_oid(client_id, class.relnamespace) {
        return None;
    }

    let attrdefs = attrdef_rows_for_relation(db, client_id, txn_ctx, relation_oid);
    let columns = attribute_rows_for_relation(db, client_id, txn_ctx, relation_oid)
        .into_iter()
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
            desc.storage.attlen = attr.attlen;
            desc.storage.attalign = attr.attalign;
            desc.storage.attstorage = attr.attstorage;
            desc.storage.attcompression = attr.attcompression;
            desc.dropped = attr.attisdropped;
            if let Some(attrdef) = attrdefs
                .iter()
                .find(|attrdef| attrdef.adrelid == relation_oid && attrdef.adnum == attr.attnum)
            {
                desc.attrdef_oid = Some(attrdef.oid);
                desc.default_expr = Some(attrdef.adbin.clone());
                desc.missing_default_value = crate::backend::parser::derive_literal_default_value(
                    &attrdef.adbin,
                    desc.sql_type,
                )
                .ok();
            }
            Some(desc)
        })
        .collect::<Option<Vec<_>>>()?;

    let index = if class.relkind == 'i' {
        index_row_by_indexrelid(db, client_id, txn_ctx, relation_oid).map(|index_row| {
            let am_row = access_method_row_by_oid(db, client_id, txn_ctx, class.relam);
            let indclass = index_row.indclass.clone();
            let resolved_opclasses = indclass
                .iter()
                .filter_map(|oid| opclass_row_by_oid(db, client_id, txn_ctx, *oid))
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
        reltoastrelid: class.reltoastrelid,
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
        let namespace_oid = namespace_oid_for_name(db, client_id, txn_ctx, &schema_name)?;
        let class = class_row_by_name_namespace(db, client_id, txn_ctx, relname, namespace_oid)?;
        let entry = relation_entry_by_oid(db, client_id, txn_ctx, class.oid)?;
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &entry),
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
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &temp),
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
        let Some(class) =
            class_row_by_name_namespace(db, client_id, txn_ctx, &normalized, namespace_oid)
        else {
            continue;
        };
        let Some(entry) = relation_entry_by_oid(db, client_id, txn_ctx, class.oid) else {
            continue;
        };
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &entry),
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

    fn type_rows(&self) -> Vec<PgTypeRow> {
        ensure_type_rows(self.db, self.client_id, self.txn_ctx)
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

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        ensure_statistic_rows(self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|row| row.starelid == relation_oid)
            .collect()
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        build_pg_views_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
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
}
