#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde_json::Value as JsonValue;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{heap_scan_begin, heap_scan_next};
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, Snapshot, TransactionManager};
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexMeta, column_desc,
};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::catalog::pg_depend::derived_pg_depend_rows;
use crate::backend::catalog::rowcodec::{
    namespace_row_from_values, pg_aggregate_row_from_values, pg_am_row_from_values,
    pg_amop_row_from_values, pg_amproc_row_from_values, pg_attrdef_row_from_values,
    pg_attribute_row_from_values, pg_auth_members_row_from_values, pg_authid_row_from_values,
    pg_cast_row_from_values, pg_class_row_from_values, pg_collation_row_from_values,
    pg_constraint_row_from_values, pg_conversion_row_from_values, pg_database_row_from_values,
    pg_depend_row_from_values, pg_description_row_from_values, pg_event_trigger_row_from_values,
    pg_foreign_data_wrapper_row_from_values, pg_foreign_server_row_from_values,
    pg_foreign_table_row_from_values, pg_index_row_from_values, pg_inherits_row_from_values,
    pg_language_row_from_values, pg_opclass_row_from_values, pg_operator_row_from_values,
    pg_opfamily_row_from_values, pg_partitioned_table_row_from_values, pg_policy_row_from_values,
    pg_proc_row_from_values, pg_publication_namespace_row_from_values,
    pg_publication_rel_row_from_values, pg_publication_row_from_values, pg_rewrite_row_from_values,
    pg_sequence_row_from_values, pg_shdepend_row_from_values,
    pg_statistic_ext_data_row_from_values, pg_statistic_ext_row_from_values,
    pg_statistic_row_from_values, pg_tablespace_row_from_values, pg_trigger_row_from_values,
    pg_ts_config_map_row_from_values, pg_ts_config_row_from_values, pg_ts_dict_row_from_values,
    pg_ts_parser_row_from_values, pg_ts_template_row_from_values, pg_type_row_from_values,
    pg_user_mapping_row_from_values,
};
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::decode_value;
use crate::backend::executor::value_io::missing_column_value;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{
    BLCKSZ, ForkNumber, MdStorageManager, RelFileLocator, StorageManager,
};
use crate::include::access::brin::BrinOptions;
use crate::include::access::brin_page::{
    BRIN_PAGE_CONTENT_OFFSET, BrinMetaPageData, brin_is_meta_page,
};
use crate::include::access::gin::{GinOptions, gin_metapage_data};
use crate::include::access::gist::{GistBufferingMode, GistOptions};
use crate::include::access::hash::{HashOptions, hash_metapage_data};
use crate::include::access::nbtree::BtreeOptions;
use crate::include::catalog::{
    BRIN_AM_OID, BTREE_AM_OID, BootstrapCatalogKind, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, PgAmRow,
    PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgIndexRow, PgNamespaceRow, PgOpclassRow, PgOpfamilyRow, PgTypeRow,
    bootstrap_catalog_kinds, bootstrap_pg_auth_members_rows, bootstrap_pg_authid_rows,
    bootstrap_pg_database_rows, bootstrap_pg_tablespace_rows, bootstrap_relation_desc,
    system_catalog_index_by_oid,
};
use crate::include::nodes::datum::Value;

use super::store::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};

pub(crate) fn load_catalog_from_physical(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows(base_dir)?;
    catalog_from_physical_rows_scoped(base_dir, rows, 1)
}

pub(crate) fn load_catalog_from_visible_physical(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_visible(base_dir, pool, txns, snapshot, client_id)?;
    catalog_from_physical_rows_scoped(base_dir, rows, 1)
}

pub(crate) fn load_catalog_from_physical_scoped(
    base_dir: &Path,
    db_oid: u32,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_scoped(base_dir, db_oid, &bootstrap_catalog_kinds())?;
    catalog_from_physical_rows_scoped(base_dir, rows, db_oid)
}

pub(crate) fn load_catalog_from_visible_physical_scoped(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    db_oid: u32,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_visible_scoped(
        base_dir,
        pool,
        txns,
        snapshot,
        client_id,
        db_oid,
        &bootstrap_catalog_kinds(),
    )?;
    catalog_from_physical_rows_scoped(base_dir, rows, db_oid)
}

pub(crate) fn load_catalog_from_visible_pool(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_visible_in_pool(pool, txns, snapshot, client_id)?;
    catalog_from_physical_rows_scoped(Path::new(""), rows, 1)
}

pub(crate) fn catalog_from_physical_rows(
    base_dir: &Path,
    rows: PhysicalCatalogRows,
) -> Result<Catalog, CatalogError> {
    catalog_from_physical_rows_scoped(base_dir, rows, 1)
}

pub(crate) fn catalog_from_physical_rows_scoped(
    base_dir: &Path,
    rows: PhysicalCatalogRows,
    db_oid: u32,
) -> Result<Catalog, CatalogError> {
    let namespace_rows = rows.namespaces;
    let type_rows = rows.types;
    let class_rows = rows.classes;
    let attribute_rows = rows.attributes;
    let attrdef_rows = rows.attrdefs;
    let depend_rows = rows.depends;
    let inherit_rows = rows.inherits;
    let rewrite_rows = rows.rewrites;
    let trigger_rows = rows.triggers;
    let event_trigger_rows = rows.event_triggers;
    let policy_rows = rows.policies;
    let publication_rows = rows.publications;
    let publication_rel_rows = rows.publication_rels;
    let publication_namespace_rows = rows.publication_namespaces;
    let statistic_ext_rows = rows.statistics_ext;
    let statistic_ext_data_rows = rows.statistics_ext_data;
    let index_rows = rows.indexes;
    let partitioned_table_rows = rows.partitioned_tables;
    let _description_rows = rows.descriptions;
    let _am_rows = rows.ams;
    let authid_rows = rows.authids;
    let auth_members_rows = rows.auth_members;
    let _language_rows = rows.languages;
    let _ts_parser_rows = rows.ts_parsers;
    let _ts_template_rows = rows.ts_templates;
    let _ts_dict_rows = rows.ts_dicts;
    let _ts_config_rows = rows.ts_configs;
    let _ts_config_map_rows = rows.ts_config_maps;
    let constraint_rows = rows.constraints;
    let _operator_rows = rows.operators;
    let _proc_rows = rows.procs;
    let _aggregate_rows = rows.aggregates;
    let _cast_rows = rows.casts;
    let _collation_rows = rows.collations;
    let database_rows = rows.databases;
    let tablespace_rows = rows.tablespaces;

    let namespace_names = namespace_rows
        .iter()
        .map(|row| (row.oid, row.nspname.as_str()))
        .collect::<BTreeMap<_, _>>();
    let type_sql_by_oid = type_rows
        .iter()
        .map(|row| (row.oid, row.sql_type))
        .collect::<BTreeMap<_, _>>();
    let type_rows_by_oid = type_rows
        .iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let mut attrs_by_relid = BTreeMap::<u32, Vec<PgAttributeRow>>::new();
    for row in attribute_rows {
        attrs_by_relid.entry(row.attrelid).or_default().push(row);
    }
    for rows in attrs_by_relid.values_mut() {
        rows.sort_by_key(|row| row.attnum);
    }
    let attrdefs_by_key = attrdef_rows
        .into_iter()
        .map(|row| ((row.adrelid, row.adnum), row))
        .collect::<BTreeMap<_, _>>();
    let not_null_constraints = constraint_rows
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_NOTNULL)
        .filter_map(|row| {
            let attnum = *row.conkey.as_ref()?.first()?;
            Some(((row.conrelid, attnum), row.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let indexes_by_relid = index_rows
        .into_iter()
        .map(|row| (row.indexrelid, row))
        .collect::<BTreeMap<_, _>>();
    let partitioned_tables_by_relid = partitioned_table_rows
        .iter()
        .cloned()
        .map(|row| (row.partrelid, row))
        .collect::<BTreeMap<_, _>>();
    // :HACK: Keep a one-time compatibility path for stores created before `pg_attrdef`
    // existed. Once old datadirs no longer need migration, delete this fallback and
    // require defaults to come only from `pg_attrdef`.
    let legacy_default_exprs = load_legacy_default_exprs(base_dir)?;

    let next_oid = class_rows
        .iter()
        .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
            next_oid
                .max(row.oid.saturating_add(1))
                .max(row.reltype.saturating_add(1))
        })
        .max(
            type_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            attrdefs_by_key
                .values()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            rewrite_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            trigger_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            event_trigger_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            policy_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            authid_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            auth_members_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            constraint_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            database_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            tablespace_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            publication_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            publication_rel_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            publication_namespace_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            statistic_ext_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        );
    let mut catalog = Catalog {
        tables: BTreeMap::new(),
        constraints: Vec::new(),
        depends: Vec::new(),
        inherits: inherit_rows,
        rewrites: Vec::new(),
        triggers: Vec::new(),
        event_triggers: Vec::new(),
        policies: policy_rows.clone(),
        partitioned_tables: partitioned_table_rows,
        publications: publication_rows,
        publication_rels: publication_rel_rows,
        publication_namespaces: publication_namespace_rows,
        statistics_ext: statistic_ext_rows,
        statistics_ext_data: statistic_ext_data_rows,
        authids: authid_rows,
        auth_members: auth_members_rows,
        databases: database_rows,
        tablespaces: tablespace_rows,
        next_rel_number: DEFAULT_FIRST_REL_NUMBER,
        next_oid,
    };
    for row in class_rows {
        let attrs = attrs_by_relid
            .get(&row.oid)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let columns = attrs
            .iter()
            .map(|attr| {
                let sql_type = type_sql_by_oid
                    .get(&attr.atttypid)
                    .copied()
                    .or_else(|| attr.attisdropped.then_some(SqlType::new(SqlTypeKind::Int4)))
                    .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
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
                desc.attstattarget = attr.attstattarget;
                desc.attinhcount = attr.attinhcount;
                desc.attislocal = attr.attislocal;
                desc.collation_oid = attr.attcollation;
                desc.fdw_options = attr.attfdwoptions.clone();
                desc.identity =
                    crate::include::nodes::parsenodes::ColumnIdentityKind::from_catalog_char(
                        attr.attidentity,
                    );
                desc.generated =
                    crate::include::nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
                        attr.attgenerated,
                    );
                desc.dropped = attr.attisdropped;
                if let Some(attrdef) = attrdefs_by_key.get(&(row.oid, attr.attnum)) {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                    desc.default_sequence_oid =
                        crate::pgrust::database::default_sequence_oid_from_default_expr(
                            &attrdef.adbin,
                        );
                    desc.missing_default_value = None;
                } else if let Some(expr) = legacy_default_exprs.get(&(row.oid, attr.attnum)) {
                    desc.default_expr = Some(expr.clone());
                    desc.attrdef_oid = Some(catalog.next_oid);
                    desc.default_sequence_oid =
                        crate::pgrust::database::default_sequence_oid_from_default_expr(expr);
                    desc.missing_default_value = None;
                    catalog.next_oid = catalog.next_oid.saturating_add(1);
                }
                if let Some(constraint) = not_null_constraints.get(&(row.oid, attr.attnum)) {
                    desc.not_null_constraint_oid = Some(constraint.oid);
                    desc.not_null_constraint_name = Some(constraint.conname.clone());
                    desc.not_null_constraint_validated = constraint.convalidated;
                    desc.not_null_constraint_is_local = constraint.conislocal;
                    desc.not_null_constraint_inhcount = constraint.coninhcount;
                    desc.not_null_constraint_no_inherit = constraint.connoinherit;
                }
                Ok(desc)
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;
        let namespace_name = namespace_names
            .get(&row.relnamespace)
            .copied()
            .unwrap_or("pg_catalog");
        let name = match namespace_name {
            "public" | "pg_catalog" => row.relname.clone(),
            other => format!("{other}.{}", row.relname),
        };
        let rel = catalog_relation_locator(row.oid, row.relfilenode, db_oid);
        let btree_options =
            load_btree_options_from_reloptions(row.relam, row.relkind, row.reloptions.as_deref());
        let gist_options =
            load_gist_options_from_reloptions(row.relam, row.relkind, row.reloptions.as_deref());
        let brin_options = load_brin_options_from_metapage(base_dir, rel, row.relam, row.relkind);
        let gin_options = load_gin_options_from_metapage(base_dir, rel, row.relam, row.relkind);
        let hash_options = load_hash_options_from_metapage(base_dir, rel, row.relam, row.relkind);
        catalog.insert(
            name,
            CatalogEntry {
                rel,
                relation_oid: row.oid,
                namespace_oid: row.relnamespace,
                owner_oid: row.relowner,
                relacl: row.relacl.clone(),
                reloptions: row.reloptions.clone(),
                of_type_oid: row.reloftype,
                row_type_oid: row.reltype,
                array_type_oid: type_rows_by_oid
                    .get(&row.reltype)
                    .map(|type_row| type_row.typarray)
                    .unwrap_or(0),
                reltoastrelid: row.reltoastrelid,
                relhasindex: row.relhasindex,
                relpersistence: row.relpersistence,
                relkind: row.relkind,
                am_oid: row.relam,
                relhassubclass: row.relhassubclass,
                relhastriggers: row.relhastriggers,
                relispartition: row.relispartition,
                relispopulated: row.relispopulated,
                relpartbound: row.relpartbound.clone(),
                relrowsecurity: row.relrowsecurity,
                relforcerowsecurity: row.relforcerowsecurity,
                relpages: row.relpages,
                reltuples: row.reltuples,
                relallvisible: row.relallvisible,
                relallfrozen: row.relallfrozen,
                relfrozenxid: row.relfrozenxid,
                desc: RelationDesc { columns },
                partitioned_table: partitioned_tables_by_relid.get(&row.oid).cloned(),
                index_meta: indexes_by_relid
                    .get(&row.oid)
                    .map(|index| CatalogIndexMeta {
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
                        indclass_options: Vec::new(),
                        indcollation: index.indcollation.clone(),
                        indoption: index.indoption.clone(),
                        indexprs: index.indexprs.clone(),
                        indpred: index.indpred.clone(),
                        btree_options,
                        brin_options: brin_options.clone(),
                        gist_options,
                        gin_options: gin_options.clone(),
                        hash_options,
                    }),
            },
        );
        catalog.next_oid = catalog
            .next_oid
            .max(row.oid.saturating_add(1))
            .max(row.reltype.saturating_add(1));
        catalog.next_rel_number = catalog
            .next_rel_number
            .max(row.relfilenode.saturating_add(1));
    }
    catalog.constraints = constraint_rows;
    catalog.depends = depend_rows;
    let primary_constraint_oids = catalog
        .constraints
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    let pk_owned_not_null = catalog
        .depends
        .iter()
        .filter(|row| {
            row.classid == crate::include::catalog::PG_CONSTRAINT_RELATION_OID
                && row.refclassid == crate::include::catalog::PG_CONSTRAINT_RELATION_OID
                && primary_constraint_oids.contains(&row.refobjid)
        })
        .map(|row| row.objid)
        .collect::<BTreeSet<_>>();
    for entry in catalog.tables.values_mut() {
        for column in &mut entry.desc.columns {
            if let Some(constraint_oid) = column.not_null_constraint_oid {
                column.not_null_primary_key_owned = pk_owned_not_null.contains(&constraint_oid);
            }
        }
    }
    catalog.rewrites = rewrite_rows;
    crate::include::catalog::sort_pg_rewrite_rows(&mut catalog.rewrites);
    catalog.triggers = trigger_rows;
    crate::include::catalog::sort_pg_trigger_rows(&mut catalog.triggers);
    catalog.event_triggers = event_trigger_rows;
    crate::include::catalog::sort_pg_event_trigger_rows(&mut catalog.event_triggers);
    catalog.policies = policy_rows.clone();
    crate::include::catalog::sort_pg_policy_rows(&mut catalog.policies);
    crate::backend::catalog::sort_pg_statistic_ext_rows(&mut catalog.statistics_ext);
    crate::backend::catalog::sort_pg_statistic_ext_data_rows(&mut catalog.statistics_ext_data);
    Ok(catalog)
}

fn load_gin_options_from_metapage(
    base_dir: &Path,
    rel: RelFileLocator,
    am_oid: u32,
    relkind: char,
) -> Option<GinOptions> {
    if am_oid != GIN_AM_OID || relkind != 'i' || base_dir.as_os_str().is_empty() {
        return None;
    }

    let mut smgr = MdStorageManager::new(base_dir);
    if !smgr.exists(rel, ForkNumber::Main) || smgr.nblocks(rel, ForkNumber::Main).ok()? == 0 {
        return None;
    }

    let mut page = [0u8; BLCKSZ];
    smgr.read_block(rel, ForkNumber::Main, 0, &mut page).ok()?;
    gin_metapage_data(&page).ok().map(|meta| meta.options())
}

fn load_btree_options_from_reloptions(
    am_oid: u32,
    relkind: char,
    reloptions: Option<&[String]>,
) -> Option<BtreeOptions> {
    if am_oid != BTREE_AM_OID || !matches!(relkind, 'i' | 'I') {
        return None;
    }
    let reloptions = reloptions?;
    let mut options = BtreeOptions::default();
    let mut saw_option = false;
    for option in reloptions {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };
        if name.eq_ignore_ascii_case("fillfactor") {
            if let Ok(fillfactor) = value.parse::<u16>() {
                options.fillfactor = fillfactor;
                saw_option = true;
            }
        } else if name.eq_ignore_ascii_case("deduplicate_items") {
            if let Some(value) = parse_reloption_bool(value) {
                options.deduplicate_items = value;
                saw_option = true;
            }
        }
    }
    saw_option.then_some(options)
}

fn load_gist_options_from_reloptions(
    am_oid: u32,
    relkind: char,
    reloptions: Option<&[String]>,
) -> Option<GistOptions> {
    if am_oid != GIST_AM_OID || !matches!(relkind, 'i' | 'I') {
        return None;
    }
    let reloptions = reloptions?;
    let mut options = GistOptions::default();
    let mut saw_option = false;
    for option in reloptions {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };
        if name.eq_ignore_ascii_case("buffering") {
            if let Some(value) = parse_gist_buffering_mode(value) {
                options.buffering_mode = value;
                saw_option = true;
            }
        }
    }
    saw_option.then_some(options)
}

fn parse_gist_buffering_mode(value: &str) -> Option<GistBufferingMode> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Some(GistBufferingMode::Auto),
        "on" => Some(GistBufferingMode::On),
        "off" => Some(GistBufferingMode::Off),
        _ => None,
    }
}

fn parse_reloption_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn load_hash_options_from_metapage(
    base_dir: &Path,
    rel: RelFileLocator,
    am_oid: u32,
    relkind: char,
) -> Option<HashOptions> {
    if am_oid != HASH_AM_OID || relkind != 'i' || base_dir.as_os_str().is_empty() {
        return None;
    }

    let mut smgr = MdStorageManager::new(base_dir);
    if !smgr.exists(rel, ForkNumber::Main) || smgr.nblocks(rel, ForkNumber::Main).ok()? == 0 {
        return None;
    }

    let mut page = [0u8; BLCKSZ];
    smgr.read_block(rel, ForkNumber::Main, 0, &mut page).ok()?;
    let meta = hash_metapage_data(&page).ok()?;
    Some(HashOptions {
        fillfactor: meta.hashm_ffactor as u16,
    })
}

fn load_brin_options_from_metapage(
    base_dir: &Path,
    rel: RelFileLocator,
    am_oid: u32,
    relkind: char,
) -> Option<BrinOptions> {
    if am_oid != BRIN_AM_OID || relkind != 'i' || base_dir.as_os_str().is_empty() {
        return None;
    }

    let mut smgr = MdStorageManager::new(base_dir);
    if !smgr.exists(rel, ForkNumber::Main) || smgr.nblocks(rel, ForkNumber::Main).ok()? == 0 {
        return None;
    }

    let mut page = [0u8; BLCKSZ];
    smgr.read_block(rel, ForkNumber::Main, 0, &mut page).ok()?;
    if !brin_is_meta_page(&page).ok()? {
        return None;
    }

    let bytes =
        page.get(BRIN_PAGE_CONTENT_OFFSET..BRIN_PAGE_CONTENT_OFFSET + BrinMetaPageData::SIZE)?;
    let pages_per_range = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    (pages_per_range > 0).then_some(BrinOptions { pages_per_range })
}

fn catalog_relation_locator(relation_oid: u32, relfilenode: u32, db_oid: u32) -> RelFileLocator {
    if let Some(kind) = bootstrap_catalog_kinds()
        .into_iter()
        .find(|kind| kind.relation_oid() == relation_oid)
    {
        return bootstrap_catalog_rel(kind, db_oid);
    }
    if let Some(descriptor) = system_catalog_index_by_oid(relation_oid) {
        let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
        return RelFileLocator {
            spc_oid: heap_rel.spc_oid,
            db_oid: heap_rel.db_oid,
            rel_number: relfilenode,
        };
    }
    RelFileLocator {
        spc_oid: 0,
        db_oid,
        rel_number: relfilenode,
    }
}

pub(crate) fn load_physical_catalog_rows_scoped(
    base_dir: &Path,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    let pool = BufferPool::new(SmgrStorageBackend::new(MdStorageManager::new(base_dir)), 64);
    let mut rows = PhysicalCatalogRows::default();
    let mut missing_database = false;
    let mut missing_authid = false;
    let mut missing_auth_members = false;
    let mut missing_tablespace = false;
    let mut missing_constraint = false;
    let mut missing_depend = false;
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        if !smgr.exists(rel, ForkNumber::Main) {
            if kind == BootstrapCatalogKind::PgDatabase {
                missing_database = true;
            }
            if kind == BootstrapCatalogKind::PgAuthId {
                missing_authid = true;
            }
            if kind == BootstrapCatalogKind::PgAuthMembers {
                missing_auth_members = true;
            }
            if kind == BootstrapCatalogKind::PgTablespace {
                missing_tablespace = true;
            }
            if kind == BootstrapCatalogKind::PgConstraint {
                missing_constraint = true;
            }
            if kind == BootstrapCatalogKind::PgDepend {
                missing_depend = true;
            }
            continue;
        }
        let values = scan_catalog_relation(&pool, rel, &bootstrap_relation_desc(kind))?;
        append_catalog_kind_rows(&mut rows, kind, values)?;
    }
    if missing_database {
        rows.databases = bootstrap_pg_database_rows().into();
    }
    if missing_authid {
        rows.authids = bootstrap_pg_authid_rows();
    }
    if missing_auth_members {
        rows.auth_members = bootstrap_pg_auth_members_rows().into();
    }
    if missing_tablespace {
        rows.tablespaces = bootstrap_pg_tablespace_rows().into();
    }
    restore_missing_first_class_catalog_rows_scoped(
        base_dir,
        &mut rows,
        db_oid,
        missing_constraint,
        missing_depend,
    )?;
    Ok(rows)
}

pub(crate) fn load_physical_catalog_rows_visible_scoped(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    db_oid: u32,
    kinds: &[BootstrapCatalogKind],
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut rows = PhysicalCatalogRows::default();
    let mut missing_database = false;
    let mut missing_authid = false;
    let mut missing_auth_members = false;
    let mut missing_tablespace = false;
    let mut missing_constraint = false;
    let mut missing_depend = false;
    for &kind in kinds {
        let rel = bootstrap_catalog_rel(kind, db_oid);
        let exists = pool.with_storage_mut(|storage| storage.smgr.exists(rel, ForkNumber::Main));
        if !exists {
            if kind == BootstrapCatalogKind::PgDatabase {
                missing_database = true;
            }
            if kind == BootstrapCatalogKind::PgAuthId {
                missing_authid = true;
            }
            if kind == BootstrapCatalogKind::PgAuthMembers {
                missing_auth_members = true;
            }
            if kind == BootstrapCatalogKind::PgTablespace {
                missing_tablespace = true;
            }
            if kind == BootstrapCatalogKind::PgConstraint {
                missing_constraint = true;
            }
            if kind == BootstrapCatalogKind::PgDepend {
                missing_depend = true;
            }
            continue;
        }
        let values = load_visible_catalog_kind_in_pool_scoped(
            pool, txns, snapshot, client_id, kind, db_oid,
        )?;
        append_catalog_kind_rows(&mut rows, kind, values)?;
    }
    if missing_database {
        rows.databases = bootstrap_pg_database_rows().into();
    }
    if missing_authid {
        rows.authids = bootstrap_pg_authid_rows();
    }
    if missing_auth_members {
        rows.auth_members = bootstrap_pg_auth_members_rows().into();
    }
    if missing_tablespace {
        rows.tablespaces = bootstrap_pg_tablespace_rows().into();
    }
    restore_missing_first_class_catalog_rows_scoped(
        base_dir,
        &mut rows,
        db_oid,
        missing_constraint,
        missing_depend,
    )?;
    Ok(rows)
}

fn append_catalog_kind_rows(
    rows: &mut PhysicalCatalogRows,
    kind: BootstrapCatalogKind,
    values: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    match kind {
        BootstrapCatalogKind::PgNamespace => {
            rows.namespaces = dedup_by_oid_keep_last(
                values
                    .into_iter()
                    .map(namespace_row_from_values)
                    .collect::<Result<Vec<_>, _>>()?,
                |row| row.oid,
            );
        }
        BootstrapCatalogKind::PgClass => {
            rows.classes = values
                .into_iter()
                .map(pg_class_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAttribute => {
            rows.attributes = values
                .into_iter()
                .map(pg_attribute_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgType => {
            rows.types = values
                .into_iter()
                .map(pg_type_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgProc => {
            rows.procs = dedup_by_oid_keep_last(
                values
                    .into_iter()
                    .map(pg_proc_row_from_values)
                    .collect::<Result<Vec<_>, _>>()?,
                |row| row.oid,
            );
        }
        BootstrapCatalogKind::PgAggregate => {
            rows.aggregates = values
                .into_iter()
                .map(pg_aggregate_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTsParser => {
            rows.ts_parsers = values
                .into_iter()
                .map(pg_ts_parser_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTsTemplate => {
            rows.ts_templates = values
                .into_iter()
                .map(pg_ts_template_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTsDict => {
            rows.ts_dicts = values
                .into_iter()
                .map(pg_ts_dict_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTsConfig => {
            rows.ts_configs = values
                .into_iter()
                .map(pg_ts_config_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTsConfigMap => {
            rows.ts_config_maps = values
                .into_iter()
                .map(pg_ts_config_map_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgLanguage => {
            rows.languages = values
                .into_iter()
                .map(pg_language_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgOperator => {
            rows.operators = values
                .into_iter()
                .map(pg_operator_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgDatabase => {
            rows.databases = values
                .into_iter()
                .map(pg_database_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAuthId => {
            rows.authids = values
                .into_iter()
                .map(pg_authid_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAuthMembers => {
            rows.auth_members = values
                .into_iter()
                .map(pg_auth_members_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgCollation => {
            rows.collations = values
                .into_iter()
                .map(pg_collation_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgLargeobject | BootstrapCatalogKind::PgLargeobjectMetadata => {}
        BootstrapCatalogKind::PgTablespace => {
            rows.tablespaces = values
                .into_iter()
                .map(pg_tablespace_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAm => {
            rows.ams = values
                .into_iter()
                .map(pg_am_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAmop => {
            rows.amops = values
                .into_iter()
                .map(pg_amop_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAmproc => {
            rows.amprocs = values
                .into_iter()
                .map(pg_amproc_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgAttrdef => {
            rows.attrdefs = values
                .into_iter()
                .map(pg_attrdef_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgCast => {
            rows.casts = values
                .into_iter()
                .map(pg_cast_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgConstraint => {
            rows.constraints = values
                .into_iter()
                .map(pg_constraint_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgConversion => {
            rows.conversions = values
                .into_iter()
                .map(pg_conversion_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgDepend => {
            rows.depends = values
                .into_iter()
                .map(pg_depend_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgDefaultAcl
        | BootstrapCatalogKind::PgExtension
        | BootstrapCatalogKind::PgTransform
        | BootstrapCatalogKind::PgSubscription
        | BootstrapCatalogKind::PgParameterAcl
        | BootstrapCatalogKind::PgShdescription
        | BootstrapCatalogKind::PgReplicationOrigin => {}
        BootstrapCatalogKind::PgShdepend => {
            rows.shdepends = values
                .into_iter()
                .map(pg_shdepend_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgDescription => {
            rows.descriptions = values
                .into_iter()
                .map(pg_description_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgForeignDataWrapper => {
            rows.foreign_data_wrappers = values
                .into_iter()
                .map(pg_foreign_data_wrapper_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgForeignServer => {
            rows.foreign_servers = values
                .into_iter()
                .map(pg_foreign_server_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgUserMapping => {
            rows.user_mappings = values
                .into_iter()
                .map(pg_user_mapping_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgForeignTable => {
            rows.foreign_tables = values
                .into_iter()
                .map(pg_foreign_table_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgIndex => {
            rows.indexes = values
                .into_iter()
                .map(pg_index_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgInherits => {
            rows.inherits = values
                .into_iter()
                .map(pg_inherits_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgPartitionedTable => {
            rows.partitioned_tables = values
                .into_iter()
                .map(pg_partitioned_table_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgRewrite => {
            rows.rewrites = values
                .into_iter()
                .map(pg_rewrite_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgSequence => {
            rows.sequences = values
                .into_iter()
                .map(pg_sequence_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgTrigger => {
            rows.triggers = values
                .into_iter()
                .map(pg_trigger_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgEventTrigger => {
            rows.event_triggers = values
                .into_iter()
                .map(pg_event_trigger_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgPolicy => {
            rows.policies = values
                .into_iter()
                .map(pg_policy_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgPublication => {
            rows.publications = values
                .into_iter()
                .map(pg_publication_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgPublicationRel => {
            rows.publication_rels = values
                .into_iter()
                .map(pg_publication_rel_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgPublicationNamespace => {
            rows.publication_namespaces = values
                .into_iter()
                .map(pg_publication_namespace_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgStatistic => {
            rows.statistics = values
                .into_iter()
                .map(pg_statistic_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgStatisticExt => {
            rows.statistics_ext = values
                .into_iter()
                .map(pg_statistic_ext_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgStatisticExtData => {
            rows.statistics_ext_data = values
                .into_iter()
                .map(pg_statistic_ext_data_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgOpclass => {
            rows.opclasses = values
                .into_iter()
                .map(pg_opclass_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
        BootstrapCatalogKind::PgOpfamily => {
            rows.opfamilies = values
                .into_iter()
                .map(pg_opfamily_row_from_values)
                .collect::<Result<Vec<_>, _>>()?;
        }
    }
    Ok(())
}

fn dedup_by_oid_keep_last<T>(rows: Vec<T>, oid: impl Fn(&T) -> u32) -> Vec<T> {
    let mut by_oid = BTreeMap::new();
    for row in rows {
        by_oid.insert(oid(&row), row);
    }
    by_oid.into_values().collect()
}

fn restore_missing_first_class_catalog_rows(
    base_dir: &Path,
    rows: &mut PhysicalCatalogRows,
    missing_constraint: bool,
    missing_depend: bool,
) -> Result<(), CatalogError> {
    restore_missing_first_class_catalog_rows_scoped(
        base_dir,
        rows,
        1,
        missing_constraint,
        missing_depend,
    )
}

fn restore_missing_first_class_catalog_rows_scoped(
    base_dir: &Path,
    rows: &mut PhysicalCatalogRows,
    db_oid: u32,
    missing_constraint: bool,
    missing_depend: bool,
) -> Result<(), CatalogError> {
    if missing_constraint {
        let catalog = catalog_from_physical_rows_scoped(base_dir, rows.clone(), db_oid)?;
        rows.constraints = catalog
            .entries()
            .filter(|(_, entry)| matches!(entry.relkind, 'r' | 'f'))
            .flat_map(|(name, entry)| {
                derived_pg_constraint_rows(
                    entry.relation_oid,
                    name.rsplit('.').next().unwrap_or(name),
                    entry.namespace_oid,
                    &entry.desc,
                )
            })
            .collect();
    }

    if missing_depend {
        let catalog = catalog_from_physical_rows_scoped(base_dir, rows.clone(), db_oid)?;
        rows.depends = catalog
            .entries()
            .flat_map(|(_, entry)| derived_pg_depend_rows(entry))
            .collect();
    }

    Ok(())
}

fn load_legacy_default_exprs(
    base_dir: &Path,
) -> Result<BTreeMap<(u32, i16), String>, CatalogError> {
    let path = base_dir.join("catalog").join("defaults.json");
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let text = fs::read_to_string(&path).map_err(|e| CatalogError::Io(e.to_string()))?;
    let json = serde_json::from_str::<JsonValue>(&text)
        .map_err(|_| CatalogError::Corrupt("invalid legacy defaults json"))?;
    let Some(entries) = json.as_array() else {
        return Err(CatalogError::Corrupt("invalid legacy defaults json root"));
    };

    let mut defaults = BTreeMap::new();
    for entry in entries {
        let relation_oid = entry
            .get("relation_oid")
            .and_then(JsonValue::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid legacy default relation oid"))?;
        let attnum = entry
            .get("attnum")
            .and_then(JsonValue::as_i64)
            .and_then(|v| i16::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid legacy default attnum"))?;
        let expr = entry
            .get("expr")
            .and_then(JsonValue::as_str)
            .ok_or(CatalogError::Corrupt("invalid legacy default expr"))?;
        defaults.insert((relation_oid, attnum), expr.to_string());
    }

    Ok(defaults)
}

pub(crate) fn load_physical_catalog_rows(
    base_dir: &Path,
) -> Result<PhysicalCatalogRows, CatalogError> {
    load_physical_catalog_rows_scoped(base_dir, 1, &bootstrap_catalog_kinds())
}

fn load_physical_catalog_rows_legacy(base_dir: &Path) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    let mut rels = BTreeMap::new();
    let mut missing_attrdef = false;
    let mut missing_depend = false;
    let mut missing_description = false;
    let mut missing_index = false;
    let mut missing_am = false;
    let mut missing_authid = false;
    let mut missing_auth_members = false;
    let mut missing_language = false;
    let mut missing_ts_parser = false;
    let mut missing_ts_template = false;
    let mut missing_ts_dict = false;
    let mut missing_ts_config = false;
    let mut missing_ts_config_map = false;
    let mut missing_constraint = false;
    let mut missing_operator = false;
    let mut missing_proc = false;
    let mut missing_cast = false;
    let mut missing_collation = false;
    let mut missing_database = false;
    let mut missing_tablespace = false;
    let mut missing_inherits = false;
    let mut missing_rewrite = false;
    let mut missing_sequence = false;
    let mut missing_statistic = false;
    let mut missing_statistic_ext = false;
    let mut missing_statistic_ext_data = false;
    for kind in bootstrap_catalog_kinds() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: kind.relation_oid(),
        };
        if !smgr.exists(rel, ForkNumber::Main) {
            if kind == BootstrapCatalogKind::PgAttrdef {
                missing_attrdef = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDepend {
                missing_depend = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDescription {
                missing_description = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgIndex {
                missing_index = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgInherits {
                missing_inherits = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAm {
                missing_am = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthId {
                missing_authid = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthMembers {
                missing_auth_members = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgLanguage {
                missing_language = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsParser {
                missing_ts_parser = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsTemplate {
                missing_ts_template = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsDict {
                missing_ts_dict = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsConfig {
                missing_ts_config = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsConfigMap {
                missing_ts_config_map = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgConstraint {
                missing_constraint = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgOperator {
                missing_operator = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgProc {
                missing_proc = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAggregate {
                continue;
            }
            if kind == BootstrapCatalogKind::PgCollation {
                missing_collation = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgCast {
                missing_cast = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDatabase {
                missing_database = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTablespace {
                missing_tablespace = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgRewrite {
                missing_rewrite = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgSequence {
                missing_sequence = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatistic {
                missing_statistic = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatisticExt {
                missing_statistic_ext = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatisticExtData {
                missing_statistic_ext_data = true;
                continue;
            }
            return Err(CatalogError::Corrupt("missing physical bootstrap catalog"));
        }
        smgr.open(rel)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        rels.insert(kind, rel);
    }
    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);

    let namespace_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgNamespace],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
    )?
    .into_iter()
    .map(namespace_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let type_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgType],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
    )?
    .into_iter()
    .map(pg_type_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let class_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgClass],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
    )?
    .into_iter()
    .map(pg_class_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let attribute_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgAttribute],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
    )?
    .into_iter()
    .map(pg_attribute_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let database_rows = if missing_database {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgDatabase],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDatabase),
        )?
        .into_iter()
        .map(pg_database_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let authid_rows = if missing_authid {
        bootstrap_pg_authid_rows().to_vec()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAuthId],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthId),
        )?
        .into_iter()
        .map(pg_authid_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let auth_members_rows = if missing_auth_members {
        bootstrap_pg_auth_members_rows().into()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAuthMembers],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthMembers),
        )?
        .into_iter()
        .map(pg_auth_members_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let language_rows = if missing_language {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgLanguage],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgLanguage),
        )?
        .into_iter()
        .map(pg_language_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_parser_rows = if missing_ts_parser {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTsParser],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsParser),
        )?
        .into_iter()
        .map(pg_ts_parser_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_template_rows = if missing_ts_template {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTsTemplate],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsTemplate),
        )?
        .into_iter()
        .map(pg_ts_template_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_dict_rows = if missing_ts_dict {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTsDict],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsDict),
        )?
        .into_iter()
        .map(pg_ts_dict_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_config_rows = if missing_ts_config {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTsConfig],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsConfig),
        )?
        .into_iter()
        .map(pg_ts_config_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_config_map_rows = if missing_ts_config_map {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTsConfigMap],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsConfigMap),
        )?
        .into_iter()
        .map(pg_ts_config_map_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let constraint_rows = if missing_constraint {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgConstraint],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgConstraint),
        )?
        .into_iter()
        .map(pg_constraint_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let operator_rows = if missing_operator {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgOperator],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgOperator),
        )?
        .into_iter()
        .map(pg_operator_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let proc_rows = if missing_proc {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgProc],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgProc),
        )?
        .into_iter()
        .map(pg_proc_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let aggregate_rows = if rels.contains_key(&BootstrapCatalogKind::PgAggregate) {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAggregate],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAggregate),
        )?
        .into_iter()
        .map(pg_aggregate_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let collation_rows = if missing_collation {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgCollation],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCollation),
        )?
        .into_iter()
        .map(pg_collation_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let cast_rows = if missing_cast {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgCast],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCast),
        )?
        .into_iter()
        .map(pg_cast_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let attrdef_rows = if missing_attrdef {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAttrdef],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAttrdef),
        )?
        .into_iter()
        .map(pg_attrdef_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let depend_rows = if missing_depend {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgDepend],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDepend),
        )?
        .into_iter()
        .map(pg_depend_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let inherit_rows = if missing_inherits {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgInherits],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgInherits),
        )?
        .into_iter()
        .map(pg_inherits_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let description_rows = if missing_description {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgDescription],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDescription),
        )?
        .into_iter()
        .map(pg_description_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let index_rows = if missing_index {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgIndex],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgIndex),
        )?
        .into_iter()
        .map(pg_index_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let am_rows = if missing_am {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAm],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAm),
        )?
        .into_iter()
        .map(pg_am_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let amop_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgAmop],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAmop),
    )?
    .into_iter()
    .map(pg_amop_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let amproc_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgAmproc],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAmproc),
    )?
    .into_iter()
    .map(pg_amproc_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let opclass_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgOpclass],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgOpclass),
    )?
    .into_iter()
    .map(pg_opclass_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let opfamily_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgOpfamily],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgOpfamily),
    )?
    .into_iter()
    .map(pg_opfamily_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let tablespace_rows = if missing_tablespace {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTablespace],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTablespace),
        )?
        .into_iter()
        .map(pg_tablespace_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let rewrite_rows = if missing_rewrite {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgRewrite],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgRewrite),
        )?
        .into_iter()
        .map(pg_rewrite_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let sequence_rows = if missing_sequence {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgSequence],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgSequence),
        )?
        .into_iter()
        .map(pg_sequence_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let policy_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgPolicy],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgPolicy),
    )?
    .into_iter()
    .map(pg_policy_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let statistic_rows = if missing_statistic {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgStatistic],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatistic),
        )?
        .into_iter()
        .map(pg_statistic_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let statistic_ext_rows = if missing_statistic_ext {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgStatisticExt],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatisticExt),
        )?
        .into_iter()
        .map(pg_statistic_ext_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let statistic_ext_data_rows = if missing_statistic_ext_data {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgStatisticExtData],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatisticExtData),
        )?
        .into_iter()
        .map(pg_statistic_ext_data_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };

    let mut rows = PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        depends: depend_rows,
        shdepends: Vec::new(),
        inherits: inherit_rows,
        descriptions: description_rows,
        foreign_data_wrappers: Vec::new(),
        foreign_servers: Vec::new(),
        foreign_tables: Vec::new(),
        user_mappings: Vec::new(),
        indexes: index_rows,
        rewrites: rewrite_rows,
        sequences: sequence_rows,
        triggers: Vec::new(),
        event_triggers: Vec::new(),
        policies: policy_rows,
        publications: Vec::new(),
        publication_rels: Vec::new(),
        publication_namespaces: Vec::new(),
        statistics_ext: statistic_ext_rows,
        statistics_ext_data: statistic_ext_data_rows,
        ams: am_rows,
        amops: amop_rows,
        amprocs: amproc_rows,
        authids: authid_rows,
        auth_members: auth_members_rows,
        languages: language_rows,
        ts_parsers: ts_parser_rows,
        ts_templates: ts_template_rows,
        ts_dicts: ts_dict_rows,
        ts_configs: ts_config_rows,
        ts_config_maps: ts_config_map_rows,
        constraints: constraint_rows,
        operators: operator_rows,
        opclasses: opclass_rows,
        opfamilies: opfamily_rows,
        partitioned_tables: Vec::new(),
        procs: proc_rows,
        aggregates: aggregate_rows,
        casts: cast_rows,
        conversions: Vec::new(),
        collations: collation_rows,
        databases: database_rows,
        tablespaces: tablespace_rows,
        statistics: statistic_rows,
        types: type_rows,
    };
    restore_missing_first_class_catalog_rows(
        base_dir,
        &mut rows,
        missing_constraint,
        missing_depend,
    )?;
    Ok(rows)
}

pub(crate) fn load_physical_catalog_rows_visible(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<PhysicalCatalogRows, CatalogError> {
    load_physical_catalog_rows_visible_scoped(
        base_dir,
        pool,
        txns,
        snapshot,
        client_id,
        1,
        &bootstrap_catalog_kinds(),
    )
}

fn load_physical_catalog_rows_visible_legacy(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut rels = BTreeMap::new();
    let mut missing_attrdef = false;
    let mut missing_depend = false;
    let mut missing_description = false;
    let mut missing_index = false;
    let mut missing_am = false;
    let mut missing_authid = false;
    let mut missing_auth_members = false;
    let mut missing_language = false;
    let mut missing_ts_parser = false;
    let mut missing_ts_template = false;
    let mut missing_ts_dict = false;
    let mut missing_ts_config = false;
    let mut missing_ts_config_map = false;
    let mut missing_constraint = false;
    let mut missing_operator = false;
    let mut missing_proc = false;
    let mut missing_cast = false;
    let mut missing_collation = false;
    let mut missing_database = false;
    let mut missing_tablespace = false;
    let mut missing_inherits = false;
    let mut missing_rewrite = false;
    let mut missing_sequence = false;
    let mut missing_statistic = false;
    let mut missing_statistic_ext = false;
    let mut missing_statistic_ext_data = false;
    for kind in bootstrap_catalog_kinds() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: kind.relation_oid(),
        };
        let exists = pool.with_storage_mut(|storage| storage.smgr.exists(rel, ForkNumber::Main));
        if !exists {
            if kind == BootstrapCatalogKind::PgAttrdef {
                missing_attrdef = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDepend {
                missing_depend = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDescription {
                missing_description = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgIndex {
                missing_index = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgInherits {
                missing_inherits = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAm {
                missing_am = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthId {
                missing_authid = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthMembers {
                missing_auth_members = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgLanguage {
                missing_language = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsParser {
                missing_ts_parser = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsTemplate {
                missing_ts_template = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsDict {
                missing_ts_dict = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsConfig {
                missing_ts_config = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTsConfigMap {
                missing_ts_config_map = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgConstraint {
                missing_constraint = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgOperator {
                missing_operator = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgProc {
                missing_proc = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAggregate {
                continue;
            }
            if kind == BootstrapCatalogKind::PgCollation {
                missing_collation = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgCast {
                missing_cast = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDatabase {
                missing_database = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTablespace {
                missing_tablespace = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgRewrite {
                missing_rewrite = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgSequence {
                missing_sequence = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatistic {
                missing_statistic = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatisticExt {
                missing_statistic_ext = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgStatisticExtData {
                missing_statistic_ext_data = true;
                continue;
            }
            return Err(CatalogError::Corrupt("missing physical bootstrap catalog"));
        }
        pool.with_storage_mut(|storage| storage.smgr.open(rel))
            .map_err(|e| CatalogError::Io(e.to_string()))?;
        rels.insert(kind, rel);
    }

    let namespace_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgNamespace],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
    )?
    .into_iter()
    .map(namespace_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let type_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgType],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
    )?
    .into_iter()
    .map(pg_type_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let class_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgClass],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
    )?
    .into_iter()
    .map(pg_class_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let attribute_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgAttribute],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
    )?
    .into_iter()
    .map(pg_attribute_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let database_rows = if missing_database {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgDatabase],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDatabase),
        )?
        .into_iter()
        .map(pg_database_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let authid_rows = if missing_authid {
        bootstrap_pg_authid_rows().to_vec()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgAuthId],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthId),
        )?
        .into_iter()
        .map(pg_authid_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let auth_members_rows = if missing_auth_members {
        bootstrap_pg_auth_members_rows().into()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgAuthMembers],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthMembers),
        )?
        .into_iter()
        .map(pg_auth_members_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let language_rows = if missing_language {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgLanguage],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgLanguage),
        )?
        .into_iter()
        .map(pg_language_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_parser_rows = if missing_ts_parser {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTsParser],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsParser),
        )?
        .into_iter()
        .map(pg_ts_parser_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_template_rows = if missing_ts_template {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTsTemplate],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsTemplate),
        )?
        .into_iter()
        .map(pg_ts_template_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_dict_rows = if missing_ts_dict {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTsDict],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsDict),
        )?
        .into_iter()
        .map(pg_ts_dict_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_config_rows = if missing_ts_config {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTsConfig],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsConfig),
        )?
        .into_iter()
        .map(pg_ts_config_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let ts_config_map_rows = if missing_ts_config_map {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTsConfigMap],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTsConfigMap),
        )?
        .into_iter()
        .map(pg_ts_config_map_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let constraint_rows = if missing_constraint {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgConstraint],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgConstraint),
        )?
        .into_iter()
        .map(pg_constraint_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let operator_rows = if missing_operator {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgOperator],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgOperator),
        )?
        .into_iter()
        .map(pg_operator_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let proc_rows = if missing_proc {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgProc],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgProc),
        )?
        .into_iter()
        .map(pg_proc_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let aggregate_rows = if rels.contains_key(&BootstrapCatalogKind::PgAggregate) {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgAggregate],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAggregate),
        )?
        .into_iter()
        .map(pg_aggregate_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let collation_rows = if missing_collation {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgCollation],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCollation),
        )?
        .into_iter()
        .map(pg_collation_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let cast_rows = if missing_cast {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgCast],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCast),
        )?
        .into_iter()
        .map(pg_cast_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let attrdef_rows = if missing_attrdef {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgAttrdef],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAttrdef),
        )?
        .into_iter()
        .map(pg_attrdef_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let depend_rows = if missing_depend {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgDepend],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDepend),
        )?
        .into_iter()
        .map(pg_depend_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let inherit_rows = if missing_inherits {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            &pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgInherits],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgInherits),
        )?
        .into_iter()
        .map(pg_inherits_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let description_rows = if missing_description {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgDescription],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDescription),
        )?
        .into_iter()
        .map(pg_description_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let index_rows = if missing_index {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgIndex],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgIndex),
        )?
        .into_iter()
        .map(pg_index_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let am_rows = if missing_am {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgAm],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAm),
        )?
        .into_iter()
        .map(pg_am_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let amop_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgAmop],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAmop),
    )?
    .into_iter()
    .map(pg_amop_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let amproc_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgAmproc],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAmproc),
    )?
    .into_iter()
    .map(pg_amproc_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let opclass_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgOpclass],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgOpclass),
    )?
    .into_iter()
    .map(pg_opclass_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let opfamily_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgOpfamily],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgOpfamily),
    )?
    .into_iter()
    .map(pg_opfamily_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let tablespace_rows = if missing_tablespace {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgTablespace],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTablespace),
        )?
        .into_iter()
        .map(pg_tablespace_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let rewrite_rows = if missing_rewrite {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgRewrite],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgRewrite),
        )?
        .into_iter()
        .map(pg_rewrite_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let sequence_rows = if missing_sequence {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgSequence],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgSequence),
        )?
        .into_iter()
        .map(pg_sequence_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let policy_rows = scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rels[&BootstrapCatalogKind::PgPolicy],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgPolicy),
    )?
    .into_iter()
    .map(pg_policy_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let statistic_rows = if missing_statistic {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgStatistic],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatistic),
        )?
        .into_iter()
        .map(pg_statistic_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let statistic_ext_rows = if missing_statistic_ext {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgStatisticExt],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatisticExt),
        )?
        .into_iter()
        .map(pg_statistic_ext_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let statistic_ext_data_rows = if missing_statistic_ext_data {
        Vec::new()
    } else {
        scan_catalog_relation_visible(
            pool,
            txns,
            snapshot,
            client_id,
            rels[&BootstrapCatalogKind::PgStatisticExtData],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgStatisticExtData),
        )?
        .into_iter()
        .map(pg_statistic_ext_data_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };

    let mut rows = PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        depends: depend_rows,
        shdepends: Vec::new(),
        inherits: inherit_rows,
        descriptions: description_rows,
        foreign_data_wrappers: Vec::new(),
        foreign_servers: Vec::new(),
        foreign_tables: Vec::new(),
        user_mappings: Vec::new(),
        indexes: index_rows,
        rewrites: rewrite_rows,
        sequences: sequence_rows,
        triggers: Vec::new(),
        event_triggers: Vec::new(),
        policies: policy_rows,
        publications: Vec::new(),
        publication_rels: Vec::new(),
        publication_namespaces: Vec::new(),
        statistics_ext: statistic_ext_rows,
        statistics_ext_data: statistic_ext_data_rows,
        ams: am_rows,
        amops: amop_rows,
        amprocs: amproc_rows,
        authids: authid_rows,
        auth_members: auth_members_rows,
        languages: language_rows,
        ts_parsers: ts_parser_rows,
        ts_templates: ts_template_rows,
        ts_dicts: ts_dict_rows,
        ts_configs: ts_config_rows,
        ts_config_maps: ts_config_map_rows,
        constraints: constraint_rows,
        operators: operator_rows,
        opclasses: opclass_rows,
        opfamilies: opfamily_rows,
        partitioned_tables: Vec::new(),
        procs: proc_rows,
        aggregates: aggregate_rows,
        casts: cast_rows,
        conversions: Vec::new(),
        collations: collation_rows,
        databases: database_rows,
        tablespaces: tablespace_rows,
        statistics: statistic_rows,
        types: type_rows,
    };
    restore_missing_first_class_catalog_rows(
        base_dir,
        &mut rows,
        missing_constraint,
        missing_depend,
    )?;
    Ok(rows)
}

pub(crate) fn load_physical_catalog_rows_visible_in_pool(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<PhysicalCatalogRows, CatalogError> {
    load_physical_catalog_rows_visible(Path::new(""), pool, txns, snapshot, client_id)
}

pub(crate) fn load_visible_namespace_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgNamespaceRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgNamespace,
    )?
    .into_iter()
    .map(namespace_row_from_values)
    .collect()
}

pub(crate) fn load_visible_type_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgTypeRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgType,
    )?
    .into_iter()
    .map(pg_type_row_from_values)
    .collect()
}

pub(crate) fn load_visible_class_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgClassRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgClass,
    )?
    .into_iter()
    .map(pg_class_row_from_values)
    .collect()
}

pub(crate) fn load_visible_attribute_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgAttributeRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgAttribute,
    )?
    .into_iter()
    .map(pg_attribute_row_from_values)
    .collect()
}

pub(crate) fn load_visible_attrdef_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgAttrdefRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgAttrdef,
    )?
    .into_iter()
    .map(pg_attrdef_row_from_values)
    .collect()
}

pub(crate) fn load_visible_index_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgIndexRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgIndex,
    )?
    .into_iter()
    .map(pg_index_row_from_values)
    .collect()
}

pub(crate) fn load_visible_constraint_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgConstraintRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgConstraint,
    )?
    .into_iter()
    .map(pg_constraint_row_from_values)
    .collect()
}

pub(crate) fn load_visible_depend_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<crate::include::catalog::PgDependRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgDepend,
    )?
    .into_iter()
    .map(pg_depend_row_from_values)
    .collect()
}

pub(crate) fn load_visible_inherit_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<crate::include::catalog::PgInheritsRow>, CatalogError> {
    load_visible_catalog_kind(
        base_dir,
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgInherits,
    )?
    .into_iter()
    .map(pg_inherits_row_from_values)
    .collect()
}

pub(crate) fn load_visible_rewrite_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<crate::include::catalog::PgRewriteRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgRewrite,
    )?
    .into_iter()
    .map(pg_rewrite_row_from_values)
    .collect()
}

pub(crate) fn load_visible_statistic_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<crate::include::catalog::PgStatisticRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgStatistic,
    )?
    .into_iter()
    .map(pg_statistic_row_from_values)
    .collect()
}

pub(crate) fn load_visible_am_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgAmRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(pool, txns, snapshot, client_id, BootstrapCatalogKind::PgAm)?
        .into_iter()
        .map(pg_am_row_from_values)
        .collect()
}

pub(crate) fn load_visible_amop_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgAmopRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgAmop,
    )?
    .into_iter()
    .map(pg_amop_row_from_values)
    .collect()
}

pub(crate) fn load_visible_amproc_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgAmprocRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgAmproc,
    )?
    .into_iter()
    .map(pg_amproc_row_from_values)
    .collect()
}

pub(crate) fn load_visible_opclass_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgOpclassRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgOpclass,
    )?
    .into_iter()
    .map(pg_opclass_row_from_values)
    .collect()
}

pub(crate) fn load_visible_opfamily_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgOpfamilyRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgOpfamily,
    )?
    .into_iter()
    .map(pg_opfamily_row_from_values)
    .collect()
}

pub(crate) fn load_visible_collation_rows(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Vec<PgCollationRow>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(
        pool,
        txns,
        snapshot,
        client_id,
        BootstrapCatalogKind::PgCollation,
    )?
    .into_iter()
    .map(pg_collation_row_from_values)
    .collect()
}

fn load_visible_catalog_kind(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    kind: BootstrapCatalogKind,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let _ = base_dir;
    load_visible_catalog_kind_in_pool(pool, txns, snapshot, client_id, kind)
}

fn load_visible_catalog_kind_in_pool(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    kind: BootstrapCatalogKind,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    load_visible_catalog_kind_in_pool_scoped(pool, txns, snapshot, client_id, kind, 1)
}

pub(crate) fn load_visible_catalog_kind_in_pool_scoped(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    kind: BootstrapCatalogKind,
    db_oid: u32,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let rel = bootstrap_catalog_rel(kind, db_oid);
    let exists = pool.with_storage_mut(|storage| storage.smgr.exists(rel, ForkNumber::Main));
    if !exists {
        return Ok(Vec::new());
    }
    scan_catalog_relation_visible(
        pool,
        txns,
        snapshot,
        client_id,
        rel,
        &bootstrap_relation_desc(kind),
    )
}

fn scan_catalog_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let rel_context = catalog_scan_context(rel, desc);
    let mut scan = heap_scan_begin(pool, rel)
        .map_err(|e| CatalogError::Io(format!("{rel_context}: heap scan begin failed: {e:?}")))?;
    let attr_descs = desc.attribute_descs();
    let mut rows = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, INVALID_TRANSACTION_ID, &mut scan)
        .map_err(|e| CatalogError::Io(format!("{rel_context}: heap scan failed: {e:?}")))?
    {
        let raw = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("{rel_context}: heap deform failed: {e:?}")))?;
        let row = desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if let Some(datum) = raw.get(index) {
                    decode_value(column, *datum).map_err(|e| {
                        CatalogError::Io(format!(
                            "{rel_context}: decode column {} failed: {e:?}",
                            column.name
                        ))
                    })
                } else {
                    Ok(missing_column_value(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn scan_catalog_relation_visible(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
    rel: RelFileLocator,
    desc: &RelationDesc,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let rel_context = catalog_scan_context(rel, desc);
    let mut scan = heap_scan_begin(pool, rel)
        .map_err(|e| CatalogError::Io(format!("{rel_context}: heap scan begin failed: {e:?}")))?;
    let attr_descs = desc.attribute_descs();
    let mut rows = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
        .map_err(|e| CatalogError::Io(format!("{rel_context}: heap scan failed: {e:?}")))?
    {
        if !snapshot.tuple_visible(txns, &tuple) {
            continue;
        }
        let raw = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("{rel_context}: heap deform failed: {e:?}")))?;
        let row = desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if let Some(datum) = raw.get(index) {
                    decode_value(column, *datum).map_err(|e| {
                        CatalogError::Io(format!(
                            "{rel_context}: decode column {} failed: {e:?}",
                            column.name
                        ))
                    })
                } else {
                    Ok(missing_column_value(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn catalog_scan_context(rel: RelFileLocator, desc: &RelationDesc) -> String {
    format!(
        "catalog rel {} db {} spc {} ({} columns)",
        rel.rel_number,
        rel.db_oid,
        rel.spc_oid,
        desc.columns.len()
    )
}
