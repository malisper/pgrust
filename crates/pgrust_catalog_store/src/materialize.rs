use std::collections::{BTreeMap, BTreeSet};

use crate::bootstrap::{bootstrap_catalog_kinds, bootstrap_catalog_rel};
use crate::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexMeta, column_desc,
    missing_default_value_from_attmissingval,
};
use crate::relcache::default_sequence_oid_from_default_expr;
use crate::rows::PhysicalCatalogRows;
use crate::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};
use pgrust_catalog_data::{
    BTREE_AM_OID, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, GIST_AM_OID, PG_CONSTRAINT_RELATION_OID,
    PgAttributeRow, PgClassRow, system_catalog_index_by_oid,
};
use pgrust_core::RelFileLocator;
use pgrust_nodes::access::{
    BrinOptions, BtreeOptions, GinOptions, GistBufferingMode, GistOptions, HashOptions,
};
use pgrust_nodes::parsenodes::{ColumnGeneratedKind, ColumnIdentityKind};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalIndexStorageOptions {
    pub brin_options: Option<BrinOptions>,
    pub gin_options: Option<GinOptions>,
    pub hash_options: Option<HashOptions>,
}

pub fn catalog_from_physical_rows_scoped(
    rows: PhysicalCatalogRows,
    db_oid: u32,
    legacy_default_exprs: BTreeMap<(u32, i16), String>,
) -> Result<Catalog, CatalogError> {
    catalog_from_physical_rows_scoped_with_index_options(
        rows,
        db_oid,
        legacy_default_exprs,
        |_rel, _class_row| PhysicalIndexStorageOptions::default(),
    )
}

pub fn catalog_from_physical_rows_scoped_with_index_options<F>(
    rows: PhysicalCatalogRows,
    db_oid: u32,
    legacy_default_exprs: BTreeMap<(u32, i16), String>,
    mut index_storage_options: F,
) -> Result<Catalog, CatalogError>
where
    F: FnMut(RelFileLocator, &PgClassRow) -> PhysicalIndexStorageOptions,
{
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
        .filter(|row| row.contype == CONSTRAINT_NOTNULL)
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
                desc.attstattarget = attr.attstattarget.unwrap_or(-1);
                desc.attinhcount = attr.attinhcount;
                desc.attislocal = attr.attislocal;
                desc.collation_oid = attr.attcollation;
                desc.fdw_options = attr.attfdwoptions.clone();
                desc.identity = ColumnIdentityKind::from_catalog_char(attr.attidentity);
                desc.generated = ColumnGeneratedKind::from_catalog_char(attr.attgenerated);
                desc.dropped = attr.attisdropped;
                desc.missing_default_value = attr
                    .attmissingval
                    .as_ref()
                    .and_then(|values| values.first().cloned())
                    .map(|value| missing_default_value_from_attmissingval(value, desc.sql_type));
                if let Some(attrdef) = attrdefs_by_key.get(&(row.oid, attr.attnum)) {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                    desc.default_sequence_oid =
                        default_sequence_oid_from_default_expr(&attrdef.adbin);
                } else if let Some(expr) = legacy_default_exprs.get(&(row.oid, attr.attnum)) {
                    desc.default_expr = Some(expr.clone());
                    desc.attrdef_oid = Some(catalog.next_oid);
                    desc.default_sequence_oid = default_sequence_oid_from_default_expr(expr);
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
        let rel = catalog_relation_locator(row.oid, row.relfilenode, row.reltablespace, db_oid);
        let btree_options =
            load_btree_options_from_reloptions(row.relam, row.relkind, row.reloptions.as_deref());
        let gist_options =
            load_gist_options_from_reloptions(row.relam, row.relkind, row.reloptions.as_deref());
        let storage_options = index_storage_options(rel, &row);
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
                        brin_options: storage_options.brin_options.clone(),
                        gist_options,
                        gin_options: storage_options.gin_options.clone(),
                        hash_options: storage_options.hash_options.clone(),
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
        .filter(|row| row.contype == CONSTRAINT_PRIMARY)
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    let pk_owned_not_null = catalog
        .depends
        .iter()
        .filter(|row| {
            row.classid == PG_CONSTRAINT_RELATION_OID
                && row.refclassid == PG_CONSTRAINT_RELATION_OID
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
    pgrust_catalog_data::sort_pg_rewrite_rows(&mut catalog.rewrites);
    catalog.triggers = trigger_rows;
    pgrust_catalog_data::sort_pg_trigger_rows(&mut catalog.triggers);
    catalog.event_triggers = event_trigger_rows;
    pgrust_catalog_data::sort_pg_event_trigger_rows(&mut catalog.event_triggers);
    catalog.policies = policy_rows.clone();
    pgrust_catalog_data::sort_pg_policy_rows(&mut catalog.policies);
    crate::pg_statistic_ext::sort_pg_statistic_ext_rows(&mut catalog.statistics_ext);
    crate::pg_statistic_ext::sort_pg_statistic_ext_data_rows(&mut catalog.statistics_ext_data);
    Ok(catalog)
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
        } else if name.eq_ignore_ascii_case("fillfactor")
            && let Ok(fillfactor) = value.parse::<u16>()
        {
            options.fillfactor = fillfactor;
            saw_option = true;
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

fn catalog_relation_locator(
    relation_oid: u32,
    relfilenode: u32,
    reltablespace: u32,
    db_oid: u32,
) -> RelFileLocator {
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
        spc_oid: reltablespace,
        db_oid,
        rel_number: relfilenode,
    }
}
