use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde_json::Value as JsonValue;

use crate::BufferPool;
use crate::backend::access::heap::heapam::{heap_scan_begin, heap_scan_next};
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, Snapshot, TransactionManager};
use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexMeta, column_desc,
};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::catalog::pg_depend::derived_pg_depend_rows;
use crate::backend::catalog::rowcodec::{
    namespace_row_from_values, pg_am_row_from_values, pg_amop_row_from_values,
    pg_amproc_row_from_values, pg_attrdef_row_from_values, pg_attribute_row_from_values,
    pg_auth_members_row_from_values, pg_authid_row_from_values, pg_cast_row_from_values,
    pg_class_row_from_values, pg_collation_row_from_values, pg_constraint_row_from_values,
    pg_database_row_from_values, pg_depend_row_from_values, pg_description_row_from_values,
    pg_index_row_from_values, pg_inherits_row_from_values, pg_language_row_from_values,
    pg_opclass_row_from_values, pg_operator_row_from_values, pg_opfamily_row_from_values,
    pg_proc_row_from_values, pg_rewrite_row_from_values, pg_statistic_row_from_values,
    pg_tablespace_row_from_values, pg_ts_config_map_row_from_values, pg_ts_config_row_from_values,
    pg_ts_dict_row_from_values, pg_ts_parser_row_from_values, pg_ts_template_row_from_values,
    pg_type_row_from_values,
};
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::decode_value;
use crate::backend::executor::value_io::missing_column_value;
use crate::backend::parser::SqlType;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::include::catalog::{
    BootstrapCatalogKind, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow,
    PgClassRow, PgCollationRow, PgConstraintRow, PgIndexRow, PgNamespaceRow, PgOpclassRow,
    PgOpfamilyRow, PgTypeRow, bootstrap_catalog_kinds, bootstrap_pg_auth_members_rows,
    bootstrap_pg_authid_rows, bootstrap_relation_desc,
};
use crate::include::nodes::datum::Value;

use super::store::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};

pub(crate) fn load_catalog_from_physical(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows(base_dir)?;
    catalog_from_physical_rows(base_dir, rows)
}

pub(crate) fn load_catalog_from_visible_physical(
    base_dir: &Path,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_visible(base_dir, pool, txns, snapshot, client_id)?;
    catalog_from_physical_rows(base_dir, rows)
}

pub(crate) fn load_catalog_from_visible_pool(
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &TransactionManager,
    snapshot: &Snapshot,
    client_id: crate::ClientId,
) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows_visible_in_pool(pool, txns, snapshot, client_id)?;
    catalog_from_physical_rows(Path::new(""), rows)
}

pub(crate) fn catalog_from_physical_rows(
    base_dir: &Path,
    rows: PhysicalCatalogRows,
) -> Result<Catalog, CatalogError> {
    let namespace_rows = rows.namespaces;
    let type_rows = rows.types;
    let class_rows = rows.classes;
    let attribute_rows = rows.attributes;
    let attrdef_rows = rows.attrdefs;
    let depend_rows = rows.depends;
    let inherit_rows = rows.inherits;
    let rewrite_rows = rows.rewrites;
    let index_rows = rows.indexes;
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
    let _cast_rows = rows.casts;
    let _collation_rows = rows.collations;
    let _database_rows = rows.databases;
    let _tablespace_rows = rows.tablespaces;

    let namespace_names = namespace_rows
        .iter()
        .map(|row| (row.oid, row.nspname.as_str()))
        .collect::<BTreeMap<_, _>>();
    let type_by_oid = type_rows
        .iter()
        .map(|row| (row.oid, row.sql_type))
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
        );
    let mut catalog = Catalog {
        tables: BTreeMap::new(),
        constraints: Vec::new(),
        depends: Vec::new(),
        inherits: inherit_rows,
        rewrites: Vec::new(),
        authids: authid_rows,
        auth_members: auth_members_rows,
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
                let sql_type = *type_by_oid
                    .get(&attr.atttypid)
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
                desc.dropped = attr.attisdropped;
                if let Some(attrdef) = attrdefs_by_key.get(&(row.oid, attr.attnum)) {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                    desc.default_sequence_oid =
                        crate::pgrust::database::default_sequence_oid_from_default_expr(
                            &attrdef.adbin,
                        );
                    desc.missing_default_value = if desc.default_sequence_oid.is_some() {
                        None
                    } else {
                        crate::backend::parser::derive_literal_default_value(
                            &attrdef.adbin,
                            desc.sql_type,
                        )
                        .ok()
                    };
                } else if let Some(expr) = legacy_default_exprs.get(&(row.oid, attr.attnum)) {
                    desc.default_expr = Some(expr.clone());
                    desc.attrdef_oid = Some(catalog.next_oid);
                    desc.default_sequence_oid =
                        crate::pgrust::database::default_sequence_oid_from_default_expr(expr);
                    desc.missing_default_value = if desc.default_sequence_oid.is_some() {
                        None
                    } else {
                        crate::backend::parser::derive_literal_default_value(expr, desc.sql_type)
                            .ok()
                    };
                    catalog.next_oid = catalog.next_oid.saturating_add(1);
                }
                if let Some(constraint) = not_null_constraints.get(&(row.oid, attr.attnum)) {
                    desc.not_null_constraint_oid = Some(constraint.oid);
                    desc.not_null_constraint_name = Some(constraint.conname.clone());
                    desc.not_null_constraint_validated = constraint.convalidated;
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
        catalog.insert(
            name,
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: row.relfilenode,
                },
                relation_oid: row.oid,
                namespace_oid: row.relnamespace,
                owner_oid: row.relowner,
                row_type_oid: row.reltype,
                reltoastrelid: row.reltoastrelid,
                relpersistence: row.relpersistence,
                relkind: row.relkind,
                relhassubclass: row.relhassubclass,
                relispartition: row.relispartition,
                relpages: row.relpages,
                reltuples: row.reltuples,
                desc: RelationDesc { columns },
                index_meta: indexes_by_relid
                    .get(&row.oid)
                    .map(|index| CatalogIndexMeta {
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
    Ok(catalog)
}

fn restore_missing_first_class_catalog_rows(
    base_dir: &Path,
    rows: &mut PhysicalCatalogRows,
    missing_constraint: bool,
    missing_depend: bool,
) -> Result<(), CatalogError> {
    if missing_constraint {
        let catalog = catalog_from_physical_rows(base_dir, rows.clone())?;
        rows.constraints = catalog
            .entries()
            .filter(|(_, entry)| entry.relkind == 'r')
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
        let catalog = catalog_from_physical_rows(base_dir, rows.clone())?;
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
    let mut missing_statistic = false;
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
            if kind == BootstrapCatalogKind::PgStatistic {
                missing_statistic = true;
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

    let mut rows = PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        depends: depend_rows,
        inherits: inherit_rows,
        descriptions: description_rows,
        indexes: index_rows,
        rewrites: rewrite_rows,
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
        procs: proc_rows,
        casts: cast_rows,
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
    let mut missing_statistic = false;
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
            if kind == BootstrapCatalogKind::PgStatistic {
                missing_statistic = true;
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

    let mut rows = PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        depends: depend_rows,
        inherits: inherit_rows,
        descriptions: description_rows,
        indexes: index_rows,
        rewrites: rewrite_rows,
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
        procs: proc_rows,
        casts: cast_rows,
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
    let rel = RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: kind.relation_oid(),
    };
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
    let mut scan = heap_scan_begin(pool, rel).map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    let attr_descs = desc.attribute_descs();
    let mut rows = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, INVALID_TRANSACTION_ID, &mut scan)
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?
    {
        let raw = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
        let row = desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if let Some(datum) = raw.get(index) {
                    decode_value(column, *datum).map_err(|e| CatalogError::Io(format!("{e:?}")))
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
    let mut scan = heap_scan_begin(pool, rel).map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    let attr_descs = desc.attribute_descs();
    let mut rows = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, client_id, &mut scan)
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?
    {
        if !snapshot.tuple_visible(txns, &tuple) {
            continue;
        }
        let raw = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
        let row = desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if let Some(datum) = raw.get(index) {
                    decode_value(column, *datum).map_err(|e| CatalogError::Io(format!("{e:?}")))
                } else {
                    Ok(missing_column_value(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}
