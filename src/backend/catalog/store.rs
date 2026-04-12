use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use crate::backend::access::heap::heapam::{heap_flush, heap_insert, heap_scan_begin, heap_scan_next};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::catalog::catalog::{column_desc, Catalog, CatalogEntry, CatalogError};
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::executor::value_io::decode_value;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::include::catalog::{
    BootstrapCatalogKind, PgAttrdefRow, PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow,
    bootstrap_catalog_kinds, bootstrap_composite_type_rows, bootstrap_relation_desc,
    builtin_type_rows,
};
use crate::include::nodes::datum::Value;
use crate::BufferPool;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;
pub(crate) const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub(crate) const DEFAULT_FIRST_USER_OID: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalCatalogRows {
    pub namespaces: Vec<PgNamespaceRow>,
    pub classes: Vec<PgClassRow>,
    pub attributes: Vec<PgAttributeRow>,
    pub attrdefs: Vec<PgAttrdefRow>,
    pub types: Vec<PgTypeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStore {
    base_dir: PathBuf,
    control_path: PathBuf,
}

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
            let mut catalog = load_catalog_from_physical(&base_dir)?;
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

        Ok(Self { base_dir, control_path })
    }

    pub fn catalog_snapshot(&self) -> Result<Catalog, CatalogError> {
        self.catalog_snapshot_with_control()
    }

    pub fn relcache(&self) -> Result<RelCache, CatalogError> {
        RelCache::from_physical(&self.base_dir)
    }

    pub fn relation(&self, name: &str) -> Result<Option<RelCacheEntry>, CatalogError> {
        Ok(self.relcache()?.get_by_name(name).cloned())
    }

    pub fn visible_table_names(&self) -> Result<Vec<String>, CatalogError> {
        let mut names = self
            .relcache()?
            .entries()
            .map(|(name, _)| name.to_string())
            .filter(|name| !name.contains('.'))
            .filter(|name| !name.starts_with("pg_"))
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
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.create_table(name, desc)?;
        self.persist_catalog(&catalog)?;
        Ok(entry)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<CatalogEntry, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.drop_table(name)?;
        self.persist_catalog(&catalog)?;
        Ok(entry)
    }

    fn persist_catalog(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        persist_control_file(
            &self.control_path,
            &CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            },
        )?;
        sync_physical_catalogs(&self.base_dir, catalog)
    }
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
}

#[cfg(test)]
pub(crate) fn sync_catalog_heaps_for_tests(
    base_dir: &Path,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    sync_catalog_rows(base_dir, &rows, 1)
}

impl CatalogStore {
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

fn sync_physical_catalogs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    sync_catalog_rows(base_dir, &rows, 1)
}

pub(crate) fn sync_catalog_rows(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for kind in bootstrap_catalog_kinds() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        };
        smgr.open(rel).map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgNamespace.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
        rows.namespaces
            .iter()
            .cloned()
            .map(namespace_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
        rows.classes.iter().cloned().map(pg_class_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgType.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
        rows.types.iter().cloned().map(pg_type_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAttribute.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
        rows.attributes
            .iter()
            .cloned()
            .map(pg_attribute_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAttrdef.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttrdef),
        rows.attrdefs
            .iter()
            .cloned()
            .map(pg_attrdef_row_values)
            .collect(),
    )?;
    Ok(())
}

fn physical_catalog_rows_from_catcache(catcache: &CatCache) -> PhysicalCatalogRows {
    PhysicalCatalogRows {
        namespaces: catcache.namespace_rows(),
        classes: catcache.class_rows(),
        attributes: catcache.attribute_rows(),
        attrdefs: catcache.attrdef_rows(),
        types: catcache.type_rows(),
    }
}

fn insert_catalog_rows(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
    rows: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    for values in rows {
        let tuple = tuple_from_values(desc, &values)
            .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
        heap_insert(pool, 0, rel, &tuple)
            .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))?;
    }
    let nblocks = pool
        .with_storage_mut(|s| s.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|e| CatalogError::Io(e.to_string()))?;
    for block in 0..nblocks {
        heap_flush(pool, 0, rel, block)
            .map_err(|e| CatalogError::Io(format!("catalog flush failed: {e:?}")))?;
    }
    Ok(())
}

fn namespace_row_values(row: PgNamespaceRow) -> Vec<Value> {
    vec![Value::Int32(row.oid as i32), Value::Text(row.nspname.into())]
}

fn pg_class_row_values(row: PgClassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.relname.into()),
        Value::Int32(row.relnamespace as i32),
        Value::Int32(row.reltype as i32),
        Value::Int32(row.relfilenode as i32),
        Value::Text(row.relkind.to_string().into()),
    ]
}

fn pg_attribute_row_values(row: PgAttributeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.attrelid as i32),
        Value::Text(row.attname.into()),
        Value::Int32(row.atttypid as i32),
        Value::Int16(row.attnum),
        Value::Bool(row.attnotnull),
        Value::Int32(row.atttypmod),
    ]
}

fn pg_type_row_values(row: PgTypeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.typname.into()),
        Value::Int32(row.typnamespace as i32),
        Value::Int32(row.typrelid as i32),
    ]
}

fn pg_attrdef_row_values(row: PgAttrdefRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.adrelid as i32),
        Value::Int16(row.adnum),
        Value::Text(row.adbin.into()),
    ]
}

fn persist_control_file(path: &Path, control: &CatalogControl) -> Result<(), CatalogError> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.next_oid.to_le_bytes());
    bytes.extend_from_slice(&control.next_rel_number.to_le_bytes());
    bytes.extend_from_slice(&(u32::from(control.bootstrap_complete)).to_le_bytes());
    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_catalog_from_physical(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows(base_dir)?;
    let namespace_rows = rows.namespaces;
    let type_rows = rows.types;
    let class_rows = rows.classes;
    let attribute_rows = rows.attributes;
    let attrdef_rows = rows.attrdefs;

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
        );
    let mut catalog = Catalog {
        tables: BTreeMap::new(),
        next_rel_number: DEFAULT_FIRST_REL_NUMBER,
        next_oid,
    };
    for row in class_rows {
        let attrs = attrs_by_relid.get(&row.oid).map(Vec::as_slice).unwrap_or(&[]);
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
                if let Some(attrdef) = attrdefs_by_key.get(&(row.oid, attr.attnum)) {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                } else if let Some(expr) = legacy_default_exprs.get(&(row.oid, attr.attnum)) {
                    desc.default_expr = Some(expr.clone());
                    desc.attrdef_oid = Some(catalog.next_oid);
                    catalog.next_oid = catalog.next_oid.saturating_add(1);
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
                row_type_oid: row.reltype,
                relkind: row.relkind,
                desc: RelationDesc { columns },
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
    Ok(catalog)
}

fn load_legacy_default_exprs(base_dir: &Path) -> Result<BTreeMap<(u32, i16), String>, CatalogError> {
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

pub(crate) fn load_physical_catalog_rows(base_dir: &Path) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    let mut rels = BTreeMap::new();
    let mut missing_attrdef = false;
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
            return Err(CatalogError::Corrupt("missing physical bootstrap catalog"));
        }
        smgr.open(rel).map_err(|e| CatalogError::Io(e.to_string()))?;
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

    Ok(PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        types: type_rows,
    })
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
            .zip(raw.into_iter())
            .map(|(column, datum)| decode_value(column, datum).map_err(|e| CatalogError::Io(format!("{e:?}"))))
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn expect_oid(value: &Value) -> Result<u32, CatalogError> {
    match value {
        Value::Int64(v) => u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value")),
        Value::Int32(v) => u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value")),
        _ => Err(CatalogError::Corrupt("expected oid value")),
    }
}

fn expect_text(value: &Value) -> Result<String, CatalogError> {
    match value {
        Value::Text(text) => Ok(text.to_string()),
        _ => Err(CatalogError::Corrupt("expected text value")),
    }
}

fn expect_bool(value: &Value) -> Result<bool, CatalogError> {
    match value {
        Value::Bool(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected bool value")),
    }
}

fn expect_int16(value: &Value) -> Result<i16, CatalogError> {
    match value {
        Value::Int16(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int2 value")),
    }
}

fn expect_int32(value: &Value) -> Result<i32, CatalogError> {
    match value {
        Value::Int32(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int4 value")),
    }
}

fn namespace_row_from_values(values: Vec<Value>) -> Result<PgNamespaceRow, CatalogError> {
    Ok(PgNamespaceRow {
        oid: expect_oid(&values[0])?,
        nspname: expect_text(&values[1])?,
    })
}

fn pg_class_row_from_values(values: Vec<Value>) -> Result<PgClassRow, CatalogError> {
    let relkind = match &values[5] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty relkind"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected relkind text")),
    };
    Ok(PgClassRow {
        oid: expect_oid(&values[0])?,
        relname: expect_text(&values[1])?,
        relnamespace: expect_oid(&values[2])?,
        reltype: expect_oid(&values[3])?,
        relfilenode: expect_oid(&values[4])?,
        relkind,
    })
}

fn pg_attribute_row_from_values(values: Vec<Value>) -> Result<PgAttributeRow, CatalogError> {
    Ok(PgAttributeRow {
        attrelid: expect_oid(&values[0])?,
        attname: expect_text(&values[1])?,
        atttypid: expect_oid(&values[2])?,
        attnum: expect_int16(&values[3])?,
        attnotnull: expect_bool(&values[4])?,
        atttypmod: expect_int32(&values[5])?,
        sql_type: SqlType::new(SqlTypeKind::Text),
    })
}

fn pg_attrdef_row_from_values(values: Vec<Value>) -> Result<PgAttrdefRow, CatalogError> {
    Ok(PgAttrdefRow {
        oid: expect_oid(&values[0])?,
        adrelid: expect_oid(&values[1])?,
        adnum: expect_int16(&values[2])?,
        adbin: expect_text(&values[3])?,
    })
}

fn pg_type_row_from_values(values: Vec<Value>) -> Result<PgTypeRow, CatalogError> {
    let oid = expect_oid(&values[0])?;
    let typrelid = expect_oid(&values[3])?;
    Ok(PgTypeRow {
        oid,
        typname: expect_text(&values[1])?,
        typnamespace: expect_oid(&values[2])?,
        typrelid,
        sql_type: decode_builtin_sql_type(oid).unwrap_or(SqlType::new(SqlTypeKind::Text)),
    })
}

fn decode_builtin_sql_type(oid: u32) -> Option<SqlType> {
    for row in builtin_type_rows().into_iter().chain(bootstrap_composite_type_rows()) {
        if row.oid == oid {
            return Some(row.sql_type);
        }
    }
    None
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
    use crate::backend::storage::smgr::segment_path;
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
        assert_eq!(entry.desc.columns[0].default_expr.as_deref(), Some("'1001'"));
        assert_eq!(entry.desc.columns[1].default_expr.as_deref(), Some("B'0101'"));
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
        assert_eq!(migrated.desc.columns[1].default_expr.as_deref(), Some("'legacy'"));
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
}
