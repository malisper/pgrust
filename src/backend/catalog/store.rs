use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::access::heap::heapam::{heap_flush, heap_insert};
use crate::backend::catalog::catalog::{column_desc, Catalog, CatalogEntry, CatalogError};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::include::catalog::{
    BootstrapCatalogKind, PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow,
    bootstrap_catalog_kinds, bootstrap_composite_type_rows, bootstrap_pg_attribute_rows,
    bootstrap_pg_class_rows, bootstrap_pg_namespace_rows, bootstrap_relation_desc,
    builtin_type_rows,
};
use crate::include::nodes::datum::Value;
use crate::BufferPool;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;
pub(crate) const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub(crate) const DEFAULT_FIRST_USER_OID: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStore {
    schema_path: PathBuf,
    control_path: PathBuf,
    catalog: Catalog,
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
        let catalog_dir = base_dir.join("catalog");
        let global_dir = base_dir.join("global");
        let schema_path = catalog_dir.join("schema");
        let control_path = global_dir.join("pg_control");
        if let Some(parent) = schema_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }
        if let Some(parent) = control_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }

        let (mut catalog, control) = if control_path.exists() {
            let control = load_control_file(&control_path)?;
            let mut catalog = if schema_path.exists() {
                load_catalog_file(&schema_path)?
            } else {
                Catalog::default()
            };
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
            bootstrap_physical_catalogs(&base_dir, &catalog)?;
            persist_control_file(&control_path, &control)?;
            persist_catalog_file(&schema_path, &catalog)?;
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

        Ok(Self {
            schema_path,
            control_path,
            catalog,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    pub fn persist(&self) -> Result<(), CatalogError> {
        persist_control_file(
            &self.control_path,
            &CatalogControl {
                next_oid: self.catalog.next_oid,
                next_rel_number: self.catalog.next_rel_number,
                bootstrap_complete: true,
            },
        )?;
        persist_catalog_file(&self.schema_path, &self.catalog)
    }
}

fn bootstrap_physical_catalogs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for kind in bootstrap_catalog_kinds() {
        let rel = catalog
            .get(kind.relation_name())
            .ok_or(CatalogError::Corrupt("missing bootstrap catalog entry"))?
            .rel;
        smgr.open(rel).map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    insert_catalog_rows(
        &pool,
        catalog
            .get("pg_namespace")
            .ok_or(CatalogError::Corrupt("missing pg_namespace"))?,
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
        bootstrap_pg_namespace_rows()
            .into_iter()
            .map(namespace_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        catalog
            .get("pg_class")
            .ok_or(CatalogError::Corrupt("missing pg_class"))?,
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
        bootstrap_pg_class_rows()
            .into_iter()
            .map(pg_class_row_values)
            .collect(),
    )?;
    let mut type_rows = builtin_type_rows();
    type_rows.extend(bootstrap_composite_type_rows());
    insert_catalog_rows(
        &pool,
        catalog
            .get("pg_type")
            .ok_or(CatalogError::Corrupt("missing pg_type"))?,
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
        type_rows.into_iter().map(pg_type_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        catalog
            .get("pg_attribute")
            .ok_or(CatalogError::Corrupt("missing pg_attribute"))?,
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
        bootstrap_pg_attribute_rows()
            .into_iter()
            .map(pg_attribute_row_values)
            .collect(),
    )?;
    Ok(())
}

fn insert_catalog_rows(
    pool: &BufferPool<SmgrStorageBackend>,
    entry: &CatalogEntry,
    desc: &RelationDesc,
    rows: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    for values in rows {
        let tuple = tuple_from_values(desc, &values)
            .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
        heap_insert(pool, 0, entry.rel, &tuple)
            .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))?;
    }
    let nblocks = pool
        .with_storage_mut(|s| s.smgr.nblocks(entry.rel, ForkNumber::Main))
        .map_err(|e| CatalogError::Io(e.to_string()))?;
    for block in 0..nblocks {
        heap_flush(pool, 0, entry.rel, block)
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

fn persist_catalog_file(path: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let mut bytes = Vec::new();
    for (name, entry) in &catalog.tables {
        bytes.extend_from_slice(
            format!(
                "table\t{name}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                entry.rel.spc_oid,
                entry.rel.db_oid,
                entry.rel.rel_number,
                entry.relation_oid,
                entry.namespace_oid,
                entry.row_type_oid,
                entry.relkind,
                entry.desc.columns.len()
            )
            .as_bytes(),
        );
        for column in &entry.desc.columns {
            bytes.extend_from_slice(
                format!(
                    "col\t{}\t{}\t{}\t{}\n",
                    column.name,
                    encode_sql_type(column.sql_type),
                    if column.storage.nullable { "null" } else { "not_null" },
                    column.sql_type.typmod,
                )
                .as_bytes(),
            );
        }
    }

    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn persist_control_file(path: &Path, control: &CatalogControl) -> Result<(), CatalogError> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.next_oid.to_le_bytes());
    bytes.extend_from_slice(&control.next_rel_number.to_le_bytes());
    bytes.extend_from_slice(&(u32::from(control.bootstrap_complete)).to_le_bytes());
    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_catalog_file(path: &Path) -> Result<Catalog, CatalogError> {
    let text = fs::read_to_string(path).map_err(|e| CatalogError::Io(e.to_string()))?;
    let mut catalog = Catalog::default();
    catalog.tables.clear();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        let mut parts = line.split('\t');
        if parts.next() != Some("table") {
            return Err(CatalogError::Corrupt("expected table record"));
        }
        let name = parts
            .next()
            .ok_or(CatalogError::Corrupt("missing table name"))?
            .to_string();
        let spc_oid = parse_u32(parts.next(), "invalid spc oid")?;
        let db_oid = parse_u32(parts.next(), "invalid db oid")?;
        let rel_number = parse_u32(parts.next(), "invalid rel number")?;
        let relation_oid = parse_u32(parts.next(), "invalid relation oid")?;
        let namespace_oid = parse_u32(parts.next(), "invalid namespace oid")?;
        let row_type_oid = parse_u32(parts.next(), "invalid row type oid")?;
        let relkind = parts
            .next()
            .and_then(|value| value.chars().next())
            .ok_or(CatalogError::Corrupt("invalid relkind"))?;
        let ncols = parse_u32(parts.next(), "invalid column count")? as usize;

        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let col_line = lines
                .next()
                .ok_or(CatalogError::Corrupt("missing column record"))?;
            let mut col_parts = col_line.split('\t');
            if col_parts.next() != Some("col") {
                return Err(CatalogError::Corrupt("expected column record"));
            }
            let column_name = col_parts
                .next()
                .ok_or(CatalogError::Corrupt("missing column name"))?
                .to_string();
            let type_name = col_parts
                .next()
                .ok_or(CatalogError::Corrupt("missing column type"))?;
            let nullable = match col_parts
                .next()
                .ok_or(CatalogError::Corrupt("missing nullable flag"))?
            {
                "null" => true,
                "not_null" => false,
                _ => return Err(CatalogError::Corrupt("invalid nullable flag")),
            };
            let typmod = col_parts
                .next()
                .ok_or(CatalogError::Corrupt("missing typmod"))?
                .parse::<i32>()
                .map_err(|_| CatalogError::Corrupt("invalid typmod"))?;
            let sql_type = decode_sql_type(type_name, typmod)?;
            columns.push(column_desc(column_name, sql_type, nullable));
        }

        catalog.insert(
            name,
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid,
                    db_oid,
                    rel_number,
                },
                relation_oid,
                namespace_oid,
                row_type_oid,
                relkind,
                desc: RelationDesc { columns },
            },
        );
    }
    Ok(catalog)
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

fn parse_u32(part: Option<&str>, err: &'static str) -> Result<u32, CatalogError> {
    part.ok_or(CatalogError::Corrupt(err))?
        .parse::<u32>()
        .map_err(|_| CatalogError::Corrupt(err))
}

fn encode_sql_type(sql_type: SqlType) -> String {
    let base = match sql_type.kind {
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Int8 => "int8",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Float4 => "float4",
        SqlTypeKind::Float8 => "float8",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "bool",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "char",
        SqlTypeKind::Varchar => "varchar",
    };
    if sql_type.is_array {
        format!("{base}[]")
    } else {
        base.to_string()
    }
}

fn decode_sql_type(name: &str, typmod: i32) -> Result<SqlType, CatalogError> {
    let is_array = name.ends_with("[]");
    let base = if is_array { &name[..name.len() - 2] } else { name };
    let mut sql_type = match base {
        "int2" => SqlType { kind: SqlTypeKind::Int2, typmod, is_array: false },
        "int4" => SqlType { kind: SqlTypeKind::Int4, typmod, is_array: false },
        "int8" => SqlType { kind: SqlTypeKind::Int8, typmod, is_array: false },
        "oid" => SqlType { kind: SqlTypeKind::Oid, typmod, is_array: false },
        "float4" => SqlType { kind: SqlTypeKind::Float4, typmod, is_array: false },
        "float8" => SqlType { kind: SqlTypeKind::Float8, typmod, is_array: false },
        "numeric" => SqlType { kind: SqlTypeKind::Numeric, typmod, is_array: false },
        "json" => SqlType { kind: SqlTypeKind::Json, typmod, is_array: false },
        "jsonb" => SqlType { kind: SqlTypeKind::Jsonb, typmod, is_array: false },
        "jsonpath" => SqlType { kind: SqlTypeKind::JsonPath, typmod, is_array: false },
        "text" => SqlType { kind: SqlTypeKind::Text, typmod, is_array: false },
        "\"char\"" => SqlType { kind: SqlTypeKind::InternalChar, typmod, is_array: false },
        "bool" => SqlType { kind: SqlTypeKind::Bool, typmod, is_array: false },
        "timestamp" => SqlType { kind: SqlTypeKind::Timestamp, typmod, is_array: false },
        "char" => SqlType { kind: SqlTypeKind::Char, typmod, is_array: false },
        "varchar" => SqlType { kind: SqlTypeKind::Varchar, typmod, is_array: false },
        other => return Err(CatalogError::UnknownType(other.to_string())),
    };
    sql_type.is_array = is_array;
    Ok(sql_type)
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
        assert!(store.catalog().get("pg_class").is_some());
        let entry = store
            .catalog_mut()
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
        store.persist().unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_entry = reopened.catalog().get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
    }

    #[test]
    fn catalog_store_bootstraps_physical_core_catalog_relfiles() {
        let base = temp_dir("physical_bootstrap");
        let store = CatalogStore::load(&base).unwrap();
        for name in ["pg_namespace", "pg_type", "pg_attribute", "pg_class"] {
            let entry = store.catalog().get(name).unwrap();
            let path = segment_path(&base, entry.rel, ForkNumber::Main, 0);
            let meta = fs::metadata(path).unwrap();
            assert!(meta.len() > 0, "{name} should have heap data");
        }
    }
}
