use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::catalog::bootstrap::{
    bootstrap_catalog_entry, bootstrap_catalog_kinds, bootstrap_namespace_oid,
};
use crate::backend::executor::{ColumnDesc, RelationDesc, ScalarType};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::access::htup::AttributeAlign;

const DEFAULT_SPC_OID: u32 = 0;
const DEFAULT_DB_OID: u32 = 1;
const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
const DEFAULT_FIRST_USER_OID: u32 = 16_384;
const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub row_type_oid: u32,
    pub relkind: char,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Io(String),
    Corrupt(&'static str),
    TableAlreadyExists(String),
    UnknownTable(String),
    UnknownType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    tables: BTreeMap<String, CatalogEntry>,
    next_rel_number: u32,
    next_oid: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        let mut catalog = Self {
            tables: BTreeMap::new(),
            next_rel_number: DEFAULT_FIRST_REL_NUMBER,
            next_oid: DEFAULT_FIRST_USER_OID,
        };
        catalog.insert_bootstrap_relations();
        catalog
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCatalog {
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

impl Catalog {
    fn insert_bootstrap_relations(&mut self) {
        for kind in bootstrap_catalog_kinds() {
            let entry = bootstrap_catalog_entry(kind);
            self.insert(kind.relation_name(), entry);
        }
    }

    pub fn insert(&mut self, name: impl Into<String>, entry: CatalogEntry) {
        let name = name.into().to_ascii_lowercase();
        self.next_rel_number = self
            .next_rel_number
            .max(entry.rel.rel_number.saturating_add(1));
        self.next_oid = self
            .next_oid
            .max(entry.relation_oid.saturating_add(1))
            .max(entry.row_type_oid.saturating_add(1));
        self.tables.insert(name, entry);
    }

    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &CatalogEntry)> {
        self.tables.iter().map(|(name, entry)| (name.as_str(), entry))
    }

    pub fn next_oid(&self) -> u32 {
        self.next_oid
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into().to_ascii_lowercase();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists(name));
        }

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid: DEFAULT_DB_OID,
                rel_number: self.next_rel_number,
            },
            relation_oid: self.next_oid,
            namespace_oid: bootstrap_namespace_oid(),
            row_type_oid: self.next_oid.saturating_add(1),
            relkind: 'r',
            desc,
        };
        self.next_rel_number = self.next_rel_number.saturating_add(1);
        self.next_oid = self.next_oid.saturating_add(2);
        self.tables.insert(name, entry.clone());
        Ok(entry)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<CatalogEntry, CatalogError> {
        self.tables
            .remove(&name.to_ascii_lowercase())
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))
    }
}

impl DurableCatalog {
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
                    if column.storage.nullable {
                        "null"
                    } else {
                        "not_null"
                    },
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

pub fn column_desc(name: impl Into<String>, sql_type: SqlType, nullable: bool) -> ColumnDesc {
    let name = name.into();
    let ty = scalar_type_for_sql_type(sql_type);
    let (attlen, attalign) = match ty {
        ScalarType::Int16 => (2, AttributeAlign::Short),
        ScalarType::Int32 => (4, AttributeAlign::Int),
        ScalarType::Int64 => (8, AttributeAlign::Double),
        ScalarType::Bytea => (-1, AttributeAlign::Int),
        ScalarType::Float32 => (4, AttributeAlign::Int),
        ScalarType::Float64 => (8, AttributeAlign::Double),
        ScalarType::Numeric => (-1, AttributeAlign::Int),
        ScalarType::Json | ScalarType::Jsonb | ScalarType::JsonPath => (-1, AttributeAlign::Int),
        ScalarType::Text => (-1, AttributeAlign::Int),
        ScalarType::Bool => (1, AttributeAlign::Char),
        ScalarType::Array(_) => (-1, AttributeAlign::Int),
    };
    ColumnDesc {
        name: name.clone(),
        storage: crate::include::access::htup::AttributeDesc {
            name,
            attlen,
            attalign,
            nullable,
        },
        ty,
        sql_type,
    }
}

fn scalar_type_for_sql_type(sql_type: SqlType) -> ScalarType {
    if sql_type.is_array {
        return ScalarType::Array(Box::new(scalar_type_for_sql_type(sql_type.element_type())));
    }
    match sql_type.kind {
        SqlTypeKind::Int2 => ScalarType::Int16,
        SqlTypeKind::Int4 => ScalarType::Int32,
        SqlTypeKind::Int8 => ScalarType::Int64,
        SqlTypeKind::Oid => ScalarType::Int32,
        SqlTypeKind::Bytea => ScalarType::Bytea,
        SqlTypeKind::Float4 => ScalarType::Float32,
        SqlTypeKind::Float8 => ScalarType::Float64,
        SqlTypeKind::Numeric => ScalarType::Numeric,
        SqlTypeKind::Json => ScalarType::Json,
        SqlTypeKind::Jsonb => ScalarType::Jsonb,
        SqlTypeKind::JsonPath => ScalarType::JsonPath,
        SqlTypeKind::Text
        | SqlTypeKind::Timestamp
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => {
            ScalarType::Text
        }
        SqlTypeKind::Bool => ScalarType::Bool,
    }
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
    let base = if is_array {
        &name[..name.len() - 2]
    } else {
        name
    };
    let mut sql_type = match base {
        "int2" => SqlType {
            kind: SqlTypeKind::Int2,
            typmod,
            is_array: false,
        },
        "int4" => SqlType {
            kind: SqlTypeKind::Int4,
            typmod,
            is_array: false,
        },
        "int8" => SqlType {
            kind: SqlTypeKind::Int8,
            typmod,
            is_array: false,
        },
        "oid" => SqlType {
            kind: SqlTypeKind::Oid,
            typmod,
            is_array: false,
        },
        "float4" => SqlType {
            kind: SqlTypeKind::Float4,
            typmod,
            is_array: false,
        },
        "float8" => SqlType {
            kind: SqlTypeKind::Float8,
            typmod,
            is_array: false,
        },
        "numeric" => SqlType {
            kind: SqlTypeKind::Numeric,
            typmod,
            is_array: false,
        },
        "json" => SqlType {
            kind: SqlTypeKind::Json,
            typmod,
            is_array: false,
        },
        "jsonb" => SqlType {
            kind: SqlTypeKind::Jsonb,
            typmod,
            is_array: false,
        },
        "jsonpath" => SqlType {
            kind: SqlTypeKind::JsonPath,
            typmod,
            is_array: false,
        },
        "text" => SqlType {
            kind: SqlTypeKind::Text,
            typmod,
            is_array: false,
        },
        "\"char\"" => SqlType {
            kind: SqlTypeKind::InternalChar,
            typmod,
            is_array: false,
        },
        "bool" => SqlType {
            kind: SqlTypeKind::Bool,
            typmod,
            is_array: false,
        },
        "timestamp" => SqlType {
            kind: SqlTypeKind::Timestamp,
            typmod,
            is_array: false,
        },
        "char" => SqlType {
            kind: SqlTypeKind::Char,
            typmod,
            is_array: false,
        },
        "varchar" => SqlType {
            kind: SqlTypeKind::Varchar,
            typmod,
            is_array: false,
        },
        other => return Err(CatalogError::UnknownType(other.to_string())),
    };
    sql_type.is_array = is_array;
    Ok(sql_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pgrust_catalog_{label}_{nanos}"))
    }

    #[test]
    fn durable_catalog_roundtrips() {
        let base = temp_dir("roundtrip");
        let mut store = DurableCatalog::load(&base).unwrap();
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
        assert_eq!(entry.namespace_oid, bootstrap_namespace_oid());
        assert!(entry.relation_oid >= DEFAULT_FIRST_USER_OID);
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let reopened_entry = reopened.catalog().get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
        assert!(reopened_entry.desc.columns[2].storage.nullable);
        let persisted = fs::read_to_string(base.join("catalog").join("schema")).unwrap();
        let control = fs::read(base.join("global").join("pg_control")).unwrap();
        assert!(persisted.contains("table\tpg_class\t"));
        assert!(persisted.contains("table\tpeople\t"));
        assert_eq!(control.len(), 16);
    }

    #[test]
    fn dropped_table_stays_gone_after_persist() {
        let base = temp_dir("drop");
        let mut store = DurableCatalog::load(&base).unwrap();
        store
            .catalog_mut()
            .create_table(
                "widgets",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let _dropped = store.catalog_mut().drop_table("widgets").unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        assert!(reopened.catalog().get("widgets").is_none());
    }

    #[test]
    fn durable_catalog_roundtrips_array_types() {
        let base = temp_dir("array_roundtrip");
        let mut store = DurableCatalog::load(&base).unwrap();
        store
            .catalog_mut()
            .create_table(
                "shipments",
                RelationDesc {
                    columns: vec![
                        column_desc(
                            "tags",
                            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                            true,
                        ),
                        column_desc(
                            "counts",
                            SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
                            true,
                        ),
                    ],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let entry = reopened.catalog().get("shipments").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
        );
        assert_eq!(
            entry.desc.columns[1].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
        );
    }

    #[test]
    fn durable_catalog_roundtrips_varchar_array_typmod() {
        let base = temp_dir("varchar_array_typmod");
        let mut store = DurableCatalog::load(&base).unwrap();
        store
            .catalog_mut()
            .create_table(
                "widgets",
                RelationDesc {
                    columns: vec![column_desc(
                        "codes",
                        SqlType::array_of(SqlType::with_char_len(SqlTypeKind::Varchar, 5)),
                        true,
                    )],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let entry = reopened.catalog().get("widgets").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::array_of(SqlType::with_char_len(SqlTypeKind::Varchar, 5))
        );
    }

    #[test]
    fn durable_catalog_roundtrips_varchar_typmod() {
        let base = temp_dir("varchar_roundtrip");
        let mut store = DurableCatalog::load(&base).unwrap();
        store
            .catalog_mut()
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc(
                        "name",
                        SqlType::with_char_len(SqlTypeKind::Varchar, 5),
                        false,
                    )],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let column = &reopened.catalog().get("people").unwrap().desc.columns[0];
        assert_eq!(
            column.sql_type,
            SqlType::with_char_len(SqlTypeKind::Varchar, 5)
        );
    }

    #[test]
    fn durable_catalog_roundtrips_numeric_types() {
        let base = temp_dir("numeric_roundtrip");
        let mut store = DurableCatalog::load(&base).unwrap();
        store
            .catalog_mut()
            .create_table(
                "metrics",
                RelationDesc {
                    columns: vec![
                        column_desc("amount", SqlType::new(SqlTypeKind::Numeric), false),
                        column_desc(
                            "samples",
                            SqlType::array_of(SqlType::new(SqlTypeKind::Numeric)),
                            true,
                        ),
                    ],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let entry = reopened.catalog().get("metrics").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::new(SqlTypeKind::Numeric)
        );
        assert_eq!(
            entry.desc.columns[1].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Numeric))
        );
    }

    #[test]
    fn durable_catalog_bootstraps_core_catalogs_on_first_load() {
        let base = temp_dir("bootstrap");
        let store = DurableCatalog::load(&base).unwrap();
        for name in ["pg_namespace", "pg_type", "pg_attribute", "pg_class"] {
            assert!(store.catalog().get(name).is_some(), "missing {name}");
        }
        assert_eq!(store.catalog().next_oid(), DEFAULT_FIRST_USER_OID);
    }

    #[test]
    fn durable_catalog_reads_pg_control_style_control_file() {
        let base = temp_dir("control_file");
        let store = DurableCatalog::load(&base).unwrap();
        let control = fs::read(base.join("global").join("pg_control")).unwrap();
        assert_eq!(
            u32::from_le_bytes(control[0..4].try_into().unwrap()),
            CONTROL_FILE_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(control[4..8].try_into().unwrap()),
            store.catalog().next_oid()
        );
        assert_eq!(
            u32::from_le_bytes(control[8..12].try_into().unwrap()),
            DEFAULT_FIRST_REL_NUMBER
        );
        assert_eq!(u32::from_le_bytes(control[12..16].try_into().unwrap()), 1);
    }
}
