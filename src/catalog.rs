use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::access::heap::tuple::AttributeAlign;
use crate::executor::{ColumnDesc, RelationDesc, ScalarType};
use crate::storage::smgr::RelFileLocator;

const CATALOG_FORMAT_VERSION: &str = "v1";
const DEFAULT_SPC_OID: u32 = 0;
const DEFAULT_DB_OID: u32 = 1;
const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub rel: RelFileLocator,
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
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            next_rel_number: DEFAULT_FIRST_REL_NUMBER,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCatalog {
    path: PathBuf,
    catalog: Catalog,
}

impl Catalog {
    pub fn insert(&mut self, name: impl Into<String>, entry: CatalogEntry) {
        let name = name.into().to_ascii_lowercase();
        self.next_rel_number = self.next_rel_number.max(entry.rel.rel_number.saturating_add(1));
        self.tables.insert(name, entry);
    }

    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
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
            desc,
        };
        self.next_rel_number = self.next_rel_number.saturating_add(1);
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
        let path = base_dir.into().join("catalog").join("schema");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }

        let catalog = if path.exists() {
            load_catalog_file(&path)?
        } else {
            let catalog = Catalog::default();
            persist_catalog_file(&path, &catalog)?;
            catalog
        };

        Ok(Self { path, catalog })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    pub fn persist(&self) -> Result<(), CatalogError> {
        persist_catalog_file(&self.path, &self.catalog)
    }
}

fn persist_catalog_file(path: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        format!("{CATALOG_FORMAT_VERSION}\t{}\n", catalog.next_rel_number).as_bytes(),
    );

    for (name, entry) in &catalog.tables {
        bytes.extend_from_slice(
            format!(
                "table\t{name}\t{}\t{}\t{}\t{}\n",
                entry.rel.spc_oid,
                entry.rel.db_oid,
                entry.rel.rel_number,
                entry.desc.columns.len()
            )
            .as_bytes(),
        );
        for column in &entry.desc.columns {
            bytes.extend_from_slice(
                format!(
                    "col\t{}\t{}\t{}\n",
                    column.name,
                    encode_scalar_type(column.ty),
                    if column.storage.nullable { "null" } else { "not_null" }
                )
                .as_bytes(),
            );
        }
    }

    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_catalog_file(path: &Path) -> Result<Catalog, CatalogError> {
    let text = fs::read_to_string(path).map_err(|e| CatalogError::Io(e.to_string()))?;
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Err(CatalogError::Corrupt("missing catalog header"));
    };
    let mut header_parts = header.split('\t');
    if header_parts.next() != Some(CATALOG_FORMAT_VERSION) {
        return Err(CatalogError::Corrupt("unknown catalog format version"));
    }
    let next_rel_number = header_parts
        .next()
        .ok_or(CatalogError::Corrupt("missing next rel number"))?
        .parse::<u32>()
        .map_err(|_| CatalogError::Corrupt("invalid next rel number"))?;

    let mut tables = BTreeMap::new();
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
            let ty = decode_scalar_type(
                col_parts
                    .next()
                    .ok_or(CatalogError::Corrupt("missing column type"))?,
            )?;
            let nullable = match col_parts
                .next()
                .ok_or(CatalogError::Corrupt("missing nullable flag"))?
            {
                "null" => true,
                "not_null" => false,
                _ => return Err(CatalogError::Corrupt("invalid nullable flag")),
            };
            columns.push(column_desc(column_name, ty, nullable));
        }

        tables.insert(
            name,
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid,
                    db_oid,
                    rel_number,
                },
                desc: RelationDesc { columns },
            },
        );
    }

    Ok(Catalog {
        tables,
        next_rel_number,
    })
}

fn parse_u32(part: Option<&str>, err: &'static str) -> Result<u32, CatalogError> {
    part.ok_or(CatalogError::Corrupt(err))?
        .parse::<u32>()
        .map_err(|_| CatalogError::Corrupt(err))
}

pub fn column_desc(name: impl Into<String>, ty: ScalarType, nullable: bool) -> ColumnDesc {
    let name = name.into();
    let (attlen, attalign) = match ty {
        ScalarType::Int32 => (4, AttributeAlign::Int),
        ScalarType::Text => (-1, AttributeAlign::Int),
        ScalarType::Bool => (1, AttributeAlign::Char),
    };
    ColumnDesc {
        name: name.clone(),
        storage: crate::access::heap::tuple::AttributeDesc {
            name,
            attlen,
            attalign,
            nullable,
        },
        ty,
    }
}

fn encode_scalar_type(ty: ScalarType) -> &'static str {
    match ty {
        ScalarType::Int32 => "int4",
        ScalarType::Text => "text",
        ScalarType::Bool => "bool",
    }
}

fn decode_scalar_type(name: &str) -> Result<ScalarType, CatalogError> {
    match name {
        "int4" => Ok(ScalarType::Int32),
        "text" => Ok(ScalarType::Text),
        "bool" => Ok(ScalarType::Bool),
        other => Err(CatalogError::UnknownType(other.to_string())),
    }
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
        let entry = store
            .catalog_mut()
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", ScalarType::Int32, false),
                        column_desc("name", ScalarType::Text, false),
                        column_desc("note", ScalarType::Text, true),
                    ],
                },
            )
            .unwrap();
        assert_eq!(entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        let reopened_entry = reopened.catalog().get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
        assert!(reopened_entry.desc.columns[2].storage.nullable);
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
                    columns: vec![column_desc("id", ScalarType::Int32, false)],
                },
            )
            .unwrap();
        store.persist().unwrap();

        let _dropped = store.catalog_mut().drop_table("widgets").unwrap();
        store.persist().unwrap();

        let reopened = DurableCatalog::load(&base).unwrap();
        assert!(reopened.catalog().get("widgets").is_none());
    }
}
