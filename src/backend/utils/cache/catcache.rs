use std::collections::BTreeMap;
use std::path::Path;

use crate::backend::catalog::catalog::Catalog;
use crate::backend::catalog::store::load_physical_catalog_rows;
use crate::backend::catalog::pg_attribute::sort_pg_attribute_rows;
use crate::backend::catalog::CatalogError;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID, BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID,
    BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID,
    PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID,
    TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID,
    bootstrap_composite_type_rows, bootstrap_pg_namespace_rows, builtin_type_rows,
};

#[derive(Debug, Clone, Default)]
pub struct CatCache {
    namespaces_by_name: BTreeMap<String, PgNamespaceRow>,
    namespaces_by_oid: BTreeMap<u32, PgNamespaceRow>,
    classes_by_name: BTreeMap<String, PgClassRow>,
    classes_by_oid: BTreeMap<u32, PgClassRow>,
    attributes_by_relid: BTreeMap<u32, Vec<PgAttributeRow>>,
    types_by_name: BTreeMap<String, PgTypeRow>,
    types_by_oid: BTreeMap<u32, PgTypeRow>,
}

impl CatCache {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();

        for row in bootstrap_pg_namespace_rows() {
            cache
                .namespaces_by_name
                .insert(row.nspname.to_ascii_lowercase(), row.clone());
            cache.namespaces_by_oid.insert(row.oid, row);
        }

        for row in builtin_type_rows() {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }

        for row in bootstrap_composite_type_rows() {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }

        for (name, entry) in catalog.entries() {
            if let Some((namespace, _)) = name.split_once('.')
                && !cache
                    .namespaces_by_oid
                    .contains_key(&entry.namespace_oid)
            {
                let namespace_row = PgNamespaceRow {
                    oid: entry.namespace_oid,
                    nspname: namespace.to_string(),
                };
                cache.namespaces_by_name.insert(
                    namespace_row.nspname.to_ascii_lowercase(),
                    namespace_row.clone(),
                );
                cache.namespaces_by_oid.insert(namespace_row.oid, namespace_row);
            }

            let relname = catalog_object_name(name);
            let class_row = PgClassRow {
                oid: entry.relation_oid,
                relname: relname.to_string(),
                relnamespace: entry.namespace_oid,
                reltype: entry.row_type_oid,
                relfilenode: entry.rel.rel_number,
                relkind: entry.relkind,
            };
            cache
                .classes_by_name
                .insert(normalize_catalog_name(name).to_ascii_lowercase(), class_row.clone());
            cache.classes_by_oid.insert(class_row.oid, class_row);

            let composite_type = PgTypeRow {
                oid: entry.row_type_oid,
                typname: relname.to_string(),
                typnamespace: entry.namespace_oid,
                typrelid: entry.relation_oid,
                sql_type: SqlType::new(SqlTypeKind::Text),
            };
            cache
                .types_by_name
                .insert(relname.to_ascii_lowercase(), composite_type.clone());
            cache.types_by_oid.insert(composite_type.oid, composite_type);

            let mut attrs = entry
                .desc
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid: sql_type_oid(column.sql_type),
                    attnum: idx.saturating_add(1) as i16,
                    attnotnull: !column.storage.nullable,
                    atttypmod: column.sql_type.typmod,
                    sql_type: column.sql_type,
                })
                .collect::<Vec<_>>();
            sort_pg_attribute_rows(&mut attrs);
            cache.attributes_by_relid.insert(entry.relation_oid, attrs);
        }

        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let rows = load_physical_catalog_rows(base_dir)?;
        Ok(Self::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.types,
        ))
    }

    pub fn from_rows(
        namespace_rows: Vec<PgNamespaceRow>,
        class_rows: Vec<PgClassRow>,
        attribute_rows: Vec<PgAttributeRow>,
        type_rows: Vec<PgTypeRow>,
    ) -> Self {
        let mut cache = Self::default();
        for row in namespace_rows {
            cache
                .namespaces_by_name
                .insert(row.nspname.to_ascii_lowercase(), row.clone());
            cache.namespaces_by_oid.insert(row.oid, row);
        }
        for row in type_rows {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }
        for row in class_rows {
            cache
                .classes_by_name
                .insert(row.relname.to_ascii_lowercase(), row.clone());
            cache.classes_by_oid.insert(row.oid, row);
        }
        let mut attrs_by_relid = BTreeMap::<u32, Vec<PgAttributeRow>>::new();
        for row in attribute_rows {
            attrs_by_relid.entry(row.attrelid).or_default().push(row);
        }
        for rows in attrs_by_relid.values_mut() {
            sort_pg_attribute_rows(rows);
        }
        cache.attributes_by_relid = attrs_by_relid;
        cache
    }

    pub fn namespace_by_name(&self, name: &str) -> Option<&PgNamespaceRow> {
        self.namespaces_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn namespace_by_oid(&self, oid: u32) -> Option<&PgNamespaceRow> {
        self.namespaces_by_oid.get(&oid)
    }

    pub fn class_by_name(&self, name: &str) -> Option<&PgClassRow> {
        self.classes_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn class_by_oid(&self, oid: u32) -> Option<&PgClassRow> {
        self.classes_by_oid.get(&oid)
    }

    pub fn attributes_by_relid(&self, relid: u32) -> Option<&[PgAttributeRow]> {
        self.attributes_by_relid.get(&relid).map(Vec::as_slice)
    }

    pub fn type_by_name(&self, name: &str) -> Option<&PgTypeRow> {
        self.types_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn type_by_oid(&self, oid: u32) -> Option<&PgTypeRow> {
        self.types_by_oid.get(&oid)
    }

    pub fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        self.namespaces_by_oid.values().cloned().collect()
    }

    pub fn class_rows(&self) -> Vec<PgClassRow> {
        self.classes_by_oid.values().cloned().collect()
    }

    pub fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        self.attributes_by_relid
            .values()
            .flat_map(|rows| rows.iter().cloned())
            .collect()
    }

    pub fn type_rows(&self) -> Vec<PgTypeRow> {
        self.types_by_oid.values().cloned().collect()
    }
}
pub fn normalize_catalog_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn catalog_object_name(name: &str) -> &str {
    name.rsplit_once('.').map(|(_, object)| object).unwrap_or(name)
}

pub fn sql_type_oid(sql_type: SqlType) -> u32 {
    match (sql_type.kind, sql_type.is_array) {
        (SqlTypeKind::Bool, false) => BOOL_TYPE_OID,
        (SqlTypeKind::Bool, true) => BOOL_ARRAY_TYPE_OID,
        (SqlTypeKind::Bytea, false) => BYTEA_TYPE_OID,
        (SqlTypeKind::Bytea, true) => BYTEA_ARRAY_TYPE_OID,
        (SqlTypeKind::InternalChar, false) => INTERNAL_CHAR_TYPE_OID,
        (SqlTypeKind::InternalChar, true) => INTERNAL_CHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Int8, false) => INT8_TYPE_OID,
        (SqlTypeKind::Int8, true) => INT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2, false) => INT2_TYPE_OID,
        (SqlTypeKind::Int2, true) => INT2_ARRAY_TYPE_OID,
        (SqlTypeKind::Int4, false) => INT4_TYPE_OID,
        (SqlTypeKind::Int4, true) => INT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Text, false) => TEXT_TYPE_OID,
        (SqlTypeKind::Text, true) => TEXT_ARRAY_TYPE_OID,
        (SqlTypeKind::Oid, false) => OID_TYPE_OID,
        (SqlTypeKind::Oid, true) => OID_ARRAY_TYPE_OID,
        (SqlTypeKind::Float4, false) => FLOAT4_TYPE_OID,
        (SqlTypeKind::Float4, true) => FLOAT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Float8, false) => FLOAT8_TYPE_OID,
        (SqlTypeKind::Float8, true) => FLOAT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Varchar, false) => VARCHAR_TYPE_OID,
        (SqlTypeKind::Varchar, true) => VARCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Char, false) => BPCHAR_TYPE_OID,
        (SqlTypeKind::Char, true) => BPCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Timestamp, false) => TIMESTAMP_TYPE_OID,
        (SqlTypeKind::Timestamp, true) => TIMESTAMP_ARRAY_TYPE_OID,
        (SqlTypeKind::Numeric, false) => NUMERIC_TYPE_OID,
        (SqlTypeKind::Numeric, true) => NUMERIC_ARRAY_TYPE_OID,
        (SqlTypeKind::Json, false) => JSON_TYPE_OID,
        (SqlTypeKind::Json, true) => JSON_ARRAY_TYPE_OID,
        (SqlTypeKind::Jsonb, false) => JSONB_TYPE_OID,
        (SqlTypeKind::Jsonb, true) => JSONB_ARRAY_TYPE_OID,
        (SqlTypeKind::JsonPath, false) => JSONPATH_TYPE_OID,
        (SqlTypeKind::JsonPath, true) => JSONPATH_ARRAY_TYPE_OID,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::CatalogStore;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pgrust_{prefix}_{nanos}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn catcache_derives_pg_class_and_pg_attribute_rows() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let cache = CatCache::from_catalog(&catalog);
        assert_eq!(cache.class_by_name("people").map(|row| row.oid), Some(entry.relation_oid));
        assert_eq!(
            cache.attributes_by_relid(entry.relation_oid).map(|rows| rows.len()),
            Some(2)
        );
        assert_eq!(
            cache.namespace_by_name("pg_catalog").map(|row| row.oid),
            Some(11)
        );
    }

    #[test]
    fn catcache_derives_builtin_pg_type_rows() {
        let cache = CatCache::from_catalog(&Catalog::default());
        assert_eq!(cache.type_by_name("int4").map(|row| row.oid), Some(INT4_TYPE_OID));
        assert_eq!(cache.type_by_name("pg_class").map(|row| row.typrelid), Some(1259));
    }

    #[test]
    fn catcache_loads_rows_from_physical_catalogs() {
        let base = temp_dir("catcache_from_physical");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let cache = CatCache::from_physical(&base).unwrap();
        assert_eq!(cache.class_by_name("people").map(|row| row.oid), Some(entry.relation_oid));
        assert_eq!(cache.attributes_by_relid(entry.relation_oid).map(|rows| rows.len()), Some(2));
        assert_eq!(cache.type_by_oid(entry.row_type_oid).map(|row| row.typrelid), Some(entry.relation_oid));
    }
}
