use std::collections::BTreeMap;

use crate::backend::catalog::catalog::Catalog;
use crate::backend::catalog::pg_attribute::sort_pg_attribute_rows;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOL_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_TYPE_OID, FLOAT4_TYPE_OID, FLOAT8_TYPE_OID,
    INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_TYPE_OID, JSON_TYPE_OID, NUMERIC_TYPE_OID, OID_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow, PUBLIC_NAMESPACE_OID, TEXT_TYPE_OID,
    TIMESTAMP_TYPE_OID, VARCHAR_TYPE_OID,
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

        for row in [
            PgNamespaceRow {
                oid: PG_CATALOG_NAMESPACE_OID,
                nspname: "pg_catalog".into(),
            },
            PgNamespaceRow {
                oid: PUBLIC_NAMESPACE_OID,
                nspname: "public".into(),
            },
        ] {
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

        for (name, entry) in catalog.entries() {
            let normalized = normalize_catalog_name(name);
            let class_row = PgClassRow {
                oid: entry.relation_oid,
                relname: normalized.to_string(),
                relnamespace: entry.namespace_oid,
                reltype: entry.row_type_oid,
                relfilenode: entry.rel.rel_number,
                relkind: entry.relkind,
            };
            cache
                .classes_by_name
                .insert(normalized.to_ascii_lowercase(), class_row.clone());
            cache.classes_by_oid.insert(class_row.oid, class_row);

            let composite_type = PgTypeRow {
                oid: entry.row_type_oid,
                typname: normalized.to_string(),
                typnamespace: entry.namespace_oid,
                typrelid: entry.relation_oid,
                sql_type: SqlType::new(SqlTypeKind::Text),
            };
            cache
                .types_by_name
                .insert(normalized.to_ascii_lowercase(), composite_type.clone());
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

    pub fn namespace_by_name(&self, name: &str) -> Option<&PgNamespaceRow> {
        self.namespaces_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
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

fn builtin_type_rows() -> Vec<PgTypeRow> {
    vec![
        builtin_type_row("bool", BOOL_TYPE_OID, SqlType::new(SqlTypeKind::Bool)),
        builtin_type_row("bytea", BYTEA_TYPE_OID, SqlType::new(SqlTypeKind::Bytea)),
        builtin_type_row("\"char\"", INTERNAL_CHAR_TYPE_OID, SqlType::new(SqlTypeKind::InternalChar)),
        builtin_type_row("int8", INT8_TYPE_OID, SqlType::new(SqlTypeKind::Int8)),
        builtin_type_row("int2", INT2_TYPE_OID, SqlType::new(SqlTypeKind::Int2)),
        builtin_type_row("int4", INT4_TYPE_OID, SqlType::new(SqlTypeKind::Int4)),
        builtin_type_row("text", TEXT_TYPE_OID, SqlType::new(SqlTypeKind::Text)),
        builtin_type_row("oid", OID_TYPE_OID, SqlType::new(SqlTypeKind::Oid)),
        builtin_type_row("float4", FLOAT4_TYPE_OID, SqlType::new(SqlTypeKind::Float4)),
        builtin_type_row("float8", FLOAT8_TYPE_OID, SqlType::new(SqlTypeKind::Float8)),
        builtin_type_row("varchar", VARCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Varchar)),
        builtin_type_row("char", BPCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Char)),
        builtin_type_row("timestamp", TIMESTAMP_TYPE_OID, SqlType::new(SqlTypeKind::Timestamp)),
        builtin_type_row("numeric", NUMERIC_TYPE_OID, SqlType::new(SqlTypeKind::Numeric)),
        builtin_type_row("json", JSON_TYPE_OID, SqlType::new(SqlTypeKind::Json)),
        builtin_type_row("jsonb", JSONB_TYPE_OID, SqlType::new(SqlTypeKind::Jsonb)),
        builtin_type_row("jsonpath", JSONPATH_TYPE_OID, SqlType::new(SqlTypeKind::JsonPath)),
    ]
}

fn builtin_type_row(name: &str, oid: u32, sql_type: SqlType) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typrelid: 0,
        sql_type,
    }
}

pub fn normalize_catalog_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.")
        .or_else(|| name.strip_prefix("public."))
        .unwrap_or(name)
}

pub fn sql_type_oid(sql_type: SqlType) -> u32 {
    if sql_type.is_array {
        return 0;
    }
    match sql_type.kind {
        SqlTypeKind::Bool => BOOL_TYPE_OID,
        SqlTypeKind::Bytea => BYTEA_TYPE_OID,
        SqlTypeKind::InternalChar => INTERNAL_CHAR_TYPE_OID,
        SqlTypeKind::Int8 => INT8_TYPE_OID,
        SqlTypeKind::Int2 => INT2_TYPE_OID,
        SqlTypeKind::Int4 => INT4_TYPE_OID,
        SqlTypeKind::Text => TEXT_TYPE_OID,
        SqlTypeKind::Oid => OID_TYPE_OID,
        SqlTypeKind::Float4 => FLOAT4_TYPE_OID,
        SqlTypeKind::Float8 => FLOAT8_TYPE_OID,
        SqlTypeKind::Varchar => VARCHAR_TYPE_OID,
        SqlTypeKind::Char => BPCHAR_TYPE_OID,
        SqlTypeKind::Timestamp => TIMESTAMP_TYPE_OID,
        SqlTypeKind::Numeric => NUMERIC_TYPE_OID,
        SqlTypeKind::Json => JSON_TYPE_OID,
        SqlTypeKind::Jsonb => JSONB_TYPE_OID,
        SqlTypeKind::JsonPath => JSONPATH_TYPE_OID,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;

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
}
