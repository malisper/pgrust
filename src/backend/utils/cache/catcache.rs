use std::collections::BTreeMap;

use crate::backend::catalog::catalog::Catalog;
use crate::include::catalog::{PgAttributeRow, PgClassRow, PgNamespaceRow};

#[derive(Debug, Clone, Default)]
pub struct CatCache {
    namespaces_by_name: BTreeMap<String, PgNamespaceRow>,
    namespaces_by_oid: BTreeMap<u32, PgNamespaceRow>,
    classes_by_name: BTreeMap<String, PgClassRow>,
    classes_by_oid: BTreeMap<u32, PgClassRow>,
    attributes_by_relid: BTreeMap<u32, Vec<PgAttributeRow>>,
}

impl CatCache {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();

        for row in [
            PgNamespaceRow {
                oid: 11,
                nspname: "pg_catalog".into(),
            },
            PgNamespaceRow {
                oid: 2200,
                nspname: "public".into(),
            },
        ] {
            cache
                .namespaces_by_name
                .insert(row.nspname.to_ascii_lowercase(), row.clone());
            cache.namespaces_by_oid.insert(row.oid, row);
        }

        for (name, entry) in catalog.entries() {
            let class_row = PgClassRow {
                oid: entry.relation_oid,
                relname: name.to_string(),
                relnamespace: entry.namespace_oid,
                reltype: entry.row_type_oid,
                relfilenode: entry.rel.rel_number,
                relkind: entry.relkind,
            };
            cache
                .classes_by_name
                .insert(name.to_ascii_lowercase(), class_row.clone());
            cache.classes_by_oid.insert(class_row.oid, class_row);

            let attrs = entry
                .desc
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid: 0,
                    attnum: idx.saturating_add(1) as i16,
                    attnotnull: !column.storage.nullable,
                    atttypmod: column.sql_type.typmod,
                    sql_type: column.sql_type,
                })
                .collect::<Vec<_>>();
            cache.attributes_by_relid.insert(entry.relation_oid, attrs);
        }

        cache
    }

    pub fn namespace_by_name(&self, name: &str) -> Option<&PgNamespaceRow> {
        self.namespaces_by_name.get(&name.to_ascii_lowercase())
    }

    pub fn class_by_name(&self, name: &str) -> Option<&PgClassRow> {
        self.classes_by_name.get(&name.to_ascii_lowercase())
    }

    pub fn class_by_oid(&self, oid: u32) -> Option<&PgClassRow> {
        self.classes_by_oid.get(&oid)
    }

    pub fn attributes_by_relid(&self, relid: u32) -> Option<&[PgAttributeRow]> {
        self.attributes_by_relid.get(&relid).map(Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

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
}
