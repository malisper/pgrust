use std::collections::BTreeMap;
use std::path::Path;

use crate::backend::catalog::catalog::Catalog;
use crate::backend::catalog::pg_am::sort_pg_am_rows;
use crate::backend::catalog::pg_attrdef::sort_pg_attrdef_rows;
use crate::backend::catalog::pg_auth_members::sort_pg_auth_members_rows;
use crate::backend::catalog::pg_authid::sort_pg_authid_rows;
use crate::backend::catalog::pg_database::sort_pg_database_rows;
use crate::backend::catalog::pg_depend::{derived_pg_depend_rows, sort_pg_depend_rows};
use crate::backend::catalog::pg_index::sort_pg_index_rows;
use crate::backend::catalog::pg_tablespace::sort_pg_tablespace_rows;
use crate::backend::catalog::store::{DEFAULT_FIRST_USER_OID, load_physical_catalog_rows};
use crate::backend::catalog::pg_attribute::sort_pg_attribute_rows;
use crate::backend::catalog::CatalogError;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID,
    BOOTSTRAP_SUPERUSER_OID, BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID,
    BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID, PgAmRow,
    PgAttrdefRow, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow, PgDatabaseRow,
    PgDependRow, PgIndexRow, PgNamespaceRow, PgTablespaceRow, PgTypeRow, TEXT_ARRAY_TYPE_OID,
    TEXT_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID, VARBIT_ARRAY_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, bootstrap_composite_type_rows,
    bootstrap_pg_am_rows, bootstrap_pg_auth_members_rows, bootstrap_pg_authid_rows,
    bootstrap_pg_database_rows, bootstrap_pg_namespace_rows, bootstrap_pg_tablespace_rows,
    builtin_type_rows,
};

#[derive(Debug, Clone, Default)]
pub struct CatCache {
    namespaces_by_name: BTreeMap<String, PgNamespaceRow>,
    namespaces_by_oid: BTreeMap<u32, PgNamespaceRow>,
    classes_by_name: BTreeMap<String, PgClassRow>,
    classes_by_oid: BTreeMap<u32, PgClassRow>,
    attributes_by_relid: BTreeMap<u32, Vec<PgAttributeRow>>,
    attrdefs_by_key: BTreeMap<(u32, i16), PgAttrdefRow>,
    depend_rows: Vec<PgDependRow>,
    index_rows: Vec<PgIndexRow>,
    am_rows: Vec<PgAmRow>,
    authid_rows: Vec<PgAuthIdRow>,
    auth_members_rows: Vec<PgAuthMembersRow>,
    database_rows: Vec<PgDatabaseRow>,
    tablespace_rows: Vec<PgTablespaceRow>,
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
        cache.am_rows.extend(bootstrap_pg_am_rows());
        sort_pg_am_rows(&mut cache.am_rows);
        cache.authid_rows.extend(bootstrap_pg_authid_rows());
        sort_pg_authid_rows(&mut cache.authid_rows);
        cache
            .auth_members_rows
            .extend(bootstrap_pg_auth_members_rows());
        sort_pg_auth_members_rows(&mut cache.auth_members_rows);
        cache.database_rows.extend(bootstrap_pg_database_rows());
        sort_pg_database_rows(&mut cache.database_rows);
        cache.tablespace_rows.extend(bootstrap_pg_tablespace_rows());
        sort_pg_tablespace_rows(&mut cache.tablespace_rows);

        for (name, entry) in catalog.entries() {
            if let Some((namespace, _)) = name.split_once('.')
                && !cache
                    .namespaces_by_oid
                    .contains_key(&entry.namespace_oid)
            {
                let namespace_row = PgNamespaceRow {
                    oid: entry.namespace_oid,
                    nspname: namespace.to_string(),
                    nspowner: BOOTSTRAP_SUPERUSER_OID,
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
                relowner: BOOTSTRAP_SUPERUSER_OID,
                relam: crate::include::catalog::relam_for_relkind(entry.relkind),
                relfilenode: entry.rel.rel_number,
                relpersistence: 'p',
                relkind: entry.relkind,
            };
            cache
                .classes_by_name
                .insert(normalize_catalog_name(name).to_ascii_lowercase(), class_row.clone());
            cache.classes_by_oid.insert(class_row.oid, class_row);

            if entry.row_type_oid != 0 {
                let composite_type = PgTypeRow {
                    oid: entry.row_type_oid,
                    typname: relname.to_string(),
                    typnamespace: entry.namespace_oid,
                    typowner: BOOTSTRAP_SUPERUSER_OID,
                    typrelid: entry.relation_oid,
                    sql_type: SqlType::new(SqlTypeKind::Text),
                };
                cache
                    .types_by_name
                    .insert(relname.to_ascii_lowercase(), composite_type.clone());
                cache.types_by_oid.insert(composite_type.oid, composite_type);
            }

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

            let mut attrdefs = entry
                .desc
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    Some(PgAttrdefRow {
                        oid: column.attrdef_oid?,
                        adrelid: entry.relation_oid,
                        adnum: idx.saturating_add(1) as i16,
                        adbin: column.default_expr.clone()?,
                    })
                })
                .collect::<Vec<_>>();
            sort_pg_attrdef_rows(&mut attrdefs);
            for row in attrdefs {
                cache.attrdefs_by_key.insert((row.adrelid, row.adnum), row);
            }

            if entry.relation_oid >= DEFAULT_FIRST_USER_OID {
                cache.depend_rows.extend(derived_pg_depend_rows(entry));
            }

            if let Some(index_meta) = &entry.index_meta {
                cache.index_rows.push(PgIndexRow {
                    indexrelid: entry.relation_oid,
                    indrelid: index_meta.indrelid,
                    indnatts: index_meta.indkey.len() as i16,
                    indnkeyatts: index_meta.indkey.len() as i16,
                    indisunique: index_meta.indisunique,
                    indisvalid: true,
                    indisready: true,
                    indislive: true,
                    indkey: format_indkey(&index_meta.indkey),
                });
            }
        }

        sort_pg_depend_rows(&mut cache.depend_rows);
        sort_pg_index_rows(&mut cache.index_rows);

        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let rows = load_physical_catalog_rows(base_dir)?;
        Ok(Self::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.indexes,
            rows.ams,
            rows.authids,
            rows.auth_members,
            rows.databases,
            rows.tablespaces,
            rows.types,
        ))
    }

    pub fn from_rows(
        namespace_rows: Vec<PgNamespaceRow>,
        class_rows: Vec<PgClassRow>,
        attribute_rows: Vec<PgAttributeRow>,
        attrdef_rows: Vec<PgAttrdefRow>,
        depend_rows: Vec<PgDependRow>,
        index_rows: Vec<PgIndexRow>,
        am_rows: Vec<PgAmRow>,
        authid_rows: Vec<PgAuthIdRow>,
        auth_members_rows: Vec<PgAuthMembersRow>,
        database_rows: Vec<PgDatabaseRow>,
        tablespace_rows: Vec<PgTablespaceRow>,
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
        let mut attrdefs = attrdef_rows;
        sort_pg_attrdef_rows(&mut attrdefs);
        for row in attrdefs {
            cache.attrdefs_by_key.insert((row.adrelid, row.adnum), row);
        }
        cache.depend_rows = depend_rows;
        sort_pg_depend_rows(&mut cache.depend_rows);
        cache.index_rows = index_rows;
        sort_pg_index_rows(&mut cache.index_rows);
        cache.am_rows = am_rows;
        sort_pg_am_rows(&mut cache.am_rows);
        cache.authid_rows = authid_rows;
        sort_pg_authid_rows(&mut cache.authid_rows);
        cache.auth_members_rows = auth_members_rows;
        sort_pg_auth_members_rows(&mut cache.auth_members_rows);
        cache.database_rows = database_rows;
        sort_pg_database_rows(&mut cache.database_rows);
        cache.tablespace_rows = tablespace_rows;
        sort_pg_tablespace_rows(&mut cache.tablespace_rows);
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

    pub fn attrdef_by_relid_attnum(&self, relid: u32, attnum: i16) -> Option<&PgAttrdefRow> {
        self.attrdefs_by_key.get(&(relid, attnum))
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

    pub fn attrdef_rows(&self) -> Vec<PgAttrdefRow> {
        self.attrdefs_by_key.values().cloned().collect()
    }

    pub fn depend_rows(&self) -> Vec<PgDependRow> {
        self.depend_rows.clone()
    }

    pub fn index_rows(&self) -> Vec<PgIndexRow> {
        self.index_rows.clone()
    }

    pub fn am_rows(&self) -> Vec<PgAmRow> {
        self.am_rows.clone()
    }

    pub fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.authid_rows.clone()
    }

    pub fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.auth_members_rows.clone()
    }

    pub fn database_rows(&self) -> Vec<PgDatabaseRow> {
        self.database_rows.clone()
    }

    pub fn tablespace_rows(&self) -> Vec<PgTablespaceRow> {
        self.tablespace_rows.clone()
    }
}
pub fn normalize_catalog_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

pub fn format_indkey(indkey: &[i16]) -> String {
    indkey
        .iter()
        .map(|attnum| attnum.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn catalog_object_name(name: &str) -> &str {
    name.rsplit_once('.').map(|(_, object)| object).unwrap_or(name)
}

pub fn sql_type_oid(sql_type: SqlType) -> u32 {
    match (sql_type.kind, sql_type.is_array) {
        (SqlTypeKind::Bool, false) => BOOL_TYPE_OID,
        (SqlTypeKind::Bool, true) => BOOL_ARRAY_TYPE_OID,
        (SqlTypeKind::Bit, false) => BIT_TYPE_OID,
        (SqlTypeKind::Bit, true) => BIT_ARRAY_TYPE_OID,
        (SqlTypeKind::VarBit, false) => VARBIT_TYPE_OID,
        (SqlTypeKind::VarBit, true) => VARBIT_ARRAY_TYPE_OID,
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
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_NAME, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, CURRENT_DATABASE_NAME,
        DEFAULT_TABLESPACE_OID, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
        HEAP_TABLE_AM_OID, PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID,
        PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID, PUBLIC_NAMESPACE_OID,
    };
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
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("name", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'anon'".into());
        let entry = store
            .create_table(
                "people",
                desc,
            )
            .unwrap();
        let index = store
            .create_index("people_name_idx", "people", true, &["name".into()])
            .unwrap();

        let cache = CatCache::from_physical(&base).unwrap();
        assert_eq!(cache.class_by_name("people").map(|row| row.oid), Some(entry.relation_oid));
        assert_eq!(cache.attributes_by_relid(entry.relation_oid).map(|rows| rows.len()), Some(2));
        assert_eq!(cache.type_by_oid(entry.row_type_oid).map(|row| row.typrelid), Some(entry.relation_oid));
        assert_eq!(
            cache
                .attrdef_by_relid_attnum(entry.relation_oid, 2)
                .map(|row| row.adbin.as_str()),
            Some("'anon'")
        );
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
                && row.deptype == DEPENDENCY_NORMAL
        }));
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_TYPE_RELATION_OID
                && row.objid == entry.row_type_oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.deptype == DEPENDENCY_INTERNAL
        }));
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_ATTRDEF_RELATION_OID
                && row.objid == entry.desc.columns[1].attrdef_oid.unwrap()
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert_eq!(
            cache.class_by_name("people_name_idx").map(|row| row.relkind),
            Some('i')
        );
        assert_eq!(
            cache.class_by_name("people_name_idx").map(|row| row.relam),
            Some(BTREE_AM_OID)
        );
        assert_eq!(
            cache.class_by_name("people_name_idx").map(|row| row.relpersistence),
            Some('p')
        );
        assert_eq!(
            cache.class_by_name("people").map(|row| row.relowner),
            Some(BOOTSTRAP_SUPERUSER_OID)
        );
        assert_eq!(
            cache.class_by_name("people").map(|row| row.relam),
            Some(HEAP_TABLE_AM_OID)
        );
        assert!(cache.database_rows().iter().any(|row| {
            row.oid == 1
                && row.datname == CURRENT_DATABASE_NAME
                && row.dattablespace == DEFAULT_TABLESPACE_OID
        }));
        assert!(cache.authid_rows().iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID
                && row.rolname == BOOTSTRAP_SUPERUSER_NAME
                && row.rolsuper
        }));
        assert!(cache.auth_members_rows().is_empty());
        assert!(cache.tablespace_rows().iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(cache.index_rows().iter().any(|row| {
            row.indexrelid == index.relation_oid
                && row.indrelid == entry.relation_oid
                && row.indisunique
                && row.indkey == "2"
        }));
        assert!(cache.am_rows().iter().any(|row| row.oid == BTREE_AM_OID && row.amname == "btree"));
    }
}
