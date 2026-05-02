use crate::desc::column_desc;
use crate::{BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub const DEFAULT_COLLATION_OID: u32 = 100;
pub const PG_C_UTF8_COLLATION_OID: u32 = 811;
pub const C_COLLATION_OID: u32 = 950;
pub const POSIX_COLLATION_OID: u32 = 951;
pub const UCS_BASIC_COLLATION_OID: u32 = 962;
pub const UNICODE_COLLATION_OID: u32 = 963;
pub const PG_UNICODE_FAST_COLLATION_OID: u32 = 6411;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgCollationRow {
    pub oid: u32,
    pub collname: String,
    pub collnamespace: u32,
    pub collowner: u32,
    pub collprovider: char,
    pub collisdeterministic: bool,
    pub collencoding: i32,
    pub collcollate: Option<String>,
    pub collctype: Option<String>,
    pub colllocale: Option<String>,
    pub collicurules: Option<String>,
    pub collversion: Option<String>,
}

pub fn pg_collation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("collname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("collnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("collowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "collprovider",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "collisdeterministic",
                SqlType::new(SqlTypeKind::Bool),
                false,
            ),
            column_desc("collencoding", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("collcollate", SqlType::new(SqlTypeKind::Text), true),
            column_desc("collctype", SqlType::new(SqlTypeKind::Text), true),
            column_desc("colllocale", SqlType::new(SqlTypeKind::Text), true),
            column_desc("collicurules", SqlType::new(SqlTypeKind::Text), true),
            column_desc("collversion", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
}

pub fn bootstrap_pg_collation_rows() -> [PgCollationRow; 7] {
    [
        // :HACK: Keep the bootstrap set narrow until pgrust has real locale and
        // ICU support. Include PostgreSQL's built-in UTF-8 rows so catalog
        // clients and focused collation regressions can resolve their names.
        PgCollationRow {
            oid: DEFAULT_COLLATION_OID,
            collname: "default".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'd',
            collisdeterministic: true,
            collencoding: -1,
            collcollate: None,
            collctype: None,
            colllocale: None,
            collicurules: None,
            collversion: None,
        },
        PgCollationRow {
            oid: C_COLLATION_OID,
            collname: "C".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'c',
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some("C".into()),
            collctype: Some("C".into()),
            colllocale: None,
            collicurules: None,
            collversion: None,
        },
        PgCollationRow {
            oid: POSIX_COLLATION_OID,
            collname: "POSIX".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'c',
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some("POSIX".into()),
            collctype: Some("POSIX".into()),
            colllocale: None,
            collicurules: None,
            collversion: None,
        },
        PgCollationRow {
            oid: UCS_BASIC_COLLATION_OID,
            collname: "ucs_basic".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'b',
            collisdeterministic: true,
            collencoding: 6,
            collcollate: None,
            collctype: None,
            colllocale: Some("C".into()),
            collicurules: None,
            collversion: Some("1".into()),
        },
        PgCollationRow {
            oid: UNICODE_COLLATION_OID,
            collname: "unicode".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'i',
            collisdeterministic: true,
            collencoding: -1,
            collcollate: None,
            collctype: None,
            colllocale: Some("und".into()),
            collicurules: None,
            collversion: None,
        },
        PgCollationRow {
            oid: PG_C_UTF8_COLLATION_OID,
            collname: "pg_c_utf8".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'b',
            collisdeterministic: true,
            collencoding: 6,
            collcollate: None,
            collctype: None,
            colllocale: Some("C.UTF-8".into()),
            collicurules: None,
            collversion: Some("1".into()),
        },
        PgCollationRow {
            oid: PG_UNICODE_FAST_COLLATION_OID,
            collname: "pg_unicode_fast".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'b',
            collisdeterministic: true,
            collencoding: 6,
            collcollate: None,
            collctype: None,
            colllocale: Some("PG_UNICODE_FAST".into()),
            collicurules: None,
            collversion: Some("1".into()),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_collation_desc_matches_expected_columns() {
        let desc = pg_collation_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "collname",
                "collnamespace",
                "collowner",
                "collprovider",
                "collisdeterministic",
                "collencoding",
                "collcollate",
                "collctype",
                "colllocale",
                "collicurules",
                "collversion",
            ]
        );
    }
}
