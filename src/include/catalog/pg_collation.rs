use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID};

pub const DEFAULT_COLLATION_OID: u32 = 100;
pub const C_COLLATION_OID: u32 = 950;
pub const POSIX_COLLATION_OID: u32 = 951;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgCollationRow {
    pub oid: u32,
    pub collname: String,
    pub collnamespace: u32,
    pub collowner: u32,
    pub collprovider: char,
    pub collisdeterministic: bool,
    pub collencoding: i32,
}

pub fn pg_collation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("collname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("collnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("collowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("collprovider", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("collisdeterministic", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("collencoding", SqlType::new(SqlTypeKind::Int4), false),
        ],
    }
}

pub fn bootstrap_pg_collation_rows() -> [PgCollationRow; 3] {
    [
        // :HACK: Keep the bootstrap set narrow until pgrust has real locale and
        // ICU support. These rows cover the canonical PostgreSQL built-ins that
        // catalog clients expect to see first.
        PgCollationRow {
            oid: DEFAULT_COLLATION_OID,
            collname: "default".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'd',
            collisdeterministic: true,
            collencoding: -1,
        },
        PgCollationRow {
            oid: C_COLLATION_OID,
            collname: "C".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'c',
            collisdeterministic: true,
            collencoding: -1,
        },
        PgCollationRow {
            oid: POSIX_COLLATION_OID,
            collname: "POSIX".into(),
            collnamespace: PG_CATALOG_NAMESPACE_OID,
            collowner: BOOTSTRAP_SUPERUSER_OID,
            collprovider: 'c',
            collisdeterministic: true,
            collencoding: -1,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_collation_desc_matches_expected_columns() {
        let desc = pg_collation_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
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
            ]
        );
    }
}
