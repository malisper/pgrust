use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, PG_CATALOG_NAMESPACE_OID};

pub const BTREE_INTEGER_FAMILY_OID: u32 = 1976;
pub const BTREE_TEXT_FAMILY_OID: u32 = 1994;
pub const BTREE_OID_FAMILY_OID: u32 = 1989;
pub const BTREE_BOOL_FAMILY_OID: u32 = 424;
pub const BTREE_NUMERIC_FAMILY_OID: u32 = 1988;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOpfamilyRow {
    pub oid: u32,
    pub opfmethod: u32,
    pub opfname: String,
    pub opfnamespace: u32,
    pub opfowner: u32,
}

pub fn pg_opfamily_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfmethod", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("opfnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfowner", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_opfamily_rows() -> Vec<PgOpfamilyRow> {
    vec![
        PgOpfamilyRow { oid: BTREE_BOOL_FAMILY_OID, opfmethod: BTREE_AM_OID, opfname: "bool_ops".into(), opfnamespace: PG_CATALOG_NAMESPACE_OID, opfowner: BOOTSTRAP_SUPERUSER_OID },
        PgOpfamilyRow { oid: BTREE_INTEGER_FAMILY_OID, opfmethod: BTREE_AM_OID, opfname: "integer_ops".into(), opfnamespace: PG_CATALOG_NAMESPACE_OID, opfowner: BOOTSTRAP_SUPERUSER_OID },
        PgOpfamilyRow { oid: BTREE_NUMERIC_FAMILY_OID, opfmethod: BTREE_AM_OID, opfname: "numeric_ops".into(), opfnamespace: PG_CATALOG_NAMESPACE_OID, opfowner: BOOTSTRAP_SUPERUSER_OID },
        PgOpfamilyRow { oid: BTREE_OID_FAMILY_OID, opfmethod: BTREE_AM_OID, opfname: "oid_ops".into(), opfnamespace: PG_CATALOG_NAMESPACE_OID, opfowner: BOOTSTRAP_SUPERUSER_OID },
        PgOpfamilyRow { oid: BTREE_TEXT_FAMILY_OID, opfmethod: BTREE_AM_OID, opfname: "text_ops".into(), opfnamespace: PG_CATALOG_NAMESPACE_OID, opfowner: BOOTSTRAP_SUPERUSER_OID },
    ]
}
