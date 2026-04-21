use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, PG_CATALOG_NAMESPACE_OID};

pub const BTREE_INTEGER_FAMILY_OID: u32 = 1976;
pub const BTREE_CHAR_FAMILY_OID: u32 = 429;
pub const BTREE_OIDVECTOR_FAMILY_OID: u32 = 1991;
pub const BTREE_TEXT_FAMILY_OID: u32 = 1994;
pub const BTREE_OID_FAMILY_OID: u32 = 1989;
pub const BTREE_BOOL_FAMILY_OID: u32 = 424;
pub const BTREE_NUMERIC_FAMILY_OID: u32 = 1988;
pub const BTREE_BIT_FAMILY_OID: u32 = 423;
pub const BTREE_BYTEA_FAMILY_OID: u32 = 428;
pub const BTREE_DATETIME_FAMILY_OID: u32 = 434;
pub const BTREE_FLOAT_FAMILY_OID: u32 = 1970;
pub const BTREE_VARBIT_FAMILY_OID: u32 = 2002;
pub const BTREE_MULTIRANGE_FAMILY_OID: u32 = 4199;

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
            column_desc("opfname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("opfnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfowner", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_opfamily_rows() -> Vec<PgOpfamilyRow> {
    vec![
        PgOpfamilyRow {
            oid: BTREE_BOOL_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bool_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BIT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bit_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BYTEA_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bytea_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_CHAR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "char_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_DATETIME_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "datetime_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_FLOAT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "float_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_INTEGER_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "integer_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_OIDVECTOR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "oidvector_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_NUMERIC_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "numeric_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_OID_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "oid_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_TEXT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "text_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_VARBIT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "varbit_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_MULTIRANGE_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "multirange_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
    ]
}
