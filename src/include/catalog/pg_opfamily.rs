use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BRIN_AM_OID, BTREE_AM_OID, GIST_AM_OID,
    PG_CATALOG_NAMESPACE_OID, SPGIST_AM_OID,
};

pub const BTREE_INTEGER_FAMILY_OID: u32 = 1976;
pub const BTREE_ARRAY_FAMILY_OID: u32 = 397;
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
pub const GIST_POINT_FAMILY_OID: u32 = 1029;
pub const GIST_BOX_FAMILY_OID: u32 = 2593;
pub const GIST_POLY_FAMILY_OID: u32 = 2594;
pub const GIST_CIRCLE_FAMILY_OID: u32 = 2595;
pub const GIST_RANGE_FAMILY_OID: u32 = 3919;
pub const SPGIST_BOX_FAMILY_OID: u32 = 4001;
pub const BRIN_BYTEA_MINMAX_FAMILY_OID: u32 = 76100;
pub const BRIN_CHAR_MINMAX_FAMILY_OID: u32 = 76101;
pub const BRIN_INTEGER_MINMAX_FAMILY_OID: u32 = 76102;
pub const BRIN_TEXT_MINMAX_FAMILY_OID: u32 = 76103;
pub const BRIN_OID_MINMAX_FAMILY_OID: u32 = 76104;
pub const BRIN_FLOAT_MINMAX_FAMILY_OID: u32 = 76105;
pub const BRIN_BPCHAR_MINMAX_FAMILY_OID: u32 = 76106;
pub const BRIN_TIME_MINMAX_FAMILY_OID: u32 = 76107;
pub const BRIN_DATETIME_MINMAX_FAMILY_OID: u32 = 76108;
pub const BRIN_TIMETZ_MINMAX_FAMILY_OID: u32 = 76109;
pub const BRIN_BIT_MINMAX_FAMILY_OID: u32 = 76110;
pub const BRIN_VARBIT_MINMAX_FAMILY_OID: u32 = 76111;

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
            oid: BTREE_ARRAY_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "array_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
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
        PgOpfamilyRow {
            oid: GIST_POINT_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "point_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_BOX_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "box_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_POLY_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "poly_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_CIRCLE_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "circle_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_RANGE_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "range_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_BOX_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "box_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_BOX_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "box_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        brin_row(BRIN_BYTEA_MINMAX_FAMILY_OID, "bytea_minmax_ops"),
        brin_row(BRIN_CHAR_MINMAX_FAMILY_OID, "char_minmax_ops"),
        brin_row(BRIN_INTEGER_MINMAX_FAMILY_OID, "integer_minmax_ops"),
        brin_row(BRIN_TEXT_MINMAX_FAMILY_OID, "text_minmax_ops"),
        brin_row(BRIN_OID_MINMAX_FAMILY_OID, "oid_minmax_ops"),
        brin_row(BRIN_FLOAT_MINMAX_FAMILY_OID, "float_minmax_ops"),
        brin_row(BRIN_BPCHAR_MINMAX_FAMILY_OID, "bpchar_minmax_ops"),
        brin_row(BRIN_TIME_MINMAX_FAMILY_OID, "time_minmax_ops"),
        brin_row(BRIN_DATETIME_MINMAX_FAMILY_OID, "datetime_minmax_ops"),
        brin_row(BRIN_TIMETZ_MINMAX_FAMILY_OID, "timetz_minmax_ops"),
        brin_row(BRIN_BIT_MINMAX_FAMILY_OID, "bit_minmax_ops"),
        brin_row(BRIN_VARBIT_MINMAX_FAMILY_OID, "varbit_minmax_ops"),
    ]
}

fn brin_row(oid: u32, name: &str) -> PgOpfamilyRow {
    PgOpfamilyRow {
        oid,
        opfmethod: BRIN_AM_OID,
        opfname: name.into(),
        opfnamespace: PG_CATALOG_NAMESPACE_OID,
        opfowner: BOOTSTRAP_SUPERUSER_OID,
    }
}
