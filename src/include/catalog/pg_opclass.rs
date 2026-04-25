use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    ANYARRAYOID, ANYMULTIRANGEOID, BIT_TYPE_OID, BOOL_TYPE_OID, BOOTSTRAP_SUPERUSER_OID,
    BOX_TYPE_OID, BPCHAR_TYPE_OID, BRIN_AM_OID, BRIN_BIT_MINMAX_FAMILY_OID,
    BRIN_BPCHAR_MINMAX_FAMILY_OID, BRIN_BYTEA_MINMAX_FAMILY_OID, BRIN_CHAR_MINMAX_FAMILY_OID,
    BRIN_DATETIME_MINMAX_FAMILY_OID, BRIN_FLOAT_MINMAX_FAMILY_OID, BRIN_INTEGER_MINMAX_FAMILY_OID,
    BRIN_OID_MINMAX_FAMILY_OID, BRIN_TEXT_MINMAX_FAMILY_OID, BRIN_TIME_MINMAX_FAMILY_OID,
    BRIN_TIMETZ_MINMAX_FAMILY_OID, BRIN_VARBIT_MINMAX_FAMILY_OID, BTREE_AM_OID,
    BTREE_ARRAY_FAMILY_OID, BTREE_BIT_FAMILY_OID, BTREE_BOOL_FAMILY_OID, BTREE_BYTEA_FAMILY_OID,
    BTREE_CHAR_FAMILY_OID, BTREE_DATETIME_FAMILY_OID, BTREE_FLOAT_FAMILY_OID,
    BTREE_INTEGER_FAMILY_OID, BTREE_MULTIRANGE_FAMILY_OID, BTREE_NUMERIC_FAMILY_OID,
    BTREE_OID_FAMILY_OID, BTREE_OIDVECTOR_FAMILY_OID, BTREE_TEXT_FAMILY_OID,
    BTREE_VARBIT_FAMILY_OID, BYTEA_TYPE_OID, DATE_TYPE_OID, DATEMULTIRANGE_TYPE_OID,
    FLOAT4_TYPE_OID, FLOAT8_TYPE_OID, GIN_AM_OID, GIN_JSONB_FAMILY_OID, GIST_AM_OID,
    GIST_BOX_FAMILY_OID, GIST_POINT_FAMILY_OID, GIST_RANGE_FAMILY_OID, HASH_AM_OID,
    HASH_BOOL_FAMILY_OID, HASH_BPCHAR_FAMILY_OID, HASH_BYTEA_FAMILY_OID, HASH_CHAR_FAMILY_OID,
    HASH_DATE_FAMILY_OID, HASH_FLOAT_FAMILY_OID, HASH_INTEGER_FAMILY_OID, HASH_NUMERIC_FAMILY_OID,
    HASH_OID_FAMILY_OID, HASH_TEXT_FAMILY_OID, HASH_TIME_FAMILY_OID, HASH_TIMESTAMP_FAMILY_OID,
    HASH_TIMESTAMPTZ_FAMILY_OID, HASH_TIMETZ_FAMILY_OID, INT2_TYPE_OID, INT4_TYPE_OID,
    INT4MULTIRANGE_TYPE_OID, INT4RANGE_TYPE_OID, INT8_TYPE_OID, INT8MULTIRANGE_TYPE_OID,
    INTERNAL_CHAR_TYPE_OID, JSONB_TYPE_OID, NAME_TYPE_OID, NUMERIC_TYPE_OID,
    NUMMULTIRANGE_TYPE_OID, OID_TYPE_OID, OIDVECTOR_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    POINT_TYPE_OID, POLYGON_TYPE_OID, SPGIST_AM_OID, SPGIST_BOX_FAMILY_OID, SPGIST_POLY_FAMILY_OID,
    TEXT_TYPE_OID, TIME_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, TIMETZ_TYPE_OID,
    TSMULTIRANGE_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, VARBIT_TYPE_OID, VARCHAR_TYPE_OID,
};

pub const ARRAY_BTREE_OPCLASS_OID: u32 = 76012;
pub const BOOL_BTREE_OPCLASS_OID: u32 = 424;
pub const INT2_BTREE_OPCLASS_OID: u32 = 1978;
pub const INT4_BTREE_OPCLASS_OID: u32 = 1979;
pub const INT8_BTREE_OPCLASS_OID: u32 = 3124;
pub const OID_BTREE_OPCLASS_OID: u32 = 1989;
pub const CHAR_BTREE_OPCLASS_OID: u32 = 10007;
pub const NAME_BTREE_OPCLASS_OID: u32 = 10028;
pub const OIDVECTOR_BTREE_OPCLASS_OID: u32 = 10032;
pub const TEXT_BTREE_OPCLASS_OID: u32 = 3126;
pub const VARCHAR_BTREE_OPCLASS_OID: u32 = 3127;
pub const BPCHAR_BTREE_OPCLASS_OID: u32 = 3128;
pub const FLOAT4_BTREE_OPCLASS_OID: u32 = 1970;
pub const FLOAT8_BTREE_OPCLASS_OID: u32 = 1971;
pub const NUMERIC_BTREE_OPCLASS_OID: u32 = 1988;
pub const TIMESTAMP_BTREE_OPCLASS_OID: u32 = 3129;
pub const BYTEA_BTREE_OPCLASS_OID: u32 = 10003;
pub const BIT_BTREE_OPCLASS_OID: u32 = 10004;
pub const VARBIT_BTREE_OPCLASS_OID: u32 = 10005;
pub const MULTIRANGE_BTREE_OPCLASS_OID: u32 = 10033;
pub const BOX_GIST_OPCLASS_OID: u32 = 76010;
pub const POINT_GIST_OPCLASS_OID: u32 = 76015;
pub const RANGE_GIST_OPCLASS_OID: u32 = 76011;
pub const BOX_SPGIST_OPCLASS_OID: u32 = 76013;
pub const POLY_SPGIST_OPCLASS_OID: u32 = 76014;
pub const JSONB_GIN_OPCLASS_OID: u32 = 10064;
pub const BYTEA_BRIN_MINMAX_OPCLASS_OID: u32 = 76120;
pub const CHAR_BRIN_MINMAX_OPCLASS_OID: u32 = 76121;
pub const INT2_BRIN_MINMAX_OPCLASS_OID: u32 = 76122;
pub const INT4_BRIN_MINMAX_OPCLASS_OID: u32 = 76123;
pub const INT8_BRIN_MINMAX_OPCLASS_OID: u32 = 76124;
pub const OID_BRIN_MINMAX_OPCLASS_OID: u32 = 76125;
pub const FLOAT4_BRIN_MINMAX_OPCLASS_OID: u32 = 76126;
pub const FLOAT8_BRIN_MINMAX_OPCLASS_OID: u32 = 76127;
pub const TEXT_BRIN_MINMAX_OPCLASS_OID: u32 = 76128;
pub const BPCHAR_BRIN_MINMAX_OPCLASS_OID: u32 = 76129;
pub const TIME_BRIN_MINMAX_OPCLASS_OID: u32 = 76130;
pub const DATE_BRIN_MINMAX_OPCLASS_OID: u32 = 76131;
pub const TIMESTAMP_BRIN_MINMAX_OPCLASS_OID: u32 = 76132;
pub const TIMESTAMPTZ_BRIN_MINMAX_OPCLASS_OID: u32 = 76133;
pub const TIMETZ_BRIN_MINMAX_OPCLASS_OID: u32 = 76134;
pub const BIT_BRIN_MINMAX_OPCLASS_OID: u32 = 76135;
pub const VARBIT_BRIN_MINMAX_OPCLASS_OID: u32 = 76136;
pub const BOOL_HASH_OPCLASS_OID: u32 = 76200;
pub const INT2_HASH_OPCLASS_OID: u32 = 76201;
pub const INT4_HASH_OPCLASS_OID: u32 = 76202;
pub const INT8_HASH_OPCLASS_OID: u32 = 76203;
pub const OID_HASH_OPCLASS_OID: u32 = 76204;
pub const CHAR_HASH_OPCLASS_OID: u32 = 76205;
pub const NAME_HASH_OPCLASS_OID: u32 = 76206;
pub const TEXT_HASH_OPCLASS_OID: u32 = 76207;
pub const VARCHAR_HASH_OPCLASS_OID: u32 = 76208;
pub const BPCHAR_HASH_OPCLASS_OID: u32 = 76209;
pub const FLOAT4_HASH_OPCLASS_OID: u32 = 76210;
pub const FLOAT8_HASH_OPCLASS_OID: u32 = 76211;
pub const NUMERIC_HASH_OPCLASS_OID: u32 = 76212;
pub const TIMESTAMP_HASH_OPCLASS_OID: u32 = 76213;
pub const TIMESTAMPTZ_HASH_OPCLASS_OID: u32 = 76214;
pub const DATE_HASH_OPCLASS_OID: u32 = 76215;
pub const TIME_HASH_OPCLASS_OID: u32 = 76216;
pub const TIMETZ_HASH_OPCLASS_OID: u32 = 76217;
pub const BYTEA_HASH_OPCLASS_OID: u32 = 76218;
pub const INT4RANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;
pub const INT8RANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;
pub const NUMRANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;
pub const DATERANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;
pub const TSRANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;
pub const TSTZRANGE_GIST_OPCLASS_OID: u32 = RANGE_GIST_OPCLASS_OID;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOpclassRow {
    pub oid: u32,
    pub opcmethod: u32,
    pub opcname: String,
    pub opcnamespace: u32,
    pub opcowner: u32,
    pub opcfamily: u32,
    pub opcintype: u32,
    pub opcdefault: bool,
    pub opckeytype: u32,
}

pub fn pg_opclass_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcmethod", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("opcnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcfamily", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcintype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opcdefault", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("opckeytype", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_opclass_rows() -> Vec<PgOpclassRow> {
    vec![
        PgOpclassRow {
            oid: ARRAY_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "array_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_ARRAY_FAMILY_OID,
            opcintype: ANYARRAYOID,
            opcdefault: true,
            opckeytype: 0,
        },
        row(
            BOOL_BTREE_OPCLASS_OID,
            "bool_ops",
            BTREE_BOOL_FAMILY_OID,
            BOOL_TYPE_OID,
        ),
        row(
            INT2_BTREE_OPCLASS_OID,
            "int2_ops",
            BTREE_INTEGER_FAMILY_OID,
            INT2_TYPE_OID,
        ),
        row(
            INT4_BTREE_OPCLASS_OID,
            "int4_ops",
            BTREE_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
        ),
        row(
            INT8_BTREE_OPCLASS_OID,
            "int8_ops",
            BTREE_INTEGER_FAMILY_OID,
            INT8_TYPE_OID,
        ),
        row(
            CHAR_BTREE_OPCLASS_OID,
            "char_ops",
            BTREE_CHAR_FAMILY_OID,
            INTERNAL_CHAR_TYPE_OID,
        ),
        row(
            OID_BTREE_OPCLASS_OID,
            "oid_ops",
            BTREE_OID_FAMILY_OID,
            OID_TYPE_OID,
        ),
        PgOpclassRow {
            oid: NAME_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "name_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_TEXT_FAMILY_OID,
            opcintype: NAME_TYPE_OID,
            opcdefault: true,
            opckeytype: 2275,
        },
        row(
            OIDVECTOR_BTREE_OPCLASS_OID,
            "oidvector_ops",
            BTREE_OIDVECTOR_FAMILY_OID,
            OIDVECTOR_TYPE_OID,
        ),
        row(
            TEXT_BTREE_OPCLASS_OID,
            "text_ops",
            BTREE_TEXT_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        row(
            VARCHAR_BTREE_OPCLASS_OID,
            "varchar_ops",
            BTREE_TEXT_FAMILY_OID,
            VARCHAR_TYPE_OID,
        ),
        row(
            BPCHAR_BTREE_OPCLASS_OID,
            "bpchar_ops",
            BTREE_TEXT_FAMILY_OID,
            BPCHAR_TYPE_OID,
        ),
        row(
            FLOAT4_BTREE_OPCLASS_OID,
            "float4_ops",
            BTREE_FLOAT_FAMILY_OID,
            FLOAT4_TYPE_OID,
        ),
        row(
            FLOAT8_BTREE_OPCLASS_OID,
            "float8_ops",
            BTREE_FLOAT_FAMILY_OID,
            FLOAT8_TYPE_OID,
        ),
        row(
            NUMERIC_BTREE_OPCLASS_OID,
            "numeric_ops",
            BTREE_NUMERIC_FAMILY_OID,
            NUMERIC_TYPE_OID,
        ),
        row(
            TIMESTAMP_BTREE_OPCLASS_OID,
            "timestamp_ops",
            BTREE_DATETIME_FAMILY_OID,
            TIMESTAMP_TYPE_OID,
        ),
        row(
            BYTEA_BTREE_OPCLASS_OID,
            "bytea_ops",
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
        ),
        row(
            BIT_BTREE_OPCLASS_OID,
            "bit_ops",
            BTREE_BIT_FAMILY_OID,
            BIT_TYPE_OID,
        ),
        row(
            VARBIT_BTREE_OPCLASS_OID,
            "varbit_ops",
            BTREE_VARBIT_FAMILY_OID,
            VARBIT_TYPE_OID,
        ),
        PgOpclassRow {
            oid: MULTIRANGE_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "multirange_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_MULTIRANGE_FAMILY_OID,
            opcintype: ANYMULTIRANGEOID,
            opcdefault: true,
            opckeytype: 0,
        },
        gist_row(
            BOX_GIST_OPCLASS_OID,
            "box_ops",
            GIST_BOX_FAMILY_OID,
            BOX_TYPE_OID,
        ),
        gist_row(
            POINT_GIST_OPCLASS_OID,
            "point_ops",
            GIST_POINT_FAMILY_OID,
            POINT_TYPE_OID,
        ),
        // :HACK: PostgreSQL models this as a single anyrange opclass. pgrust does
        // not have an anyrange pseudo-type yet, so keep one catalog row and use
        // the concrete type only as a placeholder for lookup.
        gist_row(
            RANGE_GIST_OPCLASS_OID,
            "range_ops",
            GIST_RANGE_FAMILY_OID,
            INT4RANGE_TYPE_OID,
        ),
        spgist_row(
            BOX_SPGIST_OPCLASS_OID,
            "box_ops",
            SPGIST_BOX_FAMILY_OID,
            BOX_TYPE_OID,
        ),
        PgOpclassRow {
            oid: POLY_SPGIST_OPCLASS_OID,
            opcmethod: SPGIST_AM_OID,
            opcname: "poly_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: SPGIST_POLY_FAMILY_OID,
            opcintype: POLYGON_TYPE_OID,
            opcdefault: true,
            opckeytype: BOX_TYPE_OID,
        },
        PgOpclassRow {
            oid: JSONB_GIN_OPCLASS_OID,
            opcmethod: GIN_AM_OID,
            opcname: "jsonb_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: GIN_JSONB_FAMILY_OID,
            opcintype: JSONB_TYPE_OID,
            opcdefault: true,
            opckeytype: TEXT_TYPE_OID,
        },
        brin_row(
            BYTEA_BRIN_MINMAX_OPCLASS_OID,
            "bytea_minmax_ops",
            BRIN_BYTEA_MINMAX_FAMILY_OID,
            BYTEA_TYPE_OID,
        ),
        brin_row(
            CHAR_BRIN_MINMAX_OPCLASS_OID,
            "char_minmax_ops",
            BRIN_CHAR_MINMAX_FAMILY_OID,
            INTERNAL_CHAR_TYPE_OID,
        ),
        brin_row(
            INT2_BRIN_MINMAX_OPCLASS_OID,
            "int2_minmax_ops",
            BRIN_INTEGER_MINMAX_FAMILY_OID,
            INT2_TYPE_OID,
        ),
        brin_row(
            INT4_BRIN_MINMAX_OPCLASS_OID,
            "int4_minmax_ops",
            BRIN_INTEGER_MINMAX_FAMILY_OID,
            INT4_TYPE_OID,
        ),
        brin_row(
            INT8_BRIN_MINMAX_OPCLASS_OID,
            "int8_minmax_ops",
            BRIN_INTEGER_MINMAX_FAMILY_OID,
            INT8_TYPE_OID,
        ),
        brin_row(
            OID_BRIN_MINMAX_OPCLASS_OID,
            "oid_minmax_ops",
            BRIN_OID_MINMAX_FAMILY_OID,
            OID_TYPE_OID,
        ),
        brin_row(
            FLOAT4_BRIN_MINMAX_OPCLASS_OID,
            "float4_minmax_ops",
            BRIN_FLOAT_MINMAX_FAMILY_OID,
            FLOAT4_TYPE_OID,
        ),
        brin_row(
            FLOAT8_BRIN_MINMAX_OPCLASS_OID,
            "float8_minmax_ops",
            BRIN_FLOAT_MINMAX_FAMILY_OID,
            FLOAT8_TYPE_OID,
        ),
        brin_row(
            TEXT_BRIN_MINMAX_OPCLASS_OID,
            "text_minmax_ops",
            BRIN_TEXT_MINMAX_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        brin_row(
            BPCHAR_BRIN_MINMAX_OPCLASS_OID,
            "bpchar_minmax_ops",
            BRIN_BPCHAR_MINMAX_FAMILY_OID,
            BPCHAR_TYPE_OID,
        ),
        brin_row(
            TIME_BRIN_MINMAX_OPCLASS_OID,
            "time_minmax_ops",
            BRIN_TIME_MINMAX_FAMILY_OID,
            TIME_TYPE_OID,
        ),
        brin_row(
            DATE_BRIN_MINMAX_OPCLASS_OID,
            "date_minmax_ops",
            BRIN_DATETIME_MINMAX_FAMILY_OID,
            DATE_TYPE_OID,
        ),
        brin_row(
            TIMESTAMP_BRIN_MINMAX_OPCLASS_OID,
            "timestamp_minmax_ops",
            BRIN_DATETIME_MINMAX_FAMILY_OID,
            TIMESTAMP_TYPE_OID,
        ),
        brin_row(
            TIMESTAMPTZ_BRIN_MINMAX_OPCLASS_OID,
            "timestamptz_minmax_ops",
            BRIN_DATETIME_MINMAX_FAMILY_OID,
            TIMESTAMPTZ_TYPE_OID,
        ),
        brin_row(
            TIMETZ_BRIN_MINMAX_OPCLASS_OID,
            "timetz_minmax_ops",
            BRIN_TIMETZ_MINMAX_FAMILY_OID,
            TIMETZ_TYPE_OID,
        ),
        brin_row(
            BIT_BRIN_MINMAX_OPCLASS_OID,
            "bit_minmax_ops",
            BRIN_BIT_MINMAX_FAMILY_OID,
            BIT_TYPE_OID,
        ),
        brin_row(
            VARBIT_BRIN_MINMAX_OPCLASS_OID,
            "varbit_minmax_ops",
            BRIN_VARBIT_MINMAX_FAMILY_OID,
            VARBIT_TYPE_OID,
        ),
        hash_row(
            BOOL_HASH_OPCLASS_OID,
            "bool_ops",
            HASH_BOOL_FAMILY_OID,
            BOOL_TYPE_OID,
        ),
        hash_row(
            INT2_HASH_OPCLASS_OID,
            "int2_ops",
            HASH_INTEGER_FAMILY_OID,
            INT2_TYPE_OID,
        ),
        hash_row(
            INT4_HASH_OPCLASS_OID,
            "int4_ops",
            HASH_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
        ),
        hash_row(
            INT8_HASH_OPCLASS_OID,
            "int8_ops",
            HASH_INTEGER_FAMILY_OID,
            INT8_TYPE_OID,
        ),
        hash_row(
            CHAR_HASH_OPCLASS_OID,
            "char_ops",
            HASH_CHAR_FAMILY_OID,
            INTERNAL_CHAR_TYPE_OID,
        ),
        hash_row(
            OID_HASH_OPCLASS_OID,
            "oid_ops",
            HASH_OID_FAMILY_OID,
            OID_TYPE_OID,
        ),
        hash_row(
            NAME_HASH_OPCLASS_OID,
            "name_ops",
            HASH_TEXT_FAMILY_OID,
            NAME_TYPE_OID,
        ),
        hash_row(
            TEXT_HASH_OPCLASS_OID,
            "text_ops",
            HASH_TEXT_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        hash_row(
            VARCHAR_HASH_OPCLASS_OID,
            "varchar_ops",
            HASH_TEXT_FAMILY_OID,
            VARCHAR_TYPE_OID,
        ),
        hash_row(
            BPCHAR_HASH_OPCLASS_OID,
            "bpchar_ops",
            HASH_BPCHAR_FAMILY_OID,
            BPCHAR_TYPE_OID,
        ),
        hash_row(
            FLOAT4_HASH_OPCLASS_OID,
            "float4_ops",
            HASH_FLOAT_FAMILY_OID,
            FLOAT4_TYPE_OID,
        ),
        hash_row(
            FLOAT8_HASH_OPCLASS_OID,
            "float8_ops",
            HASH_FLOAT_FAMILY_OID,
            FLOAT8_TYPE_OID,
        ),
        hash_row(
            NUMERIC_HASH_OPCLASS_OID,
            "numeric_ops",
            HASH_NUMERIC_FAMILY_OID,
            NUMERIC_TYPE_OID,
        ),
        hash_row(
            TIMESTAMP_HASH_OPCLASS_OID,
            "timestamp_ops",
            HASH_TIMESTAMP_FAMILY_OID,
            TIMESTAMP_TYPE_OID,
        ),
        hash_row(
            TIMESTAMPTZ_HASH_OPCLASS_OID,
            "timestamptz_ops",
            HASH_TIMESTAMPTZ_FAMILY_OID,
            TIMESTAMPTZ_TYPE_OID,
        ),
        hash_row(
            DATE_HASH_OPCLASS_OID,
            "date_ops",
            HASH_DATE_FAMILY_OID,
            DATE_TYPE_OID,
        ),
        hash_row(
            TIME_HASH_OPCLASS_OID,
            "time_ops",
            HASH_TIME_FAMILY_OID,
            TIME_TYPE_OID,
        ),
        hash_row(
            TIMETZ_HASH_OPCLASS_OID,
            "timetz_ops",
            HASH_TIMETZ_FAMILY_OID,
            TIMETZ_TYPE_OID,
        ),
        hash_row(
            BYTEA_HASH_OPCLASS_OID,
            "bytea_ops",
            HASH_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
        ),
    ]
}

fn row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: BTREE_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: true,
        opckeytype: 0,
    }
}

fn gist_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: GIST_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: true,
        opckeytype: 0,
    }
}

fn spgist_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: SPGIST_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: true,
        opckeytype: 0,
    }
}

fn brin_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: BRIN_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: true,
        opckeytype: 0,
    }
}

fn hash_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: HASH_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: true,
        opckeytype: 0,
    }
}

pub fn default_btree_opclass_oid(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        BOOL_TYPE_OID => BOOL_BTREE_OPCLASS_OID,
        INT2_TYPE_OID => INT2_BTREE_OPCLASS_OID,
        INT4_TYPE_OID => INT4_BTREE_OPCLASS_OID,
        INT8_TYPE_OID => INT8_BTREE_OPCLASS_OID,
        OID_TYPE_OID => OID_BTREE_OPCLASS_OID,
        INTERNAL_CHAR_TYPE_OID => CHAR_BTREE_OPCLASS_OID,
        NAME_TYPE_OID => NAME_BTREE_OPCLASS_OID,
        OIDVECTOR_TYPE_OID => OIDVECTOR_BTREE_OPCLASS_OID,
        TEXT_TYPE_OID => TEXT_BTREE_OPCLASS_OID,
        VARCHAR_TYPE_OID => VARCHAR_BTREE_OPCLASS_OID,
        BPCHAR_TYPE_OID => BPCHAR_BTREE_OPCLASS_OID,
        FLOAT4_TYPE_OID => FLOAT4_BTREE_OPCLASS_OID,
        FLOAT8_TYPE_OID => FLOAT8_BTREE_OPCLASS_OID,
        NUMERIC_TYPE_OID => NUMERIC_BTREE_OPCLASS_OID,
        TIMESTAMP_TYPE_OID => TIMESTAMP_BTREE_OPCLASS_OID,
        BYTEA_TYPE_OID => BYTEA_BTREE_OPCLASS_OID,
        BIT_TYPE_OID => BIT_BTREE_OPCLASS_OID,
        VARBIT_TYPE_OID => VARBIT_BTREE_OPCLASS_OID,
        INT4MULTIRANGE_TYPE_OID
        | NUMMULTIRANGE_TYPE_OID
        | TSMULTIRANGE_TYPE_OID
        | TSTZMULTIRANGE_TYPE_OID
        | DATEMULTIRANGE_TYPE_OID
        | INT8MULTIRANGE_TYPE_OID => MULTIRANGE_BTREE_OPCLASS_OID,
        _ => return None,
    })
}

pub fn default_hash_opclass_oid(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        BOOL_TYPE_OID => BOOL_HASH_OPCLASS_OID,
        INT2_TYPE_OID => INT2_HASH_OPCLASS_OID,
        INT4_TYPE_OID => INT4_HASH_OPCLASS_OID,
        INT8_TYPE_OID => INT8_HASH_OPCLASS_OID,
        OID_TYPE_OID => OID_HASH_OPCLASS_OID,
        INTERNAL_CHAR_TYPE_OID => CHAR_HASH_OPCLASS_OID,
        NAME_TYPE_OID => NAME_HASH_OPCLASS_OID,
        TEXT_TYPE_OID => TEXT_HASH_OPCLASS_OID,
        VARCHAR_TYPE_OID => VARCHAR_HASH_OPCLASS_OID,
        BPCHAR_TYPE_OID => BPCHAR_HASH_OPCLASS_OID,
        FLOAT4_TYPE_OID => FLOAT4_HASH_OPCLASS_OID,
        FLOAT8_TYPE_OID => FLOAT8_HASH_OPCLASS_OID,
        NUMERIC_TYPE_OID => NUMERIC_HASH_OPCLASS_OID,
        TIMESTAMP_TYPE_OID => TIMESTAMP_HASH_OPCLASS_OID,
        TIMESTAMPTZ_TYPE_OID => TIMESTAMPTZ_HASH_OPCLASS_OID,
        DATE_TYPE_OID => DATE_HASH_OPCLASS_OID,
        TIME_TYPE_OID => TIME_HASH_OPCLASS_OID,
        TIMETZ_TYPE_OID => TIMETZ_HASH_OPCLASS_OID,
        BYTEA_TYPE_OID => BYTEA_HASH_OPCLASS_OID,
        _ => return None,
    })
}
