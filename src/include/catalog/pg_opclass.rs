use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::*;

pub const ARRAY_BTREE_OPCLASS_OID: u32 = 76012;
pub const BOOL_BTREE_OPCLASS_OID: u32 = 424;
pub const INT2_BTREE_OPCLASS_OID: u32 = 1979;
pub const INT4_BTREE_OPCLASS_OID: u32 = 1978;
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
pub const INTERVAL_BTREE_OPCLASS_OID: u32 = 10036;
pub const TIMESTAMP_BTREE_OPCLASS_OID: u32 = 3129;
pub const TIMESTAMPTZ_BTREE_OPCLASS_OID: u32 = 3130;
pub const BYTEA_BTREE_OPCLASS_OID: u32 = 10003;
pub const UUID_BTREE_OPCLASS_OID: u32 = 2969;
pub const BIT_BTREE_OPCLASS_OID: u32 = 10004;
pub const VARBIT_BTREE_OPCLASS_OID: u32 = 10005;
pub const MACADDR_BTREE_OPCLASS_OID: u32 = 76042;
pub const MACADDR8_BTREE_OPCLASS_OID: u32 = 76043;
pub const MULTIRANGE_BTREE_OPCLASS_OID: u32 = 10033;
pub const CIDR_BTREE_OPCLASS_OID: u32 = 10034;
pub const INET_BTREE_OPCLASS_OID: u32 = 10035;
pub const ENUM_BTREE_OPCLASS_OID: u32 = 76170;
pub const JSONB_BTREE_OPCLASS_OID: u32 = 10054;
pub const RANGE_BTREE_OPCLASS_OID: u32 = 10055;
pub const RECORD_BTREE_OPCLASS_OID: u32 = 10037;
pub const RECORD_IMAGE_BTREE_OPCLASS_OID: u32 = 10038;
pub const TSQUERY_BTREE_OPCLASS_OID: u32 = 10039;
pub const TSVECTOR_BTREE_OPCLASS_OID: u32 = 10040;
pub const TEXT_PATTERN_BTREE_OPCLASS_OID: u32 = 4217;
pub const VARCHAR_PATTERN_BTREE_OPCLASS_OID: u32 = 4218;
pub const BPCHAR_PATTERN_BTREE_OPCLASS_OID: u32 = 4219;
pub const TSVECTOR_GIST_OPCLASS_OID: u32 = 10043;
pub const TSVECTOR_GIN_OPCLASS_OID: u32 = 10044;
pub const RANGE_SPGIST_OPCLASS_OID: u32 = 10045;
pub const TEXT_SPGIST_OPCLASS_OID: u32 = 10046;
pub const INET_BRIN_INCLUSION_OPCLASS_OID: u32 = 10047;
pub const RANGE_BRIN_INCLUSION_OPCLASS_OID: u32 = 10048;
pub const BOX_BRIN_INCLUSION_OPCLASS_OID: u32 = 10049;
pub const ARRAY_GIN_OPCLASS_OID: u32 = 10051;
pub const POINT_SPGIST_OPCLASS_OID: u32 = 10052;
pub const TEXT_BRIN_BLOOM_OPCLASS_OID: u32 = 10053;
pub const BOX_GIST_OPCLASS_OID: u32 = 76010;
pub const POINT_GIST_OPCLASS_OID: u32 = 76015;
pub const RANGE_GIST_OPCLASS_OID: u32 = 76011;
pub const BOX_SPGIST_OPCLASS_OID: u32 = 76013;
pub const POLY_SPGIST_OPCLASS_OID: u32 = 76014;
pub const INET_GIST_OPCLASS_OID: u32 = 76016;
pub const INET_SPGIST_OPCLASS_OID: u32 = 76017;
pub const MULTIRANGE_GIST_OPCLASS_OID: u32 = 76018;
pub const QUAD_POINT_SPGIST_OPCLASS_OID: u32 = 76019;
pub const KD_POINT_SPGIST_OPCLASS_OID: u32 = 76020;
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
pub const MACADDR_BRIN_MINMAX_OPCLASS_OID: u32 = 76137;
pub const MACADDR8_BRIN_MINMAX_OPCLASS_OID: u32 = 76138;
pub const MACADDR_BRIN_MINMAX_MULTI_OPCLASS_OID: u32 = 76139;
pub const MACADDR8_BRIN_MINMAX_MULTI_OPCLASS_OID: u32 = 76140;
pub const MACADDR_BRIN_BLOOM_OPCLASS_OID: u32 = 76141;
pub const MACADDR8_BRIN_BLOOM_OPCLASS_OID: u32 = 76142;
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
pub const UUID_HASH_OPCLASS_OID: u32 = 76219;
pub const ENUM_HASH_OPCLASS_OID: u32 = 76220;
pub const MULTIRANGE_HASH_OPCLASS_OID: u32 = 76221;
pub const RANGE_HASH_OPCLASS_OID: u32 = 76222;
pub const JSONB_HASH_OPCLASS_OID: u32 = 76223;
pub const MACADDR_HASH_OPCLASS_OID: u32 = 76232;
pub const MACADDR8_HASH_OPCLASS_OID: u32 = 76233;
pub const INTERVAL_HASH_OPCLASS_OID: u32 = 76234;
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
        PgOpclassRow {
            oid: ENUM_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "enum_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_ENUM_FAMILY_OID,
            opcintype: ANYENUMOID,
            opcdefault: false,
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
        PgOpclassRow {
            oid: VARCHAR_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "varchar_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_TEXT_FAMILY_OID,
            opcintype: TEXT_TYPE_OID,
            opcdefault: false,
            opckeytype: 0,
        },
        row(
            BPCHAR_BTREE_OPCLASS_OID,
            "bpchar_ops",
            BTREE_BPCHAR_FAMILY_OID,
            BPCHAR_TYPE_OID,
        ),
        non_default_btree_row(
            TEXT_PATTERN_BTREE_OPCLASS_OID,
            "text_pattern_ops",
            BTREE_TEXT_PATTERN_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        non_default_btree_row(
            VARCHAR_PATTERN_BTREE_OPCLASS_OID,
            "varchar_pattern_ops",
            BTREE_TEXT_PATTERN_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        non_default_btree_row(
            BPCHAR_PATTERN_BTREE_OPCLASS_OID,
            "bpchar_pattern_ops",
            BTREE_BPCHAR_PATTERN_FAMILY_OID,
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
            TIMESTAMPTZ_BTREE_OPCLASS_OID,
            "timestamptz_ops",
            BTREE_DATETIME_FAMILY_OID,
            TIMESTAMPTZ_TYPE_OID,
        ),
        row(
            BYTEA_BTREE_OPCLASS_OID,
            "bytea_ops",
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
        ),
        row(
            UUID_BTREE_OPCLASS_OID,
            "uuid_ops",
            BTREE_UUID_FAMILY_OID,
            UUID_TYPE_OID,
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
        row(
            MACADDR_BTREE_OPCLASS_OID,
            "macaddr_ops",
            BTREE_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
        ),
        row(
            MACADDR8_BTREE_OPCLASS_OID,
            "macaddr8_ops",
            BTREE_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
        ),
        PgOpclassRow {
            oid: RANGE_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "range_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_RANGE_FAMILY_OID,
            opcintype: ANYRANGEOID,
            opcdefault: true,
            opckeytype: 0,
        },
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
        row(
            CIDR_BTREE_OPCLASS_OID,
            "cidr_ops",
            BTREE_NETWORK_FAMILY_OID,
            CIDR_TYPE_OID,
        ),
        row(
            INTERVAL_BTREE_OPCLASS_OID,
            "interval_ops",
            BTREE_INTERVAL_FAMILY_OID,
            INTERVAL_TYPE_OID,
        ),
        row(
            JSONB_BTREE_OPCLASS_OID,
            "jsonb_ops",
            BTREE_JSONB_FAMILY_OID,
            JSONB_TYPE_OID,
        ),
        row(
            RECORD_BTREE_OPCLASS_OID,
            "record_ops",
            BTREE_RECORD_FAMILY_OID,
            RECORD_TYPE_OID,
        ),
        PgOpclassRow {
            oid: RECORD_IMAGE_BTREE_OPCLASS_OID,
            opcmethod: BTREE_AM_OID,
            opcname: "record_image_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BTREE_RECORD_IMAGE_FAMILY_OID,
            opcintype: RECORD_TYPE_OID,
            opcdefault: false,
            opckeytype: 0,
        },
        row(
            TSQUERY_BTREE_OPCLASS_OID,
            "tsquery_ops",
            BTREE_TSQUERY_FAMILY_OID,
            TSQUERY_TYPE_OID,
        ),
        row(
            TSVECTOR_BTREE_OPCLASS_OID,
            "tsvector_ops",
            BTREE_TSVECTOR_FAMILY_OID,
            TSVECTOR_TYPE_OID,
        ),
        row(
            INET_BTREE_OPCLASS_OID,
            "inet_ops",
            BTREE_NETWORK_FAMILY_OID,
            INET_TYPE_OID,
        ),
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
        gist_row(
            RANGE_GIST_OPCLASS_OID,
            "range_ops",
            GIST_RANGE_FAMILY_OID,
            ANYRANGEOID,
        ),
        gist_row(
            TSVECTOR_GIST_OPCLASS_OID,
            "tsvector_ops",
            GIST_TSVECTOR_FAMILY_OID,
            TSVECTOR_TYPE_OID,
        ),
        gist_row(
            INET_GIST_OPCLASS_OID,
            "inet_ops",
            GIST_NETWORK_FAMILY_OID,
            INET_TYPE_OID,
        ),
        // :HACK: PostgreSQL stores GiST multirange keys as compressed range
        // bounding boxes via opckeytype=anyrange. pgrust has not wired GiST
        // compress support through tuple I/O yet, so store multirange keys
        // directly and let the support functions build multirange bounding
        // boxes for internal tuples.
        gist_row(
            MULTIRANGE_GIST_OPCLASS_OID,
            "multirange_ops",
            GIST_MULTIRANGE_FAMILY_OID,
            ANYMULTIRANGEOID,
        ),
        spgist_row(
            QUAD_POINT_SPGIST_OPCLASS_OID,
            "quad_point_ops",
            SPGIST_QUAD_POINT_FAMILY_OID,
            POINT_TYPE_OID,
        ),
        PgOpclassRow {
            oid: KD_POINT_SPGIST_OPCLASS_OID,
            opcmethod: SPGIST_AM_OID,
            opcname: "kd_point_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: SPGIST_KD_POINT_FAMILY_OID,
            opcintype: POINT_TYPE_OID,
            opcdefault: false,
            opckeytype: 0,
        },
        spgist_row(
            TEXT_SPGIST_OPCLASS_OID,
            "text_ops",
            SPGIST_TEXT_FAMILY_OID,
            TEXT_TYPE_OID,
        ),
        spgist_row(
            BOX_SPGIST_OPCLASS_OID,
            "box_ops",
            SPGIST_BOX_FAMILY_OID,
            BOX_TYPE_OID,
        ),
        spgist_row(
            INET_SPGIST_OPCLASS_OID,
            "inet_ops",
            SPGIST_NETWORK_FAMILY_OID,
            INET_TYPE_OID,
        ),
        spgist_row(
            RANGE_SPGIST_OPCLASS_OID,
            "range_ops",
            SPGIST_RANGE_FAMILY_OID,
            ANYRANGEOID,
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
            oid: ARRAY_GIN_OPCLASS_OID,
            opcmethod: GIN_AM_OID,
            opcname: "array_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: GIN_ARRAY_FAMILY_OID,
            opcintype: ANYARRAYOID,
            opcdefault: true,
            opckeytype: ANYELEMENTOID,
        },
        PgOpclassRow {
            oid: TSVECTOR_GIN_OPCLASS_OID,
            opcmethod: GIN_AM_OID,
            opcname: "tsvector_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: GIN_TSVECTOR_FAMILY_OID,
            opcintype: TSVECTOR_TYPE_OID,
            opcdefault: true,
            opckeytype: TEXT_TYPE_OID,
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
        PgOpclassRow {
            oid: TEXT_BRIN_BLOOM_OPCLASS_OID,
            opcmethod: BRIN_AM_OID,
            opcname: "text_bloom_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: BRIN_TEXT_BLOOM_FAMILY_OID,
            opcintype: TEXT_TYPE_OID,
            opcdefault: false,
            opckeytype: 0,
        },
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
        brin_row(
            MACADDR_BRIN_MINMAX_OPCLASS_OID,
            "macaddr_minmax_ops",
            BRIN_MACADDR_MINMAX_FAMILY_OID,
            MACADDR_TYPE_OID,
        ),
        brin_row(
            MACADDR8_BRIN_MINMAX_OPCLASS_OID,
            "macaddr8_minmax_ops",
            BRIN_MACADDR8_MINMAX_FAMILY_OID,
            MACADDR8_TYPE_OID,
        ),
        // :HACK: Generic BRIN minmax-multi and bloom runtime support is not
        // implemented yet; these rows expose PostgreSQL-compatible catalogs.
        brin_non_default_row(
            MACADDR_BRIN_MINMAX_MULTI_OPCLASS_OID,
            "macaddr_minmax_multi_ops",
            BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID,
            MACADDR_TYPE_OID,
        ),
        brin_non_default_row(
            MACADDR8_BRIN_MINMAX_MULTI_OPCLASS_OID,
            "macaddr8_minmax_multi_ops",
            BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID,
            MACADDR8_TYPE_OID,
        ),
        brin_non_default_row(
            MACADDR_BRIN_BLOOM_OPCLASS_OID,
            "macaddr_bloom_ops",
            BRIN_MACADDR_BLOOM_FAMILY_OID,
            MACADDR_TYPE_OID,
        ),
        brin_non_default_row(
            MACADDR8_BRIN_BLOOM_OPCLASS_OID,
            "macaddr8_bloom_ops",
            BRIN_MACADDR8_BLOOM_FAMILY_OID,
            MACADDR8_TYPE_OID,
        ),
        brin_row(
            INET_BRIN_INCLUSION_OPCLASS_OID,
            "inet_inclusion_ops",
            BRIN_NETWORK_INCLUSION_FAMILY_OID,
            INET_TYPE_OID,
        ),
        brin_row(
            RANGE_BRIN_INCLUSION_OPCLASS_OID,
            "range_inclusion_ops",
            BRIN_RANGE_INCLUSION_FAMILY_OID,
            ANYRANGEOID,
        ),
        brin_row(
            BOX_BRIN_INCLUSION_OPCLASS_OID,
            "box_inclusion_ops",
            BRIN_BOX_INCLUSION_FAMILY_OID,
            BOX_TYPE_OID,
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
        hash_row(
            UUID_HASH_OPCLASS_OID,
            "uuid_ops",
            HASH_UUID_FAMILY_OID,
            UUID_TYPE_OID,
        ),
        hash_row(
            RANGE_HASH_OPCLASS_OID,
            "range_ops",
            HASH_RANGE_FAMILY_OID,
            ANYRANGEOID,
        ),
        hash_row(
            INTERVAL_HASH_OPCLASS_OID,
            "interval_ops",
            HASH_INTERVAL_FAMILY_OID,
            INTERVAL_TYPE_OID,
        ),
        PgOpclassRow {
            oid: ENUM_HASH_OPCLASS_OID,
            opcmethod: HASH_AM_OID,
            opcname: "enum_ops".into(),
            opcnamespace: PG_CATALOG_NAMESPACE_OID,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: HASH_ENUM_FAMILY_OID,
            opcintype: ANYENUMOID,
            opcdefault: false,
            opckeytype: 0,
        },
        hash_row(
            MULTIRANGE_HASH_OPCLASS_OID,
            "multirange_ops",
            HASH_MULTIRANGE_FAMILY_OID,
            ANYMULTIRANGEOID,
        ),
        hash_row(
            JSONB_HASH_OPCLASS_OID,
            "jsonb_ops",
            HASH_JSONB_FAMILY_OID,
            JSONB_TYPE_OID,
        ),
        hash_row(
            MACADDR_HASH_OPCLASS_OID,
            "macaddr_ops",
            HASH_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
        ),
        hash_row(
            MACADDR8_HASH_OPCLASS_OID,
            "macaddr8_ops",
            HASH_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
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

fn non_default_btree_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        oid,
        opcmethod: BTREE_AM_OID,
        opcname: opcname.into(),
        opcnamespace: PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family,
        opcintype: input_type,
        opcdefault: false,
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

fn brin_non_default_row(oid: u32, opcname: &str, family: u32, input_type: u32) -> PgOpclassRow {
    PgOpclassRow {
        opcdefault: false,
        ..brin_row(oid, opcname, family, input_type)
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
        INTERVAL_TYPE_OID => INTERVAL_BTREE_OPCLASS_OID,
        TIMESTAMP_TYPE_OID => TIMESTAMP_BTREE_OPCLASS_OID,
        TIMESTAMPTZ_TYPE_OID => TIMESTAMPTZ_BTREE_OPCLASS_OID,
        BYTEA_TYPE_OID => BYTEA_BTREE_OPCLASS_OID,
        UUID_TYPE_OID => UUID_BTREE_OPCLASS_OID,
        CIDR_TYPE_OID => CIDR_BTREE_OPCLASS_OID,
        INET_TYPE_OID => INET_BTREE_OPCLASS_OID,
        BIT_TYPE_OID => BIT_BTREE_OPCLASS_OID,
        VARBIT_TYPE_OID => VARBIT_BTREE_OPCLASS_OID,
        INT4RANGE_TYPE_OID | INT8RANGE_TYPE_OID | NUMRANGE_TYPE_OID | DATERANGE_TYPE_OID
        | TSRANGE_TYPE_OID | TSTZRANGE_TYPE_OID => RANGE_BTREE_OPCLASS_OID,
        MACADDR_TYPE_OID => MACADDR_BTREE_OPCLASS_OID,
        MACADDR8_TYPE_OID => MACADDR8_BTREE_OPCLASS_OID,
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
        UUID_TYPE_OID => UUID_HASH_OPCLASS_OID,
        INT4RANGE_TYPE_OID | INT8RANGE_TYPE_OID | NUMRANGE_TYPE_OID | DATERANGE_TYPE_OID
        | TSRANGE_TYPE_OID | TSTZRANGE_TYPE_OID => RANGE_HASH_OPCLASS_OID,
        INT4MULTIRANGE_TYPE_OID
        | NUMMULTIRANGE_TYPE_OID
        | TSMULTIRANGE_TYPE_OID
        | TSTZMULTIRANGE_TYPE_OID
        | DATEMULTIRANGE_TYPE_OID
        | INT8MULTIRANGE_TYPE_OID => MULTIRANGE_HASH_OPCLASS_OID,
        MACADDR_TYPE_OID => MACADDR_HASH_OPCLASS_OID,
        MACADDR8_TYPE_OID => MACADDR8_HASH_OPCLASS_OID,
        INTERVAL_TYPE_OID => INTERVAL_HASH_OPCLASS_OID,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_rows_include_macaddr_opclasses() {
        let rows = bootstrap_pg_opclass_rows();
        for (oid, method, name, family, input_type, default) in [
            (
                MACADDR_BTREE_OPCLASS_OID,
                BTREE_AM_OID,
                "macaddr_ops",
                BTREE_MACADDR_FAMILY_OID,
                MACADDR_TYPE_OID,
                true,
            ),
            (
                MACADDR8_BTREE_OPCLASS_OID,
                BTREE_AM_OID,
                "macaddr8_ops",
                BTREE_MACADDR8_FAMILY_OID,
                MACADDR8_TYPE_OID,
                true,
            ),
            (
                MACADDR_BRIN_MINMAX_OPCLASS_OID,
                BRIN_AM_OID,
                "macaddr_minmax_ops",
                BRIN_MACADDR_MINMAX_FAMILY_OID,
                MACADDR_TYPE_OID,
                true,
            ),
            (
                MACADDR8_BRIN_MINMAX_MULTI_OPCLASS_OID,
                BRIN_AM_OID,
                "macaddr8_minmax_multi_ops",
                BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID,
                MACADDR8_TYPE_OID,
                false,
            ),
            (
                MACADDR_BRIN_BLOOM_OPCLASS_OID,
                BRIN_AM_OID,
                "macaddr_bloom_ops",
                BRIN_MACADDR_BLOOM_FAMILY_OID,
                MACADDR_TYPE_OID,
                false,
            ),
            (
                MACADDR_HASH_OPCLASS_OID,
                HASH_AM_OID,
                "macaddr_ops",
                HASH_MACADDR_FAMILY_OID,
                MACADDR_TYPE_OID,
                true,
            ),
            (
                MACADDR8_HASH_OPCLASS_OID,
                HASH_AM_OID,
                "macaddr8_ops",
                HASH_MACADDR8_FAMILY_OID,
                MACADDR8_TYPE_OID,
                true,
            ),
        ] {
            assert!(
                rows.iter().any(|row| {
                    row.oid == oid
                        && row.opcmethod == method
                        && row.opcname == name
                        && row.opcfamily == family
                        && row.opcintype == input_type
                        && row.opcdefault == default
                }),
                "missing opclass {name} ({oid})"
            );
        }

        assert_eq!(
            default_btree_opclass_oid(MACADDR_TYPE_OID),
            Some(MACADDR_BTREE_OPCLASS_OID)
        );
        assert_eq!(
            default_btree_opclass_oid(MACADDR8_TYPE_OID),
            Some(MACADDR8_BTREE_OPCLASS_OID)
        );
        assert_eq!(
            default_hash_opclass_oid(MACADDR_TYPE_OID),
            Some(MACADDR_HASH_OPCLASS_OID)
        );
        assert_eq!(
            default_hash_opclass_oid(MACADDR8_TYPE_OID),
            Some(MACADDR8_HASH_OPCLASS_OID)
        );
    }
}
