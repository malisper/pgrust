use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::parser::SqlTypeKind;
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYMULTIRANGEOID, ANYRANGEOID, BIT_ARRAY_TYPE_OID,
    BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID, BOOTSTRAP_SUPERUSER_OID, BOX_TYPE_OID,
    BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CIDR_ARRAY_TYPE_OID, CIDR_TYPE_OID, CIRCLE_TYPE_OID, DATE_ARRAY_TYPE_OID, DATE_TYPE_OID,
    DATEMULTIRANGE_ARRAY_TYPE_OID, DATEMULTIRANGE_TYPE_OID, DATERANGE_ARRAY_TYPE_OID,
    DATERANGE_TYPE_OID, FDW_HANDLER_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INET_ARRAY_TYPE_OID, INET_TYPE_OID,
    INT2_ARRAY_TYPE_OID, INT2_TYPE_OID, INT2VECTOR_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID,
    INT4MULTIRANGE_ARRAY_TYPE_OID, INT4MULTIRANGE_TYPE_OID, INT4RANGE_ARRAY_TYPE_OID,
    INT4RANGE_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID, INT8MULTIRANGE_ARRAY_TYPE_OID,
    INT8MULTIRANGE_TYPE_OID, INT8RANGE_ARRAY_TYPE_OID, INT8RANGE_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, INTERNAL_TYPE_OID,
    INTERVAL_ARRAY_TYPE_OID, INTERVAL_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID, JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID,
    LINE_TYPE_OID, LSEG_TYPE_OID, MACADDR_ARRAY_TYPE_OID, MACADDR_TYPE_OID,
    MACADDR8_ARRAY_TYPE_OID, MACADDR8_TYPE_OID, MONEY_ARRAY_TYPE_OID, MONEY_TYPE_OID,
    NAME_ARRAY_TYPE_OID, NAME_TYPE_OID, NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID,
    NUMMULTIRANGE_ARRAY_TYPE_OID, NUMMULTIRANGE_TYPE_OID, NUMRANGE_ARRAY_TYPE_OID,
    NUMRANGE_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID, OIDVECTOR_TYPE_OID, PATH_TYPE_OID,
    PG_ATTRIBUTE_RELATION_OID, PG_ATTRIBUTE_ROWTYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PG_CLASS_RELATION_OID, PG_CLASS_ROWTYPE_OID, PG_DATABASE_RELATION_OID, PG_DATABASE_ROWTYPE_OID,
    PG_DEPENDENCIES_TYPE_OID, PG_LSN_ARRAY_TYPE_OID, PG_LSN_TYPE_OID, PG_MCV_LIST_TYPE_OID,
    PG_NAMESPACE_RELATION_OID, PG_NAMESPACE_ROWTYPE_OID, PG_NDISTINCT_TYPE_OID,
    PG_NODE_TREE_TYPE_OID, PG_PROC_RELATION_OID, PG_PROC_ROWTYPE_OID, PG_STATISTIC_ARRAY_TYPE_OID,
    PG_STATISTIC_EXT_ARRAY_TYPE_OID, PG_STATISTIC_EXT_DATA_ARRAY_TYPE_OID,
    PG_STATISTIC_EXT_DATA_RELATION_OID, PG_STATISTIC_EXT_DATA_ROWTYPE_OID,
    PG_STATISTIC_EXT_RELATION_OID, PG_STATISTIC_EXT_ROWTYPE_OID, PG_STATISTIC_RELATION_OID,
    PG_STATISTIC_ROWTYPE_OID, PG_TYPE_RELATION_OID, PG_TYPE_ROWTYPE_OID, POINT_TYPE_OID,
    POLYGON_TYPE_OID, RECORD_ARRAY_TYPE_OID, RECORD_TYPE_OID, REFCURSOR_ARRAY_TYPE_OID,
    REFCURSOR_TYPE_OID, REGCLASS_ARRAY_TYPE_OID, REGCLASS_TYPE_OID, REGCOLLATION_ARRAY_TYPE_OID,
    REGCOLLATION_TYPE_OID, REGCONFIG_ARRAY_TYPE_OID, REGCONFIG_TYPE_OID,
    REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, REGNAMESPACE_ARRAY_TYPE_OID,
    REGNAMESPACE_TYPE_OID, REGOPER_ARRAY_TYPE_OID, REGOPER_TYPE_OID, REGOPERATOR_ARRAY_TYPE_OID,
    REGOPERATOR_TYPE_OID, REGPROC_ARRAY_TYPE_OID, REGPROC_TYPE_OID, REGPROCEDURE_ARRAY_TYPE_OID,
    REGPROCEDURE_TYPE_OID, REGROLE_TYPE_OID, REGTYPE_TYPE_OID, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID,
    TID_ARRAY_TYPE_OID, TID_TYPE_OID, TIME_ARRAY_TYPE_OID, TIME_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID,
    TIMESTAMP_TYPE_OID, TIMESTAMPTZ_ARRAY_TYPE_OID, TIMESTAMPTZ_TYPE_OID, TIMETZ_ARRAY_TYPE_OID,
    TIMETZ_TYPE_OID, TRIGGER_TYPE_OID, TSMULTIRANGE_ARRAY_TYPE_OID, TSMULTIRANGE_TYPE_OID,
    TSQUERY_ARRAY_TYPE_OID, TSQUERY_TYPE_OID, TSRANGE_ARRAY_TYPE_OID, TSRANGE_TYPE_OID,
    TSTZMULTIRANGE_ARRAY_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, TSTZRANGE_ARRAY_TYPE_OID,
    TSTZRANGE_TYPE_OID, TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID, TXID_SNAPSHOT_ARRAY_TYPE_OID,
    TXID_SNAPSHOT_TYPE_OID, UUID_ARRAY_TYPE_OID, UUID_TYPE_OID, VARBIT_ARRAY_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, VOID_TYPE_OID, XID_ARRAY_TYPE_OID,
    XID_TYPE_OID, XML_ARRAY_TYPE_OID, XML_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTypeRow {
    pub oid: u32,
    pub typname: String,
    pub typnamespace: u32,
    pub typowner: u32,
    pub typlen: i16,
    pub typalign: AttributeAlign,
    pub typstorage: AttributeStorage,
    pub typrelid: u32,
    pub typelem: u32,
    pub typarray: u32,
    pub sql_type: SqlType,
}

pub fn pg_type_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("typnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typlen", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("typalign", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typstorage", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typelem", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typarray", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn builtin_type_rows() -> Vec<PgTypeRow> {
    let mut rows = vec![
        builtin_type_row(
            "anyelement",
            ANYELEMENTOID,
            SqlType::new(SqlTypeKind::AnyElement),
        ),
        builtin_type_row("anyarray", ANYARRAYOID, SqlType::new(SqlTypeKind::AnyArray)),
        builtin_type_row("anyrange", ANYRANGEOID, SqlType::new(SqlTypeKind::AnyRange)),
        builtin_type_row(
            "anymultirange",
            ANYMULTIRANGEOID,
            SqlType::new(SqlTypeKind::AnyMultirange),
        ),
        builtin_type_row(
            "anycompatible",
            ANYCOMPATIBLEOID,
            SqlType::new(SqlTypeKind::AnyCompatible),
        ),
        builtin_type_row(
            "anycompatiblearray",
            ANYCOMPATIBLEARRAYOID,
            SqlType::new(SqlTypeKind::AnyCompatibleArray),
        ),
        builtin_type_row(
            "anycompatiblerange",
            ANYCOMPATIBLERANGEOID,
            SqlType::new(SqlTypeKind::AnyCompatibleRange),
        ),
        builtin_type_row(
            "anycompatiblemultirange",
            ANYCOMPATIBLEMULTIRANGEOID,
            SqlType::new(SqlTypeKind::AnyCompatibleMultirange),
        ),
        builtin_type_row("record", RECORD_TYPE_OID, SqlType::record(RECORD_TYPE_OID)),
        builtin_type_row(
            "_record",
            RECORD_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::record(RECORD_TYPE_OID)),
        ),
        builtin_type_row("bool", BOOL_TYPE_OID, SqlType::new(SqlTypeKind::Bool)),
        builtin_type_row(
            "_bool",
            BOOL_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Bool)),
        ),
        builtin_type_row("bit", BIT_TYPE_OID, SqlType::new(SqlTypeKind::Bit)),
        builtin_type_row(
            "_bit",
            BIT_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Bit)),
        ),
        builtin_type_row("varbit", VARBIT_TYPE_OID, SqlType::new(SqlTypeKind::VarBit)),
        builtin_type_row(
            "_varbit",
            VARBIT_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::VarBit)),
        ),
        builtin_type_row("bytea", BYTEA_TYPE_OID, SqlType::new(SqlTypeKind::Bytea)),
        builtin_type_row(
            "_bytea",
            BYTEA_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Bytea)),
        ),
        fixed_builtin_type_row(
            "uuid",
            UUID_TYPE_OID,
            SqlType::new(SqlTypeKind::Uuid),
            16,
            AttributeAlign::Char,
        ),
        builtin_type_row(
            "_uuid",
            UUID_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Uuid)),
        ),
        builtin_type_row(
            "\"char\"",
            INTERNAL_CHAR_TYPE_OID,
            SqlType::new(SqlTypeKind::InternalChar),
        ),
        builtin_type_row(
            "_char",
            INTERNAL_CHAR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar)),
        ),
        builtin_type_row("name", NAME_TYPE_OID, SqlType::new(SqlTypeKind::Name)),
        builtin_type_row(
            "_name",
            NAME_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Name)),
        ),
        builtin_type_row("int8", INT8_TYPE_OID, SqlType::new(SqlTypeKind::Int8)),
        builtin_type_row(
            "_int8",
            INT8_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int8)),
        ),
        builtin_type_row("int2", INT2_TYPE_OID, SqlType::new(SqlTypeKind::Int2)),
        builtin_type_row(
            "int2vector",
            INT2VECTOR_TYPE_OID,
            SqlType::new(SqlTypeKind::Int2Vector),
        ),
        builtin_type_row(
            "_int2",
            INT2_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
        ),
        builtin_type_row("int4", INT4_TYPE_OID, SqlType::new(SqlTypeKind::Int4)),
        builtin_type_row(
            "_int4",
            INT4_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
        ),
        builtin_type_row("text", TEXT_TYPE_OID, SqlType::new(SqlTypeKind::Text)),
        builtin_type_row(
            "_text",
            TEXT_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        ),
        builtin_type_row(
            "refcursor",
            REFCURSOR_TYPE_OID,
            SqlType::new(SqlTypeKind::Text).with_identity(REFCURSOR_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_refcursor",
            REFCURSOR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Text).with_identity(REFCURSOR_TYPE_OID, 0)),
        ),
        builtin_type_row("void", VOID_TYPE_OID, SqlType::new(SqlTypeKind::Void)),
        builtin_type_row(
            "internal",
            INTERNAL_TYPE_OID,
            SqlType::new(SqlTypeKind::Internal),
        ),
        builtin_type_row(
            "trigger",
            TRIGGER_TYPE_OID,
            SqlType::new(SqlTypeKind::Trigger),
        ),
        fixed_builtin_type_row(
            "fdw_handler",
            FDW_HANDLER_TYPE_OID,
            SqlType::new(SqlTypeKind::FdwHandler),
            4,
            AttributeAlign::Int,
        ),
        builtin_type_row("oid", OID_TYPE_OID, SqlType::new(SqlTypeKind::Oid)),
        builtin_type_row(
            "regproc",
            REGPROC_TYPE_OID,
            SqlType::new(SqlTypeKind::RegProc),
        ),
        builtin_type_row(
            "regclass",
            REGCLASS_TYPE_OID,
            SqlType::new(SqlTypeKind::RegClass),
        ),
        builtin_type_row(
            "regtype",
            REGTYPE_TYPE_OID,
            SqlType::new(SqlTypeKind::RegType),
        ),
        builtin_type_row(
            "regrole",
            REGROLE_TYPE_OID,
            SqlType::new(SqlTypeKind::RegRole),
        ),
        builtin_type_row(
            "regnamespace",
            REGNAMESPACE_TYPE_OID,
            SqlType::new(SqlTypeKind::RegNamespace),
        ),
        builtin_type_row(
            "regoper",
            REGOPER_TYPE_OID,
            SqlType::new(SqlTypeKind::RegOper),
        ),
        builtin_type_row(
            "regprocedure",
            REGPROCEDURE_TYPE_OID,
            SqlType::new(SqlTypeKind::RegProcedure),
        ),
        builtin_type_row(
            "regoperator",
            REGOPERATOR_TYPE_OID,
            SqlType::new(SqlTypeKind::RegOperator),
        ),
        builtin_type_row(
            "regcollation",
            REGCOLLATION_TYPE_OID,
            SqlType::new(SqlTypeKind::RegCollation),
        ),
        builtin_type_row("tid", TID_TYPE_OID, SqlType::new(SqlTypeKind::Tid)),
        builtin_type_row(
            "_tid",
            TID_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Tid)),
        ),
        builtin_type_row("xid", XID_TYPE_OID, SqlType::new(SqlTypeKind::Xid)),
        builtin_type_row(
            "_xid",
            XID_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Xid)),
        ),
        builtin_type_row(
            "txid_snapshot",
            TXID_SNAPSHOT_TYPE_OID,
            SqlType::new(SqlTypeKind::Text).with_identity(TXID_SNAPSHOT_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_txid_snapshot",
            TXID_SNAPSHOT_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::new(SqlTypeKind::Text).with_identity(TXID_SNAPSHOT_TYPE_OID, 0),
            )
            .with_identity(TXID_SNAPSHOT_ARRAY_TYPE_OID, 0),
        ),
        builtin_type_row(
            "oidvector",
            OIDVECTOR_TYPE_OID,
            SqlType::new(SqlTypeKind::OidVector),
        ),
        builtin_type_row(
            "_oid",
            OID_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
        ),
        builtin_type_row(
            "_regclass",
            REGCLASS_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegClass)),
        ),
        builtin_type_row(
            "_regproc",
            REGPROC_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegProc)),
        ),
        builtin_type_row(
            "_regnamespace",
            REGNAMESPACE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegNamespace)),
        ),
        builtin_type_row(
            "_regoper",
            REGOPER_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegOper)),
        ),
        builtin_type_row(
            "_regprocedure",
            REGPROCEDURE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegProcedure)),
        ),
        builtin_type_row(
            "_regoperator",
            REGOPERATOR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegOperator)),
        ),
        builtin_type_row(
            "_regcollation",
            REGCOLLATION_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegCollation)),
        ),
        builtin_type_row("float4", FLOAT4_TYPE_OID, SqlType::new(SqlTypeKind::Float4)),
        builtin_type_row(
            "_float4",
            FLOAT4_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
        ),
        builtin_type_row("float8", FLOAT8_TYPE_OID, SqlType::new(SqlTypeKind::Float8)),
        builtin_type_row(
            "_float8",
            FLOAT8_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Float8)),
        ),
        builtin_type_row("money", MONEY_TYPE_OID, SqlType::new(SqlTypeKind::Money)),
        builtin_type_row(
            "_money",
            MONEY_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Money)),
        ),
        builtin_type_row("cidr", CIDR_TYPE_OID, SqlType::new(SqlTypeKind::Cidr)),
        builtin_type_row(
            "_cidr",
            CIDR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Cidr)),
        ),
        fixed_builtin_type_row(
            "macaddr",
            MACADDR_TYPE_OID,
            SqlType::new(SqlTypeKind::MacAddr),
            6,
            AttributeAlign::Int,
        ),
        builtin_type_row(
            "_macaddr",
            MACADDR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr)),
        ),
        fixed_builtin_type_row(
            "macaddr8",
            MACADDR8_TYPE_OID,
            SqlType::new(SqlTypeKind::MacAddr8),
            8,
            AttributeAlign::Int,
        ),
        builtin_type_row(
            "_macaddr8",
            MACADDR8_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr8)),
        ),
        builtin_type_row("inet", INET_TYPE_OID, SqlType::new(SqlTypeKind::Inet)),
        builtin_type_row(
            "_inet",
            INET_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Inet)),
        ),
        builtin_type_row("point", POINT_TYPE_OID, SqlType::new(SqlTypeKind::Point)),
        builtin_type_row("lseg", LSEG_TYPE_OID, SqlType::new(SqlTypeKind::Lseg)),
        builtin_type_row("path", PATH_TYPE_OID, SqlType::new(SqlTypeKind::Path)),
        builtin_type_row("box", BOX_TYPE_OID, SqlType::new(SqlTypeKind::Box)),
        builtin_type_row(
            "polygon",
            POLYGON_TYPE_OID,
            SqlType::new(SqlTypeKind::Polygon),
        ),
        builtin_type_row("line", LINE_TYPE_OID, SqlType::new(SqlTypeKind::Line)),
        builtin_type_row("circle", CIRCLE_TYPE_OID, SqlType::new(SqlTypeKind::Circle)),
        builtin_type_row(
            "varchar",
            VARCHAR_TYPE_OID,
            SqlType::new(SqlTypeKind::Varchar),
        ),
        builtin_type_row(
            "_varchar",
            VARCHAR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
        ),
        builtin_type_row("char", BPCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Char)),
        builtin_type_row(
            "_bpchar",
            BPCHAR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Char)),
        ),
        builtin_type_row("date", DATE_TYPE_OID, SqlType::new(SqlTypeKind::Date)),
        builtin_range_type_row(
            "daterange",
            DATERANGE_TYPE_OID,
            DATE_TYPE_OID,
            DATEMULTIRANGE_TYPE_OID,
            true,
        ),
        builtin_multirange_type_row(
            "datemultirange",
            DATEMULTIRANGE_TYPE_OID,
            DATERANGE_TYPE_OID,
            DATE_TYPE_OID,
            true,
        ),
        builtin_type_row(
            "_date",
            DATE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Date)),
        ),
        builtin_type_row("time", TIME_TYPE_OID, SqlType::new(SqlTypeKind::Time)),
        builtin_type_row(
            "_time",
            TIME_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Time)),
        ),
        builtin_type_row("timetz", TIMETZ_TYPE_OID, SqlType::new(SqlTypeKind::TimeTz)),
        builtin_type_row(
            "_timetz",
            TIMETZ_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::TimeTz)),
        ),
        builtin_type_row(
            "timestamp",
            TIMESTAMP_TYPE_OID,
            SqlType::new(SqlTypeKind::Timestamp),
        ),
        builtin_range_type_row(
            "tsrange",
            TSRANGE_TYPE_OID,
            TIMESTAMP_TYPE_OID,
            TSMULTIRANGE_TYPE_OID,
            false,
        ),
        builtin_multirange_type_row(
            "tsmultirange",
            TSMULTIRANGE_TYPE_OID,
            TSRANGE_TYPE_OID,
            TIMESTAMP_TYPE_OID,
            false,
        ),
        builtin_type_row(
            "_timestamp",
            TIMESTAMP_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Timestamp)),
        ),
        builtin_type_row(
            "timestamptz",
            TIMESTAMPTZ_TYPE_OID,
            SqlType::new(SqlTypeKind::TimestampTz),
        ),
        builtin_range_type_row(
            "tstzrange",
            TSTZRANGE_TYPE_OID,
            TIMESTAMPTZ_TYPE_OID,
            TSTZMULTIRANGE_TYPE_OID,
            false,
        ),
        builtin_multirange_type_row(
            "tstzmultirange",
            TSTZMULTIRANGE_TYPE_OID,
            TSTZRANGE_TYPE_OID,
            TIMESTAMPTZ_TYPE_OID,
            false,
        ),
        builtin_type_row(
            "_timestamptz",
            TIMESTAMPTZ_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::TimestampTz)),
        ),
        builtin_type_row(
            "interval",
            INTERVAL_TYPE_OID,
            SqlType::new(SqlTypeKind::Interval),
        ),
        builtin_type_row(
            "_interval",
            INTERVAL_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Interval)),
        ),
        builtin_type_row(
            "numeric",
            NUMERIC_TYPE_OID,
            SqlType::new(SqlTypeKind::Numeric),
        ),
        builtin_range_type_row(
            "numrange",
            NUMRANGE_TYPE_OID,
            NUMERIC_TYPE_OID,
            NUMMULTIRANGE_TYPE_OID,
            false,
        ),
        builtin_multirange_type_row(
            "nummultirange",
            NUMMULTIRANGE_TYPE_OID,
            NUMRANGE_TYPE_OID,
            NUMERIC_TYPE_OID,
            false,
        ),
        builtin_type_row(
            "_numeric",
            NUMERIC_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Numeric)),
        ),
        builtin_range_type_row(
            "int4range",
            INT4RANGE_TYPE_OID,
            INT4_TYPE_OID,
            INT4MULTIRANGE_TYPE_OID,
            true,
        ),
        builtin_multirange_type_row(
            "int4multirange",
            INT4MULTIRANGE_TYPE_OID,
            INT4RANGE_TYPE_OID,
            INT4_TYPE_OID,
            true,
        ),
        builtin_range_type_row(
            "int8range",
            INT8RANGE_TYPE_OID,
            INT8_TYPE_OID,
            INT8MULTIRANGE_TYPE_OID,
            true,
        ),
        builtin_multirange_type_row(
            "int8multirange",
            INT8MULTIRANGE_TYPE_OID,
            INT8RANGE_TYPE_OID,
            INT8_TYPE_OID,
            true,
        ),
        builtin_type_row("json", JSON_TYPE_OID, SqlType::new(SqlTypeKind::Json)),
        builtin_type_row(
            "_json",
            JSON_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Json)),
        ),
        builtin_type_row("jsonb", JSONB_TYPE_OID, SqlType::new(SqlTypeKind::Jsonb)),
        builtin_type_row(
            "_jsonb",
            JSONB_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Jsonb)),
        ),
        builtin_type_row(
            "jsonpath",
            JSONPATH_TYPE_OID,
            SqlType::new(SqlTypeKind::JsonPath),
        ),
        builtin_type_row("xml", XML_TYPE_OID, SqlType::new(SqlTypeKind::Xml)),
        builtin_type_row(
            "_xml",
            XML_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Xml)),
        ),
        builtin_type_row(
            "tsvector",
            TSVECTOR_TYPE_OID,
            SqlType::new(SqlTypeKind::TsVector),
        ),
        builtin_type_row(
            "_tsvector",
            TSVECTOR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::TsVector)),
        ),
        builtin_type_row(
            "tsquery",
            TSQUERY_TYPE_OID,
            SqlType::new(SqlTypeKind::TsQuery),
        ),
        builtin_type_row(
            "_tsquery",
            TSQUERY_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::TsQuery)),
        ),
        builtin_type_row("pg_lsn", PG_LSN_TYPE_OID, SqlType::new(SqlTypeKind::PgLsn)),
        builtin_type_row(
            "_pg_lsn",
            PG_LSN_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::PgLsn)),
        ),
        builtin_type_row(
            "regconfig",
            REGCONFIG_TYPE_OID,
            SqlType::new(SqlTypeKind::RegConfig),
        ),
        builtin_type_row(
            "_regconfig",
            REGCONFIG_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegConfig)),
        ),
        builtin_type_row(
            "regdictionary",
            REGDICTIONARY_TYPE_OID,
            SqlType::new(SqlTypeKind::RegDictionary),
        ),
        builtin_type_row(
            "_regdictionary",
            REGDICTIONARY_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegDictionary)),
        ),
        builtin_type_row(
            "pg_node_tree",
            PG_NODE_TREE_TYPE_OID,
            SqlType::new(SqlTypeKind::PgNodeTree),
        ),
        builtin_type_row(
            "pg_ndistinct",
            PG_NDISTINCT_TYPE_OID,
            SqlType::new(SqlTypeKind::Bytea).with_identity(PG_NDISTINCT_TYPE_OID, 0),
        ),
        builtin_type_row(
            "pg_dependencies",
            PG_DEPENDENCIES_TYPE_OID,
            SqlType::new(SqlTypeKind::Bytea).with_identity(PG_DEPENDENCIES_TYPE_OID, 0),
        ),
        builtin_type_row(
            "pg_mcv_list",
            PG_MCV_LIST_TYPE_OID,
            SqlType::new(SqlTypeKind::Bytea).with_identity(PG_MCV_LIST_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_jsonpath",
            JSONPATH_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::JsonPath)),
        ),
    ];
    rows.extend([
        builtin_type_row(
            "_int4range",
            INT4RANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(INT4RANGE_TYPE_OID, INT4_TYPE_OID).with_range_metadata(
                    INT4_TYPE_OID,
                    INT4MULTIRANGE_TYPE_OID,
                    true,
                ),
            ),
        ),
        builtin_type_row(
            "_numrange",
            NUMRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(NUMRANGE_TYPE_OID, NUMERIC_TYPE_OID).with_range_metadata(
                    NUMERIC_TYPE_OID,
                    NUMMULTIRANGE_TYPE_OID,
                    false,
                ),
            ),
        ),
        builtin_type_row(
            "_tsrange",
            TSRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(TSRANGE_TYPE_OID, TIMESTAMP_TYPE_OID).with_range_metadata(
                    TIMESTAMP_TYPE_OID,
                    TSMULTIRANGE_TYPE_OID,
                    false,
                ),
            ),
        ),
        builtin_type_row(
            "_tstzrange",
            TSTZRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(TSTZRANGE_TYPE_OID, TIMESTAMPTZ_TYPE_OID).with_range_metadata(
                    TIMESTAMPTZ_TYPE_OID,
                    TSTZMULTIRANGE_TYPE_OID,
                    false,
                ),
            ),
        ),
        builtin_type_row(
            "_daterange",
            DATERANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(DATERANGE_TYPE_OID, DATE_TYPE_OID).with_range_metadata(
                    DATE_TYPE_OID,
                    DATEMULTIRANGE_TYPE_OID,
                    true,
                ),
            ),
        ),
        builtin_type_row(
            "_int8range",
            INT8RANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(INT8RANGE_TYPE_OID, INT8_TYPE_OID).with_range_metadata(
                    INT8_TYPE_OID,
                    INT8MULTIRANGE_TYPE_OID,
                    true,
                ),
            ),
        ),
        builtin_type_row(
            "_int4multirange",
            INT4MULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(INT4MULTIRANGE_TYPE_OID, INT4RANGE_TYPE_OID)
                    .with_range_metadata(INT4_TYPE_OID, INT4MULTIRANGE_TYPE_OID, true)
                    .with_multirange_range_oid(INT4RANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_nummultirange",
            NUMMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(NUMMULTIRANGE_TYPE_OID, NUMRANGE_TYPE_OID)
                    .with_range_metadata(NUMERIC_TYPE_OID, NUMMULTIRANGE_TYPE_OID, false)
                    .with_multirange_range_oid(NUMRANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_tsmultirange",
            TSMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(TSMULTIRANGE_TYPE_OID, TSRANGE_TYPE_OID)
                    .with_range_metadata(TIMESTAMP_TYPE_OID, TSMULTIRANGE_TYPE_OID, false)
                    .with_multirange_range_oid(TSRANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_tstzmultirange",
            TSTZMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(TSTZMULTIRANGE_TYPE_OID, TSTZRANGE_TYPE_OID)
                    .with_range_metadata(TIMESTAMPTZ_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, false)
                    .with_multirange_range_oid(TSTZRANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_datemultirange",
            DATEMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(DATEMULTIRANGE_TYPE_OID, DATERANGE_TYPE_OID)
                    .with_range_metadata(DATE_TYPE_OID, DATEMULTIRANGE_TYPE_OID, true)
                    .with_multirange_range_oid(DATERANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_int8multirange",
            INT8MULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(INT8MULTIRANGE_TYPE_OID, INT8RANGE_TYPE_OID)
                    .with_range_metadata(INT8_TYPE_OID, INT8MULTIRANGE_TYPE_OID, true)
                    .with_multirange_range_oid(INT8RANGE_TYPE_OID),
            ),
        ),
    ]);
    annotate_array_type_links(&mut rows);
    rows
}

pub fn builtin_type_name_for_oid(oid: u32) -> Option<String> {
    builtin_type_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| row.typname)
}

pub fn bootstrap_composite_type_rows() -> Vec<PgTypeRow> {
    let mut rows = vec![
        composite_type_row(
            "pg_namespace",
            PG_NAMESPACE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_NAMESPACE_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_type",
            PG_TYPE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_TYPE_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_proc",
            PG_PROC_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_PROC_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_attribute",
            PG_ATTRIBUTE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_ATTRIBUTE_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_class",
            PG_CLASS_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_CLASS_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_database",
            PG_DATABASE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_DATABASE_RELATION_OID,
            0,
        ),
        composite_type_row(
            "pg_statistic",
            PG_STATISTIC_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_RELATION_OID,
            PG_STATISTIC_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_statistic_ext",
            PG_STATISTIC_EXT_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_EXT_RELATION_OID,
            PG_STATISTIC_EXT_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_statistic_ext_data",
            PG_STATISTIC_EXT_DATA_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_EXT_DATA_RELATION_OID,
            PG_STATISTIC_EXT_DATA_ARRAY_TYPE_OID,
        ),
        composite_array_type_row(
            "pg_statistic",
            PG_STATISTIC_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_ROWTYPE_OID,
            PG_STATISTIC_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_statistic_ext",
            PG_STATISTIC_EXT_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_EXT_ROWTYPE_OID,
            PG_STATISTIC_EXT_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_statistic_ext_data",
            PG_STATISTIC_EXT_DATA_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_STATISTIC_EXT_DATA_ROWTYPE_OID,
            PG_STATISTIC_EXT_DATA_RELATION_OID,
        ),
    ];
    annotate_array_type_links(&mut rows);
    rows
}

fn builtin_type_row(name: &str, oid: u32, sql_type: SqlType) -> PgTypeRow {
    let storage = column_desc("datum", sql_type, true).storage;
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typowner: BOOTSTRAP_SUPERUSER_OID,
        typlen: storage.attlen,
        typalign: storage.attalign,
        typstorage: storage.attstorage,
        typrelid: 0,
        typelem: 0,
        typarray: 0,
        sql_type,
    }
}

fn fixed_builtin_type_row(
    name: &str,
    oid: u32,
    sql_type: SqlType,
    typlen: i16,
    typalign: AttributeAlign,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typowner: BOOTSTRAP_SUPERUSER_OID,
        typlen,
        typalign,
        typstorage: AttributeStorage::Plain,
        typrelid: 0,
        typelem: 0,
        typarray: 0,
        sql_type,
    }
}

fn builtin_range_type_row(
    name: &str,
    oid: u32,
    subtype_oid: u32,
    multirange_oid: u32,
    discrete: bool,
) -> PgTypeRow {
    builtin_type_row(
        name,
        oid,
        SqlType::range(oid, subtype_oid).with_range_metadata(subtype_oid, multirange_oid, discrete),
    )
}

fn builtin_multirange_type_row(
    name: &str,
    oid: u32,
    range_oid: u32,
    subtype_oid: u32,
    discrete: bool,
) -> PgTypeRow {
    builtin_type_row(
        name,
        oid,
        SqlType::multirange(oid, range_oid)
            .with_range_metadata(subtype_oid, oid, discrete)
            .with_multirange_range_oid(range_oid),
    )
}

pub fn composite_type_row(
    name: &str,
    oid: u32,
    namespace_oid: u32,
    relid: u32,
    array_oid: u32,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: namespace_oid,
        typowner: BOOTSTRAP_SUPERUSER_OID,
        typlen: -1,
        typalign: AttributeAlign::Double,
        typstorage: AttributeStorage::Extended,
        typrelid: relid,
        typelem: 0,
        typarray: array_oid,
        sql_type: SqlType::named_composite(oid, relid),
    }
}

pub fn composite_array_type_row(
    name: &str,
    oid: u32,
    namespace_oid: u32,
    elem_oid: u32,
    relid: u32,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: format!("_{name}"),
        typnamespace: namespace_oid,
        typowner: BOOTSTRAP_SUPERUSER_OID,
        typlen: -1,
        typalign: AttributeAlign::Double,
        typstorage: AttributeStorage::Extended,
        typrelid: 0,
        typelem: elem_oid,
        typarray: 0,
        sql_type: SqlType::array_of(SqlType::named_composite(elem_oid, relid)),
    }
}

fn annotate_array_type_links(rows: &mut [PgTypeRow]) {
    let snapshot = rows.to_vec();
    for row in rows.iter_mut() {
        if row.sql_type.is_array {
            row.typelem = snapshot
                .iter()
                .find(|type_row| type_row.sql_type == row.sql_type.element_type())
                .map(|type_row| type_row.oid)
                .unwrap_or(row.sql_type.type_oid);
        } else if let Some(array_oid) = snapshot
            .iter()
            .find(|array_row| array_row.sql_type == SqlType::array_of(row.sql_type))
            .map(|array_row| array_row.oid)
        {
            row.typarray = array_oid;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::PG_NODE_TREE_TYPE_OID;

    #[test]
    fn bootstrap_composite_types_match_core_catalogs() {
        let rows = bootstrap_composite_type_rows();
        let names: Vec<_> = rows.iter().map(|row| row.typname.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "pg_namespace",
                "pg_type",
                "pg_proc",
                "pg_attribute",
                "pg_class",
                "pg_database",
                "pg_statistic",
                "pg_statistic_ext",
                "pg_statistic_ext_data",
                "_pg_statistic",
                "_pg_statistic_ext",
                "_pg_statistic_ext_data",
            ]
        );
        assert_eq!(rows[0].oid, PG_NAMESPACE_ROWTYPE_OID);
        assert_eq!(rows[2].oid, PG_PROC_ROWTYPE_OID);
        assert_eq!(rows[4].oid, PG_CLASS_ROWTYPE_OID);
        assert_eq!(rows[5].oid, PG_DATABASE_ROWTYPE_OID);
        assert_eq!(rows[6].oid, PG_STATISTIC_ROWTYPE_OID);
        assert_eq!(rows[7].oid, PG_STATISTIC_EXT_ROWTYPE_OID);
        assert_eq!(rows[8].oid, PG_STATISTIC_EXT_DATA_ROWTYPE_OID);
    }

    #[test]
    fn builtin_types_include_pg_node_tree() {
        assert!(builtin_type_rows().iter().any(|row| {
            row.oid == PG_NODE_TREE_TYPE_OID
                && row.typname == "pg_node_tree"
                && row.sql_type == SqlType::new(SqlTypeKind::PgNodeTree)
        }));
    }

    #[test]
    fn builtin_types_include_postgres_macaddr_oids() {
        let rows = builtin_type_rows();
        let macaddr = rows
            .iter()
            .find(|row| row.typname == "macaddr")
            .expect("macaddr type row");
        assert_eq!(macaddr.oid, MACADDR_TYPE_OID);
        assert_eq!(macaddr.typlen, 6);
        assert_eq!(macaddr.typalign, AttributeAlign::Int);
        assert_eq!(macaddr.typstorage, AttributeStorage::Plain);
        assert_eq!(macaddr.typarray, MACADDR_ARRAY_TYPE_OID);
        assert_eq!(macaddr.sql_type, SqlType::new(SqlTypeKind::MacAddr));

        let macaddr8 = rows
            .iter()
            .find(|row| row.typname == "macaddr8")
            .expect("macaddr8 type row");
        assert_eq!(macaddr8.oid, MACADDR8_TYPE_OID);
        assert_eq!(macaddr8.typlen, 8);
        assert_eq!(macaddr8.typalign, AttributeAlign::Int);
        assert_eq!(macaddr8.typstorage, AttributeStorage::Plain);
        assert_eq!(macaddr8.typarray, MACADDR8_ARRAY_TYPE_OID);
        assert_eq!(macaddr8.sql_type, SqlType::new(SqlTypeKind::MacAddr8));

        assert!(rows.iter().any(|row| {
            row.oid == MACADDR_ARRAY_TYPE_OID
                && row.typname == "_macaddr"
                && row.typelem == MACADDR_TYPE_OID
        }));
        assert!(rows.iter().any(|row| {
            row.oid == MACADDR8_ARRAY_TYPE_OID
                && row.typname == "_macaddr8"
                && row.typelem == MACADDR8_TYPE_OID
        }));
    }

    #[test]
    fn builtin_types_include_statistics_payload_types() {
        for (oid, name) in [
            (PG_NDISTINCT_TYPE_OID, "pg_ndistinct"),
            (PG_DEPENDENCIES_TYPE_OID, "pg_dependencies"),
            (PG_MCV_LIST_TYPE_OID, "pg_mcv_list"),
        ] {
            assert!(builtin_type_rows().iter().any(|row| {
                row.oid == oid
                    && row.typname == name
                    && row.sql_type == SqlType::new(SqlTypeKind::Bytea).with_identity(oid, 0)
            }));
        }
    }

    #[test]
    fn builtin_types_include_datetime_rows() {
        let rows = builtin_type_rows();
        for (oid, name, sql_type) in [
            (DATE_TYPE_OID, "date", SqlType::new(SqlTypeKind::Date)),
            (
                DATE_ARRAY_TYPE_OID,
                "_date",
                SqlType::array_of(SqlType::new(SqlTypeKind::Date)),
            ),
            (TIME_TYPE_OID, "time", SqlType::new(SqlTypeKind::Time)),
            (
                TIME_ARRAY_TYPE_OID,
                "_time",
                SqlType::array_of(SqlType::new(SqlTypeKind::Time)),
            ),
            (TID_TYPE_OID, "tid", SqlType::new(SqlTypeKind::Tid)),
            (
                TID_ARRAY_TYPE_OID,
                "_tid",
                SqlType::array_of(SqlType::new(SqlTypeKind::Tid)),
            ),
            (XID_TYPE_OID, "xid", SqlType::new(SqlTypeKind::Xid)),
            (
                XID_ARRAY_TYPE_OID,
                "_xid",
                SqlType::array_of(SqlType::new(SqlTypeKind::Xid)),
            ),
            (TIMETZ_TYPE_OID, "timetz", SqlType::new(SqlTypeKind::TimeTz)),
            (
                TIMETZ_ARRAY_TYPE_OID,
                "_timetz",
                SqlType::array_of(SqlType::new(SqlTypeKind::TimeTz)),
            ),
            (
                TIMESTAMP_TYPE_OID,
                "timestamp",
                SqlType::new(SqlTypeKind::Timestamp),
            ),
            (
                TIMESTAMP_ARRAY_TYPE_OID,
                "_timestamp",
                SqlType::array_of(SqlType::new(SqlTypeKind::Timestamp)),
            ),
            (
                TIMESTAMPTZ_TYPE_OID,
                "timestamptz",
                SqlType::new(SqlTypeKind::TimestampTz),
            ),
            (
                TIMESTAMPTZ_ARRAY_TYPE_OID,
                "_timestamptz",
                SqlType::array_of(SqlType::new(SqlTypeKind::TimestampTz)),
            ),
            (
                INTERVAL_TYPE_OID,
                "interval",
                SqlType::new(SqlTypeKind::Interval),
            ),
            (
                INTERVAL_ARRAY_TYPE_OID,
                "_interval",
                SqlType::array_of(SqlType::new(SqlTypeKind::Interval)),
            ),
        ] {
            assert!(
                rows.iter().any(|row| {
                    row.oid == oid && row.typname == name && row.sql_type == sql_type
                })
            );
        }
    }

    #[test]
    fn builtin_types_include_record() {
        let row = builtin_type_rows()
            .into_iter()
            .find(|row| row.oid == RECORD_TYPE_OID)
            .expect("record row");
        assert_eq!(row.typname, "record");
        assert_eq!(row.typlen, -1);
        assert_eq!(row.typalign, AttributeAlign::Double);
        assert_eq!(row.typstorage, AttributeStorage::Extended);
        assert_eq!(row.sql_type, SqlType::record(RECORD_TYPE_OID));
    }

    #[test]
    fn builtin_types_include_fdw_handler() {
        let row = builtin_type_rows()
            .into_iter()
            .find(|row| row.oid == FDW_HANDLER_TYPE_OID)
            .expect("fdw_handler row");
        assert_eq!(row.typname, "fdw_handler");
        assert_eq!(row.typlen, 4);
        assert_eq!(row.sql_type, SqlType::new(SqlTypeKind::FdwHandler));
    }

    #[test]
    fn composite_types_use_varlena_storage_metadata() {
        let row = bootstrap_composite_type_rows()
            .into_iter()
            .find(|row| row.typname == "pg_class")
            .unwrap();
        assert_eq!(row.typlen, -1);
        assert_eq!(row.typalign, AttributeAlign::Double);
        assert_eq!(row.typstorage, AttributeStorage::Extended);
    }

    #[test]
    fn composite_types_preserve_row_type_identity() {
        let row = bootstrap_composite_type_rows()
            .into_iter()
            .find(|row| row.typname == "pg_class")
            .unwrap();
        assert_eq!(row.oid, PG_CLASS_ROWTYPE_OID);
        assert_eq!(row.typrelid, PG_CLASS_RELATION_OID);
        assert_eq!(
            row.sql_type,
            SqlType::named_composite(PG_CLASS_ROWTYPE_OID, PG_CLASS_RELATION_OID)
        );
    }

    #[test]
    fn builtin_range_types_expose_array_links() {
        let rows = builtin_type_rows();
        for (range_oid, range_name, array_oid, array_name) in [
            (
                INT4RANGE_TYPE_OID,
                "int4range",
                INT4RANGE_ARRAY_TYPE_OID,
                "_int4range",
            ),
            (
                NUMRANGE_TYPE_OID,
                "numrange",
                NUMRANGE_ARRAY_TYPE_OID,
                "_numrange",
            ),
            (
                TSRANGE_TYPE_OID,
                "tsrange",
                TSRANGE_ARRAY_TYPE_OID,
                "_tsrange",
            ),
            (
                TSTZRANGE_TYPE_OID,
                "tstzrange",
                TSTZRANGE_ARRAY_TYPE_OID,
                "_tstzrange",
            ),
            (
                DATERANGE_TYPE_OID,
                "daterange",
                DATERANGE_ARRAY_TYPE_OID,
                "_daterange",
            ),
            (
                INT8RANGE_TYPE_OID,
                "int8range",
                INT8RANGE_ARRAY_TYPE_OID,
                "_int8range",
            ),
        ] {
            let range_row = rows
                .iter()
                .find(|row| row.oid == range_oid)
                .unwrap_or_else(|| panic!("missing builtin range type row for {range_name}"));
            let array_row = rows
                .iter()
                .find(|row| row.oid == array_oid)
                .unwrap_or_else(|| panic!("missing builtin range array type row for {array_name}"));

            assert_eq!(range_row.typname, range_name);
            assert_eq!(range_row.typarray, array_oid);
            assert_eq!(array_row.typname, array_name);
            assert_eq!(array_row.typelem, range_oid);
        }
    }
}
