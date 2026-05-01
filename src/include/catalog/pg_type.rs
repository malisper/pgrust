use std::sync::OnceLock;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::parser::SqlTypeKind;
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::catalog::*;

const ARRAY_IN_PROC_OID: u32 = 750;
const ARRAY_OUT_PROC_OID: u32 = 751;
const ARRAY_RECV_PROC_OID: u32 = 2400;
const ARRAY_SEND_PROC_OID: u32 = 2401;
const ARRAY_TYPANALYZE_PROC_OID: u32 = 3816;
const ARRAY_SUBSCRIPT_HANDLER_PROC_OID: u32 = 6179;
const RAW_ARRAY_SUBSCRIPT_HANDLER_PROC_OID: u32 = 6180;
const RECORD_IN_PROC_OID: u32 = 2290;
const RECORD_OUT_PROC_OID: u32 = 2291;
const RECORD_RECV_PROC_OID: u32 = 2402;
const RECORD_SEND_PROC_OID: u32 = 2403;
const INT2VECTOR_IN_PROC_OID: u32 = 40;
const INT2VECTOR_OUT_PROC_OID: u32 = 41;
const TEXT_IN_PROC_OID: u32 = 46;
const TEXT_OUT_PROC_OID: u32 = 47;
const INT4_IN_PROC_OID: u32 = 42;
const INT4_OUT_PROC_OID: u32 = 43;
const OIDVECTOR_IN_PROC_OID: u32 = 54;
const OIDVECTOR_OUT_PROC_OID: u32 = 55;
const VARCHAR_IN_PROC_OID: u32 = 1046;
const VARCHAR_OUT_PROC_OID: u32 = 1047;
const BOOL_IN_PROC_OID: u32 = 1242;
const BOOL_OUT_PROC_OID: u32 = 1243;
const NUMERIC_OUT_PROC_OID: u32 = 1702;
const INT2VECTOR_RECV_PROC_OID: u32 = 2410;
const INT2VECTOR_SEND_PROC_OID: u32 = 2411;
const TEXT_RECV_PROC_OID: u32 = 2414;
const TEXT_SEND_PROC_OID: u32 = 2415;
const VARCHAR_TYPMOD_IN_PROC_OID: u32 = 2915;
const VARCHAR_TYPMOD_OUT_PROC_OID: u32 = 2916;
const NUMERIC_TYPMOD_IN_PROC_OID: u32 = 2917;
const NUMERIC_TYPMOD_OUT_PROC_OID: u32 = 2918;
const OIDVECTOR_RECV_PROC_OID: u32 = 2420;
const OIDVECTOR_SEND_PROC_OID: u32 = 2421;
const BRIN_BLOOM_SUMMARY_IN_PROC_OID: u32 = 4596;
const BRIN_BLOOM_SUMMARY_OUT_PROC_OID: u32 = 4597;
const BRIN_BLOOM_SUMMARY_RECV_PROC_OID: u32 = 4598;
const BRIN_BLOOM_SUMMARY_SEND_PROC_OID: u32 = 4599;
const BRIN_MINMAX_MULTI_SUMMARY_IN_PROC_OID: u32 = 4638;
const BRIN_MINMAX_MULTI_SUMMARY_OUT_PROC_OID: u32 = 4639;
const BRIN_MINMAX_MULTI_SUMMARY_RECV_PROC_OID: u32 = 4640;
const BRIN_MINMAX_MULTI_SUMMARY_SEND_PROC_OID: u32 = 4641;
const GTSVECTOR_IN_PROC_OID: u32 = 3646;
const GTSVECTOR_OUT_PROC_OID: u32 = 3647;
const INTERNAL_IN_PROC_OID: u32 = 2304;
const ANYARRAY_IN_PROC_OID: u32 = 2296;
const ANYARRAY_OUT_PROC_OID: u32 = 2297;
const ANYELEMENT_IN_PROC_OID: u32 = 2312;
const ANYENUM_IN_PROC_OID: u32 = 3504;
const ANYENUM_OUT_PROC_OID: u32 = 3505;
const ANYNONARRAY_IN_PROC_OID: u32 = 2777;
const ANYRANGE_IN_PROC_OID: u32 = 3832;
const ANYRANGE_OUT_PROC_OID: u32 = 3833;
const RANGE_IN_PROC_OID: u32 = 3834;
const RANGE_OUT_PROC_OID: u32 = 3835;
const RANGE_RECV_PROC_OID: u32 = 3836;
const RANGE_SEND_PROC_OID: u32 = 3837;
const RANGE_TYPANALYZE_PROC_OID: u32 = 3916;
const ANYMULTIRANGE_IN_PROC_OID: u32 = 4229;
const ANYMULTIRANGE_OUT_PROC_OID: u32 = 4230;
const MULTIRANGE_IN_PROC_OID: u32 = 4231;
const MULTIRANGE_OUT_PROC_OID: u32 = 4232;
const MULTIRANGE_RECV_PROC_OID: u32 = 4233;
const MULTIRANGE_SEND_PROC_OID: u32 = 4234;
const MULTIRANGE_TYPANALYZE_PROC_OID: u32 = 4242;
const ANYCOMPATIBLE_IN_PROC_OID: u32 = 5086;
const ANYCOMPATIBLEARRAY_IN_PROC_OID: u32 = 5088;
const ANYCOMPATIBLEARRAY_OUT_PROC_OID: u32 = 5089;
const ANYCOMPATIBLENONARRAY_IN_PROC_OID: u32 = 5092;
const ANYCOMPATIBLERANGE_IN_PROC_OID: u32 = 5094;
const ANYCOMPATIBLERANGE_OUT_PROC_OID: u32 = 5095;
const ANYCOMPATIBLEMULTIRANGE_IN_PROC_OID: u32 = 4226;
const ANYCOMPATIBLEMULTIRANGE_OUT_PROC_OID: u32 = 4227;
const DOMAIN_IN_PROC_OID: u32 = 2597;
const DOMAIN_RECV_PROC_OID: u32 = 2598;
const ENUM_IN_PROC_OID: u32 = 3506;
const ENUM_OUT_PROC_OID: u32 = 3507;
const ENUM_RECV_PROC_OID: u32 = 3532;
const ENUM_SEND_PROC_OID: u32 = 3533;
const CSTRING_IN_PROC_OID: u32 = 2292;
const CSTRING_OUT_PROC_OID: u32 = 2293;
const VOID_IN_PROC_OID: u32 = 2298;
const VOID_OUT_PROC_OID: u32 = 2299;
const PG_RUST_TYPE_INPUT_PROC_BASE: u32 = 100_000;
const PG_RUST_TYPE_OUTPUT_PROC_BASE: u32 = 200_000;
const UNKNOWN_IN_PROC_OID: u32 = 109;
const UNKNOWN_OUT_PROC_OID: u32 = 110;
const PGRUST_SANITY_ENUM_TYPE_OID: u32 = 60008;
const PGRUST_SANITY_ENUM_ARRAY_TYPE_OID: u32 = 60009;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTypeRow {
    pub oid: u32,
    pub typname: String,
    pub typnamespace: u32,
    pub typowner: u32,
    pub typacl: Option<Vec<String>>,
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: char,
    pub typisdefined: bool,
    pub typalign: AttributeAlign,
    pub typstorage: AttributeStorage,
    pub typrelid: u32,
    pub typsubscript: u32,
    pub typelem: u32,
    pub typarray: u32,
    pub typinput: u32,
    pub typoutput: u32,
    pub typreceive: u32,
    pub typsend: u32,
    pub typmodin: u32,
    pub typmodout: u32,
    pub typdelim: char,
    pub typanalyze: u32,
    pub typbasetype: u32,
    pub typcollation: u32,
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
            column_desc("typbyval", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("typtype", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typisdefined", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("typalign", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typstorage", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typsubscript", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typelem", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typarray", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typinput", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typoutput", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typreceive", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typsend", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typmodin", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typmodout", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typdelim", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("typanalyze", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("typbasetype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typtypmod", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("typcollation", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typnotnull", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("typdefault", SqlType::new(SqlTypeKind::Text), true),
            column_desc(
                "typacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn builtin_type_rows() -> Vec<PgTypeRow> {
    builtin_type_rows_ref().to_vec()
}

pub fn builtin_type_row_by_oid(oid: u32) -> Option<PgTypeRow> {
    builtin_type_rows_ref()
        .iter()
        .find(|row| row.oid == oid)
        .cloned()
}

pub fn builtin_type_row_by_name(name: &str) -> Option<PgTypeRow> {
    builtin_type_rows_ref()
        .iter()
        .find(|row| row.typname.eq_ignore_ascii_case(name))
        .cloned()
}

fn builtin_type_rows_ref() -> &'static [PgTypeRow] {
    static ROWS: OnceLock<Vec<PgTypeRow>> = OnceLock::new();
    ROWS.get_or_init(build_builtin_type_rows)
}

fn build_builtin_type_rows() -> Vec<PgTypeRow> {
    let mut rows = vec![
        builtin_type_row(
            "any",
            ANYOID,
            SqlType::new(SqlTypeKind::AnyElement).with_identity(ANYOID, 0),
        ),
        {
            let mut row = fixed_builtin_type_row(
                "unknown",
                UNKNOWN_TYPE_OID,
                SqlType::new(SqlTypeKind::Text).with_identity(UNKNOWN_TYPE_OID, 0),
                -2,
                AttributeAlign::Char,
            );
            row.typtype = 'p';
            row
        },
        builtin_type_row(
            "anyelement",
            ANYELEMENTOID,
            SqlType::new(SqlTypeKind::AnyElement),
        ),
        builtin_type_row(
            "anynonarray",
            ANYNONARRAYOID,
            SqlType::new(SqlTypeKind::AnyElement).with_identity(ANYNONARRAYOID, 0),
        ),
        builtin_type_row("anyarray", ANYARRAYOID, SqlType::new(SqlTypeKind::AnyArray)),
        builtin_type_row("anyrange", ANYRANGEOID, SqlType::new(SqlTypeKind::AnyRange)),
        builtin_type_row(
            "anymultirange",
            ANYMULTIRANGEOID,
            SqlType::new(SqlTypeKind::AnyMultirange),
        ),
        builtin_type_row("anyenum", ANYENUMOID, SqlType::new(SqlTypeKind::AnyEnum)),
        // :HACK: pgrust does not persist CREATE TYPE AS ENUM entries across
        // the regression setup-server restart yet. Keep one non-conflicting
        // enum shape in pg_type so type_sanity can validate enum I/O metadata.
        {
            let mut row = builtin_type_row(
                "pgrust_sanity_enum",
                PGRUST_SANITY_ENUM_TYPE_OID,
                SqlType::new(SqlTypeKind::Enum).with_identity(PGRUST_SANITY_ENUM_TYPE_OID, 0),
            );
            row.typarray = PGRUST_SANITY_ENUM_ARRAY_TYPE_OID;
            row
        },
        builtin_type_row(
            "_pgrust_sanity_enum",
            PGRUST_SANITY_ENUM_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::new(SqlTypeKind::Enum).with_identity(PGRUST_SANITY_ENUM_TYPE_OID, 0),
            ),
        ),
        builtin_type_row(
            "anycompatible",
            ANYCOMPATIBLEOID,
            SqlType::new(SqlTypeKind::AnyCompatible),
        ),
        builtin_type_row(
            "anycompatiblenonarray",
            ANYCOMPATIBLENONARRAYOID,
            SqlType::new(SqlTypeKind::AnyCompatible).with_identity(ANYCOMPATIBLENONARRAYOID, 0),
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
        fixed_builtin_type_row(
            "cstring",
            CSTRING_TYPE_OID,
            SqlType::new(SqlTypeKind::Cstring),
            -2,
            AttributeAlign::Char,
        ),
        builtin_type_row(
            "_cstring",
            CSTRING_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Cstring)),
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
        builtin_range_type_row(
            "varbitrange",
            VARBITRANGE_TYPE_OID,
            VARBIT_TYPE_OID,
            VARBITMULTIRANGE_TYPE_OID,
            false,
        ),
        builtin_multirange_type_row(
            "varbitmultirange",
            VARBITMULTIRANGE_TYPE_OID,
            VARBITRANGE_TYPE_OID,
            VARBIT_TYPE_OID,
            false,
        ),
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
            "char",
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
            "_int2vector",
            INT2VECTOR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int2Vector)),
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
        builtin_type_row(
            "event_trigger",
            EVENT_TRIGGER_TYPE_OID,
            SqlType::new(SqlTypeKind::EventTrigger),
        ),
        fixed_builtin_type_row(
            "fdw_handler",
            FDW_HANDLER_TYPE_OID,
            SqlType::new(SqlTypeKind::FdwHandler),
            4,
            AttributeAlign::Int,
        ),
        fixed_builtin_type_row(
            "index_am_handler",
            INDEX_AM_HANDLER_TYPE_OID,
            SqlType::new(SqlTypeKind::FdwHandler).with_identity(INDEX_AM_HANDLER_TYPE_OID, 0),
            4,
            AttributeAlign::Int,
        ),
        fixed_builtin_type_row(
            "table_am_handler",
            TABLE_AM_HANDLER_TYPE_OID,
            SqlType::new(SqlTypeKind::FdwHandler).with_identity(TABLE_AM_HANDLER_TYPE_OID, 0),
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
            "cid",
            CID_TYPE_OID,
            SqlType::new(SqlTypeKind::Int4).with_identity(CID_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_cid",
            CID_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4).with_identity(CID_TYPE_OID, 0))
                .with_identity(CID_ARRAY_TYPE_OID, 0),
        ),
        builtin_type_row(
            "xid8",
            XID8_TYPE_OID,
            SqlType::new(SqlTypeKind::Int8).with_identity(XID8_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_xid8",
            XID8_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Int8).with_identity(XID8_TYPE_OID, 0))
                .with_identity(XID8_ARRAY_TYPE_OID, 0),
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
            "pg_snapshot",
            PG_SNAPSHOT_TYPE_OID,
            SqlType::new(SqlTypeKind::Text).with_identity(PG_SNAPSHOT_TYPE_OID, 0),
        ),
        builtin_type_row(
            "_pg_snapshot",
            PG_SNAPSHOT_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::new(SqlTypeKind::Text).with_identity(PG_SNAPSHOT_TYPE_OID, 0),
            )
            .with_identity(PG_SNAPSHOT_ARRAY_TYPE_OID, 0),
        ),
        builtin_type_row(
            "oidvector",
            OIDVECTOR_TYPE_OID,
            SqlType::new(SqlTypeKind::OidVector),
        ),
        builtin_type_row(
            "_oidvector",
            OIDVECTOR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::OidVector)),
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
            "_regtype",
            REGTYPE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegType)),
        ),
        builtin_type_row(
            "_regrole",
            REGROLE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::RegRole)),
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
        builtin_type_row(
            "_point",
            POINT_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Point)),
        ),
        builtin_type_row("lseg", LSEG_TYPE_OID, SqlType::new(SqlTypeKind::Lseg)),
        builtin_type_row(
            "_lseg",
            LSEG_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Lseg)),
        ),
        builtin_type_row("path", PATH_TYPE_OID, SqlType::new(SqlTypeKind::Path)),
        builtin_type_row(
            "_path",
            PATH_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Path)),
        ),
        builtin_type_row("box", BOX_TYPE_OID, SqlType::new(SqlTypeKind::Box)),
        builtin_type_row(
            "_box",
            BOX_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Box)),
        ),
        builtin_type_row(
            "polygon",
            POLYGON_TYPE_OID,
            SqlType::new(SqlTypeKind::Polygon),
        ),
        builtin_type_row(
            "_polygon",
            POLYGON_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Polygon)),
        ),
        builtin_type_row("line", LINE_TYPE_OID, SqlType::new(SqlTypeKind::Line)),
        builtin_type_row(
            "_line",
            LINE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Line)),
        ),
        builtin_type_row("circle", CIRCLE_TYPE_OID, SqlType::new(SqlTypeKind::Circle)),
        builtin_type_row(
            "_circle",
            CIRCLE_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Circle)),
        ),
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
        builtin_type_row("bpchar", BPCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Char)),
        builtin_type_row(
            "_bpchar",
            BPCHAR_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Char)),
        ),
        fixed_builtin_type_row(
            "aclitem",
            ACLITEM_TYPE_OID,
            SqlType::new(SqlTypeKind::Text).with_identity(ACLITEM_TYPE_OID, 0),
            16,
            AttributeAlign::Double,
        ),
        builtin_type_row(
            "_aclitem",
            ACLITEM_ARRAY_TYPE_OID,
            SqlType::array_of(SqlType::new(SqlTypeKind::Text).with_identity(ACLITEM_TYPE_OID, 0))
                .with_identity(ACLITEM_ARRAY_TYPE_OID, 0),
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
        builtin_range_type_row(
            "arrayrange",
            ARRAYRANGE_TYPE_OID,
            INT4_ARRAY_TYPE_OID,
            ARRAYMULTIRANGE_TYPE_OID,
            false,
        ),
        builtin_multirange_type_row(
            "arraymultirange",
            ARRAYMULTIRANGE_TYPE_OID,
            ARRAYRANGE_TYPE_OID,
            INT4_ARRAY_TYPE_OID,
            false,
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
        gtsvector_type_row(),
        builtin_type_row(
            "_gtsvector",
            GTSVECTOR_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::new(SqlTypeKind::TsVector).with_identity(GTSVECTOR_TYPE_OID, 0),
            )
            .with_identity(GTSVECTOR_ARRAY_TYPE_OID, 0),
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
        brin_summary_type_row(
            "pg_brin_bloom_summary",
            PG_BRIN_BLOOM_SUMMARY_TYPE_OID,
            BRIN_BLOOM_SUMMARY_IN_PROC_OID,
            BRIN_BLOOM_SUMMARY_OUT_PROC_OID,
            BRIN_BLOOM_SUMMARY_RECV_PROC_OID,
            BRIN_BLOOM_SUMMARY_SEND_PROC_OID,
        ),
        brin_summary_type_row(
            "pg_brin_minmax_multi_summary",
            PG_BRIN_MINMAX_MULTI_SUMMARY_TYPE_OID,
            BRIN_MINMAX_MULTI_SUMMARY_IN_PROC_OID,
            BRIN_MINMAX_MULTI_SUMMARY_OUT_PROC_OID,
            BRIN_MINMAX_MULTI_SUMMARY_RECV_PROC_OID,
            BRIN_MINMAX_MULTI_SUMMARY_SEND_PROC_OID,
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
        builtin_type_row(
            "_arrayrange",
            ARRAYRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(ARRAYRANGE_TYPE_OID, INT4_ARRAY_TYPE_OID).with_range_metadata(
                    INT4_ARRAY_TYPE_OID,
                    ARRAYMULTIRANGE_TYPE_OID,
                    false,
                ),
            ),
        ),
        builtin_type_row(
            "_arraymultirange",
            ARRAYMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(ARRAYMULTIRANGE_TYPE_OID, ARRAYRANGE_TYPE_OID)
                    .with_range_metadata(INT4_ARRAY_TYPE_OID, ARRAYMULTIRANGE_TYPE_OID, false)
                    .with_multirange_range_oid(ARRAYRANGE_TYPE_OID),
            ),
        ),
        builtin_type_row(
            "_varbitrange",
            VARBITRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::range(VARBITRANGE_TYPE_OID, VARBIT_TYPE_OID).with_range_metadata(
                    VARBIT_TYPE_OID,
                    VARBITMULTIRANGE_TYPE_OID,
                    false,
                ),
            ),
        ),
        builtin_type_row(
            "_varbitmultirange",
            VARBITMULTIRANGE_ARRAY_TYPE_OID,
            SqlType::array_of(
                SqlType::multirange(VARBITMULTIRANGE_TYPE_OID, VARBITRANGE_TYPE_OID)
                    .with_range_metadata(VARBIT_TYPE_OID, VARBITMULTIRANGE_TYPE_OID, false)
                    .with_multirange_range_oid(VARBITRANGE_TYPE_OID),
            ),
        ),
    ]);
    rows.extend(information_schema_domain_type_rows(&rows));
    annotate_array_type_links(&mut rows);
    annotate_catalog_type_io_procs(&mut rows);
    rows
}

pub fn builtin_type_name_for_oid(oid: u32) -> Option<String> {
    builtin_type_row_by_oid(oid).map(|row| row.typname)
}

pub fn bootstrap_composite_type_rows() -> Vec<PgTypeRow> {
    let mut rows = vec![
        composite_type_row(
            "pg_type",
            PG_TYPE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_TYPE_RELATION_OID,
            PG_TYPE_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_proc",
            PG_PROC_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_PROC_RELATION_OID,
            PG_PROC_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_attribute",
            PG_ATTRIBUTE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_ATTRIBUTE_RELATION_OID,
            PG_ATTRIBUTE_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_class",
            PG_CLASS_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_CLASS_RELATION_OID,
            PG_CLASS_ARRAY_TYPE_OID,
        ),
        composite_type_row(
            "pg_database",
            PG_DATABASE_ROWTYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_DATABASE_RELATION_OID,
            PG_DATABASE_ARRAY_TYPE_OID,
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
            "pg_type",
            PG_TYPE_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_TYPE_ROWTYPE_OID,
            PG_TYPE_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_proc",
            PG_PROC_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_PROC_ROWTYPE_OID,
            PG_PROC_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_attribute",
            PG_ATTRIBUTE_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_ATTRIBUTE_ROWTYPE_OID,
            PG_ATTRIBUTE_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_class",
            PG_CLASS_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_CLASS_ROWTYPE_OID,
            PG_CLASS_RELATION_OID,
        ),
        composite_array_type_row(
            "pg_database",
            PG_DATABASE_ARRAY_TYPE_OID,
            PG_CATALOG_NAMESPACE_OID,
            PG_DATABASE_ROWTYPE_OID,
            PG_DATABASE_RELATION_OID,
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
    let typelem = if sql_type.is_array {
        sql_type.element_type().type_oid
    } else {
        0
    };
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typowner: BOOTSTRAP_SUPERUSER_OID,
        typacl: None,
        typlen: storage.attlen,
        typbyval: typbyval_for_sql_type(sql_type, storage.attlen),
        typtype: typtype_for_sql_type(sql_type),
        typisdefined: true,
        typalign: storage.attalign,
        typstorage: storage.attstorage,
        typrelid: 0,
        typsubscript: 0,
        typelem,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typmodin: 0,
        typmodout: 0,
        typdelim: ',',
        typanalyze: 0,
        typbasetype: 0,
        typcollation: crate::backend::catalog::catalog::default_column_collation_oid(sql_type),
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
        typacl: None,
        typlen,
        typbyval: typbyval_for_sql_type(sql_type, typlen),
        typtype: typtype_for_sql_type(sql_type),
        typisdefined: true,
        typalign,
        typstorage: AttributeStorage::Plain,
        typrelid: 0,
        typsubscript: 0,
        typelem: 0,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typmodin: 0,
        typmodout: 0,
        typdelim: ',',
        typanalyze: 0,
        typbasetype: 0,
        typcollation: crate::backend::catalog::catalog::default_column_collation_oid(sql_type),
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
    let mut row = builtin_type_row(
        name,
        oid,
        SqlType::range(oid, subtype_oid).with_range_metadata(subtype_oid, multirange_oid, discrete),
    );
    if matches!(
        subtype_oid,
        INT8_TYPE_OID | TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
    ) {
        row.typalign = AttributeAlign::Double;
    }
    row
}

fn builtin_multirange_type_row(
    name: &str,
    oid: u32,
    range_oid: u32,
    subtype_oid: u32,
    discrete: bool,
) -> PgTypeRow {
    let mut row = builtin_type_row(
        name,
        oid,
        SqlType::multirange(oid, range_oid)
            .with_range_metadata(subtype_oid, oid, discrete)
            .with_multirange_range_oid(range_oid),
    );
    if matches!(
        subtype_oid,
        INT8_TYPE_OID | TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
    ) {
        row.typalign = AttributeAlign::Double;
    }
    row
}

fn brin_summary_type_row(
    name: &str,
    oid: u32,
    typinput: u32,
    typoutput: u32,
    typreceive: u32,
    typsend: u32,
) -> PgTypeRow {
    let mut row = builtin_type_row(
        name,
        oid,
        SqlType::new(SqlTypeKind::Bytea).with_identity(oid, 0),
    );
    row.typlen = -1;
    row.typbyval = false;
    row.typalign = AttributeAlign::Int;
    row.typstorage = AttributeStorage::Extended;
    row.typinput = typinput;
    row.typoutput = typoutput;
    row.typreceive = typreceive;
    row.typsend = typsend;
    row.typcollation = DEFAULT_COLLATION_OID;
    row
}

fn gtsvector_type_row() -> PgTypeRow {
    // :HACK: PostgreSQL exposes gtsvector as a GiST support type. pgrust
    // catalogs it so regtype/catalog sanity checks work, but there is no
    // executable runtime behavior behind this type yet.
    let mut row = builtin_type_row(
        "gtsvector",
        GTSVECTOR_TYPE_OID,
        SqlType::new(SqlTypeKind::TsVector).with_identity(GTSVECTOR_TYPE_OID, 0),
    );
    row.typinput = GTSVECTOR_IN_PROC_OID;
    row.typoutput = GTSVECTOR_OUT_PROC_OID;
    row
}

fn information_schema_domain_type_rows(existing: &[PgTypeRow]) -> Vec<PgTypeRow> {
    let specs = [
        (
            "cardinal_number",
            INFORMATION_SCHEMA_CARDINAL_NUMBER_TYPE_OID,
            INFORMATION_SCHEMA_CARDINAL_NUMBER_ARRAY_TYPE_OID,
            INT4_TYPE_OID,
            SqlType::new(SqlTypeKind::Int4),
        ),
        (
            "character_data",
            INFORMATION_SCHEMA_CHARACTER_DATA_TYPE_OID,
            INFORMATION_SCHEMA_CHARACTER_DATA_ARRAY_TYPE_OID,
            VARCHAR_TYPE_OID,
            SqlType::new(SqlTypeKind::Varchar),
        ),
        (
            "sql_identifier",
            INFORMATION_SCHEMA_SQL_IDENTIFIER_TYPE_OID,
            INFORMATION_SCHEMA_SQL_IDENTIFIER_ARRAY_TYPE_OID,
            NAME_TYPE_OID,
            SqlType::new(SqlTypeKind::Name),
        ),
        (
            "time_stamp",
            INFORMATION_SCHEMA_TIME_STAMP_TYPE_OID,
            INFORMATION_SCHEMA_TIME_STAMP_ARRAY_TYPE_OID,
            TIMESTAMPTZ_TYPE_OID,
            SqlType::with_time_precision(SqlTypeKind::TimestampTz, 2),
        ),
        (
            "yes_or_no",
            INFORMATION_SCHEMA_YES_OR_NO_TYPE_OID,
            INFORMATION_SCHEMA_YES_OR_NO_ARRAY_TYPE_OID,
            VARCHAR_TYPE_OID,
            SqlType::with_char_len(SqlTypeKind::Varchar, 3),
        ),
    ];

    specs
        .into_iter()
        .flat_map(|(name, oid, array_oid, base_oid, sql_type)| {
            let base = existing
                .iter()
                .find(|row| row.oid == base_oid)
                .expect("information_schema domain base type");
            let domain_sql_type = sql_type.with_identity(oid, 0);
            [
                PgTypeRow {
                    oid,
                    typname: name.into(),
                    typnamespace: INFORMATION_SCHEMA_NAMESPACE_OID,
                    typowner: BOOTSTRAP_SUPERUSER_OID,
                    typacl: None,
                    typlen: base.typlen,
                    typbyval: base.typbyval,
                    typtype: 'd',
                    typisdefined: true,
                    typalign: base.typalign,
                    typstorage: base.typstorage,
                    typrelid: 0,
                    typsubscript: 0,
                    typelem: 0,
                    typarray: array_oid,
                    typinput: 0,
                    typoutput: base.typoutput,
                    typreceive: base.typreceive,
                    typsend: base.typsend,
                    typmodin: base.typmodin,
                    typmodout: base.typmodout,
                    typdelim: base.typdelim,
                    typanalyze: base.typanalyze,
                    typbasetype: base_oid,
                    typcollation: base.typcollation,
                    sql_type: domain_sql_type,
                },
                PgTypeRow {
                    oid: array_oid,
                    typname: format!("_{name}"),
                    typnamespace: INFORMATION_SCHEMA_NAMESPACE_OID,
                    typowner: BOOTSTRAP_SUPERUSER_OID,
                    typacl: None,
                    typlen: -1,
                    typbyval: false,
                    typtype: 'b',
                    typisdefined: true,
                    typalign: if base.typalign == AttributeAlign::Double {
                        AttributeAlign::Double
                    } else {
                        AttributeAlign::Int
                    },
                    typstorage: AttributeStorage::Extended,
                    typrelid: 0,
                    typsubscript: ARRAY_SUBSCRIPT_HANDLER_PROC_OID,
                    typelem: oid,
                    typarray: 0,
                    typinput: ARRAY_IN_PROC_OID,
                    typoutput: ARRAY_OUT_PROC_OID,
                    typreceive: ARRAY_RECV_PROC_OID,
                    typsend: ARRAY_SEND_PROC_OID,
                    typmodin: 0,
                    typmodout: 0,
                    typdelim: ',',
                    typanalyze: ARRAY_TYPANALYZE_PROC_OID,
                    typbasetype: 0,
                    typcollation: 0,
                    sql_type: SqlType::array_of(domain_sql_type).with_identity(array_oid, 0),
                },
            ]
        })
        .collect()
}

pub fn composite_type_row(
    name: &str,
    oid: u32,
    namespace_oid: u32,
    relid: u32,
    array_oid: u32,
) -> PgTypeRow {
    composite_type_row_with_owner(
        name,
        oid,
        namespace_oid,
        BOOTSTRAP_SUPERUSER_OID,
        relid,
        array_oid,
    )
}

pub fn composite_type_row_with_owner(
    name: &str,
    oid: u32,
    namespace_oid: u32,
    owner_oid: u32,
    relid: u32,
    array_oid: u32,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: namespace_oid,
        typowner: owner_oid,
        typacl: None,
        typlen: -1,
        typbyval: false,
        typtype: 'c',
        typisdefined: true,
        typalign: AttributeAlign::Double,
        typstorage: AttributeStorage::Extended,
        typrelid: relid,
        typsubscript: 0,
        typelem: 0,
        typarray: array_oid,
        typinput: RECORD_IN_PROC_OID,
        typoutput: RECORD_OUT_PROC_OID,
        typreceive: RECORD_RECV_PROC_OID,
        typsend: RECORD_SEND_PROC_OID,
        typmodin: 0,
        typmodout: 0,
        typdelim: ',',
        typanalyze: 0,
        typbasetype: 0,
        typcollation: 0,
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
    composite_array_type_row_with_owner(
        name,
        oid,
        namespace_oid,
        BOOTSTRAP_SUPERUSER_OID,
        elem_oid,
        relid,
    )
}

pub fn composite_array_type_row_with_owner(
    name: &str,
    oid: u32,
    namespace_oid: u32,
    owner_oid: u32,
    elem_oid: u32,
    relid: u32,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: format!("_{name}"),
        typnamespace: namespace_oid,
        typowner: owner_oid,
        typacl: None,
        typlen: -1,
        typbyval: false,
        typtype: 'b',
        typisdefined: true,
        typalign: AttributeAlign::Double,
        typstorage: AttributeStorage::Extended,
        typrelid: 0,
        typsubscript: ARRAY_SUBSCRIPT_HANDLER_PROC_OID,
        typelem: elem_oid,
        typarray: 0,
        typinput: ARRAY_IN_PROC_OID,
        typoutput: ARRAY_OUT_PROC_OID,
        typreceive: ARRAY_RECV_PROC_OID,
        typsend: ARRAY_SEND_PROC_OID,
        typmodin: 0,
        typmodout: 0,
        typdelim: ',',
        typanalyze: ARRAY_TYPANALYZE_PROC_OID,
        typbasetype: 0,
        typcollation: 0,
        sql_type: SqlType::array_of(SqlType::named_composite(elem_oid, relid)),
    }
}

fn annotate_array_type_links(rows: &mut [PgTypeRow]) {
    let snapshot = rows.to_vec();
    for row in rows.iter_mut() {
        if row.sql_type.is_array {
            let elem_oid = array_element_oid_override(row.oid)
                .or_else(|| {
                    snapshot
                        .iter()
                        .find(|base_row| {
                            !base_row.sql_type.is_array
                                && SqlType::array_of(base_row.sql_type) == row.sql_type
                        })
                        .map(|base_row| base_row.oid)
                })
                .unwrap_or(row.typelem);
            if elem_oid != 0 {
                row.typelem = elem_oid;
                if let Some(base_row) = snapshot.iter().find(|base_row| base_row.oid == elem_oid) {
                    row.typalign = if base_row.typalign == AttributeAlign::Double {
                        AttributeAlign::Double
                    } else {
                        AttributeAlign::Int
                    };
                }
            }
            row.typsubscript = ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        } else {
            annotate_builtin_element_type(row);
            if let Some(array_oid) = snapshot
                .iter()
                .find(|array_row| {
                    array_element_oid_override(array_row.oid) == Some(row.oid)
                        || array_row.typelem == row.oid
                        || array_row.sql_type == SqlType::array_of(row.sql_type)
                })
                .map(|array_row| array_row.oid)
            {
                row.typarray = array_oid;
            }
        }
    }
}

fn array_element_oid_override(array_oid: u32) -> Option<u32> {
    match array_oid {
        CID_ARRAY_TYPE_OID => Some(CID_TYPE_OID),
        XID8_ARRAY_TYPE_OID => Some(XID8_TYPE_OID),
        TXID_SNAPSHOT_ARRAY_TYPE_OID => Some(TXID_SNAPSHOT_TYPE_OID),
        PG_SNAPSHOT_ARRAY_TYPE_OID => Some(PG_SNAPSHOT_TYPE_OID),
        ACLITEM_ARRAY_TYPE_OID => Some(ACLITEM_TYPE_OID),
        GTSVECTOR_ARRAY_TYPE_OID => Some(GTSVECTOR_TYPE_OID),
        _ => None,
    }
}

fn annotate_builtin_element_type(row: &mut PgTypeRow) {
    match row.oid {
        INT2VECTOR_TYPE_OID => {
            row.typelem = INT2_TYPE_OID;
            row.typsubscript = ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        }
        OIDVECTOR_TYPE_OID => {
            row.typelem = OID_TYPE_OID;
            row.typsubscript = ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        }
        POINT_TYPE_OID | LINE_TYPE_OID => {
            row.typelem = FLOAT8_TYPE_OID;
            row.typsubscript = RAW_ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        }
        LSEG_TYPE_OID | BOX_TYPE_OID => {
            row.typelem = POINT_TYPE_OID;
            row.typsubscript = RAW_ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        }
        _ => {}
    }
}

fn typbyval_for_sql_type(sql_type: SqlType, typlen: i16) -> bool {
    if sql_type.is_array || typlen < 0 {
        return false;
    }
    matches!(
        sql_type.kind,
        SqlTypeKind::Bool
            | SqlTypeKind::InternalChar
            | SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Oid
            | SqlTypeKind::RegProc
            | SqlTypeKind::RegClass
            | SqlTypeKind::RegType
            | SqlTypeKind::RegRole
            | SqlTypeKind::RegNamespace
            | SqlTypeKind::RegOper
            | SqlTypeKind::RegOperator
            | SqlTypeKind::RegProcedure
            | SqlTypeKind::RegCollation
            | SqlTypeKind::Xid
            | SqlTypeKind::Date
            | SqlTypeKind::Float4
            | SqlTypeKind::Int8
            | SqlTypeKind::Float8
            | SqlTypeKind::Money
            | SqlTypeKind::Time
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::PgLsn
            | SqlTypeKind::Enum
            | SqlTypeKind::AnyElement
            | SqlTypeKind::AnyEnum
    )
}

fn typtype_for_sql_type(sql_type: SqlType) -> char {
    if sql_type.is_array {
        return 'b';
    }
    match sql_type.kind {
        SqlTypeKind::Range => 'r',
        SqlTypeKind::Multirange => 'm',
        SqlTypeKind::Composite => 'c',
        SqlTypeKind::Enum => 'e',
        SqlTypeKind::AnyArray
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::AnyCompatibleArray
        | SqlTypeKind::AnyCompatibleMultirange
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyElement
        | SqlTypeKind::AnyEnum
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyRange
        | SqlTypeKind::Cstring
        | SqlTypeKind::FdwHandler
        | SqlTypeKind::Internal
        | SqlTypeKind::Record
        | SqlTypeKind::Shell
        | SqlTypeKind::Trigger
        | SqlTypeKind::EventTrigger
        | SqlTypeKind::Void => 'p',
        _ => 'b',
    }
}

pub fn annotate_catalog_type_io_procs(rows: &mut [PgTypeRow]) {
    for row in rows.iter_mut() {
        if row.typtype == 'd' {
            row.typinput = DOMAIN_IN_PROC_OID;
            row.typreceive = DOMAIN_RECV_PROC_OID;
            if row.typoutput == 0 {
                row.typoutput = synthetic_type_output_proc_oid(row.oid);
            }
            continue;
        }

        if row.sql_type.is_array {
            row.typsubscript = ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
            row.typinput = ARRAY_IN_PROC_OID;
            row.typoutput = ARRAY_OUT_PROC_OID;
            row.typreceive = ARRAY_RECV_PROC_OID;
            row.typsend = ARRAY_SEND_PROC_OID;
            row.typanalyze = ARRAY_TYPANALYZE_PROC_OID;
            continue;
        }

        row.typinput = match row.oid {
            INTERNAL_TYPE_OID => INTERNAL_IN_PROC_OID,
            ANYARRAYOID => ANYARRAY_IN_PROC_OID,
            ANYELEMENTOID => ANYELEMENT_IN_PROC_OID,
            ANYENUMOID => ANYENUM_IN_PROC_OID,
            ANYNONARRAYOID => ANYNONARRAY_IN_PROC_OID,
            ANYRANGEOID => ANYRANGE_IN_PROC_OID,
            ANYMULTIRANGEOID => ANYMULTIRANGE_IN_PROC_OID,
            ANYCOMPATIBLEOID => ANYCOMPATIBLE_IN_PROC_OID,
            ANYCOMPATIBLEARRAYOID => ANYCOMPATIBLEARRAY_IN_PROC_OID,
            ANYCOMPATIBLENONARRAYOID => ANYCOMPATIBLENONARRAY_IN_PROC_OID,
            ANYCOMPATIBLERANGEOID => ANYCOMPATIBLERANGE_IN_PROC_OID,
            ANYCOMPATIBLEMULTIRANGEOID => ANYCOMPATIBLEMULTIRANGE_IN_PROC_OID,
            CSTRING_TYPE_OID => CSTRING_IN_PROC_OID,
            BOOL_TYPE_OID => BOOL_IN_PROC_OID,
            INT4_TYPE_OID => INT4_IN_PROC_OID,
            VARCHAR_TYPE_OID => VARCHAR_IN_PROC_OID,
            VOID_TYPE_OID => VOID_IN_PROC_OID,
            RECORD_TYPE_OID => RECORD_IN_PROC_OID,
            REFCURSOR_TYPE_OID => TEXT_IN_PROC_OID,
            INT2VECTOR_TYPE_OID => INT2VECTOR_IN_PROC_OID,
            OIDVECTOR_TYPE_OID => OIDVECTOR_IN_PROC_OID,
            UNKNOWN_TYPE_OID => UNKNOWN_IN_PROC_OID,
            TIMESTAMP_TYPE_OID => 1312,
            TIMESTAMPTZ_TYPE_OID => 1150,
            TXID_SNAPSHOT_TYPE_OID => 2939,
            PG_SNAPSHOT_TYPE_OID => 5055,
            TSVECTOR_TYPE_OID => 3610,
            TSQUERY_TYPE_OID => 3612,
            _ if matches!(
                row.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            ) =>
            {
                RECORD_IN_PROC_OID
            }
            _ if matches!(row.sql_type.kind, SqlTypeKind::Enum) => ENUM_IN_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Range) => RANGE_IN_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Multirange) => MULTIRANGE_IN_PROC_OID,
            _ if row.typinput != 0 => row.typinput,
            _ => synthetic_type_input_proc_oid(row.oid),
        };
        if row.sql_type.is_array {
            row.typoutput = ARRAY_OUT_PROC_OID;
            row.typreceive = ARRAY_RECV_PROC_OID;
            row.typsend = ARRAY_SEND_PROC_OID;
            row.typanalyze = ARRAY_TYPANALYZE_PROC_OID;
            row.typsubscript = ARRAY_SUBSCRIPT_HANDLER_PROC_OID;
        }
        row.typoutput = match row.oid {
            ANYARRAYOID => ANYARRAY_OUT_PROC_OID,
            ANYENUMOID => ANYENUM_OUT_PROC_OID,
            ANYRANGEOID => ANYRANGE_OUT_PROC_OID,
            ANYMULTIRANGEOID => ANYMULTIRANGE_OUT_PROC_OID,
            ANYCOMPATIBLEARRAYOID => ANYCOMPATIBLEARRAY_OUT_PROC_OID,
            ANYCOMPATIBLERANGEOID => ANYCOMPATIBLERANGE_OUT_PROC_OID,
            ANYCOMPATIBLEMULTIRANGEOID => ANYCOMPATIBLEMULTIRANGE_OUT_PROC_OID,
            CSTRING_TYPE_OID => CSTRING_OUT_PROC_OID,
            BOOL_TYPE_OID => BOOL_OUT_PROC_OID,
            INT4_TYPE_OID => INT4_OUT_PROC_OID,
            VARCHAR_TYPE_OID => VARCHAR_OUT_PROC_OID,
            NUMERIC_TYPE_OID => NUMERIC_OUT_PROC_OID,
            VOID_TYPE_OID => VOID_OUT_PROC_OID,
            RECORD_TYPE_OID => RECORD_OUT_PROC_OID,
            REFCURSOR_TYPE_OID => TEXT_OUT_PROC_OID,
            INT2VECTOR_TYPE_OID => INT2VECTOR_OUT_PROC_OID,
            OIDVECTOR_TYPE_OID => OIDVECTOR_OUT_PROC_OID,
            UNKNOWN_TYPE_OID => UNKNOWN_OUT_PROC_OID,
            TXID_SNAPSHOT_TYPE_OID => 2940,
            PG_SNAPSHOT_TYPE_OID => 5056,
            TSVECTOR_TYPE_OID => 3611,
            TSQUERY_TYPE_OID => 3613,
            _ if matches!(
                row.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            ) =>
            {
                RECORD_OUT_PROC_OID
            }
            _ if matches!(row.sql_type.kind, SqlTypeKind::Enum) => ENUM_OUT_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Range) => RANGE_OUT_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Multirange) => MULTIRANGE_OUT_PROC_OID,
            _ if row.typoutput != 0 => row.typoutput,
            _ => synthetic_type_output_proc_oid(row.oid),
        };
        row.typreceive = match row.oid {
            ANYARRAYOID => 2502,
            ANYCOMPATIBLEARRAYOID => 5090,
            RECORD_TYPE_OID => RECORD_RECV_PROC_OID,
            REFCURSOR_TYPE_OID => TEXT_RECV_PROC_OID,
            INT2VECTOR_TYPE_OID => INT2VECTOR_RECV_PROC_OID,
            OIDVECTOR_TYPE_OID => OIDVECTOR_RECV_PROC_OID,
            _ if matches!(
                row.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            ) =>
            {
                RECORD_RECV_PROC_OID
            }
            _ if matches!(row.sql_type.kind, SqlTypeKind::Enum) => ENUM_RECV_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Range) => RANGE_RECV_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Multirange) => MULTIRANGE_RECV_PROC_OID,
            _ => row.typreceive,
        };
        row.typsend = match row.oid {
            RECORD_TYPE_OID => RECORD_SEND_PROC_OID,
            REFCURSOR_TYPE_OID => TEXT_SEND_PROC_OID,
            INT2VECTOR_TYPE_OID => INT2VECTOR_SEND_PROC_OID,
            OIDVECTOR_TYPE_OID => OIDVECTOR_SEND_PROC_OID,
            _ if matches!(
                row.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            ) =>
            {
                RECORD_SEND_PROC_OID
            }
            _ if matches!(row.sql_type.kind, SqlTypeKind::Enum) => ENUM_SEND_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Range) => RANGE_SEND_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Multirange) => MULTIRANGE_SEND_PROC_OID,
            _ => row.typsend,
        };
        match row.oid {
            VARCHAR_TYPE_OID => {
                row.typmodin = VARCHAR_TYPMOD_IN_PROC_OID;
                row.typmodout = VARCHAR_TYPMOD_OUT_PROC_OID;
            }
            NUMERIC_TYPE_OID => {
                row.typmodin = NUMERIC_TYPMOD_IN_PROC_OID;
                row.typmodout = NUMERIC_TYPMOD_OUT_PROC_OID;
            }
            _ => {}
        }
        row.typanalyze = match row.oid {
            _ if matches!(row.sql_type.kind, SqlTypeKind::Range) => RANGE_TYPANALYZE_PROC_OID,
            _ if matches!(row.sql_type.kind, SqlTypeKind::Multirange) => {
                MULTIRANGE_TYPANALYZE_PROC_OID
            }
            _ => row.typanalyze,
        };
    }

    let annotated_rows = rows.to_vec();
    for row in rows.iter_mut().filter(|row| row.typtype == 'd') {
        if let Some(base) = annotated_rows
            .iter()
            .find(|base| base.oid == row.typbasetype)
        {
            row.typoutput = base.typoutput;
            row.typsend = base.typsend;
            row.typmodin = base.typmodin;
            row.typmodout = base.typmodout;
            row.typanalyze = base.typanalyze;
        }
    }

    let annotated_rows = rows.to_vec();
    for row in rows.iter_mut().filter(|row| row.sql_type.is_array) {
        if let Some(element) = annotated_rows
            .iter()
            .find(|element| element.oid == row.typelem)
        {
            row.typmodin = element.typmodin;
            row.typmodout = element.typmodout;
        }
    }
}

pub fn synthetic_type_input_proc_oid(type_oid: u32) -> u32 {
    PG_RUST_TYPE_INPUT_PROC_BASE + type_oid
}

pub fn synthetic_type_output_proc_oid(type_oid: u32) -> u32 {
    PG_RUST_TYPE_OUTPUT_PROC_BASE + type_oid
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
                "pg_type",
                "pg_proc",
                "pg_attribute",
                "pg_class",
                "pg_database",
                "pg_statistic",
                "pg_statistic_ext",
                "pg_statistic_ext_data",
                "_pg_type",
                "_pg_proc",
                "_pg_attribute",
                "_pg_class",
                "_pg_database",
                "_pg_statistic",
                "_pg_statistic_ext",
                "_pg_statistic_ext_data",
            ]
        );
        assert!(rows.iter().all(|row| row.oid != 0));
        assert_eq!(rows[0].oid, PG_TYPE_ROWTYPE_OID);
        assert_eq!(rows[1].oid, PG_PROC_ROWTYPE_OID);
        assert_eq!(rows[3].oid, PG_CLASS_ROWTYPE_OID);
        assert_eq!(rows[4].oid, PG_DATABASE_ROWTYPE_OID);
        assert_eq!(rows[5].oid, PG_STATISTIC_ROWTYPE_OID);
        assert_eq!(rows[6].oid, PG_STATISTIC_EXT_ROWTYPE_OID);
        assert_eq!(rows[7].oid, PG_STATISTIC_EXT_DATA_ROWTYPE_OID);
        assert_eq!(rows[0].typarray, PG_TYPE_ARRAY_TYPE_OID);
        assert!(
            rows.iter()
                .any(|row| row.oid == PG_TYPE_ARRAY_TYPE_OID && row.typelem == PG_TYPE_ROWTYPE_OID)
        );
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
    fn builtin_type_array_links_cover_identity_and_vector_types() {
        let rows = builtin_type_rows();
        for (base_oid, array_oid, elem_oid) in [
            (
                INTERNAL_CHAR_TYPE_OID,
                INTERNAL_CHAR_ARRAY_TYPE_OID,
                INTERNAL_CHAR_TYPE_OID,
            ),
            (CID_TYPE_OID, CID_ARRAY_TYPE_OID, CID_TYPE_OID),
            (ACLITEM_TYPE_OID, ACLITEM_ARRAY_TYPE_OID, ACLITEM_TYPE_OID),
            (
                INT2VECTOR_TYPE_OID,
                INT2VECTOR_ARRAY_TYPE_OID,
                INT2VECTOR_TYPE_OID,
            ),
            (
                OIDVECTOR_TYPE_OID,
                OIDVECTOR_ARRAY_TYPE_OID,
                OIDVECTOR_TYPE_OID,
            ),
            (
                GTSVECTOR_TYPE_OID,
                GTSVECTOR_ARRAY_TYPE_OID,
                GTSVECTOR_TYPE_OID,
            ),
            (POINT_TYPE_OID, POINT_ARRAY_TYPE_OID, POINT_TYPE_OID),
        ] {
            let base = rows
                .iter()
                .find(|row| row.oid == base_oid)
                .expect("base type row");
            let array = rows
                .iter()
                .find(|row| row.oid == array_oid)
                .expect("array type row");
            assert_eq!(base.typarray, array_oid);
            assert_eq!(array.typelem, elem_oid);
            assert_eq!(array.typtype, 'b');
        }

        let int2vector = rows
            .iter()
            .find(|row| row.oid == INT2VECTOR_TYPE_OID)
            .expect("int2vector type row");
        assert_eq!(int2vector.typelem, INT2_TYPE_OID);
        assert_eq!(int2vector.typinput, INT2VECTOR_IN_PROC_OID);
        assert_eq!(int2vector.typoutput, INT2VECTOR_OUT_PROC_OID);
    }

    #[test]
    fn builtin_types_include_statistics_payload_types() {
        for (oid, name) in [
            (PG_NDISTINCT_TYPE_OID, "pg_ndistinct"),
            (PG_DEPENDENCIES_TYPE_OID, "pg_dependencies"),
            (PG_BRIN_BLOOM_SUMMARY_TYPE_OID, "pg_brin_bloom_summary"),
            (
                PG_BRIN_MINMAX_MULTI_SUMMARY_TYPE_OID,
                "pg_brin_minmax_multi_summary",
            ),
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
    fn builtin_types_include_cstring() {
        let rows = builtin_type_rows();
        let row = rows
            .iter()
            .find(|row| row.oid == CSTRING_TYPE_OID)
            .expect("cstring row");
        assert_eq!(row.typname, "cstring");
        assert_eq!(row.typlen, -2);
        assert_eq!(row.typalign, AttributeAlign::Char);
        assert_eq!(row.typstorage, AttributeStorage::Plain);
        assert_eq!(row.typarray, CSTRING_ARRAY_TYPE_OID);
        assert_eq!(row.sql_type, SqlType::new(SqlTypeKind::Cstring));

        let array_row = rows
            .iter()
            .find(|row| row.oid == CSTRING_ARRAY_TYPE_OID)
            .expect("_cstring row");
        assert_eq!(array_row.typname, "_cstring");
        assert_eq!(array_row.typelem, CSTRING_TYPE_OID);
        assert_eq!(
            array_row.sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Cstring))
        );
    }

    #[test]
    fn builtin_types_include_unknown() {
        let row = builtin_type_rows()
            .into_iter()
            .find(|row| row.oid == UNKNOWN_TYPE_OID)
            .expect("unknown row");
        assert_eq!(row.typname, "unknown");
        assert_eq!(row.typlen, -2);
        assert_eq!(row.typalign, AttributeAlign::Char);
        assert_eq!(row.typstorage, AttributeStorage::Plain);
        assert_eq!(row.typinput, UNKNOWN_IN_PROC_OID);
        assert_eq!(row.typoutput, UNKNOWN_OUT_PROC_OID);
        assert_eq!(
            row.sql_type,
            SqlType::new(SqlTypeKind::Text).with_identity(UNKNOWN_TYPE_OID, 0)
        );
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
