use super::{
    pg_am_desc, pg_amop_desc, pg_amproc_desc, pg_attrdef_desc, pg_auth_members_desc,
    pg_authid_desc, pg_cast_desc, pg_class_desc, pg_collation_desc, pg_constraint_desc,
    pg_database_desc, pg_depend_desc, pg_index_desc, pg_inherits_desc, pg_language_desc,
    pg_largeobject_metadata_desc, pg_namespace_desc, pg_opclass_desc, pg_operator_desc,
    pg_opfamily_desc, pg_proc_desc, pg_publication_desc, pg_publication_namespace_desc,
    pg_publication_rel_desc, pg_rewrite_desc, pg_statistic_desc, pg_statistic_ext_data_desc,
    pg_statistic_ext_desc, pg_tablespace_desc, pg_type_desc,
};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::parser::SqlTypeKind;
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    ANYARRAYOID, BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID,
    BOX_TYPE_OID, BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CIRCLE_TYPE_OID, DATE_ARRAY_TYPE_OID, DATE_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT2VECTOR_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, INTERVAL_ARRAY_TYPE_OID,
    INTERVAL_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID, JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, LINE_TYPE_OID, LSEG_TYPE_OID, MONEY_ARRAY_TYPE_OID,
    MONEY_TYPE_OID, NAME_ARRAY_TYPE_OID, NAME_TYPE_OID, NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID,
    OID_ARRAY_TYPE_OID, OID_TYPE_OID, OIDVECTOR_TYPE_OID, PATH_TYPE_OID, PG_AM_RELATION_OID,
    PG_AMOP_RELATION_OID, PG_AMPROC_RELATION_OID, PG_ATTRDEF_RELATION_OID,
    PG_ATTRIBUTE_RELATION_OID, PG_AUTH_MEMBERS_RELATION_OID, PG_AUTHID_RELATION_OID,
    PG_CAST_RELATION_OID, PG_CLASS_RELATION_OID, PG_COLLATION_RELATION_OID,
    PG_CONSTRAINT_RELATION_OID, PG_DATABASE_RELATION_OID, PG_DEPEND_RELATION_OID,
    PG_INDEX_RELATION_OID, PG_INHERITS_RELATION_OID, PG_LANGUAGE_RELATION_OID,
    PG_LARGEOBJECT_METADATA_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_NODE_TREE_TYPE_OID,
    PG_OPCLASS_RELATION_OID, PG_OPERATOR_RELATION_OID, PG_OPFAMILY_RELATION_OID,
    PG_PROC_RELATION_OID, PG_PUBLICATION_NAMESPACE_RELATION_OID, PG_PUBLICATION_REL_RELATION_OID,
    PG_PUBLICATION_RELATION_OID, PG_REWRITE_RELATION_OID, PG_STATISTIC_EXT_DATA_RELATION_OID,
    PG_STATISTIC_EXT_RELATION_OID, PG_STATISTIC_RELATION_OID, PG_TABLESPACE_RELATION_OID,
    PG_TYPE_RELATION_OID, POINT_TYPE_OID, POLYGON_TYPE_OID, REGCONFIG_ARRAY_TYPE_OID,
    REGCONFIG_TYPE_OID, REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, TEXT_ARRAY_TYPE_OID,
    TEXT_TYPE_OID, TID_ARRAY_TYPE_OID, TID_TYPE_OID, TIME_ARRAY_TYPE_OID, TIME_TYPE_OID,
    TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_ARRAY_TYPE_OID, TIMESTAMPTZ_TYPE_OID,
    TIMETZ_ARRAY_TYPE_OID, TIMETZ_TYPE_OID, TSQUERY_ARRAY_TYPE_OID, TSQUERY_TYPE_OID,
    TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID, VARBIT_ARRAY_TYPE_OID, VARBIT_TYPE_OID,
    VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, XID_ARRAY_TYPE_OID, XID_TYPE_OID, XML_ARRAY_TYPE_OID,
    XML_TYPE_OID, bootstrap_composite_type_rows, builtin_type_rows, range_type_ref_for_sql_type,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAttributeRow {
    pub attrelid: u32,
    pub attname: String,
    pub atttypid: u32,
    pub attlen: i16,
    pub attnum: i16,
    pub attnotnull: bool,
    pub attisdropped: bool,
    pub atttypmod: i32,
    pub attalign: AttributeAlign,
    pub attstorage: AttributeStorage,
    pub attcompression: AttributeCompression,
    pub attstattarget: i16,
    pub attinhcount: i16,
    pub attislocal: bool,
    pub attidentity: char,
    pub attgenerated: char,
    pub sql_type: SqlType,
}

pub fn pg_attribute_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("attrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("attname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("atttypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("attlen", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attnotnull", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("attisdropped", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("atttypmod", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("attalign", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("attstorage", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc(
                "attcompression",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("attstattarget", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attinhcount", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attislocal", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "attidentity",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "attgenerated",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
        ],
    }
}

pub fn bootstrap_pg_attribute_rows() -> Vec<PgAttributeRow> {
    let mut rows = Vec::new();
    rows.extend(attribute_rows_for_desc(
        PG_NAMESPACE_RELATION_OID,
        &pg_namespace_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_TYPE_RELATION_OID,
        &pg_type_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_PROC_RELATION_OID,
        &pg_proc_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_LANGUAGE_RELATION_OID,
        &pg_language_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_OPERATOR_RELATION_OID,
        &pg_operator_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_ATTRIBUTE_RELATION_OID,
        &pg_attribute_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_CLASS_RELATION_OID,
        &pg_class_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_AUTHID_RELATION_OID,
        &pg_authid_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_AUTH_MEMBERS_RELATION_OID,
        &pg_auth_members_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_COLLATION_RELATION_OID,
        &pg_collation_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_LARGEOBJECT_METADATA_RELATION_OID,
        &pg_largeobject_metadata_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_DATABASE_RELATION_OID,
        &pg_database_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_TABLESPACE_RELATION_OID,
        &pg_tablespace_desc(),
    ));
    rows.extend(attribute_rows_for_desc(PG_AM_RELATION_OID, &pg_am_desc()));
    rows.extend(attribute_rows_for_desc(
        PG_AMOP_RELATION_OID,
        &pg_amop_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_AMPROC_RELATION_OID,
        &pg_amproc_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_ATTRDEF_RELATION_OID,
        &pg_attrdef_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_CAST_RELATION_OID,
        &pg_cast_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_CONSTRAINT_RELATION_OID,
        &pg_constraint_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_DEPEND_RELATION_OID,
        &pg_depend_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_INDEX_RELATION_OID,
        &pg_index_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_INHERITS_RELATION_OID,
        &pg_inherits_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_REWRITE_RELATION_OID,
        &pg_rewrite_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_STATISTIC_RELATION_OID,
        &pg_statistic_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_STATISTIC_EXT_RELATION_OID,
        &pg_statistic_ext_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_STATISTIC_EXT_DATA_RELATION_OID,
        &pg_statistic_ext_data_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_PUBLICATION_RELATION_OID,
        &pg_publication_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_PUBLICATION_REL_RELATION_OID,
        &pg_publication_rel_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_PUBLICATION_NAMESPACE_RELATION_OID,
        &pg_publication_namespace_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_OPCLASS_RELATION_OID,
        &pg_opclass_desc(),
    ));
    rows.extend(attribute_rows_for_desc(
        PG_OPFAMILY_RELATION_OID,
        &pg_opfamily_desc(),
    ));
    rows
}

fn attribute_rows_for_desc(relid: u32, desc: &RelationDesc) -> Vec<PgAttributeRow> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(idx, column)| PgAttributeRow {
            attrelid: relid,
            attname: column.name.clone(),
            atttypid: sql_type_oid(column.sql_type),
            attlen: column.storage.attlen,
            attnum: idx.saturating_add(1) as i16,
            attnotnull: !column.storage.nullable,
            attisdropped: column.dropped,
            atttypmod: column.sql_type.typmod,
            attalign: column.storage.attalign,
            attstorage: column.storage.attstorage,
            attcompression: column.storage.attcompression,
            attstattarget: column.attstattarget,
            attinhcount: column.attinhcount,
            attislocal: column.attislocal,
            attidentity: column
                .identity
                .map(|kind| kind.catalog_char())
                .unwrap_or('\0'),
            attgenerated: column
                .generated
                .map(|kind| kind.catalog_char())
                .unwrap_or('\0'),
            sql_type: column.sql_type,
        })
        .collect()
}

fn sql_type_oid(sql_type: SqlType) -> u32 {
    if !sql_type.is_array && sql_type.type_oid != 0 {
        return sql_type.type_oid;
    }
    if let Some(row) = builtin_type_rows()
        .into_iter()
        .chain(bootstrap_composite_type_rows())
        .find(|row| row.sql_type == sql_type)
    {
        return row.oid;
    }
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        if sql_type.is_array {
            if sql_type.type_oid != 0 && matches!(sql_type.kind, SqlTypeKind::Range) {
                return sql_type.type_oid;
            }
            if let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == range_type.type_oid())
            {
                return array_row.oid;
            }
            unreachable!("range arrays are unsupported");
        }
        return range_type.type_oid();
    }
    if let Some(multirange_type) =
        crate::include::catalog::multirange_type_ref_for_sql_type(sql_type)
    {
        if sql_type.is_array {
            if sql_type.type_oid != 0 && matches!(sql_type.kind, SqlTypeKind::Multirange) {
                return sql_type.type_oid;
            }
            if let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == multirange_type.type_oid())
            {
                return array_row.oid;
            }
            unreachable!("multirange arrays are unsupported");
        }
        return multirange_type.type_oid();
    }
    if !sql_type.is_array && sql_type.type_oid != 0 {
        return sql_type.type_oid;
    }
    match (sql_type.kind, sql_type.is_array) {
        (SqlTypeKind::Range, false) => sql_type.type_oid,
        (SqlTypeKind::Range, true) => sql_type.type_oid,
        (SqlTypeKind::Multirange, false) => sql_type.type_oid,
        (SqlTypeKind::Multirange, true) => sql_type.type_oid,
        (SqlTypeKind::AnyElement, false) => crate::include::catalog::ANYELEMENTOID,
        (SqlTypeKind::AnyElement, true) => unreachable!("anyelement arrays are unsupported"),
        (SqlTypeKind::AnyArray, false) => ANYARRAYOID,
        (SqlTypeKind::AnyArray, true) => unreachable!("anyarray arrays are unsupported"),
        (SqlTypeKind::AnyRange, false) => crate::include::catalog::ANYRANGEOID,
        (SqlTypeKind::AnyRange, true) => unreachable!("anyrange arrays are unsupported"),
        (SqlTypeKind::AnyMultirange, false) => crate::include::catalog::ANYMULTIRANGEOID,
        (SqlTypeKind::AnyMultirange, true) => unreachable!("anymultirange arrays are unsupported"),
        (SqlTypeKind::AnyCompatible, false) => crate::include::catalog::ANYCOMPATIBLEOID,
        (SqlTypeKind::AnyCompatible, true) => {
            unreachable!("anycompatible arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleArray, false) => crate::include::catalog::ANYCOMPATIBLEARRAYOID,
        (SqlTypeKind::AnyCompatibleArray, true) => {
            unreachable!("anycompatiblearray arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleRange, false) => crate::include::catalog::ANYCOMPATIBLERANGEOID,
        (SqlTypeKind::AnyCompatibleRange, true) => {
            unreachable!("anycompatiblerange arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleMultirange, false) => {
            crate::include::catalog::ANYCOMPATIBLEMULTIRANGEOID
        }
        (SqlTypeKind::AnyCompatibleMultirange, true) => {
            unreachable!("anycompatiblemultirange arrays are unsupported")
        }
        (SqlTypeKind::Record, false) => sql_type.type_oid,
        (SqlTypeKind::Record, true) => crate::include::catalog::RECORD_ARRAY_TYPE_OID,
        (SqlTypeKind::Composite, false) => sql_type.type_oid,
        (SqlTypeKind::Composite, true) => crate::include::catalog::RECORD_ARRAY_TYPE_OID,
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
        (SqlTypeKind::Internal, false) => crate::include::catalog::INTERNAL_TYPE_OID,
        (SqlTypeKind::Internal, true) => unreachable!("internal arrays are unsupported"),
        (SqlTypeKind::Void, false) => crate::include::catalog::VOID_TYPE_OID,
        (SqlTypeKind::Void, true) => unreachable!("void arrays are unsupported"),
        (SqlTypeKind::Trigger, false) => crate::include::catalog::TRIGGER_TYPE_OID,
        (SqlTypeKind::Trigger, true) => unreachable!("trigger arrays are unsupported"),
        (SqlTypeKind::FdwHandler, false) => crate::include::catalog::FDW_HANDLER_TYPE_OID,
        (SqlTypeKind::FdwHandler, true) => unreachable!("fdw_handler arrays are unsupported"),
        (SqlTypeKind::Int8, false) => INT8_TYPE_OID,
        (SqlTypeKind::Int8, true) => INT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Name, false) => NAME_TYPE_OID,
        (SqlTypeKind::Name, true) => NAME_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2, false) => INT2_TYPE_OID,
        (SqlTypeKind::Int2, true) => INT2_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2Vector, false) => INT2VECTOR_TYPE_OID,
        (SqlTypeKind::Int2Vector, true) => unreachable!("int2vector arrays are unsupported"),
        (SqlTypeKind::Int4, false) => INT4_TYPE_OID,
        (SqlTypeKind::Int4, true) => INT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Text, false) => TEXT_TYPE_OID,
        (SqlTypeKind::Text, true) => TEXT_ARRAY_TYPE_OID,
        (SqlTypeKind::Oid, false) => OID_TYPE_OID,
        (SqlTypeKind::Oid, true) => OID_ARRAY_TYPE_OID,
        (SqlTypeKind::RegClass, false) => crate::include::catalog::REGCLASS_TYPE_OID,
        (SqlTypeKind::RegClass, true) => crate::include::catalog::REGCLASS_ARRAY_TYPE_OID,
        (SqlTypeKind::RegType, false) => crate::include::catalog::REGTYPE_TYPE_OID,
        (SqlTypeKind::RegType, true) => unreachable!("regtype arrays are unsupported"),
        (SqlTypeKind::RegRole, false) => crate::include::catalog::REGROLE_TYPE_OID,
        (SqlTypeKind::RegRole, true) => unreachable!("regrole arrays are unsupported"),
        (SqlTypeKind::RegNamespace, false) => crate::include::catalog::REGNAMESPACE_TYPE_OID,
        (SqlTypeKind::RegNamespace, true) => crate::include::catalog::REGNAMESPACE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegOperator, false) => crate::include::catalog::REGOPERATOR_TYPE_OID,
        (SqlTypeKind::RegOperator, true) => crate::include::catalog::REGOPERATOR_ARRAY_TYPE_OID,
        (SqlTypeKind::RegProcedure, false) => crate::include::catalog::REGPROCEDURE_TYPE_OID,
        (SqlTypeKind::RegProcedure, true) => crate::include::catalog::REGPROCEDURE_ARRAY_TYPE_OID,
        (SqlTypeKind::Tid, false) => TID_TYPE_OID,
        (SqlTypeKind::Tid, true) => TID_ARRAY_TYPE_OID,
        (SqlTypeKind::Xid, false) => XID_TYPE_OID,
        (SqlTypeKind::Xid, true) => XID_ARRAY_TYPE_OID,
        (SqlTypeKind::OidVector, false) => OIDVECTOR_TYPE_OID,
        (SqlTypeKind::OidVector, true) => unreachable!("oidvector arrays are unsupported"),
        (SqlTypeKind::Float4, false) => FLOAT4_TYPE_OID,
        (SqlTypeKind::Float4, true) => FLOAT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Float8, false) => FLOAT8_TYPE_OID,
        (SqlTypeKind::Float8, true) => FLOAT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Money, false) => MONEY_TYPE_OID,
        (SqlTypeKind::Money, true) => MONEY_ARRAY_TYPE_OID,
        (SqlTypeKind::Inet, false) => crate::include::catalog::INET_TYPE_OID,
        (SqlTypeKind::Inet, true) => crate::include::catalog::INET_ARRAY_TYPE_OID,
        (SqlTypeKind::Cidr, false) => crate::include::catalog::CIDR_TYPE_OID,
        (SqlTypeKind::Cidr, true) => crate::include::catalog::CIDR_ARRAY_TYPE_OID,
        (SqlTypeKind::Varchar, false) => VARCHAR_TYPE_OID,
        (SqlTypeKind::Varchar, true) => VARCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Char, false) => BPCHAR_TYPE_OID,
        (SqlTypeKind::Char, true) => BPCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Date, false) => DATE_TYPE_OID,
        (SqlTypeKind::Date, true) => DATE_ARRAY_TYPE_OID,
        (SqlTypeKind::Time, false) => TIME_TYPE_OID,
        (SqlTypeKind::Time, true) => TIME_ARRAY_TYPE_OID,
        (SqlTypeKind::TimeTz, false) => TIMETZ_TYPE_OID,
        (SqlTypeKind::TimeTz, true) => TIMETZ_ARRAY_TYPE_OID,
        (SqlTypeKind::Interval, false) => INTERVAL_TYPE_OID,
        (SqlTypeKind::Interval, true) => INTERVAL_ARRAY_TYPE_OID,
        (SqlTypeKind::Timestamp, false) => TIMESTAMP_TYPE_OID,
        (SqlTypeKind::Timestamp, true) => TIMESTAMP_ARRAY_TYPE_OID,
        (SqlTypeKind::TimestampTz, false) => TIMESTAMPTZ_TYPE_OID,
        (SqlTypeKind::TimestampTz, true) => TIMESTAMPTZ_ARRAY_TYPE_OID,
        (SqlTypeKind::Numeric, false) => NUMERIC_TYPE_OID,
        (SqlTypeKind::Numeric, true) => NUMERIC_ARRAY_TYPE_OID,
        (SqlTypeKind::Json, false) => JSON_TYPE_OID,
        (SqlTypeKind::Json, true) => JSON_ARRAY_TYPE_OID,
        (SqlTypeKind::Jsonb, false) => JSONB_TYPE_OID,
        (SqlTypeKind::Jsonb, true) => JSONB_ARRAY_TYPE_OID,
        (SqlTypeKind::JsonPath, false) => JSONPATH_TYPE_OID,
        (SqlTypeKind::JsonPath, true) => JSONPATH_ARRAY_TYPE_OID,
        (SqlTypeKind::Xml, false) => XML_TYPE_OID,
        (SqlTypeKind::Xml, true) => XML_ARRAY_TYPE_OID,
        (SqlTypeKind::Point, false) => POINT_TYPE_OID,
        (SqlTypeKind::Point, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Lseg, false) => LSEG_TYPE_OID,
        (SqlTypeKind::Lseg, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Path, false) => PATH_TYPE_OID,
        (SqlTypeKind::Path, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Box, false) => BOX_TYPE_OID,
        (SqlTypeKind::Box, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Polygon, false) => POLYGON_TYPE_OID,
        (SqlTypeKind::Polygon, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Line, false) => LINE_TYPE_OID,
        (SqlTypeKind::Line, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Circle, false) => CIRCLE_TYPE_OID,
        (SqlTypeKind::Circle, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::TsVector, false) => TSVECTOR_TYPE_OID,
        (SqlTypeKind::TsVector, true) => TSVECTOR_ARRAY_TYPE_OID,
        (SqlTypeKind::TsQuery, false) => TSQUERY_TYPE_OID,
        (SqlTypeKind::TsQuery, true) => TSQUERY_ARRAY_TYPE_OID,
        (SqlTypeKind::RegConfig, false) => REGCONFIG_TYPE_OID,
        (SqlTypeKind::RegConfig, true) => REGCONFIG_ARRAY_TYPE_OID,
        (SqlTypeKind::RegDictionary, false) => REGDICTIONARY_TYPE_OID,
        (SqlTypeKind::RegDictionary, true) => REGDICTIONARY_ARRAY_TYPE_OID,
        (SqlTypeKind::PgNodeTree, false) => PG_NODE_TREE_TYPE_OID,
        (SqlTypeKind::PgNodeTree, true) => unreachable!("pg_node_tree arrays are unsupported"),
        (SqlTypeKind::Int4Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::Int8Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::NumericRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::DateRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampTzRange, _) => unreachable!("range handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{INTERNAL_CHAR_TYPE_OID, PG_CLASS_RELATION_OID};

    #[test]
    fn bootstrap_pg_attribute_rows_cover_core_catalog_columns() {
        let rows = bootstrap_pg_attribute_rows();
        let expected = [
            pg_namespace_desc().columns.len(),
            pg_type_desc().columns.len(),
            pg_proc_desc().columns.len(),
            pg_language_desc().columns.len(),
            pg_operator_desc().columns.len(),
            pg_attribute_desc().columns.len(),
            pg_class_desc().columns.len(),
            pg_authid_desc().columns.len(),
            pg_auth_members_desc().columns.len(),
            pg_collation_desc().columns.len(),
            pg_largeobject_metadata_desc().columns.len(),
            pg_database_desc().columns.len(),
            pg_tablespace_desc().columns.len(),
            pg_am_desc().columns.len(),
            pg_amop_desc().columns.len(),
            pg_amproc_desc().columns.len(),
            pg_attrdef_desc().columns.len(),
            pg_cast_desc().columns.len(),
            pg_constraint_desc().columns.len(),
            pg_depend_desc().columns.len(),
            pg_index_desc().columns.len(),
            pg_inherits_desc().columns.len(),
            pg_rewrite_desc().columns.len(),
            pg_statistic_desc().columns.len(),
            pg_statistic_ext_desc().columns.len(),
            pg_statistic_ext_data_desc().columns.len(),
            pg_opclass_desc().columns.len(),
            pg_opfamily_desc().columns.len(),
            pg_publication_desc().columns.len(),
            pg_publication_rel_desc().columns.len(),
            pg_publication_namespace_desc().columns.len(),
        ]
        .into_iter()
        .sum::<usize>();
        assert_eq!(rows.len(), expected);
        assert!(rows.iter().any(|row| {
            row.attrelid == PG_CLASS_RELATION_OID
                && row.attname == "relkind"
                && row.atttypid == INTERNAL_CHAR_TYPE_OID
        }));
    }
}
