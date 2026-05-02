pub use crate::backend::catalog::state::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, CatalogIndexMeta,
};
use crate::backend::executor::{ColumnDesc, RelationDesc, ScalarType};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    ACLITEM_ARRAY_TYPE_OID, ACLITEM_TYPE_OID, C_COLLATION_OID, DEFAULT_COLLATION_OID,
    INT8_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, multirange_type_ref_for_sql_type,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::Value;

const FIRST_NORMAL_OBJECT_OID: u32 = 16_384;

pub fn column_desc(name: impl Into<String>, sql_type: SqlType, nullable: bool) -> ColumnDesc {
    let name = name.into();
    let ty = scalar_type_for_sql_type(sql_type);
    let (attlen, attalign) = match ty {
        ScalarType::Int16 => (2, AttributeAlign::Short),
        ScalarType::Int32 => (4, AttributeAlign::Int),
        ScalarType::Int64 => (8, AttributeAlign::Double),
        ScalarType::Money => (8, AttributeAlign::Double),
        ScalarType::Date => (4, AttributeAlign::Int),
        ScalarType::Time => (8, AttributeAlign::Double),
        ScalarType::TimeTz => (12, AttributeAlign::Double),
        ScalarType::Timestamp => (8, AttributeAlign::Double),
        ScalarType::TimestampTz => (8, AttributeAlign::Double),
        ScalarType::Interval => (16, AttributeAlign::Double),
        ScalarType::BitString => (-1, AttributeAlign::Int),
        ScalarType::Bytea => (-1, AttributeAlign::Int),
        ScalarType::Uuid => (16, AttributeAlign::Char),
        ScalarType::Inet | ScalarType::Cidr => (-1, AttributeAlign::Int),
        ScalarType::MacAddr => (6, AttributeAlign::Int),
        ScalarType::MacAddr8 => (8, AttributeAlign::Int),
        ScalarType::Point => (16, AttributeAlign::Double),
        ScalarType::Lseg => (32, AttributeAlign::Double),
        ScalarType::Path => (-1, AttributeAlign::Int),
        ScalarType::Line => (24, AttributeAlign::Double),
        ScalarType::Box => (32, AttributeAlign::Double),
        ScalarType::Polygon => (-1, AttributeAlign::Int),
        ScalarType::Circle => (24, AttributeAlign::Double),
        ScalarType::Range(_) => (-1, AttributeAlign::Int),
        ScalarType::Multirange(_) => (-1, AttributeAlign::Int),
        ScalarType::Float32 => (4, AttributeAlign::Int),
        ScalarType::Float64 => (8, AttributeAlign::Double),
        ScalarType::Numeric => (-1, AttributeAlign::Int),
        ScalarType::Json | ScalarType::Jsonb | ScalarType::JsonPath | ScalarType::Xml => {
            (-1, AttributeAlign::Int)
        }
        ScalarType::TsVector | ScalarType::TsQuery => (-1, AttributeAlign::Int),
        ScalarType::PgLsn => (8, AttributeAlign::Double),
        ScalarType::Text => (-1, AttributeAlign::Int),
        ScalarType::Enum => (4, AttributeAlign::Int),
        ScalarType::Record => (-1, AttributeAlign::Double),
        ScalarType::Bool => (1, AttributeAlign::Char),
        ScalarType::Array(_) => (-1, array_attribute_align(sql_type)),
    };
    ColumnDesc {
        name: name.clone(),
        storage: crate::include::access::htup::AttributeDesc {
            name,
            attlen,
            attalign,
            attstorage: default_attribute_storage(sql_type, attlen),
            attcompression: default_attribute_compression(sql_type, attlen),
            nullable,
        },
        ty,
        sql_type,
        dropped: false,
        attstattarget: -1,
        attinhcount: 0,
        attislocal: true,
        collation_oid: default_column_collation_oid(sql_type),
        not_null_constraint_oid: None,
        not_null_constraint_name: None,
        not_null_constraint_validated: !nullable,
        not_null_constraint_is_local: true,
        not_null_constraint_inhcount: 0,
        not_null_constraint_no_inherit: false,
        not_null_primary_key_owned: false,
        attacl: None,
        attrdef_oid: None,
        default_expr: None,
        default_sequence_oid: None,
        generated: None,
        identity: None,
        missing_default_value: None,
        fdw_options: None,
    }
}

pub fn catalog_attmissingval_for_column(column: &ColumnDesc) -> Option<Vec<Value>> {
    let value = column.missing_default_value.clone()?;
    Some(vec![catalog_attmissingval_value(value)])
}

fn catalog_attmissingval_value(value: Value) -> Value {
    match value {
        // :HACK: pg_attribute.attmissingval is an anyarray containing one
        // element of the column type. pgrust's anyarray storage cannot yet
        // encode array-typed elements, so persist array defaults as their SQL
        // array literal and parse them back when rebuilding the relation desc.
        Value::Array(values) => {
            Value::Text(crate::backend::executor::value_io::format_array_text(&values).into())
        }
        Value::PgArray(array) => {
            Value::Text(crate::backend::executor::value_io::format_array_value_text(&array).into())
        }
        other => other,
    }
}

pub fn missing_default_value_from_attmissingval(value: Value, sql_type: SqlType) -> Value {
    if sql_type.is_array && matches!(value, Value::Text(_) | Value::TextRef(_, _)) {
        crate::backend::executor::cast_value(value.clone(), sql_type).unwrap_or(value)
    } else {
        value
    }
}

fn array_attribute_align(sql_type: SqlType) -> AttributeAlign {
    if sql_type.type_oid == ACLITEM_ARRAY_TYPE_OID {
        return AttributeAlign::Double;
    }

    let element = sql_type.element_type();
    if array_element_uses_double_align(element) {
        AttributeAlign::Double
    } else {
        AttributeAlign::Int
    }
}

fn array_element_uses_double_align(element: SqlType) -> bool {
    if element.type_oid == ACLITEM_TYPE_OID {
        return true;
    }

    match element.kind {
        SqlTypeKind::Int8
        | SqlTypeKind::Float8
        | SqlTypeKind::Money
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Interval
        | SqlTypeKind::PgLsn
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Line
        | SqlTypeKind::Box
        | SqlTypeKind::Circle
        | SqlTypeKind::Record
        | SqlTypeKind::Composite => true,
        SqlTypeKind::Range | SqlTypeKind::Multirange => matches!(
            element.range_subtype_oid,
            INT8_TYPE_OID | TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
        ),
        _ => false,
    }
}

pub(crate) fn default_column_collation_oid(sql_type: SqlType) -> u32 {
    if sql_type.is_array {
        return 0;
    }
    match sql_type.kind {
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
            DEFAULT_COLLATION_OID
        }
        _ => 0,
    }
}

pub(crate) fn catalog_attribute_collation_oid(relation_oid: u32, collation_oid: u32) -> u32 {
    if relation_oid < FIRST_NORMAL_OBJECT_OID && collation_oid == DEFAULT_COLLATION_OID {
        C_COLLATION_OID
    } else {
        collation_oid
    }
}

fn default_attribute_storage(sql_type: SqlType, attlen: i16) -> AttributeStorage {
    if attlen > 0 {
        return AttributeStorage::Plain;
    }

    if sql_type.is_array {
        return AttributeStorage::Extended;
    }

    if sql_type.is_range() {
        return AttributeStorage::Extended;
    }
    if sql_type.is_multirange() {
        return AttributeStorage::Extended;
    }

    match sql_type.kind {
        SqlTypeKind::AnyArray
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyCompatibleArray
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyCompatibleMultirange => AttributeStorage::Extended,
        SqlTypeKind::Name
        | SqlTypeKind::Void
        | SqlTypeKind::Trigger
        | SqlTypeKind::EventTrigger
        | SqlTypeKind::FdwHandler
        | SqlTypeKind::Shell
        | SqlTypeKind::Cstring
        | SqlTypeKind::AnyElement
        | SqlTypeKind::AnyEnum
        | SqlTypeKind::AnyRange
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Internal
        | SqlTypeKind::Date
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::InternalChar
        | SqlTypeKind::PgLsn => AttributeStorage::Plain,
        SqlTypeKind::Bit
        | SqlTypeKind::VarBit
        | SqlTypeKind::Bytea
        | SqlTypeKind::Uuid
        | SqlTypeKind::Inet
        | SqlTypeKind::Cidr
        | SqlTypeKind::Record
        | SqlTypeKind::Composite
        | SqlTypeKind::Varchar
        | SqlTypeKind::Char
        | SqlTypeKind::Path
        | SqlTypeKind::Polygon
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::Xml
        | SqlTypeKind::Tid
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::Text => AttributeStorage::Extended,
        SqlTypeKind::Bool
        | SqlTypeKind::Int2
        | SqlTypeKind::Int4
        | SqlTypeKind::Int8
        | SqlTypeKind::Money
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
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::MacAddr
        | SqlTypeKind::MacAddr8
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Line
        | SqlTypeKind::Box
        | SqlTypeKind::Circle
        | SqlTypeKind::Float4
        | SqlTypeKind::Float8
        | SqlTypeKind::Interval
        | SqlTypeKind::Enum => AttributeStorage::Plain,
        SqlTypeKind::Numeric => AttributeStorage::Main,
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    }
}

fn default_attribute_compression(_sql_type: SqlType, _attlen: i16) -> AttributeCompression {
    AttributeCompression::Default
}

pub fn allocate_relation_object_oids(desc: &mut RelationDesc, next_oid: &mut u32) {
    for column in &mut desc.columns {
        if !column.storage.nullable && column.not_null_constraint_oid.is_none() {
            column.not_null_constraint_oid = Some(*next_oid);
            *next_oid = next_oid.saturating_add(1);
        }
        if !column.storage.nullable {
            column.not_null_constraint_validated = true;
        }
        if column.default_expr.is_some() && column.attrdef_oid.is_none() {
            column.attrdef_oid = Some(*next_oid);
            *next_oid = next_oid.saturating_add(1);
        }
    }
}

pub(crate) fn scalar_type_for_sql_type(sql_type: SqlType) -> ScalarType {
    if sql_type.is_array {
        return ScalarType::Array(Box::new(scalar_type_for_sql_type(sql_type.element_type())));
    }
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        return ScalarType::Range(range_type);
    }
    if let Some(multirange_type) = multirange_type_ref_for_sql_type(sql_type) {
        return ScalarType::Multirange(multirange_type);
    }
    match sql_type.kind {
        SqlTypeKind::AnyArray | SqlTypeKind::AnyCompatibleArray => {
            ScalarType::Array(Box::new(ScalarType::Text))
        }
        SqlTypeKind::AnyElement
        | SqlTypeKind::AnyEnum
        | SqlTypeKind::AnyRange
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyCompatibleMultirange => ScalarType::Text,
        SqlTypeKind::Void => ScalarType::Text,
        SqlTypeKind::Trigger => ScalarType::Text,
        SqlTypeKind::EventTrigger => ScalarType::Text,
        SqlTypeKind::FdwHandler => ScalarType::Text,
        SqlTypeKind::Shell => ScalarType::Text,
        SqlTypeKind::Cstring => ScalarType::Text,
        SqlTypeKind::Int2 => ScalarType::Int16,
        SqlTypeKind::Int2Vector => ScalarType::Text,
        SqlTypeKind::Int4 => ScalarType::Int32,
        SqlTypeKind::Int8 => ScalarType::Int64,
        SqlTypeKind::Money => ScalarType::Money,
        // :HACK: tid is currently routed through the text storage path until
        // it gets a dedicated fixed-width runtime representation.
        SqlTypeKind::Name => ScalarType::Text,
        SqlTypeKind::Oid => ScalarType::Int32,
        SqlTypeKind::RegProc => ScalarType::Int32,
        SqlTypeKind::RegClass => ScalarType::Int32,
        SqlTypeKind::RegType => ScalarType::Int32,
        SqlTypeKind::RegRole => ScalarType::Int32,
        SqlTypeKind::RegNamespace => ScalarType::Int32,
        SqlTypeKind::RegOper => ScalarType::Int32,
        SqlTypeKind::RegOperator => ScalarType::Int32,
        SqlTypeKind::RegProcedure => ScalarType::Int32,
        SqlTypeKind::RegCollation => ScalarType::Int32,
        SqlTypeKind::Tid => ScalarType::Text,
        SqlTypeKind::Xid => ScalarType::Int32,
        SqlTypeKind::OidVector => ScalarType::Text,
        SqlTypeKind::Bit | SqlTypeKind::VarBit => ScalarType::BitString,
        SqlTypeKind::Bytea => ScalarType::Bytea,
        SqlTypeKind::Uuid => ScalarType::Uuid,
        SqlTypeKind::Inet => ScalarType::Inet,
        SqlTypeKind::Cidr => ScalarType::Cidr,
        SqlTypeKind::MacAddr => ScalarType::MacAddr,
        SqlTypeKind::MacAddr8 => ScalarType::MacAddr8,
        SqlTypeKind::Point => ScalarType::Point,
        SqlTypeKind::Lseg => ScalarType::Lseg,
        SqlTypeKind::Path => ScalarType::Path,
        SqlTypeKind::Line => ScalarType::Line,
        SqlTypeKind::Box => ScalarType::Box,
        SqlTypeKind::Polygon => ScalarType::Polygon,
        SqlTypeKind::Circle => ScalarType::Circle,
        SqlTypeKind::Float4 => ScalarType::Float32,
        SqlTypeKind::Float8 => ScalarType::Float64,
        SqlTypeKind::Numeric => ScalarType::Numeric,
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
        SqlTypeKind::Json => ScalarType::Json,
        SqlTypeKind::Jsonb => ScalarType::Jsonb,
        SqlTypeKind::JsonPath => ScalarType::JsonPath,
        SqlTypeKind::Xml => ScalarType::Xml,
        SqlTypeKind::Date => ScalarType::Date,
        SqlTypeKind::Time => ScalarType::Time,
        SqlTypeKind::TimeTz => ScalarType::TimeTz,
        SqlTypeKind::Interval => ScalarType::Interval,
        SqlTypeKind::TsVector => ScalarType::TsVector,
        SqlTypeKind::TsQuery => ScalarType::TsQuery,
        SqlTypeKind::PgLsn => ScalarType::PgLsn,
        SqlTypeKind::RegConfig | SqlTypeKind::RegDictionary => ScalarType::Int32,
        SqlTypeKind::Record | SqlTypeKind::Composite => ScalarType::Record,
        SqlTypeKind::Enum => ScalarType::Enum,
        SqlTypeKind::Text
        | SqlTypeKind::Internal
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => ScalarType::Text,
        SqlTypeKind::Timestamp => ScalarType::Timestamp,
        SqlTypeKind::TimestampTz => ScalarType::TimestampTz,
        SqlTypeKind::Bool => ScalarType::Bool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_column_alignment_follows_postgres_element_alignment() {
        let aclitem_array =
            SqlType::array_of(SqlType::new(SqlTypeKind::Text).with_identity(ACLITEM_TYPE_OID, 0))
                .with_identity(ACLITEM_ARRAY_TYPE_OID, 0);
        let relacl = column_desc("relacl", aclitem_array, true);
        assert_eq!(relacl.storage.attalign, AttributeAlign::Double);

        let int4_array = column_desc(
            "items",
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            true,
        );
        assert_eq!(int4_array.storage.attalign, AttributeAlign::Int);

        let float8_array = column_desc(
            "items",
            SqlType::array_of(SqlType::new(SqlTypeKind::Float8)),
            true,
        );
        assert_eq!(float8_array.storage.attalign, AttributeAlign::Double);
    }
}
