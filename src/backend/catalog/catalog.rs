pub use crate::backend::catalog::state::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, CatalogIndexMeta,
};
use crate::backend::executor::{ColumnDesc, RelationDesc, ScalarType};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::range_type_ref_for_sql_type;

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
        ScalarType::BitString => (-1, AttributeAlign::Int),
        ScalarType::Bytea => (-1, AttributeAlign::Int),
        ScalarType::Point => (16, AttributeAlign::Double),
        ScalarType::Lseg => (32, AttributeAlign::Double),
        ScalarType::Path => (-1, AttributeAlign::Int),
        ScalarType::Line => (24, AttributeAlign::Double),
        ScalarType::Box => (32, AttributeAlign::Double),
        ScalarType::Polygon => (-1, AttributeAlign::Int),
        ScalarType::Circle => (24, AttributeAlign::Double),
        ScalarType::Range(_) => (-1, AttributeAlign::Int),
        ScalarType::Float32 => (4, AttributeAlign::Int),
        ScalarType::Float64 => (8, AttributeAlign::Double),
        ScalarType::Numeric => (-1, AttributeAlign::Int),
        ScalarType::Json | ScalarType::Jsonb | ScalarType::JsonPath => (-1, AttributeAlign::Int),
        ScalarType::TsVector | ScalarType::TsQuery => (-1, AttributeAlign::Int),
        ScalarType::Text => (-1, AttributeAlign::Int),
        ScalarType::Record => (-1, AttributeAlign::Double),
        ScalarType::Bool => (1, AttributeAlign::Char),
        ScalarType::Array(_) => (-1, AttributeAlign::Int),
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
        not_null_constraint_oid: None,
        not_null_constraint_name: None,
        not_null_constraint_validated: !nullable,
        not_null_constraint_is_local: true,
        not_null_constraint_inhcount: 0,
        not_null_constraint_no_inherit: false,
        not_null_primary_key_owned: false,
        attrdef_oid: None,
        default_expr: None,
        default_sequence_oid: None,
        missing_default_value: None,
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

    match sql_type.kind {
        SqlTypeKind::AnyArray => AttributeStorage::Extended,
        SqlTypeKind::Name
        | SqlTypeKind::Void
        | SqlTypeKind::Trigger
        | SqlTypeKind::FdwHandler
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Date
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::InternalChar
        | SqlTypeKind::PgNodeTree => AttributeStorage::Plain,
        SqlTypeKind::Bit
        | SqlTypeKind::VarBit
        | SqlTypeKind::Bytea
        | SqlTypeKind::Record
        | SqlTypeKind::Composite
        | SqlTypeKind::Varchar
        | SqlTypeKind::Char
        | SqlTypeKind::Numeric
        | SqlTypeKind::Path
        | SqlTypeKind::Polygon
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::Tid
        | SqlTypeKind::Interval
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::Text => AttributeStorage::Extended,
        SqlTypeKind::Bool
        | SqlTypeKind::Int2
        | SqlTypeKind::Int4
        | SqlTypeKind::Int8
        | SqlTypeKind::Money
        | SqlTypeKind::Oid
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::Xid
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Line
        | SqlTypeKind::Box
        | SqlTypeKind::Circle
        | SqlTypeKind::Float4
        | SqlTypeKind::Float8 => AttributeStorage::Plain,
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
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
    match sql_type.kind {
        SqlTypeKind::AnyArray => ScalarType::Array(Box::new(ScalarType::Text)),
        SqlTypeKind::Void => ScalarType::Text,
        SqlTypeKind::Trigger => ScalarType::Text,
        SqlTypeKind::FdwHandler => ScalarType::Text,
        SqlTypeKind::Int2 => ScalarType::Int16,
        SqlTypeKind::Int2Vector => ScalarType::Text,
        SqlTypeKind::Int4 => ScalarType::Int32,
        SqlTypeKind::Int8 => ScalarType::Int64,
        SqlTypeKind::Money => ScalarType::Money,
        // :HACK: tid and interval are currently routed through the text storage
        // path so ALTER TABLE ADD COLUMN and basic I/O work before we add
        // dedicated fixed-width runtime representations.
        SqlTypeKind::Name => ScalarType::Text,
        SqlTypeKind::Oid => ScalarType::Int32,
        SqlTypeKind::RegRole => ScalarType::Int32,
        SqlTypeKind::RegProcedure => ScalarType::Int32,
        SqlTypeKind::Tid => ScalarType::Text,
        SqlTypeKind::Xid => ScalarType::Int32,
        SqlTypeKind::OidVector => ScalarType::Text,
        SqlTypeKind::Bit | SqlTypeKind::VarBit => ScalarType::BitString,
        SqlTypeKind::Bytea => ScalarType::Bytea,
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
        SqlTypeKind::Json => ScalarType::Json,
        SqlTypeKind::Jsonb => ScalarType::Jsonb,
        SqlTypeKind::JsonPath => ScalarType::JsonPath,
        SqlTypeKind::Date => ScalarType::Date,
        SqlTypeKind::Time => ScalarType::Time,
        SqlTypeKind::TimeTz => ScalarType::TimeTz,
        SqlTypeKind::Interval => ScalarType::Text,
        SqlTypeKind::TsVector => ScalarType::TsVector,
        SqlTypeKind::TsQuery => ScalarType::TsQuery,
        SqlTypeKind::RegConfig | SqlTypeKind::RegDictionary => ScalarType::Int32,
        SqlTypeKind::Record | SqlTypeKind::Composite => ScalarType::Record,
        SqlTypeKind::Text
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => ScalarType::Text,
        SqlTypeKind::Timestamp => ScalarType::Timestamp,
        SqlTypeKind::TimestampTz => ScalarType::TimestampTz,
        SqlTypeKind::Bool => ScalarType::Bool,
    }
}
