pub use crate::backend::catalog::state::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, CatalogIndexMeta,
};
use crate::backend::executor::{ColumnDesc, RelationDesc, ScalarType};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::AttributeAlign;

pub fn column_desc(name: impl Into<String>, sql_type: SqlType, nullable: bool) -> ColumnDesc {
    let name = name.into();
    let ty = scalar_type_for_sql_type(sql_type);
    let (attlen, attalign) = match ty {
        ScalarType::Int16 => (2, AttributeAlign::Short),
        ScalarType::Int32 => (4, AttributeAlign::Int),
        ScalarType::Int64 => (8, AttributeAlign::Double),
        ScalarType::BitString => (-1, AttributeAlign::Int),
        ScalarType::Bytea => (-1, AttributeAlign::Int),
        ScalarType::Point => (16, AttributeAlign::Double),
        ScalarType::Lseg => (32, AttributeAlign::Double),
        ScalarType::Path => (-1, AttributeAlign::Int),
        ScalarType::Line => (24, AttributeAlign::Double),
        ScalarType::Box => (32, AttributeAlign::Double),
        ScalarType::Polygon => (-1, AttributeAlign::Int),
        ScalarType::Circle => (24, AttributeAlign::Double),
        ScalarType::Float32 => (4, AttributeAlign::Int),
        ScalarType::Float64 => (8, AttributeAlign::Double),
        ScalarType::Numeric => (-1, AttributeAlign::Int),
        ScalarType::Json | ScalarType::Jsonb | ScalarType::JsonPath => (-1, AttributeAlign::Int),
        ScalarType::Text => (-1, AttributeAlign::Int),
        ScalarType::Bool => (1, AttributeAlign::Char),
        ScalarType::Array(_) => (-1, AttributeAlign::Int),
    };
    ColumnDesc {
        name: name.clone(),
        storage: crate::include::access::htup::AttributeDesc {
            name,
            attlen,
            attalign,
            nullable,
        },
        ty,
        sql_type,
        not_null_constraint_oid: None,
        attrdef_oid: None,
        default_expr: None,
        missing_default_value: None,
    }
}

pub fn allocate_relation_object_oids(desc: &mut RelationDesc, next_oid: &mut u32) {
    for column in &mut desc.columns {
        if !column.storage.nullable && column.not_null_constraint_oid.is_none() {
            column.not_null_constraint_oid = Some(*next_oid);
            *next_oid = next_oid.saturating_add(1);
        }
        if column.default_expr.is_some() && column.attrdef_oid.is_none() {
            column.attrdef_oid = Some(*next_oid);
            *next_oid = next_oid.saturating_add(1);
        }
    }
}

fn scalar_type_for_sql_type(sql_type: SqlType) -> ScalarType {
    if sql_type.is_array {
        return ScalarType::Array(Box::new(scalar_type_for_sql_type(sql_type.element_type())));
    }
    match sql_type.kind {
        SqlTypeKind::Int2 => ScalarType::Int16,
        SqlTypeKind::Int2Vector => ScalarType::Text,
        SqlTypeKind::Int4 => ScalarType::Int32,
        SqlTypeKind::Int8 => ScalarType::Int64,
        SqlTypeKind::Name => ScalarType::Text,
        SqlTypeKind::Oid => ScalarType::Int32,
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
        SqlTypeKind::Json => ScalarType::Json,
        SqlTypeKind::Jsonb => ScalarType::Jsonb,
        SqlTypeKind::JsonPath => ScalarType::JsonPath,
        SqlTypeKind::Text
        | SqlTypeKind::Timestamp
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => ScalarType::Text,
        SqlTypeKind::Bool => ScalarType::Bool,
    }
}
