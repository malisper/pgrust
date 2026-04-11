use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::parser::SqlTypeKind;
use super::{pg_class_desc, pg_namespace_desc, pg_type_desc};
use crate::include::catalog::{
    BOOL_TYPE_OID, BYTEA_TYPE_OID, BPCHAR_TYPE_OID, FLOAT4_TYPE_OID, FLOAT8_TYPE_OID,
    INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_TYPE_OID, JSON_TYPE_OID, NUMERIC_TYPE_OID, OID_TYPE_OID, PG_ATTRIBUTE_RELATION_OID,
    PG_CLASS_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID, TEXT_TYPE_OID,
    TIMESTAMP_TYPE_OID, VARCHAR_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAttributeRow {
    pub attrelid: u32,
    pub attname: String,
    pub atttypid: u32,
    pub attnum: i16,
    pub attnotnull: bool,
    pub atttypmod: i32,
    pub sql_type: SqlType,
}

pub fn pg_attribute_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("attrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("attname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("atttypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("attnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attnotnull", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("atttypmod", SqlType::new(SqlTypeKind::Int4), false),
        ],
    }
}

pub fn bootstrap_pg_attribute_rows() -> Vec<PgAttributeRow> {
    let mut rows = Vec::new();
    rows.extend(attribute_rows_for_desc(PG_NAMESPACE_RELATION_OID, &pg_namespace_desc()));
    rows.extend(attribute_rows_for_desc(PG_TYPE_RELATION_OID, &pg_type_desc()));
    rows.extend(attribute_rows_for_desc(PG_ATTRIBUTE_RELATION_OID, &pg_attribute_desc()));
    rows.extend(attribute_rows_for_desc(PG_CLASS_RELATION_OID, &pg_class_desc()));
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
            attnum: idx.saturating_add(1) as i16,
            attnotnull: !column.storage.nullable,
            atttypmod: column.sql_type.typmod,
            sql_type: column.sql_type,
        })
        .collect()
}

fn sql_type_oid(sql_type: SqlType) -> u32 {
    if sql_type.is_array {
        return 0;
    }
    match sql_type.kind {
        SqlTypeKind::Bool => BOOL_TYPE_OID,
        SqlTypeKind::Bytea => BYTEA_TYPE_OID,
        SqlTypeKind::InternalChar => INTERNAL_CHAR_TYPE_OID,
        SqlTypeKind::Int8 => INT8_TYPE_OID,
        SqlTypeKind::Int2 => INT2_TYPE_OID,
        SqlTypeKind::Int4 => INT4_TYPE_OID,
        SqlTypeKind::Text => TEXT_TYPE_OID,
        SqlTypeKind::Oid => OID_TYPE_OID,
        SqlTypeKind::Float4 => FLOAT4_TYPE_OID,
        SqlTypeKind::Float8 => FLOAT8_TYPE_OID,
        SqlTypeKind::Varchar => VARCHAR_TYPE_OID,
        SqlTypeKind::Char => BPCHAR_TYPE_OID,
        SqlTypeKind::Timestamp => TIMESTAMP_TYPE_OID,
        SqlTypeKind::Numeric => NUMERIC_TYPE_OID,
        SqlTypeKind::Json => JSON_TYPE_OID,
        SqlTypeKind::Jsonb => JSONB_TYPE_OID,
        SqlTypeKind::JsonPath => JSONPATH_TYPE_OID,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{INTERNAL_CHAR_TYPE_OID, PG_CLASS_RELATION_OID};

    #[test]
    fn bootstrap_pg_attribute_rows_cover_core_catalog_columns() {
        let rows = bootstrap_pg_attribute_rows();
        assert_eq!(rows.len(), 18);
        assert!(rows.iter().any(|row| {
            row.attrelid == PG_CLASS_RELATION_OID
                && row.attname == "relkind"
                && row.atttypid == INTERNAL_CHAR_TYPE_OID
        }));
    }
}
