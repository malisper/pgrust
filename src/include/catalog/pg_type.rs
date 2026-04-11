use crate::backend::parser::SqlType;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlTypeKind;
use crate::include::catalog::{
    BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID,
    BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID,
    BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID,
    PG_ATTRIBUTE_RELATION_OID, PG_ATTRIBUTE_ROWTYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PG_CLASS_RELATION_OID, PG_CLASS_ROWTYPE_OID, PG_NAMESPACE_RELATION_OID,
    PG_NAMESPACE_ROWTYPE_OID, PG_TYPE_RELATION_OID, PG_TYPE_ROWTYPE_OID, TEXT_ARRAY_TYPE_OID,
    TEXT_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID, VARBIT_ARRAY_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTypeRow {
    pub oid: u32,
    pub typname: String,
    pub typnamespace: u32,
    pub typrelid: u32,
    pub sql_type: SqlType,
}

pub fn pg_type_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("typnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("typrelid", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn builtin_type_rows() -> Vec<PgTypeRow> {
    vec![
        builtin_type_row("bool", BOOL_TYPE_OID, SqlType::new(SqlTypeKind::Bool)),
        builtin_type_row("_bool", BOOL_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Bool))),
        builtin_type_row("bit", BIT_TYPE_OID, SqlType::new(SqlTypeKind::Bit)),
        builtin_type_row("_bit", BIT_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Bit))),
        builtin_type_row("varbit", VARBIT_TYPE_OID, SqlType::new(SqlTypeKind::VarBit)),
        builtin_type_row("_varbit", VARBIT_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::VarBit))),
        builtin_type_row("bytea", BYTEA_TYPE_OID, SqlType::new(SqlTypeKind::Bytea)),
        builtin_type_row("_bytea", BYTEA_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Bytea))),
        builtin_type_row("\"char\"", INTERNAL_CHAR_TYPE_OID, SqlType::new(SqlTypeKind::InternalChar)),
        builtin_type_row("_char", INTERNAL_CHAR_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar))),
        builtin_type_row("int8", INT8_TYPE_OID, SqlType::new(SqlTypeKind::Int8)),
        builtin_type_row("_int8", INT8_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Int8))),
        builtin_type_row("int2", INT2_TYPE_OID, SqlType::new(SqlTypeKind::Int2)),
        builtin_type_row("_int2", INT2_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Int2))),
        builtin_type_row("int4", INT4_TYPE_OID, SqlType::new(SqlTypeKind::Int4)),
        builtin_type_row("_int4", INT4_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Int4))),
        builtin_type_row("text", TEXT_TYPE_OID, SqlType::new(SqlTypeKind::Text)),
        builtin_type_row("_text", TEXT_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        builtin_type_row("oid", OID_TYPE_OID, SqlType::new(SqlTypeKind::Oid)),
        builtin_type_row("_oid", OID_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Oid))),
        builtin_type_row("float4", FLOAT4_TYPE_OID, SqlType::new(SqlTypeKind::Float4)),
        builtin_type_row("_float4", FLOAT4_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Float4))),
        builtin_type_row("float8", FLOAT8_TYPE_OID, SqlType::new(SqlTypeKind::Float8)),
        builtin_type_row("_float8", FLOAT8_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Float8))),
        builtin_type_row("varchar", VARCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Varchar)),
        builtin_type_row("_varchar", VARCHAR_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))),
        builtin_type_row("char", BPCHAR_TYPE_OID, SqlType::new(SqlTypeKind::Char)),
        builtin_type_row("_bpchar", BPCHAR_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Char))),
        builtin_type_row("timestamp", TIMESTAMP_TYPE_OID, SqlType::new(SqlTypeKind::Timestamp)),
        builtin_type_row("_timestamp", TIMESTAMP_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Timestamp))),
        builtin_type_row("numeric", NUMERIC_TYPE_OID, SqlType::new(SqlTypeKind::Numeric)),
        builtin_type_row("_numeric", NUMERIC_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Numeric))),
        builtin_type_row("json", JSON_TYPE_OID, SqlType::new(SqlTypeKind::Json)),
        builtin_type_row("_json", JSON_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Json))),
        builtin_type_row("jsonb", JSONB_TYPE_OID, SqlType::new(SqlTypeKind::Jsonb)),
        builtin_type_row("_jsonb", JSONB_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::Jsonb))),
        builtin_type_row("jsonpath", JSONPATH_TYPE_OID, SqlType::new(SqlTypeKind::JsonPath)),
        builtin_type_row("_jsonpath", JSONPATH_ARRAY_TYPE_OID, SqlType::array_of(SqlType::new(SqlTypeKind::JsonPath))),
    ]
}

pub fn bootstrap_composite_type_rows() -> [PgTypeRow; 4] {
    [
        composite_type_row("pg_namespace", PG_NAMESPACE_ROWTYPE_OID, PG_NAMESPACE_RELATION_OID),
        composite_type_row("pg_type", PG_TYPE_ROWTYPE_OID, PG_TYPE_RELATION_OID),
        composite_type_row("pg_attribute", PG_ATTRIBUTE_ROWTYPE_OID, PG_ATTRIBUTE_RELATION_OID),
        composite_type_row("pg_class", PG_CLASS_ROWTYPE_OID, PG_CLASS_RELATION_OID),
    ]
}

fn builtin_type_row(name: &str, oid: u32, sql_type: SqlType) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typrelid: 0,
        sql_type,
    }
}

fn composite_type_row(name: &str, oid: u32, relid: u32) -> PgTypeRow {
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typrelid: relid,
        sql_type: SqlType::new(SqlTypeKind::Text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_composite_types_match_core_catalogs() {
        let rows = bootstrap_composite_type_rows();
        let names: Vec<_> = rows.iter().map(|row| row.typname.as_str()).collect();
        assert_eq!(names, vec!["pg_namespace", "pg_type", "pg_attribute", "pg_class"]);
        assert_eq!(rows[0].oid, PG_NAMESPACE_ROWTYPE_OID);
        assert_eq!(rows[3].oid, PG_CLASS_ROWTYPE_OID);
    }
}
