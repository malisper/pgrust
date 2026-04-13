use super::{
    pg_am_desc, pg_amop_desc, pg_amproc_desc, pg_attrdef_desc, pg_auth_members_desc,
    pg_authid_desc, pg_cast_desc, pg_class_desc, pg_collation_desc, pg_constraint_desc,
    pg_database_desc, pg_depend_desc, pg_index_desc, pg_language_desc, pg_namespace_desc,
    pg_opclass_desc, pg_operator_desc, pg_opfamily_desc, pg_proc_desc, pg_tablespace_desc,
    pg_type_desc,
};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::parser::SqlTypeKind;
use crate::include::catalog::{
    BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID, BPCHAR_ARRAY_TYPE_OID,
    BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT2VECTOR_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID, JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID,
    NAME_ARRAY_TYPE_OID, NAME_TYPE_OID, NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID,
    OID_ARRAY_TYPE_OID, OID_TYPE_OID, OIDVECTOR_TYPE_OID, PG_AM_RELATION_OID,
    PG_AMOP_RELATION_OID, PG_AMPROC_RELATION_OID, PG_ATTRDEF_RELATION_OID,
    PG_ATTRIBUTE_RELATION_OID, PG_AUTH_MEMBERS_RELATION_OID, PG_AUTHID_RELATION_OID,
    PG_CAST_RELATION_OID, PG_CLASS_RELATION_OID, PG_COLLATION_RELATION_OID,
    PG_CONSTRAINT_RELATION_OID, PG_DATABASE_RELATION_OID, PG_DEPEND_RELATION_OID,
    PG_INDEX_RELATION_OID, PG_LANGUAGE_RELATION_OID, PG_NAMESPACE_RELATION_OID,
    PG_NODE_TREE_TYPE_OID, PG_OPCLASS_RELATION_OID, PG_OPERATOR_RELATION_OID,
    PG_OPFAMILY_RELATION_OID, PG_PROC_RELATION_OID, PG_TABLESPACE_RELATION_OID,
    PG_TYPE_RELATION_OID, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID,
    TIMESTAMP_TYPE_OID, VARBIT_ARRAY_TYPE_OID, VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID,
    VARCHAR_TYPE_OID,
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
            column_desc("attname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("atttypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("attnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("attnotnull", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("atttypmod", SqlType::new(SqlTypeKind::Int4), false),
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
            attnum: idx.saturating_add(1) as i16,
            attnotnull: !column.storage.nullable,
            atttypmod: column.sql_type.typmod,
            sql_type: column.sql_type,
        })
        .collect()
}

fn sql_type_oid(sql_type: SqlType) -> u32 {
    match (sql_type.kind, sql_type.is_array) {
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
        (SqlTypeKind::OidVector, false) => OIDVECTOR_TYPE_OID,
        (SqlTypeKind::OidVector, true) => unreachable!("oidvector arrays are unsupported"),
        (SqlTypeKind::Float4, false) => FLOAT4_TYPE_OID,
        (SqlTypeKind::Float4, true) => FLOAT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Float8, false) => FLOAT8_TYPE_OID,
        (SqlTypeKind::Float8, true) => FLOAT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Varchar, false) => VARCHAR_TYPE_OID,
        (SqlTypeKind::Varchar, true) => VARCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Char, false) => BPCHAR_TYPE_OID,
        (SqlTypeKind::Char, true) => BPCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Timestamp, false) => TIMESTAMP_TYPE_OID,
        (SqlTypeKind::Timestamp, true) => TIMESTAMP_ARRAY_TYPE_OID,
        (SqlTypeKind::Numeric, false) => NUMERIC_TYPE_OID,
        (SqlTypeKind::Numeric, true) => NUMERIC_ARRAY_TYPE_OID,
        (SqlTypeKind::Json, false) => JSON_TYPE_OID,
        (SqlTypeKind::Json, true) => JSON_ARRAY_TYPE_OID,
        (SqlTypeKind::Jsonb, false) => JSONB_TYPE_OID,
        (SqlTypeKind::Jsonb, true) => JSONB_ARRAY_TYPE_OID,
        (SqlTypeKind::JsonPath, false) => JSONPATH_TYPE_OID,
        (SqlTypeKind::JsonPath, true) => JSONPATH_ARRAY_TYPE_OID,
        (SqlTypeKind::PgNodeTree, false) => PG_NODE_TREE_TYPE_OID,
        (SqlTypeKind::PgNodeTree, true) => unreachable!("pg_node_tree arrays are unsupported"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{INTERNAL_CHAR_TYPE_OID, PG_CLASS_RELATION_OID};

    #[test]
    fn bootstrap_pg_attribute_rows_cover_core_catalog_columns() {
        let rows = bootstrap_pg_attribute_rows();
        assert_eq!(rows.len(), 192);
        assert!(rows.iter().any(|row| {
            row.attrelid == PG_CLASS_RELATION_OID
                && row.attname == "relkind"
                && row.atttypid == INTERNAL_CHAR_TYPE_OID
        }));
    }
}
