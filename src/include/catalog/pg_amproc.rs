use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BIT_CMP_EQ_PROC_OID, BIT_TYPE_OID, BOOL_CMP_EQ_PROC_OID, BOOL_TYPE_OID, BTREE_BIT_FAMILY_OID,
    BTREE_BOOL_FAMILY_OID, BTREE_BYTEA_FAMILY_OID, BTREE_INTEGER_FAMILY_OID, BTREE_TEXT_FAMILY_OID,
    BTREE_VARBIT_FAMILY_OID, BYTEA_CMP_EQ_PROC_OID, BYTEA_TYPE_OID, INT4_CMP_EQ_PROC_OID,
    INT4_TYPE_OID, TEXT_CMP_EQ_PROC_OID, TEXT_TYPE_OID, VARBIT_CMP_EQ_PROC_OID, VARBIT_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAmprocRow {
    pub oid: u32,
    pub amprocfamily: u32,
    pub amproclefttype: u32,
    pub amprocrighttype: u32,
    pub amprocnum: i16,
    pub amproc: u32,
}

pub fn pg_amproc_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocfamily", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amproclefttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocrighttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("amproc", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_amproc_rows() -> Vec<PgAmprocRow> {
    let mut oid = 8100u32;
    let mut rows = Vec::new();
    for (family, type_oid, proc_oid) in [
        (BTREE_BOOL_FAMILY_OID, BOOL_TYPE_OID, BOOL_CMP_EQ_PROC_OID),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
            INT4_CMP_EQ_PROC_OID,
        ),
        (BTREE_TEXT_FAMILY_OID, TEXT_TYPE_OID, TEXT_CMP_EQ_PROC_OID),
        (BTREE_BIT_FAMILY_OID, BIT_TYPE_OID, BIT_CMP_EQ_PROC_OID),
        (
            BTREE_VARBIT_FAMILY_OID,
            VARBIT_TYPE_OID,
            VARBIT_CMP_EQ_PROC_OID,
        ),
        (
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
            BYTEA_CMP_EQ_PROC_OID,
        ),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: family,
            amproclefttype: type_oid,
            amprocrighttype: type_oid,
            amprocnum: 1,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    rows
}
