use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BIT_TYPE_OID, BOOL_TYPE_OID, BTREE_AM_OID, BTREE_BIT_FAMILY_OID, BTREE_BOOL_FAMILY_OID,
    BTREE_BYTEA_FAMILY_OID, BTREE_INTEGER_FAMILY_OID, BTREE_TEXT_FAMILY_OID,
    BTREE_VARBIT_FAMILY_OID, BYTEA_TYPE_OID, INT4_TYPE_OID, TEXT_TYPE_OID, VARBIT_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAmopRow {
    pub oid: u32,
    pub amopfamily: u32,
    pub amoplefttype: u32,
    pub amoprighttype: u32,
    pub amopstrategy: i16,
    pub amoppurpose: char,
    pub amopopr: u32,
    pub amopmethod: u32,
    pub amopsortfamily: u32,
}

pub fn pg_amop_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopfamily", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amoplefttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amoprighttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopstrategy", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("amoppurpose", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("amopopr", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopmethod", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopsortfamily", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_amop_rows() -> Vec<PgAmopRow> {
    let mut oid = 8000u32;
    let mut rows = Vec::new();
    for (family, type_oid, operators) in [
        (BTREE_BOOL_FAMILY_OID, BOOL_TYPE_OID, [58, 1694, 91, 1695, 59]),
        (BTREE_INTEGER_FAMILY_OID, INT4_TYPE_OID, [97, 523, 96, 525, 521]),
        (BTREE_TEXT_FAMILY_OID, TEXT_TYPE_OID, [664, 665, 98, 667, 666]),
        (BTREE_BIT_FAMILY_OID, BIT_TYPE_OID, [1786, 1788, 1784, 1789, 1787]),
        (BTREE_VARBIT_FAMILY_OID, VARBIT_TYPE_OID, [1806, 1808, 1804, 1809, 1807]),
        (BTREE_BYTEA_FAMILY_OID, BYTEA_TYPE_OID, [1957, 1958, 1955, 1960, 1959]),
    ] {
        for (strategy, operator_oid) in (1_i16..=5).zip(operators) {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: type_oid,
                amoprighttype: type_oid,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid,
                amopmethod: BTREE_AM_OID,
                amopsortfamily: family,
            });
            oid = oid.saturating_add(1);
        }
    }
    rows
}
