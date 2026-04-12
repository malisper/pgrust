use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

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
    Vec::new()
}
