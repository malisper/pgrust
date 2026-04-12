use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

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
    Vec::new()
}
