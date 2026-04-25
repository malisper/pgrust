use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq)]
pub struct PgEnumRow {
    pub oid: u32,
    pub enumtypid: u32,
    pub enumsortorder: f64,
    pub enumlabel: String,
}

pub fn pg_enum_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("enumtypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("enumsortorder", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("enumlabel", SqlType::new(SqlTypeKind::Name), false),
        ],
    }
}

pub fn bootstrap_pg_enum_rows() -> [PgEnumRow; 0] {
    []
}
