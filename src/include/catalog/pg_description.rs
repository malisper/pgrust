use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgDescriptionRow {
    pub objoid: u32,
    pub classoid: u32,
    pub objsubid: i32,
    pub description: String,
}

pub fn pg_description_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("objoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("classoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objsubid", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("description", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

