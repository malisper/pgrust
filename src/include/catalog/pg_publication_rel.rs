use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPublicationRelRow {
    pub oid: u32,
    pub prpubid: u32,
    pub prrelid: u32,
    pub prexcept: bool,
    pub prqual: Option<String>,
    pub prattrs: Option<Vec<i16>>,
}

pub fn pg_publication_rel_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prpubid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prexcept", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("prqual", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc("prattrs", SqlType::new(SqlTypeKind::Int2Vector), true),
        ],
    }
}

pub fn bootstrap_pg_publication_rel_rows() -> [PgPublicationRelRow; 0] {
    []
}
