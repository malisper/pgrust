use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::RangeCanonicalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRangeRow {
    pub rngtypid: u32,
    pub rngsubtype: u32,
    pub rngmultitypid: u32,
    pub rngcollation: u32,
    pub rngsubopc: u32,
    pub rngcanonical: Option<String>,
    pub rngsubdiff: Option<String>,
    pub canonicalization: RangeCanonicalization,
}

pub fn pg_range_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("rngtypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rngsubtype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rngmultitypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rngcollation", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rngsubopc", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rngcanonical", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("rngsubdiff", SqlType::new(SqlTypeKind::RegProc), false),
        ],
    }
}
