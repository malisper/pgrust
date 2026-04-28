use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgSequenceRow {
    pub seqrelid: u32,
    pub seqtypid: u32,
    pub seqstart: i64,
    pub seqincrement: i64,
    pub seqmax: i64,
    pub seqmin: i64,
    pub seqcache: i64,
    pub seqcycle: bool,
}

pub fn pg_sequence_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("seqrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("seqtypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("seqstart", SqlType::new(SqlTypeKind::Int8), false),
            column_desc("seqincrement", SqlType::new(SqlTypeKind::Int8), false),
            column_desc("seqmax", SqlType::new(SqlTypeKind::Int8), false),
            column_desc("seqmin", SqlType::new(SqlTypeKind::Int8), false),
            column_desc("seqcache", SqlType::new(SqlTypeKind::Int8), false),
            column_desc("seqcycle", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn sort_pg_sequence_rows(rows: &mut [PgSequenceRow]) {
    rows.sort_by_key(|row| row.seqrelid);
}
