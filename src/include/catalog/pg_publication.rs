use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const PUBLISH_GENCOLS_NONE: char = 'n';
pub const PUBLISH_GENCOLS_STORED: char = 's';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPublicationRow {
    pub oid: u32,
    pub pubname: String,
    pub pubowner: u32,
    pub puballtables: bool,
    pub puballsequences: bool,
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
    pub pubviaroot: bool,
    pub pubgencols: char,
}

pub fn pg_publication_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("pubname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("pubowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("puballtables", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("puballsequences", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubinsert", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubupdate", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubdelete", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubtruncate", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubviaroot", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("pubgencols", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

pub fn bootstrap_pg_publication_rows() -> [PgPublicationRow; 0] {
    []
}
