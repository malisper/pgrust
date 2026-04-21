use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPublicationNamespaceRow {
    pub oid: u32,
    pub pnpubid: u32,
    pub pnnspid: u32,
}

pub fn pg_publication_namespace_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("pnpubid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("pnnspid", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_publication_namespace_rows() -> [PgPublicationNamespaceRow; 0] {
    []
}
