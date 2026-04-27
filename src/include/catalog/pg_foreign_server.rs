use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgForeignServerRow {
    pub oid: u32,
    pub srvname: String,
    pub srvowner: u32,
    pub srvfdw: u32,
    pub srvtype: Option<String>,
    pub srvversion: Option<String>,
    pub srvacl: Option<Vec<String>>,
    pub srvoptions: Option<Vec<String>>,
}

pub fn pg_foreign_server_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("srvname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("srvowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("srvfdw", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("srvtype", SqlType::new(SqlTypeKind::Text), true),
            column_desc("srvversion", SqlType::new(SqlTypeKind::Text), true),
            column_desc(
                "srvacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "srvoptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_foreign_server_rows() -> [PgForeignServerRow; 0] {
    []
}
