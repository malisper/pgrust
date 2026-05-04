use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgForeignDataWrapperRow {
    pub oid: u32,
    pub fdwname: String,
    pub fdwowner: u32,
    pub fdwhandler: u32,
    pub fdwvalidator: u32,
    pub fdwacl: Option<Vec<String>>,
    pub fdwoptions: Option<Vec<String>>,
}

pub fn pg_foreign_data_wrapper_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("fdwname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("fdwowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("fdwhandler", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("fdwvalidator", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "fdwacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "fdwoptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_foreign_data_wrapper_rows() -> [PgForeignDataWrapperRow; 0] {
    []
}
