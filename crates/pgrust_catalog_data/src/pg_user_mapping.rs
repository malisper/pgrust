use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgUserMappingRow {
    pub oid: u32,
    pub umuser: u32,
    pub umserver: u32,
    pub umoptions: Option<Vec<String>>,
}

pub fn pg_user_mapping_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("umuser", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("umserver", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "umoptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_user_mapping_rows() -> [PgUserMappingRow; 0] {
    []
}
