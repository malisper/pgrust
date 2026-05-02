use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgForeignTableRow {
    pub ftrelid: u32,
    pub ftserver: u32,
    pub ftoptions: Option<Vec<String>>,
}

pub fn pg_foreign_table_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("ftrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("ftserver", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "ftoptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_foreign_table_rows() -> [PgForeignTableRow; 0] {
    []
}
