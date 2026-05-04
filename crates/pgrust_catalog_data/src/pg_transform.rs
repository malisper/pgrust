use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTransformRow {
    pub oid: u32,
    pub trftype: u32,
    pub trflang: u32,
    pub trffromsql: u32,
    pub trftosql: u32,
}

pub fn pg_transform_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("trftype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("trflang", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("trffromsql", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("trftosql", SqlType::new(SqlTypeKind::RegProc), false),
        ],
    }
}
