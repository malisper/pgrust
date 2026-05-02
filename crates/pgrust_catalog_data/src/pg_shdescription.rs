use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub fn pg_shdescription_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("objoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("classoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("description", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}
