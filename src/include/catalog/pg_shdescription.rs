use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub fn pg_shdescription_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("objoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("classoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("description", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}
