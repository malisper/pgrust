use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub fn pg_extension_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("extname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("extowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("extnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("extrelocatable", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("extversion", SqlType::new(SqlTypeKind::Text), false),
            column_desc(
                "extconfig",
                SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
                true,
            ),
            column_desc(
                "extcondition",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}
