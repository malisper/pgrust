use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub fn pg_parameter_acl_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("parname", SqlType::new(SqlTypeKind::Text), false),
            column_desc(
                "paracl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}
