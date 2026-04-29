use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgDefaultAclRow {
    pub oid: u32,
    pub defaclrole: u32,
    pub defaclnamespace: u32,
    pub defaclobjtype: char,
    pub defaclacl: Option<Vec<String>>,
}

pub fn pg_default_acl_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("defaclrole", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("defaclnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "defaclobjtype",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "defaclacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}
