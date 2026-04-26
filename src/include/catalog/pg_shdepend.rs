use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const SHARED_DEPENDENCY_OWNER: char = 'o';
pub const SHARED_DEPENDENCY_ACL: char = 'a';
pub const SHARED_DEPENDENCY_INITACL: char = 'i';
pub const SHARED_DEPENDENCY_POLICY: char = 'r';
pub const SHARED_DEPENDENCY_TABLESPACE: char = 't';

pub fn pg_shdepend_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("dbid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("classid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objsubid", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("refclassid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("refobjid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("deptype", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_shdepend_desc_contains_dependency_columns() {
        let desc = pg_shdepend_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "dbid",
                "classid",
                "objid",
                "objsubid",
                "refclassid",
                "refobjid",
                "deptype",
            ]
        );
    }
}
