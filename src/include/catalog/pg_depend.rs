use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const DEPENDENCY_NORMAL: char = 'n';
pub const DEPENDENCY_AUTO: char = 'a';
pub const DEPENDENCY_INTERNAL: char = 'i';
pub const DEPENDENCY_PARTITION_PRI: char = 'P';
pub const DEPENDENCY_PARTITION_SEC: char = 'S';
pub const DEPENDENCY_EXTENSION: char = 'e';
pub const DEPENDENCY_AUTO_EXTENSION: char = 'x';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgDependRow {
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
    pub refclassid: u32,
    pub refobjid: u32,
    pub refobjsubid: i32,
    pub deptype: char,
}

pub fn pg_depend_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("classid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("objsubid", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("refclassid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("refobjid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("refobjsubid", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("deptype", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_depend_desc_contains_dependency_columns() {
        let desc = pg_depend_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "classid",
                "objid",
                "objsubid",
                "refclassid",
                "refobjid",
                "refobjsubid",
                "deptype",
            ]
        );
    }
}
