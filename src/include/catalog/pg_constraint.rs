use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const CONSTRAINT_CHECK: char = 'c';
pub const CONSTRAINT_FOREIGN: char = 'f';
pub const CONSTRAINT_NOTNULL: char = 'n';
pub const CONSTRAINT_PRIMARY: char = 'p';
pub const CONSTRAINT_UNIQUE: char = 'u';
pub const CONSTRAINT_TRIGGER: char = 't';
pub const CONSTRAINT_EXCLUSION: char = 'x';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgConstraintRow {
    pub oid: u32,
    pub conname: String,
    pub connamespace: u32,
    pub contype: char,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: u32,
    pub contypid: u32,
    pub conindid: u32,
    pub conparentid: u32,
    pub confrelid: u32,
    pub confupdtype: char,
    pub confdeltype: char,
    pub confmatchtype: char,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
}

pub fn pg_constraint_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("connamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("contype", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("condeferrable", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("condeferred", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("conenforced", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("convalidated", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("conrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("contypid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conindid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conparentid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("confrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "confupdtype",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "confdeltype",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "confmatchtype",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("conislocal", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("coninhcount", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("connoinherit", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("conperiod", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn bootstrap_pg_constraint_rows() -> [PgConstraintRow; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_constraint_desc_matches_expected_columns() {
        let desc = pg_constraint_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "conname",
                "connamespace",
                "contype",
                "condeferrable",
                "condeferred",
                "conenforced",
                "convalidated",
                "conrelid",
                "contypid",
                "conindid",
                "conparentid",
                "confrelid",
                "confupdtype",
                "confdeltype",
                "confmatchtype",
                "conislocal",
                "coninhcount",
                "connoinherit",
                "conperiod",
            ]
        );
    }
}
