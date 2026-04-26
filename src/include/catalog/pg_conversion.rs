use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgConversionRow {
    pub oid: u32,
    pub conname: String,
    pub connamespace: u32,
    pub conowner: u32,
    pub conforencoding: i32,
    pub contoencoding: i32,
    pub conproc: u32,
    pub condefault: bool,
}

pub fn pg_conversion_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("connamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("conforencoding", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("contoencoding", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("conproc", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("condefault", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn bootstrap_pg_conversion_rows() -> [PgConversionRow; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_conversion_desc_matches_expected_columns() {
        let desc = pg_conversion_desc();
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
                "conowner",
                "conforencoding",
                "contoencoding",
                "conproc",
                "condefault",
            ]
        );
    }
}
