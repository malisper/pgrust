use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgIndexRow {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indisvalid: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indkey: String,
}

pub fn pg_index_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("indexrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("indrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("indnatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("indnkeyatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("indisunique", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisvalid", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisready", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indislive", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indkey", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_index_desc_contains_index_columns() {
        let desc = pg_index_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "indexrelid",
                "indrelid",
                "indnatts",
                "indnkeyatts",
                "indisunique",
                "indisvalid",
                "indisready",
                "indislive",
                "indkey",
            ]
        );
    }
}
