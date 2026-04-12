use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAttrdefRow {
    pub oid: u32,
    pub adrelid: u32,
    pub adnum: i16,
    pub adbin: String,
}

pub fn pg_attrdef_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("adrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("adnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("adbin", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_attrdef_desc_contains_default_columns() {
        let desc = pg_attrdef_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["oid", "adrelid", "adnum", "adbin"]);
    }
}
