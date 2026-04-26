use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub fn pg_largeobject_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("loid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("pageno", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("data", SqlType::new(SqlTypeKind::Bytea), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_largeobject_desc_matches_expected_columns() {
        let desc = pg_largeobject_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["loid", "pageno", "data"]);
    }
}
