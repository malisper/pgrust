use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub fn pg_replication_origin_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("roident", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("roname", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_replication_origin_desc_matches_expected_columns() {
        let desc = pg_replication_origin_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["roident", "roname"]);
    }
}
