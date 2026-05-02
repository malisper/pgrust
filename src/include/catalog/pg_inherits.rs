use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
pub use pgrust_core::PgInheritsRow;

pub fn pg_inherits_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("inhrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("inhparent", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("inhseqno", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("inhdetachpending", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn bootstrap_pg_inherits_rows() -> [PgInheritsRow; 0] {
    []
}

pub fn sort_pg_inherits_rows(rows: &mut [PgInheritsRow]) {
    rows.sort_by_key(|row| (row.inhrelid, row.inhseqno, row.inhparent));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_inherits_desc_matches_expected_columns() {
        let desc = pg_inherits_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["inhrelid", "inhparent", "inhseqno", "inhdetachpending"]
        );
    }
}
