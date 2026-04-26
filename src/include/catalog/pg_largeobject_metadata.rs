use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{ACLITEM_ARRAY_TYPE_OID, ACLITEM_TYPE_OID};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgLargeobjectMetadataRow {
    pub oid: u32,
    pub lomowner: u32,
    pub lomacl: Vec<String>,
}

pub fn pg_largeobject_metadata_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("lomowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "lomacl",
                SqlType::array_of(
                    SqlType::new(SqlTypeKind::Text).with_identity(ACLITEM_TYPE_OID, 0),
                )
                .with_identity(ACLITEM_ARRAY_TYPE_OID, 0),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_largeobject_metadata_rows() -> [PgLargeobjectMetadataRow; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_largeobject_metadata_desc_matches_expected_columns() {
        let desc = pg_largeobject_metadata_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["oid", "lomowner", "lomacl"]);
    }
}
