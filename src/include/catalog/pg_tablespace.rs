use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::BOOTSTRAP_SUPERUSER_OID;

pub const DEFAULT_TABLESPACE_OID: u32 = 1663;
pub const GLOBAL_TABLESPACE_OID: u32 = 1664;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTablespaceRow {
    pub oid: u32,
    pub spcname: String,
    pub spcowner: u32,
    pub spcacl: Option<Vec<String>>,
    pub spcoptions: Option<Vec<String>>,
}

pub fn pg_tablespace_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("spcname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("spcowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "spcacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "spcoptions",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_tablespace_rows() -> [PgTablespaceRow; 2] {
    [
        PgTablespaceRow {
            oid: DEFAULT_TABLESPACE_OID,
            spcname: "pg_default".into(),
            spcowner: BOOTSTRAP_SUPERUSER_OID,
            spcacl: None,
            spcoptions: None,
        },
        PgTablespaceRow {
            oid: GLOBAL_TABLESPACE_OID,
            spcname: "pg_global".into(),
            spcowner: BOOTSTRAP_SUPERUSER_OID,
            spcacl: None,
            spcoptions: None,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_tablespace_desc_matches_expected_columns() {
        let desc = pg_tablespace_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["oid", "spcname", "spcowner", "spcacl", "spcoptions"]
        );
    }
}
