use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgIndexRow {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub indkey: Vec<i16>,
    pub indcollation: Vec<u32>,
    pub indclass: Vec<u32>,
    pub indoption: Vec<i16>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
}

pub fn pg_index_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("indexrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("indrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("indnatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("indnkeyatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("indisunique", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "indnullsnotdistinct",
                SqlType::new(SqlTypeKind::Bool),
                false,
            ),
            column_desc("indisprimary", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisexclusion", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indimmediate", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisclustered", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisvalid", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indcheckxmin", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisready", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indislive", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indisreplident", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("indkey", SqlType::new(SqlTypeKind::Int2Vector), false),
            column_desc("indcollation", SqlType::new(SqlTypeKind::OidVector), false),
            column_desc("indclass", SqlType::new(SqlTypeKind::OidVector), false),
            column_desc("indoption", SqlType::new(SqlTypeKind::Int2Vector), false),
            column_desc("indexprs", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc("indpred", SqlType::new(SqlTypeKind::PgNodeTree), true),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_index_desc_contains_index_columns() {
        let desc = pg_index_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "indexrelid",
                "indrelid",
                "indnatts",
                "indnkeyatts",
                "indisunique",
                "indnullsnotdistinct",
                "indisprimary",
                "indisexclusion",
                "indimmediate",
                "indisclustered",
                "indisvalid",
                "indcheckxmin",
                "indisready",
                "indislive",
                "indisreplident",
                "indkey",
                "indcollation",
                "indclass",
                "indoption",
                "indexprs",
                "indpred",
            ]
        );
    }
}
