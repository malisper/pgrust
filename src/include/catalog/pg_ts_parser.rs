use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;

pub const DEFAULT_TS_PARSER_OID: u32 = 3722;
const PRSD_START_PROC_OID: u32 = 3717;
const PRSD_NEXTTOKEN_PROC_OID: u32 = 3718;
const PRSD_END_PROC_OID: u32 = 3719;
const PRSD_HEADLINE_PROC_OID: u32 = 3720;
const PRSD_LEXTYPE_PROC_OID: u32 = 3721;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTsParserRow {
    pub oid: u32,
    pub prsname: String,
    pub prsnamespace: u32,
    pub prsstart: u32,
    pub prstoken: u32,
    pub prsend: u32,
    pub prsheadline: Option<u32>,
    pub prslextype: u32,
}

pub fn pg_ts_parser_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prsname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("prsnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prsstart", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prstoken", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prsend", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prsheadline", SqlType::new(SqlTypeKind::Oid), true),
            column_desc("prslextype", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_ts_parser_rows() -> [PgTsParserRow; 1] {
    [PgTsParserRow {
        oid: DEFAULT_TS_PARSER_OID,
        prsname: "default".into(),
        prsnamespace: PG_CATALOG_NAMESPACE_OID,
        prsstart: PRSD_START_PROC_OID,
        prstoken: PRSD_NEXTTOKEN_PROC_OID,
        prsend: PRSD_END_PROC_OID,
        prsheadline: Some(PRSD_HEADLINE_PROC_OID),
        prslextype: PRSD_LEXTYPE_PROC_OID,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_ts_parser_desc_matches_expected_columns() {
        let desc = pg_ts_parser_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "prsname",
                "prsnamespace",
                "prsstart",
                "prstoken",
                "prsend",
                "prsheadline",
                "prslextype",
            ]
        );
    }
}
