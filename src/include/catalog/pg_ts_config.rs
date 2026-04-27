use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::pg_ts_parser::DEFAULT_TS_PARSER_OID;
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID};

pub const SIMPLE_TS_CONFIG_OID: u32 = 3748;
pub const ENGLISH_TS_CONFIG_OID: u32 = 10080;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTsConfigRow {
    pub oid: u32,
    pub cfgname: String,
    pub cfgnamespace: u32,
    pub cfgowner: u32,
    pub cfgparser: u32,
}

pub fn pg_ts_config_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("cfgname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("cfgnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("cfgowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("cfgparser", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_ts_config_rows() -> [PgTsConfigRow; 2] {
    [
        PgTsConfigRow {
            oid: SIMPLE_TS_CONFIG_OID,
            cfgname: "simple".into(),
            cfgnamespace: PG_CATALOG_NAMESPACE_OID,
            cfgowner: BOOTSTRAP_SUPERUSER_OID,
            cfgparser: DEFAULT_TS_PARSER_OID,
        },
        PgTsConfigRow {
            oid: ENGLISH_TS_CONFIG_OID,
            cfgname: "english".into(),
            cfgnamespace: PG_CATALOG_NAMESPACE_OID,
            cfgowner: BOOTSTRAP_SUPERUSER_OID,
            cfgparser: DEFAULT_TS_PARSER_OID,
        },
    ]
}
