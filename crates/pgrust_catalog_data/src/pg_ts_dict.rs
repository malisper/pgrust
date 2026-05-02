use crate::desc::column_desc;
use crate::{BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub const SIMPLE_TS_DICTIONARY_OID: u32 = 3765;
pub const ENGLISH_STEM_TS_DICTIONARY_OID: u32 = 12_001;
const SIMPLE_TS_TEMPLATE_OID: u32 = 3727;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTsDictRow {
    pub oid: u32,
    pub dictname: String,
    pub dictnamespace: u32,
    pub dictowner: u32,
    pub dicttemplate: u32,
    pub dictinitoption: Option<String>,
}

pub fn pg_ts_dict_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("dictname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("dictnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("dictowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("dicttemplate", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("dictinitoption", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
}

pub fn bootstrap_pg_ts_dict_rows() -> [PgTsDictRow; 2] {
    [
        PgTsDictRow {
            oid: SIMPLE_TS_DICTIONARY_OID,
            dictname: "simple".into(),
            dictnamespace: PG_CATALOG_NAMESPACE_OID,
            dictowner: BOOTSTRAP_SUPERUSER_OID,
            dicttemplate: SIMPLE_TS_TEMPLATE_OID,
            dictinitoption: None,
        },
        PgTsDictRow {
            oid: ENGLISH_STEM_TS_DICTIONARY_OID,
            dictname: "english_stem".into(),
            dictnamespace: PG_CATALOG_NAMESPACE_OID,
            dictowner: BOOTSTRAP_SUPERUSER_OID,
            dicttemplate: SIMPLE_TS_TEMPLATE_OID,
            dictinitoption: None,
        },
    ]
}
