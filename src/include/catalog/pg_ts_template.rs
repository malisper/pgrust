use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;

pub const SIMPLE_TS_TEMPLATE_OID: u32 = 3727;
pub const SYNONYM_TS_TEMPLATE_OID: u32 = 3730;
pub const ISPELL_TS_TEMPLATE_OID: u32 = 3733;
pub const THESAURUS_TS_TEMPLATE_OID: u32 = 3742;

const DSIMPLE_INIT_PROC_OID: u32 = 3725;
const DSIMPLE_LEXIZE_PROC_OID: u32 = 3726;
const DSYNONYM_INIT_PROC_OID: u32 = 3728;
const DSYNONYM_LEXIZE_PROC_OID: u32 = 3729;
const DISPELL_INIT_PROC_OID: u32 = 3731;
const DISPELL_LEXIZE_PROC_OID: u32 = 3732;
const THESAURUS_INIT_PROC_OID: u32 = 3740;
const THESAURUS_LEXIZE_PROC_OID: u32 = 3741;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTsTemplateRow {
    pub oid: u32,
    pub tmplname: String,
    pub tmplnamespace: u32,
    pub tmplinit: Option<u32>,
    pub tmpllexize: u32,
}

pub fn pg_ts_template_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tmplname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("tmplnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tmplinit", SqlType::new(SqlTypeKind::Oid), true),
            column_desc("tmpllexize", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_ts_template_rows() -> [PgTsTemplateRow; 4] {
    [
        PgTsTemplateRow {
            oid: SIMPLE_TS_TEMPLATE_OID,
            tmplname: "simple".into(),
            tmplnamespace: PG_CATALOG_NAMESPACE_OID,
            tmplinit: Some(DSIMPLE_INIT_PROC_OID),
            tmpllexize: DSIMPLE_LEXIZE_PROC_OID,
        },
        PgTsTemplateRow {
            oid: SYNONYM_TS_TEMPLATE_OID,
            tmplname: "synonym".into(),
            tmplnamespace: PG_CATALOG_NAMESPACE_OID,
            tmplinit: Some(DSYNONYM_INIT_PROC_OID),
            tmpllexize: DSYNONYM_LEXIZE_PROC_OID,
        },
        PgTsTemplateRow {
            oid: ISPELL_TS_TEMPLATE_OID,
            tmplname: "ispell".into(),
            tmplnamespace: PG_CATALOG_NAMESPACE_OID,
            tmplinit: Some(DISPELL_INIT_PROC_OID),
            tmpllexize: DISPELL_LEXIZE_PROC_OID,
        },
        PgTsTemplateRow {
            oid: THESAURUS_TS_TEMPLATE_OID,
            tmplname: "thesaurus".into(),
            tmplnamespace: PG_CATALOG_NAMESPACE_OID,
            tmplinit: Some(THESAURUS_INIT_PROC_OID),
            tmpllexize: THESAURUS_LEXIZE_PROC_OID,
        },
    ]
}
