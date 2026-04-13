use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::pg_ts_config::SIMPLE_TS_CONFIG_OID;
use crate::include::catalog::pg_ts_dict::SIMPLE_TS_DICTIONARY_OID;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTsConfigMapRow {
    pub mapcfg: u32,
    pub maptokentype: i32,
    pub mapseqno: i32,
    pub mapdict: u32,
}

pub fn pg_ts_config_map_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("mapcfg", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("maptokentype", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("mapseqno", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("mapdict", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_ts_config_map_rows() -> Vec<PgTsConfigMapRow> {
    [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22,
    ]
    .into_iter()
    .map(|token| PgTsConfigMapRow {
        mapcfg: SIMPLE_TS_CONFIG_OID,
        maptokentype: token,
        mapseqno: 1,
        mapdict: SIMPLE_TS_DICTIONARY_OID,
    })
    .collect()
}
