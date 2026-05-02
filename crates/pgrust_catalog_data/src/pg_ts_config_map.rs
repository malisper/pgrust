use crate::desc::column_desc;
use crate::pg_ts_config::{ENGLISH_TS_CONFIG_OID, SIMPLE_TS_CONFIG_OID};
use crate::pg_ts_dict::{ENGLISH_STEM_TS_DICTIONARY_OID, SIMPLE_TS_DICTIONARY_OID};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

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
    let mut rows = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22,
    ]
    .into_iter()
    .map(|token| PgTsConfigMapRow {
        mapcfg: SIMPLE_TS_CONFIG_OID,
        maptokentype: token,
        mapseqno: 1,
        mapdict: SIMPLE_TS_DICTIONARY_OID,
    })
    .collect::<Vec<_>>();
    rows.extend(
        [
            (1, ENGLISH_STEM_TS_DICTIONARY_OID),
            (2, ENGLISH_STEM_TS_DICTIONARY_OID),
            (3, ENGLISH_STEM_TS_DICTIONARY_OID),
            (4, SIMPLE_TS_DICTIONARY_OID),
            (5, SIMPLE_TS_DICTIONARY_OID),
            (6, SIMPLE_TS_DICTIONARY_OID),
            (7, SIMPLE_TS_DICTIONARY_OID),
            (8, SIMPLE_TS_DICTIONARY_OID),
            (9, ENGLISH_STEM_TS_DICTIONARY_OID),
            (10, ENGLISH_STEM_TS_DICTIONARY_OID),
            (11, ENGLISH_STEM_TS_DICTIONARY_OID),
            (15, ENGLISH_STEM_TS_DICTIONARY_OID),
            (16, ENGLISH_STEM_TS_DICTIONARY_OID),
            (17, ENGLISH_STEM_TS_DICTIONARY_OID),
            (18, SIMPLE_TS_DICTIONARY_OID),
            (19, SIMPLE_TS_DICTIONARY_OID),
            (20, SIMPLE_TS_DICTIONARY_OID),
            (21, SIMPLE_TS_DICTIONARY_OID),
            (22, SIMPLE_TS_DICTIONARY_OID),
        ]
        .into_iter()
        .map(|(token, dict)| PgTsConfigMapRow {
            mapcfg: ENGLISH_TS_CONFIG_OID,
            maptokentype: token,
            mapseqno: 1,
            mapdict: dict,
        }),
    );
    rows
}
