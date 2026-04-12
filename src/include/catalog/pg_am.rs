use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const HEAP_TABLE_AM_OID: u32 = 2;
pub const BTREE_AM_OID: u32 = 403;
pub const HASH_AM_OID: u32 = 405;
pub const GIST_AM_OID: u32 = 783;
pub const GIN_AM_OID: u32 = 2742;
pub const BRIN_AM_OID: u32 = 3580;
pub const SPGIST_AM_OID: u32 = 4000;

const HEAP_TABLE_AM_HANDLER_OID: u32 = 3;
const BTREE_AM_HANDLER_OID: u32 = 330;
const HASH_AM_HANDLER_OID: u32 = 331;
const GIST_AM_HANDLER_OID: u32 = 332;
const GIN_AM_HANDLER_OID: u32 = 333;
const SPGIST_AM_HANDLER_OID: u32 = 334;
const BRIN_AM_HANDLER_OID: u32 = 335;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAmRow {
    pub oid: u32,
    pub amname: String,
    pub amhandler: u32,
    pub amtype: char,
}

pub fn pg_am_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("amhandler", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amtype", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

pub fn bootstrap_pg_am_rows() -> [PgAmRow; 7] {
    [
        PgAmRow {
            oid: HEAP_TABLE_AM_OID,
            amname: "heap".into(),
            amhandler: HEAP_TABLE_AM_HANDLER_OID,
            amtype: 't',
        },
        PgAmRow {
            oid: BTREE_AM_OID,
            amname: "btree".into(),
            amhandler: BTREE_AM_HANDLER_OID,
            amtype: 'i',
        },
        PgAmRow {
            oid: HASH_AM_OID,
            amname: "hash".into(),
            amhandler: HASH_AM_HANDLER_OID,
            amtype: 'i',
        },
        PgAmRow {
            oid: GIST_AM_OID,
            amname: "gist".into(),
            amhandler: GIST_AM_HANDLER_OID,
            amtype: 'i',
        },
        PgAmRow {
            oid: GIN_AM_OID,
            amname: "gin".into(),
            amhandler: GIN_AM_HANDLER_OID,
            amtype: 'i',
        },
        PgAmRow {
            oid: BRIN_AM_OID,
            amname: "brin".into(),
            amhandler: BRIN_AM_HANDLER_OID,
            amtype: 'i',
        },
        PgAmRow {
            oid: SPGIST_AM_OID,
            amname: "spgist".into(),
            amhandler: SPGIST_AM_HANDLER_OID,
            amtype: 'i',
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_am_desc_matches_expected_columns() {
        let desc = pg_am_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["oid", "amname", "amhandler", "amtype"]);
    }
}
