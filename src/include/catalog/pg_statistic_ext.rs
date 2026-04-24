use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    PG_DEPENDENCIES_TYPE_OID, PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID,
    PG_STATISTIC_RELATION_OID, PG_STATISTIC_ROWTYPE_OID,
};
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgStatisticExtRow {
    pub oid: u32,
    pub stxrelid: u32,
    pub stxname: String,
    pub stxnamespace: u32,
    pub stxowner: u32,
    pub stxkeys: Vec<i16>,
    pub stxstattarget: Option<i16>,
    pub stxkind: Vec<u8>,
    pub stxexprs: Option<String>,
}

pub fn pg_statistic_ext_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stxrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stxname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("stxnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stxowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stxkeys", SqlType::new(SqlTypeKind::Int2Vector), false),
            column_desc("stxstattarget", SqlType::new(SqlTypeKind::Int2), true),
            column_desc(
                "stxkind",
                SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar)),
                false,
            ),
            column_desc("stxexprs", SqlType::new(SqlTypeKind::PgNodeTree), true),
        ],
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PgStatisticExtDataRow {
    pub stxoid: u32,
    pub stxdinherit: bool,
    pub stxdndistinct: Option<Vec<u8>>,
    pub stxddependencies: Option<Vec<u8>>,
    pub stxdmcv: Option<Vec<u8>>,
    pub stxdexpr: Option<Vec<crate::include::catalog::PgStatisticRow>>,
}

pub fn pg_statistic_ext_data_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("stxoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stxdinherit", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "stxdndistinct",
                SqlType::new(SqlTypeKind::Bytea).with_identity(PG_NDISTINCT_TYPE_OID, 0),
                true,
            ),
            column_desc(
                "stxddependencies",
                SqlType::new(SqlTypeKind::Bytea).with_identity(PG_DEPENDENCIES_TYPE_OID, 0),
                true,
            ),
            column_desc(
                "stxdmcv",
                SqlType::new(SqlTypeKind::Bytea).with_identity(PG_MCV_LIST_TYPE_OID, 0),
                true,
            ),
            column_desc(
                "stxdexpr",
                SqlType::array_of(SqlType::named_composite(
                    PG_STATISTIC_ROWTYPE_OID,
                    PG_STATISTIC_RELATION_OID,
                )),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_statistic_ext_rows() -> [PgStatisticExtRow; 0] {
    []
}

pub fn bootstrap_pg_statistic_ext_data_rows() -> [PgStatisticExtDataRow; 0] {
    []
}

pub fn statistic_ext_kind_values(kinds: &[u8]) -> Vec<Value> {
    kinds.iter().copied().map(Value::InternalChar).collect()
}
