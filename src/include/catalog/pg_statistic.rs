use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::ArrayValue;

#[derive(Debug, Clone, PartialEq)]
pub struct PgStatisticRow {
    pub starelid: u32,
    pub staattnum: i16,
    pub stainherit: bool,
    pub stanullfrac: f64,
    pub stawidth: i32,
    pub stadistinct: f64,
    pub stakind: [i16; 5],
    pub staop: [u32; 5],
    pub stacoll: [u32; 5],
    pub stanumbers: [Option<ArrayValue>; 5],
    pub stavalues: [Option<ArrayValue>; 5],
}

pub fn pg_statistic_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("starelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("staattnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("stainherit", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("stanullfrac", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("stawidth", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("stadistinct", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("stakind1", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("stakind2", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("stakind3", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("stakind4", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("stakind5", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("staop1", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("staop2", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("staop3", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("staop4", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("staop5", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stacoll1", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stacoll2", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stacoll3", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stacoll4", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("stacoll5", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "stanumbers1",
                SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                true,
            ),
            column_desc(
                "stanumbers2",
                SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                true,
            ),
            column_desc(
                "stanumbers3",
                SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                true,
            ),
            column_desc(
                "stanumbers4",
                SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                true,
            ),
            column_desc(
                "stanumbers5",
                SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                true,
            ),
            column_desc(
                "stavalues1",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "stavalues2",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "stavalues3",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "stavalues4",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "stavalues5",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn sort_pg_statistic_rows(rows: &mut [PgStatisticRow]) {
    rows.sort_by_key(|row| (row.starelid, row.staattnum, row.stainherit));
}
