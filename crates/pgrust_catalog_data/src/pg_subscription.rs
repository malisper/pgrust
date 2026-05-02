use crate::desc::column_desc;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgSubscriptionRow {
    pub oid: u32,
    pub subdbid: u32,
    pub subskiplsn: u64,
    pub subname: String,
    pub subowner: u32,
    pub subenabled: bool,
    pub subbinary: bool,
    pub substream: char,
    pub subtwophasestate: char,
    pub subdisableonerr: bool,
    pub subpasswordrequired: bool,
    pub subrunasowner: bool,
    pub subfailover: bool,
    pub subconninfo: String,
    pub subslotname: Option<String>,
    pub subsynccommit: String,
    pub subpublications: Vec<String>,
    pub suborigin: String,
}

pub fn pg_subscription_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("subdbid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("subskiplsn", SqlType::new(SqlTypeKind::PgLsn), false),
            column_desc("subname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("subowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("subenabled", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("subbinary", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("substream", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc(
                "subtwophasestate",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("subdisableonerr", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "subpasswordrequired",
                SqlType::new(SqlTypeKind::Bool),
                false,
            ),
            column_desc("subrunasowner", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("subfailover", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("subconninfo", SqlType::new(SqlTypeKind::Text), false),
            column_desc("subslotname", SqlType::new(SqlTypeKind::Name), true),
            column_desc("subsynccommit", SqlType::new(SqlTypeKind::Text), false),
            column_desc(
                "subpublications",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                false,
            ),
            column_desc("suborigin", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}
