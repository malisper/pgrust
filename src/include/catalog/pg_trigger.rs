use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTriggerRow {
    pub oid: u32,
    pub tgrelid: u32,
    pub tgparentid: u32,
    pub tgname: String,
    pub tgfoid: u32,
    pub tgtype: i16,
    pub tgenabled: char,
    pub tgisinternal: bool,
    pub tgconstrrelid: u32,
    pub tgconstrindid: u32,
    pub tgconstraint: u32,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgattr: Vec<i16>,
    pub tgargs: Vec<String>,
    pub tgqual: Option<String>,
    pub tgoldtable: Option<String>,
    pub tgnewtable: Option<String>,
}

pub fn pg_trigger_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgparentid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("tgfoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgtype", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("tgenabled", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("tgisinternal", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("tgconstrrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgconstrindid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgconstraint", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("tgdeferrable", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("tginitdeferred", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("tgnargs", SqlType::new(SqlTypeKind::Int2), false),
            column_desc(
                "tgattr",
                SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
                false,
            ),
            column_desc(
                "tgargs",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                false,
            ),
            column_desc("tgqual", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc("tgoldtable", SqlType::new(SqlTypeKind::Name), true),
            column_desc("tgnewtable", SqlType::new(SqlTypeKind::Name), true),
        ],
    }
}

pub fn bootstrap_pg_trigger_rows() -> [PgTriggerRow; 0] {
    []
}

pub fn sort_pg_trigger_rows(rows: &mut [PgTriggerRow]) {
    rows.sort_by(|left, right| {
        left.tgrelid
            .cmp(&right.tgrelid)
            .then_with(|| left.tgname.cmp(&right.tgname))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
