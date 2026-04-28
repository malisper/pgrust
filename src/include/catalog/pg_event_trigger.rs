use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgEventTriggerRow {
    pub oid: u32,
    pub evtname: String,
    pub evtevent: String,
    pub evtowner: u32,
    pub evtfoid: u32,
    pub evtenabled: char,
    pub evttags: Option<Vec<String>>,
}

pub fn pg_event_trigger_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("evtname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("evtevent", SqlType::new(SqlTypeKind::Name), false),
            column_desc("evtowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("evtfoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("evtenabled", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc(
                "evttags",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_event_trigger_rows() -> [PgEventTriggerRow; 0] {
    []
}

pub fn sort_pg_event_trigger_rows(rows: &mut [PgEventTriggerRow]) {
    rows.sort_by(|left, right| {
        left.evtname
            .cmp(&right.evtname)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
