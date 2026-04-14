use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRewriteRow {
    pub oid: u32,
    pub rulename: String,
    pub ev_class: u32,
    pub ev_type: char,
    pub ev_enabled: char,
    pub is_instead: bool,
    pub ev_qual: String,
    pub ev_action: String,
}

pub fn pg_rewrite_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rulename", SqlType::new(SqlTypeKind::Name), false),
            column_desc("ev_class", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("ev_type", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("ev_enabled", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("is_instead", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("ev_qual", SqlType::new(SqlTypeKind::PgNodeTree), false),
            column_desc("ev_action", SqlType::new(SqlTypeKind::PgNodeTree), false),
        ],
    }
}

pub fn sort_pg_rewrite_rows(rows: &mut [PgRewriteRow]) {
    rows.sort_by(|left, right| {
        left.ev_class
            .cmp(&right.ev_class)
            .then_with(|| left.rulename.cmp(&right.rulename))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
