use crate::desc::column_desc;
pub use pgrust_core::PolicyCommand;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPolicyRow {
    pub oid: u32,
    pub polname: String,
    pub polrelid: u32,
    pub polcmd: PolicyCommand,
    pub polpermissive: bool,
    pub polroles: Vec<u32>,
    pub polqual: Option<String>,
    pub polwithcheck: Option<String>,
}

pub fn pg_policy_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("polname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("polrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("polcmd", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("polpermissive", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "polroles",
                SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
                false,
            ),
            column_desc("polqual", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc("polwithcheck", SqlType::new(SqlTypeKind::PgNodeTree), true),
        ],
    }
}

pub fn bootstrap_pg_policy_rows() -> [PgPolicyRow; 0] {
    []
}

pub fn sort_pg_policy_rows(rows: &mut [PgPolicyRow]) {
    rows.sort_by(|left, right| {
        left.polrelid
            .cmp(&right.polrelid)
            .then_with(|| left.polname.cmp(&right.polname))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
