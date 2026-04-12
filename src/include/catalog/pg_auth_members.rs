use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAuthMembersRow {
    pub oid: u32,
    pub roleid: u32,
    pub member: u32,
    pub grantor: u32,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

pub fn pg_auth_members_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("roleid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("member", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("grantor", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("admin_option", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("inherit_option", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("set_option", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn bootstrap_pg_auth_members_rows() -> [PgAuthMembersRow; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_auth_members_desc_matches_expected_columns() {
        let desc = pg_auth_members_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "roleid",
                "member",
                "grantor",
                "admin_option",
                "inherit_option",
                "set_option",
            ]
        );
    }
}
