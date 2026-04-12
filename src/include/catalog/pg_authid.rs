use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const BOOTSTRAP_SUPERUSER_OID: u32 = 10;
pub const BOOTSTRAP_SUPERUSER_NAME: &str = "postgres";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAuthIdRow {
    pub oid: u32,
    pub rolname: String,
    pub rolsuper: bool,
    pub rolinherit: bool,
    pub rolcreaterole: bool,
    pub rolcreatedb: bool,
    pub rolcanlogin: bool,
    pub rolreplication: bool,
    pub rolbypassrls: bool,
    pub rolconnlimit: i32,
}

pub fn pg_authid_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("rolname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("rolsuper", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolinherit", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolcreaterole", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolcreatedb", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolcanlogin", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolreplication", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolbypassrls", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("rolconnlimit", SqlType::new(SqlTypeKind::Int4), false),
        ],
    }
}

pub fn bootstrap_pg_authid_rows() -> [PgAuthIdRow; 1] {
    [
        // :HACK: PostgreSQL bootstraps additional predefined roles, but pgrust
        // still models a single built-in superuser until ownership and ACL
        // semantics are implemented more fully.
        PgAuthIdRow {
            oid: BOOTSTRAP_SUPERUSER_OID,
            rolname: BOOTSTRAP_SUPERUSER_NAME.into(),
            rolsuper: true,
            rolinherit: true,
            rolcreaterole: true,
            rolcreatedb: true,
            rolcanlogin: true,
            rolreplication: true,
            rolbypassrls: true,
            rolconnlimit: -1,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_authid_desc_matches_expected_columns() {
        let desc = pg_authid_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "rolname",
                "rolsuper",
                "rolinherit",
                "rolcreaterole",
                "rolcreatedb",
                "rolcanlogin",
                "rolreplication",
                "rolbypassrls",
                "rolconnlimit",
            ]
        );
    }
}
