use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub const BOOTSTRAP_SUPERUSER_OID: u32 = 10;
pub const BOOTSTRAP_SUPERUSER_NAME: &str = "postgres";
pub const PG_DATABASE_OWNER_OID: u32 = 6171;
pub const PG_READ_ALL_DATA_OID: u32 = 6181;
pub const PG_WRITE_ALL_DATA_OID: u32 = 6182;
pub const PG_MONITOR_OID: u32 = 3373;
pub const PG_READ_ALL_SETTINGS_OID: u32 = 3374;
pub const PG_READ_ALL_STATS_OID: u32 = 3375;
pub const PG_STAT_SCAN_TABLES_OID: u32 = 3377;
pub const PG_READ_SERVER_FILES_OID: u32 = 4569;
pub const PG_WRITE_SERVER_FILES_OID: u32 = 4570;
pub const PG_EXECUTE_SERVER_PROGRAM_OID: u32 = 4571;
pub const PG_SIGNAL_BACKEND_OID: u32 = 4200;

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
            column_desc("rolname", SqlType::new(SqlTypeKind::Name), false),
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

pub fn bootstrap_pg_authid_rows() -> Vec<PgAuthIdRow> {
    vec![
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
        predefined_role(PG_DATABASE_OWNER_OID, "pg_database_owner"),
        predefined_role(PG_READ_ALL_DATA_OID, "pg_read_all_data"),
        predefined_role(PG_WRITE_ALL_DATA_OID, "pg_write_all_data"),
        predefined_role(PG_MONITOR_OID, "pg_monitor"),
        predefined_role(PG_READ_ALL_SETTINGS_OID, "pg_read_all_settings"),
        predefined_role(PG_READ_ALL_STATS_OID, "pg_read_all_stats"),
        predefined_role(PG_STAT_SCAN_TABLES_OID, "pg_stat_scan_tables"),
        predefined_role(PG_READ_SERVER_FILES_OID, "pg_read_server_files"),
        predefined_role(PG_WRITE_SERVER_FILES_OID, "pg_write_server_files"),
        predefined_role(PG_EXECUTE_SERVER_PROGRAM_OID, "pg_execute_server_program"),
        predefined_role(PG_SIGNAL_BACKEND_OID, "pg_signal_backend"),
    ]
}

fn predefined_role(oid: u32, name: &str) -> PgAuthIdRow {
    PgAuthIdRow {
        oid,
        rolname: name.into(),
        rolsuper: false,
        rolinherit: true,
        rolcreaterole: false,
        rolcreatedb: false,
        rolcanlogin: false,
        rolreplication: false,
        rolbypassrls: false,
        rolconnlimit: -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_authid_desc_matches_expected_columns() {
        let desc = pg_authid_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
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

    #[test]
    fn bootstrap_pg_authid_rows_include_predefined_roles() {
        let rows = bootstrap_pg_authid_rows();
        assert!(
            rows.iter()
                .any(|row| row.rolname == BOOTSTRAP_SUPERUSER_NAME)
        );
        assert!(rows.iter().any(|row| row.rolname == "pg_database_owner"));
        assert!(rows.iter().any(|row| row.rolname == "pg_monitor"));
    }
}
