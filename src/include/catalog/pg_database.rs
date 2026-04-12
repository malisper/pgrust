use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, DEFAULT_TABLESPACE_OID};

pub const CURRENT_DATABASE_OID: u32 = 1;
pub const CURRENT_DATABASE_NAME: &str = "postgres";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgDatabaseRow {
    pub oid: u32,
    pub datname: String,
    pub datdba: u32,
    pub dattablespace: u32,
    pub datistemplate: bool,
    pub datallowconn: bool,
}

pub fn pg_database_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("datname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("datdba", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("dattablespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("datistemplate", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("datallowconn", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

pub fn bootstrap_pg_database_rows() -> [PgDatabaseRow; 1] {
    [
        // :HACK: pgrust is still a single-database server, so bootstrap a
        // single connectable database row instead of PostgreSQL's full
        // template/postgres initdb flow.
        PgDatabaseRow {
            oid: CURRENT_DATABASE_OID,
            datname: CURRENT_DATABASE_NAME.into(),
            datdba: BOOTSTRAP_SUPERUSER_OID,
            dattablespace: DEFAULT_TABLESPACE_OID,
            datistemplate: false,
            datallowconn: true,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_database_desc_matches_expected_columns() {
        let desc = pg_database_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "datname",
                "datdba",
                "dattablespace",
                "datistemplate",
                "datallowconn",
            ]
        );
    }
}
