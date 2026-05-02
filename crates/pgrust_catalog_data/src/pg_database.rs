use crate::desc::column_desc;
use crate::{BOOTSTRAP_SUPERUSER_OID, DEFAULT_TABLESPACE_OID};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub const POSTGRES_DATABASE_OID: u32 = 1;
pub const POSTGRES_DATABASE_NAME: &str = "postgres";
pub const TEMPLATE0_DATABASE_OID: u32 = 2;
pub const TEMPLATE0_DATABASE_NAME: &str = "template0";
pub const TEMPLATE1_DATABASE_OID: u32 = 3;
pub const TEMPLATE1_DATABASE_NAME: &str = "template1";
pub const CURRENT_DATABASE_OID: u32 = POSTGRES_DATABASE_OID;
pub const CURRENT_DATABASE_NAME: &str = POSTGRES_DATABASE_NAME;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgDatabaseRow {
    pub oid: u32,
    pub datname: String,
    pub datdba: u32,
    pub encoding: i32,
    pub datlocprovider: char,
    pub dattablespace: u32,
    pub datistemplate: bool,
    pub datallowconn: bool,
    pub datconnlimit: i32,
    pub datcollate: String,
    pub datctype: String,
    pub datlocale: Option<String>,
    pub daticurules: Option<String>,
    pub datcollversion: Option<String>,
    pub datacl: Option<Vec<String>>,
    pub dathasloginevt: bool,
}

pub fn pg_database_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("datname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("datdba", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("encoding", SqlType::new(SqlTypeKind::Int4), false),
            column_desc(
                "datlocprovider",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("dattablespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("datistemplate", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("datallowconn", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("datconnlimit", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("datcollate", SqlType::new(SqlTypeKind::Text), false),
            column_desc("datctype", SqlType::new(SqlTypeKind::Text), false),
            column_desc("datlocale", SqlType::new(SqlTypeKind::Text), true),
            column_desc("daticurules", SqlType::new(SqlTypeKind::Text), true),
            column_desc("datcollversion", SqlType::new(SqlTypeKind::Text), true),
            column_desc(
                "datacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc("dathasloginevt", SqlType::new(SqlTypeKind::Bool), false),
        ],
    }
}

fn bootstrap_row(oid: u32, name: &str, datistemplate: bool, datallowconn: bool) -> PgDatabaseRow {
    PgDatabaseRow {
        oid,
        datname: name.into(),
        datdba: BOOTSTRAP_SUPERUSER_OID,
        encoding: 6,
        datlocprovider: 'c',
        dattablespace: DEFAULT_TABLESPACE_OID,
        datistemplate,
        datallowconn,
        datconnlimit: -1,
        datcollate: "C".into(),
        datctype: "C".into(),
        datlocale: None,
        daticurules: None,
        datcollversion: None,
        datacl: None,
        dathasloginevt: false,
    }
}

pub fn bootstrap_pg_database_rows() -> [PgDatabaseRow; 3] {
    [
        bootstrap_row(TEMPLATE0_DATABASE_OID, TEMPLATE0_DATABASE_NAME, true, false),
        bootstrap_row(TEMPLATE1_DATABASE_OID, TEMPLATE1_DATABASE_NAME, true, true),
        bootstrap_row(POSTGRES_DATABASE_OID, POSTGRES_DATABASE_NAME, false, true),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_database_desc_matches_expected_columns() {
        let desc = pg_database_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "datname",
                "datdba",
                "encoding",
                "datlocprovider",
                "dattablespace",
                "datistemplate",
                "datallowconn",
                "datconnlimit",
                "datcollate",
                "datctype",
                "datlocale",
                "daticurules",
                "datcollversion",
                "datacl",
                "dathasloginevt",
            ]
        );
    }

    #[test]
    fn bootstrap_pg_database_rows_include_templates_and_postgres() {
        let names: Vec<_> = bootstrap_pg_database_rows()
            .into_iter()
            .map(|row| row.datname)
            .collect();
        assert_eq!(names, vec!["template0", "template1", "postgres"]);
    }
}
