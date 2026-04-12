use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::BOOTSTRAP_SUPERUSER_OID;

pub const PG_LANGUAGE_INTERNAL_OID: u32 = 12;
pub const PG_LANGUAGE_C_OID: u32 = 13;
pub const PG_LANGUAGE_SQL_OID: u32 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgLanguageRow {
    pub oid: u32,
    pub lanname: String,
    pub lanowner: u32,
    pub lanispl: bool,
    pub lanpltrusted: bool,
    pub lanplcallfoid: u32,
    pub laninline: u32,
    pub lanvalidator: u32,
}

pub fn pg_language_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("lanname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("lanowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("lanispl", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("lanpltrusted", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("lanplcallfoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("laninline", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("lanvalidator", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_language_rows() -> [PgLanguageRow; 3] {
    // :HACK: Keep handler and validator links at zero until pgrust exposes the
    // backing pg_proc rows for language support functions.
    [
        PgLanguageRow {
            oid: PG_LANGUAGE_INTERNAL_OID,
            lanname: "internal".into(),
            lanowner: BOOTSTRAP_SUPERUSER_OID,
            lanispl: false,
            lanpltrusted: false,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 0,
        },
        PgLanguageRow {
            oid: PG_LANGUAGE_C_OID,
            lanname: "c".into(),
            lanowner: BOOTSTRAP_SUPERUSER_OID,
            lanispl: false,
            lanpltrusted: false,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 0,
        },
        PgLanguageRow {
            oid: PG_LANGUAGE_SQL_OID,
            lanname: "sql".into(),
            lanowner: BOOTSTRAP_SUPERUSER_OID,
            lanispl: false,
            lanpltrusted: true,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 0,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_language_desc_matches_expected_columns() {
        let desc = pg_language_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "lanname",
                "lanowner",
                "lanispl",
                "lanpltrusted",
                "lanplcallfoid",
                "laninline",
                "lanvalidator",
            ]
        );
    }
}
