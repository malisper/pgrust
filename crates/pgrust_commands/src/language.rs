use pgrust_catalog_data::PgLanguageRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LanguageCommandError {
    Owner { name: String },
    Duplicate { name: String },
    Missing { name: String },
    MissingRole { name: String },
}

pub fn normalize_language_name(name: &str) -> String {
    name.trim_matches('"').to_ascii_lowercase()
}

pub fn language_owner_error(name: &str) -> LanguageCommandError {
    LanguageCommandError::Owner { name: name.into() }
}

pub fn language_duplicate_error(name: &str) -> LanguageCommandError {
    LanguageCommandError::Duplicate { name: name.into() }
}

pub fn language_missing_error(name: &str) -> LanguageCommandError {
    LanguageCommandError::Missing { name: name.into() }
}

pub fn language_missing_role_error(name: &str) -> LanguageCommandError {
    LanguageCommandError::MissingRole { name: name.into() }
}

pub fn find_language_by_name(rows: Vec<PgLanguageRow>, name: &str) -> Option<PgLanguageRow> {
    let normalized = normalize_language_name(name);
    rows.into_iter()
        .find(|row| row.lanname.eq_ignore_ascii_case(&normalized))
}

pub fn create_language_row(language_name: &str, owner_oid: u32, handler_oid: u32) -> PgLanguageRow {
    PgLanguageRow {
        oid: 0,
        lanname: normalize_language_name(language_name),
        lanowner: owner_oid,
        lanispl: true,
        lanpltrusted: true,
        lanplcallfoid: handler_oid,
        laninline: 0,
        lanvalidator: 0,
    }
}
