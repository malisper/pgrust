#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseCommandError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn option_non_default(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .filter(|value| !value.eq_ignore_ascii_case("default"))
}

pub fn database_encoding_code(encoding: &str) -> Result<i32, DatabaseCommandError> {
    match encoding.to_ascii_lowercase().replace('-', "_").as_str() {
        "utf8" | "unicode" => Ok(6),
        "sql_ascii" => Ok(0),
        _ => Err(DatabaseCommandError::Detailed {
            message: format!("{} is not a valid encoding name", encoding),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}
