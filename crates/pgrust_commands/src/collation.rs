use pgrust_catalog_data::PgCollationRow;
use pgrust_nodes::parsenodes::RelOption;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollationError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn create_collation_row_from_options(
    collname: String,
    collnamespace: u32,
    collowner: u32,
    options: &[RelOption],
) -> Result<PgCollationRow, CollationError> {
    let provider = collation_option_value(options, "provider");
    if !provider
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("builtin"))
    {
        return Err(detailed_error(
            "only builtin collation provider is supported",
            None,
            "0A000",
        ));
    }

    let Some(locale) = collation_option_value(options, "locale") else {
        return Err(detailed_error(
            "parameter \"locale\" must be specified",
            None,
            "42P17",
        ));
    };
    let (canonical_locale, collencoding) = validate_builtin_collation_locale(&locale)?;
    Ok(PgCollationRow {
        oid: 0,
        collname,
        collnamespace,
        collowner,
        collprovider: 'b',
        collisdeterministic: true,
        collencoding,
        collcollate: None,
        collctype: None,
        colllocale: Some(canonical_locale.into()),
        collicurules: None,
        collversion: Some("1".into()),
    })
}

pub fn collation_row_by_name_namespace(
    rows: &[PgCollationRow],
    namespace_oid: u32,
    object_name: &str,
) -> Option<PgCollationRow> {
    rows.iter()
        .find(|row| {
            row.collnamespace == namespace_oid && row.collname.eq_ignore_ascii_case(object_name)
        })
        .cloned()
}

pub fn split_schema_qualified_name(raw_name: &str) -> (Option<String>, String) {
    raw_name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.to_ascii_lowercase()), name.to_ascii_lowercase()))
        .unwrap_or_else(|| (None, raw_name.to_ascii_lowercase()))
}

fn collation_option_value(options: &[RelOption], name: &str) -> Option<String> {
    options
        .iter()
        .find(|option| option.name.eq_ignore_ascii_case(name))
        .map(|option| option.value.clone())
}

fn validate_builtin_collation_locale(locale: &str) -> Result<(&'static str, i32), CollationError> {
    match locale {
        "C" => Ok(("C", -1)),
        "C.UTF8" | "C.UTF-8" => Ok(("C.UTF-8", 6)),
        "PG_UNICODE_FAST" => Ok(("PG_UNICODE_FAST", 6)),
        _ => Err(detailed_error(
            format!("invalid locale name \"{}\" for builtin provider", locale),
            None,
            "42809",
        )),
    }
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> CollationError {
    CollationError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
