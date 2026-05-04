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
    validate_collation_options(options)?;
    let provider = collation_option_value(options, "provider");
    let locale = collation_option_value(options, "locale");
    let lc_collate = collation_option_value(options, "lc_collate");
    let lc_ctype = collation_option_value(options, "lc_ctype");

    if locale.is_some() && (lc_collate.is_some() || lc_ctype.is_some()) {
        return Err(detailed_error(
            "conflicting or redundant options",
            Some("LOCALE cannot be specified together with LC_COLLATE or LC_CTYPE.".into()),
            "42601",
        ));
    }
    if collation_option_value(options, "from").is_some() && options.len() > 1 {
        return Err(detailed_error(
            "conflicting or redundant options",
            Some("FROM cannot be specified together with any other options.".into()),
            "42601",
        ));
    }

    if provider
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("builtin"))
    {
        return Err(detailed_error(
            "only builtin collation provider is supported",
            None,
            "0A000",
        ));
    }

    if provider
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("builtin"))
        && locale.is_none()
    {
        return Err(detailed_error(
            "parameter \"locale\" must be specified",
            None,
            "42P17",
        ));
    }

    if let Some(lc_collate) = lc_collate {
        let lc_ctype = lc_ctype.unwrap_or_else(|| lc_collate.clone());
        if !matches!(lc_collate.as_str(), "C" | "POSIX")
            || !matches!(lc_ctype.as_str(), "C" | "POSIX")
        {
            return Err(detailed_error(
                format!("collation \"{collname}\" for encoding \"UTF8\" does not exist"),
                None,
                "42704",
            ));
        }
        return Ok(PgCollationRow {
            oid: 0,
            collname,
            collnamespace,
            collowner,
            collprovider: 'c',
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some(lc_collate),
            collctype: Some(lc_ctype),
            colllocale: None,
            collicurules: None,
            collversion: None,
        });
    }

    let Some(locale) = locale else {
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

fn validate_collation_options(options: &[RelOption]) -> Result<(), CollationError> {
    let mut seen = std::collections::BTreeSet::new();
    for option in options {
        if option.name.chars().any(|ch| ch.is_ascii_uppercase()) {
            return Err(detailed_error(
                format!("collation attribute \"{}\" not recognized", option.name),
                None,
                "42601",
            ));
        }
        let name = option.name.to_ascii_lowercase();
        if !matches!(
            name.as_str(),
            "provider"
                | "locale"
                | "lc_collate"
                | "lc_ctype"
                | "deterministic"
                | "version"
                | "from"
        ) {
            return Err(detailed_error(
                format!("collation attribute \"{}\" not recognized", option.name),
                None,
                "42601",
            ));
        }
        if !seen.insert(name) {
            return Err(detailed_error(
                "conflicting or redundant options",
                None,
                "42601",
            ));
        }
    }
    Ok(())
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
