use std::collections::BTreeMap;
use std::path::Path;

use pgrust_nodes::parsenodes::RelOption;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TablespaceError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn normalize_tablespace_options(
    options: &[RelOption],
) -> Result<Option<Vec<String>>, TablespaceError> {
    if options.is_empty() {
        return Ok(None);
    }
    let mut normalized = Vec::with_capacity(options.len());
    for option in options {
        ensure_supported_tablespace_option(&option.name)?;
        normalized.push(format!("{}={}", option.name, option.value));
    }
    Ok(Some(normalized))
}

pub fn merge_tablespace_options(
    existing: Option<Vec<String>>,
    options: &[RelOption],
) -> Result<Option<Vec<String>>, TablespaceError> {
    let mut map = existing_tablespace_option_map(existing);
    for option in options {
        ensure_supported_tablespace_option(&option.name)?;
        map.insert(option.name.clone(), option.value.clone());
    }
    Ok(option_map_to_vec(map))
}

pub fn reset_tablespace_options(
    existing: Option<Vec<String>>,
    names: &[String],
) -> Result<Option<Vec<String>>, TablespaceError> {
    let mut map = existing_tablespace_option_map(existing);
    for name in names {
        ensure_supported_tablespace_option(name)?;
        map.remove(name);
    }
    Ok(option_map_to_vec(map))
}

pub fn normalize_tablespace_location(
    location: &str,
    allow_in_place_tablespaces: bool,
) -> Result<String, TablespaceError> {
    let trimmed = location.trim();
    if trimmed.contains('\'') {
        return Err(detailed_error(
            "tablespace location cannot contain single quotes",
            None,
            "42602",
        ));
    }
    if trimmed.is_empty() {
        if allow_in_place_tablespaces {
            return Ok(String::new());
        }
        return Err(detailed_error(
            "tablespace location must be an absolute path",
            None,
            "42P17",
        ));
    }

    let mut normalized = trimmed.to_string();
    while normalized.len() > 1 && normalized.ends_with(std::path::MAIN_SEPARATOR) {
        normalized.pop();
    }
    if !Path::new(&normalized).is_absolute() {
        return Err(detailed_error(
            "tablespace location must be an absolute path",
            None,
            "42P17",
        ));
    }
    Ok(normalized)
}

fn existing_tablespace_option_map(existing: Option<Vec<String>>) -> BTreeMap<String, String> {
    existing
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            let (name, value) = item.split_once('=')?;
            Some((name.to_string(), value.to_string()))
        })
        .collect()
}

fn option_map_to_vec(map: BTreeMap<String, String>) -> Option<Vec<String>> {
    let options = map
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    (!options.is_empty()).then_some(options)
}

fn ensure_supported_tablespace_option(name: &str) -> Result<(), TablespaceError> {
    if matches!(
        name.to_ascii_lowercase().as_str(),
        "random_page_cost" | "seq_page_cost" | "effective_io_concurrency"
    ) {
        return Ok(());
    }
    Err(detailed_error(
        format!("unrecognized parameter \"{name}\""),
        None,
        "22023",
    ))
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> TablespaceError {
    TablespaceError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
