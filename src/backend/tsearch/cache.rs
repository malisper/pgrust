#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextSearchConfig {
    Simple,
    English,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextSearchDictionary {
    Simple,
    EnglishStem,
}

fn normalize_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name).trim()
}

pub(crate) fn resolve_config(config_name: Option<&str>) -> Result<TextSearchConfig, String> {
    let normalized = config_name
        .map(normalize_name)
        .unwrap_or("simple")
        .to_ascii_lowercase();
    match normalized.as_str() {
        "default" | "simple" => Ok(TextSearchConfig::Simple),
        "english" => Ok(TextSearchConfig::English),
        other => Err(format!("unknown text search configuration: {other}")),
    }
}

pub(crate) fn resolve_dictionary(name: &str) -> Result<TextSearchDictionary, String> {
    match normalize_name(name).to_ascii_lowercase().as_str() {
        "simple" => Ok(TextSearchDictionary::Simple),
        "english" | "english_stem" => Ok(TextSearchDictionary::EnglishStem),
        other => Err(format!("unknown text search dictionary: {other}")),
    }
}
