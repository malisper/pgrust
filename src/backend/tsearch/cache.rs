use std::collections::BTreeMap;

use crate::backend::parser::CatalogLookup;
use crate::include::catalog::pg_ts_dict::{
    ENGLISH_STEM_TS_DICTIONARY_OID, SIMPLE_TS_DICTIONARY_OID,
};
use crate::include::catalog::pg_ts_template::{
    ISPELL_TS_TEMPLATE_OID, SIMPLE_TS_TEMPLATE_OID, SYNONYM_TS_TEMPLATE_OID,
    THESAURUS_TS_TEMPLATE_OID,
};
use crate::include::catalog::{PgTsConfigMapRow, PgTsDictRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TextSearchConfig {
    Simple,
    English,
    Custom {
        mappings: BTreeMap<i32, Vec<TextSearchDictionary>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TextSearchDictionary {
    Simple,
    EnglishStem,
    Ispell,
    Synonym { case_sensitive: bool },
    Thesaurus,
}

fn normalize_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name).trim()
}

fn unqualified_name(name: &str) -> String {
    normalize_name(name)
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim()
        .to_ascii_lowercase()
}

pub(crate) fn resolve_config(
    config_name: Option<&str>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TextSearchConfig, String> {
    let normalized = config_name
        .map(unqualified_name)
        .unwrap_or_else(|| "simple".into());
    match normalized.as_str() {
        "default" | "simple" => Ok(TextSearchConfig::Simple),
        "english" => Ok(TextSearchConfig::English),
        other => {
            let Some(catalog) = catalog else {
                return Err(format!("unknown text search configuration: {other}"));
            };
            let config = catalog
                .ts_config_rows()
                .into_iter()
                .find(|row| row.cfgname.eq_ignore_ascii_case(other))
                .ok_or_else(|| format!("unknown text search configuration: {other}"))?;
            let mut map_rows = catalog
                .ts_config_map_rows()
                .into_iter()
                .filter(|row| row.mapcfg == config.oid)
                .collect::<Vec<_>>();
            map_rows.sort_by_key(|row| (row.maptokentype, row.mapseqno));
            let mut mappings = BTreeMap::<i32, Vec<TextSearchDictionary>>::new();
            for row in map_rows {
                mappings
                    .entry(row.maptokentype)
                    .or_default()
                    .push(resolve_dictionary_by_oid(row.mapdict, catalog)?);
            }
            Ok(TextSearchConfig::Custom { mappings })
        }
    }
}

pub(crate) fn resolve_dictionary(
    name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TextSearchDictionary, String> {
    match unqualified_name(name).as_str() {
        "simple" => Ok(TextSearchDictionary::Simple),
        "english" | "english_stem" => Ok(TextSearchDictionary::EnglishStem),
        other => {
            let Some(catalog) = catalog else {
                return Err(format!("unknown text search dictionary: {other}"));
            };
            let row = catalog
                .ts_dict_rows()
                .into_iter()
                .find(|row| row.dictname.eq_ignore_ascii_case(other))
                .ok_or_else(|| format!("unknown text search dictionary: {other}"))?;
            dictionary_from_row(&row)
        }
    }
}

fn resolve_dictionary_by_oid(
    oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<TextSearchDictionary, String> {
    match oid {
        SIMPLE_TS_DICTIONARY_OID => Ok(TextSearchDictionary::Simple),
        ENGLISH_STEM_TS_DICTIONARY_OID => Ok(TextSearchDictionary::EnglishStem),
        other => {
            let row = catalog
                .ts_dict_rows()
                .into_iter()
                .find(|row| row.oid == other)
                .ok_or_else(|| format!("unknown text search dictionary oid: {other}"))?;
            dictionary_from_row(&row)
        }
    }
}

fn dictionary_from_row(row: &PgTsDictRow) -> Result<TextSearchDictionary, String> {
    match row.dicttemplate {
        SIMPLE_TS_TEMPLATE_OID => Ok(TextSearchDictionary::Simple),
        ISPELL_TS_TEMPLATE_OID => Ok(TextSearchDictionary::Ispell),
        SYNONYM_TS_TEMPLATE_OID => Ok(TextSearchDictionary::Synonym {
            case_sensitive: dict_option_bool(row.dictinitoption.as_deref(), "casesensitive")
                .unwrap_or(false),
        }),
        THESAURUS_TS_TEMPLATE_OID => Ok(TextSearchDictionary::Thesaurus),
        other => Err(format!("unsupported text search template oid: {other}")),
    }
}

fn dict_option_bool(options: Option<&str>, name: &str) -> Option<bool> {
    let options = options?;
    for item in options.split(',') {
        let Some((key, value)) = item.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case(name) {
            return parse_bool(value.trim().trim_matches('\''));
        }
    }
    None
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

pub(crate) fn dictionaries_for_asciiword(config: &TextSearchConfig) -> Vec<TextSearchDictionary> {
    match config {
        TextSearchConfig::Simple => vec![TextSearchDictionary::Simple],
        TextSearchConfig::English => vec![TextSearchDictionary::EnglishStem],
        TextSearchConfig::Custom { mappings } => mappings.get(&1).cloned().unwrap_or_default(),
    }
}

#[allow(dead_code)]
pub(crate) fn sort_map_rows(rows: &mut [PgTsConfigMapRow]) {
    rows.sort_by_key(|row| (row.mapcfg, row.maptokentype, row.mapseqno, row.mapdict));
}
