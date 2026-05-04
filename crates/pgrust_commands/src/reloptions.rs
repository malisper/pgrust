use pgrust_catalog_data::{
    BRIN_AM_OID, BRIN_DATETIME_MINMAX_MULTI_FAMILY_OID, BRIN_FLOAT_MINMAX_MULTI_FAMILY_OID,
    BRIN_INTEGER_MINMAX_MULTI_FAMILY_OID, BRIN_INTERVAL_MINMAX_MULTI_FAMILY_OID,
    BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID, BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID,
    BRIN_NETWORK_MINMAX_MULTI_FAMILY_OID, BRIN_NUMERIC_MINMAX_MULTI_FAMILY_OID,
    BRIN_OID_MINMAX_MULTI_FAMILY_OID, BRIN_PG_LSN_MINMAX_MULTI_FAMILY_OID,
    BRIN_TID_MINMAX_MULTI_FAMILY_OID, BRIN_TIME_MINMAX_MULTI_FAMILY_OID,
    BRIN_TIMETZ_MINMAX_MULTI_FAMILY_OID, BRIN_UUID_MINMAX_MULTI_FAMILY_OID, PgOpclassRow,
};
use pgrust_nodes::access::{
    BrinOptions, BtreeOptions, GinOptions, GistBufferingMode, GistOptions, HashOptions,
};
use pgrust_nodes::parsenodes::{IndexColumnDef, ParseError, RelOption};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelOptionError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

fn detailed_reloption_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> RelOptionError {
    RelOptionError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}

fn reloption_bounds_error(name: &str, value: &str, min: &str, max: &str) -> RelOptionError {
    detailed_reloption_error(
        format!("value {value} out of bounds for option \"{name}\""),
        Some(format!("Valid values are between \"{min}\" and \"{max}\".")),
        "22023",
    )
}

fn reloption_name(option: &str) -> String {
    option
        .split_once('=')
        .map(|(name, _)| name)
        .unwrap_or(option)
        .to_ascii_lowercase()
}

pub fn set_reloptions(current: Option<Vec<String>>, updates: &[String]) -> Option<Vec<String>> {
    let mut reloptions = current.unwrap_or_default();
    for update in updates {
        let name = reloption_name(update);
        reloptions.retain(|existing| reloption_name(existing) != name);
        reloptions.push(update.clone());
    }
    (!reloptions.is_empty()).then_some(reloptions)
}

pub fn reset_reloptions(
    current: Option<Vec<String>>,
    reset_options: &[String],
) -> Option<Vec<String>> {
    let reset = reset_options
        .iter()
        .map(|option| option.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let reloptions = current?
        .into_iter()
        .filter(|option| !reset.contains(&reloption_name(option)))
        .collect::<Vec<_>>();
    (!reloptions.is_empty()).then_some(reloptions)
}

pub fn set_view_reloptions(
    current: Option<Vec<String>>,
    options: &[RelOption],
) -> Result<Option<Vec<String>>, RelOptionError> {
    let mut reloptions = current.unwrap_or_default();
    for option in options {
        let normalized = normalize_view_reloption(option)?;
        let name = reloption_name(&normalized);
        reloptions.retain(|existing| reloption_name(existing) != name);
        reloptions.push(normalized);
    }
    Ok((!reloptions.is_empty()).then_some(reloptions))
}

pub fn reset_view_reloptions(
    current: Option<Vec<String>>,
    options: &[String],
) -> Result<Option<Vec<String>>, RelOptionError> {
    let resets = options
        .iter()
        .map(|option| normalize_view_reset_reloption(option))
        .collect::<Result<Vec<_>, RelOptionError>>()?;
    let reloptions = reset_reloptions(current, &resets).unwrap_or_default();
    if resets
        .iter()
        .any(|option| option.eq_ignore_ascii_case("check_option"))
    {
        return Ok(Some(reloptions));
    }
    Ok((!reloptions.is_empty()).then_some(reloptions))
}

fn normalize_view_reloption(option: &RelOption) -> Result<String, RelOptionError> {
    let name = option.name.to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "security_barrier" | "security_invoker" | "check_option"
    ) {
        return Err(detailed_reloption_error(
            format!("unrecognized parameter \"{}\"", option.name),
            None,
            "22023",
        ));
    }
    let value = if name == "check_option" {
        match option.value.to_ascii_lowercase().as_str() {
            "local" => "local",
            "cascaded" => "cascaded",
            _ => {
                return Err(detailed_reloption_error(
                    format!(
                        "invalid value for enum option \"check_option\": {}",
                        option.value
                    ),
                    Some("Valid values are \"local\" and \"cascaded\".".into()),
                    "22023",
                ));
            }
        }
    } else {
        match option.value.to_ascii_lowercase().as_str() {
            "true" | "on" => "true",
            "false" | "off" => "false",
            _ => {
                return Err(detailed_reloption_error(
                    format!(
                        "invalid value for boolean option \"{name}\": {}",
                        option.value
                    ),
                    None,
                    "22023",
                ));
            }
        }
    };
    Ok(format!("{name}={value}"))
}

fn normalize_view_reset_reloption(option: &str) -> Result<String, RelOptionError> {
    // PostgreSQL accepts RESET of reloptions that are not valid SET options for
    // views, such as autovacuum_enabled, because only the resulting reloptions
    // array is validated after matching entries are removed.
    Ok(option.to_ascii_lowercase())
}

pub fn is_brin_minmax_multi_opclass(opclass: &PgOpclassRow) -> bool {
    matches!(
        opclass.opcfamily,
        BRIN_INTEGER_MINMAX_MULTI_FAMILY_OID
            | BRIN_NUMERIC_MINMAX_MULTI_FAMILY_OID
            | BRIN_OID_MINMAX_MULTI_FAMILY_OID
            | BRIN_TID_MINMAX_MULTI_FAMILY_OID
            | BRIN_FLOAT_MINMAX_MULTI_FAMILY_OID
            | BRIN_TIME_MINMAX_MULTI_FAMILY_OID
            | BRIN_DATETIME_MINMAX_MULTI_FAMILY_OID
            | BRIN_TIMETZ_MINMAX_MULTI_FAMILY_OID
            | BRIN_INTERVAL_MINMAX_MULTI_FAMILY_OID
            | BRIN_UUID_MINMAX_MULTI_FAMILY_OID
            | BRIN_PG_LSN_MINMAX_MULTI_FAMILY_OID
            | BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID
            | BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID
            | BRIN_NETWORK_MINMAX_MULTI_FAMILY_OID
    )
}

pub fn validate_brin_minmax_multi_opclass_options(
    options: &[RelOption],
) -> Result<Vec<(String, String)>, RelOptionError> {
    let mut seen_values_per_range = false;
    let mut resolved = Vec::with_capacity(options.len());
    for option in options {
        if !option.name.eq_ignore_ascii_case("values_per_range") {
            return Err(detailed_reloption_error(
                format!("unrecognized parameter \"{}\"", option.name),
                None,
                "22023",
            ));
        }
        if seen_values_per_range {
            return Err(detailed_reloption_error(
                "parameter \"values_per_range\" specified more than once",
                None,
                "22023",
            ));
        }
        seen_values_per_range = true;
        let value = option.value.parse::<i32>().map_err(|_| {
            detailed_reloption_error(
                format!(
                    "invalid value for option \"values_per_range\": \"{}\"",
                    option.value
                ),
                None,
                "22023",
            )
        })?;
        if !(8..=256).contains(&value) {
            return Err(reloption_bounds_error(
                "values_per_range",
                &option.value,
                "8",
                "256",
            ));
        }
        resolved.push((option.name.clone(), option.value.clone()));
    }
    Ok(resolved)
}

pub fn resolve_index_opclass_options(
    access_method_oid: u32,
    opclass: &PgOpclassRow,
    column: &IndexColumnDef,
) -> Result<Vec<(String, String)>, RelOptionError> {
    if column.opclass_options.is_empty() {
        return Ok(Vec::new());
    }
    if access_method_oid == BRIN_AM_OID && is_brin_minmax_multi_opclass(opclass) {
        return validate_brin_minmax_multi_opclass_options(&column.opclass_options);
    }
    Ok(Vec::new())
}

pub fn resolve_brin_options(options: &[RelOption]) -> Result<BrinOptions, RelOptionError> {
    let mut resolved = BrinOptions::default();
    for option in options {
        if option.name.eq_ignore_ascii_case("pages_per_range") {
            let pages_per_range = option.value.parse::<u32>().map_err(|_| {
                RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "positive integer pages_per_range",
                    actual: option.value.clone(),
                })
            })?;
            if pages_per_range == 0 {
                return Err(RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "positive integer pages_per_range",
                    actual: option.value.clone(),
                }));
            }
            resolved.pages_per_range = pages_per_range;
            continue;
        }

        if option.name.eq_ignore_ascii_case("autosummarize") {
            return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
                "BRIN option \"autosummarize\"".into(),
            )));
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("BRIN option \"{}\"", option.name),
        )));
    }
    Ok(resolved)
}

pub fn resolve_btree_options(
    options: &[RelOption],
) -> Result<Option<BtreeOptions>, RelOptionError> {
    if options.is_empty() {
        return Ok(None);
    }

    let mut resolved = BtreeOptions::default();
    for option in options {
        if option.name.eq_ignore_ascii_case("fillfactor") {
            resolved.fillfactor = parse_index_fillfactor(option)?;
            continue;
        }

        if option.name.eq_ignore_ascii_case("deduplicate_items") {
            // :HACK: accepted for catalog compatibility; nbtree posting-list
            // deduplication still needs storage/executor support.
            resolved.deduplicate_items = match option.value.to_ascii_lowercase().as_str() {
                "on" | "true" | "yes" | "1" => true,
                "off" | "false" | "no" | "0" => false,
                _ => {
                    return Err(RelOptionError::Parse(ParseError::UnexpectedToken {
                        expected: "boolean deduplicate_items",
                        actual: option.value.clone(),
                    }));
                }
            };
            continue;
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("btree index option \"{}\"", option.name),
        )));
    }
    Ok(Some(resolved))
}

pub fn resolve_gin_options(options: &[RelOption]) -> Result<GinOptions, RelOptionError> {
    let mut resolved = GinOptions::default();
    for option in options {
        if option.name.eq_ignore_ascii_case("fastupdate") {
            resolved.fastupdate = match option.value.to_ascii_lowercase().as_str() {
                "on" | "true" | "yes" | "1" => true,
                "off" | "false" | "no" | "0" => false,
                _ => {
                    return Err(RelOptionError::Parse(ParseError::UnexpectedToken {
                        expected: "boolean fastupdate",
                        actual: option.value.clone(),
                    }));
                }
            };
            continue;
        }

        if option.name.eq_ignore_ascii_case("gin_pending_list_limit") {
            let pending_list_limit_kb = option.value.parse::<u32>().map_err(|_| {
                RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "positive integer gin_pending_list_limit",
                    actual: option.value.clone(),
                })
            })?;
            if pending_list_limit_kb == 0 {
                return Err(RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "positive integer gin_pending_list_limit",
                    actual: option.value.clone(),
                }));
            }
            resolved.pending_list_limit_kb = pending_list_limit_kb;
            continue;
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("GIN option \"{}\"", option.name),
        )));
    }
    Ok(resolved)
}

pub fn resolve_hash_options(options: &[RelOption]) -> Result<HashOptions, RelOptionError> {
    let mut resolved = HashOptions::default();
    for option in options {
        if option.name.eq_ignore_ascii_case("fillfactor") {
            let fillfactor = option.value.parse::<u16>().map_err(|_| {
                RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "integer fillfactor between 10 and 100",
                    actual: option.value.clone(),
                })
            })?;
            if !(10..=100).contains(&fillfactor) {
                return Err(RelOptionError::Parse(ParseError::UnexpectedToken {
                    expected: "integer fillfactor between 10 and 100",
                    actual: option.value.clone(),
                }));
            }
            resolved.fillfactor = fillfactor;
            continue;
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("hash index option \"{}\"", option.name),
        )));
    }
    Ok(resolved)
}

pub fn resolve_gist_options(options: &[RelOption]) -> Result<GistOptions, RelOptionError> {
    let mut resolved = GistOptions::default();
    for option in options {
        if option.name.eq_ignore_ascii_case("fillfactor") {
            resolved.fillfactor = parse_index_fillfactor(option)?;
            continue;
        }

        if option.name.eq_ignore_ascii_case("buffering") {
            resolved.buffering_mode = match option.value.to_ascii_lowercase().as_str() {
                "auto" => GistBufferingMode::Auto,
                "on" => GistBufferingMode::On,
                "off" => GistBufferingMode::Off,
                _ => {
                    return Err(detailed_reloption_error(
                        format!(
                            "invalid value for enum option \"buffering\": {}",
                            option.value
                        ),
                        Some("Valid values are \"on\", \"off\", and \"auto\".".into()),
                        "22023",
                    ));
                }
            };
            continue;
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("GiST option \"{}\"", option.name),
        )));
    }
    Ok(resolved)
}

pub fn resolve_spgist_options(options: &[RelOption]) -> Result<(), RelOptionError> {
    for option in options {
        if option.name.eq_ignore_ascii_case("fillfactor") {
            parse_index_fillfactor(option)?;
            continue;
        }

        return Err(RelOptionError::Parse(ParseError::FeatureNotSupported(
            format!("SP-GiST option \"{}\"", option.name),
        )));
    }
    Ok(())
}

pub fn parse_index_fillfactor(option: &RelOption) -> Result<u16, RelOptionError> {
    let fillfactor = option
        .value
        .parse::<u16>()
        .map_err(|_| invalid_fillfactor_error(&option.value))?;
    if !(10..=100).contains(&fillfactor) {
        return Err(invalid_fillfactor_error(&option.value));
    }
    Ok(fillfactor)
}

fn invalid_fillfactor_error(value: &str) -> RelOptionError {
    reloption_bounds_error("fillfactor", value, "10", "100")
}

pub fn index_reloptions(options: &[RelOption]) -> Option<Vec<String>> {
    (!options.is_empty()).then(|| {
        options
            .iter()
            .map(|option| format!("{}={}", option.name.to_ascii_lowercase(), option.value))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reloption(name: &str, value: &str) -> RelOption {
        RelOption {
            name: name.into(),
            value: value.into(),
        }
    }

    #[test]
    fn gist_fillfactor_and_buffering_are_resolved() {
        let options = [
            reloption("fillfactor", "75"),
            reloption("buffering", "auto"),
        ];
        let resolved = resolve_gist_options(&options).unwrap();
        assert_eq!(resolved.fillfactor, 75);
        assert_eq!(resolved.buffering_mode, GistBufferingMode::Auto);
    }

    #[test]
    fn btree_options_parse_fillfactor_and_deduplicate_items() {
        let options = [
            reloption("fillfactor", "75"),
            reloption("deduplicate_items", "off"),
        ];
        let resolved = resolve_btree_options(&options).unwrap().unwrap();
        assert_eq!(resolved.fillfactor, 75);
        assert!(!resolved.deduplicate_items);
        assert!(resolve_btree_options(&[]).unwrap().is_none());
    }

    #[test]
    fn brin_pages_per_range_is_resolved() {
        let resolved = resolve_brin_options(&[reloption("pages_per_range", "32")]).unwrap();
        assert_eq!(resolved.pages_per_range, 32);
    }

    #[test]
    fn fillfactor_rejects_out_of_range_values() {
        let err = parse_index_fillfactor(&reloption("fillfactor", "9")).unwrap_err();
        assert!(matches!(
            err,
            RelOptionError::Detailed {
                sqlstate: "22023",
                ..
            }
        ));
    }

    #[test]
    fn index_reloptions_lowercases_names() {
        let options = [reloption("FillFactor", "90")];
        assert_eq!(
            index_reloptions(&options),
            Some(vec!["fillfactor=90".into()])
        );
    }
}
