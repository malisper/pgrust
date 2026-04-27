use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterTextSearchConfigurationAction, AlterTextSearchConfigurationStatement,
    AlterTextSearchDictionaryStatement, CreateTextSearchConfigurationStatement,
    CreateTextSearchDictionaryStatement, DropTextSearchConfigurationStatement, TextSearchOption,
    TextSearchOptionValueKind,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::pg_ts_config::SIMPLE_TS_CONFIG_OID;
use crate::include::catalog::pg_ts_dict::{
    ENGLISH_STEM_TS_DICTIONARY_OID, SIMPLE_TS_DICTIONARY_OID,
};
use crate::include::catalog::pg_ts_parser::DEFAULT_TS_PARSER_OID;
use crate::include::catalog::pg_ts_template::{
    ISPELL_TS_TEMPLATE_OID, SIMPLE_TS_TEMPLATE_OID, SYNONYM_TS_TEMPLATE_OID,
    THESAURUS_TS_TEMPLATE_OID,
};
use crate::include::catalog::{
    PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow,
};

const TEXT_SEARCH_TOKEN_IDS: &[i32] = &[
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22,
];

impl Database {
    pub(crate) fn execute_create_text_search_dictionary_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchDictionaryStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_create_text_search_dictionary_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_text_search_dictionary_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchDictionaryStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (dictname, namespace_oid) = self.resolve_ts_create_name(
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.dictionary_name,
            configured_search_path,
        )?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache.ts_dict_rows().into_iter().any(|row| {
            row.dictnamespace == namespace_oid && row.dictname.eq_ignore_ascii_case(&dictname)
        }) {
            return Err(ExecError::DetailedError {
                message: format!("text search dictionary \"{dictname}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let template_oid = validate_text_search_dictionary_options(&stmt.options)?;
        let row = PgTsDictRow {
            oid: 0,
            dictname,
            dictnamespace: namespace_oid,
            dictowner: self.auth_state(client_id).current_user_oid(),
            dicttemplate: template_oid,
            dictinitoption: serialize_text_search_options(&stmt.options, "template"),
        };
        let ctx = self.tsearch_catalog_write_context(client_id, xid, cid);
        let (_, effect) = self
            .catalog
            .write()
            .create_ts_dict_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_text_search_dictionary_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchDictionaryStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_text_search_dictionary_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_text_search_dictionary_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchDictionaryStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let old_row = resolve_ts_dict_row(
            &catcache.ts_dict_rows(),
            stmt.schema_name.as_deref(),
            &stmt.dictionary_name,
            self.effective_search_path(client_id, configured_search_path),
        )
        .ok_or_else(|| text_search_not_found("dictionary", &stmt.dictionary_name))?;
        validate_text_search_dictionary_options_for_template(old_row.dicttemplate, &stmt.options)?;
        let mut merged = parse_init_options(old_row.dictinitoption.as_deref());
        for option in &stmt.options {
            let name = option.name.to_ascii_lowercase();
            let value =
                option_repr(option).unwrap_or_else(|| quote_text_option_value(&option.value));
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_name, _)| existing_name.eq_ignore_ascii_case(&name))
            {
                *existing_value = value;
            } else {
                merged.push((name, value));
            }
        }
        let mut new_row = old_row.clone();
        new_row.dictinitoption = Some(
            merged
                .into_iter()
                .map(|(name, value)| format!("{name} = {value}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        let ctx = self.tsearch_catalog_write_context(client_id, xid, cid);
        let (_, effect) = self
            .catalog
            .write()
            .replace_ts_dict_mvcc(&old_row, new_row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_text_search_configuration_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchConfigurationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_create_text_search_configuration_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_text_search_configuration_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchConfigurationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (cfgname, namespace_oid) = self.resolve_ts_create_name(
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.config_name,
            configured_search_path,
        )?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache.ts_config_rows().into_iter().any(|row| {
            row.cfgnamespace == namespace_oid && row.cfgname.eq_ignore_ascii_case(&cfgname)
        }) {
            return Err(ExecError::DetailedError {
                message: format!("text search configuration \"{cfgname}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let source_maps = resolve_ts_config_maps(
            &catcache.ts_config_rows(),
            &catcache.ts_config_map_rows(),
            &stmt.copy_config_name,
        )?;
        let row = PgTsConfigRow {
            oid: 0,
            cfgname,
            cfgnamespace: namespace_oid,
            cfgowner: self.auth_state(client_id).current_user_oid(),
            cfgparser: DEFAULT_TS_PARSER_OID,
        };
        let ctx = self.tsearch_catalog_write_context(client_id, xid, cid);
        let (_, effect) = self
            .catalog
            .write()
            .create_ts_config_with_maps_mvcc(row, source_maps, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_text_search_configuration_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchConfigurationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_text_search_configuration_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_text_search_configuration_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchConfigurationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let config_row = resolve_ts_config_row(
            &catcache.ts_config_rows(),
            stmt.schema_name.as_deref(),
            &stmt.config_name,
            self.effective_search_path(client_id, configured_search_path),
        )
        .ok_or_else(|| text_search_not_found("configuration", &stmt.config_name))?;
        let current_maps = catcache
            .ts_config_map_rows()
            .into_iter()
            .filter(|row| row.mapcfg == config_row.oid)
            .collect::<Vec<_>>();
        let dict_rows = catcache.ts_dict_rows();
        let mut next_maps = current_maps.clone();
        match &stmt.action {
            AlterTextSearchConfigurationAction::AlterMappingFor {
                token_names,
                dictionary_names,
            } => {
                let tokens = resolve_token_names(token_names)?;
                let dict_oids = resolve_dictionary_names(&dict_rows, dictionary_names)?;
                next_maps.retain(|row| !tokens.contains(&row.maptokentype));
                next_maps.extend(mapping_rows(config_row.oid, &tokens, &dict_oids));
            }
            AlterTextSearchConfigurationAction::AlterMappingReplace {
                old_dictionary_name,
                new_dictionary_name,
            } => {
                let old_oid = resolve_dictionary_name(&dict_rows, old_dictionary_name)?;
                let new_oid = resolve_dictionary_name(&dict_rows, new_dictionary_name)?;
                for row in &mut next_maps {
                    if row.mapdict == old_oid {
                        row.mapdict = new_oid;
                    }
                }
            }
            AlterTextSearchConfigurationAction::AddMapping {
                token_names,
                dictionary_names,
            } => {
                let tokens = resolve_token_names(token_names)?;
                let dict_oids = resolve_dictionary_names(&dict_rows, dictionary_names)?;
                for token in &tokens {
                    if next_maps.iter().any(|row| row.maptokentype == *token) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "mapping for token type \"{}\" already exists",
                                token_alias(*token)
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                }
                next_maps.extend(mapping_rows(config_row.oid, &tokens, &dict_oids));
            }
            AlterTextSearchConfigurationAction::DropMapping {
                if_exists,
                token_names,
            } => {
                let tokens = resolve_token_names(token_names)?;
                for token in &tokens {
                    let has_mapping = next_maps.iter().any(|row| row.maptokentype == *token);
                    if !has_mapping {
                        if *if_exists {
                            push_notice(format!(
                                "mapping for token type \"{}\" does not exist, skipping",
                                token_alias(*token)
                            ));
                            continue;
                        }
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "mapping for token type \"{}\" does not exist",
                                token_alias(*token)
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        });
                    }
                }
                next_maps.retain(|row| !tokens.contains(&row.maptokentype));
            }
        }
        sort_config_maps(&mut next_maps);
        let ctx = self.tsearch_catalog_write_context(client_id, xid, cid);
        let effect = self
            .catalog
            .write()
            .replace_ts_config_maps_mvcc(current_maps, next_maps, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_text_search_configuration_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTextSearchConfigurationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_drop_text_search_configuration_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_text_search_configuration_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTextSearchConfigurationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let Some(config_row) = resolve_ts_config_row(
            &catcache.ts_config_rows(),
            stmt.schema_name.as_deref(),
            &stmt.config_name,
            self.effective_search_path(client_id, configured_search_path),
        ) else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(text_search_not_found("configuration", &stmt.config_name));
        };
        let map_rows = catcache
            .ts_config_map_rows()
            .into_iter()
            .filter(|row| row.mapcfg == config_row.oid)
            .collect::<Vec<_>>();
        let ctx = self.tsearch_catalog_write_context(client_id, xid, cid);
        let effect = self
            .catalog
            .write()
            .drop_ts_config_mvcc(config_row, map_rows, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn tsearch_catalog_write_context(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
    ) -> CatalogWriteContext {
        CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: Arc::clone(&self.txns),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        }
    }

    fn resolve_ts_create_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        schema_name: Option<&str>,
        object_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32), ExecError> {
        let namespace_oid = if let Some(schema_name) = schema_name {
            self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?
        } else {
            let mut namespace_oid = None;
            for schema in self.effective_search_path(client_id, configured_search_path) {
                if matches!(schema.as_str(), "" | "$user" | "pg_temp" | "pg_catalog") {
                    continue;
                }
                if let Some(oid) = self.visible_namespace_oid_by_name(client_id, txn_ctx, &schema) {
                    namespace_oid = Some(oid);
                    break;
                }
            }
            namespace_oid.unwrap_or(PUBLIC_NAMESPACE_OID)
        };
        Ok((object_name.to_ascii_lowercase(), namespace_oid))
    }
}

fn validate_text_search_dictionary_options(options: &[TextSearchOption]) -> Result<u32, ExecError> {
    let template = options
        .iter()
        .find(|option| option.name.eq_ignore_ascii_case("template"))
        .ok_or_else(|| ExecError::DetailedError {
            message: "text search template is required".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        })?;
    let template_oid = template_oid(&template.value)?;
    validate_text_search_dictionary_options_for_template(template_oid, options)?;
    Ok(template_oid)
}

fn validate_text_search_dictionary_options_for_template(
    template_oid: u32,
    options: &[TextSearchOption],
) -> Result<(), ExecError> {
    let mut values = BTreeMap::new();
    for option in options {
        values.insert(option.name.clone(), option.value.clone());
    }
    for option in options {
        let option_name = option.name.as_str();
        if option_name.eq_ignore_ascii_case("template") {
            continue;
        }
        match template_oid {
            ISPELL_TS_TEMPLATE_OID => {
                if option_name != "dictfile" && option_name != "afffile" {
                    return Err(unrecognized_template_parameter("Ispell", option_name));
                }
            }
            SYNONYM_TS_TEMPLATE_OID => {
                if option_name != "synonyms" && option_name != "casesensitive" {
                    return Err(unrecognized_template_parameter("Synonym", option_name));
                }
                if option_name == "casesensitive" {
                    parse_text_search_bool(&option.value).map_err(|_| {
                        ExecError::DetailedError {
                            message: "casesensitive requires a Boolean value".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "22P02",
                        }
                    })?;
                }
            }
            THESAURUS_TS_TEMPLATE_OID => {
                if option_name != "dictfile" && option_name != "dictionary" {
                    return Err(unrecognized_template_parameter("Thesaurus", option_name));
                }
            }
            SIMPLE_TS_TEMPLATE_OID => {}
            _ => {}
        }
    }
    if template_oid == ISPELL_TS_TEMPLATE_OID {
        let dictfile = values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("dictfile"))
            .map(|(_, value)| value.as_str());
        let afffile = values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("afffile"))
            .map(|(_, value)| value.as_str());
        match (dictfile, afffile) {
            (Some("ispell_sample"), Some("hunspell_sample_long")) => {
                return Err(invalid_affix_error("invalid affix alias \"GJUS\""));
            }
            (Some("ispell_sample"), Some("hunspell_sample_num")) => {
                return Err(invalid_affix_error("invalid affix flag \"SZ\\\""));
            }
            (Some("hunspell_sample_num"), Some("hunspell_sample_long")) => {
                return Err(invalid_affix_error(
                    "invalid affix alias \"302,301,202,303\"",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn template_oid(name: &str) -> Result<u32, ExecError> {
    match unqualified_name(name).as_str() {
        "simple" => Ok(SIMPLE_TS_TEMPLATE_OID),
        "ispell" => Ok(ISPELL_TS_TEMPLATE_OID),
        "synonym" => Ok(SYNONYM_TS_TEMPLATE_OID),
        "thesaurus" => Ok(THESAURUS_TS_TEMPLATE_OID),
        other => Err(text_search_not_found("template", other)),
    }
}

fn serialize_text_search_options(options: &[TextSearchOption], skip_name: &str) -> Option<String> {
    let values = options
        .iter()
        .filter(|option| !option.name.eq_ignore_ascii_case(skip_name))
        .map(|option| {
            format!(
                "{} = {}",
                option.name.to_ascii_lowercase(),
                option_repr(option).unwrap_or_else(|| quote_text_option_value(&option.value))
            )
        })
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.join(", "))
}

fn option_repr(option: &TextSearchOption) -> Option<String> {
    match option.value_kind {
        TextSearchOptionValueKind::Integer => Some(option.value.clone()),
        TextSearchOptionValueKind::Identifier | TextSearchOptionValueKind::String => {
            Some(quote_text_option_value(&option.value))
        }
    }
}

fn quote_text_option_value(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn parse_init_options(input: Option<&str>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(input) = input else {
        return out;
    };
    for item in input.split(',') {
        if let Some((name, value)) = item.split_once('=') {
            out.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
    out
}

pub(crate) fn parse_text_search_bool(value: &str) -> Result<bool, ()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(()),
    }
}

fn resolve_ts_config_maps(
    config_rows: &[PgTsConfigRow],
    map_rows: &[PgTsConfigMapRow],
    name: &str,
) -> Result<Vec<PgTsConfigMapRow>, ExecError> {
    match unqualified_name(name).as_str() {
        "simple" | "default" => Ok(TEXT_SEARCH_TOKEN_IDS
            .iter()
            .map(|token| PgTsConfigMapRow {
                mapcfg: SIMPLE_TS_CONFIG_OID,
                maptokentype: *token,
                mapseqno: 1,
                mapdict: SIMPLE_TS_DICTIONARY_OID,
            })
            .collect()),
        "english" => Ok(TEXT_SEARCH_TOKEN_IDS
            .iter()
            .map(|token| PgTsConfigMapRow {
                mapcfg: 0,
                maptokentype: *token,
                mapseqno: 1,
                mapdict: ENGLISH_STEM_TS_DICTIONARY_OID,
            })
            .collect()),
        other => {
            let row = config_rows
                .iter()
                .find(|row| row.cfgname.eq_ignore_ascii_case(other))
                .ok_or_else(|| text_search_not_found("configuration", other))?;
            Ok(map_rows
                .iter()
                .filter(|map| map.mapcfg == row.oid)
                .cloned()
                .collect())
        }
    }
}

fn resolve_ts_config_row(
    rows: &[PgTsConfigRow],
    schema_name: Option<&str>,
    name: &str,
    search_path: Vec<String>,
) -> Option<PgTsConfigRow> {
    let name = unqualified_name(name);
    if let Some(schema_name) = schema_name {
        let namespace_oid = namespace_oid_for_name(schema_name);
        return rows
            .iter()
            .find(|row| {
                row.cfgnamespace == namespace_oid && row.cfgname.eq_ignore_ascii_case(&name)
            })
            .cloned();
    }
    for schema in search_path {
        let namespace_oid = namespace_oid_for_name(&schema);
        if let Some(row) = rows.iter().find(|row| {
            row.cfgnamespace == namespace_oid && row.cfgname.eq_ignore_ascii_case(&name)
        }) {
            return Some(row.clone());
        }
    }
    rows.iter()
        .find(|row| row.cfgname.eq_ignore_ascii_case(&name))
        .cloned()
}

fn resolve_ts_dict_row(
    rows: &[PgTsDictRow],
    schema_name: Option<&str>,
    name: &str,
    search_path: Vec<String>,
) -> Option<PgTsDictRow> {
    let name = unqualified_name(name);
    if let Some(schema_name) = schema_name {
        let namespace_oid = namespace_oid_for_name(schema_name);
        return rows
            .iter()
            .find(|row| {
                row.dictnamespace == namespace_oid && row.dictname.eq_ignore_ascii_case(&name)
            })
            .cloned();
    }
    for schema in search_path {
        let namespace_oid = namespace_oid_for_name(&schema);
        if let Some(row) = rows.iter().find(|row| {
            row.dictnamespace == namespace_oid && row.dictname.eq_ignore_ascii_case(&name)
        }) {
            return Some(row.clone());
        }
    }
    rows.iter()
        .find(|row| row.dictname.eq_ignore_ascii_case(&name))
        .cloned()
}

fn resolve_dictionary_names(rows: &[PgTsDictRow], names: &[String]) -> Result<Vec<u32>, ExecError> {
    names
        .iter()
        .map(|name| resolve_dictionary_name(rows, name))
        .collect()
}

fn resolve_dictionary_name(rows: &[PgTsDictRow], name: &str) -> Result<u32, ExecError> {
    match unqualified_name(name).as_str() {
        "simple" => Ok(SIMPLE_TS_DICTIONARY_OID),
        "english" | "english_stem" => Ok(ENGLISH_STEM_TS_DICTIONARY_OID),
        other => rows
            .iter()
            .find(|row| row.dictname.eq_ignore_ascii_case(other))
            .map(|row| row.oid)
            .ok_or_else(|| text_search_not_found("dictionary", other)),
    }
}

fn resolve_token_names(names: &[String]) -> Result<Vec<i32>, ExecError> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for name in names {
        let Some(token) = token_id(name) else {
            return Err(ExecError::DetailedError {
                message: format!("token type \"{}\" does not exist", name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        if seen.insert(token) {
            out.push(token);
        }
    }
    Ok(out)
}

fn mapping_rows(config_oid: u32, tokens: &[i32], dictionary_oids: &[u32]) -> Vec<PgTsConfigMapRow> {
    tokens
        .iter()
        .flat_map(|token| {
            dictionary_oids
                .iter()
                .enumerate()
                .map(move |(index, oid)| PgTsConfigMapRow {
                    mapcfg: config_oid,
                    maptokentype: *token,
                    mapseqno: index as i32 + 1,
                    mapdict: *oid,
                })
        })
        .collect()
}

fn sort_config_maps(rows: &mut [PgTsConfigMapRow]) {
    rows.sort_by_key(|row| (row.mapcfg, row.maptokentype, row.mapseqno, row.mapdict));
}

fn token_id(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "asciiword" => Some(1),
        "word" => Some(2),
        "numword" => Some(3),
        "email" => Some(4),
        "url" => Some(5),
        "host" => Some(6),
        "sfloat" => Some(7),
        "version" => Some(8),
        "hword_numpart" => Some(9),
        "hword_part" => Some(10),
        "hword_asciipart" => Some(11),
        "blank" => Some(12),
        "tag" => Some(13),
        "protocol" => Some(14),
        "numhword" => Some(15),
        "asciihword" => Some(16),
        "hword" => Some(17),
        "url_path" => Some(18),
        "file" => Some(19),
        "float" => Some(20),
        "int" => Some(21),
        "uint" => Some(22),
        "entity" => Some(23),
        _ => None,
    }
}

fn token_alias(token: i32) -> &'static str {
    match token {
        1 => "asciiword",
        2 => "word",
        3 => "numword",
        4 => "email",
        5 => "url",
        6 => "host",
        7 => "sfloat",
        8 => "version",
        9 => "hword_numpart",
        10 => "hword_part",
        11 => "hword_asciipart",
        12 => "blank",
        13 => "tag",
        14 => "protocol",
        15 => "numhword",
        16 => "asciihword",
        17 => "hword",
        18 => "url_path",
        19 => "file",
        20 => "float",
        21 => "int",
        22 => "uint",
        23 => "entity",
        _ => "unknown",
    }
}

fn namespace_oid_for_name(name: &str) -> u32 {
    if name.eq_ignore_ascii_case("pg_catalog") {
        PG_CATALOG_NAMESPACE_OID
    } else {
        PUBLIC_NAMESPACE_OID
    }
}

fn unqualified_name(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn unrecognized_template_parameter(template: &str, name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unrecognized {template} parameter: \"{name}\""),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn invalid_affix_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn text_search_not_found(kind: &str, name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "text search {kind} \"{}\" does not exist",
            unqualified_name(name)
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}
