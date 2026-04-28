use super::super::*;
use crate::backend::parser::RelOption;
use crate::backend::utils::misc::notices::push_notice;
use crate::include::access::nbtree::BtreeOptions;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{ensure_relation_owner, lookup_index_relation_for_alter_index};

#[derive(Debug, Clone, Default)]
pub(super) struct TableReloptions {
    pub(super) heap: Option<Vec<String>>,
    pub(super) toast: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct TableReloptionResets {
    pub(super) heap: Vec<String>,
    pub(super) toast: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableReloptionTarget {
    Heap,
    Toast,
}

fn reloption_error(message: impl Into<String>, detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate: "22023",
    }
}

fn unrecognized_parameter(name: &str) -> ExecError {
    reloption_error(format!("unrecognized parameter \"{name}\""), None)
}

fn unrecognized_parameter_namespace(namespace: &str) -> ExecError {
    reloption_error(
        format!("unrecognized parameter namespace \"{namespace}\""),
        None,
    )
}

fn duplicate_parameter(name: &str) -> ExecError {
    reloption_error(
        format!("parameter \"{name}\" specified more than once"),
        None,
    )
}

fn invalid_bool_option(name: &str, value: &str) -> ExecError {
    reloption_error(
        format!("invalid value for boolean option \"{name}\": {value}"),
        None,
    )
}

fn invalid_int_option(name: &str, value: &str) -> ExecError {
    reloption_error(
        format!("invalid value for integer option \"{name}\": {value}"),
        None,
    )
}

fn invalid_real_option(name: &str, value: &str) -> ExecError {
    reloption_error(
        format!("invalid value for floating point option \"{name}\": {value}"),
        None,
    )
}

fn int_bounds_error(name: &str, value: &str, min: i64, max: i64) -> ExecError {
    reloption_error(
        format!("value {value} out of bounds for option \"{name}\""),
        Some(format!("Valid values are between \"{min}\" and \"{max}\".")),
    )
}

fn real_bounds_error(name: &str, value: &str, min: f64, max: f64) -> ExecError {
    reloption_error(
        format!("value {value} out of bounds for option \"{name}\""),
        Some(format!(
            "Valid values are between \"{min:.6}\" and \"{max:.6}\"."
        )),
    )
}

fn normalize_bool_option(name: &str, value: &str) -> Result<String, ExecError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok("true".into()),
        "false" | "off" | "no" | "0" => Ok("false".into()),
        _ => Err(invalid_bool_option(name, value)),
    }
}

fn normalize_int_option(name: &str, value: &str, min: i64, max: i64) -> Result<String, ExecError> {
    if let Ok(parsed) = value.parse::<i64>() {
        if (min..=max).contains(&parsed) {
            return Ok(value.to_string());
        }
        return Err(int_bounds_error(name, value, min, max));
    }
    if let Ok(parsed) = value.parse::<f64>()
        && (parsed < min as f64 || parsed > max as f64)
    {
        return Err(int_bounds_error(name, value, min, max));
    }
    Err(invalid_int_option(name, value))
}

fn normalize_real_option(name: &str, value: &str, min: f64, max: f64) -> Result<String, ExecError> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| invalid_real_option(name, value))?;
    if !(min..=max).contains(&parsed) {
        return Err(real_bounds_error(name, value, min, max));
    }
    Ok(value.to_string())
}

fn table_option_target_and_name(
    option_name: &str,
) -> Result<(TableReloptionTarget, String), ExecError> {
    if let Some((namespace, name)) = option_name.split_once('.') {
        if !namespace.eq_ignore_ascii_case("toast") {
            return Err(unrecognized_parameter_namespace(namespace));
        }
        return Ok((TableReloptionTarget::Toast, name.to_ascii_lowercase()));
    }
    Ok((TableReloptionTarget::Heap, option_name.to_ascii_lowercase()))
}

fn normalize_table_option(option: &RelOption) -> Result<(TableReloptionTarget, String), ExecError> {
    let (target, name) = table_option_target_and_name(&option.name)?;
    let value = match (target, name.as_str()) {
        (TableReloptionTarget::Heap, "fillfactor") => {
            normalize_int_option(&name, &option.value, 10, 100)?
        }
        (TableReloptionTarget::Heap, "autovacuum_enabled")
        | (TableReloptionTarget::Toast, "autovacuum_enabled")
        | (TableReloptionTarget::Heap, "vacuum_truncate")
        | (TableReloptionTarget::Toast, "vacuum_truncate") => {
            normalize_bool_option(&name, &option.value)?
        }
        (TableReloptionTarget::Heap, "autovacuum_analyze_scale_factor") => {
            normalize_real_option(&name, &option.value, 0.0, 100.0)?
        }
        (TableReloptionTarget::Heap, "autovacuum_vacuum_cost_delay")
        | (TableReloptionTarget::Toast, "autovacuum_vacuum_cost_delay") => {
            normalize_real_option(&name, &option.value, 0.0, 100.0)?
        }
        (TableReloptionTarget::Heap, "parallel_workers") => {
            normalize_int_option(&name, &option.value, 0, 1024)?
        }
        (TableReloptionTarget::Heap, "toast_tuple_target") => {
            normalize_int_option(&name, &option.value, 128, 8192)?
        }
        _ => return Err(unrecognized_parameter(&name)),
    };
    Ok((target, format!("{name}={value}")))
}

fn reloption_name(option: &str) -> String {
    option
        .split_once('=')
        .map(|(name, _)| name)
        .unwrap_or(option)
        .to_ascii_lowercase()
}

pub(super) fn normalize_create_table_reloptions(
    options: &[RelOption],
) -> Result<TableReloptions, ExecError> {
    let mut heap = Vec::new();
    let mut toast = Vec::new();
    let mut seen_heap = std::collections::BTreeSet::new();
    let mut seen_toast = std::collections::BTreeSet::new();
    for option in options {
        let (target, normalized) = normalize_table_option(option)?;
        let name = reloption_name(&normalized);
        let (seen, reloptions) = match target {
            TableReloptionTarget::Heap => (&mut seen_heap, &mut heap),
            TableReloptionTarget::Toast => (&mut seen_toast, &mut toast),
        };
        if !seen.insert(name.clone()) {
            return Err(duplicate_parameter(&name));
        }
        reloptions.push(normalized);
    }
    Ok(TableReloptions {
        heap: (!heap.is_empty()).then_some(heap),
        toast: (!toast.is_empty()).then_some(toast),
    })
}

pub(super) fn normalize_btree_reloptions(
    options: &[RelOption],
) -> Result<(Option<BtreeOptions>, Option<Vec<String>>), ExecError> {
    if options.is_empty() {
        return Ok((None, None));
    }

    let mut resolved = BtreeOptions::default();
    let mut reloptions = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for option in options {
        if let Some((namespace, _)) = option.name.split_once('.') {
            return Err(unrecognized_parameter_namespace(namespace));
        }
        let name = option.name.to_ascii_lowercase();
        if !seen.insert(name.clone()) {
            return Err(duplicate_parameter(&name));
        }
        match name.as_str() {
            "fillfactor" => {
                let value = normalize_int_option(&name, &option.value, 10, 100)?;
                resolved.fillfactor = value
                    .parse::<u16>()
                    .map_err(|_| invalid_int_option(&name, &option.value))?;
                reloptions.push(format!("{name}={value}"));
            }
            "deduplicate_items" => {
                let value = normalize_bool_option(&name, &option.value)?;
                resolved.deduplicate_items = value == "true";
                reloptions.push(format!("{name}={value}"));
            }
            _ => return Err(unrecognized_parameter(&name)),
        }
    }
    Ok((Some(resolved), Some(reloptions)))
}

pub(super) fn set_reloptions(
    current: Option<Vec<String>>,
    updates: &[String],
) -> Option<Vec<String>> {
    let mut reloptions = current.unwrap_or_default();
    for update in updates {
        let name = reloption_name(update);
        reloptions.retain(|existing| reloption_name(existing) != name);
        reloptions.push(update.clone());
    }
    (!reloptions.is_empty()).then_some(reloptions)
}

pub(super) fn reset_reloptions(
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

pub(super) fn split_table_reloption_resets(
    options: &[String],
) -> Result<TableReloptionResets, ExecError> {
    let mut resets = TableReloptionResets::default();
    for option in options {
        let (target, name) = table_option_target_and_name(option)?;
        match target {
            TableReloptionTarget::Heap => resets.heap.push(name),
            TableReloptionTarget::Toast => resets.toast.push(name),
        }
    }
    Ok(resets)
}

fn toast_reloptions_for_relation(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
) -> Option<Vec<String>> {
    relation
        .toast
        .as_ref()
        .and_then(|toast| catalog.class_row_by_oid(toast.relation_oid))
        .and_then(|row| row.reloptions)
}

impl Database {
    pub(crate) fn execute_alter_table_set_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_set_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_set_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        if relation.relkind == 'v' {
            return self.execute_alter_view_set_options_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
            );
        }
        if !matches!(relation.relkind, 'r' | 'p') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.table_name.clone(),
                expected: "table",
            }));
        }
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE SET options",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;

        let updates = normalize_create_table_reloptions(&alter_stmt.options)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        if let Some(heap_updates) = updates.heap.as_deref() {
            let current = catalog
                .class_row_by_oid(relation.relation_oid)
                .and_then(|row| row.reloptions);
            let reloptions = set_reloptions(current, heap_updates);
            if let Some(toast) = relation.toast.as_ref() {
                for update in heap_updates {
                    if let Some((name, value)) = update.split_once('=')
                        && name.eq_ignore_ascii_case("toast_tuple_target")
                        && let Ok(target) = value.parse::<usize>()
                    {
                        crate::backend::access::table::toast_helper::set_toast_tuple_target_for_toast_relation(
                            toast.relation_oid,
                            target,
                        );
                    }
                }
            }
            let effect = self
                .catalog
                .write()
                .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        if let (Some(toast_updates), Some(toast)) =
            (updates.toast.as_deref(), relation.toast.as_ref())
        {
            let current = toast_reloptions_for_relation(&catalog, &relation);
            let reloptions = set_reloptions(current, toast_updates);
            let effect = self
                .catalog
                .write()
                .alter_relation_reloptions_mvcc(toast.relation_oid, reloptions, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_index_set_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterIndexSetStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_index_relation_for_alter_index(
            &catalog,
            &alter_stmt.index_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_index_set_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_index_set_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterIndexSetStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_index_relation_for_alter_index(
            &catalog,
            &alter_stmt.index_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.index_name)?;
        let (_btree_options, updates) = normalize_btree_reloptions(&alter_stmt.options)?;
        let current = catalog
            .class_row_by_oid(relation.relation_oid)
            .and_then(|row| row.reloptions);
        let reloptions = set_reloptions(current, updates.as_deref().unwrap_or_default());
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
