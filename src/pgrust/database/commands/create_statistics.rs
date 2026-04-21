use super::super::*;
use crate::backend::parser::ParseError;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{is_system_column_name, relation_kind_name};

impl Database {
    pub(crate) fn execute_create_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &crate::backend::parser::CreateStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_statistics_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &crate::backend::parser::CreateStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation_name = normalize_statistics_from_clause(&create_stmt.from_clause)?;
        let relation = match catalog.lookup_any_relation(&relation_name) {
            Some(entry) if crate::include::catalog::relkind_is_analyzable(entry.relkind) => entry,
            Some(entry) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: relation_name.clone(),
                    expected: relation_kind_name(entry.relkind),
                }));
            }
            None => return Err(ExecError::Parse(ParseError::UnknownTable(relation_name))),
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for CREATE STATISTICS",
                actual: "system catalog".into(),
            }));
        }
        validate_statistics_kinds(&create_stmt.kinds)?;
        validate_statistics_targets(&relation.desc, &create_stmt.targets)?;
        Ok(StatementResult::AffectedRows(0))
    }
}

fn normalize_statistics_from_clause(from_clause: &str) -> Result<String, ExecError> {
    let input = from_clause.trim();
    if input.is_empty() {
        return Err(ExecError::Parse(ParseError::UnexpectedEof));
    }
    if input.contains(char::is_whitespace) || input.contains('(') {
        return Err(ExecError::DetailedError {
            message: "CREATE STATISTICS only supports relation names in the FROM clause".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(input.trim_matches('"').to_ascii_lowercase())
}

fn validate_statistics_kinds(kinds: &[String]) -> Result<(), ExecError> {
    for kind in kinds {
        match kind.as_str() {
            "ndistinct" | "dependencies" | "mcv" => {}
            other => {
                return Err(ExecError::DetailedError {
                    message: format!("unrecognized statistics kind \"{other}\""),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
        }
    }
    Ok(())
}

fn validate_statistics_targets(
    desc: &crate::backend::executor::RelationDesc,
    targets: &[String],
) -> Result<(), ExecError> {
    if targets.len() > 8 {
        return Err(ExecError::DetailedError {
            message: "cannot have more than 8 columns in statistics".into(),
            detail: None,
            hint: None,
            sqlstate: "54011",
        });
    }

    let mut seen_columns = std::collections::BTreeSet::new();
    let mut seen_exprs = std::collections::BTreeSet::new();
    for target in targets {
        let trimmed = target.trim();
        if let Some(column_name) = simple_statistics_column(trimmed) {
            if is_system_column_name(column_name) {
                return Err(ExecError::DetailedError {
                    message: "statistics creation on system columns is not supported".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            if !desc
                .columns
                .iter()
                .any(|col| !col.dropped && col.name.eq_ignore_ascii_case(column_name))
            {
                return Err(ExecError::Parse(ParseError::UnknownColumn(
                    column_name.to_string(),
                )));
            }
            if !seen_columns.insert(column_name.to_ascii_lowercase()) {
                return Err(ExecError::DetailedError {
                    message: "duplicate column name in statistics definition".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42701",
                });
            }
        } else {
            let normalized = trimmed.to_ascii_lowercase();
            if !seen_exprs.insert(normalized) {
                return Err(ExecError::DetailedError {
                    message: "duplicate expression in statistics definition".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42701",
                });
            }
        }
    }
    Ok(())
}

fn simple_statistics_column(target: &str) -> Option<&str> {
    let trimmed = target.trim();
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        trimmed[1..trimmed.len() - 1].trim()
    } else {
        trimmed
    };
    if inner.is_empty() {
        return None;
    }
    if inner
        .chars()
        .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && inner
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
    {
        Some(inner)
    } else {
        None
    }
}
