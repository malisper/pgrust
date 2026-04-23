use super::super::*;
use crate::backend::parser::ParseError;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::include::nodes::primnodes::ColumnDesc;
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
                    sqlstate: "42601",
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
    let mut expression_count = 0usize;
    for target in targets {
        let trimmed = target.trim();
        match classify_statistics_target(trimmed)? {
            StatisticsTarget::Column(column_name) => {
                if is_system_column_name(column_name) {
                    return Err(ExecError::DetailedError {
                        message: "statistics creation on system columns is not supported".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                let column = desc
                    .columns
                    .iter()
                    .find(|col| !col.dropped && col.name.eq_ignore_ascii_case(column_name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(column_name.to_string()))
                    })?;
                validate_statistics_column_type(column)?;
                if !seen_columns.insert(column_name.to_ascii_lowercase()) {
                    return Err(ExecError::DetailedError {
                        message: "duplicate column name in statistics definition".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42701",
                    });
                }
            }
            StatisticsTarget::Expression => {
                expression_count += 1;
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
    }

    if targets.len() < 2 && expression_count != 1 {
        return Err(ExecError::DetailedError {
            message: "extended statistics require at least 2 columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    Ok(())
}

enum StatisticsTarget<'a> {
    Column(&'a str),
    Expression,
}

fn classify_statistics_target(target: &str) -> Result<StatisticsTarget<'_>, ExecError> {
    let trimmed = target.trim();
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = trimmed[1..trimmed.len() - 1].trim();
        if has_unparenthesized_character(inner, ',') {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "statistics expression",
                actual: "syntax error at or near \",\"".into(),
            }));
        }
        inner
    } else {
        if let Some(token) = first_unparenthesized_statistics_operator(trimmed) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "statistics expression",
                actual: format!("syntax error at or near \"{token}\""),
            }));
        }
        trimmed
    };
    if inner.is_empty() {
        return Ok(StatisticsTarget::Expression);
    }
    if inner
        .chars()
        .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && inner
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
    {
        Ok(StatisticsTarget::Column(inner))
    } else {
        Ok(StatisticsTarget::Expression)
    }
}

fn first_unparenthesized_statistics_operator(target: &str) -> Option<&'static str> {
    let mut paren_depth = 0usize;
    let mut in_single_quote = false;
    let mut prev = '\0';
    for ch in target.chars() {
        if ch == '\'' && prev != '\\' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth = paren_depth.saturating_sub(1),
                '+' | '-' | '*' | '/' | ',' if paren_depth == 0 => {
                    return Some(match ch {
                        '+' => "+",
                        '-' => "-",
                        '*' => "*",
                        '/' => "/",
                        ',' => ",",
                        _ => unreachable!(),
                    });
                }
                _ => {}
            }
        }
        prev = ch;
    }
    None
}

fn has_unparenthesized_character(target: &str, needle: char) -> bool {
    let mut paren_depth = 0usize;
    let mut in_single_quote = false;
    let mut prev = '\0';
    for ch in target.chars() {
        if ch == '\'' && prev != '\\' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth = paren_depth.saturating_sub(1),
                _ if ch == needle && paren_depth == 0 => return true,
                _ => {}
            }
        }
        prev = ch;
    }
    false
}

fn validate_statistics_column_type(column: &ColumnDesc) -> Result<(), ExecError> {
    if matches!(
        column.sql_type.kind,
        crate::backend::parser::SqlTypeKind::Xid
    ) {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{}\" cannot be used in statistics because its type {} has no default btree operator class",
                column.name,
                crate::pgrust::database::ddl::format_sql_type_name(column.sql_type.clone())
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}
