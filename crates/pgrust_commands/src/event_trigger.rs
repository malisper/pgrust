use std::collections::HashMap;

use pgrust_nodes::parsenodes::{AlterTableTriggerMode, CreateEventTriggerStatement};

pub const EVENT_TRIGGER_DISABLED: char = 'D';
pub const EVENT_TRIGGER_ENABLED_ORIGIN: char = 'O';
pub const EVENT_TRIGGER_ENABLED_REPLICA: char = 'R';
pub const EVENT_TRIGGER_ENABLED_ALWAYS: char = 'A';

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventTriggerError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn event_trigger_enabled_char(mode: AlterTableTriggerMode) -> char {
    match mode {
        AlterTableTriggerMode::Disable => EVENT_TRIGGER_DISABLED,
        AlterTableTriggerMode::EnableOrigin => EVENT_TRIGGER_ENABLED_ORIGIN,
        AlterTableTriggerMode::EnableReplica => EVENT_TRIGGER_ENABLED_REPLICA,
        AlterTableTriggerMode::EnableAlways => EVENT_TRIGGER_ENABLED_ALWAYS,
    }
}

pub fn event_triggers_guc_enabled(gucs: &HashMap<String, String>) -> bool {
    !gucs.get("event_triggers").is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "off" | "false" | "0" | "no"
        )
    })
}

pub fn validate_event_trigger_event(event_name: &str) -> Result<(), EventTriggerError> {
    match event_name.to_ascii_lowercase().as_str() {
        "ddl_command_start" | "ddl_command_end" | "sql_drop" | "login" | "table_rewrite" => Ok(()),
        _ => Err(detailed_error(
            format!("unrecognized event name \"{}\"", event_name),
            None,
            "42601",
        )),
    }
}

pub fn normalize_event_trigger_when_clauses(
    stmt: &CreateEventTriggerStatement,
) -> Result<Option<Vec<String>>, EventTriggerError> {
    let is_login_event = stmt.event_name.eq_ignore_ascii_case("login");
    let mut saw_tag = false;
    let mut tags = Vec::new();
    for clause in &stmt.when_clauses {
        if !clause.variable.eq_ignore_ascii_case("tag") {
            return Err(detailed_error(
                format!("unrecognized filter variable \"{}\"", clause.variable),
                None,
                "42601",
            ));
        }
        if saw_tag {
            return Err(detailed_error(
                "filter variable \"tag\" specified more than once",
                None,
                "42601",
            ));
        }
        saw_tag = true;
        for value in &clause.values {
            if !is_login_event {
                validate_event_trigger_tag(value)?;
            }
            tags.push(value.to_ascii_uppercase());
        }
    }
    if is_login_event && saw_tag {
        return Err(detailed_error(
            "tag filtering is not supported for login event triggers",
            None,
            "0A000",
        ));
    }
    if tags.is_empty() {
        Ok(None)
    } else {
        tags.sort();
        tags.dedup();
        Ok(Some(tags))
    }
}

fn validate_event_trigger_tag(tag: &str) -> Result<(), EventTriggerError> {
    let normalized = tag.to_ascii_uppercase();
    if matches!(
        normalized.as_str(),
        "CREATE EVENT TRIGGER"
            | "ALTER EVENT TRIGGER"
            | "DROP EVENT TRIGGER"
            | "CREATE DATABASE"
            | "DROP DATABASE"
            | "CREATE TABLESPACE"
            | "DROP TABLESPACE"
            | "CREATE ROLE"
            | "ALTER ROLE"
            | "DROP ROLE"
    ) {
        return Err(detailed_error(
            format!("event triggers are not supported for {}", tag),
            None,
            "0A000",
        ));
    }
    if !event_trigger_tag_is_known(&normalized) {
        return Err(detailed_error(
            format!(
                "filter value \"{}\" not recognized for filter variable \"tag\"",
                tag
            ),
            None,
            "42601",
        ));
    }
    Ok(())
}

fn event_trigger_tag_is_known(tag: &str) -> bool {
    matches!(
        tag,
        "ALTER DEFAULT PRIVILEGES"
            | "ALTER POLICY"
            | "ALTER TABLE"
            | "COMMENT"
            | "CREATE AGGREGATE"
            | "CREATE FOREIGN DATA WRAPPER"
            | "CREATE FUNCTION"
            | "CREATE INDEX"
            | "CREATE MATERIALIZED VIEW"
            | "CREATE OPERATOR CLASS"
            | "CREATE OPERATOR FAMILY"
            | "CREATE POLICY"
            | "CREATE PROCEDURE"
            | "CREATE SCHEMA"
            | "CREATE SERVER"
            | "CREATE TABLE"
            | "CREATE TYPE"
            | "CREATE USER MAPPING"
            | "CREATE VIEW"
            | "DROP AGGREGATE"
            | "DROP FUNCTION"
            | "DROP INDEX"
            | "DROP MATERIALIZED VIEW"
            | "DROP OWNED"
            | "DROP POLICY"
            | "DROP PROCEDURE"
            | "DROP ROUTINE"
            | "DROP SCHEMA"
            | "DROP TABLE"
            | "DROP VIEW"
            | "GRANT"
            | "REINDEX"
            | "REVOKE"
    )
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> EventTriggerError {
    EventTriggerError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
