use crate::backend::parser::CatalogLookup;
use crate::include::catalog::PgTriggerRow;

const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FormattedTriggerDefinition {
    pub definition: String,
    pub event_manipulations: Vec<&'static str>,
    pub action_condition: Option<String>,
    pub action_statement: String,
    pub action_orientation: &'static str,
    pub action_timing: &'static str,
    pub action_reference_old_table: Option<String>,
    pub action_reference_new_table: Option<String>,
}

pub(crate) fn format_trigger_definition(
    catalog: &dyn CatalogLookup,
    row: &PgTriggerRow,
    pretty: bool,
) -> Option<FormattedTriggerDefinition> {
    let relation = catalog
        .relation_by_oid(row.tgrelid)
        .or_else(|| catalog.lookup_relation_by_oid(row.tgrelid))?;
    let class_row = catalog.class_row_by_oid(row.tgrelid)?;
    let schema_name = catalog
        .namespace_row_by_oid(class_row.relnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "public".into());
    let proc_row = catalog.proc_row_by_oid(row.tgfoid)?;
    let relation_name = if pretty {
        class_row.relname
    } else {
        format!("{}.{}", schema_name, class_row.relname)
    };
    let event_manipulations = trigger_event_manipulations(row.tgtype);
    let event_list = trigger_event_list(row, &relation.desc.columns);
    let action_timing = trigger_timing(row.tgtype);
    let action_orientation = if (row.tgtype & TRIGGER_TYPE_ROW) != 0 {
        "ROW"
    } else {
        "STATEMENT"
    };
    let referencing_clause = format_referencing_clause(row);
    let action_statement = format_trigger_function_call(&proc_row.proname, &row.tgargs);
    let when_clause = row
        .tgqual
        .as_deref()
        .map(|when_sql| format_when_clause(when_sql, pretty))
        .unwrap_or_default();
    let definition = format!(
        "CREATE TRIGGER {} {} {} ON {}{} FOR EACH {}{} EXECUTE FUNCTION {}",
        row.tgname,
        action_timing,
        event_list,
        relation_name,
        referencing_clause,
        action_orientation,
        when_clause,
        action_statement,
    );
    Some(FormattedTriggerDefinition {
        definition,
        event_manipulations,
        action_condition: row.tgqual.as_deref().map(format_action_condition),
        action_statement,
        action_orientation,
        action_timing,
        action_reference_old_table: row.tgoldtable.clone(),
        action_reference_new_table: row.tgnewtable.clone(),
    })
}

fn format_referencing_clause(row: &PgTriggerRow) -> String {
    let mut parts = Vec::new();
    if let Some(name) = row.tgoldtable.as_deref() {
        parts.push(format!("OLD TABLE AS {name}"));
    }
    if let Some(name) = row.tgnewtable.as_deref() {
        parts.push(format!("NEW TABLE AS {name}"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" REFERENCING {}", parts.join(" "))
    }
}

fn trigger_event_manipulations(tgtype: i16) -> Vec<&'static str> {
    let mut events = Vec::new();
    if (tgtype & TRIGGER_TYPE_INSERT) != 0 {
        events.push("INSERT");
    }
    if (tgtype & TRIGGER_TYPE_UPDATE) != 0 {
        events.push("UPDATE");
    }
    if (tgtype & TRIGGER_TYPE_DELETE) != 0 {
        events.push("DELETE");
    }
    if (tgtype & TRIGGER_TYPE_TRUNCATE) != 0 {
        events.push("TRUNCATE");
    }
    events
}

fn trigger_event_list(
    row: &PgTriggerRow,
    columns: &[crate::backend::executor::ColumnDesc],
) -> String {
    let mut events = Vec::new();
    if (row.tgtype & TRIGGER_TYPE_INSERT) != 0 {
        events.push("INSERT".to_string());
    }
    if (row.tgtype & TRIGGER_TYPE_UPDATE) != 0 {
        if row.tgattr.is_empty() {
            events.push("UPDATE".to_string());
        } else {
            let update_columns = row
                .tgattr
                .iter()
                .filter_map(|attnum| {
                    usize::try_from(attnum.saturating_sub(1))
                        .ok()
                        .and_then(|index| columns.get(index))
                        .map(|column| column.name.clone())
                })
                .collect::<Vec<_>>();
            if update_columns.is_empty() {
                events.push("UPDATE".to_string());
            } else {
                events.push(format!("UPDATE OF {}", update_columns.join(", ")));
            }
        }
    }
    if (row.tgtype & TRIGGER_TYPE_DELETE) != 0 {
        events.push("DELETE".to_string());
    }
    if (row.tgtype & TRIGGER_TYPE_TRUNCATE) != 0 {
        events.push("TRUNCATE".to_string());
    }
    events.join(" OR ")
}

fn trigger_timing(tgtype: i16) -> &'static str {
    if (tgtype & TRIGGER_TYPE_INSTEAD) != 0 {
        "INSTEAD OF"
    } else if (tgtype & TRIGGER_TYPE_BEFORE) != 0 {
        "BEFORE"
    } else {
        "AFTER"
    }
}

fn format_when_clause(when_sql: &str, pretty: bool) -> String {
    let normalized = normalize_trigger_record_refs(when_sql.trim());
    if pretty || is_simple_when_expr(&normalized) {
        format!(" WHEN ({normalized})")
    } else {
        format!(" WHEN (({normalized}))")
    }
}

fn format_action_condition(when_sql: &str) -> String {
    let normalized = normalize_trigger_record_refs(when_sql.trim());
    if is_simple_when_expr(&normalized) {
        normalized
    } else {
        format!("({normalized})")
    }
}

fn is_simple_when_expr(expr: &str) -> bool {
    matches!(
        expr.to_ascii_lowercase().as_str(),
        "true" | "false" | "null"
    )
}

fn normalize_trigger_record_refs(when_sql: &str) -> String {
    [
        ("OLD.*", "old.*"),
        ("NEW.*", "new.*"),
        ("OLD.", "old."),
        ("NEW.", "new."),
    ]
    .into_iter()
    .fold(when_sql.to_string(), |sql, (needle, replacement)| {
        replace_case_insensitive(&sql, needle, replacement)
    })
}

fn replace_case_insensitive(haystack: &str, needle: &str, replacement: &str) -> String {
    let haystack_lower = haystack.to_ascii_lowercase();
    let needle_lower = needle.to_ascii_lowercase();
    let mut normalized = String::with_capacity(haystack.len());
    let mut search_from = 0usize;

    while let Some(relative) = haystack_lower[search_from..].find(&needle_lower) {
        let index = search_from + relative;
        normalized.push_str(&haystack[search_from..index]);
        normalized.push_str(replacement);
        search_from = index + needle.len();
    }

    normalized.push_str(&haystack[search_from..]);
    normalized
}

fn format_trigger_function_call(function_name: &str, args: &[String]) -> String {
    let args = args
        .iter()
        .map(|arg| {
            if arg.chars().all(|ch| ch.is_ascii_digit()) {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "''"))
            }
        })
        .collect::<Vec<_>>();
    format!("{function_name}({})", args.join(", "))
}
