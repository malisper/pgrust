use crate::include::catalog::PgRewriteRow;

pub(crate) fn split_stored_rule_action_sql(ev_action: &str) -> Vec<&str> {
    if ev_action.is_empty() {
        Vec::new()
    } else {
        ev_action
            .split(";\n")
            .map(str::trim)
            .filter(|sql| !sql.is_empty())
            .collect()
    }
}

pub(crate) fn format_stored_rule_definition(rule: &PgRewriteRow, relation_name: &str) -> String {
    let mut definition = format!(
        "CREATE RULE {} AS ON {} TO {}",
        rule.rulename,
        format_rule_event(rule.ev_type),
        relation_name,
    );
    if !rule.ev_qual.is_empty() {
        definition.push_str(" WHERE ");
        definition.push_str(&rule.ev_qual);
    }
    definition.push_str(" DO ");
    definition.push_str(if rule.is_instead { "INSTEAD" } else { "ALSO" });

    let actions = split_stored_rule_action_sql(&rule.ev_action);
    if actions.is_empty() {
        definition.push_str(" NOTHING");
    } else if actions.len() == 1 {
        definition.push(' ');
        definition.push_str(actions[0]);
    } else {
        definition.push_str(" (\n");
        for (index, action) in actions.iter().enumerate() {
            definition.push_str("    ");
            definition.push_str(action);
            if index + 1 != actions.len() {
                definition.push_str(";\n");
            } else {
                definition.push('\n');
            }
        }
        definition.push(')');
    }

    definition
}

fn format_rule_event(ev_type: char) -> &'static str {
    match ev_type {
        '1' => "SELECT",
        '2' => "UPDATE",
        '3' => "INSERT",
        '4' => "DELETE",
        _ => "UNKNOWN",
    }
}
