mod ast;
mod cache;
mod compile;
mod exec;
mod gram;

use std::collections::{HashMap, HashSet};

use crate::backend::executor::{ExecError, ExecutorContext, StatementResult};
use crate::backend::parser::{
    Catalog, CatalogLookup, DoStatement, ParseError, SqlType, SqlTypeKind, parse_statement,
};

pub use ast::*;
pub use cache::PlpgsqlFunctionCache;
pub use compile::{CompiledFunction, TriggerTransitionTable};
pub use exec::{
    EventTriggerCallContext, EventTriggerDdlCommandRow, EventTriggerDroppedObjectRow,
    PlpgsqlNotice, TriggerCallContext, TriggerFunctionResult, TriggerOperation, clear_notices,
    current_event_trigger_ddl_commands, current_event_trigger_dropped_objects,
    current_event_trigger_table_rewrite, take_notices,
};
pub(crate) use exec::{
    execute_user_defined_event_trigger_function, execute_user_defined_procedure_values,
    execute_user_defined_scalar_function, execute_user_defined_scalar_function_values,
    execute_user_defined_scalar_function_values_with_arg_types,
    execute_user_defined_set_returning_function, execute_user_defined_trigger_function,
};
pub use gram::parse_block;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlValidationNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
}

pub(crate) fn validate_create_function_body(
    body: &str,
    has_output_args: bool,
) -> Result<(), ParseError> {
    validate_create_function_body_with_options(
        body,
        has_output_args,
        false,
        false,
        false,
        &[],
        &[],
        None,
    )
    .map(|_| ())
}

pub(crate) fn validate_create_function_body_with_options(
    body: &str,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
    arg_names: &[String],
    arg_types: &[(String, SqlType)],
    gucs: Option<&HashMap<String, String>>,
) -> Result<Vec<PlpgsqlValidationNotice>, ParseError> {
    let block = parse_block(body)?;
    validate_declared_cursor_arguments(&block)?;
    validate_raise_placeholders(&block)?;
    validate_return_statements(
        &block,
        has_output_args,
        returns_void,
        returns_set,
        allows_bare_return,
    )?;
    validate_get_diagnostics_targets(&block, arg_types)?;
    validate_static_sql(&block)?;
    let mut notices = Vec::new();
    validate_shadowed_variables(&block, arg_names, gucs, &mut notices)?;
    Ok(notices)
}

fn validate_get_diagnostics_targets(
    block: &Block,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    let hidden_names = block
        .declarations
        .iter()
        .map(|decl| match decl {
            Decl::Var(decl) => &decl.name,
            Decl::Cursor(decl) => &decl.name,
            Decl::Alias(decl) => &decl.name,
        })
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let visible_arg_types = arg_types
        .iter()
        .filter(|(name, _)| !hidden_names.contains(&name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    for stmt in &block.statements {
        validate_get_diagnostics_targets_in_stmt(stmt, &visible_arg_types)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_get_diagnostics_targets_in_stmt(stmt, &visible_arg_types)?;
        }
    }
    Ok(())
}

fn validate_get_diagnostics_targets_in_stmt(
    stmt: &Stmt,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_get_diagnostics_targets_in_stmt(stmt, arg_types),
        Stmt::GetDiagnostics { items, .. } => {
            for (target, _) in items {
                validate_get_diagnostics_target(target, arg_types)?;
            }
            Ok(())
        }
        Stmt::Block(block) => validate_get_diagnostics_targets(block, arg_types),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
                }
            }
            for stmt in else_branch {
                validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_get_diagnostics_target(
    target: &AssignTarget,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    let AssignTarget::Name(name) = target else {
        return Ok(());
    };
    let Some((_, ty)) = arg_types
        .iter()
        .find(|(arg_name, _)| arg_name.eq_ignore_ascii_case(name))
    else {
        return Ok(());
    };
    if matches!(ty.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
        return Err(ParseError::DetailedError {
            message: format!("\"{name}\" is not a scalar variable"),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_raise_placeholders(block: &Block) -> Result<(), ParseError> {
    for stmt in &block.statements {
        validate_raise_placeholders_in_stmt(stmt)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_raise_placeholders_in_stmt(stmt)?;
        }
    }
    Ok(())
}

fn validate_raise_placeholders_in_stmt(stmt: &Stmt) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_raise_placeholders_in_stmt(stmt),
        Stmt::Block(block) => validate_raise_placeholders(block),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_raise_placeholders_in_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_raise_placeholders_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_raise_placeholders_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::Raise {
            message: Some(message),
            params,
            ..
        } => {
            let placeholder_count = count_raise_placeholders(message);
            if placeholder_count < params.len() {
                return Err(raise_placeholder_error(
                    "too many parameters specified for RAISE",
                ));
            }
            if placeholder_count > params.len() {
                return Err(raise_placeholder_error(
                    "too few parameters specified for RAISE",
                ));
            }
            Ok(())
        }
        Stmt::Raise {
            message: None,
            params,
            ..
        } if !params.is_empty() => Err(raise_placeholder_error(
            "too many parameters specified for RAISE",
        )),
        _ => Ok(()),
    }
}

fn count_raise_placeholders(message: &str) -> usize {
    let mut count = 0usize;
    let mut chars = message.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            if chars.peek() == Some(&'%') {
                chars.next();
            } else {
                count += 1;
            }
        }
    }
    count
}

fn raise_placeholder_error(message: &str) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn validate_declared_cursor_arguments(block: &Block) -> Result<(), ParseError> {
    validate_declared_cursor_arguments_in_block(block, &mut Vec::new())
}

fn validate_declared_cursor_arguments_in_block(
    block: &Block,
    scopes: &mut Vec<HashMap<String, Vec<String>>>,
) -> Result<(), ParseError> {
    scopes.push(
        block
            .declarations
            .iter()
            .filter_map(|decl| match decl {
                Decl::Cursor(cursor) => Some((
                    cursor.name.to_ascii_lowercase(),
                    cursor
                        .params
                        .iter()
                        .map(|param| param.name.clone())
                        .collect(),
                )),
                _ => None,
            })
            .collect(),
    );
    for stmt in &block.statements {
        validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
        }
    }
    scopes.pop();
    Ok(())
}

fn validate_declared_cursor_arguments_in_stmt(
    stmt: &Stmt,
    scopes: &mut Vec<HashMap<String, Vec<String>>>,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_declared_cursor_arguments_in_stmt(stmt, scopes),
        Stmt::Block(block) => validate_declared_cursor_arguments_in_block(block, scopes),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
                }
            }
            for stmt in else_branch {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::ForQuery { source, body, .. } => {
            if let ForQuerySource::Cursor { name, args } = source {
                let params = visible_declared_cursor_params(name, scopes).ok_or_else(|| {
                    ParseError::DetailedError {
                        message: "cursor FOR loop must use a bound cursor variable".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    }
                })?;
                validate_cursor_arg_list(name, args, params)?;
            }
            for stmt in body {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::OpenCursor { name, source } => {
            if let OpenCursorSource::Declared { args } = source
                && let Some(params) = visible_declared_cursor_params(name, scopes)
            {
                validate_cursor_arg_list(name, args, params)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn visible_declared_cursor_params<'a>(
    name: &str,
    scopes: &'a [HashMap<String, Vec<String>>],
) -> Option<&'a [String]> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.get(&name.to_ascii_lowercase()))
        .map(Vec::as_slice)
}

fn validate_cursor_arg_list(
    cursor_name: &str,
    args: &[CursorArg],
    params: &[String],
) -> Result<(), ParseError> {
    let mut assigned = vec![false; params.len()];
    for (arg_index, arg) in args.iter().enumerate() {
        match arg {
            CursorArg::Positional(_) => {
                let Some(param_name) = params.get(arg_index) else {
                    return Err(cursor_arg_error(format!(
                        "too many arguments for cursor \"{cursor_name}\""
                    )));
                };
                if assigned[arg_index] {
                    return Err(duplicate_cursor_param_error(cursor_name, param_name));
                }
                assigned[arg_index] = true;
            }
            CursorArg::Named { name, .. } => {
                let Some(index) = params
                    .iter()
                    .position(|param| param.eq_ignore_ascii_case(name))
                else {
                    return Err(cursor_arg_error(format!(
                        "cursor \"{cursor_name}\" has no argument named \"{name}\""
                    )));
                };
                if assigned[index] {
                    return Err(duplicate_cursor_param_error(cursor_name, &params[index]));
                }
                assigned[index] = true;
            }
        }
    }
    if assigned.iter().any(|assigned| !assigned) {
        return Err(cursor_arg_error(format!(
            "not enough arguments for cursor \"{cursor_name}\""
        )));
    }
    Ok(())
}

fn duplicate_cursor_param_error(cursor_name: &str, param_name: &str) -> ParseError {
    cursor_arg_error(format!(
        "value for parameter \"{param_name}\" of cursor \"{cursor_name}\" specified more than once"
    ))
}

fn cursor_arg_error(message: String) -> ParseError {
    ParseError::DetailedError {
        message,
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn validate_return_statements(
    block: &Block,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
) -> Result<(), ParseError> {
    for stmt in &block.statements {
        validate_return_stmt_in_stmt(
            stmt,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        )?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_return_stmt_in_stmt(
                stmt,
                has_output_args,
                returns_void,
                returns_set,
                allows_bare_return,
            )?;
        }
    }
    Ok(())
}

fn validate_return_stmt_in_stmt(
    stmt: &Stmt,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_return_stmt_in_stmt(
            stmt,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        ),
        Stmt::Return { expr: Some(_), .. } if has_output_args => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function with OUT parameters".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        Stmt::Return { expr: Some(_), .. } if returns_void => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning void".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        Stmt::Return { expr: None, .. }
            if !has_output_args && !returns_void && !returns_set && !allows_bare_return =>
        {
            Err(ParseError::DetailedError {
                message: "missing expression at or near \";\"".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            })
        }
        Stmt::Block(block) => validate_return_statements(
            block,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        ),
        Stmt::Continue => Ok(()),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_return_stmt_in_stmt(
                        stmt,
                        has_output_args,
                        returns_void,
                        returns_set,
                        allows_bare_return,
                    )?;
                }
            }
            for stmt in else_branch {
                validate_return_stmt_in_stmt(
                    stmt,
                    has_output_args,
                    returns_void,
                    returns_set,
                    allows_bare_return,
                )?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_return_stmt_in_stmt(
                    stmt,
                    has_output_args,
                    returns_void,
                    returns_set,
                    allows_bare_return,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_static_sql(block: &Block) -> Result<(), ParseError> {
    for decl in &block.declarations {
        if let Decl::Cursor(cursor) = decl {
            validate_static_select_sql(&cursor.query)?;
        }
    }
    for stmt in &block.statements {
        validate_static_sql_in_stmt(stmt)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_static_sql_in_stmt(stmt)?;
        }
    }
    Ok(())
}

fn validate_static_sql_in_stmt(stmt: &Stmt) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_static_sql_in_stmt(stmt),
        Stmt::Block(block) => validate_static_sql(block),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_static_sql_in_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::ForQuery { source, body, .. } => {
            if let ForQuerySource::Static(sql) = source {
                validate_static_select_sql(sql)?;
            }
            for stmt in body {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::ReturnQuery { source } => {
            if let ForQuerySource::Static(sql) = source {
                validate_static_select_sql(sql)?;
            }
            Ok(())
        }
        Stmt::OpenCursor {
            source: OpenCursorSource::Static(sql),
            ..
        } => validate_static_select_sql(sql),
        Stmt::Perform { sql, .. } => validate_static_sql_text(&format!("select {sql}")),
        Stmt::ExecSql { sql } if should_validate_exec_sql(sql) => validate_static_sql_text(sql),
        _ => Ok(()),
    }
}

fn validate_static_select_sql(sql: &str) -> Result<(), ParseError> {
    validate_static_sql_text(sql)
}

fn should_validate_exec_sql(sql: &str) -> bool {
    let lowered = sql.to_ascii_lowercase();
    let words = lowered.split_whitespace().collect::<Vec<_>>();
    !sql.contains('$') && !words.iter().any(|word| *word == "into")
}

fn validate_static_sql_text(sql: &str) -> Result<(), ParseError> {
    if let Some(token) = malformed_select_alias_token(sql) {
        return Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: format!("syntax error at or near \"{token}\""),
        });
    }
    if should_defer_static_sql_validation(sql) {
        return Ok(());
    }
    match parse_statement(sql) {
        Ok(_) => Ok(()),
        Err(err) if is_static_sql_syntax_error(err.unpositioned()) => {
            Err(err.unpositioned().clone())
        }
        Err(_) => Ok(()),
    }
}

fn is_static_sql_syntax_error(err: &ParseError) -> bool {
    match err {
        ParseError::UnexpectedToken { actual, .. } => {
            actual.starts_with("syntax error at or near ")
        }
        ParseError::UnexpectedEof => true,
        _ => false,
    }
}

fn should_defer_static_sql_validation(sql: &str) -> bool {
    let Some(first_word) = sql.split_whitespace().next() else {
        return true;
    };
    if sql.contains(":=") || sql.contains('[') {
        return true;
    }
    matches!(
        first_word.to_ascii_lowercase().as_str(),
        "alter"
            | "call"
            | "close"
            | "comment"
            | "create"
            | "delete"
            | "drop"
            | "execute"
            | "fetch"
            | "insert"
            | "move"
            | "open"
            | "reset"
            | "select"
            | "set"
            | "truncate"
            | "update"
            | "values"
            | "with"
    )
}

fn malformed_select_alias_token(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select ") {
        return None;
    }
    let after_select = trimmed.get("select".len()..).unwrap_or_default();
    let after_select_lower = lower.get("select".len()..).unwrap_or_default();
    let select_list = after_select_lower
        .find(" from ")
        .and_then(|index| after_select.get(..index))
        .unwrap_or(after_select);
    for item in select_list.split(',') {
        let words = item.split_whitespace().take(3).collect::<Vec<_>>();
        if words.len() < 3 {
            continue;
        }
        if words.iter().all(|word| is_bare_identifier(word))
            && !words[1..].iter().any(|word| is_select_expr_keyword(word))
        {
            return Some(words[2].trim_matches('"').to_string());
        }
    }
    None
}

fn is_bare_identifier(word: &str) -> bool {
    let mut chars = word.chars();
    matches!(chars.next(), Some(ch) if ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_select_expr_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "and"
            | "as"
            | "between"
            | "case"
            | "collate"
            | "else"
            | "end"
            | "from"
            | "full"
            | "cross"
            | "inner"
            | "join"
            | "in"
            | "is"
            | "left"
            | "like"
            | "not"
            | "null"
            | "on"
            | "or"
            | "right"
            | "then"
            | "when"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationCheckLevel {
    Warning,
    Error,
}

fn validate_shadowed_variables(
    block: &Block,
    arg_names: &[String],
    gucs: Option<&HashMap<String, String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    let Some(level) = validation_extra_check_level(gucs, "shadowed_variables") else {
        return Ok(());
    };
    let mut scopes = vec![
        arg_names
            .iter()
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<std::collections::HashSet<_>>(),
    ];
    validate_shadowed_variables_in_block(block, level, &mut scopes, notices)
}

fn validate_shadowed_variables_in_block(
    block: &Block,
    level: ValidationCheckLevel,
    scopes: &mut Vec<std::collections::HashSet<String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    scopes.push(std::collections::HashSet::new());
    for decl in &block.declarations {
        match decl {
            Decl::Var(decl) => validate_decl_name_shadow(&decl.name, level, scopes, notices)?,
            Decl::Alias(decl) => validate_decl_name_shadow(&decl.name, level, scopes, notices)?,
            Decl::Cursor(decl) => {
                validate_decl_name_shadow(&decl.name, level, scopes, notices)?;
                for param in &decl.params {
                    validate_decl_name_shadow(&param.name, level, scopes, notices)?;
                }
            }
        }
    }
    for stmt in &block.statements {
        validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
        }
    }
    scopes.pop();
    Ok(())
}

fn validate_shadowed_variables_in_stmt(
    stmt: &Stmt,
    level: ValidationCheckLevel,
    scopes: &mut Vec<std::collections::HashSet<String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => {
            validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)
        }
        Stmt::Block(block) => validate_shadowed_variables_in_block(block, level, scopes, notices),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
                }
            }
            for stmt in else_branch {
                validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_decl_name_shadow(
    name: &str,
    level: ValidationCheckLevel,
    scopes: &mut [std::collections::HashSet<String>],
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    let normalized = name.to_ascii_lowercase();
    if scopes.iter().rev().any(|scope| scope.contains(&normalized)) {
        let message = format!("variable \"{name}\" shadows a previously defined variable");
        match level {
            ValidationCheckLevel::Warning => notices.push(PlpgsqlValidationNotice {
                severity: "WARNING",
                sqlstate: "01000",
                message,
            }),
            ValidationCheckLevel::Error => {
                return Err(ParseError::DetailedError {
                    message,
                    detail: None,
                    hint: None,
                    sqlstate: "42712",
                });
            }
        }
    }
    if let Some(scope) = scopes.last_mut() {
        scope.insert(normalized);
    }
    Ok(())
}

fn validation_extra_check_level(
    gucs: Option<&HashMap<String, String>>,
    check: &str,
) -> Option<ValidationCheckLevel> {
    let gucs = gucs?;
    if validation_extra_check_enabled(gucs.get("plpgsql.extra_errors"), check) {
        Some(ValidationCheckLevel::Error)
    } else if validation_extra_check_enabled(gucs.get("plpgsql.extra_warnings"), check) {
        Some(ValidationCheckLevel::Warning)
    } else {
        None
    }
}

fn validation_extra_check_enabled(value: Option<&String>, check: &str) -> bool {
    value.is_some_and(|value| {
        value.eq_ignore_ascii_case("all")
            || value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case(check))
    })
}

pub fn execute_do(stmt: &DoStatement) -> Result<StatementResult, ExecError> {
    let gucs = HashMap::new();
    execute_do_with_gucs(stmt, &gucs)
}

pub fn execute_do_with_gucs(
    stmt: &DoStatement,
    gucs: &HashMap<String, String>,
) -> Result<StatementResult, ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
        let language = stmt
            .language
            .as_deref()
            .unwrap_or("plpgsql")
            .to_ascii_lowercase();
        if language != "plpgsql" {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "LANGUAGE plpgsql",
                actual: format!("LANGUAGE {}", stmt.language.as_deref().unwrap_or("plpgsql")),
            }));
        }
        exec::clear_notices();
        let block = parse_block(&stmt.code)?;
        let compiled =
            compile::compile_do_block_with_gucs(&block, &Catalog::default(), Some(gucs))?;
        exec::execute_block_with_gucs(&compiled, gucs)
    })
}

pub(crate) fn execute_do_with_context(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_do_with_context_options(stmt, catalog, ctx, true)
}

pub(crate) fn execute_do_with_context_preserving_notices(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_do_with_context_options(stmt, catalog, ctx, false)
}

fn execute_do_with_context_options(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    clear_notice_queue: bool,
) -> Result<StatementResult, ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
        let language = stmt
            .language
            .as_deref()
            .unwrap_or("plpgsql")
            .to_ascii_lowercase();
        if language != "plpgsql" {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "LANGUAGE plpgsql",
                actual: format!("LANGUAGE {}", stmt.language.as_deref().unwrap_or("plpgsql")),
            }));
        }
        if clear_notice_queue {
            exec::clear_notices();
        }
        let block = parse_block(&stmt.code)?;
        let compiled = compile::compile_do_function(&block, catalog, Some(&ctx.gucs))?;
        exec::execute_do_function(&compiled, ctx)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;

    fn unline(stmt: &Stmt) -> &Stmt {
        match stmt {
            Stmt::WithLine { stmt, .. } => stmt,
            stmt => stmt,
        }
    }

    fn run_plpgsql_test<F>(name: &str, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new()
            .name(name.into())
            .stack_size(8 * 1024 * 1024)
            .spawn(f)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn execute_do_runs_core_control_flow() {
        run_plpgsql_test("execute_do_runs_core_control_flow", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare
                        total int4 := 0;
                    begin
                        total := total + 1;
                        if total > 0 then
                            raise notice 'value %', total;
                        elsif total < 0 then
                            raise warning 'bad';
                        else
                            null;
                        end if;
                        for i in 1..3 loop
                            total := total + i;
                        end loop;
                        if total = 7 then
                            raise notice 'done %', total;
                        else
                            raise exception 'wrong total %', total;
                        end if;
                    end
                "#
                .into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![
                    PlpgsqlNotice::new(RaiseLevel::Notice, "value 1"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "done 7"),
                ]
            );
        });
    }

    #[test]
    fn execute_do_runs_elsif_branch() {
        run_plpgsql_test("execute_do_runs_elsif_branch", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    begin
                        if 1 = 0 then
                            raise exception 'wrong if';
                        elsif 2 = 2 then
                            raise notice 'elsif';
                        else
                            raise exception 'wrong else';
                        end if;
                    end
                "#
                .into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "elsif")]
            );
        });
    }

    #[test]
    fn execute_do_rejects_non_plpgsql_language() {
        let stmt = DoStatement {
            language: Some("sql".into()),
            code: "begin null; end".into(),
        };
        let err = execute_do(&stmt).unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn execute_do_raise_info_queues_info_notice() {
        run_plpgsql_test("execute_do_raise_info_queues_info_notice", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare r boolean;
                    begin
                        execute $e$ select 2 !=-- comment
                          1 $e$ into r;
                        raise info 'r = %', r;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Info, "r = t")]
            );
        });
    }

    #[test]
    fn execute_do_raise_exception_surfaces_message() {
        run_plpgsql_test("execute_do_raise_exception_surfaces_message", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise exception 'boom %', 42; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::RaiseException(message) if message == "boom 42"
            ));
        });
    }

    #[test]
    fn execute_do_exception_block_handles_raise() {
        run_plpgsql_test("execute_do_exception_block_handles_raise", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    begin
                        begin
                            raise exception 'boom';
                        exception when others then
                            raise notice 'handled';
                        end;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
            );
        });
    }

    #[test]
    fn execute_do_handles_condition_raise_using_message() {
        run_plpgsql_test("execute_do_handles_condition_raise_using_message", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    begin
                        begin
                            raise reading_sql_data_not_permitted using message = 'round and round again';
                        exception when reading_sql_data_not_permitted then
                            raise notice 'handled';
                        end;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
            );
        });
    }

    #[test]
    fn execute_do_condition_raise_uses_condition_sqlstate() {
        run_plpgsql_test("execute_do_condition_raise_uses_condition_sqlstate", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise data_corrupted using message = 'bad rows'; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError { message, sqlstate: "XX001", .. } if message == "bad rows"
            ));
        });
    }

    #[test]
    fn execute_do_raise_sqlstate_uses_literal_message_and_handler() {
        run_plpgsql_test(
            "execute_do_raise_sqlstate_uses_literal_message_and_handler",
            || {
                let err = execute_do(&DoStatement {
                    language: None,
                    code: "begin raise exception sqlstate 'U9999'; end".into(),
                })
                .unwrap_err();
                assert!(matches!(
                    err,
                    ExecError::DetailedError { message, sqlstate: "U9999", .. } if message == "U9999"
                ));

                let stmt = DoStatement {
                    language: None,
                    code: r#"
                    begin
                        begin
                            raise exception sqlstate 'U9999';
                        exception when sqlstate 'U9999' then
                            raise notice 'handled';
                        end;
                    end
                "#
                    .into(),
                };

                assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
                assert_eq!(
                    take_notices(),
                    vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
                );
            },
        );
    }

    #[test]
    fn execute_do_assert_raises_assert_failure() {
        run_plpgsql_test("execute_do_assert_raises_assert_failure", || {
            let stmt = DoStatement {
                language: None,
                code: "begin assert false, 'bad assert'; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError { message, sqlstate: "P0004", .. } if message == "bad assert"
            ));
        });
    }

    #[test]
    fn execute_do_check_asserts_guc_disables_assert() {
        run_plpgsql_test("execute_do_check_asserts_guc_disables_assert", || {
            let stmt = DoStatement {
                language: None,
                code: "begin assert false, 'bad assert'; end".into(),
            };
            let mut gucs = std::collections::HashMap::new();
            gucs.insert("plpgsql.check_asserts".into(), "off".into());

            assert_eq!(
                execute_do_with_gucs(&stmt, &gucs).unwrap(),
                StatementResult::AffectedRows(0)
            );
        });
    }

    #[test]
    fn execute_do_raise_accepts_dollar_quoted_message() {
        run_plpgsql_test("execute_do_raise_accepts_dollar_quoted_message", || {
            let stmt = DoStatement {
                language: None,
                code: r#"begin raise exception $$Patchfield "%" does not exist$$, 'PF0_1'; end"#
                    .into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::RaiseException(message) if message == "Patchfield \"PF0_1\" does not exist"
            ));
        });
    }

    #[test]
    fn execute_do_accepts_top_level_end_semicolon() {
        run_plpgsql_test("execute_do_accepts_top_level_end_semicolon", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise notice 'done'; end;".into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "done")]
            );
        });
    }

    #[test]
    fn execute_do_accepts_raise_info() {
        run_plpgsql_test("execute_do_accepts_raise_info", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise info 'progress: %', 3; end".into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Info, "progress: 3")]
            );
        });
    }

    #[test]
    fn execute_do_raise_treats_double_percent_as_literal() {
        run_plpgsql_test("execute_do_raise_treats_double_percent_as_literal", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise notice 'done %%'; end".into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "done %")]
            );
        });
    }

    #[test]
    fn execute_do_raise_using_message_detail_and_errcode() {
        run_plpgsql_test("execute_do_raise_using_message_detail_and_errcode", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise using message = 'custom' || ' message', detail = 'extra', errcode = '22012'; end".into(),
            };

            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError {
                    message,
                    detail: Some(detail),
                    sqlstate: "22012",
                    ..
                } if message == "custom message" && detail == "extra"
            ));
        });
    }

    #[test]
    fn execute_do_runs_while_loop() {
        run_plpgsql_test("execute_do_runs_while_loop", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                        declare
                            o int;
                            a int[] := array[1,2,3,2,3,1,2];
                        begin
                            o := array_position(a, 2);
                            while o is not null
                            loop
                                raise notice '%', o;
                                o := array_position(a, 2, o + 1);
                            end loop;
                        end
                    "#
                .into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![
                    PlpgsqlNotice::new(RaiseLevel::Notice, "2"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "4"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "7"),
                ]
            );
        });
    }

    fn execute_do_assigns_array_subscript() {
        run_plpgsql_test("execute_do_assigns_array_subscript", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare
                        vals int4[] := array[1,2,3];
                    begin
                        vals[2] := 7;
                        raise notice '%', vals;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "{1,7,3}")]
            );
        });
    }

    #[test]
    fn execute_do_runs_continue_in_loop() {
        run_plpgsql_test("execute_do_runs_continue_in_loop", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare
                        total int4 := 0;
                    begin
                        for i in 1..4 loop
                            if i = 2 then
                                continue;
                            end if;
                            total := total + i;
                        end loop;
                        raise notice '%', total;
                    end
                "#
                .into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "8")]
            );
        });
    }

    #[test]
    fn parse_block_accepts_comments_in_declare_section() {
        let block = parse_block(
            r#"
                declare
                    n int4 := 1000;        -- sample count
                    c float8 := 1.94947;   /* critical value */
                begin
                    null;
                end
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 2);
        let Decl::Var(n_decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        let Decl::Var(c_decl) = &block.declarations[1] else {
            panic!("expected variable declaration");
        };
        assert_eq!(n_decl.name, "n");
        assert_eq!(n_decl.default_expr.as_deref(), Some("1000"));
        assert_eq!(c_decl.name, "c");
        assert_eq!(c_decl.default_expr.as_deref(), Some("1.94947"));
    }

    #[test]
    fn parse_block_accepts_top_level_end_semicolon() {
        let block = parse_block(
            r#"
                begin
                    null;
                end;
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 0);
        assert_eq!(block.statements.len(), 1);
    }

    #[test]
    fn parse_block_accepts_plpgsql_regression_syntax() {
        let block = parse_block(
            r#"
                declare
                    c refcursor;
                    cur cursor for select 1;
                    x int4 strict;
                begin
                    open cur;
                    fetch cur into x;
                    close cur;
                    get diagnostics x = row_count;
                    perform cast(1 as int4);
                    perform '{"a": 1}'::jsonb -> 'a';
                    perform f(a => 1);
                    savepoint s;
                end
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 3);
        assert!(matches!(
            &block.declarations[0],
            Decl::Var(decl) if decl.name == "c" && decl.type_name.eq_ignore_ascii_case("refcursor")
        ));
        assert!(matches!(
            &block.declarations[1],
            Decl::Cursor(decl) if decl.name == "cur"
        ));
        assert!(matches!(
            &block.declarations[2],
            Decl::Var(decl) if decl.name == "x" && decl.strict
        ));
        assert!(matches!(
            unline(&block.statements[0]),
            Stmt::OpenCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[1]),
            Stmt::FetchCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[2]),
            Stmt::CloseCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[3]),
            Stmt::GetDiagnostics { .. }
        ));
    }
}
