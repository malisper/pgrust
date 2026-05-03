mod ast;
mod gram;

use std::collections::{HashMap, HashSet};

use pgrust_nodes::{SqlType, SqlTypeKind};
use pgrust_parser::{ParseError, parse_statement};

pub use ast::*;
pub use gram::parse_block;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlValidationNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
}

pub fn validate_create_function_body(body: &str, has_output_args: bool) -> Result<(), ParseError> {
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

pub fn validate_create_function_body_with_options(
    body: &str,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
    arg_names: &[String],
    arg_types: &[(String, SqlType)],
    gucs: Option<&HashMap<String, String>>,
) -> Result<Vec<PlpgsqlValidationNotice>, ParseError> {
    let block = crate::parse_block(body)?;
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
        Stmt::Continue { .. } => Ok(()),
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
