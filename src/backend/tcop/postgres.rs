use std::collections::HashMap;
use std::io::{self, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::{ExecError, QueryColumn, StatementResult};
use crate::backend::libpq::pqcomm::{
    cstr_from_bytes, read_byte, read_cstr, read_i16_bytes, read_i32, read_i32_bytes,
};
use crate::backend::libpq::pqformat::{
    FloatFormatOptions, format_bytea_text, format_exec_error, format_exec_error_hint,
    infer_command_tag, send_auth_ok, send_backend_key_data, send_bind_complete,
    send_close_complete, send_command_complete, send_copy_in_response, send_empty_query,
    send_error, send_error_with_fields, send_error_with_hint, send_no_data, send_notice,
    send_notice_with_severity, send_notification_response, send_parameter_description,
    send_parameter_status, send_parse_complete, send_portal_suspended, send_query_result,
    send_ready_for_query, send_row_description, send_row_description_with_formats,
    send_typed_data_row, validate_binary_result_formats,
};
use crate::backend::parser::UngroupedColumnClause;
use crate::backend::parser::comments::sql_is_effectively_empty_after_comments;
use crate::backend::parser::{CatalogLookup, SelectStatement, Statement};
use crate::backend::parser::{SqlType, SqlTypeKind, parse_expr};
use crate::backend::rewrite::format_view_definition;
use crate::backend::utils::misc::guc_datetime::{DateTimeConfig, format_datestyle};
use crate::backend::utils::misc::notices::{
    clear_notices as clear_backend_notices, take_notices as take_backend_notices,
};
use crate::backend::utils::misc::stack_depth::StackDepthGuard;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::access::htup::TupleError;
use crate::include::catalog::RECORD_TYPE_OID;
use crate::include::nodes::datetime::{DateADT, TimeADT, TimeTzADT, TimestampADT, TimestampTzADT};
use crate::include::nodes::datum::{
    ArrayDimension, ArrayValue, RecordDescriptor, RecordValue, Value,
};
use crate::include::nodes::primnodes::RelationDesc;
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::database::ddl::format_sql_type_name;
use crate::pl::plpgsql::{PlpgsqlNotice, RaiseLevel, clear_notices, take_notices};

fn exec_error_sqlstate(e: &ExecError) -> &'static str {
    match e {
        ExecError::WithContext { source, .. } => exec_error_sqlstate(source),
        ExecError::Regex(err) => err.sqlstate,
        ExecError::JsonInput { sqlstate, .. } => sqlstate,
        ExecError::XmlInput { sqlstate, .. } => sqlstate,
        ExecError::DetailedError { sqlstate, .. } => sqlstate,
        ExecError::Parse(crate::backend::parser::ParseError::InvalidInteger(_))
        | ExecError::Parse(crate::backend::parser::ParseError::InvalidNumeric(_))
        | ExecError::InvalidIntegerInput { .. }
        | ExecError::ArrayInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidGeometryInput { .. }
        | ExecError::InvalidBitInput { .. }
        | ExecError::InvalidBooleanInput { .. }
        | ExecError::InvalidFloatInput { .. } => "22P02",
        ExecError::InvalidByteaHexDigit { .. } | ExecError::InvalidByteaHexOddDigits { .. } => {
            "22023"
        }
        ExecError::BitStringLengthMismatch { .. }
        | ExecError::BitStringTooLong { .. }
        | ExecError::BitStringSizeMismatch { .. } => "22026",
        ExecError::BitIndexOutOfRange { .. } => "2202E",
        ExecError::NegativeSubstringLength => "22011",
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { .. }) => "42883",
        ExecError::UniqueViolation { .. } => "23505",
        ExecError::NotNullViolation { .. } => "23502",
        ExecError::CheckViolation { .. } => "23514",
        ExecError::ForeignKeyViolation { .. } => "23503",
        ExecError::Parse(crate::backend::parser::ParseError::UnknownTable(_))
        | ExecError::Parse(crate::backend::parser::ParseError::TableDoesNotExist(_))
        | ExecError::Parse(crate::backend::parser::ParseError::InvalidFromClauseReference(_))
        | ExecError::Parse(crate::backend::parser::ParseError::MissingFromClauseEntry(_)) => {
            "42P01"
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnknownColumn(_))
        | ExecError::Parse(crate::backend::parser::ParseError::MissingKeyColumn(_)) => "42703",
        ExecError::Parse(crate::backend::parser::ParseError::AmbiguousColumn(_)) => "42702",
        ExecError::Parse(crate::backend::parser::ParseError::DuplicateTableName(_)) => "42712",
        ExecError::Parse(crate::backend::parser::ParseError::TableAlreadyExists(_)) => "42P07",
        ExecError::Parse(crate::backend::parser::ParseError::UnknownConfigurationParameter(_))
        | ExecError::Parse(crate::backend::parser::ParseError::UnsupportedType(_))
        | ExecError::Parse(crate::backend::parser::ParseError::MissingDefaultOpclass { .. }) => {
            "42704"
        }
        ExecError::Parse(crate::backend::parser::ParseError::CantChangeRuntimeParam(_)) => "55P02",
        ExecError::Parse(crate::backend::parser::ParseError::NoSchemaSelectedForCreate) => "3F000",
        ExecError::Parse(crate::backend::parser::ParseError::WindowingError(_)) => "42P20",
        ExecError::Parse(crate::backend::parser::ParseError::InvalidRecursion(_)) => "42P19",
        ExecError::Parse(crate::backend::parser::ParseError::InvalidTableDefinition(_)) => "42P16",
        ExecError::Parse(crate::backend::parser::ParseError::WrongObjectType { .. }) => "42809",
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError {
            sqlstate, ..
        }) => sqlstate,
        ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(_))
        | ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupportedMessage(_))
        | ExecError::Parse(crate::backend::parser::ParseError::OuterLevelAggregateNestedCte(_)) => {
            "0A000"
        }
        ExecError::Parse(crate::backend::parser::ParseError::ActiveSqlTransaction(_)) => "25001",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::NumericNaNToInt { .. }
        | ExecError::NumericInfinityToInt { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::NumericFieldOverflow
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow => "22003",
        ExecError::Interrupted(reason) => reason.sqlstate(),
        ExecError::RequestedLengthTooLarge => "54000",
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { .. })) => "54000",
        ExecError::RaiseException(_) => "P0001",
        ExecError::DivisionByZero(_) => "22012",
        ExecError::GenerateSeriesInvalidArg(_, _) => "22023",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::CardinalityViolation { .. } => "21000",
        ExecError::Heap(HeapError::DeadlockDetected) => "40P01",
        ExecError::Parse(_) => "42601",
        _ => "XX000",
    }
}

fn exec_error_detail(e: &ExecError) -> Option<&str> {
    match e {
        ExecError::WithContext { source, .. } => exec_error_detail(source),
        ExecError::Parse(
            crate::backend::parser::ParseError::InvalidPublicationParameterValue {
                parameter, ..
            },
        ) if parameter == "publish_generated_columns" => {
            Some("Valid values are \"none\" and \"stored\".")
        }
        ExecError::Regex(err) => err.detail.as_deref(),
        ExecError::JsonInput { detail, .. } => detail.as_deref(),
        ExecError::XmlInput { detail, .. } => detail.as_deref(),
        ExecError::DetailedError { detail, .. } => detail.as_deref(),
        ExecError::UniqueViolation { detail, .. } => detail.as_deref(),
        ExecError::NotNullViolation { detail, .. } => detail.as_deref(),
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { detail, .. }) => {
            detail.as_deref()
        }
        ExecError::ForeignKeyViolation { detail, .. } => detail.as_deref(),
        ExecError::ArrayInput { detail, .. } => detail.as_deref(),
        _ => None,
    }
}

fn exec_error_hint(e: &ExecError) -> Option<&str> {
    match e {
        ExecError::WithContext { source, .. } => exec_error_hint(source),
        ExecError::Regex(err) => err.hint.as_deref(),
        ExecError::DetailedError { hint, .. } => hint.as_deref(),
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { hint, .. }) => {
            hint.as_deref()
        }
        _ => None,
    }
}

fn exec_error_context(e: &ExecError) -> Option<String> {
    match e {
        ExecError::WithContext { source, context } => match exec_error_context(source) {
            Some(inner) => Some(format!("{inner}\n{context}")),
            None => Some(context.clone()),
        },
        ExecError::JsonInput { context, .. } => context.clone(),
        ExecError::XmlInput { context, .. } => context.clone(),
        ExecError::Regex(err) => err.context.clone(),
        _ => None,
    }
}

fn exec_error_position(sql: &str, e: &ExecError) -> Option<usize> {
    if let ExecError::WithContext { source, .. } = e {
        return exec_error_position(sql, source);
    }
    if matches!(e, ExecError::InvalidBooleanInput { .. })
        && sql.to_ascii_lowercase().contains("::text::boolean")
    {
        return None;
    }
    if matches!(
        e,
        ExecError::DetailedError { message, .. }
            if message == "invalid input syntax for type numeric: \" \""
    ) && sql.to_ascii_lowercase().contains("to_number(")
    {
        return None;
    }
    let value = match e {
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected, ..
        }) if matches!(*expected, "valid binary digit" | "valid hexadecimal digit") => {
            return find_bit_literal_position(sql);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            actual, ..
        }) if actual.starts_with("syntax error at or near \"") => {
            return extract_syntax_error_token(actual)
                .and_then(|token| sql.rfind(token).map(|index| index + 1));
        }
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { message, .. })
            if message == "duplicate trigger events specified at or near \"ON\"" =>
        {
            return find_last_case_insensitive_token_position(sql, "ON");
        }
        ExecError::Parse(crate::backend::parser::ParseError::UngroupedColumn {
            token,
            clause,
            ..
        }) => {
            return find_ungrouped_column_position(sql, token, clause);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
            actual,
        }) if actual.starts_with("Length(") => {
            return sql
                .to_ascii_lowercase()
                .find("length(")
                .map(|index| index + 1);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { op, .. }) => {
            return sql.find(op).map(|index| index + 1);
        }
        ExecError::Parse(crate::backend::parser::ParseError::MissingKeyColumn(_)) => {
            return find_without_overlaps_constraint_position(sql);
        }
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { message, .. }) => {
            if let Some(position) = publication_where_error_position(sql, message, None) {
                return Some(position);
            }
            if message.starts_with("column \"") && message.contains("WITHOUT OVERLAPS") {
                return find_without_overlaps_constraint_position(sql);
            }
            if let Some(position) = trigger_when_error_position(sql, message) {
                return Some(position);
            }
            if message.starts_with("cannot subscript type ") {
                return find_subscript_expression_position(sql);
            }
            if let Some(position) = find_detailed_operator_position(sql, message) {
                return Some(position);
            }
            if let Some(value) = extract_quoted_error_value(message) {
                value
            } else {
                return None;
            }
        }
        ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupportedMessage(
            message,
        )) => {
            if matches!(
                message.as_str(),
                "a column list with SET NULL is only supported for ON DELETE actions"
                    | "a column list with SET DEFAULT is only supported for ON DELETE actions"
            ) {
                return find_case_insensitive_token_position(sql, "ON UPDATE");
            }
            return None;
        }
        ExecError::Parse(crate::backend::parser::ParseError::InvalidPublicationTableName(name))
        | ExecError::Parse(crate::backend::parser::ParseError::InvalidPublicationSchemaName(
            name,
        )) => {
            return find_case_insensitive_token_position(sql, name);
        }
        ExecError::Parse(crate::backend::parser::ParseError::ConflictingOrRedundantOptions {
            option,
        }) => {
            return find_second_option_occurrence(sql, option);
        }
        ExecError::InvalidIntegerInput { value, .. } => value.as_str(),
        ExecError::ArrayInput { value, .. } => value.as_str(),
        ExecError::IntegerOutOfRange { value, .. } => value.as_str(),
        ExecError::InvalidNumericInput(value) => value.as_str(),
        ExecError::InvalidByteaInput { value } => value.as_str(),
        ExecError::InvalidByteaHexDigit { value, .. } => value.as_str(),
        ExecError::InvalidByteaHexOddDigits { value } => value.as_str(),
        ExecError::InvalidGeometryInput { value, .. } => value.as_str(),
        ExecError::InvalidBooleanInput { value } => value.as_str(),
        ExecError::InvalidFloatInput { value, .. } => value.as_str(),
        ExecError::FloatOutOfRange { value, .. } => value.as_str(),
        ExecError::InvalidStorageValue { details, .. } => {
            if let Some(value) = extract_quoted_error_value(details) {
                value
            } else {
                return None;
            }
        }
        ExecError::DetailedError {
            message, detail, ..
        } => {
            if message.starts_with("invalid value for parameter \"default_toast_compression\"") {
                return None;
            }
            if message.starts_with("invalid size: \"") {
                return None;
            }
            if message == "wrong flag in flag array: \"\"" {
                return None;
            }
            if let Some(position) =
                publication_where_error_position(sql, message, detail.as_deref())
            {
                return Some(position);
            }
            if message.starts_with("column \"") && message.contains("WITHOUT OVERLAPS") {
                return find_without_overlaps_constraint_position(sql);
            }
            if let Some(position) = trigger_when_error_position(sql, message) {
                return Some(position);
            }
            if let Some(target) = extract_subscripted_assignment_target(message) {
                return find_subscripted_assignment_position(sql, target);
            }
            if let Some(value) = extract_quoted_error_value(message) {
                value
            } else {
                return None;
            }
        }
        ExecError::JsonInput { raw_input, .. } => {
            return find_json_literal_position(sql, raw_input)
                .or_else(|| sql.find(raw_input).map(|index| index + 1));
        }
        ExecError::XmlInput { raw_input, .. } => raw_input.as_str(),
        _ => return None,
    };
    find_error_value_position(sql, value)
}

fn publication_where_error_position(
    sql: &str,
    message: &str,
    detail: Option<&str>,
) -> Option<usize> {
    if message == "WHERE clause not allowed for schema" {
        return find_case_insensitive_token_position(sql, "WHERE");
    }
    if message.starts_with("argument of PUBLICATION WHERE must be type boolean") {
        return find_publication_where_expression_position(sql);
    }
    if message == "aggregate functions are not allowed in WHERE" {
        return find_case_insensitive_token_position(sql, "AVG(")
            .or_else(|| find_case_insensitive_token_position(sql, "WHERE"));
    }
    if message == "invalid publication WHERE expression" {
        if detail == Some("System columns are not allowed.") {
            return find_case_insensitive_token_position(sql, "ctid");
        }
        return find_case_insensitive_token_position(sql, "WHERE");
    }
    if message == "cannot use a WHERE clause when removing a table from a publication" {
        return find_case_insensitive_token_position(sql, "WHERE");
    }
    None
}

fn find_publication_where_expression_position(sql: &str) -> Option<usize> {
    let where_position = find_case_insensitive_token_position(sql, "WHERE")?;
    let mut index = where_position - 1 + "WHERE".len();
    let bytes = sql.as_bytes();
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    if index < bytes.len() && bytes[index] == b'(' {
        index += 1;
    }
    Some(index + 1)
}

fn find_without_overlaps_constraint_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    let overlap_index = lower.find("without overlaps")?;
    let prefix = &lower[..overlap_index];
    if let Some(index) = prefix.rfind("constraint") {
        return Some(index + 1);
    }
    if let Some(index) = prefix.rfind("primary key") {
        return Some(index + 1);
    }
    prefix.rfind("unique").map(|index| index + 1)
}

fn find_json_literal_position(sql: &str, raw_input: &str) -> Option<usize> {
    let escaped_literal = format!("'{}'", raw_input.replace('\'', "''"));
    if let Some(index) = sql.find(&escaped_literal) {
        return Some(index + 1);
    }
    find_dollar_quoted_literal_position(sql, raw_input)
}

fn find_dollar_quoted_literal_position(sql: &str, raw_input: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() {
        if bytes[start] != b'$' {
            start += 1;
            continue;
        }

        let mut tag_end = start + 1;
        while tag_end < bytes.len() && bytes[tag_end] != b'$' {
            let ch = bytes[tag_end] as char;
            if !(ch.is_ascii_alphanumeric() || ch == '_') {
                break;
            }
            tag_end += 1;
        }
        if tag_end >= bytes.len() || bytes[tag_end] != b'$' {
            start += 1;
            continue;
        }

        let delimiter = &sql[start..=tag_end];
        let body_start = tag_end + 1;
        let Some(relative_end) = sql[body_start..].find(delimiter) else {
            start += 1;
            continue;
        };
        let body_end = body_start + relative_end;
        if &sql[body_start..body_end] == raw_input {
            return Some(start + 1);
        }
        start = body_end + delimiter.len();
    }
    None
}

fn extract_quoted_error_value(message: &str) -> Option<&str> {
    if let Some(start) = message.find("value \"") {
        let rest = &message[start + "value \"".len()..];
        let end = rest.find('"')?;
        return Some(&rest[..end]);
    }

    let (_, rest) = message.rsplit_once(": \"")?;
    rest.strip_suffix('"')
}

fn trigger_when_error_position(sql: &str, message: &str) -> Option<usize> {
    match message {
        "INSERT trigger's WHEN condition cannot reference OLD values" => {
            find_case_insensitive_token_position(sql, "OLD.")
        }
        "DELETE trigger's WHEN condition cannot reference NEW values" => {
            find_case_insensitive_token_position(sql, "NEW.")
        }
        "statement trigger's WHEN condition cannot reference column values" => {
            find_case_insensitive_token_position(sql, "OLD.")
                .or_else(|| find_case_insensitive_token_position(sql, "NEW."))
        }
        "BEFORE trigger's WHEN condition cannot reference NEW system columns" => {
            find_case_insensitive_token_position(sql, "NEW.tableoid")
                .or_else(|| find_case_insensitive_token_position(sql, "NEW.ctid"))
        }
        _ => None,
    }
}

fn extract_subscripted_assignment_target(message: &str) -> Option<&str> {
    let prefix = "subscripted assignment to \"";
    let rest = message.strip_prefix(prefix)?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn find_error_value_position(sql: &str, value: &str) -> Option<usize> {
    let needle = format!("'{}'", value.replace('\'', "''"));
    if let Some(index) = sql.rfind(&needle) {
        let prefix = sql[..index].trim_end();
        let last_word = prefix
            .rsplit(|ch: char| !ch.is_ascii_alphabetic())
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(
            last_word.as_str(),
            "date" | "time" | "timetz" | "timestamp" | "timestamptz" | "interval"
        ) {
            return Some(index + 2);
        }
        return Some(index + 1);
    }
    sql.find(value).map(|index| index + 1)
}

fn find_detailed_operator_position(sql: &str, message: &str) -> Option<usize> {
    let (_, detail) = message.rsplit_once(": ")?;
    for op in ["<>", "<=", ">=", "+", "-", "*", "/", "%", "<", ">", "="] {
        if detail.contains(&format!(" {op} ")) {
            return sql.find(op).map(|index| index + 1);
        }
    }
    None
}

fn find_subscripted_assignment_position(sql: &str, target: &str) -> Option<usize> {
    let candidates = [format!("{target}["), format!("\"{target}\"[")];
    for candidate in candidates {
        if let Some(index) = find_case_insensitive_token_position(sql, &candidate) {
            return Some(index);
        }
    }
    None
}

fn find_subscript_expression_position(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let bracket = bytes.iter().position(|byte| *byte == b'[')?;
    let start = find_subscript_base_start(bytes, bracket)?;
    Some(start + 1)
}

fn find_subscript_base_start(bytes: &[u8], bracket: usize) -> Option<usize> {
    let mut pos = bracket.checked_sub(1)?;
    while bytes
        .get(pos)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        pos = pos.checked_sub(1)?;
    }
    match *bytes.get(pos)? {
        b')' => {
            let mut depth = 1usize;
            let mut idx = pos;
            while idx > 0 {
                idx -= 1;
                match bytes[idx] {
                    b')' => depth += 1,
                    b'(' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(extend_identifier_chain_left(bytes, idx));
                        }
                    }
                    _ => {}
                }
            }
            Some(extend_identifier_chain_left(bytes, pos))
        }
        _ => Some(extend_identifier_chain_left(bytes, pos)),
    }
}

fn extend_identifier_chain_left(bytes: &[u8], pos: usize) -> usize {
    let mut start = pos;
    while start > 0 {
        let prev = bytes[start - 1];
        if prev.is_ascii_alphanumeric() || matches!(prev, b'_' | b'.' | b'"') {
            start -= 1;
            continue;
        }
        break;
    }
    start
}

fn extract_syntax_error_token(message: &str) -> Option<&str> {
    let prefix = "syntax error at or near \"";
    let start = message.strip_prefix(prefix)?;
    let end = start.rfind('"')?;
    Some(&start[..end])
}

fn find_second_option_occurrence(sql: &str, option: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    let mut search_from = 0usize;
    let mut seen = 0usize;
    while let Some(relative) = lower[search_from..].find(option) {
        let index = search_from + relative;
        seen += 1;
        if seen == 2 {
            return Some(index + 1);
        }
        search_from = index.saturating_add(option.len());
    }
    None
}

fn find_case_insensitive_token_position(sql: &str, token: &str) -> Option<usize> {
    if let Some(index) = sql.find(token) {
        return Some(index + 1);
    }
    if token.contains('.') {
        let quoted = token
            .split('.')
            .map(|part| format!("\"{part}\""))
            .collect::<Vec<_>>()
            .join(".");
        if let Some(index) = sql.find(&quoted) {
            return Some(index + 1);
        }
        let quoted_lower = quoted.to_ascii_lowercase();
        if let Some(index) = sql.to_ascii_lowercase().find(&quoted_lower) {
            return Some(index + 1);
        }
    }
    let token_lower = token.to_ascii_lowercase();
    sql.to_ascii_lowercase()
        .find(&token_lower)
        .map(|index| index + 1)
}

fn find_last_case_insensitive_token_position(sql: &str, token: &str) -> Option<usize> {
    let token_lower = token.to_ascii_lowercase();
    sql.to_ascii_lowercase()
        .rfind(&token_lower)
        .map(|index| index + 1)
}

struct ExecErrorResponse {
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    context: Option<String>,
    position: Option<usize>,
}

struct SessionActivityGuard<'a> {
    db: &'a Database,
    client_id: ClientId,
}

impl<'a> SessionActivityGuard<'a> {
    fn new(db: &'a Database, client_id: ClientId, query: &str) -> Self {
        db.set_session_query_active(client_id, query);
        Self { db, client_id }
    }
}

impl Drop for SessionActivityGuard<'_> {
    fn drop(&mut self) {
        self.db.set_session_query_idle(self.client_id);
    }
}

fn exec_error_response(sql: &str, e: &ExecError) -> ExecErrorResponse {
    let message = format_exec_error(e);
    let mut response = ExecErrorResponse {
        message,
        detail: None,
        hint: None,
        context: exec_error_context(e),
        position: exec_error_position(sql, e),
    };

    match response.message.as_str() {
        "unsafe use of string constant with Unicode escapes" => {
            response.detail = Some(
                "String constants with Unicode escapes cannot be used when \"standard_conforming_strings\" is off.".into(),
            );
            response.position = find_unicode_string_position(sql).or(response.position);
        }
        "invalid Unicode escape" => {
            response.hint = Some(if sql.contains("unistr(") {
                "Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.".into()
            } else if sql.contains("E'") {
                "Unicode escapes must be \\uXXXX or \\UXXXXXXXX.".into()
            } else {
                "Unicode escapes must be \\XXXX or \\+XXXXXX.".into()
            });
            if sql.contains("unistr(") {
                response.position = None;
            } else {
                response.position = find_unicode_escape_position(sql).or(response.position);
            }
        }
        "invalid Unicode surrogate pair" | "invalid Unicode escape value" => {
            if sql.contains("unistr(") {
                response.position = None;
            } else {
                response.position = find_unicode_escape_position(sql).or(response.position);
            }
            if sql.contains("E'") {
                if response.message == "invalid Unicode surrogate pair" {
                    if let Some(token) = find_e_unicode_near_token(sql) {
                        response.message =
                            format!("invalid Unicode surrogate pair at or near \"{token}\"");
                    }
                } else if response.message == "invalid Unicode escape value" {
                    if let Some(token) = find_e_unicode_escape_token(sql) {
                        response.message =
                            format!("invalid Unicode escape value at or near \"{token}\"");
                    }
                }
            }
        }
        msg if msg.starts_with("UESCAPE must be followed by a simple string literal") => {
            response.position = find_uescape_token_position(sql).or(response.position);
        }
        msg if msg.starts_with("invalid Unicode escape character at or near") => {
            response.position = find_uescape_literal_position(sql).or(response.position);
        }
        _ => {}
    }

    if response.detail.is_none()
        && let ExecError::Parse(crate::backend::parser::ParseError::OuterLevelAggregateNestedCte(
            cte_name,
        )) = e
    {
        response.detail = Some(format!(
            "CTE \"{cte_name}\" is below the aggregate's semantic level."
        ));
    }

    response
}

fn find_unicode_string_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower.find("u&'").map(|idx| idx + 1)
}

fn find_unicode_escape_position(sql: &str) -> Option<usize> {
    sql.find('\\').map(|idx| idx + 1)
}

fn find_uescape_token_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower.find("uescape").and_then(|idx| {
        let tail = &sql[idx + "UESCAPE".len()..];
        let offset = tail.find(|ch: char| !ch.is_ascii_whitespace())?;
        Some(idx + "UESCAPE".len() + offset + 1)
    })
}

fn find_uescape_literal_position(sql: &str) -> Option<usize> {
    sql.rfind("'+'").map(|idx| idx + 1)
}

fn extract_e_literal(sql: &str) -> Option<&str> {
    let start = sql.find("E'")? + 2;
    let end = sql[start..].rfind('\'')? + start;
    Some(&sql[start..end])
}

fn find_e_unicode_near_token(sql: &str) -> Option<String> {
    let raw = extract_e_literal(sql)?;
    let bytes = raw.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
            continue;
        }
        let (len, code) = parse_e_unicode_escape(bytes, i)?;
        if !(0xD800..=0xDBFF).contains(&code) {
            i += len;
            continue;
        }
        let next = i + len;
        if next >= bytes.len() {
            return Some("'".into());
        }
        if bytes[next] != b'\\' {
            return Some((bytes[next] as char).to_string());
        }
        if next + 1 >= bytes.len() || bytes[next + 1] == b'\\' {
            return Some("\\".into());
        }
        let next_len = match bytes[next + 1] {
            b'u' => 6,
            b'U' => 10,
            _ => 1,
        };
        let end = (next + next_len).min(bytes.len());
        return Some(raw[next..end].to_string());
    }
    None
}

fn find_e_unicode_escape_token(sql: &str) -> Option<String> {
    let raw = extract_e_literal(sql)?;
    let start = raw.find('\\')?;
    let bytes = raw.as_bytes();
    let len = match bytes.get(start + 1)? {
        b'u' => 6,
        b'U' => 10,
        _ => 5,
    };
    let end = (start + len).min(bytes.len());
    Some(raw[start..end].to_string())
}

fn parse_e_unicode_escape(bytes: &[u8], start: usize) -> Option<(usize, u32)> {
    if start + 2 > bytes.len() || bytes[start] != b'\\' {
        return None;
    }
    let (len, digits_start, digits_end) = match bytes[start + 1] {
        b'u' => (6, start + 2, start + 6),
        b'U' => (10, start + 2, start + 10),
        _ => return None,
    };
    let digits = std::str::from_utf8(&bytes[digits_start..digits_end]).ok()?;
    let code = u32::from_str_radix(digits, 16).ok()?;
    Some((len, code))
}

fn send_exec_error(stream: &mut impl Write, sql: &str, e: &ExecError) -> io::Result<()> {
    let mut response = exec_error_response(sql, e);
    if response.detail.is_none() {
        response.detail = exec_error_detail(e).map(str::to_string);
    }
    if response.hint.is_none() {
        response.hint = exec_error_hint(e).map(str::to_string);
    }
    if response.hint.is_none() {
        response.hint = format_exec_error_hint(e);
    }
    send_error_with_fields(
        stream,
        exec_error_sqlstate(e),
        &response.message,
        response.detail.as_deref(),
        response.hint.as_deref(),
        response.context.as_deref(),
        response.position,
    )
}

fn find_bit_literal_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower
        .find("b'")
        .or_else(|| lower.find("x'"))
        .map(|index| index + 1)
}

fn find_ungrouped_column_position(
    sql: &str,
    token: &str,
    clause: &UngroupedColumnClause,
) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    let (start, end) = match clause {
        UngroupedColumnClause::SelectTarget => {
            let start = lower.find("select")? + "select".len();
            let end = lower.find(" from ").or_else(|| lower.find(" from"))?;
            (start, end)
        }
        UngroupedColumnClause::Having => {
            let start = lower.find("having")? + "having".len();
            (start, sql.len())
        }
        UngroupedColumnClause::Other => (0, sql.len()),
    };
    let segment = &sql[start..end];
    find_identifier_in_segment(segment, token).map(|offset| start + offset + 1)
}

fn find_identifier_in_segment(segment: &str, token: &str) -> Option<usize> {
    let token_lower = token.to_ascii_lowercase();
    let segment_lower = segment.to_ascii_lowercase();
    let mut from = 0;
    while let Some(found) = segment_lower[from..].find(&token_lower) {
        let idx = from + found;
        let before = segment[..idx].chars().next_back();
        let after = segment[idx + token.len()..].chars().next();
        let is_ident = |ch: char| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.';
        if !before.is_some_and(is_ident) && !after.is_some_and(is_ident) {
            return Some(idx);
        }
        from = idx + token.len();
    }
    None
}
use crate::ClientId;
use crate::pgrust::cluster::Cluster;
use crate::pgrust::database::Database;
use crate::pgrust::portal::{CursorOptions, PortalFetchDirection, PortalFetchLimit};
use crate::pgrust::session::Session;

const SSL_REQUEST_CODE: i32 = 80877103;
pub(crate) const PROTOCOL_VERSION_3_0: i32 = 196608;

static NEXT_CLIENT_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Default)]
struct PreparedStatement {
    sql: String,
    param_type_oids: Vec<u32>,
}

#[derive(Debug, Clone)]
enum BoundParam {
    Null,
    Text(String),
    SqlExpression(String),
}

struct ConnectionState {
    session: Session,
    prepared: HashMap<String, PreparedStatement>,
    portals: HashMap<String, ()>,
    copy_in: Option<CopyInState>,
}

struct CopyInState {
    table_name: String,
    columns: Option<Vec<String>>,
    null_marker: String,
    pending: Vec<u8>,
}

struct ConnectionCleanupGuard<'a> {
    db: &'a Database,
    cluster: &'a Cluster,
    state: &'a mut ConnectionState,
}

impl Drop for ConnectionCleanupGuard<'_> {
    fn drop(&mut self) {
        let client_id = self.state.session.client_id;
        let temp_backend_id = self.state.session.temp_backend_id;
        self.state.session.cleanup_on_disconnect(self.db);
        self.db.cleanup_client_temp_relations(client_id);
        self.db.clear_temp_backend_id(client_id);
        self.db.clear_session_activity(client_id);
        self.db.clear_interrupt_state(client_id);
        self.cluster.unregister_connection(self.db.database_oid);
        self.cluster.release_temp_backend_id(temp_backend_id);
    }
}

pub fn serve(addr: &str, cluster: Cluster) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("pgrust: listening on {addr}");

    for stream in listener.incoming() {
        let stream = stream?;
        let peer = stream.peer_addr().ok();
        let cluster = cluster.clone();
        let connection = move || {
            let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
            cluster
                .shared()
                .pool
                .with_storage_mut(|s| s.smgr.acquire_external_fd());
            if let Some(peer) = &peer {
                eprintln!("pgrust: connection from {peer} (client {client_id})");
            }
            if let Err(e) = handle_connection(stream, &cluster, client_id) {
                if e.kind() != io::ErrorKind::UnexpectedEof
                    && e.kind() != io::ErrorKind::ConnectionReset
                {
                    eprintln!("pgrust: client {client_id} error: {e}");
                }
            }
            if let Some(peer) = &peer {
                eprintln!("pgrust: client {client_id} ({peer}) disconnected");
            }
            cluster
                .shared()
                .pool
                .with_storage_mut(|s| s.smgr.release_external_fd());
        };
        #[cfg(debug_assertions)]
        thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(connection)
            .map_err(|err| io::Error::other(format!("failed to spawn client thread: {err}")))?;
        #[cfg(not(debug_assertions))]
        thread::spawn(connection);
    }
    Ok(())
}

pub(crate) fn handle_connection_with_io<R, W>(
    mut reader: R,
    writer: W,
    cluster: &Cluster,
    client_id: ClientId,
) -> io::Result<()>
where
    R: Read,
    W: Write,
{
    let mut writer = BufWriter::new(writer);

    let startup_params = loop {
        let len = read_i32(&mut reader)? as usize;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "startup packet too short",
            ));
        }
        let mut payload = vec![0u8; len - 4];
        reader.read_exact(&mut payload)?;

        let code = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        match code {
            SSL_REQUEST_CODE => {
                writer.write_all(b"N")?;
                writer.flush()?;
                continue;
            }
            PROTOCOL_VERSION_3_0 => {
                break parse_startup_parameters(&payload[4..])?;
            }
            _ => {
                send_error(
                    &mut writer,
                    "08P01",
                    &format!("unsupported protocol version: {code}"),
                    None,
                    None,
                    None,
                )?;
                writer.flush()?;
                return Ok(());
            }
        }
    };

    let requested_database = startup_params
        .get("database")
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| {
            startup_params
                .get("user")
                .filter(|value| !value.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| "postgres".into());
    let db = match cluster.connect_database(&requested_database) {
        Ok(db) => db,
        Err(err) => {
            send_error(
                &mut writer,
                exec_error_sqlstate(&err),
                &format_exec_error(&err),
                exec_error_detail(&err),
                exec_error_hint(&err),
                None,
            )?;
            writer.flush()?;
            return Ok(());
        }
    };
    cluster.register_connection(db.database_oid);
    let temp_backend_id = cluster.allocate_temp_backend_id();
    db.install_temp_backend_id(client_id, temp_backend_id);

    let mut state = ConnectionState {
        session: Session::with_temp_backend_id(client_id, temp_backend_id),
        prepared: HashMap::new(),
        portals: HashMap::new(),
        copy_in: None,
    };
    if let Err(err) = state.session.apply_startup_parameters(&startup_params) {
        db.clear_temp_backend_id(client_id);
        cluster.release_temp_backend_id(temp_backend_id);
        cluster.unregister_connection(db.database_oid);
        send_error(
            &mut writer,
            exec_error_sqlstate(&err),
            &format_exec_error(&err),
            exec_error_detail(&err),
            exec_error_hint(&err),
            None,
        )?;
        writer.flush()?;
        return Ok(());
    }
    send_auth_ok(&mut writer)?;
    send_parameter_status(&mut writer, "server_version", "18.3")?;
    send_parameter_status(&mut writer, "server_encoding", "UTF8")?;
    send_parameter_status(&mut writer, "client_encoding", "UTF8")?;
    send_parameter_status(
        &mut writer,
        "DateStyle",
        &format_datestyle(state.session.datetime_config()),
    )?;
    send_parameter_status(
        &mut writer,
        "TimeZone",
        &state.session.datetime_config().time_zone,
    )?;
    send_parameter_status(&mut writer, "integer_datetimes", "on")?;
    send_parameter_status(
        &mut writer,
        "standard_conforming_strings",
        if state.session.standard_conforming_strings() {
            "on"
        } else {
            "off"
        },
    )?;
    send_backend_key_data(&mut writer, client_id as i32, client_id as i32)?;
    send_ready_for_query(&mut writer, b'I')?;
    writer.flush()?;

    db.register_session_activity(client_id);
    let cleanup = ConnectionCleanupGuard {
        db: &db,
        cluster,
        state: &mut state,
    };

    let result = {
        let state = &mut *cleanup.state;
        loop {
            let msg_type = match read_byte(&mut reader) {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break Ok(()),
                Err(e) => break Err(e),
            };

            let len = read_i32(&mut reader)? as usize;
            if len < 4 {
                break Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message too short",
                ));
            }
            let mut body = vec![0u8; len - 4];
            reader.read_exact(&mut body)?;

            match msg_type {
                b'Q' => {
                    let sql = cstr_from_bytes(&body);
                    handle_query(&mut writer, &db, state, &sql)?;
                    writer.flush()?;
                }
                b'P' => {
                    handle_parse(&mut writer, state, &body)?;
                    flush_pending_backend_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'B' => {
                    handle_bind(&mut writer, &db, state, &body)?;
                    flush_pending_backend_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'D' => {
                    handle_describe(&mut writer, &db, state, &body)?;
                    flush_pending_backend_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'E' => {
                    handle_execute(&mut writer, &db, state, &body)?;
                    writer.flush()?;
                }
                b'S' => {
                    state.session.interrupts().reset_statement_state();
                    db.interrupt_state(state.session.client_id)
                        .reset_statement_state();
                    send_ready_with_pending_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'C' => {
                    handle_close(&mut writer, state, &body)?;
                    flush_pending_backend_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'H' => {
                    flush_pending_backend_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
                b'd' => handle_copy_data(state, &body)?,
                b'c' => {
                    handle_copy_done(&mut writer, &db, state)?;
                    writer.flush()?;
                }
                b'f' => {
                    handle_copy_fail(&mut writer, &db, state, &body)?;
                    writer.flush()?;
                }
                b'X' => break Ok(()),
                _ => {
                    send_error(
                        &mut writer,
                        "0A000",
                        &format!("unsupported message type: '{}'", msg_type as char),
                        None,
                        None,
                        None,
                    )?;
                    send_ready_with_pending_messages(&mut writer, &db, &state.session)?;
                    writer.flush()?;
                }
            }
        }
    };
    drop(cleanup);
    result
}

pub(crate) fn handle_connection(
    stream: TcpStream,
    cluster: &Cluster,
    client_id: ClientId,
) -> io::Result<()> {
    let reader = stream.try_clone()?;
    handle_connection_with_io(reader, stream, cluster, client_id)
}

fn parse_startup_parameters(payload: &[u8]) -> io::Result<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut offset = 0usize;
    while offset < payload.len() {
        let key = read_cstr(payload, &mut offset)?;
        if key.is_empty() {
            break;
        }
        let value = read_cstr(payload, &mut offset)?;
        params.insert(key, value);
    }
    Ok(params)
}

fn handle_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<()> {
    state.session.interrupts().reset_statement_state();
    db.interrupt_state(state.session.client_id)
        .reset_statement_state();
    if sql_is_effectively_empty_after_comments(sql) {
        send_empty_query(stream)?;
        send_ready_with_pending_messages(stream, db, &state.session)?;
        return Ok(());
    }
    let mut executed_any = false;
    let mut copy_in_started = false;
    for raw_stmt in split_simple_query_statements(sql) {
        if sql_is_effectively_empty_after_comments(raw_stmt) {
            continue;
        }
        executed_any = true;
        match execute_query_statement(stream, db, state, raw_stmt)? {
            QueryStatementFlow::Continue => {}
            QueryStatementFlow::Stop => break,
            QueryStatementFlow::CopyInStarted => {
                copy_in_started = true;
                break;
            }
        }
    }

    if !executed_any {
        send_empty_query(stream)?;
    }
    if copy_in_started {
        return Ok(());
    }
    send_ready_with_pending_messages(stream, db, &state.session)?;
    Ok(())
}

enum QueryStatementFlow {
    Continue,
    Stop,
    CopyInStarted,
}

fn handle_portal_statement(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
    stmt: &Statement,
) -> io::Result<Option<QueryStatementFlow>> {
    match stmt {
        Statement::DeclareCursor(declare_stmt) => {
            let options = CursorOptions {
                holdable: declare_stmt.hold,
                binary: declare_stmt.binary,
                scroll: matches!(
                    declare_stmt.scroll,
                    crate::backend::parser::CursorScrollOption::Scroll
                ),
                no_scroll: matches!(
                    declare_stmt.scroll,
                    crate::backend::parser::CursorScrollOption::NoScroll
                ),
                visible: true,
            };
            match state.session.declare_cursor(
                db,
                &declare_stmt.name,
                sql.trim().trim_end_matches(';').to_string(),
                &declare_stmt.query,
                options,
            ) {
                Ok(()) => send_command_complete(stream, "DECLARE CURSOR")?,
                Err(e) => {
                    state.session.mark_transaction_failed();
                    send_exec_error(stream, sql, &e)?;
                    return Ok(Some(QueryStatementFlow::Stop));
                }
            }
            Ok(Some(QueryStatementFlow::Continue))
        }
        Statement::Fetch(fetch_stmt) | Statement::Move(fetch_stmt) => {
            let move_only = matches!(stmt, Statement::Move(_));
            match state.session.fetch_cursor(
                &fetch_stmt.cursor_name,
                protocol_direction_from_fetch(&fetch_stmt.direction),
                move_only,
            ) {
                Ok(mut result) => {
                    if move_only {
                        send_command_complete(stream, &format!("MOVE {}", result.processed))?;
                    } else {
                        let catalog = state.session.catalog_lookup(db);
                        annotate_query_columns_with_wire_type_oids(&mut result.columns, &catalog);
                        send_query_result(
                            stream,
                            &result.columns,
                            &result.rows,
                            &format!("FETCH {}", result.processed),
                            FloatFormatOptions {
                                extra_float_digits: state.session.extra_float_digits(),
                                bytea_output: state.session.bytea_output(),
                                datetime_config: state.session.datetime_config().clone(),
                            },
                            None,
                            None,
                            None,
                            None,
                        )?;
                    }
                }
                Err(e) => {
                    state.session.mark_transaction_failed();
                    send_exec_error(stream, sql, &e)?;
                    return Ok(Some(QueryStatementFlow::Stop));
                }
            }
            Ok(Some(QueryStatementFlow::Continue))
        }
        Statement::ClosePortal(close_stmt) => {
            let result = if let Some(name) = &close_stmt.name {
                state.session.close_portal(name)
            } else {
                state.session.close_all_cursors();
                Ok(())
            };
            match result {
                Ok(()) => send_command_complete(stream, "CLOSE CURSOR")?,
                Err(e) => {
                    state.session.mark_transaction_failed();
                    send_exec_error(stream, sql, &e)?;
                    return Ok(Some(QueryStatementFlow::Stop));
                }
            }
            Ok(Some(QueryStatementFlow::Continue))
        }
        _ => Ok(None),
    }
}

fn protocol_direction_from_fetch(
    direction: &crate::backend::parser::FetchDirection,
) -> PortalFetchDirection {
    use crate::backend::parser::FetchDirection;
    match direction {
        FetchDirection::Next => PortalFetchDirection::Next,
        FetchDirection::Prior => PortalFetchDirection::Prior,
        FetchDirection::First => PortalFetchDirection::First,
        FetchDirection::Last => PortalFetchDirection::Last,
        FetchDirection::Absolute(value) => PortalFetchDirection::Absolute(*value),
        FetchDirection::Relative(value) => PortalFetchDirection::Relative(*value),
        FetchDirection::Forward(count) => {
            PortalFetchDirection::Forward(fetch_limit_from_i64(*count))
        }
        FetchDirection::Backward(count) => {
            PortalFetchDirection::Backward(fetch_limit_from_i64(*count))
        }
    }
}

fn fetch_limit_from_i64(count: Option<i64>) -> PortalFetchLimit {
    match count {
        None => PortalFetchLimit::All,
        Some(value) if value <= 0 => PortalFetchLimit::Count(0),
        Some(value) => PortalFetchLimit::Count(value as usize),
    }
}

fn try_handle_pg_cursors_query(
    stream: &mut impl Write,
    state: &ConnectionState,
    sql: &str,
) -> io::Result<bool> {
    let normalized = sql.to_ascii_lowercase();
    if !normalized.contains("from pg_cursors") && !normalized.contains("from pg_catalog.pg_cursors")
    {
        return Ok(false);
    }
    let name_only = normalized.trim_start().starts_with("select name ");
    let columns = if name_only {
        vec![QueryColumn::text("name")]
    } else {
        vec![
            QueryColumn::text("name"),
            QueryColumn::text("statement"),
            QueryColumn {
                name: "is_holdable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "is_binary".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "is_scrollable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
        ]
    };
    let rows = state
        .session
        .cursor_view_rows()
        .into_iter()
        .map(|row| {
            if name_only {
                vec![Value::Text(row.name.into())]
            } else {
                vec![
                    Value::Text(row.name.into()),
                    Value::Text(row.statement.into()),
                    Value::Bool(row.is_holdable),
                    Value::Bool(row.is_binary),
                    Value::Bool(row.is_scrollable),
                ]
            }
        })
        .collect::<Vec<_>>();
    send_query_result(
        stream,
        &columns,
        &rows,
        &format!("SELECT {}", rows.len()),
        FloatFormatOptions {
            extra_float_digits: state.session.extra_float_digits(),
            bytea_output: state.session.bytea_output(),
            datetime_config: state.session.datetime_config().clone(),
        },
        None,
        None,
        None,
        None,
    )?;
    Ok(true)
}

fn execute_query_statement(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<QueryStatementFlow> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Ok(QueryStatementFlow::Continue);
    }
    let _activity_guard = SessionActivityGuard::new(db, state.session.client_id, sql);
    if try_handle_float_shell_ddl(stream, sql)? {
        return Ok(QueryStatementFlow::Continue);
    }
    let sql = rewrite_regression_sql(sql);

    if try_handle_psql_describe_query(stream, db, state, &sql)? {
        return Ok(QueryStatementFlow::Continue);
    }
    if try_handle_statistics_catalog_query(stream, db, state, &sql)? {
        return Ok(QueryStatementFlow::Continue);
    }

    if let Some((table_name, columns, null_marker)) = parse_copy_from_stdin(&sql) {
        state.copy_in = Some(CopyInState {
            table_name,
            columns,
            null_marker,
            pending: Vec::new(),
        });
        send_copy_in_response(stream)?;
        return Ok(QueryStatementFlow::CopyInStarted);
    }

    let parsed = if state.session.standard_conforming_strings() {
        db.plan_cache
            .get_statement_with_options(
                &sql,
                crate::backend::parser::ParseOptions {
                    max_stack_depth_kb: state.session.datetime_config().max_stack_depth_kb,
                    ..crate::backend::parser::ParseOptions::default()
                },
            )
            .map_err(|e| io::Error::other(format!("{e:?}")))
    } else {
        crate::backend::parser::parse_statement_with_options(
            &sql,
            crate::backend::parser::ParseOptions {
                standard_conforming_strings: false,
                max_stack_depth_kb: state.session.datetime_config().max_stack_depth_kb,
            },
        )
        .map_err(|e| io::Error::other(format!("{e:?}")))
    };
    if let Ok(stmt) = parsed.as_ref()
        && let Some(flow) = handle_portal_statement(stream, db, state, sql.as_ref(), stmt)?
    {
        return Ok(flow);
    }
    if try_handle_pg_cursors_query(stream, state, sql.as_ref())? {
        return Ok(QueryStatementFlow::Continue);
    }
    if let Ok(Statement::Select(ref select_stmt)) = parsed
        && !raw_select_contains_pg_notify(select_stmt)
    {
        let max_stack_depth_kb = state.session.datetime_config().max_stack_depth_kb;
        return stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(max_stack_depth_kb)
                .run(|| execute_streaming_select_statement(stream, db, state, &sql, select_stmt))
        });
    }

    clear_backend_notices();
    clear_notices();
    match state.session.execute(db, &sql) {
        Ok(StatementResult::Query {
            mut columns, rows, ..
        }) => {
            let catalog = state.session.catalog_lookup(db);
            let role_names = role_name_map(&catalog);
            let relation_names = relation_name_map(&catalog);
            let proc_names = proc_name_map(&catalog);
            let namespace_names = namespace_name_map(&catalog);
            annotate_query_columns_with_wire_type_oids(&mut columns, &catalog);
            flush_pending_backend_messages(stream, db, &state.session)?;
            send_query_result(
                stream,
                &columns,
                &rows,
                &format!("SELECT {}", rows.len()),
                FloatFormatOptions {
                    extra_float_digits: state.session.extra_float_digits(),
                    bytea_output: state.session.bytea_output(),
                    datetime_config: state.session.datetime_config().clone(),
                },
                Some(&role_names),
                Some(&relation_names),
                Some(&proc_names),
                Some(&namespace_names),
            )?;
            Ok(QueryStatementFlow::Continue)
        }
        Ok(StatementResult::AffectedRows(n)) => {
            flush_pending_backend_messages(stream, db, &state.session)?;
            send_command_complete(stream, &infer_command_tag(&sql, n))?;
            Ok(QueryStatementFlow::Continue)
        }
        Err(e) => {
            send_queued_notices(stream)?;
            send_exec_error(stream, &sql, &e)?;
            Ok(QueryStatementFlow::Stop)
        }
    }
}

fn execute_streaming_select_statement(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
    select_stmt: &SelectStatement,
) -> io::Result<QueryStatementFlow> {
    clear_backend_notices();
    clear_notices();
    match state.session.execute_streaming(db, select_stmt) {
        Ok(mut guard) => {
            use crate::backend::executor::exec_next;
            let mut columns = guard.columns.clone();
            let catalog = state.session.catalog_lookup(db);
            let role_names = role_name_map(&catalog);
            let relation_names = relation_name_map(&catalog);
            let proc_names = proc_name_map(&catalog);
            let namespace_names = namespace_name_map(&catalog);
            annotate_query_columns_with_wire_type_oids(&mut columns, &catalog);
            let mut row_buf = Vec::new();
            let mut row_count = 0usize;
            let mut header_sent = false;
            let mut err = None;

            loop {
                match exec_next(&mut guard.state, &mut guard.ctx) {
                    Ok(Some(slot)) => {
                        if !header_sent {
                            send_row_description(stream, &columns)?;
                            header_sent = true;
                        }
                        match slot.values() {
                            Ok(values) => {
                                send_typed_data_row(
                                    stream,
                                    values,
                                    &columns,
                                    &[],
                                    &mut row_buf,
                                    FloatFormatOptions {
                                        extra_float_digits: state.session.extra_float_digits(),
                                        bytea_output: state.session.bytea_output(),
                                        datetime_config: state.session.datetime_config().clone(),
                                    },
                                    Some(&role_names),
                                    Some(&relation_names),
                                    Some(&proc_names),
                                    Some(&namespace_names),
                                )?;
                                row_count += 1;
                            }
                            Err(e) => {
                                err = Some(e);
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            drop(guard);

            if let Some(e) = err {
                send_queued_notices(stream)?;
                send_exec_error(stream, sql, &e)?;
                return Ok(QueryStatementFlow::Stop);
            }

            flush_pending_backend_messages(stream, db, &state.session)?;
            if !header_sent {
                send_row_description(stream, &columns)?;
            }
            send_command_complete(stream, &format!("SELECT {row_count}"))?;
            Ok(QueryStatementFlow::Continue)
        }
        Err(e) => {
            send_queued_notices(stream)?;
            send_exec_error(stream, sql, &e)?;
            Ok(QueryStatementFlow::Stop)
        }
    }
}

fn split_simple_query_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut block_comment_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut line_comment = false;
    let mut dollar_quote: Option<String> = None;

    while i < bytes.len() {
        if line_comment {
            if bytes[i] == b'\n' {
                line_comment = false;
            }
            i += 1;
            continue;
        }
        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(tag) = &dollar_quote {
            if sql[i..].starts_with(tag) {
                i += tag.len();
                dollar_quote = None;
            } else {
                i += 1;
            }
            continue;
        }
        if single_quote {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if double_quote {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    double_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            single_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            double_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'$' {
            if let Some(tag_end) = sql[i + 1..].find('$') {
                let delimiter = &sql[i..=i + 1 + tag_end];
                if delimiter[1..delimiter.len() - 1]
                    .chars()
                    .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
                {
                    dollar_quote = Some(delimiter.to_string());
                    i += delimiter.len();
                    continue;
                }
            }
        }
        if bytes[i] == b'(' {
            paren_depth += 1;
            i += 1;
            continue;
        }
        if bytes[i] == b')' {
            paren_depth = paren_depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if bytes[i] == b';' && paren_depth == 0 {
            statements.push(&sql[start..=i]);
            start = i + 1;
        }
        i += 1;
    }

    if start < sql.len() {
        statements.push(&sql[start..]);
    }
    statements
}

fn role_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .materialize_visible_catalog()
        .map(|visible| {
            visible
                .authid_rows()
                .into_iter()
                .map(|row| (row.oid, row.rolname))
                .collect()
        })
        .unwrap_or_default()
}

fn proc_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .materialize_visible_catalog()
        .map(|visible| {
            visible
                .proc_rows()
                .into_iter()
                .map(|row| (row.oid, row.proname))
                .collect()
        })
        .unwrap_or_else(|| {
            crate::include::catalog::bootstrap_pg_proc_rows()
                .into_iter()
                .map(|row| (row.oid, row.proname))
                .collect()
        })
}

fn relation_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .materialize_visible_catalog()
        .map(|visible| {
            visible
                .relcache()
                .entries()
                .map(|(name, entry)| {
                    let relname = name
                        .rsplit_once('.')
                        .map(|(_, relname)| relname)
                        .unwrap_or(name)
                        .to_string();
                    (entry.relation_oid, relname)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn namespace_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .materialize_visible_catalog()
        .map(|visible| {
            visible
                .namespace_rows()
                .into_iter()
                .map(|row| (row.oid, row.nspname))
                .collect()
        })
        .unwrap_or_default()
}

fn try_handle_psql_describe_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<bool> {
    let Some((columns, rows)) = execute_psql_describe_query(db, &state.session, sql) else {
        return Ok(false);
    };
    let catalog = state.session.catalog_lookup(db);
    let role_names = role_name_map(&catalog);
    let relation_names = relation_name_map(&catalog);
    let proc_names = proc_name_map(&catalog);
    let namespace_names = namespace_name_map(&catalog);
    send_query_result(
        stream,
        &columns,
        &rows,
        &format!("SELECT {}", rows.len()),
        FloatFormatOptions {
            extra_float_digits: state.session.extra_float_digits(),
            bytea_output: state.session.bytea_output(),
            datetime_config: state.session.datetime_config().clone(),
        },
        Some(&role_names),
        Some(&relation_names),
        Some(&proc_names),
        Some(&namespace_names),
    )?;
    Ok(true)
}

fn execute_psql_describe_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    // :HACK: psql's `\d bit_defaults` emits a long chain of catalog-heavy
    // describe queries. We short-circuit the specific shapes bit.sql needs
    // instead of implementing LEFT JOIN, format_type, regex operators,
    // COLLATE, publications, inheritance footers, and related describe-only
    // catalog features in the main SQL engine.
    let lower = sql.to_ascii_lowercase();
    if lower.contains("from pg_catalog.pg_class c")
        && lower.contains("left join pg_catalog.pg_namespace n on n.oid = c.relnamespace")
        && lower.contains("operator(pg_catalog.~)")
        && lower.contains("pg_catalog.pg_table_is_visible(c.oid)")
    {
        return Some(psql_describe_lookup_query(db, session, sql));
    }
    if lower.starts_with("select c.relchecks, c.relkind, c.relhasindex")
        && lower.contains("from pg_catalog.pg_class c")
        && lower.contains("where c.oid = '")
    {
        return psql_describe_tableinfo_query(db, session, sql);
    }
    if lower.starts_with("select a.attname")
        && lower.contains("pg_catalog.format_type(a.atttypid, a.atttypmod)")
        && lower.contains("from pg_catalog.pg_attribute a")
        && lower.contains("where a.attrelid = '")
    {
        return psql_describe_columns_query(db, session, sql);
    }
    if lower.starts_with("select c2.relname, i.indisprimary, i.indisunique")
        && lower.contains("pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)")
        && lower
            .contains("from pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i")
    {
        return psql_describe_indexes_query(db, session, sql);
    }
    if lower.contains("from pg_catalog.pg_constraint")
        && lower.contains("pg_get_constraintdef")
        && lower.contains("conrelid")
    {
        return psql_describe_constraints_query(db, session, sql);
    }
    if lower.starts_with("select pg_catalog.pg_get_viewdef(") && lower.contains("::pg_catalog.oid")
    {
        return psql_get_viewdef_query(db, session, sql);
    }
    if (lower.starts_with("select col_description(")
        || lower.starts_with("select pg_catalog.col_description("))
        && lower.contains("::regclass")
    {
        return psql_col_description_query(db, session, sql);
    }
    if lower.starts_with("select indexrelid::regclass::text as index")
        && lower.contains("obj_description(indexrelid, 'pg_class')")
        && lower.contains("from pg_index")
    {
        return psql_index_obj_description_query(db, session, sql);
    }
    if lower.contains("obj_description(oid, 'pg_constraint')")
        && lower.contains("from pg_constraint")
    {
        return psql_constraint_obj_description_query(db, session, sql);
    }
    if lower.starts_with("select relname,")
        && lower.contains("obj_description(c.oid, 'pg_class')")
        && lower.contains("from pg_class c left join old_oids using (relname)")
    {
        return psql_relation_obj_description_query(db, session, sql);
    }
    if lower.contains("from pg_catalog.pg_policy pol") && lower.contains("pol.polroles") {
        return Some((vec![QueryColumn::text("Policies")], Vec::new()));
    }
    if lower.contains("pg_catalog.pg_statistic_ext")
        && lower.contains("stxrelid")
        && lower.contains("stxname")
    {
        return psql_describe_statistics_query(db, session, sql);
    }
    if lower.contains("from pg_catalog.pg_class c, pg_catalog.pg_inherits i")
        && lower.contains("::pg_catalog.regclass")
    {
        let columns = if lower.contains("c.relkind") {
            vec![
                QueryColumn::text("regclass"),
                QueryColumn::text("relkind"),
                QueryColumn::text("inhdetachpending"),
                QueryColumn::text("pg_get_expr"),
            ]
        } else {
            vec![QueryColumn::text("regclass")]
        };
        return Some((
            columns,
            psql_describe_inherits_query_rows(db, session, sql, lower.contains("c.relkind")),
        ));
    }
    None
}

fn psql_describe_inherits_query_rows(
    db: &Database,
    session: &Session,
    sql: &str,
    include_relkind: bool,
) -> Vec<Vec<Value>> {
    let lower = sql.to_ascii_lowercase();
    let txn_ctx = session.catalog_txn_ctx();
    let search_path = session.configured_search_path();
    let catalog = session.catalog_lookup(db);

    let inherits = if lower.contains("i.inhrelid =") {
        let Some(oid) = extract_single_quoted_literal_after(sql, "i.inhrelid =")
            .and_then(|value| value.parse::<u32>().ok())
        else {
            return Vec::new();
        };
        catalog.inheritance_parents(oid)
    } else if lower.contains("i.inhparent =") {
        let Some(oid) = extract_single_quoted_literal_after(sql, "i.inhparent =")
            .and_then(|value| value.parse::<u32>().ok())
        else {
            return Vec::new();
        };
        catalog.inheritance_children(oid)
    } else {
        return Vec::new();
    };

    let mut inherits = inherits;
    inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    inherits
        .into_iter()
        .filter_map(|row| {
            let oid = if lower.contains("i.inhrelid =") {
                row.inhparent
            } else {
                row.inhrelid
            };
            let relation = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
            let name = db
                .relation_display_name(session.client_id, txn_ctx, search_path.as_deref(), oid)
                .unwrap_or_else(|| oid.to_string());
            if include_relkind {
                Some(vec![
                    Value::Text(name.into()),
                    Value::InternalChar(relation.relkind as u8),
                    Value::Bool(row.inhdetachpending),
                    Value::Text(String::new().into()),
                ])
            } else {
                Some(vec![Value::Text(name.into())])
            }
        })
        .collect()
}

fn psql_describe_statistics_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let relation_oid = extract_single_quoted_literal_after(sql, "where stxrelid =")?
        .parse::<u32>()
        .ok()?;
    let txn_ctx = session.catalog_txn_ctx();
    let search_path = session.configured_search_path();
    let catalog = session.catalog_lookup(db);
    let mut rows = catalog
        .statistic_ext_rows_for_relation(relation_oid)
        .into_iter()
        .filter_map(|row| {
            let relation_name = db.relation_display_name(
                session.client_id,
                txn_ctx,
                search_path.as_deref(),
                row.stxrelid,
            )?;
            let schema_name = catalog.namespace_row_by_oid(row.stxnamespace)?.nspname;
            let columns = statistics_row_columns_text(&catalog, &row)?;
            Some(vec![
                Value::Int32(row.oid as i32),
                Value::Text(relation_name.into()),
                Value::Text(schema_name.into()),
                Value::Text(row.stxname.clone().into()),
                Value::Text(columns.into()),
                Value::Bool(statistics_row_kind_enabled(&row, b'd')),
                Value::Bool(statistics_row_kind_enabled(&row, b'f')),
                Value::Bool(statistics_row_kind_enabled(&row, b'm')),
                row.stxstattarget.map_or(Value::Null, Value::Int16),
            ])
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        let left_schema = match &left[2] {
            Value::Text(value) => value.as_str(),
            _ => "",
        };
        let right_schema = match &right[2] {
            Value::Text(value) => value.as_str(),
            _ => "",
        };
        let left_name = match &left[3] {
            Value::Text(value) => value.as_str(),
            _ => "",
        };
        let right_name = match &right[3] {
            Value::Text(value) => value.as_str(),
            _ => "",
        };
        left_schema
            .cmp(right_schema)
            .then_with(|| left_name.cmp(right_name))
    });
    Some((
        vec![
            QueryColumn::text("oid"),
            QueryColumn::text("stxrelid"),
            QueryColumn::text("nsp"),
            QueryColumn::text("stxname"),
            QueryColumn::text("columns"),
            QueryColumn::text("ndist_enabled"),
            QueryColumn::text("deps_enabled"),
            QueryColumn::text("mcv_enabled"),
            QueryColumn::text("stxstattarget"),
        ],
        rows,
    ))
}

fn try_handle_statistics_catalog_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<bool> {
    let Some((columns, rows)) = execute_statistics_catalog_query(db, &state.session, sql) else {
        return Ok(false);
    };
    let catalog = state.session.catalog_lookup(db);
    let role_names = role_name_map(&catalog);
    let relation_names = relation_name_map(&catalog);
    let proc_names = proc_name_map(&catalog);
    let namespace_names = namespace_name_map(&catalog);
    send_query_result(
        stream,
        &columns,
        &rows,
        &format!("SELECT {}", rows.len()),
        FloatFormatOptions {
            extra_float_digits: state.session.extra_float_digits(),
            bytea_output: state.session.bytea_output(),
            datetime_config: state.session.datetime_config().clone(),
        },
        Some(&role_names),
        Some(&relation_names),
        Some(&proc_names),
        Some(&namespace_names),
    )?;
    Ok(true)
}

fn execute_statistics_catalog_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("from pg_statistic_ext s left join pg_statistic_ext_data d")
        && lower.contains("where s.stxname =")
    {
        return statistics_object_data_query(session, db, sql);
    }
    if lower.contains("from pg_statistic_ext s, pg_statistic_ext_data d")
        || lower.contains("from pg_statistic_ext s join pg_statistic_ext_data d")
    {
        return Some(statistics_catalog_empty_result(sql));
    }
    if lower.contains("from pg_statistic_ext ")
        || lower.contains("from pg_statistic_ext s")
        || lower.contains("from pg_statistic_ext_data ")
        || lower.contains("from pg_statistic_ext_data d")
    {
        return Some(statistics_catalog_empty_result(sql));
    }
    let _ = session;
    None
}

fn statistics_object_data_query(
    session: &Session,
    db: &Database,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let name = extract_single_quoted_literal_after(sql, "where s.stxname =")?;
    let catalog = session.catalog_lookup(db);
    let rows = catalog
        .statistic_ext_rows()
        .into_iter()
        .filter(|row| row.stxname.eq_ignore_ascii_case(&name))
        .map(|row| {
            vec![
                Value::Text(row.stxname.into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]
        })
        .collect::<Vec<_>>();
    Some((
        vec![
            QueryColumn::text("stxname"),
            QueryColumn::text("stxdndistinct"),
            QueryColumn::text("stxddependencies"),
            QueryColumn::text("stxdmcv"),
            QueryColumn::text("stxdinherit"),
        ],
        rows,
    ))
}

fn statistics_catalog_empty_result(sql: &str) -> (Vec<QueryColumn>, Vec<Vec<Value>>) {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("select stxname, stxdndistinct, stxddependencies, stxdmcv, stxdinherit") {
        return (
            vec![
                QueryColumn::text("stxname"),
                QueryColumn::text("stxdndistinct"),
                QueryColumn::text("stxddependencies"),
                QueryColumn::text("stxdmcv"),
                QueryColumn::text("stxdinherit"),
            ],
            Vec::new(),
        );
    }
    (vec![QueryColumn::text("?column?")], Vec::new())
}

fn split_qualified_statistics_name(name: &str) -> (&str, &str) {
    name.rsplit_once('.')
        .map(|(schema, base)| (schema, base))
        .unwrap_or(("public", name))
}

fn statistics_row_kind_enabled(row: &crate::include::catalog::PgStatisticExtRow, kind: u8) -> bool {
    row.stxkind.is_empty() || row.stxkind.contains(&kind)
}

fn statistics_row_columns_text(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgStatisticExtRow,
) -> Option<String> {
    let relation = catalog.relation_by_oid(row.stxrelid)?;
    let mut items = Vec::new();
    for key in &row.stxkeys {
        let attr_index = usize::try_from(key.saturating_sub(1)).ok()?;
        let column = relation.desc.columns.get(attr_index)?;
        items.push(column.name.to_string());
    }
    if let Some(exprs) = row.stxexprs.as_deref() {
        items.extend(serde_json::from_str::<Vec<String>>(exprs).ok()?);
    }
    Some(items.join(", "))
}

fn extract_single_quoted_literal_after<'a>(sql: &'a str, needle: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let start = lower.find(needle)? + needle.len();
    let tail = sql.get(start..)?.trim_start();
    let tail = tail.strip_prefix('\'')?;
    let end = tail.find('\'')?;
    Some(tail[..end].to_string())
}

fn psql_describe_lookup_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> (Vec<QueryColumn>, Vec<Vec<Value>>) {
    let catalog = session.catalog_lookup(db);
    let txn_ctx = session.catalog_txn_ctx();
    let search_path = session.configured_search_path();
    let relation_name = extract_psql_pattern_name(sql);
    let rows = relation_name
        .and_then(|name| catalog.lookup_any_relation(name).map(|entry| (name, entry)))
        .map(|(name, entry)| {
            let nspname = db
                .relation_namespace_name(session.client_id, txn_ctx, entry.relation_oid)
                .or_else(|| name.split_once('.').map(|(schema, _)| schema.to_string()))
                .unwrap_or_else(|| "public".to_string());
            let relname = db
                .relation_display_name(
                    session.client_id,
                    txn_ctx,
                    search_path.as_deref(),
                    entry.relation_oid,
                )
                .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name).to_string());
            vec![vec![
                Value::Int32(entry.relation_oid as i32),
                Value::Text(nspname.into()),
                Value::Text(
                    relname
                        .rsplit('.')
                        .next()
                        .unwrap_or(relname.as_str())
                        .to_string()
                        .into(),
                ),
            ]]
        })
        .unwrap_or_default();
    (
        vec![
            QueryColumn {
                name: "oid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn::text("nspname"),
            QueryColumn::text("relname"),
        ],
        rows,
    )
}

fn psql_describe_tableinfo_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let txn_ctx = session.catalog_txn_ctx();
    let entry = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
    let relhasindex = db.has_index_on_relation(session.client_id, txn_ctx, oid);
    let amname = db.access_method_name_for_relation(session.client_id, txn_ctx, oid);
    let visible_amname = match entry.relkind {
        // :HACK: psql's verbose \d+ footer only renders a table access method
        // when pg_class.relam points at a non-default AM. pgrust stores the
        // default heap AM directly, so suppress that footer here until the
        // catalog can distinguish explicit from implicit table AM selection.
        'r' | 'p' | 'm' if amname.as_deref() == Some("heap") => None,
        _ => amname,
    };
    Some((
        vec![
            QueryColumn {
                name: "relchecks".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relkind".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relhasindex".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relhasrules".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relhastriggers".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relrowsecurity".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relforcerowsecurity".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relhasoids".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relispartition".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn::text("?column?"),
            QueryColumn {
                name: "reltablespace".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn::text("reloftype"),
            QueryColumn {
                name: "relpersistence".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "relreplident".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
                wire_type_oid: None,
            },
            QueryColumn::text("amname"),
        ],
        vec![vec![
            Value::Int32(0),
            Value::InternalChar(entry.relkind as u8),
            Value::Bool(relhasindex),
            Value::Bool(false),
            Value::Bool(entry.relhastriggers),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Text("".into()),
            Value::Int32(0),
            Value::Text("".into()),
            Value::InternalChar(entry.relpersistence as u8),
            Value::InternalChar(b'd'),
            visible_amname
                .map(|name| Value::Text(name.into()))
                .unwrap_or(Value::Null),
        ]],
    ))
}

fn psql_describe_columns_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let entry = db.describe_relation_by_oid(session.client_id, session.catalog_txn_ctx(), oid)?;
    let lower = sql.to_ascii_lowercase();
    let include_attrdef = lower.contains("pg_catalog.pg_get_expr(d.adbin");
    let include_attnotnull = lower.contains("a.attnotnull");
    let include_attcollation = lower.contains("as attcollation");
    let include_attidentity = lower.contains("attidentity");
    let include_attgenerated = lower.contains("attgenerated");
    let include_is_key = lower.contains("as is_key");
    let include_indexdef = lower.contains("as indexdef");
    let include_attfdwoptions = lower.contains("as attfdwoptions");
    let include_attstorage = lower.contains("a.attstorage");
    let include_attcompression = lower.contains("attcompression");
    let include_attstattarget = lower.contains("attstattarget");
    let include_attdescr = lower.contains("pg_catalog.col_description(");
    let index_display_columns = entry
        .index
        .as_ref()
        .map(|index_meta| psql_index_display_columns(db, session, &entry.desc, index_meta));

    let mut columns = vec![
        QueryColumn::text("attname"),
        QueryColumn::text("format_type"),
    ];
    if include_attrdef {
        columns.push(QueryColumn::text("pg_get_expr"));
    }
    if include_attnotnull {
        columns.push(QueryColumn {
            name: "attnotnull".into(),
            sql_type: SqlType::new(SqlTypeKind::Bool),
            wire_type_oid: None,
        });
    }
    if include_attcollation {
        columns.push(QueryColumn::text("attcollation"));
    }
    if include_attidentity {
        columns.push(QueryColumn {
            name: "attidentity".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
            wire_type_oid: None,
        });
    }
    if include_attgenerated {
        columns.push(QueryColumn {
            name: "attgenerated".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
            wire_type_oid: None,
        });
    }
    if include_is_key {
        columns.push(QueryColumn::text("is_key"));
    }
    if include_indexdef {
        columns.push(QueryColumn::text("indexdef"));
    }
    if include_attfdwoptions {
        columns.push(QueryColumn::text("attfdwoptions"));
    }
    if include_attstorage {
        columns.push(QueryColumn {
            name: "attstorage".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
            wire_type_oid: None,
        });
    }
    if include_attcompression {
        columns.push(QueryColumn {
            name: "attcompression".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
            wire_type_oid: None,
        });
    }
    if include_attstattarget {
        columns.push(QueryColumn {
            name: "attstattarget".into(),
            sql_type: SqlType::new(SqlTypeKind::Int2),
            wire_type_oid: None,
        });
    }
    if include_attdescr {
        columns.push(QueryColumn::text("col_description"));
    }

    let rows = entry
        .desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let index_display = index_display_columns
                .as_ref()
                .and_then(|columns| columns.get(index));
            let index_display_type_oid = entry.index.as_ref().and_then(|index_meta| {
                index_meta
                    .opckeytype_oids
                    .get(index)
                    .copied()
                    .filter(|oid| *oid != 0)
            });
            let mut row = vec![
                Value::Text(
                    index_display
                        .map(|display| display.display_name.clone())
                        .unwrap_or_else(|| column.name.clone())
                        .into(),
                ),
                Value::Text(
                    format_psql_display_type(db, session, column.sql_type, index_display_type_oid)
                        .into(),
                ),
            ];
            if include_attrdef {
                row.push(
                    column
                        .default_expr
                        .as_ref()
                        .map(|expr| {
                            Value::Text(
                                format_psql_default(db, session, column.sql_type, expr).into(),
                            )
                        })
                        .unwrap_or(Value::Null),
                );
            }
            if include_attnotnull {
                row.push(Value::Bool(!column.storage.nullable));
            }
            if include_attcollation {
                row.push(Value::Null);
            }
            if include_attidentity {
                row.push(Value::InternalChar(
                    column
                        .identity
                        .map(|kind| kind.catalog_char() as u8)
                        .unwrap_or(0),
                ));
            }
            if include_attgenerated {
                row.push(Value::InternalChar(
                    column
                        .generated
                        .map(|kind| kind.catalog_char() as u8)
                        .unwrap_or(0),
                ));
            }
            if include_is_key {
                let is_key = entry
                    .index
                    .as_ref()
                    .is_some_and(|index_meta| index < index_meta.indnkeyatts as usize);
                row.push(Value::Text(if is_key { "yes" } else { "no" }.into()));
            }
            if include_indexdef {
                row.push(Value::Text(
                    index_display
                        .map(|display| display.definition.clone())
                        .unwrap_or_else(|| column.name.clone())
                        .into(),
                ));
            }
            if include_attfdwoptions {
                row.push(Value::Text("".into()));
            }
            if include_attstorage {
                row.push(Value::InternalChar(
                    column.storage.attstorage.as_char() as u8
                ));
            }
            if include_attcompression {
                row.push(Value::InternalChar(
                    column.storage.attcompression.as_char() as u8
                ));
            }
            if include_attstattarget {
                row.push(if column.attstattarget < 0 {
                    Value::Null
                } else {
                    Value::Int16(column.attstattarget)
                });
            }
            if include_attdescr {
                row.push(Value::Null);
            }
            row
        })
        .collect::<Vec<_>>();
    Some((columns, rows))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PsqlIndexDisplayColumn {
    display_name: String,
    definition: String,
}

fn psql_index_display_columns(
    db: &Database,
    session: &Session,
    index_desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Vec<PsqlIndexDisplayColumn> {
    let base_relation = db.describe_relation_by_oid(
        session.client_id,
        session.catalog_txn_ctx(),
        index_meta.indrelid,
    );
    let expression_sqls = index_meta
        .indexprs
        .as_deref()
        .and_then(|sql| serde_json::from_str::<Vec<String>>(sql).ok())
        .unwrap_or_default();
    let mut expression_index = 0usize;
    index_meta
        .indkey
        .iter()
        .enumerate()
        .map(|(index, attnum)| {
            if *attnum > 0 {
                let name = base_relation
                    .as_ref()
                    .and_then(|relation| {
                        relation
                            .desc
                            .columns
                            .get((*attnum as usize).saturating_sub(1))
                            .map(|column| column.name.clone())
                    })
                    .or_else(|| {
                        index_desc
                            .columns
                            .get(index)
                            .map(|column| column.name.clone())
                    })
                    .unwrap_or_else(|| format!("column{}", index + 1));
                return PsqlIndexDisplayColumn {
                    display_name: name.clone(),
                    definition: name,
                };
            }
            let expression_sql = expression_sqls
                .get(expression_index)
                .cloned()
                .or_else(|| {
                    index_desc
                        .columns
                        .get(index)
                        .map(|column| column.name.clone())
                })
                .unwrap_or_else(|| format!("expr{}", index + 1));
            expression_index += 1;
            PsqlIndexDisplayColumn {
                display_name: "expr".into(),
                definition: parenthesized_index_expression(&expression_sql),
            }
        })
        .collect()
}

fn parenthesized_index_expression(expr_sql: &str) -> String {
    let trimmed = expr_sql.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        trimmed.to_string()
    } else {
        format!("({trimmed})")
    }
}

fn psql_describe_constraints_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    let oid = extract_constraint_relid(sql).or_else(|| {
        extract_quoted_oid_with_markers(
            sql,
            &[
                "pg_partition_ancestors('",
                "values ('",
                "conrelid = '",
                "confrelid = '",
            ],
        )
    })?;
    let contype_filter = if lower.contains("contype = 'f'") {
        Some(crate::include::catalog::CONSTRAINT_FOREIGN)
    } else if lower.contains("contype = 'c'") {
        Some(crate::include::catalog::CONSTRAINT_CHECK)
    } else if lower.contains("contype = 'p'") {
        Some(crate::include::catalog::CONSTRAINT_PRIMARY)
    } else if lower.contains("contype = 'u'") {
        Some(crate::include::catalog::CONSTRAINT_UNIQUE)
    } else if lower.contains("contype = 'n'") {
        Some(crate::include::catalog::CONSTRAINT_NOTNULL)
    } else {
        None
    };
    let txn_ctx = session.catalog_txn_ctx();
    let include_sametable = lower.contains("as sametable");
    let incoming_refs = lower.contains("where confrelid in")
        || lower.contains("where c.confrelid in")
        || lower.contains("where r.confrelid in")
        || lower.contains("where confrelid = ")
        || lower.contains("where c.confrelid = ")
        || lower.contains("where r.confrelid = ");
    let rows = if incoming_refs {
        crate::backend::utils::cache::syscache::ensure_constraint_rows(
            db,
            session.client_id,
            txn_ctx,
        )
        .into_iter()
        .filter(|row| row.confrelid == oid)
        .filter(|row| contype_filter.is_none_or(|contype| row.contype == contype))
        .filter(|row| !lower.contains("conparentid = 0") || row.conparentid == 0)
        .filter_map(|row| {
            let ontable = db
                .relation_display_name(
                    session.client_id,
                    txn_ctx,
                    session.configured_search_path().as_deref(),
                    row.conrelid,
                )
                .unwrap_or_else(|| row.conrelid.to_string());
            let condef = constraint_def_for_row(db, session, None, &row)?;
            Some(vec![
                Value::Text(row.conname.into()),
                Value::Text(ontable.into()),
                Value::Text(condef.into()),
            ])
        })
        .collect::<Vec<_>>()
    } else {
        let relation = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
        let relname = db
            .relation_display_name(
                session.client_id,
                txn_ctx,
                session.configured_search_path().as_deref(),
                oid,
            )
            .unwrap_or_else(|| oid.to_string());
        db.constraint_rows_for_relation(session.client_id, txn_ctx, oid)
            .into_iter()
            .filter(|row| contype_filter.is_none_or(|contype| row.contype == contype))
            .filter(|row| !lower.contains("conparentid = 0") || row.conparentid == 0)
            .filter_map(|row| {
                let condef = constraint_def_for_row(db, session, Some(&relation), &row)?;
                if include_sametable {
                    Some(vec![
                        Value::Bool(row.conrelid == oid),
                        Value::Text(row.conname.into()),
                        Value::Text(condef.into()),
                        Value::Text(relname.clone().into()),
                    ])
                } else {
                    Some(vec![
                        Value::Text(row.conname.into()),
                        Value::Text(relname.clone().into()),
                        Value::Text(condef.into()),
                    ])
                }
            })
            .collect::<Vec<_>>()
    };
    let mut rows = rows;
    rows.sort_by(|left, right| {
        match (
            left.get(usize::from(include_sametable)),
            right.get(usize::from(include_sametable)),
        ) {
            (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
            _ => std::cmp::Ordering::Equal,
        }
    });
    let columns = if include_sametable {
        vec![
            QueryColumn {
                name: "sametable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn::text("conname"),
            QueryColumn::text("condef"),
            QueryColumn::text("ontable"),
        ]
    } else {
        vec![
            QueryColumn::text("conname"),
            QueryColumn::text("ontable"),
            QueryColumn::text("condef"),
        ]
    };
    Some((columns, rows))
}

fn constraint_def_for_row(
    db: &Database,
    session: &Session,
    relation: Option<&crate::backend::utils::cache::relcache::RelCacheEntry>,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    match row.contype {
        crate::include::catalog::CONSTRAINT_NOTNULL => Some("NOT NULL".to_string()),
        crate::include::catalog::CONSTRAINT_CHECK => row
            .conbin
            .as_deref()
            .map(|expr_sql| format!("CHECK ({expr_sql})")),
        crate::include::catalog::CONSTRAINT_PRIMARY
        | crate::include::catalog::CONSTRAINT_UNIQUE => {
            let relation = relation.cloned().or_else(|| {
                db.describe_relation_by_oid(
                    session.client_id,
                    session.catalog_txn_ctx(),
                    row.conrelid,
                )
            })?;
            index_backed_constraint_def(
                db,
                session.client_id,
                session.catalog_txn_ctx(),
                &relation,
                row,
            )
        }
        crate::include::catalog::CONSTRAINT_FOREIGN => {
            let relation = relation.cloned().or_else(|| {
                db.describe_relation_by_oid(
                    session.client_id,
                    session.catalog_txn_ctx(),
                    row.conrelid,
                )
            })?;
            foreign_key_constraint_def(db, session, &relation, row)
        }
        _ => None,
    }
}

fn index_backed_constraint_def(
    db: &Database,
    client_id: u32,
    txn_ctx: Option<(u32, u32)>,
    relation: &crate::backend::utils::cache::relcache::RelCacheEntry,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let index = db
        .describe_relation_by_oid(client_id, txn_ctx, row.conindid)?
        .index?;
    let mut columns = index
        .indkey
        .iter()
        .map(|attnum| {
            (*attnum > 0)
                .then(|| {
                    relation
                        .desc
                        .columns
                        .get((*attnum as usize).saturating_sub(1))
                })
                .flatten()
                .map(|column| column.name.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    if row.conperiod
        && let Some(period_column) = columns.last_mut()
    {
        period_column.push_str(" WITHOUT OVERLAPS");
    }
    let prefix = if row.contype == crate::include::catalog::CONSTRAINT_PRIMARY {
        "PRIMARY KEY"
    } else {
        "UNIQUE"
    };
    Some(format!("{prefix} ({})", columns.join(", ")))
}

fn foreign_key_constraint_def(
    db: &Database,
    session: &Session,
    relation: &crate::backend::utils::cache::relcache::RelCacheEntry,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let local_columns = row
        .conkey
        .as_ref()?
        .iter()
        .map(|attnum| {
            (*attnum > 0)
                .then(|| {
                    relation
                        .desc
                        .columns
                        .get((*attnum as usize).saturating_sub(1))
                })
                .flatten()
                .map(|column| column.name.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    let referenced_relation =
        db.describe_relation_by_oid(session.client_id, session.catalog_txn_ctx(), row.confrelid)?;
    let referenced_relation_name = db.relation_display_name(
        session.client_id,
        session.catalog_txn_ctx(),
        None,
        row.confrelid,
    )?;
    let referenced_columns = row
        .confkey
        .as_ref()?
        .iter()
        .map(|attnum| {
            (*attnum > 0)
                .then(|| {
                    referenced_relation
                        .desc
                        .columns
                        .get((*attnum as usize).saturating_sub(1))
                })
                .flatten()
                .map(|column| column.name.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    let mut def = format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        local_columns.join(", "),
        referenced_relation_name,
        referenced_columns.join(", ")
    );
    if row.confdeltype == 'r' {
        def.push_str(" ON DELETE RESTRICT");
    }
    if row.confupdtype == 'r' {
        def.push_str(" ON UPDATE RESTRICT");
    }
    Some(def)
}

fn psql_describe_indexes_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let txn_ctx = session.catalog_txn_ctx();
    let relation = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
    let constraints = db.constraint_rows_for_relation(session.client_id, txn_ctx, oid);
    let mut rows = session
        .catalog_lookup(db)
        .index_relations_for_heap(oid)
        .into_iter()
        .map(|index| {
            let constraint = constraints.iter().find(|row| {
                row.conindid == index.relation_oid && matches!(row.contype, 'p' | 'u' | 'x')
            });
            let condef = constraint
                .and_then(|row| constraint_def_for_row(db, session, Some(&relation), row))
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null);
            let contype = constraint
                .map(|row| Value::InternalChar(row.contype as u8))
                .unwrap_or(Value::Null);
            let condeferrable = constraint
                .map(|row| Value::Bool(row.condeferrable))
                .unwrap_or(Value::Null);
            let condeferred = constraint
                .map(|row| Value::Bool(row.condeferred))
                .unwrap_or(Value::Null);
            vec![
                Value::Text(index.name.clone().into()),
                Value::Bool(index.index_meta.indisprimary),
                Value::Bool(index.index_meta.indisunique),
                Value::Bool(index.index_meta.indisclustered),
                Value::Bool(index.index_meta.indisvalid),
                Value::Text(format_psql_indexdef(db, session, &index).into()),
                condef,
                contype,
                condeferrable,
                condeferred,
                Value::Bool(index.index_meta.indisreplident),
                Value::Int32(0),
                constraint
                    .map(|row| Value::Bool(row.conperiod))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        let left_primary = matches!(left.get(1), Some(Value::Bool(true)));
        let right_primary = matches!(right.get(1), Some(Value::Bool(true)));
        right_primary
            .cmp(&left_primary)
            .then_with(|| match (left.first(), right.first()) {
                (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
                _ => std::cmp::Ordering::Equal,
            })
    });
    Some((
        vec![
            QueryColumn::text("relname"),
            QueryColumn {
                name: "indisprimary".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "indisunique".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "indisclustered".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "indisvalid".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn::text("pg_get_indexdef"),
            QueryColumn::text("pg_get_constraintdef"),
            QueryColumn {
                name: "contype".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "condeferrable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "condeferred".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "indisreplident".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "reltablespace".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "conperiod".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
        ],
        rows,
    ))
}

fn psql_get_viewdef_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid_with_markers(sql, &["pg_get_viewdef('"])?;
    let catalog = session.catalog_lookup(db);
    let value = catalog
        .lookup_relation_by_oid(oid)
        .filter(|relation| relation.relkind == 'v')
        .and_then(|relation| format_view_definition(oid, &relation.desc, &catalog).ok())
        .map(|definition| Value::Text(definition.into()))
        .unwrap_or(Value::Null);
    Some((vec![QueryColumn::text("pg_get_viewdef")], vec![vec![value]]))
}

fn psql_col_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let relation = extract_quoted_literal_with_markers(
        sql,
        &["col_description('", "pg_catalog.col_description('"],
    )?;
    let attnum = extract_col_description_attnum(sql)?;
    let relation_oid = resolve_regclass_literal(db, session, relation)?;
    let comment = catalog_description_value(
        db,
        session,
        relation_oid,
        crate::include::catalog::PG_CLASS_RELATION_OID,
        attnum,
    );
    Some((vec![QueryColumn::text("comment")], vec![vec![comment]]))
}

fn psql_index_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let relation = extract_quoted_literal_with_markers(sql, &["where indrelid = '"])?;
    let relation_oid = resolve_regclass_literal(db, session, relation)?;
    let mut rows = session
        .catalog_lookup(db)
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .map(|index| {
            vec![
                Value::Text(index.name.into()),
                catalog_description_value(
                    db,
                    session,
                    index.relation_oid,
                    crate::include::catalog::PG_CLASS_RELATION_OID,
                    0,
                ),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| match (left.first(), right.first()) {
        (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    });
    Some((
        vec![QueryColumn::text("index"), QueryColumn::text("comment")],
        rows,
    ))
}

fn psql_constraint_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    let value_column = if lower.contains(" as desc") {
        "desc"
    } else {
        "comment"
    };
    if let Some(relation) = extract_quoted_literal_with_markers(sql, &["where conrelid = '"]) {
        let relation_oid = resolve_regclass_literal(db, session, relation)?;
        let mut rows = db
            .constraint_rows_for_relation(
                session.client_id,
                session.catalog_txn_ctx(),
                relation_oid,
            )
            .into_iter()
            .map(|row| {
                vec![
                    Value::Text(row.conname.into()),
                    catalog_description_value(
                        db,
                        session,
                        row.oid,
                        crate::include::catalog::PG_CONSTRAINT_RELATION_OID,
                        0,
                    ),
                ]
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| match (left.first(), right.first()) {
            (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
            _ => std::cmp::Ordering::Equal,
        });
        return Some((
            vec![
                QueryColumn::text("constraint"),
                QueryColumn::text(value_column),
            ],
            rows,
        ));
    }
    let pattern = extract_quoted_literal_with_markers(sql, &["where conname like '"])?;
    let helper_sql = format!(
        "select oid, conname from pg_constraint where conname like '{}' order by conname",
        sql_quote_literal(pattern)
    );
    let rows = query_rows_with_search_path(db, session, &helper_sql)?
        .into_iter()
        .filter_map(|row| {
            let oid = value_as_u32(row.first()?)?;
            let conname = value_as_text(row.get(1)?)?;
            Some(vec![
                Value::Text(conname.into()),
                catalog_description_value(
                    db,
                    session,
                    oid,
                    crate::include::catalog::PG_CONSTRAINT_RELATION_OID,
                    0,
                ),
            ])
        })
        .collect::<Vec<_>>();
    Some((
        vec![
            QueryColumn::text("conname"),
            QueryColumn::text(value_column),
        ],
        rows,
    ))
}

fn psql_relation_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let pattern = extract_quoted_literal_with_markers(sql, &["where relname like '"])?;
    let current_sql = format!(
        "select relname, oid, relfilenode from pg_class where relname like '{}' order by relname",
        sql_quote_literal(pattern)
    );
    let current_rows = query_rows_with_search_path(db, session, &current_sql)?;
    let old_rows = query_rows_with_search_path(
        db,
        session,
        "select relname, oldoid, oldfilenode from old_oids order by relname",
    )
    .unwrap_or_default();
    let old_rows = old_rows
        .into_iter()
        .filter_map(|row| {
            Some((
                value_as_text(row.first()?)?,
                (
                    row.get(1).and_then(value_as_u32),
                    row.get(2).and_then(value_as_u32),
                ),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let rows = current_rows
        .into_iter()
        .filter_map(|row| {
            let relname = value_as_text(row.first()?)?;
            let oid = value_as_u32(row.get(1)?)?;
            let relfilenode = value_as_u32(row.get(2)?)?;
            let (oldoid, oldfilenode) = old_rows.get(&relname).cloned().unwrap_or((None, None));
            let orig_oid = oldoid
                .map(|oldoid| Value::Bool(oldoid == oid))
                .unwrap_or(Value::Null);
            let storage = if relfilenode == 0 {
                "none"
            } else if relfilenode == oid {
                "own"
            } else if Some(relfilenode) == oldfilenode {
                "orig"
            } else {
                "OTHER"
            };
            Some(vec![
                Value::Text(relname.into()),
                orig_oid,
                Value::Text(storage.into()),
                catalog_description_value(
                    db,
                    session,
                    oid,
                    crate::include::catalog::PG_CLASS_RELATION_OID,
                    0,
                ),
            ])
        })
        .collect::<Vec<_>>();
    Some((
        vec![
            QueryColumn::text("relname"),
            QueryColumn {
                name: "orig_oid".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
            QueryColumn::text("storage"),
            QueryColumn::text("desc"),
        ],
        rows,
    ))
}

pub(crate) fn format_psql_indexdef(
    db: &Database,
    session: &Session,
    index: &crate::backend::parser::BoundIndexRelation,
) -> String {
    let txn_ctx = session.catalog_txn_ctx();
    let table_name = db
        .relation_display_name(
            session.client_id,
            txn_ctx,
            session.configured_search_path().as_deref(),
            index.index_meta.indrelid,
        )
        .unwrap_or_else(|| index.index_meta.indrelid.to_string());
    let amname = db
        .access_method_name_for_relation(session.client_id, txn_ctx, index.relation_oid)
        .unwrap_or_else(|| "btree".to_string());
    let only = db
        .describe_relation_by_oid(session.client_id, txn_ctx, index.relation_oid)
        .filter(|relation| relation.relkind == 'I')
        .map(|_| " ONLY")
        .unwrap_or("");
    let column_names = psql_index_display_columns(db, session, &index.desc, &index.index_meta)
        .into_iter()
        .map(|column| column.definition)
        .collect::<Vec<_>>();
    let unique = if index.index_meta.indisunique {
        "UNIQUE "
    } else {
        ""
    };
    let mut definition = format!(
        "CREATE {unique}INDEX {} ON{only} {} USING {} ({})",
        index.name,
        table_name,
        amname,
        column_names.join(", ")
    );
    if let Some(predicate) = index
        .index_meta
        .indpred
        .as_deref()
        .filter(|pred| !pred.is_empty())
    {
        definition.push_str(" WHERE (");
        definition.push_str(predicate);
        definition.push(')');
    }
    definition
}

fn extract_psql_pattern_name(sql: &str) -> Option<&str> {
    let marker = "operator(pg_catalog.~) '";
    let lower = sql.to_ascii_lowercase();
    let start = lower.find(marker)? + marker.len();
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    let pattern = &rest[..end];
    pattern.strip_prefix("^(")?.strip_suffix(")$")
}

fn extract_quoted_oid(sql: &str) -> Option<u32> {
    let lower = sql.to_ascii_lowercase();
    let marker = "where c.oid = '";
    let alt_marker = "where a.attrelid = '";
    let start = lower
        .find(marker)
        .map(|idx| idx + marker.len())
        .or_else(|| lower.find(alt_marker).map(|idx| idx + alt_marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    rest[..end].parse::<u32>().ok()
}

fn extract_constraint_relid(sql: &str) -> Option<u32> {
    extract_quoted_oid_with_markers(
        sql,
        &[
            "where c.conrelid = '",
            "where r.conrelid = '",
            "and c.conrelid = '",
            "and r.conrelid = '",
            "where conrelid = '",
            "and conrelid = '",
            "where c.confrelid = '",
            "where r.confrelid = '",
            "and c.confrelid = '",
            "and r.confrelid = '",
            "where confrelid = '",
            "and confrelid = '",
        ],
    )
}

fn extract_quoted_literal_with_markers<'a>(sql: &'a str, markers: &[&str]) -> Option<&'a str> {
    let lower = sql.to_ascii_lowercase();
    let start = markers
        .iter()
        .find_map(|marker| lower.find(marker).map(|idx| idx + marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

fn extract_quoted_oid_with_markers(sql: &str, markers: &[&str]) -> Option<u32> {
    extract_quoted_literal_with_markers(sql, markers)?
        .parse::<u32>()
        .ok()
}

fn extract_col_description_attnum(sql: &str) -> Option<i32> {
    let lower = sql.to_ascii_lowercase();
    let marker = lower
        .find("::pg_catalog.regclass,")
        .map(|idx| idx + "::pg_catalog.regclass,".len())
        .or_else(|| {
            lower
                .find("::regclass,")
                .map(|idx| idx + "::regclass,".len())
        })?;
    let rest = sql[marker..].trim_start();
    let end = rest.find(')')?;
    rest[..end].trim().parse::<i32>().ok()
}

fn resolve_regclass_literal(db: &Database, session: &Session, literal: &str) -> Option<u32> {
    literal.parse::<u32>().ok().or_else(|| {
        session
            .catalog_lookup(db)
            .lookup_any_relation(literal)
            .map(|entry| entry.relation_oid)
    })
}

fn query_rows_with_search_path(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<Vec<Vec<Value>>> {
    match db
        .execute_with_search_path(
            session.client_id,
            sql,
            session.configured_search_path().as_deref(),
        )
        .ok()?
    {
        StatementResult::Query { rows, .. } => Some(rows),
        _ => None,
    }
}

fn catalog_description_value(
    db: &Database,
    session: &Session,
    objoid: u32,
    classoid: u32,
    objsubid: i32,
) -> Value {
    let sql = format!(
        "select description from pg_description where objoid = {objoid} and classoid = {classoid} and objsubid = {objsubid}"
    );
    query_rows_with_search_path(db, session, &sql)
        .and_then(|mut rows| rows.pop())
        .and_then(|mut row| row.pop())
        .unwrap_or(Value::Null)
}

fn value_as_u32(value: &Value) -> Option<u32> {
    match value {
        Value::Int16(value) => (*value >= 0).then_some(*value as u32),
        Value::Int32(value) => (*value >= 0).then_some(*value as u32),
        Value::Int64(value) => (*value >= 0).then_some(*value as u32),
        Value::Text(value) => value.parse::<u32>().ok(),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.to_string()),
        _ => None,
    }
}

fn sql_quote_literal(value: &str) -> String {
    value.replace('\'', "''")
}

const CSTRING_TYPE_OID: u32 = 2275;

fn format_psql_display_type(
    db: &Database,
    session: &Session,
    fallback_sql_type: SqlType,
    display_type_oid: Option<u32>,
) -> String {
    match display_type_oid {
        Some(CSTRING_TYPE_OID) => "cstring".into(),
        Some(type_oid) => session
            .catalog_lookup(db)
            .type_by_oid(type_oid)
            .map(|row| format_psql_type(row.sql_type))
            .unwrap_or_else(|| format_psql_type(fallback_sql_type)),
        None => format_psql_type(fallback_sql_type),
    }
}

fn format_psql_type(sql_type: SqlType) -> String {
    match sql_type.kind {
        SqlTypeKind::Bit => format!("bit({})", sql_type.bit_len().unwrap_or(1)),
        SqlTypeKind::VarBit => match sql_type.bit_len() {
            Some(len) => format!("bit varying({len})"),
            None => "bit varying".into(),
        },
        SqlTypeKind::Varchar => match sql_type.char_len() {
            Some(len) => format!("character varying({len})"),
            None => "character varying".into(),
        },
        SqlTypeKind::Char => format!("character({})", sql_type.char_len().unwrap_or(1)),
        _ => format_sql_type_name(sql_type).into(),
    }
}

fn format_psql_default(
    db: &Database,
    session: &Session,
    sql_type: SqlType,
    expr_sql: &str,
) -> String {
    if let Some(rendered) = format_regclass_nextval_default(db, session, sql_type, expr_sql) {
        return rendered;
    }
    if let Ok(expr) = parse_expr(expr_sql) {
        if let crate::backend::parser::SqlExpr::Const(Value::Bit(bits)) = expr {
            return format!("'{}'::\"bit\"", bits.render());
        }
    }
    match sql_type.kind {
        SqlTypeKind::VarBit => format!("{expr_sql}::bit varying"),
        SqlTypeKind::Bit => format!("{expr_sql}::\"bit\""),
        _ => expr_sql.to_string(),
    }
}

fn format_regclass_nextval_default(
    db: &Database,
    session: &Session,
    sql_type: SqlType,
    expr_sql: &str,
) -> Option<String> {
    if !matches!(
        sql_type.kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
    ) {
        return None;
    }
    let oid = parse_nextval_relation_oid(expr_sql)?;
    let relation_name = db.relation_display_name(
        session.client_id,
        session.catalog_txn_ctx(),
        session.configured_search_path().as_deref(),
        oid,
    )?;
    Some(format!(
        "nextval({}::regclass)",
        quote_sql_string(&relation_name)
    ))
}

fn parse_nextval_relation_oid(expr_sql: &str) -> Option<u32> {
    let expr_sql = expr_sql.trim();
    let rest = expr_sql.strip_prefix("nextval(")?;
    let close = rest.find(')')?;
    let oid = rest[..close].trim().parse().ok()?;
    let trailing = rest[close + 1..].trim();
    if trailing.is_empty() || trailing.starts_with("::") {
        Some(oid)
    } else {
        None
    }
}

fn handle_copy_data(state: &mut ConnectionState, body: &[u8]) -> io::Result<()> {
    let Some(copy) = state.copy_in.as_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received CopyData outside copy-in mode",
        ));
    };
    copy.pending.extend_from_slice(body);
    Ok(())
}

fn handle_copy_done(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
) -> io::Result<()> {
    let Some(copy) = state.copy_in.take() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received CopyDone outside copy-in mode",
        ));
    };

    let text = String::from_utf8_lossy(&copy.pending);
    let rows = text
        .lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.is_empty() && *line != "\\.")
        .map(|line| {
            line.split('\t')
                .map(|part| part.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let copy_sql = if let Some(columns) = &copy.columns {
        format!(
            "copy {} ({}) from stdin",
            copy.table_name,
            columns.join(", ")
        )
    } else {
        format!("copy {} from stdin", copy.table_name)
    };
    if let Err(e) = state.session.copy_from_rows_into_with_null_marker(
        db,
        &copy.table_name,
        copy.columns.as_deref(),
        &rows,
        &copy.null_marker,
    ) {
        send_exec_error(stream, &copy_sql, &e)?;
        send_ready_with_pending_messages(stream, db, &state.session)?;
        return Ok(());
    }

    flush_pending_backend_messages(stream, db, &state.session)?;
    send_command_complete(stream, "COPY")?;
    send_ready_with_pending_messages(stream, db, &state.session)?;
    Ok(())
}

fn handle_copy_fail(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    state.copy_in = None;
    let message = cstr_from_bytes(body);
    send_error(
        stream,
        "57014",
        &format!("copy failed: {message}"),
        None,
        None,
        None,
    )?;
    send_ready_with_pending_messages(stream, db, &state.session)?;
    Ok(())
}

fn parse_copy_from_stdin(sql: &str) -> Option<(String, Option<Vec<String>>, String)> {
    let lower = sql.to_ascii_lowercase();
    let prefix = "copy ";
    let source = " from stdin";
    if !lower.starts_with(prefix) || !lower.contains(source) {
        return None;
    }
    let end = lower.find(source)?;
    let target = sql[prefix.len()..end].trim();
    if target.is_empty() {
        return None;
    }
    let options = sql[end + source.len()..].trim();
    let null_marker = parse_copy_null_marker(options)?;
    let (table, columns) = if let Some(open_paren) = target.find('(') {
        let close_paren = target.rfind(')')?;
        if close_paren < open_paren {
            return None;
        }
        let table = target[..open_paren].trim();
        let columns = target[open_paren + 1..close_paren]
            .split(',')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .map(|part| part.to_string())
            .collect::<Vec<_>>();
        if table.is_empty() || columns.is_empty() {
            return None;
        }
        (table.to_string(), Some(columns))
    } else {
        (target.to_string(), None)
    };
    Some((table, columns, null_marker))
}

fn parse_copy_null_marker(options: &str) -> Option<String> {
    let options = options.trim();
    if options.is_empty() {
        return Some("\\N".into());
    }
    let lower = options.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("null")
        .and_then(|_| options.get(4..))?
        .trim_start();
    parse_single_quoted_copy_option(rest)
}

fn parse_single_quoted_copy_option(input: &str) -> Option<String> {
    let mut chars = input.char_indices();
    if chars.next()?.1 != '\'' {
        return None;
    }
    let mut out = String::new();
    let mut end = None;
    let mut iter = input[1..].char_indices().peekable();
    while let Some((idx, ch)) = iter.next() {
        if ch == '\'' {
            if matches!(iter.peek(), Some((_, '\''))) {
                iter.next();
                out.push('\'');
                continue;
            }
            end = Some(idx + 2);
            break;
        }
        out.push(ch);
    }
    let end = end?;
    input[end..].trim().is_empty().then_some(out)
}

fn handle_parse(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let statement_name = read_cstr(body, &mut offset)?;
    let sql = read_cstr(body, &mut offset)?;
    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    let mut param_type_oids = Vec::with_capacity(nparams);
    for _ in 0..nparams {
        param_type_oids.push(read_i32_bytes(body, &mut offset)? as u32);
    }
    state.prepared.insert(
        statement_name,
        PreparedStatement {
            sql,
            param_type_oids,
        },
    );
    send_parse_complete(stream)
}

fn handle_bind(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let statement_name = read_cstr(body, &mut offset)?;
    let n_format_codes = read_i16_bytes(body, &mut offset)? as usize;
    let mut param_formats = Vec::with_capacity(n_format_codes);
    for _ in 0..n_format_codes {
        param_formats.push(read_i16_bytes(body, &mut offset)?);
    }
    if param_formats.iter().any(|code| !matches!(*code, 0 | 1)) {
        send_error(
            stream,
            "0A000",
            "unsupported parameter format code",
            None,
            None,
            None,
        )?;
        state.session.mark_transaction_failed();
        return Ok(());
    }
    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    if !(param_formats.is_empty() || param_formats.len() == 1 || param_formats.len() == nparams) {
        send_error(
            stream,
            "08P01",
            "bind message has invalid parameter format code count",
            None,
            None,
            None,
        )?;
        state.session.mark_transaction_failed();
        return Ok(());
    }
    let mut raw_params = Vec::with_capacity(nparams);
    for _ in 0..nparams {
        let len = read_i32_bytes(body, &mut offset)?;
        if len < 0 {
            raw_params.push(None);
        } else {
            let len = len as usize;
            let bytes = &body[offset..offset + len];
            offset += len;
            raw_params.push(Some(bytes.to_vec()));
        }
    }
    let n_result_codes = read_i16_bytes(body, &mut offset)? as usize;
    let mut result_formats = Vec::with_capacity(n_result_codes);
    for _ in 0..n_result_codes {
        result_formats.push(read_i16_bytes(body, &mut offset)?);
    }
    if result_formats.iter().any(|code| !matches!(*code, 0 | 1)) {
        send_error(
            stream,
            "0A000",
            "unsupported result format code",
            None,
            None,
            None,
        )?;
        state.session.mark_transaction_failed();
        return Ok(());
    }

    let Some(stmt) = state.prepared.get(&statement_name) else {
        send_error(
            stream,
            "26000",
            "unknown prepared statement",
            None,
            None,
            None,
        )?;
        state.session.mark_transaction_failed();
        return Ok(());
    };
    let required_params = required_bind_param_count(stmt);
    if nparams != required_params {
        let name = if statement_name.is_empty() {
            "<unnamed>"
        } else {
            &statement_name
        };
        send_error(
            stream,
            "08P01",
            &format!(
                "bind message supplies {nparams} parameters, but prepared statement \"{name}\" requires {required_params}"
            ),
            None,
            None,
            None,
        )?;
        state.session.mark_transaction_failed();
        return Ok(());
    }
    let catalog = state.session.catalog_lookup(db);
    let mut params = Vec::with_capacity(nparams);
    for (index, raw) in raw_params.iter().enumerate() {
        let format_code = parameter_format_code(&param_formats, index);
        match decode_bound_param(
            raw.as_deref(),
            format_code,
            stmt.param_type_oids.get(index).copied().unwrap_or(0),
            &catalog,
            state.session.datetime_config(),
        ) {
            Ok(param) => params.push(param),
            Err(e) => {
                let message = format_exec_error(&e);
                let hint = format_exec_error_hint(&e);
                send_error_with_hint(
                    stream,
                    exec_error_sqlstate(&e),
                    &message,
                    hint.as_deref(),
                    None,
                )?;
                state.session.mark_transaction_failed();
                return Ok(());
            }
        }
    }
    let sql = substitute_params(&stmt.sql, &params, &catalog);
    let prep_stmt_name = (!statement_name.is_empty()).then_some(statement_name);
    match state.session.bind_protocol_portal(
        db,
        &portal_name,
        prep_stmt_name,
        &sql,
        result_formats.clone(),
    ) {
        Ok(()) => {
            if let Some(cols) = state.session.portal_columns(&portal_name)
                && result_formats.len() > 1
                && result_formats.len() != cols.len()
            {
                send_error(
                    stream,
                    "08P01",
                    &format!(
                        "bind message has {} result formats but query has {} columns",
                        result_formats.len(),
                        cols.len()
                    ),
                    None,
                    None,
                    None,
                )?;
                state.session.close_portal(&portal_name).ok();
                state.session.mark_transaction_failed();
                return Ok(());
            }
            send_bind_complete(stream)
        }
        Err(e) => {
            let message = format_exec_error(&e);
            let hint = format_exec_error_hint(&e);
            state.session.mark_transaction_failed();
            send_error_with_hint(
                stream,
                exec_error_sqlstate(&e),
                &message,
                hint.as_deref(),
                None,
            )
        }
    }
}

fn handle_describe(
    stream: &mut impl Write,
    db: &Database,
    state: &ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let target_type = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "describe target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    match target_type {
        b'S' => match state.prepared.get(&name) {
            Some(stmt) => {
                let param_type_oids = stmt
                    .param_type_oids
                    .iter()
                    .map(|oid| *oid as i32)
                    .collect::<Vec<_>>();
                send_parameter_description(stream, &param_type_oids)?;
                match describe_sql(db, &state.session, &stmt.sql, &[]) {
                    Some(cols) => send_row_description(stream, &cols),
                    None => send_no_data(stream),
                }
            }
            None => send_no_data(stream),
        },
        b'P' => match state.session.portal_columns(&name) {
            Some(mut cols) => {
                let catalog = state.session.catalog_lookup(db);
                annotate_query_columns_with_wire_type_oids(&mut cols, &catalog);
                let formats = state
                    .session
                    .portal_result_formats(&name)
                    .unwrap_or_default();
                send_row_description_with_formats(stream, &cols, &formats)
            }
            None => send_error(stream, "34000", "portal does not exist", None, None, None),
        },
        _ => send_no_data(stream),
    }
}

fn handle_execute(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let max_rows = read_i32_bytes(body, &mut offset)?;
    let limit = if max_rows <= 0 {
        PortalFetchLimit::All
    } else {
        PortalFetchLimit::Count(max_rows as usize)
    };
    match state
        .session
        .execute_portal_forward(db, &portal_name, limit)
    {
        Ok(mut result) => {
            let catalog = state.session.catalog_lookup(db);
            annotate_query_columns_with_wire_type_oids(&mut result.columns, &catalog);
            if !result.columns.is_empty() {
                let formats = state
                    .session
                    .portal_result_formats(&portal_name)
                    .unwrap_or_default();
                if let Err(e) =
                    validate_binary_result_formats(&result.rows, &result.columns, &formats)
                {
                    let message = format_exec_error(&e);
                    let hint = format_exec_error_hint(&e);
                    send_error_with_hint(
                        stream,
                        exec_error_sqlstate(&e),
                        &message,
                        hint.as_deref(),
                        None,
                    )?;
                    return Ok(());
                }
                let role_names = role_name_map(&catalog);
                let relation_names = relation_name_map(&catalog);
                let proc_names = proc_name_map(&catalog);
                let namespace_names = namespace_name_map(&catalog);
                let mut row_buf = Vec::new();
                for row in &result.rows {
                    send_typed_data_row(
                        stream,
                        row,
                        &result.columns,
                        &formats,
                        &mut row_buf,
                        FloatFormatOptions {
                            extra_float_digits: state.session.extra_float_digits(),
                            bytea_output: state.session.bytea_output(),
                            datetime_config: state.session.datetime_config().clone(),
                        },
                        Some(&role_names),
                        Some(&relation_names),
                        Some(&proc_names),
                        Some(&namespace_names),
                    )?;
                }
                if result.completed {
                    send_command_complete(stream, &format!("SELECT {}", result.processed))
                } else {
                    send_portal_suspended(stream)
                }
            } else {
                let tag = result
                    .command_tag
                    .unwrap_or_else(|| format!("SELECT {}", result.processed));
                send_command_complete(stream, &tag)
            }
        }
        Err(e) => {
            let message = format_exec_error(&e);
            let hint = format_exec_error_hint(&e);
            state.session.mark_transaction_failed();
            send_error_with_hint(
                stream,
                exec_error_sqlstate(&e),
                &message,
                hint.as_deref(),
                None,
            )
        }
    }
}

fn handle_close(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let target_type = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "close target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    match target_type {
        b'S' => {
            state.prepared.remove(&name);
        }
        b'P' => {
            let _ = state.session.close_portal(&name);
        }
        _ => {}
    }
    send_close_complete(stream)
}

fn send_plpgsql_notices(stream: &mut impl Write, notices: &[PlpgsqlNotice]) -> io::Result<()> {
    for notice in notices {
        let (severity, sqlstate) = match notice.level {
            RaiseLevel::Notice => ("NOTICE", "00000"),
            RaiseLevel::Warning => ("WARNING", "01000"),
            RaiseLevel::Exception => continue,
        };
        send_notice_with_severity(stream, severity, sqlstate, &notice.message, None, None)?;
    }
    Ok(())
}

fn send_queued_notices(stream: &mut impl Write) -> io::Result<()> {
    for notice in take_backend_notices() {
        send_notice_with_severity(
            stream,
            notice.severity,
            notice.sqlstate,
            &notice.message,
            notice.detail.as_deref(),
            notice.position,
        )?;
    }
    send_plpgsql_notices(stream, &take_notices())
}

fn send_queued_notifications(
    stream: &mut impl Write,
    db: &Database,
    session: &Session,
) -> io::Result<()> {
    for notification in db.async_notify_runtime.drain(session.client_id) {
        send_notification_response(
            stream,
            notification.sender_pid,
            &notification.channel,
            &notification.payload,
        )?;
    }
    Ok(())
}

fn flush_pending_backend_messages(
    stream: &mut impl Write,
    db: &Database,
    session: &Session,
) -> io::Result<()> {
    send_queued_notices(stream)?;
    send_queued_notifications(stream, db, session)
}

fn send_ready_with_pending_messages(
    stream: &mut impl Write,
    db: &Database,
    session: &Session,
) -> io::Result<()> {
    flush_pending_backend_messages(stream, db, session)?;
    send_ready_for_query(stream, session.ready_status())
}

fn raw_select_contains_pg_notify(select_stmt: &crate::backend::parser::SelectStatement) -> bool {
    select_stmt.with.iter().any(raw_cte_contains_pg_notify)
        || select_stmt
            .targets
            .iter()
            .any(|target| raw_expr_contains_pg_notify(&target.expr))
        || select_stmt
            .from
            .as_ref()
            .is_some_and(raw_from_item_contains_pg_notify)
        || select_stmt
            .where_clause
            .as_ref()
            .is_some_and(raw_expr_contains_pg_notify)
        || select_stmt.group_by.iter().any(raw_expr_contains_pg_notify)
        || select_stmt
            .having
            .as_ref()
            .is_some_and(raw_expr_contains_pg_notify)
        || select_stmt
            .window_clauses
            .iter()
            .any(|clause| raw_window_spec_contains_pg_notify(&clause.spec))
        || select_stmt
            .order_by
            .iter()
            .any(raw_order_by_contains_pg_notify)
        || select_stmt
            .set_operation
            .as_ref()
            .is_some_and(|set_operation| raw_set_operation_contains_pg_notify(set_operation))
}

fn raw_cte_contains_pg_notify(cte: &crate::backend::parser::CommonTableExpr) -> bool {
    raw_cte_body_contains_pg_notify(&cte.body)
}

fn raw_cte_body_contains_pg_notify(body: &crate::backend::parser::CteBody) -> bool {
    match body {
        crate::backend::parser::CteBody::Select(select_stmt) => {
            raw_select_contains_pg_notify(select_stmt)
        }
        crate::backend::parser::CteBody::Values(values_stmt) => {
            raw_values_statement_contains_pg_notify(values_stmt)
        }
        crate::backend::parser::CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => raw_cte_body_contains_pg_notify(anchor) || raw_select_contains_pg_notify(recursive),
    }
}

fn raw_values_statement_contains_pg_notify(
    values_stmt: &crate::backend::parser::ValuesStatement,
) -> bool {
    values_stmt.with.iter().any(raw_cte_contains_pg_notify)
        || values_stmt
            .rows
            .iter()
            .flatten()
            .any(raw_expr_contains_pg_notify)
        || values_stmt
            .order_by
            .iter()
            .any(raw_order_by_contains_pg_notify)
}

fn raw_set_operation_contains_pg_notify(
    set_operation: &crate::backend::parser::SetOperationStatement,
) -> bool {
    set_operation
        .inputs
        .iter()
        .any(raw_select_contains_pg_notify)
}

fn raw_from_item_contains_pg_notify(from_item: &crate::backend::parser::FromItem) -> bool {
    match from_item {
        crate::backend::parser::FromItem::Table { .. } => false,
        crate::backend::parser::FromItem::Values { rows } => {
            rows.iter().flatten().any(raw_expr_contains_pg_notify)
        }
        crate::backend::parser::FromItem::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| raw_expr_contains_pg_notify(&arg.value)),
        crate::backend::parser::FromItem::Lateral(inner)
        | crate::backend::parser::FromItem::Alias { source: inner, .. } => {
            raw_from_item_contains_pg_notify(inner)
        }
        crate::backend::parser::FromItem::DerivedTable(select_stmt) => {
            raw_select_contains_pg_notify(select_stmt)
        }
        crate::backend::parser::FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            raw_from_item_contains_pg_notify(left)
                || raw_from_item_contains_pg_notify(right)
                || match constraint {
                    crate::backend::parser::JoinConstraint::On(expr) => {
                        raw_expr_contains_pg_notify(expr)
                    }
                    crate::backend::parser::JoinConstraint::None
                    | crate::backend::parser::JoinConstraint::Using(_)
                    | crate::backend::parser::JoinConstraint::Natural => false,
                }
        }
    }
}

fn raw_order_by_contains_pg_notify(order_by: &crate::backend::parser::OrderByItem) -> bool {
    raw_expr_contains_pg_notify(&order_by.expr)
}

fn raw_window_spec_contains_pg_notify(spec: &crate::backend::parser::RawWindowSpec) -> bool {
    spec.partition_by.iter().any(raw_expr_contains_pg_notify)
        || spec.order_by.iter().any(raw_order_by_contains_pg_notify)
        || spec
            .frame
            .as_ref()
            .is_some_and(|frame| raw_window_frame_contains_pg_notify(frame))
}

fn raw_window_frame_contains_pg_notify(frame: &crate::backend::parser::RawWindowFrame) -> bool {
    raw_window_frame_bound_contains_pg_notify(&frame.start_bound)
        || raw_window_frame_bound_contains_pg_notify(&frame.end_bound)
}

fn raw_window_frame_bound_contains_pg_notify(
    bound: &crate::backend::parser::RawWindowFrameBound,
) -> bool {
    match bound {
        crate::backend::parser::RawWindowFrameBound::OffsetPreceding(expr)
        | crate::backend::parser::RawWindowFrameBound::OffsetFollowing(expr) => {
            raw_expr_contains_pg_notify(expr)
        }
        crate::backend::parser::RawWindowFrameBound::UnboundedPreceding
        | crate::backend::parser::RawWindowFrameBound::CurrentRow
        | crate::backend::parser::RawWindowFrameBound::UnboundedFollowing => false,
    }
}

fn raw_expr_contains_pg_notify(expr: &crate::backend::parser::SqlExpr) -> bool {
    match expr {
        crate::backend::parser::SqlExpr::Column(_)
        | crate::backend::parser::SqlExpr::Default
        | crate::backend::parser::SqlExpr::Const(_)
        | crate::backend::parser::SqlExpr::IntegerLiteral(_)
        | crate::backend::parser::SqlExpr::NumericLiteral(_)
        | crate::backend::parser::SqlExpr::Random
        | crate::backend::parser::SqlExpr::CurrentDate
        | crate::backend::parser::SqlExpr::CurrentUser
        | crate::backend::parser::SqlExpr::SessionUser
        | crate::backend::parser::SqlExpr::CurrentRole => false,
        crate::backend::parser::SqlExpr::CurrentTime { .. }
        | crate::backend::parser::SqlExpr::CurrentTimestamp { .. }
        | crate::backend::parser::SqlExpr::LocalTime { .. }
        | crate::backend::parser::SqlExpr::LocalTimestamp { .. } => false,
        crate::backend::parser::SqlExpr::UnaryPlus(inner)
        | crate::backend::parser::SqlExpr::Negate(inner)
        | crate::backend::parser::SqlExpr::BitNot(inner)
        | crate::backend::parser::SqlExpr::Subscript { expr: inner, .. }
        | crate::backend::parser::SqlExpr::PrefixOperator { expr: inner, .. }
        | crate::backend::parser::SqlExpr::Cast(inner, _)
        | crate::backend::parser::SqlExpr::Not(inner)
        | crate::backend::parser::SqlExpr::IsNull(inner)
        | crate::backend::parser::SqlExpr::IsNotNull(inner)
        | crate::backend::parser::SqlExpr::FieldSelect { expr: inner, .. } => {
            raw_expr_contains_pg_notify(inner)
        }
        crate::backend::parser::SqlExpr::GeometryUnaryOp { expr: inner, .. } => {
            raw_expr_contains_pg_notify(inner)
        }
        crate::backend::parser::SqlExpr::Collate { expr: inner, .. } => {
            raw_expr_contains_pg_notify(inner)
        }
        crate::backend::parser::SqlExpr::Add(left, right)
        | crate::backend::parser::SqlExpr::Sub(left, right)
        | crate::backend::parser::SqlExpr::BitAnd(left, right)
        | crate::backend::parser::SqlExpr::BitOr(left, right)
        | crate::backend::parser::SqlExpr::BitXor(left, right)
        | crate::backend::parser::SqlExpr::Shl(left, right)
        | crate::backend::parser::SqlExpr::Shr(left, right)
        | crate::backend::parser::SqlExpr::Mul(left, right)
        | crate::backend::parser::SqlExpr::Div(left, right)
        | crate::backend::parser::SqlExpr::Mod(left, right)
        | crate::backend::parser::SqlExpr::Concat(left, right)
        | crate::backend::parser::SqlExpr::Eq(left, right)
        | crate::backend::parser::SqlExpr::NotEq(left, right)
        | crate::backend::parser::SqlExpr::Lt(left, right)
        | crate::backend::parser::SqlExpr::LtEq(left, right)
        | crate::backend::parser::SqlExpr::Gt(left, right)
        | crate::backend::parser::SqlExpr::GtEq(left, right)
        | crate::backend::parser::SqlExpr::RegexMatch(left, right)
        | crate::backend::parser::SqlExpr::And(left, right)
        | crate::backend::parser::SqlExpr::Or(left, right)
        | crate::backend::parser::SqlExpr::IsDistinctFrom(left, right)
        | crate::backend::parser::SqlExpr::IsNotDistinctFrom(left, right)
        | crate::backend::parser::SqlExpr::ArrayOverlap(left, right)
        | crate::backend::parser::SqlExpr::ArrayContains(left, right)
        | crate::backend::parser::SqlExpr::ArrayContained(left, right)
        | crate::backend::parser::SqlExpr::JsonbContains(left, right)
        | crate::backend::parser::SqlExpr::JsonbContained(left, right)
        | crate::backend::parser::SqlExpr::JsonbExists(left, right)
        | crate::backend::parser::SqlExpr::JsonbExistsAny(left, right)
        | crate::backend::parser::SqlExpr::JsonbExistsAll(left, right)
        | crate::backend::parser::SqlExpr::JsonbPathExists(left, right)
        | crate::backend::parser::SqlExpr::JsonbPathMatch(left, right)
        | crate::backend::parser::SqlExpr::JsonGet(left, right)
        | crate::backend::parser::SqlExpr::JsonGetText(left, right)
        | crate::backend::parser::SqlExpr::JsonPath(left, right)
        | crate::backend::parser::SqlExpr::JsonPathText(left, right) => {
            raw_expr_contains_pg_notify(left) || raw_expr_contains_pg_notify(right)
        }
        crate::backend::parser::SqlExpr::BinaryOperator { left, right, .. }
        | crate::backend::parser::SqlExpr::GeometryBinaryOp { left, right, .. } => {
            raw_expr_contains_pg_notify(left) || raw_expr_contains_pg_notify(right)
        }
        crate::backend::parser::SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | crate::backend::parser::SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            raw_expr_contains_pg_notify(expr)
                || raw_expr_contains_pg_notify(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
        }
        crate::backend::parser::SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_ref()
                .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
                || args.iter().any(|case_when| {
                    raw_expr_contains_pg_notify(&case_when.expr)
                        || raw_expr_contains_pg_notify(&case_when.result)
                })
                || defresult
                    .as_ref()
                    .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
        }
        crate::backend::parser::SqlExpr::ArrayLiteral(exprs)
        | crate::backend::parser::SqlExpr::Row(exprs) => {
            exprs.iter().any(raw_expr_contains_pg_notify)
        }
        crate::backend::parser::SqlExpr::ScalarSubquery(select_stmt)
        | crate::backend::parser::SqlExpr::ArraySubquery(select_stmt)
        | crate::backend::parser::SqlExpr::Exists(select_stmt) => {
            raw_select_contains_pg_notify(select_stmt)
        }
        crate::backend::parser::SqlExpr::InSubquery { expr, subquery, .. } => {
            raw_expr_contains_pg_notify(expr) || raw_select_contains_pg_notify(subquery)
        }
        crate::backend::parser::SqlExpr::QuantifiedSubquery { left, subquery, .. } => {
            raw_expr_contains_pg_notify(left) || raw_select_contains_pg_notify(subquery)
        }
        crate::backend::parser::SqlExpr::QuantifiedArray { left, array, .. } => {
            raw_expr_contains_pg_notify(left) || raw_expr_contains_pg_notify(array)
        }
        crate::backend::parser::SqlExpr::ArraySubscript { array, subscripts } => {
            raw_expr_contains_pg_notify(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
                })
        }
        crate::backend::parser::SqlExpr::Xml(xml) => {
            xml.child_exprs().any(raw_expr_contains_pg_notify)
        }
        crate::backend::parser::SqlExpr::FuncCall {
            name,
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            name.eq_ignore_ascii_case("pg_notify")
                || crate::backend::parser::function_arg_values(args)
                    .any(raw_expr_contains_pg_notify)
                || order_by.iter().any(raw_order_by_contains_pg_notify)
                || filter
                    .as_ref()
                    .is_some_and(|expr| raw_expr_contains_pg_notify(expr))
                || over
                    .as_ref()
                    .is_some_and(raw_window_spec_contains_pg_notify)
        }
    }
}

fn rewrite_regression_sql(sql: &str) -> std::borrow::Cow<'_, str> {
    let rewritten = rewrite_hex_bit_literals(sql);
    let rewritten = rewrite_shobj_description_calls(&rewritten);
    let rewritten = rewritten
        .replace(
            "bits::bigint::xfloat8::float8",
            "bitcast_bigint_to_float8(bits)",
        )
        .replace(
            "bits::integer::xfloat4::float4",
            "bitcast_integer_to_float4(bits)",
        );
    if rewritten == sql {
        std::borrow::Cow::Borrowed(sql)
    } else {
        std::borrow::Cow::Owned(rewritten)
    }
}

fn rewrite_hex_bit_literals(sql: &str) -> String {
    static HEX_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = HEX_RE.get_or_init(|| regex::Regex::new(r"x'([0-9A-Fa-f]+)'").unwrap());
    re.replace_all(sql, |captures: &regex::Captures<'_>| {
        let hex = &captures[1];
        match hex.len() {
            8 => u32::from_str_radix(hex, 16)
                .map(|bits| (bits as i32).to_string())
                .unwrap_or_else(|_| captures[0].to_string()),
            16 => u64::from_str_radix(hex, 16)
                .map(|bits| (bits as i64).to_string())
                .unwrap_or_else(|_| captures[0].to_string()),
            _ => captures[0].to_string(),
        }
    })
    .into_owned()
}

fn rewrite_shobj_description_calls(sql: &str) -> String {
    static SHOBJ_RE: OnceLock<regex::Regex> = OnceLock::new();
    static REGROLE_LITERAL_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = SHOBJ_RE.get_or_init(|| {
        regex::Regex::new(r"(?i)shobj_description\(([^,]+),\s*'pg_authid'\)").unwrap()
    });
    let regrole_re = REGROLE_LITERAL_RE
        .get_or_init(|| regex::Regex::new(r"(?i)^'((?:[^']|'')+)'\s*::\s*regrole$").unwrap());
    re.replace_all(sql, |captures: &regex::Captures<'_>| {
        let objoid = captures[1].trim();
        let objoid = if let Some(regrole) = regrole_re.captures(objoid) {
            let role_name = &regrole[1];
            format!("(select oid from pg_authid where rolname = '{role_name}')")
        } else {
            objoid.to_string()
        };
        format!(
            "(select description from pg_description where objoid = ({objoid}) and classoid = 1260 and objsubid = 0)"
        )
    })
    .into_owned()
}

fn try_handle_float_shell_ddl(stream: &mut impl Write, sql: &str) -> io::Result<bool> {
    let normalized = sql.trim().to_ascii_lowercase();
    let notices = if normalized == "create type xfloat4" || normalized == "create type xfloat8" {
        send_command_complete(stream, "CREATE TYPE")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat4in(") {
        send_notice(stream, "return type xfloat4 is only a shell", None, None)?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat8in(") {
        send_notice(stream, "return type xfloat8 is only a shell", None, None)?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat4out(") {
        send_notice(
            stream,
            "argument type xfloat4 is only a shell",
            None,
            sql.find("xfloat4)").map(|idx| idx + 1),
        )?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat8out(") {
        send_notice(
            stream,
            "argument type xfloat8 is only a shell",
            None,
            sql.find("xfloat8)").map(|idx| idx + 1),
        )?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create type xfloat4 (")
        || normalized.starts_with("create type xfloat8 (")
    {
        if normalized.contains("like = no_such_type") {
            send_error(
                stream,
                "42704",
                "type \"no_such_type\" does not exist",
                None,
                None,
                sql.find("no_such_type").map(|idx| idx + 1),
            )?;
            return Ok(true);
        }
        send_command_complete(stream, "CREATE TYPE")?;
        return Ok(true);
    } else if normalized.starts_with("create cast (xfloat4 as ")
        || normalized.starts_with("create cast (float4 as xfloat4)")
        || normalized.starts_with("create cast (xfloat8 as ")
        || normalized.starts_with("create cast (float8 as xfloat8)")
        || normalized.starts_with("create cast (integer as xfloat4)")
        || normalized.starts_with("create cast (bigint as xfloat8)")
    {
        send_command_complete(stream, "CREATE CAST")?;
        return Ok(true);
    } else if normalized == "drop type xfloat4 cascade" {
        Some(vec![
            "drop cascades to function xfloat4in(cstring)",
            "drop cascades to function xfloat4out(xfloat4)",
            "drop cascades to cast from xfloat4 to real",
            "drop cascades to cast from real to xfloat4",
            "drop cascades to cast from xfloat4 to integer",
            "drop cascades to cast from integer to xfloat4",
        ])
    } else if normalized == "drop type xfloat8 cascade" {
        Some(vec![
            "drop cascades to function xfloat8in(cstring)",
            "drop cascades to function xfloat8out(xfloat8)",
            "drop cascades to cast from xfloat8 to double precision",
            "drop cascades to cast from double precision to xfloat8",
            "drop cascades to cast from xfloat8 to bigint",
            "drop cascades to cast from bigint to xfloat8",
        ])
    } else {
        return Ok(false);
    };

    if let Some(notices) = notices {
        for notice in notices {
            send_notice(stream, notice, None, None)?;
        }
        send_command_complete(stream, "DROP TYPE")?;
        return Ok(true);
    }
    Ok(false)
}

fn describe_sql(
    db: &Database,
    session: &Session,
    sql: &str,
    params: &[BoundParam],
) -> Option<Vec<QueryColumn>> {
    let catalog = session.catalog_lookup(db);
    let sql = rewrite_regression_sql(&substitute_params(sql, params, &catalog)).into_owned();
    match crate::backend::parser::parse_statement_with_options(
        &sql,
        crate::backend::parser::ParseOptions {
            max_stack_depth_kb: session.datetime_config().max_stack_depth_kb,
            ..crate::backend::parser::ParseOptions::default()
        },
    )
    .ok()?
    {
        Statement::Select(stmt) => crate::backend::parser::pg_plan_query(&stmt, &catalog)
            .ok()
            .map(|planned_stmt| {
                let mut columns = planned_stmt.columns();
                annotate_query_columns_with_wire_type_oids(&mut columns, &catalog);
                columns
            }),
        Statement::Explain(_) => Some(vec![QueryColumn::text("QUERY PLAN")]),
        _ => None,
    }
}

fn substitute_params(sql: &str, params: &[BoundParam], catalog: &dyn CatalogLookup) -> String {
    let mut out = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let regclass_value = match param {
            BoundParam::Null => "null".to_string(),
            BoundParam::Text(v) => resolve_regclass_param(v, catalog),
            BoundParam::SqlExpression(expr) => expr.clone(),
        };
        out = out.replace(
            &format!("{placeholder}::pg_catalog.regclass"),
            &regclass_value,
        );
        out = out.replace(&format!("{placeholder}::regclass"), &regclass_value);
        let value = match param {
            BoundParam::Null => "null".to_string(),
            BoundParam::Text(v) if v.parse::<i64>().is_ok() => v.clone(),
            BoundParam::Text(v) => quote_sql_string(v),
            BoundParam::SqlExpression(expr) => expr.clone(),
        };
        out = out.replace(&placeholder, &value);
    }
    out
}

fn annotate_query_columns_with_wire_type_oids(
    columns: &mut [QueryColumn],
    catalog: &dyn CatalogLookup,
) {
    for column in columns {
        if column.wire_type_oid.is_some() {
            continue;
        }
        if column.sql_type.is_array
            || matches!(
                column.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            column.wire_type_oid = catalog.type_oid_for_sql_type(column.sql_type);
        }
    }
}

fn parameter_format_code(format_codes: &[i16], index: usize) -> i16 {
    match format_codes {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

fn required_bind_param_count(stmt: &PreparedStatement) -> usize {
    stmt.param_type_oids
        .len()
        .max(highest_sql_parameter_ref(&stmt.sql))
}

fn highest_sql_parameter_ref(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut highest = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'$' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > start {
            if let Ok(param) = sql[start..end].parse::<usize>() {
                highest = highest.max(param);
            }
            index = end;
        } else {
            index += 1;
        }
    }
    highest
}

fn feature_not_supported_error(feature: impl Into<String>) -> ExecError {
    ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(
        feature.into(),
    ))
}

fn decode_bound_param(
    raw: Option<&[u8]>,
    format_code: i16,
    declared_type_oid: u32,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<BoundParam, ExecError> {
    match (raw, format_code) {
        (None, _) => Ok(BoundParam::Null),
        (Some(bytes), 0) => Ok(BoundParam::Text(
            String::from_utf8_lossy(bytes).into_owned(),
        )),
        (Some(bytes), 1) => {
            if declared_type_oid == 0 {
                return Err(feature_not_supported_error(
                    "binary parameters require declared type OIDs",
                ));
            }
            let value = decode_binary_parameter_value(declared_type_oid, bytes, catalog)?;
            let sql =
                render_bound_value_sql(&value, Some(declared_type_oid), catalog, datetime_config)?;
            Ok(BoundParam::SqlExpression(sql))
        }
        (_, code) => Err(feature_not_supported_error(format!(
            "parameter format code {code}"
        ))),
    }
}

fn decode_binary_parameter_value(
    type_oid: u32,
    bytes: &[u8],
    catalog: &dyn CatalogLookup,
) -> Result<Value, ExecError> {
    let type_row = catalog.type_by_oid(type_oid).ok_or_else(|| {
        feature_not_supported_error(format!("binary parameter type oid {type_oid}"))
    })?;
    if type_row.sql_type.is_array {
        return decode_binary_array_parameter(&type_row, bytes, catalog);
    }
    match type_row.sql_type.kind {
        SqlTypeKind::Int2 => {
            let raw = require_be_i16(bytes, "int2 binary parameter")?;
            Ok(Value::Int16(raw))
        }
        SqlTypeKind::Int4 => {
            let raw = require_be_i32(bytes, "int4 binary parameter")?;
            Ok(Value::Int32(raw))
        }
        SqlTypeKind::Int8 => {
            let raw = require_be_i64(bytes, "int8 binary parameter")?;
            Ok(Value::Int64(raw))
        }
        SqlTypeKind::Oid
        | SqlTypeKind::Xid
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary => {
            let raw = require_be_u32(bytes, "oid binary parameter")?;
            Ok(Value::Int64(raw as i64))
        }
        SqlTypeKind::Money => Ok(Value::Money(require_be_i64(
            bytes,
            "money binary parameter",
        )?)),
        SqlTypeKind::Bool => Ok(Value::Bool(
            require_exact_len(bytes, 1, "bool binary parameter")?[0] != 0,
        )),
        SqlTypeKind::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        SqlTypeKind::Text
        | SqlTypeKind::Varchar
        | SqlTypeKind::Char
        | SqlTypeKind::Name
        | SqlTypeKind::PgNodeTree => Ok(Value::Text(CompactString::from_owned(
            String::from_utf8_lossy(bytes).into_owned(),
        ))),
        SqlTypeKind::Json => Ok(Value::Json(CompactString::from_owned(
            String::from_utf8_lossy(bytes).into_owned(),
        ))),
        SqlTypeKind::JsonPath => Ok(Value::JsonPath(CompactString::from_owned(
            String::from_utf8_lossy(bytes).into_owned(),
        ))),
        SqlTypeKind::InternalChar => Ok(Value::InternalChar(
            require_exact_len(bytes, 1, "internal char binary parameter")?[0],
        )),
        SqlTypeKind::Float4 => {
            let bits = require_be_u32(bytes, "float4 binary parameter")?;
            Ok(Value::Float64(f32::from_bits(bits) as f64))
        }
        SqlTypeKind::Float8 => {
            let bits = require_be_u64(bytes, "float8 binary parameter")?;
            Ok(Value::Float64(f64::from_bits(bits)))
        }
        SqlTypeKind::Date => Ok(Value::Date(DateADT(require_be_i32(
            bytes,
            "date binary parameter",
        )?))),
        SqlTypeKind::Time => Ok(Value::Time(TimeADT(require_be_i64(
            bytes,
            "time binary parameter",
        )?))),
        SqlTypeKind::TimeTz => {
            let raw = require_exact_len(bytes, 12, "timetz binary parameter")?;
            Ok(Value::TimeTz(TimeTzADT {
                time: TimeADT(i64::from_be_bytes(raw[0..8].try_into().unwrap())),
                offset_seconds: i32::from_be_bytes(raw[8..12].try_into().unwrap()),
            }))
        }
        SqlTypeKind::Timestamp => Ok(Value::Timestamp(TimestampADT(require_be_i64(
            bytes,
            "timestamp binary parameter",
        )?))),
        SqlTypeKind::TimestampTz => Ok(Value::TimestampTz(TimestampTzADT(require_be_i64(
            bytes,
            "timestamptz binary parameter",
        )?))),
        SqlTypeKind::Record | SqlTypeKind::Composite => {
            decode_binary_record_parameter(&type_row, bytes, catalog)
        }
        other => Err(feature_not_supported_error(format!(
            "binary input for {:?}",
            other
        ))),
    }
}

fn decode_binary_array_parameter(
    array_type_row: &crate::include::catalog::PgTypeRow,
    bytes: &[u8],
    catalog: &dyn CatalogLookup,
) -> Result<Value, ExecError> {
    if bytes.len() < 12 {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "array binary parameter header truncated".into(),
        });
    }
    let ndim = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if ndim < 0 {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "array binary parameter ndim cannot be negative".into(),
        });
    }
    let ndim = ndim as usize;
    let element_oid = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
    let expected_element_oid = if array_type_row.typelem != 0 {
        array_type_row.typelem
    } else {
        array_type_row.sql_type.element_type().type_oid
    };
    if expected_element_oid != 0 && element_oid != expected_element_oid {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: format!(
                "array binary parameter element oid {} does not match expected {}",
                element_oid, expected_element_oid
            ),
        });
    }
    catalog
        .type_by_oid(element_oid)
        .ok_or_else(|| feature_not_supported_error(format!("array element oid {element_oid}")))?;
    let mut offset = 12usize;
    let mut dimensions = Vec::with_capacity(ndim);
    for _ in 0..ndim {
        if offset + 8 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "array binary parameter dimensions truncated".into(),
            });
        }
        let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let lower_bound = i32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        if length < 0 {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "array binary parameter length cannot be negative".into(),
            });
        }
        dimensions.push(ArrayDimension {
            lower_bound,
            length: length as usize,
        });
        offset += 8;
    }
    let item_count = dimensions
        .iter()
        .try_fold(1usize, |acc, dim| acc.checked_mul(dim.length))
        .unwrap_or(0);
    let mut elements = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "array binary parameter elements truncated".into(),
            });
        }
        let len = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len < 0 {
            elements.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "array binary parameter element payload truncated".into(),
            });
        }
        let value =
            decode_binary_parameter_value(element_oid, &bytes[offset..offset + len], catalog)?;
        elements.push(value);
        offset += len;
    }
    Ok(Value::PgArray(
        ArrayValue::from_dimensions(dimensions, elements).with_element_type_oid(element_oid),
    ))
}

fn decode_binary_record_parameter(
    type_row: &crate::include::catalog::PgTypeRow,
    bytes: &[u8],
    catalog: &dyn CatalogLookup,
) -> Result<Value, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "record binary parameter header truncated".into(),
        });
    }
    let field_count = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if field_count < 0 {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "record binary parameter field count cannot be negative".into(),
        });
    }
    let field_count = field_count as usize;
    let mut offset = 4usize;

    let named_fields = if type_row.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(type_row.typrelid)
            .ok_or_else(|| {
                feature_not_supported_error(format!(
                    "composite type relation {}",
                    type_row.typrelid
                ))
            })?;
        Some(
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    if let Some(fields) = &named_fields
        && fields.len() != field_count
    {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: format!(
                "record binary parameter field count {} does not match named composite width {}",
                field_count,
                fields.len()
            ),
        });
    }

    let mut descriptor_fields = Vec::with_capacity(field_count);
    let mut values = Vec::with_capacity(field_count);
    for index in 0..field_count {
        if offset + 8 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "record binary parameter fields truncated".into(),
            });
        }
        let field_oid = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let len = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let (field_name, field_type_oid, field_sql_type) = if let Some(fields) = &named_fields {
            let (name, sql_type) = fields[index].clone();
            let resolved_oid = catalog.type_oid_for_sql_type(sql_type).unwrap_or(field_oid);
            (name, resolved_oid, sql_type)
        } else {
            let sql_type = catalog
                .type_by_oid(field_oid)
                .map(|row| row.sql_type)
                .unwrap_or_else(|| SqlType::record(field_oid));
            (format!("f{}", index + 1), field_oid, sql_type)
        };

        if len < 0 {
            descriptor_fields.push((field_name, field_sql_type));
            values.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<bind>".into(),
                details: "record binary parameter field payload truncated".into(),
            });
        }
        let payload = &bytes[offset..offset + len];
        offset += len;
        let value = decode_binary_parameter_value(field_type_oid, payload, catalog)?;
        descriptor_fields.push((field_name, field_sql_type));
        values.push(value);
    }

    let descriptor = if type_row.typrelid != 0 {
        RecordDescriptor::named(type_row.oid, type_row.typrelid, -1, descriptor_fields)
    } else {
        assign_anonymous_record_descriptor(descriptor_fields)
    };
    Ok(Value::Record(RecordValue::from_descriptor(
        descriptor, values,
    )))
}

fn render_bound_value_sql(
    value: &Value,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let base = render_bound_value_base_sql(value, declared_type_oid, catalog, datetime_config)?;
    if matches!(declared_type_oid, Some(RECORD_TYPE_OID)) {
        return Ok(base);
    }
    if let Some(type_oid) = declared_type_oid.filter(|oid| *oid != 0) {
        return Ok(format!(
            "({base})::{}",
            render_type_name(type_oid, catalog)?
        ));
    }
    Ok(base)
}

fn render_bound_value_base_sql(
    value: &Value,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    Ok(match value {
        Value::Null => "null".to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => {
            if v.is_finite() {
                v.to_string()
            } else {
                quote_sql_string(&v.to_string())
            }
        }
        Value::Bool(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Text(text) => quote_sql_string(text),
        Value::TextRef(_, _) => quote_sql_string(value.as_text().unwrap_or_default()),
        Value::Json(text) => quote_sql_string(text),
        Value::JsonPath(text) => quote_sql_string(text),
        Value::Bytea(bytes) => quote_sql_string(&format_bytea_text(
            bytes,
            crate::pgrust::session::ByteaOutputFormat::Hex,
        )),
        Value::InternalChar(byte) => {
            quote_sql_string(&crate::backend::executor::render_internal_char_text(*byte))
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => quote_sql_string(
            &crate::backend::executor::render_datetime_value_text_with_config(
                value,
                datetime_config,
            )
            .unwrap_or_default(),
        ),
        Value::TsVector(vector) => {
            quote_sql_string(&crate::backend::executor::render_tsvector_text(vector))
        }
        Value::TsQuery(query) => {
            quote_sql_string(&crate::backend::executor::render_tsquery_text(query))
        }
        Value::Jsonb(bytes) => quote_sql_string(
            &crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap_or_default(),
        ),
        Value::Record(record) => {
            let mut fields = Vec::with_capacity(record.fields.len());
            for (field, field_value) in record.iter() {
                let field_type_oid =
                    catalog
                        .type_oid_for_sql_type(field.sql_type)
                        .or((field.sql_type.type_oid != 0).then_some(field.sql_type.type_oid));
                fields.push(render_bound_value_sql(
                    field_value,
                    field_type_oid,
                    catalog,
                    datetime_config,
                )?);
            }
            format!("ROW({})", fields.join(", "))
        }
        Value::Array(items) => {
            let array = ArrayValue::from_1d(items.clone());
            render_array_sql(&array, declared_type_oid, catalog, datetime_config)?
        }
        Value::PgArray(array) => {
            render_array_sql(array, declared_type_oid, catalog, datetime_config)?
        }
        other => {
            return Err(feature_not_supported_error(format!(
                "binary parameter rendering for {:?}",
                other.sql_type_hint()
            )));
        }
    })
}

fn render_array_sql(
    array: &ArrayValue,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    if array.dimensions.is_empty() {
        return Ok("ARRAY[]".to_string());
    }
    let element_type_oid = array.element_type_oid.or_else(|| {
        declared_type_oid.and_then(|oid| catalog.type_by_oid(oid).map(|row| row.typelem))
    });
    let mut index = 0usize;
    let body = render_array_dimension_sql(
        &array.dimensions,
        &array.elements,
        0,
        &mut index,
        element_type_oid,
        catalog,
        datetime_config,
    )?;
    Ok(format!("ARRAY{body}"))
}

fn render_array_dimension_sql(
    dimensions: &[ArrayDimension],
    elements: &[Value],
    depth: usize,
    index: &mut usize,
    element_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let dim = dimensions
        .get(depth)
        .ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "array dimension index out of bounds".into(),
        })?;
    let mut parts = Vec::with_capacity(dim.length);
    for _ in 0..dim.length {
        if depth + 1 == dimensions.len() {
            let value = elements
                .get(*index)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "<bind>".into(),
                    details: "array element index out of bounds".into(),
                })?;
            parts.push(render_bound_value_sql(
                value,
                element_type_oid,
                catalog,
                datetime_config,
            )?);
            *index += 1;
        } else {
            parts.push(render_array_dimension_sql(
                dimensions,
                elements,
                depth + 1,
                index,
                element_type_oid,
                catalog,
                datetime_config,
            )?);
        }
    }
    Ok(format!("[{}]", parts.join(", ")))
}

fn render_type_name(type_oid: u32, catalog: &dyn CatalogLookup) -> Result<String, ExecError> {
    let row = catalog
        .type_by_oid(type_oid)
        .ok_or_else(|| feature_not_supported_error(format!("type oid {type_oid}")))?;
    Ok(quote_identifier(&row.typname))
}

fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn require_exact_len<'a>(
    bytes: &'a [u8],
    expected: usize,
    label: &str,
) -> Result<&'a [u8], ExecError> {
    if bytes.len() != expected {
        return Err(ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: format!("{label} expected {expected} bytes, got {}", bytes.len()),
        });
    }
    Ok(bytes)
}

fn require_be_i16(bytes: &[u8], label: &str) -> Result<i16, ExecError> {
    Ok(i16::from_be_bytes(
        require_exact_len(bytes, 2, label)?.try_into().unwrap(),
    ))
}

fn require_be_i32(bytes: &[u8], label: &str) -> Result<i32, ExecError> {
    Ok(i32::from_be_bytes(
        require_exact_len(bytes, 4, label)?.try_into().unwrap(),
    ))
}

fn require_be_i64(bytes: &[u8], label: &str) -> Result<i64, ExecError> {
    Ok(i64::from_be_bytes(
        require_exact_len(bytes, 8, label)?.try_into().unwrap(),
    ))
}

fn require_be_u32(bytes: &[u8], label: &str) -> Result<u32, ExecError> {
    Ok(u32::from_be_bytes(
        require_exact_len(bytes, 4, label)?.try_into().unwrap(),
    ))
}

fn require_be_u64(bytes: &[u8], label: &str) -> Result<u64, ExecError> {
    Ok(u64::from_be_bytes(
        require_exact_len(bytes, 8, label)?.try_into().unwrap(),
    ))
}

fn resolve_regclass_param(value: &str, catalog: &dyn CatalogLookup) -> String {
    if value.parse::<u32>().is_ok() {
        return value.to_string();
    }
    catalog
        .lookup_relation(value)
        .map(|entry| entry.relation_oid.to_string())
        .unwrap_or_else(|| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::Catalog;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::pgrust::cluster::Cluster;
    use crate::pgrust::database::Database;
    use crate::pgrust::session::Session;
    use std::io::{self, Cursor, Read, Write};
    #[cfg(not(unix))]
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    #[cfg(unix)]
    type TestStream = UnixStream;
    #[cfg(not(unix))]
    type TestStream = TcpStream;

    fn temp_dir(name: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("pgrust_tcop_{name}_{}_{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn startup_packet(user: &str, database: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        payload.extend_from_slice(b"user");
        payload.push(0);
        payload.extend_from_slice(user.as_bytes());
        payload.push(0);
        payload.extend_from_slice(b"database");
        payload.push(0);
        payload.extend_from_slice(database.as_bytes());
        payload.push(0);
        payload.push(0);

        let mut packet = Vec::new();
        packet.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        packet.extend_from_slice(&payload);
        packet
    }

    fn frontend_message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut packet = vec![tag];
        packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn query_message(sql: &str) -> Vec<u8> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        frontend_message(b'Q', &body)
    }

    fn terminate_message() -> Vec<u8> {
        let mut packet = vec![b'X'];
        packet.extend_from_slice(&4_i32.to_be_bytes());
        packet
    }

    #[cfg(unix)]
    fn start_test_connection_with_cluster(
        cluster: Cluster,
        client_id: u32,
    ) -> (TestStream, thread::JoinHandle<io::Result<()>>) {
        let (server_stream, client_stream) = UnixStream::pair().unwrap();
        let reader = server_stream.try_clone().unwrap();
        let server = thread::spawn(move || {
            handle_connection_with_io(reader, server_stream, &cluster, client_id)
        });
        client_stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        (client_stream, server)
    }

    #[cfg(not(unix))]
    fn start_test_connection_with_cluster(
        cluster: Cluster,
        client_id: u32,
    ) -> (TestStream, thread::JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let reader = stream.try_clone().unwrap();
            handle_connection_with_io(reader, stream, &cluster, client_id)
        });
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        (stream, server)
    }

    fn write_packet(stream: &mut impl Write, packet: &[u8]) {
        stream.write_all(packet).unwrap();
        stream.flush().unwrap();
    }

    fn parse_message(statement_name: &str, sql: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(statement_name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i16.to_be_bytes());
        frontend_message(b'P', &body)
    }

    fn flush_message() -> Vec<u8> {
        frontend_message(b'H', &[])
    }

    fn read_message(stream: &mut impl Read, label: &str) -> (u8, Vec<u8>) {
        let mut kind = [0u8; 1];
        stream
            .read_exact(&mut kind)
            .unwrap_or_else(|e| panic!("{label}: failed reading kind: {e}"));
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .unwrap_or_else(|e| panic!("{label}: failed reading length: {e}"));
        let len = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; len - 4];
        stream
            .read_exact(&mut body)
            .unwrap_or_else(|e| panic!("{label}: failed reading body: {e}"));
        (kind[0], body)
    }

    fn try_read_message(stream: &mut TestStream, label: &str) -> Option<(u8, Vec<u8>)> {
        let mut kind = [0u8; 1];
        match stream.read_exact(&mut kind) {
            Ok(()) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return None;
            }
            Err(err) => panic!("{label}: failed reading kind: {err}"),
        }
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .unwrap_or_else(|e| panic!("{label}: failed reading length: {e}"));
        let len = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; len - 4];
        stream
            .read_exact(&mut body)
            .unwrap_or_else(|e| panic!("{label}: failed reading body: {e}"));
        Some((kind[0], body))
    }

    fn read_until_ready(stream: &mut TestStream, label: &str) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        loop {
            let message = read_message(stream, label);
            let done = message.0 == b'Z';
            messages.push(message);
            if done {
                return messages;
            }
        }
    }

    fn read_available_messages(stream: &mut TestStream, label: &str) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        while let Some(message) = try_read_message(stream, label) {
            messages.push(message);
        }
        messages
    }

    fn first_error_response_position(output: &[u8]) -> Option<usize> {
        let mut offset = 0;
        while offset + 5 <= output.len() {
            let tag = output[offset];
            let len = i32::from_be_bytes(output[offset + 1..offset + 5].try_into().ok()?) as usize;
            if len < 4 || offset + 1 + len > output.len() {
                return None;
            }
            let body = &output[offset + 5..offset + 1 + len];
            offset += 1 + len;

            if tag != b'E' {
                continue;
            }

            let mut body_offset = 0;
            while body_offset < body.len() {
                let field_type = *body.get(body_offset)?;
                body_offset += 1;
                if field_type == 0 {
                    break;
                }
                let field_end = body[body_offset..]
                    .iter()
                    .position(|byte| *byte == 0)
                    .map(|pos| body_offset + pos)?;
                if field_type == b'P' {
                    return std::str::from_utf8(&body[body_offset..field_end])
                        .ok()?
                        .parse()
                        .ok();
                }
                body_offset = field_end + 1;
            }
        }
        None
    }

    #[test]
    fn parse_errors_use_postgres_sqlstates() {
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::UnknownTable("items".into(),)
            )),
            "42P01"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::UnknownColumn("name".into()),
            )),
            "42703"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::AmbiguousColumn("name".into()),
            )),
            "42702"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::DuplicateTableName("items".into()),
            )),
            "42712"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::TableAlreadyExists("items".into()),
            )),
            "42P07"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::UnsupportedType("widget".into()),
            )),
            "42704"
        );
        assert_eq!(
            exec_error_sqlstate(&ExecError::Parse(
                crate::backend::parser::ParseError::WrongObjectType {
                    name: "items".into(),
                    expected: "table",
                },
            )),
            "42809"
        );
    }

    fn parameter_status_value(output: &[u8], key: &str) -> Option<String> {
        let mut offset = 0;
        while offset + 5 <= output.len() {
            let tag = output[offset];
            let len = i32::from_be_bytes(output[offset + 1..offset + 5].try_into().ok()?) as usize;
            if len < 4 || offset + 1 + len > output.len() {
                return None;
            }
            let body = &output[offset + 5..offset + 1 + len];
            offset += 1 + len;

            if tag != b'S' {
                continue;
            }

            let key_end = body.iter().position(|byte| *byte == 0)?;
            let value_start = key_end + 1;
            let value_end = body[value_start..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|pos| value_start + pos)?;
            if &body[..key_end] == key.as_bytes() {
                return Some(String::from_utf8_lossy(&body[value_start..value_end]).into_owned());
            }
        }
        None
    }

    #[derive(Debug, PartialEq, Eq)]
    struct NotificationResponseMessage {
        sender_pid: i32,
        channel: String,
        payload: String,
    }

    fn backend_messages(output: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        let mut offset = 0;
        while offset + 5 <= output.len() {
            let tag = output[offset];
            let len =
                i32::from_be_bytes(output[offset + 1..offset + 5].try_into().unwrap()) as usize;
            if len < 4 || offset + 1 + len > output.len() {
                break;
            }
            let body = output[offset + 5..offset + 1 + len].to_vec();
            messages.push((tag, body));
            offset += 1 + len;
        }
        messages
    }

    fn backend_key_data_pid(output: &[u8]) -> Option<i32> {
        backend_messages(output)
            .into_iter()
            .find(|(tag, _)| *tag == b'K')
            .and_then(|(_, body)| {
                body.get(..4)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(i32::from_be_bytes)
            })
    }

    fn notification_responses(output: &[u8]) -> Vec<NotificationResponseMessage> {
        notification_responses_from_messages(&backend_messages(output))
    }

    fn notification_responses_from_messages(
        messages: &[(u8, Vec<u8>)],
    ) -> Vec<NotificationResponseMessage> {
        messages
            .iter()
            .filter(|(tag, _)| *tag == b'A')
            .filter_map(|(_, body)| {
                let sender_pid = i32::from_be_bytes(body.get(..4)?.try_into().ok()?);
                let mut offset = 4usize;
                let channel = read_cstr(&body, &mut offset).ok()?.to_string();
                let payload = read_cstr(&body, &mut offset).ok()?.to_string();
                Some(NotificationResponseMessage {
                    sender_pid,
                    channel,
                    payload,
                })
            })
            .collect()
    }

    fn command_complete_tags(output: &[u8]) -> Vec<String> {
        backend_messages(output)
            .into_iter()
            .filter(|(tag, _)| *tag == b'C')
            .map(|(_, body)| cstr_from_bytes(&body).to_string())
            .collect()
    }

    fn output_contains_message(output: &[u8], message: &str) -> bool {
        output
            .windows(message.len() + 1)
            .any(|window| window == format!("{message}\0").as_bytes())
    }

    #[test]
    fn simple_query_role_creation_is_visible_to_next_query() {
        let db = Database::open(temp_dir("role_visibility"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(&mut output, &db, &mut state, "create role tenant login;").unwrap();
        assert!(
            db.backend_catcache(2, None)
                .unwrap()
                .authid_rows()
                .into_iter()
                .any(|row| row.rolname == "tenant")
        );

        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut state,
            "set session authorization tenant;",
        )
        .unwrap();

        let tenant_oid = db
            .backend_catcache(2, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == "tenant")
            .map(|row| row.oid)
            .unwrap();
        assert_eq!(state.session.current_user_oid(), tenant_oid);
    }

    #[test]
    fn simple_query_executes_multiple_statements_in_order() {
        let db = Database::open(temp_dir("multi_statement"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role tenant login; set session authorization tenant;",
        )
        .unwrap();

        let tenant_oid = db
            .backend_catcache(2, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == "tenant")
            .map(|row| row.oid)
            .unwrap();
        assert_eq!(state.session.current_user_oid(), tenant_oid);
    }

    #[test]
    fn simple_query_drop_role_sees_granted_by_dependencies_from_prior_statements() {
        let db = Database::open(temp_dir("drop_role_granted_by_dependency"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role user1;\
             create role user2;\
             create role user3;\
             grant user1 to user2 with admin option;\
             grant user1 to user3 granted by user2;\
             drop role user2;",
        )
        .unwrap();

        assert!(output_contains_message(
            &output,
            "role \"user2\" cannot be dropped because some objects depend on it"
        ));
        assert!(output_contains_message(
            &output,
            "privileges for membership of role user3 in role user1"
        ));
        assert!(
            db.backend_catcache(2, None)
                .unwrap()
                .authid_rows()
                .into_iter()
                .any(|row| row.rolname == "user2")
        );
    }

    #[test]
    fn simple_query_reassign_and_drop_owned_preserve_role_until_final_drop() {
        let db = Database::open(temp_dir("drop_owned_granted_by_dependency"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        for sql in [
            "create role user1",
            "create role user2",
            "create role user3",
            "create role user4",
            "grant user1 to user2 with admin option",
            "grant user1 to user3 granted by user2",
            "drop role user2",
            "reassign owned by user2 to user4",
            "drop role user2",
            "drop owned by user2",
            "drop role user2",
        ] {
            handle_query(&mut output, &db, &mut state, sql).unwrap();
        }

        assert!(output_contains_message(
            &output,
            "role \"user2\" cannot be dropped because some objects depend on it"
        ));
        assert!(output_contains_message(
            &output,
            "privileges for membership of role user3 in role user1"
        ));
        assert!(!output_contains_message(
            &output,
            "role \"user2\" does not exist"
        ));
        assert!(
            !db.backend_catcache(2, None)
                .unwrap()
                .authid_rows()
                .into_iter()
                .any(|row| row.rolname == "user2")
        );
    }

    #[test]
    fn simple_query_session_authorization_sees_created_schema_for_qualified_create_table() {
        let db = Database::open(temp_dir("pub_session_auth_schema"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role regress_publication_user login superuser;\
             set session authorization regress_publication_user;\
             create schema pub_test;\
             create table pub_test.testpub_nopk (foo int4, bar int4);",
        )
        .unwrap();

        assert!(!output_contains_message(
            &output,
            "schema \"pub_test\" does not exist"
        ));
        assert!(
            state
                .session
                .catalog_lookup(&db)
                .lookup_any_relation("pub_test.testpub_nopk")
                .is_some()
        );
    }

    #[test]
    fn simple_query_publication_footer_query_runs_after_session_authorization_setup() {
        let db = Database::open(temp_dir("pub_session_auth_footer"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role regress_publication_user login superuser;\
             set session authorization regress_publication_user;\
             create schema pub_test;\
             create table testpub_tbl1 (id int4);\
             create publication pub for table testpub_tbl1;\
             alter publication pub add tables in schema pub_test;",
        )
        .unwrap();

        let publication_oid = db
            .backend_catcache(2, None)
            .unwrap()
            .publication_row_by_name("pub")
            .map(|row| row.oid)
            .unwrap();
        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut state,
            &format!(
                "SELECT n.nspname, c.relname, \
                     pg_get_expr(pr.prqual, c.oid), \
                     (CASE WHEN pr.prattrs IS NOT NULL THEN \
                         pg_catalog.array_to_string( \
                           ARRAY(SELECT attname \
                                   FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, \
                                        pg_catalog.pg_attribute \
                                  WHERE attrelid = c.oid AND attnum = prattrs[s]), ', ') \
                      ELSE NULL END) \
                 FROM pg_catalog.pg_class c, \
                      pg_catalog.pg_namespace n, \
                      pg_catalog.pg_publication_rel pr \
                 WHERE c.relnamespace = n.oid \
                   AND c.oid = pr.prrelid \
                   AND pr.prpubid = '{}' \
                 ORDER BY 1,2",
                publication_oid
            ),
        )
        .unwrap();

        assert!(!output_contains_message(
            &output,
            "unknown table: pg_catalog.pg_class"
        ));
    }

    #[test]
    fn simple_query_explicit_pg_catalog_pg_class_lookup_runs_via_native_sql() {
        let db = Database::open(temp_dir("explicit_pg_class_lookup"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "select relname from pg_catalog.pg_class where relname = 'pg_class'",
        )
        .unwrap();

        assert!(!output_contains_message(
            &output,
            "unknown table: pg_catalog.pg_class"
        ));
    }

    #[test]
    fn simple_query_substring_similar_error_includes_context_field() {
        let db = Database::open(temp_dir("substring_similar_error_context"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "select substring('abcdefg' similar 'a*#\"%#\"g*#\"x' escape '#')",
        )
        .unwrap();

        assert!(
            output
                .windows(
                    "MSQL regular expression may not contain more than two escape-double-quote separators\0"
                        .len()
                )
                .any(|window| {
                    window
                        == b"MSQL regular expression may not contain more than two escape-double-quote separators\0"
                })
        );
        assert!(
            output
                .windows("WSQL function \"substring\" statement 1\0".len())
                .any(|window| window == b"WSQL function \"substring\" statement 1\0")
        );
    }

    #[test]
    fn terminate_message_releases_backend_locks_and_aborts_open_transaction() {
        let cluster = Cluster::open(temp_dir("terminate_cleanup"), 16).unwrap();
        let db = cluster.connect_database("postgres").unwrap();
        let mut waiter = Session::new(2);

        db.execute(1, "create table widgets (id int4)").unwrap();

        let mut input = startup_packet("postgres", "postgres");
        input.extend(query_message(
            "begin; comment on table widgets is 'held by terminated backend';",
        ));
        input.extend(terminate_message());

        let mut output = Vec::new();
        handle_connection_with_io(Cursor::new(input), &mut output, &cluster, 41).unwrap();

        assert!(cluster.shared().session_activity.read().is_empty());
        assert!(!db.table_locks.has_locks_for_client(41));
        let snapshot = db
            .txns
            .read()
            .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
            .unwrap();
        assert_eq!(snapshot.xmin, snapshot.xmax);

        waiter.execute(&db, "set statement_timeout = '1s'").unwrap();
        match waiter.execute(&db, "select count(*) from widgets").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int64(0)]]);
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn startup_reports_server_version_18_3() {
        let cluster = Cluster::open(temp_dir("startup_server_version"), 16).unwrap();
        let mut input = startup_packet("postgres", "postgres");
        input.extend(terminate_message());

        let mut output = Vec::new();
        handle_connection_with_io(Cursor::new(input), &mut output, &cluster, 41).unwrap();

        assert_eq!(
            parameter_status_value(&output, "server_version").as_deref(),
            Some("18.3")
        );
    }

    #[test]
    fn simple_query_listener_receives_notification_response_on_next_interaction() {
        let db = Database::open(temp_dir("simple_query_notification_response"), 16).unwrap();
        let mut listener = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut sender = ConnectionState {
            session: Session::new(1),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(&mut output, &db, &mut listener, "listen alerts;").unwrap();
        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut sender,
            "select pg_notify('alerts', 'hello');",
        )
        .unwrap();

        output.clear();
        handle_query(&mut output, &db, &mut listener, "select 1;").unwrap();

        assert_eq!(
            notification_responses(&output),
            vec![NotificationResponseMessage {
                sender_pid: 1,
                channel: "alerts".to_string(),
                payload: "hello".to_string(),
            }]
        );
    }

    #[test]
    fn extended_protocol_parse_receives_notification_response_on_next_interaction() {
        let cluster = Cluster::open(temp_dir("extended_parse_notification_response"), 16).unwrap();
        let db = cluster.connect_database("postgres").unwrap();
        let (mut listener, server) = start_test_connection_with_cluster(cluster, 2);

        write_packet(&mut listener, &startup_packet("postgres", "postgres"));
        let _ = read_until_ready(&mut listener, "startup");
        write_packet(&mut listener, &query_message("listen alerts"));
        let _ = read_until_ready(&mut listener, "listen");

        let mut sender = Session::new(41);
        sender.execute(&db, "notify alerts, 'hello'").unwrap();

        write_packet(&mut listener, &parse_message("noop_stmt", "select 1"));
        let response = read_available_messages(&mut listener, "parse");

        assert_eq!(
            response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
            vec![b'1', b'A']
        );
        assert_eq!(
            notification_responses_from_messages(&response),
            vec![NotificationResponseMessage {
                sender_pid: 41,
                channel: "alerts".to_string(),
                payload: "hello".to_string(),
            }]
        );

        write_packet(&mut listener, &terminate_message());
        drop(listener);
        server.join().unwrap().unwrap();
    }

    #[test]
    fn flush_message_receives_notification_response_on_next_interaction() {
        let cluster = Cluster::open(temp_dir("flush_notification_response"), 16).unwrap();
        let db = cluster.connect_database("postgres").unwrap();
        let (mut listener, server) = start_test_connection_with_cluster(cluster, 2);

        write_packet(&mut listener, &startup_packet("postgres", "postgres"));
        let _ = read_until_ready(&mut listener, "startup");
        write_packet(&mut listener, &query_message("listen alerts"));
        let _ = read_until_ready(&mut listener, "listen");

        let mut sender = Session::new(7);
        sender.execute(&db, "notify alerts, 'flushed'").unwrap();

        write_packet(&mut listener, &flush_message());
        let response = read_available_messages(&mut listener, "flush");

        assert_eq!(
            response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
            vec![b'A']
        );
        assert_eq!(
            notification_responses_from_messages(&response),
            vec![NotificationResponseMessage {
                sender_pid: 7,
                channel: "alerts".to_string(),
                payload: "flushed".to_string(),
            }]
        );

        write_packet(&mut listener, &terminate_message());
        drop(listener);
        server.join().unwrap().unwrap();
    }

    #[test]
    fn notification_sender_pid_matches_startup_backend_key_data_pid() {
        let cluster = Cluster::open(temp_dir("notification_sender_pid"), 16).unwrap();
        let db = cluster.connect_database("postgres").unwrap();
        let mut startup_input = startup_packet("postgres", "postgres");
        startup_input.extend(terminate_message());
        let mut startup_output = Vec::new();

        handle_connection_with_io(
            Cursor::new(startup_input),
            &mut startup_output,
            &cluster,
            41,
        )
        .unwrap();
        assert_eq!(backend_key_data_pid(&startup_output), Some(41));

        let mut listener = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();
        handle_query(&mut output, &db, &mut listener, "listen alerts;").unwrap();

        let mut sender = Session::new(41);
        sender.execute(&db, "notify alerts, 'pid-check'").unwrap();

        output.clear();
        handle_query(&mut output, &db, &mut listener, "select 1;").unwrap();

        assert_eq!(
            notification_responses(&output),
            vec![NotificationResponseMessage {
                sender_pid: 41,
                channel: "alerts".to_string(),
                payload: "pid-check".to_string(),
            }]
        );
    }

    #[test]
    fn listen_unlisten_and_notify_emit_expected_command_tags() {
        let db = Database::open(temp_dir("async_command_tags"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "listen alerts; unlisten alerts; notify alerts;",
        )
        .unwrap();

        assert_eq!(
            command_complete_tags(&output),
            vec![
                "LISTEN".to_string(),
                "UNLISTEN".to_string(),
                "NOTIFY".to_string(),
            ]
        );
    }

    #[test]
    fn simple_query_handles_multiline_create_role_membership_clause() {
        let db = Database::open(temp_dir("multiline_create_role"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role regress_role_admin createrole;\n\
             create role regress_role_super superuser;\n\
             create role regress_createdb createdb;\n\
             create role regress_createrole createrole;\n\
             create role regress_login login;\n\
             create role regress_inherit inherit;\n\
             create role regress_connection_limit connection limit 5;\n\
             create role regress_encrypted_password encrypted password 'foo';\n\
             create role regress_password_null password null;\n\
             set session authorization regress_role_admin;",
        )
        .unwrap();

        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut state,
            "create role regress_inroles role\n\
\tregress_role_super, regress_createdb, regress_createrole, regress_login,\n\
\tregress_inherit, regress_connection_limit, regress_encrypted_password, regress_password_null;",
        )
        .unwrap();

        assert!(
            db.backend_catcache(2, None)
                .unwrap()
                .authid_rows()
                .into_iter()
                .any(|row| row.rolname == "regress_inroles")
        );
    }

    #[test]
    fn rewrite_shobj_description_handles_regrole_literal() {
        let rewritten =
            rewrite_regression_sql("select shobj_description('app_role'::regrole, 'pg_authid')")
                .into_owned();
        assert!(rewritten.contains("select oid from pg_authid where rolname = 'app_role'"));
        assert!(!rewritten.contains("::regrole"));
    }

    #[test]
    fn substitute_params_resolves_regclass_parameters_to_relation_oids() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "widgets",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let sql = substitute_params(
            "select relkind from pg_catalog.pg_class where oid=$1::pg_catalog.regclass",
            &[BoundParam::Text("widgets".into())],
            &catalog,
        );
        assert_eq!(
            sql,
            format!(
                "select relkind from pg_catalog.pg_class where oid={}",
                entry.relation_oid
            )
        );
    }

    #[test]
    fn bind_param_count_uses_highest_sql_parameter_ref() {
        assert_eq!(highest_sql_parameter_ref("select 1"), 0);
        assert_eq!(highest_sql_parameter_ref("select $2, $10, $1"), 10);
        assert_eq!(
            required_bind_param_count(&PreparedStatement {
                sql: "select $2".into(),
                param_type_oids: vec![],
            }),
            2
        );
        assert_eq!(
            required_bind_param_count(&PreparedStatement {
                sql: "select 1".into(),
                param_type_oids: vec![23, 25],
            }),
            2
        );
    }

    #[test]
    fn psql_describe_constraint_query_returns_not_null_rows() {
        let db = Database::open(temp_dir("describe_constraints"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null, note text)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_id_not_null".into()),
                Value::Text("widgets".into()),
                Value::Text("NOT NULL".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_returns_primary_key_and_unique_rows() {
        let db = Database::open(temp_dir("describe_constraints_keys"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("widgets_code_key".into()),
                    Value::Text("widgets".into()),
                    Value::Text("UNIQUE (code)".into()),
                ],
                vec![
                    Value::Text("widgets_id_not_null".into()),
                    Value::Text("widgets".into()),
                    Value::Text("NOT NULL".into()),
                ],
                vec![
                    Value::Text("widgets_pkey".into()),
                    Value::Text("widgets".into()),
                    Value::Text("PRIMARY KEY (id)".into()),
                ],
            ]
        );
    }

    #[test]
    fn psql_describe_constraint_query_prints_without_overlaps() {
        let db = Database::open(temp_dir("describe_constraints_without_overlaps"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table temporal_widgets (\
                id int4, \
                valid_at int4range, \
                constraint temporal_widgets_pk primary key (id, valid_at without overlaps)\
             )",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("temporal_widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert!(rows.iter().any(|row| {
            row == &vec![
                Value::Text("temporal_widgets_pk".into()),
                Value::Text("temporal_widgets".into()),
                Value::Text("PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)".into()),
            ]
        }));
    }

    #[test]
    fn psql_describe_constraint_query_returns_check_rows() {
        let db = Database::open(temp_dir("describe_constraints_check"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4, note text constraint widgets_note_nonempty check (note <> ''))",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_note_nonempty".into()),
                Value::Text("widgets".into()),
                Value::Text("CHECK (note <> '')".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_lookup_query_uses_visible_namespace_name() {
        let db = Database::open(temp_dir("describe_lookup_temp"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create temp table widgets (id int4 not null)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = "select c.oid, n.nspname, c.relname \
             from pg_catalog.pg_class c \
             left join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.relkind in ('r','p','v','m','S','f','') \
             and pg_catalog.pg_table_is_visible(c.oid) \
             and c.relname operator(pg_catalog.~) '^(widgets)$'";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Int32(entry.relation_oid as i32),
                Value::Text("pg_temp_1".into()),
                Value::Text("widgets".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_uses_qualified_visible_name_when_needed() {
        let db = Database::open(temp_dir("describe_constraints_temp_qual"), 16).unwrap();
        db.execute(1, "create table widgets (id int4 not null, note text)")
            .unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create temp table widgets (id int4 not null, note text)",
            )
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("pg_temp.widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_id_not_null".into()),
                Value::Text("pg_temp_1.widgets".into()),
                Value::Text("NOT NULL".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_matches_r_alias_shape() {
        let db = Database::open(temp_dir("describe_constraints_r_alias"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT true as sametable, conname, \
                 pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \
                 conrelid::pg_catalog.regclass AS ontable \
             FROM pg_catalog.pg_constraint r \
             WHERE r.conrelid = '{}' AND r.contype = 'f' \
             ORDER BY conname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn psql_describe_columns_query_matches_verbose_view_shape() {
        let db = Database::open(temp_dir("describe_columns_view_verbose"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4, note text)")
            .unwrap();
        db.execute(1, "create view widget_view as select * from widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widget_view")
            .unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
                    FROM pg_catalog.pg_attrdef d \
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
                 a.attnotnull, \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
                 a.attidentity, \
                 a.attgenerated, \
                 a.attstorage, \
                 pg_catalog.col_description(a.attrelid, a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 9);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 9));
        assert_eq!(rows[0][7], Value::InternalChar(b'p'));
        assert_eq!(rows[1][7], Value::InternalChar(b'x'));
        assert_eq!(rows[0][8], Value::Null);
    }

    #[test]
    fn psql_describe_columns_query_matches_verbose_table_shape() {
        let db = Database::open(temp_dir("describe_columns_table_verbose"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4, note text)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
                    FROM pg_catalog.pg_attrdef d \
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
                 a.attnotnull, \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
                 a.attidentity, \
                 a.attgenerated, \
                 a.attstorage, \
                 a.attcompression AS attcompression, \
                 CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, \
                 pg_catalog.col_description(a.attrelid, a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 11);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 11));
        assert_eq!(rows[0][7], Value::InternalChar(b'p'));
        assert_eq!(rows[0][8], Value::InternalChar(0));
        assert_eq!(rows[0][9], Value::Null);
    }

    #[test]
    fn psql_describe_columns_query_formats_pg18_serial_defaults_like_postgres() {
        let db = Database::open(temp_dir("describe_columns_serial_verbose"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table widgets (id serial primary key, note text)",
            )
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
                    FROM pg_catalog.pg_attrdef d \
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
                 a.attnotnull, \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
                 a.attidentity, \
                 a.attgenerated, \
                 a.attstorage, \
                 a.attcompression AS attcompression, \
                 CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, \
                 pg_catalog.col_description(a.attrelid, a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 11);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0][2],
            Value::Text("nextval('widgets_id_seq'::regclass)".into())
        );
        assert_eq!(rows[0][3], Value::Bool(true));
        assert_eq!(rows[0][7], Value::InternalChar(b'p'));
        assert_eq!(rows[0][8], Value::InternalChar(0));
        assert_eq!(rows[0][9], Value::Null);
    }

    #[test]
    fn psql_describe_indexes_query_returns_primary_and_unique_rows() {
        let db = Database::open(temp_dir("describe_indexes_footer"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT c2.relname, i.indisprimary, i.indisunique, \
                 i.indisclustered, i.indisvalid, \
                 pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), \
                 pg_catalog.pg_get_constraintdef(con.oid, true), \
                 contype, condeferrable, condeferred, \
                 i.indisreplident, c2.reltablespace, false AS conperiod \
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i \
             LEFT JOIN pg_catalog.pg_constraint con \
               ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p', 'u', 'x')) \
             WHERE c.oid = '{}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid \
             ORDER BY i.indisprimary DESC, c2.relname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Text("widgets_pkey".into()));
        assert_eq!(rows[0][6], Value::Text("PRIMARY KEY (id)".into()));
        assert!(matches!(&rows[0][5], Value::Text(text) if text.contains("USING btree (id)")));
        assert_eq!(rows[1][0], Value::Text("widgets_code_key".into()));
        assert_eq!(rows[1][6], Value::Text("UNIQUE (code)".into()));
        assert!(matches!(&rows[1][5], Value::Text(text) if text.contains("USING btree (code)")));
    }

    #[test]
    fn psql_describe_indexes_query_marks_without_overlaps_indexes() {
        let db = Database::open(temp_dir("describe_indexes_without_overlaps"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table temporal_widgets (\
                id int4, \
                valid_at int4range, \
                constraint temporal_widgets_pk primary key (id, valid_at without overlaps)\
             )",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("temporal_widgets")
            .unwrap();

        let sql = format!(
            "SELECT c2.relname, i.indisprimary, i.indisunique, \
                 i.indisclustered, i.indisvalid, \
                 pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), \
                 pg_catalog.pg_get_constraintdef(con.oid, true), \
                 contype, condeferrable, condeferred, \
                 i.indisreplident, c2.reltablespace, false AS conperiod \
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i \
             LEFT JOIN pg_catalog.pg_constraint con \
               ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p', 'u', 'x')) \
             WHERE c.oid = '{}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid \
             ORDER BY i.indisprimary DESC, c2.relname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("temporal_widgets_pk".into()));
        assert!(
            matches!(&rows[0][5], Value::Text(text) if text.contains("USING gist (id, valid_at)"))
        );
        assert_eq!(
            rows[0][6],
            Value::Text("PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)".into())
        );
        assert_eq!(rows[0][12], Value::Bool(true));
    }

    #[test]
    fn psql_describe_columns_query_formats_expression_index_columns_like_postgres() {
        let db = Database::open(temp_dir("describe_expression_index_columns"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table attmp (a int4, d float8, e float8, b name)")
            .unwrap();
        db.execute(1, "create index attmp_idx on attmp (a, (d + e), b)")
            .unwrap();
        db.execute(
            1,
            "alter index attmp_idx alter column 2 set statistics 1000",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("attmp_idx")
            .unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 false AS is_key, \
                 pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, true) AS indexdef, \
                 a.attstorage, \
                 CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Text("a".into()));
        assert_eq!(rows[0][1], Value::Text("integer".into()));
        assert_eq!(rows[1][0], Value::Text("expr".into()));
        assert_eq!(rows[1][1], Value::Text("double precision".into()));
        assert_eq!(rows[1][3], Value::Text("(d + e)".into()));
        assert_eq!(rows[1][5], Value::Int16(1000));
        assert_eq!(rows[2][0], Value::Text("b".into()));
        assert_eq!(rows[2][1], Value::Text("cstring".into()));
    }

    #[test]
    fn psql_describe_constraint_query_matches_referenced_by_partition_shape() {
        let db = Database::open(temp_dir("describe_constraints_referenced_by"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT conname, conrelid::pg_catalog.regclass AS ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) AS condef \
             FROM pg_catalog.pg_constraint c \
             WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{0}') \
                                 UNION ALL VALUES ('{0}'::pg_catalog.regclass)) \
               AND contype = 'f' AND conparentid = 0 \
             ORDER BY conname",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 3);
        assert!(rows.is_empty());
    }

    #[test]
    fn psql_get_viewdef_query_returns_return_rule_sql() {
        let db = Database::open(temp_dir("describe_viewdef"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4)").unwrap();
        db.execute(1, "create view widget_view as select id from widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widget_view")
            .unwrap();

        let sql = format!(
            "SELECT pg_catalog.pg_get_viewdef('{}'::pg_catalog.oid, true);",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![Value::Text(" SELECT id\n   FROM widgets;".into())]]
        );
    }

    #[test]
    fn psql_index_obj_description_query_returns_null_comments() {
        let db = Database::open(temp_dir("describe_index_comments"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();

        let sql = "SELECT indexrelid::regclass::text as index, \
             obj_description(indexrelid, 'pg_class') as comment \
             FROM pg_index where indrelid = 'widgets'::regclass ORDER BY 1, 2;";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0][1], Value::Null));
        assert!(matches!(rows[1][1], Value::Null));
    }

    #[test]
    fn psql_relation_obj_description_query_reports_relation_comments() {
        let db = Database::open(temp_dir("describe_relation_comments"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4 not null)")
            .unwrap();
        session
            .execute(&db, "comment on table widgets is 'hello world'")
            .unwrap();
        session
            .execute(
                &db,
                "create temp table old_oids as \
                 select relname, oid as oldoid, relfilenode as oldfilenode \
                 from pg_class where relname like 'widgets%'",
            )
            .unwrap();

        let sql = "select relname, \
             c.oid = oldoid as orig_oid, \
             case relfilenode \
               when 0 then 'none' \
               when c.oid then 'own' \
               when oldfilenode then 'orig' \
               else 'OTHER' \
             end as storage, \
             obj_description(c.oid, 'pg_class') as desc \
             from pg_class c left join old_oids using (relname) \
             where relname like 'widgets%' \
             order by relname";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("widgets".into()));
        assert_eq!(rows[0][3], Value::Text("hello world".into()));
    }

    #[test]
    fn psql_publication_list_query_runs_via_native_sql() {
        let db = Database::open(temp_dir("describe_publication_list"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        let sql = "SELECT pubname AS \"Name\", \
             pg_catalog.pg_get_userbyid(pubowner) AS \"Owner\", \
             puballtables AS \"All tables\", \
             pubinsert AS \"Inserts\", \
             pubupdate AS \"Updates\", \
             pubdelete AS \"Deletes\", \
             pubtruncate AS \"Truncates\", \
             (CASE pubgencols \
                WHEN 'n' THEN 'none' \
                WHEN 's' THEN 'stored' \
              END) AS \"Generated columns\", \
             pubviaroot AS \"Via root\" \
             FROM pg_catalog.pg_publication \
             ORDER BY 1";
        let rows = match session.execute(&db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("pub".into()),
                Value::Text("postgres".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Text("none".into()),
                Value::Bool(false),
            ]]
        );
    }

    #[test]
    fn psql_publication_footer_query_reports_relation_publications() {
        let db = Database::open(temp_dir("describe_publication_footer"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "SELECT pubname \
                 , NULL \
                 , NULL \
             FROM pg_catalog.pg_publication p \
                  JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid \
                  JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid \
             WHERE pc.oid ='{}' and pg_catalog.pg_relation_is_publishable('{}') \
             UNION \
             SELECT pubname \
                 , pg_get_expr(pr.prqual, c.oid) \
                 , (CASE WHEN pr.prattrs IS NOT NULL THEN \
                     (SELECT string_agg(attname, ', ') \
                        FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, \
                             pg_catalog.pg_attribute \
                       WHERE attrelid = pr.prrelid AND attnum = prattrs[s]) \
                    ELSE NULL END) \
             FROM pg_catalog.pg_publication p \
                  JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid \
                  JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid \
             WHERE pr.prrelid = '{}' \
             UNION \
             SELECT pubname \
                 , NULL \
                 , NULL \
             FROM pg_catalog.pg_publication p \
             WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('{}') \
             ORDER BY 1",
            entry.relation_oid, entry.relation_oid, entry.relation_oid, entry.relation_oid
        );
        let rows = match session.execute(&db, &sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(
            rows,
            vec![vec![Value::Text("pub".into()), Value::Null, Value::Null,]]
        );
    }

    #[test]
    fn psql_publication_detail_query_runs_via_native_sql() {
        let db = Database::open(temp_dir("describe_publication_detail"), 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create publication pub").unwrap();

        let sql = "SELECT oid, pubname, \
             pg_catalog.pg_get_userbyid(pubowner) AS owner, \
             puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, \
             (CASE pubgencols WHEN 'n' THEN 'none' WHEN 's' THEN 'stored' END) AS \"Generated columns\", \
             pubviaroot \
             FROM pg_catalog.pg_publication \
             WHERE pubname OPERATOR(pg_catalog.~) '^(pub)$' COLLATE pg_catalog.default \
             ORDER BY 2";
        let rows = match session.execute(&db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("pub".into()));
        assert_eq!(rows[0][2], Value::Text("postgres".into()));
        assert_eq!(rows[0][3], Value::Bool(false));
        assert_eq!(rows[0][8], Value::Text("none".into()));
        assert_eq!(rows[0][9], Value::Bool(false));
    }

    #[test]
    fn psql_publication_detail_footer_queries_run_via_native_sql() {
        let db = Database::open(temp_dir("describe_publication_detail_footers"), 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets, tables in schema pub_test",
            )
            .unwrap();
        let publication_oid = db
            .backend_catcache(1, None)
            .unwrap()
            .publication_row_by_name("pub")
            .map(|row| row.oid)
            .unwrap();

        let tables_sql = format!(
            "SELECT n.nspname, c.relname, \
                 pg_get_expr(pr.prqual, c.oid), \
                 (CASE WHEN pr.prattrs IS NOT NULL THEN \
                     pg_catalog.array_to_string( \
                       ARRAY(SELECT attname \
                               FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, \
                                    pg_catalog.pg_attribute \
                              WHERE attrelid = c.oid AND attnum = prattrs[s]), ', ') \
                  ELSE NULL END) \
             FROM pg_catalog.pg_class c, \
                  pg_catalog.pg_namespace n, \
                  pg_catalog.pg_publication_rel pr \
             WHERE c.relnamespace = n.oid \
               AND c.oid = pr.prrelid \
               AND pr.prpubid = '{}' \
             ORDER BY 1,2",
            publication_oid
        );
        let table_rows = match session.execute(&db, &tables_sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(
            table_rows,
            vec![vec![
                Value::Text("public".into()),
                Value::Text("widgets".into()),
                Value::Null,
                Value::Null,
            ]]
        );

        let schemas_sql = format!(
            "SELECT n.nspname \
             FROM pg_catalog.pg_namespace n \
                  JOIN pg_catalog.pg_publication_namespace pn ON n.oid = pn.pnnspid \
             WHERE pn.pnpubid = '{}' \
             ORDER BY 1",
            publication_oid
        );
        let schema_rows = match session.execute(&db, &schemas_sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(schema_rows, vec![vec![Value::Text("pub_test".into())]]);
    }

    #[test]
    fn publication_obj_description_query_reads_pg_description() {
        let db = Database::open(temp_dir("describe_publication_comment"), 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create publication pub").unwrap();
        session
            .execute(&db, "comment on publication pub is 'hello world'")
            .unwrap();

        let sql = "SELECT obj_description(p.oid, 'pg_publication') \
             FROM pg_catalog.pg_publication p \
             WHERE p.pubname = 'pub'";
        let rows = match session.execute(&db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(rows, vec![vec![Value::Text("hello world".into())]]);
    }

    #[test]
    fn psql_col_description_query_returns_null_without_column_comments() {
        let db = Database::open(temp_dir("describe_column_comment"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4)").unwrap();

        let sql = "SELECT col_description('widgets'::regclass, 1) as comment;";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows, vec![vec![Value::Null]]);
    }

    #[test]
    fn psql_describe_tableinfo_query_reports_visible_indexes() {
        let db = Database::open(temp_dir("describe_tableinfo_indexes"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        db.execute(1, "create index widgets_id_idx on widgets (id)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select c.relchecks, c.relkind, c.relhasindex \
                 from pg_catalog.pg_class c \
                 where c.oid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows[0][2], Value::Bool(true));
    }

    #[test]
    fn psql_describe_tableinfo_query_reports_visible_access_method() {
        let db = Database::open(temp_dir("describe_tableinfo_am"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        db.execute(1, "create index widgets_id_idx on widgets (id)")
            .unwrap();
        let index = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets_id_idx")
            .unwrap();

        let sql = format!(
            "select c.relchecks, c.relkind, c.relhasindex \
                 from pg_catalog.pg_class c \
                 where c.oid = '{}'",
            index.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows[0][14], Value::Text("btree".into()));
    }

    #[test]
    fn psql_describe_tableinfo_query_hides_default_heap_access_method() {
        let db = Database::open(temp_dir("describe_tableinfo_heap_am"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        let table = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select c.relchecks, c.relkind, c.relhasindex \
                 from pg_catalog.pg_class c \
                 where c.oid = '{}'",
            table.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows[0][14], Value::Null);
    }

    #[test]
    fn psql_describe_statistics_query_returns_statistics_objects_for_relation() {
        let db = Database::open(temp_dir("describe_statistics_objects"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (a int4, b int4)")
            .unwrap();
        db.execute(1, "create statistics widgets_stats on a, b from widgets")
            .unwrap();
        db.execute(1, "alter statistics widgets_stats set statistics 0")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select oid, stxrelid::pg_catalog.regclass, \
                 stxnamespace::pg_catalog.regnamespace::pg_catalog.text as nsp, stxname, \
                 pg_catalog.pg_get_statisticsobjdef_columns(oid) as columns, \
                 'd' = any(stxkind) as ndist_enabled, \
                 'f' = any(stxkind) as deps_enabled, \
                 'm' = any(stxkind) as mcv_enabled, \
                 stxstattarget \
             from pg_catalog.pg_statistic_ext \
             where stxrelid = '{}' \
             order by nsp, stxname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("widgets".into()));
        assert_eq!(rows[0][2], Value::Text("public".into()));
        assert_eq!(rows[0][3], Value::Text("widgets_stats".into()));
        assert_eq!(rows[0][4], Value::Text("a, b".into()));
        assert_eq!(rows[0][5], Value::Bool(true));
        assert_eq!(rows[0][6], Value::Bool(true));
        assert_eq!(rows[0][7], Value::Bool(true));
        assert_eq!(rows[0][8], Value::Int16(0));
    }

    #[test]
    fn statistics_catalog_query_returns_null_data_columns_for_known_object() {
        let db = Database::open(temp_dir("statistics_catalog_query"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (a int4, b int4)")
            .unwrap();
        db.execute(1, "create statistics widgets_stats on a, b from widgets")
            .unwrap();

        let sql = "select stxname, stxdndistinct, stxddependencies, stxdmcv, stxdinherit \
             from pg_statistic_ext s left join pg_statistic_ext_data d on (d.stxoid = s.oid) \
             where s.stxname = 'widgets_stats'";
        let (_, rows) = execute_statistics_catalog_query(&db, &session, sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_stats".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]]
        );
    }

    #[test]
    fn extract_quoted_error_value_handles_date_input_messages() {
        assert_eq!(
            extract_quoted_error_value("invalid input syntax for type date: \"garbage\""),
            Some("garbage")
        );
        assert_eq!(
            extract_quoted_error_value("date/time field value out of range: \"1997-02-29\""),
            Some("1997-02-29")
        );
        assert_eq!(
            extract_quoted_error_value("date out of range: \"5874898-01-01\""),
            Some("5874898-01-01")
        );
    }

    #[test]
    fn exec_error_detail_reports_publication_generated_columns_valid_values() {
        let err = ExecError::Parse(
            crate::backend::parser::ParseError::InvalidPublicationParameterValue {
                parameter: "publish_generated_columns".into(),
                value: "foo".into(),
            },
        );

        assert_eq!(
            exec_error_detail(&err),
            Some("Valid values are \"none\" and \"stored\".")
        );
    }

    #[test]
    fn exec_error_position_points_at_second_conflicting_publication_option() {
        let sql = "create publication pub with (publish_via_partition_root = true, publish_via_partition_root = false)";
        let err = ExecError::Parse(
            crate::backend::parser::ParseError::ConflictingOrRedundantOptions {
                option: "publish_via_partition_root".into(),
            },
        );

        assert_eq!(
            exec_error_position(sql, &err),
            sql.to_ascii_lowercase()
                .match_indices("publish_via_partition_root")
                .nth(1)
                .map(|(index, _)| index + 1)
        );
    }

    #[test]
    fn exec_error_position_finds_quoted_publication_schema_name_case_insensitively() {
        let sql = "create publication pub for tables in schema \"Foo\".\"Bar\"";
        let err = ExecError::Parse(
            crate::backend::parser::ParseError::InvalidPublicationSchemaName("Foo.Bar".into()),
        );

        assert_eq!(
            exec_error_position(sql, &err),
            sql.find("\"Foo\".\"Bar\"").map(|index| index + 1)
        );
    }

    #[test]
    fn exec_error_position_points_at_date_literal_contents() {
        let sql = "select date '1997-02-29';";
        let err = ExecError::DetailedError {
            message: "date/time field value out of range: \"1997-02-29\"".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        };

        assert_eq!(exec_error_position(sql, &err), Some(14));
    }

    #[test]
    fn exec_error_position_points_at_string_literal_quote_for_cast_errors() {
        let sql = "select '25:00:00'::time;";
        let err = ExecError::DetailedError {
            message: "date/time field value out of range: \"25:00:00\"".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        };

        assert_eq!(exec_error_position(sql, &err), Some(8));
    }

    #[test]
    fn exec_error_position_omits_empty_jsonb_tsvector_flag() {
        let sql = "select jsonb_to_tsvector('english', '{\"a\": \"aaa\"}'::jsonb, '\"\"');";
        let err = ExecError::DetailedError {
            message: "wrong flag in flag array: \"\"".into(),
            detail: None,
            hint: Some(
                "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\"."
                    .into(),
            ),
            sqlstate: "22023",
        };

        assert_eq!(exec_error_position(sql, &err), None);
    }

    #[test]
    fn exec_error_position_points_at_trigger_when_refs_for_detailed_errors() {
        for (sql, message, token) in [
            (
                "create trigger t before insert on items for each row when (OLD.a <> NEW.a) execute function f()",
                "INSERT trigger's WHEN condition cannot reference OLD values",
                "OLD.",
            ),
            (
                "create trigger t before delete on items for each row when (OLD.a <> NEW.a) execute function f()",
                "DELETE trigger's WHEN condition cannot reference NEW values",
                "NEW.",
            ),
            (
                "create trigger t before update on items for each row when (NEW.tableoid <> 0) execute function f()",
                "BEFORE trigger's WHEN condition cannot reference NEW system columns",
                "NEW.tableoid",
            ),
            (
                "create trigger t before update on items for each statement when (OLD.* IS DISTINCT FROM NEW.*) execute function f()",
                "statement trigger's WHEN condition cannot reference column values",
                "OLD.",
            ),
        ] {
            let err = ExecError::DetailedError {
                message: message.into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            };
            assert_eq!(
                exec_error_position(sql, &err),
                find_case_insensitive_token_position(sql, token)
            );
        }
    }

    #[test]
    fn exec_error_position_points_at_operator_for_ambiguous_operator_errors() {
        let sql = "select f1 + time '00:01' from time_tbl";
        let err = ExecError::Parse(crate::backend::parser::ParseError::DetailedError {
            message: "operator is not unique: time without time zone + time without time zone"
                .into(),
            detail: None,
            hint: Some(
                "Could not choose a best candidate operator. You might need to add explicit type casts."
                    .into(),
            ),
            sqlstate: "42725",
        });

        assert_eq!(
            exec_error_position(sql, &err),
            sql.find('+').map(|index| index + 1)
        );
    }

    #[test]
    fn exec_error_position_points_at_subscripted_assignment_target() {
        let sql = "insert into arrtest (b[1:2]) values(now())";
        let err = ExecError::DetailedError {
            message:
                "subscripted assignment to \"b\" requires type integer[] but expression is of type timestamp with time zone"
                    .into(),
            detail: None,
            hint: Some("You will need to rewrite or cast the expression.".into()),
            sqlstate: "42804",
        };

        assert_eq!(exec_error_position(sql, &err), Some(22));
    }

    #[test]
    fn exec_error_position_points_at_single_quoted_json_literal_start() {
        let sql = "SELECT '\"abc'::jsonb;";
        let err = ExecError::JsonInput {
            raw_input: "\"abc".into(),
            message: "invalid input syntax for type json".into(),
            detail: Some("Token \"\"abc\" is invalid.".into()),
            context: Some("JSON data, line 1: \"abc".into()),
            sqlstate: "22P02",
        };

        assert_eq!(exec_error_position(sql, &err), Some(8));
    }

    #[test]
    fn exec_error_position_points_at_dollar_quoted_json_literal_start() {
        let sql = "SELECT $$''$$::jsonb;";
        let err = ExecError::JsonInput {
            raw_input: "''".into(),
            message: "invalid input syntax for type json".into(),
            detail: Some("Token \"'\" is invalid.".into()),
            context: Some("JSON data, line 1: '...".into()),
            sqlstate: "22P02",
        };

        assert_eq!(exec_error_position(sql, &err), Some(8));
    }

    #[test]
    fn exec_error_position_omits_to_number_roman_empty_input() {
        let sql = "SELECT to_number('', 'RN');";
        let err = ExecError::DetailedError {
            message: "invalid input syntax for type numeric: \" \"".into(),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        };

        assert_eq!(exec_error_position(sql, &err), None);
    }

    #[test]
    fn exec_error_position_omits_invalid_size_detail_errors() {
        let sql = "SELECT pg_size_bytes('1 AB');";
        let err = ExecError::DetailedError {
            message: "invalid size: \"1 AB\"".into(),
            detail: Some("Invalid size unit: \"AB\".".into()),
            hint: Some(
                "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\"."
                    .into(),
            ),
            sqlstate: "22023",
        };

        assert_eq!(exec_error_position(sql, &err), None);
    }

    #[test]
    fn exec_error_position_omits_default_toast_compression_guc_errors() {
        let sql = "SET default_toast_compression = 'lz4';";
        let err = ExecError::DetailedError {
            message: "invalid value for parameter \"default_toast_compression\": \"lz4\"".into(),
            detail: None,
            hint: Some("Available values: pglz.".into()),
            sqlstate: "22023",
        };

        assert_eq!(exec_error_position(sql, &err), None);
    }

    #[test]
    fn exec_error_position_points_at_on_update_for_fk_set_null_column_lists() {
        let sql = "CREATE TABLE FKTABLE (tid int, id int, foo int, FOREIGN KEY (tid, foo) REFERENCES PKTABLE ON UPDATE SET NULL (foo));";
        let err = ExecError::Parse(
            crate::backend::parser::ParseError::FeatureNotSupportedMessage(
                "a column list with SET NULL is only supported for ON DELETE actions".into(),
            ),
        );

        assert_eq!(exec_error_position(sql, &err), Some(91));
    }

    #[test]
    fn simple_query_reports_position_for_date_input_error() {
        let db = Database::open(temp_dir("date_error_position"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(&mut output, &db, &mut state, "select date '1997-02-29';").unwrap();

        assert_eq!(first_error_response_position(&output), Some(14));
    }

    #[test]
    fn simple_query_reports_position_for_fk_set_null_column_lists() {
        let db = Database::open(temp_dir("fk_set_null_column_list_position"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "CREATE TABLE FKTABLE (tid int, id int, foo int, FOREIGN KEY (tid, foo) REFERENCES PKTABLE ON UPDATE SET NULL (foo));",
        )
        .unwrap();

        assert_eq!(first_error_response_position(&output), Some(91));
    }

    #[test]
    fn simple_query_reports_position_for_subscripted_assignment_error() {
        let db = Database::open(temp_dir("subscripted_assignment_error_position"), 16).unwrap();
        db.execute(1, "create table arrtest (b int4[][][])")
            .unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "insert into arrtest (b[2]) values(now())",
        )
        .unwrap();

        assert_eq!(first_error_response_position(&output), Some(22));
    }

    #[test]
    fn simple_query_reports_position_for_unsupported_subscript_error() {
        let db = Database::open(temp_dir("unsupported_subscript_error_position"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(&mut output, &db, &mut state, "select (now())[1]").unwrap();

        assert!(output_contains_message(
            &output,
            "cannot subscript type timestamp with time zone because it does not support subscripting"
        ));
        assert_eq!(first_error_response_position(&output), Some(8));
    }

    #[test]
    fn simple_query_reports_duplicate_key_detail_for_unique_array() {
        let db = Database::open(temp_dir("unique_array_detail_simple_query"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "create temp table arr_tbl (f1 int[] unique);",
        )
        .unwrap();

        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut state,
            "insert into arr_tbl values ('{1,2,3}');",
        )
        .unwrap();

        output.clear();
        handle_query(
            &mut output,
            &db,
            &mut state,
            "insert into arr_tbl values ('{1,2,3}');",
        )
        .unwrap();

        assert!(output_contains_message(
            &output,
            "duplicate key value violates unique constraint \"arr_tbl_f1_key\""
        ));
        assert!(output_contains_message(
            &output,
            "Key (f1)=({1,2,3}) already exists."
        ));
    }

    #[test]
    fn simple_query_omits_position_for_to_number_roman_empty_input() {
        let db = Database::open(temp_dir("to_number_roman_empty_input_position"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(&mut output, &db, &mut state, "SELECT to_number('', 'RN');").unwrap();

        assert_eq!(first_error_response_position(&output), None);
    }

    #[test]
    fn simple_query_renders_interval_array_literals_with_interval_text() {
        let db = Database::open(temp_dir("interval_array_literal_output"), 16).unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "select '{0 second,1 hour 42 minutes 20 seconds}'::interval[];",
        )
        .unwrap();

        assert!(
            output
                .windows("{\"@ 0\",\"@ 1 hour 42 mins 20 secs\"}".len())
                .any(|window| window == b"{\"@ 0\",\"@ 1 hour 42 mins 20 secs\"}")
        );
    }

    #[test]
    fn simple_query_reports_program_limit_for_overflowed_array_assignment() {
        let db = Database::open(temp_dir("array_assignment_overflow_query"), 16).unwrap();
        db.execute(1, "create table arr_pk_tbl (pk int4 primary key, f1 int[])")
            .unwrap();
        db.execute(
            1,
            "insert into arr_pk_tbl values (10, '[-2147483648:-2147483647]={1,2}')",
        )
        .unwrap();
        let mut state = ConnectionState {
            session: Session::new(2),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            copy_in: None,
        };
        let mut output = Vec::new();

        handle_query(
            &mut output,
            &db,
            &mut state,
            "update arr_pk_tbl set f1[2147483647] = 42 where pk = 10;",
        )
        .unwrap();

        assert!(
            output
                .windows("C54000\0".len())
                .any(|window| window == b"C54000\0")
        );
    }

    fn split_simple_query_statements_keeps_rule_action_lists_together() {
        let sql = "create rule r as on update to widgets do also (\n    update other set id = new.id where id = old.id;\n    delete from audit where id = old.id\n);\nselect 1;\n";

        assert_eq!(
            split_simple_query_statements(sql),
            vec![
                "create rule r as on update to widgets do also (\n    update other set id = new.id where id = old.id;\n    delete from audit where id = old.id\n);",
                "\nselect 1;",
                "\n",
            ]
        );
    }

    #[test]
    fn send_queued_notices_emits_backend_warning_severity() {
        clear_backend_notices();
        crate::backend::utils::misc::notices::push_warning("lowering statistics target to 10000");
        let mut buf = Vec::new();
        send_queued_notices(&mut buf).unwrap();
        let payload = String::from_utf8_lossy(&buf);
        assert!(payload.contains("WARNING"));
        assert!(payload.contains("01000"));
        assert!(payload.contains("lowering statistics target to 10000"));
    }
}
