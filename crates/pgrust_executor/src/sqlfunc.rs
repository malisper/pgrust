#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlFunctionBodyError {
    UnexpectedEof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFunctionSubstitutionError<E> {
    InvalidParameterReference,
    ParameterOutOfRange { position: usize },
    Render(E),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFunctionMetadataError {
    InvalidArgumentMetadata { metadata: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlFunctionRecordFieldTypeMismatch {
    pub ordinal: usize,
    pub returned_type: SqlType,
    pub expected_type: SqlType,
}

pub fn starts_with_sql_command(sql: &str, command: &str) -> bool {
    let Some(rest) = sql.strip_prefix(command) else {
        return false;
    };
    rest.chars()
        .next()
        .map(|ch| ch.is_whitespace() || ch == '(' || ch == ';')
        .unwrap_or(true)
}

pub fn normalized_sql_function_body(source: &str) -> String {
    let body = source.trim().trim_end_matches(';').trim();
    if body
        .get(.."return".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("return"))
        && body
            .get("return".len()..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| ch.is_whitespace())
    {
        return format!("select {}", body["return".len()..].trim());
    }
    sql_standard_function_body_inner(body)
        .unwrap_or(body)
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string()
}

pub fn sql_function_statement_needs_database_executor(statement: &str) -> bool {
    let lower = statement.trim_start().to_ascii_lowercase();
    starts_with_sql_command(&lower, "create")
        || starts_with_sql_command(&lower, "alter")
        || starts_with_sql_command(&lower, "grant")
        || starts_with_sql_command(&lower, "revoke")
}

pub fn normalize_sql_function_statement_for_execution(
    statement: &str,
) -> std::borrow::Cow<'_, str> {
    let trimmed = statement.trim_start();
    if trimmed
        .get(.."return".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("return"))
        && trimmed
            .get("return".len()..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| ch.is_whitespace())
    {
        std::borrow::Cow::Owned(format!("select {}", trimmed["return".len()..].trim()))
    } else {
        std::borrow::Cow::Borrowed(statement)
    }
}

pub fn sql_function_sets_row_security_off(sql: &str) -> bool {
    let compact: String = sql
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect();
    compact.contains("set_config('row_security','false',")
        || compact.contains("set_config('row_security','off',")
}

pub fn sql_function_body_is_inline_select_candidate(body: &str) -> bool {
    let lower = body.trim_start().to_ascii_lowercase();
    starts_with_sql_command(&lower, "select")
        || starts_with_sql_command(&lower, "with")
        || starts_with_sql_command(&lower, "values")
}

pub fn split_sql_function_body(body: &str) -> Result<Vec<String>, SqlFunctionBodyError> {
    let body = sql_standard_function_body_inner(body).unwrap_or(body);
    let mut statements = Vec::new();
    let mut start = 0usize;
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = scan_sql_delimited_end(bytes, i, b'\'')?;
                continue;
            }
            b'"' => {
                i = scan_sql_delimited_end(bytes, i, b'"')?;
                continue;
            }
            b'$' => {
                if let Some(end) = scan_sql_dollar_string_end(body, i) {
                    i = end;
                    continue;
                }
            }
            b';' => {
                let statement = body[start..i].trim();
                if !statement.is_empty() && !statement.eq_ignore_ascii_case("end") {
                    statements.push(statement.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let statement = body[start..].trim();
    if !statement.is_empty() && !statement.eq_ignore_ascii_case("end") {
        statements.push(statement.to_string());
    }
    Ok(statements)
}

fn scan_sql_delimited_end(
    bytes: &[u8],
    start: usize,
    delimiter: u8,
) -> Result<usize, SqlFunctionBodyError> {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
                continue;
            }
            return Ok(i + 1);
        }
        i += 1;
    }
    Err(SqlFunctionBodyError::UnexpectedEof)
}

fn scan_sql_dollar_string_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut tag_end = start + 1;
    while tag_end < bytes.len() && bytes[tag_end] != b'$' {
        let ch = bytes[tag_end] as char;
        if !(ch == '_' || ch.is_ascii_alphanumeric()) {
            return None;
        }
        tag_end += 1;
    }
    if tag_end >= bytes.len() {
        return None;
    }
    let tag = &input[start..=tag_end];
    let rest = &input[tag_end + 1..];
    let closing = rest.find(tag)?;
    Some(tag_end + 1 + closing + tag.len())
}

pub fn sql_standard_function_body_inner(body: &str) -> Option<&str> {
    let trimmed = body.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("begin atomic") {
        return None;
    }
    let without_trailing_semicolon = trimmed.trim_end_matches(';').trim_end();
    let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
    let end = if lowered_without_semicolon.ends_with("end") {
        without_trailing_semicolon.len().saturating_sub("end".len())
    } else {
        trimmed.len()
    };
    trimmed.get("begin atomic".len()..end).map(str::trim)
}

pub fn substitute_sql_fragment_outside_quotes(
    input: &str,
    needle: &str,
    replacement: &str,
) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            _ if sql_fragment_matches_at(input, needle, i) => {
                out.push_str(replacement);
                i += needle.len();
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    out
}

fn sql_fragment_matches_at(input: &str, needle: &str, index: usize) -> bool {
    let Some(candidate) = input.get(index..index.saturating_add(needle.len())) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(needle) {
        return false;
    }
    let before = input[..index].chars().next_back();
    let after = input[index + needle.len()..].chars().next();
    !before.is_some_and(|ch| ch == '.' || is_sql_identifier_char(ch))
        && !after.is_some_and(is_sql_identifier_char)
}

fn is_sql_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

pub fn substitute_named_arg(input: &str, name: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < chars.len() {
        let ch = chars[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            _ if ch == '_' || ch.is_ascii_alphabetic() => {
                let start = i;
                i += 1;
                while i < chars.len() {
                    let ch = chars[i] as char;
                    if ch == '_' || ch.is_ascii_alphanumeric() {
                        i += 1;
                    } else {
                        break;
                    }
                }
                let ident = &input[start..i];
                if ident.eq_ignore_ascii_case(name) {
                    if named_arg_occurrence_is_window_unbounded_keyword(input, name, i)
                        || named_arg_occurrence_is_qualified_field(input, start)
                        || named_arg_occurrence_is_column_alias_list(input, start)
                        || named_arg_occurrence_is_update_set_target(input, start, i)
                    {
                        out.push_str(ident);
                    } else {
                        out.push_str(replacement);
                    }
                } else {
                    out.push_str(ident);
                }
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    out
}

pub fn substitute_positional_args_with_renderer<E>(
    input: &str,
    arg_count: usize,
    mut render_arg: impl FnMut(usize) -> Result<String, E>,
) -> Result<String, SqlFunctionSubstitutionError<E>> {
    let mut out = String::with_capacity(input.len());
    let chars = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < chars.len() {
        let ch = chars[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            '$' => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end] as char).is_ascii_digit() {
                    end += 1;
                }
                if end == start {
                    out.push(ch);
                    i += 1;
                    continue;
                }
                let position = input[start..end]
                    .parse::<usize>()
                    .map_err(|_| SqlFunctionSubstitutionError::InvalidParameterReference)?;
                if position == 0 || position > arg_count {
                    return Err(SqlFunctionSubstitutionError::ParameterOutOfRange { position });
                }
                out.push_str(
                    &render_arg(position - 1).map_err(SqlFunctionSubstitutionError::Render)?,
                );
                i = end;
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    Ok(out)
}

pub fn parse_proc_argtype_oids(argtypes: &str) -> Result<Vec<u32>, SqlFunctionMetadataError> {
    if argtypes.trim().is_empty() {
        return Ok(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| {
            part.parse::<u32>()
                .map_err(|_| SqlFunctionMetadataError::InvalidArgumentMetadata {
                    metadata: argtypes.into(),
                })
        })
        .collect()
}

pub fn sql_function_is_array_append_transition(
    row: &PgProcRow,
) -> Result<bool, SqlFunctionMetadataError> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    Ok(matches!(
        declared_oids.as_slice(),
        [ANYARRAYOID, ANYELEMENTOID] | [ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID]
    ))
}

pub fn proc_input_arg_type_oids(row: &PgProcRow) -> Vec<u32> {
    row.proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .collect()
}

pub fn effective_sql_function_arg_type_oids(
    row: &PgProcRow,
    arg_count: usize,
    call_arg_type_oids: Option<&[u32]>,
) -> Vec<u32> {
    let declared = proc_input_arg_type_oids(row);
    (0..arg_count)
        .map(|index| {
            call_arg_type_oids
                .and_then(|oids| oids.get(index).copied())
                .filter(|oid| *oid != 0)
                .or_else(|| declared.get(index).copied())
                .unwrap_or(0)
        })
        .collect()
}

pub fn is_sql_function_polymorphic_type_oid(type_oid: u32) -> bool {
    matches!(
        type_oid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

pub fn is_polymorphic_sql_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::AnyArray
            | SqlTypeKind::AnyElement
            | SqlTypeKind::AnyRange
            | SqlTypeKind::AnyMultirange
            | SqlTypeKind::AnyCompatible
            | SqlTypeKind::AnyCompatibleArray
            | SqlTypeKind::AnyCompatibleRange
            | SqlTypeKind::AnyCompatibleMultirange
            | SqlTypeKind::AnyEnum
    )
}

pub fn merge_polymorphic_runtime_subtype(current: &mut Option<SqlType>, inferred: SqlType) -> bool {
    match *current {
        None => {
            *current = Some(inferred);
            true
        }
        Some(existing) => sql_types_match_for_polymorphic_runtime(existing, inferred),
    }
}

pub fn sql_types_match_for_polymorphic_runtime(left: SqlType, right: SqlType) -> bool {
    left.kind == right.kind
        && left.is_array == right.is_array
        && (left.type_oid == 0 || right.type_oid == 0 || left.type_oid == right.type_oid)
}

pub fn can_coerce_to_compatible_runtime_anchor(actual: SqlType, target: SqlType) -> bool {
    if sql_types_match_for_polymorphic_runtime(actual, target) {
        return true;
    }
    if actual.is_array || target.is_array {
        return false;
    }
    matches!(
        (actual.kind, target.kind),
        (SqlTypeKind::Int2, SqlTypeKind::Int4)
            | (SqlTypeKind::Int2, SqlTypeKind::Int8)
            | (SqlTypeKind::Int2, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int2, SqlTypeKind::Float4)
            | (SqlTypeKind::Int2, SqlTypeKind::Float8)
            | (SqlTypeKind::Int4, SqlTypeKind::Int8)
            | (SqlTypeKind::Int4, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int4, SqlTypeKind::Float4)
            | (SqlTypeKind::Int4, SqlTypeKind::Float8)
            | (SqlTypeKind::Int8, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int8, SqlTypeKind::Float4)
            | (SqlTypeKind::Int8, SqlTypeKind::Float8)
            | (SqlTypeKind::Float4, SqlTypeKind::Float8)
    )
}

pub fn sql_function_return_types_match(returned_type: SqlType, expected_type: SqlType) -> bool {
    returned_type.kind == expected_type.kind
        && returned_type.is_array == expected_type.is_array
        && (returned_type.type_oid == expected_type.type_oid
            || returned_type.type_oid == 0
            || expected_type.type_oid == 0)
}

pub fn quote_sql_identifier(name: &str) -> String {
    if is_plain_sql_identifier(name) {
        return name.into();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn is_plain_sql_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_lowercase())
        && chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

pub fn quote_sql_string(text: &str) -> String {
    let escaped = text.replace('\'', "''");
    if text.contains('\\') {
        let escaped = escaped.replace('\\', "\\\\");
        format!("E'{escaped}'")
    } else {
        format!("'{escaped}'")
    }
}

pub fn sql_function_outputs_single_composite_column(output_columns: &[QueryColumn]) -> bool {
    output_columns.len() == 1
        && matches!(
            output_columns[0].sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
}

pub fn sql_function_single_value_is_whole_result(
    runtime_result_type: Option<SqlType>,
    output_columns: &[QueryColumn],
    values: &[Value],
) -> bool {
    if matches!(values, [Value::Record(_)]) {
        return true;
    }
    let Some(return_type) = runtime_result_type else {
        return true;
    };
    if !matches!(
        return_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) {
        return true;
    }
    output_columns.len() == 1
        && matches!(
            output_columns[0].sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
}

pub fn should_pack_sql_set_returning_record_row(
    output_columns: &[QueryColumn],
    values: &[Value],
) -> bool {
    output_columns.len() == 1
        && matches!(output_columns[0].sql_type.kind, SqlTypeKind::Record)
        && !matches!(values, [Value::Record(_)])
}

pub fn pack_sql_function_record_row(values: Vec<Value>, columns: &[QueryColumn]) -> Value {
    let descriptor = RecordDescriptor::anonymous(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
        -1,
    );
    Value::Record(RecordValue::from_descriptor(descriptor, values))
}

pub fn sql_function_result_row_for_output(
    value: Value,
    output_width: usize,
    expand_single_record: bool,
) -> Vec<Value> {
    if (expand_single_record || output_width > 1)
        && let Value::Record(record) = value
    {
        let mut fields = record.fields;
        if fields.len() < output_width {
            fields.resize(output_width, Value::Null);
        }
        fields.truncate(output_width);
        return fields;
    }

    let mut row = vec![value];
    if row.len() < output_width {
        row.resize(output_width, Value::Null);
    }
    row.truncate(output_width);
    row
}

pub fn validate_sql_function_record_field_types(
    record: &RecordValue,
    expected_columns: &[QueryColumn],
) -> Result<(), SqlFunctionRecordFieldTypeMismatch> {
    if record.descriptor.fields.len() != expected_columns.len() {
        return Ok(());
    }
    for (index, ((returned, value), expected)) in record
        .descriptor
        .fields
        .iter()
        .zip(record.fields.iter())
        .zip(expected_columns.iter())
        .enumerate()
    {
        let returned_type = value.sql_type_hint().unwrap_or(returned.sql_type);
        if !sql_function_return_types_match(returned_type, expected.sql_type) {
            return Err(SqlFunctionRecordFieldTypeMismatch {
                ordinal: index + 1,
                returned_type,
                expected_type: expected.sql_type,
            });
        }
    }
    Ok(())
}

fn named_arg_occurrence_is_qualified_field(input: &str, start: usize) -> bool {
    input[..start]
        .chars()
        .rev()
        .find(|ch| !ch.is_whitespace())
        .is_some_and(|ch| ch == '.')
}

fn named_arg_occurrence_is_column_alias_list(input: &str, start: usize) -> bool {
    let Some(open) = nearest_open_paren_before(input, start) else {
        return false;
    };
    if !input[open + 1..start]
        .chars()
        .all(|ch| ch == '_' || ch == ',' || ch.is_ascii_alphanumeric() || ch.is_whitespace())
    {
        return false;
    }
    let Some((token, token_start)) = token_before(input, open) else {
        return false;
    };
    if token.eq_ignore_ascii_case("insert") {
        return true;
    }
    let Some((previous, _)) = token_before(input, token_start) else {
        return false;
    };
    previous.eq_ignore_ascii_case("as") || previous.eq_ignore_ascii_case("into")
}

fn named_arg_occurrence_is_update_set_target(input: &str, start: usize, end: usize) -> bool {
    let rest = input[end..].trim_start();
    if !rest.starts_with('=') {
        return false;
    }

    let mut best: Option<(&str, usize)> = None;
    let mut index = 0usize;
    while index < start {
        let ch = input.as_bytes()[index] as char;
        if ch == '_' || ch.is_ascii_alphabetic() {
            let token_start = index;
            index += 1;
            while index < start {
                let ch = input.as_bytes()[index] as char;
                if ch == '_' || ch.is_ascii_alphanumeric() {
                    index += 1;
                } else {
                    break;
                }
            }
            let token = &input[token_start..index];
            if matches!(
                token.to_ascii_lowercase().as_str(),
                "set"
                    | "where"
                    | "when"
                    | "then"
                    | "returning"
                    | "values"
                    | "from"
                    | "on"
                    | "insert"
                    | "update"
                    | "delete"
                    | "merge"
            ) {
                best = Some((token, token_start));
            }
        } else {
            index += 1;
        }
    }
    best.is_some_and(|(token, _)| token.eq_ignore_ascii_case("set"))
}

fn nearest_open_paren_before(input: &str, start: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (idx, ch) in input[..start].char_indices().rev() {
        match ch {
            ')' => depth += 1,
            '(' if depth == 0 => return Some(idx),
            '(' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    None
}

fn token_before(input: &str, end: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    let mut idx = end;
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    let token_end = idx;
    while idx > 0 {
        let byte = bytes[idx - 1];
        if byte == b'_' || byte.is_ascii_alphanumeric() {
            idx -= 1;
        } else {
            break;
        }
    }
    (idx < token_end).then_some((&input[idx..token_end], idx))
}

fn named_arg_occurrence_is_window_unbounded_keyword(input: &str, name: &str, end: usize) -> bool {
    if !name.eq_ignore_ascii_case("unbounded") {
        return false;
    }
    let rest = input[end..].trim_start();
    keyword_at_start_ascii(rest, "preceding") || keyword_at_start_ascii(rest, "following")
}

fn keyword_at_start_ascii(input: &str, keyword: &str) -> bool {
    let Some(prefix) = input.get(..keyword.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(keyword)
        && input[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| !(ch == '_' || ch.is_ascii_alphanumeric()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_proc_row(argtypes: &str) -> PgProcRow {
        PgProcRow {
            oid: 0,
            proname: "f".into(),
            pronamespace: 0,
            proowner: 0,
            proacl: None,
            prolang: 0,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind: 'f',
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'i',
            proparallel: 's',
            pronargs: 0,
            pronargdefaults: 0,
            prorettype: 0,
            proargtypes: argtypes.into(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
            proargdefaults: None,
            prosrc: String::new(),
            probin: None,
            prosqlbody: None,
            proconfig: None,
        }
    }

    #[test]
    fn normalizes_return_and_standard_body() {
        assert_eq!(normalized_sql_function_body("RETURN 1;"), "select 1");
        assert_eq!(
            normalized_sql_function_body("BEGIN ATOMIC SELECT 1; END"),
            "SELECT 1"
        );
        assert_eq!(
            normalize_sql_function_statement_for_execution(" RETURN 2").as_ref(),
            "select 2"
        );
        assert!(sql_function_statement_needs_database_executor(
            " alter table t add column x int"
        ));
        assert!(!sql_function_statement_needs_database_executor("select 1"));
        assert!(sql_function_body_is_inline_select_candidate(" values (1)"));
        assert!(sql_function_sets_row_security_off(
            "select set_config('row_security', 'off', true)"
        ));
    }

    #[test]
    fn splits_body_around_quoted_semicolons() {
        assert_eq!(
            split_sql_function_body("select ';'; select $$;$$;").unwrap(),
            vec!["select ';'".to_string(), "select $$;$$".to_string()]
        );
    }

    #[test]
    fn split_body_reports_unexpected_eof() {
        assert_eq!(
            split_sql_function_body("select 'unterminated").unwrap_err(),
            SqlFunctionBodyError::UnexpectedEof
        );
    }

    #[test]
    fn substitute_named_arg_skips_quotes_and_contextual_identifiers() {
        let sql = "select a, 'a', t.a from t where b = a";
        assert_eq!(
            substitute_named_arg(sql, "a", "42"),
            "select 42, 'a', t.a from t where b = 42"
        );
        assert_eq!(
            substitute_named_arg("select unbounded preceding", "unbounded", "42"),
            "select unbounded preceding"
        );
        assert_eq!(
            substitute_named_arg("update t set a = a", "a", "42"),
            "update t set a = 42"
        );
    }

    #[test]
    fn fragment_substitution_skips_quoted_text() {
        assert_eq!(
            substitute_sql_fragment_outside_quotes("select x, 'x'", "x", "42"),
            "select 42, 'x'"
        );
    }

    #[test]
    fn positional_substitution_skips_quotes_and_reports_bounds() {
        let rendered =
            substitute_positional_args_with_renderer("select $1, '$2', \"$3\", $2", 2, |index| {
                Ok::<_, ()>(format!("arg{}", index + 1))
            })
            .unwrap();
        assert_eq!(rendered, "select arg1, '$2', \"$3\", arg2");

        assert_eq!(
            substitute_positional_args_with_renderer("select $3", 2, |index| {
                Ok::<_, ()>(format!("arg{index}"))
            })
            .unwrap_err(),
            SqlFunctionSubstitutionError::ParameterOutOfRange { position: 3 }
        );
    }

    #[test]
    fn proc_argtype_helpers_parse_and_overlay_call_types() {
        let row = test_proc_row("23 25");
        assert_eq!(
            parse_proc_argtype_oids(&row.proargtypes).unwrap(),
            vec![23, 25]
        );
        assert_eq!(
            effective_sql_function_arg_type_oids(&row, 3, Some(&[0, 1043, 16])),
            vec![23, 1043, 16]
        );
        assert_eq!(
            parse_proc_argtype_oids("23 nope").unwrap_err(),
            SqlFunctionMetadataError::InvalidArgumentMetadata {
                metadata: "23 nope".into()
            }
        );
    }

    #[test]
    fn sql_quote_helpers_match_postgres_literal_style() {
        assert_eq!(quote_sql_identifier("simple_name"), "simple_name");
        assert_eq!(quote_sql_identifier("NeedsQuote"), "\"NeedsQuote\"");
        assert_eq!(quote_sql_identifier("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_sql_string("it isn't"), "'it isn''t'");
        assert_eq!(quote_sql_string("c:\\tmp"), "E'c:\\\\tmp'");
    }

    #[test]
    fn polymorphic_runtime_type_helpers_allow_numeric_widening() {
        let int4 = SqlType::new(SqlTypeKind::Int4);
        let int8 = SqlType::new(SqlTypeKind::Int8);
        let text = SqlType::new(SqlTypeKind::Text);
        assert!(sql_types_match_for_polymorphic_runtime(int4, int4));
        assert!(can_coerce_to_compatible_runtime_anchor(int4, int8));
        assert!(!can_coerce_to_compatible_runtime_anchor(text, int8));

        let mut current = None;
        assert!(merge_polymorphic_runtime_subtype(&mut current, int4));
        assert_eq!(current, Some(int4));
        assert!(!merge_polymorphic_runtime_subtype(&mut current, text));
    }

    #[test]
    fn sql_function_record_row_pack_and_expand_roundtrip() {
        let columns = vec![
            QueryColumn {
                name: "a".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "b".into(),
                sql_type: SqlType::new(SqlTypeKind::Text),
                wire_type_oid: None,
            },
        ];
        let packed =
            pack_sql_function_record_row(vec![Value::Int32(1), Value::Text("x".into())], &columns);

        assert_eq!(
            sql_function_result_row_for_output(packed, 3, true),
            vec![Value::Int32(1), Value::Text("x".into()), Value::Null]
        );
    }

    #[test]
    fn sql_function_record_pack_predicate_checks_shape() {
        let columns = vec![QueryColumn {
            name: "record".into(),
            sql_type: SqlType::record(2249),
            wire_type_oid: None,
        }];

        assert!(should_pack_sql_set_returning_record_row(
            &columns,
            &[Value::Int32(1), Value::Int32(2)]
        ));
        assert!(!should_pack_sql_set_returning_record_row(
            &columns,
            &[Value::Record(RecordValue::anonymous(Vec::new()))]
        ));
    }

    #[test]
    fn record_field_type_validation_reports_first_mismatch() {
        let record = RecordValue::from_descriptor(
            RecordDescriptor::anonymous(
                vec![
                    ("a".into(), SqlType::new(SqlTypeKind::Int4)),
                    ("b".into(), SqlType::new(SqlTypeKind::Text)),
                ],
                -1,
            ),
            vec![Value::Int32(1), Value::Text("x".into())],
        );
        let expected = vec![
            QueryColumn {
                name: "a".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "b".into(),
                sql_type: SqlType::new(SqlTypeKind::Int8),
                wire_type_oid: None,
            },
        ];

        assert_eq!(
            validate_sql_function_record_field_types(&record, &expected).unwrap_err(),
            SqlFunctionRecordFieldTypeMismatch {
                ordinal: 2,
                returned_type: SqlType::new(SqlTypeKind::Text),
                expected_type: SqlType::new(SqlTypeKind::Int8),
            }
        );
    }
}
use pgrust_catalog_data::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYMULTIRANGEOID, ANYRANGEOID, PgProcRow,
};
use pgrust_nodes::datum::{RecordDescriptor, RecordValue, Value};
use pgrust_nodes::primnodes::QueryColumn;
use pgrust_nodes::{SqlType, SqlTypeKind};
