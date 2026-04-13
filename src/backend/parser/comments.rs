pub(crate) fn strip_sql_comments_preserving_layout(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    let mut block_depth = 0usize;
    let mut dollar_tag: Option<Vec<u8>> = None;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Normal,
        LineComment,
        BlockComment,
        SingleQuote,
        QuotedIdentifier,
        DollarString,
    }

    let mut state = State::Normal;

    while i < bytes.len() {
        match state {
            State::Normal => {
                if starts_line_comment(bytes, i) {
                    out.extend_from_slice(b"  ");
                    i += 2;
                    state = State::LineComment;
                } else if starts_block_comment(bytes, i) {
                    out.extend_from_slice(b"  ");
                    i += 2;
                    block_depth = 1;
                    state = State::BlockComment;
                } else if bytes[i] == b'\'' {
                    out.push(bytes[i]);
                    i += 1;
                    state = State::SingleQuote;
                } else if bytes[i] == b'"' {
                    out.push(bytes[i]);
                    i += 1;
                    state = State::QuotedIdentifier;
                } else if let Some((tag, len)) = parse_dollar_tag(bytes, i) {
                    out.extend_from_slice(&bytes[i..i + len]);
                    i += len;
                    dollar_tag = Some(tag);
                    state = State::DollarString;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            State::LineComment => {
                let byte = bytes[i];
                if byte == b'\n' || byte == b'\r' {
                    out.push(byte);
                    i += 1;
                    state = State::Normal;
                } else {
                    out.push(b' ');
                    i += 1;
                }
            }
            State::BlockComment => {
                if starts_block_comment(bytes, i) {
                    out.extend_from_slice(b"  ");
                    i += 2;
                    block_depth += 1;
                } else if ends_block_comment(bytes, i) {
                    out.extend_from_slice(b"  ");
                    i += 2;
                    block_depth -= 1;
                    if block_depth == 0 {
                        state = State::Normal;
                    }
                } else {
                    let byte = bytes[i];
                    out.push(if byte == b'\n' || byte == b'\r' {
                        byte
                    } else {
                        b' '
                    });
                    i += 1;
                }
            }
            State::SingleQuote => {
                out.push(bytes[i]);
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push(bytes[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                        state = State::Normal;
                    }
                } else {
                    i += 1;
                }
            }
            State::QuotedIdentifier => {
                out.push(bytes[i]);
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push(bytes[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                        state = State::Normal;
                    }
                } else {
                    i += 1;
                }
            }
            State::DollarString => {
                if let Some(tag) = dollar_tag.as_ref() {
                    if matches_dollar_end(bytes, i, tag) {
                        let len = tag.len() + 2;
                        out.extend_from_slice(&bytes[i..i + len]);
                        i += len;
                        dollar_tag = None;
                        state = State::Normal;
                    } else {
                        out.push(bytes[i]);
                        i += 1;
                    }
                } else {
                    out.push(bytes[i]);
                    i += 1;
                    state = State::Normal;
                }
            }
        }
    }

    String::from_utf8(out).expect("comment stripping preserves UTF-8")
}

pub(crate) fn normalize_string_continuation_preserving_layout(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let (mut merged, next) = parse_plain_string_literal(sql, i);
            i = next;

            loop {
                let mut j = i;
                let mut saw_newline = false;
                while j < bytes.len() && matches!(bytes[j], b' ' | b'\t' | b'\r' | b'\n') {
                    saw_newline |= matches!(bytes[j], b'\r' | b'\n');
                    j += 1;
                }
                if saw_newline && j < bytes.len() && bytes[j] == b'\'' {
                    let (continued, next_continued) = parse_plain_string_literal(sql, j);
                    merged.push_str(&continued);
                    i = next_continued;
                    continue;
                }
                break;
            }
            out.push_str(&render_sql_string_literal(&merged));
        } else if starts_escape_string(bytes, i) || starts_unicode_string(bytes, i) {
            let quote = i + 1 + usize::from(bytes[i + 1] == b'&');
            let next = parse_delimited_token_end(bytes, quote, b'\'');
            out.push_str(&sql[i..next]);
            i = next;
        } else if starts_unicode_identifier(bytes, i) {
            let next = parse_delimited_token_end(bytes, i + 2, b'"');
            out.push_str(&sql[i..next]);
            i = next;
        } else if bytes[i] == b'"' {
            let next = parse_delimited_token_end(bytes, i, b'"');
            out.push_str(&sql[i..next]);
            i = next;
        } else if let Some((tag, len)) = parse_dollar_tag(bytes, i) {
            let mut j = i + len;
            while j < bytes.len() && !matches_dollar_end(bytes, j, &tag) {
                j += sql[j..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
            }
            let end = if j < bytes.len() { j + tag.len() + 2 } else { bytes.len() };
            out.push_str(&sql[i..end]);
            i = end;
        } else {
            let ch = sql[i..].chars().next().expect("valid utf-8");
            out.push(ch);
            i += ch.len_utf8();
        }
    }

    out
}

pub(crate) fn normalize_position_syntax_preserving_layout(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = sql.as_bytes().to_vec();
    let mut i = 0usize;
    let mut dollar_tag: Option<Vec<u8>> = None;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        QuotedIdentifier,
        DollarString,
    }

    let mut state = State::Normal;

    while i < bytes.len() {
        match state {
            State::Normal => {
                if bytes[i] == b'\'' {
                    i += 1;
                    state = State::SingleQuote;
                } else if bytes[i] == b'"' {
                    i += 1;
                    state = State::QuotedIdentifier;
                } else if let Some((tag, len)) = parse_dollar_tag(bytes, i) {
                    i += len;
                    dollar_tag = Some(tag);
                    state = State::DollarString;
                } else if starts_position_call(bytes, i) {
                    out[i..i + "position".len()].copy_from_slice(b"position");
                    i = normalize_position_call(bytes, &mut out, i + "position".len());
                } else {
                    i += 1;
                }
            }
            State::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        state = State::Normal;
                    }
                } else {
                    i += 1;
                }
            }
            State::QuotedIdentifier => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        i += 1;
                        state = State::Normal;
                    }
                } else {
                    i += 1;
                }
            }
            State::DollarString => {
                if let Some(tag) = dollar_tag.as_ref() {
                    if matches_dollar_end(bytes, i, tag) {
                        i += tag.len() + 2;
                        dollar_tag = None;
                        state = State::Normal;
                    } else {
                        i += 1;
                    }
                } else {
                    i += 1;
                    state = State::Normal;
                }
            }
        }
    }

    String::from_utf8(out).expect("position normalization preserves UTF-8")
}

fn parse_plain_string_literal(sql: &str, start: usize) -> (String, usize) {
    let bytes = sql.as_bytes();
    let mut i = start + 1;
    let mut out = String::new();
    while i < bytes.len() {
        let ch = sql[i..].chars().next().expect("valid utf-8");
        if ch == '\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
            } else {
                return (out, i + 1);
            }
        } else {
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    (out, bytes.len())
}

fn parse_delimited_token_end(bytes: &[u8], start: usize, delimiter: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn starts_escape_string(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && matches!(bytes[i], b'e' | b'E') && bytes[i + 1] == b'\''
}

fn starts_unicode_string(bytes: &[u8], i: usize) -> bool {
    i + 2 < bytes.len()
        && matches!(bytes[i], b'u' | b'U')
        && bytes[i + 1] == b'&'
        && bytes[i + 2] == b'\''
}

fn starts_unicode_identifier(bytes: &[u8], i: usize) -> bool {
    i + 2 < bytes.len()
        && matches!(bytes[i], b'u' | b'U')
        && bytes[i + 1] == b'&'
        && bytes[i + 2] == b'"'
}

fn render_sql_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

pub(crate) fn sql_is_effectively_empty_after_comments(sql: &str) -> bool {
    strip_sql_comments_preserving_layout(sql)
        .trim()
        .trim_end_matches(';')
        .trim()
        .is_empty()
}

fn starts_line_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-'
}

fn starts_block_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*'
}

fn ends_block_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/'
}

fn parse_dollar_tag(bytes: &[u8], start: usize) -> Option<(Vec<u8>, usize)> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() && bytes[end] != b'$' {
        let byte = bytes[end];
        if !(byte.is_ascii_alphanumeric() || byte == b'_') {
            return None;
        }
        end += 1;
    }
    if end >= bytes.len() || bytes[end] != b'$' {
        return None;
    }
    Some((bytes[start + 1..end].to_vec(), end - start + 1))
}

fn matches_dollar_end(bytes: &[u8], start: usize, tag: &[u8]) -> bool {
    if bytes.get(start) != Some(&b'$') {
        return false;
    }
    let end = start + tag.len() + 1;
    end < bytes.len() && &bytes[start + 1..end] == tag && bytes[end] == b'$'
}

fn starts_position_call(bytes: &[u8], i: usize) -> bool {
    let keyword = b"position";
    if i + keyword.len() > bytes.len() || !bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword)
    {
        return false;
    }
    if i > 0 && is_identifier_continue(bytes[i - 1]) {
        return false;
    }
    let mut j = i + keyword.len();
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    j < bytes.len() && bytes[j] == b'('
}

fn normalize_position_call(bytes: &[u8], out: &mut [u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'(' {
        return i;
    }
    let mut depth = 1usize;
    let mut j = i + 1;
    let mut dollar_tag: Option<Vec<u8>> = None;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        QuotedIdentifier,
        DollarString,
    }

    let mut state = State::Normal;

    while j < bytes.len() {
        match state {
            State::Normal => {
                if bytes[j] == b'\'' {
                    j += 1;
                    state = State::SingleQuote;
                } else if bytes[j] == b'"' {
                    j += 1;
                    state = State::QuotedIdentifier;
                } else if let Some((tag, len)) = parse_dollar_tag(bytes, j) {
                    j += len;
                    dollar_tag = Some(tag);
                    state = State::DollarString;
                } else if bytes[j] == b'(' {
                    depth += 1;
                    j += 1;
                } else if bytes[j] == b')' {
                    depth -= 1;
                    j += 1;
                    if depth == 0 {
                        return j;
                    }
                } else if depth == 1 && matches_kw_in(bytes, j) {
                    out[j] = b',';
                    if j + 1 < out.len() {
                        out[j + 1] = b' ';
                    }
                    return j + 2;
                } else {
                    j += 1;
                }
            }
            State::SingleQuote => {
                if bytes[j] == b'\'' {
                    if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                        j += 2;
                    } else {
                        j += 1;
                        state = State::Normal;
                    }
                } else {
                    j += 1;
                }
            }
            State::QuotedIdentifier => {
                if bytes[j] == b'"' {
                    if j + 1 < bytes.len() && bytes[j + 1] == b'"' {
                        j += 2;
                    } else {
                        j += 1;
                        state = State::Normal;
                    }
                } else {
                    j += 1;
                }
            }
            State::DollarString => {
                if let Some(tag) = dollar_tag.as_ref() {
                    if matches_dollar_end(bytes, j, tag) {
                        j += tag.len() + 2;
                        dollar_tag = None;
                        state = State::Normal;
                    } else {
                        j += 1;
                    }
                } else {
                    j += 1;
                    state = State::Normal;
                }
            }
        }
    }

    j
}

fn matches_kw_in(bytes: &[u8], start: usize) -> bool {
    start + 1 < bytes.len()
        && bytes[start].eq_ignore_ascii_case(&b'i')
        && bytes[start + 1].eq_ignore_ascii_case(&b'n')
        && (start == 0 || !is_identifier_continue(bytes[start - 1]))
        && (start + 2 >= bytes.len() || !is_identifier_continue(bytes[start + 2]))
}

fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.')
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_position_syntax_preserving_layout, sql_is_effectively_empty_after_comments,
        strip_sql_comments_preserving_layout,
    };

    #[test]
    fn strips_embedded_and_nested_comments() {
        let sql = "SELECT /* level 1 /* level 2 */ still level 1 */ 'x' AS v";
        let stripped = strip_sql_comments_preserving_layout(sql);
        assert!(stripped.contains("SELECT"));
        assert!(stripped.contains("'x' AS v"));
        assert!(!stripped.contains("level 1"));
    }

    #[test]
    fn preserves_comment_markers_inside_strings_and_dollar_quotes() {
        let sql = "select '--', '/* */', $$/* not a comment */$$, \"--id\"";
        let stripped = strip_sql_comments_preserving_layout(sql);
        assert_eq!(stripped, sql);
    }

    #[test]
    fn comment_only_sql_is_effectively_empty() {
        assert!(sql_is_effectively_empty_after_comments(
            "/* only comment */\n-- trailing\n;"
        ));
        assert!(sql_is_effectively_empty_after_comments(
            "/* and this is the end of the file */"
        ));
    }

    #[test]
    fn normalizes_position_in_syntax() {
        let sql = "select position('bc' in 'abcd')";
        assert_eq!(
            normalize_position_syntax_preserving_layout(sql),
            "select position('bc' ,  'abcd')"
        );
    }
}
