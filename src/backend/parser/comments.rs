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

#[cfg(test)]
mod tests {
    use super::{sql_is_effectively_empty_after_comments, strip_sql_comments_preserving_layout};

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
}
