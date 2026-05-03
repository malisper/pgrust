// :HACK: PostgreSQL asks its regex compiler for fixed-prefix metadata. pgrust
// does not expose an equivalent regex AST yet, so keep this planner helper
// conservative: return prefixes only for shapes we can prove from syntax.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RegexFixedPrefix {
    pub(super) prefix: String,
    pub(super) exact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegexPrefixEnd {
    PatternEnd,
    EndAnchor,
    GroupEnd,
    TopLevelAlternation,
    Stopped,
}

pub(super) fn regex_fixed_prefix(pattern: &str) -> Option<RegexFixedPrefix> {
    let bytes = pattern.as_bytes();
    if !bytes.starts_with(b"^") {
        return None;
    }
    if has_top_level_alternation(&bytes[1..]) {
        return None;
    }
    let (prefix, _pos, end) = read_regex_required_prefix(bytes, 1, false);
    (end != RegexPrefixEnd::TopLevelAlternation && !prefix.is_empty()).then_some(RegexFixedPrefix {
        prefix,
        exact: end == RegexPrefixEnd::EndAnchor,
    })
}

fn has_top_level_alternation(bytes: &[u8]) -> bool {
    let mut depth = 0usize;
    let mut in_bracket = false;
    let mut pos = 0usize;
    while pos < bytes.len() {
        match bytes[pos] {
            b'\\' => pos += 2,
            b'[' if !in_bracket => {
                in_bracket = true;
                pos += 1;
            }
            b']' if in_bracket => {
                in_bracket = false;
                pos += 1;
            }
            _ if in_bracket => pos += 1,
            b'(' => {
                depth += 1;
                pos += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                pos += 1;
            }
            b'|' if depth == 0 => return true,
            _ => pos += 1,
        }
    }
    false
}

fn read_regex_required_prefix(
    bytes: &[u8],
    mut pos: usize,
    stop_at_group_end: bool,
) -> (String, usize, RegexPrefixEnd) {
    let mut prefix = String::new();
    while pos < bytes.len() {
        match bytes[pos] {
            b'$' if !stop_at_group_end && pos + 1 == bytes.len() => {
                return (prefix, pos + 1, RegexPrefixEnd::EndAnchor);
            }
            b')' if stop_at_group_end => return (prefix, pos + 1, RegexPrefixEnd::GroupEnd),
            b'|' if !stop_at_group_end => {
                return (prefix, pos, RegexPrefixEnd::TopLevelAlternation);
            }
            b'(' => {
                if bytes.get(pos + 1) == Some(&b'?') {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                }
                let before_group_len = prefix.len();
                let (group_prefix, after_group, group_end) =
                    read_regex_required_prefix(bytes, pos + 1, true);
                if group_end != RegexPrefixEnd::GroupEnd {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                }
                let effect = regex_quantifier_effect(bytes, after_group);
                if effect.include_token {
                    prefix.push_str(&group_prefix);
                } else {
                    prefix.truncate(before_group_len);
                }
                pos = effect.next_pos;
                if effect.stop_after_token {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                }
            }
            b'\\' => {
                let Some(&escaped) = bytes.get(pos + 1) else {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                };
                let Some(literal) = escaped_regex_literal(escaped) else {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                };
                let effect = regex_quantifier_effect(bytes, pos + 2);
                if effect.include_token {
                    prefix.push(literal);
                }
                pos = effect.next_pos;
                if effect.stop_after_token {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                }
            }
            byte if is_plain_regex_literal(byte) => {
                let effect = regex_quantifier_effect(bytes, pos + 1);
                if effect.include_token {
                    prefix.push(byte as char);
                }
                pos = effect.next_pos;
                if effect.stop_after_token {
                    return (prefix, pos, RegexPrefixEnd::Stopped);
                }
            }
            _ => return (prefix, pos, RegexPrefixEnd::Stopped),
        }
    }
    (prefix, pos, RegexPrefixEnd::PatternEnd)
}

#[derive(Debug, Clone, Copy)]
struct RegexQuantifierEffect {
    include_token: bool,
    next_pos: usize,
    stop_after_token: bool,
}

fn regex_quantifier_effect(bytes: &[u8], pos: usize) -> RegexQuantifierEffect {
    let Some(&byte) = bytes.get(pos) else {
        return RegexQuantifierEffect {
            include_token: true,
            next_pos: pos,
            stop_after_token: false,
        };
    };
    match byte {
        b'*' | b'?' => RegexQuantifierEffect {
            include_token: false,
            next_pos: pos + 1,
            stop_after_token: true,
        },
        b'+' => RegexQuantifierEffect {
            include_token: true,
            next_pos: pos + 1,
            stop_after_token: true,
        },
        b'{' => {
            let (min, end_pos) = regex_brace_quantifier_min(bytes, pos).unwrap_or((1, pos + 1));
            RegexQuantifierEffect {
                include_token: min > 0,
                next_pos: end_pos,
                stop_after_token: true,
            }
        }
        _ => RegexQuantifierEffect {
            include_token: true,
            next_pos: pos,
            stop_after_token: false,
        },
    }
}

fn regex_brace_quantifier_min(bytes: &[u8], pos: usize) -> Option<(usize, usize)> {
    let mut idx = pos + 1;
    let start = idx;
    while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
        idx += 1;
    }
    if idx == start {
        return None;
    }
    let min = std::str::from_utf8(&bytes[start..idx])
        .ok()?
        .parse::<usize>()
        .ok()?;
    while idx < bytes.len() && bytes[idx] != b'}' {
        if !matches!(bytes[idx], b',' | b'0'..=b'9') {
            return None;
        }
        idx += 1;
    }
    (idx < bytes.len()).then_some((min, idx + 1))
}

fn is_plain_regex_literal(byte: u8) -> bool {
    byte.is_ascii()
        && !matches!(
            byte,
            b'.' | b'^' | b'$' | b'*' | b'+' | b'?' | b'(' | b')' | b'[' | b'{' | b'|' | b'\\'
        )
}

fn escaped_regex_literal(byte: u8) -> Option<char> {
    matches!(
        byte,
        b'.' | b'^'
            | b'$'
            | b'*'
            | b'+'
            | b'?'
            | b'('
            | b')'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b'|'
            | b'\\'
    )
    .then_some(byte as char)
}

pub(super) fn regex_prefix_upper_bound(prefix: &str) -> Option<String> {
    if !prefix.is_ascii() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(last) = bytes.pop() {
        if last < 0x7f {
            bytes.push(last + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{regex_fixed_prefix, regex_prefix_upper_bound};

    fn fixed(pattern: &str) -> Option<(String, bool)> {
        regex_fixed_prefix(pattern).map(|prefix| (prefix.prefix, prefix.exact))
    }

    #[test]
    fn extracts_regression_regex_prefixes() {
        assert_eq!(fixed("^abc"), Some(("abc".into(), false)));
        assert_eq!(fixed("^abc$"), Some(("abc".into(), true)));
        assert_eq!(fixed("^abcd*e"), Some(("abc".into(), false)));
        assert_eq!(fixed("^abc+d"), Some(("abc".into(), false)));
        assert_eq!(fixed("^(abc)(def)"), Some(("abcdef".into(), false)));
        assert_eq!(fixed("^(abc)$"), Some(("abc".into(), true)));
        assert_eq!(fixed("^(abc)?d"), None);
        assert_eq!(fixed("^abcd(x|(?=\\w\\w)q)"), Some(("abcd".into(), false)));
    }

    #[test]
    fn avoids_top_level_alternation_prefixes() {
        assert_eq!(fixed("^abc|def"), None);
        assert_eq!(fixed("^abc[de]|def"), None);
    }

    #[test]
    fn increments_ascii_prefix_upper_bound() {
        assert_eq!(regex_prefix_upper_bound("abc"), Some("abd".into()));
        assert_eq!(regex_prefix_upper_bound("abcdef"), Some("abcdeg".into()));
    }
}
