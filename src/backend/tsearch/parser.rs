use crate::backend::tsearch::cache::TextSearchConfig;
use crate::backend::tsearch::ts_utils::lexize_token_for_config_and_type;
use crate::include::nodes::tsearch::{TsQueryNode, TsQueryOperand};

pub(crate) const ASCIIWORD: i32 = 1;
pub(crate) const WORD_T: i32 = 2;
pub(crate) const NUMWORD: i32 = 3;
pub(crate) const EMAIL: i32 = 4;
pub(crate) const URL_T: i32 = 5;
pub(crate) const HOST: i32 = 6;
pub(crate) const SCIENTIFIC: i32 = 7;
pub(crate) const VERSIONNUMBER: i32 = 8;
pub(crate) const NUMPARTHWORD: i32 = 9;
pub(crate) const PARTHWORD: i32 = 10;
pub(crate) const ASCIIPARTHWORD: i32 = 11;
pub(crate) const SPACE: i32 = 12;
pub(crate) const TAG_T: i32 = 13;
pub(crate) const PROTOCOL: i32 = 14;
pub(crate) const NUMHWORD: i32 = 15;
pub(crate) const ASCIIHWORD: i32 = 16;
pub(crate) const HWORD: i32 = 17;
pub(crate) const URLPATH: i32 = 18;
pub(crate) const FILEPATH: i32 = 19;
pub(crate) const DECIMAL_T: i32 = 20;
pub(crate) const SIGNEDINT: i32 = 21;
pub(crate) const UNSIGNEDINT: i32 = 22;
pub(crate) const XMLENTITY: i32 = 23;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TextSearchTokenKind {
    pub tokid: i32,
    pub alias: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedTextSearchToken {
    pub tokid: i32,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentToken {
    pub tokid: i32,
    pub token: String,
    pub position: u16,
}

pub(crate) const TOKEN_KINDS: [TextSearchTokenKind; 23] = [
    TextSearchTokenKind {
        tokid: ASCIIWORD,
        alias: "asciiword",
        description: "Word, all ASCII",
    },
    TextSearchTokenKind {
        tokid: WORD_T,
        alias: "word",
        description: "Word, all letters",
    },
    TextSearchTokenKind {
        tokid: NUMWORD,
        alias: "numword",
        description: "Word, letters and digits",
    },
    TextSearchTokenKind {
        tokid: EMAIL,
        alias: "email",
        description: "Email address",
    },
    TextSearchTokenKind {
        tokid: URL_T,
        alias: "url",
        description: "URL",
    },
    TextSearchTokenKind {
        tokid: HOST,
        alias: "host",
        description: "Host",
    },
    TextSearchTokenKind {
        tokid: SCIENTIFIC,
        alias: "sfloat",
        description: "Scientific notation",
    },
    TextSearchTokenKind {
        tokid: VERSIONNUMBER,
        alias: "version",
        description: "Version number",
    },
    TextSearchTokenKind {
        tokid: NUMPARTHWORD,
        alias: "hword_numpart",
        description: "Hyphenated word part, letters and digits",
    },
    TextSearchTokenKind {
        tokid: PARTHWORD,
        alias: "hword_part",
        description: "Hyphenated word part, all letters",
    },
    TextSearchTokenKind {
        tokid: ASCIIPARTHWORD,
        alias: "hword_asciipart",
        description: "Hyphenated word part, all ASCII",
    },
    TextSearchTokenKind {
        tokid: SPACE,
        alias: "blank",
        description: "Space symbols",
    },
    TextSearchTokenKind {
        tokid: TAG_T,
        alias: "tag",
        description: "XML tag",
    },
    TextSearchTokenKind {
        tokid: PROTOCOL,
        alias: "protocol",
        description: "Protocol head",
    },
    TextSearchTokenKind {
        tokid: NUMHWORD,
        alias: "numhword",
        description: "Hyphenated word, letters and digits",
    },
    TextSearchTokenKind {
        tokid: ASCIIHWORD,
        alias: "asciihword",
        description: "Hyphenated word, all ASCII",
    },
    TextSearchTokenKind {
        tokid: HWORD,
        alias: "hword",
        description: "Hyphenated word, all letters",
    },
    TextSearchTokenKind {
        tokid: URLPATH,
        alias: "url_path",
        description: "URL path",
    },
    TextSearchTokenKind {
        tokid: FILEPATH,
        alias: "file",
        description: "File or path name",
    },
    TextSearchTokenKind {
        tokid: DECIMAL_T,
        alias: "float",
        description: "Decimal notation",
    },
    TextSearchTokenKind {
        tokid: SIGNEDINT,
        alias: "int",
        description: "Signed integer",
    },
    TextSearchTokenKind {
        tokid: UNSIGNEDINT,
        alias: "uint",
        description: "Unsigned integer",
    },
    TextSearchTokenKind {
        tokid: XMLENTITY,
        alias: "entity",
        description: "XML entity",
    },
];

pub(crate) fn token_kinds() -> &'static [TextSearchTokenKind] {
    &TOKEN_KINDS
}

pub(crate) fn token_kind(tokid: i32) -> Option<&'static TextSearchTokenKind> {
    TOKEN_KINDS.iter().find(|kind| kind.tokid == tokid)
}

pub(crate) fn parse_default(text: &str) -> Vec<ParsedTextSearchToken> {
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < text.len() {
        if let Some((tokid, token, consumed, extras)) = next_token(&text[index..]) {
            out.push(ParsedTextSearchToken { tokid, token });
            out.extend(extras);
            index += consumed;
            continue;
        }
        let consumed = take_separator_run(&text[index..]);
        out.push(ParsedTextSearchToken {
            tokid: SPACE,
            token: text[index..index + consumed].to_string(),
        });
        index += consumed;
    }
    out
}

pub(crate) fn document_tokens(text: &str) -> Vec<DocumentToken> {
    let mut position = 1u16;
    let mut out = Vec::new();
    for token in parse_default(text) {
        if !token_has_dictionary(token.tokid) {
            continue;
        }
        out.push(DocumentToken {
            tokid: token.tokid,
            token: token.token,
            position,
        });
        position = position.saturating_add(1);
    }
    out
}

pub(crate) fn normalized_query_node_from_text(
    config: &TextSearchConfig,
    text: &str,
    template: &TsQueryOperand,
) -> Option<TsQueryNode> {
    let mut terms = Vec::new();
    for token in document_tokens(text) {
        if let Some(lexeme) = lexize_token_for_config_and_type(config, token.tokid, &token.token) {
            let mut operand = template.clone();
            operand.lexeme = lexeme.into();
            terms.push((token.position, TsQueryNode::Operand(operand)));
        }
    }
    phrase_node_from_positioned_terms(terms)
}

pub(crate) fn phrase_node_from_positioned_terms(
    terms: Vec<(u16, TsQueryNode)>,
) -> Option<TsQueryNode> {
    let mut iter = terms.into_iter();
    let Some((mut prev_position, mut root)) = iter.next() else {
        return None;
    };
    for (position, term) in iter {
        let distance = position.saturating_sub(prev_position).max(1);
        root = TsQueryNode::Phrase {
            left: Box::new(root),
            right: Box::new(term),
            distance,
        };
        prev_position = position;
    }
    Some(root)
}

pub(crate) fn token_has_dictionary(tokid: i32) -> bool {
    !matches!(tokid, SPACE | TAG_T | PROTOCOL | XMLENTITY)
}

fn next_token(input: &str) -> Option<(i32, String, usize, Vec<ParsedTextSearchToken>)> {
    if input.is_empty() {
        return None;
    }
    if let Some(consumed) = take_xml_tag(input) {
        return Some((TAG_T, input[..consumed].to_string(), consumed, Vec::new()));
    }
    if let Some(consumed) = take_xml_entity(input) {
        return Some((
            XMLENTITY,
            input[..consumed].to_string(),
            consumed,
            Vec::new(),
        ));
    }
    if let Some((protocol_len, consumed, extras)) = take_protocol_url(input) {
        return Some((
            PROTOCOL,
            input[..protocol_len].to_string(),
            consumed,
            extras,
        ));
    }
    if let Some((consumed, extras)) = take_email(input) {
        return Some((EMAIL, input[..consumed].to_string(), consumed, extras));
    }
    if let Some((consumed, extras)) = take_bare_url_or_host(input) {
        let tokid = if extras.is_empty() { HOST } else { URL_T };
        return Some((tokid, input[..consumed].to_string(), consumed, extras));
    }
    if let Some(consumed) = take_scientific(input) {
        return Some((
            SCIENTIFIC,
            input[..consumed].to_string(),
            consumed,
            Vec::new(),
        ));
    }
    if let Some(consumed) = take_decimal(input) {
        return Some((
            DECIMAL_T,
            input[..consumed].to_string(),
            consumed,
            Vec::new(),
        ));
    }
    if let Some((consumed, extras)) = take_hyphenated(input) {
        return Some((ASCIIHWORD, input[..consumed].to_string(), consumed, extras));
    }
    if let Some(consumed) = take_file_path(input) {
        return Some((
            FILEPATH,
            input[..consumed].to_string(),
            consumed,
            Vec::new(),
        ));
    }
    if let Some(consumed) = take_word(input) {
        let token = &input[..consumed];
        let tokid = if token.chars().all(|ch| ch.is_ascii_digit()) {
            UNSIGNEDINT
        } else if token.chars().any(|ch| ch.is_ascii_digit()) {
            NUMWORD
        } else if token.is_ascii() {
            ASCIIWORD
        } else {
            WORD_T
        };
        return Some((tokid, token.to_string(), consumed, Vec::new()));
    }
    None
}

fn take_separator_run(input: &str) -> usize {
    if input.chars().next().is_some_and(|ch| ch.is_whitespace()) {
        let whitespace = take_while(input, |ch| ch.is_whitespace()).unwrap_or(1);
        let rest = &input[whitespace..];
        if rest.starts_with('<') || rest.is_empty() || next_token(rest).is_some() {
            return whitespace;
        }
        return whitespace + take_separator_run(rest);
    }

    let mut consumed = 0usize;
    while consumed < input.len() {
        let rest = &input[consumed..];
        if consumed > 0 && next_token(rest).is_some() {
            break;
        }
        let Some(ch) = rest.chars().next() else {
            break;
        };
        consumed += ch.len_utf8();
        if ch.is_whitespace() {
            break;
        }
    }
    consumed
}

fn take_while(input: &str, mut pred: impl FnMut(char) -> bool) -> Option<usize> {
    let mut consumed = 0usize;
    for ch in input.chars() {
        if !pred(ch) {
            break;
        }
        consumed += ch.len_utf8();
    }
    (consumed > 0).then_some(consumed)
}

fn take_xml_tag(input: &str) -> Option<usize> {
    if !input.starts_with('<') {
        return None;
    }
    let mut quote = None;
    let mut end = None;
    for (offset, ch) in input.char_indices().skip(1) {
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '<' => return None,
            '>' => {
                end = Some(offset);
                break;
            }
            _ => {}
        }
    }
    let end = end?;
    let body = &input[1..end];
    let name_start = body.strip_prefix('/').unwrap_or(body);
    if name_start.starts_with(char::is_whitespace) {
        return None;
    }
    let name_len = take_while(name_start, |ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.')
    })?;
    let name = &name_start[..name_len];
    name.chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic())
        .then_some(end + 1)
}

fn take_xml_entity(input: &str) -> Option<usize> {
    if !input.starts_with('&') {
        return None;
    }
    let end = input.find(';')?;
    if end > 1
        && input[1..end]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '#')
    {
        Some(end + 1)
    } else {
        None
    }
}

fn take_protocol_url(input: &str) -> Option<(usize, usize, Vec<ParsedTextSearchToken>)> {
    let protocol_len = protocol_len(input)?;
    let rest = &input[protocol_len..];
    let host_len = take_host_prefix(rest)?;
    let host = &rest[..host_len];
    let after_host = &rest[host_len..];
    let path_len = take_url_path_prefix(after_host).unwrap_or(0);
    let mut consumed = protocol_len + host_len + path_len;
    let mut extras = Vec::new();
    if path_len > 1 {
        extras.push(ParsedTextSearchToken {
            tokid: URL_T,
            token: input[protocol_len..consumed].to_string(),
        });
    }
    extras.push(ParsedTextSearchToken {
        tokid: HOST,
        token: host.to_string(),
    });
    if path_len > 1 {
        extras.push(ParsedTextSearchToken {
            tokid: URLPATH,
            token: after_host[..path_len].to_string(),
        });
    } else if path_len == 1 {
        let trailing_space = take_while(&after_host[1..], |ch| ch.is_whitespace()).unwrap_or(0);
        consumed = protocol_len + host_len + 1 + trailing_space;
        extras.push(ParsedTextSearchToken {
            tokid: SPACE,
            token: after_host[..1 + trailing_space].into(),
        });
    }
    Some((protocol_len, consumed, extras))
}

fn protocol_len(input: &str) -> Option<usize> {
    let lower = input.get(..input.len().min(16))?.to_ascii_lowercase();
    for protocol in ["http://", "https://", "ftp://", "file://"] {
        if lower.starts_with(protocol) {
            return Some(protocol.len());
        }
    }
    None
}

fn take_email(input: &str) -> Option<(usize, Vec<ParsedTextSearchToken>)> {
    let at = input.find('@')?;
    if at == 0 || !input[..at].chars().all(is_email_local_char) {
        return None;
    }
    let domain = &input[at + 1..];
    let host_len = take_host_prefix(domain)?;
    if host_len == 0 {
        return None;
    }
    let consumed = at + 1 + host_len;
    Some((consumed, Vec::new()))
}

fn take_bare_url_or_host(input: &str) -> Option<(usize, Vec<ParsedTextSearchToken>)> {
    let host_len = take_host_prefix(input)?;
    let host = &input[..host_len];
    let path_len = take_url_path_prefix(&input[host_len..]).unwrap_or(0);
    if path_len > 1 {
        let path = &input[host_len..host_len + path_len];
        return Some((
            host_len + path_len,
            vec![
                ParsedTextSearchToken {
                    tokid: HOST,
                    token: host.to_string(),
                },
                ParsedTextSearchToken {
                    tokid: URLPATH,
                    token: path.to_string(),
                },
            ],
        ));
    }
    Some((host_len, Vec::new()))
}

fn take_host_prefix(input: &str) -> Option<usize> {
    let consumed = take_while(input, |ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | ':' | '_')
    })?;
    let host = input[..consumed].trim_end_matches(['.', ':', '-']);
    if !is_host_like(host) {
        return None;
    }
    Some(host.len())
}

fn is_host_like(token: &str) -> bool {
    let Some(last_dot) = token.rfind('.') else {
        return false;
    };
    let suffix = &token[last_dot + 1..];
    suffix.len() >= 2
        && suffix
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
        && token[..last_dot]
            .chars()
            .any(|ch| ch.is_ascii_alphanumeric())
}

fn take_url_path_prefix(input: &str) -> Option<usize> {
    if !input.starts_with('/') {
        return None;
    }
    take_while(input, |ch| {
        !ch.is_whitespace()
            && !matches!(ch, '<' | '>' | '"' | '\'')
            && !matches!(ch, '(' | ')' | '[' | ']')
    })
}

fn take_scientific(input: &str) -> Option<usize> {
    let (mantissa_len, _) = take_number_with_optional_sign(input, true)?;
    let rest = &input[mantissa_len..];
    let e = rest.chars().next()?;
    if !matches!(e, 'e' | 'E') {
        return None;
    }
    let exp_start = mantissa_len + e.len_utf8();
    let (exp_len, _) = take_number_with_optional_sign(&input[exp_start..], false)?;
    Some(exp_start + exp_len)
}

fn take_decimal(input: &str) -> Option<usize> {
    let (len, saw_dot) = take_number_with_optional_sign(input, true)?;
    saw_dot.then_some(len)
}

fn take_number_with_optional_sign(input: &str, allow_dot: bool) -> Option<(usize, bool)> {
    let mut index = 0usize;
    if input.starts_with('+') || input.starts_with('-') {
        index += 1;
    }
    let mut saw_digit = false;
    let mut saw_dot = false;
    while let Some(ch) = input[index..].chars().next() {
        if ch.is_ascii_digit() {
            saw_digit = true;
            index += 1;
        } else if allow_dot && ch == '.' && !saw_dot {
            saw_dot = true;
            index += 1;
        } else {
            break;
        }
    }
    (saw_digit
        && (!saw_dot
            || input[..index]
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .count()
                > 0))
        .then_some((index, saw_dot))
}

fn take_hyphenated(input: &str) -> Option<(usize, Vec<ParsedTextSearchToken>)> {
    let mut parts = Vec::new();
    let mut index = 0usize;
    loop {
        let part_len = take_word(&input[index..])?;
        parts.push((index, part_len));
        index += part_len;
        if !input[index..].starts_with('-') {
            break;
        }
        index += 1;
        if starts_decimal_after_hyphen(&input[index..]) {
            return None;
        }
        if take_word(&input[index..]).is_none() {
            return None;
        }
    }
    if parts.len() < 2 {
        return None;
    }
    let mut extras = Vec::new();
    for (part_index, (start, len)) in parts.into_iter().enumerate() {
        if part_index > 0 {
            extras.push(ParsedTextSearchToken {
                tokid: SPACE,
                token: "-".into(),
            });
        }
        let token = &input[start..start + len];
        let tokid = if token.chars().any(|ch| ch.is_ascii_digit()) {
            NUMPARTHWORD
        } else if token.is_ascii() {
            ASCIIPARTHWORD
        } else {
            PARTHWORD
        };
        extras.push(ParsedTextSearchToken {
            tokid,
            token: token.to_string(),
        });
    }
    Some((index, extras))
}

fn take_file_path(input: &str) -> Option<usize> {
    let consumed = take_while(input, |ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-')
    })?;
    let token = &input[..consumed];
    if token.contains('/') || token.contains('.') {
        if !token.contains('/') && token.split('-').skip(1).any(starts_decimal_after_hyphen) {
            return None;
        }
        let trimmed_len = token.trim_end_matches('.').len();
        (trimmed_len > 0).then_some(trimmed_len)
    } else {
        None
    }
}

fn starts_decimal_after_hyphen(input: &str) -> bool {
    let Some((digits_len, _)) = take_number_with_optional_sign(input, false) else {
        return false;
    };
    input[digits_len..].starts_with('.')
}

fn take_word(input: &str) -> Option<usize> {
    take_while(input, |ch| ch.is_alphanumeric() || ch == '_')
}

fn is_email_local_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_emits_overlapping_url_tokens() {
        let tokens = parse_default("http://aew.wer0c.ewr/id?ad=qwe&dw<span>");
        let pairs = tokens
            .iter()
            .map(|token| (token.tokid, token.token.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                (PROTOCOL, "http://"),
                (URL_T, "aew.wer0c.ewr/id?ad=qwe&dw"),
                (HOST, "aew.wer0c.ewr"),
                (URLPATH, "/id?ad=qwe&dw"),
                (TAG_T, "<span>")
            ]
        );
    }

    #[test]
    fn document_tokens_skip_protocol_and_tag_positions() {
        let tokens = document_tokens("http://www.harewoodsolutions.co.uk/press.aspx</span>");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| (token.tokid, token.token, token.position))
                .collect::<Vec<_>>(),
            vec![
                (
                    URL_T,
                    "www.harewoodsolutions.co.uk/press.aspx".to_string(),
                    1
                ),
                (HOST, "www.harewoodsolutions.co.uk".to_string(), 2),
                (URLPATH, "/press.aspx".to_string(), 3)
            ]
        );
    }

    #[test]
    fn parser_consumes_trailing_path_dots() {
        let tokens = parse_default("4.2. gist.c.");
        assert!(tokens.iter().any(|token| token.token == "."));
        assert_eq!(tokens.last().map(|token| token.token.as_str()), Some("."));
    }

    #[test]
    fn parser_splits_signed_decimal_after_word() {
        let tokens = parse_default("readline-4.2");
        assert_eq!(
            tokens
                .iter()
                .map(|token| (token.tokid, token.token.as_str()))
                .collect::<Vec<_>>(),
            vec![(ASCIIWORD, "readline"), (DECIMAL_T, "-4.2")]
        );
    }

    #[test]
    fn parser_groups_separator_runs_before_next_token() {
        let tokens = parse_default("qwe@efd.r ' http://www.com/ http://x.y/");
        let pairs = tokens
            .iter()
            .map(|token| (token.tokid, token.token.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                (ASCIIWORD, "qwe"),
                (SPACE, "@"),
                (FILEPATH, "efd.r"),
                (SPACE, " ' "),
                (PROTOCOL, "http://"),
                (HOST, "www.com"),
                (SPACE, "/ "),
                (ASCIIWORD, "http"),
                (SPACE, ":"),
                (FILEPATH, "//x.y/"),
            ]
        );
    }

    #[test]
    fn parser_rejects_unquoted_nested_xml_tag_start() {
        let tokens = parse_default("sdjk<we hjwer <werrwe>");
        assert!(tokens.iter().any(|token| token.token == "<"));
        assert!(tokens.iter().any(|token| token.token == "<werrwe>"));
    }
}
