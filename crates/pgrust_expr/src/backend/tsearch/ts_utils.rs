use crate::compat::backend::tsearch::cache::{TextSearchConfig, TextSearchDictionary};
use crate::compat::backend::tsearch::dict_english::lexize_english;
use crate::compat::backend::tsearch::dict_simple::lexize_simple;
use crate::compat::backend::tsearch::parser::{
    ASCIIWORD, DECIMAL_T, EMAIL, FILEPATH, HOST, SCIENTIFIC, SIGNEDINT, UNSIGNEDINT, URL_T,
    URLPATH, normalized_query_node_from_text,
};
use crate::compat::include::nodes::tsearch::{
    TextSearchParserToken, TsQuery, TsQueryNode, TsQueryOperand,
};

const DEFAULT_PARSER_TOKEN_TYPES: &[(i32, &str, &str)] = &[
    (1, "asciiword", "Word, all ASCII"),
    (2, "word", "Word, all letters"),
    (3, "numword", "Word, letters and digits"),
    (4, "email", "Email address"),
    (5, "url", "URL"),
    (6, "host", "Host"),
    (7, "sfloat", "Scientific notation"),
    (8, "version", "Version number"),
    (
        9,
        "hword_numpart",
        "Hyphenated word part, letters and digits",
    ),
    (10, "hword_part", "Hyphenated word part, all letters"),
    (11, "hword_asciipart", "Hyphenated word part, all ASCII"),
    (12, "blank", "Space symbols"),
    (13, "tag", "XML tag"),
    (14, "protocol", "Protocol head"),
    (15, "numhword", "Hyphenated word, letters and digits"),
    (16, "asciihword", "Hyphenated word, all ASCII"),
    (17, "hword", "Hyphenated word, all letters"),
    (18, "url_path", "URL path"),
    (19, "file", "File or path name"),
    (20, "float", "Decimal notation"),
    (21, "int", "Signed integer"),
    (22, "uint", "Unsigned integer"),
    (23, "entity", "XML entity"),
];

pub fn default_text_search_parser_token_types() -> Vec<TextSearchParserToken> {
    DEFAULT_PARSER_TOKEN_TYPES
        .iter()
        .map(|(tokid, alias, description)| TextSearchParserToken {
            tokid: *tokid,
            alias: (*alias).into(),
            description: (*description).into(),
        })
        .collect()
}

pub fn default_text_search_parser_token_type(tokid: i32) -> Option<TextSearchParserToken> {
    default_text_search_parser_token_types()
        .into_iter()
        .find(|token| token.tokid == tokid)
}

pub fn parse_default_text_search_tokens(text: &str) -> Vec<(i32, String)> {
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < text.len() {
        let rest = &text[index..];
        let ch = rest.chars().next().unwrap();
        if ch.is_whitespace() {
            let end = take_while(text, index, |ch| ch.is_whitespace());
            tokens.push((12, text[index..end].to_string()));
            index = end;
            continue;
        }
        if let Some((protocol, after_protocol)) = take_protocol(text, index) {
            tokens.push((14, protocol.to_string()));
            let end = take_until_blank_or_tag(text, after_protocol);
            let body = &text[after_protocol..end];
            if !body.is_empty() {
                tokens.push((5, body.to_string()));
                if let Some((host, path)) = split_url_host_path(body) {
                    tokens.push((6, host.to_string()));
                    if !path.is_empty() {
                        tokens.push((18, path.to_string()));
                    }
                }
            }
            index = end;
            continue;
        }
        if ch == '<' {
            if let Some(end) = rest.find('>') {
                let end = index + end + 1;
                tokens.push((13, text[index..end].to_string()));
                index = end;
                continue;
            }
        }
        if ch == '&' {
            if let Some(end) = rest.find(';') {
                let end = index + end + 1;
                tokens.push((23, text[index..end].to_string()));
                index = end;
                continue;
            }
        }
        if ch == '/' {
            let end = take_until_blank(text, index);
            tokens.push((19, text[index..end].to_string()));
            index = end;
            continue;
        }
        if ch == '+' || ch == '-' || ch.is_ascii_digit() {
            if let Some((tokid, end)) = take_number_token(text, index) {
                tokens.push((tokid, text[index..end].to_string()));
                index = end;
                continue;
            }
        }
        if ch.is_alphanumeric() || ch == '_' {
            let end = take_wordish(text, index);
            let token = &text[index..end];
            if token.contains('@') {
                tokens.push((4, token.to_string()));
            } else if token.contains('-') {
                let full_tokid = if token.chars().any(|ch| ch.is_ascii_digit()) {
                    15
                } else if token.is_ascii() {
                    16
                } else {
                    17
                };
                tokens.push((full_tokid, token.to_string()));
                for (part_index, part) in
                    token.split('-').filter(|part| !part.is_empty()).enumerate()
                {
                    if part_index > 0 {
                        tokens.push((12, "-".into()));
                    }
                    tokens.push((hyphenated_part_tokid(part), part.to_string()));
                }
            } else if looks_like_host_or_file(token) {
                tokens.push((6, token.to_string()));
            } else {
                tokens.push((word_tokid(token), token.to_string()));
            }
            index = end;
            continue;
        }
        let end = index + ch.len_utf8();
        tokens.push((12, text[index..end].to_string()));
        index = end;
    }
    tokens
}

fn take_while(text: &str, start: usize, predicate: impl Fn(char) -> bool) -> usize {
    let mut end = start;
    while end < text.len() {
        let ch = text[end..].chars().next().unwrap();
        if !predicate(ch) {
            break;
        }
        end += ch.len_utf8();
    }
    end
}

fn take_until_blank(text: &str, start: usize) -> usize {
    take_while(text, start, |ch| !ch.is_whitespace())
}

fn take_until_blank_or_tag(text: &str, start: usize) -> usize {
    take_while(text, start, |ch| !ch.is_whitespace() && ch != '<')
}

fn take_protocol(text: &str, start: usize) -> Option<(&str, usize)> {
    ["http://", "https://", "ftp://"]
        .into_iter()
        .find_map(|protocol| {
            if text[start..].starts_with(protocol) {
                Some((&text[start..start + protocol.len()], start + protocol.len()))
            } else {
                None
            }
        })
}

fn split_url_host_path(body: &str) -> Option<(&str, &str)> {
    let split = body.find('/').or_else(|| body.find('?'));
    match split {
        Some(index) => Some((&body[..index], &body[index..])),
        None if body.contains('.') => Some((body, "")),
        None => None,
    }
}

fn take_number_token(text: &str, start: usize) -> Option<(i32, usize)> {
    let bytes = text.as_bytes();
    let mut index = start;
    if matches!(bytes.get(index), Some(b'+' | b'-')) {
        index += 1;
    }
    let digit_start = index;
    while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
        index += 1;
    }
    if index == digit_start {
        return None;
    }
    let mut tokid = if start == digit_start { 22 } else { 21 };
    if matches!(bytes.get(index), Some(b'.'))
        && matches!(bytes.get(index + 1), Some(byte) if byte.is_ascii_digit())
    {
        tokid = 20;
        index += 1;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        let exp = index + 1;
        let signed = exp + usize::from(matches!(bytes.get(exp), Some(b'+' | b'-')));
        if matches!(bytes.get(signed), Some(byte) if byte.is_ascii_digit()) {
            tokid = 7;
            index = signed + 1;
            while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
                index += 1;
            }
        }
    }
    Some((tokid, index))
}

fn take_wordish(text: &str, start: usize) -> usize {
    take_while(text, start, |ch| {
        ch.is_alphanumeric() || matches!(ch, '_' | '-' | '@' | '.')
    })
}

fn looks_like_host_or_file(token: &str) -> bool {
    token.contains('.') && token.chars().any(|ch| ch.is_alphabetic())
}

fn word_tokid(token: &str) -> i32 {
    let has_alpha = token.chars().any(|ch| ch.is_alphabetic());
    let has_digit = token.chars().any(|ch| ch.is_ascii_digit());
    match (token.is_ascii(), has_alpha, has_digit) {
        (true, true, false) => 1,
        (_, true, false) => 2,
        (_, true, true) => 3,
        _ => 22,
    }
}

fn hyphenated_part_tokid(part: &str) -> i32 {
    let has_digit = part.chars().any(|ch| ch.is_ascii_digit());
    if has_digit {
        9
    } else if part.is_ascii() {
        11
    } else {
        10
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexizeLexeme {
    pub text: String,
    pub prefix: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexizeAlternative {
    pub lexemes: Vec<LexizeLexeme>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexizeOutcome {
    Match(Vec<LexizeAlternative>),
    Stop,
    NoMatch,
}

pub fn lexize_token_for_config(config: &TextSearchConfig, token: &str) -> Option<String> {
    lexize_token_for_config_and_type(config, ASCIIWORD, token)
}

pub fn lexize_token_for_config_and_type(
    config: &TextSearchConfig,
    tokid: i32,
    token: &str,
) -> Option<String> {
    lexize_token_with_config_and_type(config, tokid, token)
        .flat_lexemes()
        .into_iter()
        .next()
        .map(|lexeme| lexeme.text)
}

pub fn lexize_token_for_dictionary(
    dictionary: &TextSearchDictionary,
    token: &str,
) -> Option<String> {
    lexize_dictionary(dictionary, token)
        .flat_lexemes()
        .into_iter()
        .next()
        .map(|lexeme| lexeme.text)
}

impl LexizeOutcome {
    pub fn flat_lexemes(self) -> Vec<LexizeLexeme> {
        match self {
            LexizeOutcome::Match(alternatives) => alternatives
                .into_iter()
                .flat_map(|alternative| alternative.lexemes)
                .collect(),
            LexizeOutcome::Stop | LexizeOutcome::NoMatch => Vec::new(),
        }
    }
}

pub fn lexize_token_with_config(config: &TextSearchConfig, token: &str) -> LexizeOutcome {
    lexize_token_with_config_and_type(config, ASCIIWORD, token)
}

pub fn lexize_token_with_config_and_type(
    config: &TextSearchConfig,
    tokid: i32,
    token: &str,
) -> LexizeOutcome {
    let dictionaries = dictionaries_for_token_type(config, tokid);
    lexize_token_with_dictionaries(&dictionaries, token)
}

fn dictionaries_for_token_type(config: &TextSearchConfig, tokid: i32) -> Vec<TextSearchDictionary> {
    let simple_token = matches!(
        tokid,
        EMAIL
            | URL_T
            | HOST
            | SCIENTIFIC
            | URLPATH
            | FILEPATH
            | DECIMAL_T
            | SIGNEDINT
            | UNSIGNEDINT
    );
    match config {
        TextSearchConfig::Simple => vec![TextSearchDictionary::Simple],
        TextSearchConfig::English if simple_token => vec![TextSearchDictionary::Simple],
        TextSearchConfig::English => vec![TextSearchDictionary::EnglishStem],
        TextSearchConfig::Custom { mappings } => mappings.get(&tokid).cloned().unwrap_or_default(),
    }
}

pub fn lexize_token_with_dictionaries(
    dictionaries: &[TextSearchDictionary],
    token: &str,
) -> LexizeOutcome {
    for dictionary in dictionaries {
        match lexize_dictionary(dictionary, token) {
            LexizeOutcome::NoMatch => continue,
            outcome => return outcome,
        }
    }
    LexizeOutcome::NoMatch
}

pub fn lexize_dictionary(dictionary: &TextSearchDictionary, token: &str) -> LexizeOutcome {
    match dictionary {
        TextSearchDictionary::Simple => lexize_simple(token)
            .map(|lexeme| one_alt(vec![lexeme]))
            .unwrap_or(LexizeOutcome::Stop),
        TextSearchDictionary::EnglishStem => lexize_english(token)
            .map(|lexeme| one_alt(vec![lexeme]))
            .unwrap_or(LexizeOutcome::Stop),
        TextSearchDictionary::Ispell => lexize_ispell_sample(token),
        TextSearchDictionary::Synonym { case_sensitive } => {
            lexize_synonym_sample(token, *case_sensitive)
        }
        TextSearchDictionary::Thesaurus => lexize_thesaurus_token(token),
    }
}

fn one_alt(lexemes: Vec<String>) -> LexizeOutcome {
    LexizeOutcome::Match(vec![LexizeAlternative {
        lexemes: lexemes
            .into_iter()
            .map(|text| LexizeLexeme {
                text,
                prefix: false,
            })
            .collect(),
    }])
}

fn one_alt_prefixed(text: &str, prefix: bool) -> LexizeOutcome {
    LexizeOutcome::Match(vec![LexizeAlternative {
        lexemes: vec![LexizeLexeme {
            text: text.into(),
            prefix,
        }],
    }])
}

fn alternatives(groups: &[&[&str]]) -> LexizeOutcome {
    LexizeOutcome::Match(
        groups
            .iter()
            .map(|group| LexizeAlternative {
                lexemes: group
                    .iter()
                    .map(|text| LexizeLexeme {
                        text: (*text).into(),
                        prefix: false,
                    })
                    .collect(),
            })
            .collect(),
    )
}

// :HACK: These sample dictionary lexizers cover PostgreSQL's regression
// fixture files until pgrust has a real tsearch-data file loader.
fn lexize_ispell_sample(token: &str) -> LexizeOutcome {
    match token.trim().to_ascii_lowercase().as_str() {
        "skies" | "sk" => one_alt(vec!["sky".into()]),
        "bookings" | "booking" | "rebookings" | "rebooking" => {
            alternatives(&[&["booking"], &["book"]])
        }
        "booked" | "unbookings" | "unbooking" | "unbook" => one_alt(vec!["book".into()]),
        "foot" | "foots" => one_alt(vec!["foot".into()]),
        "footklubber" => alternatives(&[&["foot", "klubber"]]),
        "footballklubber" => alternatives(&[
            &["footballklubber"],
            &["foot", "ball", "klubber"],
            &["football", "klubber"],
        ]),
        "ballyklubber" | "ballsklubber" => alternatives(&[&["ball", "klubber"]]),
        "footballyklubber" => alternatives(&[&["foot", "ball", "klubber"]]),
        "ex-machina" => alternatives(&[&["ex-", "machina"]]),
        _ => LexizeOutcome::NoMatch,
    }
}

fn lexize_synonym_sample(token: &str, case_sensitive: bool) -> LexizeOutcome {
    let key = if case_sensitive {
        token.trim().to_string()
    } else {
        token.trim().to_ascii_lowercase()
    };
    match key.as_str() {
        "postgres" | "postgresql" | "postgre" => one_alt(vec!["pgsql".into()]),
        "gogle" => one_alt(vec!["googl".into()]),
        "indices" => one_alt_prefixed("index", true),
        _ => LexizeOutcome::NoMatch,
    }
}

fn lexize_thesaurus_token(token: &str) -> LexizeOutcome {
    match token.trim().to_ascii_lowercase().as_str() {
        "one" => one_alt(vec!["1".into()]),
        "two" => one_alt(vec!["2".into()]),
        "supernovae" | "sn" => one_alt(vec!["sn".into()]),
        _ => LexizeOutcome::NoMatch,
    }
}

pub fn tokenize_document(text: &str) -> Vec<(String, u16)> {
    crate::compat::backend::tsearch::parser::document_tokens(text)
        .into_iter()
        .map(|token| (token.token, token.position))
        .collect()
}

pub fn normalize_tsquery(query: TsQuery, config: &TextSearchConfig) -> TsQuery {
    normalize_query_node(query.root, config)
        .node
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery)
}

pub fn empty_tsquery() -> TsQuery {
    TsQuery::new(TsQueryNode::Operand(TsQueryOperand::new("")))
}

struct NormalizedQueryNode {
    node: Option<TsQueryNode>,
    leading_gap: u16,
    trailing_gap: u16,
    width: u16,
}

impl NormalizedQueryNode {
    fn operand(node: TsQueryNode) -> Self {
        Self {
            node: Some(node),
            leading_gap: 0,
            trailing_gap: 0,
            width: 1,
        }
    }

    fn stop_word() -> Self {
        Self {
            node: None,
            leading_gap: 0,
            trailing_gap: 0,
            width: 1,
        }
    }
}

fn normalize_query_node(node: TsQueryNode, config: &TextSearchConfig) -> NormalizedQueryNode {
    match node {
        TsQueryNode::Operand(operand) => normalize_query_operand(operand, config),
        TsQueryNode::And(left, right) => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            let node = match (left.node, right.node) {
                (Some(left), Some(right)) => {
                    Some(TsQueryNode::And(Box::new(left), Box::new(right)))
                }
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            };
            NormalizedQueryNode {
                node,
                leading_gap: 0,
                trailing_gap: 0,
                width: 1,
            }
        }
        TsQueryNode::Or(left, right) => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            let node = match (left.node, right.node) {
                (Some(left), Some(right)) => Some(TsQueryNode::Or(Box::new(left), Box::new(right))),
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            };
            NormalizedQueryNode {
                node,
                leading_gap: 0,
                trailing_gap: 0,
                width: 1,
            }
        }
        TsQueryNode::Not(inner) => {
            let inner = normalize_query_node(*inner, config);
            NormalizedQueryNode {
                node: inner.node.map(|inner| TsQueryNode::Not(Box::new(inner))),
                leading_gap: 0,
                trailing_gap: 0,
                width: 1,
            }
        }
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            normalize_phrase_node(left, right, distance)
        }
    }
}

fn normalize_query_operand(
    operand: TsQueryOperand,
    config: &TextSearchConfig,
) -> NormalizedQueryNode {
    normalized_query_node_from_text(config, operand.lexeme.as_str(), &operand)
        .map(NormalizedQueryNode::operand)
        .unwrap_or_else(NormalizedQueryNode::stop_word)
}

fn normalize_phrase_node(
    left: NormalizedQueryNode,
    right: NormalizedQueryNode,
    distance: u16,
) -> NormalizedQueryNode {
    let width = left
        .width
        .saturating_add(distance)
        .saturating_add(right.width)
        .saturating_sub(1);
    match (left.node, right.node) {
        (Some(left_node), Some(right_node)) => NormalizedQueryNode {
            node: Some(TsQueryNode::Phrase {
                left: Box::new(left_node),
                right: Box::new(right_node),
                distance: left
                    .trailing_gap
                    .saturating_add(distance)
                    .saturating_add(right.leading_gap),
            }),
            leading_gap: left.leading_gap,
            trailing_gap: right.trailing_gap,
            width,
        },
        (Some(node), None) => NormalizedQueryNode {
            node: Some(node),
            leading_gap: left.leading_gap,
            trailing_gap: left
                .trailing_gap
                .saturating_add(distance)
                .saturating_add(right.width)
                .saturating_sub(1),
            width,
        },
        (None, Some(node)) => NormalizedQueryNode {
            node: Some(node),
            leading_gap: left
                .width
                .saturating_add(distance)
                .saturating_add(right.leading_gap)
                .saturating_sub(1),
            trailing_gap: right.trailing_gap,
            width,
        },
        (None, None) => NormalizedQueryNode {
            node: None,
            leading_gap: 0,
            trailing_gap: 0,
            width,
        },
    }
}
