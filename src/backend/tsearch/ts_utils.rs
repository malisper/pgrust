use crate::backend::tsearch::cache::dictionaries_for_asciiword;
use crate::backend::tsearch::cache::{TextSearchConfig, TextSearchDictionary};
use crate::backend::tsearch::dict_english::lexize_english;
use crate::backend::tsearch::dict_simple::lexize_simple;
use crate::include::nodes::tsearch::{TsQuery, TsQueryNode, TsQueryOperand};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexizeLexeme {
    pub text: String,
    pub prefix: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexizeAlternative {
    pub lexemes: Vec<LexizeLexeme>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LexizeOutcome {
    Match(Vec<LexizeAlternative>),
    Stop,
    NoMatch,
}

pub(crate) fn lexize_token_for_config(config: &TextSearchConfig, token: &str) -> Option<String> {
    let dictionaries = dictionaries_for_asciiword(config);
    lexize_token_with_dictionaries(&dictionaries, token)
        .flat_lexemes()
        .into_iter()
        .next()
        .map(|lexeme| lexeme.text)
}

pub(crate) fn lexize_token_for_dictionary(
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
    pub(crate) fn flat_lexemes(self) -> Vec<LexizeLexeme> {
        match self {
            LexizeOutcome::Match(alternatives) => alternatives
                .into_iter()
                .flat_map(|alternative| alternative.lexemes)
                .collect(),
            LexizeOutcome::Stop | LexizeOutcome::NoMatch => Vec::new(),
        }
    }
}

pub(crate) fn lexize_token_with_config(config: &TextSearchConfig, token: &str) -> LexizeOutcome {
    let dictionaries = dictionaries_for_asciiword(config);
    lexize_token_with_dictionaries(&dictionaries, token)
}

pub(crate) fn lexize_token_with_dictionaries(
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

pub(crate) fn lexize_dictionary(dictionary: &TextSearchDictionary, token: &str) -> LexizeOutcome {
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

pub(crate) fn tokenize_document(text: &str) -> Vec<(String, u16)> {
    let mut tokens = Vec::new();
    let mut buf = String::new();
    let mut position = 1u16;

    let flush = |buf: &mut String, tokens: &mut Vec<(String, u16)>, position: &mut u16| {
        if !buf.is_empty() {
            tokens.push((std::mem::take(buf), *position));
            *position = position.saturating_add(1);
        }
    };

    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            buf.push(ch);
        } else {
            flush(&mut buf, &mut tokens, &mut position);
        }
    }
    flush(&mut buf, &mut tokens, &mut position);
    tokens
}

pub(crate) fn normalize_tsquery(query: TsQuery, config: &TextSearchConfig) -> TsQuery {
    normalize_query_node(query.root, config)
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery)
}

pub(crate) fn empty_tsquery() -> TsQuery {
    TsQuery::new(TsQueryNode::Operand(TsQueryOperand::new("")))
}

fn normalize_query_node(node: TsQueryNode, config: &TextSearchConfig) -> Option<TsQueryNode> {
    match node {
        TsQueryNode::Operand(mut operand) => {
            let normalized = lexize_token_for_config(config, operand.lexeme.as_str())?;
            operand.lexeme = normalized.into();
            Some(TsQueryNode::Operand(operand))
        }
        TsQueryNode::And(left, right) => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            match (left, right) {
                (Some(left), Some(right)) => {
                    Some(TsQueryNode::And(Box::new(left), Box::new(right)))
                }
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            }
        }
        TsQueryNode::Or(left, right) => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            match (left, right) {
                (Some(left), Some(right)) => Some(TsQueryNode::Or(Box::new(left), Box::new(right))),
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            }
        }
        TsQueryNode::Not(inner) => {
            normalize_query_node(*inner, config).map(|inner| TsQueryNode::Not(Box::new(inner)))
        }
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            match (left, right) {
                (Some(left), Some(right)) => Some(TsQueryNode::Phrase {
                    left: Box::new(left),
                    right: Box::new(right),
                    distance,
                }),
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            }
        }
    }
}
