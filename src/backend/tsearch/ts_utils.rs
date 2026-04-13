use crate::backend::tsearch::cache::{TextSearchConfig, TextSearchDictionary};
use crate::backend::tsearch::dict_english::lexize_english;
use crate::backend::tsearch::dict_simple::lexize_simple;
use crate::include::nodes::tsearch::{TsQuery, TsQueryNode, TsQueryOperand};

pub(crate) fn lexize_token_for_config(config: TextSearchConfig, token: &str) -> Option<String> {
    match config {
        TextSearchConfig::Simple => lexize_simple(token),
        TextSearchConfig::English => lexize_english(token),
    }
}

pub(crate) fn lexize_token_for_dictionary(
    dictionary: TextSearchDictionary,
    token: &str,
) -> Option<String> {
    match dictionary {
        TextSearchDictionary::Simple => lexize_simple(token),
        TextSearchDictionary::EnglishStem => lexize_english(token),
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

pub(crate) fn normalize_tsquery(query: TsQuery, config: TextSearchConfig) -> TsQuery {
    normalize_query_node(query.root, config)
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery)
}

pub(crate) fn empty_tsquery() -> TsQuery {
    TsQuery::new(TsQueryNode::Operand(TsQueryOperand::new("")))
}

fn normalize_query_node(node: TsQueryNode, config: TextSearchConfig) -> Option<TsQueryNode> {
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
