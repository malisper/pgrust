use crate::backend::tsearch::cache::{resolve_config, resolve_dictionary};
use crate::backend::tsearch::ts_utils::{
    empty_tsquery, lexize_token_for_config, lexize_token_for_dictionary, normalize_tsquery,
    tokenize_document,
};
use crate::include::nodes::tsearch::{
    TsLexeme, TsPosition, TsQuery, TsQueryNode, TsQueryOperand, TsVector,
};

pub(crate) fn to_tsvector_with_config_name(
    config_name: Option<&str>,
    text: &str,
) -> Result<TsVector, String> {
    let config = resolve_config(config_name)?;
    let lexemes = tokenize_document(text)
        .into_iter()
        .filter_map(|(token, position)| {
            lexize_token_for_config(config, &token).map(|lexeme| TsLexeme {
                text: lexeme.into(),
                positions: vec![TsPosition {
                    position,
                    weight: None,
                }],
            })
        })
        .collect();
    Ok(TsVector::new(lexemes))
}

pub(crate) fn tsvector_lexemes_with_config_name(
    config_name: Option<&str>,
    text: &str,
    start_position: u16,
) -> Result<(Vec<TsLexeme>, u16), String> {
    let config = resolve_config(config_name)?;
    let tokens = tokenize_document(text);
    let next_position = start_position.saturating_add(tokens.len() as u16);
    let lexemes = tokens
        .into_iter()
        .filter_map(|(token, position)| {
            lexize_token_for_config(config, &token).map(|lexeme| TsLexeme {
                text: lexeme.into(),
                positions: vec![TsPosition {
                    position: start_position.saturating_add(position.saturating_sub(1)),
                    weight: None,
                }],
            })
        })
        .collect();
    Ok((lexemes, next_position))
}

pub(crate) fn to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name)?;
    let query = TsQuery::parse(text)?;
    Ok(normalize_tsquery(query, config))
}

pub(crate) fn plainto_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name)?;
    let mut terms = tokenize_document(text)
        .into_iter()
        .filter_map(|(token, _)| lexize_token_for_config(config, &token))
        .map(|lexeme| TsQueryNode::Operand(TsQueryOperand::new(lexeme)));
    let Some(mut root) = terms.next() else {
        return Ok(empty_tsquery());
    };
    for term in terms {
        root = TsQueryNode::And(Box::new(root), Box::new(term));
    }
    Ok(TsQuery::new(root))
}

pub(crate) fn phraseto_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name)?;
    let mut terms = tokenize_document(text)
        .into_iter()
        .filter_map(|(token, _)| lexize_token_for_config(config, &token))
        .map(|lexeme| TsQueryNode::Operand(TsQueryOperand::new(lexeme)));
    let Some(mut root) = terms.next() else {
        return Ok(empty_tsquery());
    };
    for term in terms {
        root = TsQueryNode::Phrase {
            left: Box::new(root),
            right: Box::new(term),
            distance: 1,
        };
    }
    Ok(TsQuery::new(root))
}

pub(crate) fn websearch_to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name)?;
    let mut terms = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in text.chars() {
        match ch {
            '"' => {
                if in_quotes && !current.is_empty() {
                    let phrase = phraseto_tsquery_with_config_name(config_name, current.trim())?;
                    terms.push(phrase.root);
                    current.clear();
                }
                in_quotes = !in_quotes;
            }
            _ if in_quotes => current.push(ch),
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        let query = build_websearch_non_phrase_query(config, current.trim());
        if let Some(query) = query {
            terms.push(query);
        }
    }
    let mut iter = terms.into_iter();
    let Some(mut root) = iter.next() else {
        return Ok(empty_tsquery());
    };
    for node in iter {
        root = TsQueryNode::And(Box::new(root), Box::new(node));
    }
    Ok(TsQuery::new(root))
}

fn build_websearch_non_phrase_query(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    text: &str,
) -> Option<TsQueryNode> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    let mut index = 0usize;
    let mut root = None;
    let mut pending_or = false;
    while index < parts.len() {
        let part = parts[index];
        index += 1;
        if part.eq_ignore_ascii_case("or") {
            pending_or = true;
            continue;
        }
        let (negated, raw) = part
            .strip_prefix('-')
            .map(|rest| (true, rest))
            .unwrap_or((false, part));
        let Some(lexeme) = lexize_token_for_config(config, raw) else {
            continue;
        };
        let mut node = TsQueryNode::Operand(TsQueryOperand::new(lexeme));
        if negated {
            node = TsQueryNode::Not(Box::new(node));
        }
        root = Some(match (root, pending_or) {
            (Some(left), true) => TsQueryNode::Or(Box::new(left), Box::new(node)),
            (Some(left), false) => TsQueryNode::And(Box::new(left), Box::new(node)),
            (None, _) => node,
        });
        pending_or = false;
    }
    root
}

pub(crate) fn ts_lexize_with_dictionary_name(
    dictionary_name: &str,
    text: &str,
) -> Result<Vec<String>, String> {
    let dictionary = resolve_dictionary(dictionary_name)?;
    Ok(lexize_token_for_dictionary(dictionary, text)
        .into_iter()
        .collect())
}
