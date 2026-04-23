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
    Ok(build_websearch_query(config, text)
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSearchItem {
    Node(TsQueryNode),
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSearchToken {
    Node(TsQueryNode),
    Or,
}

fn build_websearch_query(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    text: &str,
) -> Option<TsQueryNode> {
    let mut items = Vec::new();
    let mut text_start = 0usize;
    let mut quote_start = None;
    let mut pending_not = 0usize;

    for (index, ch) in text.char_indices() {
        if ch != '"' {
            continue;
        }
        if let Some(start) = quote_start {
            append_websearch_phrase(config, &text[start..index], pending_not, &mut items);
            quote_start = None;
            text_start = index + ch.len_utf8();
            pending_not = 0;
            continue;
        }

        let prefix = &text[text_start..index];
        let (text_before_quote, quote_not) = split_quote_not_prefix(prefix);
        append_websearch_text(config, text_before_quote, &mut items);
        pending_not = quote_not;
        quote_start = Some(index + ch.len_utf8());
    }

    if let Some(start) = quote_start {
        append_websearch_phrase(config, &text[start..], pending_not, &mut items);
    } else {
        append_websearch_text(config, &text[text_start..], &mut items);
    }
    combine_websearch_items(config, items)
}

fn split_quote_not_prefix(text: &str) -> (&str, usize) {
    let trimmed_end = text.trim_end();
    let mut count = 0usize;
    for ch in trimmed_end.chars().rev() {
        if ch == '-' {
            count += 1;
        } else {
            break;
        }
    }
    if count == 0 {
        return (text, 0);
    }
    let split_at = trimmed_end.len() - count;
    if !trimmed_end[..split_at]
        .chars()
        .next_back()
        .is_none_or(|ch| ch.is_whitespace())
    {
        return (text, 0);
    }
    (&trimmed_end[..split_at], count)
}

fn append_websearch_text(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    text: &str,
    items: &mut Vec<WebSearchItem>,
) {
    for raw in websearch_text_units(text) {
        if raw.eq_ignore_ascii_case("or") {
            items.push(WebSearchItem::Or);
            continue;
        }

        let (not_count, unit) = take_leading_hyphens(&raw);
        if unit.eq_ignore_ascii_case("or") {
            items.push(WebSearchItem::Or);
            continue;
        }
        if let Some(mut node) = websearch_unit_to_node(config, unit) {
            for _ in 0..not_count {
                node = TsQueryNode::Not(Box::new(node));
            }
            items.push(WebSearchItem::Node(node));
        }
    }
}

fn append_websearch_phrase(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    text: &str,
    not_count: usize,
    items: &mut Vec<WebSearchItem>,
) {
    if let Some(mut node) = phrase_node_from_terms(websearch_terms(config, text)) {
        for _ in 0..not_count {
            node = TsQueryNode::Not(Box::new(node));
        }
        items.push(WebSearchItem::Node(node));
    }
}

fn websearch_text_units(text: &str) -> Vec<String> {
    let mut units = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        if ch.is_whitespace() {
            push_websearch_unit(&mut units, &mut current);
            continue;
        }
        if ch.eq_ignore_ascii_case(&'o') && current.eq_ignore_ascii_case("or") {
            push_websearch_unit(&mut units, &mut current);
        }
        if is_websearch_unit_char(ch) || ch == '-' {
            current.push(ch);
        } else {
            push_websearch_unit(&mut units, &mut current);
        }
    }
    push_websearch_unit(&mut units, &mut current);
    units
}

fn push_websearch_unit(units: &mut Vec<String>, current: &mut String) {
    let unit = current.trim_matches('-');
    if !unit.is_empty() {
        units.push(std::mem::take(current));
    } else {
        current.clear();
    }
}

fn is_websearch_unit_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '*' || ch == '\'' || ch == '\\'
}

fn take_leading_hyphens(text: &str) -> (usize, &str) {
    let count = text.chars().take_while(|ch| *ch == '-').count();
    let offset = text
        .char_indices()
        .nth(count)
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    (count, &text[offset..])
}

fn websearch_unit_to_node(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    unit: &str,
) -> Option<TsQueryNode> {
    let terms = websearch_terms(config, unit);
    if unit.contains('-') || unit.contains('*') || unit.contains('_') || unit.contains('\'') {
        return phrase_node_from_terms(terms);
    }
    and_node_from_terms(terms)
}

fn websearch_terms(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    text: &str,
) -> Vec<String> {
    let mut terms = Vec::new();
    for raw in websearch_raw_terms(text) {
        if let Some(lexeme) = lexize_token_for_config(config, &raw) {
            terms.push(lexeme);
        }
    }
    terms
}

fn websearch_raw_terms(text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = String::new();
    let mut hyphenated_parts = Vec::new();

    let flush_word = |current: &mut String, hyphenated_parts: &mut Vec<String>| {
        if current.is_empty() {
            return;
        }
        hyphenated_parts.push(std::mem::take(current));
    };
    let flush_group =
        |terms: &mut Vec<String>, current: &mut String, hyphenated_parts: &mut Vec<String>| {
            flush_word(current, hyphenated_parts);
            match hyphenated_parts.len() {
                0 => {}
                1 => terms.push(hyphenated_parts.pop().unwrap()),
                _ => {
                    terms.push(hyphenated_parts.join("-"));
                    terms.append(hyphenated_parts);
                }
            }
        };

    for ch in text.chars() {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if ch == '-' && !current.is_empty() {
            flush_word(&mut current, &mut hyphenated_parts);
        } else {
            flush_group(&mut terms, &mut current, &mut hyphenated_parts);
        }
    }
    flush_group(&mut terms, &mut current, &mut hyphenated_parts);
    terms
}

fn phrase_node_from_terms(terms: Vec<String>) -> Option<TsQueryNode> {
    let mut terms = terms
        .into_iter()
        .map(|term| TsQueryNode::Operand(TsQueryOperand::new(term)));
    let Some(mut root) = terms.next() else {
        return None;
    };
    for term in terms {
        root = TsQueryNode::Phrase {
            left: Box::new(root),
            right: Box::new(term),
            distance: 1,
        };
    }
    Some(root)
}

fn and_node_from_terms(terms: Vec<String>) -> Option<TsQueryNode> {
    let mut terms = terms
        .into_iter()
        .map(|term| TsQueryNode::Operand(TsQueryOperand::new(term)));
    let Some(mut root) = terms.next() else {
        return None;
    };
    for term in terms {
        root = TsQueryNode::And(Box::new(root), Box::new(term));
    }
    Some(root)
}

fn combine_websearch_items(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    items: Vec<WebSearchItem>,
) -> Option<TsQueryNode> {
    let tokens = websearch_items_to_tokens(config, items);
    let mut or_groups = Vec::new();
    let mut and_group = Vec::new();

    for token in tokens {
        match token {
            WebSearchToken::Node(node) => and_group.push(node),
            WebSearchToken::Or => {
                push_websearch_and_group(&mut or_groups, &mut and_group);
            }
        }
    }
    push_websearch_and_group(&mut or_groups, &mut and_group);

    let mut groups = or_groups.into_iter();
    let Some(mut root) = groups.next() else {
        return None;
    };
    for group in groups {
        root = TsQueryNode::Or(Box::new(root), Box::new(group));
    }
    Some(root)
}

fn websearch_items_to_tokens(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    items: Vec<WebSearchItem>,
) -> Vec<WebSearchToken> {
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < items.len() {
        match &items[index] {
            WebSearchItem::Node(node) => tokens.push(WebSearchToken::Node(node.clone())),
            WebSearchItem::Or if matches!(tokens.last(), Some(WebSearchToken::Node(_))) => {
                if has_websearch_operand(config, &items[index + 1..]) {
                    tokens.push(WebSearchToken::Or);
                } else if let Some(node) = websearch_unit_to_node(config, "or") {
                    tokens.push(WebSearchToken::Node(node));
                }
            }
            WebSearchItem::Or => {
                if let Some(node) = websearch_unit_to_node(config, "or") {
                    tokens.push(WebSearchToken::Node(node));
                }
            }
        }
        index += 1;
    }
    tokens
}

fn has_websearch_operand(
    config: crate::backend::tsearch::cache::TextSearchConfig,
    items: &[WebSearchItem],
) -> bool {
    for item in items {
        match item {
            WebSearchItem::Node(_) => return true,
            WebSearchItem::Or if websearch_unit_to_node(config, "or").is_some() => return true,
            WebSearchItem::Or => {}
        }
    }
    false
}

fn push_websearch_and_group(groups: &mut Vec<TsQueryNode>, group: &mut Vec<TsQueryNode>) {
    let mut nodes = group.drain(..);
    let Some(mut root) = nodes.next() else {
        return;
    };
    for node in nodes {
        root = TsQueryNode::And(Box::new(root), Box::new(node));
    }
    groups.push(root);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn websearch(config_name: &str, text: &str) -> String {
        websearch_to_tsquery_with_config_name(Some(config_name), text)
            .unwrap()
            .render()
    }

    #[test]
    fn websearch_ignores_tsquery_syntax_and_weights() {
        assert_eq!(
            websearch("simple", "I have a fat:*ABCD cat"),
            "i & have & a & fat & abcd & cat"
        );
        assert_eq!(
            websearch("simple", "fat:A!cat:B|rat:C<"),
            "fat & a & cat & b & rat & c"
        );
        assert_eq!(websearch("simple", "abc : def"), "abc & def");
        assert_eq!(websearch("simple", ":"), "''");
        assert_eq!(websearch("simple", "abc & def"), "abc & def");
        assert_eq!(websearch("simple", "abc <-> def"), "abc & def");
    }

    #[test]
    fn websearch_handles_phrases_and_document_tokens() {
        assert_eq!(websearch("simple", "fat*rat"), "fat <-> rat");
        assert_eq!(websearch("simple", "fat-rat"), "'fat-rat' <-> fat <-> rat");
        assert_eq!(websearch("simple", "fat_rat"), "fat <-> rat");
        assert_eq!(
            websearch("english", "\"pg_class pg\""),
            "pg <-> class <-> pg"
        );
        assert_eq!(
            websearch("english", "abc \"pg_class pg\""),
            "abc & pg <-> class <-> pg"
        );
    }

    #[test]
    fn websearch_handles_or_and_negated_phrases() {
        assert_eq!(websearch("simple", "cat or rat"), "cat | rat");
        assert_eq!(websearch("simple", "cat OR"), "cat & or");
        assert_eq!(websearch("simple", "OR rat"), "or & rat");
        assert_eq!(websearch("simple", "or OR or"), "or | or");
        assert_eq!(
            websearch("simple", "(foo bar) or (ding dong)"),
            "foo & bar | ding & dong"
        );
        assert_eq!(
            websearch("simple", "\"fat cat\"or\"fat rat\""),
            "fat <-> cat | fat <-> rat"
        );
        assert_eq!(
            websearch("english", "cat -\"fat rat\" cheese"),
            "cat & !(fat <-> rat) & chees"
        );
        assert_eq!(websearch("english", "this is ----fine"), "!!!!fine");
        assert_eq!(
            websearch("english", " or \"pg pg_class pg\" or "),
            "pg <-> pg <-> class <-> pg"
        );
    }
}
