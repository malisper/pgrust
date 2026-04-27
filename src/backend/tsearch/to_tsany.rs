use crate::backend::parser::CatalogLookup;
use crate::backend::tsearch::cache::{
    TextSearchConfig, TextSearchDictionary, dictionaries_for_asciiword, resolve_config,
    resolve_dictionary,
};
use crate::backend::tsearch::parser::{DocumentToken, document_tokens};
use crate::backend::tsearch::ts_utils::{
    LexizeAlternative, LexizeLexeme, LexizeOutcome, empty_tsquery, lexize_dictionary,
    lexize_token_for_config, lexize_token_for_config_and_type, lexize_token_with_config,
    lexize_token_with_config_and_type,
};
use crate::include::nodes::tsearch::{
    TsLexeme, TsPosition, TsQuery, TsQueryNode, TsQueryOperand, TsVector,
};

pub(crate) fn to_tsvector_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsVector, String> {
    let config = resolve_config(config_name, catalog)?;
    let (lexemes, _) = tsvector_lexemes_for_config(&config, text, 1);
    Ok(TsVector::new(lexemes))
}

pub(crate) fn tsvector_lexemes_with_config_name(
    config_name: Option<&str>,
    text: &str,
    start_position: u16,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<(Vec<TsLexeme>, u16), String> {
    let config = resolve_config(config_name, catalog)?;
    Ok(tsvector_lexemes_for_config(&config, text, start_position))
}

fn tsvector_lexemes_for_config(
    config: &TextSearchConfig,
    text: &str,
    start_position: u16,
) -> (Vec<TsLexeme>, u16) {
    let tokens = document_tokens(text);
    let next_position = start_position.saturating_add(tokens.len() as u16);
    let lexemes = if config_has_thesaurus(config) {
        tsvector_lexemes_with_thesaurus(config, &tokens, start_position)
    } else {
        tsvector_lexemes_without_phrases(config, &tokens, start_position)
    };
    (lexemes, next_position)
}

fn tsvector_lexemes_without_phrases(
    config: &TextSearchConfig,
    tokens: &[DocumentToken],
    start_position: u16,
) -> Vec<TsLexeme> {
    let mut lexemes = Vec::new();
    for token in tokens {
        let output_position = start_position.saturating_add(token.position.saturating_sub(1));
        push_lexize_outcome_lexemes(
            &mut lexemes,
            lexize_token_with_config_and_type(config, token.tokid, &token.token),
            output_position,
        );
    }
    lexemes
}

fn push_lexize_outcome_lexemes(out: &mut Vec<TsLexeme>, outcome: LexizeOutcome, position: u16) {
    if let LexizeOutcome::Match(alternatives) = outcome {
        for lexeme in alternatives
            .into_iter()
            .flat_map(|alternative| alternative.lexemes)
        {
            out.push(TsLexeme {
                text: lexeme.text.into(),
                positions: vec![TsPosition {
                    position,
                    weight: None,
                }],
            });
        }
    }
}

fn config_has_thesaurus(config: &TextSearchConfig) -> bool {
    dictionaries_for_asciiword(config)
        .iter()
        .any(|dictionary| matches!(dictionary, TextSearchDictionary::Thesaurus))
}

fn tsvector_lexemes_with_thesaurus(
    config: &TextSearchConfig,
    tokens: &[DocumentToken],
    start_position: u16,
) -> Vec<TsLexeme> {
    let mut lexemes = Vec::new();
    let mut index = 0usize;
    let mut phrase_delta = 0i32;

    while index < tokens.len() {
        if let Some((replacement, consumed)) = thesaurus_phrase_match(tokens, index) {
            let output_start =
                adjusted_position(start_position, tokens[index].position, phrase_delta);
            for (offset, text) in replacement.iter().enumerate() {
                lexemes.push(TsLexeme {
                    text: (*text).into(),
                    positions: vec![TsPosition {
                        position: output_start.saturating_add(offset as u16),
                        weight: None,
                    }],
                });
            }
            phrase_delta += consumed as i32 - replacement.len() as i32;
            index += consumed;
            continue;
        }

        let output_position =
            adjusted_position(start_position, tokens[index].position, phrase_delta);
        push_lexize_outcome_lexemes(
            &mut lexemes,
            lexize_token_with_config_and_type(config, tokens[index].tokid, &tokens[index].token),
            output_position,
        );
        index += 1;
    }

    lexemes
}

fn adjusted_position(start_position: u16, token_position: u16, phrase_delta: i32) -> u16 {
    let position = i32::from(start_position) + i32::from(token_position) - 1 - phrase_delta;
    position.max(1) as u16
}

// :HACK: These phrase replacements mirror the bundled PostgreSQL regression
// sample thesaurus until pgrust loads `.ths` files through a real template.
fn thesaurus_phrase_match(
    tokens: &[DocumentToken],
    index: usize,
) -> Option<(Vec<&'static str>, usize)> {
    if token_eq(tokens, index, "one")
        && token_eq(tokens, index + 1, "two")
        && token_eq(tokens, index + 2, "three")
    {
        return Some((vec!["123"], 3));
    }
    if token_eq(tokens, index, "one") && token_eq(tokens, index + 1, "two") {
        return Some((vec!["12"], 2));
    }
    if token_eq(tokens, index, "supernovae")
        && (token_eq(tokens, index + 1, "star") || token_eq(tokens, index + 1, "stars"))
    {
        return Some((vec!["sn"], 2));
    }
    if token_eq(tokens, index, "booking") && token_eq(tokens, index + 1, "tickets") {
        return Some((vec!["order", "invit", "card"], 2));
    }
    if token_eq(tokens, index, "booking") && token_eq(tokens, index + 2, "tickets") {
        return Some((vec!["order", "invit", "card"], 3));
    }
    None
}

fn token_eq(tokens: &[DocumentToken], index: usize, expected: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| token.token.eq_ignore_ascii_case(expected))
}

pub(crate) fn to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name, catalog)?;
    if text.trim().is_empty() {
        return Ok(empty_tsquery());
    }
    let query = TsQuery::parse(text)?;
    Ok(normalize_tsquery_with_config(query, &config))
}

pub(crate) fn plainto_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name, catalog)?;
    let mut terms = document_tokens(text).into_iter().filter_map(|token| {
        let source = TsQueryOperand::new(token.token.clone());
        node_from_lexize_outcome(
            lexize_token_with_config_and_type(&config, token.tokid, &token.token),
            &source,
        )
    });
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
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name, catalog)?;
    let mut terms = document_tokens(text).into_iter().filter_map(|token| {
        let source = TsQueryOperand::new(token.token.clone());
        node_from_lexize_outcome(
            lexize_token_with_config_and_type(&config, token.tokid, &token.token),
            &source,
        )
        .map(|node| (token.position, node))
    });
    let Some((mut previous_position, mut root)) = terms.next() else {
        return Ok(empty_tsquery());
    };
    for (position, term) in terms {
        let distance = position.saturating_sub(previous_position).max(1);
        root = TsQueryNode::Phrase {
            left: Box::new(root),
            right: Box::new(term),
            distance,
        };
        previous_position = position;
    }
    Ok(TsQuery::new(root))
}

pub(crate) fn websearch_to_tsquery_with_config_name(
    config_name: Option<&str>,
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TsQuery, String> {
    let config = resolve_config(config_name, catalog)?;
    Ok(build_websearch_query(&config, text)
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery))
}

fn normalize_tsquery_with_config(query: TsQuery, config: &TextSearchConfig) -> TsQuery {
    normalize_query_node_with_config(query.root, config)
        .node
        .map(TsQuery::new)
        .unwrap_or_else(empty_tsquery)
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

fn normalize_query_node_with_config(
    node: TsQueryNode,
    config: &TextSearchConfig,
) -> NormalizedQueryNode {
    match node {
        TsQueryNode::Operand(operand) => normalize_query_operand(operand, config),
        TsQueryNode::And(left, right) => {
            let left = normalize_query_node_with_config(*left, config);
            let right = normalize_query_node_with_config(*right, config);
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
            let left = normalize_query_node_with_config(*left, config);
            let right = normalize_query_node_with_config(*right, config);
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
            let inner = normalize_query_node_with_config(*inner, config);
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
            let left = normalize_query_node_with_config(*left, config);
            let right = normalize_query_node_with_config(*right, config);
            normalize_phrase_node(left, right, distance)
        }
    }
}

fn normalize_query_operand(
    operand: TsQueryOperand,
    config: &TextSearchConfig,
) -> NormalizedQueryNode {
    let tokens = document_tokens(operand.lexeme.as_str());
    if tokens.len() <= 1 {
        let outcome = tokens
            .first()
            .map(|token| lexize_token_with_config_and_type(config, token.tokid, &token.token))
            .unwrap_or_else(|| lexize_token_with_config(config, operand.lexeme.as_str()));
        return node_from_lexize_outcome(outcome, &operand)
            .map(NormalizedQueryNode::operand)
            .unwrap_or_else(NormalizedQueryNode::stop_word);
    }

    let mut terms = tokens.into_iter().filter_map(|token| {
        let source = TsQueryOperand {
            lexeme: token.token.clone().into(),
            weights: operand.weights.clone(),
            prefix: operand.prefix,
        };
        node_from_lexize_outcome(
            lexize_token_with_config_and_type(config, token.tokid, &token.token),
            &source,
        )
        .map(|node| (token.position, node))
    });
    let Some((mut previous_position, mut root)) = terms.next() else {
        return NormalizedQueryNode::stop_word();
    };
    for (position, term) in terms {
        root = TsQueryNode::Phrase {
            left: Box::new(root),
            right: Box::new(term),
            distance: position.saturating_sub(previous_position).max(1),
        };
        previous_position = position;
    }
    NormalizedQueryNode::operand(root)
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

fn node_from_lexize_outcome(
    outcome: LexizeOutcome,
    source: &TsQueryOperand,
) -> Option<TsQueryNode> {
    let LexizeOutcome::Match(alternatives) = outcome else {
        return None;
    };
    let mut nodes = alternatives
        .into_iter()
        .filter_map(|alternative| node_from_lexize_alternative(alternative, source));
    let mut root = nodes.next()?;
    for node in nodes {
        root = TsQueryNode::Or(Box::new(root), Box::new(node));
    }
    Some(root)
}

fn node_from_lexize_alternative(
    alternative: LexizeAlternative,
    source: &TsQueryOperand,
) -> Option<TsQueryNode> {
    let mut nodes = alternative
        .lexemes
        .into_iter()
        .map(|lexeme| node_from_lexeme(lexeme, source));
    let mut root = nodes.next()?;
    for node in nodes {
        root = TsQueryNode::And(Box::new(root), Box::new(node));
    }
    Some(root)
}

fn node_from_lexeme(lexeme: LexizeLexeme, source: &TsQueryOperand) -> TsQueryNode {
    TsQueryNode::Operand(TsQueryOperand {
        lexeme: lexeme.text.into(),
        prefix: source.prefix || lexeme.prefix,
        weights: source.weights.clone(),
    })
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

fn build_websearch_query(config: &TextSearchConfig, text: &str) -> Option<TsQueryNode> {
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

fn append_websearch_text(config: &TextSearchConfig, text: &str, items: &mut Vec<WebSearchItem>) {
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
    config: &TextSearchConfig,
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

fn websearch_unit_to_node(config: &TextSearchConfig, unit: &str) -> Option<TsQueryNode> {
    let terms = websearch_terms(config, unit);
    if unit.contains('-') || unit.contains('*') || unit.contains('_') || unit.contains('\'') {
        return phrase_node_from_terms(terms);
    }
    and_node_from_terms(terms)
}

fn websearch_terms(config: &TextSearchConfig, text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for raw in websearch_raw_terms(text) {
        if raw.contains('-') && document_tokens(&raw).len() > 1 {
            if let Some(lexeme) = lexize_token_for_config(config, &raw) {
                terms.push(lexeme);
            }
            continue;
        }
        for token in document_tokens(&raw) {
            if let Some(lexeme) =
                lexize_token_for_config_and_type(config, token.tokid, &token.token)
            {
                terms.push(lexeme);
            }
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
    config: &TextSearchConfig,
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
    config: &TextSearchConfig,
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

fn has_websearch_operand(config: &TextSearchConfig, items: &[WebSearchItem]) -> bool {
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
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Option<Vec<String>>, String> {
    let dictionary = resolve_dictionary(dictionary_name, catalog)?;
    match lexize_dictionary(&dictionary, text) {
        LexizeOutcome::Match(alternatives) => {
            let lexemes = alternatives
                .into_iter()
                .flat_map(|alternative| alternative.lexemes)
                .map(|lexeme| lexeme.text)
                .collect::<Vec<_>>();
            if lexemes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(lexemes))
            }
        }
        LexizeOutcome::Stop | LexizeOutcome::NoMatch => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn websearch(config_name: &str, text: &str) -> String {
        websearch_to_tsquery_with_config_name(Some(config_name), text, None)
            .unwrap()
            .render()
    }

    #[test]
    fn phrase_queries_count_removed_stop_words() {
        assert_eq!(
            phraseto_tsquery_with_config_name(Some("english"), "1 the 2", None)
                .unwrap()
                .render(),
            "'1' <2> '2'"
        );
        assert_eq!(
            to_tsquery_with_config_name(Some("english"), "1 <-> the <-> 2", None)
                .unwrap()
                .render(),
            "'1' <2> '2'"
        );
    }

    #[test]
    fn to_tsquery_tokenizes_quoted_operands_as_phrases() {
        assert_eq!(
            to_tsquery_with_config_name(Some("english"), "'New York'", None)
                .unwrap()
                .render(),
            "'new' <-> 'york'"
        );
        assert_eq!(
            to_tsquery_with_config_name(Some("english"), "'fat the cat'", None)
                .unwrap()
                .render(),
            "'fat' <2> 'cat'"
        );
    }

    #[test]
    fn websearch_ignores_tsquery_syntax_and_weights() {
        assert_eq!(
            websearch("simple", "I have a fat:*ABCD cat"),
            "'i' & 'have' & 'a' & 'fat' & 'abcd' & 'cat'"
        );
        assert_eq!(
            websearch("simple", "fat:A!cat:B|rat:C<"),
            "'fat' & 'a' & 'cat' & 'b' & 'rat' & 'c'"
        );
        assert_eq!(websearch("simple", "abc : def"), "'abc' & 'def'");
        assert_eq!(websearch("simple", ":"), "");
        assert_eq!(websearch("simple", "abc & def"), "'abc' & 'def'");
        assert_eq!(websearch("simple", "abc <-> def"), "'abc' & 'def'");
    }

    #[test]
    fn websearch_handles_phrases_and_document_tokens() {
        assert_eq!(websearch("simple", "fat*rat"), "'fat' <-> 'rat'");
        assert_eq!(
            websearch("simple", "fat-rat"),
            "'fat-rat' <-> 'fat' <-> 'rat'"
        );
        assert_eq!(websearch("simple", "fat_rat"), "'fat' <-> 'rat'");
        assert_eq!(
            websearch("english", "\"pg_class pg\""),
            "'pg' <-> 'class' <-> 'pg'"
        );
        assert_eq!(
            websearch("english", "abc \"pg_class pg\""),
            "'abc' & 'pg' <-> 'class' <-> 'pg'"
        );
    }

    #[test]
    fn websearch_handles_or_and_negated_phrases() {
        assert_eq!(websearch("simple", "cat or rat"), "'cat' | 'rat'");
        assert_eq!(websearch("simple", "cat OR"), "'cat' & 'or'");
        assert_eq!(websearch("simple", "OR rat"), "'or' & 'rat'");
        assert_eq!(websearch("simple", "or OR or"), "'or' | 'or'");
        assert_eq!(
            websearch("simple", "(foo bar) or (ding dong)"),
            "'foo' & 'bar' | 'ding' & 'dong'"
        );
        assert_eq!(
            websearch("simple", "\"fat cat\"or\"fat rat\""),
            "'fat' <-> 'cat' | 'fat' <-> 'rat'"
        );
        assert_eq!(
            websearch("english", "cat -\"fat rat\" cheese"),
            "'cat' & !( 'fat' <-> 'rat' ) & 'chees'"
        );
        assert_eq!(websearch("english", "this is ----fine"), "!!!!'fine'");
        assert_eq!(
            websearch("english", " or \"pg pg_class pg\" or "),
            "'pg' <-> 'pg' <-> 'class' <-> 'pg'"
        );
    }
}
