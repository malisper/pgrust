use super::super::ExecError;
use crate::compat::backend::parser::CatalogLookup;
use crate::compat::backend::tsearch::cache::{TextSearchConfig, resolve_config};
use crate::compat::backend::tsearch::lexize_token_for_config_and_type;
use crate::compat::backend::tsearch::parser::{
    ASCIIHWORD, DECIMAL_T, HWORD, NUMHWORD, SCIENTIFIC, SIGNEDINT, TAG_T, UNSIGNEDINT, URL_T,
    VERSIONNUMBER, parse_default, token_has_dictionary,
};
use crate::compat::include::nodes::tsearch::{TsQuery, TsQueryNode, TsQueryOperand};

#[derive(Debug, Clone)]
struct HeadlineOptions {
    min_words: usize,
    max_words: usize,
    short_word: usize,
    max_fragments: usize,
    start_sel: String,
    stop_sel: String,
    fragment_delimiter: String,
    highlight_all: bool,
}

#[derive(Debug, Clone)]
struct HeadlinePiece {
    text: String,
    tokid: i32,
    highlight: bool,
    word_index: Option<usize>,
}

#[derive(Debug, Clone)]
struct HeadlineWord {
    piece_index: usize,
    tokid: i32,
    normalized: Option<String>,
    text_len: usize,
    position: u16,
    item: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cover {
    start_word: usize,
    end_word: usize,
    start_pos: u16,
    end_pos: u16,
}

impl Default for HeadlineOptions {
    fn default() -> Self {
        Self {
            min_words: 15,
            max_words: 35,
            short_word: 3,
            max_fragments: 0,
            start_sel: "<b>".into(),
            stop_sel: "</b>".into(),
            fragment_delimiter: " ... ".into(),
            highlight_all: false,
        }
    }
}

pub fn ts_headline(
    config_name: Option<&str>,
    document: &str,
    query: &TsQuery,
    options: Option<&str>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<String, ExecError> {
    if crate::compat::backend::executor::render_tsquery_text(query).is_empty() {
        return Ok(document.into());
    }
    let options = parse_headline_options(options)?;
    let config = resolve_config(config_name, catalog)
        .map_err(|message| text_search_error("ts_headline", message))?;
    let operands = positive_query_operands(&query.root);
    if operands.is_empty() {
        return Ok(document.into());
    }
    let (mut pieces, words) = parse_headline_document(document, &config, &operands);
    if words.is_empty() {
        return Ok(document.into());
    }
    let covers = query_covers(&query.root, &words);

    let ranges = if options.highlight_all {
        vec![(0, words.len().saturating_sub(1))]
    } else if options.max_fragments > 0 {
        fragment_ranges(&words, &covers, &options)
    } else {
        vec![best_headline_range(&words, &covers, &options)]
    };
    let preserve_leading_space =
        !options.highlight_all && options.max_fragments > 0 && covers.is_empty();

    for word in &words {
        if word.item {
            pieces[word.piece_index].highlight = true;
        }
    }

    let rendered = if options.highlight_all {
        vec![render_piece_range(
            &pieces,
            0,
            pieces.len().saturating_sub(1),
            &options,
        )]
    } else {
        ranges
            .into_iter()
            .filter_map(|(start, end)| {
                let text = normalized_headline_text(
                    &pieces,
                    &words,
                    start,
                    end,
                    &options,
                    preserve_leading_space,
                );
                (!text.is_empty()).then_some(text)
            })
            .filter(|text: &String| !text.is_empty())
            .collect::<Vec<_>>()
    };
    if rendered.is_empty() {
        return Ok(render_prefix(&pieces, &words, &options));
    }
    Ok(rendered.join(&options.fragment_delimiter))
}

fn text_search_error(op: &'static str, message: String) -> ExecError {
    ExecError::Parse(
        crate::compat::backend::parser::ParseError::UnexpectedToken {
            expected: "valid text search input",
            actual: format!("{op}: {message}"),
        },
    )
}

fn parse_headline_options(options: Option<&str>) -> Result<HeadlineOptions, ExecError> {
    let mut parsed = HeadlineOptions::default();
    let Some(options) = options else {
        return Ok(parsed);
    };
    for raw_part in options.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((name, value)) = part.split_once('=') else {
            return Err(invalid_headline_parameter(part));
        };
        let name = name.trim();
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "maxwords" => parsed.max_words = parse_usize_option(name, value)?,
            "minwords" => parsed.min_words = parse_usize_option(name, value)?,
            "shortword" => parsed.short_word = parse_usize_option(name, value)?,
            "maxfragments" => parsed.max_fragments = parse_usize_option(name, value)?,
            "startsel" => parsed.start_sel = value.into(),
            "stopsel" => parsed.stop_sel = value.into(),
            "fragmentdelimiter" => parsed.fragment_delimiter = value.into(),
            "highlightall" => parsed.highlight_all = parse_bool_option(value),
            _ => return Err(invalid_headline_parameter(name)),
        }
    }
    if !parsed.highlight_all {
        if parsed.min_words >= parsed.max_words {
            return Err(headline_option_error("MinWords must be less than MaxWords"));
        }
        if parsed.min_words == 0 {
            return Err(headline_option_error("MinWords must be positive"));
        }
    }
    Ok(parsed)
}

fn invalid_headline_parameter(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unrecognized headline parameter: \"{name}\""),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn headline_option_error(message: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn parse_usize_option(name: &str, value: &str) -> Result<usize, ExecError> {
    value
        .parse::<usize>()
        .map_err(|_| ExecError::DetailedError {
            message: format!("invalid value for headline parameter \"{name}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        })
}

fn parse_bool_option(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "on" | "true" | "t" | "y" | "yes"
    )
}

fn positive_query_operands(node: &TsQueryNode) -> Vec<TsQueryOperand> {
    fn walk(node: &TsQueryNode, negated: bool, out: &mut Vec<TsQueryOperand>) {
        match node {
            TsQueryNode::Operand(operand) if !negated && !operand.lexeme.as_str().is_empty() => {
                if !out.contains(operand) {
                    out.push(operand.clone());
                }
            }
            TsQueryNode::Operand(_) => {}
            TsQueryNode::Not(inner) => walk(inner, true, out),
            TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
                walk(left, negated, out);
                walk(right, negated, out);
            }
            TsQueryNode::Phrase { left, right, .. } => {
                walk(left, negated, out);
                walk(right, negated, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(node, false, &mut out);
    out
}

fn parse_headline_document(
    document: &str,
    config: &TextSearchConfig,
    operands: &[TsQueryOperand],
) -> (Vec<HeadlinePiece>, Vec<HeadlineWord>) {
    let mut pieces = Vec::new();
    let mut words = Vec::new();
    let mut position = 1u16;
    for token in parse_default(document) {
        push_headline_piece(
            &mut pieces,
            &mut words,
            token.token,
            token.tokid,
            &mut position,
            config,
            operands,
        );
    }
    (pieces, words)
}

fn push_headline_piece(
    pieces: &mut Vec<HeadlinePiece>,
    words: &mut Vec<HeadlineWord>,
    text: String,
    tokid: i32,
    position: &mut u16,
    config: &TextSearchConfig,
    operands: &[TsQueryOperand],
) {
    let piece_index = pieces.len();
    pieces.push(HeadlinePiece {
        text: text.clone(),
        tokid,
        highlight: false,
        word_index: None,
    });
    if token_has_dictionary(tokid) {
        let normalized = lexize_token_for_config_and_type(config, tokid, &text);
        let item = word_matches_operands(normalized.as_deref(), operands);
        let word_index = words.len();
        pieces[piece_index].word_index = Some(word_index);
        words.push(HeadlineWord {
            piece_index,
            tokid,
            normalized,
            text_len: text.chars().count(),
            position: *position,
            item,
        });
        *position = position.saturating_add(1);
    }
}

fn word_matches_operands(normalized: Option<&str>, operands: &[TsQueryOperand]) -> bool {
    let Some(normalized) = normalized else {
        return false;
    };
    operands.iter().any(|operand| {
        if operand.prefix {
            normalized.starts_with(operand.lexeme.as_str())
        } else {
            normalized == operand.lexeme.as_str()
        }
    })
}

fn query_covers(node: &TsQueryNode, words: &[HeadlineWord]) -> Vec<Cover> {
    let mut covers = query_covers_inner(node, words);
    covers.sort_by_key(|cover| {
        (
            cover.start_pos,
            cover.end_pos,
            cover.start_word,
            cover.end_word,
        )
    });
    covers.dedup();
    covers
}

fn query_covers_inner(node: &TsQueryNode, words: &[HeadlineWord]) -> Vec<Cover> {
    match node {
        TsQueryNode::Operand(operand) if !operand.lexeme.as_str().is_empty() => words
            .iter()
            .enumerate()
            .filter_map(|(index, word)| {
                word_matches_operands(word.normalized.as_deref(), std::slice::from_ref(operand))
                    .then_some(Cover {
                        start_word: index,
                        end_word: index,
                        start_pos: word.position,
                        end_pos: word.position,
                    })
            })
            .collect(),
        TsQueryNode::Operand(_) | TsQueryNode::Not(_) => Vec::new(),
        TsQueryNode::Or(left, right) => {
            let mut covers = query_covers_inner(left, words);
            covers.extend(query_covers_inner(right, words));
            covers
        }
        TsQueryNode::And(left, right) => merge_cover_sets(
            query_covers_inner(left, words),
            query_covers_inner(right, words),
            None,
        ),
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => merge_cover_sets(
            query_covers_inner(left, words),
            query_covers_inner(right, words),
            Some(*distance),
        ),
    }
}

fn merge_cover_sets(left: Vec<Cover>, right: Vec<Cover>, distance: Option<u16>) -> Vec<Cover> {
    if left.is_empty() || right.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for left_cover in &left {
        for right_cover in &right {
            if let Some(distance) = distance
                && left_cover.end_pos.saturating_add(distance) != right_cover.start_pos
            {
                continue;
            }
            out.push(Cover {
                start_word: left_cover.start_word.min(right_cover.start_word),
                end_word: left_cover.end_word.max(right_cover.end_word),
                start_pos: left_cover.start_pos.min(right_cover.start_pos),
                end_pos: left_cover.end_pos.max(right_cover.end_pos),
            });
        }
    }
    out
}

fn best_headline_range(
    words: &[HeadlineWord],
    covers: &[Cover],
    options: &HeadlineOptions,
) -> (usize, usize) {
    let mut best = None::<(usize, usize, usize, bool, u16, u16)>;
    for cover in covers {
        let (start, end) = ordinary_range_for_cover(words, *cover, options);
        let poslen = count_interesting_words(words, start, end);
        let includes_cover = start <= cover.start_word && end >= cover.end_word;
        let candidate = (
            start,
            end,
            poslen,
            includes_cover,
            cover.start_pos,
            cover.end_pos,
        );
        if best.is_none_or(|best| headline_candidate_better(words, candidate, best, options)) {
            best = Some(candidate);
        }
    }
    best.map(|(start, end, _, _, _, _)| (start, end))
        .unwrap_or_else(|| prefix_range(words, options.min_words))
}

fn fragment_ranges(
    words: &[HeadlineWord],
    covers: &[Cover],
    options: &HeadlineOptions,
) -> Vec<(usize, usize)> {
    if covers.is_empty() {
        return vec![prefix_range(words, options.min_words)];
    }

    let mut candidates = Vec::new();
    for cover in covers {
        if cover.end_word.saturating_sub(cover.start_word) + 1 <= options.max_words {
            candidates.push(fragment_range_for_seed(
                words,
                cover.start_word,
                cover.end_word,
                options,
            ));
        } else {
            for index in cover.start_word..=cover.end_word {
                if words[index].item {
                    candidates.push(fragment_range_for_seed(words, index, index, options));
                }
            }
        }
    }

    candidates.sort_by_key(|(start, end)| {
        (
            std::cmp::Reverse(count_interesting_words(words, *start, *end)),
            *start,
            end.saturating_sub(*start),
        )
    });
    let mut selected = Vec::<(usize, usize)>::new();
    for candidate in candidates {
        if selected
            .iter()
            .any(|(start, end)| ranges_overlap(candidate, (*start, *end)))
        {
            continue;
        }
        selected.push(candidate);
        if selected.len() >= options.max_fragments {
            break;
        }
    }
    selected.sort_by_key(|(start, end)| (*start, *end));
    selected
}

fn ordinary_range_for_cover(
    words: &[HeadlineWord],
    cover: Cover,
    options: &HeadlineOptions,
) -> (usize, usize) {
    let mut start = cover.start_word;
    let mut end = cover.end_word;
    let mut len = end.saturating_sub(start) + 1;
    if len > options.max_words {
        end = start
            .saturating_add(options.max_words.saturating_sub(1))
            .min(words.len().saturating_sub(1));
        len = end.saturating_sub(start) + 1;
    }
    if len >= options.min_words && !bad_endpoint(words, end, options.short_word) {
        return (start, end);
    }
    if len < options.max_words {
        while end + 1 < words.len() && len < options.max_words {
            end += 1;
            len += 1;
            if len >= options.min_words && !bad_endpoint(words, end, options.short_word) {
                break;
            }
        }
        if len < options.min_words {
            while start > 0 && len < options.max_words {
                start -= 1;
                len += 1;
                if len >= options.min_words && !bad_endpoint(words, start, options.short_word) {
                    break;
                }
            }
        }
    } else {
        while len > options.min_words && bad_endpoint(words, end, options.short_word) {
            end -= 1;
            len -= 1;
        }
    }
    (start, end)
}

fn headline_candidate_better(
    words: &[HeadlineWord],
    candidate: (usize, usize, usize, bool, u16, u16),
    best: (usize, usize, usize, bool, u16, u16),
    options: &HeadlineOptions,
) -> bool {
    let (
        _,
        candidate_end,
        candidate_poslen,
        candidate_covers,
        candidate_start_pos,
        candidate_end_pos,
    ) = candidate;
    let (_, best_end, best_poslen, best_covers, best_start_pos, best_end_pos) = best;
    candidate_covers > best_covers
        || (candidate_covers == best_covers && candidate_end_pos < best_end_pos)
        || (candidate_covers == best_covers
            && candidate_end_pos == best_end_pos
            && candidate_start_pos > best_start_pos)
        || (candidate_covers == best_covers
            && candidate_end_pos == best_end_pos
            && candidate_start_pos == best_start_pos
            && candidate_poslen > best_poslen)
        || (candidate_covers == best_covers
            && candidate_end_pos == best_end_pos
            && candidate_start_pos == best_start_pos
            && candidate_poslen == best_poslen
            && !bad_endpoint(words, candidate_end, options.short_word)
            && bad_endpoint(words, best_end, options.short_word))
}

fn fragment_range_for_seed(
    words: &[HeadlineWord],
    seed_start: usize,
    seed_end: usize,
    options: &HeadlineOptions,
) -> (usize, usize) {
    let mut start = seed_start;
    let mut end = seed_end;
    let mut len = end.saturating_sub(start) + 1;
    let mut left_budget = options.max_words.saturating_sub(len) / 2;
    let near_document_end = seed_end.saturating_add(left_budget) >= words.len().saturating_sub(1);
    if near_document_end {
        left_budget = left_budget.min(6);
    }
    let mut stretched_left = 0usize;
    while start > 0 && stretched_left < left_budget {
        start -= 1;
        len += 1;
        stretched_left += 1;
    }
    while start < seed_start && bad_endpoint(words, start, options.short_word) {
        start += 1;
        len = len.saturating_sub(1);
    }
    if !near_document_end {
        while end + 1 < words.len() && len < options.max_words {
            end += 1;
            len += 1;
        }
    }
    while end > seed_end && bad_endpoint(words, end, options.short_word) {
        end -= 1;
    }
    (start, end)
}

fn prefix_range(words: &[HeadlineWord], min_words: usize) -> (usize, usize) {
    if words.is_empty() {
        return (0, 0);
    }
    let end = min_words
        .saturating_sub(1)
        .min(words.len().saturating_sub(1));
    (0, end)
}

fn count_interesting_words(words: &[HeadlineWord], start: usize, end: usize) -> usize {
    words[start..=end].iter().filter(|word| word.item).count()
}

fn ranges_overlap(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 <= right.1 && right.0 <= left.1
}

fn bad_endpoint(words: &[HeadlineWord], index: usize, short_word: usize) -> bool {
    let word = &words[index];
    (matches!(
        word.tokid,
        SCIENTIFIC | VERSIONNUMBER | DECIMAL_T | SIGNEDINT | UNSIGNEDINT
    ) || word.text_len <= short_word)
        && !word.item
}

fn render_word_range(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    start_word: usize,
    end_word: usize,
    options: &HeadlineOptions,
) -> String {
    let start_piece = words[start_word].piece_index;
    let mut end_piece = words[end_word].piece_index;
    while end_piece + 1 < pieces.len() && pieces[end_piece + 1].word_index.is_none() {
        end_piece += 1;
    }
    render_piece_range(pieces, start_piece, end_piece, options)
}

fn normalized_headline_text(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    start_word: usize,
    end_word: usize,
    options: &HeadlineOptions,
    preserve_leading_space: bool,
) -> String {
    let raw = if preserve_leading_space && start_word == 0 {
        render_piece_range(
            pieces,
            0,
            end_piece_for_word_range(pieces, words, end_word),
            options,
        )
    } else {
        render_word_range(pieces, words, start_word, end_word, options)
    };
    let mut text = if preserve_leading_space {
        raw.trim_end_matches(|ch: char| ch.is_whitespace())
            .to_string()
    } else {
        raw.trim_matches(|ch: char| ch.is_whitespace()).to_string()
    };
    if should_trim_trailing_punctuation(pieces, words, start_word, end_word) {
        text = text
            .trim_end_matches(|ch: char| matches!(ch, ',' | '.' | ';' | '('))
            .trim_end_matches(|ch: char| ch.is_whitespace())
            .to_string();
    }
    text
}

fn should_trim_trailing_punctuation(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    start_word: usize,
    end_word: usize,
) -> bool {
    !word_range_reaches_document_end(pieces, words, end_word) || start_word == 0
}

fn word_range_reaches_document_end(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    end_word: usize,
) -> bool {
    let end_piece = end_piece_for_word_range(pieces, words, end_word);
    end_piece + 1 >= pieces.len()
}

fn end_piece_for_word_range(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    end_word: usize,
) -> usize {
    let mut end_piece = words[end_word].piece_index;
    while end_piece + 1 < pieces.len() && pieces[end_piece + 1].word_index.is_none() {
        end_piece += 1;
    }
    end_piece
}

fn render_piece_range(
    pieces: &[HeadlinePiece],
    start_piece: usize,
    end_piece: usize,
    options: &HeadlineOptions,
) -> String {
    let mut out = String::new();
    for piece in &pieces[start_piece..=end_piece] {
        if should_skip_piece(piece, options.highlight_all) {
            continue;
        }
        if should_replace_piece(piece, options.highlight_all) {
            out.push(' ');
            continue;
        }
        if piece.highlight {
            out.push_str(&options.start_sel);
            out.push_str(&piece.text);
            out.push_str(&options.stop_sel);
        } else {
            out.push_str(&piece.text);
        }
    }
    out
}

fn should_replace_piece(piece: &HeadlinePiece, highlight_all: bool) -> bool {
    !highlight_all && piece.tokid == TAG_T
}

fn should_skip_piece(piece: &HeadlinePiece, highlight_all: bool) -> bool {
    if highlight_all {
        matches!(piece.tokid, URL_T | NUMHWORD | ASCIIHWORD | HWORD)
    } else {
        matches!(piece.tokid, URL_T | NUMHWORD | ASCIIHWORD | HWORD)
    }
}

fn render_prefix(
    pieces: &[HeadlinePiece],
    words: &[HeadlineWord],
    options: &HeadlineOptions,
) -> String {
    if words.is_empty() {
        return String::new();
    }
    let (start, end) = prefix_range(words, options.min_words);
    render_word_range(pieces, words, start, end, options)
}
