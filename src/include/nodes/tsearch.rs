use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::pgrust::compact_string::CompactString;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TsWeight {
    D,
    C,
    B,
    A,
}

impl TsWeight {
    pub fn from_char(ch: char) -> Option<Self> {
        match ch.to_ascii_uppercase() {
            'A' => Some(Self::A),
            'B' => Some(Self::B),
            'C' => Some(Self::C),
            'D' => Some(Self::D),
            _ => None,
        }
    }

    pub fn as_char(self) -> char {
        match self {
            Self::A => 'A',
            Self::B => 'B',
            Self::C => 'C',
            Self::D => 'D',
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TsPosition {
    pub position: u16,
    pub weight: Option<TsWeight>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsLexeme {
    pub text: CompactString,
    pub positions: Vec<TsPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct TsVector {
    pub lexemes: Vec<TsLexeme>,
}

impl TsVector {
    pub fn new(mut lexemes: Vec<TsLexeme>) -> Self {
        let mut by_lexeme = BTreeMap::<String, BTreeSet<TsPosition>>::new();
        for lexeme in lexemes.drain(..) {
            by_lexeme
                .entry(lexeme.text.as_str().to_string())
                .or_default()
                .extend(lexeme.positions);
        }
        Self {
            lexemes: by_lexeme
                .into_iter()
                .map(|(text, positions)| TsLexeme {
                    text: text.into(),
                    positions: positions.into_iter().collect(),
                })
                .collect(),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let mut chars = text.chars().peekable();
        let mut lexemes = Vec::new();
        loop {
            while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
                chars.next();
            }
            if chars.peek().is_none() {
                break;
            }
            let lexeme = parse_tsvector_lexeme(&mut chars)?;
            let mut positions = Vec::new();
            if matches!(chars.peek(), Some(':')) {
                chars.next();
                loop {
                    let mut digits = String::new();
                    while matches!(chars.peek(), Some(ch) if ch.is_ascii_digit()) {
                        digits.push(chars.next().unwrap());
                    }
                    if digits.is_empty() {
                        return Err("expected tsvector position".into());
                    }
                    let position = digits
                        .parse::<u16>()
                        .map_err(|_| "invalid tsvector position".to_string())?;
                    let weight = match chars.peek().copied() {
                        Some(ch) if TsWeight::from_char(ch).is_some() => {
                            let ch = chars.next().unwrap();
                            TsWeight::from_char(ch)
                        }
                        _ => None,
                    };
                    positions.push(TsPosition { position, weight });
                    if !matches!(chars.peek(), Some(',')) {
                        break;
                    }
                    chars.next();
                }
            }
            lexemes.push(TsLexeme {
                text: lexeme.into(),
                positions,
            });
        }
        Ok(Self::new(lexemes))
    }

    pub fn render(&self) -> String {
        self.lexemes
            .iter()
            .map(|lexeme| {
                let mut out = quote_ts_text(lexeme.text.as_str());
                if !lexeme.positions.is_empty() {
                    out.push(':');
                    for (index, position) in lexeme.positions.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        out.push_str(&position.position.to_string());
                        if let Some(weight) = position.weight {
                            out.push(weight.as_char());
                        }
                    }
                }
                out
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn contains_term(&self, operand: &TsQueryOperand) -> bool {
        self.positions_for_operand(operand).next().is_some()
    }

    pub fn positions_for_operand<'a>(
        &'a self,
        operand: &'a TsQueryOperand,
    ) -> impl Iterator<Item = u16> + 'a {
        self.lexemes
            .iter()
            .filter(move |lexeme| {
                if operand.prefix {
                    lexeme.text.as_str().starts_with(operand.lexeme.as_str())
                } else {
                    lexeme.text == operand.lexeme
                }
            })
            .flat_map(move |lexeme| {
                lexeme.positions.iter().filter_map(move |position| {
                    if operand.weights.is_empty()
                        || operand
                            .weights
                            .iter()
                            .any(|weight| Some(*weight) == position.weight)
                    {
                        Some(position.position)
                    } else {
                        None
                    }
                })
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsQueryOperand {
    pub lexeme: CompactString,
    pub prefix: bool,
    pub weights: Vec<TsWeight>,
}

impl TsQueryOperand {
    pub fn new(lexeme: impl Into<CompactString>) -> Self {
        Self {
            lexeme: lexeme.into(),
            prefix: false,
            weights: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TsQueryNode {
    Operand(TsQueryOperand),
    And(Box<TsQueryNode>, Box<TsQueryNode>),
    Or(Box<TsQueryNode>, Box<TsQueryNode>),
    Not(Box<TsQueryNode>),
    Phrase {
        left: Box<TsQueryNode>,
        right: Box<TsQueryNode>,
        distance: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsQuery {
    pub root: TsQueryNode,
}

impl TsQuery {
    pub fn new(root: TsQueryNode) -> Self {
        Self { root }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let tokens = TsQueryTokenizer::new(text).tokenize()?;
        let mut parser = TsQueryParser::new(tokens);
        let root = parser.parse_expr()?;
        if !matches!(parser.peek(), TsQueryToken::End) {
            return Err("unexpected trailing tsquery token".into());
        }
        Ok(Self { root })
    }

    pub fn render(&self) -> String {
        render_tsquery_node(&self.root, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchParserToken {
    pub tokid: i32,
    pub alias: CompactString,
    pub description: CompactString,
}

fn parse_tsvector_lexeme<I>(chars: &mut std::iter::Peekable<I>) -> Result<String, String>
where
    I: Iterator<Item = char>,
{
    match chars.peek().copied() {
        Some('\'') => parse_quoted_ts_text(chars),
        Some(_) => {
            let mut out = String::new();
            while let Some(ch) = chars.peek().copied() {
                if ch.is_whitespace() || ch == ':' {
                    break;
                }
                out.push(ch);
                chars.next();
            }
            if out.is_empty() {
                Err("expected tsvector lexeme".into())
            } else {
                Ok(out)
            }
        }
        None => Err("unexpected end of tsvector input".into()),
    }
}

fn parse_quoted_ts_text<I>(chars: &mut std::iter::Peekable<I>) -> Result<String, String>
where
    I: Iterator<Item = char>,
{
    let Some('\'') = chars.next() else {
        return Err("expected quoted text".into());
    };
    let mut out = String::new();
    loop {
        match chars.next() {
            Some('\'') if matches!(chars.peek(), Some('\'')) => {
                chars.next();
                out.push('\'');
            }
            Some('\'') => return Ok(out),
            Some(ch) => out.push(ch),
            None => return Err("unterminated quoted text".into()),
        }
    }
}

fn parse_quoted_ts_text_with_consumed(input: &str) -> Result<(String, usize), String> {
    let bytes = input.as_bytes();
    if bytes.first().copied() != Some(b'\'') {
        return Err("expected quoted text".into());
    }
    let mut out = String::new();
    let mut index = 1usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' if bytes.get(index + 1).copied() == Some(b'\'') => {
                out.push('\'');
                index += 2;
            }
            b'\'' => return Ok((out, index + 1)),
            _ => {
                let ch = input[index..]
                    .chars()
                    .next()
                    .ok_or_else(|| "unterminated quoted text".to_string())?;
                out.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    Err("unterminated quoted text".into())
}

fn quote_ts_text(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TsQueryToken {
    Operand(TsQueryOperand),
    And,
    Or,
    Not,
    Phrase(u16),
    LParen,
    RParen,
    End,
}

struct TsQueryTokenizer<'a> {
    input: &'a str,
    index: usize,
}

impl<'a> TsQueryTokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, index: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<TsQueryToken>, String> {
        let mut tokens = Vec::new();
        loop {
            self.skip_ws();
            let rest = &self.input[self.index..];
            if rest.is_empty() {
                break;
            }
            if rest.starts_with("<->") {
                self.index += 3;
                tokens.push(TsQueryToken::Phrase(1));
                continue;
            }
            if rest.starts_with('<') {
                if let Some(end) = rest.find('>') {
                    let digits = &rest[1..end];
                    if !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()) {
                        self.index += end + 1;
                        let distance = digits
                            .parse::<u16>()
                            .map_err(|_| "invalid phrase distance".to_string())?;
                        tokens.push(TsQueryToken::Phrase(distance));
                        continue;
                    }
                }
            }
            match rest.chars().next().unwrap() {
                '&' => {
                    self.index += 1;
                    tokens.push(TsQueryToken::And);
                }
                '|' => {
                    self.index += 1;
                    tokens.push(TsQueryToken::Or);
                }
                '!' => {
                    self.index += 1;
                    tokens.push(TsQueryToken::Not);
                }
                '(' => {
                    self.index += 1;
                    tokens.push(TsQueryToken::LParen);
                }
                ')' => {
                    self.index += 1;
                    tokens.push(TsQueryToken::RParen);
                }
                '\'' => tokens.push(TsQueryToken::Operand(self.parse_operand_quoted()?)),
                _ => tokens.push(TsQueryToken::Operand(self.parse_operand_bare()?)),
            }
        }
        tokens.push(TsQueryToken::End);
        Ok(tokens)
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.input[self.index..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            self.index += ch.len_utf8();
        }
    }

    fn parse_operand_quoted(&mut self) -> Result<TsQueryOperand, String> {
        let (lexeme, consumed) = parse_quoted_ts_text_with_consumed(&self.input[self.index..])?;
        self.index += consumed;
        self.finish_operand(lexeme)
    }

    fn parse_operand_bare(&mut self) -> Result<TsQueryOperand, String> {
        let mut lexeme = String::new();
        while let Some(ch) = self.input[self.index..].chars().next() {
            if ch.is_whitespace() || matches!(ch, '&' | '|' | '!' | '(' | ')' | '<') {
                break;
            }
            if ch == ':' {
                break;
            }
            lexeme.push(ch);
            self.index += ch.len_utf8();
        }
        if lexeme.is_empty() {
            return Err("expected tsquery operand".into());
        }
        self.finish_operand(lexeme)
    }

    fn finish_operand(&mut self, lexeme: String) -> Result<TsQueryOperand, String> {
        let mut operand = TsQueryOperand::new(lexeme);
        if self.input[self.index..].starts_with(':') {
            self.index += 1;
            while let Some(ch) = self.input[self.index..].chars().next() {
                if ch == '*' {
                    operand.prefix = true;
                    self.index += 1;
                    continue;
                }
                if let Some(weight) = TsWeight::from_char(ch) {
                    if !operand.weights.contains(&weight) {
                        operand.weights.push(weight);
                    }
                    self.index += 1;
                    continue;
                }
                break;
            }
            operand.weights.sort_by(|left, right| match (left, right) {
                (TsWeight::A, TsWeight::A)
                | (TsWeight::B, TsWeight::B)
                | (TsWeight::C, TsWeight::C)
                | (TsWeight::D, TsWeight::D) => Ordering::Equal,
                (TsWeight::A, _) => Ordering::Less,
                (_, TsWeight::A) => Ordering::Greater,
                (TsWeight::B, _) => Ordering::Less,
                (_, TsWeight::B) => Ordering::Greater,
                (TsWeight::C, _) => Ordering::Less,
                (_, TsWeight::C) => Ordering::Greater,
            });
        }
        Ok(operand)
    }
}

struct TsQueryParser {
    tokens: Vec<TsQueryToken>,
    index: usize,
}

impl TsQueryParser {
    fn new(tokens: Vec<TsQueryToken>) -> Self {
        Self { tokens, index: 0 }
    }

    fn peek(&self) -> &TsQueryToken {
        &self.tokens[self.index]
    }

    fn next(&mut self) -> &TsQueryToken {
        let token = &self.tokens[self.index];
        self.index += 1;
        token
    }

    fn parse_expr(&mut self) -> Result<TsQueryNode, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<TsQueryNode, String> {
        let mut node = self.parse_and()?;
        while matches!(self.peek(), TsQueryToken::Or) {
            self.next();
            let right = self.parse_and()?;
            node = TsQueryNode::Or(Box::new(node), Box::new(right));
        }
        Ok(node)
    }

    fn parse_and(&mut self) -> Result<TsQueryNode, String> {
        let mut node = self.parse_phrase()?;
        while matches!(self.peek(), TsQueryToken::And) {
            self.next();
            let right = self.parse_phrase()?;
            node = TsQueryNode::And(Box::new(node), Box::new(right));
        }
        Ok(node)
    }

    fn parse_phrase(&mut self) -> Result<TsQueryNode, String> {
        let mut node = self.parse_unary()?;
        while let TsQueryToken::Phrase(distance) = self.peek() {
            let distance = *distance;
            self.next();
            let right = self.parse_unary()?;
            node = TsQueryNode::Phrase {
                left: Box::new(node),
                right: Box::new(right),
                distance,
            };
        }
        Ok(node)
    }

    fn parse_unary(&mut self) -> Result<TsQueryNode, String> {
        if matches!(self.peek(), TsQueryToken::Not) {
            self.next();
            return Ok(TsQueryNode::Not(Box::new(self.parse_unary()?)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<TsQueryNode, String> {
        match self.next().clone() {
            TsQueryToken::Operand(operand) => Ok(TsQueryNode::Operand(operand)),
            TsQueryToken::LParen => {
                let node = self.parse_expr()?;
                match self.next() {
                    TsQueryToken::RParen => Ok(node),
                    _ => Err("expected ')' in tsquery".into()),
                }
            }
            _ => Err("expected tsquery operand".into()),
        }
    }
}

fn render_tsquery_node(node: &TsQueryNode, parent_precedence: u8) -> String {
    let (rendered, precedence) = match node {
        TsQueryNode::Operand(operand) => (render_tsquery_operand(operand), 4),
        TsQueryNode::Not(inner) => (format!("!{}", render_tsquery_node(inner, 4)), 4),
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => (
            format!(
                "{} {} {}",
                render_tsquery_node(left, 3),
                if *distance == 1 {
                    "<->".to_string()
                } else {
                    format!("<{}>", distance)
                },
                render_tsquery_node(right, 3)
            ),
            3,
        ),
        TsQueryNode::And(left, right) => (
            format!(
                "{} & {}",
                render_tsquery_node(left, 2),
                render_tsquery_node(right, 2)
            ),
            2,
        ),
        TsQueryNode::Or(left, right) => (
            format!(
                "{} | {}",
                render_tsquery_node(left, 1),
                render_tsquery_node(right, 1)
            ),
            1,
        ),
    };
    if precedence < parent_precedence {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn render_tsquery_operand(operand: &TsQueryOperand) -> String {
    let mut out = quote_ts_text(operand.lexeme.as_str());
    if operand.prefix || !operand.weights.is_empty() {
        out.push(':');
        for weight in &operand.weights {
            out.push(weight.as_char());
        }
        if operand.prefix {
            out.push('*');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tsvector_round_trip() {
        let vector = TsVector::parse("'bar baz':2A foo:1,3").unwrap();
        assert_eq!(vector.render(), "'bar baz':2A 'foo':1,3");
    }

    #[test]
    fn tsquery_round_trip() {
        let query = TsQuery::parse("foo & !bar <2> baz:*A").unwrap();
        assert_eq!(query.render(), "foo & !bar <2> baz:A*");
    }
}
