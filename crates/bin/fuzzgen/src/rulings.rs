//! The rulings ledger (plan §4.2): `docs/fuzzing/rulings.toml` loaded
//! through `contracts::toml`, first-match semantics, per-ruling hit
//! counting, and the `rulings audit` verdict (stale = hard fail,
//! expired-but-firing = warn + renewal queue).
//!
//! How a row matches a plane divergence (the contract `diff.rs` relies on;
//! the ledger header repeats it for the people editing rows):
//!
//! 1. `plane` must equal the divergence's plane — one plane per ruling.
//! 2. `stmt`, when present, is a case-insensitive regex searched over the
//!    whitespace-normalized step SQL (`^COPY\b`, `\bpg_settings\b`).
//! 3. When `a` and/or `b` are present, `field` names the plane field
//!    whose canonical value on each side must match its regex (an absent
//!    value matches as the empty string, so `a = "^$"` means "A had no
//!    error"). Which fields actually differed is not consulted: the
//!    regexes describe the tuple, and the ruling covers the whole plane
//!    divergence.
//! 4. Without regexes but with a `field`, the ruling covers a divergence
//!    whose differing-field set is exactly `{field}` — a delta ruling.
//!    Structural masks the comparator implements (`ulp`, `soft`,
//!    `oid-literal`, `toast-name`, `binary-udt-oid`, `cmp-sign`,
//!    `counter`, `timing`, `planning-buffers`, `materialize`) surface as a
//!    differing set of exactly that mask name, so they are ruled by rows
//!    of this shape and never by accident.
//! 5. With neither regexes nor `field`, the ruling covers every
//!    divergence on that plane that passes the `stmt` scope.
//!
//! Rows are tried in file order; the first match wins. A hit never hides
//! the divergence: `diff.rs` still records it with `ruled:<id>`, and the
//! ledger counts the hit for `audit`.
//!
//! Expiry policy (critic gap 10): only cosmetic / HINT-plane rulings
//! carry `expires`, staggered by creation week:
//! `created + 45 d + (ISO week of created mod 4) × 7 d`. Everything else is
//! permanent until deleted. `audit` hard-fails only on STALE rows (zero
//! hits over the last `AUDIT_RUNS` = 5 CI cluster runs: a dead mask hides
//! regressions) and warns with a renewal queue on rows that are past
//! `expires` but still firing, so a nightly can never redden on a
//! calendar alone.
//!
//! The regex engine is the small backtracking matcher in `re` below:
//! fuzzgen carries no regex crate (CONTRACTS.md "Serialization choice"),
//! and the ledger needs negative lookahead (`(?!XX000:)`), which the
//! crates.io `regex` crate does not offer anyway.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::contracts::{Ruling, Rulings};

/// CI runs a ruling must be silent over before `audit` calls it stale.
pub const AUDIT_RUNS: usize = 5;

/// Days a cosmetic ruling lives before the stagger is added.
pub const EXPIRY_BASE_DAYS: i64 = 45;

// ---------------------------------------------------------------------
// re: the regex subset the ledger uses
// ---------------------------------------------------------------------

pub mod re {
    //! Backtracking regex over chars. Supported: literals, `.`, `[...]`
    //! classes with ranges and `^` negation, escapes `\d \D \w \W \s \S
    //! \b \n \t` and `\<punct>`, groups `(...)`, `(?:...)`, lookahead
    //! `(?=...)` / `(?!...)`, alternation `|`, quantifiers `* + ? {n}
    //! {n,} {n,m}` (a trailing `?` is accepted and treated as greedy —
    //! `is_match` does not care), anchors `^` / `$`, and a leading `(?i)`
    //! for case-insensitive matching. Anything else is a compile error
    //! naming the offset, so a typo in the ledger fails `Ledger::parse`
    //! instead of silently never matching.

    #[derive(Clone, Debug, PartialEq)]
    enum Node {
        Char(char),
        Any,
        Class { neg: bool, items: Vec<ClassItem> },
        Start,
        End,
        WordBoundary,
        Group(Alt),
        Look { neg: bool, alt: Alt },
        Repeat { node: Box<Node>, min: u32, max: Option<u32> },
    }

    #[derive(Clone, Debug, PartialEq)]
    enum ClassItem {
        Range(char, char),
        Digit(bool),
        Word(bool),
        Space(bool),
    }

    type Seq = Vec<Node>;
    type Alt = Vec<Seq>;

    #[derive(Clone, Debug, PartialEq)]
    pub struct Regex {
        alt: Alt,
        icase: bool,
        source: String,
    }

    struct Parser<'a> {
        chars: Vec<char>,
        pos: usize,
        src: &'a str,
    }

    impl<'a> Parser<'a> {
        fn err<T>(&self, what: &str) -> Result<T, String> {
            Err(format!("regex {:?}: {} at offset {}", self.src, what, self.pos))
        }
        fn peek(&self) -> Option<char> {
            self.chars.get(self.pos).copied()
        }
        fn bump(&mut self) -> Option<char> {
            let c = self.peek();
            if c.is_some() {
                self.pos += 1;
            }
            c
        }
        fn eat(&mut self, c: char) -> bool {
            if self.peek() == Some(c) {
                self.pos += 1;
                true
            } else {
                false
            }
        }

        fn parse_alt(&mut self) -> Result<Alt, String> {
            let mut alt = vec![self.parse_seq()?];
            while self.eat('|') {
                alt.push(self.parse_seq()?);
            }
            Ok(alt)
        }

        fn parse_seq(&mut self) -> Result<Seq, String> {
            let mut seq = Vec::new();
            while let Some(c) = self.peek() {
                if c == '|' || c == ')' {
                    break;
                }
                let atom = self.parse_atom()?;
                let atom = self.parse_quant(atom)?;
                seq.push(atom);
            }
            Ok(seq)
        }

        fn parse_quant(&mut self, atom: Node) -> Result<Node, String> {
            let (min, max) = match self.peek() {
                Some('*') => {
                    self.pos += 1;
                    (0, None)
                }
                Some('+') => {
                    self.pos += 1;
                    (1, None)
                }
                Some('?') => {
                    self.pos += 1;
                    (0, Some(1))
                }
                Some('{') => {
                    let save = self.pos;
                    self.pos += 1;
                    let n = self.parse_int();
                    match (n, self.peek()) {
                        (Some(n), Some('}')) => {
                            self.pos += 1;
                            (n, Some(n))
                        }
                        (Some(n), Some(',')) => {
                            self.pos += 1;
                            let m = self.parse_int();
                            if !self.eat('}') {
                                return self.err("unterminated {n,m}");
                            }
                            if let Some(m) = m {
                                if m < n {
                                    return self.err("{n,m} with m < n");
                                }
                            }
                            (n, m)
                        }
                        _ => {
                            // A bare '{' that is not a quantifier is a literal.
                            self.pos = save;
                            return Ok(atom);
                        }
                    }
                }
                _ => return Ok(atom),
            };
            if matches!(atom, Node::Start | Node::End | Node::WordBoundary | Node::Look { .. }) {
                return self.err("quantifier on an anchor or lookahead");
            }
            // Lazy marker: accepted, greedy semantics (match/no-match is identical).
            self.eat('?');
            Ok(Node::Repeat { node: Box::new(atom), min, max })
        }

        fn parse_int(&mut self) -> Option<u32> {
            let start = self.pos;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
            if start == self.pos {
                return None;
            }
            self.chars[start..self.pos].iter().collect::<String>().parse().ok()
        }

        fn parse_atom(&mut self) -> Result<Node, String> {
            let c = match self.bump() {
                Some(c) => c,
                None => return self.err("unexpected end"),
            };
            match c {
                '(' => {
                    let node = if self.eat('?') {
                        match self.bump() {
                            Some(':') => Node::Group(self.parse_alt()?),
                            Some('=') => Node::Look { neg: false, alt: self.parse_alt()? },
                            Some('!') => Node::Look { neg: true, alt: self.parse_alt()? },
                            _ => return self.err("unsupported (? construct"),
                        }
                    } else {
                        Node::Group(self.parse_alt()?)
                    };
                    if !self.eat(')') {
                        return self.err("unterminated group");
                    }
                    Ok(node)
                }
                '[' => self.parse_class(),
                '.' => Ok(Node::Any),
                '^' => Ok(Node::Start),
                '$' => Ok(Node::End),
                '\\' => self.parse_escape(false),
                '*' | '+' | '?' => self.err("quantifier without operand"),
                c => Ok(Node::Char(c)),
            }
        }

        fn parse_escape(&mut self, in_class: bool) -> Result<Node, String> {
            let c = match self.bump() {
                Some(c) => c,
                None => return self.err("trailing backslash"),
            };
            Ok(match c {
                'd' => Node::Class { neg: false, items: vec![ClassItem::Digit(true)] },
                'D' => Node::Class { neg: false, items: vec![ClassItem::Digit(false)] },
                'w' => Node::Class { neg: false, items: vec![ClassItem::Word(true)] },
                'W' => Node::Class { neg: false, items: vec![ClassItem::Word(false)] },
                's' => Node::Class { neg: false, items: vec![ClassItem::Space(true)] },
                'S' => Node::Class { neg: false, items: vec![ClassItem::Space(false)] },
                'b' if !in_class => Node::WordBoundary,
                'n' => Node::Char('\n'),
                't' => Node::Char('\t'),
                'r' => Node::Char('\r'),
                c if c.is_ascii_punctuation() || c == ' ' => Node::Char(c),
                _ => return self.err("unsupported escape"),
            })
        }

        fn parse_class(&mut self) -> Result<Node, String> {
            let neg = self.eat('^');
            let mut items = Vec::new();
            let mut first = true;
            loop {
                let c = match self.bump() {
                    Some(c) => c,
                    None => return self.err("unterminated class"),
                };
                if c == ']' && !first {
                    break;
                }
                first = false;
                let lo = if c == '\\' {
                    match self.parse_escape(true)? {
                        Node::Char(ch) => ch,
                        Node::Class { items: sub, .. } => {
                            items.extend(sub);
                            continue;
                        }
                        _ => return self.err("bad class escape"),
                    }
                } else {
                    c
                };
                if self.peek() == Some('-') && self.chars.get(self.pos + 1).is_some_and(|&n| n != ']') {
                    self.pos += 1;
                    let hi = match self.bump() {
                        Some('\\') => match self.parse_escape(true)? {
                            Node::Char(ch) => ch,
                            _ => return self.err("bad range end"),
                        },
                        Some(ch) => ch,
                        None => return self.err("unterminated class"),
                    };
                    if hi < lo {
                        return self.err("reversed range");
                    }
                    items.push(ClassItem::Range(lo, hi));
                } else {
                    items.push(ClassItem::Range(lo, lo));
                }
            }
            Ok(Node::Class { neg, items })
        }
    }

    fn is_word(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    fn fold(c: char, icase: bool) -> char {
        if icase {
            c.to_lowercase().next().unwrap_or(c)
        } else {
            c
        }
    }

    fn class_hit(items: &[ClassItem], c: char, icase: bool) -> bool {
        items.iter().any(|it| match *it {
            ClassItem::Range(lo, hi) => {
                (lo..=hi).contains(&c)
                    || (icase && {
                        let f = fold(c, true);
                        let u = c.to_uppercase().next().unwrap_or(c);
                        (fold(lo, true)..=fold(hi, true)).contains(&f) || (lo..=hi).contains(&u)
                    })
            }
            ClassItem::Digit(want) => c.is_ascii_digit() == want,
            ClassItem::Word(want) => is_word(c) == want,
            ClassItem::Space(want) => c.is_whitespace() == want,
        })
    }

    struct Ctx<'t> {
        text: &'t [char],
        icase: bool,
    }

    /// Match `seq[i..]` at `pos`, then the continuation `k` on the end position.
    fn m_seq(ctx: &Ctx, seq: &[Node], i: usize, pos: usize, k: &mut dyn FnMut(usize) -> bool) -> bool {
        if i == seq.len() {
            return k(pos);
        }
        match &seq[i] {
            Node::Repeat { node, min, max } => m_repeat(ctx, node, *min, *max, 0, seq, i, pos, k),
            node => m_node(ctx, node, pos, &mut |p| m_seq(ctx, seq, i + 1, p, k)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn m_repeat(
        ctx: &Ctx,
        node: &Node,
        min: u32,
        max: Option<u32>,
        count: u32,
        seq: &[Node],
        i: usize,
        pos: usize,
        k: &mut dyn FnMut(usize) -> bool,
    ) -> bool {
        // Greedy: try one more repetition first, then fall back to
        // continuing the sequence. Zero-width iterations stop the loop.
        if max.is_none_or(|m| count < m) {
            let more = m_node(ctx, node, pos, &mut |p| {
                if p == pos {
                    return false;
                }
                m_repeat(ctx, node, min, max, count + 1, seq, i, p, k)
            });
            if more {
                return true;
            }
        }
        if count >= min {
            return m_seq(ctx, seq, i + 1, pos, k);
        }
        // Below the minimum and the node can only match empty: allow it.
        if m_node(ctx, node, pos, &mut |p| p == pos) {
            return m_seq(ctx, seq, i + 1, pos, k);
        }
        false
    }

    fn m_alt(ctx: &Ctx, alt: &Alt, pos: usize, k: &mut dyn FnMut(usize) -> bool) -> bool {
        alt.iter().any(|seq| m_seq(ctx, seq, 0, pos, k))
    }

    fn m_node(ctx: &Ctx, node: &Node, pos: usize, k: &mut dyn FnMut(usize) -> bool) -> bool {
        let t = ctx.text;
        match node {
            Node::Char(c) => {
                t.get(pos).is_some_and(|&x| fold(x, ctx.icase) == fold(*c, ctx.icase)) && k(pos + 1)
            }
            Node::Any => t.get(pos).is_some_and(|&x| x != '\n') && k(pos + 1),
            Node::Class { neg, items } => {
                t.get(pos).is_some_and(|&x| class_hit(items, x, ctx.icase) != *neg) && k(pos + 1)
            }
            Node::Start => pos == 0 && k(pos),
            Node::End => pos == t.len() && k(pos),
            Node::WordBoundary => {
                let before = pos > 0 && is_word(t[pos - 1]);
                let after = pos < t.len() && is_word(t[pos]);
                before != after && k(pos)
            }
            Node::Group(alt) => m_alt(ctx, alt, pos, k),
            Node::Look { neg, alt } => {
                let hit = m_alt(ctx, alt, pos, &mut |_| true);
                hit != *neg && k(pos)
            }
            Node::Repeat { .. } => unreachable!("Repeat is handled by m_seq"),
        }
    }

    impl Regex {
        pub fn new(pattern: &str) -> Result<Regex, String> {
            let (icase, body) = match pattern.strip_prefix("(?i)") {
                Some(rest) => (true, rest),
                None => (false, pattern),
            };
            let mut p = Parser { chars: body.chars().collect(), pos: 0, src: pattern };
            let alt = p.parse_alt()?;
            if p.pos != p.chars.len() {
                return p.err("unbalanced ')'");
            }
            Ok(Regex { alt, icase, source: pattern.to_string() })
        }

        pub fn as_str(&self) -> &str {
            &self.source
        }

        /// Unanchored search: true when the pattern matches anywhere.
        pub fn is_match(&self, text: &str) -> bool {
            let chars: Vec<char> = text.chars().collect();
            let ctx = Ctx { text: &chars, icase: self.icase };
            (0..=chars.len()).any(|start| m_alt(&ctx, &self.alt, start, &mut |_| true))
        }
    }
}

use re::Regex;

// ---------------------------------------------------------------------
// Dates: YYYY-MM-DD arithmetic without a clock or a crate
// ---------------------------------------------------------------------

/// Days since 1970-01-01 for a proleptic Gregorian civil date
/// (Howard Hinnant's days_from_civil).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Inverse of `days_from_civil`.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parse `YYYY-MM-DD` to days since the epoch.
pub fn parse_date(s: &str) -> Result<i64, String> {
    let bad = || format!("bad date {:?} (want YYYY-MM-DD)", s);
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(bad());
    }
    let y: i64 = s[0..4].parse().map_err(|_| bad())?;
    let m: u32 = s[5..7].parse().map_err(|_| bad())?;
    let d: u32 = s[8..10].parse().map_err(|_| bad())?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    let days = days_from_civil(y, m, d);
    if civil_from_days(days) != (y, m, d) {
        return Err(bad());
    }
    Ok(days)
}

pub fn format_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// ISO-8601 week number (1..=53) of a date given as days since the epoch.
pub fn iso_week(days: i64) -> u32 {
    // 1970-01-01 was a Thursday; weekday: Mon=1 .. Sun=7.
    let weekday = ((days + 3).rem_euclid(7) + 1) as u32;
    // The Thursday of this week decides the ISO year.
    let thursday = days + 4 - weekday as i64;
    let (ty, _, _) = civil_from_days(thursday);
    let jan1 = days_from_civil(ty, 1, 1);
    ((thursday - jan1) / 7 + 1) as u32
}

/// The expiry date the policy assigns to a cosmetic ruling created on
/// `created`: `created + 45 d + (ISO week mod 4) × 7 d`.
pub fn expiry_for(created: &str) -> Result<String, String> {
    let days = parse_date(created)?;
    let stagger = (iso_week(days) % 4) as i64 * 7;
    Ok(format_date(days + EXPIRY_BASE_DAYS + stagger))
}

// ---------------------------------------------------------------------
// The ledger
// ---------------------------------------------------------------------

/// The live ledger text, compiled in so every consumer (the legacy
/// `diff::classify` adapter included) sees the same rows without a
/// filesystem dependency. `Ledger::load` reads a file instead.
pub const EMBEDDED_LEDGER: &str = include_str!("../../../../docs/fuzzing/rulings.toml");

/// Fields on the E/N planes whose rulings are cosmetic and therefore
/// expire: a pgrust-only HINT, the routine name.
const COSMETIC_ERR_FIELDS: &[&str] = &["H", "R"];
/// Log-plane fields whose rulings are prefix formatting, cosmetic.
const COSMETIC_LOG_FIELDS: &[&str] = &["zone", "backend_type", "app", "prefix"];

/// True for rulings the expiry policy applies to.
pub fn is_cosmetic_ruling(r: &Ruling) -> bool {
    match (r.plane.as_str(), r.field.as_deref()) {
        ("wire:E" | "wire:N", Some(f)) => COSMETIC_ERR_FIELDS.contains(&f),
        ("log", Some(f)) => COSMETIC_LOG_FIELDS.contains(&f),
        _ => false,
    }
}

struct Compiled {
    a: Option<Regex>,
    b: Option<Regex>,
    stmt: Option<Regex>,
}

/// One plane divergence offered to the ledger.
pub struct Candidate<'a> {
    /// `wire:E`, `wire:N`, `rows`, `explain`, `meta`, `tag`, `notify`, `log`, ...
    pub plane: &'a str,
    /// Differing fields in first-differing order (a structural mask name
    /// when the comparator's mask made the plane equal).
    pub differing: &'a [String],
    /// The step SQL (`""` for non-SQL steps).
    pub stmt: &'a str,
    /// Canonical value of a named field on side A / side B (None = absent).
    pub value_a: &'a dyn Fn(&str) -> Option<String>,
    pub value_b: &'a dyn Fn(&str) -> Option<String>,
}

pub struct Ledger {
    pub rulings: Rulings,
    compiled: Vec<Compiled>,
    hits: Mutex<BTreeMap<String, u64>>,
}

impl Clone for Ledger {
    fn clone(&self) -> Ledger {
        let mut l = Ledger::from_rulings(self.rulings.clone()).expect("a compiled ledger re-compiles");
        l.hits = Mutex::new(self.hits());
        l
    }
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ledger({} rulings, hits {:?})", self.rulings.rulings.len(), self.hits())
    }
}

/// Collapse whitespace runs to one space and trim: the text `stmt`
/// regexes run over.
pub fn normalize_stmt(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut space = true;
    for c in sql.chars() {
        if c.is_whitespace() {
            if !space {
                out.push(' ');
            }
            space = true;
        } else {
            out.push(c);
            space = false;
        }
    }
    out.trim_end().to_string()
}

impl Ledger {
    pub fn empty() -> Ledger {
        Ledger { rulings: Rulings::default(), compiled: Vec::new(), hits: Mutex::new(BTreeMap::new()) }
    }

    /// Parse ledger text (TOML subset per `contracts::toml`), compile every
    /// regex, and check the expiry policy.
    pub fn parse(text: &str) -> Result<Ledger, String> {
        Ledger::from_rulings(Rulings::parse(text)?)
    }

    pub fn from_rulings(rulings: Rulings) -> Result<Ledger, String> {
        let mut compiled = Vec::with_capacity(rulings.rulings.len());
        for r in &rulings.rulings {
            let rx = |s: &Option<String>, what: &str| -> Result<Option<Regex>, String> {
                match s {
                    None => Ok(None),
                    Some(p) => Regex::new(p).map(Some).map_err(|e| format!("ruling {:?} {}: {}", r.id, what, e)),
                }
            };
            let stmt = match &r.stmt {
                None => None,
                Some(p) => Some(
                    Regex::new(&format!("(?i){}", p)).map_err(|e| format!("ruling {:?} stmt: {}", r.id, e))?,
                ),
            };
            if (r.a.is_some() || r.b.is_some()) && r.field.is_none() {
                return Err(format!("ruling {:?}: a/b regexes need a field to apply to", r.id));
            }
            if r.expires.is_some() != is_cosmetic_ruling(r) {
                return Err(format!(
                    "ruling {:?}: `expires` is required exactly for cosmetic/HINT-plane rulings (plane {} field {:?})",
                    r.id, r.plane, r.field
                ));
            }
            if let Some(e) = &r.expires {
                if parse_date(e)? < parse_date(&r.created)? {
                    return Err(format!("ruling {:?}: expires before created", r.id));
                }
            }
            compiled.push(Compiled { a: rx(&r.a, "a")?, b: rx(&r.b, "b")?, stmt });
        }
        let hits = rulings.rulings.iter().map(|r| (r.id.clone(), 0)).collect();
        Ok(Ledger { rulings, compiled, hits: Mutex::new(hits) })
    }

    pub fn load(path: &std::path::Path) -> Result<Ledger, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
        Ledger::parse(&text)
    }

    /// The compiled-in live ledger.
    pub fn embedded() -> Ledger {
        Ledger::parse(EMBEDDED_LEDGER).expect("docs/fuzzing/rulings.toml parses (pinned by test)")
    }

    pub fn get(&self, id: &str) -> Option<&Ruling> {
        self.rulings.get(id)
    }

    pub fn len(&self) -> usize {
        self.rulings.rulings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rulings.rulings.is_empty()
    }

    fn matches(&self, idx: usize, c: &Candidate) -> bool {
        let r = &self.rulings.rulings[idx];
        let cx = &self.compiled[idx];
        if r.plane != c.plane {
            return false;
        }
        if let Some(stmt) = &cx.stmt {
            if !stmt.is_match(&normalize_stmt(c.stmt)) {
                return false;
            }
        }
        match (&r.field, cx.a.is_some() || cx.b.is_some()) {
            (Some(field), true) => {
                let va = (c.value_a)(field).unwrap_or_default();
                let vb = (c.value_b)(field).unwrap_or_default();
                cx.a.as_ref().is_none_or(|rx| rx.is_match(&va)) && cx.b.as_ref().is_none_or(|rx| rx.is_match(&vb))
            }
            (Some(field), false) => c.differing.len() == 1 && &c.differing[0] == field,
            (None, _) => true,
        }
    }

    /// First matching ruling, in file order; no hit is recorded.
    pub fn find(&self, c: &Candidate) -> Option<&Ruling> {
        (0..self.rulings.rulings.len()).find(|&i| self.matches(i, c)).map(|i| &self.rulings.rulings[i])
    }

    /// `find` plus a hit on the matched row; returns the id.
    pub fn resolve(&self, c: &Candidate) -> Option<String> {
        let id = self.find(c)?.id.clone();
        self.hit(&id);
        Some(id)
    }

    pub fn hit(&self, id: &str) {
        *self.hits.lock().unwrap().entry(id.to_string()).or_insert(0) += 1;
    }

    /// Hits recorded on this ledger since it was built (per id, every id present).
    pub fn hits(&self) -> BTreeMap<String, u64> {
        self.hits.lock().unwrap().clone()
    }

    pub fn reset_hits(&self) {
        for v in self.hits.lock().unwrap().values_mut() {
            *v = 0;
        }
    }

    /// Fold this run's hits into the rows' `hits` counters (what `audit`
    /// writes back) and return the ledger text to store.
    pub fn render_with_hits(&self) -> String {
        let hits = self.hits();
        let mut rulings = self.rulings.clone();
        for r in &mut rulings.rulings {
            r.hits += hits.get(&r.id).copied().unwrap_or(0);
        }
        render(&rulings)
    }

    /// The `rulings audit` verdict. `hits_over_runs` is one map (id ->
    /// hits) per CI cluster run, most recent last; `today` is `YYYY-MM-DD`.
    pub fn audit(&self, hits_over_runs: &[BTreeMap<String, u64>], today: &str) -> Result<AuditReport, String> {
        let today_days = parse_date(today)?;
        let recent = &hits_over_runs[hits_over_runs.len().saturating_sub(AUDIT_RUNS)..];
        let mut report = AuditReport { runs: recent.len(), ..AuditReport::default() };
        for r in &self.rulings.rulings {
            let created = parse_date(&r.created)?;
            if created > today_days {
                return Err(format!("ruling {:?}: created {} is after today {}", r.id, r.created, today));
            }
            let total: u64 = recent.iter().map(|m| m.get(&r.id).copied().unwrap_or(0)).sum();
            let expired = match &r.expires {
                Some(e) => parse_date(e)? < today_days,
                None => false,
            };
            if recent.len() >= AUDIT_RUNS && total == 0 {
                report.stale.push(r.id.clone());
            }
            if expired && total > 0 {
                report.expired_firing.push(Renewal {
                    id: r.id.clone(),
                    expires: r.expires.clone().unwrap_or_default(),
                    hits: total,
                    proposed: expiry_for(today)?,
                });
            } else if expired {
                // Expired and silent: the stale verdict (when K runs are
                // in) already covers it; otherwise it just waits.
                report.expired_silent.push(r.id.clone());
            }
        }
        Ok(report)
    }
}

/// Render the ledger with the policy header (the on-disk form; the
/// `contracts::Rulings::render` header is the fixture's shorter one).
pub fn render(rulings: &Rulings) -> String {
    let mut out = String::from(LEDGER_HEADER);
    for r in &rulings.rulings {
        out.push('\n');
        out.push_str(&r.render());
    }
    out
}

/// A ruling that is past `expires` but still firing: renew (owner
/// re-affirms) or delete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Renewal {
    pub id: String,
    pub expires: String,
    pub hits: u64,
    /// `expiry_for(today)`: the date a renewal today would carry.
    pub proposed: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AuditReport {
    /// Runs the verdict looked at (≤ AUDIT_RUNS).
    pub runs: usize,
    /// Zero hits over AUDIT_RUNS runs: hard fail (dead masks hide regressions).
    pub stale: Vec<String>,
    /// Past `expires` and still firing: warning + renewal queue.
    pub expired_firing: Vec<Renewal>,
    /// Past `expires` and silent in the window (informational).
    pub expired_silent: Vec<String>,
}

impl AuditReport {
    pub fn hard_fail(&self) -> bool {
        !self.stale.is_empty()
    }

    pub fn render(&self) -> String {
        let mut out = format!("rulings audit over {} run(s)\n", self.runs);
        for id in &self.stale {
            out.push_str(&format!("FAIL stale: {} (0 hits over {} runs)\n", id, self.runs));
        }
        for r in &self.expired_firing {
            out.push_str(&format!(
                "WARN expired-but-firing: {} (expired {}, {} hits) -> renew to {} or delete\n",
                r.id, r.expires, r.hits, r.proposed
            ));
        }
        for id in &self.expired_silent {
            out.push_str(&format!("note expired, silent in window: {}\n", id));
        }
        if !self.hard_fail() && self.expired_firing.is_empty() {
            out.push_str("OK\n");
        }
        out
    }
}

/// The comment block at the top of `docs/fuzzing/rulings.toml`.
pub const LEDGER_HEADER: &str = "\
# sitediff rulings ledger (plan §4.2). One [[ruling]] per row; first match
# wins; a ruling covers exactly one plane; every ruled hit is still recorded
# as ruled:<id>. Only cosmetic/HINT rulings carry `expires`. `hits` is
# maintained by `sitediff rulings audit`. Schema: crates/bin/fuzzgen/schemas/rulings.schema.json
#
# Policy (owner-serial; crates/bin/fuzzgen/src/rulings.rs is the reader):
#   plane   one of wire:E wire:N rows explain meta tag notify log copy
#           probe:<deck>; a ruling never covers a second plane.
#   stmt    case-insensitive regex searched over the whitespace-normalized
#           step SQL (anchor with ^ for a statement kind).
#   field   with a/b regexes: the plane field the regexes test on each side
#           (absent value = \"\"; wire:E/wire:N offer the raw fields plus
#           CM = \"<sqlstate>: <message>\"); without regexes: the divergence's
#           differing-field set must be exactly {field}. Structural masks the
#           comparator implements are ruled by that shape: rows ulp | soft |
#           oid-literal | toast-name | binary-udt-oid | cmp-sign, explain
#           counter | timing | planning-buffers | materialize, rows order |
#           count | cell.
#   expires only cosmetic rulings (wire:E/wire:N field H or R; log prefix
#           fields) carry it, as created + 45 d + (ISO week of created mod 4)
#           x 7 d; the reader rejects any other row carrying one, and a
#           cosmetic row without one.
#   audit   hard-fails on stale rows (0 hits over the last 5 CI cluster runs) and
#           warns with a renewal queue on rows past `expires` that still fire.
#   order   rows are tried top to bottom; put the narrower scope first
#           (copy-order before tie-ordering).
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_subset_matches() {
        let ok = |p: &str, t: &str| Regex::new(p).unwrap().is_match(t);
        assert!(ok("^unsupported XML feature$", "unsupported XML feature"));
        assert!(!ok("^unsupported XML feature$", "unsupported XML features"));
        assert!(ok("^-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?$", "-1.5e+10"));
        assert!(!ok("^-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?$", "1.5.5"));
        assert!(ok("^(?!XX000:)", "0A000: x"));
        assert!(!ok("^(?!XX000:)", "XX000: x"));
        assert!(ok("^(?!XX000:)", ""));
        assert!(ok("^$", ""));
        assert!(!ok("^$", "a"));
        assert!(ok("(?i)^copy\\b", "COPY t TO STDOUT"));
        assert!(!ok("(?i)^copy\\b", "COPYX t"));
        assert!(ok("pg_toast_\\d{5,}", "pg_toast.pg_toast_48594"));
        assert!(!ok("pg_toast_\\d{5,}", "pg_toast_2619"));
        assert!(ok("^[0-9A-F]+/[0-9A-F]+$", "0/16000028"));
        assert!(ok("a|b", "xbx"));
        assert!(ok("^(a|bc)+d$", "abcabcd"));
        assert!(ok("^x*$", ""));
        assert!(ok("^[^a-c]$", "d"));
        assert!(!ok("^[^a-c]$", "b"));
        assert!(ok("^\\w+\\s\\S$", "ab_1 x"));
        assert!(ok("tuple concurrently (updated|deleted)$", "tuple concurrently deleted"));
        assert!(ok("^VACUUM ?(\\([^)]*\\bFULL\\b|FULL\\b)", "VACUUM (ANALYZE, FULL) t"));
        assert!(ok("^a{2}b{1,}c{0,1}$", "aabbb"));
        assert!(ok("x{", "x{"));
        assert!(Regex::new("(").is_err());
        assert!(Regex::new("*a").is_err());
        assert!(Regex::new("[a").is_err());
        assert!(Regex::new("\\q").is_err());
    }

    #[test]
    fn dates_and_expiry_policy() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(format_date(parse_date("2026-09-02").unwrap()), "2026-09-02");
        assert!(parse_date("2026-02-30").is_err());
        assert!(parse_date("2026-9-2").is_err());
        // 2026-09-02 is a Wednesday in ISO week 36; 36 mod 4 = 0.
        assert_eq!(iso_week(parse_date("2026-09-02").unwrap()), 36);
        assert_eq!(expiry_for("2026-09-02").unwrap(), "2026-10-17");
        // 2026-09-09: week 37 -> +7.
        assert_eq!(expiry_for("2026-09-09").unwrap(), "2026-10-31");
        // 2026-01-01 (Thursday) is ISO week 1 -> +7.
        assert_eq!(iso_week(parse_date("2026-01-01").unwrap()), 1);
        assert_eq!(expiry_for("2026-01-01").unwrap(), "2026-02-22");
        // 2027-01-01 is a Friday: ISO week 53 of 2026 -> 53 mod 4 = 1.
        assert_eq!(iso_week(parse_date("2027-01-01").unwrap()), 53);
    }

    fn ledger(rows: &str) -> Ledger {
        Ledger::parse(rows).unwrap()
    }

    const ROW_REGEX: &str = "[[ruling]]\nid = \"x\"\nplane = \"wire:E\"\nfield = \"CM\"\na = \"^0A000: unsupported XML feature$\"\nb = \"^(?!XX000:)\"\nref = \"r\"\nowner = \"o\"\ncreated = \"2026-09-02\"\nhits = 0\n";
    const ROW_DELTA: &str = "[[ruling]]\nid = \"r-field\"\nplane = \"wire:E\"\nfield = \"R\"\nref = \"r\"\nowner = \"o\"\ncreated = \"2026-09-02\"\nexpires = \"2026-10-17\"\nhits = 0\n";
    const ROW_PLANE: &str = "[[ruling]]\nid = \"inst\"\nplane = \"rows\"\nstmt = \"\\\\bpg_locks\\\\b\"\nref = \"r\"\nowner = \"o\"\ncreated = \"2026-09-02\"\nhits = 0\n";

    fn cand<'a>(
        plane: &'a str,
        differing: &'a [String],
        stmt: &'a str,
        va: &'a dyn Fn(&str) -> Option<String>,
        vb: &'a dyn Fn(&str) -> Option<String>,
    ) -> Candidate<'a> {
        Candidate { plane, differing, stmt, value_a: va, value_b: vb }
    }

    #[test]
    fn regex_rulings_test_the_named_field_on_both_sides() {
        let l = ledger(ROW_REGEX);
        let d = vec!["C".to_string(), "M".to_string()];
        let va = |f: &str| (f == "CM").then(|| "0A000: unsupported XML feature".to_string());
        let vb = |f: &str| (f == "CM").then(|| "22023: something else".to_string());
        assert_eq!(l.resolve(&cand("wire:E", &d, "SELECT xml", &va, &vb)).as_deref(), Some("x"));
        // B panicked: the b regex refuses.
        let vb2 = |f: &str| (f == "CM").then(|| "XX000: boom".to_string());
        assert!(l.find(&cand("wire:E", &d, "SELECT xml", &va, &vb2)).is_none());
        // B absent (succeeded): "" matches the negative lookahead.
        let vb3 = |_: &str| None;
        assert!(l.find(&cand("wire:E", &d, "SELECT xml", &va, &vb3)).is_some());
        // Wrong plane never matches.
        assert!(l.find(&cand("wire:N", &d, "SELECT xml", &va, &vb)).is_none());
        assert_eq!(l.hits().get("x"), Some(&1));
    }

    #[test]
    fn delta_rulings_need_exactly_that_field() {
        let l = ledger(ROW_DELTA);
        let none = |_: &str| None;
        let only_r = vec!["R".to_string()];
        assert!(l.find(&cand("wire:E", &only_r, "SELECT 1", &none, &none)).is_some());
        // R present on A only (first LIVE smoke: A R="jsonb_delete", B no R
        // on `cannot delete from scalar`): the differing set is still {R}.
        let va = |f: &str| (f == "R").then(|| "jsonb_delete".to_string());
        assert!(l.find(&cand("wire:E", &only_r, "SELECT '\"\"'::jsonb - 'b'", &va, &none)).is_some());
        let r_and_p = vec!["R".to_string(), "P".to_string()];
        assert!(l.find(&cand("wire:E", &r_and_p, "SELECT 1", &none, &none)).is_none());
        let m = vec!["M".to_string()];
        assert!(l.find(&cand("wire:E", &m, "SELECT 1", &none, &none)).is_none());
    }

    #[test]
    fn whole_plane_rulings_scope_by_stmt_only() {
        let l = ledger(ROW_PLANE);
        let none = |_: &str| None;
        let d = vec!["count".to_string()];
        assert!(l.find(&cand("rows", &d, "select count(*)\n  from PG_LOCKS where not granted", &none, &none)).is_some());
        assert!(l.find(&cand("rows", &d, "select * from t", &none, &none)).is_none());
        assert!(l.find(&cand("meta", &d, "select * from pg_locks", &none, &none)).is_none());
    }

    #[test]
    fn first_match_wins_in_file_order() {
        let two = format!(
            "{}{}",
            ROW_PLANE.replace("id = \"inst\"", "id = \"first\""),
            ROW_PLANE.replace("id = \"inst\"", "id = \"second\"")
        );
        let l = ledger(&two);
        let none = |_: &str| None;
        let d = vec!["cell".to_string()];
        assert_eq!(l.find(&cand("rows", &d, "select * from pg_locks", &none, &none)).unwrap().id, "first");
    }

    #[test]
    fn expiry_policy_is_enforced_at_parse() {
        // A non-cosmetic row with expires is refused.
        let bad = ROW_PLANE.replace("hits = 0", "expires = \"2027-01-01\"\nhits = 0");
        assert!(Ledger::parse(&bad).unwrap_err().contains("expires"));
        // A cosmetic row without expires is refused.
        let bad = ROW_DELTA.replace("expires = \"2026-10-17\"\n", "");
        assert!(Ledger::parse(&bad).unwrap_err().contains("expires"));
        // Regexes need a field.
        let bad = ROW_REGEX.replace("field = \"CM\"\n", "");
        assert!(Ledger::parse(&bad).unwrap_err().contains("field"));
        // A bad regex is loud.
        let bad = ROW_REGEX.replace("^(?!XX000:)", "(");
        assert!(Ledger::parse(&bad).unwrap_err().contains("regex"));
    }

    #[test]
    fn audit_stale_and_expired_with_fixed_dates() {
        let l = ledger(&format!("{}{}{}", ROW_REGEX, ROW_DELTA, ROW_PLANE));
        let run = |x: u64, r: u64, i: u64| {
            BTreeMap::from([("x".to_string(), x), ("r-field".to_string(), r), ("inst".to_string(), i)])
        };
        // Four runs only: nothing can be stale yet.
        let four = vec![run(0, 0, 0); 4];
        let rep = l.audit(&four, "2026-10-01").unwrap();
        assert!(rep.stale.is_empty());
        assert!(!rep.hard_fail());
        assert_eq!(rep.runs, 4);
        // Five runs, `inst` never fires: stale = hard fail; x fires.
        let five = vec![run(1, 1, 0); 5];
        let rep = l.audit(&five, "2026-10-01").unwrap();
        assert_eq!(rep.stale, vec!["inst".to_string()]);
        assert!(rep.hard_fail());
        assert!(rep.expired_firing.is_empty(), "not yet expired on 2026-10-01");
        // Past expiry (2026-10-17) and still firing: warn + renewal queue,
        // proposed = expiry_for(today) (2026-10-20 is ISO week 43, 43 mod 4 = 3).
        let rep = l.audit(&five, "2026-10-20").unwrap();
        assert_eq!(
            rep.expired_firing,
            vec![Renewal {
                id: "r-field".to_string(),
                expires: "2026-10-17".to_string(),
                hits: 5,
                proposed: "2026-12-25".to_string()
            }]
        );
        assert!(rep.render().contains("WARN expired-but-firing: r-field"));
        assert!(rep.render().contains("FAIL stale: inst"));
        // Expired and silent shows up as informational (and stale once K runs are in).
        let silent = vec![run(1, 0, 1); 5];
        let rep = l.audit(&silent, "2026-10-20").unwrap();
        assert_eq!(rep.expired_silent, vec!["r-field".to_string()]);
        assert_eq!(rep.stale, vec!["r-field".to_string()]);
        // Only the last K runs count.
        let mut old_hits = vec![run(5, 5, 5); 3];
        old_hits.extend(vec![run(1, 1, 0); 5]);
        let rep = l.audit(&old_hits, "2026-10-01").unwrap();
        assert_eq!(rep.stale, vec!["inst".to_string()]);
    }

    #[test]
    fn render_with_hits_folds_counters() {
        let l = ledger(ROW_PLANE);
        l.hit("inst");
        l.hit("inst");
        let text = l.render_with_hits();
        assert!(text.starts_with(LEDGER_HEADER));
        assert!(text.contains("hits = 2"));
        let again = Ledger::parse(&text).unwrap();
        assert_eq!(again.get("inst").unwrap().hits, 2);
    }

    #[test]
    fn embedded_ledger_parses_and_is_the_live_file() {
        let l = Ledger::embedded();
        assert!(l.len() >= 25, "{} rows", l.len());
        // Every migrated ruled.rs id is present.
        for e in crate::ruled::default_table() {
            assert!(l.get(e.id).is_some(), "missing migrated ruling {}", e.id);
        }
        // Cosmetic rows expire per policy; nothing else does.
        for r in &l.rulings.rulings {
            if is_cosmetic_ruling(r) {
                assert_eq!(r.expires.as_deref(), Some(expiry_for(&r.created).unwrap().as_str()), "{}", r.id);
            } else {
                assert!(r.expires.is_none(), "{}", r.id);
            }
        }
        // The on-disk text is exactly the rendered form (parse -> render pin).
        assert_eq!(render(&l.rulings), EMBEDDED_LEDGER);
    }
}
