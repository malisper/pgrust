//! recipe: the sitediff recipe model (plan v2 §3.1 "recipe.rs", §3.2
//! "Recipe"; lane L0.6).
//!
//! A recipe is one `.sql` file under `recipes/<subsystem>/<cfile-stem>/
//! <id>.sql`: a `-- key: value` header block (`contracts::RecipeHeader`)
//! followed by a body of statements, directives and comments. This
//! module turns the body into typed items with a dollar-quote-aware
//! statement splitter, computes the per-statement `ordered:` value with a
//! small SELECT-shape scanner, renders the recipe back to canonical text
//! (parse → render is byte-stable on canonical files, and render is
//! idempotent on any parseable input) and emits `contracts::StepRecord`
//! values for the runner.
//!
//! Body grammar (one item per line in the canonical form):
//!
//! * `-- @<directive> ...` — a directive line. Known directives: `@s<N>`
//!   (switch session), `@connect k=v ...`, `@file <name>`, `@mark <name>`,
//!   `@probe <deck>`, `@expect-crash`, `@hold`, `@settle <id> max=<ms>`,
//!   `@storm n=<n> hold=<ms>`, `@pressure mb=<mb> queries=<n>`,
//!   `@ordered total|partial|none` (overrides the next statement's inferred
//!   value), `@expect <expectation>` (attaches an `expect_c` to the next
//!   statement), and the block openers `@psql` / `@manual` closed by `@end`.
//!   An unknown `@` directive is a parse error — a comment that merely
//!   starts with `@` must be written `-- \@...`.
//! * `-- ...` / `/* ... */` — comments, kept verbatim for round-tripping.
//! * `\...` — a psql meta-command line (also a statement that ends in one,
//!   `select 1 \parse p`), and `COPY ... FROM STDIN;` plus its data lines
//!   up to `\.` — all become `Psql` items flagged not-wire
//!   (plan §3.2: run by stock psql on both sides; the psql→wire translator
//!   is L1.11).
//! * anything else — SQL statements, split at top-level `;` honoring
//!   `'...'` (with `''`), `E'...'` (backslash escapes), `"..."`, `$$...$$`
//!   and `$tag$...$tag$` bodies, `--` and `/* */` comments (nested), so a
//!   `;` inside a function body never splits.
//!
//! `ordered:` per statement (plan §3.2): a top-level ORDER BY whose items
//! cover every output column (by expression text, alias or ordinal) ⇒
//! `total`; an ORDER BY that does not provably cover them (`*`, a
//! non-listed column) ⇒ `partial`; no top-level ORDER BY ⇒ `none`. Two
//! single-row shapes are `total` without ORDER BY: a SELECT with no FROM
//! and no set-returning call, and an aggregate query without GROUP BY or
//! OVER. EXPLAIN / SHOW / FETCH output is sequential and `total`. An ORDER
//! BY inside a subquery is not top-level. A `-- @ordered` directive
//! overrides one statement; the header's `ordered:` is the recipe-wide
//! summary (`Recipe::ordered_summary`: the weakest value over the
//! row-returning statements, `none` when there are none) that the bank
//! indexes on. The importer (`tools/sitediff/import_audit.py`) writes the
//! same summary with the same rules and the bank test cross-checks them.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::contracts::{Ordered, RecipeHeader, StepKind, StepRecord, XProto};

// ---------------------------------------------------------------------
// Items
// ---------------------------------------------------------------------

/// A parsed directive line (`-- @name args`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Directive {
    /// `@s1`, `@s2`, ... — subsequent steps run on that session.
    Session(String),
    /// `@connect user= db= options= password= protocol=` (any keys).
    Connect(BTreeMap<String, String>),
    /// `@file <name>` — an fs fixture / artifact step.
    File(String),
    /// `@mark <name>` — a server-log slicing marker.
    Mark(String),
    /// `@probe <deck>`.
    Probe(String),
    /// `@expect-crash` — the next statement is expected to crash B.
    ExpectCrash,
    /// `@hold` — this session waits until the other session is blocked.
    Hold,
    /// `@settle <id> max=<ms>`.
    Settle { id: String, max_ms: Option<u64> },
    /// `@storm n=<n> hold=<ms>`.
    Storm { n: Option<u64>, hold_ms: Option<u64> },
    /// `@pressure mb=<mb> queries=<n>`.
    Pressure { mb: Option<u64>, queries: Option<u64> },
    /// `@ordered total|partial|none` — overrides the next statement.
    Ordered(Ordered),
    /// `@expect <expectation>` — `expect_c` for the next statement.
    Expect(String),
}

/// How a `Psql` item was recognised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PsqlKind {
    /// A `\meta` command line.
    Meta,
    /// `COPY ... FROM STDIN;` followed by data lines and `\.`.
    CopyIn,
    /// An explicit `-- @psql` ... `-- @end` block.
    Block,
}

/// One body item, in file order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    /// A comment line (`-- ...`) or block (`/* ... */`), verbatim.
    Comment(String),
    Directive(Directive),
    /// A wire SQL statement: normalized text (trimmed, `;`-terminated) and
    /// an optional trailing same-line comment.
    Stmt { text: String, trailing: Option<String> },
    /// Text run by stock psql (not wire).
    Psql { kind: PsqlKind, lines: Vec<String> },
    /// A `-- @manual` block: prose steps a harness performs (not wire).
    Manual(Vec<String>),
}

/// A parsed recipe: header + body items.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recipe {
    pub header: RecipeHeader,
    pub items: Vec<Item>,
}

/// A computed step (the runner-facing view of one item).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipeStep {
    /// Index into `Recipe::items`.
    pub item: usize,
    pub session: String,
    pub kind: StepKind,
    pub sql: Option<String>,
    /// False for psql / manual steps (not sent over the wire).
    pub wire: bool,
    pub ordered: Ordered,
    pub expect_c: Option<String>,
    pub bracket: Option<String>,
    pub slots: BTreeMap<String, String>,
}

// ---------------------------------------------------------------------
// Lexical helpers shared by the splitter and the shape scanner
// ---------------------------------------------------------------------

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// If a quoted lexeme or comment starts at `i`, return the byte index just
/// past its end (EOF-truncated lexemes end at `len`). Handles `'...'`
/// (with `''`), `E'...'` (backslash escapes), `"..."` (with `""`),
/// `$tag$...$tag$`, `-- ...\n`, nested `/* ... */`.
fn skip_lexeme(b: &[u8], i: usize) -> Option<usize> {
    let n = b.len();
    let c = b[i];
    if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
        let mut j = i + 2;
        while j < n && b[j] != b'\n' {
            j += 1;
        }
        return Some(j);
    }
    if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
        let mut depth = 1usize;
        let mut j = i + 2;
        while j < n {
            if b[j] == b'/' && j + 1 < n && b[j + 1] == b'*' {
                depth += 1;
                j += 2;
            } else if b[j] == b'*' && j + 1 < n && b[j + 1] == b'/' {
                depth -= 1;
                j += 2;
                if depth == 0 {
                    return Some(j);
                }
            } else {
                j += 1;
            }
        }
        return Some(n);
    }
    if c == b'\'' {
        let escaped = i > 0 && (b[i - 1] == b'E' || b[i - 1] == b'e') && (i < 2 || !is_ident_char(b[i - 2]));
        let mut j = i + 1;
        while j < n {
            if escaped && b[j] == b'\\' {
                j += 2;
                continue;
            }
            if b[j] == b'\'' {
                if j + 1 < n && b[j + 1] == b'\'' {
                    j += 2;
                    continue;
                }
                return Some(j + 1);
            }
            j += 1;
        }
        return Some(n);
    }
    if c == b'"' {
        let mut j = i + 1;
        while j < n {
            if b[j] == b'"' {
                if j + 1 < n && b[j + 1] == b'"' {
                    j += 2;
                    continue;
                }
                return Some(j + 1);
            }
            j += 1;
        }
        return Some(n);
    }
    if c == b'$' {
        // $tag$ where tag is empty or an identifier not starting with a digit
        let mut j = i + 1;
        if j < n && (b[j].is_ascii_alphabetic() || b[j] == b'_') {
            while j < n && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
        }
        if j < n && b[j] == b'$' && (i == 0 || !is_ident_char(b[i - 1])) {
            let tag = &b[i..=j];
            let mut k = j + 1;
            while k + tag.len() <= n {
                if &b[k..k + tag.len()] == tag {
                    return Some(k + tag.len());
                }
                k += 1;
            }
            return Some(n);
        }
        return None;
    }
    None
}

// ---------------------------------------------------------------------
// Body splitter
// ---------------------------------------------------------------------

fn first_word(s: &str) -> String {
    s.trim_start().chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect::<String>().to_ascii_lowercase()
}

fn is_copy_from_stdin(stmt: &str) -> bool {
    if first_word(stmt) != "copy" {
        return false;
    }
    let toks = lex(stmt);
    let mut i = 0;
    while i + 1 < toks.len() {
        if toks[i].tok == Tok::Word("from".into()) && toks[i + 1].tok == Tok::Word("stdin".into()) {
            return true;
        }
        i += 1;
    }
    false
}

fn parse_kv_args(args: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    let b = args.as_bytes();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let ks = i;
        while i < b.len() && b[i] != b'=' && !b[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &args[ks..i];
        if i >= b.len() || b[i] != b'=' {
            return Err(format!("directive argument {:?} needs key=value", key));
        }
        i += 1;
        let val;
        if i < b.len() && b[i] == b'\'' {
            let mut j = i + 1;
            let mut s = String::new();
            loop {
                if j >= b.len() {
                    return Err(format!("unterminated quoted value for {:?}", key));
                }
                if b[j] == b'\'' {
                    if j + 1 < b.len() && b[j + 1] == b'\'' {
                        s.push('\'');
                        j += 2;
                        continue;
                    }
                    j += 1;
                    break;
                }
                s.push(b[j] as char);
                j += 1;
            }
            val = s;
            i = j;
        } else {
            let vs = i;
            while i < b.len() && !b[i].is_ascii_whitespace() {
                i += 1;
            }
            val = args[vs..i].to_string();
        }
        if key.is_empty() {
            return Err("directive argument with empty key".into());
        }
        out.insert(key.to_string(), val);
    }
    Ok(out)
}

fn arg_u64(m: &BTreeMap<String, String>, k: &str, what: &str) -> Result<Option<u64>, String> {
    match m.get(k) {
        None => Ok(None),
        Some(v) => v.parse::<u64>().map(Some).map_err(|_| format!("{} {}= must be an integer, got {:?}", what, k, v)),
    }
}

fn quote_kv(v: &str) -> String {
    if v.is_empty() || v.chars().any(|c| c.is_ascii_whitespace() || c == '\'') {
        format!("'{}'", v.replace('\'', "''"))
    } else {
        v.to_string()
    }
}

impl Directive {
    /// Parse the text after `-- @` (name and arguments).
    pub fn parse(line: &str) -> Result<Directive, String> {
        let line = line.trim();
        let (name, args) = match line.find(char::is_whitespace) {
            Some(p) => (&line[..p], line[p..].trim()),
            None => (line, ""),
        };
        let no_args = |d: Directive| if args.is_empty() { Ok(d) } else { Err(format!("@{} takes no arguments", name)) };
        let one_arg = |what: &str| -> Result<String, String> {
            if args.is_empty() || args.contains(char::is_whitespace) {
                Err(format!("@{} takes exactly one {}", name, what))
            } else {
                Ok(args.to_string())
            }
        };
        if let Some(num) = name.strip_prefix('s') {
            if !num.is_empty() && num.bytes().all(|c| c.is_ascii_digit()) {
                return no_args(Directive::Session(name.to_string()));
            }
        }
        match name {
            "connect" => Ok(Directive::Connect(parse_kv_args(args)?)),
            "file" => Ok(Directive::File(one_arg("name")?)),
            "mark" => Ok(Directive::Mark(one_arg("name")?)),
            "probe" => Ok(Directive::Probe(one_arg("deck")?)),
            "expect-crash" => no_args(Directive::ExpectCrash),
            "hold" => no_args(Directive::Hold),
            "settle" => {
                let (id, rest) = match args.find(char::is_whitespace) {
                    Some(p) => (&args[..p], args[p..].trim()),
                    None => (args, ""),
                };
                if id.is_empty() {
                    return Err("@settle needs a predicate id".into());
                }
                let m = parse_kv_args(rest)?;
                for k in m.keys() {
                    if k != "max" {
                        return Err(format!("@settle: unknown argument {:?}", k));
                    }
                }
                Ok(Directive::Settle { id: id.to_string(), max_ms: arg_u64(&m, "max", "@settle")? })
            }
            "storm" => {
                let m = parse_kv_args(args)?;
                for k in m.keys() {
                    if k != "n" && k != "hold" {
                        return Err(format!("@storm: unknown argument {:?}", k));
                    }
                }
                Ok(Directive::Storm { n: arg_u64(&m, "n", "@storm")?, hold_ms: arg_u64(&m, "hold", "@storm")? })
            }
            "pressure" => {
                let m = parse_kv_args(args)?;
                for k in m.keys() {
                    if k != "mb" && k != "queries" {
                        return Err(format!("@pressure: unknown argument {:?}", k));
                    }
                }
                Ok(Directive::Pressure { mb: arg_u64(&m, "mb", "@pressure")?, queries: arg_u64(&m, "queries", "@pressure")? })
            }
            "ordered" => Ok(Directive::Ordered(Ordered::parse(&one_arg("value")?)?)),
            "expect" => {
                if args.is_empty() {
                    Err("@expect needs an expectation".into())
                } else {
                    Ok(Directive::Expect(args.to_string()))
                }
            }
            "psql" | "manual" | "end" => Err(format!("@{} is a block marker, not a directive", name)),
            _ => Err(format!("unknown directive @{}", name)),
        }
    }

    /// Canonical `-- @...` line.
    pub fn render(&self) -> String {
        match self {
            Directive::Session(s) => format!("-- @{}", s),
            Directive::Connect(m) => {
                let mut out = String::from("-- @connect");
                for (k, v) in m {
                    let _ = write!(out, " {}={}", k, quote_kv(v));
                }
                out
            }
            Directive::File(n) => format!("-- @file {}", n),
            Directive::Mark(n) => format!("-- @mark {}", n),
            Directive::Probe(d) => format!("-- @probe {}", d),
            Directive::ExpectCrash => "-- @expect-crash".into(),
            Directive::Hold => "-- @hold".into(),
            Directive::Settle { id, max_ms } => match max_ms {
                Some(ms) => format!("-- @settle {} max={}", id, ms),
                None => format!("-- @settle {}", id),
            },
            Directive::Storm { n, hold_ms } => {
                let mut out = String::from("-- @storm");
                if let Some(n) = n {
                    let _ = write!(out, " n={}", n);
                }
                if let Some(h) = hold_ms {
                    let _ = write!(out, " hold={}", h);
                }
                out
            }
            Directive::Pressure { mb, queries } => {
                let mut out = String::from("-- @pressure");
                if let Some(mb) = mb {
                    let _ = write!(out, " mb={}", mb);
                }
                if let Some(q) = queries {
                    let _ = write!(out, " queries={}", q);
                }
                out
            }
            Directive::Ordered(o) => format!("-- @ordered {}", o.as_str()),
            Directive::Expect(e) => format!("-- @expect {}", e),
        }
    }
}

/// Byte index of the end of the line containing `i` (exclusive of `\n`).
fn line_end(b: &[u8], i: usize) -> usize {
    let mut j = i;
    while j < b.len() && b[j] != b'\n' {
        j += 1;
    }
    j
}

/// Split a recipe body into items. `body` is the text after the header.
pub fn split_body(body: &str) -> Result<Vec<Item>, String> {
    let b = body.as_bytes();
    let n = b.len();
    let mut items = Vec::new();
    let mut i = 0usize;
    // line numbers for error messages
    let line_of = |pos: usize| 1 + body[..pos.min(body.len())].matches('\n').count();
    while i < n {
        // skip whitespace / blank lines
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // line comment or directive
        if b[i] == b'-' && i + 1 < n && b[i + 1] == b'-' {
            let e = line_end(b, i);
            let line = body[i..e].trim_end_matches('\r');
            let rest = line[2..].trim_start();
            if let Some(d) = rest.strip_prefix('@') {
                let name = d.split(char::is_whitespace).next().unwrap_or("");
                match name {
                    "psql" | "manual" => {
                        if !d[name.len()..].trim().is_empty() {
                            return Err(format!("line {}: @{} takes no arguments", line_of(i), name));
                        }
                        let mut lines = Vec::new();
                        let mut j = e + 1;
                        let mut closed = false;
                        while j < n {
                            let le = line_end(b, j);
                            let l = body[j..le].trim_end_matches('\r');
                            let t = l.trim();
                            if t.starts_with("--") && t[2..].trim() == "@end" {
                                closed = true;
                                j = le + 1;
                                break;
                            }
                            if name == "manual" {
                                let t = l.trim_start();
                                let Some(c) = t.strip_prefix("--") else {
                                    return Err(format!("line {}: @manual block lines must be `-- ` comments", line_of(j)));
                                };
                                lines.push(c.strip_prefix(' ').unwrap_or(c).to_string());
                            } else {
                                lines.push(l.to_string());
                            }
                            j = le + 1;
                        }
                        if !closed {
                            return Err(format!("line {}: @{} block without -- @end", line_of(i), name));
                        }
                        items.push(if name == "manual" {
                            Item::Manual(lines)
                        } else {
                            Item::Psql { kind: PsqlKind::Block, lines }
                        });
                        i = j.min(n);
                        continue;
                    }
                    "end" => return Err(format!("line {}: -- @end without an open block", line_of(i))),
                    _ => {
                        let dir = Directive::parse(d).map_err(|m| format!("line {}: {}", line_of(i), m))?;
                        items.push(Item::Directive(dir));
                        i = e;
                        continue;
                    }
                }
            }
            items.push(Item::Comment(line.to_string()));
            i = e;
            continue;
        }
        if b[i] == b'/' && i + 1 < n && b[i + 1] == b'*' {
            let e = skip_lexeme(b, i).unwrap();
            items.push(Item::Comment(body[i..e].trim_end().to_string()));
            i = e;
            continue;
        }
        // psql meta-command line
        if b[i] == b'\\' {
            let e = line_end(b, i);
            items.push(Item::Psql { kind: PsqlKind::Meta, lines: vec![body[i..e].trim_end().to_string()] });
            i = e;
            continue;
        }
        // SQL statement up to a top-level ';'
        let start = i;
        let mut j = i;
        let mut end = None;
        let mut meta = false;
        while j < n {
            if let Some(e) = skip_lexeme(b, j) {
                j = e;
                continue;
            }
            if b[j] == b';' {
                end = Some(j + 1);
                break;
            }
            if b[j] == b'\\' {
                // a psql meta-command inside a statement (`select 1 \parse p`,
                // `\g`): the statement runs to the end of the line and is
                // psql text, not wire
                meta = true;
                end = Some(line_end(b, j));
                break;
            }
            j += 1;
        }
        if meta {
            let e = end.unwrap();
            items.push(Item::Psql { kind: PsqlKind::Meta, lines: vec![body[start..e].trim_end().to_string()] });
            i = e;
            continue;
        }
        let stmt_end = end.unwrap_or(n);
        let mut text = body[start..stmt_end].trim_end().to_string();
        if !text.ends_with(';') {
            text.push(';');
        }
        i = stmt_end;
        // trailing same-line comment after the ';'
        let mut trailing = None;
        if end.is_some() {
            let le = line_end(b, i);
            let rest = body[i..le].trim();
            if rest.starts_with("--") {
                trailing = Some(rest.to_string());
                i = le;
            }
        }
        if is_copy_from_stdin(&text) {
            // data lines up to a line that is exactly `\.`
            let mut lines = vec![match trailing.take() {
                Some(t) => format!("{} {}", text, t),
                None => text.clone(),
            }];
            // move to the next line
            i = line_end(b, i);
            if i < n {
                i += 1;
            }
            let mut closed = false;
            while i < n {
                let le = line_end(b, i);
                let l = body[i..le].trim_end_matches('\r');
                i = if le < n { le + 1 } else { n };
                if l.trim() == "\\." {
                    lines.push("\\.".to_string());
                    closed = true;
                    break;
                }
                lines.push(l.to_string());
            }
            if !closed {
                return Err(format!("line {}: COPY ... FROM STDIN block without a closing \\.", line_of(start)));
            }
            items.push(Item::Psql { kind: PsqlKind::CopyIn, lines });
            continue;
        }
        items.push(Item::Stmt { text, trailing });
    }
    Ok(items)
}

// ---------------------------------------------------------------------
// SELECT-shape scanner for `ordered:`
// ---------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    /// Unquoted identifier / keyword, lowercased.
    Word(String),
    /// Quoted identifier, verbatim including the quotes.
    Quoted(String),
    Num(String),
    Str,
    Punct(String),
}

#[derive(Clone, Debug)]
struct T {
    tok: Tok,
    depth: usize,
}

fn lex(sql: &str) -> Vec<T> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut depth = 0usize;
    while i < n {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if (c == b'-' && i + 1 < n && b[i + 1] == b'-') || (c == b'/' && i + 1 < n && b[i + 1] == b'*') {
            i = skip_lexeme(b, i).unwrap();
            continue;
        }
        if c == b'\'' || c == b'$' || c == b'"' {
            if let Some(e) = skip_lexeme(b, i) {
                if c == b'"' {
                    out.push(T { tok: Tok::Quoted(sql[i..e].to_string()), depth });
                } else if c == b'\'' {
                    // E'...' : the E was lexed as a Word; drop it
                    if let Some(last) = out.last() {
                        if last.tok == Tok::Word("e".into()) && i > 0 && (b[i - 1] == b'E' || b[i - 1] == b'e') {
                            out.pop();
                        }
                    }
                    out.push(T { tok: Tok::Str, depth });
                } else {
                    out.push(T { tok: Tok::Str, depth });
                }
                i = e;
                continue;
            }
        }
        if c == b'$' && i + 1 < n && b[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < n && b[j].is_ascii_digit() {
                j += 1;
            }
            out.push(T { tok: Tok::Num(sql[i..j].to_string()), depth });
            i = j;
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && i + 1 < n && b[i + 1].is_ascii_digit()) {
            let mut j = i;
            while j < n && (b[j].is_ascii_alphanumeric() || b[j] == b'.' || b[j] == b'_') {
                j += 1;
            }
            out.push(T { tok: Tok::Num(sql[i..j].to_string()), depth });
            i = j;
            continue;
        }
        if is_ident_start(c) {
            let mut j = i;
            while j < n && is_ident_char(b[j]) {
                j += 1;
            }
            out.push(T { tok: Tok::Word(sql[i..j].to_ascii_lowercase()), depth });
            i = j;
            continue;
        }
        if c == b'(' {
            out.push(T { tok: Tok::Punct("(".into()), depth });
            depth += 1;
            i += 1;
            continue;
        }
        if c == b')' {
            depth = depth.saturating_sub(1);
            out.push(T { tok: Tok::Punct(")".into()), depth });
            i += 1;
            continue;
        }
        if c == b':' && i + 1 < n && b[i + 1] == b':' {
            out.push(T { tok: Tok::Punct("::".into()), depth });
            i += 2;
            continue;
        }
        if b",.;[]:".contains(&c) {
            out.push(T { tok: Tok::Punct((c as char).to_string()), depth });
            i += 1;
            continue;
        }
        // operator characters
        let mut j = i;
        while j < n && b"+-*/<>=~!@#%^&|`?".contains(&b[j]) {
            j += 1;
        }
        if j == i {
            j = i + 1;
        }
        out.push(T { tok: Tok::Punct(sql[i..j].to_string()), depth });
        i = j;
    }
    out
}

fn is_word(t: &T, w: &str) -> bool {
    matches!(&t.tok, Tok::Word(x) if x == w)
}

fn tok_text(t: &Tok) -> &str {
    match t {
        Tok::Word(s) | Tok::Quoted(s) | Tok::Num(s) | Tok::Punct(s) => s,
        Tok::Str => "'…'",
    }
}

fn norm_text(ts: &[T]) -> String {
    ts.iter().map(|t| tok_text(&t.tok)).collect::<Vec<_>>().join(" ")
}

/// Statement shapes for the ordered pass and the summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    Select,
    Values,
    Table,
    /// EXPLAIN / SHOW / FETCH: sequential output.
    Sequential,
    /// DML with RETURNING and every other statement.
    Other,
}

const SRF_NAMES: &[&str] = &[
    "generate_series", "generate_subscripts", "unnest", "regexp_matches", "regexp_split_to_table", "string_to_table",
    "json_each", "json_each_text", "jsonb_each", "jsonb_each_text", "json_array_elements", "json_array_elements_text",
    "jsonb_array_elements", "jsonb_array_elements_text", "json_object_keys", "jsonb_object_keys",
    "json_populate_recordset", "jsonb_populate_recordset", "json_to_recordset", "jsonb_to_recordset",
    "jsonb_path_query", "pg_ls_dir", "pg_listening_channels", "pg_show_all_settings", "pg_get_keywords",
    "pg_options_to_table", "pg_tablespace_databases", "pg_timezone_names", "pg_timezone_abbrevs",
    "pg_stat_file", "pg_ls_waldir", "pg_available_extensions", "pg_available_extension_versions", "pg_cursor",
    "pg_prepared_statement", "pg_prepared_xacts", "pg_locks", "pg_stat_get_activity", "txid_snapshot_xip",
    "pg_snapshot_xip", "aclexplode", "pg_partition_tree", "pg_partition_ancestors", "ts_debug", "ts_parse",
    "ts_token_type", "ts_stat", "pg_get_publication_tables", "pg_logical_slot_get_changes",
    "pg_logical_slot_peek_changes", "pg_event_trigger_ddl_commands", "pg_event_trigger_dropped_objects",
    "json_table", "xmltable", "rows_from", "brin_page_items", "bt_page_items", "gin_leafpage_items",
    "heap_page_items", "page_header", "pg_control_checkpoint",
];

const AGG_NAMES: &[&str] = &[
    "count", "sum", "min", "max", "avg", "array_agg", "string_agg", "bool_and", "bool_or", "every", "json_agg",
    "jsonb_agg", "json_object_agg", "jsonb_object_agg", "bit_and", "bit_or", "bit_xor", "xmlagg", "stddev",
    "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "corr", "covar_pop", "covar_samp", "regr_avgx",
    "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
    "mode", "percentile_cont", "percentile_disc", "rank", "dense_rank", "percent_rank", "cume_dist", "any_value",
    "range_agg", "range_intersect_agg",
];

/// Skip a leading `WITH ... ` CTE prefix: returns the index of the first
/// depth-0 statement keyword after it (or 0 when there is no WITH).
fn skip_cte_prefix(ts: &[T]) -> usize {
    if ts.is_empty() || !is_word(&ts[0], "with") {
        return 0;
    }
    for (i, t) in ts.iter().enumerate().skip(1) {
        if t.depth == 0 {
            if let Tok::Word(w) = &t.tok {
                if matches!(w.as_str(), "select" | "values" | "table" | "insert" | "update" | "delete" | "merge") {
                    // `WITH x AS (...) SELECT` — but not the `AS` list's names.
                    // A statement keyword right after ')' or ',' would be a
                    // CTE name only if followed by AS; check that.
                    let next_is_as = ts.get(i + 1).map(|n| is_word(n, "as")).unwrap_or(false);
                    if !next_is_as {
                        return i;
                    }
                }
            }
        }
    }
    0
}

/// The statement's shape (after any CTE prefix).
pub fn shape_of(sql: &str) -> Shape {
    let ts = lex(sql);
    let s = skip_cte_prefix(&ts);
    let Some(first) = ts.get(s) else { return Shape::Other };
    let w = match &first.tok {
        Tok::Word(w) => w.as_str(),
        Tok::Punct(p) if p == "(" => {
            // ( SELECT ... ) UNION ...
            return match ts.get(s + 1) {
                Some(t) if is_word(t, "select") => Shape::Select,
                Some(t) if is_word(t, "values") => Shape::Values,
                _ => Shape::Other,
            };
        }
        _ => return Shape::Other,
    };
    match w {
        "select" => Shape::Select,
        "values" => Shape::Values,
        "table" => Shape::Table,
        "explain" | "show" | "fetch" => Shape::Sequential,
        _ => Shape::Other,
    }
}

/// Split depth-0 comma-separated items of `ts` (all tokens at the same
/// base depth).
fn split_commas(ts: &[T], base: usize) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    for t in ts {
        if t.depth == base && t.tok == Tok::Punct(",".into()) {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(t.clone());
        }
    }
    if !cur.is_empty() || !out.is_empty() {
        out.push(cur);
    }
    out
}

fn is_keyword_alias_blocker(w: &str) -> bool {
    matches!(
        w,
        "from" | "where" | "group" | "having" | "window" | "order" | "limit" | "offset" | "fetch" | "for" | "union"
            | "intersect" | "except" | "into" | "and" | "or" | "not" | "is" | "in" | "like" | "ilike" | "similar"
            | "between" | "collate" | "at" | "over" | "filter" | "within" | "as" | "then" | "else" | "end" | "when"
            | "case" | "null" | "true" | "false" | "asc" | "desc" | "nulls" | "first" | "last" | "using" | "escape"
            | "distinct" | "all" | "array" | "row" | "cast" | "interval" | "on" | "with" | "select" | "any" | "some"
            | "exists" | "isnull" | "notnull"
    )
}

/// Keys that identify one output column: normalized expression text, alias,
/// the last component of a dotted reference, and the ordinal.
fn column_keys(col: &[T], ordinal: usize) -> (Vec<String>, bool) {
    // returns (keys, is_star)
    let mut keys = vec![ordinal.to_string()];
    if col.is_empty() {
        return (keys, false);
    }
    let last = col.last().unwrap();
    let is_star = matches!(&last.tok, Tok::Punct(p) if p == "*")
        && (col.len() == 1 || matches!(&col[col.len() - 2].tok, Tok::Punct(p) if p == "."));
    if is_star {
        return (keys, true);
    }
    let mut expr: &[T] = col;
    // explicit alias: ... AS name
    if col.len() >= 3 && is_word(&col[col.len() - 2], "as") {
        keys.push(tok_text(&last.tok).to_string());
        expr = &col[..col.len() - 2];
    } else if col.len() >= 2 {
        // implicit alias: a trailing identifier after a non-operator token
        let prev = &col[col.len() - 2];
        let last_is_name = matches!(&last.tok, Tok::Word(w) if !is_keyword_alias_blocker(w)) || matches!(&last.tok, Tok::Quoted(_));
        let prev_ok = match &prev.tok {
            Tok::Punct(p) => p == ")" || p == "]",
            Tok::Word(w) => !is_keyword_alias_blocker(w) || w == "null" || w == "true" || w == "false" || w == "end",
            _ => true,
        };
        if last_is_name && prev_ok && prev.depth == last.depth {
            keys.push(tok_text(&last.tok).to_string());
            expr = &col[..col.len() - 1];
        }
    }
    keys.push(norm_text(expr));
    // dotted reference: also the last component
    if expr.len() >= 3 && matches!(&expr[expr.len() - 2].tok, Tok::Punct(p) if p == ".") {
        keys.push(tok_text(&expr[expr.len() - 1].tok).to_string());
    }
    if expr.len() == 1 {
        keys.push(tok_text(&expr[0].tok).to_string());
    }
    (keys, false)
}

fn order_item_key(item: &[T]) -> Vec<String> {
    // strip ASC/DESC, NULLS FIRST/LAST, USING op
    let mut end = item.len();
    loop {
        if end >= 2 && is_word(&item[end - 2], "nulls") && (is_word(&item[end - 1], "first") || is_word(&item[end - 1], "last")) {
            end -= 2;
            continue;
        }
        if end >= 1 && (is_word(&item[end - 1], "asc") || is_word(&item[end - 1], "desc")) {
            end -= 1;
            continue;
        }
        if end >= 2 && is_word(&item[end - 2], "using") {
            end -= 2;
            continue;
        }
        break;
    }
    let expr = &item[..end];
    let mut keys = vec![norm_text(expr)];
    if expr.len() >= 3 && matches!(&expr[expr.len() - 2].tok, Tok::Punct(p) if p == ".") {
        keys.push(tok_text(&expr[expr.len() - 1].tok).to_string());
    }
    if expr.len() == 1 {
        keys.push(tok_text(&expr[0].tok).to_string());
    }
    keys
}

/// Infer the `ordered:` value of one statement (see the module docs).
pub fn infer_ordered(sql: &str) -> Ordered {
    let shape = shape_of(sql);
    let ts = lex(sql);
    let s = skip_cte_prefix(&ts);
    let ts = &ts[s..];
    match shape {
        Shape::Other => return Ordered::None,
        Shape::Sequential => return Ordered::Total,
        _ => {}
    }
    // base depth: statements wrapped as ( SELECT ... ) UNION ... start at depth 0 anyway
    let base = 0usize;
    let clause_kw = |w: &str| {
        matches!(
            w,
            "from" | "where" | "group" | "having" | "window" | "order" | "limit" | "offset" | "fetch" | "for" | "union"
                | "intersect" | "except" | "into"
        )
    };
    // locate the last depth-0 ORDER BY
    let mut order_at = None;
    for i in 0..ts.len().saturating_sub(1) {
        if ts[i].depth == base && is_word(&ts[i], "order") && is_word(&ts[i + 1], "by") {
            order_at = Some(i);
        }
    }
    let order_items: Option<Vec<Vec<T>>> = order_at.map(|o| {
        let mut e = o + 2;
        while e < ts.len() {
            if ts[e].depth == base {
                if let Tok::Word(w) = &ts[e].tok {
                    if matches!(w.as_str(), "limit" | "offset" | "fetch" | "for") {
                        break;
                    }
                }
                if ts[e].tok == Tok::Punct(";".into()) {
                    break;
                }
            }
            e += 1;
        }
        split_commas(&ts[o + 2..e], base)
    });
    match shape {
        Shape::Values => {
            // count top-level rows
            let rows = ts.iter().filter(|t| t.depth == base && t.tok == Tok::Punct(",".into())).count() + 1;
            if order_items.is_some() {
                return Ordered::Partial;
            }
            return if rows == 1 { Ordered::Total } else { Ordered::None };
        }
        Shape::Table => {
            return if order_items.is_some() { Ordered::Partial } else { Ordered::None };
        }
        _ => {}
    }
    // select list: after SELECT [ALL|DISTINCT [ON (...)]] up to a clause keyword
    let mut i = 0;
    // `( SELECT ...` set-op wrapper: analyse the first branch at depth 1
    let base = if ts.first().map(|t| t.tok == Tok::Punct("(".into())).unwrap_or(false) { 1 } else { base };
    while i < ts.len() && !(ts[i].depth == base && is_word(&ts[i], "select")) {
        i += 1;
    }
    i += 1;
    if i < ts.len() && is_word(&ts[i], "all") {
        i += 1;
    } else if i < ts.len() && is_word(&ts[i], "distinct") {
        i += 1;
        if i < ts.len() && is_word(&ts[i], "on") {
            // skip ( ... )
            i += 1;
            while i < ts.len() && !(ts[i].depth == base && ts[i].tok == Tok::Punct(")".into())) {
                i += 1;
            }
            i += 1;
        }
    }
    let list_start = i;
    let mut e = list_start;
    let mut has_from = false;
    let mut has_group = false;
    let mut has_over = false;
    let mut has_setop = false;
    while e < ts.len() {
        if ts[e].depth == base {
            if let Tok::Word(w) = &ts[e].tok {
                if clause_kw(w) {
                    break;
                }
            }
            if ts[e].tok == Tok::Punct(";".into()) {
                break;
            }
        }
        e += 1;
    }
    for t in &ts[e..] {
        if t.depth == base {
            if is_word(t, "from") {
                has_from = true;
            }
            if is_word(t, "group") {
                has_group = true;
            }
            if is_word(t, "union") || is_word(t, "intersect") || is_word(t, "except") {
                has_setop = true;
            }
        }
    }
    let list = &ts[list_start..e];
    for t in list {
        if t.depth == base && is_word(t, "over") {
            has_over = true;
        }
    }
    let cols = split_commas(list, base);
    if let Some(items) = order_items {
        let mut keys: Vec<String> = Vec::new();
        for it in &items {
            keys.extend(order_item_key(it));
        }
        let mut all = true;
        for (idx, col) in cols.iter().enumerate() {
            let (ck, star) = column_keys(col, idx + 1);
            if star || !ck.iter().any(|k| keys.contains(k)) {
                all = false;
                break;
            }
        }
        return if all && !cols.is_empty() { Ordered::Total } else { Ordered::Partial };
    }
    if has_setop {
        return Ordered::None;
    }
    // single-row shapes
    let calls: Vec<&str> = list
        .iter()
        .enumerate()
        .filter_map(|(k, t)| match &t.tok {
            Tok::Word(w) if list.get(k + 1).map(|n| n.tok == Tok::Punct("(".into())).unwrap_or(false) => Some(w.as_str()),
            _ => None,
        })
        .collect();
    let has_srf = calls.iter().any(|c| SRF_NAMES.contains(c));
    let has_agg = calls.iter().any(|c| AGG_NAMES.contains(c));
    let has_star = cols.iter().enumerate().any(|(idx, c)| column_keys(c, idx + 1).1);
    if !has_from && !has_srf && !has_star {
        return Ordered::Total;
    }
    if has_agg && !has_group && !has_over && !has_srf {
        return Ordered::Total;
    }
    Ordered::None
}

fn weaker(a: Ordered, b: Ordered) -> Ordered {
    let rank = |o: Ordered| match o {
        Ordered::None => 0,
        Ordered::Partial => 1,
        Ordered::Total => 2,
    };
    if rank(b) < rank(a) {
        b
    } else {
        a
    }
}

// ---------------------------------------------------------------------
// Recipe: parse, render, steps
// ---------------------------------------------------------------------

fn txn_open(word: &str) -> bool {
    matches!(word, "begin" | "start")
}

fn txn_close(word: &str, text: &str) -> bool {
    match word {
        "commit" | "end" | "rollback" | "abort" => {
            // `ROLLBACK TO SAVEPOINT` keeps the bracket open
            let ts = lex(text);
            !(ts.len() >= 2 && is_word(&ts[1], "to"))
        }
        "prepare" => {
            let ts = lex(text);
            ts.len() >= 2 && is_word(&ts[1], "transaction")
        }
        _ => false,
    }
}

impl Recipe {
    /// Parse a whole recipe file.
    pub fn parse(text: &str) -> Result<Recipe, String> {
        let (header, body_start) = RecipeHeader::parse(text)?;
        let items = split_body(&text[body_start..])?;
        Ok(Recipe { header, items })
    }

    /// Canonical text: the header block then one item per line.
    pub fn render(&self) -> String {
        let mut out = self.header.render();
        for it in &self.items {
            match it {
                Item::Comment(c) => {
                    out.push_str(c);
                    out.push('\n');
                }
                Item::Directive(d) => {
                    out.push_str(&d.render());
                    out.push('\n');
                }
                Item::Stmt { text, trailing } => {
                    out.push_str(text);
                    if let Some(t) = trailing {
                        out.push(' ');
                        out.push_str(t);
                    }
                    out.push('\n');
                }
                Item::Psql { kind, lines } => {
                    if *kind == PsqlKind::Block {
                        out.push_str("-- @psql\n");
                    }
                    for l in lines {
                        out.push_str(l);
                        out.push('\n');
                    }
                    if *kind == PsqlKind::Block {
                        out.push_str("-- @end\n");
                    }
                }
                Item::Manual(lines) => {
                    out.push_str("-- @manual\n");
                    for l in lines {
                        if l.is_empty() {
                            out.push_str("--\n");
                        } else {
                            out.push_str("-- ");
                            out.push_str(l);
                            out.push('\n');
                        }
                    }
                    out.push_str("-- @end\n");
                }
            }
        }
        out
    }

    /// The wire SQL statements in order.
    pub fn statements(&self) -> Vec<&str> {
        self.items.iter().filter_map(|it| match it {
            Item::Stmt { text, .. } => Some(text.as_str()),
            _ => None,
        }).collect()
    }

    /// True when every step is a wire step (no psql / manual items).
    pub fn is_wire(&self) -> bool {
        self.items.iter().all(|it| !matches!(it, Item::Psql { .. } | Item::Manual(_)))
    }

    /// The recipe-wide `ordered:` summary: the weakest per-statement value
    /// over the row-returning statements (SELECT / VALUES / TABLE /
    /// EXPLAIN / SHOW / FETCH shapes, after `@ordered` overrides); `none`
    /// when there are none.
    pub fn ordered_summary(&self) -> Ordered {
        let mut acc: Option<Ordered> = None;
        for st in self.steps() {
            if st.kind != StepKind::Sql && st.kind != StepKind::Xproto {
                continue;
            }
            let Some(sql) = &st.sql else { continue };
            if shape_of(sql) == Shape::Other {
                continue;
            }
            acc = Some(match acc {
                None => st.ordered,
                Some(a) => weaker(a, st.ordered),
            });
        }
        acc.unwrap_or(Ordered::None)
    }

    /// Compute the steps: sessions, kinds, ordered, brackets, expectations.
    pub fn steps(&self) -> Vec<RecipeStep> {
        let mut out = Vec::new();
        let mut session = "s1".to_string();
        let mut pending_ordered: Option<Ordered> = None;
        let mut pending_expect: Option<String> = None;
        let mut open_bracket: BTreeMap<String, String> = BTreeMap::new();
        let mut bracket_seq = 0usize;
        let has_expect_directive = self.items.iter().any(|it| matches!(it, Item::Directive(Directive::Expect(_))));
        let mut last_sql_step: Option<usize> = None;
        let sql_kind = if self.header.protocol == "extended" { StepKind::Xproto } else { StepKind::Sql };
        for (idx, it) in self.items.iter().enumerate() {
            let mut step = RecipeStep {
                item: idx,
                session: session.clone(),
                kind: StepKind::Sql,
                sql: None,
                wire: true,
                ordered: Ordered::None,
                expect_c: None,
                bracket: open_bracket.get(&session).cloned(),
                slots: BTreeMap::new(),
            };
            match it {
                Item::Comment(_) => continue,
                Item::Directive(d) => match d {
                    Directive::Session(s) => {
                        session = s.clone();
                        continue;
                    }
                    Directive::Ordered(o) => {
                        pending_ordered = Some(*o);
                        continue;
                    }
                    Directive::Expect(e) => {
                        pending_expect = Some(e.clone());
                        continue;
                    }
                    Directive::Connect(m) => {
                        step.kind = StepKind::Connect;
                        step.sql = Some(m.iter().map(|(k, v)| format!("{}={}", k, quote_kv(v))).collect::<Vec<_>>().join(" "));
                        step.slots = m.clone();
                    }
                    Directive::File(n) => {
                        step.kind = StepKind::Env("file".into());
                        step.sql = Some(n.clone());
                    }
                    Directive::Mark(n) => {
                        step.kind = StepKind::Env("mark".into());
                        step.sql = Some(n.clone());
                    }
                    Directive::Probe(deck) => step.kind = StepKind::Probe(deck.clone()),
                    Directive::ExpectCrash => step.kind = StepKind::Env("expect-crash".into()),
                    Directive::Hold => step.kind = StepKind::SleepUntilBlocked,
                    Directive::Settle { id, max_ms } => {
                        step.kind = StepKind::Settle;
                        step.sql = Some(id.clone());
                        if let Some(ms) = max_ms {
                            step.slots.insert("max".into(), ms.to_string());
                        }
                    }
                    Directive::Storm { n, hold_ms } => {
                        step.kind = StepKind::Storm;
                        if let Some(n) = n {
                            step.slots.insert("n".into(), n.to_string());
                        }
                        if let Some(h) = hold_ms {
                            step.slots.insert("hold".into(), h.to_string());
                        }
                    }
                    Directive::Pressure { mb, queries } => {
                        step.kind = StepKind::Pressure;
                        if let Some(mb) = mb {
                            step.slots.insert("mb".into(), mb.to_string());
                        }
                        if let Some(q) = queries {
                            step.slots.insert("queries".into(), q.to_string());
                        }
                    }
                },
                Item::Stmt { text, .. } => {
                    let w = first_word(text);
                    if txn_open(&w) && !open_bracket.contains_key(&session) {
                        bracket_seq += 1;
                        let id = format!("txn{}", bracket_seq);
                        open_bracket.insert(session.clone(), id.clone());
                        step.bracket = Some(id);
                    }
                    step.kind = sql_kind.clone();
                    step.sql = Some(text.clone());
                    step.ordered = pending_ordered.take().unwrap_or_else(|| infer_ordered(text));
                    step.expect_c = pending_expect.take();
                    if txn_close(&w, text) {
                        open_bracket.remove(&session);
                    }
                    last_sql_step = Some(out.len());
                }
                Item::Psql { kind, lines } => {
                    step.wire = false;
                    step.kind = if *kind == PsqlKind::CopyIn { StepKind::CopyIn } else { StepKind::Env("psql".into()) };
                    step.sql = Some(lines.join("\n"));
                    step.expect_c = pending_expect.take();
                    pending_ordered = None;
                }
                Item::Manual(lines) => {
                    step.wire = false;
                    step.kind = StepKind::Env("manual".into());
                    step.sql = Some(lines.join("\n"));
                    step.expect_c = pending_expect.take();
                    pending_ordered = None;
                }
            }
            out.push(step);
        }
        if !has_expect_directive {
            if let (Some(e), Some(i)) = (&self.header.expect_c, last_sql_step) {
                out[i].expect_c = Some(e.clone());
            }
        }
        out
    }

    /// Emit `StepRecord`s for the stream: `scenario`, sequence numbers
    /// starting at `seq0`.
    pub fn to_steps(&self, scenario: &str, seq0: u64) -> Vec<StepRecord> {
        let targets: Vec<String> = self.header.targets.iter().filter(|t| t.as_str() != "-").cloned().collect();
        self.steps()
            .into_iter()
            .enumerate()
            .map(|(k, st)| {
                let xproto = if st.kind == StepKind::Xproto {
                    Some(XProto {
                        mode: "parse_bind_execute".into(),
                        params: Vec::new(),
                        stmt: String::new(),
                        portal: String::new(),
                        limit: 0,
                        describe: String::new(),
                    })
                } else {
                    None
                };
                let mut slots = self.header.slots.keys().map(|k| (k.clone(), String::new())).collect::<BTreeMap<_, _>>();
                for (k, v) in st.slots {
                    slots.insert(k, v);
                }
                StepRecord {
                    scenario: scenario.to_string(),
                    seq: seq0 + k as u64,
                    session: st.session,
                    role: self.header.session.clone(),
                    kind: st.kind,
                    sql: st.sql,
                    xproto,
                    productions: Vec::new(),
                    targets: targets.clone(),
                    ordered: st.ordered,
                    expect_c: st.expect_c,
                    bracket: st.bracket,
                    recipe: Some(self.header.id.clone()),
                    mutant: None,
                    slots,
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn stmts(body: &str) -> Vec<String> {
        split_body(body)
            .unwrap()
            .into_iter()
            .filter_map(|it| match it {
                Item::Stmt { text, .. } => Some(text),
                _ => None,
            })
            .collect()
    }

    const HDR: &str = "-- id: t/x/y\n-- targets: -\n-- env: base\n-- session: superuser\n-- protocol: simple\n-- ordered: none\n-- oracle: transcript\n-- origin: author\n";

    #[test]
    fn split_plain_and_multi_per_line() {
        let v = stmts("create table t(a int); insert into t values (1);\nselect 1\n");
        assert_eq!(v, vec!["create table t(a int);", "insert into t values (1);", "select 1;"]);
    }

    #[test]
    fn split_dollar_quotes() {
        let v = stmts("do $$ begin raise notice 'a;b'; end $$;\ncreate function f() returns int language plpgsql as $x$ begin return 1; end; $x$;\nselect 1;");
        assert_eq!(v.len(), 3);
        assert!(v[0].starts_with("do $$"));
        assert!(v[1].contains("$x$ begin return 1; end; $x$"));
    }

    #[test]
    fn split_nested_dollar_tags() {
        let v = stmts("create function f() returns void language plpgsql as $outer$ begin execute $inner$ select 1; $inner$; end; $outer$;\nselect 2;");
        assert_eq!(v.len(), 2);
        assert!(v[0].ends_with("$outer$;"));
    }

    #[test]
    fn split_quotes_and_escapes() {
        let v = stmts("select 'a;b', E'c\\';d', \"e;f\", $1;\nselect 2;");
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("E'c\\';d'"));
        // $1 is a parameter, not a dollar tag
        let v = stmts("select $1 || ';' ; select $2;");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn split_comments_inside_and_between() {
        let items = split_body("-- lead\nselect 1, -- inner ; comment\n 2 /* block ; */;\n/* between\n;\n*/\nselect 3; -- trailing\n").unwrap();
        assert_eq!(items[0], Item::Comment("-- lead".into()));
        assert!(matches!(&items[1], Item::Stmt { text, .. } if text.contains("-- inner ; comment") && text.ends_with("*/;")));
        assert!(matches!(&items[2], Item::Comment(c) if c.starts_with("/* between")));
        assert_eq!(items[3], Item::Stmt { text: "select 3;".into(), trailing: Some("-- trailing".into()) });
    }

    #[test]
    fn split_copy_stdin_and_psql_meta() {
        let items = split_body("copy t from stdin;\n1\tx\n2\ty\n\\.\n\\c otherdb\nselect 1;\n").unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Item::Psql { kind: PsqlKind::CopyIn, lines: vec!["copy t from stdin;".into(), "1\tx".into(), "2\ty".into(), "\\.".into()] });
        assert_eq!(items[1], Item::Psql { kind: PsqlKind::Meta, lines: vec!["\\c otherdb".into()] });
        assert!(matches!(&items[2], Item::Stmt { .. }));
        assert!(split_body("copy t from stdin;\n1\n").is_err());
        // a meta-command ending a statement makes the whole line psql text
        let items = split_body("select $1::int \\parse p\n\\bind_named p 16 \\g\nselect 2;\n").unwrap();
        assert_eq!(items[0], Item::Psql { kind: PsqlKind::Meta, lines: vec!["select $1::int \\parse p".into()] });
        assert_eq!(items[1], Item::Psql { kind: PsqlKind::Meta, lines: vec!["\\bind_named p 16 \\g".into()] });
        assert_eq!(items[2], Item::Stmt { text: "select 2;".into(), trailing: None });
        // COPY TO STDOUT / FROM a file are wire statements
        let v = stmts("copy t to stdout; copy t from '/tmp/x';");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn split_explicit_blocks() {
        let items = split_body("-- @psql\n\\set x 1\nselect :x;\n-- @end\n-- @manual\n-- do the thing\n--\n-- then this\n-- @end\n").unwrap();
        assert_eq!(items[0], Item::Psql { kind: PsqlKind::Block, lines: vec!["\\set x 1".into(), "select :x;".into()] });
        assert_eq!(items[1], Item::Manual(vec!["do the thing".into(), "".into(), "then this".into()]));
        assert!(split_body("-- @psql\nselect 1;\n").is_err());
        assert!(split_body("-- @end\n").is_err());
    }

    #[test]
    fn directives_parse_and_render() {
        let cases = [
            ("s2", Directive::Session("s2".into())),
            ("file parquet", Directive::File("parquet".into())),
            ("mark m1", Directive::Mark("m1".into())),
            ("probe locks", Directive::Probe("locks".into())),
            ("expect-crash", Directive::ExpectCrash),
            ("hold", Directive::Hold),
            ("settle apply-caught-up max=5000", Directive::Settle { id: "apply-caught-up".into(), max_ms: Some(5000) }),
            ("settle p1", Directive::Settle { id: "p1".into(), max_ms: None }),
            ("storm n=64 hold=200", Directive::Storm { n: Some(64), hold_ms: Some(200) }),
            ("pressure mb=512 queries=8", Directive::Pressure { mb: Some(512), queries: Some(8) }),
            ("ordered total", Directive::Ordered(Ordered::Total)),
            ("expect E:42501:permission denied for schema s", Directive::Expect("E:42501:permission denied for schema s".into())),
        ];
        for (text, want) in cases {
            let d = Directive::parse(text).unwrap();
            assert_eq!(d, want, "{}", text);
            assert_eq!(d.render(), format!("-- @{}", text));
            assert_eq!(Directive::parse(&d.render()[4..]).unwrap(), want);
        }
        let d = Directive::parse("connect user=u1 db=d1 options='-c work_mem=8MB' protocol=3.2 password=''").unwrap();
        let Directive::Connect(m) = &d else { panic!() };
        assert_eq!(m["options"], "-c work_mem=8MB");
        assert_eq!(m["password"], "");
        assert_eq!(d.render(), "-- @connect db=d1 options='-c work_mem=8MB' password='' protocol=3.2 user=u1");
        assert!(Directive::parse("bogus").is_err());
        assert!(Directive::parse("storm n=x").is_err());
        assert!(Directive::parse("settle").is_err());
        assert!(Directive::parse("hold now").is_err());
        assert!(split_body("-- @nope\n").is_err());
        // an escaped '@' comment is a plain comment
        assert_eq!(split_body("-- \\@nope\n").unwrap()[0], Item::Comment("-- \\@nope".into()));
    }

    #[test]
    fn ordered_inference() {
        let t = |s: &str| infer_ordered(s);
        assert_eq!(t("select a, b from t order by a, b;"), Ordered::Total);
        assert_eq!(t("select a, b from t order by a;"), Ordered::Partial);
        assert_eq!(t("select a, b from t;"), Ordered::None);
        assert_eq!(t("select * from t order by a;"), Ordered::Partial);
        assert_eq!(t("select t.a, count(*) as c from t group by 1 order by 1, c desc nulls last;"), Ordered::Total);
        assert_eq!(t("select t.a, count(*) c from t group by t.a order by a, 2;"), Ordered::Total);
        assert_eq!(t("select x from (select a as x from t order by a) s;"), Ordered::None);
        assert_eq!(t("select x from (select a as x from t order by a) s order by x;"), Ordered::Total);
        assert_eq!(t("select (select max(a) from t) as m, b from u order by 1;"), Ordered::Partial);
        assert_eq!(t("with c as (select a from t) select a from c order by a;"), Ordered::Total);
        assert_eq!(t("with c as (select a from t order by a) select a from c;"), Ordered::None);
        assert_eq!(t("select 1;"), Ordered::Total);
        assert_eq!(t("select cbrt(2::float8)::text;"), Ordered::Total);
        assert_eq!(t("select generate_series(1, 3);"), Ordered::None);
        assert_eq!(t("select count(*) from t;"), Ordered::Total);
        assert_eq!(t("select count(*) from t group by a;"), Ordered::None);
        assert_eq!(t("select count(*) over () from t;"), Ordered::None);
        assert_eq!(t("select a from t union select a from u;"), Ordered::None);
        assert_eq!(t("select a from t union select a from u order by 1;"), Ordered::Total);
        assert_eq!(t("(select a from t) union (select a from u order by a);"), Ordered::None);
        assert_eq!(t("values (1, 2);"), Ordered::Total);
        assert_eq!(t("values (1), (2);"), Ordered::None);
        assert_eq!(t("table t;"), Ordered::None);
        assert_eq!(t("table t order by a;"), Ordered::Partial);
        assert_eq!(t("explain (costs off) select * from t;"), Ordered::Total);
        assert_eq!(t("show work_mem;"), Ordered::Total);
        assert_eq!(t("fetch 1 from c;"), Ordered::Total);
        assert_eq!(t("insert into t values (1) returning a;"), Ordered::None);
        assert_eq!(t("create table t (a int);"), Ordered::None);
        assert_eq!(t("select a from t order by a using <;"), Ordered::Total);
        assert_eq!(t("select a, b from t order by a limit 1;"), Ordered::Partial);
        assert_eq!(t("select distinct on (a) a, b from t order by a, b;"), Ordered::Total);
        assert_eq!(t("select a from t where b = 'order by' order by a;"), Ordered::Total);
        assert_eq!(t("select a from t where b = 'x order by y';"), Ordered::None);
        assert_eq!(t("select 'a', 1 as one;"), Ordered::Total);
        assert_eq!(t("select s.relname, l.mode from pg_locks l join pg_class s on l.relation = s.oid order by 1, 2;"), Ordered::Total);
    }

    #[test]
    fn shapes() {
        assert_eq!(shape_of("with x as (select 1) insert into t select * from x;"), Shape::Other);
        assert_eq!(shape_of("with x as (select 1) select * from x;"), Shape::Select);
        assert_eq!(shape_of("with x as (select 1), select as (select 2) select * from x;"), Shape::Select);
        assert_eq!(shape_of("(select 1) union (select 2);"), Shape::Select);
        assert_eq!(shape_of("EXPLAIN select 1;"), Shape::Sequential);
        assert_eq!(shape_of("update t set a = 1 returning a;"), Shape::Other);
    }

    #[test]
    fn steps_sessions_brackets_and_kinds() {
        let text = format!(
            "{}-- @connect user=u1 db=d1\nbegin;\ninsert into t values (1);\n-- @s2\n-- @hold\nselect a from t order by a;\n-- @s1\n-- @probe locks\ncommit;\nrollback to savepoint x;\n-- @ordered partial\nselect a, b from t order by a, b;\n-- @mark m\n-- @settle done max=10\n-- @storm n=4 hold=1\n-- @pressure mb=1 queries=2\n-- @expect-crash\nselect 1;\ncopy t from stdin;\n1\n\\.\n\\dt\n-- @manual\n-- kick it\n-- @end\n",
            HDR
        );
        let r = Recipe::parse(&text).unwrap();
        let st = r.steps();
        let kinds: Vec<String> = st.iter().map(|s| s.kind.to_string_key()).collect();
        assert_eq!(
            kinds,
            vec![
                "connect", "sql", "sql", "sleep_until_blocked", "sql", "probe:locks", "sql", "sql", "sql", "env:mark", "settle", "storm",
                "pressure", "env:expect-crash", "sql", "copy_in", "env:psql", "env:manual"
            ]
        );
        assert_eq!(st[0].sql.as_deref(), Some("db=d1 user=u1"));
        assert_eq!(st[0].slots["user"], "u1");
        assert_eq!(st[1].bracket.as_deref(), Some("txn1"));
        assert_eq!(st[2].bracket.as_deref(), Some("txn1"));
        assert_eq!(st[3].session, "s2");
        assert_eq!(st[4].session, "s2");
        assert_eq!(st[4].bracket, None);
        assert_eq!(st[4].ordered, Ordered::Total);
        assert_eq!(st[5].session, "s1");
        assert_eq!(st[5].bracket.as_deref(), Some("txn1"));
        assert_eq!(st[6].bracket.as_deref(), Some("txn1"));
        assert_eq!(st[7].bracket, None);
        assert_eq!(st[8].ordered, Ordered::Partial);
        assert_eq!(st[10].sql.as_deref(), Some("done"));
        assert_eq!(st[10].slots["max"], "10");
        assert_eq!(st[11].slots["n"], "4");
        assert_eq!(st[12].slots["queries"], "2");
        assert!(!st[15].wire && !st[16].wire && !st[17].wire);
        assert_eq!(st[17].sql.as_deref(), Some("kick it"));
        assert!(!r.is_wire());
        assert_eq!(r.ordered_summary(), Ordered::Partial);
    }

    #[test]
    fn expect_c_attachment() {
        let text = format!("{}select 1;\nselect 2;\n", HDR).replace("-- ordered: none\n", "-- ordered: total\n-- expect_c: E:22012:division by zero\n");
        let r = Recipe::parse(&text).unwrap();
        let st = r.steps();
        assert_eq!(st[0].expect_c, None);
        assert_eq!(st[1].expect_c.as_deref(), Some("E:22012:division by zero"));
        let text2 = format!("{}-- @expect E:22012:division by zero\nselect 1/0;\nselect 2;\n", HDR);
        let st = Recipe::parse(&text2).unwrap().steps();
        assert_eq!(st[0].expect_c.as_deref(), Some("E:22012:division by zero"));
        assert_eq!(st[1].expect_c, None);
    }

    #[test]
    fn step_record_emission() {
        let text = format!("{}begin;\nselect a from t order by a;\ncommit;\n", HDR).replace("-- targets: -\n", "-- targets: ereport:src/x.c:1, ereport:src/x.c:2\n-- slots: n:int4\n");
        let r = Recipe::parse(&text).unwrap();
        let recs = r.to_steps("scn", 10);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].seq, 10);
        assert_eq!(recs[2].seq, 12);
        assert_eq!(recs[1].kind, StepKind::Sql);
        assert_eq!(recs[1].sql.as_deref(), Some("select a from t order by a;"));
        assert_eq!(recs[1].ordered, Ordered::Total);
        assert_eq!(recs[1].bracket.as_deref(), Some("txn1"));
        assert_eq!(recs[1].recipe.as_deref(), Some("t/x/y"));
        assert_eq!(recs[1].role, "superuser");
        assert_eq!(recs[1].session, "s1");
        assert_eq!(recs[1].targets, vec!["ereport:src/x.c:1".to_string(), "ereport:src/x.c:2".to_string()]);
        assert_eq!(recs[1].slots.get("n").map(String::as_str), Some(""));
        // JSONL round trip through the contract
        let back = StepRecord::from_jsonl(&recs[1].to_jsonl()).unwrap();
        assert_eq!(back, recs[1]);
        // extended protocol → xproto steps
        let r2 = Recipe::parse(&text.replace("-- protocol: simple", "-- protocol: extended")).unwrap();
        let recs2 = r2.to_steps("scn", 0);
        assert_eq!(recs2[1].kind, StepKind::Xproto);
        assert_eq!(recs2[1].xproto.as_ref().unwrap().mode, "parse_bind_execute");
        // unmatched targets ('-') are dropped from the stream
        let r3 = Recipe::parse(&format!("{}select 1;\n", HDR)).unwrap();
        assert!(r3.to_steps("s", 0)[0].targets.is_empty());
    }

    #[test]
    fn render_round_trip_and_idempotent() {
        let text = format!("{}-- note: x\ncreate table t(a int); insert into t values (1);\n\n  select a\n    from t order by a; -- tail\n-- @s2\ndo $$ begin raise notice ';'; end $$;\ncopy t from stdin;\n1\n\\.\n-- @manual\n-- prose\n-- @end\n", HDR);
        let r = Recipe::parse(&text).unwrap();
        let once = r.render();
        assert_ne!(once, text, "non-canonical input is normalized");
        let r2 = Recipe::parse(&once).unwrap();
        assert_eq!(r2, r);
        assert_eq!(r2.render(), once);
        assert!(once.contains("create table t(a int);\ninsert into t values (1);\nselect a\n    from t order by a; -- tail\n-- @s2\n"));
    }

    fn workspace_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().unwrap()
    }

    #[test]
    fn fixture_round_trip() {
        let path = workspace_root().join("crates/bin/fuzzgen/fixtures/contracts/recipe-header.sql");
        let text = std::fs::read_to_string(&path).unwrap();
        let r = Recipe::parse(&text).unwrap();
        assert_eq!(r.render(), text, "fixture is canonical");
        assert_eq!(r.statements().len(), 4);
        let st = r.steps();
        assert_eq!(st[2].sql.as_deref(), Some("analyze verbose :t;"));
        assert_eq!(st[3].expect_c.as_deref(), Some("N:INFO:analyzing \"public.%s\""));
    }
}
