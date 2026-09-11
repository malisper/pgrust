//! Per-side server-log tailer (sitediff plan §4.3, lane L0.3).
//!
//! Every cell runs both servers with stderr-direct logging under the
//! cell's `log_line_prefix` (`%m %b[%p] %q%a ` in the base cell). This
//! module turns a growing log file into `contracts::LogLine` records:
//!
//! * `PrefixSpec` compiles the prefix template into a backtracking matcher
//!   (no regex crate in the workspace) that recovers `%m`/`%t` timestamps,
//!   `%b` backend type, `%p` pid, `%a` application name and `%e` SQLSTATE,
//!   then the `LEVEL:  message` body. `%q` handles the non-session form:
//!   a postmaster/checkpointer line stops the prefix right there.
//! * `LogParser` is the pure line-level state machine: prefixed lines,
//!   tab-indented continuation lines (inherit the previous line's
//!   attribution, as C prints them), the Rust panic marker block
//!   (`thread '...' panicked at <site>:` + message + `panicking backend
//!   query: <q>`, the seams_init hook's `eprintln`), and raw retention for
//!   everything else. The csvlog / jsonlog collector files of the `elog`
//!   cell go through `parse_csv_record` / `parse_json_record` into the
//!   same record shape (`source` = `csvlog` | `jsonlog`).
//! * `Tail` owns the file offset and the parsed line store, hands out
//!   `Mark`s (`@mark` slices: the auth phase has no BackendKeyData so the
//!   connect step is attributed by the mark pair written around it), and
//!   attributes a step's slice by pid.
//! * `prefix_violations` is the prefix invariant, evaluated over the whole
//!   file, never a slice (the memwatchdog boot line is written before the
//!   first mark).
//! * `self_test_sql` / `self_test_verdict` are the boot-time tailer self
//!   test around `pgrust: crash backend <vpid> quit`.
//!
//! Nothing here reads a clock; timestamps are whatever the server printed.

use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::contracts::{json, Bytes, LogLine, Panic};

/// Levels the body parser accepts after the prefix (`LEVEL:  message`).
/// `STATEMENT`, `DETAIL`, `HINT`, `CONTEXT`, `QUERY`, `LOCATION` are the
/// auxiliary lines both engines print with the same prefix.
pub const LEVELS: &[&str] = &[
    "DEBUG5", "DEBUG4", "DEBUG3", "DEBUG2", "DEBUG1", "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL",
    "PANIC", "STATEMENT", "DETAIL", "HINT", "CONTEXT", "QUERY", "LOCATION",
];

/// One `log_line_prefix` escape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    /// `%m` — `YYYY-MM-DD HH:MM:SS.mmm ZONE`.
    TsMillis,
    /// `%t` — `YYYY-MM-DD HH:MM:SS ZONE`.
    TsSecs,
    /// `%n` — epoch `secs.millis`.
    TsEpoch,
    /// `%p`.
    Pid,
    /// `%b`.
    BackendType,
    /// `%a`.
    App,
    /// `%e` — five-character SQLSTATE.
    SqlState,
    /// `%P`, `%l`, `%x` — digits (possibly empty for `%P`).
    Digits,
    /// `%u %d %i %h %r %c %s %v %L` — free text, backtracked against the
    /// following literal.
    Text,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Lit(String),
    Field(Field),
    /// `%q` — the prefix may end here for non-session processes.
    Stop,
}

/// Captures recovered from one prefix match.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Caps {
    ts: Option<String>,
    pid: Option<u32>,
    backend_type: Option<String>,
    app: Option<String>,
    sqlstate: Option<String>,
}

/// A compiled `log_line_prefix`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixSpec {
    template: String,
    tokens: Vec<Token>,
}

impl PrefixSpec {
    /// Compile a `log_line_prefix` template. Unknown escapes are kept as
    /// text fields (C prints them verbatim only for `%%`; anything else
    /// is a GUC error at the server, so a cell never carries one).
    pub fn compile(template: &str) -> PrefixSpec {
        let mut tokens = Vec::new();
        let mut lit = String::new();
        let chars: Vec<char> = template.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c != '%' {
                lit.push(c);
                i += 1;
                continue;
            }
            i += 1;
            // Optional padding: -?[0-9]+ (ignored for matching; the field
            // shapes below absorb padding spaces via the literal boundary).
            if i < chars.len() && chars[i] == '-' {
                i += 1;
            }
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i >= chars.len() {
                lit.push('%');
                break;
            }
            let e = chars[i];
            i += 1;
            if e == '%' {
                lit.push('%');
                continue;
            }
            if !lit.is_empty() {
                tokens.push(Token::Lit(std::mem::take(&mut lit)));
            }
            tokens.push(match e {
                'm' => Token::Field(Field::TsMillis),
                't' => Token::Field(Field::TsSecs),
                'n' => Token::Field(Field::TsEpoch),
                'p' => Token::Field(Field::Pid),
                'b' => Token::Field(Field::BackendType),
                'a' => Token::Field(Field::App),
                'e' => Token::Field(Field::SqlState),
                'P' | 'l' | 'x' => Token::Field(Field::Digits),
                'q' => Token::Stop,
                _ => Token::Field(Field::Text),
            });
        }
        if !lit.is_empty() {
            tokens.push(Token::Lit(lit));
        }
        PrefixSpec { template: template.to_string(), tokens }
    }

    pub fn template(&self) -> &str {
        &self.template
    }

    /// True when the template is empty: nothing to match, nothing to
    /// enforce (the invariant is off).
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Match the prefix at the start of `line`. Returns the captures and
    /// the byte offset where the `LEVEL:  message` body starts.
    fn match_line(&self, line: &str) -> Option<(Caps, usize)> {
        let mut caps = Caps::default();
        let end = self.walk(0, line, 0, &mut caps)?;
        Some((caps, end))
    }

    /// Backtracking walk over the tokens. Text-shaped fields try every
    /// length shortest-first so `%b[` stops at the first `[`.
    fn walk(&self, ti: usize, line: &str, pos: usize, caps: &mut Caps) -> Option<usize> {
        if ti == self.tokens.len() {
            // The prefix ended: the body must start with a level.
            return if body_level(&line[pos..]).is_some() { Some(pos) } else { None };
        }
        let rest = &line[pos..];
        match &self.tokens[ti] {
            Token::Lit(l) => {
                if rest.starts_with(l.as_str()) {
                    self.walk(ti + 1, line, pos + l.len(), caps)
                } else {
                    None
                }
            }
            Token::Stop => {
                // Session form first: the remaining tokens match.
                let saved = caps.clone();
                if let Some(end) = self.walk(ti + 1, line, pos, caps) {
                    return Some(end);
                }
                *caps = saved;
                // Non-session form: the prefix stops here; C emits the
                // level right after (tolerate the trailing-literal space
                // some builds still print).
                let trimmed = rest.trim_start_matches(' ');
                let skipped = rest.len() - trimmed.len();
                if body_level(trimmed).is_some() {
                    Some(pos + skipped)
                } else {
                    None
                }
            }
            Token::Field(f) => {
                let f = *f;
                match f {
                    Field::TsMillis | Field::TsSecs => {
                        let n = timestamp_len(rest, matches!(f, Field::TsMillis))?;
                        let saved = caps.ts.take();
                        caps.ts = Some(rest[..n].to_string());
                        if let Some(end) = self.walk(ti + 1, line, pos + n, caps) {
                            return Some(end);
                        }
                        caps.ts = saved;
                        None
                    }
                    Field::TsEpoch => {
                        let n = epoch_len(rest)?;
                        let saved = caps.ts.take();
                        caps.ts = Some(rest[..n].to_string());
                        if let Some(end) = self.walk(ti + 1, line, pos + n, caps) {
                            return Some(end);
                        }
                        caps.ts = saved;
                        None
                    }
                    Field::Pid => {
                        let n = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
                        if n == 0 {
                            return None;
                        }
                        let saved = caps.pid.take();
                        caps.pid = rest[..n].parse().ok();
                        if let Some(end) = self.walk(ti + 1, line, pos + n, caps) {
                            return Some(end);
                        }
                        caps.pid = saved;
                        None
                    }
                    Field::Digits => {
                        let n = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
                        self.walk(ti + 1, line, pos + n, caps)
                    }
                    Field::SqlState => {
                        if rest.len() < 5 || !rest.as_bytes()[..5].iter().all(|b| b.is_ascii_alphanumeric()) {
                            return None;
                        }
                        let saved = caps.sqlstate.take();
                        caps.sqlstate = Some(rest[..5].to_string());
                        if let Some(end) = self.walk(ti + 1, line, pos + 5, caps) {
                            return Some(end);
                        }
                        caps.sqlstate = saved;
                        None
                    }
                    Field::BackendType | Field::App | Field::Text => {
                        // Shortest-first over char boundaries. `%a` and `%b`
                        // are never empty (an empty application_name prints
                        // as `[unknown]` on both engines), so an empty
                        // capture cannot masquerade as the non-session form.
                        let mut cut = match f {
                            Field::Text => 0usize,
                            _ => rest.chars().next().map(char::len_utf8)?,
                        };
                        loop {
                            let value = &rest[..cut];
                            let saved = match f {
                                Field::BackendType => caps.backend_type.replace(value.to_string()),
                                Field::App => caps.app.replace(value.to_string()),
                                _ => None,
                            };
                            if let Some(end) = self.walk(ti + 1, line, pos + cut, caps) {
                                return Some(end);
                            }
                            match f {
                                Field::BackendType => caps.backend_type = saved,
                                Field::App => caps.app = saved,
                                _ => {}
                            }
                            if cut >= rest.len() {
                                return None;
                            }
                            cut += rest[cut..].chars().next().map(char::len_utf8).unwrap_or(1);
                        }
                    }
                }
            }
        }
    }
}

/// `YYYY-MM-DD HH:MM:SS[.mmm] ZONE` length, if `s` starts with one.
fn timestamp_len(s: &str, millis: bool) -> Option<usize> {
    let b = s.as_bytes();
    let pat: &[u8] = if millis { b"dddd-dd-dd dd:dd:dd.ddd " } else { b"dddd-dd-dd dd:dd:dd " };
    if b.len() < pat.len() {
        return None;
    }
    for (i, p) in pat.iter().enumerate() {
        let ok = match p {
            b'd' => b[i].is_ascii_digit(),
            other => b[i] == *other,
        };
        if !ok {
            return None;
        }
    }
    let zone = s[pat.len()..].bytes().take_while(|c| !c.is_ascii_whitespace()).count();
    if zone == 0 {
        return None;
    }
    Some(pat.len() + zone)
}

/// `secs.mmm` length.
fn epoch_len(s: &str) -> Option<usize> {
    let secs = s.bytes().take_while(|b| b.is_ascii_digit()).count();
    if secs == 0 || !s[secs..].starts_with('.') {
        return None;
    }
    let ms = s[secs + 1..].bytes().take_while(|b| b.is_ascii_digit()).count();
    if ms == 0 {
        return None;
    }
    Some(secs + 1 + ms)
}

/// `(level, message_offset)` when `body` starts with `LEVEL:` and one of
/// the known levels.
fn body_level(body: &str) -> Option<(&'static str, usize)> {
    let colon = body.find(':')?;
    let word = &body[..colon];
    let level = LEVELS.iter().copied().find(|l| *l == word)?;
    let after = &body[colon + 1..];
    let skipped = after.len() - after.trim_start_matches(' ').len();
    Some((level, colon + 1 + skipped))
}

/// What one stderr line turned out to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LineKind {
    /// Matched the prefix.
    Prefixed,
    /// Tab-indented continuation of the previous prefixed line.
    Continuation,
    /// Blank line.
    Blank,
    /// Part of a Rust panic block (marker, message, query, backtrace).
    PanicBlock,
    /// Nothing matched: raw retention only.
    Unparsed,
}

/// A parsed line plus its classification and file line number (1-based).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailLine {
    pub line_no: u64,
    pub kind: LineKind,
    pub rec: LogLine,
}

/// Panic-block parser state.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PanicState {
    Idle,
    /// Saw `panicked at <site>:`; the next line is the message.
    WantMessage { site: String },
    /// Have site + message; waiting for `panicking backend query:` or the
    /// backtrace / note lines that end the block.
    WantQuery { site: String, message: Vec<u8> },
    /// Inside a backtrace (`stack backtrace:` seen); ends at the first
    /// non-indented line.
    Backtrace,
}

/// The pure line-level parser. Feed lines in file order; read back
/// parsed lines, completed panic marker pairs and the attribution state.
#[derive(Clone, Debug)]
pub struct LogParser {
    prefix: PrefixSpec,
    source: String,
    lines: Vec<TailLine>,
    panics: Vec<(u64, Panic)>,
    panic_state: PanicState,
    /// Line number of the open panic block's marker line.
    pending_panic_line: Option<u64>,
    /// The last prefixed line's captures (continuation lines inherit).
    last_caps: Option<Caps>,
}

impl LogParser {
    pub fn new(prefix: PrefixSpec, source: &str) -> LogParser {
        LogParser {
            prefix,
            source: source.to_string(),
            lines: Vec::new(),
            panics: Vec::new(),
            panic_state: PanicState::Idle,
            pending_panic_line: None,
            last_caps: None,
        }
    }

    pub fn prefix(&self) -> &PrefixSpec {
        &self.prefix
    }

    pub fn lines(&self) -> &[TailLine] {
        &self.lines
    }

    /// Completed panic marker pairs, each with the line number of the
    /// `panicked at` line.
    pub fn panics(&self) -> &[(u64, Panic)] {
        &self.panics
    }

    /// Feed one line (without its newline). Bytes are kept raw; parsing
    /// runs over the lossy-UTF-8 view only for field recovery.
    pub fn feed(&mut self, raw: &[u8]) {
        let line_no = self.lines.len() as u64 + 1;
        let text = String::from_utf8_lossy(raw).into_owned();
        let mut rec = LogLine {
            source: self.source.clone(),
            raw: Bytes(raw.to_vec()),
            ts: None,
            backend_type: None,
            pid: None,
            app: None,
            level: None,
            sqlstate: None,
            message: None,
            location: None,
        };

        // Panic block first: its lines never carry the prefix, and the
        // message line can look like anything.
        if let Some(kind) = self.feed_panic(line_no, &text, raw) {
            self.lines.push(TailLine { line_no, kind, rec });
            return;
        }

        if text.is_empty() {
            self.lines.push(TailLine { line_no, kind: LineKind::Blank, rec });
            return;
        }

        if let Some((caps, body_at)) = self.prefix.match_line(&text) {
            let body = &text[body_at..];
            let (level, msg_at) = body_level(body).expect("match_line guarantees a level");
            let msg = &raw[body_at + msg_at..];
            rec.ts = caps.ts.clone();
            rec.backend_type = caps.backend_type.clone();
            rec.pid = caps.pid;
            rec.app = caps.app.clone();
            rec.sqlstate = caps.sqlstate.clone();
            rec.level = Some(level.to_string());
            rec.message = Some(Bytes(msg.to_vec()));
            if level == "LOCATION" {
                rec.location = Some(String::from_utf8_lossy(msg).into_owned());
            }
            self.last_caps = Some(caps);
            self.lines.push(TailLine { line_no, kind: LineKind::Prefixed, rec });
            return;
        }

        if self.prefix.is_empty() {
            // No prefix configured: every line is body-only.
            if let Some((level, msg_at)) = body_level(&text) {
                rec.level = Some(level.to_string());
                rec.message = Some(Bytes(raw[msg_at..].to_vec()));
                self.lines.push(TailLine { line_no, kind: LineKind::Prefixed, rec });
                return;
            }
        }

        if text.starts_with('\t') {
            if let Some(caps) = &self.last_caps {
                rec.ts = caps.ts.clone();
                rec.backend_type = caps.backend_type.clone();
                rec.pid = caps.pid;
                rec.app = caps.app.clone();
            }
            rec.message = Some(Bytes(raw[1..].to_vec()));
            self.lines.push(TailLine { line_no, kind: LineKind::Continuation, rec });
            return;
        }

        self.lines.push(TailLine { line_no, kind: LineKind::Unparsed, rec });
    }

    /// Advance the panic state machine; Some(kind) when the line belongs
    /// to a panic block.
    fn feed_panic(&mut self, line_no: u64, text: &str, raw: &[u8]) -> Option<LineKind> {
        let state = std::mem::replace(&mut self.panic_state, PanicState::Idle);
        match state {
            PanicState::Idle => {
                if let Some(site_or_pair) = parse_panicked_at(text) {
                    match site_or_pair {
                        PanickedAt::Modern { site } => {
                            self.panic_state = PanicState::WantMessage { site };
                        }
                        PanickedAt::Legacy { site, message } => {
                            // Pre-1.73 one-line form: message is inline.
                            self.panic_state =
                                PanicState::WantQuery { site, message: message.into_bytes() };
                        }
                    }
                    self.pending_panic_line = Some(line_no);
                    return Some(LineKind::PanicBlock);
                }
                if let Some(q) = text.strip_prefix("panicking backend query: ") {
                    // Query without a preceding marker (the hook printed it
                    // after a suppressed/foreign panic line): still a pair
                    // member — record with an unknown site.
                    self.panics.push((
                        line_no,
                        Panic { site: String::new(), message: Bytes::default(), query: Some(Bytes::text(q)) },
                    ));
                    return Some(LineKind::PanicBlock);
                }
                if text.starts_with("note: run with") {
                    return Some(LineKind::PanicBlock);
                }
                if text == "stack backtrace:" {
                    self.panic_state = PanicState::Backtrace;
                    return Some(LineKind::PanicBlock);
                }
                None
            }
            PanicState::WantMessage { site } => {
                self.panic_state = PanicState::WantQuery { site, message: raw.to_vec() };
                Some(LineKind::PanicBlock)
            }
            PanicState::WantQuery { site, message } => {
                let start = self.pending_panic_line.take().unwrap_or(line_no);
                if let Some(q) = text.strip_prefix("panicking backend query: ") {
                    self.panics.push((start, Panic { site, message: Bytes(message), query: Some(Bytes::text(q)) }));
                    return Some(LineKind::PanicBlock);
                }
                // The block ended without a query line: close the pair
                // with query = None, then re-dispatch this line.
                self.panics.push((start, Panic { site, message: Bytes(message), query: None }));
                if text.starts_with("note: run with") {
                    return Some(LineKind::PanicBlock);
                }
                if text == "stack backtrace:" {
                    self.panic_state = PanicState::Backtrace;
                    return Some(LineKind::PanicBlock);
                }
                // Not a panic line after all; fall through to normal
                // handling by returning None (state already Idle).
                self.feed_panic_idle_retry(line_no, text)
            }
            PanicState::Backtrace => {
                if text.starts_with(' ') || text.starts_with('\t') || text.starts_with("note: run with") {
                    self.panic_state = PanicState::Backtrace;
                    return Some(LineKind::PanicBlock);
                }
                self.feed_panic_idle_retry(line_no, text)
            }
        }
    }

    /// Re-dispatch a line in the Idle state (after a block closed).
    fn feed_panic_idle_retry(&mut self, line_no: u64, text: &str) -> Option<LineKind> {
        self.panic_state = PanicState::Idle;
        // Only the Idle arm's own checks; `raw` is not needed there.
        if let Some(p) = parse_panicked_at(text) {
            match p {
                PanickedAt::Modern { site } => self.panic_state = PanicState::WantMessage { site },
                PanickedAt::Legacy { site, message } => {
                    self.panic_state = PanicState::WantQuery { site, message: message.into_bytes() }
                }
            }
            self.pending_panic_line = Some(line_no);
            return Some(LineKind::PanicBlock);
        }
        if text == "stack backtrace:" {
            self.panic_state = PanicState::Backtrace;
            return Some(LineKind::PanicBlock);
        }
        if let Some(q) = text.strip_prefix("panicking backend query: ") {
            self.panics.push((
                line_no,
                Panic { site: String::new(), message: Bytes::default(), query: Some(Bytes::text(q)) },
            ));
            return Some(LineKind::PanicBlock);
        }
        None
    }

    /// Flush a dangling panic pair at end of input (a `panicked at` with a
    /// message but no query line yet, e.g. the file ends mid-block).
    pub fn finish(&mut self) {
        let state = std::mem::replace(&mut self.panic_state, PanicState::Idle);
        if let PanicState::WantQuery { site, message } = state {
            let start = self.pending_panic_line.take().unwrap_or(self.lines.len() as u64);
            self.panics.push((start, Panic { site, message: Bytes(message), query: None }));
        }
    }
}

/// The two shapes of the Rust default hook's marker line.
enum PanickedAt {
    /// `thread 'x' panicked at src/lib.rs:12:3:` (message on the next line).
    Modern { site: String },
    /// `thread 'x' panicked at 'msg', src/lib.rs:12:3` (pre-1.73).
    Legacy { site: String, message: String },
}

fn parse_panicked_at(text: &str) -> Option<PanickedAt> {
    let idx = text.find(" panicked at ")?;
    if !text.starts_with("thread '") && !text.starts_with("thread ") {
        return None;
    }
    let after = &text[idx + " panicked at ".len()..];
    if let Some(rest) = after.strip_prefix('\'') {
        // Legacy: 'message', site
        let close = rest.rfind("', ")?;
        let message = rest[..close].to_string();
        let site = rest[close + 3..].trim_end_matches(':').to_string();
        return Some(PanickedAt::Legacy { site, message });
    }
    let site = after.trim_end().trim_end_matches(':').to_string();
    if site.is_empty() {
        return None;
    }
    Some(PanickedAt::Modern { site })
}

// ---------------------------------------------------------------------
// Collector files (the `elog` cell): csvlog + jsonlog
// ---------------------------------------------------------------------

/// PostgreSQL 18 csvlog column order (`log_destination = csvlog`).
pub const CSV_COLUMNS: &[&str] = &[
    "log_time", "user_name", "database_name", "process_id", "connection_from", "session_id", "session_line_num",
    "command_tag", "session_start_time", "virtual_transaction_id", "transaction_id", "error_severity",
    "sql_state_code", "message", "detail", "hint", "internal_query", "internal_query_pos", "context", "query",
    "query_pos", "location", "application_name", "backend_type", "leader_pid", "query_id",
];

/// Split one CSV record (which may span physical lines inside quotes)
/// into fields. Returns None when the record is unterminated (a quoted
/// field runs past the end: the caller buffers more input).
pub fn split_csv_record(rec: &str) -> Option<Vec<String>> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut chars = rec.chars().peekable();
    let mut in_quotes = false;
    let mut quoted_field = false;
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' if cur.is_empty() && !quoted_field => {
                    in_quotes = true;
                    quoted_field = true;
                }
                ',' => {
                    fields.push(std::mem::take(&mut cur));
                    quoted_field = false;
                }
                _ => cur.push(c),
            }
        }
    }
    if in_quotes {
        return None;
    }
    fields.push(cur);
    Some(fields)
}

/// One csvlog record → a `LogLine` with `source = csvlog`. Fields beyond
/// the fixed column set are ignored; a short record keeps only `raw`.
pub fn parse_csv_record(rec: &str) -> LogLine {
    let mut out = LogLine {
        source: "csvlog".into(),
        raw: Bytes::text(rec),
        ts: None,
        backend_type: None,
        pid: None,
        app: None,
        level: None,
        sqlstate: None,
        message: None,
        location: None,
    };
    let Some(fields) = split_csv_record(rec) else { return out };
    let col = |name: &str| -> Option<&str> {
        let i = CSV_COLUMNS.iter().position(|c| *c == name)?;
        fields.get(i).map(String::as_str)
    };
    if fields.len() < 14 {
        return out;
    }
    out.ts = col("log_time").filter(|s| !s.is_empty()).map(str::to_string);
    out.pid = col("process_id").and_then(|s| s.parse().ok());
    out.level = col("error_severity").filter(|s| !s.is_empty()).map(str::to_string);
    out.sqlstate = col("sql_state_code").filter(|s| !s.is_empty()).map(str::to_string);
    out.message = col("message").map(Bytes::text);
    out.location = col("location").filter(|s| !s.is_empty()).map(str::to_string);
    out.app = col("application_name").filter(|s| !s.is_empty()).map(str::to_string);
    out.backend_type = col("backend_type").filter(|s| !s.is_empty()).map(str::to_string);
    out
}

/// One jsonlog line → a `LogLine` with `source = jsonlog`.
pub fn parse_json_record(rec: &str) -> LogLine {
    let mut out = LogLine {
        source: "jsonlog".into(),
        raw: Bytes::text(rec),
        ts: None,
        backend_type: None,
        pid: None,
        app: None,
        level: None,
        sqlstate: None,
        message: None,
        location: None,
    };
    let Ok(v) = json::parse(rec) else { return out };
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    out.ts = s("timestamp");
    out.pid = v.get("pid").and_then(|x| x.as_i64()).map(|p| p as u32);
    out.level = s("error_severity");
    out.sqlstate = s("state_code");
    out.message = s("message").map(|m| Bytes::text(&m));
    out.app = s("application_name");
    out.backend_type = s("backend_type");
    let func = s("func_name");
    let file = s("file_name");
    let line = v.get("file_line_num").and_then(|x| x.as_i64());
    if file.is_some() || func.is_some() {
        out.location = Some(format!(
            "{}, {}:{}",
            func.unwrap_or_default(),
            file.unwrap_or_default(),
            line.map(|l| l.to_string()).unwrap_or_default()
        ));
    }
    out
}

// ---------------------------------------------------------------------
// Marks, slices, the prefix invariant
// ---------------------------------------------------------------------

/// A position in the parsed line store (`@mark`): the count of lines
/// parsed when the mark was taken. Slices are half-open `[from, to)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mark(pub u64);

/// A prefix-invariant violation: a B line under a configured prefix that
/// matched nothing (evaluated over the whole file).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixViolation {
    pub line_no: u64,
    pub raw: Bytes,
}

/// Lines that break the prefix invariant. Blank, continuation and panic
/// block lines are not violations (C prints the first two without a
/// prefix; the panic block is its own finding).
pub fn prefix_violations(prefix: &PrefixSpec, lines: &[TailLine]) -> Vec<PrefixViolation> {
    if prefix.is_empty() {
        return Vec::new();
    }
    lines
        .iter()
        .filter(|l| l.kind == LineKind::Unparsed)
        .map(|l| PrefixViolation { line_no: l.line_no, raw: l.rec.raw.clone() })
        .collect()
}

/// The lines of `[from, to)` attributed to `pid`: prefixed lines carrying
/// that pid, continuation lines under them, plus every line without a
/// pid (panic block, unparsed) — those have no other home and the panic
/// pair must land on the step that caused it.
pub fn slice_for_pid(lines: &[TailLine], from: Mark, to: Mark, pid: Option<u32>) -> Vec<LogLine> {
    lines
        .iter()
        .filter(|l| l.line_no > from.0 && l.line_no <= to.0)
        .filter(|l| match l.rec.pid {
            Some(p) => pid == Some(p),
            None => true,
        })
        .map(|l| l.rec.clone())
        .collect()
}

/// `slice_for_pid` for a set of pids (the step's backend plus a backend
/// a disconnect step just dropped); unattributed lines are kept.
pub fn slice_for_pids(lines: &[TailLine], from: Mark, to: Mark, pids: &[u32]) -> Vec<LogLine> {
    lines
        .iter()
        .filter(|l| l.line_no > from.0 && l.line_no <= to.0)
        .filter(|l| l.rec.pid.is_none_or(|p| pids.contains(&p)))
        .map(|l| l.rec.clone())
        .collect()
}

/// Every line of `[from, to)` regardless of pid: the `@mark` slice used
/// for the auth phase of a connect step (no BackendKeyData yet) and for
/// restarts.
pub fn slice_all(lines: &[TailLine], from: Mark, to: Mark) -> Vec<LogLine> {
    lines.iter().filter(|l| l.line_no > from.0 && l.line_no <= to.0).map(|l| l.rec.clone()).collect()
}

/// Panic pairs whose marker line falls in `(from, to]`.
pub fn panics_in(parser: &LogParser, from: Mark, to: Mark) -> Vec<Panic> {
    parser.panics().iter().filter(|(n, _)| *n > from.0 && *n <= to.0).map(|(_, p)| p.clone()).collect()
}

/// Pids seen in `(from, to]` (which backends wrote in the window).
pub fn pids_in(lines: &[TailLine], from: Mark, to: Mark) -> BTreeSet<u32> {
    lines.iter().filter(|l| l.line_no > from.0 && l.line_no <= to.0).filter_map(|l| l.rec.pid).collect()
}

// ---------------------------------------------------------------------
// Death / restart witnesses in the log stream (shared with supervisor)
// ---------------------------------------------------------------------

/// Server-lifecycle lines the supervisor keys on. Both engines print the
/// C postmaster's texts (`postmaster/src/lib.rs:630`,
/// `statemachine.rs:354`, `startup.rs`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Witness {
    /// `server process (PID n) was terminated by signal s[: name]`.
    TerminatedBySignal { pid: Option<u32>, signal: String },
    /// `all server processes terminated; reinitializing`.
    Reinitializing,
    /// `database system was not properly shut down; automatic recovery in progress`.
    RecoveryInProgress,
    /// `redo starts at X/Y`.
    RedoStartsAt(String),
    /// `database system is ready to accept connections`.
    ReadyToAccept,
    /// `database system is shut down`.
    ShutDown,
    /// pgrust's own crash-handler line: `pgrust: FATAL: server process was terminated by signal ...`.
    CrashHandler(String),
}

/// Recognize a lifecycle witness in one log message (prefix already
/// stripped, or the raw line — both work since the texts are searched).
pub fn witness_of(text: &str) -> Option<Witness> {
    if let Some(i) = text.find("was terminated by signal ") {
        let head = &text[..i];
        let pid = head.rfind("(PID ").and_then(|p| {
            let rest = &head[p + 5..];
            let n: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            n.parse().ok()
        });
        let sig = text[i + "was terminated by signal ".len()..].trim().to_string();
        if text.starts_with("pgrust: FATAL") || text.contains("pgrust: FATAL: server process") {
            return Some(Witness::CrashHandler(sig));
        }
        return Some(Witness::TerminatedBySignal { pid, signal: sig });
    }
    if text.contains("all server processes terminated; reinitializing") {
        return Some(Witness::Reinitializing);
    }
    if text.contains("database system was not properly shut down; automatic recovery in progress") {
        return Some(Witness::RecoveryInProgress);
    }
    if let Some(i) = text.find("redo starts at ") {
        let lsn: String = text[i + "redo starts at ".len()..].chars().take_while(|c| !c.is_whitespace()).collect();
        return Some(Witness::RedoStartsAt(lsn));
    }
    if text.contains("database system is ready to accept connections") {
        return Some(Witness::ReadyToAccept);
    }
    if text.contains("database system is shut down") {
        return Some(Witness::ShutDown);
    }
    None
}

/// Signal name for the number/text a `terminated by signal` line carries
/// (`6: Abort trap` → `SIGABRT`; an unknown number is returned as text).
pub fn signal_name(sig: &str) -> String {
    let num: String = sig.chars().take_while(|c| c.is_ascii_digit()).collect();
    match num.as_str() {
        "1" => "SIGHUP",
        "2" => "SIGINT",
        "3" => "SIGQUIT",
        "4" => "SIGILL",
        "5" => "SIGTRAP",
        "6" => "SIGABRT",
        "7" => "SIGBUS",
        "8" => "SIGFPE",
        "9" => "SIGKILL",
        "10" => "SIGUSR1",
        "11" => "SIGSEGV",
        "13" => "SIGPIPE",
        "15" => "SIGTERM",
        _ => return sig.to_string(),
    }
    .to_string()
}

// ---------------------------------------------------------------------
// Boot-time self test: `pgrust: crash backend <vpid> quit`
// ---------------------------------------------------------------------

/// The self-test statement (simple_query.rs:391, `PGRUST_CRASH_TEST`-gated
/// on the server). Issued on a throwaway session against the vpid of a
/// second throwaway session; the tailer must see that backend's death.
pub fn self_test_sql(vpid: u32) -> String {
    format!("pgrust: crash backend {vpid} quit")
}

/// Outcome of the tailer self test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelfTest {
    /// The tailer observed a death witness (or panic pair) in the slice.
    pub witnessed: bool,
    /// The witness lines in the slice.
    pub witness_lines: Vec<Bytes>,
    /// True when the target vpid appeared in the slice (pid attribution
    /// works end to end).
    pub pid_attributed: bool,
}

/// Judge the slice `(from, to]` written around the self-test statement.
pub fn self_test_verdict(parser: &LogParser, from: Mark, to: Mark, vpid: u32) -> SelfTest {
    let lines = parser.lines();
    let mut witness_lines = Vec::new();
    let mut pid_attributed = false;
    for l in lines.iter().filter(|l| l.line_no > from.0 && l.line_no <= to.0) {
        let text = String::from_utf8_lossy(&l.rec.raw.0);
        if witness_of(&text).is_some() {
            witness_lines.push(l.rec.raw.clone());
        }
        if l.rec.pid == Some(vpid) || text.contains(&format!("(PID {vpid})")) || text.contains(&format!("[{vpid}]")) {
            pid_attributed = true;
        }
    }
    let panicked = !panics_in(parser, from, to).is_empty();
    SelfTest { witnessed: !witness_lines.is_empty() || panicked, witness_lines, pid_attributed }
}

// ---------------------------------------------------------------------
// File tailer
// ---------------------------------------------------------------------

/// Which collector-file flavour a path holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    Stderr,
    CsvLog,
    JsonLog,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Stderr => "stderr",
            Source::CsvLog => "csvlog",
            Source::JsonLog => "jsonlog",
        }
    }
}

/// A tailer over one growing file: remembers the byte offset and the
/// partial trailing line, feeds complete lines to the parser.
#[derive(Debug)]
pub struct Tail {
    path: PathBuf,
    source: Source,
    offset: u64,
    partial: Vec<u8>,
    /// Buffered csv text for a record spanning lines.
    csv_pending: String,
    parser: LogParser,
}

impl Tail {
    pub fn new(path: &Path, source: Source, prefix: &PrefixSpec) -> Tail {
        Tail {
            path: path.to_path_buf(),
            source,
            offset: 0,
            partial: Vec::new(),
            csv_pending: String::new(),
            parser: LogParser::new(prefix.clone(), source.as_str()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn parser(&self) -> &LogParser {
        &self.parser
    }

    pub fn parser_mut(&mut self) -> &mut LogParser {
        &mut self.parser
    }

    /// Current mark (`@mark`): the number of lines parsed so far.
    pub fn mark(&self) -> Mark {
        Mark(self.parser.lines().len() as u64)
    }

    /// Read whatever the file grew by and parse the complete lines.
    /// A missing file is not an error (the collector creates it lazily).
    pub fn poll(&mut self) -> std::io::Result<usize> {
        let mut f = match std::fs::File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let len = f.metadata()?.len();
        if len < self.offset {
            // Truncated/rotated: start over at the new file's head.
            self.offset = 0;
            self.partial.clear();
        }
        f.seek(SeekFrom::Start(self.offset))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        self.offset += buf.len() as u64;
        Ok(self.feed_bytes(&buf))
    }

    /// Feed bytes as if read from the file (tests, canned logs). Returns
    /// the number of complete lines parsed.
    pub fn feed_bytes(&mut self, bytes: &[u8]) -> usize {
        let mut n = 0;
        let mut data = std::mem::take(&mut self.partial);
        data.extend_from_slice(bytes);
        let mut start = 0;
        while let Some(i) = data[start..].iter().position(|b| *b == b'\n') {
            let mut line = &data[start..start + i];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            self.feed_line(line);
            n += 1;
            start += i + 1;
        }
        self.partial = data[start..].to_vec();
        n
    }

    fn feed_line(&mut self, line: &[u8]) {
        match self.source {
            Source::Stderr => self.parser.feed(line),
            Source::CsvLog => {
                let text = String::from_utf8_lossy(line);
                if !self.csv_pending.is_empty() {
                    self.csv_pending.push('\n');
                }
                self.csv_pending.push_str(&text);
                if split_csv_record(&self.csv_pending).is_some() {
                    let rec = std::mem::take(&mut self.csv_pending);
                    let parsed = parse_csv_record(&rec);
                    self.parser.push_collector(parsed);
                }
            }
            Source::JsonLog => {
                let text = String::from_utf8_lossy(line);
                let parsed = parse_json_record(&text);
                self.parser.push_collector(parsed);
            }
        }
    }

    /// Flush a dangling final line (no trailing newline) and any open
    /// panic block. Call at stream end / before a restart.
    pub fn finish(&mut self) {
        if !self.partial.is_empty() {
            let line = std::mem::take(&mut self.partial);
            self.feed_line(&line);
        }
        self.parser.finish();
    }
}

impl LogParser {
    /// Store a collector-file record (already parsed by its own format).
    /// Collector records are `Prefixed`-kind: the fixed column set is the
    /// prefix's equivalent, and they never enter the prefix invariant.
    pub fn push_collector(&mut self, rec: LogLine) {
        let line_no = self.lines.len() as u64 + 1;
        self.lines.push(TailLine { line_no, kind: LineKind::Prefixed, rec });
    }
}

/// The collector files of a data directory's `log/` dir (the `elog` cell:
/// `log_destination = 'stderr,csvlog,jsonlog'`), sorted by name.
pub fn collector_files(log_dir: &Path) -> Vec<(PathBuf, Source)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(log_dir) else { return out };
    for e in rd.flatten() {
        let p = e.path();
        let src = match p.extension().and_then(|x| x.to_str()) {
            Some("csv") => Source::CsvLog,
            Some("json") => Source::JsonLog,
            _ => continue,
        };
        out.push((p, src));
    }
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "%m %b[%p] %q%a ";

    fn parse_all(text: &str) -> LogParser {
        let mut p = LogParser::new(PrefixSpec::compile(PREFIX), "stderr");
        for line in text.lines() {
            p.feed(line.as_bytes());
        }
        p.finish();
        p
    }

    #[test]
    fn compile_tokens() {
        let s = PrefixSpec::compile(PREFIX);
        assert_eq!(
            s.tokens,
            vec![
                Token::Field(Field::TsMillis),
                Token::Lit(" ".into()),
                Token::Field(Field::BackendType),
                Token::Lit("[".into()),
                Token::Field(Field::Pid),
                Token::Lit("] ".into()),
                Token::Stop,
                Token::Field(Field::App),
                Token::Lit(" ".into()),
            ]
        );
        let s2 = PrefixSpec::compile("%m [%p] %%x %e ");
        assert!(matches!(s2.tokens[3], Token::Lit(ref l) if l == "] %x "));
        assert!(matches!(s2.tokens[4], Token::Field(Field::SqlState)));
    }

    #[test]
    fn c_session_line_parses() {
        // The C 18.6 shape under the base cell (app name empty prints [unknown]).
        let p = parse_all(
            "2026-09-02 10:20:41.902 PDT client backend[41233] [unknown] ERROR:  relation \"nope\" does not exist at character 15",
        );
        let l = &p.lines()[0];
        assert_eq!(l.kind, LineKind::Prefixed);
        assert_eq!(l.rec.ts.as_deref(), Some("2026-09-02 10:20:41.902 PDT"));
        assert_eq!(l.rec.backend_type.as_deref(), Some("client backend"));
        assert_eq!(l.rec.pid, Some(41233));
        assert_eq!(l.rec.app.as_deref(), Some("[unknown]"));
        assert_eq!(l.rec.level.as_deref(), Some("ERROR"));
        assert_eq!(
            l.rec.message,
            Some(Bytes::text("relation \"nope\" does not exist at character 15"))
        );
        assert!(l.rec.location.is_none());
    }

    #[test]
    fn app_name_with_spaces_and_location_line() {
        let p = parse_all(
            "2026-09-02 10:20:41.902 PDT client backend[7] my app name STATEMENT:  SELECT 1\n\
             2026-09-02 10:20:41.902 PDT client backend[7] my app name LOCATION:  exec_simple_query, postgres.c:1234",
        );
        assert_eq!(p.lines()[0].rec.app.as_deref(), Some("my app name"));
        assert_eq!(p.lines()[0].rec.level.as_deref(), Some("STATEMENT"));
        assert_eq!(p.lines()[1].rec.location.as_deref(), Some("exec_simple_query, postgres.c:1234"));
    }

    #[test]
    fn non_session_lines_stop_at_q() {
        // One space (C's %q returns before the trailing literal) and the
        // two-space variant both parse; pid attribution intact.
        for line in [
            "2026-09-02 10:20:41.903 PDT postmaster[41200] LOG:  all server processes terminated; reinitializing",
            "2026-09-02 10:20:41.903 PDT postmaster[41200]  LOG:  all server processes terminated; reinitializing",
            "2026-09-02 10:20:41.903 PDT checkpointer[41201] LOG:  checkpoint starting: shutdown immediate",
        ] {
            let p = parse_all(line);
            let l = &p.lines()[0];
            assert_eq!(l.kind, LineKind::Prefixed, "{line}");
            assert!(l.rec.app.is_none(), "{line}");
            assert_eq!(l.rec.level.as_deref(), Some("LOG"));
            assert!(l.rec.pid.is_some());
        }
    }

    #[test]
    fn pgrust_shapes_parse_identically() {
        // pgrust prints the same C text through elog/report.rs; the
        // synthetic MyProcPid is the number in brackets.
        let p = parse_all(
            "2026-09-02 10:20:41.902 America/Los_Angeles client backend[19] [unknown] WARNING:  something\n\
             \tcontinuation of the warning\n\
             2026-09-02 10:20:41.902 America/Los_Angeles not initialized[0] LOG:  boot line",
        );
        assert_eq!(p.lines()[0].rec.pid, Some(19));
        assert_eq!(p.lines()[0].rec.ts.as_deref(), Some("2026-09-02 10:20:41.902 America/Los_Angeles"));
        assert_eq!(p.lines()[1].kind, LineKind::Continuation);
        assert_eq!(p.lines()[1].rec.pid, Some(19), "continuation inherits attribution");
        assert_eq!(p.lines()[1].rec.message, Some(Bytes::text("continuation of the warning")));
        assert_eq!(p.lines()[2].rec.backend_type.as_deref(), Some("not initialized"));
    }

    #[test]
    fn panic_pair_is_detected_and_pinned_to_fixture_shape() {
        let p = parse_all(
            "thread 'backend-19' panicked at crates/backend/commands/analyze/src/lib.rs:475:17:\n\
             index has 2 expressions but indexprs lists 1\n\
             panicking backend query: ANALYZE t\n\
             note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n\
             2026-09-02 10:20:41.902 PDT postmaster[41200] LOG:  server process (PID 41233) was terminated by signal 6: Abort trap",
        );
        assert_eq!(p.panics().len(), 1);
        let (line_no, panic) = &p.panics()[0];
        assert_eq!(*line_no, 1);
        assert_eq!(
            panic,
            &Panic {
                site: "crates/backend/commands/analyze/src/lib.rs:475:17".into(),
                message: Bytes::text("index has 2 expressions but indexprs lists 1"),
                query: Some(Bytes::text("ANALYZE t")),
            }
        );
        for l in &p.lines()[..4] {
            assert_eq!(l.kind, LineKind::PanicBlock, "{:?}", l.rec.raw);
        }
        assert_eq!(p.lines()[4].kind, LineKind::Prefixed);
        // The panic block never counts as a prefix violation.
        assert!(prefix_violations(p.prefix(), p.lines()).is_empty());
        // Fixture parity: the ObservationRecord panic object.
        let v = crate::contracts::json::parse(include_str!("../fixtures/contracts/observation-crash.json")).unwrap();
        let fp = v.get("panic").unwrap();
        assert_eq!(fp.get("site").unwrap().as_str(), Some(panic.site.as_str()));
        assert_eq!(fp.get("query").unwrap().as_str(), Some("ANALYZE t"));
        assert_eq!(fp.get("message").unwrap().as_str(), Some("index has 2 expressions but indexprs lists 1"));
    }

    #[test]
    fn panic_without_query_and_with_backtrace_closes_at_next_prefixed_line() {
        let p = parse_all(
            "thread 'backend-3' panicked at src/x.rs:1:2:\n\
             boom\n\
             stack backtrace:\n\
             \x20  0: std::panicking::begin_panic\n\
             \x20        at /rustc/abc/library/std/src/panicking.rs:1\n\
             2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  after",
        );
        assert_eq!(p.panics().len(), 1);
        assert_eq!(p.panics()[0].1.query, None);
        assert_eq!(p.panics()[0].1.message, Bytes::text("boom"));
        assert_eq!(p.lines()[5].kind, LineKind::Prefixed);
        assert!(prefix_violations(p.prefix(), p.lines()).is_empty());
    }

    #[test]
    fn legacy_one_line_panic_form() {
        let p = parse_all("thread 'main' panicked at 'index out of bounds', src/lib.rs:9:5\npanicking backend query: SELECT 1");
        assert_eq!(p.panics().len(), 1);
        assert_eq!(p.panics()[0].1.site, "src/lib.rs:9:5");
        assert_eq!(p.panics()[0].1.message, Bytes::text("index out of bounds"));
        assert_eq!(p.panics()[0].1.query, Some(Bytes::text("SELECT 1")));
    }

    #[test]
    fn prefix_invariant_flags_unprefixed_boot_line_only() {
        let p = parse_all(
            "memwatchdog: armed at 512 MiB\n\
             \n\
             2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  starting pgrust\n\
             \tsome continuation",
        );
        let v = prefix_violations(p.prefix(), p.lines());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].line_no, 1);
        assert_eq!(v[0].raw, Bytes::text("memwatchdog: armed at 512 MiB"));
        assert_eq!(p.lines()[0].kind, LineKind::Unparsed);
        assert!(p.lines()[0].rec.pid.is_none() && p.lines()[0].rec.level.is_none());
        // No configured prefix: nothing to enforce.
        let empty = PrefixSpec::compile("");
        assert!(prefix_violations(&empty, p.lines()).is_empty());
    }

    #[test]
    fn empty_prefix_parses_body_only() {
        let mut p = LogParser::new(PrefixSpec::compile(""), "stderr");
        p.feed(b"LOG:  database system is ready to accept connections");
        assert_eq!(p.lines()[0].kind, LineKind::Prefixed);
        assert_eq!(p.lines()[0].rec.level.as_deref(), Some("LOG"));
    }

    #[test]
    fn marks_slice_by_pid_and_keep_unattributed_lines() {
        let mut t = Tail::new(Path::new("/nonexistent/b.log"), Source::Stderr, &PrefixSpec::compile(PREFIX));
        t.feed_bytes(b"2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  boot\n");
        let m0 = t.mark();
        t.feed_bytes(
            b"2026-09-02 10:20:41.902 PDT client backend[19] [unknown] WARNING:  w1\n\
              2026-09-02 10:20:41.902 PDT client backend[20] [unknown] WARNING:  w2\n\
              thread 'backend-19' panicked at src/a.rs:1:1:\nmsg\npanicking backend query: SELECT 1\n\
              2026-09-02 10:20:41.902 PDT client backend[19] [unknown] ERROR:  e1\n",
        );
        let m1 = t.mark();
        t.feed_bytes(b"2026-09-02 10:20:41.902 PDT client backend[19] [unknown] LOG:  after\n");
        let s19 = slice_for_pid(t.parser().lines(), m0, m1, Some(19));
        let raws: Vec<String> = s19.iter().map(|l| String::from_utf8_lossy(&l.raw.0).into_owned()).collect();
        assert_eq!(raws.len(), 5, "{raws:?}");
        assert!(raws.iter().all(|r| !r.contains("w2") && !r.contains("after")));
        assert!(raws[1].starts_with("thread 'backend-19' panicked"));
        assert_eq!(slice_all(t.parser().lines(), m0, m1).len(), 6);
        assert_eq!(panics_in(t.parser(), m0, m1).len(), 1);
        assert!(panics_in(t.parser(), m1, t.mark()).is_empty());
        assert_eq!(pids_in(t.parser().lines(), m0, m1).into_iter().collect::<Vec<_>>(), vec![19, 20]);
        // A partial trailing line waits for its newline.
        t.feed_bytes(b"2026-09-02 10:20:41.902 PDT client backend[19] [unknown] LOG:  part");
        assert_eq!(t.mark(), Mark(8));
        t.feed_bytes(b"ial\n");
        assert_eq!(t.mark(), Mark(9));
        assert_eq!(t.parser().lines()[8].rec.message, Some(Bytes::text("partial")));
    }

    #[test]
    fn csv_record_parses_fixed_columns() {
        let rec = "2026-09-02 10:20:41.902 PDT,\"fuzz\",\"fuzz\",41233,\"[local]\",68b7d4c9.a111,3,\"SELECT\",2026-09-02 10:20:40 PDT,3/12,0,ERROR,42P01,\"relation \"\"nope\"\" does not exist\",,,,,,\"SELECT * FROM nope\",15,\"parserOpenTable, parse_relation.c:1449\",\"psql\",\"client backend\",,0";
        let l = parse_csv_record(rec);
        assert_eq!(l.source, "csvlog");
        assert_eq!(l.pid, Some(41233));
        assert_eq!(l.level.as_deref(), Some("ERROR"));
        assert_eq!(l.sqlstate.as_deref(), Some("42P01"));
        assert_eq!(l.message, Some(Bytes::text("relation \"nope\" does not exist")));
        assert_eq!(l.location.as_deref(), Some("parserOpenTable, parse_relation.c:1449"));
        assert_eq!(l.app.as_deref(), Some("psql"));
        assert_eq!(l.backend_type.as_deref(), Some("client backend"));
        assert_eq!(l.ts.as_deref(), Some("2026-09-02 10:20:41.902 PDT"));
        // Unterminated quote: the record spans lines.
        assert!(split_csv_record("a,\"multi\nline").is_none());
        assert_eq!(split_csv_record("a,\"multi\nline\",c").unwrap(), vec!["a", "multi\nline", "c"]);
        // A short record keeps raw only.
        let short = parse_csv_record("a,b,c");
        assert!(short.pid.is_none() && short.level.is_none());
        assert_eq!(short.raw, Bytes::text("a,b,c"));
    }

    #[test]
    fn csv_tail_buffers_multiline_records() {
        let mut t = Tail::new(Path::new("/nonexistent/x.csv"), Source::CsvLog, &PrefixSpec::compile(PREFIX));
        t.feed_bytes(b"2026-09-02 10:20:41.902 PDT,\"fuzz\",\"fuzz\",5,\"[local]\",x.y,3,\"SELECT\",2026-09-02 10:20:40 PDT,3/12,0,LOG,00000,\"line one\nline two\",,,,,,,,,\"psql\",\"client backend\",,0\n");
        assert_eq!(t.mark(), Mark(1));
        assert_eq!(t.parser().lines()[0].rec.message, Some(Bytes::text("line one\nline two")));
        assert_eq!(t.parser().lines()[0].rec.pid, Some(5));
    }

    #[test]
    fn json_record_parses() {
        let rec = r#"{"timestamp":"2026-09-02 10:20:41.902 PDT","user":"fuzz","dbname":"fuzz","pid":41233,"remote_host":"[local]","session_id":"68b7d4c9.a111","line_num":3,"ps":"SELECT","session_start":"2026-09-02 10:20:40 PDT","vxid":"3/12","txid":0,"error_severity":"ERROR","state_code":"42P01","message":"relation \"nope\" does not exist","statement":"SELECT * FROM nope","cursor_position":15,"func_name":"parserOpenTable","file_name":"parse_relation.c","file_line_num":1449,"application_name":"psql","backend_type":"client backend","query_id":0}"#;
        let l = parse_json_record(rec);
        assert_eq!(l.source, "jsonlog");
        assert_eq!(l.pid, Some(41233));
        assert_eq!(l.level.as_deref(), Some("ERROR"));
        assert_eq!(l.sqlstate.as_deref(), Some("42P01"));
        assert_eq!(l.message, Some(Bytes::text("relation \"nope\" does not exist")));
        assert_eq!(l.location.as_deref(), Some("parserOpenTable, parse_relation.c:1449"));
        assert_eq!(l.backend_type.as_deref(), Some("client backend"));
        let bad = parse_json_record("not json");
        assert!(bad.pid.is_none());
        assert_eq!(bad.raw, Bytes::text("not json"));
    }

    #[test]
    fn witnesses_and_signal_names() {
        assert_eq!(
            witness_of("server process (PID 41233) was terminated by signal 6: Abort trap"),
            Some(Witness::TerminatedBySignal { pid: Some(41233), signal: "6: Abort trap".into() })
        );
        assert_eq!(witness_of("all server processes terminated; reinitializing"), Some(Witness::Reinitializing));
        assert_eq!(witness_of("redo starts at 0/1A2B3C4"), Some(Witness::RedoStartsAt("0/1A2B3C4".into())));
        assert_eq!(witness_of("database system is ready to accept connections"), Some(Witness::ReadyToAccept));
        assert_eq!(
            witness_of("pgrust: FATAL: server process was terminated by signal 11"),
            Some(Witness::CrashHandler("11".into()))
        );
        assert_eq!(witness_of("checkpoint complete"), None);
        assert_eq!(signal_name("6: Abort trap"), "SIGABRT");
        assert_eq!(signal_name("11"), "SIGSEGV");
        assert_eq!(signal_name("64"), "64");
    }

    #[test]
    fn self_test_sees_the_backend_death() {
        let mut t = Tail::new(Path::new("/nonexistent/b.log"), Source::Stderr, &PrefixSpec::compile(PREFIX));
        t.feed_bytes(b"2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  ready\n");
        let m0 = t.mark();
        assert_eq!(self_test_sql(27), "pgrust: crash backend 27 quit");
        t.feed_bytes(
            b"2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  server process (PID 27) was terminated by signal 3: Quit\n\
              2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  terminating any other active server processes\n",
        );
        let m1 = t.mark();
        let v = self_test_verdict(t.parser(), m0, m1, 27);
        assert!(v.witnessed && v.pid_attributed);
        assert_eq!(v.witness_lines.len(), 1);
        let none = self_test_verdict(t.parser(), Mark(0), m0, 27);
        assert!(!none.witnessed && !none.pid_attributed);
    }

    #[test]
    fn tail_polls_a_real_file_and_survives_truncation() {
        let dir = std::env::temp_dir().join(format!("fuzzgen-logtail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("b.log");
        std::fs::write(&path, "2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  one\n").unwrap();
        let mut t = Tail::new(&path, Source::Stderr, &PrefixSpec::compile(PREFIX));
        assert_eq!(t.poll().unwrap(), 1);
        assert_eq!(t.poll().unwrap(), 0);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  two\n")
            .unwrap();
        assert_eq!(t.poll().unwrap(), 1);
        std::fs::write(&path, "2026-09-02 10:20:41.902 PDT postmaster[1] LOG:  rotated\n").unwrap();
        assert_eq!(t.poll().unwrap(), 1);
        assert_eq!(t.parser().lines().len(), 3);
        let missing = Tail::new(&dir.join("absent.log"), Source::Stderr, &PrefixSpec::compile(PREFIX));
        let mut missing = missing;
        assert_eq!(missing.poll().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    use std::io::Write;
}
