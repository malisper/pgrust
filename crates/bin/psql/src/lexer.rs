//! Statement extraction, modeled on psqlscan.l: splits input into
//! semicolon-terminated SQL statements and backslash commands, respecting
//! single/double quotes, E'' strings, dollar quoting, `--` and nested
//! `/* */` comments; performs :var / :'var' / :"var" interpolation; and
//! exposes the quote state psql's PROMPT2 %R needs.

use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QuoteState {
    None,
    /// Inside '...' (standard_conforming_strings on: backslash literal).
    Single,
    /// Inside E'...' (backslash escapes).
    EString,
    /// Inside "..."
    Double,
    /// Inside $tag$ ... $tag$ — payload is the full opening tag (e.g. "$x$").
    Dollar,
    /// Inside /* ... */ — payload depth.
    Comment,
}

pub struct ScanState {
    /// Accumulated (incomplete) statement text.
    pub buf: String,
    state: QuoteState,
    dollar_tag: String,
    comment_depth: u32,
    pub paren_depth: i32,
    pub standard_strings: bool,
}

pub enum ScanItem {
    /// A complete statement, including its terminating semicolon.
    Statement(String),
    /// A backslash command line (without the leading backslash).
    Backslash(String),
}

impl Default for ScanState {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanState {
    pub fn new() -> Self {
        ScanState {
            buf: String::new(),
            state: QuoteState::None,
            dollar_tag: String::new(),
            comment_depth: 0,
            paren_depth: 0,
            standard_strings: true,
        }
    }

    #[allow(dead_code)]
    pub fn quote_state(&self) -> QuoteState {
        self.state
    }

    /// True when nothing (or only whitespace) is buffered.
    pub fn buffer_empty(&self) -> bool {
        self.buf.trim().is_empty()
    }

    #[allow(dead_code)]
    pub fn reset_buffer(&mut self) {
        self.buf.clear();
        self.state = QuoteState::None;
        self.dollar_tag.clear();
        self.comment_depth = 0;
        self.paren_depth = 0;
    }

    /// PROMPT2's %R character.
    pub fn prompt2_char(&self) -> char {
        match self.state {
            QuoteState::Single | QuoteState::EString => '\'',
            QuoteState::Double => '"',
            QuoteState::Dollar => '$',
            QuoteState::Comment => '*',
            QuoteState::None => {
                if self.paren_depth > 0 {
                    '('
                } else {
                    '-'
                }
            }
        }
    }

    /// Feed one input line (no trailing newline). Returns the extracted
    /// complete items in order. Variable interpolation uses `vars`.
    pub fn scan_line(&mut self, line: &str, vars: &HashMap<String, String>) -> Vec<ScanItem> {
        let mut out = Vec::new();
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0usize;
        let n = chars.len();

        // If this is a fresh line being appended to a non-empty buffer,
        // join with a newline (psql keeps the original line structure).
        let mut appended_sep = false;

        macro_rules! push {
            ($c:expr) => {{
                if !appended_sep && !self.buf.is_empty() {
                    self.buf.push('\n');
                    appended_sep = true;
                } else {
                    appended_sep = true;
                }
                self.buf.push($c);
            }};
        }
        macro_rules! push_str {
            ($s:expr) => {{
                for c in $s.chars() {
                    push!(c);
                }
            }};
        }

        while i < n {
            let c = chars[i];
            match self.state {
                QuoteState::Single => {
                    push!(c);
                    if c == '\'' {
                        // '' is an escaped quote
                        if i + 1 < n && chars[i + 1] == '\'' {
                            push!('\'');
                            i += 2;
                            continue;
                        }
                        self.state = QuoteState::None;
                    } else if c == '\\' && !self.standard_strings {
                        if i + 1 < n {
                            push!(chars[i + 1]);
                            i += 2;
                            continue;
                        }
                    }
                    i += 1;
                }
                QuoteState::EString => {
                    push!(c);
                    if c == '\\' {
                        if i + 1 < n {
                            push!(chars[i + 1]);
                            i += 2;
                            continue;
                        }
                    } else if c == '\'' {
                        if i + 1 < n && chars[i + 1] == '\'' {
                            push!('\'');
                            i += 2;
                            continue;
                        }
                        self.state = QuoteState::None;
                    }
                    i += 1;
                }
                QuoteState::Double => {
                    push!(c);
                    if c == '"' {
                        if i + 1 < n && chars[i + 1] == '"' {
                            push!('"');
                            i += 2;
                            continue;
                        }
                        self.state = QuoteState::None;
                    }
                    i += 1;
                }
                QuoteState::Dollar => {
                    // Check for the closing tag at this position.
                    let tag: Vec<char> = self.dollar_tag.chars().collect();
                    if c == '$' && i + tag.len() <= n && chars[i..i + tag.len()].iter().copied().eq(tag.iter().copied())
                    {
                        push_str!(&self.dollar_tag.clone());
                        i += tag.len();
                        self.state = QuoteState::None;
                        self.dollar_tag.clear();
                    } else {
                        push!(c);
                        i += 1;
                    }
                }
                QuoteState::Comment => {
                    if c == '*' && i + 1 < n && chars[i + 1] == '/' {
                        push!('*');
                        push!('/');
                        i += 2;
                        self.comment_depth -= 1;
                        if self.comment_depth == 0 {
                            self.state = QuoteState::None;
                        }
                    } else if c == '/' && i + 1 < n && chars[i + 1] == '*' {
                        push!('/');
                        push!('*');
                        i += 2;
                        self.comment_depth += 1;
                    } else {
                        push!(c);
                        i += 1;
                    }
                }
                QuoteState::None => {
                    // Whitespace at start of an empty buffer: drop it.
                    if self.buf.is_empty() && (c == ' ' || c == '\t' || c == '\r') {
                        i += 1;
                        continue;
                    }
                    match c {
                        '\\' => {
                            // Backslash command: rest of line.
                            let rest: String = chars[i + 1..].iter().collect();
                            out.push(ScanItem::Backslash(rest));
                            i = n;
                        }
                        ';' => {
                            push!(';');
                            i += 1;
                            let stmt = std::mem::take(&mut self.buf);
                            self.paren_depth = 0;
                            appended_sep = false;
                            out.push(ScanItem::Statement(stmt));
                        }
                        '\'' => {
                            // E'...' if the preceding pushed char was e/E and
                            // it began a token — approximate psqlscan: check
                            // the char before in the buffer.
                            let prev = self.buf.chars().last();
                            let estring = matches!(prev, Some('e') | Some('E'))
                                && !prev_is_ident_continuation(&self.buf);
                            push!('\'');
                            self.state =
                                if estring { QuoteState::EString } else { QuoteState::Single };
                            i += 1;
                        }
                        '"' => {
                            push!('"');
                            self.state = QuoteState::Double;
                            i += 1;
                        }
                        '$' => {
                            // Dollar-quote open: $tag$ where tag is empty or
                            // an identifier (no digits first).
                            if let Some(tag_len) = dollar_tag_len(&chars[i..]) {
                                let tag: String = chars[i..i + tag_len].iter().collect();
                                push_str!(&tag);
                                self.dollar_tag = tag;
                                self.state = QuoteState::Dollar;
                                i += tag_len;
                            } else {
                                push!('$');
                                i += 1;
                            }
                        }
                        '-' if i + 1 < n && chars[i + 1] == '-' => {
                            // -- comment to end of line. psql drops it when
                            // the query buffer is still empty (a leading
                            // comment-only line never reaches the server);
                            // mid-statement it is kept verbatim.
                            if self.buf.is_empty() {
                                i = n;
                            } else {
                                while i < n {
                                    push!(chars[i]);
                                    i += 1;
                                }
                            }
                        }
                        '/' if i + 1 < n && chars[i + 1] == '*' => {
                            push!('/');
                            push!('*');
                            self.state = QuoteState::Comment;
                            self.comment_depth = 1;
                            i += 2;
                        }
                        '(' => {
                            self.paren_depth += 1;
                            push!('(');
                            i += 1;
                        }
                        ')' => {
                            if self.paren_depth > 0 {
                                self.paren_depth -= 1;
                            }
                            push!(')');
                            i += 1;
                        }
                        ':' => {
                            // :: cast, :name, :'name', :"name"
                            if i + 1 < n && chars[i + 1] == ':' {
                                push!(':');
                                push!(':');
                                i += 2;
                            } else if i + 1 < n
                                && (chars[i + 1] == '\'' || chars[i + 1] == '"')
                            {
                                let q = chars[i + 1];
                                // find closing quote
                                if let Some(endrel) =
                                    chars[i + 2..].iter().position(|&x| x == q)
                                {
                                    let name: String =
                                        chars[i + 2..i + 2 + endrel].iter().collect();
                                    if let Some(v) = vars.get(&name) {
                                        let rep = if q == '\'' {
                                            quote_literal(v)
                                        } else {
                                            quote_ident_forced(v)
                                        };
                                        push_str!(&rep);
                                        i += 2 + endrel + 1;
                                    } else {
                                        push!(':');
                                        i += 1;
                                    }
                                } else {
                                    push!(':');
                                    i += 1;
                                }
                            } else if i + 1 < n && is_ident_start(chars[i + 1]) {
                                let mut j = i + 1;
                                while j < n && is_ident_cont(chars[j]) {
                                    j += 1;
                                }
                                let name: String = chars[i + 1..j].iter().collect();
                                if let Some(v) = vars.get(&name) {
                                    push_str!(v);
                                    i = j;
                                } else {
                                    push!(':');
                                    i += 1;
                                }
                            } else {
                                push!(':');
                                i += 1;
                            }
                        }
                        _ => {
                            push!(c);
                            i += 1;
                        }
                    }
                }
            }
        }
        out
    }
}

fn prev_is_ident_continuation(buf: &str) -> bool {
    // For E'': the e must start its own token — i.e. the char before the
    // final e/E must not be an identifier char.
    let mut it = buf.chars().rev();
    let _e = it.next();
    match it.next() {
        None => false,
        Some(c) => is_ident_cont(c),
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || (c as u32) >= 0x80
}

fn is_ident_cont(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || (c as u32) >= 0x80
}

/// If chars (starting with '$') opens a dollar quote, return the tag length
/// including both '$'s.
fn dollar_tag_len(chars: &[char]) -> Option<usize> {
    debug_assert_eq!(chars[0], '$');
    let mut j = 1;
    while j < chars.len() {
        let c = chars[j];
        if c == '$' {
            return Some(j + 1);
        }
        let ok = if j == 1 { is_ident_start(c) } else { is_ident_cont(c) };
        if !ok {
            return None;
        }
        j += 1;
    }
    None
}

/// SQL-literal-quote a string (for :'var').
pub fn quote_literal(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('\'');
    for c in v.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Identifier-quote a string unconditionally (for :"var").
pub fn quote_ident_forced(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_all(lines: &[&str]) -> (Vec<String>, Vec<String>, ScanState) {
        let mut st = ScanState::new();
        let vars = HashMap::new();
        let mut stmts = Vec::new();
        let mut metas = Vec::new();
        for l in lines {
            for item in st.scan_line(l, &vars) {
                match item {
                    ScanItem::Statement(s) => stmts.push(s),
                    ScanItem::Backslash(s) => metas.push(s),
                }
            }
        }
        (stmts, metas, st)
    }

    #[test]
    fn simple_split() {
        let (s, _, st) = scan_all(&["select 1; select 2;"]);
        assert_eq!(s, vec!["select 1;", "select 2;"]);
        assert!(st.buffer_empty());
    }

    #[test]
    fn multiline() {
        let (s, _, _) = scan_all(&["select", "1;"]);
        assert_eq!(s, vec!["select\n1;"]);
    }

    #[test]
    fn quotes_hold_semicolons() {
        let (s, _, _) = scan_all(&["select 'a;b';"]);
        assert_eq!(s, vec!["select 'a;b';"]);
        let (s, _, _) = scan_all(&["select \"a;b\" from t;"]);
        assert_eq!(s, vec!["select \"a;b\" from t;"]);
    }

    #[test]
    fn dollar_quote() {
        let (s, _, st) = scan_all(&["select $x$ ; $ $x$;"]);
        assert_eq!(s, vec!["select $x$ ; $ $x$;"]);
        assert_eq!(st.quote_state(), QuoteState::None);
        let (s, _, st) = scan_all(&["select $$a;"]);
        assert!(s.is_empty());
        assert_eq!(st.quote_state(), QuoteState::Dollar);
    }

    #[test]
    fn comments() {
        let (s, _, _) = scan_all(&["select 1 -- trailing ; not a term", "+ 1;"]);
        assert_eq!(s, vec!["select 1 -- trailing ; not a term\n+ 1;"]);
        let (s, _, _) = scan_all(&["select /* ; /* nested ; */ still ; */ 1;"]);
        assert_eq!(s, vec!["select /* ; /* nested ; */ still ; */ 1;"]);
    }

    #[test]
    fn backslash_after_stmt() {
        let (s, m, _) = scan_all(&["select 1; \\dt foo"]);
        assert_eq!(s, vec!["select 1;"]);
        assert_eq!(m, vec!["dt foo"]);
    }

    #[test]
    fn estring_backslash_quote() {
        let (s, _, _) = scan_all(&["select E'a\\';b';"]);
        assert_eq!(s, vec!["select E'a\\';b';"]);
    }

    #[test]
    fn interpolation() {
        let mut st = ScanState::new();
        let mut vars = HashMap::new();
        vars.insert("who".to_string(), "wor'ld".to_string());
        let items = st.scan_line("select :'who', :who, :missing;", &vars);
        match &items[0] {
            ScanItem::Statement(s) => {
                assert_eq!(s, "select 'wor''ld', wor'ld, :missing;")
            }
            _ => panic!(),
        }
    }

    #[test]
    fn prompt_chars() {
        let (_, _, st) = scan_all(&["select 'abc"]);
        assert_eq!(st.prompt2_char(), '\'');
        let (_, _, st) = scan_all(&["select (1 +"]);
        assert_eq!(st.prompt2_char(), '(');
        let (_, _, st) = scan_all(&["select 1 +"]);
        assert_eq!(st.prompt2_char(), '-');
    }
}
