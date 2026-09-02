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
    // upstream 29921259e83b (18.6): Teach psql to skip in-line COPY ... FROM STDIN data after a failure.
    // (psqlscan.l's statement-start tracking.)
    begin_depth: i32,
    copy_stdin_count: i32,
    init_idents_count: usize,
    init_idents: [u8; 8],
}

pub enum ScanItem {
    /// A complete statement, including its terminating semicolon, and the
    /// number of COPY ... FROM STDIN commands it contains.
    Statement(String, i32),
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
            begin_depth: 0,
            copy_stdin_count: 0,
            init_idents_count: 0,
            init_idents: [0; 8],
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

    /// psql_scan_reset: before a new query string starts.
    pub fn reset_buffer(&mut self) {
        self.buf.clear();
        self.state = QuoteState::None;
        self.dollar_tag.clear();
        self.comment_depth = 0;
        self.paren_depth = 0;
        self.reset_statement_tracking();
    }

    fn reset_statement_tracking(&mut self) {
        self.begin_depth = 0;
        self.copy_stdin_count = 0;
        self.init_idents_count = 0;
    }

    /// psqlscan_track_identifier: the first few keywords of a statement
    /// (COPY ... FROM STDIN) and routine BEGIN/CASE ... END nesting.
    fn track_identifier(&mut self, ident: &str) {
        if self.paren_depth != 0 {
            return;
        }
        if self.init_idents_count == 0 {
            self.init_idents = [0; 8];
        }
        if self.init_idents_count < self.init_idents.len() {
            let first = ident.as_bytes()[0];
            // Routine keywords lower case, COPY keywords upper case, else 0.
            let mark = if ["create", "function", "procedure", "or", "replace"]
                .iter()
                .any(|k| ident.eq_ignore_ascii_case(k))
            {
                first.to_ascii_lowercase()
            } else if ["copy", "from", "stdin", "stdout"].iter().any(|k| ident.eq_ignore_ascii_case(k))
            {
                first.to_ascii_uppercase()
            } else {
                0
            };
            self.init_idents[self.init_idents_count] = mark;
            self.init_idents_count += 1;
        }
        if is_create_routine(&self.init_idents) {
            if ident.eq_ignore_ascii_case("begin") {
                self.begin_depth += 1;
            } else if ident.eq_ignore_ascii_case("case") {
                if self.begin_depth >= 1 {
                    self.begin_depth += 1;
                }
            } else if ident.eq_ignore_ascii_case("end") && self.begin_depth > 0 {
                self.begin_depth -= 1;
            }
        }
    }

    /// Interpolated variable text: track its unquoted words.
    fn track_identifiers_in(&mut self, text: &str) {
        let chars: Vec<char> = text.chars().collect();
        let mut i = 0;
        let mut quote: Option<char> = None;
        while i < chars.len() {
            let c = chars[i];
            if let Some(q) = quote {
                if c == q {
                    quote = None;
                }
                i += 1;
            } else if c == '\'' || c == '"' {
                quote = Some(c);
                i += 1;
            } else if is_ident_start(c) {
                let mut j = i + 1;
                while j < chars.len() && is_ident_cont(chars[j]) {
                    j += 1;
                }
                let ident: String = chars[i..j].iter().collect();
                self.track_identifier(&ident);
                i = j;
            } else {
                i += 1;
            }
        }
    }

    /// psqlscan_is_copy_from_stdin: COPY, then FROM STDIN among the words.
    fn is_copy_from_stdin(&self) -> bool {
        let id = &self.init_idents;
        if id[0] != b'C' {
            return false;
        }
        for i in 1..id.len() - 1 {
            if id[i] != b'F' {
                continue;
            }
            return id[i + 1] == b'S';
        }
        false
    }

    /// psql_scan_count_copy_from_stdin (a trailing command counts once).
    pub fn count_copy_from_stdin(&mut self) -> i32 {
        if self.init_idents_count > 0 {
            if self.is_copy_from_stdin() {
                self.copy_stdin_count += 1;
            }
            self.init_idents_count = 0;
        }
        self.copy_stdin_count
    }

    /// SendQuery's -1 path: one psql_scan over a caller-supplied string.
    pub fn count_copy_from_stdin_in_string(query: &str, standard_strings: bool) -> i32 {
        let mut st = ScanState::new();
        st.standard_strings = standard_strings;
        let vars = HashMap::new();
        for line in query.split('\n') {
            for item in st.scan_line(line, &vars) {
                return match item {
                    ScanItem::Statement(_, n) => n,
                    ScanItem::Backslash(_) => st.count_copy_from_stdin(),
                };
            }
        }
        st.count_copy_from_stdin()
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
    #[allow(unused_assignments)] // Rust-structural: push! sets appended_sep in both arms for uniformity; the pre-flush instance is dead by construction (no psqlscan.l counterpart)
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
                            // upstream 29921259e83b (18.6): a top-level ';'
                            // books a COPY FROM STDIN and resets the tracking.
                            if self.paren_depth == 0 && self.begin_depth == 0 {
                                if self.is_copy_from_stdin() {
                                    self.copy_stdin_count += 1;
                                }
                                let stmt = std::mem::take(&mut self.buf);
                                let ncopy = self.copy_stdin_count;
                                self.reset_statement_tracking();
                                appended_sep = false;
                                out.push(ScanItem::Statement(stmt, ncopy));
                            }
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
                                    self.track_identifiers_in(v);
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
                        c if is_ident_start(c) => {
                            // {identifier}; the E'..'/N/B/X/U& prefixes are not.
                            let mut j = i + 1;
                            while j < n && is_ident_cont(chars[j]) {
                                j += 1;
                            }
                            let literal_prefix = j == i + 1
                                && ((matches!(c, 'e' | 'E' | 'n' | 'N' | 'b' | 'B' | 'x' | 'X')
                                    && j < n
                                    && chars[j] == '\'')
                                    || (matches!(c, 'u' | 'U')
                                        && j + 1 < n
                                        && chars[j] == '&'
                                        && (chars[j + 1] == '\'' || chars[j + 1] == '"')));
                            if !literal_prefix {
                                let ident: String = chars[i..j].iter().collect();
                                self.track_identifier(&ident);
                            }
                            while i < j {
                                push!(chars[i]);
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

/// psqlscan_is_create_routine: CREATE [OR REPLACE] {FUNCTION|PROCEDURE}.
fn is_create_routine(id: &[u8; 8]) -> bool {
    id[0] == b'c'
        && (id[1] == b'f'
            || id[1] == b'p'
            || (id[1] == b'o' && id[2] == b'r' && (id[3] == b'f' || id[3] == b'p')))
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
                    ScanItem::Statement(s, _) => stmts.push(s),
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
            ScanItem::Statement(s, _) => {
                assert_eq!(s, "select 'wor''ld', wor'ld, :missing;")
            }
            _ => panic!(),
        }
    }

    // upstream 29921259e83b (18.6): Teach psql to skip in-line COPY ... FROM STDIN data after a failure.
    fn copy_counts(lines: &[&str]) -> Vec<i32> {
        let mut st = ScanState::new();
        let vars = HashMap::new();
        let mut counts = Vec::new();
        for l in lines {
            for item in st.scan_line(l, &vars) {
                if let ScanItem::Statement(_, n) = item {
                    counts.push(n);
                }
            }
        }
        counts
    }

    #[test]
    fn copy_from_stdin_is_counted_per_statement() {
        assert_eq!(copy_counts(&["copy t from stdin;"]), vec![1]);
        assert_eq!(copy_counts(&["COPY BINARY public.t FROM STDIN;"]), vec![1]);
        assert_eq!(copy_counts(&["copy t (a, b) from stdin with (format csv);"]), vec![1]);
        assert_eq!(copy_counts(&["copy t", "from", "stdin;"]), vec![1]);
        assert_eq!(copy_counts(&["copy t to stdout;"]), vec![0]);
        assert_eq!(copy_counts(&["copy (select 1) to stdout;"]), vec![0]);
        assert_eq!(copy_counts(&["select 'copy t from stdin';"]), vec![0]);
        assert_eq!(copy_counts(&["select E'x' from stdin;"]), vec![0]);
        assert_eq!(copy_counts(&["select 1; copy t from stdin; select 2;"]), vec![0, 1, 0]);
        assert_eq!(copy_counts(&["copy t from stdin", "copy t from stdin;"]), vec![1]);
        assert_eq!(copy_counts(&["copy a b c d e f g from stdin;"]), vec![0]);
        let mut st = ScanState::new();
        let mut vars = HashMap::new();
        vars.insert("cmd".to_string(), "copy t from stdin".to_string());
        match st.scan_line(":cmd;", &vars).pop() {
            Some(ScanItem::Statement(s, n)) => {
                assert_eq!(s, "copy t from stdin;");
                assert_eq!(n, 1);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn trailing_copy_from_stdin_counts_once_at_eof() {
        let (s, _, mut st) = scan_all(&["copy t from stdin"]);
        assert!(s.is_empty());
        assert_eq!(st.count_copy_from_stdin(), 1);
        assert_eq!(st.count_copy_from_stdin(), 1);
        st.reset_buffer();
        assert_eq!(st.count_copy_from_stdin(), 0);
        let (_, _, mut st) = scan_all(&["copy t from stdin;", "select 1"]);
        assert_eq!(st.count_copy_from_stdin(), 0);
    }

    #[test]
    fn count_copy_from_stdin_in_string_stops_at_first_terminator() {
        assert_eq!(ScanState::count_copy_from_stdin_in_string("copy t from stdin", true), 1);
        assert_eq!(ScanState::count_copy_from_stdin_in_string("copy t from stdin;\nselect 1", true), 1);
        assert_eq!(ScanState::count_copy_from_stdin_in_string("select 1; copy t from stdin", true), 0);
        assert_eq!(ScanState::count_copy_from_stdin_in_string("copy t from stdin \\echo x", true), 1);
        assert_eq!(ScanState::count_copy_from_stdin_in_string("select 1", true), 0);
    }

    #[test]
    fn routine_body_semicolons_do_not_end_the_statement() {
        let (s, _, _) = scan_all(&[
            "create function f() returns int language sql begin atomic",
            "  select case when true then 1 else 2 end;",
            "  select 2;",
            "end;",
            "select 3;",
        ]);
        assert_eq!(s.len(), 2, "{s:?}");
        assert!(s[0].ends_with("\nend;"));
        assert_eq!(s[1], "select 3;");
        let (s, _, _) =
            scan_all(&["CREATE OR REPLACE PROCEDURE p() BEGIN ATOMIC select 1; END; select 2;"]);
        assert_eq!(s, vec!["CREATE OR REPLACE PROCEDURE p() BEGIN ATOMIC select 1; END;", "select 2;"]);
        assert_eq!(scan_all(&["begin; select 1; end;"]).0.len(), 3);
        let (s, _, st) = scan_all(&["select (1;"]);
        assert!(s.is_empty());
        assert_eq!(st.prompt2_char(), '(');
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
