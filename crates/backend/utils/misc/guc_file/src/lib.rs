#![allow(non_snake_case)]

// guc-file.l. The flex scanner is a hand lexer with the same token classes and
// maximal-munch rule order; the C STRING token cannot cross a newline
// ([^'\\\n], and `\\.`'s `.` excludes \n), so per-logical-line scanning is
// exactly equivalent to the flex buffer.

use std::path::{Path, PathBuf};

use conffiles_seams::{absolute_config_location, get_conf_files_in_dir};
use elog::ereport;
use types_error::{
    ErrorLevel, PgError, PgResult, DEBUG1, DEBUG2, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};
use types_guc::{GucContext, PGC_POSTMASTER, PGC_SIGHUP};

#[cfg(test)]
mod tests;

pub const CONF_FILE_START_DEPTH: i32 = 0;
pub const CONF_FILE_MAX_DEPTH: i32 = 10;

// struct ConfigVariable (utils/conffiles.h); the C linked list is the owning
// Vec the parser appends to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigVariable {
    pub name: Option<String>,
    pub value: Option<String>,
    pub errmsg: Option<String>,
    pub filename: Option<PathBuf>,
    pub sourceline: i32,
    pub ignore: bool,
    pub applied: bool,
}

impl ConfigVariable {
    pub fn setting(name: String, value: String, filename: PathBuf, sourceline: i32) -> Self {
        Self {
            name: Some(name),
            value: Some(value),
            errmsg: None,
            filename: Some(filename),
            sourceline,
            ignore: false,
            applied: false,
        }
    }

    pub fn error(errmsg: String, filename: Option<PathBuf>, sourceline: i32) -> Self {
        Self {
            name: None,
            value: None,
            errmsg: Some(errmsg),
            filename,
            sourceline,
            ignore: true,
            applied: false,
        }
    }
}

// ProcessConfigFile(context): the C body runs ProcessConfigFileInternal in a
// throwaway context; the parse list here is an owned Vec freed on return.
pub fn ProcessConfigFile(context: GucContext) -> PgResult<()> {
    debug_assert!(
        (context == PGC_POSTMASTER && !init_small::globals::IsUnderPostmaster())
            || context == PGC_SIGHUP
    );

    // Only the postmaster bleats loudly about config file problems.
    let elevel = if init_small::globals::IsUnderPostmaster() { DEBUG2 } else { LOG };

    guc_seams::process_config_file_internal::call(context, true, elevel)
}

#[allow(clippy::too_many_arguments)]
pub fn ParseConfigFile(
    config_file: &str,
    strict: bool,
    calling_file: Option<&Path>,
    calling_lineno: i32,
    depth: i32,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<bool> {
    // An all-blank (or empty) name would read the containing directory.
    if config_file.bytes().all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n')) {
        let error = ereport(elevel)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("empty configuration file name: \"{config_file}\""))
            .into_error();
        record_or_throw(
            elevel,
            error,
            "empty configuration file name",
            calling_file,
            calling_lineno,
            variables,
        )?;
        return Ok(false);
    }

    if depth > CONF_FILE_MAX_DEPTH {
        let error = ereport(elevel)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "could not open configuration file \"{config_file}\": maximum nesting depth exceeded"
            ))
            .into_error();
        record_or_throw(elevel, error, "nesting depth exceeded", calling_file, calling_lineno, variables)?;
        return Ok(false);
    }

    let abs_path =
        absolute_config_location::call(config_file.to_string(), calling_file.map(Path::to_path_buf));

    // Reject direct recursion (canonicalization above makes strcmp likely to
    // match; indirect recursion is caught by the depth limit).
    if calling_file.is_some_and(|calling_file| abs_path == calling_file) {
        let error = ereport(elevel)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "configuration file recursion in \"{}\"",
                calling_file.unwrap().display()
            ))
            .into_error();
        record_or_throw(
            elevel,
            error,
            "configuration file recursion",
            calling_file,
            calling_lineno,
            variables,
        )?;
        return Ok(false);
    }

    // The scanner is %option 8bit (high-bit bytes are LETTERs): read raw
    // bytes, not UTF-8.
    let contents = match std::fs::read(&abs_path) {
        Ok(contents) => contents,
        Err(error) if strict => {
            let mut builder = ereport(elevel);
            if let Some(errno) = error.raw_os_error() {
                builder = builder.with_saved_errno(errno).errcode_for_file_access();
            }
            let pg_error = builder
                .errmsg(format!(
                    "could not open configuration file \"{}\": %m",
                    abs_path.display()
                ))
                .into_error();
            record_or_throw(
                elevel,
                pg_error,
                format!("could not open file \"{}\"", abs_path.display()),
                calling_file,
                calling_lineno,
                variables,
            )?;
            return Ok(false);
        }
        Err(_) => {
            let e = ereport(LOG)
                .errmsg(format!("skipping missing configuration file \"{}\"", abs_path.display()))
                .into_error();
            if elog::message_level_is_interesting(LOG) {
                elog::emit_error_report_for(&e);
            }
            return Ok(true);
        }
    };

    ParseConfigFp(&contents, &abs_path, depth, elevel, variables)
}

pub fn ParseConfigFp(
    contents: &[u8],
    config_file: &Path,
    depth: i32,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<bool> {
    let mut ok = true;
    let mut errorcount = 0;

    let lines = logical_lines(contents);
    let line_count = lines.len();
    // Whether the last logical line ends at EOF with no terminating \n. The
    // C scanner's ConfigFileLineno counts consumed \n tokens: a
    // near-end-of-line syntax error on such a line reports
    // ConfigFileLineno - 1 == line_no - 1 (the EOF adjustment of bug 4752
    // applies only to the successful-setting path, and the error path
    // keeps the off-by-one — match it exactly; found by guc_file_diff).
    let last_line_unterminated = contents.last() != Some(&b'\n');

    for (idx, raw_line) in lines.into_iter().enumerate() {
        let line_no = idx as i32 + 1;
        let mut lexer = Lexer::new(raw_line);
        let Some(first) = lexer.next_token() else {
            continue;
        };

        match parse_line(&mut lexer, first) {
            Ok((name, value)) => {
                // include* directives aren't variables; process immediately.
                if name.eq_ignore_ascii_case("include_dir") {
                    if !ParseConfigDirectory(&value, Some(config_file), line_no, depth + 1, elevel, variables)? {
                        ok = false;
                    }
                } else if name.eq_ignore_ascii_case("include_if_exists") {
                    if !ParseConfigFile(&value, false, Some(config_file), line_no, depth + 1, elevel, variables)? {
                        ok = false;
                    }
                } else if name.eq_ignore_ascii_case("include") {
                    if !ParseConfigFile(&value, true, Some(config_file), line_no, depth + 1, elevel, variables)? {
                        ok = false;
                    }
                } else {
                    variables.push(ConfigVariable::setting(
                        name,
                        value,
                        config_file.to_path_buf(),
                        line_no,
                    ));
                }
            }
            Err(ParseLineError::NearEnd) => {
                let report_line = if idx + 1 == line_count && last_line_unterminated {
                    line_no - 1
                } else {
                    line_no
                };
                report_syntax_error(config_file, report_line, None, elevel, variables)?;
                ok = false;
                errorcount += 1;
            }
            Err(ParseLineError::NearToken(token)) => {
                report_syntax_error(config_file, line_no, Some(&token), elevel, variables)?;
                ok = false;
                errorcount += 1;
            }
        }

        // Give up after 100 syntax errors per file, or immediately when only
        // logging at DEBUG level.
        if errorcount > 0 && (errorcount >= 100 || elevel <= DEBUG1) {
            let e = ereport(elevel)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "too many syntax errors found, abandoning file \"{}\"",
                    config_file.display()
                ))
                .into_error();
            if elog::message_level_is_interesting(elevel) {
                elog::emit_error_report_for(&e);
            }
            break;
        }
    }

    Ok(ok)
}

pub fn ParseConfigDirectory(
    includedir: &str,
    calling_file: Option<&Path>,
    calling_lineno: i32,
    depth: i32,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<bool> {
    let files =
        get_conf_files_in_dir::call(includedir.to_string(), calling_file.map(Path::to_path_buf), elevel)?;
    if let Some(err_msg) = files.err_msg {
        record_config_file_error(err_msg, calling_file, calling_lineno, variables);
        return Ok(false);
    }

    for filename in files.filenames {
        let filename = filename.to_string_lossy().into_owned();
        if !ParseConfigFile(&filename, true, calling_file, calling_lineno, depth, elevel, variables)? {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn record_config_file_error(
    errmsg: impl Into<String>,
    config_file: Option<&Path>,
    lineno: i32,
    variables: &mut Vec<ConfigVariable>,
) {
    variables.push(ConfigVariable::error(errmsg.into(), config_file.map(Path::to_path_buf), lineno));
}

pub fn FreeConfigVariables(list: &mut Vec<ConfigVariable>) {
    list.clear();
}

// DeescapeQuotedString: strip surrounding quotes, collapse '' and the C-style
// backslash escapes.
//
// The &str form is the exported (bootstrap-scanner) entry point; the parser
// calls the BYTE core directly. The distinction is load-bearing: C operates
// on the raw yytext bytes, so running the escape arithmetic over an already
// UTF-8-lossy String changes which byte is dropped as the trailing quote and
// how far octal runs reach (each invalid byte becomes a 3-byte U+FFFD).
// Found by guc_file_diff on a value of high-bit bytes.
pub fn DeescapeQuotedString(s: &str) -> String {
    String::from_utf8_lossy(&deescape_quoted_bytes(s.as_bytes())).into_owned()
}

// C's body, byte-for-byte. `s` is the raw token text as C sees it: NUL
// truncated (strlen), leading quote present, trailing quote present unless
// the NUL truncation removed it.
//
// The structure matters and is easy to get subtly wrong: C copies EVERY
// input byte (including the trailing quote) and then overwrites the LAST
// OUTPUT byte with NUL. Stopping the loop one input byte early instead is
// equivalent only while every body element is one byte wide — escapes
// compress, so "'x\n\t\4" (NUL-truncated, no closing quote) keeps a byte in
// pgrust that C drops. Found by guc_file_diff.
//
// UPSTREAM DEFECT (PostgreSQL 18.3 guc-file.l, unreported): when the
// NUL-truncated token is just "'", C's len becomes 0, palloc(0) returns a
// zero-length chunk, the copy loop never runs and `newStr[--j]` with j == 0
// writes one byte BEFORE the allocation. Confirmed under ASan against the
// vendored scanner (heap-buffer-overflow, WRITE of size 1, 1 byte before a
// 1-byte region) for the config line `a = '<NUL>x'`. pgrust cannot
// reproduce a wild write, so it returns empty there; the differential
// target carves that input class rather than comparing against UB.
pub fn deescape_quoted_bytes(bytes: &[u8]) -> Vec<u8> {
    // C only Asserts the surrounding quotes, and Assert() is compiled out in
    // the release build that is the behavior of record; the assertion is in
    // fact reachable upstream via the NUL truncation above. A debug_assert
    // here would be a ported-in constraint C does not enforce.
    if bytes.is_empty() {
        return Vec::new();
    }
    let s = &bytes[1..]; // C: s++, len--
    let len = s.len();
    // C's buffer is NUL-terminated, so s[len] reads as 0 and the scans below
    // stop there exactly as they do in C.
    let at = |i: usize| -> u8 { if i < len { s[i] } else { 0 } };

    let mut out = Vec::with_capacity(len);
    let mut i = 0;
    while i < len {
        if at(i) == b'\\' {
            i += 1;
            match at(i) {
                b'b' => out.push(0x08),
                b'f' => out.push(0x0c),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'0'..=b'7' => {
                    let mut oct = 0u8;
                    let mut k = 0;
                    while k < 3 && matches!(at(i + k), b'0'..=b'7') {
                        oct = (oct << 3).wrapping_add(at(i + k) - b'0');
                        k += 1;
                    }
                    out.push(oct);
                    i = i + k - 1;
                }
                other => out.push(other),
            }
        } else if at(i) == b'\'' && at(i + 1) == b'\'' {
            i += 1;
            out.push(at(i));
        } else {
            out.push(at(i));
        }
        i += 1;
    }

    // C: newStr[--j] = '\0' — the ending quote was copied, so drop it.
    // j == 0 is the upstream underflow carved above; pgrust yields empty.
    out.pop();
    out
}

// C records below ERROR and longjmps at/above it.
fn record_or_throw(
    elevel: ErrorLevel,
    error: PgError,
    errmsg: impl Into<String>,
    config_file: Option<&Path>,
    lineno: i32,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<()> {
    if elevel >= ERROR {
        Err(error.into())
    } else {
        if elog::message_level_is_interesting(elevel) {
            elog::emit_error_report_for(&error);
        }
        record_config_file_error(errmsg, config_file, lineno, variables);
        Ok(())
    }
}

fn report_syntax_error(
    config_file: &Path,
    line_no: i32,
    token: Option<&str>,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<()> {
    let message = match token {
        Some(token) => format!(
            "syntax error in file \"{}\" line {}, near token \"{}\"",
            config_file.display(),
            line_no,
            token
        ),
        None => format!(
            "syntax error in file \"{}\" line {}, near end of line",
            config_file.display(),
            line_no
        ),
    };
    let error = ereport(elevel).errcode(ERRCODE_SYNTAX_ERROR).errmsg(message).into_error();
    record_or_throw(elevel, error, "syntax error", Some(config_file), line_no, variables)
}

fn logical_lines(contents: &[u8]) -> Vec<&[u8]> {
    if contents.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&[u8]> = contents.split(|&b| b == b'\n').collect();
    if contents.last() == Some(&b'\n') {
        lines.pop();
    }
    lines
        .into_iter()
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect()
}

// The %% token rules, in listing order (flex maximal munch: longest match,
// ties to the first-listed rule):
//   ID              {LETTER}{LETTER_OR_DIGIT}*
//   QUALIFIED_ID    {ID}"."{ID}
//   STRING          \'([^'\\\n]|\\.|\'\')*\'
//   UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
//   INTEGER         {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*
//   REAL            {SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?
//   EQUALS          "="
//   .               GUC_ERROR
// LETTER = [A-Za-z_\200-\377].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenKind {
    Id,
    QualifiedId,
    String,
    Integer,
    Real,
    UnquotedString,
    Equals,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Token<'a> {
    kind: TokenKind,
    /// C's pstrdup(yytext)/%s view: UTF-8-lossy, truncated at the first NUL.
    text: String,
    /// The same bytes BEFORE the lossy conversion (still NUL-truncated, as
    /// strlen() sees them). DeescapeQuotedString's arithmetic runs on these.
    raw: &'a [u8],
}

enum ParseLineError {
    NearEnd,
    NearToken(String),
}

struct Lexer<'a> {
    line: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(line: &'a [u8]) -> Self {
        Self { line, pos: 0 }
    }

    fn next_token(&mut self) -> Option<Token<'a>> {
        self.skip_ws();
        let first = self.line.get(self.pos).copied()?;
        if first == b'#' {
            self.pos = self.line.len();
            return None;
        }

        let rest = &self.line[self.pos..];
        let candidates = [
            (match_id(rest), TokenKind::Id),
            (match_qualified_id(rest), TokenKind::QualifiedId),
            (match_string(rest), TokenKind::String),
            (match_unquoted_string(rest), TokenKind::UnquotedString),
            (match_integer(rest), TokenKind::Integer),
            (match_real(rest), TokenKind::Real),
            (match_equals(rest), TokenKind::Equals),
        ];

        let mut best_len = 0usize;
        let mut best_kind = TokenKind::Error;
        for (len, kind) in candidates {
            if len > best_len {
                best_len = len;
                best_kind = kind;
            }
        }

        if best_len == 0 {
            // The catch-all `.` consumes one byte and returns GUC_ERROR (also
            // the unterminated-quote path).
            self.pos += 1;
            let raw = &self.line[self.pos - 1..self.pos];
            return Some(Token { kind: TokenKind::Error, text: token_text(raw), raw: nul_trunc(raw) });
        }

        let text = &rest[..best_len];
        self.pos += best_len;
        Some(Token { kind: best_kind, text: token_text(text), raw: nul_trunc(text) })
    }

    fn skip_ws(&mut self) {
        while self.pos < self.line.len() && matches!(self.line[self.pos], b' ' | b'\t' | b'\r') {
            self.pos += 1;
        }
    }
}

// The per-line grammar of ParseConfigFp: NAME [=] VALUE.
fn parse_line<'a>(
    lexer: &mut Lexer<'a>,
    first: Token<'a>,
) -> Result<(String, String), ParseLineError> {
    if !matches!(first.kind, TokenKind::Id | TokenKind::QualifiedId) {
        return Err(ParseLineError::NearToken(first.text));
    }
    let name = first.text;

    let mut token = lexer.next_token().ok_or(ParseLineError::NearEnd)?;
    if token.kind == TokenKind::Equals {
        token = lexer.next_token().ok_or(ParseLineError::NearEnd)?;
    }

    let value = match token.kind {
        TokenKind::Id | TokenKind::Integer | TokenKind::Real | TokenKind::UnquotedString => {
            token.text
        }
        // C: opt_value = DeescapeQuotedString(yytext) — raw bytes in, and the
        // palloc'd result is read back as a C string (so it ends at the first
        // NUL an escape may have produced).
        TokenKind::String => {
            let de = deescape_quoted_bytes(token.raw);
            String::from_utf8_lossy(nul_trunc(&de)).into_owned()
        }
        TokenKind::QualifiedId | TokenKind::Equals | TokenKind::Error => {
            return Err(ParseLineError::NearToken(token.text));
        }
    };

    if let Some(extra) = lexer.next_token() {
        return Err(ParseLineError::NearToken(extra.text));
    }

    Ok((name, value))
}

// Token text as C materializes it. The scanner advances over the FULL match
// (yyleng), but every consumer copies it as a C string — pstrdup(yytext) for
// names/values, %s for the syntax-error messages, strlen() inside
// DeescapeQuotedString. All of those stop at the first NUL byte, so a match
// containing an embedded NUL yields a TRUNCATED string on the C side while
// the scan position still advances past the whole token. Reproduce that
// exactly (found by guc_file_diff: "a\0b = 1" and "x = 'nul\0byte'").
fn token_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(nul_trunc(bytes)).into_owned()
}

/// The prefix a C string API (strlen/pstrdup/%s) would see.
fn nul_trunc(bytes: &[u8]) -> &[u8] {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    &bytes[..end]
}

fn match_id(rest: &[u8]) -> usize {
    let Some((&first, tail)) = rest.split_first() else {
        return 0;
    };
    if !is_letter(first) {
        return 0;
    }
    1 + tail.iter().take_while(|&&b| is_letter_or_digit(b)).count()
}

fn match_qualified_id(rest: &[u8]) -> usize {
    let left = match_id(rest);
    if left == 0 || rest.get(left) != Some(&b'.') {
        return 0;
    }
    let right = match_id(&rest[left + 1..]);
    if right == 0 {
        return 0;
    }
    left + 1 + right
}

// STRING = \'([^'\\\n]|\\.|\'\')*\'. flex takes the LONGEST match, and the
// DFA backtracks: a doubled '' may be body content OR the closing quote
// followed by an unrelated quote, and only the longest decomposition that
// actually terminates counts. So record a candidate end at EVERY quote and
// keep scanning through doubled quotes; a LONE quote can only be the
// terminator, and nothing valid follows it.
//
// Consequence on a run of N quotes: matches exist exactly at even lengths,
// so the match is the largest even number <= N (verified against the
// vendored scanner for N = 1..41). Two guc_file_diff divergences came from
// getting this wrong: scanning on past a lone quote (over-long match on
// "ate''='doubled''quote''end'"), then failing an odd run outright instead
// of backtracking one pair ("'" x 39).
fn match_string(rest: &[u8]) -> usize {
    if rest.first() != Some(&b'\'') {
        return 0;
    }
    let mut i = 1;
    let mut best = 0;
    while i < rest.len() {
        match rest[i] {
            b'\n' => break,
            b'\\' => {
                // \\. matches any single char except newline, and cannot run
                // off the end of the buffer.
                if i + 1 >= rest.len() || rest[i + 1] == b'\n' {
                    break;
                }
                i += 2;
            }
            b'\'' => {
                best = i + 1; // the string can validly close here
                if rest.get(i + 1) == Some(&b'\'') {
                    i += 2; // ...or '' is body content; keep looking for longer
                } else {
                    break; // a lone quote is the terminator
                }
            }
            _ => i += 1,
        }
    }
    best
}

fn match_unquoted_string(rest: &[u8]) -> usize {
    let Some((&first, tail)) = rest.split_first() else {
        return 0;
    };
    if !is_letter(first) {
        return 0;
    }
    1 + tail
        .iter()
        .take_while(|&&b| is_letter_or_digit(b) || matches!(b, b'-' | b'.' | b':' | b'/'))
        .count()
}

// INTEGER = {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*. The two mantissa
// alternatives are independent flex alternatives, and the rule as a whole
// takes the LONGEST overall match — so a failed 0x form must fall back to
// the decimal form rather than failing the rule. "0x" therefore lexes as
// INTEGER (digits "0" + unit letter "x"), not as an error, and "0x1f" wins
// with the hex form. Found by guc_file_diff (pgrust returned a syntax error
// where C accepted the setting).
fn match_integer(rest: &[u8]) -> usize {
    let sign = match_sign(rest);
    let body = &rest[sign..];

    let hex_mantissa = match body.strip_prefix(b"0x") {
        Some(hex) => match hex.iter().take_while(|b| b.is_ascii_hexdigit()).count() {
            0 => 0,
            n => 2 + n,
        },
        None => 0,
    };
    let dec_mantissa = body.iter().take_while(|b| b.is_ascii_digit()).count();

    let mut best = 0;
    for mantissa in [hex_mantissa, dec_mantissa] {
        if mantissa == 0 {
            continue;
        }
        let mut end = sign + mantissa;
        end += rest[end..].iter().take_while(|b| b.is_ascii_alphabetic()).count();
        if end > best {
            best = end;
        }
    }
    best
}

fn match_real(rest: &[u8]) -> usize {
    let mut i = match_sign(rest);
    i += rest[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    if rest.get(i) != Some(&b'.') {
        return 0;
    }
    i += 1;
    i += rest[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    // EXPONENT is consumed only when [Ee]{SIGN}?{DIGIT}+ fully matches.
    if matches!(rest.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        j += match_sign(&rest[j..]);
        let digits = rest[j..].iter().take_while(|b| b.is_ascii_digit()).count();
        if digits > 0 {
            i = j + digits;
        }
    }
    i
}

fn match_equals(rest: &[u8]) -> usize {
    usize::from(rest.first() == Some(&b'='))
}

fn match_sign(rest: &[u8]) -> usize {
    usize::from(matches!(rest.first(), Some(b'-' | b'+')))
}

fn is_letter(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn is_letter_or_digit(b: u8) -> bool {
    is_letter(b) || b.is_ascii_digit()
}

pub fn init_seams() {
    guc_file_seams::process_config_file::set(ProcessConfigFile);
}
