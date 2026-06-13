#![allow(non_snake_case)]

//! Idiomatic port of `src/backend/utils/misc/guc-file.l` — the configuration
//! file scanner and parser: `ProcessConfigFile`, `ParseConfigFile`,
//! `ParseConfigFp`, `ParseConfigDirectory`, `record_config_file_error`,
//! `FreeConfigVariables`, `DeescapeQuotedString`, and the flex tokenizer.
//!
//! The flex scanner in `guc-file.l` is reproduced here as a hand-written
//! [`Lexer`]/[`parse_line`] pair with the same token classes (`GUC_ID`,
//! `GUC_QUALIFIED_ID`, `GUC_STRING`, `GUC_INTEGER`, `GUC_REAL`,
//! `GUC_UNQUOTED_STRING`, `GUC_EQUALS`, `GUC_EOL`/`GUC_ERROR`), the recursive
//! `include`/`include_if_exists`/`include_dir` handling, the syntax-error
//! record-vs-throw distinction (keyed on `elevel`), and the
//! 100-error/`DEBUG1` abandonment rule.
//!
//! `AbsoluteConfigLocation` / `GetConfFilesInDir` are owned by `conffiles.c`
//! (routed through [`backend_utils_misc_conffiles_seams`]).
//! `ProcessConfigFileInternal` — the parse-then-apply core — is owned by
//! `guc.c` (routed through [`backend_utils_misc_guc_seams`]); `ProcessConfigFile`
//! here is the thin memory-context wrapper that drives it.

use std::path::{Path, PathBuf};

use backend_utils_error::{ereport, PgError, PgResult};
use backend_utils_misc_conffiles_seams::{absolute_config_location, get_conf_files_in_dir};
use types_error::{
    ErrorLevel, DEBUG1, DEBUG2, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};
use types_guc::{GucContext, PGC_POSTMASTER, PGC_SIGHUP};

/// `CONF_FILE_START_DEPTH` (`utils/conffiles.h`).
pub const CONF_FILE_START_DEPTH: i32 = 0;
/// `CONF_FILE_MAX_DEPTH` (`utils/conffiles.h`).
pub const CONF_FILE_MAX_DEPTH: i32 = 10;

/// A single parsed configuration entry, or a recorded parse error
/// (`struct ConfigVariable`, `utils/conffiles.h`). The C list links entries
/// through `next`; here the list is the owning `Vec` the parser appends to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigVariable {
    /// Setting name, or `None` for an error record.
    pub name: Option<String>,
    /// Setting value (deescaped), or `None` for an error record.
    pub value: Option<String>,
    /// The recorded error message, or `None` for a real setting.
    pub errmsg: Option<String>,
    /// The file the entry/error came from.
    pub filename: Option<PathBuf>,
    /// Line number within `filename`.
    pub sourceline: i32,
    /// True for an error record (skipped by GUC application).
    pub ignore: bool,
    /// Marked true once the GUC core has applied this setting.
    pub applied: bool,
}

impl ConfigVariable {
    /// A real `name = value` setting.
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

    /// A recorded parse error (the `record_config_file_error` path).
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

/// `ProcessConfigFile(GucContext context)` (guc-file.l) — re-read and apply the
/// configuration file at `context` (`PGC_POSTMASTER` during startup, or
/// `PGC_SIGHUP` on reload).
///
/// The C function runs `ProcessConfigFileInternal` in a private memory context
/// so leaked allocations do not accumulate across SIGHUP cycles, then drops it.
/// The owned `Vec` the parse path here returns is freed when the call unwinds,
/// so the wrapper is just the elevel selection and the internal call (routed to
/// `guc.c` via its seam — `ProcessConfigFileInternal` belongs to guc.c).
pub fn ProcessConfigFile(context: GucContext) -> PgResult<()> {
    debug_assert!(
        (context == PGC_POSTMASTER
            && !backend_utils_init_small_seams::is_under_postmaster::call())
            || context == PGC_SIGHUP
    );

    // To avoid cluttering the log, only the postmaster bleats loudly about
    // problems with the config file.
    let elevel = if backend_utils_init_small_seams::is_under_postmaster::call() {
        DEBUG2
    } else {
        LOG
    };

    backend_utils_misc_guc_seams::process_config_file_internal::call(context, true, elevel)
}

/// `ParseConfigFile` — open and parse one configuration file, appending its
/// settings (and any nested includes) to `variables`.  Returns whether parsing
/// succeeded; sub-`ERROR` errors are recorded rather than thrown.
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
    // Reject file name that is all-blank (including empty): strspn(name,
    // " \t\r\n") == strlen(name).
    if config_file
        .bytes()
        .all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
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

    // Reject too-deep include nesting depth.
    if depth > CONF_FILE_MAX_DEPTH {
        let error = ereport(elevel)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "could not open configuration file \"{config_file}\": maximum nesting depth exceeded"
            ))
            .into_error();
        record_or_throw(
            elevel,
            error,
            "nesting depth exceeded",
            calling_file,
            calling_lineno,
            variables,
        )?;
        return Ok(false);
    }

    let abs_path = absolute_config_location::call(
        config_file.to_string(),
        calling_file.map(Path::to_path_buf),
    );

    // Reject direct recursion (indirect recursion isn't worth detecting).
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

    let contents = match std::fs::read_to_string(&abs_path) {
        Ok(contents) => contents,
        // AllocateFile() == NULL: a strict include fails, a non-strict one is
        // silently skipped (the include_if_exists case).
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
            // ereport(LOG, "skipping missing configuration file ...")
            let _ = ereport(LOG)
                .errmsg(format!(
                    "skipping missing configuration file \"{}\"",
                    abs_path.display()
                ))
                .into_error();
            return Ok(true);
        }
    };

    ParseConfigFp(&contents, &abs_path, depth, elevel, variables)
}

/// `ParseConfigFp` — parse already-read configuration file `contents` line by
/// line (the flex scanner's `yylex` driven by the parser), appending settings
/// to `variables` and recursing for `include*` directives.
pub fn ParseConfigFp(
    contents: &str,
    config_file: &Path,
    depth: i32,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<bool> {
    let mut ok = true;
    let mut errorcount = 0;

    for (idx, raw_line) in logical_lines(contents).into_iter().enumerate() {
        let line_no = idx as i32 + 1;
        let mut lexer = Lexer::new(raw_line);
        let Some(first) = lexer.next_token() else {
            continue; // empty or comment line (GUC_EOL)
        };

        match parse_line(&mut lexer, first) {
            Ok(Some((name, value))) => {
                // An include* directive isn't a variable; process immediately.
                // The C uses ConfigFileLineno - 1 here (EOL already bumped the
                // counter); our line_no is the line the directive sits on.
                if guc_name_compare(&name, "include_dir") {
                    if !ParseConfigDirectory(
                        &value,
                        Some(config_file),
                        line_no,
                        depth + 1,
                        elevel,
                        variables,
                    )? {
                        ok = false;
                    }
                } else if guc_name_compare(&name, "include_if_exists") {
                    if !ParseConfigFile(
                        &value,
                        false,
                        Some(config_file),
                        line_no,
                        depth + 1,
                        elevel,
                        variables,
                    )? {
                        ok = false;
                    }
                } else if guc_name_compare(&name, "include") {
                    if !ParseConfigFile(
                        &value,
                        true,
                        Some(config_file),
                        line_no,
                        depth + 1,
                        elevel,
                        variables,
                    )? {
                        ok = false;
                    }
                } else {
                    // ordinary variable, append to list
                    variables.push(ConfigVariable::setting(
                        name,
                        value,
                        config_file.to_path_buf(),
                        line_no,
                    ));
                }
            }
            Ok(None) => {}
            Err(ParseLineError::NearEnd) => {
                report_syntax_error(config_file, line_no, None, elevel, variables)?;
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
        // logging at DEBUG level. At/above ERROR the report already threw.
        if errorcount > 0 && (errorcount >= 100 || elevel <= DEBUG1) {
            let _ = ereport(elevel)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "too many syntax errors found, abandoning file \"{}\"",
                    config_file.display()
                ))
                .into_error();
            break;
        }
    }

    Ok(ok)
}

/// `ParseConfigDirectory` — parse every `*.conf` file in `includedir`, in
/// alphabetical order (the `include_dir` directive).
pub fn ParseConfigDirectory(
    includedir: &str,
    calling_file: Option<&Path>,
    calling_lineno: i32,
    depth: i32,
    elevel: ErrorLevel,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<bool> {
    let files = get_conf_files_in_dir::call(
        includedir.to_string(),
        calling_file.map(Path::to_path_buf),
        elevel,
    )?;
    if let Some(err_msg) = files.err_msg {
        record_config_file_error(err_msg, calling_file, calling_lineno, variables);
        return Ok(false);
    }

    for filename in files.filenames {
        let filename = filename.to_string_lossy();
        if !ParseConfigFile(
            &filename,
            true,
            calling_file,
            calling_lineno,
            depth,
            elevel,
            variables,
        )? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `record_config_file_error` — capture an error message in the
/// `ConfigVariable` list returned by config file parsing.
pub fn record_config_file_error(
    errmsg: impl Into<String>,
    config_file: Option<&Path>,
    lineno: i32,
    variables: &mut Vec<ConfigVariable>,
) {
    variables.push(ConfigVariable::error(
        errmsg.into(),
        config_file.map(Path::to_path_buf),
        lineno,
    ));
}

/// `FreeConfigVariables` — free a list of `ConfigVariable`s (names, values,
/// errmsgs, filenames). The owning `Vec` frees its contents when cleared.
pub fn FreeConfigVariables(list: &mut Vec<ConfigVariable>) {
    list.clear();
}

/// `DeescapeQuotedString` — strip the surrounding quotes, collapse embedded
/// `''` sequences and the C-style backslash escapes flex emits.
pub fn DeescapeQuotedString(s: &str) -> String {
    let bytes = s.as_bytes();
    debug_assert!(bytes.len() >= 2 && bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'');

    // C: skip the leading quote, copy until the trailing quote (handled by the
    // i + 1 < len bound, which stops before the closing quote).
    let mut out = Vec::with_capacity(bytes.len().saturating_sub(2));
    let mut i = 1;
    while i + 1 < bytes.len() {
        match bytes[i] {
            b'\\' if i + 1 < bytes.len() => {
                i += 1;
                match bytes[i] {
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'0'..=b'7' => {
                        let mut oct = 0u8;
                        let mut k = 0;
                        while i + k < bytes.len() && k < 3 && matches!(bytes[i + k], b'0'..=b'7') {
                            oct = (oct << 3).wrapping_add(bytes[i + k] - b'0');
                            k += 1;
                        }
                        out.push(oct);
                        i += k - 1;
                    }
                    other => out.push(other),
                }
            }
            b'\'' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
                // doubled quote becomes just one quote
                i += 1;
                out.push(b'\'');
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Mirror the C "record below ERROR, throw at/above it" pattern: at or above
/// `ERROR` the C code `longjmp`s (return the `Err`); below it, record the error
/// in `variables` and continue.
fn record_or_throw(
    elevel: ErrorLevel,
    error: PgError,
    errmsg: impl Into<String>,
    config_file: Option<&Path>,
    lineno: i32,
    variables: &mut Vec<ConfigVariable>,
) -> PgResult<()> {
    if elevel >= ERROR {
        Err(error)
    } else {
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
    let error = ereport(elevel)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(message)
        .into_error();
    record_or_throw(
        elevel,
        error,
        "syntax error",
        Some(config_file),
        line_no,
        variables,
    )
}

/// `guc_name_compare` for the `include*` directive names: case-insensitive.
fn guc_name_compare(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

/// Split file contents into logical lines (the flex `\n` token boundaries).
fn logical_lines(contents: &str) -> Vec<&str> {
    if contents.is_empty() {
        return Vec::new();
    }
    contents.lines().collect()
}

// ----------------------------------------------------------------------------
// The hand-written analog of the flex scanner in guc-file.l.
//
// Token classes (guc-file.l %% rules):
//   ID              {LETTER}{LETTER_OR_DIGIT}*
//   QUALIFIED_ID    {ID}"."{ID}
//   STRING          \'([^'\\\n]|\\.|\'\')*\'
//   UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
//   INTEGER         {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*
//   REAL            {SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?
//   EQUALS          "="
// where LETTER is [A-Za-z_\200-\377], i.e. high-bit bytes count as letters.
// ----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
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
struct Token {
    kind: TokenKind,
    text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParseLineError {
    NearEnd,
    NearToken(String),
}

struct Lexer<'a> {
    line: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(line: &'a str) -> Self {
        Self { line, pos: 0 }
    }

    fn next_token(&mut self) -> Option<Token> {
        self.skip_ws();
        let rest = &self.line[self.pos..];
        let first = rest.as_bytes().first().copied()?;
        if first == b'#' {
            // comment: eat to end of line (GUC_EOL)
            self.pos = self.line.len();
            return None;
        }
        if first == b'=' {
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Equals,
                text: "=".into(),
            });
        }
        if first == b'\'' {
            return Some(self.scan_quoted());
        }

        // Longest non-delimiter run, then classify against the token regexes.
        let start = self.pos;
        while self.pos < self.line.len() {
            let b = self.line.as_bytes()[self.pos];
            if matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'#' | b'=') {
                break;
            }
            self.pos += 1;
        }
        let text = &self.line[start..self.pos];
        if text.is_empty() {
            // A byte that matches no rule but the catch-all "." → GUC_ERROR.
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Error,
                text: String::from_utf8_lossy(&[first]).into_owned(),
            });
        }
        Some(Token {
            kind: classify_token(text),
            text: text.to_owned(),
        })
    }

    fn skip_ws(&mut self) {
        while self.pos < self.line.len()
            && matches!(self.line.as_bytes()[self.pos], b' ' | b'\t' | b'\r')
        {
            self.pos += 1;
        }
    }

    fn scan_quoted(&mut self) -> Token {
        // STRING: \'([^'\\\n]|\\.|\'\')*\'
        let start = self.pos;
        self.pos += 1;
        while self.pos < self.line.len() {
            match self.line.as_bytes()[self.pos] {
                b'\\' => {
                    // \\. — a backslash escapes the next byte
                    self.pos = (self.pos + 2).min(self.line.len());
                }
                b'\'' => {
                    self.pos += 1;
                    if self.pos < self.line.len() && self.line.as_bytes()[self.pos] == b'\'' {
                        // '' — doubled quote stays inside the string
                        self.pos += 1;
                    } else {
                        return Token {
                            kind: TokenKind::String,
                            text: self.line[start..self.pos].to_owned(),
                        };
                    }
                }
                _ => self.pos += 1,
            }
        }
        // Unterminated quote: the catch-all "." matches the lone '.
        Token {
            kind: TokenKind::Error,
            text: "'".into(),
        }
    }
}

/// The `parse_file` grammar in `guc-file.l`: `NAME [=] VALUE` per line.
fn parse_line(
    lexer: &mut Lexer<'_>,
    first: Token,
) -> Result<Option<(String, String)>, ParseLineError> {
    // first token on line is option name
    if !matches!(first.kind, TokenKind::Id | TokenKind::QualifiedId) {
        return Err(ParseLineError::NearToken(first.text));
    }
    let name = first.text;

    // next we have an optional equal sign; discard if present
    let mut token = lexer.next_token().ok_or(ParseLineError::NearEnd)?;
    if token.kind == TokenKind::Equals {
        token = lexer.next_token().ok_or(ParseLineError::NearEnd)?;
    }

    // now we must have the option value
    let value = match token.kind {
        TokenKind::Id | TokenKind::Integer | TokenKind::Real | TokenKind::UnquotedString => {
            token.text
        }
        TokenKind::String => DeescapeQuotedString(&token.text),
        TokenKind::QualifiedId | TokenKind::Equals | TokenKind::Error => {
            return Err(ParseLineError::NearToken(token.text));
        }
    };

    // now we'd like an end of line
    if let Some(extra) = lexer.next_token() {
        return Err(ParseLineError::NearToken(extra.text));
    }

    Ok(Some((name, value)))
}

fn classify_token(text: &str) -> TokenKind {
    // flex picks the longest match, breaking ties by rule order: ID,
    // QUALIFIED_ID, STRING, UNQUOTED_STRING, INTEGER, REAL. ID and
    // QUALIFIED_ID are disjoint; an all-letter token is ID, a LETTER.LETTER
    // token is QUALIFIED_ID. We test the more specific classes first.
    if is_qualified_id(text) {
        TokenKind::QualifiedId
    } else if is_id(text) {
        TokenKind::Id
    } else if is_integer(text) {
        TokenKind::Integer
    } else if is_real(text) {
        TokenKind::Real
    } else if is_unquoted_string(text) {
        TokenKind::UnquotedString
    } else {
        TokenKind::Error
    }
}

fn is_id(text: &str) -> bool {
    let mut bytes = text.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    is_letter(first) && bytes.all(is_letter_or_digit)
}

fn is_qualified_id(text: &str) -> bool {
    let mut parts = text.split('.');
    let Some(left) = parts.next() else {
        return false;
    };
    let Some(right) = parts.next() else {
        return false;
    };
    parts.next().is_none() && is_id(left) && is_id(right)
}

fn is_unquoted_string(text: &str) -> bool {
    let mut bytes = text.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    is_letter(first)
        && bytes.all(|b| is_letter_or_digit(b) || matches!(b, b'-' | b'.' | b':' | b'/'))
}

fn is_integer(text: &str) -> bool {
    let Some(text) = strip_sign(text) else {
        return false;
    };
    let (digits, unit_start) = if let Some(rest) = text.strip_prefix("0x") {
        let len = rest.bytes().take_while(|b| b.is_ascii_hexdigit()).count();
        (len > 0, 2 + len)
    } else {
        let len = text.bytes().take_while(|b| b.is_ascii_digit()).count();
        (len > 0, len)
    };
    digits && text[unit_start..].bytes().all(|b| b.is_ascii_alphabetic())
}

fn is_real(text: &str) -> bool {
    let Some(text) = strip_sign(text) else {
        return false;
    };
    let Some(dot) = text.find('.') else {
        return false;
    };
    if !text[..dot].bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let after_dot = &text[dot + 1..];
    let exponent_at = after_dot.find(['e', 'E']);
    let (fraction, exponent) = match exponent_at {
        Some(pos) => (&after_dot[..pos], Some(&after_dot[pos + 1..])),
        None => (after_dot, None),
    };
    if !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    match exponent {
        Some(exponent) => {
            let Some(exponent) = strip_sign(exponent) else {
                return false;
            };
            !exponent.is_empty() && exponent.bytes().all(|b| b.is_ascii_digit())
        }
        None => true,
    }
}

fn strip_sign(text: &str) -> Option<&str> {
    let text = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);
    (!text.is_empty()).then_some(text)
}

fn is_letter(b: u8) -> bool {
    // LETTER = [A-Za-z_\200-\377]
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn is_letter_or_digit(b: u8) -> bool {
    is_letter(b) || b.is_ascii_digit()
}

/// `init_seams()` — install this crate's inward seams (the functions other
/// crates reach across a cycle: `ProcessConfigFile`).
pub fn init_seams() {
    backend_utils_misc_guc_file_seams::process_config_file::set(ProcessConfigFile);
}

#[cfg(test)]
mod tests;
