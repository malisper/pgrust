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
//! (routed through [`conffiles_seams`]).
//! `ProcessConfigFileInternal` — the parse-then-apply core — is owned by
//! `guc.c` (routed through [`guc_seams`]); `ProcessConfigFile`
//! here is the thin memory-context wrapper that drives it.

#[cfg(not(target_family = "wasm"))]
use std::fs as osfs_free;
#[cfg(target_family = "wasm")]
use wasm_libc_shim::fscompat as osfs_free;

use std::path::{Path, PathBuf};

use ::utils_error::{ereport, PgError, PgResult};
use ::conffiles_seams::{absolute_config_location, get_conf_files_in_dir};
use ::types_error::{
    ErrorLevel, DEBUG1, DEBUG2, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};
use ::types_guc::{GucContext, PGC_POSTMASTER, PGC_SIGHUP};

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
            && !init_small_seams::is_under_postmaster::call())
            || context == PGC_SIGHUP
    );

    // To avoid cluttering the log, only the postmaster bleats loudly about
    // problems with the config file.
    let elevel = if init_small_seams::is_under_postmaster::call() {
        DEBUG2
    } else {
        LOG
    };

    guc_seams::process_config_file_internal::call(context, true, elevel)
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

    // The flex scanner is `%option 8bit` and reads the file as raw bytes, so a
    // config file that is not valid UTF-8 (high-bit bytes are valid `LETTER`s,
    // \200-\377) must still parse. Read bytes, not a UTF-8 `String`.
    let contents = match osfs_free::read(&abs_path) {
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
///
/// `contents` is a raw byte slice: the flex scanner is `%option 8bit` and the
/// `LETTER` class includes `\200-\377`, so the input is not required to be
/// valid UTF-8.
pub fn ParseConfigFp(
    contents: &[u8],
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

/// Split file contents into logical lines on the flex `\n` token boundaries,
/// stripping a trailing `\r` (flex eats `\r` as whitespace). Mirrors
/// `str::lines` but over raw bytes so non-UTF-8 input parses.
fn logical_lines(contents: &[u8]) -> Vec<&[u8]> {
    if contents.is_empty() {
        return Vec::new();
    }
    let mut lines = Vec::new();
    for line in contents.split(|&b| b == b'\n') {
        // `split` yields a trailing empty element when `contents` ends in `\n`;
        // skip it so a final newline doesn't add a spurious empty line (matches
        // `str::lines`).
        lines.push(line);
    }
    if contents.last() == Some(&b'\n') {
        lines.pop();
    }
    lines
        .into_iter()
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect()
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
    line: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(line: &'a [u8]) -> Self {
        Self { line, pos: 0 }
    }

    fn next_token(&mut self) -> Option<Token> {
        // flex rule `[ \t\r]+` eats whitespace (the per-line `\n` rule is the
        // GUC_EOL boundary we split on; here `\n` never appears mid-line).
        self.skip_ws();
        let first = self.line.get(self.pos).copied()?;
        // flex rule `#.*` eats a comment to end of line → GUC_EOL (no token).
        if first == b'#' {
            self.pos = self.line.len();
            return None;
        }

        // Faithful flex maximal munch: at this position, find the longest match
        // among every `%%` rule, breaking length ties by rule order. The rule
        // order in guc-file.l is: ID, QUALIFIED_ID, STRING, UNQUOTED_STRING,
        // INTEGER, REAL, EQUALS, then the catch-all `.` (a single byte). flex
        // chooses the rule with the longest match; on equal length the rule
        // listed first wins. `.` never matches `\n`, but mid-line input has none.
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

        // Pick the longest; first-listed rule wins ties (so a strictly-greater
        // length is required to displace an earlier rule).
        let mut best_len = 0usize;
        let mut best_kind = TokenKind::Error;
        for (len, kind) in candidates {
            if len > best_len {
                best_len = len;
                best_kind = kind;
            }
        }

        if best_len == 0 {
            // No rule matched: the catch-all `.` consumes exactly one byte and
            // returns GUC_ERROR (this is also the unterminated-`'` path, since a
            // `'` with no valid STRING match falls through to `.`).
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Error,
                text: String::from_utf8_lossy(&[first]).into_owned(),
            });
        }

        let text = &rest[..best_len];
        self.pos += best_len;
        Some(Token {
            kind: best_kind,
            text: String::from_utf8_lossy(text).into_owned(),
        })
        // NB: the match_* helpers run on raw bytes (so high-bit `LETTER`s
        // \200-\377 classify correctly); `text` is only lossy-stringified for
        // storage in the `ConfigVariable`.
    }

    fn skip_ws(&mut self) {
        // flex `[ \t\r]+` whitespace class (the `\n` boundary is handled by the
        // logical-line split; flex also eats a stray `\r`).
        while self.pos < self.line.len() && matches!(self.line[self.pos], b' ' | b'\t' | b'\r') {
            self.pos += 1;
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

// ----------------------------------------------------------------------------
// `match_*`: the per-rule longest-match functions, one per flex `%%` token
// regex. Each returns the length (in bytes) of the longest prefix of `rest`
// that the rule matches, or 0 if the rule does not match at this position. The
// scanner picks the rule with the greatest length, ties broken by rule order
// (guc-file.l listing order). This reproduces flex maximal munch exactly,
// including the token boundaries that drive the `near token "..."` diagnostics
// and the single-byte GUC_ERROR catch-all.
//
// LETTER         = [A-Za-z_\200-\377]
// LETTER_OR_DIGIT = [A-Za-z_0-9\200-\377]
// SIGN           = ("-"|"+")
// DIGIT          = [0-9]
// HEXDIGIT       = [0-9a-fA-F]
// UNIT_LETTER    = [a-zA-Z]
// ----------------------------------------------------------------------------

/// `ID = {LETTER}{LETTER_OR_DIGIT}*`
fn match_id(rest: &[u8]) -> usize {
    let Some((&first, tail)) = rest.split_first() else {
        return 0;
    };
    if !is_letter(first) {
        return 0;
    }
    1 + tail.iter().take_while(|&&b| is_letter_or_digit(b)).count()
}

/// `QUALIFIED_ID = {ID}"."{ID}` — exactly one dot, an ID on each side. Because
/// `.` is not in `LETTER_OR_DIGIT`, the leading `{ID}` stops at the first dot;
/// the trailing `{ID}` then runs maximally (stopping at a second dot).
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

/// `STRING = \'([^'\\\n]|\\.|\'\')*\'`
///
/// Maximal munch with the subtlety that flex returns the longest match that
/// reaches a *closing* quote. A doubled `''` is body content only when the
/// string still terminates afterwards; otherwise the inner `'` is the closing
/// quote of a shorter (still valid) STRING. We scan greedily but remember the
/// most recent position at which a complete STRING ends, and return that.
fn match_string(rest: &[u8]) -> usize {
    if rest.first() != Some(&b'\'') {
        return 0;
    }
    let mut i = 1;
    let mut best = 0; // longest length ending in a closing quote
    while i < rest.len() {
        match rest[i] {
            b'\n' => break, // [^'\\\n] excludes newline; \\. and \'\' can't start with it
            b'\\' => {
                // \\. — backslash escapes exactly one following byte. A trailing
                // backslash (no following byte) matches nothing further.
                if i + 1 >= rest.len() {
                    break;
                }
                i += 2;
            }
            b'\'' => {
                // This quote can close the string: record a complete match...
                best = i + 1;
                // ...and `''` may instead be a doubled quote inside a longer
                // string, so keep scanning past it.
                if rest.get(i + 1) == Some(&b'\'') {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1, // [^'\\\n]
        }
    }
    best
}

/// `UNQUOTED_STRING = {LETTER}({LETTER_OR_DIGIT}|[-._:/])*`
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

/// `INTEGER = {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*`
fn match_integer(rest: &[u8]) -> usize {
    let mut i = match_sign(rest);
    let body = &rest[i..];
    let mantissa = if let Some(hex) = body.strip_prefix(b"0x") {
        let n = hex.iter().take_while(|b| b.is_ascii_hexdigit()).count();
        if n == 0 {
            return 0; // `0x` with no hex digits is not an INTEGER
        }
        2 + n
    } else {
        let n = body.iter().take_while(|b| b.is_ascii_digit()).count();
        if n == 0 {
            return 0;
        }
        n
    };
    i += mantissa;
    // UNIT_LETTER* = [a-zA-Z]*
    i += rest[i..]
        .iter()
        .take_while(|b| b.is_ascii_alphabetic())
        .count();
    i
}

/// `REAL = {SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?`, `EXPONENT = [Ee]{SIGN}?{DIGIT}+`
fn match_real(rest: &[u8]) -> usize {
    let mut i = match_sign(rest);
    // {DIGIT}*
    i += rest[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    // required "."
    if rest.get(i) != Some(&b'.') {
        return 0;
    }
    i += 1;
    // {DIGIT}*
    i += rest[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    // optional {EXPONENT}: only consumed if it fully matches [Ee]{SIGN}?{DIGIT}+
    if matches!(rest.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        j += match_sign(&rest[j..]);
        let digits = rest[j..].iter().take_while(|b| b.is_ascii_digit()).count();
        if digits > 0 {
            i = j + digits;
        }
        // else: leave `i` before the bare exponent letter; REAL ends at the
        // mantissa, the [Ee] is a separate token (flex maximal munch).
    }
    i
}

/// `EQUALS = "="`
fn match_equals(rest: &[u8]) -> usize {
    usize::from(rest.first() == Some(&b'='))
}

/// `{SIGN}?` — length of an optional leading `-`/`+`.
fn match_sign(rest: &[u8]) -> usize {
    usize::from(matches!(rest.first(), Some(b'-' | b'+')))
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
    guc_file_seams::process_config_file::set(ProcessConfigFile);
}

#[cfg(test)]
mod tests;
