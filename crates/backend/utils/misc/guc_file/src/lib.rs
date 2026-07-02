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

    for (idx, raw_line) in logical_lines(contents).into_iter().enumerate() {
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
pub fn DeescapeQuotedString(s: &str) -> String {
    let bytes = s.as_bytes();
    debug_assert!(bytes.len() >= 2 && bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'');

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
                i += 1;
                out.push(b'\'');
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
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
struct Token {
    kind: TokenKind,
    text: String,
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

    fn next_token(&mut self) -> Option<Token> {
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
            return Some(Token {
                kind: TokenKind::Error,
                text: String::from_utf8_lossy(&[first]).into_owned(),
            });
        }

        let text = &rest[..best_len];
        self.pos += best_len;
        Some(Token { kind: best_kind, text: String::from_utf8_lossy(text).into_owned() })
    }

    fn skip_ws(&mut self) {
        while self.pos < self.line.len() && matches!(self.line[self.pos], b' ' | b'\t' | b'\r') {
            self.pos += 1;
        }
    }
}

// The per-line grammar of ParseConfigFp: NAME [=] VALUE.
fn parse_line(lexer: &mut Lexer<'_>, first: Token) -> Result<(String, String), ParseLineError> {
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
        TokenKind::String => DeescapeQuotedString(&token.text),
        TokenKind::QualifiedId | TokenKind::Equals | TokenKind::Error => {
            return Err(ParseLineError::NearToken(token.text));
        }
    };

    if let Some(extra) = lexer.next_token() {
        return Err(ParseLineError::NearToken(extra.text));
    }

    Ok((name, value))
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

// Longest match that still ends at a closing quote: a doubled '' is body
// content only when the string terminates afterwards.
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
                if i + 1 >= rest.len() {
                    break;
                }
                i += 2;
            }
            b'\'' => {
                best = i + 1;
                if rest.get(i + 1) == Some(&b'\'') {
                    i += 2;
                } else {
                    i += 1;
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

fn match_integer(rest: &[u8]) -> usize {
    let mut i = match_sign(rest);
    let body = &rest[i..];
    let mantissa = if let Some(hex) = body.strip_prefix(b"0x") {
        let n = hex.iter().take_while(|b| b.is_ascii_hexdigit()).count();
        if n == 0 {
            return 0;
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
    i += rest[i..].iter().take_while(|b| b.is_ascii_alphabetic()).count();
    i
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
